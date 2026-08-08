"""
tests/test_catalog_preview.py — Unit tests for modules/catalog_preview.py.

All pipeline-stage dependencies (qc, astrometry, subtraction, catalog_matcher)
are mocked. A real, minimal FITS file backs each test so fits.open() and the
matplotlib rendering path actually run — this module's whole job is to
produce a real PNG, so that part is deliberately not mocked away.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock

import numpy as np
import pytest
from astropy.io import fits

from modules import catalog_preview


@pytest.fixture
def fits_file(tmp_path):
    """A real, minimal, valid FITS file with actual pixel data and headers."""
    path = tmp_path / "frame.fits"
    hdu = fits.PrimaryHDU(data=np.random.default_rng(0).random((50, 50)).astype(np.float32))
    hdu.header["OBJECT"] = "M51"
    hdu.header["FILTER"] = "L"
    hdu.header["DATE-OBS"] = "2024-03-15T22:01:34"
    hdu.writeto(path)
    return str(path)


class _FakeWCS:
    """Minimal WCS stand-in — world_to_pixel just needs to return a pixel pair."""

    def world_to_pixel(self, skycoord):
        return (25.0, 25.0)


@pytest.fixture
def mock_stages(monkeypatch, tmp_path):
    """Patch every pipeline-stage dependency catalog_preview.render() calls."""
    monkeypatch.setattr(catalog_preview.config, "FITS_ARCHIVE", str(tmp_path / "archive"))

    qc_mock = AsyncMock(return_value={
        "quality_flag": "OK", "fwhm_median": 3.0, "elongation_median": 1.1, "star_count": 10,
    })
    monkeypatch.setattr(catalog_preview.qc, "analyze", qc_mock)

    astro_mock = AsyncMock(return_value={
        "ra_center": 202.47, "dec_center": 47.2, "fov_deg": 1.0,
        "sources": [{"ra": 202.47, "dec": 47.2, "flux": 100.0, "elongation": 1.1}],
        "sources_all": [
            {"ra": 202.47, "dec": 47.2, "flux": 100.0, "elongation": 1.1},
            {"ra": 202.48, "dec": 47.21, "flux": 50.0, "elongation": 1.2},
        ],
        "wcs": _FakeWCS(),
    })
    monkeypatch.setattr(catalog_preview.astrometry, "solve", astro_mock)

    sub_mock = AsyncMock(return_value={"performed": False, "reference_frame_count": 0, "candidates": []})
    monkeypatch.setattr(catalog_preview.subtraction, "run", sub_mock)

    # First source matches a catalog, second doesn't — proof matched/total
    # counting and the green/red split are driven by real catalog_name values.
    async def mock_match(sources, frame_meta):
        sources[0]["catalog_name"] = "Gaia DR3"
        sources[0]["catalog_id"] = "123"
        sources[1].setdefault("catalog_name", None)
        sources[1].setdefault("catalog_id", None)
        return sources
    match_mock = AsyncMock(side_effect=mock_match)
    monkeypatch.setattr(catalog_preview.catalog_matcher, "match", match_mock)

    return {"qc": qc_mock, "astro": astro_mock, "sub": sub_mock, "match": match_mock}


class TestRender:
    async def test_returns_summary_and_png_bytes(self, mock_stages, fits_file):
        result = await catalog_preview.render(fits_file)

        assert result["matched"] == 1
        assert result["total"] == 2
        assert result["quality_flag"] == "OK"
        # A real PNG, not a placeholder — matplotlib actually rendered it.
        assert isinstance(result["png_bytes"], bytes)
        assert result["png_bytes"][:8] == b"\x89PNG\r\n\x1a\n"

    async def test_no_local_file_is_left_behind(self, mock_stages, fits_file, tmp_path):
        """Nothing here should write a file that outlives the call — the PNG
        goes straight to the caller as bytes, and astap's scratch dir
        (astrometry.solve()'s output_base) must be a temp dir that's removed
        on the way out, not something under the project tree."""
        before = set(os.listdir(tmp_path))

        await catalog_preview.render(fits_file)

        after = set(os.listdir(tmp_path))
        assert after == before  # only fits_file's own directory contents, unchanged

    async def test_qc_never_moves_a_rejected_frame(self, mock_stages, fits_file):
        """This module is a read-only diagnostic — it must call qc.analyze()
        with move_on_reject=False, never letting it relocate the input file
        (see debug/README.md's "Background" for the incident this guards)."""
        await catalog_preview.render(fits_file)

        mock_stages["qc"].assert_called_once()
        assert mock_stages["qc"].call_args.kwargs.get("move_on_reject") is False

    async def test_raises_when_astrometry_fails(self, mock_stages, fits_file):
        mock_stages["astro"].return_value = {}

        with pytest.raises(RuntimeError, match="Astrometry failed"):
            await catalog_preview.render(fits_file)

    async def test_subtraction_failure_does_not_abort(self, mock_stages, fits_file):
        mock_stages["sub"].side_effect = RuntimeError("no reference frames yet")

        result = await catalog_preview.render(fits_file)

        assert result["total"] == 2  # subtraction candidates simply weren't added, no crash

    async def test_input_file_is_never_modified(self, mock_stages, fits_file):
        before = os.path.getmtime(fits_file)

        await catalog_preview.render(fits_file)

        assert os.path.getmtime(fits_file) == before

    async def test_catalog_matcher_receives_frame_meta(self, mock_stages, fits_file):
        await catalog_preview.render(fits_file)

        frame_meta = mock_stages["match"].call_args.args[1]
        assert frame_meta["ra_center"] == 202.47
        assert frame_meta["dec_center"] == 47.2
        assert frame_meta["fov_deg"] == 1.0
