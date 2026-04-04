"""
tests/test_astrometry.py — Unit tests for modules/astrometry.py

All external I/O is mocked:
  - modules.astrometry.subprocess.run  → controlled CompletedProcess
  - modules.astrometry.fits.open       → context manager returning fake HDU
  - modules.astrometry.sep.Background  → _FakeBackground with fixed globalrms
  - modules.astrometry.sep.extract     → structured numpy array of fake sources
  - modules.astrometry.WCS             → astropy WCS built from known parameters

All tests are async because astrometry.solve() is declared async.
asyncio_mode = auto is set in pytest.ini, so no @pytest.mark.asyncio required.
"""

from __future__ import annotations

import subprocess
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from astropy.wcs import WCS as AstropyWCS

from modules import astrometry


# ---------------------------------------------------------------------------
# Synthetic WCS helper
# ---------------------------------------------------------------------------

def _make_wcs(
    ra: float = 202.47,
    dec: float = 47.20,
    scale_deg: float = 0.000278,
    celestial: bool = True,
) -> AstropyWCS:
    """
    Return a simple TAN WCS centred at (ra, dec) with the given pixel scale.

    Parameters
    ----------
    ra, dec:
        Reference sky coordinates in decimal degrees.
    scale_deg:
        Pixel scale in degrees/pixel (~1 arcsec/px at default 0.000278).
    celestial:
        If False, build a non-celestial (LINEAR) WCS so that has_celestial
        returns False — used to test the invalid-WCS path.
    """
    w = AstropyWCS(naxis=2)
    if celestial:
        w.wcs.ctype = ["RA---TAN", "DEC--TAN"]
        w.wcs.crpix = [512.0, 512.0]
        w.wcs.crval = [ra, dec]
        # RA axis is conventionally negative (increasing to the West)
        w.wcs.cdelt = [-scale_deg, scale_deg]
    else:
        w.wcs.ctype = ["LINEAR", "LINEAR"]
        w.wcs.crpix = [512.0, 512.0]
        w.wcs.crval = [0.0, 0.0]
        w.wcs.cdelt = [1.0, 1.0]
    w.wcs.set()
    return w


# ---------------------------------------------------------------------------
# Synthetic source catalogue helper
# ---------------------------------------------------------------------------

def _make_sources(
    n: int = 20,
    a: float = 1.5,
    b: float = 1.4,
    flux: float = 5000.0,
    peak: float = 1000.0,
) -> np.ndarray:
    """
    Return a structured array that mimics sep.extract() output.

    Only the fields consumed by astrometry.solve() are populated:
    x, y (pixel position), a, b (semi-axes), flux, peak.
    """
    dtype = np.dtype([
        ("x",    np.float64),
        ("y",    np.float64),
        ("a",    np.float64),
        ("b",    np.float64),
        ("flux", np.float64),
        ("peak", np.float64),
    ])
    arr = np.zeros(n, dtype=dtype)
    arr["x"]    = np.linspace(100.0, 900.0, n)
    arr["y"]    = np.linspace(100.0, 900.0, n)
    arr["a"]    = a
    arr["b"]    = b
    arr["flux"] = flux
    arr["peak"] = peak
    return arr


# ---------------------------------------------------------------------------
# Fake sep.Background
# ---------------------------------------------------------------------------

class _FakeBackground:
    """
    Minimal stand-in for sep.Background.

    Subtraction ``data - bkg`` must work; we implement __rsub__ on the class
    so that NumPy's array.__sub__ can delegate to it.
    """

    def __init__(self, globalrms: float = 20.0, globalback: float = 800.0) -> None:
        self.globalrms:  float = globalrms
        self.globalback: float = globalback
        self._back = globalback

    def __rsub__(self, other: np.ndarray) -> np.ndarray:
        return np.ascontiguousarray(other - self._back)


# ---------------------------------------------------------------------------
# Fake FITS HDU infrastructure
# ---------------------------------------------------------------------------

_IMAGE_SHAPE = (1024, 1024)
_FITS_PATH   = "/fake/fits/incoming/frame_test.fits"


class _FakeHeader:
    """Minimal stand-in for astropy.io.fits.Header."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]
    
    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def copy(self) -> "_FakeHeader":
        return _FakeHeader(self._data.copy())


class _FakeHDU:
    """Minimal stand-in for an astropy PrimaryHDU."""

    def __init__(self, data: np.ndarray, header: dict[str, Any]) -> None:
        self.data   = data
        self.header = _FakeHeader(header)


class _FakeHDUL:
    """Context-manager wrapper around a single HDU list."""

    def __init__(self, hdu: _FakeHDU) -> None:
        self._hdu = hdu

    def __enter__(self) -> "_FakeHDUL":
        return self

    def __exit__(self, *args: Any) -> bool:
        return False

    def __getitem__(self, idx: int) -> _FakeHDU:
        return self._hdu


def _make_hdul(
    image: np.ndarray | None = None,
    header: dict[str, Any] | None = None,
) -> _FakeHDUL:
    """Build a fits.open()-compatible context manager."""
    if image is None:
        image = np.ones(_IMAGE_SHAPE, dtype=np.float64)
    if header is None:
        header = {"NAXIS1": _IMAGE_SHAPE[1], "NAXIS2": _IMAGE_SHAPE[0]}
    return _FakeHDUL(_FakeHDU(image, header))


# ---------------------------------------------------------------------------
# Shared patch fixture — applies all external mocks in one place
# ---------------------------------------------------------------------------

@contextmanager
def _patch_astrometry(
    subprocess_rc: int = 0,
    subprocess_raises: type[Exception] | None = None,
    wcs: AstropyWCS | None = None,
    sources: np.ndarray | None = None,
    sep_background_raises: bool = False,
    naxis1: int = _IMAGE_SHAPE[1],
    naxis2: int = _IMAGE_SHAPE[0],
):
    """
    Patch every external dependency of astrometry.py in one shot.

    Parameters
    ----------
    subprocess_rc:
        Return code for the mocked subprocess.run call (0 = success).
    subprocess_raises:
        If set, subprocess.run raises this exception type instead.
    wcs:
        WCS object returned by the mocked WCS() constructor.
        Defaults to a valid celestial TAN projection centred at (202.47, 47.20).
    sources:
        Structured array returned by sep.extract().
        Defaults to _make_sources() (20 sources).
    sep_background_raises:
        If True, sep.Background raises RuntimeError.
    naxis1, naxis2:
        Image dimensions embedded in the fake FITS header.
    """
    if wcs is None:
        wcs = _make_wcs()
    if sources is None:
        sources = _make_sources()

    header = {"NAXIS1": naxis1, "NAXIS2": naxis2}
    image  = np.ones((naxis2, naxis1), dtype=np.float64)
    hdul   = _make_hdul(image, header)

    fake_bkg = _FakeBackground()

    def _subprocess_run(*args, **kwargs):
        if subprocess_raises is not None:
            if subprocess_raises is subprocess.TimeoutExpired:
                raise subprocess.TimeoutExpired(cmd="astap", timeout=60)
            raise subprocess_raises()
        # Return success with "Solution found" in output (required by astrometry.py)
        return MagicMock(
            returncode=subprocess_rc, 
            stdout="Solution found: RA=12h34m, Dec=+45d" if subprocess_rc == 0 else "",
            stderr=""
        )

    def _sep_background(data):
        if sep_background_raises:
            raise RuntimeError("sep.Background intentional failure")
        return fake_bkg

    # WCS is imported at module level as `from astropy.wcs import WCS`, so we
    # patch the name in the astrometry module's namespace directly.
    with (
        patch("modules.astrometry.subprocess.run", side_effect=_subprocess_run),
        patch("modules.astrometry.fits.open", return_value=hdul),
        patch("modules.astrometry.WCS", return_value=wcs),
        patch("modules.astrometry.sep.Background", side_effect=_sep_background),
        patch("modules.astrometry.sep.extract", return_value=sources),
    ):
        yield


# ---------------------------------------------------------------------------
# Test 1 — Successful solve returns all required keys
# ---------------------------------------------------------------------------

class TestSuccessfulSolve:
    async def test_successful_solve_returns_all_keys(self):
        with _patch_astrometry():
            result = await astrometry.solve(_FITS_PATH)

        assert isinstance(result, dict)
        for key in ("ra_center", "dec_center", "fov_deg", "sources", "wcs"):
            assert key in result, f"Missing key: {key}"

    async def test_ra_dec_center_correct(self):
        """
        WCS centred at (202.47, 47.20); 1024x1024 image.
        The centre pixel should map back to (202.47, 47.20) within 0.01 deg.
        """
        wcs = _make_wcs(ra=202.47, dec=47.20)
        with _patch_astrometry(wcs=wcs, naxis1=1024, naxis2=1024):
            result = await astrometry.solve(_FITS_PATH)

        assert abs(result["ra_center"]  - 202.47) < 0.01
        assert abs(result["dec_center"] -  47.20) < 0.01

    async def test_fov_computed(self):
        """FOV must be positive."""
        with _patch_astrometry():
            result = await astrometry.solve(_FITS_PATH)

        assert result["fov_deg"] > 0.0

    async def test_fov_value_is_reasonable(self):
        """
        1024 px × 0.000278 deg/px ≈ 0.285 deg.
        Allow 50 % tolerance for the column-norm derivation from cdelt.
        """
        wcs = _make_wcs(scale_deg=0.000278)
        with _patch_astrometry(wcs=wcs, naxis1=1024, naxis2=1024):
            result = await astrometry.solve(_FITS_PATH)

        expected = 1024 * 0.000278
        assert abs(result["fov_deg"] - expected) / expected < 0.5


# ---------------------------------------------------------------------------
# Test 2 — Source dict shape and types
# ---------------------------------------------------------------------------

class TestSourceFormat:
    async def test_sources_have_correct_keys(self):
        """Every source dict must carry exactly: ra, dec, flux, fwhm, elongation."""
        with _patch_astrometry():
            result = await astrometry.solve(_FITS_PATH)

        required = {"ra", "dec", "flux", "fwhm", "elongation"}
        for src in result["sources"]:
            assert set(src.keys()) == required

    async def test_sources_ra_dec_are_floats(self):
        with _patch_astrometry():
            result = await astrometry.solve(_FITS_PATH)

        for src in result["sources"]:
            assert isinstance(src["ra"],  float), "ra must be a Python float"
            assert isinstance(src["dec"], float), "dec must be a Python float"

    async def test_sources_all_fields_are_floats(self):
        with _patch_astrometry():
            result = await astrometry.solve(_FITS_PATH)

        for src in result["sources"]:
            for key, val in src.items():
                assert isinstance(val, float), f"{key} must be a Python float, got {type(val)}"

    async def test_source_count_matches_sep_output(self):
        """The number of returned sources must equal what sep.extract returned."""
        n = 20
        with _patch_astrometry(sources=_make_sources(n=n)):
            result = await astrometry.solve(_FITS_PATH)

        assert len(result["sources"]) == n

    async def test_elongation_positive(self):
        """Elongation is a/b and must be >= 1 for well-formed sources."""
        with _patch_astrometry(sources=_make_sources(a=1.5, b=1.4)):
            result = await astrometry.solve(_FITS_PATH)

        for src in result["sources"]:
            assert src["elongation"] >= 1.0

    async def test_fwhm_positive(self):
        with _patch_astrometry():
            result = await astrometry.solve(_FITS_PATH)

        for src in result["sources"]:
            assert src["fwhm"] > 0.0


# ---------------------------------------------------------------------------
# Test 3 — astap failure modes
# ---------------------------------------------------------------------------

class TestAstapFailures:
    async def test_astap_nonzero_exit_returns_empty(self):
        """Non-zero return code from astap must produce an empty result dict."""
        with _patch_astrometry(subprocess_rc=1):
            result = await astrometry.solve(_FITS_PATH)

        assert result == {}

    async def test_astap_timeout_returns_empty(self):
        """TimeoutExpired during astap must produce an empty result dict."""
        with _patch_astrometry(subprocess_raises=subprocess.TimeoutExpired):
            result = await astrometry.solve(_FITS_PATH)

        assert result == {}

    async def test_astap_not_found_returns_empty(self):
        """FileNotFoundError (binary missing) must produce an empty result dict."""
        with _patch_astrometry(subprocess_raises=FileNotFoundError):
            result = await astrometry.solve(_FITS_PATH)

        assert result == {}


# ---------------------------------------------------------------------------
# Test 4 — Zero-source frame
# ---------------------------------------------------------------------------

class TestNoSources:
    async def test_no_sources_returns_empty_list(self):
        """sep returning 0 sources must yield sources == [] — not an error."""
        with _patch_astrometry(sources=_make_sources(n=0)):
            result = await astrometry.solve(_FITS_PATH)

        assert isinstance(result, dict)
        assert "sources" in result
        assert result["sources"] == []
        # Other keys should still be present
        assert "ra_center" in result
        assert "dec_center" in result
        assert "fov_deg" in result
        assert "wcs" in result


# ---------------------------------------------------------------------------
# Test 5 — sep failure
# ---------------------------------------------------------------------------

class TestSepFailure:
    async def test_sep_failure_returns_empty(self):
        """If sep.Background raises, the function must return {} — not re-raise."""
        with _patch_astrometry(sep_background_raises=True):
            result = await astrometry.solve(_FITS_PATH)

        assert result == {}


# ---------------------------------------------------------------------------
# Test 6 — Invalid WCS
# ---------------------------------------------------------------------------

class TestInvalidWcs:
    async def test_invalid_wcs_returns_empty(self):
        """
        A WCS without celestial axes (has_celestial == False) must cause the
        function to log an error and return {}.
        """
        non_celestial_wcs = _make_wcs(celestial=False)
        with _patch_astrometry(wcs=non_celestial_wcs):
            result = await astrometry.solve(_FITS_PATH)

        assert result == {}


# ---------------------------------------------------------------------------
# Test 7 — WCS object is propagated to caller
# ---------------------------------------------------------------------------

class TestWcsPropagated:
    async def test_wcs_object_in_result(self):
        """The wcs key must hold an astropy WCS instance."""
        with _patch_astrometry():
            result = await astrometry.solve(_FITS_PATH)

        assert isinstance(result["wcs"], AstropyWCS)


# ---------------------------------------------------------------------------
# Test 8 — Zero minor-axis guard (degenerate sources)
# ---------------------------------------------------------------------------

class TestDegenerateSource:
    async def test_zero_b_axis_does_not_raise(self):
        """
        Sources with b=0 (degenerate ellipse) must not cause ZeroDivisionError.
        They are filtered out by the star detection criteria (elongation too high).
        """
        degenerate = _make_sources(n=5, a=2.0, b=0.0)
        with _patch_astrometry(sources=degenerate):
            result = await astrometry.solve(_FITS_PATH)

        # Should succeed without raising ZeroDivisionError
        assert isinstance(result, dict)
        # All sources are filtered out because elongation = a/1e-6 >> 2.0
        assert len(result.get("sources", [])) == 0
