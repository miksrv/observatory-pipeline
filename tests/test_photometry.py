"""
tests/test_photometry.py — Unit tests for modules/photometry.py

All external I/O is mocked:
  - modules.photometry.fits.open         → context manager returning fake HDU
  - modules.photometry.WCS               → astropy WCS built from known parameters
  - modules.photometry.sigma_clipped_stats → fixed (mean, median, sigma) triple
  - modules.photometry.aperture_photometry → table with fixed aperture_sum
  - modules.photometry.ApertureStats      → object with fixed .median sky value

All tests are async because photometry.measure() is declared async.
asyncio_mode = auto is set in pytest.ini, so no @pytest.mark.asyncio required.
"""

from __future__ import annotations

import math
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from astropy.wcs import WCS as AstropyWCS

from modules import photometry


# ---------------------------------------------------------------------------
# Synthetic WCS helper (mirrors test_astrometry.py)
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
        If False, build a non-celestial (LINEAR) WCS so has_celestial returns
        False — used to test the invalid-WCS path.
    """
    w = AstropyWCS(naxis=2)
    if celestial:
        w.wcs.ctype = ["RA---TAN", "DEC--TAN"]
        w.wcs.crpix = [512.0, 512.0]
        w.wcs.crval = [ra, dec]
        w.wcs.cdelt = [-scale_deg, scale_deg]
    else:
        w.wcs.ctype = ["LINEAR", "LINEAR"]
        w.wcs.crpix = [512.0, 512.0]
        w.wcs.crval = [0.0, 0.0]
        w.wcs.cdelt = [1.0, 1.0]
    w.wcs.set()
    return w


# ---------------------------------------------------------------------------
# Fake FITS HDU infrastructure (mirrors test_astrometry.py)
# ---------------------------------------------------------------------------

_IMAGE_SHAPE = (1024, 1024)
_FITS_PATH   = "/fake/fits/archive/M51/frame_test.fits"

_BASE_HEADER: dict[str, Any] = {
    "NAXIS1": _IMAGE_SHAPE[1],
    "NAXIS2": _IMAGE_SHAPE[0],
}


class _FakeHeader:
    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)


class _FakeHDU:
    def __init__(self, data: np.ndarray, header: dict[str, Any]) -> None:
        self.data   = data
        self.header = _FakeHeader(header)


class _FakeHDUL:
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
    if image is None:
        image = np.ones(_IMAGE_SHAPE, dtype=np.float64) * 1000.0
    if header is None:
        header = dict(_BASE_HEADER)
    return _FakeHDUL(_FakeHDU(image, header))


# ---------------------------------------------------------------------------
# Synthetic source list helpers
# ---------------------------------------------------------------------------

def _make_source(
    ra: float = 202.47,
    dec: float = 47.20,
    flux: float = 50000.0,
    fwhm: float = 3.0,
    elongation: float = 1.1,
    catalog_name: str | None = None,
    catalog_mag: float | None = None,
) -> dict:
    src: dict[str, Any] = {
        "ra":         ra,
        "dec":        dec,
        "flux":       flux,
        "fwhm":       fwhm,
        "elongation": elongation,
    }
    if catalog_name is not None:
        src["catalog_name"] = catalog_name
        src["catalog_mag"]  = catalog_mag
    return src


def _make_sources(
    n: int = 5,
    ra_center: float = 202.47,
    dec_center: float = 47.20,
    flux: float = 50000.0,
    fwhm: float = 3.0,
    catalog_name: str | None = None,
    catalog_mag: float | None = None,
) -> list[dict]:
    """
    Build a list of *n* sources spread ±0.05 deg around (ra_center, dec_center).
    """
    offsets = np.linspace(-0.05, 0.05, n)
    return [
        _make_source(
            ra=ra_center + float(offsets[i]),
            dec=dec_center + float(offsets[i]),
            flux=flux,
            fwhm=fwhm,
            catalog_name=catalog_name,
            catalog_mag=catalog_mag,
        )
        for i in range(n)
    ]


def _make_gaia_sources(
    n: int = 5,
    ra_center: float = 202.47,
    dec_center: float = 47.20,
    flux: float = 50000.0,
    fwhm: float = 3.0,
    catalog_mag: float = 14.0,
) -> list[dict]:
    """Sources pre-labelled as Gaia DR3 for calibration tests."""
    return _make_sources(
        n=n,
        ra_center=ra_center,
        dec_center=dec_center,
        flux=flux,
        fwhm=fwhm,
        catalog_name="Gaia DR3",
        catalog_mag=catalog_mag,
    )


# ---------------------------------------------------------------------------
# Shared patch context manager
# ---------------------------------------------------------------------------

@contextmanager
def _patch_photometry(
    wcs: AstropyWCS | None = None,
    hdul: _FakeHDUL | None = None,
    sky_median: float = 800.0,
    sky_sigma: float  = 20.0,
    aperture_sum: float = 60000.0,
    annulus_sky_per_px: float = 50.0,
    fits_open_raises: type[Exception] | None = None,
    wcs_celestial: bool = True,
):
    """
    Patch every external dependency of photometry.py in one shot.

    Parameters
    ----------
    wcs:
        WCS returned by the mocked WCS() constructor.
    hdul:
        FITS HDU list returned by fits.open().
    sky_median, sky_sigma:
        Values returned by sigma_clipped_stats mock.
    aperture_sum:
        ``aperture_sum`` column value in the aperture_photometry table mock.
    annulus_sky_per_px:
        ``ApertureStats.median`` — local sky per pixel from the annulus.
    fits_open_raises:
        If set, fits.open raises this exception type.
    wcs_celestial:
        Forwarded to _make_wcs when *wcs* is None.
    """
    if wcs is None:
        wcs = _make_wcs(celestial=wcs_celestial)
    if hdul is None:
        hdul = _make_hdul()

    # sigma_clipped_stats returns (mean, median, std)
    fake_stats = (sky_median, sky_median, sky_sigma)

    # aperture_photometry returns a QTable with an 'aperture_sum' column
    fake_phot_table = MagicMock()
    fake_phot_table.__getitem__ = lambda self, key: (
        np.array([aperture_sum]) if key == "aperture_sum" else MagicMock()
    )

    # ApertureStats.median is the per-pixel sky from the annulus
    fake_ann_stats = MagicMock()
    fake_ann_stats.median = annulus_sky_per_px

    def _fits_open(*args, **kwargs):
        if fits_open_raises is not None:
            raise fits_open_raises("mocked fits.open error")
        return hdul

    with (
        patch("modules.photometry.fits.open", side_effect=_fits_open),
        patch("modules.photometry.WCS", return_value=wcs),
        patch("modules.photometry.sigma_clipped_stats", return_value=fake_stats),
        patch("modules.photometry.aperture_photometry", return_value=fake_phot_table),
        patch("modules.photometry.ApertureStats", return_value=fake_ann_stats),
    ):
        yield


# ---------------------------------------------------------------------------
# Test 1 — Output structure: all keys always present
# ---------------------------------------------------------------------------

class TestOutputStructure:
    async def test_all_phot_keys_present(self):
        """Every returned source must carry all nine photometry keys."""
        srcs = _make_sources(n=3)
        with _patch_photometry():
            result = await photometry.measure(_FITS_PATH, srcs)

        required = {
            "flux_aperture", "flux_err", "mag_instrumental",
            "mag_calibrated", "mag_err", "calibrated",
            "edge_flag", "zero_point", "zero_point_err",
        }
        for src in result:
            assert required.issubset(src.keys()), (
                f"Missing keys: {required - src.keys()}"
            )

    async def test_original_keys_preserved(self):
        """Input fields (ra, dec, flux, fwhm, elongation) must be preserved."""
        srcs = _make_sources(n=2)
        with _patch_photometry():
            result = await photometry.measure(_FITS_PATH, srcs)

        for out, inp in zip(result, srcs):
            for key in ("ra", "dec", "flux", "fwhm", "elongation"):
                assert out[key] == inp[key]

    async def test_source_count_unchanged(self):
        """Output list must contain the same number of entries as input."""
        n = 7
        srcs = _make_sources(n=n)
        with _patch_photometry():
            result = await photometry.measure(_FITS_PATH, srcs)

        assert len(result) == n

    async def test_empty_sources_returns_empty_list(self):
        """Empty input must return an empty list, not an error."""
        with _patch_photometry():
            result = await photometry.measure(_FITS_PATH, [])

        assert result == []


# ---------------------------------------------------------------------------
# Test 2 — Aperture photometry values
# ---------------------------------------------------------------------------

class TestAperturePhotometry:
    async def test_flux_aperture_is_net_flux(self):
        """
        net_flux = aperture_sum - sky_per_px * ap_area.
        With annulus_sky_per_px=50 and ap_area derived from r=2*fwhm_px,
        the net flux must be less than aperture_sum but > 0.
        """
        srcs = _make_sources(n=1, flux=50000.0, fwhm=3.0)
        with _patch_photometry(aperture_sum=60000.0, annulus_sky_per_px=50.0):
            result = await photometry.measure(_FITS_PATH, srcs)

        assert result[0]["flux_aperture"] is not None
        assert 0 < result[0]["flux_aperture"] < 60000.0

    async def test_flux_err_is_finite_and_positive(self):
        srcs = _make_sources(n=3)
        with _patch_photometry():
            result = await photometry.measure(_FITS_PATH, srcs)

        for src in result:
            if src["flux_aperture"] is not None and src["flux_aperture"] > 0:
                assert src["flux_err"] is not None
                assert math.isfinite(src["flux_err"])
                assert src["flux_err"] > 0.0

    async def test_mag_instrumental_negative_log_relation(self):
        """
        mag_instrumental = -2.5 * log10(flux_aperture).
        For large flux, magnitude should be small (bright).
        """
        srcs = _make_sources(n=1, fwhm=3.0)
        with _patch_photometry(aperture_sum=100000.0, annulus_sky_per_px=0.0):
            result = await photometry.measure(_FITS_PATH, srcs)

        mag = result[0]["mag_instrumental"]
        assert mag is not None
        assert math.isfinite(mag)
        assert mag < 0.0  # log10(100000) ≈ 5, -2.5*5 = -12.5

    async def test_mag_err_formula(self):
        """
        mag_err = 1.0857 * flux_err / flux_aperture.
        Verify the ratio holds for any source with valid measurements.
        """
        srcs = _make_sources(n=3)
        with _patch_photometry(aperture_sum=80000.0, annulus_sky_per_px=10.0):
            result = await photometry.measure(_FITS_PATH, srcs)

        for src in result:
            if (
                src["mag_err"] is not None
                and src["flux_err"] is not None
                and src["flux_aperture"] is not None
                and src["flux_aperture"] > 0
            ):
                expected = 1.0857 * src["flux_err"] / src["flux_aperture"]
                assert abs(src["mag_err"] - expected) < 1e-9


# ---------------------------------------------------------------------------
# Test 3 — Edge flag
# ---------------------------------------------------------------------------

class TestEdgeFlag:
    async def test_central_source_not_edge_flagged(self):
        """Source at image centre must have edge_flag == False."""
        # WCS centred at (202.47, 47.20); pixel centre should map to ~centre
        wcs = _make_wcs(ra=202.47, dec=47.20)
        srcs = [_make_source(ra=202.47, dec=47.20)]
        with _patch_photometry(wcs=wcs):
            result = await photometry.measure(_FITS_PATH, srcs)

        assert result[0]["edge_flag"] is False

    async def test_source_near_edge_is_flagged(self):
        """
        A source at pixel (5, 5) — within 10 px of the left and bottom border —
        must have edge_flag == True.

        We map pixel (5, 5) back to sky coords using the known WCS, then feed
        those sky coords as the source position.
        """
        wcs = _make_wcs(ra=202.47, dec=47.20, scale_deg=0.000278)
        sky = wcs.all_pix2world([[5.0, 5.0]], 0)
        ra_edge, dec_edge = float(sky[0][0]), float(sky[0][1])

        srcs = [_make_source(ra=ra_edge, dec=dec_edge)]
        with _patch_photometry(wcs=wcs):
            result = await photometry.measure(_FITS_PATH, srcs)

        assert result[0]["edge_flag"] is True


# ---------------------------------------------------------------------------
# Test 4 — Out-of-bounds sources
# ---------------------------------------------------------------------------

class TestOutOfBoundsSources:
    async def test_out_of_bounds_source_has_null_photometry(self):
        """
        A source whose (RA, Dec) maps to a pixel outside the image must have
        all photometry fields set to None (not crash).
        """
        wcs = _make_wcs(ra=202.47, dec=47.20, scale_deg=0.000278)
        # Sky coords that project far outside a 1024×1024 image
        sky = wcs.all_pix2world([[5000.0, 5000.0]], 0)
        ra_oob, dec_oob = float(sky[0][0]), float(sky[0][1])

        srcs = [_make_source(ra=ra_oob, dec=dec_oob)]
        with _patch_photometry(wcs=wcs):
            result = await photometry.measure(_FITS_PATH, srcs)

        assert len(result) == 1
        assert result[0]["flux_aperture"]    is None
        assert result[0]["mag_instrumental"] is None
        assert result[0]["mag_calibrated"]   is None

    async def test_in_bounds_sources_unaffected_by_oob_source(self):
        """
        A mix of in-bounds and out-of-bounds sources: only the in-bounds ones
        should have flux_aperture populated.
        """
        wcs = _make_wcs(ra=202.47, dec=47.20, scale_deg=0.000278)
        sky_oob = wcs.all_pix2world([[5000.0, 5000.0]], 0)
        ra_oob, dec_oob = float(sky_oob[0][0]), float(sky_oob[0][1])

        srcs = [
            _make_source(ra=202.47, dec=47.20),       # centre — in bounds
            _make_source(ra=ra_oob, dec=dec_oob),      # far outside
        ]
        with _patch_photometry(wcs=wcs):
            result = await photometry.measure(_FITS_PATH, srcs)

        assert result[0]["flux_aperture"] is not None
        assert result[1]["flux_aperture"] is None


# ---------------------------------------------------------------------------
# Test 5 — Negative / zero flux handling
# ---------------------------------------------------------------------------

class TestNegativeFlux:
    async def test_negative_net_flux_gives_null_magnitude(self):
        """
        When the sky background exceeds the aperture sum, net flux <= 0.
        mag_instrumental and mag_calibrated must be None (not math domain error).
        """
        # annulus_sky_per_px * ap_area >> aperture_sum → net flux << 0
        srcs = _make_sources(n=2, fwhm=3.0)
        with _patch_photometry(aperture_sum=100.0, annulus_sky_per_px=10000.0):
            result = await photometry.measure(_FITS_PATH, srcs)

        for src in result:
            assert src["mag_instrumental"] is None
            assert src["mag_calibrated"]   is None
            assert src["mag_err"]          is None

    async def test_negative_flux_calibrated_flag_false(self):
        srcs = _make_sources(n=2, fwhm=3.0)
        with _patch_photometry(aperture_sum=100.0, annulus_sky_per_px=10000.0):
            result = await photometry.measure(_FITS_PATH, srcs)

        for src in result:
            assert src["calibrated"] is False


# ---------------------------------------------------------------------------
# Test 6 — Differential photometry / zero-point
# ---------------------------------------------------------------------------

class TestZeroPoint:
    async def test_calibrated_true_with_enough_gaia_stars(self):
        """With >= 3 Gaia DR3 reference stars, calibrated must be True."""
        srcs = _make_gaia_sources(n=5, catalog_mag=14.0)
        with _patch_photometry(aperture_sum=80000.0, annulus_sky_per_px=10.0):
            result = await photometry.measure(_FITS_PATH, srcs)

        assert all(src["calibrated"] is True for src in result)

    async def test_mag_calibrated_equals_inst_plus_zp(self):
        """mag_calibrated = mag_instrumental + zero_point for each source."""
        srcs = _make_gaia_sources(n=5, catalog_mag=14.0)
        with _patch_photometry(aperture_sum=80000.0, annulus_sky_per_px=10.0):
            result = await photometry.measure(_FITS_PATH, srcs)

        for src in result:
            if src["mag_calibrated"] is not None and src["mag_instrumental"] is not None:
                expected = src["mag_instrumental"] + src["zero_point"]
                assert abs(src["mag_calibrated"] - expected) < 1e-9

    async def test_zero_point_same_for_all_sources(self):
        """The zero_point value must be identical across all output sources."""
        srcs = _make_gaia_sources(n=5, catalog_mag=14.0)
        with _patch_photometry(aperture_sum=80000.0, annulus_sky_per_px=10.0):
            result = await photometry.measure(_FITS_PATH, srcs)

        zps = [src["zero_point"] for src in result]
        assert len(set(zps)) == 1, "zero_point differs between sources"

    async def test_fewer_than_3_gaia_stars_uncalibrated(self):
        """With < 3 Gaia DR3 references, mag_calibrated must be None for all."""
        gaia_srcs  = _make_gaia_sources(n=2, catalog_mag=14.0)
        plain_srcs = _make_sources(n=3)
        srcs = gaia_srcs + plain_srcs

        with _patch_photometry(aperture_sum=80000.0, annulus_sky_per_px=10.0):
            result = await photometry.measure(_FITS_PATH, srcs)

        assert all(src["mag_calibrated"] is None for src in result)
        assert all(src["calibrated"] is False for src in result)

    async def test_zero_point_err_is_nonnegative(self):
        """MAD-based zero_point_err must be >= 0."""
        srcs = _make_gaia_sources(n=6, catalog_mag=14.0)
        with _patch_photometry(aperture_sum=80000.0, annulus_sky_per_px=10.0):
            result = await photometry.measure(_FITS_PATH, srcs)

        for src in result:
            if src["zero_point_err"] is not None:
                assert src["zero_point_err"] >= 0.0

    async def test_no_catalog_fields_means_uncalibrated(self):
        """Sources without catalog_name / catalog_mag must not be calibrated."""
        srcs = _make_sources(n=5)  # no catalog fields
        with _patch_photometry(aperture_sum=80000.0, annulus_sky_per_px=10.0):
            result = await photometry.measure(_FITS_PATH, srcs)

        assert all(src["calibrated"] is False for src in result)
        assert all(src["zero_point"] is None for src in result)


# ---------------------------------------------------------------------------
# Test 7 — Frame-level failure modes
# ---------------------------------------------------------------------------

class TestFrameLevelFailures:
    async def test_fits_open_failure_returns_null_sources(self):
        """When fits.open raises, all sources get null photometry — not an error."""
        srcs = _make_sources(n=3)
        with _patch_photometry(fits_open_raises=OSError):
            result = await photometry.measure(_FITS_PATH, srcs)

        assert len(result) == len(srcs)
        for src in result:
            assert src["flux_aperture"] is None
            assert src["mag_instrumental"] is None
            assert src["mag_calibrated"]   is None

    async def test_fits_open_failure_preserves_input_fields(self):
        """Even on FITS failure, original ra/dec/flux/fwhm values are kept."""
        srcs = _make_sources(n=2)
        with _patch_photometry(fits_open_raises=OSError):
            result = await photometry.measure(_FITS_PATH, srcs)

        for out, inp in zip(result, srcs):
            assert out["ra"]  == inp["ra"]
            assert out["dec"] == inp["dec"]

    async def test_invalid_wcs_returns_null_sources(self):
        """Non-celestial WCS must not raise — return nulls for all sources."""
        srcs = _make_sources(n=3)
        with _patch_photometry(wcs_celestial=False):
            result = await photometry.measure(_FITS_PATH, srcs)

        assert len(result) == len(srcs)
        for src in result:
            assert src["flux_aperture"] is None

    async def test_no_image_data_returns_null_sources(self):
        """HDU with data=None must not raise — return null sources."""
        srcs = _make_sources(n=2)
        null_hdul = _FakeHDUL(_FakeHDU(data=None, header=dict(_BASE_HEADER)))  # type: ignore[arg-type]
        with _patch_photometry(hdul=null_hdul):
            result = await photometry.measure(_FITS_PATH, srcs)

        assert len(result) == len(srcs)
        for src in result:
            assert src["flux_aperture"] is None


# ---------------------------------------------------------------------------
# Test 8 — calibrated flag semantics
# ---------------------------------------------------------------------------

class TestCalibratedFlag:
    async def test_calibrated_is_always_bool(self):
        """calibrated key must always be a Python bool, never None."""
        srcs = _make_sources(n=4)
        with _patch_photometry():
            result = await photometry.measure(_FITS_PATH, srcs)

        for src in result:
            assert isinstance(src["calibrated"], bool)

    async def test_edge_flag_is_always_bool(self):
        """edge_flag key must always be a Python bool."""
        srcs = _make_sources(n=4)
        with _patch_photometry():
            result = await photometry.measure(_FITS_PATH, srcs)

        for src in result:
            assert isinstance(src["edge_flag"], bool)


# ---------------------------------------------------------------------------
# Test 9 — Fwhm fallback when fwhm is missing or zero
# ---------------------------------------------------------------------------

class TestFwhmFallback:
    async def test_missing_fwhm_does_not_raise(self):
        """Source dict without a 'fwhm' key must not crash the function."""
        srcs = [
            {"ra": 202.47, "dec": 47.20, "flux": 50000.0, "elongation": 1.1}
        ]
        with _patch_photometry():
            result = await photometry.measure(_FITS_PATH, srcs)

        assert len(result) == 1
        # Should fall back to default and still populate flux_aperture
        # (or at least not raise).  We just assert no exception occurred.

    async def test_zero_fwhm_does_not_raise(self):
        """Source with fwhm=0 triggers fallback to default 3-pixel FWHM."""
        srcs = [_make_source(fwhm=0.0)]
        with _patch_photometry():
            result = await photometry.measure(_FITS_PATH, srcs)

        assert len(result) == 1
