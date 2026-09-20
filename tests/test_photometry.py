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

import config
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
    saturated: bool = False,
) -> dict:
    src: dict[str, Any] = {
        "ra":         ra,
        "dec":        dec,
        "flux":       flux,
        "fwhm":       fwhm,
        "elongation": elongation,
        "saturated":  saturated,
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
# Test 2.5 — Sensor gain in the flux-error Poisson term (audit finding C7)
# ---------------------------------------------------------------------------

class TestGainInFluxError:
    """
    flux_err = sqrt(|net_flux| / gain + ap_area * sky_sigma**2).

    The aperture sum is in ADU, but photon shot noise is Poissonian in
    ELECTRONS: N_e = net_flux * gain, whose variance converts back to ADU as
    N_e / gain**2 = net_flux / gain. Treating net_flux itself as the variance
    silently assumed exactly 1 e-/ADU, which real cameras almost never are,
    so every SNR in the frame was biased in one direction or the other —
    above 1 e-/ADU the error was overstated and forced photometry dropped
    real faint recoveries against FORCED_PHOTOMETRY_MIN_SNR (audit finding
    C7).

    sky_sigma is deliberately NOT gain-converted: it is the empirical
    per-pixel background scatter measured off this frame's own ADU values,
    so it already carries read noise and sky shot noise together in ADU.
    Every test here therefore uses sky_sigma=0.0, isolating the Poisson term.
    """

    @staticmethod
    async def _flux_err(header: dict[str, Any] | None = None, **kwargs) -> tuple[float, float]:
        srcs = _make_sources(n=1, fwhm=3.0)
        with _patch_photometry(
            hdul=_make_hdul(header=header),
            aperture_sum=100000.0,
            annulus_sky_per_px=0.0,
            sky_sigma=0.0,
        ):
            result = await photometry.measure(_FITS_PATH, srcs, **kwargs)
        return float(result[0]["flux_aperture"]), float(result[0]["flux_err"])

    async def test_missing_gain_header_assumes_unity(self):
        """No EGAIN/GAIN anywhere — the pre-existing implicit assumption,
        preserved so an unheadered frame behaves exactly as before."""
        net_flux, flux_err = await self._flux_err()
        assert flux_err == pytest.approx(math.sqrt(net_flux))

    async def test_egain_header_divides_the_poisson_term(self):
        header = dict(_BASE_HEADER, EGAIN=4.0)
        net_flux, flux_err = await self._flux_err(header)
        assert flux_err == pytest.approx(math.sqrt(net_flux / 4.0))

    async def test_egain_is_preferred_over_gain(self):
        """
        On most CMOS cameras EGAIN is the true e-/ADU conversion while GAIN
        holds the camera's own gain SETTING in arbitrary vendor units. This
        is the one place in the pipeline where that preference matters.
        """
        header = dict(_BASE_HEADER, EGAIN=2.0, GAIN=120.0)
        net_flux, flux_err = await self._flux_err(header)
        assert flux_err == pytest.approx(math.sqrt(net_flux / 2.0))

    async def test_gain_header_is_used_when_plausible(self):
        header = dict(_BASE_HEADER, GAIN=0.5)
        net_flux, flux_err = await self._flux_err(header)
        assert flux_err == pytest.approx(math.sqrt(net_flux / 0.5))

    async def test_implausible_gain_falls_back_to_unity(self):
        """
        A ZWO-style GAIN=120 is a gain setting, not a conversion factor.
        Dividing by it would understate the error by an order of magnitude —
        far more wrong than the 1.0 assumption it replaced — so it is
        rejected outright.
        """
        header = dict(_BASE_HEADER, GAIN=120.0)
        net_flux, flux_err = await self._flux_err(header)
        assert flux_err == pytest.approx(math.sqrt(net_flux))

    async def test_non_numeric_gain_falls_back_to_unity(self):
        header = dict(_BASE_HEADER, GAIN="High")
        net_flux, flux_err = await self._flux_err(header)
        assert flux_err == pytest.approx(math.sqrt(net_flux))

    async def test_config_override_beats_the_header(self, monkeypatch):
        monkeypatch.setattr(config, "PHOTOMETRY_GAIN_E_PER_ADU", 3.0)
        header = dict(_BASE_HEADER, EGAIN=1.0)
        net_flux, flux_err = await self._flux_err(header)
        assert flux_err == pytest.approx(math.sqrt(net_flux / 3.0))

    async def test_caller_argument_beats_everything(self, monkeypatch):
        monkeypatch.setattr(config, "PHOTOMETRY_GAIN_E_PER_ADU", 3.0)
        header = dict(_BASE_HEADER, EGAIN=1.0)
        net_flux, flux_err = await self._flux_err(header, gain=5.0)
        assert flux_err == pytest.approx(math.sqrt(net_flux / 5.0))

    async def test_higher_gain_raises_the_reported_snr(self):
        """
        The consequence that matters downstream: at gain > 1 the old formula
        overstated flux_err, so snr came out understated and
        forced_photometry.py failed real faint objects against
        FORCED_PHOTOMETRY_MIN_SNR.
        """
        srcs = _make_sources(n=1, fwhm=3.0)

        async def _snr(header):
            with _patch_photometry(
                hdul=_make_hdul(header=header),
                aperture_sum=100000.0,
                annulus_sky_per_px=0.0,
                sky_sigma=0.0,
            ):
                result = await photometry.measure(_FITS_PATH, list(srcs))
            return result[0]["snr"]

        assert await _snr(dict(_BASE_HEADER, EGAIN=4.0)) > await _snr(dict(_BASE_HEADER))


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
# Colour term in the zero point — audit 2026-08-18, finding H5
# ---------------------------------------------------------------------------

def _ref(delta: float, color: float | None, inst: float = -10.0) -> dict:
    """One Gaia reference star with a chosen (catalog_mag - mag_instrumental)."""
    return {
        "catalog_name": "Gaia DR3",
        "catalog_mag": inst + delta,
        "mag_instrumental": inst,
        "_catalog_color": color,
    }


class TestColorTerm:
    """
    A star's instrumental magnitude in R/B/V differs from its Gaia broadband
    G magnitude by an amount that depends on the star's own colour, not by a
    constant. Fitting one median offset leaves a systematic bias in every
    mag_calibrated that drifts with the reference set's colour mix — enough
    to shift many stars in one epoch together past DELTA_MAG_ALERT.
    """

    def _colored_refs(self, k: float = 0.4, zp: float = 24.0, n: int = 20) -> list[dict]:
        """References following delta = zp + k * (color - 1.0) exactly."""
        colors = [0.2 + 0.1 * i for i in range(n)]
        return [_ref(zp + k * (c - 1.0), c) for c in colors]

    def test_slope_is_recovered(self):
        sol = photometry._compute_zero_point(self._colored_refs(k=0.4, zp=24.0))

        assert sol.color_term == pytest.approx(0.4, abs=0.02)
        assert sol.color_ref == pytest.approx(1.15, abs=0.1)
        # The zero point is reported AT the reference colour, so it equals the
        # line's value there rather than the mean of a tilted set.
        assert sol.zero_point == pytest.approx(24.0 + 0.4 * (sol.color_ref - 1.0), abs=0.02)

    def test_an_outlier_does_not_tilt_the_slope(self):
        refs = self._colored_refs(k=0.4, zp=24.0)
        refs.append(_ref(24.0 + 5.0, 2.2))  # one blended/variable reference
        sol = photometry._compute_zero_point(refs)

        assert sol.color_term == pytest.approx(0.4, abs=0.05)

    def test_references_without_colors_behave_exactly_as_before(self):
        """The pre-existing path: no colour anywhere, plain median offset."""
        refs = [_ref(24.0, None) for _ in range(20)]
        sol = photometry._compute_zero_point(refs)

        assert sol.color_term == 0.0
        assert sol.color_ref is None
        assert sol.zero_point == pytest.approx(24.0)

    def test_a_narrow_color_span_is_not_fitted(self):
        """
        A slope fitted over a field whose stars all share one colour is
        unconstrained — extrapolating it is worse than not correcting.
        """
        refs = [_ref(24.0, 1.0 + 0.001 * i) for i in range(20)]
        sol = photometry._compute_zero_point(refs)

        assert sol.color_term == 0.0

    def test_too_few_colored_references_are_not_fitted(self):
        refs = self._colored_refs(n=4) + [_ref(24.0, None) for _ in range(10)]
        sol = photometry._compute_zero_point(refs)

        assert sol.color_term == 0.0

    def test_an_implausible_slope_is_discarded(self):
        sol = photometry._compute_zero_point(self._colored_refs(k=5.0, zp=24.0))

        assert sol.color_term == 0.0

    def test_disabled_by_config(self, monkeypatch):
        monkeypatch.setattr(config, "PHOTOMETRY_COLOR_TERM_ENABLED", False)
        sol = photometry._compute_zero_point(self._colored_refs(k=0.4))

        assert sol.color_term == 0.0

    async def test_a_colored_source_gets_the_term_applied(self):
        """
        End to end through measure(): two Gaia stars of different colours, on
        a reference set whose fitted slope is non-zero, must end up with
        different mag_calibrated offsets from their (identical) instrumental
        magnitudes.
        """
        srcs = _make_gaia_sources(n=20, catalog_mag=14.0)
        # Give the set a real colour-magnitude relation: delta grows with colour.
        for i, src in enumerate(srcs):
            color = 0.2 + 0.1 * i
            src["_catalog_color"] = color
            src["catalog_mag"] = 14.0 + 0.4 * color

        with _patch_photometry(aperture_sum=80000.0, annulus_sky_per_px=10.0):
            result = await photometry.measure(_FITS_PATH, srcs)

        assert result[0]["calibrated"] is True
        offsets = {
            round(src["mag_calibrated"] - src["mag_instrumental"], 6)
            for src in result
        }
        assert len(offsets) > 1, "colour term was not applied per source"

    async def test_a_colorless_source_keeps_the_bare_zero_point_and_a_wider_error(self):
        """
        An uncatalogued transient — the case that matters — has no colour to
        transform with, so it uses the zero point at the reference colour and
        carries the colour term's own reach in its mag_err instead.
        """
        srcs = _make_gaia_sources(n=20, catalog_mag=14.0)
        for i, src in enumerate(srcs):
            color = 0.2 + 0.1 * i
            src["_catalog_color"] = color
            src["catalog_mag"] = 14.0 + 0.4 * color
        plain = _make_sources(n=1)
        plain[0]["ra"] = srcs[0]["ra"]
        plain[0]["dec"] = srcs[0]["dec"]

        with _patch_photometry(aperture_sum=80000.0, annulus_sky_per_px=10.0):
            with_term = await photometry.measure(_FITS_PATH, srcs + plain)
            monochrome = [dict(s) for s in srcs]
            for s in monochrome:
                s["_catalog_color"] = None
                s["catalog_mag"] = 14.0
            without_term = await photometry.measure(_FITS_PATH, monochrome + [dict(plain[0])])

        target_with = with_term[-1]
        target_without = without_term[-1]

        assert target_with["mag_calibrated"] == pytest.approx(
            target_with["mag_instrumental"] + target_with["zero_point"]
        )
        assert target_with["mag_err"] > target_without["mag_err"]


# ---------------------------------------------------------------------------
# Reference-star screening and small-sample scatter — audit 2026-08-18, H6
# ---------------------------------------------------------------------------

class TestReferenceScreening:
    """
    Gaia publishes its own opinion of each star's reliability and none of it
    was consulted: a catalogued variable, a duplicated_source, or a star with
    a poor astrometric fit (high RUWE — usually an unresolved binary or a
    blend) could silently anchor the frame's whole photometric calibration.
    """

    def _flagged(self, delta: float, **flags) -> dict:
        src = _ref(delta, None)
        src["_catalog_flags"] = {"ruwe": None, "variable": None, "duplicated": None}
        src["_catalog_flags"].update(flags)
        return src

    def test_a_variable_reference_is_excluded(self):
        good = [self._flagged(24.0) for _ in range(5)]
        bad = self._flagged(30.0, variable=True)
        sol = photometry._compute_zero_point(good + [bad])

        assert sol.zero_point == pytest.approx(24.0)

    def test_a_duplicated_reference_is_excluded(self):
        good = [self._flagged(24.0) for _ in range(5)]
        bad = self._flagged(30.0, duplicated=True)
        sol = photometry._compute_zero_point(good + [bad])

        assert sol.zero_point == pytest.approx(24.0)

    def test_a_high_ruwe_reference_is_excluded(self):
        good = [self._flagged(24.0, ruwe=1.0) for _ in range(5)]
        bad = self._flagged(30.0, ruwe=5.0)
        sol = photometry._compute_zero_point(good + [bad])

        assert sol.zero_point == pytest.approx(24.0)

    def test_a_reference_with_no_flags_at_all_is_kept(self):
        """
        An astroquery version returning a narrower column set must keep
        calibrating exactly as before, not lose every reference.
        """
        refs = [_ref(24.0, None) for _ in range(4)]
        sol = photometry._compute_zero_point(refs)

        assert sol.zero_point == pytest.approx(24.0)

    def test_screening_below_three_falls_back_to_the_unscreened_set(self):
        """A worse zero point beats losing calibration for the whole frame."""
        refs = [self._flagged(24.0, variable=True) for _ in range(4)]
        sol = photometry._compute_zero_point(refs)

        assert sol.zero_point == pytest.approx(24.0)


class TestSmallSampleScatter:
    """
    The plain 1.4826 x MAD collapsed to exactly zero for the minimum n=3 "two
    good references plus one outlier" set — reporting a perfect zero_point_err
    at the moment the calibration is least trustworthy.
    """

    def test_three_references_with_an_outlier_do_not_report_zero_error(self):
        refs = [_ref(24.0, None), _ref(24.0, None), _ref(25.0, None)]
        sol = photometry._compute_zero_point(refs)

        assert sol.zero_point == pytest.approx(24.0)
        assert sol.zero_point_err > 0.0

    def test_identical_references_still_report_zero_error(self):
        """No scatter genuinely means no scatter — the floor must not invent one."""
        refs = [_ref(24.0, None) for _ in range(4)]
        sol = photometry._compute_zero_point(refs)

        assert sol.zero_point_err == pytest.approx(0.0)

    def test_the_small_sample_correction_fades_with_n(self):
        """
        The same relative spread must not be reported as a larger scatter for
        a large reference set than the asymptotic MAD would give.
        """
        spread = [-1.0, -0.5, 0.0, 0.5, 1.0]
        many = [_ref(24.0 + d, None) for d in spread * 12]
        sol = photometry._compute_zero_point(many)

        plain_mad = 0.5  # median |x - median| of the spread above
        assert sol.zero_point_err == pytest.approx(1.4826 * plain_mad, rel=0.01)


# ---------------------------------------------------------------------------
# Test 6.4 — skip_calibration (narrowband filters)
# ---------------------------------------------------------------------------

class TestSkipCalibration:
    async def test_skip_calibration_true_uncalibrated_even_with_enough_gaia_stars(self):
        """
        skip_calibration=True must leave every source uncalibrated even when
        there would otherwise be >= 3 valid Gaia DR3 references — a
        narrowband bandpass makes the zero-point itself untrustworthy
        regardless of how many Gaia stars happen to match (see measure()'s
        docstring / CLAUDE.md's "Filters — real astronomy context").
        """
        srcs = _make_gaia_sources(n=5, catalog_mag=14.0)
        with _patch_photometry(aperture_sum=80000.0, annulus_sky_per_px=10.0):
            result = await photometry.measure(_FITS_PATH, srcs, skip_calibration=True)

        assert all(src["calibrated"] is False for src in result)
        assert all(src["mag_calibrated"] is None for src in result)
        assert all(src["zero_point"] is None for src in result)
        assert all(src["zero_point_err"] is None for src in result)

    async def test_skip_calibration_still_measures_instrumental_flux(self):
        """
        skip_calibration only skips the Gaia zero-point step — aperture
        photometry (flux_aperture, mag_instrumental) must still run normally.
        """
        srcs = _make_gaia_sources(n=5, catalog_mag=14.0)
        with _patch_photometry(aperture_sum=80000.0, annulus_sky_per_px=10.0):
            result = await photometry.measure(_FITS_PATH, srcs, skip_calibration=True)

        assert all(src["flux_aperture"] is not None for src in result)
        assert all(src["mag_instrumental"] is not None for src in result)

    async def test_skip_calibration_false_is_default_and_unaffected(self):
        """The default (skip_calibration omitted) must behave exactly as before."""
        srcs = _make_gaia_sources(n=5, catalog_mag=14.0)
        with _patch_photometry(aperture_sum=80000.0, annulus_sky_per_px=10.0):
            result = await photometry.measure(_FITS_PATH, srcs)

        assert all(src["calibrated"] is True for src in result)


# ---------------------------------------------------------------------------
# Test 6.5 — Saturated sources (docs/ISSUES.md #2)
# ---------------------------------------------------------------------------

class TestSaturatedSources:
    async def test_saturated_source_gets_null_photometry(self):
        """
        A source flagged saturated=True must never be measured — aperture
        flux on a clipped PSF core is not physically meaningful and was the
        root cause of extreme (e.g. -14) magnitudes reaching the API.
        """
        srcs = [_make_source(saturated=True)]
        with _patch_photometry(aperture_sum=1_000_000.0, annulus_sky_per_px=0.0):
            result = await photometry.measure(_FITS_PATH, srcs)

        assert result[0]["flux_aperture"]    is None
        assert result[0]["mag_instrumental"] is None
        assert result[0]["mag_calibrated"]   is None
        assert result[0]["calibrated"]       is False

    async def test_saturated_flag_preserved_in_output(self):
        srcs = [_make_source(saturated=True)]
        with _patch_photometry():
            result = await photometry.measure(_FITS_PATH, srcs)

        assert result[0]["saturated"] is True

    async def test_non_saturated_source_unaffected(self):
        """A mix of saturated and normal sources: only the normal one gets measured."""
        srcs = [
            _make_source(saturated=True),
            _make_source(saturated=False),
        ]
        with _patch_photometry(aperture_sum=60000.0, annulus_sky_per_px=50.0):
            result = await photometry.measure(_FITS_PATH, srcs)

        assert result[0]["flux_aperture"] is None
        assert result[1]["flux_aperture"] is not None

    async def test_saturated_gaia_star_excluded_from_zero_point(self):
        """
        A saturated source must never be used as a Gaia DR3 zero-point
        reference, even if catalog-matched — its aperture flux is garbage.
        With one saturated + two normal Gaia stars, fewer than 3 valid
        references remain, so calibration must fail.
        """
        srcs = _make_gaia_sources(n=2, catalog_mag=14.0)
        srcs.append(_make_source(catalog_name="Gaia DR3", catalog_mag=14.0, saturated=True))

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
