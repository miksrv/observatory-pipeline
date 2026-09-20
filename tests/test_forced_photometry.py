"""
tests/test_forced_photometry.py — Unit tests for modules/forced_photometry.py

All FITS I/O is mocked (modules.forced_photometry.fits.open returns a fake
HDU wrapping a real numpy array); WCS is a real astropy TAN projection so
pixel<->sky round-trips are exact. Aperture photometry itself
(photutils.aperture) runs for real against synthetic pixel data — no
network access anywhere.

asyncio_mode = auto is set in pytest.ini, so async tests need no decorator.
"""

from __future__ import annotations

import math
from typing import Any
from unittest.mock import patch

import numpy as np
import pytest
from astropy.wcs import WCS as AstropyWCS

import config
from modules import forced_photometry as fp


# ---------------------------------------------------------------------------
# Synthetic WCS + image helpers
# ---------------------------------------------------------------------------

_IMAGE_SHAPE = (320, 320)  # (naxis2, naxis1)
_FITS_PATH = "/fake/fits/archive/M51/frame_test.fits"


def _make_wcs(ra: float = 200.0, dec: float = 10.0, scale_deg: float = 1.0 / 3600.0) -> AstropyWCS:
    w = AstropyWCS(naxis=2)
    w.wcs.ctype = ["RA---TAN", "DEC--TAN"]
    w.wcs.crpix = [160.0, 160.0]
    w.wcs.crval = [ra, dec]
    w.wcs.cdelt = [-scale_deg, scale_deg]
    w.wcs.set()
    return w


def _pix_to_world(wcs: AstropyWCS, x: float, y: float) -> tuple[float, float]:
    ra, dec = wcs.all_pix2world([[x, y]], 0)[0]
    return float(ra), float(dec)


class _FakeHDU:
    def __init__(self, data: np.ndarray, header: dict | None = None) -> None:
        self.data = data
        # run() reads EGAIN/GAIN off this for the Poisson term of the flux
        # error (_resolve_gain()); an empty header is the "no usable gain
        # in this frame, assume 1.0 e-/ADU" case.
        self.header = header if header is not None else {}


class _FakeHDUL:
    def __init__(self, hdu: _FakeHDU) -> None:
        self._hdu = hdu

    def __enter__(self) -> "_FakeHDUL":
        return self

    def __exit__(self, *args: Any) -> bool:
        return False

    def __getitem__(self, idx: int) -> _FakeHDU:
        return self._hdu


def _make_image() -> np.ndarray:
    """
    A background with realistic noise (so sigma_clipped_stats reports a
    nonzero sky_sigma, making the SNR check on a real star meaningful),
    plus:
      - a bright Gaussian "star" blob at (100, 100) — a clean detection
      - a second bright blob at (150, 100) — used for the "already matched,
        must be skipped" test
      - a third bright blob at (100, 150) — used for the MPC precovery test
      - a saturated flat spike at (200, 200)
      - (100, 200) is left as pure background — the "genuine non-detection" case
    """
    rng = np.random.default_rng(42)
    image = 1000.0 + rng.normal(0.0, 5.0, size=_IMAGE_SHAPE)

    def _add_star(cx: float, cy: float, amplitude: float, sigma: float = 2.0) -> None:
        y0, y1 = int(cy) - 10, int(cy) + 11
        x0, x1 = int(cx) - 10, int(cx) + 11
        yy, xx = np.mgrid[y0:y1, x0:x1]
        image[y0:y1, x0:x1] += amplitude * np.exp(
            -((xx - cx) ** 2 + (yy - cy) ** 2) / (2.0 * sigma ** 2)
        )

    _add_star(100, 100, amplitude=3000.0)
    _add_star(150, 100, amplitude=3000.0)
    _add_star(100, 150, amplitude=3000.0)
    image[195:206, 195:206] = 65000.0  # well above default SATURATION_ADU=60000
    return image


@pytest.fixture
def scene(monkeypatch):
    """Patch fits.open to return the synthetic image; return (image, wcs)."""
    image = _make_image()
    wcs = _make_wcs()
    monkeypatch.setattr(
        "modules.forced_photometry.fits.open",
        lambda *a, **kw: _FakeHDUL(_FakeHDU(image)),
    )
    return image, wcs


def _gaia_star(wcs: AstropyWCS, px: float, py: float, source_id: str, mag: float = 15.0) -> dict:
    ra, dec = _pix_to_world(wcs, px, py)
    return {
        "ra": ra, "dec": dec, "source_id": source_id, "phot_g_mean_mag": mag,
        "pmra": None, "pmdec": None, "ref_epoch": 2016.0,
    }


def _mpc_object(wcs: AstropyWCS, px: float, py: float, designation: str) -> dict:
    ra, dec = _pix_to_world(wcs, px, py)
    return {"ra": ra, "dec": dec, "designation": designation, "object_type": "ASTEROID"}


# ---------------------------------------------------------------------------
# run() — orchestration
# ---------------------------------------------------------------------------


class TestRunGuards:
    async def test_returns_empty_when_disabled(self, scene, monkeypatch):
        _, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", False)
        gaia = [_gaia_star(wcs, 100, 100, "1")]

        result = await fp.run(
            _FITS_PATH, [], gaia_stars=gaia, mpc_objects=[], wcs=wcs,
            naxis1=320, naxis2=320, zero_point=None, zero_point_err=None, obs_time=None,
        )
        assert result == []

    async def test_returns_empty_when_wcs_is_none(self, scene, monkeypatch):
        _, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        gaia = [_gaia_star(wcs, 100, 100, "1")]

        result = await fp.run(
            _FITS_PATH, [], gaia_stars=gaia, mpc_objects=[], wcs=None,
            naxis1=320, naxis2=320, zero_point=None, zero_point_err=None, obs_time=None,
        )
        assert result == []

    async def test_returns_empty_when_no_catalog_lists(self, scene, monkeypatch):
        _, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)

        result = await fp.run(
            _FITS_PATH, [], gaia_stars=[], mpc_objects=[], wcs=wcs,
            naxis1=320, naxis2=320, zero_point=None, zero_point_err=None, obs_time=None,
        )
        assert result == []

    async def test_fits_open_failure_returns_empty(self, monkeypatch):
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        wcs = _make_wcs()
        monkeypatch.setattr(
            "modules.forced_photometry.fits.open",
            lambda *a, **kw: (_ for _ in ()).throw(OSError("disk error")),
        )
        gaia = [_gaia_star(wcs, 100, 100, "1")]

        result = await fp.run(
            _FITS_PATH, [], gaia_stars=gaia, mpc_objects=[], wcs=wcs,
            naxis1=320, naxis2=320, zero_point=None, zero_point_err=None, obs_time=None,
        )
        assert result == []


class TestRunRecovery:
    async def test_recovers_unmatched_bright_gaia_star(self, scene, monkeypatch):
        image, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MAG_LIMIT", 20.0)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MIN_SNR", 3.0)
        gaia = [_gaia_star(wcs, 100, 100, "gaia-1", mag=17.5)]

        result = await fp.run(
            _FITS_PATH, sources=[], gaia_stars=gaia, mpc_objects=[], wcs=wcs,
            naxis1=320, naxis2=320, zero_point=24.0, zero_point_err=0.05, obs_time=None,
            psf_fwhm_arcsec=None,
        )

        assert len(result) == 1
        rec = result[0]
        assert rec["catalog_name"] == "Gaia DR3"
        assert rec["catalog_id"] == "gaia-1"
        assert rec["catalog_mag"] == pytest.approx(17.5)
        assert rec["object_type"] == "STAR"
        assert rec["flux_aperture"] > 0
        assert rec["calibrated"] is True
        assert rec["mag_calibrated"] == pytest.approx(rec["mag_instrumental"] + 24.0)
        assert rec["saturated"] is False
        assert rec["_forced_photometry"] is True

    async def test_skips_a_blended_pair(self, scene, monkeypatch):
        """
        Audit 2026-08-18, finding M8: a fixed aperture is measured at a
        catalog position without ever asking what else is in it. Two stars
        within a couple of FWHM share most of their light, so the measurement
        is really the pair's combined flux, reported as one star's magnitude
        with nothing on the wire to say otherwise.
        """
        _, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MAG_LIMIT", 20.0)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MIN_SNR", 3.0)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_BLEND_FWHM", 2.0)

        # Two catalog stars two pixels apart, with the frame's FWHM given as
        # 3" at ~1"/px — comfortably inside the 2xFWHM blend radius.
        gaia = [
            _gaia_star(wcs, 100, 100, "blend-a", mag=17.5),
            _gaia_star(wcs, 102, 100, "blend-b", mag=17.6),
        ]

        result = await fp.run(
            _FITS_PATH, sources=[], gaia_stars=gaia, mpc_objects=[], wcs=wcs,
            naxis1=320, naxis2=320, zero_point=24.0, zero_point_err=0.05, obs_time=None,
            psf_fwhm_arcsec=3.0,
        )

        assert result == []

    async def test_an_isolated_star_is_unaffected_by_the_blend_check(self, scene, monkeypatch):
        _, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MAG_LIMIT", 20.0)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MIN_SNR", 3.0)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_BLEND_FWHM", 2.0)

        gaia = [
            _gaia_star(wcs, 100, 100, "isolated", mag=17.5),
            _gaia_star(wcs, 150, 100, "far-away", mag=17.6),
        ]
        sources = [{"ra": 0.0, "dec": 0.0, "catalog_name": "Gaia DR3", "catalog_id": "far-away"}]

        result = await fp.run(
            _FITS_PATH, sources=sources, gaia_stars=gaia, mpc_objects=[], wcs=wcs,
            naxis1=320, naxis2=320, zero_point=24.0, zero_point_err=0.05, obs_time=None,
            psf_fwhm_arcsec=3.0,
        )

        assert [r["catalog_id"] for r in result] == ["isolated"]

    async def test_the_blend_check_can_be_disabled(self, scene, monkeypatch):
        _, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MAG_LIMIT", 20.0)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MIN_SNR", 3.0)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_BLEND_FWHM", 0.0)

        gaia = [
            _gaia_star(wcs, 100, 100, "blend-a", mag=17.5),
            _gaia_star(wcs, 102, 100, "blend-b", mag=17.6),
        ]

        result = await fp.run(
            _FITS_PATH, sources=[], gaia_stars=gaia, mpc_objects=[], wcs=wcs,
            naxis1=320, naxis2=320, zero_point=24.0, zero_point_err=0.05, obs_time=None,
            psf_fwhm_arcsec=3.0,
        )

        assert len(result) == 2

    async def test_an_unknown_frame_fwhm_disables_the_blend_check(self, scene, monkeypatch):
        """Without a PSF width there is no scale to judge "close" against."""
        _, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MAG_LIMIT", 20.0)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MIN_SNR", 3.0)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_BLEND_FWHM", 2.0)

        gaia = [
            _gaia_star(wcs, 100, 100, "blend-a", mag=17.5),
            _gaia_star(wcs, 102, 100, "blend-b", mag=17.6),
        ]

        result = await fp.run(
            _FITS_PATH, sources=[], gaia_stars=gaia, mpc_objects=[], wcs=wcs,
            naxis1=320, naxis2=320, zero_point=24.0, zero_point_err=0.05, obs_time=None,
            psf_fwhm_arcsec=None,
        )

        assert len(result) == 2

    async def test_skips_star_already_matched_in_sources(self, scene, monkeypatch):
        _, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MAG_LIMIT", 20.0)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MIN_SNR", 3.0)
        # The (150, 100) star is already caught by blind detection + forward
        # matching — must NOT be force-measured again.
        gaia = [_gaia_star(wcs, 150, 100, "already-matched", mag=17.0)]
        sources = [{"ra": 0.0, "dec": 0.0, "catalog_name": "Gaia DR3", "catalog_id": "already-matched"}]

        result = await fp.run(
            _FITS_PATH, sources=sources, gaia_stars=gaia, mpc_objects=[], wcs=wcs,
            naxis1=320, naxis2=320, zero_point=24.0, zero_point_err=0.05, obs_time=None,
        )
        assert result == []

    async def test_skips_star_fainter_than_mag_limit(self, scene, monkeypatch):
        _, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MAG_LIMIT", 18.0)
        gaia = [_gaia_star(wcs, 100, 100, "too-faint", mag=19.5)]  # below the frame's own depth cutoff

        result = await fp.run(
            _FITS_PATH, sources=[], gaia_stars=gaia, mpc_objects=[], wcs=wcs,
            naxis1=320, naxis2=320, zero_point=24.0, zero_point_err=0.05, obs_time=None,
        )
        assert result == []

    async def test_genuine_non_detection_is_dropped_not_reported(self, scene, monkeypatch):
        """
        A catalog position with no real signal (pure background) must be
        silently dropped — never reported as an "upper limit" magnitude
        (see module docstring: the wire schema has no field for that).
        """
        _, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MAG_LIMIT", 20.0)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MIN_SNR", 3.0)
        gaia = [_gaia_star(wcs, 100, 200, "no-signal-here", mag=17.0)]  # pure background, no star

        result = await fp.run(
            _FITS_PATH, sources=[], gaia_stars=gaia, mpc_objects=[], wcs=wcs,
            naxis1=320, naxis2=320, zero_point=24.0, zero_point_err=0.05, obs_time=None,
        )
        assert result == []

    async def test_skips_saturated_position(self, scene, monkeypatch):
        _, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MAG_LIMIT", 20.0)
        gaia = [_gaia_star(wcs, 200, 200, "saturated-star", mag=8.0)]

        result = await fp.run(
            _FITS_PATH, sources=[], gaia_stars=gaia, mpc_objects=[], wcs=wcs,
            naxis1=320, naxis2=320, zero_point=24.0, zero_point_err=0.05, obs_time=None,
        )
        assert result == []

    async def test_skips_out_of_bounds_position(self, scene, monkeypatch):
        _, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        # Far outside the frame's footprint entirely.
        gaia = [{
            "ra": wcs.wcs.crval[0] + 30.0, "dec": wcs.wcs.crval[1] + 30.0,
            "source_id": "far-away", "phot_g_mean_mag": 15.0,
            "pmra": None, "pmdec": None, "ref_epoch": 2016.0,
        }]

        result = await fp.run(
            _FITS_PATH, sources=[], gaia_stars=gaia, mpc_objects=[], wcs=wcs,
            naxis1=320, naxis2=320, zero_point=24.0, zero_point_err=0.05, obs_time=None,
        )
        assert result == []

    async def test_recovers_mpc_precovery_candidate(self, scene, monkeypatch):
        _, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MIN_SNR", 3.0)
        mpc = [_mpc_object(wcs, 100, 150, "2014 RY1")]

        result = await fp.run(
            _FITS_PATH, sources=[], gaia_stars=[], mpc_objects=mpc, wcs=wcs,
            naxis1=320, naxis2=320, zero_point=24.0, zero_point_err=0.05, obs_time=None,
        )

        assert len(result) == 1
        rec = result[0]
        assert rec["catalog_name"] == "MPC"
        assert rec["catalog_id"] == "2014 RY1"
        assert rec["object_type"] == "ASTEROID"
        assert rec["catalog_mag"] is None

    async def test_skips_mpc_object_already_matched(self, scene, monkeypatch):
        _, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        mpc = [_mpc_object(wcs, 100, 150, "2014 RY1")]
        sources = [{"ra": 0.0, "dec": 0.0, "catalog_name": "MPC", "catalog_id": "2014 RY1"}]

        result = await fp.run(
            _FITS_PATH, sources=sources, gaia_stars=[], mpc_objects=mpc, wcs=wcs,
            naxis1=320, naxis2=320, zero_point=24.0, zero_point_err=0.05, obs_time=None,
        )
        assert result == []

    async def test_uncalibrated_frame_leaves_mag_calibrated_none(self, scene, monkeypatch):
        """Same convention as photometry.py: zero_point=None -> calibrated=False, mag_calibrated=None."""
        _, wcs = scene
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MAG_LIMIT", 20.0)
        gaia = [_gaia_star(wcs, 100, 100, "gaia-uncal", mag=17.5)]

        result = await fp.run(
            _FITS_PATH, sources=[], gaia_stars=gaia, mpc_objects=[], wcs=wcs,
            naxis1=320, naxis2=320, zero_point=None, zero_point_err=None, obs_time=None,
        )

        assert len(result) == 1
        assert result[0]["calibrated"] is False
        assert result[0]["mag_calibrated"] is None
        assert result[0]["mag_instrumental"] is not None


# ---------------------------------------------------------------------------
# _propagate_gaia_position — proper motion correction
# ---------------------------------------------------------------------------


class TestPropagateGaiaPosition:
    def test_no_correction_without_obs_jyear(self):
        star = {"ra": 10.0, "dec": 20.0, "pmra": 100.0, "pmdec": 100.0, "ref_epoch": 2016.0}
        ra, dec = fp._propagate_gaia_position(star, obs_jyear=None)
        assert (ra, dec) == (10.0, 20.0)

    def test_no_correction_without_pm(self):
        star = {"ra": 10.0, "dec": 20.0, "pmra": None, "pmdec": None, "ref_epoch": 2016.0}
        ra, dec = fp._propagate_gaia_position(star, obs_jyear=2026.0)
        assert (ra, dec) == (10.0, 20.0)

    def test_high_proper_motion_star_shifts_over_a_decade(self):
        # 1000 mas/yr in each axis over 10 years = 10000 mas = ~2.78" total per axis
        star = {"ra": 10.0, "dec": 0.0, "pmra": 1000.0, "pmdec": 1000.0, "ref_epoch": 2016.0}
        ra, dec = fp._propagate_gaia_position(star, obs_jyear=2026.0)

        expected_shift_deg = 1000.0 / 1000.0 / 3600.0 * 10.0  # 10000 mas -> deg
        assert dec == pytest.approx(0.0 + expected_shift_deg, abs=1e-9)
        # dec=0 -> cos(dec)=1, so RA shift equals the same magnitude here
        assert ra == pytest.approx(10.0 + expected_shift_deg, abs=1e-9)

    def test_pole_guard_does_not_raise(self):
        star = {"ra": 10.0, "dec": 89.9999999, "pmra": 500.0, "pmdec": 500.0, "ref_epoch": 2016.0}
        # Must not raise even where cos(dec) is tiny; falls back to uncorrected position.
        ra, dec = fp._propagate_gaia_position(star, obs_jyear=2026.0)
        assert math.isfinite(ra) and math.isfinite(dec)


# ---------------------------------------------------------------------------
# _measure_at_pixel — aperture photometry primitive
# ---------------------------------------------------------------------------


class TestMeasureAtPixel:
    def test_measures_positive_flux_on_synthetic_star(self):
        image = _make_image()
        data_sub = image - 1000.0
        result = fp._measure_at_pixel(data_sub, image, 100.0, 100.0, ap_radius=6.0, annulus_inner=12.0, annulus_outer=18.0, sky_sigma=5.0)
        assert result is not None
        net_flux, flux_err = result
        assert net_flux > 0
        assert flux_err > 0

    def test_returns_none_near_edge(self):
        image = _make_image()
        data_sub = image - 1000.0
        result = fp._measure_at_pixel(data_sub, image, 2.0, 2.0, ap_radius=6.0, annulus_inner=12.0, annulus_outer=18.0, sky_sigma=5.0)
        assert result is None

    def test_returns_none_when_saturated(self):
        image = _make_image()
        data_sub = image - 1000.0
        result = fp._measure_at_pixel(data_sub, image, 200.0, 200.0, ap_radius=6.0, annulus_inner=12.0, annulus_outer=18.0, sky_sigma=5.0)
        assert result is None

    def test_a_saturated_pixel_outside_the_aperture_is_tolerated(self):
        """
        Audit 2026-08-18, finding M7: the check scanned the square bounding
        the ANNULUS, nearly twice the area of the aperture circle and with
        most of the surplus in the corners — the part of the neighbourhood
        that contributes nothing to the flux. A bright star there discarded a
        perfectly good recovery over a pixel the measurement never touches.
        """
        image = _make_image()
        # A saturated pixel 10 px away: inside the annulus' bounding square
        # for these radii, well outside the 6 px photometric aperture.
        image[100, 110] = 65000.0
        data_sub = image - 1000.0

        result = fp._measure_at_pixel(
            data_sub, image, 100.0, 100.0,
            ap_radius=6.0, annulus_inner=12.0, annulus_outer=18.0, sky_sigma=5.0,
        )

        assert result is not None

    def test_a_saturated_pixel_inside_the_aperture_still_rejects(self):
        """The core of the measurement is what must not be clipped."""
        image = _make_image()
        image[100, 103] = 65000.0
        data_sub = image - 1000.0

        result = fp._measure_at_pixel(
            data_sub, image, 100.0, 100.0,
            ap_radius=6.0, annulus_inner=12.0, annulus_outer=18.0, sky_sigma=5.0,
        )

        assert result is None

    def test_gain_divides_the_poisson_term(self):
        """
        flux_err = sqrt(|net_flux| / gain + ap_area * sky_sigma**2) — the
        aperture sum is in ADU but shot noise is Poissonian in electrons
        (audit finding C7). sky_sigma=0 isolates the Poisson term; the
        empirical background scatter is already in ADU and is deliberately
        not gain-converted.
        """
        image = _make_image()
        data_sub = image - 1000.0
        kwargs = dict(ap_radius=6.0, annulus_inner=12.0, annulus_outer=18.0, sky_sigma=0.0)

        net_unity, err_unity = fp._measure_at_pixel(data_sub, image, 100.0, 100.0, **kwargs)
        net_gain4, err_gain4 = fp._measure_at_pixel(
            data_sub, image, 100.0, 100.0, gain_e_per_adu=4.0, **kwargs
        )

        assert net_gain4 == pytest.approx(net_unity)  # the flux itself is unchanged
        assert err_unity == pytest.approx(math.sqrt(abs(net_unity)))
        assert err_gain4 == pytest.approx(math.sqrt(abs(net_unity) / 4.0))


class TestRunGain:
    """
    run() resolves the gain from the frame's own EGAIN/GAIN header. The
    consequence that matters here is the one C7 calls out for this module
    specifically: at gain > 1 the old formula overstated flux_err, so the
    significance came out understated and real faint recoveries were dropped
    against FORCED_PHOTOMETRY_MIN_SNR — defeating the point of precovery.
    """

    @staticmethod
    def _scene_with_header(monkeypatch, header: dict):
        image = _make_image()
        wcs = _make_wcs()
        monkeypatch.setattr(
            "modules.forced_photometry.fits.open",
            lambda *a, **kw: _FakeHDUL(_FakeHDU(image, header)),
        )
        return wcs

    async def _recover(self, monkeypatch, header: dict, min_snr: float) -> list[dict]:
        wcs = self._scene_with_header(monkeypatch, header)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_ENABLED", True)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MAG_LIMIT", 20.0)
        monkeypatch.setattr(config, "FORCED_PHOTOMETRY_MIN_SNR", min_snr)
        gaia = [_gaia_star(wcs, 100, 100, "gaia-1", mag=17.5)]
        return await fp.run(
            _FITS_PATH, sources=[], gaia_stars=gaia, mpc_objects=[], wcs=wcs,
            naxis1=320, naxis2=320, zero_point=24.0, zero_point_err=0.05,
            obs_time=None, psf_fwhm_arcsec=None,
        )

    async def test_header_gain_raises_the_measured_significance(self, monkeypatch):
        without = await self._recover(monkeypatch, {}, min_snr=0.0)
        with_gain = await self._recover(monkeypatch, {"EGAIN": 4.0}, min_snr=0.0)

        assert len(without) == 1 and len(with_gain) == 1
        assert with_gain[0]["flux_err"] < without[0]["flux_err"]
        assert with_gain[0]["flux_aperture"] == pytest.approx(without[0]["flux_aperture"])

    async def test_implausible_gain_header_is_ignored(self, monkeypatch):
        """A ZWO-style GAIN=120 is a gain setting, not e-/ADU — using it
        would understate the error by an order of magnitude."""
        plain = await self._recover(monkeypatch, {}, min_snr=0.0)
        bogus = await self._recover(monkeypatch, {"GAIN": 120.0}, min_snr=0.0)

        assert bogus[0]["flux_err"] == pytest.approx(plain[0]["flux_err"])
