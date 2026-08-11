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
    def __init__(self, data: np.ndarray) -> None:
        self.data = data


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
