"""
tests/test_astrometry.py — Unit tests for the modules/astrometry/ package

All external I/O is mocked, on the top-level package for everything
accessed as a module-attribute chain (safe regardless of which submodule
under modules/astrometry/ actually makes the call), except WCS, which is
patched on the specific submodule that imports it as a bare name
(_wcs.py) — see that package's __init__.py docstring for why:
  - modules.astrometry.subprocess.run  → controlled CompletedProcess
  - modules.astrometry.fits.open       → context manager returning fake HDU
  - modules.astrometry.sep.Background  → _FakeBackground with fixed globalrms
  - modules.astrometry.sep.extract     → structured numpy array of fake sources
  - modules.astrometry._wcs.WCS        → astropy WCS built from known parameters

All tests are async because astrometry.solve() is declared async.
asyncio_mode = auto is set in pytest.ini, so no @pytest.mark.asyncio required.
"""

from __future__ import annotations

import math
import subprocess
import threading
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from astropy.wcs import WCS as AstropyWCS

import config
from modules import astrometry
from modules.astrometry import _wcs as _wcs_mod
from modules.astrometry._frame_geometry import (
    _frame_center_and_scale,
    _position_angle_deg,
)


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


def _rotate_wcs(wcs: AstropyWCS, theta_deg: float) -> AstropyWCS:
    """
    Return a copy of *wcs* as if its underlying frame had been rotated by
    ``theta_deg`` in the exact sense of ``scipy.ndimage.rotate(data,
    angle=theta_deg)`` — i.e. CD_new = CD_old @ R_ccw(theta_deg).

    Used to build known-rotation fixtures for TestPositionAngle below and
    for modules/subtraction.py's pre-rotation tests: this is the same
    relation _position_angle_deg()'s own docstring claims and
    modules/subtraction.py's pre-rotation math relies on, verified here by
    round-tripping through a real astropy WCS rather than trusting the
    algebra alone.
    """
    theta = math.radians(theta_deg)
    rot = np.array([
        [math.cos(theta), -math.sin(theta)],
        [math.sin(theta),  math.cos(theta)],
    ])
    cd_old = np.array(wcs.pixel_scale_matrix)
    cd_new = cd_old @ rot

    w = AstropyWCS(naxis=2)
    w.wcs.ctype = wcs.wcs.ctype
    w.wcs.crpix = wcs.wcs.crpix
    w.wcs.crval = wcs.wcs.crval
    w.wcs.cd = cd_new.tolist()
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

    ``peak`` defaults to 1000.0, well below the default
    ``config.SATURATION_ADU`` (60000) once the fake background (globalback=800.0,
    see _FakeBackground below) is added back — so sources built with the
    default ``peak`` are never flagged ``saturated`` unless the test raises
    ``peak`` explicitly past that threshold.
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


def _make_sources_at(
    positions: list[tuple[float, float]],
    a: float = 1.5,
    b: float = 1.4,
    flux: float = 5000.0,
    peak: float = 1000.0,
) -> np.ndarray:
    """Like _make_sources(), but with explicit (x, y) pixel positions — used
    by TestNearEdgeFlag to place sources at known distances from the frame
    edge."""
    dtype = np.dtype([
        ("x",    np.float64),
        ("y",    np.float64),
        ("a",    np.float64),
        ("b",    np.float64),
        ("flux", np.float64),
        ("peak", np.float64),
    ])
    arr = np.zeros(len(positions), dtype=dtype)
    arr["x"]    = [p[0] for p in positions]
    arr["y"]    = [p[1] for p in positions]
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

    def _sep_background(data, mask=None):
        # mask= is passed by the second, streak-excluded background pass
        # (_extraction.py re-measures the RMS once a trail has been masked).
        if sep_background_raises:
            raise RuntimeError("sep.Background intentional failure")
        return fake_bkg

    # WCS is imported in _wcs.py as `from astropy.wcs import WCS` (a bare
    # name, not a module-attribute chain), so we patch it on that specific
    # submodule rather than on the top-level astrometry package.
    with (
        patch("modules.astrometry.subprocess.run", side_effect=_subprocess_run),
        patch("modules.astrometry.fits.open", return_value=hdul),
        patch("modules.astrometry._wcs.WCS", return_value=wcs),
        patch("modules.astrometry.sep.Background", side_effect=_sep_background),
        patch("modules.astrometry.sep.extract", side_effect=_make_sep_extract_side_effect(sources)),
    ):
        yield


# ---------------------------------------------------------------------------
# sep.extract side_effect helper — solve() now calls sep.extract() TWICE:
# once for _build_streak_mask()'s coarse, non-deblended, segmentation_map=True
# pre-pass, and once for the real point-source extraction. A plain
# return_value= mock (the old approach) would hand the coarse pass the same
# structured array meant for the real extraction, which solve() then tries to
# unpack as `objs, seg = sep.extract(...)` — breaking every test. This
# discriminates on the segmentation_map kwarg so the coarse pass gets an
# empty "nothing streak-like found" result by default, leaving every
# pre-existing test's behavior unchanged; TestStreakMasking below overrides
# it explicitly to exercise the masking path itself.
# ---------------------------------------------------------------------------

def _empty_coarse_objects() -> np.ndarray:
    return np.zeros(0, dtype=[
        ("a", np.float64), ("b", np.float64),
        ("xmin", np.int32), ("xmax", np.int32),
        ("ymin", np.int32), ("ymax", np.int32),
    ])


def _make_sep_extract_side_effect(sources: np.ndarray):
    def _sep_extract(data, *args, **kwargs):
        if kwargs.get("segmentation_map"):
            return _empty_coarse_objects(), np.zeros(np.asarray(data).shape, dtype=np.int32)
        return sources
    return _sep_extract


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
# Test 1.5 — Position angle (camera rotation diagnostics; see CLAUDE.md's
# "camera rotation" discussion and modules/subtraction.py's pre-rotation use)
# ---------------------------------------------------------------------------

class TestPositionAngle:
    def test_unrotated_wcs_reports_zero(self):
        """A WCS with a plain diagonal CD (North exactly up) measures PA=0."""
        wcs = _make_wcs()
        pa = _position_angle_deg(wcs, wcs.wcs.crpix[0] - 1.0, wcs.wcs.crpix[1] - 1.0)

        assert pa == pytest.approx(0.0, abs=1e-6)

    @pytest.mark.parametrize("theta_deg", [30.0, 90.0, 180.0, 270.0, -45.0])
    def test_rotated_wcs_reports_matching_delta(self, theta_deg):
        """
        A WCS built by rotating the base one by theta_deg (in the
        scipy.ndimage.rotate(angle=theta_deg) sense — see _rotate_wcs())
        must measure a position angle exactly theta_deg larger, mod 360.
        This is the exact relation modules/subtraction.py's pre-rotation
        step relies on to undo a known orientation difference between a
        reference frame and the new frame.
        """
        base = _make_wcs()
        rotated = _rotate_wcs(base, theta_deg)

        pa_base = _position_angle_deg(base, base.wcs.crpix[0] - 1.0, base.wcs.crpix[1] - 1.0)
        pa_rotated = _position_angle_deg(rotated, rotated.wcs.crpix[0] - 1.0, rotated.wcs.crpix[1] - 1.0)

        delta = (pa_rotated - pa_base) % 360.0
        assert delta == pytest.approx(theta_deg % 360.0, abs=1e-6)

    async def test_solve_result_has_position_angle_key(self):
        with _patch_astrometry():
            result = await astrometry.solve(_FITS_PATH)

        assert "position_angle_deg" in result
        # Circular comparison: tiny WCS round-trip float noise can land
        # either side of the 0/360 wraparound (e.g. 359.9997 instead of
        # 0.0003) — both are "basically zero degrees" and must not fail.
        wrapped = min(result["position_angle_deg"] % 360.0, 360.0 - result["position_angle_deg"] % 360.0)
        assert wrapped == pytest.approx(0.0, abs=1e-2)

    async def test_solve_reports_rotated_wcs_position_angle(self):
        """
        Regression fixture for the 2026-08 "camera rotation" investigation
        (source_id 6a7cfbae64e706.89320404): a frame plate-solved with a
        WCS rotated 180deg relative to the unrotated baseline (e.g. a
        meridian flip) must report position_angle_deg=180, not silently
        drop or misreport the frame's real orientation.
        """
        wcs = _rotate_wcs(_make_wcs(), 180.0)
        with _patch_astrometry(wcs=wcs):
            result = await astrometry.solve(_FITS_PATH)

        assert result["position_angle_deg"] == pytest.approx(180.0, abs=1e-2)


# ---------------------------------------------------------------------------
# PSF anchor re-scaled onto the solved plate scale (audit 2026-08-18, M16)
# ---------------------------------------------------------------------------

class TestPsfAnchorUsesSolvedScale:
    @contextmanager
    def _capture_anchor(self):
        """Run solve() and capture the psf_fwhm_arcsec _extract_sources got."""
        seen: dict[str, Any] = {}

        def _fake_extract(fits_path, wcs, pixel_scale_arcsec, naxis1, naxis2,
                          psf_fwhm_arcsec, fits_filename):
            seen["psf_fwhm_arcsec"] = psf_fwhm_arcsec
            seen["pixel_scale_arcsec"] = pixel_scale_arcsec
            return [], []

        with patch("modules.astrometry._extract_sources", side_effect=_fake_extract):
            yield seen

    async def test_pixel_value_is_converted_with_the_solved_scale(self):
        """
        The header claimed 1.0"/px (so QC reported 2.0 px as 2.0"), but the
        solve found 1.0"/px too here — the point of this case is only that
        the pixel value is what gets converted.
        """
        with _patch_astrometry():                      # _make_wcs() -> ~1.0008"/px
            with self._capture_anchor() as seen:
                await astrometry.solve(
                    _FITS_PATH, psf_fwhm_arcsec=2.0, psf_fwhm_px=2.0,
                )

        assert seen["psf_fwhm_arcsec"] == pytest.approx(
            2.0 * seen["pixel_scale_arcsec"], rel=1e-9
        )

    async def test_solved_scale_overrides_a_wrong_header_scale(self):
        """
        Finding M16: an unaccounted focal reducer makes the header's plate
        scale disagree with the solve. The FWHM bounds _extract_sources
        derives from the anchor are compared against source FWHMs computed
        with the *solved* scale, so a header-derived anchor skewed them by
        the ratio between the two — rejecting every real star in the frame,
        or nothing at all.
        """
        solved_scale_deg = 0.000556                    # 2.0"/px
        with _patch_astrometry(wcs=_make_wcs(scale_deg=solved_scale_deg)):
            with self._capture_anchor() as seen:
                await astrometry.solve(
                    _FITS_PATH,
                    psf_fwhm_arcsec=2.0,               # headers said 1.0"/px
                    psf_fwhm_px=2.0,
                )

        assert seen["psf_fwhm_arcsec"] == pytest.approx(4.0, rel=1e-3)

    async def test_pixel_value_alone_is_enough(self):
        """
        A frame whose headers carry no plate scale at all reaches solve()
        with psf_fwhm_arcsec=None. It now gets an anchor anyway.
        """
        with _patch_astrometry(wcs=_make_wcs(scale_deg=0.000556)):
            with self._capture_anchor() as seen:
                await astrometry.solve(
                    _FITS_PATH, psf_fwhm_arcsec=None, psf_fwhm_px=2.0,
                )

        assert seen["psf_fwhm_arcsec"] == pytest.approx(4.0, rel=1e-3)

    async def test_arcsec_value_is_used_when_no_pixel_value_is_given(self):
        """An ad hoc caller with only the arcsec figure keeps working."""
        with _patch_astrometry():
            with self._capture_anchor() as seen:
                await astrometry.solve(_FITS_PATH, psf_fwhm_arcsec=2.0)

        assert seen["psf_fwhm_arcsec"] == pytest.approx(2.0)

    async def test_solve_returns_the_solved_pixel_scale(self):
        """
        pipeline.py re-anchors everything downstream of solve() off this,
        so it has to travel out of the result dict.
        """
        with _patch_astrometry(wcs=_make_wcs(scale_deg=0.000556)):
            result = await astrometry.solve(_FITS_PATH)

        assert result["pixel_scale_arcsec"] == pytest.approx(2.0, rel=1e-3)
# ---------------------------------------------------------------------------
# Frame geometry — per-axis plate scale (audit 2026-08-18, finding M15)
# ---------------------------------------------------------------------------

def _make_anisotropic_wcs(scale_x_deg: float, scale_y_deg: float) -> AstropyWCS:
    """A TAN WCS whose two pixel axes have genuinely different scales."""
    w = AstropyWCS(naxis=2)
    w.wcs.ctype = ["RA---TAN", "DEC--TAN"]
    w.wcs.crpix = [512.0, 512.0]
    w.wcs.crval = [202.47, 47.20]
    w.wcs.cdelt = [-scale_x_deg, scale_y_deg]
    w.wcs.set()
    return w


class TestAnisotropicPlateScale:
    def test_square_pixels_are_unchanged(self):
        """
        The overwhelmingly common case must behave exactly as before the
        per-axis split: with square sky pixels the geometric mean of the
        two column norms IS the column-0 norm.
        """
        scale_deg = 0.000278
        wcs = _make_wcs(scale_deg=scale_deg)

        _, _, fov_deg, pixel_scale_arcsec, _ = _frame_center_and_scale(
            wcs, 1024, 768, "square.fits"
        )

        assert pixel_scale_arcsec == pytest.approx(scale_deg * 3600.0, rel=1e-9)
        assert fov_deg == pytest.approx(1024 * scale_deg, rel=1e-9)

    def test_portrait_frame_fov_uses_the_long_axis_own_scale(self):
        """
        Finding M15: a portrait frame (NAXIS2 > NAXIS1) with 2x1 binning
        has its long dimension along y, but fov_deg used to multiply that
        dimension by the *x* axis' scale — under-reporting the field of
        view by the axis ratio, and with it every catalog query radius
        derived from fov_deg.
        """
        scale_x_deg = 0.000278          # 1"/px, the binned-2x axis
        scale_y_deg = 0.000139          # 0.5"/px
        wcs = _make_anisotropic_wcs(scale_x_deg, scale_y_deg)
        naxis1, naxis2 = 1024, 4096

        _, _, fov_deg, _, _ = _frame_center_and_scale(
            wcs, naxis1, naxis2, "portrait.fits"
        )

        assert fov_deg == pytest.approx(
            max(naxis1 * scale_x_deg, naxis2 * scale_y_deg), rel=1e-9
        )
        # The old formula multiplied the larger *dimension* by the x scale.
        assert fov_deg != pytest.approx(max(naxis1, naxis2) * scale_x_deg, rel=1e-6)

    def test_pixel_scale_is_the_geometric_mean_of_both_axes(self):
        scale_x_deg = 0.000278
        scale_y_deg = 0.000139
        wcs = _make_anisotropic_wcs(scale_x_deg, scale_y_deg)

        _, _, _, pixel_scale_arcsec, _ = _frame_center_and_scale(
            wcs, 1024, 1024, "aniso.fits"
        )

        assert pixel_scale_arcsec == pytest.approx(
            math.sqrt(scale_x_deg * scale_y_deg) * 3600.0, rel=1e-9
        )

    def test_anisotropy_is_warned_about(self, caplog):
        """
        Everything downstream treats the returned scale as isotropic, so a
        frame where that is materially untrue has to say so.
        """
        wcs = _make_anisotropic_wcs(0.000278, 0.000139)

        with caplog.at_level("WARNING", logger="modules.astrometry._frame_geometry"):
            _frame_center_and_scale(wcs, 1024, 1024, "aniso.fits")

        assert any("Anisotropic plate scale" in r.message for r in caplog.records)

    def test_square_pixels_are_not_warned_about(self, caplog):
        wcs = _make_wcs()

        with caplog.at_level("WARNING", logger="modules.astrometry._frame_geometry"):
            _frame_center_and_scale(wcs, 1024, 1024, "square.fits")

        assert not any("Anisotropic plate scale" in r.message for r in caplog.records)

# ---------------------------------------------------------------------------
# Test 2 — Source dict shape and types
# ---------------------------------------------------------------------------

class TestSourceFormat:
    async def test_sources_have_correct_keys(self):
        """Every source dict must carry exactly: ra, dec, flux, fwhm, elongation, saturated, near_edge."""
        with _patch_astrometry():
            result = await astrometry.solve(_FITS_PATH)

        required = {"ra", "dec", "flux", "fwhm", "elongation", "saturated", "near_edge"}
        for src in result["sources"]:
            assert set(src.keys()) == required

    async def test_sources_ra_dec_are_floats(self):
        with _patch_astrometry():
            result = await astrometry.solve(_FITS_PATH)

        for src in result["sources"]:
            assert isinstance(src["ra"],  float), "ra must be a Python float"
            assert isinstance(src["dec"], float), "dec must be a Python float"

    async def test_sources_all_fields_are_floats(self):
        """Every field except the boolean ``saturated``/``near_edge`` flags must be a Python float."""
        with _patch_astrometry():
            result = await astrometry.solve(_FITS_PATH)

        for src in result["sources"]:
            for key, val in src.items():
                if key in ("saturated", "near_edge"):
                    assert isinstance(val, bool), f"{key} must be a Python bool"
                    continue
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
# Test 2.5 — Saturation flag (docs/ISSUES.md #2)
# ---------------------------------------------------------------------------

class TestSaturationFlag:
    """
    sep's "peak" field is background-subtracted; astrometry.solve() adds
    bkg.globalback (800.0 in _FakeBackground) back to approximate the raw
    ADU value and compares it against config.SATURATION_ADU (60000 default).
    """

    async def test_bright_peak_is_flagged_saturated(self):
        bright = _make_sources(n=3, peak=70000.0)  # 70000 + 800 >= 60000
        with _patch_astrometry(sources=bright):
            result = await astrometry.solve(_FITS_PATH)

        assert len(result["sources"]) == 3
        assert all(src["saturated"] is True for src in result["sources"])

    async def test_faint_peak_is_not_flagged_saturated(self):
        with _patch_astrometry():  # default peak=1000.0, well below threshold
            result = await astrometry.solve(_FITS_PATH)

        assert result["sources"]
        assert all(src["saturated"] is False for src in result["sources"])

    async def test_saturated_flag_present_in_sources_all_too(self):
        bright = _make_sources(n=3, peak=70000.0)
        with _patch_astrometry(sources=bright):
            result = await astrometry.solve(_FITS_PATH)

        assert result["sources_all"]
        assert all(src["saturated"] is True for src in result["sources_all"])


# ---------------------------------------------------------------------------
# Test 2.6 — Near-edge geometry flag (coma false-positive fix, 2026-08-07)
# ---------------------------------------------------------------------------

class TestNearEdgeFlag:
    """
    A 1024x1024 frame with the default EDGE_MARGIN_FRAC=0.05 has a 51.2px
    margin on every side — sources inside [51.2, 972.8] on both axes are
    "central", everything else is "near_edge".
    """

    async def test_central_source_is_not_near_edge(self):
        centre = _make_sources_at([(512.0, 512.0)])
        with _patch_astrometry(sources=centre, naxis1=1024, naxis2=1024):
            result = await astrometry.solve(_FITS_PATH)

        assert result["sources"][0]["near_edge"] is False

    async def test_corner_source_is_near_edge(self):
        corner = _make_sources_at([(20.0, 20.0)])
        with _patch_astrometry(sources=corner, naxis1=1024, naxis2=1024):
            result = await astrometry.solve(_FITS_PATH)

        assert result["sources"][0]["near_edge"] is True

    async def test_near_right_or_bottom_edge_is_near_edge(self):
        # Only one axis needs to be within the margin — OR across all 4 sides.
        edges = _make_sources_at([(1000.0, 512.0), (512.0, 1000.0)])
        with _patch_astrometry(sources=edges, naxis1=1024, naxis2=1024):
            result = await astrometry.solve(_FITS_PATH)

        assert all(src["near_edge"] is True for src in result["sources"])

    async def test_near_edge_flag_present_in_sources_all_too(self):
        corner = _make_sources_at([(20.0, 20.0)])
        with _patch_astrometry(sources=corner, naxis1=1024, naxis2=1024):
            result = await astrometry.solve(_FITS_PATH)

        assert result["sources_all"]
        assert result["sources_all"][0]["near_edge"] is True

    async def test_margin_scales_with_frame_size(self, monkeypatch):
        """
        The margin is a FRACTION of NAXIS1/NAXIS2, not a fixed pixel count:
        one and the same x=30 is near-edge in a 1024px-wide frame (51.2px
        margin) yet comfortably interior in a 256px-wide one (12.8px margin).

        EDGE_MARGIN_FRAC is pinned explicitly rather than relying on the
        config default, so that tuning that default (as 66cf519 did, from 0.1
        to 0.05) can't silently invalidate the arithmetic this test asserts.
        """
        monkeypatch.setattr(config, "EDGE_MARGIN_FRAC", 0.05)

        with _patch_astrometry(sources=_make_sources_at([(30.0, 512.0)]),
                               naxis1=1024, naxis2=1024):
            large = await astrometry.solve(_FITS_PATH)

        with _patch_astrometry(sources=_make_sources_at([(30.0, 128.0)]),
                               naxis1=256, naxis2=256):
            small = await astrometry.solve(_FITS_PATH)

        assert large["sources"][0]["near_edge"] is True
        assert small["sources"][0]["near_edge"] is False

    async def test_custom_edge_margin_frac_widens_the_zone(self, monkeypatch):
        """A source comfortably central under the default 0.05 margin becomes
        near-edge once EDGE_MARGIN_FRAC is widened to cover it."""
        monkeypatch.setattr(config, "EDGE_MARGIN_FRAC", 0.4)
        mid = _make_sources_at([(300.0, 512.0)])  # within 409.6px of the left edge
        with _patch_astrometry(sources=mid, naxis1=1024, naxis2=1024):
            result = await astrometry.solve(_FITS_PATH)

        assert result["sources"][0]["near_edge"] is True


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
# ASTAP timeout budgets — unit tests
#
# Regression coverage for the 2026-08-13 IC3322A incident: the wide/blind
# retry attempt used to share the narrow attempt's ASTAP_TIMEOUT_SEC (60s)
# budget, which is nowhere near enough for a blind search over tens of
# degrees against the full star catalog — re-running ANALYZE on 24
# mis-pointed frames only fixed 1 of them, because the wide retry was almost
# always killed by the shared timeout before astap could finish. These test
# _run_astap_attempt() directly (not through the higher-level solve()) since
# that's the one place that decides which budget applies.
# ---------------------------------------------------------------------------

from modules.astrometry._astap import _run_astap, _run_astap_attempt  # noqa: E402


class TestAstapTimeoutBudgets:

    async def test_narrow_attempt_uses_astap_timeout_sec(self, monkeypatch):
        monkeypatch.setattr(config, "ASTAP_TIMEOUT_SEC", 42.0)
        monkeypatch.setattr(config, "ASTAP_WIDE_SEARCH_TIMEOUT_SEC", 999.0)

        run_mock = MagicMock(return_value=MagicMock(
            returncode=0, stdout="Solution found", stderr="",
        ))
        with patch("modules.astrometry.subprocess.run", run_mock):
            outcome = await _run_astap_attempt(_FITS_PATH, None, wide_radius_deg=None)

        assert outcome == "solved"
        assert run_mock.call_args.kwargs["timeout"] == 42.0

    async def test_wide_attempt_uses_astap_wide_search_timeout_sec(self, monkeypatch):
        """The wide retry must get its OWN, separate (larger) budget — not
        ASTAP_TIMEOUT_SEC, the narrow attempt's own timeout."""
        monkeypatch.setattr(config, "ASTAP_TIMEOUT_SEC", 42.0)
        monkeypatch.setattr(config, "ASTAP_WIDE_SEARCH_TIMEOUT_SEC", 999.0)

        run_mock = MagicMock(return_value=MagicMock(
            returncode=0, stdout="Solution found", stderr="",
        ))
        with patch("modules.astrometry.subprocess.run", run_mock):
            outcome = await _run_astap_attempt(_FITS_PATH, None, wide_radius_deg=30.0)

        assert outcome == "solved"
        assert run_mock.call_args.kwargs["timeout"] == 999.0

    async def test_wide_retry_timeout_is_not_further_retried(self, monkeypatch):
        """A timeout on the wide retry itself must not trigger yet another
        attempt — "error" (from either attempt) is always terminal."""
        monkeypatch.setattr(config, "ASTAP_RETRY_WIDE_SEARCH", True)
        monkeypatch.setattr(config, "ASTAP_WIDE_SEARCH_RADIUS_DEG", 30.0)

        def _side_effect(cmd, **kwargs):
            if "30.0" in cmd:
                raise subprocess.TimeoutExpired(cmd="astap", timeout=kwargs["timeout"])
            return MagicMock(returncode=1, stdout="No solution found", stderr="")

        run_mock = MagicMock(side_effect=_side_effect)
        with patch("modules.astrometry.subprocess.run", run_mock):
            result = await _run_astap(_FITS_PATH, None)

        assert result is False
        assert run_mock.call_count == 2  # narrow (no_solution), then wide (timeout) — no third attempt

    async def test_full_retry_flow_uses_distinct_timeouts_for_each_attempt(self, monkeypatch):
        """End-to-end through _run_astap(): narrow attempt reports no
        solution, is retried wide, and each subprocess.run call carries its
        own attempt's configured timeout, not the other one's."""
        monkeypatch.setattr(config, "ASTAP_RETRY_WIDE_SEARCH", True)
        monkeypatch.setattr(config, "ASTAP_WIDE_SEARCH_RADIUS_DEG", 30.0)
        monkeypatch.setattr(config, "ASTAP_TIMEOUT_SEC", 42.0)
        monkeypatch.setattr(config, "ASTAP_WIDE_SEARCH_TIMEOUT_SEC", 999.0)

        def _side_effect(cmd, **kwargs):
            if "30.0" in cmd:
                return MagicMock(returncode=0, stdout="Solution found", stderr="")
            return MagicMock(returncode=1, stdout="No solution found", stderr="")

        run_mock = MagicMock(side_effect=_side_effect)
        with patch("modules.astrometry.subprocess.run", run_mock):
            result = await _run_astap(_FITS_PATH, None)

        assert result is True
        assert [c.kwargs["timeout"] for c in run_mock.call_args_list] == [42.0, 999.0]

    async def test_the_subprocess_does_not_run_on_the_event_loop_thread(self):
        """
        Audit 2026-08-18, finding L6: astap is a seconds-to-minutes blocking
        call inside an `async def`. Run inline it pins the event loop for
        its whole duration — harmless while the worker drains one item at a
        time, but it means a caller that gathers several solves gets them
        strictly one after another while the code reads as if it doesn't.
        """
        loop_thread = threading.get_ident()
        seen: list[int] = []

        def _record(cmd, **kwargs):
            seen.append(threading.get_ident())
            return MagicMock(returncode=0, stdout="Solution found", stderr="")

        with patch("modules.astrometry.subprocess.run", MagicMock(side_effect=_record)):
            assert await _run_astap_attempt(
                _FITS_PATH, None, wide_radius_deg=None,
            ) == "solved"

        assert seen and all(tid != loop_thread for tid in seen)

    async def test_timeout_still_kills_the_child_via_subprocess_run(self, monkeypatch):
        """
        The budget stays on subprocess.run's own `timeout=` rather than an
        asyncio.wait_for around the thread: only the former actually kills
        and reaps the astap child. A TimeoutExpired raised inside the worker
        thread must still surface as this attempt's "error".
        """
        monkeypatch.setattr(config, "ASTAP_TIMEOUT_SEC", 7.0)

        def _timeout(cmd, **kwargs):
            raise subprocess.TimeoutExpired(cmd="astap", timeout=kwargs["timeout"])

        with patch("modules.astrometry.subprocess.run", MagicMock(side_effect=_timeout)):
            outcome = await _run_astap_attempt(_FITS_PATH, None, wide_radius_deg=None)

        assert outcome == "error"


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
# WCS plausibility — audit 2026-08-18, finding H15
#
# A solved WCS is authoritative by construction: every source position, every
# catalog match and every anomaly's coordinates come from it, and no
# downstream module has anything to check it against. Nothing checked it here
# either, beyond astap reporting a solution and the axes being celestial, so a
# false star-pattern match — most likely under ASTAP_RETRY_WIDE_SEARCH's blind
# 30-degree retry — became a systematic position error for the whole frame
# with no distinguishing log line.
# ---------------------------------------------------------------------------

class TestWcsPlausibility:

    async def test_an_absurdly_fine_plate_scale_is_rejected(self):
        """
        The 2026-08-06 CD/PC double-scaling incident produced exactly this:
        0.78"/px read back as 0.0002"/px. It was caught then by every FWHM
        collapsing to zero; here it is caught outright.
        """
        absurd = _make_wcs(scale_deg=1e-9)
        with _patch_astrometry(wcs=absurd):
            result = await astrometry.solve(_FITS_PATH)

        assert result == {}

    async def test_an_absurdly_coarse_plate_scale_is_rejected(self):
        absurd = _make_wcs(scale_deg=1.0)  # 3600"/px
        with _patch_astrometry(wcs=absurd):
            result = await astrometry.solve(_FITS_PATH)

        assert result == {}

    async def test_an_ordinary_plate_scale_is_accepted(self):
        with _patch_astrometry(wcs=_make_wcs(scale_deg=0.000278)):
            result = await astrometry.solve(_FITS_PATH)

        assert result != {}
        assert result["ra_center"] == pytest.approx(202.47, abs=0.5)

    def test_a_degenerate_transform_is_rejected(self):
        """A collapsed axis maps the whole frame onto a line."""
        wcs = _make_wcs()
        wcs.wcs.cd = np.array([[1e-4, 1e-4], [1e-4, 1e-4]])
        wcs.wcs.set()

        assert astrometry._wcs._is_plausible_wcs(wcs, 1024, 1024, "frame.fits") is False

    def test_off_sphere_reference_coordinates_are_rejected(self):
        # astropy refuses to *build* such a WCS, so the value is written in
        # after construction — which is exactly how a corrupt or
        # hand-edited .wcs side file would reach this code.
        wcs = _make_wcs()
        wcs.wcs.crval = [202.47, 120.0]

        assert astrometry._wcs._is_plausible_wcs(wcs, 1024, 1024, "frame.fits") is False

    def test_the_window_is_configurable(self, monkeypatch):
        """Widen the bounds for an unusual instrument rather than disabling."""
        wcs = _make_wcs(scale_deg=1.0)  # 3600"/px
        monkeypatch.setattr(config, "ASTROMETRY_PIXEL_SCALE_MAX_ARCSEC", 7200.0)

        assert astrometry._wcs._is_plausible_wcs(wcs, 1024, 1024, "frame.fits") is True


# ---------------------------------------------------------------------------
# Test 6b — astap's fresh .wcs side file is preferred over a pre-existing,
# already-celestial WCS in the FITS header itself.
#
# Regression test for the 2026-08-06 "UGC_6930" incident: the incoming FITS
# already carried a plausible-looking celestial WCS (written by capture
# software from mount pointing, not a genuine plate solve). The old code
# only ever consulted the .wcs side file when the header's own WCS lacked
# celestial axes — so a header WCS that merely *looked* valid, but was off
# by ~178", was silently trusted over astap's own freshly-solved output.
# ---------------------------------------------------------------------------

class TestSidecarPcCdeltCleanup:
    """
    astap writes BOTH CD* and PC*+CDELT* into its .wcs sidecar, and astropy
    multiplies the two — so the PC/CDELT pair is stripped when CD is present.
    That cleanup matched by PREFIX, and "PC" is also the first two letters of
    PCOUNT, a structural HDU keyword with nothing to do with the WCS. Only
    indexed cards belong to the transform.
    """

    def test_it_matches_indexed_pc_and_cdelt_cards(self):
        for card in ("PC1_1", "PC2_1", "PC1_1A", "CDELT1", "CDELT2", "CDELT1A"):
            assert _wcs_mod._PC_CDELT_CARD_RE.match(card), card

    def test_it_leaves_pcount_and_other_cards_alone(self):
        for card in ("PCOUNT", "GCOUNT", "CD1_1", "CDELTA", "PC", "CCDTEMP"):
            assert not _wcs_mod._PC_CDELT_CARD_RE.match(card), card


class TestPrefersFreshWcsSidecarOverStaleHeader:
    async def test_sidecar_wcs_wins_over_celestial_header_wcs(self):
        stale_wcs = _make_wcs(ra=100.0, dec=10.0)     # e.g. mount-pointing estimate
        fresh_wcs = _make_wcs(ra=202.47, dec=47.20)   # astap's own fresh solve

        header_hdul = _make_hdul(header={"NAXIS1": _IMAGE_SHAPE[1], "NAXIS2": _IMAGE_SHAPE[0]})
        sidecar_hdul = _make_hdul(header={"_MARKER": "sidecar"})
        primary_header_obj = header_hdul[0].header
        sidecar_header_obj = sidecar_hdul[0].header

        def _fits_open(path, *args, **kwargs):
            return sidecar_hdul if path.endswith(".wcs") else header_hdul

        def _wcs_ctor(hdr, *args, **kwargs):
            return fresh_wcs if hdr is sidecar_header_obj else stale_wcs

        fake_bkg = _FakeBackground()

        with (
            patch(
                "modules.astrometry.subprocess.run",
                return_value=MagicMock(
                    returncode=0,
                    stdout="Solution found: RA=13h29m52s, Dec=+47d12m00s",
                    stderr="",
                ),
            ),
            patch("modules.astrometry.fits.open", side_effect=_fits_open),
            patch("modules.astrometry.os.path.exists", return_value=True),
            patch("modules.astrometry._wcs.WCS", side_effect=_wcs_ctor),
            patch("modules.astrometry.sep.Background", return_value=fake_bkg),
            patch("modules.astrometry.sep.extract", return_value=_make_sources()),
        ):
            result = await astrometry.solve(_FITS_PATH)

        assert result != {}
        # Must match fresh_wcs's centre (202.47, 47.20), NOT stale_wcs's (100.0, 10.0).
        assert result["ra_center"] == pytest.approx(202.47, abs=0.01)
        assert result["dec_center"] == pytest.approx(47.20, abs=0.01)

    async def test_falls_back_to_header_wcs_when_sidecar_missing(self):
        """
        No .wcs side file on disk (os.path.exists → False) → must fall back
        to whatever WCS the FITS header itself carries, same as before this
        fix, rather than failing outright.
        """
        with _patch_astrometry(wcs=_make_wcs(ra=202.47, dec=47.20)):
            with patch("modules.astrometry.os.path.exists", return_value=False):
                result = await astrometry.solve(_FITS_PATH)

        assert result != {}
        assert result["ra_center"] == pytest.approx(202.47, abs=0.01)


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

class TestSourcesAllElongationBound:
    """
    Audit finding C2 — `sources_all` is the ONLY detection list
    catalog_matcher and anomaly_detector ever receive (pipeline.py's step 6),
    so its elongation ceiling decides what the SPACE_DEBRIS classification
    can ever see. A hardcoded 5.0 sat below SPACE_DEBRIS_EDGE_ELONGATION_MIN
    (6.0), the deliberately raised bar for a `near_edge` source, making that
    branch structurally unreachable: a trailed source near the frame edge was
    cut here and never reached the classifier as anything, not even UNKNOWN.

    A trailed a=7/b=1 detection is used throughout: elongation 7.0 sits above
    the edge threshold and below the new default ceiling of 15.0.
    """

    async def test_trailed_source_above_the_edge_threshold_survives(self):
        trail = _make_sources(n=3, a=7.0, b=1.0)
        with _patch_astrometry(sources=trail):
            result = await astrometry.solve(_FITS_PATH)

        assert len(result["sources_all"]) == 3
        assert all(
            src["elongation"] > config.SPACE_DEBRIS_EDGE_ELONGATION_MIN
            for src in result["sources_all"]
        )

    async def test_trailed_source_is_still_kept_out_of_the_strict_list(self):
        """`sources` stays a star list — the loosened ceiling applies only to
        `sources_all`, and photometric calibration still runs off `sources`."""
        trail = _make_sources(n=3, a=7.0, b=1.0)
        with _patch_astrometry(sources=trail):
            result = await astrometry.solve(_FITS_PATH)

        assert result["sources"] == []

    async def test_degenerate_minor_axis_reads_as_a_one_pixel_wide_feature(self):
        """
        Audit 2026-08-18, finding L4: a minor axis below the pixel grid's own
        resolution limit is clamped to that limit (1/sqrt(12) px), not to an
        epsilon. The reported elongation is then "how elongated this feature
        would be if it were exactly one pixel wide" — a shape that could
        actually exist — instead of the 10^6 an epsilon produced, which was
        not a measurement of anything and cleared every elongation threshold
        in the pipeline on its way to being persisted as the source's shape.
        """
        degenerate = _make_sources(n=5, a=2.0, b=0.0)
        with _patch_astrometry(sources=degenerate):
            result = await astrometry.solve(_FITS_PATH)

        assert len(result["sources_all"]) == 5
        for src in result["sources_all"]:
            assert src["elongation"] == pytest.approx(2.0 * math.sqrt(12.0), rel=1e-6)

    async def test_a_realistic_degenerate_fit_is_still_cut_by_the_ceiling(self):
        """
        The clamp does not quietly open the gate the ceiling was closing by
        accident. At SEP_MIN_AREA=15 a sub-pixel-wide detection has to be a
        line of at least ~15 pixels, whose semi-major axis is ~15/sqrt(12);
        clamped, that reads as an elongation of ~15 and the default ceiling
        still cuts it. What changes is that the ceiling now cuts it for a
        reason a reader can check, rather than because the divisor was
        arbitrary.
        """
        line = _make_sources(n=5, a=15.0 / math.sqrt(12.0), b=0.0)
        with _patch_astrometry(sources=line):
            result = await astrometry.solve(_FITS_PATH)

        assert result["sources_all"] == []

    async def test_ceiling_is_config_driven(self, monkeypatch):
        """The bound is no longer hardcoded — lowering it past the trail's
        own elongation drops the same detection."""
        monkeypatch.setattr(config, "SOURCES_ALL_ELONGATION_MAX", 3.0)
        trail = _make_sources(n=3, a=7.0, b=1.0)
        with _patch_astrometry(sources=trail):
            result = await astrometry.solve(_FITS_PATH)

        assert result["sources_all"] == []

    async def test_warns_when_configured_back_into_the_dead_state(self, monkeypatch, caplog):
        """
        Configuring the ceiling at or below SPACE_DEBRIS_EDGE_ELONGATION_MIN
        reinstates exactly the defect this finding is about. It stays
        possible — an operator may have a reason — but it must not be silent.
        """
        monkeypatch.setattr(config, "SOURCES_ALL_ELONGATION_MAX", 5.0)
        monkeypatch.setattr(config, "SPACE_DEBRIS_EDGE_ELONGATION_MIN", 6.0)

        with caplog.at_level("WARNING", logger="modules.astrometry._extraction"):
            with _patch_astrometry(sources=_make_sources(n=3)):
                await astrometry.solve(_FITS_PATH)

        assert any(
            "SPACE_DEBRIS branch unreachable" in rec.getMessage()
            for rec in caplog.records
        )

    async def test_no_warning_with_the_default_configuration(self, caplog):
        with caplog.at_level("WARNING", logger="modules.astrometry._extraction"):
            with _patch_astrometry(sources=_make_sources(n=3)):
                await astrometry.solve(_FITS_PATH)

        assert not any(
            "SOURCES_ALL_ELONGATION_MAX" in rec.getMessage()
            for rec in caplog.records
        )


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
        # Still filtered out of the strict star list: clamped at the pixel
        # grid's resolution limit the ratio is a/(1/sqrt(12)) ~= 6.9, which
        # is a physically possible shape but nothing like a star.
        assert len(result.get("sources", [])) == 0


# ---------------------------------------------------------------------------
# Test 9 — Streak masking (satellite trails / diffraction spikes)
#
# Real incident, 2026-08-07, T_CrB_Light_L_60_2024-05-28T19-06-10.fits: a
# full-frame satellite trail fragmented into several small, roundish "stars"
# at the ordinary extraction settings. _build_streak_mask()'s coarse,
# non-deblended pre-pass finds long+elongated coarse features and zeroes
# their pixels in data_sub before the real extraction runs.
# ---------------------------------------------------------------------------

def _make_coarse_object(
    a: float, xmin: int, xmax: int, ymin: int, ymax: int, b: float = 1.0,
    x: float | None = None, y: float | None = None,
    flux: float = 50_000.0, peak: float = 900.0,
) -> np.ndarray:
    """
    One row of the coarse pre-pass's sep.extract() output.

    Carries x/y/flux/peak as well as the shape fields, because
    _build_streak_mask() now also hands each masked streak back to
    _extraction.py as a detection of its own (audit 2026-08-18, finding H16).
    """
    obj = np.zeros(1, dtype=[
        ("a", np.float64), ("b", np.float64),
        ("x", np.float64), ("y", np.float64),
        ("flux", np.float64), ("peak", np.float64),
        ("xmin", np.int32), ("xmax", np.int32),
        ("ymin", np.int32), ("ymax", np.int32),
    ])
    obj["a"] = a
    obj["b"] = b
    obj["x"] = x if x is not None else (xmin + xmax) / 2.0
    obj["y"] = y if y is not None else (ymin + ymax) / 2.0
    obj["flux"] = flux
    obj["peak"] = peak
    obj["xmin"], obj["xmax"] = xmin, xmax
    obj["ymin"], obj["ymax"] = ymin, ymax
    return obj


class TestStreakMasking:
    async def test_long_elongated_streak_is_masked_before_final_extraction(self):
        """
        A coarse candidate that is both highly elongated and far longer than
        any real star's footprint must have its pixels zeroed in the data
        the real (second) sep.extract() call receives.
        """
        calls: list[tuple[np.ndarray, dict]] = []
        streak_col = 10  # a 1px-wide vertical streak at x=10, y in [10, 200)

        def _sep_extract(data, *args, **kwargs):
            arr = np.asarray(data)
            calls.append((arr.copy(), kwargs))
            if kwargs.get("segmentation_map"):
                # elongation = 190/1 = 190; bbox diagonal ~= 190px, both well
                # past the default STREAK_ELONGATION_MIN (5.0) and
                # STREAK_MIN_LENGTH_ARCSEC (30") at ~1"/px default WCS scale.
                coarse = _make_coarse_object(a=95.0, xmin=streak_col, xmax=streak_col, ymin=10, ymax=200)
                seg = np.zeros(arr.shape, dtype=np.int32)
                seg[10:200, streak_col] = 1
                return coarse, seg
            return _make_sources(n=5)

        with _patch_astrometry(sources=_make_sources(n=5)):
            with patch("modules.astrometry.sep.extract", side_effect=_sep_extract):
                result = await astrometry.solve(_FITS_PATH)

        assert result != {}
        assert len(calls) == 2
        final_data, final_kwargs = calls[1]
        assert not final_kwargs.get("segmentation_map")
        assert np.all(final_data[10:200, streak_col] == 0.0)

    async def test_a_masked_streak_is_re_emitted_as_a_detection(self):
        """
        Audit 2026-08-18, finding H16: the two thresholds behind the mask
        cannot geometrically tell a satellite trail from a genuine fast NEO
        trailing within one exposure, so a real moving object's pixels were
        erased before sep.extract() ever ran — with no second chance, since
        the frame is never re-analysed from other data. The mask stays (it
        stops the trail fragmenting into false stars), but the streak comes
        back as one detection at its own centroid.
        """
        streak_col = 10

        def _sep_extract(data, *args, **kwargs):
            arr = np.asarray(data)
            if kwargs.get("segmentation_map"):
                coarse = _make_coarse_object(
                    a=95.0, xmin=streak_col, xmax=streak_col, ymin=10, ymax=200,
                    x=float(streak_col), y=105.0, flux=123_456.0,
                )
                seg = np.zeros(arr.shape, dtype=np.int32)
                seg[10:200, streak_col] = 1
                return coarse, seg
            return _make_sources(n=5)

        with _patch_astrometry(sources=_make_sources(n=5)):
            with patch("modules.astrometry.sep.extract", side_effect=_sep_extract):
                result = await astrometry.solve(_FITS_PATH)

        streaks = [s for s in result["sources_all"] if s["flux"] == pytest.approx(123_456.0)]
        assert len(streaks) == 1
        assert streaks[0]["elongation"] == pytest.approx(95.0)
        # Marked so photometry never sizes a PSF aperture from its length.
        assert streaks[0]["_streak"] is True
        # Not a star: it must never reach the photometric reference set.
        assert not any(s["flux"] == pytest.approx(123_456.0) for s in result["sources"])

    async def test_extraction_uses_the_background_re_measured_without_the_streak(self):
        """
        Audit 2026-08-18, finding H12: once the trail is masked, the RMS the
        real extraction thresholds on must be the one measured with the trail
        EXCLUDED, not the first pass's trail-inflated figure.
        """
        streak_col = 10
        seen_err: list[float] = []
        unmasked = _FakeBackground(globalrms=20.0)
        masked = _FakeBackground(globalrms=7.0)

        def _sep_background(data, mask=None):
            return unmasked if mask is None else masked

        def _sep_extract(data, *args, **kwargs):
            arr = np.asarray(data)
            if kwargs.get("segmentation_map"):
                coarse = _make_coarse_object(
                    a=95.0, xmin=streak_col, xmax=streak_col, ymin=10, ymax=200,
                    x=float(streak_col), y=105.0, flux=123_456.0,
                )
                seg = np.zeros(arr.shape, dtype=np.int32)
                seg[10:200, streak_col] = 1
                return coarse, seg
            seen_err.append(kwargs.get("err"))
            return _make_sources(n=5)

        with _patch_astrometry(sources=_make_sources(n=5)):
            with (
                patch("modules.astrometry.sep.extract", side_effect=_sep_extract),
                patch("modules.astrometry.sep.Background", side_effect=_sep_background),
            ):
                await astrometry.solve(_FITS_PATH)

        assert seen_err == [pytest.approx(7.0)]

    async def test_a_streak_bypasses_the_sources_all_elongation_ceiling(self):
        """
        SOURCES_ALL_ELONGATION_MAX exists to reject the degenerate a/b of a
        near-zero minor axis, not a feature deliberately selected for being
        elongated — a full-frame trail's ratio is far past it.
        """
        def _sep_extract(data, *args, **kwargs):
            arr = np.asarray(data)
            if kwargs.get("segmentation_map"):
                coarse = _make_coarse_object(
                    a=300.0, b=1.0, xmin=10, xmax=10, ymin=10, ymax=600,
                    x=10.0, y=305.0, flux=999.0,
                )
                seg = np.zeros(arr.shape, dtype=np.int32)
                seg[10:600, 10] = 1
                return coarse, seg
            return _make_sources(n=5)

        with _patch_astrometry(sources=_make_sources(n=5)):
            with patch("modules.astrometry.sep.extract", side_effect=_sep_extract):
                result = await astrometry.solve(_FITS_PATH)

        streaks = [s for s in result["sources_all"] if s["flux"] == pytest.approx(999.0)]
        assert len(streaks) == 1
        assert streaks[0]["elongation"] > config.SOURCES_ALL_ELONGATION_MAX

    async def test_a_degenerate_streaks_elongation_stays_physical(self):
        """
        Audit 2026-08-18, finding L4: a re-emitted streak is the one source
        that bypasses SOURCES_ALL_ELONGATION_MAX entirely, so whatever
        elongation the coarse pass computed for it travels untouched into
        anomaly_detector.py and onto the wire. A coarse fit with b=0 used to
        make that 10^6; clamped at the pixel grid's resolution limit it is
        a/(1/sqrt(12)) — large, because a trail genuinely is, but a shape
        that could exist.
        """
        def _sep_extract(data, *args, **kwargs):
            arr = np.asarray(data)
            if kwargs.get("segmentation_map"):
                coarse = _make_coarse_object(
                    a=300.0, b=0.0, xmin=10, xmax=10, ymin=10, ymax=600,
                    x=10.0, y=305.0, flux=999.0,
                )
                seg = np.zeros(arr.shape, dtype=np.int32)
                seg[10:600, 10] = 1
                return coarse, seg
            return _make_sources(n=5)

        with _patch_astrometry(sources=_make_sources(n=5)):
            with patch("modules.astrometry.sep.extract", side_effect=_sep_extract):
                result = await astrometry.solve(_FITS_PATH)

        streaks = [s for s in result["sources_all"] if s["flux"] == pytest.approx(999.0)]
        assert len(streaks) == 1
        assert streaks[0]["elongation"] == pytest.approx(
            300.0 * math.sqrt(12.0), rel=1e-6
        )

    async def test_short_elongated_feature_is_not_masked(self):
        """A coarse candidate elongated enough but far shorter than
        STREAK_MIN_LENGTH_ARCSEC (an ordinary elongated star, not a streak)
        must be left alone."""
        calls: list[tuple[np.ndarray, dict]] = []

        def _sep_extract(data, *args, **kwargs):
            arr = np.asarray(data)
            calls.append((arr.copy(), kwargs))
            if kwargs.get("segmentation_map"):
                # a=6, elongation=6 (>5.0) but bbox diagonal only ~6px — far
                # below the length floor, so this must NOT be treated as a streak.
                coarse = _make_coarse_object(a=6.0, xmin=10, xmax=16, ymin=10, ymax=10)
                seg = np.zeros(arr.shape, dtype=np.int32)
                seg[10, 10:16] = 1
                return coarse, seg
            return _make_sources(n=5)

        with _patch_astrometry(sources=_make_sources(n=5)):
            with patch("modules.astrometry.sep.extract", side_effect=_sep_extract):
                await astrometry.solve(_FITS_PATH)

        final_data, _ = calls[1]
        assert not np.any(final_data[10, 10:16] == 0.0)

    async def test_long_but_round_feature_is_not_masked(self):
        """A coarse candidate that spans a long bounding box but isn't
        elongated (e.g. a large round nebula/galaxy core) must not be
        mistaken for a streak."""
        calls: list[tuple[np.ndarray, dict]] = []

        def _sep_extract(data, *args, **kwargs):
            arr = np.asarray(data)
            calls.append((arr.copy(), kwargs))
            if kwargs.get("segmentation_map"):
                # a == b -> elongation == 1.0, well under STREAK_ELONGATION_MIN,
                # even though the bbox itself is long.
                coarse = _make_coarse_object(a=100.0, b=100.0, xmin=10, xmax=210, ymin=10, ymax=210)
                seg = np.zeros(arr.shape, dtype=np.int32)
                seg[10:210, 10:210] = 1
                return coarse, seg
            return _make_sources(n=5)

        with _patch_astrometry(sources=_make_sources(n=5)):
            with patch("modules.astrometry.sep.extract", side_effect=_sep_extract):
                await astrometry.solve(_FITS_PATH)

        final_data, _ = calls[1]
        assert not np.any(final_data[10:210, 10:210] == 0.0)

    async def test_no_streak_found_leaves_data_untouched(self):
        """Default fixture behavior (empty coarse pass) — pre-existing tests'
        assumption that data_sub reaches the real extraction unmodified."""
        calls: list[tuple[np.ndarray, dict]] = []

        def _sep_extract(data, *args, **kwargs):
            arr = np.asarray(data)
            calls.append((arr.copy(), kwargs))
            if kwargs.get("segmentation_map"):
                return _empty_coarse_objects(), np.zeros(arr.shape, dtype=np.int32)
            return _make_sources(n=5)

        with _patch_astrometry(sources=_make_sources(n=5)):
            with patch("modules.astrometry.sep.extract", side_effect=_sep_extract):
                await astrometry.solve(_FITS_PATH)

        coarse_data, _ = calls[0]
        final_data, _ = calls[1]
        assert np.array_equal(coarse_data, final_data)
