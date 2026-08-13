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

import subprocess
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from astropy.wcs import WCS as AstropyWCS

import config
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

    def _sep_background(data):
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
    A 1024x1024 frame with the default EDGE_MARGIN_FRAC=0.1 has a 102.4px
    margin on every side — sources inside [102.4, 921.6] on both axes are
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

    async def test_margin_scales_with_frame_size(self):
        """The margin is a FRACTION of NAXIS1/NAXIS2, not a fixed pixel count —
        x=20 sits inside the 25.6px margin of a 256px-wide frame."""
        small_frame_edge = _make_sources_at([(20.0, 128.0)])
        with _patch_astrometry(sources=small_frame_edge, naxis1=256, naxis2=256):
            result = await astrometry.solve(_FITS_PATH)

        assert result["sources"][0]["near_edge"] is True

    async def test_custom_edge_margin_frac_widens_the_zone(self, monkeypatch):
        """A source comfortably central under the default 0.1 margin becomes
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

    def test_narrow_attempt_uses_astap_timeout_sec(self, monkeypatch):
        monkeypatch.setattr(config, "ASTAP_TIMEOUT_SEC", 42.0)
        monkeypatch.setattr(config, "ASTAP_WIDE_SEARCH_TIMEOUT_SEC", 999.0)

        run_mock = MagicMock(return_value=MagicMock(
            returncode=0, stdout="Solution found", stderr="",
        ))
        with patch("modules.astrometry.subprocess.run", run_mock):
            outcome = _run_astap_attempt(_FITS_PATH, None, wide_radius_deg=None)

        assert outcome == "solved"
        assert run_mock.call_args.kwargs["timeout"] == 42.0

    def test_wide_attempt_uses_astap_wide_search_timeout_sec(self, monkeypatch):
        """The wide retry must get its OWN, separate (larger) budget — not
        ASTAP_TIMEOUT_SEC, the narrow attempt's own timeout."""
        monkeypatch.setattr(config, "ASTAP_TIMEOUT_SEC", 42.0)
        monkeypatch.setattr(config, "ASTAP_WIDE_SEARCH_TIMEOUT_SEC", 999.0)

        run_mock = MagicMock(return_value=MagicMock(
            returncode=0, stdout="Solution found", stderr="",
        ))
        with patch("modules.astrometry.subprocess.run", run_mock):
            outcome = _run_astap_attempt(_FITS_PATH, None, wide_radius_deg=30.0)

        assert outcome == "solved"
        assert run_mock.call_args.kwargs["timeout"] == 999.0

    def test_wide_retry_timeout_is_not_further_retried(self, monkeypatch):
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
            result = _run_astap(_FITS_PATH, None)

        assert result is False
        assert run_mock.call_count == 2  # narrow (no_solution), then wide (timeout) — no third attempt

    def test_full_retry_flow_uses_distinct_timeouts_for_each_attempt(self, monkeypatch):
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
            result = _run_astap(_FITS_PATH, None)

        assert result is True
        assert [c.kwargs["timeout"] for c in run_mock.call_args_list] == [42.0, 999.0]


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


# ---------------------------------------------------------------------------
# Test 9 — Streak masking (satellite trails / diffraction spikes)
#
# Real incident, 2026-08-07, T_CrB_Light_L_60_2024-05-28T19-06-10.fits: a
# full-frame satellite trail fragmented into several small, roundish "stars"
# at the ordinary extraction settings. _build_streak_mask()'s coarse,
# non-deblended pre-pass finds long+elongated coarse features and zeroes
# their pixels in data_sub before the real extraction runs.
# ---------------------------------------------------------------------------

def _make_coarse_object(a: float, xmin: int, xmax: int, ymin: int, ymax: int, b: float = 1.0) -> np.ndarray:
    obj = np.zeros(1, dtype=[
        ("a", np.float64), ("b", np.float64),
        ("xmin", np.int32), ("xmax", np.int32),
        ("ymin", np.int32), ("ymax", np.int32),
    ])
    obj["a"] = a
    obj["b"] = b
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
