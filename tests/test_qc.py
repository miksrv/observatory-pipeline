"""
tests/test_qc.py — Unit tests for modules/qc.py

All external I/O is mocked:
  - astropy.io.fits.open      → controlled HDU with small numpy image + header dict
  - sep.Background            → returns a mock background object
  - sep.extract               → returns a numpy structured array of fake sources
  - sep.sum_circle            → returns (flux, fluxerr, flags) arrays
  - astroscrappy.detect_cosmics → returns (zero bool mask, clean array)
  - modules.fits_header.extract_headers → returns a minimal header dict
  - shutil.move               → intercepted so no real filesystem moves happen
    (except in tests that explicitly test filesystem behaviour, which create
     real temporary FITS files)

All tests are async because qc.analyze() is declared async.
"""

from __future__ import annotations

import os
import shutil
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch, call

import numpy as np
import pytest
import pytest_asyncio

# ---------------------------------------------------------------------------
# Module under test
# ---------------------------------------------------------------------------
from modules import qc


# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

_IMAGE_SHAPE = (64, 64)
_N_SOURCES = 15       # above QC_STARS_MIN (10)
_A_NORMAL = 1.5       # semi-major axis in pixels → elongation ≈ 1.07, FWHM ≈ 2.4 px
_B_NORMAL = 1.4       # semi-minor axis in pixels
_A_BLUR   = 20.0      # large semi-axes → FWHM ≈ 32 px  × 1.0 arcsec/px = 32 arcsec >> 8
_B_BLUR   = 19.0
_A_TRAIL  = 4.0       # elongation = 4.0 >> QC_ELONGATION_MAX (2.0), FWHM ≈ 6.9 px < 8.0 arcsec threshold
_B_TRAIL  = 1.0

_SKY_BACK   = 800.0
_SKY_RMS    = 20.0
_FITS_PATH  = "/fake/fits/incoming/frame_test.fits"
_OBJECT_NAME = "M51"


# ---------------------------------------------------------------------------
# Helpers — build a numpy structured array matching the sep.extract() layout
# ---------------------------------------------------------------------------

def _make_sources(n: int, a: float, b: float) -> np.ndarray:
    """Return a structured array that mimics sep.extract() output."""
    dtype = np.dtype([
        ("x",    np.float64),
        ("y",    np.float64),
        ("a",    np.float64),
        ("b",    np.float64),
        ("flux", np.float64),
    ])
    sources = np.zeros(n, dtype=dtype)
    sources["x"]    = np.linspace(5.0, 59.0, n)
    sources["y"]    = np.linspace(5.0, 59.0, n)
    sources["a"]    = a
    sources["b"]    = b
    sources["flux"] = 10000.0
    return sources


def _make_image() -> np.ndarray:
    """Return a small float64 C-contiguous image array."""
    return np.ascontiguousarray(
        np.random.default_rng(42).poisson(800, _IMAGE_SHAPE).astype(np.float64)
    )


# ---------------------------------------------------------------------------
# Mock factories
# ---------------------------------------------------------------------------

class _FakeBackground:
    """
    Minimal stand-in for sep.Background.

    The critical property is that ``ndarray - fake_bkg`` works correctly.
    Python's operator dispatch for ``data - bkg`` calls
    ``type(bkg).__rsub__(bkg, data)`` only when ``data.__sub__`` returns
    NotImplemented.  NumPy's ndarray.__sub__ normally handles unknown types
    by delegating to the right-hand operand's __rsub__, so we must define it
    on the *class*, not the instance.
    """

    def __init__(self, back: float = _SKY_BACK, rms: float = _SKY_RMS) -> None:
        self.globalback: float = back
        self.globalrms:  float = rms
        self._back = back

    def __rsub__(self, other: np.ndarray) -> np.ndarray:
        return np.ascontiguousarray(other - self._back)


def _make_bkg_mock() -> _FakeBackground:
    """Return a background object whose subtraction works with numpy arrays."""
    return _FakeBackground()


class _FakeHDU:
    """Minimal stand-in for an astropy PrimaryHDU."""

    def __init__(self, image: np.ndarray, header: dict[str, Any]) -> None:
        self.data   = image
        self.header = _FakeHeader(header)


class _FakeHeader:
    """Minimal stand-in for astropy.io.fits.Header that supports .get() and []."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)


class _FakeHDUL:
    """Context-manager wrapper around a list of HDUs."""

    def __init__(self, hdus: list[_FakeHDU]) -> None:
        self._hdus = hdus

    def __enter__(self) -> list[_FakeHDU]:
        return self._hdus

    def __exit__(self, *args: Any) -> bool:
        return False


def _make_hdu_mock(image: np.ndarray, header: dict[str, Any]) -> _FakeHDUL:
    """Return a fits.open()-compatible context manager with a single HDU."""
    return _FakeHDUL([_FakeHDU(image, header)])


def _sum_circle_return(n: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Simulate sep.sum_circle returning healthy flux and flux-error arrays."""
    flux    = np.full(n, 5000.0, dtype=np.float64)
    fluxerr = np.full(n,   50.0, dtype=np.float64)   # SNR = 100 per source
    flags   = np.zeros(n, dtype=np.int32)
    return flux, fluxerr, flags


# ---------------------------------------------------------------------------
# Context manager: patch all external calls at once
# ---------------------------------------------------------------------------

@contextmanager
def _patch_qc(
    sources: np.ndarray,
    header: dict[str, Any] | None = None,
    header_info: dict[str, Any] | None = None,
    cr_raises: bool = False,
):
    """
    Patch every external dependency of qc.py in one shot.

    Parameters
    ----------
    sources:
        Structured array returned by sep.extract().
    header:
        Raw FITS header dict (returned from fits.open()[0].header).
    header_info:
        Dict returned by fits_header.extract_headers() (provides object_name).
    cr_raises:
        If True, astroscrappy.detect_cosmics raises RuntimeError.
    """
    image = _make_image()

    if header is None:
        # Provide XPIXSZ + FOCALLEN so plate scale resolves to ~1 arcsec/px
        # XPIXSZ=4.83 µm, FOCALLEN=997 mm → 206265 * 0.00483 / 997 ≈ 1.0 arcsec/px
        header = {"XPIXSZ": 4.83, "FOCALLEN": 997.0, "PIXSCALE": None}

    if header_info is None:
        header_info = {
            "object_name": _OBJECT_NAME,
            "instrument":  {"focal_length_mm": 997.0},
        }

    hdul_mock = _make_hdu_mock(image, header)
    bkg_mock  = _make_bkg_mock()
    n = len(sources)

    def _detect_cosmics(data):
        if cr_raises:
            raise RuntimeError("astroscrappy intentional failure")
        crmask = np.zeros(_IMAGE_SHAPE, dtype=bool)
        crmask[0, 0] = True   # exactly 1 CR pixel → cr_fraction = 1/4096
        return crmask, data.astype(np.float32)

    with (
        patch("modules.qc.fits.open", return_value=hdul_mock),
        patch("modules.qc.sep.Background", return_value=bkg_mock),
        patch("modules.qc.sep.extract",    return_value=sources),
        patch("modules.qc.sep.sum_circle", return_value=_sum_circle_return(n)),
        patch("modules.qc.astroscrappy.detect_cosmics", side_effect=_detect_cosmics),
        patch("modules.qc.extract_headers",             return_value=header_info),
    ):
        yield


# ---------------------------------------------------------------------------
# Test 1 — OK frame
# ---------------------------------------------------------------------------

class TestOkFlag:
    @pytest.mark.asyncio
    async def test_ok_flag(self):
        sources = _make_sources(_N_SOURCES, _A_NORMAL, _B_NORMAL)
        with _patch_qc(sources):
            result = await qc.analyze(_FITS_PATH)

        assert result["quality_flag"] == "OK"
        assert result["rejected_path"] is None

    @pytest.mark.asyncio
    async def test_ok_returns_numeric_metrics(self):
        sources = _make_sources(_N_SOURCES, _A_NORMAL, _B_NORMAL)
        with _patch_qc(sources):
            result = await qc.analyze(_FITS_PATH)

        assert result["fwhm_median"] is not None
        assert result["fwhm_median"] > 0.0
        assert result["elongation_median"] is not None
        assert result["elongation_median"] >= 1.0
        assert result["snr_median"] is not None
        assert result["snr_median"] > 0.0
        assert result["sky_background"] == _SKY_BACK
        assert result["sky_sigma"] == _SKY_RMS
        # star_count is filtered, so it may be <= _N_SOURCES
        # With normal FWHM and elongation, all sources should pass filters
        assert result["star_count"] is not None
        assert result["star_count"] > 0


# ---------------------------------------------------------------------------
# Test 2 — BLUR flag
# ---------------------------------------------------------------------------

class TestBlurFlag:
    @pytest.mark.asyncio
    async def test_blur_flag(self):
        """
        Large semi-axes produce FWHM >> QC_FWHM_MAX_ARCSEC (8.0).
        Plate scale is ~1 arcsec/px (XPIXSZ=4.83 µm, FOCALLEN=997 mm).
        FWHM_px for a=20, b=19 ≈ 2.355 * sqrt((400+361)/2) ≈ 32.5 px
        → ~32.5 arcsec >> 8.0 arcsec threshold.
        """
        sources = _make_sources(_N_SOURCES, _A_BLUR, _B_BLUR)
        with _patch_qc(sources):
            result = await qc.analyze(_FITS_PATH)

        assert result["quality_flag"] == "BLUR"
        assert result["fwhm_unit"] == "arcsec"
        assert result["fwhm_median"] > 8.0

    @pytest.mark.asyncio
    async def test_blur_no_flag_when_unit_is_pixels(self):
        """
        Without plate scale info, FWHM is in pixels and the BLUR rule is
        suppressed — we cannot compare pixels to the arcsec threshold.
        """
        sources = _make_sources(_N_SOURCES, _A_BLUR, _B_BLUR)
        # Header has no pixel size or focal length — plate scale unknown
        header_no_scale = {"PIXSCALE": None}
        with _patch_qc(sources, header=header_no_scale):
            result = await qc.analyze(_FITS_PATH)

        assert result["fwhm_unit"] == "pixels"
        # Must NOT be BLUR when unit is pixels (could be OK, TRAIL, etc.)
        assert result["quality_flag"] != "BLUR"


# ---------------------------------------------------------------------------
# Test 3 — TRAIL flag
# ---------------------------------------------------------------------------

class TestTrailFlag:
    @pytest.mark.asyncio
    async def test_trail_flag(self):
        """a=4, b=1 → elongation=4.0 > QC_ELONGATION_MAX (2.0); FWHM ≈ 6.9 px stays under 8.0 arcsec."""
        sources = _make_sources(_N_SOURCES, _A_TRAIL, _B_TRAIL)
        with _patch_qc(sources):
            result = await qc.analyze(_FITS_PATH)

        assert result["quality_flag"] == "TRAIL"
        assert result["elongation_median"] > 2.0


# ---------------------------------------------------------------------------
# Test 4 — LOW_STARS flag
# ---------------------------------------------------------------------------

class TestLowStarsFlag:
    @pytest.mark.asyncio
    async def test_low_stars_flag(self):
        """5 sources < QC_STARS_MIN (10) → LOW_STARS."""
        sources = _make_sources(5, _A_NORMAL, _B_NORMAL)
        with _patch_qc(sources):
            result = await qc.analyze(_FITS_PATH)

        assert result["quality_flag"] == "LOW_STARS"
        assert result["star_count"] == 5

    @pytest.mark.asyncio
    async def test_exactly_stars_min_is_not_low(self):
        """Exactly QC_STARS_MIN sources should pass (not LOW_STARS)."""
        import config
        sources = _make_sources(config.QC_STARS_MIN, _A_NORMAL, _B_NORMAL)
        with _patch_qc(sources):
            result = await qc.analyze(_FITS_PATH)

        assert result["quality_flag"] != "LOW_STARS"


# ---------------------------------------------------------------------------
# Test 5 — BAD flag (multiple issues)
# ---------------------------------------------------------------------------

class TestBadFlag:
    @pytest.mark.asyncio
    async def test_bad_flag_blur_and_trail(self):
        """
        a=20, b=1 → FWHM huge (BLUR) AND elongation=20 (TRAIL)
        → two issues → BAD.
        """
        sources = _make_sources(_N_SOURCES, 20.0, 1.0)
        with _patch_qc(sources):
            result = await qc.analyze(_FITS_PATH)

        assert result["quality_flag"] == "BAD"

    @pytest.mark.asyncio
    async def test_blur_with_low_stars_is_blur(self):
        """
        BLUR + few stars → BLUR (not BAD).
        
        LOW_STARS is only counted when BLUR and TRAIL are both false,
        because a blurred or trailed image naturally explains why
        star_count is low (sources get filtered out). This prevents
        double-counting the same underlying issue.
        """
        sources = _make_sources(5, _A_BLUR, _B_BLUR)
        with _patch_qc(sources):
            result = await qc.analyze(_FITS_PATH)

        # BLUR is the root cause; LOW_STARS is a consequence
        assert result["quality_flag"] == "BLUR"


# ---------------------------------------------------------------------------
# Test 6 — Rejected file is actually moved to the correct path
# ---------------------------------------------------------------------------

class TestRejectedFileMoved:
    @pytest.mark.asyncio
    async def test_rejected_file_moved_to_correct_path(self, tmp_path):
        """
        When the frame is rejected, the file must be moved from its source
        path to {FITS_REJECTED}/{object_name}/{FLAG}_{filename}.
        We use tmp_path to avoid touching the real filesystem.
        """
        import config

        # Write a real (tiny) file we can actually move
        src_file = tmp_path / "frame_test.fits"
        src_file.write_bytes(b"SIMPLE  =                    T")  # minimal content

        rejected_root = tmp_path / "rejected"
        rejected_root.mkdir()

        sources = _make_sources(_N_SOURCES, _A_TRAIL, _B_TRAIL)
        header_info = {"object_name": "NGC_1234", "instrument": {"focal_length_mm": None}}

        with (
            patch("modules.qc.fits.open", return_value=_make_hdu_mock(_make_image(), {})),
            patch("modules.qc.sep.Background", return_value=_make_bkg_mock()),
            patch("modules.qc.sep.extract",    return_value=sources),
            patch("modules.qc.sep.sum_circle",
                  return_value=_sum_circle_return(len(sources))),
            patch("modules.qc.astroscrappy.detect_cosmics",
                  return_value=(np.zeros(_IMAGE_SHAPE, bool), _make_image().astype(np.float32))),
            patch("modules.qc.extract_headers", return_value=header_info),
            patch.object(config, "FITS_REJECTED", str(rejected_root)),
        ):
            result = await qc.analyze(str(src_file))

        assert result["quality_flag"] == "TRAIL"
        rejected_path = result["rejected_path"]
        assert rejected_path is not None
        assert os.path.isfile(rejected_path)
        assert os.path.basename(rejected_path) == f"TRAIL_{src_file.name}"
        assert not src_file.exists(), "Source file should have been moved"


# ---------------------------------------------------------------------------
# Test 7 — Destination directory is created automatically
# ---------------------------------------------------------------------------

class TestDestinationDirectoryCreated:
    @pytest.mark.asyncio
    async def test_destination_directory_created(self, tmp_path):
        """
        The destination directory does not exist before analyze() runs.
        It must be created automatically.
        """
        import config

        src_file = tmp_path / "frame_dir_test.fits"
        src_file.write_bytes(b"SIMPLE  =                    T")

        rejected_root = tmp_path / "rejected_new"
        # Intentionally NOT creating rejected_root — qc.py must create it

        sources = _make_sources(_N_SOURCES, _A_TRAIL, _B_TRAIL)
        header_info = {"object_name": "Andromeda", "instrument": {"focal_length_mm": None}}

        with (
            patch("modules.qc.fits.open",
                  return_value=_make_hdu_mock(_make_image(), {})),
            patch("modules.qc.sep.Background", return_value=_make_bkg_mock()),
            patch("modules.qc.sep.extract",    return_value=sources),
            patch("modules.qc.sep.sum_circle",
                  return_value=_sum_circle_return(len(sources))),
            patch("modules.qc.astroscrappy.detect_cosmics",
                  return_value=(np.zeros(_IMAGE_SHAPE, bool), _make_image().astype(np.float32))),
            patch("modules.qc.extract_headers", return_value=header_info),
            patch.object(config, "FITS_REJECTED", str(rejected_root)),
        ):
            result = await qc.analyze(str(src_file))

        assert result["rejected_path"] is not None
        expected_dir = rejected_root / "Andromeda"
        assert expected_dir.is_dir(), "Rejected sub-directory was not created"


# ---------------------------------------------------------------------------
# Test 8 — cr_fraction is present and in [0, 1]
# ---------------------------------------------------------------------------

class TestCrFraction:
    @pytest.mark.asyncio
    async def test_cr_fraction_in_result(self):
        sources = _make_sources(_N_SOURCES, _A_NORMAL, _B_NORMAL)
        with _patch_qc(sources):
            result = await qc.analyze(_FITS_PATH)

        assert "cr_fraction" in result
        assert result["cr_fraction"] is not None
        assert isinstance(result["cr_fraction"], float)
        assert 0.0 <= result["cr_fraction"] <= 1.0

    @pytest.mark.asyncio
    async def test_cr_fraction_value_matches_mask(self):
        """
        _patch_qc sets exactly 1 True pixel in the mask for a 64×64 image.
        cr_fraction should equal 1/4096.
        """
        sources = _make_sources(_N_SOURCES, _A_NORMAL, _B_NORMAL)
        with _patch_qc(sources):
            result = await qc.analyze(_FITS_PATH)

        expected = 1.0 / (_IMAGE_SHAPE[0] * _IMAGE_SHAPE[1])
        assert abs(result["cr_fraction"] - expected) < 1e-10


# ---------------------------------------------------------------------------
# Test 9 — astroscrappy failure does not crash the pipeline
# ---------------------------------------------------------------------------

class TestAstroscrappyFailure:
    @pytest.mark.asyncio
    async def test_astroscrappy_failure_does_not_crash(self):
        sources = _make_sources(_N_SOURCES, _A_NORMAL, _B_NORMAL)
        with _patch_qc(sources, cr_raises=True):
            result = await qc.analyze(_FITS_PATH)

        # Pipeline must not raise; cr_fraction must be None
        assert result["cr_fraction"] is None
        # Other metrics should still be computed
        assert result["quality_flag"] == "OK"
        assert result["fwhm_median"] is not None
        assert result["elongation_median"] is not None


# ---------------------------------------------------------------------------
# Test 10 — Degenerate frame with fewer than 3 detected sources
# ---------------------------------------------------------------------------

class TestDegenerateFrameFewSources:
    @pytest.mark.asyncio
    async def test_fewer_than_3_sources_returns_none_metrics(self):
        """
        When sep.extract returns fewer than 3 sources, FWHM / elongation /
        SNR cannot be reliably estimated.  All metric fields must be None
        and the flag must be LOW_STARS.
        """
        sources = _make_sources(2, _A_NORMAL, _B_NORMAL)
        with _patch_qc(sources):
            result = await qc.analyze(_FITS_PATH)

        assert result["quality_flag"] == "LOW_STARS"
        assert result["fwhm_median"] is None
        assert result["fwhm_unit"] == "pixels"   # default when metrics absent
        assert result["elongation_median"] is None
        assert result["snr_median"] is None
        assert result["star_count"] == 2

    @pytest.mark.asyncio
    async def test_zero_sources_returns_low_stars(self):
        sources = _make_sources(0, _A_NORMAL, _B_NORMAL)
        with _patch_qc(sources):
            result = await qc.analyze(_FITS_PATH)

        assert result["quality_flag"] == "LOW_STARS"
        assert result["star_count"] == 0


# ---------------------------------------------------------------------------
# Test 11 — Returned dict always has all required keys
# ---------------------------------------------------------------------------

class TestResultHasAllKeys:
    REQUIRED_KEYS = frozenset({
        "quality_flag",
        "fwhm_median",
        "fwhm_unit",
        "elongation_median",
        "snr_median",
        "sky_background",
        "sky_sigma",
        "star_count",
        "cr_fraction",
        "rejected_path",
    })

    @pytest.mark.asyncio
    async def test_ok_frame_has_all_keys(self):
        sources = _make_sources(_N_SOURCES, _A_NORMAL, _B_NORMAL)
        with _patch_qc(sources):
            result = await qc.analyze(_FITS_PATH)

        assert self.REQUIRED_KEYS == set(result.keys())

    @pytest.mark.asyncio
    async def test_rejected_frame_has_all_keys(self):
        sources = _make_sources(_N_SOURCES, _A_TRAIL, _B_TRAIL)
        with (
            _patch_qc(sources),
            patch("modules.qc._move_rejected", return_value="/fake/rejected/path"),
        ):
            result = await qc.analyze(_FITS_PATH)

        assert self.REQUIRED_KEYS == set(result.keys())

    @pytest.mark.asyncio
    async def test_degenerate_frame_has_all_keys(self):
        sources = _make_sources(0, _A_NORMAL, _B_NORMAL)
        with (
            _patch_qc(sources),
            patch("modules.qc._move_rejected", return_value="/fake/rejected/path"),
        ):
            result = await qc.analyze(_FITS_PATH)

        assert self.REQUIRED_KEYS == set(result.keys())

    @pytest.mark.asyncio
    async def test_astroscrappy_failure_has_all_keys(self):
        sources = _make_sources(_N_SOURCES, _A_NORMAL, _B_NORMAL)
        with _patch_qc(sources, cr_raises=True):
            result = await qc.analyze(_FITS_PATH)

        assert self.REQUIRED_KEYS == set(result.keys())


# ---------------------------------------------------------------------------
# Test 12 — Plate scale logic
# ---------------------------------------------------------------------------

class TestPlateScale:
    @pytest.mark.asyncio
    async def test_fwhm_unit_arcsec_when_scale_known(self):
        sources = _make_sources(_N_SOURCES, _A_NORMAL, _B_NORMAL)
        with _patch_qc(sources):
            result = await qc.analyze(_FITS_PATH)

        assert result["fwhm_unit"] == "arcsec"

    @pytest.mark.asyncio
    async def test_fwhm_unit_pixels_when_scale_unknown(self):
        sources = _make_sources(_N_SOURCES, _A_NORMAL, _B_NORMAL)
        with _patch_qc(sources, header={"PIXSCALE": None}):
            result = await qc.analyze(_FITS_PATH)

        assert result["fwhm_unit"] == "pixels"

    @pytest.mark.asyncio
    async def test_direct_pixscale_keyword_used(self):
        """
        When PIXSCALE is present as a direct arcsec/px value, it should be
        used instead of deriving from XPIXSZ + FOCALLEN.
        """
        sources = _make_sources(_N_SOURCES, _A_NORMAL, _B_NORMAL)
        # PIXSCALE=1.5 arcsec/px directly; no XPIXSZ/FOCALLEN
        header = {"PIXSCALE": 1.5}
        with _patch_qc(sources, header=header):
            result = await qc.analyze(_FITS_PATH)

        assert result["fwhm_unit"] == "arcsec"
        # FWHM in pixels for a=1.5, b=1.4 ≈ 2.4 px → in arcsec ≈ 3.6
        assert result["fwhm_median"] is not None
        assert result["fwhm_median"] > 0.0
