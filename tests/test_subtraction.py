"""
tests/test_subtraction.py — Unit tests for modules/subtraction.py

Covers:
  - _find_archive_frames(): same-filter matching + fallback to any filter
  - _load_frame_data(): loading a 2-D image HDU, gracefully returning None
  - _align_frame(): success/failure wrapping of astroalign.register()
  - _detect_diff_sources(): SEP detection on a synthetic difference image
  - _pixel_to_sky(): WCS pixel -> sky conversion
  - run(): end-to-end orchestration

run()'s own control flow is tested by monkeypatching its private helpers
directly (_find_archive_frames / _load_frame_data / _align_frame /
_detect_diff_sources / _pixel_to_sky). This keeps these tests fast and
focused on subtraction.py's own orchestration logic rather than re-testing
astroalign/sep themselves, and lets us simulate scenarios (e.g. reference
frames with a different pixel resolution than the new frame) without
needing real multi-megapixel FITS fixtures.

asyncio_mode = auto is set in pytest.ini, so async tests need no decorator.
"""

from __future__ import annotations

import numpy as np
import pytest
from astropy.io import fits
from astropy.wcs import WCS as AstropyWCS

import config
from modules import subtraction


# ---------------------------------------------------------------------------
# _find_archive_frames
# ---------------------------------------------------------------------------

class TestFindArchiveFrames:

    def test_missing_directory_returns_empty(self, tmp_path):
        assert subtraction._find_archive_frames(str(tmp_path / "does_not_exist"), None) == []

    def test_no_filter_returns_all_frames(self, tmp_path):
        for name in ("a.fits", "b.fit", "c.FITS"):
            (tmp_path / name).write_bytes(b"x")

        result = subtraction._find_archive_frames(str(tmp_path), None)

        assert len(result) == 3

    def test_filter_token_matching_with_enough_same_filter_frames(self, tmp_path):
        # 3 Ha frames (>= default SUBTRACTION_MIN_FRAMES=3) + 1 R frame.
        for name in (
            "M51_L_Ha_120_2024-01-01T00-00-00.fits",
            "M51_L_Ha_120_2024-01-02T00-00-00.fits",
            "M51_L_Ha_120_2024-01-03T00-00-00.fits",
            "M51_L_R_120_2024-01-04T00-00-00.fits",
        ):
            (tmp_path / name).write_bytes(b"x")

        result = subtraction._find_archive_frames(str(tmp_path), "Ha")

        assert len(result) == 3
        assert all("_HA_" in p.upper() for p in result)

    def test_filter_token_falls_back_to_all_when_too_few_same_filter(self, tmp_path):
        # Only 2 Ha frames (< SUBTRACTION_MIN_FRAMES=3) + 2 R frames — must
        # fall back to cross-filter subtraction using all 4 frames.
        for name in (
            "M51_L_Ha_120_a.fits",
            "M51_L_Ha_120_b.fits",
            "M51_L_R_120_c.fits",
            "M51_L_R_120_d.fits",
        ):
            (tmp_path / name).write_bytes(b"x")

        result = subtraction._find_archive_frames(str(tmp_path), "Ha")

        assert len(result) == 4


# ---------------------------------------------------------------------------
# _load_frame_data
# ---------------------------------------------------------------------------

class TestLoadFrameData:

    def test_loads_2d_image_hdu(self, tmp_path):
        data = np.arange(12, dtype=np.float64).reshape(3, 4)
        path = tmp_path / "frame.fits"
        fits.PrimaryHDU(data).writeto(path)

        loaded = subtraction._load_frame_data(str(path))

        assert loaded is not None
        assert loaded.shape == (3, 4)
        assert loaded.dtype == np.float32

    def test_missing_file_returns_none(self, tmp_path):
        assert subtraction._load_frame_data(str(tmp_path / "missing.fits")) is None

    def test_header_only_hdu_returns_none(self, tmp_path):
        path = tmp_path / "empty.fits"
        fits.PrimaryHDU().writeto(path)

        assert subtraction._load_frame_data(str(path)) is None


# ---------------------------------------------------------------------------
# _align_frame
# ---------------------------------------------------------------------------

class TestAlignFrame:

    def test_align_success_returns_registered_array(self, monkeypatch):
        import astroalign
        fake_aligned = np.zeros((5, 5), dtype=np.float64)
        monkeypatch.setattr(astroalign, "register", lambda source, target: (fake_aligned, None))

        result = subtraction._align_frame(np.ones((3, 3)), np.ones((5, 5)))

        assert result is not None
        assert result.shape == (5, 5)
        assert result.dtype == np.float32

    def test_align_failure_returns_none(self, monkeypatch):
        import astroalign

        def _raise(source, target):
            raise ValueError("not enough matching triangles")

        monkeypatch.setattr(astroalign, "register", _raise)

        result = subtraction._align_frame(np.ones((3, 3)), np.ones((5, 5)))

        assert result is None


# ---------------------------------------------------------------------------
# _detect_diff_sources
# ---------------------------------------------------------------------------

class TestDetectDiffSources:

    def test_detects_injected_blob(self):
        rng = np.random.default_rng(42)
        diff = rng.normal(loc=0.0, scale=5.0, size=(100, 100))

        # Inject a bright Gaussian blob well above SUBTRACTION_DETECT_SIGMA * rms
        yy, xx = np.mgrid[0:100, 0:100]
        blob = 800.0 * np.exp(-(((xx - 60) ** 2 + (yy - 40) ** 2) / (2 * 3.0 ** 2)))
        diff = diff + blob

        candidates = subtraction._detect_diff_sources(diff)

        assert len(candidates) >= 1
        closest = min(candidates, key=lambda c: (c["x"] - 60) ** 2 + (c["y"] - 40) ** 2)
        assert abs(closest["x"] - 60) < 3
        assert abs(closest["y"] - 40) < 3
        assert closest["flux"] > 0

    def test_pure_noise_finds_nothing(self):
        rng = np.random.default_rng(7)
        diff = rng.normal(loc=0.0, scale=5.0, size=(100, 100))

        candidates = subtraction._detect_diff_sources(diff)

        assert candidates == []

    def test_zero_rms_returns_empty(self):
        # A perfectly flat image has rms == 0 — must not raise or divide by zero.
        diff = np.zeros((50, 50))

        assert subtraction._detect_diff_sources(diff) == []

    def test_masked_blob_is_not_detected(self):
        """A candidate whose pixels fall entirely inside the mask must be suppressed."""
        rng = np.random.default_rng(42)
        diff = rng.normal(loc=0.0, scale=5.0, size=(100, 100))
        yy, xx = np.mgrid[0:100, 0:100]
        blob = 800.0 * np.exp(-(((xx - 60) ** 2 + (yy - 40) ** 2) / (2 * 3.0 ** 2)))
        diff = diff + blob

        mask = np.zeros((100, 100), dtype=bool)
        mask[20:60, 40:80] = True  # covers the blob at (x=60, y=40)

        assert subtraction._detect_diff_sources(diff, mask=mask) == []

    def test_mask_elsewhere_does_not_suppress_blob(self):
        """A mask that doesn't overlap the blob must not affect detection."""
        rng = np.random.default_rng(42)
        diff = rng.normal(loc=0.0, scale=5.0, size=(100, 100))
        yy, xx = np.mgrid[0:100, 0:100]
        blob = 800.0 * np.exp(-(((xx - 60) ** 2 + (yy - 40) ** 2) / (2 * 3.0 ** 2)))
        diff = diff + blob

        mask = np.zeros((100, 100), dtype=bool)
        mask[0:10, 0:10] = True  # nowhere near the blob

        candidates = subtraction._detect_diff_sources(diff, mask=mask)

        assert len(candidates) >= 1

    def test_mismatched_mask_shape_is_ignored(self):
        """A mask whose shape doesn't match diff must be silently ignored, not raise."""
        rng = np.random.default_rng(42)
        diff = rng.normal(loc=0.0, scale=5.0, size=(100, 100))
        yy, xx = np.mgrid[0:100, 0:100]
        blob = 800.0 * np.exp(-(((xx - 60) ** 2 + (yy - 40) ** 2) / (2 * 3.0 ** 2)))
        diff = diff + blob

        wrong_shape_mask = np.zeros((10, 10), dtype=bool)

        candidates = subtraction._detect_diff_sources(diff, mask=wrong_shape_mask)

        assert len(candidates) >= 1

    def test_fwhm_floor_rejects_sharper_than_floor_candidate(self):
        """
        A hot/warm pixel candidate (unresolved, near-delta-function profile —
        much narrower than any real PSF-shaped blob) must be dropped when a
        fwhm_min_px floor is given, even though it clears the detection
        threshold and minarea just as easily as a real source would.
        """
        rng = np.random.default_rng(3)
        diff = rng.normal(loc=0.0, scale=5.0, size=(100, 100))
        # A near-single-pixel spike: sigma=0.5px is far too sharp to be a real
        # PSF-convolved point source at any normal seeing/plate scale.
        yy, xx = np.mgrid[0:100, 0:100]
        spike = 800.0 * np.exp(-(((xx - 60) ** 2 + (yy - 40) ** 2) / (2 * 0.5 ** 2)))
        diff = diff + spike

        # Without a floor, the sharp spike is detected like any other source.
        assert subtraction._detect_diff_sources(diff) != []

        # With a floor well above the spike's own FWHM, it must be rejected.
        assert subtraction._detect_diff_sources(diff, fwhm_min_px=5.0) == []

    def test_fwhm_floor_keeps_candidate_at_or_above_floor(self):
        """A candidate whose FWHM already meets the floor must still pass."""
        rng = np.random.default_rng(42)
        diff = rng.normal(loc=0.0, scale=5.0, size=(100, 100))
        yy, xx = np.mgrid[0:100, 0:100]
        blob = 800.0 * np.exp(-(((xx - 60) ** 2 + (yy - 40) ** 2) / (2 * 3.0 ** 2)))
        diff = diff + blob

        candidates = subtraction._detect_diff_sources(diff, fwhm_min_px=1.0)

        assert len(candidates) >= 1

    def test_central_candidate_is_not_near_edge(self):
        """
        100x100 diff, default EDGE_MARGIN_FRAC=0.1 → 10px margin. A blob at
        (60, 40) sits well inside [10, 90] on both axes.
        """
        rng = np.random.default_rng(42)
        diff = rng.normal(loc=0.0, scale=5.0, size=(100, 100))
        yy, xx = np.mgrid[0:100, 0:100]
        blob = 800.0 * np.exp(-(((xx - 60) ** 2 + (yy - 40) ** 2) / (2 * 3.0 ** 2)))
        diff = diff + blob

        candidates = subtraction._detect_diff_sources(diff)

        assert len(candidates) >= 1
        assert all(c["near_edge"] is False for c in candidates)

    def test_corner_candidate_is_filtered_out(self):
        """A blob at (5, 5) on a 100x100 diff falls inside the 10px margin
        and is now rejected — coma residuals at the edge are not real
        transients (see EDGE_MARGIN_FRAC filtering in _detect_diff_sources).
        """
        rng = np.random.default_rng(9)
        diff = rng.normal(loc=0.0, scale=5.0, size=(100, 100))
        yy, xx = np.mgrid[0:100, 0:100]
        blob = 800.0 * np.exp(-(((xx - 5) ** 2 + (yy - 5) ** 2) / (2 * 2.0 ** 2)))
        diff = diff + blob

        candidates = subtraction._detect_diff_sources(diff)

        # The corner blob must NOT appear in the output — it was in the
        # edge zone and should have been filtered.
        corner_hits = [c for c in candidates if (c["x"] - 5) ** 2 + (c["y"] - 5) ** 2 < 25]
        assert len(corner_hits) == 0


# ---------------------------------------------------------------------------
# Streak masking on the difference image (docs/ISSUES.md-style real incident,
# 2026-08-07, T_CrB test frames: a satellite trail present in the new frame
# but absent from the reference stack fragmented into 42 separate
# elongation>3 candidates on the diff image — each individually
# classifiable by anomaly_detector.py as its own SPACE_DEBRIS anomaly).
# Uses real (unmocked) sep, same style as TestDetectDiffSources above.
# ---------------------------------------------------------------------------

class TestDetectDiffSourcesStreakMasking:

    def test_streak_segments_are_suppressed_round_blob_survives(self):
        """
        Two disjoint elongated strips (simulating a satellite trail that
        fragmented into disconnected segments — see
        _build_streak_mask()'s docstring) must be suppressed, while an
        ordinary round transient elsewhere in the same diff image is
        unaffected.
        """
        rng = np.random.default_rng(11)
        diff = rng.normal(loc=0.0, scale=5.0, size=(300, 300))

        # Two disjoint 100x4px vertical strips ~60px apart — elongation ~25,
        # bbox diagonal ~100px. At pixel_scale_arcsec=1.0, that's 100" —
        # well past the default STREAK_MIN_LENGTH_ARCSEC (30").
        diff[20:120, 148:152] += 400.0
        diff[180:280, 148:152] += 400.0

        # A genuine round transient far from the streak.
        yy, xx = np.mgrid[0:300, 0:300]
        blob = 600.0 * np.exp(-(((xx - 250) ** 2 + (yy - 250) ** 2) / (2 * 3.0 ** 2)))
        diff = diff + blob

        candidates = subtraction._detect_diff_sources(diff, pixel_scale_arcsec=1.0)

        # No candidate should land inside the masked streak columns.
        assert not any(140 <= c["x"] <= 160 for c in candidates)
        # The round transient must still be found.
        assert any(abs(c["x"] - 250) < 3 and abs(c["y"] - 250) < 3 for c in candidates)

    def test_without_pixel_scale_short_strip_is_not_masked(self):
        """
        A strip whose bbox diagonal falls under the fixed 200px fallback
        floor (used when pixel_scale_arcsec is None) must be left alone —
        this is what keeps an ordinary elongated blend from being treated
        as a streak when no plate scale is available at all.
        """
        rng = np.random.default_rng(5)
        diff = rng.normal(loc=0.0, scale=5.0, size=(300, 300))
        diff[100:160, 148:152] += 400.0  # ~60px tall — below the 200px floor

        candidates = subtraction._detect_diff_sources(diff)  # pixel_scale_arcsec=None

        assert any(140 <= c["x"] <= 160 and 100 <= c["y"] <= 160 for c in candidates)


# ---------------------------------------------------------------------------
# _build_saturation_mask (docs/ISSUES.md #1, #2)
# ---------------------------------------------------------------------------

class TestBuildSaturationMask:

    def test_no_saturation_returns_none(self):
        new_data = np.full((20, 20), 100.0, dtype=np.float32)
        refs = [np.full((20, 20), 100.0, dtype=np.float32)]

        assert subtraction._build_saturation_mask(new_data, refs, radius_px=2) is None

    def test_saturated_pixel_in_new_frame_is_flagged(self):
        new_data = np.full((20, 20), 100.0, dtype=np.float32)
        new_data[10, 10] = config.SATURATION_ADU + 1000.0

        mask = subtraction._build_saturation_mask(new_data, [], radius_px=0)

        assert mask is not None
        assert bool(mask[10, 10]) is True
        assert int(mask.sum()) == 1  # no dilation requested

    def test_saturated_pixel_in_reference_frame_is_flagged(self):
        new_data = np.full((20, 20), 100.0, dtype=np.float32)
        ref = np.full((20, 20), 100.0, dtype=np.float32)
        ref[5, 5] = config.SATURATION_ADU + 1000.0

        mask = subtraction._build_saturation_mask(new_data, [ref], radius_px=0)

        assert mask is not None
        assert bool(mask[5, 5]) is True

    def test_dilation_grows_mask_around_saturated_pixel(self):
        new_data = np.full((20, 20), 100.0, dtype=np.float32)
        new_data[10, 10] = config.SATURATION_ADU + 1000.0

        mask = subtraction._build_saturation_mask(new_data, [], radius_px=2)

        assert mask is not None
        assert bool(mask[10, 12]) is True    # within the dilation radius
        assert bool(mask[10, 17]) is False   # well outside it

    def test_mismatched_reference_shape_is_skipped(self):
        new_data = np.full((20, 20), 100.0, dtype=np.float32)
        wrong_shape_ref = np.full((5, 5), config.SATURATION_ADU + 1000.0, dtype=np.float32)

        # Must not raise despite the shape mismatch; the mismatched ref is
        # skipped and new_data itself has no saturation.
        assert subtraction._build_saturation_mask(new_data, [wrong_shape_ref], radius_px=0) is None


# ---------------------------------------------------------------------------
# _pixel_scale_arcsec
# ---------------------------------------------------------------------------

class TestPixelScaleArcsec:

    def test_returns_scale_matching_wcs(self, tmp_path):
        path = TestPixelToSky._make_wcs_fits(tmp_path, scale_deg=0.000278)

        scale = subtraction._pixel_scale_arcsec(path)

        assert scale is not None
        assert scale == pytest.approx(0.000278 * 3600.0, rel=1e-3)

    def test_no_wcs_returns_none(self, tmp_path):
        data = np.zeros((10, 10), dtype=np.float32)
        path = tmp_path / "no_wcs.fits"
        fits.PrimaryHDU(data=data).writeto(path)

        assert subtraction._pixel_scale_arcsec(str(path)) is None

    def test_missing_file_returns_none(self, tmp_path):
        assert subtraction._pixel_scale_arcsec(str(tmp_path / "missing.fits")) is None

    def test_explicit_wcs_overrides_file_header(self, tmp_path):
        """
        A passed-in wcs must win over fits_path's own header WCS entirely —
        _open_wcs(fits_path) must not even be consulted. Regression test:
        subtraction.run() forwards astrometry.solve()'s already-solved WCS
        specifically so this candidate's pixel scale doesn't come from
        fits_path's own (possibly still-stale, not yet corrected) header —
        see run()'s docstring and modules/astrometry/_wcs.py's fix history.
        """
        path = TestPixelToSky._make_wcs_fits(tmp_path, scale_deg=0.000278)

        different_wcs = AstropyWCS(naxis=2)
        different_wcs.wcs.ctype = ["RA---TAN", "DEC--TAN"]
        different_wcs.wcs.crpix = [50.0, 50.0]
        different_wcs.wcs.crval = [10.0, 10.0]
        different_wcs.wcs.cdelt = [-0.001, 0.001]  # a very different scale
        different_wcs.wcs.set()

        scale = subtraction._pixel_scale_arcsec(path, wcs=different_wcs)

        assert scale == pytest.approx(0.001 * 3600.0, rel=1e-3)


# ---------------------------------------------------------------------------
# _pixel_to_sky
# ---------------------------------------------------------------------------

class TestPixelToSky:

    @staticmethod
    def _make_wcs_fits(tmp_path, ra=202.47, dec=47.20, scale_deg=0.000278):
        w = AstropyWCS(naxis=2)
        w.wcs.ctype = ["RA---TAN", "DEC--TAN"]
        w.wcs.crpix = [50.0, 50.0]
        w.wcs.crval = [ra, dec]
        w.wcs.cdelt = [-scale_deg, scale_deg]
        w.wcs.set()

        data = np.zeros((100, 100), dtype=np.float32)
        path = tmp_path / "solved.fits"
        fits.PrimaryHDU(data=data, header=w.to_header()).writeto(path)
        return str(path)

    def test_converts_pixel_to_expected_sky_position(self, tmp_path):
        path = self._make_wcs_fits(tmp_path, ra=202.47, dec=47.20)
        # CRPIX is 1-indexed per the FITS standard ([50.0, 50.0] in
        # _make_wcs_fits); pixel_to_world() takes 0-indexed pixel
        # coordinates (see _pixel_to_sky's own docstring), so 0-indexed
        # pixel (49.0, 49.0) is the one that lands exactly on CRVAL.
        candidates = [{"x": 49.0, "y": 49.0, "flux": 123.0}]

        result = subtraction._pixel_to_sky(candidates, path)

        assert len(result) == 1
        assert result[0]["ra"] == pytest.approx(202.47, abs=1e-6)
        assert result[0]["dec"] == pytest.approx(47.20, abs=1e-6)
        assert "x" not in result[0] and "y" not in result[0]
        assert result[0]["flux"] == 123.0

    def test_near_edge_flag_survives_conversion(self, tmp_path):
        """Only x/y are stripped — near_edge (and any other candidate key)
        must pass through untouched."""
        path = self._make_wcs_fits(tmp_path)
        candidates = [{"x": 49.0, "y": 49.0, "flux": 123.0, "near_edge": True}]

        result = subtraction._pixel_to_sky(candidates, path)

        assert result[0]["near_edge"] is True

    def test_no_wcs_returns_empty(self, tmp_path):
        data = np.zeros((10, 10), dtype=np.float32)
        path = tmp_path / "no_wcs.fits"
        fits.PrimaryHDU(data=data).writeto(path)

        result = subtraction._pixel_to_sky([{"x": 5.0, "y": 5.0}], str(path))

        assert result == []

    def test_missing_file_returns_empty(self, tmp_path):
        result = subtraction._pixel_to_sky([{"x": 5.0, "y": 5.0}], str(tmp_path / "missing.fits"))
        assert result == []

    def test_explicit_wcs_overrides_file_header(self, tmp_path):
        """
        A passed-in wcs must win over fits_path's own header WCS entirely.
        Regression test for the class of bug fixed alongside the 2026-08-06
        UGC_6930 incident (modules/astrometry/_wcs.py): without this, subtraction
        candidates would get a different systematic sky-position offset
        than every other source in the same frame, since fits_path's own
        header isn't corrected until pipeline.py archives the frame — well
        after subtraction.run() has already been called.
        """
        # The file's OWN header WCS says (202.47, 47.20)...
        path = self._make_wcs_fits(tmp_path, ra=202.47, dec=47.20)

        # ...but we pass a WCS centred somewhere completely different.
        different_wcs = AstropyWCS(naxis=2)
        different_wcs.wcs.ctype = ["RA---TAN", "DEC--TAN"]
        different_wcs.wcs.crpix = [50.0, 50.0]
        different_wcs.wcs.crval = [10.0, -30.0]
        different_wcs.wcs.cdelt = [-0.000278, 0.000278]
        different_wcs.wcs.set()

        result = subtraction._pixel_to_sky([{"x": 49.0, "y": 49.0}], path, wcs=different_wcs)

        assert len(result) == 1
        assert result[0]["ra"] == pytest.approx(10.0, abs=1e-6)
        assert result[0]["dec"] == pytest.approx(-30.0, abs=1e-6)


# ---------------------------------------------------------------------------
# run() — end-to-end orchestration
# ---------------------------------------------------------------------------

class TestRun:

    async def test_skips_when_too_few_archive_frames(self, monkeypatch, tmp_path):
        monkeypatch.setattr(subtraction, "_find_archive_frames", lambda d, f: ["a.fits", "b.fits"])

        result = await subtraction.run(str(tmp_path / "new.fits"), str(tmp_path), None)

        assert result == {"performed": False, "reference_frame_count": 0, "candidates": []}

    async def test_skips_when_new_frame_unloadable(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            subtraction, "_find_archive_frames",
            lambda d, f: ["a.fits", "b.fits", "c.fits"],
        )
        monkeypatch.setattr(subtraction, "_load_frame_data", lambda p: None)

        result = await subtraction.run(str(tmp_path / "new.fits"), str(tmp_path), None)

        assert result["performed"] is False
        assert result["candidates"] == []

    async def test_regression_differently_shaped_reference_frames_are_still_aligned(
        self, monkeypatch, tmp_path,
    ):
        """
        Regression test for the fixed shape-mismatch bug: reference frames
        whose pixel dimensions differ from the new frame (e.g. archived with
        a different camera/resolution) must still be handed to
        _align_frame() — not silently skipped by a shape-equality check
        before alignment is ever attempted. astroalign resamples onto the
        target's pixel grid regardless of the source's original shape, so
        this scenario is exactly what subtraction.py is meant to handle.
        """
        new_shape = (80, 100)
        ref_shape = (50, 60)  # deliberately different resolution
        new_data = np.ones(new_shape, dtype=np.float32)

        def fake_load(path):
            return new_data if path.endswith("new.fits") else np.ones(ref_shape, dtype=np.float32)

        def fake_align(source, target):
            assert source.shape == ref_shape  # the differently-shaped ref was actually passed through
            return np.ones(target.shape, dtype=np.float32)

        monkeypatch.setattr(
            subtraction, "_find_archive_frames",
            lambda d, f: ["ref1.fits", "ref2.fits", "ref3.fits"],
        )
        monkeypatch.setattr(subtraction, "_load_frame_data", fake_load)
        monkeypatch.setattr(subtraction, "_align_frame", fake_align)
        monkeypatch.setattr(subtraction, "_detect_diff_sources", lambda diff, mask=None, fwhm_min_px=None, pixel_scale_arcsec=None: [])
        monkeypatch.setattr(subtraction, "_pixel_to_sky", lambda cands, path, wcs=None: [])

        result = await subtraction.run(str(tmp_path / "new.fits"), str(tmp_path), None)

        assert result["performed"] is True
        assert result["reference_frame_count"] == 3

    async def test_skips_when_alignment_fails_for_all_frames(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            subtraction, "_find_archive_frames",
            lambda d, f: ["ref1.fits", "ref2.fits", "ref3.fits"],
        )
        monkeypatch.setattr(subtraction, "_load_frame_data", lambda p: np.ones((10, 10), dtype=np.float32))
        monkeypatch.setattr(subtraction, "_align_frame", lambda s, t: None)

        result = await subtraction.run(str(tmp_path / "new.fits"), str(tmp_path), None)

        assert result["performed"] is False

    async def test_successful_run_returns_candidates_flagged_from_subtraction(
        self, monkeypatch, tmp_path,
    ):
        shape = (10, 10)
        monkeypatch.setattr(
            subtraction, "_find_archive_frames",
            lambda d, f: ["ref1.fits", "ref2.fits", "ref3.fits"],
        )
        monkeypatch.setattr(subtraction, "_load_frame_data", lambda p: np.ones(shape, dtype=np.float32))
        monkeypatch.setattr(subtraction, "_align_frame", lambda s, t: np.ones(shape, dtype=np.float32))
        monkeypatch.setattr(
            subtraction, "_detect_diff_sources",
            lambda diff, mask=None, fwhm_min_px=None, pixel_scale_arcsec=None: [{"x": 5.0, "y": 5.0, "flux": 100.0, "snr": 8.0, "fwhm": 2.5, "elongation": 1.1}],
        )
        monkeypatch.setattr(
            subtraction, "_pixel_to_sky",
            lambda cands, path, wcs=None: [{"ra": 10.0, "dec": 20.0, "flux": 100.0, "snr": 8.0, "fwhm": 2.5, "elongation": 1.1}],
        )

        result = await subtraction.run(str(tmp_path / "new.fits"), str(tmp_path), None)

        assert result["performed"] is True
        assert result["reference_frame_count"] == 3
        assert len(result["candidates"]) == 1

        cand = result["candidates"][0]
        assert cand["_from_subtraction"] is True
        assert cand["mag"] is None
        assert cand["ra"] == 10.0
        assert cand["dec"] == 20.0

    async def test_saturation_mask_is_built_and_passed_to_detect_diff_sources(
        self, monkeypatch, tmp_path,
    ):
        """
        Regression test for docs/ISSUES.md #1/#2: a saturated pixel in the new
        frame must produce a non-None mask that reaches _detect_diff_sources(),
        so astroalign residual artifacts around it get excluded from detection.
        """
        shape = (20, 20)
        new_data = np.full(shape, 100.0, dtype=np.float32)
        new_data[5, 5] = config.SATURATION_ADU + 5000.0  # saturated pixel

        monkeypatch.setattr(
            subtraction, "_find_archive_frames",
            lambda d, f: ["ref1.fits", "ref2.fits", "ref3.fits"],
        )
        monkeypatch.setattr(
            subtraction, "_load_frame_data",
            lambda p: new_data if p.endswith("new.fits") else np.full(shape, 100.0, dtype=np.float32),
        )
        monkeypatch.setattr(subtraction, "_align_frame", lambda s, t: np.full(shape, 100.0, dtype=np.float32))
        # Force the fixed-pixel dilation fallback (no WCS lookup in this test).
        monkeypatch.setattr(subtraction, "_pixel_scale_arcsec", lambda path, wcs=None: None)

        captured: dict = {}

        def fake_detect(diff, mask=None, fwhm_min_px=None, pixel_scale_arcsec=None):
            captured["mask"] = mask
            return []

        monkeypatch.setattr(subtraction, "_detect_diff_sources", fake_detect)
        monkeypatch.setattr(subtraction, "_pixel_to_sky", lambda cands, path, wcs=None: [])

        result = await subtraction.run(str(tmp_path / "new.fits"), str(tmp_path), None)

        assert result["performed"] is True
        assert captured["mask"] is not None
        assert bool(captured["mask"][5, 5]) is True

    async def test_no_saturation_passes_none_mask(self, monkeypatch, tmp_path):
        """The common case (nothing saturated) must not pass a mask at all."""
        shape = (10, 10)
        monkeypatch.setattr(
            subtraction, "_find_archive_frames",
            lambda d, f: ["ref1.fits", "ref2.fits", "ref3.fits"],
        )
        monkeypatch.setattr(subtraction, "_load_frame_data", lambda p: np.full(shape, 100.0, dtype=np.float32))
        monkeypatch.setattr(subtraction, "_align_frame", lambda s, t: np.full(shape, 100.0, dtype=np.float32))

        captured: dict = {}

        def fake_detect(diff, mask=None, fwhm_min_px=None, pixel_scale_arcsec=None):
            captured["mask"] = mask
            return []

        monkeypatch.setattr(subtraction, "_detect_diff_sources", fake_detect)
        monkeypatch.setattr(subtraction, "_pixel_to_sky", lambda cands, path, wcs=None: [])

        result = await subtraction.run(str(tmp_path / "new.fits"), str(tmp_path), None)

        assert result["performed"] is True
        assert captured["mask"] is None

    async def test_wcs_param_is_forwarded_to_pixel_conversion(self, monkeypatch, tmp_path):
        """
        run()'s own wcs parameter must reach both _pixel_scale_arcsec() and
        _pixel_to_sky() — this is the actual fix: pipeline.py passes
        astro_result["wcs"] here specifically so subtraction candidates
        share the same sky-coordinate solution as every other source in the
        frame, instead of each call independently re-deriving WCS from
        fits_path's own (possibly stale) header.
        """
        shape = (10, 10)
        monkeypatch.setattr(
            subtraction, "_find_archive_frames",
            lambda d, f: ["ref1.fits", "ref2.fits", "ref3.fits"],
        )
        monkeypatch.setattr(subtraction, "_load_frame_data", lambda p: np.ones(shape, dtype=np.float32))
        monkeypatch.setattr(subtraction, "_align_frame", lambda s, t: np.ones(shape, dtype=np.float32))
        monkeypatch.setattr(
            subtraction, "_detect_diff_sources",
            lambda diff, mask=None, fwhm_min_px=None, pixel_scale_arcsec=None: [{"x": 5.0, "y": 5.0, "flux": 100.0, "snr": 8.0, "fwhm": 2.5, "elongation": 1.1}],
        )

        sentinel_wcs = AstropyWCS(naxis=2)
        captured: dict = {}

        def fake_pixel_scale(path, wcs=None):
            captured["pixel_scale_wcs"] = wcs
            return None

        def fake_pixel_to_sky(cands, path, wcs=None):
            captured["pixel_to_sky_wcs"] = wcs
            return []

        monkeypatch.setattr(subtraction, "_pixel_scale_arcsec", fake_pixel_scale)
        monkeypatch.setattr(subtraction, "_pixel_to_sky", fake_pixel_to_sky)

        await subtraction.run(str(tmp_path / "new.fits"), str(tmp_path), None, wcs=sentinel_wcs)

        assert captured["pixel_scale_wcs"] is sentinel_wcs
        assert captured["pixel_to_sky_wcs"] is sentinel_wcs

    async def test_new_frame_path_excluded_from_own_reference_stack(self, monkeypatch, tmp_path):
        """
        Re-analyzing an already-archived frame (see pipeline.py's
        _resolve_bare_filename()) can pass a fits_path that already sits
        inside archive_dir; _find_archive_frames() globs the whole directory
        with no idea which file is "the new one", so run() itself must
        filter the new frame's own (realpath-equal) path out of its
        candidate reference stack before it's ever aligned/averaged into the
        median reference — otherwise a re-analyzed frame would subtract a
        resampled copy of itself as part of its own reference.
        """
        new_path = str(tmp_path / "new.fits")
        monkeypatch.setattr(
            subtraction, "_find_archive_frames",
            lambda d, f: [new_path, "ref1.fits", "ref2.fits", "ref3.fits"],
        )

        loaded_paths: list[str] = []

        def fake_load(path):
            loaded_paths.append(path)
            return np.ones((10, 10), dtype=np.float32)

        monkeypatch.setattr(subtraction, "_load_frame_data", fake_load)
        monkeypatch.setattr(subtraction, "_align_frame", lambda s, t: np.ones((10, 10), dtype=np.float32))
        monkeypatch.setattr(
            subtraction, "_detect_diff_sources",
            lambda diff, mask=None, fwhm_min_px=None, pixel_scale_arcsec=None: [],
        )
        monkeypatch.setattr(subtraction, "_pixel_to_sky", lambda cands, path, wcs=None: [])

        result = await subtraction.run(new_path, str(tmp_path), None)

        assert result["performed"] is True
        # Only the 3 genuinely-different reference frames were aligned — the
        # new frame's own path was excluded from the reference stack, not
        # counted as a 4th reference.
        assert result["reference_frame_count"] == 3
        # The new frame's own path is loaded exactly once — as the new frame
        # itself (run()'s own `_load_frame_data(fits_path)` call) — never a
        # second time as one of its own references.
        assert loaded_paths.count(new_path) == 1
        assert loaded_paths.count("ref1.fits") == 1
        assert loaded_paths.count("ref2.fits") == 1
        assert loaded_paths.count("ref3.fits") == 1
