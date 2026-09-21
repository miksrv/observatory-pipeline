"""
tests/test_subtraction.py — Unit tests for modules/subtraction.py

Covers:
  - _find_archive_frames(): same-filter matching + fallback to any filter
  - _load_frame_data(): loading a 2-D image HDU, gracefully returning None
  - _align_frame(): success/failure wrapping of astroalign.register()
  - _detect_diff_sources(): SEP detection on a synthetic difference image
  - _pixel_to_sky(): WCS pixel -> sky conversion
  - _position_angle_deg() / _prerotate_reference() / PA-aware reference
    selection: camera-rotation handling (see CLAUDE.md's "camera rotation"
    discussion)
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

import math
import os
from unittest.mock import patch

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
# _parse_normalized_filename (audit 2026-08-18, C5)
# ---------------------------------------------------------------------------

class TestParseNormalizedFilename:

    def test_light_frame_with_filter(self):
        assert subtraction._parse_normalized_filename(
            "M45_Light_B_60_2020-10-15T01-24-51.fits"
        ) == ("Light", "B")

    def test_calibration_frame_has_no_filter_field(self):
        assert subtraction._parse_normalized_filename(
            "M42_Dark_300_2024-03-15T22-01-34.fits"
        ) == ("Dark", None)

    def test_object_name_containing_underscores(self):
        """The fields are anchored from the right for exactly this reason."""
        assert subtraction._parse_normalized_filename(
            "Andromeda_Galaxy_Light_Ha_300_2024-03-15T22-01-34.fits"
        ) == ("Light", "Ha")

    def test_sequence_suffix_is_ignored(self):
        assert subtraction._parse_normalized_filename(
            "NGC1234_Light_L_120_2024-03-15T22-01-34_001.fits"
        ) == ("Light", "L")

    def test_fractional_exposure_time(self):
        assert subtraction._parse_normalized_filename(
            "M42_Bias_0.001_2024-03-15T22-01-34.fits"
        ) == ("Bias", None)

    def test_legacy_frame_type_code_is_resolved_positionally(self):
        """
        The collision the finding is named for: under the earlier filename
        revision 'L' was the Light FrameType code AND the Luminance filter
        code. Its position in the name says which is which.
        """
        assert subtraction._parse_normalized_filename(
            "M51_L_Ha_120_2024-01-01T00-00-00.fits"
        ) == ("Light", "Ha")
        assert subtraction._parse_normalized_filename(
            "M51_L_L_120_2024-01-01T00-00-00.fits"
        ) == ("Light", "L")
        assert subtraction._parse_normalized_filename(
            "M51_B_0_2024-01-01T00-00-00.fits"
        ) == ("Bias", None)

    def test_unrecognized_name_returns_nothing(self):
        assert subtraction._parse_normalized_filename("IMG_0042.fits") == (None, None)
        assert subtraction._parse_normalized_filename("M51_L_Ha_120_a.fits") == (None, None)


class TestCalibrationFramesAreNotReferences:

    def _dir_with(self, tmp_path, names):
        for name in names:
            (tmp_path / name).write_bytes(b"x")
        return str(tmp_path)

    def test_calibration_frames_are_excluded(self, tmp_path):
        """
        pipeline.py archives Dark/Flat/Bias into the same per-object
        directory as the science frames. A starless calibration frame is not
        a reference for anything, and being recent it would crowd real
        science frames out of the newest-first selection.
        """
        archive = self._dir_with(tmp_path, [
            "M51_Light_L_120_2024-01-01T00-00-00.fits",
            "M51_Light_L_120_2024-01-02T00-00-00.fits",
            "M51_Light_L_120_2024-01-03T00-00-00.fits",
            "M51_Dark_120_2024-01-04T00-00-00.fits",
            "M51_Flat_3_2024-01-05T00-00-00.fits",
            "M51_Bias_0_2024-01-06T00-00-00.fits",
        ])

        result = subtraction._find_archive_frames(archive, None)

        assert len(result) == 3
        assert all("Light" in os.path.basename(p) for p in result)

    def test_bias_frames_do_not_answer_a_request_for_the_blue_filter(self, tmp_path):
        """The 'B' half of the token collision: Bias frames vs. the B filter."""
        archive = self._dir_with(tmp_path, [
            "M51_Light_B_120_2024-01-01T00-00-00.fits",
            "M51_Bias_0_2024-01-02T00-00-00.fits",
            "M51_Bias_0_2024-01-03T00-00-00.fits",
            "M51_Bias_0_2024-01-04T00-00-00.fits",
        ])

        result = subtraction._find_archive_frames(archive, "B")

        assert [os.path.basename(p) for p in result] == [
            "M51_Light_B_120_2024-01-01T00-00-00.fits"
        ]

    def test_legacy_light_code_does_not_pass_as_the_luminance_filter(self, tmp_path):
        """
        Every Light frame carried '_L_' as its FrameType code under the older
        filename revision, so a request for Luminance used to return the whole
        directory — Ha and OIII frames included — as a "same-filter" stack.
        """
        archive = self._dir_with(tmp_path, [
            "M51_L_Ha_300_2024-01-01T00-00-00.fits",
            "M51_L_Ha_300_2024-01-02T00-00-00.fits",
            "M51_L_OIII_300_2024-01-03T00-00-00.fits",
            "M51_L_OIII_300_2024-01-04T00-00-00.fits",
        ])

        result = subtraction._find_archive_frames(archive, "L")

        # No frame actually carries the Luminance filter, so this falls back
        # to the cross-filter pool rather than pretending all four match.
        assert len(result) == 4

    def test_same_filter_selection_still_works_on_the_current_format(self, tmp_path):
        archive = self._dir_with(tmp_path, [
            "M51_Light_Ha_300_2024-01-01T00-00-00.fits",
            "M51_Light_Ha_300_2024-01-02T00-00-00.fits",
            "M51_Light_Ha_300_2024-01-03T00-00-00.fits",
            "M51_Light_L_120_2024-01-04T00-00-00.fits",
            "M51_Dark_300_2024-01-05T00-00-00.fits",
        ])

        result = subtraction._find_archive_frames(archive, "Ha")

        assert len(result) == 3
        assert all("_Ha_" in os.path.basename(p) for p in result)

    def test_unparseable_names_keep_the_old_substring_behaviour(self, tmp_path):
        """
        A hand-placed or non-normalized archive can't be parsed positionally;
        it must keep whatever same-filter matching it had rather than losing
        subtraction entirely.
        """
        archive = self._dir_with(tmp_path, [
            "session1_Ha_a.fits",
            "session1_Ha_b.fits",
            "session1_Ha_c.fits",
            "session1_R_d.fits",
        ])

        result = subtraction._find_archive_frames(archive, "Ha")

        assert len(result) == 3
        assert all("_Ha_" in os.path.basename(p) for p in result)


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
        monkeypatch.setattr(astroalign, "register", lambda source, target, propagate_mask=False: (fake_aligned, None))

        result = subtraction._align_frame(np.ones((3, 3)), np.ones((5, 5)))

        assert result is not None
        aligned, footprint = result
        assert aligned.shape == (5, 5)
        assert aligned.dtype == np.float32
        assert footprint is None

    def test_align_failure_returns_none(self, monkeypatch):
        import astroalign

        def _raise(source, target, propagate_mask=False):
            raise ValueError("not enough matching triangles")

        monkeypatch.setattr(astroalign, "register", _raise)

        result = subtraction._align_frame(np.ones((3, 3)), np.ones((5, 5)))

        assert result is None


# ---------------------------------------------------------------------------
# Footprint handling in the median stack — audit 2026-08-18, finding H8
# ---------------------------------------------------------------------------

class TestAlignmentFootprint:
    """
    astroalign's second return value marks the target pixels it could NOT
    fill from the source frame — the band a shift or rotation leaves empty,
    the region outside a smaller sensor's field. It was discarded, so those
    non-measurements entered the median stack and produced residuals in the
    difference image that none of the saturation/streak/near_edge filters
    are looking for.
    """

    def test_footprint_is_returned_when_astroalign_supplies_one(self, monkeypatch):
        import astroalign
        fake_aligned = np.zeros((5, 5), dtype=np.float64)
        fake_footprint = np.zeros((5, 5), dtype=bool)
        fake_footprint[0, :] = True
        monkeypatch.setattr(
            astroalign, "register",
            lambda source, target, propagate_mask=False: (fake_aligned, fake_footprint),
        )

        aligned, footprint = subtraction._align_frame(np.ones((3, 3)), np.ones((5, 5)))

        assert footprint is not None
        assert footprint.dtype == bool
        assert footprint[0, 0] is np.True_ or bool(footprint[0, 0]) is True

    def test_a_mismatched_footprint_is_ignored(self, monkeypatch):
        """Anything that isn't a usable mask of the right shape is dropped."""
        import astroalign
        monkeypatch.setattr(
            astroalign, "register",
            lambda source, target, propagate_mask=False: (np.zeros((5, 5)), np.zeros((3, 3), dtype=bool)),
        )

        _, footprint = subtraction._align_frame(np.ones((3, 3)), np.ones((5, 5)))

        assert footprint is None

    def test_uncovered_values_are_excluded_from_the_median(self):
        """
        Two references hold 100 at a pixel; the third holds a wild
        extrapolated value there but declares it uncovered. The median must
        come out at 100, not be dragged by the value that isn't a measurement.
        """
        stack = np.stack([
            np.full((4, 4), 100.0, dtype=np.float32),
            np.full((4, 4), 100.0, dtype=np.float32),
            np.full((4, 4), 9000.0, dtype=np.float32),
        ])
        fp = np.zeros((4, 4), dtype=bool)
        fp[1, 1] = True
        new_data = np.full((4, 4), 100.0, dtype=np.float32)

        reference = subtraction._median_reference(stack, [None, None, fp], new_data)

        assert reference[1, 1] == pytest.approx(100.0)
        # Elsewhere the third frame still counts, so the median rises.
        assert reference[0, 0] == pytest.approx(100.0)

    def test_a_pixel_no_reference_covers_holds_the_difference_at_zero(self):
        stack = np.stack([
            np.full((4, 4), 5000.0, dtype=np.float32),
            np.full((4, 4), 5000.0, dtype=np.float32),
        ])
        fp = np.zeros((4, 4), dtype=bool)
        fp[2, 2] = True
        new_data = np.full((4, 4), 100.0, dtype=np.float32)

        reference = subtraction._median_reference(stack, [fp, fp.copy()], new_data)

        assert reference[2, 2] == pytest.approx(new_data[2, 2])
        assert (new_data - reference)[2, 2] == pytest.approx(0.0)

    def test_no_footprints_at_all_is_a_plain_median(self):
        stack = np.stack([
            np.full((3, 3), 10.0, dtype=np.float32),
            np.full((3, 3), 20.0, dtype=np.float32),
            np.full((3, 3), 30.0, dtype=np.float32),
        ])
        new_data = np.zeros((3, 3), dtype=np.float32)

        reference = subtraction._median_reference(stack, [None, None, None], new_data)

        assert np.allclose(reference, 20.0)


# ---------------------------------------------------------------------------
# NaN handling — audit 2026-08-18, finding H9
# ---------------------------------------------------------------------------

class TestNonFiniteHandling:
    """
    np.median() does not ignore NaN, it propagates it. One NaN pixel in one
    archived file — not rare; masked pixels from a previous calibration pass
    leave them — nulled the reference at that position and the difference
    image with it, silently, for every frame that archive is ever a reference
    for.
    """

    def test_one_nan_reference_pixel_does_not_null_the_median(self):
        stack = np.stack([
            np.full((4, 4), 100.0, dtype=np.float32),
            np.full((4, 4), 100.0, dtype=np.float32),
            np.full((4, 4), 100.0, dtype=np.float32),
        ])
        stack[0, 2, 2] = np.nan
        new_data = np.zeros((4, 4), dtype=np.float32)

        reference = subtraction._median_reference(stack, [None, None, None], new_data)

        assert np.isfinite(reference).all()
        assert reference[2, 2] == pytest.approx(100.0)

    def test_an_infinite_reference_pixel_is_excluded_too(self):
        stack = np.stack([
            np.full((4, 4), 100.0, dtype=np.float32),
            np.full((4, 4), 100.0, dtype=np.float32),
            np.full((4, 4), 100.0, dtype=np.float32),
        ])
        stack[1, 1, 1] = np.inf
        new_data = np.zeros((4, 4), dtype=np.float32)

        reference = subtraction._median_reference(stack, [None, None, None], new_data)

        assert reference[1, 1] == pytest.approx(100.0)

    def test_a_pixel_nan_in_every_reference_falls_back_to_the_new_frame(self):
        stack = np.stack([
            np.full((4, 4), 100.0, dtype=np.float32),
            np.full((4, 4), 100.0, dtype=np.float32),
        ])
        stack[:, 3, 3] = np.nan
        new_data = np.full((4, 4), 7.0, dtype=np.float32)

        reference = subtraction._median_reference(stack, [None, None], new_data)

        assert reference[3, 3] == pytest.approx(7.0)
        assert (new_data - reference)[3, 3] == pytest.approx(0.0)

    async def test_a_nan_in_the_new_frame_is_excluded_from_detection(
        self, monkeypatch, tmp_path,
    ):
        """
        The reference stack cannot repair a pixel the new frame has no value
        for. Left alone it reaches sep, whose background/RMS estimate it
        corrupts for the whole frame — so it is zeroed and added to the same
        detection mask the saturated vicinity uses.
        """
        shape = (20, 20)
        new_data = np.ones(shape, dtype=np.float32)
        new_data[5, 5] = np.nan

        def fake_load(path):
            return new_data if path.endswith("new.fits") else np.ones(shape, dtype=np.float32)

        seen: dict = {}

        def fake_detect(diff, mask=None, fwhm_min_px=None, pixel_scale_arcsec=None):
            seen["diff"] = diff
            seen["mask"] = mask
            return []

        monkeypatch.setattr(
            subtraction, "_find_archive_frames",
            lambda d, f, pa=None, fwhm=None: ["a.fits", "b.fits", "c.fits"],
        )
        monkeypatch.setattr(subtraction, "_load_frame_data", fake_load)
        monkeypatch.setattr(
            subtraction, "_align_frame",
            lambda s, t: (np.ones(shape, dtype=np.float32), None),
        )
        monkeypatch.setattr(subtraction, "_detect_diff_sources", fake_detect)
        monkeypatch.setattr(subtraction, "_pixel_to_sky", lambda cands, path, wcs=None: [])

        result = await subtraction.run(str(tmp_path / "new.fits"), str(tmp_path), None)

        assert result["performed"] is True
        assert np.isfinite(seen["diff"]).all()
        assert seen["mask"] is not None
        assert bool(seen["mask"][5, 5]) is True


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

    def test_an_elongated_corner_candidate_is_filtered_out(self):
        """
        A stretched blob at (5, 5) on a 100x100 diff falls inside the 10px
        margin and looks like what it is — a coma/aberration residual, whose
        signature is exactly that arc shape (see EDGE_MARGIN_FRAC filtering
        in _detect_diff_sources).
        """
        rng = np.random.default_rng(9)
        diff = rng.normal(loc=0.0, scale=5.0, size=(100, 100))
        yy, xx = np.mgrid[0:100, 0:100]
        # Elongated along x — a/b well past SUBTRACTION_EDGE_ELONGATION_MAX
        blob = 800.0 * np.exp(-(((xx - 5) ** 2 / (2 * 6.0 ** 2)) + ((yy - 5) ** 2 / (2 * 1.5 ** 2))))
        diff = diff + blob

        candidates = subtraction._detect_diff_sources(diff)

        corner_hits = [c for c in candidates if (c["x"] - 5) ** 2 + (c["y"] - 5) ** 2 < 25]
        assert len(corner_hits) == 0

    def test_a_round_strong_corner_candidate_survives(self):
        """
        Audit 2026-08-18, finding H11: the edge zone used to be rejected
        outright, so a genuine transient landing there — routine under a
        dithering pattern — could never be found by subtraction at all. A
        round, strong residual is not the shape an aberration takes.
        """
        rng = np.random.default_rng(9)
        diff = rng.normal(loc=0.0, scale=5.0, size=(100, 100))
        yy, xx = np.mgrid[0:100, 0:100]
        blob = 800.0 * np.exp(-(((xx - 5) ** 2 + (yy - 5) ** 2) / (2 * 2.0 ** 2)))
        diff = diff + blob

        candidates = subtraction._detect_diff_sources(diff)

        corner_hits = [c for c in candidates if (c["x"] - 5) ** 2 + (c["y"] - 5) ** 2 < 25]
        assert len(corner_hits) == 1
        assert corner_hits[0]["near_edge"] is True
        assert corner_hits[0]["elongation"] <= config.SUBTRACTION_EDGE_ELONGATION_MAX
        assert corner_hits[0]["snr"] >= config.SUBTRACTION_EDGE_SNR_MIN

    def test_a_weak_round_corner_candidate_is_still_filtered_out(self, monkeypatch):
        """Strength is the second half of the bar, not an afterthought."""
        monkeypatch.setattr(config, "SUBTRACTION_EDGE_SNR_MIN", 1e6)

        rng = np.random.default_rng(9)
        diff = rng.normal(loc=0.0, scale=5.0, size=(100, 100))
        yy, xx = np.mgrid[0:100, 0:100]
        blob = 800.0 * np.exp(-(((xx - 5) ** 2 + (yy - 5) ** 2) / (2 * 2.0 ** 2)))
        diff = diff + blob

        candidates = subtraction._detect_diff_sources(diff)

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

class TestPrerotationDoesNotCropCorners:
    """
    Audit 2026-08-18, finding H14: scipy.ndimage.rotate(reshape=False) is a
    crop for any angle that isn't a multiple of 90 degrees — the corners
    rotate off the canvas and are lost. The gate is 2 degrees, so this fired
    on modest field rotation, not only on meridian flips, and the stars it
    discarded are the ones astroalign needs to find a transform at all.
    """

    def _prerotate(self, monkeypatch, tmp_path, ref_pa, new_pa, shape=(60, 80)):
        data = np.arange(shape[0] * shape[1], dtype=np.float32).reshape(shape) + 1.0
        path = str(tmp_path / "ref.fits")

        monkeypatch.setattr(subtraction, "_open_wcs", lambda p: object())
        monkeypatch.setattr(subtraction, "_position_angle_deg", lambda w: ref_pa)

        return data, subtraction._prerotate_reference(data, path, new_pa)

    def test_the_canvas_grows_so_no_content_is_lost(self, monkeypatch, tmp_path):
        data, rotated = self._prerotate(monkeypatch, tmp_path, ref_pa=0.0, new_pa=30.0)

        assert rotated.shape[0] > data.shape[0]
        assert rotated.shape[1] > data.shape[1]

    def test_the_padding_is_masked_rather_than_left_as_a_hard_zero_edge(
        self, monkeypatch, tmp_path,
    ):
        _, rotated = self._prerotate(monkeypatch, tmp_path, ref_pa=0.0, new_pa=30.0)

        assert isinstance(rotated, np.ma.MaskedArray)
        # The corners of the enlarged canvas are outside the original frame.
        assert bool(rotated.mask[0, 0]) is True
        # The centre came from real data.
        h, w = rotated.shape
        assert bool(rotated.mask[h // 2, w // 2]) is False

    def test_a_negligible_angle_is_left_untouched(self, monkeypatch, tmp_path):
        data, rotated = self._prerotate(monkeypatch, tmp_path, ref_pa=0.0, new_pa=0.5)

        assert rotated is data

    def test_the_mask_reaches_astroalign(self, monkeypatch):
        """
        The padding only stays out of the median stack because astroalign is
        asked to propagate a masked source's mask into its footprint.
        """
        import astroalign

        seen: dict = {}

        def fake_register(source, target, propagate_mask=False):
            seen["propagate_mask"] = propagate_mask
            return np.zeros(target.shape, dtype=np.float32), None

        monkeypatch.setattr(astroalign, "register", fake_register)
        subtraction._align_frame(np.ones((5, 5)), np.ones((5, 5)))

        assert seen["propagate_mask"] is True


class TestCorrelatedNoiseCorrection:
    """
    Audit 2026-08-18, finding H13: a candidate's significance was
    flux / (rms * sqrt(npix)), which assumes each pixel's noise is
    independent of its neighbours'. Astroalign's resampling — and the
    optional pre-rotation before it — spreads each input pixel's noise across
    several output pixels, so the aperture holds fewer independent
    measurements than pixels and the figure overstates significance.
    """

    def _white(self, scale=5.0, size=(300, 300)):
        rng = np.random.default_rng(0)
        return rng.normal(0.0, scale, size)

    def _correlated(self, scale=5.0, size=(300, 300)):
        from scipy.ndimage import gaussian_filter
        img = gaussian_filter(self._white(scale, size), sigma=1.2)
        return img * (scale / img.std())

    def _rms(self, img):
        return 1.4826 * float(np.median(np.abs(img - np.median(img))))

    def test_uncorrelated_noise_needs_no_correction(self):
        img = self._white()
        factor = subtraction._noise_correlation_factor(img, None, self._rms(img))

        assert factor == pytest.approx(1.0, abs=0.1)

    def test_correlated_noise_is_detected(self):
        img = self._correlated()
        factor = subtraction._noise_correlation_factor(img, None, self._rms(img))

        assert factor > 1.5

    def test_the_factor_is_capped(self, monkeypatch):
        monkeypatch.setattr(config, "SUBTRACTION_NOISE_CORR_MAX", 1.5)
        img = self._correlated()

        factor = subtraction._noise_correlation_factor(img, None, self._rms(img))

        assert factor == pytest.approx(1.5)

    def test_a_cap_of_one_disables_the_correction(self, monkeypatch):
        monkeypatch.setattr(config, "SUBTRACTION_NOISE_CORR_MAX", 1.0)
        img = self._correlated()

        assert subtraction._noise_correlation_factor(img, None, self._rms(img)) == 1.0

    def test_too_small_an_image_returns_one(self):
        img = self._white(size=(8, 8))

        assert subtraction._noise_correlation_factor(img, None, self._rms(img)) == 1.0

    def test_reported_snr_drops_on_a_correlated_diff_image(self, monkeypatch):
        """
        End to end: the same blob on the same per-pixel RMS must be reported
        at a lower significance when the noise around it is correlated.
        """
        yy, xx = np.mgrid[0:300, 0:300]
        blob = 400.0 * np.exp(-(((xx - 150) ** 2 + (yy - 150) ** 2) / (2 * 3.0 ** 2)))

        white_snr = subtraction._detect_diff_sources(self._white() + blob)
        corr_snr = subtraction._detect_diff_sources(self._correlated() + blob)

        white_hit = next(c for c in white_snr if abs(c["x"] - 150) < 3)
        corr_hit = next(c for c in corr_snr if abs(c["x"] - 150) < 3)

        assert corr_hit["snr"] < white_hit["snr"]


class TestBackgroundReMeasuredAfterStreakMasking:
    """
    Audit 2026-08-18, finding H12: the streak mask can only be found on an
    already-background-subtracted image, so the first pass necessarily
    measured the RMS with the trail still in frame. That RMS is the scale of
    the detection threshold, so one bright satellite track quietly raised the
    bar for every faint real transient elsewhere in the same frame.
    """

    def _diff_with_trail_and_faint_blob(self):
        rng = np.random.default_rng(3)
        diff = rng.normal(loc=0.0, scale=5.0, size=(300, 300))
        # A long, bright trail — elongation and length well past the
        # STREAK_* thresholds at pixel_scale_arcsec=1.0.
        diff[20:280, 148:152] += 3000.0
        # A faint round source far from it, close to the detection limit.
        yy, xx = np.mgrid[0:300, 0:300]
        diff = diff + 45.0 * np.exp(-(((xx - 250) ** 2 + (yy - 250) ** 2) / (2 * 3.0 ** 2)))
        return diff

    def test_the_rms_used_for_the_threshold_excludes_the_trail(self, monkeypatch):
        """
        The second sep.Background() call must be the one whose globalrms sets
        the threshold, and it must see a lower RMS than the first.
        """
        seen: list[float] = []
        real_background = subtraction.sep.Background

        def spy(data, mask=None):
            bkg = real_background(data, mask=mask) if mask is not None else real_background(data)
            seen.append(float(bkg.globalrms))
            return bkg

        monkeypatch.setattr(subtraction.sep, "Background", spy)

        subtraction._detect_diff_sources(
            self._diff_with_trail_and_faint_blob(), pixel_scale_arcsec=1.0,
        )

        assert len(seen) >= 2, "background was not re-measured after streak masking"
        assert seen[1] < seen[0]


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
# Degenerate minor axis (audit 2026-08-18, finding L4)
# ---------------------------------------------------------------------------

class TestDegenerateMinorAxis:
    @staticmethod
    def _one_object(a: float, b: float) -> np.ndarray:
        obj = np.zeros(1, dtype=[
            ("x", np.float64), ("y", np.float64),
            ("a", np.float64), ("b", np.float64),
            ("flux", np.float64), ("npix", np.int32),
        ])
        obj["x"], obj["y"] = 150.0, 150.0
        obj["a"], obj["b"] = a, b
        obj["flux"], obj["npix"] = 5000.0, 9
        return obj

    def test_zero_minor_axis_reads_as_a_one_pixel_wide_feature(self, monkeypatch):
        """
        `sep` reports b=0 for a degenerate second-moment fit. Dividing by the
        old 0.001 epsilon made the candidate's elongation 1000x its semi-major
        axis — a number that is not a measurement, and that
        anomaly_detector.py reads straight off the candidate as evidence of a
        SPACE_DEBRIS trail. Clamped at the pixel grid's own resolution limit
        it reads as the most elongated the feature could honestly be.
        """
        def _extract(data, *args, **kwargs):
            if kwargs.get("segmentation_map"):
                raise RuntimeError("no coarse pass in this test")
            return self._one_object(a=2.0, b=0.0)

        monkeypatch.setattr(subtraction.sep, "extract", _extract)

        rng = np.random.default_rng(3)
        candidates = subtraction._detect_diff_sources(
            rng.normal(loc=0.0, scale=5.0, size=(300, 300))
        )

        assert len(candidates) == 1
        assert candidates[0]["elongation"] == pytest.approx(
            2.0 * math.sqrt(12.0), rel=1e-6
        )

    def test_a_measurable_minor_axis_is_left_alone(self, monkeypatch):
        """The clamp is a floor, not a rescaling — a real b passes through."""
        def _extract(data, *args, **kwargs):
            if kwargs.get("segmentation_map"):
                raise RuntimeError("no coarse pass in this test")
            return self._one_object(a=6.0, b=2.0)

        monkeypatch.setattr(subtraction.sep, "extract", _extract)

        rng = np.random.default_rng(3)
        candidates = subtraction._detect_diff_sources(
            rng.normal(loc=0.0, scale=5.0, size=(300, 300))
        )

        assert candidates[0]["elongation"] == pytest.approx(3.0)


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
# _position_angle_deg / _prerotate_reference / PA-aware reference selection
#
# Regression coverage for the 2026-08 "camera rotation" investigation
# (source_id 6a7cfbae64e706.89320404, CLAUDE.md's "camera rotation"
# discussion): a reference frame captured at a very different camera/rotator
# orientation than the new frame (e.g. a ~180 deg meridian flip) must be
# coarse-pre-rotated toward the new frame's orientation, NOT excluded.
# ---------------------------------------------------------------------------

def _rotate_wcs(wcs: AstropyWCS, theta_deg: float) -> AstropyWCS:
    """
    Same construction as tests/test_astrometry.py's helper of the same name
    (see that module for the full derivation/verification): a copy of *wcs*
    as if its data had been produced by ``scipy.ndimage.rotate(data,
    angle=theta_deg)`` — CD_new = CD_old @ R_ccw(theta_deg).
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


def _write_wcs_fits(tmp_path, name: str, wcs: AstropyWCS, data: np.ndarray) -> str:
    path = tmp_path / name
    fits.PrimaryHDU(data=data.astype(np.float32), header=wcs.to_header()).writeto(path)
    return str(path)


def _base_wcs(scale_deg: float = 0.000278) -> AstropyWCS:
    w = AstropyWCS(naxis=2)
    w.wcs.ctype = ["RA---TAN", "DEC--TAN"]
    w.wcs.crpix = [50.0, 50.0]
    w.wcs.crval = [202.47, 47.20]
    w.wcs.cd = [[-scale_deg, 0.0], [0.0, scale_deg]]
    w.wcs.set()
    return w


class TestPositionAngleDeg:

    def test_unrotated_wcs_reports_zero(self):
        assert subtraction._position_angle_deg(_base_wcs()) == pytest.approx(0.0, abs=1e-6)

    @pytest.mark.parametrize("theta_deg", [30.0, 90.0, 180.0, -45.0])
    def test_rotated_wcs_reports_matching_delta(self, theta_deg):
        base = _base_wcs()
        rotated = _rotate_wcs(base, theta_deg)

        pa_base = subtraction._position_angle_deg(base)
        pa_rotated = subtraction._position_angle_deg(rotated)

        delta = (pa_rotated - pa_base) % 360.0
        assert delta == pytest.approx(theta_deg % 360.0, abs=1e-6)

    def test_degenerate_wcs_returns_none(self):
        w = AstropyWCS(naxis=2)  # never configured — pixel_to_world() will fail
        assert subtraction._position_angle_deg(w) is None


class TestPrerotateReference:

    def test_none_new_pa_is_noop(self, tmp_path):
        data = np.arange(100.0).reshape(10, 10)
        path = _write_wcs_fits(tmp_path, "ref.fits", _base_wcs(), data)

        result = subtraction._prerotate_reference(data, path, None)

        assert result is data

    def test_below_threshold_delta_is_noop(self, tmp_path):
        """A near-identical orientation (well under SUBTRACTION_PREROTATE_MIN_DEG) must not be rotated."""
        data = np.arange(100.0).reshape(10, 10)
        path = _write_wcs_fits(tmp_path, "ref.fits", _base_wcs(), data)
        tiny_delta_new_pa = config.SUBTRACTION_PREROTATE_MIN_DEG / 2.0

        result = subtraction._prerotate_reference(data, path, tiny_delta_new_pa)

        assert result is data

    def test_missing_wcs_is_noop(self, tmp_path):
        data = np.zeros((10, 10), dtype=np.float32)
        path = tmp_path / "no_wcs.fits"
        fits.PrimaryHDU(data=data).writeto(path)

        result = subtraction._prerotate_reference(data, str(path), 90.0)

        assert result is data

    def test_rotation_undoes_known_180_degree_offset(self, tmp_path):
        """
        This is the user's actual reported scenario: a reference frame whose
        camera was rotated ~180 deg relative to the new frame (e.g. a
        meridian flip between sessions). The pre-rotated reference must line
        up with the new frame's own (unrotated) star field, not merely
        "align eventually via astroalign" — this test checks the geometry
        directly, without astroalign in the loop at all.
        """
        # A handful of point sources, asymmetric so a wrong rotation
        # direction is unambiguously detectable via cross-correlation.
        size = 101
        base_data = np.zeros((size, size), dtype=np.float64)
        for r, c in [(30, 60), (70, 45), (55, 20)]:
            base_data[r, c] = 1000.0

        base_wcs = _base_wcs()
        # The reference frame's actual pixel content was captured with the
        # camera physically rotated 180 deg relative to "new" — same
        # relation as scipy.ndimage.rotate(base_data, angle=180).
        from scipy.ndimage import rotate as ndi_rotate
        ref_data = ndi_rotate(base_data, angle=180.0, reshape=False, order=1)
        ref_wcs = _rotate_wcs(base_wcs, 180.0)
        ref_path = _write_wcs_fits(tmp_path, "ref_180.fits", ref_wcs, ref_data)

        new_pa = subtraction._position_angle_deg(base_wcs)
        corrected = subtraction._prerotate_reference(ref_data, ref_path, new_pa)

        # The corrected reference should now closely match the new frame's
        # own (unrotated) star field, not the original 180-degree-rotated data.
        assert np.corrcoef(corrected.ravel(), base_data.ravel())[0, 1] > 0.99
        assert not np.allclose(corrected, ref_data)

    def test_rotation_is_skipped_when_scipy_missing(self, tmp_path, monkeypatch):
        """A rotation failure (e.g. scipy unavailable) must degrade gracefully, not raise."""
        data = np.arange(100.0).reshape(10, 10)
        rotated_wcs = _rotate_wcs(_base_wcs(), 90.0)
        path = _write_wcs_fits(tmp_path, "ref.fits", rotated_wcs, data)

        def _raise(*args, **kwargs):
            raise ImportError("no scipy")

        monkeypatch.setattr(subtraction, "_open_wcs", lambda p: rotated_wcs)
        with patch("scipy.ndimage.rotate", side_effect=_raise):
            result = subtraction._prerotate_reference(data, path, 0.0)

        assert result is data


class TestFindArchiveFramesPositionAngle:

    def test_none_pa_preserves_recency_only_order(self, tmp_path):
        """Backward compatibility: omitting new_position_angle_deg must not change behavior at all."""
        names = [f"f{i}.fits" for i in range(5)]
        for name in names:
            (tmp_path / name).write_bytes(b"x")

        result = subtraction._find_archive_frames(str(tmp_path), None)

        assert len(result) == 5  # under _MAX_FRAMES, nothing to reorder anyway

    def test_prefers_closest_orientation_when_over_capacity(self, tmp_path, monkeypatch):
        """
        With more candidates than _MAX_FRAMES, the ones closest in PA to the
        new frame must be preferred over merely-more-recent ones with a
        very different orientation — a soft ranking, not a hard filter (see
        TestRunPositionAngle below for proof that a bad-PA frame is still
        usable, just deprioritized when there's a choice).
        """
        # More files than _MAX_FRAMES (10) so the PA-based re-sort actually
        # has an effect on which _MAX_FRAMES survive the cap.
        good_names = [f"good{i}.fits" for i in range(6)]
        bad_names = [f"bad{i}.fits" for i in range(6)]
        for name in good_names + bad_names:
            (tmp_path / name).write_bytes(b"x")

        def fake_open_wcs(path):
            return "good_wcs" if "good" in path else "bad_wcs"

        def fake_pa(wcs):
            return 0.0 if wcs == "good_wcs" else 179.0

        monkeypatch.setattr(subtraction, "_open_wcs", fake_open_wcs)
        monkeypatch.setattr(subtraction, "_position_angle_deg", fake_pa)

        result = subtraction._find_archive_frames(str(tmp_path), None, new_position_angle_deg=0.0)

        assert len(result) == subtraction._MAX_FRAMES
        # All 6 "good"-orientation candidates must survive the _MAX_FRAMES
        # cap (none bumped out by a merely-more-recent "bad"-orientation
        # one), and must be ordered ahead of whichever "bad" ones fill the
        # remaining slots.
        assert sum("good" in p for p in result) == len(good_names)
        first_bad_idx = next(i for i, p in enumerate(result) if "bad" in p)
        assert all("good" in p for p in result[:first_bad_idx])


# ---------------------------------------------------------------------------
# Photometric normalization of reference frames (audit 2026-08-18, C4)
# ---------------------------------------------------------------------------

def _write_frame(path, exptime=None, egain=None, gain=None, value=1.0, shape=(8, 8)):
    """Write a tiny FITS file carrying the given exposure/gain keywords."""
    hdu = fits.PrimaryHDU(np.full(shape, value, dtype=np.float32))
    if exptime is not None:
        hdu.header["EXPTIME"] = exptime
    if egain is not None:
        hdu.header["EGAIN"] = egain
    if gain is not None:
        hdu.header["GAIN"] = gain
    hdu.writeto(str(path), overwrite=True)
    return str(path)


def _write_qc_frame(path, flag=None, fwhm=None, shape=(8, 8)):
    """Write a tiny FITS file carrying the QC headers pipeline.py stamps."""
    hdu = fits.PrimaryHDU(np.ones(shape, dtype=np.float32))
    if flag is not None:
        hdu.header["QCFLAG"] = flag
    if fwhm is not None:
        hdu.header["QCFWHM"] = fwhm
    hdu.writeto(str(path), overwrite=True)
    return str(path)


class TestReferenceQualityScreen:
    """
    Audit 2026-08-18, finding H10: reference selection was recency (plus
    PA-closeness) only and never looked at quality. Harmless while QC-failed
    frames went to /fits/rejected — but they are archived now, into the very
    directory the reference stack is drawn from, and differencing a sharp
    frame against a blurred one leaves a ring residual at every star.
    """

    def test_a_qc_failed_reference_is_excluded(self, tmp_path):
        good = [_write_qc_frame(tmp_path / f"good{i}.fits", flag="OK", fwhm=3.0) for i in range(3)]
        bad = _write_qc_frame(tmp_path / "blur.fits", flag="BLUR", fwhm=3.0)

        kept = subtraction._screen_by_quality(good + [bad], psf_fwhm_arcsec=3.0)

        assert bad not in kept
        assert set(kept) == set(good)

    def test_a_reference_far_blurrier_than_the_new_frame_is_excluded(self, tmp_path):
        good = [_write_qc_frame(tmp_path / f"good{i}.fits", flag="OK", fwhm=3.0) for i in range(3)]
        blurry = _write_qc_frame(tmp_path / "soft.fits", flag="OK", fwhm=9.0)

        kept = subtraction._screen_by_quality(good + [blurry], psf_fwhm_arcsec=3.0)

        assert blurry not in kept

    def test_comparable_seeing_is_kept(self, tmp_path):
        good = [_write_qc_frame(tmp_path / f"good{i}.fits", flag="OK", fwhm=3.0) for i in range(3)]
        similar = _write_qc_frame(tmp_path / "similar.fits", flag="OK", fwhm=4.0)

        kept = subtraction._screen_by_quality(good + [similar], psf_fwhm_arcsec=3.0)

        assert similar in kept

    def test_a_frame_with_no_qc_headers_is_kept(self, tmp_path):
        """An archive written before those headers existed must not lose subtraction."""
        good = [_write_qc_frame(tmp_path / f"good{i}.fits", flag="OK", fwhm=3.0) for i in range(3)]
        legacy = _write_qc_frame(tmp_path / "legacy.fits")

        kept = subtraction._screen_by_quality(good + [legacy], psf_fwhm_arcsec=3.0)

        assert legacy in kept

    def test_no_new_frame_fwhm_leaves_only_the_flag_half_in_force(self, tmp_path):
        good = [_write_qc_frame(tmp_path / f"good{i}.fits", flag="OK", fwhm=3.0) for i in range(3)]
        blurry = _write_qc_frame(tmp_path / "soft.fits", flag="OK", fwhm=9.0)
        bad = _write_qc_frame(tmp_path / "blur.fits", flag="TRAIL", fwhm=3.0)

        kept = subtraction._screen_by_quality(good + [blurry, bad], psf_fwhm_arcsec=None)

        assert blurry in kept
        assert bad not in kept

    def test_screening_below_the_minimum_falls_back_to_the_unscreened_set(self, tmp_path):
        """An imperfect reference stack beats losing subtraction entirely."""
        paths = [_write_qc_frame(tmp_path / f"b{i}.fits", flag="BLUR", fwhm=3.0) for i in range(4)]

        kept = subtraction._screen_by_quality(paths, psf_fwhm_arcsec=3.0)

        assert kept == paths

    def test_find_archive_frames_applies_the_screen(self, tmp_path):
        for i in range(3):
            _write_qc_frame(tmp_path / f"M51_Light_L_60_2024-03-1{i}T00-00-00.fits", flag="OK", fwhm=3.0)
        _write_qc_frame(tmp_path / "M51_Light_L_60_2024-03-20T00-00-00.fits", flag="BLUR", fwhm=3.0)

        found = subtraction._find_archive_frames(str(tmp_path), "L", None, 3.0)

        assert len(found) == 3
        assert all("2024-03-20" not in os.path.basename(f) for f in found)


class TestFluxScaleKeys:

    def test_reads_exptime_and_egain(self, tmp_path):
        path = _write_frame(tmp_path / "f.fits", exptime=120.0, egain=1.5)

        assert subtraction._read_flux_scale_keys(path) == (120.0, 1.5)

    def test_exposure_is_accepted_as_an_exptime_alias(self, tmp_path):
        hdu = fits.PrimaryHDU(np.zeros((4, 4), dtype=np.float32))
        hdu.header["EXPOSURE"] = 60.0
        hdu.writeto(str(tmp_path / "f.fits"))

        exptime, _ = subtraction._read_flux_scale_keys(str(tmp_path / "f.fits"))

        assert exptime == 60.0

    def test_egain_wins_over_gain(self, tmp_path):
        """
        Same preference as photometry._resolve_gain(): on most CMOS cameras
        GAIN is the vendor gain *setting*, EGAIN the real e-/ADU conversion.
        """
        path = _write_frame(tmp_path / "f.fits", exptime=60.0, egain=0.8, gain=120.0)

        assert subtraction._read_flux_scale_keys(path)[1] == 0.8

    def test_implausible_gain_is_rejected(self, tmp_path):
        """A bare GAIN=120 is a vendor setting, not e-/ADU — must not be used."""
        path = _write_frame(tmp_path / "f.fits", exptime=60.0, gain=120.0)

        assert subtraction._read_flux_scale_keys(path)[1] is None

    def test_missing_keywords_return_none(self, tmp_path):
        path = _write_frame(tmp_path / "f.fits")

        assert subtraction._read_flux_scale_keys(path) == (None, None)

    def test_unreadable_file_returns_none(self, tmp_path):
        assert subtraction._read_flux_scale_keys(str(tmp_path / "missing.fits")) == (None, None)


class TestFluxScaleFactor:

    def test_exposure_ratio(self, tmp_path):
        """A 60s reference must be doubled to sit on a 120s frame's scale."""
        ref = _write_frame(tmp_path / "ref.fits", exptime=60.0)

        assert subtraction._flux_scale_factor(ref, 120.0, None) == pytest.approx(2.0)

    def test_gain_ratio(self, tmp_path):
        """
        ADU scales as exptime / gain(e-/ADU), so a reference read out at
        2 e-/ADU carries half the counts of a 1 e-/ADU frame and must be
        scaled up by g_ref / g_new.
        """
        ref = _write_frame(tmp_path / "ref.fits", exptime=60.0, egain=2.0)

        assert subtraction._flux_scale_factor(ref, 60.0, 1.0) == pytest.approx(2.0)

    def test_missing_exptime_falls_back_to_unity(self, tmp_path):
        """An archive with no EXPTIME behaves exactly as it did before C4."""
        ref = _write_frame(tmp_path / "ref.fits")

        assert subtraction._flux_scale_factor(ref, 120.0, None) == 1.0

    def test_missing_gain_still_applies_the_exposure_ratio(self, tmp_path):
        ref = _write_frame(tmp_path / "ref.fits", exptime=30.0)

        assert subtraction._flux_scale_factor(ref, 120.0, 1.5) == pytest.approx(4.0)


class TestReferenceNormalizationInRun:

    def _setup(self, monkeypatch, tmp_path, new_exptime, ref_exptime, star_flux=500.0):
        """
        Build a new frame and three references of the same star field, each
        recorded at its own exposure time, and return the diff image run()
        ends up detecting on.
        """
        shape = (12, 12)
        sky = 100.0

        def frame(exptime):
            data = np.full(shape, sky * exptime / new_exptime, dtype=np.float32)
            data[6, 6] += star_flux * exptime / new_exptime
            return data

        new_path = _write_frame(tmp_path / "new.fits", exptime=new_exptime)
        ref_paths = [
            _write_frame(tmp_path / f"ref{i}.fits", exptime=ref_exptime)
            for i in range(3)
        ]

        data_by_path = {new_path: frame(new_exptime)}
        for ref_path in ref_paths:
            data_by_path[ref_path] = frame(ref_exptime)

        monkeypatch.setattr(subtraction, "_find_archive_frames", lambda d, f, pa=None, fwhm=None: ref_paths)
        monkeypatch.setattr(subtraction, "_load_frame_data", lambda p: data_by_path[p].copy())
        monkeypatch.setattr(subtraction, "_align_frame", lambda s, t: (s, None))

        captured: dict = {}

        def fake_detect(diff, mask=None, fwhm_min_px=None, pixel_scale_arcsec=None):
            captured["diff"] = diff
            return []

        monkeypatch.setattr(subtraction, "_detect_diff_sources", fake_detect)
        monkeypatch.setattr(subtraction, "_pixel_to_sky", lambda cands, path, wcs=None: [])

        return new_path, captured

    async def test_mixed_exposure_archive_leaves_no_stellar_residual(
        self, monkeypatch, tmp_path,
    ):
        """
        Audit 2026-08-18, finding C4: a 120s frame differenced against 60s
        references used to leave ~(K-1) x flux at the position of EVERY star
        in the frame — hundreds of false candidates. Normalizing the
        references onto the new frame's own scale cancels them.
        """
        new_path, captured = self._setup(monkeypatch, tmp_path, 120.0, 60.0)

        result = await subtraction.run(new_path, str(tmp_path), None)

        assert result["performed"] is True
        diff = captured["diff"]
        # The star cancels, and so does the sky — both scaled identically.
        assert diff[6, 6] == pytest.approx(0.0, abs=1e-3)
        assert float(np.abs(diff).max()) == pytest.approx(0.0, abs=1e-3)

    async def test_equal_exposures_are_left_untouched(self, monkeypatch, tmp_path):
        """The ordinary homogeneous-archive case must behave exactly as before."""
        new_path, captured = self._setup(monkeypatch, tmp_path, 60.0, 60.0)

        await subtraction.run(new_path, str(tmp_path), None)

        assert float(np.abs(captured["diff"]).max()) == pytest.approx(0.0, abs=1e-3)

    async def test_saturation_mask_sees_unscaled_reference_values(
        self, monkeypatch, tmp_path,
    ):
        """
        The scale is applied to the median stack only, never to the aligned
        references themselves: _build_saturation_mask() compares those against
        SATURATION_ADU, and a reference scaled down (here 300s -> 60s, x0.2)
        would otherwise drop its saturated core below the threshold and escape
        masking entirely.
        """
        shape = (12, 12)
        new_path = _write_frame(tmp_path / "new.fits", exptime=60.0)
        ref_paths = [
            _write_frame(tmp_path / f"ref{i}.fits", exptime=300.0) for i in range(3)
        ]

        new_data = np.full(shape, 10.0, dtype=np.float32)
        ref_data = np.full(shape, 10.0, dtype=np.float32)
        ref_data[5, 5] = float(config.SATURATION_ADU)

        data_by_path = {new_path: new_data}
        for ref_path in ref_paths:
            data_by_path[ref_path] = ref_data

        monkeypatch.setattr(subtraction, "_find_archive_frames", lambda d, f, pa=None, fwhm=None: ref_paths)
        monkeypatch.setattr(subtraction, "_load_frame_data", lambda p: data_by_path[p].copy())
        monkeypatch.setattr(subtraction, "_align_frame", lambda s, t: (s, None))
        monkeypatch.setattr(subtraction, "_pixel_scale_arcsec", lambda path, wcs=None: 1.0)

        captured: dict = {}

        def fake_detect(diff, mask=None, fwhm_min_px=None, pixel_scale_arcsec=None):
            captured["mask"] = mask
            return []

        monkeypatch.setattr(subtraction, "_detect_diff_sources", fake_detect)
        monkeypatch.setattr(subtraction, "_pixel_to_sky", lambda cands, path, wcs=None: [])

        await subtraction.run(new_path, str(tmp_path), None)

        assert captured["mask"] is not None
        assert bool(captured["mask"][5, 5]) is True


# ---------------------------------------------------------------------------
# run() — end-to-end orchestration
# ---------------------------------------------------------------------------

class TestRun:

    async def test_skips_when_too_few_archive_frames(self, monkeypatch, tmp_path):
        monkeypatch.setattr(subtraction, "_find_archive_frames", lambda d, f, pa=None, fwhm=None: ["a.fits", "b.fits"])

        result = await subtraction.run(str(tmp_path / "new.fits"), str(tmp_path), None)

        assert result == {"performed": False, "reference_frame_count": 0, "candidates": []}

    async def test_skips_when_new_frame_unloadable(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            subtraction, "_find_archive_frames",
            lambda d, f, pa=None, fwhm=None: ["a.fits", "b.fits", "c.fits"],
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
            return np.ones(target.shape, dtype=np.float32), None

        monkeypatch.setattr(
            subtraction, "_find_archive_frames",
            lambda d, f, pa=None, fwhm=None: ["ref1.fits", "ref2.fits", "ref3.fits"],
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
            lambda d, f, pa=None, fwhm=None: ["ref1.fits", "ref2.fits", "ref3.fits"],
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
            lambda d, f, pa=None, fwhm=None: ["ref1.fits", "ref2.fits", "ref3.fits"],
        )
        monkeypatch.setattr(subtraction, "_load_frame_data", lambda p: np.ones(shape, dtype=np.float32))
        monkeypatch.setattr(subtraction, "_align_frame", lambda s, t: (np.ones(shape, dtype=np.float32), None))
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
            lambda d, f, pa=None, fwhm=None: ["ref1.fits", "ref2.fits", "ref3.fits"],
        )
        monkeypatch.setattr(
            subtraction, "_load_frame_data",
            lambda p: new_data if p.endswith("new.fits") else np.full(shape, 100.0, dtype=np.float32),
        )
        monkeypatch.setattr(subtraction, "_align_frame", lambda s, t: (np.full(shape, 100.0, dtype=np.float32), None))
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
            lambda d, f, pa=None, fwhm=None: ["ref1.fits", "ref2.fits", "ref3.fits"],
        )
        monkeypatch.setattr(subtraction, "_load_frame_data", lambda p: np.full(shape, 100.0, dtype=np.float32))
        monkeypatch.setattr(subtraction, "_align_frame", lambda s, t: (np.full(shape, 100.0, dtype=np.float32), None))

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
            lambda d, f, pa=None, fwhm=None: ["ref1.fits", "ref2.fits", "ref3.fits"],
        )
        monkeypatch.setattr(subtraction, "_load_frame_data", lambda p: np.ones(shape, dtype=np.float32))
        monkeypatch.setattr(subtraction, "_align_frame", lambda s, t: (np.ones(shape, dtype=np.float32), None))
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
            lambda d, f, pa=None, fwhm=None: [new_path, "ref1.fits", "ref2.fits", "ref3.fits"],
        )

        loaded_paths: list[str] = []

        def fake_load(path):
            loaded_paths.append(path)
            return np.ones((10, 10), dtype=np.float32)

        monkeypatch.setattr(subtraction, "_load_frame_data", fake_load)
        monkeypatch.setattr(subtraction, "_align_frame", lambda s, t: (np.ones((10, 10), dtype=np.float32), None))
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

    async def test_180_degree_rotated_reference_is_prerotated_not_excluded(
        self, monkeypatch, tmp_path,
    ):
        """
        The user's actual reported scenario, end-to-end through run(): a
        reference frame captured with the camera rotated ~180 deg relative
        to the new frame (e.g. a meridian flip between sessions) must still
        be aligned and counted in reference_frame_count — never dropped
        purely for having a very different orientation — and must reach
        _align_frame() already coarse-pre-rotated toward the new frame's own
        orientation, not as raw un-rotated pixel data. See CLAUDE.md's
        "camera rotation" discussion.
        """
        new_wcs = _base_wcs()
        same_orientation_wcs = _base_wcs()
        flipped_wcs = _rotate_wcs(_base_wcs(), 180.0)

        wcs_by_path = {
            "ref_same.fits": same_orientation_wcs,
            "ref_flipped.fits": flipped_wcs,
            "ref_other.fits": same_orientation_wcs,
        }
        raw_ref_data = np.arange(100.0).reshape(10, 10)

        monkeypatch.setattr(
            subtraction, "_find_archive_frames",
            lambda d, f, pa=None, fwhm=None: list(wcs_by_path.keys()),
        )
        monkeypatch.setattr(subtraction, "_load_frame_data", lambda p: raw_ref_data.copy())
        monkeypatch.setattr(subtraction, "_open_wcs", lambda p: wcs_by_path.get(p))

        align_calls: list[np.ndarray] = []

        def fake_align(source, target):
            align_calls.append(source)
            return np.ones((10, 10), dtype=np.float32), None

        monkeypatch.setattr(subtraction, "_align_frame", fake_align)
        monkeypatch.setattr(
            subtraction, "_detect_diff_sources",
            lambda diff, mask=None, fwhm_min_px=None, pixel_scale_arcsec=None: [],
        )
        monkeypatch.setattr(subtraction, "_pixel_to_sky", lambda cands, path, wcs=None: [])

        result = await subtraction.run(
            str(tmp_path / "new.fits"), str(tmp_path), None, wcs=new_wcs,
        )

        # Not excluded: all 3 references (including the 180-degree-flipped
        # one) were successfully aligned.
        assert result["performed"] is True
        assert result["reference_frame_count"] == 3
        assert len(align_calls) == 3

        # The same-orientation references reach _align_frame() unchanged
        # (delta ~= 0, below SUBTRACTION_PREROTATE_MIN_DEG - not worth
        # rotating).
        assert np.array_equal(align_calls[0], raw_ref_data)
        assert np.array_equal(align_calls[2], raw_ref_data)

        # The 180-degree-flipped reference reaches _align_frame() already
        # pre-rotated - i.e. NOT the raw, still-flipped data.
        assert not np.array_equal(align_calls[1], raw_ref_data)
