"""
tests/test_pipeline.py — Unit tests for pipeline.py and watcher.py.

All external dependencies (API client, module functions, subprocess) are
mocked at the pipeline module level. No real FITS I/O or network calls occur.
"""

from __future__ import annotations

import copy
import os
from unittest.mock import AsyncMock, MagicMock, patch, Mock

import pytest

import pipeline
import config


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_GOOD_HEADER = {
    "object_name": "M51",
    "obs_time": "2024-03-15T22:01:34",
    "ra": 202.47,
    "dec": 47.20,
    "observation": {
        "object": "M51",
        "exptime": 120.0,
        "filter": "V",
        "frame_type": "Light",
        "airmass": 1.2,
    },
    "instrument": {},
    "sensor": {},
    "observer": {},
    "software": {},
}

_GOOD_QC = {
    "quality_flag": "OK",
    "fwhm_median": 3.2,
    "fwhm_unit": "arcsec",
    "elongation_median": 1.1,
    "snr_median": 42.0,
    "sky_background": 850.0,
    "sky_sigma": 20.0,
    "star_count": 150,
    "cr_fraction": 0.001,
    "rejected_path": None,
}

_GOOD_ASTRO = {
    "ra_center": 202.47,
    "dec_center": 47.20,
    "fov_deg": 1.25,
    "sources": [
        {"ra": 202.47, "dec": 47.20, "flux": 1000.0, "fwhm": 3.0, "elongation": 1.1},
        {"ra": 202.48, "dec": 47.21, "flux": 800.0, "fwhm": 2.8, "elongation": 1.2},
    ],
}


@pytest.fixture
def fits_file(tmp_path):
    """Create a minimal fake FITS file on disk."""
    f = tmp_path / "frame.fits"
    f.write_bytes(b"SIMPLE  =                    T")
    return f


@pytest.fixture
def mock_modules(monkeypatch, fits_file, tmp_path):
    """
    Patch all pipeline-level module references with controllable mocks.

    Returns the fits file path as a convenience so tests can refer to it.
    """
    # Patch config.FITS_ARCHIVE so archive moves land inside tmp_path
    monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path / "archive"))

    # fits_header — synchronous
    monkeypatch.setattr(
        "pipeline.fits_header.extract_headers",
        lambda p: _GOOD_HEADER,
    )

    # qc — async
    qc_mock = AsyncMock(return_value=_GOOD_QC)
    monkeypatch.setattr("pipeline.qc.analyze", qc_mock)

    # api_client
    api_mock = MagicMock()
    api_mock.post_frame = AsyncMock(return_value="frame-42")
    api_mock.post_sources = AsyncMock(return_value=None)
    api_mock.post_anomalies = AsyncMock(return_value=None)
    monkeypatch.setattr("pipeline.api_client", api_mock)

    # astrometry
    astro_mock = MagicMock()
    # A fresh deep copy per call — pipeline.py mutates source dicts in place
    # (catalog fields, "mag", "_source_id", ...); returning the same shared
    # _GOOD_ASTRO object across tests would leak state between them.
    astro_mock.solve = AsyncMock(side_effect=lambda *a, **kw: copy.deepcopy(_GOOD_ASTRO))
    monkeypatch.setattr("pipeline.astrometry", astro_mock)

    # subtraction — no candidates by default (individual tests override this
    # to exercise the Step 3.5 merge path)
    sub_mock = MagicMock()
    sub_mock.run = AsyncMock(return_value={
        "performed": False,
        "reference_frame_count": 0,
        "candidates": [],
    })
    monkeypatch.setattr("pipeline.subtraction", sub_mock)

    # catalog_matcher — returns enriched sources (same structure, adds catalog fields)
    cat_mock = MagicMock()
    # Match returns sources with catalog fields added. Each source gets a
    # DISTINCT catalog_id (not the same literal "12345" for all of them) —
    # otherwise pipeline.py's Step 4.5 dedup-by-catalog-identity would
    # collapse every source in a test frame into one, which is realistic
    # for genuine duplicate detections of the same object but wrong for
    # these fixtures, which model distinct stars.
    async def mock_match(sources, frame_meta):
        for i, s in enumerate(sources):
            s.setdefault("catalog_name", "Gaia DR3")
            s.setdefault("catalog_id", f"12345-{i}")
            s.setdefault("catalog_mag", 14.5)
            s.setdefault("object_type", "STAR")
        return sources
    cat_mock.match = AsyncMock(side_effect=mock_match)
    monkeypatch.setattr("pipeline.catalog_matcher", cat_mock)

    # photometry — returns sources with photometry fields added
    phot_mock = MagicMock()
    async def mock_measure(fits_path, sources):
        for s in sources:
            s.setdefault("flux_aperture", 1000.0)
            s.setdefault("mag_instrumental", -7.5)
            s.setdefault("mag_calibrated", 14.5)
            s.setdefault("calibrated", True)
        return sources
    phot_mock.measure = AsyncMock(side_effect=mock_measure)
    monkeypatch.setattr("pipeline.photometry", phot_mock)

    # anomaly_detector
    anom_mock = MagicMock()
    anom_mock.detect = AsyncMock(return_value=[])
    monkeypatch.setattr("pipeline.anomaly_detector", anom_mock)

    # Enable normalization in config
    monkeypatch.setattr(config, "NORMALIZE_ENABLED", True)

    # normalizer — mock to return predictable normalized filename
    norm_mock = MagicMock()
    norm_mock.normalize_headers = lambda h: h  # Pass through
    norm_mock.generate_normalized_filename = lambda **kwargs: "M51_L_V_120_2024-03-15T22-01-34.fits"
    monkeypatch.setattr("pipeline.normalizer", norm_mock)

    return fits_file


# Expected normalized filename from mock
_NORMALIZED_FILENAME = "M51_L_V_120_2024-03-15T22-01-34.fits"


# ---------------------------------------------------------------------------
# Pipeline tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_qc_ok_all_steps_called(mock_modules, tmp_path):
    """When QC passes, every downstream step must be called."""
    fits_path = str(mock_modules)
    await pipeline.run(fits_path)

    pipeline.astrometry.solve.assert_called_once_with(fits_path, psf_fwhm_arcsec=3.2)
    pipeline.photometry.measure.assert_called_once()
    pipeline.api_client.post_frame.assert_called_once()
    pipeline.api_client.post_sources.assert_called_once()
    pipeline.catalog_matcher.match.assert_called_once()
    pipeline.anomaly_detector.detect.assert_called_once()
    pipeline.api_client.post_anomalies.assert_called_once()

    # File must have been archived with normalized filename
    archive_path = os.path.join(
        config.FITS_ARCHIVE, "M51", _NORMALIZED_FILENAME
    )
    assert os.path.exists(archive_path), f"Expected archived file at {archive_path}"


@pytest.mark.asyncio
async def test_qc_rejected_stops_pipeline(monkeypatch, fits_file, tmp_path):
    """When QC returns a non-OK flag, no downstream steps should be called."""
    monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path / "archive"))
    monkeypatch.setattr(
        "pipeline.fits_header.extract_headers", lambda p: _GOOD_HEADER
    )
    monkeypatch.setattr(
        "pipeline.qc.analyze",
        AsyncMock(
            return_value={**_GOOD_QC, "quality_flag": "BLUR", "rejected_path": None}
        ),
    )

    api_mock = MagicMock()
    api_mock.post_frame = AsyncMock(return_value="frame-99")
    monkeypatch.setattr("pipeline.api_client", api_mock)

    astro_mock = MagicMock()
    # A fresh deep copy per call — pipeline.py mutates source dicts in place
    # (catalog fields, "mag", "_source_id", ...); returning the same shared
    # _GOOD_ASTRO object across tests would leak state between them.
    astro_mock.solve = AsyncMock(side_effect=lambda *a, **kw: copy.deepcopy(_GOOD_ASTRO))
    monkeypatch.setattr("pipeline.astrometry", astro_mock)

    await pipeline.run(str(fits_file))

    astro_mock.solve.assert_not_called()
    api_mock.post_frame.assert_not_called()

    # No archive directory should have been created
    assert not os.path.exists(os.path.join(config.FITS_ARCHIVE, "M51"))


@pytest.mark.asyncio
async def test_frame_id_propagated(mock_modules):
    """post_sources and post_anomalies must receive the frame_id from post_frame."""
    fits_path = str(mock_modules)
    await pipeline.run(fits_path)

    # post_sources first positional arg is frame_id
    call_args = pipeline.api_client.post_sources.call_args
    assert call_args.args[0] == "frame-42"

    # post_anomalies first positional arg is frame_id
    call_args = pipeline.api_client.post_anomalies.call_args
    assert call_args.args[0] == "frame-42"


@pytest.mark.asyncio
async def test_archive_move_correct_path(mock_modules, tmp_path):
    """The archived file must land at {FITS_ARCHIVE}/{object_name}/{normalized_filename}."""
    fits_path = str(mock_modules)

    await pipeline.run(fits_path)

    # File is archived with normalized filename, not original
    expected = os.path.join(config.FITS_ARCHIVE, "M51", _NORMALIZED_FILENAME)
    assert os.path.exists(expected), f"Expected archived file at {expected}"


@pytest.mark.asyncio
async def test_finder_chart_runs_after_archive_move(mock_modules, monkeypatch):
    """
    Regression test for Known Issues #11: the archive move (step 14.5) must
    happen BEFORE the finder-chart step (step 15), not after — otherwise
    finder_chart.update_charts_for_sources() looks for this frame's own
    epoch at FITS_ARCHIVE/{object}/{filename} before it has actually been
    moved there, and the chart for any single-epoch (first-time) anomaly
    can never be rendered.
    """
    fits_path = str(mock_modules)

    # Give this frame one anomaly with a resolved source_id, so the
    # finder_chart step actually has something to act on.
    pipeline.anomaly_detector.detect = AsyncMock(
        return_value=[{"anomaly_type": "UNKNOWN", "source_id": "src-1"}]
    )

    expected_archive_path = os.path.join(config.FITS_ARCHIVE, "M51", _NORMALIZED_FILENAME)
    archive_existed_at_chart_time = {}

    async def fake_update_charts_for_sources(anomaly_type_by_source_id):
        archive_existed_at_chart_time["exists"] = os.path.exists(expected_archive_path)
        return {sid: True for sid in anomaly_type_by_source_id}

    finder_chart_mock = MagicMock()
    finder_chart_mock.update_charts_for_sources = AsyncMock(
        side_effect=fake_update_charts_for_sources
    )
    monkeypatch.setattr("pipeline.finder_chart", finder_chart_mock)
    monkeypatch.setattr(config, "CHART_ENABLED", True)

    await pipeline.run(fits_path)

    finder_chart_mock.update_charts_for_sources.assert_called_once_with(
        {"src-1": "UNKNOWN"}
    )
    assert archive_existed_at_chart_time.get("exists") is True, (
        "finder_chart.update_charts_for_sources() ran before the archive move — "
        "this frame's own epoch would never be found on disk"
    )
    # And the archive move must still have actually happened.
    assert os.path.exists(expected_archive_path)


@pytest.mark.asyncio
async def test_astrometry_failure_continues(mock_modules):
    """A crash in astrometry.solve must not abort the pipeline."""
    pipeline.astrometry.solve.side_effect = RuntimeError("astap binary missing")

    await pipeline.run(str(mock_modules))

    # Pipeline must have continued and posted the frame
    pipeline.api_client.post_frame.assert_called_once()
    pipeline.api_client.post_sources.assert_called_once()


@pytest.mark.asyncio
async def test_post_frame_failure_aborts(mock_modules):
    """If post_frame raises, post_sources must not be called and no archive move."""
    pipeline.api_client.post_frame.side_effect = RuntimeError("API unreachable")

    fits_path = str(mock_modules)
    await pipeline.run(fits_path)

    pipeline.api_client.post_sources.assert_not_called()

    archive_path = os.path.join(
        config.FITS_ARCHIVE, "M51", os.path.basename(fits_path)
    )
    assert not os.path.exists(archive_path)


@pytest.mark.asyncio
async def test_optional_modules_absent(monkeypatch, fits_file, tmp_path):
    """
    When optional modules are set to None (not yet implemented), the pipeline
    must still run to completion: post the frame and archive the file.
    """
    monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path / "archive"))
    monkeypatch.setattr(
        "pipeline.fits_header.extract_headers", lambda p: _GOOD_HEADER
    )
    monkeypatch.setattr("pipeline.qc.analyze", AsyncMock(return_value=_GOOD_QC))

    api_mock = MagicMock()
    api_mock.post_frame = AsyncMock(return_value="frame-99")
    api_mock.post_sources = AsyncMock(return_value=None)
    api_mock.post_anomalies = AsyncMock(return_value=None)
    monkeypatch.setattr("pipeline.api_client", api_mock)

    # Disable all optional modules (including normalizer)
    monkeypatch.setattr("pipeline.astrometry", None)
    monkeypatch.setattr("pipeline.photometry", None)
    monkeypatch.setattr("pipeline.catalog_matcher", None)
    monkeypatch.setattr("pipeline.anomaly_detector", None)
    monkeypatch.setattr("pipeline.normalizer", None)

    await pipeline.run(str(fits_file))

    api_mock.post_frame.assert_called_once()
    api_mock.post_sources.assert_called_once()
    api_mock.post_anomalies.assert_called_once()

    # When normalizer is None, file is archived with original name
    archive_path = os.path.join(
        config.FITS_ARCHIVE, "M51", fits_file.name
    )
    assert os.path.exists(archive_path)


# ---------------------------------------------------------------------------
# Exception-handler branch coverage tests (lines 159-160, 172-176, 208-209,
# 228-229, 249-250, 269-270, 292-293)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_photometry_failure_continues(mock_modules):
    """photometry.measure raising must not abort the pipeline (lines 159-160)."""
    pipeline.photometry.measure.side_effect = RuntimeError("sensor error")

    await pipeline.run(str(mock_modules))

    # Pipeline must have continued past the photometry failure
    pipeline.api_client.post_frame.assert_called_once()


@pytest.mark.asyncio
async def test_api_client_none_skips_api_steps(mock_modules, monkeypatch):
    """api_client set to None triggers early-return guard (lines 172-176)."""
    monkeypatch.setattr("pipeline.api_client", None)

    await pipeline.run(str(mock_modules))

    # No archive file should exist — pipeline returned before archive step
    archive_path = os.path.join(
        config.FITS_ARCHIVE, "M51", os.path.basename(str(mock_modules))
    )
    assert not os.path.exists(archive_path)


@pytest.mark.asyncio
async def test_post_sources_exception_continues(mock_modules):
    """post_sources raising must not abort the pipeline (lines 208-209)."""
    pipeline.api_client.post_sources.side_effect = RuntimeError("network timeout")

    await pipeline.run(str(mock_modules))

    # Catalog matching and anomaly posting must still have run
    pipeline.api_client.post_anomalies.assert_called_once()


@pytest.mark.asyncio
async def test_catalog_match_exception_continues(mock_modules):
    """catalog_matcher.match raising must not abort the pipeline (lines 228-229)."""
    pipeline.catalog_matcher.match.side_effect = RuntimeError("Gaia outage")

    await pipeline.run(str(mock_modules))

    # Anomaly detection and posting must still proceed
    pipeline.api_client.post_anomalies.assert_called_once()


@pytest.mark.asyncio
async def test_anomaly_detection_exception_continues(mock_modules):
    """anomaly_detector.detect raising must not abort the pipeline (lines 249-250)."""
    pipeline.anomaly_detector.detect.side_effect = RuntimeError("JPL timeout")

    await pipeline.run(str(mock_modules))

    # post_anomalies must still be called (with an empty anomaly list)
    pipeline.api_client.post_anomalies.assert_called_once()


@pytest.mark.asyncio
async def test_post_anomalies_exception_continues(mock_modules):
    """post_anomalies raising must not abort the pipeline (lines 269-270)."""
    pipeline.api_client.post_anomalies.side_effect = RuntimeError("API down")

    fits_path = str(mock_modules)
    await pipeline.run(fits_path)

    # Archive move must still have happened (with normalized filename)
    archive_path = os.path.join(
        config.FITS_ARCHIVE, "M51", _NORMALIZED_FILENAME
    )
    assert os.path.exists(archive_path)


@pytest.mark.asyncio
async def test_archive_failure_is_logged(mock_modules, monkeypatch):
    """shutil.move raising must be caught and swallowed (lines 292-293)."""
    monkeypatch.setattr(
        "pipeline.shutil.move",
        MagicMock(side_effect=OSError("disk full")),
    )

    # Must complete without raising
    await pipeline.run(str(mock_modules))

    # All API steps must have completed before the failed archive move
    pipeline.api_client.post_anomalies.assert_called_once()


# ---------------------------------------------------------------------------
# Regression tests for previously-fixed bugs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subtraction_candidates_merged_without_crashing(mock_modules):
    """
    Regression test: subtraction candidates must be merged into `sources`
    without raising UnboundLocalError.

    Previously, `sources` was first assigned in what is now Step 4 (after
    Step 3.5's `sources = sources + sub_candidates`), so any frame where
    subtraction.run() returned non-empty candidates crashed with
    UnboundLocalError (caught, but silently discarding every candidate —
    see CLAUDE.md Known Issues #3, now fixed).
    """
    subtraction_candidate = {
        "ra": 202.50, "dec": 47.25,
        "flux": 500.0, "snr": 8.0, "fwhm": 2.9, "elongation": 1.3,
        "mag": None,
        "_from_subtraction": True,
    }
    pipeline.subtraction.run.return_value = {
        "performed": True,
        "reference_frame_count": 5,
        "candidates": [subtraction_candidate],
    }

    await pipeline.run(str(mock_modules))

    # catalog_matcher.match must have received the 2 astrometry sources
    # PLUS the 1 subtraction candidate (3 total) — proof the merge happened
    # before catalog matching, not that it silently vanished.
    match_call_sources = pipeline.catalog_matcher.match.call_args.args[0]
    assert len(match_call_sources) == 3
    assert any(s.get("_from_subtraction") for s in match_call_sources)

    # And it must have made it all the way through to post_sources/detect.
    posted_sources = pipeline.api_client.post_sources.call_args.args[2]
    assert len(posted_sources) == 3
    assert any(s.get("_from_subtraction") for s in posted_sources)


@pytest.mark.asyncio
async def test_mag_field_populated_from_calibrated_magnitude(mock_modules):
    """
    Regression test: every source must get a top-level "mag" field derived
    from photometry, matching the POST /frames/{id}/sources payload
    documented in CLAUDE.md.

    Previously nothing ever set "mag" (photometry.py only sets
    mag_instrumental/mag_calibrated), so it stayed None for every real
    source, delta_mag in anomaly_detector.py was always None, and
    VARIABLE_STAR/BINARY_STAR/SUPERNOVA_CANDIDATE could never fire.
    """
    await pipeline.run(str(mock_modules))

    posted_sources = pipeline.api_client.post_sources.call_args.args[2]
    assert len(posted_sources) == 2
    for s in posted_sources:
        assert s["mag"] == pytest.approx(14.5)  # mock_measure's mag_calibrated

    # anomaly_detector.detect must see the same populated "mag" values.
    detect_call_sources = pipeline.anomaly_detector.detect.call_args.args[1]
    for s in detect_call_sources:
        assert s["mag"] == pytest.approx(14.5)


@pytest.mark.asyncio
async def test_source_id_propagated_to_anomaly_detector(mock_modules):
    """
    Regression test: the "source_ids" array returned by post_sources() must
    be zipped back onto each source dict as "_source_id", so
    anomaly_detector.py can populate anomalies[].source_id — previously
    nothing on the pipeline side ever looked up sources.id at all, so
    anomalies.source_id was always null in the API (see CLAUDE.md Known
    Issues).
    """
    pipeline.api_client.post_sources.return_value = ["src-id-0", "src-id-1"]

    await pipeline.run(str(mock_modules))

    detect_call_sources = pipeline.anomaly_detector.detect.call_args.args[1]
    assert [s["_source_id"] for s in detect_call_sources] == ["src-id-0", "src-id-1"]


@pytest.mark.asyncio
async def test_source_id_not_attached_on_length_mismatch(mock_modules):
    """If post_sources() returns a source_ids list of the wrong length (API
    contract violation / partial failure), the pipeline must not attach
    bogus source_ids and must not crash."""
    pipeline.api_client.post_sources.return_value = ["only-one-id"]

    await pipeline.run(str(mock_modules))

    detect_call_sources = pipeline.anomaly_detector.detect.call_args.args[1]
    assert all("_source_id" not in s for s in detect_call_sources)


@pytest.mark.asyncio
async def test_source_id_absent_when_post_sources_returns_none(mock_modules):
    """post_sources() returning None (older API, or the call failed) must
    not attach "_source_id" to any source, and must not crash."""
    pipeline.api_client.post_sources.return_value = None

    await pipeline.run(str(mock_modules))

    detect_call_sources = pipeline.anomaly_detector.detect.call_args.args[1]
    assert all("_source_id" not in s for s in detect_call_sources)


@pytest.mark.asyncio
async def test_mag_field_is_none_when_uncalibrated(mock_modules):
    """
    Regression test for docs/ISSUES.md #2: when photometry could not
    calibrate a source (fewer than 3 Gaia DR3 references in the frame),
    "mag" must be None — it must NEVER fall back to the raw uncalibrated
    mag_instrumental. mag_instrumental has no absolute zero-point; falling
    back to it was exactly what produced extreme (e.g. -15) "magnitude"
    values for entire uncalibrated frames in production.
    """

    async def mock_measure_uncalibrated(fits_path, sources):
        for s in sources:
            s["flux_aperture"] = 100.0
            s["mag_instrumental"] = -5.0
            s["mag_calibrated"] = None
            s["calibrated"] = False
        return sources

    pipeline.photometry.measure.side_effect = mock_measure_uncalibrated

    await pipeline.run(str(mock_modules))

    posted_sources = pipeline.api_client.post_sources.call_args.args[2]
    for s in posted_sources:
        assert s["mag"] is None


@pytest.mark.asyncio
async def test_mag_field_uses_calibrated_value_when_available(mock_modules):
    """When photometry did calibrate a source, "mag" must be mag_calibrated."""

    async def mock_measure_calibrated(fits_path, sources):
        for s in sources:
            s["flux_aperture"]   = 100.0
            s["mag_instrumental"] = -5.0
            s["mag_calibrated"]   = 17.3
            s["calibrated"]       = True
        return sources

    pipeline.photometry.measure.side_effect = mock_measure_calibrated

    await pipeline.run(str(mock_modules))

    posted_sources = pipeline.api_client.post_sources.call_args.args[2]
    for s in posted_sources:
        assert s["mag"] == pytest.approx(17.3)


# ---------------------------------------------------------------------------
# _dedupe_by_catalog_identity — unit tests
#
# Regression coverage for the Vesta observation_count=9-for-6-frames bug:
# a single moving object matched by BOTH the normal source extractor AND one
# or more nearby image-subtraction candidates (MOVING_CONE_ARCSEC is wide)
# resolved to the same MPC identity multiple times within one frame, each
# posted as a separate `sources` observation and classified as a separate
# duplicate ASTEROID/COMET anomaly.
# ---------------------------------------------------------------------------


class TestDedupeByCatalogIdentity:

    def test_no_duplicates_returns_all_sources_unchanged(self):
        sources = [
            {"ra": 1.0, "dec": 1.0, "catalog_name": "Gaia DR3", "catalog_id": "A", "flux": 100.0},
            {"ra": 2.0, "dec": 2.0, "catalog_name": "Gaia DR3", "catalog_id": "B", "flux": 200.0},
        ]
        result = pipeline._dedupe_by_catalog_identity(sources, {})
        assert result == sources

    def test_uncatalogued_sources_are_never_merged(self):
        """Sources with catalog_name=None have no stable identity to merge
        on — every uncatalogued detection must be kept, even if several
        share identical ra/dec (which _dedupe never inspects)."""
        sources = [
            {"ra": 1.0, "dec": 1.0, "catalog_name": None, "catalog_id": None, "flux": 100.0},
            {"ra": 1.0, "dec": 1.0, "catalog_name": None, "catalog_id": None, "flux": 100.0},
        ]
        result = pipeline._dedupe_by_catalog_identity(sources, {})
        assert len(result) == 2

    def test_duplicate_catalog_identity_collapses_to_one(self):
        """Regression: the Vesta case — the same MPC object detected twice
        (once from normal extraction, once from a subtraction candidate)
        within one frame must collapse to a single source."""
        normal_detection = {
            "ra": 167.274, "dec": 17.359, "catalog_name": "MPC", "catalog_id": "Vesta",
            "flux": 500.0, "_from_subtraction": False,
        }
        subtraction_candidate = {
            "ra": 167.275, "dec": 17.360, "catalog_name": "MPC", "catalog_id": "Vesta",
            "flux": 9000.0, "_from_subtraction": True,
        }
        other_asteroid = {
            "ra": 10.0, "dec": 10.0, "catalog_name": "MPC", "catalog_id": "2011 WZ72",
            "flux": 50.0,
        }

        result = pipeline._dedupe_by_catalog_identity(
            [normal_detection, subtraction_candidate, other_asteroid], {}
        )

        assert len(result) == 2
        vesta_kept = next(s for s in result if s["catalog_id"] == "Vesta")
        # The non-subtraction detection wins even though its flux is lower —
        # preferred as the more astrometrically/photometrically precise one.
        assert vesta_kept is normal_detection

    def test_prefers_non_subtraction_detection_regardless_of_order(self):
        """The non-subtraction detection wins whether it's seen first or
        second — the preference isn't just "first one wins"."""
        subtraction_candidate = {
            "ra": 1.0, "dec": 1.0, "catalog_name": "MPC", "catalog_id": "Vesta",
            "flux": 9000.0, "_from_subtraction": True,
        }
        normal_detection = {
            "ra": 1.0, "dec": 1.0, "catalog_name": "MPC", "catalog_id": "Vesta",
            "flux": 500.0, "_from_subtraction": False,
        }

        result = pipeline._dedupe_by_catalog_identity(
            [subtraction_candidate, normal_detection], {}
        )

        assert len(result) == 1
        assert result[0] is normal_detection

    def test_prefers_brighter_among_same_kind_duplicates(self):
        """When both duplicates are the same kind (e.g. two subtraction
        candidates both matching the same wide MPC cone), keep the
        brighter (higher flux) one."""
        dim = {
            "ra": 1.0, "dec": 1.0, "catalog_name": "MPC", "catalog_id": "Vesta",
            "flux": 100.0, "_from_subtraction": True,
        }
        bright = {
            "ra": 1.01, "dec": 1.01, "catalog_name": "MPC", "catalog_id": "Vesta",
            "flux": 9000.0, "_from_subtraction": True,
        }

        result = pipeline._dedupe_by_catalog_identity([dim, bright], {})

        assert len(result) == 1
        assert result[0] is bright

    def test_empty_list_returns_empty_list(self):
        assert pipeline._dedupe_by_catalog_identity([], {}) == []


@pytest.mark.asyncio
async def test_pipeline_dedupes_duplicate_catalog_matches_before_posting(mock_modules):
    """
    Full-pipeline regression test: when catalog_matcher (mocked here) matches
    two sources in this frame to the SAME catalog identity — the real-world
    scenario being a subtraction candidate and a normal detection both
    resolving to the same MPC asteroid — only one must reach POST
    /frames/{id}/sources and anomaly_detector.detect(), not two.
    """
    subtraction_candidate = {
        "ra": 202.4705, "dec": 47.2005,
        "flux": 5000.0, "snr": 20.0, "fwhm": 3.0, "elongation": 1.2,
        "mag": None,
        "_from_subtraction": True,
    }
    pipeline.subtraction.run.return_value = {
        "performed": True,
        "reference_frame_count": 5,
        "candidates": [subtraction_candidate],
    }

    # Force BOTH the first astrometry source and the subtraction candidate
    # to resolve to the identical MPC identity, mimicking Vesta being seen
    # twice in one frame.
    async def mock_match_same_identity(sources, frame_meta):
        for s in sources:
            if s.get("_from_subtraction") or s["ra"] == 202.47:
                s["catalog_name"] = "MPC"
                s["catalog_id"] = "Vesta"
                s["object_type"] = "ASTEROID"
            else:
                s.setdefault("catalog_name", "Gaia DR3")
                s.setdefault("catalog_id", "other-star")
                s.setdefault("catalog_mag", 14.5)
                s.setdefault("object_type", "STAR")
        return sources

    pipeline.catalog_matcher.match.side_effect = mock_match_same_identity

    await pipeline.run(str(mock_modules))

    posted_sources = pipeline.api_client.post_sources.call_args.args[2]
    vesta_entries = [s for s in posted_sources if s.get("catalog_id") == "Vesta"]
    assert len(vesta_entries) == 1, "Duplicate Vesta detections in one frame must collapse to one"

    detect_sources = pipeline.anomaly_detector.detect.call_args.args[1]
    vesta_in_detect = [s for s in detect_sources if s.get("catalog_id") == "Vesta"]
    assert len(vesta_in_detect) == 1


# ---------------------------------------------------------------------------
# Watcher tests
# ---------------------------------------------------------------------------


def _make_event(src_path: str, is_directory: bool = False) -> MagicMock:
    """Build a minimal watchdog FileCreatedEvent-like mock."""
    event = MagicMock()
    event.src_path = src_path
    event.is_directory = is_directory
    return event


def test_watcher_ignores_non_fits():
    """on_created with a .jpg file must not dispatch to the pipeline."""
    from watcher import FitsEventHandler

    handler = FitsEventHandler()
    event = _make_event("/fits/incoming/photo.jpg")

    with patch("watcher.asyncio.run") as mock_run:
        handler.on_created(event)
        mock_run.assert_not_called()


def test_watcher_dispatches_fits():
    """on_created with a .fits file must call asyncio.run(pipeline.run(...))."""
    from watcher import FitsEventHandler

    handler = FitsEventHandler()
    event = _make_event("/fits/incoming/frame.fits")

    # process_fits_file() now requires the path to exist (duplicate-event
    # guard) — the test path is fake, so stub existence for this call.
    with (
        patch("watcher.time.sleep"),
        patch("watcher.os.path.exists", return_value=True),
        patch("watcher.asyncio.run") as mock_run,
    ):
        handler.on_created(event)
        mock_run.assert_called_once()
        # The coroutine passed to asyncio.run is pipeline.run(path)
        called_coro = mock_run.call_args.args[0]
        # Coroutine name should be 'run' (from pipeline.run)
        assert called_coro.__name__ == "run"
        # Clean up the coroutine to avoid ResourceWarning
        called_coro.close()


def test_watcher_dispatches_fit_uppercase():
    """on_created with a .FIT extension (uppercase) must also be dispatched."""
    from watcher import FitsEventHandler

    handler = FitsEventHandler()
    event = _make_event("/fits/incoming/FRAME.FIT")

    with (
        patch("watcher.time.sleep"),
        patch("watcher.os.path.exists", return_value=True),
        patch("watcher.asyncio.run") as mock_run,
    ):
        handler.on_created(event)
        mock_run.assert_called_once()
        called_coro = mock_run.call_args.args[0]
        assert called_coro.__name__ == "run"
        called_coro.close()


def test_watcher_ignores_directory_event():
    """Directory creation events must be silently ignored."""
    from watcher import FitsEventHandler

    handler = FitsEventHandler()
    event = _make_event("/fits/incoming/subdir/", is_directory=True)

    with patch("watcher.asyncio.run") as mock_run:
        handler.on_created(event)
        mock_run.assert_not_called()


# ---------------------------------------------------------------------------
# Watcher — duplicate-dispatch guard (regression: the same file was
# registered as two separate frames a few seconds apart — see the
# `_paths_in_flight` module comment in watcher.py).
# ---------------------------------------------------------------------------


def test_process_fits_file_skips_nonexistent_path():
    """A path that doesn't exist (already processed and moved away by an
    earlier duplicate event) must not be dispatched."""
    import watcher

    with patch("watcher.asyncio.run") as mock_run:
        watcher.process_fits_file("/fits/incoming/does-not-exist.fits")
        mock_run.assert_not_called()


def test_process_fits_file_skips_path_already_in_flight(tmp_path):
    """A path already marked in-flight (a duplicate on_created event firing
    while the first dispatch is still running) must be skipped, not
    processed a second time."""
    import watcher

    fits_path = str(tmp_path / "frame.fits")
    (tmp_path / "frame.fits").write_bytes(b"SIMPLE  =                    T")
    real_path = os.path.realpath(fits_path)

    watcher._paths_in_flight.add(real_path)
    try:
        with patch("watcher.asyncio.run") as mock_run:
            watcher.process_fits_file(fits_path)
            mock_run.assert_not_called()
    finally:
        watcher._paths_in_flight.discard(real_path)


def test_process_fits_file_clears_in_flight_marker_after_success(tmp_path):
    """The in-flight marker must be released once processing finishes, so a
    later, legitimate re-processing of a different file at the same path
    isn't permanently blocked."""
    import watcher

    fits_path = str(tmp_path / "frame.fits")
    (tmp_path / "frame.fits").write_bytes(b"SIMPLE  =                    T")
    real_path = os.path.realpath(fits_path)

    with patch("watcher.asyncio.run") as mock_run:
        watcher.process_fits_file(fits_path)
        mock_run.assert_called_once()

    assert real_path not in watcher._paths_in_flight


def test_process_fits_file_clears_in_flight_marker_even_on_failure(tmp_path):
    """A crash inside asyncio.run() must not leave the path permanently
    stuck in `_paths_in_flight` (which would silently black-hole every
    future file at that same path)."""
    import watcher

    fits_path = str(tmp_path / "frame.fits")
    (tmp_path / "frame.fits").write_bytes(b"SIMPLE  =                    T")
    real_path = os.path.realpath(fits_path)

    with patch("watcher.asyncio.run", side_effect=RuntimeError("boom")) as mock_run:
        with pytest.raises(RuntimeError):
            watcher.process_fits_file(fits_path)
        # asyncio.run() is mocked out, so the coroutine it was given is
        # never actually awaited — close it explicitly to avoid an unrelated
        # ResourceWarning from the garbage collector.
        mock_run.call_args.args[0].close()

    assert real_path not in watcher._paths_in_flight
