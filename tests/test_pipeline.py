"""
tests/test_pipeline.py — Unit tests for pipeline.py and watcher.py.

All external dependencies (API client, module functions, subprocess) are
mocked at the pipeline module level. No real FITS I/O or network calls occur.
"""

from __future__ import annotations

import copy
import os
from unittest.mock import AsyncMock, MagicMock, patch, Mock

import numpy as np
import pytest
from astropy.io import fits
from astropy.wcs import WCS as AstropyWCS

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
    # The same measurement before any plate scale was applied — 2.0 px at
    # _GOOD_ASTRO's solved 1.6"/px is exactly the 3.2" above, so the default
    # fixture is a frame whose headers and plate solve agree.
    "fwhm_median_px": 2.0,
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
    "pixel_scale_arcsec": 1.6,
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
    # [] rather than None: None now means "the sources did not reach the API"
    # and triggers a recovery re-queue (audit 2026-08-18, finding H19), which
    # is not what most of these tests are exercising.
    api_mock.post_sources = AsyncMock(return_value=[])
    api_mock.post_anomalies = AsyncMock(return_value=True)
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
    # get_gaia_stars/get_mpc_objects — used by pipeline.py's forced-photometry
    # step (Step 5.6) to reuse the field lists match() already fetched. Empty
    # by default so forced_photometry.run() below has nothing eligible to
    # force in the common case; individual tests override these to exercise
    # the merge path.
    cat_mock.get_gaia_stars = MagicMock(return_value=[])
    cat_mock.get_mpc_objects = MagicMock(return_value=[])
    monkeypatch.setattr("pipeline.catalog_matcher", cat_mock)

    # forced_photometry — no recovered sources by default (mirrors
    # subtraction's sub_mock above; individual tests override this to
    # exercise the Step 5.6 merge path)
    forced_phot_mock = MagicMock()
    forced_phot_mock.run = AsyncMock(return_value=[])
    monkeypatch.setattr("pipeline.forced_photometry", forced_phot_mock)

    # photometry — returns sources with photometry fields added
    phot_mock = MagicMock()
    async def mock_measure(fits_path, sources, skip_calibration=False, **kwargs):
        for s in sources:
            s.setdefault("flux_aperture", 1000.0)
            s.setdefault("mag_instrumental", -7.5)
            s.setdefault("mag_calibrated", 14.5)
            s.setdefault("calibrated", True)
            # zero_point/zero_point_err — same value on every source, mirroring
            # the real photometry.measure(); pipeline.py's Step 5.6 reads this
            # back off any source to calibrate its own forced measurements.
            s.setdefault("zero_point", 14.5)
            s.setdefault("zero_point_err", 0.05)
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
    # is_narrowband — mirrors the real modules.normalizer.is_narrowband()
    # closely enough for these fixtures: every test header filter used here
    # ("V" by default) is already a canonical short code, so a plain
    # membership check against config.NARROWBAND_FILTERS gives the same
    # answer the real function would.
    norm_mock.is_narrowband = lambda f: f in config.NARROWBAND_FILTERS
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

    pipeline.astrometry.solve.assert_called_once_with(
        fits_path, psf_fwhm_arcsec=3.2, psf_fwhm_px=2.0,
    )
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


# ---------------------------------------------------------------------------
# Narrowband filter wiring: skip_calibration + per-source "_filter" tagging
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_broadband_filter_does_not_skip_calibration(mock_modules):
    """_GOOD_HEADER's filter is 'V' (broadband) — skip_calibration must be False."""
    await pipeline.run(str(mock_modules))

    _, kwargs = pipeline.photometry.measure.call_args
    assert kwargs.get("skip_calibration") is False


@pytest.mark.asyncio
async def test_narrowband_filter_skips_calibration(monkeypatch, mock_modules):
    """A narrowband (Hα) frame must call photometry.measure(skip_calibration=True)."""
    narrowband_header = copy.deepcopy(_GOOD_HEADER)
    narrowband_header["observation"]["filter"] = "Ha"
    monkeypatch.setattr("pipeline.fits_header.extract_headers", lambda p: narrowband_header)

    await pipeline.run(str(mock_modules))

    _, kwargs = pipeline.photometry.measure.call_args
    assert kwargs.get("skip_calibration") is True


@pytest.mark.asyncio
async def test_sources_are_tagged_with_frame_filter(mock_modules):
    """
    Every source reaching anomaly_detector.detect() must carry "_filter" —
    needed to restrict its historical Δmag comparison to same-filter epochs
    (see modules/anomaly_detector/_history.py's _same_filter_history()).
    """
    await pipeline.run(str(mock_modules))

    detect_call_sources = pipeline.anomaly_detector.detect.call_args.args[1]
    assert detect_call_sources, "expected at least one source"
    assert all(s.get("_filter") == "V" for s in detect_call_sources)


# ---------------------------------------------------------------------------
# _from_wire_source() / detect_anomalies_for_frame_id(): standalone-task
# filter propagation
# ---------------------------------------------------------------------------


def test_from_wire_source_attaches_frame_filter():
    """
    _from_wire_source() has no per-source filter field on the wire
    (source_observations has none) — the caller must pass the parent
    frame's own filter through explicitly.
    """
    api_source = {"ra": 202.47, "dec": 47.20, "mag": 14.5, "source_id": "src-1"}
    result = pipeline._from_wire_source(api_source, frame_filter="Ha")
    assert result["_filter"] == "Ha"


def test_from_wire_source_defaults_filter_to_none():
    """Omitting frame_filter must not crash and must leave "_filter" as None."""
    api_source = {"ra": 202.47, "dec": 47.20, "mag": 14.5}
    result = pipeline._from_wire_source(api_source)
    assert result["_filter"] is None


def test_from_wire_source_reconstructs_near_edge():
    """
    Regression for the 2026-08-07 T_CrB coma fix: "near_edge" must round-trip
    through the wire and back, same as "saturated" — a standalone
    DETECT_ANOMALIES re-run has no in-memory pixel position to recompute it
    from, so anomaly_detector.py's edge-aware SPACE_DEBRIS threshold would
    silently never fire on that path if this were lost.
    """
    api_source = {"ra": 202.47, "dec": 47.20, "mag": 14.5, "near_edge": True}
    result = pipeline._from_wire_source(api_source)
    assert result["near_edge"] is True


def test_from_wire_source_defaults_near_edge_to_false():
    """An API response predating this field (or a central source) must not crash."""
    api_source = {"ra": 202.47, "dec": 47.20, "mag": 14.5}
    result = pipeline._from_wire_source(api_source)
    assert result["near_edge"] is False


def test_from_wire_source_reconstructs_mag_err_and_snr():
    """
    A standalone DETECT_ANOMALIES re-run judges a source on the same terms as
    the in-process one: M3's significance gate reads mag_err, and H11's
    _survives_edge_zone() reads snr (audit 2026-08-18).
    """
    api_source = {"ra": 1.0, "dec": 2.0, "mag": 14.5, "mag_err": 0.05, "snr": 12.3}
    result = pipeline._from_wire_source(api_source)
    assert result["mag_err"] == pytest.approx(0.05)
    assert result["snr"] == pytest.approx(12.3)


def test_from_wire_source_defaults_mag_err_and_snr_to_none():
    result = pipeline._from_wire_source({"ra": 1.0, "dec": 2.0, "mag": 14.5})
    assert result["mag_err"] is None
    assert result["snr"] is None


@pytest.mark.asyncio
async def test_detect_anomalies_for_frame_id_propagates_frame_filter(monkeypatch):
    """
    detect_anomalies_for_frame_id() reconstructs sources purely from the API
    — every reconstructed source must carry the PARENT FRAME's own filter
    (GET /frames/{id}'s flattened "filter" field), since source_observations
    itself has no per-source filter column.
    """
    api_mock = MagicMock()
    api_mock.get_frame = AsyncMock(return_value={
        "filename": "M51_L_Ha_300_2024-03-15T22-01-34.fits",
        "obs_time": "2024-03-15T22:01:34",
        "filter": "Ha",
    })
    api_mock.get_frame_sources = AsyncMock(return_value=[
        {"ra": 202.47, "dec": 47.20, "mag": 14.5, "source_id": "src-1"},
    ])
    api_mock.post_anomalies = AsyncMock(return_value=None)
    monkeypatch.setattr("pipeline.api_client", api_mock)

    anom_mock = MagicMock()
    anom_mock.detect = AsyncMock(return_value=[])
    monkeypatch.setattr("pipeline.anomaly_detector", anom_mock)

    await pipeline.detect_anomalies_for_frame_id("frame-123")

    detect_call_sources = pipeline.anomaly_detector.detect.call_args.args[1]
    assert detect_call_sources[0]["_filter"] == "Ha"


@pytest.mark.asyncio
@pytest.mark.parametrize("frame_extra", [
    {"exptime": 120},                    # flattened (docs/API.md shape)
    {"observation": {"exptime": 120}},   # nested shape some API versions return
])
async def test_detect_anomalies_for_frame_id_recomputes_the_midpoint(monkeypatch, frame_extra):
    """
    Audit 2026-08-18, finding C9: the standalone path has no local FITS, so
    it rebuilds obs_time_mid from the stored obs_time + exptime.
    """
    api_mock = MagicMock()
    api_mock.get_frame = AsyncMock(return_value={
        "filename": "M51_Light_V_120_2024-03-15T22-00-00.fits",
        "obs_time": "2024-03-15T22:00:00",
        **frame_extra,
    })
    api_mock.get_frame_sources = AsyncMock(return_value=[])
    api_mock.post_anomalies = AsyncMock(return_value=True)
    monkeypatch.setattr("pipeline.api_client", api_mock)

    anom_mock = MagicMock()
    anom_mock.detect = AsyncMock(return_value=[])
    monkeypatch.setattr("pipeline.anomaly_detector", anom_mock)

    await pipeline.detect_anomalies_for_frame_id("frame-123")

    frame_meta = anom_mock.detect.call_args.args[3]
    assert frame_meta["obs_time"] == "2024-03-15T22:00:00"
    assert frame_meta["obs_time_mid"].startswith("2024-03-15T22:01:00")


@pytest.mark.asyncio
async def test_qc_rejected_registers_frame_with_empty_sources_and_archives(mock_modules, tmp_path):
    """
    When QC returns a non-OK flag, analyze_frame() no longer stops early:
    astrometry/subtraction/catalog matching/photometry/forced photometry are
    all skipped, but the frame IS still registered (with its QC metrics and
    the non-"OK" quality_flag), POST /frames/{id}/sources IS still called
    with an empty list (retracting any previously-linked sources), and the
    file is still archived — there is no separate "rejected" move for a QC
    failure anymore.
    """
    fits_path = str(mock_modules)
    pipeline.qc.analyze.return_value = {**_GOOD_QC, "quality_flag": "BLUR", "rejected_path": None}

    result = await pipeline.analyze_frame(fits_path)

    assert result is not None
    assert result["quality_flag"] == "BLUR"
    assert result["sources"] == []

    # Every downstream detection/measurement step must be skipped.
    pipeline.astrometry.solve.assert_not_called()
    pipeline.subtraction.run.assert_not_called()
    pipeline.catalog_matcher.match.assert_not_called()
    pipeline.photometry.measure.assert_not_called()
    pipeline.forced_photometry.run.assert_not_called()

    # The frame IS still registered, and its (empty) source list IS still
    # posted — this is what retracts/purges a re-analysis "downgrade".
    pipeline.api_client.post_frame.assert_called_once()
    pipeline.api_client.post_sources.assert_called_once_with(
        result["frame_id"], os.path.basename(fits_path), [],
    )

    # The file is still archived — no separate "rejected" move for a QC
    # failure anymore.
    archive_path = os.path.join(config.FITS_ARCHIVE, "M51", _NORMALIZED_FILENAME)
    assert os.path.exists(archive_path), f"Expected archived file at {archive_path}"


@pytest.mark.asyncio
async def test_reanalysis_downgrade_retracts_sources_without_move_crash(mock_modules, tmp_path):
    """
    Re-analyzing an already-archived frame that now fails QC (e.g. after
    tightening a threshold) must retract its previously-posted sources via
    an empty POST /frames/{id}/sources call, and the same-path shutil.move
    guard (Step 8) must not raise even though fits_path already equals
    dest_path for this second pass.
    """
    fits_path = str(mock_modules)

    # First pass: QC passes, frame is archived normally.
    first_result = await pipeline.analyze_frame(fits_path)
    assert first_result["quality_flag"] == "OK"
    archived_path = os.path.join(config.FITS_ARCHIVE, "M51", _NORMALIZED_FILENAME)
    assert os.path.exists(archived_path)

    # Reset call counters so the second pass's assertions are unambiguous.
    pipeline.astrometry.solve.reset_mock()
    pipeline.subtraction.run.reset_mock()
    pipeline.catalog_matcher.match.reset_mock()
    pipeline.photometry.measure.reset_mock()
    pipeline.forced_photometry.run.reset_mock()
    pipeline.api_client.post_frame.reset_mock()
    pipeline.api_client.post_sources.reset_mock()

    # Second pass: re-analyze the now-archived file (a bare-path re-run, same
    # convention as _resolve_bare_filename()'s target) — QC now rejects it.
    pipeline.qc.analyze.return_value = {**_GOOD_QC, "quality_flag": "BLUR", "rejected_path": None}

    second_result = await pipeline.analyze_frame(archived_path)

    assert second_result["quality_flag"] == "BLUR"
    assert second_result["sources"] == []
    pipeline.astrometry.solve.assert_not_called()
    pipeline.subtraction.run.assert_not_called()
    pipeline.catalog_matcher.match.assert_not_called()
    pipeline.photometry.measure.assert_not_called()
    pipeline.forced_photometry.run.assert_not_called()

    pipeline.api_client.post_frame.assert_called_once()
    pipeline.api_client.post_sources.assert_called_once_with(
        second_result["frame_id"], os.path.basename(archived_path), [],
    )

    # The same-path shutil.move guard must have skipped the move (fits_path
    # already equals dest_path here) rather than raising — the file must
    # still be sitting at the archive path afterwards.
    assert os.path.exists(archived_path)


@pytest.mark.asyncio
async def test_run_skips_anomaly_detection_and_charts_when_qc_rejected(monkeypatch, fits_file):
    """
    pipeline.run()'s new quality_flag gate: when analyze_frame()'s result
    carries a non-"OK" quality_flag, anomaly detection and chart generation
    must not run at all (mirrors detect_anomalies_for_frame_id()'s own
    skip-on-non-OK guard for the standalone task path).
    """
    analyze_mock = AsyncMock(return_value={
        "frame_id": "frame-99",
        "filename": _NORMALIZED_FILENAME,
        "basename": "frame.fits",
        "sources": [],
        "object_name": "M51",
        "obs_time": "2024-03-15T22:01:34",
        "subtraction_performed": False,
        "quality_flag": "BLUR",
    })
    monkeypatch.setattr(pipeline, "analyze_frame", analyze_mock)
    detect_mock = AsyncMock()
    monkeypatch.setattr(pipeline, "detect_anomalies_for_frame_data", detect_mock)
    charts_mock = AsyncMock()
    monkeypatch.setattr(pipeline, "generate_charts_for_anomalies", charts_mock)

    await pipeline.run(str(fits_file))

    analyze_mock.assert_called_once()
    detect_mock.assert_not_called()
    charts_mock.assert_not_called()


@pytest.mark.asyncio
async def test_detect_anomalies_for_frame_id_skips_when_quality_flag_not_ok(monkeypatch):
    """
    detect_anomalies_for_frame_id()'s new guard: a frame whose quality_flag
    is neither None nor "OK" has no sources at all (see analyze_frame()) —
    skip straight to [] with no GET /frames/{id}/sources call and no
    POST /frames/{id}/anomalies call.
    """
    api_mock = MagicMock()
    api_mock.get_frame = AsyncMock(return_value={
        "filename": "M51_L_V_120_2024-03-15T22-01-34.fits",
        "obs_time": "2024-03-15T22:01:34",
        "filter": "V",
        "quality_flag": "BLUR",
    })
    api_mock.get_frame_sources = AsyncMock()
    api_mock.post_anomalies = AsyncMock()
    monkeypatch.setattr("pipeline.api_client", api_mock)

    result = await pipeline.detect_anomalies_for_frame_id("frame-1")

    assert result == []
    api_mock.get_frame_sources.assert_not_called()
    api_mock.post_anomalies.assert_not_called()


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
async def test_archived_file_gets_solved_wcs(mock_modules, tmp_path):
    """
    Regression test for the 2026-08-06 UGC_6930 incident: the archived FITS
    file must carry astap's own freshly-solved WCS (astro_result["wcs"]),
    not whatever WCS (if any) it originally arrived with — see
    pipeline.py's _write_solved_wcs(). Without this, modules/finder_chart.py
    (and any future code) reading WCS back out of the archived file would
    silently re-inherit stale/mount-pointing coordinates forever, since
    astap itself never writes into the FITS file (no -update).
    """
    fits_path = mock_modules  # real path from the fits_file fixture

    # The fits_file fixture writes a deliberately-minimal, invalid byte stub
    # (fine for the other, fully-mocked steps) — overwrite it with a real,
    # valid minimal FITS so this test can actually exercise the header
    # rewrite in mode="update".
    real_hdu = fits.PrimaryHDU(data=np.zeros((10, 10), dtype=np.float32))
    real_hdu.header["OBJECT"] = "M51"
    real_hdu.writeto(fits_path, overwrite=True)

    solved_wcs = AstropyWCS(naxis=2)
    solved_wcs.wcs.ctype = ["RA---TAN", "DEC--TAN"]
    solved_wcs.wcs.crval = [202.47, 47.20]
    solved_wcs.wcs.crpix = [5, 5]
    solved_wcs.wcs.cdelt = [-0.001, 0.001]
    solved_wcs.wcs.set()

    pipeline.astrometry.solve = AsyncMock(
        return_value={**copy.deepcopy(_GOOD_ASTRO), "wcs": solved_wcs}
    )

    await pipeline.run(str(fits_path))

    archive_path = os.path.join(config.FITS_ARCHIVE, "M51", _NORMALIZED_FILENAME)
    assert os.path.exists(archive_path), f"Expected archived file at {archive_path}"

    with fits.open(archive_path) as hdul:
        assert hdul[0].header["CRVAL1"] == pytest.approx(202.47, abs=1e-6)
        assert hdul[0].header["CRVAL2"] == pytest.approx(47.20, abs=1e-6)


# ---------------------------------------------------------------------------
# Recovering a lost science payload — audit 2026-08-18, finding H19
#
# The frame record survives an API outage: POST /frames has already succeeded
# and the file is archived. What is lost once the 3-attempt retry on POST
# /sources or POST /anomalies is exhausted is that run's entire source or
# anomaly list — no re-post, no "needs re-analysis" flag, nothing afterwards
# to show anything is missing.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failed_post_sources_queues_an_analyze_task(mock_modules):
    pipeline.api_client.post_sources = AsyncMock(return_value=None)
    pipeline.api_client.create_task = AsyncMock(return_value={"id": "task-9"})

    await pipeline.analyze_frame(str(mock_modules))

    pipeline.api_client.create_task.assert_awaited_once()
    task_type, = pipeline.api_client.create_task.call_args.args
    items = pipeline.api_client.create_task.call_args.kwargs["items"]
    assert task_type == "ANALYZE"
    # Queued against the ARCHIVE path — that is where the file will be by the
    # time the worker picks the task up.
    assert items[0]["filename"].endswith(_NORMALIZED_FILENAME)
    assert config.FITS_ARCHIVE in items[0]["filename"]
    assert items[0]["payload"]["recovery_attempt"] == 1


@pytest.mark.asyncio
async def test_recovery_falls_back_to_the_incoming_path_when_archiving_fails(mock_modules, monkeypatch):
    """
    A failed archive move leaves the file where it arrived; the lost source
    list must still be re-queued, against that path.
    """
    pipeline.api_client.post_sources = AsyncMock(return_value=None)
    pipeline.api_client.create_task = AsyncMock(return_value={"id": "task-9"})

    def failing_move(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(pipeline.shutil, "move", failing_move)

    await pipeline.analyze_frame(str(mock_modules))

    pipeline.api_client.create_task.assert_awaited_once()
    items = pipeline.api_client.create_task.call_args.kwargs["items"]
    assert items[0]["filename"] == str(mock_modules)


@pytest.mark.asyncio
async def test_a_successful_post_queues_nothing(mock_modules):
    pipeline.api_client.create_task = AsyncMock(return_value={"id": "task-9"})

    await pipeline.analyze_frame(str(mock_modules))

    pipeline.api_client.create_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_recovery_stops_at_the_attempt_limit(mock_modules, monkeypatch):
    """
    Not every failure is transient: the retry decorator deliberately does not
    retry a 4xx, and such a frame would otherwise re-queue itself forever.
    """
    monkeypatch.setattr(config, "API_RECOVERY_MAX_ATTEMPTS", 2)
    pipeline.api_client.post_sources = AsyncMock(return_value=None)
    pipeline.api_client.create_task = AsyncMock(return_value={"id": "task-9"})

    await pipeline.analyze_frame(str(mock_modules), 2)

    pipeline.api_client.create_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_recovery_can_be_disabled(mock_modules, monkeypatch):
    monkeypatch.setattr(config, "API_RECOVERY_MAX_ATTEMPTS", 0)
    pipeline.api_client.post_sources = AsyncMock(return_value=None)
    pipeline.api_client.create_task = AsyncMock(return_value={"id": "task-9"})

    await pipeline.analyze_frame(str(mock_modules))

    pipeline.api_client.create_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failed_recovery_task_is_not_fatal(mock_modules):
    """
    If the API is down hard enough to lose the payload it is probably down
    hard enough to refuse the task too. That must be logged, not raised.
    """
    pipeline.api_client.post_sources = AsyncMock(return_value=None)
    pipeline.api_client.create_task = AsyncMock(return_value=None)

    result = await pipeline.analyze_frame(str(mock_modules))

    assert result is not None
    assert result["frame_id"] == "frame-42"


@pytest.mark.asyncio
async def test_failed_post_anomalies_queues_a_detect_task(mock_modules):
    pipeline.api_client.post_anomalies = AsyncMock(return_value=False)
    pipeline.api_client.create_task = AsyncMock(return_value={"id": "task-11"})
    pipeline.anomaly_detector.detect = AsyncMock(return_value=[{"anomaly_type": "UNKNOWN"}])

    await pipeline.detect_anomalies_for_frame_data(
        "frame-42", [], {"obs_time": "2024-03-15T22:01:34"}, post_filename="f.fits",
    )

    pipeline.api_client.create_task.assert_awaited_once()
    task_type, = pipeline.api_client.create_task.call_args.args
    items = pipeline.api_client.create_task.call_args.kwargs["items"]
    assert task_type == "DETECT_ANOMALIES"
    assert items[0]["frame_id"] == "frame-42"
    assert items[0]["payload"]["recovery_attempt"] == 1


@pytest.mark.asyncio
async def test_archived_file_carries_only_one_wcs_representation(mock_modules, tmp_path):
    """
    Audit 2026-08-18, finding H17: WCS.to_header() emits PC+CDELT even when
    the WCS came from a CD matrix, and header.update() only sets the keys it
    is given — it does not remove the ones it isn't. An incoming file whose
    capture software wrote a CD matrix from mount pointing would keep those
    cards alongside astap's fresh PC+CDELT, describing two transforms at once.
    What a reader does with that is not guaranteed: _read_wcs() had to be
    taught to strip PC/CDELT when CD is present, after astropy multiplied the
    two and turned 0.78"/px into 0.0002"/px.
    """
    fits_path = mock_modules

    real_hdu = fits.PrimaryHDU(data=np.zeros((10, 10), dtype=np.float32))
    real_hdu.header["OBJECT"] = "M51"
    # A mount-pointing estimate in the older CD representation.
    real_hdu.header["CTYPE1"] = "RA---TAN"
    real_hdu.header["CTYPE2"] = "DEC--TAN"
    real_hdu.header["CRVAL1"] = 100.0
    real_hdu.header["CRVAL2"] = 10.0
    real_hdu.header["CRPIX1"] = 5.0
    real_hdu.header["CRPIX2"] = 5.0
    real_hdu.header["CD1_1"] = -0.001
    real_hdu.header["CD1_2"] = 0.0
    real_hdu.header["CD2_1"] = 0.0
    real_hdu.header["CD2_2"] = 0.001
    real_hdu.header["CROTA2"] = 12.0
    real_hdu.writeto(fits_path, overwrite=True)

    solved_wcs = AstropyWCS(naxis=2)
    solved_wcs.wcs.ctype = ["RA---TAN", "DEC--TAN"]
    solved_wcs.wcs.crval = [202.47, 47.20]
    solved_wcs.wcs.crpix = [5, 5]
    solved_wcs.wcs.cdelt = [-0.001, 0.001]
    solved_wcs.wcs.set()

    pipeline.astrometry.solve = AsyncMock(
        return_value={**copy.deepcopy(_GOOD_ASTRO), "wcs": solved_wcs}
    )

    await pipeline.run(str(fits_path))

    archive_path = os.path.join(config.FITS_ARCHIVE, "M51", _NORMALIZED_FILENAME)
    with fits.open(archive_path) as hdul:
        header = hdul[0].header
        has_cd = any(k.startswith("CD1_") or k.startswith("CD2_") for k in header)
        has_pc = any(k.startswith("PC") for k in header)
        assert not (has_cd and has_pc), "archived header carries two WCS representations"
        assert "CROTA2" not in header
        # And the solve that survived is astap's, not the mount's.
        assert header["CRVAL1"] == pytest.approx(202.47, abs=1e-6)


def test_stripping_the_wcs_leaves_non_wcs_cards_alone():
    """
    The cleanup matched by PREFIX, and "PC" is also the first two letters of
    PCOUNT — a structural keyword describing an HDU's parameter count, not a
    WCS matrix term at all. Deleting it while replacing a WCS corrupts the
    very header this helper exists to leave well-defined. Only indexed cards
    (PCi_j, CDi_j, CDELTi, CROTAi, with an optional alternate-WCS suffix)
    belong to a linear transform.
    """
    header = fits.Header()
    header["PC1_1"] = 1.0
    header["PC1_2"] = 0.0
    header["CD2_1"] = 0.0
    header["CDELT1"] = -0.001
    header["CROTA2"] = 12.0
    header["CD1_1A"] = 0.5          # alternate-WCS suffix — still a WCS card
    header["PCOUNT"] = 0            # structural, not WCS
    header["GCOUNT"] = 1
    header["CCDTEMP"] = -10.0

    pipeline._strip_wcs_representation(header)

    for gone in ("PC1_1", "PC1_2", "CD2_1", "CDELT1", "CROTA2", "CD1_1A"):
        assert gone not in header
    assert header["PCOUNT"] == 0
    assert header["GCOUNT"] == 1
    assert header["CCDTEMP"] == pytest.approx(-10.0)


@pytest.mark.asyncio
async def test_archived_file_gets_qc_headers(mock_modules, tmp_path):
    """
    Audit 2026-08-18, finding H10: a later frame's subtraction picks its
    reference stack out of this same archive directory and has no way to ask
    the API what it is about to stack — the pipeline has no database access
    and the API cannot see the observatory's filesystem. The frame's own
    header is where the two meet, so the QC verdict is stamped there before
    the archive move.
    """
    fits_path = mock_modules

    real_hdu = fits.PrimaryHDU(data=np.zeros((10, 10), dtype=np.float32))
    real_hdu.header["OBJECT"] = "M51"
    real_hdu.writeto(fits_path, overwrite=True)

    await pipeline.run(str(fits_path))

    archive_path = os.path.join(config.FITS_ARCHIVE, "M51", _NORMALIZED_FILENAME)
    with fits.open(archive_path) as hdul:
        assert hdul[0].header["QCFLAG"] == "OK"
        assert hdul[0].header["QCFWHM"] == pytest.approx(3.2)


@pytest.mark.asyncio
async def test_qc_fwhm_header_comes_from_the_solved_scale(mock_modules, tmp_path):
    """
    Audit 2026-08-18, finding M16: the FWHM stamped under QCFWHM is the PSF
    anchor re-converted with the plate scale astrometry actually solved for,
    not qc.analyze()'s own conversion through the header's claimed optical
    setup. Subtraction screens a reference's stamped QCFWHM against the new
    frame's anchor, so both sides of that ratio have to be measured the same
    way.
    """
    fits_path = mock_modules

    real_hdu = fits.PrimaryHDU(data=np.zeros((10, 10), dtype=np.float32))
    real_hdu.header["OBJECT"] = "M51"
    real_hdu.writeto(fits_path, overwrite=True)

    # Headers claiming 1.0"/px against a solve that found 1.6"/px — an
    # unaccounted reducer, a swapped camera, a wrong FOCALLEN.
    pipeline.qc.analyze.return_value = {
        **_GOOD_QC, "fwhm_median": 2.0, "fwhm_median_px": 2.0,
    }

    await pipeline.run(str(fits_path))

    archive_path = os.path.join(config.FITS_ARCHIVE, "M51", _NORMALIZED_FILENAME)
    with fits.open(archive_path) as hdul:
        assert hdul[0].header["QCFWHM"] == pytest.approx(3.2)


@pytest.mark.asyncio
async def test_qc_fwhm_header_written_even_when_headers_had_no_plate_scale(
    mock_modules, tmp_path,
):
    """
    A frame whose headers carry no plate scale at all (an all-sky lens with
    no XPIXSZ/FOCALLEN) used to be archived with no QCFWHM whatsoever, and
    ran subtraction and forced photometry with no PSF anchor. The solve
    supplies the missing scale, so it now has both (finding M16).
    """
    fits_path = mock_modules

    real_hdu = fits.PrimaryHDU(data=np.zeros((10, 10), dtype=np.float32))
    real_hdu.header["OBJECT"] = "M51"
    real_hdu.writeto(fits_path, overwrite=True)

    pipeline.qc.analyze.return_value = {
        **_GOOD_QC, "fwhm_median": 2.0, "fwhm_unit": "pixels", "fwhm_median_px": 2.0,
    }

    await pipeline.run(str(fits_path))

    archive_path = os.path.join(config.FITS_ARCHIVE, "M51", _NORMALIZED_FILENAME)
    with fits.open(archive_path) as hdul:
        assert hdul[0].header["QCFWHM"] == pytest.approx(3.2)


@pytest.mark.asyncio
async def test_qc_fwhm_header_omitted_when_nothing_can_supply_a_scale(
    mock_modules, tmp_path,
):
    """
    Neither the headers nor the solve know the plate scale — writing a raw
    pixel count under a keyword documented as arcsec would make a later
    subtraction compare two incompatible quantities, so it is still simply
    not written.
    """
    fits_path = mock_modules

    real_hdu = fits.PrimaryHDU(data=np.zeros((10, 10), dtype=np.float32))
    real_hdu.header["OBJECT"] = "M51"
    real_hdu.writeto(fits_path, overwrite=True)

    pipeline.qc.analyze.return_value = {**_GOOD_QC, "fwhm_unit": "pixels"}
    solved = {k: v for k, v in _GOOD_ASTRO.items() if k != "pixel_scale_arcsec"}
    pipeline.astrometry.solve = AsyncMock(return_value=solved)

    await pipeline.run(str(fits_path))

    archive_path = os.path.join(config.FITS_ARCHIVE, "M51", _NORMALIZED_FILENAME)
    with fits.open(archive_path) as hdul:
        assert hdul[0].header["QCFLAG"] == "OK"
        assert "QCFWHM" not in hdul[0].header


@pytest.mark.asyncio
async def test_solved_scale_reanchors_the_subtraction_psf(mock_modules, tmp_path):
    """
    The anchor handed to subtraction (and forced photometry) is the solved
    one too, not the header's — subtraction divides it straight back by the
    solved scale to get a pixel floor, so a header-derived arcsec value
    would be skewed by the ratio between the two scales (finding M16).
    """
    fits_path = str(mock_modules)

    pipeline.qc.analyze.return_value = {
        **_GOOD_QC, "fwhm_median": 2.0, "fwhm_median_px": 2.0,
    }

    await pipeline.run(fits_path)

    _, kwargs = pipeline.subtraction.run.call_args
    assert kwargs["psf_fwhm_arcsec"] == pytest.approx(3.2)


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

    async def fake_update_charts_for_sources(anomaly_types_by_source_id, designation_by_source_id=None):
        archive_existed_at_chart_time["exists"] = os.path.exists(expected_archive_path)
        return {sid: {t: True for t in types} for sid, types in anomaly_types_by_source_id.items()}

    finder_chart_mock = MagicMock()
    finder_chart_mock.update_charts_for_sources = AsyncMock(
        side_effect=fake_update_charts_for_sources
    )
    monkeypatch.setattr("pipeline.finder_chart", finder_chart_mock)
    monkeypatch.setattr(config, "CHART_ENABLED", True)

    await pipeline.run(fits_path)

    # designation_by_source_id is {} here: mock_modules' post_sources mock
    # returns None by default, so no source ever gets "_source_id" attached
    # (see pipeline.py Step 7) and the designation lookup has nothing to key on.
    finder_chart_mock.update_charts_for_sources.assert_called_once_with(
        {"src-1": ["UNKNOWN"]}, {}
    )
    assert archive_existed_at_chart_time.get("exists") is True, (
        "finder_chart.update_charts_for_sources() ran before the archive move — "
        "this frame's own epoch would never be found on disk"
    )
    # And the archive move must still have actually happened.
    assert os.path.exists(expected_archive_path)


@pytest.mark.asyncio
async def test_finder_chart_receives_catalog_designation(mock_modules, monkeypatch):
    """
    A catalog-matched source's designation (catalog_id) must reach
    finder_chart.update_charts_for_sources() as designation_by_source_id, so
    the rendered chart's title can show it next to anomaly_type (e.g.
    "ASTEROID (4 Vesta)") — see modules/finder_chart.py. An uncatalogued
    source's source_id must be absent from that dict, not present with a
    None/empty value.
    """
    fits_path = str(mock_modules)

    async def mock_match(sources, frame_meta):
        sources[0]["catalog_name"] = "MPC"
        sources[0]["catalog_id"] = "4 Vesta"
        sources[1]["catalog_name"] = None
        sources[1]["catalog_id"] = None
        return sources
    pipeline.catalog_matcher.match = AsyncMock(side_effect=mock_match)

    pipeline.api_client.post_sources.return_value = ["src-vesta", "src-uncat"]
    pipeline.anomaly_detector.detect = AsyncMock(return_value=[
        {"anomaly_type": "ASTEROID", "source_id": "src-vesta"},
        {"anomaly_type": "UNKNOWN", "source_id": "src-uncat"},
    ])

    captured = {}

    async def fake_update_charts_for_sources(anomaly_types_by_source_id, designation_by_source_id=None):
        captured["anomaly_types_by_source_id"] = anomaly_types_by_source_id
        captured["designation_by_source_id"] = designation_by_source_id
        return {sid: {t: True for t in types} for sid, types in anomaly_types_by_source_id.items()}

    finder_chart_mock = MagicMock()
    finder_chart_mock.update_charts_for_sources = AsyncMock(side_effect=fake_update_charts_for_sources)
    monkeypatch.setattr("pipeline.finder_chart", finder_chart_mock)
    monkeypatch.setattr(config, "CHART_ENABLED", True)

    await pipeline.run(fits_path)

    assert captured["anomaly_types_by_source_id"] == {"src-vesta": ["ASTEROID"], "src-uncat": ["UNKNOWN"]}
    assert captured["designation_by_source_id"] == {"src-vesta": "4 Vesta"}
    assert "src-uncat" not in captured["designation_by_source_id"]


@pytest.mark.asyncio
async def test_finder_chart_designation_prefers_mpc_designation_over_stale_source_catalog_id(
    mock_modules, monkeypatch,
):
    """
    Regression test, real incident 2026-08-06 (Vesta_A807_FA test data):
    the API can resolve "_source_id" positionally, so a moving object that
    passes near an already-catalogued star's position can get folded into
    that SAME `sources` row — whose catalog_name/catalog_id then reflects
    the star, not the asteroid this specific anomaly is actually about. The
    anomaly's own "mpc_designation" (set by anomaly_detector.py from the
    exact source that triggered THIS classification) must win over that
    stale/misleading `sources`.catalog_id every time.
    """
    fits_path = str(mock_modules)

    # sources[0] carries a Gaia DR3 identity totally unrelated to the
    # asteroid — modelling the real `sources` row having been last updated
    # by a different (star) detection sharing the same source_id.
    async def mock_match(sources, frame_meta):
        sources[0]["catalog_name"] = "Gaia DR3"
        sources[0]["catalog_id"] = "3971465931154563840"
        sources[1]["catalog_name"] = None
        sources[1]["catalog_id"] = None
        return sources
    pipeline.catalog_matcher.match = AsyncMock(side_effect=mock_match)

    pipeline.api_client.post_sources.return_value = ["src-shared", "src-uncat"]
    pipeline.anomaly_detector.detect = AsyncMock(return_value=[
        {"anomaly_type": "ASTEROID", "source_id": "src-shared", "mpc_designation": "2014 RY1"},
        {"anomaly_type": "UNKNOWN", "source_id": "src-uncat"},
    ])

    captured = {}

    async def fake_update_charts_for_sources(anomaly_types_by_source_id, designation_by_source_id=None):
        captured["designation_by_source_id"] = designation_by_source_id
        return {sid: {t: True for t in types} for sid, types in anomaly_types_by_source_id.items()}

    finder_chart_mock = MagicMock()
    finder_chart_mock.update_charts_for_sources = AsyncMock(side_effect=fake_update_charts_for_sources)
    monkeypatch.setattr("pipeline.finder_chart", finder_chart_mock)
    monkeypatch.setattr(config, "CHART_ENABLED", True)

    await pipeline.run(fits_path)

    assert captured["designation_by_source_id"] == {"src-shared": "2014 RY1"}


@pytest.mark.asyncio
async def test_generate_charts_for_anomalies_collects_multiple_types_per_source(monkeypatch):
    """
    Regression for the 2026-08-11 UI report: two anomalies resolving to the
    SAME source_id but carrying DIFFERENT anomaly_types (e.g. the API
    resolving "_source_id" positionally onto one row for two detections in
    the same frame) must both reach finder_chart.update_charts_for_sources()
    as a list, not have the second one silently dropped by a "first wins"
    dict assignment.
    """
    sources = [
        {"_source_id": "src-1", "catalog_name": None, "catalog_id": None},
    ]
    anomalies = [
        {"anomaly_type": "MOVING_UNKNOWN", "source_id": "src-1"},
        {"anomaly_type": "UNKNOWN", "source_id": "src-1"},
    ]

    captured = {}

    async def fake_update_charts_for_sources(anomaly_types_by_source_id, designation_by_source_id=None):
        captured["anomaly_types_by_source_id"] = anomaly_types_by_source_id
        return {sid: {t: True for t in types} for sid, types in anomaly_types_by_source_id.items()}

    finder_chart_mock = MagicMock()
    finder_chart_mock.update_charts_for_sources = AsyncMock(side_effect=fake_update_charts_for_sources)
    monkeypatch.setattr("pipeline.finder_chart", finder_chart_mock)
    monkeypatch.setattr(config, "CHART_ENABLED", True)

    result = await pipeline.generate_charts_for_anomalies(sources, anomalies)

    assert captured["anomaly_types_by_source_id"] == {"src-1": ["MOVING_UNKNOWN", "UNKNOWN"]}
    assert result == {"src-1": {"MOVING_UNKNOWN": True, "UNKNOWN": True}}


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
    # Anomaly classification never ran, so there is no result to post. An
    # empty set would REPLACE whatever the API already holds for this frame
    # (docs/API.md) — see test_anomaly_detection_failure_does_not_replace_anomalies.
    api_mock.post_anomalies.assert_not_called()

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

    # The frame is still archived — detection failing is not fatal.
    archive_path = os.path.join(config.FITS_ARCHIVE, "M51", _NORMALIZED_FILENAME)
    assert os.path.exists(archive_path)


@pytest.mark.asyncio
async def test_anomaly_detection_failure_does_not_replace_anomalies(mock_modules):
    """
    Audit 2026-08-18, finding C8: POST /frames/{id}/anomalies REPLACES the
    frame's whole anomaly set, so posting [] because detection FAILED erased
    every anomaly a previous successful run had stored — one transient
    Horizons outage turned into permanent data loss. Nothing is posted at all
    now unless classification actually ran to completion.
    """
    pipeline.anomaly_detector.detect.side_effect = RuntimeError("JPL timeout")

    await pipeline.run(str(mock_modules))

    pipeline.api_client.post_anomalies.assert_not_called()


@pytest.mark.asyncio
async def test_empty_anomaly_result_is_still_posted(mock_modules):
    """
    The other half of the same contract: an empty list IS meaningful when
    detection genuinely found nothing, and must still be posted so a frame
    whose anomalies were resolved away is cleared.
    """
    pipeline.anomaly_detector.detect.return_value = []

    await pipeline.run(str(mock_modules))

    pipeline.api_client.post_anomalies.assert_called_once()
    assert pipeline.api_client.post_anomalies.call_args[0][2] == []


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


# ---------------------------------------------------------------------------
# Step 5.6 — forced photometry / reverse matching (originally proposed as ROADMAP.md #1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_forced_photometry_results_merged_into_sources(mock_modules):
    """
    A source recovered by forced_photometry.run() must be merged into
    `sources` and reach both the "mag"/"_filter" tagging loop (Step 5.5) and
    the final POST /frames/{id}/sources call — same merge contract as
    subtraction candidates above.
    """
    forced_source = {
        "ra": 202.55, "dec": 47.30,
        "flux": 300.0, "fwhm": 3.1, "elongation": 1.0,
        "saturated": False, "near_edge": False,
        "catalog_name": "Gaia DR3", "catalog_id": "999888777",
        "catalog_mag": 18.9, "object_type": "STAR",
        "flux_aperture": 300.0, "flux_err": 40.0,
        "mag_instrumental": -6.2, "mag_calibrated": 18.7, "mag_err": 0.15,
        "calibrated": True, "edge_flag": False,
        "zero_point": 24.9, "zero_point_err": 0.05,
        "_from_subtraction": False, "_forced_photometry": True,
    }
    pipeline.forced_photometry.run.return_value = [forced_source]

    await pipeline.run(str(mock_modules))

    # The 2 astrometry sources PLUS the 1 forced-photometry recovery.
    posted_sources = pipeline.api_client.post_sources.call_args.args[2]
    assert len(posted_sources) == 3
    recovered = [s for s in posted_sources if s.get("_forced_photometry")]
    assert len(recovered) == 1
    # Step 5.5 must have tagged it with "mag"/"_filter" like every other source.
    assert recovered[0]["mag"] == pytest.approx(18.7)
    assert recovered[0]["_filter"] == "V"


@pytest.mark.asyncio
async def test_forced_photometry_reuses_cached_catalog_lists(mock_modules):
    """
    forced_photometry.run() must be called with the Gaia/MPC field lists from
    catalog_matcher.get_gaia_stars()/get_mpc_objects() — never from a fresh
    network query of its own (see modules/forced_photometry.py's docstring).
    """
    gaia_stars = [{"ra": 202.5, "dec": 47.3, "source_id": "1", "phot_g_mean_mag": 15.0}]
    mpc_objects = [{"ra": 202.6, "dec": 47.4, "designation": "2014 RY1", "object_type": "ASTEROID"}]
    pipeline.catalog_matcher.get_gaia_stars.return_value = gaia_stars
    pipeline.catalog_matcher.get_mpc_objects.return_value = mpc_objects

    await pipeline.run(str(mock_modules))

    _, kwargs = pipeline.forced_photometry.run.call_args
    assert kwargs["gaia_stars"] == gaia_stars
    assert kwargs["mpc_objects"] == mpc_objects
    assert kwargs["zero_point"] == pytest.approx(14.5)  # from mock_measure's mag_calibrated fixture


@pytest.mark.asyncio
async def test_forced_photometry_gets_midpoint_epoch_and_colour_term(mock_modules, monkeypatch):
    """
    Forced photometry propagates Gaia proper motion to the exposure MIDPOINT
    (finding C9), and transforms its magnitudes with the same colour-term
    solution photometry.measure() fitted (finding H5) — both read by
    pipeline.py and forwarded, so both are pinned here.
    """
    header = dict(_GOOD_HEADER)
    header["obs_time"] = "2024-03-15T22:00:00"
    header["obs_time_mid"] = "2024-03-15T22:01:00"
    monkeypatch.setattr("pipeline.fits_header.extract_headers", lambda p: header)

    original_measure = pipeline.photometry.measure.side_effect

    async def measure_with_colour(fits_path, sources, skip_calibration=False, **kwargs):
        sources = await original_measure(fits_path, sources, skip_calibration)
        for s in sources:
            s["_color_term"] = 0.12
            s["_color_ref"] = 0.85
            s["_color_scatter"] = 0.3
        return sources

    pipeline.photometry.measure.side_effect = measure_with_colour

    await pipeline.run(str(mock_modules))

    _, kwargs = pipeline.forced_photometry.run.call_args
    assert kwargs["obs_time"] == "2024-03-15T22:01:00"
    assert kwargs["color_term"] == pytest.approx(0.12)
    assert kwargs["color_ref"] == pytest.approx(0.85)
    assert kwargs["color_scatter"] == pytest.approx(0.3)
    # The SkyBot accessor must hit the same epoch match() queried with.
    assert "2024-03-15T22:01:00" in pipeline.catalog_matcher.get_mpc_objects.call_args.args


@pytest.mark.asyncio
async def test_photometry_gets_the_solved_wcs(mock_modules):
    """
    photometry.measure() runs before the archive step writes astap's solve
    into the file, so it must be handed the solved WCS explicitly.
    """
    solved_wcs = MagicMock(name="solved_wcs")
    pipeline.astrometry.solve = AsyncMock(
        return_value={**copy.deepcopy(_GOOD_ASTRO), "wcs": solved_wcs}
    )

    await pipeline.run(str(mock_modules))

    assert pipeline.photometry.measure.call_args.kwargs["wcs"] is solved_wcs
    # ... and the frame's filter, which selects its fixed colour term.
    assert pipeline.photometry.measure.call_args.kwargs["filter_name"] == \
        _GOOD_HEADER["observation"]["filter"]


@pytest.mark.asyncio
async def test_forced_photometry_failure_does_not_abort_pipeline(mock_modules):
    """A crash in forced_photometry.run() must not abort the pipeline."""
    pipeline.forced_photometry.run.side_effect = RuntimeError("aperture photometry blew up")

    await pipeline.run(str(mock_modules))

    pipeline.api_client.post_sources.assert_called_once()
    pipeline.api_client.post_anomalies.assert_called_once()


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

    async def mock_measure_uncalibrated(fits_path, sources, skip_calibration=False, **kwargs):
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

    async def mock_measure_calibrated(fits_path, sources, skip_calibration=False, **kwargs):
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


class TestPreferCandidateSeparation:
    """
    Audit 2026-08-18, finding M14: the dedup preference always kept the blind
    detection over the subtraction candidate. Only the MPC stage matches
    within the wide MOVING_CONE_ARCSEC (120"), so at that separation the pair
    is a moving object and an unrelated star that fell in the same cone — and
    keeping the "ordinary" one substituted the star's position for the
    mover's, in the very record the ephemeris and the track chart are built
    from.
    """

    def _pair(self, separation_deg: float) -> tuple[dict, dict]:
        blind = {
            "ra": 167.274, "dec": 17.359, "catalog_name": "MPC",
            "catalog_id": "Vesta", "flux": 500.0, "_from_subtraction": False,
        }
        sub = {
            "ra": 167.274 + separation_deg, "dec": 17.359, "catalog_name": "MPC",
            "catalog_id": "Vesta", "flux": 100.0, "_from_subtraction": True,
        }
        return blind, sub

    def test_a_close_pair_still_prefers_the_blind_detection(self):
        """
        For a stationary object both describe the same thing, and the blind
        one is measured on the frame's own pixels.
        """
        blind, sub = self._pair(separation_deg=1.0 / 3600.0)

        assert pipeline._prefer_candidate(sub, blind) is False
        assert pipeline._prefer_candidate(blind, sub) is True

    def test_a_distant_pair_prefers_the_subtraction_candidate(self):
        """
        A static star cancels in the difference image and never becomes a
        candidate there, so the subtraction one is the one that can be the
        mover.
        """
        blind, sub = self._pair(separation_deg=100.0 / 3600.0)

        assert pipeline._prefer_candidate(sub, blind) is True
        assert pipeline._prefer_candidate(blind, sub) is False

    def test_a_missing_position_falls_back_to_the_old_rule(self):
        blind, sub = self._pair(separation_deg=100.0 / 3600.0)
        sub.pop("ra")

        assert pipeline._prefer_candidate(sub, blind) is False

    def test_two_of_the_same_kind_are_still_decided_by_flux(self):
        a = {"ra": 1.0, "dec": 1.0, "flux": 100.0, "_from_subtraction": False}
        b = {"ra": 1.0, "dec": 1.0, "flux": 500.0, "_from_subtraction": False}

        assert pipeline._prefer_candidate(b, a) is True
        assert pipeline._prefer_candidate(a, b) is False

    def test_the_distant_pair_survives_the_full_dedup(self):
        blind, sub = self._pair(separation_deg=100.0 / 3600.0)

        result = pipeline._dedupe_by_catalog_identity([blind, sub], {})

        assert len(result) == 1
        assert result[0]["_from_subtraction"] is True


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


# ---------------------------------------------------------------------------
# _dedupe_uncatalogued_subtraction_pair — unit tests
#
# Regression coverage for the C_2020_R4_ATLAS incident (2026-08-11): an
# uncatalogued object (MPC/SkyBot carries no ephemeris data for this comet at
# all — see docs/ISSUES.md) has no catalog identity for
# _dedupe_by_catalog_identity() to key on, so its normal sep detection and its
# own image-subtraction candidate both survived as separate `sources` entries
# — inflating observation_count and producing two MOVING_UNKNOWN anomalies
# per frame for one physical detection.
# ---------------------------------------------------------------------------


class TestDedupeUncataloguedSubtractionPair:

    def test_no_sources_or_single_source_returns_unchanged(self):
        assert pipeline._dedupe_uncatalogued_subtraction_pair([], {}) == []

        one = [{"ra": 1.0, "dec": 1.0, "catalog_name": None, "flux": 1.0}]
        assert pipeline._dedupe_uncatalogued_subtraction_pair(one, {}) == one

    def test_normal_and_subtraction_detection_of_same_comet_collapse_to_one(self):
        """Regression: the C_2020_R4_ATLAS case — one comet detected twice
        within one frame (once by the ordinary extractor, once by
        subtraction.py), ~1" apart, neither carrying a catalog identity."""
        normal_detection = {
            "ra": 222.65332, "dec": 32.62380, "catalog_name": None, "catalog_id": None,
            "flux": 2846890.0, "fwhm": 10.13, "_from_subtraction": False,
        }
        subtraction_candidate = {
            "ra": 222.65327, "dec": 32.62347, "catalog_name": None, "catalog_id": None,
            "flux": 4002950.0, "fwhm": 17.90, "_from_subtraction": True,
        }

        result = pipeline._dedupe_uncatalogued_subtraction_pair(
            [normal_detection, subtraction_candidate], {}
        )

        assert len(result) == 1
        # Non-subtraction detection wins even though the subtraction
        # candidate has higher flux — same _prefer_candidate() rule as the
        # catalogued case above.
        assert result[0] is normal_detection

    def test_prefers_non_subtraction_regardless_of_order(self):
        subtraction_candidate = {
            "ra": 1.0, "dec": 1.0, "catalog_name": None, "flux": 9000.0,
            "_from_subtraction": True,
        }
        normal_detection = {
            "ra": 1.0, "dec": 1.0, "catalog_name": None, "flux": 500.0,
            "_from_subtraction": False,
        }

        result = pipeline._dedupe_uncatalogued_subtraction_pair(
            [subtraction_candidate, normal_detection], {}
        )

        assert len(result) == 1
        assert result[0] is normal_detection

    def test_far_apart_normal_and_subtraction_detections_are_not_merged(self):
        """Separation well beyond MATCH_CONE_ARCSEC (5" default) — these are
        two different objects, not a duplicate of the same one."""
        normal_detection = {
            "ra": 1.0, "dec": 1.0, "catalog_name": None, "flux": 100.0,
            "_from_subtraction": False,
        }
        far_subtraction_candidate = {
            "ra": 1.0, "dec": 1.5, "catalog_name": None, "flux": 100.0,  # 1800" away
            "_from_subtraction": True,
        }

        result = pipeline._dedupe_uncatalogued_subtraction_pair(
            [normal_detection, far_subtraction_candidate], {}
        )

        assert len(result) == 2

    def test_two_ordinary_detections_close_together_are_not_merged(self):
        """Two non-subtraction uncatalogued detections sitting close
        together might genuinely be two faint objects in a crowded field —
        this dedup only pairs a subtraction candidate with a non-subtraction
        detection, never two of the same kind."""
        a = {"ra": 1.0, "dec": 1.0, "catalog_name": None, "flux": 100.0, "_from_subtraction": False}
        b = {"ra": 1.0, "dec": 1.0, "catalog_name": None, "flux": 200.0, "_from_subtraction": False}

        result = pipeline._dedupe_uncatalogued_subtraction_pair([a, b], {})

        assert len(result) == 2

    def test_two_subtraction_candidates_close_together_are_not_merged(self):
        a = {"ra": 1.0, "dec": 1.0, "catalog_name": None, "flux": 100.0, "_from_subtraction": True}
        b = {"ra": 1.0, "dec": 1.0, "catalog_name": None, "flux": 200.0, "_from_subtraction": True}

        result = pipeline._dedupe_uncatalogued_subtraction_pair([a, b], {})

        assert len(result) == 2

    def test_catalogued_sources_are_left_untouched(self):
        """This dedup only ever considers catalog_name is None entries — a
        catalogued pair is _dedupe_by_catalog_identity()'s job, not this
        one's, even if it also carries a subtraction/non-subtraction split."""
        normal_detection = {
            "ra": 222.65332, "dec": 32.62380, "catalog_name": None, "catalog_id": None,
            "flux": 2846890.0, "_from_subtraction": False,
        }
        subtraction_candidate = {
            "ra": 222.65327, "dec": 32.62347, "catalog_name": None, "catalog_id": None,
            "flux": 4002950.0, "_from_subtraction": True,
        }
        catalogued_normal = {
            "ra": 10.0, "dec": 10.0, "catalog_name": "MPC", "catalog_id": "Vesta",
            "flux": 500.0, "_from_subtraction": False,
        }
        catalogued_subtraction = {
            "ra": 10.0, "dec": 10.0, "catalog_name": "MPC", "catalog_id": "Vesta",
            "flux": 9000.0, "_from_subtraction": True,
        }

        result = pipeline._dedupe_uncatalogued_subtraction_pair(
            [normal_detection, subtraction_candidate, catalogued_normal, catalogued_subtraction], {}
        )

        # The uncatalogued pair collapses to 1; the catalogued pair (this
        # function's job is explicitly NOT to touch it) passes through as 2
        # separate entries — that's _dedupe_by_catalog_identity()'s job,
        # which normally runs before this step in analyze_frame().
        assert len(result) == 3
        assert catalogued_normal in result
        assert catalogued_subtraction in result


# ---------------------------------------------------------------------------
# _dedupe_cross_catalog_duplicates — unit tests
#
# Regression coverage for the 2026-08-12 IC3322A incident: forced_photometry.py
# checks "already recovered" only against its OWN catalog's catalog_id, so a
# star catalog_matcher.match() already matched to an earlier catalog in its
# sequential-exclusive order (e.g. Simbad) looks unclaimed to it and gets
# appended a second time under its Gaia DR3 identity — 16 such pairs found in
# a single frame.
# ---------------------------------------------------------------------------


class TestDedupeUnmatchedNearMatched:
    """
    Step 4.6 suppresses an uncatalogued source sitting on top of a
    catalogue-matched one — sep splitting a single distorted PSF into two
    components, one of which lands just outside the catalog cone.

    Audit finding C6: the step had no exemption for a source already
    confirmed as a real pixel-level change by image subtraction, so a
    transient flaring in projection near a catalogued star was dropped here,
    before anomaly_detector.py and before POST /frames/{id}/sources — the
    event was never stored in any form.
    """

    def test_no_matched_sources_returns_unchanged(self):
        only_unmatched = [{"ra": 1.0, "dec": 1.0, "catalog_name": None}]
        assert pipeline._dedupe_unmatched_near_matched(only_unmatched, {}) is only_unmatched

    def test_deblending_artifact_near_a_matched_star_is_suppressed(self):
        """The case the step exists for — unchanged by the C6 exemption."""
        star = {"ra": 10.0, "dec": 20.0, "catalog_name": "Gaia DR3", "catalog_id": "A"}
        artifact = {"ra": 10.0003, "dec": 20.0, "catalog_name": None}  # ~1" away

        result = pipeline._dedupe_unmatched_near_matched([star, artifact], {})

        assert result == [star]

    def test_subtraction_candidate_near_a_matched_star_is_kept(self):
        """
        A supernova/nova flaring within MATCH_CONE_ARCSEC of a catalogued
        star — routine in a dense field, and the expected geometry near a
        known host galaxy. subtraction.run() has already confirmed the
        pixel-level change against the object's own archived history, which
        is evidence of a different kind from "nothing within 5 arcsec
        claims it".
        """
        star = {"ra": 10.0, "dec": 20.0, "catalog_name": "Gaia DR3", "catalog_id": "A"}
        transient = {
            "ra": 10.0003, "dec": 20.0, "catalog_name": None,
            "_from_subtraction": True,
        }

        result = pipeline._dedupe_unmatched_near_matched([star, transient], {})

        assert result == [star, transient]

    def test_distant_unmatched_source_is_kept_either_way(self):
        star = {"ra": 10.0, "dec": 20.0, "catalog_name": "Gaia DR3", "catalog_id": "A"}
        far = {"ra": 10.5, "dec": 20.0, "catalog_name": None}

        result = pipeline._dedupe_unmatched_near_matched([star, far], {})

        assert result == [star, far]

    def test_matched_sources_are_never_removed(self):
        """Two catalogued sources at the same position are this step's
        business only as reference points — neither is ever dropped here."""
        a = {"ra": 10.0, "dec": 20.0, "catalog_name": "Simbad", "catalog_id": "A"}
        b = {"ra": 10.0, "dec": 20.0, "catalog_name": "Gaia DR3", "catalog_id": "B"}

        result = pipeline._dedupe_unmatched_near_matched([a, b], {})

        assert result == [a, b]


class TestDedupeCrossCatalogDuplicates:

    def test_fewer_than_two_matched_sources_returns_unchanged(self):
        assert pipeline._dedupe_cross_catalog_duplicates([], {}) == []

        one = [{"ra": 1.0, "dec": 1.0, "catalog_name": "Gaia DR3", "catalog_id": "A"}]
        assert pipeline._dedupe_cross_catalog_duplicates(one, {}) == one

    def test_simbad_and_gaia_duplicate_of_same_star_collapses_to_simbad(self):
        """Regression: forced photometry re-adds a Simbad-matched star under
        its Gaia DR3 identity — Simbad wins (queried first)."""
        simbad_detection = {
            "ra": 186.4313, "dec": 7.2146, "catalog_name": "Simbad", "catalog_id": "HD 108903",
            "mag": None, "_from_subtraction": False,
        }
        forced_gaia_duplicate = {
            "ra": 186.4314, "dec": 7.2147, "catalog_name": "Gaia DR3", "catalog_id": "6028...1234",
            "mag": 6.28, "_from_subtraction": False,
        }
        other_star = {
            "ra": 10.0, "dec": 10.0, "catalog_name": "Gaia DR3", "catalog_id": "unrelated",
            "mag": 12.0,
        }

        result = pipeline._dedupe_cross_catalog_duplicates(
            [simbad_detection, forced_gaia_duplicate, other_star], {}
        )

        assert len(result) == 2
        assert simbad_detection in result
        assert forced_gaia_duplicate not in result
        assert other_star in result

    def test_kept_entry_wins_regardless_of_order(self):
        """Simbad wins whether it's seen before or after the Gaia duplicate."""
        gaia = {"ra": 1.0, "dec": 1.0, "catalog_name": "Gaia DR3", "catalog_id": "X", "mag": 6.0}
        simbad = {"ra": 1.0, "dec": 1.0, "catalog_name": "Simbad", "catalog_id": "Y", "mag": None}

        result = pipeline._dedupe_cross_catalog_duplicates([gaia, simbad], {})

        assert len(result) == 1
        assert result[0] is simbad

    def test_same_catalog_different_ids_are_not_merged(self):
        """Two distinct Gaia DR3 stars sitting close together in a crowded
        field are two real objects, not a duplicate — same-catalog pairs are
        left alone regardless of separation."""
        a = {"ra": 1.0, "dec": 1.0, "catalog_name": "Gaia DR3", "catalog_id": "A", "mag": 10.0}
        b = {"ra": 1.0, "dec": 1.0, "catalog_name": "Gaia DR3", "catalog_id": "B", "mag": 11.0}

        result = pipeline._dedupe_cross_catalog_duplicates([a, b], {})

        assert len(result) == 2

    def test_far_apart_different_catalogs_are_not_merged(self):
        """Separation well beyond MATCH_CONE_ARCSEC — two different objects
        that simply happen to be matched to different catalogs."""
        simbad = {"ra": 1.0, "dec": 1.0, "catalog_name": "Simbad", "catalog_id": "A", "mag": None}
        far_gaia = {"ra": 1.0, "dec": 1.5, "catalog_name": "Gaia DR3", "catalog_id": "B", "mag": 10.0}

        result = pipeline._dedupe_cross_catalog_duplicates([simbad, far_gaia], {})

        assert len(result) == 2

    def test_uncatalogued_sources_are_never_considered(self):
        """This dedup only ever looks at catalog_name is not None entries —
        an unmatched source near a matched one is _dedupe_unmatched_near_matched()'s
        job, not this one's."""
        matched = {"ra": 1.0, "dec": 1.0, "catalog_name": "Gaia DR3", "catalog_id": "A", "mag": 10.0}
        unmatched = {"ra": 1.0, "dec": 1.0, "catalog_name": None, "catalog_id": None, "mag": None}

        result = pipeline._dedupe_cross_catalog_duplicates([matched, unmatched], {})

        assert len(result) == 2

    def test_mpc_asteroid_and_gaia_star_duplicate_collapses_to_mpc(self):
        """An MPC-matched asteroid re-added under a background Gaia star's
        identity (or vice versa) at the same instant is the same shape of
        bug — MPC is queried last, so it only wins here because the other
        side of the pair is checked against the priority table, not because
        MPC is generally preferred."""
        mpc = {"ra": 5.0, "dec": 5.0, "catalog_name": "MPC", "catalog_id": "Vesta", "mag": 7.1}
        gaia = {"ra": 5.0, "dec": 5.0, "catalog_name": "Gaia DR3", "catalog_id": "other", "mag": 15.0}

        result = pipeline._dedupe_cross_catalog_duplicates([gaia, mpc], {})

        # Gaia DR3 outranks MPC in the sequential-match order, so the Gaia
        # entry is the one that survives here.
        assert len(result) == 1
        assert result[0] is gaia


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


@pytest.mark.asyncio
async def test_pipeline_dedupes_forced_photometry_cross_catalog_duplicate(mock_modules):
    """
    Full-pipeline regression test for the 2026-08-12 IC3322A incident:
    catalog_matcher (mocked here) matches the first source to Simbad;
    forced_photometry (mocked here) then "recovers" that same star again
    under its Gaia DR3 identity, at essentially the same position, because
    its own eligibility check only compares against Gaia catalog_ids. Only
    the Simbad entry must reach POST /frames/{id}/sources.
    """
    async def mock_match_first_to_simbad(sources, frame_meta):
        for i, s in enumerate(sources):
            if i == 0:
                s["catalog_name"] = "Simbad"
                s["catalog_id"] = "HD 108903"
                s["object_type"] = "STAR"
            else:
                s.setdefault("catalog_name", "Gaia DR3")
                s.setdefault("catalog_id", f"12345-{i}")
                s.setdefault("catalog_mag", 14.5)
                s.setdefault("object_type", "STAR")
        return sources

    pipeline.catalog_matcher.match.side_effect = mock_match_first_to_simbad

    # _GOOD_ASTRO's first source sits at (202.47, 47.20) — forced photometry
    # "recovers" a Gaia DR3 star less than 1" away, mimicking a duplicate
    # measurement of the very same physical star.
    forced_gaia_duplicate = {
        "ra": 202.47003, "dec": 47.20003,
        "flux": 12000.0, "mag_calibrated": 6.28, "calibrated": True,
        "catalog_name": "Gaia DR3", "catalog_id": "gaia-forced-dup",
        "catalog_mag": 6.28, "object_type": "STAR",
    }
    pipeline.forced_photometry.run.return_value = [forced_gaia_duplicate]

    await pipeline.run(str(mock_modules))

    posted_sources = pipeline.api_client.post_sources.call_args.args[2]
    assert not any(s.get("catalog_id") == "gaia-forced-dup" for s in posted_sources), (
        "forced photometry's duplicate of the already Simbad-matched star must be dropped"
    )
    simbad_entries = [s for s in posted_sources if s.get("catalog_id") == "HD 108903"]
    assert len(simbad_entries) == 1


# ---------------------------------------------------------------------------
# Watcher tests
#
# watcher.py no longer dispatches to pipeline.run() directly — it buffers
# arriving paths and submits them as batched ANALYZE tasks via
# api_client.create_task() (see that module's docstring). These tests mock
# `watcher.enqueue_path` / `watcher.api_client` / `watcher.threading.Timer`
# rather than `pipeline.run`/`asyncio.run` the old suite mocked.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_watcher_state():
    """
    watcher.py's pending-batch state (`_pending_paths`, `_pending_realpaths`,
    `_flush_timer`) is module-level mutable state shared across tests —
    without resetting it, a timer or buffered path left over from one test
    could leak into the next. Runs for every test in this file (cheap and
    harmless for non-watcher tests), not just the watcher ones below.
    """
    import watcher

    def _clear():
        watcher._pending_paths.clear()
        watcher._pending_realpaths.clear()
        if watcher._flush_timer is not None:
            watcher._flush_timer.cancel()
            watcher._flush_timer = None

    _clear()
    yield
    _clear()


def _make_event(src_path: str, is_directory: bool = False) -> MagicMock:
    """Build a minimal watchdog FileCreatedEvent-like mock."""
    event = MagicMock()
    event.src_path = src_path
    event.is_directory = is_directory
    return event


class TestFitsEventHandler:
    def test_ignores_non_fits(self, monkeypatch):
        """on_created with a .jpg file must not enqueue anything."""
        import watcher

        enqueue_mock = MagicMock()
        monkeypatch.setattr(watcher, "enqueue_path", enqueue_mock)
        handler = watcher.FitsEventHandler()

        handler.on_created(_make_event("/fits/incoming/photo.jpg"))

        enqueue_mock.assert_not_called()

    def test_enqueues_fits_file(self, monkeypatch):
        """on_created with a .fits file must enqueue its path."""
        import watcher

        enqueue_mock = MagicMock()
        monkeypatch.setattr(watcher, "enqueue_path", enqueue_mock)
        monkeypatch.setattr(watcher.time, "sleep", MagicMock())
        handler = watcher.FitsEventHandler()

        handler.on_created(_make_event("/fits/incoming/frame.fits"))

        enqueue_mock.assert_called_once_with("/fits/incoming/frame.fits")

    def test_enqueues_fit_uppercase(self, monkeypatch):
        """on_created with a .FIT extension (uppercase) must also be enqueued."""
        import watcher

        enqueue_mock = MagicMock()
        monkeypatch.setattr(watcher, "enqueue_path", enqueue_mock)
        monkeypatch.setattr(watcher.time, "sleep", MagicMock())
        handler = watcher.FitsEventHandler()

        handler.on_created(_make_event("/fits/incoming/FRAME.FIT"))

        enqueue_mock.assert_called_once_with("/fits/incoming/FRAME.FIT")

    def test_ignores_directory_event(self, monkeypatch):
        """Directory creation events must be silently ignored."""
        import watcher

        enqueue_mock = MagicMock()
        monkeypatch.setattr(watcher, "enqueue_path", enqueue_mock)
        handler = watcher.FitsEventHandler()

        handler.on_created(_make_event("/fits/incoming/subdir/", is_directory=True))

        enqueue_mock.assert_not_called()


# ---------------------------------------------------------------------------
# enqueue_path — buffering, debounce (re)arming, duplicate-event guard
# ---------------------------------------------------------------------------


class TestEnqueuePath:
    def test_skips_nonexistent_path(self, monkeypatch):
        """A path that doesn't exist (e.g. a duplicate event for an already
        flushed-and-processed file) must not be buffered or arm a timer."""
        import watcher

        timer_mock = MagicMock()
        monkeypatch.setattr(watcher.threading, "Timer", timer_mock)

        watcher.enqueue_path("/fits/incoming/does-not-exist.fits")

        assert watcher._pending_paths == []
        timer_mock.assert_not_called()

    def test_adds_to_buffer_and_arms_debounce_timer(self, monkeypatch, tmp_path):
        import watcher

        monkeypatch.setattr(config, "WATCHER_DEBOUNCE_SEC", 5.0)
        monkeypatch.setattr(config, "WATCHER_MAX_BATCH_SIZE", 200)
        timer_instance = MagicMock()
        timer_mock = MagicMock(return_value=timer_instance)
        monkeypatch.setattr(watcher.threading, "Timer", timer_mock)

        fits_path = str(tmp_path / "frame.fits")
        (tmp_path / "frame.fits").write_bytes(b"SIMPLE  =                    T")

        watcher.enqueue_path(fits_path)

        assert watcher._pending_paths == [fits_path]
        timer_mock.assert_called_once_with(5.0, watcher.flush_pending_batch)
        timer_instance.start.assert_called_once()

    def test_second_arrival_cancels_first_timer_and_rearms(self, monkeypatch, tmp_path):
        """Debounce: a new arrival must cancel the previous timer and start
        a fresh one, rather than letting both run."""
        import watcher

        monkeypatch.setattr(config, "WATCHER_DEBOUNCE_SEC", 5.0)
        monkeypatch.setattr(config, "WATCHER_MAX_BATCH_SIZE", 200)
        first_timer, second_timer = MagicMock(), MagicMock()
        timer_mock = MagicMock(side_effect=[first_timer, second_timer])
        monkeypatch.setattr(watcher.threading, "Timer", timer_mock)

        path_a = str(tmp_path / "a.fits")
        path_b = str(tmp_path / "b.fits")
        (tmp_path / "a.fits").write_bytes(b"x")
        (tmp_path / "b.fits").write_bytes(b"x")

        watcher.enqueue_path(path_a)
        watcher.enqueue_path(path_b)

        first_timer.cancel.assert_called_once()
        second_timer.start.assert_called_once()
        assert watcher._pending_paths == [path_a, path_b]

    def test_skips_duplicate_already_in_pending_batch(self, monkeypatch, tmp_path):
        """A duplicate on_created event for a path already buffered (not
        yet flushed) must not be added twice — see watcher.py's real
        incident this guards against (the same file registered as two
        separate frames a few seconds apart)."""
        import watcher

        monkeypatch.setattr(watcher.threading, "Timer", MagicMock())

        fits_path = str(tmp_path / "frame.fits")
        (tmp_path / "frame.fits").write_bytes(b"x")

        watcher.enqueue_path(fits_path)
        watcher.enqueue_path(fits_path)

        assert watcher._pending_paths == [fits_path]

    def test_flushes_immediately_at_max_batch_size(self, monkeypatch, tmp_path):
        """Reaching WATCHER_MAX_BATCH_SIZE must flush right away (a
        zero-delay timer) instead of waiting out the full debounce window."""
        import watcher

        monkeypatch.setattr(config, "WATCHER_DEBOUNCE_SEC", 5.0)
        monkeypatch.setattr(config, "WATCHER_MAX_BATCH_SIZE", 1)
        timer_instance = MagicMock()
        timer_mock = MagicMock(return_value=timer_instance)
        monkeypatch.setattr(watcher.threading, "Timer", timer_mock)

        fits_path = str(tmp_path / "frame.fits")
        (tmp_path / "frame.fits").write_bytes(b"x")

        watcher.enqueue_path(fits_path)

        timer_mock.assert_called_once_with(0.0, watcher.flush_pending_batch)
        timer_instance.start.assert_called_once()


# ---------------------------------------------------------------------------
# flush_pending_batch — batched ANALYZE task submission
# ---------------------------------------------------------------------------


class TestFlushPendingBatch:
    def test_empty_buffer_is_a_noop(self, monkeypatch):
        import watcher

        create_task_mock = AsyncMock()
        monkeypatch.setattr(watcher.api_client, "create_task", create_task_mock)

        watcher.flush_pending_batch()

        create_task_mock.assert_not_called()

    def test_submits_one_task_with_every_buffered_path(self, monkeypatch):
        import watcher

        watcher._pending_paths.extend(["/fits/incoming/a.fits", "/fits/incoming/b.fits"])
        watcher._pending_realpaths.update({"/fits/incoming/a.fits", "/fits/incoming/b.fits"})

        create_task_mock = AsyncMock(return_value={"id": "task-1"})
        monkeypatch.setattr(watcher.api_client, "create_task", create_task_mock)

        watcher.flush_pending_batch()

        create_task_mock.assert_called_once_with(
            "ANALYZE",
            [{"filename": "/fits/incoming/a.fits"}, {"filename": "/fits/incoming/b.fits"}],
        )
        # Buffer must be cleared so the next arrival starts a fresh batch.
        assert watcher._pending_paths == []
        assert watcher._pending_realpaths == set()

    def test_create_task_returning_none_is_logged_not_raised(self, monkeypatch):
        import watcher

        watcher._pending_paths.append("/fits/incoming/a.fits")
        watcher._pending_realpaths.add("/fits/incoming/a.fits")
        monkeypatch.setattr(watcher.api_client, "create_task", AsyncMock(return_value=None))

        watcher.flush_pending_batch()  # must not raise

        assert watcher._pending_paths == []

    def test_create_task_exception_is_caught(self, monkeypatch):
        import watcher

        watcher._pending_paths.append("/fits/incoming/a.fits")
        watcher._pending_realpaths.add("/fits/incoming/a.fits")
        monkeypatch.setattr(
            watcher.api_client, "create_task",
            AsyncMock(side_effect=RuntimeError("API unreachable")),
        )

        watcher.flush_pending_batch()  # must not raise

        # The buffer was already captured-and-cleared before the failing
        # call, so the (now-lost) files simply aren't resubmitted — they're
        # still on disk, unlike the old per-file dispatch which would have
        # already moved a successfully-processed file away.
        assert watcher._pending_paths == []


# ---------------------------------------------------------------------------
# process_existing_files — startup scan
# ---------------------------------------------------------------------------


class TestProcessExistingFiles:
    def test_enqueues_every_fits_file_found(self, monkeypatch, tmp_path):
        import watcher

        (tmp_path / "a.fits").write_bytes(b"x")
        (tmp_path / "b.fit").write_bytes(b"x")
        (tmp_path / "notes.txt").write_bytes(b"x")
        (tmp_path / "subdir").mkdir()

        enqueue_mock = MagicMock()
        monkeypatch.setattr(watcher, "enqueue_path", enqueue_mock)

        count = watcher.process_existing_files(str(tmp_path))

        assert count == 2
        enqueued = {c.args[0] for c in enqueue_mock.call_args_list}
        assert enqueued == {str(tmp_path / "a.fits"), str(tmp_path / "b.fit")}

    def test_enqueues_fits_in_subdirectories_recursively(self, monkeypatch, tmp_path):
        """FITS files nested inside subdirectories of incoming are discovered."""
        import watcher

        subdir = tmp_path / "m31"
        subdir.mkdir()
        (subdir / "frame1.fits").write_bytes(b"x")

        deep = subdir / "session1"
        deep.mkdir()
        (deep / "frame2.fit").write_bytes(b"x")

        (tmp_path / "top.fits").write_bytes(b"x")

        enqueue_mock = MagicMock()
        monkeypatch.setattr(watcher, "enqueue_path", enqueue_mock)

        count = watcher.process_existing_files(str(tmp_path))

        assert count == 3
        enqueued = {c.args[0] for c in enqueue_mock.call_args_list}
        assert str(subdir / "frame1.fits") in enqueued
        assert str(deep / "frame2.fit") in enqueued
        assert str(tmp_path / "top.fits") in enqueued


class TestCleanupEmptyIncomingParents:
    """Verify that empty subdirectories inside FITS_INCOMING are removed after
    a file is moved out."""

    def test_removes_empty_parent_dirs_up_to_incoming(self, monkeypatch, tmp_path):
        incoming = tmp_path / "incoming"
        subdir = incoming / "m31"
        subdir.mkdir(parents=True)

        fits_file = subdir / "frame.fits"
        fits_file.write_bytes(b"x")

        monkeypatch.setattr(config, "FITS_INCOMING", str(incoming))

        # Simulate the file being moved away (pipeline.py does this via shutil.move)
        fits_file.unlink()

        pipeline._cleanup_empty_incoming_parents(str(fits_file))

        assert not subdir.exists(), "Empty m31/ subdirectory should have been removed"
        assert incoming.exists(), "FITS_INCOMING itself must never be removed"

    def test_stops_at_non_empty_parent(self, monkeypatch, tmp_path):
        incoming = tmp_path / "incoming"
        subdir = incoming / "m31"
        subdir.mkdir(parents=True)

        fits_file = subdir / "frame1.fits"
        fits_file.write_bytes(b"x")
        (subdir / "frame2.fits").write_bytes(b"x")  # another file still here

        monkeypatch.setattr(config, "FITS_INCOMING", str(incoming))
        fits_file.unlink()

        pipeline._cleanup_empty_incoming_parents(str(fits_file))

        assert subdir.exists(), "m31/ should NOT be removed because frame2.fits is still there"

    def test_removes_deeply_nested_empty_dirs(self, monkeypatch, tmp_path):
        incoming = tmp_path / "incoming"
        deep = incoming / "m31" / "session1"
        deep.mkdir(parents=True)

        fits_file = deep / "frame.fits"
        fits_file.write_bytes(b"x")

        monkeypatch.setattr(config, "FITS_INCOMING", str(incoming))
        fits_file.unlink()

        pipeline._cleanup_empty_incoming_parents(str(fits_file))

        assert not deep.exists(), "session1/ should be removed"
        assert not (incoming / "m31").exists(), "m31/ should be removed too (now empty)"
        assert incoming.exists(), "FITS_INCOMING itself must never be removed"


# ---------------------------------------------------------------------------
# preview_catalog_match — Stage 4 (diagnostic tool, uploads the chart itself)
# ---------------------------------------------------------------------------


class TestPreviewCatalogMatch:
    async def test_renders_then_uploads_chart_and_returns_summary(self, monkeypatch):
        render_mock = AsyncMock(return_value={
            "png_bytes": b"\x89PNG\r\n\x1a\nfakepngdata",
            "matched": 82, "total": 98, "quality_flag": "OK",
        })
        monkeypatch.setattr(pipeline.catalog_preview, "render", render_mock)
        upload_mock = AsyncMock(return_value=True)
        monkeypatch.setattr(pipeline.api_client, "upload_task_item_chart", upload_mock)

        result = await pipeline.preview_catalog_match("/fits/archive/M51/frame.fits", "task-1", "item-1")

        render_mock.assert_called_once_with("/fits/archive/M51/frame.fits")
        upload_mock.assert_called_once_with(
            "task-1", "item-1", b"\x89PNG\r\n\x1a\nfakepngdata",
            style="catalog_preview", frame_count=1,
        )
        assert result == {"matched": 82, "total": 98, "quality_flag": "OK", "chart_uploaded": True}

    async def test_upload_failure_does_not_raise_and_reports_not_uploaded(self, monkeypatch):
        monkeypatch.setattr(pipeline.catalog_preview, "render", AsyncMock(return_value={
            "png_bytes": b"\x89PNG\r\n\x1a\n", "matched": 1, "total": 1, "quality_flag": "OK",
        }))
        monkeypatch.setattr(pipeline.api_client, "upload_task_item_chart", AsyncMock(return_value=False))

        result = await pipeline.preview_catalog_match("/x.fits", "task-1", "item-1")

        assert result["chart_uploaded"] is False

    async def test_astrometry_failure_propagates(self, monkeypatch):
        monkeypatch.setattr(
            pipeline.catalog_preview, "render",
            AsyncMock(side_effect=RuntimeError("Astrometry failed for frame.fits")),
        )

        with pytest.raises(RuntimeError, match="Astrometry failed"):
            await pipeline.preview_catalog_match("/x.fits", "task-1", "item-1")

    async def test_no_api_client_skips_upload_without_crashing(self, monkeypatch):
        monkeypatch.setattr(pipeline.catalog_preview, "render", AsyncMock(return_value={
            "png_bytes": b"\x89PNG\r\n\x1a\n", "matched": 1, "total": 1, "quality_flag": "OK",
        }))
        monkeypatch.setattr(pipeline, "api_client", None)

        result = await pipeline.preview_catalog_match("/x.fits", "task-1", "item-1")

        assert result["chart_uploaded"] is False

    async def test_bare_filename_is_resolved_against_archive_before_render(self, tmp_path, monkeypatch):
        """
        A task_item built from an already-registered frame's `frames.filename` (see
        observatory-api's Web\\FramesController::createTask()) carries a basename only. This
        must resolve to the real archived path before catalog_preview.render() is called.
        """
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path / "archive"))
        archived = tmp_path / "archive" / "M51" / "frame.fits"
        archived.parent.mkdir(parents=True)
        archived.touch()

        render_mock = AsyncMock(return_value={
            "png_bytes": b"\x89PNG\r\n\x1a\n", "matched": 1, "total": 1, "quality_flag": "OK",
        })
        monkeypatch.setattr(pipeline.catalog_preview, "render", render_mock)
        monkeypatch.setattr(pipeline, "api_client", None)

        await pipeline.preview_catalog_match("frame.fits", "task-1", "item-1")

        render_mock.assert_called_once_with(str(archived))


# ---------------------------------------------------------------------------
# _resolve_bare_filename — fallback for task_items built from frames.filename
# (a basename only) instead of a full path, e.g. observatory-api's debug UI
# ---------------------------------------------------------------------------


class TestResolveBareFilename:
    def test_path_with_directory_component_is_returned_unchanged(self):
        assert pipeline._resolve_bare_filename("/fits/incoming/frame.fits") == "/fits/incoming/frame.fits"
        assert pipeline._resolve_bare_filename("relative/frame.fits") == "relative/frame.fits"

    def test_bare_filename_resolves_to_its_single_archive_match(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path / "archive"))
        archived = tmp_path / "archive" / "M51" / "frame.fits"
        archived.parent.mkdir(parents=True)
        archived.touch()

        assert pipeline._resolve_bare_filename("frame.fits") == str(archived)

    def test_bare_filename_with_no_archive_match_is_returned_unchanged(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path / "archive"))
        (tmp_path / "archive").mkdir()

        assert pipeline._resolve_bare_filename("missing.fits") == "missing.fits"

    def test_bare_filename_with_multiple_archive_matches_uses_first_sorted(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path / "archive"))
        for obj in ("M51", "NGC1234"):
            d = tmp_path / "archive" / obj
            d.mkdir(parents=True)
            (d / "dup.fits").touch()

        result = pipeline._resolve_bare_filename("dup.fits")

        assert result == str(tmp_path / "archive" / "M51" / "dup.fits")


# ---------------------------------------------------------------------------
# _compute_pointing_error — mount pointing error vs. the plate-solved centre
# ---------------------------------------------------------------------------


class TestComputePointingError:
    def test_returns_none_when_header_has_no_mount_position(self):
        header = {"ra": None, "dec": None}
        astro_result = {"ra_center": 202.47, "dec_center": 47.20}

        result = pipeline._compute_pointing_error(header, astro_result)

        assert result == {
            "pointing_error_arcsec": None,
            "pointing_error_ra_arcsec": None,
            "pointing_error_dec_arcsec": None,
        }

    def test_returns_none_when_astrometry_never_solved(self):
        header = {"ra": 202.40, "dec": 47.10}

        result = pipeline._compute_pointing_error(header, {})

        assert result == {
            "pointing_error_arcsec": None,
            "pointing_error_ra_arcsec": None,
            "pointing_error_dec_arcsec": None,
        }

    def test_zero_offset_when_mount_and_solve_agree(self):
        header = {"ra": 202.47, "dec": 47.20}
        astro_result = {"ra_center": 202.47, "dec_center": 47.20}

        result = pipeline._compute_pointing_error(header, astro_result)

        assert result["pointing_error_arcsec"] == pytest.approx(0.0, abs=1e-6)
        assert result["pointing_error_ra_arcsec"] == pytest.approx(0.0, abs=1e-6)
        assert result["pointing_error_dec_arcsec"] == pytest.approx(0.0, abs=1e-6)

    def test_dec_only_offset_matches_signed_arcsec_delta(self):
        # 1 arcmin = 1/60 deg north of the mount's reported position.
        header = {"ra": 200.0, "dec": 10.0}
        astro_result = {"ra_center": 200.0, "dec_center": 10.0 + 1.0 / 60.0}

        result = pipeline._compute_pointing_error(header, astro_result)

        assert result["pointing_error_dec_arcsec"] == pytest.approx(60.0, abs=1e-3)
        assert result["pointing_error_ra_arcsec"] == pytest.approx(0.0, abs=1e-6)
        assert result["pointing_error_arcsec"] == pytest.approx(60.0, abs=1e-3)

    def test_ra_offset_is_scaled_by_cos_dec(self):
        # 1 deg of RA at dec=60 corresponds to cos(60)=0.5 deg of great-circle
        # separation, i.e. 1800 arcsec rather than the naive 3600.
        header = {"ra": 100.0, "dec": 60.0}
        astro_result = {"ra_center": 101.0, "dec_center": 60.0}

        result = pipeline._compute_pointing_error(header, astro_result)

        assert result["pointing_error_ra_arcsec"] == pytest.approx(1800.0, rel=1e-3)
        assert result["pointing_error_dec_arcsec"] == pytest.approx(0.0, abs=1e-6)
        assert result["pointing_error_arcsec"] == pytest.approx(1800.0, rel=1e-3)

    def test_wraps_across_the_0h_24h_ra_boundary(self):
        # Mount reports 359.9 deg, solve lands at 0.1 deg — a real 0.2 deg
        # (=720") offset, not a ~360 deg one.
        header = {"ra": 359.9, "dec": 0.0}
        astro_result = {"ra_center": 0.1, "dec_center": 0.0}

        result = pipeline._compute_pointing_error(header, astro_result)

        assert result["pointing_error_ra_arcsec"] == pytest.approx(720.0, rel=1e-3)
        assert result["pointing_error_arcsec"] == pytest.approx(720.0, rel=1e-3)

    def test_build_frame_payload_includes_pointing_error_fields(self):
        header = dict(_GOOD_HEADER)
        astro_result = dict(_GOOD_ASTRO)

        payload = pipeline._build_frame_payload(
            "frame.fits", header, _GOOD_QC, astro_result, filename="frame.fits"
        )

        assert payload["pointing_error_arcsec"] == pytest.approx(0.0, abs=1e-6)
        assert payload["pointing_error_ra_arcsec"] == pytest.approx(0.0, abs=1e-6)
        assert payload["pointing_error_dec_arcsec"] == pytest.approx(0.0, abs=1e-6)


# ---------------------------------------------------------------------------
# Step 0 — one-shot-colour (Bayer) frames
# ---------------------------------------------------------------------------

def _write_cfa_frame(path, bayer: str | None = "RGGB") -> None:
    hdu = fits.PrimaryHDU(data=np.full((40, 60), 5000, dtype=np.uint16))
    if bayer:
        hdu.header["BAYERPAT"] = bayer
    hdu.header["OBJECT"] = "NGC 7331"
    hdu.header["IMAGETYP"] = "Light"
    hdu.header["DATE-OBS"] = "2026-09-21T16:56:14"
    hdu.writeto(path)


def _sha(path) -> str:
    import hashlib
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


@pytest.fixture
def raw_archive(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "FITS_RAW_ARCHIVE", str(tmp_path / "raw"))
    monkeypatch.setattr(config, "NORMALIZE_ENABLED", True)
    return tmp_path / "raw"


def test_cfa_frame_converted_in_place_and_original_kept_byte_identical(raw_archive, tmp_path):
    frame = tmp_path / "incoming" / "Light_NGC 7331_60.0s_0001.fit"
    frame.parent.mkdir()
    _write_cfa_frame(frame)
    original_sha = _sha(frame)
    inode = os.stat(frame).st_ino

    pipeline._convert_cfa_frame(str(frame), {})

    raw_copy = raw_archive / "NGC7331" / frame.name
    assert _sha(raw_copy) == original_sha
    with fits.open(frame) as hdul:
        assert hdul[0].header["CFACONV"] is True
        assert hdul[0].data.shape == (20, 30)
    # Same inode: the watcher sees a modification, never a new file to enqueue.
    assert os.stat(frame).st_ino == inode


def test_cfa_conversion_is_idempotent(raw_archive, tmp_path):
    frame = tmp_path / "frame.fit"
    _write_cfa_frame(frame)

    pipeline._convert_cfa_frame(str(frame), {})
    converted_sha = _sha(frame)
    pipeline._convert_cfa_frame(str(frame), {})

    assert _sha(frame) == converted_sha
    assert len(list((raw_archive / "NGC7331").iterdir())) == 1


def test_cfa_raw_copy_never_overwrites_an_earlier_one(raw_archive, tmp_path):
    frame = tmp_path / "frame.fit"
    _write_cfa_frame(frame)
    earlier = raw_archive / "NGC7331" / "frame.fit"
    earlier.parent.mkdir(parents=True)
    earlier.write_bytes(b"an earlier original")

    pipeline._convert_cfa_frame(str(frame), {})

    assert earlier.read_bytes() == b"an earlier original"
    assert (raw_archive / "NGC7331" / "frame_1.fit").exists()


def test_mono_frame_is_left_alone(raw_archive, tmp_path):
    frame = tmp_path / "frame.fit"
    _write_cfa_frame(frame, bayer=None)
    before = _sha(frame)

    pipeline._convert_cfa_frame(str(frame), {})

    assert _sha(frame) == before
    assert not raw_archive.exists()


def test_unreadable_file_is_left_for_the_ordinary_failure(raw_archive, fits_file):
    before = fits_file.read_bytes()
    pipeline._convert_cfa_frame(str(fits_file), {})
    assert fits_file.read_bytes() == before


def test_failed_conversion_touches_nothing(raw_archive, tmp_path, monkeypatch):
    frame = tmp_path / "frame.fit"
    _write_cfa_frame(frame)
    before = _sha(frame)
    monkeypatch.setattr(pipeline.cfa, "to_superpixel", Mock(side_effect=OSError("disk full")))

    with pytest.raises(OSError):
        pipeline._convert_cfa_frame(str(frame), {})

    assert _sha(frame) == before
    assert not raw_archive.exists()


async def test_cfa_conversion_runs_before_header_extraction(mock_modules, monkeypatch):
    order = []
    monkeypatch.setattr(pipeline, "_convert_cfa_frame", lambda p, extra: order.append(("convert", p)))
    monkeypatch.setattr(
        "pipeline.fits_header.extract_headers",
        lambda p: order.append(("headers", p)) or _GOOD_HEADER,
    )

    await pipeline.analyze_frame(str(mock_modules))

    assert order[0] == ("convert", str(mock_modules))
    assert order[1][0] == "headers"


# ---------------------------------------------------------------------------
# Step 0 — files that are not a single exposure
# ---------------------------------------------------------------------------

def _write_frame(path, data: np.ndarray, **cards) -> None:
    hdu = fits.PrimaryHDU(data=data)
    hdu.header["OBJECT"] = "NGC 7331"
    for key, value in cards.items():
        hdu.header[key] = value
    hdu.writeto(path)


@pytest.fixture
def rejected_dir(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "FITS_REJECTED", str(tmp_path / "rejected"))
    monkeypatch.setattr(config, "NORMALIZE_ENABLED", True)
    return tmp_path / "rejected"


def test_colour_cube_is_moved_to_rejected(rejected_dir, tmp_path):
    frame = tmp_path / "cube.fit"
    _write_frame(frame, np.zeros((3, 20, 30), dtype=np.uint16), BAYERPAT="RGGB")

    assert pipeline._reject_unsupported_frame(str(frame), {}) is True

    assert not frame.exists()
    assert (rejected_dir / "NGC7331" / "UNSUPPORTED_cube.fit").exists()


@pytest.mark.parametrize("key", ["STACKCNT", "NCOMBINE"])
def test_stack_is_moved_to_rejected(rejected_dir, tmp_path, key):
    frame = tmp_path / "stack.fit"
    _write_frame(frame, np.zeros((20, 30), dtype=np.uint16), **{key: 48})

    assert pipeline._reject_unsupported_frame(str(frame), {}) is True
    assert (rejected_dir / "NGC7331" / "UNSUPPORTED_stack.fit").exists()


@pytest.mark.parametrize("cards", [{}, {"STACKCNT": 1}, {"NCOMBINE": 1}, {"BAYERPAT": "RGGB"}])
def test_single_exposure_is_accepted(rejected_dir, tmp_path, cards):
    frame = tmp_path / "single.fit"
    _write_frame(frame, np.zeros((20, 30), dtype=np.uint16), **cards)

    assert pipeline._reject_unsupported_frame(str(frame), {}) is False
    assert frame.exists()
    assert not rejected_dir.exists()


def test_unsupported_rejection_never_overwrites(rejected_dir, tmp_path):
    earlier = rejected_dir / "NGC7331" / "UNSUPPORTED_stack.fit"
    earlier.parent.mkdir(parents=True)
    earlier.write_bytes(b"earlier")
    frame = tmp_path / "stack.fit"
    _write_frame(frame, np.zeros((20, 30), dtype=np.uint16), STACKCNT=31)

    pipeline._reject_unsupported_frame(str(frame), {})

    assert earlier.read_bytes() == b"earlier"
    assert (rejected_dir / "NGC7331" / "UNSUPPORTED_stack_1.fit").exists()


def test_unreadable_file_is_not_rejected_here(rejected_dir, fits_file):
    assert pipeline._reject_unsupported_frame(str(fits_file), {}) is False
    assert fits_file.exists()


async def test_unsupported_frame_stops_before_any_analysis(mock_modules, monkeypatch):
    monkeypatch.setattr(pipeline, "_reject_unsupported_frame", lambda p, extra: True)
    convert = Mock()
    monkeypatch.setattr(pipeline, "_convert_cfa_frame", convert)

    result = await pipeline.analyze_frame(str(mock_modules))

    assert result is None
    convert.assert_not_called()
    pipeline.qc.analyze.assert_not_called()
    pipeline.api_client.post_frame.assert_not_called()
