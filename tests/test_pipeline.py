"""
tests/test_pipeline.py — Unit tests for pipeline.py and watcher.py.

All external dependencies (API client, module functions, subprocess) are
mocked at the pipeline module level. No real FITS I/O or network calls occur.
"""

from __future__ import annotations

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
    astro_mock.solve = AsyncMock(return_value=_GOOD_ASTRO)
    monkeypatch.setattr("pipeline.astrometry", astro_mock)

    # catalog_matcher — returns enriched sources (same structure, adds catalog fields)
    cat_mock = MagicMock()
    # Match returns sources with catalog fields added
    async def mock_match(sources, frame_meta):
        for s in sources:
            s.setdefault("catalog_name", "Gaia DR3")
            s.setdefault("catalog_id", "12345")
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
    astro_mock.solve = AsyncMock(return_value=_GOOD_ASTRO)
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

    with patch("watcher.time.sleep"), patch("watcher.asyncio.run") as mock_run:
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

    with patch("watcher.time.sleep"), patch("watcher.asyncio.run") as mock_run:
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
