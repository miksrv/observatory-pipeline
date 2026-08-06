"""
watcher.py — Entry point for the observatory-pipeline service.

Monitors the FITS_INCOMING directory using watchdog and dispatches each
new FITS file to the pipeline for processing.
"""

import asyncio
import logging
import os
import threading
import time

from watchdog.events import FileSystemEventHandler, FileCreatedEvent
from watchdog.observers import Observer

import config
import pipeline

logger = logging.getLogger(__name__)


FITS_EXTENSIONS: frozenset[str] = frozenset({".fits", ".fit"})

# ---------------------------------------------------------------------------
# Duplicate-dispatch guard.
#
# Regression: the same FITS file was seen registered as TWO separate frames
# a few seconds apart (same filename, same obs_time, same photometry —
# `Vesta_A807_FA_Light_L_60_2021-03-14T17-05-27.fits` got two distinct
# frame_ids in the API, inflating object_stats.frame_count and
# sources.observation_count for every source in it). watchdog is known to
# occasionally deliver two FileCreatedEvent for one path — e.g. on the
# polling-based emitter used for Docker Desktop bind mounts on macOS, or
# when a capture program writes-then-renames the file. `_paths_in_flight`
# rejects a second dispatch for a path that's still being processed; the
# `os.path.exists` check in `process_fits_file` catches the remaining case
# where the duplicate event arrives strictly after the first dispatch
# already finished and moved the file out of FITS_INCOMING.
# ---------------------------------------------------------------------------
_paths_in_flight: set[str] = set()
_paths_in_flight_lock = threading.Lock()


class FitsEventHandler(FileSystemEventHandler):
    """Handle filesystem events, dispatching FITS files to the pipeline."""

    def on_created(self, event: FileCreatedEvent) -> None:
        """Respond to file-creation events in the monitored directory."""
        if event.is_directory:
            return

        ext = os.path.splitext(event.src_path)[1].lower()
        if ext not in FITS_EXTENSIONS:
            return

        logger.info("New FITS file detected: %s", event.src_path)

        # Wait briefly for the writing process to finish flushing the file.
        time.sleep(2)

        process_fits_file(event.src_path)


def process_fits_file(fits_path: str) -> None:
    """
    Process a single FITS file through the pipeline.

    Guards against processing the same file twice from a duplicate
    filesystem event (see the `_paths_in_flight` module comment above):
    skips dispatch if this exact path is already being processed, or if it
    no longer exists (already processed and moved out of FITS_INCOMING by
    an earlier, still-in-flight or already-finished call).
    """
    if not os.path.exists(fits_path):
        logger.warning(
            "Skipping %s — file no longer exists (likely a duplicate "
            "filesystem event for an already-processed file)",
            fits_path,
        )
        return

    real_path = os.path.realpath(fits_path)
    with _paths_in_flight_lock:
        if real_path in _paths_in_flight:
            logger.warning(
                "Skipping %s — already being processed by another event "
                "(duplicate on_created event for the same file)",
                fits_path,
            )
            return
        _paths_in_flight.add(real_path)

    try:
        logger.info("Dispatching to pipeline: %s", fits_path)
        asyncio.run(pipeline.run(fits_path))
    finally:
        with _paths_in_flight_lock:
            _paths_in_flight.discard(real_path)


def process_existing_files(directory: str) -> int:
    """
    Scan directory for existing FITS files and process them.

    Parameters
    ----------
    directory:
        Path to the directory to scan.

    Returns
    -------
    int
        Number of files processed.
    """
    count = 0
    try:
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)

            # Skip directories
            if os.path.isdir(filepath):
                continue

            ext = os.path.splitext(filename)[1].lower()
            if ext not in FITS_EXTENSIONS:
                continue

            logger.info("Found existing FITS file: %s", filepath)
            process_fits_file(filepath)
            count += 1
    except OSError as exc:
        logger.error("Error scanning directory %s: %s", directory, exc)

    return count


if __name__ == "__main__":
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)-5s %(module)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("Starting watcher on %s (log level: %s)", config.FITS_INCOMING, config.LOG_LEVEL)

    # Process any FITS files that already exist in the incoming directory
    existing_count = process_existing_files(config.FITS_INCOMING)
    if existing_count > 0:
        logger.info("Processed %d existing file(s)", existing_count)

    # Now start watching for new files
    event_handler = FitsEventHandler()
    observer = Observer()
    observer.schedule(event_handler, config.FITS_INCOMING, recursive=False)
    observer.start()

    logger.info("Watching for new FITS files in %s", config.FITS_INCOMING)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown requested — stopping")
        observer.stop()

    observer.join()
    logger.info("Watcher stopped")
