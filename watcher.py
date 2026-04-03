"""
watcher.py — Entry point for the observatory-pipeline service.

Monitors the FITS_INCOMING directory using watchdog and dispatches each
new FITS file to the pipeline for processing.
"""

import asyncio
import logging
import os
import time

from watchdog.events import FileSystemEventHandler, FileCreatedEvent
from watchdog.observers import Observer

import config
import pipeline

logger = logging.getLogger(__name__)


FITS_EXTENSIONS: frozenset[str] = frozenset({".fits", ".fit"})


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
    """Process a single FITS file through the pipeline."""
    logger.info("Dispatching to pipeline: %s", fits_path)
    asyncio.run(pipeline.run(fits_path))


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
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )

    logger.info("Starting observatory-pipeline watcher on %s", config.FITS_INCOMING)

    # Process any FITS files that already exist in the incoming directory
    existing_count = process_existing_files(config.FITS_INCOMING)
    if existing_count > 0:
        logger.info("Processed %d existing FITS file(s)", existing_count)

    # Now start watching for new files
    event_handler = FitsEventHandler()
    observer = Observer()
    observer.schedule(event_handler, config.FITS_INCOMING, recursive=False)
    observer.start()

    logger.info("Watching for new FITS files...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown requested — stopping observer")
        observer.stop()

    observer.join()
    logger.info("Observatory-pipeline watcher stopped")
