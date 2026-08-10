"""
watcher.py — Entry point for the observatory-pipeline service.

Monitors the FITS_INCOMING directory using watchdog and batches newly
arrived files into ANALYZE tasks submitted to observatory-api's job queue
(api_client.create_task, docs/API.md section 15) instead of running each
file through the pipeline directly — worker.py is the process that actually
consumes these tasks (pipeline.analyze_frame() per item).

Batching, not one task per file, and why:
  - A bulk import (a whole archive dropped into FITS_INCOMING at once) would
    otherwise create one task PER FILE — thousands of tiny tasks instead of
    a handful of reasonably-sized ones, defeating the point of task-level
    batching entirely.
  - A live overnight run (one frame every 5-10 minutes) naturally produces
    small batches of 1 (or a handful, if several frames land close
    together) — the SAME debounce mechanism below handles both cases
    without a separate "bulk mode" code path.

Debounce mechanism: every newly arrived path is added to an in-memory
buffer, and a WATCHER_DEBOUNCE_SEC-second timer is (re)armed. If no new file
arrives before the timer fires, the buffer is flushed into one ANALYZE task.
If the buffer reaches WATCHER_MAX_BATCH_SIZE first (a large bulk drop),
it's flushed immediately instead of waiting out the full debounce window —
this also means progress becomes visible sooner during a big import instead
of one giant task sitting at 0% for a long time.

The flush itself (an HTTP call to observatory-api) always runs via
threading.Timer — even for the immediate/max-size case (a zero-delay timer)
— never inline inside the watchdog observer's own event-delivery thread, so
a slow API call never delays detection of the next arriving file.
"""

import asyncio
import logging
import os
import threading
import time

from watchdog.events import FileSystemEventHandler, FileCreatedEvent
from watchdog.observers import Observer

import config
from api_client import client as api_client

logger = logging.getLogger(__name__)


FITS_EXTENSIONS: frozenset[str] = frozenset({".fits", ".fit"})

# ---------------------------------------------------------------------------
# Pending-batch state, guarded by one lock — shared between the watchdog
# observer thread (on_created, via enqueue_path) and whichever thread a
# debounce/flush timer fires on (flush_pending_batch).
#
# `_pending_paths` holds the ORIGINAL path strings, in arrival order, that
# get submitted as the task's items. `_pending_realpaths` mirrors it as a
# set of resolved paths, used only for the duplicate-event dedup check
# below — kept as a separate structure (rather than deduping on the
# original path) because watchdog is known to occasionally deliver two
# FileCreatedEvents for the same file whose reported path can differ in
# resolvable ways (e.g. a symlinked bind mount).
# ---------------------------------------------------------------------------
_pending_paths: list[str] = []
_pending_realpaths: set[str] = set()
_pending_lock = threading.Lock()
_flush_timer: threading.Timer | None = None


class FitsEventHandler(FileSystemEventHandler):
    """Handle filesystem events, buffering FITS files for batched ANALYZE tasks."""

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

        enqueue_path(event.src_path)


def enqueue_path(fits_path: str) -> None:
    """
    Add one file path to the pending ANALYZE batch and (re)arm the debounce
    timer — or flush immediately if the batch has now reached
    WATCHER_MAX_BATCH_SIZE.

    Skips a path that no longer exists (e.g. a duplicate filesystem event
    delivered after this exact file was already flushed into a task and
    processed) or one already sitting in the current pending batch
    (watchdog occasionally delivers two FileCreatedEvents for the same
    path — e.g. the polling emitter used for Docker Desktop bind mounts on
    macOS, or a capture program that writes-then-renames the file).
    """
    if not os.path.exists(fits_path):
        logger.warning(
            "Skipping %s — file no longer exists (likely a duplicate "
            "filesystem event for an already-enqueued file)",
            fits_path,
        )
        return

    real_path = os.path.realpath(fits_path)

    with _pending_lock:
        if real_path in _pending_realpaths:
            logger.warning(
                "Skipping %s — already in the pending batch (duplicate "
                "on_created event for the same file)",
                fits_path,
            )
            return

        _pending_paths.append(fits_path)
        _pending_realpaths.add(real_path)
        batch_size = len(_pending_paths)

        _cancel_flush_timer_locked()

        if batch_size >= config.WATCHER_MAX_BATCH_SIZE:
            logger.info(
                "Pending batch reached WATCHER_MAX_BATCH_SIZE=%d — flushing immediately",
                config.WATCHER_MAX_BATCH_SIZE,
            )
            _arm_flush_timer_locked(delay=0.0)
        else:
            _arm_flush_timer_locked(delay=config.WATCHER_DEBOUNCE_SEC)


def _cancel_flush_timer_locked() -> None:
    """Cancel the currently armed flush timer, if any. Caller must hold `_pending_lock`."""
    global _flush_timer
    if _flush_timer is not None:
        _flush_timer.cancel()
        _flush_timer = None


def _arm_flush_timer_locked(delay: float) -> None:
    """(Re)arm the flush timer for `flush_pending_batch()`. Caller must hold `_pending_lock`."""
    global _flush_timer
    _flush_timer = threading.Timer(delay, flush_pending_batch)
    _flush_timer.daemon = True
    _flush_timer.start()


def flush_pending_batch() -> None:
    """
    Submit every currently-buffered path as one ANALYZE task and clear the
    buffer.

    Runs on the debounce timer's own thread (or, for a shutdown-triggered
    flush, the main thread) — never inline inside the watchdog observer
    thread — so a slow API call here never delays detection of the next
    arriving file. Best-effort: any failure submitting the task is logged;
    the files involved simply aren't processed until resubmitted (they're
    still sitting in FITS_INCOMING, unlike the old per-file dispatch, which
    would have already moved a successfully-processed file out of it).
    """
    global _flush_timer

    with _pending_lock:
        paths = _pending_paths.copy()
        _pending_paths.clear()
        _pending_realpaths.clear()
        _flush_timer = None

    if not paths:
        return

    items = [{"filename": p} for p in paths]

    try:
        created = asyncio.run(api_client.create_task("ANALYZE", items))
    except Exception:
        logger.exception("Failed to submit ANALYZE task for %d file(s)", len(paths))
        return

    if created is None:
        logger.warning(
            "ANALYZE task submission returned no task for %d file(s) — "
            "these files will not be processed until resubmitted",
            len(paths),
        )
        return

    logger.info(
        "Submitted ANALYZE task_id=%s for %d file(s)",
        created.get("id"),
        len(paths),
    )


def process_existing_files(directory: str) -> int:
    """
    Scan directory for existing FITS files and enqueue them into the
    pending batch — the same path a freshly-arrived file takes. Handles the
    "pipeline was down, files piled up" startup case as one bulk batch
    (or a few, if WATCHER_MAX_BATCH_SIZE is exceeded) rather than dispatching
    each file as its own separate event.

    Parameters
    ----------
    directory:
        Path to the directory to scan.

    Returns
    -------
    int
        Number of files found and enqueued.
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
            enqueue_path(filepath)
            count += 1
    except OSError as exc:
        logger.error("Error scanning directory %s: %s", directory, exc)

    return count


async def _fetch_and_apply_remote_settings() -> None:
    """Fetch remote config from the API and overlay it onto config globals."""
    settings = await api_client.get_settings()
    if settings:
        applied = config.apply_remote_settings(settings)
        logger.info("Applied %d remote setting(s) from API", applied)
    else:
        logger.info("No remote settings fetched — using local defaults only")


if __name__ == "__main__":
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)-5s %(module)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Fetch remote configuration from the API before anything else.
    # If the API is unreachable or returns an error, all local defaults
    # from config.py remain in effect — the watcher still starts normally.
    asyncio.run(_fetch_and_apply_remote_settings())

    # Re-apply log level in case the remote settings changed it.
    logging.getLogger().setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))

    logger.info(
        "Starting watcher on %s (log level: %s, debounce: %.1fs, max batch: %d)",
        config.FITS_INCOMING, config.LOG_LEVEL, config.WATCHER_DEBOUNCE_SEC, config.WATCHER_MAX_BATCH_SIZE,
    )

    # Enqueue any FITS files that already exist in the incoming directory
    existing_count = process_existing_files(config.FITS_INCOMING)
    if existing_count > 0:
        logger.info("Enqueued %d existing file(s)", existing_count)

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
        # Submit whatever's still buffered right now rather than dropping it
        # silently — otherwise files that arrived just before the debounce
        # window would have fired are lost from the task queue's view (they
        # remain on disk, but nothing would ever pick them up again until
        # the watcher restarts and rescans FITS_INCOMING).
        with _pending_lock:
            _cancel_flush_timer_locked()
        flush_pending_batch()

    observer.join()
    logger.info("Watcher stopped")
