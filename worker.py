"""
worker.py — Task-queue consumer for observatory-pipeline.

Polls `GET /tasks?status=PENDING` on observatory-api and dispatches each
task's items to the matching pipeline.py stage function:

    ANALYZE                -> pipeline.analyze_frame(fits_path)
    DETECT_ANOMALIES       -> pipeline.detect_anomalies_for_frame_id(frame_id)
    GENERATE_CHARTS        -> pipeline.generate_charts_for_source_ids(...)
    PREVIEW_CATALOG_MATCH  -> pipeline.preview_catalog_match(fits_path, task_id, item_id)
    RESTART                -> clean exit (Docker restarts the container with fresh settings)

DETECT_ANOMALIES and GENERATE_CHARTS are decoupled: anomaly detection saves
its results to the API, and the operator then decides which anomalies need
charts and creates a GENERATE_CHARTS task from the UI (referencing
anomaly_ids from the anomalies table). There is no automatic follow-up task
creation between these two stages.

This is deliberately a SEPARATE process from watcher.py (see docker-compose's
`worker` service) so it can be scaled, restarted, or stopped independently
of the filesystem watcher. Tasks reach this worker two ways: watcher.py
submits batched `ANALYZE` tasks as files arrive (see that module's own
docstring for the debounce/batching design), and anything else — a manual
`api_client.create_task()` call, a future reprocess CLI, ad hoc via the API
— for the other three types.

Politeness ("не нагружать сервер"):
  - Polls at TASK_POLL_INTERVAL_SEC when idle, backing off exponentially up
    to TASK_POLL_BACKOFF_MAX_SEC on consecutive empty polls, resetting the
    moment a task is found. A busy queue is drained back-to-back with no
    sleep between tasks; an idle one is checked increasingly rarely.
  - Fetches exactly one task per poll (`limit=1`, `order=asc` — oldest
    PENDING task first) rather than a large batch, so a burst of newly
    submitted tasks is worked through steadily rather than fetched all at
    once and held in memory.
  - Progress is reported via one POST /tasks/{id}/items/progress call per
    processed item — simpler than batching for this first version; if a
    task's item count grows large enough for that to matter, batching every
    N items is a small, local change (see TasksController::postItemsProgress
    in observatory-api, which already accepts an array either way).

Known limitation: a task that a worker claims (PATCH status=RUNNING) and
then crashes on — before finishing — stays stuck at RUNNING forever; this
first version has no lease/heartbeat/timeout mechanism to recover it
automatically. During testing, reset it back to PENDING by hand
(`PATCH /tasks/{id} {"status": "PENDING"}`) if that happens.
"""

from __future__ import annotations

import asyncio
import logging
import sys

import config
import pipeline
from api_client import client as api_client

logger = logging.getLogger(__name__)


class RestartRequested(Exception):
    """Raised by the RESTART task handler to signal a clean exit + restart."""


# ---------------------------------------------------------------------------
# Per-item handlers
# ---------------------------------------------------------------------------


async def _handle_analyze_item(item: dict) -> dict:
    """
    Run analyze_frame() for one ANALYZE task item.

    `item["filename"]` is the full path to the FITS file, as submitted at
    task-creation time — not just a basename; whatever created this task is
    responsible for knowing where the file actually is (FITS_INCOMING for a
    freshly-arrived file, or an archive/rejected path for a manual re-run).

    Returns
    -------
    dict
        {"item_id", "status": "DONE"|"FAILED", "frame_id"?, "error"?} — the
        exact shape POST /tasks/{id}/items/progress expects for one entry.
    """
    fits_path = item.get("filename")

    if not fits_path:
        return {"item_id": item["id"], "status": "FAILED", "error": "Item has no filename"}

    try:
        result = await pipeline.analyze_frame(fits_path)
    except Exception as exc:
        logger.exception("ANALYZE item failed for %s", fits_path)
        return {"item_id": item["id"], "status": "FAILED", "error": str(exc)}

    if result is None:
        # Not necessarily an error — a calibration frame or a QC rejection
        # is a normal, successful outcome with no frame_id to report.
        return {"item_id": item["id"], "status": "DONE"}

    return {"item_id": item["id"], "status": "DONE", "frame_id": result["frame_id"]}


async def _run_analyze_task(task: dict, items: list[dict]) -> None:
    """
    Process every PENDING item of one ANALYZE task, sequentially, reporting
    each item's outcome to the API immediately after it finishes — not
    accumulated and reported once at the end of the whole task. Each file
    can take real wall-clock time (plate solving, catalog queries), so
    batching every item's progress into one call at the very end would leave
    `task_items` showing every item stuck at PENDING for the task's entire
    duration and then flip them all to DONE/FAILED at once — no live
    visibility into which file is currently being processed.
    """
    for item in items:
        outcome = await _handle_analyze_item(item)
        await api_client.post_task_items_progress(task["id"], [outcome])


async def _run_detect_task(task: dict, items: list[dict]) -> None:
    """
    Process every PENDING item of one DETECT_ANOMALIES task — reporting each
    frame's outcome immediately after it finishes, same reasoning as
    _run_analyze_task() above.

    Unlike before, this handler no longer auto-creates a follow-up
    GENERATE_CHARTS task. The operator decides which anomalies need charts
    and submits a GENERATE_CHARTS task from the UI, referencing specific
    anomaly_ids from the anomalies table.
    """
    for item in items:
        frame_id = item.get("frame_id")
        if not frame_id:
            await api_client.post_task_items_progress(
                task["id"], [{"item_id": item["id"], "status": "FAILED", "error": "Item has no frame_id"}],
            )
            continue

        try:
            await pipeline.detect_anomalies_for_frame_id(frame_id)
        except Exception as exc:
            logger.exception("DETECT_ANOMALIES item failed for frame_id=%s", frame_id)
            await api_client.post_task_items_progress(
                task["id"], [{"item_id": item["id"], "status": "FAILED", "error": str(exc)}],
            )
            continue

        await api_client.post_task_items_progress(task["id"], [{"item_id": item["id"], "status": "DONE"}])



async def _run_charts_task(task: dict, items: list[dict]) -> None:
    """
    Process one GENERATE_CHARTS task — a single batched call covering every
    item's source_id at once, regardless of how many items the task has.

    `payload.anomaly_type` is optional, not required: an item created from
    an anomaly (observatory-api's AnomaliesController::createTask) carries
    it, but an item created directly from a source with no anomaly at all
    (SourcesController::createTask, `/ui/sources/generate-charts`) never
    does — that endpoint intentionally sends only `source_id`, leaving style
    selection to the pipeline (see that controller's own docstring). A
    missing anomaly_type is passed through as None rather than failing the
    item; modules/finder_chart.py already falls back gracefully for it (see
    that module's `_style_for_source()`/`_style_for_anomaly_type()`) —
    "before_after" for a single-epoch source, "stamp_strip" otherwise, with
    no anomaly_type in the chart title.
    """
    anomaly_type_by_source_id: dict = {}
    designation_by_source_id: dict = {}
    valid_items: list[dict] = []

    progress = []
    for item in items:
        source_id = item.get("source_id")
        payload = item.get("payload") or {}
        anomaly_type = payload.get("anomaly_type")

        if not source_id:
            progress.append({
                "item_id": item["id"], "status": "FAILED",
                "error": "Item missing source_id",
            })
            continue

        anomaly_type_by_source_id[source_id] = anomaly_type
        if payload.get("designation"):
            designation_by_source_id[source_id] = payload["designation"]
        valid_items.append(item)

    results: dict = {}
    if anomaly_type_by_source_id:
        try:
            results = await pipeline.generate_charts_for_source_ids(
                anomaly_type_by_source_id, designation_by_source_id,
            )
        except Exception as exc:
            logger.exception("GENERATE_CHARTS task_id=%s failed", task["id"])
            for item in valid_items:
                progress.append({"item_id": item["id"], "status": "FAILED", "error": str(exc)})
            valid_items = []  # already recorded above — don't record twice below

    for item in valid_items:
        ok = results.get(item.get("source_id"))
        progress.append({
            "item_id": item["id"],
            "status": "DONE" if ok else "FAILED",
            **({} if ok else {"error": "Chart update failed or was skipped"}),
        })

    await api_client.post_task_items_progress(task["id"], progress)


async def _run_preview_task(task: dict, items: list[dict]) -> None:
    """
    Process every PENDING item of one PREVIEW_CATALOG_MATCH task,
    sequentially, reporting each item's outcome immediately after it
    finishes — same reasoning as _run_analyze_task() above.
    pipeline.preview_catalog_match() already uploads the rendered chart to
    the API itself (POST /tasks/{task_id}/items/{item_id}/chart); the
    matched/total/quality_flag/chart_uploaded summary is written back onto
    that item's own `payload` (visible via GET /tasks/{id}) so the outcome
    is discoverable from the task itself without digging through logs.
    """
    for item in items:
        fits_path = item.get("filename")

        if not fits_path:
            await api_client.post_task_items_progress(
                task["id"], [{"item_id": item["id"], "status": "FAILED", "error": "Item has no filename"}],
            )
            continue

        try:
            result = await pipeline.preview_catalog_match(fits_path, task["id"], item["id"])
        except Exception as exc:
            logger.exception("PREVIEW_CATALOG_MATCH item failed for %s", fits_path)
            await api_client.post_task_items_progress(
                task["id"], [{"item_id": item["id"], "status": "FAILED", "error": str(exc)}],
            )
            continue

        await api_client.post_task_items_progress(
            task["id"],
            [{
                "item_id": item["id"],
                "status": "DONE",
                "payload": {
                    "matched": result["matched"],
                    "total": result["total"],
                    "quality_flag": result["quality_flag"],
                    "chart_uploaded": result["chart_uploaded"],
                },
            }],
        )


async def _run_restart_task(task: dict, items: list[dict]) -> None:
    """
    Handle a RESTART task — a signal from the API that the worker should
    restart itself (e.g. because remote settings changed).

    RESTART is a signal task with no items. The handler simply raises
    RestartRequested, which _process_one_task() catches and re-raises after
    marking the task COMPLETED — the main loop in run_forever() then lets
    the process exit cleanly. Docker's `restart: unless-stopped` policy
    restarts the container, which re-fetches settings from the API on
    startup.
    """
    logger.info("RESTART task received — will exit after marking task completed")
    raise RestartRequested()


_HANDLERS = {
    "ANALYZE": _run_analyze_task,
    "DETECT_ANOMALIES": _run_detect_task,
    "GENERATE_CHARTS": _run_charts_task,
    "PREVIEW_CATALOG_MATCH": _run_preview_task,
    "RESTART": _run_restart_task,
}

# Task types that carry no items — the task itself is the action.
_SIGNAL_TASK_TYPES = frozenset({"RESTART"})


# ---------------------------------------------------------------------------
# Task dispatch
# ---------------------------------------------------------------------------


async def _process_one_task(task_summary: dict) -> None:
    """Fetch a task's full detail, claim it, dispatch to its handler."""
    task_id = task_summary["id"]

    detail = await api_client.get_task(task_id)
    if detail is None:
        logger.warning("Could not fetch detail for task_id=%s — skipping this poll", task_id)
        return

    task = detail["task"]
    items = [i for i in detail["items"] if i["status"] == "PENDING"]

    # Signal tasks (e.g. RESTART) have no items — the task itself is the action.
    is_signal_task = task["type"] in _SIGNAL_TASK_TYPES

    if not items and not is_signal_task:
        # Nothing left to do (e.g. every item already resolved by an earlier,
        # interrupted run) — just let the API's own bumpProgress() logic
        # settle its status; nothing for this worker to claim.
        return

    handler = _HANDLERS.get(task["type"])
    if handler is None:
        logger.error("Unknown task type=%s for task_id=%s", task["type"], task_id)
        await api_client.update_task(task_id, "FAILED", error=f"Unknown task type: {task['type']}")
        return

    await api_client.update_task(task_id, "RUNNING")
    logger.info(
        "Processing task_id=%s type=%s (%d pending item(s))",
        task_id, task["type"], len(items),
    )

    try:
        await handler(task, items)
    except RestartRequested:
        await api_client.update_task(task_id, "COMPLETED")
        raise
    except Exception as exc:
        logger.exception("Task_id=%s (%s) failed", task_id, task["type"])
        await api_client.update_task(task_id, "FAILED", error=str(exc))


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


async def run_forever() -> None:
    """
    Poll for PENDING tasks forever, one at a time (oldest first), backing
    off when idle. Runs until cancelled (KeyboardInterrupt in __main__).
    """
    # Fetch remote configuration from the API before entering the poll loop.
    # If the API is unreachable or returns an error, all local defaults from
    # config.py remain in effect — the worker still starts normally.
    settings = await api_client.get_settings()
    if settings:
        applied = config.apply_remote_settings(settings)
        logger.info("Applied %d remote setting(s) from API", applied)

        # Re-apply log level in case the remote settings changed it.
        logging.getLogger().setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))
    else:
        logger.info("No remote settings fetched — using local defaults only")

    base_interval = config.TASK_POLL_INTERVAL_SEC
    max_interval = config.TASK_POLL_BACKOFF_MAX_SEC
    interval = base_interval

    logger.info(
        "Worker started — polling every %.0fs when idle (backs off up to %.0fs)",
        base_interval, max_interval,
    )

    while True:
        try:
            tasks = await api_client.get_tasks(status="PENDING", limit=1, order="asc")
        except Exception:
            logger.exception("Failed to poll for pending tasks")
            tasks = []

        if tasks:
            interval = base_interval
            try:
                await _process_one_task(tasks[0])
            except RestartRequested:
                logger.info("Restart requested — exiting so the container restarts with fresh settings")
                sys.exit(0)
            continue  # check again immediately — the queue may have more work

        await asyncio.sleep(interval)
        interval = min(interval * 2, max_interval)


if __name__ == "__main__":
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)-5s %(module)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        logger.info("Shutdown requested — stopping worker")
