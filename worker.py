"""
worker.py — Task-queue consumer for observatory-pipeline.

Polls `GET /tasks?status=PENDING&claimable=1` on observatory-api and
dispatches each task's items to the matching pipeline.py stage function:

    ANALYZE                -> pipeline.analyze_frame(fits_path)
    DETECT_ANOMALIES       -> pipeline.detect_anomalies_for_frame_id(frame_id)
    GENERATE_CHARTS        -> pipeline.generate_charts_for_source_ids(...)
    PREVIEW_CATALOG_MATCH  -> pipeline.preview_catalog_match(fits_path, task_id, item_id)
    DELETE_FRAME           -> pipeline.move_archived_file_to_rejected(filename, object_name)
    RESTART                -> clean exit (Docker restarts the container with fresh settings)

DELETE_FRAME is operator-initiated (observatory-api's Web\\FramesController,
same frame-selection UI as ANALYZE/DETECT_ANOMALIES) — the file is only
relocated to FITS_REJECTED, never deleted; observatory-api performs the
actual DB-side cascade delete once the move is reported DONE.

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
    in observatory-api, which already accepts an array either way). Each item
    also gets one extra, best-effort "RUNNING" call right before real work on
    it starts (see _mark_running()), so an operator watching task_items sees
    which file is currently being worked on instead of only PENDING flipping
    straight to DONE/FAILED with nothing in between.

Several pipelines (one per observatory server) can share one API, each with
its own API_KEY — the key is how the API knows which pipeline is calling. A
task that needs local files is targeted at the pipeline that registered
them, polling asks only for `claimable` tasks (its own or untargeted), and
the API refuses a claim on another pipeline's task (409) — the same "could
not claim" path as losing a race to another worker.

Graceful shutdown: SIGTERM (`docker compose stop`/`down`) or SIGINT lets the
item in progress finish, then stops — no further item or task is started. A
task left with unprocessed items is put back to PENDING, so the next start
(this worker or another) picks up exactly the rest. docker-compose.yml gives
the worker a long `stop_grace_period` for that; a second signal exits at once.

Known limitation: a task whose worker dies without that chance — killed
(OOM, SIGKILL after the grace period), or crashed — stays stuck at RUNNING
forever; there is no lease/heartbeat/timeout mechanism to recover it
automatically. Reset it back to PENDING by hand
(`PATCH /tasks/{id} {"status": "PENDING"}`) if that happens.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from collections.abc import Iterable, Iterator

import config
from logging_setup import configure_logging
import pipeline
import api_client
import version

logger = logging.getLogger(__name__)


class RestartRequested(Exception):
    """Raised by the RESTART task handler to signal a clean exit + restart."""


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

# Set by the SIGTERM/SIGINT handler installed in run_forever(). Checked only
# between items, never inside one: an item interrupted halfway could have
# registered a frame without its sources, or moved a file without reporting it.
_shutdown = asyncio.Event()


def _request_shutdown(signame: str) -> None:
    """Runs on the event loop (scheduled by the signal handler below)."""
    if _shutdown.is_set():
        # A second signal means "now": the item in progress is abandoned and
        # its task stays RUNNING, exactly like a kill.
        logger.warning("%s received again — exiting immediately", signame)
        os._exit(1)
    logger.info(
        "%s received — finishing the current item, then stopping (send it again to exit at once)",
        signame,
    )
    _shutdown.set()


# Signals received by this process; counted in the handler itself, so a
# second one is recognised even before the loop has run the first one's callback.
_signals_seen = 0


def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> dict:
    """
    SIGTERM/SIGINT through signal.signal(), not loop.add_signal_handler():
    the latter only runs once the event loop regains control, and QC,
    extraction, subtraction and photometry run synchronously on the loop's
    thread for minutes — a second signal would wait them out instead of
    exiting at once. A Python-level handler runs between bytecodes of
    whatever is executing; the first signal still only hands the request to
    the loop (call_soon_threadsafe), so no loop state is touched mid-callback.
    """
    global _signals_seen
    _signals_seen = 0

    def _handler(signum: int, frame: object) -> None:
        global _signals_seen
        _signals_seen += 1
        name = signal.Signals(signum).name
        if _signals_seen > 1:
            logger.warning("%s received again — exiting immediately", name)
            os._exit(1)
        loop.call_soon_threadsafe(_request_shutdown, name)

    return {sig: signal.signal(sig, _handler) for sig in (signal.SIGTERM, signal.SIGINT)}


def _until_shutdown(items: Iterable) -> Iterator:
    """Yield items until a shutdown is requested; the rest stay PENDING on the API."""
    for item in items:
        if _shutdown.is_set():
            return
        yield item


# ---------------------------------------------------------------------------
# Progress reporting
# ---------------------------------------------------------------------------

# An item still waiting for its outcome: PENDING, or RUNNING once _mark_running()
# reached the API but the item's DONE/FAILED never did (lost report, or the
# worker died mid-item). Both count as unfinished — filtering on PENDING alone
# would skip a RUNNING item on the next claim, and the task would never settle.
_UNFINISHED_ITEM_STATUSES = ("PENDING", "RUNNING")

# Item outcomes of the task in progress whose report never reached the API.
# Such an item stays PENDING (or RUNNING) there, so the API never settles the task: it
# would sit at RUNNING forever. _process_one_task() retries them once the
# handler returns and puts the task back in the queue if they still fail.
_unreported: list[dict] = []

# How often each task has been put back in the queue because item reports
# were lost — see _process_one_task(); capped by API_RECOVERY_MAX_ATTEMPTS.
_lost_report_requeues: dict[str, int] = {}


async def _report_progress(task_id: str, outcomes: list[dict]) -> None:
    """Report item outcomes; remember the ones the API did not take."""
    if await api_client.post_task_items_progress(task_id, outcomes) is None:
        logger.warning(
            "Progress for %d item(s) of task_id=%s was not recorded — will retry",
            len(outcomes), task_id,
        )
        _unreported.extend(outcomes)


async def _settle_unreported(task_id: str) -> bool:
    """
    Retry the outcomes _report_progress() could not deliver. True when
    nothing is left unreported.
    """
    if not _unreported:
        return True
    pending = list(_unreported)
    _unreported.clear()
    if await api_client.post_task_items_progress(task_id, pending) is not None:
        logger.info("Delivered %d late item report(s) for task_id=%s", len(pending), task_id)
        return True
    logger.error(
        "Could not report %d item outcome(s) of task_id=%s: %s",
        len(pending), task_id, pending,
    )
    return False


async def _mark_running(task_id: str, item_id: str) -> None:
    """
    Best-effort live-progress hint sent right before work starts on an item.

    Deliberately NOT routed through _report_progress()/_unreported: that
    queue is replayed later by _settle_unreported(), after the item's real
    DONE/FAILED outcome may already have landed. (observatory-api also never
    moves a DONE/FAILED item back to RUNNING, so even a late one is harmless.)
    The HTTP call itself still gets api_client's usual transport retries; once
    those are exhausted the hint is dropped — that only costs a late UI
    update, never correctness. A RUNNING item is still unfinished
    (_UNFINISHED_ITEM_STATUSES), so it is picked up again if its outcome is
    lost.
    """
    if await api_client.post_task_items_progress(task_id, [{"item_id": item_id, "status": "RUNNING"}]) is None:
        logger.debug("Could not mark item_id=%s RUNNING on task_id=%s (best-effort)", item_id, task_id)


# ---------------------------------------------------------------------------
# Per-item handlers
# ---------------------------------------------------------------------------


def _recovery_attempt(item: dict) -> int:
    """
    How many times this item is already a re-queue of failed work.

    `pipeline._queue_recovery_task()` puts a frame back on the queue when its
    sources or anomalies could not be posted (audit 2026-08-18, finding H19),
    writing the running count into the new item's own `payload`. Reading it
    back here is what bounds the loop: without it, a permanently failing frame
    — a 4xx the retry decorator deliberately does not retry — would re-queue
    itself forever.

    Anything unreadable counts as 0, i.e. an ordinary first attempt.
    """
    payload = item.get("payload")
    if not isinstance(payload, dict):
        return 0
    try:
        return max(0, int(payload.get("recovery_attempt", 0)))
    except (TypeError, ValueError):
        return 0


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
        result = await pipeline.analyze_frame(fits_path, _recovery_attempt(item))
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
    duration and then flip them all to DONE/FAILED at once. A best-effort
    "RUNNING" report is sent first, right before `_handle_analyze_item()`
    starts real work on the item, so there is live visibility into which
    file is currently being processed — but only once the item has passed
    its own validation (a missing filename still goes straight to FAILED,
    with no RUNNING report in between).
    """
    for item in _until_shutdown(items):
        if item.get("filename"):
            await _mark_running(task["id"], item["id"])
        outcome = await _handle_analyze_item(item)
        await _report_progress(task["id"], [outcome])


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
    for item in _until_shutdown(items):
        frame_id = item.get("frame_id")
        if not frame_id:
            await _report_progress(
                task["id"], [{"item_id": item["id"], "status": "FAILED", "error": "Item has no frame_id"}],
            )
            continue

        await _mark_running(task["id"], item["id"])

        try:
            await pipeline.detect_anomalies_for_frame_id(frame_id, _recovery_attempt(item))
        except Exception as exc:
            logger.exception("DETECT_ANOMALIES item failed for frame_id=%s", frame_id)
            await _report_progress(
                task["id"], [{"item_id": item["id"], "status": "FAILED", "error": str(exc)}],
            )
            continue

        await _report_progress(task["id"], [{"item_id": item["id"], "status": "DONE"}])



async def _run_charts_task(task: dict, items: list[dict]) -> None:
    """
    Process one GENERATE_CHARTS task, one source_id at a time, reporting each
    source's items as soon as its charts are done.

    It used to render every source in one batched call and report progress
    once at the very end, so a 96-item task sat at "0/96" in the UI for as
    long as the whole task took while charts were visibly being uploaded
    (2026-09-23). Rendering dominates the cost; one extra small
    POST /sources/tracks/batch per source is what the live progress costs.

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

    A task can carry MORE THAN ONE item for the same source_id, each with a
    different anomaly_type — observatory-api's AnomaliesController submits
    one item per distinct anomaly_type within a source's group rather than
    collapsing them to one (see that controller's createTask() docstring),
    since a source classified more than one way over its lifetime (e.g.
    UNKNOWN then MOVING_UNKNOWN once it moved) needs both its "track" and
    "stamp_strip" charts. All of a source_id's items therefore go into the
    same call, and each item's own outcome is looked up by
    (source_id, anomaly_type) afterwards, not by source_id alone — see
    modules/finder_chart.py's update_charts_for_sources() for why the result
    dict is nested that way.
    """
    items_by_source_id: dict[str, list[dict]] = {}
    malformed = []
    for item in items:
        source_id = item.get("source_id")
        if not source_id:
            malformed.append({
                "item_id": item["id"], "status": "FAILED",
                "error": "Item missing source_id",
            })
            continue
        items_by_source_id.setdefault(source_id, []).append(item)

    if malformed:
        await _report_progress(task["id"], malformed)

    for source_id, source_items in _until_shutdown(items_by_source_id.items()):
        for item in source_items:
            await _mark_running(task["id"], item["id"])

        payloads = [item.get("payload") or {} for item in source_items]
        designations = [p["designation"] for p in payloads if p.get("designation")]

        progress = []
        try:
            results = await pipeline.generate_charts_for_source_ids(
                {source_id: [p.get("anomaly_type") for p in payloads]},
                {source_id: designations[-1]} if designations else {},
            )
        except Exception as exc:
            logger.exception("GENERATE_CHARTS task_id=%s failed for source_id=%s", task["id"], source_id)
            progress = [
                {"item_id": item["id"], "status": "FAILED", "error": str(exc)}
                for item in source_items
            ]
        else:
            source_results = results.get(source_id) or {}
            for item, payload in zip(source_items, payloads):
                ok = source_results.get(payload.get("anomaly_type"))
                progress.append({
                    "item_id": item["id"],
                    "status": "DONE" if ok else "FAILED",
                    **({} if ok else {"error": "Chart update failed or was skipped"}),
                })

        await _report_progress(task["id"], progress)


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
    for item in _until_shutdown(items):
        fits_path = item.get("filename")

        if not fits_path:
            await _report_progress(
                task["id"], [{"item_id": item["id"], "status": "FAILED", "error": "Item has no filename"}],
            )
            continue

        await _mark_running(task["id"], item["id"])

        try:
            result = await pipeline.preview_catalog_match(fits_path, task["id"], item["id"])
        except Exception as exc:
            logger.exception("PREVIEW_CATALOG_MATCH item failed for %s", fits_path)
            await _report_progress(
                task["id"], [{"item_id": item["id"], "status": "FAILED", "error": str(exc)}],
            )
            continue

        await _report_progress(
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


async def _run_delete_frame_task(task: dict, items: list[dict]) -> None:
    """
    Process every PENDING item of one DELETE_FRAME task — sequentially,
    reporting each item's outcome immediately after it finishes, same
    reasoning as _run_analyze_task() above.

    Each item carries `frame_id` (observatory-api's Web\\FramesController
    builds these directly off an operator's frame selection — see
    CLAUDE.md's job-queue table). For each one:
      1. Fetch the frame via GET /frames/{id} for its `filename`/`object`.
      2. Physically move the file from its FITS_ARCHIVE location to
         FITS_REJECTED/{object}/ via pipeline.move_archived_file_to_rejected()
         — never deletes it.
      3. Report the item DONE. observatory-api's own
         TasksController::postItemsProgress() is responsible for the actual
         DB-side cascade delete (frames row + source_observations/
         frame_sources/anomalies + orphaned-source purge) once it sees this
         item transition to DONE for a DELETE_FRAME task — see that repo's
         CLAUDE.md.

    A missing file (already moved/deleted out-of-band) is logged but not
    treated as a failure — the actual goal is the DB-side deletion, which
    doesn't depend on the physical file still being found.
    """
    for item in _until_shutdown(items):
        frame_id = item.get("frame_id")
        if not frame_id:
            await _report_progress(
                task["id"], [{"item_id": item["id"], "status": "FAILED", "error": "Item has no frame_id"}],
            )
            continue

        await _mark_running(task["id"], item["id"])

        try:
            frame = await api_client.get_frame(frame_id)
        except Exception as exc:
            logger.exception("DELETE_FRAME item failed to fetch frame_id=%s", frame_id)
            await _report_progress(
                task["id"], [{"item_id": item["id"], "status": "FAILED", "error": str(exc)}],
            )
            continue

        if frame is None:
            await _report_progress(
                task["id"],
                [{"item_id": item["id"], "status": "FAILED", "error": f"frame_id={frame_id} not found"}],
            )
            continue

        filename = frame.get("filename")
        object_name = frame.get("object") or "_UNKNOWN"

        try:
            pipeline.move_archived_file_to_rejected(filename, object_name)
        except Exception as exc:
            logger.exception("DELETE_FRAME item failed to move file for frame_id=%s", frame_id)
            await _report_progress(
                task["id"], [{"item_id": item["id"], "status": "FAILED", "error": str(exc)}],
            )
            continue

        await _report_progress(task["id"], [{"item_id": item["id"], "status": "DONE"}])


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
    "DELETE_FRAME": _run_delete_frame_task,
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
    items = [i for i in detail["items"] if i["status"] in _UNFINISHED_ITEM_STATUSES]

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

    # The claim is atomic on the API side: PATCH status=RUNNING succeeds only
    # while the task is still PENDING and answers 409 otherwise (API audit
    # 2026-08-20, finding C2). update_task() returns None on any 4xx, so None
    # here means another worker got there first (or the task was cancelled
    # meanwhile) — processing it anyway would redo every item and overwrite
    # the other worker's results.
    if await api_client.update_task(task_id, "RUNNING") is None:
        logger.warning(
            "Could not claim task_id=%s (already claimed, cancelled, or the API "
            "refused) — leaving it to whoever holds it",
            task_id,
        )
        return

    logger.info(
        "Processing task_id=%s type=%s (%d pending item(s))",
        task_id, task["type"], len(items),
    )

    _unreported.clear()
    try:
        await handler(task, items)
    except RestartRequested:
        await api_client.update_task(task_id, "COMPLETED")
        raise
    except Exception as exc:
        logger.exception("Task_id=%s (%s) failed", task_id, task["type"])
        await api_client.update_task(task_id, "FAILED", error=str(exc))
        return

    all_reported = await _settle_unreported(task_id)
    if all_reported:
        _lost_report_requeues.pop(task_id, None)
    else:
        # Bounded like pipeline._queue_recovery_task(): a report the API keeps
        # refusing (a 4xx is never retried) or a progress endpoint that stays
        # down would otherwise re-queue the task, have it re-claimed at once
        # and its items redone, forever.
        requeues = _lost_report_requeues.get(task_id, 0) + 1
        if requeues > config.API_RECOVERY_MAX_ATTEMPTS:
            _lost_report_requeues.pop(task_id, None)
            logger.error(
                "Task_id=%s: item reports still undeliverable after %d re-queue(s) — failing the task",
                task_id, requeues - 1,
            )
            await api_client.update_task(
                task_id, "FAILED", error="Item progress could not be reported to the API",
            )
            return
        _lost_report_requeues[task_id] = requeues

    if _shutdown.is_set() or not all_reported:
        # Shutdown: the unstarted items go back in the queue. Lost reports:
        # those items are still PENDING on the API, which would otherwise
        # leave the task RUNNING forever; re-queued, they are redone.
        await _release_unfinished_task(task_id)


async def _release_unfinished_task(task_id: str) -> None:
    """
    Put a task interrupted by shutdown back in the queue — only if it still
    has unfinished (PENDING or RUNNING) items. One whose items were all reported has already been
    settled by the API (COMPLETED); flipping that back would re-queue
    finished work.
    """
    detail = await api_client.get_task(task_id)
    if detail is None:
        logger.warning("Could not re-check task_id=%s on shutdown — it may stay RUNNING", task_id)
        return

    remaining = sum(1 for i in detail["items"] if i["status"] in _UNFINISHED_ITEM_STATUSES)
    if remaining == 0:
        return

    if await api_client.update_task(task_id, "PENDING") is None:
        logger.warning("Could not release task_id=%s on shutdown — it stays RUNNING", task_id)
        return
    logger.info("Released task_id=%s back to PENDING with %d unprocessed item(s)", task_id, remaining)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


async def _idle_wait(seconds: float) -> None:
    """Sleep between empty polls, but wake the moment a shutdown is requested."""
    try:
        await asyncio.wait_for(_shutdown.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


async def run_forever() -> None:
    """
    Poll for PENDING tasks, one at a time (oldest first), backing off when
    idle, until SIGTERM/SIGINT requests a shutdown (see _request_shutdown()).
    """
    # A fresh event per run: an asyncio.Event binds to the first loop that
    # waits on it, and each asyncio.run() (each test, too) is a new loop.
    global _shutdown
    _shutdown = asyncio.Event()
    previous_handlers = _install_signal_handlers(asyncio.get_running_loop())
    try:
        await _poll_until_shutdown()
    finally:
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)


async def _poll_until_shutdown() -> None:
    """run_forever()'s body: settings, then the poll loop."""

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
        "Worker %s started — polling every %.0fs when idle (backs off up to %.0fs)",
        version.VERSION or "(unknown version)", base_interval, max_interval,
    )

    while not _shutdown.is_set():
        try:
            tasks = await api_client.get_tasks(
                status="PENDING", limit=1, order="asc", claimable=True,
            )
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

        await _idle_wait(interval)
        interval = min(interval * 2, max_interval)

    logger.info("Worker stopped")


if __name__ == "__main__":
    configure_logging("worker")

    asyncio.run(run_forever())
