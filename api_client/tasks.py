"""
api_client/tasks.py — the job-queue resource backing worker.py: task
creation/listing/status updates, per-item progress reporting, and the
task-item-scoped chart upload used by PREVIEW_CATALOG_MATCH.

See docs/API.md section 14 and CLAUDE.md's "Job queue" section.
"""

from __future__ import annotations

import logging

from ._shared import _content_type_for_image_bytes, _make_client, _retry, _RETRYABLE

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task queue — POST /tasks, GET /tasks(/{id}), PATCH /tasks/{id},
# POST /tasks/{id}/items/progress
#
# Backs worker.py's task consumption and any future task-submitting tool
# (a manual reprocess CLI, a future watcher.py rewrite). See CLAUDE.md's
# job-queue section.
# ---------------------------------------------------------------------------

@_retry
async def _create_task_with_retry(
    task_type: str,
    items: list[dict],
    scope: dict | None,
    parent_task_id: str | None,
) -> dict | None:
    """Inner retryable core for create_task."""
    payload: dict = {"type": task_type, "items": items}
    if scope is not None:
        payload["scope"] = scope
    if parent_task_id is not None:
        payload["parent_task_id"] = parent_task_id

    async with _make_client() as client:
        response = await client.post("/tasks", json=payload)

        if 400 <= response.status_code < 500:
            logger.error(
                "API rejected POST /tasks (type=%s) with HTTP %d: %s",
                task_type, response.status_code, response.text,
                extra={"frame_id": None, "log_filename": None},
            )
            return None

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    return resp_json if isinstance(resp_json, dict) else None


async def create_task(
    task_type: str,
    items: list[dict] | None = None,
    scope: dict | None = None,
    parent_task_id: str | None = None,
) -> dict | None:
    """
    Create a task with its full, fixed item list.

    Parameters
    ----------
    task_type:
        "ANALYZE", "DETECT_ANOMALIES", "GENERATE_CHARTS",
        "PREVIEW_CATALOG_MATCH", or "RESTART".
    items:
        List of item dicts, each with exactly one of "filename" (ANALYZE),
        "frame_id" (DETECT_ANOMALIES), or "source_id" (GENERATE_CHARTS) —
        plus an optional "payload" (arbitrary JSON-encodable value, e.g. a
        GENERATE_CHARTS item's {"anomaly_type": ..., "designation": ...}).
        May be None or empty for signal tasks (RESTART).
    scope:
        Optional descriptive scope ({"object": ..., "date_from": ...,
        "date_to": ...}) — display/filtering only, not authoritative.
    parent_task_id:
        Optional — links a re-run to the task it re-runs.

    Returns
    -------
    dict | None
        {"id", "type", "status", "total_items", "message"}, or None on any
        failure — the caller (worker.py) must handle a None return (log and
        move on; the work that would have been queued is simply not queued).
    """
    if items is None:
        items = []
    logger.info(
        "POST /tasks type=%s items=%d",
        task_type, len(items),
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _create_task_with_retry(task_type, items, scope, parent_task_id)
    except _RETRYABLE as exc:
        logger.error(
            "All retries exhausted creating task type=%s: %s",
            task_type, exc,
            extra={"frame_id": None, "log_filename": None},
        )
    return None


@_retry
async def _get_tasks_with_retry(
    status: str | None,
    task_type: str | None,
    object_name: str | None,
    limit: int,
    order: str | None,
) -> list:
    """Inner retryable core for get_tasks."""
    params: dict = {"limit": limit}
    if status is not None:
        params["status"] = status
    if task_type is not None:
        params["type"] = task_type
    if object_name is not None:
        params["object"] = object_name
    if order is not None:
        params["order"] = order

    async with _make_client() as client:
        response = await client.get("/tasks", params=params)

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    data = resp_json.get("data") if isinstance(resp_json, dict) else None
    return data if isinstance(data, list) else []


async def get_tasks(
    status: str | None = None,
    task_type: str | None = None,
    object_name: str | None = None,
    limit: int = 50,
    order: str | None = None,
) -> list:
    """
    List tasks, filtered by status/type/object.

    Parameters
    ----------
    status, task_type, object_name:
        Optional exact-match filters.
    limit:
        Max rows (the API caps at 500).
    order:
        "asc" or "desc" (API default "desc" — most recent first). A FIFO
        worker polling for PENDING work should pass "asc" to claim the
        oldest queued task first rather than whatever was just submitted.

    Returns
    -------
    list
        List of task summary dicts (no "items" — see get_task() for that).
        [] on any failure.
    """
    logger.debug(
        "GET /tasks status=%s type=%s object=%s order=%s",
        status, task_type, object_name, order,
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _get_tasks_with_retry(status, task_type, object_name, limit, order)
    except Exception as exc:
        logger.error(
            "Error querying /tasks: %s",
            exc,
            extra={"frame_id": None, "log_filename": None},
        )
        return []


@_retry
async def _get_task_with_retry(task_id: str) -> dict | None:
    """Inner retryable core for get_task."""
    async with _make_client() as client:
        response = await client.get(f"/tasks/{task_id}")

        if response.status_code == 404:
            return None

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    return resp_json if isinstance(resp_json, dict) else None


async def get_task(task_id: str) -> dict | None:
    """
    Retrieve one task's detail, including its full item list.

    Returns
    -------
    dict | None
        {"task": {...}, "items": [...]}, or None if not found or on any
        failure — a caller can't tell those two cases apart from the return
        value alone.
    """
    logger.debug(
        "GET /tasks/%s",
        task_id,
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _get_task_with_retry(task_id)
    except Exception as exc:
        logger.error(
            "Error querying /tasks/%s: %s",
            task_id, exc,
            extra={"frame_id": None, "log_filename": None},
        )
        return None


@_retry
async def _update_task_with_retry(task_id: str, status: str, error: str | None) -> dict | None:
    """Inner retryable core for update_task."""
    payload: dict = {"status": status}
    if error is not None:
        payload["error"] = error

    async with _make_client() as client:
        response = await client.patch(f"/tasks/{task_id}", json=payload)

        if 400 <= response.status_code < 500:
            logger.error(
                "API rejected PATCH /tasks/%s with HTTP %d: %s",
                task_id, response.status_code, response.text,
                extra={"frame_id": None, "log_filename": None},
            )
            return None

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    return resp_json.get("task") if isinstance(resp_json, dict) else None


async def update_task(task_id: str, status: str, error: str | None = None) -> dict | None:
    """
    Update a task's own status — e.g. worker.py flips PENDING -> RUNNING
    when it picks a task up, or -> FAILED (with `error`) on an unhandled
    exception. Reaching COMPLETED normally happens automatically on the API
    side once every item resolves (see post_task_items_progress()) — this is
    for the states that are always explicit.

    Returns
    -------
    dict | None
        The updated task dict, or None on any failure.
    """
    logger.info(
        "PATCH /tasks/%s status=%s",
        task_id, status,
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _update_task_with_retry(task_id, status, error)
    except _RETRYABLE as exc:
        logger.error(
            "All retries exhausted updating task_id=%s to status=%s: %s",
            task_id, status, exc,
            extra={"frame_id": None, "log_filename": None},
        )
    return None


@_retry
async def _post_task_items_progress_with_retry(task_id: str, items: list[dict]) -> dict | None:
    """Inner retryable core for post_task_items_progress."""
    async with _make_client() as client:
        response = await client.post(f"/tasks/{task_id}/items/progress", json={"items": items})

        if 400 <= response.status_code < 500:
            logger.error(
                "API rejected POST /tasks/%s/items/progress with HTTP %d: %s",
                task_id, response.status_code, response.text,
                extra={"frame_id": None, "log_filename": None},
            )
            return None

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    return resp_json if isinstance(resp_json, dict) else None


async def post_task_items_progress(task_id: str, items: list[dict]) -> dict | None:
    """
    Report the outcome of one or more task items in a single call.

    Parameters
    ----------
    task_id:
        The task these items belong to.
    items:
        List of {"item_id": ..., "status": "DONE"|"FAILED", "frame_id": ...
        (optional, ANALYZE items only), "error": ... (optional)}.

    Returns
    -------
    dict | None
        {"results": [...], "task": {...}} (the task's updated counters), or
        None on any failure — a caller should treat that the same as "we
        don't know whether this was recorded" and is free to retry the same
        call later (already-resolved items are a no-op server-side, see
        docs/API.md).
    """
    if not items:
        return None

    logger.debug(
        "POST /tasks/%s/items/progress items=%d",
        task_id, len(items),
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _post_task_items_progress_with_retry(task_id, items)
    except _RETRYABLE as exc:
        logger.error(
            "All retries exhausted reporting item progress for task_id=%s: %s",
            task_id, exc,
            extra={"frame_id": None, "log_filename": None},
        )
    return None


# ---------------------------------------------------------------------------
# upload_task_item_chart — POST /tasks/{task_id}/items/{item_id}/chart
#
# The task_item_id-keyed counterpart of upload_source_chart(): backs
# pipeline.preview_catalog_match(), which has no source_id at all to key a
# chart on (a PREVIEW_CATALOG_MATCH item is a whole-frame diagnostic, not
# tied to any celestial object).
# ---------------------------------------------------------------------------

@_retry
async def _upload_task_item_chart_with_retry(
    task_id: str,
    item_id: str,
    png_bytes: bytes,
    style: str,
    frame_count: int,
) -> bool:
    """Inner retryable core for upload_task_item_chart."""
    url = f"/tasks/{task_id}/items/{item_id}/chart"

    async with _make_client() as client:
        # The request body IS the image — not JSON — same override as
        # upload_source_chart() needs, for the same reason. Content-Type is
        # sniffed the same way too, for consistency, though every caller of
        # this function today only ever sends PNG bytes.
        response = await client.post(
            f"/tasks/{task_id}/items/{item_id}/chart",
            params={"style": style, "frame_count": frame_count},
            content=png_bytes,
            headers={"Content-Type": _content_type_for_image_bytes(png_bytes)},
        )

        if 400 <= response.status_code < 500:
            logger.error(
                "API rejected POST %s with HTTP %d: %s",
                url, response.status_code, response.text,
                extra={"frame_id": None, "log_filename": None},
            )
            return False

        if response.status_code >= 500:
            response.raise_for_status()

    return True


async def upload_task_item_chart(
    task_id: str,
    item_id: str,
    png_bytes: bytes,
    style: str = "catalog_preview",
    frame_count: int = 1,
) -> bool:
    """
    Upload the diagnostic chart PNG for a PREVIEW_CATALOG_MATCH task item,
    replacing any previous one for that item.

    Parameters
    ----------
    task_id, item_id:
        Identify the task item this chart belongs to.
    png_bytes:
        Encoded PNG image bytes (the full request body — no JSON envelope).
    style:
        Always "catalog_preview" today; parameterized for symmetry with
        upload_source_chart() rather than hardcoded on the wire.
    frame_count:
        Always 1 (a single-frame chart, not an epoch series); same reasoning.

    Returns
    -------
    bool
        True on success, False on any failure (never raises — this is a
        best-effort feature and must not affect item processing; the caller
        still has a rendered result even if the upload itself failed).
    """
    logger.info(
        "Uploading %s chart for task_id=%s item_id=%s (%d bytes)",
        style, task_id, item_id, len(png_bytes),
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _upload_task_item_chart_with_retry(task_id, item_id, png_bytes, style, frame_count)
    except _RETRYABLE as exc:
        logger.error(
            "All retries exhausted uploading chart for task_id=%s item_id=%s: %s",
            task_id, item_id, exc,
            extra={"frame_id": None, "log_filename": None},
        )
    return False
