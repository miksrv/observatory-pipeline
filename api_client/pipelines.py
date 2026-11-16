"""
api_client/pipelines.py — what this pipeline reports about itself.

See docs/API.md section 17.
"""

from __future__ import annotations

import logging

from ._shared import _make_client

logger = logging.getLogger(__name__)


# post_catalog_status()'s default: the report carries no `waiting` key at all.
_KEEP = object()


async def post_catalog_status(catalogs: list[dict], waiting: dict | None | object = _KEEP) -> bool:
    """
    Report the online catalog check (`modules.catalog_health.check_all()`) with
    ``POST /pipelines/catalogs``. Returns True on 200.

    `waiting` says the worker holds an ANALYZE item until catalogs answer
    again (`worker._wait_for_catalogs()`): ``{"reason": "catalogs",
    "catalogs": [...], "since": ISO time, "task_id", "filename"}``, or None
    when the wait ends. Left out (the default — the periodic check), the key
    is not sent and the API keeps whatever state it has: only the wait sets
    and clears it, so a periodic report racing a wait can never erase it. An
    API older than 1.6.1 ignores the field.

    One attempt, no ``@_retry``: the next check follows in
    CATALOG_HEALTH_INTERVAL_SEC anyway, and a status report must never hold
    anything up. Every failure is logged and returns False (an API without the
    endpoint — not yet upgraded — answers 404; one not yet migrated, 503).
    """
    body: dict = {"catalogs": catalogs}
    if waiting is not _KEEP:
        body["waiting"] = waiting
    try:
        async with _make_client(timeout=15.0) as client:
            resp = await client.post("/pipelines/catalogs", json=body)
    except Exception as exc:
        logger.warning("POST /pipelines/catalogs failed: %s", exc)
        return False

    if resp.status_code != 200:
        logger.warning("POST /pipelines/catalogs returned HTTP %d", resp.status_code)
        return False
    return True
