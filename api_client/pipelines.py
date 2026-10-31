"""
api_client/pipelines.py — what this pipeline reports about itself.

See docs/API.md section 17.
"""

from __future__ import annotations

import logging

from ._shared import _make_client

logger = logging.getLogger(__name__)


async def post_catalog_status(catalogs: list[dict]) -> bool:
    """
    Report the online catalog check (`modules.catalog_health.check_all()`) with
    ``POST /pipelines/catalogs``. Returns True on 200.

    One attempt, no ``@_retry``: the next check follows in
    CATALOG_HEALTH_INTERVAL_SEC anyway, and a status report must never hold
    anything up. Every failure is logged and returns False (an API without the
    endpoint — not yet upgraded — answers 404; one not yet migrated, 503).
    """
    try:
        async with _make_client(timeout=15.0) as client:
            resp = await client.post("/pipelines/catalogs", json={"catalogs": catalogs})
    except Exception as exc:
        logger.warning("POST /pipelines/catalogs failed: %s", exc)
        return False

    if resp.status_code != 200:
        logger.warning("POST /pipelines/catalogs returned HTTP %d", resp.status_code)
        return False
    return True
