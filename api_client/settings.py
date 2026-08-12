"""
api_client/settings.py — remote pipeline configuration.

See docs/API.md section 16.
"""

from __future__ import annotations

import logging

import httpx

from ._shared import _make_client

logger = logging.getLogger(__name__)


async def get_settings() -> dict[str, str] | None:
    """
    Fetch pipeline configuration from ``GET /settings``.

    Returns
    -------
    dict[str, str] | None
        Flat ``{param_name: string_value}`` map on success, or ``None``
        when the API is unreachable or returns an error (any HTTP status
        other than 200, or a transport/timeout failure).  The caller
        should fall back to local defaults in that case.

    This function deliberately does **not** use the ``@_retry`` decorator —
    it is called once at startup, and retrying a failing settings fetch
    would delay the entire pipeline boot.  A single attempt with a short
    timeout is preferable: if the API is down, we just use local defaults.
    """
    try:
        async with _make_client() as client:
            resp = await client.get("/settings", timeout=10.0)

        if resp.status_code != 200:
            logger.warning(
                "GET /settings returned HTTP %d — using local defaults",
                resp.status_code,
            )
            return None

        body = resp.json()
        data = body.get("data") if isinstance(body, dict) else None
        if not isinstance(data, dict):
            logger.warning(
                "GET /settings response has unexpected shape — using local defaults",
            )
            return None

        logger.info("Fetched %d remote setting(s) from API", len(data))
        return data

    except (httpx.TransportError, httpx.TimeoutException) as exc:
        logger.warning(
            "GET /settings failed (%s) — using local defaults", exc,
        )
        return None
    except Exception as exc:
        logger.warning(
            "GET /settings unexpected error (%s) — using local defaults", exc,
        )
        return None
