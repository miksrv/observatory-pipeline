"""
api_client/_shared.py — internal HTTP infrastructure shared by every
resource submodule in this package (frames.py, sources.py, anomalies.py,
tasks.py, settings.py).

Leading underscore, same convention as the rest of this codebase ("internal,
not for the wire" / "internal, not part of the public surface") — nothing
here is meant to be imported from outside the api_client package.
"""

from __future__ import annotations

import logging

import httpx
import tenacity

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared retry decorator.
#
# Retries on:
#   - httpx.TransportError   (connection refused, DNS failure, etc.)
#   - httpx.TimeoutException (read/connect timeout)
#   - httpx.HTTPStatusError  (raised explicitly for HTTP 5xx inside each fn)
#
# HTTP 4xx errors are handled inline and never raise HTTPStatusError, so they
# are NOT retried.
# ---------------------------------------------------------------------------

_retry = tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=10),
    retry=tenacity.retry_if_exception_type(
        (httpx.TransportError, httpx.TimeoutException, httpx.HTTPStatusError)
    ),
    reraise=True,
    before_sleep=tenacity.before_sleep_log(logger, logging.WARNING),
)

# Types that tenacity will retry — used in outer-wrapper catches.
_RETRYABLE = (httpx.TransportError, httpx.TimeoutException, httpx.HTTPStatusError)


def _normalize_batch_results(resp_json: object) -> dict:
    """
    Normalize the "results" field of a batch-endpoint response to a
    dict mapping position index (as string) -> list of result dicts.

    API.md documents "results" as a JSON *object* keyed by string index
    (e.g. {"0": [...], "1": [...]}), but the observatory-api's actual
    responses have been observed to serialize it as a plain JSON *array*
    instead (e.g. [[...], [...]] — PHP's json_encode() does this for any
    array with sequential integer keys, which is exactly what a
    foreach-built results array normally has). Silently coercing a
    non-dict "results" to {} — the previous behaviour — discarded every
    batch result on every call, making anomaly_detector.py permanently
    blind to history/coverage regardless of how much data the API
    actually holds. Accept both shapes here so a fix on either side of
    the API contract keeps working.
    """
    if not isinstance(resp_json, dict):
        return {}

    data = resp_json.get("results", resp_json)

    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        return {str(i): v for i, v in enumerate(data)}
    return {}


def _make_client() -> httpx.AsyncClient:
    """Return a configured AsyncClient for the observatory API."""
    return httpx.AsyncClient(
        base_url=config.API_BASE_URL,
        headers={
            "X-API-Key": config.API_KEY,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        timeout=30.0,
    )


# ---------------------------------------------------------------------------
# Chart image Content-Type sniffing
#
# Every chart upload endpoint (upload_source_chart, upload_task_item_chart)
# used to hardcode "Content-Type: image/png" unconditionally — true for
# every chart modules/finder_chart.py produced until it grew a "_gif"-suffixed
# style (see that module's _pngs_to_gif()). Sniffing the image's own magic
# bytes, rather than switching on the `style` string, keeps this decoupled
# from any particular style-naming convention and matches observatory-api's
# own upload validation (SourcesController::uploadChart(), which checks the
# same PNG signature rather than trusting a client-supplied header).
# ---------------------------------------------------------------------------

_GIF_SIGNATURES = (b"GIF87a", b"GIF89a")


def _content_type_for_image_bytes(image_bytes: bytes) -> str:
    if image_bytes[:6] in _GIF_SIGNATURES:
        return "image/gif"
    return "image/png"
