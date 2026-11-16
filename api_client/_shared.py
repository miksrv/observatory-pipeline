"""
api_client/_shared.py — internal HTTP infrastructure shared by every
resource submodule in this package (frames.py, sources.py, anomalies.py,
tasks.py, settings.py).

Leading underscore, same convention as the rest of this codebase ("internal,
not for the wire" / "internal, not part of the public surface") — nothing
here is meant to be imported from outside the api_client package.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import time
from collections.abc import Callable

import httpx
import tenacity

import config
import version

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
#
# Three quick attempts first. Once they are spent, a process that called
# wait_out_api_outages() (the worker) asks whether the API answers at all
# (_api_answers()); if it does not, the call waits for it — see
# _wait_out_outage().
# ---------------------------------------------------------------------------

_quick_retry = tenacity.retry(
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


class ApiWaitInterrupted(BaseException):
    """
    A shutdown was requested while a call waited for the API to come back.

    A BaseException, like asyncio.CancelledError, on purpose: the pipeline
    catches `Exception` around nearly every API call to keep a frame going
    with partial results, and this must pass through all of them — the work
    in hand is neither done nor failed, it is left for the next start.
    """


# Set by wait_out_api_outages(): None means "do not wait" (the default, and
# the watcher's mode), else asked between waits whether to give up.
_should_stop: Callable[[], bool] | None = None

# An outage still going on after this long is logged at ERROR once.
_OUTAGE_ALERT_AFTER_SEC = 3600.0


def wait_out_api_outages(should_stop: Callable[[], bool] | None) -> None:
    """
    Make every retrying API call of this process wait out an API outage
    (`should_stop` says when to give up — a requested shutdown), or with None
    stop waiting. Only the worker turns it on: it has the work in hand, and
    holding it is better than reporting it done or lost.
    """
    global _should_stop
    _should_stop = should_stop


async def _api_answers() -> bool:
    """
    Whether the API answers at all: GET /settings — authenticated, reads the
    database, cheap. Any answer below 500 counts, a 401 too (that is not an
    outage, and waiting would not cure it).
    """
    try:
        async with _make_client(timeout=15.0) as client:
            response = await client.get("/settings")
    except Exception:
        return False
    return response.status_code < 500


async def _sleep_unless_stopped(seconds: float) -> None:
    """Sleep, checking every second for a shutdown (then ApiWaitInterrupted)."""
    deadline = time.monotonic() + seconds
    while True:
        if _should_stop is not None and _should_stop():
            raise ApiWaitInterrupted()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        await asyncio.sleep(min(1.0, remaining))


def _describe(exc: BaseException) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code}"
    text = str(exc).strip()
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


async def _wait_out_outage(what: str, exc: BaseException) -> bool:
    """
    After the quick retries of `what` failed with `exc`: True once the API
    answers again (retry the call), False when there is nothing to wait for —
    waiting is off, or the API answers and it is this request that fails (a
    PHP error on one payload, a query over the API's own time limit), so it
    fails as before. Raises ApiWaitInterrupted on a shutdown.
    """
    if _should_stop is None or config.API_OUTAGE_RETRY_SEC <= 0:
        return False
    if _should_stop():
        raise ApiWaitInterrupted()
    if await _api_answers():
        return False

    interval = config.API_OUTAGE_RETRY_SEC
    started = time.monotonic()
    alerted = False
    logger.warning(
        "API unavailable (%s) — %s waits for it, retrying every %.0f s",
        _describe(exc), what, interval,
    )
    while True:
        await _sleep_unless_stopped(interval)
        waited_min = (time.monotonic() - started) / 60.0
        if await _api_answers():
            logger.info("API answers again after %.0f min — retrying %s", waited_min, what)
            return True
        if not alerted and waited_min * 60.0 >= _OUTAGE_ALERT_AFTER_SEC:
            alerted = True
            logger.error("API still unavailable after %.0f min — %s keeps waiting", waited_min, what)
        else:
            logger.warning("API still unavailable after %.0f min — next try in %.0f s", waited_min, interval)


def _retry(fn: Callable) -> Callable:
    """
    The decorator every retrying API call uses: three quick attempts
    (_quick_retry), then — in a process that waits out outages — as many
    more as it takes while the API does not answer at all.
    """
    quick = _quick_retry(fn)
    what = fn.__name__.removeprefix("_").removesuffix("_with_retry")

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        while True:
            try:
                return await quick(*args, **kwargs)
            except _RETRYABLE as exc:
                if not await _wait_out_outage(what, exc):
                    raise

    return wrapper


def _normalize_batch_results(resp_json: object, keys: list[str] | None = None) -> dict:
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

    `keys` is what the array form is re-keyed by. The batch endpoints do not
    agree on what a result is addressed by: /sources/near/batch and
    /frames/covering/batch are documented as keyed by the request position
    (modules/anomaly_detector/_prefetch.py reads them back as `str(i)`), while
    /sources/tracks/batch is keyed by `source_id` (modules/finder_chart reads
    it back as `tracks.get(source_id)`). Indexing the array form positionally
    is right for the first two and silently wrong for the third — every track
    would land under "0"/"1" and every chart would be built from no epochs at
    all. A caller whose endpoint is keyed by id passes the request's own id
    list here; one whose endpoint is keyed by position passes nothing.
    """
    if not isinstance(resp_json, dict):
        return {}

    data = resp_json.get("results", resp_json)

    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        if keys is not None:
            # A short/long response is the API disagreeing with its own
            # contract; keep whatever pairs up rather than guessing at the
            # rest, so a caller sees missing entries and not misattributed
            # ones.
            return {key: value for key, value in zip(keys, data)}
        return {str(i): v for i, v in enumerate(data)}
    return {}


def _make_client(timeout: float = 30.0) -> httpx.AsyncClient:
    """Return a configured AsyncClient for the observatory API."""
    headers = {
        "X-API-Key": config.API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if version.VERSION is not None:
        # Recorded with the heartbeat and shown on the API's dashboard.
        headers["X-Pipeline-Version"] = version.VERSION
    return httpx.AsyncClient(base_url=config.API_BASE_URL, headers=headers, timeout=timeout)


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
