"""
api_client/client.py — HTTP client for the observatory REST API.

Public functions:
  post_frame(frame_data)                                    → str (frame_id)
  post_sources(frame_id, filename, sources)                 → list[str | None] | None
  post_anomalies(frame_id, filename, anomalies)             → None
  get_sources_near(ra, dec, radius_arcsec, before_time)     → list
  get_frames_covering(ra, dec, before_time)                 → list
  get_nearest_frame_before(object_name, before_time)        → dict | None
  get_sources_near_batch(positions, radius_arcsec, before_time)  → dict
  get_frames_covering_batch(positions, before_time)         → dict
  get_source_track(source_id)                               → list
  get_source_tracks_batch(source_ids)                       → dict
  upload_source_chart(source_id, png_bytes, style, frame_count)  → bool
  upload_source_charts_batch(charts)                        → dict

  get_frames(object_name, date_from, date_to, limit, offset) → list
  get_frame(frame_id)                                        → dict | None
  get_frame_sources(frame_id)                                → list

  create_task(task_type, items, scope, parent_task_id)       → dict | None
  get_tasks(status, task_type, object_name, limit, order)    → list
  get_task(task_id)                                          → dict | None
  update_task(task_id, status, error)                        → dict | None
  post_task_items_progress(task_id, items)                   → dict | None
  upload_task_item_chart(task_id, item_id, png_bytes, style, frame_count)  → bool
"""

from __future__ import annotations

import base64
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
# ML-7-2: post_frame
#
# Allowed to propagate exceptions — the pipeline orchestrator must handle them.
# ---------------------------------------------------------------------------

@_retry
async def post_frame(frame_data: dict) -> str:
    """
    Register a new processed frame with the API.

    Parameters
    ----------
    frame_data:
        Full frame payload (POST /frames body as defined in CLAUDE.md).

    Returns
    -------
    str
        Frame ID assigned by the API.

    Raises
    ------
    RuntimeError
        On HTTP 4xx, or if the response body does not contain "id".
        (Tenacity retries on 5xx/transport errors, then re-raises.)
    """
    filename = frame_data.get("filename", "<unknown>")
    url = f"{config.API_BASE_URL}/frames"
    logger.info(
        "POST %s filename=%s",
        url,
        filename,
        extra={"frame_id": None, "log_filename": filename},
    )

    async with _make_client() as client:
        response = await client.post("/frames", json=frame_data)

        # 4xx — client error: log and raise immediately, no retry
        if 400 <= response.status_code < 500:
            logger.error(
                "API rejected POST /frames with HTTP %d: %s",
                response.status_code,
                response.text,
                extra={"frame_id": None, "log_filename": filename},
            )
            raise RuntimeError(
                f"API returned HTTP {response.status_code} for POST /frames"
            )

        # 5xx — raise HTTPStatusError so tenacity retries
        if response.status_code >= 500:
            response.raise_for_status()

        resp_json: dict = response.json()

    if "id" not in resp_json:
        raise RuntimeError("API did not return frame id")

    frame_id = str(resp_json["id"])
    logger.info(
        "Frame registered, frame_id=%s filename=%s",
        frame_id,
        filename,
        extra={"frame_id": frame_id, "log_filename": filename},
    )
    return frame_id


# ---------------------------------------------------------------------------
# ML-7-3: post_sources
# ---------------------------------------------------------------------------

def _to_wire_source(source: dict) -> dict:
    """
    Translate one pipeline-internal source dict into the shape
    POST /frames/{id}/sources documents.

    Two things happen here, not before:
      - Rename "_from_subtraction" (modules/subtraction.py's internal flag,
        also read by modules/anomaly_detector.py and pipeline.py's Step 4.5
        dedup) to the wire field name "from_subtraction" the API persists on
        source_observations — see that migration's docblock for why this is
        persisted at all: a standalone DETECT_ANOMALIES task re-run later,
        with no in-memory source list, needs it back from stored data.
      - Drop every other underscore-prefixed key ("_source_id",
        "_wcs_offset_ra", "_wcs_offset_dec", ...) — these are pipeline-only
        bookkeeping with no place in the API schema. Leading-underscore is
        this codebase's existing convention for "internal, not for the wire"
        (mirrors "_from_subtraction" and "_source_id" themselves), so this is
        one generic rule rather than a hardcoded list that would need
        updating every time a module grows a new internal field.

    Fields with no underscore prefix (mag_instrumental, calibrated, edge_flag,
    saturated, near_edge, etc.) still travel through unfiltered, exactly as
    before this function existed — the API already ignores keys it doesn't
    recognize, and tightening that further is out of scope here. "saturated"
    and "near_edge" are deliberately named without a leading underscore
    (unlike "_from_subtraction") for exactly this reason: both need to be
    persisted and later reconstructed by pipeline.py's own
    `_from_wire_source()` for a standalone DETECT_ANOMALIES re-run, same
    rationale as "_from_subtraction" above.

    "from_subtraction" is only added to the wire dict when the source
    actually carries "_from_subtraction" truthy — not unconditionally set to
    False for every source — so a source dict with no such key at all (the
    normal case for anything from astrometry.py, not subtraction.py) travels
    over the wire completely unchanged from before this function existed.
    The API defaults an omitted "from_subtraction" to false itself, so
    nothing is lost by not sending an explicit false.
    """
    wire = {k: v for k, v in source.items() if not k.startswith("_")}
    if source.get("_from_subtraction"):
        wire["from_subtraction"] = True
    return wire


@_retry
async def _post_sources_with_retry(
    frame_id: str,
    filename: str,
    sources: list,
) -> list | None:
    """Inner retryable core for post_sources."""
    url = f"{config.API_BASE_URL}/frames/{frame_id}/sources"
    logger.info(
        "POST %s count=%d",
        url,
        len(sources),
        extra={"frame_id": frame_id, "log_filename": filename},
    )

    wire_sources = [_to_wire_source(s) for s in sources]

    async with _make_client() as client:
        response = await client.post(
            f"/frames/{frame_id}/sources",
            json={"filename": filename, "sources": wire_sources},
        )

        if 400 <= response.status_code < 500:
            logger.error(
                "API rejected POST %s with HTTP %d: %s",
                url,
                response.status_code,
                response.text,
                extra={"frame_id": frame_id, "log_filename": filename},
            )
            return None  # 4xx: do not retry, just log

        if response.status_code >= 500:
            response.raise_for_status()  # triggers retry

        resp_json = response.json()

    # API.md documents "source_ids" as positionally parallel to the request's
    # "sources" array — see FramesController::saveSources. Missing/malformed
    # is treated as "the API didn't tell us" rather than an error: callers
    # must already tolerate None (e.g. an old API version predating this field).
    source_ids = resp_json.get("source_ids") if isinstance(resp_json, dict) else None
    return source_ids if isinstance(source_ids, list) else None


async def post_sources(frame_id: str, filename: str, sources: list) -> list | None:
    """
    POST detected sources for a processed frame.

    Parameters
    ----------
    frame_id:
        Frame ID returned by post_frame().
    filename:
        Original FITS filename — included in the request body for log correlation.
    sources:
        List of source dicts as defined in CLAUDE.md.  An empty list is valid.

    Returns
    -------
    list | None
        The API's `source_ids` array — positionally parallel to `sources`
        (same length/order), each entry the resolved `sources.id` or `None`
        for a skipped entry. Returns `None` (not a list of Nones) if the API
        call failed entirely or didn't return the field, so callers can tell
        "we don't know any source ids" apart from "every source was skipped".
    """
    logger.info(
        "Posting %d sources for frame_id=%s",
        len(sources),
        frame_id,
        extra={"frame_id": frame_id, "log_filename": filename},
    )
    try:
        return await _post_sources_with_retry(frame_id, filename, sources)
    except _RETRYABLE as exc:
        logger.error(
            "All retries exhausted posting sources for frame_id=%s: %s",
            frame_id,
            exc,
            extra={"frame_id": frame_id, "log_filename": filename},
        )
    return None


# ---------------------------------------------------------------------------
# ML-7-3: post_anomalies
# ---------------------------------------------------------------------------

@_retry
async def _post_anomalies_with_retry(
    frame_id: str,
    filename: str,
    anomalies: list,
) -> None:
    """Inner retryable core for post_anomalies."""
    url = f"{config.API_BASE_URL}/frames/{frame_id}/anomalies"
    logger.info(
        "POST %s count=%d",
        url,
        len(anomalies),
        extra={"frame_id": frame_id, "log_filename": filename},
    )

    async with _make_client() as client:
        response = await client.post(
            f"/frames/{frame_id}/anomalies",
            json={"filename": filename, "anomalies": anomalies},
        )

        if 400 <= response.status_code < 500:
            logger.error(
                "API rejected POST %s with HTTP %d: %s",
                url,
                response.status_code,
                response.text,
                extra={"frame_id": frame_id, "log_filename": filename},
            )
            return None

        if response.status_code >= 500:
            response.raise_for_status()

    return None


async def post_anomalies(frame_id: str, filename: str, anomalies: list) -> None:
    """
    POST detected anomalies for a processed frame.

    Parameters
    ----------
    frame_id:
        Frame ID returned by post_frame().
    filename:
        Original FITS filename — included in the request body for log correlation.
    anomalies:
        List of anomaly dicts as defined in CLAUDE.md.  An empty list is valid.
    """
    logger.info(
        "Posting %d anomalies for frame_id=%s",
        len(anomalies),
        frame_id,
        extra={"frame_id": frame_id, "log_filename": filename},
    )
    try:
        await _post_anomalies_with_retry(frame_id, filename, anomalies)
    except _RETRYABLE as exc:
        logger.error(
            "All retries exhausted posting anomalies for frame_id=%s: %s",
            frame_id,
            exc,
            extra={"frame_id": frame_id, "log_filename": filename},
        )
    return None


# ---------------------------------------------------------------------------
# ML-7-4: get_sources_near
# ---------------------------------------------------------------------------

@_retry
async def _get_sources_near_with_retry(
    ra: float,
    dec: float,
    radius_arcsec: float,
    before_time: str,
) -> list:
    """Inner retryable core for get_sources_near."""
    params = {
        "ra": ra,
        "dec": dec,
        "radius_arcsec": radius_arcsec,
        "before_time": before_time,
    }

    async with _make_client() as client:
        response = await client.get("/sources/near", params=params)

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    data = resp_json.get("data", resp_json) if isinstance(resp_json, dict) else resp_json
    return data if isinstance(data, list) else []


async def get_sources_near(
    ra: float,
    dec: float,
    radius_arcsec: float,
    before_time: str,
) -> list:
    """
    Retrieve historical sources near a sky position from the API.

    Parameters
    ----------
    ra, dec:
        Sky coordinates in decimal degrees.
    radius_arcsec:
        Cone radius in arcseconds.
    before_time:
        ISO 8601 timestamp — only return sources from frames before this time.

    Returns
    -------
    list
        List of source dicts, or [] on any failure.
    """
    logger.debug(
        "GET /sources/near ra=%.4f dec=%.4f radius=%.1f",
        ra,
        dec,
        radius_arcsec,
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _get_sources_near_with_retry(ra, dec, radius_arcsec, before_time)
    except Exception as exc:
        logger.error(
            "Error querying /sources/near ra=%.4f dec=%.4f: %s",
            ra,
            dec,
            exc,
            extra={"frame_id": None, "log_filename": None},
        )
        return []


# ---------------------------------------------------------------------------
# ML-7-4: get_frames_covering
# ---------------------------------------------------------------------------

@_retry
async def _get_frames_covering_with_retry(
    ra: float,
    dec: float,
    before_time: str,
) -> list:
    """Inner retryable core for get_frames_covering."""
    params = {
        "ra": ra,
        "dec": dec,
        "before_time": before_time,
    }

    async with _make_client() as client:
        response = await client.get("/frames/covering", params=params)

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    data = resp_json.get("data", resp_json) if isinstance(resp_json, dict) else resp_json
    return data if isinstance(data, list) else []


async def get_frames_covering(
    ra: float,
    dec: float,
    before_time: str,
) -> list:
    """
    Retrieve frames that covered a given sky position prior to a timestamp.

    Parameters
    ----------
    ra, dec:
        Sky coordinates in decimal degrees.
    before_time:
        ISO 8601 timestamp — only return frames observed before this time.

    Returns
    -------
    list
        List of frame dicts, or [] on any failure.
    """
    logger.debug(
        "GET /frames/covering ra=%.4f dec=%.4f",
        ra,
        dec,
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _get_frames_covering_with_retry(ra, dec, before_time)
    except Exception as exc:
        logger.error(
            "Error querying /frames/covering ra=%.4f dec=%.4f: %s",
            ra,
            dec,
            exc,
            extra={"frame_id": None, "log_filename": None},
        )
        return []


# ---------------------------------------------------------------------------
# get_nearest_frame_before — the single most recent frame of an object
# strictly before a given time. Used by modules/finder_chart.py's
# "before_after" chart style (see docs/API.md section 13) — distinct from
# get_frames_covering above, which is a spatial "was this position ever
# imaged" query, not a temporal "what's this object's previous frame" one.
# ---------------------------------------------------------------------------

@_retry
async def _get_nearest_frame_before_with_retry(object_name: str, before_time: str) -> dict | None:
    """Inner retryable core for get_nearest_frame_before."""
    params = {
        "object": object_name,
        "before_time": before_time,
    }

    async with _make_client() as client:
        response = await client.get("/frames/nearest-before", params=params)

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    return resp_json.get("frame") if isinstance(resp_json, dict) else None


async def get_nearest_frame_before(object_name: str, before_time: str) -> dict | None:
    """
    Retrieve the most recent frame of `object_name` strictly before `before_time`.

    Parameters
    ----------
    object_name:
        Normalized object/archive-directory name, exactly as stored on
        `frames.object`.
    before_time:
        ISO 8601 timestamp — only consider frames observed before this time.

    Returns
    -------
    dict | None
        `{"id", "filename", "object", "obs_time"}`, or `None` if no earlier
        frame exists, or on any failure (network error, retries exhausted,
        malformed response) — a caller can't tell those two cases apart from
        the return value alone, same as get_source_tracks_batch's per-source
        absence; modules/finder_chart.py treats both as "no before panel".
    """
    logger.debug(
        "GET /frames/nearest-before object=%s before_time=%s",
        object_name,
        before_time,
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _get_nearest_frame_before_with_retry(object_name, before_time)
    except Exception as exc:
        logger.error(
            "Error querying /frames/nearest-before object=%s: %s",
            object_name,
            exc,
            extra={"frame_id": None, "log_filename": None},
        )
        return None


# ---------------------------------------------------------------------------
# ML-7-5: get_sources_near_batch (BATCH)
# ---------------------------------------------------------------------------

@_retry
async def _get_sources_near_batch_with_retry(
    positions: list[dict],
    radius_arcsec: float,
    before_time: str,
) -> dict:
    """Inner retryable core for get_sources_near_batch."""
    payload = {
        "positions": positions,
        "radius_arcsec": radius_arcsec,
        "before_time": before_time,
    }

    async with _make_client() as client:
        response = await client.post("/sources/near/batch", json=payload)

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    # Documented format: {"results": {"0": [...], "1": [...], ...}}
    # Also accepted: {"results": [[...], [...], ...]} — see _normalize_batch_results.
    return _normalize_batch_results(resp_json)


async def get_sources_near_batch(
    positions: list[dict],
    radius_arcsec: float,
    before_time: str,
) -> dict:
    """
    Retrieve historical sources near multiple sky positions from the API
    in a single batch request.

    Parameters
    ----------
    positions:
        List of position dicts, each with "ra" and "dec" keys in decimal degrees.
        Example: [{"ra": 123.45, "dec": 67.89}, {"ra": 124.00, "dec": 68.00}]
    radius_arcsec:
        Cone radius in arcseconds (same for all positions).
    before_time:
        ISO 8601 timestamp — only return sources from frames before this time.

    Returns
    -------
    dict
        Dictionary mapping position index (as string) to list of source dicts.
        Example: {"0": [source1, source2], "1": [], "2": [source3]}
        Returns {} on any failure.
    """
    if not positions:
        return {}

    logger.debug(
        "POST /sources/near/batch positions=%d radius=%.1f",
        len(positions),
        radius_arcsec,
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _get_sources_near_batch_with_retry(positions, radius_arcsec, before_time)
    except Exception as exc:
        logger.error(
            "Error querying /sources/near/batch: %s",
            exc,
            extra={"frame_id": None, "log_filename": None},
        )
        return {}


# ---------------------------------------------------------------------------
# ML-7-6: get_frames_covering_batch (BATCH)
# ---------------------------------------------------------------------------

@_retry
async def _get_frames_covering_batch_with_retry(
    positions: list[dict],
    before_time: str,
) -> dict:
    """Inner retryable core for get_frames_covering_batch."""
    payload = {
        "positions": positions,
        "before_time": before_time,
    }

    async with _make_client() as client:
        response = await client.post("/frames/covering/batch", json=payload)

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    # Documented format: {"results": {"0": [...], "1": [...], ...}}
    # Also accepted: {"results": [[...], [...], ...]} — see _normalize_batch_results.
    return _normalize_batch_results(resp_json)


async def get_frames_covering_batch(
    positions: list[dict],
    before_time: str,
) -> dict:
    """
    Retrieve frames covering multiple sky positions from the API
    in a single batch request.

    Parameters
    ----------
    positions:
        List of position dicts, each with "ra" and "dec" keys in decimal degrees.
        Example: [{"ra": 123.45, "dec": 67.89}, {"ra": 124.00, "dec": 68.00}]
    before_time:
        ISO 8601 timestamp — only return frames observed before this time.

    Returns
    -------
    dict
        Dictionary mapping position index (as string) to list of frame dicts.
        Example: {"0": [frame1, frame2], "1": [], "2": [frame3]}
        Returns {} on any failure.
    """
    if not positions:
        return {}

    logger.debug(
        "POST /frames/covering/batch positions=%d",
        len(positions),
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _get_frames_covering_batch_with_retry(positions, before_time)
    except Exception as exc:
        logger.error(
            "Error querying /frames/covering/batch: %s",
            exc,
            extra={"frame_id": None, "log_filename": None},
        )
        return {}


# ---------------------------------------------------------------------------
# get_source_track
# ---------------------------------------------------------------------------

@_retry
async def _get_source_track_with_retry(source_id: str) -> list:
    """Inner retryable core for get_source_track."""
    async with _make_client() as client:
        response = await client.get(f"/sources/{source_id}/track")

        if response.status_code == 404:
            return []

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    data = resp_json.get("epochs") if isinstance(resp_json, dict) else None
    return data if isinstance(data, list) else []


async def get_source_track(source_id: str) -> list:
    """
    Retrieve the per-epoch position track for a source: every frame it was
    observed on, chronologically, with the (ra, dec) it was actually
    detected at on that specific frame plus enough frame metadata
    (filename, object) to locate the FITS file in the local archive.

    Used by modules/finder_chart.py to build the per-source finder chart.

    Parameters
    ----------
    source_id:
        The `sources.id` this anomaly was attached to (from `_source_id` —
        see pipeline.py Step 7).

    Returns
    -------
    list
        List of epoch dicts (frame_id, filename, object, obs_time, ra, dec,
        mag), chronologically ordered. [] on any failure or if the source
        has no observations.
    """
    logger.debug(
        "GET /sources/%s/track",
        source_id,
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _get_source_track_with_retry(source_id)
    except Exception as exc:
        logger.error(
            "Error querying /sources/%s/track: %s",
            source_id,
            exc,
            extra={"frame_id": None, "log_filename": None},
        )
        return []


# ---------------------------------------------------------------------------
# get_source_tracks_batch (BATCH)
# ---------------------------------------------------------------------------

@_retry
async def _get_source_tracks_batch_with_retry(source_ids: list[str]) -> dict:
    """Inner retryable core for get_source_tracks_batch."""
    async with _make_client() as client:
        response = await client.post("/sources/tracks/batch", json={"source_ids": source_ids})

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    # Documented format: {"results": {"<source_id>": [epoch, ...], ...}}
    # Also accepted: {"results": [[...], [...], ...]} — see _normalize_batch_results.
    return _normalize_batch_results(resp_json)


async def get_source_tracks_batch(source_ids: list[str]) -> dict:
    """
    Retrieve the per-epoch position track for MULTIPLE sources in a single
    batch request — the batch counterpart of get_source_track(), used by
    modules/finder_chart.py so that a frame with several anomalies fetches
    every one of their tracks in one round trip instead of one GET per
    source_id.

    Parameters
    ----------
    source_ids:
        List of `sources.id` values.

    Returns
    -------
    dict
        Dictionary mapping source_id -> list of epoch dicts (chronologically
        ordered, same shape as get_source_track()'s return value). A
        source_id missing from the response (unknown to the API, or the API
        predates this endpoint) is absent from the dict — callers should
        treat that the same as an empty track. Returns {} on any failure.
    """
    if not source_ids:
        return {}

    logger.debug(
        "POST /sources/tracks/batch source_ids=%d",
        len(source_ids),
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _get_source_tracks_batch_with_retry(source_ids)
    except Exception as exc:
        logger.error(
            "Error querying /sources/tracks/batch: %s",
            exc,
            extra={"frame_id": None, "log_filename": None},
        )
        return {}


# ---------------------------------------------------------------------------
# upload_source_chart
# ---------------------------------------------------------------------------

@_retry
async def _upload_source_chart_with_retry(
    source_id: str,
    png_bytes: bytes,
    style: str,
    frame_count: int,
) -> bool:
    """Inner retryable core for upload_source_chart."""
    url = f"{config.API_BASE_URL}/sources/{source_id}/chart"

    async with _make_client() as client:
        # The request body IS the image — not JSON — so the client's default
        # "Content-Type: application/json" header (set in _make_client) must
        # be overridden here. httpx merges per-request headers on top of the
        # client's default headers, with the per-request value winning on a
        # matching key, so this override is safe and local to this one call.
        response = await client.post(
            f"/sources/{source_id}/chart",
            params={"style": style, "frame_count": frame_count},
            content=png_bytes,
            headers={"Content-Type": "image/png"},
        )

        if 400 <= response.status_code < 500:
            logger.error(
                "API rejected POST %s with HTTP %d: %s",
                url,
                response.status_code,
                response.text,
                extra={"frame_id": None, "log_filename": None},
            )
            return False

        if response.status_code >= 500:
            response.raise_for_status()

    return True


async def upload_source_chart(
    source_id: str,
    png_bytes: bytes,
    style: str,
    frame_count: int,
) -> bool:
    """
    Upload the finder-chart PNG for a source, replacing any previous one.

    Parameters
    ----------
    source_id:
        The `sources.id` this chart is for.
    png_bytes:
        Encoded PNG image bytes (the full request body — no JSON envelope).
    style:
        "track" (moving objects) or "stamp_strip" (stationary anomalies).
    frame_count:
        Number of epochs actually included in the image (may be less than
        the full track if some archived FITS files were missing locally).

    Returns
    -------
    bool
        True on success, False on any failure (never raises — this is a
        best-effort feature and must not affect frame processing).
    """
    logger.info(
        "Uploading %s chart for source_id=%s (%d epochs, %d bytes)",
        style,
        source_id,
        frame_count,
        len(png_bytes),
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _upload_source_chart_with_retry(source_id, png_bytes, style, frame_count)
    except _RETRYABLE as exc:
        logger.error(
            "All retries exhausted uploading chart for source_id=%s: %s",
            source_id,
            exc,
            extra={"frame_id": None, "log_filename": None},
        )
    return False


# ---------------------------------------------------------------------------
# upload_source_charts_batch (BATCH)
# ---------------------------------------------------------------------------

@_retry
async def _upload_source_charts_batch_with_retry(charts: list[dict]) -> dict:
    """Inner retryable core for upload_source_charts_batch."""
    payload = {
        "charts": [
            {
                "source_id": chart["source_id"],
                "style": chart["style"],
                "frame_count": chart["frame_count"],
                # Unlike the single-chart endpoint (raw PNG body), the batch
                # endpoint's request is one JSON envelope for every chart, so
                # each PNG travels base64-encoded inside it.
                "png_base64": base64.b64encode(chart["png_bytes"]).decode("ascii"),
            }
            for chart in charts
        ],
    }

    async with _make_client() as client:
        response = await client.post("/sources/charts/batch", json=payload)

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    # Documented format: {"results": [{"source_id":..., "status": "ok"|"error", ...}, ...]},
    # positionally parallel to the request's "charts" array.
    results = resp_json.get("results") if isinstance(resp_json, dict) else None
    if not isinstance(results, list):
        return {}

    outcomes: dict = {}
    for chart, result in zip(charts, results):
        source_id = chart["source_id"]
        ok = isinstance(result, dict) and result.get("status") == "ok"
        if not ok:
            logger.error(
                "API rejected chart for source_id=%s: %s",
                source_id,
                result.get("error") if isinstance(result, dict) else result,
                extra={"frame_id": None, "log_filename": None},
            )
        outcomes[source_id] = ok
    return outcomes


async def upload_source_charts_batch(charts: list[dict]) -> dict:
    """
    Upload finder-chart PNGs for MULTIPLE sources in a single batch request
    — the batch counterpart of upload_source_chart(), used by
    modules/finder_chart.py so that a frame with several anomalies uploads
    every one of their charts in one round trip instead of one POST per
    source_id.

    Parameters
    ----------
    charts:
        List of dicts, each with keys "source_id", "png_bytes", "style",
        "frame_count" — the same values upload_source_chart() takes
        individually.

    Returns
    -------
    dict
        Dictionary mapping source_id -> bool (True on successful upload).
        A source_id missing from the dict means the batch call failed
        entirely (network error, 4xx/5xx on the whole request) — as
        opposed to `False`, which means the API processed the batch but
        rejected that one entry. Callers should treat both as "not
        uploaded". Returns {} if `charts` is empty or the whole call failed.
    """
    if not charts:
        return {}

    logger.info(
        "Uploading %d chart(s) in one batch request",
        len(charts),
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _upload_source_charts_batch_with_retry(charts)
    except _RETRYABLE as exc:
        logger.error(
            "All retries exhausted uploading chart batch (%d charts): %s",
            len(charts),
            exc,
            extra={"frame_id": None, "log_filename": None},
        )
    return {}


# ---------------------------------------------------------------------------
# get_frames — GET /frames (list, filtered by object/date range)
#
# The scope-resolution query behind a standalone DETECT_ANOMALIES/
# GENERATE_CHARTS task: turns "object=M51" (optionally + a date range) into a
# concrete list of frame ids covering that object's entire observation
# history, old and new frames alike — not just frames from one pipeline run.
# ---------------------------------------------------------------------------

@_retry
async def _get_frames_with_retry(
    object_name: str | None,
    date_from: str | None,
    date_to: str | None,
    limit: int,
    offset: int,
) -> list:
    """Inner retryable core for get_frames."""
    params: dict = {"limit": limit, "offset": offset}
    if object_name is not None:
        params["object"] = object_name
    if date_from is not None:
        params["date_from"] = date_from
    if date_to is not None:
        params["date_to"] = date_to

    async with _make_client() as client:
        response = await client.get("/frames", params=params)

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    data = resp_json.get("data") if isinstance(resp_json, dict) else None
    return data if isinstance(data, list) else []


async def get_frames(
    object_name: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list:
    """
    List previously registered frames, optionally filtered by object and/or
    an obs_time range.

    Parameters
    ----------
    object_name:
        Exact match on the normalized object/archive-directory name, or None
        for no object filter.
    date_from, date_to:
        ISO 8601 timestamps bounding obs_time (date_from inclusive, date_to
        exclusive), or None for no bound on that side.
    limit, offset:
        Pagination — the API caps limit at 1000 regardless of what's passed.

    Returns
    -------
    list
        List of frame dicts (same shape as get_frame()'s single-frame
        return value). [] on any failure or if nothing matches.
    """
    logger.debug(
        "GET /frames object=%s date_from=%s date_to=%s",
        object_name, date_from, date_to,
        extra={"frame_id": None, "log_filename": None},
    )
    try:
        return await _get_frames_with_retry(object_name, date_from, date_to, limit, offset)
    except Exception as exc:
        logger.error(
            "Error querying /frames object=%s: %s",
            object_name, exc,
            extra={"frame_id": None, "log_filename": None},
        )
        return []


# ---------------------------------------------------------------------------
# get_frame — GET /frames/{id}
#
# A single frame's full stored record, echoed back flat. Lets a standalone
# task reconstruct the frame_meta anomaly_detector.py needs without any
# local filesystem access to the original FITS file at all.
# ---------------------------------------------------------------------------

@_retry
async def _get_frame_with_retry(frame_id: str) -> dict | None:
    """Inner retryable core for get_frame."""
    async with _make_client() as client:
        response = await client.get(f"/frames/{frame_id}")

        if response.status_code == 404:
            return None

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    return resp_json.get("frame") if isinstance(resp_json, dict) else None


async def get_frame(frame_id: str) -> dict | None:
    """
    Retrieve a single previously registered frame's full stored record.

    Parameters
    ----------
    frame_id:
        Frame ID returned by post_frame() (or from a DETECT_ANOMALIES task
        item's "frame_id").

    Returns
    -------
    dict | None
        The frame's fields (filename, obs_time, ra_center, dec_center,
        fov_deg, object, filter, ...), or None if not found or on any
        failure — a caller can't tell those two cases apart from the return
        value alone (same convention as get_nearest_frame_before()).
    """
    logger.debug(
        "GET /frames/%s",
        frame_id,
        extra={"frame_id": frame_id, "log_filename": None},
    )
    try:
        return await _get_frame_with_retry(frame_id)
    except Exception as exc:
        logger.error(
            "Error querying /frames/%s: %s",
            frame_id, exc,
            extra={"frame_id": frame_id, "log_filename": None},
        )
        return None


# ---------------------------------------------------------------------------
# get_frame_sources — GET /frames/{id}/sources
#
# The piece a standalone DETECT_ANOMALIES task needs to reconstruct
# anomaly_detector.py's per-source input for an already-processed frame
# entirely from stored data — no re-running astrometry/photometry, no local
# FITS access required.
# ---------------------------------------------------------------------------

@_retry
async def _get_frame_sources_with_retry(frame_id: str) -> list:
    """Inner retryable core for get_frame_sources."""
    async with _make_client() as client:
        response = await client.get(f"/frames/{frame_id}/sources")

        if response.status_code == 404:
            return []

        if response.status_code >= 500:
            response.raise_for_status()

        resp_json = response.json()

    data = resp_json.get("data") if isinstance(resp_json, dict) else None
    return data if isinstance(data, list) else []


async def get_frame_sources(frame_id: str) -> list:
    """
    Retrieve the sources currently linked to a frame, each with that frame's
    own measured values (ra, dec, mag, elongation, saturated,
    from_subtraction, ...) plus catalog identity fields.

    Parameters
    ----------
    frame_id:
        Frame ID returned by post_frame().

    Returns
    -------
    list
        List of source dicts in the API's wire shape (see
        docs/API.md's GET /frames/{id}/sources) — NOT yet translated into
        the internal shape anomaly_detector.py expects (leading-underscore
        keys etc.); see pipeline.py's detect_anomalies_for_frame_id() for
        that translation. [] on any failure or if the frame has no sources.
    """
    logger.debug(
        "GET /frames/%s/sources",
        frame_id,
        extra={"frame_id": frame_id, "log_filename": None},
    )
    try:
        return await _get_frame_sources_with_retry(frame_id)
    except Exception as exc:
        logger.error(
            "Error querying /frames/%s/sources: %s",
            frame_id, exc,
            extra={"frame_id": frame_id, "log_filename": None},
        )
        return []


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
    items: list[dict],
    scope: dict | None = None,
    parent_task_id: str | None = None,
) -> dict | None:
    """
    Create a task with its full, fixed item list.

    Parameters
    ----------
    task_type:
        "ANALYZE", "DETECT_ANOMALIES", or "GENERATE_CHARTS".
    items:
        List of item dicts, each with exactly one of "filename" (ANALYZE),
        "frame_id" (DETECT_ANOMALIES), or "source_id" (GENERATE_CHARTS) —
        plus an optional "payload" (arbitrary JSON-encodable value, e.g. a
        GENERATE_CHARTS item's {"anomaly_type": ..., "designation": ...}).
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
    url = f"{config.API_BASE_URL}/tasks/{task_id}/items/{item_id}/chart"

    async with _make_client() as client:
        # The request body IS the image — not JSON — same override as
        # upload_source_chart() needs, for the same reason.
        response = await client.post(
            f"/tasks/{task_id}/items/{item_id}/chart",
            params={"style": style, "frame_count": frame_count},
            content=png_bytes,
            headers={"Content-Type": "image/png"},
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

