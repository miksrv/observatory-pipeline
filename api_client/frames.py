"""
api_client/frames.py — the "frames" resource: registering a frame,
listing/reading frames back, and the coverage/nearest-before spatial and
temporal queries scoped to frames rather than sources.

See docs/API.md sections 1, 5, 7, 12, 13.
"""

from __future__ import annotations

import logging

import config
from ._shared import _make_client, _normalize_batch_results, _retry

logger = logging.getLogger(__name__)


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
