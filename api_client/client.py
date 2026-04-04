"""
api_client/client.py — HTTP client for the observatory REST API.

Public functions:
  post_frame(frame_data)                                    → str (frame_id)
  post_sources(frame_id, filename, sources)                 → None
  post_anomalies(frame_id, filename, anomalies)             → None
  get_sources_near(ra, dec, radius_arcsec, before_time)     → list
  get_frames_covering(ra, dec, before_time)                 → list
  get_sources_near_batch(positions, radius_arcsec, before_time)  → dict
  get_frames_covering_batch(positions, before_time)         → dict
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

@_retry
async def _post_sources_with_retry(
    frame_id: str,
    filename: str,
    sources: list,
) -> None:
    """Inner retryable core for post_sources."""
    url = f"{config.API_BASE_URL}/frames/{frame_id}/sources"
    logger.info(
        "POST %s count=%d",
        url,
        len(sources),
        extra={"frame_id": frame_id, "log_filename": filename},
    )

    async with _make_client() as client:
        response = await client.post(
            f"/frames/{frame_id}/sources",
            json={"filename": filename, "sources": sources},
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

    return None


async def post_sources(frame_id: str, filename: str, sources: list) -> None:
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
    """
    logger.info(
        "Posting %d sources for frame_id=%s",
        len(sources),
        frame_id,
        extra={"frame_id": frame_id, "log_filename": filename},
    )
    try:
        await _post_sources_with_retry(frame_id, filename, sources)
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

    # Expected format: {"results": {"0": [...], "1": [...], ...}}
    # where keys are string indices matching input positions array
    data = resp_json.get("results", resp_json) if isinstance(resp_json, dict) else {}
    return data if isinstance(data, dict) else {}


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

    # Expected format: {"results": {"0": [...], "1": [...], ...}}
    data = resp_json.get("results", resp_json) if isinstance(resp_json, dict) else {}
    return data if isinstance(data, dict) else {}


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

