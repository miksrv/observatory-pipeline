"""
api_client/sources.py — the "sources" resource: saving detected
sources for a frame, historical near-position queries, position tracks, and
finder-chart uploads.

See docs/API.md sections 2, 4, 6, 8, 9, 11.
"""

from __future__ import annotations

import logging

import config
from ._shared import (
    _content_type_for_image_bytes,
    _make_client,
    _normalize_batch_results,
    _retry,
    _RETRYABLE,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ML-7-3: post_sources
# ---------------------------------------------------------------------------

def _to_wire_source(source: dict) -> dict:
    """
    Translate one pipeline-internal source dict into the shape
    POST /frames/{id}/sources documents.

    Two things happen here, not before:
      - Rename "_from_subtraction" (modules/subtraction.py's internal flag,
        also read by modules/anomaly_detector/ and pipeline.py's Step 4.5
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
            headers={"Content-Type": _content_type_for_image_bytes(png_bytes)},
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
    Upload the finder-chart image for a source, replacing any previous chart
    of the same `style` for that source.

    Parameters
    ----------
    source_id:
        The `sources.id` this chart is for.
    png_bytes:
        The full request body — no JSON envelope. Despite the name, this can
        be either encoded PNG bytes (styles "track", "stamp_strip",
        "before_after") or an animated GIF (styles "track_gif",
        "stamp_strip_gif" — see modules/finder_chart.py's _pngs_to_gif()).
        The Content-Type header is set from the bytes' own magic number, not
        from `style` — see _content_type_for_image_bytes() above.
    style:
        "track" / "stamp_strip" / "before_after", or their animated "_gif"
        counterparts (see modules/finder_chart.py).
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
