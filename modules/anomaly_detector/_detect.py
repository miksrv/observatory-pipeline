"""
modules/anomaly_detector/_detect.py — the package's public entry point,
orchestrating prefetch → classify → ephemeris resolution for one frame's
worth of sources.
"""

from __future__ import annotations

import logging

from ._classify import _classify_source_sync
from ._ephemeris_resolution import _resolve_ephemerides
from ._prefetch import _prefetch_history_data
from .types import _ALERT_TYPES

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def detect(
    frame_id: str,
    sources: list[dict],
    catalog_matches: list[dict],
    frame_meta: dict,
) -> list[dict]:
    """
    Detect and classify anomalies for all sources in a processed frame.

    Uses BATCH API queries to minimize network round-trips:
    - One POST /sources/near/batch for every source's narrow-cone history
    - One POST /sources/near/batch for the uncatalogued sources' wide cone
    - One POST /frames/covering/batch for all coverage checks

    This reduces API calls from O(N) to O(1) where N is the number of sources.

    Parameters
    ----------
    frame_id:
        Frame ID returned by api_client.post_frame() — used in log records.
    sources:
        List of source dicts as enriched by catalog_matcher.match().
        Each dict must have at minimum: ra, dec, mag, catalog_name, catalog_id,
        object_type, elongation. Also reads "_source_id" if present — the
        resolved sources.id that pipeline.py attaches after POST
        /frames/{id}/sources, propagated into each anomaly's "source_id".
    catalog_matches:
        Same list as sources (catalog_matcher enriches in-place). The
        parameter exists for API compatibility with the pipeline orchestrator.
    frame_meta:
        Dict with keys: frame_id, obs_time (ISO 8601), filename,
        ra_center, dec_center, fov_deg.

    Returns
    -------
    list[dict]
        Anomaly dicts ready to be sent to POST /frames/{id}/anomalies.
        FIRST_OBSERVATION and KNOWN_CATALOG_NEW are suppressed (not returned).
        Each returned dict has keys: anomaly_type, source_id, ra, dec,
        magnitude, delta_mag, mpc_designation, ephemeris, notes.
    """
    obs_time     = str(frame_meta.get("obs_time", ""))
    # Ephemerides are computed at the exposure MIDPOINT (see
    # fits_header.midpoint_time()); the history/coverage queries below keep
    # the start time, which is what the frame is registered under.
    obs_time_mid = str(frame_meta.get("obs_time_mid") or obs_time)
    log_filename = str(frame_meta.get("filename", "<unknown>"))
    extra        = {"frame_id": frame_id, "log_filename": log_filename}

    logger.info(
        "Anomaly detection started: %d source(s) frame_id=%s",
        len(sources), frame_id,
        extra=extra,
    )

    if not sources:
        logger.info("No sources to classify frame_id=%s", frame_id, extra=extra)
        return []

    # ------------------------------------------------------------------
    # BATCH PREFETCH: Get all historical data in at most THREE API calls
    # ------------------------------------------------------------------
    narrow_by_source, wide_by_source, coverage_by_tile = await _prefetch_history_data(
        sources, obs_time, frame_id, log_filename
    )

    # ------------------------------------------------------------------
    # Classify all sources using prefetched data (no additional API calls)
    # ------------------------------------------------------------------
    # Positions of every source detected in THIS frame — used by
    # _is_position_shifted() (via _is_still_occupied()) to tell whether a
    # candidate historical position has genuinely emptied out (a real mover
    # left it) or is still occupied by something in this very frame (a
    # permanent neighbour, not evidence of motion). Built once for the whole
    # frame rather than per-source.
    current_frame_positions: list[tuple[float, float]] = []
    for s in sources:
        s_ra, s_dec = s.get("ra"), s.get("dec")
        if s_ra is None or s_dec is None:
            continue
        try:
            current_frame_positions.append((float(s_ra), float(s_dec)))
        except (TypeError, ValueError):
            continue

    # Catalogued sources with a measured magnitude, for telling a subtraction
    # candidate that is merely a star's own residual from a transient — see
    # _classify._is_residual_of_catalogued_star().
    catalogued_frame_sources: list[tuple[float, float, float]] = []
    for s in sources:
        if s.get("catalog_name") is None:
            continue
        try:
            catalogued_frame_sources.append((float(s["ra"]), float(s["dec"]), float(s["mag"])))
        except (KeyError, TypeError, ValueError):
            continue

    anomalies: list[dict] = []

    for i, source in enumerate(sources):
        try:
            result = _classify_source_sync(
                source,
                frame_id=frame_id,
                log_filename=log_filename,
                narrow_history=narrow_by_source[i],
                wide_pool=wide_by_source.get(i, []),
                coverage_by_tile=coverage_by_tile,
                current_frame_positions=current_frame_positions,
                obs_time=obs_time,
                catalogued_frame_sources=catalogued_frame_sources,
            )
            if result is not None:
                anomalies.append(result)
        except Exception as exc:
            logger.error(
                "Unexpected error classifying source ra=%s dec=%s: %s",
                source.get("ra", "?"), source.get("dec", "?"), exc,
                extra=extra,
            )

    # Resolve ephemerides concurrently for all MPC-matched objects
    await _resolve_ephemerides(anomalies, obs_time_mid, frame_id, log_filename)

    n_alert = sum(1 for a in anomalies if a["anomaly_type"] in _ALERT_TYPES)

    logger.info(
        "Anomaly detection complete: %d anomaly/anomalies found (%d alert-worthy) "
        "frame_id=%s",
        len(anomalies), n_alert, frame_id,
        extra=extra,
    )

    return anomalies
