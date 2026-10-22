"""
modules/anomaly_detector/_detect.py — the package's public entry point,
orchestrating prefetch → classify → ephemeris resolution for one frame's
worth of sources.
"""

from __future__ import annotations

import logging
import math

import config

from ._classify import _classify_source_sync
from ._confirm import _confirm_alerts
from ._ephemeris_resolution import _resolve_ephemerides
from ._frames import _frame_records, _same_optics
from ._prefetch import _prefetch_history_data
from .types import _ALERT_TYPES, AnomalyType

logger = logging.getLogger(__name__)


def _finite_or_none(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _ghost_sources(sources: list[dict], frame_psf_arcsec: float | None) -> list[dict]:
    """
    Detections that look like ordinary stars — round (`STAR_ELONGATION_MAX`),
    no wider than 1.5 × the frame's PSF when that is known, unsaturated, not
    a subtraction candidate — brighter than `SUSPECT_FRAME_MAG`, and in no
    catalog. Gaia is complete far below that magnitude, so several of them in
    one frame are copies of real stars (a mount jump, a double exposure)
    rather than new objects. Uncalibrated frames have no magnitudes and
    never count any.
    """
    ghosts = []
    for s in sources:
        if s.get("catalog_name") is not None or s.get("_from_subtraction") or s.get("saturated"):
            continue
        mag = _finite_or_none(s.get("mag"))
        if mag is None or mag > config.SUSPECT_FRAME_MAG:
            continue
        elongation = _finite_or_none(s.get("elongation"))
        if elongation is None or elongation > config.STAR_ELONGATION_MAX:
            continue
        fwhm = _finite_or_none(s.get("fwhm"))
        if frame_psf_arcsec and fwhm is not None and fwhm > 1.5 * frame_psf_arcsec:
            continue
        ghosts.append(s)
    return ghosts


async def _mark_history_optics(
    narrow_by_source: list[list[dict]],
    frame_record: dict,
    extra: dict,
) -> None:
    """
    Mark every narrow-cone history row `_same_optics` — True/False when the
    stored record of the frame behind it says whether that frame was taken
    through this frame's optics (`_frames._same_optics()`), None when it
    cannot say. A lookup failure leaves every row unmarked, which the
    classifier treats exactly like None.
    """
    frame_ids = {row.get("frame_id") for rows in narrow_by_source for row in rows}
    frame_ids.discard(None)
    if not frame_ids:
        return
    try:
        records = await _frame_records(frame_ids, frame_record.get("object"))
    except Exception as exc:
        logger.warning(
            "Could not look up the frames behind this frame's history (%s) — "
            "magnitudes are compared across optics", exc,
            extra=extra,
        )
        return
    verdicts = {fid: _same_optics(frame_record, rec) for fid, rec in records.items()}
    for rows in narrow_by_source:
        for row in rows:
            row["_same_optics"] = verdicts.get(str(row.get("frame_id")))


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
        ra_center, dec_center, fov_deg. Optionally "frame": the frame's
        stored record (GET /frames/{id}) — its camera, plate scale and QC
        seeing; without it magnitudes are compared across optics and the
        light-curve branch has no PSF to judge extended sources by.

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
    extra        = {"frame_id": frame_id, "fits_filename": log_filename}

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

    # This frame's stored record (GET /frames/{id}) — present on the
    # standalone DETECT_ANOMALIES path. It tells which optics took the frame,
    # so the star branches can compare a source only with epochs from the
    # same optics, and gives the frame's own seeing for the light-curve
    # branch's extended-source test. Without it both stay as before.
    frame_record = frame_meta.get("frame") or None
    frame_psf_arcsec = _finite_or_none((frame_record or {}).get("qc_fwhm_median"))
    if frame_record:
        await _mark_history_optics(narrow_by_source, frame_record, extra)

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

    # Catalogued sources with a measured magnitude or flux, for telling a subtraction
    # candidate that is merely a star's own residual from a transient — see
    # _classify._is_residual_of_catalogued_star().
    catalogued_frame_sources: list[tuple[float, float, float | None, float | None]] = []
    for s in sources:
        if s.get("catalog_name") is None:
            continue
        try:
            ra_, dec_ = float(s["ra"]), float(s["dec"])
        except (KeyError, TypeError, ValueError):
            continue
        mag_, flux_ = _finite_or_none(s.get("mag")), _finite_or_none(s.get("flux"))
        if mag_ is not None or flux_ is not None:
            # (ra, dec, mag, flux) — the flux stands in for brightness on a
            # frame without calibrated magnitudes (narrowband, 8-bit).
            catalogued_frame_sources.append((ra_, dec_, mag_, flux_))

    anomalies: list[dict] = []

    for i, source in enumerate(sources):
        try:
            classify_args = dict(
                frame_id=frame_id,
                log_filename=log_filename,
                narrow_history=narrow_by_source[i],
                wide_pool=wide_by_source.get(i, []),
                coverage_by_tile=coverage_by_tile,
                current_frame_positions=current_frame_positions,
                obs_time=obs_time,
                catalogued_frame_sources=catalogued_frame_sources,
                frame_psf_arcsec=frame_psf_arcsec,
            )
            result = _classify_source_sync(source, **classify_args)
            if result is not None:
                # For the alert confirmation below; stripped before returning.
                result["_source"] = source
                if result["anomaly_type"] == AnomalyType.MOVING_UNKNOWN:
                    fallback = _classify_source_sync(source, allow_moving=False, **classify_args)
                    if fallback is not None:
                        fallback["_source"] = source
                    result["_fallback"] = fallback
                anomalies.append(result)
        except Exception as exc:
            logger.error(
                "Unexpected error classifying source ra=%s dec=%s: %s",
                source.get("ra", "?"), source.get("dec", "?"), exc,
                extra=extra,
            )

    # Check the alerts against the frames around this one — see _confirm.py.
    # A frame full of bright uncatalogued "stars" is holding ghost copies of
    # real ones; there only a confirmed alert stands.
    ghosts = _ghost_sources(sources, frame_psf_arcsec)
    suspect = None
    if len(ghosts) >= max(1, config.SUSPECT_FRAME_MIN_SOURCES):
        suspect = (
            f"{len(ghosts)} uncatalogued star-like sources brighter than "
            f"{config.SUSPECT_FRAME_MAG:g} mag in a suspect frame — ghost images "
            f"(mount jump, double exposure)?"
        )
        logger.warning(
            "Suspect frame: %s — its alerts stand only if the frames around it "
            "confirm them",
            suspect,
            extra=extra,
        )
    anomalies = await _confirm_alerts(
        anomalies, frame_id, obs_time, frame_meta, current_frame_positions, extra,
        suspect=suspect, frame_psf_arcsec=frame_psf_arcsec,
    )
    for anomaly in anomalies:
        anomaly.pop("_source", None)
        anomaly.pop("_fallback", None)

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
