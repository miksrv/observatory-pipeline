"""
modules/anomaly_detector/_confirm.py — checking an alert against the other
frames of the same field taken around it (`ALERT_CONFIRM_WINDOW_MIN`, before
and after), once the frame's sources have been classified.

A MOVING_UNKNOWN is classified from two detections: this one, and an
uncatalogued one nearby in an earlier frame whose position is empty now. On
the 2026-10-22 M51/M63/M82 run most of the 120 were not movers at all: a hot
pixel or cosmic ray, which a dithered sequence puts at a new sky position in
every frame, paired with another one. A real mover is in the neighbouring
frames too, on a straight line at a steady rate; a hot pixel is fixed on the
sensor, so it "moves" exactly as the telescope pointing does.

So a MOVING_UNKNOWN is kept when a third detection lies on its track
(confirmed) or when no frame is there to test the track (untested — a
sequence's last frame, or detection run as each frame arrives). It is
withdrawn only when the track moves with the telescope, or when every frame
that should show it — QC passed, plate-solved, the same filter, covering the
predicted position — shows nothing there. A withdrawn mover falls back to
what the source is without the motion evidence (usually UNKNOWN).

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import math
from dataclasses import dataclass

import api_client
import config

from ._frames import _covers, _frame_records, _is_solved_ok
from ._geometry import _haversine_arcsec
from ._movement import _is_still_occupied, _parse_obs_time, _wide_cone_radius_arcsec
from .types import AnomalyType

logger = logging.getLogger(__name__)

# How far a two-point track is extrapolated, in MATCH_CONE_ARCSEC. A velocity
# measured over a gap g predicts the position Δt away with an error growing
# as Δt/g; past this the prediction says too little to confirm or refute.
_MAX_TOLERANCE_CONES = 3.0


@dataclass(frozen=True)
class _Detection:
    ra: float
    dec: float
    minutes: float  # from this frame's obs_time; negative = earlier
    frame_id: str


def _offset_arcsec(ra0: float, dec0: float, ra: float, dec: float) -> tuple[float, float]:
    """Tangent-plane offset of (ra, dec) from (ra0, dec0): east, north in arcsec."""
    d_ra = (ra - ra0 + 180.0) % 360.0 - 180.0
    return d_ra * math.cos(math.radians(dec0)) * 3600.0, (dec - dec0) * 3600.0


def _apply_offset(ra0: float, dec0: float, east: float, north: float) -> tuple[float, float]:
    """Inverse of `_offset_arcsec()` (small offsets)."""
    return ra0 + east / 3600.0 / math.cos(math.radians(dec0)), dec0 + north / 3600.0


def _centre(frame: dict | None) -> tuple[float, float] | None:
    try:
        return float(frame["ra_center"]), float(frame["dec_center"])
    except (KeyError, TypeError, ValueError):
        return None


def _minutes_from(t0: datetime.datetime, value) -> float | None:
    t = _parse_obs_time(value)
    return None if t is None else (t - t0).total_seconds() / 60.0


def _track_verdict(
    ra: float,
    dec: float,
    obs_time: str,
    frame_id: str,
    filter_name: str | None,
    pool: list[_Detection],
    frames: dict[str, tuple[float, dict]],
    current_frame_positions: list[tuple[float, float]],
    current_centre: tuple[float, float] | None,
) -> tuple[str, str]:
    """
    ("confirmed" | "untested" | "refuted", explanation) for a candidate mover
    at (ra, dec). `pool` holds the uncatalogued detections around it within
    the window, `frames` the frames of the window (id → (minutes, record)).

    Every detection in `pool` that could be this object's position at
    another time — more than MATCH_CONE_ARCSEC away, within the reach of a
    mover over that gap (`_wide_cone_radius_arcsec()`), and at a position
    nothing in this frame occupies — is a pairing with a velocity. A pairing
    whose displacement equals the shift of the telescope pointing between the
    two frames is a sensor-fixed artifact; one that moved no more than twice
    MATCH_CONE_ARCSEC can neither confirm nor refute anything. Any other
    pairing is confirmed by a third detection on its line, refuted by each
    frame that should show the object and does not, and untested otherwise.
    """
    cone = config.MATCH_CONE_ARCSEC
    pool_radius = max(config.MOVING_CONE_ARCSEC, config.MOVING_CONE_MAX_ARCSEC)

    pairings: list[tuple[_Detection, float, float]] = []
    n_sensor_fixed = 0
    n_short = 0
    for det in pool:
        if det.frame_id == frame_id or det.minutes == 0.0:
            continue
        separation = _haversine_arcsec(ra, dec, det.ra, det.dec)
        if separation <= cone:
            continue
        reach = _wide_cone_radius_arcsec(
            obs_time,
            (_parse_obs_time(obs_time) + datetime.timedelta(minutes=det.minutes)).isoformat(),
        )
        if separation > reach:
            continue
        if _is_still_occupied(det.ra, det.dec, current_frame_positions):
            continue
        east, north = _offset_arcsec(ra, dec, det.ra, det.dec)
        other_centre = _centre(frames.get(det.frame_id, (None, None))[1])
        if current_centre is not None and other_centre is not None:
            p_east, p_north = _offset_arcsec(*current_centre, *other_centre)
            if math.hypot(east - p_east, north - p_north) <= cone:
                n_sensor_fixed += 1
                continue
        if separation <= 2.0 * cone:
            # Within twice the matching cone, motion and centroid scatter look
            # alike: a 13.9 mag "mover" at 0.4"/min on M63 (2020-05-08) moved
            # 6" in 15 min, and JPL knows no small body within 0.25°.
            n_short += 1
            continue
        pairings.append((det, east / det.minutes, north / det.minutes))

    if not pairings:
        if n_short:
            return "untested", (
                f"moved at most {2.0 * cone:g}\" between frames — too little to tell "
                f"motion from centroid scatter"
            )
        if n_sensor_fixed:
            return "refuted", (
                f"moves with the telescope pointing in {n_sensor_fixed} pairing(s) — "
                f"fixed on the sensor (hot pixel), not on the sky"
            )
        return "untested", "no detection within the window to pair it with"

    untested = False
    refuted_by: set[str] = set()
    for det, v_east, v_north in pairings:
        used = {frame_id, det.frame_id}

        for other in pool:
            if other.frame_id in used:
                continue
            tol = cone * max(1.0, abs(other.minutes / det.minutes))
            if tol > _MAX_TOLERANCE_CONES * cone:
                continue
            east, north = _offset_arcsec(ra, dec, other.ra, other.dec)
            if math.hypot(east - v_east * other.minutes, north - v_north * other.minutes) <= tol:
                rate = math.hypot(v_east, v_north)
                return "confirmed", (
                    f"Track confirmed by detections in two other frames "
                    f"({det.minutes:+.0f} and {other.minutes:+.0f} min), "
                    f"rate {rate:.2f}\"/min."
                )

        tested = False
        for other_id, (minutes, record) in frames.items():
            if other_id in used:
                continue
            tol = cone * max(1.0, abs(minutes / det.minutes))
            if tol > _MAX_TOLERANCE_CONES * cone:
                continue
            p_east, p_north = v_east * minutes, v_north * minutes
            if math.hypot(p_east, p_north) + tol > pool_radius:
                continue
            if not _is_solved_ok(record) or record.get("filter") != filter_name:
                continue
            if _covers(record, *_apply_offset(ra, dec, p_east, p_north)):
                tested = True
                refuted_by.add(other_id)
        if not tested:
            untested = True

    if untested or n_short:
        return "untested", "no frame within the window to test its track in"
    return "refuted", (
        f"no detection on its track in {len(refuted_by)} frame(s) that cover it "
        f"through the same filter"
    )


async def _confirm_alerts(
    anomalies: list[dict],
    frame_id: str,
    obs_time: str,
    frame_meta: dict,
    current_frame_positions: list[tuple[float, float]],
    extra: dict,
) -> list[dict]:
    """
    Check every MOVING_UNKNOWN in `anomalies` against the frames around it
    (see the module docstring) and return the anomaly list with the refuted
    ones replaced by their fallback classification (`_fallback`, set by
    detect(); None drops it). Needs each anomaly's `_source`.

    Two batch requests for the whole frame — the uncatalogued detections and
    the covering frames up to `ALERT_CONFIRM_WINDOW_MIN` after this frame —
    plus the stored records of the frames involved. If any of it cannot be
    fetched the anomalies are returned untouched: a check that cannot run
    never withdraws an alert.
    """
    window = config.ALERT_CONFIRM_WINDOW_MIN
    movers = [a for a in anomalies if a.get("anomaly_type") == AnomalyType.MOVING_UNKNOWN]
    t0 = _parse_obs_time(obs_time)
    if window <= 0 or not movers or t0 is None:
        return anomalies

    until = (t0 + datetime.timedelta(minutes=window)).isoformat() + "Z"
    positions = [{"ra": float(a["ra"]), "dec": float(a["dec"])} for a in movers]
    pool_radius = max(config.MOVING_CONE_ARCSEC, config.MOVING_CONE_MAX_ARCSEC)

    try:
        pool_result, coverage_result = await asyncio.gather(
            api_client.get_sources_near_batch(positions, pool_radius, until, uncatalogued_only=True),
            api_client.get_frames_covering_batch(positions, until),
        )
    except Exception as exc:
        pool_result = coverage_result = None
        logger.warning("Alert confirmation: request failed (%s)", exc, extra=extra)
    if not isinstance(pool_result, dict) or not isinstance(coverage_result, dict):
        logger.warning(
            "Alert confirmation skipped: the frames around this one could not be "
            "fetched — %d MOVING_UNKNOWN kept unchecked", len(movers),
            extra=extra,
        )
        return anomalies

    pools: list[list[_Detection]] = []
    window_frames: list[dict[str, float]] = []
    frame_ids: set[str] = set()
    for k in range(len(movers)):
        pool: list[_Detection] = []
        for row in pool_result.get(str(k), []) or []:
            minutes = _minutes_from(t0, row.get("obs_time"))
            if minutes is None or abs(minutes) > window or not row.get("frame_id"):
                continue
            try:
                pool.append(_Detection(float(row["ra"]), float(row["dec"]), minutes, str(row["frame_id"])))
            except (KeyError, TypeError, ValueError):
                continue
            frame_ids.add(str(row["frame_id"]))
        pools.append(pool)

        frames: dict[str, float] = {}
        for frame in coverage_result.get(str(k), []) or []:
            minutes = _minutes_from(t0, frame.get("obs_time"))
            fid = frame.get("id")
            if minutes is None or abs(minutes) > window or not fid or str(fid) == frame_id:
                continue
            frames[str(fid)] = minutes
            frame_ids.add(str(fid))
        window_frames.append(frames)

    records: dict[str, dict] = {}
    if frame_ids:
        object_name = (frame_meta.get("frame") or {}).get("object")
        try:
            records = await _frame_records(frame_ids, object_name)
        except Exception as exc:
            logger.warning("Alert confirmation: frame lookup failed (%s)", exc, extra=extra)

    current_record = frame_meta.get("frame") or frame_meta
    current_centre = _centre(current_record)

    verdicts: dict[int, tuple[str, str]] = {}
    for k, anomaly in enumerate(movers):
        source = anomaly.get("_source") or {}
        frames = {
            fid: (minutes, records[fid])
            for fid, minutes in window_frames[k].items() if fid in records
        }
        # Pool detections need their frame's pointing for the sensor-fixed test.
        for det in pools[k]:
            if det.frame_id in records and det.frame_id not in frames:
                frames[det.frame_id] = (det.minutes, records[det.frame_id])
        verdicts[id(anomaly)] = _track_verdict(
            float(anomaly["ra"]), float(anomaly["dec"]), obs_time, frame_id,
            source.get("_filter"), pools[k], frames, current_frame_positions, current_centre,
        )

    result: list[dict] = []
    counts = {"confirmed": 0, "untested": 0, "refuted": 0}
    for anomaly in anomalies:
        verdict = verdicts.get(id(anomaly))
        if verdict is None:
            result.append(anomaly)
            continue
        state, note = verdict
        counts[state] += 1
        if state == "refuted":
            fallback = anomaly.get("_fallback")
            logger.info(
                "MOVING_UNKNOWN withdrawn at ra=%.4f dec=%.4f: %s — now %s",
                anomaly["ra"], anomaly["dec"], note,
                fallback["anomaly_type"] if fallback else "no anomaly",
                extra=extra,
            )
            if fallback is not None:
                fallback["notes"] = (
                    f"{fallback.get('notes') or ''} Classified MOVING_UNKNOWN first, "
                    f"withdrawn: {note}."
                ).strip()
                result.append(fallback)
            continue
        if state == "untested":
            note = f"Track not confirmed yet: {note} ({window:g} min)."
        anomaly["notes"] = f"{anomaly.get('notes') or ''} {note}".strip()
        result.append(anomaly)

    logger.info(
        "Alert confirmation: MOVING_UNKNOWN %d confirmed, %d untested, %d withdrawn",
        counts["confirmed"], counts["untested"], counts["refuted"],
        extra=extra,
    )
    return result
