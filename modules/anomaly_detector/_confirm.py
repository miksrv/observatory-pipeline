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

An UNKNOWN is the other way round: it should still be there. Most UNKNOWN
alerts on that run were in one frame only — cosmic rays, hot pixels,
subtraction residuals. It is kept when a later frame detects it again (or
detects anything there — a catalogued source says nothing either way) or
when no later frame images it yet,
reclassified MOVING_UNKNOWN when it lies on a track through the later frames
(a mover seen first here), and withdrawn when every later same-filter frame
covering it shows nothing. Earlier frames never withdraw an alert: that they
show nothing here is why it is new.

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

from ._frames import _covers, _frame_records, _is_solved_ok, _same_optics
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
    current_record: dict | None,
) -> tuple[str, str, list[tuple[str, float, float, float]]]:
    """
    ("confirmed" | "untested" | "refuted" | "unpaired", explanation, checks)
    for a candidate mover at (ra, dec) — "unpaired" when no detection in the
    window could be it at another time. `pool` holds the uncatalogued
    detections around it within the window, `frames` the frames of the
    window (id → (minutes, record)), `current_record` this frame's stored
    record. A refutation by frames comes with its `checks` — (frame id, ra,
    dec, tolerance) of every predicted position it rests on — for the caller
    to make sure no catalogued detection sits there (the pool holds
    uncatalogued detections only, and a mover passing a star is matched to
    it).

    Every detection in `pool` that could be this object's position at
    another time — more than MATCH_CONE_ARCSEC away, within the reach of a
    mover over that gap (`_wide_cone_radius_arcsec()`), and at a position
    nothing in this frame occupies — is a pairing with a velocity. A pairing
    whose displacement equals the shift of the telescope pointing between the
    two frames — both plate-solved, through the same optics — is a
    sensor-fixed artifact; one that moved no more than twice
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
        # The pointing shift is only meaningful between two solved centres of
        # the same optics: an unsolved frame's centre is the mount's guess,
        # and another telescope's frame says nothing about this sensor.
        other_record = frames.get(det.frame_id, (None, None))[1]
        current_centre, other_centre = _centre(current_record), _centre(other_record)
        if (
            current_centre is not None and other_centre is not None
            and _is_solved_ok(other_record)
            and _same_optics(current_record, other_record) is not False
        ):
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
        # A detection exactly where the dither puts a hot pixel outweighs a
        # pairing too short to mean anything.
        if n_sensor_fixed:
            return "refuted", (
                f"moves with the telescope pointing in {n_sensor_fixed} pairing(s) — "
                f"fixed on the sensor (hot pixel), not on the sky"
            ), []
        if n_short:
            return "untested", (
                f"moved at most {2.0 * cone:g}\" between frames — too little to tell "
                f"motion from centroid scatter"
            ), []
        return "unpaired", "no detection within the window to pair it with", []

    untested = False
    refuted_by: set[str] = set()
    checks: list[tuple[str, float, float, float]] = []
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
                ), []

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
            predicted = _apply_offset(ra, dec, p_east, p_north)
            if _covers(record, *predicted):
                tested = True
                refuted_by.add(other_id)
                checks.append((other_id, predicted[0], predicted[1], tol))
        if not tested:
            untested = True

    if untested or n_short:
        return "untested", "no frame within the window to test its track in", []
    return "refuted", (
        f"no detection on its track in {len(refuted_by)} frame(s) that cover it "
        f"through the same filter"
    ), checks


async def _confirm_alerts(
    anomalies: list[dict],
    frame_id: str,
    obs_time: str,
    frame_meta: dict,
    current_frame_positions: list[tuple[float, float]],
    extra: dict,
    suspect: str | None = None,
    frame_psf_arcsec: float | None = None,
) -> list[dict]:
    """
    Check the alerts in `anomalies` against the frames around this one and
    return the list with the outcome applied (see the module docstring).
    Needs each anomaly's `_source`, and a MOVING_UNKNOWN's `_fallback` (a
    callable returning what the source is without the motion evidence).

    `suspect` is why this frame cannot be trusted on its own (detect()'s
    ghost-source count), or None. `frame_psf_arcsec` is the frame's seeing,
    for `_needs_confirmation()`. In a suspect frame, and for a diffuse
    subtraction candidate anywhere, an alert the frames around it cannot
    confirm is withdrawn instead of kept.

    Batch requests for the whole frame — detections at each alert's own
    position (uncatalogued, and of any kind), the uncatalogued detections
    around it, the covering frames, all up to `ALERT_CONFIRM_WINDOW_MIN`
    after this frame, then the detections of any kind at the positions a
    refuted track predicts — plus the stored records of the frames involved.
    If any of it cannot be fetched the anomalies are returned untouched: a
    check that cannot run never withdraws an alert.
    """
    window = config.ALERT_CONFIRM_WINDOW_MIN
    alerts = [a for a in anomalies if _is_checked(a)]
    t0 = _parse_obs_time(obs_time)
    if window <= 0 or not alerts or t0 is None:
        return anomalies

    until = (t0 + datetime.timedelta(minutes=window)).isoformat() + "Z"
    positions = [{"ra": float(a["ra"]), "dec": float(a["dec"])} for a in alerts]
    pool_radius = max(config.MOVING_CONE_ARCSEC, config.MOVING_CONE_MAX_ARCSEC)
    cone = config.MATCH_CONE_ARCSEC

    def _skipped(reason: str) -> list[dict]:
        logger.warning(
            "Alert confirmation skipped (%s) — %d alert(s) kept unchecked",
            reason, len(alerts),
            extra=extra,
        )
        return anomalies

    try:
        here_new, here_any, pool_result, coverage_result = await asyncio.gather(
            api_client.get_sources_near_batch(positions, cone, until, uncatalogued_only=True),
            api_client.get_sources_near_batch(positions, cone, until),
            api_client.get_sources_near_batch(positions, pool_radius, until, uncatalogued_only=True),
            api_client.get_frames_covering_batch(positions, until),
        )
    except Exception as exc:
        return _skipped(f"request failed: {exc}")
    if not all(isinstance(r, dict) for r in (here_new, here_any, pool_result, coverage_result)):
        return _skipped("the frames around this one could not be fetched")

    def _rows(result: dict, key) -> list[_Detection]:
        detections = []
        for row in result.get(str(key), []) or []:
            minutes = _minutes_from(t0, row.get("obs_time"))
            if minutes is None or abs(minutes) > window or not row.get("frame_id"):
                continue
            if str(row["frame_id"]) == frame_id:
                continue
            try:
                detections.append(_Detection(float(row["ra"]), float(row["dec"]), minutes, str(row["frame_id"])))
            except (KeyError, TypeError, ValueError):
                continue
        return detections

    seen_new = [_rows(here_new, k) for k in range(len(alerts))]
    seen_any = [_rows(here_any, k) for k in range(len(alerts))]
    pools = [_rows(pool_result, k) for k in range(len(alerts))]
    window_frames: list[dict[str, float]] = []
    for k in range(len(alerts)):
        frames: dict[str, float] = {}
        for frame in coverage_result.get(str(k), []) or []:
            minutes = _minutes_from(t0, frame.get("obs_time"))
            fid = frame.get("id")
            if minutes is None or abs(minutes) > window or not fid or str(fid) == frame_id:
                continue
            frames[str(fid)] = minutes
        window_frames.append(frames)

    frame_ids = {d.frame_id for pool in pools for d in pool}
    frame_ids.update(fid for frames in window_frames for fid in frames)
    records: dict[str, dict] = {}
    current_record = frame_meta.get("frame") or frame_meta
    if frame_ids:
        try:
            records = await _frame_records(frame_ids, (frame_meta.get("frame") or {}).get("object"))
        except Exception as exc:
            logger.warning("Alert confirmation: frame lookup failed (%s)", exc, extra=extra)

    def _frames_for(k: int) -> dict[str, tuple[float, dict]]:
        frames = {fid: (m, records[fid]) for fid, m in window_frames[k].items() if fid in records}
        # Pool detections need their frame's pointing for the sensor-fixed test.
        for det in pools[k]:
            if det.frame_id in records and det.frame_id not in frames:
                frames[det.frame_id] = (det.minutes, records[det.frame_id])
        return frames

    # One track verdict per alert, used by both checks below.
    tracks = [
        _track_verdict(
            float(a["ra"]), float(a["dec"]), obs_time, frame_id,
            (a.get("_source") or {}).get("_filter"), pools[k], _frames_for(k),
            current_frame_positions, current_record,
        )
        for k, a in enumerate(alerts)
    ]

    # A refuted track rests on frames that show no uncatalogued detection at
    # the predicted position — the object may be there all the same, matched
    # to a star it passes. Any detection there leaves the track untested.
    checks = [(k, c) for k, (state, _, cs) in enumerate(tracks) if state == "refuted" for c in cs]
    if checks:
        radius = max(c[3] for _, c in checks)
        try:
            found = await api_client.get_sources_near_batch(
                [{"ra": c[1], "dec": c[2]} for _, c in checks], radius, until
            )
        except Exception as exc:
            found = None
            logger.warning("Alert confirmation: track check failed (%s)", exc, extra=extra)
        if not isinstance(found, dict):
            return _skipped("the predicted track positions could not be checked")
        for n, (k, (fid, ra, dec, tol)) in enumerate(checks):
            if tracks[k][0] != "refuted":
                continue
            for row in found.get(str(n), []) or []:
                try:
                    hit = str(row.get("frame_id")) == fid and _haversine_arcsec(
                        ra, dec, float(row["ra"]), float(row["dec"])) <= tol
                except (KeyError, TypeError, ValueError):
                    continue
                if hit:
                    tracks[k] = ("untested", "a catalogued source lies on its predicted track", [])
                    break

    def _stationary(anomaly: dict, k: int) -> tuple[str, str]:
        """
        ("confirmed" | "moving" | "untested" | "refuted", explanation) for an
        alert that should still be there in the next frames: a nova stays for
        days, a cosmic ray or a hot pixel is gone (dithered).
        """
        later = sorted({d.frame_id for d in seen_new[k] if d.minutes > 0.0})
        if later:
            return "confirmed", f"Seen again in {len(later)} later frame(s) within {window:g} min."
        if any(d.minutes > 0.0 for d in seen_any[k]):
            return "untested", "a catalogued source is detected here in later frames"

        # Gone from here — but a mover first seen in this frame is somewhere
        # else in the next ones: a track through it is a MOVING_UNKNOWN.
        track_state, track_note, _ = tracks[k]
        if track_state == "confirmed":
            return "moving", track_note
        if track_state == "untested":
            return "untested", f"absent from later frames here, but {track_note}"

        filter_name = (anomaly.get("_source") or {}).get("_filter")
        ra, dec = float(anomaly["ra"]), float(anomaly["dec"])
        refuting = [
            fid for fid, (minutes, record) in _frames_for(k).items()
            if minutes > 0.0 and _is_solved_ok(record)
            and record.get("filter") == filter_name and _covers(record, ra, dec)
        ]
        if refuting:
            return "refuted", (
                f"absent from {len(refuting)} later frame(s) that cover it through the "
                f"same filter — a cosmic ray, hot pixel or residual, not an object"
            )
        return "untested", "no later frame of this position through the same filter"

    index = {id(a): k for k, a in enumerate(alerts)}
    counts: dict[str, int] = {}
    result: list[dict] = []
    for anomaly in anomalies:
        if id(anomaly) not in index:
            result.append(anomaly)
            continue
        k = index[id(anomaly)]
        distrust = suspect or _needs_confirmation(anomaly, frame_psf_arcsec)
        if anomaly.get("anomaly_type") == AnomalyType.MOVING_UNKNOWN:
            state, note, _ = tracks[k]
            if state == "unpaired":
                state = "untested"
            if state == "untested" and distrust:
                state, note = "withdrawn", f"unconfirmed ({distrust})"
            counts[f"MOVING_UNKNOWN {state}"] = counts.get(f"MOVING_UNKNOWN {state}", 0) + 1
            if state in ("confirmed", "untested"):
                if state == "untested":
                    note = f"Track not confirmed yet: {note} ({window:g} min)."
                anomaly["notes"] = f"{anomaly.get('notes') or ''} {note}".strip()
                result.append(anomaly)
                continue
            fallback = None
            if state == "refuted":
                try:
                    fallback = (anomaly.get("_fallback") or (lambda: None))()
                except Exception as exc:
                    logger.error(
                        "MOVING_UNKNOWN at ra=%.4f dec=%.4f refuted (%s), but classifying it "
                        "without the motion failed (%s) — kept",
                        anomaly["ra"], anomaly["dec"], note, exc,
                        extra=extra,
                    )
                    result.append(anomaly)
                    continue
            logger.info(
                "MOVING_UNKNOWN withdrawn at ra=%.4f dec=%.4f: %s — now %s",
                anomaly["ra"], anomaly["dec"], note,
                fallback["anomaly_type"] if fallback else "no anomaly",
                extra=extra,
            )
            if fallback is None:
                continue
            fallback.setdefault("_source", anomaly.get("_source"))
            fallback["notes"] = (
                f"{fallback.get('notes') or ''} Classified MOVING_UNKNOWN first, "
                f"withdrawn: {note}."
            ).strip()
            if not _is_checked(fallback):
                result.append(fallback)
                continue
            anomaly = fallback  # checked as what it fell back to, below
            distrust = suspect or _needs_confirmation(anomaly, frame_psf_arcsec)

        state, note = _stationary(anomaly, k)
        if state == "untested" and distrust:
            state, note = "refuted", f"unconfirmed ({distrust})"
        label = str(getattr(anomaly["anomaly_type"], "value", anomaly["anomaly_type"]))
        counts[f"{label} {state}"] = counts.get(f"{label} {state}", 0) + 1
        if state == "refuted":
            logger.info(
                "%s withdrawn at ra=%.4f dec=%.4f: %s",
                label, anomaly["ra"], anomaly["dec"], note,
                extra=extra,
            )
            continue
        if state == "moving":
            logger.warning(
                "ALERT — MOVING_UNKNOWN: %s at ra=%.4f dec=%.4f is on a track through "
                "the next frames — %s",
                label, anomaly["ra"], anomaly["dec"], note,
                extra=extra,
            )
            anomaly["anomaly_type"] = AnomalyType.MOVING_UNKNOWN
            note = f"First seen in this frame, then moved: {note}"
        elif state == "untested":
            note = f"Not confirmed yet: {note} ({window:g} min)."
        anomaly["notes"] = f"{anomaly.get('notes') or ''} {note}".strip()
        result.append(anomaly)

    logger.info(
        "Alert confirmation: %s",
        ", ".join(f"{k} {v}" for k, v in sorted(counts.items())) or "nothing to check",
        extra=extra,
    )
    return result


def _is_checked(anomaly: dict) -> bool:
    """
    The alerts this module checks: MOVING_UNKNOWN and UNKNOWN.

    Not a SUPERNOVA_CANDIDATE: a new one is a subtraction candidate matched
    to its host galaxy, so every later frame "detects it again" — the galaxy
    itself — and nothing on the wire tells the two apart; a brightening one
    has already been confirmed over `VARIABILITY_CONFIRM_EPOCHS`. It stands
    as in 1.5.0.
    """
    return anomaly.get("anomaly_type") in (AnomalyType.MOVING_UNKNOWN, AnomalyType.UNKNOWN)


def _needs_confirmation(anomaly: dict, frame_psf_arcsec: float | None) -> str | None:
    """
    Why this alert stands only if the frames around it confirm it, or None.

    A subtraction candidate wider than `SUBTRACTION_DIFFUSE_FWHM_RATIO` ×
    the frame's PSF is not a point source appearing: on the 2026-10-22
    M51/M63/M82 run 60 alerts were such blobs, 3-20 × the PSF, and the frames
    themselves show only noise there — a stretch of the difference image's
    background left uncancelled (a narrowband frame against broadband
    references, a gradient). A new comet would be diffuse too; it is still
    there in the next frames, so it is kept once they confirm it, and a
    known one is matched through MPC before any of this.
    """
    source = anomaly.get("_source") or {}
    if not source.get("_from_subtraction") or not frame_psf_arcsec:
        return None
    try:
        fwhm = float(source.get("fwhm"))
    except (TypeError, ValueError):
        return None
    ratio = config.SUBTRACTION_DIFFUSE_FWHM_RATIO
    if ratio <= 0 or not math.isfinite(fwhm) or fwhm <= ratio * frame_psf_arcsec:
        return None
    return f"diffuse subtraction candidate, FWHM {fwhm:.1f}\" > {ratio:g} × PSF {frame_psf_arcsec:.1f}\""
