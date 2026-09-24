"""
modules/anomaly_detector/_movement.py — "has this source actually moved?"
evidence used by _classify.py's unmatched-source branches.

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import datetime

import config

from ._geometry import _haversine_arcsec

# ---------------------------------------------------------------------------
# Moving-object detection
# ---------------------------------------------------------------------------

def _parse_obs_time(value) -> datetime.datetime | None:
    """
    Parse an ISO 8601 timestamp into a naive UTC datetime, or None when it is
    missing or unparseable.

    Normalised to naive UTC so that two timestamps can be subtracted whether
    or not either carried an explicit offset — the API writes "Z", while a
    frame's own obs_time comes straight out of the FITS header and usually
    carries none.
    """
    if not value:
        return None
    text = str(value).strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return parsed


def _wide_cone_radius_arcsec(obs_time, hist_obs_time) -> float:
    """
    Return the radius within which a historical detection could plausibly be
    the same object as one detected at `obs_time` — `MOVING_CONE_ARCSEC` as a
    floor, extended by how far a fast mover could actually have travelled in
    the elapsed time.

    A fixed cone is the wrong shape for this question: how far an object can
    legitimately have moved between two frames is a rate times a time gap, not
    a constant. At `MOVING_CONE_ARCSEC = 120"` an object that moved further
    than that between frames has its own previous position outside the search
    entirely, so the "shifted" test can never confirm it and a genuine fast
    mover falls through to a generic UNKNOWN or is dropped as
    FIRST_OBSERVATION (audit 2026-08-18, finding H3).

    The extension is bounded twice over, because a wider cone sweeps in more
    unrelated historical detections and each one is another candidate for the
    "its old position has vacated" half of the evidence — the exact
    false-positive mode that test exists to prevent (docs/ISSUES.md #1):

    * `MOVING_CONE_MAX_ARCSEC` caps the radius outright.
    * `MOVING_EXTEND_MAX_GAP_MIN` declines to extend at all for a historical
      detection older than that. Past that gap `rate × time` exceeds the cap
      for any rate worth considering, so the extension would degenerate into
      "the cap, always" — a permanently wide cone rather than a
      physically-motivated one.

    Falls back to the plain `MOVING_CONE_ARCSEC` — i.e. exactly the previous
    behaviour — whenever either timestamp is missing or unparseable.
    """
    base = config.MOVING_CONE_ARCSEC

    t_now = _parse_obs_time(obs_time)
    t_hist = _parse_obs_time(hist_obs_time)
    if t_now is None or t_hist is None:
        return base

    gap_min = abs((t_now - t_hist).total_seconds()) / 60.0
    if gap_min > config.MOVING_EXTEND_MAX_GAP_MIN:
        return base

    reach = config.MOVING_RATE_ARCSEC_PER_MIN * gap_min
    return min(max(base, reach), max(base, config.MOVING_CONE_MAX_ARCSEC))


def _find_wide_history(
    ra: float,
    dec: float,
    pool: list[dict],
    obs_time,
) -> tuple[list[dict], float]:
    """
    Return the historical detections that could be this source's own previous
    position, together with the largest radius actually applied.

    Unlike `_geometry._find_sources_within_radius()`, the radius is not one
    number for the whole call: each candidate is tested against a cone sized
    from ITS OWN age via `_wide_cone_radius_arcsec()`, so a detection from
    three minutes ago is admitted out to a fast mover's three-minute reach
    while one from last week is held to the plain `MOVING_CONE_ARCSEC`.

    The second return value is for the anomaly's `notes` field only — the
    widest cone this call ended up considering, so an operator reading a
    MOVING_UNKNOWN can tell whether it rests on the base cone or on the
    time-scaled extension.
    """
    candidates: list[dict] = []
    radius_max = config.MOVING_CONE_ARCSEC

    for src in pool:
        src_ra = src.get("ra")
        src_dec = src.get("dec")
        if src_ra is None or src_dec is None:
            continue
        try:
            sep = _haversine_arcsec(ra, dec, float(src_ra), float(src_dec))
        except (TypeError, ValueError):
            continue

        radius = _wide_cone_radius_arcsec(obs_time, src.get("obs_time"))
        radius_max = max(radius_max, radius)
        if sep <= radius:
            candidates.append(src)

    return candidates, radius_max


def _is_still_occupied(
    hist_ra: float,
    hist_dec: float,
    current_frame_positions: list[tuple[float, float]],
) -> bool:
    """
    Return True if some OTHER source in the same current frame sits within
    MATCH_CONE_ARCSEC of a historical detection's position.

    Used to tell "something used to be here and genuinely isn't anymore"
    (the actual signature of a mover having left) apart from "something is
    just permanently parked nearby" (a neighbouring star/galaxy that is
    still sitting at that same spot in THIS frame too, and therefore cannot
    be the thing that moved to the position under test).
    """
    threshold = config.MATCH_CONE_ARCSEC
    for cur_ra, cur_dec in current_frame_positions:
        if _haversine_arcsec(hist_ra, hist_dec, cur_ra, cur_dec) <= threshold:
            return True
    return False


def _is_position_shifted(
    narrow_history: list[dict],
    wide_history: list[dict],
    current_frame_positions: list[tuple[float, float]],
) -> bool:
    """
    Return True only when BOTH hold:

    1. Nothing was ever detected within MATCH_CONE_ARCSEC of the CURRENT
       position (`narrow_history` is empty) — this exact spot is new.
    2. At least one historical detection within the wider MOVING_CONE_ARCSEC
       neighbourhood has genuinely vanished — no source in the CURRENT frame
       sits near its old position anymore (`_is_still_occupied` is False for it).

    Checking only condition 1 (the old behaviour looked at wide_history
    alone) false-positived on almost every uncatalogued source: MOVING_CONE_ARCSEC
    (120″ by default) covers a large enough patch of sky that *some* unrelated
    historical detection — a neighbouring star, a galaxy smudge, anything ever
    recorded nearby — is virtually always present there, whether or not this
    particular source moved a single pixel (see docs/ISSUES.md #1; the tiny
    sub-arcsecond scatter between epochs on an otherwise-static source is
    ordinary centroid/seeing noise, not motion, and used to be enough to
    trigger this branch purely because *something else* happened to be in the
    neighbourhood). Requiring the old position to have actually emptied out
    (condition 2) rules out that class of false positive while still catching
    real movers, whose old position is — by definition — no longer occupied
    by anything once they've moved away from it.
    """
    if narrow_history:
        return False

    for hist_src in wide_history:
        hist_ra  = hist_src.get("ra")
        hist_dec = hist_src.get("dec")
        if hist_ra is None or hist_dec is None:
            continue
        try:
            hist_ra_f, hist_dec_f = float(hist_ra), float(hist_dec)
        except (TypeError, ValueError):
            continue
        if not _is_still_occupied(hist_ra_f, hist_dec_f, current_frame_positions):
            return True
    return False
