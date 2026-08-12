"""
modules/anomaly_detector/_movement.py — "has this source actually moved?"
evidence used by _classify.py's unmatched-source branches.

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import config

from ._geometry import _haversine_arcsec

# ---------------------------------------------------------------------------
# Moving-object detection
# ---------------------------------------------------------------------------

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
