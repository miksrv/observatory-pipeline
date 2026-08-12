"""
modules/anomaly_detector/_geometry.py — coordinate/tile arithmetic and
spatial filtering shared by prefetching, movement detection, and
classification.

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import math

# ---------------------------------------------------------------------------
# Coordinate helpers
# ---------------------------------------------------------------------------

def _haversine_arcsec(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
    """
    Great-circle angular separation between two points in arcseconds.

    Uses the haversine formula to avoid catastrophic cancellation near the poles.
    All inputs in decimal degrees.
    """
    ra1_r  = math.radians(ra1)
    ra2_r  = math.radians(ra2)
    dec1_r = math.radians(dec1)
    dec2_r = math.radians(dec2)

    delta_ra  = ra2_r  - ra1_r
    delta_dec = dec2_r - dec1_r

    a = (
        math.sin(delta_dec / 2.0) ** 2
        + math.cos(dec1_r) * math.cos(dec2_r) * math.sin(delta_ra / 2.0) ** 2
    )
    sep_rad = 2.0 * math.asin(math.sqrt(a))
    return math.degrees(sep_rad) * 3600.0  # convert degrees → arcseconds


# ---------------------------------------------------------------------------
# Tile helpers for batch queries
# ---------------------------------------------------------------------------

def _tile_key(ra: float, dec: float, tile_size: float = 0.1) -> tuple[float, float]:
    """
    Round RA/Dec to tiles of given size (in degrees) for batch query optimization.

    Default tile_size=0.1 degrees (~6 arcmin) groups nearby sources together.
    """
    return round(ra / tile_size) * tile_size, round(dec / tile_size) * tile_size


# ---------------------------------------------------------------------------
# Radius filtering
#
# Grouped here rather than with the other "History helpers" in _history.py
# (where this function sat in the original monolithic anomaly_detector.py) —
# it is a pure spatial filter with no notion of "history" of its own; both
# _prefetch.py's batch results and _classify.py's per-source narrow/wide
# cones are filtered through it the same way.
# ---------------------------------------------------------------------------

def _find_sources_within_radius(
    ra: float,
    dec: float,
    radius_arcsec: float,
    all_sources: list[dict],
) -> list[dict]:
    """
    Filter sources that fall within radius_arcsec from (ra, dec).

    This is used to find sources near a specific position from the batch
    query results which cover a larger tile area.
    """
    result = []
    for src in all_sources:
        src_ra = src.get("ra")
        src_dec = src.get("dec")
        if src_ra is None or src_dec is None:
            continue
        try:
            sep = _haversine_arcsec(ra, dec, float(src_ra), float(src_dec))
            if sep <= radius_arcsec:
                result.append(src)
        except (TypeError, ValueError):
            continue
    return result
