"""
modules/anomaly_detector/_history.py — extracting and filtering historical
magnitude data for the Δmag comparison in _classify.py.

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import statistics

# ---------------------------------------------------------------------------
# History helpers
# ---------------------------------------------------------------------------

def _extract_mag(source_dict: dict) -> float | None:
    """
    Safely extract a magnitude float from a historical source dict returned
    by the API.  The API may use 'mag' or 'magnitude' as the key.
    """
    for key in ("mag", "magnitude"):
        val = source_dict.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return None


def _history_median_mag(history: list[dict]) -> float | None:
    """
    Compute the median magnitude across all prior detections.

    Returns None if no valid magnitude values are present.
    """
    mags = [m for src in history if (m := _extract_mag(src)) is not None]
    if not mags:
        return None
    return statistics.median(mags)


def _same_filter_history(history: list[dict], filter_name: str | None) -> list[dict]:
    """
    Restrict a history list to entries observed through the SAME filter as
    the current source, for magnitude comparison only.

    Real astronomy background: a star's brightness in filter R differs from
    its brightness in filter G (or Gaia's broadband G-band) purely because of
    its color/temperature — a "color term" — independent of any actual
    change in the object. Comparing today's L-band magnitude against last
    week's R-band (or Hα) detection of the same object would therefore read
    as a brightness change that is really just a filter swap, and could
    misfire VARIABLE_STAR / BINARY_STAR / the brightening branch of
    SUPERNOVA_CANDIDATE. Every serious survey (ZTF, LSST, ...) keeps its
    light curves per-filter for exactly this reason.

    Deliberately scoped to the delta_mag computation ONLY — the "does this
    position have any prior detection at all" existence check (`history`
    itself, and therefore FIRST_OBSERVATION / UNKNOWN / KNOWN_CATALOG_NEW)
    must stay filter-agnostic: a normal LRGB sequence re-images the same
    field in 3-4 different filters within one session, and a position that
    was only ever detected in, say, L must not be treated as "brand new"
    the moment an R-filtered frame of the same field comes in.

    A historical entry with no "filter" key at all (the API predates this
    field, or the frame that produced it was analyzed before pipeline.py
    started tagging sources with "_filter") never matches — it's excluded
    rather than optimistically assumed to be the same filter, since a wrong
    assumption here is exactly the false-positive this function exists to
    prevent. Returns [] outright when the current source's own filter is
    unknown (`filter_name is None`), for the same reason.
    """
    if filter_name is None:
        return []
    return [src for src in history if src.get("filter") == filter_name]


def _history_mag_epochs(history: list[dict]) -> int:
    """
    How many of these historical detections actually carry a usable magnitude
    — the photometric baseline's real length.

    `len(history)` is not that number: a detection is recorded whether or not
    photometry could calibrate it, so a position observed three times with
    only two measured magnitudes would still clear VARIABILITY_MIN_EPOCHS
    while `_history_mag_scatter()` below had only two values to work with.
    The light-curve VARIABLE_STAR branch gates on a *photometric* baseline
    being long enough to trust its own scatter, so it must count the same
    rows the scatter is computed from.
    """
    return sum(1 for src in history if _extract_mag(src) is not None)


def _history_mag_scatter(history: list[dict]) -> float | None:
    """
    Robust scatter (1-sigma equivalent) of a source's OWN historical
    magnitudes — the baseline against which _classify.py decides whether a
    magnitude change is remarkable *for this particular source*, without
    consulting any catalog's opinion of what the source is.

    Uses the median absolute deviation scaled by 1.4826 (the MAD→sigma
    factor for a normal distribution) rather than a plain standard
    deviation: a light curve's own outlier — one bad epoch through cloud, one
    cosmic ray in the aperture — would inflate an RMS enough to mask the very
    change this number exists to calibrate, while the MAD barely moves.

    Returns None when fewer than two magnitudes are available (a single
    epoch has no scatter to speak of). May legitimately return 0.0 for a
    source whose historical magnitudes are identical — callers must treat
    that as "no measurable scatter" and fall back to an absolute threshold
    (config.DELTA_MAG_ALERT) rather than dividing by it.
    """
    mags = [m for src in history if (m := _extract_mag(src)) is not None]
    if len(mags) < 2:
        return None

    median = statistics.median(mags)
    mad = statistics.median([abs(m - median) for m in mags])
    return 1.4826 * mad
