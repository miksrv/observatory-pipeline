"""
modules/finder_chart/_style.py — chart style routing.

Decides WHICH of the three rendering styles (see the package's own
__init__.py docstring — track / stamp_strip / before_after) a source's
anomaly_type(s) and loaded-epoch count resolve to. Pure routing logic — no
rendering, no I/O, no API calls — so it can be unit-tested (and read)
independently of everything that actually draws a chart.
"""
from __future__ import annotations

from typing import Optional

# Anomaly types for which the source is expected to have moved between
# epochs — see modules/anomaly_detector/'s classification table.
MOVING_TYPES = frozenset({"ASTEROID", "COMET", "MOVING_UNKNOWN", "SPACE_DEBRIS"})

STYLE_TRACK = "track"
STYLE_STAMP_STRIP = "stamp_strip"
STYLE_BEFORE_AFTER = "before_after"

# Animated counterparts of STYLE_TRACK / STYLE_STAMP_STRIP — see
# _track._render_track_gif() / _stamp_strip._render_stamp_strip_gif(). There
# is no "_gif" style for STYLE_BEFORE_AFTER: a single-occurrence source has
# only one (or two, counting the earlier-frame lookup) still image to show,
# and a 1-2 frame "animation" carries no more information than the existing
# static side-by-side panels already do.
STYLE_TRACK_GIF = "track_gif"
STYLE_STAMP_STRIP_GIF = "stamp_strip_gif"


def _style_for_anomaly_type(anomaly_type: Optional[str]) -> str:
    """
    Pick the chart style for the anomaly type that just triggered a chart
    update. `anomaly_type=None` — a chart requested directly for a source
    with no anomaly at all (observatory-api's `/ui/sources/generate-charts`,
    which sends only `source_id` — see worker.py's `_run_charts_task()`) —
    falls through the same way any other non-MOVING_TYPES value does: there
    is no motion evidence to justify "track", so "stamp_strip" is the
    correct, conservative default for an object with no known reason to move.
    """
    return STYLE_TRACK if anomaly_type in MOVING_TYPES else STYLE_STAMP_STRIP


def _style_for_source(anomaly_type: Optional[str], n_epochs: int) -> str:
    """
    Pick the chart style for a source with `n_epochs` loaded epochs. A
    source detected on only one epoch so far gets STYLE_BEFORE_AFTER
    regardless of anomaly_type — there's no track/blink-strip to draw from a
    single point. 2+ epochs use _style_for_anomaly_type()'s existing
    anomaly_type-based routing, unchanged (including its None handling).
    """
    if n_epochs <= 1:
        return STYLE_BEFORE_AFTER
    return _style_for_anomaly_type(anomaly_type)


def _dedupe_preserve_order(values: list[Optional[str]]) -> list[Optional[str]]:
    """Deduplicate a list, keeping only the first occurrence of each value."""
    seen: set[Optional[str]] = set()
    out: list[Optional[str]] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _group_types_by_style(
    anomaly_types: list[Optional[str]], n_epochs: int,
) -> dict[str, list[Optional[str]]]:
    """
    Partition a source's (already deduplicated) anomaly_types into the
    distinct chart styles they imply, so update_charts_for_sources() can
    render one chart per style rather than collapsing everything down to a
    single arbitrary one.

    With only 1 loaded epoch every type collapses to STYLE_BEFORE_AFTER
    regardless (see _style_for_source()) — a single group holding every
    input type. With 2+ epochs, each type is routed independently via
    _style_for_anomaly_type(), so e.g. ["MOVING_UNKNOWN", "UNKNOWN"]
    produces two groups: {"track": ["MOVING_UNKNOWN"], "stamp_strip":
    ["UNKNOWN"]}. Preserves `anomaly_types`' own order within each group's
    list, and returns groups in first-encountered order — both matter for
    picking a deterministic, informative representative type for the chart
    title (see __init__.py's _render_charts_for_source()).
    """
    if n_epochs <= 1:
        return {STYLE_BEFORE_AFTER: list(anomaly_types)}

    groups: dict[str, list[Optional[str]]] = {}
    for t in anomaly_types:
        groups.setdefault(_style_for_anomaly_type(t), []).append(t)
    return groups
