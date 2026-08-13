"""
modules/finder_chart — Per-source finder/discovery chart generation.

For an anomaly with a resolved `source_id`, builds a small PNG visualizing
every frame that source has ever been detected on, with its position marked
on each, and uploads it to the API (see api_client.upload_source_chart /
observatory-api's source_charts table). The chart is always fully
regenerated from the source's complete track — never patched in place — so
each new epoch simply produces an updated image with one more mark on it.

Three rendering styles. A source with only one detected epoch always gets
"before_after", regardless of anomaly_type — there's no track or blink-strip
to draw from a single point, and "was anything at this exact position a
moment ago" is the informative question for exactly that case. A source
with 2+ epochs picks between "track"/"stamp_strip" by anomaly_type, as
before:

  - "before_after" — any anomaly_type, when the source has exactly one
                    detected epoch so far. A crop of the most recent EARLIER
                    frame of the same object at this exact sky position
                    (nothing expected there yet) next to a crop of the frame
                    the source was actually detected on (circled) — the
                    direct "blink test" for a brand-new single-epoch
                    detection. Falls back to a single "after only" panel,
                    with an explicit note why, if no earlier frame of the
                    object exists yet or it can't be loaded. See
                    _style_before_after.py.
  - "track"       — ASTEROID / COMET / MOVING_UNKNOWN / SPACE_DEBRIS, 2+
                    epochs. One background image (the most recent epoch's
                    own frame) with a colored filled marker at every epoch's
                    true position, connected by a gradient track line with a
                    direction arrowhead. See _style_track.py.
  - "stamp_strip" — everything else (SUPERNOVA_CANDIDATE, UNKNOWN,
                    VARIABLE_STAR, BINARY_STAR, KNOWN_CATALOG_NEW,
                    FIRST_OBSERVATION), 2+ epochs. One small crop per epoch,
                    centred on that epoch's own detected position using that
                    frame's own WCS, each circled and labelled — a classic
                    "blink" strip for a source that isn't expected to move.
                    See _style_stamp_strip.py.

If the source is catalog-matched (e.g. an MPC-identified asteroid, or a
Simbad-known variable/binary star), the chart's title also carries that
designation next to anomaly_type, e.g. "ASTEROID (4 Vesta)" — see
`designation_by_source_id` below. An uncatalogued source's chart is titled
with just its anomaly_type.

The single public entry point is:

    await finder_chart.update_charts_for_sources(
        anomaly_types_by_source_id, designation_by_source_id=None,
    ) -> dict[str, dict[Optional[str], bool]]

It takes every (source_id -> [anomaly_type, ...]) pair at once and fetches
all their tracks via one POST /sources/tracks/batch call, then uploads each
rendered chart individually via POST /sources/{id}/chart — one request per
chart. `designation_by_source_id` is optional and keyed the same way, built
by pipeline.py from the already catalog-matched `sources` list (catalog_name/
catalog_id) — this module never queries a catalog itself.

A source's list of anomaly_types can (and, over its lifetime, often does)
contain more than one distinct value — modules/anomaly_detector/ classifies
a source independently on every frame it appears on, so the same source_id
can collect e.g. an UNKNOWN anomaly on the frame it was first seen and a
MOVING_UNKNOWN once it had moved. Rather than collapsing that list down to a
single style and silently dropping the other classification's evidence,
this module renders and uploads ONE CHART PER DISTINCT STYLE the list
implies — see `_style._group_types_by_style()`.

Best-effort throughout: every failure (missing local archive file, API
error, rendering error) is caught and logged, and only ever downgrades that
one source_id's own result to False — it never raises and never prevents
the other source_ids in the same call from being processed.

Animated GIF companions (config.CHART_GIF_ENABLED, default true): whenever a
source's chart style is "track" or "stamp_strip" (2+ loaded epochs), an
animated GIF is also rendered and uploaded right alongside the static PNG,
as its own chart with its own `style` value: "track_gif" or
"stamp_strip_gif". The GIF is a bonus asset: update_charts_for_sources()'s
own return value reports only the static chart's outcome, and a GIF
render/upload failure is logged and otherwise ignored.

--------------------------------------------------------------------------
Split into one file per chart style, plus shared infrastructure:

  _style.py         style routing: MOVING_TYPES, STYLE_*, _style_for_source,
                     _group_types_by_style — no rendering, no I/O
  _io.py             shared FITS loading, display stretch, plate-scale/
                     stamp-size conversion, PNG/GIF assembly — used by 2+ of
                     the style files below
  _style_track.py        "track" style + its GIF counterpart (_render_track_gif)
  _style_stamp_strip.py  "stamp_strip" style + its GIF counterpart
  _style_before_after.py "before_after" style + the earlier-frame lookup it needs

`__init__.py` itself stays the orchestrator (same convention as
modules/astrometry/'s `solve()`): `_render_charts_for_source()` and
`update_charts_for_sources()` live here, not in a separate file.

This matters for test-mock compatibility, not just style: tests/test_finder_
chart.py patches `_render_track_chart`, `_render_stamp_strip`, and
`_render_track_gif` as BARE attributes directly on this package
(`monkeypatch.setattr(finder_chart, "_render_track_chart", spy)`), to spy on
/replace what `_render_charts_for_source()` calls. A bare-name patch like
that only takes effect if the code doing the lookup resolves the name
through THIS module's own namespace at call time — which is exactly what
happens for a global name referenced by code defined directly in
`__init__.py` (Python resolves it against `__init__.py`'s own `__dict__`,
which is the very same object `finder_chart.<name> = ...` mutates), but
would NOT happen if `_render_charts_for_source()` instead lived in some
other submodule and imported `_render_track_chart` as its own bare name —
patching the package attribute would then leave that submodule's own,
separate global binding untouched. So `_render_track_chart`/
`_render_stamp_strip`/`_render_track_gif`/`_render_stamp_strip_gif`/
`_render_before_after_chart` are all imported here as bare names (`from
._style_track import _render_track_chart, ...`) specifically so the orchestrator
below can call them the same way the pre-split module's own functions
always did. See
`.claude/agent-memory/python-senior-dev/feedback_module_to_package_split.md`
rule 2/4 for the general pattern this follows (same one modules/astrometry/
and modules/catalog_matcher/ already established).

`api_client` is imported here (and re-exported as `finder_chart.api_client`)
for the same reason `modules/anomaly_detector/`'s `__init__.py` does:
`monkeypatch.setattr(finder_chart.api_client, "get_source_tracks_batch",
...)`-style patches mutate the one shared `api_client` module object, so
they take effect regardless of which submodule (here, `_style_before_after.py` for
`get_nearest_frame_before`, or this file for `get_source_tracks_batch`/
`upload_source_chart`) actually performs the call.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import api_client
import config

from ._io import (
    _arcsec_per_pixel,
    _crop_around,
    _fig_to_png_bytes,
    _load_frame,
    _local_fits_path,
    _pngs_to_gif,
    _stamp_half_size_px,
    _stretch,
)
from ._style import (
    MOVING_TYPES,
    STYLE_BEFORE_AFTER,
    STYLE_STAMP_STRIP,
    STYLE_STAMP_STRIP_GIF,
    STYLE_TRACK,
    STYLE_TRACK_GIF,
    _dedupe_preserve_order,
    _group_types_by_style,
    _style_for_anomaly_type,
    _style_for_source,
)
from ._style_before_after import (
    _fetch_and_load_earlier_frame,
    _get_earlier_frame_epoch,
    _render_before_after_chart,
)
from ._style_stamp_strip import _grid_layout, _render_stamp_strip, _render_stamp_strip_gif
from ._style_track import (
    _angular_separation_arcsec,
    _epoch_colors,
    _format_angular_shift,
    _format_short_time,
    _label_positions,
    _parse_delta_hours,
    _render_track_chart,
    _render_track_gif,
)

logger = logging.getLogger(__name__)

__all__ = ["update_charts_for_sources"]


# ---------------------------------------------------------------------------
# Rendering (local — no API calls). Caps to CHART_MAX_EPOCHS, loads whatever
# epochs are still present in the local archive, and renders the PNG.
# Failure at any step logs and returns None rather than raising, so a single
# source_id's rendering trouble never affects any other source_id in the
# same update_charts_for_sources() batch.
# ---------------------------------------------------------------------------

async def _render_charts_for_source(
    source_id: str, epochs: list[dict], anomaly_types: list[Optional[str]], designation: Optional[str] = None,
    earlier_frame_cache: Optional[dict] = None,
) -> list[tuple[str, list[Optional[str]], bytes, int, Optional[tuple[str, bytes]]]]:
    """
    Renders one chart PER DISTINCT STYLE implied by `anomaly_types` — see
    `_style._group_types_by_style()`. Most sources only ever have one style's
    worth of anomaly_types and get exactly one chart back; a source with
    e.g. both a MOVING_UNKNOWN and an UNKNOWN anomaly gets two.

    Returns a list of (style, anomaly_types_covered_by_this_style,
    png_bytes, frame_count, gif) — one entry per distinct style, in
    first-encountered order. `gif` is `(gif_style, gif_bytes)` when
    CHART_GIF_ENABLED and this style is STYLE_TRACK/STYLE_STAMP_STRIP (2+
    loaded epochs), else `None`: disabled, STYLE_BEFORE_AFTER (nothing to
    animate), or the GIF render itself failed (logged, not fatal to the
    style's own PNG entry). Empty list if no epoch could be loaded from the
    local archive at all — the epoch-loading failure is source-wide, not
    per-style.

    `anomaly_types`: the (already deduplicated) anomaly_types requesting a
    chart for this source. `None` is a valid entry — a chart requested
    directly by source_id, with no anomaly behind it at all (see worker.py's
    `_run_charts_task()`) — handled by `_style_for_anomaly_type()` the same
    as any other non-MOVING_TYPES value, just without an anomaly_type in
    that chart's title.

    `designation`: the source's catalog identity (e.g. an MPC name for an
    ASTEROID/COMET, or a Simbad main_id for a known VARIABLE_STAR/BINARY_STAR),
    when the underlying source is catalog-matched at all — shown alongside
    each chart's own representative anomaly_type as its title, e.g.
    "ASTEROID (4 Vesta)". None for an uncatalogued source, in which case the
    chart is titled with just its representative anomaly_type. When both are
    None (uncatalogued source, no anomaly), the chart has no title at all.

    `earlier_frame_cache`: shared across one update_charts_for_sources()
    call — see `_before_after._get_earlier_frame_epoch()`. Only ever
    read/written when this source ends up needing the "before_after" style
    (exactly one loaded epoch); unused (and safe to omit) otherwise.
    """
    # Epochs come back chronologically ordered (oldest first) — keep the
    # most recent CHART_MAX_EPOCHS so the image size and the number of local
    # FITS files opened stay bounded for a source with a very long history.
    if len(epochs) > config.CHART_MAX_EPOCHS:
        logger.info(
            "finder_chart: source_id=%s has %d epochs — keeping the most recent %d",
            source_id, len(epochs), config.CHART_MAX_EPOCHS,
        )
        epochs = epochs[-config.CHART_MAX_EPOCHS:]

    loaded: list[dict[str, Any]] = []
    for epoch in epochs:
        path = _local_fits_path(epoch)
        frame = _load_frame(path)
        if frame is None:
            logger.debug("finder_chart: skipping epoch, cannot load %s", path)
            continue
        data, wcs = frame
        loaded.append({**epoch, "data": data, "wcs": wcs})

    if not loaded:
        logger.warning(
            "finder_chart: none of %d epoch(s) for source_id=%s could be loaded from the local archive",
            len(epochs), source_id,
        )
        return []

    style_groups = _group_types_by_style(anomaly_types, len(loaded))

    rendered: list[tuple[str, list[Optional[str]], bytes, int, Optional[tuple[str, bytes]]]] = []
    for style, types_in_group in style_groups.items():
        # Prefer a non-None type as the chart's representative/title — a
        # None mixed in with a real type (unusual, but possible if the same
        # source is ever requested both via an anomaly and via the
        # source-only /ui/sources/generate-charts path in one task) should
        # still show the real classification, not fall back to the bare
        # designation.
        representative = next((t for t in types_in_group if t), types_in_group[0] if types_in_group else None)
        if representative and designation:
            label = f"{representative} ({designation})"
        else:
            label = representative or designation

        try:
            if style == STYLE_BEFORE_AFTER:
                current_ep = loaded[-1]
                before_ep, missing_reason = await _get_earlier_frame_epoch(
                    current_ep, earlier_frame_cache if earlier_frame_cache is not None else {},
                )
                png_bytes = _render_before_after_chart(current_ep, before_ep, label=label, missing_reason=missing_reason)
                frame_count = 1 + (1 if before_ep else 0)
            elif style == STYLE_TRACK:
                png_bytes = _render_track_chart(loaded, label=label)
                frame_count = len(loaded)
            else:
                png_bytes = _render_stamp_strip(loaded, label=label)
                frame_count = len(loaded)
        except Exception as exc:
            logger.warning("finder_chart: rendering (%s) failed for source_id=%s: %s", style, source_id, exc)
            continue

        # Animated counterpart, best-effort: a GIF rendering failure only
        # drops the GIF (`gif` stays None below) — it never invalidates the
        # static chart already rendered above. STYLE_BEFORE_AFTER never
        # reaches here (see this function's docstring).
        gif: Optional[tuple[str, bytes]] = None
        if config.CHART_GIF_ENABLED and style in (STYLE_TRACK, STYLE_STAMP_STRIP):
            gif_style = STYLE_TRACK_GIF if style == STYLE_TRACK else STYLE_STAMP_STRIP_GIF
            try:
                gif_bytes = (
                    _render_track_gif(loaded, label=label) if style == STYLE_TRACK
                    else _render_stamp_strip_gif(loaded, label=label)
                )
                gif = (gif_style, gif_bytes)
            except Exception as exc:
                logger.warning(
                    "finder_chart: GIF rendering (%s) failed for source_id=%s: %s", gif_style, source_id, exc,
                )

        rendered.append((style, types_in_group, png_bytes, frame_count, gif))

    return rendered


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def update_charts_for_sources(
    anomaly_types_by_source_id: dict[str, list[Optional[str]]],
    designation_by_source_id: Optional[dict[str, str]] = None,
) -> dict[str, dict[Optional[str], bool]]:
    """
    (Re)generate and upload finder charts for every given source_id,
    reflecting each source's complete track, after a batch of anomalies was
    just detected/selected.

    Fetches all tracks in a single POST /sources/tracks/batch call, renders
    each chart locally, then uploads each rendered chart individually via
    POST /sources/{id}/chart — one request per chart.

    Parameters
    ----------
    anomaly_types_by_source_id:
        Maps each `sources.id` with a just-detected/selected anomaly (from
        `_source_id` — see pipeline.py Step 7) to the list of anomaly_types
        requesting a chart for it. Together with how many epochs the source
        actually has, this decides which chart style(s) get rendered — see
        `_style._group_types_by_style()`: exactly one epoch → a single
        "before_after" chart regardless of anomaly_type; 2+ epochs → one
        "track" chart if any type is a MOVING_TYPES member, one
        "stamp_strip" chart if any isn't, or both if the list has a mix of
        the two.
        A list entry of None is valid and means "no anomaly behind this
        chart at all" — a chart requested directly for a source (worker.py's
        `_run_charts_task()` passes payload.anomaly_type through verbatim,
        and observatory-api's `/ui/sources/generate-charts` never sets it).
        Handled the same as any other non-MOVING_TYPES anomaly_type, just
        without an anomaly_type in that chart's title. Duplicate entries in
        a source's list are harmless (deduplicated internally) — callers
        don't need to pre-deduplicate.
    designation_by_source_id:
        Optional. Maps a subset of the same source_ids to their resolved
        catalog identity (e.g. the MPC designation for an ASTEROID/COMET, or
        a Simbad main_id for a known VARIABLE_STAR/BINARY_STAR) — shown next
        to each chart's own representative anomaly_type as its title, e.g.
        "ASTEROID (4 Vesta)". A source_id absent from this dict (including
        when the dict itself is omitted) gets charts titled with just their
        representative anomaly_type — the normal case for an uncatalogued
        source, which has no designation to show.

    Returns
    -------
    dict[str, dict[Optional[str], bool]]
        One outer entry per key in `anomaly_types_by_source_id`, one inner
        entry per (deduplicated) anomaly_type in that source's list: True if
        the chart covering that type's style was rendered and uploaded,
        False if disabled, no usable epochs were found, rendering failed, or
        the upload was rejected/failed. Two types that resolve to the SAME
        style (e.g. two different MOVING_TYPES members) share that style's
        single upload result. Never raises.

        When CHART_GIF_ENABLED and a style is STYLE_TRACK/STYLE_STAMP_STRIP,
        an animated GIF ("track_gif"/"stamp_strip_gif") is also rendered and
        uploaded alongside that style's static PNG — but this return value
        deliberately reports only the static chart's own outcome. The GIF is
        a bonus asset: its own render/upload failure is logged and otherwise
        ignored, and must never downgrade a `True` this function already has
        for that anomaly_type's real (PNG) chart.
    """
    if not anomaly_types_by_source_id:
        return {}

    # Deduplicate each source's own type list up front — every downstream
    # step (grouping, results dict) assumes no repeats.
    types_by_source_id: dict[str, list[Optional[str]]] = {
        source_id: _dedupe_preserve_order(types)
        for source_id, types in anomaly_types_by_source_id.items()
    }

    if not config.CHART_ENABLED:
        return {
            source_id: {t: False for t in types}
            for source_id, types in types_by_source_id.items()
        }

    designation_by_source_id = designation_by_source_id or {}
    source_ids = list(types_by_source_id.keys())

    try:
        tracks = await api_client.get_source_tracks_batch(source_ids)
    except Exception as exc:
        logger.warning("finder_chart: could not fetch tracks batch for %d source(s): %s", len(source_ids), exc)
        tracks = {}

    results: dict[str, dict[Optional[str], bool]] = {
        source_id: {t: False for t in types}
        for source_id, types in types_by_source_id.items()
    }
    # Shared across every source in this call — see
    # _before_after._get_earlier_frame_epoch() for why this collapses to at
    # most one extra API call regardless of how many sources end up needing
    # the "before_after" style.
    earlier_frame_cache: dict[tuple[Any, Any], tuple[Optional[dict], Optional[str]]] = {}

    for source_id in source_ids:
        epochs = tracks.get(source_id) or []
        if not epochs:
            logger.debug("finder_chart: no epochs for source_id=%s — skipping", source_id)
            continue

        rendered = await _render_charts_for_source(
            source_id, epochs, types_by_source_id[source_id],
            designation_by_source_id.get(source_id),
            earlier_frame_cache,
        )

        for style, types_in_group, png_bytes, frame_count, gif in rendered:
            try:
                ok = await api_client.upload_source_chart(source_id, png_bytes, style, frame_count)
            except Exception as exc:
                logger.warning(
                    "finder_chart: upload failed for source_id=%s style=%s: %s", source_id, style, exc,
                )
                ok = False

            for t in types_in_group:
                results[source_id][t] = ok

            if gif is not None:
                gif_style, gif_bytes = gif
                try:
                    gif_ok = await api_client.upload_source_chart(source_id, gif_bytes, gif_style, frame_count)
                except Exception as exc:
                    logger.warning(
                        "finder_chart: GIF upload failed for source_id=%s style=%s: %s",
                        source_id, gif_style, exc,
                    )
                    gif_ok = False
                if not gif_ok:
                    # Deliberately not reflected in `results` — see this
                    # function's docstring: the GIF is a bonus asset and its
                    # failure must not downgrade the PNG's own outcome above.
                    logger.warning(
                        "finder_chart: GIF chart (%s) not uploaded for source_id=%s", gif_style, source_id,
                    )

    return results
