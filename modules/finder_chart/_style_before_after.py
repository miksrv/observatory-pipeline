"""
modules/finder_chart/_style_before_after.py — "before_after" chart style: the
fallback for any source with only one detected epoch so far, regardless of
anomaly_type — there's no track/blink-strip to draw from a single point.

A crop of the most recent EARLIER frame of the same object at this exact sky
position (nothing expected there yet) next to a crop of the frame the source
was actually detected on (circled) — see _render_before_after_chart()'s own
docstring for the full description, and the package's __init__.py docstring
for how/when this style is chosen.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from ._io import (
    _arcsec_per_pixel,
    _crop_around,
    _fig_to_png_bytes,
    _load_frame,
    _local_fits_path,
    _split_label_designation,
    _stamp_half_size_px,
    _stretch,
)
import matplotlib.pyplot as plt

import api_client

logger = logging.getLogger(__name__)

# Colors for the "before_after" style — the dashed grey BEFORE circle marks
# "look here, nothing expected"; the solid red AFTER circle matches the
# marker/circle color every other style in this module uses.
_BEFORE_AFTER_BEFORE_COLOR = "#999999"
_BEFORE_AFTER_AFTER_COLOR = "#ff5050"


def _render_before_after_chart(
    current_ep: dict, before_ep: Optional[dict], label: Optional[str] = None,
    missing_reason: Optional[str] = None,
) -> bytes:
    """
    2-panel chart for a source detected on only one epoch so far: a crop of
    the most recent EARLIER frame of the same object at this exact sky
    position (nothing expected there yet) next to a crop of the frame the
    source was actually detected on (circled). Falls back to a single
    "after only" panel — with `missing_reason` shown as an explicit note —
    if `before_ep` is None (no earlier frame of the object exists yet, or it
    couldn't be loaded).

    Both panels are centred on the SAME (ra, dec) — current_ep's own
    detected position — on purpose: a single-occurrence source has exactly
    one detected position, so there's no "each panel's own position" to
    begin with; the whole point of the comparison is "was anything at this
    one, fixed sky position before vs. after". That shared coordinate is
    shown once, in the figure's overall title, rather than repeated
    identically under both panels — see this package's docstring.

    `label`, if given (e.g. "MOVING_UNKNOWN (2014 RY1)" — the anomaly_type
    plus its resolved catalog designation, see update_charts_for_sources()),
    is shown as part of the figure's overall title.
    """
    n_panels = 2 if before_ep else 1
    fig, axes = plt.subplots(1, n_panels, figsize=(4.6 * n_panels, 4.7), dpi=120)
    axes = [axes] if n_panels == 1 else list(axes)

    if before_ep:
        ax_before = axes[0]
        try:
            half_px = _stamp_half_size_px(before_ep["wcs"])
            crop, (cx, cy) = _crop_around(
                before_ep["data"], before_ep["wcs"], current_ep["ra"], current_ep["dec"], half_px,
            )
            ax_before.imshow(_stretch(crop), cmap="gray", origin="lower")
            r = max(6.0, 10.0 / _arcsec_per_pixel(before_ep["wcs"]))
            ax_before.add_patch(plt.Circle((cx, cy), radius=r, edgecolor=_BEFORE_AFTER_BEFORE_COLOR,
                                            facecolor="none", linewidth=1.2, linestyle="--"))
        except Exception as exc:
            logger.debug("finder_chart: before_after BEFORE crop failed: %s", exc)
            ax_before.text(0.5, 0.5, "n/a", ha="center", va="center", transform=ax_before.transAxes)
        ax_before.set_title(f"BEFORE: {before_ep.get('obs_time', '')}\n(nothing expected here)",
                             fontsize=9, color=_BEFORE_AFTER_BEFORE_COLOR)
        ax_before.set_xticks([]); ax_before.set_yticks([])

    ax_after = axes[-1]
    try:
        half_px = _stamp_half_size_px(current_ep["wcs"])
        crop, (cx, cy) = _crop_around(
            current_ep["data"], current_ep["wcs"], current_ep["ra"], current_ep["dec"], half_px,
        )
        ax_after.imshow(_stretch(crop), cmap="gray", origin="lower")
        r = max(6.0, 10.0 / _arcsec_per_pixel(current_ep["wcs"]))
        ax_after.add_patch(plt.Circle((cx, cy), radius=r, edgecolor=_BEFORE_AFTER_AFTER_COLOR,
                                       facecolor="none", linewidth=1.8))
    except Exception as exc:
        logger.debug("finder_chart: before_after AFTER crop failed: %s", exc)
        ax_after.text(0.5, 0.5, "n/a", ha="center", va="center", transform=ax_after.transAxes)
    mag_txt = f"  mag {current_ep['mag']:.2f}" if current_ep.get("mag") is not None else ""
    ax_after.set_title(f"AFTER: {current_ep.get('obs_time', '')}{mag_txt}\n(object detected)",
                        fontsize=9, color=_BEFORE_AFTER_AFTER_COLOR)
    ax_after.set_xticks([]); ax_after.set_yticks([])

    coord_line = f"Fixed query position: RA {current_ep['ra']:.4f}°  Dec {current_ep['dec']:.4f}°"

    # Same convention as _style_stamp_strip_gif.py: the anomaly_type and its
    # "(designation)" (if any) each get their own header line, ahead of the
    # coordinate line.
    header_lines = []
    has_designation = False
    if label:
        anomaly_type, designation = _split_label_designation(label)
        header_lines.append(anomaly_type)
        if designation:
            header_lines.append(designation)
            has_designation = True
    header_lines.append(coord_line)
    fig.suptitle("\n".join(header_lines), fontsize=10)

    if missing_reason:
        fig.text(0.5, 0.01, missing_reason, ha="center", fontsize=7.5, color=_BEFORE_AFTER_BEFORE_COLOR)
    fig.tight_layout(rect=(0, 0.04, 1, 0.86 if has_designation else 0.90))

    return _fig_to_png_bytes(fig)


async def _fetch_and_load_earlier_frame(
    object_name: Optional[str], before_obs_time: Optional[str],
) -> tuple[Optional[dict], Optional[str]]:
    """
    Query GET /frames/nearest-before for the most recent frame of
    `object_name` strictly before `before_obs_time`, then load it locally.

    Returns (loaded_epoch_or_None, missing_reason_or_None) — the loaded dict
    has "data"/"wcs"/"obs_time", ready for _render_before_after_chart()'s
    BEFORE panel. `missing_reason` is always set when the loaded dict is
    None, for the chart's own explicit "why there's no before panel" note.
    """
    if not object_name or not before_obs_time:
        return None, "current epoch has no object/obs_time to query an earlier frame with"

    try:
        frame_info = await api_client.get_nearest_frame_before(object_name, before_obs_time)
    except Exception as exc:
        logger.warning("finder_chart: GET /frames/nearest-before failed for object=%s: %s", object_name, exc)
        return None, f"could not query for an earlier frame of {object_name}"

    if frame_info is None:
        # get_nearest_frame_before() can't distinguish "queried fine, no
        # earlier frame exists" from "the query itself failed" (same as
        # get_source_tracks_batch's per-source absence) — the caption stays
        # accurate either way.
        return None, f"no earlier frame of {object_name} available (none exists yet, or the lookup failed)"

    path = _local_fits_path({"object": frame_info.get("object") or object_name, "filename": frame_info.get("filename")})
    frame = _load_frame(path)
    if frame is None:
        return None, f"earlier frame {frame_info.get('filename')} of {object_name} exists but could not be loaded locally"

    data, wcs = frame
    return {"data": data, "wcs": wcs, "obs_time": frame_info.get("obs_time", "")}, None


async def _get_earlier_frame_epoch(
    current_ep: dict, cache: dict[tuple[Any, Any], tuple[Optional[dict], Optional[str]]],
) -> tuple[Optional[dict], Optional[str]]:
    """
    Cached wrapper around _fetch_and_load_earlier_frame(), keyed by
    (object, obs_time). Every single-occurrence source rendered within one
    update_charts_for_sources() call shares the exact same object and
    current obs_time — they're all anomalies from the one frame just
    processed — so this collapses to at most one GET /frames/nearest-before
    call per update_charts_for_sources() call, not one per source.
    """
    cache_key = (current_ep.get("object"), current_ep.get("obs_time"))
    if cache_key not in cache:
        cache[cache_key] = await _fetch_and_load_earlier_frame(*cache_key)
    return cache[cache_key]
