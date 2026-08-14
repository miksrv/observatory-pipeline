"""
modules/finder_chart/_style_stamp_strip_gif.py — animated "stamp_strip_gif"
companion to the static "stamp_strip" chart (_style_stamp_strip.py).

Deliberately NOT a thin wrapper around _style_stamp_strip.py's
_render_stamp_strip() (the way this used to work) — that reused this file's
own grid-layout machinery (`_grid_layout()`) for what's always a 1x1 grid
when called with a single-epoch list, coupling this animation's per-frame
rendering to the static chart's own implementation. Each frame here is its
own standalone single-panel render instead: a crop centred on that epoch's
own detected position (using that epoch's own WCS), circled, captioned with
its timestamp/magnitude/RA/Dec — visually the same as one cell of the
static "stamp_strip" grid, but implemented independently so this file's own
"blink" animation (per-frame styling, timing, whatever else) can be tuned
without touching _style_stamp_strip.py's static multi-panel rendering at
all, and vice versa.
"""
from __future__ import annotations

import logging
from typing import Optional

from ._io import (
    _arcsec_per_pixel,
    _crop_around,
    _fig_to_png_bytes,
    _pngs_to_gif,
    _split_label_designation,
    _stamp_half_size_px,
    _stretch,
)
import matplotlib.pyplot as plt

import config

logger = logging.getLogger(__name__)

# Fixed per-frame figure geometry — a single-panel size, independent of
# _style_stamp_strip.py's own cell-size constant so the two can diverge.
_FIGSIZE = (3.0, 3.6)
_DPI = 120

# See _io.py's _split_label_designation() — splits a label built by
# update_charts_for_sources() into its anomaly_type and "(designation)"
# parts so each can go on its own line, rather than either wrapping
# mid-word at a fixed character width or overflowing this frame's narrow
# 3.0" canvas (real complaint, 2026-08-14).
_LABEL_FONTSIZE = 8
_CAPTION_FONTSIZE = 7.5

# Same fig.suptitle() + ax.set_title() pair as _style_stamp_strip.py's
# static grid, but positioned via subplots_adjust(top=...) rather than
# tight_layout(rect=...) — same reason as _style_track_gif.py's own
# per-frame caption: tight_layout pads generously around a suptitle to
# guarantee no overlap, which leaves a large blank gap between the label
# and the caption right below it (real complaint, 2026-08-14). An explicit
# top fraction keeps that gap small and predictable instead — a two-line
# label (classification + designation) needs a bit more headroom than a
# bare one-line anomaly_type.
_TOP_NO_LABEL = 0.92
_TOP_LABEL_ONE_LINE = 0.90
_TOP_LABEL_TWO_LINES = 0.85


def _render_one_stamp_gif_frame(ep: dict, label: Optional[str] = None) -> bytes:
    """
    Render a single "blink" frame for one epoch: a crop centred on that
    epoch's own detected position (its own WCS), circled, captioned with
    timestamp/magnitude/RA/Dec — the per-frame counterpart of one cell of
    _style_stamp_strip.py's static grid, but drawn independently here.
    """
    fig, ax = plt.subplots(figsize=_FIGSIZE, dpi=_DPI)

    wcs = ep["wcs"]
    half_px = _stamp_half_size_px(wcs)
    try:
        crop, (cx, cy) = _crop_around(ep["data"], wcs, ep["ra"], ep["dec"], half_px)
        ax.imshow(_stretch(crop), cmap="gray", origin="lower")
        circle_radius_px = max(6.0, 8.0 / _arcsec_per_pixel(wcs))
        ax.add_patch(plt.Circle((cx, cy), radius=circle_radius_px,
                                 edgecolor="#ff5050", facecolor="none", linewidth=1.5))
    except Exception as exc:
        logger.debug("finder_chart: stamp_strip_gif crop failed for %s: %s", ep.get("filename"), exc)
        ax.text(0.5, 0.5, "n/a", ha="center", va="center", transform=ax.transAxes)

    ax.set_xticks([])
    ax.set_yticks([])

    # Date/time and magnitude share one line (space-separated, same
    # convention as _style_track_gif.py's per-frame caption) rather than
    # magnitude getting its own line — keeps the caption to two lines
    # instead of three.
    caption = ep.get("obs_time", "")
    if ep.get("mag") is not None:
        caption += f"   mag {ep['mag']:.2f}"
    caption += f"\nRA {ep['ra']:.4f}°  Dec {ep['dec']:.4f}°"
    ax.set_title(caption, fontsize=_CAPTION_FONTSIZE)

    top = _TOP_NO_LABEL
    if label:
        # Split "TYPE (designation)" onto two lines; a bare "TYPE" (no
        # designation) stays on one.
        anomaly_type, designation = _split_label_designation(label)
        if designation:
            fig.suptitle(f"{anomaly_type}\n{designation}", fontsize=_LABEL_FONTSIZE)
            top = _TOP_LABEL_TWO_LINES
        else:
            fig.suptitle(label, fontsize=_LABEL_FONTSIZE)
            top = _TOP_LABEL_ONE_LINE
    fig.subplots_adjust(left=0.03, right=0.97, bottom=0.001, top=top)

    return _fig_to_png_bytes(fig)


def _render_stamp_strip_gif(loaded_epochs: list[dict], label: Optional[str] = None) -> bytes:
    """
    "Blink" animation for a stationary source: one frame per epoch, each
    independently rendered by _render_one_stamp_gif_frame() — a genuine
    blink through each epoch's own crop, rather than a static side-by-side
    grid.
    """
    frames = [_render_one_stamp_gif_frame(ep, label=label) for ep in loaded_epochs]
    return _pngs_to_gif(frames, config.CHART_GIF_FRAME_DURATION_MS)
