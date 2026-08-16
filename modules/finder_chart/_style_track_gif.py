"""
modules/finder_chart/_style_track_gif.py — animated "track_gif" companion to
the static "track" chart (_style_track.py).

Deliberately NOT a thin wrapper around _style_track.py's _render_track_chart()
(the way this used to work) — two independent problems with that approach:

  1. _render_track_chart()'s own figure height grows with epoch count
     (`6.0 + 0.18 * n`), sized to fit its bottom legend block. Calling it
     once per k in 1..N therefore produced N differently-sized PNGs —
     Pillow composites those onto the FIRST frame's canvas size when
     assembling a GIF (_io._pngs_to_gif()), so the legend (and everything
     below the first frame's own height) got silently cropped on every
     later, taller frame. A GIF needs one fixed canvas shared by every
     frame; this module picks one (`_FIGSIZE`/`_DPI`) up front and never
     varies it, and drops the bottom legend block entirely — there's no
     room for a growing block on a fixed canvas anyway, and scrubbing
     through the animation itself already shows what it listed.
  2. Every frame reused the SAME single background image (still correctly
     "that frame's own most-recent epoch" — that part was already right —
     but the epochs before it were never actually shown, only their
     synthetic dots were). Real survey/discovery "blink comparators" rely
     on the STAR FIELD ITSELF visibly shifting between real exposures —
     that's what actually sells "this moved" to a human eye, not just a
     dot's coordinates changing on an otherwise-unchanging picture. So
     every frame here crops THAT epoch's own real pixel data around a
     common, FIXED sky window (same RA/Dec centre + angular half-size,
     worked out once from every epoch's position — see _sky_window()) —
     the window itself never moves between frames, only what's actually
     projected into it differs, because it really is a different exposure
     each time.

Each frame keeps the cumulative marker trail for every PAST transition
(epochs 1..k-1, not epoch k itself) so the "track" identity — the
accumulating path, not just a single blink — stays visible within the
animation itself, each position projected into THAT frame's own WCS (a
marker's sky position is fixed; its pixel position within a shared window
can still differ slightly frame to frame, since each is a genuinely
different plate solve). Colors are a fixed cool→warm gradient over ALL
epochs (`_style_track._epoch_colors()`, called once with the full count —
not per-frame), so a given epoch keeps the same color across every frame it
appears in.

No circle or marker is ever drawn at the CURRENT epoch's own position (the
real object visible in this frame's own pixel data) — a filled dot or a
circle sized to be visible at a glance otherwise sits right on top of the
very asteroid it's marking (real complaint, 2026-08-13). The frame's newest
segment (epoch k-1 → epoch k) is instead rendered as a short directional
arrow anchored AT epoch k-1's position (the "previous value") and capped
well short of reaching epoch k's position — it shows which way the object
is headed without ever overlapping it. So frame 1 (no prior epoch at all)
shows no arrow; frame 2 shows a short arrow sitting at epoch 1's position,
pointing toward epoch 2 but stopping short of it; and so on for every later
frame — the arrow always trails one step behind the object actually visible
in that frame.

A short, fixed-height, two-line caption (date/time + magnitude, then
coordinates + shift/velocity from the previous epoch) replaces the static
chart's detailed legend — fixed height, doesn't grow with epoch count. The
overall `label` (e.g. "ASTEROID (4 Vesta)") is shown identically on every
frame via one unchanging fig.suptitle().
"""
from __future__ import annotations

import logging
import math
from typing import Optional

from ._io import (
    _arcsec_per_pixel,
    _fig_to_png_bytes,
    _pngs_to_gif,
    _prerotation_delta_deg,
    _rotate_crop,
    _rotate_point_in_crop,
    _split_label_designation,
    _stretch,
)
from ._style_track import (
    _angular_separation_arcsec,
    _epoch_colors,
    _format_shift_and_velocity,
    _parse_delta_hours,
)
import matplotlib.pyplot as plt
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.wcs import WCS

import config

logger = logging.getLogger(__name__)

# Fixed figure geometry — every frame in the GIF shares exactly this canvas;
# unlike _style_track.py's _render_track_chart(), nothing here scales with
# epoch count.
_FIGSIZE = (6.0, 6.3)
_DPI = 120

# Multiplier applied to the epoch cluster's own pixel span (see
# _sky_window()) to leave breathing room around the outermost epochs — same
# 1.6x margin _style_track.py's _render_track_chart() already uses for its
# own crop.
_WINDOW_MARGIN_FACTOR = 1.6
_WINDOW_MIN_FACTOR = 2.5


def _sky_window(loaded_epochs: list[dict]) -> tuple[float, float, float]:
    """
    A single (center_ra, center_dec, half_size_arcsec) window that covers
    every epoch's position with margin, computed ONCE from the full epoch
    set. Every animation frame crops around this same sky window (via its
    own WCS) — the framing never jumps between frames even though each
    frame's own image is a genuinely different exposure.
    """
    reference = loaded_epochs[-1]
    wcs = reference["wcs"]

    xs: list[float] = []
    ys: list[float] = []
    for ep in loaded_epochs:
        x, y = wcs.world_to_pixel(SkyCoord(ra=ep["ra"], dec=ep["dec"], unit="deg"))
        xs.append(float(x))
        ys.append(float(y))

    cluster_cx, cluster_cy = float(np.mean(xs)), float(np.mean(ys))
    cluster_half_span_px = max(
        (max(abs(x - cluster_cx), abs(y - cluster_cy)) for x, y in zip(xs, ys)), default=0.0,
    )

    arcsec_per_px = _arcsec_per_pixel(wcs)
    half_size_arcsec = max(
        _WINDOW_MIN_FACTOR * config.CHART_STAMP_SIZE_ARCSEC,
        cluster_half_span_px * _WINDOW_MARGIN_FACTOR * arcsec_per_px,
    )

    center = wcs.pixel_to_world(cluster_cx, cluster_cy)
    return float(center.ra.deg), float(center.dec.deg), half_size_arcsec


def _crop_to_window(
    ep: dict, center_ra: float, center_dec: float, half_size_arcsec: float,
) -> tuple[np.ndarray, float, float]:
    """
    Crop `ep`'s own pixel data to the shared sky window, using ITS OWN WCS —
    so the crop always covers the same patch of sky even though each
    epoch's own pixel scale/orientation/dimensions can differ.

    Returns (crop, offset_x, offset_y) — the crop's own array, plus the
    pixel offset of the crop's origin within `ep`'s full frame, so a
    position can be translated into crop-local coordinates by subtracting
    (offset_x, offset_y) from its full-frame pixel position.
    """
    wcs = ep["wcs"]
    cx, cy = wcs.world_to_pixel(SkyCoord(ra=center_ra, dec=center_dec, unit="deg"))
    cx, cy = float(cx), float(cy)

    half_px = max(10, int(round(half_size_arcsec / _arcsec_per_pixel(wcs))))
    height, width = ep["data"].shape
    x0 = int(max(0, round(cx - half_px)))
    x1 = int(min(width, round(cx + half_px)))
    y0 = int(max(0, round(cy - half_px)))
    y1 = int(min(height, round(cy + half_px)))

    return ep["data"][y0:y1, x0:x1], float(x0), float(y0)


def _render_one_track_gif_frame(
    loaded_epochs: list[dict], k: int, center_ra: float, center_dec: float,
    half_size_arcsec: float, colors: list[str], label: Optional[str],
    reference_wcs: Optional[WCS] = None,
) -> bytes:
    """Render animation frame k (1-indexed): epoch k's own real pixel data,
    cropped to the shared window, with the cumulative trail for epochs
    1..k-1 plus a short directional arrow stubbed at epoch k-1's position —
    epoch k's own position (the real object visible in this frame) is never
    marked — all projected into epoch k's own WCS.

    reference_wcs: the whole animation's shared reference orientation (see
    _render_track_gif() below, same WCS _sky_window() itself already uses)
    — this frame's crop is coarse-pre-rotated toward it first if epoch k's
    own camera/rotator orientation differs enough
    (config.CHART_PREROTATE_MIN_DEG), so the star field's orientation stays
    stable across the whole animation instead of visibly spinning between
    frames for reasons unrelated to real motion — see CLAUDE.md's "camera
    rotation" discussion.
    """
    current = loaded_epochs[k - 1]

    try:
        crop, off_x, off_y = _crop_to_window(current, center_ra, center_dec, half_size_arcsec)
        delta_deg = _prerotation_delta_deg(current["wcs"], reference_wcs)
        if delta_deg is not None:
            crop = _rotate_crop(crop, delta_deg)
        image = _stretch(crop)
    except Exception as exc:
        logger.debug("finder_chart: track_gif frame %d crop failed: %s", k, exc)
        crop, off_x, off_y, image, delta_deg = None, 0.0, 0.0, None, None

    fig, ax = plt.subplots(figsize=_FIGSIZE, dpi=_DPI)

    if image is not None:
        ax.imshow(image, cmap="gray", origin="lower")

        # Project every epoch seen so far (1..k) into THIS frame's own WCS —
        # each is a genuinely different exposure, so the same sky position
        # can land at a slightly different pixel in each one. Epoch k itself
        # (the last entry, xs[-1]/ys[-1]) is only ever used as a DIRECTION
        # reference below — nothing is ever drawn ON it, since that's the
        # real object visible in this frame's own pixel data.
        xs: list[float] = []
        ys: list[float] = []
        for ep in loaded_epochs[:k]:
            x, y = current["wcs"].world_to_pixel(SkyCoord(ra=ep["ra"], dec=ep["dec"], unit="deg"))
            x, y = float(x) - off_x, float(y) - off_y
            if delta_deg is not None:
                x, y = _rotate_point_in_crop(x, y, crop.shape, delta_deg)
            xs.append(x)
            ys.append(y)

        # Cumulative history trail: plain connecting lines between past
        # positions only (epochs 1..k-1) — never touches epoch k's own
        # position. No circles/dots at any of these either (real complaint,
        # 2026-08-13: a marker sized to be visible at a glance sat right on
        # top of the object it was marking).
        for i in range(1, len(xs) - 1):
            ax.plot([xs[i - 1], xs[i]], [ys[i - 1], ys[i]], "-",
                    color=colors[i], linewidth=1.2, alpha=0.6, zorder=2)

        # Short directional arrow anchored AT the previous position
        # (xs[-2]/ys[-2]), pointing toward the current one but capped well
        # short of reaching it — shows which way the object is headed
        # without ever overlapping it. Replaces the old full-length arrow
        # (whose head used to land exactly on the current object) and the
        # circles entirely.
        if len(xs) >= 2:
            dx, dy = xs[-1] - xs[-2], ys[-1] - ys[-2]
            dist = math.hypot(dx, dy)
            if dist > 1e-6:
                stub_len_px = max(6.0, 10.0 / _arcsec_per_pixel(current["wcs"]))
                arrow_len = min(dist * 0.5, stub_len_px * 3)
                ux, uy = dx / dist, dy / dist
                tip_x, tip_y = xs[-2] + ux * arrow_len, ys[-2] + uy * arrow_len
                ax.annotate(
                    "", xy=(tip_x, tip_y), xytext=(xs[-2], ys[-2]),
                    arrowprops=dict(arrowstyle="-|>", color=colors[-1], lw=1.5, mutation_scale=12),
                    zorder=2,
                )
    else:
        ax.text(0.5, 0.5, "n/a", ha="center", va="center", transform=ax.transAxes)

    # Short, fixed-height (two-line, regardless of k) per-frame caption —
    # replaces the static chart's bottom legend block, which doesn't fit a
    # fixed canvas. Line 1: this epoch's own date/time + magnitude. Line 2:
    # its coordinates, plus — from the second frame on — the angular
    # shift/time-gap/velocity from the *previous* epoch, via the same
    # _format_shift_and_velocity() the static "track" chart's own per-epoch
    # legend line uses (real motion diagnostic, and a quick way to spot a
    # bogus jump between two consecutive frames).
    caption = current.get("obs_time", "")
    if current.get("mag") is not None:
        caption += f"   mag {current['mag']:.1f}"
    coord_line = f"RA {current['ra']:.4f}°  Dec {current['dec']:+.4f}°"
    if k >= 2:
        prev = loaded_epochs[k - 2]
        sep_arcsec = _angular_separation_arcsec(prev["ra"], prev["dec"], current["ra"], current["dec"])
        delta_h = _parse_delta_hours(prev.get("obs_time", ""), current.get("obs_time", ""))
        coord_line += "   " + _format_shift_and_velocity(sep_arcsec, delta_h)
    ax.set_title(f"{caption}\n{coord_line}", fontsize=8)
    ax.set_xticks([])
    ax.set_yticks([])

    # subplots_adjust with an explicit top, not tight_layout(rect=...) — tight_layout
    # pads generously around a suptitle to guarantee no overlap, which left a large
    # blank gap between the suptitle and the per-frame caption right above the image
    # (real complaint, 2026-08-13). A fixed top fraction (same convention
    # _style_track.py's _render_track_chart() already uses) keeps the gap small and
    # predictable instead. The caption is two lines now (date/mag, then
    # coordinates + shift/velocity) regardless of k, so both branches need more
    # headroom than the old single-line caption did — 0.97 clipped the top line
    # off the canvas entirely once a second line was added (real regression
    # caught rendering a preview GIF without a label).
    # Same convention as _style_track.py/_style_stamp_strip_gif.py: the
    # anomaly_type and its "(designation)" (if any) each get their own
    # title line rather than being crammed onto one.
    has_designation = False
    if label:
        anomaly_type, designation = _split_label_designation(label)
        has_designation = designation is not None
        fig.suptitle(f"{anomaly_type}\n{designation}" if designation else label, fontsize=10)

    if not label:
        top = 0.92
    else:
        top = 0.85 if has_designation else 0.90
    fig.subplots_adjust(left=0.02, right=0.98, bottom=0.04, top=top)

    return _fig_to_png_bytes(fig)


def _render_track_gif(loaded_epochs: list[dict], label: Optional[str] = None) -> bytes:
    """
    Cumulative-reveal "blink" animation for a moving source: frame k shows
    epoch k's own real pixel data (cropped to a fixed sky window shared by
    every frame — see _sky_window()), with a connecting line for every past
    epoch from 1 to k-1 plus a short directional arrow stubbed at epoch
    k-1's position — epoch k's own position is deliberately left unmarked,
    since that's the real object visible in this frame's own pixel data —
    each projected into epoch k's OWN WCS so the trail lines up correctly
    even though every frame's underlying image is a different real
    exposure.

    Deliberately independent of _style_track.py's _render_track_chart() —
    see this module's own docstring for the full rationale.
    """
    n = len(loaded_epochs)
    center_ra, center_dec, half_size_arcsec = _sky_window(loaded_epochs)
    colors = _epoch_colors(n)
    # Same reference epoch _sky_window() itself already uses — every frame's
    # crop is coarse-pre-rotated toward this one shared orientation so the
    # star field doesn't visibly spin between frames purely from a
    # camera/rotator orientation difference (see CLAUDE.md's "camera
    # rotation" discussion).
    reference_wcs = loaded_epochs[-1]["wcs"]

    frames = [
        _render_one_track_gif_frame(
            loaded_epochs, k, center_ra, center_dec, half_size_arcsec, colors, label,
            reference_wcs=reference_wcs,
        )
        for k in range(1, n + 1)
    ]
    return _pngs_to_gif(frames, config.CHART_GIF_FRAME_DURATION_MS)
