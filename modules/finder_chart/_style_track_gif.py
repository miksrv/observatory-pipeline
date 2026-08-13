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

Each frame keeps the cumulative marker trail (epochs 1..k, not just epoch k
alone) so the "track" identity — the accumulating path, not just a single
blink — stays visible within the animation itself, each position projected
into THAT frame's own WCS (a marker's sky position is fixed; its pixel
position within a shared window can still differ slightly frame to frame,
since each is a genuinely different plate solve). Colors are a fixed
cool→warm gradient over ALL epochs (`_style_track._epoch_colors()`, called
once with the full count — not per-frame), so a given epoch keeps the same
color across every frame it appears in.

A short single-line caption (date/time + magnitude of THAT frame's own
epoch) replaces the static chart's detailed legend — fixed height, doesn't
grow with epoch count. The overall `label` (e.g. "ASTEROID (4 Vesta)") is
shown identically on every frame via one unchanging fig.suptitle().
"""
from __future__ import annotations

import logging
from typing import Optional

from ._io import _arcsec_per_pixel, _fig_to_png_bytes, _pngs_to_gif, _stretch
from ._style_track import _epoch_colors
import matplotlib.pyplot as plt
import numpy as np
from astropy.coordinates import SkyCoord

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
_WINDOW_MIN_FACTOR = 3.0


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
) -> bytes:
    """Render animation frame k (1-indexed): epoch k's own real pixel data,
    cropped to the shared window, with the cumulative marker trail for
    epochs 1..k projected into epoch k's own WCS."""
    current = loaded_epochs[k - 1]

    try:
        crop, off_x, off_y = _crop_to_window(current, center_ra, center_dec, half_size_arcsec)
        image = _stretch(crop)
    except Exception as exc:
        logger.debug("finder_chart: track_gif frame %d crop failed: %s", k, exc)
        crop, off_x, off_y, image = None, 0.0, 0.0, None

    fig, ax = plt.subplots(figsize=_FIGSIZE, dpi=_DPI)

    if image is not None:
        ax.imshow(image, cmap="gray", origin="lower")

        # Project every epoch seen so far (1..k) into THIS frame's own WCS —
        # each is a genuinely different exposure, so the same sky position
        # can land at a slightly different pixel in each one.
        xs: list[float] = []
        ys: list[float] = []
        for ep in loaded_epochs[:k]:
            x, y = current["wcs"].world_to_pixel(SkyCoord(ra=ep["ra"], dec=ep["dec"], unit="deg"))
            xs.append(float(x) - off_x)
            ys.append(float(y) - off_y)

        for i in range(1, len(xs)):
            ax.plot([xs[i - 1], xs[i]], [ys[i - 1], ys[i]], "-",
                    color=colors[i], linewidth=1.2, alpha=0.6, zorder=2)
        if len(xs) >= 2:
            ax.annotate(
                "", xy=(xs[-1], ys[-1]), xytext=(xs[-2], ys[-2]),
                arrowprops=dict(arrowstyle="-|>", color=colors[len(xs) - 1], lw=1.5, mutation_scale=12),
                zorder=2,
            )
        for i, (x, y) in enumerate(zip(xs, ys)):
            ax.plot(x, y, "o", color=colors[i], markeredgecolor="white",
                    markeredgewidth=0.5, markersize=6, zorder=5)
    else:
        ax.text(0.5, 0.5, "n/a", ha="center", va="center", transform=ax.transAxes)

    # Short, fixed-height per-frame caption — replaces the static chart's
    # bottom legend block, which doesn't fit a fixed canvas.
    caption = current.get("obs_time", "")
    if current.get("mag") is not None:
        caption += f"   mag {current['mag']:.1f}"
    ax.set_title(caption, fontsize=8)
    ax.set_xticks([])
    ax.set_yticks([])

    # subplots_adjust with an explicit top, not tight_layout(rect=...) — tight_layout
    # pads generously around a suptitle to guarantee no overlap, which left a large
    # blank gap between the suptitle and the per-frame caption right above the image
    # (real complaint, 2026-08-13). A fixed top fraction (same convention
    # _style_track.py's _render_track_chart() already uses) keeps the gap small and
    # predictable instead.
    if label:
        fig.suptitle(label, fontsize=10)
    fig.subplots_adjust(left=0.02, right=0.98, bottom=0.04, top=0.90 if label else 0.97)

    return _fig_to_png_bytes(fig)


def _render_track_gif(loaded_epochs: list[dict], label: Optional[str] = None) -> bytes:
    """
    Cumulative-reveal "blink" animation for a moving source: frame k shows
    epoch k's own real pixel data (cropped to a fixed sky window shared by
    every frame — see _sky_window()), with a marker + connecting line for
    every epoch from 1 to k, each projected into epoch k's OWN WCS so the
    trail lines up correctly even though every frame's underlying image is
    a different real exposure.

    Deliberately independent of _style_track.py's _render_track_chart() —
    see this module's own docstring for the full rationale.
    """
    n = len(loaded_epochs)
    center_ra, center_dec, half_size_arcsec = _sky_window(loaded_epochs)
    colors = _epoch_colors(n)

    frames = [
        _render_one_track_gif_frame(loaded_epochs, k, center_ra, center_dec, half_size_arcsec, colors, label)
        for k in range(1, n + 1)
    ]
    return _pngs_to_gif(frames, config.CHART_GIF_FRAME_DURATION_MS)
