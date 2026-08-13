"""
modules/finder_chart/_style_stamp_strip.py — "stamp_strip" chart style: a "blink"
strip for stationary anomalies (SUPERNOVA_CANDIDATE, UNKNOWN, VARIABLE_STAR,
BINARY_STAR, KNOWN_CATALOG_NEW, FIRST_OBSERVATION).

One small crop per epoch, centred on that epoch's own detected position
using that frame's own WCS, each circled and labelled with its timestamp,
magnitude, and RA/Dec — see _render_stamp_strip()'s own docstring for the
full description, and the package's __init__.py docstring for how/when this
style is chosen over "track"/"before_after".
"""
from __future__ import annotations

import logging
import math
from typing import Optional

from ._io import _arcsec_per_pixel, _crop_around, _fig_to_png_bytes, _pngs_to_gif, _stamp_half_size_px, _stretch
import matplotlib.pyplot as plt

import config

logger = logging.getLogger(__name__)


def _grid_layout(n: int) -> tuple[int, int]:
    """
    Compute (nrows, ncols) for `n` stamps so the resulting image is as close
    to square as possible. For n <= 3 a single row is fine; beyond that we
    pick ncols = ceil(sqrt(n)) and nrows = ceil(n / ncols).
    """
    if n <= 3:
        return 1, n
    ncols = math.ceil(math.sqrt(n))
    nrows = math.ceil(n / ncols)
    return nrows, ncols


def _render_stamp_strip(loaded_epochs: list[dict], label: Optional[str] = None) -> bytes:
    """
    One small labelled crop per epoch, arranged in a roughly square grid,
    each circled at its own detected position and captioned with its own
    RA/Dec.

    `label`, if given (e.g. "VARIABLE_STAR (TYC 1430-1407-1)" — the
    anomaly_type plus its resolved catalog designation, see
    update_charts_for_sources()), is shown as the figure's overall title.
    """
    n = len(loaded_epochs)
    nrows, ncols = _grid_layout(n)
    cell_size = 3.0
    fig, axes = plt.subplots(nrows, ncols, figsize=(cell_size * ncols, (cell_size + 0.6) * nrows), dpi=120)

    # Normalize axes to a flat list regardless of grid shape.
    if n == 1:
        axes_flat = [axes]
    elif nrows == 1 or ncols == 1:
        axes_flat = list(axes)
    else:
        axes_flat = [ax for row in axes for ax in row]

    for idx, ax in enumerate(axes_flat):
        if idx >= n:
            # Hide unused cells in the last row.
            ax.set_visible(False)
            continue

        ep = loaded_epochs[idx]
        wcs = ep["wcs"]
        half_px = _stamp_half_size_px(wcs)
        try:
            crop, (cx, cy) = _crop_around(ep["data"], wcs, ep["ra"], ep["dec"], half_px)
            ax.imshow(_stretch(crop), cmap="gray", origin="lower")
            circle_radius_px = max(6.0, 8.0 / _arcsec_per_pixel(wcs))
            ax.add_patch(plt.Circle((cx, cy), radius=circle_radius_px,
                                     edgecolor="#ff5050", facecolor="none", linewidth=1.5))
        except Exception as exc:
            logger.debug("finder_chart: stamp crop failed for %s: %s", ep.get("filename"), exc)
            ax.text(0.5, 0.5, "n/a", ha="center", va="center", transform=ax.transAxes)

        title = ep.get("obs_time", "")
        if ep.get("mag") is not None:
            title += f"\nmag {ep['mag']:.2f}"
        title += f"\nRA {ep['ra']:.4f}°  Dec {ep['dec']:.4f}°"
        ax.set_title(title, fontsize=7.5)
        ax.set_xticks([])
        ax.set_yticks([])

    if label:
        fig.suptitle(label, fontsize=11)
    fig.tight_layout(rect=(0, 0, 1, 0.96) if label else (0, 0, 1, 1))
    return _fig_to_png_bytes(fig)


def _render_stamp_strip_gif(loaded_epochs: list[dict], label: Optional[str] = None) -> bytes:
    """
    "Blink" animation for a stationary source: one frame per epoch, each
    simply _render_stamp_strip() called with a single-epoch list — the exact
    same crop/circle/caption a "stamp_strip" chart already draws for one
    cell of its grid, just returned as its own standalone frame instead of
    being laid out next to the others.
    """
    frames = [_render_stamp_strip([ep], label=label) for ep in loaded_epochs]
    return _pngs_to_gif(frames, config.CHART_GIF_FRAME_DURATION_MS)
