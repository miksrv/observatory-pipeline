"""
modules/finder_chart/_style_track.py — "track" chart style: motion evidence for
moving objects (ASTEROID, COMET, MOVING_UNKNOWN, SPACE_DEBRIS).

A crop around the epoch cluster (the most recent epoch's own frame, zoomed
to where the epochs actually are) with a colored marker at every epoch's
true position, converted into the background frame's own WCS, connected by
a track line with a direction arrowhead — see _render_track_chart()'s own
docstring for the full description, and the package's __init__.py docstring
for how/when this style is chosen over "stamp_strip"/"before_after".

`_format_angular_shift()` and `_angular_separation_arcsec()` live here
(rather than in the shared `_io.py`) because, despite being generic-looking
geometry/formatting helpers, this track chart's own per-epoch legend is
their only caller in this module.
"""
from __future__ import annotations

from typing import Optional

from ._io import _arcsec_per_pixel, _fig_to_png_bytes, _pngs_to_gif, _stamp_half_size_px, _stretch
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from astropy.coordinates import SkyCoord

import config


def _format_angular_shift(sep_arcsec: float) -> str:
    """
    Format an angular separation in whichever unit keeps it readable — arcsec
    below 1', arcmin below 1°, degrees above that. Used for the track chart's
    per-epoch "moved by" legend line (see _render_track_chart()): a slow
    asteroid drifts a few arcsec between epochs, but a wide MOVING_UNKNOWN/
    SPACE_DEBRIS gap between sparse epochs can span arcminutes or more, and a
    single fixed unit would make one of those two cases unreadable.
    """
    if sep_arcsec < 60.0:
        return f"{sep_arcsec:.2f}″"  # ″
    if sep_arcsec < 3600.0:
        return f"{sep_arcsec / 60.0:.2f}′"  # ′
    return f"{sep_arcsec / 3600.0:.3f}°"  # °


def _angular_separation_arcsec(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
    """Great-circle separation between two (ra, dec) points, in arcseconds."""
    c1 = SkyCoord(ra=ra1, dec=dec1, unit="deg")
    c2 = SkyCoord(ra=ra2, dec=dec2, unit="deg")
    return float(c1.separation(c2).arcsecond)


def _label_positions(
    xs: list[float], ys: list[float], clearance_px: float,
    img_w: float = 0.0, img_h: float = 0.0,
    label_h_px: float = 12.0,
) -> list[tuple[float, float]]:
    """
    Place each label close to its own marker with a short offset, then
    nudge any overlapping labels apart so they don't collide. Labels stay
    near their markers rather than being pushed to the image edges.

    Each label starts at (x + clearance_px, y + clearance_px) — slightly
    above-right of its dot. Then a simple iterative pass pushes any pair
    that's vertically too close apart by the minimum needed distance,
    keeping leader lines short.
    """
    n = len(xs)
    if n == 0:
        return []

    min_gap = label_h_px * 1.1  # minimum vertical gap between label centres

    # Initial placement: each label slightly above-right of its own dot.
    positions = [(x + clearance_px, y + clearance_px) for x, y in zip(xs, ys)]

    # Sort indices by y-position so we can resolve overlaps bottom-to-top
    # (matplotlib "lower" origin: higher y = higher on screen).
    order = sorted(range(n), key=lambda i: positions[i][1])

    # Push overlapping labels apart (3 passes is enough for typical counts).
    pos_list = list(positions)
    for _ in range(5):
        changed = False
        for k in range(1, n):
            i_below = order[k - 1]
            i_above = order[k]
            dy = pos_list[i_above][1] - pos_list[i_below][1]
            if dy < min_gap:
                shift = (min_gap - dy) / 2.0 + 0.5
                pos_list[i_below] = (pos_list[i_below][0], pos_list[i_below][1] - shift)
                pos_list[i_above] = (pos_list[i_above][0], pos_list[i_above][1] + shift)
                changed = True
        if not changed:
            break
        order = sorted(range(n), key=lambda i: pos_list[i][1])

    return pos_list


def _format_short_time(obs_time: str) -> str:
    """
    Extract a full date-time label from an ISO-ish obs_time string.

    E.g. "2024-05-28T19:06:10" → "28/05/2024 19:06",
         "2024-05-28 19:06:10" → "28/05/2024 19:06".
    Falls back to the first 16 characters if parsing fails.
    """
    if not obs_time:
        return ""
    # Handle both "T" and " " separator between date and time, strip trailing Z
    clean = obs_time.replace("T", " ").rstrip("Z").strip()
    parts = clean.split(" ")
    if len(parts) >= 2:
        date_part = parts[0]   # "2024-05-28"
        time_part = parts[1]   # "19:06:10"
        date_tokens = date_part.split("-")
        time_tokens = time_part.split(":")
        if len(date_tokens) == 3 and len(time_tokens) >= 2:
            # DD/MM/YYYY HH:MM
            return f"{date_tokens[2]}/{date_tokens[1]}/{date_tokens[0]} {time_tokens[0]}:{time_tokens[1]}"
    return obs_time[:16]


def _parse_delta_hours(obs_time1: str, obs_time2: str) -> Optional[float]:
    """
    Time difference in hours between two ISO-ish obs_time strings.
    Returns None if either string can't be parsed.
    """
    from datetime import datetime
    fmts = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%SZ"]
    def _parse(s: str) -> Optional[datetime]:
        for fmt in fmts:
            try:
                return datetime.strptime(s.strip(), fmt)
            except (ValueError, TypeError):
                continue
        return None
    t1, t2 = _parse(obs_time1), _parse(obs_time2)
    if t1 is None or t2 is None:
        return None
    return abs((t2 - t1).total_seconds()) / 3600.0


def _epoch_colors(n: int) -> list[str]:
    """
    Return a list of n hex color strings forming a gradient from cool
    (oldest epoch) to warm (newest epoch). Uses a blue→cyan→yellow→red
    progression so early/late epochs are visually distinct at a glance.
    """
    if n <= 1:
        return ["#ff5050"]
    cmap = matplotlib.colormaps["plasma"]
    return [
        "#{:02x}{:02x}{:02x}".format(
            int(c[0] * 255), int(c[1] * 255), int(c[2] * 255),
        )
        for c in (cmap(i / (n - 1)) for i in range(n))
    ]


def _render_track_chart(loaded_epochs: list[dict], label: Optional[str] = None) -> bytes:
    """
    A crop around the epoch cluster (the most recent epoch's own frame,
    zoomed to where the epochs actually are) with a colored marker at every
    epoch's true position, converted into the background frame's own WCS,
    connected by a track line with a direction arrowhead. Each marker
    carries a small numbered badge (1, 2, 3…) placed close to its dot with
    a minimal leader line; overlapping badges are nudged apart just enough
    to stay readable. Markers use a color gradient from cool (oldest) to
    warm (newest) so time progression is visible at a glance. The detailed
    legend below the image shows full date, coordinates, magnitude, angular
    separation, time gap, and angular velocity — keyed by the same colored
    frame number as the badge on the chart.

    `label`, if given (e.g. "ASTEROID (4 Vesta)" — the anomaly_type plus its
    resolved catalog designation, see update_charts_for_sources()), is shown
    as the figure's overall title.
    """
    background = loaded_epochs[-1]
    wcs = background["wcs"]
    marker_radius_px = max(3.0, 4.0 / _arcsec_per_pixel(wcs))

    xs: list[float] = []
    ys: list[float] = []
    for ep in loaded_epochs:
        x, y = wcs.world_to_pixel(SkyCoord(ra=ep["ra"], dec=ep["dec"], unit="deg"))
        xs.append(float(x))
        ys.append(float(y))

    # Crop to the epoch cluster (same logic as before — see the detailed
    # comment in the previous revision of this function).
    cluster_cx, cluster_cy = float(np.mean(xs)), float(np.mean(ys))
    cluster_half_span = max(
        (max(abs(x - cluster_cx), abs(y - cluster_cy)) for x, y in zip(xs, ys)), default=0.0,
    )
    half_size_px = max(3.0 * _stamp_half_size_px(wcs), cluster_half_span * 1.6)

    height, width = background["data"].shape
    x0 = int(max(0, round(cluster_cx - half_size_px)))
    x1 = int(min(width, round(cluster_cx + half_size_px)))
    y0 = int(max(0, round(cluster_cy - half_size_px)))
    y1 = int(min(height, round(cluster_cy + half_size_px)))
    image = _stretch(background["data"][y0:y1, x0:x1])
    xs = [x - x0 for x in xs]
    ys = [y - y0 for y in ys]

    n = len(loaded_epochs)
    colors = _epoch_colors(n)

    # Labels placed near their markers, nudged apart to avoid overlap.
    label_positions = _label_positions(
        xs, ys, clearance_px=max(6.0, marker_radius_px + 4.0),
    )

    # Reserve extra height for the coordinate legend below the image.
    fig_height = 6.0 + 0.18 * n
    fig, ax = plt.subplots(figsize=(6, fig_height), dpi=120)
    ax.imshow(image, cmap="gray", origin="lower")

    # Track line: draw per-segment with gradient color so older segments are
    # cooler and newer ones warmer — matches the markers' own gradient.
    for i in range(1, n):
        ax.plot([xs[i - 1], xs[i]], [ys[i - 1], ys[i]], "-",
                color=colors[i], linewidth=1.2, alpha=0.6, zorder=2)

    # Direction arrowhead on the last segment — immediately shows which way
    # the object is moving without reading any label.
    if n >= 2:
        ax.annotate(
            "", xy=(xs[-1], ys[-1]), xytext=(xs[-2], ys[-2]),
            arrowprops=dict(arrowstyle="-|>", color=colors[-1],
                            lw=1.5, mutation_scale=12),
            zorder=2,
        )

    # Colored markers + compact number labels near each dot.
    for i, ((x, y), (lx, ly), ep, color) in enumerate(
        zip(zip(xs, ys), label_positions, loaded_epochs, colors),
        start=1,
    ):
        # Filled dot — color encodes chronological position.
        ax.plot(x, y, "o", color=color, markeredgecolor="white",
                markeredgewidth=0.5, markersize=6, zorder=5)

        # Label: just the frame number — date/mag details are in the legend.
        ax.annotate(
            str(i), xy=(x, y), xytext=(lx, ly),
            fontsize=6, fontweight="bold", color="white",
            ha="center", va="center", zorder=6,
            bbox=dict(boxstyle="round,pad=0.12", fc=color, ec="none", alpha=0.85),
            arrowprops=dict(arrowstyle="-", color=color, linewidth=0.6,
                            alpha=0.6, shrinkA=0.5, shrinkB=marker_radius_px + 0.5),
        )

    ax.set_title(
        f"{n} epoch(s) — background: {background.get('obs_time', '')}",
        fontsize=9,
    )
    ax.set_xticks([])
    ax.set_yticks([])

    # Detailed legend below the image: each epoch's number is colored to
    # match the marker on the chart. Full date, coordinates, and movement
    # details are here since on-chart labels show only the frame number.
    legend_lines: list[tuple[str, str, str]] = []  # (number_prefix, rest_of_line, color)
    for i, ep in enumerate(loaded_epochs, start=1):
        prefix = f"({i})"
        full_time = _format_short_time(ep.get("obs_time", ""))
        rest = f" {full_time}  RA {ep['ra']:.4f}°  Dec {ep['dec']:.4f}°"
        if ep.get("mag") is not None:
            rest += f"  {ep['mag']:.1f}m"
        if i > 1:
            prev = loaded_epochs[i - 2]
            sep_arcsec = _angular_separation_arcsec(prev["ra"], prev["dec"], ep["ra"], ep["dec"])
            delta_h = _parse_delta_hours(prev.get("obs_time", ""), ep.get("obs_time", ""))
            shift_str = _format_angular_shift(sep_arcsec)
            if delta_h and delta_h > 0:
                velocity = sep_arcsec / delta_h
                # Format Δt readably
                if delta_h < 1.0:
                    dt_str = f"{delta_h * 60:.0f}min"
                elif delta_h < 24.0:
                    dt_str = f"{delta_h:.1f}hr"
                else:
                    dt_str = f"{delta_h / 24:.1f}d"
                rest += f"  Δ{shift_str} in {dt_str} ({velocity:.1f}″/hr)"
            else:
                rest += f"  Δ{shift_str}"
        legend_lines.append((prefix, rest, colors[i - 1]))

    # Render legend lines bottom-up so the first epoch is at the top.
    line_height_frac = 0.014
    for idx, (prefix, rest, color) in enumerate(reversed(legend_lines)):
        y_pos = 0.01 + idx * line_height_frac
        # Colored number prefix
        fig.text(0.02, y_pos, prefix, fontsize=6.5, family="monospace",
                 ha="left", va="bottom", color=color, fontweight="bold")
        # Rest of line in default color
        fig.text(0.02 + 0.035, y_pos, rest, fontsize=6.5, family="monospace",
                 ha="left", va="bottom", color="#222222")

    legend_frac = 0.03 + 0.016 * n
    top_frac = 0.92 if label else 0.96
    fig.subplots_adjust(left=0.02, right=0.98, bottom=legend_frac, top=top_frac)
    if label:
        fig.suptitle(label, fontsize=11)

    return _fig_to_png_bytes(fig)


def _render_track_gif(loaded_epochs: list[dict], label: Optional[str] = None) -> bytes:
    """
    Cumulative-reveal animation for a moving source: frame k re-renders the
    ordinary "track" chart using only the first k epochs (loaded_epochs[:k]),
    so the marker trail grows one point per frame and the final frame is
    exactly the same image update_charts_for_sources() also uploads as the
    static "track" PNG.

    Each frame's own background is that frame's own most-recent epoch (i.e.
    _render_track_chart()'s own choice of `loaded_epochs[-1]` for whatever
    subset it's given) — the background image genuinely is a different
    single exposure at each step, exactly as it would be if this chart had
    been (re)generated right after each epoch arrived historically, so a
    background that changes between animation frames is the accurate
    picture rather than an artifact to avoid.
    """
    frames = [
        _render_track_chart(loaded_epochs[:k], label=label)
        for k in range(1, len(loaded_epochs) + 1)
    ]
    return _pngs_to_gif(frames, config.CHART_GIF_FRAME_DURATION_MS)
