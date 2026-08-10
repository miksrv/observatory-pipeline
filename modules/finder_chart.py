"""
modules/finder_chart.py — Per-source finder/discovery chart generation.

For an anomaly with a resolved source_id, builds a small PNG visualizing
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
                    detection. Both panels are centred on the SAME (ra, dec)
                    on purpose (a single-occurrence source has exactly one
                    detected position) — that shared coordinate is shown
                    once, in the figure's title, not repeated under both
                    panels (see debug/README.md's "Third follow-up" for why:
                    the repeat read as a bug — unmoving coordinates —
                    instead of the intentional fixed query point it is).
                    Falls back to a single "after only" panel, with an
                    explicit note why, if no earlier frame of the object
                    exists yet (this is its first-ever frame) or it can't be
                    loaded. The earlier frame comes from
                    GET /frames/nearest-before (see api_client.client and
                    docs/API.md section 13) — the one query this module
                    needs that isn't already answered by a source's own
                    track, since it asks about a DIFFERENT frame of the same
                    object, not this source's own history.
  - "track"       — ASTEROID / COMET / MOVING_UNKNOWN / SPACE_DEBRIS, 2+
                    epochs. One background image (the most recent epoch's
                    own frame) with a small marker at every epoch's true
                    position, connected by a faint line in chronological
                    order. Every epoch's (ra, dec) is converted into the
                    *background* epoch's WCS pixel grid via
                    WCS.world_to_pixel() — no pixel-level alignment between
                    frames is needed, only a per-epoch coordinate transform,
                    since all that matters is where each epoch's position
                    falls on the one displayed image. Each marker's epoch
                    number sits at the end of a short leader line, spread
                    evenly around the point cluster's centroid rather than
                    stacked directly on the marker — epochs are frequently
                    only a few pixels apart (e.g. a slow-moving asteroid on
                    a wide-field frame), and a label placed right on top of
                    the point would both obscure it and collide with its
                    neighbours' labels.
  - "stamp_strip" — everything else (SUPERNOVA_CANDIDATE, UNKNOWN,
                    VARIABLE_STAR, BINARY_STAR, KNOWN_CATALOG_NEW,
                    FIRST_OBSERVATION), 2+ epochs. One small crop per epoch,
                    centred on that epoch's own detected position using that
                    frame's own WCS, each circled and labelled with its
                    timestamp and magnitude — a classic "blink" strip for a
                    source that isn't expected to move.

Every epoch's own (RA, Dec) is captioned on its "stamp_strip" panel, and
listed in a small legend under the "track" image (keyed by the same number
as its marker) — added 2026-08-06 because neither style otherwise told a
viewer exactly which sky position each mark/crop was at. The "track" legend
additionally shows each epoch's angular separation from the PREVIOUS epoch
(arcsec/arcmin/degrees, whichever keeps the number readable — see
_format_angular_shift()), added 2026-08-07: RA/Dec alone forced a viewer to
eyeball two coordinates themselves to judge how fast the object was actually
moving, which matters for telling a genuine mover from mere centroid/seeing
jitter at a glance.

If the source is catalog-matched (e.g. an MPC-identified asteroid, or a
Simbad-known variable/binary star), the chart's title also carries that
designation next to anomaly_type, e.g. "ASTEROID (4 Vesta)" — see
`designation_by_source_id` below. An uncatalogued source's chart is titled
with just its anomaly_type, as before.

The single public entry point is:

    await finder_chart.update_charts_for_sources(
        anomaly_type_by_source_id, designation_by_source_id=None,
    ) -> dict[str, bool]

It takes every (source_id -> anomaly_type) pair for one frame at once and
fetches all their tracks via one POST /sources/tracks/batch call, then
uploads each rendered chart individually via POST /sources/{id}/chart — one
request per chart. See api_client.get_source_tracks_batch / upload_source_chart.
`designation_by_source_id` is optional and keyed the same way, built by
pipeline.py from the already catalog-matched `sources` list (catalog_name/
catalog_id) — this module never queries a catalog itself. When one or more
sources need the "before_after" style, one additional
GET /frames/nearest-before call is made and cached for the rest of this same
update_charts_for_sources() call — every single-occurrence source in one
call shares the exact same object and current obs_time (they're all
anomalies from the one frame just processed), so this never grows past one
extra request regardless of how many such sources there are.

Best-effort throughout: every failure (missing local archive file, API
error, rendering error) is caught and logged, and only ever downgrades that
one source_id's own result to False — it never raises and never prevents
the other source_ids in the same call from being processed. See
pipeline.py's Step 9.5, which calls this unconditionally for every anomaly
with a source_id and must not have chart generation affect frame processing.
"""
from __future__ import annotations

import io
import logging
import os
from typing import Any, Optional

import matplotlib
matplotlib.use("Agg")  # headless: no display available (or wanted) on the observatory server
import matplotlib.pyplot as plt
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.visualization import AsinhStretch, ImageNormalize, ZScaleInterval
from astropy.wcs import WCS

import config
from api_client import client as api_client

logger = logging.getLogger(__name__)

# Anomaly types for which the source is expected to have moved between
# epochs — see modules/anomaly_detector.py's classification table.
MOVING_TYPES = frozenset({"ASTEROID", "COMET", "MOVING_UNKNOWN", "SPACE_DEBRIS"})

STYLE_TRACK = "track"
STYLE_STAMP_STRIP = "stamp_strip"
STYLE_BEFORE_AFTER = "before_after"

# Colors for the "before_after" style — the dashed grey BEFORE circle marks
# "look here, nothing expected"; the solid red AFTER circle matches the
# marker/circle color every other style in this module uses.
_BEFORE_AFTER_BEFORE_COLOR = "#999999"
_BEFORE_AFTER_AFTER_COLOR = "#ff5050"


def _style_for_anomaly_type(anomaly_type: str) -> str:
    """Pick the chart style for the anomaly type that just triggered a chart update."""
    return STYLE_TRACK if anomaly_type in MOVING_TYPES else STYLE_STAMP_STRIP


def _style_for_source(anomaly_type: str, n_epochs: int) -> str:
    """
    Pick the chart style for a source with `n_epochs` loaded epochs. A
    source detected on only one epoch so far gets STYLE_BEFORE_AFTER
    regardless of anomaly_type — there's no track/blink-strip to draw from a
    single point. 2+ epochs use _style_for_anomaly_type()'s existing
    anomaly_type-based routing, unchanged.
    """
    if n_epochs <= 1:
        return STYLE_BEFORE_AFTER
    return _style_for_anomaly_type(anomaly_type)


# ---------------------------------------------------------------------------
# FITS I/O helpers
# ---------------------------------------------------------------------------

def _local_fits_path(epoch: dict) -> str:
    """
    Resolve an epoch dict (from GET /sources/{id}/track) to its local path
    in the FITS archive. Frames are archived under FITS_ARCHIVE/{object}/
    (see pipeline.py Step 10) — "object" here is that same normalized
    directory name, as recorded on the `frames` row.
    """
    return os.path.join(config.FITS_ARCHIVE, epoch.get("object") or "_UNKNOWN", epoch["filename"])


def _load_frame(fits_path: str) -> Optional[tuple[np.ndarray, WCS]]:
    """
    Load the first 2-D image extension plus its WCS from a FITS file.

    Returns None if the file is missing, unreadable, or has no usable
    celestial WCS (mirrors modules/subtraction.py's _load_frame_data /
    _pixel_to_sky loading pattern).
    """
    try:
        with fits.open(fits_path) as hdul:
            for hdu in hdul:
                if hdu.data is None or hdu.data.ndim != 2:
                    continue
                if not hdu.header.get("CTYPE1"):
                    continue
                try:
                    wcs = WCS(hdu.header)
                except Exception:
                    continue
                if not wcs.has_celestial:
                    continue
                return hdu.data.astype(np.float32), wcs
        return None
    except Exception as exc:
        logger.debug("finder_chart: cannot load %s: %s", fits_path, exc)
        return None


def _stretch(data: np.ndarray) -> np.ndarray:
    """Zscale + asinh stretch for display — the standard DS9-style visualization."""
    vmin, vmax = ZScaleInterval().get_limits(data)
    norm = ImageNormalize(vmin=vmin, vmax=vmax, stretch=AsinhStretch())
    return norm(data)


def _arcsec_per_pixel(wcs: WCS) -> float:
    """Mean plate scale in arcsec/pixel, with a generic fallback if WCS is degenerate."""
    try:
        # proj_plane_pixel_scales() returns a list of astropy Quantity (one
        # per axis, in degrees) — NOT a Quantity array, so np.mean() on the
        # raw list tries to coerce each element to a bare float via numpy's
        # asanyarray() and raises (Quantity.__float__ refuses non-dimensionless
        # units). Pull out the plain degree values first.
        scales_deg = [scale.to_value("deg") for scale in wcs.proj_plane_pixel_scales()]
        arcsec_per_px = float(np.mean(scales_deg)) * 3600.0
        if arcsec_per_px > 0:
            return arcsec_per_px
    except Exception:
        pass
    return 1.5


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


def _fig_to_png_bytes(fig) -> bytes:
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Track chart (moving objects)
# ---------------------------------------------------------------------------

def _label_positions(xs: list[float], ys: list[float], clearance_px: float) -> list[tuple[float, float]]:
    """
    Anchor point for each epoch's number label: spread evenly around the
    point cluster's centroid, at a radius clear of every marker in the
    cluster — not just its centroid — so labels don't collide with each
    other even when epochs sit only a few pixels apart (the common case for
    a slow-moving object on a wide-field background frame). Angular order
    around the centroid is preserved when assigning label angles, so leader
    lines fan out without crossing one another.
    """
    n = len(xs)
    cx, cy = float(np.mean(xs)), float(np.mean(ys))
    cluster_radius = max((float(np.hypot(x - cx, y - cy)) for x, y in zip(xs, ys)), default=0.0)
    label_radius = cluster_radius + clearance_px

    order = sorted(range(n), key=lambda i: float(np.arctan2(ys[i] - cy, xs[i] - cx)))
    angle_by_index = {idx: 2 * np.pi * rank / n for rank, idx in enumerate(order)}

    return [
        (cx + label_radius * np.cos(angle_by_index[i]), cy + label_radius * np.sin(angle_by_index[i]))
        for i in range(n)
    ]


def _render_track_chart(loaded_epochs: list[dict], label: Optional[str] = None) -> bytes:
    """
    A crop around the epoch cluster (the most recent epoch's own frame,
    zoomed to where the epochs actually are — see below) with a small marker
    at every epoch's true position, converted into the background frame's own
    WCS, connected by a faint line in chronological order. Each marker's
    number is placed at the far end of a short leader line rather than on
    top of the marker itself — see _label_positions().

    Numbered markers on the image stay bare (adding each epoch's own RA/Dec
    text there would collide for a slow mover whose epochs sit only a few
    pixels apart — see debug/README.md). Instead every epoch's coordinates,
    plus its angular separation from the previous epoch (arcsec/arcmin/
    degrees — see _format_angular_shift()), are listed in a small monospace
    legend under the image, keyed by the same number as its marker.

    `label`, if given (e.g. "ASTEROID (4 Vesta)" — the anomaly_type plus its
    resolved catalog designation, see update_charts_for_sources()), is shown
    as the figure's overall title.
    """
    background = loaded_epochs[-1]
    wcs = background["wcs"]
    # Smaller and fixed-ish vs. the old 8"-derived radius: this only needs to
    # mark a point, not represent an angular tolerance, so it should stay
    # small enough to not itself obscure a nearby star.
    marker_radius_px = max(3.0, 4.0 / _arcsec_per_pixel(wcs))

    xs: list[float] = []
    ys: list[float] = []
    for ep in loaded_epochs:
        x, y = wcs.world_to_pixel(SkyCoord(ra=ep["ra"], dec=ep["dec"], unit="deg"))
        xs.append(float(x))
        ys.append(float(y))

    # Crop to the epoch cluster instead of showing the whole (often
    # thousands-of-pixels-wide) frame: a slow mover's few dozen pixels of
    # drift between epochs is otherwise invisible once the full frame is
    # scaled down to fit the figure — the markers end up on top of each
    # other with no visible motion. Half-size is whichever is bigger: a
    # generous fixed context window (3x the stamp_strip crop size), or the
    # cluster's own footprint plus margin, so a genuinely wide multi-epoch
    # trail across most of the frame still renders in full rather than
    # getting clipped.
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

    label_positions = _label_positions(xs, ys, clearance_px=6.0 * marker_radius_px)

    # Reserve extra height for the coordinate legend below the image —
    # scales with epoch count so it doesn't get cramped for a long history.
    fig_height = 6.0 + 0.16 * len(loaded_epochs)
    fig, ax = plt.subplots(figsize=(6, fig_height), dpi=120)
    ax.imshow(image, cmap="gray", origin="lower")
    ax.plot(xs, ys, "-", color="#ff5050", linewidth=1.0, alpha=0.6, zorder=2)
    for i, ((x, y), (lx, ly)) in enumerate(zip(zip(xs, ys), label_positions), start=1):
        ax.add_patch(plt.Circle((x, y), radius=marker_radius_px,
                                 edgecolor="#ff5050", facecolor="none", linewidth=1.3, zorder=3))
        ax.annotate(
            str(i), xy=(x, y), xytext=(lx, ly),
            color="#ff5050", fontsize=9, fontweight="bold", ha="center", va="center", zorder=4,
            arrowprops=dict(arrowstyle="-", color="#ff5050", linewidth=1.0, alpha=0.9,
                             shrinkA=2.0, shrinkB=marker_radius_px + 2.0),
        )
    ax.set_title(f"{len(loaded_epochs)} epoch(s) — background: {background.get('obs_time', '')}", fontsize=9)
    ax.set_xticks([])
    ax.set_yticks([])

    # Δ from the previous epoch (arcsec/arcmin/degrees — see
    # _format_angular_shift()) tells a viewer at a glance how fast the object
    # is actually moving, instead of leaving them to eyeball two RA/Dec
    # values themselves. loaded_epochs is chronologically ordered (see
    # _render_chart_for_source()), so "previous" here means "previous in
    # time", not "previous marker number" — the two coincide since epochs are
    # never reordered. The first epoch has nothing to compare against.
    legend_lines = []
    for i, ep in enumerate(loaded_epochs, start=1):
        line = f"{i}: RA {ep['ra']:.4f}°  Dec {ep['dec']:.4f}°  {ep.get('obs_time', '')}"
        if i > 1:
            prev = loaded_epochs[i - 2]
            sep_arcsec = _angular_separation_arcsec(prev["ra"], prev["dec"], ep["ra"], ep["dec"])
            line += f"  (moved {_format_angular_shift(sep_arcsec)} from epoch {i - 1})"
        legend_lines.append(line)
    legend = "\n".join(legend_lines)
    fig.text(0.02, 0.01, legend, fontsize=6.5, family="monospace", ha="left", va="bottom")

    legend_frac = 0.03 + 0.016 * len(loaded_epochs)
    top_frac = 0.90 if label else 0.96
    fig.subplots_adjust(left=0.02, right=0.98, bottom=legend_frac, top=top_frac)
    if label:
        fig.suptitle(label, fontsize=11)

    return _fig_to_png_bytes(fig)


# ---------------------------------------------------------------------------
# Stamp strip (stationary anomalies)
# ---------------------------------------------------------------------------

def _crop_around(data: np.ndarray, wcs: WCS, ra: float, dec: float, half_size_px: int) -> tuple[np.ndarray, tuple[float, float]]:
    """
    Crop a square region of `data` centred on the pixel position of (ra, dec)
    in `wcs`, clipped to the image bounds.

    Returns (crop, (cx, cy)) where (cx, cy) is the anomaly's position within
    the *returned crop* (not the original frame) — needed because clipping
    at an image edge shifts the crop off-centre from the nominal box.
    """
    x, y = wcs.world_to_pixel(SkyCoord(ra=ra, dec=dec, unit="deg"))
    x, y = float(x), float(y)

    height, width = data.shape
    x0 = int(max(0, round(x - half_size_px)))
    x1 = int(min(width, round(x + half_size_px)))
    y0 = int(max(0, round(y - half_size_px)))
    y1 = int(min(height, round(y + half_size_px)))

    if x1 <= x0 or y1 <= y0:
        # The anomaly's own detected position falls outside its own frame's
        # bounds — shouldn't normally happen, but guard rather than crash.
        raise ValueError("crop region is empty")

    return data[y0:y1, x0:x1], (x - x0, y - y0)


def _stamp_half_size_px(wcs: WCS) -> int:
    """CHART_STAMP_SIZE_ARCSEC converted to pixels using this frame's own plate scale."""
    return max(10, int(round(config.CHART_STAMP_SIZE_ARCSEC / _arcsec_per_pixel(wcs))))


def _render_stamp_strip(loaded_epochs: list[dict], label: Optional[str] = None) -> bytes:
    """
    One small labelled crop per epoch, side by side, each circled at its own
    detected position and captioned with its own RA/Dec.

    `label`, if given (e.g. "VARIABLE_STAR (TYC 1430-1407-1)" — the
    anomaly_type plus its resolved catalog designation, see
    update_charts_for_sources()), is shown as the figure's overall title.
    """
    n = len(loaded_epochs)
    fig, axes = plt.subplots(1, n, figsize=(3 * n, 3.6), dpi=120)
    axes = [axes] if n == 1 else list(axes)

    for ax, ep in zip(axes, loaded_epochs):
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
    fig.tight_layout(rect=(0, 0, 1, 0.90) if label else (0, 0, 1, 1))
    return _fig_to_png_bytes(fig)


# ---------------------------------------------------------------------------
# Before/after (single-epoch sources)
# ---------------------------------------------------------------------------

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
    identically under both panels — see this module's docstring.

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
        ax_before.set_title(f"BEFORE — {before_ep.get('obs_time', '')}\n(nothing expected here)",
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
    ax_after.set_title(f"AFTER — {current_ep.get('obs_time', '')}{mag_txt}",
                        fontsize=9, color=_BEFORE_AFTER_AFTER_COLOR)
    ax_after.set_xticks([]); ax_after.set_yticks([])

    coord_line = (
        f"fixed query position: RA {current_ep['ra']:.4f}°  Dec {current_ep['dec']:.4f}°"
        "  (same in both panels, by design)"
    )
    fig.suptitle(f"{label}\n{coord_line}" if label else coord_line, fontsize=10)
    if missing_reason:
        fig.text(0.5, 0.01, missing_reason, ha="center", fontsize=7.5, color=_BEFORE_AFTER_BEFORE_COLOR)
    fig.tight_layout(rect=(0, 0.04, 1, 0.86))

    return _fig_to_png_bytes(fig)


# ---------------------------------------------------------------------------
# Rendering (local — no API calls). Caps to CHART_MAX_EPOCHS, loads whatever
# epochs are still present in the local archive, and renders the PNG.
# Failure at any step logs and returns None rather than raising, so a single
# source_id's rendering trouble never affects any other source_id in the
# same update_charts_for_sources() batch.
# ---------------------------------------------------------------------------

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


async def _render_chart_for_source(
    source_id: str, epochs: list[dict], anomaly_type: str, designation: Optional[str] = None,
    earlier_frame_cache: Optional[dict] = None,
) -> Optional[tuple[bytes, str, int]]:
    """
    Returns (png_bytes, style, frame_count), or None on any failure.

    `designation`: the source's catalog identity (e.g. an MPC name for an
    ASTEROID/COMET, or a Simbad main_id for a known VARIABLE_STAR/BINARY_STAR),
    when the underlying source is catalog-matched at all — shown alongside
    anomaly_type as the chart's title, e.g. "ASTEROID (4 Vesta)". None for an
    uncatalogued source, in which case the chart is titled with just
    anomaly_type.

    `earlier_frame_cache`: shared across one update_charts_for_sources()
    call — see _get_earlier_frame_epoch(). Only ever read/written when this
    source ends up needing the "before_after" style (exactly one loaded
    epoch); unused (and safe to omit) otherwise.
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
        return None

    style = _style_for_source(anomaly_type, len(loaded))
    label = f"{anomaly_type} ({designation})" if designation else anomaly_type

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
        return None

    return png_bytes, style, frame_count


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def update_charts_for_sources(
    anomaly_type_by_source_id: dict[str, str],
    designation_by_source_id: Optional[dict[str, str]] = None,
) -> dict[str, bool]:
    """
    (Re)generate and upload finder charts for every given source_id,
    reflecting each source's complete track, after a batch of anomalies was
    just detected on one frame.

    Fetches all tracks in a single POST /sources/tracks/batch call, renders
    each chart locally, then uploads each rendered chart individually via
    POST /sources/{id}/chart — one request per chart.

    Parameters
    ----------
    anomaly_type_by_source_id:
        Maps each `sources.id` with a just-detected anomaly (from
        `_source_id` — see pipeline.py Step 7) to that anomaly's
        anomaly_type. Together with how many epochs the source actually has,
        this decides the chart style — see _style_for_source(): exactly one
        epoch → "before_after" regardless of anomaly_type; 2+ epochs →
        "track" for MOVING_TYPES, "stamp_strip" otherwise.
    designation_by_source_id:
        Optional. Maps a subset of the same source_ids to their resolved
        catalog identity (e.g. the MPC designation for an ASTEROID/COMET, or
        a Simbad main_id for a known VARIABLE_STAR/BINARY_STAR) — shown next
        to anomaly_type as the chart's title, e.g. "ASTEROID (4 Vesta)". A
        source_id absent from this dict (including when the dict itself is
        omitted) gets a chart titled with just its anomaly_type — the normal
        case for an uncatalogued source, which has no designation to show.

    Returns
    -------
    dict[str, bool]
        One entry per key in `anomaly_type_by_source_id`: True if that
        source's chart was rendered and uploaded, False if disabled, no
        usable epochs were found, rendering failed, or the upload was
        rejected/failed. Never raises.
    """
    if not anomaly_type_by_source_id:
        return {}

    if not config.CHART_ENABLED:
        return {source_id: False for source_id in anomaly_type_by_source_id}

    designation_by_source_id = designation_by_source_id or {}
    source_ids = list(anomaly_type_by_source_id.keys())

    try:
        tracks = await api_client.get_source_tracks_batch(source_ids)
    except Exception as exc:
        logger.warning("finder_chart: could not fetch tracks batch for %d source(s): %s", len(source_ids), exc)
        tracks = {}

    results: dict[str, bool] = {source_id: False for source_id in source_ids}
    # Shared across every source in this call — see _get_earlier_frame_epoch()
    # for why this collapses to at most one extra API call regardless of how
    # many sources end up needing the "before_after" style.
    earlier_frame_cache: dict[tuple[Any, Any], tuple[Optional[dict], Optional[str]]] = {}

    for source_id in source_ids:
        epochs = tracks.get(source_id) or []
        if not epochs:
            logger.debug("finder_chart: no epochs for source_id=%s — skipping", source_id)
            continue

        rendered = await _render_chart_for_source(
            source_id, epochs, anomaly_type_by_source_id[source_id],
            designation_by_source_id.get(source_id),
            earlier_frame_cache,
        )
        if rendered is None:
            continue

        png_bytes, style, frame_count = rendered

        try:
            ok = await api_client.upload_source_chart(source_id, png_bytes, style, frame_count)
        except Exception as exc:
            logger.warning("finder_chart: upload failed for source_id=%s: %s", source_id, exc)
            ok = False

        results[source_id] = ok

    return results
