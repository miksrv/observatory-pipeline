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
viewer exactly which sky position each mark/crop was at.

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
fetches all their tracks via one GET /sources/tracks/batch call, then
uploads every rendered chart via one POST /sources/charts/batch call —
instead of one GET + one POST per source, which is what this module did
before and what drove up API request volume on frames with several
anomalies. See api_client.get_source_tracks_batch / upload_source_charts_batch.
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
    pixels apart — see debug/README.md). Instead every epoch's coordinates
    are listed in a small monospace legend under the image, keyed by the
    same number as its marker.

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

    legend = "\n".join(
        f"{i}: RA {ep['ra']:.4f}°  Dec {ep['dec']:.4f}°  {ep.get('obs_time', '')}"
        for i, ep in enumerate(loaded_epochs, start=1)
    )
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
# Rendering (local — no API calls). Caps to CHART_MAX_EPOCHS, loads whatever
# epochs are still present in the local archive, and renders the PNG.
# Failure at any step logs and returns None rather than raising, so a single
# source_id's rendering trouble never affects any other source_id in the
# same update_charts_for_sources() batch.
# ---------------------------------------------------------------------------

def _render_chart_for_source(
    source_id: str, epochs: list[dict], anomaly_type: str, designation: Optional[str] = None,
) -> Optional[tuple[bytes, str, int]]:
    """
    Returns (png_bytes, style, frame_count), or None on any failure.

    `designation`: the source's catalog identity (e.g. an MPC name for an
    ASTEROID/COMET, or a Simbad main_id for a known VARIABLE_STAR/BINARY_STAR),
    when the underlying source is catalog-matched at all — shown alongside
    anomaly_type as the chart's title, e.g. "ASTEROID (4 Vesta)". None for an
    uncatalogued source, in which case the chart is titled with just
    anomaly_type.
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

    style = _style_for_anomaly_type(anomaly_type)
    label = f"{anomaly_type} ({designation})" if designation else anomaly_type

    try:
        png_bytes = _render_track_chart(loaded, label=label) if style == STYLE_TRACK else _render_stamp_strip(loaded, label=label)
    except Exception as exc:
        logger.warning("finder_chart: rendering (%s) failed for source_id=%s: %s", style, source_id, exc)
        return None

    return png_bytes, style, len(loaded)


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

    Fetches all tracks in a single GET /sources/tracks/batch call and
    uploads all rendered charts in a single POST /sources/charts/batch
    call — regardless of how many source_ids are passed — instead of one
    GET+POST round trip per source_id.

    Parameters
    ----------
    anomaly_type_by_source_id:
        Maps each `sources.id` with a just-detected anomaly (from
        `_source_id` — see pipeline.py Step 7) to that anomaly's
        anomaly_type, which decides the chart style (MOVING_TYPES →
        "track", everything else → "stamp_strip").
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
    charts_to_upload: list[dict[str, Any]] = []

    for source_id in source_ids:
        epochs = tracks.get(source_id) or []
        if not epochs:
            logger.debug("finder_chart: no epochs for source_id=%s — skipping", source_id)
            continue

        rendered = _render_chart_for_source(
            source_id, epochs, anomaly_type_by_source_id[source_id],
            designation_by_source_id.get(source_id),
        )
        if rendered is None:
            continue

        png_bytes, style, frame_count = rendered
        charts_to_upload.append({
            "source_id": source_id,
            "png_bytes": png_bytes,
            "style": style,
            "frame_count": frame_count,
        })

    if not charts_to_upload:
        return results

    try:
        outcomes = await api_client.upload_source_charts_batch(charts_to_upload)
    except Exception as exc:
        logger.warning("finder_chart: chart batch upload failed for %d source(s): %s", len(charts_to_upload), exc)
        outcomes = {}

    for chart in charts_to_upload:
        results[chart["source_id"]] = bool(outcomes.get(chart["source_id"], False))

    return results
