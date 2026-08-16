"""
modules/finder_chart/_io.py — shared FITS loading and image-assembly helpers.

Infrastructure used by more than one rendering style (_style_track.py,
_style_stamp_strip.py, _style_before_after.py, _style_stamp_strip_gif.py):
local FITS I/O, the display stretch, plate-scale/stamp-size conversion,
splitting a label into its anomaly_type/designation parts, and turning a
rendered matplotlib figure (or a list of them) into PNG/GIF bytes. No
style-specific drawing lives here — see __init__.py's docstring for the
package's file map.

Sets the Agg backend (headless — no display available or wanted on the
observatory server) before any of this package's other files import
matplotlib.pyplot; each of those files imports from this module before
importing pyplot itself, so the backend is always set first regardless of
which submodule Python happens to import first.
"""
from __future__ import annotations

import io
import logging
import math
import os
import re
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import astropy.units as u
import matplotlib.pyplot as plt
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.visualization import AsinhStretch, ImageNormalize, ZScaleInterval
from astropy.wcs import WCS
from PIL import Image

import config

logger = logging.getLogger(__name__)


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


def _stamp_half_size_px(wcs: WCS) -> int:
    """CHART_STAMP_SIZE_ARCSEC converted to pixels using this frame's own plate scale."""
    return max(10, int(round(config.CHART_STAMP_SIZE_ARCSEC / _arcsec_per_pixel(wcs))))


def _crop_around(data: np.ndarray, wcs: WCS, ra: float, dec: float, half_size_px: int) -> tuple[np.ndarray, tuple[float, float]]:
    """
    Crop a square region of `data` centred on the pixel position of (ra, dec)
    in `wcs`, clipped to the image bounds. Shared by _style_stamp_strip.py and
    _style_before_after.py — both need exactly this same "crop centred on a sky
    position" primitive.

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


# ---------------------------------------------------------------------------
# Camera rotation — see CLAUDE.md's "camera rotation" discussion. Shared by
# _style_stamp_strip.py / _style_stamp_strip_gif.py / _style_track_gif.py:
# each renders one or more crops from a DIFFERENT epoch's own raw pixel
# data, so — unlike _style_track.py's "track" style, which only ever
# displays ONE epoch's own pixel data and projects every other epoch onto
# it purely as a sky-coordinate marker (orientation-independent by
# construction) — a camera/rotator orientation difference between epochs
# would otherwise show up as the star field being visibly rotated/flipped
# between stamps or animation frames, defeating the entire point of a
# "blink comparator" (only the astrophysical content is supposed to
# change). Same _position_angle_deg() formula/sign-convention as
# modules/astrometry/_frame_geometry.py's and modules/subtraction.py's
# identical copies (duplicated here rather than imported, same convention
# this codebase already uses for its other small geometry helpers — see
# those modules' own sections in CLAUDE.md) — empirically verified there
# (tests/test_astrometry.py::TestPositionAngle,
# tests/test_subtraction.py::TestPositionAngleDeg /
# TestPrerotateReference).
# ---------------------------------------------------------------------------

def _position_angle_deg(wcs: WCS) -> Optional[float]:
    """
    This frame's own orientation on the sky (0 = North up, increasing
    clockwise toward the image's +X pixel axis), evaluated at the WCS's own
    reference pixel (CRPIX). Returns None on any failure (pathological/
    degenerate WCS) — see modules/subtraction.py's identical helper for the
    full derivation.
    """
    try:
        cx = float(wcs.wcs.crpix[0]) - 1.0
        cy = float(wcs.wcs.crpix[1]) - 1.0
        center = wcs.pixel_to_world(cx, cy)
        north = SkyCoord(ra=center.ra, dec=center.dec + 1.0 * u.arcsec)
        north_x, north_y = wcs.world_to_pixel(north)
        dx = float(north_x) - cx
        dy = float(north_y) - cy
        if dx == 0.0 and dy == 0.0:
            return None
        return math.degrees(math.atan2(dx, dy)) % 360.0
    except Exception as exc:
        logger.debug("finder_chart: position angle computation failed: %s", exc)
        return None


def _prerotation_delta_deg(own_wcs: WCS, reference_wcs: Optional[WCS]) -> Optional[float]:
    """
    Angle (degrees, in the ``scipy.ndimage.rotate(data, angle=...)`` sense)
    to rotate a crop taken with *own_wcs* by so its orientation matches
    *reference_wcs*'s — or None when no rotation is warranted: either WCS
    unavailable, either PA undeterminable, or the difference is below
    config.CHART_PREROTATE_MIN_DEG (not worth the interpolation cost for a
    negligible angle). Callers must treat None as "leave the crop and any
    point within it untouched" — never a hard failure.
    """
    if reference_wcs is None:
        return None
    own_pa = _position_angle_deg(own_wcs)
    ref_pa = _position_angle_deg(reference_wcs)
    if own_pa is None or ref_pa is None:
        return None
    delta = (ref_pa - own_pa) % 360.0
    if delta > 180.0:
        delta -= 360.0
    if abs(delta) < config.CHART_PREROTATE_MIN_DEG:
        return None
    return delta


def _rotate_crop(crop: np.ndarray, delta_deg: float) -> np.ndarray:
    """
    Rotate *crop* by delta_deg (scipy.ndimage.rotate's own convention),
    about the array's own centre, with the array's own shape preserved
    (``reshape=False``). Returns *crop* unchanged (logged, not raised) on
    any failure — e.g. scipy unavailable — same best-effort convention as
    modules/subtraction.py's _prerotate_reference().
    """
    try:
        from scipy.ndimage import rotate as _ndi_rotate
        rotated = _ndi_rotate(crop, angle=delta_deg, reshape=False, order=1, mode="constant", cval=0.0)
        return np.asarray(rotated, dtype=crop.dtype)
    except Exception as exc:
        logger.debug("finder_chart: crop rotation failed (%s) — using un-rotated crop", exc)
        return crop


def _rotate_point_in_crop(
    x: float, y: float, crop_shape: tuple[int, int], delta_deg: float,
) -> tuple[float, float]:
    """
    Rotate point (x, y) — in the SAME crop-local pixel coordinates
    _crop_around()/_crop_to_window() return — to match what _rotate_crop()
    does to the crop's own pixel data, so a marker/track point still lands
    on the same real feature after the crop it's drawn over has been
    rotated.

    Pivots about the array's own centre ``((w-1)/2, (h-1)/2)`` — the same
    centre scipy.ndimage.rotate(..., reshape=False) rotates the array
    around — and applies the *inverse* rotation to the point, since
    scipy's forward-mapping convention moves image CONTENT one way for a
    given angle while a fixed sky point's pixel coordinate moves the other
    way (verified empirically; see tests/test_finder_chart.py).
    """
    height, width = crop_shape
    cx, cy = (width - 1) / 2.0, (height - 1) / 2.0
    theta = math.radians(delta_deg)
    dx, dy = x - cx, y - cy
    new_dx = dx * math.cos(theta) + dy * math.sin(theta)
    new_dy = -dx * math.sin(theta) + dy * math.cos(theta)
    return cx + new_dx, cy + new_dy


# A label built by update_charts_for_sources() is either a bare
# `anomaly_type` (e.g. "VARIABLE_STAR", uncatalogued source) or
# `anomaly_type` + " (" + designation + ")" (e.g. "VARIABLE_STAR (TYC
# 1430-1407-1)"). Shared by _style_stamp_strip_gif.py and
# _style_before_after.py, both of which show the two parts on separate
# lines rather than either wrapping mid-word at a fixed character width or
# overflowing their own (narrow) canvas.
_LABEL_DESIGNATION_RE = re.compile(r"^(.*\S)\s+(\(.+\))$")


def _split_label_designation(label: str) -> tuple[str, Optional[str]]:
    """
    Split `label` into (anomaly_type_or_label, designation_or_None) — the
    second element is the "(designation)" part with its own parentheses
    still attached, or None when `label` is a bare anomaly_type.
    """
    match = _LABEL_DESIGNATION_RE.match(label)
    if match:
        return match.group(1), match.group(2)
    return label, None


def _fig_to_png_bytes(fig) -> bytes:
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()


def _pngs_to_gif(png_frames: list[bytes], duration_ms: int) -> bytes:
    """
    Assemble already-rendered PNG frames (each from _fig_to_png_bytes(), one
    matplotlib figure per epoch) into a single looping animated GIF.

    Frames are decoded and converted to RGB — GIF has no native support for
    matplotlib's RGBA figure output, and every frame here is an opaque
    astronomical image on a solid figure background anyway, so the alpha
    channel carries no information worth keeping. Pillow palettizes each
    frame internally when saving as GIF; some banding on the grayscale
    stretch is an acceptable trade-off for a "does it move" animation, not a
    replacement for the full-precision static PNG uploaded alongside it.
    """
    images = [Image.open(io.BytesIO(png)).convert("RGB") for png in png_frames]
    buf = io.BytesIO()
    images[0].save(
        buf, format="GIF", save_all=True, append_images=images[1:],
        duration=duration_ms, loop=0,
    )
    return buf.getvalue()
