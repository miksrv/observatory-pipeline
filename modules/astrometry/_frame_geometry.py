"""
modules/astrometry/_frame_geometry.py — Step 3: deriving the frame centre,
field of view, pixel scale, and position angle from a solved WCS.

Internal helper only — not part of this package's public surface.
"""

from __future__ import annotations

import logging
import math

import astropy.units as u
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.wcs import WCS

logger = logging.getLogger(__name__)


def _position_angle_deg(wcs: WCS, cx: float, cy: float) -> float | None:
    """
    Position angle (degrees, 0-360) of this frame's own orientation on the
    sky, evaluated at pixel (cx, cy) — how far celestial North is rotated
    away from the image's own +Y (pixel-row-increasing) axis, measured
    toward +X. PA=0 means "North is up"; PA=90 means North points along
    +X ("North is to the right"); the value increases clockwise as the
    frame's own rotation increases, matching a plain
    ``scipy.ndimage.rotate(data, angle=+PA)`` sense (see
    modules/subtraction.py's identical copy of this helper, which relies on
    that exact correspondence to pre-rotate a reference frame onto a new
    frame's orientation before alignment).

    Deliberately computed via a WCS pixel<->world round trip rather than by
    decoding the CD/PC matrix's trig algebraically (the classic CROTA2
    derivation): a round trip stays correct regardless of the matrix's
    flip/determinant sign convention and any SIP/TPV distortion terms the
    WCS carries, at the cost of two cheap coordinate transforms instead of
    a few trig ops on the raw matrix elements. Verified empirically against
    a hand-built rotated CD matrix (see tests/test_astrometry.py) — a frame
    whose data was produced by rotating another by exactly N degrees
    (in the scipy.ndimage.rotate sense) measures a position_angle_deg N
    degrees larger, mod 360.

    Two frames with ``|PA_a - PA_b| ~= 180`` are rotated ~180 deg relative
    to each other on the sky (e.g. a meridian flip) — this is diagnostic
    information, not a reason to exclude either frame from anything; see
    CLAUDE.md's "camera rotation" discussion.

    Returns None if the WCS round trip fails for any reason (e.g. a
    pathological/degenerate WCS) — callers must treat this the same as any
    other "orientation unknown" case, not crash.
    """
    try:
        center = wcs.pixel_to_world(cx, cy)
        # A point 1 arcsec further north (same RA, slightly greater Dec)
        # than the evaluation pixel — small enough to stay in the WCS's
        # locally-linear regime for any real telescope's plate scale, and
        # unaffected by the local RA wrap/pole singularities that a larger
        # offset could hit.
        north = SkyCoord(ra=center.ra, dec=center.dec + 1.0 * u.arcsec)
        north_x, north_y = wcs.world_to_pixel(north)
        dx = float(north_x) - cx
        dy = float(north_y) - cy
        if dx == 0.0 and dy == 0.0:
            return None
        return math.degrees(math.atan2(dx, dy)) % 360.0
    except Exception as exc:
        logger.debug("Position angle computation failed: %s", exc)
        return None


def _frame_center_and_scale(
    wcs: WCS,
    naxis1: int,
    naxis2: int,
    fits_filename: str,
) -> tuple[float, float, float, float, float | None]:
    """
    Compute the frame centre (RA, Dec), field of view, pixel scale, and
    position angle from a solved WCS.

    pixel_scale_matrix is [[CD1_1, CD1_2], [CD2_1, CD2_2]] in deg/px. The
    true plate scale along each axis is the norm of each column (handles
    rotation and shear). We use column 0 (RA axis) which carries the
    dominant scale factor.

    Returns
    -------
    tuple[float, float, float, float, float | None]
        (ra_center, dec_center, fov_deg, pixel_scale_arcsec, position_angle_deg)
    """
    cx: float = naxis1 / 2.0
    cy: float = naxis2 / 2.0
    sky = wcs.all_pix2world([[cx, cy]], 0)
    ra_center: float = float(sky[0][0])
    dec_center: float = float(sky[0][1])

    ps_matrix = wcs.pixel_scale_matrix   # shape (2, 2), units deg/px
    pixel_scale_deg: float = float(
        np.sqrt(ps_matrix[0, 0] ** 2 + ps_matrix[1, 0] ** 2)
    )
    pixel_scale_arcsec: float = pixel_scale_deg * 3600.0

    fov_deg: float = float(max(naxis1, naxis2) * pixel_scale_deg)

    position_angle_deg = _position_angle_deg(wcs, cx, cy)

    logger.info(
        "WCS solution: center=(%.5f, %.5f)  fov=%.4f°  scale=%.4f\"/px  "
        "PA=%s  image=%dx%d px  file=%s",
        ra_center,
        dec_center,
        fov_deg,
        pixel_scale_arcsec,
        f"{position_angle_deg:.2f}°" if position_angle_deg is not None else "unknown",
        naxis1, naxis2,
        fits_filename,
    )

    # Log WCS matrix for debugging
    logger.debug(
        "WCS matrix: CD1_1=%.6e CD1_2=%.6e CD2_1=%.6e CD2_2=%.6e  file=%s",
        ps_matrix[0, 0], ps_matrix[0, 1],
        ps_matrix[1, 0], ps_matrix[1, 1],
        fits_filename,
    )

    return ra_center, dec_center, fov_deg, pixel_scale_arcsec, position_angle_deg
