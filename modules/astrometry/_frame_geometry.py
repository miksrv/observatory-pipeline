"""
modules/astrometry/_frame_geometry.py — Step 3: deriving the frame centre,
field of view, and pixel scale from a solved WCS.

Internal helper only — not part of this package's public surface.
"""

from __future__ import annotations

import logging

import numpy as np
from astropy.wcs import WCS

logger = logging.getLogger(__name__)


def _frame_center_and_scale(
    wcs: WCS,
    naxis1: int,
    naxis2: int,
    fits_filename: str,
) -> tuple[float, float, float, float]:
    """
    Compute the frame centre (RA, Dec), field of view, and pixel scale from
    a solved WCS.

    pixel_scale_matrix is [[CD1_1, CD1_2], [CD2_1, CD2_2]] in deg/px. The
    true plate scale along each axis is the norm of each column (handles
    rotation and shear). We use column 0 (RA axis) which carries the
    dominant scale factor.

    Returns
    -------
    tuple[float, float, float, float]
        (ra_center, dec_center, fov_deg, pixel_scale_arcsec)
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

    logger.info(
        "WCS solution: center=(%.5f, %.5f)  fov=%.4f°  scale=%.4f\"/px  "
        "image=%dx%d px  file=%s",
        ra_center,
        dec_center,
        fov_deg,
        pixel_scale_arcsec,
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

    return ra_center, dec_center, fov_deg, pixel_scale_arcsec
