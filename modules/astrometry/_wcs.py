"""
modules/astrometry/_wcs.py — Step 2: reading the WCS astap just solved (or
falling back to whatever WCS the FITS header itself already carries) and
validating it actually has celestial axes.

Internal helper only — not part of this package's public surface.
"""

from __future__ import annotations

import logging
import math
import os
import re

import astropy.io.fits as fits
import numpy as np
from astropy.wcs import WCS

import config

logger = logging.getLogger(__name__)


def _log_astap_solve_report(wcs_hdr, fits_filename: str) -> None:
    """
    Surface astap's own account of the solve — the free-text line it writes
    into the `.wcs` header's COMMENT/HISTORY cards, e.g. "Solved in 0.1 sec.
    Offset 3.0'. Mount offset RA=-0.2', DEC=-2.9'".

    Nothing had ever read it (audit 2026-08-18, finding H15). It is not
    machine-readable enough to gate on — astap's wording varies by version and
    by search mode — but it is the only per-solve quality statement the solver
    produces, and an operator looking at a suspect frame should not have to
    re-run astap by hand to see it.
    """
    try:
        for card_key in ("COMMENT", "HISTORY"):
            if card_key not in wcs_hdr:
                continue
            for line in wcs_hdr[card_key]:
                text = str(line).strip()
                if "solved" in text.lower() or "offset" in text.lower():
                    logger.info("astap solve report for %s: %s", fits_filename, text)
    except Exception:
        pass  # diagnostic only — never let it affect the solve


def _is_plausible_wcs(wcs: WCS, naxis1: int, naxis2: int, fits_filename: str) -> bool:
    """
    Whether a solved WCS describes a physically possible image of the sky.

    The WCS is authoritative by construction — every source position, every
    catalog match and every anomaly's coordinates come from it, and no
    downstream module has anything to check it against. Until now nothing
    checked it either, beyond astap reporting "Solution found" and the axes
    being celestial, so a false star-pattern match became a systematic
    position error for the whole frame with no distinguishing log line (audit
    2026-08-18, finding H15). The risk is concentrated in
    `ASTAP_RETRY_WIDE_SEARCH`'s blind 30-degree retry, where a wrong match is
    most likely to be found in the first place.

    The checks are structural rather than statistical, because astap's own
    residual and matched-star count exist only in a free-text comment whose
    wording varies by version (logged separately by
    `_log_astap_solve_report()`):

    * the reference coordinates are on the sphere at all;
    * the plate scale is finite and between
      ASTROMETRY_PIXEL_SCALE_MIN/MAX_ARCSEC — below the floor no amateur
      telescope resolves, above the ceiling the frame is not an image of a
      star field;
    * the transform is non-degenerate, i.e. the CD matrix has a non-zero
      determinant (a collapsed axis maps the whole frame onto a line);
    * a pixel-to-world round trip at the frame centre returns finite,
      on-sphere coordinates.

    A failure here is a hard failure, not a warning: a WCS this wrong is worse
    than none, since the frame's sources would be posted at confidently wrong
    coordinates and then compared against history at those coordinates.
    """
    try:
        crval = wcs.wcs.crval
        ra, dec = float(crval[0]), float(crval[1])
        if not (math.isfinite(ra) and math.isfinite(dec)) or not (-90.0 <= dec <= 90.0):
            logger.error(
                "Implausible WCS for %s: reference coordinates RA=%s Dec=%s are not on the sphere",
                fits_filename, ra, dec,
            )
            return False

        matrix = wcs.pixel_scale_matrix
        scale_x = math.hypot(float(matrix[0, 0]), float(matrix[1, 0])) * 3600.0
        scale_y = math.hypot(float(matrix[0, 1]), float(matrix[1, 1])) * 3600.0
        low = config.ASTROMETRY_PIXEL_SCALE_MIN_ARCSEC
        high = config.ASTROMETRY_PIXEL_SCALE_MAX_ARCSEC

        for axis, scale in (("x", scale_x), ("y", scale_y)):
            if not math.isfinite(scale) or not (low <= scale <= high):
                logger.error(
                    "Implausible WCS for %s: %s plate scale %.4f\"/px is outside "
                    "the %.2f-%.1f\"/px window — treating the solve as failed",
                    fits_filename, axis, scale, low, high,
                )
                return False

        determinant = float(np.linalg.det(matrix))
        if not math.isfinite(determinant) or determinant == 0.0:
            logger.error(
                "Implausible WCS for %s: the transform is degenerate "
                "(CD determinant %s) — the whole frame maps onto a line",
                fits_filename, determinant,
            )
            return False

        centre = wcs.all_pix2world([[naxis1 / 2.0, naxis2 / 2.0]], 0)[0]
        if not (math.isfinite(centre[0]) and math.isfinite(centre[1])) or not (-90.0 <= centre[1] <= 90.0):
            logger.error(
                "Implausible WCS for %s: the frame centre projects to RA=%s Dec=%s",
                fits_filename, centre[0], centre[1],
            )
            return False
    except Exception as exc:
        logger.error(
            "Could not validate the WCS for %s (%s) — treating the solve as failed",
            fits_filename, exc,
        )
        return False

    return True


# Indexed PC matrix / CDELT scale cards, with an optional alternate-WCS
# suffix letter. Matched as whole card names rather than by prefix: a bare
# "PC" also matches `PCOUNT`, a structural HDU keyword that has nothing to do
# with the WCS, so prefix matching would silently drop non-WCS metadata while
# reconciling astap's sidecar.
_PC_CDELT_CARD_RE = re.compile(r"^PC\d+_\d+[A-Z]?$|^CDELT\d+[A-Z]?$")


def _read_wcs(fits_path: str, output_base: str | None) -> tuple[WCS, int, int] | None:
    """
    Read the WCS for a just-solved frame, preferring astap's own fresh
    ``.wcs`` side file over whatever WCS keywords ``fits_path``'s own header
    may already carry.

    Real incident (2026-08-06, "UGC_6930" test frame): the incoming FITS
    already had CTYPE1/CRVAL1/CD* in its header (written by the capture
    software from mount pointing, per its own "Generated by INDI" comment —
    not a genuine plate solve). wcs.has_celestial was already True for THAT
    header, so the old code here never even looked at the .wcs file astap
    had just written — it silently kept using the mount-pointing estimate
    for every source's RA/Dec, Gaia zero-point, everything downstream.
    astap's own .wcs comment for that same run: "Solved in 0.1 sec. Offset
    3.0'. Mount offset RA=-0.2', DEC=-2.9'" — astap had already found and
    reported the ~178" correction; the old priority order just threw it
    away. By the time this is called, `_astap._run_astap()` has already
    confirmed ("Solution found" in astap_output) that this run's solve
    succeeded, so its own .wcs is the authoritative result — read it first,
    and only fall back to the FITS header's own WCS if that side file is
    missing or unexpectedly invalid.

    Returns
    -------
    tuple[WCS, int, int] | None
        (wcs, naxis1, naxis2) on success. None when the resulting WCS has no
        celestial axes at all — already logged in detail (the offending
        keywords found in the header, for diagnosis) before returning, so
        the caller (``solve()``) only needs to turn this into ``{}``.
    """
    fits_filename = os.path.basename(fits_path)

    wcs = None
    hdr = None
    naxis1: int = 0
    naxis2: int = 0

    with fits.open(fits_path) as hdul:
        hdr = hdul[0].header.copy()
        naxis1 = int(hdr.get("NAXIS1", 0))
        naxis2 = int(hdr.get("NAXIS2", 0))

    wcs_base = output_base if output_base else os.path.splitext(fits_path)[0]
    wcs_file_path = wcs_base + ".wcs"
    if os.path.exists(wcs_file_path):
        try:
            with fits.open(wcs_file_path) as wcs_hdul:
                wcs_hdr = wcs_hdul[0].header
                # astap writes BOTH CD* (correct, direct deg/px) AND
                # PC*+CDELT* into .wcs files. The PC values are NOT a
                # proper rotation matrix (det≈1) as the FITS standard
                # requires — they're just copies of the CD values.
                # When astropy sees both, pixel_scale_matrix computes
                # PC * diag(CDELT), double-applying the scale (real
                # incident 2026-08-06: 0.78"/px became 0.0002"/px,
                # making all FWHM values ≈0 and rejecting every source).
                # Fix: strip PC/CDELT when CD is present — CD is the
                # authoritative representation from astap's solver.
                if "CD1_1" in wcs_hdr:
                    for key in list(wcs_hdr.keys()):
                        if _PC_CDELT_CARD_RE.match(key.upper()):
                            del wcs_hdr[key]
                _log_astap_solve_report(wcs_hdr, fits_filename)
                wcs_candidate = WCS(wcs_hdr)
                if wcs_candidate.has_celestial:
                    wcs = wcs_candidate
                    # Merge WCS keywords into main header for downstream use
                    for key in ["CTYPE1", "CTYPE2", "CRVAL1", "CRVAL2",
                                "CRPIX1", "CRPIX2", "CD1_1", "CD1_2",
                                "CD2_1", "CD2_2", "CDELT1", "CDELT2"]:
                        if key in wcs_hdr:
                            hdr[key] = wcs_hdr[key]
                else:
                    logger.warning(
                        "astap's .wcs file %s has no celestial axes — "
                        "falling back to fits_path's own header WCS",
                        wcs_file_path,
                    )
        except Exception as wcs_exc:
            logger.warning(
                "Failed to read astap's .wcs file %s: %s — falling back "
                "to fits_path's own header WCS",
                wcs_file_path,
                wcs_exc,
            )
    else:
        logger.warning(
            "astap's .wcs file not found at %s despite a reported "
            "solution — falling back to fits_path's own header WCS",
            wcs_file_path,
        )

    if wcs is None:
        # Fallback: whatever WCS (if any) fits_path's own header already
        # carries. May be a genuine prior plate solve, or may just be an
        # approximate mount-pointing estimate — see the incident above.
        with fits.open(fits_path) as hdul:
            wcs = WCS(hdul[0].header)
        if wcs.has_celestial:
            logger.warning(
                "Using fits_path's own header WCS as a fallback for %s — "
                "this was not verified against astap's own solve and may "
                "be no more than a mount-pointing estimate",
                fits_filename,
            )

    if not wcs.has_celestial:
        # Log detailed WCS info for debugging
        logger.error(
            "WCS has no celestial axes after plate solve for %s", fits_path
        )
        # Check for common WCS keywords to diagnose the issue
        wcs_keys = ["CTYPE1", "CTYPE2", "CRVAL1", "CRVAL2", "CRPIX1", "CRPIX2",
                    "CD1_1", "CD1_2", "CD2_1", "CD2_2", "CDELT1", "CDELT2"]
        found_keys = {k: hdr.get(k) for k in wcs_keys if k in hdr}
        logger.error(
            "WCS keywords found: %s  file=%s",
            found_keys if found_keys else "NONE",
            fits_filename,
        )
        # Also check if astap wrote solution info
        astap_keys = ["PLTSOLVD", "CRVAL1", "CRVAL2"]
        astap_found = {k: hdr.get(k) for k in astap_keys if k in hdr}
        logger.error(
            "ASTAP solution keywords: %s  file=%s",
            astap_found if astap_found else "NONE",
            fits_filename,
        )
        return None

    # Celestial axes are necessary but nowhere near sufficient — see
    # _is_plausible_wcs(). A WCS that passes has_celestial while describing
    # something physically impossible is worse than no solve at all: the
    # frame's sources get posted at confidently wrong coordinates and are then
    # compared against history at those same wrong coordinates.
    if not _is_plausible_wcs(wcs, naxis1, naxis2, fits_filename):
        return None

    return wcs, naxis1, naxis2
