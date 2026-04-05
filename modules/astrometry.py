"""
modules/astrometry.py — Plate solving and source extraction for FITS frames.

The single public entry point is:

    await astrometry.solve(fits_path: str) -> dict

It calls the astap binary for plate solving (writing WCS keywords back into
the FITS file), then reads the WCS, computes the frame centre and FOV, and
runs sep (SourceExtractor) to build a source list with (RA, Dec) coordinates.

Returns an empty dict on any failure so the pipeline can detect and handle
the error without crashing.
"""

from __future__ import annotations

import logging
import os
import subprocess
from typing import Any

import astropy.io.fits as fits
import numpy as np
import sep
from astropy.wcs import WCS

import config

logger = logging.getLogger(__name__)


async def solve(fits_path: str, psf_fwhm_arcsec: float | None = None) -> dict[str, Any]:
    """
    Plate-solve a FITS frame and extract calibrated source positions.

    Runs astap for plate solving, then reads the WCS it writes back into
    the file, computes the frame centre and FOV, and runs sep for source
    extraction.  All (x, y) pixel positions are converted to (RA, Dec)
    using the solved WCS.

    Parameters
    ----------
    fits_path:
        Absolute path to the FITS file on disk.
    psf_fwhm_arcsec:
        Median PSF FWHM in arcseconds from QC analysis. When provided, the
        star filter upper FWHM bound is tightened to ``psf_fwhm_arcsec * 1.5``
        (capped at ``STAR_FWHM_MAX_ARCSEC``) to better reject compact galaxies
        and other extended sources whose FWHM significantly exceeds stellar PSF.

    Returns
    -------
    On success, a dict with keys:
        ra_center   float   – frame centre RA in decimal degrees
        dec_center  float   – frame centre Dec in decimal degrees
        fov_deg     float   – field of view (larger image dimension) in degrees
        naxis1      int     – image width in pixels
        naxis2      int     – image height in pixels
        sources     list    – list of source dicts; each has:
                              ra, dec, flux, fwhm (arcsec), elongation (a/b)
        wcs         WCS     – astropy WCS object for downstream coordinate work

    Returns ``{}`` on any failure (astap error, WCS invalid, sep failure).
    """
    fits_filename = os.path.basename(fits_path)
    logger.info("Starting astrometry for fits_filename=%s", fits_filename)

    # ------------------------------------------------------------------
    # Step 1 — Run astap plate solver
    # ------------------------------------------------------------------
    # Use xvfb-run to provide a virtual display for astap (GTK app)
    cmd: list[str] = [
        "xvfb-run", "-a",
        config.ASTAP_BINARY,
        "-f", fits_path,
        "-d", config.ASTAP_CATALOGS,
        "-speed", "0",    # accuracy: 0 = highest
        "-wcs",           # write WCS back into the FITS file
    ]
    
    # Add FOV hint if configured (helps with plate scale accuracy)
    if config.ASTAP_FOV_HINT > 0:
        cmd.extend(["-fov", str(config.ASTAP_FOV_HINT)])
        cmd.extend(["-r", "10"])  # narrow search radius when FOV is known
        logger.debug("ASTAP using explicit FOV hint: %.2f°", config.ASTAP_FOV_HINT)
    else:
        cmd.extend(["-r", "0"])   # auto-detect from FITS headers

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        logger.warning("astap timed out after 60s for %s", fits_path)
        return {}
    except FileNotFoundError:
        logger.error(
            "astap binary not found at %s — plate solving disabled",
            config.ASTAP_BINARY,
        )
        return {}
    except PermissionError:
        logger.error(
            "astap binary at %s is not executable (permission denied). "
            "This may happen if astap is not available for this CPU architecture "
            "(e.g., running on ARM/Apple Silicon when only amd64 binary exists). "
            "Plate solving is disabled.",
            config.ASTAP_BINARY,
        )
        return {}
    except OSError as exc:
        logger.error(
            "Failed to execute astap at %s: %s — plate solving disabled",
            config.ASTAP_BINARY,
            exc,
        )
        return {}

    if result.returncode != 0:
        logger.warning(
            "astap failed (rc=%d) for %s: %s",
            result.returncode,
            fits_path,
            result.stderr[:200],
        )
        return {}

    # Check astap stdout for "Solution found" or similar success indicator
    # astap outputs "Solution found:" when plate solve succeeds
    astap_output = result.stdout + result.stderr
    if "Solution found" not in astap_output and "solution found" not in astap_output.lower():
        logger.warning(
            "astap returned rc=0 but no solution found in output for %s. Output: %s",
            fits_path,
            astap_output[:500],
        )
        return {}

    logger.debug("astap succeeded for %s", fits_filename)

    # ------------------------------------------------------------------
    # Steps 2–4 — WCS extraction, centre/FOV computation, sep extraction
    # ------------------------------------------------------------------
    try:
        # Step 2 — Read WCS from the solved FITS file
        # astap with -wcs flag should write WCS keywords into the FITS header.
        # As a fallback, check for a separate .wcs file created by astap.
        
        wcs = None
        hdr = None
        naxis1: int = 0
        naxis2: int = 0
        
        with fits.open(fits_path) as hdul:
            hdr = hdul[0].header.copy()
            naxis1 = int(hdr.get("NAXIS1", 0))
            naxis2 = int(hdr.get("NAXIS2", 0))
            wcs = WCS(hdr)
        
        # Check if WCS from FITS has celestial coordinates
        if not wcs.has_celestial:
            # Try to read from .wcs file that astap creates
            wcs_file_path = os.path.splitext(fits_path)[0] + ".wcs"
            if os.path.exists(wcs_file_path):
                logger.info(
                    "FITS has no celestial WCS, trying .wcs file: %s",
                    wcs_file_path,
                )
                try:
                    with fits.open(wcs_file_path) as wcs_hdul:
                        wcs_hdr = wcs_hdul[0].header
                        wcs = WCS(wcs_hdr)
                        # Merge WCS keywords into main header for downstream use
                        for key in ["CTYPE1", "CTYPE2", "CRVAL1", "CRVAL2", 
                                    "CRPIX1", "CRPIX2", "CD1_1", "CD1_2", 
                                    "CD2_1", "CD2_2", "CDELT1", "CDELT2"]:
                            if key in wcs_hdr:
                                hdr[key] = wcs_hdr[key]
                except Exception as wcs_exc:
                    logger.warning(
                        "Failed to read .wcs file %s: %s",
                        wcs_file_path,
                        wcs_exc,
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
            return {}

        # Step 3 — Frame centre and FOV
        cx: float = naxis1 / 2.0
        cy: float = naxis2 / 2.0
        sky = wcs.all_pix2world([[cx, cy]], 0)
        ra_center: float = float(sky[0][0])
        dec_center: float = float(sky[0][1])

        # pixel_scale_matrix is [[CD1_1, CD1_2], [CD2_1, CD2_2]] in deg/px.
        # The true plate scale along each axis is the norm of each column
        # (handles rotation and shear).  We use column 0 (RA axis) which
        # carries the dominant scale factor.
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

        # Step 4 — Source extraction with sep
        with fits.open(fits_path) as hdul:
            data: np.ndarray = np.ascontiguousarray(
                hdul[0].data.astype(np.float64)
            )

        bkg = sep.Background(data)
        data_sub: np.ndarray = data - bkg
        
        # Extract sources using configurable thresholds
        # Higher thresh = fewer detections (more conservative)
        # Higher minarea = reject smaller artifacts
        objects = sep.extract(
            data_sub, 
            thresh=config.SEP_DETECT_THRESH,
            err=bkg.globalrms,
            minarea=config.SEP_MIN_AREA,
            deblend_cont=0.005,
        )
        
        logger.info(
            "SEP extraction: %d raw objects (thresh=%.1fσ, minarea=%d)  file=%s",
            len(objects),
            config.SEP_DETECT_THRESH,
            config.SEP_MIN_AREA,
            fits_filename,
        )

        sources: list[dict[str, float]]
        if len(objects) > 0:
            coords = wcs.all_pix2world(
                np.column_stack([objects["x"], objects["y"]]), 0
            )

            # FWHM formula: 2 * sqrt(2 * ln(2) * mean_variance)
            # where mean_variance = (a^2 + b^2) / 2 (quadrature mean of axes)
            fwhm_px: np.ndarray = (
                2.0
                * np.sqrt(
                    2.0
                    * np.log(2.0)
                    * (objects["a"] ** 2 + objects["b"] ** 2)
                    / 2.0
                )
            )
            fwhm_arcsec: np.ndarray = fwhm_px * pixel_scale_arcsec

            # Guard against zero minor axis (degenerate sources)
            safe_b: np.ndarray = np.where(objects["b"] > 0, objects["b"], 1e-6)
            elongations: np.ndarray = objects["a"] / safe_b
            
            # SNR calculation using peak value over background RMS
            # This is more meaningful than flux-based SNR for star detection
            # peak = maximum pixel value in the source aperture (above background)
            # SNR = peak / bkg.globalrms
            snr: np.ndarray = objects["peak"] / bkg.globalrms

            # ---------------------------------------------------------
            # Star filtering criteria:
            # 1. Elongation < max (stars are round, trails/galaxies are elongated)
            # 2. FWHM in reasonable range (reject hot pixels and extended objects)
            # 3. SNR > min (reject faint noise detections)
            # 4. Positive flux (reject artifacts)
            #
            # When psf_fwhm_arcsec is provided from QC, the upper FWHM bound is
            # tightened to psf_fwhm_arcsec * 1.5 to reject compact galaxies that
            # are slightly broader than the stellar PSF but still pass the
            # static STAR_FWHM_MAX_ARCSEC threshold.
            # ---------------------------------------------------------

            fwhm_max_arcsec = config.STAR_FWHM_MAX_ARCSEC
            if psf_fwhm_arcsec is not None and psf_fwhm_arcsec > 0:
                fwhm_max_arcsec = min(config.STAR_FWHM_MAX_ARCSEC, psf_fwhm_arcsec * 1.5)

            # Count rejections per criterion for debugging
            mask_elongation = elongations < config.STAR_ELONGATION_MAX
            mask_fwhm_min = fwhm_arcsec >= config.STAR_FWHM_MIN_ARCSEC
            mask_fwhm_max = fwhm_arcsec <= fwhm_max_arcsec
            mask_snr = snr >= config.STAR_SNR_MIN
            mask_flux = objects["flux"] > 0
            
            star_mask = mask_elongation & mask_fwhm_min & mask_fwhm_max & mask_snr & mask_flux
            
            n_total = len(objects)
            n_stars = int(np.sum(star_mask))
            
            # Detailed rejection stats
            rej_elongation = int(np.sum(~mask_elongation))
            rej_fwhm_small = int(np.sum(~mask_fwhm_min))
            rej_fwhm_large = int(np.sum(~mask_fwhm_max))
            rej_snr = int(np.sum(~mask_snr))
            rej_flux = int(np.sum(~mask_flux))
            
            # Log SNR and FWHM ranges for tuning
            logger.info(
                "Source stats: SNR=[%.1f-%.1f], FWHM=[%.2f-%.2f]\", elong=[%.2f-%.2f]  file=%s",
                float(np.min(snr)), float(np.max(snr)),
                float(np.min(fwhm_arcsec)), float(np.max(fwhm_arcsec)),
                float(np.min(elongations)), float(np.max(elongations)),
                fits_filename,
            )
            
            logger.info(
                "Star filter: %d raw → %d stars | rejected: elongation=%d, fwhm_small=%d, "
                "fwhm_large=%d, low_snr=%d, neg_flux=%d  file=%s",
                n_total, n_stars, rej_elongation, rej_fwhm_small, 
                rej_fwhm_large, rej_snr, rej_flux, fits_filename,
            )
            
            # Log filter thresholds for reference
            logger.info(
                "Filter thresholds: FWHM=[%.1f-%.1f]\"%s, elong<%.1f, SNR>%.1f  file=%s",
                config.STAR_FWHM_MIN_ARCSEC, fwhm_max_arcsec,
                " (PSF-based)" if psf_fwhm_arcsec is not None else "",
                config.STAR_ELONGATION_MAX, config.STAR_SNR_MIN, fits_filename,
            )

            sources = [
                {
                    "ra":         float(coords[i, 0]),
                    "dec":        float(coords[i, 1]),
                    "flux":       float(objects["flux"][i]),
                    "fwhm":       float(fwhm_arcsec[i]),
                    "elongation": float(elongations[i]),
                }
                for i in range(len(objects))
                if star_mask[i]
            ]

            # ----------------------------------------------------------
            # "sources_all" — loose filter for anomaly detection.
            #
            # The strict star_mask above intentionally rejects:
            #   - Bright saturated objects (large FWHM, e.g. asteroids)
            #   - Faint stars below STAR_SNR_MIN (useful for WCS correction)
            #   - Compact galaxies above the PSF-based FWHM limit
            #
            # sources_all keeps everything with:
            #   - FWHM >= STAR_FWHM_MIN_ARCSEC (rejects single-pixel hot pixels)
            #   - elongation < 5.0  (rejects strongly trailed cosmic rays)
            #   - positive flux
            #
            # Used by: catalog_matcher (more sources → better WCS correction),
            #          anomaly_detector (detects moving/transient objects),
            #          API post_sources (complete detection record).
            # Photometry calibration still uses `sources` (strict stars only).
            # ----------------------------------------------------------
            mask_all = mask_fwhm_min & (elongations < 5.0) & mask_flux
            n_all = int(np.sum(mask_all))

            sources_all = [
                {
                    "ra":         float(coords[i, 0]),
                    "dec":        float(coords[i, 1]),
                    "flux":       float(objects["flux"][i]),
                    "fwhm":       float(fwhm_arcsec[i]),
                    "elongation": float(elongations[i]),
                }
                for i in range(len(objects))
                if mask_all[i]
            ]

            logger.info(
                "Astrometry complete: %d strict stars + %d total detections (sources_all)  file=%s",
                len(sources), n_all, fits_filename,
            )
        else:
            sources = []
            sources_all = []
            logger.info("Astrometry complete: 0 sources extracted  file=%s", fits_filename)

        return {
            "ra_center":   ra_center,
            "dec_center":  dec_center,
            "fov_deg":     fov_deg,
            "naxis1":      naxis1,
            "naxis2":      naxis2,
            "sources":     sources,      # strict stars: for photometry calibration only
            "sources_all": sources_all,  # all detections: for catalog matching + anomaly detection
            "wcs":         wcs,
        }

    except Exception as exc:
        logger.error(
            "Astrometry post-processing failed for %s: %s", fits_path, exc
        )
        return {}
