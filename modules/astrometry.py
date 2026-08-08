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


# ---------------------------------------------------------------------------
# Streak masking — see config.STREAK_* and CLAUDE.md's astrometry.py section.
# ---------------------------------------------------------------------------

def _build_streak_mask(
    data_sub: np.ndarray,
    rms: float,
    pixel_scale_arcsec: float | None,
) -> np.ndarray | None:
    """
    Coarse, low-threshold, non-deblended pre-pass that finds long thin
    streaks — satellite/aircraft trails crossing a single exposure, and
    diffraction-spike arms radiating from bright/saturated stars — and
    returns a boolean pixel mask covering them, or None if none were found.

    Why a *separate* pass rather than tuning the real extraction's own
    deblend_cont: a trail/spike is frequently too faint along parts of its
    length to stay connected as one sep object even with deblending fully
    disabled — real data (2026-08-07, T_CrB test frame with a full-frame
    satellite trail): the trail split into two disconnected coarse
    components purely from brightness gaps along its own length, unrelated
    to deblending. Because of that, this pre-pass's job is only to flag
    *which* connected components are streak-like and mask them; the real
    extraction below is completely untouched and stays exactly as capable of
    splitting a genuinely close double star in a crowded field as before
    this fix — only pixels belonging to a coarse candidate that is BOTH
    highly elongated (>= config.STREAK_ELONGATION_MIN) AND far longer than
    any real stellar PSF footprint (bounding-box diagonal >=
    config.STREAK_MIN_LENGTH_ARCSEC) get masked, a combination no ordinary
    star (or deblended star pair) reaches.

    Verified against real data (2026-08-07,
    T_CrB_Light_L_60_2024-05-28T19-06-10.fits): without this pre-pass, the
    production extraction (thresh=10σ, deblend_cont=0.005) reported 5
    separate small, roundish (elongation < 3), unmatched "stars" sitting
    exactly along the satellite trail. With it, all 5 disappear and the star
    count elsewhere in the frame is unaffected (757 -> 752 raw detections).

    Parameters
    ----------
    data_sub:
        Background-subtracted image data — the same array the real
        extraction will run on.
    rms:
        Background RMS (bkg.globalrms) — used as this coarse pass's relative
        detection threshold (config.STREAK_DETECT_SIGMA), same convention as
        the real extraction below.
    pixel_scale_arcsec:
        Plate scale in arcsec/px, used to convert
        config.STREAK_MIN_LENGTH_ARCSEC/STREAK_MASK_DILATE_ARCSEC into
        pixels. None or <= 0 (qc.py, when the FITS header carries no
        XPIXSZ/FOCALLEN/PIXSCALE) falls back to a conservative fixed 200px
        length floor and a 1px dilation, so this pre-pass still does
        something rather than silently never firing.

    Returns
    -------
    np.ndarray | None
        Boolean mask, same shape as data_sub, or None when nothing
        streak-like was found (the common case) or the coarse pass itself
        failed — callers should treat None as "nothing to mask".
    """
    if rms is None or rms <= 0:
        return None

    try:
        objs, seg = sep.extract(
            data_sub,
            thresh=config.STREAK_DETECT_SIGMA,
            err=rms,
            minarea=config.SEP_MIN_AREA,
            deblend_cont=1.0,  # disabled — see docstring above
            segmentation_map=True,
        )
    except Exception as exc:
        logger.debug("Streak coarse pass failed: %s", exc)
        return None

    if len(objs) == 0:
        return None

    safe_b = np.where(objs["b"] > 0, objs["b"], 1e-6)
    elongation = objs["a"] / safe_b
    bbox_diag_px = np.sqrt(
        (objs["xmax"] - objs["xmin"]).astype(np.float64) ** 2
        + (objs["ymax"] - objs["ymin"]).astype(np.float64) ** 2
    )

    if pixel_scale_arcsec and pixel_scale_arcsec > 0:
        min_len_px = config.STREAK_MIN_LENGTH_ARCSEC / pixel_scale_arcsec
    else:
        min_len_px = 200.0

    streak_idx = np.where(
        (elongation >= config.STREAK_ELONGATION_MIN) & (bbox_diag_px >= min_len_px)
    )[0]
    if len(streak_idx) == 0:
        return None

    # seg is 1-indexed (0 = background), matching objs' row order.
    mask = np.isin(seg, streak_idx + 1)

    dilate_px = 1
    if pixel_scale_arcsec and pixel_scale_arcsec > 0:
        dilate_px = max(1, int(round(config.STREAK_MASK_DILATE_ARCSEC / pixel_scale_arcsec)))
    try:
        from scipy.ndimage import binary_dilation
        structure = np.ones((2 * dilate_px + 1, 2 * dilate_px + 1), dtype=bool)
        mask = binary_dilation(mask, structure=structure)
    except Exception as exc:
        logger.debug(
            "Streak mask dilation failed (%s) — using un-dilated mask", exc
        )

    logger.info(
        "Streak masking: %d streak-like feature(s) found (elongation>=%.1f, "
        "length>=%.0fpx), masking %d pixel(s)",
        len(streak_idx), config.STREAK_ELONGATION_MIN, min_len_px, int(mask.sum()),
    )
    return mask


async def solve(
    fits_path: str,
    psf_fwhm_arcsec: float | None = None,
    output_base: str | None = None,
) -> dict[str, Any]:
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
        star filter's FWHM bounds are tightened around it: the upper bound to
        ``psf_fwhm_arcsec * 1.5`` (capped at ``STAR_FWHM_MAX_ARCSEC``) to
        better reject compact galaxies and other extended sources whose FWHM
        significantly exceeds stellar PSF, and the lower bound to
        ``psf_fwhm_arcsec / 1.5`` (floored at ``STAR_FWHM_MIN_ARCSEC``) to
        reject hot/warm pixel clusters and other artifacts that are far
        sharper than any real star in this frame.
    output_base:
        Base path (no extension) astap should write its own output files
        under — ``-o`` on the astap command line. astap only ever opens
        ``fits_path`` itself for writing when invoked with ``-update``
        (which we never pass, here or anywhere else in this module); without
        it, ``-o`` affects only where the ``.ini``/``.wcs``/``.log`` side
        files land, never the input. Defaults to None, which keeps astap's
        own default of writing them next to ``fits_path`` (production
        behaviour, unchanged) — debug/debug_catalog_match.py passes a
        scratch path here instead, so repeatedly running it doesn't litter
        the debug frame's own directory with side files.

    Returns
    -------
    On success, a dict with keys:
        ra_center   float   – frame centre RA in decimal degrees
        dec_center  float   – frame centre Dec in decimal degrees
        fov_deg     float   – field of view (larger image dimension) in degrees
        naxis1      int     – image width in pixels
        naxis2      int     – image height in pixels
        sources     list    – list of source dicts; each has:
                              ra, dec, flux, fwhm (arcsec), elongation (a/b),
                              saturated (bool — peak ADU >= config.SATURATION_ADU;
                              see docs/ISSUES.md #2),
                              near_edge (bool — pixel position within
                              config.EDGE_MARGIN_FRAC of any frame edge; lets
                              anomaly_detector.py demand stronger elongation
                              evidence there, since coma inflates it near the
                              edge of a wide-field frame)
        wcs         WCS     – astropy WCS object for downstream coordinate work

    Before source extraction, a coarse streak-masking pre-pass (see
    ``_build_streak_mask()`` above and ``config.STREAK_*``) removes satellite/
    aircraft trails and bright-star diffraction-spike arms from the image so
    they cannot fragment into spurious point-like "stars" in either
    ``sources`` or ``sources_all``.

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
        "-wcs",           # write the solved WCS to a .wcs side file (see
                          # Step 2 below — no -update here, so fits_path
                          # itself is never opened for writing by astap)
    ]

    if output_base:
        cmd.extend(["-o", output_base])  # redirect .ini/.wcs/.log only

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
        # Step 2 — Read WCS: prefer astap's own freshly-solved .wcs side
        # file over whatever WCS keywords fits_path's own header may already
        # carry.
        #
        # Real incident (2026-08-06, "UGC_6930" test frame): the incoming
        # FITS already had CTYPE1/CRVAL1/CD* in its header (written by the
        # capture software from mount pointing, per its own "Generated by
        # INDI" comment — not a genuine plate solve). wcs.has_celestial was
        # already True for THAT header, so the old code here never even
        # looked at the .wcs file astap had just written — it silently kept
        # using the mount-pointing estimate for every source's RA/Dec, Gaia
        # zero-point, everything downstream. astap's own .wcs comment for
        # that same run: "Solved in 0.1 sec. Offset 3.0'. Mount offset
        # RA=-0.2', DEC=-2.9'" — astap had already found and reported the
        # ~178" correction; the old priority order just threw it away. We
        # just confirmed above ("Solution found" in astap_output) that this
        # run's solve succeeded, so its own .wcs is the authoritative
        # result — read it first, and only fall back to the FITS header's
        # own WCS if that side file is missing or unexpectedly invalid.
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
                            if key.startswith("PC") or key.startswith("CDELT"):
                                del wcs_hdr[key]
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

        # Streak masking — satellite/aircraft trails and diffraction-spike
        # arms, run BEFORE the real point-source extraction below so they
        # can never fragment into false stars. See config.STREAK_* and
        # _build_streak_mask()'s docstring above.
        streak_mask = _build_streak_mask(data_sub, bkg.globalrms, pixel_scale_arcsec)
        if streak_mask is not None:
            data_sub = np.array(data_sub, copy=True)
            data_sub[streak_mask] = 0.0

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
            # Saturation flag — see docs/ISSUES.md #2.
            #
            # sep's "peak" field is background-SUBTRACTED, so add the global
            # background level back to approximate the true raw ADU value at
            # the object's brightest pixel. This is a coarse per-object check
            # (not pixel-exact), but sufficient to flag the saturated cores
            # that were otherwise silently producing extreme (e.g. -14 mag)
            # magnitudes downstream in photometry.py, since aperture flux on
            # a clipped PSF core is not a physically meaningful measurement.
            # ---------------------------------------------------------
            raw_peak: np.ndarray = objects["peak"] + bkg.globalback
            saturated_mask: np.ndarray = raw_peak >= config.SATURATION_ADU
            n_saturated = int(np.sum(saturated_mask))
            if n_saturated:
                logger.info(
                    "Saturation check: %d/%d raw detection(s) at/above "
                    "SATURATION_ADU=%.0f  file=%s",
                    n_saturated, len(objects), config.SATURATION_ADU,
                    fits_filename,
                )

            # ---------------------------------------------------------
            # Near-edge geometry flag — see config.EDGE_MARGIN_FRAC.
            #
            # Coma and other off-axis aberrations progressively distort a
            # star's PSF toward the edges/corners of a wide-field frame,
            # inflating its measured elongation for purely optical reasons —
            # not because it's moving or trailing. Flagged here, once,
            # geometrically from each detection's own pixel position (no WCS
            # needed — this is a purely detector-space property), so that
            # anomaly_detector.py can require stronger elongation evidence
            # before treating an edge source as a single-exposure trail (real
            # incident, 2026-08-07: 4 T_CrB frames produced 305 anomalies,
            # dominated by coma-elongated but otherwise ordinary corner stars
            # firing the SPACE_DEBRIS shortcut). Deliberately NO leading
            # underscore (unlike "_source_id"/"_filter" etc.) — mirrors
            # "saturated" below: it must survive api_client._to_wire_source()
            # and be persisted on source_observations, since a standalone
            # DETECT_ANOMALIES re-run (pipeline.py's _from_wire_source())
            # reconstructs sources purely from stored API data, with no
            # in-memory pixel position to recompute this from.
            # ---------------------------------------------------------
            margin_x = config.EDGE_MARGIN_FRAC * naxis1
            margin_y = config.EDGE_MARGIN_FRAC * naxis2
            near_edge_mask: np.ndarray = (
                (objects["x"] < margin_x)
                | (objects["x"] > naxis1 - margin_x)
                | (objects["y"] < margin_y)
                | (objects["y"] > naxis2 - margin_y)
            )

            # ---------------------------------------------------------
            # Star filtering criteria:
            # 1. Elongation < max (stars are round, trails/galaxies are elongated)
            # 2. FWHM in reasonable range (reject hot pixels and extended objects)
            # 3. SNR > min (reject faint noise detections)
            # 4. Positive flux (reject artifacts)
            #
            # When psf_fwhm_arcsec is provided from QC, BOTH FWHM bounds are
            # tightened around it:
            #   - upper bound -> psf_fwhm_arcsec * 1.5, to reject compact galaxies
            #     that are slightly broader than the stellar PSF but still pass
            #     the static STAR_FWHM_MAX_ARCSEC threshold.
            #   - lower bound -> psf_fwhm_arcsec / 1.5, to reject sources that are
            #     dramatically SHARPER than every real star in this same frame.
            #     A genuine point source's profile is set by the shared
            #     atmospheric/optical PSF, so it cannot be much narrower than
            #     what every other star in the frame actually measures.
            #     STAR_FWHM_MIN_ARCSEC alone is a static, site-agnostic floor
            #     (default 2.5") that a hot/warm pixel cluster can sit
            #     comfortably above while still being far more compact than any
            #     real star here — this was observed 2026-08-06 on Vesta test
            #     frames, where sensor hot pixels around 2.6-3.0" FWHM sailed
            #     through the static floor even though this frame's own stars
            #     measured ~4.5" FWHM, and ended up posted as UNKNOWN anomalies.
            # ---------------------------------------------------------

            fwhm_max_arcsec = config.STAR_FWHM_MAX_ARCSEC
            fwhm_min_arcsec = config.STAR_FWHM_MIN_ARCSEC
            if psf_fwhm_arcsec is not None and psf_fwhm_arcsec > 0:
                fwhm_max_arcsec = min(config.STAR_FWHM_MAX_ARCSEC, psf_fwhm_arcsec * 1.5)
                fwhm_min_arcsec = max(config.STAR_FWHM_MIN_ARCSEC, psf_fwhm_arcsec / 1.5)

            # Count rejections per criterion for debugging
            mask_elongation = elongations < config.STAR_ELONGATION_MAX
            mask_fwhm_min = fwhm_arcsec >= fwhm_min_arcsec
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
                fwhm_min_arcsec, fwhm_max_arcsec,
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
                    "saturated":  bool(saturated_mask[i]),
                    "near_edge":  bool(near_edge_mask[i]),
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
            #   - FWHM >= fwhm_min_arcsec (same PSF-tightened floor as star_mask
            #     above — rejects single-pixel hot pixels, and, when a per-frame
            #     PSF estimate is available, multi-pixel hot/warm pixel clusters
            #     too, even when their measured FWHM clears the static
            #     STAR_FWHM_MIN_ARCSEC default)
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
                    "saturated":  bool(saturated_mask[i]),
                    "near_edge":  bool(near_edge_mask[i]),
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
