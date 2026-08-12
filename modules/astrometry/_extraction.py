"""
modules/astrometry/_extraction.py — Step 4: sep source extraction, star
filtering, and the "sources"/"sources_all" split.

Internal helper only — not part of this package's public surface.
"""

from __future__ import annotations

import logging

import astropy.io.fits as fits
import numpy as np
import sep
from astropy.wcs import WCS

import config

from ._streak import _build_streak_mask

logger = logging.getLogger(__name__)


def _extract_sources(
    fits_path: str,
    wcs: WCS,
    pixel_scale_arcsec: float,
    naxis1: int,
    naxis2: int,
    psf_fwhm_arcsec: float | None,
    fits_filename: str,
) -> tuple[list[dict], list[dict]]:
    """
    Run sep source extraction (after streak masking) and split the result
    into the strict ``sources`` star filter and the loose ``sources_all``
    detection list.

    Before extraction, a coarse streak-masking pre-pass (see
    ``_streak._build_streak_mask()``) removes satellite/aircraft trails and
    bright-star diffraction-spike arms from the image so they cannot
    fragment into spurious point-like "stars" in either list.

    Returns
    -------
    tuple[list[dict], list[dict]]
        (sources, sources_all) — see ``__init__.py``'s ``solve()`` docstring
        for the field shape of each source dict.
    """
    with fits.open(fits_path) as hdul:
        data: np.ndarray = np.ascontiguousarray(
            hdul[0].data.astype(np.float64)
        )

    bkg = sep.Background(data)
    data_sub: np.ndarray = data - bkg

    # Streak masking — satellite/aircraft trails and diffraction-spike
    # arms, run BEFORE the real point-source extraction below so they
    # can never fragment into false stars. See config.STREAK_* and
    # _build_streak_mask()'s docstring.
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

    return sources, sources_all
