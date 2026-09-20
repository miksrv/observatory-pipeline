"""
modules/qc.py — Quality control analysis for raw FITS frames.

The single public entry point is:

    await qc.analyze(fits_path: str) -> dict

It measures image quality metrics (FWHM, elongation, SNR, sky background,
star count, cosmic ray fraction), classifies the frame, and — when rejected —
moves the file to the appropriate subdirectory under FITS_REJECTED before
returning (unless called with ``move_on_reject=False``).  No plate-solving
is required; all measurements use the raw pixel data only.

Rejected frames are never sent to the API.  The pipeline orchestrator checks
``quality_flag`` and stops processing if it is not ``"OK"``.
"""

from __future__ import annotations

import logging
import math
import os
import shutil
from datetime import datetime
from typing import Any

import astropy.io.fits as fits
import astroscrappy
import numpy as np
import sep

import config
from modules import fits_header
from modules.fits_header import extract_headers, sanitize_object_name
from modules.normalizer import is_narrowband

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _read_pixel_scale(hdr: fits.Header) -> float | None:
    """
    Derive the plate scale in arcsec/pixel from FITS headers.

    Delegates to `modules.fits_header.resolve_pixel_scale_arcsec()` rather
    than reading the keywords itself. This module and that one used to
    interpret the same ambiguous keywords differently, and both unsafely: this
    one took `PIXSCALE` as arcsec/px whenever it fell in a wide range, while
    that one took `PIXSCALE1` as microns unconditionally. The two ranges
    overlap — a 3.76 micron pixel and a 3.76"/px plate scale are the same
    number — so no range check can tell them apart, and the two modules could
    reach opposite conclusions about the same frame (audit 2026-08-18,
    finding M2).

    This is one of the few places this codebase shares a helper rather than
    hand-duplicating it, precisely because the finding is that the two copies
    disagreed.

    Returns None when the headers don't carry enough, which the caller
    already handles.
    """
    return fits_header.resolve_pixel_scale_arcsec(hdr)


def _build_streak_mask(
    data_sub: np.ndarray,
    rms: float,
    pixel_scale_arcsec: float | None,
) -> np.ndarray | None:
    """
    Coarse, low-threshold, non-deblended pre-pass that finds long thin
    streaks — satellite/aircraft trails and bright-star diffraction-spike
    arms — and returns a boolean pixel mask covering them, or None if none
    were found.

    Duplicated from modules/astrometry/_streak.py's identical helper rather
    than imported, mirroring how this module already duplicates the FWHM/
    elongation star-filtering formulas above/below it (both cross-reference
    each other in comments) — see that module's docstring for the full
    rationale and real-data verification (2026-08-07, T_CrB test frame).
    Kept in sync by hand with astrometry/_streak.py's version.

    pixel_scale_arcsec may be None here (this module runs before plate
    solving, so it only has whatever XPIXSZ/FOCALLEN/PIXSCALE the FITS
    header itself carries via _read_pixel_scale() above) — falls back to a
    conservative fixed 200px length floor and 1px dilation in that case.
    """
    if rms is None or rms <= 0:
        return None

    try:
        objs, seg = sep.extract(
            data_sub,
            thresh=config.STREAK_DETECT_SIGMA,
            err=rms,
            minarea=config.SEP_MIN_AREA,
            deblend_cont=1.0,
            segmentation_map=True,
        )
    except Exception as exc:
        logger.debug("QC: streak coarse pass failed: %s", exc)
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

    mask = np.isin(seg, streak_idx + 1)

    dilate_px = 1
    if pixel_scale_arcsec and pixel_scale_arcsec > 0:
        dilate_px = max(1, int(round(config.STREAK_MASK_DILATE_ARCSEC / pixel_scale_arcsec)))
    try:
        from scipy.ndimage import binary_dilation
        structure = np.ones((2 * dilate_px + 1, 2 * dilate_px + 1), dtype=bool)
        mask = binary_dilation(mask, structure=structure)
    except Exception as exc:
        logger.debug("QC: streak mask dilation failed (%s) — using un-dilated mask", exc)

    logger.info(
        "QC: streak masking: %d streak-like feature(s) found, masking %d pixel(s)",
        len(streak_idx), int(mask.sum()),
    )
    return mask


def _compute_fwhm_pixels(a: float, b: float) -> float:
    """
    Compute per-source FWHM in pixels from SEP semi-axes a and b.

    Formula: FWHM = 2 * sqrt(2 * ln(2)) * sqrt((a^2 + b^2) / 2)
    which is equivalent to the quadrature-mean of the two-axis FWHMs of a
    circular Gaussian approximation.
    """
    return 2.0 * math.sqrt(2.0 * math.log(2.0)) * math.sqrt((a ** 2 + b ** 2) / 2.0)


# Below this many members a subset is not a population — the raw
# all-detections median is used instead. Matches the hard floor of 3 raw
# detections analyze() already refuses to compute any statistics below.
_MEDIAN_MIN_SOURCES = 3


# How much broader than the frame's own compact population a source may be
# before it is treated as extended rather than as a blurred star — the same
# stellar-PSF tolerance modules/astrometry/_extraction.py applies around its
# own psf_fwhm_arcsec estimate.
_EXTENDED_FWHM_FACTOR = 1.5


def _clip_broad_outliers(
    fwhm_values: np.ndarray,
    subset: np.ndarray,
    fits_filename: str,
) -> np.ndarray:
    """
    Narrow *subset* by dropping sources far broader than the compact
    population it already contains.

    A roundness cut removes filaments and edge-on galaxies, but not a
    face-on galaxy or a round nebula knot — extended and round at once. Those
    still inflate fwhm_median, and no ABSOLUTE upper bound can remove them
    without also capping the very quantity BLUR tests (see analyze()'s
    step 4).

    So the bound is relative: the lower quartile of the subset's own FWHM
    distribution — star-dominated in any field where stars outnumber extended
    objects by more than 1:3 — times _EXTENDED_FWHM_FACTOR. A uniformly
    blurred frame shifts that quartile up with everything else and nothing is
    clipped, so BLUR remains reachable at any blur level; only a source
    broader than the compact population *of this same frame* is dropped.

    Returns *subset* unchanged whenever it is too small to estimate a
    quartile from, or when clipping would leave too few sources to take a
    median over.
    """
    selected = fwhm_values[subset]
    if selected.size < _MEDIAN_MIN_SOURCES:
        return subset

    ceiling = float(np.percentile(selected, 25)) * _EXTENDED_FWHM_FACTOR
    clipped = subset & (fwhm_values <= ceiling)

    n_dropped = int(subset.sum()) - int(clipped.sum())
    if int(clipped.sum()) < _MEDIAN_MIN_SOURCES:
        return subset
    if n_dropped:
        logger.debug(
            "QC: dropped %d extended source(s) broader than %.2f px "
            "(%.1fx the compact population) from the FWHM median  file=%s",
            n_dropped, ceiling, _EXTENDED_FWHM_FACTOR, fits_filename,
        )
    return clipped


def _subset_median(
    values: np.ndarray,
    subset: np.ndarray,
    metric: str,
    fits_filename: str,
) -> float:
    """
    Median of *values* over *subset*, falling back to the median of all of
    *values* when the subset is too small to be meaningful.

    The fallback is not just defensive: it is what keeps BLUR and TRAIL
    reachable on a frame so badly blurred or trailed that its own stars fall
    outside the opposite axis' bound and empty the subset out — see the long
    note in analyze()'s step 4.
    """
    n = int(subset.sum())
    if n >= _MEDIAN_MIN_SOURCES:
        return float(np.median(values[subset]))

    logger.debug(
        "QC: %s subset holds only %d source(s) — using the median over all "
        "%d detections instead  file=%s",
        metric, n, values.size, fits_filename,
    )
    return float(np.median(values))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def analyze(fits_path: str, move_on_reject: bool = True) -> dict:
    """
    Measure quality metrics for a single FITS frame and classify it.

    The function is declared async for pipeline interface consistency even
    though all operations are CPU-bound and synchronous internally.

    Parameters
    ----------
    fits_path:
        Absolute path to the FITS file on disk.
    move_on_reject:
        When True (the default — what pipeline.py relies on), a non-"OK"
        quality_flag moves fits_path to FITS_REJECTED before returning, same
        as always. Pass False to compute metrics/flag only and leave the
        file exactly where it is — used by pipeline.py's analyze_frame()
        (which owns the file's fate itself, see that module's docstring) and
        by modules/catalog_preview.py, a read-only diagnostic that must never
        move/touch the frame it's given but otherwise calls this same
        production analyze() path.

    Returns
    -------
    dict with keys:
        quality_flag        "OK" | "BLUR" | "TRAIL" | "LOW_STARS" | "HIGH_BACKGROUND" | "BAD"
        fwhm_median         float | None
        fwhm_unit           "arcsec" | "pixels"
        elongation_median   float | None
        snr_median          float | None
        sky_background      float | None   (median sky ADU)
        sky_sigma           float | None   (sky background RMS)
        star_count          int | None
        cr_fraction         float | None   ([0.0, 1.0])
        rejected_path       str | None     (set when the file was moved)
    """
    logger.info("QC analysis starting: %s", fits_path)

    # ------------------------------------------------------------------
    # 1. Load FITS data and headers
    # ------------------------------------------------------------------
    try:
        with fits.open(fits_path, mode="readonly", ignore_missing_simple=True) as hdul:
            raw_data: np.ndarray = hdul[0].data
            hdr: fits.Header = hdul[0].header
    except Exception as exc:
        logger.error("QC: failed to open FITS file %s: %s", fits_path, exc)
        return _result(
            quality_flag="BAD",
            rejected_path=_move_rejected(fits_path, "BAD", "_UNKNOWN") if move_on_reject else None,
        )

    if raw_data is None:
        logger.error("QC: primary HDU has no image data in %s", fits_path)
        return _result(
            quality_flag="BAD",
            rejected_path=_move_rejected(fits_path, "BAD", "_UNKNOWN") if move_on_reject else None,
        )

    # Normalize to C-contiguous float64 as required by sep
    data: np.ndarray = np.ascontiguousarray(raw_data.astype(np.float64))

    # Extract structured header info for object name + pixel scale
    header_info: dict = extract_headers(fits_path)
    object_name: str = header_info.get("object_name", "_UNKNOWN") or "_UNKNOWN"

    plate_scale: float | None = _read_pixel_scale(hdr)
    logger.debug(
        "QC: object=%s  plate_scale=%s arcsec/px  file=%s",
        object_name,
        f"{plate_scale:.4f}" if plate_scale is not None else "unknown",
        os.path.basename(fits_path),
    )

    # A narrowband (Hα/[OIII]/[SII]/[NII]) frame of the exact same field
    # genuinely detects far fewer stars than a broadband one — only the
    # sliver of stellar continuum that leaks through the line filter is
    # visible at all — so the star-count floor below uses a separate, softer
    # threshold for it rather than QC_STARS_MIN. Checked directly off the raw
    # header value (not pipeline.py's already-normalized `header` dict, which
    # this function never sees) so the right threshold is picked regardless
    # of NORMALIZE_ENABLED.
    raw_filter = header_info.get("observation", {}).get("filter")
    narrowband = is_narrowband(raw_filter)
    effective_stars_min = config.QC_STARS_MIN_NARROWBAND if narrowband else config.QC_STARS_MIN

    # ------------------------------------------------------------------
    # 2. Sky background estimation
    # ------------------------------------------------------------------
    sky_background: float | None = None
    sky_sigma: float | None = None

    try:
        bkg = sep.Background(data)
        sky_background = float(bkg.globalback)
        sky_sigma = float(bkg.globalrms)
        data_sub: np.ndarray = np.ascontiguousarray(data - bkg)

        # Streak masking — see _build_streak_mask()'s docstring and
        # modules/astrometry/_streak.py's identical pre-pass. Keeps this module's own
        # fwhm_median/elongation_median/star_count consistent with what
        # astrometry/_extraction.py will end up extracting from the same frame (a trail
        # fragmenting into several roundish "stars" would otherwise inflate
        # star_count here too).
        streak_mask = _build_streak_mask(data_sub, sky_sigma, plate_scale)
        if streak_mask is not None:
            data_sub = np.array(data_sub, copy=True)
            data_sub[streak_mask] = 0.0

        logger.debug(
            "QC: sky_background=%.2f sky_sigma=%.2f file=%s",
            sky_background,
            sky_sigma,
            os.path.basename(fits_path),
        )
    except Exception as exc:
        logger.error("QC: sep.Background failed for %s: %s", fits_path, exc)
        return _result(
            quality_flag="BAD",
            sky_background=sky_background,
            sky_sigma=sky_sigma,
            rejected_path=_move_rejected(fits_path, "BAD", object_name) if move_on_reject else None,
        )

    # ------------------------------------------------------------------
    # 3. Source detection
    # ------------------------------------------------------------------
    # Use the same detection parameters as astrometry.py for consistent star counts
    # SEP_DETECT_THRESH is in units of sigma (background RMS)
    try:
        objects = sep.extract(
            data_sub,
            thresh=config.SEP_DETECT_THRESH,
            err=bkg.globalrms,
            minarea=config.SEP_MIN_AREA,
        )
    except Exception as exc:
        logger.error("QC: sep.extract failed for %s: %s", fits_path, exc)
        return _result(
            quality_flag="BAD",
            sky_background=sky_background,
            sky_sigma=sky_sigma,
            rejected_path=_move_rejected(fits_path, "BAD", object_name) if move_on_reject else None,
        )

    raw_detection_count: int = len(objects)
    logger.debug("QC: detected %d raw sources in %s", raw_detection_count, os.path.basename(fits_path))

    # Degenerate frame — too few sources to compute reliable statistics
    if raw_detection_count < 3:
        logger.warning(
            "QC: only %d sources detected (< 3), flagging LOW_STARS: %s",
            raw_detection_count,
            fits_path,
        )
        return _result(
            quality_flag="LOW_STARS",
            sky_background=sky_background,
            sky_sigma=sky_sigma,
            star_count=raw_detection_count,
            rejected_path=_move_rejected(fits_path, "LOW_STARS", object_name) if move_on_reject else None,
        )

    # ------------------------------------------------------------------
    # 4. Per-source shape, and the subsets the two medians are taken over
    # ------------------------------------------------------------------
    # fwhm_median/elongation_median gate BLUR/TRAIL, so they must describe
    # the frame's STARS. Taken over every raw detection — as they were until
    # audit 2026-08-18, finding C11 — they also carry whatever extended,
    # non-stellar morphology the field contains (nebula filaments, galaxies,
    # compact clumps), which is broader and less round than any point source.
    # A well-focused, well-tracked narrowband frame of a nebula could be
    # rejected BLUR/TRAIL purely for what it was pointed at, and the same
    # skewed FWHM then travelled downstream as psf_fwhm_arcsec to
    # astrometry.solve()/subtraction.run().
    #
    # The obvious fix — reuse the star_mask computed below for star_count —
    # does NOT work: that mask cuts at STAR_FWHM_MAX_ARCSEC and
    # STAR_ELONGATION_MAX, whose defaults (8.0", 1.5) sit at or below
    # QC_FWHM_MAX_ARCSEC (8.0") and QC_ELONGATION_MAX (2.0). A median taken
    # over survivors of those cuts can never exceed either QC threshold, so
    # BLUR and TRAIL would both become dead branches — the same
    # cut-below-the-threshold-being-tested failure as finding C2.
    #
    # Each median is therefore taken over sources filtered on the OTHER axis,
    # never on the one being measured:
    #
    #   fwhm_median       — over ROUND sources (elongation < STAR_ELONGATION_MAX),
    #                       then with sources far broader than that subset's own
    #                       compact population dropped relative to it (see
    #                       _clip_broad_outliers(), which is what catches the
    #                       round-AND-extended case a roundness cut cannot).
    #                       Filaments, edge-on galaxies, streak remnants and
    #                       face-on blobs go; every blurred star stays, however
    #                       blurred it is.
    #   elongation_median — over COMPACT sources (fwhm <= STAR_FWHM_MAX_ARCSEC).
    #                       Nebula clumps and galaxies go; a trailed star's FWHM
    #                       only grows as sqrt((e^2+1)/2), so it stays well
    #                       inside that bound across the elongation range TRAIL
    #                       actually discriminates.
    #
    # Both also require positive flux and reject anything sharper than
    # STAR_FWHM_MIN_ARCSEC (hot/warm pixel clusters — a floor can only bias
    # the estimate UPWARD, so it cannot hide blur). A subset with fewer than
    # _MEDIAN_MIN_SOURCES members is not a population at all: the raw
    # all-detections median is used instead, which is also what restores
    # BLUR/TRAIL on a frame so badly blurred or trailed that its own stars
    # fall outside the opposite axis' bound.
    fwhm_pixels_arr: np.ndarray = np.array(
        [_compute_fwhm_pixels(float(o["a"]), float(o["b"])) for o in objects],
        dtype=np.float64,
    )
    elongation_arr: np.ndarray = np.array(
        [float(o["a"]) / float(o["b"]) if float(o["b"]) > 0.0 else 1.0 for o in objects],
        dtype=np.float64,
    )

    mask_flux: np.ndarray = objects["flux"] > 0
    mask_round: np.ndarray = elongation_arr < config.STAR_ELONGATION_MAX

    fwhm_per_source: np.ndarray | None = None
    if plate_scale is not None:
        fwhm_per_source = fwhm_pixels_arr * plate_scale
        mask_fwhm_min: np.ndarray = fwhm_per_source >= config.STAR_FWHM_MIN_ARCSEC
        mask_fwhm_max: np.ndarray = fwhm_per_source <= config.STAR_FWHM_MAX_ARCSEC
    else:
        # Without a plate scale the arcsec bounds are meaningless; only the
        # scale-free roundness and flux cuts can be applied.
        mask_fwhm_min = np.ones(raw_detection_count, dtype=bool)
        mask_fwhm_max = np.ones(raw_detection_count, dtype=bool)

    fwhm_subset: np.ndarray = mask_flux & mask_fwhm_min & mask_round
    # A round source can still be extended (face-on galaxy, round nebula
    # knot) — one more, purely relative pass removes those; see
    # _clip_broad_outliers().
    fwhm_subset = _clip_broad_outliers(
        fwhm_pixels_arr, fwhm_subset, os.path.basename(fits_path)
    )
    # The elongation subset needs no counterpart: what contaminates it is
    # extended morphology, which the compactness cut above already removes,
    # and a relative clip on elongation itself would start eating into the
    # uniformly-trailed case TRAIL exists to catch.
    elongation_subset: np.ndarray = mask_flux & mask_fwhm_min & mask_fwhm_max

    # ------------------------------------------------------------------
    # 4b. FWHM (pixels → arcsec when plate scale is available)
    # ------------------------------------------------------------------
    fwhm_px_median: float = _subset_median(
        fwhm_pixels_arr, fwhm_subset, "fwhm", os.path.basename(fits_path)
    )

    if plate_scale is not None:
        fwhm_median: float | None = fwhm_px_median * plate_scale
        fwhm_unit = "arcsec"
    else:
        fwhm_median = fwhm_px_median
        fwhm_unit = "pixels"

    logger.debug(
        "QC: fwhm_median=%.3f %s (%.3f px, over %d/%d round sources)  file=%s",
        fwhm_median,
        fwhm_unit,
        fwhm_px_median,
        int(fwhm_subset.sum()),
        raw_detection_count,
        os.path.basename(fits_path),
    )

    # ------------------------------------------------------------------
    # 5. Elongation
    # ------------------------------------------------------------------
    elongation_median: float | None = _subset_median(
        elongation_arr, elongation_subset, "elongation", os.path.basename(fits_path)
    )
    logger.debug(
        "QC: elongation_median=%.3f (over %d/%d compact sources)  file=%s",
        elongation_median,
        int(elongation_subset.sum()),
        raw_detection_count,
        os.path.basename(fits_path),
    )

    # ------------------------------------------------------------------
    # 5b. Filter to count only real stars (consistent with astrometry.py)
    # ------------------------------------------------------------------
    # Apply the same filtering criteria as astrometry.py uses:
    # - Elongation < STAR_ELONGATION_MAX (round sources only)
    # - FWHM in valid range (reject hot pixels and extended objects)
    # - Positive flux
    #
    # Note: We use QC thresholds (QC_ELONGATION_MAX, QC_FWHM_MAX_ARCSEC) for
    # the BLUR/TRAIL quality flags, but use the stricter STAR_* thresholds
    # here to count only genuine point sources, matching what astrometry
    # will actually extract as stars. This is the one place those stricter
    # bounds are applied on BOTH axes at once — see the long note in step 4
    # for why the two medians above deliberately cannot be.
    star_mask: np.ndarray = mask_flux & mask_round & mask_fwhm_min & mask_fwhm_max
    star_count: int = int(np.sum(star_mask))

    if plate_scale is not None:
        logger.debug(
            "QC: star filter: %d raw → %d stars (elong<%0.1f, fwhm=[%.1f-%.1f]\")  file=%s",
            raw_detection_count,
            star_count,
            config.STAR_ELONGATION_MAX,
            config.STAR_FWHM_MIN_ARCSEC,
            config.STAR_FWHM_MAX_ARCSEC,
            os.path.basename(fits_path),
        )
    else:
        logger.debug(
            "QC: star filter (no plate scale): %d raw → %d stars  file=%s",
            raw_detection_count,
            star_count,
            os.path.basename(fits_path),
        )

    # ------------------------------------------------------------------
    # 6. SNR via aperture photometry
    # ------------------------------------------------------------------
    snr_median: float | None = None
    try:
        # Use 3× the median FWHM in pixels as aperture radius; fall back to 5 px
        aperture_radius: float = max(3.0 * fwhm_px_median, 5.0)
        x_coords: np.ndarray = objects["x"].astype(np.float64)
        y_coords: np.ndarray = objects["y"].astype(np.float64)

        flux_arr, fluxerr_arr, _ = sep.sum_circle(
            data_sub,
            x_coords,
            y_coords,
            aperture_radius,
            err=bkg.globalrms,
        )

        # Guard against zero / negative errors
        valid_mask: np.ndarray = fluxerr_arr > 0.0
        if valid_mask.any():
            snr_arr: np.ndarray = flux_arr[valid_mask] / fluxerr_arr[valid_mask]
            snr_median = float(np.median(snr_arr))
        else:
            snr_median = None

        logger.debug(
            "QC: snr_median=%s  aperture_radius=%.1f px  file=%s",
            f"{snr_median:.2f}" if snr_median is not None else "None",
            aperture_radius,
            os.path.basename(fits_path),
        )
    except Exception as exc:
        logger.warning("QC: SNR computation failed for %s: %s", fits_path, exc)
        snr_median = None

    # ------------------------------------------------------------------
    # 7. Cosmic ray fraction
    # ------------------------------------------------------------------
    cr_fraction: float | None = None
    try:
        crmask, _ = astroscrappy.detect_cosmics(data.astype(np.float32))
        cr_fraction = float(crmask.sum()) / float(crmask.size)
        logger.debug(
            "QC: cr_fraction=%.5f  file=%s",
            cr_fraction,
            os.path.basename(fits_path),
        )
    except Exception as exc:
        logger.warning(
            "QC: astroscrappy.detect_cosmics failed for %s: %s", fits_path, exc
        )
        cr_fraction = None

    # ------------------------------------------------------------------
    # 8. Quality flag classification
    # ------------------------------------------------------------------
    # Note: BLUR, TRAIL, and HIGH_BACKGROUND explain why star_count might be
    # low (sources are filtered out, or too faint to detect, due to poor
    # image quality). So we check those first, and only check LOW_STARS if
    # all three are false.

    blur: bool = (
        fwhm_unit == "arcsec"
        and fwhm_median is not None
        and fwhm_median > config.QC_FWHM_MAX_ARCSEC
    )
    trail: bool = (
        elongation_median is not None
        and elongation_median > config.QC_ELONGATION_MAX
    )
    # A raised sky background (twilight, moonlight, cloud, stray light) can
    # leave FWHM and elongation looking perfectly normal — it degrades depth
    # (fainter stars are lost in the noise), not sharpness or tracking — so
    # BLUR/TRAIL alone can miss a genuinely bad frame.
    high_background: bool = (
        sky_background is not None
        and sky_background > config.QC_SKY_BACKGROUND_MAX
    )

    # LOW_STARS only applies when BLUR, TRAIL, and HIGH_BACKGROUND are all
    # false (otherwise a low star count is a consequence of one of those).
    # Uses effective_stars_min (QC_STARS_MIN_NARROWBAND on a narrowband
    # frame, QC_STARS_MIN otherwise — see above) rather than QC_STARS_MIN
    # unconditionally.
    low_stars: bool = (
        not blur and not trail and not high_background
        and star_count < effective_stars_min
    )

    issue_count: int = sum([blur, trail, high_background, low_stars])

    if issue_count >= 2:
        quality_flag = "BAD"
    elif blur:
        quality_flag = "BLUR"
    elif trail:
        quality_flag = "TRAIL"
    elif high_background:
        quality_flag = "HIGH_BACKGROUND"
    elif low_stars:
        quality_flag = "LOW_STARS"
    else:
        quality_flag = "OK"

    logger.info(
        "QC: quality_flag=%s  fwhm=%.3f %s  elongation=%.3f  stars=%d "
        "(min=%d%s)  sky_background=%.1f  file=%s",
        quality_flag,
        fwhm_median if fwhm_median is not None else 0.0,
        fwhm_unit,
        elongation_median if elongation_median is not None else 0.0,
        star_count,
        effective_stars_min,
        ", narrowband" if narrowband else "",
        sky_background if sky_background is not None else 0.0,
        os.path.basename(fits_path),
    )

    # ------------------------------------------------------------------
    # 9. Move rejected frames
    # ------------------------------------------------------------------
    rejected_path: str | None = None
    if quality_flag != "OK" and move_on_reject:
        rejected_path = _move_rejected(fits_path, quality_flag, object_name)

    return _result(
        quality_flag=quality_flag,
        fwhm_median=fwhm_median,
        fwhm_unit=fwhm_unit,
        elongation_median=elongation_median,
        snr_median=snr_median,
        sky_background=sky_background,
        sky_sigma=sky_sigma,
        star_count=star_count,
        cr_fraction=cr_fraction,
        rejected_path=rejected_path,
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _result(
    quality_flag: str = "OK",
    fwhm_median: float | None = None,
    fwhm_unit: str = "pixels",
    elongation_median: float | None = None,
    snr_median: float | None = None,
    sky_background: float | None = None,
    sky_sigma: float | None = None,
    star_count: int | None = None,
    cr_fraction: float | None = None,
    rejected_path: str | None = None,
) -> dict:
    """Construct the canonical QC result dictionary."""
    return {
        "quality_flag":      quality_flag,
        "fwhm_median":       fwhm_median,
        "fwhm_unit":         fwhm_unit,
        "elongation_median": elongation_median,
        "snr_median":        snr_median,
        "sky_background":    sky_background,
        "sky_sigma":         sky_sigma,
        "star_count":        star_count,
        "cr_fraction":       cr_fraction,
        "rejected_path":     rejected_path,
    }


def _cleanup_empty_incoming_parents(moved_path: str) -> None:
    """Remove empty parent dirs between *moved_path* and FITS_INCOMING."""
    incoming_real = os.path.realpath(config.FITS_INCOMING)
    parent = os.path.dirname(moved_path)
    while True:
        parent_real = os.path.realpath(parent)
        if parent_real == incoming_real or not parent_real.startswith(incoming_real + os.sep):
            break
        try:
            os.rmdir(parent)
            logger.debug("Removed empty incoming subdirectory: %s", parent)
        except OSError:
            break
        parent = os.path.dirname(parent)


# Upper bound on the numeric suffixes _unique_destination() will try before
# falling back to a timestamp. Reaching it means something is looping; a
# bounded probe keeps that from becoming an unbounded one.
_MAX_REJECTED_SUFFIX = 1000


def _unique_destination(dest_path: str) -> str:
    """
    Return *dest_path*, or a non-colliding variant of it when a file is
    already there: ``BLUR_frame.fits`` → ``BLUR_frame_1.fits`` → ``_2`` …

    `shutil.move()` overwrites silently on POSIX, which destroyed the earlier
    file outright — in the one subsystem whose entire purpose is to keep a
    rejected frame around for manual review (audit 2026-08-18, finding C12).
    A collision is not exotic: a re-run against the same original filename, a
    test retry, or two frames that normalize to the same name all produce one.

    The probe is not atomic, but the pipeline has a single writer per file and
    the fallback below terminates regardless.
    """
    if not os.path.exists(dest_path):
        return dest_path

    stem, ext = os.path.splitext(dest_path)
    for n in range(1, _MAX_REJECTED_SUFFIX):
        candidate = f"{stem}_{n}{ext}"
        if not os.path.exists(candidate):
            return candidate

    # Practically unreachable; guarantees a terminating, still-unique answer.
    return f"{stem}_{datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')}{ext}"


def _move_rejected(fits_path: str, flag: str, object_name: str) -> str | None:
    """
    Move a rejected FITS file to the configured rejected directory.

    Destination: {FITS_REJECTED}/{object_name}/{flag}_{original_filename},
    with a numeric suffix appended on collision rather than overwriting
    whatever is already there — see _unique_destination().

    Returns the destination path, or None if the move fails (logged as error).
    """
    safe_name = sanitize_object_name(object_name)
    dest_dir = os.path.join(config.FITS_REJECTED, safe_name)
    try:
        os.makedirs(dest_dir, exist_ok=True)
    except OSError as exc:
        logger.error(
            "QC: failed to create rejected directory %s: %s", dest_dir, exc
        )
        return None

    original_filename = os.path.basename(fits_path)
    dest_filename = f"{flag}_{original_filename}"
    dest_path = os.path.join(dest_dir, dest_filename)

    unique_path = _unique_destination(dest_path)
    if unique_path != dest_path:
        logger.warning(
            "QC: %s already exists — storing this rejection as %s instead of "
            "overwriting it",
            dest_path, os.path.basename(unique_path),
        )
        dest_path = unique_path

    try:
        shutil.move(fits_path, dest_path)
        logger.info("QC: moved rejected frame to %s", dest_path)
        _cleanup_empty_incoming_parents(fits_path)
        return dest_path
    except OSError as exc:
        logger.error(
            "QC: failed to move %s to %s: %s", fits_path, dest_path, exc
        )
        return None
