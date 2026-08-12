"""
modules/astrometry/_streak.py — the coarse, low-threshold, non-deblended
pre-pass that finds long thin streaks (satellite/aircraft trails,
diffraction-spike arms) before the real point-source extraction runs.

Internal helper only — not part of this package's public surface.
"""

from __future__ import annotations

import logging

import numpy as np
import sep

import config

logger = logging.getLogger(__name__)


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
