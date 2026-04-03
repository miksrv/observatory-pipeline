"""
modules/qc.py — Quality control analysis for raw FITS frames.

The single public entry point is:

    await qc.analyze(fits_path: str) -> dict

It measures image quality metrics (FWHM, elongation, SNR, sky background,
star count, cosmic ray fraction), classifies the frame, and — when rejected —
moves the file to the appropriate subdirectory under FITS_REJECTED before
returning.  No plate-solving is required; all measurements use the raw pixel
data only.

Rejected frames are never sent to the API.  The pipeline orchestrator checks
``quality_flag`` and stops processing if it is not ``"OK"``.
"""

from __future__ import annotations

import logging
import math
import os
import shutil
from typing import Any

import astropy.io.fits as fits
import astroscrappy
import numpy as np
import sep

import config
from modules.fits_header import extract_headers, sanitize_object_name

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_PLATE_SCALE_PIXEL_KEYWORDS: tuple[str, ...] = ("XPIXSZ", "PIXSIZE", "PIXSCALE")


def _read_pixel_scale(hdr: fits.Header) -> float | None:
    """
    Derive the plate scale in arcsec/pixel from FITS headers.

    Requires both a pixel size (XPIXSZ, PIXSIZE, or PIXSCALE in microns) and a
    focal length (FOCALLEN in mm).  Some cameras write PIXSCALE directly as
    arcsec/px — detected when the value is small (< 20) and no pixel size +
    focal length pair is available.

    Returns None when the necessary headers are absent.
    """
    # Direct arcsec/px keyword (written by some capture software)
    pixscale_direct = hdr.get("PIXSCALE")
    if pixscale_direct is not None:
        try:
            val = float(pixscale_direct)
            # Sanity-check: a valid plate scale is between 0.01 and 200 arcsec/px
            if 0.01 <= val <= 200.0:
                return val
        except (TypeError, ValueError):
            pass

    # Derive from pixel size + focal length
    xpixsz: float | None = None
    for kw in ("XPIXSZ", "PIXSIZE"):
        raw = hdr.get(kw)
        if raw is not None:
            try:
                xpixsz = float(raw)
                break
            except (TypeError, ValueError):
                continue

    focal_length: float | None = None
    raw_fl = hdr.get("FOCALLEN")
    if raw_fl is not None:
        try:
            focal_length = float(raw_fl)
        except (TypeError, ValueError):
            pass

    if xpixsz is not None and focal_length is not None and focal_length > 0.0:
        # plate_scale = 206265 * (pixel_size_um / 1000) / focal_length_mm
        return 206265.0 * (xpixsz / 1000.0) / focal_length

    return None


def _compute_fwhm_pixels(a: float, b: float) -> float:
    """
    Compute per-source FWHM in pixels from SEP semi-axes a and b.

    Formula: FWHM = 2 * sqrt(2 * ln(2)) * sqrt((a^2 + b^2) / 2)
    which is equivalent to the quadrature-mean of the two-axis FWHMs of a
    circular Gaussian approximation.
    """
    return 2.0 * math.sqrt(2.0 * math.log(2.0)) * math.sqrt((a ** 2 + b ** 2) / 2.0)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def analyze(fits_path: str) -> dict:
    """
    Measure quality metrics for a single FITS frame and classify it.

    The function is declared async for pipeline interface consistency even
    though all operations are CPU-bound and synchronous internally.

    Parameters
    ----------
    fits_path:
        Absolute path to the FITS file on disk.

    Returns
    -------
    dict with keys:
        quality_flag        "OK" | "BLUR" | "TRAIL" | "LOW_STARS" | "BAD"
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
            rejected_path=_move_rejected(fits_path, "BAD", "_UNKNOWN"),
        )

    if raw_data is None:
        logger.error("QC: primary HDU has no image data in %s", fits_path)
        return _result(
            quality_flag="BAD",
            rejected_path=_move_rejected(fits_path, "BAD", "_UNKNOWN"),
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
            rejected_path=_move_rejected(fits_path, "BAD", object_name),
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
            rejected_path=_move_rejected(fits_path, "BAD", object_name),
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
            rejected_path=_move_rejected(fits_path, "LOW_STARS", object_name),
        )

    # ------------------------------------------------------------------
    # 4. FWHM (pixels → arcsec when plate scale is available)
    # ------------------------------------------------------------------
    fwhm_pixels_arr: np.ndarray = np.array(
        [_compute_fwhm_pixels(float(o["a"]), float(o["b"])) for o in objects],
        dtype=np.float64,
    )
    fwhm_px_median: float = float(np.median(fwhm_pixels_arr))

    if plate_scale is not None:
        fwhm_median: float | None = fwhm_px_median * plate_scale
        fwhm_unit = "arcsec"
    else:
        fwhm_median = fwhm_px_median
        fwhm_unit = "pixels"

    logger.debug(
        "QC: fwhm_median=%.3f %s (%.3f px)  file=%s",
        fwhm_median,
        fwhm_unit,
        fwhm_px_median,
        os.path.basename(fits_path),
    )

    # ------------------------------------------------------------------
    # 5. Elongation
    # ------------------------------------------------------------------
    elongation_arr: np.ndarray = np.array(
        [float(o["a"]) / float(o["b"]) if float(o["b"]) > 0.0 else 1.0 for o in objects],
        dtype=np.float64,
    )
    elongation_median: float | None = float(np.median(elongation_arr))
    logger.debug(
        "QC: elongation_median=%.3f  file=%s",
        elongation_median,
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
    # will actually extract as stars.
    
    # Compute per-source FWHM in arcsec (or pixels if no plate scale)
    fwhm_per_source: np.ndarray
    if plate_scale is not None:
        fwhm_per_source = fwhm_pixels_arr * plate_scale
        
        # Apply star filters
        mask_elongation = elongation_arr < config.STAR_ELONGATION_MAX
        mask_fwhm_min = fwhm_per_source >= config.STAR_FWHM_MIN_ARCSEC
        mask_fwhm_max = fwhm_per_source <= config.STAR_FWHM_MAX_ARCSEC
        mask_flux = objects["flux"] > 0
        
        star_mask = mask_elongation & mask_fwhm_min & mask_fwhm_max & mask_flux
        star_count: int = int(np.sum(star_mask))
        
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
        # Without plate scale, use only elongation and flux filters
        mask_elongation = elongation_arr < config.STAR_ELONGATION_MAX
        mask_flux = objects["flux"] > 0
        star_mask = mask_elongation & mask_flux
        star_count = int(np.sum(star_mask))
        
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
    # Note: BLUR and TRAIL explain why star_count might be low (sources are
    # filtered out due to poor image quality). So we check BLUR and TRAIL
    # first, and only check LOW_STARS if those are both false.
    
    blur: bool = (
        fwhm_unit == "arcsec"
        and fwhm_median is not None
        and fwhm_median > config.QC_FWHM_MAX_ARCSEC
    )
    trail: bool = (
        elongation_median is not None
        and elongation_median > config.QC_ELONGATION_MAX
    )
    
    # LOW_STARS only applies when BLUR and TRAIL are false
    # (otherwise low star count is a consequence of the BLUR/TRAIL issue)
    low_stars: bool = (not blur and not trail) and star_count < config.QC_STARS_MIN

    issue_count: int = sum([blur, trail, low_stars])

    if issue_count >= 2:
        quality_flag = "BAD"
    elif blur:
        quality_flag = "BLUR"
    elif trail:
        quality_flag = "TRAIL"
    elif low_stars:
        quality_flag = "LOW_STARS"
    else:
        quality_flag = "OK"

    logger.info(
        "QC: quality_flag=%s  fwhm=%.3f %s  elongation=%.3f  stars=%d  file=%s",
        quality_flag,
        fwhm_median if fwhm_median is not None else 0.0,
        fwhm_unit,
        elongation_median if elongation_median is not None else 0.0,
        star_count,
        os.path.basename(fits_path),
    )

    # ------------------------------------------------------------------
    # 9. Move rejected frames
    # ------------------------------------------------------------------
    rejected_path: str | None = None
    if quality_flag != "OK":
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


def _move_rejected(fits_path: str, flag: str, object_name: str) -> str | None:
    """
    Move a rejected FITS file to the configured rejected directory.

    Destination: {FITS_REJECTED}/{object_name}/{flag}_{original_filename}

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

    try:
        shutil.move(fits_path, dest_path)
        logger.info("QC: moved rejected frame to %s", dest_path)
        return dest_path
    except OSError as exc:
        logger.error(
            "QC: failed to move %s to %s: %s", fits_path, dest_path, exc
        )
        return None
