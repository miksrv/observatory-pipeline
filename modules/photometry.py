"""
modules/photometry.py — Aperture photometry and magnitude calibration for FITS frames.

The single public entry point is:

    await photometry.measure(fits_path: str, sources: list[dict]) -> list[dict]

It performs aperture photometry on each source using photutils, calibrates
instrumental magnitudes via differential photometry against Gaia DR3 reference
stars in the field, and returns the enriched source list with photometry fields
added.

On failure to open the FITS file or build a valid WCS, the function returns the
input source list with all photometry fields set to None rather than raising.
Individual source errors are caught per-source and also result in None values.
"""

from __future__ import annotations

import logging
import math
import os
from typing import Any

import astropy.io.fits as fits
import numpy as np
from astropy.stats import sigma_clipped_stats
from astropy.wcs import WCS
from photutils.aperture import (
    ApertureStats,
    CircularAnnulus,
    CircularAperture,
    aperture_photometry,
)

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Photometry output keys — always present in every output source dict
# ---------------------------------------------------------------------------

_PHOT_KEYS: tuple[str, ...] = (
    "flux_aperture",
    "flux_err",
    "mag_instrumental",
    "mag_calibrated",
    "mag_err",
    "calibrated",
    "edge_flag",
    "zero_point",
    "zero_point_err",
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _null_phot_fields(calibrated: bool = False) -> dict[str, Any]:
    """Return a dict with all photometry output keys set to their null values."""
    return {
        "flux_aperture":    None,
        "flux_err":         None,
        "mag_instrumental": None,
        "mag_calibrated":   None,
        "mag_err":          None,
        "calibrated":       calibrated,
        "edge_flag":        False,
        "zero_point":       None,
        "zero_point_err":   None,
    }


def _inject_nulls(
    sources: list[dict],
    zero_point: float | None = None,
    zero_point_err: float | None = None,
) -> list[dict]:
    """
    Return a copy of *sources* with all photometry fields set to None.

    Used for the whole-frame fallback when FITS or WCS loading fails.
    ``zero_point`` and ``zero_point_err`` are forwarded when known (normally
    they will also be None in fallback scenarios).
    """
    result: list[dict] = []
    for src in sources:
        out = dict(src)
        out.update(_null_phot_fields())
        out["zero_point"]     = zero_point
        out["zero_point_err"] = zero_point_err
        result.append(out)
    return result


def _pixel_scale_from_wcs(wcs: WCS) -> float:
    """
    Derive the plate scale in arcsec/pixel from a WCS object.

    Uses the column-norm of the pixel_scale_matrix (handles rotation / shear).
    The result matches the formula used in astrometry.py for consistency.
    """
    ps_matrix = wcs.pixel_scale_matrix   # (2, 2), units deg/px
    pixel_scale_deg: float = float(
        np.sqrt(ps_matrix[0, 0] ** 2 + ps_matrix[1, 0] ** 2)
    )
    return pixel_scale_deg * 3600.0      # arcsec/px


def _compute_zero_point(
    sources: list[dict],
) -> tuple[float | None, float | None]:
    """
    Compute the differential photometry zero-point from Gaia DR3 reference stars.

    Requires at least 3 sources with ``catalog_name == "Gaia DR3"``,
    a finite ``catalog_mag``, and a finite ``mag_instrumental``.

    Returns
    -------
    (zero_point, zero_point_err)
        Both are None when fewer than 3 valid references are available.
    """
    deltas: list[float] = []
    for src in sources:
        if src.get("catalog_name") != "Gaia DR3":
            continue
        cat_mag = src.get("catalog_mag")
        inst_mag = src.get("mag_instrumental")
        if cat_mag is None or inst_mag is None:
            continue
        if not (math.isfinite(cat_mag) and math.isfinite(inst_mag)):
            continue
        deltas.append(cat_mag - inst_mag)

    if len(deltas) < 3:
        logger.warning(
            "photometry: only %d Gaia DR3 reference stars available "
            "(need >= 3) — mag_calibrated will be None for all sources",
            len(deltas),
        )
        return None, None

    arr = np.array(deltas, dtype=np.float64)
    zp: float = float(np.median(arr))
    # Median absolute deviation (no scipy dependency)
    mad: float = float(np.median(np.abs(arr - zp)))

    logger.info(
        "photometry: zero_point=%.4f  zero_point_err(MAD)=%.4f  "
        "n_ref_stars=%d",
        zp,
        mad,
        len(deltas),
    )
    return zp, mad


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def measure(fits_path: str, sources: list[dict]) -> list[dict]:
    """
    Perform aperture photometry and differential magnitude calibration.

    The function is declared async for pipeline interface consistency even
    though all operations (photutils, FITS I/O) are CPU-bound and synchronous
    internally.

    Parameters
    ----------
    fits_path:
        Absolute path to the FITS file on disk.
    sources:
        Source list produced by ``astrometry.solve()``, optionally enriched by
        ``catalog_matcher.match()`` (may carry ``catalog_name`` / ``catalog_mag``
        keys).  Input dicts are never modified; copies are returned.

    Returns
    -------
    List of source dicts with all original fields preserved plus:

        flux_aperture       float | None   net aperture flux (ADU)
        flux_err            float | None   Poisson + sky noise in quadrature
        mag_instrumental    float | None   -2.5 * log10(flux_aperture)
        mag_calibrated      float | None   mag_instrumental + zero_point
        mag_err             float | None   1.0857 * flux_err / flux_aperture
        calibrated          bool           True when zero_point was applied
        edge_flag           bool           True when centroid is within 10 px of edge
        zero_point          float | None   frame-level ZP (same for all sources)
        zero_point_err      float | None   MAD of reference-star ZP offsets

    On any frame-level failure the input sources are returned with all
    photometry keys set to None.
    """
    fits_filename = os.path.basename(fits_path)
    logger.info(
        "Photometry starting: %d sources  file=%s",
        len(sources),
        fits_filename,
    )

    if not sources:
        logger.info("photometry: no sources to measure for %s", fits_filename)
        return []

    # ------------------------------------------------------------------
    # Step 1 — Open FITS file and build WCS
    # ------------------------------------------------------------------
    try:
        with fits.open(fits_path, mode="readonly", ignore_missing_simple=True) as hdul:
            hdr = hdul[0].header
            raw_data: np.ndarray = hdul[0].data
            naxis1: int = int(hdr.get("NAXIS1", 0))
            naxis2: int = int(hdr.get("NAXIS2", 0))
    except Exception as exc:
        logger.error(
            "photometry: failed to open FITS file %s: %s", fits_path, exc
        )
        return _inject_nulls(sources)

    if raw_data is None:
        logger.error(
            "photometry: primary HDU has no image data in %s", fits_path
        )
        return _inject_nulls(sources)

    data: np.ndarray = np.ascontiguousarray(raw_data.astype(np.float64))

    # Try to get WCS from FITS header first, then fallback to .wcs file
    wcs = None
    try:
        wcs = WCS(hdr)
        if not wcs.has_celestial:
            # Try to read from .wcs file that astap creates
            wcs_file_path = os.path.splitext(fits_path)[0] + ".wcs"
            if os.path.exists(wcs_file_path):
                logger.info(
                    "photometry: FITS has no celestial WCS, trying .wcs file: %s",
                    wcs_file_path,
                )
                try:
                    with fits.open(wcs_file_path) as wcs_hdul:
                        wcs_hdr = wcs_hdul[0].header
                        wcs = WCS(wcs_hdr)
                except Exception as wcs_exc:
                    logger.warning(
                        "photometry: failed to read .wcs file %s: %s",
                        wcs_file_path,
                        wcs_exc,
                    )
                    wcs = None
            
        if wcs is None or not wcs.has_celestial:
            raise ValueError("WCS has no celestial axes")
    except Exception as exc:
        logger.error(
            "photometry: invalid or missing WCS in %s: %s", fits_path, exc
        )
        return _inject_nulls(sources)

    try:
        pixel_scale_arcsec: float = _pixel_scale_from_wcs(wcs)
    except Exception as exc:
        logger.error(
            "photometry: could not derive pixel scale from WCS in %s: %s",
            fits_path, exc,
        )
        return _inject_nulls(sources)

    logger.debug(
        "photometry: pixel_scale=%.4f arcsec/px  image=%dx%d  file=%s",
        pixel_scale_arcsec,
        naxis1,
        naxis2,
        fits_filename,
    )

    # ------------------------------------------------------------------
    # Step 2 — Sky background (sigma-clipped statistics)
    # ------------------------------------------------------------------
    try:
        _, sky_median, sky_sigma = sigma_clipped_stats(data, sigma=3.0)
        sky_median = float(sky_median)
        sky_sigma  = float(sky_sigma)
    except Exception as exc:
        logger.error(
            "photometry: sigma_clipped_stats failed for %s: %s", fits_path, exc
        )
        return _inject_nulls(sources)

    data_sub: np.ndarray = data - sky_median

    logger.debug(
        "photometry: sky_median=%.2f sky_sigma=%.4f  file=%s",
        sky_median,
        sky_sigma,
        fits_filename,
    )

    # ------------------------------------------------------------------
    # Step 3 — Convert all (RA, Dec) → pixel positions in one WCS call
    # ------------------------------------------------------------------
    try:
        sky_coords = np.array(
            [[src["ra"], src["dec"]] for src in sources],
            dtype=np.float64,
        )
        pix_coords = wcs.all_world2pix(sky_coords, 0)  # shape (N, 2)
    except Exception as exc:
        logger.error(
            "photometry: WCS coordinate conversion failed for %s: %s",
            fits_path, exc,
        )
        return _inject_nulls(sources)

    # ------------------------------------------------------------------
    # Step 4 — Per-source aperture photometry
    # ------------------------------------------------------------------
    output: list[dict] = []

    for i, src in enumerate(sources):
        out = dict(src)
        out.update(_null_phot_fields())

        x_px: float = float(pix_coords[i, 0])
        y_px: float = float(pix_coords[i, 1])

        # Sources outside image bounds — skip measurement, keep nulls
        if not (0 <= x_px < naxis1 and 0 <= y_px < naxis2):
            logger.warning(
                "photometry: source %d (%s) at pixel (%.1f, %.1f) is outside "
                "image bounds (%dx%d), skipping  file=%s",
                i,
                f"ra={src['ra']:.5f} dec={src['dec']:.5f}",
                x_px, y_px,
                naxis1, naxis2,
                fits_filename,
            )
            output.append(out)
            continue

        # Edge flag: within 10 px of any border
        out["edge_flag"] = (
            x_px < 10.0
            or y_px < 10.0
            or x_px > naxis1 - 10.0
            or y_px > naxis2 - 10.0
        )

        try:
            # Aperture sizes in pixels, derived from FWHM
            fwhm_arcsec: float = float(src.get("fwhm") or 0.0)
            if fwhm_arcsec > 0.0 and pixel_scale_arcsec > 0.0:
                fwhm_px: float = fwhm_arcsec / pixel_scale_arcsec
            else:
                # Fallback: assume 3-pixel FWHM if missing
                fwhm_px = 3.0

            ap_radius: float    = 2.0 * fwhm_px
            annulus_inner: float = 4.0 * fwhm_px
            annulus_outer: float = 6.0 * fwhm_px

            position = (x_px, y_px)
            aperture = CircularAperture(position, r=ap_radius)
            annulus  = CircularAnnulus(
                position, r_in=annulus_inner, r_out=annulus_outer
            )

            # Sky estimate from annulus
            ann_stats  = ApertureStats(data_sub, annulus)
            sky_per_px: float = float(ann_stats.median)

            # Aperture photometry on background-subtracted data
            phot_table = aperture_photometry(data_sub, aperture)
            ap_sum: float = float(phot_table["aperture_sum"][0])

            # Net flux after per-pixel sky correction
            ap_area: float   = float(aperture.area)
            net_flux: float  = ap_sum - sky_per_px * ap_area

            # Flux uncertainty: Poisson noise + sky noise
            flux_err: float  = math.sqrt(
                abs(net_flux) + ap_area * sky_sigma ** 2
            )

            out["flux_aperture"] = net_flux
            out["flux_err"]      = flux_err

            # Instrumental magnitude
            if net_flux > 0.0:
                out["mag_instrumental"] = -2.5 * math.log10(net_flux)
                if math.isfinite(flux_err) and net_flux > 0.0:
                    out["mag_err"] = 1.0857 * flux_err / net_flux
            else:
                out["mag_instrumental"] = None
                out["mag_err"]          = None

            logger.debug(
                "photometry: source %d  net_flux=%.2f  flux_err=%.2f  "
                "mag_inst=%s  file=%s",
                i,
                net_flux,
                flux_err,
                f"{out['mag_instrumental']:.4f}"
                if out["mag_instrumental"] is not None else "None",
                fits_filename,
            )

        except Exception as exc:
            logger.warning(
                "photometry: per-source error at index %d (ra=%.5f dec=%.5f) "
                "in %s: %s",
                i,
                src.get("ra", float("nan")),
                src.get("dec", float("nan")),
                fits_filename,
                exc,
            )
            # flux_aperture, flux_err, mag_instrumental, mag_err stay None

        output.append(out)

    # ------------------------------------------------------------------
    # Step 5 — Differential magnitude calibration
    # ------------------------------------------------------------------
    zero_point, zero_point_err = _compute_zero_point(output)

    for out in output:
        out["zero_point"]     = zero_point
        out["zero_point_err"] = zero_point_err

        if zero_point is not None and out["mag_instrumental"] is not None:
            out["mag_calibrated"] = out["mag_instrumental"] + zero_point
            out["calibrated"]     = True
        else:
            out["mag_calibrated"] = None
            out["calibrated"]     = False

    calibrated_count = sum(1 for o in output if o["calibrated"])
    logger.info(
        "Photometry complete: %d/%d sources measured, %d calibrated  "
        "zero_point=%s  file=%s",
        sum(1 for o in output if o["flux_aperture"] is not None),
        len(output),
        calibrated_count,
        f"{zero_point:.4f}" if zero_point is not None else "None",
        fits_filename,
    )

    return output
