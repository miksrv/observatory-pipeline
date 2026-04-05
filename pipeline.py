"""
pipeline.py — Orchestrator for processing a single FITS file end-to-end.

The single public entry point is:

    await pipeline.run(fits_path: str) -> None

It calls each module in sequence, handles optional modules gracefully when
they are not yet implemented, and ensures that no exception from an individual
step crashes the entire service.
"""

from __future__ import annotations

import logging
import os
import shutil

import config
from modules import fits_header, qc

# ---------------------------------------------------------------------------
# Optional modules — each wrapped in try/except ImportError so that the
# pipeline continues to run even when a module is not yet implemented.
# ---------------------------------------------------------------------------

try:
    from modules import astrometry
except ImportError:
    astrometry = None  # type: ignore[assignment]

try:
    from modules import photometry
except ImportError:
    photometry = None  # type: ignore[assignment]

try:
    from modules import catalog_matcher
except ImportError:
    catalog_matcher = None  # type: ignore[assignment]

try:
    from modules import anomaly_detector
except ImportError:
    anomaly_detector = None  # type: ignore[assignment]

try:
    from modules import normalizer
except ImportError:
    normalizer = None  # type: ignore[assignment]

try:
    from api_client import client as api_client
except ImportError:
    api_client = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def run(fits_path: str) -> None:
    """
    Process a single FITS file through the full pipeline.

    Steps:
        1. Extract FITS headers
        2. Quality control — stop on rejection
        3. Astrometry (optional)
        4. Catalog matching (optional) — enriches sources with Gaia/Simbad/MPC IDs
        5. Photometry (optional) — uses Gaia DR3 matches for zero-point calibration
        6. POST frame to API — stop on failure
        7. POST sources to API
        8. Anomaly detection (optional)
        9. POST anomalies to API
        10. Move file to archive

    Parameters
    ----------
    fits_path:
        Absolute path to the incoming FITS file.
    """
    basename = os.path.basename(fits_path)
    # Note: "filename" is a reserved key in logging.LogRecord (set to the
    # source file name of the logger call). Use "fits_filename" instead so
    # that extra= does not clash with that built-in attribute.
    extra = {"fits_filename": basename}

    # ------------------------------------------------------------------
    # Step 1 — Header extraction and normalization
    # ------------------------------------------------------------------
    header: dict = fits_header.extract_headers(fits_path)
    
    # Apply normalization if enabled in config
    if config.NORMALIZE_ENABLED and normalizer is not None:
        header = normalizer.normalize_headers(header)
    
    # Get object name for directory organization
    object_name: str = header.get("object_name", "_UNKNOWN") or "_UNKNOWN"
    
    # Generate filename (normalized if enabled)
    original_filename = basename
    if config.NORMALIZE_ENABLED and normalizer is not None:
        observation = header.get("observation", {})
        normalized_filename = normalizer.generate_normalized_filename(
            object_name=object_name,
            frame_type=observation.get("frame_type"),
            filter_name=observation.get("filter"),
            exptime=observation.get("exptime"),
            obs_time=header.get("obs_time"),
        )
    else:
        normalized_filename = basename

    logger.info(
        "Starting pipeline for filename=%s object=%s%s",
        original_filename,
        object_name,
        f" → {normalized_filename}" if normalized_filename != original_filename else "",
        extra=extra,
    )

    # ------------------------------------------------------------------
    # Step 2 — Quality control
    # ------------------------------------------------------------------
    qc_result: dict = await qc.analyze(fits_path)
    quality_flag: str = qc_result.get("quality_flag", "BAD")

    logger.info(
        "QC result: flag=%s fwhm=%.2f stars=%d",
        quality_flag,
        qc_result.get("fwhm_median") or 0.0,
        qc_result.get("star_count") or 0,
        extra=extra,
    )

    if quality_flag != "OK":
        logger.warning(
            "Frame rejected by QC: flag=%s filename=%s",
            quality_flag,
            basename,
            extra=extra,
        )
        return

    # ------------------------------------------------------------------
    # Step 3 — Astrometry (optional)
    # ------------------------------------------------------------------
    astro_result: dict = {}
    if astrometry is not None:
        try:
            astro_result = await astrometry.solve(
                fits_path,
                psf_fwhm_arcsec=qc_result.get("fwhm_median"),
            )
            logger.debug(
                "Astrometry complete: ra=%.4f dec=%.4f sources=%d",
                astro_result.get("ra_center") or 0.0,
                astro_result.get("dec_center") or 0.0,
                len(astro_result.get("sources") or []),
                extra=extra,
            )
        except Exception as exc:
            logger.error(
                "Astrometry failed: %s — continuing with empty result",
                exc,
                extra=extra,
            )
            astro_result = {}
    else:
        logger.debug("Astrometry module not available — skipping", extra=extra)

    # ------------------------------------------------------------------
    # Step 4 — Catalog matching (run BEFORE photometry so Gaia DR3 stars
    #          can be used as reference for zero-point calibration)
    #
    # Use "sources_all" (loose filter) which includes bright saturated
    # objects (asteroids, comets) and faint stars rejected by the strict
    # star filter. This gives more sources for WCS offset correction and
    # ensures moving/transient objects reach the anomaly detector.
    # Falls back to "sources" (strict stars) if sources_all is unavailable.
    # ------------------------------------------------------------------
    sources: list = astro_result.get("sources_all") or astro_result.get("sources") or []
    sources_stars: list = astro_result.get("sources") or []
    if len(sources) != len(sources_stars):
        logger.info(
            "Using sources_all for catalog matching: %d detections (%d strict stars)  fits_filename=%s",
            len(sources), len(sources_stars), basename,
            extra=extra,
        )
    if catalog_matcher is not None and sources:
        try:
            # Build frame_meta with all fields required by catalog_matcher
            frame_meta = {
                "filename": basename,
                "ra_center": astro_result.get("ra_center") or header.get("ra"),
                "dec_center": astro_result.get("dec_center") or header.get("dec"),
                "fov_deg": astro_result.get("fov_deg") or 1.0,
                "naxis1": astro_result.get("naxis1"),
                "naxis2": astro_result.get("naxis2"),
                "obs_time": header.get("obs_time"),
            }
            sources = await catalog_matcher.match(sources, frame_meta)
            matched_count = sum(1 for s in sources if s.get("catalog_name") is not None)
            logger.info(
                "Catalog matching complete: %d/%d sources matched",
                matched_count,
                len(sources),
                extra=extra,
            )
        except Exception as exc:
            logger.error(
                "Catalog matching failed: %s — continuing without matches",
                exc,
                extra=extra,
            )
    else:
        if catalog_matcher is None:
            logger.debug("Catalog matcher not available — skipping", extra=extra)

    # ------------------------------------------------------------------
    # Step 5 — Photometry (runs AFTER catalog matching to use Gaia DR3
    #          reference stars for magnitude calibration).
    #
    # Photometry is run on `sources` (the full list including sources_all)
    # so that Gaia-matched stars in the list serve as calibration reference.
    # Non-stellar sources (asteroids, galaxies) also get instrumental
    # magnitudes, which is useful for anomaly detection.
    # ------------------------------------------------------------------
    if photometry is not None and sources:
        try:
            sources = await photometry.measure(fits_path, sources)
            calibrated_count = sum(1 for s in sources if s.get("calibrated"))
            logger.info(
                "Photometry complete: %d sources measured, %d calibrated",
                len(sources),
                calibrated_count,
                extra=extra,
            )
        except Exception as exc:
            logger.error(
                "Photometry failed: %s — continuing with uncalibrated sources",
                exc,
                extra=extra,
            )
    else:
        if photometry is None:
            logger.debug("Photometry module not available — skipping", extra=extra)

    # ------------------------------------------------------------------
    # Step 6 — Post frame to API
    # ------------------------------------------------------------------
    if api_client is None:
        logger.warning(
            "API client not available — skipping all API steps and archive move",
            extra=extra,
        )
        return

    frame_data: dict = _build_frame_payload(
        fits_path, header, qc_result, astro_result,
        filename=normalized_filename,
    )


    try:
        frame_id: str = await api_client.post_frame(frame_data)
        logger.info(
            "Frame registered: frame_id=%s filename=%s",
            frame_id,
            normalized_filename,
            extra=extra,
        )
    except Exception as exc:
        logger.error(
            "Failed to post frame to API: %s — aborting pipeline for filename=%s",
            exc,
            basename,
            extra=extra,
        )
        # Clean up astap temp files even on failure
        _cleanup_astap_files(fits_path)
        return

    # ------------------------------------------------------------------
    # Step 7 — Post sources (includes catalog match info from step 4)
    # ------------------------------------------------------------------

    try:
        await api_client.post_sources(frame_id, basename, sources)
        logger.debug(
            "Sources posted: frame_id=%s count=%d",
            frame_id,
            len(sources),
            extra=extra,
        )
    except Exception as exc:
        logger.error(
            "Failed to post sources: frame_id=%s error=%s — continuing",
            frame_id,
            exc,
            extra=extra,
        )


    # ------------------------------------------------------------------
    # Step 8 — Anomaly detection (optional)
    # ------------------------------------------------------------------
    anomalies: list = []
    if anomaly_detector is not None:
        try:
            # Build frame_meta with all fields required by anomaly_detector
            anomaly_frame_meta = {
                "filename": basename,
                "obs_time": header.get("obs_time"),
                "ra_center": astro_result.get("ra_center") or header.get("ra"),
                "dec_center": astro_result.get("dec_center") or header.get("dec"),
                "fov_deg": astro_result.get("fov_deg") or 1.0,
            }
            # sources already have catalog_name/catalog_id from step 4
            anomalies = await anomaly_detector.detect(frame_id, sources, sources, anomaly_frame_meta)
            logger.debug(
                "Anomaly detection complete: %d anomalies",
                len(anomalies),
                extra=extra,
            )
        except Exception as exc:
            logger.error(
                "Anomaly detection failed: %s — continuing",
                exc,
                extra=extra,
            )
    else:
        logger.debug("Anomaly detector not available — skipping", extra=extra)

    # ------------------------------------------------------------------
    # Step 9 — Post anomalies
    # ------------------------------------------------------------------
    try:
        await api_client.post_anomalies(frame_id, fits_path, anomalies)
        logger.debug(
            "Anomalies posted: frame_id=%s count=%d",
            frame_id,
            len(anomalies),
            extra=extra,
        )
    except Exception as exc:
        logger.error(
            "Failed to post anomalies: frame_id=%s error=%s — continuing",
            frame_id,
            exc,
            extra=extra,
        )

    # ------------------------------------------------------------------
    # Step 10 — Archive move and cleanup
    # ------------------------------------------------------------------
    try:
        # Use object name for directory structure (normalized if normalization enabled)
        dest_dir = os.path.join(config.FITS_ARCHIVE, object_name)
        os.makedirs(dest_dir, exist_ok=True)
        
        # Rename file to normalized filename (if normalization enabled)
        dest_path = os.path.join(dest_dir, normalized_filename)
        shutil.move(fits_path, dest_path)
        
        if normalized_filename != original_filename:
            logger.info(
                "Pipeline complete: %s → %s archived_to=%s frame_id=%s",
                original_filename,
                normalized_filename,
                dest_path,
                frame_id,
                extra=extra,
            )
        else:
            logger.info(
                "Pipeline complete: filename=%s archived_to=%s frame_id=%s",
                normalized_filename,
                dest_path,
                frame_id,
                extra=extra,
            )

        # Clean up astap temporary files (.ini, .wcs) left in incoming directory
        _cleanup_astap_files(fits_path)

    except Exception as exc:
        logger.error("Failed to archive file: %s", exc, extra=extra)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _cleanup_astap_files(fits_path: str) -> None:
    """
    Remove temporary files created by astap plate solver.

    Astap creates .ini and .wcs files alongside the FITS file during
    plate solving. This function removes them after processing is complete.

    Parameters
    ----------
    fits_path:
        Original path to the FITS file (before it was moved to archive).
    """
    base_path = os.path.splitext(fits_path)[0]
    extensions = (".ini", ".wcs")

    for ext in extensions:
        temp_file = base_path + ext
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
                logger.debug("Removed astap temp file: %s", temp_file)
        except OSError as exc:
            logger.warning("Failed to remove astap temp file %s: %s", temp_file, exc)


def _build_frame_payload(
    fits_path: str,
    header: dict,
    qc_result: dict,
    astro_result: dict,
    *,
    filename: str,
) -> dict:
    """
    Assemble the POST /frames request body from module outputs.

    The structure matches the POST /frames API payload defined in CLAUDE.md,
    with nested sub-dicts for observation, instrument, sensor, observer, software, and qc.
    
    If normalization is enabled, all values in header are already normalized.
    """
    # Get fov_deg from astrometry, or calculate from FITS headers as fallback
    fov_deg = astro_result.get("fov_deg")
    if fov_deg is None:
        fov_deg = _calculate_fov_from_headers(header)

    return {
        "filename": filename,
        "original_filepath": fits_path,
        "obs_time": header.get("obs_time"),
        "ra_center": astro_result.get("ra_center") or header.get("ra"),
        "dec_center": astro_result.get("dec_center") or header.get("dec"),
        "fov_deg": fov_deg,
        "quality_flag": qc_result.get("quality_flag"),
        "observation": header.get("observation", {}),
        "instrument": header.get("instrument", {}),
        "sensor": header.get("sensor", {}),
        "observer": header.get("observer", {}),
        "software": header.get("software", {}),
        "qc": {
            "fwhm_median":    qc_result.get("fwhm_median"),
            "elongation":     qc_result.get("elongation_median"),
            "snr_median":     qc_result.get("snr_median"),
            "sky_background": qc_result.get("sky_background"),
            "star_count":     qc_result.get("star_count"),
        },
    }


def _calculate_fov_from_headers(header: dict) -> float | None:
    """
    Calculate field of view (in degrees) from FITS header information.

    Uses pixel size from headers if available (XPIXSZ keyword), otherwise
    falls back to a reasonable default for modern CMOS cameras (3.76µm).

    Returns the FOV of the longer axis in degrees, or None if calculation fails.
    """
    sensor = header.get("sensor", {})
    instrument = header.get("instrument", {})

    width_px = sensor.get("width_px")
    height_px = sensor.get("height_px")

    if width_px is None or height_px is None:
        logger.debug("Cannot calculate FOV: missing image dimensions")
        return None

    focal_length_mm = instrument.get("focal_length_mm")

    if focal_length_mm is None or focal_length_mm <= 0:
        logger.debug("Cannot calculate FOV: missing or invalid focal_length_mm")
        return None

    # Get pixel size from header, or use default
    pixel_size_um = sensor.get("pixel_size_um")
    if pixel_size_um is None or pixel_size_um <= 0:
        # Use a reasonable default for modern CMOS cameras
        # Common values: 3.76µm (ASI294/IMX294), 2.9µm (ASI533)
        pixel_size_um = 3.76
        logger.debug("Using default pixel size: %.2f µm", pixel_size_um)

    # Account for binning if present
    binning_x = sensor.get("binning_x") or 1
    binning_y = sensor.get("binning_y") or 1
    effective_pixel_size_um = pixel_size_um * max(binning_x, binning_y)

    # Calculate plate scale in arcsec/pixel
    # plate_scale = 206.265 * pixel_size_mm / focal_length_mm
    # 206.265 is the conversion factor from radians to arcseconds
    pixel_size_mm = effective_pixel_size_um / 1000.0
    plate_scale_arcsec = 206.265 * pixel_size_mm / focal_length_mm

    # Calculate FOV for both axes
    fov_x_arcsec = width_px * plate_scale_arcsec
    fov_y_arcsec = height_px * plate_scale_arcsec

    # Return the larger FOV (longest axis) in degrees
    fov_max_arcsec = max(fov_x_arcsec, fov_y_arcsec)
    fov_deg = fov_max_arcsec / 3600.0

    logger.debug(
        "Calculated FOV from headers: %.3f deg (plate_scale=%.2f arcsec/px, "
        "pixel=%.2fµm, binning=%dx%d, focal=%dmm)",
        fov_deg,
        plate_scale_arcsec,
        pixel_size_um,
        binning_x,
        binning_y,
        focal_length_mm,
    )

    return fov_deg

