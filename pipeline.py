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
    # Step 1 — Header extraction
    # ------------------------------------------------------------------
    header: dict = fits_header.extract_headers(fits_path)
    object_name: str = header.get("object_name", "_UNKNOWN") or "_UNKNOWN"

    logger.info(
        "Starting pipeline for filename=%s object=%s",
        basename,
        object_name,
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
            astro_result = await astrometry.solve(fits_path)
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
    # ------------------------------------------------------------------
    sources: list = astro_result.get("sources") or []
    if catalog_matcher is not None and sources:
        try:
            # Build frame_meta with all fields required by catalog_matcher
            frame_meta = {
                "filename": basename,
                "ra_center": astro_result.get("ra_center") or header.get("ra"),
                "dec_center": astro_result.get("dec_center") or header.get("dec"),
                "fov_deg": astro_result.get("fov_deg") or 1.0,
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
    #          reference stars for magnitude calibration)
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

    frame_data: dict = _build_frame_payload(fits_path, header, qc_result, astro_result)


    try:
        frame_id: str = await api_client.post_frame(frame_data)
        logger.info(
            "Frame registered: frame_id=%s filename=%s",
            frame_id,
            basename,
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
        dest_dir = os.path.join(config.FITS_ARCHIVE, object_name)
        os.makedirs(dest_dir, exist_ok=True)
        dest_path = os.path.join(dest_dir, basename)
        shutil.move(fits_path, dest_path)
        logger.info(
            "Pipeline complete: filename=%s archived_to=%s frame_id=%s",
            basename,
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
) -> dict:
    """
    Assemble the POST /frames request body from module outputs.

    The structure matches the POST /frames API payload defined in CLAUDE.md,
    with nested sub-dicts for observation, instrument, sensor, observer, software, and qc.
    """
    return {
        "filename": os.path.basename(fits_path),
        "original_filepath": fits_path,
        "obs_time": header.get("obs_time"),
        "ra_center": astro_result.get("ra_center") or header.get("ra"),
        "dec_center": astro_result.get("dec_center") or header.get("dec"),
        "fov_deg": astro_result.get("fov_deg"),
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
