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
    from modules import subtraction
except ImportError:
    subtraction = None  # type: ignore[assignment]

try:
    from modules import finder_chart
except ImportError:
    finder_chart = None  # type: ignore[assignment]

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
        9.5. Move file to archive
        10. Finder chart update (optional) — per-source discovery chart

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

    # Fallback: extract object name from filename when OBJECT header is empty
    if (object_name == "_UNKNOWN" or not object_name) and normalizer is not None:
        extracted = normalizer.extract_object_from_filename(basename)
        if extracted != "_UNKNOWN":
            object_name = extracted
            header["object_name"] = object_name
            observation = header.get("observation", {})
            if isinstance(observation, dict):
                observation["object"] = object_name

    # Write normalized object name back to FITS file
    if config.NORMALIZE_ENABLED and normalizer is not None:
        normalizer.update_fits_object_header(fits_path, object_name)

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

    if normalized_filename != original_filename:
        logger.info('Processing "%s" (%s) → %s', original_filename, object_name, normalized_filename, extra=extra)
    else:
        logger.info('Processing "%s" (%s)', original_filename, object_name, extra=extra)

    # ------------------------------------------------------------------
    # Step 1.5 — Early exit for calibration frames (Dark, Flat, Bias)
    # ------------------------------------------------------------------
    frame_type: str | None = header.get("observation", {}).get("frame_type")
    if frame_type in ("Dark", "Flat", "Bias"):
        dest_dir = os.path.join(config.FITS_ARCHIVE, object_name)
        dest_path = os.path.join(dest_dir, normalized_filename)
        os.makedirs(dest_dir, exist_ok=True)
        shutil.move(fits_path, dest_path)
        _cleanup_astap_files(fits_path)
        logger.info("Archived %s frame: %s → %s", frame_type, basename, dest_path, extra=extra)
        return

    # ------------------------------------------------------------------
    # Step 2 — Quality control
    # ------------------------------------------------------------------
    qc_result: dict = await qc.analyze(fits_path)
    quality_flag: str = qc_result.get("quality_flag", "BAD")

    logger.info(
        "QC: flag=%s fwhm=%.2f stars=%d",
        quality_flag,
        qc_result.get("fwhm_median") or 0.0,
        qc_result.get("star_count") or 0,
        extra=extra,
    )

    if quality_flag != "OK":
        logger.warning("QC rejected %s: %s", basename, quality_flag, extra=extra)
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
            logger.info(
                "Astrometry: ra=%.4f dec=%.4f sources=%d",
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
    # Sources for subtraction / catalog matching / photometry.
    #
    # Use "sources_all" (loose filter) which includes bright saturated
    # objects (asteroids, comets) and faint stars rejected by the strict
    # star filter. This gives more sources for WCS offset correction and
    # ensures moving/transient objects reach the anomaly detector.
    # Falls back to "sources" (strict stars) if sources_all is unavailable.
    #
    # NOTE: this must be assigned BEFORE Step 3.5 (image subtraction) below,
    # which merges its candidates into `sources`. Assigning it later (as an
    # earlier revision of this file did, in what is now Step 4) made
    # `sources` a local variable whose first assignment happened after the
    # read in Step 3.5, raising UnboundLocalError on every frame where
    # subtraction actually found candidates.
    # ------------------------------------------------------------------
    sources: list = astro_result.get("sources_all") or astro_result.get("sources") or []
    sources_stars: list = astro_result.get("sources") or []
    if len(sources) != len(sources_stars):
        logger.info(
            "Using sources_all for catalog matching: %d detections (%d strict stars)  fits_filename=%s",
            len(sources), len(sources_stars), basename,
            extra=extra,
        )

    # ------------------------------------------------------------------
    # Step 3.5 — Image subtraction (transient / moving object detection)
    # Runs against archived frames of the same object + filter.
    # Produces subtraction_candidates: sources confirmed new by pixel diff.
    # These are merged into `sources` and flagged _from_subtraction=True
    # so the anomaly detector can treat them with higher confidence.
    # ------------------------------------------------------------------
    subtraction_info: dict = {"performed": False, "reference_frame_count": 0, "candidates_count": 0}
    if subtraction is not None and astro_result:
        try:
            filter_name = header.get("observation", {}).get("filter")
            archive_object_dir = os.path.join(config.FITS_ARCHIVE, object_name)
            sub_result = await subtraction.run(
                fits_path=fits_path,
                archive_dir=archive_object_dir,
                filter_name=filter_name,
            )
            sub_candidates = sub_result.get("candidates", [])
            subtraction_info = {
                "performed": sub_result.get("performed", False),
                "reference_frame_count": sub_result.get("reference_frame_count", 0),
                "candidates_count": len(sub_candidates),
            }
            if sub_candidates:
                # Initialise catalog fields so catalog_matcher can process them
                for cand in sub_candidates:
                    cand.setdefault("catalog_name", None)
                    cand.setdefault("catalog_id",   None)
                    cand.setdefault("catalog_mag",  None)
                    cand.setdefault("object_type",  None)
                sources = sources + sub_candidates
                logger.info(
                    "Subtraction found %d candidate(s); total sources for matching: %d  fits_filename=%s",
                    len(sub_candidates), len(sources), basename,
                    extra=extra,
                )
        except Exception as exc:
            logger.error(
                "Image subtraction failed: %s — continuing without subtraction",
                exc,
                extra=extra,
            )
    else:
        if subtraction is None:
            logger.debug("Subtraction module not available — skipping", extra=extra)

    # ------------------------------------------------------------------
    # Step 4 — Catalog matching (run BEFORE photometry so Gaia DR3 stars
    #          can be used as reference for zero-point calibration)
    # ------------------------------------------------------------------
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
    # Step 4.5 — Deduplicate sources sharing a catalog identity.
    #
    # A single physical object can appear more than once in `sources` for
    # this one frame: e.g. a moving MPC-matched asteroid is often detected
    # both by the normal source extractor AND as one or more nearby
    # image-subtraction candidates (MOVING_CONE_ARCSEC is wide — 120" by
    # default — so several nearby diff-image blobs can all independently
    # match the same MPC object). Each such duplicate would otherwise be
    # posted as a separate row to POST /frames/{id}/sources, inflating
    # sources.observation_count for one real observation, and separately
    # classified by anomaly_detector.py — duplicate ASTEROID/COMET rows in
    # `anomalies` for what is physically one object seen once in this frame.
    # Sources with no catalog match are never merged; only entries sharing
    # the same (catalog_name, catalog_id) collapse into one.
    # ------------------------------------------------------------------
    sources = _dedupe_by_catalog_identity(sources, extra)

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
    # Step 5.5 — Populate the unified "mag" field.
    #
    # This is the field the API payload documents (POST /frames/{id}/sources)
    # and the one anomaly_detector.py reads for magnitude-change comparisons
    # (VARIABLE_STAR / BINARY_STAR / SUPERNOVA_CANDIDATE). photometry.measure()
    # only ever sets mag_instrumental/mag_calibrated — never "mag" itself —
    # so without this step every source's "mag" stayed at whatever
    # placeholder it had before (None for catalog/subtraction candidates),
    # delta_mag in anomaly_detector.py was always None, and no
    # magnitude-change anomaly could ever fire.
    #
    # "mag" is ONLY ever the Gaia-calibrated magnitude — never a fallback to
    # the uncalibrated instrumental one. mag_instrumental = -2.5*log10(flux_ADU)
    # has no absolute zero-point and is not a real magnitude on its own; when
    # a frame has fewer than 3 Gaia DR3 references (e.g. a narrow/crowded
    # field), photometry.py sets calibrated=False for every source in it, and
    # an earlier revision of this step fell back to mag_instrumental in that
    # case — which is exactly what produced the extreme (e.g. -15) "magnitude"
    # values investigated in docs/ISSUES.md #2: two whole real frames with
    # zero_point=None had every single source's "mag" come out negative and
    # implausible, while frames that did calibrate looked completely normal
    # (+11 to +19). See that doc for the live reproduction. Sources without a
    # calibrated magnitude simply get mag=None — delta_mag-based
    # classifications correctly don't fire for them rather than firing on a
    # meaningless number.
    # ------------------------------------------------------------------
    for _src in sources:
        _src["mag"] = _src.get("mag_calibrated") if _src.get("calibrated") else None

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
        logger.info("Frame registered: frame_id=%s", frame_id, extra=extra)
    except Exception as exc:
        logger.error("Failed to register frame %s: %s", basename, exc, extra=extra)
        # Clean up astap temp files even on failure
        _cleanup_astap_files(fits_path)
        return

    # ------------------------------------------------------------------
    # Step 7 — Post sources (includes catalog match info from step 4)
    #
    # The API returns `source_ids`, positionally parallel to `sources`, so
    # each source dict can be tagged with its resolved `sources.id` here.
    # anomaly_detector.py reads this back (as "_source_id") to populate
    # `anomalies[].source_id` — otherwise the API has no way to know which
    # catalog source an anomaly refers to (see CLAUDE.md Known Issues).
    # ------------------------------------------------------------------

    try:
        source_ids = await api_client.post_sources(frame_id, basename, sources)
        logger.debug(
            "Sources posted: frame_id=%s count=%d",
            frame_id,
            len(sources),
            extra=extra,
        )
        if source_ids is not None and len(source_ids) == len(sources):
            for src, source_id in zip(sources, source_ids):
                src["_source_id"] = source_id
        elif source_ids is not None:
            logger.warning(
                "post_sources returned %d source_ids for %d sources — "
                "length mismatch, not attaching source_id to anomalies",
                len(source_ids),
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
                "subtraction_performed": subtraction_info.get("performed", False),
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
        await api_client.post_anomalies(frame_id, normalized_filename, anomalies)
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
    # Step 9.5 — Archive move and cleanup
    #
    # This runs BEFORE the finder chart step (10) below on purpose: chart
    # rendering loads each epoch's FITS file from
    # FITS_ARCHIVE/{object}/{filename} (see modules/finder_chart.py's
    # _local_fits_path()), and that includes *this* frame's own epoch. If
    # the archive move happened after chart generation, this frame's file
    # would still be sitting at its pre-archive location and every chart
    # attempt for a source whose track consists only of this one frame
    # (i.e. any first-time alert) would find 0 of its epochs loadable and
    # silently skip the chart entirely — see CLAUDE.md Known Issues.
    # ------------------------------------------------------------------
    try:
        # Use object name for directory structure (normalized if normalization enabled)
        dest_dir = os.path.join(config.FITS_ARCHIVE, object_name)
        os.makedirs(dest_dir, exist_ok=True)

        # Rename file to normalized filename (if normalization enabled)
        dest_path = os.path.join(dest_dir, normalized_filename)
        shutil.move(fits_path, dest_path)

        logger.info("Done: %s → %s", original_filename, dest_path, extra=extra)

        # Clean up astap temporary files (.ini, .wcs) left in incoming directory
        _cleanup_astap_files(fits_path)

    except Exception as exc:
        logger.error("Failed to archive file: %s", exc, extra=extra)

    # ------------------------------------------------------------------
    # Step 10 — Finder charts (optional)
    #
    # For every anomaly with a resolved source_id, (re)generate and upload
    # its finder/discovery chart — a small PNG showing every frame that
    # source has ever been detected on, with its position circled on each
    # (see modules/finder_chart.py). Best-effort: failures here (missing
    # local archive file, API hiccup, rendering error) are logged and never
    # affect frame processing. Deduped by source_id — in practice Step 4.5
    # already collapses multiple detections of the same catalog identity
    # within one frame down to a single source, so seeing more than one
    # anomaly per source_id here would be unusual; this dict-based dedup is
    # defensive rather than the expected common case.
    #
    # Runs AFTER the archive move (step 9.5 above) so that this frame's own
    # FITS file is already present at FITS_ARCHIVE/{object}/{filename} by
    # the time chart rendering looks for it there.
    #
    # All source_ids for this frame are handled in one call to
    # finder_chart.update_charts_for_sources() — it fetches every track via
    # one GET /sources/tracks/batch and uploads every chart via one
    # POST /sources/charts/batch, instead of one GET+POST round trip per
    # source_id.
    # ------------------------------------------------------------------
    if finder_chart is not None and config.CHART_ENABLED:
        anomaly_type_by_source: dict = {}
        for anomaly in anomalies:
            source_id = anomaly.get("source_id")
            if source_id and source_id not in anomaly_type_by_source:
                anomaly_type_by_source[source_id] = anomaly.get("anomaly_type")

        if anomaly_type_by_source:
            try:
                chart_results = await finder_chart.update_charts_for_sources(anomaly_type_by_source)
                for source_id, anomaly_type in anomaly_type_by_source.items():
                    logger.debug(
                        "Finder chart %s for source_id=%s (%s)",
                        "updated" if chart_results.get(source_id) else "skipped",
                        source_id,
                        anomaly_type,
                        extra=extra,
                    )
            except Exception as exc:
                logger.warning(
                    "Finder chart batch update failed for %d source(s): %s — continuing",
                    len(anomaly_type_by_source),
                    exc,
                    extra=extra,
                )
    else:
        if finder_chart is None:
            logger.debug("Finder chart module not available — skipping", extra=extra)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _dedupe_by_catalog_identity(sources: list, extra: dict) -> list:
    """
    Collapse multiple detections in this frame's source list that resolved
    to the very same catalog identity (catalog_name, catalog_id) into a
    single representative source. See Step 4.5's comment in run() above for
    the rationale.

    Sources with no catalog match (catalog_name is None, or catalog_id is
    None) are never merged — every uncatalogued detection is kept as a
    distinct source, since it has no stable identity to deduplicate on.

    When two detections share an identity, prefer the one NOT tagged
    `_from_subtraction` (normal source-extractor detections are typically
    more astrometrically/photometrically precise than a diff-image blob);
    among two of the same kind, prefer the brighter one (higher flux).
    """
    kept_index_by_key: dict[tuple, int] = {}
    deduped: list = []
    n_merged = 0

    for src in sources:
        catalog_name = src.get("catalog_name")
        catalog_id = src.get("catalog_id")
        if catalog_name is None or catalog_id is None:
            deduped.append(src)
            continue

        key = (catalog_name, catalog_id)
        idx = kept_index_by_key.get(key)
        if idx is None:
            kept_index_by_key[key] = len(deduped)
            deduped.append(src)
            continue

        n_merged += 1
        if _prefer_candidate(src, deduped[idx]):
            deduped[idx] = src

    if n_merged:
        logger.info(
            "Deduplicated %d source(s) sharing a catalog identity with "
            "another detection in this frame (%d unique sources remain)",
            n_merged,
            len(deduped),
            extra=extra,
        )

    return deduped


def _prefer_candidate(candidate: dict, existing: dict) -> bool:
    """Return True if `candidate` should replace `existing` as the kept detection."""
    existing_is_sub = bool(existing.get("_from_subtraction"))
    candidate_is_sub = bool(candidate.get("_from_subtraction"))
    if candidate_is_sub != existing_is_sub:
        return existing_is_sub  # prefer the non-subtraction detection
    return (candidate.get("flux") or 0.0) > (existing.get("flux") or 0.0)


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

