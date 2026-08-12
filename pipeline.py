"""
pipeline.py — Composable pipeline stages for processing FITS frames.

Split into three independently callable stages (see CLAUDE.md's job-queue
section for the pipeline-side design this backs):

    await pipeline.analyze_frame(fits_path)                           -> dict | None
    await pipeline.detect_anomalies_for_frame_data(...)                -> list[dict]
    await pipeline.detect_anomalies_for_frame_id(frame_id)             -> list[dict]
    await pipeline.generate_charts_for_anomalies(sources, anomalies)   -> dict
    await pipeline.generate_charts_for_source_ids(...)                 -> dict
    await pipeline.run(fits_path)                                     -> None
    await pipeline.preview_catalog_match(fits_path, task_id, item_id)  -> dict

`analyze_frame()` is "Module 1": header extraction, normalization, QC,
astrometry, subtraction, catalog matching, photometry, POST /frames +
POST /frames/{id}/sources, and the archive move — everything that needs the
FITS file itself on local disk. It ends there: the archive move happens
immediately after posting sources, NOT after anomaly detection as an
earlier, monolithic revision of this file did. Anomaly detection never
touches pixels or the local file at all, so gating the archive move behind
it only delayed archiving for no benefit, and would have blocked decoupling
Module 2 into a task that might run much later, well after this file is
long gone from FITS_INCOMING (see observatory-api's GET /frames/{id} and
GET /frames/{id}/sources, which is exactly what Module 2 needs instead).

`detect_anomalies_for_frame_data()` / `detect_anomalies_for_frame_id()` are
"Module 2". The first operates on an in-memory `sources` list — used right
after `analyze_frame()`, in the same process, by `run()` below. The second
reconstructs that same input purely from the API for a standalone task
processing an already-analyzed frame — e.g. re-running anomaly detection
under a fixed/changed classifier without re-running astrometry/photometry.
Neither touches local FITS files.

`generate_charts_for_anomalies()` / `generate_charts_for_source_ids()` are
"Module 3" — thin wrappers around modules/finder_chart.py's batching. The
`_source_ids` variant is what a standalone GENERATE_CHARTS task uses; the
`_anomalies` variant additionally knows how to derive each source's display
designation (MPC designation, or catalog_id as a fallback) from an in-memory
`sources` list, the way `run()` needs.

`run(fits_path)` composes all three for the single-frame case. watcher.py no
longer calls this on the live ingestion path — it submits batched ANALYZE
tasks instead (see watcher.py's own docstring) — so in practice `run()` is
now mainly a convenience composition for tests and any ad hoc single-file
invocation; worker.py is the actual task consumer for the three stages
above, dispatching each item to `analyze_frame()` /
`detect_anomalies_for_frame_id()` / `generate_charts_for_source_ids()`
directly rather than through `run()`.

`preview_catalog_match()` is a fourth, separate stage — a diagnostic tool
(backing the `PREVIEW_CATALOG_MATCH` task type), not part of the
ANALYZE/DETECT_ANOMALIES/GENERATE_CHARTS production path. It never moves/
archives its input file, and its only API call is uploading the rendered
chart (`POST /tasks/{task_id}/items/{item_id}/chart`) — no frame or source
is ever registered. See modules/catalog_preview.py for what it actually
renders; nothing from this stage is kept locally, on disk, after it returns.
"""

from __future__ import annotations

import glob
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
    from modules import catalog_preview
except ImportError:
    catalog_preview = None  # type: ignore[assignment]

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
    from modules import forced_photometry
except ImportError:
    forced_photometry = None  # type: ignore[assignment]

try:
    from modules import finder_chart
except ImportError:
    finder_chart = None  # type: ignore[assignment]

try:
    from api_client import client as api_client
except ImportError:
    api_client = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


def _cleanup_empty_incoming_parents(moved_path: str) -> None:
    """
    Remove empty parent directories between *moved_path* and FITS_INCOMING.

    After a file is moved out of the incoming tree (archived or rejected),
    this walks upward from the file's former parent directory, removing each
    directory that is now empty, stopping at (and never removing)
    FITS_INCOMING itself.  Handles the case where FITS files were placed
    inside subdirectories of FITS_INCOMING (e.g. ``incoming/m31/``).

    Best-effort: any ``OSError`` (directory not empty because another file
    landed there in the meantime, permission issue, already gone) is silently
    ignored — the directory will simply stay until the next cleanup
    opportunity.
    """
    incoming_real = os.path.realpath(config.FITS_INCOMING)
    parent = os.path.dirname(moved_path)

    while True:
        parent_real = os.path.realpath(parent)
        # Never remove FITS_INCOMING itself, and stop if we've walked past it.
        if parent_real == incoming_real or not parent_real.startswith(incoming_real + os.sep):
            break
        try:
            os.rmdir(parent)  # only succeeds if empty
            logger.debug("Removed empty incoming subdirectory: %s", parent)
        except OSError:
            break  # not empty or already gone — stop climbing
        parent = os.path.dirname(parent)


# ---------------------------------------------------------------------------
# Stage 1 — analyze a single FITS file (Module 1)
# ---------------------------------------------------------------------------


async def analyze_frame(fits_path: str) -> dict | None:
    """
    Process a single FITS file through header extraction, QC, astrometry,
    subtraction, catalog matching, photometry, forced photometry, and
    register it (+ its sources) with the API. Archives the file before
    returning.

    Steps:
        1. Extract FITS headers
        2. Quality control — stop on rejection
        3. Astrometry (optional)
        4. Catalog matching (optional) — enriches sources with Gaia/Simbad/MPC IDs
        5. Photometry (optional) — uses Gaia DR3 matches for zero-point calibration
        5.6. Forced photometry / reverse matching (optional) — measures flux at
             every catalog/MPC position not already caught by blind detection
        6. POST frame to API — stop on failure
        7. POST sources to API
        8. Move file to archive

    Parameters
    ----------
    fits_path:
        Absolute path to the incoming FITS file. A bare basename (no
        directory component) is treated as a reference to an
        already-archived frame and resolved against FITS_ARCHIVE first —
        see `_resolve_bare_filename()` below.

    Returns
    -------
    dict | None
        `None` if processing stopped early — a Dark/Flat/Bias calibration
        frame (archived with no analysis), a QC rejection, no `api_client`
        configured, or `POST /frames` itself failing. Otherwise:
            {
                "frame_id": str,
                "filename": str,      # normalized filename actually archived
                "basename": str,      # original incoming filename
                "sources": list[dict],  # enriched; "_source_id" attached where resolved
                "object_name": str,
                "obs_time": str | None,
                "subtraction_performed": bool,
            }
        A failure in any individual downstream step (catalog matching,
        photometry, posting sources, the archive move itself) is caught and
        logged — it degrades the result but does not turn it into `None`;
        the frame is still registered and its (possibly incompletely
        enriched) sources are still returned.
    """
    fits_path = _resolve_bare_filename(fits_path)
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
        _cleanup_empty_incoming_parents(fits_path)
        logger.info("Archived %s frame: %s → %s", frame_type, basename, dest_path, extra=extra)
        return None

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
        return None

    # qc.analyze() reports fwhm_median in "arcsec" when the frame's headers
    # carried enough info to derive a plate scale, otherwise in raw "pixels"
    # (see modules/qc.py's _read_pixel_scale()). Both astrometry.solve() and
    # subtraction.run() below treat this value as an angular PSF FWHM to
    # tighten their own detection filters — passing a pixel count through as
    # if it were arcsec would silently corrupt those filters (e.g. all-sky
    # cameras/lenses with no XPIXSZ/FOCALLEN headers), so only pass it on
    # when the unit actually matches.
    psf_fwhm_arcsec: float | None = (
        qc_result.get("fwhm_median")
        if qc_result.get("fwhm_unit") == "arcsec"
        else None
    )

    # ------------------------------------------------------------------
    # Step 3 — Astrometry (optional)
    # ------------------------------------------------------------------
    astro_result: dict = {}
    if astrometry is not None:
        try:
            astro_result = await astrometry.solve(
                fits_path,
                psf_fwhm_arcsec=psf_fwhm_arcsec,
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
    # earlier revision of this file did) made `sources` a local variable
    # whose first assignment happened after the read in Step 3.5, raising
    # UnboundLocalError on every frame where subtraction actually found
    # candidates.
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
                wcs=astro_result.get("wcs"),
                psf_fwhm_arcsec=psf_fwhm_arcsec,
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
    # Step 4.5b — Collapse an uncatalogued object's own normal-detection /
    # subtraction-candidate pair. _dedupe_by_catalog_identity() above only
    # catches this when both sides share a catalog identity; an
    # uncatalogued object (e.g. a comet MPC/SkyBot has no ephemeris data
    # for — see docs/ISSUES.md) has none, so its ordinary sep detection and
    # its own subtraction candidate both survived as separate `sources`
    # entries (real incident, 2026-08-11, C_2020_R4_ATLAS frames: every
    # frame produced two MOVING_UNKNOWN anomalies — one per detection path —
    # for what was physically one comet seen once per frame).
    # ------------------------------------------------------------------
    sources = _dedupe_uncatalogued_subtraction_pair(sources, extra)

    # ------------------------------------------------------------------
    # Step 4.6 — Positional dedup: suppress unmatched sources sitting on
    # top of a matched source (deblending artifacts, not real objects).
    # ------------------------------------------------------------------
    sources = _dedupe_unmatched_near_matched(sources, extra)

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
            # Narrowband (Hα/[OIII]/[SII]/[NII]) frames never get a Gaia
            # zero-point — see photometry.measure()'s skip_calibration
            # docstring and CLAUDE.md's "Filters — real astronomy context".
            # Checked off the raw header filter (not yet necessarily
            # normalized if NORMALIZE_ENABLED is false) via
            # normalizer.is_narrowband(), which normalizes internally.
            skip_calibration = bool(
                normalizer is not None
                and normalizer.is_narrowband(header.get("observation", {}).get("filter"))
            )
            sources = await photometry.measure(fits_path, sources, skip_calibration=skip_calibration)
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
    # values investigated in docs/ISSUES.md #2. Sources without a calibrated
    # magnitude simply get mag=None — delta_mag-based classifications
    # correctly don't fire for them rather than firing on a meaningless number.
    # ------------------------------------------------------------------
    # Also tag every source with this frame's own (normalized, if enabled)
    # filter as "_filter" — leading underscore because, like "_source_id",
    # it's internal-only and api_client._to_wire_source() strips it before
    # POST /frames/{id}/sources (source_observations has no filter column;
    # the frame-level filter already travels via POST /frames' own
    # "observation.filter"). anomaly_detector.py reads it to restrict its
    # historical Δmag comparison to same-filter detections only — comparing
    # an L-band magnitude against an old R-band or Hα epoch is a color-term
    # artifact, not real variability (see that module's section in
    # CLAUDE.md's "Filters — real astronomy context").
    frame_filter = header.get("observation", {}).get("filter")

    def _tag_mag_and_filter(source_list: list) -> None:
        for _src in source_list:
            _src["mag"] = _src.get("mag_calibrated") if _src.get("calibrated") else None
            _src["_filter"] = frame_filter

    _tag_mag_and_filter(sources)

    # ------------------------------------------------------------------
    # Step 5.6 — Forced photometry / reverse matching (see
    # modules/forced_photometry.py; originally proposed as ROADMAP.md #1,
    # see git log for that history).
    #
    # A second, independent pass: for every Gaia DR3 star / MPC object in
    # this frame's footprint with no corresponding entry in `sources`,
    # measure the flux at its predicted pixel position anyway instead of
    # silently treating it as "not detected". Runs AFTER Step 5 because it
    # needs this frame's own zero_point (read off any already-measured
    # source — photometry.measure() sets the same "zero_point" on every
    # source it returns) to calibrate its own measurements the same way.
    # Runs AFTER Step 5.5 above rather than before it, so the numbering
    # stays in execution order; its own results are tagged with "mag"/
    # "_filter" via the same _tag_mag_and_filter() helper instead of by a
    # second pass over the whole (by-then-larger) `sources` list.
    #
    # gaia_stars/mpc_objects are NOT re-queried: catalog_matcher.get_gaia_stars()/
    # get_mpc_objects() are cache hits against the exact query Step 4's
    # catalog_matcher.match() already made for this same field a moment ago
    # — see that module's section in CLAUDE.md.
    # ------------------------------------------------------------------
    if forced_photometry is not None and astro_result and catalog_matcher is not None:
        try:
            zero_point = next((s.get("zero_point") for s in sources if s.get("zero_point") is not None), None)
            zero_point_err = next((s.get("zero_point_err") for s in sources if s.get("zero_point_err") is not None), None)
            gaia_stars = catalog_matcher.get_gaia_stars(
                astro_result.get("ra_center") or header.get("ra") or 0.0,
                astro_result.get("dec_center") or header.get("dec") or 0.0,
                astro_result.get("fov_deg") or 1.0,
            )
            mpc_objects = catalog_matcher.get_mpc_objects(
                astro_result.get("ra_center") or header.get("ra") or 0.0,
                astro_result.get("dec_center") or header.get("dec") or 0.0,
                header.get("obs_time") or "",
                astro_result.get("fov_deg") or 1.0,
            )
            forced_sources = await forced_photometry.run(
                fits_path,
                sources,
                gaia_stars=gaia_stars,
                mpc_objects=mpc_objects,
                wcs=astro_result.get("wcs"),
                naxis1=astro_result.get("naxis1"),
                naxis2=astro_result.get("naxis2"),
                zero_point=zero_point,
                zero_point_err=zero_point_err,
                obs_time=header.get("obs_time"),
                psf_fwhm_arcsec=psf_fwhm_arcsec,
            )
            if forced_sources:
                _tag_mag_and_filter(forced_sources)
                sources = sources + forced_sources
                logger.info(
                    "Forced photometry recovered %d catalog position(s) not caught by blind "
                    "detection; total sources: %d",
                    len(forced_sources), len(sources),
                    extra=extra,
                )
        except Exception as exc:
            logger.error(
                "Forced photometry failed: %s — continuing without it",
                exc,
                extra=extra,
            )
    else:
        if forced_photometry is None:
            logger.debug("Forced photometry module not available — skipping", extra=extra)

    # ------------------------------------------------------------------
    # Step 6 — Post frame to API
    # ------------------------------------------------------------------
    if api_client is None:
        logger.warning(
            "API client not available — skipping all API steps and archive move",
            extra=extra,
        )
        return None

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
        return None

    # ------------------------------------------------------------------
    # Step 7 — Post sources (includes catalog match info from step 4)
    #
    # The API returns `source_ids`, positionally parallel to `sources`, so
    # each source dict can be tagged with its resolved `sources.id` here.
    # anomaly_detector.py reads this back (as "_source_id") to populate
    # `anomalies[].source_id` — otherwise the API has no way to know which
    # catalog source an anomaly refers to.
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
    # Step 8 — Archive move and cleanup.
    #
    # Runs immediately after posting sources — NOT after anomaly detection.
    # Anomaly detection (Module 2) never touches the local file, so there is
    # no reason to keep it sitting in FITS_INCOMING any longer than this;
    # doing so would also block Module 2 from ever running as a standalone
    # task well after this function has returned (see module docstring).
    # ------------------------------------------------------------------
    try:
        # Bake astap's verified, freshly-solved WCS into fits_path's own
        # header before it gets archived — see _write_solved_wcs()'s
        # docstring below for why (2026-08-06 UGC_6930 incident).
        solved_wcs = astro_result.get("wcs")
        if solved_wcs is not None:
            _write_solved_wcs(fits_path, solved_wcs)

        # Use object name for directory structure (normalized if normalization enabled)
        dest_dir = os.path.join(config.FITS_ARCHIVE, object_name)
        os.makedirs(dest_dir, exist_ok=True)

        # Rename file to normalized filename (if normalization enabled)
        dest_path = os.path.join(dest_dir, normalized_filename)
        shutil.move(fits_path, dest_path)

        logger.info("Archived: %s → %s", original_filename, dest_path, extra=extra)

        # Clean up astap temporary files (.ini, .wcs) left in incoming directory
        _cleanup_astap_files(fits_path)

        # Remove empty parent directories inside FITS_INCOMING (e.g. after
        # processing incoming/m31/frame.fits the now-empty m31/ is removed).
        _cleanup_empty_incoming_parents(fits_path)

    except Exception as exc:
        logger.error("Failed to archive file: %s", exc, extra=extra)

    return {
        "frame_id": frame_id,
        "filename": normalized_filename,
        "basename": basename,
        "sources": sources,
        "object_name": object_name,
        "obs_time": header.get("obs_time"),
        "subtraction_performed": subtraction_info.get("performed", False),
    }


# ---------------------------------------------------------------------------
# Stage 2 — anomaly detection (Module 2)
# ---------------------------------------------------------------------------


async def detect_anomalies_for_frame_data(
    frame_id: str,
    sources: list,
    frame_meta: dict,
    *,
    post_filename: str,
) -> list[dict]:
    """
    Classify anomalies for an already-in-memory `sources` list and post the
    result to the API. The core of Module 2 — shared by `run()` (called
    right after `analyze_frame()`, same process) and
    `detect_anomalies_for_frame_id()` below (called with `sources`
    reconstructed from the API for a standalone task).

    Parameters
    ----------
    frame_id:
        Frame ID this classification is for.
    sources:
        Enriched source dicts (ra, dec, mag, catalog_name, catalog_id,
        object_type, elongation, saturated, near_edge, "_from_subtraction",
        "_source_id" — see modules/anomaly_detector.py's `detect()`).
    frame_meta:
        Dict with at least "filename" and "obs_time" — the two fields
        anomaly_detector.py actually reads (used for logging and the
        before_time bound of its history queries).
    post_filename:
        Filename to send with `POST /frames/{id}/anomalies` — kept as a
        separate parameter from `frame_meta["filename"]` because `run()`
        historically posts the *normalized* filename here while logging
        under the frame's *original* incoming basename; preserved rather
        than unified so existing behavior doesn't shift.

    Returns
    -------
    list[dict]
        The anomaly dicts (whatever anomaly_detector.detect() returned, or
        [] if the module is unavailable or classification failed). Posted
        to the API regardless (an empty list is a valid, meaningful
        payload — see docs/API.md's replace semantics for
        POST /frames/{id}/anomalies).
    """
    extra = {"fits_filename": frame_meta.get("filename", post_filename)}

    anomalies: list = []
    if anomaly_detector is not None:
        try:
            anomalies = await anomaly_detector.detect(frame_id, sources, sources, frame_meta)
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

    if api_client is not None:
        try:
            await api_client.post_anomalies(frame_id, post_filename, anomalies)
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

    return anomalies


def _from_wire_source(api_source: dict, frame_filter: str | None = None) -> dict:
    """
    Translate one entry of GET /frames/{id}/sources' response into the
    internal shape modules/anomaly_detector.py expects — the inverse of
    api_client._to_wire_source(). Used by detect_anomalies_for_frame_id()
    below to reconstruct a frame's `sources` list purely from API data, with
    no in-memory state carried over from when the frame was first analyzed.

    `frame_filter` is the parent frame's own filter (GET /frames/{id}'s
    flattened "filter" field) — every source in a GET /frames/{id}/sources
    response was observed on that one frame, so they all share it. There is
    no per-source filter field on the wire (source_observations has no such
    column; see api_client.py) — this is the standalone-task counterpart of
    analyze_frame()'s in-memory "_filter" tagging (Step 5.5), needed so
    anomaly_detector.py's same-filter Δmag comparison also works when
    DETECT_ANOMALIES is re-run later as its own task.
    """
    return {
        "ra": api_source.get("ra"),
        "dec": api_source.get("dec"),
        "mag": api_source.get("mag"),
        "catalog_name": api_source.get("catalog_name"),
        "catalog_id": api_source.get("catalog_id"),
        "object_type": api_source.get("object_type"),
        "elongation": api_source.get("elongation") or 0.0,
        "saturated": bool(api_source.get("saturated")),
        # No leading underscore on the wire, same as "saturated" — see
        # astrometry.py's near_edge docstring for why it must survive here.
        "near_edge": bool(api_source.get("near_edge")),
        "_from_subtraction": bool(api_source.get("from_subtraction")),
        "_source_id": api_source.get("source_id"),
        "_filter": frame_filter,
    }


async def detect_anomalies_for_frame_id(frame_id: str) -> list[dict]:
    """
    Standalone DETECT_ANOMALIES worker entry point.

    Reconstructs a frame's `sources` list purely from the API
    (GET /frames/{id}, GET /frames/{id}/sources) and runs anomaly detection
    against it — no local FITS access needed at all. This is what lets
    anomaly detection be re-run for an already-analyzed frame (a fixed or
    new classifier, or simply an object's entire observation history at
    once, old and new frames alike) without re-running astrometry/photometry.

    Returns
    -------
    list[dict]
        Same shape as detect_anomalies_for_frame_data(), with one addition:
        every anomaly that has a "source_id" also gets a "_designation" key
        (its mpc_designation if set, else the matching source's catalog_id,
        else None) — see modules/finder_chart.py's designation-title
        feature. worker.py reads this to build a GENERATE_CHARTS task's
        per-item payload without a second round trip. [] if the frame
        doesn't exist, the API client isn't configured, or the fetch fails.
    """
    if api_client is None:
        logger.warning("detect_anomalies_for_frame_id: API client not available")
        return []

    frame = await api_client.get_frame(frame_id)
    if frame is None:
        logger.warning("detect_anomalies_for_frame_id: frame_id=%s not found", frame_id)
        return []

    api_sources = await api_client.get_frame_sources(frame_id)
    frame_filter = frame.get("filter")
    sources = [_from_wire_source(s, frame_filter) for s in api_sources]

    frame_meta = {
        "filename": frame.get("filename"),
        "obs_time": frame.get("obs_time"),
    }

    anomalies = await detect_anomalies_for_frame_data(
        frame_id,
        sources,
        frame_meta,
        post_filename=frame.get("filename") or "<unknown>",
    )

    # Same "prefer mpc_designation, fall back to catalog_id via source_id"
    # rule as generate_charts_for_anomalies() below uses for the in-memory
    # case — see that function's comment for the real incident this guards
    # against (a shared/stale sources.catalog_id).
    catalog_id_by_source_id = {
        s["_source_id"]: s["catalog_id"]
        for s in sources
        if s.get("_source_id") and s.get("catalog_name") and s.get("catalog_id")
    }
    for anomaly in anomalies:
        source_id = anomaly.get("source_id")
        if source_id:
            anomaly["_designation"] = anomaly.get("mpc_designation") or catalog_id_by_source_id.get(source_id)

    return anomalies


# ---------------------------------------------------------------------------
# Stage 3 — finder charts (Module 3)
# ---------------------------------------------------------------------------


async def generate_charts_for_source_ids(
    anomaly_types_by_source_id: dict,
    designation_by_source_id: dict | None = None,
) -> dict:
    """
    Standalone GENERATE_CHARTS worker entry point — a thin wrapper kept for
    a consistent pipeline.py import surface; all the real batching logic
    lives in modules/finder_chart.py. Safe to call with every source_id a
    GENERATE_CHARTS task covers at once, regardless of how many frames they
    originally came from — one call renders and uploads every one of them.

    `anomaly_types_by_source_id` maps each source_id to a LIST of
    anomaly_types requesting a chart for it (not a single value) — a source
    can legitimately be classified more than one way over its lifetime (see
    modules/finder_chart.py's module docstring), and each distinct style
    those types imply gets its own chart rather than one arbitrarily
    overwriting the rest. A list entry of None is valid — a chart requested
    directly for a source with no anomaly behind it at all (e.g.
    observatory-api's `/ui/sources/generate-charts`, which never sets
    payload.anomaly_type — see worker.py's `_run_charts_task()`). That's not
    a missing/invalid entry; modules/finder_chart.py picks a sensible
    fallback style for it (see that module's `_style_for_source()`).

    Returns
    -------
    dict
        source_id -> {anomaly_type: bool} (True on successful chart update
        for that type's style). {} if the module is unavailable, charting is
        disabled (CHART_ENABLED=false), or `anomaly_types_by_source_id` is
        empty.
    """
    if finder_chart is None or not config.CHART_ENABLED or not anomaly_types_by_source_id:
        return {}
    return await finder_chart.update_charts_for_sources(anomaly_types_by_source_id, designation_by_source_id)


async def generate_charts_for_anomalies(sources: list, anomalies: list) -> dict:
    """
    Build the (source_id -> [anomaly_type, ...]) / (source_id -> designation)
    maps from an in-memory `sources` + `anomalies` pair and update every
    affected source's finder chart(s) in one batch. This is what `run()`
    uses right after `detect_anomalies_for_frame_data()`, in the same
    process.

    Grouped by source_id, collecting every anomaly_type seen for it (not
    just the first) — a source can, in principle, end up with more than one
    anomaly on the very same frame (e.g. the API resolving "_source_id"
    positionally onto the same row for two different detections — see the
    designation-resolution caveat below), and modules/finder_chart.py needs
    every one of those types to render each distinct chart style they imply
    (see that module's docstring). In practice this is usually a
    single-element list per source_id, since pipeline.py's
    dedup-by-catalog-identity step already collapses multiple detections of
    the same catalog identity within one frame down to a single source.

    Designation resolution prefers each anomaly's OWN "mpc_designation" (set
    by anomaly_detector.py straight from the one `source` dict that produced
    that specific classification) over `sources`.catalog_id looked up by
    "_source_id". The latter is NOT always safe: the API resolves
    "_source_id" positionally, so a moving object that happens to pass near
    an already-catalogued star's position can get folded into that SAME
    `sources` row — whose catalog_name/catalog_id may then reflect the star
    (from a different detection, possibly on a different frame entirely),
    not the asteroid actually detected here (real incident, 2026-08-06,
    Vesta_A807_FA test data — see modules/finder_chart.py). Only anomaly
    types without an mpc_designation field at all fall back to the
    sources-table lookup, which is safe in practice since those don't hinge
    on a per-frame moving-object identity the way MPC matches do.

    Best-effort: any exception from the underlying chart update is caught
    and logged, never raised — this must never affect frame processing.

    Returns
    -------
    dict
        source_id -> {anomaly_type: bool}, or {} if there was nothing to
        chart (no anomaly had a resolved source_id) or charting is
        disabled/unavailable.
    """
    if finder_chart is None or not config.CHART_ENABLED:
        if finder_chart is None:
            logger.debug("Finder chart module not available — skipping")
        return {}

    anomaly_types_by_source: dict = {}
    for anomaly in anomalies:
        source_id = anomaly.get("source_id")
        if source_id:
            anomaly_types_by_source.setdefault(source_id, []).append(anomaly.get("anomaly_type"))

    if not anomaly_types_by_source:
        return {}

    designation_by_source: dict = {}
    mpc_designation_by_source: dict = {}
    for anomaly in anomalies:
        source_id = anomaly.get("source_id")
        mpc_designation = anomaly.get("mpc_designation")
        if source_id and mpc_designation and source_id not in mpc_designation_by_source:
            mpc_designation_by_source[source_id] = mpc_designation

    catalog_id_by_source_id = {
        src["_source_id"]: src["catalog_id"]
        for src in sources
        if src.get("_source_id") and src.get("catalog_name") and src.get("catalog_id")
    }

    for source_id in anomaly_types_by_source:
        designation = mpc_designation_by_source.get(source_id) or catalog_id_by_source_id.get(source_id)
        if designation:
            designation_by_source[source_id] = designation

    try:
        chart_results = await generate_charts_for_source_ids(anomaly_types_by_source, designation_by_source)
        for source_id, anomaly_types in anomaly_types_by_source.items():
            source_results = chart_results.get(source_id) or {}
            logger.debug(
                "Finder chart(s) for source_id=%s (%s): %s",
                source_id,
                anomaly_types,
                source_results,
            )
        return chart_results
    except Exception as exc:
        logger.warning(
            "Finder chart batch update failed for %d source(s): %s — continuing",
            len(anomaly_types_by_source),
            exc,
        )
        return {}


# ---------------------------------------------------------------------------
# Public entry point — composes the three stages for one FITS file
# ---------------------------------------------------------------------------


async def run(fits_path: str) -> None:
    """
    Process a single FITS file end-to-end: analyze, detect anomalies,
    generate charts — the composition of the three stages above, still used
    by watcher.py for every file it sees land in FITS_INCOMING.

    Parameters
    ----------
    fits_path:
        Absolute path to the incoming FITS file.
    """
    result = await analyze_frame(fits_path)
    if result is None:
        return

    anomaly_frame_meta = {
        "filename": result["basename"],
        "obs_time": result["obs_time"],
        "subtraction_performed": result["subtraction_performed"],
    }

    anomalies = await detect_anomalies_for_frame_data(
        result["frame_id"],
        result["sources"],
        anomaly_frame_meta,
        post_filename=result["filename"],
    )

    await generate_charts_for_anomalies(result["sources"], anomalies)


# ---------------------------------------------------------------------------
# Stage 4 — catalog-match preview (diagnostic tool, not part of the
# ANALYZE/DETECT_ANOMALIES/GENERATE_CHARTS production path)
# ---------------------------------------------------------------------------


async def preview_catalog_match(fits_path: str, task_id: str, item_id: str) -> dict:
    """
    Standalone PREVIEW_CATALOG_MATCH worker entry point. Renders the
    diagnostic image via modules/catalog_preview.py's render() (never
    calling the API itself, never moving/archiving `fits_path`), then
    uploads the resulting PNG to observatory-api via
    POST /tasks/{task_id}/items/{item_id}/chart — the task_item_id-keyed
    counterpart of a source's finder chart (see docs/API.md section 15 and
    observatory-api's SourceChartModel). No local copy of the PNG is kept;
    it lives only in the API's storage once this returns.

    Reuses modules/catalog_matcher.py directly (not a separate copy of the
    matching logic), so repeated frames of the same object/session within
    one task benefit from its on-disk cache exactly like a production
    ANALYZE run would — only the first frame per sky tile actually re-hits
    Gaia/Simbad/2MASS/Pan-STARRS/MPC.

    Parameters
    ----------
    fits_path:
        Path to the FITS file — may still be sitting in FITS_INCOMING, or
        already archived/rejected; never modified, moved, or removed. A bare
        basename (no directory component) is treated as a reference to an
        already-archived frame and resolved against FITS_ARCHIVE first —
        see `_resolve_bare_filename()`.
    task_id, item_id:
        Identify the PREVIEW_CATALOG_MATCH task item this chart belongs to
        — both are required to build the upload URL.

    Returns
    -------
    dict
        {"matched": int, "total": int, "quality_flag": str,
         "chart_uploaded": bool} — `chart_uploaded` is False if rendering
        succeeded but the API upload itself failed (logged by api_client;
        never raises), so the caller can still tell the two apart even
        though neither is treated as a FAILED item on its own — a rendered-
        but-unuploaded chart is a real, if incomplete, result, not nothing.

    Raises
    ------
    RuntimeError
        Propagated from render() if astrometry fails — the caller
        (worker.py) is expected to catch this and report the item FAILED,
        same as any other stage's per-item failure handling.
    """
    if catalog_preview is None:
        raise RuntimeError("modules.catalog_preview is not available")

    fits_path = _resolve_bare_filename(fits_path)
    result = await catalog_preview.render(fits_path)

    chart_uploaded = False
    if api_client is not None:
        chart_uploaded = await api_client.upload_task_item_chart(
            task_id, item_id, result["png_bytes"], style="catalog_preview", frame_count=1,
        )

    return {
        "matched": result["matched"],
        "total": result["total"],
        "quality_flag": result["quality_flag"],
        "chart_uploaded": chart_uploaded,
    }


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _resolve_bare_filename(fits_path: str) -> str:
    """
    Resolve a bare filename (no directory component at all) to its actual
    location under FITS_ARCHIVE.

    ANALYZE and PREVIEW_CATALOG_MATCH task items are documented (see
    CLAUDE.md's job-queue table, docs/API.md section 15, and this module's
    own docstrings) as carrying the FULL path to the FITS file — the caller
    that creates the task_item is supposed to know where the file actually
    is. observatory-api's Web\\FramesController::createTask() debug page
    violates that: it builds these task_items straight from an
    already-registered frame's `frames.filename` column, which is a
    basename only (see `_build_frame_payload()` above — the API never
    receives a full path at all). The API can't fix this itself: it has no
    filesystem access to /fits/... whatsoever (see CLAUDE.md's "Architecture:
    Two Repositories") and therefore no way to know FITS_ARCHIVE's actual
    value for this deployment — only this process does, so the fallback has
    to live here rather than on the API side.

    A path that already has a directory component (relative or absolute) is
    returned unchanged — this only kicks in for a bare basename, which is
    unambiguous: a real path is never mistaken for one. Searches every
    FITS_ARCHIVE/{object}/ subdirectory for an exact filename match and
    returns the first hit. Zero or more than one match returns the input
    unchanged, so the caller's normal "file not found" failure still
    surfaces rather than a confusing resolver-internal one; more than one
    match is also logged, since normalized filenames are expected to be
    unique across the whole archive.
    """
    if os.path.dirname(fits_path):
        return fits_path

    matches = sorted(glob.glob(os.path.join(config.FITS_ARCHIVE, "*", fits_path)))

    if len(matches) == 1:
        return matches[0]

    if len(matches) > 1:
        logger.warning(
            "Bare filename %r matches multiple archived frames; using the first: %s",
            fits_path, matches,
        )
        return matches[0]

    return fits_path


def _dedupe_by_catalog_identity(sources: list, extra: dict) -> list:
    """
    Collapse multiple detections in this frame's source list that resolved
    to the very same catalog identity (catalog_name, catalog_id) into a
    single representative source. See analyze_frame()'s Step 4.5 comment
    for the rationale.

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


def _dedupe_uncatalogued_subtraction_pair(sources: list, extra: dict) -> list:
    """
    Collapse an uncatalogued source's own normal-extraction detection with
    its own image-subtraction candidate, within this one frame, when both
    refer to the same physical object.

    _dedupe_by_catalog_identity() above only collapses duplicates that share
    a catalog identity (catalog_name, catalog_id) — an uncatalogued object
    has none, so a normal detection and a nearby subtraction candidate of
    the very same object both survive as two separate `sources` entries.
    Each is later posted as its own source_observations row (inflating
    sources.observation_count for one real observation) and independently
    classified by anomaly_detector.py — two anomalies in `anomalies` for
    what is physically one detection (real incident, 2026-08-11,
    C_2020_R4_ATLAS: every frame produced two MOVING_UNKNOWN anomalies,
    positions ~1" apart, one from the ordinary extractor and one from
    modules/subtraction.py).

    Deliberately scoped narrower than a general "merge any two nearby
    uncatalogued sources" — that would risk collapsing two genuinely
    different faint objects sitting close together in a crowded field. This
    only pairs an entry carrying `_from_subtraction=True` with one that
    doesn't, within MATCH_CONE_ARCSEC — exactly the shape this specific bug
    produces — and reuses the same _prefer_candidate() preference already
    used for the catalogued case above (non-subtraction wins; ties broken
    by flux).
    """
    from astropy.coordinates import SkyCoord
    import astropy.units as u

    candidate_positions = [i for i, s in enumerate(sources) if s.get("catalog_name") is None]
    if len(candidate_positions) < 2:
        return sources

    threshold = config.MATCH_CONE_ARCSEC * u.arcsec
    dropped: set[int] = set()
    n_merged = 0

    for a_pos, idx_a in enumerate(candidate_positions):
        if idx_a in dropped:
            continue
        src_a = sources[idx_a]
        is_sub_a = bool(src_a.get("_from_subtraction"))

        for idx_b in candidate_positions[a_pos + 1:]:
            if idx_b in dropped:
                continue
            src_b = sources[idx_b]

            # Only pair a subtraction candidate with a non-subtraction
            # detection — two ordinary uncatalogued detections that happen
            # to sit close together are left alone (might be two real faint
            # objects in a crowded field, not a duplicate).
            if is_sub_a == bool(src_b.get("_from_subtraction")):
                continue

            sep = SkyCoord(ra=src_a["ra"] * u.deg, dec=src_a["dec"] * u.deg).separation(
                SkyCoord(ra=src_b["ra"] * u.deg, dec=src_b["dec"] * u.deg)
            )
            if sep >= threshold:
                continue

            n_merged += 1
            if _prefer_candidate(src_b, src_a):
                dropped.add(idx_a)
                break  # src_a lost; stop pairing it against further candidates
            dropped.add(idx_b)

    if n_merged:
        logger.info(
            "Deduplicated %d uncatalogued normal-detection/subtraction-candidate "
            "pair(s) of the same object (%d sources remain)",
            n_merged, len(sources) - len(dropped),
            extra=extra,
        )

    return [s for i, s in enumerate(sources) if i not in dropped]


def _dedupe_unmatched_near_matched(sources: list, extra: dict) -> list:
    """
    Remove uncatalogued sources that sit within MATCH_CONE_ARCSEC of a
    catalogue-matched source in the same frame — these are almost always
    deblending artifacts (sep splitting one elongated/coma-distorted PSF near
    frame edges into two components, one of which lands just outside the
    catalog cone) rather than real distinct objects (real incident, 2026-08-10:
    Vesta frames showed overlapping green+red circles for a single star at the
    edge of the field — the red circle was an UNKNOWN false-positive waiting to
    happen). A matched source at that position already accounts for the star's
    existence, so the duplicate adds no information and would only trigger a
    spurious UNKNOWN anomaly downstream.

    Only suppresses the UNMATCHED duplicate — a matched source is never
    removed, and two unmatched sources near each other are left alone (they
    might genuinely be two faint uncatalogued objects in a crowded field).
    """
    from astropy.coordinates import SkyCoord
    import astropy.units as u

    matched = [s for s in sources if s.get("catalog_name") is not None]
    if not matched:
        return sources

    matched_coords = SkyCoord(
        ra=[s["ra"] for s in matched] * u.deg,
        dec=[s["dec"] for s in matched] * u.deg,
    )

    threshold = config.MATCH_CONE_ARCSEC * u.arcsec
    kept: list = []
    n_suppressed = 0

    for src in sources:
        if src.get("catalog_name") is not None:
            # Matched sources are always kept
            kept.append(src)
            continue

        src_coord = SkyCoord(ra=src["ra"] * u.deg, dec=src["dec"] * u.deg)
        sep = src_coord.separation(matched_coords).min()
        if sep < threshold:
            n_suppressed += 1
            continue

        kept.append(src)

    if n_suppressed:
        logger.info(
            "Suppressed %d unmatched source(s) within %.1f\" of a matched source "
            "(deblending artifacts) — %d sources remain",
            n_suppressed, config.MATCH_CONE_ARCSEC, len(kept),
            extra=extra,
        )

    return kept


def _prefer_candidate(candidate: dict, existing: dict) -> bool:
    """Return True if `candidate` should replace `existing` as the kept detection."""
    existing_is_sub = bool(existing.get("_from_subtraction"))
    candidate_is_sub = bool(candidate.get("_from_subtraction"))
    if candidate_is_sub != existing_is_sub:
        return existing_is_sub  # prefer the non-subtraction detection
    return (candidate.get("flux") or 0.0) > (existing.get("flux") or 0.0)


def _write_solved_wcs(fits_path: str, wcs) -> bool:
    """
    Write astap's verified, freshly-solved WCS into fits_path's own header.

    astap runs without ``-update`` (see modules/astrometry.py), so it never
    writes its solution into the FITS file itself — only into a `.wcs` side
    file that _cleanup_astap_files() below deletes right after this frame's
    processing finishes. If nothing here corrected the header first, the
    archived file would keep whatever WCS it originally arrived with —
    e.g. a capture program's mount-pointing/tracking estimate, not a real
    plate solve — forever. modules/finder_chart.py (and any future code)
    reads WCS straight back out of the archived file's own header when
    rendering a source's history, so every consumer downstream would
    silently re-inherit the same stale-coordinate problem astrometry.py was
    just fixed to stop trusting.

    Real incident (2026-08-06, "UGC_6930" test frame): the header's own WCS
    and astap's fresh solve differed by ~178" (astap's own solve log had
    already reported and corrected that same "Mount offset").

    Called right before the archive move, while fits_path is still writable
    at its pre-archive location. Best-effort: any failure is logged and
    returns False — never raises, never blocks the archive move.
    """
    try:
        from astropy.io import fits as astropy_fits  # noqa: PLC0415

        with astropy_fits.open(fits_path, mode="update", output_verify="silentfix") as hdul:
            hdul[0].header.update(wcs.to_header())
        return True
    except Exception as exc:
        logger.warning("Could not write solved WCS into %s: %s", fits_path, exc)
        return False


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

    The structure matches the POST /frames API payload defined in docs/API.md,
    with nested sub-dicts for observation, instrument, sensor, observer, software, and qc.

    If normalization is enabled, all values in header are already normalized.
    """
    # Get fov_deg from astrometry, or calculate from FITS headers as fallback
    fov_deg = astro_result.get("fov_deg")
    if fov_deg is None:
        fov_deg = _calculate_fov_from_headers(header)

    return {
        "filename": filename,
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
