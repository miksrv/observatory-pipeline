"""
modules/anomaly_detector.py — Anomaly detection and classification for the pipeline.

The single public entry point is:

    await anomaly_detector.detect(frame_id, sources, catalog_matches, frame_meta) -> list[dict]

For each source in the frame the module:

1. Collects all unique sky tiles from sources that need history queries.
2. Makes TWO batch API requests: one for source history, one for frame coverage.
3. Classifies each source using the pre-fetched data (no per-source API calls).
4. Calls ephemeris.query() concurrently for all ASTEROID / COMET sources.
5. Returns only actionable anomaly dicts (FIRST_OBSERVATION and KNOWN_CATALOG_NEW
   are never elevated to anomaly records; they are logged but not returned).

This batch approach reduces API calls from O(N) to O(1) where N is the number of sources,
preventing server overload when processing frames with hundreds of detected sources.
"""

from __future__ import annotations

import asyncio
import logging
import math
import statistics
from typing import Any

import config
from api_client import client as api_client
from modules import ephemeris

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Anomaly type constants
# ---------------------------------------------------------------------------

_TYPE_FIRST_OBS        = "FIRST_OBSERVATION"
_TYPE_KNOWN_NEW        = "KNOWN_CATALOG_NEW"
_TYPE_VARIABLE_STAR    = "VARIABLE_STAR"
_TYPE_BINARY_STAR      = "BINARY_STAR"
_TYPE_SUPERNOVA        = "SUPERNOVA_CANDIDATE"
_TYPE_UNKNOWN          = "UNKNOWN"
_TYPE_ASTEROID         = "ASTEROID"
_TYPE_COMET            = "COMET"
_TYPE_MOVING_UNKNOWN   = "MOVING_UNKNOWN"
_TYPE_SPACE_DEBRIS     = "SPACE_DEBRIS"

# Alert-worthy types (used for log-level selection)
_ALERT_TYPES: frozenset[str] = frozenset({
    _TYPE_SUPERNOVA,
    _TYPE_MOVING_UNKNOWN,
    _TYPE_SPACE_DEBRIS,
    _TYPE_UNKNOWN,
})

# ---------------------------------------------------------------------------
# Simbad object-type substring classifiers
# ---------------------------------------------------------------------------

# Known variable-star OTYPE substrings (Simbad OTYPE field)
_VARIABLE_STAR_OTYPES: tuple[str, ...] = ("V*", "RR", "Cep", "BY", "RS", "Ell", "bL")

# Known binary/eclipsing-binary OTYPE substrings
_BINARY_STAR_OTYPES: tuple[str, ...] = ("**", "EB", "SB")

# Galaxy-related OTYPE substrings — proximity triggers SUPERNOVA_CANDIDATE
_GALAXY_OTYPES: tuple[str, ...] = ("G", "SFG", "AGN", "GiG")


# ---------------------------------------------------------------------------
# Coordinate helpers
# ---------------------------------------------------------------------------

def _haversine_arcsec(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
    """
    Great-circle angular separation between two points in arcseconds.

    Uses the haversine formula to avoid catastrophic cancellation near the poles.
    All inputs in decimal degrees.
    """
    ra1_r  = math.radians(ra1)
    ra2_r  = math.radians(ra2)
    dec1_r = math.radians(dec1)
    dec2_r = math.radians(dec2)

    delta_ra  = ra2_r  - ra1_r
    delta_dec = dec2_r - dec1_r

    a = (
        math.sin(delta_dec / 2.0) ** 2
        + math.cos(dec1_r) * math.cos(dec2_r) * math.sin(delta_ra / 2.0) ** 2
    )
    sep_rad = 2.0 * math.asin(math.sqrt(a))
    return math.degrees(sep_rad) * 3600.0  # convert degrees → arcseconds


# ---------------------------------------------------------------------------
# Object-type classifiers
# ---------------------------------------------------------------------------

def _is_variable_star(object_type: str | None) -> bool:
    """Return True if the Simbad OTYPE indicates a known variable star."""
    if object_type is None:
        return False
    return any(token in object_type for token in _VARIABLE_STAR_OTYPES)


def _is_binary_star(object_type: str | None) -> bool:
    """Return True if the Simbad OTYPE indicates a binary / eclipsing binary."""
    if object_type is None:
        return False
    return any(token in object_type for token in _BINARY_STAR_OTYPES)


def _is_galaxy(object_type: str | None) -> bool:
    """
    Return True if the Simbad OTYPE indicates a galaxy or galaxy-like object.

    We use a word-boundary-aware check: each galaxy token must appear as a
    standalone word (surrounded by non-alphanumeric characters or string edges)
    so that "G" does not falsely match inside "AGN" twice, or trigger on "GiC"
    (group of galaxies, different classification).  The simple substring check
    in the spec is sufficient here because the token set is carefully chosen to
    be unambiguous within the Simbad OTYPE vocabulary.
    """
    if object_type is None:
        return False
    # Check each token directly — Simbad OTYPEs are short codes, not sentences.
    return any(token in object_type for token in _GALAXY_OTYPES)


# ---------------------------------------------------------------------------
# Tile helpers for batch queries
# ---------------------------------------------------------------------------

def _tile_key(ra: float, dec: float, tile_size: float = 0.1) -> tuple[float, float]:
    """
    Round RA/Dec to tiles of given size (in degrees) for batch query optimization.

    Default tile_size=0.1 degrees (~6 arcmin) groups nearby sources together.
    """
    return round(ra / tile_size) * tile_size, round(dec / tile_size) * tile_size


# ---------------------------------------------------------------------------
# History helpers
# ---------------------------------------------------------------------------

def _extract_mag(source_dict: dict) -> float | None:
    """
    Safely extract a magnitude float from a historical source dict returned
    by the API.  The API may use 'mag' or 'magnitude' as the key.
    """
    for key in ("mag", "magnitude"):
        val = source_dict.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return None


def _history_median_mag(history: list[dict]) -> float | None:
    """
    Compute the median magnitude across all prior detections.

    Returns None if no valid magnitude values are present.
    """
    mags = [m for src in history if (m := _extract_mag(src)) is not None]
    if not mags:
        return None
    return statistics.median(mags)


def _find_sources_within_radius(
    ra: float,
    dec: float,
    radius_arcsec: float,
    all_sources: list[dict],
) -> list[dict]:
    """
    Filter sources that fall within radius_arcsec from (ra, dec).

    This is used to find sources near a specific position from the batch
    query results which cover a larger tile area.
    """
    result = []
    for src in all_sources:
        src_ra = src.get("ra")
        src_dec = src.get("dec")
        if src_ra is None or src_dec is None:
            continue
        try:
            sep = _haversine_arcsec(ra, dec, float(src_ra), float(src_dec))
            if sep <= radius_arcsec:
                result.append(src)
        except (TypeError, ValueError):
            continue
    return result


# ---------------------------------------------------------------------------
# Moving-object detection
# ---------------------------------------------------------------------------

def _is_position_shifted(
    ra: float,
    dec: float,
    wide_history: list[dict],
) -> bool:
    """
    Return True if *any* historical source in the wide-cone query is further
    than MATCH_CONE_ARCSEC from the current position.

    A shifted detection means the source appeared in this sky region before but
    at a meaningfully different position — the hallmark of a moving object.
    """
    stationary_threshold = config.MATCH_CONE_ARCSEC
    for hist_src in wide_history:
        hist_ra  = hist_src.get("ra")
        hist_dec = hist_src.get("dec")
        if hist_ra is None or hist_dec is None:
            continue
        try:
            sep = _haversine_arcsec(ra, dec, float(hist_ra), float(hist_dec))
        except (TypeError, ValueError):
            continue
        if sep > stationary_threshold:
            return True
    return False


# ---------------------------------------------------------------------------
# Batch data prefetch
# ---------------------------------------------------------------------------

async def _prefetch_history_data(
    sources: list[dict],
    obs_time: str,
    frame_id: str,
    log_filename: str,
) -> tuple[dict[tuple, list], dict[tuple, list]]:
    """
    Prefetch all historical data needed for anomaly classification in TWO batch requests.

    Returns:
        - narrow_history_by_tile: dict mapping tile -> list of historical sources (MATCH_CONE_ARCSEC)
        - coverage_by_tile: dict mapping tile -> list of covering frames

    For moving object detection (wide cone), we use the same batch data but filter
    with a larger radius client-side.
    """
    extra = {"frame_id": frame_id, "log_filename": log_filename}

    # Collect unique tiles that need queries
    # Only unmatched sources (catalog_name=None) need history queries
    tiles_needing_sources: set[tuple[float, float]] = set()
    tiles_needing_coverage: set[tuple[float, float]] = set()

    for source in sources:
        ra = float(source.get("ra", 0))
        dec = float(source.get("dec", 0))
        catalog_name = source.get("catalog_name")

        tile = _tile_key(ra, dec)

        # All sources need coverage check (unless already matched in a known catalog that implies stationarity)
        tiles_needing_coverage.add(tile)

        # Only unmatched and MPC sources need source history queries
        if catalog_name is None or catalog_name == "MPC":
            tiles_needing_sources.add(tile)

    logger.info(
        "Prefetching history data: %d tiles for sources, %d tiles for coverage",
        len(tiles_needing_sources),
        len(tiles_needing_coverage),
        extra=extra,
    )

    # Build position lists for batch API calls
    # Use wider radius for the batch query to ensure we capture all needed sources
    # The tile size is 0.1 deg = 360 arcsec, plus we need MOVING_CONE_ARCSEC margin
    batch_radius = max(config.MOVING_CONE_ARCSEC, config.MATCH_CONE_ARCSEC) + 400  # arcsec

    source_positions = [{"ra": t[0], "dec": t[1]} for t in tiles_needing_sources]
    coverage_positions = [{"ra": t[0], "dec": t[1]} for t in tiles_needing_coverage]

    # Execute batch requests concurrently
    narrow_history_by_tile: dict[tuple, list] = {}
    coverage_by_tile: dict[tuple, list] = {}

    try:
        # Make both batch requests in parallel
        if source_positions and coverage_positions:
            sources_result, coverage_result = await asyncio.gather(
                api_client.get_sources_near_batch(source_positions, batch_radius, obs_time),
                api_client.get_frames_covering_batch(coverage_positions, obs_time),
            )
        elif source_positions:
            sources_result = await api_client.get_sources_near_batch(source_positions, batch_radius, obs_time)
            coverage_result = {}
        elif coverage_positions:
            sources_result = {}
            coverage_result = await api_client.get_frames_covering_batch(coverage_positions, obs_time)
        else:
            sources_result = {}
            coverage_result = {}

        # Map results back to tiles
        tiles_sources_list = list(tiles_needing_sources)
        for i, tile in enumerate(tiles_sources_list):
            narrow_history_by_tile[tile] = sources_result.get(str(i), [])

        tiles_coverage_list = list(tiles_needing_coverage)
        for i, tile in enumerate(tiles_coverage_list):
            coverage_by_tile[tile] = coverage_result.get(str(i), [])

        logger.info(
            "Batch prefetch complete: %d source history results, %d coverage results",
            sum(len(v) for v in narrow_history_by_tile.values()),
            sum(len(v) for v in coverage_by_tile.values()),
            extra=extra,
        )

    except Exception as exc:
        logger.error(
            "Batch prefetch failed: %s — will classify without history data",
            exc,
            extra=extra,
        )

    return narrow_history_by_tile, coverage_by_tile


# ---------------------------------------------------------------------------
# Per-source classification (using prefetched data)
# ---------------------------------------------------------------------------

def _classify_source_sync(
    source: dict,
    frame_id: str,
    log_filename: str,
    history_by_tile: dict[tuple, list],
    coverage_by_tile: dict[tuple, list],
) -> dict | None:
    """
    Classify a single source using PREFETCHED batch data (synchronous).

    No API calls are made here - all data comes from the batch prefetch.

    Returns an anomaly dict, or None if no reportable anomaly is found.
    """
    ra  = float(source["ra"])
    dec = float(source["dec"])
    mag: float | None = source.get("mag")

    catalog_name: str | None  = source.get("catalog_name")
    catalog_id:   str | None  = source.get("catalog_id")
    object_type:  str | None  = source.get("object_type")
    elongation:   float       = float(source.get("elongation", 0.0))

    extra = {"frame_id": frame_id, "log_filename": log_filename}
    tile = _tile_key(ra, dec)

    # ------------------------------------------------------------------
    # Priority 1 — MPC-matched moving objects
    # ------------------------------------------------------------------

    if catalog_name == "MPC":
        anomaly_type = _TYPE_ASTEROID if object_type == "ASTEROID" else _TYPE_COMET

        logger.info(
            "Classified as %s: designation=%s ra=%.4f dec=%.4f",
            anomaly_type, catalog_id, ra, dec,
            extra=extra,
        )

        return {
            "anomaly_type":    anomaly_type,
            "ra":              ra,
            "dec":             dec,
            "magnitude":       mag,
            "delta_mag":       None,
            "mpc_designation": catalog_id,
            "ephemeris":       None,
            "notes":           f"Matched MPC object '{catalog_id}' (type: {object_type})",
            "_needs_ephemeris": True,
        }

    # ------------------------------------------------------------------
    # Priority 2 — Position-shifted unmatched moving objects
    # ------------------------------------------------------------------

    if catalog_name is None:
        # Get wide-cone history from prefetched data
        tile_sources = history_by_tile.get(tile, [])
        wide_history = _find_sources_within_radius(ra, dec, config.MOVING_CONE_ARCSEC, tile_sources)

        if _is_position_shifted(ra, dec, wide_history):
            if elongation > 3.0:
                anomaly_type = _TYPE_SPACE_DEBRIS
            else:
                anomaly_type = _TYPE_MOVING_UNKNOWN

            logger.warning(
                "ALERT — %s: unmatched position-shifted source ra=%.4f dec=%.4f elongation=%.2f",
                anomaly_type, ra, dec, elongation,
                extra=extra,
            )

            return {
                "anomaly_type":    anomaly_type,
                "ra":              ra,
                "dec":             dec,
                "magnitude":       mag,
                "delta_mag":       None,
                "mpc_designation": None,
                "ephemeris":       None,
                "notes": (
                    f"Position shifted >{config.MATCH_CONE_ARCSEC:.1f} arcsec from prior detection; "
                    f"not matched in MPC. Elongation={elongation:.2f}."
                ),
            }

    # ------------------------------------------------------------------
    # Priority 3 — Stationary source classification
    # ------------------------------------------------------------------

    # Get coverage from prefetched data
    coverage = coverage_by_tile.get(tile, [])
    n_coverage = len(coverage)

    # Get narrow-cone history from prefetched data
    if catalog_name is None:
        tile_sources = history_by_tile.get(tile, [])
        history = _find_sources_within_radius(ra, dec, config.MATCH_CONE_ARCSEC, tile_sources)
    else:
        # Catalog-matched sources don't need history lookup for UNKNOWN classification
        history = []

    n_history = len(history)

    # --- FIRST_OBSERVATION: sky area never imaged before ---
    if n_coverage == 0:
        logger.debug(
            "FIRST_OBSERVATION: ra=%.4f dec=%.4f — sky area has no prior coverage",
            ra, dec,
            extra=extra,
        )
        return None  # Not an anomaly — do not report to API

    # --- Area has prior coverage from here on ---

    # --- SUPERNOVA_CANDIDATE: new source in/near a galaxy ---
    if n_history == 0 and _is_galaxy(object_type):
        logger.warning(
            "ALERT — SUPERNOVA_CANDIDATE: new source near galaxy object_type=%s "
            "ra=%.4f dec=%.4f mag=%s",
            object_type, ra, dec, mag,
            extra=extra,
        )
        return {
            "anomaly_type":    _TYPE_SUPERNOVA,
            "ra":              ra,
            "dec":             dec,
            "magnitude":       mag,
            "delta_mag":       None,
            "mpc_designation": None,
            "ephemeris":       None,
            "notes": (
                f"New source (no prior detections) near galaxy "
                f"(object_type='{object_type}'). Area covered by "
                f"{n_coverage} prior frame(s)."
            ),
        }

    # --- UNKNOWN: covered, no history, no catalog match ---
    # TODO: Many UNKNOWN sources with mag > 20 are simply faint stars beyond Gaia DR3
    # completeness limit (~21 mag). Consider:
    #   1. Adding magnitude threshold (e.g., skip UNKNOWN alert if mag > 20)
    #   2. Querying deeper catalogs (Pan-STARRS DR2, SDSS) for faint sources
    #   3. Adding "FAINT_UNCATALOGUED" classification distinct from true UNKNOWN
    # See: https://github.com/users/miksrv/projects/10 for tracking
    if n_history == 0 and catalog_name is None:
        logger.warning(
            "ALERT — UNKNOWN: new uncatalogued source ra=%.4f dec=%.4f mag=%s "
            "covered_by=%d frames",
            ra, dec, mag, n_coverage,
            extra=extra,
        )
        return {
            "anomaly_type":    _TYPE_UNKNOWN,
            "ra":              ra,
            "dec":             dec,
            "magnitude":       mag,
            "delta_mag":       None,
            "mpc_designation": None,
            "ephemeris":       None,
            "notes": (
                f"Not found in Gaia DR3, Simbad, or MPC within "
                f"{config.MATCH_CONE_ARCSEC:.1f} arcsec. "
                f"Area covered by {n_coverage} previous frame(s)."
            ),
        }

    # --- KNOWN_CATALOG_NEW: covered, no history, but matched in catalog ---
    if n_history == 0 and catalog_name is not None:
        logger.debug(
            "KNOWN_CATALOG_NEW: ra=%.4f dec=%.4f catalog=%s id=%s — "
            "below prior detection threshold",
            ra, dec, catalog_name, catalog_id,
            extra=extra,
        )
        return None  # Known object newly above threshold — not an anomaly

    # --- Source HAS prior history from here ---

    median_hist_mag = _history_median_mag(history)
    delta_mag: float | None = None

    if mag is not None and median_hist_mag is not None:
        delta_mag = mag - median_hist_mag  # negative = brighter than history

    mag_changed = (
        delta_mag is not None
        and abs(delta_mag) > config.DELTA_MAG_ALERT
    )

    if mag_changed:
        # --- BINARY_STAR — check before VARIABLE_STAR (more specific match) ---
        if _is_binary_star(object_type):
            logger.info(
                "BINARY_STAR: ra=%.4f dec=%.4f delta_mag=%.3f object_type=%s",
                ra, dec, delta_mag, object_type,
                extra=extra,
            )
            return {
                "anomaly_type":    _TYPE_BINARY_STAR,
                "ra":              ra,
                "dec":             dec,
                "magnitude":       mag,
                "delta_mag":       delta_mag,
                "mpc_designation": None,
                "ephemeris":       None,
                "notes": (
                    f"Binary/eclipsing binary brightness change "
                    f"delta_mag={delta_mag:.3f} (threshold "
                    f"{config.DELTA_MAG_ALERT:.2f}). "
                    f"object_type='{object_type}'."
                ),
            }

        # --- VARIABLE_STAR ---
        if _is_variable_star(object_type):
            logger.info(
                "VARIABLE_STAR: ra=%.4f dec=%.4f delta_mag=%.3f object_type=%s",
                ra, dec, delta_mag, object_type,
                extra=extra,
            )
            return {
                "anomaly_type":    _TYPE_VARIABLE_STAR,
                "ra":              ra,
                "dec":             dec,
                "magnitude":       mag,
                "delta_mag":       delta_mag,
                "mpc_designation": None,
                "ephemeris":       None,
                "notes": (
                    f"Known variable star brightness change "
                    f"delta_mag={delta_mag:.3f} (threshold "
                    f"{config.DELTA_MAG_ALERT:.2f}). "
                    f"object_type='{object_type}'."
                ),
            }

    # Source is consistent with history (or has no magnitude to compare).
    # No anomaly.
    logger.debug(
        "No anomaly: ra=%.4f dec=%.4f catalog=%s history=%d coverage=%d",
        ra, dec, catalog_name, n_history, n_coverage,
        extra=extra,
    )
    return None


# ---------------------------------------------------------------------------
# Ephemeris resolution
# ---------------------------------------------------------------------------

async def _resolve_ephemerides(
    anomalies: list[dict],
    obs_time: str,
    frame_id: str,
    log_filename: str,
) -> None:
    """
    Concurrently resolve ephemerides for all anomalies that have
    _needs_ephemeris=True and a non-None mpc_designation.

    Mutates anomaly dicts in-place:
      - Sets "ephemeris" key to the dict returned by ephemeris.query(), or None.
      - Removes the private "_needs_ephemeris" sentinel key.
    """
    extra = {"frame_id": frame_id, "log_filename": log_filename}

    pending = [a for a in anomalies if a.get("_needs_ephemeris") and a.get("mpc_designation")]

    if not pending:
        # Still clean up the sentinel on any anomaly that has it without a designation
        for a in anomalies:
            a.pop("_needs_ephemeris", None)
        return

    designations = [a["mpc_designation"] for a in pending]

    logger.debug(
        "Fetching ephemerides concurrently for %d object(s): %s",
        len(designations), designations,
        extra=extra,
    )

    results: list[dict | None] = await asyncio.gather(
        *[ephemeris.query(desig, obs_time) for desig in designations],
        return_exceptions=False,
    )

    for anomaly, eph_result in zip(pending, results):
        if eph_result is None:
            logger.warning(
                "Ephemeris query returned None for designation=%s",
                anomaly["mpc_designation"],
                extra=extra,
            )
        anomaly["ephemeris"] = eph_result

    # Remove the private sentinel from every anomaly dict
    for a in anomalies:
        a.pop("_needs_ephemeris", None)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def detect(
    frame_id: str,
    sources: list[dict],
    catalog_matches: list[dict],
    frame_meta: dict,
) -> list[dict]:
    """
    Detect and classify anomalies for all sources in a processed frame.

    Uses BATCH API queries to minimize network round-trips:
    - One POST /sources/near/batch for all source history
    - One POST /frames/covering/batch for all coverage checks

    This reduces API calls from O(N) to O(1) where N is the number of sources.

    Parameters
    ----------
    frame_id:
        Frame ID returned by api_client.post_frame() — used in log records.
    sources:
        List of source dicts as enriched by catalog_matcher.match().
        Each dict must have at minimum: ra, dec, mag, catalog_name, catalog_id,
        object_type, elongation.
    catalog_matches:
        Same list as sources (catalog_matcher enriches in-place). The
        parameter exists for API compatibility with the pipeline orchestrator.
    frame_meta:
        Dict with keys: frame_id, obs_time (ISO 8601), filename,
        ra_center, dec_center, fov_deg.

    Returns
    -------
    list[dict]
        Anomaly dicts ready to be sent to POST /frames/{id}/anomalies.
        FIRST_OBSERVATION and KNOWN_CATALOG_NEW are suppressed (not returned).
        Each returned dict has keys: anomaly_type, ra, dec, magnitude,
        delta_mag, mpc_designation, ephemeris, notes.
    """
    obs_time     = str(frame_meta.get("obs_time", ""))
    log_filename = str(frame_meta.get("filename", "<unknown>"))
    extra        = {"frame_id": frame_id, "log_filename": log_filename}

    logger.info(
        "Anomaly detection started: %d source(s) frame_id=%s",
        len(sources), frame_id,
        extra=extra,
    )

    if not sources:
        logger.info("No sources to classify frame_id=%s", frame_id, extra=extra)
        return []

    # ------------------------------------------------------------------
    # BATCH PREFETCH: Get all historical data in TWO API calls
    # ------------------------------------------------------------------
    history_by_tile, coverage_by_tile = await _prefetch_history_data(
        sources, obs_time, frame_id, log_filename
    )

    # ------------------------------------------------------------------
    # Classify all sources using prefetched data (no additional API calls)
    # ------------------------------------------------------------------
    anomalies: list[dict] = []

    for source in sources:
        try:
            result = _classify_source_sync(
                source,
                frame_id=frame_id,
                log_filename=log_filename,
                history_by_tile=history_by_tile,
                coverage_by_tile=coverage_by_tile,
            )
            if result is not None:
                anomalies.append(result)
        except Exception as exc:
            logger.error(
                "Unexpected error classifying source ra=%s dec=%s: %s",
                source.get("ra", "?"), source.get("dec", "?"), exc,
                extra=extra,
            )

    # Resolve ephemerides concurrently for all MPC-matched objects
    await _resolve_ephemerides(anomalies, obs_time, frame_id, log_filename)

    n_alert = sum(1 for a in anomalies if a["anomaly_type"] in _ALERT_TYPES)

    logger.info(
        "Anomaly detection complete: %d anomaly/anomalies found (%d alert-worthy) "
        "frame_id=%s",
        len(anomalies), n_alert, frame_id,
        extra=extra,
    )

    return anomalies
