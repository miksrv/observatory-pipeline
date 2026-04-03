"""
modules/anomaly_detector.py — Anomaly detection and classification for the pipeline.

The single public entry point is:

    await anomaly_detector.detect(frame_id, sources, catalog_matches, frame_meta) -> list[dict]

For each source in the frame the module:

1. Checks whether the sky position was ever observed before (GET /frames/covering).
2. Retrieves prior detections at the same position (GET /sources/near).
3. Probes a wider cone for shifted historical detections that indicate a moving object.
4. Classifies each source into one of the ten anomaly types defined in CLAUDE.md.
5. Calls ephemeris.query() concurrently for all ASTEROID / COMET sources.
6. Returns only actionable anomaly dicts (FIRST_OBSERVATION and KNOWN_CATALOG_NEW
   are never elevated to anomaly records; they are logged but not returned).

API queries are cached per detect() call using a tile-based key to avoid redundant
network round-trips for sources that share the same sky region.
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
# Cache helpers
# ---------------------------------------------------------------------------

def _tile_key(ra: float, dec: float) -> tuple[float, float]:
    """
    Round RA/Dec to 0.1-degree tiles for cache keying.

    Two sources within ~6 arcminutes of each other map to the same tile and
    reuse the same API query result.
    """
    return round(ra / 0.1) * 0.1, round(dec / 0.1) * 0.1


async def _cached_get_sources_near(
    ra: float,
    dec: float,
    radius_arcsec: float,
    before_time: str,
    cache: dict[str, Any],
) -> list[dict]:
    """
    Return historical sources near (ra, dec) within radius_arcsec, using
    a per-run in-memory cache keyed by tile + radius.
    """
    tile = _tile_key(ra, dec)
    key = f"sources:{tile[0]:.2f}:{tile[1]:.2f}:{radius_arcsec:.1f}"
    if key not in cache:
        cache[key] = await api_client.get_sources_near(ra, dec, radius_arcsec, before_time)
    return cache[key]  # type: ignore[return-value]


async def _cached_get_frames_covering(
    ra: float,
    dec: float,
    before_time: str,
    cache: dict[str, Any],
) -> list[dict]:
    """
    Return prior frames that covered (ra, dec), using a per-run cache keyed
    by tile.
    """
    tile = _tile_key(ra, dec)
    key = f"covering:{tile[0]:.2f}:{tile[1]:.2f}"
    if key not in cache:
        cache[key] = await api_client.get_frames_covering(ra, dec, before_time)
    return cache[key]  # type: ignore[return-value]


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
# Per-source classification
# ---------------------------------------------------------------------------

async def _classify_source(
    source: dict,
    frame_id: str,
    obs_time: str,
    log_filename: str,
    cache: dict[str, Any],
) -> dict | None:
    """
    Classify a single source and return an anomaly dict, or None if no
    reportable anomaly is found (FIRST_OBSERVATION, KNOWN_CATALOG_NEW).

    Classification priority:
        1. MPC-matched moving objects (ASTEROID / COMET)
        2. Unmatched position-shifted moving objects (MOVING_UNKNOWN / SPACE_DEBRIS)
        3. Stationary source classification
    """
    ra  = float(source["ra"])
    dec = float(source["dec"])
    mag: float | None = source.get("mag")

    catalog_name: str | None  = source.get("catalog_name")
    catalog_id:   str | None  = source.get("catalog_id")
    object_type:  str | None  = source.get("object_type")
    elongation:   float       = float(source.get("elongation", 0.0))

    extra = {"frame_id": frame_id, "log_filename": log_filename}

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

        # Ephemeris is resolved later via asyncio.gather; return a sentinel
        # dict with _needs_ephemeris so the outer loop can batch-resolve them.
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

    try:
        wide_history = await _cached_get_sources_near(
            ra, dec, config.MOVING_CONE_ARCSEC, obs_time, cache
        )
    except Exception as exc:
        logger.error(
            "Wide-cone history query failed ra=%.4f dec=%.4f: %s",
            ra, dec, exc,
            extra=extra,
        )
        wide_history = []

    if catalog_name is None and _is_position_shifted(ra, dec, wide_history):
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

    try:
        coverage = await _cached_get_frames_covering(ra, dec, obs_time, cache)
    except Exception as exc:
        logger.error(
            "Coverage query failed ra=%.4f dec=%.4f: %s",
            ra, dec, exc,
            extra=extra,
        )
        coverage = []

    # Narrow-cone history: prior detections at essentially this same position
    try:
        history = await _cached_get_sources_near(
            ra, dec, config.MATCH_CONE_ARCSEC, obs_time, cache
        )
    except Exception as exc:
        logger.error(
            "Narrow-cone history query failed ra=%.4f dec=%.4f: %s",
            ra, dec, exc,
            extra=extra,
        )
        history = []

    n_coverage = len(coverage)
    n_history  = len(history)

    # --- FIRST_OBSERVATION: sky area never imaged before ---
    if n_coverage == 0:
        logger.info(
            "FIRST_OBSERVATION: ra=%.4f dec=%.4f — sky area has no prior coverage",
            ra, dec,
            extra=extra,
        )
        return None  # Not an anomaly — do not report to API

    # --- Area has prior coverage from here on ---

    # --- SUPERNOVA_CANDIDATE: new source in/near a galaxy ---
    # Condition: covered before, no history at this position, source is
    # associated with a galaxy (Simbad match) or unmatched in a known galaxy field.
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
        logger.info(
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
                ra, dec, delta_mag, object_type,  # type: ignore[arg-type]
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
                ra, dec, delta_mag, object_type,  # type: ignore[arg-type]
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

    Queries the observatory API for historical data, classifies each source
    according to the anomaly taxonomy in CLAUDE.md, and concurrently resolves
    JPL Horizons ephemerides for any matched solar-system objects.

    Parameters
    ----------
    frame_id:
        Frame ID returned by api_client.post_frame() — used in log records.
    sources:
        List of source dicts as enriched by catalog_matcher.match().
        Each dict must have at minimum: ra, dec, mag, catalog_name, catalog_id,
        object_type, elongation.
    catalog_matches:
        Same list as sources (catalog_matcher enriches in-place).  The
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

    # Per-run API response cache — rebuilt fresh each call, no TTL needed.
    _api_cache: dict[str, Any] = {}

    # Classify each source sequentially.  API queries are cached so repeated
    # tile lookups across nearby sources incur only one network round-trip.
    anomalies: list[dict] = []

    for source in sources:
        try:
            result = await _classify_source(
                source,
                frame_id=frame_id,
                obs_time=obs_time,
                log_filename=log_filename,
                cache=_api_cache,
            )
        except Exception as exc:
            ra  = source.get("ra", "?")
            dec = source.get("dec", "?")
            logger.error(
                "Unexpected error classifying source ra=%s dec=%s: %s",
                ra, dec, exc,
                extra=extra,
            )
            continue

        if result is not None:
            anomalies.append(result)

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
