"""
modules/catalog_matcher.py — Cross-match detected sources against external catalogs.

The single public entry point is:

    await catalog_matcher.match(sources: list[dict], frame_meta: dict) -> list[dict]

Each source dict is enriched in-place with four catalog fields:
    catalog_name  — "Gaia DR3", "Simbad", "MPC", or None
    catalog_id    — catalog's identifier string, or None
    catalog_mag   — Gaia G-band magnitude (float), or None for Simbad/MPC/unmatched
    object_type   — "STAR", Simbad OTYPE string, "ASTEROID", "COMET", or None

Catalogs are queried in order: Gaia DR3 → Simbad → MPC. Once a source is matched,
subsequent catalogs skip it. Catalog query results are cached in-process for 1 hour
to avoid redundant network calls across sources that share the same field.

All catalog errors are caught and logged; a failing catalog never crashes the pipeline.
"""

from __future__ import annotations

import datetime
import logging
import math
import warnings
from typing import Any

import astropy.units as u
import numpy as np
from astropy.coordinates import SkyCoord
from astroquery.gaia import Gaia
from astroquery.simbad import Simbad

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configure Gaia query limits
# Default ROW_LIMIT is 50, which is far too few for typical FITS frames
# with thousands of sources. Increase to 50000 to cover most use cases.
# ---------------------------------------------------------------------------
Gaia.ROW_LIMIT = 50000

# ---------------------------------------------------------------------------
# Module-level query cache — 1-hour TTL, keyed by catalog + sky region
# ---------------------------------------------------------------------------

_cache: dict[str, dict[str, Any]] = {}
_CACHE_TTL = datetime.timedelta(hours=1)


def _cache_get(key: str) -> Any | None:
    """Return cached data if present and within TTL, else None."""
    entry = _cache.get(key)
    if entry and (datetime.datetime.now(datetime.timezone.utc) - entry["fetched_at"]) < _CACHE_TTL:
        return entry["data"]
    return None


def _cache_set(key: str, data: Any) -> None:
    """Store data in the cache with the current timestamp."""
    _cache[key] = {"data": data, "fetched_at": datetime.datetime.now(datetime.timezone.utc)}


# ---------------------------------------------------------------------------
# Gaia DR3
# ---------------------------------------------------------------------------

def _query_gaia(ra_center: float, dec_center: float, fov_deg: float) -> list[dict]:
    """
    Query Gaia DR3 for all stars within fov_deg/2 of the frame centre.

    Returns a list of dicts with keys: ra, dec, source_id, phot_g_mean_mag.
    Returns [] on any error so the pipeline can continue with partial results.
    """
    cache_key = f"gaia:{ra_center:.3f}:{dec_center:.3f}:{fov_deg:.3f}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached  # type: ignore[return-value]

    try:
        coord = SkyCoord(ra=ra_center * u.deg, dec=dec_center * u.deg)
        radius = (fov_deg / 2.0) * u.deg
        job = Gaia.cone_search(coord, radius=radius)
        table = job.get_results()

        stars: list[dict] = []
        for row in table:
            mag = row["phot_g_mean_mag"]
            # Skip rows with masked or NaN magnitude — they can't be used for matching
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    mag_float = float(mag)
                if not math.isfinite(mag_float):
                    continue
            except (TypeError, ValueError):
                continue

            stars.append({
                "ra":             float(row["ra"]),
                "dec":            float(row["dec"]),
                "source_id":      str(row["source_id"]),
                "phot_g_mean_mag": mag_float,
            })

        _cache_set(cache_key, stars)
        logger.debug("Gaia DR3 query returned %d stars for ra=%.3f dec=%.3f", len(stars), ra_center, dec_center)
        return stars

    except Exception as exc:
        logger.warning("Gaia DR3 query failed for ra=%.3f dec=%.3f: %s", ra_center, dec_center, exc)
        return []


def _match_gaia(sources: list[dict], gaia_stars: list[dict]) -> None:
    """
    Mutate sources in-place: set catalog fields for any source within
    MATCH_CONE_ARCSEC of a Gaia DR3 star.

    This function includes automatic WCS offset correction:
    1. First pass with wider tolerance to find initial matches
    2. Compute median RA/Dec offset from matched pairs
    3. Apply correction and re-match with tight tolerance

    Gaia always runs first, so all sources have catalog_name=None at entry.
    """
    if not gaia_stars:
        return

    source_coords = SkyCoord(
        ra=[s["ra"] for s in sources] * u.deg,
        dec=[s["dec"] for s in sources] * u.deg,
    )
    gaia_coords = SkyCoord(
        ra=[g["ra"] for g in gaia_stars] * u.deg,
        dec=[g["dec"] for g in gaia_stars] * u.deg,
    )

    # First pass: match with wider tolerance to detect systematic offset
    idx, sep2d, _ = source_coords.match_to_catalog_sky(gaia_coords)
    sep_arcsec = sep2d.to(u.arcsec).value
    
    # Log initial match statistics
    within_5 = sum(1 for s in sep_arcsec if s <= 5.0)
    within_10 = sum(1 for s in sep_arcsec if s <= 10.0)
    within_30 = sum(1 for s in sep_arcsec if s <= 30.0)
    within_60 = sum(1 for s in sep_arcsec if s <= 60.0)
    min_sep = min(sep_arcsec) if len(sep_arcsec) > 0 else 0
    max_sep = max(sep_arcsec) if len(sep_arcsec) > 0 else 0
    median_sep = float(np.median(sep_arcsec)) if len(sep_arcsec) > 0 else 0
    
    logger.info(
        "Gaia match (initial): min=%.2f\" median=%.2f\" max=%.2f\"  "
        "within 5\"=%d, 10\"=%d, 30\"=%d, 60\"=%d (threshold=%.1f\")",
        min_sep, median_sep, max_sep, within_5, within_10, within_30, within_60, config.MATCH_CONE_ARCSEC,
    )

    # Check if we need WCS offset correction
    # Only attempt if median is large but we have some reasonably close matches
    if median_sep > 10.0 and within_60 >= 3:
        logger.info(
            "Gaia match: detected potential WCS offset (median=%.2f\"), attempting correction...",
            median_sep,
        )
        
        # Strategy: use MUTUAL matching to find reliable pairs
        # A pair is valid only if:
        # 1. Source A's nearest Gaia star is B
        # 2. Gaia star B's nearest source is A
        # This eliminates false pairings in crowded fields
        
        # Forward match: source → Gaia (already done)
        # Backward match: Gaia → source
        idx_reverse, sep2d_reverse, _ = gaia_coords.match_to_catalog_sky(source_coords)
        
        # Find mutual matches within tolerance
        offset_threshold = 60.0  # arcsec
        mutual_matches = []
        
        for i in range(len(sources)):
            if sep_arcsec[i] < offset_threshold:
                gaia_idx = idx[i]
                # Check if this Gaia star's nearest source is this source
                if idx_reverse[gaia_idx] == i:
                    mutual_matches.append((i, gaia_idx, sep_arcsec[i]))
        
        logger.info(
            "Gaia match: found %d mutual matches within %.0f\" (forward matches: %d)",
            len(mutual_matches), offset_threshold, within_60,
        )
        
        if len(mutual_matches) >= 3:
            # Compute offset in RA and Dec
            delta_ra_list = []
            delta_dec_list = []
            
            for src_i, gaia_i, _ in mutual_matches:
                gaia_match = gaia_stars[gaia_i]
                source = sources[src_i]
                # Offset = Gaia - Source (we need to add this to source coords)
                # Note: RA offset needs cos(dec) correction for proper arcsec conversion
                delta_ra = gaia_match["ra"] - source["ra"]
                delta_dec = gaia_match["dec"] - source["dec"]
                delta_ra_list.append(delta_ra)
                delta_dec_list.append(delta_dec)
            
            # Median offset (robust to outliers)
            offset_ra = float(np.median(delta_ra_list))
            offset_dec = float(np.median(delta_dec_list))
            
            # Convert to arcsec for logging (with cos(dec) correction for RA)
            mean_dec = float(np.mean([sources[m[0]]["dec"] for m in mutual_matches]))
            cos_dec = np.cos(np.radians(mean_dec))
            offset_ra_arcsec = offset_ra * 3600.0 * cos_dec
            offset_dec_arcsec = offset_dec * 3600.0
            total_offset_arcsec = np.sqrt(offset_ra_arcsec**2 + offset_dec_arcsec**2)
            
            logger.info(
                "Gaia match: computed WCS offset: dRA=%.2f\" dDec=%.2f\" (total=%.2f\", from %d mutual matches)",
                offset_ra_arcsec, offset_dec_arcsec, total_offset_arcsec, len(mutual_matches),
            )
            
            # Only apply correction if offset is significant (> 2 arcsec)
            if total_offset_arcsec > 2.0:
                # Apply correction and re-match
                corrected_coords = SkyCoord(
                    ra=[(s["ra"] + offset_ra) for s in sources] * u.deg,
                    dec=[(s["dec"] + offset_dec) for s in sources] * u.deg,
                )
                
                idx, sep2d, _ = corrected_coords.match_to_catalog_sky(gaia_coords)
                sep_arcsec = sep2d.to(u.arcsec).value
                
                # Log corrected statistics
                within_5_new = sum(1 for s in sep_arcsec if s <= 5.0)
                within_10_new = sum(1 for s in sep_arcsec if s <= 10.0)
                min_sep_new = min(sep_arcsec) if len(sep_arcsec) > 0 else 0
                median_sep_new = float(np.median(sep_arcsec)) if len(sep_arcsec) > 0 else 0
                
                logger.info(
                    "Gaia match (after offset): min=%.2f\" median=%.2f\"  within 5\"=%d, 10\"=%d",
                    min_sep_new, median_sep_new, within_5_new, within_10_new,
                )
                
                # Check if correction helped
                if median_sep_new < median_sep * 0.5:
                    logger.info("WCS offset correction successful (median reduced by >50%%)")
                    # Store offset in sources for downstream use
                    for source in sources:
                        source["_wcs_offset_ra"] = offset_ra
                        source["_wcs_offset_dec"] = offset_dec
                else:
                    # Offset correction didn't help - investigate plate scale issue
                    # Compare distances from center for matched pairs
                    logger.warning(
                        "WCS offset correction did NOT help significantly (median %.2f\" → %.2f\"). "
                        "Investigating plate scale...",
                        median_sep, median_sep_new,
                    )
                    
                    # Check plate scale by comparing angular distances
                    # Take pairs that are far from frame center and compare their separations
                    if len(mutual_matches) >= 10:
                        src_ras = np.array([sources[m[0]]["ra"] for m in mutual_matches])
                        src_decs = np.array([sources[m[0]]["dec"] for m in mutual_matches])
                        gaia_ras = np.array([gaia_stars[m[1]]["ra"] for m in mutual_matches])
                        gaia_decs = np.array([gaia_stars[m[1]]["dec"] for m in mutual_matches])
                        
                        # Compute center
                        src_center_ra = np.median(src_ras)
                        src_center_dec = np.median(src_decs)
                        
                        # Compute distances from center
                        cos_dec = np.cos(np.radians(src_center_dec))
                        src_dist = np.sqrt(((src_ras - src_center_ra) * cos_dec)**2 + 
                                          (src_decs - src_center_dec)**2) * 3600  # arcsec
                        gaia_dist = np.sqrt(((gaia_ras - src_center_ra) * cos_dec)**2 + 
                                           (gaia_decs - src_center_dec)**2) * 3600  # arcsec
                        
                        # Compute scale ratio for stars not at center
                        far_mask = src_dist > 100  # at least 100 arcsec from center
                        if np.sum(far_mask) >= 5:
                            scale_ratios = gaia_dist[far_mask] / src_dist[far_mask]
                            scale_ratio = float(np.median(scale_ratios))
                            scale_ratio_std = float(np.std(scale_ratios))
                            
                            if abs(scale_ratio - 1.0) > 0.01:  # more than 1% scale error
                                logger.warning(
                                    "Plate scale error detected: Gaia/Source distance ratio = %.4f "
                                    "(std=%.4f). WCS scale is off by %.2f%%",
                                    scale_ratio, scale_ratio_std, (scale_ratio - 1.0) * 100,
                                )
                            else:
                                logger.warning(
                                    "Plate scale appears OK (ratio=%.4f), "
                                    "issue may be rotation or non-linear distortion",
                                    scale_ratio,
                                )
                    
                    # Revert to original matching
                    idx, sep2d, _ = source_coords.match_to_catalog_sky(gaia_coords)
                    sep_arcsec = sep2d.to(u.arcsec).value
            else:
                logger.debug("WCS offset too small (%.2f\"), skipping correction", total_offset_arcsec)

    # Final matching with configured threshold
    threshold = config.MATCH_CONE_ARCSEC * u.arcsec
    matched_count = 0
    for i, source in enumerate(sources):
        if sep2d[i] < threshold:
            matched = gaia_stars[idx[i]]
            source["catalog_name"] = "Gaia DR3"
            source["catalog_id"]   = matched["source_id"]
            source["catalog_mag"]  = matched["phot_g_mean_mag"]
            source["object_type"]  = "STAR"
            matched_count += 1


# ---------------------------------------------------------------------------
# Simbad
# ---------------------------------------------------------------------------

def _query_simbad(ra_center: float, dec_center: float, fov_deg: float) -> list[dict]:
    """
    Query Simbad for all named objects within fov_deg/2 of the frame centre.

    Returns a list of dicts with keys: ra, dec, main_id, otype.
    Returns [] on any error or when Simbad returns None.
    """
    cache_key = f"simbad:{ra_center:.3f}:{dec_center:.3f}:{fov_deg:.3f}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached  # type: ignore[return-value]

    try:
        simbad = Simbad()
        simbad.add_votable_fields("otype")

        coord = SkyCoord(ra=ra_center * u.deg, dec=dec_center * u.deg)
        radius = (fov_deg / 2.0) * u.deg
        result = simbad.query_region(coord, radius=radius)

        if result is None:
            _cache_set(cache_key, [])
            return []

        # Column names vary across astroquery versions; normalise to upper-case
        colnames_upper = {c.upper(): c for c in result.colnames}

        ra_col    = colnames_upper.get("RA",      "RA")
        dec_col   = colnames_upper.get("DEC",     "DEC")
        id_col    = colnames_upper.get("MAIN_ID", "MAIN_ID")
        otype_col = colnames_upper.get("OTYPE",   "OTYPE")

        objects: list[dict] = []
        for row in result:
            try:
                # Simbad returns RA/Dec as space-separated sexagesimal strings
                # when using the default ICRS frame.  We parse via SkyCoord.
                sky = SkyCoord(
                    ra=str(row[ra_col]),
                    dec=str(row[dec_col]),
                    unit=(u.hourangle, u.deg),
                )
                objects.append({
                    "ra":      float(sky.ra.deg),
                    "dec":     float(sky.dec.deg),
                    "main_id": str(row[id_col]),
                    "otype":   str(row[otype_col]),
                })
            except Exception as row_exc:
                logger.debug("Skipping malformed Simbad row: %s", row_exc)
                continue

        _cache_set(cache_key, objects)
        logger.debug(
            "Simbad query returned %d objects for ra=%.3f dec=%.3f",
            len(objects), ra_center, dec_center,
        )
        return objects

    except Exception as exc:
        logger.warning("Simbad query failed for ra=%.3f dec=%.3f: %s", ra_center, dec_center, exc)
        return []


def _match_simbad(sources: list[dict], simbad_objects: list[dict]) -> None:
    """
    Mutate sources in-place: set catalog fields for unmatched sources within
    MATCH_CONE_ARCSEC of a Simbad object.

    Skips sources that already have catalog_name set (e.g. from Gaia).
    """
    unmatched = [s for s in sources if s["catalog_name"] is None]
    if not unmatched or not simbad_objects:
        return

    unmatched_coords = SkyCoord(
        ra=[s["ra"] for s in unmatched] * u.deg,
        dec=[s["dec"] for s in unmatched] * u.deg,
    )
    simbad_coords = SkyCoord(
        ra=[o["ra"] for o in simbad_objects] * u.deg,
        dec=[o["dec"] for o in simbad_objects] * u.deg,
    )

    idx, sep2d, _ = unmatched_coords.match_to_catalog_sky(simbad_coords)
    threshold = config.MATCH_CONE_ARCSEC * u.arcsec

    for i, source in enumerate(unmatched):
        if sep2d[i] < threshold:
            matched = simbad_objects[idx[i]]
            source["catalog_name"] = "Simbad"
            source["catalog_id"]   = matched["main_id"]
            source["catalog_mag"]  = None
            source["object_type"]  = matched["otype"]


# ---------------------------------------------------------------------------
# MPC / SkyBot (Minor Planet Center / IMCCE)
# ---------------------------------------------------------------------------

def _query_mpc(ra_center: float, dec_center: float, obs_time: str, fov_deg: float = 1.0) -> list[dict]:
    """
    Query for known asteroids and comets near the frame centre at observation time.

    Uses IMCCE SkyBot service which provides cone search for solar system objects
    at a specific epoch. Falls back gracefully on any error.

    Returns a list of dicts with keys: ra, dec, designation, object_type.
    """
    cache_key = f"mpc:{ra_center:.3f}:{dec_center:.3f}:{obs_time}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached  # type: ignore[return-value]

    try:
        from astroquery.imcce import Skybot
        from astropy.time import Time

        # Parse observation time
        if not obs_time:
            logger.debug("No obs_time provided for MPC/SkyBot query, skipping")
            _cache_set(cache_key, [])
            return []

        coord = SkyCoord(ra=ra_center * u.deg, dec=dec_center * u.deg)
        epoch = Time(obs_time)
        # SkyBot uses FOV in arcmin
        fov_arcmin = fov_deg * 60.0

        result = Skybot.cone_search(coord, rad=fov_arcmin * u.arcmin, epoch=epoch)

        if result is None or len(result) == 0:
            _cache_set(cache_key, [])
            return []

        objects: list[dict] = []
        for row in result:
            try:
                # SkyBot returns RA/Dec in degrees
                ra_val = float(row["RA"])
                dec_val = float(row["DEC"])
                name = str(row["Name"])
                obj_class = str(row.get("Class", "Asteroid"))

                # Determine object type based on class
                if "comet" in obj_class.lower():
                    obj_type = "COMET"
                else:
                    obj_type = "ASTEROID"

                objects.append({
                    "ra":          ra_val,
                    "dec":         dec_val,
                    "designation": name,
                    "object_type": obj_type,
                })
            except Exception as row_exc:
                logger.debug("Skipping malformed SkyBot row: %s", row_exc)
                continue

        _cache_set(cache_key, objects)
        logger.debug(
            "SkyBot query returned %d objects for ra=%.3f dec=%.3f at %s",
            len(objects), ra_center, dec_center, obs_time,
        )
        return objects

    except ImportError:
        logger.warning("astroquery.imcce.Skybot not available, skipping MPC matching")
        _cache_set(cache_key, [])
        return []
    except Exception as exc:
        logger.warning("SkyBot query failed for ra=%.3f dec=%.3f: %s", ra_center, dec_center, exc)
        return []


def _match_mpc(sources: list[dict], mpc_objects: list[dict]) -> None:
    """
    Mutate sources in-place: set catalog fields for unmatched sources within
    MOVING_CONE_ARCSEC of a known MPC object.

    Uses a wider cone than Gaia/Simbad matching to account for object motion
    between the MPC ephemeris epoch and the actual observation time.
    Skips sources that already have catalog_name set.
    """
    unmatched = [s for s in sources if s["catalog_name"] is None]
    if not unmatched or not mpc_objects:
        return

    unmatched_coords = SkyCoord(
        ra=[s["ra"] for s in unmatched] * u.deg,
        dec=[s["dec"] for s in unmatched] * u.deg,
    )
    mpc_coords = SkyCoord(
        ra=[o["ra"] for o in mpc_objects] * u.deg,
        dec=[o["dec"] for o in mpc_objects] * u.deg,
    )

    idx, sep2d, _ = unmatched_coords.match_to_catalog_sky(mpc_coords)
    threshold = config.MOVING_CONE_ARCSEC * u.arcsec

    for i, source in enumerate(unmatched):
        if sep2d[i] < threshold:
            matched = mpc_objects[idx[i]]
            source["catalog_name"] = "MPC"
            source["catalog_id"]   = matched["designation"]
            source["catalog_mag"]  = None
            source["object_type"]  = matched["object_type"]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def match(sources: list[dict], frame_meta: dict) -> list[dict]:
    """
    Enrich each source in-place with catalog identification fields.

    Queries Gaia DR3, Simbad, and MPC in order. Each catalog stage is
    isolated; a failure in one does not prevent the others from running.
    Query results are cached for 1 hour to avoid redundant network calls.

    Parameters
    ----------
    sources:
        List of source dicts as returned by astrometry.solve().
        Each dict must have at minimum: ra (float), dec (float).
    frame_meta:
        Dict with keys: ra_center, dec_center, fov_deg, obs_time (ISO 8601).

    Returns
    -------
    The same list (mutated in-place), with four new keys on every element:
        catalog_name  str | None
        catalog_id    str | None
        catalog_mag   float | None
        object_type   str | None
    """
    fits_filename = frame_meta.get("filename", "<unknown>")

    # Initialise catalog fields on all sources
    for source in sources:
        source.setdefault("catalog_name", None)
        source.setdefault("catalog_id",   None)
        source.setdefault("catalog_mag",  None)
        source.setdefault("object_type",  None)

    if not sources:
        logger.info("Catalog matching: 0 sources — nothing to match  fits_filename=%s", fits_filename)
        return sources

    ra_center  = float(frame_meta.get("ra_center",  0.0))
    dec_center = float(frame_meta.get("dec_center", 0.0))
    fov_deg    = float(frame_meta.get("fov_deg",    1.0))
    obs_time   = str(frame_meta.get("obs_time",    ""))

    # --- Gaia DR3 ---
    gaia_stars: list[dict] = []
    try:
        gaia_stars = _query_gaia(ra_center, dec_center, fov_deg)
        logger.info(
            "Gaia query: ra=%.4f dec=%.4f fov=%.4f° radius=%.4f° → %d catalog stars  fits_filename=%s",
            ra_center, dec_center, fov_deg, fov_deg / 2.0, len(gaia_stars), fits_filename,
        )
        _match_gaia(sources, gaia_stars)
    except Exception as exc:
        logger.warning("Gaia matching stage failed for fits_filename=%s: %s", fits_filename, exc)

    # --- Simbad ---
    simbad_objects: list[dict] = []
    try:
        simbad_objects = _query_simbad(ra_center, dec_center, fov_deg)
        _match_simbad(sources, simbad_objects)
    except Exception as exc:
        logger.warning("Simbad matching stage failed for fits_filename=%s: %s", fits_filename, exc)

    # --- MPC / SkyBot ---
    mpc_objects: list[dict] = []
    try:
        mpc_objects = _query_mpc(ra_center, dec_center, obs_time, fov_deg)
        _match_mpc(sources, mpc_objects)
    except Exception as exc:
        logger.warning("MPC/SkyBot matching stage failed for fits_filename=%s: %s", fits_filename, exc)

    n_gaia    = sum(1 for s in sources if s["catalog_name"] == "Gaia DR3")
    n_simbad  = sum(1 for s in sources if s["catalog_name"] == "Simbad")
    n_mpc     = sum(1 for s in sources if s["catalog_name"] == "MPC")
    n_unmatched = sum(1 for s in sources if s["catalog_name"] is None)

    logger.info(
        "Catalog matching: %d sources — Gaia: %d, Simbad: %d, MPC: %d, unmatched: %d  fits_filename=%s",
        len(sources), n_gaia, n_simbad, n_mpc, n_unmatched, fits_filename,
    )

    return sources
