"""
modules/catalog_matcher.py — Cross-match detected sources against external catalogs.

The single public entry point is:

    await catalog_matcher.match(sources: list[dict], frame_meta: dict) -> list[dict]

Each source dict is enriched in-place with four catalog fields:
    catalog_name  — "Simbad", "Gaia DR3", "2MASS", "MPC", or None
    catalog_id    — catalog's identifier string, or None
    catalog_mag   — magnitude (float): G-band for Gaia, J-band for 2MASS, None otherwise
    object_type   — "STAR", Simbad OTYPE string, "ASTEROID", "COMET", or None

Catalogs are queried in order: Simbad → Gaia DR3 → 2MASS → MPC. Once a source is
matched, subsequent catalogs skip it. This order prioritises rich object-type
information from Simbad (variable stars, galaxies, named objects) over generic
Gaia stellar matching, and uses 2MASS as a fallback for red/cool stars that are
faint or absent in Gaia DR3.

Rationale for 2MASS as third catalog:
    - 2MASS (Two Micron All Sky Survey) covers ~470 million point sources to K≈14.3
    - Complements Gaia for late-type (M/K) stars that are bright in NIR but faint
      in the Gaia G band, and for heavily reddened regions near the Galactic plane
    - Accessed via VizieR catalog II/246; magnitudes stored as J-band (Jmag)

Rate limits of the online services (all free, no auth required):
    Simbad:    CDS infrastructure, ~5–6 req/sec recommended; 1-hr cache is sufficient
    Gaia DR3:  ESA TAP+, no hard limit; queries take 1–5 s; 1-hr cache is sufficient
    2MASS:     CDS/VizieR infrastructure, same limits as Simbad; cache is sufficient
    MPC/SkyBot: IMCCE, no hard limit; epoch-dependent so shorter natural TTL

Catalog query results are cached in-process for 1 hour to avoid redundant network
calls across sources that share the same field.

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
from astroquery.vizier import Vizier

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
        # Use sqrt(2)/2 × fov_deg to cover the full field diagonal.
        # fov_deg is the larger dimension; for any aspect ratio the half-diagonal
        # is at most fov_deg × sqrt(2)/2, so this radius covers all corners.
        radius = (fov_deg * math.sqrt(2) / 2.0) * u.deg
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


def _compute_wcs_offset(sources: list[dict], gaia_stars: list[dict]) -> tuple[float, float]:
    """
    Compute and return the systematic WCS offset (dRA, dDec) in degrees.

    Matches all detected sources against the Gaia catalog using a 2D vote
    accumulator on (dRA, dDec) vectors. True source→star pairs vote for the
    same offset bin and produce a sharp histogram peak; random/false matches
    are scattered uniformly and create only background noise.

    Returns (offset_ra_deg, offset_dec_deg). Returns (0.0, 0.0) when:
      - Not enough sources or Gaia stars to compute reliably
      - Median separation is already within tolerance (no correction needed)
      - No statistically significant peak found (field dominated by galaxies, etc.)

    The caller is responsible for applying the returned offset to source
    coordinates before any catalog matching.
    """
    if not gaia_stars or not sources:
        return 0.0, 0.0

    source_coords = SkyCoord(
        ra=[s["ra"] for s in sources] * u.deg,
        dec=[s["dec"] for s in sources] * u.deg,
    )
    gaia_coords = SkyCoord(
        ra=[g["ra"] for g in gaia_stars] * u.deg,
        dec=[g["dec"] for g in gaia_stars] * u.deg,
    )

    idx, sep2d, _ = source_coords.match_to_catalog_sky(gaia_coords)
    sep_arcsec = sep2d.to(u.arcsec).value

    within_5  = int(np.sum(sep_arcsec <= 5.0))
    within_10 = int(np.sum(sep_arcsec <= 10.0))
    within_30 = int(np.sum(sep_arcsec <= 30.0))
    within_60 = int(np.sum(sep_arcsec <= 60.0))
    min_sep    = float(np.min(sep_arcsec))   if len(sep_arcsec) > 0 else 0.0
    max_sep    = float(np.max(sep_arcsec))   if len(sep_arcsec) > 0 else 0.0
    median_sep = float(np.median(sep_arcsec)) if len(sep_arcsec) > 0 else 0.0

    logger.info(
        "Gaia match (raw): min=%.2f\" median=%.2f\" max=%.2f\"  "
        "within 5\"=%d, 10\"=%d, 30\"=%d, 60\"=%d (threshold=%.1f\")",
        min_sep, median_sep, max_sep, within_5, within_10, within_30, within_60,
        config.MATCH_CONE_ARCSEC,
    )

    if median_sep <= 10.0 or within_60 < 10:
        # Separation already acceptable or too few pairs — no correction needed
        return 0.0, 0.0

    # ------------------------------------------------------------------
    # 2D vote accumulator on (dRA, dDec) vectors
    #
    # For each source we have a (dRA, dDec) vector pointing from the source
    # to its nearest Gaia star. If a systematic WCS error exists, all true
    # source→star pairs vote for the same offset bin and produce a sharp peak.
    # Random/false matches are scattered uniformly and produce only background.
    #
    # This is robust regardless of the source/catalog density ratio — unlike
    # mutual nearest-neighbour matching, which breaks down when the catalog is
    # much denser than the source list (10k Gaia vs 421 sources = 24:1 ratio).
    # ------------------------------------------------------------------
    mean_dec = float(np.mean([sources[i]["dec"] for i in range(len(sources))
                              if sep_arcsec[i] <= 60.0]))
    cos_dec = math.cos(math.radians(mean_dec))

    dra_arcsec  = np.array([(gaia_stars[idx[i]]["ra"]  - sources[i]["ra"])  * cos_dec * 3600.0
                             for i in range(len(sources)) if sep_arcsec[i] <= 60.0])
    ddec_arcsec = np.array([(gaia_stars[idx[i]]["dec"] - sources[i]["dec"]) * 3600.0
                             for i in range(len(sources)) if sep_arcsec[i] <= 60.0])

    BIN_SIZE = 2.0
    RANGE    = 62.0
    n_bins   = int(2 * RANGE / BIN_SIZE)  # 62 bins per axis

    H, ra_edges, dec_edges = np.histogram2d(
        dra_arcsec, ddec_arcsec,
        bins=n_bins,
        range=[[-RANGE, RANGE], [-RANGE, RANGE]],
    )
    peak_i, peak_j = np.unravel_index(np.argmax(H), H.shape)
    peak_count     = int(H[peak_i, peak_j])
    # Require peak to hold at least 5% of matches and be ≥ 15 counts
    sig_threshold  = max(15, len(dra_arcsec) * 0.05)

    peak_dra_arcsec  = float((ra_edges[peak_i]  + ra_edges[peak_i + 1])  / 2.0)
    peak_ddec_arcsec = float((dec_edges[peak_j] + dec_edges[peak_j + 1]) / 2.0)
    total_offset     = math.sqrt(peak_dra_arcsec ** 2 + peak_ddec_arcsec ** 2)

    expected_bg = len(dra_arcsec) / float(n_bins ** 2)
    logger.info(
        "Gaia offset accumulator: %d pairs within 60\" — peak=%d (bg≈%.2f, threshold=%.0f) "
        "at dRA=%.1f\" dDec=%.1f\" (total=%.1f\")",
        len(dra_arcsec), peak_count, expected_bg, sig_threshold,
        peak_dra_arcsec, peak_ddec_arcsec, total_offset,
    )

    if peak_count < sig_threshold or total_offset <= 2.0:
        logger.debug(
            "No significant WCS offset detected (peak=%d < threshold=%.0f, offset=%.1f\"). "
            "High median separation may indicate a galaxy-rich field where most detections "
            "are extended sources not present in Gaia.",
            peak_count, sig_threshold, total_offset,
        )
        return 0.0, 0.0

    # Refine: median of all matches within 2 bins of the peak
    near_mask = (
        (np.abs(dra_arcsec  - peak_dra_arcsec)  <= BIN_SIZE * 2) &
        (np.abs(ddec_arcsec - peak_ddec_arcsec) <= BIN_SIZE * 2)
    )
    refined_dra  = float(np.median(dra_arcsec[near_mask]))  if near_mask.any() else peak_dra_arcsec
    refined_ddec = float(np.median(ddec_arcsec[near_mask])) if near_mask.any() else peak_ddec_arcsec

    offset_ra_deg  = refined_dra  / (cos_dec * 3600.0)
    offset_dec_deg = refined_ddec / 3600.0

    logger.info(
        "WCS offset detected: dRA=%.2f\" dDec=%.2f\" (total=%.2f\") — "
        "will be applied to all source coordinates before catalog matching",
        refined_dra, refined_ddec, math.sqrt(refined_dra ** 2 + refined_ddec ** 2),
    )
    return offset_ra_deg, offset_dec_deg


def _match_gaia(sources: list[dict], gaia_stars: list[dict]) -> None:
    """
    Mutate sources in-place: set catalog fields for unmatched sources within
    MATCH_CONE_ARCSEC of a Gaia DR3 star.

    Called after _compute_wcs_offset() has already been applied to source
    coordinates, so no offset correction is needed here — just matching.
    Only assigns catalog fields to sources not already matched by Simbad.
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

    idx, sep2d, _ = source_coords.match_to_catalog_sky(gaia_coords)
    sep_arcsec = sep2d.to(u.arcsec).value

    within_5  = int(np.sum(sep_arcsec <= 5.0))
    within_10 = int(np.sum(sep_arcsec <= 10.0))
    median_sep = float(np.median(sep_arcsec)) if len(sep_arcsec) > 0 else 0.0

    logger.info(
        "Gaia match (corrected): median=%.2f\" within 5\"=%d, 10\"=%d (threshold=%.1f\")",
        median_sep, within_5, within_10, config.MATCH_CONE_ARCSEC,
    )

    threshold = config.MATCH_CONE_ARCSEC * u.arcsec
    for i, source in enumerate(sources):
        if source["catalog_name"] is None and sep2d[i] < threshold:
            matched = gaia_stars[idx[i]]
            source["catalog_name"] = "Gaia DR3"
            source["catalog_id"]   = matched["source_id"]
            source["catalog_mag"]  = matched["phot_g_mean_mag"]
            source["object_type"]  = "STAR"


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
                ra_raw  = row[ra_col]
                dec_raw = row[dec_col]
                str_ra  = str(ra_raw).strip()
                str_dec = str(dec_raw).strip()

                # astroquery >= 0.4.7 (new SIMBAD TAP service) returns RA/Dec as
                # decimal degrees (float).  Older versions returned sexagesimal
                # strings in HMS/DMS format.  Detect the format and parse accordingly.
                try:
                    # If it parses as a plain float → decimal degrees (new API)
                    ra_deg  = float(str_ra)
                    dec_deg = float(str_dec)
                    sky = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg)
                except ValueError:
                    # Sexagesimal string, e.g. "03 47 29.1" / "+24 06 18"  (old API)
                    sky = SkyCoord(ra=str_ra, dec=str_dec, unit=(u.hourangle, u.deg))

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

    Simbad runs first in the matching chain, so at this point all sources have
    catalog_name=None. Simbad provides rich object-type info (V*, EB*, G, etc.)
    for named objects: variable stars, binaries, galaxies, nebulae, etc.
    Plain stars not in Simbad fall through to Gaia in the next stage.
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
# 2MASS (Two Micron All Sky Survey — VizieR catalog II/246)
# ---------------------------------------------------------------------------

def _query_2mass(ra_center: float, dec_center: float, fov_deg: float) -> list[dict]:
    """
    Query 2MASS Point Source Catalog (VizieR II/246) within fov_deg/2 of the frame centre.

    Returns a list of dicts with keys: ra, dec, designation, jmag.
    J-band magnitude is used as catalog_mag because it is the most sensitive
    2MASS band and closest in wavelength to the Gaia G band.

    Returns [] on any error so the pipeline can continue with partial results.
    """
    cache_key = f"2mass:{ra_center:.3f}:{dec_center:.3f}:{fov_deg:.3f}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached  # type: ignore[return-value]

    try:
        coord = SkyCoord(ra=ra_center * u.deg, dec=dec_center * u.deg)
        # Same radius strategy as Gaia: half-diagonal to cover all frame corners
        radius = (fov_deg * math.sqrt(2) / 2.0) * u.deg

        viz = Vizier(
            columns=["RAJ2000", "DEJ2000", "_2MASS", "Jmag"],
            row_limit=-1,   # unlimited rows
        )
        result = viz.query_region(coord, radius=radius, catalog="II/246")

        if result is None or len(result) == 0:
            _cache_set(cache_key, [])
            return []

        table = result[0]
        logger.debug("2MASS VizieR result columns: %s", table.colnames)

        # Determine the actual column names present in the result.
        # VizieR may return "_2MASS" as a meta-column under varying names
        # depending on the astroquery version.
        col_names = set(table.colnames)
        desig_col = next(
            (c for c in ("_2MASS", "2MASS", "_2mass", "2mass") if c in col_names),
            None,
        )

        stars: list[dict] = []
        for row in table:
            try:
                jmag = float(row["Jmag"])
                if not math.isfinite(jmag):
                    continue

                ra_val  = float(row["RAJ2000"])
                dec_val = float(row["DEJ2000"])

                # Use the designation column if available; otherwise generate an
                # ID from coordinates in standard 2MASS format (Jhhmmss.s±ddmmss).
                if desig_col is not None:
                    desig = str(row[desig_col]).strip()
                else:
                    coord  = SkyCoord(ra=ra_val * u.deg, dec=dec_val * u.deg)
                    ra_hms = coord.ra.to_string(unit=u.hourangle, sep="", precision=1, pad=True)
                    dec_dms = coord.dec.to_string(sep="", precision=0, alwayssign=True, pad=True)
                    desig  = f"J{ra_hms}{dec_dms}"

                stars.append({
                    "ra":          ra_val,
                    "dec":         dec_val,
                    "designation": desig,
                    "jmag":        jmag,
                })
            except (TypeError, ValueError):
                continue

        _cache_set(cache_key, stars)
        logger.debug(
            "2MASS query returned %d stars for ra=%.3f dec=%.3f",
            len(stars), ra_center, dec_center,
        )
        return stars

    except Exception as exc:
        logger.warning("2MASS query failed for ra=%.3f dec=%.3f: %s", ra_center, dec_center, exc)
        return []


def _match_2mass(sources: list[dict], twomass_stars: list[dict]) -> None:
    """
    Mutate sources in-place: set catalog fields for unmatched sources within
    MATCH_CONE_ARCSEC of a 2MASS point source.

    2MASS runs after Simbad and Gaia DR3, catching stars that are faint or
    absent in Gaia (e.g. late-type M/K stars, heavily reddened stars near the
    Galactic plane). catalog_mag is set to J-band magnitude.
    """
    unmatched = [s for s in sources if s["catalog_name"] is None]
    if not unmatched or not twomass_stars:
        return

    unmatched_coords = SkyCoord(
        ra=[s["ra"] for s in unmatched] * u.deg,
        dec=[s["dec"] for s in unmatched] * u.deg,
    )
    twomass_coords = SkyCoord(
        ra=[o["ra"] for o in twomass_stars] * u.deg,
        dec=[o["dec"] for o in twomass_stars] * u.deg,
    )

    idx, sep2d, _ = unmatched_coords.match_to_catalog_sky(twomass_coords)
    threshold = config.MATCH_CONE_ARCSEC * u.arcsec

    for i, source in enumerate(unmatched):
        if sep2d[i] < threshold:
            matched = twomass_stars[idx[i]]
            source["catalog_name"] = "2MASS"
            source["catalog_id"]   = matched["designation"]
            source["catalog_mag"]  = matched["jmag"]
            source["object_type"]  = "STAR"


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

    Queries catalogs in order: Simbad → Gaia DR3 → 2MASS → MPC.
    Each catalog stage is isolated; a failure in one does not prevent the
    others from running. Query results are cached for 1 hour to avoid
    redundant network calls when multiple frames cover the same sky area.

    Matching order rationale:
        1. Simbad first — rich object types (V*, EB*, G, QSO, etc.) for
           named objects; plain stars fall through to Gaia.
        2. Gaia DR3 — dense stellar catalog with G-band magnitudes; also
           performs WCS offset correction using all sources.
        3. 2MASS — fallback for red/cool stars faint or absent in Gaia
           (late M/K dwarfs, reddened stars near Galactic plane); J-band mag.
        4. MPC/SkyBot — solar system objects (asteroids, comets); wider cone.

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
        catalog_name  str | None   — "Simbad", "Gaia DR3", "2MASS", "MPC", or None
        catalog_id    str | None
        catalog_mag   float | None — G-band (Gaia), J-band (2MASS), or None
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

    # ------------------------------------------------------------------
    # Phase 1: Query Gaia to compute WCS offset, then apply it to ALL
    # source coordinates BEFORE any catalog matching begins.
    #
    # Why: The WCS solution from ASTAP can have a residual systematic
    # offset (typically < 30"). If we don't correct it first, Simbad and
    # 2MASS will match against wrong coordinates and return zero results.
    # Gaia is used because it is dense enough to compute a robust
    # statistical offset via vote accumulator even with a large initial
    # error. The corrected coordinates are written back to source["ra"]
    # and source["dec"] so all subsequent stages benefit automatically.
    # ------------------------------------------------------------------
    gaia_stars: list[dict] = []
    try:
        gaia_stars = _query_gaia(ra_center, dec_center, fov_deg)
        logger.info(
            "Gaia query: ra=%.4f dec=%.4f fov=%.4f° radius=%.4f° → %d catalog stars  fits_filename=%s",
            ra_center, dec_center, fov_deg, fov_deg * math.sqrt(2) / 2.0, len(gaia_stars), fits_filename,
        )
    except Exception as exc:
        logger.warning("Gaia query failed for fits_filename=%s: %s", fits_filename, exc)

    # Compute WCS offset and apply to source coordinates in-place
    try:
        offset_ra_deg, offset_dec_deg = _compute_wcs_offset(sources, gaia_stars)
        if offset_ra_deg != 0.0 or offset_dec_deg != 0.0:
            for source in sources:
                source["ra"]  += offset_ra_deg
                source["dec"] += offset_dec_deg
                source["_wcs_offset_ra"]  = offset_ra_deg
                source["_wcs_offset_dec"] = offset_dec_deg
            logger.info(
                "Applied WCS correction dRA=%.2f\" dDec=%.2f\" to %d sources  fits_filename=%s",
                offset_ra_deg * 3600.0, offset_dec_deg * 3600.0, len(sources), fits_filename,
            )
    except Exception as exc:
        logger.warning("WCS offset computation failed for fits_filename=%s: %s", fits_filename, exc)

    # ------------------------------------------------------------------
    # Phase 2: Match catalogs in order using corrected coordinates.
    # Simbad → Gaia DR3 → 2MASS → MPC
    # ------------------------------------------------------------------

    # --- 1. Simbad (named objects with rich type info) ---
    simbad_objects: list[dict] = []
    try:
        simbad_objects = _query_simbad(ra_center, dec_center, fov_deg)
        logger.info(
            "Simbad query: ra=%.4f dec=%.4f fov=%.4f° → %d objects  fits_filename=%s",
            ra_center, dec_center, fov_deg, len(simbad_objects), fits_filename,
        )
        _match_simbad(sources, simbad_objects)
    except Exception as exc:
        logger.warning("Simbad matching stage failed for fits_filename=%s: %s", fits_filename, exc)

    # --- 2. Gaia DR3 (dense stellar catalog, WCS offset already applied) ---
    try:
        _match_gaia(sources, gaia_stars)
    except Exception as exc:
        logger.warning("Gaia matching stage failed for fits_filename=%s: %s", fits_filename, exc)

    # --- 3. 2MASS (fallback for red/cool stars absent in Gaia) ---
    twomass_stars: list[dict] = []
    try:
        twomass_stars = _query_2mass(ra_center, dec_center, fov_deg)
        logger.info(
            "2MASS query: ra=%.4f dec=%.4f fov=%.4f° → %d catalog stars  fits_filename=%s",
            ra_center, dec_center, fov_deg, len(twomass_stars), fits_filename,
        )
        _match_2mass(sources, twomass_stars)
    except Exception as exc:
        logger.warning("2MASS matching stage failed for fits_filename=%s: %s", fits_filename, exc)

    # --- 4. MPC / SkyBot (solar system objects; wider cone) ---
    mpc_objects: list[dict] = []
    try:
        mpc_objects = _query_mpc(ra_center, dec_center, obs_time, fov_deg)
        _match_mpc(sources, mpc_objects)
    except Exception as exc:
        logger.warning("MPC/SkyBot matching stage failed for fits_filename=%s: %s", fits_filename, exc)

    n_simbad    = sum(1 for s in sources if s["catalog_name"] == "Simbad")
    n_gaia      = sum(1 for s in sources if s["catalog_name"] == "Gaia DR3")
    n_2mass     = sum(1 for s in sources if s["catalog_name"] == "2MASS")
    n_mpc       = sum(1 for s in sources if s["catalog_name"] == "MPC")
    n_unmatched = sum(1 for s in sources if s["catalog_name"] is None)

    logger.info(
        "Catalog matching: %d sources — Simbad: %d, Gaia: %d, 2MASS: %d, MPC: %d, unmatched: %d  fits_filename=%s",
        len(sources), n_simbad, n_gaia, n_2mass, n_mpc, n_unmatched, fits_filename,
    )

    # Warn when very few sources match any stellar catalog — expected for fields at
    # high galactic latitude (galaxy clusters) where most detections are compact
    # galaxies rather than stars. If you expect more matches, check:
    #   1. Galactic latitude of the target (|b| > 60° → few stars, many galaxies)
    #   2. STAR_SNR_MIN threshold — lowering it detects fainter stars
    #   3. Run on a Milky Way field to verify the pipeline works for star-rich frames
    if len(sources) > 0:
        n_stellar = n_simbad + n_gaia + n_2mass
        match_rate = n_stellar / len(sources)
        if match_rate < 0.05 and len(sources) >= 20:
            logger.warning(
                "Low catalog match rate: %.1f%% (%d/%d sources matched Simbad/Gaia/2MASS). "
                "This is expected for high-galactic-latitude fields where most detections "
                "are compact galaxies not present in stellar catalogs.  fits_filename=%s",
                match_rate * 100, n_stellar, len(sources), fits_filename,
            )

    return sources
