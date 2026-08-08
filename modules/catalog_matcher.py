"""
modules/catalog_matcher.py — Cross-match detected sources against external catalogs.

The single public entry point is:

    await catalog_matcher.match(sources: list[dict], frame_meta: dict) -> list[dict]

Each source dict is enriched in-place with four catalog fields:
    catalog_name  — "Simbad", "Gaia DR3", "2MASS", "MPC", or None
    catalog_id    — catalog's identifier string, or None
    catalog_mag   — magnitude (float): G-band for Gaia, J-band for 2MASS, None otherwise
    object_type   — "STAR", Simbad OTYPE string, "ASTEROID", "COMET", or None

Catalogs are queried in order: Simbad → Gaia DR3 → 2MASS → Pan-STARRS → MPC.
Once a source is matched, subsequent catalogs skip it. This order prioritises rich
object-type information from Simbad over generic Gaia stellar matching, uses 2MASS
for red/cool stars absent in Gaia, Pan-STARRS for faint optical sources below Gaia's
completeness limit, and MPC last for solar system objects.

Rationale for 2MASS as third catalog:
    - 2MASS (Two Micron All Sky Survey) covers ~470 million point sources to K≈14.3
    - Complements Gaia for late-type (M/K) stars that are bright in NIR but faint
      in the Gaia G band, and for heavily reddened regions near the Galactic plane
    - Accessed via VizieR catalog II/246; magnitudes stored as J-band (Jmag)

Rationale for Pan-STARRS DR1 as fourth catalog:
    - Covers ~1 billion sources to r~23.3 mag (Gaia DR3 complete only to ~21 mag)
    - Reduces false UNKNOWN anomaly alerts for faint sources simply below Gaia depth
    - Coverage limited to declination > -30° (optical survey from Haleakala, Hawaii)
    - Accessed via VizieR catalog II/349/ps1; magnitudes stored as r-band (rmag)

Rate limits of the online services (all free, no auth required):
    Simbad:      CDS infrastructure, ~5–6 req/sec recommended; 1-hr cache is sufficient
    Gaia DR3:    ESA TAP+, no hard limit; queries take 1–5 s; 1-hr cache is sufficient
    2MASS:       CDS/VizieR infrastructure, same limits as Simbad; cache is sufficient
    Pan-STARRS:  CDS/VizieR infrastructure, same limits as Simbad; cache is sufficient
    MPC/SkyBot:  IMCCE, no hard limit; epoch-dependent so shorter natural TTL

Catalog query results are cached in-process for 1 hour to avoid redundant network
calls across sources that share the same field.

All catalog errors are caught and logged; a failing catalog never crashes the pipeline.
"""

from __future__ import annotations

import datetime
import json
import logging
import math
import os
import tempfile
import warnings
from typing import Any

import astropy.units as u
import numpy as np
from astropy.coordinates import SkyCoord, search_around_sky
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
# Query cache — TTL from config.CACHE_TTL_HOURS (default 1h), keyed by
# catalog + sky region.
#
# Two-tier: an in-process dict (fast path — no disk I/O for a key already
# read this run) backed by files under config.CATALOG_CACHE_DIR. The disk
# tier exists so restarting the pipeline (frequent during testing, and after
# every code change without --reload) doesn't throw away query results and
# re-hit Gaia/Simbad/2MASS/Pan-STARRS/MPC for the same sky region — the
# in-process dict alone dies with the process. See config.py's docstring for
# CATALOG_CACHE_DIR on why this must be mounted from outside the container.
# ---------------------------------------------------------------------------

_cache: dict[str, dict[str, Any]] = {}
_CACHE_TTL = datetime.timedelta(hours=config.CACHE_TTL_HOURS)


def _cache_file_path(key: str) -> str:
    """Filesystem-safe path under CATALOG_CACHE_DIR for a given cache key."""
    safe_key = key.replace("/", "_").replace(":", "_")
    return os.path.join(config.CATALOG_CACHE_DIR, f"{safe_key}.json")


def _cache_get(key: str) -> Any | None:
    """
    Return cached data if present and within TTL, else None.

    Checks the in-process dict first; on a miss there, falls back to the
    on-disk cache (populating the in-process dict from it on a hit, so later
    calls in this same run skip the file read). A stale or unreadable/corrupt
    on-disk entry is treated the same as a miss — the caller re-queries the
    network and _cache_set() overwrites the bad file.
    """
    entry = _cache.get(key)
    if entry and (datetime.datetime.now(datetime.timezone.utc) - entry["fetched_at"]) < _CACHE_TTL:
        return entry["data"]

    path = _cache_file_path(key)
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return None  # no on-disk entry either

    age = datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromtimestamp(
        mtime, tz=datetime.timezone.utc
    )
    if age >= _CACHE_TTL:
        return None  # stale on-disk entry — treat as a miss, re-query and overwrite it

    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(
            "Corrupt or unreadable catalog cache file %s: %s — treating as a cache miss",
            path, exc,
        )
        return None

    _cache[key] = {"data": data, "fetched_at": datetime.datetime.now(datetime.timezone.utc)}
    return data


def _cache_set(key: str, data: Any) -> None:
    """
    Store data in the in-process dict AND on disk.

    The disk write is best-effort: any failure (permission error, disk full,
    CATALOG_CACHE_DIR not mounted) is logged and swallowed, degrading to
    in-process-only caching for the rest of this run rather than breaking
    catalog matching. Writes to a temp file and renames into place — cheap
    insurance against a crash mid-write leaving a truncated/corrupt file
    behind that would otherwise poison every read of this key until
    CACHE_TTL expires it.
    """
    _cache[key] = {"data": data, "fetched_at": datetime.datetime.now(datetime.timezone.utc)}

    path = _cache_file_path(key)
    tmp_path: str | None = None
    try:
        os.makedirs(config.CATALOG_CACHE_DIR, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=config.CATALOG_CACHE_DIR, suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp_path, path)
    except OSError as exc:
        logger.warning(
            "Failed to write catalog cache file %s: %s — continuing with in-process cache only",
            path, exc,
        )
        if tmp_path is not None and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


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

    # ------------------------------------------------------------------
    # Quick nearest-neighbour pass — logging and early-exit only.
    # ------------------------------------------------------------------
    idx_nn, sep2d_nn, _ = source_coords.match_to_catalog_sky(gaia_coords)
    sep_nn = sep2d_nn.to(u.arcsec).value

    within_5  = int(np.sum(sep_nn <= 5.0))
    within_10 = int(np.sum(sep_nn <= 10.0))
    within_30 = int(np.sum(sep_nn <= 30.0))
    within_60 = int(np.sum(sep_nn <= 60.0))
    min_sep    = float(np.min(sep_nn))    if len(sep_nn) > 0 else 0.0
    max_sep    = float(np.max(sep_nn))    if len(sep_nn) > 0 else 0.0
    median_sep = float(np.median(sep_nn)) if len(sep_nn) > 0 else 0.0

    logger.info(
        "Gaia match (raw): min=%.2f\" median=%.2f\" max=%.2f\"  "
        "within 5\"=%d, 10\"=%d, 30\"=%d, 60\"=%d (threshold=%.1f\")",
        min_sep, median_sep, max_sep, within_5, within_10, within_30, within_60,
        config.MATCH_CONE_ARCSEC,
    )

    if median_sep <= 10.0:
        # WCS is already accurate — no correction needed
        return 0.0, 0.0

    # ------------------------------------------------------------------
    # All-pairs vote accumulator
    #
    # The nearest-neighbour approach fails for sparse fields with large
    # WCS offsets: with only ~10 sources and an offset of 30-60", each
    # source's nearest Gaia star is typically a WRONG star at ~10" away,
    # so every source votes for a different random offset → peak = 1.
    #
    # The fix: use search_around_sky to collect ALL (source, Gaia-star)
    # pairs within MAX_SEARCH_RADIUS. True pairs (correct star at the
    # systematic offset distance) all vote for the same histogram bin and
    # produce a sharp peak. False pairs (nearby wrong stars) are scattered
    # uniformly and create only background noise.
    # ------------------------------------------------------------------
    MAX_SEARCH_ARCSEC = 90.0
    idx_src, idx_cat, _, _ = search_around_sky(
        source_coords, gaia_coords, MAX_SEARCH_ARCSEC * u.arcsec
    )
    n_pairs = len(idx_src)

    if n_pairs < 3:
        logger.debug(
            "Too few source-Gaia pairs (%d) within %.0f\" — cannot compute WCS offset",
            n_pairs, MAX_SEARCH_ARCSEC,
        )
        return 0.0, 0.0

    logger.info(
        "WCS offset search: %d source-Gaia pairs within %.0f\" (%d sources, %d Gaia stars)",
        n_pairs, MAX_SEARCH_ARCSEC, len(sources), len(gaia_stars),
    )

    mean_dec = float(np.mean([s["dec"] for s in sources]))
    cos_dec  = math.cos(math.radians(mean_dec))

    dra_arcsec  = np.array([
        (gaia_stars[idx_cat[k]]["ra"]  - sources[idx_src[k]]["ra"])  * cos_dec * 3600.0
        for k in range(n_pairs)
    ])
    ddec_arcsec = np.array([
        (gaia_stars[idx_cat[k]]["dec"] - sources[idx_src[k]]["dec"]) * 3600.0
        for k in range(n_pairs)
    ])

    # ------------------------------------------------------------------
    # Bin size and significance test.
    #
    # Real incident (2026-08-06, "Vesta_A807_FA" field, 40 sources vs 4453
    # Gaia stars in a ~1° frame — i.e. ~10000 stars/deg², so almost EVERY
    # source has several Gaia stars within 90" by pure chance):
    #   - True offset (confirmed independently against ASTAP's own solve
    #     log): dRA≈+18" dDec≈-60" (total≈63").
    #   - With the old BIN_SIZE=2" bins, the ~15-20 true pairs spread out
    #     over several adjacent 2"-bins (real seeing/centroid scatter on
    #     top of the systematic offset), so no single bin held more than
    #     2 votes — below even the old "≥3 votes" floor. Meanwhile a
    #     random noise bin elsewhere reached 2 votes just from the sheer
    #     number of background pairs (245 pairs over 8100 bins), and the
    #     old `expected_bg * 20` threshold is a flat multiplicative rule
    #     that — at this bin size — was *more* permissive for that noise
    #     bin than for the real, larger, but more spread-out peak.
    #
    # Fix: use a coarser bin (COARSE_BIN_ARCSEC) so real pairs sharing the
    # same systematic offset land in one bin despite a few arcsec of
    # individual scatter, and replace the flat "20x background" rule with
    # a proper Poisson excess test (peak must clear background by several
    # σ) *plus* an absolute floor on the vote count — the σ-only test is
    # unreliable at very low background (sqrt(tiny number) is tiny, so
    # even a 2-vote noise bin can look "many σ" above almost-zero
    # background, which is exactly the false peak seen above).
    # ------------------------------------------------------------------
    COARSE_BIN_ARCSEC = 15.0
    RANGE    = MAX_SEARCH_ARCSEC
    n_bins   = int(2 * RANGE / COARSE_BIN_ARCSEC)

    H, ra_edges, dec_edges = np.histogram2d(
        dra_arcsec, ddec_arcsec,
        bins=n_bins,
        range=[[-RANGE, RANGE], [-RANGE, RANGE]],
    )
    peak_i, peak_j = np.unravel_index(np.argmax(H), H.shape)
    peak_count  = int(H[peak_i, peak_j])
    expected_bg = n_pairs / float(n_bins ** 2)

    MIN_PEAK_VOTES = 5     # absolute floor — never trust a 2-3 vote "peak"
    SIGMA_MARGIN   = 5.0   # peak must exceed background by this many Poisson σ
    sig_threshold = expected_bg + SIGMA_MARGIN * math.sqrt(max(expected_bg, 0.5))

    peak_dra  = float((ra_edges[peak_i]  + ra_edges[peak_i + 1])  / 2.0)
    peak_ddec = float((dec_edges[peak_j] + dec_edges[peak_j + 1]) / 2.0)
    total_offset = math.sqrt(peak_dra ** 2 + peak_ddec ** 2)

    logger.info(
        "WCS offset accumulator: %d pairs → peak=%d (bg≈%.3f, threshold=%.1f, "
        "min_votes=%d) at dRA=%.1f\" dDec=%.1f\" (total=%.1f\")",
        n_pairs, peak_count, expected_bg, sig_threshold, MIN_PEAK_VOTES,
        peak_dra, peak_ddec, total_offset,
    )

    if peak_count < MIN_PEAK_VOTES or peak_count < sig_threshold or total_offset <= 2.0:
        logger.debug(
            "No significant WCS offset detected (peak=%d < threshold=%.1f or votes, "
            "offset=%.1f\"). This is expected for galaxy-rich fields where most "
            "detections are extended sources not present in Gaia.",
            peak_count, sig_threshold, total_offset,
        )
        return 0.0, 0.0

    # Refine: iterative sigma-clip-style median around the coarse peak.
    #
    # A single median over the ±COARSE_BIN_ARCSEC window (the old behaviour)
    # is still contaminated by background pairs caught in that same wide
    # window — in the real incident above, that pulled the one-shot estimate
    # to (12.6", -57.3") vs. a true offset around (21", -57"). Re-centering
    # the window on each new estimate and re-taking the median sheds most of
    # that contamination within a few passes, since true pairs stay inside
    # a tight window around the true offset while background pairs fall out
    # once the window re-centers. REFINE_RADIUS_ARCSEC is deliberately
    # tighter than COARSE_BIN_ARCSEC for this reason.
    REFINE_RADIUS_ARCSEC = 10.0
    refined_dra, refined_ddec = peak_dra, peak_ddec
    for _ in range(15):
        near_mask = (
            (np.abs(dra_arcsec  - refined_dra)  <= REFINE_RADIUS_ARCSEC) &
            (np.abs(ddec_arcsec - refined_ddec) <= REFINE_RADIUS_ARCSEC)
        )
        if not near_mask.any():
            break
        new_dra  = float(np.median(dra_arcsec[near_mask]))
        new_ddec = float(np.median(ddec_arcsec[near_mask]))
        if math.isclose(new_dra, refined_dra, abs_tol=0.05) and math.isclose(new_ddec, refined_ddec, abs_tol=0.05):
            refined_dra, refined_ddec = new_dra, new_ddec
            break
        refined_dra, refined_ddec = new_dra, new_ddec

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
# Pan-STARRS DR1 (VizieR catalog II/349/ps1)
# ---------------------------------------------------------------------------

def _query_panstarrs(ra_center: float, dec_center: float, fov_deg: float) -> list[dict]:
    """
    Query Pan-STARRS DR1 (VizieR II/349/ps1) within fov_deg/2 of the frame centre.

    Pan-STARRS reaches r~23.3 mag, significantly deeper than Gaia DR3 (~21 mag),
    and reduces false UNKNOWN alerts for faint sources that are simply below Gaia's
    completeness limit. Coverage is restricted to declination > -30°.

    Returns a list of dicts with keys: ra, dec, obj_id, rmag.
    Returns [] outside coverage or on any error.
    """
    if dec_center < -30.0:
        logger.debug(
            "Pan-STARRS skipped: dec=%.3f is outside coverage (dec > -30° required)",
            dec_center,
        )
        return []

    cache_key = f"panstarrs:{ra_center:.3f}:{dec_center:.3f}:{fov_deg:.3f}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached  # type: ignore[return-value]

    try:
        coord  = SkyCoord(ra=ra_center * u.deg, dec=dec_center * u.deg)
        radius = (fov_deg * math.sqrt(2) / 2.0) * u.deg

        viz = Vizier(
            columns=["RAJ2000", "DEJ2000", "objID", "rmag"],
            row_limit=-1,
        )
        result = viz.query_region(coord, radius=radius, catalog="II/349/ps1")

        if result is None or len(result) == 0:
            _cache_set(cache_key, [])
            return []

        table = result[0]

        sources: list[dict] = []
        for row in table:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    rmag = float(row["rmag"])
                if not math.isfinite(rmag):
                    continue
                sources.append({
                    "ra":     float(row["RAJ2000"]),
                    "dec":    float(row["DEJ2000"]),
                    "obj_id": str(row["objID"]),
                    "rmag":   rmag,
                })
            except (TypeError, ValueError):
                continue

        _cache_set(cache_key, sources)
        logger.debug(
            "Pan-STARRS query returned %d sources for ra=%.3f dec=%.3f",
            len(sources), ra_center, dec_center,
        )
        return sources

    except Exception as exc:
        logger.warning("Pan-STARRS query failed for ra=%.3f dec=%.3f: %s", ra_center, dec_center, exc)
        return []


def _match_panstarrs(sources: list[dict], ps_sources: list[dict]) -> None:
    """
    Mutate sources in-place: set catalog fields for unmatched sources within
    MATCH_CONE_ARCSEC of a Pan-STARRS DR1 source.

    Runs after Simbad, Gaia DR3, and 2MASS — catches faint sources (r < 23.3)
    that fall below Gaia's completeness limit (~21 mag), reducing false UNKNOWN
    alerts. catalog_mag is set to r-band magnitude.
    """
    unmatched = [s for s in sources if s["catalog_name"] is None]
    if not unmatched or not ps_sources:
        return

    unmatched_coords = SkyCoord(
        ra=[s["ra"] for s in unmatched] * u.deg,
        dec=[s["dec"] for s in unmatched] * u.deg,
    )
    ps_coords = SkyCoord(
        ra=[o["ra"] for o in ps_sources] * u.deg,
        dec=[o["dec"] for o in ps_sources] * u.deg,
    )

    idx, sep2d, _ = unmatched_coords.match_to_catalog_sky(ps_coords)
    threshold = config.MATCH_CONE_ARCSEC * u.arcsec

    for i, source in enumerate(unmatched):
        if sep2d[i] < threshold:
            matched = ps_sources[idx[i]]
            source["catalog_name"] = "Pan-STARRS"
            source["catalog_id"]   = f"PS1 {matched['obj_id']}"
            source["catalog_mag"]  = matched["rmag"]
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

        if not obs_time:
            logger.warning("SkyBot skipped: obs_time is empty (check DATE-OBS header in FITS)")
            _cache_set(cache_key, [])
            return []

        coord = SkyCoord(ra=ra_center * u.deg, dec=dec_center * u.deg)
        epoch = Time(obs_time)
        fov_arcmin = fov_deg * 60.0

        logger.info(
            "SkyBot query: ra=%.4f dec=%.4f radius=%.1f' epoch=%s (UTC)",
            ra_center, dec_center, fov_arcmin, epoch.utc.iso,
        )

        result = Skybot.cone_search(coord, rad=fov_arcmin * u.arcmin, epoch=epoch)

        if result is None or len(result) == 0:
            logger.info(
                "SkyBot: no solar system objects found at ra=%.4f dec=%.4f epoch=%s",
                ra_center, dec_center, epoch.utc.iso,
            )
            _cache_set(cache_key, [])
            return []

        # Log available columns once to help diagnose column name variations
        # across astroquery versions (Name/name, RA/ra, Class/Type etc.)
        logger.info(
            "SkyBot returned %d row(s), columns: %s",
            len(result), list(result.colnames),
        )

        # Normalise column names to uppercase for version-independent access
        col_map = {c.upper(): c for c in result.colnames}

        ra_col    = col_map.get("RA")
        dec_col   = col_map.get("DEC")
        name_col  = col_map.get("NAME") or col_map.get("OBJECT") or col_map.get("DESIGNATION")
        class_col = col_map.get("CLASS") or col_map.get("TYPE") or col_map.get("OBJECTTYPE")

        if not ra_col or not dec_col or not name_col:
            logger.warning(
                "SkyBot result missing expected columns. Available: %s", list(result.colnames)
            )
            _cache_set(cache_key, [])
            return []

        objects: list[dict] = []
        for row in result:
            try:
                # SkyBot returns RA/DEC as astropy Quantities (with angular units).
                # .value extracts the numeric value in the column's native unit (degrees).
                raw_ra  = row[ra_col]
                raw_dec = row[dec_col]
                ra_val  = float(raw_ra.value)  if hasattr(raw_ra,  "value") else float(raw_ra)
                dec_val = float(raw_dec.value) if hasattr(raw_dec, "value") else float(raw_dec)
                name    = str(row[name_col]).strip()
                obj_class = str(row[class_col]).strip() if class_col else "Asteroid"

                obj_type = "COMET" if "comet" in obj_class.lower() else "ASTEROID"

                logger.info(
                    "SkyBot object: %s  type=%s  ra=%.4f dec=%.4f",
                    name, obj_type, ra_val, dec_val,
                )
                objects.append({
                    "ra":          ra_val,
                    "dec":         dec_val,
                    "designation": name,
                    "object_type": obj_type,
                })
            except Exception as row_exc:
                logger.warning("SkyBot: skipping malformed row: %s", row_exc)
                continue

        _cache_set(cache_key, objects)
        return objects

    except ImportError:
        logger.warning("astroquery.imcce.Skybot not available, skipping MPC matching")
        _cache_set(cache_key, [])
        return []
    except Exception as exc:
        logger.warning(
            "SkyBot query failed: ra=%.4f dec=%.4f obs_time=%r — %s",
            ra_center, dec_center, obs_time, exc,
        )
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

    # --- 4. Pan-STARRS DR1 (deep optical catalog, dec > -30°; catches faint sources
    #        below Gaia completeness limit ~21 mag, reduces false UNKNOWN alerts) ---
    ps_sources: list[dict] = []
    try:
        ps_sources = _query_panstarrs(ra_center, dec_center, fov_deg)
        if ps_sources:
            logger.info(
                "Pan-STARRS query: ra=%.4f dec=%.4f fov=%.4f° → %d sources  fits_filename=%s",
                ra_center, dec_center, fov_deg, len(ps_sources), fits_filename,
            )
        _match_panstarrs(sources, ps_sources)
    except Exception as exc:
        logger.warning("Pan-STARRS matching stage failed for fits_filename=%s: %s", fits_filename, exc)

    # --- 5. MPC / SkyBot (solar system objects; wider cone) ---
    mpc_objects: list[dict] = []
    try:
        mpc_objects = _query_mpc(ra_center, dec_center, obs_time, fov_deg)
        _match_mpc(sources, mpc_objects)
    except Exception as exc:
        logger.warning("MPC/SkyBot matching stage failed for fits_filename=%s: %s", fits_filename, exc)

    n_simbad     = sum(1 for s in sources if s["catalog_name"] == "Simbad")
    n_gaia       = sum(1 for s in sources if s["catalog_name"] == "Gaia DR3")
    n_2mass      = sum(1 for s in sources if s["catalog_name"] == "2MASS")
    n_panstarrs  = sum(1 for s in sources if s["catalog_name"] == "Pan-STARRS")
    n_mpc        = sum(1 for s in sources if s["catalog_name"] == "MPC")
    n_unmatched  = sum(1 for s in sources if s["catalog_name"] is None)

    logger.info(
        "Catalog matching: %d sources — Simbad: %d, Gaia: %d, 2MASS: %d, Pan-STARRS: %d, MPC: %d, unmatched: %d  fits_filename=%s",
        len(sources), n_simbad, n_gaia, n_2mass, n_panstarrs, n_mpc, n_unmatched, fits_filename,
    )

    # Warn when very few sources match any stellar catalog — expected for fields at
    # high galactic latitude (galaxy clusters) where most detections are compact
    # galaxies rather than stars. If you expect more matches, check:
    #   1. Galactic latitude of the target (|b| > 60° → few stars, many galaxies)
    #   2. STAR_SNR_MIN threshold — lowering it detects fainter stars
    #   3. Run on a Milky Way field to verify the pipeline works for star-rich frames
    if len(sources) > 0:
        n_stellar = n_simbad + n_gaia + n_2mass + n_panstarrs
        match_rate = n_stellar / len(sources)
        if match_rate < 0.05 and len(sources) >= 20:
            logger.warning(
                "Low catalog match rate: %.1f%% (%d/%d sources matched Simbad/Gaia/2MASS). "
                "This is expected for high-galactic-latitude fields where most detections "
                "are compact galaxies not present in stellar catalogs.  fits_filename=%s",
                match_rate * 100, n_stellar, len(sources), fits_filename,
            )

    return sources
