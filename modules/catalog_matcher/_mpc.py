"""
modules/catalog_matcher/_mpc.py — MPC / SkyBot (Minor Planet Center / IMCCE)
querying and matching for known solar system objects.

Internal helpers only — not part of this package's public surface, except
`get_mpc_objects()` which is re-exported by `__init__.py`.
"""

from __future__ import annotations

import logging

import astropy.units as u
from astropy.coordinates import SkyCoord

import config

from ._cache import _cache_get, _cache_set

logger = logging.getLogger(__name__)


def _query_mpc(ra_center: float, dec_center: float, obs_time: str, fov_deg: float = 1.0) -> list[dict]:
    """
    Query for known asteroids and comets near the frame centre at observation time.

    Uses IMCCE SkyBot service which provides cone search for solar system objects
    at a specific epoch. Falls back gracefully on any error.

    Returns a list of dicts with keys: ra, dec, designation, object_type.
    """
    cache_key = f"mpc:{ra_center:.1f}:{dec_center:.1f}:{obs_time}"
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
        mag_col   = col_map.get("V") or col_map.get("MV") or col_map.get("VMAG")

        if not ra_col or not dec_col or not name_col:
            logger.warning(
                "SkyBot result missing expected columns. Available: %s", list(result.colnames)
            )
            _cache_set(cache_key, [])
            return []

        mag_limit = config.MPC_MAG_LIMIT
        n_skipped_faint = 0

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

                # Parse predicted visual magnitude — skip objects too faint to
                # be detectable by this telescope. Without this filter, SkyBot
                # returns dozens of mag > 20 asteroids in any field, and the
                # matching logic assigns each one to its nearest unmatched
                # background star, producing spurious non-moving "ASTEROID"
                # anomalies (real incident, 2026-08-10, Vesta field: 130+
                # asteroids returned, only Vesta at V=6.2 was actually
                # detectable on 60s exposures; 2014 RY1 at V=21.1 was matched
                # to a star).
                v_mag: float | None = None
                if mag_col:
                    try:
                        raw_mag = row[mag_col]
                        v_mag = float(raw_mag.value) if hasattr(raw_mag, "value") else float(raw_mag)
                    except (TypeError, ValueError):
                        pass

                if v_mag is not None and v_mag > mag_limit:
                    n_skipped_faint += 1
                    continue

                logger.info(
                    "SkyBot object: %s  type=%s  V=%.1f  ra=%.4f dec=%.4f",
                    name, obj_type, v_mag if v_mag is not None else -99.0, ra_val, dec_val,
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

        if n_skipped_faint:
            logger.info(
                "SkyBot: skipped %d object(s) fainter than MPC_MAG_LIMIT=%.1f",
                n_skipped_faint, mag_limit,
            )

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

    One-to-one matching: each MPC object is assigned to at most ONE detected
    source (the nearest unmatched source within the threshold). The previous
    implementation matched in the opposite direction (for each source, find
    the nearest MPC object) which allowed multiple sources to claim the same
    MPC designation — then _dedupe_by_catalog_identity() kept the brightest,
    which for a faint asteroid (e.g. 2014 RY1 at mag 21) was invariably a
    nearby uncatalogued background star rather than the real asteroid. The
    finder chart then showed that star's unchanging position as the
    "asteroid's track" (real incident, 2026-08-10: 2014 RY1 appeared
    stationary on its track chart while Vesta on the same frames moved
    correctly — Vesta is bright enough to always win the dedup, but 2014 RY1
    is not).
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

    threshold = config.MOVING_CONE_ARCSEC * u.arcsec

    # Match in the MPC→source direction: for each MPC object, find its
    # nearest unmatched source. This ensures each MPC designation is
    # assigned to at most one source (the closest detection to the
    # predicted ephemeris position), preventing a faint real asteroid from
    # being out-competed by a brighter background star that also happened
    # to fall within MOVING_CONE_ARCSEC.
    idx, sep2d, _ = mpc_coords.match_to_catalog_sky(unmatched_coords)

    # Track which unmatched sources have already been claimed, so that if
    # two MPC objects both want the same source, only the closer one wins.
    claimed: dict[int, int] = {}  # unmatched_index → mpc_index that claimed it

    # Process MPC objects nearest-match-first so a closer match always wins
    # over a more distant one when two MPC objects compete for the same source.
    order = sorted(range(len(mpc_objects)), key=lambda k: sep2d[k].arcsec)

    for mpc_idx in order:
        if sep2d[mpc_idx] >= threshold:
            continue

        src_idx = int(idx[mpc_idx])

        # If this source was already claimed by a closer MPC object, skip.
        if src_idx in claimed:
            continue

        claimed[src_idx] = mpc_idx
        source = unmatched[src_idx]
        obj = mpc_objects[mpc_idx]
        source["catalog_name"] = "MPC"
        source["catalog_id"]   = obj["designation"]
        source["catalog_mag"]  = None
        source["object_type"]  = obj["object_type"]


# ---------------------------------------------------------------------------
# Public accessor for the already-fetched, region-wide MPC field list
#
# modules/forced_photometry.py's reverse-matching pass (ROADMAP.md #1) needs
# the exact same MPC/SkyBot field list _match.match() already queried for
# forward matching. See _gaia.get_gaia_stars()'s docstring for the full
# rationale — this is the same pattern, just for MPC objects instead of
# Gaia stars: a cache hit against the in-process dict match() itself just
# populated a moment earlier, no new network round trip in the common case.
# ---------------------------------------------------------------------------

def get_mpc_objects(ra_center: float, dec_center: float, obs_time: str, fov_deg: float = 1.0) -> list[dict]:
    """Return the same MPC/SkyBot field list match() uses for moving-object matching."""
    return _query_mpc(ra_center, dec_center, obs_time, fov_deg)
