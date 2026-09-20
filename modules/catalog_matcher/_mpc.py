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
    Mutate sources in-place: set catalog fields for sources matching a known
    MPC object, using a wider cone (MOVING_CONE_ARCSEC) than Gaia/Simbad to
    account for object motion between the MPC ephemeris epoch and the actual
    observation time.

    One-to-one matching: each MPC object is assigned to at most ONE detected
    source (the nearest candidate within the threshold). The original
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

    Candidate pool — why already-matched sources are considered too
    ------------------------------------------------------------------
    This stage runs last, after Simbad/Gaia/2MASS/Pan-STARRS have each
    claimed what they could. Restricting it to the leftovers (the original
    behaviour) meant a solar system object projecting within
    MATCH_CONE_ARCSEC of any background star — routine in a dense field or
    near the galactic plane — was permanently tagged with that star's
    identity before SkyBot ever got a look, losing its ASTEROID/COMET
    classification and its ephemeris (audit 2026-08-18, finding C3). Worse,
    the MPC object was then handed to whatever *other* unmatched source
    happened to be nearest within the 120" cone — a false stationary
    "asteroid" on top of the real miss.

    So the pool is every source, and conflicts are resolved by an explicit
    positional rule rather than by catalog order:

    * A source already claimed by another catalog is taken over **only**
      when the MPC prediction sits within the tight MATCH_CONE_ARCSEC of it —
      i.e. the ephemeris and the detection genuinely coincide, which is
      exactly the blend the finding describes. The wide MOVING_CONE_ARCSEC
      (120") is far too loose to justify overwriting an established
      identification: at that radius some catalogued star is almost always
      present whether or not it has anything to do with the moving object.
    * Beyond that tight cone, the MPC object falls back to the nearest
      *unclaimed* source within MOVING_CONE_ARCSEC — the previous behaviour,
      unchanged.

    A takeover is logged at INFO with both identities, since it is the one
    place in catalog matching where an already-assigned identity changes.
    """
    if not sources or not mpc_objects:
        return

    mpc_coords = SkyCoord(
        ra=[o["ra"] for o in mpc_objects] * u.deg,
        dec=[o["dec"] for o in mpc_objects] * u.deg,
    )
    all_coords = SkyCoord(
        ra=[s["ra"] for s in sources] * u.deg,
        dec=[s["dec"] for s in sources] * u.deg,
    )

    # Nearest source of any kind, per MPC object — the takeover candidate.
    all_idx, all_sep, _ = mpc_coords.match_to_catalog_sky(all_coords)

    # Nearest *unclaimed* source, per MPC object — the ordinary candidate.
    unmatched_pos = [i for i, s in enumerate(sources) if s["catalog_name"] is None]
    un_idx = un_sep = None
    if unmatched_pos:
        un_coords = SkyCoord(
            ra=[sources[i]["ra"] for i in unmatched_pos] * u.deg,
            dec=[sources[i]["dec"] for i in unmatched_pos] * u.deg,
        )
        un_idx, un_sep, _ = mpc_coords.match_to_catalog_sky(un_coords)

    # (separation_arcsec, mpc_index, source_index) — one proposal per MPC object.
    proposals: list[tuple[float, int, int]] = []
    for mpc_idx in range(len(mpc_objects)):
        sep_any = float(all_sep[mpc_idx].arcsec)
        src_any = int(all_idx[mpc_idx])
        if sep_any <= config.MATCH_CONE_ARCSEC:
            proposals.append((sep_any, mpc_idx, src_any))
            continue
        if un_idx is not None:
            sep_un = float(un_sep[mpc_idx].arcsec)
            if sep_un < config.MOVING_CONE_ARCSEC:
                proposals.append((sep_un, mpc_idx, unmatched_pos[int(un_idx[mpc_idx])]))

    # Nearest proposal first, so a closer MPC object always wins over a more
    # distant one when two of them compete for the same source.
    proposals.sort(key=lambda p: p[0])

    claimed: set[int] = set()
    for sep_arcsec, mpc_idx, src_idx in proposals:
        if src_idx in claimed:
            continue
        claimed.add(src_idx)

        source = sources[src_idx]
        obj = mpc_objects[mpc_idx]

        if source["catalog_name"] is not None:
            logger.info(
                "MPC takeover: source at ra=%.5f dec=%.5f reassigned from %s (%s) to "
                "MPC %s — ephemeris %.2f\" away, within MATCH_CONE_ARCSEC=%.1f\"",
                source["ra"], source["dec"],
                source["catalog_name"], source["catalog_id"],
                obj["designation"], sep_arcsec, config.MATCH_CONE_ARCSEC,
            )

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
