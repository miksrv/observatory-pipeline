"""
modules/catalog_matcher/_mpc.py — known solar system objects: querying (IMCCE
SkyBot, with JPL's Small-Body Identification API as the second source — see
`_sbident.py`) and matching.

Internal helpers only — not part of this package's public surface, except
`get_mpc_objects()` which is re-exported by `__init__.py`.
"""

from __future__ import annotations

import logging
import math
import time

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time

import config

from . import _sbident
from ._cache import (
    _cache_fov_deg,
    _cache_get,
    _cache_position,
    _cache_radius_margin_deg,
    _cache_set,
)

logger = logging.getLogger(__name__)


def _cone_search_with_retry(skybot, coord: SkyCoord, radius_arcmin: float, epoch):
    """
    `Skybot.cone_search()`, retried on any failure (see SKYBOT_MAX_ATTEMPTS).

    IMCCE intermittently answers a valid query with a body that is not a
    VOTable, which astropy reports as "No table found"; the identical query
    succeeds moments later. A genuinely empty field is not a failure — it
    parses to an empty table — so every exception here is worth a retry.

    `cache=False` is what makes the retry meaningful: astroquery caches the
    raw HTTP response *before* parsing it, so with its default `cache=True`
    a malformed answer would be stored and every retry (and every later frame
    at the same epoch) would re-read the same broken body. Reuse is handled by
    this package's own cache, which stores only successfully parsed results.

    Blocking sleep, like the query itself: catalog matching already runs these
    synchronous astroquery calls inline.
    """
    attempts = max(1, config.SKYBOT_MAX_ATTEMPTS)
    delay = config.SKYBOT_RETRY_DELAY_SEC
    for attempt in range(1, attempts + 1):
        try:
            return skybot.cone_search(coord, rad=radius_arcmin * u.arcmin, epoch=epoch, cache=False)
        except Exception as exc:
            if attempt == attempts:
                raise
            logger.warning(
                "SkyBot attempt %d/%d failed (%s) — retrying in %.1fs",
                attempt, attempts, exc, delay,
            )
            time.sleep(delay)
            delay *= 2
    raise AssertionError("unreachable")


def _query_skybot(ra_deg: float, dec_deg: float, radius_deg: float, epoch: Time) -> list[dict]:
    """
    IMCCE SkyBot cone search, parsed into dicts with keys ra, dec,
    designation, object_type; objects fainter than MPC_MAG_LIMIT dropped.

    Raises on any failure — every retry spent, astroquery missing, or an
    answer without the expected columns (a malformed answer, not an empty
    sky) — so `_query_mpc()` can ask the next source. An empty field is not a
    failure: it returns [].
    """
    from astroquery.imcce import Skybot

    coord = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg)
    radius_arcmin = radius_deg * 60.0

    logger.info(
        "SkyBot query: ra=%.4f dec=%.4f radius=%.1f' epoch=%s (UTC)",
        ra_deg, dec_deg, radius_arcmin, epoch.utc.iso,
    )

    result = _cone_search_with_retry(Skybot, coord, radius_arcmin, epoch)

    if result is None or len(result) == 0:
        logger.info(
            "SkyBot: no solar system objects found at ra=%.4f dec=%.4f epoch=%s",
            ra_deg, dec_deg, epoch.utc.iso,
        )
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
        raise ValueError(f"SkyBot result missing expected columns. Available: {list(result.colnames)}")

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
    return objects


# Every source of known solar system objects: MPC_SOURCES name → log label.
_SOURCE_LABELS: dict[str, str] = {"skybot": "SkyBot", "jpl": "JPL sb_ident"}

# Source name → time.monotonic() before which it counts as down. Set when a
# source fails (after its own retries), cleared when it answers.
_down_until: dict[str, float] = {}
# Unknown MPC_SOURCES names already warned about (once per process, not per frame).
_warned_unknown: set[str] = set()


def _query_source(source: str, ra_deg: float, dec_deg: float, radius_deg: float, epoch: Time) -> list[dict]:
    # Through the module reference, so tests can patch _sbident._query_sbident.
    if source == "skybot":
        return _query_skybot(ra_deg, dec_deg, radius_deg, epoch)
    return _sbident._query_sbident(ra_deg, dec_deg, radius_deg, epoch)


def _sources_in_order() -> list[str]:
    """
    MPC_SOURCES in its configured order, except that a source which failed
    within the last MPC_SOURCE_COOLDOWN_SEC goes to the back.

    A down source is not skipped outright — it is still asked when every
    other one fails too — but during an outage lasting days (SkyBot,
    2026-10-23) a frame no longer spends SkyBot's whole retry budget before
    reaching the source that works.
    """
    configured: list[str] = []
    for name in config.MPC_SOURCES:
        if name not in _SOURCE_LABELS:
            if name not in _warned_unknown:
                _warned_unknown.add(name)
                logger.warning("MPC_SOURCES: unknown source %r ignored (known: %s)", name, ", ".join(_SOURCE_LABELS))
        elif name not in configured:
            configured.append(name)
    now = time.monotonic()
    up = [s for s in configured if _down_until.get(s, 0.0) <= now]
    return up + [s for s in configured if s not in up]


def _query_mpc(ra_center: float, dec_center: float, obs_time: str, fov_deg: float = 1.0) -> list[dict]:
    """
    Query for known asteroids and comets near the frame centre at observation time.

    Asks the sources in MPC_SOURCES in turn (IMCCE SkyBot, then JPL's
    Small-Body Identification API — see `_sbident`), and the first one that
    answers decides the result, an empty field included. Both yield the same
    designations, so it does not matter to history which one answered. If
    every source fails, returns [] without caching it.

    The radius is `fov_deg × sqrt(2)/2` — the frame's half-diagonal, the same
    strategy every other catalog here uses — **plus** `MOVING_CONE_ARCSEC`.
    That last term is this catalog's own: `_match_mpc()` pairs an ephemeris
    position with a source up to 120" away, far wider than the 5" every
    stellar catalog matches within, so an object that legitimately matches a
    corner source can sit that much outside the frame and still has to be in
    the result set.

    The full `fov_deg` used until the 2026-08-18 audit (finding L1) was ~1.41x
    the half-diagonal, i.e. about twice the sky area, on every frame. It was
    never a match-loss bug — it over-covered — but IMCCE is a shared public
    service and SkyBot is the one catalog here whose cache key includes the
    exact observation epoch, so a night of frames re-queries it far more often
    than it re-queries Gaia or Simbad.

    Returns a list of dicts with keys: ra, dec, designation, object_type.
    """
    # Query around the TILE the cache key rounds to, with the tile's own
    # half-diagonal added to the radius — not around this frame's exact
    # centre. A key covers a 0.1 deg tile, so a later frame sharing it (same
    # epoch, same tile) can sit up to a tile diagonal away, and a circle drawn
    # around THIS frame need not contain that one's edge region at all (audit
    # 2026-08-18, finding M5). See _cache._cache_position().
    # The FOV belongs in the key for the same reason: the radius below is
    # derived from it, so two frames of different widths at the same tile and
    # epoch do NOT share a cone — omitting it entirely (as this key did) let a
    # narrow frame's result satisfy a wider one, whose own edge region, moving
    # margin included, had never been queried. Bucketed upward so a hit always
    # covers its requester. See _cache._cache_fov_deg().
    key_ra, key_dec = _cache_position(ra_center, dec_center)
    key_fov = _cache_fov_deg(fov_deg)
    cache_key = f"mpc:{key_ra:.1f}:{key_dec:.1f}:{key_fov:.1f}:{obs_time}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached  # type: ignore[return-value]

    if not obs_time:
        logger.warning("MPC query skipped: obs_time is empty (check DATE-OBS header in FITS)")
        _cache_set(cache_key, [])
        return []

    try:
        epoch = Time(obs_time)
    except Exception as exc:
        logger.warning(
            "MPC query skipped: ra=%.4f dec=%.4f obs_time=%r is not a time — %s",
            ra_center, dec_center, obs_time, exc,
        )
        return []

    sources = _sources_in_order()
    if not sources:
        # MPC_SOURCES empty (or only unknown names): MPC matching is off.
        _cache_set(cache_key, [])
        return []

    radius_deg = (
        key_fov * math.sqrt(2) / 2.0
        + config.MOVING_CONE_ARCSEC / 3600.0
        + _cache_radius_margin_deg()
    )

    for source in sources:
        label = _SOURCE_LABELS[source]
        try:
            objects = _query_source(source, key_ra, key_dec, radius_deg, epoch)
        except Exception as exc:
            _down_until[source] = time.monotonic() + config.MPC_SOURCE_COOLDOWN_SEC
            logger.warning(
                "%s query failed: ra=%.4f dec=%.4f obs_time=%r — %s",
                label, ra_center, dec_center, obs_time, exc,
            )
            continue
        if source in _down_until:
            logger.info("%s answers again", label)
            del _down_until[source]
        logger.info("MPC: %d object(s) from %s", len(objects), label)
        _cache_set(cache_key, objects)
        return objects

    logger.warning(
        "MPC: no source answered for ra=%.4f dec=%.4f obs_time=%r — frame gets no MPC matching",
        ra_center, dec_center, obs_time,
    )
    return []


def _match_mpc(sources: list[dict], mpc_objects: list[dict]) -> None:
    """
    Mutate sources in-place: set catalog fields for sources matching a known
    MPC object, using a wider cone (MOVING_CONE_ARCSEC) than Gaia/Simbad to
    account for object motion between the MPC ephemeris epoch and the actual
    observation time.

    One-to-one matching: each MPC object is assigned to at most ONE detected
    source, and each source to at most one object. The assignment is greedy
    over EVERY eligible (object, source) pair ordered by separation, not over
    one nearest proposal per object — so an object that loses its nearest
    source to a closer competitor still gets its next-nearest eligible one
    rather than going unmatched. The original
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

    # Whether a source was already claimed by an earlier catalog, captured
    # BEFORE this pass starts reassigning anything — the eligibility rule
    # below asks about the state _match_mpc() was handed, not the one it is
    # building.
    unclaimed = [s["catalog_name"] is None for s in sources]

    # EVERY (mpc object, source) pair inside the widest cone this stage can
    # use, not just each object's own nearest source. A single nearest
    # proposal per object cannot be retried: when two objects proposed the
    # same source, the loser was dropped outright even with another perfectly
    # eligible unclaimed source of its own inside the cone, and a real MPC
    # detection was lost to an ordering accident. Building the full edge set
    # and assigning greedily over it gives every loser its next-nearest
    # eligible source instead.
    idx_mpc, idx_src, sep2d, _ = all_coords.search_around_sky(
        mpc_coords, config.MOVING_CONE_ARCSEC * u.arcsec
    )

    # (separation_arcsec, mpc_index, source_index) for every eligible edge.
    # Eligibility is the same positional rule as before: the tight cone
    # permits a takeover of an already-identified source, the wide one only
    # reaches sources no other catalog claimed.
    proposals: list[tuple[float, int, int]] = []
    for edge in range(len(idx_mpc)):
        mpc_idx = int(idx_mpc[edge])
        src_idx = int(idx_src[edge])
        sep_arcsec = float(sep2d[edge].arcsec)
        if sep_arcsec <= config.MATCH_CONE_ARCSEC or (
            unclaimed[src_idx] and sep_arcsec < config.MOVING_CONE_ARCSEC
        ):
            proposals.append((sep_arcsec, mpc_idx, src_idx))

    # Nearest edge first, so a closer MPC object always wins over a more
    # distant one when two of them compete for the same source — and the
    # loser simply falls through to its own next edge in this same list.
    proposals.sort(key=lambda p: p[0])

    claimed: set[int] = set()
    assigned_mpc: set[int] = set()
    for sep_arcsec, mpc_idx, src_idx in proposals:
        if src_idx in claimed or mpc_idx in assigned_mpc:
            continue
        claimed.add(src_idx)
        assigned_mpc.add(mpc_idx)

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
        # A takeover inherits nothing from the star it displaced: its Gaia
        # BP-RP would otherwise be applied through the colour term to the
        # moving object's own calibrated magnitude (photometry.py reads
        # _catalog_color regardless of catalog_name), and its G magnitude
        # would make the moving object a zero-point reference — with its
        # quality flags gone, one that even passes the reference screen.
        source.pop("_catalog_color", None)
        source.pop("_catalog_flags", None)
        source.pop("_gaia_mag", None)


# ---------------------------------------------------------------------------
# Public accessor for the already-fetched, region-wide MPC field list
#
# modules/forced_photometry.py's reverse-matching pass needs
# the exact same MPC/SkyBot field list _match.match() already queried for
# forward matching. See _gaia.get_gaia_stars()'s docstring for the full
# rationale — this is the same pattern, just for MPC objects instead of
# Gaia stars: a cache hit against the in-process dict match() itself just
# populated a moment earlier, no new network round trip in the common case.
# ---------------------------------------------------------------------------

def get_mpc_objects(ra_center: float, dec_center: float, obs_time: str, fov_deg: float = 1.0) -> list[dict]:
    """Return the same MPC/SkyBot field list match() uses for moving-object matching."""
    return _query_mpc(ra_center, dec_center, obs_time, fov_deg)
