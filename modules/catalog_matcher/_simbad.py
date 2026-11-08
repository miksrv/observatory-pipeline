"""
modules/catalog_matcher/_simbad.py — Simbad querying and matching.

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import logging
import math

import astropy.units as u
import requests.adapters
from astropy.coordinates import SkyCoord
from astroquery.simbad import Simbad
from astroquery.simbad import conf as simbad_conf

import config

from ._cache import (
    _cache_fov_deg,
    _cache_get,
    _cache_position,
    _cache_radius_margin_deg,
    _cache_set,
)
from . import _outcome
from ._sources import SourceRotation, unique

logger = logging.getLogger(__name__)

# Which SIMBAD_SERVERS failed and until when (see _sources).
_rotation = SourceRotation()
# SIMBAD_SERVERS entries astroquery does not know, warned about once per process.
_warned_unknown: set[str] = set()


def configured_servers() -> list[str]:
    """
    SIMBAD_SERVERS in its configured order, each once, keeping only the
    servers astroquery knows (`conf.servers_list`: the CDS original and its
    Harvard mirror) — `Simbad.server` refuses any other host. The catalog
    health check probes this same list.
    """
    configured: list[str] = []
    for host in unique(config.SIMBAD_SERVERS):
        if host not in simbad_conf.servers_list:
            if host not in _warned_unknown:
                _warned_unknown.add(host)
                logger.warning(
                    "SIMBAD_SERVERS: unknown server %r ignored (known: %s)",
                    host, ", ".join(simbad_conf.servers_list),
                )
            continue
        configured.append(host)
    return configured


class _TimeoutAdapter(requests.adapters.HTTPAdapter):
    """
    An HTTP adapter that gives every request a timeout. astroquery's Simbad
    runs a synchronous TAP query through pyvo on the instance's own
    `requests.Session`, with no client-side timeout (its `timeout` is the
    server-side execution limit, applied to asynchronous jobs only): a host
    that accepts the connection and never answers would hold the frame
    forever, and the fallback to the next server, which needs an exception,
    would never happen.
    """

    def __init__(self, timeout_sec: float) -> None:
        super().__init__()
        self.timeout_sec = timeout_sec

    def send(self, request, *, timeout=None, **kwargs):  # type: ignore[override]
        return super().send(request, timeout=timeout if timeout is not None else self.timeout_sec, **kwargs)


def _client(host: str) -> Simbad:
    simbad = Simbad()
    simbad.server = host
    simbad._session.mount("https://", _TimeoutAdapter(config.SIMBAD_TIMEOUT_SEC))
    simbad.add_votable_fields("otype")
    return simbad


def _parse_simbad_table(result) -> list[dict]:
    """Rows of a `query_region()` table → `{ra, dec, main_id, otype}` dicts; malformed rows skipped."""
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
    return objects


def _query_simbad(ra_center: float, dec_center: float, fov_deg: float) -> list[dict]:
    """
    Query Simbad for all named objects within the frame's half-diagonal of its
    centre.

    The radius is `fov_deg × sqrt(2)/2`, the same strategy Gaia/2MASS/Pan-STARRS
    already use: `fov_deg` is the frame's larger dimension, so for any aspect
    ratio the half-diagonal is at most that, and the circle covers every corner.
    A plain `fov_deg / 2` — this catalog's own radius until the 2026-08-18 audit
    (finding H2) — inscribes a circle in the frame instead, leaving the corner
    regions (up to ~29% of a square frame's area) never queried. A named object
    there falls through to Gaia's generic `object_type="STAR"` and permanently
    loses the ability to trigger VARIABLE_STAR/BINARY_STAR/SUPERNOVA_CANDIDATE,
    every one of which gates on a real Simbad OTYPE.

    Asks the servers in SIMBAD_SERVERS in turn (the CDS original, then its
    Harvard mirror); the first that answers decides the result, an empty
    field included — both serve the same database. A failed server is asked
    last for CATALOG_SOURCE_COOLDOWN_SEC (see `_sources`). CDS was unreachable
    for days from 2026-10-28 and every frame lost its object types meanwhile.

    Returns a list of dicts with keys: ra, dec, main_id, otype.
    Returns [] when every server fails (nothing cached) or when Simbad returns None.
    """
    # Query around the TILE the cache key rounds to, with the tile's own
    # half-diagonal added to the radius — not around this frame's exact
    # centre. A key covers a 0.1 deg tile, so a later frame sharing it can sit
    # up to a tile diagonal away, and a circle drawn around THIS frame need
    # not contain that one's edge region at all (audit 2026-08-18, finding
    # M5). See _cache._cache_position().
    key_ra, key_dec = _cache_position(ra_center, dec_center)
    # The FOV is bucketed UPWARD to the same 0.1 deg resolution, and the query
    # below uses the bucket rather than this frame's exact FOV — otherwise the
    # cached cone is not a function of the key it is stored under, and a
    # narrower frame filling the bucket first leaves a wider one's edge
    # unqueried. See _cache._cache_fov_deg().
    key_fov = _cache_fov_deg(fov_deg)
    cache_key = f"simbad:{key_ra:.1f}:{key_dec:.1f}:{key_fov:.1f}"
    cached = _cache_get(cache_key)
    if cached is not None:
        _outcome.record("Simbad", _outcome.OK)
        return cached  # type: ignore[return-value]

    servers = configured_servers()
    if not servers:
        # SIMBAD_SERVERS empty (or only unknown hosts): Simbad matching is
        # off. Not cached — the tile key is shared with a configuration that
        # has servers, and an empty answer would outlive the setting.
        _outcome.record("Simbad", _outcome.DISABLED)
        return []

    coord = SkyCoord(ra=key_ra * u.deg, dec=key_dec * u.deg)
    radius = ((key_fov * math.sqrt(2) / 2.0) + _cache_radius_margin_deg()) * u.deg

    answer = _rotation.first_answering(
        servers,
        lambda host: _client(host).query_region(coord, radius=radius),
        cooldown_sec=config.CATALOG_SOURCE_COOLDOWN_SEC,
        catalog="Simbad",
        where=f"ra={ra_center:.3f} dec={dec_center:.3f}",
    )
    if answer is None:
        _outcome.record("Simbad", _outcome.FAILED)
        return []
    host, result = answer
    objects = [] if result is None else _parse_simbad_table(result)
    _cache_set(cache_key, objects)
    _outcome.record("Simbad", _outcome.OK)
    logger.info("Simbad: %d object(s) from %s", len(objects), host)
    return objects


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
