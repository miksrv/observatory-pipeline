"""
modules/catalog_matcher/_panstarrs.py — Pan-STARRS DR1 (VizieR catalog
II/349/ps1) querying and matching.

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import logging
import math
import warnings

import astropy.units as u
from astropy.coordinates import SkyCoord
from astroquery.vizier import Vizier

import config

from . import _gaia_archive, _outcome
from ._cache import (
    _cache_fov_deg,
    _cache_get,
    _cache_position,
    _cache_radius_margin_deg,
    _cache_set,
)
from ._sources import GAIA_ARCHIVE, SourceRotation, source_label, unique

logger = logging.getLogger(__name__)

# Which PANSTARRS_SOURCES failed and until when (see _sources).
_rotation = SourceRotation()


def _query_vizier(host: str, coord: SkyCoord, radius) -> list[dict]:
    """Pan-STARRS DR1 rows from one VizieR host; raises on any transport or service error."""
    viz = Vizier(
        columns=["RAJ2000", "DEJ2000", "objID", "rmag"],
        row_limit=-1,
        vizier_server=host,
    )
    result = viz.query_region(coord, radius=radius, catalog="II/349/ps1")
    if result is None or len(result) == 0:
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
    return sources


def _query_panstarrs(ra_center: float, dec_center: float, fov_deg: float) -> list[dict]:
    """
    Query Pan-STARRS DR1 within the frame's half-diagonal of its centre.

    Pan-STARRS reaches r~23.3 mag, significantly deeper than Gaia DR3 (~21 mag),
    and reduces false UNKNOWN alerts for faint sources that are simply below Gaia's
    completeness limit. Coverage is restricted to declination > -30°.

    Asks the sources in PANSTARRS_SOURCES in turn — VizieR hosts or `gaia`,
    the ESA Gaia archive's copy of DR1 (`_gaia_archive`; no VizieR mirror
    carries this table); the first that answers decides the result, an empty
    field included, and a failed source is asked last for
    CATALOG_SOURCE_COOLDOWN_SEC (see `_sources`). Every source gives the same
    `objID`, so history does not split by source.

    Returns a list of dicts with keys: ra, dec, obj_id, rmag.
    Returns [] outside coverage, or when every source fails (nothing cached).
    """
    if dec_center < -30.0:
        logger.debug(
            "Pan-STARRS skipped: dec=%.3f is outside coverage (dec > -30° required)",
            dec_center,
        )
        _outcome.record("Pan-STARRS", _outcome.NOT_APPLICABLE)
        return []

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
    cache_key = f"panstarrs:{key_ra:.1f}:{key_dec:.1f}:{key_fov:.1f}"
    cached = _cache_get(cache_key)
    if cached is not None:
        _outcome.record("Pan-STARRS", _outcome.OK)
        return cached  # type: ignore[return-value]

    sources = unique(config.PANSTARRS_SOURCES)
    if not sources:
        # PANSTARRS_SOURCES empty: Pan-STARRS matching is off. Not cached —
        # the tile key is shared with a configuration that has sources.
        _outcome.record("Pan-STARRS", _outcome.DISABLED)
        return []

    coord  = SkyCoord(ra=key_ra * u.deg, dec=key_dec * u.deg)
    radius_deg = (key_fov * math.sqrt(2) / 2.0) + _cache_radius_margin_deg()

    def ask(source: str) -> list[dict]:
        if source == GAIA_ARCHIVE:
            return _gaia_archive._query_panstarrs(key_ra, key_dec, radius_deg)
        return _query_vizier(source, coord, radius_deg * u.deg)

    answer = _rotation.first_answering(
        sources, ask,
        cooldown_sec=config.CATALOG_SOURCE_COOLDOWN_SEC,
        catalog="Pan-STARRS",
        where=f"ra={ra_center:.3f} dec={dec_center:.3f}",
        label=source_label,
    )
    if answer is None:
        _outcome.record("Pan-STARRS", _outcome.FAILED)
        return []
    source, ps_sources = answer
    _cache_set(cache_key, ps_sources)
    _outcome.record("Pan-STARRS", _outcome.OK)
    logger.info("Pan-STARRS: %d source(s) from %s", len(ps_sources), source_label(source))
    return ps_sources


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
