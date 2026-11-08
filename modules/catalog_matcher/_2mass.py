"""
modules/catalog_matcher/_2mass.py — 2MASS (Two Micron All Sky Survey,
VizieR catalog II/246) querying and matching.

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import logging
import math

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

# Which TWOMASS_SOURCES failed and until when (see _sources).
_rotation = SourceRotation()


def _designation_from_position(ra_deg: float, dec_deg: float) -> str:
    """
    A 2MASS-style designation (`hhmmssss±ddmmsss`, as the catalog's own
    `2MASS` column writes it) built from the position, for a VizieR answer
    that carries no designation column. The same star must get the same
    `catalog_id` whichever source named it, so the format is the catalog's,
    not a readable variant of it.
    """
    # 2MASS truncates every component (01335356+3037049 lies at 30.618048°,
    # i.e. 04.97"), so this truncates too — rounding would name one star in
    # three differently.
    def _parts(value: float, last_digits: int) -> tuple[int, int, int]:
        whole = int(value)
        minutes = int((value - whole) * 60.0)
        seconds = ((value - whole) * 60.0 - minutes) * 60.0
        return whole, minutes, int(seconds * 10 ** last_digits + 1e-6)

    h, m, s = _parts(ra_deg / 15.0, 2)
    sign = "-" if dec_deg < 0 else "+"
    d, dm, ds = _parts(abs(dec_deg), 1)
    return f"{h:02d}{m:02d}{s:04d}{sign}{d:02d}{dm:02d}{ds:03d}"


def _query_vizier(host: str, coord: SkyCoord, radius) -> list[dict]:
    """2MASS PSC rows from one VizieR host; raises on any transport or service error."""
    # "2MASS" is the catalog's designation column; the CDS VizieR also offers
    # it as the "_2MASS" alias, a mirror (vizier.idia.ac.za) only under its
    # own name. Asking for both gets it from either (unknown names are
    # ignored).
    viz = Vizier(
        columns=["RAJ2000", "DEJ2000", "_2MASS", "2MASS", "Jmag"],
        row_limit=-1,   # unlimited rows
        vizier_server=host,
    )
    result = viz.query_region(coord, radius=radius, catalog="II/246")
    if result is None or len(result) == 0:
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

            if desig_col is not None:
                desig = str(row[desig_col]).strip()
            else:
                desig = _designation_from_position(ra_val, dec_val)

            stars.append({
                "ra":          ra_val,
                "dec":         dec_val,
                "designation": desig,
                "jmag":        jmag,
            })
        except (TypeError, ValueError):
            continue
    return stars


def _query_2mass(ra_center: float, dec_center: float, fov_deg: float) -> list[dict]:
    """
    Query the 2MASS Point Source Catalog within the frame's half-diagonal of
    its centre.

    Asks the sources in TWOMASS_SOURCES in turn — VizieR hosts (the CDS
    original, a mirror) or `gaia`, the ESA Gaia archive's copy of the same
    catalog (`_gaia_archive`); the first that answers decides the result, an
    empty field included, and a failed source is asked last for
    CATALOG_SOURCE_COOLDOWN_SEC (see `_sources`). Every source gives the same
    designations, so history does not split by source.

    Returns a list of dicts with keys: ra, dec, designation, jmag.
    J-band magnitude is used as catalog_mag because it is the most sensitive
    2MASS band and closest in wavelength to the Gaia G band.

    Returns [] when every source fails (nothing cached), so the pipeline can
    continue with partial results.
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
    cache_key = f"2mass:{key_ra:.1f}:{key_dec:.1f}:{key_fov:.1f}"
    cached = _cache_get(cache_key)
    if cached is not None:
        _outcome.record("2MASS", _outcome.OK)
        return cached  # type: ignore[return-value]

    sources = unique(config.TWOMASS_SOURCES)
    if not sources:
        # TWOMASS_SOURCES empty: 2MASS matching is off. Not cached — the tile
        # key is shared with a configuration that has sources.
        _outcome.record("2MASS", _outcome.DISABLED)
        return []

    coord = SkyCoord(ra=key_ra * u.deg, dec=key_dec * u.deg)
    # Same radius strategy as Gaia: half-diagonal to cover all frame corners
    radius_deg = (key_fov * math.sqrt(2) / 2.0) + _cache_radius_margin_deg()

    def ask(source: str) -> list[dict]:
        if source == GAIA_ARCHIVE:
            return _gaia_archive._query_2mass(key_ra, key_dec, radius_deg)
        return _query_vizier(source, coord, radius_deg * u.deg)

    answer = _rotation.first_answering(
        sources, ask,
        cooldown_sec=config.CATALOG_SOURCE_COOLDOWN_SEC,
        catalog="2MASS",
        where=f"ra={ra_center:.3f} dec={dec_center:.3f}",
        label=source_label,
    )
    if answer is None:
        _outcome.record("2MASS", _outcome.FAILED)
        return []
    source, stars = answer
    _cache_set(cache_key, stars)
    _outcome.record("2MASS", _outcome.OK)
    logger.info("2MASS: %d star(s) from %s", len(stars), source_label(source))
    return stars


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
