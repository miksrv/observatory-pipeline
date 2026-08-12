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

from ._cache import _cache_get, _cache_set

logger = logging.getLogger(__name__)


def _query_2mass(ra_center: float, dec_center: float, fov_deg: float) -> list[dict]:
    """
    Query 2MASS Point Source Catalog (VizieR II/246) within fov_deg/2 of the frame centre.

    Returns a list of dicts with keys: ra, dec, designation, jmag.
    J-band magnitude is used as catalog_mag because it is the most sensitive
    2MASS band and closest in wavelength to the Gaia G band.

    Returns [] on any error so the pipeline can continue with partial results.
    """
    cache_key = f"2mass:{ra_center:.1f}:{dec_center:.1f}:{fov_deg:.1f}"
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
