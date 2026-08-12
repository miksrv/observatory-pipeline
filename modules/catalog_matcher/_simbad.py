"""
modules/catalog_matcher/_simbad.py — Simbad querying and matching.

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import logging

import astropy.units as u
from astropy.coordinates import SkyCoord
from astroquery.simbad import Simbad

import config

from ._cache import _cache_get, _cache_set

logger = logging.getLogger(__name__)


def _query_simbad(ra_center: float, dec_center: float, fov_deg: float) -> list[dict]:
    """
    Query Simbad for all named objects within fov_deg/2 of the frame centre.

    Returns a list of dicts with keys: ra, dec, main_id, otype.
    Returns [] on any error or when Simbad returns None.
    """
    cache_key = f"simbad:{ra_center:.1f}:{dec_center:.1f}:{fov_deg:.1f}"
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
