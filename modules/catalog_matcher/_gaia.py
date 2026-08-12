"""
modules/catalog_matcher/_gaia.py — Gaia DR3 querying and matching.

Internal helpers only — not part of this package's public surface, except
`get_gaia_stars()` which is re-exported by `__init__.py`.
"""

from __future__ import annotations

import logging
import math
import warnings

import astropy.units as u
import numpy as np
from astropy.coordinates import SkyCoord
from astroquery.gaia import Gaia

import config

from ._cache import _cache_get, _cache_set

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configure Gaia query limits
# Default ROW_LIMIT is 50, which is far too few for typical FITS frames
# with thousands of sources. Increase to 50000 to cover most use cases.
# ---------------------------------------------------------------------------
Gaia.ROW_LIMIT = 50000


def _query_gaia(ra_center: float, dec_center: float, fov_deg: float) -> list[dict]:
    """
    Query Gaia DR3 for all stars within fov_deg/2 of the frame centre.

    Returns a list of dicts with keys: ra, dec, source_id, phot_g_mean_mag,
    pmra, pmdec, ref_epoch. The last three are proper motion in RA*cos(dec)
    and Dec (mas/yr) and the epoch (Julian year, J2016.0 for Gaia DR3) those
    positions/motions are referenced to — needed by
    modules/forced_photometry.py to propagate a star's position forward to
    the actual observation epoch before projecting it to a pixel position
    (a star can move several arcsec between Gaia's DR3 epoch and "now" for
    high proper-motion objects). `Gaia.cone_search()`'s default column set
    includes these; pmra/pmdec/ref_epoch fall back to None/None/2016.0 if a
    row is missing them (e.g. Gaia has no astrometric solution for that
    source) or the installed astroquery version returns a narrower column
    set — callers must treat a None pmra/pmdec as "no proper-motion
    correction available", not as zero motion.

    Returns [] on any error so the pipeline can continue with partial results.
    """
    cache_key = f"gaia:{ra_center:.1f}:{dec_center:.1f}:{fov_deg:.1f}"
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

        has_pmra      = "pmra"      in table.colnames
        has_pmdec     = "pmdec"     in table.colnames
        has_ref_epoch = "ref_epoch" in table.colnames

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

            pmra: float | None = None
            pmdec: float | None = None
            if has_pmra and has_pmdec:
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", UserWarning)
                        pmra_val  = float(row["pmra"])
                        pmdec_val = float(row["pmdec"])
                    if math.isfinite(pmra_val) and math.isfinite(pmdec_val):
                        pmra, pmdec = pmra_val, pmdec_val
                except (TypeError, ValueError):
                    pass

            ref_epoch = 2016.0  # Gaia DR3 reference epoch (J2016.0)
            if has_ref_epoch:
                try:
                    ref_epoch = float(row["ref_epoch"])
                except (TypeError, ValueError):
                    pass

            stars.append({
                "ra":              float(row["ra"]),
                "dec":             float(row["dec"]),
                "source_id":       str(row["source_id"]),
                "phot_g_mean_mag": mag_float,
                "pmra":            pmra,
                "pmdec":           pmdec,
                "ref_epoch":       ref_epoch,
            })

        _cache_set(cache_key, stars)
        logger.debug("Gaia DR3 query returned %d stars for ra=%.3f dec=%.3f", len(stars), ra_center, dec_center)
        return stars

    except Exception as exc:
        logger.warning("Gaia DR3 query failed for ra=%.3f dec=%.3f: %s", ra_center, dec_center, exc)
        return []


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
# Public accessor for the already-fetched, region-wide Gaia field list
#
# modules/forced_photometry.py's reverse-matching pass (ROADMAP.md #1) needs
# the exact same Gaia DR3 field list _match.match() already queried for
# forward matching — measuring flux at a catalog star's predicted pixel
# position is only worth doing for a star this frame's footprint actually
# covers. Rather than threading that data out through match()'s return value
# (which would change its signature and the shape every existing
# caller/test relies on), this thin wrapper just calls the same private,
# cached _query_gaia() again: for the same (ra_center, dec_center, fov_deg)
# key, this is a cache hit against the in-process dict match() itself just
# populated a moment earlier in the same frame's processing — no new network
# round trip. A cache miss (e.g. this is called well after match(), or from
# a context that never called match() at all) simply re-queries Gaia
# directly, which is still correct, just not free.
# ---------------------------------------------------------------------------

def get_gaia_stars(ra_center: float, dec_center: float, fov_deg: float) -> list[dict]:
    """Return the same Gaia DR3 field list match() uses for WCS-offset correction / matching."""
    return _query_gaia(ra_center, dec_center, fov_deg)
