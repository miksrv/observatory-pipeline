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

from ._cache import _cache_get, _cache_set

logger = logging.getLogger(__name__)


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

    cache_key = f"panstarrs:{ra_center:.1f}:{dec_center:.1f}:{fov_deg:.1f}"
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
