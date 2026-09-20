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
from astropy.time import Time
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
    positions/motions are referenced to — needed by _propagate_to_epoch()
    below (for matching and the WCS-offset accumulator) and by
    modules/forced_photometry.py, to propagate a star's position forward to
    the actual observation epoch before matching or projecting it to a pixel
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


def _propagate_to_epoch(gaia_stars: list[dict], obs_time: str | None) -> list[dict]:
    """
    Return a copy of `gaia_stars` whose ra/dec have been proper-motion
    propagated from each star's own Gaia `ref_epoch` (J2016.0 for DR3) to the
    frame's observation epoch.

    Gaia DR3's positions are a decade old by now, and a high-proper-motion
    star (hundreds of mas/yr) has drifted several arcsec since — comparable to
    MATCH_CONE_ARCSEC itself. Left uncorrected, such a star simply fails to
    match at the Gaia stage, ends up `catalog_name=None`, satisfies the
    anomaly detector's "shifted" condition, and is reported MOVING_UNKNOWN;
    it also votes for a wrong (dRA, dDec) in the WCS-offset accumulator,
    degrading the correction applied to every other source in the frame
    (audit 2026-08-18, finding H1).

    The star dicts are copied rather than mutated: `_query_gaia()` hands back
    the cached list itself, and the same sky region is routinely re-used by
    frames from other epochs within the cache's TTL — propagating in place
    would write one frame's epoch into every later frame's catalog.

    A star with no astrometric proper-motion solution (`pmra`/`pmdec` None)
    keeps its catalog position, as does every star when `obs_time` is missing
    or unparseable — in both cases this function degrades to exactly the
    previous, uncorrected behaviour rather than failing the stage.

    The per-star formula is a hand-duplicated copy of
    `modules/forced_photometry.py`'s `_propagate_gaia_position()` (which
    applies the same correction to its own pixel projection), kept in sync by
    hand — the same convention this codebase already uses for the streak-mask
    and gain-resolution helpers.
    """
    if not gaia_stars:
        return gaia_stars

    obs_jyear = _obs_jyear(obs_time)
    if obs_jyear is None:
        return gaia_stars

    propagated: list[dict] = []
    n_corrected = 0
    max_shift_arcsec = 0.0

    for star in gaia_stars:
        ra = float(star["ra"])
        dec = float(star["dec"])
        pmra = star.get("pmra")
        pmdec = star.get("pmdec")

        if pmra is None or pmdec is None:
            propagated.append(star)
            continue

        dt_years = obs_jyear - float(star.get("ref_epoch") or 2016.0)
        cos_dec = math.cos(math.radians(dec))
        if dt_years == 0.0 or abs(cos_dec) < 1e-9:
            # At the pole the cos(dec) division blows up; a zero baseline has
            # nothing to correct. Either way the catalog position stands.
            propagated.append(star)
            continue

        # pmra is Gaia's mu_alpha* — already multiplied by cos(dec) — so
        # dividing it back out recovers the true angular RA offset. Parallax
        # and perspective acceleration are ignored: this is about landing
        # inside a 5" matching cone, not precision astrometry.
        d_ra_deg = (pmra / 1000.0 / 3600.0) * dt_years / cos_dec
        d_dec_deg = (pmdec / 1000.0 / 3600.0) * dt_years

        shifted = dict(star)
        shifted["ra"] = ra + d_ra_deg
        shifted["dec"] = dec + d_dec_deg
        propagated.append(shifted)

        n_corrected += 1
        shift_arcsec = math.hypot(d_ra_deg * cos_dec, d_dec_deg) * 3600.0
        max_shift_arcsec = max(max_shift_arcsec, shift_arcsec)

    logger.debug(
        "Gaia proper motion: %d/%d stars propagated to J%.2f (largest shift %.2f\")",
        n_corrected, len(gaia_stars), obs_jyear, max_shift_arcsec,
    )
    return propagated


def _obs_jyear(obs_time: str | None) -> float | None:
    """
    Convert an ISO 8601 observation timestamp to a Julian year, or None when
    it is missing/unparseable — in which case the caller keeps Gaia's own
    catalog epoch rather than guessing one.
    """
    if not obs_time:
        return None
    try:
        return float(Time(str(obs_time)).jyear)
    except Exception as exc:  # astropy raises a variety of parse errors
        logger.debug("Could not parse obs_time %r as a Julian year: %s", obs_time, exc)
        return None


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
    """
    Return the same Gaia DR3 field list match() uses for WCS-offset correction / matching.

    Positions are the raw catalog ones, at Gaia's own ref_epoch — deliberately
    NOT run through _propagate_to_epoch() the way match() runs them, because
    modules/forced_photometry.py (this accessor's only caller) applies its own
    identical proper-motion correction before projecting a star to a pixel.
    Propagating here as well would apply the shift twice.
    """
    return _query_gaia(ra_center, dec_center, fov_deg)
