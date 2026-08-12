"""
modules/catalog_matcher/_match.py — the package's public entry point,
orchestrating the WCS-offset correction and the five sequential
catalog-matching stages (Simbad → Gaia DR3 → 2MASS → Pan-STARRS → MPC).

Every catalog stage is imported as its own submodule (`from . import _gaia`,
etc.) and called via a qualified attribute (`_gaia._query_gaia(...)`) rather
than a bare name — deliberately, so that
``patch("modules.catalog_matcher._gaia.Gaia", ...)``-style tests (which
patch each catalog's own client class/query function on ITS OWN submodule)
correctly affect the call this function makes too. A bare
``from ._gaia import _query_gaia`` would create a separate name binding in
this file's own globals that such a patch would not reach — see
`.claude/agent-memory/python-senior-dev/feedback_module_to_package_split.md`
for the general rule this avoids.
"""

from __future__ import annotations

import logging
import math

import astropy.units as u
import numpy as np
from astropy.coordinates import SkyCoord

import config

from . import _2mass, _gaia, _mpc, _panstarrs, _simbad, _wcs_offset

logger = logging.getLogger(__name__)


async def match(sources: list[dict], frame_meta: dict) -> list[dict]:
    """
    Enrich each source in-place with catalog identification fields.

    Queries catalogs in order: Simbad → Gaia DR3 → 2MASS → MPC.
    Each catalog stage is isolated; a failure in one does not prevent the
    others from running. Query results are cached for 1 hour to avoid
    redundant network calls when multiple frames cover the same sky area.

    Matching order rationale:
        1. Simbad first — rich object types (V*, EB*, G, QSO, etc.) for
           named objects; plain stars fall through to Gaia.
        2. Gaia DR3 — dense stellar catalog with G-band magnitudes; also
           performs WCS offset correction using all sources.
        3. 2MASS — fallback for red/cool stars faint or absent in Gaia
           (late M/K dwarfs, reddened stars near Galactic plane); J-band mag.
        4. MPC/SkyBot — solar system objects (asteroids, comets); wider cone.

    Parameters
    ----------
    sources:
        List of source dicts as returned by astrometry.solve().
        Each dict must have at minimum: ra (float), dec (float).
    frame_meta:
        Dict with keys: ra_center, dec_center, fov_deg, obs_time (ISO 8601).

    Returns
    -------
    The same list (mutated in-place), with four new keys on every element:
        catalog_name  str | None   — "Simbad", "Gaia DR3", "2MASS", "MPC", or None
        catalog_id    str | None
        catalog_mag   float | None — G-band (Gaia), J-band (2MASS), or None
        object_type   str | None
    """
    fits_filename = frame_meta.get("filename", "<unknown>")

    # Initialise catalog fields on all sources
    for source in sources:
        source.setdefault("catalog_name", None)
        source.setdefault("catalog_id",   None)
        source.setdefault("catalog_mag",  None)
        source.setdefault("object_type",  None)

    if not sources:
        logger.info("Catalog matching: 0 sources — nothing to match  fits_filename=%s", fits_filename)
        return sources

    ra_center  = float(frame_meta.get("ra_center",  0.0))
    dec_center = float(frame_meta.get("dec_center", 0.0))
    fov_deg    = float(frame_meta.get("fov_deg",    1.0))
    obs_time   = str(frame_meta.get("obs_time",    ""))

    # ------------------------------------------------------------------
    # Phase 1: Query Gaia to compute WCS offset, then apply it to ALL
    # source coordinates BEFORE any catalog matching begins.
    #
    # Why: The WCS solution from ASTAP can have a residual systematic
    # offset (typically < 30"). If we don't correct it first, Simbad and
    # 2MASS will match against wrong coordinates and return zero results.
    # Gaia is used because it is dense enough to compute a robust
    # statistical offset via vote accumulator even with a large initial
    # error. The corrected coordinates are written back to source["ra"]
    # and source["dec"] so all subsequent stages benefit automatically.
    # ------------------------------------------------------------------
    gaia_stars: list[dict] = []
    try:
        gaia_stars = _gaia._query_gaia(ra_center, dec_center, fov_deg)
        logger.info(
            "Gaia query: ra=%.4f dec=%.4f fov=%.4f° radius=%.4f° → %d catalog stars  fits_filename=%s",
            ra_center, dec_center, fov_deg, fov_deg * math.sqrt(2) / 2.0, len(gaia_stars), fits_filename,
        )
    except Exception as exc:
        logger.warning("Gaia query failed for fits_filename=%s: %s", fits_filename, exc)

    # Compute WCS offset and apply to source coordinates in-place
    try:
        offset_ra_deg, offset_dec_deg = _wcs_offset._compute_wcs_offset(sources, gaia_stars)
        if offset_ra_deg != 0.0 or offset_dec_deg != 0.0:
            for source in sources:
                source["ra"]  += offset_ra_deg
                source["dec"] += offset_dec_deg
                source["_wcs_offset_ra"]  = offset_ra_deg
                source["_wcs_offset_dec"] = offset_dec_deg
            logger.info(
                "Applied WCS correction dRA=%.2f\" dDec=%.2f\" to %d sources  fits_filename=%s",
                offset_ra_deg * 3600.0, offset_dec_deg * 3600.0, len(sources), fits_filename,
            )

            # Post-correction validation: re-match corrected coordinates
            # against Gaia to confirm the correction actually improved
            # positions.  The pre-correction median_sep was already logged
            # by _compute_wcs_offset() above ("Gaia match (raw): ..."), so
            # the operator can compare the two numbers at a glance.
            if gaia_stars and len(sources) >= 3:
                try:
                    corrected_coords = SkyCoord(
                        ra=[s["ra"] for s in sources] * u.deg,
                        dec=[s["dec"] for s in sources] * u.deg,
                    )
                    gaia_coords_val = SkyCoord(
                        ra=[g["ra"] for g in gaia_stars] * u.deg,
                        dec=[g["dec"] for g in gaia_stars] * u.deg,
                    )
                    _, sep_post, _ = corrected_coords.match_to_catalog_sky(gaia_coords_val)
                    sep_post_arcsec = sep_post.to(u.arcsec).value
                    median_post = float(np.median(sep_post_arcsec))
                    within_cone = int(np.sum(sep_post_arcsec <= config.MATCH_CONE_ARCSEC))
                    logger.info(
                        "WCS offset validation (post-correction): median=%.2f\", "
                        "within %.1f\"=%d/%d  fits_filename=%s",
                        median_post, config.MATCH_CONE_ARCSEC,
                        within_cone, len(sources), fits_filename,
                    )
                except Exception:
                    pass  # validation is best-effort — never block matching
    except Exception as exc:
        logger.warning("WCS offset computation failed for fits_filename=%s: %s", fits_filename, exc)

    # ------------------------------------------------------------------
    # Phase 2: Match catalogs in order using corrected coordinates.
    # Simbad → Gaia DR3 → 2MASS → MPC
    # ------------------------------------------------------------------

    # --- 1. Simbad (named objects with rich type info) ---
    simbad_objects: list[dict] = []
    try:
        simbad_objects = _simbad._query_simbad(ra_center, dec_center, fov_deg)
        logger.info(
            "Simbad query: ra=%.4f dec=%.4f fov=%.4f° → %d objects  fits_filename=%s",
            ra_center, dec_center, fov_deg, len(simbad_objects), fits_filename,
        )
        _simbad._match_simbad(sources, simbad_objects)
    except Exception as exc:
        logger.warning("Simbad matching stage failed for fits_filename=%s: %s", fits_filename, exc)

    # --- 2. Gaia DR3 (dense stellar catalog, WCS offset already applied) ---
    try:
        _gaia._match_gaia(sources, gaia_stars)
    except Exception as exc:
        logger.warning("Gaia matching stage failed for fits_filename=%s: %s", fits_filename, exc)

    # --- 3. 2MASS (fallback for red/cool stars absent in Gaia) ---
    twomass_stars: list[dict] = []
    try:
        twomass_stars = _2mass._query_2mass(ra_center, dec_center, fov_deg)
        logger.info(
            "2MASS query: ra=%.4f dec=%.4f fov=%.4f° → %d catalog stars  fits_filename=%s",
            ra_center, dec_center, fov_deg, len(twomass_stars), fits_filename,
        )
        _2mass._match_2mass(sources, twomass_stars)
    except Exception as exc:
        logger.warning("2MASS matching stage failed for fits_filename=%s: %s", fits_filename, exc)

    # --- 4. Pan-STARRS DR1 (deep optical catalog, dec > -30°; catches faint sources
    #        below Gaia completeness limit ~21 mag, reduces false UNKNOWN alerts) ---
    ps_sources: list[dict] = []
    try:
        ps_sources = _panstarrs._query_panstarrs(ra_center, dec_center, fov_deg)
        if ps_sources:
            logger.info(
                "Pan-STARRS query: ra=%.4f dec=%.4f fov=%.4f° → %d sources  fits_filename=%s",
                ra_center, dec_center, fov_deg, len(ps_sources), fits_filename,
            )
        _panstarrs._match_panstarrs(sources, ps_sources)
    except Exception as exc:
        logger.warning("Pan-STARRS matching stage failed for fits_filename=%s: %s", fits_filename, exc)

    # --- 5. MPC / SkyBot (solar system objects; wider cone) ---
    mpc_objects: list[dict] = []
    try:
        mpc_objects = _mpc._query_mpc(ra_center, dec_center, obs_time, fov_deg)
        _mpc._match_mpc(sources, mpc_objects)
    except Exception as exc:
        logger.warning("MPC/SkyBot matching stage failed for fits_filename=%s: %s", fits_filename, exc)

    n_simbad     = sum(1 for s in sources if s["catalog_name"] == "Simbad")
    n_gaia       = sum(1 for s in sources if s["catalog_name"] == "Gaia DR3")
    n_2mass      = sum(1 for s in sources if s["catalog_name"] == "2MASS")
    n_panstarrs  = sum(1 for s in sources if s["catalog_name"] == "Pan-STARRS")
    n_mpc        = sum(1 for s in sources if s["catalog_name"] == "MPC")
    n_unmatched  = sum(1 for s in sources if s["catalog_name"] is None)

    logger.info(
        "Catalog matching: %d sources — Simbad: %d, Gaia: %d, 2MASS: %d, Pan-STARRS: %d, MPC: %d, unmatched: %d  fits_filename=%s",
        len(sources), n_simbad, n_gaia, n_2mass, n_panstarrs, n_mpc, n_unmatched, fits_filename,
    )

    # Warn when very few sources match any stellar catalog — expected for fields at
    # high galactic latitude (galaxy clusters) where most detections are compact
    # galaxies rather than stars. If you expect more matches, check:
    #   1. Galactic latitude of the target (|b| > 60° → few stars, many galaxies)
    #   2. STAR_SNR_MIN threshold — lowering it detects fainter stars
    #   3. Run on a Milky Way field to verify the pipeline works for star-rich frames
    if len(sources) > 0:
        n_stellar = n_simbad + n_gaia + n_2mass + n_panstarrs
        match_rate = n_stellar / len(sources)
        if match_rate < 0.05 and len(sources) >= 20:
            logger.warning(
                "Low catalog match rate: %.1f%% (%d/%d sources matched Simbad/Gaia/2MASS). "
                "This is expected for high-galactic-latitude fields where most detections "
                "are compact galaxies not present in stellar catalogs.  fits_filename=%s",
                match_rate * 100, n_stellar, len(sources), fits_filename,
            )

    return sources
