"""
modules/forced_photometry.py — Forced photometry / reverse matching (precovery).

The single public entry point is:

    await forced_photometry.run(...) -> list[dict]

A second, independent pass run AFTER catalog_matcher.py's forward matching
(and photometry.py's zero-point calibration): for every Gaia DR3 star and
every MPC/SkyBot object within this frame's footprint that has no
corresponding entry in `sources`, measure the flux at that exact predicted
pixel position anyway, instead of silently treating it as "not detected".
Originally proposed as ROADMAP.md #1 (see git log for that history). This closes
two gaps forward matching alone leaves open:

  - A star/object genuinely too faint for the blind SEP extraction's
    necessarily-high detection threshold (SEP_DETECT_THRESH) to have found
    at all ("precovery" in MPC terminology for solar-system objects). Forced
    photometry tests exactly one hypothesis (a specific known position)
    rather than scanning every independent resolution element in the frame
    for an unknown number of sources, so a much lower significance is
    statistically justified here (the "look-elsewhere effect" — see
    FORCED_PHOTOMETRY_MIN_SNR in config.py).
  - A star bright enough to detect that the blind extractor's own star
    filter (elongation/FWHM/SNR bounds), a WCS residual, or streak masking
    happened to miss anyway — forced photometry recovers these "for free"
    since it never depends on SEP having found the source in the first
    place.

Scope decisions for this pass (see the "Open considerations" in ROADMAP.md #1's
original proposal — git log — for the tradeoffs behind them):
  - Catalogs: Gaia DR3 + MPC/SkyBot only. 2MASS/Pan-STARRS are not forced
    here (still available via forward matching in catalog_matcher.py) —
    left for a possible future pass.
  - A genuine non-detection (flux consistent with zero within the noise,
    below FORCED_PHOTOMETRY_MIN_SNR) is silently dropped, not reported as an
    "upper limit" — the API's source payload (docs/API.md §2) has no field
    to distinguish a real magnitude from an upper limit, and adding one
    would be a separate, cross-repo change to observatory-api's schema, not
    made here.
  - No new network queries: the Gaia/MPC field lists are the exact same ones
    catalog_matcher.match() already fetched for forward matching (see that
    module's get_gaia_stars()/get_mpc_objects(), which are cache hits when
    called right after match() in the same frame's processing) — this
    module only ever does local pixel-position math and aperture photometry
    on data already on disk.

Reuses aperture-photometry math from modules/photometry.py (aperture/annulus
sizing from FWHM, net-flux and flux-error formulas) — duplicated by hand
rather than imported, the same convention modules/qc.py and
modules/subtraction.py already use for astrometry.py's streak-mask helper:
kept in sync by hand rather than introducing a shared dependency between two
independently evolving detection paths.

Errors anywhere in this module are caught and logged; a failure here never
crashes frame processing — same convention as every other pipeline stage.
"""

from __future__ import annotations

import logging
import math
import os

import astropy.io.fits as fits
import numpy as np
from astropy.stats import sigma_clipped_stats
from astropy.time import Time
from astropy.wcs import WCS
from photutils.aperture import ApertureStats, CircularAnnulus, CircularAperture, aperture_photometry

import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _propagate_gaia_position(star: dict, obs_jyear: float | None) -> tuple[float, float]:
    """
    Return (ra, dec) in decimal degrees, proper-motion-corrected from the
    star's Gaia ref_epoch to obs_jyear when both pmra/pmdec and obs_jyear are
    available; otherwise returns the star's catalog position unchanged.

    pmra is Gaia's own convention: proper motion in the RA direction already
    multiplied by cos(dec) (mu_alpha* = dalpha/dt * cos(dec)), so dividing
    back out by cos(dec) here recovers the actual angular RA offset. Ignores
    parallax and perspective acceleration — this is meant to land an
    aperture on the right star, not to be a precision astrometric
    correction.
    """
    ra = float(star["ra"])
    dec = float(star["dec"])
    pmra = star.get("pmra")
    pmdec = star.get("pmdec")
    if obs_jyear is None or pmra is None or pmdec is None:
        return ra, dec

    dt_years = obs_jyear - float(star.get("ref_epoch") or 2016.0)
    if dt_years == 0.0:
        return ra, dec

    cos_dec = math.cos(math.radians(dec))
    if abs(cos_dec) < 1e-9:
        return ra, dec  # at the pole — cos(dec) division would blow up; skip correction

    d_ra_deg = (pmra / 1000.0 / 3600.0) * dt_years / cos_dec
    d_dec_deg = (pmdec / 1000.0 / 3600.0) * dt_years

    return ra + d_ra_deg, dec + d_dec_deg


def _measure_at_pixel(
    data_sub: np.ndarray,
    raw_data: np.ndarray,
    x_px: float,
    y_px: float,
    ap_radius: float,
    annulus_inner: float,
    annulus_outer: float,
    sky_sigma: float,
) -> tuple[float, float] | None:
    """
    Aperture-photometer a single fixed position. Mirrors modules/photometry.py's
    per-source measurement math (aperture/annulus sizing, net-flux, flux-error
    formulas) — duplicated by hand rather than shared, same convention as the
    streak-mask helper duplicated between astrometry.py/qc.py/subtraction.py.

    Returns (net_flux, flux_err), or None if the annulus falls even partially
    outside data_sub's bounds, or any pixel under the aperture is at/above
    SATURATION_ADU — a forced measurement on a saturated core is exactly as
    physically meaningless as it is for a blindly-detected source (see
    photometry.py's own saturated-source handling).
    """
    naxis2, naxis1 = data_sub.shape

    r = int(math.ceil(annulus_outer)) + 1
    x0, x1 = int(x_px) - r, int(x_px) + r + 1
    y0, y1 = int(y_px) - r, int(y_px) + r + 1
    if x0 < 0 or y0 < 0 or x1 > naxis1 or y1 > naxis2:
        return None  # too close to the edge for the full annulus to fit

    if float(np.max(raw_data[y0:y1, x0:x1])) >= config.SATURATION_ADU:
        return None

    position = (x_px, y_px)
    aperture = CircularAperture(position, r=ap_radius)
    annulus = CircularAnnulus(position, r_in=annulus_inner, r_out=annulus_outer)

    ann_stats = ApertureStats(data_sub, annulus)
    sky_per_px = float(ann_stats.median)

    phot_table = aperture_photometry(data_sub, aperture)
    ap_sum = float(phot_table["aperture_sum"][0])
    ap_area = float(aperture.area)

    net_flux = ap_sum - sky_per_px * ap_area
    flux_err = math.sqrt(abs(net_flux) + ap_area * sky_sigma ** 2)
    return net_flux, flux_err


def _build_result(
    ra: float,
    dec: float,
    catalog_name: str,
    catalog_id: str,
    catalog_mag: float | None,
    object_type: str,
    net_flux: float,
    flux_err: float,
    mag_instrumental: float,
    mag_err: float,
    zero_point: float | None,
    zero_point_err: float | None,
    fwhm_arcsec: float | None,
    near_edge: bool,
) -> dict:
    """Assemble one forced-photometry result in the same shape as an ordinary source dict."""
    calibrated = zero_point is not None
    return {
        "ra": ra,
        "dec": dec,
        "flux": net_flux,
        "fwhm": fwhm_arcsec,
        "elongation": 1.0,  # not independently measured — a fixed aperture assumes a point source
        "saturated": False,  # _measure_at_pixel() already rejects a saturated position outright
        "near_edge": near_edge,
        "catalog_name": catalog_name,
        "catalog_id": catalog_id,
        "catalog_mag": catalog_mag,
        "object_type": object_type,
        "flux_aperture": net_flux,
        "flux_err": flux_err,
        "mag_instrumental": mag_instrumental,
        "mag_calibrated": mag_instrumental + zero_point if calibrated else None,
        "mag_err": mag_err,
        "calibrated": calibrated,
        "edge_flag": near_edge,
        "zero_point": zero_point,
        "zero_point_err": zero_point_err,
        "_from_subtraction": False,
        # Leading underscore — internal-only for v1 (see module docstring on
        # why this is not yet persisted on the wire); api_client's
        # _to_wire_source() strips it before POST /frames/{id}/sources.
        "_forced_photometry": True,
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def run(
    fits_path: str,
    sources: list[dict],
    gaia_stars: list[dict],
    mpc_objects: list[dict],
    wcs: WCS | None,
    naxis1: int | None,
    naxis2: int | None,
    zero_point: float | None,
    zero_point_err: float | None,
    obs_time: str | None,
    psf_fwhm_arcsec: float | None = None,
) -> list[dict]:
    """
    Force-measure every catalog star/MPC object not already present in
    `sources`, at its predicted pixel position.

    Parameters
    ----------
    fits_path:
        Absolute path to the FITS file — opened again here (same convention
        as photometry.py) for the raw pixel data.
    sources:
        This frame's already-matched source list (astrometry.solve() +
        catalog_matcher.match(), post-dedup) — used only to build the set of
        already-catalogued identities to skip; never mutated.
    gaia_stars:
        The Gaia DR3 field list from catalog_matcher.get_gaia_stars() — the
        same list match() already fetched for forward matching/WCS offset
        correction.
    mpc_objects:
        The MPC/SkyBot field list from catalog_matcher.get_mpc_objects() —
        already filtered by MPC_MAG_LIMIT (see that module's _query_mpc()),
        so no separate depth cutoff is applied to it here.
    wcs:
        This frame's solved WCS (astrometry.solve()'s `wcs`). Nothing here
        runs when this is None — the pass is meaningless without a plate
        solve.
    naxis1, naxis2:
        Frame dimensions in pixels, for the near_edge geometry flag (same
        convention as astrometry.py/subtraction.py). Recomputed from the
        opened FITS data if not provided.
    zero_point, zero_point_err:
        This frame's Gaia DR3 photometric zero-point from photometry.py's
        own calibration step (pipeline.py reads it off any already-measured
        source's "zero_point" field). None skips calibration exactly like an
        ordinary source with too few Gaia references — mag_calibrated stays
        None for every forced measurement in that case.
    obs_time:
        ISO 8601 observation timestamp — used to proper-motion-correct Gaia
        positions to this frame's actual epoch. MPC positions need no such
        correction; SkyBot already returns them at the exact obs_time.
    psf_fwhm_arcsec:
        This frame's own measured stellar PSF FWHM (qc.py's fwhm_median, in
        arcsec — same value astrometry.solve()/subtraction.run() receive).
        Sets the fixed aperture/annulus radii, same formula as
        photometry.py. Falls back to a fixed 3-pixel FWHM assumption when
        unavailable (mirrors photometry.py's own fallback).

    Returns
    -------
    list[dict]
        New source dicts, shaped like an ordinary catalog-matched source —
        ready to be appended to `sources` and flow through pipeline.py's
        existing "mag"/"_filter" tagging, API posting, and anomaly detection
        unchanged. [] on any failure, when FORCED_PHOTOMETRY_ENABLED is
        false, or when there is nothing eligible to force.

        A measurement whose significance (net_flux / flux_err) is below
        FORCED_PHOTOMETRY_MIN_SNR is a non-detection and is NOT included —
        see this module's docstring for why it isn't reported as an upper
        limit either.
    """
    fits_filename = os.path.basename(fits_path)

    if not config.FORCED_PHOTOMETRY_ENABLED:
        return []

    if wcs is None or (not gaia_stars and not mpc_objects):
        return []

    already_matched_gaia = {
        s["catalog_id"] for s in sources
        if s.get("catalog_name") == "Gaia DR3" and s.get("catalog_id")
    }
    already_matched_mpc = {
        s["catalog_id"] for s in sources
        if s.get("catalog_name") == "MPC" and s.get("catalog_id")
    }

    eligible_gaia = [
        g for g in gaia_stars
        if g.get("source_id") not in already_matched_gaia
        and g.get("phot_g_mean_mag", math.inf) <= config.FORCED_PHOTOMETRY_MAG_LIMIT
    ]
    eligible_mpc = [
        m for m in mpc_objects
        if m.get("designation") not in already_matched_mpc
    ]

    if not eligible_gaia and not eligible_mpc:
        return []

    try:
        with fits.open(fits_path, mode="readonly", ignore_missing_simple=True) as hdul:
            raw_data = hdul[0].data
    except Exception as exc:
        logger.warning("forced_photometry: failed to open %s: %s", fits_path, exc)
        return []

    if raw_data is None:
        return []

    try:
        data = np.ascontiguousarray(raw_data.astype(np.float64))
        _, sky_median, sky_sigma = sigma_clipped_stats(data, sigma=3.0)
        sky_median = float(sky_median)
        sky_sigma = float(sky_sigma)
    except Exception as exc:
        logger.warning("forced_photometry: background stats failed for %s: %s", fits_path, exc)
        return []

    data_sub = data - sky_median
    naxis2_actual, naxis1_actual = data.shape
    naxis1 = naxis1 or naxis1_actual
    naxis2 = naxis2 or naxis2_actual

    try:
        ps_matrix = wcs.pixel_scale_matrix
        pixel_scale_arcsec = float(np.sqrt(ps_matrix[0, 0] ** 2 + ps_matrix[1, 0] ** 2)) * 3600.0
    except Exception:
        pixel_scale_arcsec = 0.0

    if psf_fwhm_arcsec and pixel_scale_arcsec > 0:
        fwhm_px = psf_fwhm_arcsec / pixel_scale_arcsec
    else:
        fwhm_px = 3.0  # same fallback as photometry.py's measure()

    ap_radius = 2.0 * fwhm_px
    annulus_inner = 4.0 * fwhm_px
    annulus_outer = 6.0 * fwhm_px

    margin_x = config.EDGE_MARGIN_FRAC * naxis1
    margin_y = config.EDGE_MARGIN_FRAC * naxis2

    obs_jyear: float | None = None
    if obs_time:
        try:
            obs_jyear = Time(obs_time).jyear
        except Exception as exc:
            logger.debug("forced_photometry: could not parse obs_time=%r: %s", obs_time, exc)

    results: list[dict] = []
    n_below_snr = 0
    n_unmeasurable = 0

    def _try_measure(ra: float, dec: float) -> tuple[float, float, float, float] | None:
        """Project (ra, dec) to a pixel and measure it, or None if out of bounds/unmeasurable."""
        try:
            x_px, y_px = (float(v) for v in wcs.all_world2pix([[ra, dec]], 0)[0])
        except Exception:
            return None
        if not (0 <= x_px < naxis1 and 0 <= y_px < naxis2):
            return None
        measured = _measure_at_pixel(
            data_sub, data, x_px, y_px, ap_radius, annulus_inner, annulus_outer, sky_sigma,
        )
        if measured is None:
            return None
        net_flux, flux_err = measured
        return x_px, y_px, net_flux, flux_err

    # ------------------------------------------------------------------
    # Gaia DR3 — proper-motion-corrected position, standard star photometry
    # ------------------------------------------------------------------
    for star in eligible_gaia:
        ra, dec = _propagate_gaia_position(star, obs_jyear)
        measured = _try_measure(ra, dec)
        if measured is None:
            n_unmeasurable += 1
            continue
        x_px, y_px, net_flux, flux_err = measured

        if flux_err <= 0 or net_flux <= 0 or (net_flux / flux_err) < config.FORCED_PHOTOMETRY_MIN_SNR:
            n_below_snr += 1
            continue

        near_edge = (
            x_px < margin_x or x_px > naxis1 - margin_x
            or y_px < margin_y or y_px > naxis2 - margin_y
        )
        results.append(_build_result(
            ra, dec, "Gaia DR3", star["source_id"], star["phot_g_mean_mag"], "STAR",
            net_flux, flux_err,
            -2.5 * math.log10(net_flux), 1.0857 * flux_err / net_flux,
            zero_point, zero_point_err, psf_fwhm_arcsec, near_edge,
        ))

    # ------------------------------------------------------------------
    # MPC / SkyBot — position already at obs_time, no PM correction needed
    # ------------------------------------------------------------------
    for obj in eligible_mpc:
        ra, dec = float(obj["ra"]), float(obj["dec"])
        measured = _try_measure(ra, dec)
        if measured is None:
            n_unmeasurable += 1
            continue
        x_px, y_px, net_flux, flux_err = measured

        if flux_err <= 0 or net_flux <= 0 or (net_flux / flux_err) < config.FORCED_PHOTOMETRY_MIN_SNR:
            n_below_snr += 1
            continue

        near_edge = (
            x_px < margin_x or x_px > naxis1 - margin_x
            or y_px < margin_y or y_px > naxis2 - margin_y
        )
        results.append(_build_result(
            ra, dec, "MPC", obj["designation"], None, obj.get("object_type") or "ASTEROID",
            net_flux, flux_err,
            -2.5 * math.log10(net_flux), 1.0857 * flux_err / net_flux,
            zero_point, zero_point_err, psf_fwhm_arcsec, near_edge,
        ))

    if results or n_below_snr or n_unmeasurable:
        logger.info(
            "Forced photometry: %d eligible Gaia + %d eligible MPC position(s) -> "
            "%d recovered, %d below FORCED_PHOTOMETRY_MIN_SNR=%.1f, %d unmeasurable "
            "(saturated/edge/out-of-bounds)  file=%s",
            len(eligible_gaia), len(eligible_mpc), len(results),
            n_below_snr, config.FORCED_PHOTOMETRY_MIN_SNR, n_unmeasurable,
            fits_filename,
        )

    return results
