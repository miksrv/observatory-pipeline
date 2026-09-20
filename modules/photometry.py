"""
modules/photometry.py — Aperture photometry and magnitude calibration for FITS frames.

The single public entry point is:

    await photometry.measure(fits_path: str, sources: list[dict]) -> list[dict]

It performs aperture photometry on each source using photutils, calibrates
instrumental magnitudes via differential photometry against Gaia DR3 reference
stars in the field, and returns the enriched source list with photometry fields
added.

On failure to open the FITS file or build a valid WCS, the function returns the
input source list with all photometry fields set to None rather than raising.
Individual source errors are caught per-source and also result in None values.
"""

from __future__ import annotations

import logging
import math
import os
from typing import Any, NamedTuple

import astropy.io.fits as fits
import numpy as np
from astropy.stats import sigma_clipped_stats
from astropy.wcs import WCS
from photutils.aperture import (
    ApertureStats,
    CircularAnnulus,
    CircularAperture,
    aperture_photometry,
)

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Photometry output keys — always present in every output source dict
# ---------------------------------------------------------------------------

_PHOT_KEYS: tuple[str, ...] = (
    "flux_aperture",
    "flux_err",
    "mag_instrumental",
    "mag_calibrated",
    "mag_err",
    "snr",
    "calibrated",
    "edge_flag",
    "zero_point",
    "zero_point_err",
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _null_phot_fields(calibrated: bool = False) -> dict[str, Any]:
    """Return a dict with all photometry output keys set to their null values."""
    return {
        "flux_aperture":    None,
        "flux_err":         None,
        "mag_instrumental": None,
        "mag_calibrated":   None,
        "mag_err":          None,
        "snr":              None,
        "calibrated":       calibrated,
        "edge_flag":        False,
        "zero_point":       None,
        "zero_point_err":   None,
    }


def _inject_nulls(
    sources: list[dict],
    zero_point: float | None = None,
    zero_point_err: float | None = None,
) -> list[dict]:
    """
    Return a copy of *sources* with all photometry fields set to None.

    Used for the whole-frame fallback when FITS or WCS loading fails.
    ``zero_point`` and ``zero_point_err`` are forwarded when known (normally
    they will also be None in fallback scenarios).
    """
    result: list[dict] = []
    for src in sources:
        out = dict(src)
        out.update(_null_phot_fields())
        out["zero_point"]     = zero_point
        out["zero_point_err"] = zero_point_err
        result.append(out)
    return result


# Plausible range for a real sensor's electrons-per-ADU conversion factor.
# CCDs sit around 0.5-2 e-/ADU and CMOS sensors from well under 1 to a few;
# nothing real reaches either end of this window. A header value outside it
# is not a conversion factor at all — overwhelmingly it is a CMOS camera's
# own gain SETTING in arbitrary vendor units written to GAIN (0-500 on a ZWO
# ASI, for instance), which would corrupt the flux error far worse than the
# 1.0 assumption it replaced. See config.PHOTOMETRY_GAIN_E_PER_ADU.
_GAIN_MIN_E_PER_ADU: float = 0.05
_GAIN_MAX_E_PER_ADU: float = 20.0


def _resolve_gain(
    hdr: Any,
    fits_filename: str,
    override: float | None = None,
) -> float:
    """
    Resolve this frame's sensor gain in electrons per ADU, for the Poisson
    term of the aperture flux error.

    Order of preference: an explicit caller-supplied `override`, then
    config.PHOTOMETRY_GAIN_E_PER_ADU, then the frame's own header — EGAIN
    first, GAIN second. That order matters: on most CMOS cameras EGAIN is the
    true conversion factor while GAIN holds the camera's own gain setting in
    arbitrary vendor units. Falls back to 1.0 (the value the formula
    implicitly assumed before this existed) whenever nothing usable is
    available or the candidate falls outside the plausible e-/ADU range.

    Duplicated by hand in modules/forced_photometry.py rather than imported,
    the same convention already used for this module's aperture/net-flux
    formulas there and for the streak-mask helper shared between
    astrometry/qc/subtraction.
    """
    candidates: list[tuple[str, Any]] = []
    if override is not None:
        candidates.append(("caller", override))
    if config.PHOTOMETRY_GAIN_E_PER_ADU is not None:
        candidates.append(("PHOTOMETRY_GAIN_E_PER_ADU", config.PHOTOMETRY_GAIN_E_PER_ADU))
    if hdr is not None:
        for key in ("EGAIN", "GAIN"):
            try:
                value = hdr.get(key)
            except Exception:
                value = None
            if value is not None:
                candidates.append((key, value))

    for origin, value in candidates:
        try:
            gain = float(value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(gain):
            continue
        if _GAIN_MIN_E_PER_ADU <= gain <= _GAIN_MAX_E_PER_ADU:
            logger.debug(
                "photometry: gain=%.4f e-/ADU (from %s)  file=%s",
                gain, origin, fits_filename,
            )
            return gain
        logger.warning(
            "photometry: %s=%s is outside the plausible %.2f-%.1f e-/ADU range "
            "— ignoring it (a CMOS camera's GAIN keyword is usually its gain "
            "SETTING, not a conversion factor); set PHOTOMETRY_GAIN_E_PER_ADU "
            "explicitly if you know the real value  file=%s",
            origin, value, _GAIN_MIN_E_PER_ADU, _GAIN_MAX_E_PER_ADU,
            fits_filename,
        )

    logger.debug(
        "photometry: no usable gain available — assuming 1.0 e-/ADU  file=%s",
        fits_filename,
    )
    return 1.0


def _pixel_scale_from_wcs(wcs: WCS) -> float:
    """
    Derive the plate scale in arcsec/pixel from a WCS object.

    Uses the column-norm of the pixel_scale_matrix (handles rotation / shear).
    The result matches the formula used in astrometry.py for consistency.
    """
    ps_matrix = wcs.pixel_scale_matrix   # (2, 2), units deg/px
    pixel_scale_deg: float = float(
        np.sqrt(ps_matrix[0, 0] ** 2 + ps_matrix[1, 0] ** 2)
    )
    return pixel_scale_deg * 3600.0      # arcsec/px


class _ZeroPoint(NamedTuple):
    """
    The frame's photometric solution: an offset, its uncertainty, and the
    colour term that offset is defined at.

    `color_term` is 0.0 and `color_ref` is None whenever no colour fit was
    made (disabled, too few references carrying a Gaia BP-RP colour, too
    narrow a colour span to constrain a slope, or an implausible fitted
    slope) — in that case this behaves exactly like the plain median offset
    this module computed before colour terms existed.
    """

    zero_point: float | None
    zero_point_err: float | None
    color_term: float
    color_ref: float | None
    color_scatter: float


def _robust_color_fit(
    colors: np.ndarray,
    deltas: np.ndarray,
    color_ref: float,
) -> tuple[float, float, float] | None:
    """
    Fit ``delta = zp + k * (color - color_ref)`` with three passes of 3-sigma
    clipping, returning ``(zp, k, residual_sigma)`` — or None when the fit
    collapses (too few survivors, or a degenerate slope).

    Clipping rather than a plain least-squares fit because the reference set
    routinely contains a blended pair, an unflagged variable, or a star whose
    aperture caught a cosmic ray; a single such point can tilt a slope fitted
    over a modest colour baseline. The residual scatter is reported as a
    MAD-derived sigma for the same reason the scatter elsewhere in this
    pipeline is (see anomaly_detector's `_history_mag_scatter()`).
    """
    keep = np.ones(len(colors), dtype=bool)
    x = colors - color_ref

    fit: tuple[float, float, float] | None = None
    for _ in range(3):
        if int(keep.sum()) < config.PHOTOMETRY_COLOR_TERM_MIN_REFS:
            return fit
        try:
            k, zp = np.polyfit(x[keep], deltas[keep], 1)
        except Exception:
            return fit

        resid = deltas - (zp + k * x)
        sigma = 1.4826 * float(np.median(np.abs(resid[keep] - np.median(resid[keep]))))
        fit = (float(zp), float(k), sigma)

        if sigma <= 0.0:
            return fit
        new_keep = np.abs(resid - np.median(resid[keep])) <= 3.0 * sigma
        if int(new_keep.sum()) < config.PHOTOMETRY_COLOR_TERM_MIN_REFS:
            return fit
        if np.array_equal(new_keep, keep):
            return fit
        keep = new_keep

    return fit


def _compute_zero_point(sources: list[dict]) -> _ZeroPoint:
    """
    Compute the differential photometry zero-point from Gaia DR3 reference stars.

    Requires at least 3 sources with ``catalog_name == "Gaia DR3"``,
    a finite ``catalog_mag``, and a finite ``mag_instrumental``.

    When enough of those references also carry a Gaia BP-RP colour spanning a
    wide enough range, the offset is fitted as a line in colour rather than
    taken as a single median. A star's instrumental magnitude in R (or B, V,
    I) differs from its Gaia broadband G magnitude by an amount that depends
    on the star's own colour; collapsing that into one constant leaves a
    systematic bias in every `mag_calibrated`, and the bias moves night to
    night with whatever mix of red and blue stars the field supplied — enough
    to shift many stars in one epoch together past `DELTA_MAG_ALERT` and read
    as a frame-wide variability signal (audit 2026-08-18, finding H5).

    The zero point is reported **at the reference colour** (the reference
    set's own median BP-RP), so it keeps meaning "the offset for a typical
    star in this field" and a source whose colour is unknown can still use it
    directly — see `measure()`, which inflates such a source's `mag_err` by
    the colour term's own reach instead.

    Returns
    -------
    _ZeroPoint
        ``zero_point``/``zero_point_err`` are None when fewer than 3 valid
        references are available. ``color_term`` is 0.0 (and ``color_ref``
        None) whenever no colour fit was made, in which case the result is
        identical to the plain median this function returned before.
    """
    deltas: list[float] = []
    colors: list[float] = []
    for src in sources:
        if src.get("catalog_name") != "Gaia DR3":
            continue
        # A saturated star's aperture flux is not a physically meaningful
        # measurement (its PSF core is clipped), so it must never be used as
        # a zero-point reference even if it happens to be Gaia-matched —
        # otherwise it would corrupt the zero-point for every other source
        # in the frame. See docs/ISSUES.md #2.
        if src.get("saturated"):
            continue
        cat_mag = src.get("catalog_mag")
        inst_mag = src.get("mag_instrumental")
        if cat_mag is None or inst_mag is None:
            continue
        if not (math.isfinite(cat_mag) and math.isfinite(inst_mag)):
            continue
        deltas.append(cat_mag - inst_mag)

        color = src.get("_catalog_color")
        colors.append(
            float(color)
            if color is not None and math.isfinite(float(color))
            else float("nan")
        )

    if len(deltas) < 3:
        logger.warning(
            "photometry: only %d Gaia DR3 reference stars available "
            "(need >= 3) — mag_calibrated will be None for all sources",
            len(deltas),
        )
        return _ZeroPoint(None, None, 0.0, None, 0.0)

    arr = np.array(deltas, dtype=np.float64)
    col = np.array(colors, dtype=np.float64)
    zp: float = float(np.median(arr))
    # Median absolute deviation (no scipy dependency)
    mad: float = float(np.median(np.abs(arr - zp)))

    # ------------------------------------------------------------------
    # Colour term
    # ------------------------------------------------------------------
    has_color = np.isfinite(col)
    n_color = int(has_color.sum())
    color_ref: float | None = None
    color_term = 0.0
    color_scatter = 0.0

    if n_color >= 2:
        color_ref = float(np.median(col[has_color]))
        color_scatter = 1.4826 * float(
            np.median(np.abs(col[has_color] - color_ref))
        )

    if config.PHOTOMETRY_COLOR_TERM_ENABLED and n_color >= config.PHOTOMETRY_COLOR_TERM_MIN_REFS:
        span = float(
            np.percentile(col[has_color], 90) - np.percentile(col[has_color], 10)
        )
        if span < config.PHOTOMETRY_COLOR_TERM_MIN_SPAN:
            logger.info(
                "photometry: colour span of the reference set is only %.2f mag "
                "(need >= %.2f) — a slope fitted over it would be "
                "unconstrained; using a constant zero-point",
                span, config.PHOTOMETRY_COLOR_TERM_MIN_SPAN,
            )
        else:
            fit = _robust_color_fit(col[has_color], arr[has_color], color_ref)
            if fit is None:
                logger.info("photometry: colour-term fit did not converge — using a constant zero-point")
            elif abs(fit[1]) > config.PHOTOMETRY_COLOR_TERM_MAX:
                logger.warning(
                    "photometry: fitted colour term k=%.3f mag/mag exceeds the "
                    "plausible %.2f — discarding it as a degenerate fit and "
                    "using a constant zero-point",
                    fit[1], config.PHOTOMETRY_COLOR_TERM_MAX,
                )
            else:
                zp, color_term, resid_sigma = fit
                mad = resid_sigma
                logger.info(
                    "photometry: colour term k=%.3f mag/mag at BP-RP=%.3f "
                    "(n_color=%d/%d, span=%.2f mag)",
                    color_term, color_ref, n_color, len(deltas), span,
                )

    logger.info(
        "photometry: zero_point=%.4f  zero_point_err=%.4f  "
        "n_ref_stars=%d  color_term=%.4f",
        zp,
        mad,
        len(deltas),
        color_term,
    )
    return _ZeroPoint(zp, mad, color_term, color_ref, color_scatter)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def measure(
    fits_path: str,
    sources: list[dict],
    skip_calibration: bool = False,
    gain: float | None = None,
) -> list[dict]:
    """
    Perform aperture photometry and differential magnitude calibration.

    The function is declared async for pipeline interface consistency even
    though all operations (photutils, FITS I/O) are CPU-bound and synchronous
    internally.

    Parameters
    ----------
    fits_path:
        Absolute path to the FITS file on disk.
    sources:
        Source list produced by ``astrometry.solve()``, optionally enriched by
        ``catalog_matcher.match()`` (may carry ``catalog_name`` / ``catalog_mag``
        keys).  Input dicts are never modified; copies are returned.
    skip_calibration:
        True when this frame's own filter is narrowband (Hα/[OIII]/[SII]/
        [NII] by default — see pipeline.py's caller, which derives this from
        ``modules.normalizer.is_narrowband()``). A narrowband bandpass lets
        through too few Gaia-bright stars for a reliable zero-point, and even
        a zero-point computed from the few that do pass through is
        systematically biased relative to Gaia's broadband G — comparing a
        narrowband instrumental magnitude to a Gaia G-band reference isn't a
        valid photometric calibration regardless of how many reference stars
        happen to match (see CLAUDE.md's "Filters — real astronomy context").
        Aperture photometry itself (``flux_aperture``, ``mag_instrumental``)
        still runs normally; only the Gaia zero-point step is skipped, so
        every source's ``calibrated`` stays False and ``mag_calibrated``
        stays None — same outward result as "fewer than 3 Gaia references",
        just without ever attempting the (untrustworthy) calibration at all.
    gain:
        Sensor gain in electrons per ADU for the Poisson term of the flux
        error. None (the default) resolves it from
        config.PHOTOMETRY_GAIN_E_PER_ADU, then from the frame's own
        EGAIN/GAIN header — see _resolve_gain().

    Returns
    -------
    List of source dicts with all original fields preserved plus:

        flux_aperture       float | None   net aperture flux (ADU)
        flux_err            float | None   Poisson + sky noise in quadrature
        mag_instrumental    float | None   -2.5 * log10(flux_aperture)
        mag_calibrated      float | None   mag_instrumental + zero_point,
                                            plus the fitted colour term for a
                                            source whose own Gaia BP-RP colour
                                            is known (see _compute_zero_point())
        mag_err             float | None   1.0857 * flux_err / flux_aperture
        snr                 float | None   flux_aperture / flux_err — same
                                            flux/noise convention as
                                            qc.py's snr_median and
                                            subtraction.py's candidate snr;
                                            overwrites any provisional value
                                            a source already carried (e.g.
                                            subtraction.py's own cruder
                                            pixel-space estimate) with this
                                            frame's real aperture-photometry
                                            measurement
        calibrated          bool           True when zero_point was applied
        edge_flag           bool           True when centroid is within 10 px of edge
        zero_point          float | None   frame-level ZP (same for all sources)
        zero_point_err      float | None   robust scatter of the reference
                                            stars about the fitted solution

    A source carrying ``saturated=True`` (set by ``astrometry.solve()``; see
    docs/ISSUES.md #2) is never measured — its photometry keys stay None
    just like an out-of-bounds source, since aperture flux on a saturated
    PSF core is not a physically meaningful measurement and was the root
    cause of extreme (e.g. -14) magnitudes reaching the API. Saturated
    sources are also excluded from the Gaia DR3 zero-point reference set.

    On any frame-level failure the input sources are returned with all
    photometry keys set to None.
    """
    fits_filename = os.path.basename(fits_path)
    logger.info(
        "Photometry starting: %d sources  file=%s",
        len(sources),
        fits_filename,
    )

    if not sources:
        logger.info("photometry: no sources to measure for %s", fits_filename)
        return []

    # ------------------------------------------------------------------
    # Step 1 — Open FITS file and build WCS
    # ------------------------------------------------------------------
    try:
        with fits.open(fits_path, mode="readonly", ignore_missing_simple=True) as hdul:
            hdr = hdul[0].header
            raw_data: np.ndarray = hdul[0].data
            naxis1: int = int(hdr.get("NAXIS1", 0))
            naxis2: int = int(hdr.get("NAXIS2", 0))
    except Exception as exc:
        logger.error(
            "photometry: failed to open FITS file %s: %s", fits_path, exc
        )
        return _inject_nulls(sources)

    if raw_data is None:
        logger.error(
            "photometry: primary HDU has no image data in %s", fits_path
        )
        return _inject_nulls(sources)

    data: np.ndarray = np.ascontiguousarray(raw_data.astype(np.float64))

    # Try to get WCS from FITS header first, then fallback to .wcs file
    wcs = None
    try:
        wcs = WCS(hdr)
        if not wcs.has_celestial:
            # Try to read from .wcs file that astap creates
            wcs_file_path = os.path.splitext(fits_path)[0] + ".wcs"
            if os.path.exists(wcs_file_path):
                logger.info(
                    "photometry: FITS has no celestial WCS, trying .wcs file: %s",
                    wcs_file_path,
                )
                try:
                    with fits.open(wcs_file_path) as wcs_hdul:
                        wcs_hdr = wcs_hdul[0].header
                        wcs = WCS(wcs_hdr)
                except Exception as wcs_exc:
                    logger.warning(
                        "photometry: failed to read .wcs file %s: %s",
                        wcs_file_path,
                        wcs_exc,
                    )
                    wcs = None
            
        if wcs is None or not wcs.has_celestial:
            raise ValueError("WCS has no celestial axes")
    except Exception as exc:
        logger.error(
            "photometry: invalid or missing WCS in %s: %s", fits_path, exc
        )
        return _inject_nulls(sources)

    try:
        pixel_scale_arcsec: float = _pixel_scale_from_wcs(wcs)
    except Exception as exc:
        logger.error(
            "photometry: could not derive pixel scale from WCS in %s: %s",
            fits_path, exc,
        )
        return _inject_nulls(sources)

    logger.debug(
        "photometry: pixel_scale=%.4f arcsec/px  image=%dx%d  file=%s",
        pixel_scale_arcsec,
        naxis1,
        naxis2,
        fits_filename,
    )

    # Sensor gain (e-/ADU) for the Poisson term of the flux error below.
    gain_e_per_adu: float = _resolve_gain(hdr, fits_filename, override=gain)

    # ------------------------------------------------------------------
    # Step 2 — Sky background (sigma-clipped statistics)
    # ------------------------------------------------------------------
    try:
        _, sky_median, sky_sigma = sigma_clipped_stats(data, sigma=3.0)
        sky_median = float(sky_median)
        sky_sigma  = float(sky_sigma)
    except Exception as exc:
        logger.error(
            "photometry: sigma_clipped_stats failed for %s: %s", fits_path, exc
        )
        return _inject_nulls(sources)

    data_sub: np.ndarray = data - sky_median

    logger.debug(
        "photometry: sky_median=%.2f sky_sigma=%.4f  file=%s",
        sky_median,
        sky_sigma,
        fits_filename,
    )

    # ------------------------------------------------------------------
    # Step 3 — Convert all (RA, Dec) → pixel positions in one WCS call
    # ------------------------------------------------------------------
    try:
        sky_coords = np.array(
            [[src["ra"], src["dec"]] for src in sources],
            dtype=np.float64,
        )
        pix_coords = wcs.all_world2pix(sky_coords, 0)  # shape (N, 2)
    except Exception as exc:
        logger.error(
            "photometry: WCS coordinate conversion failed for %s: %s",
            fits_path, exc,
        )
        return _inject_nulls(sources)

    # ------------------------------------------------------------------
    # Step 4 — Per-source aperture photometry
    # ------------------------------------------------------------------
    output: list[dict] = []

    for i, src in enumerate(sources):
        out = dict(src)
        out.update(_null_phot_fields())

        x_px: float = float(pix_coords[i, 0])
        y_px: float = float(pix_coords[i, 1])

        # Sources outside image bounds — skip measurement, keep nulls
        if not (0 <= x_px < naxis1 and 0 <= y_px < naxis2):
            logger.warning(
                "photometry: source %d (%s) at pixel (%.1f, %.1f) is outside "
                "image bounds (%dx%d), skipping  file=%s",
                i,
                f"ra={src['ra']:.5f} dec={src['dec']:.5f}",
                x_px, y_px,
                naxis1, naxis2,
                fits_filename,
            )
            output.append(out)
            continue

        # Edge flag: within 10 px of any border
        out["edge_flag"] = (
            x_px < 10.0
            or y_px < 10.0
            or x_px > naxis1 - 10.0
            or y_px > naxis2 - 10.0
        )

        # Saturated sources (flagged upstream by astrometry.py / carried
        # through by subtraction.py) never get a magnitude measurement:
        # aperture photometry on a clipped PSF core returns a hugely
        # inflated net_flux, which -2.5*log10() legitimately turns into an
        # extreme (e.g. -14) magnitude that is not physically meaningful.
        # See docs/ISSUES.md #2. flux_aperture/mag_* stay None, "saturated"
        # itself is already carried over from `src` via dict(src) above.
        if bool(src.get("saturated")):
            output.append(out)
            continue

        try:
            # Aperture sizes in pixels, derived from FWHM
            fwhm_arcsec: float = float(src.get("fwhm") or 0.0)
            if fwhm_arcsec > 0.0 and pixel_scale_arcsec > 0.0:
                fwhm_px: float = fwhm_arcsec / pixel_scale_arcsec
            else:
                # Fallback: assume 3-pixel FWHM if missing
                fwhm_px = 3.0

            ap_radius: float    = 2.0 * fwhm_px
            annulus_inner: float = 4.0 * fwhm_px
            annulus_outer: float = 6.0 * fwhm_px

            position = (x_px, y_px)
            aperture = CircularAperture(position, r=ap_radius)
            annulus  = CircularAnnulus(
                position, r_in=annulus_inner, r_out=annulus_outer
            )

            # Sky estimate from annulus
            ann_stats  = ApertureStats(data_sub, annulus)
            sky_per_px: float = float(ann_stats.median)

            # Aperture photometry on background-subtracted data
            phot_table = aperture_photometry(data_sub, aperture)
            ap_sum: float = float(phot_table["aperture_sum"][0])

            # Net flux after per-pixel sky correction
            ap_area: float   = float(aperture.area)
            net_flux: float  = ap_sum - sky_per_px * ap_area

            # Flux uncertainty: Poisson noise + sky noise.
            #
            # net_flux is in ADU, but photon shot noise is Poissonian in
            # ELECTRONS: N_e = net_flux * gain electrons, whose variance
            # N_e converts back to ADU as N_e / gain**2 = net_flux / gain.
            # Using net_flux directly as the variance — as this did before
            # — silently assumed exactly 1 e-/ADU, which real cameras
            # almost never are, biasing every SNR in the frame in one
            # direction or the other (audit 2026-08-18, finding C7).
            #
            # sky_sigma needs no such conversion: it is the empirical
            # per-pixel background scatter measured off this frame's own
            # ADU values, so it already carries read noise and sky shot
            # noise together in ADU.
            flux_err: float  = math.sqrt(
                abs(net_flux) / gain_e_per_adu + ap_area * sky_sigma ** 2
            )

            out["flux_aperture"] = net_flux
            out["flux_err"]      = flux_err

            # SNR of this aperture flux measurement — same "flux / flux_err"
            # convention already used by qc.py's snr_median and (as a cruder
            # pixel-space proxy) subtraction.py's own candidate snr. Computed
            # here rather than reused from astrometry.py's detection-time
            # peak/globalrms significance, since that metric is tuned for
            # star-vs-noise filtering (STAR_SNR_MIN), not for reporting the
            # actual significance of the flux this source is photometered
            # at. Not gated on net_flux > 0.0 — a low/negative net_flux with
            # a well-defined flux_err correctly yields a low/negative snr,
            # which is itself meaningful (non-detection), rather than a
            # missing value.
            if math.isfinite(flux_err) and flux_err > 0.0:
                out["snr"] = net_flux / flux_err

            # Instrumental magnitude
            if net_flux > 0.0:
                out["mag_instrumental"] = -2.5 * math.log10(net_flux)
                if math.isfinite(flux_err) and net_flux > 0.0:
                    out["mag_err"] = 1.0857 * flux_err / net_flux
            else:
                out["mag_instrumental"] = None
                out["mag_err"]          = None

            logger.debug(
                "photometry: source %d  net_flux=%.2f  flux_err=%.2f  "
                "mag_inst=%s  file=%s",
                i,
                net_flux,
                flux_err,
                f"{out['mag_instrumental']:.4f}"
                if out["mag_instrumental"] is not None else "None",
                fits_filename,
            )

        except Exception as exc:
            logger.warning(
                "photometry: per-source error at index %d (ra=%.5f dec=%.5f) "
                "in %s: %s",
                i,
                src.get("ra", float("nan")),
                src.get("dec", float("nan")),
                fits_filename,
                exc,
            )
            # flux_aperture, flux_err, mag_instrumental, mag_err, snr stay None

        output.append(out)

    # ------------------------------------------------------------------
    # Step 5 — Differential magnitude calibration
    # ------------------------------------------------------------------
    if skip_calibration:
        logger.info(
            "photometry: skipping Gaia DR3 zero-point calibration for %s — "
            "frame's filter is narrowband, so mag_calibrated stays None for "
            "every source regardless of Gaia match count (see measure()'s "
            "docstring)",
            fits_filename,
        )
        solution = _ZeroPoint(None, None, 0.0, None, 0.0)
    else:
        solution = _compute_zero_point(output)

    zero_point     = solution.zero_point
    zero_point_err = solution.zero_point_err
    n_color_corrected = 0

    for out in output:
        out["zero_point"]     = zero_point
        out["zero_point_err"] = zero_point_err
        # Leading underscore: pipeline-internal, stripped by api_client's
        # _to_wire_source(). pipeline.py reads these back off the measured
        # sources to hand the same solution to modules/forced_photometry.py,
        # exactly as it already does for zero_point/zero_point_err.
        out["_color_term"]    = solution.color_term
        out["_color_ref"]     = solution.color_ref
        out["_color_scatter"] = solution.color_scatter

        if zero_point is not None and out["mag_instrumental"] is not None:
            color = out.get("_catalog_color")
            try:
                color_f = float(color) if color is not None else None
            except (TypeError, ValueError):
                color_f = None

            if (
                solution.color_term
                and solution.color_ref is not None
                and color_f is not None
                and math.isfinite(color_f)
            ):
                # This source's own colour is known, so the transformation to
                # Gaia's G system is exact rather than assumed.
                out["mag_calibrated"] = (
                    out["mag_instrumental"]
                    + zero_point
                    + solution.color_term * (color_f - solution.color_ref)
                )
                n_color_corrected += 1
            else:
                # No colour for this source — the overwhelmingly common case
                # for exactly the sources that matter, since an uncatalogued
                # transient has no Gaia entry to take a colour from. The zero
                # point is defined at the reference set's median colour, so
                # applying it bare amounts to assuming this source has a
                # typical colour for the field. That assumption is worth what
                # the colour term can move across the field's own colour
                # spread, so it is folded into the reported uncertainty
                # instead of being left silent.
                out["mag_calibrated"] = out["mag_instrumental"] + zero_point
                if solution.color_term and out.get("mag_err") is not None:
                    color_unc = abs(solution.color_term) * solution.color_scatter
                    out["mag_err"] = math.sqrt(out["mag_err"] ** 2 + color_unc ** 2)
            out["calibrated"]     = True
        else:
            out["mag_calibrated"] = None
            out["calibrated"]     = False

    if solution.color_term:
        logger.info(
            "photometry: colour term applied per-source to %d/%d source(s); "
            "the rest use the zero point at BP-RP=%.3f with their mag_err "
            "widened by %.3f mag  file=%s",
            n_color_corrected, len(output), solution.color_ref or 0.0,
            abs(solution.color_term) * solution.color_scatter,
            fits_filename,
        )

    calibrated_count = sum(1 for o in output if o["calibrated"])
    logger.info(
        "Photometry complete: %d/%d sources measured, %d calibrated  "
        "zero_point=%s  file=%s",
        sum(1 for o in output if o["flux_aperture"] is not None),
        len(output),
        calibrated_count,
        f"{zero_point:.4f}" if zero_point is not None else "None",
        fits_filename,
    )

    return output
