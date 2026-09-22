"""
modules/subtraction.py — Image subtraction for transient and moving object detection.

Algorithm:
  1. Find N >= SUBTRACTION_MIN_FRAMES archived FITS of same object/filter.
  2. Load new frame data + WCS.
  3. Align each reference frame to the new frame using astroalign triangle matching.
  3.5. Normalize each aligned reference onto the new frame's own photometric
     scale (exposure time and sensor gain) — an archive routinely mixes
     exposure times, and stacking those in raw ADU leaves a residual at the
     position of every star in the frame (see _flux_scale_factor()).
  4. Median-stack aligned frames -> clean reference (removes cosmic rays and hot
     pixels FROM THE REFERENCE STACK — each reference's own detector-fixed
     defects get scattered to different pixels by the sky-based astroalign
     resampling, then averaged away by the median. This does NOT remove the
     NEW frame's own hot pixels, which are still sitting at their native,
     unresampled positions — see step 7's FWHM floor below for how those get
     filtered instead).
  5. diff = new_frame - reference.
  6. Mask the vicinity of any saturated pixel (new frame or a reference) —
     astroalign resampling leaves large non-Gaussian residuals there even
     under near-perfect registration, which sep would otherwise report as
     spurious bright "transients" (see docs/ISSUES.md #1, #2).
  6.5. Also mask any streak-like feature (satellite/aircraft trail present in
     the new frame but absent from the reference stack) found by a coarse,
     low-threshold pre-pass — see _build_streak_mask() and config.STREAK_* —
     before it can fragment into dozens of separate elongated candidates.
  7. Run SEP detection on positive residuals in the (masked) diff, rejecting
     candidates far sharper than this frame's own measured stellar PSF (see
     run()'s psf_fwhm_arcsec docstring) — this is what catches the new
     frame's own hot/warm pixels, which step 4 above cannot.
  8. Convert pixel coords to RA/Dec via WCS.
  9. Return candidates list.

Returns candidates with _from_subtraction=True flag for pipeline routing.
These bypass the history-check in anomaly_detector (subtraction already confirms
they are new relative to the reference stack).
"""
from __future__ import annotations

import glob
import logging
import math
import os
import re
from typing import Optional

import astropy.units as u
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.wcs import WCS
import sep

import config

logger = logging.getLogger(__name__)

# The narrowest second-moment semi-minor axis a pixel grid can express:
# 1/sqrt(12) px, the standard deviation of a uniform distribution across one
# pixel. `sep` can report a smaller — even exactly zero — `b` for a
# degenerate fit (a detection lying along a single pixel row, a cosmic-ray
# track, a bad column), and `a / b` then depends entirely on whatever
# epsilon is substituted to avoid dividing by zero. The old sentinels (1e-6
# here, 0.001 in _detect_diff_sources) turned such a fit into an elongation
# of 10^3-10^6, a number that is not a measurement of anything but clears
# every elongation threshold in the pipeline on the way to being persisted
# as the source's shape (audit 2026-08-18, finding L4).
#
# Clamping at the pixel limit instead caps the ratio at `a / 0.2887` — how
# elongated the feature would be if it were exactly one pixel wide, which is
# the most elongated it can honestly be claimed to be. A feature now has to
# be genuinely long in `a` to read as a trail, which is what the thresholds
# were written to mean.
#
# Hand-duplicated across modules/qc.py, modules/subtraction.py,
# modules/astrometry/_streak.py and modules/astrometry/_extraction.py, the
# same convention those four already follow for the streak-mask helper
# itself. Keep them in sync.
_MIN_SEMI_MINOR_PX: float = 1.0 / math.sqrt(12.0)   # ~= 0.2887

_MAX_FRAMES = 10


# ---------------------------------------------------------------------------
# Normalized-filename parsing
#
# modules/normalizer.py writes archived frames as
#     Light:          {Object}_Light_{Filter}_{Exptime}_{DateTime}[_{Seq}].fits
#     Dark/Flat/Bias: {Object}_{FrameType}_{Exptime}_{DateTime}[_{Seq}].fits
#
# Reference selection used to look for the filter as a bare substring
# (``"_HA_" in basename``), which cannot tell a field apart from the object
# name that precedes it and knows nothing about the frame type at all (audit
# 2026-08-18, finding C5). Two consequences, one of them live today: the
# pipeline archives Dark/Flat/Bias frames into the SAME per-object directory
# as the science frames, so a starless calibration frame was a perfectly
# eligible reference — and being recent, it would crowd real science frames
# out of the newest-first _MAX_FRAMES selection. The other is the token
# collision the finding is named for: under the earlier filename revision the
# FrameType codes were L/D/F/B, so ``_L_`` matched every Light frame whatever
# its actual filter, and ``_B_`` matched Bias frames when looking for Blue.
#
# Parsing positionally instead removes both, and is anchored from the RIGHT
# because the object name itself may contain underscores ("Andromeda_Galaxy",
# "4_Vesta"). The anchor is the DateTime field, the one token with a
# distinctive shape; the exposure, filter and frame-type fields then sit at
# fixed offsets before it. That also disambiguates the legacy single-letter
# codes for free — "L" in the FrameType position is Light, "L" in the filter
# position is Luminance — so an archive written by the older revision parses
# correctly too.
# ---------------------------------------------------------------------------

_FRAME_TYPE_TOKENS: dict[str, str] = {
    "LIGHT": "Light", "DARK": "Dark", "FLAT": "Flat", "BIAS": "Bias",
    # Legacy one-letter codes (pre-full-word filename revision)
    "L": "Light", "D": "Dark", "F": "Flat", "B": "Bias",
}

_CALIBRATION_FRAME_TYPES = frozenset({"Dark", "Flat", "Bias"})

# The DateTime field as build_filename() writes it: an ISO timestamp with
# ":" replaced by "-". Matched as a prefix so a trailing zone marker ("Z",
# "+00-00") or a seconds-less timestamp still anchors.
_DATETIME_TOKEN_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}")


def _parse_normalized_filename(basename: str) -> tuple[Optional[str], Optional[str]]:
    """
    Return ``(frame_type, filter_name)`` parsed out of a normalized filename,
    or ``(None, None)`` when the name doesn't follow the convention at all
    (NORMALIZE_ENABLED=false, or a file placed in the archive by hand).

    ``filter_name`` is None for a calibration frame, and also for a Light
    frame written without a filter field.
    """
    tokens = os.path.splitext(basename)[0].split("_")

    # Anchor on the rightmost DateTime-shaped token: everything to its left
    # is at a fixed offset, everything to its right is the optional sequence
    # number.
    idx = -1
    for i, token in enumerate(tokens):
        if _DATETIME_TOKEN_RE.match(token):
            idx = i
    if idx < 2:
        return None, None

    # The field immediately before it must be the exposure time.
    try:
        float(tokens[idx - 1])
    except ValueError:
        return None, None

    # {FrameType}_{Filter}_{Exptime}_{DateTime} — preferred over the
    # filter-less reading below, so that the legacy "M51_L_L_120_<dt>"
    # (Light frame, Luminance filter) resolves the way it was written.
    if idx >= 3:
        frame_type = _FRAME_TYPE_TOKENS.get(tokens[idx - 3].upper())
        if frame_type is not None:
            return frame_type, tokens[idx - 2]

    # {FrameType}_{Exptime}_{DateTime} — a calibration frame, or a Light
    # frame whose filter was unknown at normalization time.
    frame_type = _FRAME_TYPE_TOKENS.get(tokens[idx - 2].upper())
    if frame_type is not None:
        return frame_type, None

    return None, None


# ---------------------------------------------------------------------------
# Archive frame discovery
# ---------------------------------------------------------------------------

def _find_archive_frames(
    archive_dir: str,
    filter_name: Optional[str],
    new_position_angle_deg: Optional[float] = None,
    psf_fwhm_arcsec: Optional[float] = None,
) -> list[str]:
    """
    Return up to _MAX_FRAMES FITS paths from archive_dir, sorted newest-first.

    Calibration frames (Dark/Flat/Bias, which pipeline.py archives into this
    same per-object directory) are never eligible — see
    _parse_normalized_filename().

    When filter_name is provided and at least SUBTRACTION_MIN_FRAMES of the
    remaining science frames carry that filter in their filename's own filter
    FIELD, only those same-filter frames are returned.  Otherwise all science
    frames are returned, allowing cross-filter subtraction as a fallback.

    Parameters
    ----------
    archive_dir:
        Absolute path to the per-object archive directory.
    filter_name:
        Normalized filter string (e.g. "Ha", "R", "L") or None.
    psf_fwhm_arcsec:
        The new frame's own measured stellar FWHM in arcsec (qc.analyze()'s
        fwhm_median, as forwarded by pipeline.py). When given, a candidate
        whose own archived QCFWHM is worse than
        SUBTRACTION_REF_MAX_FWHM_RATIO times it is excluded — see
        _passes_quality_screen(). None leaves only the QCFLAG half of the
        screen in force.
    new_position_angle_deg:
        The new frame's own WCS-derived position angle (run()'s own
        _position_angle_deg(wcs) — see that helper's docstring). When given,
        and there are more recency/filter-matching candidates than
        _MAX_FRAMES, the final _MAX_FRAMES selection prefers frames whose
        own orientation is CLOSEST to this one over merely-most-recent
        ones — a reference needing less geometric correction is a mildly
        better bet for alignment quality. The whole candidate list is
        ranked, however far back in the archive it reaches; see
        _sort_by_pa_closeness() for why it used to be only the 30 newest
        and why that bounded nothing. This is a soft, non-exclusionary
        preference, not a hard filter: run()'s _prerotate_reference() step
        already coarse-corrects for whatever orientation difference remains
        in whichever frames end up selected here, including a ~180deg
        meridian-flip difference (see CLAUDE.md's "camera rotation"
        discussion) — a large PA difference is never, on its own, a reason
        to drop a reference. None (the default) preserves the exact prior
        recency-only behavior.

    Returns
    -------
    list[str]
        Up to _MAX_FRAMES absolute FITS file paths, newest-first unless
        reordered by PA-closeness per new_position_angle_deg above.
    """
    if not os.path.isdir(archive_dir):
        return []

    all_files: list[str] = []
    for ext in ("*.fits", "*.fit", "*.FITS", "*.FIT"):
        all_files.extend(glob.glob(os.path.join(archive_dir, ext)))

    all_files.sort(key=os.path.getmtime, reverse=True)

    # Drop calibration frames before anything else. pipeline.py archives
    # Dark/Flat/Bias into this same per-object directory, and a starless
    # calibration frame is not a reference for anything — being recent, it
    # would also crowd genuine science frames out of the newest-first
    # _MAX_FRAMES selection below (audit 2026-08-18, finding C5). A file
    # whose name doesn't follow the convention is kept: it can't be
    # identified as calibration, and an archive normalized by hand or with
    # NORMALIZE_ENABLED=false must not lose subtraction over it.
    parsed = {f: _parse_normalized_filename(os.path.basename(f)) for f in all_files}
    science_files = [f for f in all_files if parsed[f][0] not in _CALIBRATION_FRAME_TYPES]
    n_calibration = len(all_files) - len(science_files)
    if n_calibration:
        logger.info(
            "Subtraction: ignoring %d calibration frame(s) in %s as reference candidates",
            n_calibration, archive_dir,
        )

    science_files = _screen_by_quality(science_files, psf_fwhm_arcsec)

    candidates = science_files
    if filter_name:
        token = f"_{filter_name.upper()}_"

        def _same_filter(path: str) -> bool:
            parsed_filter = parsed[path][1]
            if parsed[path][0] is not None:
                # Positional match against the filename's own filter FIELD.
                # A substring test can't tell that field apart from the object
                # name before it, nor from the frame-type code that used to
                # share its alphabet (see _parse_normalized_filename()).
                return parsed_filter is not None and parsed_filter.upper() == filter_name.upper()
            # Unparseable name — fall back to the old substring test rather
            # than excluding it outright, so a non-normalized archive keeps
            # whatever same-filter matching it had before.
            return token in os.path.basename(path).upper()

        matching = [f for f in science_files if _same_filter(f)]
        if len(matching) >= config.SUBTRACTION_MIN_FRAMES:
            candidates = matching
        elif matching:
            logger.info(
                "Subtraction: only %d frame(s) match filter %s (need %d) — "
                "falling back to all %d science frame(s)",
                len(matching), filter_name, config.SUBTRACTION_MIN_FRAMES,
                len(science_files),
            )

    if new_position_angle_deg is not None and len(candidates) > _MAX_FRAMES:
        candidates = _sort_by_pa_closeness(candidates, new_position_angle_deg)

    return candidates[:_MAX_FRAMES]


def _read_qc_headers(path: str) -> tuple[Optional[str], Optional[float]]:
    """
    Read the QCFLAG/QCFWHM pair pipeline.py stamps into a frame's header at
    archive time. Either or both are None for a frame archived before those
    existed, or one placed in the directory by hand.

    A header-only read: no pixel data is touched, so screening the whole
    candidate list costs a handful of small reads.
    """
    try:
        header = fits.getheader(path)
    except Exception as exc:
        logger.debug("Subtraction: could not read QC headers from %s: %s", path, exc)
        return None, None

    flag = header.get("QCFLAG")
    flag_str = str(flag).strip().upper() if flag is not None else None

    fwhm: Optional[float] = None
    raw = header.get("QCFWHM")
    if raw is not None:
        try:
            value = float(raw)
            if math.isfinite(value) and value > 0:
                fwhm = value
        except (TypeError, ValueError):
            pass

    return flag_str, fwhm


def _screen_by_quality(
    paths: list[str],
    psf_fwhm_arcsec: Optional[float],
) -> list[str]:
    """
    Drop reference candidates that would damage the difference image: frames
    their own QC marked as failed, and frames whose seeing is far worse than
    the new frame's.

    Reference selection used to be recency (plus PA-closeness) alone and never
    looked at quality at all. That was harmless while QC-failed frames were
    moved to /fits/rejected, but they are archived now — into the very
    directory the reference stack is drawn from (audit 2026-08-18, finding
    H10). Differencing a sharp new frame against a blurred reference leaves
    the classic ring-shaped residual at every star in the field: a PSF
    mismatch, reported as a crowd of transients, on top of a noise floor
    raised enough to bury the faint real ones.

    A candidate with no QC headers at all is kept — it cannot be judged, and
    an archive written before those headers existed must not lose subtraction
    over it. Screening never costs the frame its subtraction either: if it
    would leave fewer than SUBTRACTION_MIN_FRAMES, the unscreened list is
    returned with a warning, on the same reasoning as photometry.py's
    zero-point reference screen — an imperfect reference stack beats none.
    """
    if not paths:
        return paths

    limit = None
    if psf_fwhm_arcsec is not None and psf_fwhm_arcsec > 0:
        limit = psf_fwhm_arcsec * config.SUBTRACTION_REF_MAX_FWHM_RATIO

    kept: list[str] = []
    n_failed_qc = 0
    n_blurred = 0

    for path in paths:
        flag, fwhm = _read_qc_headers(path)
        if flag is not None and flag != "OK":
            n_failed_qc += 1
            continue
        if limit is not None and fwhm is not None and fwhm > limit:
            n_blurred += 1
            continue
        kept.append(path)

    if not (n_failed_qc or n_blurred):
        return paths

    if len(kept) < config.SUBTRACTION_MIN_FRAMES:
        logger.warning(
            "Subtraction: only %d of %d reference candidate(s) pass the quality "
            "screen (%d failed QC, %d blurred beyond %.2f\") — stacking the "
            "unscreened set instead, so expect PSF-mismatch residuals",
            len(kept), len(paths), n_failed_qc, n_blurred, limit or 0.0,
        )
        return paths

    logger.info(
        "Subtraction: excluded %d QC-failed and %d blurred reference "
        "candidate(s) of %d",
        n_failed_qc, n_blurred, len(paths),
    )
    return kept


def _sort_by_pa_closeness(paths: list[str], new_position_angle_deg: float) -> list[str]:
    """
    Re-sort *paths* (already recency-sorted) so that frames whose own
    position angle is closest to new_position_angle_deg come first — using
    the existing recency order as the tiebreaker (Python's sort is stable),
    not replacing it outright.

    Every candidate is ranked, not just a shortlist of the most recent ones.
    This used to open only the 30 newest candidates' WCS, to bound the extra
    I/O, which quietly defeated the ranking in exactly the case it exists
    for (audit 2026-08-18, finding L5): an archive that splits into two
    orientation clusters — the ordinary result of a German equatorial
    mount's meridian flip — can easily have its 30 newest frames all on one
    side of the flip, so a new frame taken on the other side saw no
    well-oriented reference at all and every frame it stacked needed the
    full ~180deg pre-rotation. The better-matched frames were sitting in the
    same directory, never opened.

    The I/O that cap was protecting is already being spent: _screen_by_quality()
    above reads every science candidate's header on every run to check its
    QCFLAG/QCFWHM (finding H10, added after this cap). So the cap was
    bounding a second, marginal read of files the same call had just read —
    at the cost of the frames the ranking is meant to find. Reading the rest
    costs one more header-level open per candidate, the same order of
    magnitude as the screen itself and nothing beside astap and astroalign.

    A candidate whose own WCS/PA can't be determined sorts last (treated as
    the worst case, same as an unknown new_position_angle_deg would be for
    it), which for an archive with no WCS headers at all leaves the recency
    order exactly as it was.
    """
    def _pa_distance(path: str) -> float:
        wcs = _open_wcs(path)
        pa = _position_angle_deg(wcs) if wcs is not None else None
        if pa is None:
            return 180.0  # unknown orientation - treat as worst case
        d = abs(pa - new_position_angle_deg) % 360.0
        return min(d, 360.0 - d)

    try:
        ranked = sorted(paths, key=_pa_distance)
    except Exception as exc:
        logger.debug("Subtraction: PA-based reference ranking failed (%s) — falling back to recency order", exc)
        return paths

    logger.debug(
        "Subtraction: ranked %d reference candidate(s) by orientation against PA=%.1f°",
        len(paths), new_position_angle_deg,
    )
    return ranked


# ---------------------------------------------------------------------------
# FITS I/O helpers
# ---------------------------------------------------------------------------

def _load_frame_data(fits_path: str) -> Optional[np.ndarray]:
    """
    Load the first 2-D image extension as a float32 array, or None on error.

    Iterates through all HDUs to handle multi-extension FITS files gracefully.
    """
    try:
        with fits.open(fits_path) as hdul:
            for hdu in hdul:
                if hdu.data is not None and hdu.data.ndim == 2:
                    return hdu.data.astype(np.float32)
        return None
    except Exception as exc:
        logger.debug("Failed to load FITS data from %s: %s", fits_path, exc)
        return None


# ---------------------------------------------------------------------------
# Photometric normalization of reference frames
#
# A frame's recorded signal in ADU scales as exposure_time / gain, where gain
# is the sensor's true conversion factor in electrons per ADU. An object's
# archive routinely mixes exposure times (auto-exposure, a different session,
# a different camera profile), and median-stacking those in raw ADU leaves a
# residual of roughly (K-1) x flux at the position of EVERY star in the frame
# once the stack is subtracted, where K is the scale mismatch — hundreds of
# false candidates across a single frame, plus an elevated noise floor that
# hides the genuine faint transients subtraction exists to find (audit
# 2026-08-18, finding C4).
#
# The gain plausibility range and the EGAIN-before-GAIN preference are the
# same as modules/photometry.py's _resolve_gain(), duplicated here by hand
# rather than imported — the convention this module already follows for the
# streak-mask and FWHM-floor logic it shares with modules/astrometry/.
# ---------------------------------------------------------------------------

_GAIN_MIN_E_PER_ADU: float = 0.05
_GAIN_MAX_E_PER_ADU: float = 20.0

# A reference needing more correction than this (in either direction) is
# reported to the operator: it still gets normalized and used, but such an
# archive is heterogeneous enough that the scaled-up reference noise measurably
# raises the detection threshold for the whole frame.
_FLUX_SCALE_WARN_FACTOR: float = 2.0


def _read_flux_scale_keys(fits_path: str) -> tuple[Optional[float], Optional[float]]:
    """
    Read (exposure_time_sec, gain_e_per_adu) from a frame's headers.

    Either element is None when the file carries nothing usable for it.
    Both the primary header and the first 2-D image HDU's own header are
    consulted, since capture software differs on where it writes these.

    `gain` prefers EGAIN over GAIN for the reason spelled out in
    modules/photometry.py's _resolve_gain(): on most CMOS cameras EGAIN is
    the true e-/ADU conversion while GAIN holds the camera's gain *setting*
    in arbitrary vendor units (0-500 on a ZWO ASI). A value outside the
    plausible e-/ADU range is rejected rather than used. config's
    PHOTOMETRY_GAIN_E_PER_ADU deliberately does NOT override anything here:
    a single deployment-wide value is by definition identical for the new
    frame and every reference, so it cancels in the ratio and would only
    mask a genuine per-frame difference.
    """
    exptime: Optional[float] = None
    gain: Optional[float] = None
    try:
        with fits.open(fits_path) as hdul:
            headers = [hdul[0].header]
            for hdu in hdul:
                if hdu.data is not None and hdu.data.ndim == 2:
                    headers.append(hdu.header)
                    break

            for key in ("EXPTIME", "EXPOSURE"):
                for hdr in headers:
                    value = hdr.get(key)
                    if value is None:
                        continue
                    try:
                        candidate = float(value)
                    except (TypeError, ValueError):
                        continue
                    if math.isfinite(candidate) and candidate > 0:
                        exptime = candidate
                        break
                if exptime is not None:
                    break

            for key in ("EGAIN", "GAIN"):
                for hdr in headers:
                    value = hdr.get(key)
                    if value is None:
                        continue
                    try:
                        candidate = float(value)
                    except (TypeError, ValueError):
                        continue
                    if math.isfinite(candidate) and _GAIN_MIN_E_PER_ADU <= candidate <= _GAIN_MAX_E_PER_ADU:
                        gain = candidate
                        break
                if gain is not None:
                    break
    except Exception as exc:
        logger.debug("Subtraction: cannot read flux-scale keys from %s: %s", fits_path, exc)

    return exptime, gain


def _flux_scale_factor(
    ref_path: str,
    new_exptime: Optional[float],
    new_gain: Optional[float],
) -> float:
    """
    Multiplier bringing *ref_path*'s pixel values onto the new frame's own
    photometric scale: ``(t_new / t_ref) * (g_ref / g_new)``.

    Each of the two factors independently falls back to 1.0 when the
    corresponding keyword is missing on either side — an archive with no
    EXPTIME at all therefore behaves exactly as it did before normalization
    existed, rather than losing subtraction entirely.

    Scaling the reference (not the new frame) is deliberate: the new frame's
    own pixel values are what candidate fluxes and the SATURATION_ADU checks
    are measured against, and must stay in their native ADU.

    The scaled reference's bias/sky pedestal comes out of the subtraction as
    a smooth ``(K-1) x pedestal`` term, which _detect_diff_sources()'s own
    `sep.Background()` pass removes before extraction — unlike the per-star
    residual this function exists to cancel, which is not smooth and is
    exactly what SEP would otherwise report.
    """
    ref_exptime, ref_gain = _read_flux_scale_keys(ref_path)

    scale = 1.0
    if new_exptime and ref_exptime:
        scale *= new_exptime / ref_exptime
    if new_gain and ref_gain:
        scale *= ref_gain / new_gain

    if not math.isfinite(scale) or scale <= 0:
        logger.warning(
            "Subtraction: implausible flux scale %r for reference %s — using 1.0",
            scale, os.path.basename(ref_path),
        )
        return 1.0

    return scale


# ---------------------------------------------------------------------------
# Image alignment
# ---------------------------------------------------------------------------

def _align_frame(
    source: np.ndarray,
    target: np.ndarray,
) -> Optional[tuple[np.ndarray, Optional[np.ndarray]]]:
    """
    Align *source* onto *target* pixel grid using astroalign triangle matching.

    Returns ``(aligned, footprint)`` — the aligned array as float32, plus
    astroalign's own boolean footprint marking the pixels it could NOT fill
    from the source frame (True = no information there) — or None if alignment
    fails (e.g. too few stars detected, common for sparse or heavily trailed
    fields).

    The footprint used to be discarded (audit 2026-08-18, finding H8). It
    matters because the geometric transform almost never maps the reference
    exactly onto the new frame's grid: a shift, a rotation, or a different
    sensor size all leave a band of target pixels with no source data behind
    them. Whatever astroalign puts there is not a measurement, and letting it
    into the median stack produces a residual in the difference image that no
    downstream filter is looking for — the saturation, streak and near_edge
    filters all address something else.

    `footprint` is None when astroalign returned something that isn't a usable
    boolean mask of the right shape; the caller then treats every pixel of
    that reference as valid, i.e. exactly the previous behaviour.
    """
    try:
        import astroalign
        # propagate_mask=True carries a masked source's own mask into the
        # returned footprint — which is how _prerotate_reference()'s padding
        # (the corners a same-canvas rotation would have cropped away) stays
        # marked as "no data here" through astroalign's own resampling.
        aligned, footprint = astroalign.register(source, target, propagate_mask=True)
        aligned_arr = np.asarray(aligned, dtype=np.float32)

        mask: Optional[np.ndarray] = None
        if footprint is not None:
            candidate = np.asarray(footprint)
            if candidate.shape == aligned_arr.shape:
                mask = candidate.astype(bool)

        return aligned_arr, mask
    except Exception as exc:
        logger.debug("astroalign failed: %s", exc)
        return None


def _median_reference(
    stack: np.ndarray,
    footprints: list[Optional[np.ndarray]],
    new_data: np.ndarray,
) -> np.ndarray:
    """
    Per-pixel median of the aligned reference stack, ignoring pixels each
    reference had no data for.

    astroalign's footprint marks the target pixels it could not fill from the
    source frame — the band a shift or rotation leaves empty, or the region
    outside a smaller sensor's field. Those values are not measurements, and
    averaging them into the reference puts a step into the difference image
    that reads as a bright residual (audit 2026-08-18, finding H8).

    A pixel no reference covered at all has no reference value to speak of, so
    it takes the new frame's own value: the difference there is then exactly
    zero and nothing can be detected in it. The alternative — leaving it at
    whatever the stack happened to hold — is precisely the false residual this
    is avoiding.

    A non-finite value in a reference is excluded the same way, and for the
    same reason: `np.median()` does not ignore NaN, it propagates it, so one
    NaN pixel in one archived file — not rare, masked pixels from a previous
    calibration pass leave them — nulled the reference at that position and
    the difference image with it (audit 2026-08-18, finding H9). Silently, and
    for every frame that archive is ever a reference for.

    Falls back to a plain median when nothing needs excluding at all, i.e. the
    behaviour before footprints were kept.

    The excluded values are set to NaN **in `stack` itself** and the stack is
    then sorted in place, so the caller must not reuse it. That is what keeps
    this affordable: `np.ma.median()` — the obvious tool — builds sorted copies
    of the data and the mask plus several index arrays, about five times the
    stack's own size on top of it (2.3 GB extra for seven 4656x3520 frames),
    which on a real archive doubled the subtraction step's peak memory and got
    the worker OOM-killed mid-task (2026-09-22 test run). NaN sorts to the end,
    so after the sort the k valid values of each pixel occupy its first k
    slots and the median is read straight off them — the same value
    `np.ma.median()` returns, including the mean of the two middle values
    when k is even.
    """
    nonfinite = ~np.isfinite(stack)
    has_footprints = any(fp is not None for fp in footprints)

    if not has_footprints and not nonfinite.any():
        return np.median(stack, axis=0).astype(np.float32)

    n_nonfinite = int(np.count_nonzero(nonfinite))
    if n_nonfinite:
        logger.warning(
            "Subtraction: %d non-finite pixel value(s) across %d reference "
            "frame(s) excluded from the median stack — one such pixel would "
            "otherwise null the reference at that position",
            n_nonfinite, stack.shape[0],
        )
    # +/-inf would sort as a value; only NaN sorts past every valid one.
    stack[nonfinite] = np.nan
    del nonfinite

    for i, fp in enumerate(footprints):
        if fp is not None and fp.shape == stack.shape[1:]:
            stack[i][fp] = np.nan

    stack.sort(axis=0)
    valid = np.count_nonzero(~np.isnan(stack), axis=0)

    n_invalid = int(stack.shape[0] * stack[0].size - valid.sum())
    if n_invalid:
        logger.info(
            "Subtraction: excluding %d uncovered or non-finite pixel value(s) "
            "across %d reference frame(s) from the median stack",
            n_invalid, stack.shape[0],
        )

    # The median of k sorted values is the mean of indices (k-1)//2 and k//2
    # — one and the same index when k is odd. k == 0 reads slot 0 (a NaN) and
    # is overwritten below as uncovered.
    lo = np.maximum((valid - 1) // 2, 0)[np.newaxis]
    hi = (valid // 2)[np.newaxis]
    reference = (
        np.take_along_axis(stack, lo, axis=0)[0]
        + np.take_along_axis(stack, hi, axis=0)[0]
    ) * np.float32(0.5)
    reference = reference.astype(np.float32, copy=False)
    del lo, hi

    uncovered = valid == 0
    if uncovered.any():
        logger.info(
            "Subtraction: %d pixel(s) have no valid reference at all — the "
            "difference image is held at zero there",
            int(uncovered.sum()),
        )
        reference[uncovered] = new_data[uncovered]
    # A pixel the new frame itself has no value for leaves the difference
    # non-finite whatever the reference says; run() masks those out of
    # detection, and zeroing the reference here keeps the arithmetic itself
    # well-defined.
    reference[~np.isfinite(reference)] = 0.0

    return reference


# ---------------------------------------------------------------------------
# Streak masking — see config.STREAK_* and modules/astrometry/_streak.py's
# identical pre-pass (duplicated here rather than imported, mirroring how
# this module already keeps its own independent copy of the FWHM-floor/
# near-edge logic used by modules/astrometry/_extraction.py).
# ---------------------------------------------------------------------------

def _build_streak_mask(
    data_sub: np.ndarray,
    rms: float,
    pixel_scale_arcsec: Optional[float],
) -> Optional[np.ndarray]:
    """
    Coarse, low-threshold, non-deblended pre-pass over a difference image
    that finds long thin streaks and returns a boolean pixel mask covering
    them, or None if none were found.

    A satellite trail crossing the *new* frame but absent from the reference
    stack shows up in the diff image as a strong positive residual just like
    any other transient — and, at the diff image's ordinary detection
    settings, fragments into dozens of separate elongated candidates rather
    than one (real data, 2026-08-07, T_CrB test frames: 42 candidates with
    elongation > 3 along a single trail — each individually classifiable by
    anomaly_detector.py as its own SPACE_DEBRIS anomaly). See
    modules/astrometry/_streak.py's identical helper for the full rationale; this
    module's own `fwhm_min_px` floor in _detect_diff_sources() below already
    rejects candidates far SHARPER than the stellar PSF (hot pixels) — it
    has no equivalent protection against a genuine, coherent, but heavily
    over-fragmented elongated feature, which is what this pre-pass adds.

    Parameters
    ----------
    data_sub:
        Background-subtracted difference image (post any saturation
        masking already applied by the caller).
    rms:
        Background RMS — used as this coarse pass's relative detection
        threshold (config.STREAK_DETECT_SIGMA).
    pixel_scale_arcsec:
        Plate scale in arcsec/px, or None (e.g. no WCS available) — falls
        back to a conservative fixed 200px length floor and 1px dilation.

    Returns
    -------
    np.ndarray | None
        Boolean mask, same shape as data_sub, or None when nothing
        streak-like was found or the coarse pass itself failed.
    """
    if rms is None or rms <= 0:
        return None

    try:
        objs, seg = sep.extract(
            data_sub,
            thresh=config.STREAK_DETECT_SIGMA,
            err=rms,
            # minarea=5, matching _detect_diff_sources()'s own final-pass
            # minarea below — NOT config.SEP_MIN_AREA (15), which belongs to
            # the main-frame extraction context in astrometry.py/qc.py.
            # Using a coarser (larger) minarea here than the real detection
            # pass would let small trail fragments slip past this pre-pass
            # invisibly while still being individually detected as their own
            # elongated candidates by the real, more sensitive pass below —
            # exactly the gap that let 40 trail-fragment candidates survive
            # in a live run against real T_CrB data before this fix.
            minarea=5,
            deblend_cont=1.0,
            segmentation_map=True,
        )
    except Exception as exc:
        logger.debug("Subtraction: streak coarse pass failed: %s", exc)
        return None

    if len(objs) == 0:
        return None

    safe_b = np.where(objs["b"] > _MIN_SEMI_MINOR_PX, objs["b"], _MIN_SEMI_MINOR_PX)
    elongation = objs["a"] / safe_b
    bbox_diag_px = np.sqrt(
        (objs["xmax"] - objs["xmin"]).astype(np.float64) ** 2
        + (objs["ymax"] - objs["ymin"]).astype(np.float64) ** 2
    )

    if pixel_scale_arcsec and pixel_scale_arcsec > 0:
        min_len_px = config.STREAK_MIN_LENGTH_ARCSEC / pixel_scale_arcsec
    else:
        min_len_px = 200.0

    streak_idx = np.where(
        (elongation >= config.STREAK_ELONGATION_MIN) & (bbox_diag_px >= min_len_px)
    )[0]
    if len(streak_idx) == 0:
        return None

    mask = np.isin(seg, streak_idx + 1)

    dilate_px = 1
    if pixel_scale_arcsec and pixel_scale_arcsec > 0:
        dilate_px = max(1, int(round(config.STREAK_MASK_DILATE_ARCSEC / pixel_scale_arcsec)))
    try:
        from scipy.ndimage import binary_dilation
        structure = np.ones((2 * dilate_px + 1, 2 * dilate_px + 1), dtype=bool)
        mask = binary_dilation(mask, structure=structure)
    except Exception as exc:
        logger.debug(
            "Subtraction: streak mask dilation failed (%s) — using un-dilated mask", exc
        )

    logger.info(
        "Subtraction: streak masking: %d streak-like feature(s) found, "
        "masking %d diff-image pixel(s)",
        len(streak_idx), int(mask.sum()),
    )
    return mask


# ---------------------------------------------------------------------------
# Difference-image source detection
# ---------------------------------------------------------------------------

_NOISE_CORR_BOX: int = 4


def _noise_correlation_factor(
    sub: np.ndarray,
    mask: Optional[np.ndarray],
    rms: float,
) -> float:
    """
    How much the difference image's noise is correlated between neighbouring
    pixels, as the factor by which ``rms * sqrt(npix)`` understates the noise
    in an aperture.

    Independent per-pixel noise averaged over a box of side `k` falls as
    `1/k`. Interpolation does not average independent samples: astroalign
    resamples every reference onto this frame's grid (and `_prerotate_reference()`
    may interpolate once more before it), which spreads each input pixel's
    noise across several output pixels. The box-averaged scatter then falls
    short of `1/k`, and the ratio of what it actually is to what it would be
    is exactly the correction an aperture's noise needs (audit 2026-08-18,
    finding H13).

    Measured from this frame's own difference image rather than assumed,
    because it depends on the interpolation each individual stack went
    through. Both scatters are MAD-derived so that the real sources in the
    image — which are not noise — don't set the answer.

    Returns 1.0 (the previous, uncorrected behaviour) when the measurement
    can't be made, comes out below 1, or SUBTRACTION_NOISE_CORR_MAX is at or
    below 1; and never returns more than that cap.
    """
    cap = config.SUBTRACTION_NOISE_CORR_MAX
    if cap <= 1.0 or rms <= 0:
        return 1.0

    try:
        from scipy.ndimage import uniform_filter  # noqa: PLC0415

        sample = sub if mask is None else sub[~mask]
        if sample.size < _NOISE_CORR_BOX ** 2 * 16:
            return 1.0

        smoothed = uniform_filter(sub, size=_NOISE_CORR_BOX)
        smoothed_sample = smoothed if mask is None else smoothed[~mask]

        median = float(np.median(smoothed_sample))
        mad = float(np.median(np.abs(smoothed_sample - median)))
        sigma_box = 1.4826 * mad
        if sigma_box <= 0:
            return 1.0

        # Expected box-averaged scatter if every pixel were independent.
        expected = rms / _NOISE_CORR_BOX
        factor = sigma_box / expected
    except Exception as exc:
        logger.debug("Subtraction: correlated-noise measurement failed (%s)", exc)
        return 1.0

    if not math.isfinite(factor) or factor <= 1.0:
        return 1.0
    return min(factor, cap)


def _detect_diff_sources(
    diff: np.ndarray,
    mask: Optional[np.ndarray] = None,
    fwhm_min_px: Optional[float] = None,
    pixel_scale_arcsec: Optional[float] = None,
) -> list[dict]:
    """
    Detect positive residuals in the difference image using SEP.

    Uses a local background model computed on the diff itself so that
    large-scale gradients (flat-field mismatch, sky gradient) do not
    pollute the threshold estimate.

    Parameters
    ----------
    diff:
        2-D float array: new_frame - reference_stack.
    mask:
        Optional boolean array, same shape as *diff*, marking pixels to
        exclude from detection — used by run() to suppress astroalign
        residual artifacts in the vicinity of saturated stars (see
        docs/ISSUES.md #1, #2). Masked pixels are excluded from the
        background model and zeroed in the background-subtracted image
        before extraction, so no candidate can be detected there. Ignored
        (treated as no mask) if its shape doesn't match *diff*.
    fwhm_min_px:
        Minimum acceptable FWHM in pixels. Candidates narrower than this are
        dropped as artifacts rather than returned — see run()'s docstring for
        why this exists and how the threshold is derived. None (the default)
        disables this filter, keeping every SEP detection as before.
    pixel_scale_arcsec:
        Plate scale in arcsec/px, forwarded to _build_streak_mask() above so
        a satellite trail present in the new frame but absent from the
        reference stack — which otherwise fragments into dozens of separate
        elongated candidates on the diff image — gets masked out before
        detection instead of producing one spurious candidate per fragment.
        None (the default) falls back to a conservative fixed-pixel length
        floor there rather than disabling the pre-pass outright.

    Returns
    -------
    list[dict]
        Pixel-space candidate dicts with keys: x, y, flux, snr, fwhm,
        elongation, near_edge (bool — see config.EDGE_MARGIN_FRAC and
        modules/astrometry/_extraction.py's identical flag; no leading underscore, same
        as "saturated" there, since it must survive to the API for
        pipeline.py's standalone DETECT_ANOMALIES reconstruction — see
        run()'s own docstring below. Survives _pixel_to_sky()'s conversion
        since only "x"/"y" are stripped there).
        Returns an empty list on any failure.
    """
    try:
        arr = np.ascontiguousarray(diff, dtype=np.float64)
        use_mask = mask if (mask is not None and mask.shape == arr.shape) else None
        bkg = sep.Background(arr, mask=use_mask) if use_mask is not None else sep.Background(arr)
        sub = arr - bkg.back()
        if use_mask is not None:
            sub[use_mask] = 0.0
        rms = float(bkg.globalrms)
        if rms <= 0:
            return []

        # The streak mask has to be found on a background-subtracted image,
        # so the pass above is unavoidable — but its background and RMS were
        # measured with the trail still in the frame. A bright satellite
        # track below the saturation threshold contributes to globalrms, and
        # since the detection threshold is SUBTRACTION_DETECT_SIGMA x rms,
        # one trail raises the bar for every faint real transient elsewhere
        # in the same frame (audit 2026-08-18, finding H12). Re-measure both
        # with the trail excluded before setting that threshold.
        streak_mask = _build_streak_mask(sub, rms, pixel_scale_arcsec)
        if streak_mask is not None and streak_mask.any():
            combined = streak_mask if use_mask is None else (use_mask | streak_mask)
            bkg = sep.Background(arr, mask=combined)
            sub = arr - bkg.back()
            sub[combined] = 0.0
            rms_masked = float(bkg.globalrms)
            if rms_masked <= 0:
                return []
            logger.info(
                "Subtraction: re-measured background with %d streak pixel(s) "
                "excluded — RMS %.3f -> %.3f",
                int(streak_mask.sum()), rms, rms_masked,
            )
            rms = rms_masked
            use_mask = combined

        # How far the aperture noise exceeds rms * sqrt(npix) because
        # neighbouring pixels' noise is not independent after resampling —
        # see _noise_correlation_factor(). Measured once per frame.
        noise_corr = _noise_correlation_factor(sub, use_mask, rms)
        if noise_corr > 1.0:
            logger.info(
                "Subtraction: difference-image noise is correlated by a factor "
                "of %.2f (resampling) — candidate SNRs divided by it",
                noise_corr,
            )

        thresh = config.SUBTRACTION_DETECT_SIGMA * rms
        try:
            objs = sep.extract(sub, thresh=thresh, minarea=5)
        except Exception:
            return []

        # Near-edge geometry flag — see config.EDGE_MARGIN_FRAC and
        # modules/astrometry/_extraction.py's identical computation for ordinary
        # detections. Coma distorts the PSF (and therefore astroalign's own
        # resampling residuals) most strongly toward the frame's edges, so a
        # diff-image candidate born there needs the same "demand stronger
        # elongation evidence" treatment in anomaly_detector.py that an
        # ordinary edge star gets. `arr.shape` is (height, width) =
        # (NAXIS2, NAXIS1), same convention as astropy.io.fits data arrays.
        height, width = arr.shape
        margin_x = config.EDGE_MARGIN_FRAC * width
        margin_y = config.EDGE_MARGIN_FRAC * height

        out: list[dict] = []
        n_rejected_sharp = 0
        n_rejected_edge = 0
        for obj in objs:
            # `obj` is a numpy.void record (one row of sep.extract()'s
            # structured array) — it supports dict-style bracket access
            # (obj["field"]) but has NO .get() method. The previous code
            # called obj.get(...) here, which raised AttributeError on every
            # single object, was swallowed by the try/except below, and
            # made this function return [] unconditionally whenever SEP
            # actually found anything on the difference image.
            # "fwhm" is also not a native sep.extract() field — it is
            # derived from the "a"/"b" second-moment axes, the same
            # Gaussian approximation used in modules/astrometry/_extraction.py.
            flux = float(obj["flux"])
            npix = int(obj["npix"])
            # rms * sqrt(npix) is the aperture noise only if each pixel's
            # noise is independent of its neighbours'. Resampling makes it
            # otherwise, so the aperture holds fewer independent measurements
            # than pixels and the uncorrected figure overstates significance.
            snr = flux / (rms * math.sqrt(npix) * noise_corr) if npix > 0 else 0.0
            a_axis = float(obj["a"])
            b_axis = max(float(obj["b"]), _MIN_SEMI_MINOR_PX)
            fwhm = 2.0 * math.sqrt(2.0 * math.log(2.0) * (a_axis ** 2 + b_axis ** 2) / 2.0)

            # Reject candidates far sharper than the frame's own stellar PSF —
            # see run()'s docstring. A real transient's light still passes
            # through the same optics/atmosphere as every star in the frame,
            # so it cannot be dramatically narrower than that shared PSF. A
            # sensor hot/warm pixel, by contrast, is a detector-space defect:
            # it doesn't move with the sky when astroalign resamples the
            # reference frames onto this frame's grid, so it never gets
            # subtracted out and shows up here as an unrealistically compact
            # positive residual (real incident, 2026-08-06, Vesta test data).
            if fwhm_min_px is not None and fwhm < fwhm_min_px:
                n_rejected_sharp += 1
                continue

            obj_x = float(obj["x"])
            obj_y = float(obj["y"])
            near_edge = (
                obj_x < margin_x or obj_x > width - margin_x
                or obj_y < margin_y or obj_y > height - margin_y
            )

            elongation = a_axis / b_axis

            # Candidates in the edge zone are held to a much higher bar.
            # Coma and the other off-axis aberrations change the PSF shape
            # between frames (rotation, guiding drift, focus shift), so the
            # median reference stack never perfectly cancels an edge star's
            # coma wing, and sep picks the leftover up as a "new source" —
            # a purely optical artifact. Real incident, 2026-08-10 analysis:
            # 53 of 80 UNKNOWN alerts were from_subtraction + near_edge,
            # every one a coma residual of an ordinary catalogued star.
            #
            # Rejecting the whole zone outright — the previous behaviour —
            # also meant a genuine transient landing near the edge, which a
            # dithering pattern makes routine, could never be found by
            # subtraction at all (audit 2026-08-18, finding H11). What
            # separates the two is shape and strength, not position: an
            # aberration stretches a PSF into an arc, and it is the mismatch
            # between two such arcs that fails to cancel, so a coma residual
            # is elongated and usually weak. A round, strong residual is not
            # that shape.
            if near_edge and not (
                elongation <= config.SUBTRACTION_EDGE_ELONGATION_MAX
                and snr >= config.SUBTRACTION_EDGE_SNR_MIN
            ):
                n_rejected_edge += 1
                continue

            out.append({
                "x":          obj_x,
                "y":          obj_y,
                "flux":       flux,
                "snr":        snr,
                "fwhm":       fwhm,
                "elongation": elongation,
                "near_edge":  near_edge,
            })

        if n_rejected_edge:
            logger.info(
                "Subtraction: rejected %d candidate(s) in the edge zone "
                "(EDGE_MARGIN_FRAC=%.2f) for being elongated beyond %.2f or "
                "weaker than SNR %.1f — likely coma/aberration residuals, "
                "not real transients",
                n_rejected_edge, config.EDGE_MARGIN_FRAC,
                config.SUBTRACTION_EDGE_ELONGATION_MAX,
                config.SUBTRACTION_EDGE_SNR_MIN,
            )

        if n_rejected_sharp:
            logger.info(
                "Subtraction: rejected %d candidate(s) narrower than %.2fpx "
                "FWHM floor (likely hot/warm pixel artifacts, not real transients)",
                n_rejected_sharp, fwhm_min_px,
            )

        return out
    except Exception as exc:
        logger.warning("SEP detection on diff image failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# WCS coordinate conversion
# ---------------------------------------------------------------------------

def _open_wcs(fits_path: str) -> Optional[WCS]:
    """
    Return the first valid celestial WCS found across *fits_path*'s HDUs, or
    None on any failure / if none is found. Shared by _pixel_to_sky() and
    _pixel_scale_arcsec() below.
    """
    try:
        with fits.open(fits_path) as hdul:
            for hdu in hdul:
                if hdu.header.get("CTYPE1"):
                    try:
                        w = WCS(hdu.header)
                        if w.has_celestial:
                            return w
                    except Exception:
                        continue
    except Exception:
        pass
    return None


def _pixel_scale_arcsec(fits_path: str, wcs: Optional[WCS] = None) -> Optional[float]:
    """
    Best-effort pixel scale (arcsec/px) from *wcs*, or derived from
    *fits_path*'s own header WCS when *wcs* is not given.

    Used to convert config.SATURATION_MASK_RADIUS_ARCSEC into a pixel radius
    for _build_saturation_mask() below. Returns None when no valid celestial
    WCS is available — the caller falls back to a fixed-pixel dilation radius.
    """
    if wcs is None:
        wcs = _open_wcs(fits_path)
    if wcs is None:
        return None
    try:
        ps_matrix = wcs.pixel_scale_matrix  # (2, 2), units deg/px
        deg = math.sqrt(ps_matrix[0, 0] ** 2 + ps_matrix[1, 0] ** 2)
        return deg * 3600.0
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Camera rotation — pre-aligning references before astroalign (see
# CLAUDE.md's "camera rotation" discussion). Independently duplicated from
# modules/astrometry/_frame_geometry.py's identical helper — same convention
# this module already uses for the streak-mask pre-pass and the FWHM-floor
# ratio (see this file's own module docstring / astrometry.py's section in
# CLAUDE.md): keeps this module free of a cross-package import for a few
# lines of trig, at the cost of keeping the two copies in sync by hand.
# ---------------------------------------------------------------------------

def _position_angle_deg(wcs: WCS) -> Optional[float]:
    """
    This frame's own orientation on the sky (0 = North up, increasing
    clockwise toward the image's +X pixel axis) — see
    modules/astrometry/_frame_geometry.py's identical helper for the full
    derivation and the empirical verification of the sign convention
    (tests/test_astrometry.py::TestPositionAngle).

    Evaluated at the WCS's own reference pixel (CRPIX) rather than the
    frame's true geometric centre: unlike
    modules/astrometry/_frame_geometry.py's copy, this one is called here
    purely to compare two frames' *relative* rotation (see
    _prerotate_reference() below), for which naxis1/naxis2 aren't otherwise
    needed — using CRPIX avoids requiring the caller to have loaded pixel
    data (and therefore know the shape) just to ask this question. The
    difference between evaluating at CRPIX vs. true centre is negligible
    for any real telescope's FOV and irrelevant here regardless, since only
    the difference between two frames' own PA values is ever used.

    Returns None on any failure (pathological/degenerate WCS).
    """
    try:
        cx = float(wcs.wcs.crpix[0]) - 1.0
        cy = float(wcs.wcs.crpix[1]) - 1.0
        center = wcs.pixel_to_world(cx, cy)
        north = SkyCoord(ra=center.ra, dec=center.dec + 1.0 * u.arcsec)
        north_x, north_y = wcs.world_to_pixel(north)
        dx = float(north_x) - cx
        dy = float(north_y) - cy
        if dx == 0.0 and dy == 0.0:
            return None
        return math.degrees(math.atan2(dx, dy)) % 360.0
    except Exception as exc:
        logger.debug("Subtraction: position angle computation failed: %s", exc)
        return None


def _prerotate_reference(
    ref_data: np.ndarray,
    ref_path: str,
    new_position_angle_deg: Optional[float],
) -> np.ndarray:
    """
    Coarse-rotate *ref_data* toward the new frame's own orientation before
    handing it to astroalign, using each frame's WCS-derived position angle
    — NOT a reason to exclude a reference whose orientation differs a lot
    (e.g. ~180deg, a meridian flip): astroalign's own triangle-matching
    finds a correct registration for any rotation on its own, but starting
    it from a near-zero residual angle rather than a large unknown one is
    both faster and less prone to a wrong/degenerate match on a sparse or
    partly-symmetric star field. See CLAUDE.md's "camera rotation"
    discussion for the reasoning and tests/test_subtraction.py for the
    empirical (not just algebraic) verification of the rotation direction.

    The correction angle is exactly ``new_pa - ref_pa`` (normalized to
    (-180, 180]) passed straight to ``scipy.ndimage.rotate(..., angle=...)``
    — that specific sign/magnitude relationship is what
    modules/astrometry/_frame_geometry.py's _position_angle_deg() docstring
    and tests/test_astrometry.py::TestPositionAngle jointly establish and
    verify: rotating a frame's pixel data by scipy angle theta increases its
    own measured PA by exactly theta.

    Silently returns *ref_data* unchanged (no rotation applied) whenever:
      - new_position_angle_deg is None (new frame's WCS/PA unavailable),
      - ref_path's own WCS/PA can't be determined,
      - the resulting |delta| is below config.SUBTRACTION_PREROTATE_MIN_DEG
        (not worth the interpolation cost for a negligible angle), or
      - the rotation itself raises (e.g. scipy missing) — logged and
        swallowed, exactly like this module's other best-effort geometry
        helpers (_build_saturation_mask, _build_streak_mask).
    This is a best-effort assist, never a hard requirement: a bad/omitted
    delta just means astroalign starts from a larger residual angle than it
    could have, not that alignment is skipped or wrong.
    """
    if new_position_angle_deg is None:
        return ref_data

    ref_wcs = _open_wcs(ref_path)
    if ref_wcs is None:
        return ref_data
    ref_pa = _position_angle_deg(ref_wcs)
    if ref_pa is None:
        return ref_data

    delta = (new_position_angle_deg - ref_pa) % 360.0
    if delta > 180.0:
        delta -= 360.0

    if abs(delta) < config.SUBTRACTION_PREROTATE_MIN_DEG:
        return ref_data

    try:
        from scipy.ndimage import rotate as _ndi_rotate

        # reshape=True, not False. Rotating onto the same canvas is a crop for
        # any angle that isn't a multiple of 90 degrees — the corners rotate
        # off the edge and are simply lost. The gate is 2 degrees, so this
        # fires on modest field rotation (an alt-az mount without a
        # de-rotator), not only on meridian flips, and the stars it discards
        # are the ones astroalign needs to find a transform at all: the
        # failure rate rose exactly for the large-angle cases pre-rotation
        # exists to help (audit 2026-08-18, finding H14). Letting the canvas
        # grow keeps every star; astroalign resamples onto the new frame's
        # grid regardless of the source's shape.
        rotated = _ndi_rotate(ref_data, angle=delta, reshape=True, order=1, mode="constant", cval=0.0)

        # The other half of that finding: the constant fill leaves a hard
        # zero/data boundary running diagonally across the frame, which the
        # median stack cannot cancel and sep reads as a bright edge-on
        # residual. Rotating a validity map by the same angle (order=0, so it
        # stays binary) marks exactly those pixels, and handing astroalign a
        # masked array with propagate_mask=True carries the marking through
        # its own resampling into the footprint _median_reference() already
        # excludes from the stack.
        valid = _ndi_rotate(
            np.ones(ref_data.shape, dtype=np.uint8),
            angle=delta, reshape=True, order=0, mode="constant", cval=0,
        )

        logger.info(
            "Subtraction: pre-rotated reference %s by %.1f deg (ref_pa=%.1f, new_pa=%.1f) before alignment",
            os.path.basename(ref_path), delta, ref_pa, new_position_angle_deg,
        )
        return np.ma.masked_array(
            np.asarray(rotated, dtype=ref_data.dtype),
            mask=(valid < 1),
        )
    except Exception as exc:
        logger.debug("Subtraction: pre-rotation of %s failed (%s) — using un-rotated reference", ref_path, exc)
        return ref_data


def _pixel_to_sky(
    pixel_candidates: list[dict], fits_path: str, wcs: Optional[WCS] = None
) -> list[dict]:
    """
    Convert pixel (x, y) detections to sky coordinates (ra, dec) using *wcs*,
    or a WCS read from *fits_path*'s own header when *wcs* is not given.

    astropy WCS.pixel_to_world() uses 0-indexed pixel coordinates internally
    (it handles the FITS CRPIX 1-indexed convention transparently), so passing
    SEP's (x, y) directly is correct.

    Parameters
    ----------
    pixel_candidates:
        List of dicts with at least ``x`` and ``y`` keys.
    fits_path:
        Absolute path to the science FITS file. Only actually read when
        *wcs* is not given — see *wcs* below.
    wcs:
        The already-solved WCS for fits_path — normally astro_result["wcs"]
        from astrometry.solve(), passed down through run() below. Preferred
        over re-deriving WCS from fits_path's own header: at the point
        run() is called, that header can still carry a stale WCS (e.g. a
        capture program's mount-pointing estimate — see the 2026-08-06
        UGC_6930 incident in CLAUDE.md); pipeline.py only corrects it later,
        right before archiving. Falling back to fits_path's header here
        would give these candidates a different systematic offset than
        every other source in the same frame, which is exactly what
        catalog_matcher's WCS-offset accumulator assumes can't happen.
        Pass None (or omit) to fall back to the old file-read behavior —
        used by callers that never ran astrometry.solve() themselves, e.g.
        tests or standalone invocations.

    Returns
    -------
    list[dict]
        Candidates with ``x``/``y`` replaced by ``ra``/``dec`` in decimal degrees.
        Candidates for which the conversion fails are silently dropped.
    """
    try:
        if wcs is None:
            wcs = _open_wcs(fits_path)

        if wcs is None:
            logger.debug(
                "No WCS in %s — cannot convert diff candidates to sky coords",
                fits_path,
            )
            return []

        result: list[dict] = []
        for cand in pixel_candidates:
            try:
                sky = wcs.pixel_to_world(cand["x"], cand["y"])
                out = {k: v for k, v in cand.items() if k not in ("x", "y")}
                out["ra"]  = float(sky.ra.deg)
                out["dec"] = float(sky.dec.deg)
                result.append(out)
            except Exception:
                continue
        return result
    except Exception as exc:
        logger.warning("WCS coordinate conversion failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Saturation masking — see docs/ISSUES.md #1, #2
# ---------------------------------------------------------------------------

def _build_saturation_mask(
    new_data: np.ndarray,
    aligned_refs: list[np.ndarray],
    radius_px: int,
) -> Optional[np.ndarray]:
    """
    Flag pixels near saturation in the new frame or any aligned reference
    frame, dilated by radius_px, for exclusion from diff-image detection.

    Saturated stars leave large, non-Gaussian residuals after astroalign
    resampling even under near-perfect registration (interpolation ringing,
    sub-pixel misalignment amplified by huge pixel values). Left unmasked,
    sep.extract() on the diff image happily reports these as bright
    "transient" candidates — uncatalogued (no star sits exactly there in any
    catalog) and, if ever photometered, at an extreme magnitude — which is
    exactly the bright-star artifact pattern suspected in docs/ISSUES.md #1
    and observed as extreme magnitudes in #2.

    Parameters
    ----------
    new_data:
        The new frame's pixel data.
    aligned_refs:
        Reference frames already resampled onto new_data's pixel grid.
        Entries whose shape doesn't match new_data (shouldn't happen post
        alignment, but checked defensively) are skipped.
    radius_px:
        Dilation radius in pixels. 0 (or a failed dilation, e.g. missing
        scipy) falls back to the un-dilated saturation mask itself.

    Returns
    -------
    np.ndarray | None
        Boolean mask, same shape as new_data, or None when nothing in the
        new frame or any reference frame is saturated (the common case) —
        callers can skip masking work entirely in that case.
    """
    saturated = new_data >= config.SATURATION_ADU
    for ref in aligned_refs:
        if ref.shape == new_data.shape:
            saturated |= (ref >= config.SATURATION_ADU)

    if not saturated.any():
        return None

    if radius_px <= 0:
        return saturated

    try:
        from scipy.ndimage import binary_dilation
        structure = np.ones((2 * radius_px + 1, 2 * radius_px + 1), dtype=bool)
        return binary_dilation(saturated, structure=structure)
    except Exception as exc:
        logger.debug(
            "Saturation mask dilation failed (%s) — using un-dilated mask", exc
        )
        return saturated


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def run(
    fits_path: str,
    archive_dir: str,
    filter_name: Optional[str],
    wcs: Optional[WCS] = None,
    psf_fwhm_arcsec: Optional[float] = None,
) -> dict:
    """
    Run image subtraction to detect transient / moving sources.

    The function is async so it integrates cleanly into the pipeline's async
    context.  The heavy work (numpy, SEP, astroalign) is CPU-bound and runs
    synchronously within the coroutine; for the current single-frame workload
    this is acceptable.  If the pipeline moves to concurrent frame processing,
    wrap in ``asyncio.to_thread()``.

    Parameters
    ----------
    fits_path:
        Absolute path to the incoming (new) science FITS file.
    archive_dir:
        Absolute path to the per-object archive directory
        (e.g. ``/fits/archive/M51/``).
    filter_name:
        Normalized filter name used to prefer same-filter reference frames
        (e.g. "Ha", "R").  Pass None to use all archived frames.
    wcs:
        The already-solved WCS for fits_path (astro_result["wcs"] from
        astrometry.solve()), forwarded to _pixel_scale_arcsec() and
        _pixel_to_sky() below so subtraction candidates get the exact same
        sky coordinates every other source in this frame does. Also used to
        derive this frame's own position angle (_position_angle_deg(wcs)),
        which softly ranks reference-frame selection and coarse-pre-rotates
        whichever references are chosen before astroalign — see
        _prerotate_reference()'s docstring and CLAUDE.md's "camera rotation"
        discussion. Pass None to fall back to reading WCS straight from
        fits_path's own header for sky-coordinate conversion (see
        _pixel_to_sky()'s docstring for why that's a fallback, not the
        default, in the real pipeline) and to disable both the PA-based
        reference ranking and the pre-rotation entirely (graceful
        degradation to this feature's pre-existing behavior).
    psf_fwhm_arcsec:
        This frame's own measured stellar PSF FWHM in arcseconds (QC's
        ``fwhm_median`` — the same value astrometry.solve() uses to tighten
        its own star filter). Converted to pixels via the frame's plate
        scale and used as a minimum-FWHM floor (``psf_fwhm_arcsec / 1.5``,
        mirroring astrometry.py's ratio) when detecting candidates on the
        difference image: a genuine astrophysical transient's light is still
        shaped by the same optical/atmospheric PSF as every star in this
        frame, so it cannot be dramatically sharper than that. A sensor
        hot/warm pixel is fixed to the *detector* grid, not the sky, so
        astroalign's per-frame resampling scatters it to a different pixel
        in each aligned reference — it never lines up with, and so never
        gets subtracted out by, the median reference stack. Left unfiltered,
        it appears in the difference image as an unrealistically compact
        positive residual and gets reported as a spurious transient
        candidate (real incident, 2026-08-06, Vesta test data — hot pixels
        far more compact than any real star ended up posted as UNKNOWN
        anomalies). None (e.g. a caller that never ran QC, such as a
        standalone script) disables this filter, preserving the old
        unfiltered behavior.

    Returns
    -------
    dict
        performed : bool
            True only when subtraction completed and produced a diff image.
        reference_frame_count : int
            Number of reference frames that were successfully aligned and
            included in the median stack.
        candidates : list[dict]
            Sky-space candidates.  Each dict has keys:
            ra, dec, flux, snr, fwhm, elongation, mag=None,
            _from_subtraction=True, near_edge (bool, see
            config.EDGE_MARGIN_FRAC — no leading underscore, unlike
            _from_subtraction, since it must be persisted to the API; see
            _detect_diff_sources()'s docstring above).
            Compatible with the source dicts produced by astrometry.solve().
    """
    empty: dict = {"performed": False, "reference_frame_count": 0, "candidates": []}

    # This frame's own orientation, derived from the already-solved wcs
    # (cheap — no pixel data needed yet). Used both to softly prefer
    # similarly-oriented references in _find_archive_frames() below and to
    # coarse-pre-rotate whichever references end up selected, before
    # astroalign — see CLAUDE.md's "camera rotation" discussion. None
    # (no wcs, or its own PA round-trip failed) degrades gracefully: recency-
    # only selection and no pre-rotation, exactly like before this feature.
    new_position_angle_deg = _position_angle_deg(wcs) if wcs is not None else None

    archive_files = _find_archive_frames(
        archive_dir, filter_name, new_position_angle_deg, psf_fwhm_arcsec,
    )
    # Exclude the new frame's own file from its candidate reference stack —
    # re-analyzing an already-archived frame (see pipeline.py's
    # _resolve_bare_filename()) passes a fits_path that may already be
    # sitting inside archive_dir itself, and _find_archive_frames() globs the
    # whole directory with no idea which file is "the new one". Without this,
    # a re-analyzed frame would subtract a resampled copy of itself as part
    # of its own reference median.
    fits_path_real = os.path.realpath(fits_path)
    archive_files = [f for f in archive_files if os.path.realpath(f) != fits_path_real]
    if len(archive_files) < config.SUBTRACTION_MIN_FRAMES:
        logger.info(
            "Subtraction skipped: %d archive frame(s) in %s (need %d)",
            len(archive_files),
            archive_dir,
            config.SUBTRACTION_MIN_FRAMES,
        )
        return empty

    new_data = _load_frame_data(fits_path)
    if new_data is None:
        logger.warning("Subtraction: cannot load new frame %s", fits_path)
        return empty

    # ------------------------------------------------------------------
    # Align each reference frame to the new frame's pixel grid
    # ------------------------------------------------------------------
    # This frame's own photometric scale, against which every reference is
    # normalized below — see _flux_scale_factor(). Read once here rather than
    # per reference.
    new_exptime, new_gain = _read_flux_scale_keys(fits_path)
    if new_exptime is None:
        logger.warning(
            "Subtraction: no usable EXPTIME on %s — reference frames cannot be "
            "normalized to its exposure, so a mixed-exposure archive will leave a "
            "residual at every star in the diff image",
            os.path.basename(fits_path),
        )

    aligned: list[np.ndarray] = []
    footprints: list[Optional[np.ndarray]] = []
    scales: list[float] = []
    for ref_path in archive_files:
        ref_data = _load_frame_data(ref_path)
        if ref_data is None:
            continue
        # Coarse-correct for a known camera/rotator orientation difference
        # (e.g. a meridian flip) BEFORE astroalign's own fine, star-matching
        # registration — see _prerotate_reference()'s docstring. A no-op
        # (returns ref_data unchanged) whenever either frame's PA is
        # unknown or the difference is below
        # config.SUBTRACTION_PREROTATE_MIN_DEG.
        ref_data = _prerotate_reference(ref_data, ref_path, new_position_angle_deg)
        # NOTE: deliberately no shape-equality gate here. astroalign performs
        # triangle-pattern star matching and resamples the reference frame
        # onto the new frame's pixel grid — it is explicitly designed to
        # align frames with different pixel dimensions, scale, rotation, and
        # FOV (e.g. an archived frame captured with a different camera/
        # resolution than tonight's frame). Rejecting shape mismatches before
        # ever calling astroalign silently disabled subtraction for exactly
        # the case it exists to handle. _align_frame()'s own try/except below
        # still catches genuine alignment failures (too few common stars,
        # no overlapping field, etc.).
        result = _align_frame(ref_data, new_data)
        if result is not None:
            result_frame, footprint = result
            aligned.append(result_frame)
            footprints.append(footprint)
            # Kept alongside rather than applied to `result_frame` itself:
            # _build_saturation_mask() below compares the aligned references
            # against SATURATION_ADU, and a scaled-down reference's saturated
            # core would drop below that threshold and escape masking.
            scales.append(_flux_scale_factor(ref_path, new_exptime, new_gain))
        else:
            logger.debug(
                "Subtraction: skipping %s (alignment failed)",
                os.path.basename(ref_path),
            )

    if len(aligned) < config.SUBTRACTION_MIN_FRAMES:
        logger.warning(
            "Subtraction: only %d frame(s) aligned successfully (need %d) — skipping",
            len(aligned),
            config.SUBTRACTION_MIN_FRAMES,
        )
        return empty

    # ------------------------------------------------------------------
    # Build median reference and compute difference image
    # ------------------------------------------------------------------
    stack = np.stack(aligned, axis=0)
    if any(abs(scale - 1.0) > 1e-3 for scale in scales):
        stack *= np.asarray(scales, dtype=np.float32).reshape(-1, 1, 1)
        logger.info(
            "Subtraction: normalized %d reference frame(s) to this frame's "
            "photometric scale (factors %.3f-%.3f)",
            len(scales), min(scales), max(scales),
        )
        extreme = [s for s in scales
                   if s > _FLUX_SCALE_WARN_FACTOR or s < 1.0 / _FLUX_SCALE_WARN_FACTOR]
        if extreme:
            logger.warning(
                "Subtraction: %d of %d reference frame(s) needed a flux scale "
                "beyond %.1fx (worst %.3f) — this archive mixes exposure times or "
                "gains widely enough that the scaled reference noise raises the "
                "detection threshold for the whole frame",
                len(extreme), len(scales), _FLUX_SCALE_WARN_FACTOR,
                max(extreme, key=lambda s: abs(math.log(s))),
            )
    reference = _median_reference(stack, footprints, new_data)
    diff = new_data - reference

    # ------------------------------------------------------------------
    # Mask the vicinity of saturated pixels (new frame or any reference)
    # before detection — see docs/ISSUES.md #1, #2 and _build_saturation_mask().
    # ------------------------------------------------------------------
    pixel_scale_arcsec = _pixel_scale_arcsec(fits_path, wcs=wcs)
    radius_px = 0
    if pixel_scale_arcsec and pixel_scale_arcsec > 0:
        radius_px = max(1, int(round(config.SATURATION_MASK_RADIUS_ARCSEC / pixel_scale_arcsec)))

    sat_mask = _build_saturation_mask(new_data, aligned, radius_px)
    if sat_mask is not None:
        logger.info(
            "Subtraction: masking %d saturated-vicinity pixel(s) (radius=%dpx) "
            "before diff detection",
            int(sat_mask.sum()),
            radius_px,
        )

    # A non-finite pixel in the NEW frame survives everything above — the
    # reference stack can't repair it — and would otherwise reach sep, whose
    # background/RMS estimate it corrupts for the whole frame. Fold it into
    # the same detection mask the saturation vicinity uses and zero it in the
    # difference, so it is simply a place nothing can be found (audit
    # 2026-08-18, finding H9).
    nonfinite_diff = ~np.isfinite(diff)
    if nonfinite_diff.any():
        logger.warning(
            "Subtraction: %d non-finite pixel(s) in the difference image "
            "(the new frame carries no value there) — excluded from detection",
            int(nonfinite_diff.sum()),
        )
        diff = np.where(nonfinite_diff, 0.0, diff).astype(np.float32)
        sat_mask = nonfinite_diff if sat_mask is None else (sat_mask | nonfinite_diff)

    # ------------------------------------------------------------------
    # Minimum-FWHM floor for candidate shape — see psf_fwhm_arcsec's
    # docstring above. Needs the same pixel scale already computed for the
    # saturation mask radius, so only convert when both are available.
    # ------------------------------------------------------------------
    fwhm_min_px: Optional[float] = None
    if psf_fwhm_arcsec is not None and psf_fwhm_arcsec > 0 and pixel_scale_arcsec and pixel_scale_arcsec > 0:
        fwhm_min_px = (psf_fwhm_arcsec / 1.5) / pixel_scale_arcsec

    # ------------------------------------------------------------------
    # Detect and project candidates
    # ------------------------------------------------------------------
    pixel_cands = _detect_diff_sources(
        diff, mask=sat_mask, fwhm_min_px=fwhm_min_px,
        pixel_scale_arcsec=pixel_scale_arcsec,
    )

    sky_cands   = _pixel_to_sky(pixel_cands, fits_path, wcs=wcs)

    for cand in sky_cands:
        cand["mag"]               = None
        cand["_from_subtraction"] = True

    logger.info(
        "Subtraction complete: %d reference frames, %d pixel candidates → %d sky candidates",
        len(aligned),
        len(pixel_cands),
        len(sky_cands),
    )

    return {
        "performed":             True,
        "reference_frame_count": len(aligned),
        "candidates":            sky_cands,
    }
