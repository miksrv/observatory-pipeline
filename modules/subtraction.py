"""
modules/subtraction.py — Image subtraction for transient and moving object detection.

Algorithm:
  1. Find N >= SUBTRACTION_MIN_FRAMES archived FITS of same object/filter.
  2. Load new frame data + WCS.
  3. Align each reference frame to the new frame using astroalign triangle matching.
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
from typing import Optional

import numpy as np
from astropy.io import fits
from astropy.wcs import WCS
import sep

import config

logger = logging.getLogger(__name__)

_MAX_FRAMES = 10


# ---------------------------------------------------------------------------
# Archive frame discovery
# ---------------------------------------------------------------------------

def _find_archive_frames(archive_dir: str, filter_name: Optional[str]) -> list[str]:
    """
    Return up to _MAX_FRAMES FITS paths from archive_dir, sorted newest-first.

    When filter_name is provided and there are at least SUBTRACTION_MIN_FRAMES
    frames whose normalized filename contains the filter token (e.g. ``_Ha_``),
    only those same-filter frames are returned.  Otherwise all frames are
    returned, allowing cross-filter subtraction as a fallback.

    Parameters
    ----------
    archive_dir:
        Absolute path to the per-object archive directory.
    filter_name:
        Normalized filter string (e.g. "Ha", "R", "L") or None.

    Returns
    -------
    list[str]
        Sorted list of absolute FITS file paths (newest first).
    """
    if not os.path.isdir(archive_dir):
        return []

    all_files: list[str] = []
    for ext in ("*.fits", "*.fit", "*.FITS", "*.FIT"):
        all_files.extend(glob.glob(os.path.join(archive_dir, ext)))

    all_files.sort(key=os.path.getmtime, reverse=True)

    if filter_name:
        token = f"_{filter_name.upper()}_"
        # Compare case-insensitively: normalized filter tokens are not all
        # uppercase (e.g. "Ha"), so a literal uppercased token would never
        # match a mixed-case filename token and this branch would silently
        # always fall through to the cross-filter fallback below.
        matching = [f for f in all_files if token in os.path.basename(f).upper()]
        if len(matching) >= config.SUBTRACTION_MIN_FRAMES:
            return matching[:_MAX_FRAMES]

    return all_files[:_MAX_FRAMES]


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
# Image alignment
# ---------------------------------------------------------------------------

def _align_frame(source: np.ndarray, target: np.ndarray) -> Optional[np.ndarray]:
    """
    Align *source* onto *target* pixel grid using astroalign triangle matching.

    Returns the aligned array as float32, or None if alignment fails (e.g.
    too few stars detected — common for sparse or heavily trailed fields).
    """
    try:
        import astroalign
        aligned, _ = astroalign.register(source, target)
        return np.asarray(aligned, dtype=np.float32)
    except Exception as exc:
        logger.debug("astroalign failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Streak masking — see config.STREAK_* and modules/astrometry.py's identical
# pre-pass (duplicated here rather than imported, mirroring how this module
# already keeps its own independent copy of the FWHM-floor/near-edge logic
# used by modules/astrometry.py).
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
    modules/astrometry.py's identical helper for the full rationale; this
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

    safe_b = np.where(objs["b"] > 0, objs["b"], 1e-6)
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
        modules/astrometry.py's identical flag; no leading underscore, same
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

        streak_mask = _build_streak_mask(sub, rms, pixel_scale_arcsec)
        if streak_mask is not None:
            sub[streak_mask] = 0.0
        thresh = config.SUBTRACTION_DETECT_SIGMA * rms
        try:
            objs = sep.extract(sub, thresh=thresh, minarea=5)
        except Exception:
            return []

        # Near-edge geometry flag — see config.EDGE_MARGIN_FRAC and
        # modules/astrometry.py's identical computation for ordinary
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
            # Gaussian approximation used in modules/astrometry.py.
            flux = float(obj["flux"])
            npix = int(obj["npix"])
            snr = flux / (rms * math.sqrt(npix)) if npix > 0 else 0.0
            a_axis = float(obj["a"])
            b_axis = max(float(obj["b"]), 0.001)
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

            out.append({
                "x":          obj_x,
                "y":          obj_y,
                "flux":       flux,
                "snr":        snr,
                "fwhm":       fwhm,
                "elongation": a_axis / b_axis,
                "near_edge":  near_edge,
            })

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
        sky coordinates every other source in this frame does. Pass None to
        fall back to reading WCS straight from fits_path's own header (see
        _pixel_to_sky()'s docstring for why that's a fallback, not the
        default, in the real pipeline).
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

    archive_files = _find_archive_frames(archive_dir, filter_name)
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
    aligned: list[np.ndarray] = []
    for ref_path in archive_files:
        ref_data = _load_frame_data(ref_path)
        if ref_data is None:
            continue
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
        result_frame = _align_frame(ref_data, new_data)
        if result_frame is not None:
            aligned.append(result_frame)
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
    reference = np.median(np.stack(aligned, axis=0), axis=0).astype(np.float32)
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
