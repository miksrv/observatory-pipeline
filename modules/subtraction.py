"""
modules/subtraction.py — Image subtraction for transient and moving object detection.

Algorithm:
  1. Find N >= SUBTRACTION_MIN_FRAMES archived FITS of same object/filter.
  2. Load new frame data + WCS.
  3. Align each reference frame to the new frame using astroalign triangle matching.
  4. Median-stack aligned frames -> clean reference (removes cosmic rays, hot pixels).
  5. diff = new_frame - reference.
  6. Run SEP detection on positive residuals in diff.
  7. Convert pixel coords to RA/Dec via WCS.
  8. Return candidates list.

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
# Difference-image source detection
# ---------------------------------------------------------------------------

def _detect_diff_sources(diff: np.ndarray) -> list[dict]:
    """
    Detect positive residuals in the difference image using SEP.

    Uses a local background model computed on the diff itself so that
    large-scale gradients (flat-field mismatch, sky gradient) do not
    pollute the threshold estimate.

    Parameters
    ----------
    diff:
        2-D float array: new_frame - reference_stack.

    Returns
    -------
    list[dict]
        Pixel-space candidate dicts with keys: x, y, flux, snr, fwhm, elongation.
        Returns an empty list on any failure.
    """
    try:
        arr = np.ascontiguousarray(diff, dtype=np.float64)
        bkg = sep.Background(arr)
        sub = arr - bkg.back()
        rms = float(bkg.globalrms)
        if rms <= 0:
            return []
        thresh = config.SUBTRACTION_DETECT_SIGMA * rms
        try:
            objs = sep.extract(sub, thresh=thresh, minarea=5)
        except Exception:
            return []

        out: list[dict] = []
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
            out.append({
                "x":          float(obj["x"]),
                "y":          float(obj["y"]),
                "flux":       flux,
                "snr":        snr,
                "fwhm":       fwhm,
                "elongation": a_axis / b_axis,
            })
        return out
    except Exception as exc:
        logger.warning("SEP detection on diff image failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# WCS coordinate conversion
# ---------------------------------------------------------------------------

def _pixel_to_sky(pixel_candidates: list[dict], fits_path: str) -> list[dict]:
    """
    Convert pixel (x, y) detections to sky coordinates (ra, dec) using the
    WCS stored in *fits_path*.

    astropy WCS.pixel_to_world() uses 0-indexed pixel coordinates internally
    (it handles the FITS CRPIX 1-indexed convention transparently), so passing
    SEP's (x, y) directly is correct.

    Parameters
    ----------
    pixel_candidates:
        List of dicts with at least ``x`` and ``y`` keys.
    fits_path:
        Absolute path to the science FITS file (must contain a valid WCS).

    Returns
    -------
    list[dict]
        Candidates with ``x``/``y`` replaced by ``ra``/``dec`` in decimal degrees.
        Candidates for which the conversion fails are silently dropped.
    """
    try:
        wcs: Optional[WCS] = None
        with fits.open(fits_path) as hdul:
            for hdu in hdul:
                if hdu.header.get("CTYPE1"):
                    try:
                        w = WCS(hdu.header)
                        if w.has_celestial:
                            wcs = w
                            break
                    except Exception:
                        continue

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
# Public entry point
# ---------------------------------------------------------------------------

async def run(
    fits_path: str,
    archive_dir: str,
    filter_name: Optional[str],
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
            _from_subtraction=True.
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
    # Detect and project candidates
    # ------------------------------------------------------------------
    pixel_cands = _detect_diff_sources(diff)
    sky_cands   = _pixel_to_sky(pixel_cands, fits_path)

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
