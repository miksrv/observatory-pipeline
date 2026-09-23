"""
modules/cfa.py — Reduce a one-shot-colour (Bayer/CFA) frame to a mono frame.

The public entry points are:

    is_cfa(header: fits.Header) -> bool
    to_superpixel(src_path: str, dest_path: str) -> dict

A colour camera without a filter wheel records a Bayer mosaic: every pixel
sees only red, green or blue, in a 2×2 pattern (``BAYERPAT = 'RGGB'`` and its
three rotations). Nothing downstream in this pipeline understands that. The
three channels sit on different sky pedestals — ~1900 ADU apart on the first
such dataset (ZWO ASI585MC, NGC 7331) — so the mosaic's checkerboard is read
as noise: `sep`'s background RMS came out 835 on the raw mosaic against 528 on
the same frame reduced to superpixels, and a 10σ extraction found 41 sources
against 197. Image subtraction would be worse still, since `astroalign`'s
sub-pixel resampling mixes the colour channels and leaves a residual at every
star.

Each aligned 2×2 block is therefore averaged into one "superpixel". Any 2×2
window of a 2-periodic pattern holds one of each of its four cells, so the
result does not depend on the Bayer phase at all (``XBAYROFF``/``YBAYROFF``
don't matter); it involves no interpolation, so it adds no correlated noise
(compare `modules/subtraction.py`'s noise-correlation factor); and it gives a
luminance-like band, the same situation as a mono "L" frame. The cost is half
the linear resolution.

A **mean** rather than a sum keeps the ADU range, so ``SATURATION_ADU`` keeps
its meaning — with one exception: a block containing a saturated sub-pixel is
written as its maximum, since the mean of one clipped and three unclipped
pixels drops below the threshold and the clipped core would then escape every
saturation check downstream.

This module only converts; it never decides what happens to the files. That
is `pipeline.py`'s job (the colour original is kept untouched for the
operator), and `modules/catalog_preview.py` converts into a temporary copy.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

import astropy.io.fits as fits
import numpy as np

import config

logger = logging.getLogger(__name__)

# Keywords capture software uses for the mosaic pattern. `BAYERPAT` is the
# de-facto standard (ASIAIR, N.I.N.A., SharpCap, MaxIm); `COLORTYP` is
# written by some older software instead.
_PATTERN_KEYWORDS: tuple[str, ...] = ("BAYERPAT", "COLORTYP")
_PATTERN_RE = re.compile(r"^[RGB]{4}$")

# Removed outright: describe the mosaic, which no longer exists.
_MOSAIC_KEYWORDS: tuple[str, ...] = (
    "BAYERPAT", "COLORTYP", "XBAYROFF", "YBAYROFF", "BAYOFFX", "BAYOFFY",
)

# Doubled: every keyword that describes the size of one pixel, whether in
# microns or arcsec. The ambiguous PIXSCALE/PIXSCALE1 (either unit, see
# fits_header.resolve_pixel_scale_arcsec) double correctly under both
# readings, so the ambiguity survives the conversion unchanged rather than
# being resolved here.
_PIXEL_SIZE_KEYWORDS: tuple[str, ...] = (
    "XPIXSZ", "YPIXSZ", "PIXSIZE", "PIXELSZ", "PIXSIZE1", "PIXSIZE2",
    "PIXSCALE", "PIXSCALE1", "PIXSCALE2", "SECPIX", "SECPIX1", "SECPIX2",
    "XBINNING", "YBINNING", "CCDXBIN", "CCDYBIN",
)

# WCS written by the capture software describes the native pixel grid, and
# astap re-solves every frame anyway. The mount's own RA/DEC/OBJCTRA/OBJCTDEC,
# EQUINOX and RADESYS are *not* WCS here: they seed astap's narrow search and
# feed pointing_error_arcsec, so they stay.
_WCS_KEY_RE = re.compile(
    r"^(WCSAXES|CTYPE\d|CUNIT\d|CRPIX\d|CRVAL\d|CDELT\d|CROTA\d|CD\d_\d|PC\d_\d"
    r"|PV\d_\d+|LONPOLE|LATPOLE|[AB]P?_ORDER|[AB]P?_\d+_\d+|PLTSOLVD)$"
)

_FILTER_KEYWORDS: tuple[str, ...] = ("FILTER", "FILTNAM", "FILTERID")
OSC_FILTER = "OSC"


def _pattern(header: fits.Header) -> str | None:
    for key in _PATTERN_KEYWORDS:
        value = header.get(key)
        if isinstance(value, str) and _PATTERN_RE.match(value.strip().upper()):
            return value.strip().upper()
    return None


def is_cfa(header: fits.Header) -> bool:
    """True for a 2-D Bayer mosaic that has not been converted yet."""
    return (
        header.get("NAXIS") == 2
        and not header.get("CFACONV", False)
        and _pattern(header) is not None
    )


def _superpixel(data: np.ndarray, saturation_adu: float) -> tuple[np.ndarray, int]:
    """2×2 block mean; a block holding a saturated pixel keeps its maximum."""
    height, width = data.shape
    h2, w2 = height - height % 2, width - width % 2
    blocks = data[:h2, :w2].astype(np.float64).reshape(h2 // 2, 2, w2 // 2, 2)
    mean = blocks.mean(axis=(1, 3))
    peak = blocks.max(axis=(1, 3))
    saturated = peak >= saturation_adu
    return np.where(saturated, peak, mean).astype(np.float32), int(saturated.sum())


def _scale_numeric(header: fits.Header, key: str, factor: float) -> None:
    value = header.get(key)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        header[key] = value * factor


def _converted_header(header: fits.Header, pattern: str, src_name: str,
                      new_shape: tuple[int, int]) -> fits.Header:
    hdr = header.copy()
    # Scaling keywords would be re-applied to the float data on write.
    for key in ("BZERO", "BSCALE", "BLANK"):
        hdr.remove(key, ignore_missing=True, remove_all=True)
    for key in _MOSAIC_KEYWORDS:
        hdr.remove(key, ignore_missing=True, remove_all=True)
    for key in [k for k in hdr.keys() if _WCS_KEY_RE.match(k)]:
        hdr.remove(key, ignore_missing=True, remove_all=True)
    for key in _PIXEL_SIZE_KEYWORDS:
        _scale_numeric(hdr, key, 2)
    # A superpixel is the mean of 4 pixels, so one of its ADU stands for 4×
    # the electrons. GAIN is usually the camera's own setting in vendor units
    # (see photometry._resolve_gain) and is deliberately left alone.
    _scale_numeric(hdr, "EGAIN", 4)
    for key, value in (("IMAGEW", new_shape[1]), ("IMAGEH", new_shape[0])):
        if key in hdr:
            hdr[key] = value
    if not any(str(hdr.get(k) or "").strip() for k in _FILTER_KEYWORDS):
        hdr["FILTER"] = (OSC_FILTER, "One-shot colour, no filter recorded")
    hdr["CFACONV"] = (True, "Bayer mosaic reduced to 2x2 superpixels")
    hdr["CFAPAT"] = (pattern, "Bayer pattern of the original frame")
    hdr["HISTORY"] = f"modules/cfa.py: {pattern} mosaic of {src_name} averaged in 2x2 blocks"
    return hdr


def to_superpixel(src_path: str, dest_path: str) -> dict[str, Any]:
    """
    Write a mono 2×2-superpixel version of the CFA frame ``src_path`` to
    ``dest_path`` (which must not exist). ``src_path`` is only read.

    Returns ``{"pattern", "native_shape", "shape", "saturated_blocks"}``.
    Raises ValueError when ``src_path`` is not an unconverted CFA frame.
    """
    with fits.open(src_path, mode="readonly") as hdul:
        header = hdul[0].header
        if not is_cfa(header):
            raise ValueError(f"not an unconverted CFA frame: {src_path}")
        pattern = _pattern(header)
        data = np.asarray(hdul[0].data)
        native_shape = data.shape
        mono, n_saturated = _superpixel(data, config.SATURATION_ADU)
        new_header = _converted_header(header, pattern, os.path.basename(src_path), mono.shape)

    fits.PrimaryHDU(data=mono, header=new_header).writeto(dest_path, overwrite=False)
    return {
        "pattern": pattern,
        "native_shape": native_shape,
        "shape": mono.shape,
        "saturated_blocks": n_saturated,
    }
