"""
modules/fits_header.py — Extract and normalize FITS header keywords.

The public entry point is:

    extract_headers(fits_path: str) -> dict

It returns a nested dict whose structure mirrors the POST /frames API payload
so that pipeline.py can forward it with minimal transformation.

All missing keywords are silently set to None — no KeyError is ever raised.
"""

from __future__ import annotations

import re
import logging
from typing import Any

import astropy.io.fits as fits
from astropy.coordinates import Angle
import astropy.units as u

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get(hdr: fits.Header, *keys: str) -> Any:
    """Return the value of the first matching keyword, or None."""
    for key in keys:
        try:
            val = hdr[key]
            if val is not None and val != "":
                return val
        except (KeyError, Exception):
            continue
    return None


def _to_float(value: Any) -> float | None:
    """Cast to float, return None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    """Cast to int, return None on failure."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _sexagesimal_to_degrees(value: Any, unit: str) -> float | None:
    """
    Convert a sexagesimal string (HMS or DMS) to decimal degrees.

    unit: 'hourangle' for RA (HMS), 'deg' for Dec (DMS).
    Returns None if parsing fails.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        ang = Angle(str(value), unit=getattr(u, unit))
        return float(ang.deg)
    except Exception:
        try:
            # Try degree interpretation as fallback
            return float(value)
        except (TypeError, ValueError):
            return None


def sanitize_object_name(name: Any) -> str:
    """
    Convert a raw FITS OBJECT value into a safe filesystem directory name.

    Rules:
    - Spaces → underscores
    - Keep only [A-Za-z0-9_\\-+.]
    - Strip leading/trailing underscores
    - Return '_UNKNOWN' if the result is empty
    """
    if name is None:
        return "_UNKNOWN"
    s = str(name).strip()
    if not s:
        return "_UNKNOWN"
    s = s.replace(" ", "_")
    s = re.sub(r"[^A-Za-z0-9_\-+.]", "", s)
    s = s.strip("_")
    return s if s else "_UNKNOWN"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_headers(fits_path: str) -> dict:
    """
    Extract all relevant FITS headers into a normalized dictionary.

    Missing headers are set to None. The returned dict shape matches the
    POST /frames API payload structure defined in CLAUDE.md.

    Returns:
        {
            "obs_time":     str | None,   # ISO-8601 UTC
            "ra":           float | None, # decimal degrees
            "dec":          float | None, # decimal degrees
            "object_name":  str,          # sanitized for use as directory name
            "observation":  { ... },
            "instrument":   { ... },
            "sensor":       { ... },
            "observer":     { ... },
            "software":     { ... },
        }
    """
    try:
        with fits.open(fits_path, mode="readonly", ignore_missing_simple=True) as hdul:
            hdr = hdul[0].header
            return _build_dict(hdr)
    except Exception as exc:
        logger.error("Failed to read FITS headers from %s: %s", fits_path, exc)
        return _empty_dict()


# ---------------------------------------------------------------------------
# Exposure midpoint
# ---------------------------------------------------------------------------

def midpoint_time(obs_time: Any, exptime: float | None) -> str | None:
    """
    Return the exposure MIDPOINT as an ISO 8601 string, or *obs_time*
    unchanged when it can't be computed.

    Per the FITS convention `DATE-OBS` is the shutter-OPEN time, but a
    moving object's position is only meaningful at the instant its light was
    centroided — the middle of the exposure, not its start. Every query that
    computes where a solar system object was (SkyBot's cone search, JPL
    Horizons) or propagates a proper motion to the observation epoch used the
    start time as-is, giving a systematic (not random) offset between the
    predicted position and the detected centroid: a fast NEO at 20-30"/min on
    a several-minute exposure is already tens of arcsec away by mid-exposure,
    a meaningful fraction of MOVING_CONE_ARCSEC (audit 2026-08-18, finding
    C9).

    Deliberately a separate value from `obs_time` rather than a correction
    applied to it. `obs_time` is what the frame is registered and archived
    under (`POST /frames`, and the DateTime field of the normalized filename)
    and must keep meaning exactly what the header says; this one exists only
    for the handful of consumers that compute a position.

    Returns None only when *obs_time* itself is None.
    """
    if not obs_time:
        return None
    if exptime is None or not (exptime > 0):
        return str(obs_time)

    try:
        from astropy.time import Time
        import astropy.units as _u

        return (Time(str(obs_time), scale="utc") + (exptime / 2.0) * _u.s).isot
    except Exception as exc:
        logger.warning(
            "Cannot compute exposure midpoint from obs_time=%r exptime=%r: %s "
            "— falling back to the exposure start",
            obs_time, exptime, exc,
        )
        return str(obs_time)


# ---------------------------------------------------------------------------
# Extraction groups (called by extract_headers)
# ---------------------------------------------------------------------------

def _build_dict(hdr: fits.Header) -> dict:
    raw_object = _get(hdr, "OBJECT", "OBJNAME", "TARGET")

    # -- Observation timestamp ------------------------------------------------
    obs_time = _get(hdr, "DATE-OBS")
    if obs_time is None:
        obs_time = _get(hdr, "TIME-OBS")
    if obs_time is None:
        mjd = _to_float(_get(hdr, "MJD-OBS"))
        if mjd is not None:
            from astropy.time import Time
            obs_time = Time(mjd, format="mjd").isot

    # -- Sky coordinates ------------------------------------------------------
    ra_raw = _get(hdr, "RA", "OBJCTRA")
    dec_raw = _get(hdr, "DEC", "OBJCTDEC")

    # RA: if it looks sexagesimal (contains spaces or colons) use hourangle
    if isinstance(ra_raw, str) and re.search(r"[\s:]", ra_raw):
        ra = _sexagesimal_to_degrees(ra_raw, "hourangle")
    else:
        ra = _to_float(ra_raw)

    # Dec: if it looks sexagesimal use deg
    if isinstance(dec_raw, str) and re.search(r"[\s:]", dec_raw):
        dec = _sexagesimal_to_degrees(dec_raw, "deg")
    else:
        dec = _to_float(dec_raw)

    return {
        "obs_time":    obs_time,
        "obs_time_mid": midpoint_time(obs_time, _to_float(_get(hdr, "EXPTIME", "EXPOSURE"))),
        "ra":          ra,
        "dec":         dec,
        "object_name": sanitize_object_name(raw_object),
        "observation": _extract_observation(hdr, raw_object),
        "instrument":  _extract_instrument(hdr),
        "sensor":      _extract_sensor(hdr),
        "observer":    _extract_observer(hdr),
        "software":    _extract_software(hdr),
    }


def _extract_observation(hdr: fits.Header, raw_object: Any) -> dict:
    return {
        "object":     raw_object,
        "exptime":    _to_float(_get(hdr, "EXPTIME", "EXPOSURE")),
        "filter":     _get(hdr, "FILTER", "FILTNAM", "FILTERID"),
        "frame_type": _get(hdr, "IMAGETYP", "FRAME"),
        "airmass":    _to_float(_get(hdr, "AIRMASS")),
    }


def _extract_instrument(hdr: fits.Header) -> dict:
    return {
        "telescope":       _get(hdr, "TELESCOP"),
        "camera":          _get(hdr, "INSTRUME", "CAMERA"),
        "focal_length_mm": _to_float(_get(hdr, "FOCALLEN")),
        "aperture_mm":     _to_float(_get(hdr, "APTDIA", "APERTURE")),
    }


def _extract_sensor(hdr: fits.Header) -> dict:
    binning = _to_int(_get(hdr, "BINNING"))
    # Pixel size in microns - try multiple keyword variations
    pixel_size_um = _to_float(_get(hdr, "XPIXSZ", "PIXSIZE", "PIXSCALE1", "PIXELSZ"))
    return {
        "temp_celsius":         _to_float(_get(hdr, "CCD-TEMP", "CCDTEMP")),
        "temp_setpoint_celsius": _to_float(_get(hdr, "SET-TEMP")),
        "binning_x":            _to_int(_get(hdr, "XBINNING")) or binning,
        "binning_y":            _to_int(_get(hdr, "YBINNING")) or binning,
        "gain":                 _to_float(_get(hdr, "GAIN", "EGAIN")),
        "offset":               _to_float(_get(hdr, "OFFSET")),
        "width_px":             _to_int(_get(hdr, "NAXIS1")),
        "height_px":            _to_int(_get(hdr, "NAXIS2")),
        "pixel_size_um":        pixel_size_um,
    }


def _extract_observer(hdr: fits.Header) -> dict:
    return {
        "name":       _get(hdr, "OBSERVER", "AUTHOR"),
        "site_name":  _get(hdr, "SITENAME", "OBSERVAT"),
        "site_lat":   _to_float(_get(hdr, "SITELAT")),
        "site_lon":   _to_float(_get(hdr, "SITELONG")),
        "site_elev_m": _to_float(_get(hdr, "SITEELEV")),
    }


def _extract_software(hdr: fits.Header) -> dict:
    return {
        "capture": _get(hdr, "SWCREATE", "SOFTWARE"),
    }


def _empty_dict() -> dict:
    """Return the correct structure with all values set to None."""
    return {
        "obs_time":    None,
        "obs_time_mid": None,
        "ra":          None,
        "dec":         None,
        "object_name": "_UNKNOWN",
        "observation": {
            "object": None, "exptime": None, "filter": None,
            "frame_type": None, "airmass": None,
        },
        "instrument": {
            "telescope": None, "camera": None,
            "focal_length_mm": None, "aperture_mm": None,
        },
        "sensor": {
            "temp_celsius": None, "temp_setpoint_celsius": None,
            "binning_x": None, "binning_y": None,
            "gain": None, "offset": None,
            "width_px": None, "height_px": None,
            "pixel_size_um": None,
        },
        "observer": {
            "name": None, "site_name": None,
            "site_lat": None, "site_lon": None, "site_elev_m": None,
        },
        "software": {"capture": None},
    }
