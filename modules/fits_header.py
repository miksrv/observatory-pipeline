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
# Observation timestamp
# ---------------------------------------------------------------------------

# A four-digit-year calendar date anywhere in the value, and a clock time
# anywhere in it. Used only to tell which COMPONENTS a keyword carries, never
# to validate the value — parsing that is astropy.time.Time's job.
_DATE_COMPONENT_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_TIME_COMPONENT_RE = re.compile(r"\d{1,2}:\d{2}")


def _resolve_obs_time(hdr: fits.Header) -> Any:
    """
    Resolve the observation timestamp from DATE-OBS / TIME-OBS / MJD-OBS.

    The older FITS convention stores only the calendar date in `DATE-OBS`,
    with the time of day in a separate `TIME-OBS`. Taking the first non-empty
    key on an either/or basis — as this did before — dropped the time of day
    entirely for such a file, silently placing every frame of the night at
    midnight: an hours-scale epoch error for the SkyBot/Horizons queries and
    for history comparisons (audit 2026-08-18, finding C10).

    Resolution order:

    1. `DATE-OBS` already carrying a time component — used as-is (the modern
       convention, and by far the common case).
    2. `DATE-OBS` (date only) combined with `TIME-OBS`. A `TIME-OBS` that is
       itself a full timestamp — some capture software writes one there —
       is preferred over splicing.
    3. `MJD-OBS`, converted to ISO.
    4. Nothing usable → None. A bare `TIME-OBS` time-of-day with no date
       anywhere is deliberately NOT returned: it cannot be parsed by anything
       downstream, and returning it only turns a missing timestamp into a
       corrupt one.

    Note that a pre-1997 `DD/MM/YY` date is not recognized as a date and so
    is passed through unchanged, exactly as before.
    """
    date_obs = _get(hdr, "DATE-OBS")
    time_obs = _get(hdr, "TIME-OBS")
    time_str = str(time_obs).strip() if time_obs is not None else None

    if date_obs is not None:
        date_str = str(date_obs).strip()
        if _TIME_COMPONENT_RE.search(date_str):
            return date_str

        if time_str:
            if _DATE_COMPONENT_RE.search(time_str):
                return time_str
            if _TIME_COMPONENT_RE.search(time_str):
                return f"{date_str}T{time_str}"
            logger.warning(
                "TIME-OBS=%r carries no recognizable time of day — using "
                "DATE-OBS=%r alone, i.e. midnight",
                time_obs, date_obs,
            )
        else:
            logger.warning(
                "DATE-OBS=%r carries no time of day and there is no TIME-OBS "
                "— this frame is timestamped at midnight",
                date_obs,
            )
        return date_str

    if time_str and _DATE_COMPONENT_RE.search(time_str):
        return time_str

    mjd = _to_float(_get(hdr, "MJD-OBS"))
    if mjd is not None:
        from astropy.time import Time
        return Time(mjd, format="mjd").isot

    if time_str:
        logger.warning(
            "TIME-OBS=%r is the only timestamp in this header and carries no "
            "date — no usable observation time",
            time_obs,
        )
    return None


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

# Words a capture program might use in the RA card's own comment to say which
# unit it wrote. Checked longest-first so "hours" isn't matched by "h" in
# something unrelated.
_RA_HOUR_WORDS: tuple[str, ...] = ("hourangle", "hours", "hour", "hrs", "hr")
_RA_DEGREE_WORDS: tuple[str, ...] = ("degrees", "degree", "deg")


# Equinoxes that already mean "the frame everything downstream works in".
# ICRS and FK5 J2000 differ by tens of milliarcsec — far below anything this
# pipeline measures — so neither needs converting.
_J2000_EQUINOXES: tuple[float, ...] = (2000.0,)


def _to_icrs(hdr: fits.Header, ra: float | None, dec: float | None) -> tuple[float | None, float | None]:
    """
    Precess the mount's reported RA/Dec to ICRS when the header says they are
    in some other equinox.

    A mount reporting apparent coordinates of date — "JNow", which many
    planetarium programs and ASCOM drivers default to — writes `EQUINOX`
    (or the older `EPOCH`) as the current year. Those coordinates differ from
    J2000 by the accumulated precession, roughly 50" per year, or about half
    an arcminute today. Read as if they were J2000, that difference lands
    whole in `pointing_error_arcsec`, masking a real mount problem or
    inventing one, and it grows every year (audit 2026-08-18, finding M1).

    `RADESYS` is consulted first, since a header declaring `ICRS` or `FK5`
    without a matching equinox means J2000 by the FITS standard's own
    defaulting rules. An unparseable or absent equinox leaves the coordinates
    alone, which is the previous behaviour.

    Applies only to the mount's own reported target position. The WCS's
    coordinates are astap's, already ICRS by construction, and are never
    routed through here.
    """
    if ra is None or dec is None:
        return ra, dec

    radesys = _get(hdr, "RADESYS", "RADECSYS")
    radesys_str = str(radesys).strip().upper() if radesys is not None else ""
    if radesys_str == "ICRS":
        return ra, dec

    equinox = _to_float(_get(hdr, "EQUINOX", "EPOCH"))
    if equinox is None or equinox in _J2000_EQUINOXES:
        return ra, dec
    if not (1900.0 <= equinox <= 2200.0):
        logger.warning(
            "EQUINOX=%r is not a plausible Julian year — leaving the mount's "
            "reported RA/Dec uncorrected", equinox,
        )
        return ra, dec

    try:
        import astropy.units as _u
        from astropy.coordinates import FK5, SkyCoord
        from astropy.time import Time

        coord = SkyCoord(
            ra=ra * _u.deg, dec=dec * _u.deg,
            frame=FK5(equinox=Time(equinox, format="jyear")),
        ).icrs
        precessed = (float(coord.ra.deg), float(coord.dec.deg))
    except Exception as exc:
        logger.warning(
            "Could not precess the mount's RA/Dec from equinox %s to ICRS (%s) "
            "— using them as-is", equinox, exc,
        )
        return ra, dec

    logger.info(
        "Mount RA/Dec precessed from equinox %.1f to ICRS: "
        "(%.5f, %.5f) -> (%.5f, %.5f)",
        equinox, ra, dec, precessed[0], precessed[1],
    )
    return precessed


def _resolve_numeric_ra(hdr: fits.Header, ra_raw: Any) -> float | None:
    """
    Interpret a bare numeric `RA`/`OBJCTRA` value, which may be in decimal
    degrees or in decimal HOURS.

    The FITS convention is degrees, and that is what this assumed
    unconditionally — but some ASCOM-driven capture software writes decimal
    hours into the same keyword, a factor of 15 (audit 2026-08-18, finding
    H18). The cost is not only a wrong `pointing_error_arcsec`: this same RA
    seeds astap's narrow search centre, so with the wrong unit the narrow
    search reliably misses and every such frame pays the full cost of a blind
    wide search.

    Any value at or above 24 is unambiguous — no clock reaches it — so only
    the [0, 24) range needs deciding, and it is decided from evidence rather
    than guessed:

    1. The card's own comment, when it names a unit ("RA of target [hours]").
       Capture software that writes hours usually says so.
    2. The frame's own `CRVAL1`, when it has one. Whichever interpretation
       lands closer to the header's own idea of where the frame is pointing
       is the right one — a 15x error is never the closer of the two, even
       against a badly mis-pointed mount.
    3. Neither available: degrees, per the convention, with a warning naming
       the ambiguity so an operator whose frames are all mis-solving has
       something to find.
    """
    ra = _to_float(ra_raw)
    if ra is None or not (0.0 <= ra < 24.0):
        return ra

    comment = ""
    for key in ("RA", "OBJCTRA"):
        try:
            if key in hdr:
                comment = str(hdr.comments[key]).lower()
                break
        except Exception:
            comment = ""
    if any(word in comment for word in _RA_HOUR_WORDS):
        logger.info("RA=%s interpreted as decimal hours (per its own card comment)", ra_raw)
        return ra * 15.0
    if any(word in comment for word in _RA_DEGREE_WORDS):
        return ra

    crval1 = _to_float(_get(hdr, "CRVAL1"))
    if crval1 is not None and 0.0 <= crval1 <= 360.0:
        as_degrees = _angular_distance_deg(ra, crval1)
        as_hours = _angular_distance_deg(ra * 15.0, crval1)
        if as_hours < as_degrees:
            logger.info(
                "RA=%s interpreted as decimal hours: %.3f deg sits %.1f deg from "
                "CRVAL1=%.3f, while %.3f deg sits %.1f deg from it",
                ra_raw, ra, as_degrees, crval1, ra * 15.0, as_hours,
            )
            return ra * 15.0
        return ra

    logger.warning(
        "RA=%s is below 24 and this header says nothing about its unit — "
        "assuming decimal degrees per the FITS convention. If this mount "
        "writes decimal HOURS, every pointing error is wrong by 15x and "
        "astap's narrow search is being seeded with the wrong centre",
        ra_raw,
    )
    return ra


def _angular_distance_deg(a: float, b: float) -> float:
    """Separation between two RA values in degrees, the short way round."""
    diff = abs(a - b) % 360.0
    return min(diff, 360.0 - diff)


def _build_dict(hdr: fits.Header) -> dict:
    raw_object = _get(hdr, "OBJECT", "OBJNAME", "TARGET")

    # -- Observation timestamp ------------------------------------------------
    obs_time = _resolve_obs_time(hdr)

    # -- Sky coordinates ------------------------------------------------------
    ra_raw = _get(hdr, "RA", "OBJCTRA")
    dec_raw = _get(hdr, "DEC", "OBJCTDEC")

    # RA: if it looks sexagesimal (contains spaces or colons) use hourangle
    if isinstance(ra_raw, str) and re.search(r"[\s:]", ra_raw):
        ra = _sexagesimal_to_degrees(ra_raw, "hourangle")
    else:
        ra = _resolve_numeric_ra(hdr, ra_raw)

    # Dec: if it looks sexagesimal use deg
    if isinstance(dec_raw, str) and re.search(r"[\s:]", dec_raw):
        dec = _sexagesimal_to_degrees(dec_raw, "deg")
    else:
        dec = _to_float(dec_raw)

    ra, dec = _to_icrs(hdr, ra, dec)

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


# Keywords that unambiguously mean "the physical size of a pixel, in microns".
_PIXEL_SIZE_UM_KEYWORDS: tuple[str, ...] = ("XPIXSZ", "PIXSIZE", "PIXELSZ")
# Keywords whose unit is genuinely ambiguous — some capture software writes
# arcsec per pixel there, some writes the pixel size in microns.
_AMBIGUOUS_SCALE_KEYWORDS: tuple[str, ...] = ("PIXSCALE", "PIXSCALE1")
# Words a card comment might use to settle that ambiguity.
_ARCSEC_WORDS: tuple[str, ...] = ("arcsec", "arcsecond", "asec", "\"/p", "arc-sec")
_MICRON_WORDS: tuple[str, ...] = ("micron", "um", "µm", "micrometre", "micrometer")
# A plate scale no real instrument falls outside of.
_PLATE_SCALE_MIN_ARCSEC: float = 0.01
_PLATE_SCALE_MAX_ARCSEC: float = 200.0


def pixel_size_um(hdr: fits.Header) -> float | None:
    """
    The physical pixel size in microns, from the keywords that can only mean
    that.

    `PIXSCALE1` used to be read here as if it were one of them. It is not:
    like `PIXSCALE`, some software writes arcsec per pixel into it, and the
    two ranges overlap — a 3.76 micron pixel and a 3.76"/px plate scale are
    the same number (audit 2026-08-18, finding M2). It is handled by
    `resolve_pixel_scale_arcsec()` below instead, which has the evidence to
    decide.
    """
    return _to_float(_get(hdr, *_PIXEL_SIZE_UM_KEYWORDS))


def _card_unit(hdr: fits.Header, key: str) -> str | None:
    """"arcsec", "micron", or None — whatever the card's own comment says."""
    try:
        comment = str(hdr.comments[key]).lower()
    except Exception:
        return None
    if any(word in comment for word in _ARCSEC_WORDS):
        return "arcsec"
    if any(word in comment for word in _MICRON_WORDS):
        return "micron"
    return None


def _scale_from_pixel_size(pixel_um: float | None, focal_mm: float | None) -> float | None:
    """206265 x (pixel_um / 1000) / focal_mm, when both are usable."""
    if pixel_um is None or focal_mm is None or focal_mm <= 0 or pixel_um <= 0:
        return None
    scale = 206265.0 * (pixel_um / 1000.0) / focal_mm
    return scale if _PLATE_SCALE_MIN_ARCSEC <= scale <= _PLATE_SCALE_MAX_ARCSEC else None


def resolve_pixel_scale_arcsec(hdr: fits.Header) -> float | None:
    """
    The frame's plate scale in arcsec per pixel, derived from its headers
    alone — what `modules/qc.py` has to work with before anything has plate
    solved.

    `modules/qc.py` and this module used to read the same ambiguous keywords
    differently, and both unsafely: one took `PIXSCALE` as arcsec/px whenever
    it fell in a wide range, the other took `PIXSCALE1` as microns
    unconditionally. The ranges overlap — a 3.76 micron pixel and a 3.76"/px
    scale are the same number — so no range check can separate them (audit
    2026-08-18, finding M2). One resolver now serves both, and it decides from
    evidence in this order:

    1. An unambiguous pixel-size keyword (`XPIXSZ`/`PIXSIZE`/`PIXELSZ`) with
       `FOCALLEN`. Nothing beats knowing both quantities outright.
    2. The ambiguous keyword's own card comment, when it names a unit
       (`PIXSCALE = 1.23 / arcsec/pixel`).
    3. Its value read as arcsec/px, if that is a plausible plate scale —
       logged as the assumption it is, since the same number could be a pixel
       size, and if `FOCALLEN` is present the microns reading is offered in
       the same line so an operator can see both.

    Returns None when the headers simply don't carry enough, which is a
    normal outcome the callers already handle.
    """
    focal_mm = _to_float(_get(hdr, "FOCALLEN"))

    derived = _scale_from_pixel_size(pixel_size_um(hdr), focal_mm)
    if derived is not None:
        return derived

    for key in _AMBIGUOUS_SCALE_KEYWORDS:
        value = _to_float(_get(hdr, key))
        if value is None or value <= 0:
            continue

        unit = _card_unit(hdr, key)
        if unit == "micron":
            from_um = _scale_from_pixel_size(value, focal_mm)
            if from_um is not None:
                return from_um
            continue
        if unit == "arcsec":
            return value if _PLATE_SCALE_MIN_ARCSEC <= value <= _PLATE_SCALE_MAX_ARCSEC else None

        if not (_PLATE_SCALE_MIN_ARCSEC <= value <= _PLATE_SCALE_MAX_ARCSEC):
            continue

        alternative = _scale_from_pixel_size(value, focal_mm)
        logger.info(
            "%s=%s carries no unit in its comment — reading it as %.3f\"/px. "
            "Read instead as a pixel size in microns it would give %s",
            key, value, value,
            f"{alternative:.3f}\"/px" if alternative is not None else "no usable scale",
        )
        return value

    return None


def _extract_sensor(hdr: fits.Header) -> dict:
    binning = _to_int(_get(hdr, "BINNING"))
    return {
        "temp_celsius":         _to_float(_get(hdr, "CCD-TEMP", "CCDTEMP")),
        "temp_setpoint_celsius": _to_float(_get(hdr, "SET-TEMP")),
        "binning_x":            _to_int(_get(hdr, "XBINNING")) or binning,
        "binning_y":            _to_int(_get(hdr, "YBINNING")) or binning,
        "gain":                 _to_float(_get(hdr, "GAIN", "EGAIN")),
        "offset":               _to_float(_get(hdr, "OFFSET")),
        "width_px":             _to_int(_get(hdr, "NAXIS1")),
        "height_px":            _to_int(_get(hdr, "NAXIS2")),
        "pixel_size_um":        pixel_size_um(hdr),
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
