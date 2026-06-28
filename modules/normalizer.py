"""
modules/normalizer.py — Normalize FITS header values and filenames.

This module provides consistent normalization for:
- Object names (M51, M 51, M_51 → M51)
- Filter names (Blue, BLUE, B → B)
- Frame types (Light Frame, light, LIGHT → Light)
- Filenames → standardized format

All functions preserve original values alongside normalized versions
so that nothing is lost.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Object name normalization
# ---------------------------------------------------------------------------

MESSIER_PATTERN = re.compile(r"^M[_\s\-]*(\d+)$", re.IGNORECASE)
CALDWELL_PATTERN = re.compile(r"^C[_\s\-]*(\d+)$", re.IGNORECASE)
SH2_PATTERN = re.compile(r"^SH2?[_\s\-]*(\d+)$", re.IGNORECASE)
ABELL_PATTERN = re.compile(r"^ABELL[_\s\-]*(\d+)$", re.IGNORECASE)

# General catalog pattern: PREFIX[_\s-]*NUMBER → PREFIX_UPPER + NUMBER
# Abell is excluded here and handled by ABELL_PATTERN above (mixed-case output).
_CATALOG_PATTERN = re.compile(
    r"^(NGC|IC|UGC|PGC|MCG|Mrk|Arp|VCC|ESO|UGCA)[_\s\-]*(\d+(?:[_\s\-]\d+)*)$",
    re.IGNORECASE,
)


def normalize_object_name(raw_name: Any) -> tuple[str, str]:
    """
    Normalize an object name to a standard format.

    Returns:
        (normalized_name, raw_name)

    Examples:
        "M 51" → ("M51", "M 51")
        "m_51" → ("M51", "m_51")
        "NGC 1234" → ("NGC1234", "NGC 1234")
        "UGC_6930" → ("UGC6930", "UGC_6930")
        "ugc 6930" → ("UGC6930", "ugc 6930")
        "Mrk_501" → ("MRK501", "Mrk_501")
        "Arp_220" → ("ARP220", "Arp_220")
        "Abell_1" → ("Abell1", "Abell_1")
        "Andromeda Galaxy" → ("Andromeda_Galaxy", "Andromeda Galaxy")
    """
    if raw_name is None:
        return ("_UNKNOWN", None)

    raw_str = str(raw_name).strip()
    if not raw_str:
        return ("_UNKNOWN", None)

    match = MESSIER_PATTERN.match(raw_str)
    if match:
        return (f"M{match.group(1)}", raw_str)

    match = CALDWELL_PATTERN.match(raw_str)
    if match:
        return (f"C{match.group(1)}", raw_str)

    match = SH2_PATTERN.match(raw_str)
    if match:
        return (f"SH2-{match.group(1)}", raw_str)

    match = ABELL_PATTERN.match(raw_str)
    if match:
        return (f"Abell{match.group(1)}", raw_str)

    match = _CATALOG_PATTERN.match(raw_str)
    if match:
        prefix = match.group(1).upper()
        number = re.sub(r"[_\s\-]", "", match.group(2))
        return (f"{prefix}{number}", raw_str)

    normalized = raw_str.replace(" ", "_")
    normalized = re.sub(r"[^A-Za-z0-9_\-+]", "", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    normalized = normalized.strip("_")

    if not normalized:
        return ("_UNKNOWN", raw_str)

    return (normalized, raw_str)


def extract_object_from_filename(filename: str) -> str:
    """
    Extract and normalize an object name from a FITS filename.

    Used as a fallback when the OBJECT FITS header is missing or empty.
    Splits on underscores and collects parts until a frame-type keyword or
    a pure-digit segment (exposure time) is encountered.

    Returns:
        Normalized object name, or "_UNKNOWN" if nothing usable is found.

    Examples:
        "UGC6930_Light_Luminance_300_secs_2021-...fits" → "UGC6930"
        "M51_L_L_60_2024-...fits" → "M51"
        "Andromeda_Galaxy_Light_L_300_2024-...fits" → "Andromeda_Galaxy"
        "NGC1234_Dark_300_2024-...fits" → "NGC1234"
    """
    _FRAME_TYPE_KEYWORDS: frozenset[str] = frozenset({
        "light", "dark", "flat", "bias",
        "l", "d", "f", "b",
        "lightframe", "darkframe", "flatframe", "biasframe",
        "science", "calibration", "object",
    })

    stem = re.sub(r"\.(fits|fit)$", "", filename, flags=re.IGNORECASE)
    parts = stem.split("_")

    collected: list[str] = []
    for part in parts:
        if part.lower() in _FRAME_TYPE_KEYWORDS:
            break
        if re.fullmatch(r"\d+", part):
            break
        collected.append(part)

    if not collected:
        return "_UNKNOWN"

    candidate = "_".join(collected)
    normalized, _ = normalize_object_name(candidate)
    return normalized


# ---------------------------------------------------------------------------
# Filter name normalization
# ---------------------------------------------------------------------------

FILTER_MAP = {
    # Luminance
    "luminance": "L",
    "lum": "L",
    "l": "L",
    "clear": "L",
    "clr": "L",

    # Red
    "red": "R",
    "r": "R",

    # Green
    "green": "G",
    "g": "G",

    # Blue
    "blue": "B",
    "b": "B",

    # Hydrogen-alpha
    "ha": "Ha",
    "h-alpha": "Ha",
    "halpha": "Ha",
    "hydrogen-alpha": "Ha",
    "h_alpha": "Ha",
    "hydrogen alpha": "Ha",

    # Oxygen III
    "oiii": "OIII",
    "o3": "OIII",
    "o-iii": "OIII",
    "oxygen-iii": "OIII",
    "oxygen iii": "OIII",
    "[oiii]": "OIII",

    # Sulfur II
    "sii": "SII",
    "s2": "SII",
    "s-ii": "SII",
    "sulfur-ii": "SII",
    "sulfur ii": "SII",
    "[sii]": "SII",

    # Nitrogen II
    "nii": "NII",
    "n2": "NII",
    "n-ii": "NII",
    "nitrogen-ii": "NII",
    "[nii]": "NII",

    # Standard photometric filters (Johnson-Cousins)
    "u": "U",
    "v": "V",
    "i": "I",
    "u'": "u'",
    "g'": "g'",
    "r'": "r'",
    "i'": "i'",
    "z'": "z'",
}


def normalize_filter_name(raw_filter: Any) -> tuple[str | None, str | None]:
    """
    Normalize a filter name to a standard short form.

    Returns:
        (normalized_filter, raw_filter)

    Examples:
        "Blue" → ("B", "Blue")
        "Luminance" → ("L", "Luminance")
        "H-Alpha" → ("Ha", "H-Alpha")
        "OIII" → ("OIII", "OIII")
        None → (None, None)
    """
    if raw_filter is None:
        return (None, None)

    raw_str = str(raw_filter).strip()
    if not raw_str:
        return (None, None)

    lookup_key = raw_str.lower()
    if lookup_key in FILTER_MAP:
        return (FILTER_MAP[lookup_key], raw_str)

    cleaned = re.sub(r"\s*filter$", "", raw_str, flags=re.IGNORECASE)
    lookup_key = cleaned.lower()
    if lookup_key in FILTER_MAP:
        return (FILTER_MAP[lookup_key], raw_str)

    return (raw_str, raw_str)


# ---------------------------------------------------------------------------
# Frame type normalization
# ---------------------------------------------------------------------------

FRAME_TYPE_MAP = {
    # Light frames
    "light": "Light",
    "light frame": "Light",
    "lightframe": "Light",
    "object": "Light",
    "science": "Light",

    # Dark frames
    "dark": "Dark",
    "dark frame": "Dark",
    "darkframe": "Dark",

    # Flat frames
    "flat": "Flat",
    "flat frame": "Flat",
    "flatframe": "Flat",
    "flat field": "Flat",
    "skyflat": "Flat",
    "domeflat": "Flat",

    # Bias frames
    "bias": "Bias",
    "bias frame": "Bias",
    "biasframe": "Bias",
    "zero": "Bias",
    "offset": "Bias",
}


def normalize_frame_type(raw_type: Any) -> tuple[str | None, str | None]:
    """
    Normalize a frame type (IMAGETYP) to a standard form.

    Returns:
        (normalized_type, raw_type)

    Examples:
        "Light Frame" → ("Light", "Light Frame")
        "dark" → ("Dark", "dark")
        "BIAS" → ("Bias", "BIAS")
    """
    if raw_type is None:
        return (None, None)

    raw_str = str(raw_type).strip()
    if not raw_str:
        return (None, None)

    lookup_key = raw_str.lower()
    if lookup_key in FRAME_TYPE_MAP:
        return (FRAME_TYPE_MAP[lookup_key], raw_str)

    return (raw_str, raw_str)


# ---------------------------------------------------------------------------
# Filename normalization
# ---------------------------------------------------------------------------


def generate_normalized_filename(
    object_name: str,
    frame_type: str | None,
    filter_name: str | None,
    exptime: float | None,
    obs_time: str | None,
    sequence_num: int | None = None,
) -> str:
    """
    Generate a standardized filename from FITS metadata.

    Format:
        Light: {Object}_Light_{Filter}_{Exptime}_{DateTime}[_{Seq}].fits
        Dark:  {Object}_Dark_{Exptime}_{DateTime}[_{Seq}].fits
        Flat:  {Object}_Flat_{Exptime}_{DateTime}[_{Seq}].fits
        Bias:  {Object}_Bias_{Exptime}_{DateTime}[_{Seq}].fits

    Examples:
        M45_Light_B_60_2020-10-15T01-24-51.fits
        M51_Light_Ha_300_2024-03-15T22-01-34.fits
        NGC1234_Light_L_120_2024-03-15T22-01-34_001.fits
        M42_Dark_300_2024-03-15T22-01-34.fits

    Returns:
        Normalized filename string
    """
    parts: list[str] = []

    parts.append(object_name or "_UNKNOWN")

    if frame_type:
        parts.append(frame_type)

    if filter_name and frame_type == "Light":
        parts.append(filter_name)

    if exptime is not None:
        if exptime == int(exptime):
            parts.append(str(int(exptime)))
        else:
            parts.append(f"{exptime:.1f}")

    if obs_time:
        dt_str = obs_time.replace(":", "-")
        dt_str = re.sub(r"\.\d+", "", dt_str)
        parts.append(dt_str)
    else:
        parts.append(datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S"))

    if sequence_num is not None:
        parts.append(f"{sequence_num:03d}")

    return "_".join(parts) + ".fits"


def sanitize_for_filesystem(name: str) -> str:
    r"""
    Make a string safe for use as a directory or file name.

    - Replaces spaces with underscores
    - Removes characters not in [A-Za-z0-9_\-+.]
    - Collapses multiple underscores
    - Strips leading/trailing underscores
    """
    if not name:
        return "_UNKNOWN"

    result = name.replace(" ", "_")
    result = re.sub(r"[^A-Za-z0-9_\-+.]", "", result)
    result = re.sub(r"_+", "_", result)
    result = result.strip("_")

    return result if result else "_UNKNOWN"


def update_fits_object_header(fits_path: str, normalized_name: str) -> None:
    """
    Write a normalized object name back into the FITS file header.

    Tries OBJECT, OBJNAME, TARGET in order — updates the first one found.
    If none exist, adds OBJECT. All I/O exceptions are caught and logged.
    """
    try:
        from astropy.io import fits as astropy_fits  # noqa: PLC0415

        with astropy_fits.open(fits_path, mode="update", output_verify="silentfix") as hdul:
            header = hdul[0].header
            for keyword in ("OBJECT", "OBJNAME", "TARGET"):
                if keyword in header:
                    header[keyword] = normalized_name
                    return
            header["OBJECT"] = normalized_name
    except Exception as exc:
        logger.warning("Could not update FITS OBJECT header in %s: %s", fits_path, exc)


# ---------------------------------------------------------------------------
# Batch normalization helper
# ---------------------------------------------------------------------------


def normalize_headers(raw_headers: dict) -> dict:
    """
    Normalize key fields in a headers dict in-place.

    Takes the output from fits_header.extract_headers() and normalizes:
    - object_name: "M 51" → "M51"
    - observation.object: "M 51" → "M51"
    - observation.filter: "Blue" → "B"
    - observation.frame_type: "Light Frame" → "Light"

    Original values are replaced with normalized versions.

    Returns:
        The same dict with normalized values
    """
    obj_norm, _ = normalize_object_name(raw_headers.get("object_name"))
    raw_headers["object_name"] = obj_norm

    observation = raw_headers.get("observation", {})
    if observation:
        if observation.get("object"):
            obj_obs_norm, _ = normalize_object_name(observation.get("object"))
            observation["object"] = obj_obs_norm

        filt_norm, _ = normalize_filter_name(observation.get("filter"))
        observation["filter"] = filt_norm

        ft_norm, _ = normalize_frame_type(observation.get("frame_type"))
        observation["frame_type"] = ft_norm

    return raw_headers
