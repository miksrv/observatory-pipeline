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

import re
from datetime import datetime
from typing import Any


# ---------------------------------------------------------------------------
# Object name normalization
# ---------------------------------------------------------------------------

# Common Messier/NGC/IC patterns
MESSIER_PATTERN = re.compile(r"^M[_\s\-]*(\d+)$", re.IGNORECASE)
NGC_PATTERN = re.compile(r"^NGC[_\s\-]*(\d+)$", re.IGNORECASE)
IC_PATTERN = re.compile(r"^IC[_\s\-]*(\d+)$", re.IGNORECASE)
CALDWELL_PATTERN = re.compile(r"^C[_\s\-]*(\d+)$", re.IGNORECASE)
SH2_PATTERN = re.compile(r"^SH2?[_\s\-]*(\d+)$", re.IGNORECASE)
ABELL_PATTERN = re.compile(r"^ABELL[_\s\-]*(\d+)$", re.IGNORECASE)


def normalize_object_name(raw_name: Any) -> tuple[str, str]:
    """
    Normalize an object name to a standard format.

    Returns:
        (normalized_name, raw_name)
        
    Examples:
        "M 51" → ("M51", "M 51")
        "m_51" → ("M51", "m_51")
        "NGC 1234" → ("NGC1234", "NGC 1234")
        "ngc_1234" → ("NGC1234", "ngc_1234")
        "Andromeda Galaxy" → ("Andromeda_Galaxy", "Andromeda Galaxy")
    """
    if raw_name is None:
        return ("_UNKNOWN", None)
    
    raw_str = str(raw_name).strip()
    if not raw_str:
        return ("_UNKNOWN", None)
    
    # Try to match standard catalog patterns
    
    # Messier: M51, M 51, m_51, M-51 → M51
    match = MESSIER_PATTERN.match(raw_str)
    if match:
        return (f"M{match.group(1)}", raw_str)
    
    # NGC: NGC1234, NGC 1234, ngc_1234 → NGC1234
    match = NGC_PATTERN.match(raw_str)
    if match:
        return (f"NGC{match.group(1)}", raw_str)
    
    # IC: IC1234, IC 1234 → IC1234
    match = IC_PATTERN.match(raw_str)
    if match:
        return (f"IC{match.group(1)}", raw_str)
    
    # Caldwell: C1, C 1 → C1
    match = CALDWELL_PATTERN.match(raw_str)
    if match:
        return (f"C{match.group(1)}", raw_str)
    
    # Sharpless: Sh2-1, SH2 1 → SH2-1
    match = SH2_PATTERN.match(raw_str)
    if match:
        return (f"SH2-{match.group(1)}", raw_str)
    
    # Abell: Abell 1, ABELL_1 → Abell1
    match = ABELL_PATTERN.match(raw_str)
    if match:
        return (f"Abell{match.group(1)}", raw_str)
    
    # For other names: replace spaces with underscores, keep alphanumeric + underscore
    normalized = raw_str.replace(" ", "_")
    normalized = re.sub(r"[^A-Za-z0-9_\-+]", "", normalized)
    normalized = re.sub(r"_+", "_", normalized)  # collapse multiple underscores
    normalized = normalized.strip("_")
    
    if not normalized:
        return ("_UNKNOWN", raw_str)
    
    return (normalized, raw_str)


# ---------------------------------------------------------------------------
# Filter name normalization
# ---------------------------------------------------------------------------

# Map of filter aliases to standard names
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
    
    # Try exact match first (case-insensitive)
    lookup_key = raw_str.lower()
    if lookup_key in FILTER_MAP:
        return (FILTER_MAP[lookup_key], raw_str)
    
    # Try removing common suffixes like " Filter" or " filter"
    cleaned = re.sub(r"\s*filter$", "", raw_str, flags=re.IGNORECASE)
    lookup_key = cleaned.lower()
    if lookup_key in FILTER_MAP:
        return (FILTER_MAP[lookup_key], raw_str)
    
    # If no match found, return the original with first letter capitalized
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
    
    # Return original if no match
    return (raw_str, raw_str)


# ---------------------------------------------------------------------------
# Filename normalization
# ---------------------------------------------------------------------------

# Short frame type codes for filename
FRAME_TYPE_CODES = {
    "Light": "L",
    "Dark": "D",
    "Flat": "F",
    "Bias": "B",
}


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

    Format: {Object}_{FrameType}_{Filter}_{Exptime}_{DateTime}[_{Seq}].fits
    
    Examples:
        M45_L_B_60_2020-10-15T01-24-51.fits
        M51_L_Ha_300_2024-03-15T22-01-34.fits
        NGC1234_L_L_120_2024-03-15T22-01-34_001.fits
        M42_D_300_2024-03-15T22-01-34.fits (no filter for dark/bias)
        
    Returns:
        Normalized filename string
    """
    parts = []
    
    # Object name (required)
    parts.append(object_name or "_UNKNOWN")
    
    # Frame type (use short code)
    if frame_type:
        frame_code = FRAME_TYPE_CODES.get(frame_type, frame_type[0].upper() if frame_type else "L")
        parts.append(frame_code)
    
    # Filter (skip for dark/bias)
    if filter_name and frame_type not in ("Dark", "Bias"):
        parts.append(filter_name)
    
    # Exposure time (no 's' suffix, just the number)
    if exptime is not None:
        if exptime == int(exptime):
            parts.append(str(int(exptime)))
        else:
            parts.append(f"{exptime:.1f}")
    
    # Observation time
    if obs_time:
        # Convert ISO time to filename-safe format
        # 2024-03-15T22:01:34 → 2024-03-15T22-01-34
        dt_str = obs_time.replace(":", "-")
        # Remove fractional seconds if present
        dt_str = re.sub(r"\.\d+", "", dt_str)
        parts.append(dt_str)
    else:
        # Use current time as fallback
        parts.append(datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S"))
    
    # Sequence number (optional)
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
    # Normalize object name
    obj_norm, _ = normalize_object_name(raw_headers.get("object_name"))
    raw_headers["object_name"] = obj_norm
    
    # Normalize observation fields
    observation = raw_headers.get("observation", {})
    if observation:
        # Normalize object in observation too
        if observation.get("object"):
            obj_obs_norm, _ = normalize_object_name(observation.get("object"))
            observation["object"] = obj_obs_norm
        
        # Normalize filter
        filt_norm, _ = normalize_filter_name(observation.get("filter"))
        observation["filter"] = filt_norm
        
        # Normalize frame type
        ft_norm, _ = normalize_frame_type(observation.get("frame_type"))
        observation["frame_type"] = ft_norm
    
    return raw_headers

