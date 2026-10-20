"""
modules/archive_layout.py — where a frame lives inside FITS_ARCHIVE.

config.py owns the archive root; this module owns the layout under it, so no
other module composes `os.path.join(config.FITS_ARCHIVE, …)` by hand. The
archive is also the root of a public read-only FTP share, which is what the
layout is shaped for:

    FITS_ARCHIVE/
    ├── _calibration/{night}/{Bias|Dark|Flat}/   every calibration frame
    ├── _UNKNOWN/{filter}/                       Lights without OBJECT
    └── {object}/{filter}/                       Lights; NONE = no filter

- The frame TYPE decides calibration, never the OBJECT card: capture software
  writes the current target into Darks and Flats, so branching on it would
  scatter one camera's Darks across every object of the night.
- {night} is the UTC date of `obs_time − 12 h`, so a 02:00 Dark lands in the
  folder of the evening it belongs to.
- No instrument level: `M51/L/` holds every L frame of M51, whatever optics
  took it. Subtraction screens references by TELESCOP/INSTRUME instead
  (modules/subtraction.py).
- The leading underscore of `_calibration`/`_UNKNOWN` cannot collide with an
  object directory — fits_header.sanitize_object_name() strips it.

The observatory-api database is the only index: it stores a frame's basename
(`frames.filename`) and its object, and `find_archived()` turns the two back
into a path. Normalized basenames are unique (DateTime + sequence), so a
search is unambiguous in practice.
"""
from __future__ import annotations

import glob
import logging
import os
from datetime import timedelta, timezone
from typing import Any

import config
from modules.fits_header import sanitize_object_name

logger = logging.getLogger(__name__)

CALIBRATION_DIR = "_calibration"
CALIBRATION_FRAME_TYPES = frozenset({"Dark", "Flat", "Bias"})
UNKNOWN = "_UNKNOWN"
# Directory and filename token for a Light/Flat without a filter. Not a short
# code such as "N": the normalizer passes unknown filter names through, and a
# real filter could be labelled that way.
NO_FILTER = "NONE"

# A night starts at local noon, approximated in UTC: everything taken before
# 12:00 UTC belongs to the previous evening.
_NIGHT_SHIFT = timedelta(hours=12)


def _single_token(value: Any) -> str | None:
    """
    *value* sanitized into one filename token, or None when nothing is left.

    Underscores (spaces included) become hyphens: subtraction.py parses a
    normalized filename positionally on `_`, so a filter or camera name must
    not add tokens of its own.
    """
    if value is None:
        return None
    token = sanitize_object_name(str(value))
    if token == UNKNOWN:
        return None
    token = token.replace("_", "-")
    return token or None


def filter_token(filter_name: Any) -> str:
    """The filter as a directory/filename token; `NONE` when there is none."""
    return _single_token(filter_name) or NO_FILTER


def camera_token(camera: Any) -> str:
    """The camera (`INSTRUME`) as a filename token; `_UNKNOWN` when missing."""
    return _single_token(camera) or UNKNOWN


def object_token(object_name: Any) -> str:
    """The object directory name; `_UNKNOWN` when missing."""
    # sanitize_object_name() would strip _UNKNOWN's own leading underscore.
    if not object_name or object_name == UNKNOWN:
        return UNKNOWN
    return sanitize_object_name(object_name)


def night_date(obs_time: Any) -> str:
    """
    The observing night *obs_time* belongs to, as `YYYY-MM-DD`: the UTC date
    of `obs_time − 12 h`. `_UNKNOWN` when the timestamp is missing or
    unreadable.
    """
    if not obs_time:
        return UNKNOWN
    try:
        from astropy.time import Time  # noqa: PLC0415

        moment = Time(str(obs_time), scale="utc").to_datetime(timezone=timezone.utc)
    except Exception as exc:
        logger.warning("Cannot read obs_time=%r for the calibration night: %s", obs_time, exc)
        return UNKNOWN
    return (moment - _NIGHT_SHIFT).date().isoformat()


def object_dir(object_name: Any) -> str:
    """`FITS_ARCHIVE/{object}` — the root subtraction draws references from."""
    return os.path.join(config.FITS_ARCHIVE, object_token(object_name))


def light_dir(object_name: Any, filter_name: Any) -> str:
    """`FITS_ARCHIVE/{object}/{filter or NONE}`."""
    return os.path.join(object_dir(object_name), filter_token(filter_name))


def calibration_dir(frame_type: str, obs_time: Any) -> str:
    """`FITS_ARCHIVE/_calibration/{night}/{Bias|Dark|Flat}`."""
    return os.path.join(config.FITS_ARCHIVE, CALIBRATION_DIR, night_date(obs_time), frame_type)


def is_calibration(frame_type: Any) -> bool:
    return frame_type in CALIBRATION_FRAME_TYPES


def destination_for(header: dict) -> str:
    """
    The archive directory for a frame, from its (normalized) header dict as
    fits_header.extract_headers() + normalizer.normalize_headers() return it.
    """
    observation = header.get("observation") or {}
    frame_type = observation.get("frame_type")
    if is_calibration(frame_type):
        return calibration_dir(frame_type, header.get("obs_time"))
    return light_dir(header.get("object_name"), observation.get("filter"))


def find_archived(filename: str, object_name: Any = None) -> str | None:
    """
    The archived path of *filename* (a basename), or None.

    Searched in tiers, the first tier with a hit wins: the object's own
    filter directories (when *object_name* is given), then every object's,
    then calibration, then the legacy flat `{object}/` layout — so an archive
    migrated only halfway (tools/migrate_archive_layout.py) still resolves.
    More than one hit in a tier is logged and the first (sorted) is used:
    normalized names are expected to be unique across the whole archive.
    """
    name = glob.escape(os.path.basename(filename))
    root = glob.escape(config.FITS_ARCHIVE)

    tiers: list[list[str]] = []
    if object_name:
        own = glob.escape(object_token(object_name))
        tiers.append([os.path.join(root, own, "*", name), os.path.join(root, own, name)])
    tiers.append([os.path.join(root, "*", "*", name)])
    tiers.append([os.path.join(root, glob.escape(CALIBRATION_DIR), "*", "*", name)])
    tiers.append([os.path.join(root, "*", name)])

    for patterns in tiers:
        matches = sorted({m for p in patterns for m in glob.glob(p) if os.path.isfile(m)})
        if not matches:
            continue
        if len(matches) > 1:
            logger.warning(
                "Archived filename %r found more than once; using the first: %s",
                filename, matches,
            )
        return matches[0]
    return None
