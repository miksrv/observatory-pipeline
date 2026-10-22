"""
modules/anomaly_detector/_frames.py — the stored records of OTHER frames,
looked up through the API: which instrument took a historical epoch, and
whether a frame of the same night really imaged a position.

History and coverage rows carry only a frame id, a time and a position, so
anything about the frame behind them — its camera, plate scale, pointing,
filter, QC flag, whether it was plate-solved at all — comes from here. Records are
cached for the life of the process: a frame's stored record does not change
once registered, and one DETECT_ANOMALIES task walks the same field's
history frame after frame.

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import asyncio
import logging
import math

import api_client
import config

from ._geometry import _haversine_arcsec

logger = logging.getLogger(__name__)

# frame_id -> stored record (GET /frames/{id}), or None when the API had none.
_RECORDS: dict[str, dict | None] = {}
# Objects whose whole frame list has already been loaded into _RECORDS.
_LOADED_OBJECTS: set[str] = set()
# GET /frames pages through at most this many frames per request.
_PAGE = 500
# Concurrent single-frame lookups for ids the object listing did not return.
_CONCURRENCY = 8


def _clear_cache() -> None:
    """Forget every cached record (tests)."""
    _RECORDS.clear()
    _LOADED_OBJECTS.clear()


async def _load_object(object_name: str) -> None:
    """One listing for the whole field instead of one request per frame."""
    if object_name in _LOADED_OBJECTS:
        return
    offset = 0
    while True:
        page = await api_client.get_frames(object_name=object_name, limit=_PAGE, offset=offset)
        for frame in page or []:
            frame_id = frame.get("id")
            if frame_id:
                _RECORDS[str(frame_id)] = frame
        if not page or len(page) < _PAGE:
            break
        offset += _PAGE
    _LOADED_OBJECTS.add(object_name)


async def _frame_records(frame_ids, object_name: str | None = None) -> dict[str, dict]:
    """
    Stored records of `frame_ids`, keyed by id. A frame the API cannot
    return is simply absent — every caller treats a missing record as "this
    frame tells us nothing", never as evidence.

    The field's own listing (`object_name`) is loaded first; a frame filed
    under another OBJECT (capture software keeps the previous target's name)
    is then fetched on its own.
    """
    wanted = {str(f) for f in frame_ids if f}
    if not wanted:
        return {}
    if object_name and wanted - _RECORDS.keys():
        try:
            await _load_object(object_name)
        except Exception as exc:  # a listing failure only costs the batch shortcut
            logger.warning("Listing frames of object=%s failed: %s", object_name, exc)

    missing = sorted(wanted - _RECORDS.keys())
    if missing:
        semaphore = asyncio.Semaphore(_CONCURRENCY)

        async def _fetch(frame_id: str) -> None:
            async with semaphore:
                _RECORDS[frame_id] = await api_client.get_frame(frame_id)

        await asyncio.gather(*(_fetch(f) for f in missing))

    return {f: _RECORDS[f] for f in wanted if _RECORDS.get(f)}


def _finite(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _plate_scale_arcsec(frame: dict | None) -> float | None:
    """The frame's solved plate scale: `fov_deg` is the longer side's length."""
    if not frame:
        return None
    fov = _finite(frame.get("fov_deg"))
    width, height = _finite(frame.get("width_px")), _finite(frame.get("height_px"))
    if fov is None or fov <= 0 or not width or not height:
        return None
    return fov * 3600.0 / max(width, height)


def _same_optics(frame: dict | None, other: dict | None) -> bool | None:
    """
    Whether two frames were taken through the same optics and camera: the
    same `camera` and a plate scale within `SUBTRACTION_REF_MAX_SCALE_DIFF` —
    the tolerance subtraction's reference screen uses for the same question.
    The telescope name is not compared: under INDI it is the mount's.

    None when either side is unknown, so a caller can keep such an epoch
    rather than judge it on no evidence.
    """
    if not frame or not other:
        return None
    camera, other_camera = frame.get("camera"), other.get("camera")
    if camera and other_camera and str(camera).strip().upper() != str(other_camera).strip().upper():
        return False
    scale, other_scale = _plate_scale_arcsec(frame), _plate_scale_arcsec(other)
    if scale is None or other_scale is None:
        return None if not (camera and other_camera) else True
    return abs(other_scale - scale) / scale <= config.SUBTRACTION_REF_MAX_SCALE_DIFF


def _is_solved_ok(frame: dict | None) -> bool:
    """
    A frame whose sources can stand as evidence that something was NOT
    there: QC passed and plate-solved. A frame QC accepted but astap could
    not solve is registered with no sources at all, so its silence proves
    nothing.
    """
    if not frame or frame.get("quality_flag") != "OK":
        return False
    return frame.get("position_angle_deg") is not None


def _covers(frame: dict | None, ra: float, dec: float) -> bool:
    """
    Whether `frame` certainly imaged (ra, dec), away from its edges: inside
    the circle inscribed in the frame's shorter side, less the edge margin.
    Conservative on purpose — the API's coverage circle reaches the corners,
    and a position just outside the frame must not count as "imaged, and
    nothing was there".
    """
    if not frame:
        return False
    scale = _plate_scale_arcsec(frame)
    centre_ra, centre_dec = _finite(frame.get("ra_center")), _finite(frame.get("dec_center"))
    width, height = _finite(frame.get("width_px")), _finite(frame.get("height_px"))
    if scale is None or centre_ra is None or centre_dec is None or not width or not height:
        return False
    radius = min(width, height) * scale * (0.5 - config.EDGE_MARGIN_FRAC)
    return _haversine_arcsec(ra, dec, centre_ra, centre_dec) <= radius
