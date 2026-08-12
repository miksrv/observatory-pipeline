"""
modules/catalog_matcher/_cache.py — the two-tier (in-process + on-disk) query
cache shared by every catalog's own query function.

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import tempfile
from typing import Any

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Query cache — TTL from config.CACHE_TTL_HOURS (default 1h), keyed by
# catalog + sky region.
#
# Two-tier: an in-process dict (fast path — no disk I/O for a key already
# read this run) backed by files under config.CATALOG_CACHE_DIR. The disk
# tier exists so restarting the pipeline (frequent during testing, and after
# every code change without --reload) doesn't throw away query results and
# re-hit Gaia/Simbad/2MASS/Pan-STARRS/MPC for the same sky region — the
# in-process dict alone dies with the process. See config.py's docstring for
# CATALOG_CACHE_DIR on why this must be mounted from outside the container.
# ---------------------------------------------------------------------------

_cache: dict[str, dict[str, Any]] = {}
_CACHE_TTL = datetime.timedelta(hours=config.CACHE_TTL_HOURS)


def _cache_file_path(key: str) -> str:
    """Filesystem-safe path under CATALOG_CACHE_DIR for a given cache key."""
    safe_key = key.replace("/", "_").replace(":", "_")
    return os.path.join(config.CATALOG_CACHE_DIR, f"{safe_key}.json")


def _cache_get(key: str) -> Any | None:
    """
    Return cached data if present and within TTL, else None.

    Checks the in-process dict first; on a miss there, falls back to the
    on-disk cache (populating the in-process dict from it on a hit, so later
    calls in this same run skip the file read). A stale or unreadable/corrupt
    on-disk entry is treated the same as a miss — the caller re-queries the
    network and _cache_set() overwrites the bad file.
    """
    entry = _cache.get(key)
    if entry and (datetime.datetime.now(datetime.timezone.utc) - entry["fetched_at"]) < _CACHE_TTL:
        return entry["data"]

    path = _cache_file_path(key)
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return None  # no on-disk entry either

    age = datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromtimestamp(
        mtime, tz=datetime.timezone.utc
    )
    if age >= _CACHE_TTL:
        return None  # stale on-disk entry — treat as a miss, re-query and overwrite it

    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(
            "Corrupt or unreadable catalog cache file %s: %s — treating as a cache miss",
            path, exc,
        )
        return None

    _cache[key] = {"data": data, "fetched_at": datetime.datetime.now(datetime.timezone.utc)}
    return data


def _cache_set(key: str, data: Any) -> None:
    """
    Store data in the in-process dict AND on disk.

    The disk write is best-effort: any failure (permission error, disk full,
    CATALOG_CACHE_DIR not mounted) is logged and swallowed, degrading to
    in-process-only caching for the rest of this run rather than breaking
    catalog matching. Writes to a temp file and renames into place — cheap
    insurance against a crash mid-write leaving a truncated/corrupt file
    behind that would otherwise poison every read of this key until
    CACHE_TTL expires it.
    """
    _cache[key] = {"data": data, "fetched_at": datetime.datetime.now(datetime.timezone.utc)}

    path = _cache_file_path(key)
    tmp_path: str | None = None
    try:
        os.makedirs(config.CATALOG_CACHE_DIR, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=config.CATALOG_CACHE_DIR, suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp_path, path)
    except OSError as exc:
        logger.warning(
            "Failed to write catalog cache file %s: %s — continuing with in-process cache only",
            path, exc,
        )
        if tmp_path is not None and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
