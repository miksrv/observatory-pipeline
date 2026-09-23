"""
modules/anomaly_detector/_prefetch.py — the batched history/coverage prefetch
that makes _classify.py's per-source classification run with zero additional
API calls.

Internal helpers only — not part of this package's public surface. See
docs/anomaly-detector.md's "Batch API prefetch" section for the full
request/response shapes.
"""

from __future__ import annotations

import asyncio
import logging

import api_client
import config

from ._geometry import _tile_key

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Batch data prefetch
# ---------------------------------------------------------------------------

async def _prefetch_history_data(
    sources: list[dict],
    obs_time: str,
    frame_id: str,
    log_filename: str,
) -> tuple[list[list[dict]], dict[int, list[dict]], dict[tuple, list]]:
    """
    Prefetch everything classification needs, in at most three concurrent
    batch requests, each shaped to what its consumer actually reads.

    Returns ``(narrow_by_source, wide_by_source, coverage_by_tile)``:

    - ``narrow_by_source[i]`` — historical detections within
      MATCH_CONE_ARCSEC of ``sources[i]``, every epoch: the existence check
      (FIRST_OBSERVATION / UNKNOWN / KNOWN_CATALOG_NEW) and the light curve
      behind every Δmag branch. Needed for every source.
    - ``wide_by_source[i]`` — the moving-object pool, only for an
      UNCATALOGUED ``sources[i]`` (the only kind that ever reaches
      `_movement._is_position_shifted()`), out to the widest cone
      `_movement._find_wide_history()` can apply, and only historical
      detections that are themselves uncatalogued or MPC
      (``uncatalogued_only``). A catalogued star does not move: when it is
      missing from tonight's frame that is a non-detection — too faint, cloud,
      the frame edge — not a vacated position, and counting it as evidence of
      motion was a route to a false MOVING_UNKNOWN.
    - ``coverage_by_tile`` — frames covering each 0.1° tile.

    This replaces one query per 0.1° tile with a radius of
    ``MOVING_CONE_MAX_ARCSEC + 400"`` (the tile's own half-diagonal on top),
    used for both cones and filtered client-side. On a field smaller than
    that radius every tile query returned the whole field's history, so each
    observation arrived once per tile — 870 828 rows for a database of 62 353
    observations on the 228-frame NGC 7331 run, and the worker was OOM-killed
    in the middle of it. Growth is now bounded by the field's real history,
    and the wide pool by how much of it no catalog explains.

    An API that predates ``uncatalogued_only`` ignores it and returns
    catalogued history in the wide pool as well: the previous semantics,
    without the saving.
    """
    extra = {"frame_id": frame_id, "log_filename": log_filename}

    narrow_by_source: list[list[dict]] = [[] for _ in sources]
    wide_by_source: dict[int, list[dict]] = {}
    coverage_by_tile: dict[tuple, list] = {}

    # Index of every source with a usable position; the API answers by the
    # position's index in the request, which is mapped back through these.
    narrow_idx: list[int] = []
    narrow_positions: list[dict] = []
    wide_idx: list[int] = []
    wide_positions: list[dict] = []
    tiles: set[tuple[float, float]] = set()

    for i, source in enumerate(sources):
        try:
            ra = float(source["ra"])
            dec = float(source["dec"])
        except (KeyError, TypeError, ValueError):
            continue
        narrow_idx.append(i)
        narrow_positions.append({"ra": ra, "dec": dec})
        if source.get("catalog_name") is None:
            wide_idx.append(i)
            wide_positions.append({"ra": ra, "dec": dec})
        tiles.add(_tile_key(ra, dec))

    tile_list = list(tiles)
    coverage_positions = [{"ra": t[0], "dec": t[1]} for t in tile_list]

    # Centred on the source itself now, so no tile margin is needed on top of
    # the widest per-candidate cone _find_wide_history() may apply (audit
    # 2026-08-18, finding H3: a candidate the API never returned cannot be
    # filtered back in afterwards).
    wide_radius = max(config.MOVING_CONE_ARCSEC, config.MOVING_CONE_MAX_ARCSEC)

    logger.info(
        "Prefetching history: %d narrow-cone position(s), %d uncatalogued "
        "wide-cone position(s), %d coverage tile(s)",
        len(narrow_positions), len(wide_positions), len(coverage_positions),
        extra=extra,
    )

    async def _none() -> dict:
        return {}

    try:
        narrow_result, wide_result, coverage_result = await asyncio.gather(
            api_client.get_sources_near_batch(
                narrow_positions, config.MATCH_CONE_ARCSEC, obs_time
            ) if narrow_positions else _none(),
            api_client.get_sources_near_batch(
                wide_positions, wide_radius, obs_time, uncatalogued_only=True
            ) if wide_positions else _none(),
            api_client.get_frames_covering_batch(
                coverage_positions, obs_time
            ) if coverage_positions else _none(),
        )

        for k, i in enumerate(narrow_idx):
            narrow_by_source[i] = narrow_result.get(str(k), [])
        for k, i in enumerate(wide_idx):
            wide_by_source[i] = wide_result.get(str(k), [])
        for k, tile in enumerate(tile_list):
            coverage_by_tile[tile] = coverage_result.get(str(k), [])

        n_narrow = sum(len(v) for v in narrow_by_source)
        n_wide = sum(len(v) for v in wide_by_source.values())
        n_coverage = sum(len(v) for v in coverage_by_tile.values())
        logger.info(
            "Batch prefetch complete: %d narrow-cone history results, %d "
            "wide-cone results, %d coverage results",
            n_narrow, n_wide, n_coverage,
            extra=extra,
        )
        if n_narrow == 0 and n_coverage == 0:
            logger.warning(
                "API returned empty history AND empty coverage — "
                "either this is the first frame of this field, or the API batch "
                "endpoints (/sources/near/batch, /frames/covering/batch) are not "
                "returning saved data. All sources will be classified as "
                "FIRST_OBSERVATION and no anomalies will be reported.",
                extra=extra,
            )

    except Exception as exc:
        logger.error(
            "Batch prefetch failed: %s — will classify without history data",
            exc,
            extra=extra,
        )

    return narrow_by_source, wide_by_source, coverage_by_tile
