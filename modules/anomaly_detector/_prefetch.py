"""
modules/anomaly_detector/_prefetch.py — the two-batch-request history/
coverage prefetch that makes _classify.py's per-source classification run
with zero additional API calls.

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
) -> tuple[dict[tuple, list], dict[tuple, list]]:
    """
    Prefetch all historical data needed for anomaly classification in TWO batch requests.

    Returns:
        - narrow_history_by_tile: dict mapping tile -> list of historical sources (MATCH_CONE_ARCSEC)
        - coverage_by_tile: dict mapping tile -> list of covering frames

    For moving object detection (wide cone), we use the same batch data but filter
    with a larger radius client-side.
    """
    extra = {"frame_id": frame_id, "log_filename": log_filename}

    # Collect unique tiles that need queries. Every source needs both a
    # coverage check and a source-history query — see the comment below.
    tiles_needing_sources: set[tuple[float, float]] = set()
    tiles_needing_coverage: set[tuple[float, float]] = set()

    for source in sources:
        ra = float(source.get("ra", 0))
        dec = float(source.get("dec", 0))

        tile = _tile_key(ra, dec)

        # All sources need coverage check (unless already matched in a known catalog that implies stationarity)
        tiles_needing_coverage.add(tile)

        # Every source needs a source-history query: unmatched/MPC sources
        # use it to detect position shifts (moving objects), and
        # catalog-matched sources (Simbad variable/binary/galaxy types) use
        # it further below to detect magnitude changes for VARIABLE_STAR /
        # BINARY_STAR / SUPERNOVA_CANDIDATE. Restricting this query to only
        # catalog_name in (None, "MPC") — as an earlier revision did — meant
        # those three classifications could never fire for any
        # catalog-matched source, since _classify_source_sync() would then
        # always see an empty history for them regardless of what actually
        # happened in previous frames.
        tiles_needing_sources.add(tile)

    logger.info(
        "Prefetching history data: %d tiles for sources, %d tiles for coverage",
        len(tiles_needing_sources),
        len(tiles_needing_coverage),
        extra=extra,
    )

    # Build position lists for batch API calls
    # Use wider radius for the batch query to ensure we capture all needed sources
    # The tile size is 0.1 deg = 360 arcsec, plus we need MOVING_CONE_ARCSEC margin
    batch_radius = max(config.MOVING_CONE_ARCSEC, config.MATCH_CONE_ARCSEC) + 400  # arcsec

    source_positions = [{"ra": t[0], "dec": t[1]} for t in tiles_needing_sources]
    coverage_positions = [{"ra": t[0], "dec": t[1]} for t in tiles_needing_coverage]

    # Execute batch requests concurrently
    narrow_history_by_tile: dict[tuple, list] = {}
    coverage_by_tile: dict[tuple, list] = {}

    try:
        # Make both batch requests in parallel
        if source_positions and coverage_positions:
            sources_result, coverage_result = await asyncio.gather(
                api_client.get_sources_near_batch(source_positions, batch_radius, obs_time),
                api_client.get_frames_covering_batch(coverage_positions, obs_time),
            )
        elif source_positions:
            sources_result = await api_client.get_sources_near_batch(source_positions, batch_radius, obs_time)
            coverage_result = {}
        elif coverage_positions:
            sources_result = {}
            coverage_result = await api_client.get_frames_covering_batch(coverage_positions, obs_time)
        else:
            sources_result = {}
            coverage_result = {}

        # Map results back to tiles
        tiles_sources_list = list(tiles_needing_sources)
        for i, tile in enumerate(tiles_sources_list):
            narrow_history_by_tile[tile] = sources_result.get(str(i), [])

        tiles_coverage_list = list(tiles_needing_coverage)
        for i, tile in enumerate(tiles_coverage_list):
            coverage_by_tile[tile] = coverage_result.get(str(i), [])

        n_history  = sum(len(v) for v in narrow_history_by_tile.values())
        n_coverage = sum(len(v) for v in coverage_by_tile.values())
        logger.info(
            "Batch prefetch complete: %d source history results, %d coverage results",
            n_history, n_coverage,
            extra=extra,
        )
        if n_history == 0 and n_coverage == 0:
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

    return narrow_history_by_tile, coverage_by_tile
