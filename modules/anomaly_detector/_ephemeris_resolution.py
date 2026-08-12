"""
modules/anomaly_detector/_ephemeris_resolution.py — concurrent ephemeris
lookup for every ASTEROID/COMET anomaly _classify.py produced.

Internal helpers only — not part of this package's public surface. Named
"_ephemeris_resolution" rather than "_ephemeris" to avoid any confusion with
the sibling modules/ephemeris.py module this file calls into.
"""

from __future__ import annotations

import asyncio
import logging

from modules import ephemeris

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Ephemeris resolution
# ---------------------------------------------------------------------------

async def _resolve_ephemerides(
    anomalies: list[dict],
    obs_time: str,
    frame_id: str,
    log_filename: str,
) -> None:
    """
    Concurrently resolve ephemerides for all anomalies that have
    _needs_ephemeris=True and a non-None mpc_designation.

    Mutates anomaly dicts in-place:
      - Sets "ephemeris" key to the dict returned by ephemeris.query(), or None.
      - Removes the private "_needs_ephemeris" sentinel key.
    """
    extra = {"frame_id": frame_id, "log_filename": log_filename}

    pending = [a for a in anomalies if a.get("_needs_ephemeris") and a.get("mpc_designation")]

    if not pending:
        # Still clean up the sentinel on any anomaly that has it without a designation
        for a in anomalies:
            a.pop("_needs_ephemeris", None)
        return

    designations = [a["mpc_designation"] for a in pending]

    logger.debug(
        "Fetching ephemerides concurrently for %d object(s): %s",
        len(designations), designations,
        extra=extra,
    )

    results: list[dict | None] = await asyncio.gather(
        *[ephemeris.query(desig, obs_time) for desig in designations],
        return_exceptions=False,
    )

    for anomaly, eph_result in zip(pending, results):
        if eph_result is None:
            logger.warning(
                "Ephemeris query returned None for designation=%s",
                anomaly["mpc_designation"],
                extra=extra,
            )
        anomaly["ephemeris"] = eph_result

    # Remove the private sentinel from every anomaly dict
    for a in anomalies:
        a.pop("_needs_ephemeris", None)
