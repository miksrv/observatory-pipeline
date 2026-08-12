"""
api_client/anomalies.py — the "anomalies" resource: saving the
anomaly set detected for a frame.

See docs/API.md section 3.
"""

from __future__ import annotations

import logging

import config
from ._shared import _make_client, _retry, _RETRYABLE

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ML-7-3: post_anomalies
# ---------------------------------------------------------------------------

@_retry
async def _post_anomalies_with_retry(
    frame_id: str,
    filename: str,
    anomalies: list,
) -> None:
    """Inner retryable core for post_anomalies."""
    url = f"{config.API_BASE_URL}/frames/{frame_id}/anomalies"
    logger.info(
        "POST %s count=%d",
        url,
        len(anomalies),
        extra={"frame_id": frame_id, "log_filename": filename},
    )

    async with _make_client() as client:
        response = await client.post(
            f"/frames/{frame_id}/anomalies",
            json={"filename": filename, "anomalies": anomalies},
        )

        if 400 <= response.status_code < 500:
            logger.error(
                "API rejected POST %s with HTTP %d: %s",
                url,
                response.status_code,
                response.text,
                extra={"frame_id": frame_id, "log_filename": filename},
            )
            return None

        if response.status_code >= 500:
            response.raise_for_status()

    return None


async def post_anomalies(frame_id: str, filename: str, anomalies: list) -> None:
    """
    POST detected anomalies for a processed frame.

    Parameters
    ----------
    frame_id:
        Frame ID returned by post_frame().
    filename:
        Original FITS filename — included in the request body for log correlation.
    anomalies:
        List of anomaly dicts as defined in CLAUDE.md.  An empty list is valid.
    """
    logger.info(
        "Posting %d anomalies for frame_id=%s",
        len(anomalies),
        frame_id,
        extra={"frame_id": frame_id, "log_filename": filename},
    )
    try:
        await _post_anomalies_with_retry(frame_id, filename, anomalies)
    except _RETRYABLE as exc:
        logger.error(
            "All retries exhausted posting anomalies for frame_id=%s: %s",
            frame_id,
            exc,
            extra={"frame_id": frame_id, "log_filename": filename},
        )
    return None
