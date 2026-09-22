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
) -> bool:
    """
    Inner retryable core for post_anomalies.

    Returns True when the API accepted the batch, False when it rejected it
    with a 4xx — which is not retried, and is as much a loss of the anomaly
    set as an exhausted retry is (audit 2026-08-18, finding H19).
    """
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
            return False

        if response.status_code >= 500:
            response.raise_for_status()

    return True


async def post_anomalies(frame_id: str, filename: str, anomalies: list) -> bool:
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

    Returns
    -------
    bool
        True when the API accepted the batch. False when it did not — an
        exhausted retry or a 4xx rejection — which pipeline.py uses to
        re-queue the work rather than let the anomaly set vanish silently
        (audit 2026-08-18, finding H19). This used to return None
        unconditionally, so a caller had no way to tell the two apart.
    """
    logger.info(
        "Posting %d anomalies for frame_id=%s",
        len(anomalies),
        frame_id,
        extra={"frame_id": frame_id, "log_filename": filename},
    )
    try:
        return bool(await _post_anomalies_with_retry(frame_id, filename, anomalies))
    except _RETRYABLE as exc:
        logger.error(
            "All retries exhausted posting anomalies for frame_id=%s: %s",
            frame_id,
            exc,
            extra={"frame_id": frame_id, "log_filename": filename},
        )
    return False
