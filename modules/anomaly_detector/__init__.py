"""
modules/anomaly_detector — Anomaly detection and classification for the pipeline.

The single public entry point is:

    await anomaly_detector.detect(frame_id, sources, catalog_matches, frame_meta) -> list[dict]

For each source in the frame the module:

1. Collects all unique sky tiles from sources that need history queries.
2. Makes TWO batch API requests: one for source history, one for frame coverage.
3. Classifies each source using the pre-fetched data (no per-source API calls).
4. Calls ephemeris.query() concurrently for all ASTEROID / COMET sources.
5. Returns only actionable anomaly dicts (FIRST_OBSERVATION and KNOWN_CATALOG_NEW
   are never elevated to anomaly records; they are logged but not returned).

This batch approach reduces API calls from O(N) to O(1) where N is the number of sources,
preventing server overload when processing frames with hundreds of detected sources.

Split into files by concern — see docs/anomaly-detector.md for the full mechanics:

  types.py                  AnomalyType enum + the alert-worthy subset
  _otypes.py                Simbad OTYPE substring classifiers (variable/binary/galaxy)
  _geometry.py              haversine separation, sky tiling, radius filtering
  _history.py               historical-magnitude extraction and same-filter restriction
  _movement.py              "has this source actually moved?" evidence
  _prefetch.py              the two-batch-request history/coverage prefetch
  _classify.py              the priority-ordered per-source classification
  _ephemeris_resolution.py  concurrent JPL Horizons lookup for ASTEROID/COMET anomalies
  _detect.py                detect() itself — orchestrates the four files above

`__init__.py` re-exports `detect`/`AnomalyType` for normal use, plus the handful of
private helpers tests/test_anomaly_detector.py exercises directly
(`ad._haversine_arcsec(...)`, etc.) — everything else stays a submodule-internal
implementation detail. `import api_client` and `from modules import ephemeris` are
also done here (unused directly) so that
``patch("modules.anomaly_detector.api_client.get_sources_near_batch", ...)`` and
``patch("modules.anomaly_detector.ephemeris.query", ...)`` — the mocking strategy
tests/test_anomaly_detector.py uses — resolve regardless of which submodule
(_prefetch.py, _ephemeris_resolution.py) actually makes the call.
"""

from __future__ import annotations

import api_client  # noqa: F401 — see docstring above; patch() target resolution needs this attribute.
from modules import ephemeris  # noqa: F401 — same reason, for ephemeris.query patches.

from ._detect import detect
from ._geometry import _haversine_arcsec
from ._history import _history_median_mag, _same_filter_history
from ._movement import _is_position_shifted, _is_still_occupied
from ._otypes import _is_binary_star, _is_galaxy, _is_variable_star
from .types import AnomalyType

__all__ = [
    "detect",
    "AnomalyType",
]
