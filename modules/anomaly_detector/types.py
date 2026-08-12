"""
modules/anomaly_detector/types.py — the anomaly-type enum and its
alert-worthy subset.
"""

from __future__ import annotations

from enum import Enum

# ---------------------------------------------------------------------------
# Anomaly type enum
# ---------------------------------------------------------------------------
# `str` mixin keeps every member usable as a plain string — json.dumps(),
# dict equality against literal strings (e.g. in tests), and API payload
# serialization all work exactly as they did with the old bare string
# constants. Must stay in sync with the ENUM column definition in
# observatory-api's 2026-04-03-000005_CreateAnomaliesTable.php and with the
# "Anomaly Types Reference" table in CLAUDE.md.


class AnomalyType(str, Enum):
    FIRST_OBSERVATION = "FIRST_OBSERVATION"
    KNOWN_CATALOG_NEW = "KNOWN_CATALOG_NEW"
    VARIABLE_STAR = "VARIABLE_STAR"
    BINARY_STAR = "BINARY_STAR"
    SUPERNOVA_CANDIDATE = "SUPERNOVA_CANDIDATE"
    UNKNOWN = "UNKNOWN"
    ASTEROID = "ASTEROID"
    COMET = "COMET"
    MOVING_UNKNOWN = "MOVING_UNKNOWN"
    SPACE_DEBRIS = "SPACE_DEBRIS"


# Alert-worthy types (used for log-level selection)
_ALERT_TYPES: frozenset[AnomalyType] = frozenset({
    AnomalyType.SUPERNOVA_CANDIDATE,
    AnomalyType.MOVING_UNKNOWN,
    AnomalyType.SPACE_DEBRIS,
    AnomalyType.UNKNOWN,
})
