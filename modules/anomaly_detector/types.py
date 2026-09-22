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


# Alert-worthy types (used for log-level selection). SPACE_DEBRIS is
# deliberately not one: a satellite/aircraft trail is recorded so that a
# genuine fast mover's track is never erased (finding H16) and so that it
# does not pollute UNKNOWN, but it is nothing an operator needs to act on
# (decided 2026-09-22 after the IC3322A test run: 38 trails on 7 frames, all
# ordinary satellite passes). observatory-api decides the persisted
# `is_alert` flag itself from its own AnomalyModel::ALERT_TYPES — see
# docs/API-TASKS.md #2 for the matching change there.
_ALERT_TYPES: frozenset[AnomalyType] = frozenset({
    AnomalyType.SUPERNOVA_CANDIDATE,
    AnomalyType.MOVING_UNKNOWN,
    AnomalyType.UNKNOWN,
})
