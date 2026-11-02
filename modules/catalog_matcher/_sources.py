"""
modules/catalog_matcher/_sources.py — which of a catalog's sources to ask first.

Several catalogs here can be read from more than one place: SkyBot or JPL
for known asteroids and comets (`_mpc`), the CDS Simbad or its Harvard
mirror (`_simbad`), the CDS VizieR, another VizieR mirror or the ESA Gaia
archive's copy for 2MASS and Pan-STARRS (`_2mass`, `_panstarrs`). Each
module keeps a configured list in order of preference; the first source
that answers decides the frame, an empty answer included, and a source that
failed is asked last for a cooldown, so that during an outage lasting days
(SkyBot from 2026-10-23, CDS from 2026-10-28) a frame no longer waits out a
dead service's timeouts before reaching the one that works. A down source
is not skipped outright — it is still asked when every other one fails too.

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Sequence


def unique(names: Iterable[str]) -> list[str]:
    """The names in their given order, each once."""
    seen: list[str] = []
    for name in names:
        if name not in seen:
            seen.append(name)
    return seen


class SourceRotation:
    """
    Per-catalog memory of which sources failed and until when. One instance
    per catalog module; `down_until` maps a source name to the
    `time.monotonic()` before which it counts as down.
    """

    def __init__(self) -> None:
        self.down_until: dict[str, float] = {}

    def order(self, configured: Sequence[str]) -> list[str]:
        """`configured` in its own order, the sources currently down moved to the back."""
        now = time.monotonic()
        up = [s for s in configured if self.down_until.get(s, 0.0) <= now]
        return up + [s for s in configured if s not in up]

    def failed(self, source: str, cooldown_sec: float) -> None:
        """A source failed (after its own retries): ask it last for `cooldown_sec`."""
        self.down_until[source] = time.monotonic() + cooldown_sec

    def answered(self, source: str) -> bool:
        """A source answered: forget its failure. True when it had been marked down."""
        return self.down_until.pop(source, None) is not None
