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

import logging
import time
from collections.abc import Callable, Iterable, Sequence
from typing import TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

# The TWOMASS_SOURCES / PANSTARRS_SOURCES entry that means the ESA Gaia
# archive's copy of the catalog (`_gaia_archive`), and how the logs and the
# catalog health report name it; every other entry is a VizieR host name.
GAIA_ARCHIVE = "gaia"
GAIA_ARCHIVE_LABEL = "Gaia archive"


def unique(names: Iterable[str]) -> list[str]:
    """The names in their given order, each once."""
    seen: list[str] = []
    for name in names:
        if name not in seen:
            seen.append(name)
    return seen


def source_label(source: str) -> str:
    """A 2MASS/Pan-STARRS source as the logs name it."""
    return GAIA_ARCHIVE_LABEL if source == GAIA_ARCHIVE else source


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

    def first_answering(
        self,
        configured: Sequence[str],
        ask: Callable[[str], T],
        *,
        cooldown_sec: float,
        catalog: str,
        where: str,
        label: Callable[[str], str] = str,
    ) -> tuple[str, T] | None:
        """
        Ask the configured sources in turn (see `order()`) until one answers;
        `(source, its answer)`, or None when every source failed — the
        caller then returns an empty result without caching it. A source
        whose `ask()` raises is marked down for `cooldown_sec`; one that
        answers after being down is logged as back. `catalog` and `where`
        (the frame's position) only shape the log lines.
        """
        for source in self.order(configured):
            try:
                result = ask(source)
            except Exception as exc:
                self.failed(source, cooldown_sec)
                logger.warning("%s (%s) query failed for %s: %s", catalog, label(source), where, exc)
                continue
            if self.answered(source):
                logger.info("%s at %s answers again", catalog, label(source))
            return source, result
        logger.warning("%s: no source answered for %s — frame gets no %s matching", catalog, where, catalog)
        return None
