"""
What each catalog's query came to during one `match()` call.

A failed catalog query returns `[]` — the frame goes on with partial
results (CLAUDE.md, "External catalogs") — and an empty answer looks exactly
like a field the catalog has nothing in. The frame record must still say
which catalogs it was matched against, so a frame matched while Gaia
answered 503 can be found and re-analysed; each query reports its outcome
here, and `match()` hands the collected outcomes to its caller.

The collector is a ContextVar set only for the duration of `match()`:
queries made outside it (forced photometry's `get_gaia_stars()` cache hits,
tests calling a query directly) record nothing.
"""

from __future__ import annotations

from contextvars import ContextVar

# Outcome values — the wire contract of POST /frames' `processing.catalogs`.
OK = "ok"                          # the catalog answered (possibly with nothing)
FAILED = "failed"                  # every configured source failed
DISABLED = "disabled"              # no source configured (e.g. PANSTARRS_SOURCES empty)
NOT_APPLICABLE = "not_applicable"  # the field is outside the catalog's coverage

_outcomes: ContextVar[dict[str, str] | None] = ContextVar("catalog_outcomes", default=None)


def record(catalog: str, outcome: str) -> None:
    """Note `catalog`'s outcome for the `match()` call in progress, if any."""
    collected = _outcomes.get()
    if collected is not None:
        collected[catalog] = outcome


def start() -> object:
    """Begin collecting; returns the token `finish()` needs."""
    return _outcomes.set({})


def finish(token: object) -> dict[str, str]:
    """Stop collecting and return what was recorded since `start()`."""
    collected = _outcomes.get() or {}
    _outcomes.reset(token)  # type: ignore[arg-type]
    return collected
