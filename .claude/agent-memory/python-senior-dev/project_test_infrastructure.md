---
name: project_test_infrastructure
description: Test environment setup, pytest configuration, and key pitfalls
type: project
---

`.venv` at project root is NOT committed/persisted across sessions/worktrees (gitignored, and
this repo's sandbox doesn't carry it over) — check `ls .venv` first; if absent, recreate it.
System `python3` in this environment is 3.9 (too old — `pipeline.py`/`watcher.py` use PEP 604
`X | None` unparenthesized annotations that break under 3.9 even with
`from __future__ import annotations` for module-level variable annotations in some cases —
actually confirmed cause: `watcher.py` module-level `_flush_timer: threading.Timer | None = None`
evaluates eagerly regardless). `/opt/homebrew/bin/python3.11` is available on this machine — use
that to create the venv: `/opt/homebrew/bin/python3.11 -m venv .venv && .venv/bin/pip install -r
requirements.txt`. Confirmed python3.12/3.13 are NOT installed here as of 2026-08-12.

`tests/conftest.py` sets `API_BASE_URL` and `API_KEY` environment variables before any module
import, satisfying `config.py`'s `_require()` calls during tests.

`pytest.ini` at project root sets `asyncio_mode = auto`. pytest-asyncio version is 1.3.0.

`watchdog` must be installed in the venv — it was missing from the initial venv setup despite
being in requirements.txt.

**How to apply:** Run `.venv/bin/python -m pytest tests/ -q` to verify the full suite.

The whole suite passes on a clean tree; CI runs it with `.env.test` copied over `.env`, so check both.
