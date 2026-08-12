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

**Known pre-existing failures (confirmed present on a clean `git stash`/HEAD, unrelated to
whatever feature you're currently testing — don't try to fix these unless that IS the task):**
as of 2026-08-12 (commit `ee52125`), 6 tests fail out of the box:
  - `tests/test_normalizer.py::TestGenerateNormalizedFilename` — 4 tests (`test_full_light_frame`,
    `test_dark_frame_no_filter`, `test_bias_frame_no_filter`, `test_fractional_exptime`) expect
    single-letter frame-type codes in the generated filename (`M51_L_Ha_300_...`) but
    `modules/normalizer.py`'s current `generate_normalized_filename()` emits the full frame_type
    word instead (`M51_Light_Ha_300_...`) — production code and CLAUDE.md's own documented
    filename convention have drifted from this test file's expectations.
  - `tests/test_astrometry.py::TestNearEdgeFlag::test_margin_scales_with_frame_size`
  - `tests/test_api_client.py::TestGetSourceTracksBatch::test_returns_empty_dict_when_results_not_a_dict`
Before reporting a full pytest run's pass/fail counts to the user, diff against this list (or
re-check via `git stash`) so a pre-existing failure isn't misattributed to whatever you just
changed.
