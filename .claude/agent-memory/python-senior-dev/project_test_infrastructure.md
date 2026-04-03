---
name: project_test_infrastructure
description: Test environment setup, pytest configuration, and key pitfalls
type: project
---

Test environment is a `.venv` at project root using Python 3.12. Use `.venv/bin/pytest` to run tests.

`tests/conftest.py` sets `API_BASE_URL` and `API_KEY` environment variables before any module import, satisfying `config.py`'s `_require()` calls during tests.

`pytest.ini` at project root sets `asyncio_mode = auto` (added during pipeline.py implementation). pytest-asyncio version is 1.3.0.

`watchdog` must be installed in the venv (`pip install watchdog`) — it was missing from the initial venv setup despite being in requirements.txt.

**How to apply:** Run `.venv/bin/pytest tests/ -v` to verify the full suite. As of the pipeline.py/watcher.py implementation, all 84 tests pass with no pre-existing failures.
