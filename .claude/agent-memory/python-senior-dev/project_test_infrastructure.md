---
name: project_test_infrastructure
description: Test environment setup, known pre-existing failures, and pytest configuration
type: project
---

Test environment is a `.venv` at project root created from Python 3.12 (found at `/Users/mik/Library/Application Support/org.siril.Siril/siril/venv/bin/python3`). There is no system Python with pip available; use `.venv/bin/pytest` to run tests.

`tests/conftest.py` sets `API_BASE_URL` and `API_KEY` environment variables before any module import, satisfying `config.py`'s `_require()` calls during tests.

Pytest asyncio mode is `STRICT` (from `pytest-asyncio` 1.3+). All async tests must use `@pytest.mark.asyncio`.

**Why:** Without `conftest.py`, importing any pipeline module in tests raises `ValueError` from `_require()`.

**Pre-existing failures in `test_fits_header.py`:** `TestStandardKeywords` class (8 tests) ERRORs in `setup_method` because astropy 6.x raises `VerifyError` when writing `NAXIS1`/`NAXIS2` onto a header-only (no-data) HDU. These failures are not caused by `conftest.py` or `qc.py` — they existed before and are unrelated to qc work.

**How to apply:** When running the full test suite, expect 8 ERRORs in `TestStandardKeywords`. Only treat new failures as regressions.
