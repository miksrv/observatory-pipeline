"""
tests/conftest.py — Session-wide test fixtures and environment bootstrap.

This file is loaded by pytest before any test module is imported.  It sets the
mandatory environment variables that config.py's _require() calls expect, so
that importing any pipeline module during testing does not raise ValueError.
"""

import os

# Must be set before any module-level import of config.py
os.environ.setdefault("API_BASE_URL", "http://test.local")
os.environ.setdefault("API_KEY", "test-key")
# The worker's catalog health thread would probe the real catalogs from any
# test that runs its poll loop; tests of that thread turn it on themselves.
os.environ["CATALOG_HEALTH_INTERVAL_SEC"] = "0"
