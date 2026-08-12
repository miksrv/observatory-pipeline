---
name: feedback_catalog_matcher_patterns
description: Non-obvious astroquery API quirks and mocking patterns for catalog_matcher tests
type: feedback
---

Simbad returns RA/Dec as sexagesimal strings (HH MM SS.ss / +DD MM SS.s), not decimal degrees. When building fake Simbad Table rows for tests, the columns must contain these string formats, and `_query_simbad` parses them via `SkyCoord(..., unit=(u.hourangle, u.deg))`. Test tables built with raw float degrees will cause SkyCoord parsing failures.

MPC's `query_objects` signature is unstable across astroquery versions. Wrap the entire `_query_mpc` body in `try/except Exception` and treat any exception (including `TypeError`) as a soft failure returning `[]`. Never let MPC outages block the pipeline.

When mocking `Simbad` (a class, not an instance), the patch target is `modules.catalog_matcher._simbad.Simbad` (the specific submodule it's imported into, NOT the top-level `modules.catalog_matcher` package — see `feedback_module_to_package_split.md`, since `catalog_matcher` is now a package split one-file-per-catalog and `_match.py`'s own `match()` calls each catalog through a qualified submodule reference for exactly this reason) and the mock must be a class mock whose `return_value` is a configured instance: `mock_cls.return_value = instance`. The module does `simbad = Simbad(); simbad.add_votable_fields(...)` — so the instance methods are what matter. Same story for `Gaia` → `modules.catalog_matcher._gaia.Gaia`, and for the bare-function patches (`_query_mpc` → `modules.catalog_matcher._mpc._query_mpc`, `_query_simbad` → `modules.catalog_matcher._simbad._query_simbad`, `_query_2mass` → `modules.catalog_matcher._2mass._query_2mass`, `_query_panstarrs` → `modules.catalog_matcher._panstarrs._query_panstarrs`).

Use `datetime.datetime.now(datetime.UTC)` (timezone-aware) instead of `datetime.datetime.utcnow()` (deprecated in Python 3.12) for cache timestamps. When manually planting stale cache entries in tests, also use `datetime.now(datetime.UTC)` so comparison arithmetic works without mixing naive and aware datetimes.

The `autouse` cache-clear fixture pattern is essential for catalog_matcher tests: `cm._cache.clear()` in `autouse=True` fixture before and after each test prevents cross-test cache contamination, especially for the cache-hit and double-call tests.

**Why:** These patterns emerged during first implementation and test run of catalog_matcher.py.

**How to apply:** Any future work on catalog_matcher or tests that involve astroquery Simbad/MPC mocking should follow these patterns verbatim.
