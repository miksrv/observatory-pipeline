# Agent Memory Index

## User
- [user_profile.md](user_profile.md) — Role and collaboration context for the observatory-pipeline project

## Project
- [project_test_infrastructure.md](project_test_infrastructure.md) — Test setup: conftest, venv, asyncio mode, pre-existing failures

## Feedback
- [feedback_mock_patterns.md](feedback_mock_patterns.md) — How to correctly mock sep, astropy, and astroscrappy in qc tests
- [feedback_logging_extra_keys.md](feedback_logging_extra_keys.md) — Reserved LogRecord keys that must not appear in extra= dicts (e.g. "filename")
- [feedback_api_client_patterns.md](feedback_api_client_patterns.md) — httpx.AsyncClient mock pattern and inner/outer retry design for api_client tests
- [feedback_catalog_matcher_patterns.md](feedback_catalog_matcher_patterns.md) — Simbad sexagesimal format, MPC instability, Simbad class-mock pattern, UTC datetime for cache
- [feedback_ephemeris_patterns.md](feedback_ephemeris_patterns.md) — Horizons mock chain, numpy.ma.masked for masked columns, KeyError injection, elevation unit (m→km)
