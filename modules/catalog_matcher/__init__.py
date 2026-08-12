"""
modules/catalog_matcher — Cross-match detected sources against external catalogs.

The single public entry point is:

    await catalog_matcher.match(sources: list[dict], frame_meta: dict) -> list[dict]

Each source dict is enriched in-place with four catalog fields:
    catalog_name  — "Simbad", "Gaia DR3", "2MASS", "MPC", or None
    catalog_id    — catalog's identifier string, or None
    catalog_mag   — magnitude (float): G-band for Gaia, J-band for 2MASS, None otherwise
    object_type   — "STAR", Simbad OTYPE string, "ASTEROID", "COMET", or None

Catalogs are queried in order: Simbad → Gaia DR3 → 2MASS → Pan-STARRS → MPC.
Once a source is matched, subsequent catalogs skip it. This order prioritises rich
object-type information from Simbad over generic Gaia stellar matching, uses 2MASS
for red/cool stars absent in Gaia, Pan-STARRS for faint optical sources below Gaia's
completeness limit, and MPC last for solar system objects.

Rationale for 2MASS as third catalog:
    - 2MASS (Two Micron All Sky Survey) covers ~470 million point sources to K≈14.3
    - Complements Gaia for late-type (M/K) stars that are bright in NIR but faint
      in the Gaia G band, and for heavily reddened regions near the Galactic plane
    - Accessed via VizieR catalog II/246; magnitudes stored as J-band (Jmag)

Rationale for Pan-STARRS DR1 as fourth catalog:
    - Covers ~1 billion sources to r~23.3 mag (Gaia DR3 complete only to ~21 mag)
    - Reduces false UNKNOWN anomaly alerts for faint sources simply below Gaia depth
    - Coverage limited to declination > -30° (optical survey from Haleakala, Hawaii)
    - Accessed via VizieR catalog II/349/ps1; magnitudes stored as r-band (rmag)

Rate limits of the online services (all free, no auth required):
    Simbad:      CDS infrastructure, ~5–6 req/sec recommended; 1-hr cache is sufficient
    Gaia DR3:    ESA TAP+, no hard limit; queries take 1–5 s; 1-hr cache is sufficient
    2MASS:       CDS/VizieR infrastructure, same limits as Simbad; cache is sufficient
    Pan-STARRS:  CDS/VizieR infrastructure, same limits as Simbad; cache is sufficient
    MPC/SkyBot:  IMCCE, no hard limit; epoch-dependent so shorter natural TTL

Catalog query results are cached in-process for 1 hour to avoid redundant network
calls across sources that share the same field.

All catalog errors are caught and logged; a failing catalog never crashes the pipeline.

Split into one file per catalog, plus shared infrastructure — see
docs/catalog-matcher.md or CLAUDE.md for the full mechanics:

  _cache.py       two-tier (in-process + on-disk) query cache shared by every catalog
  _wcs_offset.py  the systematic WCS-offset vote accumulator, run against Gaia first
  _gaia.py        Gaia DR3 query + match + get_gaia_stars()
  _simbad.py      Simbad query + match
  _2mass.py       2MASS (VizieR II/246) query + match
  _panstarrs.py   Pan-STARRS DR1 (VizieR II/349/ps1) query + match
  _mpc.py         MPC/SkyBot query + match + get_mpc_objects()
  _match.py       match() itself — orchestrates the five catalog stages above

`__init__.py` re-exports `match`/`get_gaia_stars`/`get_mpc_objects` for normal use,
plus the private per-catalog query/match functions and the cache internals that
tests/test_catalog_matcher.py exercises directly (`cm._query_gaia(...)`,
`cm._cache.clear()`, etc.).

Every test that PATCHES a catalog's query function or client class (rather than just
calling it directly) targets the specific submodule it's defined in —
`patch("modules.catalog_matcher._gaia.Gaia", ...)`, not
`patch("modules.catalog_matcher.Gaia", ...)` — because `_match.py`'s own `match()`
calls each catalog through a qualified submodule reference (`_gaia._query_gaia(...)`)
rather than a bare imported name, for exactly this reason. See `_match.py`'s own
docstring and `.claude/agent-memory/python-senior-dev/feedback_module_to_package_split.md`.
"""

from __future__ import annotations

from ._2mass import _match_2mass, _query_2mass
from ._cache import _cache, _cache_get, _cache_set
from ._gaia import _match_gaia, _query_gaia, get_gaia_stars
from ._match import match
from ._mpc import _match_mpc, _query_mpc, get_mpc_objects
from ._panstarrs import _match_panstarrs, _query_panstarrs
from ._simbad import _match_simbad, _query_simbad
from ._wcs_offset import _compute_wcs_offset

__all__ = [
    "match",
    "get_gaia_stars",
    "get_mpc_objects",
]
