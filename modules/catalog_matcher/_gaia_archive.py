"""
modules/catalog_matcher/_gaia_archive.py — 2MASS and Pan-STARRS DR1 read from
the ESA Gaia archive instead of VizieR.

The Gaia archive carries the external catalogues Gaia was cross-matched
against: `gaiadr1.tmass_original_valid` is the 2MASS Point Source Catalog and
`gaiadr2.panstarrs1_original_valid` the Pan-STARRS DR1 mean-object table —
the same catalogues VizieR serves as II/246 and II/349/ps1, with the same
identifiers (the 2MASS designation `01334809+3037048`, the PS1 `objID`), so a
star matched through either source keeps one `catalog_id` and its history
does not split. They are the fallback (`TWOMASS_SOURCES` / `PANSTARRS_SOURCES`
entry `gaia`) for when every VizieR host is down: CDS was unreachable for
days from 2026-10-28, the archive is the host `_gaia` queries anyway, and no
VizieR mirror carries the Pan-STARRS table (vizier.idia.ac.za answers it
with an error VOTable).

One synchronous ADQL cone per query. The archive aborts a job after about a
minute, which a Pan-STARRS cone over a dense field does not meet (NGC 6946,
M33: several hundred thousand objects within a 0.75° radius — the CDS VizieR
delivers those in one request); a sparse 1.5° field (M51, 32 000 objects)
takes ~50 s. A timeout is a failed source like any other: logged, the frame
goes on without Pan-STARRS. An `ORDER BY` magnitude (so that the row cap
keeps the bright end) is what makes even a sparse field time out, so the cap
is simply high enough not to bite, and an answer that does reach it is
refused (`ConeOverflow`) rather than cached as complete.

astroquery's Gaia client sets no socket timeout, and the archive's own job
abort is not to be relied on: on 2026-11-04 a Pan-STARRS cone over M33 sat
on an open, silent socket for 30 h. `_gaia._bound_archive_sockets()` gives
every connection of the shared `Gaia` client `GAIA_ARCHIVE_TIMEOUT_SEC`
(imported below so that it is in place whoever imports this module first);
a stall then raises like any other failed source.

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import logging
import math
import warnings

from astroquery.gaia import Gaia

from . import _gaia  # noqa: F401 — installs the socket timeout on the shared Gaia client

logger = logging.getLogger(__name__)

# Rows per cone. Pan-STARRS reaches ~55 000 for the densest field the pipeline
# has seen through VizieR (M45, 1.5°); the archive gives up on a far denser
# one long before this cap would.
_ROW_LIMIT = 300_000

_TWOMASS_TABLE = "gaiadr1.tmass_original_valid"
_PANSTARRS_TABLE = "gaiadr2.panstarrs1_original_valid"


def _cone_query(table: str, columns: list[str], mag_column: str, ra: float, dec: float, radius_deg: float) -> str:
    return (
        f"SELECT TOP {_ROW_LIMIT} {', '.join(columns)} FROM {table} "
        f"WHERE {mag_column} IS NOT NULL "
        f"AND 1=CONTAINS(POINT('ICRS', ra, dec), CIRCLE('ICRS', {ra:.6f}, {dec:.6f}, {radius_deg:.6f}))"
    )


class ConeOverflow(RuntimeError):
    """The cone holds more rows than `_ROW_LIMIT`: the answer would be an arbitrary subset."""


def _cone(table: str, columns: list[str], mag_column: str, ra: float, dec: float, radius_deg: float):
    """
    The archive's rows for one cone; raises on any transport or server error,
    and on an answer cut at `_ROW_LIMIT` — an unsorted partial catalog would
    be cached as the complete one and every star it lacks would read as
    uncatalogued (a false UNKNOWN), so a cone this dense is a failed source.
    """
    result = Gaia.launch_job(_cone_query(table, columns, mag_column, ra, dec, radius_deg)).get_results()
    if len(result) >= _ROW_LIMIT:
        raise ConeOverflow(f"{table}: more than {_ROW_LIMIT} rows within {radius_deg:.2f}° — answer would be partial")
    return result


def _finite(row, column: str) -> float | None:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            value = float(row[column])
    except (TypeError, ValueError, KeyError):
        return None
    return value if math.isfinite(value) else None


def _query_2mass(ra: float, dec: float, radius_deg: float) -> list[dict]:
    """2MASS PSC entries within the cone, shaped as `_2mass._query_2mass()` returns them."""
    table = _cone(_TWOMASS_TABLE, ["designation", "ra", "dec", "j_m"], "j_m", ra, dec, radius_deg)
    stars: list[dict] = []
    for row in table:
        jmag = _finite(row, "j_m")
        ra_val = _finite(row, "ra")
        dec_val = _finite(row, "dec")
        if jmag is None or ra_val is None or dec_val is None:
            continue
        stars.append({
            "ra":          ra_val,
            "dec":         dec_val,
            "designation": str(row["designation"]).strip(),
            "jmag":        jmag,
        })
    return stars


def _query_panstarrs(ra: float, dec: float, radius_deg: float) -> list[dict]:
    """Pan-STARRS DR1 mean objects within the cone, shaped as `_panstarrs._query_panstarrs()` returns them."""
    table = _cone(_PANSTARRS_TABLE, ["obj_id", "ra", "dec", "r_mean_psf_mag"], "r_mean_psf_mag", ra, dec, radius_deg)
    sources: list[dict] = []
    for row in table:
        rmag = _finite(row, "r_mean_psf_mag")
        ra_val = _finite(row, "ra")
        dec_val = _finite(row, "dec")
        if rmag is None or ra_val is None or dec_val is None:
            continue
        sources.append({
            "ra":     ra_val,
            "dec":    dec_val,
            "obj_id": str(row["obj_id"]),
            "rmag":   rmag,
        })
    return sources
