"""
modules/catalog_health.py — is each online catalog the pipeline uses reachable?

The worker probes every catalog once per CATALOG_HEALTH_INTERVAL_SEC on a
thread of its own (worker._CatalogHealthReporter) and reports the result to
the API (`POST /pipelines/catalogs`), which shows it on the dashboard. A
catalog that silently fails costs a frame its matches without stopping it
(catalog errors are only logged): CDS was unreachable for the whole
2026-10-28 ingest — 590 frames without Simbad, 2MASS and Pan-STARRS — and
only the log said so.

A probe is one small GET against the host the real query goes to. "ok"
means the service answered with any status below 500 — a 4xx to a bare
request still proves it is up (JPL sb_ident answers 400 without an observer);
a 5xx (SkyBot's 502 of 2026-10-23), a timeout or a refused connection means
it is down. 2MASS and Pan-STARRS are VizieR tables, probed by one tiny query
each; VizieR reports a broken or missing table as HTTP 200 with
`QUERY_STATUS = ERROR` in the VOTable, which counts as down too (that is how
a mirror says it does not carry the table).

A catalog with several configured sources (SIMBAD_SERVERS, TWOMASS_SOURCES,
PANSTARRS_SOURCES — see catalog_matcher._sources) gets one probe per source
and one line in the report: up while any source answers, with the log
saying which one serves it while a preferred one is down; the `error` of a
catalog that is down names every source's failure.
httpx's timeout is per phase (connect, read, ...), so a server trickling bytes
can hold one probe past CATALOG_HEALTH_TIMEOUT_SEC — the report is then late,
nothing else waits for it.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import httpx

import config
from modules.catalog_matcher import _simbad
from modules.catalog_matcher._sources import GAIA_ARCHIVE, GAIA_ARCHIVE_LABEL, unique

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Probe:
    name: str
    url: str
    # Text in a 2xx-4xx body that still means "broken" (VizieR's error VOTable).
    error_marker: str | None = None
    # Which of the catalog's configured sources this probes (a host name,
    # "Gaia archive"); None for a catalog with a single, fixed source.
    source: str | None = None


_VIZIER_ERROR = 'name="QUERY_STATUS" value="ERROR"'
# The Gaia DR3 probe and the `gaia` entries of TWOMASS_SOURCES/PANSTARRS_SOURCES
# share this URL, sent once per check (`check_all()` probes each URL once). A
# `SELECT TOP 1` from the copy tables would prove more, but takes 11 s on
# the 2MASS table and times out on the Pan-STARRS one.
_GAIA_ARCHIVE_URL = "https://gea.esac.esa.int/tap-server/tap/availability"


def _simbad_probe(host: str) -> Probe:
    return Probe("Simbad", f"https://{host}/simbad/sim-tap/availability", source=host)


def _vizier_probe(name: str, table: str, host: str) -> Probe:
    return Probe(
        name,
        f"https://{host}/viz-bin/votable?-source={table}&-out.max=1&-c=0.0%2B0.0&-c.rs=60",
        _VIZIER_ERROR,
        source=host,
    )


def _table_probe(name: str, table: str, source: str) -> Probe:
    """One TWOMASS_SOURCES / PANSTARRS_SOURCES entry: a VizieR host, or `gaia` for the archive copy."""
    if source == GAIA_ARCHIVE:
        return Probe(name, _GAIA_ARCHIVE_URL, source=GAIA_ARCHIVE_LABEL)
    return _vizier_probe(name, table, source)


_GAIA = Probe("Gaia DR3", _GAIA_ARCHIVE_URL)
# The CDS probes, kept by name for tests.
_SIMBAD = _simbad_probe("simbad.cds.unistra.fr")
_2MASS = _vizier_probe("2MASS", "II/246/out", "vizier.cds.unistra.fr")
_PANSTARRS = _vizier_probe("Pan-STARRS", "II/349/ps1", "vizier.cds.unistra.fr")
_SKYBOT = Probe(
    "SkyBot",
    "https://ssp.imcce.fr/webservices/skybot/api/conesearch.php"
    "?-ep=2460000.5&-ra=0&-dec=0&-rd=0.01&-mime=text&-output=object&-loc=500",
)
_JPL_SBIDENT = Probe("JPL SB Ident", "https://ssd-api.jpl.nasa.gov/sb_ident.api")
_JPL_HORIZONS = Probe("JPL Horizons", "https://ssd.jpl.nasa.gov/api/horizons.api")

# MPC_SOURCES name → its probe; only the configured sources are probed.
_MPC_SOURCE_PROBES: dict[str, Probe] = {"skybot": _SKYBOT, "jpl": _JPL_SBIDENT}


def probes() -> list[Probe]:
    """
    One probe per configured source, in catalog_matcher's order (Simbad, Gaia,
    2MASS, Pan-STARRS, MPC), then ephemerides. A catalog's probes share its
    name; `check_all()` folds them into one report line. The Simbad list is
    the matcher's own (`_simbad.configured_servers()`), so a host it ignores
    is not reported as serving the catalog.
    """
    selected = [_simbad_probe(host) for host in _simbad.configured_servers()]
    selected.append(_GAIA)
    selected += [_table_probe("2MASS", "II/246/out", s) for s in unique(config.TWOMASS_SOURCES)]
    selected += [_table_probe("Pan-STARRS", "II/349/ps1", s) for s in unique(config.PANSTARRS_SOURCES)]
    selected += [_MPC_SOURCE_PROBES[name] for name in config.MPC_SOURCES if name in _MPC_SOURCE_PROBES]
    selected.append(_JPL_HORIZONS)
    return selected


def _error_text(exc: Exception) -> str:
    text = str(exc).strip()
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def check(probe: Probe, client: httpx.Client) -> dict:
    """One probe → `{name, ok, latency_ms, error}` (the wire shape of POST /pipelines/catalogs)."""
    started = time.monotonic()
    try:
        response = client.get(probe.url)
    except Exception as exc:  # timeout, DNS, refused, TLS — all mean "not reachable"
        return {"name": probe.name, "ok": False, "latency_ms": None, "error": _error_text(exc)}

    latency_ms = int(round((time.monotonic() - started) * 1000))
    if response.status_code >= 500:
        return {"name": probe.name, "ok": False, "latency_ms": latency_ms, "error": f"HTTP {response.status_code}"}
    if probe.error_marker is not None and probe.error_marker in response.text:
        return {"name": probe.name, "ok": False, "latency_ms": latency_ms, "error": "the service reported an error"}
    return {"name": probe.name, "ok": True, "latency_ms": latency_ms, "error": None}


def fold(selected: list[Probe], results: list[dict]) -> list[dict]:
    """
    One report line per catalog name, in first-seen order: up with the first
    answering source's latency while any source answers (a preferred source
    down is logged), else down with every source's failure in `error`.
    """
    by_name: dict[str, list[tuple[Probe, dict]]] = {}
    for probe, result in zip(selected, results):
        by_name.setdefault(probe.name, []).append((probe, result))

    report: list[dict] = []
    for name, entries in by_name.items():
        answering = [(p, r) for p, r in entries if r["ok"]]
        if answering:
            probe, result = answering[0]
            down = [(p, r) for p, r in entries if not r["ok"]]
            if down:
                logger.warning(
                    "%s: %s — served by %s",
                    name,
                    "; ".join(f"{p.source} down ({r['error']})" for p, r in down),
                    probe.source,
                )
            report.append({"name": name, "ok": True, "latency_ms": result["latency_ms"], "error": None})
            continue
        if len(entries) == 1:
            report.append(entries[0][1])
            continue
        latencies = [r["latency_ms"] for _, r in entries if r["latency_ms"] is not None]
        report.append({
            "name": name,
            "ok": False,
            "latency_ms": max(latencies) if latencies else None,
            "error": "; ".join(f"{p.source}: {r['error']}" for p, r in entries),
        })
    return report


def check_all() -> list[dict]:
    """
    Probe every source in parallel (blocking; at most about
    CATALOG_HEALTH_TIMEOUT_SEC), one result per catalog. Probes sharing a URL
    (the Gaia archive's) are sent once.
    """
    selected = probes()
    distinct = unique(p.url for p in selected)
    timeout = config.CATALOG_HEALTH_TIMEOUT_SEC
    with httpx.Client(timeout=timeout, follow_redirects=True) as client, ThreadPoolExecutor(
        max_workers=len(distinct), thread_name_prefix="catalog-probe",
    ) as pool:
        by_url = dict(zip(distinct, pool.map(
            lambda url: check(next(p for p in selected if p.url == url), client), distinct,
        )))
    results = [dict(by_url[p.url], name=p.name) for p in selected]

    report = fold(selected, results)
    down = [r["name"] for r in report if not r["ok"]]
    if down:
        logger.warning("Online catalogs not reachable: %s", ", ".join(down))
    else:
        logger.info("All %d online catalogs reachable", len(report))
    return report
