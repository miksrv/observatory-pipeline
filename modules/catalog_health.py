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
it is down. 2MASS and Pan-STARRS are both VizieR tables, probed by one tiny
query each; VizieR reports a broken table as HTTP 200 with
`QUERY_STATUS = ERROR` in the VOTable, which counts as down too.
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

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Probe:
    name: str
    url: str
    # Text in a 2xx-4xx body that still means "broken" (VizieR's error VOTable).
    error_marker: str | None = None


_VIZIER_ERROR = 'name="QUERY_STATUS" value="ERROR"'


_SIMBAD = Probe("Simbad", "https://simbad.cds.unistra.fr/simbad/sim-tap/availability")
_GAIA = Probe("Gaia DR3", "https://gea.esac.esa.int/tap-server/tap/availability")
_2MASS = Probe(
    "2MASS",
    "https://vizier.cds.unistra.fr/viz-bin/votable?-source=II/246/out&-out.max=1&-c=0.0%2B0.0&-c.rs=60",
    _VIZIER_ERROR,
)
_PANSTARRS = Probe(
    "Pan-STARRS",
    "https://vizier.cds.unistra.fr/viz-bin/votable?-source=II/349/ps1&-out.max=1&-c=0.0%2B0.0&-c.rs=60",
    _VIZIER_ERROR,
)
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
    """The catalogs this configuration uses, in catalog_matcher's order, then ephemerides."""
    selected = [_SIMBAD, _GAIA, _2MASS, _PANSTARRS]
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


def check_all() -> list[dict]:
    """Probe every catalog in parallel (blocking; at most about CATALOG_HEALTH_TIMEOUT_SEC)."""
    selected = probes()
    timeout = config.CATALOG_HEALTH_TIMEOUT_SEC
    with httpx.Client(timeout=timeout, follow_redirects=True) as client, ThreadPoolExecutor(
        max_workers=len(selected), thread_name_prefix="catalog-probe",
    ) as pool:
        results = list(pool.map(lambda probe: check(probe, client), selected))

    down = [r["name"] for r in results if not r["ok"]]
    if down:
        logger.warning("Online catalogs not reachable: %s", ", ".join(down))
    else:
        logger.info("All %d online catalogs reachable", len(results))
    return results
