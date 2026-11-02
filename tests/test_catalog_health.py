"""
tests/test_catalog_health.py — the online catalog check (modules/catalog_health.py),
its report (api_client.post_catalog_status) and the worker thread that runs both.
"""

from __future__ import annotations

import threading
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

import api_client
import config
import worker
from modules import catalog_health


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


class TestCheck:
    def test_an_answer_below_500_is_ok(self):
        probe = catalog_health.Probe("JPL SB Ident", "https://example.test/sb")
        result = catalog_health.check(probe, _client(lambda request: httpx.Response(400)))
        assert result["ok"] is True and result["error"] is None
        assert isinstance(result["latency_ms"], int)

    def test_a_5xx_is_down(self):
        probe = catalog_health.Probe("SkyBot", "https://example.test/skybot")
        result = catalog_health.check(probe, _client(lambda request: httpx.Response(502)))
        assert result == {"name": "SkyBot", "ok": False, "latency_ms": result["latency_ms"], "error": "HTTP 502"}

    def test_vizier_error_votable_is_down(self):
        body = '<VOTABLE><INFO name="QUERY_STATUS" value="ERROR">table not found</INFO></VOTABLE>'
        result = catalog_health.check(catalog_health._2MASS, _client(lambda request: httpx.Response(200, text=body)))
        assert result["ok"] is False and result["error"] == "the service reported an error"

    def test_a_vizier_answer_without_error_is_ok(self):
        body = '<VOTABLE><INFO name="QUERY_STATUS" value="OK"/></VOTABLE>'
        assert catalog_health.check(catalog_health._2MASS, _client(lambda request: httpx.Response(200, text=body)))["ok"]

    def test_a_timeout_is_down(self):
        def handler(request):
            raise httpx.ConnectTimeout("timed out")

        result = catalog_health.check(catalog_health.Probe("Simbad", "https://example.test"), _client(handler))
        assert result == {"name": "Simbad", "ok": False, "latency_ms": None, "error": "ConnectTimeout: timed out"}


def _single_sources(monkeypatch):
    monkeypatch.setattr(config, "SIMBAD_SERVERS", ("simbad.cds.unistra.fr",))
    monkeypatch.setattr(config, "TWOMASS_SOURCES", ("vizier.cds.unistra.fr",))
    monkeypatch.setattr(config, "PANSTARRS_SOURCES", ("vizier.cds.unistra.fr",))


class TestProbes:
    def test_only_the_configured_mpc_sources_are_probed(self, monkeypatch):
        _single_sources(monkeypatch)
        monkeypatch.setattr(config, "MPC_SOURCES", ("jpl",))
        names = [p.name for p in catalog_health.probes()]
        assert names == ["Simbad", "Gaia DR3", "2MASS", "Pan-STARRS", "JPL SB Ident", "JPL Horizons"]

    def test_every_configured_source_of_a_catalog_is_probed(self, monkeypatch):
        monkeypatch.setattr(config, "SIMBAD_SERVERS", ("simbad.cds.unistra.fr", "simbad.harvard.edu"))
        monkeypatch.setattr(config, "TWOMASS_SOURCES", ("vizier.cds.unistra.fr", "vizier.idia.ac.za", "gaia"))
        monkeypatch.setattr(config, "PANSTARRS_SOURCES", ("vizier.cds.unistra.fr", "gaia", "gaia"))
        monkeypatch.setattr(config, "MPC_SOURCES", ("skybot",))
        probes = [(p.name, p.source) for p in catalog_health.probes()]
        assert probes == [
            ("Simbad", "simbad.cds.unistra.fr"), ("Simbad", "simbad.harvard.edu"),
            ("Gaia DR3", None),
            ("2MASS", "vizier.cds.unistra.fr"), ("2MASS", "vizier.idia.ac.za"), ("2MASS", "Gaia archive"),
            ("Pan-STARRS", "vizier.cds.unistra.fr"), ("Pan-STARRS", "Gaia archive"),
            ("SkyBot", None), ("JPL Horizons", None),
        ]
        by_source = {(p.name, p.source): p for p in catalog_health.probes()}
        assert by_source[("Simbad", "simbad.harvard.edu")].url == "https://simbad.harvard.edu/simbad/sim-tap/availability"
        assert by_source[("2MASS", "vizier.idia.ac.za")].url.startswith("https://vizier.idia.ac.za/viz-bin/votable?-source=II/246/out")
        assert by_source[("Pan-STARRS", "Gaia archive")].url == catalog_health._GAIA_ARCHIVE_URL

    def test_names_fit_the_api_contract(self, monkeypatch):
        """observatory-api: 1-40 chars of A-Z a-z 0-9 space . _ / ( ) -"""
        import re
        monkeypatch.setattr(config, "MPC_SOURCES", ("skybot", "jpl"))
        for probe in catalog_health.probes():
            assert re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ._/()-]{0,39}", probe.name)

    def test_check_all_returns_one_result_per_catalog(self, monkeypatch):
        monkeypatch.setattr(config, "MPC_SOURCES", ("skybot",))
        monkeypatch.setattr(
            catalog_health, "check",
            lambda probe, client: {
                "name": probe.name, "ok": probe.name != "Simbad",
                "latency_ms": 1, "error": None if probe.name != "Simbad" else "HTTP 502",
            },
        )
        results = catalog_health.check_all()
        assert [r["name"] for r in results][0] == "Simbad" and results[0]["ok"] is False
        assert [r["name"] for r in results] == list(dict.fromkeys(p.name for p in catalog_health.probes()))


def _ok(name, latency=5):
    return {"name": name, "ok": True, "latency_ms": latency, "error": None}


def _down(name, error="ConnectTimeout: timed out", latency=None):
    return {"name": name, "ok": False, "latency_ms": latency, "error": error}


class TestFold:
    _CDS = catalog_health._simbad_probe("simbad.cds.unistra.fr")
    _HARVARD = catalog_health._simbad_probe("simbad.harvard.edu")

    def test_a_catalog_is_up_while_any_source_answers(self, caplog):
        with caplog.at_level("WARNING"):
            report = catalog_health.fold([self._CDS, self._HARVARD], [_down("Simbad"), _ok("Simbad", 250)])
        assert report == [{"name": "Simbad", "ok": True, "latency_ms": 250, "error": None}]
        assert "Simbad: simbad.cds.unistra.fr down (ConnectTimeout: timed out) — served by simbad.harvard.edu" in caplog.text

    def test_the_preferred_source_answering_is_not_worth_a_warning(self, caplog):
        with caplog.at_level("WARNING"):
            report = catalog_health.fold([self._CDS, self._HARVARD], [_ok("Simbad", 40), _ok("Simbad", 250)])
        assert report[0]["latency_ms"] == 40 and caplog.text == ""

    def test_every_source_down_names_each_failure(self):
        report = catalog_health.fold(
            [self._CDS, self._HARVARD], [_down("Simbad"), _down("Simbad", "HTTP 502", latency=3020)],
        )
        assert report == [{
            "name": "Simbad", "ok": False, "latency_ms": 3020,
            "error": "simbad.cds.unistra.fr: ConnectTimeout: timed out; simbad.harvard.edu: HTTP 502",
        }]

    def test_a_single_source_catalog_passes_through(self):
        result = _down("SkyBot", "HTTP 502", latency=100)
        assert catalog_health.fold([catalog_health._SKYBOT], [result]) == [result]

    def test_catalogs_keep_their_order(self):
        probes = [self._CDS, self._HARVARD, catalog_health._GAIA, catalog_health._SKYBOT]
        results = [_down("Simbad"), _ok("Simbad"), _ok("Gaia DR3"), _ok("SkyBot")]
        assert [r["name"] for r in catalog_health.fold(probes, results)] == ["Simbad", "Gaia DR3", "SkyBot"]


class TestPostCatalogStatus:
    @pytest.mark.parametrize("status,expected", [(200, True), (404, False), (503, False)])
    async def test_status(self, status, expected):
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        client.post = AsyncMock(return_value=httpx.Response(status, json={}))
        with patch("api_client.pipelines._make_client", return_value=client):
            assert await api_client.post_catalog_status([{"name": "Simbad", "ok": True}]) is expected
        client.post.assert_awaited_once_with("/pipelines/catalogs", json={"catalogs": [{"name": "Simbad", "ok": True}]})

    async def test_a_transport_error_is_false(self):
        client = MagicMock()
        client.__aenter__ = AsyncMock(side_effect=httpx.ConnectError("down"))
        client.__aexit__ = AsyncMock(return_value=False)
        with patch("api_client.pipelines._make_client", return_value=client):
            assert await api_client.post_catalog_status([]) is False


class TestReporter:
    def test_disabled_at_zero(self, monkeypatch):
        monkeypatch.setattr(config, "CATALOG_HEALTH_INTERVAL_SEC", 0.0)
        assert worker._start_catalog_health_reporter() is None

    def test_checks_and_reports_until_stopped(self, monkeypatch):
        reported = threading.Event()
        results = [{"name": "Simbad", "ok": False, "latency_ms": None, "error": "x"}]
        monkeypatch.setattr(catalog_health, "check_all", lambda: results)
        post = AsyncMock(side_effect=lambda catalogs: reported.set() or True)
        monkeypatch.setattr(worker.api_client, "post_catalog_status", post)
        monkeypatch.setattr(config, "CATALOG_HEALTH_INTERVAL_SEC", 3600.0)

        reporter = worker._start_catalog_health_reporter()
        assert reporter is not None
        try:
            assert reported.wait(5)
        finally:
            reporter.stop()
        reporter._thread.join(5)
        assert not reporter._thread.is_alive()
        post.assert_awaited_once_with(results)

    def test_a_failing_check_does_not_kill_the_thread(self, monkeypatch):
        calls = []
        done = threading.Event()

        def check_all():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("boom")
            done.set()
            return []

        monkeypatch.setattr(catalog_health, "check_all", check_all)
        monkeypatch.setattr(worker.api_client, "post_catalog_status", AsyncMock(return_value=True))
        reporter = worker._CatalogHealthReporter(0.01)
        reporter.start()
        try:
            assert done.wait(5)
        finally:
            reporter.stop()
        reporter._thread.join(5)
