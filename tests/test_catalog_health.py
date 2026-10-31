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


class TestProbes:
    def test_only_the_configured_mpc_sources_are_probed(self, monkeypatch):
        monkeypatch.setattr(config, "MPC_SOURCES", ("jpl",))
        names = [p.name for p in catalog_health.probes()]
        assert names == ["Simbad", "Gaia DR3", "2MASS", "Pan-STARRS", "JPL SB Ident", "JPL Horizons"]

    def test_names_fit_the_api_contract(self, monkeypatch):
        """observatory-api: 1-40 chars of A-Z a-z 0-9 space . _ / ( ) -"""
        import re
        monkeypatch.setattr(config, "MPC_SOURCES", ("skybot", "jpl"))
        for probe in catalog_health.probes():
            assert re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ._/()-]{0,39}", probe.name)

    def test_check_all_returns_one_result_per_probe(self, monkeypatch):
        monkeypatch.setattr(config, "MPC_SOURCES", ("skybot",))
        monkeypatch.setattr(
            catalog_health, "check",
            lambda probe, client: {"name": probe.name, "ok": probe.name != "Simbad", "latency_ms": 1, "error": None},
        )
        results = catalog_health.check_all()
        assert [r["name"] for r in results][0] == "Simbad" and results[0]["ok"] is False
        assert len(results) == len(catalog_health.probes())


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
