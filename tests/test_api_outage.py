"""
tests/test_api_outage.py — the worker waits out an API outage (api_client._shared).

After the three quick retries, a process that called wait_out_api_outages()
probes the API; while the probe fails too, the call waits and retries every
API_OUTAGE_RETRY_SEC instead of failing. No network: the quick retry and the
probe are replaced.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

import config
from api_client import _shared


def _server_error() -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "http://test.local/frames")
    return httpx.HTTPStatusError("boom", request=request, response=httpx.Response(503, request=request))


@pytest.fixture
def outage(monkeypatch):
    """No quick-retry sleeps, a tiny wait interval, waiting turned off after the test."""
    monkeypatch.setattr(_shared, "_quick_retry", lambda fn: fn)
    monkeypatch.setattr(config, "API_OUTAGE_RETRY_SEC", 0.01)
    yield
    _shared.wait_out_api_outages(None)


def _flaky(failures: int, result: str = "ok") -> tuple:
    """A decorated call failing `failures` times with a 503, then returning `result`."""
    calls = {"n": 0}

    async def call():
        calls["n"] += 1
        if calls["n"] <= failures:
            raise _server_error()
        return result

    return _shared._retry(call), calls


class TestWaitingOutAnOutage:
    async def test_without_waiting_turned_on_a_failure_is_raised(self, outage, monkeypatch):
        probe = AsyncMock(return_value=False)
        monkeypatch.setattr(_shared, "_api_answers", probe)
        call, calls = _flaky(1)

        with pytest.raises(httpx.HTTPStatusError):
            await call()

        assert calls["n"] == 1
        probe.assert_not_called()

    async def test_while_the_api_does_not_answer_the_call_waits_and_succeeds_later(self, outage, monkeypatch):
        # Probe after the failure: down, down; then up.
        monkeypatch.setattr(_shared, "_api_answers", AsyncMock(side_effect=[False, False, True]))
        _shared.wait_out_api_outages(lambda: False)
        call, calls = _flaky(1)

        assert await call() == "ok"
        assert calls["n"] == 2

    async def test_a_request_failing_while_the_api_answers_is_its_own_error(self, outage, monkeypatch):
        monkeypatch.setattr(_shared, "_api_answers", AsyncMock(return_value=True))
        _shared.wait_out_api_outages(lambda: False)
        call, calls = _flaky(5)

        with pytest.raises(httpx.HTTPStatusError):
            await call()

        assert calls["n"] == 1

    async def test_a_shutdown_ends_the_wait(self, outage, monkeypatch):
        monkeypatch.setattr(_shared, "_api_answers", AsyncMock(return_value=False))
        _shared.wait_out_api_outages(lambda: True)
        call, _ = _flaky(10)

        with pytest.raises(_shared.ApiWaitInterrupted):
            await call()

    async def test_during_a_shutdown_a_request_failing_while_the_api_answers_fails_as_before(
        self, outage, monkeypatch,
    ):
        # Not ApiWaitInterrupted: that would skip the worker's lost-report
        # handling for an item that did finish.
        monkeypatch.setattr(_shared, "_api_answers", AsyncMock(return_value=True))
        _shared.wait_out_api_outages(lambda: True)
        call, _ = _flaky(5)

        with pytest.raises(httpx.HTTPStatusError):
            await call()

    async def test_zero_interval_keeps_the_old_behaviour(self, outage, monkeypatch):
        monkeypatch.setattr(config, "API_OUTAGE_RETRY_SEC", 0)
        probe = AsyncMock(return_value=False)
        monkeypatch.setattr(_shared, "_api_answers", probe)
        _shared.wait_out_api_outages(lambda: False)
        call, _ = _flaky(1)

        with pytest.raises(httpx.HTTPStatusError):
            await call()
        probe.assert_not_called()

    def test_the_interruption_passes_through_except_exception(self):
        assert not issubclass(_shared.ApiWaitInterrupted, Exception)


class TestProbe:
    @pytest.mark.parametrize(("status", "answers"), [(200, True), (401, True), (500, False), (503, False)])
    async def test_any_answer_below_500_counts(self, monkeypatch, status, answers):
        client = AsyncMock()
        client.get = AsyncMock(return_value=MagicMock(status_code=status))
        factory = MagicMock()
        factory.return_value.__aenter__ = AsyncMock(return_value=client)
        factory.return_value.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(_shared.httpx, "AsyncClient", factory)

        assert await _shared._api_answers() is answers

    async def test_no_connection_is_no_answer(self, monkeypatch):
        factory = MagicMock()
        factory.return_value.__aenter__ = AsyncMock(side_effect=httpx.ConnectError("refused"))
        factory.return_value.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(_shared.httpx, "AsyncClient", factory)

        assert await _shared._api_answers() is False
