"""
tests/test_api_client.py — Unit tests for api_client/client.py

All HTTP I/O is mocked via unittest.mock.AsyncMock — respx is NOT used.
The mock strategy patches `api_client.client.httpx.AsyncClient` as an
async context manager whose .post() / .get() methods return controlled
Response-like objects.

asyncio_mode = auto is set in pytest.ini — no @pytest.mark.asyncio needed.
"""

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from api_client import client as api_client_module
from api_client.client import (
    get_frames_covering,
    get_sources_near,
    post_anomalies,
    post_frame,
    post_sources,
)


# ---------------------------------------------------------------------------
# Mock response helper
# ---------------------------------------------------------------------------

def _mock_response(
    status_code: int = 200,
    json_data: Any = None,
    raise_for_status_exc: Exception | None = None,
) -> MagicMock:
    """
    Build a MagicMock that resembles an httpx.Response.

    Parameters
    ----------
    status_code:
        HTTP status code to report.
    json_data:
        Data returned by response.json().
    raise_for_status_exc:
        If set, response.raise_for_status() raises this exception.
    """
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = str(json_data)
    resp.json = MagicMock(return_value=json_data if json_data is not None else {})
    if raise_for_status_exc is not None:
        resp.raise_for_status = MagicMock(side_effect=raise_for_status_exc)
    else:
        resp.raise_for_status = MagicMock(return_value=None)
    return resp


def _make_5xx_status_error() -> httpx.HTTPStatusError:
    """Build a realistic HTTPStatusError for a 500 response."""
    request = httpx.Request("POST", "http://test.local/frames")
    response = httpx.Response(500, request=request)
    return httpx.HTTPStatusError("Internal Server Error", request=request, response=response)


# ---------------------------------------------------------------------------
# Context manager that patches httpx.AsyncClient in the module under test.
#
# Usage:
#   with _patch_client(post_response=...) as mock_client:
#       result = await post_frame(...)
#       mock_client.post.assert_called_once_with(...)
# ---------------------------------------------------------------------------

@contextmanager
def _patch_client(
    post_response: MagicMock | None = None,
    get_response: MagicMock | None = None,
    post_side_effect: Exception | None = None,
    get_side_effect: Exception | None = None,
):
    """
    Replace httpx.AsyncClient with an AsyncMock context manager.

    The mock_client yielded is the object returned by `async with _make_client()`.
    """
    mock_client = AsyncMock()

    if post_side_effect is not None:
        mock_client.post = AsyncMock(side_effect=post_side_effect)
    elif post_response is not None:
        mock_client.post = AsyncMock(return_value=post_response)
    else:
        mock_client.post = AsyncMock(return_value=_mock_response())

    if get_side_effect is not None:
        mock_client.get = AsyncMock(side_effect=get_side_effect)
    elif get_response is not None:
        mock_client.get = AsyncMock(return_value=get_response)
    else:
        mock_client.get = AsyncMock(return_value=_mock_response(json_data=[]))

    # Make AsyncClient() behave as `async with AsyncClient() as client:`
    mock_client_class = MagicMock()
    mock_client_class.return_value.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client_class.return_value.__aexit__ = AsyncMock(return_value=False)

    with patch("api_client.client.httpx.AsyncClient", mock_client_class):
        yield mock_client


# ---------------------------------------------------------------------------
# Test 1: post_frame — happy path
# ---------------------------------------------------------------------------

class TestPostFrame:
    async def test_post_frame_returns_frame_id(self):
        """POST /frames returns {"id": "frame-123"} — expect "frame-123" back."""
        resp = _mock_response(status_code=200, json_data={"id": "frame-123"})
        with _patch_client(post_response=resp):
            result = await post_frame({"filename": "test.fits"})

        assert result == "frame-123"

    async def test_post_frame_returns_frame_id_as_string(self):
        """API may return id as int — must be coerced to str."""
        resp = _mock_response(status_code=200, json_data={"id": 42})
        with _patch_client(post_response=resp):
            result = await post_frame({"filename": "test.fits"})

        assert result == "42"
        assert isinstance(result, str)

    async def test_post_frame_raises_on_missing_id(self):
        """Response body without 'id' must raise RuntimeError."""
        resp = _mock_response(status_code=200, json_data={"result": "ok"})
        with _patch_client(post_response=resp):
            with pytest.raises(RuntimeError, match="did not return frame id"):
                await post_frame({"filename": "test.fits"})

    async def test_post_frame_raises_on_4xx(self):
        """HTTP 422 must raise RuntimeError immediately — no retry."""
        resp = _mock_response(status_code=422, json_data={"error": "invalid"})
        with _patch_client(post_response=resp):
            with pytest.raises(RuntimeError, match="HTTP 422"):
                await post_frame({"filename": "test.fits"})

    async def test_post_frame_4xx_does_not_retry(self):
        """
        On HTTP 4xx the client must call POST exactly once (no retries).
        We verify by counting how many times post() was invoked.
        """
        resp = _mock_response(status_code=400, json_data={"error": "bad request"})
        with _patch_client(post_response=resp) as mock_client:
            with pytest.raises(RuntimeError):
                await post_frame({"filename": "test.fits"})

        assert mock_client.post.call_count == 1

    async def test_post_frame_uses_frames_endpoint(self):
        """Ensure the request targets /frames."""
        resp = _mock_response(status_code=200, json_data={"id": "x1"})
        with _patch_client(post_response=resp) as mock_client:
            await post_frame({"filename": "f.fits"})

        call_args = mock_client.post.call_args
        assert call_args[0][0] == "/frames"

    async def test_post_frame_sends_json_body(self):
        """frame_data must be forwarded as the JSON body."""
        payload = {"filename": "f.fits", "obs_time": "2024-01-01T00:00:00Z"}
        resp = _mock_response(status_code=200, json_data={"id": "abc"})
        with _patch_client(post_response=resp) as mock_client:
            await post_frame(payload)

        call_kwargs = mock_client.post.call_args[1]
        assert call_kwargs["json"] == payload


# ---------------------------------------------------------------------------
# Test 2: post_sources
# ---------------------------------------------------------------------------

class TestPostSources:
    async def test_post_sources_sends_correct_body(self):
        """post_sources must POST to /frames/{id}/sources with the right body."""
        resp = _mock_response(status_code=200, json_data={"saved": 3})
        sources = [{"ra": 1.0, "dec": 2.0, "mag": 14.5}]
        with _patch_client(post_response=resp) as mock_client:
            await post_sources("frame-42", "f.fits", sources)

        mock_client.post.assert_called_once()
        call_args, call_kwargs = mock_client.post.call_args
        assert call_args[0] == "/frames/frame-42/sources"
        assert call_kwargs["json"] == {"filename": "f.fits", "sources": sources}

    async def test_post_sources_empty_list_is_valid(self):
        """Empty source list must still be sent — not silently dropped."""
        resp = _mock_response(status_code=200, json_data={})
        with _patch_client(post_response=resp) as mock_client:
            await post_sources("frame-1", "f.fits", [])

        _, call_kwargs = mock_client.post.call_args
        assert call_kwargs["json"]["sources"] == []

    async def test_post_sources_returns_none(self):
        """post_sources always returns None."""
        resp = _mock_response(status_code=200, json_data={})
        with _patch_client(post_response=resp):
            result = await post_sources("frame-1", "f.fits", [])

        assert result is None

    async def test_post_sources_silent_on_http_error(self):
        """
        When all retries are exhausted (simulated by a persistent 5xx error),
        post_sources must return None without raising.
        """
        exc = _make_5xx_status_error()
        # We patch the _inner retryable function directly to avoid real sleep
        with patch.object(
            api_client_module,
            "_post_sources_with_retry",
            AsyncMock(side_effect=exc),
        ):
            result = await post_sources("frame-1", "f.fits", [{"ra": 1.0}])

        assert result is None

    async def test_post_sources_silent_on_transport_error(self):
        """TransportError after retries exhausted must not propagate."""
        exc = httpx.ConnectError("connection refused")
        with patch.object(
            api_client_module,
            "_post_sources_with_retry",
            AsyncMock(side_effect=exc),
        ):
            result = await post_sources("frame-1", "f.fits", [])

        assert result is None

    async def test_post_sources_silent_on_4xx(self):
        """HTTP 4xx must be handled silently — function returns None."""
        resp = _mock_response(status_code=422, json_data={"error": "bad"})
        with _patch_client(post_response=resp):
            result = await post_sources("frame-1", "f.fits", [])

        assert result is None


# ---------------------------------------------------------------------------
# Test 3: post_anomalies
# ---------------------------------------------------------------------------

class TestPostAnomalies:
    async def test_post_anomalies_sends_correct_body(self):
        """post_anomalies must POST to /frames/{id}/anomalies with the right body."""
        resp = _mock_response(status_code=200, json_data={"saved": 1})
        anomalies = [{"anomaly_type": "UNKNOWN", "ra": 1.0, "dec": 2.0}]
        with _patch_client(post_response=resp) as mock_client:
            await post_anomalies("frame-99", "frame.fits", anomalies)

        mock_client.post.assert_called_once()
        call_args, call_kwargs = mock_client.post.call_args
        assert call_args[0] == "/frames/frame-99/anomalies"
        assert call_kwargs["json"] == {"filename": "frame.fits", "anomalies": anomalies}

    async def test_post_anomalies_empty_list_is_valid(self):
        """Empty anomaly list must still be sent."""
        resp = _mock_response(status_code=200, json_data={})
        with _patch_client(post_response=resp) as mock_client:
            await post_anomalies("frame-1", "f.fits", [])

        _, call_kwargs = mock_client.post.call_args
        assert call_kwargs["json"]["anomalies"] == []

    async def test_post_anomalies_returns_none(self):
        resp = _mock_response(status_code=200, json_data={})
        with _patch_client(post_response=resp):
            result = await post_anomalies("frame-1", "f.fits", [])

        assert result is None

    async def test_post_anomalies_silent_on_http_error(self):
        """Exhausted retries (5xx) must not propagate out of post_anomalies."""
        exc = _make_5xx_status_error()
        with patch.object(
            api_client_module,
            "_post_anomalies_with_retry",
            AsyncMock(side_effect=exc),
        ):
            result = await post_anomalies("frame-1", "f.fits", [])

        assert result is None

    async def test_post_anomalies_silent_on_4xx(self):
        """HTTP 4xx must be handled silently."""
        resp = _mock_response(status_code=404, json_data={"error": "not found"})
        with _patch_client(post_response=resp):
            result = await post_anomalies("frame-1", "f.fits", [])

        assert result is None


# ---------------------------------------------------------------------------
# Test 4: get_sources_near
# ---------------------------------------------------------------------------

class TestGetSourcesNear:
    async def test_get_sources_near_returns_list(self):
        """Standard {"data": [...]} response must return the inner list."""
        payload = {"data": [{"ra": 1.0, "dec": 2.0}]}
        resp = _mock_response(status_code=200, json_data=payload)
        with _patch_client(get_response=resp):
            result = await get_sources_near(1.0, 2.0, 5.0, "2024-01-01T00:00:00Z")

        assert result == [{"ra": 1.0, "dec": 2.0}]

    async def test_get_sources_near_bare_list_response(self):
        """If the API returns a bare list (no 'data' wrapper), it must still work."""
        payload = [{"ra": 3.0}, {"ra": 4.0}]
        resp = _mock_response(status_code=200, json_data=payload)
        with _patch_client(get_response=resp):
            result = await get_sources_near(3.0, 4.0, 5.0, "2024-01-01T00:00:00Z")

        assert result == [{"ra": 3.0}, {"ra": 4.0}]

    async def test_get_sources_near_returns_empty_on_transport_error(self):
        """TransportError (after retries exhausted) must return []."""
        exc = httpx.ConnectError("connection refused")
        with patch.object(
            api_client_module,
            "_get_sources_near_with_retry",
            AsyncMock(side_effect=exc),
        ):
            result = await get_sources_near(1.0, 2.0, 5.0, "2024-01-01T00:00:00Z")

        assert result == []

    async def test_get_sources_near_returns_empty_on_timeout(self):
        """TimeoutException must return []."""
        exc = httpx.ReadTimeout("read timeout")
        with patch.object(
            api_client_module,
            "_get_sources_near_with_retry",
            AsyncMock(side_effect=exc),
        ):
            result = await get_sources_near(1.0, 2.0, 5.0, "2024-01-01T00:00:00Z")

        assert result == []

    async def test_get_sources_near_passes_correct_params(self):
        """Verify the params dict contains all 4 required keys."""
        resp = _mock_response(status_code=200, json_data={"data": []})
        with _patch_client(get_response=resp) as mock_client:
            await get_sources_near(10.5, -20.3, 15.0, "2024-06-01T12:00:00Z")

        _, call_kwargs = mock_client.get.call_args
        params = call_kwargs["params"]
        assert params["ra"] == 10.5
        assert params["dec"] == -20.3
        assert params["radius_arcsec"] == 15.0
        assert params["before_time"] == "2024-06-01T12:00:00Z"

    async def test_get_sources_near_returns_empty_when_data_not_list(self):
        """If 'data' is not a list, must return []."""
        resp = _mock_response(status_code=200, json_data={"data": {"unexpected": "dict"}})
        with _patch_client(get_response=resp):
            result = await get_sources_near(1.0, 2.0, 5.0, "2024-01-01T00:00:00Z")

        assert result == []

    async def test_get_sources_near_uses_correct_endpoint(self):
        resp = _mock_response(status_code=200, json_data={"data": []})
        with _patch_client(get_response=resp) as mock_client:
            await get_sources_near(1.0, 2.0, 5.0, "2024-01-01T00:00:00Z")

        call_args = mock_client.get.call_args
        assert call_args[0][0] == "/sources/near"


# ---------------------------------------------------------------------------
# Test 5: get_frames_covering
# ---------------------------------------------------------------------------

class TestGetFramesCovering:
    async def test_get_frames_covering_returns_list(self):
        """Standard {"data": [...]} response must return the inner list."""
        payload = {"data": [{"id": "f1"}, {"id": "f2"}]}
        resp = _mock_response(status_code=200, json_data=payload)
        with _patch_client(get_response=resp):
            result = await get_frames_covering(5.0, 10.0, "2024-01-01T00:00:00Z")

        assert result == [{"id": "f1"}, {"id": "f2"}]

    async def test_get_frames_covering_handles_bare_list_response(self):
        """Bare list response (no 'data' wrapper) must be returned as-is."""
        payload = [{"id": "f1"}]
        resp = _mock_response(status_code=200, json_data=payload)
        with _patch_client(get_response=resp):
            result = await get_frames_covering(5.0, 10.0, "2024-01-01T00:00:00Z")

        assert result == [{"id": "f1"}]

    async def test_get_frames_covering_passes_correct_params(self):
        """Verify the params dict has ra, dec, and before_time (no radius_arcsec)."""
        resp = _mock_response(status_code=200, json_data={"data": []})
        with _patch_client(get_response=resp) as mock_client:
            await get_frames_covering(123.45, -67.89, "2025-03-15T08:00:00Z")

        _, call_kwargs = mock_client.get.call_args
        params = call_kwargs["params"]
        assert params["ra"] == 123.45
        assert params["dec"] == -67.89
        assert params["before_time"] == "2025-03-15T08:00:00Z"
        assert "radius_arcsec" not in params

    async def test_get_frames_covering_returns_empty_on_error(self):
        """Any exception after retries exhausted must return []."""
        exc = httpx.ConnectError("connection refused")
        with patch.object(
            api_client_module,
            "_get_frames_covering_with_retry",
            AsyncMock(side_effect=exc),
        ):
            result = await get_frames_covering(1.0, 2.0, "2024-01-01T00:00:00Z")

        assert result == []

    async def test_get_frames_covering_uses_correct_endpoint(self):
        resp = _mock_response(status_code=200, json_data={"data": []})
        with _patch_client(get_response=resp) as mock_client:
            await get_frames_covering(1.0, 2.0, "2024-01-01T00:00:00Z")

        call_args = mock_client.get.call_args
        assert call_args[0][0] == "/frames/covering"

    async def test_get_frames_covering_returns_empty_on_5xx(self):
        """HTTP 5xx (after retries exhausted via inner mock) must return []."""
        exc = _make_5xx_status_error()
        with patch.object(
            api_client_module,
            "_get_frames_covering_with_retry",
            AsyncMock(side_effect=exc),
        ):
            result = await get_frames_covering(1.0, 2.0, "2024-01-01T00:00:00Z")

        assert result == []
