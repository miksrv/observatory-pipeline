---
name: api_client mock patterns
description: How to correctly mock httpx.AsyncClient as a context manager in api_client tests, and the inner/outer function pattern for tenacity retry + safe return
type: feedback
---

For testing api_client/client.py, patch `api_client.client.httpx.AsyncClient` (not `httpx.AsyncClient`) using a `MagicMock` whose `return_value.__aenter__` is an `AsyncMock` returning the mock client, and `__aexit__` is an `AsyncMock` returning `False`.

The retry-vs-safe-return design uses a private `_foo_with_retry` inner function decorated with `@_retry`, and a public `foo` outer function that calls the inner and catches `_RETRYABLE` exceptions to return `None`/`[]` instead of propagating. Tests for "silent on error" patch `api_client.client._foo_with_retry` directly with `AsyncMock(side_effect=exc)` to avoid tenacity sleep.

**Why:** respx is not installed; unittest.mock is the only option. The inner/outer split lets tenacity retry the I/O path while the public API never raises, satisfying the "must not crash the pipeline" requirement.

**How to apply:** Whenever writing or reviewing api_client tests, use the `_patch_client` contextmanager pattern from test_api_client.py. For exhausted-retry tests, patch the inner `_*_with_retry` function directly.
