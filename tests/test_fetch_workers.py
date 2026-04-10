"""Tests for concurrent fetch worker behavior."""

from __future__ import annotations

import asyncio

import pytest

from skycoll.commands.fetch import _request_json_with_backoff
from skycoll.errors import RateLimitError


class _DummyResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


class _DummyClient:
    def __init__(self, responses):
        self._responses = list(responses)

    async def get(self, url: str, params=None):
        if not self._responses:
            return _DummyResponse(200, {"ok": True})
        return self._responses.pop(0)


def test_request_json_with_backoff_retries_429(monkeypatch):
    calls: list[float] = []

    async def _fake_sleep(seconds: float) -> None:
        calls.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    client = _DummyClient([
        _DummyResponse(429),
        _DummyResponse(429),
        _DummyResponse(200, {"ok": True}),
    ])

    data = asyncio.run(_request_json_with_backoff(client, "https://example.test/xrpc", {}))
    assert data["ok"] is True
    assert calls == [1.0, 2.0]


def test_request_json_with_backoff_raises_after_retry_limit(monkeypatch):
    async def _fake_sleep(_: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    client = _DummyClient([_DummyResponse(429) for _ in range(8)])

    with pytest.raises(RateLimitError, match="rate-limited"):
        asyncio.run(_request_json_with_backoff(client, "https://example.test/xrpc", {}))
