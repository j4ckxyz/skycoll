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


def test_worker_resolves_each_handle_pds(monkeypatch):
    from skycoll.commands import fetch as fetch_mod

    resolved: list[str] = []
    profile_calls: list[tuple[str, str]] = []
    follows_calls: list[tuple[str, str]] = []

    def _fake_resolve(handle: str) -> dict:
        resolved.append(handle)
        return {
            "did": f"did:plc:{handle}",
            "handle": handle,
            "pds": f"https://{handle}.pds.example.com",
        }

    async def _fake_fetch_profile(client, actor: str, pds_endpoint: str) -> dict:
        del client
        profile_calls.append((actor, pds_endpoint))
        return {"did": actor, "displayName": actor, "avatar": ""}

    async def _fake_fetch_follows(client, actor: str, pds_endpoint: str) -> list[dict]:
        del client
        follows_calls.append((actor, pds_endpoint))
        return []

    monkeypatch.setattr(fetch_mod, "resolve", _fake_resolve)
    monkeypatch.setattr(fetch_mod, "_fetch_profile", _fake_fetch_profile)
    monkeypatch.setattr(fetch_mod, "_fetch_follows", _fake_fetch_follows)
    monkeypatch.setattr(fetch_mod, "write_fdat", lambda handle, profile, follows: f"/tmp/{handle}.dat")
    monkeypatch.setattr(fetch_mod, "_download_avatar_async", lambda *args, **kwargs: asyncio.sleep(0))
    monkeypatch.setattr(fetch_mod, "_fdat_exists", lambda _handle: False)

    follows = [
        {"handle": "alice.bsky.social", "did": "did:plc:alice"},
        {"handle": "bob.bsky.social", "did": "did:plc:bob"},
    ]

    asyncio.run(fetch_mod._run_workers(follows, workers=2, skip_existing=True))

    assert sorted(resolved) == ["alice.bsky.social", "bob.bsky.social"]
    assert ("did:plc:alice.bsky.social", "https://alice.bsky.social.pds.example.com") in profile_calls
    assert ("did:plc:bob.bsky.social", "https://bob.bsky.social.pds.example.com") in profile_calls
    assert ("did:plc:alice.bsky.social", "https://alice.bsky.social.pds.example.com") in follows_calls
    assert ("did:plc:bob.bsky.social", "https://bob.bsky.social.pds.example.com") in follows_calls


def test_worker_skip_existing_short_circuit(monkeypatch):
    from skycoll.commands import fetch as fetch_mod

    resolved: list[str] = []

    def _fake_resolve(handle: str) -> dict:
        resolved.append(handle)
        return {"did": "did:plc:x", "handle": handle, "pds": "https://x.example.com"}

    monkeypatch.setattr(fetch_mod, "resolve", _fake_resolve)
    monkeypatch.setattr(fetch_mod, "_fdat_exists", lambda _handle: True)

    async def _should_not_run(*args, **kwargs):
        raise AssertionError("network fetch should not run when file exists")

    monkeypatch.setattr(fetch_mod, "_fetch_profile", _should_not_run)
    monkeypatch.setattr(fetch_mod, "_fetch_follows", _should_not_run)

    follows = [{"handle": "alice.bsky.social", "did": "did:plc:alice"}]
    asyncio.run(fetch_mod._run_workers(follows, workers=1, skip_existing=True))
    assert resolved == []
