"""fetch sub-command — fetch follows of every handle in .dat, write to fdat/."""

from __future__ import annotations

import asyncio
import os
from typing import Any

import httpx

from skycoll.errors import NetworkError, NotFoundError, ParseError, RateLimitError, SkycollError
from skycoll.output import info, ok, warn
from skycoll.resolve import resolve
from skycoll.storage import read_dat, write_fdat, avatar_path


_MAX_WORKERS = 10
_RATE_LIMIT_RETRIES = 5
_REQUEST_TIMEOUT = 30


async def _safe_info(lock: asyncio.Lock, message: str) -> None:
    async with lock:
        info(message)


async def _safe_warn(lock: asyncio.Lock, message: str) -> None:
    async with lock:
        warn(message)


def _xrpc_url(pds_endpoint: str, path: str) -> str:
    base = pds_endpoint.rstrip("/")
    return f"{base}{path}"


def _rate_limit_backoff(attempt: int) -> float:
    return float(2 ** attempt)


async def _request_json_with_backoff(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, Any],
) -> dict:
    attempt = 0
    while True:
        try:
            resp = await client.get(url, params=params)
        except httpx.HTTPError as exc:
            raise NetworkError(f"could not reach {url}: {exc}") from exc

        if resp.status_code == 429:
            if attempt >= _RATE_LIMIT_RETRIES:
                raise RateLimitError(f"rate-limited on {url} after {_RATE_LIMIT_RETRIES} retries")
            wait = _rate_limit_backoff(attempt)
            attempt += 1
            await asyncio.sleep(wait)
            continue

        if resp.status_code != 200:
            raise NetworkError(f"API error on {url}: HTTP {resp.status_code}")

        try:
            data = resp.json()
        except ValueError as exc:
            raise ParseError(f"invalid JSON response from {url}") from exc
        if not isinstance(data, dict):
            raise ParseError(f"invalid response payload from {url}: expected an object")
        return data


async def _fetch_profile(client: httpx.AsyncClient, actor: str, pds_endpoint: str) -> dict:
    url = _xrpc_url(pds_endpoint, "/xrpc/app.bsky.actor.getProfile")
    return await _request_json_with_backoff(client, url, {"actor": actor})


async def _fetch_follows(client: httpx.AsyncClient, actor: str, pds_endpoint: str) -> list[dict]:
    url = _xrpc_url(pds_endpoint, "/xrpc/app.bsky.graph.getFollows")
    params: dict[str, Any] = {"actor": actor, "limit": 100}
    out: list[dict] = []

    while True:
        page = await _request_json_with_backoff(client, url, params)
        items = page.get("follows", [])
        if not isinstance(items, list):
            raise ParseError(f"invalid follows payload for '{actor}': expected a list")
        for item in items:
            if isinstance(item, dict):
                out.append(item)

        cursor = page.get("cursor")
        if not cursor:
            break
        params["cursor"] = cursor

    return out


def _write_avatar_bytes(dest_path: str, content: bytes) -> None:
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with open(dest_path, "wb") as f:
        f.write(content)


async def _download_avatar_async(client: httpx.AsyncClient, avatar_url: str, dest_path: str) -> None:
    if not avatar_url:
        return
    try:
        resp = await client.get(avatar_url)
    except httpx.HTTPError:
        return
    if resp.status_code != 200:
        return
    await asyncio.to_thread(_write_avatar_bytes, dest_path, resp.content)


async def _worker(
    worker_id: int,
    queue: asyncio.Queue[tuple[int, dict] | None],
    total: int,
    pds_endpoint: str,
    client: httpx.AsyncClient,
    print_lock: asyncio.Lock,
) -> None:
    del worker_id  # workers are intentionally interchangeable.

    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            return

        idx, person = item
        try:
            friend_handle = person.get("handle", "") if isinstance(person, dict) else ""
            friend_did = person.get("did", "") if isinstance(person, dict) else ""
            if not friend_handle:
                await _safe_warn(print_lock, f"[{idx}/{total}] Skipping entry without handle")
                continue

            try:
                profile = await _fetch_profile(client, friend_did or friend_handle, pds_endpoint)
            except SkycollError as exc:
                await _safe_warn(
                    print_lock,
                    f"[{idx}/{total}] Profile fetch failed for {friend_handle}: {exc}",
                )
                continue

            friend_follows: list[dict] = []
            try:
                friend_follows = await _fetch_follows(client, friend_did or friend_handle, pds_endpoint)
            except SkycollError as exc:
                await _safe_warn(
                    print_lock,
                    f"[{idx}/{total}] Follows fetch failed for {friend_handle}: {exc}",
                )

            path = await asyncio.to_thread(write_fdat, friend_handle, profile, friend_follows)

            avatar_url = profile.get("avatar", "") if isinstance(profile, dict) else ""
            if avatar_url:
                await _download_avatar_async(client, avatar_url, avatar_path(friend_handle))

            await _safe_info(
                print_lock,
                f"  [{idx}/{total}] {friend_handle} → {path} ({len(friend_follows)} follows)",
            )
        finally:
            queue.task_done()


async def _run_workers(follows: list[dict], pds_endpoint: str, workers: int) -> None:
    queue: asyncio.Queue[tuple[int, dict] | None] = asyncio.Queue()
    total = len(follows)
    for idx, person in enumerate(follows, 1):
        queue.put_nowait((idx, person))
    for _ in range(workers):
        queue.put_nowait(None)

    print_lock = asyncio.Lock()
    async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT, follow_redirects=True) as client:
        tasks = [
            asyncio.create_task(
                _worker(
                    worker_id=worker_id,
                    queue=queue,
                    total=total,
                    pds_endpoint=pds_endpoint,
                    client=client,
                    print_lock=print_lock,
                )
            )
            for worker_id in range(workers)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            raise result


def run(handle: str, workers: int = 1) -> None:
    """Fetch the follows of every user listed in ``<handle>.dat``.

    Reads ``<handle>.dat``, iterates over every followed user, fetches
    their profile and follows, and writes ``fdat/<friend>.dat``.

    Args:
        handle: The focal user's handle (used to find ``<handle>.dat``).
        workers: Number of concurrent workers (1..10).
    """
    try:
        if workers < 1 or workers > _MAX_WORKERS:
            raise ParseError(f"workers must be between 1 and {_MAX_WORKERS}")

        try:
            data = read_dat(handle)
        except FileNotFoundError as exc:
            raise NotFoundError(
                f"No .dat file found for '{handle}'. Run: skycoll init {handle}"
            ) from exc

        identity = resolve(handle)
        pds = identity.get("pds")
        if not pds:
            raise ParseError(f"resolved identity for '{handle}' is missing a PDS endpoint")

        follows = data.get("follows", [])
        if not isinstance(follows, list):
            raise ParseError(f"invalid .dat data for '{handle}': follows must be a list")
        info(f"Fetching follows for {len(follows)} users …")
        asyncio.run(_run_workers(follows, pds, workers))

        ok("Done.")
    except SkycollError:
        raise
    except OSError as exc:
        raise ParseError(f"failed to write fetched data for '{handle}': {exc}") from exc
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid fetch data for '{handle}': {exc}") from exc
    except Exception as exc:
        raise ParseError(f"unexpected fetch error for '{handle}': {exc}") from exc
