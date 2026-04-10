"""firehose sub-command — connect to the AT Protocol event stream.

Connects to a relay's ``subscribeRepos`` WebSocket endpoint and filters
events by handle or DID.  Prints matching events to stdout in real time.
"""

from __future__ import annotations

import json
import inspect
import sys


def _event_repo_did(event: object) -> str | None:
    """Extract a repo DID from a firehose event object.

    Supports both direct event fields and nested commit payloads across
    atproto client versions.
    """
    repo_did = getattr(event, "did", None) or getattr(event, "repo", None)
    if not repo_did and hasattr(event, "commit"):
        commit = getattr(event, "commit")
        repo_did = getattr(commit, "did", None) or getattr(commit, "repo", None)
    return repo_did


def _event_payload(event: object) -> object:
    """Convert a firehose event object to a JSON-serializable payload."""
    if hasattr(event, "model_dump"):
        return event.model_dump()
    if hasattr(event, "__dict__"):
        return event.__dict__
    return str(event)


def run(
    handle: str | None = None,
    did: str | None = None,
    relay: str = "wss://bsky.network",
    limit: int | None = None,
) -> None:
    """Connect to the firehose and print matching events.

    Uses the ``atproto`` library's firehose client to subscribe to repo
    events and filter them by the given handle or DID.

    Args:
        handle: Filter events for this handle.
        did: Filter events for this DID.
        relay: WebSocket URL of the relay (default ``wss://bsky.network``).
        limit: Stop after *limit* matching events.
    """
    try:
        from atproto import AsyncFirehoseSubscribeReposClient
        import asyncio
    except ImportError:
        print("Error: the 'atproto' package is required for the firehose command.")
        print("Install it with: pip install atproto")
        sys.exit(1)

    # Resolve handle → DID if needed
    filter_did = did
    if handle and not filter_did:
        from skycoll.resolve import resolve_handle_to_did
        try:
            filter_did = resolve_handle_to_did(handle)
            print(f"Resolved {handle} → {filter_did}")
        except RuntimeError as exc:
            print(f"Could not resolve handle {handle}: {exc}")
            sys.exit(1)

    if not filter_did and not handle:
        print("Listening to all events (no --handle or --did filter).")

    count = 0

    async def _run() -> None:
        nonlocal count
        client = AsyncFirehoseSubscribeReposClient(base_uri=relay)

        async def _on_message(event: object) -> None:
            nonlocal count

            repo_did = _event_repo_did(event)
            if filter_did and repo_did != filter_did:
                return

            event_data = _event_payload(event)
            print(json.dumps(event_data, default=str, ensure_ascii=False))
            sys.stdout.flush()

            count += 1
            if limit and count >= limit:
                print(f"\nReached limit of {limit} events.")
                await client.stop()

        # atproto API compatibility:
        # - Newer versions: start(on_message_callback, ...)
        # - Older versions: async iterator from start()
        try:
            start_sig = inspect.signature(client.start)
            if len(start_sig.parameters) >= 1:
                await client.start(_on_message)
                return
        except Exception:
            pass

        async for event in client.start():
            await _on_message(event)
            if limit and count >= limit:
                break

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        print(f"\nStopped after {count} events.")
