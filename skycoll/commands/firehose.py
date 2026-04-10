"""firehose sub-command — connect to the AT Protocol event stream.

Connects to a relay's ``subscribeRepos`` WebSocket endpoint and filters
events by handle or DID.  Prints matching events to stdout in real time.
"""

from __future__ import annotations

import json
import sys


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

        async for event in client.start():
            repo_did = getattr(event, 'did', None) or getattr(event, 'repo', None)
            if not repo_did:
                # Try the commit/message structure
                if hasattr(event, 'commit'):
                    repo_did = getattr(event.commit, 'did', None)

            if filter_did and repo_did != filter_did:
                continue

            event_data = {}
            if hasattr(event, 'model_dump'):
                event_data = event.model_dump()
            elif hasattr(event, '__dict__'):
                event_data = event.__dict__
            else:
                event_data = str(event)

            print(json.dumps(event_data, default=str, ensure_ascii=False))
            sys.stdout.flush()

            count += 1
            if limit and count >= limit:
                print(f"\nReached limit of {limit} events.")
                await client.stop()
                break

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        print(f"\nStopped after {count} events.")