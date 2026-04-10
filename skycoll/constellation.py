"""Constellation backlinks index client.

Constellation (https://github.com/at-microcosm/microcosm-rs/tree/main/constellation)
is a self-hostable AT Protocol backlinks index.  This module queries its REST
API to enrich profile and graph data.

All network errors are handled gracefully — a warning is printed and an
empty result is returned rather than hard-failing.
"""

from __future__ import annotations

from typing import Optional

import httpx


def get_all_backlink_counts(host: str, target: str) -> Optional[dict]:
    """Query ``/links/all/count`` for aggregate backlink counts.

    Args:
        host: Constellation host (e.g. ``https://constellation.example.com``).
        target: A DID or AT URI to query counts for.

    Returns:
        Dict of backlink counts grouped by collection and path, or ``None``
        on failure.
    """
    url = f"{host.rstrip('/')}/links/all/count"
    try:
        resp = httpx.get(url, params={"target": target}, timeout=15)
        if resp.status_code != 200:
            print(f"  ⚠ Constellation returned HTTP {resp.status_code}")
            return None
        return resp.json()
    except httpx.HTTPError as exc:
        print(f"  ⚠ Constellation unavailable: {exc}")
        return None


def get_backlink_count(host: str, target: str, collection: str, path: str) -> Optional[int]:
    """Query ``/links/count`` for backlink count for a specific collection and path.

    Args:
        host: Constellation host.
        target: Target AT URI.
        collection: NSID collection (e.g. ``app.bsky.feed.like``).
        path: JSON path (e.g. ``/subject``).

    Returns:
        Integer count, or ``None`` on failure.
    """
    url = f"{host.rstrip('/')}/links/count"
    try:
        resp = httpx.get(
            url,
            params={"target": target, "collection": collection, "path": path},
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"  ⚠ Constellation returned HTTP {resp.status_code}")
            return None
        data = resp.json()
        return data.get("count")
    except httpx.HTTPError as exc:
        print(f"  ⚠ Constellation unavailable: {exc}")
        return None