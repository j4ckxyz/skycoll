"""backlinks sub-command — query a Constellation backlinks index."""

from __future__ import annotations

from skycoll.constellation import get_all_backlink_counts
from skycoll.resolve import resolve


def run(handle: str, constellation_host: str) -> None:
    """Query Constellation for backlink counts and pretty-print them.

    Args:
        handle: The user's Bluesky handle.
        constellation_host: Constellation host URL (e.g. ``https://constellation.example.com``).
    """
    identity = resolve(handle)
    did = identity["did"]
    print(f"Querying Constellation at {constellation_host} for {handle} ({did}) …")

    data = get_all_backlink_counts(constellation_host, did)
    if data is None:
        print("No backlink data available.")
        return

    if not data:
        print("No backlinks found.")
        return

    print(f"\nBacklink breakdown for {handle}:\n")
    for collection, paths in data.items():
        if isinstance(paths, dict):
            print(f"  {collection}:")
            for path_key, count in paths.items():
                print(f"    {path_key}: {count}")
        elif isinstance(paths, (int, float)):
            print(f"  {collection}: {int(paths)}")