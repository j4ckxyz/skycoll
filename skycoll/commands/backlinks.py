"""backlinks sub-command — query a Constellation backlinks index."""

from __future__ import annotations

from skycoll.constellation import get_all_backlink_counts
from skycoll.errors import ParseError, SkycollError
from skycoll.output import info
from skycoll.resolve import resolve


def run(handle: str, constellation_host: str) -> None:
    """Query Constellation for backlink counts and pretty-print them.

    Args:
        handle: The user's Bluesky handle.
        constellation_host: Constellation host URL (e.g. ``https://constellation.example.com``).
    """
    try:
        identity = resolve(handle)
        did = identity.get("did") if isinstance(identity, dict) else None
        if not did:
            raise ParseError(f"resolve returned incomplete identity data for '{handle}'")
        info(f"Querying Constellation at {constellation_host} for {handle} ({did}) …")

        data = get_all_backlink_counts(constellation_host, did)
        if data is None:
            info("No backlink data available.")
            return

        if not data:
            info("No backlinks found.")
            return

        if not isinstance(data, dict):
            raise ParseError(f"invalid backlink payload for '{handle}': expected an object")

        info(f"\nBacklink breakdown for {handle}:\n")
        for collection, paths in data.items():
            if isinstance(paths, dict):
                info(f"  {collection}:")
                for path_key, count in paths.items():
                    info(f"    {path_key}: {count}")
            elif isinstance(paths, (int, float)):
                info(f"  {collection}: {int(paths)}")
    except SkycollError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid backlinks data for '{handle}': {exc}") from exc
    except Exception as exc:
        raise ParseError(f"unexpected backlinks error for '{handle}': {exc}") from exc
