"""resolve sub-command — resolve a handle to a DID or vice versa."""

from __future__ import annotations

from skycoll.resolve import resolve


def run(identifier: str) -> None:
    """Resolve a handle or DID and print the result.

    Args:
        identifier: A Bluesky handle or a DID string.
    """
    result = resolve(identifier)
    print(f"did:     {result['did']}")
    print(f"handle:  {result['handle']}")
    print(f"pds:     {result['pds']}")