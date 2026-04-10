"""resolve sub-command — resolve a handle to a DID or vice versa."""

from __future__ import annotations

from skycoll.errors import ParseError, SkycollError
from skycoll.output import info
from skycoll.resolve import resolve


def run(identifier: str) -> None:
    """Resolve a handle or DID and print the result.

    Args:
        identifier: A Bluesky handle or a DID string.
    """
    try:
        result = resolve(identifier)
        did = result.get("did") if isinstance(result, dict) else None
        handle = result.get("handle") if isinstance(result, dict) else None
        pds = result.get("pds") if isinstance(result, dict) else None
        if not did or not handle or not pds:
            raise ParseError(f"resolve returned incomplete identity data for '{identifier}'")

        info(f"did:     {did}")
        info(f"handle:  {handle}")
        info(f"pds:     {pds}")
    except SkycollError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid resolve data for '{identifier}': {exc}") from exc
    except Exception as exc:
        raise ParseError(f"unexpected resolve error for '{identifier}': {exc}") from exc
