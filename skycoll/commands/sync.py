"""sync sub-command — download the full repo CAR for archival.

Fetches the complete CAR (Content Addressable aRchive) of the user's AT
Protocol repository and writes it to ``<handle>.car`` as raw bytes.  No
parsing is performed.
"""

from __future__ import annotations

from skycoll.api import get_repo_car
from skycoll.auth import get_authenticated_session
from skycoll.errors import ParseError, SkycollError
from skycoll.output import info, ok
from skycoll.resolve import resolve
from skycoll.storage import write_car


def run(handle: str) -> None:
    """Download the full repo CAR for *handle* and write ``<handle>.car``.

    Args:
        handle: The user's Bluesky handle.
    """
    try:
        target = resolve(handle)
        target_did = target.get("did") if isinstance(target, dict) else None
        if not target_did:
            raise ParseError(f"resolve returned incomplete identity data for '{handle}'")

        info(f"Authenticating as {handle} …")
        session = get_authenticated_session(handle)

        info(f"Downloading full repo CAR for {target_did} …")
        car_bytes = get_repo_car(session, target_did)

        path = write_car(handle, car_bytes)
        ok(f"Wrote {len(car_bytes)} bytes → {path}")
    except SkycollError:
        raise
    except OSError as exc:
        raise ParseError(f"failed to write CAR file for '{handle}': {exc}") from exc
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid sync data for '{handle}': {exc}") from exc
    except Exception as exc:
        raise ParseError(f"unexpected sync error for '{handle}': {exc}") from exc
