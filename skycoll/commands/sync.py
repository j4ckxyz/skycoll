"""sync sub-command — download the full repo CAR for archival.

Fetches the complete CAR (Content Addressable aRchive) of the user's AT
Protocol repository and writes it to ``<handle>.car`` as raw bytes.  No
parsing is performed.
"""

from __future__ import annotations

from skycoll.api import get_repo_car
from skycoll.auth import get_authenticated_session
from skycoll.storage import write_car


def run(handle: str) -> None:
    """Download the full repo CAR for *handle* and write ``<handle>.car``.

    Args:
        handle: The user's Bluesky handle.
    """
    print(f"Authenticating as {handle} …")
    session = get_authenticated_session(handle)

    print(f"Downloading full repo CAR for {session.did} …")
    car_bytes = get_repo_car(session, session.did)

    path = write_car(handle, car_bytes)
    print(f"Wrote {len(car_bytes)} bytes → {path}")