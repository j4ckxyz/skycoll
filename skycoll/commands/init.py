"""init sub-command — fetch profile + follows/followers, write .dat and avatars.

Extended flags:
  --lists   Also fetch lists the user has created (app.bsky.graph.getLists).
  --labels  Include self-labels and server-assigned labels in the .dat header.
"""

from __future__ import annotations

from skycoll.api import (
    get_profile,
    get_follows,
    get_followers,
    get_lists,
    get_starter_packs,
    download_avatar,
)
from skycoll.auth import get_authenticated_session
from skycoll.storage import write_dat, avatar_path


def run(handle: str, fetch_lists: bool = False, include_labels: bool = False) -> None:
    """Initialize skycoll data for *handle*.

    Fetches the user's profile, their follows, and their followers, then
    writes ``<handle>.dat`` and downloads avatars to ``img/``.

    Args:
        handle: The user's Bluesky handle.
        fetch_lists: If ``True``, also fetch and include user lists.
        include_labels: If ``True``, include self-labels and server labels.
    """
    print(f"Authenticating as {handle} …")
    session = get_authenticated_session(handle)
    print(f"Authenticated as {session.handle} ({session.did})")

    print("Fetching profile …")
    profile = get_profile(session, session.did)

    print("Fetching follows …")
    follows = list(get_follows(session, session.did))

    print("Fetching followers …")
    followers = list(get_followers(session, session.did))

    print(f"Follows: {len(follows)}, Followers: {len(followers)}")

    lists_data = []
    if fetch_lists:
        print("Fetching lists …")
        lists_data = list(get_lists(session, session.did))
        print(f"Lists: {len(lists_data)}")

    starter_packs_data = []
    print("Fetching starter packs …")
    try:
        starter_packs_data = list(get_starter_packs(session, session.did))
        print(f"Starter packs: {len(starter_packs_data)}")
    except RuntimeError as exc:
        print(f"  ⚠ Could not fetch starter packs: {exc}")

    dat_path = write_dat(
        handle,
        profile,
        follows,
        followers,
        lists=lists_data,
        starter_packs=starter_packs_data,
    )
    print(f"Wrote {dat_path}")

    # Download profile avatar
    avatar_url = profile.get("avatar", "")
    if avatar_url:
        dest = avatar_path(handle)
        print(f"Downloading avatar → {dest}")
        download_avatar(session, avatar_url, dest)

    # Download follow avatars
    for person in follows:
        avatar_url = person.get("avatar", "")
        if avatar_url:
            person_handle = person.get("handle", "")
            if person_handle:
                dest = avatar_path(person_handle)
                download_avatar(session, avatar_url, dest)

    print("Done.")