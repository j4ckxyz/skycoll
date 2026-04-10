"""init sub-command — fetch profile + follows/followers, write .dat and avatars.

Extended flags:
  --lists         Also fetch lists (app.bsky.graph.getLists).
  --labels        Include self-labels and server-assigned labels in .dat.
  --appview       Route through a specific AppView (e.g. ``blacksky``).
  --constellation  Query a Constellation backlinks index and include results.
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
from skycoll.appview import resolve_appview
from skycoll.auth import get_authenticated_session
from skycoll.constellation import get_all_backlink_counts
from skycoll.storage import write_dat, avatar_path


def run(
    handle: str,
    fetch_lists: bool = False,
    include_labels: bool = False,
    appview: str | None = None,
    constellation: str | None = None,
) -> None:
    """Initialize skycoll data for *handle*.

    Args:
        handle: The user's Bluesky handle.
        fetch_lists: If ``True``, also fetch and include user lists.
        include_labels: If ``True``, include self-labels and server labels.
        appview: Optional AppView name or DID (for ``atproto-proxy`` header).
        constellation: Optional Constellation host URL for backlinks.
    """
    appview_did = resolve_appview(appview)

    print(f"Authenticating as {handle} …")
    session = get_authenticated_session(handle)
    print(f"Authenticated as {session.handle} ({session.did})")

    print("Fetching profile …")
    profile = get_profile(session, session.did, appview=appview_did)

    if not include_labels:
        profile = dict(profile)
        profile.pop("labels", None)
        profile.pop("selfLabels", None)

    print("Fetching follows …")
    follows = list(get_follows(session, session.did, appview=appview_did))

    print("Fetching followers …")
    followers = list(get_followers(session, session.did, appview=appview_did))

    print(f"Follows: {len(follows)}, Followers: {len(followers)}")

    lists_data = []
    if fetch_lists:
        print("Fetching lists …")
        lists_data = list(get_lists(session, session.did, appview=appview_did))
        print(f"Lists: {len(lists_data)}")

    starter_packs_data = []
    print("Fetching starter packs …")
    try:
        starter_packs_data = list(get_starter_packs(session, session.did, appview=appview_did))
        print(f"Starter packs: {len(starter_packs_data)}")
    except RuntimeError as exc:
        print(f"  ⚠ Could not fetch starter packs: {exc}")

    backlinks = None
    if constellation:
        print(f"Querying Constellation backlinks at {constellation} …")
        backlinks = get_all_backlink_counts(constellation, session.did)
        if backlinks:
            print(f"  Backlink data received")
        else:
            print("  ⚠ No backlink data available")

    dat_path = write_dat(
        handle,
        profile,
        follows,
        followers,
        lists=lists_data,
        starter_packs=starter_packs_data,
        backlinks=backlinks,
    )
    print(f"Wrote {dat_path}")

    avatar_url = profile.get("avatar", "")
    if avatar_url:
        dest = avatar_path(handle)
        print(f"Downloading avatar → {dest}")
        download_avatar(session, avatar_url, dest)

    for person in follows:
        avatar_url = person.get("avatar", "")
        if avatar_url:
            person_handle = person.get("handle", "")
            if person_handle:
                dest = avatar_path(person_handle)
                download_avatar(session, avatar_url, dest)

    print("Done.")
