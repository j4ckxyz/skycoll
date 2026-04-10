"""fetch sub-command — fetch follows of every handle in .dat, write to fdat/."""

from __future__ import annotations

from skycoll.api import get_profile, get_follows, download_avatar
from skycoll.auth import get_any_session
from skycoll.storage import read_dat, write_fdat, avatar_path


def run(handle: str) -> None:
    """Fetch the follows of every user listed in ``<handle>.dat``.

    Reads ``<handle>.dat``, iterates over every followed user, fetches
    their profile and follows, and writes ``fdat/<friend>.dat``.

    Args:
        handle: The focal user's handle (used to find ``<handle>.dat``).
    """
    try:
        data = read_dat(handle)
    except FileNotFoundError:
        print(f"No .dat file found for '{handle}'. Run: skycoll init {handle}")
        raise SystemExit(1)

    follows = data["follows"]
    print(f"Fetching follows for {len(follows)} users …")

    session = get_any_session()
    if session is None:
        print("No cached OAuth session found. Run: skycoll init <your-handle>")
        raise SystemExit(1)
    print(f"Using cached session: {session.handle} ({session.did})")

    for i, person in enumerate(follows, 1):
        friend_handle = person.get("handle", "")
        friend_did = person.get("did", "")
        if not friend_handle:
            print(f"  [{i}/{len(follows)}] Skipping entry without handle")
            continue

        print(f"  [{i}/{len(follows)}] {friend_handle}")

        try:
            profile = get_profile(session, friend_did or friend_handle)
        except RuntimeError as exc:
            print(f"    ⚠ Profile fetch failed: {exc}")
            continue

        friend_follows = []
        try:
            for f in get_follows(session, friend_did or friend_handle):
                friend_follows.append(f)
        except RuntimeError as exc:
            print(f"    ⚠ Follows fetch failed: {exc}")

        path = write_fdat(friend_handle, profile, friend_follows)
        print(f"    → {path} ({len(friend_follows)} follows)")

        # Download their avatar
        avatar_url = profile.get("avatar", "")
        if avatar_url:
            download_avatar(session, avatar_url, avatar_path(friend_handle))

    print("Done.")
