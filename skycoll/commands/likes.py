"""likes sub-command — download likes, write .fav file; -p purges likes."""

from __future__ import annotations

from skycoll.api import get_likes, delete_like
from skycoll.appview import resolve_appview
from skycoll.auth import get_authenticated_session, get_any_session
from skycoll.resolve import resolve
from skycoll.storage import write_fav


def run(handle: str, purge: bool = False, appview: str | None = None) -> None:
    """Download (and optionally purge) likes for *handle*.

    Without ``--purge``: writes ``<handle>.fav`` with all likes.
    With ``--purge``: deletes all likes (only performs the deletion, does
    not write a ``.fav`` file first).

    Args:
        handle: The user's Bluesky handle.
        purge: If ``True``, delete all likes instead of just writing the file.
        appview: Optional AppView name or DID (for ``atproto-proxy`` header on reads).
    """
    appview_did = resolve_appview(appview)

    target = resolve(handle)
    target_did = target["did"]

    if purge:
        print(f"Authenticating as {handle} …")
        session = get_authenticated_session(handle)

        # Safety: purge must operate on the authenticated account only.
        if session.did != target_did:
            raise RuntimeError(
                f"Refusing to purge likes for {target_did} while authenticated as {session.did}. "
                "Log in as the target account first."
            )

        print("Purging all likes …")
        count = 0
        for like_record in get_likes(session, target_did, appview=appview_did):
            uri = like_record.get("uri", "")
            if uri:
                try:
                    delete_like(session, uri)
                    count += 1
                    if count % 100 == 0:
                        print(f"  Deleted {count} likes …")
                except RuntimeError as exc:
                    print(f"  ⚠ Failed to delete {uri}: {exc}")
        print(f"Deleted {count} likes.")
        return

    session = get_any_session()
    if session is None:
        print("No cached OAuth session found. Run: skycoll init <your-handle>")
        raise SystemExit(1)
    print(f"Using cached session: {session.handle} ({session.did})")

    print("Fetching likes …")
    likes = []
    for like_record in get_likes(session, target_did, appview=appview_did):
        likes.append(like_record)
        if len(likes) % 500 == 0:
            print(f"  {len(likes)} likes …")

    path = write_fav(handle, likes)
    print(f"Wrote {len(likes)} likes → {path}")
