"""likes sub-command — download likes, write .fav file; -p purges likes."""

from __future__ import annotations

from skycoll.api import get_likes, delete_like
from skycoll.appview import resolve_appview
from skycoll.auth import get_authenticated_session
from skycoll.errors import AuthError, ParseError, SkycollError
from skycoll.output import info, ok, warn
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
    try:
        appview_did = resolve_appview(appview)

        target = resolve(handle)
        target_did = target.get("did") if isinstance(target, dict) else None
        pds = target.get("pds") if isinstance(target, dict) else None
        if not target_did or not pds:
            raise ParseError(f"resolve returned incomplete identity data for '{handle}'")

        if purge:
            info(f"Authenticating as {handle} …")
            session = get_authenticated_session(handle)

            if session.did != target_did:
                raise AuthError(
                    f"Refusing to purge likes for {target_did} while authenticated as {session.did}. "
                    "Log in as the target account first."
                )

            info("Purging all likes …")
            count = 0
            for like_record in get_likes(session, target_did, appview=appview_did):
                uri = like_record.get("uri", "") if isinstance(like_record, dict) else ""
                if uri:
                    try:
                        delete_like(session, uri)
                        count += 1
                        if count % 100 == 0:
                            info(f"  Deleted {count} likes …")
                    except SkycollError as exc:
                        warn(f"Failed to delete {uri}: {exc}")
            ok(f"Deleted {count} likes.")
            return

        info("Fetching likes …")
        likes = []
        for like_record in get_likes(None, target_did, appview=appview_did, pds_endpoint=pds):
            likes.append(like_record)
            if len(likes) % 500 == 0:
                info(f"  {len(likes)} likes …")

        path = write_fav(handle, likes)
        ok(f"Wrote {len(likes)} likes → {path}")
    except SkycollError:
        raise
    except OSError as exc:
        raise ParseError(f"failed to write likes output for '{handle}': {exc}") from exc
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid likes data for '{handle}': {exc}") from exc
    except Exception as exc:
        raise ParseError(f"unexpected likes error for '{handle}': {exc}") from exc
