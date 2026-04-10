"""fetch sub-command — fetch follows of every handle in .dat, write to fdat/."""

from __future__ import annotations

from skycoll.api import get_profile, get_follows, download_avatar
from skycoll.errors import NotFoundError, ParseError, SkycollError
from skycoll.output import info, ok, warn
from skycoll.resolve import resolve
from skycoll.storage import read_dat, write_fdat, avatar_path


def run(handle: str) -> None:
    """Fetch the follows of every user listed in ``<handle>.dat``.

    Reads ``<handle>.dat``, iterates over every followed user, fetches
    their profile and follows, and writes ``fdat/<friend>.dat``.

    Args:
        handle: The focal user's handle (used to find ``<handle>.dat``).
    """
    try:
        try:
            data = read_dat(handle)
        except FileNotFoundError as exc:
            raise NotFoundError(
                f"No .dat file found for '{handle}'. Run: skycoll init {handle}"
            ) from exc

        identity = resolve(handle)
        pds = identity.get("pds")
        if not pds:
            raise ParseError(f"resolved identity for '{handle}' is missing a PDS endpoint")

        follows = data.get("follows", [])
        if not isinstance(follows, list):
            raise ParseError(f"invalid .dat data for '{handle}': follows must be a list")
        info(f"Fetching follows for {len(follows)} users …")

        for i, person in enumerate(follows, 1):
            friend_handle = person.get("handle", "") if isinstance(person, dict) else ""
            friend_did = person.get("did", "") if isinstance(person, dict) else ""
            if not friend_handle:
                warn(f"[{i}/{len(follows)}] Skipping entry without handle")
                continue

            info(f"  [{i}/{len(follows)}] {friend_handle}")

            try:
                profile = get_profile(None, friend_did or friend_handle, pds_endpoint=pds)
            except Exception as exc:
                warn(f"Profile fetch failed for {friend_handle}: {exc}")
                continue

            friend_follows = []
            try:
                for friend_follow in get_follows(None, friend_did or friend_handle, pds_endpoint=pds):
                    friend_follows.append(friend_follow)
            except Exception as exc:
                warn(f"Follows fetch failed for {friend_handle}: {exc}")

            path = write_fdat(friend_handle, profile, friend_follows)
            info(f"    → {path} ({len(friend_follows)} follows)")

            avatar_url = profile.get("avatar", "") if isinstance(profile, dict) else ""
            if avatar_url:
                download_avatar(None, avatar_url, avatar_path(friend_handle))

        ok("Done.")
    except SkycollError:
        raise
    except OSError as exc:
        raise ParseError(f"failed to write fetched data for '{handle}': {exc}") from exc
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid fetch data for '{handle}': {exc}") from exc
    except Exception as exc:
        raise ParseError(f"unexpected fetch error for '{handle}': {exc}") from exc
