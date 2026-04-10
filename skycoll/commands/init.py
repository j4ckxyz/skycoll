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
from skycoll.constellation import get_all_backlink_counts
from skycoll.errors import ParseError, SkycollError
from skycoll.output import info, ok, warn
from skycoll.resolve import resolve
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
    try:
        appview_did = resolve_appview(appview)
        identity = resolve(handle)
        did = identity.get("did") if isinstance(identity, dict) else None
        pds = identity.get("pds") if isinstance(identity, dict) else None
        resolved_handle = identity.get("handle") if isinstance(identity, dict) else None
        if not did or not pds or not resolved_handle:
            raise ParseError(f"resolve returned incomplete identity data for '{handle}'")

        info(f"Resolved {resolved_handle} ({did})")

        info("Fetching profile …")
        profile = get_profile(None, did, appview=appview_did, pds_endpoint=pds)

        if not include_labels:
            profile = dict(profile)
            profile.pop("labels", None)
            profile.pop("selfLabels", None)

        info("Fetching follows …")
        follows = list(get_follows(None, did, appview=appview_did, pds_endpoint=pds))

        info("Fetching followers …")
        followers = list(get_followers(None, did, appview=appview_did, pds_endpoint=pds))

        info(f"Follows: {len(follows)}, Followers: {len(followers)}")

        lists_data = []
        if fetch_lists:
            info("Fetching lists …")
            lists_data = list(get_lists(None, did, appview=appview_did, pds_endpoint=pds))
            info(f"Lists: {len(lists_data)}")

        starter_packs_data = []
        info("Fetching starter packs …")
        try:
            starter_packs_data = list(get_starter_packs(None, did, appview=appview_did, pds_endpoint=pds))
            info(f"Starter packs: {len(starter_packs_data)}")
        except Exception as exc:
            warn(f"Could not fetch starter packs: {exc}")

        backlinks = None
        if constellation:
            info(f"Querying Constellation backlinks at {constellation} …")
            backlinks = get_all_backlink_counts(constellation, did)
            if backlinks:
                ok("Backlink data received")
            else:
                warn("No backlink data available")

        dat_path = write_dat(
            handle,
            profile,
            follows,
            followers,
            lists=lists_data,
            starter_packs=starter_packs_data,
            backlinks=backlinks,
        )
        ok(f"Wrote {dat_path}")

        avatar_url = profile.get("avatar", "") if isinstance(profile, dict) else ""
        if avatar_url:
            dest = avatar_path(handle)
            info(f"Downloading avatar → {dest}")
            download_avatar(None, avatar_url, dest)

        for person in follows:
            avatar_url = person.get("avatar", "") if isinstance(person, dict) else ""
            if avatar_url:
                person_handle = person.get("handle", "") if isinstance(person, dict) else ""
                if person_handle:
                    dest = avatar_path(person_handle)
                    download_avatar(None, avatar_url, dest)

        ok("Done.")
    except SkycollError:
        raise
    except OSError as exc:
        raise ParseError(f"failed to write init output for '{handle}': {exc}") from exc
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid init data for '{handle}': {exc}") from exc
    except Exception as exc:
        raise ParseError(f"unexpected init error for '{handle}': {exc}") from exc
