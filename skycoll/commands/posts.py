"""posts sub-command — download posts via getAuthorFeed, write .twt file.

By default, uses ``app.bsky.feed.getAuthorFeed`` with cursor-based pagination
(no artificial cap — pages until the cursor is exhausted).

Use ``--car`` to download the full repo CAR via ``com.atproto.sync.getRepo``
and extract all post/repost/quote records.  This is slower but gives a
complete archive including records the feed may not surface.
"""

from __future__ import annotations

from skycoll.api import get_repo_car, parse_car_records, get_author_feed
from skycoll.appview import resolve_appview
from skycoll.auth import get_any_session
from skycoll.resolve import resolve
from skycoll.storage import write_twt


def run(handle: str, use_car: bool = False, appview: str | None = None) -> None:
    """Download posts for *handle* and write ``<handle>.twt``.

    Args:
        handle: The user's Bluesky handle.
        use_car: If ``True``, use CAR repo sync instead of feed pagination.
        appview: Optional AppView name or DID (for ``atproto-proxy`` header).
    """
    appview_did = resolve_appview(appview)
    target = resolve(handle)
    target_did = target["did"]

    session = get_any_session()
    if session is None:
        print("No cached OAuth session found. Run: skycoll init <your-handle>")
        raise SystemExit(1)
    print(f"Using cached session: {session.handle} ({session.did})")

    if use_car:
        print("Downloading full repo CAR (--car mode) …")
        car_bytes = get_repo_car(session, target_did, appview=appview_did)
        print(f"CAR downloaded ({len(car_bytes)} bytes), parsing records …")

        all_records = parse_car_records(car_bytes)
        print(f"Total records parsed: {len(all_records)}")

        posts = [
            r for r in all_records
            if r.get("collection") in (
                "app.bsky.feed.post",
                "app.bsky.feed.repost",
            )
        ]

        print(f"Post/repost records: {len(posts)}")
    else:
        print("Fetching posts via getAuthorFeed …")
        posts = []
        for item in get_author_feed(session, target_did, appview=appview_did):
            feed_item = item.get("post", item)
            value = feed_item.get("record", {})
            collection = "app.bsky.feed.post"

            reason = item.get("reason", {})
            if reason.get("$type") == "app.bsky.feed.defs#ReasonRepost":
                collection = "app.bsky.feed.repost"

            embed = value.get("embed", {})
            if embed.get("$type") == "app.bsky.embed.record":
                collection = "quote"

            uri = feed_item.get("uri", "")
            # Expand replies
            reply = value.get("reply", {})
            reply_to_uri = reply.get("parent", {}).get("uri", "") if reply else ""
            root_uri = reply.get("root", {}).get("uri", "") if reply else ""

            posts.append({
                "uri": uri,
                "collection": collection,
                "value": value,
            })
            if len(posts) % 500 == 0:
                print(f"  {len(posts)} items …")

        print(f"Total items fetched: {len(posts)}")

    path = write_twt(handle, posts)
    print(f"Wrote {len(posts)} records → {path}")
