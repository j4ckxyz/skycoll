"""posts sub-command — download posts via CAR repo sync, write .twt file.

By default, uses ``com.atproto.sync.getRepo`` to download the full CAR and
extracts all ``app.bsky.feed.post`` and ``app.bsky.feed.repost`` records.
This removes any artificial post cap.

Use ``--feed`` to fall back to the older ``app.bsky.feed.getAuthorFeed``
paginated approach (limited to ~3000 items).
"""

from __future__ import annotations

from skycoll.api import get_repo_car, parse_car_records, get_author_feed
from skycoll.auth import get_authenticated_session
from skycoll.storage import write_twt


def run(handle: str, use_feed: bool = False) -> None:
    """Download posts for *handle* and write ``<handle>.twt``.

    Args:
        handle: The user's Bluesky handle.
        use_feed: If ``True``, use ``app.bsky.feed.getAuthorFeed`` instead
            of CAR sync (limited to ~3000 items).
    """
    print(f"Authenticating as {handle} …")
    session = get_authenticated_session(handle)
    did = session.did

    if use_feed:
        print("Fetching posts via getAuthorFeed (--feed mode) …")
        posts = []
        for item in get_author_feed(session, did):
            feed_item = item.get("post", item)
            value = feed_item.get("record", {})
            collection = "app.bsky.feed.post"

            # Detect reposts and quotes via feed-specific fields
            reason = item.get("reason", {})
            if reason.get("$type") == "app.bsky.feed.defs#ReasonRepost":
                collection = "app.bsky.feed.repost"

            embed = value.get("embed", {})
            if embed.get("$type") == "app.bsky.embed.record":
                collection = "quote"

            uri = feed_item.get("uri", "")
            posts.append({
                "uri": uri,
                "collection": collection,
                "value": value,
            })
            if len(posts) % 500 == 0:
                print(f"  {len(posts)} items …")

        print(f"Total items fetched: {len(posts)}")
    else:
        print("Downloading full repo CAR …")
        car_bytes = get_repo_car(session, did)
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
        # Mark quote posts (posts that embed another post)
        for post in posts:
            if post["collection"] == "app.bsky.feed.post":
                value = post.get("value", {})
                embed = value.get("embed", {})
                if embed.get("$type") == "app.bsky.embed.record":
                    pass

        print(f"Post/repost records: {len(posts)}")

    path = write_twt(handle, posts)
    print(f"Wrote {len(posts)} records → {path}")