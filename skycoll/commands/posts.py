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
from skycoll.auth import get_authenticated_session
from skycoll.errors import ParseError, SkycollError
from skycoll.output import info, ok
from skycoll.resolve import resolve
from skycoll.storage import write_twt


def run(handle: str, use_car: bool = False, appview: str | None = None) -> None:
    """Download posts for *handle* and write ``<handle>.twt``.

    Args:
        handle: The user's Bluesky handle.
        use_car: If ``True``, use CAR repo sync instead of feed pagination.
        appview: Optional AppView name or DID (for ``atproto-proxy`` header).
    """
    try:
        appview_did = resolve_appview(appview)
        target = resolve(handle)
        target_did = target.get("did") if isinstance(target, dict) else None
        pds = target.get("pds") if isinstance(target, dict) else None
        if not target_did or not pds:
            raise ParseError(f"resolve returned incomplete identity data for '{handle}'")

        if use_car:
            info(f"Authenticating for CAR sync as {handle} …")
            session = get_authenticated_session(handle)
            info("Downloading full repo CAR (--car mode) …")
            car_bytes = get_repo_car(session, target_did, appview=appview_did)
            info(f"CAR downloaded ({len(car_bytes)} bytes), parsing records …")

            all_records = parse_car_records(car_bytes)
            if not isinstance(all_records, list):
                raise ParseError("CAR parser returned invalid records payload")
            info(f"Total records parsed: {len(all_records)}")

            posts = [
                record
                for record in all_records
                if isinstance(record, dict)
                and record.get("collection")
                in (
                    "app.bsky.feed.post",
                    "app.bsky.feed.repost",
                )
            ]

            info(f"Post/repost records: {len(posts)}")
        else:
            info("Fetching posts via getAuthorFeed …")
            posts = []
            for item in get_author_feed(None, target_did, appview=appview_did, pds_endpoint=pds):
                if not isinstance(item, dict):
                    continue
                feed_item = item.get("post", item)
                if not isinstance(feed_item, dict):
                    continue
                value = feed_item.get("record", {})
                if not isinstance(value, dict):
                    value = {}
                collection = "app.bsky.feed.post"

                reason = item.get("reason", {})
                if isinstance(reason, dict) and reason.get("$type") == "app.bsky.feed.defs#ReasonRepost":
                    collection = "app.bsky.feed.repost"

                embed = value.get("embed", {})
                if isinstance(embed, dict) and embed.get("$type") == "app.bsky.embed.record":
                    collection = "quote"

                uri = feed_item.get("uri", "")

                posts.append({
                    "uri": uri,
                    "collection": collection,
                    "value": value,
                })
                if len(posts) % 500 == 0:
                    info(f"  {len(posts)} items …")

            info(f"Total items fetched: {len(posts)}")

        path = write_twt(handle, posts)
        ok(f"Wrote {len(posts)} records → {path}")
    except SkycollError:
        raise
    except OSError as exc:
        raise ParseError(f"failed to write posts output for '{handle}': {exc}") from exc
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid posts data for '{handle}': {exc}") from exc
    except Exception as exc:
        raise ParseError(f"unexpected posts error for '{handle}': {exc}") from exc
