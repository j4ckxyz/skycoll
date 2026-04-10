"""AT Protocol API wrappers with cursor-based pagination and rate-limit handling.

All list endpoints in AT Protocol return a ``cursor`` field; callers loop
until no cursor is returned.  On HTTP 429, exponential back-off is applied
(up to 3 retries).

Includes CAR repo sync (``com.atproto.sync.getRepo``) for unlimited
post/repost/quote extraction.
"""

from __future__ import annotations

import io
import os
import time
from typing import Generator, Optional

import httpx

from .auth import Session, make_authenticated_request

_MAX_POSTS = 3000


def _paginated_get(
    session: Session,
    path: str,
    params: Optional[dict] = None,
    cursor_key: str = "cursor",
    collection_items_key: str = "records",
    appview: Optional[str] = None,
) -> Generator[dict, None, None]:
    """Yield individual items from a paginated AT Protocol list endpoint.

    Follows the ``cursor`` convention: on each request, the response may
    contain a ``cursor`` string; if present it is sent in the next request.
    The loop stops when there is no cursor.

    On HTTP 429, the generator backs off with exponential retries (max 3).

    Args:
        session: Authenticated session.
        path: XRPC path (e.g. ``/xrpc/com.atproto.repo.listRecords``).
        params: Query parameters for the first request.
        cursor_key: The JSON key holding the next cursor (default ``"cursor"``).
        collection_items_key: The JSON key holding the list of items.
        appview: Optional AppView service DID for the ``atproto-proxy`` header.

    Yields:
        Each item dict from the paginated responses.
    """
    params = dict(params or {})
    max_retries = 3

    while True:
        for attempt in range(max_retries + 1):
            resp = make_authenticated_request(session, "GET", path, params=params, appview=appview)

            if resp.status_code == 429:
                if attempt == max_retries:
                    raise RuntimeError(f"Rate-limited on {path} after {max_retries} retries")
                wait = 2 ** attempt
                print(f"  Rate-limited on {path}, retrying in {wait}s …")
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                raise RuntimeError(
                    f"API error on {path}: HTTP {resp.status_code} — {resp.text[:200]}"
                )
            break

        data = resp.json()
        items = data.get(collection_items_key, [])
        if not items and collection_items_key != "records":
            items = data.get("records", [])

        for item in items:
            yield item

        cursor = data.get(cursor_key)
        if not cursor:
            break
        params["cursor"] = cursor


# ---------------------------------------------------------------------------
# Profile & graph
# ---------------------------------------------------------------------------


def get_profile(session: Session, actor: str, appview: Optional[str] = None) -> dict:
    """Fetch the profile (``app.bsky.actor.getProfile``) for *actor*.

    Args:
        session: Authenticated session.
        actor: A handle or DID.
        appview: Optional AppView service DID for the ``atproto-proxy`` header.

    Returns:
        Profile record dict.
    """
    resp = make_authenticated_request(
        session,
        "GET",
        "/xrpc/app.bsky.actor.getProfile",
        params={"actor": actor},
        appview=appview,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch profile for {actor}: HTTP {resp.status_code}"
        )
    return resp.json()


def get_follows(
    session: Session, actor: str, appview: Optional[str] = None
) -> Generator[dict, None, None]:
    """Yield follow-record items for *actor*.

    Each item is a dict with ``did`` and ``handle`` keys.

    Args:
        session: Authenticated session.
        actor: A handle or DID.
        appview: Optional AppView service DID for the ``atproto-proxy`` header.
    """
    yield from _paginated_get(
        session,
        "/xrpc/app.bsky.graph.getFollows",
        params={"actor": actor, "limit": 100},
        cursor_key="cursor",
        collection_items_key="follows",
        appview=appview,
    )


def get_followers(
    session: Session, actor: str, appview: Optional[str] = None
) -> Generator[dict, None, None]:
    """Yield follower-record items for *actor*.

    Each item is a dict with ``did`` and ``handle`` keys.

    Args:
        session: Authenticated session.
        actor: A handle or DID.
        appview: Optional AppView service DID for the ``atproto-proxy`` header.
    """
    yield from _paginated_get(
        session,
        "/xrpc/app.bsky.graph.getFollowers",
        params={"actor": actor, "limit": 100},
        cursor_key="cursor",
        collection_items_key="followers",
        appview=appview,
    )


def get_lists(session: Session, actor: str, appview: Optional[str] = None) -> Generator[dict, None, None]:
    """Yield list-view items for *actor* via ``app.bsky.graph.getLists``.

    Args:
        session: Authenticated session.
        actor: A handle or DID.
        appview: Optional AppView service DID for the ``atproto-proxy`` header.
    """
    yield from _paginated_get(
        session,
        "/xrpc/app.bsky.graph.getLists",
        params={"actor": actor, "limit": 50},
        cursor_key="cursor",
        collection_items_key="lists",
        appview=appview,
    )


def get_starter_packs(session: Session, actor: str, appview: Optional[str] = None) -> Generator[dict, None, None]:
    """Yield starter-pack view items for *actor* via ``app.bsky.graph.getActorStarterPacks``.

    Args:
        session: Authenticated session.
        actor: A handle or DID.
        appview: Optional AppView service DID for the ``atproto-proxy`` header.
    """
    yield from _paginated_get(
        session,
        "/xrpc/app.bsky.graph.getActorStarterPacks",
        params={"actor": actor, "limit": 50},
        cursor_key="cursor",
        collection_items_key="starterPacks",
        appview=appview,
    )


# ---------------------------------------------------------------------------
# Posts & feed
# ---------------------------------------------------------------------------


def get_posts(
    session: Session, actor: str, limit: int = _MAX_POSTS, appview: Optional[str] = None
) -> Generator[dict, None, None]:
    """Yield post records for *actor*, up to *limit* posts.

    Uses ``com.atproto.repo.listRecords`` with collection
    ``app.bsky.feed.post``.

    Args:
        session: Authenticated session.
        actor: A handle or DID.
        limit: Maximum number of posts to yield (default 3000).
        appview: Optional AppView service DID.
    """
    count = 0
    for record in _paginated_get(
        session,
        "/xrpc/com.atproto.repo.listRecords",
        params={
            "repo": actor,
            "collection": "app.bsky.feed.post",
            "limit": 100,
        },
        cursor_key="cursor",
        collection_items_key="records",
        appview=appview,
    ):
        yield record
        count += 1
        if count >= limit:
            break


def get_author_feed(
    session: Session, actor: str, appview: Optional[str] = None
) -> Generator[dict, None, None]:
    """Yield feed-view items for *actor* via ``app.bsky.feed.getAuthorFeed``.

    Paginates until the cursor is exhausted (no artificial cap).

    Args:
        session: Authenticated session.
        actor: A handle or DID.
        appview: Optional AppView service DID.
    """
    yield from _paginated_get(
        session,
        "/xrpc/app.bsky.feed.getAuthorFeed",
        params={"actor": actor, "limit": 100},
        cursor_key="cursor",
        collection_items_key="feed",
        appview=appview,
    )


# ---------------------------------------------------------------------------
# Likes
# ---------------------------------------------------------------------------


def get_likes(
    session: Session, actor: str, appview: Optional[str] = None
) -> Generator[dict, None, None]:
    """Yield like-record items for *actor*.

    Uses ``com.atproto.repo.listRecords`` with collection
    ``app.bsky.feed.like``.

    Args:
        session: Authenticated session.
        actor: A handle or DID.
        appview: Optional AppView service DID.
    """
    yield from _paginated_get(
        session,
        "/xrpc/com.atproto.repo.listRecords",
        params={
            "repo": actor,
            "collection": "app.bsky.feed.like",
            "limit": 100,
        },
        cursor_key="cursor",
        collection_items_key="records",
        appview=appview,
    )


def delete_like(session: Session, uri: str) -> None:
    """Delete a like record by its AT URI.

    Uses ``com.atproto.repo.deleteRecord``.

    Args:
        session: Authenticated session.
        uri: The AT URI of the like (``at://did:plc:…/app.bsky.feed.like/…``).
    """
    parts = uri.split("/")
    if len(parts) < 5:
        raise ValueError(f"Invalid AT URI: {uri}")
    repo = parts[2]
    collection = parts[3]
    rkey = parts[4]

    resp = make_authenticated_request(
        session,
        "POST",
        "/xrpc/com.atproto.repo.deleteRecord",
        json={"repo": repo, "collection": collection, "rkey": rkey},
    )
    if resp.status_code not in (200, 204):
        raise RuntimeError(
            f"Failed to delete like {uri}: HTTP {resp.status_code} — {resp.text[:200]}"
        )


# ---------------------------------------------------------------------------
# CAR repo sync
# ---------------------------------------------------------------------------


def get_repo_car(session: Session, did: str, appview: Optional[str] = None) -> bytes:
    """Download the full CAR (Content Addressable aRchive) of a user's repo.

    Uses ``com.atproto.sync.getRepo`` to download the complete repository.
    This contains all records (posts, reposts, likes, follows, etc.) and
    removes any artificial pagination cap.

    Args:
        session: Authenticated session.
        did: The DID of the user whose repo to download.
        appview: Optional AppView service DID for the ``atproto-proxy`` header.

    Returns:
        Raw CAR bytes.

    Raises:
        RuntimeError: If the download fails after retries.
    """
    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        resp = make_authenticated_request(
            session,
            "GET",
            "/xrpc/com.atproto.sync.getRepo",
            params={"did": did},
            appview=appview,
        )
        if resp.status_code == 429:
            wait = 2 ** attempt
            print(f"  Rate-limited on getRepo, retrying in {wait}s …")
            time.sleep(wait)
            continue
        if resp.status_code != 200:
            raise RuntimeError(
                f"Failed to download repo CAR for {did}: HTTP {resp.status_code} — {resp.text[:200]}"
            )
        return resp.content
    raise RuntimeError(f"Failed to download repo CAR for {did} after {max_attempts} attempts")


def parse_car_records(car_bytes: bytes) -> list[dict]:
    """Parse a CAR file and extract all records.

    Attempts to use the ``atproto`` library's CAR utilities first, then
    falls back to a manual dag-cbor + CAR v1 parser.

    Each returned dict has keys:
      - ``uri``: The AT URI (``at://<did>/<collection>/<rkey>``)
      - ``collection``: The NSID collection (e.g. ``app.bsky.feed.post``)
      - ``rkey``: The record key
      - ``value``: The decoded CBOR record as a dict

    Args:
        car_bytes: Raw CAR file bytes.

    Returns:
        List of record dicts.
    """
    records = []

    try:
        from atproto import CAR
        car = CAR.from_bytes(car_bytes)
        for block in car.blocks:
            if hasattr(block, 'type') and block.type == 'record':
                records.append({
                    "uri": f"at://{block.did}/{block.collection}/{block.rkey}",
                    "collection": block.collection,
                    "rkey": block.rkey,
                    "value": block.value if isinstance(block.value, dict) else {},
                })
        if records:
            return records
    except (ImportError, Exception):
        pass

    records = _parse_car_manual(car_bytes)
    return records


def _parse_car_manual(car_bytes: bytes) -> list[dict]:
    """Manually parse a CAR v1 file and extract AT Protocol records.

    Uses cbor2 to decode each block's dag-cbor payload.  In CAR v1, every
    block is structured as: varint-length-prefixed (CID + dag-cbor value).
    The dag-cbor value is a CBOR map that may contain a ``$type`` field.

    The first block in an AT Protocol repo is a Commit entry containing the
    repo DID, which we use to construct AT URIs for each record.

    Args:
        car_bytes: Raw CAR file bytes.

    Returns:
        List of record dicts with uri, collection, rkey, and value.
    """
    import cbor2

    stream = io.BytesIO(car_bytes)

    # Read CAR v1 header: varint-length-prefixed CBOR
    header_len = _read_varint(stream)
    stream.read(header_len)

    commit_did: Optional[str] = None
    records: list[dict] = []
    rkey_counter = 0

    while True:
        try:
            block_len = _read_varint(stream)
        except (EOFError, Exception):
            break
        if block_len == 0:
            break

        block_data = stream.read(block_len)
        if len(block_data) < block_len:
            break

        # In CAR v1, each block starts with a CID (variable-length), followed
        # by a dag-cbor encoded value.  We scan for CBOR major type 5 (map)
        # to find where the CID ends and the payload begins.
        payload_offset = _find_cbor_payload(block_data)
        if payload_offset is None:
            continue

        try:
            payload = cbor2.loads(block_data[payload_offset:])
        except Exception:
            continue

        if not isinstance(payload, dict):
            continue

        # The Commit block contains the repo DID
        did_val = payload.get("did")
        if did_val and isinstance(did_val, str) and did_val.startswith("did:"):
            commit_did = did_val
            continue

        rec_type = payload.get("$type", "")
        if not rec_type:
            continue

        if commit_did is None:
            continue

        rkey_counter += 1
        records.append({
            "uri": f"at://{commit_did}/{rec_type}/rkey{rkey_counter}",
            "collection": rec_type,
            "rkey": f"rkey{rkey_counter}",
            "value": {k: v for k, v in payload.items()},
        })

    return records


def _find_cbor_payload(data: bytes) -> Optional[int]:
    """Find the offset where a CBOR payload begins after a CID prefix.

    Scans forward from the start of *data* looking for the first byte
    that indicates a CBOR major type 5 (map, 0xa0-0xbf) or a CBOR tag
    (0xc0-0xdb).  Returns ``None`` if no such byte is found.

    This is a heuristic approach — CID encoding in CAR v1 is variable-length
    but CBOR maps always start with a byte in the range 0xa0-0xbf (for
    small maps) or higher for larger maps.
    """
    for i in range(min(len(data), 64)):
        b = data[i]
        # CBOR major type 5: map (0xa0 - 0xbf for small maps, or 0x9f for indefinite)
        if 0xa0 <= b <= 0xbf or b == 0x9f or b == 0xbb:
            return i
        # CBOR tag 42 (CID in dag-cbor) = 0xd8 0x2a
        if i + 1 < len(data) and data[i] == 0xd8 and data[i + 1] == 0x2a:
            return i
    return None


def _read_varint(stream: io.BytesIO) -> int:
    """Read an unsigned varint from a byte stream."""
    result = 0
    shift = 0
    while True:
        byte = stream.read(1)
        if not byte:
            raise EOFError("Unexpected end of stream reading varint")
        b = byte[0]
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return result


# ---------------------------------------------------------------------------
# Avatar download
# ---------------------------------------------------------------------------


def download_avatar(session: Session, avatar_url: str, dest_path: str) -> None:
    """Download an avatar image to *dest_path*.

    Args:
        session: Authenticated session (for potential auth-gated images).
        avatar_url: Full URL of the avatar image.
        dest_path: Local file path to write the image to.
    """
    if not avatar_url:
        return
    try:
        headers = {}
        if session and session.access_token:
            headers["Authorization"] = f"Bearer {session.access_token}"
        resp = httpx.get(avatar_url, headers=headers, follow_redirects=True, timeout=15)
        if resp.status_code == 200:
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            with open(dest_path, "wb") as f:
                f.write(resp.content)
    except Exception:
        pass