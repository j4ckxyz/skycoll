"""threads sub-command — reconstruct reply threads from .twt data.

Reads an existing ``<handle>.twt`` file and builds thread trees using the
``reply_to_uri`` and ``root_uri`` fields.  Outputs a ``<handle>.threads``
file as a JSON array of thread trees, each with a ``root`` post and nested
``replies``.
"""

from __future__ import annotations

from skycoll.storage import read_twt, write_threads


def _build_threads(posts: list[dict]) -> list[dict]:
    """Build thread trees from a flat list of posts.

    Only posts of type ``post`` (not ``repost``) that have a ``root_uri``
    or ``reply_to_uri`` are organised into threads.  Root posts (those
    without a ``reply_to_uri``) form the top of each thread.

    Args:
        posts: List of post dicts from ``read_twt``.

    Returns:
        List of thread dicts, each with ``root`` and ``replies`` keys.
    """
    by_uri: dict[str, dict] = {}
    for p in posts:
        if p.get("type") != "repost":
            uri = p.get("uri", "")
            if uri:
                by_uri[uri] = p

    children: dict[str, list[str]] = {}
    roots: list[str] = []

    for uri, p in by_uri.items():
        reply_to = p.get("reply_to_uri", "")
        if reply_to and reply_to in by_uri:
            children.setdefault(reply_to, []).append(uri)
        else:
            roots.append(uri)

    def _build_tree(uri: str, depth: int = 0) -> dict:
        post = by_uri[uri]
        replies = []
        for child_uri in children.get(uri, []):
            if depth < 50:
                replies.append(_build_tree(child_uri, depth + 1))
        return {
            "uri": uri,
            "timestamp": post.get("timestamp", ""),
            "text": post.get("text", ""),
            "root_uri": post.get("root_uri", ""),
            "reply_to_uri": post.get("reply_to_uri", ""),
            "replies": replies,
        }

    threads: list[dict] = []
    for root_uri in roots:
        threads.append(_build_tree(root_uri))

    return threads


def run(handle: str) -> None:
    """Reconstruct reply threads from ``<handle>.twt`` and write ``<handle>.threads``.

    Args:
        handle: The user's handle (used to find and write files).
    """
    print(f"Reading {handle}.twt …")
    posts = read_twt(handle)
    print(f"  {len(posts)} entries loaded")

    # Filter to actual posts (not reposts) that could be in threads
    post_entries = [p for p in posts if p.get("type") in ("post", "quote")]
    print(f"  {len(post_entries)} posts/quotes")

    threads = _build_threads(post_entries)
    print(f"  {len(threads)} threads reconstructed")

    path = write_threads(handle, threads)
    print(f"Wrote {path}")