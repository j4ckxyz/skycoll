"""File I/O for twecoll-compatible data files.

File formats:

  ``<handle>.dat`` — Social-graph data for a single user.
    Line format (tab-separated):
      Header: ``handle<TAB>did<TAB>display_name<TAB>avatar_url<TAB>description<TAB>labels<TAB>lists``
      Follow (``F`` prefix): ``F<TAB>handle<TAB>did<TAB>display_name<TAB>avatar_url<...>``
      Follower (``B`` prefix): ``B<TAB>handle<TAB>did<TAB>display_name<TAB>avatar_url<...>``
      List (``L`` prefix): ``L<TAB>uri<TAB>name<TAB>purpose``
      Starter pack (``S`` prefix): ``S<TAB>uri<TAB>name<TAB>item_count``

  ``fdat/<handle>.dat`` — Same format, one file per followed user (populated by ``fetch``).

  ``<handle>.twt`` — Posts/reposts/quotes (tab-separated):
    ``type<TAB>uri<TAB>timestamp<TAB>reply_to_uri<TAB>root_uri<TAB>text``

  ``<handle>.fav`` — Likes (tab-separated):
    ``uri<TAB>timestamp<TAB>author_did<TAB>author_handle<TAB>text``

  ``<handle>.threads`` — Reconstructed threads (JSON array of thread trees).

  ``img/<handle>`` — Avatar image for each user.

  ``<handle>.gml`` — GML graph (produced by ``edgelist``).

  ``<handle>.car`` — Raw CAR archive (produced by ``sync``).
"""

from __future__ import annotations

import csv
import json
import os
from typing import Optional


def _base_dir() -> str:
    """Return the current working directory (where data files are written)."""
    return os.getcwd()


# ---------------------------------------------------------------------------
# .dat files
# ---------------------------------------------------------------------------


def write_dat(
    handle: str,
    profile: dict,
    follows: list[dict],
    followers: list[dict],
    lists: Optional[list[dict]] = None,
    starter_packs: Optional[list[dict]] = None,
    backlinks: Optional[dict] = None,
) -> str:
    """Write a ``.dat`` file for *handle*.

    The header line stores the profile, followed by ``F``/``B``/``L``/``S``/``K`` prefixed
    rows for follows, followers, lists, starter packs, and backlinks respectively.

    Args:
        handle: The user's handle (used as file name).
        profile: Profile dict from ``app.bsky.actor.getProfile``.
        follows: List of follow dicts.
        followers: List of follower dicts.
        lists: Optional list of list dicts (``L`` rows).
        starter_packs: Optional list of starter-pack dicts (``S`` rows).
        backlinks: Optional backlink counts dict from Constellation (``K`` rows).

    Returns:
        Path to the written file.
    """
    path = os.path.join(_base_dir(), f"{handle}.dat")

    # Extract labels from profile
    labels_str = ""
    if profile.get("labels"):
        label_vals = []
        for lbl in profile["labels"]:
            if isinstance(lbl, dict):
                label_vals.append(lbl.get("val", ""))
            else:
                label_vals.append(str(lbl))
        labels_str = "|".join(label_vals)

    # Extract self-labels from the profile join
    self_labels = profile.get("selfLabels", [])
    if self_labels:
        sl_vals = [lbl.get("val", "") if isinstance(lbl, dict) else str(lbl) for lbl in self_labels]
        if labels_str:
            labels_str += "|" + "|".join(sl_vals)
        else:
            labels_str = "|".join(sl_vals)

    with open(path, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow([
            handle,
            profile.get("did", ""),
            profile.get("displayName", ""),
            profile.get("avatar", ""),
            profile.get("description", ""),
            labels_str,
        ])
        for follow in follows:
            writer.writerow([
                "F",
                follow.get("handle", ""),
                follow.get("did", ""),
                follow.get("displayName", follow.get("handle", "")),
                follow.get("avatar", ""),
                "",
            ])
        for follower in followers:
            writer.writerow([
                "B",
                follower.get("handle", ""),
                follower.get("did", ""),
                follower.get("displayName", follower.get("handle", "")),
                follower.get("avatar", ""),
                "",
            ])
        for lst in (lists or []):
            writer.writerow([
                "L",
                lst.get("uri", ""),
                lst.get("name", ""),
                lst.get("purpose", ""),
            ])
        for sp in (starter_packs or []):
            writer.writerow([
                "S",
                sp.get("uri", ""),
                sp.get("name", sp.get("record", {}).get("name", "")),
                str(sp.get("listItemCount", sp.get("listItemCount", 0))),
            ])
        if backlinks and isinstance(backlinks, dict):
            for collection, paths in backlinks.items():
                if isinstance(paths, dict):
                    for path_key, count in paths.items():
                        writer.writerow([
                            "K",
                            collection,
                            path_key,
                            str(count) if count is not None else "0",
                        ])
                elif isinstance(paths, (int, float)):
                    writer.writerow(["K", collection, "", str(int(paths))])
    return path


def read_dat(handle: str) -> dict:
    """Read a ``.dat`` file for *handle*.

    Returns:
        Dict with keys ``profile``, ``follows``, ``followers``, ``lists``, ``starter_packs``, ``backlinks``.
    """
    path = os.path.join(_base_dir(), f"{handle}.dat")
    profile: Optional[dict] = None
    follows: list[dict] = []
    followers: list[dict] = []
    lists: list[dict] = []
    starter_packs: list[dict] = []
    backlinks: dict[str, dict[str, int]] = {}

    try:
        with open(path, newline="") as f:
            reader = csv.reader(f, delimiter="\t")
            for i, row in enumerate(reader):
                if i == 0:
                    profile = {
                        "handle": row[0],
                        "did": row[1],
                        "displayName": row[2],
                        "avatar": row[3],
                        "description": row[4] if len(row) > 4 else "",
                        "labels": row[5] if len(row) > 5 else "",
                    }
                elif len(row) >= 2:
                    prefix = row[0]
                    if prefix == "F" and len(row) >= 3:
                        follows.append({
                            "handle": row[1],
                            "did": row[2],
                            "displayName": row[3] if len(row) > 3 else row[1],
                            "avatar": row[4] if len(row) > 4 else "",
                        })
                    elif prefix == "B" and len(row) >= 3:
                        followers.append({
                            "handle": row[1],
                            "did": row[2],
                            "displayName": row[3] if len(row) > 3 else row[1],
                            "avatar": row[4] if len(row) > 4 else "",
                        })
                    elif prefix == "L" and len(row) >= 3:
                        lists.append({
                            "uri": row[1],
                            "name": row[2],
                            "purpose": row[3] if len(row) > 3 else "",
                        })
                    elif prefix == "S" and len(row) >= 3:
                        starter_packs.append({
                            "uri": row[1],
                            "name": row[2],
                            "item_count": int(row[3]) if len(row) > 3 and row[3].isdigit() else 0,
                        })
                    elif prefix == "K" and len(row) >= 3:
                        collection = row[1]
                        path_key = row[2] if len(row) > 2 else ""
                        count = int(row[3]) if len(row) > 3 and row[3].isdigit() else 0
                        backlinks.setdefault(collection, {})[path_key] = count
    except FileNotFoundError:
        raise FileNotFoundError(path)

    return {
        "profile": profile,
        "follows": follows,
        "followers": followers,
        "lists": lists,
        "starter_packs": starter_packs,
        "backlinks": backlinks,
    }


# ---------------------------------------------------------------------------
# fdat/ files
# ---------------------------------------------------------------------------


def write_fdat(handle: str, profile: dict, follows: list[dict]) -> str:
    """Write a ``fdat/<handle>.dat`` file for a followed user.

    Same format as ``.dat`` but without followers (only profile + follows of
    the followed user).

    Args:
        handle: The followed user's handle.
        profile: Their profile dict.
        follows: Their follows list.

    Returns:
        Path to the written file.
    """
    fdat_dir = os.path.join(_base_dir(), "fdat")
    os.makedirs(fdat_dir, exist_ok=True)
    path = os.path.join(fdat_dir, f"{handle}.dat")
    with open(path, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow([
            handle,
            profile.get("did", ""),
            profile.get("displayName", ""),
            profile.get("avatar", ""),
            profile.get("description", ""),
            "",
        ])
        for follow in follows:
            writer.writerow([
                "F",
                follow.get("handle", ""),
                follow.get("did", ""),
                follow.get("displayName", follow.get("handle", "")),
                follow.get("avatar", ""),
                "",
            ])
    return path


# ---------------------------------------------------------------------------
# .twt files  (richer format)
# ---------------------------------------------------------------------------


def write_twt(handle: str, posts: list[dict]) -> str:
    """Write a ``.twt`` file (posts/reposts/quotes) for *handle*.

    Tab-separated columns: ``type\\turi\\ttimestamp\\treply_to_uri\\troot_uri\\ttext``

    *type* is one of ``post``, ``repost``, or ``quote``.

    Args:
        handle: The user's handle.
        posts: List of record dicts. Each dict must contain at minimum
            ``uri``, ``collection``, and ``value`` keys (as returned by
            CAR parsing or listRecords).

    Returns:
        Path to the written file.
    """
    path = os.path.join(_base_dir(), f"{handle}.twt")
    with open(path, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        for post in posts:
            uri = post.get("uri", "")
            collection = post.get("collection", "")
            value = post.get("value", {})

            if collection == "app.bsky.feed.post":
                rec_type = "post"
                text = value.get("text", "")
                reply = value.get("reply", {})
                reply_to_uri = reply.get("parent", {}).get("uri", "") if reply else ""
                root_uri = reply.get("root", {}).get("uri", "") if reply else ""
                # Quote posts embed another post
                embed = value.get("embed", {})
                if embed.get("$type") == "app.bsky.embed.record":
                    rec_type = "quote"
            elif collection == "app.bsky.feed.repost":
                rec_type = "repost"
                text = ""
                subject = value.get("subject", {})
                reply_to_uri = subject.get("uri", "")
                root_uri = ""
            else:
                continue

            created_at = value.get("createdAt", "")
            text = text.replace("\n", "\\n").replace("\r", "")
            writer.writerow([rec_type, uri, created_at, reply_to_uri, root_uri, text])
    return path


def read_twt(handle: str) -> list[dict]:
    """Read a ``.twt`` file for *handle*.

    Returns:
        List of dicts with keys ``type``, ``uri``, ``timestamp``,
        ``reply_to_uri``, ``root_uri``, ``text``.
    """
    path = os.path.join(_base_dir(), f"{handle}.twt")
    posts: list[dict] = []
    with open(path, newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            if len(row) >= 6:
                posts.append({
                    "type": row[0],
                    "uri": row[1],
                    "timestamp": row[2],
                    "reply_to_uri": row[3],
                    "root_uri": row[4],
                    "text": row[5],
                })
            elif len(row) >= 2:
                posts.append({
                    "type": "post",
                    "uri": row[1] if len(row) > 1 else "",
                    "timestamp": row[0],
                    "reply_to_uri": "",
                    "root_uri": "",
                    "text": row[2] if len(row) > 2 else "",
                })
    return posts


# ---------------------------------------------------------------------------
# .fav files
# ---------------------------------------------------------------------------


def write_fav(handle: str, likes: list[dict]) -> str:
    """Write a ``.fav`` file (likes) for *handle*.

    Tab-separated columns: ``uri\\ttimestamp\\tauthor_did\\tauthor_handle\\ttext``

    Args:
        handle: The user's handle.
        likes: List of like record dicts.

    Returns:
        Path to the written file.
    """
    path = os.path.join(_base_dir(), f"{handle}.fav")
    with open(path, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        for like in likes:
            value = like.get("value", {})
            subject = value.get("subject", {})
            uri = subject.get("uri", "")
            created_at = value.get("createdAt", "")
            author_did = ""
            if uri.startswith("at://"):
                parts = uri.split("/")
                if len(parts) >= 3:
                    author_did = parts[2]
            text = subject.get("text", "")
            text = text.replace("\n", "\\n").replace("\r", "")
            writer.writerow([uri, created_at, author_did, "", text])
    return path


# ---------------------------------------------------------------------------
# .threads files
# ---------------------------------------------------------------------------


def write_threads(handle: str, threads: list[dict]) -> str:
    """Write a ``.threads`` file (JSON array of thread trees).

    Each thread is a dict with ``root`` (a post dict) and ``replies``
    (a nested list of post dicts forming a tree).

    Args:
        handle: The user's handle.
        threads: List of thread dicts.

    Returns:
        Path to the written file.
    """
    path = os.path.join(_base_dir(), f"{handle}.threads")
    with open(path, "w") as f:
        json.dump(threads, f, indent=2, ensure_ascii=False)
    return path


def read_threads(handle: str) -> list[dict]:
    """Read a ``.threads`` file for *handle*.

    Returns:
        List of thread dicts.
    """
    path = os.path.join(_base_dir(), f"{handle}.threads")
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# .car files
# ---------------------------------------------------------------------------


def write_car(handle: str, data: bytes) -> str:
    """Write raw CAR bytes to ``<handle>.car``.

    Args:
        handle: The user's handle.
        data: Raw CAR file bytes.

    Returns:
        Path to the written file.
    """
    path = os.path.join(_base_dir(), f"{handle}.car")
    with open(path, "wb") as f:
        f.write(data)
    return path


# ---------------------------------------------------------------------------
# img/
# ---------------------------------------------------------------------------


def avatar_path(handle: str) -> str:
    """Return the local path where *handle*'s avatar should be stored."""
    img_dir = os.path.join(_base_dir(), "img")
    os.makedirs(img_dir, exist_ok=True)
    return os.path.join(img_dir, handle)


# ---------------------------------------------------------------------------
# .gml files  (with mutual_only edge property)
# ---------------------------------------------------------------------------


def write_gml(
    handle: str,
    nodes: list[dict],
    edges: list[tuple[str, str, bool]],
) -> str:
    """Write a GML file describing the social graph around *handle*.

    Args:
        handle: The focal user's handle.
        nodes: List of dicts with at least ``id`` (handle), ``label``, and
            optional ``node_type`` keys.
        edges: List of ``(source, target, mutual_only)`` tuples.

    Returns:
        Path to the written file.
    """
    path = os.path.join(_base_dir(), f"{handle}.gml")
    with open(path, "w") as f:
        f.write("graph [\n  directed 1\n")
        for node in nodes:
            nid = node.get("id", "")
            label = node.get("label", nid).replace('"', '\\"')
            node_type = node.get("node_type", "person")
            f.write(f'  node [\n    id "{nid}"\n    label "{label}"\n    node_type "{node_type}"\n  ]\n')
        for src, tgt, mutual in edges:
            f.write(
                f'  edge [\n    source "{src}"\n    target "{tgt}"\n    mutual_only {"1" if mutual else "0"}\n  ]\n'
            )
        f.write("]\n")
    return path
