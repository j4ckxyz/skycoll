"""edgelist sub-command — generate graph files from .dat/.fdat."""

from __future__ import annotations

import csv
import os

from skycoll.constellation import get_backlink_count
from skycoll.errors import NotFoundError, ParseError, SkycollError
from skycoll.output import info, ok
from skycoll.storage import read_dat, write_gexf, write_gml


def _node_did_lookup(data: dict) -> dict[str, str]:
    """Build handle->DID mapping from .dat data and fdat headers."""
    did_by_handle: dict[str, str] = {}
    profile = data.get("profile") or {}
    if profile.get("handle") and profile.get("did"):
        did_by_handle[profile["handle"]] = profile["did"]

    for person in data.get("follows", []):
        h = person.get("handle", "")
        d = person.get("did", "")
        if h and d:
            did_by_handle[h] = d

    for person in data.get("followers", []):
        h = person.get("handle", "")
        d = person.get("did", "")
        if h and d:
            did_by_handle[h] = d

    return did_by_handle


def _summarize_backlinks(data: dict) -> dict[str, float]:
    """Collapse ``K`` backlink rows into a single numeric score per node.

    Current .dat backlink rows are graph-global, not per-node. We expose this
    total value on the self node (and 0 for others) so it remains available in
    GEXF without inventing fake per-node values.
    """
    total = 0.0
    backlinks = data.get("backlinks", {})
    if isinstance(backlinks, dict):
        for val in backlinks.values():
            if isinstance(val, dict):
                for n in val.values():
                    try:
                        total += float(n)
                    except (TypeError, ValueError):
                        pass
            elif isinstance(val, (int, float)):
                total += float(val)
    return {"self_total": total}


def _read_fdat_rows() -> list[list[str]]:
    rows_out: list[list[str]] = []
    fdat_dir = os.path.join(os.getcwd(), "fdat")
    if not os.path.isdir(fdat_dir):
        return rows_out

    for fname in os.listdir(fdat_dir):
        if not fname.endswith(".dat"):
            continue
        fpath = os.path.join(fdat_dir, fname)
        try:
            with open(fpath, newline="") as f:
                reader = csv.reader(f, delimiter="\t")
                rows = list(reader)
        except Exception:
            continue
        if rows:
            rows_out.extend(rows)
    return rows_out


def run(
    handle: str,
    render: bool = True,
    constellation: str | None = None,
    write_gexf_file: bool = False,
    write_gml_file: bool = True,
) -> None:
    """Generate graph files for *handle* from local data.

    Args:
        handle: The focal user's handle.
        render: Whether to attempt PNG rendering.
        constellation: Optional Constellation host URL.
        write_gexf_file: Whether to also write ``<handle>.gexf``.
        write_gml_file: Whether to write ``<handle>.gml``.
    """
    try:
        if not write_gml_file and not write_gexf_file:
            raise ParseError("no output format selected. Use default GML or pass --gexf")

        data = read_dat(handle)
        if not isinstance(data, dict):
            raise ParseError(f"invalid .dat data for '{handle}': expected an object")
        profile = data.get("profile")
        if not isinstance(profile, dict):
            raise NotFoundError(f"no profile found in {handle}.dat — run `skycoll init {handle}` first")

        ego_handle = profile.get("handle")
        if not ego_handle:
            raise ParseError(f"invalid profile in {handle}.dat: missing handle")
        did_by_handle = _node_did_lookup(data)

        node_map: dict[str, dict] = {}

        def _add_node(
            h: str,
            label: str = "",
            node_type: str = "follow",
            display_name: str = "",
            avatar_url: str = "",
        ) -> None:
            if not h:
                return
            if h not in node_map:
                node_map[h] = {
                    "handle": h,
                    "id": did_by_handle.get(h, h),
                    "label": h,
                    "display_name": display_name or label or h,
                    "followers_count": 0,
                    "follows_count": 0,
                    "node_type": node_type,
                    "avatar_url": avatar_url,
                    "backlinks": 0.0,
                }

        _add_node(
            ego_handle,
            label=ego_handle,
            node_type="self",
            display_name=profile.get("displayName", ego_handle),
            avatar_url=profile.get("avatar", ""),
        )

        ego_follows_set: set[str] = set()
        ego_followers_set: set[str] = set()

        follows = data.get("follows", [])
        followers = data.get("followers", [])
        if not isinstance(follows, list) or not isinstance(followers, list):
            raise ParseError(f"invalid .dat data for '{handle}': follows/followers must be lists")

        for person in follows:
            if not isinstance(person, dict):
                continue
            h = person.get("handle", "")
            if h:
                _add_node(
                    h,
                    label=h,
                    node_type="follow",
                    display_name=person.get("displayName", h),
                    avatar_url=person.get("avatar", ""),
                )
                ego_follows_set.add(h)

        for person in followers:
            if not isinstance(person, dict):
                continue
            h = person.get("handle", "")
            if h:
                _add_node(
                    h,
                    label=h,
                    node_type="follower",
                    display_name=person.get("displayName", h),
                    avatar_url=person.get("avatar", ""),
                )
                ego_followers_set.add(h)

        for followed_handle in ego_follows_set:
            node_map[followed_handle]["follows_count"] = int(node_map[followed_handle].get("follows_count", 0))
        for follower_handle in ego_followers_set:
            node_map[follower_handle]["followers_count"] = int(
                node_map[follower_handle].get("followers_count", 0)
            )

        node_map[ego_handle]["follows_count"] = len(ego_follows_set)
        node_map[ego_handle]["followers_count"] = len(ego_followers_set)

        final_edges: list[tuple[str, str, bool]] = []
        for followed_handle in ego_follows_set:
            mutual = followed_handle in ego_followers_set
            final_edges.append((ego_handle, followed_handle, mutual))
        for follower_handle in ego_followers_set:
            if follower_handle not in ego_follows_set:
                final_edges.append((follower_handle, ego_handle, False))

        fdat_rows = _read_fdat_rows()
        owner_follow_counts: dict[str, int] = {}
        if fdat_rows:
            cur_owner = ""
            for row in fdat_rows:
                if row and row[0] not in ("F", "B", "L", "S", "K"):
                    cur_owner = row[0]
                    owner_display = row[2] if len(row) > 2 else cur_owner
                    owner_avatar = row[3] if len(row) > 3 else ""
                    _add_node(
                        cur_owner,
                        label=cur_owner,
                        node_type=node_map.get(cur_owner, {}).get("node_type", "follow"),
                        display_name=owner_display,
                        avatar_url=owner_avatar,
                    )
                    owner_follow_counts.setdefault(cur_owner, 0)
                    continue
                if len(row) >= 3 and row[0] == "F" and cur_owner:
                    friend_handle = row[1]
                    if friend_handle:
                        friend_display = row[3] if len(row) > 3 and row[3] else friend_handle
                        _add_node(
                            friend_handle,
                            label=friend_handle,
                            node_type=node_map.get(friend_handle, {}).get("node_type", "follow"),
                            display_name=friend_display,
                            avatar_url=row[4] if len(row) > 4 else "",
                        )
                        final_edges.append((cur_owner, friend_handle, False))
                        owner_follow_counts[cur_owner] = owner_follow_counts.get(cur_owner, 0) + 1

        out_count: dict[str, int] = {}
        in_count: dict[str, int] = {}
        for src, tgt, _ in final_edges:
            out_count[src] = out_count.get(src, 0) + 1
            in_count[tgt] = in_count.get(tgt, 0) + 1
        for node_handle, node in node_map.items():
            node["follows_count"] = owner_follow_counts.get(
                node_handle,
                out_count.get(node_handle, int(node.get("follows_count", 0) or 0)),
            )
            node["followers_count"] = in_count.get(node_handle, int(node.get("followers_count", 0) or 0))

        backlinks_summary = _summarize_backlinks(data)
        node_map[ego_handle]["backlinks"] = backlinks_summary.get("self_total", 0.0)

        enriched_edges: list[tuple[str, str, bool, int, int]] | None = None
        if constellation:
            info("Enriching edges with Constellation likes data …")
            enriched_edges = []
            for src, tgt, mutual in final_edges:
                src_did = _resolve_handle_to_did_cached(src)
                likes_given = 0
                likes_received = 0
                if src_did:
                    count = get_backlink_count(
                        constellation,
                        f"at://{src_did}",
                        "app.bsky.feed.like",
                        "/subject",
                    )
                    if count is not None:
                        likes_given = count
                enriched_edges.append((src, tgt, mutual, likes_given, likes_received))

        nodes_for_gml = [
            {
                "id": n["handle"],
                "label": n["display_name"],
                "node_type": n.get("node_type", "follow"),
            }
            for n in node_map.values()
        ]

        if write_gml_file:
            if enriched_edges is not None:
                path = os.path.join(os.getcwd(), f"{handle}.gml")
                with open(path, "w") as f:
                    f.write("graph [\n  directed 1\n")
                    for node in nodes_for_gml:
                        nid = node.get("id", "")
                        label = node.get("label", nid).replace('"', '\\"')
                        node_type = node.get("node_type", "follow")
                        f.write(
                            f'  node [\n    id "{nid}"\n    label "{label}"\n    node_type "{node_type}"\n  ]\n'
                        )
                    for src, tgt, mutual, likes_given, likes_received in enriched_edges:
                        f.write(
                            f'  edge [\n    source "{src}"\n    target "{tgt}"\n'
                            f'    mutual_only {"0" if mutual else "1"}\n'
                            f'    likes_given {likes_given}\n    likes_received {likes_received}\n  ]\n'
                        )
                    f.write("]\n")
                ok(
                    f"Wrote {path} ({len(nodes_for_gml)} nodes, {len(enriched_edges)} edges) [Constellation enriched]"
                )
            else:
                gml_edges = [(src, tgt, not mutual) for src, tgt, mutual in final_edges]
                gml_path = write_gml(handle, nodes_for_gml, gml_edges)
                ok(f"Wrote {gml_path} ({len(nodes_for_gml)} nodes, {len(final_edges)} edges)")

        if write_gexf_file:
            did_by_handle_full = dict(did_by_handle)
            for node_handle in node_map:
                if node_handle not in did_by_handle_full:
                    resolved = _resolve_handle_to_did_cached(node_handle)
                    if resolved:
                        did_by_handle_full[node_handle] = resolved

            nodes_for_gexf = []
            for n in node_map.values():
                did = did_by_handle_full.get(n["handle"], n["id"])
                nodes_for_gexf.append(
                    {
                        "id": did,
                        "label": n["handle"],
                        "display_name": n.get("display_name", n["handle"]),
                        "followers_count": int(n.get("followers_count", 0) or 0),
                        "follows_count": int(n.get("follows_count", 0) or 0),
                        "node_type": n.get("node_type", "follow"),
                        "avatar_url": n.get("avatar_url", ""),
                        "backlinks": float(n.get("backlinks", 0.0) or 0.0),
                    }
                )

            gexf_edges = []
            for src, tgt, mutual in final_edges:
                src_did = did_by_handle_full.get(src, src)
                tgt_did = did_by_handle_full.get(tgt, tgt)
                if not src_did or not tgt_did:
                    continue
                gexf_edges.append({"source": src_did, "target": tgt_did, "mutual": bool(mutual)})

            gexf_path = write_gexf(handle, nodes_for_gexf, gexf_edges)
            ok(f"Wrote {gexf_path} ({len(nodes_for_gexf)} nodes, {len(gexf_edges)} edges)")

        if render and write_gml_file:
            try:
                import igraph
            except ImportError:
                info("python-igraph not installed — skipping PNG rendering.")
                info("Install it with: pip install python-igraph")
                return

            g = igraph.Graph()
            node_ids = [n["id"] for n in nodes_for_gml]
            g.add_vertices(node_ids)
            for v, n in zip(g.vs, nodes_for_gml):
                v["label"] = n.get("label", n["id"])
                v["node_type"] = n.get("node_type", "follow")

            name_to_idx = {name: idx for idx, name in enumerate(g.vs["name"])}
            edge_indices = []
            mutual_flags = []
            for src, tgt, mutual in final_edges:
                if src in name_to_idx and tgt in name_to_idx:
                    edge_indices.append((name_to_idx[src], name_to_idx[tgt]))
                    mutual_flags.append(not mutual)

            g.add_edges(edge_indices)
            g.es["mutual_only"] = mutual_flags

            png_path = os.path.join(os.getcwd(), f"{handle}.png")
            layout = g.layout("fr")
            igraph.plot(g, png_path, layout=layout, bbox=(1200, 900), margin=40)
            ok(f"Wrote {png_path}")
    except SkycollError:
        raise
    except OSError as exc:
        raise ParseError(f"failed to write graph output for '{handle}': {exc}") from exc
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise ParseError(f"invalid graph data for '{handle}': {exc}") from exc
    except Exception as exc:
        raise ParseError(f"unexpected graph error for '{handle}': {exc}") from exc


_did_cache: dict[str, str] = {}


def _resolve_handle_to_did_cached(handle: str) -> str | None:
    """Resolve a handle to a DID, with simple caching."""
    if handle in _did_cache:
        return _did_cache[handle]
    try:
        from skycoll.resolve import resolve_handle_to_did

        did = resolve_handle_to_did(handle)
        _did_cache[handle] = did
        return did
    except Exception:
        return None
