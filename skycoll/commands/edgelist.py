"""edgelist sub-command — generate .gml from .dat/.fdat; optionally render .png.

Builds a bidirectional social graph using both getFollows and getFollowers.
Each edge carries a ``mutual_only`` attribute.  If ``--constellation`` is
provided, edges are further annotated with likes_given / likes_received
counts from the Constellation backlinks index.
"""

from __future__ import annotations

import csv
import os

from skycoll.constellation import get_backlink_count
from skycoll.storage import read_dat, write_gml


def run(
    handle: str,
    render: bool = True,
    constellation: str | None = None,
) -> None:
    """Generate a GML graph file for *handle* from local data.

    Args:
        handle: The focal user's handle.
        render: Whether to attempt PNG rendering (default ``True``).
        constellation: Optional Constellation host URL for likes enrichment.
    """
    data = read_dat(handle)
    profile = data["profile"]
    if profile is None:
        raise RuntimeError(f"No profile found in {handle}.dat — run `skycoll init {handle}` first")

    ego_handle = profile["handle"]

    node_map: dict[str, dict] = {}
    edge_set: dict[tuple[str, str], bool] = {}

    def _add_node(h: str, label: str = "", node_type: str = "person") -> None:
        if h and h not in node_map:
            node_map[h] = {"id": h, "label": label or h, "node_type": node_type}

    _add_node(ego_handle, profile.get("displayName", ego_handle))

    ego_follows_set: set[str] = set()
    ego_followers_set: set[str] = set()

    for person in data["follows"]:
        h = person.get("handle", "")
        if h:
            _add_node(h, person.get("displayName", h))
            ego_follows_set.add(h)

    for person in data["followers"]:
        h = person.get("handle", "")
        if h:
            _add_node(h, person.get("displayName", h))
            ego_followers_set.add(h)

    final_edges: list[tuple[str, str, bool]] = []

    for h in ego_follows_set:
        mutual = h in ego_followers_set
        final_edges.append((ego_handle, h, not mutual))

    for h in ego_followers_set:
        if h not in ego_follows_set:
            final_edges.append((h, ego_handle, True))

    # Starter packs as nodes
    for sp in data.get("starter_packs", []):
        sp_uri = sp.get("uri", "")
        sp_name = sp.get("name", "Starter Pack")
        if sp_uri:
            _add_node(sp_uri, sp_name, "starter_pack")
            final_edges.append((ego_handle, sp_uri, True))

    # fdat/ extended network
    fdat_dir = os.path.join(os.getcwd(), "fdat")
    if os.path.isdir(fdat_dir):
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
            if not rows:
                continue
            fhandle = rows[0][0]
            _add_node(fhandle, rows[0][2] if len(rows[0]) > 2 else fhandle)
            for row in rows[1:]:
                if len(row) >= 3 and row[0] == "F":
                    friend_handle = row[1]
                    if friend_handle:
                        _add_node(friend_handle, row[3] if len(row) > 3 and row[3] else friend_handle)
                        final_edges.append((fhandle, friend_handle, True))

    # Constellation enrichment: annotate edges with likes counts
    if constellation:
        print("Enriching edges with Constellation likes data …")
        enriched_edges: list[tuple[str, str, bool, int, int]] = []
        for src, tgt, mutual in final_edges:
            src_did = _resolve_handle_to_did_cached(src)
            tgt_did = _resolve_handle_to_did_cached(tgt)
            likes_given = 0
            likes_received = 0
            if src_did:
                count = get_backlink_count(
                    constellation, f"at://{src_did}", "app.bsky.feed.like", "/subject"
                )
                if count is not None:
                    likes_given = count
            enriched_edges.append((src, tgt, mutual, likes_given, likes_received))
        # Write enriched GML
        nodes = list(node_map.values())
        path = os.path.join(os.getcwd(), f"{handle}.gml")
        with open(path, "w") as f:
            f.write("graph [\n  directed 1\n")
            for node in nodes:
                nid = node.get("id", "")
                label = node.get("label", nid).replace('"', '\\"')
                node_type = node.get("node_type", "person")
                f.write(f'  node [\n    id "{nid}"\n    label "{label}"\n    node_type "{node_type}"\n  ]\n')
            for edge in enriched_edges:
                src, tgt, mutual, likes_given, likes_received = edge
                f.write(
                    f'  edge [\n    source "{src}"\n    target "{tgt}"\n'
                    f'    mutual_only {"1" if mutual else "0"}\n'
                    f'    likes_given {likes_given}\n    likes_received {likes_received}\n  ]\n'
                )
            f.write("]\n")
        print(f"Wrote {path} ({len(nodes)} nodes, {len(enriched_edges)} edges) [Constellation enriched]")
    else:
        nodes = list(node_map.values())
        gml_path = write_gml(handle, nodes, final_edges)
        print(f"Wrote {gml_path} ({len(nodes)} nodes, {len(final_edges)} edges)")

    if render:
        try:
            import igraph
        except ImportError:
            print("python-igraph not installed — skipping PNG rendering.")
            print("Install it with: pip install python-igraph")
            return

        g = igraph.Graph()
        node_ids = [n["id"] for n in node_map.values()]
        g.add_vertices(node_ids)
        for v, n in zip(g.vs, node_map.values()):
            v["label"] = n.get("label", n["id"])
            v["node_type"] = n.get("node_type", "person")

        name_to_idx = {name: idx for idx, name in enumerate(g.vs["name"])}
        edge_indices = []
        mutual_flags = []
        for src, tgt, mutual in final_edges:
            if src in name_to_idx and tgt in name_to_idx:
                edge_indices.append((name_to_idx[src], name_to_idx[tgt]))
                mutual_flags.append(mutual)

        g.add_edges(edge_indices)
        g.es["mutual_only"] = mutual_flags

        png_path = os.path.join(os.getcwd(), f"{handle}.png")
        layout = g.layout("fr")
        igraph.plot(g, png_path, layout=layout, bbox=(1200, 900), margin=40)
        print(f"Wrote {png_path}")


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
    except RuntimeError:
        return None