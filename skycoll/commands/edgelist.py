"""edgelist sub-command — generate .gml from .dat/.fdat; optionally render .png.

Builds a bidirectional social graph using both getFollows and getFollowers.
Each edge carries a ``mutual_only`` attribute: ``1`` if the follow is
one-directional, ``0`` if the relationship is mutual (both people follow
each other).  Starter packs from ``.dat`` are included as node-type
``starter_pack`` nodes.
"""

from __future__ import annotations

import csv
import os

from skycoll.storage import read_dat, write_gml


def run(handle: str, render: bool = True) -> None:
    """Generate a GML graph file for *handle* from local data.

    Reads ``<handle>.dat`` and ``fdat/*.dat`` to build the ego-network
    graph, then writes ``<handle>.gml``.  If *render* is ``True`` and
    ``python-igraph`` is installed, also produces ``<handle>.png``.

    Edges include a ``mutual_only`` attribute: 0 if the follow is mutual
    (both A→B and B→A), 1 if one-directional.

    Starter packs from the ``.dat`` file are added as graph nodes with
    ``node_type "starter_pack"``.

    Args:
        handle: The focal user's handle.
        render: Whether to attempt PNG rendering (default ``True``).
    """
    data = read_dat(handle)
    profile = data["profile"]
    if profile is None:
        raise RuntimeError(f"No profile found in {handle}.dat — run `skycoll init {handle}` first")

    ego_handle = profile["handle"]

    # Build a set of who follows whom (directed edges ego sees)
    # We use both follows and followers from .dat to determine mutual edges
    node_map: dict[str, dict] = {}
    edge_set: dict[tuple[str, str], bool] = {}

    def _add_node(h: str, label: str = "", node_type: str = "person") -> None:
        if h and h not in node_map:
            node_map[h] = {"id": h, "label": label or h, "node_type": node_type}

    # Focal user
    _add_node(ego_handle, profile.get("displayName", ego_handle))

    # Follows set (for mutual detection)
    ego_follows_set: set[str] = set()
    ego_followers_set: set[str] = set()

    # Edges from ego to follows
    for person in data["follows"]:
        h = person.get("handle", "")
        if h:
            _add_node(h, person.get("displayName", h))
            ego_follows_set.add(h)
            edge_set[(ego_handle, h)] = False  # will be updated for mutual

    # Edges from followers to ego
    for person in data["followers"]:
        h = person.get("handle", "")
        if h:
            _add_node(h, person.get("displayName", h))
            ego_followers_set.add(h)
            edge_set[(h, ego_handle)] = False

    # Mark mutual edges for ego's direct connections
    final_edges: list[tuple[str, str, bool]] = []

    # Ego → follow is mutual if the person also follows ego
    for h in ego_follows_set:
        mutual = h in ego_followers_set
        final_edges.append((ego_handle, h, not mutual))

    # Follower → ego is mutual if ego also follows them (already covered above,
    # but we add the edge direction from follower to ego)
    for h in ego_followers_set:
        if h not in ego_follows_set:
            final_edges.append((h, ego_handle, True))

    # Starter packs as nodes
    for sp in data.get("starter_packs", []):
        sp_uri = sp.get("uri", "")
        sp_name = sp.get("name", "Starter Pack")
        if sp_uri:
            _add_node(sp_uri, sp_name, "starter_pack")
            # Edge from ego to starter pack
            final_edges.append((ego_handle, sp_uri, True))

    # Read fdat/ files for extended network
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

            # First row = profile of this followed user
            fhandle = rows[0][0]
            _add_node(fhandle, rows[0][2] if len(rows[0]) > 2 else fhandle)

            # Their follows
            f_follows_set: set[str] = set()
            for row in rows[1:]:
                if len(row) >= 3 and row[0] == "F":
                    friend_handle = row[1]
                    if friend_handle:
                        _add_node(friend_handle, row[3] if len(row) > 3 and row[3] else friend_handle)
                        f_follows_set.add(friend_handle)
                        edge_set[(fhandle, friend_handle)] = False

            # Check mutual within fdat network
            for friend in f_follows_set:
                if (friend, fhandle) in edge_set:
                    # Both directions exist → not one-sided
                    final_edges.append((fhandle, friend, False))
                else:
                    final_edges.append((fhandle, friend, True))

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
        node_ids = [n["id"] for n in nodes]
        g.add_vertices(node_ids)
        for v, n in zip(g.vs, nodes):
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
        visual_style = {
            "layout": layout,
            "bbox": (1200, 900),
            "margin": 40,
            "vertex_label": g.vs["label"],
        }
        igraph.plot(g, png_path, **visual_style)
        print(f"Wrote {png_path}")