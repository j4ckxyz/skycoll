"""convert sub-command — convert existing graph files between GML and GEXF."""

from __future__ import annotations

import os

from skycoll.storage import read_gexf, read_gml, write_gexf, write_gml


def run(handle: str, to_format: str) -> None:
    """Convert ``<handle>.gml`` <-> ``<handle>.gexf``.

    Args:
        handle: Handle basename used for file names.
        to_format: Target format, ``"gml"`` or ``"gexf"``.
    """
    to_format = to_format.lower()
    if to_format not in ("gml", "gexf"):
        raise RuntimeError(f"Unsupported conversion format: {to_format}")

    src_gml = os.path.join(os.getcwd(), f"{handle}.gml")
    src_gexf = os.path.join(os.getcwd(), f"{handle}.gexf")

    if to_format == "gexf":
        if not os.path.exists(src_gml):
            raise RuntimeError(f"Missing source file: {src_gml}")
        nodes, edges = read_gml(src_gml)
        path = write_gexf(handle, nodes, edges)
        print(f"Converted {src_gml} -> {path}")
        return

    if not os.path.exists(src_gexf):
        raise RuntimeError(f"Missing source file: {src_gexf}")

    nodes, edges = read_gexf(src_gexf)

    # Map GEXF node ids (DIDs) to labels (handles) for GML readability.
    id_to_handle = {n.get("id", ""): n.get("label", n.get("id", "")) for n in nodes}
    gml_nodes = [
        {
            "id": id_to_handle.get(n.get("id", ""), n.get("id", "")),
            "label": n.get("display_name", n.get("label", "")),
            "node_type": n.get("node_type", "follow"),
        }
        for n in nodes
    ]
    gml_edges = []
    for e in edges:
        src = id_to_handle.get(e.get("source", ""), e.get("source", ""))
        tgt = id_to_handle.get(e.get("target", ""), e.get("target", ""))
        mutual = bool(e.get("mutual", False))
        gml_edges.append((src, tgt, not mutual))

    path = write_gml(handle, gml_nodes, gml_edges)
    print(f"Converted {src_gexf} -> {path}")
