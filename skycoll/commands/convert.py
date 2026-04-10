"""convert sub-command — convert existing graph files between GML and GEXF."""

from __future__ import annotations

import os

from skycoll.errors import NotFoundError, ParseError, SkycollError
from skycoll.output import ok
from skycoll.storage import read_gexf, read_gml, write_gexf, write_gml


def run(handle: str, to_format: str) -> None:
    """Convert ``<handle>.gml`` <-> ``<handle>.gexf``.

    Args:
        handle: Handle basename used for file names.
        to_format: Target format, ``"gml"`` or ``"gexf"``.
    """
    try:
        to_format = to_format.lower()
        if to_format not in ("gml", "gexf"):
            raise ParseError(f"unsupported conversion format: {to_format}")

        src_gml = os.path.join(os.getcwd(), f"{handle}.gml")
        src_gexf = os.path.join(os.getcwd(), f"{handle}.gexf")

        if to_format == "gexf":
            if not os.path.exists(src_gml):
                raise NotFoundError(f"missing source file: {src_gml}")
            nodes, edges = read_gml(src_gml)
            path = write_gexf(handle, nodes, edges)
            ok(f"Converted {src_gml} -> {path}")
            return

        if not os.path.exists(src_gexf):
            raise NotFoundError(f"missing source file: {src_gexf}")

        nodes, edges = read_gexf(src_gexf)
        if not isinstance(nodes, list) or not isinstance(edges, list):
            raise ParseError(f"invalid graph payload while converting '{handle}'")

        id_to_handle = {n.get("id", ""): n.get("label", n.get("id", "")) for n in nodes if isinstance(n, dict)}
        gml_nodes = [
            {
                "id": id_to_handle.get(n.get("id", ""), n.get("id", "")),
                "label": n.get("display_name", n.get("label", "")),
                "node_type": n.get("node_type", "follow"),
            }
            for n in nodes
            if isinstance(n, dict)
        ]
        gml_edges = []
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            src = id_to_handle.get(edge.get("source", ""), edge.get("source", ""))
            tgt = id_to_handle.get(edge.get("target", ""), edge.get("target", ""))
            mutual = bool(edge.get("mutual", False))
            gml_edges.append((src, tgt, not mutual))

        path = write_gml(handle, gml_nodes, gml_edges)
        ok(f"Converted {src_gexf} -> {path}")
    except SkycollError:
        raise
    except OSError as exc:
        raise ParseError(f"failed to write converted graph for '{handle}': {exc}") from exc
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid conversion data for '{handle}': {exc}") from exc
    except Exception as exc:
        raise ParseError(f"unexpected conversion error for '{handle}': {exc}") from exc
