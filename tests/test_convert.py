"""Tests for graph format conversion command."""

from __future__ import annotations

import os

import pytest

from skycoll.storage import write_gml, write_gexf


@pytest.fixture
def tmp_dir(tmp_path):
    original = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(original)


def test_convert_gml_to_gexf(tmp_dir):
    from skycoll.commands.convert import run

    write_gml(
        "alice",
        [{"id": "alice", "label": "Alice", "node_type": "self"}],
        [],
    )
    run("alice", to_format="gexf")
    assert os.path.exists("alice.gexf")


def test_convert_gexf_to_gml(tmp_dir):
    from skycoll.commands.convert import run

    write_gexf(
        "alice",
        [
            {
                "id": "did:plc:alice",
                "label": "alice.bsky.social",
                "display_name": "Alice",
                "followers_count": 1,
                "follows_count": 2,
                "node_type": "self",
                "avatar_url": "",
                "backlinks": 0.0,
            }
        ],
        [],
    )
    run("alice", to_format="gml")
    assert os.path.exists("alice.gml")


def test_convert_missing_source_raises(tmp_dir):
    from skycoll.commands.convert import run

    with pytest.raises(RuntimeError, match="Missing source file"):
        run("alice", to_format="gexf")
