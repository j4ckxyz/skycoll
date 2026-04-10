"""Tests for edgelist command output modes."""

from __future__ import annotations

import os

import pytest

from skycoll.storage import write_dat, read_gexf


@pytest.fixture
def tmp_dir(tmp_path):
    original = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(original)


def _seed_dat(handle: str) -> None:
    profile = {
        "did": "did:plc:self",
        "displayName": "Self",
        "avatar": "https://img.example.com/self.jpg",
        "description": "",
        "labels": [],
    }
    follows = [
        {
            "handle": "bob.bsky.social",
            "did": "did:plc:bob",
            "displayName": "Bob",
            "avatar": "",
        }
    ]
    followers = [
        {
            "handle": "carol.bsky.social",
            "did": "did:plc:carol",
            "displayName": "Carol",
            "avatar": "",
        }
    ]
    write_dat(handle, profile, follows, followers)


def test_edgelist_writes_gexf_only(tmp_dir):
    from skycoll.commands.edgelist import run

    _seed_dat("alice")
    run("alice", render=False, write_gexf_file=True, write_gml_file=False)

    assert os.path.exists("alice.gexf")
    assert not os.path.exists("alice.gml")

    nodes, edges = read_gexf("alice.gexf")
    assert len(nodes) >= 3
    assert len(edges) >= 2


def test_edgelist_writes_both_gml_and_gexf(tmp_dir):
    from skycoll.commands.edgelist import run

    _seed_dat("alice")
    run("alice", render=False, write_gexf_file=True, write_gml_file=True)

    assert os.path.exists("alice.gml")
    assert os.path.exists("alice.gexf")


def test_edgelist_requires_at_least_one_format(tmp_dir):
    from skycoll.commands.edgelist import run

    _seed_dat("alice")
    with pytest.raises(RuntimeError, match="No output format selected"):
        run("alice", render=False, write_gexf_file=False, write_gml_file=False)
