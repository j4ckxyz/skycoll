"""Tests for skycoll.storage — file I/O round-trips."""

from __future__ import annotations

import json
import os
import tempfile

import pytest

from skycoll.storage import (
    write_dat,
    read_dat,
    write_twt,
    read_twt,
    write_fav,
    write_threads,
    read_threads,
    write_car,
    write_gml,
    write_gexf,
    read_gml,
    read_gexf,
    write_fdat,
    avatar_path,
)


@pytest.fixture
def tmp_dir(tmp_path):
    """Set CWD to tmp_path for file operations."""
    original = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(original)


class TestDatRoundTrip:
    """Round-trip tests for .dat file write/read."""

    def test_basic_round_trip(self, tmp_dir):
        profile = {
            "did": "did:plc:abc123",
            "displayName": "Alice",
            "avatar": "https://img.example.com/alice.jpg",
            "description": "Hello world",
            "labels": [],
        }
        follows = [
            {"handle": "bob.bsky.social", "did": "did:plc:bob", "displayName": "Bob", "avatar": ""},
        ]
        followers = [
            {"handle": "carol.bsky.social", "did": "did:plc:carol", "displayName": "Carol", "avatar": ""},
        ]

        path = write_dat("alice", profile, follows, followers)
        assert os.path.exists(path)

        data = read_dat("alice")
        assert data["profile"]["handle"] == "alice"
        assert data["profile"]["did"] == "did:plc:abc123"
        assert data["profile"]["displayName"] == "Alice"
        assert len(data["follows"]) == 1
        assert data["follows"][0]["handle"] == "bob.bsky.social"
        assert len(data["followers"]) == 1
        assert data["followers"][0]["handle"] == "carol.bsky.social"

    def test_with_lists_and_starter_packs(self, tmp_dir):
        profile = {
            "did": "did:plc:abc",
            "displayName": "Test",
            "avatar": "",
            "description": "",
            "labels": [],
        }
        lists = [{"uri": "at://did:plc:abc/app.bsky.graph.list/1", "name": "My List", "purpose": "app.bsky.graph.defs#curatelist"}]
        starter_packs = [{"uri": "at://did:plc:abc/app.bsky.graph.starterpack/1", "name": "Starter", "listItemCount": 5}]

        path = write_dat("test", profile, [], [], lists=lists, starter_packs=starter_packs)
        data = read_dat("test")
        assert len(data["lists"]) == 1
        assert data["lists"][0]["name"] == "My List"
        assert len(data["starter_packs"]) == 1
        assert data["starter_packs"][0]["name"] == "Starter"

    def test_with_labels(self, tmp_dir):
        profile = {
            "did": "did:plc:abc",
            "displayName": "Test",
            "avatar": "",
            "description": "",
            "labels": [{"val": "adult"}, {"val": "spam"}],
        }
        path = write_dat("labeled", profile, [], [])
        data = read_dat("labeled")
        labels = data["profile"]["labels"]
        assert "adult" in labels
        assert "spam" in labels


class TestTwtRoundTrip:
    """Round-trip tests for the richer .twt format."""

    def test_round_trip(self, tmp_dir):
        posts = [
            {
                "uri": "at://did:plc:abc/app.bsky.feed.post/rkey1",
                "collection": "app.bsky.feed.post",
                "value": {
                    "text": "Hello world",
                    "createdAt": "2025-01-01T00:00:00Z",
                },
            },
            {
                "uri": "at://did:plc:abc/app.bsky.feed.post/rkey2",
                "collection": "app.bsky.feed.post",
                "value": {
                    "text": "Replying",
                    "createdAt": "2025-01-02T00:00:00Z",
                    "reply": {
                        "parent": {"uri": "at://did:plc:abc/app.bsky.feed.post/rkey1"},
                        "root": {"uri": "at://did:plc:abc/app.bsky.feed.post/rkey1"},
                    },
                },
            },
            {
                "uri": "at://did:plc:abc/app.bsky.feed.repost/rkey3",
                "collection": "app.bsky.feed.repost",
                "value": {
                    "subject": {"uri": "at://did:plc:other/app.bsky.feed.post/rkeyx"},
                    "createdAt": "2025-01-03T00:00:00Z",
                },
            },
        ]

        path = write_twt("testuser", posts)
        assert os.path.exists(path)

        loaded = read_twt("testuser")
        assert len(loaded) == 3

        # First post
        assert loaded[0]["type"] == "post"
        assert loaded[0]["uri"] == "at://did:plc:abc/app.bsky.feed.post/rkey1"
        assert loaded[0]["text"] == "Hello world"

        # Reply
        assert loaded[1]["type"] == "post"
        assert loaded[1]["reply_to_uri"] == "at://did:plc:abc/app.bsky.feed.post/rkey1"
        assert loaded[1]["root_uri"] == "at://did:plc:abc/app.bsky.feed.post/rkey1"

        # Repost
        assert loaded[2]["type"] == "repost"

    def test_multiline_post(self, tmp_dir):
        posts = [
            {
                "uri": "at://did:plc:abc/app.bsky.feed.post/rkey1",
                "collection": "app.bsky.feed.post",
                "value": {
                    "text": "Line one\nLine two\rMore",
                    "createdAt": "2025-01-01T00:00:00Z",
                },
            },
        ]
        write_twt("multiline", posts)
        loaded = read_twt("multiline")
        assert "\\n" in loaded[0]["text"]
        assert "\r" not in loaded[0]["text"]


class TestFavWrite:
    """Test .fav file writing."""

    def test_write_fav(self, tmp_dir):
        likes = [
            {
                "uri": "at://did:plc:abc/app.bsky.feed.like/rkey1",
                "value": {
                    "subject": {"uri": "at://did:plc:other/app.bsky.feed.post/post1"},
                    "createdAt": "2025-01-01T00:00:00Z",
                },
            },
        ]
        path = write_fav("testuser", likes)
        assert os.path.exists(path)

        with open(path) as f:
            lines = f.read().strip().split("\n")
        assert len(lines) == 1
        parts = lines[0].split("\t")
        assert parts[0] == "at://did:plc:other/app.bsky.feed.post/post1"
        assert parts[2] == "did:plc:other"


class TestThreadsRoundTrip:
    """Round-trip tests for .threads files."""

    def test_round_trip(self, tmp_dir):
        threads = [
            {
                "uri": "at://did:plc:abc/app.bsky.feed.post/1",
                "timestamp": "2025-01-01T00:00:00Z",
                "text": "Root post",
                "root_uri": "",
                "reply_to_uri": "",
                "replies": [
                    {
                        "uri": "at://did:plc:abc/app.bsky.feed.post/2",
                        "timestamp": "2025-01-01T01:00:00Z",
                        "text": "A reply",
                        "root_uri": "at://did:plc:abc/app.bsky.feed.post/1",
                        "reply_to_uri": "at://did:plc:abc/app.bsky.feed.post/1",
                        "replies": [],
                    },
                ],
            }
        ]
        path = write_threads("testuser", threads)
        assert os.path.exists(path)

        loaded = read_threads("testuser")
        assert len(loaded) == 1
        assert loaded[0]["uri"] == "at://did:plc:abc/app.bsky.feed.post/1"
        assert len(loaded[0]["replies"]) == 1
        assert loaded[0]["replies"][0]["text"] == "A reply"


class TestCarWrite:
    """Test .car file writing."""

    def test_write_car(self, tmp_dir):
        data = b"\x1a\xa1rootid\x83" * 100
        path = write_car("testuser", data)
        assert os.path.exists(path)
        with open(path, "rb") as f:
            assert f.read() == data


class TestGmlWrite:
    """Test .gml file writing with mutual_only edges."""

    def test_write_gml(self, tmp_dir):
        nodes = [
            {"id": "alice", "label": "Alice", "node_type": "person"},
            {"id": "bob", "label": "Bob", "node_type": "person"},
            {"id": "sp1", "label": "Tech Pack", "node_type": "starter_pack"},
        ]
        edges = [
            ("alice", "bob", False),
            ("bob", "alice", False),
            ("alice", "sp1", True),
        ]

        path = write_gml("testuser", nodes, edges)
        assert os.path.exists(path)

        with open(path) as f:
            content = f.read()
        assert "directed 1" in content
        assert 'node_type "person"' in content
        assert 'node_type "starter_pack"' in content
        assert "mutual_only 0" in content
        assert "mutual_only 1" in content


class TestGexfIO:
    """Test GEXF write/read and GML read helpers."""

    def test_write_and_read_gexf(self, tmp_dir):
        nodes = [
            {
                "id": "did:plc:alice",
                "label": "alice.bsky.social",
                "display_name": "Alice",
                "followers_count": 10,
                "follows_count": 5,
                "node_type": "self",
                "avatar_url": "https://img.example.com/alice.jpg",
                "backlinks": 2.0,
            },
            {
                "id": "did:plc:bob",
                "label": "bob.bsky.social",
                "display_name": "Bob",
                "followers_count": 4,
                "follows_count": 7,
                "node_type": "follow",
                "avatar_url": "",
                "backlinks": 0.0,
            },
        ]
        edges = [
            {"source": "did:plc:alice", "target": "did:plc:bob", "mutual": True},
            {"source": "did:plc:bob", "target": "did:plc:alice", "mutual": False},
        ]

        path = write_gexf("alice", nodes, edges)
        assert os.path.exists(path)

        loaded_nodes, loaded_edges = read_gexf(path)
        assert len(loaded_nodes) == 2
        assert len(loaded_edges) == 2
        assert loaded_nodes[0]["id"].startswith("did:")
        assert loaded_nodes[0]["followers_count"] >= 0
        assert isinstance(loaded_edges[0]["mutual"], bool)

    def test_read_gml_from_writer_shape(self, tmp_dir):
        nodes = [
            {"id": "alice", "label": "Alice", "node_type": "self"},
            {"id": "bob", "label": "Bob", "node_type": "follow"},
        ]
        edges = [("alice", "bob", False), ("bob", "alice", True)]
        path = write_gml("alice", nodes, edges)

        loaded_nodes, loaded_edges = read_gml(path)
        assert len(loaded_nodes) == 2
        assert len(loaded_edges) == 2
        assert loaded_edges[0]["source"] in ("alice", "bob")


class TestFdatWrite:
    """Test fdat/ file writing."""

    def test_write_fdat(self, tmp_dir):
        profile = {
            "did": "did:plc:friend",
            "displayName": "Friend",
            "avatar": "",
            "description": "A friend",
        }
        follows = [
            {"handle": "alice.bsky.social", "did": "did:plc:alice", "displayName": "Alice", "avatar": ""},
        ]

        path = write_fdat("friend", profile, follows)
        assert os.path.exists(path)
        assert "fdat" in path
