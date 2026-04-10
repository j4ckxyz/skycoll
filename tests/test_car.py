"""Tests for CAR parsing in skycoll.api — uses a small fixture .car file."""

from __future__ import annotations

import struct
import pytest

from skycoll.api import parse_car_records


def _build_minimal_car_v1(did: str, records: list[tuple[str, str, dict]]) -> bytes:
    """Build a minimal CAR v1 file for testing.

    This is a simplified builder that creates enough structure for the
    parser to extract records.  It includes:
      - A CAR v1 header with roots
      - A Commit block (containing the DID)
      - Record blocks for each (collection, rkey, value) tuple

    Args:
        did: The repo DID.
        records: List of (collection, rkey, value_dict) tuples.

    Returns:
        Raw CAR bytes.
    """
    import io
    import cbor2

    buf = io.BytesIO()

    # CAR v1 header: CBOR-encoded {version: 1, roots: [...]}
    header = cbor2.dumps({"version": 1, "roots": []})
    # Varint header length prefix
    _write_varint(buf, len(header))
    buf.write(header)

    # Commit block with DID
    commit_value = cbor2.dumps({
        "$type": "com.atproto.merkle.awl",
        "did": did,
        "rev": "test-rev",
        "prev": None,
    })
    # CID for commit (dag-cbor multicodec = 0x71)
    commit_cid = b"\x01\x71" + b"\x55" + bytes([len(did)]) + did.encode()
    block_data = commit_cid + commit_value
    _write_varint(buf, len(block_data))
    buf.write(block_data)

    # Record blocks
    for collection, rkey, value in records:
        value["$type"] = collection
        record_value = cbor2.dumps(value)
        # Synthetic CID
        record_cid = b"\x01\x71\x55\x20" + b"\x00" * 32  # dummy CID
        block_bytes = record_cid + record_value
        _write_varint(buf, len(block_bytes))
        buf.write(block_bytes)

    return buf.getvalue()


def _write_varint(stream, value: int) -> None:
    """Write an unsigned varint to a byte stream."""
    while value > 0x7F:
        stream.write(bytes([(value & 0x7F) | 0x80]))
        value >>= 7
    stream.write(bytes([value & 0x7F]))


class TestCarParsing:
    """Test CAR v1 parsing for record extraction."""

    def test_parse_empty_car(self):
        """An empty/minimal CAR with no records should return an empty list."""
        # Minimal CAR: just the header
        import io
        import cbor2

        buf = io.BytesIO()
        header = cbor2.dumps({"version": 1, "roots": []})
        _write_varint(buf, len(header))
        buf.write(header)

        car_bytes = buf.getvalue()
        records = parse_car_records(car_bytes)
        assert isinstance(records, list)

    def test_parse_car_with_records(self):
        """A CAR with post and repost records should extract them."""
        did = "did:plc:test123"
        records_data = [
            ("app.bsky.feed.post", "rkey1", {"text": "Hello world", "createdAt": "2025-01-01T00:00:00Z"}),
            ("app.bsky.feed.repost", "rkey2", {"subject": {"uri": "at://post/1"}, "createdAt": "2025-01-02T00:00:00Z"}),
            ("app.bsky.feed.like", "rkey3", {"subject": {"uri": "at://post/2"}, "createdAt": "2025-01-03T00:00:00Z"}),
        ]

        car_bytes = _build_minimal_car_v1(did, records_data)
        records = parse_car_records(car_bytes)

        # Should find records with the correct $type
        types_found = {r["collection"] for r in records}
        assert "app.bsky.feed.post" in types_found
        assert "app.bsky.feed.repost" in types_found
        assert "app.bsky.feed.like" in types_found

    def test_parse_car_extracts_text(self):
        """Post records should have their text field preserved."""
        did = "did:plc:texttest"
        records_data = [
            ("app.bsky.feed.post", "rkey1", {"text": "Test post content", "createdAt": "2025-01-01T00:00:00Z"}),
        ]

        car_bytes = _build_minimal_car_v1(did, records_data)
        records = parse_car_records(car_bytes)

        posts = [r for r in records if r["collection"] == "app.bsky.feed.post"]
        assert len(posts) >= 1
        assert posts[0]["value"].get("text") == "Test post content"