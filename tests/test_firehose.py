"""Tests for firehose event filtering and event-shape helpers."""

from __future__ import annotations


def _filter_event(event: dict, filter_did: str | None) -> bool:
    """Determine if a firehose event matches the given DID filter.

    This is the pure filtering logic extracted for unit testing.
    """
    if not filter_did:
        return True
    repo_did = event.get("did") or event.get("repo")
    return repo_did == filter_did


class TestFirehoseFilter:
    """Test firehose event filtering."""

    def test_no_filter_passes_all(self):
        """With no DID filter, all events pass."""
        assert _filter_event({"did": "did:plc:abc"}, None) is True
        assert _filter_event({"did": "did:plc:other"}, None) is True

    def test_did_filter_matches(self):
        """Events matching the DID filter should pass."""
        assert _filter_event({"did": "did:plc:abc"}, "did:plc:abc") is True

    def test_did_filter_rejects(self):
        """Events not matching the DID filter should be rejected."""
        assert _filter_event({"did": "did:plc:other"}, "did:plc:abc") is False

    def test_repo_field_matching(self):
        """Should also check the 'repo' field for DID matching."""
        assert _filter_event({"repo": "did:plc:abc"}, "did:plc:abc") is True

    def test_missing_did_rejected(self):
        """Events without a DID or repo field should be rejected when filter is set."""
        assert _filter_event({"text": "hello"}, "did:plc:abc") is False


class _Obj:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def test_event_repo_did_from_direct_fields() -> None:
    from skycoll.commands.firehose import _event_repo_did

    assert _event_repo_did(_Obj(did="did:plc:abc")) == "did:plc:abc"
    assert _event_repo_did(_Obj(repo="did:plc:def")) == "did:plc:def"


def test_event_repo_did_from_commit_fields() -> None:
    from skycoll.commands.firehose import _event_repo_did

    ev1 = _Obj(commit=_Obj(did="did:plc:abc"))
    ev2 = _Obj(commit=_Obj(repo="did:plc:def"))
    assert _event_repo_did(ev1) == "did:plc:abc"
    assert _event_repo_did(ev2) == "did:plc:def"


def test_event_payload_prefers_model_dump() -> None:
    from skycoll.commands.firehose import _event_payload

    class _M:
        def model_dump(self):
            return {"ok": True}

    assert _event_payload(_M()) == {"ok": True}
