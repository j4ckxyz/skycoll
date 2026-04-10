"""Tests for AppView configuration."""

from __future__ import annotations

from skycoll.appview import resolve_appview, list_appviews


class TestResolveAppview:
    """Test AppView name resolution."""

    def test_none_returns_none(self):
        assert resolve_appview(None) is None

    def test_empty_returns_none(self):
        assert resolve_appview("") is None

    def test_bluesky_builtin(self):
        result = resolve_appview("bluesky")
        assert result == "did:web:api.bsky.app#bsky_appview"

    def test_blacksky_builtin(self):
        result = resolve_appview("blacksky")
        assert result == "did:web:api.blacksky.community#bsky_appview"

    def test_case_insensitive(self):
        assert resolve_appview("Bluesky") == resolve_appview("bluesky")
        assert resolve_appview("BlackSky") == resolve_appview("blacksky")

    def test_raw_did_passthrough(self):
        custom = "did:web:custom.example#my_appview"
        assert resolve_appview(custom) == custom

    def test_list_appviews(self):
        views = list_appviews()
        assert len(views) >= 2
        names = [v["name"] for v in views]
        assert "bluesky" in names
        assert "blacksky" in names