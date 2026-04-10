"""Tests for read-command session selection behavior."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest


class TestFetchCommand:
    @patch("skycoll.commands.fetch.read_dat")
    def test_missing_dat_prints_friendly_error(self, mock_read_dat, capsys):
        from skycoll.commands.fetch import run

        mock_read_dat.side_effect = FileNotFoundError("missing")

        with pytest.raises(SystemExit) as exc:
            run("alice.bsky.social")

        assert exc.value.code == 1
        out = capsys.readouterr().out
        assert "No .dat file found for 'alice.bsky.social'." in out
        assert "Run: skycoll init alice.bsky.social" in out

    @patch("skycoll.commands.fetch.get_any_session")
    @patch("skycoll.commands.fetch.read_dat")
    def test_uses_cached_session(self, mock_read_dat, mock_get_any_session, capsys):
        from skycoll.commands.fetch import run

        mock_read_dat.return_value = {"follows": []}
        mock_get_any_session.return_value = SimpleNamespace(
            handle="viewer.bsky.social",
            did="did:plc:viewer",
        )

        run("alice.bsky.social")
        out = capsys.readouterr().out
        assert "Using cached session: viewer.bsky.social (did:plc:viewer)" in out


class TestPostsCommand:
    @patch("skycoll.commands.posts.write_twt")
    @patch("skycoll.commands.posts.get_author_feed")
    @patch("skycoll.commands.posts.get_any_session")
    @patch("skycoll.commands.posts.resolve")
    def test_reads_target_did_with_any_session(
        self,
        mock_resolve,
        mock_get_any_session,
        mock_get_author_feed,
        mock_write_twt,
    ):
        from skycoll.commands.posts import run

        mock_resolve.return_value = {
            "did": "did:plc:target",
            "handle": "alice.bsky.social",
            "pds": "https://pds.example.com",
        }
        session = SimpleNamespace(handle="viewer.bsky.social", did="did:plc:viewer")
        mock_get_any_session.return_value = session
        mock_get_author_feed.return_value = []
        mock_write_twt.return_value = "/tmp/alice.twt"

        run("alice.bsky.social", use_car=False, appview=None)

        args, kwargs = mock_get_author_feed.call_args
        assert args[0] is session
        assert args[1] == "did:plc:target"


class TestLikesCommand:
    @patch("skycoll.commands.likes.write_fav")
    @patch("skycoll.commands.likes.get_likes")
    @patch("skycoll.commands.likes.get_any_session")
    @patch("skycoll.commands.likes.resolve")
    def test_non_purge_uses_any_session_and_target(
        self,
        mock_resolve,
        mock_get_any_session,
        mock_get_likes,
        mock_write_fav,
    ):
        from skycoll.commands.likes import run

        mock_resolve.return_value = {
            "did": "did:plc:target",
            "handle": "alice.bsky.social",
            "pds": "https://pds.example.com",
        }
        session = SimpleNamespace(handle="viewer.bsky.social", did="did:plc:viewer")
        mock_get_any_session.return_value = session
        mock_get_likes.return_value = []
        mock_write_fav.return_value = "/tmp/alice.fav"

        run("alice.bsky.social", purge=False, appview=None)

        args, kwargs = mock_get_likes.call_args
        assert args[0] is session
        assert args[1] == "did:plc:target"

    @patch("skycoll.commands.likes.get_any_session")
    @patch("skycoll.commands.likes.get_likes")
    @patch("skycoll.commands.likes.get_authenticated_session")
    @patch("skycoll.commands.likes.resolve")
    def test_purge_uses_target_authenticated_session(
        self,
        mock_resolve,
        mock_get_authenticated_session,
        mock_get_likes,
        mock_get_any_session,
    ):
        from skycoll.commands.likes import run

        mock_resolve.return_value = {
            "did": "did:plc:target",
            "handle": "alice.bsky.social",
            "pds": "https://pds.example.com",
        }
        session = SimpleNamespace(handle="alice.bsky.social", did="did:plc:target")
        mock_get_authenticated_session.return_value = session
        mock_get_likes.return_value = []

        run("alice.bsky.social", purge=True, appview=None)

        mock_get_authenticated_session.assert_called_once_with("alice.bsky.social")
        mock_get_any_session.assert_not_called()

    @patch("skycoll.commands.likes.get_authenticated_session")
    @patch("skycoll.commands.likes.resolve")
    def test_purge_rejects_mismatched_authenticated_account(
        self,
        mock_resolve,
        mock_get_authenticated_session,
    ):
        from skycoll.commands.likes import run

        mock_resolve.return_value = {
            "did": "did:plc:target",
            "handle": "alice.bsky.social",
            "pds": "https://pds.example.com",
        }
        mock_get_authenticated_session.return_value = SimpleNamespace(
            handle="other.bsky.social",
            did="did:plc:other",
        )

        with pytest.raises(RuntimeError, match="Refusing to purge likes"):
            run("alice.bsky.social", purge=True, appview=None)


class TestSyncCommand:
    @patch("skycoll.commands.sync.write_car")
    @patch("skycoll.commands.sync.get_repo_car")
    @patch("skycoll.commands.sync.get_any_session")
    @patch("skycoll.commands.sync.resolve")
    def test_sync_uses_any_session_and_target_did(
        self,
        mock_resolve,
        mock_get_any_session,
        mock_get_repo_car,
        mock_write_car,
    ):
        from skycoll.commands.sync import run

        mock_resolve.return_value = {
            "did": "did:plc:target",
            "handle": "alice.bsky.social",
            "pds": "https://pds.example.com",
        }
        session = SimpleNamespace(handle="viewer.bsky.social", did="did:plc:viewer")
        mock_get_any_session.return_value = session
        mock_get_repo_car.return_value = b"car-bytes"
        mock_write_car.return_value = "/tmp/alice.car"

        run("alice.bsky.social")

        mock_get_repo_car.assert_called_once_with(session, "did:plc:target")
