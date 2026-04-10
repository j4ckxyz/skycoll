"""Tests for read-command session selection behavior."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest


class TestFetchCommand:
    @patch("skycoll.commands.fetch.read_dat")
    def test_missing_dat_is_wrapped_as_not_found_error(self, mock_read_dat):
        from skycoll.commands.fetch import run
        from skycoll.errors import NotFoundError

        mock_read_dat.side_effect = FileNotFoundError("missing")

        with pytest.raises(NotFoundError) as exc:
            run("alice.bsky.social")

        assert "No .dat file found for 'alice.bsky.social'." in str(exc.value)
        assert "Run: skycoll init alice.bsky.social" in str(exc.value)

    @patch("skycoll.commands.fetch._run_workers", new_callable=AsyncMock)
    @patch("skycoll.commands.fetch.resolve")
    @patch("skycoll.commands.fetch.read_dat")
    def test_fetch_uses_worker_pipeline(
        self,
        mock_read_dat,
        mock_resolve,
        mock_run_workers,
    ):
        from skycoll.commands.fetch import run

        mock_read_dat.return_value = {
            "follows": [{"handle": "bob.bsky.social", "did": "did:plc:bob"}],
        }
        mock_resolve.return_value = {
            "did": "did:plc:target",
            "handle": "alice.bsky.social",
            "pds": "https://pds.example.com",
        }

        run("alice.bsky.social", workers=3, skip_existing=False)
        mock_run_workers.assert_awaited_once_with(
            [{"handle": "bob.bsky.social", "did": "did:plc:bob"}],
            3,
            False,
        )

    def test_fetch_rejects_invalid_worker_count(self):
        from skycoll.commands.fetch import run
        from skycoll.errors import ParseError

        with pytest.raises(ParseError, match="workers must be between 1 and 10"):
            run("alice.bsky.social", workers=0)

        with pytest.raises(ParseError, match="workers must be between 1 and 10"):
            run("alice.bsky.social", workers=11)


class TestPostsCommand:
    @patch("skycoll.commands.posts.write_twt")
    @patch("skycoll.commands.posts.get_author_feed")
    @patch("skycoll.commands.posts.get_authenticated_session")
    @patch("skycoll.commands.posts.resolve")
    def test_reads_target_did_without_auth_in_feed_mode(
        self,
        mock_resolve,
        mock_get_authenticated_session,
        mock_get_author_feed,
        mock_write_twt,
    ):
        from skycoll.commands.posts import run

        mock_resolve.return_value = {
            "did": "did:plc:target",
            "handle": "alice.bsky.social",
            "pds": "https://pds.example.com",
        }
        mock_get_authenticated_session.return_value = SimpleNamespace(
            handle="viewer.bsky.social", did="did:plc:viewer"
        )
        mock_get_author_feed.return_value = []
        mock_write_twt.return_value = "/tmp/alice.twt"

        run("alice.bsky.social", use_car=False, appview=None)

        args, kwargs = mock_get_author_feed.call_args
        assert args[0] is None
        assert args[1] == "did:plc:target"
        assert kwargs["pds_endpoint"] == "https://pds.example.com"
        mock_get_authenticated_session.assert_not_called()


class TestLikesCommand:
    @patch("skycoll.commands.likes.write_fav")
    @patch("skycoll.commands.likes.get_likes")
    @patch("skycoll.commands.likes.get_authenticated_session")
    @patch("skycoll.commands.likes.resolve")
    def test_non_purge_is_public_and_uses_target(
        self,
        mock_resolve,
        mock_get_authenticated_session,
        mock_get_likes,
        mock_write_fav,
    ):
        from skycoll.commands.likes import run

        mock_resolve.return_value = {
            "did": "did:plc:target",
            "handle": "alice.bsky.social",
            "pds": "https://pds.example.com",
        }
        mock_get_authenticated_session.return_value = SimpleNamespace(
            handle="viewer.bsky.social", did="did:plc:viewer"
        )
        mock_get_likes.return_value = []
        mock_write_fav.return_value = "/tmp/alice.fav"

        run("alice.bsky.social", purge=False, appview=None)

        args, kwargs = mock_get_likes.call_args
        assert args[0] is None
        assert args[1] == "did:plc:target"
        assert kwargs["pds_endpoint"] == "https://pds.example.com"
        mock_get_authenticated_session.assert_not_called()

    @patch("skycoll.commands.likes.get_likes")
    @patch("skycoll.commands.likes.get_authenticated_session")
    @patch("skycoll.commands.likes.resolve")
    def test_purge_uses_target_authenticated_session(
        self,
        mock_resolve,
        mock_get_authenticated_session,
        mock_get_likes,
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

    @patch("skycoll.commands.likes.get_authenticated_session")
    @patch("skycoll.commands.likes.resolve")
    def test_purge_rejects_mismatched_authenticated_account(
        self,
        mock_resolve,
        mock_get_authenticated_session,
    ):
        from skycoll.commands.likes import run
        from skycoll.errors import AuthError

        mock_resolve.return_value = {
            "did": "did:plc:target",
            "handle": "alice.bsky.social",
            "pds": "https://pds.example.com",
        }
        mock_get_authenticated_session.return_value = SimpleNamespace(
            handle="other.bsky.social",
            did="did:plc:other",
        )

        with pytest.raises(AuthError, match="Refusing to purge likes"):
            run("alice.bsky.social", purge=True, appview=None)


class TestSyncCommand:
    @patch("skycoll.commands.sync.write_car")
    @patch("skycoll.commands.sync.get_repo_car")
    @patch("skycoll.commands.sync.get_authenticated_session")
    @patch("skycoll.commands.sync.resolve")
    def test_sync_requires_auth_and_uses_target_did(
        self,
        mock_resolve,
        mock_get_authenticated_session,
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
        mock_get_authenticated_session.return_value = session
        mock_get_repo_car.return_value = b"car-bytes"
        mock_write_car.return_value = "/tmp/alice.car"

        run("alice.bsky.social")

        mock_get_repo_car.assert_called_once_with(session, "did:plc:target")
        mock_get_authenticated_session.assert_called_once_with("alice.bsky.social")
