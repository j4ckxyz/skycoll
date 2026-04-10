"""Tests for skycoll.api — pagination and rate-limit handling."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from skycoll.auth import Session


def _make_session() -> Session:
    """Create a minimal mock session for testing."""
    from skycoll.auth import generate_dpop_keypair
    key = generate_dpop_keypair()
    return Session(
        did="did:plc:test",
        handle="test.bsky.social",
        access_token="test_token",
        refresh_token="test_refresh",
        dpop_key=key,
        pds_endpoint="https://bsky.social",
        access_token_expiry=9999999999.0,
        refresh_token_expiry=9999999999.0,
    )


class TestPagination:
    """Test cursor-based pagination logic."""

    @patch("skycoll.api.make_authenticated_request")
    def test_single_page(self, mock_request):
        """When the response has no cursor, only one page is fetched."""
        from skycoll.api import _paginated_get

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "records": [{"uri": "at://1"}, {"uri": "at://2"}],
        }
        mock_resp.headers = {}
        mock_request.return_value = mock_resp

        session = _make_session()
        items = list(_paginated_get(
            session, "/xrpc/test",
            params={"collection": "app.bsky.feed.post"},
            cursor_key="cursor",
            collection_items_key="records",
        ))

        assert len(items) == 2
        assert items[0]["uri"] == "at://1"
        assert mock_request.call_count == 1

    @patch("skycoll.api.make_authenticated_request")
    def test_multi_page(self, mock_request):
        """When cursor is present, multiple pages are fetched until no cursor."""
        from skycoll.api import _paginated_get

        page1 = MagicMock()
        page1.status_code = 200
        page1.json.return_value = {
            "cursor": "page2cursor",
            "records": [{"uri": "at://1"}],
        }
        page1.headers = {}

        page2 = MagicMock()
        page2.status_code = 200
        page2.json.return_value = {
            "records": [{"uri": "at://2"}],
        }
        page2.headers = {}

        mock_request.side_effect = [page1, page2]

        session = _make_session()
        items = list(_paginated_get(
            session, "/xrpc/test",
            params={},
            cursor_key="cursor",
            collection_items_key="records",
        ))

        assert len(items) == 2
        assert mock_request.call_count == 2

    @patch("skycoll.api.make_authenticated_request")
    def test_custom_items_key(self, mock_request):
        """The collection_items_key parameter controls which key contains items."""
        from skycoll.api import _paginated_get

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "follows": [{"handle": "a"}, {"handle": "b"}],
        }
        mock_resp.headers = {}
        mock_request.return_value = mock_resp

        session = _make_session()
        items = list(_paginated_get(
            session, "/xrpc/test",
            params={},
            collection_items_key="follows",
        ))

        assert len(items) == 2
        assert items[0]["handle"] == "a"


class TestRateLimitRetry:
    """Test exponential back-off on HTTP 429."""

    @patch("skycoll.api.time.sleep")
    @patch("skycoll.api.make_authenticated_request")
    def test_retry_on_429(self, mock_request, mock_sleep):
        """Should retry on 429 with exponential back-off."""
        from skycoll.api import _paginated_get

        resp_429 = MagicMock()
        resp_429.status_code = 429
        resp_429.headers = {}

        resp_ok = MagicMock()
        resp_ok.status_code = 200
        resp_ok.json.return_value = {"records": [{"uri": "ok"}]}
        resp_ok.headers = {}

        mock_request.side_effect = [resp_429, resp_ok]

        session = _make_session()
        items = list(_paginated_get(session, "/xrpc/test"))

        assert len(items) == 1
        assert items[0]["uri"] == "ok"
        mock_sleep.assert_called_once_with(1)

    @patch("skycoll.api.time.sleep")
    @patch("skycoll.api.make_authenticated_request")
    def test_max_retries_exceeded(self, mock_request, mock_sleep):
        """Should raise RateLimitError after max retries on 429."""
        from skycoll.api import _paginated_get
        from skycoll.errors import RateLimitError

        resp_429 = MagicMock()
        resp_429.status_code = 429
        resp_429.headers = {}

        mock_request.return_value = resp_429

        session = _make_session()
        with pytest.raises(RateLimitError, match="after 3 retries"):
            list(_paginated_get(session, "/xrpc/test"))
