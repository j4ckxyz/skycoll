"""Tests for Constellation client — mocked HTTP."""

from __future__ import annotations

from unittest.mock import patch, MagicMock
import httpx

from skycoll.constellation import get_all_backlink_counts, get_backlink_count


class TestConstellation:
    """Test Constellation HTTP client with mocked responses."""

    @patch("skycoll.constellation.httpx.get")
    def test_get_all_backlink_counts(self, mock_get):
        """Should return parsed JSON on success."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "app.bsky.feed.like": {"/subject": 42},
            "app.bsky.graph.follow": {"/subject": 100},
        }
        mock_get.return_value = mock_resp

        result = get_all_backlink_counts("https://const.example.com", "did:plc:abc")
        assert result is not None
        assert result["app.bsky.feed.like"]["/subject"] == 42
        assert result["app.bsky.graph.follow"]["/subject"] == 100
        mock_get.assert_called_once()

    @patch("skycoll.constellation.httpx.get")
    def test_get_all_backlink_counts_failure(self, mock_get):
        """Should return None on HTTP failure and not raise."""
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_get.return_value = mock_resp

        result = get_all_backlink_counts("https://const.example.com", "did:plc:abc")
        assert result is None

    @patch("skycoll.constellation.httpx.get")
    def test_get_all_backlink_counts_network_error(self, mock_get):
        """Should return None on network error and not raise."""
        mock_get.side_effect = httpx.ConnectError("connection refused")

        result = get_all_backlink_counts("https://const.example.com", "did:plc:abc")
        assert result is None

    @patch("skycoll.constellation.httpx.get")
    def test_get_backlink_count(self, mock_get):
        """Should return integer count on success."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"count": 7}
        mock_get.return_value = mock_resp

        result = get_backlink_count("https://const.example.com", "at://did:plc:abc/app.bsky.feed.post/1", "app.bsky.feed.like", "/subject")
        assert result == 7

    @patch("skycoll.constellation.httpx.get")
    def test_get_backlink_count_failure(self, mock_get):
        """Should return None on failure."""
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_get.return_value = mock_resp

        result = get_backlink_count("https://const.example.com", "at://x", "app.bsky.feed.like", "/subject")
        assert result is None