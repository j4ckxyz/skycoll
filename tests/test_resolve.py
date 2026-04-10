"""Tests for skycoll.resolve — handle/DID/PDS resolution."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import httpx
import pytest


def _mock_response(status_code=200, json_data=None, text=""):
    """Build a minimal mock httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = text or (json.dumps(json_data) if json_data else "")
    return resp


# ---------------------------------------------------------------------------
# resolve_handle_to_did
# ---------------------------------------------------------------------------


class TestResolveHandleToDid:
    """Test handle → DID resolution via well-known, XRPC, and DNS."""

    @patch("skycoll.resolve.httpx.get")
    def test_well_known_path(self, mock_get):
        """Resolve handle via HTTPS well-known path."""
        from skycoll.resolve import resolve_handle_to_did

        mock_get.return_value = _mock_response(200, text="did:plc:abc123")
        did = resolve_handle_to_did("alice.bsky.social")
        assert did == "did:plc:abc123"
        mock_get.assert_called_once()
        call_url = mock_get.call_args[0][0]
        assert "alice.bsky.social" in call_url

    @patch("skycoll.resolve.httpx.get")
    def test_xrpc_fallback(self, mock_get):
        """Resolve handle via XRPC when well-known fails."""
        from skycoll.resolve import resolve_handle_to_did

        # First call (well-known) fails, second (XRPC) succeeds
        fail_resp = _mock_response(404)
        success_resp = _mock_response(200, json_data={"did": "did:plc:xwy789"})
        mock_get.side_effect = [fail_resp, success_resp]

        did = resolve_handle_to_did("bob.bsky.social")
        assert did == "did:plc:xwy789"

    @patch("skycoll.resolve.httpx.get")
    def test_all_methods_fail(self, mock_get):
        """Raise RuntimeError when all resolution methods fail."""
        from skycoll.resolve import resolve_handle_to_did

        fail_resp = _mock_response(404)
        mock_get.return_value = fail_resp

        try:
            import dns.resolver
            with patch.object(dns.resolver, "resolve", side_effect=Exception("DNS fail")):
                with pytest.raises(RuntimeError, match="Cannot resolve handle"):
                    resolve_handle_to_did("nonexistent.example")
        except ImportError:
            with pytest.raises(RuntimeError, match="Cannot resolve handle"):
                resolve_handle_to_did("nonexistent.example")

    @patch("skycoll.resolve.httpx.get")
    def test_error_message_includes_methods_tried(self, mock_get):
        """Error message should list the methods that were tried."""
        from skycoll.resolve import resolve_handle_to_did

        mock_get.return_value = _mock_response(404)

        try:
            import dns.resolver
            with patch.object(dns.resolver, "resolve", side_effect=Exception("DNS fail")):
                with pytest.raises(RuntimeError, match="Tried: HTTPS well-known"):
                    resolve_handle_to_did("nope.example")
        except ImportError:
            with pytest.raises(RuntimeError, match="Tried: HTTPS well-known"):
                resolve_handle_to_did("nope.example")


# ---------------------------------------------------------------------------
# resolve_did_to_handle
# ---------------------------------------------------------------------------


class TestResolveDidToHandle:
    """Test DID → handle resolution."""

    @patch("skycoll.resolve.fetch_did_document")
    def test_resolve_did_plc(self, mock_fetch):
        """Extract handle from did:plc document."""
        from skycoll.resolve import resolve_did_to_handle

        mock_fetch.return_value = {
            "alsoKnownAs": ["at://alice.bsky.social"],
            "service": [],
        }
        handle = resolve_did_to_handle("did:plc:abc123")
        assert handle == "alice.bsky.social"

    @patch("skycoll.resolve.httpx.get")
    @patch("skycoll.resolve.fetch_did_document")
    def test_xrpc_fallback_on_did_doc_failure(self, mock_fetch, mock_get):
        """Fall back to bsky.social XRPC when DID document fetch fails."""
        from skycoll.resolve import resolve_did_to_handle

        mock_fetch.side_effect = RuntimeError("DID doc fetch failed")
        mock_get.return_value = _mock_response(200, json_data={"handle": "alice.bsky.social"})

        handle = resolve_did_to_handle("did:plc:abc123")
        assert handle == "alice.bsky.social"

    @patch("skycoll.resolve.httpx.get")
    @patch("skycoll.resolve.fetch_did_document")
    def test_all_methods_fail_includes_reasons(self, mock_fetch, mock_get):
        """Error message includes reasons when DID doc and XRPC both fail."""
        from skycoll.resolve import resolve_did_to_handle

        mock_fetch.side_effect = RuntimeError("plc.directory down")
        mock_get.return_value = _mock_response(500)

        with pytest.raises(RuntimeError, match="DID document fetch failed"):
            resolve_did_to_handle("did:plc:abc123")


# ---------------------------------------------------------------------------
# fetch_did_document
# ---------------------------------------------------------------------------


class TestFetchDidDocument:
    """Test DID document fetching for did:plc and did:web."""

    @patch("skycoll.resolve.httpx.get")
    def test_did_plc(self, mock_get):
        """Fetch did:plc document from plc.directory."""
        from skycoll.resolve import fetch_did_document

        doc = {"id": "did:plc:abc123", "alsoKnownAs": ["at://alice.bsky.social"]}
        mock_get.return_value = _mock_response(200, json_data=doc)

        result = fetch_did_document("did:plc:abc123")
        assert result["id"] == "did:plc:abc123"
        call_url = mock_get.call_args[0][0]
        assert "plc.directory" in call_url

    @patch("skycoll.resolve.httpx.get")
    def test_did_web(self, mock_get):
        """Fetch did:web document from domain well-known path."""
        from skycoll.resolve import fetch_did_document

        doc = {"id": "did:web:example.com"}
        mock_get.return_value = _mock_response(200, json_data=doc)

        result = fetch_did_document("did:web:example.com")
        assert result["id"] == "did:web:example.com"
        call_url = mock_get.call_args[0][0]
        assert "example.com" in call_url

    def test_unsupported_did_method(self):
        """Raise RuntimeError for unsupported DID methods."""
        from skycoll.resolve import fetch_did_document

        with pytest.raises(RuntimeError, match="Unsupported DID method"):
            fetch_did_document("did:key:abc")

    def test_unsupported_did_method_message(self):
        """Error message mentions only did:plc and did:web are supported."""
        from skycoll.resolve import fetch_did_document

        with pytest.raises(RuntimeError, match="Only did:plc and did:web"):
            fetch_did_document("did:key:abc")

    @patch("skycoll.resolve.httpx.get")
    def test_404_error_message(self, mock_get):
        """404 response produces a helpful error about DID not found."""
        from skycoll.resolve import fetch_did_document

        mock_get.return_value = _mock_response(404)

        with pytest.raises(RuntimeError, match="DID not found"):
            fetch_did_document("did:plc:nonexistent12345")

    @patch("skycoll.resolve.httpx.get")
    def test_network_error(self, mock_get):
        """Network error produces an error mentioning the failure."""
        from skycoll.resolve import fetch_did_document

        mock_get.side_effect = httpx.ConnectError("Connection refused")

        with pytest.raises(RuntimeError, match="Network error"):
            fetch_did_document("did:plc:abc123")

    @patch("skycoll.resolve.httpx.get")
    def test_invalid_json_response(self, mock_get):
        """Non-JSON response produces an error about invalid JSON."""
        from skycoll.resolve import fetch_did_document

        resp = _mock_response(200)
        resp.json.side_effect = json.JSONDecodeError("err", "doc", 0)
        mock_get.return_value = resp

        with pytest.raises(RuntimeError, match="not valid JSON"):
            fetch_did_document("did:plc:abc123")


class TestResolvePdsEndpoint:
    """Test PDS endpoint extraction from DID documents."""

    @patch("skycoll.resolve.httpx.get")
    def test_explicit_pds(self, mock_get):
        """Extract explicit PDS from DID document."""
        from skycoll.resolve import resolve_pds_endpoint

        doc = {
            "service": [
                {"id": "#atproto_pds", "serviceEndpoint": "https://pds.example.com"}
            ]
        }
        mock_get.return_value = _mock_response(200, json_data=doc)

        pds = resolve_pds_endpoint("did:plc:abc123")
        assert pds == "https://pds.example.com"

    @patch("skycoll.resolve.httpx.get")
    def test_fallback_bsky_social(self, mock_get):
        """Fall back to bsky.social when no PDS service entry exists."""
        from skycoll.resolve import resolve_pds_endpoint

        mock_get.return_value = _mock_response(200, json_data={"service": []})
        pds = resolve_pds_endpoint("did:plc:abc123")
        assert pds == "https://bsky.social"


class TestResolve:
    """Test the unified resolve() function."""

    @patch("skycoll.resolve.resolve_pds_endpoint")
    @patch("skycoll.resolve.resolve_handle_to_did")
    def test_handle_input(self, mock_handle, mock_pds):
        """Resolve a handle to did + handle + pds."""
        from skycoll.resolve import resolve

        mock_handle.return_value = "did:plc:abc123"
        mock_pds.return_value = "https://bsky.social"

        result = resolve("alice.bsky.social")
        assert result["did"] == "did:plc:abc123"
        assert result["handle"] == "alice.bsky.social"
        assert result["pds"] == "https://bsky.social"

    @patch("skycoll.resolve.resolve_pds_endpoint")
    @patch("skycoll.resolve.resolve_did_to_handle")
    def test_did_input(self, mock_did, mock_pds):
        """Resolve a DID to did + handle + pds."""
        from skycoll.resolve import resolve

        mock_did.return_value = "alice.bsky.social"
        mock_pds.return_value = "https://pds.example.com"

        result = resolve("did:plc:abc123")
        assert result["did"] == "did:plc:abc123"
        assert result["handle"] == "alice.bsky.social"