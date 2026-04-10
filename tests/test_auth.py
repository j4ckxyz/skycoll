"""Tests for skycoll.auth — PKCE, DPoP proof structure, session persistence, OAuth discovery."""

from __future__ import annotations

import json
import os
import stat
import tempfile
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from skycoll.auth import (
    generate_pkce,
    build_dpop_proof,
    generate_dpop_keypair,
    private_key_to_jwk,
    jwk_to_private_key,
    Session,
    discover_auth_server,
    _do_token_request,
    make_authenticated_request,
    _build_localhost_client_id,
    _b64url,
)


def _mock_response(status_code=200, json_data=None, text="", headers=None):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = text or (json.dumps(json_data) if json_data else "")
    resp.headers = headers or {}
    return resp


class TestPKCE:
    """Test PKCE verifier/challenge generation (S256)."""

    def test_pkce_generates_verifier_and_challenge(self):
        verifier, challenge = generate_pkce()
        assert verifier is not None
        assert challenge is not None
        assert len(verifier) > 0
        assert len(challenge) > 0

    def test_pkce_challenge_is_s256(self):
        """The challenge should be the SHA-256 hash of the verifier, base64url-encoded."""
        import hashlib

        verifier, challenge = generate_pkce()
        expected = _b64url(hashlib.sha256(verifier.encode()).digest())
        assert challenge == expected

    def test_pkce_verifier_length(self):
        """PKCE verifier should be at least 43 chars (per RFC 7636)."""
        verifier, _ = generate_pkce()
        assert len(verifier) >= 43

    def test_pkce_unique_per_call(self):
        """Each call should produce a different verifier."""
        v1, _ = generate_pkce()
        v2, _ = generate_pkce()
        assert v1 != v2


class TestDPoPProof:
    """Test DPoP proof JWT structure and signing."""

    def test_proof_structure(self):
        """DPoP proof should be a valid JWS with correct header fields."""
        key = generate_dpop_keypair()
        proof = build_dpop_proof(key, "GET", "https://example.com/resource")

        parts = proof.split(".")
        assert len(parts) == 3, "DPoP proof should be a 3-part JWS"

        import base64
        import json as _json

        # Decode header
        header_padded = parts[0] + "==" 
        header_bytes = base64.urlsafe_b64decode(header_padded)
        header = _json.loads(header_bytes)
        assert header["typ"] == "dpop+jwt"
        assert header["alg"] == "ES256"
        assert "jwk" in header
        assert header["jwk"]["kty"] == "EC"
        assert header["jwk"]["crv"] == "P-256"
        assert "kid" in header

    def test_proof_payload_claims(self):
        """DPoP proof payload should contain htm, htu, iat, and jti."""
        import base64
        import json as _json

        key = generate_dpop_keypair()
        proof = build_dpop_proof(key, "POST", "https://pds.example.com/xrpc/test")

        payload_padded = parts[1] + "==" if len((parts := proof.split("."))) > 1 else ""
        payload_bytes = base64.urlsafe_b64decode(payload_padded)
        payload = _json.loads(payload_bytes)
        assert payload["htm"] == "POST"
        assert payload["htu"] == "https://pds.example.com/xrpc/test"
        assert "iat" in payload
        assert "jti" in payload

    def test_proof_with_access_token_includes_ath(self):
        """DPoP proof with an access token should include the ath claim."""
        import base64
        import hashlib
        import json as _json

        key = generate_dpop_keypair()
        token = "test_access_token_123"
        proof = build_dpop_proof(key, "GET", "https://example.com", access_token=token)

        payload_padded = proof.split(".")[1] + "=="
        payload_bytes = base64.urlsafe_b64decode(payload_padded)
        payload = _json.loads(payload_bytes)
        assert "ath" in payload
        expected_ath = _b64url(hashlib.sha256(token.encode()).digest())
        assert payload["ath"] == expected_ath

    def test_proof_with_nonce(self):
        """DPoP proof with a nonce should include it in the payload."""
        import base64
        import json as _json

        key = generate_dpop_keypair()
        proof = build_dpop_proof(key, "GET", "https://example.com", nonce="server-nonce-123")

        payload_padded = proof.split(".")[1] + "=="
        payload_bytes = base64.urlsafe_b64decode(payload_padded)
        payload = _json.loads(payload_bytes)
        assert payload["nonce"] == "server-nonce-123"

    def test_no_token_leakage_in_proof(self):
        """DPoP proof header should not contain access tokens or private keys."""
        key = generate_dpop_keypair()
        proof = build_dpop_proof(key, "GET", "https://example.com", access_token="secret-token")

        # Header should NOT contain private key material (d field)
        import base64
        import json as _json

        header_padded = proof.split(".")[0] + "=="
        header_bytes = base64.urlsafe_b64decode(header_padded)
        header = _json.loads(header_bytes)
        assert "d" not in header.get("jwk", {}), "Private key should not appear in DPoP header"
        # The token itself should not appear in header or payload as plain text
        assert "secret-token" not in proof


class TestSessionFilePermissions:
    """Test that session files are created with mode 0600."""

    def test_session_file_permissions(self, tmp_path, monkeypatch):
        """Created session files should have 0600 permissions."""
        sessions_dir = str(tmp_path / "sessions")
        monkeypatch.setattr("skycoll.auth.SESSIONS_DIR", sessions_dir)

        key = generate_dpop_keypair()
        session = Session(
            did="did:plc:test123",
            handle="test.bsky.social",
            access_token="test_token",
            refresh_token="test_refresh",
            dpop_key=key,
            pds_endpoint="https://bsky.social",
            token_expiry=9999999999.0,
        )
        session.save()

        path = session._path()
        assert os.path.exists(path)
        mode = stat.S_IMODE(os.stat(path).st_mode)
        assert mode == 0o600, f"Expected 0o600, got {oct(mode)}"

    def test_session_round_trip(self, tmp_path, monkeypatch):
        """Session data should survive a save/load cycle."""
        sessions_dir = str(tmp_path / "sessions")
        monkeypatch.setattr("skycoll.auth.SESSIONS_DIR", sessions_dir)

        key = generate_dpop_keypair()
        session = Session(
            did="did:plc:roundtrip",
            handle="roundtrip.bsky.social",
            access_token="access_tok",
            refresh_token="refresh_tok",
            dpop_key=key,
            dpop_nonce_as="nonce_as",
            dpop_nonce_pds="nonce_pds",
            pds_endpoint="https://pds.example.com",
            token_expiry=1234567890.0,
            auth_server_url="https://auth.example.com/token",
        )
        session.save()

        loaded = Session.load("did:plc:roundtrip")
        assert loaded is not None
        assert loaded.did == "did:plc:roundtrip"
        assert loaded.handle == "roundtrip.bsky.social"
        assert loaded.access_token == "access_tok"
        assert loaded.refresh_token == "refresh_tok"
        assert loaded.pds_endpoint == "https://pds.example.com"
        assert loaded.dpop_nonce_as == "nonce_as"
        assert loaded.dpop_nonce_pds == "nonce_pds"

    def test_jwk_round_trip(self):
        """JWK serialisation should round-trip correctly."""
        key = generate_dpop_keypair()
        jwk = private_key_to_jwk(key)
        assert jwk["kty"] == "EC"
        assert jwk["crv"] == "P-256"
        assert "d" in jwk  # private key present

        restored = jwk_to_private_key(jwk)
        # Verify the keys produce the same public key
        orig_pub = key.public_key().public_numbers()
        restored_pub = restored.public_key().public_numbers()
        assert orig_pub.x == restored_pub.x
        assert orig_pub.y == restored_pub.y


class TestDiscoverAuthServer:
    """Test OAuth auth server discovery via protected-resource metadata."""

    @patch("skycoll.auth.httpx.get")
    def test_delegated_auth_server(self, mock_get):
        """PDS delegates to a separate auth server (e.g. bsky.social entryway)."""
        pr_resp = MagicMock(spec=httpx.Response)
        pr_resp.status_code = 200
        pr_resp.json.return_value = {
            "resource": "https://pds.example.com",
            "authorization_servers": ["https://bsky.social"],
        }
        pr_resp.headers = {}

        as_resp = MagicMock(spec=httpx.Response)
        as_resp.status_code = 200
        as_resp.json.return_value = {
            "issuer": "https://bsky.social",
            "authorization_endpoint": "https://bsky.social/oauth/authorize",
            "token_endpoint": "https://bsky.social/oauth/token",
            "pushed_authorization_request_endpoint": "https://bsky.social/oauth/par",
        }
        as_resp.headers = {}

        mock_get.side_effect = [pr_resp, as_resp]
        meta, origin = discover_auth_server("https://pds.example.com")
        assert origin == "https://bsky.social"
        assert meta["authorization_endpoint"] == "https://bsky.social/oauth/authorize"
        assert meta["token_endpoint"] == "https://bsky.social/oauth/token"

    @patch("skycoll.auth.httpx.get")
    def test_pds_is_its_own_auth_server(self, mock_get):
        """Self-hosted PDS that is also its own auth server."""
        pr_resp = MagicMock(spec=httpx.Response)
        pr_resp.status_code = 200
        pr_resp.json.return_value = {
            "resource": "https://self-hosted.example.com",
            "authorization_servers": ["https://self-hosted.example.com"],
        }
        pr_resp.headers = {}

        as_resp = MagicMock(spec=httpx.Response)
        as_resp.status_code = 200
        as_resp.json.return_value = {
            "issuer": "https://self-hosted.example.com",
            "authorization_endpoint": "https://self-hosted.example.com/oauth/authorize",
            "token_endpoint": "https://self-hosted.example.com/oauth/token",
        }
        as_resp.headers = {}

        mock_get.side_effect = [pr_resp, as_resp]
        meta, origin = discover_auth_server("https://self-hosted.example.com")
        assert origin == "https://self-hosted.example.com"

    @patch("skycoll.auth.httpx.get")
    def test_fallback_when_no_protected_resource(self, mock_get):
        """Fall back to direct auth server metadata when protected-resource is 404."""
        pr_resp = MagicMock(spec=httpx.Response)
        pr_resp.status_code = 404
        pr_resp.headers = {}

        as_resp = MagicMock(spec=httpx.Response)
        as_resp.status_code = 200
        as_resp.json.return_value = {
            "issuer": "https://pds.example.com",
            "authorization_endpoint": "https://pds.example.com/oauth/authorize",
            "token_endpoint": "https://pds.example.com/oauth/token",
        }
        as_resp.headers = {}

        mock_get.side_effect = [pr_resp, as_resp]
        meta, origin = discover_auth_server("https://pds.example.com")
        assert origin == "https://pds.example.com"
        assert meta["authorization_endpoint"] == "https://pds.example.com/oauth/authorize"

    @patch("skycoll.auth.httpx.get")
    def test_all_methods_fail(self, mock_get):
        """Raise RuntimeError when auth server cannot be discovered."""
        mock_get.return_value = MagicMock(status_code=404, headers={})
        with pytest.raises(RuntimeError, match="Cannot discover OAuth auth server"):
            discover_auth_server("https://unreachable.example.com")


class TestTokenRequest:
    """Test token endpoint nonce retry behavior."""

    @patch("skycoll.auth.httpx.post")
    def test_retries_with_nonce_after_400(self, mock_post):
        """Retry token request once when server returns DPoP-Nonce on 400."""
        import base64

        key = generate_dpop_keypair()
        first = _mock_response(
            400,
            json_data={"error": "use_dpop_nonce"},
            headers={"DPoP-Nonce": "nonce-123"},
        )
        second = _mock_response(
            200,
            json_data={"access_token": "tok", "refresh_token": "ref", "sub": "did:plc:abc"},
        )
        mock_post.side_effect = [first, second]

        data, nonce = _do_token_request(
            "https://bsky.social/oauth/token",
            {"grant_type": "authorization_code"},
            key,
            None,
        )

        assert data["access_token"] == "tok"
        assert nonce == "nonce-123"
        assert mock_post.call_count == 2

        first_proof = mock_post.call_args_list[0].kwargs["headers"]["DPoP"]
        second_proof = mock_post.call_args_list[1].kwargs["headers"]["DPoP"]

        first_payload = json.loads(base64.urlsafe_b64decode(first_proof.split(".")[1] + "=="))
        second_payload = json.loads(base64.urlsafe_b64decode(second_proof.split(".")[1] + "=="))

        assert "nonce" not in first_payload
        assert second_payload["nonce"] == "nonce-123"

    @patch("skycoll.auth.httpx.post")
    def test_400_without_nonce_raises(self, mock_post):
        """Do not retry when 400 response has no DPoP-Nonce header."""
        key = generate_dpop_keypair()
        mock_post.return_value = _mock_response(
            400,
            json_data={"error": "invalid_request"},
            headers={},
        )

        with pytest.raises(RuntimeError, match="Token request failed"):
            _do_token_request(
                "https://bsky.social/oauth/token",
                {"grant_type": "authorization_code"},
                key,
                None,
            )

        assert mock_post.call_count == 1


class TestAuthenticatedRequest:
    """Test nonce handling on authenticated PDS requests."""

    @patch("skycoll.auth.Session.save")
    @patch("skycoll.auth.httpx.request")
    def test_401_with_new_nonce_is_retried(self, mock_request, mock_save):
        """A 401 response with DPoP-Nonce should trigger a retry in the same call."""
        key = generate_dpop_keypair()
        session = Session(
            did="did:plc:test",
            handle="test.bsky.social",
            access_token="token-abc",
            refresh_token="refresh-abc",
            dpop_key=key,
            dpop_nonce_as="nonce-as",
            dpop_nonce_pds=None,
            pds_endpoint="https://pds.example.com",
            token_expiry=9999999999.0,
            auth_server_url="https://bsky.social/oauth/token",
        )

        first = _mock_response(401, json_data={"error": "use_dpop_nonce"}, headers={"DPoP-Nonce": "nonce-pds-1"})
        second = _mock_response(200, json_data={"ok": True}, headers={})
        mock_request.side_effect = [first, second]

        resp = make_authenticated_request(session, "GET", "/xrpc/com.atproto.server.describeServer")

        assert resp.status_code == 200
        assert mock_request.call_count == 2
        assert session.dpop_nonce_pds == "nonce-pds-1"
        mock_save.assert_called()


class TestLocalhostClientId:
    """Test localhost-development client_id generation."""

    def test_localhost_client_id_uses_required_origin(self):
        """Client ID must use http://localhost origin with query params."""
        redirect_uri = "http://127.0.0.1:54321/callback"
        scope = "atproto transition:generic"

        client_id = _build_localhost_client_id(redirect_uri, scope)
        parsed = urlparse(client_id)

        assert parsed.scheme == "http"
        assert parsed.netloc == "localhost"
        assert parsed.path == "/"

        qs = parse_qs(parsed.query)
        assert qs.get("redirect_uri") == [redirect_uri]
        assert qs.get("scope") == [scope]
