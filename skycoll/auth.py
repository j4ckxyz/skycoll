"""AT Protocol OAuth 2.0 authentication with PKCE and DPoP.

Implements the full public-client (native-app) authorisation code flow:

  1. Generate PKCE verifier / S256 challenge.
  2. Generate ES256 DPoP keypair.
  3. Discover the authorisation server via
     ``/.well-known/oauth-protected-resource`` (with fallback).
  4. Submit a Pushed Authorization Request (PAR) to the auth server.
  5. Open the browser to the authorisation endpoint so the user can consent.
  6. Listen on a loopback redirect URI for the callback.
  7. Exchange the authorisation code for tokens, binding DPoP proofs.
  8. Persist the session to ``~/.skycoll/sessions/<did>.json`` (mode 0600).

Scopes requested:
  - ``atproto`` — always required by the protocol.
  - ``transition:generic`` — provides read access and like-deletion.

  NOTE: When AT Protocol proposal 0011 (granular scopes) stabilises,
  ``transition:generic`` should be narrowed to only
  ``app.bsky.feed.*`` reads and ``app.bsky.feed.like`` delete.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import sys
import threading
import time
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional
from urllib.parse import urlencode, parse_qs, urlparse, quote

import httpx
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PublicFormat,
    PrivateFormat,
    NoEncryption,
)
from cryptography.hazmat.primitives import hashes

from .resolve import resolve
from .errors import AuthError, NetworkError, RateLimitError, NotFoundError, ParseError
from .output import info, warn
from .verbosity import vprint

SESSIONS_DIR = os.path.expanduser("~/.skycoll/sessions")

SCOPES = "atproto transition:generic"


# ---------------------------------------------------------------------------
# DPoP helpers
# ---------------------------------------------------------------------------


def _b64url(data: bytes) -> str:
    """Base64url-encode *data* without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    """Decode base64url text with optional missing padding."""
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("ascii"))


def _jwt_expiry(token: str) -> float:
    """Extract the ``exp`` claim from a JWT as a Unix timestamp.

    Returns ``0.0`` if the token is empty, malformed, or does not contain
    a numeric ``exp`` claim.
    """
    if not token or "." not in token:
        return 0.0

    try:
        parts = token.split(".")
        if len(parts) < 2:
            return 0.0
        payload_raw = _b64url_decode(parts[1])
        payload = json.loads(payload_raw.decode("utf-8"))
        exp = payload.get("exp")
        if isinstance(exp, (int, float)):
            return float(exp)
    except (TypeError, ValueError, json.JSONDecodeError):
        return 0.0

    return 0.0


def generate_dpop_keypair() -> ec.EllipticCurvePrivateKey:
    """Generate an ES256 (P-256) keypair for DPoP proofs."""
    return ec.generate_private_key(ec.SECP256R1())


def private_key_to_jwk(key: ec.EllipticCurvePrivateKey) -> dict:
    """Serialise a P-256 private key to a JWK dict.

    Only the fields required for DPoP proof construction are included.
    """
    pub = key.public_key()
    pub_numbers = pub.public_numbers()
    priv_numbers = key.private_numbers()
    return {
        "kty": "EC",
        "crv": "P-256",
        "x": _b64url(pub_numbers.x.to_bytes(32, "big")),
        "y": _b64url(pub_numbers.y.to_bytes(32, "big")),
        "d": _b64url(priv_numbers.private_value.to_bytes(32, "big")),
    }


def jwk_to_private_key(jwk: dict) -> ec.EllipticCurvePrivateKey:
    """Deserialise a JWK dict back to a P-256 private key object."""
    x = int.from_bytes(base64.urlsafe_b64decode(jwk["x"] + "=="), "big")
    y = int.from_bytes(base64.urlsafe_b64decode(jwk["y"] + "=="), "big")
    d = int.from_bytes(base64.urlsafe_b64decode(jwk["d"] + "=="), "big")
    pub_numbers = ec.EllipticCurvePublicNumbers(x, y, ec.SECP256R1())
    priv_numbers = ec.EllipticCurvePrivateNumbers(d, pub_numbers)
    return priv_numbers.private_key()


def _dpop_header_and_sig(key: ec.EllipticCurvePrivateKey) -> tuple[dict, str]:
    """Return the JWK *header* (public parts) and a compact-JWS *kid* thumbprint."""
    pub = key.public_key()
    pub_numbers = pub.public_numbers()
    jwk_pub = {
        "kty": "EC",
        "crv": "P-256",
        "x": _b64url(pub_numbers.x.to_bytes(32, "big")),
        "y": _b64url(pub_numbers.y.to_bytes(32, "big")),
    }
    thumb = _b64url(
        hashlib.sha256(
            json.dumps(jwk_pub, separators=(",", ":"), sort_keys=True).encode()
        ).digest()
    )
    return jwk_pub, thumb


def build_dpop_proof(
    key: ec.EllipticCurvePrivateKey,
    method: str,
    url: str,
    access_token: Optional[str] = None,
    nonce: Optional[str] = None,
) -> str:
    """Construct a DPoP proof JWT (ES256, compact serialisation).

    Args:
        key: The DPoP private key.
        method: HTTP method (``GET``, ``POST``, etc.).
        url: The target URL (the ``htu`` claim).
        access_token: If provided, a SHA-256 ``ath`` claim is added.
        nonce: The ``DPoP-Nonce`` from the server, if any.

    Returns:
        Compact JWS string (``header.payload.signature``).
    """
    jwk_pub, kid = _dpop_header_and_sig(key)

    header = {
        "typ": "dpop+jwt",
        "alg": "ES256",
        "kid": kid,
        "jwk": jwk_pub,
    }

    payload: dict = {
        "jti": secrets.token_urlsafe(16),
        "htm": method.upper(),
        "htu": url,
        "iat": int(time.time()),
    }
    if access_token is not None:
        payload["ath"] = _b64url(hashlib.sha256(access_token.encode()).digest())
    if nonce is not None:
        payload["nonce"] = nonce

    header_b64 = _b64url(json.dumps(header, separators=(",", ":")).encode())
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode())

    signing_input = f"{header_b64}.{payload_b64}".encode()
    der_sig = key.sign(signing_input, ec.ECDSA(hashes.SHA256()))
    # DER to raw (r || s) per RFC 7518 § 3.4
    from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature
    r, s = decode_dss_signature(der_sig)
    sig_bytes = r.to_bytes(32, "big") + s.to_bytes(32, "big")
    sig_b64 = _b64url(sig_bytes)

    return f"{header_b64}.{payload_b64}.{sig_b64}"


# ---------------------------------------------------------------------------
# PKCE helpers
# ---------------------------------------------------------------------------


def generate_pkce() -> tuple[str, str]:
    """Generate a PKCE code verifier and its S256 challenge.

    Returns:
        Tuple of ``(verifier, challenge)``.
    """
    verifier = secrets.token_urlsafe(64)
    challenge = _b64url(hashlib.sha256(verifier.encode()).digest())
    return verifier, challenge


# ---------------------------------------------------------------------------
# Authorisation-server discovery
# ---------------------------------------------------------------------------


def discover_auth_server(pds_url: str) -> tuple[dict, str]:
    """Discover the OAuth 2.0 Authorization Server for a PDS.

    Follows the AT Protocol OAuth discovery flow:

      1. Fetch ``/.well-known/oauth-protected-resource`` from the PDS to
         find the Authorization Server origin(s).
      2. Fetch ``/.well-known/oauth-authorization-server`` from the
         discovered Authorization Server.

    If the PDS does not publish protected-resource metadata, falls back
    to trying ``/.well-known/oauth-authorization-server`` on the PDS
    directly (for self-hosted PDS instances that are also auth servers).

    Args:
        pds_url: Origin of the PDS (e.g. ``https://bsky.social``).

    Returns:
        Tuple of (auth_server_metadata, auth_server_origin).

    Raises:
        RuntimeError: If metadata cannot be discovered by any method.
    """
    auth_server_origin = None

    # Step 1: Try protected-resource metadata to find the auth server
    try:
        pr_url = f"{pds_url}/.well-known/oauth-protected-resource"
        vprint(f"auth discovery: GET {pr_url}")
        resp = httpx.get(pr_url, follow_redirects=True, timeout=15)
        if resp.status_code == 200:
            pr_data = resp.json()
            servers = pr_data.get("authorization_servers", [])
            if servers:
                auth_server_origin = servers[0].rstrip("/")
                vprint(f"auth discovery: protected-resource auth server -> {auth_server_origin}")
    except httpx.HTTPError:
        vprint("auth discovery: protected-resource fetch failed")
        pass

    # Step 2: Fetch auth server metadata from the discovered origin
    if auth_server_origin:
        try:
            as_url = f"{auth_server_origin}/.well-known/oauth-authorization-server"
            vprint(f"auth discovery: GET {as_url}")
            resp = httpx.get(as_url, follow_redirects=True, timeout=15)
            if resp.status_code == 200:
                meta = resp.json()
                if meta.get("issuer", "").rstrip("/") == auth_server_origin:
                    vprint("auth discovery: using delegated authorization server metadata")
                    return meta, auth_server_origin
        except httpx.HTTPError:
            vprint("auth discovery: delegated authorization-server metadata fetch failed")
            pass

    # Step 3: Fallback — try auth server metadata directly on the PDS
    try:
        as_url = f"{pds_url}/.well-known/oauth-authorization-server"
        vprint(f"auth discovery: fallback GET {as_url}")
        resp = httpx.get(as_url, follow_redirects=True, timeout=15)
        if resp.status_code == 200:
            meta = resp.json()
            origin = meta.get("issuer", pds_url).rstrip("/")
            vprint(f"auth discovery: fallback metadata accepted (issuer={origin})")
            return meta, origin
    except httpx.HTTPError:
        vprint("auth discovery: fallback authorization-server metadata fetch failed")
        pass

    raise AuthError(
        f"Cannot discover OAuth auth server for {pds_url!r}.\n"
        f"  Tried:\n"
        f"    • PDS protected-resource metadata (.well-known/oauth-protected-resource)\n"
        f"    • Authorization-server metadata on the PDS\n"
        f"  If this PDS does not support OAuth, you may need to use an App Password.\n"
        f"  See: skycoll init --help"
    )


# ---------------------------------------------------------------------------
# Session persistence
# ---------------------------------------------------------------------------


class Session:
    """Persistent OAuth 2.0 session for an AT Protocol identity.

    Attributes:
        did: The DID this session belongs to.
        handle: The handle this session belongs to.
        access_token: Current access token.
        refresh_token: Current refresh token.
        dpop_key: The ES256 private key used for DPoP proofs.
        dpop_nonce_as: DPoP nonce for the authorisation server.
        dpop_nonce_pds: DPoP nonce for the PDS / resource server.
        pds_endpoint: Origin of the user's PDS.
        access_token_expiry: Unix timestamp when the access token expires.
        refresh_token_expiry: Unix timestamp when the refresh token expires.
        auth_server_url: The authorisation server URL (for refresh).
    """

    def __init__(
        self,
        did: str,
        handle: str,
        access_token: str,
        refresh_token: str,
        dpop_key: ec.EllipticCurvePrivateKey,
        dpop_nonce_as: Optional[str] = None,
        dpop_nonce_pds: Optional[str] = None,
        pds_endpoint: str = "",
        access_token_expiry: float = 0.0,
        refresh_token_expiry: float = 0.0,
        token_expiry: float = 0.0,
        auth_server_url: str = "",
    ) -> None:
        self.did = did
        self.handle = handle
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.dpop_key = dpop_key
        self.dpop_nonce_as = dpop_nonce_as
        self.dpop_nonce_pds = dpop_nonce_pds
        self.pds_endpoint = pds_endpoint
        self.access_token_expiry = access_token_expiry or token_expiry
        self.refresh_token_expiry = refresh_token_expiry
        self.auth_server_url = auth_server_url

    # -- serialisation -------------------------------------------------------

    def _path(self) -> str:
        """Return the session file path for this DID."""
        safe = self.did.replace(":", "_")
        return os.path.join(SESSIONS_DIR, f"{safe}.json")

    def save(self) -> None:
        """Persist this session to disk (mode ``0600``)."""
        os.makedirs(SESSIONS_DIR, exist_ok=True)
        refresh_expiry = self.refresh_token_expiry or _jwt_expiry(self.refresh_token)
        if refresh_expiry > 0:
            self.refresh_token_expiry = refresh_expiry
        data = {
            "did": self.did,
            "handle": self.handle,
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "dpop_private_key_jwk": private_key_to_jwk(self.dpop_key),
            "dpop_nonce_as": self.dpop_nonce_as,
            "dpop_nonce_pds": self.dpop_nonce_pds,
            "pds_endpoint": self.pds_endpoint,
            "access_token_expiry": self.access_token_expiry,
            "refresh_token_expiry": self.refresh_token_expiry,
            "auth_server_url": self.auth_server_url,
        }
        path = self._path()
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        os.chmod(path, 0o600)

    @classmethod
    def load(cls, did: str) -> Optional["Session"]:
        """Load a session for *did* from disk.

        Returns:
            A :class:`Session` instance, or ``None`` if no session exists.
        """
        safe = did.replace(":", "_")
        path = os.path.join(SESSIONS_DIR, f"{safe}.json")
        if not os.path.exists(path):
            return None
        with open(path) as f:
            data = json.load(f)
        access_token_expiry = float(
            data.get("access_token_expiry", data.get("token_expiry", 0.0)) or 0.0
        )
        refresh_token_expiry = float(data.get("refresh_token_expiry", 0.0) or 0.0)
        if refresh_token_expiry <= 0:
            refresh_token_expiry = _jwt_expiry(str(data.get("refresh_token", "")))
        return cls(
            did=data["did"],
            handle=data["handle"],
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            dpop_key=jwk_to_private_key(data["dpop_private_key_jwk"]),
            dpop_nonce_as=data.get("dpop_nonce_as"),
            dpop_nonce_pds=data.get("dpop_nonce_pds"),
            pds_endpoint=data.get("pds_endpoint", ""),
            access_token_expiry=access_token_expiry,
            refresh_token_expiry=refresh_token_expiry,
            auth_server_url=data.get("auth_server_url", ""),
        )


# ---------------------------------------------------------------------------
# OAuth 2.0 flow
# ---------------------------------------------------------------------------

_CALLBACK_RESULT: dict = {}
_CALLBACK_LOCK = threading.Lock()
_CALLBACK_EVENT = threading.Event()


class _CallbackHandler(BaseHTTPRequestHandler):
    """Minimal handler that captures the OAuth callback query string."""

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        with _CALLBACK_LOCK:
            _CALLBACK_RESULT.update(qs)
        _CALLBACK_EVENT.set()
        if "error" in qs:
            body = f"<h1>Auth error: {qs['error'][0]}</h1>".encode()
        else:
            body = b"<h1>Authenticated! You can close this tab.</h1>"
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):  # noqa: ANN001
        pass  # silence request logs


class _LoopbackHandler(BaseHTTPRequestHandler):
    """Loopback handler that only accepts OAuth callback requests."""

    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/callback"):
            _CallbackHandler.do_GET(self)
            return

        body = b"Not found"
        self.send_response(404)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):  # noqa: ANN001
        pass


def _start_callback_server(port: int) -> HTTPServer:
    """Start a loopback HTTP server on *port* for OAuth callbacks."""
    server = HTTPServer(("127.0.0.1", port), _LoopbackHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def _build_localhost_client_id(redirect_uri: str, scope: str) -> str:
    """Build a localhost-development client_id for atproto OAuth.

    Per spec, localhost-development client_ids must use the origin
    ``http://localhost`` (no explicit port), with optional query params.
    We include redirect_uri and scope so auth servers can build virtual
    client metadata without fetching from loopback addresses.
    """
    query = urlencode({"redirect_uri": redirect_uri, "scope": scope})
    return f"http://localhost/?{query}"


def _extract_nonce_from_response(resp: httpx.Response) -> Optional[str]:
    """Extract the ``DPoP-Nonce`` header from an HTTP response, if present."""
    return resp.headers.get("DPoP-Nonce")


def _do_token_request(
    token_url: str,
    body: dict,
    dpop_key: ec.EllipticCurvePrivateKey,
    dpop_nonce: Optional[str] = None,
) -> tuple[dict, Optional[str]]:
    """Make a token request with a DPoP proof attached.

    Returns:
        Tuple of (parsed JSON response, new DPoP-Nonce value).
    """
    nonce = dpop_nonce
    for attempt in range(2):
        proof = build_dpop_proof(dpop_key, "POST", token_url, nonce=nonce)
        headers = {"DPoP": proof, "Content-Type": "application/x-www-form-urlencoded"}
        vprint(f"token request attempt={attempt + 1} nonce={'set' if nonce else 'none'}")
        try:
            resp = httpx.post(
                token_url,
                data=body,
                headers=headers,
                follow_redirects=True,
                timeout=30,
            )
        except httpx.HTTPError as exc:
            raise NetworkError(f"could not reach auth server token endpoint: {exc}") from exc
        new_nonce = _extract_nonce_from_response(resp)
        vprint(f"token response status={resp.status_code} nonce_header={'present' if new_nonce else 'absent'}")

        if resp.status_code == 200:
            return resp.json(), (new_nonce or nonce)

        # AT Protocol OAuth servers can require a fresh DPoP nonce and
        # signal this with HTTP 400 + DPoP-Nonce. Retry once with it.
        if resp.status_code == 400 and new_nonce and new_nonce != nonce:
            vprint("token request: retrying with server-provided DPoP nonce")
            nonce = new_nonce
            continue

        raise AuthError(
            f"Token request failed (HTTP {resp.status_code}): {resp.text}"
        )

    raise AuthError("Token request failed after DPoP nonce retry")


def authenticate(handle: str) -> Session:
    """Run the full AT Protocol OAuth 2.0 public-client flow.

    Discovers the PDS and authorisation server via
    ``/.well-known/oauth-protected-resource``, uses PAR (Pushed
    Authorization Request), launches a browser for user consent,
    exchanges the code, and persists the session.

    Args:
        handle: The user's Bluesky handle.

    Returns:
        An authenticated :class:`Session`.

    Raises:
        RuntimeError: On any step failure.
    """
    identity = resolve(handle)
    did = identity["did"]
    pds = identity["pds"]
    vprint(f"auth start handle={handle} did={did} pds={pds}")

    existing = Session.load(did)
    if existing is not None:
        try:
            return _maybe_refresh(existing)
        except AuthError:
            vprint(f"cached session for {handle} is expired; continuing with fresh login")

    # --- PKCE ---
    code_verifier, code_challenge = generate_pkce()

    # --- DPoP key ---
    dpop_key = generate_dpop_keypair()

    # --- Discover auth server ---
    meta, auth_server_origin = discover_auth_server(pds)
    auth_url_base = meta["authorization_endpoint"]
    token_url = meta["token_endpoint"]
    par_url = meta.get("pushed_authorization_request_endpoint")
    vprint(
        "auth server metadata: "
        f"origin={auth_server_origin} auth_endpoint={auth_url_base} "
        f"token_endpoint={token_url} par_endpoint={par_url or 'none'}"
    )

    # --- Loopback redirect URI ---
    port = secrets.randbelow(65535 - 49152) + 49152
    redirect_uri = f"http://127.0.0.1:{port}/callback"
    client_id = _build_localhost_client_id(redirect_uri, SCOPES)
    vprint(f"oauth client_id={client_id}")

    # --- Start temporary server ---
    server = _start_callback_server(port)
    try:
        # --- PAR (Pushed Authorization Request) ---
        # AT Protocol requires PAR. If the auth server publishes a
        # pushed_authorization_request_endpoint, we POST our request
        # parameters there first and receive a request_uri.
        state = secrets.token_urlsafe(16)
        auth_params = {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": SCOPES,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
            "state": state,
            "login_hint": handle,
        }

        if par_url:
            par_body = dict(auth_params)
            dpop_nonce: Optional[str] = None

            # First PAR attempt (may need DPoP nonce from 400 response)
            proof = build_dpop_proof(dpop_key, "POST", par_url)
            par_headers = {
                "DPoP": proof,
                "Content-Type": "application/x-www-form-urlencoded",
            }
            try:
                resp = httpx.post(
                    par_url,
                    data=par_body,
                    headers=par_headers,
                    follow_redirects=True,
                    timeout=30,
                )
            except httpx.HTTPError as exc:
                raise NetworkError(f"could not reach auth server PAR endpoint: {exc}") from exc
            vprint(f"PAR response status={resp.status_code}")

            # If the server requires a DPoP nonce, retry once with it.
            new_nonce = _extract_nonce_from_response(resp)
            if new_nonce:
                dpop_nonce = new_nonce
                vprint("PAR response includes DPoP-Nonce")
            if resp.status_code == 400:
                if not new_nonce:
                    new_nonce = resp.headers.get("DPoP-Nonce")
                if new_nonce:
                    dpop_nonce = new_nonce
                    vprint("PAR retrying with server-provided DPoP nonce")
                    proof = build_dpop_proof(dpop_key, "POST", par_url, nonce=dpop_nonce)
                    par_headers["DPoP"] = proof
                    try:
                        resp = httpx.post(
                            par_url,
                            data=par_body,
                            headers=par_headers,
                            follow_redirects=True,
                            timeout=30,
                        )
                    except httpx.HTTPError as exc:
                        raise NetworkError(f"could not reach auth server PAR endpoint: {exc}") from exc
                    vprint(f"PAR retry response status={resp.status_code}")
                    new_nonce = _extract_nonce_from_response(resp)
                    if new_nonce:
                        dpop_nonce = new_nonce

            if resp.status_code not in (200, 201):
                raise AuthError(
                    f"PAR request failed (HTTP {resp.status_code}): {resp.text}"
                )

            par_data = resp.json()
            request_uri = par_data.get("request_uri")
            if not request_uri:
                raise AuthError(
                    f"PAR response missing request_uri: {par_data}"
                )
            vprint("PAR succeeded; received request_uri")

            # Extract DPoP nonce from PAR response for later use
            if not dpop_nonce:
                dpop_nonce = new_nonce

            # Build authorization URL with request_uri
            auth_url = (
                f"{auth_url_base}?client_id={quote(client_id, safe='')}"
                f"&request_uri={quote(request_uri, safe='')}"
            )
        else:
            # Fallback: auth server doesn't require PAR, build URL directly
            vprint("PAR endpoint missing; falling back to direct authorization URL")
            auth_url = f"{auth_url_base}?{urlencode(auth_params)}"
            dpop_nonce = None

        info(f"Opening browser for {handle} …")
        info(f"If the browser doesn't open, visit:\n  {auth_url}")
        webbrowser.open(auth_url)

        # --- Wait for callback ---
        if not _CALLBACK_EVENT.wait(timeout=300):
            raise AuthError("Timed out waiting for OAuth callback")
        with _CALLBACK_LOCK:
            result = dict(_CALLBACK_RESULT)
            _CALLBACK_RESULT.clear()
        _CALLBACK_EVENT.clear()

        if "error" in result:
            raise AuthError(
                f"OAuth error: {result.get('error', ['unknown'])[0]} — "
                f"{result.get('error_description', [''])[0]}"
            )

        code = result["code"][0]

        # --- Exchange code for tokens ---
        body = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "code_verifier": code_verifier,
        }
        token_data, new_nonce = _do_token_request(token_url, body, dpop_key, dpop_nonce)
        dpop_nonce = new_nonce
        vprint("token exchange succeeded")

        # --- Verify sub matches expected DID ---
        returned_sub = token_data.get("sub", "")
        if returned_sub and returned_sub != did:
            raise AuthError(
                f"Token sub mismatch: expected {did}, got {returned_sub}"
            )

        session = Session(
            did=did,
            handle=identity["handle"],
            access_token=token_data["access_token"],
            refresh_token=token_data.get("refresh_token", ""),
            dpop_key=dpop_key,
            dpop_nonce_as=dpop_nonce,
            dpop_nonce_pds=None,
            pds_endpoint=pds,
            access_token_expiry=time.time() + token_data.get("expires_in", 3600),
            refresh_token_expiry=_jwt_expiry(token_data.get("refresh_token", "")),
            auth_server_url=token_url,
        )
        session.save()
        return session

    finally:
        server.shutdown()


def _refresh_session(session: Session) -> Session:
    """Refresh a session's access token using its refresh token.

    Returns the updated session on success.

    Raises:
        AuthError: If the session cannot be refreshed (missing/invalid token,
            revoked session, or protocol mismatch).
        NetworkError: On transport-level failures while contacting the auth server.
        ParseError: If the token endpoint returns malformed JSON.
    """
    if not session.refresh_token:
        raise AuthError(
            f"Session expired for {session.handle} — run: skycoll auth login {session.handle}."
        )

    token_url = session.auth_server_url
    if not token_url:
        raise AuthError(
            f"Session expired for {session.handle} — run: skycoll auth login {session.handle}."
        )

    body = {
        "grant_type": "refresh_token",
        "refresh_token": session.refresh_token,
    }

    nonce = session.dpop_nonce_as
    resp = None
    for attempt in range(2):
        proof = build_dpop_proof(
            session.dpop_key,
            "POST",
            token_url,
            nonce=nonce,
        )
        headers = {"DPoP": proof, "Content-Type": "application/x-www-form-urlencoded"}

        try:
            resp = httpx.post(token_url, data=body, headers=headers, follow_redirects=True, timeout=30)
        except httpx.HTTPError as exc:
            raise NetworkError(f"could not reach auth server token endpoint: {exc}") from exc
        vprint(f"refresh token response status={resp.status_code}")

        new_nonce = _extract_nonce_from_response(resp)
        if new_nonce:
            session.dpop_nonce_as = new_nonce

        if resp.status_code == 400 and new_nonce and new_nonce != nonce and attempt == 0:
            nonce = new_nonce
            continue

        break

    if resp is None:
        raise NetworkError(f"refresh token request failed for {session.handle}: no response")

    if 400 <= resp.status_code < 500:
        raise AuthError(
            f"Session expired for {session.handle} — run: skycoll auth login {session.handle}."
        )
    if resp.status_code != 200:
        raise NetworkError(
            f"refresh token request failed for {session.handle}: HTTP {resp.status_code}"
        )

    try:
        data = resp.json()
    except ValueError as exc:
        raise ParseError(f"invalid refresh token JSON for {session.handle}") from exc

    returned_sub = data.get("sub", "")
    if returned_sub and returned_sub != session.did:
        raise AuthError(
            f"Session expired for {session.handle} — run: skycoll auth login {session.handle}."
        )

    access_token = data.get("access_token", "")
    if not access_token:
        raise ParseError(f"refresh response missing access_token for {session.handle}")

    session.access_token = access_token
    maybe_refresh_token = data.get("refresh_token")
    if maybe_refresh_token:
        session.refresh_token = maybe_refresh_token

    session.access_token_expiry = time.time() + data.get("expires_in", 3600)
    parsed_refresh_expiry = _jwt_expiry(session.refresh_token)
    if parsed_refresh_expiry > 0:
        session.refresh_token_expiry = parsed_refresh_expiry
    session.save()
    return session


def _maybe_refresh(session: Session) -> Session:
    """Refresh the access token if needed and return a valid session.

    Access tokens are proactively refreshed when they have <=60 seconds left.
    """
    if time.time() < session.access_token_expiry - 60:
        return session

    return _refresh_session(session)


def get_authenticated_session(handle: str) -> Session:
    """Return an authenticated session for *handle*, authenticating if needed.

    Convenience wrapper around :func:`authenticate` and :func:`Session.load`.

    Args:
        handle: The user's Bluesky handle.

    Returns:
        A valid :class:`Session` with a non-expired access token.
    """
    identity = resolve(handle)
    existing = Session.load(identity["did"])
    if existing is not None:
        return _maybe_refresh(existing)
    # No valid session — kick off full flow
    return authenticate(handle)


def get_any_session() -> Optional[Session]:
    """Return the first valid cached session from ``~/.skycoll/sessions``.

    Sessions are scanned from disk and the first valid (or silently refreshed)
    session is returned. This is intended for read-only commands where any
    authenticated account can be used to read public data.

    Returns:
        A valid :class:`Session`, or ``None`` if no valid cached session exists.
    """
    if not os.path.isdir(SESSIONS_DIR):
        return None

    for entry in sorted(os.listdir(SESSIONS_DIR)):
        if not entry.endswith(".json"):
            continue

        path = os.path.join(SESSIONS_DIR, entry)
        try:
            with open(path) as f:
                data = json.load(f)
            did = data.get("did", "")
            if not did:
                continue

            session = Session.load(did)
            if session is None:
                continue

            try:
                refreshed = _maybe_refresh(session)
            except AuthError as exc:
                vprint(f"skipping expired session {session.handle} ({session.did}): {exc}")
                continue

            vprint(f"using cached session for {refreshed.handle} ({refreshed.did})")
            return refreshed
        except (OSError, ValueError, TypeError, ParseError) as exc:
            vprint(f"skipping invalid session file {path}: {exc}")
            continue

    return None


def login(handle: str) -> Session:
    """Explicitly authenticate and persist a session for *handle*."""
    return authenticate(handle)


def logout(handle: str) -> tuple[str, str]:
    """Delete the saved session for *handle*.

    Returns:
        Tuple of (resolved_handle, did).
    """
    identity = resolve(handle)
    did = identity["did"]
    safe = did.replace(":", "_")
    path = os.path.join(SESSIONS_DIR, f"{safe}.json")
    if not os.path.exists(path):
        raise NotFoundError(f"no saved session for {identity['handle']} ({did})")
    try:
        os.remove(path)
    except OSError as exc:
        raise AuthError(f"failed to remove session file: {exc}") from exc
    return identity["handle"], did


def list_saved_sessions() -> list[dict]:
    """List saved sessions from disk with refresh-aware validity metadata."""
    if not os.path.isdir(SESSIONS_DIR):
        return []

    out: list[dict] = []
    for entry in sorted(os.listdir(SESSIONS_DIR)):
        if not entry.endswith(".json"):
            continue
        path = os.path.join(SESSIONS_DIR, entry)
        try:
            with open(path) as f:
                data = json.load(f)
            did = data.get("did", "")
            if not did:
                raise ParseError(f"missing did in session file {path}")
            session = Session.load(did)
            if session is None:
                continue

            refreshed = False
            access_expired = time.time() >= session.access_token_expiry
            status = "valid"
            dead_reason = ""

            if access_expired:
                try:
                    session = _refresh_session(session)
                    refreshed = True
                except AuthError as exc:
                    status = "dead"
                    dead_reason = str(exc)
                except (NetworkError, ParseError):
                    # Keep session visible; refresh can be retried later.
                    if access_expired:
                        status = "access_expired"

            refresh_delta = session.refresh_token_expiry - time.time()
            out.append(
                {
                    "handle": session.handle,
                    "did": session.did,
                    "access_token_expiry": float(session.access_token_expiry or 0.0),
                    "refresh_token_expiry": float(session.refresh_token_expiry or 0.0),
                    "refresh_token_expires_in": refresh_delta,
                    "access_token_expired": access_expired,
                    "refreshed": refreshed,
                    "status": status,
                    "is_valid": status == "valid",
                    "error": dead_reason,
                }
            )
        except (OSError, json.JSONDecodeError, ParseError, ValueError, TypeError) as exc:
            raise ParseError(f"invalid session file {path}: {exc}") from exc

    return out


def make_authenticated_request(
    session: Session,
    method: str,
    path: str,
    appview: Optional[str] = None,
    **kwargs,
) -> httpx.Response:
    """Make an authenticated request to the user's PDS with DPoP.

    Args:
        session: An active session.
        method: HTTP method (``GET``, ``POST``, etc.).
        path: XRPC path or relative URL on the PDS (e.g. ``/xrpc/com.atproto.identity.resolveHandle``).
        appview: Optional AppView service DID for the ``atproto-proxy`` header.
        **kwargs: Additional keyword arguments forwarded to :func:`httpx.request`.

    Returns:
        The HTTP response.

    Raises:
        RuntimeError: If the request fails after retries (including 429).
    """
    session = _maybe_refresh(session)
    url = path if path.startswith("http") else f"{session.pds_endpoint}{path}"
    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        vprint(
            f"request attempt={attempt} method={method.upper()} url={url} "
            f"appview={appview or 'none'}"
        )
        proof = build_dpop_proof(
            session.dpop_key,
            method,
            url,
            access_token=session.access_token,
            nonce=session.dpop_nonce_pds,
        )
        headers = dict(kwargs.pop("headers", {}))
        headers["Authorization"] = f"DPoP {session.access_token}"
        headers["DPoP"] = proof
        if appview:
            headers["atproto-proxy"] = appview

        try:
            resp = httpx.request(method, url, headers=headers, timeout=30, **kwargs)
        except httpx.HTTPError as exc:
            raise NetworkError(f"could not reach {url}: {exc}") from exc
        vprint(f"response status={resp.status_code} url={url}")

        new_nonce = _extract_nonce_from_response(resp)
        if new_nonce:
            session.dpop_nonce_pds = new_nonce
            session.save()
            vprint("updated PDS DPoP nonce from response")

        if resp.status_code == 429:
            wait = 2 ** attempt
            warn(f"Rate-limited, retrying in {wait}s …")
            time.sleep(wait)
            continue

        if resp.status_code == 401:
            # First retry with server-provided PDS DPoP nonce if present.
            if new_nonce:
                continue

            # Otherwise access token might be expired/revoked; refresh once.
            session = _refresh_session(session)
            continue

        return resp

    raise RateLimitError(f"request to {url} failed after {max_attempts} attempts")
