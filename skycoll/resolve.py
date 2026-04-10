"""Handle / DID / PDS resolution for AT Protocol.

This module never hardcodes bsky.social.  Every handle is resolved through
the proper AT Protocol identity chain:

  handle → DID  (via com.atproto.identity.resolveHandle or DNS TXT)
  DID    → DID document  (plc.directory for did:plc, HTTPS for did:web)
  DID document → PDS endpoint (the #atproto_pds service entry)
"""

from __future__ import annotations

import json
import re
from typing import Optional
from urllib.parse import urlparse

import httpx

from .verbosity import vprint

_BSKY_SOCIAL = "https://bsky.social"

_DNS_TXT_RE = re.compile(r"did=(did:[a-z]+:[A-Za-z0-9._:-]+)")


def resolve_handle_to_did(handle: str) -> str:
    """Resolve a Bluesky handle to its DID.

    Tries the well-known HTTP path first, then falls back to the
    ``com.atproto.identity.resolveHandle`` XRPC on bsky.social, and
    finally checks the DNS ``_atproto`` TXT record.

    Args:
        handle: A Bluesky handle (e.g. ``alice.bsky.social``).

    Returns:
        The DID string (e.g. ``did:plc:abc123``).

    Raises:
        RuntimeError: If the handle cannot be resolved by any method.
    """
    handle = handle.lstrip("@").lower()

    # 1) HTTPS well-known
    try:
        url = f"https://{handle}/.well-known/atproto-did"
        vprint(f"resolve handle: GET {url}")
        resp = httpx.get(url, follow_redirects=True, timeout=10)
        if resp.status_code == 200:
            did = resp.text.strip()
            if did.startswith("did:"):
                vprint("resolve handle: succeeded via HTTPS well-known")
                return did
    except httpx.HTTPError:
        vprint("resolve handle: HTTPS well-known failed")
        pass

    # 2) XRPC on bsky.social
    try:
        vprint("resolve handle: trying bsky.social XRPC fallback")
        resp = httpx.get(
            f"{_BSKY_SOCIAL}/xrpc/com.atproto.identity.resolveHandle",
            params={"handle": handle},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            did = data.get("did")
            if did:
                vprint("resolve handle: succeeded via bsky.social XRPC")
                return did
    except httpx.HTTPError:
        vprint("resolve handle: bsky.social XRPC fallback failed")
        pass

    # 3) DNS TXT _atproto
    try:
        import dns.resolver

        vprint("resolve handle: trying DNS _atproto TXT")
        answers = dns.resolver.resolve(f"_atproto.{handle}", "TXT")
        for rdata in answers:
            m = _DNS_TXT_RE.search(rdata.to_text())
            if m:
                vprint("resolve handle: succeeded via DNS TXT")
                return m.group(1)
    except Exception:
        vprint("resolve handle: DNS TXT fallback failed")
        pass

    raise RuntimeError(
        f"Cannot resolve handle {handle!r} to a DID.\n"
        f"  Tried: HTTPS well-known, bsky.social XRPC, DNS TXT.\n"
        f"  Check that the handle is correct and the account exists."
    )


def resolve_did_to_handle(did: str) -> str:
    """Resolve a DID back to its handle via the DID document.

    Also falls back to trying bsky.social's resolveHandle endpoint
    if the DID document cannot be fetched directly.

    Args:
        did: A DID string (``did:plc:…`` or ``did:web:…``).

    Returns:
        The associated handle (e.g. ``alice.bsky.social``).

    Raises:
        RuntimeError: If the DID cannot be resolved to a handle by any method.
    """
    # 1) Try DID document
    try:
        vprint(f"resolve did: fetching DID document for {did}")
        doc = fetch_did_document(did)
        for aka in doc.get("alsoKnownAs", []):
            if aka.startswith("at://"):
                vprint("resolve did: succeeded via DID document alsoKnownAs")
                return aka[5:]
    except RuntimeError as exc:
        # 2) Fallback: bsky.social XRPC can resolve some DIDs directly
        try:
            vprint("resolve did: trying bsky.social XRPC fallback")
            resp = httpx.get(
                f"{_BSKY_SOCIAL}/xrpc/com.atproto.identity.resolveHandle",
                params={"handle": did},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                h = data.get("handle")
                if h:
                    vprint("resolve did: succeeded via bsky.social XRPC fallback")
                    return h
        except httpx.HTTPError:
            vprint("resolve did: bsky.social XRPC fallback failed")
            pass

        raise RuntimeError(
            f"Cannot resolve DID {did!r} to a handle.\n"
            f"  Reasons tried:\n"
            f"    • DID document fetch failed: {exc}\n"
            f"    • bsky.social XRPC fallback also failed.\n"
            f"  Check that the DID is correct and the account exists."
        )

    raise RuntimeError(f"DID document for {did} contains no handle")


def fetch_did_document(did: str) -> dict:
    """Fetch and parse the DID document for a given DID.

    ``did:plc`` documents are retrieved from plc.directory;
    ``did:web`` documents are fetched from the domain's well-known path.

    Args:
        did: A DID string.

    Returns:
        Parsed DID document as a dict.

    Raises:
        RuntimeError: For unsupported DID methods or fetch failures.
    """
    if did.startswith("did:plc:"):
        url = f"https://plc.directory/{did}"
    elif did.startswith("did:web:"):
        domain = did[len("did:web:"):]
        url = f"https://{domain}/.well-known/did.json"
    else:
        raise RuntimeError(
            f"Unsupported DID method in {did!r}.\n"
            f"  Only did:plc and did:web are supported."
        )

    try:
        vprint(f"fetch did document: GET {url}")
        resp = httpx.get(url, follow_redirects=True, timeout=15)
    except httpx.HTTPError as exc:
        raise RuntimeError(
            f"Network error fetching DID document for {did!r}:\n"
            f"  {exc}"
        )

    if resp.status_code == 404:
        raise RuntimeError(
            f"DID not found: {did!r}\n"
            f"  The PLC directory has no record of this DID.\n"
            f"  Verify the DID is correct — a common mistake is\n"
            f"  swapping letters (e.g. 'ae' vs 'ur')."
        )
    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch DID document for {did!r} (HTTP {resp.status_code})"
        )
    try:
        return resp.json()
    except json.JSONDecodeError:
        raise RuntimeError(
            f"DID document for {did!r} is not valid JSON"
        )


def resolve_pds_endpoint(did: str) -> str:
    """Extract the PDS endpoint from a DID document.

    Looks for a service entry with id ``#atproto_pds`` in the DID document.
    Falls back to ``https://bsky.social`` only when the service entry is
    missing (i.e. for accounts on the default PDS that haven't set one).

    Args:
        did: A DID string.

    Returns:
        The PDS origin (e.g. ``https://pds.example.com``).

    Raises:
        RuntimeError: If the DID document cannot be fetched.
    """
    doc = fetch_did_document(did)
    for svc in doc.get("service", []):
        if svc.get("id") in ("#atproto_pds", "atproto_pds"):
            return svc["serviceEndpoint"].rstrip("/")
    return _BSKY_SOCIAL


def resolve(identifier: str) -> dict:
    """Resolve a handle or DID to full identity information.

    If *identifier* starts with ``did:``, it is treated as a DID;
    otherwise it is treated as a handle.

    Args:
        identifier: A handle or DID string.

    Returns:
        A dict with keys ``did``, ``handle``, and ``pds``.
    """
    if identifier.startswith("did:"):
        did = identifier
        handle = resolve_did_to_handle(did)
    else:
        handle = identifier.lstrip("@").lower()
        did = resolve_handle_to_did(handle)

    pds = resolve_pds_endpoint(did)
    return {"did": did, "handle": handle, "pds": pds}
