"""plc sub-command — fetch PLC directory operation log for a DID."""

from __future__ import annotations

import json
import os

import httpx


def _fetch_plc_log(did: str) -> list[dict]:
    """Fetch the full operation log from plc.directory.

    Args:
        did: A ``did:plc`` DID.

    Returns:
        List of operation dicts (newest first).
    """
    url = f"https://plc.directory/{did}/log"
    resp = httpx.get(url, follow_redirects=True, timeout=15)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to fetch PLC log for {did}: HTTP {resp.status_code}")
    return resp.json()


def _audit_summary(ops: list[dict]) -> str:
    """Produce a human-readable audit summary from an operation log.

    Args:
        ops: List of operation dicts.

    Returns:
        Multi-line summary string.
    """
    if not ops:
        return "No operations found."

    lines = []
    lines.append(f"Operations: {len(ops)}")

    # Current handle (from the latest op that sets one)
    current_handle = None
    current_pds = None
    first_created = None

    for op in reversed(ops):
        if first_created is None and op.get("createdAt"):
            first_created = op["createdAt"]
        handle = op.get("handle") or (op.get("alsoKnownAs", [None]) or [None])[0] if not current_handle else None
        if handle and not current_handle:
            # Strip at:// prefix if present
            if isinstance(handle, str) and handle.startswith("at://"):
                handle = handle[5:]
            current_handle = handle
        pds = op.get("pds") or op.get("service", {}).get("serviceEndpoint") if not current_pds else None
        if pds and not current_pds:
            current_pds = pds

    # Walk in chronological order (oldest first) to get the latest state
    for op in ops:
        if op.get("createdAt"):
            first_created = op["createdAt"]
        ako = op.get("alsoKnownAs") or op.get("handle")
        if isinstance(ako, list) and ako:
            current_handle = ako[0]
            if isinstance(current_handle, str) and current_handle.startswith("at://"):
                current_handle = current_handle[5:]
        elif isinstance(ako, str):
            current_handle = ako
        svc = op.get("service")
        if isinstance(svc, dict) and "serviceEndpoint" in svc:
            current_pds = svc["serviceEndpoint"]

    if current_handle:
        lines.append(f"Current handle: {current_handle}")
    if current_pds:
        lines.append(f"Current PDS: {current_pds}")
    if first_created:
        lines.append(f"First operation: {first_created}")

    return "\n".join(lines)


def run(did: str, audit: bool = False) -> None:
    """Fetch the PLC directory operation log for *did* and write it to ``<did>.plc``.

    Args:
        did: A ``did:plc`` DID.
        audit: If ``True``, also print a human-readable summary.
    """
    print(f"Fetching PLC operation log for {did} …")
    ops = _fetch_plc_log(did)
    print(f"  {len(ops)} operations found")

    # Write JSON log
    safe_did = did.replace(":", "_")
    path = os.path.join(os.getcwd(), f"{safe_did}.plc")
    with open(path, "w") as f:
        json.dump(ops, f, indent=2, ensure_ascii=False)
    print(f"Wrote {path}")

    if audit:
        print(f"\n{_audit_summary(ops)}")