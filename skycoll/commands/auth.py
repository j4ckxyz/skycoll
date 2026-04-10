"""auth sub-command — explicit session management."""

from __future__ import annotations

import time

from skycoll.auth import login, logout, list_saved_sessions
from skycoll.errors import ParseError, SkycollError
from skycoll.output import info, ok


def _format_future(seconds: float) -> str:
    if seconds < 60:
        return f"expires in {int(seconds)}s"
    if seconds < 3600:
        return f"expires in {int(seconds // 60)}m"
    if seconds < 86400:
        return f"expires in {int(seconds // 3600)}h"
    return f"expires in {int(seconds // 86400)}d"


def _format_past(seconds: float) -> str:
    if seconds < 60:
        return f"expired {int(seconds)}s ago"
    if seconds < 3600:
        return f"expired {int(seconds // 60)}m ago"
    if seconds < 86400:
        return f"expired {int(seconds // 3600)}h ago"
    return f"expired {int(seconds // 86400)}d ago"


def run_login(handle: str) -> None:
    try:
        session = login(handle)
        ok(f"Logged in as {session.handle} ({session.did})")
    except SkycollError:
        raise
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid auth response for login '{handle}': {exc}") from exc
    except Exception as exc:
        raise ParseError(f"unexpected auth login error for '{handle}': {exc}") from exc


def run_logout(handle: str) -> None:
    try:
        resolved_handle, did = logout(handle)
        ok(f"Logged out {resolved_handle} ({did})")
    except SkycollError:
        raise
    except (TypeError, ValueError) as exc:
        raise ParseError(f"invalid auth response for logout '{handle}': {exc}") from exc
    except Exception as exc:
        raise ParseError(f"unexpected auth logout error for '{handle}': {exc}") from exc


def run_list() -> None:
    try:
        sessions = list_saved_sessions()
        if not sessions:
            info("Saved sessions:\n  (none)")
            return

        info("Saved sessions:")
        now = time.time()
        for session in sessions:
            handle = session.get("handle", "")
            did = session.get("did", "")
            expiry = float(session.get("token_expiry", 0.0) or 0.0)
            delta = expiry - now
            if delta >= 0:
                status = _format_future(delta)
                info(f"  ✓  {handle}  ({did})  {status}")
            else:
                status = _format_past(abs(delta))
                info(f"  ✗  {handle}  ({did})  {status}")
    except SkycollError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid saved session data: {exc}") from exc
    except Exception as exc:
        raise ParseError(f"unexpected auth list error: {exc}") from exc
