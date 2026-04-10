"""Simple global verbosity controls for CLI debug output."""

from __future__ import annotations

import os

_TRUE_VALUES = {"1", "true", "yes", "on", "debug"}
_VERBOSE = os.environ.get("SKYCOLL_VERBOSE", "").strip().lower() in _TRUE_VALUES


def set_verbose(enabled: bool) -> None:
    """Enable or disable verbose output globally."""
    global _VERBOSE
    _VERBOSE = bool(enabled)


def is_verbose() -> bool:
    """Return whether verbose output is enabled."""
    return _VERBOSE


def vprint(*args, **kwargs) -> None:
    """Print a message only when verbose mode is enabled."""
    if _VERBOSE:
        print("[verbose]", *args, **kwargs)
