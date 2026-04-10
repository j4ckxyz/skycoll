"""Typed skycoll exceptions for user-friendly CLI error handling."""

from __future__ import annotations


class SkycollError(RuntimeError):
    """Base class for all expected skycoll errors."""

    label = "Error"


class AuthError(SkycollError):
    """Authentication/session related error."""

    label = "Auth error"


class NetworkError(SkycollError):
    """Network request related error."""

    label = "Network error"


class NotFoundError(SkycollError):
    """Missing local file or remote resource."""

    label = "Not found"


class RateLimitError(SkycollError):
    """Rate limiting condition."""

    label = "Rate limited"


class ParseError(SkycollError):
    """Parse/serialization related error."""

    label = "Parse error"
