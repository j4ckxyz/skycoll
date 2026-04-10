"""AppView configuration — maps named AppViews to service DIDs.

The ``atproto-proxy`` HTTP header routes API requests through a specific
AppView service.  Named AppViews are convenience aliases; any raw
``did:xxx#fragment`` string is also accepted.
"""

from __future__ import annotations

BUILTIN_APPVIEWS: dict[str, dict[str, str]] = {
    "bluesky": {
        "did": "did:web:api.bsky.app#bsky_appview",
        "description": "Bluesky official AppView (default)",
    },
    "blacksky": {
        "did": "did:web:api.blacksky.community#bsky_appview",
        "description": "Blacksky community AppView",
    },
}


def resolve_appview(name: str | None) -> str | None:
    """Resolve a ``--appview`` flag value to a service DID string.

    * If *name* is ``None`` or empty, returns ``None`` (no proxy header).
    * If *name* matches a built-in name (case-insensitive), returns its DID.
    * Otherwise treats *name* as a raw DID+fragment string and returns it as-is.

    Args:
        name: The ``--appview`` flag value.

    Returns:
        A service DID string, or ``None`` if no proxy is requested.
    """
    if not name:
        return None
    key = name.lower()
    if key in BUILTIN_APPVIEWS:
        return BUILTIN_APPVIEWS[key]["did"]
    return name


def list_appviews() -> list[dict[str, str]]:
    """Return the built-in AppView definitions.

    Returns:
        List of dicts with ``name``, ``did``, and ``description`` keys.
    """
    return [
        {"name": k, "did": v["did"], "description": v["description"]}
        for k, v in BUILTIN_APPVIEWS.items()
    ]