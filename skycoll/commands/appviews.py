"""appviews sub-command — list built-in AppView service DIDs."""

from __future__ import annotations

from skycoll.appview import list_appviews


def run() -> None:
    """Print the built-in AppView names and their service DIDs."""
    views = list_appviews()
    print("Built-in AppViews:\n")
    for v in views:
        print(f"  {v['name']:<12} {v['did']}")
        print(f"              {v['description']}")
    print("\nYou can also pass a raw DID+fragment string to --appview, e.g.:")
    print("  --appview did:web:custom.example#bsky_appview")