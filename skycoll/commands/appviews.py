"""appviews sub-command — list built-in AppView service DIDs."""

from __future__ import annotations

from skycoll.errors import ParseError, SkycollError
from skycoll.appview import list_appviews
from skycoll.output import info


def run() -> None:
    """Print the built-in AppView names and their service DIDs."""
    try:
        views = list_appviews()
        if not isinstance(views, list):
            raise ParseError("invalid appview registry payload")
        info("Built-in AppViews:\n")
        for view in views:
            name = view.get("name", "") if isinstance(view, dict) else ""
            did = view.get("did", "") if isinstance(view, dict) else ""
            description = view.get("description", "") if isinstance(view, dict) else ""
            info(f"  {name:<12} {did}")
            info(f"              {description}")
        info("\nYou can also pass a raw DID+fragment string to --appview, e.g.:")
        info("  --appview did:web:custom.example#bsky_appview")
    except SkycollError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid appview data: {exc}") from exc
    except Exception as exc:
        raise ParseError(f"unexpected appviews error: {exc}") from exc
