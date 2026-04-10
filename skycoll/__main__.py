#!/usr/bin/env python3
"""skycoll — Bluesky/AT Protocol social-graph CLI (twecoll equivalent).

Usage:
    skycoll [-v] resolve <handle-or-did>
    skycoll [-v] init <handle> [--lists] [--labels] [--appview NAME] [--constellation HOST]
    skycoll [-v] fetch <handle>
    skycoll [-v] posts <handle> [--car] [--appview NAME]
    skycoll [-v] likes <handle> [-p|--purge] [--appview NAME]
    skycoll [-v] threads <handle>
    skycoll [-v] edgelist <handle> [--constellation HOST]
    skycoll [-v] sync <handle>
    skycoll [-v] backlinks <handle> --constellation HOST
    skycoll [-v] plc <did> [--audit]
    skycoll [-v] appviews
    skycoll [-v] firehose [--did DID] [--handle HANDLE] [--relay URL] [--limit N]
"""

from __future__ import annotations

import argparse
import sys

from skycoll import __version__
from skycoll.verbosity import set_verbose


def main() -> None:
    """Entry point for the skycoll CLI."""
    parser = argparse.ArgumentParser(
        prog="skycoll",
        description="skycoll — Bluesky/AT Protocol social-graph CLI",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose debug logging")

    sub = parser.add_subparsers(dest="command", help="Available commands")

    # resolve
    p_resolve = sub.add_parser("resolve", help="Resolve a handle to a DID or vice versa")
    p_resolve.add_argument("identifier", help="A Bluesky handle or DID")

    # init
    p_init = sub.add_parser("init", help="Fetch profile + follows/followers, write .dat and avatars")
    p_init.add_argument("handle", help="Your Bluesky handle")
    p_init.add_argument("--lists", action="store_true", help="Fetch user lists (app.bsky.graph.getLists)")
    p_init.add_argument("--labels", action="store_true", help="Include self-labels and server labels in .dat")
    p_init.add_argument("--appview", default=None, help="AppView to route through (bluesky, blacksky, or DID)")
    p_init.add_argument("--constellation", default=None, help="Constellation backlinks host (e.g. https://constellation.example.com)")

    # fetch
    p_fetch = sub.add_parser("fetch", help="Fetch follows of every handle in .dat, write to fdat/")
    p_fetch.add_argument("handle", help="The handle used in `skycoll init`")

    # posts
    p_posts = sub.add_parser("posts", help="Download posts via feed (or --car for full repo sync), write .twt")
    p_posts.add_argument("handle", help="Your Bluesky handle")
    p_posts.add_argument("--car", action="store_true", help="Use full CAR repo sync instead of paginated feed (slower but complete)")
    p_posts.add_argument("--appview", default=None, help="AppView to route through")

    # likes
    p_likes = sub.add_parser("likes", help="Download all likes, write .fav")
    p_likes.add_argument("handle", help="Your Bluesky handle")
    p_likes.add_argument("-p", "--purge", action="store_true", help="Purge (delete) all likes")
    p_likes.add_argument("--appview", default=None, help="AppView to route through")

    # threads
    p_threads = sub.add_parser("threads", help="Reconstruct reply threads from .twt")
    p_threads.add_argument("handle", help="The handle used in `skycoll posts`")

    # edgelist
    p_edgelist = sub.add_parser("edgelist", help="Generate .gml from .dat/.fdat data")
    p_edgelist.add_argument("handle", help="The handle used in `skycoll init`")
    p_edgelist.add_argument("--constellation", default=None, help="Constellation host for likes enrichment")

    # sync
    p_sync = sub.add_parser("sync", help="Download full repo CAR for archival")
    p_sync.add_argument("handle", help="Your Bluesky handle")

    # backlinks
    p_backlinks = sub.add_parser("backlinks", help="Query Constellation backlinks for a handle")
    p_backlinks.add_argument("handle", help="Bluesky handle to query")
    p_backlinks.add_argument("--constellation", required=True, help="Constellation host URL (e.g. https://constellation.example.com)")

    # plc
    p_plc = sub.add_parser("plc", help="Fetch PLC directory operation log for a DID")
    p_plc.add_argument("did", help="A did:plc DID")
    p_plc.add_argument("--audit", action="store_true", help="Print a human-readable summary")

    # appviews
    p_appviews = sub.add_parser("appviews", help="List built-in AppView service DIDs")

    # firehose
    p_firehose = sub.add_parser("firehose", help="Stream AT Protocol repo events in real time")
    p_firehose.add_argument("--did", default=None, help="Filter events by DID")
    p_firehose.add_argument("--handle", default=None, help="Filter events by handle (resolved to DID)")
    p_firehose.add_argument("--relay", default="wss://bsky.network", help="Relay WebSocket URL (default: wss://bsky.network)")
    p_firehose.add_argument("--limit", type=int, default=None, help="Stop after N matching events")

    args = parser.parse_args()
    set_verbose(bool(args.verbose))

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    if args.command == "resolve":
        from skycoll.commands.resolve import run
        run(args.identifier)

    elif args.command == "init":
        from skycoll.commands.init import run
        run(args.handle, fetch_lists=args.lists, include_labels=args.labels, appview=args.appview, constellation=args.constellation)

    elif args.command == "fetch":
        from skycoll.commands.fetch import run
        run(args.handle)

    elif args.command == "posts":
        from skycoll.commands.posts import run
        run(args.handle, use_car=args.car, appview=args.appview)

    elif args.command == "likes":
        from skycoll.commands.likes import run
        run(args.handle, purge=args.purge, appview=args.appview)

    elif args.command == "threads":
        from skycoll.commands.threads import run
        run(args.handle)

    elif args.command == "edgelist":
        from skycoll.commands.edgelist import run
        run(args.handle, constellation=args.constellation)

    elif args.command == "sync":
        from skycoll.commands.sync import run
        run(args.handle)

    elif args.command == "backlinks":
        from skycoll.commands.backlinks import run
        run(args.handle, constellation_host=args.constellation)

    elif args.command == "plc":
        from skycoll.commands.plc import run
        run(args.did, audit=args.audit)

    elif args.command == "appviews":
        from skycoll.commands.appviews import run
        run()

    elif args.command == "firehose":
        from skycoll.commands.firehose import run
        run(handle=args.handle, did=args.did, relay=args.relay, limit=args.limit)


if __name__ == "__main__":
    main()
