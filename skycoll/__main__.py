#!/usr/bin/env python3
"""skycoll — Bluesky/AT Protocol social-graph CLI (twecoll equivalent).

Usage:
    skycoll resolve <handle-or-did>
    skycoll init <handle> [--lists] [--labels]
    skycoll fetch <handle>
    skycoll posts <handle> [--feed]
    skycoll likes <handle> [-p|--purge]
    skycoll threads <handle>
    skycoll edgelist <handle>
    skycoll sync <handle>
"""

from __future__ import annotations

import argparse
import sys

from skycoll import __version__


def main() -> None:
    """Entry point for the skycoll CLI."""
    parser = argparse.ArgumentParser(
        prog="skycoll",
        description="skycoll — Bluesky/AT Protocol social-graph CLI",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    sub = parser.add_subparsers(dest="command", help="Available commands")

    # resolve
    p_resolve = sub.add_parser("resolve", help="Resolve a handle to a DID or vice versa")
    p_resolve.add_argument("identifier", help="A Bluesky handle or DID")

    # init
    p_init = sub.add_parser("init", help="Fetch profile + follows/followers, write .dat and avatars")
    p_init.add_argument("handle", help="Your Bluesky handle")
    p_init.add_argument("--lists", action="store_true", help="Fetch user lists (app.bsky.graph.getLists)")
    p_init.add_argument("--labels", action="store_true", help="Include self-labels and server labels in .dat")

    # fetch
    p_fetch = sub.add_parser("fetch", help="Fetch follows of every handle in .dat, write to fdat/")
    p_fetch.add_argument("handle", help="The handle used in `skycoll init`")

    # posts
    p_posts = sub.add_parser("posts", help="Download posts via CAR sync, write .twt")
    p_posts.add_argument("handle", help="Your Bluesky handle")
    p_posts.add_argument(
        "--feed",
        action="store_true",
        help="Use getAuthorFeed instead of CAR (limited to ~3000 items)",
    )

    # likes
    p_likes = sub.add_parser("likes", help="Download all likes, write .fav")
    p_likes.add_argument("handle", help="Your Bluesky handle")
    p_likes.add_argument(
        "-p", "--purge",
        action="store_true",
        help="Purge (delete) all likes instead of just listing them",
    )

    # threads
    p_threads = sub.add_parser("threads", help="Reconstruct reply threads from .twt")
    p_threads.add_argument("handle", help="The handle used in `skycoll posts`")

    # edgelist
    p_edgelist = sub.add_parser("edgelist", help="Generate .gml from .dat/.fdat data")
    p_edgelist.add_argument("handle", help="The handle used in `skycoll init`")

    # sync
    p_sync = sub.add_parser("sync", help="Download full repo CAR for archival")
    p_sync.add_argument("handle", help="Your Bluesky handle")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    if args.command == "resolve":
        from skycoll.commands.resolve import run
        run(args.identifier)

    elif args.command == "init":
        from skycoll.commands.init import run
        run(args.handle, fetch_lists=args.lists, include_labels=args.labels)

    elif args.command == "fetch":
        from skycoll.commands.fetch import run
        run(args.handle)

    elif args.command == "posts":
        from skycoll.commands.posts import run
        run(args.handle, use_feed=args.feed)

    elif args.command == "likes":
        from skycoll.commands.likes import run
        run(args.handle, purge=args.purge)

    elif args.command == "threads":
        from skycoll.commands.threads import run
        run(args.handle)

    elif args.command == "edgelist":
        from skycoll.commands.edgelist import run
        run(args.handle)

    elif args.command == "sync":
        from skycoll.commands.sync import run
        run(args.handle)


if __name__ == "__main__":
    main()