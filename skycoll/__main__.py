#!/usr/bin/env python3
"""skycoll — Bluesky/AT Protocol social-graph CLI (twecoll equivalent)."""

from __future__ import annotations

import argparse
import sys

from skycoll import __version__
from skycoll.errors import AuthError, SkycollError
from skycoll.output import err
from skycoll.verbosity import is_verbose, set_verbose


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

    # auth
    p_auth = sub.add_parser("auth", help="Manage saved OAuth sessions")
    sub_auth = p_auth.add_subparsers(dest="auth_command", help="Auth commands")
    p_auth_login = sub_auth.add_parser("login", help="Log in and save a session")
    p_auth_login.add_argument("handle", help="Handle to authenticate")
    p_auth_logout = sub_auth.add_parser("logout", help="Delete a saved session")
    p_auth_logout.add_argument("handle", help="Handle whose session should be removed")
    sub_auth.add_parser("list", help="List saved sessions")

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
    p_posts.add_argument("--car", action="store_true", help="Use full CAR repo sync instead of paginated feed")
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
    p_edgelist = sub.add_parser("edgelist", help="Generate .gml/.gexf from .dat/.fdat data")
    p_edgelist.add_argument("handle", help="The handle used in `skycoll init`")
    p_edgelist.add_argument("--constellation", default=None, help="Constellation host for likes enrichment")
    p_edgelist.add_argument("--gexf", action="store_true", help="Also write <handle>.gexf (Gephi format)")
    p_edgelist.add_argument("--no-gml", action="store_true", help="Do not write <handle>.gml")

    # convert
    p_convert = sub.add_parser("convert", help="Convert existing graph file between GML and GEXF")
    p_convert.add_argument("handle", help="Handle basename of existing graph file")
    p_convert.add_argument("--to", required=True, choices=["gml", "gexf"], help="Target graph format")

    # sync
    p_sync = sub.add_parser("sync", help="Download full repo CAR for archival")
    p_sync.add_argument("handle", help="Your Bluesky handle")

    # backlinks
    p_backlinks = sub.add_parser("backlinks", help="Query Constellation backlinks for a handle")
    p_backlinks.add_argument("handle", help="Bluesky handle to query")
    p_backlinks.add_argument("--constellation", required=True, help="Constellation host URL")

    # plc
    p_plc = sub.add_parser("plc", help="Fetch PLC directory operation log for a DID")
    p_plc.add_argument("did", help="A did:plc DID")
    p_plc.add_argument("--audit", action="store_true", help="Print a human-readable summary")

    # appviews
    sub.add_parser("appviews", help="List built-in AppView service DIDs")

    # firehose
    p_firehose = sub.add_parser("firehose", help="Stream AT Protocol repo events in real time")
    p_firehose.add_argument("--did", default=None, help="Filter events by DID")
    p_firehose.add_argument("--handle", default=None, help="Filter events by handle (resolved to DID)")
    p_firehose.add_argument("--relay", default="wss://bsky.network", help="Relay WebSocket URL")
    p_firehose.add_argument("--limit", type=int, default=None, help="Stop after N matching events")

    args = parser.parse_args()
    set_verbose(bool(args.verbose))

    try:
        if args.command is None:
            parser.print_help()
            sys.exit(1)

        if args.command == "resolve":
            from skycoll.commands.resolve import run

            run(args.identifier)

        elif args.command == "auth":
            from skycoll.commands.auth import run_list, run_login, run_logout

            if args.auth_command == "login":
                run_login(args.handle)
            elif args.auth_command == "logout":
                run_logout(args.handle)
            elif args.auth_command == "list":
                run_list()
            else:
                raise SkycollError("missing auth subcommand (login/logout/list)")

        elif args.command == "init":
            from skycoll.commands.init import run

            run(
                args.handle,
                fetch_lists=args.lists,
                include_labels=args.labels,
                appview=args.appview,
                constellation=args.constellation,
            )

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

            run(
                args.handle,
                constellation=args.constellation,
                write_gexf_file=args.gexf,
                write_gml_file=not args.no_gml,
            )

        elif args.command == "convert":
            from skycoll.commands.convert import run

            run(args.handle, to_format=args.to)

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
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)
    except SkycollError as exc:
        label = getattr(exc, "label", "Error")
        if isinstance(exc, AuthError) and str(exc).startswith("Session expired for "):
            err(str(exc))
        else:
            err(f"{label}: {exc}")
        sys.exit(1)
    except Exception as e:
        if is_verbose():
            raise
        err(f"Unexpected error: {e}")
        print("run with -v for details")
        sys.exit(1)


if __name__ == "__main__":
    main()
