"""CLI wiring tests for global flags and command argument forwarding."""

from __future__ import annotations

from unittest.mock import patch


def test_global_verbose_flag_sets_verbosity() -> None:
    from skycoll import __main__ as cli

    with patch("sys.argv", ["skycoll", "--verbose", "appviews"]):
        with patch("skycoll.__main__.set_verbose") as mock_set_verbose:
            with patch("skycoll.commands.appviews.run") as mock_run:
                cli.main()

    mock_set_verbose.assert_called_once_with(True)
    mock_run.assert_called_once_with()


def test_likes_appview_is_forwarded() -> None:
    from skycoll import __main__ as cli

    with patch("sys.argv", ["skycoll", "likes", "alice.bsky.social", "--appview", "blacksky"]):
        with patch("skycoll.commands.likes.run") as mock_run:
            cli.main()

    mock_run.assert_called_once_with("alice.bsky.social", purge=False, appview="blacksky")


def test_edgelist_gexf_flags_forwarded() -> None:
    from skycoll import __main__ as cli

    with patch("sys.argv", ["skycoll", "edgelist", "alice", "--gexf", "--no-gml"]):
        with patch("skycoll.commands.edgelist.run") as mock_run:
            cli.main()

    mock_run.assert_called_once_with(
        "alice",
        constellation=None,
        write_gexf_file=True,
        write_gml_file=False,
    )


def test_convert_command_forwarded() -> None:
    from skycoll import __main__ as cli

    with patch("sys.argv", ["skycoll", "convert", "alice", "--to", "gexf"]):
        with patch("skycoll.commands.convert.run") as mock_run:
            cli.main()

    mock_run.assert_called_once_with("alice", to_format="gexf")
