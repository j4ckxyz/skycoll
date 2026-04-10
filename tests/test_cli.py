"""CLI wiring tests for global flags and command argument forwarding."""

from __future__ import annotations

import pytest

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


def test_auth_login_command_forwarded() -> None:
    from skycoll import __main__ as cli

    with patch("sys.argv", ["skycoll", "auth", "login", "alice.bsky.social"]):
        with patch("skycoll.commands.auth.run_login") as mock_login:
            cli.main()

    mock_login.assert_called_once_with("alice.bsky.social")


def test_auth_list_command_forwarded() -> None:
    from skycoll import __main__ as cli

    with patch("sys.argv", ["skycoll", "auth", "list"]):
        with patch("skycoll.commands.auth.run_list") as mock_list:
            cli.main()

    mock_list.assert_called_once_with()


def test_cli_handles_typed_errors_without_traceback(capsys) -> None:
    from skycoll import __main__ as cli
    from skycoll.errors import NotFoundError

    with patch("sys.argv", ["skycoll", "resolve", "alice.bsky.social"]):
        with patch("skycoll.commands.resolve.run", side_effect=NotFoundError("missing test record")):
            with pytest.raises(SystemExit) as exc:
                cli.main()

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "✗ Not found: missing test record" in out


@pytest.mark.parametrize(
    ("argv", "expected_message"),
    [
        (
            ["edgelist", "missing"],
            "No .dat file found for 'missing'. Run: skycoll init missing",
        ),
        (
            ["fetch", "missing"],
            "No .dat file found for 'missing'. Run: skycoll init missing",
        ),
        (
            ["threads", "missing"],
            "No .twt file found for 'missing'. Run: skycoll posts missing",
        ),
    ],
)
def test_cli_missing_local_files_are_clean_one_line_errors(
    tmp_path,
    monkeypatch,
    capsys,
    argv,
    expected_message,
) -> None:
    from skycoll import __main__ as cli

    monkeypatch.chdir(tmp_path)

    with patch("sys.argv", ["skycoll", *argv]):
        with pytest.raises(SystemExit) as exc:
            cli.main()

    assert exc.value.code == 1
    out_lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert out_lines == [f"✗ Not found: {expected_message}"]


def test_cli_fallback_handler_for_untyped_exceptions(capsys) -> None:
    from skycoll import __main__ as cli

    with patch("sys.argv", ["skycoll", "resolve", "alice.bsky.social"]):
        with patch("skycoll.commands.resolve.run", side_effect=ValueError("boom")):
            with pytest.raises(SystemExit) as exc:
                cli.main()

    assert exc.value.code == 1
    out_lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert out_lines == [
        "✗ Unexpected error: boom",
        "run with -v for details",
    ]


def test_cli_auth_error_session_expired_omits_label_prefix(capsys) -> None:
    from skycoll import __main__ as cli
    from skycoll.errors import AuthError

    with patch("sys.argv", ["skycoll", "resolve", "alice.bsky.social"]):
        with patch(
            "skycoll.commands.resolve.run",
            side_effect=AuthError("Session expired for alice.bsky.social — run: skycoll auth login alice.bsky.social."),
        ):
            with pytest.raises(SystemExit) as exc:
                cli.main()

    assert exc.value.code == 1
    out_lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert out_lines == [
        "✗ Session expired for alice.bsky.social — run: skycoll auth login alice.bsky.social.",
    ]
