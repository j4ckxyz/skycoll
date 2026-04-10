"""Tests for auth command behavior."""

from __future__ import annotations

from unittest.mock import patch


@patch("skycoll.commands.auth.login")
def test_auth_login_prints_success(mock_login, capsys):
    from skycoll.commands.auth import run_login

    class _S:
        handle = "alice.bsky.social"
        did = "did:plc:alice"

    mock_login.return_value = _S()
    run_login("alice.bsky.social")
    out = capsys.readouterr().out
    assert "Logged in as alice.bsky.social (did:plc:alice)" in out


@patch("skycoll.commands.auth.logout")
def test_auth_logout_prints_success(mock_logout, capsys):
    from skycoll.commands.auth import run_logout

    mock_logout.return_value = ("alice.bsky.social", "did:plc:alice")
    run_logout("alice.bsky.social")
    out = capsys.readouterr().out
    assert "Logged out alice.bsky.social (did:plc:alice)" in out


@patch("skycoll.commands.auth.list_saved_sessions")
def test_auth_list_formats_valid_and_expired(mock_list, capsys):
    from skycoll.commands.auth import run_list

    mock_list.return_value = [
        {
            "handle": "valid.bsky.social",
            "did": "did:plc:valid",
            "token_expiry": 9999999999.0,
            "is_valid": True,
        },
        {
            "handle": "old.bsky.social",
            "did": "did:plc:old",
            "token_expiry": 1.0,
            "is_valid": False,
        },
    ]

    run_list()
    out = capsys.readouterr().out
    assert "Saved sessions:" in out
    assert "✓  valid.bsky.social" in out
    assert "✗  old.bsky.social" in out
