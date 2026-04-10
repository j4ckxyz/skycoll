"""Tests for skycoll.verbosity."""

from __future__ import annotations

from skycoll.verbosity import is_verbose, set_verbose, vprint


def test_set_verbose_toggles_flag() -> None:
    set_verbose(False)
    assert is_verbose() is False

    set_verbose(True)
    assert is_verbose() is True

    set_verbose(False)
    assert is_verbose() is False


def test_vprint_only_prints_when_enabled(capsys) -> None:
    set_verbose(False)
    vprint("hidden message")
    assert capsys.readouterr().out == ""

    set_verbose(True)
    vprint("visible message")
    out = capsys.readouterr().out
    assert "[verbose]" in out
    assert "visible message" in out

    set_verbose(False)
