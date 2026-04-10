"""Consistent CLI output helpers."""

from __future__ import annotations


def ok(msg: str) -> None:
    print(f"✓ {msg}")


def warn(msg: str) -> None:
    print(f"⚠ {msg}")


def err(msg: str) -> None:
    print(f"✗ {msg}")


def info(msg: str) -> None:
    print(msg)
