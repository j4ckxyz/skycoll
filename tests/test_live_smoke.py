"""Opt-in live tests against real AT Protocol endpoints and handles.

These tests are skipped by default and are intended for manual verification:

  SKYCOLL_LIVE=1 pytest -m live -v tests/test_live_smoke.py

For auth-required command tests, you must also set:

  SKYCOLL_LIVE_FULL=1
  SKYCOLL_LIVE_HANDLE=<your handle>

and have an existing local session in ``~/.skycoll/sessions`` so tests do not
trigger an interactive browser login.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from importlib.util import find_spec
from pathlib import Path

import pytest


def _live_enabled() -> bool:
    return os.environ.get("SKYCOLL_LIVE", "").strip().lower() in {"1", "true", "yes", "on"}


def _live_full_enabled() -> bool:
    return os.environ.get("SKYCOLL_LIVE_FULL", "").strip().lower() in {"1", "true", "yes", "on"}


def _run_cli(args: list[str], cwd: Path, timeout: int = 180) -> subprocess.CompletedProcess[str]:
    cmd = [sys.executable, "-m", "skycoll", "--verbose", *args]
    repo_root = Path(__file__).resolve().parents[1]
    env = dict(os.environ)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        str(repo_root) if not existing_pythonpath else f"{repo_root}:{existing_pythonpath}"
    )
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _extract_resolve_field(output: str, key: str) -> str:
    prefix = f"{key}:"
    for line in output.splitlines():
        if line.strip().startswith(prefix):
            return line.split(":", 1)[1].strip()
    return ""


@pytest.mark.live
def test_live_resolve_and_plc(tmp_path: Path) -> None:
    if not _live_enabled():
        pytest.skip("Set SKYCOLL_LIVE=1 to run live tests")

    handle = os.environ.get("SKYCOLL_LIVE_HANDLE", "j4ck.xyz")

    r = _run_cli(["resolve", handle], cwd=tmp_path, timeout=60)
    assert r.returncode == 0, r.stderr or r.stdout

    did = _extract_resolve_field(r.stdout, "did")
    resolved_handle = _extract_resolve_field(r.stdout, "handle")
    pds = _extract_resolve_field(r.stdout, "pds")

    assert did.startswith("did:"), r.stdout
    assert resolved_handle, r.stdout
    assert pds.startswith("https://"), r.stdout

    r2 = _run_cli(["resolve", did], cwd=tmp_path, timeout=60)
    assert r2.returncode == 0, r2.stderr or r2.stdout
    assert "handle:" in r2.stdout

    r3 = _run_cli(["plc", did, "--audit"], cwd=tmp_path, timeout=60)
    assert r3.returncode == 0, r3.stderr or r3.stdout

    plc_path = tmp_path / f"{did.replace(':', '_')}.plc"
    assert plc_path.exists(), r3.stdout
    data = json.loads(plc_path.read_text())
    assert isinstance(data, list)


@pytest.mark.live
def test_live_appviews_and_firehose(tmp_path: Path) -> None:
    if not _live_enabled():
        pytest.skip("Set SKYCOLL_LIVE=1 to run live tests")

    r = _run_cli(["appviews"], cwd=tmp_path, timeout=30)
    assert r.returncode == 0, r.stderr or r.stdout
    assert "bluesky" in r.stdout.lower()
    assert "blacksky" in r.stdout.lower()

    if find_spec("atproto") is None:
        pytest.skip("Install 'atproto' to run live firehose smoke test")

    # A short, unfiltered firehose run should quickly receive at least one event.
    # In some environments (eg CI, restricted networks), relays may not be reachable.
    try:
        r2 = _run_cli(["firehose", "--limit", "1"], cwd=tmp_path, timeout=45)
    except subprocess.TimeoutExpired:
        pytest.skip("Firehose relay did not produce events within timeout")
    assert r2.returncode == 0, r2.stderr or r2.stdout
    assert "Reached limit of 1 events." in r2.stdout


@pytest.mark.live_full
def test_live_full_command_smoke(tmp_path: Path) -> None:
    if not _live_enabled() or not _live_full_enabled():
        pytest.skip("Set SKYCOLL_LIVE=1 and SKYCOLL_LIVE_FULL=1 to run full live smoke tests")

    handle = os.environ.get("SKYCOLL_LIVE_HANDLE", "").strip()
    if not handle:
        pytest.skip("Set SKYCOLL_LIVE_HANDLE for full live smoke tests")

    # Preflight: require an existing local session to avoid interactive auth prompts.
    from skycoll.auth import Session
    from skycoll.resolve import resolve

    did = resolve(handle)["did"]
    if Session.load(did) is None:
        pytest.skip("No local session found in ~/.skycoll/sessions for this handle")

    # init (with --lists and --labels)
    r_init = _run_cli(["init", handle, "--lists", "--labels"], cwd=tmp_path, timeout=240)
    assert r_init.returncode == 0, r_init.stderr or r_init.stdout
    assert (tmp_path / f"{handle}.dat").exists()

    # fetch
    r_fetch = _run_cli(["fetch", handle], cwd=tmp_path, timeout=300)
    assert r_fetch.returncode == 0, r_fetch.stderr or r_fetch.stdout
    assert (tmp_path / "fdat").exists()

    # posts via author feed
    r_posts = _run_cli(["posts", handle], cwd=tmp_path, timeout=300)
    assert r_posts.returncode == 0, r_posts.stderr or r_posts.stdout
    assert (tmp_path / f"{handle}.twt").exists()

    # posts via CAR
    r_posts_car = _run_cli(["posts", handle, "--car"], cwd=tmp_path, timeout=420)
    assert r_posts_car.returncode == 0, r_posts_car.stderr or r_posts_car.stdout
    assert (tmp_path / f"{handle}.twt").exists()

    # likes (read-only fetch)
    r_likes = _run_cli(["likes", handle, "--appview", "bluesky"], cwd=tmp_path, timeout=300)
    assert r_likes.returncode == 0, r_likes.stderr or r_likes.stdout
    assert (tmp_path / f"{handle}.fav").exists()

    # threads
    r_threads = _run_cli(["threads", handle], cwd=tmp_path, timeout=90)
    assert r_threads.returncode == 0, r_threads.stderr or r_threads.stdout
    assert (tmp_path / f"{handle}.threads").exists()

    # edgelist
    r_edgelist = _run_cli(["edgelist", handle], cwd=tmp_path, timeout=120)
    assert r_edgelist.returncode == 0, r_edgelist.stderr or r_edgelist.stdout
    assert (tmp_path / f"{handle}.gml").exists()

    # sync
    r_sync = _run_cli(["sync", handle], cwd=tmp_path, timeout=420)
    assert r_sync.returncode == 0, r_sync.stderr or r_sync.stdout
    assert (tmp_path / f"{handle}.car").exists()

    # optional backlinks smoke
    constellation = os.environ.get("SKYCOLL_LIVE_CONSTELLATION", "").strip()
    if constellation:
        r_backlinks = _run_cli(
            ["backlinks", handle, "--constellation", constellation],
            cwd=tmp_path,
            timeout=120,
        )
        assert r_backlinks.returncode == 0, r_backlinks.stderr or r_backlinks.stdout
