"""Tests for static site export (src/unpicked_intent/export_static.py)."""

from __future__ import annotations

import csv
import json
import re
import shutil
import tempfile
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

import unpicked_intent.api.main as api_mod
import unpicked_intent.export_static as export_mod
import unpicked_intent.pipeline.build as build_mod
import unpicked_intent.scoring.baseline as baseline_mod
import unpicked_intent.scoring.signals as signals_mod
from tests.fixtures.create_synthetic import build_rich_synthetic_grid

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def _shared_synthetic_db():
    """Build a synthetic DuckDB once per module for fast read-only export tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        grid = tmp / "grid"
        build_rich_synthetic_grid(grid, team_a="47351", team_b="340")

        shared_derived = tmp / "derived"
        shared_db = shared_derived / "unpicked.duckdb"

        orig_build_derived, orig_build_db = build_mod.DERIVED, build_mod.DB_PATH
        orig_bl_derived, orig_bl_db = baseline_mod.DERIVED, baseline_mod.DB_PATH
        orig_sig_derived, orig_sig_db = signals_mod.DERIVED, signals_mod.DB_PATH

        try:
            build_mod.DERIVED = shared_derived
            build_mod.DB_PATH = shared_db
            baseline_mod.DERIVED = shared_derived
            baseline_mod.DB_PATH = shared_db
            signals_mod.DERIVED = shared_derived
            signals_mod.DB_PATH = shared_db

            build_mod.run_pipeline(str(grid))
            baseline_mod.run_baseline()

            player_roles_file = shared_derived / "player_roles.csv"
            with open(player_roles_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["player_id", "role", "share", "flex"])
                writer.writeheader()
                for pid, role in [
                    ("P1", "top"),
                    ("P2", "mid"),
                    ("P3", "jungle"),
                    ("P4", "bot"),
                    ("P5", "support"),
                    ("P6", "top"),
                    ("P7", "mid"),
                    ("P8", "jungle"),
                    ("P9", "bot"),
                    ("P10", "support"),
                ]:
                    writer.writerow({"player_id": pid, "role": role, "share": 1.0, "flex": False})

            signals_mod.run_signals(str(grid))
            yield shared_db
        finally:
            build_mod.DERIVED, build_mod.DB_PATH = orig_build_derived, orig_build_db
            baseline_mod.DERIVED, baseline_mod.DB_PATH = orig_bl_derived, orig_bl_db
            signals_mod.DERIVED, signals_mod.DB_PATH = orig_sig_derived, orig_sig_db


@pytest.fixture
def synthetic_export_db(_shared_synthetic_db):
    """Point api_mod.DB_PATH at the shared read-only synthetic DuckDB."""
    orig_api_derived, orig_api_db = api_mod.DERIVED, api_mod.DB_PATH
    try:
        api_mod.DERIVED = _shared_synthetic_db.parent
        api_mod.DB_PATH = _shared_synthetic_db
        yield _shared_synthetic_db
    finally:
        api_mod.DERIVED, api_mod.DB_PATH = orig_api_derived, orig_api_db


def test_export_writes_exact_api_responses(synthetic_export_db, tmp_path):
    """Exporter writes teams.json, one silence file per team, and self-scout.json matching API."""
    out_dir = tmp_path / "dist"
    summary = export_mod.export_static(out_dir)

    client = TestClient(api_mod.app)
    api_teams = client.get("/teams").json()
    assert len(api_teams) > 0

    teams_file = out_dir / "data" / "teams.json"
    assert teams_file.is_file()
    exported_teams = json.loads(teams_file.read_text(encoding="utf-8"))
    assert exported_teams == api_teams
    assert summary["teams"] == len(api_teams)
    assert summary["silence_files"] == len(api_teams)

    silence_dir = out_dir / "data" / "silence"
    assert silence_dir.is_dir()
    exported_silence_files = sorted(p.name for p in silence_dir.iterdir() if p.is_file())
    expected_silence_files = sorted(f"{t['team_id']}.json" for t in exported_teams)
    assert exported_silence_files == expected_silence_files

    for team in exported_teams:
        tid = str(team["team_id"])
        api_report = client.get(f"/silence/{tid}").json()
        exported_report = json.loads((silence_dir / f"{tid}.json").read_text(encoding="utf-8"))
        assert exported_report == api_report

    self_scout_file = out_dir / "data" / "self-scout.json"
    assert self_scout_file.is_file()
    exported_self_scout = json.loads(self_scout_file.read_text(encoding="utf-8"))
    assert exported_self_scout == client.get("/self-scout").json()

    # Draft Replay files: one games.json per TARGET_TEAM and one {game_id}.json per recent game
    replay_dir = out_dir / "data" / "replay"
    assert replay_dir.is_dir()
    exported_replay_teams = sorted(p.name for p in replay_dir.iterdir() if p.is_dir())
    expected_replay_teams = sorted(str(tid) for tid in api_mod.TARGET_TEAMS)
    assert exported_replay_teams == expected_replay_teams
    assert summary["replay_teams"] == len(api_mod.TARGET_TEAMS)

    total_replay_games = 0
    for target_id in api_mod.TARGET_TEAMS:
        tid = str(target_id)
        games_file = replay_dir / tid / "games.json"
        assert games_file.is_file()
        exported_games = json.loads(games_file.read_text(encoding="utf-8"))
        api_games = client.get(f"/replay/{tid}/games").json()
        assert exported_games == api_games

        for game in exported_games["games"]:
            gid = str(game["game_id"])
            detail_file = replay_dir / tid / f"{gid}.json"
            assert detail_file.is_file()
            exported_detail = json.loads(detail_file.read_text(encoding="utf-8"))
            api_detail = client.get(f"/replay/{tid}/{gid}").json()
            assert exported_detail == api_detail
            total_replay_games += 1

    # Synthetic fixture populates 5 recent games each for 47351 and 340 -> 10 replay game files
    assert total_replay_games == 10
    assert summary["replay_games"] == total_replay_games
    # Non-target teams must never be exported under data/replay/
    assert not (replay_dir / "T1").exists()


def test_export_web_assets_static_flag_and_offline(synthetic_export_db, tmp_path):
    """dist/index.html has data-static="1" (source does not) and no http(s) URLs exist in dist."""
    out_dir = tmp_path / "dist"
    export_mod.export_static(out_dir)

    dist_index = out_dir / "index.html"
    dist_js = out_dir / "app.js"
    dist_css = out_dir / "styles.css"
    assert dist_index.is_file()
    assert dist_js.is_file()
    assert dist_css.is_file()

    dist_html = dist_index.read_text(encoding="utf-8")
    assert re.search(r'<html\b[^>]*\bdata-static="1"[^>]*>', dist_html)
    assert 'href="styles.css"' in dist_html
    assert 'src="app.js"' in dist_html

    # Source web/index.html must NOT have data-static="1"
    src_html = (api_mod.WEB_DIR / "index.html").read_text(encoding="utf-8")
    assert 'data-static="1"' not in src_html

    # Relative static paths in app.js
    js_text = dist_js.read_text(encoding="utf-8")
    assert "data/teams.json" in js_text
    assert "data/self-scout.json" in js_text
    assert "data/silence/${teamId}.json" in js_text
    assert "data/replay/${rest}.json" in js_text

    # No external http(s) URLs in any exported HTML/JS/CSS
    for asset in out_dir.rglob("*"):
        if asset.is_file() and asset.suffix.lower() in {".html", ".js", ".css"}:
            text = asset.read_text(encoding="utf-8")
            assert "http://" not in text, f"Found http:// in {asset}"
            assert "https://" not in text, f"Found https:// in {asset}"


def test_export_fails_loudly_when_db_missing(tmp_path):
    """Missing derived/unpicked.duckdb must raise FileNotFoundError and return non-zero from CLI."""
    orig_db = api_mod.DB_PATH
    try:
        api_mod.DB_PATH = tmp_path / "missing.duckdb"
        out_dir = tmp_path / "dist"
        with pytest.raises(FileNotFoundError, match="not found"):
            export_mod.export_static(out_dir)
        assert not out_dir.exists()

        rc = export_mod.main(["--out", str(out_dir)])
        assert rc != 0
    finally:
        api_mod.DB_PATH = orig_db


def test_export_fails_loudly_on_non_200(synthetic_export_db, tmp_path):
    """Any non-200 API response (e.g., missing self-scout team 47351) must fail loudly."""
    copied_db = tmp_path / "mutated.duckdb"
    shutil.copy2(synthetic_export_db, copied_db)
    con = duckdb.connect(str(copied_db))
    con.execute("DELETE FROM names WHERE kind = 'team' AND id = '47351'")
    con.execute("DELETE FROM left_open WHERE team = '47351'")
    con.execute("DELETE FROM early_ban WHERE team = '47351'")
    con.close()

    api_mod.DB_PATH = copied_db
    out_dir = tmp_path / "dist"
    with pytest.raises(RuntimeError, match="HTTP 404"):
        export_mod.export_static(out_dir)
    assert not out_dir.exists()


def test_cli_main_exports_successfully(synthetic_export_db, tmp_path, capsys):
    """CLI entry point writes the static site and exits 0."""
    out_dir = tmp_path / "dist"
    rc = export_mod.main(["--out", str(out_dir)])
    assert rc == 0
    captured = capsys.readouterr()
    assert "Exported static site" in captured.out
    assert (out_dir / "index.html").is_file()
    assert (out_dir / "data" / "teams.json").is_file()


def test_data_boundary_ignores_and_blocks_dist():
    """dist/ must be listed in .gitignore and rejected by scripts/check_data_boundary.sh."""
    gitignore = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8")
    assert any(line.strip() == "dist/" for line in gitignore.splitlines())

    hook_bytes = (REPO_ROOT / "scripts" / "check_data_boundary.sh").read_bytes()
    assert b"\r\n" not in hook_bytes, "check_data_boundary.sh must use LF line endings"
    hook = hook_bytes.decode("utf-8")
    assert re.search(r"\(data\|derived\|dist\)/", hook), (
        "check_data_boundary.sh must reject paths under dist/"
    )
