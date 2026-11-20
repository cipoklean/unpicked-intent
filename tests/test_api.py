"""Tests for the FastAPI scouting API (src/unpicked_intent/api/main.py).

Builds a small DuckDB from synthetic fixtures via the real build → baseline → signals
pipeline and exercises all endpoints using FastAPI's TestClient.
"""

from __future__ import annotations

import csv
import tempfile
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

import unpicked_intent.api.main as api_mod
import unpicked_intent.pipeline.build as build_mod
import unpicked_intent.scoring.baseline as baseline_mod
import unpicked_intent.scoring.signals as signals_mod
from tests.fixtures.create_synthetic import build_rich_synthetic_grid


@pytest.fixture
def synthetic_api_db():
    """Build a small DuckDB from synthetic fixtures with target teams (47351, 340) + T1/T2.

    Patches api_mod.DB_PATH to point to the temporary DuckDB and restores it on teardown.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        grid = tmp / "grid"
        # Use 47351 (Cloud9 Kia) and 340 (FlyQuest) as the two teams in the synthetic series
        build_rich_synthetic_grid(grid, team_a="47351", team_b="340")

        shared_derived = tmp / "derived"
        shared_db = shared_derived / "unpicked.duckdb"

        orig_build_derived, orig_build_db = build_mod.DERIVED, build_mod.DB_PATH
        orig_bl_derived, orig_bl_db = baseline_mod.DERIVED, baseline_mod.DB_PATH
        orig_sig_derived, orig_sig_db = signals_mod.DERIVED, signals_mod.DB_PATH
        orig_api_derived, orig_api_db = api_mod.DERIVED, api_mod.DB_PATH

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
                    ("P1", "top"), ("P2", "mid"), ("P3", "jungle"), ("P4", "bot"), ("P5", "support"),
                    ("P6", "top"), ("P7", "mid"), ("P8", "jungle"), ("P9", "bot"), ("P10", "support"),
                ]:
                    writer.writerow({"player_id": pid, "role": role, "share": 1.0, "flex": False})

            signals_mod.run_signals(str(grid))

            api_mod.DERIVED = shared_derived
            api_mod.DB_PATH = shared_db
            yield shared_db
        finally:
            build_mod.DERIVED, build_mod.DB_PATH = orig_build_derived, orig_build_db
            baseline_mod.DERIVED, baseline_mod.DB_PATH = orig_bl_derived, orig_bl_db
            signals_mod.DERIVED, signals_mod.DB_PATH = orig_sig_derived, orig_sig_db
            api_mod.DERIVED, api_mod.DB_PATH = orig_api_derived, orig_api_db


class TestApi:
    """FastAPI TestClient suite covering all endpoints and error cases."""

    def test_derived_path_next_to_pyproject(self):
        """api.main.DERIVED must resolve to <repo_root>/derived next to pyproject.toml."""
        assert (api_mod.DERIVED.parent / "pyproject.toml").is_file()
        assert api_mod.DERIVED.name == "derived"

    def test_target_teams_constant(self):
        """TARGET_TEAMS must list the 8 target LCS teams in exact order."""
        assert api_mod.TARGET_TEAMS == [
            340, 47363, 54198, 53073, 52416, 47499, 56389, 47351
        ]

    def test_health(self):
        client = TestClient(api_mod.app)
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

    def test_teams_lists_target_teams_first_with_counts(self, synthetic_api_db):
        client = TestClient(api_mod.app)
        resp = client.get("/teams")
        assert resp.status_code == 200
        teams = resp.json()
        assert len(teams) > 0

        # Target teams present in DB (340 FlyQuest, 47351 Cloud9 Kia) must come first
        # in TARGET_TEAMS order (340 before 47351), before non-target teams (T1..T4).
        team_ids = [t["team_id"] for t in teams]
        assert team_ids[:2] == ["340", "47351"], f"Expected target teams first, got {team_ids}"

        by_id = {t["team_id"]: t for t in teams}
        assert by_id["340"]["name"] == "FlyQuest"
        assert by_id["47351"]["name"] == "Cloud9 Kia"
        assert by_id["47351"]["left_open_count"] > 0
        assert by_id["47351"]["early_ban_count"] > 0
        assert by_id["340"]["early_ban_count"] > 0

        # Valorant team must not appear
        assert "V1" not in by_id

    def test_silence_endpoint_shape_and_sorting(self, synthetic_api_db):
        client = TestClient(api_mod.app)
        resp = client.get("/silence/47351")
        assert resp.status_code == 200
        data = resp.json()

        assert data["team_id"] == "47351"
        assert data["team_name"] == "Cloud9 Kia"
        assert data["min_n"] == 4
        assert data["note"] == "Only tells with at least 4 qualifying games are shown."

        left_open = data["left_open"]
        assert len(left_open) > 0
        # Verify required fields and player name resolution
        for row in left_open:
            assert set(row.keys()) >= {
                "player_name", "champion", "role", "k", "taken",
                "expected", "p_value", "q_value", "fit", "label",
            }
            assert row["k"] >= 4
            assert row["label"] in ("strong", "lead")
        # Player P1 must resolve to AlphaTop from names table
        player_names = {r["player_name"] for r in left_open}
        assert "AlphaTop" in player_names

        # Sorted by p_value ascending
        p_vals = [r["p_value"] for r in left_open]
        assert p_vals == sorted(p_vals)

        early_ban = data["early_ban"]
        assert len(early_ban) >= 2
        for row in early_ban:
            assert set(row.keys()) == {
                "champion", "n_bans", "team_mean_slot", "baseline_mean_slot", "delta"
            }
            assert row["n_bans"] >= 4

        # Sorted by delta descending (Azir delta=+6.0 before Viktor delta=+0.5)
        deltas = [r["delta"] for r in early_ban]
        assert deltas == sorted(deltas, reverse=True)
        assert early_ban[0]["champion"] == "Azir"
        assert early_ban[0]["delta"] > early_ban[1]["delta"]

    def test_self_scout_matches_silence_47351(self, synthetic_api_db):
        client = TestClient(api_mod.app)
        resp_self = client.get("/self-scout")
        resp_c9 = client.get("/silence/47351")
        assert resp_self.status_code == 200
        assert resp_c9.status_code == 200
        assert resp_self.json() == resp_c9.json()

    def test_strong_lead_boundary_at_q_0_2(self, synthetic_api_db):
        """label is 'strong' strictly when q_value < 0.2, and 'lead' when q_value >= 0.2."""
        # Insert deterministic boundary rows for team T1 into the synthetic DuckDB
        con = duckdb.connect(str(synthetic_api_db))
        con.execute("DELETE FROM left_open WHERE team = 'T1'")
        con.executemany(
            "INSERT INTO left_open VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("T1", "P1", "top", "Aatrox", 6, 0, 3.0, 0.01, 0.1999, 0.5),
                ("T1", "P2", "mid", "Orianna", 6, 1, 3.0, 0.05, 0.2, 0.4),
                ("T1", "P3", "jungle", "Graves", 6, 2, 3.0, 0.15, 0.2001, 0.3),
                # Also add a k < 4 row to verify API enforces min_n = 4
                ("T1", "P4", "bot", "Jinx", 3, 0, 1.5, 0.001, 0.01, 0.5),
            ],
        )
        con.close()

        client = TestClient(api_mod.app)
        resp = client.get("/silence/T1")
        assert resp.status_code == 200
        tells = resp.json()["left_open"]

        # k=3 row must be suppressed
        assert len(tells) == 3
        by_champ = {t["champion"]: t for t in tells}

        assert by_champ["Aatrox"]["q_value"] == pytest.approx(0.1999)
        assert by_champ["Aatrox"]["label"] == "strong"

        # Exactly at q = 0.2 -> must be 'lead' (strict < 0.2)
        assert by_champ["Orianna"]["q_value"] == pytest.approx(0.2)
        assert by_champ["Orianna"]["label"] == "lead"

        assert by_champ["Graves"]["q_value"] == pytest.approx(0.2001)
        assert by_champ["Graves"]["label"] == "lead"

        # Sorted by p_value ascending
        assert [t["champion"] for t in tells] == ["Aatrox", "Orianna", "Graves"]

    def test_unknown_team_returns_404(self, synthetic_api_db):
        client = TestClient(api_mod.app)
        resp = client.get("/silence/999999")
        assert resp.status_code == 404
        assert "999999" in resp.json()["detail"]

    def test_missing_database_returns_503(self):
        orig_db = api_mod.DB_PATH
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                api_mod.DB_PATH = Path(tmpdir) / "nonexistent.duckdb"
                client = TestClient(api_mod.app)

                for endpoint in ("/teams", "/silence/47351", "/self-scout"):
                    resp = client.get(endpoint)
                    assert resp.status_code == 503, f"{endpoint} returned {resp.status_code}"
                    detail = resp.json()["detail"]
                    assert "not found" in detail.lower()
        finally:
            api_mod.DB_PATH = orig_db

    def test_opens_database_read_only(self, synthetic_api_db, monkeypatch):
        """API must open DuckDB with read_only=True."""
        seen_kwargs = []
        real_connect = duckdb.connect

        def spy_connect(database, *args, **kwargs):
            seen_kwargs.append(kwargs)
            return real_connect(database, *args, **kwargs)

        monkeypatch.setattr(api_mod.duckdb, "connect", spy_connect)

        client = TestClient(api_mod.app)
        assert client.get("/teams").status_code == 200
        assert client.get("/silence/47351").status_code == 200
        assert client.get("/self-scout").status_code == 200

        assert len(seen_kwargs) == 3
        assert all(kw.get("read_only") is True for kw in seen_kwargs)
