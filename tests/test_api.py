"""Tests for the FastAPI scouting API (src/unpicked_intent/api/main.py).

Builds a small DuckDB from synthetic fixtures via the real build → baseline → signals
pipeline and exercises all endpoints using FastAPI's TestClient.
"""

from __future__ import annotations

import csv
import json
import re
import subprocess
import tempfile
from pathlib import Path

import duckdb
import pytest
from fastapi.routing import APIRoute
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

    def test_meta_endpoint_counts_qualifying_left_open(self, synthetic_api_db):
        """GET /meta returns exact count of left_open rows with k >= 4."""
        client = TestClient(api_mod.app)
        resp = client.get("/meta")
        assert resp.status_code == 200
        meta = resp.json()
        assert meta["min_n"] == 4
        assert meta["strong_q_threshold"] == pytest.approx(0.2)

        teams = client.get("/teams").json()
        expected_total = sum(t["left_open_count"] for t in teams)
        assert meta["total_tests"] == expected_total
        assert meta["total_tests"] > 0

        # Inserting a k < 4 row must NOT change total_tests, while k >= 4 must increment it.
        con = duckdb.connect(str(synthetic_api_db))
        con.executemany(
            "INSERT INTO left_open VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("T1", "P1", "top", "Teemo", 3, 0, 1.5, 0.01, 0.05, 0.5),
                ("T1", "P1", "top", "Mordekaiser", 5, 0, 2.5, 0.01, 0.05, 0.5),
            ],
        )
        con.close()

        meta_after = client.get("/meta").json()
        assert meta_after["total_tests"] == expected_total + 1

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
        assert "strong_count" in by_id["47351"]
        assert "strong_count" in by_id["340"]

        # Valorant team must not appear
        assert "V1" not in by_id

    def test_teams_strong_count_preserves_target_order(self, synthetic_api_db):
        """strong_count counts q < 0.2 rows (k >= 4) without reordering TARGET_TEAMS."""
        con = duckdb.connect(str(synthetic_api_db))
        con.execute("DELETE FROM left_open WHERE team IN ('340', '56389')")
        con.executemany(
            "INSERT INTO names VALUES (?, ?, ?)",
            [("team", "56389", "Sentinels")],
        )
        con.executemany(
            "INSERT INTO left_open VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                # FlyQuest (340): lead only (q = 0.25)
                ("340", "P6", "top", "Aatrox", 6, 1, 3.0, 0.08, 0.25, 0.4),
                # Sentinels (56389): 1 strong (q = 0.05) + 1 boundary lead (q = 0.2) + 1 k=3 ignored
                ("56389", "P2", "mid", "Azir", 21, 2, 8.3, 0.0003, 0.05, 0.3),
                ("56389", "P2", "mid", "Orianna", 6, 1, 3.0, 0.05, 0.2, 0.3),
                ("56389", "P2", "mid", "Syndra", 3, 0, 1.5, 0.001, 0.01, 0.3),
            ],
        )
        con.close()

        client = TestClient(api_mod.app)
        teams = client.get("/teams").json()
        team_ids = [t["team_id"] for t in teams]
        # Rail order must keep TARGET_TEAMS order: 340 (FlyQuest) before 56389 (Sentinels) before 47351 (C9)
        assert team_ids[:3] == ["340", "56389", "47351"]

        by_id = {t["team_id"]: t for t in teams}
        assert by_id["340"]["strong_count"] == 0
        assert by_id["56389"]["left_open_count"] == 2
        assert by_id["56389"]["strong_count"] == 1

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

                for endpoint in (
                    "/meta",
                    "/teams",
                    "/silence/47351",
                    "/self-scout",
                    "/replay/47351/games",
                    "/replay/47351/RG5_2",
                ):
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
        assert client.get("/meta").status_code == 200
        assert client.get("/teams").status_code == 200
        assert client.get("/silence/47351").status_code == 200
        assert client.get("/self-scout").status_code == 200
        games_resp = client.get("/replay/47351/games")
        assert games_resp.status_code == 200
        first_gid = games_resp.json()["games"][0]["game_id"]
        assert client.get(f"/replay/47351/{first_gid}").status_code == 200

        assert len(seen_kwargs) == 6
        assert all(kw.get("read_only") is True for kw in seen_kwargs)

    def test_replay_games_returns_5_most_recent_for_target_team(self, synthetic_api_db):
        """GET /replay/{team_id}/games returns 5 most recent complete games by played_at."""
        client = TestClient(api_mod.app)
        resp = client.get("/replay/47351/games")
        assert resp.status_code == 200
        data = resp.json()

        assert data["team_id"] == "47351"
        assert data["team_name"] == "Cloud9 Kia"
        games = data["games"]
        assert len(games) == 5

        # Rich synthetic fixture creates 10 games across 2026-11-01..2026-11-05 (G1 at 10:00, G2 at 11:00)
        assert [g["game_id"] for g in games] == ["RG5_2", "RG5_1", "RG4_2", "RG4_1", "RG3_2"]

        expected_keys = {
            "game_id", "series_id", "game_num", "played_at",
            "patch", "opponent_id", "opponent_name", "side",
        }
        for g in games:
            assert set(g.keys()) == expected_keys
            assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", g["played_at"])
            assert g["opponent_id"] == "340"
            assert g["opponent_name"] == "FlyQuest"
            assert g["side"] == "blue"
            assert g["patch"] == "16.20"

        # Dates are non-increasing (most recent first)
        dates = [g["played_at"] for g in games]
        assert dates == sorted(dates, reverse=True)

    def test_replay_404_for_non_target_team_and_game_outside_top_5(self, synthetic_api_db):
        """Non-target teams and games outside a target team's 5 most recent must return 404."""
        client = TestClient(api_mod.app)

        # Non-target team (known in names table as T1, and completely unknown 999999)
        for non_target in ("T1", "999999"):
            resp_games = client.get(f"/replay/{non_target}/games")
            assert resp_games.status_code == 404
            resp_detail = client.get(f"/replay/{non_target}/RG5_2")
            assert resp_detail.status_code == 404

        # RG1_1 is a real complete game for 47351, but outside its 5 most recent games
        resp_old = client.get("/replay/47351/RG1_1")
        assert resp_old.status_code == 404

        # Non-existent game id for a target team
        resp_missing = client.get("/replay/47351/NO_SUCH_GAME")
        assert resp_missing.status_code == 404

    def test_replay_detail_slots_ordered_1_to_20_and_all_tell_states(self, synthetic_api_db):
        """GET /replay/{team_id}/{game_id} returns 20 ordered slots, Fearless burned, and per-slot tell states."""
        # Seed deterministic tells for team 47351 covering all 5 possible slot states across RG5_1 & RG5_2:
        # In RG5_1 (game_num=1):
        #   - Azir: banned at slot 1 -> banned from slot 1..20
        #   - Yone: banned at slot 13 -> open slots 1..12, banned slots 13..20
        #   - Garen: picked by 47351 (blue) at slot 7 -> open slots 1..6, picked_by_team slots 7..20
        #   - Renekton: picked by 340 (red) at slot 8 -> open slots 1..7, picked_by_opponent slots 8..20
        #   - Aatrox: never picked or banned in RG5_1 -> open all 20 slots
        # In RG5_2 (game_num=2, same series RICH-5):
        #   - Garen & Renekton were picked in RG5_1 -> burned across all 20 slots!
        #   - Darius: picked by 47351 at slot 7 -> open 1..6, picked_by_team 7..20
        con = duckdb.connect(str(synthetic_api_db))
        con.execute("DELETE FROM left_open WHERE team = '47351'")
        con.executemany(
            "INSERT INTO left_open VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("47351", "P1", "top", "Aatrox", 7, 3, 4.5, 0.01, 0.15, 0.3),
                ("47351", "P1", "top", "Garen", 5, 2, 3.0, 0.04, 0.25, 0.2),
                ("47351", "P1", "top", "Renekton", 6, 1, 3.5, 0.08, 0.35, 0.2),
                ("47351", "P2", "mid", "Azir", 6, 1, 3.2, 0.10, 0.40, 0.2),
                ("47351", "P2", "mid", "Yone", 5, 1, 2.5, 0.15, 0.45, 0.2),
                ("47351", "P1", "top", "Darius", 5, 2, 3.0, 0.20, 0.50, 0.2),
                # k < 4 must be filtered out just like /silence
                ("47351", "P4", "bot", "Jinx", 3, 0, 1.5, 0.001, 0.01, 0.5),
            ],
        )
        con.close()

        client = TestClient(api_mod.app)

        # --- Game 1 (RG5_1): nothing burned ---
        resp_g1 = client.get("/replay/47351/RG5_1")
        assert resp_g1.status_code == 200
        g1 = resp_g1.json()

        assert g1["team_id"] == "47351"
        assert g1["team_name"] == "Cloud9 Kia"
        assert g1["min_n"] == 4
        assert g1["note"] == "We only show tells with at least 4 qualifying games."
        assert g1["burned"] == []

        slots_g1 = g1["slots"]
        assert len(slots_g1) == 20
        assert [s["slot"] for s in slots_g1] == list(range(1, 21))
        allowed_slot_keys = {"slot", "phase", "side", "team_id", "team_name", "action", "champion"}
        for s in slots_g1:
            assert set(s.keys()) == allowed_slot_keys
            assert s["action"] in ("ban", "pick")
            assert s["side"] in ("blue", "red")

        tells_g1 = {t["champion"]: t for t in g1["tells"]}
        assert "Jinx" not in tells_g1  # k=3 excluded
        assert len(tells_g1) == 6
        assert tells_g1["Aatrox"]["label"] == "strong"
        assert tells_g1["Garen"]["label"] == "lead"

        # Check exact state trajectories across all 20 slots
        assert tells_g1["Aatrox"]["slot_states"] == ["open"] * 20
        assert tells_g1["Azir"]["slot_states"] == ["banned"] * 20
        assert tells_g1["Yone"]["slot_states"] == ["open"] * 12 + ["banned"] * 8
        assert tells_g1["Garen"]["slot_states"] == ["open"] * 6 + ["picked_by_team"] * 14
        assert tells_g1["Renekton"]["slot_states"] == ["open"] * 7 + ["picked_by_opponent"] * 13

        # --- Game 2 (RG5_2): Fearless burned computed from RG5_1 picks in series RICH-5 ---
        resp_g2 = client.get("/replay/47351/RG5_2")
        assert resp_g2.status_code == 200
        g2 = resp_g2.json()

        expected_burned_g2 = sorted(
            {s["champion"] for s in slots_g1 if s["action"] == "pick"}
        )
        assert len(expected_burned_g2) == 10
        assert g2["burned"] == expected_burned_g2
        assert [s["slot"] for s in g2["slots"]] == list(range(1, 21))

        tells_g2 = {t["champion"]: t for t in g2["tells"]}
        # Garen and Renekton were picked in RG5_1 -> burned across all 20 slots of RG5_2
        assert tells_g2["Garen"]["slot_states"] == ["burned"] * 20
        assert tells_g2["Renekton"]["slot_states"] == ["burned"] * 20
        # Darius is picked by 47351 at slot 7 in RG5_2
        assert tells_g2["Darius"]["slot_states"] == ["open"] * 6 + ["picked_by_team"] * 14
        # Azir is banned at slot 13 in RG5_2 (index 6 in g2_bans)
        assert tells_g2["Azir"]["slot_states"] == ["open"] * 12 + ["banned"] * 8


class TestWebUI:
    """TestClient suite for the static Silence Report web UI and JS endpoint contract."""

    def test_root_serves_index_html_offline(self):
        client = TestClient(api_mod.app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
        html = resp.text

        assert "UNPICKED INTENT" in html
        assert "Self-scout: Cloud9 Kia" in html
        assert "Only tells with at least 4 qualifying games are shown." in html
        assert "We only show tells with at least 4 qualifying games." in html
        assert "Strict filter: none survive correction yet" in html
        assert (
            "Leads are patterns worth watching; strong tells survive correction "
            "for testing thousands of patterns."
        ) not in html
        expected_explainer = (
            'STRONG (q &lt; 0.2) survives Benjamini-Hochberg correction across '
            '<span class="total-tests-count">2,029</span> tested player-champion '
            "patterns. LEADS are directional: real patterns, not yet proven."
        )
        assert html.count(expected_explainer) == 2
        assert 'href="/styles.css"' in html
        assert 'src="/app.js"' in html

        # Must work completely offline (no external CDN URLs)
        assert "http://" not in html
        assert "https://" not in html

    def test_static_assets_load_and_are_offline(self):
        client = TestClient(api_mod.app)

        css_resp = client.get("/styles.css")
        assert css_resp.status_code == 200
        assert "text/css" in css_resp.headers.get("content-type", "")
        assert len(css_resp.text) > 100
        assert "http://" not in css_resp.text
        assert "https://" not in css_resp.text

        js_resp = client.get("/app.js")
        assert js_resp.status_code == 200
        assert "javascript" in js_resp.headers.get("content-type", "")
        assert len(js_resp.text) > 100
        assert "http://" not in js_resp.text
        assert "https://" not in js_resp.text

    def test_every_js_endpoint_exists_in_api(self, synthetic_api_db):
        """Every API URL called by src/unpicked_intent/web/app.js must exist as an API route."""
        js_path = api_mod.WEB_DIR / "app.js"
        assert js_path.is_file()
        js_text = js_path.read_text(encoding="utf-8")

        # Extract literal/template URLs passed to fetchJson(...) or fetch(...)
        raw_urls = re.findall(r"""(?:fetchJson|fetch)\(\s*["'`]([^"'`]+)["'`]""", js_text)
        assert len(raw_urls) >= 5, f"Expected at least 5 API calls in app.js, found {raw_urls}"

        client = TestClient(api_mod.app)
        first_game_id = client.get("/replay/47351/games").json()["games"][0]["game_id"]

        def _materialize(url: str) -> str:
            parts = re.split(r"\$\{[^}]+\}", url)
            if len(parts) == 1:
                return url
            # First template placeholder is team_id; second (if any) is game_id.
            out = parts[0] + "47351" + parts[1]
            for extra in parts[2:]:
                out += first_game_id + extra
            return out

        concrete_paths = sorted({_materialize(url) for url in raw_urls})
        assert "/meta" in concrete_paths
        assert "/teams" in concrete_paths
        assert "/self-scout" in concrete_paths
        assert "/silence/47351" in concrete_paths
        assert "/replay/47351/games" in concrete_paths
        assert f"/replay/47351/{first_game_id}" in concrete_paths

        # Every extracted path must match a registered FastAPI APIRoute (not StaticFiles)
        api_routes = [r for r in api_mod.app.routes if isinstance(r, APIRoute)]
        for path in concrete_paths:
            matched = any(
                route.matches({"type": "http", "path": path, "method": "GET"})[0].name == "FULL"
                for route in api_routes
            )
            assert matched, f"JS calls '{path}', which does not match any FastAPI APIRoute"

        # And every endpoint responds 200 against the synthetic DuckDB
        for path in concrete_paths:
            resp = client.get(path)
            assert resp.status_code == 200, f"JS endpoint '{path}' returned {resp.status_code}"

    def test_self_scout_banner_and_lead_action_wording(self):
        """Self-scout banner uses hedged wording and action list hedges leads with p < 0.05."""
        client = TestClient(api_mod.app)

        html = client.get("/").text
        assert (
            "This is the exact Fearless Draft Silence Report opposing coaches see for Cloud9 Kia"
            not in html
        )
        assert (
            "This is what an opponent could read from Cloud9 Kia's own drafts using this report"
            in html
        )
        assert "We only show tells with at least 4 qualifying games." in html
        assert "Draft Replay" in html

        js = client.get("/app.js").text
        assert "So far, leaving it open has rarely drawn it." in js
        assert "Leaving it open rarely draws it." in js
        assert "ACTION_LEAD_MAX_P = 0.05" in js
        assert "Open now, history says they skip it" in js
        assert "data/replay/${rest}.json" in js
        assert "data/meta.json" in js
        assert "kpi-strong-zero" in js
        assert "TARGET_TEAMS_ORDER" in js
        assert "strong_count" in js

        css = client.get("/styles.css").text
        assert ".kpi-chip.kpi-strong.kpi-strong-zero" in css
        assert ".kpi-sub" in css

    def test_ui_polish_passes_contract(self):
        """Verify Passes 1-4: sentence-case labels, quiet KPI + Patterns tested, 6-col tells, muted accents."""
        client = TestClient(api_mod.app)
        html = client.get("/").text
        css = client.get("/styles.css").text
        js = client.get("/app.js").text

        # PASS 1: uppercase CSS restricted strictly to badge chips (.badge-signal / .badge-mini)
        assert css.count("text-transform: uppercase") == 1
        assert "Opponent silence report" in html
        assert "OPPONENT SILENCE REPORT" not in html
        assert "OPPONENT SILENCE REPORT" not in js
        assert "LCS targets (" in js
        assert "All other teams (" in js
        assert '<span class="c9-tag">Self</span>' in js
        assert '<span class="c9-tag">SELF</span>' not in js

        # PASS 2: quiet KPI strip (no amber .kpi-strong box) + 4th Patterns tested chip (not hardcoded)
        assert ".kpi-chip.kpi-strong {" not in css
        assert ".kpi-chip.kpi-strong .kpi-value {" not in css
        assert '<span class="kpi-label">Patterns tested</span>' in html
        assert '<span id="kpi-patterns-tested-count" class="kpi-value">&mdash;</span>' in html
        assert "kpiPatternsTestedCount" in js

        # PASS 3: Left-Open Tells table drops duplicate n/Obs/Exp columns and uses exact headers
        assert "<th>Tier</th>" in js
        assert "<th>Champion</th>" in js
        assert "<th>Player</th>" in js
        assert "<th>Observed vs expected (n open games)</th>" in js
        assert '<th class="num">p</th>' in js
        assert '<th class="num">q</th>' in js
        assert '<th class="num">n</th>' not in js
        assert '<th class="num">Obs</th>' not in js
        assert '<th class="num">Exp</th>' not in js

        # PASS 4: accent consolidation & no gradients
        assert "linear-gradient" not in css
        eyebrow_block = re.search(r"\.report-mode-eyebrow\s*\{[^}]*\}", css)
        assert eyebrow_block is not None
        assert "color: var(--text-muted);" in eyebrow_block.group(0)
        assert "var(--accent)" not in eyebrow_block.group(0)

        dot_block = re.search(r"\.honesty-dot\s*\{[^}]*\}", css)
        assert dot_block is not None
        assert "background: var(--text-muted);" in dot_block.group(0)

        search_icon_block = re.search(r"\.search-icon\s*\{[^}]*\}", css)
        assert search_icon_block is not None
        assert "color: var(--text-muted);" in search_icon_block.group(0)

    def test_replay_next_open_tell_advances_and_disables(self, synthetic_api_db):
        """Next open tell advances to the next team-acting slot with an open tell and disables when exhausted."""
        client = TestClient(api_mod.app)
        html = client.get("/").text
        js_path = api_mod.WEB_DIR / "app.js"
        js_text = js_path.read_text(encoding="utf-8")

        # Markup & shortcut contract
        stepper_match = re.search(
            r'<div class="stepper-controls">[\s\S]*?</div>', html
        )
        assert stepper_match is not None
        stepper_html = stepper_match.group(0)
        assert 'id="replay-prev-btn"' in stepper_html
        assert 'id="replay-next-btn"' in stepper_html
        assert 'id="replay-next-open-tell-btn"' in stepper_html
        assert "Next open tell" in stepper_html
        assert 'e.key === "n"' in js_text

        # Real replay payload from fixture DB
        games = client.get("/replay/47351/games").json()["games"]
        assert len(games) > 0
        real_detail = client.get(f"/replay/47351/{games[0]['game_id']}").json()

        # Execute the real JS helper functions from app.js in Node
        node_script = r"""
const fs = require("fs");
const jsPath = process.argv[1];
const detailJson = fs.readFileSync(0, "utf8");
const src = fs.readFileSync(jsPath, "utf8");

const fnFind = src.match(/function findNextOpenTellSlotIdx\(detail, fromIdx\) \{[\s\S]*?\n\}/);
const fnAdvance = src.match(/function advanceToNextOpenTellSlot\(\) \{[\s\S]*?\n\}/);
if (!fnFind || !fnAdvance) {
  throw new Error("Missing findNextOpenTellSlotIdx or advanceToNextOpenTellSlot in app.js");
}

const state = { replayDetail: null, replaySlotIdx: 0 };
const btn = { disabled: true };
let renders = 0;
function renderReplaySlotView() {
  renders += 1;
  btn.disabled = findNextOpenTellSlotIdx(state.replayDetail, state.replaySlotIdx) < 0;
}

eval(fnFind[0] + "\n" + fnAdvance[0]);

// 1. Controlled synthetic detail:
// slot 0: opp acting (open) -> skipped
// slot 1: team acting, tell A open -> QUALIFIES (idx 1)
// slot 2: opp acting (open) -> skipped
// slot 3: team acting, both tells closed (banned / picked_by_opponent) -> skipped
// slot 4: team acting, tell B open -> QUALIFIES (idx 4)
// slot 5: team acting, both tells closed -> exhausted
const synthDetail = {
  team_id: "47351",
  slots: [
    { slot: 1, team_id: "340" },
    { slot: 2, team_id: "47351" },
    { slot: 3, team_id: "340" },
    { slot: 4, team_id: "47351" },
    { slot: 5, team_id: "47351" },
    { slot: 6, team_id: "47351" },
  ],
  tells: [
    { champion: "Azir", slot_states: ["open", "open", "banned", "banned", "banned", "banned"] },
    { champion: "Orianna", slot_states: ["burned", "burned", "burned", "picked_by_opponent", "open", "picked_by_team"] },
  ],
};

state.replayDetail = synthDetail;
state.replaySlotIdx = 0;
renderReplaySlotView();
if (btn.disabled !== false) throw new Error("Expected button enabled at slot 0");

if (!advanceToNextOpenTellSlot() || state.replaySlotIdx !== 1) {
  throw new Error("Expected first jump to idx 1, got " + state.replaySlotIdx);
}
if (btn.disabled !== false) throw new Error("Expected button still enabled at idx 1");

if (!advanceToNextOpenTellSlot() || state.replaySlotIdx !== 4) {
  throw new Error("Expected second jump to idx 4, got " + state.replaySlotIdx);
}
if (btn.disabled !== true) throw new Error("Expected button disabled once exhausted at idx 4");

// No wrap when exhausted
if (advanceToNextOpenTellSlot() !== false || state.replaySlotIdx !== 4) {
  throw new Error("Expected exhausted jump to return false without wrapping");
}

// Re-enables on game change
const freshGame = {
  team_id: "47351",
  slots: [
    { slot: 1, team_id: "340" },
    { slot: 2, team_id: "47351" },
  ],
  tells: [{ champion: "Azir", slot_states: ["open", "open"] }],
};
state.replayDetail = freshGame;
state.replaySlotIdx = 0;
renderReplaySlotView();
if (btn.disabled !== false) throw new Error("Expected button re-enabled on game change");

// 2. Real replay payload from TestClient
const realDetail = JSON.parse(detailJson);
state.replayDetail = realDetail;
state.replaySlotIdx = 0;
renderReplaySlotView();

const visited = [];
while (advanceToNextOpenTellSlot()) {
  const idx = state.replaySlotIdx;
  visited.push(idx);
  const slot = realDetail.slots[idx];
  if (String(slot.team_id) !== String(realDetail.team_id)) {
    throw new Error("Jumped to non-team slot " + idx);
  }
  const hasOpen = realDetail.tells.some((t) => t.slot_states[idx] === "open");
  if (!hasOpen) {
    throw new Error("Jumped to slot with no open tells: " + idx);
  }
}
if (visited.length === 0) throw new Error("Expected at least one qualifying slot in fixture replay");
if (btn.disabled !== true) throw new Error("Expected button disabled after exhausting fixture replay");

console.log(JSON.stringify({ visited, renders }));
"""
        proc = subprocess.run(
            ["node", "-e", node_script, str(js_path)],
            input=json.dumps(real_detail),
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, f"Node test failed:\nSTDOUT: {proc.stdout}\nSTDERR: {proc.stderr}"
        result = json.loads(proc.stdout.strip())
        assert len(result["visited"]) > 0
