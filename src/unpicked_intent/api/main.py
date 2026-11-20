"""FastAPI layer — serves PRECOMPUTED aggregates from derived/unpicked.duckdb only.

Never expose raw feed rows or read raw feeds. DuckDB is opened read-only.

Endpoints:
    GET /health             — liveness probe
    GET /teams              — teams with tell and early-ban counts, target teams first
    GET /silence/{team_id}  — ranked left-open tells + early-ban pressure (n >= 4 only)
    GET /self-scout         — same as /silence/47351 (Cloud9 Kia)
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles

# DERIVED is the repo root's derived/ folder (next to pyproject.toml).
# src/unpicked_intent/api/main.py → parents[3] = repo root.
DERIVED = Path(__file__).resolve().parents[3] / "derived"
DB_PATH = DERIVED / "unpicked.duckdb"
WEB_DIR = Path(__file__).resolve().parents[1] / "web"

# Target LCS teams listed first in /teams (in this order):
# FlyQuest, Team Liquid, LYON, Shopify Rebellion, Disguised, Dignitas, Sentinels, Cloud9 Kia
TARGET_TEAMS = [340, 47363, 54198, 53073, 52416, 47499, 56389, 47351]
SELF_SCOUT_TEAM_ID = "47351"

MIN_N = 4
STRONG_Q_THRESHOLD = 0.2
MIN_N_NOTE = "Only tells with at least 4 qualifying games are shown."

app = FastAPI(title="Unpicked Intent Engine")


def _connect_readonly() -> duckdb.DuckDBPyConnection:
    """Open derived/unpicked.duckdb in read-only mode, or raise HTTP 503 if missing."""
    if not DB_PATH.is_file():
        raise HTTPException(
            status_code=503,
            detail=(
                f"Scouting database not found at {DB_PATH}. "
                "Run the build, baseline, and signals pipeline first."
            ),
        )
    return duckdb.connect(str(DB_PATH), read_only=True)


def _classify_label(q_value: float) -> str:
    """Classify a tell as 'strong' (q < 0.2) or 'lead' (q >= 0.2)."""
    return "strong" if q_value < STRONG_Q_THRESHOLD else "lead"


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/teams")
def list_teams() -> list[dict]:
    """Return all teams with their name, left_open count, and early_ban count.

    Target teams (TARGET_TEAMS) are listed first in their defined order,
    followed by all other teams sorted by name.
    """
    con = _connect_readonly()
    try:
        name_rows = con.execute(
            "SELECT id, name FROM names WHERE kind = 'team'"
        ).fetchall()
        team_names: dict[str, str] = {str(r[0]): str(r[1]) for r in name_rows}

        lo_rows = con.execute(
            "SELECT team, COUNT(*) FROM left_open WHERE k >= ? GROUP BY team",
            [MIN_N],
        ).fetchall()
        lo_counts: dict[str, int] = {str(r[0]): int(r[1]) for r in lo_rows}

        eb_rows = con.execute(
            "SELECT team, COUNT(*) FROM early_ban GROUP BY team"
        ).fetchall()
        eb_counts: dict[str, int] = {str(r[0]): int(r[1]) for r in eb_rows}
    finally:
        con.close()

    all_team_ids = set(team_names.keys()) | set(lo_counts.keys()) | set(eb_counts.keys())
    target_order = {str(tid): idx for idx, tid in enumerate(TARGET_TEAMS)}

    def _sort_key(tid: str) -> tuple[int, int, str, str]:
        if tid in target_order:
            return (0, target_order[tid], "", tid)
        return (1, 0, team_names.get(tid, tid).lower(), tid)

    result = []
    for tid in sorted(all_team_ids, key=_sort_key):
        result.append(
            {
                "team_id": tid,
                "name": team_names.get(tid, tid),
                "left_open_count": lo_counts.get(tid, 0),
                "early_ban_count": eb_counts.get(tid, 0),
            }
        )
    return result


def _get_silence_report(team_id: str) -> dict:
    """Build the Silence Report payload for a single team."""
    tid = str(team_id)
    con = _connect_readonly()
    try:
        name_row = con.execute(
            "SELECT name FROM names WHERE kind = 'team' AND id = ?",
            [tid],
        ).fetchone()

        lo_exists = con.execute(
            "SELECT COUNT(*) FROM left_open WHERE team = ?",
            [tid],
        ).fetchone()[0]

        eb_exists = con.execute(
            "SELECT COUNT(*) FROM early_ban WHERE team = ?",
            [tid],
        ).fetchone()[0]

        if name_row is None and lo_exists == 0 and eb_exists == 0:
            raise HTTPException(status_code=404, detail=f"Team '{tid}' not found.")

        team_name = str(name_row[0]) if name_row is not None else tid

        lo_rows = con.execute(
            """
            SELECT
                lo.player_id,
                COALESCE(n.name, lo.player_id) AS player_name,
                lo.champion,
                lo.role,
                lo.k,
                lo.t,
                lo.expected,
                lo.p_value,
                lo.q_value,
                lo.fit
            FROM left_open lo
            LEFT JOIN names n
              ON n.kind = 'player' AND n.id = lo.player_id
            WHERE lo.team = ? AND lo.k >= ?
            ORDER BY lo.p_value ASC, lo.champion ASC, lo.player_id ASC
            """,
            [tid, MIN_N],
        ).fetchall()

        eb_rows = con.execute(
            """
            SELECT
                champion,
                n_bans,
                team_mean_slot,
                baseline_mean_slot,
                delta
            FROM early_ban
            WHERE team = ?
            ORDER BY delta DESC, champion ASC
            """,
            [tid],
        ).fetchall()
    finally:
        con.close()

    left_open = [
        {
            "player_id": str(r[0]),
            "player_name": str(r[1]),
            "champion": str(r[2]),
            "role": str(r[3]),
            "k": int(r[4]),
            "taken": int(r[5]),
            "expected": float(r[6]),
            "p_value": float(r[7]),
            "q_value": float(r[8]),
            "fit": float(r[9]),
            "label": _classify_label(float(r[8])),
        }
        for r in lo_rows
    ]

    early_ban = [
        {
            "champion": str(r[0]),
            "n_bans": int(r[1]),
            "team_mean_slot": float(r[2]),
            "baseline_mean_slot": float(r[3]),
            "delta": float(r[4]),
        }
        for r in eb_rows
    ]

    return {
        "team_id": tid,
        "team_name": team_name,
        "left_open": left_open,
        "early_ban": early_ban,
        "min_n": MIN_N,
        "note": MIN_N_NOTE,
    }


@app.get("/silence/{team_id}")
def silence_report(team_id: str) -> dict:
    """Return left-open tells and early-ban pressure for a single team."""
    return _get_silence_report(team_id)


@app.get("/self-scout")
def self_scout() -> dict:
    """Return the Silence Report for Cloud9 Kia (team 47351)."""
    return _get_silence_report(SELF_SCOUT_TEAM_ID)


# Mount the plain HTML/CSS/JS Silence Report UI at / AFTER all API routes so
# /health, /teams, /silence/{team_id}, and /self-scout always match first.
app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")
