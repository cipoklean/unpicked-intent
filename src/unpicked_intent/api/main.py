"""FastAPI layer — serves PRECOMPUTED aggregates from derived/unpicked.duckdb only.

Never expose raw feed rows or read raw feeds. DuckDB is opened read-only.

Endpoints:
    GET /health                       — liveness probe
    GET /meta                         — dataset-level methodology counts (total_tests, min_n)
    GET /teams                        — teams with tell, strong, and early-ban counts, target teams first
    GET /silence/{team_id}            — ranked left-open tells + early-ban pressure (n >= 4 only)
    GET /self-scout                   — same as /silence/47351 (Cloud9 Kia)
    GET /replay/{team_id}/games       — 5 most recent complete games for a target team
    GET /replay/{team_id}/{game_id}   — 20-slot draft replay + per-slot tell states (target teams)
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
TARGET_TEAM_IDS = {str(tid) for tid in TARGET_TEAMS}
SELF_SCOUT_TEAM_ID = "47351"

MIN_N = 4
STRONG_Q_THRESHOLD = 0.2
MIN_N_NOTE = "Only tells with at least 4 qualifying games are shown."
REPLAY_MAX_GAMES = 5
REPLAY_MIN_N_NOTE = "We only show tells with at least 4 qualifying games."

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


@app.get("/meta")
def get_meta() -> dict:
    """Return dataset-level methodology counts from derived/unpicked.duckdb."""
    con = _connect_readonly()
    try:
        row = con.execute(
            "SELECT COUNT(*) FROM left_open WHERE k >= ?",
            [MIN_N],
        ).fetchone()
        total_tests = int(row[0]) if row is not None else 0
    finally:
        con.close()

    return {
        "total_tests": total_tests,
        "min_n": MIN_N,
        "strong_q_threshold": STRONG_Q_THRESHOLD,
    }


@app.get("/teams")
def list_teams() -> list[dict]:
    """Return all teams with their name, left_open, strong, and early_ban counts.

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
            """
            SELECT
                team,
                COUNT(*) AS left_open_count,
                SUM(CASE WHEN q_value < ? THEN 1 ELSE 0 END) AS strong_count
            FROM left_open
            WHERE k >= ?
            GROUP BY team
            """,
            [STRONG_Q_THRESHOLD, MIN_N],
        ).fetchall()
        lo_counts: dict[str, int] = {str(r[0]): int(r[1]) for r in lo_rows}
        strong_counts: dict[str, int] = {str(r[0]): int(r[2] or 0) for r in lo_rows}

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
                "strong_count": strong_counts.get(tid, 0),
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


def _format_iso_date(value: object) -> str:
    """Convert a DuckDB TIMESTAMP or ISO string to an ISO date (YYYY-MM-DD)."""
    if value is None:
        return ""
    if hasattr(value, "date"):
        return value.date().isoformat()
    text = str(value).strip()
    return text[:10] if len(text) >= 10 else text


def _require_target_team(team_id: str) -> str:
    """Validate team_id is one of the 8 TARGET_TEAMS or raise HTTP 404."""
    tid = str(team_id)
    if tid not in TARGET_TEAM_IDS:
        raise HTTPException(
            status_code=404,
            detail=f"Draft Replay is only available for target teams; '{tid}' is not a target team.",
        )
    return tid


def _get_recent_target_games(con: duckdb.DuckDBPyConnection, team_id: str) -> list[dict]:
    """Return up to REPLAY_MAX_GAMES (5) most recent complete games for team_id."""
    rows = con.execute(
        """
        WITH team_games AS (
            SELECT
                de.game_id,
                de.series_id,
                de.game_num,
                MAX(de.played_at) AS game_played_at,
                MIN(de.patch) AS patch,
                MIN(de.side) AS side
            FROM draft_events de
            INNER JOIN game_flags gf
              ON gf.series_id = de.series_id
             AND gf.game_num = de.game_num
            WHERE de.team = ?
              AND gf.is_complete = TRUE
            GROUP BY de.game_id, de.series_id, de.game_num
            ORDER BY
                game_played_at DESC NULLS LAST,
                de.series_id DESC,
                de.game_num DESC,
                de.game_id DESC
            LIMIT ?
        ),
        opponents AS (
            SELECT
                de.game_id,
                MIN(de.team) AS opponent_id
            FROM draft_events de
            INNER JOIN team_games tg
              ON tg.game_id = de.game_id
            WHERE de.team <> ?
            GROUP BY de.game_id
        )
        SELECT
            tg.game_id,
            tg.series_id,
            tg.game_num,
            tg.game_played_at,
            tg.patch,
            opp.opponent_id,
            COALESCE(n.name, opp.opponent_id) AS opponent_name,
            tg.side
        FROM team_games tg
        LEFT JOIN opponents opp
          ON opp.game_id = tg.game_id
        LEFT JOIN names n
          ON n.kind = 'team' AND n.id = opp.opponent_id
        ORDER BY
            tg.game_played_at DESC NULLS LAST,
            tg.series_id DESC,
            tg.game_num DESC,
            tg.game_id DESC
        """,
        [team_id, REPLAY_MAX_GAMES, team_id],
    ).fetchall()

    return [
        {
            "game_id": str(r[0]),
            "series_id": str(r[1]),
            "game_num": int(r[2]),
            "played_at": _format_iso_date(r[3]),
            "patch": str(r[4]),
            "opponent_id": str(r[5]) if r[5] is not None else "",
            "opponent_name": str(r[6]) if r[6] is not None else "",
            "side": str(r[7]),
        }
        for r in rows
    ]


def _compute_tell_slot_states(
    champion: str,
    team_id: str,
    slots: list[dict],
    burned: set[str],
) -> list[str]:
    """Return a tell's state after each draft slot (1..20).

    States:
      - 'burned'             : Fearless — picked in an earlier game of the same series
      - 'banned'             : banned by either team at or before this slot
      - 'picked_by_team'     : picked by the scouted team at or before this slot
      - 'picked_by_opponent' : picked by the opponent at or before this slot
      - 'open'               : still available after this slot
    """
    if champion in burned:
        return ["burned"] * len(slots)

    states: list[str] = []
    current = "open"
    for slot_ev in slots:
        if current == "open" and slot_ev["champion"] == champion:
            if slot_ev["action"] == "ban":
                current = "banned"
            elif slot_ev["team_id"] == team_id:
                current = "picked_by_team"
            else:
                current = "picked_by_opponent"
        states.append(current)
    return states


@app.get("/replay/{team_id}/games")
def replay_games(team_id: str) -> dict:
    """Return the 5 most recent complete games for one of the 8 target teams."""
    tid = _require_target_team(team_id)
    con = _connect_readonly()
    try:
        name_row = con.execute(
            "SELECT name FROM names WHERE kind = 'team' AND id = ?",
            [tid],
        ).fetchone()
        team_name = str(name_row[0]) if name_row is not None else tid
        games = _get_recent_target_games(con, tid)
    finally:
        con.close()

    return {
        "team_id": tid,
        "team_name": team_name,
        "games": games,
    }


@app.get("/replay/{team_id}/{game_id}")
def replay_game_detail(team_id: str, game_id: str) -> dict:
    """Return 20-slot draft replay and per-slot tell states for one recent target-team game."""
    tid = _require_target_team(team_id)
    gid = str(game_id)
    con = _connect_readonly()
    try:
        recent_games = _get_recent_target_games(con, tid)
        game_meta = next((g for g in recent_games if g["game_id"] == gid), None)
        if game_meta is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Game '{gid}' is not in team '{tid}'s "
                    f"{REPLAY_MAX_GAMES} most recent complete games."
                ),
            )

        name_rows = con.execute(
            "SELECT id, name FROM names WHERE kind = 'team'"
        ).fetchall()
        team_names: dict[str, str] = {str(r[0]): str(r[1]) for r in name_rows}
        team_name = team_names.get(tid, tid)

        slot_rows = con.execute(
            """
            SELECT slot, phase, side, team, action, champion
            FROM draft_events
            WHERE game_id = ?
            ORDER BY slot ASC
            """,
            [gid],
        ).fetchall()

        burned_set: set[str] = set()
        if game_meta["game_num"] > 1:
            burned_rows = con.execute(
                """
                SELECT DISTINCT champion
                FROM draft_events
                WHERE series_id = ? AND game_num < ? AND action = 'pick'
                ORDER BY champion ASC
                """,
                [game_meta["series_id"], game_meta["game_num"]],
            ).fetchall()
            burned_set = {str(r[0]) for r in burned_rows}

        lo_rows = con.execute(
            """
            SELECT
                COALESCE(n.name, lo.player_id) AS player_name,
                lo.champion,
                lo.role,
                lo.k,
                lo.t,
                lo.expected,
                lo.p_value,
                lo.q_value
            FROM left_open lo
            LEFT JOIN names n
              ON n.kind = 'player' AND n.id = lo.player_id
            WHERE lo.team = ? AND lo.k >= ?
            ORDER BY lo.p_value ASC, lo.champion ASC, lo.player_id ASC
            """,
            [tid, MIN_N],
        ).fetchall()
    finally:
        con.close()

    slots = [
        {
            "slot": int(r[0]),
            "phase": str(r[1]),
            "side": str(r[2]),
            "team_id": str(r[3]),
            "team_name": team_names.get(str(r[3]), str(r[3])),
            "action": str(r[4]),
            "champion": str(r[5]),
        }
        for r in slot_rows
    ]

    tells = [
        {
            "player_name": str(r[0]),
            "champion": str(r[1]),
            "role": str(r[2]),
            "k": int(r[3]),
            "taken": int(r[4]),
            "expected": float(r[5]),
            "p_value": float(r[6]),
            "q_value": float(r[7]),
            "label": _classify_label(float(r[7])),
            "slot_states": _compute_tell_slot_states(str(r[1]), tid, slots, burned_set),
        }
        for r in lo_rows
    ]

    return {
        "team_id": tid,
        "team_name": team_name,
        "game": game_meta,
        "burned": sorted(burned_set),
        "slots": slots,
        "tells": tells,
        "min_n": MIN_N,
        "note": REPLAY_MIN_N_NOTE,
    }


# Mount the plain HTML/CSS/JS Silence Report UI at / AFTER all API routes so
# /health, /meta, /teams, /silence/{team_id}, /self-scout, and /replay/* always match first.
app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")
