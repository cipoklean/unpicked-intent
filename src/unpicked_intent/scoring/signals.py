"""Observed-behavior signals — the thin layer.

Unit of evidence = ONE GAME where the champion stayed legal through that
player's pick. Never count slots: within one draft a champion is legal at
all five pick slots and those are not independent chances.

    left-open:  champion legal in k games, taken t
                significance = Poisson-binomial CDF (each game has its own p)
    early-ban:  mean ban position vs. meta-average (min ban count applies)
    fit:        candidate pool = player's pool ∩ meta-relevant champions;
                rank left-open tells by how strongly the flag predicted a pick

Honesty rules:
    - rank tells by p-value
    - only emit tells with n >= 4 qualifying games (MIN_N below)
    - every emitted tell carries n and expected-vs-observed
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

import duckdb

MIN_N = 4  # Minimum qualifying games to emit a tell
POOL_MIN_GAMES = 3  # Champion must appear ≥3 times in player's history


DERIVED = Path(__file__).resolve().parents[3] / "derived"
DB_PATH = DERIVED / "unpicked.duckdb"


# ── Poisson-binomial CDF via DP ──────────────────────────────────────────────

def poisson_binomial_cdf(k: int, probs: list[float]) -> float:
    """Compute P(X ≤ k) for a Poisson-binomial distribution.

    Uses a simple DP loop: dp[j] = P(exactly j successes).
    Each game can have a different probability p.

    Args:
        k: Number of successes to compute CDF for.
        probs: List of per-trial success probabilities.

    Returns:
        P(X ≤ k) — cumulative probability.
    """
    n = len(probs)
    if n == 0:
        return 1.0 if k >= 0 else 0.0

    # dp[j] = P(exactly j successes after processing trials so far)
    dp = [0.0] * (n + 1)
    dp[0] = 1.0

    for p in probs:
        # Update backwards to avoid using updated values
        for j in range(n, 0, -1):
            dp[j] = dp[j] * (1.0 - p) + dp[j - 1] * p
        dp[0] = dp[0] * (1.0 - p)

    # CDF: P(X ≤ k)
    result = 0.0
    for j in range(min(k, n) + 1):
        result += dp[j]
    return result


def poisson_binomial_p_value(taken: int, probs: list[float]) -> float:
    """Compute P(T ≤ taken) under Poisson-binomial.

    This is the left-tail p-value: probability of observing ≤ taken
    successes given per-game probabilities.

    Args:
        taken: Number of observed successes.
        probs: List of per-trial success probabilities.

    Returns:
        P(T ≤ taken) — left-tail p-value.
    """
    return poisson_binomial_cdf(taken, probs)


# ── Evidence rows (left-open) ────────────────────────────────────────────────

def build_evidence_rows(
    champion_game_rows: list[dict],
    draft_events: list[dict],
    game_flags: list[dict],
    baseline_rows: list[dict],
    player_roles: dict[str, dict],
    rosters_rows: list[dict],
    champion_roles: dict[str, str],
) -> list[dict]:
    """Build evidence rows for left-open analysis.

    For each (team, champion, game) with resolution skipped or taken, where
    the champion is meta_relevant in the baseline for that game's patch:
      - Find the team's player whose inferred role equals the champion's role.
      - Skip if zero or two such players.
      - Keep only if the champion is in that player's pool (played ≥ POOL_MIN_GAMES).
      - Attach baseline pick_rate for that game's patch.

    Args:
        champion_game_rows: Rows from champion_game table joined with patch.
        draft_events: Rows from draft_events table.
        game_flags: Rows from game_flags table.
        baseline_rows: Rows from baseline table.
        player_roles: {player_id: {"role": str, ...}} from infer_players.
        rosters_rows: Rows from rosters.csv (title=="lol", active only).
        champion_roles: {champion: role} mapping.

    Returns:
        List of evidence dicts with keys: team, player_id, role, champion,
        game_id, patch, resolution, pick_rate.
    """
    # Build baseline lookup: (patch, champion) -> baseline row
    baseline_lookup: dict[tuple[str, str], dict] = {}
    for row in baseline_rows:
        if row["meta_relevant"]:
            baseline_lookup[(row["patch"], row["champion"])] = row

    # Build roster lookup: (game_id, team, character_name) -> player_id
    roster_lookup: dict[tuple[str, str, str], str] = {}
    for row in rosters_rows:
        if row.get("title") == "lol" and row.get("participation_status") == "active":
            key = (row["game_id"], row["team_id"], row["character_name"])
            roster_lookup[key] = row["player_id"]

    # Build player→champion history for pool check
    player_champ_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rosters_rows:
        if row.get("title") == "lol" and row.get("participation_status") == "active":
            player_champ_counts[row["player_id"]][row["character_name"]] += 1

    # Build complete game set (non-Bo1, complete)
    complete_games: set[tuple[str, int]] = set()
    for flag in game_flags:
        if flag["is_complete"] and not flag["is_bo1"]:
            complete_games.add((flag["series_id"], flag["game_num"]))

    # Build game_id -> (series_id, game_num) mapping from draft_events
    game_id_to_series: dict[str, tuple[str, int]] = {}
    for ev in draft_events:
        gid = ev.get("game_id", "")
        if gid:
            game_id_to_series[gid] = (ev["series_id"], ev["game_num"])

    # Build roster lookup by (series_id, game_num, team, champion) -> list of player_ids
    # We need to join rosters on game_id, so build game_id from draft_events
    roster_by_game_team_champ: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for row in rosters_rows:
        if row.get("title") == "lol" and row.get("participation_status") == "active":
            key = (row["game_id"], row["team_id"], row["character_name"])
            roster_by_game_team_champ[key].append(row["player_id"])

    # For each game, build (team, role) -> list of player_ids
    # A player's role in a game is their inferred role
    game_team_role_players: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for row in rosters_rows:
        if row.get("title") == "lol" and row.get("participation_status") == "active":
            pid = row["player_id"]
            if pid in player_roles:
                role = player_roles[pid]["role"]
                key = (row["game_id"], row["team_id"], role)
                game_team_role_players[key].append(pid)

    # Build champion_game lookup by game_id
    # champion_game has (series_id, game_num), need game_id
    cg_by_game: dict[str, list[dict]] = defaultdict(list)
    for row in champion_game_rows:
        # Find game_id from series_id + game_num
        # We need a reverse mapping
        pass

    # Build series_id, game_num -> game_id mapping
    series_game_to_id: dict[tuple[str, int], str] = {}
    for ev in draft_events:
        gid = ev.get("game_id", "")
        if gid:
            series_game_to_id[(ev["series_id"], ev["game_num"])] = gid

    # Group champion_game rows by game_id
    cg_by_game_id: dict[str, list[dict]] = defaultdict(list)
    for row in champion_game_rows:
        gid = series_game_to_id.get((row["series_id"], row["game_num"]))
        if gid:
            cg_by_game_id[gid].append(row)

    evidence: list[dict] = []
    for gid, rows in cg_by_game_id.items():
        # Check if this game is complete and non-Bo1
        # Find series_id, game_num for this game_id
        for row in rows:
            sgn = (row["series_id"], row["game_num"])
            break
        else:
            continue

        if sgn not in complete_games:
            continue

        patch = rows[0].get("patch", "")

        for row in rows:
            resolution = row["resolution"]
            if resolution not in ("skipped", "taken"):
                continue

            champion = row["champion"]
            team = row["team"]

            # Check if champion is meta-relevant for this patch
            bl = baseline_lookup.get((patch, champion))
            if bl is None:
                continue

            champ_role = champion_roles.get(champion)
            if not champ_role:
                continue

            # Find the player on this team whose role matches the champion's role
            role_key = (gid, team, champ_role)
            players_with_role = game_team_role_players.get(role_key, [])

            if len(players_with_role) != 1:
                # Skip if zero or two+ players with this role
                continue

            player_id = players_with_role[0]

            # Check if champion is in player's pool (played ≥ POOL_MIN_GAMES)
            champ_count = player_champ_counts.get(player_id, {}).get(champion, 0)
            if champ_count < POOL_MIN_GAMES:
                continue

            evidence.append({
                "team": team,
                "player_id": player_id,
                "role": champ_role,
                "champion": champion,
                "game_id": gid,
                "patch": patch,
                "resolution": resolution,
                "pick_rate": bl["pick_rate"],
            })

    return evidence


def compute_left_open(
    evidence_rows: list[dict],
) -> list[dict]:
    """Compute left-open tells, one per (team, player, champion).

    For each group:
      - k = number of evidence games
      - t = number taken
      - expected = sum of per-game pick_rates
      - p_value = P(T ≤ t) under Poisson-binomial
      - fit = player's share of games on that champion

    Drop tells with k < MIN_N. Rank by p_value ascending.

    Args:
        evidence_rows: Output from build_evidence_rows().

    Returns:
        List of tell dicts sorted by p_value ascending.
    """
    # Group by (team, player_id, champion)
    groups: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for row in evidence_rows:
        key = (row["team"], row["player_id"], row["champion"])
        groups[key].append(row)

    tells: list[dict] = []
    for (team, player_id, champion), rows in groups.items():
        k = len(rows)
        if k < MIN_N:
            continue

        t = sum(1 for r in rows if r["resolution"] == "taken")
        probs = [r["pick_rate"] for r in rows]
        expected = sum(probs)
        p_value = poisson_binomial_p_value(t, probs)

        # fit = player's share of their games on that champion
        fit = t / k if k > 0 else 0.0

        role = rows[0]["role"]

        tells.append({
            "team": team,
            "player_id": player_id,
            "role": role,
            "champion": champion,
            "k": k,
            "t": t,
            "expected": round(expected, 6),
            "p_value": round(p_value, 8),
            "fit": round(fit, 6),
        })

    # Rank by p_value ascending
    tells.sort(key=lambda x: x["p_value"])
    return tells


# ── Early-ban pressure ───────────────────────────────────────────────────────

def compute_early_ban(
    draft_events: list[dict],
    game_flags: list[dict],
    baseline_rows: list[dict],
) -> list[dict]:
    """Compute early-ban pressure, one per (team, champion).

    From a team's own bans in observed games (complete, non-Bo1):
      - n_bans: count of bans
      - team_mean_slot: mean draft slot (1-20) of those bans
      - baseline_mean_slot: average of baseline mean_ban_slot for each game's patch
      - delta = baseline_mean_slot - team_mean_slot (positive = team bans earlier)

    Keep only n_bans >= 4 with a valid baseline value.

    Args:
        draft_events: Rows from draft_events table.
        game_flags: Rows from game_flags table.
        baseline_rows: Rows from baseline table.

    Returns:
        List of early-ban dicts.
    """
    # Build complete, non-Bo1 game set
    complete_games: set[tuple[str, int]] = set()
    for flag in game_flags:
        if flag["is_complete"] and not flag["is_bo1"]:
            complete_games.add((flag["series_id"], flag["game_num"]))

    # Build baseline lookup: (patch, champion) -> mean_ban_slot
    baseline_ban_slot: dict[tuple[str, str], float | None] = {}
    for row in baseline_rows:
        baseline_ban_slot[(row["patch"], row["champion"])] = row["mean_ban_slot"]

    # Group bans by (team, champion) over observed games
    # Each ban: (team, champion, slot, patch)
    bans_by_team_champ: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for ev in draft_events:
        if ev["action"] != "ban":
            continue
        sgn = (ev["series_id"], ev["game_num"])
        if sgn not in complete_games:
            continue
        bans_by_team_champ[(ev["team"], ev["champion"])].append({
            "slot": ev["slot"],
            "patch": ev["patch"],
        })

    results: list[dict] = []
    for (team, champion), bans in bans_by_team_champ.items():
        n_bans = len(bans)
        if n_bans < 4:
            continue

        team_mean_slot = sum(b["slot"] for b in bans) / n_bans

        # Compute baseline mean slot: average of baseline mean_ban_slot for each game's patch
        baseline_slots: list[float] = []
        for b in bans:
            bl_slot = baseline_ban_slot.get((b["patch"], champion))
            if bl_slot is not None:
                baseline_slots.append(bl_slot)

        if not baseline_slots:
            continue

        baseline_mean_slot = sum(baseline_slots) / len(baseline_slots)
        delta = baseline_mean_slot - team_mean_slot

        results.append({
            "team": team,
            "champion": champion,
            "n_bans": n_bans,
            "team_mean_slot": round(team_mean_slot, 4),
            "baseline_mean_slot": round(baseline_mean_slot, 4),
            "delta": round(delta, 4),
        })

    return results


# ── CLI ──────────────────────────────────────────────────────────────────────

def run_signals(grid_path: str) -> None:
    """Read derived/unpicked.duckdb and compute signals.

    Args:
        grid_path: Path to GRID starter repo (contains meta/rosters.csv).
    """
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Run build first.")
        sys.exit(1)

    # Read rosters
    rosters_file = Path(grid_path) / "meta" / "rosters.csv"
    if not rosters_file.is_file():
        print(f"Rosters not found at {rosters_file}")
        sys.exit(1)

    rosters_rows: list[dict] = []
    with open(rosters_file, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rosters_rows.append(row)

    # Read player roles
    player_roles_file = DERIVED / "player_roles.csv"
    player_roles: dict[str, dict] = {}
    if player_roles_file.is_file():
        with open(player_roles_file, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                player_roles[row["player_id"]] = {
                    "role": row["role"],
                    "share": float(row["share"]),
                    "flex": row["flex"] == "True",
                }
    else:
        print(f"Player roles not found at {player_roles_file}. Run infer_players first.")
        sys.exit(1)

    # Load champion roles
    from unpicked_intent.roles.champion_roles import load as load_champion_roles
    champion_roles = load_champion_roles()

    # Read from DuckDB
    con = duckdb.connect(str(DB_PATH))

    # champion_game joined with patch from draft_events
    cg_rows = con.execute("""
        SELECT cg.series_id, cg.game_num, cg.team, cg.champion, cg.resolution,
               de.patch
        FROM champion_game cg
        JOIN (SELECT DISTINCT series_id, game_num, patch FROM draft_events) de
          ON cg.series_id = de.series_id AND cg.game_num = de.game_num
    """).fetchall()
    cg_columns = ["series_id", "game_num", "team", "champion", "resolution", "patch"]
    cg_dicts = [dict(zip(cg_columns, row)) for row in cg_rows]

    de_rows = con.execute("SELECT * FROM draft_events").fetchall()
    de_columns = ["series_id", "game_num", "slot", "phase", "side", "team",
                   "action", "champion", "patch", "league", "played_at"]
    de_dicts = [dict(zip(de_columns, row)) for row in de_rows]

    gf_rows = con.execute("SELECT * FROM game_flags").fetchall()
    gf_columns = ["series_id", "game_num", "n_picks", "n_bans",
                   "is_complete", "is_bo1", "fearless_status"]
    gf_dicts = [dict(zip(gf_columns, row)) for row in gf_rows]

    bl_rows = con.execute("SELECT * FROM baseline").fetchall()
    bl_columns = ["patch", "champion", "role", "taken", "skipped", "pick_rate",
                   "presence", "meta_relevant", "mean_ban_slot", "window_start", "window_games"]
    bl_dicts = [dict(zip(bl_columns, row)) for row in bl_rows]

    con.close()

    # Compute left-open
    evidence = build_evidence_rows(cg_dicts, de_dicts, gf_dicts, bl_dicts,
                                   player_roles, rosters_rows, champion_roles)
    left_open_tells = compute_left_open(evidence)

    # Compute early-ban
    early_ban_tells = compute_early_ban(de_dicts, gf_dicts, bl_dicts)

    # Write tables
    con = duckdb.connect(str(DB_PATH))

    con.execute("""
        CREATE TABLE IF NOT EXISTS left_open (
            team        VARCHAR NOT NULL,
            player_id   VARCHAR NOT NULL,
            role        VARCHAR NOT NULL,
            champion    VARCHAR NOT NULL,
            k           INTEGER NOT NULL,
            t           INTEGER NOT NULL,
            expected    DOUBLE NOT NULL,
            p_value     DOUBLE NOT NULL,
            fit         DOUBLE NOT NULL
        )
    """)

    if left_open_tells:
        tuples = [
            (t["team"], t["player_id"], t["role"], t["champion"],
             t["k"], t["t"], t["expected"], t["p_value"], t["fit"])
            for t in left_open_tells
        ]
        con.executemany(
            "INSERT INTO left_open VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tuples,
        )

    con.execute("""
        CREATE TABLE IF NOT EXISTS early_ban (
            team             VARCHAR NOT NULL,
            champion         VARCHAR NOT NULL,
            n_bans           INTEGER NOT NULL,
            team_mean_slot   DOUBLE NOT NULL,
            baseline_mean_slot DOUBLE NOT NULL,
            delta            DOUBLE NOT NULL
        )
    """)

    if early_ban_tells:
        tuples = [
            (t["team"], t["champion"], t["n_bans"],
             t["team_mean_slot"], t["baseline_mean_slot"], t["delta"])
            for t in early_ban_tells
        ]
        con.executemany(
            "INSERT INTO early_ban VALUES (?, ?, ?, ?, ?, ?)",
            tuples,
        )

    con.close()

    print(f"\nWrote left_open table: {len(left_open_tells)} rows")
    print(f"Wrote early_ban table: {len(early_ban_tells)} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute observed-behavior signals.")
    parser.add_argument("--grid", required=True, help="Path to GRID starter repo.")
    args = parser.parse_args()
    run_signals(args.grid)


if __name__ == "__main__":
    main()
