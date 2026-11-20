"""Player -> role inference. GENERATED artifact — output goes to derived/.

Method: player role = argmax over the player's champion history weighted
by champion->role priors (a mid plays mid champions). Flex players are
corrected by a hand-maintained override file under derived/ (gitignored).

Required because rosters.csv has player->champion per game but no role
column, and draft events are team-level: mapping a pick to its player
goes through rosters + this role map. Load-bearing — ~1 day of work.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path

DERIVED = Path(__file__).resolve().parents[3] / "derived"
OVERRIDES_PATH = DERIVED / "player_role_overrides.csv"
OUTPUT_PATH = DERIVED / "player_roles.csv"

FLEX_THRESHOLD = 0.5


def infer(
    champion_history: dict[str, list[str]],
    priors: dict[str, str],
) -> dict[str, dict]:
    """Infer each player's primary role from their champion history.

    For each player, count how many times each role appears across their
    champions (via the champion→role priors). The role with the highest
    count wins. Also compute the role share (top role count / total) so
    callers can flag flex players whose top role is under FLEX_THRESHOLD (50%).

    Args:
        champion_history: {player_id: [champion, ...]} built from
            meta/rosters.csv (only active participations).
        priors: {champion_name: role} from roles.champion_roles.load().

    Returns:
        {player_id: {"role": str, "share": float, "flex": bool}}
    """
    result: dict[str, dict] = {}
    for player_id, champs in champion_history.items():
        role_counts: Counter = Counter()
        for ch in champs:
            role = priors.get(ch)
            if role:
                role_counts[role] += 1

        if not role_counts:
            result[player_id] = {"role": "unknown", "share": 0.0, "flex": True}
            continue

        top_role, top_count = role_counts.most_common(1)[0]
        total = sum(role_counts.values())
        share = top_count / total
        result[player_id] = {
            "role": top_role,
            "share": round(share, 4),
            "flex": share < FLEX_THRESHOLD,
        }

    return result


def apply_overrides(
    inferred: dict[str, dict],
    overrides_path: Path | None = None,
) -> dict[str, dict]:
    """Apply hand-maintained role overrides for flex players.

    Reads derived/player_role_overrides.csv (player_id, role) when it
    exists and overrides the inferred role for those players.

    Args:
        inferred: Output from infer().
        overrides_path: Path to the overrides CSV. Defaults to
            derived/player_role_overrides.csv.

    Returns:
        Updated {player_id: {"role": str, "share": float, "flex": bool}}.
    """
    if overrides_path is None:
        overrides_path = OVERRIDES_PATH

    if not overrides_path.is_file():
        return inferred

    overrides: dict[str, str] = {}
    with overrides_path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            overrides[row["player_id"]] = row["role"]

    for player_id, override_role in overrides.items():
        if player_id in inferred:
            inferred[player_id]["role"] = override_role
            inferred[player_id]["flex"] = False

    return inferred


def build_champion_history(
    rosters_path: str,
) -> dict[str, list[str]]:
    """Build {player_id: [champion, ...]} from meta/rosters.csv.

    Uses only rows where participation_status == "active".

    Args:
        rosters_path: Path to meta/rosters.csv.

    Returns:
        {player_id: [champion, ...]}
    """
    history: dict[str, list[str]] = defaultdict(list)
    with open(rosters_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("title") == "lol" and row.get("participation_status") == "active":
                history[row["player_id"]].append(row["character_name"])
    return dict(history)


def map_team_picks(
    draft_events: list[dict],
    rosters_path: str,
) -> list[dict]:
    """Map each team pick in a game to its player from rosters.csv.

    Joins rosters.csv (title == 'lol', participation_status == 'active') on
    game_id + team_id + character_name to resolve which player picked which
    champion. Use map_team_picks_with_roles() to attach inferred roles.

    Args:
        draft_events: Normalized draft events (after undo folding and
            slot assignment), containing only complete games.
        rosters_path: Path to meta/rosters.csv.

    Returns:
        List of dicts with keys: series_id, game_id, game_num, team,
        side, slot, phase, action, champion, player_id, player_name.
    """
    # Build roster lookup: (game_id, team_id, character_name) -> player info
    roster_lookup: dict[tuple[str, str, str], dict] = {}
    with open(rosters_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("title") == "lol" and row.get("participation_status") == "active":
                key = (row["game_id"], row["team_id"], row["character_name"])
                roster_lookup[key] = {
                    "player_id": row["player_id"],
                    "player_name": row["player_name"],
                }

    # Build player→role map from inferred roles (will be populated by caller)
    # For now, we return the mapped events; caller attaches roles.
    results: list[dict] = []
    for ev in draft_events:
        if ev["action"] != "pick":
            continue
        key = (ev["game_id"], ev["team"], ev["champion"])
        player_info = roster_lookup.get(key, {})
        results.append({
            "series_id": ev["series_id"],
            "game_id": ev["game_id"],
            "game_num": ev["game_num"],
            "team": ev["team"],
            "side": ev.get("side", ""),
            "slot": ev.get("slot"),
            "phase": ev.get("phase"),
            "action": ev["action"],
            "champion": ev["champion"],
            "player_id": player_info.get("player_id", ""),
            "player_name": player_info.get("player_name", ""),
        })

    return results


def map_team_picks_with_roles(
    draft_events: list[dict],
    rosters_path: str,
    player_roles: dict[str, dict],
) -> list[dict]:
    """Map each team pick to (player_id, role), attaching the inferred role.

    Args:
        draft_events: Normalized draft events (after undo folding and
            slot assignment), containing only complete games.
        rosters_path: Path to meta/rosters.csv.
        player_roles: Output from infer() or apply_overrides().

    Returns:
        List of dicts with keys: series_id, game_id, game_num, team,
        side, slot, phase, action, champion, player_id, player_name, role.
    """
    mapped = map_team_picks(draft_events, rosters_path)
    for row in mapped:
        pid = row["player_id"]
        if pid in player_roles:
            row["role"] = player_roles[pid]["role"]
        else:
            row["role"] = "unknown"
    return mapped


def write_player_roles(
    inferred: dict[str, dict],
    output_path: Path | None = None,
) -> Path:
    """Write inferred player roles to a CSV under derived/.

    Args:
        inferred: Output from infer() or apply_overrides().
        output_path: Output CSV path. Defaults to derived/player_roles.csv.

    Returns:
        The path written.
    """
    if output_path is None:
        output_path = OUTPUT_PATH

    DERIVED.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["player_id", "role", "share", "flex"])
        writer.writeheader()
        for player_id in sorted(inferred.keys()):
            info = inferred[player_id]
            writer.writerow({
                "player_id": player_id,
                "role": info["role"],
                "share": info["share"],
                "flex": info["flex"],
            })

    return output_path


def run_infer_players(grid_path: str) -> Path:
    """Infer LoL player roles from *grid_path*/meta/rosters.csv and write derived/player_roles.csv."""
    rosters_path = Path(grid_path) / "meta" / "rosters.csv"

    if not rosters_path.is_file():
        print(f"Error: {rosters_path} not found", file=sys.stderr)
        sys.exit(1)

    # Load champion→role priors
    from unpicked_intent.roles.champion_roles import load as load_priors
    priors = load_priors()

    # Build champion history and infer roles
    history = build_champion_history(str(rosters_path))
    inferred = infer(history, priors)

    # Apply overrides if they exist
    inferred = apply_overrides(inferred)

    # Write output — never outside derived/
    output = write_player_roles(inferred)
    print(f"Wrote {output.name} ({len(inferred)} players)")
    return output


def main():
    """CLI entry: python -m unpicked_intent.roles.infer_players --grid <path>"""
    parser = argparse.ArgumentParser(description="Infer player roles from GRID rosters.")
    parser.add_argument("--grid", required=True, help="Path to GRID starter repo (contains meta/rosters.csv)")
    args = parser.parse_args()
    run_infer_players(args.grid)


if __name__ == "__main__":
    main()
