"""Expected-behavior baseline — the fat layer.

Keyed on (played-patch window x role), where each window covers at least 3
played patches and extends backwards until it holds >= MIN_WINDOW_GAMES (100)
complete games, pooled across ALL regions (Bo1 included). Region stays out of
the key: LCS-only in a window is the same thin data we escaped.

'Played patch' means a patch with actual games — 16.5, 16.12, 16.13 have
none (competitive skipped them). Build windows over the ordered list of
played patches, not patch ordinals.

Outputs:
    meta_relevant(champion, role, window) -> bool
    expected_pick_rate(champion, role, window) -> float   # binomial p
    mean_ban_position(champion, window) -> float | None   # None if too few bans
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import duckdb

META_PRESENCE = 0.10  # Champion must appear in ≥10% of games to be meta-relevant
MIN_BANS = 10  # Minimum bans to compute mean_ban_slot
MIN_WINDOW_GAMES = 100  # Window extends back until it holds at least this many games

DERIVED = Path(__file__).resolve().parents[3] / "derived"
DB_PATH = DERIVED / "unpicked.duckdb"


def _patch_sort_key(patch: str):
    """Sort patches numerically: 16.9 comes before 16.10.

    Parses "major.minor" and returns (major, minor) as ints.
    Handles skipped patch numbers gracefully — 16.9 < 16.10 < 16.11.
    """
    m = re.match(r"(\d+)\.(\d+)", patch)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    # Fallback: treat as string if parsing fails
    return (0, 0)


def played_patch_windows(
    patches: list[str],
    games_per_patch: dict[str, int],
    width: int = 3,
) -> dict[str, list[str]]:
    """Map each played patch to a rolling window of itself + previous played patches.

    Sorts patches numerically (16.10 comes after 16.9) and keeps only patches
    that have games. For each patch P, start with P and the 2 previous played
    patches, then keep adding earlier played patches until the window's total
    games >= MIN_WINDOW_GAMES or no earlier patch is left. The first patches
    get shorter windows (stop at what's available).

    Args:
        patches: List of patch strings that have games (e.g. from champion_game table).
        games_per_patch: Dict mapping patch -> number of complete games in that patch.
        width: Minimum window size (default 3: current + 2 previous).

    Returns:
        Dict mapping each patch (as string) to its window list.
        The window's newest patch is the key.
    """
    # Get unique patches sorted numerically
    unique_patches = sorted(set(patches), key=_patch_sort_key)

    windows: dict[str, list[str]] = {}
    for i, patch in enumerate(unique_patches):
        # Start with current patch and up to width-1 previous patches
        start = max(0, i - width + 1)
        window = unique_patches[start: i + 1]

        # Extend window backwards until it holds at least MIN_WINDOW_GAMES
        window_games = sum(games_per_patch.get(wp, 0) for wp in window)
        while window_games < MIN_WINDOW_GAMES and start > 0:
            start -= 1
            window = unique_patches[start: i + 1]
            window_games = sum(games_per_patch.get(wp, 0) for wp in window)

        windows[patch] = window

    return windows


def compute_baseline(
    champion_game_rows: list[dict],
    draft_events: list[dict],
    champion_roles: dict[str, str],
    width: int = 3,
) -> list[dict]:
    """Compute baseline statistics for every (window, champion).

    For every (window, champion), computed over all complete games in that
    window, all regions, Bo1 included:
      - taken: count of 'taken' resolutions
      - skipped: count of 'skipped' resolutions
      - pick_rate = taken / (taken + skipped), or 0 when both are 0
      - presence = games where anyone picked or banned it / games in window
      - meta_relevant = presence >= META_PRESENCE (0.10)
      - mean_ban_slot = mean draft slot (1-20) of its bans; None when bans < MIN_BANS (10)
      - role from champion_roles

    Args:
        champion_game_rows: Rows from champion_game table (series_id, game_num, team, champion, resolution).
        draft_events: Rows from draft_events table (includes slot info for bans).
        champion_roles: {champion: role} mapping.
        width: Patch window width.

    Returns:
        List of baseline dicts keyed by the window's newest patch.
    """
    # Build patch windows
    patches = [row["patch"] for row in champion_game_rows]

    # Compute games per patch
    games_by_patch_set: dict[str, set[tuple[str, int]]] = defaultdict(set)
    for row in champion_game_rows:
        games_by_patch_set[row["patch"]].add((row["series_id"], row["game_num"]))
    games_per_patch = {patch: len(games) for patch, games in games_by_patch_set.items()}

    windows = played_patch_windows(patches, games_per_patch, width)

    if not windows:
        return []

    # Group champion_game rows by (patch, champion)

    # For presence: games where anyone picked or banned the champion
    # "picked" = taken by either team; "banned" = banned this game
    pick_ban_games_by_patch_champ: dict[tuple[str, str], set[tuple[str, int]]] = defaultdict(set)
    for row in champion_game_rows:
        if row["resolution"] in ("taken", "banned"):
            key = (row["patch"], row["champion"])
            pick_ban_games_by_patch_champ[key].add((row["series_id"], row["game_num"]))

    # Count taken and skipped per (patch, champion)
    taken_by_patch_champ: dict[tuple[str, str], int] = Counter()
    skipped_by_patch_champ: dict[tuple[str, str], int] = Counter()
    for row in champion_game_rows:
        key = (row["patch"], row["champion"])
        if row["resolution"] == "taken":
            taken_by_patch_champ[key] += 1
        elif row["resolution"] == "skipped":
            skipped_by_patch_champ[key] += 1

    # Compute mean ban slot per (patch, champion) from draft_events
    # Only count ban actions
    ban_slots_by_patch_champ: dict[tuple[str, str], list[int]] = defaultdict(list)
    for ev in draft_events:
        if ev["action"] == "ban":
            key = (ev["patch"], ev["champion"])
            ban_slots_by_patch_champ[key].append(ev["slot"])

    # Get all champions in the universe
    all_champions = set(champion_roles.keys())
    # Also include any champions that appear in the data
    for row in champion_game_rows:
        all_champions.add(row["champion"])

    # Build baseline rows
    baseline_rows: list[dict] = []
    for newest_patch, window_patches in windows.items():
        # Aggregate over the window
        # Total games in window (sum of games_per_patch across window patches)
        window_game_count = sum(games_per_patch.get(wp, 0) for wp in window_patches)
        num_games = window_game_count if window_game_count > 0 else 1  # avoid div by zero

        window_start = window_patches[0] if window_patches else newest_patch

        for champion in sorted(all_champions):
            role = champion_roles.get(champion, "unknown")

            # Aggregate taken and skipped over window
            total_taken = 0
            total_skipped = 0
            for wp in window_patches:
                key = (wp, champion)
                total_taken += taken_by_patch_champ.get(key, 0)
                total_skipped += skipped_by_patch_champ.get(key, 0)

            # pick_rate: taken / (taken + skipped), excluding denied/banned/burned
            if total_taken + total_skipped > 0:
                pick_rate = total_taken / (total_taken + total_skipped)
            else:
                pick_rate = 0.0

            # presence: games where anyone picked or banned it / games in window
            pb_games: set[tuple[str, int]] = set()
            for wp in window_patches:
                key = (wp, champion)
                pb_games |= pick_ban_games_by_patch_champ.get(key, set())
            presence = len(pb_games) / num_games if num_games > 0 else 0.0

            meta_relevant = presence >= META_PRESENCE

            # mean_ban_slot: mean draft slot of bans in window
            all_ban_slots: list[int] = []
            for wp in window_patches:
                key = (wp, champion)
                all_ban_slots.extend(ban_slots_by_patch_champ.get(key, []))

            if len(all_ban_slots) >= MIN_BANS:
                mean_ban_slot = sum(all_ban_slots) / len(all_ban_slots)
            else:
                mean_ban_slot = None

            baseline_rows.append({
                "patch": newest_patch,
                "champion": champion,
                "role": role,
                "taken": total_taken,
                "skipped": total_skipped,
                "pick_rate": round(pick_rate, 6),
                "presence": round(presence, 6),
                "meta_relevant": meta_relevant,
                "mean_ban_slot": round(mean_ban_slot, 4) if mean_ban_slot is not None else None,
                "window_start": window_start,
                "window_games": window_game_count,
            })

    return baseline_rows


def run_baseline(grid_path: str | None = None) -> None:
    """Compute and write baseline table to derived/unpicked.duckdb.

    Args:
        grid_path: If provided, runs build.py first. Otherwise reads existing DB.
    """
    from unpicked_intent.roles.champion_roles import load

    if grid_path:
        from unpicked_intent.pipeline.build import run_pipeline
        run_pipeline(grid_path)

    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Run build first.")
        sys.exit(1)

    con = duckdb.connect(str(DB_PATH))

    # Read champion_game joined with draft_events to get patch.
    # champion_game has no patch column; patch comes from draft_events via (series_id, game_num).
    cg_rows = con.execute("""
        SELECT cg.series_id, cg.game_num, cg.team, cg.champion, cg.resolution,
               de.patch
        FROM champion_game cg
        JOIN (SELECT DISTINCT series_id, game_num, patch FROM draft_events) de
          ON cg.series_id = de.series_id AND cg.game_num = de.game_num
    """).fetchall()
    cg_columns = ["series_id", "game_num", "team", "champion", "resolution", "patch"]
    cg_dicts = [dict(zip(cg_columns, row)) for row in cg_rows]

    de_rows = con.execute(
        "SELECT series_id, game_id, game_num, slot, phase, side, team, "
        "action, champion, patch, league, played_at FROM draft_events"
    ).fetchall()
    de_columns = ["series_id", "game_id", "game_num", "slot", "phase", "side", "team",
                   "action", "champion", "patch", "league", "played_at"]
    de_dicts = [dict(zip(de_columns, row)) for row in de_rows]

    con.close()

    champion_roles = load()
    baseline_rows = compute_baseline(cg_dicts, de_dicts, champion_roles)

    # Write baseline table (replace on every run)
    con = duckdb.connect(str(DB_PATH))
    con.execute("""
        CREATE OR REPLACE TABLE baseline (
            patch         VARCHAR NOT NULL,
            champion      VARCHAR NOT NULL,
            role          VARCHAR NOT NULL,
            taken         INTEGER NOT NULL,
            skipped       INTEGER NOT NULL,
            pick_rate     DOUBLE NOT NULL,
            presence      DOUBLE NOT NULL,
            meta_relevant BOOLEAN NOT NULL,
            mean_ban_slot DOUBLE,
            window_start  VARCHAR NOT NULL,
            window_games  INTEGER NOT NULL,
            PRIMARY KEY (patch, champion)
        )
    """)

    if baseline_rows:
        from unpicked_intent.pipeline.build import _bulk_insert

        tuples = (
            (
                r["patch"], r["champion"], r["role"],
                r["taken"], r["skipped"], r["pick_rate"],
                r["presence"], r["meta_relevant"], r["mean_ban_slot"],
                r["window_start"], r["window_games"],
            )
            for r in baseline_rows
        )
        _bulk_insert(
            con,
            "baseline",
            tuples,
            (
                ("patch", "VARCHAR"),
                ("champion", "VARCHAR"),
                ("role", "VARCHAR"),
                ("taken", "INTEGER"),
                ("skipped", "INTEGER"),
                ("pick_rate", "DOUBLE"),
                ("presence", "DOUBLE"),
                ("meta_relevant", "BOOLEAN"),
                ("mean_ban_slot", "DOUBLE"),
                ("window_start", "VARCHAR"),
                ("window_games", "INTEGER"),
            ),
        )

    con.close()

    # Summary
    meta_count = sum(1 for r in baseline_rows if r["meta_relevant"])
    print(f"\nWrote baseline table: {len(baseline_rows)} rows")
    print(f"Meta-relevant champions: {meta_count}")
    patches_with_baseline = set(r["patch"] for r in baseline_rows)
    print(f"Patches with baseline: {', '.join(sorted(patches_with_baseline, key=_patch_sort_key))}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute and write baseline statistics.")
    parser.add_argument("--grid", default=None, help="Path to GRID repo (runs build first if provided).")
    args = parser.parse_args()

    run_baseline(args.grid)


if __name__ == "__main__":
    main()
