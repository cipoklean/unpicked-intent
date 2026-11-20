"""Expected-behavior baseline — the fat layer.

Keyed on (3-played-patch window x role), pooled across ALL regions
(1,563 games). Region stays out of the key: LCS-only in a window is the
same thin data we escaped; LCS rates are computed for display context
only.

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


def played_patch_windows(patches: list[str], width: int = 3) -> dict[str, list[str]]:
    """Map each played patch to a rolling window of itself + previous played patches.

    Sorts patches numerically (16.10 comes after 16.9) and keeps only patches
    that have games. Each patch maps to itself plus the `width-1` previous
    played patches. The first patches get shorter windows.

    Args:
        patches: List of patch strings that have games (e.g. from champion_game table).
        width: Window size (default 3: current + 2 previous).

    Returns:
        Dict mapping each patch (as string) to its window list.
        The window's newest patch is the key.
    """
    # Get unique patches sorted numerically
    unique_patches = sorted(set(patches), key=_patch_sort_key)

    windows: dict[str, list[str]] = {}
    for i, patch in enumerate(unique_patches):
        # Window includes current patch and up to width-1 previous patches
        start = max(0, i - width + 1)
        windows[patch] = unique_patches[start: i + 1]

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
    windows = played_patch_windows(patches, width)

    if not windows:
        return []

    # Group champion_game rows by (patch, champion)
    # First, build a set of (series_id, game_num) per patch for game counting
    games_by_patch: dict[str, set[tuple[str, int]]] = defaultdict(set)
    for row in champion_game_rows:
        games_by_patch[row["patch"]].add((row["series_id"], row["game_num"]))

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
        # Total games in window (union of games across window patches)
        window_games: set[tuple[str, int]] = set()
        for wp in window_patches:
            window_games |= games_by_patch.get(wp, set())
        num_games = len(window_games) if window_games else 1  # avoid div by zero

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
            })

    return baseline_rows


def run_baseline(grid_path: str | None = None) -> None:
    """Compute and write baseline table to derived/unpicked.duckdb.

    Args:
        grid_path: If provided, runs build.py first. Otherwise reads existing DB.
    """
    from unpicked_intent.roles.champion_roles import load

    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Run build first.")
        sys.exit(1)

    con = duckdb.connect(str(DB_PATH))

    # Read champion_game and draft_events
    cg_rows = con.execute("SELECT * FROM champion_game").fetchall()
    cg_columns = ["series_id", "game_num", "team", "champion", "resolution"]
    cg_dicts = [dict(zip(cg_columns, row)) for row in cg_rows]

    de_rows = con.execute("SELECT * FROM draft_events").fetchall()
    de_columns = ["series_id", "game_num", "slot", "phase", "side", "team",
                   "action", "champion", "patch", "league", "played_at"]
    de_dicts = [dict(zip(de_columns, row)) for row in de_rows]

    con.close()

    champion_roles = load()
    baseline_rows = compute_baseline(cg_dicts, de_dicts, champion_roles)

    # Write baseline table
    con = duckdb.connect(str(DB_PATH))
    con.execute("""
        CREATE TABLE IF NOT EXISTS baseline (
            patch         VARCHAR NOT NULL,
            champion      VARCHAR NOT NULL,
            role          VARCHAR NOT NULL,
            taken         INTEGER NOT NULL,
            skipped       INTEGER NOT NULL,
            pick_rate     DOUBLE NOT NULL,
            presence      DOUBLE NOT NULL,
            meta_relevant BOOLEAN NOT NULL,
            mean_ban_slot DOUBLE,
            PRIMARY KEY (patch, champion)
        )
    """)

    if baseline_rows:
        tuples = [
            (
                r["patch"], r["champion"], r["role"],
                r["taken"], r["skipped"], r["pick_rate"],
                r["presence"], r["meta_relevant"], r["mean_ban_slot"],
            )
            for r in baseline_rows
        ]
        con.execute("INSERT INTO baseline VALUES ?", [tuples])

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

    if args.grid:
        from unpicked_intent.pipeline.build import run_pipeline
        run_pipeline(args.grid)

    run_baseline()


if __name__ == "__main__":
    main()
