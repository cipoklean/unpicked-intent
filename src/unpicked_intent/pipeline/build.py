"""Build step: run ingest → normalize → legal_pool and write to DuckDB.

CLI: python -m unpicked_intent.pipeline.build --grid <path to GRID starter repo>

Reads all events_*_grid.jsonl.zst feeds from the GRID repo, runs the full
pipeline, and writes draft_events, game_flags, and champion_game tables to
derived/unpicked.duckdb.

Only complete games (10 bans + 10 picks after undo folding) are written.
"""

from __future__ import annotations

import argparse
import glob
import sys
from collections import Counter
from pathlib import Path

import duckdb

# DERIVED is the repo root's derived/ folder (next to pyproject.toml).
# src/unpicked_intent/pipeline/build.py → parents[3] = repo root.
DERIVED = Path(__file__).resolve().parents[3] / "derived"
DB_PATH = DERIVED / "unpicked.duckdb"

SCHEMA_SQL = Path(__file__).resolve().parent / "schema.sql"


def run_pipeline(grid_path: str) -> None:
    """Run the full pipeline over all feeds in *grid_path* and write DuckDB.

    Args:
        grid_path: Path to the GRID starter repo root (contains data/ and meta/).
    """
    from unpicked_intent.ingest.feeds import iter_draft_events
    from unpicked_intent.pipeline.normalize import (
        apply_undos,
        assign_slots,
        check_completeness,
        flag_games,
    )
    from unpicked_intent.pipeline.legal_pool import resolve_champion_games
    from unpicked_intent.roles.champion_roles import load

    grid = Path(grid_path)
    games_csv = str(grid / "meta" / "games.csv")

    # Find all feed files — GRID starter repo keeps them in lol/, fall back to data/
    feed_files: list[str] = []
    for subdir in ("lol", "data"):
        feed_pattern = str(grid / subdir / "events_*_grid.jsonl.zst")
        feed_files = sorted(glob.glob(feed_pattern))
        if feed_files:
            print(f"Found feeds in {subdir}/")
            break
    if not feed_files:
        print(f"No feed files found in {grid}/lol/ or {grid}/data/")
        sys.exit(1)

    print(f"Found {len(feed_files)} feed files")

    # --- Ingest ---
    all_events: list[dict] = []
    total_skipped = 0
    total_read = 0
    for fp in feed_files:
        events_iter, stats = iter_draft_events(fp, games_csv)
        for ev in events_iter:
            all_events.append(ev)
        total_skipped += stats.skipped_games
        total_read += stats.events_read

    print(f"Ingested {total_read} events, skipped {total_skipped} abandoned drafts")

    # --- Normalize: apply undos ---
    folded = apply_undos(all_events)

    # --- Normalize: assign slots ---
    slotted, _norm_stats = assign_slots(folded)

    # Compute series_game_count BEFORE dropping incomplete games
    series_game_ids: dict[str, set[str]] = {}
    for ev in slotted:
        sid = ev["series_id"]
        if sid not in series_game_ids:
            series_game_ids[sid] = set()
        series_game_ids[sid].add(ev["game_id"])
    series_game_count: dict[str, int] = {sid: len(gids) for sid, gids in series_game_ids.items()}

    # --- Normalize: flag games (before drop, for correct Bo1/Fearless) ---
    all_flags = flag_games(slotted, series_game_count=series_game_count)

    # --- Normalize: drop incomplete ---
    complete, dropped = check_completeness(slotted)
    complete_game_ids = {ev["game_id"] for ev in complete}
    print(f"Dropped {dropped} incomplete games, {len(complete_game_ids)} complete remain")

    # Filter flags to complete games only
    complete_flags = [f for f in all_flags if f["game_id"] in complete_game_ids]

    # --- Legal pool ---
    champion_roles = load()
    cg_rows = resolve_champion_games(complete, champion_roles, complete_flags)

    # --- Write DuckDB ---
    DERIVED.mkdir(parents=True, exist_ok=True)

    # Remove existing DB to start fresh
    if DB_PATH.exists():
        DB_PATH.unlink()

    con = duckdb.connect(str(DB_PATH))

    # Run schema
    schema_text = SCHEMA_SQL.read_text()
    con.execute(schema_text)

    # Write draft_events (complete games only) using DuckDB's append
    draft_tuples = [
        (
            ev["series_id"],
            ev["game_num"],
            ev["slot"],
            ev["phase"],
            ev["side"],
            ev["team"],
            ev["action"],
            ev["champion"],
            ev["patch"],
            ev.get("league", ""),
            ev.get("played_at", ""),
        )
        for ev in complete
    ]
    if draft_tuples:
        con.executemany(
            "INSERT INTO draft_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            draft_tuples,
        )

    # Write game_flags
    flag_tuples = [
        (
            f["series_id"],
            f["game_num"],
            10 if f["is_complete"] else 0,
            10 if f["is_complete"] else 0,
            f["is_complete"],
            f["is_bo1"],
            f["fearless_status"],
        )
        for f in complete_flags
    ]
    if flag_tuples:
        con.executemany(
            "INSERT INTO game_flags VALUES (?, ?, ?, ?, ?, ?, ?)",
            flag_tuples,
        )

    # Write champion_game
    cg_tuples = [
        (r["series_id"], r["game_num"], r["team"], r["champion"], r["status"])
        for r in cg_rows
    ]
    if cg_tuples:
        con.executemany(
            "INSERT INTO champion_game VALUES (?, ?, ?, ?, ?)",
            cg_tuples,
        )

    con.close()

    # --- Print summary ---
    game_count = len(complete_game_ids)
    cg_count = len(cg_rows)
    resolution_counts = Counter(r["status"] for r in cg_rows)

    print(f"\nWrote {DB_PATH}")
    print(f"Games written: {game_count}")
    print(f"Champion-game rows: {cg_count}")
    print("Counts per resolution:")
    for status in ("burned", "banned", "taken", "denied", "skipped"):
        print(f"  {status}: {resolution_counts.get(status, 0)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full pipeline and write DuckDB.")
    parser.add_argument("--grid", required=True, help="Path to the GRID starter repo root.")
    args = parser.parse_args()
    run_pipeline(args.grid)


if __name__ == "__main__":
    main()
