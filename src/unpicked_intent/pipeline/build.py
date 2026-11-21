"""Build step: run ingest → normalize → legal_pool and write to DuckDB.

CLI: python -m unpicked_intent.pipeline.build --grid <path to GRID starter repo>

Reads all events_*_grid.jsonl.zst feeds from the GRID repo, runs the full
pipeline, and writes draft_events, game_flags, and champion_game tables to
derived/unpicked.duckdb.

Only complete games (10 bans + 10 picks after undo folding) are written.
"""

from __future__ import annotations

import argparse
import csv
import glob
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path

import duckdb

# DERIVED is the repo root's derived/ folder (next to pyproject.toml).
# src/unpicked_intent/pipeline/build.py → parents[3] = repo root.
DERIVED = Path(__file__).resolve().parents[3] / "derived"
DB_PATH = DERIVED / "unpicked.duckdb"

SCHEMA_SQL = Path(__file__).resolve().parent / "schema.sql"

NULL_SENTINEL = r"\N"

DRAFT_EVENTS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("series_id", "VARCHAR"),
    ("game_id", "VARCHAR"),
    ("game_num", "INTEGER"),
    ("slot", "INTEGER"),
    ("phase", "VARCHAR"),
    ("side", "VARCHAR"),
    ("team", "VARCHAR"),
    ("action", "VARCHAR"),
    ("champion", "VARCHAR"),
    ("patch", "VARCHAR"),
    ("league", "VARCHAR"),
    ("played_at", "TIMESTAMP"),
)

GAME_FLAGS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("series_id", "VARCHAR"),
    ("game_num", "INTEGER"),
    ("n_picks", "INTEGER"),
    ("n_bans", "INTEGER"),
    ("is_complete", "BOOLEAN"),
    ("is_bo1", "BOOLEAN"),
    ("fearless_status", "VARCHAR"),
)

CHAMPION_GAME_COLUMNS: tuple[tuple[str, str], ...] = (
    ("series_id", "VARCHAR"),
    ("game_num", "INTEGER"),
    ("team", "VARCHAR"),
    ("champion", "VARCHAR"),
    ("resolution", "VARCHAR"),
)

NAMES_COLUMNS: tuple[tuple[str, str], ...] = (
    ("kind", "VARCHAR"),
    ("id", "VARCHAR"),
    ("name", "VARCHAR"),
)


def _normalize_columns(
    columns: Mapping[str, str] | Sequence[str] | Sequence[tuple[str, str]],
) -> list[tuple[str, str | None]]:
    """Normalize *columns* into ``[(name, optional_sql_type), ...]``."""
    if isinstance(columns, Mapping):
        return [(str(col), str(dtype)) for col, dtype in columns.items()]
    result: list[tuple[str, str | None]] = []
    for item in columns:
        if isinstance(item, tuple):
            col, dtype = item
            result.append((str(col), str(dtype)))
        else:
            result.append((str(item), None))
    return result


def _bulk_insert(
    con: duckdb.DuckDBPyConnection,
    table: str,
    rows: Iterable[Sequence[object]],
    columns: Mapping[str, str] | Sequence[str] | Sequence[tuple[str, str]],
) -> int:
    """Bulk-load *rows* into DuckDB *table* via a temporary CSV and ``COPY``.

    DuckDB's Python ``con.executemany`` executes one row at a time and can stall
    for tens of minutes on ~594k rows (especially on ARM). Writing to a temp CSV
    inside ``tempfile.TemporaryDirectory`` and loading via ``COPY ... FROM`` with
    ``AUTO_DETECT false`` uses the target table's explicit schema types and
    preserves ``None`` (via ``NULLSTR '\\N'``) and ``bool`` semantics identically
    to ``executemany``.

    Args:
        con: Open DuckDB connection.
        table: Target table name (must already exist in *con*).
        rows: Iterable of row tuples matching *columns* order.
        columns: Column names (``Sequence[str]``) or explicit ``(name, type)``
            pairs / mapping matching the table's schema.

    Returns:
        Number of rows written.
    """
    col_specs = _normalize_columns(columns)
    if not col_specs:
        raise ValueError(f"columns must not be empty for table {table!r}")

    # Validate column names (and explicit types, when provided) against the table schema.
    schema_rows = con.execute(f"DESCRIBE {table}").fetchall()
    schema_types = {row[0]: str(row[1]).upper() for row in schema_rows}
    col_names: list[str] = []
    for col_name, expected_type in col_specs:
        if col_name not in schema_types:
            raise ValueError(f"Column {col_name!r} does not exist on table {table!r}")
        if expected_type is not None and schema_types[col_name] != expected_type.upper():
            raise ValueError(
                f"Type mismatch for {table}.{col_name}: "
                f"expected {expected_type.upper()}, schema has {schema_types[col_name]}"
            )
        col_names.append(col_name)

    t0 = time.perf_counter()
    row_count = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / f"{table}.csv"
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(
                f,
                delimiter=",",
                quotechar='"',
                doublequote=True,
                quoting=csv.QUOTE_MINIMAL,
                lineterminator="\n",
            )
            writer.writerow(col_names)
            for row in rows:
                writer.writerow(
                    NULL_SENTINEL
                    if v is None
                    else ("true" if v is True else ("false" if v is False else v))
                    for v in row
                )
                row_count += 1

        if row_count > 0:
            sql_path = csv_path.as_posix().replace("'", "''")
            col_list = ", ".join(col_names)
            con.execute(
                f"COPY {table} ({col_list}) FROM '{sql_path}' ("
                "HEADER true, "
                "DELIMITER ',', "
                "QUOTE '\"', "
                "ESCAPE '\"', "
                "NEW_LINE '\\n', "
                f"NULLSTR '{NULL_SENTINEL}', "
                "ALLOW_QUOTED_NULLS false, "
                "AUTO_DETECT false)"
            )

    elapsed = time.perf_counter() - t0
    print(f"{table}: {row_count:,} rows in {elapsed:.1f}s")
    return row_count


def load_names(meta_dir: Path) -> list[tuple[str, str, str]]:
    """Load LoL team and player display names from meta/teams.csv and meta/players.csv.

    Keeps only rows where ``title == 'lol'`` and deduplicates by ``(kind, id)``.

    Args:
        meta_dir: Path to the GRID ``meta/`` directory.

    Returns:
        Sorted list of ``(kind, id, name)`` tuples where ``kind`` is
        ``'team'`` or ``'player'``.
    """
    names_map: dict[tuple[str, str], str] = {}

    sources = (
        ("team", meta_dir / "teams.csv", "team_id"),
        ("player", meta_dir / "players.csv", "player_id"),
    )
    for kind, csv_path, id_col in sources:
        if not csv_path.is_file():
            continue
        with open(csv_path, encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                if row.get("title") != "lol":
                    continue
                item_id = (row.get(id_col) or "").strip()
                item_name = (row.get("name") or "").strip()
                if item_id and item_name:
                    names_map[(kind, item_id)] = item_name

    return [(kind, item_id, name) for (kind, item_id), name in sorted(names_map.items())]


def run_pipeline(grid_path: str) -> None:
    """Run the full pipeline over all feeds in *grid_path* and write DuckDB.

    Args:
        grid_path: Path to the GRID starter repo root (contains data/ and meta/).
    """
    from unpicked_intent.ingest.feeds import iter_draft_events
    from unpicked_intent.pipeline.legal_pool import resolve_champion_games
    from unpicked_intent.pipeline.normalize import (
        apply_undos,
        assign_slots,
        check_completeness,
        flag_games,
    )
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
    schema_text = SCHEMA_SQL.read_text(encoding="utf-8")
    con.execute(schema_text)

    # Write draft_events (complete games only)
    draft_tuples = (
        (
            ev["series_id"],
            ev["game_id"],
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
    )
    _bulk_insert(con, "draft_events", draft_tuples, DRAFT_EVENTS_COLUMNS)

    # Write game_flags
    flag_tuples = (
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
    )
    _bulk_insert(con, "game_flags", flag_tuples, GAME_FLAGS_COLUMNS)

    # Write champion_game
    cg_tuples = (
        (r["series_id"], r["game_num"], r["team"], r["champion"], r["status"])
        for r in cg_rows
    )
    _bulk_insert(con, "champion_game", cg_tuples, CHAMPION_GAME_COLUMNS)

    # Write names (LoL teams and players from meta/teams.csv and meta/players.csv)
    names_tuples = load_names(grid / "meta")
    _bulk_insert(con, "names", names_tuples, NAMES_COLUMNS)

    con.close()

    # --- Print summary ---
    game_count = len(complete_game_ids)
    cg_count = len(cg_rows)
    resolution_counts = Counter(r["status"] for r in cg_rows)

    print(f"\nWrote {DB_PATH}")
    print(f"Games written: {game_count}")
    print(f"Champion-game rows: {cg_count}")
    print(f"Names rows: {len(names_tuples)}")
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
