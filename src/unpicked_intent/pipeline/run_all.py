"""Single-command full pipeline runner.

CLI:
    python -m unpicked_intent.pipeline.run_all --grid <path to GRID starter repo>
    python -m unpicked_intent.pipeline.run_all --grid <path to GRID starter repo> --out dist

Runs all pipeline stages in the required order:
    1. pipeline.build       — ingest -> normalize -> legal_pool -> derived/unpicked.duckdb
    2. roles.infer_players  — LoL player role inference -> derived/player_roles.csv
    3. scoring.baseline     — variable-length played-patch windows -> baseline table
    4. scoring.signals      — Poisson-binomial left-open tells + early-ban pressure
    5. export_static        — (optional, when --out is given) static JSON + web assets
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path


def run_all(grid_path: str, out_dir: str | Path | None = None) -> None:
    """Execute the full Unpicked Intent build and scoring pipeline end to end."""
    from unpicked_intent.pipeline.build import run_pipeline
    from unpicked_intent.roles.infer_players import run_infer_players
    from unpicked_intent.scoring.baseline import run_baseline
    from unpicked_intent.scoring.signals import run_signals

    t0 = time.perf_counter()

    print("[1/4] Building normalized draft tables & Hard Fearless legal pools...")
    run_pipeline(grid_path)

    print("\n[2/4] Inferring LoL player roles...")
    run_infer_players(grid_path)

    print("\n[3/4] Computing variable-length patch-window baselines...")
    run_baseline()

    print("\n[4/4] Scoring left-open tells (Poisson-binomial + BH) & early-ban pressure...")
    run_signals(grid_path)

    if out_dir is not None:
        from unpicked_intent.export_static import export_static

        print(f"\n[Export] Writing static site to {out_dir}...")
        summary = export_static(out_dir)
        print(
            f"Exported {summary['teams']} teams, {summary['silence_files']} silence files, "
            "self-scout.json, and web assets."
        )

    elapsed = time.perf_counter() - t0
    print(f"\nPipeline complete in {elapsed:.1f}s.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the full Unpicked Intent pipeline (build -> roles -> baseline -> signals)."
    )
    parser.add_argument(
        "--grid",
        required=True,
        help="Path to the GRID starter repo root (contains lol/ or data/ and meta/).",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Optional output directory to also export the static site (e.g. dist).",
    )
    args = parser.parse_args(argv)

    try:
        run_all(args.grid, out_dir=args.out)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
