# Unpicked Intent Engine

Scout what opponents *didn't* pick. A League of Legends Fearless Draft
scouting tool built for the
[Cloud9 × JetBrains "Sky's The Limit" Hackathon 2026](https://cloud9.devpost.com/)
— Category 2: Macro Scouting & Draft Management.

Read [BUILD.md](BUILD.md) for the full spec, data-gate results, and
timeline.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
git config core.hooksPath .githooks   # enables the license-boundary pre-push check
```

## Data (never committed)

Raw GRID feeds go in `data/`, all generated artifacts in `derived/`, and
static site exports in `dist/` — all gitignored. Verify before pushing:

```bash
./scripts/check_data_boundary.sh
# equivalent manual check:
git ls-files | grep -nE '(^|/)(data|derived|dist)/|\.(zst|jsonl|parquet|duckdb|db|sqlite3?|csv)$' \
  | grep -vE 'static/champion_roles\.csv|tests/fixtures/' && echo "VIOLATION" || echo "clean"
```

Rule of thumb: **could this file exist without the feed?** If not, it
doesn't get committed.

`static/champion_roles.csv` is the one committed CSV: a hand-maintained
champion→role table with all 173 champions, sourced from game knowledge
(not computed from GRID data). It is a plain two-column CSV — no comment
lines, as `csv.DictReader` and DuckDB `read_csv` do not skip them.

## Tests

```bash
pytest
```

Tests run on synthetic fixtures only — never on feed data.

## Deploy

Export the precomputed Silence Report UI and JSON payloads into `dist/` (gitignored) and deploy to Vercel as a static site:

```bash
PYTHONPATH=src python -m unpicked_intent.export_static --out dist
vercel deploy dist --prod
```
