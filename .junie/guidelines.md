# Junie project guidelines — Unpicked Intent Engine

## The one rule that overrides everything

**License boundary: "could this file exist without the feed?"**
GRID feed data and anything derived from it must never be written to a
repo-tracked path. Raw feeds → `data/`; all generated artifacts
(normalized events, legal pools, aggregates, player→role maps, any
csv/sqlite/duckdb/parquet/jsonl/zst) → `derived/`. Both are gitignored
and a pre-push hook blocks violations. `static/champion_roles.csv` is
the only committed CSV — it is hand-sourced game knowledge.

## Architecture (see BUILD.md)

- `src/unpicked_intent/ingest/` — stream GRID feeds → normalized draft events
- `src/unpicked_intent/pipeline/` — undos, completeness, Bo1 flagging, legal-pool reconstruction (Hard Fearless: prior-game picks burned for BOTH teams)
- `src/unpicked_intent/scoring/` — baseline (3-played-patch window × role, all regions) and signals (per-GAME evidence units, binomial expected-vs-observed, n≥4 display floor)
- `src/unpicked_intent/roles/` — champion priors (static file) + generated player inference
- `src/unpicked_intent/api/` — FastAPI serving PRECOMPUTED aggregates only; never serve raw feed rows

## Conventions

- Python 3.11, DuckDB for storage/aggregation, FastAPI for the web layer.
- Unit of evidence for all statistics is ONE GAME, never a draft slot.
- Every surfaced claim carries n and expected-vs-observed; suppress
  anything with n < 4.
- Tests run on synthetic fixtures under `tests/fixtures/` — never on
  feed data.
- pytest for tests. Keep functions small; the pipeline stages are the
  seams.
