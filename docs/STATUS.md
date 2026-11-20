# Project status — shared log for David, Devin, Junie and Hark

Newest entries at the bottom. Every agent appends one entry per finished task.

## Where things stand (as of 2026-11-20)
- **Hackathon:** Cloud9 x JetBrains "Sky's The Limit" 2026, Category 2 (LoL). Deadline Jan 12, 2027, 11:00 AM PST. Video recorded by Jan 5.
- **Spec:** `BUILD.md` (locked Nov 20). Data gate: GO.
- **Week 1–2 goal:** ingest + normalize, role inference, legal-pool reconstruction.
- **Dev setup:** David on Windows, PyCharm 2026.3 (Pro trial ends ~Dec 20), uv-managed `.venv` (Python 3.11), Junie Free via JetBrains Air. Laptop has 8 GB RAM and a nearly full C: drive, so stream everything and keep memory low.

## GRID feed shape (verified on real data, not in the starter docs)
- Files: `events_<series_id>_grid.jsonl.zst`. Each line is an envelope: `id, correlationId, occurredAt, seriesId, sequenceNumber, events[]`.
- Each event: `id, includesFullState, type, actor, action, target, seriesStateDelta, seriesState`. `seriesState` is huge, so never parse it.
- Draft types: `team-banned-character`, `team-picked-character`, `team-!banned-character` (ban undo), `team-!picked-character` (pick undo).
- Team: `ev["actor"]["id"]`. Champion: `ev["target"]["state"]["name"]`.
- **Game id: `ev["seriesStateDelta"]["games"][0]["id"]`** (there is no `gameId` key).
- Draft order: `ev["seriesStateDelta"]["games"][0]["draftActions"][0]["sequenceNumber"]`.
- `meta/games.csv` gives patch, game_number and team sides. `meta/rosters.csv` gives per-game player→champion.
- Expected totals across the dataset: 1,739 games, 23 incomplete (dropped) → 1,716, 288 ban undos, 173 pick undos.

## Log
- **2026-11-20 · Devin** · Scaffolded repo: BUILD.md, .gitignore license boundary, pre-push hook, `scripts/check_data_boundary.sh`, stubs, contract tests.
- **2026-11-20 · David** · Rewrote all commits to cipoklean; added `.aiignore`; committed `.idea/`.
- **2026-11-20 · Junie** · Completed `static/champion_roles.csv` (173 champions). Hark checked it against Riot's list and pro usage; fixed Maokai/Zyra → jungle, Elise → support, Vladimir → top, Nunu → "Nunu & Willump".
- **2026-11-20 · Junie** · Implemented `ingest/feeds.py` (true zstd streaming) and `pipeline/normalize.py` (undo folding, slots, completeness drop, game flags); 20 tests pass.
- **2026-11-20 · Hark** · Ran ingest on real data: game id came out empty and side defaulted to blue (wrong field). Fix prompt sent to Junie (read the game id from `seriesStateDelta`, order by `sequenceNumber`, never default side).
- **2026-11-20 · Devin** · Added `.github/workflows/ci.yml`: pytest + `scripts/check_data_boundary.sh` on every push and PR, Python 3.11 via uv (`uv sync --locked --extra dev`). First run failed: `uv.lock` was stale (missing `zstandard`); regenerated with `uv lock` and re-pushed — the `--locked` check did its job. Whoever edits `pyproject.toml` deps must run `uv lock` in the same commit.

## Next up
1. Junie: game-id fix above → Hark reruns the full-dataset check against the expected totals.
2. Role inference (player → role, generated into `derived/`, not committed).
3. Legal-pool reconstruction (Hard Fearless).
4. David: register on Devpost (trial codes arrive within 5 business days); start `docs/ai-workflow.md` for the $1k bonus.
- **2026-11-20 · Hark** · Full-dataset run of the ingest + normalize pipeline (8598b6d). Findings: (1) 38 series raise because 44 feed games aren't in `meta/games.csv`. These are abandoned or restarted drafts, and **all 461 undo events live in those 44 games; real games have no undos.** Fix: skip and count them, don't raise. (2) Grouping by `game_num` collides across series, so group by `game_id`. (3) `sequenceNumber` *is* the draft slot (1–20), and an undo carries the same seq as the action it undoes. Slot-template walking gave 3,021 `unknown` phases; use seq directly (1–6 ban1, 7–12 pick1, 13–16 ban2, 17–20 pick2). (4) Replace the `_skipped_count` sentinel row with a separate stats return. Fix prompt sent to Junie.
