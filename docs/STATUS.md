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
- **2026-11-20 · Junie** · Fixed all four bugs: (1) `feeds.py` skips abandoned drafts, returns `IngestStats(skipped_games, events_read)`. (2) `normalize.py` groups by `game_id` everywhere. (3) `apply_undos` matches by seq, fallback to champion+team; `assign_slots` sets slot=seq, phase from seq ranges, counts side mismatches via `NormalizeStats`. (4) Removed `_skipped_count` from event stream. Added SYN-3 fixture with game_num=1 collision + abandoned draft. 35 tests pass.
- **2026-11-20 · Hark** · Reran the full dataset on 5363338. 0 errors, 44 abandoned drafts skipped, 1,735 games, **1,716 complete (matches the gate)**, 19 dropped, no unknown phases. Remaining: (1) `DRAFT_SIDE_ORDER` slots 13–20 are wrong; real pro order is ban2 R B R B, pick2 R B B R (confirmed on data). (2) In 240 games, `games.csv` sides are flipped against the draft order (blue always acts at seq 1). Derive side from the draft and count meta disagreements. (3) Bo1 and Fearless flags are computed after incomplete games are dropped, so 6 series turn into fake Bo1s (177 vs 176, 563 vs 569 multi-game). Count series games before dropping.
- **2026-11-20 · Junie** · Fixed three issues from full-dataset check: (1) Corrected `DRAFT_SIDE_ORDER` slots 13–20 to ban2 R B R B, pick2 R B B R. (2) Derive each team's side from the draft (team at seq 1 = blue); keep games.csv side as `meta_side`; added `meta_side_disagreements` to `NormalizeStats` (expect ~240 on real data, 0 `side_mismatches`). (3) `flag_games` accepts `series_game_count` (pre-drop) so `is_bo1` and `fearless_status` use the series' full game count, not the count after incomplete games are dropped. Added G4 fixture with swapped sides. 42 tests pass.
- **2026-11-20 · Hark** · Third full-dataset check (a0fca6d): 44 abandoned drafts skipped, 1,716 complete games, 241 games where `games.csv` sides disagree with the draft, 0 phase errors. Flags: 568 Hard Fearless multi-game series, 172 Bo1, 0 non-Fearless. The 5 series missing from the gate's 745 have no complete game. The only leftover is 18 side mismatches, all in one game (series 2901037), where GRID credits the seq 1 ban to the wrong team. Fix: derive blue by majority vote across all 20 slots, not seq 1 alone. **Ingest + normalize is otherwise validated; next is the legal-pool reconstruction.**
