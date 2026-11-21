# AI workflow story: building Unpicked Intent with PyCharm, Junie and JetBrains Air

> Bonus-prize write-up for Cloud9 x JetBrains "Sky's The Limit" 2026.
> Living document: one row per Junie task, updated as the build goes on.

## TL;DR

I build alone, on an 8 GB Windows laptop with a nearly full drive. Every line of
feature code in this repo was written by **Junie** inside **PyCharm 2026.3**
(through **JetBrains Air**), from short task prompts I pasted in. Junie's own
tests ran on synthetic fixtures and passed every time. The interesting part of
the story is what happened next: after every push, an independent run on the
full GRID dataset (1,716 pro games) checked the result against known totals,
and that run found **15 real-data bugs that every green test had missed**.
Each one went back to Junie as a fix prompt, and each fix added a test that
would have caught it. The suite grew from 20 tests to 149.

## Setup

| Piece | What it does here |
|---|---|
| PyCharm 2026.3 | IDE, uv-managed `.venv` (Python 3.11), test runner. `.idea/` is committed as evidence. |
| Junie (via JetBrains Air) | Writes all feature code and tests from task prompts, runs pytest, commits and pushes. |
| `.junie/guidelines.md` | Standing rules Junie reads on every task: the license boundary, the architecture, "one game is one unit of evidence", n ≥ 4, synthetic fixtures only. |
| `.aiignore` | Keeps GRID feeds and every derived artifact out of Junie's context, so the agent never sees licensed data. |
| `BUILD.md` | The locked spec. Junie implements against it; only I change it (two amendments so far, both dated). |
| `AGENTS.md` + `docs/STATUS.md` | Shared memory. Every agent appends one entry per finished task, so the next task starts from the current state instead of from my memory. |

Other AI help, for transparency: Devin set up CI (pytest plus a data-boundary
check on every push), and a second assistant (Hark) ran the full-data checks
and drafted the Junie prompts. Junie wrote all the product code.

## The loop

1. **Prompt.** A short, specific task: what to build, the exact columns and
   thresholds, the tests to write, and "pull first, append to STATUS, push".
2. **Junie builds it in PyCharm.** Code plus tests on synthetic fixtures, with
   pytest green before it pushes. The pre-push hook blocks any licensed data.
3. **Real-data check.** The pushed commit runs end to end on the full GRID set,
   and its output is compared against totals we already know (1,716 complete
   games, 593,736 champion-game rows, 10 bans per team-game, and so on).
4. **Fix prompt.** Every mismatch becomes a precise prompt with a cause, a
   fix and a regression test. Back to step 2.

The lesson for anyone using a coding agent on real data: **synthetic tests
check the code against your assumptions, and real data checks your
assumptions.** The second check is what made this work.

## What the real data caught

| # | Commit checked | Green tests, but on real data… | Root cause | Test Junie added |
|---|---|---|---|---|
| 1 | 8598b6d | game id empty, every side "blue" | read the wrong JSON field | game id from `seriesStateDelta` |
| 2 | 8598b6d | 38 series crashed | 44 abandoned drafts not in `games.csv` (and all 461 undo events live in them) | skip and count them |
| 3 | 8598b6d | games merged across series | grouped by game number, not game id | group by `game_id` |
| 4 | 5363338 | wrong sides in late draft slots | slots 13–20 order wrong | real pro draft order |
| 5 | 5363338 | 240 games with sides flipped | `games.csv` sides disagree with the draft | side from the draft, majority vote |
| 6 | 7538134 | output in `src/derived/` | path off by one directory | path assertion |
| 7 | 7538134 | 1,101 "players", 819 of them Valorant | rosters file mixes games | `title == "lol"` filter |
| 8 | 7538134 | 33 one-role pros flagged "flex" | 70% threshold too strict for flex champions | threshold 0.5 |
| 9 | e1f252b | "no feed files found" | feeds live in `lol/`, not `data/` | look in both |
| 10 | e1f252b | baseline crashed (`KeyError: patch`) | table had no patch column; fixture dicts did | end-to-end test through DuckDB |
| 11 | 5df6d02 | **0 scouting tells** | `game_id` never written to the table | assert tells > 0 end to end |
| 12 | 5df6d02 | rerun doubled every row | tables appended instead of replaced | run twice, counts unchanged |
| 13 | a1e543a | early-ban dropped to 0 rows | a new column shifted a `SELECT *` | explicit column names everywhere |
| 14 | 62d5eaf | pytest silently deleted the scouting DB (found when the static export failed) | `finally` restored `build_mod.DB_PATH` to the real path before calling `unlink()` | guard test asserts real `derived/unpicked.duckdb` is untouched |
| 15 | 8a3a9ad | build stalled >50 min on ARM VM right after dropping incomplete games | `con.executemany` wrote ~594k `champion_game` rows one-by-one in DuckDB Python API | bulk CSV `COPY` matches `executemany` row counts & `SUM(hash(...))` on every table |

## Where a human made the call

Junie implemented; decisions that change what the tool *claims* stayed with me:

- **Patch windows.** Late 2026 patches have 2–6 games each, so I amended
  BUILD.md so every window grows back to at least 100 games.
- **Multiple testing.** With 2,029 candidate tells, about 100 would look
  "significant" by chance. Junie added Benjamini-Hochberg q-values, and the UI
  labels only q < 0.2 as strong. Everything else is a lead, always shown
  with its n and expected-vs-observed.

## Task log

| Date | Commit | Junie task | Tests after |
|---|---|---|---|
| 2026-11-20 | b9e8712 | Champion → role table (173 champions) | n/a |
| 2026-11-20 | 8598b6d | Streaming zstd ingest + normalize (undos, slots, completeness) | 20 |
| 2026-11-20 | 5363338 | Four ingest/normalize fixes (#1–3) | n/a |
| 2026-11-20 | a0fca6d | Draft order and side fixes (#4–5) | n/a |
| 2026-11-20 | e7c2cdf | Majority-vote sides + Hard Fearless legal pool | n/a |
| 2026-11-20 | 7538134 | Player → role inference | 67 |
| 2026-11-20 | ca6747d | Role fixes (#6–8) | 71 |
| 2026-11-20 | e1f252b | DuckDB build step + baseline | 83 |
| 2026-11-20 | d49e065 | Feed path + patch join (#9–10) | 84 |
| 2026-11-20 | 94c6a0c | Variable-length patch windows (≥100 games) | 88 |
| 2026-11-20 | 5df6d02 | Signals: left-open (Poisson-binomial) + early-ban | 110 |
| 2026-11-20 | a1e543a | game_id, idempotent tables, BH q-values (#11–12) | 116 |
| 2026-11-20 | 34a80e0 | Explicit-column reads (#13) | 116 |
| 2026-11-20 | 19d7651 | Non-vacuous E2E test, `names` table, read-only scouting API | 127 |
| 2026-11-20 | 26e73e7 | Silence Report web UI + self-scout toggle (UI designed by Gemini in Junie; offline HTML/CSS/JS via FastAPI `StaticFiles`) | 130 |
| 2026-11-20 | e13cf46 | Web UI wording fixes: hedge lead action lines (`p < 0.05`) in opponent & self-scout views, update self-scout banner copy | 131 |
| 2026-11-20 | 62d5eaf | Static site exporter (`export_static.py`) for Vercel, `app.js` `data-static="1"` switch, `dist/` data boundary & Deploy README | 137 |
| 2026-11-20 | 8a3a9ad | Fix pytest deleting real `derived/unpicked.duckdb` (#14), audit test `unlink`s, add real-DB guard test | 138 |
| 2026-11-20 | 0dc8e45 | Replace DuckDB `executemany` with CSV `COPY` `_bulk_insert` across build/baseline/signals (#15), timing logs, checksum equivalence test | 139 |
| 2026-11-20 | 5109d79 | Full code/lint/CI cleanup (0 ruff errors, O(N) legal-pool grouping, dead-code removal, `played_at` sort fallback, `pipeline/run_all.py` single-command runner, `.gitignore`/`.aiignore` fixes) & hackathon README curation | 142 |
| 2026-11-21 | a841dbc | Draft Replay view: target-team 5-game replay API (`/replay/{team_id}/games`, `/replay/{team_id}/{game_id}`) with Fearless burned + per-slot tell states, static exporter support, 20-cell Blue/Red slot stepper UI, and API/export tests | 145 |
| 2026-11-21 | 6544a1f | UI framing tweaks (no scoring changes): `GET /meta` (`total_tests`) + `data/meta.json` export, BH honesty-bar copy across Silence Report & Draft Replay, muted 0-strong KPI with subtitle, `strong_count` on `/teams` + strong-tell default opponent in `getDefaultOpponentId()` | 147 |
| 2026-11-21 | ec5e2fa | Forensic UI polish passes 1–4 (sentence-case labels, quiet KPI strip + live `Patterns tested` chip, 6-col tells table dropping duplicate `n`/`Obs`/`Exp`, cyan accent consolidation) & Draft Replay "Next open tell" button + `n` shortcut | 149 |
| 2026-11-21 | 7ab521b | Tighten Draft Replay "Next open tell" to team pick slots with an open `strong` tell or `p < 0.05` lead (reusing `isQualifyingActionLead`), update tooltip title, and expand Node-executed replay test | 149 |

*Current status: scouting API, offline Silence Report + Draft Replay web UI, and static Vercel site live at https://unpicked-intent.vercel.app. Next: re-export and redeploy Draft Replay to Vercel, capture PyCharm/Junie session screenshots (before ~Dec 20 Pro trial expiry), and record the ≤3 min demo video (by Jan 5).*
