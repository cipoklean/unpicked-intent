# BUILD — Unpicked Intent Engine

Cloud9 × JetBrains "Sky's The Limit" Hackathon 2026 — Category 2 (LoL: Macro
Scouting & Draft Management). Submission deadline **Jan 12, 2027 @ 11:00am PST**.
Video recorded by **Jan 5, 2027** (buffer across holidays).

## The idea, one paragraph

Every scouting tool counts what a team picked. This tool counts what they
*didn't* pick but should have. Under Fearless Draft rules a champion picked in
an earlier game is burned for both teams, so the legal pool shrinks every game
— and the counterfactual "what could they have taken at this slot" is
reconstructible from data. A champion that was legal, meta-relevant, in the
player's pool, and skipped repeatedly is a tell: can't pilot it, or baiting a
counter. Output: a per-opponent "Silence Report" of evidence-backed tells,
plus a draft-replay UI a coach steps through slot by slot.

## Why it wins

- Answers the rubric verbatim: *"How does the opponent's draft strategy shift
  in a Fearless Draft Game 3, 4, or 5 scenario?"*
- Produces recommendations ("save the Azir ban"), matching the spec's
  *"Data-Backed How to Win Insights"* next-level.
- Novel axis: negative-space analysis vs. every pick-rate dashboard.

## Data gate results (GO, Nov 20 2026)

- 745 LoL series / 1,739 games across LCK, LEC, LTA, LCS (2025–2026).
- **569 multi-game series (1,563 games): zero champion repeats** → all
  consistent with Hard Fearless. Legal pool = all champions minus this-game
  bans minus prior-game picks (either team).
- 176 Bo1 series: excluded from cross-game logic, still feed the baseline.
- Draft completeness: 1,716/1,739 games have full 10 picks + 10 bans.
  23 incomplete games dropped. **288 ban undos + 173 pick undos exist**
  (`team-!banned-character`) — must be applied before legality is computed.
- Median live-LCS opponent: **28 Fearless series, ~390 pick decisions.**
  2026-active opponents: FlyQuest, Team Liquid, LYON, Shopify Rebellion,
  Disguised, Dignitas, Sentinels. (100T, LOUD are 2025-only.)
- Cloud9 Kia: 45 Fearless series / 134 games → self-scout mode viable.
- Patch windows: use **3 last-played patches** (16.5, 16.12, 16.13 have no
  games — competitive skipped them) or trailing ~8 weeks.
- LCK alone: 209 Fearless series → cross-region prior fallback if needed.

## License boundary — "could this file exist without the feed?"

The test for whether a file may be committed:

- **Commit:** code, schemas, tests, synthetic fixtures, `BUILD.md`, the
  champion→role table (hand-sourced game knowledge).
- **Never commit:** raw feeds, and anything *derived* from them — normalized
  events, legal pools, aggregates, player→role maps, sqlite/duckdb/parquet/
  csv extracts. All generated artifacts live under `derived/` or `data/`,
  both gitignored. A pre-push hook enforces this (`scripts/check_data_boundary.sh`).
- **Open question for C9 Discord/office hours:** whether small derived
  aggregates (e.g., a per-player role CSV) are acceptable in a public repo,
  and whether README screenshots of the UI showing aggregates are okay.
- Showing insights in the demo/UI/video is fine; redistributing
  machine-readable data is what's barred.

## Data contract

Normalized draft event (one row per draft action, undos applied):

```
series_id, game_num, slot, phase(ban1|pick1|ban2|pick2), side(blue|red),
team, action(ban|pick), champion, patch, league, played_at
```

Per (team, champion) within a series, each game resolves to exactly one of:

- **skipped** — legal, unbanned, opponent didn't take it first, team didn't pick
- **taken** — team picked it
- **denied** — opponent picked it first (separate signal: pool denial)
- **banned** — banned this game (either team)
- **burned** — picked by anyone in a prior game of the series

Only `skipped` and `taken` enter the binomial.

## Signals

Unit of evidence = **one game** where the champion stayed legal through that
player's pick. Slots are shown as color in the replay UI, never as n.

1. **Left-open rate** — "legal in k games, taken t, expected e from
   baseline." Significance = binomial(taken | k, baseline_rate).
2. **Early-ban pressure** — champion's mean ban position this window vs.
   its meta-average ban position. Only computed when it has enough bans to
   have a meaningful mean.
3. **Fit score** — falls out of normalization: only champions in the
   player's pool ∩ meta-flagged are candidates. Rank left-open tells by
   how strongly pool ∩ meta predicted a pick.

**Honesty rules:** rank tells by p-value; only display tells with n ≥ 4
qualifying games; print the n and expected-vs-observed on every claim —
"we only show tells with at least 4 qualifying games" appears on screen.

Optional later: stratify expected rates by side (blue first-picks
contested champions more often).

## Baseline vs. observed (the two scoring jobs)

- **Baseline (fat):** expected pick/ban rates keyed on
  **3-played-patch window × role**, pooled over **all regions** (1,563
  games). LCS-only rate shown as context only. Feeds the meta-relevant
  flag and the binomial expectation.
- **Observed (thin):** pooled per opponent across the whole split; each
  skip judged against the baseline *of its own patch window*.

## Roles

- **Champion→role:** committed static table (`static/champion_roles.csv`),
  hand-sourced from game knowledge — NOT computed from GRID (keeps it
  outside the derivative question entirely).
- **Player→role:** generated at build time: player role = argmax over
  champion history × champion-role priors; hand-fix the few flex players
  in a local override file under `derived/`. Never committed.
- Picks map to players via `meta/rosters.csv` (draft events are
  team-level only).

## Stack

- **Engine:** Python, DuckDB for aggregates. Built in PyCharm with Junie;
  `.junie/` committed (evidence + guidelines). AI-workflow bonus ($1k)
  story documented as we go.
- **UI:** FastAPI + light web frontend serving **precomputed aggregates
  only** — raw feeds never ship. Live link required for a website
  submission.
- **Repo:** public GitHub, OSI license file at root.

## UI pages

1. **Draft replay** — pick a series/game, step through slots; at each
   slot show legal pool, what was passed, expected-vs-observed.
2. **Silence report** — one-pager per opponent: ranked tells, evidence
   trails, recommended actions. Defaults to Cloud9 Kia's next opponent
   (live-LCS seven).
3. **Self-scout toggle** — same engine pointed at C9's own drafts:
   "what opponents infer about us."

## Demo script (<3 min)

Pick a real C9 opponent → step through their Game 3 draft slot by slot →
stop on the pick where their mid's best champion sat legal → silence
report → the "save the ban / expect the bait" recommendation → one line
on how Junie/JetBrains built it.

## Timeline

| Weeks | Work |
|---|---|
| 1–2 | Ingest+normalize (undos, completeness), role inference, legal-pool reconstruction, validated vs known series |
| 3–4 | Baseline + observed layers, binomial signals, first real tells |
| 5–6 | UI (replay + report), self-scout toggle, hosting |
| 7 | Polish, methodology + tooling write-ups, **record video by Jan 5** |
| buffer | Submit by Jan 12 @ 11:00am PST |

## Submission checklist

- [ ] Public repo + open-source LICENSE at root; `.junie` committed
- [ ] Live demo link or judge-accessible build
- [ ] ≤3 min public YouTube video
- [ ] Methodology write-up + JetBrains tooling explanation
- [ ] Category 2 selected on submission form
- [ ] (optional) Bonus: AI-workflow case study URL
- [ ] `scripts/check_data_boundary.sh` clean; Discord answer on aggregates
