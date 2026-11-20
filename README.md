# Unpicked Intent Engine

**Scout what opponents *didn't* pick.**

A League of Legends Hard Fearless Draft scouting engine built for the
[Cloud9 × JetBrains "Sky's The Limit" Hackathon 2026](https://cloud9.devpost.com/) —
**Category 2: Macro Scouting & Draft Management**.

- **Live Demo:** [unpicked-intent.vercel.app](https://unpicked-intent.vercel.app)
- **AI Workflow Case Study ($1k Bonus):** [AI_SHOWCASE.md](AI_SHOWCASE.md)
- **Spec & Data Gate:** [BUILD.md](BUILD.md)

---

## The Problem & Insight

Every conventional scouting dashboard counts what a team *picked*. Under **Hard
Fearless Draft** rules—where any champion picked in an earlier game of a series
is permanently burned for both teams—the exact legal champion pool at every
draft decision is reconstructible from data.

When a champion is **legal**, **meta-relevant**, and **proven in a player's
champion pool**, yet the team repeatedly leaves it untouched, that silence is a
scouting tell: either the player is uncomfortable piloting it in the current
meta, or the team is baiting a response.

Unpicked Intent surfaces three views built strictly from precomputed aggregates:

1. **Left-Open Tells** — Per `(team, player, champion)` avoidance patterns
   ranked by exact Poisson-binomial `p`-value and corrected for multiple testing
   via Benjamini–Hochberg `q`-values, filterable and groupable by role, player,
   and signal tier.
2. **Early-Ban Pressure** — Champions a team bans significantly earlier than
   the meta baseline across draft slots `1–20` (`delta = baseline_mean_slot - team_mean_slot`),
   surfacing respect/fear bans.
3. **Self-Scout: Cloud9 Kia** — One-click mirror view (`GET /self-scout`)
   running the exact same engine on Cloud9 Kia's drafts to show what opposing
   coaches can read about Cloud9.

---

## Statistical Honesty Rules

Designed for pro coaching rooms—never inflating sample sizes or overclaiming
noisy leads:

- **One game = one unit of evidence.** Draft slots within the same game are
  never counted as independent trials.
- **Exact Hard Fearless resolution.** Every `(game, team, champion)` resolves
  to exactly one status with strict priority:
  `burned > banned > taken > denied > skipped`. Only `taken` and `skipped`
  enter the binomial test (`denied` by the opponent is never miscounted as a
  skip).
- **Variable-length played-patch windows.** Baselines pool all regions over at
  least 3 played patches and extend backwards until each window holds at least
  **100 complete games** (`MIN_WINDOW_GAMES = 100`), handling skipped
  competitive patches cleanly.
- **Exact Poisson-binomial + Benjamini–Hochberg FDR.** Every qualifying game
  contributes its own patch-window baseline rate (exact dynamic programming, no
  normal approximation). Only tells surviving `q < 0.2` are labelled
  **`STRONG`**; all other qualifying patterns are visibly marked **`LEAD`** with
  hedged action copy.
- **Hard `n ≥ 4` floor.** Every surfaced claim prints `n`, `observed`, and
  `expected` in plain language alongside `"Only tells with at least 4
  qualifying games are shown."`

### Validated Full-Dataset Invariants (2025–2026 Pro Play: LCK, LEC, LTA, LCS)

| Metric | Verified Total |
|---|---|
| Pro series / complete games ingested | **745 series / 1,716 complete games** (19 incomplete dropped, 44 abandoned drafts skipped) |
| Hard Fearless multi-game series | **568 series** — zero champion repeats |
| Exact `champion_game` resolutions | **593,736 rows** (`1,716 × 2 × 173` champions, exact) |
| Side disagreements auto-corrected | **241 games** where `games.csv` sides disagreed with the 20-slot draft order |
| Active LoL players / teams resolved | **282 players / 45 teams** (8 live LCS targets listed first) |
| Scored scouting outputs | **2,029 left-open tells** (4 `strong` at `q < 0.2`) · **1,346 early-ban rows** |

---

## Architecture

```text
GRID .jsonl.zst feeds + meta/*.csv (local, gitignored)
  │
  ├─► ingest/feeds.py          True line-by-line zstd streaming; skips abandoned drafts
  ├─► pipeline/normalize.py    Undo folding, 20-slot majority-vote side derivation, Bo1/Fearless flags
  ├─► pipeline/legal_pool.py   Hard Fearless burn tracking -> burned/banned/taken/denied/skipped
  ├─► roles/infer_players.py   LoL-only player->role argmax over static/champion_roles.csv (flex < 50%)
  ├─► pipeline/build.py        Bulk CSV COPY loader -> derived/unpicked.duckdb
  │
  ├─► scoring/baseline.py      Played-patch windows (>=3 patches, >=100 games) -> baseline table
  ├─► scoring/signals.py       Exact Poisson-binomial DP + BH q-values -> left_open & early_ban tables
  │
  ├─► api/main.py              Read-only FastAPI (/teams, /silence/{id}, /self-scout) + StaticFiles UI
  ├─► web/                     Zero-build, zero-CDN offline HTML/CSS/JS coaching UI
  └─► export_static.py         Exports byte-identical JSON + static UI -> dist/ (Vercel)
```

### Repository Map

| Path | Purpose |
|---|---|
| `src/unpicked_intent/ingest/` | Streaming zstd GRID feed reader (`feeds.py`) |
| `src/unpicked_intent/pipeline/` | Normalization, Hard Fearless legal pools, DuckDB schema/bulk loader (`build.py`), single-command runner (`run_all.py`) |
| `src/unpicked_intent/roles/` | Hand-sourced champion priors loader (`champion_roles.py`) and player role inference (`infer_players.py`) |
| `src/unpicked_intent/scoring/` | Patch-window baselines (`baseline.py`) and Poisson-binomial + early-ban scoring (`signals.py`) |
| `src/unpicked_intent/api/` | Read-only FastAPI service (`main.py`) serving precomputed DuckDB aggregates only |
| `src/unpicked_intent/web/` | Offline Silence Report & Self-Scout frontend (`index.html`, `styles.css`, `app.js`) |
| `src/unpicked_intent/export_static.py` | Static site exporter via FastAPI `TestClient` for backend-free Vercel hosting |
| `static/champion_roles.csv` | The one committed CSV: 173 LoL champions → primary role (hand-sourced game knowledge) |
| `tests/` | 142 automated tests on synthetic fixtures only (`tests/fixtures/`) |
| `scripts/check_data_boundary.sh` | License-boundary guard run in `.githooks/pre-push` and GitHub Actions CI |

---

## Quick Start

Requires Python 3.11+ and [`uv`](https://docs.astral.sh/uv/).

### 1. Install & Verify (No GRID Data Needed)

All 142 tests run on synthetic fixtures under `tests/fixtures/`:

```bash
uv sync --locked --extra dev
git config core.hooksPath .githooks     # enables pre-push license-boundary check
uv run pytest -q
uv run ruff check src tests
bash scripts/check_data_boundary.sh
```

### 2. Run the Full Pipeline on GRID Data

Point `--grid` at your local GRID starter dataset (never committed):

```bash
uv run python -m unpicked_intent.pipeline.run_all --grid <path-to-grid-repo>
```

This executes all four stages in order—writing `derived/unpicked.duckdb` and
`derived/player_roles.csv` (both gitignored). Pass `--out dist` to also export
the static site in the same run, or run stages individually:

```bash
uv run python -m unpicked_intent.pipeline.build --grid <path-to-grid-repo>
uv run python -m unpicked_intent.roles.infer_players --grid <path-to-grid-repo>
uv run python -m unpicked_intent.scoring.baseline
uv run python -m unpicked_intent.scoring.signals --grid <path-to-grid-repo>
```

### 3. Launch the Local Web UI & API

Zero Node, zero build step, zero external CDNs:

```bash
uv run uvicorn unpicked_intent.api.main:app --reload --host 127.0.0.1 --port 8000
```

Open `http://127.0.0.1:8000/`. API endpoints: `GET /health`, `GET /teams`,
`GET /silence/{team_id}`, and `GET /self-scout`.

### 4. Export & Deploy Static Site (Vercel)

```bash
uv run python -m unpicked_intent.export_static --out dist
vercel deploy dist --prod
```

---

## Data Governance & License Boundary

Every file in this repository follows one strict rule:

> **Could this file exist without the GRID feed?** If not, it never gets committed.

- `data/` (raw feeds), `derived/` (`unpicked.duckdb`, `player_roles.csv`,
  optional `player_role_overrides.csv`), and `dist/` (static exports) are all
  gitignored, excluded in `.aiignore`, and blocked by
  `scripts/check_data_boundary.sh` on both `.githooks/pre-push` and CI.
- `static/champion_roles.csv` is the only committed CSV—a plain two-column,
  hand-sourced table of all 173 champions with no comment lines (so
  `csv.DictReader` and DuckDB `read_csv` parse it directly).

---

## Built with PyCharm & Junie

All feature code and tests were written with **Junie** inside **PyCharm 2026.3**
via **JetBrains Air** (`.idea/` and `.junie/` are committed as tooling
evidence). Keeping Junie blind to licensed feeds via `.aiignore` while running
independent full-dataset invariant checks after each push caught **15 subtle
real-data bugs** that green synthetic tests missed—and turned every single one
into a permanent regression test. Read the full case study in
[AI_SHOWCASE.md](AI_SHOWCASE.md).

---

## License

[MIT](LICENSE)
