-- Normalized draft-event table (DuckDB).
-- One row per draft action, AFTER undo events have been applied.
-- Populated by pipeline/normalize.py from raw feeds under data/.
-- This table is a derived extract: it lives in derived/*.duckdb and is
-- never committed.

CREATE TABLE IF NOT EXISTS draft_events (
    series_id   VARCHAR NOT NULL,
    game_num    INTEGER NOT NULL,          -- 1-indexed within the series
    slot        INTEGER NOT NULL,          -- order within the draft
    phase       VARCHAR NOT NULL CHECK (phase IN ('ban1','pick1','ban2','pick2')),
    side        VARCHAR NOT NULL CHECK (side IN ('blue','red')),
    team        VARCHAR NOT NULL,
    action      VARCHAR NOT NULL CHECK (action IN ('ban','pick')),
    champion    VARCHAR NOT NULL,
    patch       VARCHAR NOT NULL,
    league      VARCHAR NOT NULL,
    played_at   TIMESTAMP,
    PRIMARY KEY (series_id, game_num, slot)
);

-- Per-game completeness + format flags, set by normalize.py
CREATE TABLE IF NOT EXISTS game_flags (
    series_id        VARCHAR NOT NULL,
    game_num         INTEGER NOT NULL,
    n_picks          INTEGER NOT NULL,
    n_bans           INTEGER NOT NULL,
    is_complete      BOOLEAN NOT NULL,     -- 10 picks + 10 bans
    is_bo1           BOOLEAN NOT NULL,     -- excluded from cross-game logic
    fearless_status  VARCHAR CHECK (fearless_status IN ('hard','ambiguous','none')),
    PRIMARY KEY (series_id, game_num)
);

-- Per (team, champion, game) resolution: skipped|taken|denied|banned|burned
CREATE TABLE IF NOT EXISTS champion_game (
    series_id   VARCHAR NOT NULL,
    game_num    INTEGER NOT NULL,
    team        VARCHAR NOT NULL,
    champion    VARCHAR NOT NULL,
    resolution  VARCHAR NOT NULL CHECK (resolution IN
                ('skipped','taken','denied','banned','burned')),
    PRIMARY KEY (series_id, game_num, team, champion)
);
