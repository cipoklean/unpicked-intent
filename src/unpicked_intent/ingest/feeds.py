"""Ingest GRID series feeds -> raw draft-event dicts.

Streams one game at a time (uncompressed feeds are 50-150x larger than
what fits in memory). Only draft events and game meta are retained;
everything else is dropped on the floor.

Feed events of interest:
    team-picked-character   -> action=pick
    team-banned-character   -> action=ban
    team-!banned-character  -> ban undo (apply in normalize.py)
    team-!picked-character  -> pick undo
Game meta: patch, league, teams, side, played_at.

Raw feeds live under data/ (gitignored). Nothing in this module may
write to a repo-tracked path.
"""

from __future__ import annotations

import csv
import json
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import zstandard as zstd


@dataclass
class IngestStats:
    """Summary statistics returned by iter_draft_events.

    Attributes:
        skipped_games: Number of events skipped because their game_id was
            not found in games.csv (abandoned or restarted drafts).
        events_read: Total number of draft events yielded.
    """
    skipped_games: int = 0
    events_read: int = 0

# Map GRID event types to our internal action names
ACTION_MAP = {
    "team-picked-character": "pick",
    "team-banned-character": "ban",
    "team-!banned-character": "undo-ban",
    "team-!picked-character": "undo-pick",
}

# Series ID is encoded in the filename: events_<series_id>_grid.jsonl.zst
SERIES_ID_RE = re.compile(r"events_(.+)_grid\.jsonl\.zst$")


def _read_games_meta(games_csv_path: str) -> dict[str, dict]:
    """Load meta/games.csv into a lookup keyed by game_id.

    Columns: series_id, game_id, game_number, patch, team1_id, team1_side,
             team2_id, team2_side
    Returns a dict mapping game_id -> {series_id, game_number, patch,
    team_ids: {team_id: side}}.
    """
    meta = {}
    path = Path(games_csv_path)
    if not path.exists():
        return meta
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            game_id = row["game_id"]
            team_ids = {
                row["team1_id"]: row["team1_side"],
                row["team2_id"]: row["team2_side"],
            }
            meta[game_id] = {
                "series_id": row["series_id"],
                "game_number": int(row["game_number"]),
                "patch": row["patch"],
                "team_ids": team_ids,
            }
    return meta


def iter_draft_events(feed_path: str, games_csv_path: str = "") -> tuple[Iterator[dict], IngestStats]:
    """Yield normalized draft-action dicts from a series feed file.

    Contract per yielded dict:
        series_id, game_id, game_num, slot, phase ('ban1'|'pick1'|'ban2'|'pick2'),
        side ('blue'|'red'), team, action ('ban'|'pick'|'undo-ban'|'undo-pick'),
        champion, patch, league, played_at, seq (optional sequence number)

    Undo events are yielded as actions so normalize.py can fold them in
    order — do NOT filter them here.

    The feed file is zstd-compressed JSONL.  Each line is an envelope with
    "occurredAt" and a list "events".  Only events whose type appears in
    ACTION_MAP are emitted.  Patch, game_num, and side are resolved from
    meta/games.csv (path passed as games_csv_path).

    If a game_id is not found in games.csv, the event is skipped (abandoned
    or restarted draft) and counted in IngestStats.skipped_games.  The rest
    of the series continues normally.

    Returns:
        A tuple of (iterator over draft-event dicts, IngestStats).
    """
    # Extract series_id from filename
    feed_name = Path(feed_path).name
    m = SERIES_ID_RE.match(feed_name)
    if not m:
        raise ValueError(f"Feed filename does not match expected pattern: {feed_name}")
    series_id = m.group(1)

    # Load games metadata
    games_meta = _read_games_meta(games_csv_path)

    stats = IngestStats()
    skipped_game_ids = set()

    def _event_generator():
        nonlocal stats
        # Stream the zstd-compressed file line by line (true streaming, no full read)
        with zstd.open(feed_path, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                envelope = json.loads(line)
                events = envelope.get("events", [])
                # Skip envelopes with no draft events before parsing anything else
                has_draft = any(ev.get("type", "") in ACTION_MAP for ev in events)
                if not has_draft:
                    continue

                for ev in events:
                    ev_type = ev.get("type", "")
                    if ev_type not in ACTION_MAP:
                        continue

                    team_id = ev["actor"]["id"]
                    champion = ev["target"]["state"]["name"]
                    action = ACTION_MAP[ev_type]

                    # Resolve game context from seriesStateDelta (real GRID shape)
                    ssd = ev.get("seriesStateDelta", {})
                    games_list = ssd.get("games", [])
                    if not games_list:
                        continue
                    game_id = games_list[0].get("id", "")
                    if not game_id:
                        continue

                    # Extract sequence number from draftActions (may be missing on undos)
                    seq = None
                    draft_actions = games_list[0].get("draftActions", [])
                    if draft_actions:
                        seq = draft_actions[0].get("sequenceNumber")

                    # Look up metadata — skip if game_id not found (abandoned draft)
                    meta = games_meta.get(game_id)
                    if meta is None:
                        if game_id not in skipped_game_ids:
                            skipped_game_ids.add(game_id)
                            stats.skipped_games += 1
                        continue

                    # Look up side — skip if team not found for this game
                    side = meta["team_ids"].get(team_id)
                    if side is None:
                        continue

                    patch = meta["patch"]
                    game_number = meta["game_number"]

                    stats.events_read += 1
                    yield {
                        "series_id": series_id,
                        "game_id": game_id,
                        "game_num": game_number,
                        "slot": 0,  # assigned in normalize.py
                        "phase": "",  # assigned in normalize.py
                        "side": side,
                        "team": team_id,
                        "action": action,
                        "champion": champion,
                        "patch": patch,
                        "league": "",  # not in games.csv; could come from feed metadata
                        "played_at": envelope.get("occurredAt", ""),
                        "seq": seq,
                    }

    return _event_generator(), stats
