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

from collections.abc import Iterator


def iter_draft_events(feed_path: str) -> Iterator[dict]:
    """Yield normalized draft-action dicts from a series feed file.

    Contract per yielded dict:
        series_id, game_num, slot, phase ('ban1'|'pick1'|'ban2'|'pick2'),
        side ('blue'|'red'), team, action ('ban'|'pick'|'undo-ban'|'undo-pick'),
        champion, patch, league, played_at

    Undo events are yielded as actions so normalize.py can fold them in
    order — do NOT filter them here.
    """
    raise NotImplementedError("gate complete; implement against GRID file-download schema")
