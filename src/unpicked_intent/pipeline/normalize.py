"""Normalize raw draft events into the draft_events table.

Order of operations (each is load-bearing):

1. Apply undos. The feeds contain 288 ban undos + 173 pick undos. A
   missed undo leaves a phantom ban -> wrong legality -> invented or
   hidden skips. Fold 'undo-*' actions into the stream before anything
   else consumes it.
2. Drop incomplete games. 23 of 1,739 games lack full 10 picks + 10
   bans; set game_flags.is_complete accordingly and exclude from scoring.
3. Flag Bo1 series -> game_flags.is_bo1. They feed the baseline but are
   excluded from cross-game logic.
4. Detect Fearless status per multi-game series:
   - any champion picked twice in the series -> 'none'
   - else -> 'hard' if provable, 'ambiguous' otherwise.
   Gate result: all 569 multi-game series had zero repeats; treat
   ambiguous under the HARD interpretation when reconstructing legality
   (conservative: produces fewer false skips).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass


@dataclass
class NormalizeStats:
    """Summary statistics returned by assign_slots.

    Attributes:
        side_mismatches: Number of events whose acting team's side did not
            match the standard pro draft order for that slot.
        meta_side_disagreements: Number of games where the derived side
            (from draft majority vote across all 20 slots) disagrees with games.csv side.
    """
    side_mismatches: int = 0
    meta_side_disagreements: int = 0


def _phase_from_seq(seq: int) -> str:
    """Map GRID sequenceNumber (1-20) to phase name.

    GRID's sequenceNumber is the draft slot:
        1-6  → ban1 (3 rounds of bans, 2 per round)
        7-12 → pick1 (3 picks per team in round 1)
        13-16 → ban2 (2 rounds of bans)
        17-20 → pick2 (remaining picks)
    """
    if 1 <= seq <= 6:
        return "ban1"
    if 7 <= seq <= 12:
        return "pick1"
    if 13 <= seq <= 16:
        return "ban2"
    if 17 <= seq <= 20:
        return "pick2"
    return "unknown"


# Standard pro draft side order for slots 1-20.
# Confirmed on real data: blue always acts at seq 1.
DRAFT_SIDE_ORDER = [
    # ban1: slots 1-6
    "blue",  # 1
    "red",   # 2
    "blue",  # 3
    "red",   # 4
    "blue",  # 5
    "red",   # 6
    # pick1: slots 7-12
    "blue",  # 7
    "red",   # 8
    "red",   # 9
    "blue",  # 10
    "blue",  # 11
    "red",   # 12
    # ban2: slots 13-16
    "red",   # 13
    "blue",  # 14
    "red",   # 15
    "blue",  # 16
    # pick2: slots 17-20
    "red",   # 17
    "blue",  # 18
    "blue",  # 19
    "red",   # 20
]


def _sort_key(ev: dict):
    """Sort key: (seq or infinity, occurredAt/played_at) so events with seq come first."""
    seq = ev.get("seq")
    if seq is None:
        seq = float("inf")
    return (seq, ev.get("occurredAt") or ev.get("played_at", ""))


def apply_undos(events: list[dict]) -> list[dict]:
    """Remove (action, champion, team, game) pairs matched by later undos.

    An undo event targets the most recent earlier action in the same game
    with the same seq.  If seq is missing on the undo, falls back to
    matching (champion, team).

    Events within each game are ordered by (seq, occurredAt) where seq exists.

    Returns the event list with undos folded in.
    """
    # Group events by game_id (not game_num — game_num repeats across series)
    by_game = defaultdict(list)
    for ev in events:
        by_game[ev["game_id"]].append(ev)

    result = []
    for game_id, game_events in by_game.items():
        # Sort by (seq, occurredAt) — seq from GRID draftActions ensures correct order
        game_events = sorted(game_events, key=_sort_key)
        kept: list[dict] = []
        for ev in game_events:
            action = ev["action"]
            if action in ("undo-ban", "undo-pick"):
                # Find the action to remove: prefer matching by seq, fallback to champion+team
                original_action = "ban" if action == "undo-ban" else "pick"
                undo_seq = ev.get("seq")
                match_idx = None
                for i in range(len(kept) - 1, -1, -1):
                    k = kept[i]
                    if k["action"] != original_action or k["team"] != ev["team"]:
                        continue
                    # Prefer seq-based matching: if undo has seq, match by seq
                    if undo_seq is not None and k.get("seq") == undo_seq:
                        match_idx = i
                        break
                    # Fallback: if undo has no seq, match by champion
                    if undo_seq is None and k["champion"] == ev["champion"]:
                        match_idx = i
                        break
                if match_idx is not None:
                    kept.pop(match_idx)
                # The undo itself is also removed (not appended)
            else:
                kept.append(ev)
        result.extend(kept)

    return result


def assign_slots(events: list[dict]) -> tuple[list[dict], NormalizeStats]:
    """Assign slot and phase to each event using GRID sequenceNumber.

    GRID's sequenceNumber is the draft slot (1-20).  An undo carries the
    same seq as the action it undoes.  Slot = seq, phase = _phase_from_seq(seq).

    Derives each team's side from the draft itself using majority vote across
    all 20 slots: for each team, count how many of its actions match the blue
    side of DRAFT_SIDE_ORDER vs the red side.  The side with the majority wins.
    Stores the games.csv side as meta_side and counts disagreements between
    derived side and meta_side.

    Also checks that the acting team's derived side matches the standard pro
    draft order for that slot and counts mismatches.

    Returns:
        A tuple of (events with slot/phase assigned, NormalizeStats).
    """
    by_game = defaultdict(list)
    for ev in events:
        by_game[ev["game_id"]].append(ev)

    stats = NormalizeStats()
    result = []
    for game_id in sorted(by_game.keys()):
        game_events = sorted(by_game[game_id], key=_sort_key)

        # Derive side from draft using majority vote across all 20 slots.
        # For each team, count how many of its actions fall on blue slots vs
        # red slots in DRAFT_SIDE_ORDER. The side with the majority wins.
        # This handles the edge case where seq 1 is credited to the wrong team.
        team_blue_votes: dict[str, int] = defaultdict(int)
        team_red_votes: dict[str, int] = defaultdict(int)
        teams: set[str] = set()
        for ev in game_events:
            seq = ev.get("seq")
            if seq is not None and 1 <= seq <= len(DRAFT_SIDE_ORDER):
                tid = ev["team"]
                teams.add(tid)
                expected = DRAFT_SIDE_ORDER[seq - 1]
                if expected == "blue":
                    team_blue_votes[tid] += 1
                else:
                    team_red_votes[tid] += 1

        # Build team -> derived side mapping from majority vote
        team_side: dict[str, str] = {}
        meta_side_mismatch = False
        for tid in teams:
            blue_v = team_blue_votes.get(tid, 0)
            red_v = team_red_votes.get(tid, 0)
            team_side[tid] = "blue" if blue_v >= red_v else "red"
            # Check against meta_side from games.csv
            meta_side = None
            for ev in game_events:
                if ev["team"] == tid:
                    meta_side = ev.get("meta_side")
                    break
            if meta_side is not None and team_side[tid] != meta_side:
                meta_side_mismatch = True

        if meta_side_mismatch:
            stats.meta_side_disagreements += 1

        for ev in game_events:
            action = ev["action"]
            if action in ("ban", "pick"):
                seq = ev.get("seq")
                if seq is not None:
                    phase = _phase_from_seq(seq)
                    # Use derived side
                    derived_side = team_side.get(ev["team"], ev.get("side", "blue"))
                    # Check side mismatch against standard draft order
                    if 1 <= seq <= len(DRAFT_SIDE_ORDER):
                        expected_side = DRAFT_SIDE_ORDER[seq - 1]
                        if derived_side != expected_side:
                            stats.side_mismatches += 1
                    ev = dict(ev)
                    ev["side"] = derived_side
                    ev["slot"] = seq
                    ev["phase"] = phase
                    result.append(ev)
                else:
                    # No seq — shouldn't happen after undos are folded, but be safe
                    ev = dict(ev)
                    ev["slot"] = 0
                    ev["phase"] = "unknown"
                    result.append(ev)
            else:
                # Shouldn't happen after apply_undos, but keep it safe
                result.append(ev)

    return result, stats


def check_completeness(events: list[dict]) -> tuple[list[dict], int]:
    """Drop games without exactly 10 bans and 10 picks after undos.

    Returns (kept_events, count_of_dropped_games).
    """
    # Group by game_id (not game_num — game_num repeats across series)
    by_game = defaultdict(list)
    for ev in events:
        by_game[ev["game_id"]].append(ev)

    kept = []
    dropped = 0
    for game_id, game_events in by_game.items():
        bans = sum(1 for e in game_events if e["action"] == "ban")
        picks = sum(1 for e in game_events if e["action"] == "pick")
        if bans == 10 and picks == 10:
            kept.extend(game_events)
        else:
            dropped += 1

    return kept, dropped


def flag_games(events: list[dict], *, series_game_count: dict[str, int] | None = None) -> list[dict]:
    """Compute game_flags rows: completeness, bo1, fearless_status.

    Args:
        events: List of draft events (may be after incomplete games dropped).
        series_game_count: Optional dict mapping series_id -> total game count
            in the series, computed *before* dropping incomplete games.  If
            provided, is_bo1 and fearless_status use this count so that a
            multi-game series doesn't become a fake Bo1 when one game is
            incomplete.  If not provided, falls back to counting games from
            the events list (legacy behavior).

    Returns a list of dicts, one per game, with keys:
        series_id, game_id, game_num, is_complete, is_bo1, fearless_status
    """
    # Group by game_id (not game_num — game_num repeats across series)
    by_game = defaultdict(list)
    for ev in events:
        by_game[ev["game_id"]].append(ev)

    # Determine which series have multiple games (use game_id to count)
    series_games = defaultdict(set)
    for ev in events:
        series_games[ev["series_id"]].add(ev["game_id"])

    flags = []
    for game_id in sorted(by_game.keys()):
        game_events = by_game[game_id]
        series_id = game_events[0]["series_id"]
        game_num = game_events[0]["game_num"]

        bans = sum(1 for e in game_events if e["action"] == "ban")
        picks = sum(1 for e in game_events if e["action"] == "pick")
        is_complete = bans == 10 and picks == 10

        # Use provided series game count (before dropping) if available,
        # otherwise fall back to counting from events
        if series_game_count is not None:
            num_games_in_series = series_game_count.get(series_id, len(series_games[series_id]))
        else:
            num_games_in_series = len(series_games[series_id])
        is_bo1 = num_games_in_series == 1

        # Fearless detection: check for champion repeats across series games
        fearless_status = _detect_fearless(series_id, series_games[series_id], by_game, num_games_in_series)

        flags.append({
            "series_id": series_id,
            "game_id": game_id,
            "game_num": game_num,
            "is_complete": is_complete,
            "is_bo1": is_bo1,
            "fearless_status": fearless_status,
        })

    return flags


def _detect_fearless(
    series_id: str, game_ids: set[str], by_game: dict[str, list[dict]],
    num_games_in_series: int = 0,
) -> str:
    """Detect Fearless draft status for a series.

    - 'none': a champion was picked twice in the series (not Fearless)
    - 'hard': no repeats found in a multi-game series (proven Fearless)
    - 'ambiguous': single-game series (can't determine)

    Args:
        num_games_in_series: Total game count from the series (before dropping
            incomplete games).  Used to determine whether the series is
            multi-game even when some games are missing from by_game.
    """
    # Use the provided count if available, otherwise fall back to by_game keys
    total_games = num_games_in_series if num_games_in_series else len(game_ids)
    if total_games <= 1:
        return "ambiguous"

    # Collect all picked champions across all games in the series
    all_picks = []
    for gid in sorted(game_ids):
        for ev in by_game[gid]:
            if ev["action"] == "pick":
                all_picks.append(ev["champion"])

    if len(all_picks) != len(set(all_picks)):
        return "none"  # repeat found → not Fearless
    return "hard"  # no repeats → Hard Fearless
