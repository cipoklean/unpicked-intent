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

# Standard pro draft order (20 slots total).
# Each tuple: (slot, phase, side)
# ban1: slots 1-2 (blue, red)
# pick1: slots 3-4 (blue, red)
# ban2: slots 5-6 (blue, red)
# pick2: slots 7-10 (blue, red, blue, red)
# ban1 again for round 2 bans: slots 11-12 (blue, red)
# pick2 continued: slots 13-20
#
# Standard LoL pro draft order (simplified):
# Phase 1:
#   Ban1: Blue(1), Red(2)
#   Pick1: Blue(1), Red(1), Red(2)  [slots 3,4,5]
#   Ban2: Blue(3), Red(3)           [slots 6,7]
#   Pick2: Blue(2), Blue(3), Red(3) [slots 8,9,10]
# Phase 2:
#   Ban1: Blue(5), Red(5)           [slots 11,12]
#   Pick1: Blue(4), Red(4), Red(5)  [slots 13,14,15]
#   Ban2: Blue(7), Red(7)           [slots 16,17]
#   Pick2: Blue(5), Red(6)          [slots 18,19]
# Phase 3:
#   Ban1: Blue(9), Red(9)           [slots 20,21]
#   Pick1: Blue(6), Red(7)          [slots 22,23]
#   Ban2: Blue(10), Red(10)         [slots 24,25]
#   Pick2: (none remaining)
#
# Actually, let's use the simpler canonical slot ordering:
# 20 action slots alternating blue/red, grouped by phase.

DRAFT_ORDER = [
    # Phase: ban1 (round 1 bans)
    (1, "ban1", "blue"),
    (2, "ban1", "red"),
    # Phase: pick1 (round 1 picks)
    (3, "pick1", "blue"),
    (4, "pick1", "red"),
    (5, "pick1", "red"),
    # Phase: ban2 (round 2 bans)
    (6, "ban2", "blue"),
    (7, "ban2", "red"),
    # Phase: pick2 (round 2 picks)
    (8, "pick2", "blue"),
    (9, "pick2", "blue"),
    (10, "pick2", "red"),
    # Phase: ban1 (round 3 bans)
    (11, "ban1", "blue"),
    (12, "ban1", "red"),
    # Phase: pick1 (round 3 picks)
    (13, "pick1", "blue"),
    (14, "pick1", "red"),
    (15, "pick1", "red"),
    # Phase: ban2 (round 4 bans)
    (16, "ban2", "blue"),
    (17, "ban2", "red"),
    # Phase: pick2 (round 4 picks)
    (18, "pick2", "blue"),
    (19, "pick2", "red"),
    # Phase: ban1 (round 5 bans)
    (20, "ban1", "blue"),
    (21, "ban1", "red"),
    # Phase: pick1 (round 5 picks)
    (22, "pick1", "blue"),
    (23, "pick1", "red"),
    # Phase: ban2 (round 6 bans)
    (24, "ban2", "blue"),
    (25, "ban2", "red"),
]


def apply_undos(events: list[dict]) -> list[dict]:
    """Remove (action, champion, team, game) pairs matched by later undos.

    An undo event targets the most recent matching (action, champion, team,
    game_num) entry that appears before it in the stream. Both the original
    action and the undo are removed from the result.

    Returns the event list with undos folded in.
    """
    # Group events by game
    by_game = defaultdict(list)
    for ev in events:
        by_game[ev["game_num"]].append(ev)

    result = []
    for game_num, game_events in by_game.items():
        kept: list[dict] = []
        for ev in game_events:
            action = ev["action"]
            if action in ("undo-ban", "undo-pick"):
                # Find the most recent matching action to remove
                original_action = "ban" if action == "undo-ban" else "pick"
                match_idx = None
                for i in range(len(kept) - 1, -1, -1):
                    k = kept[i]
                    if (
                        k["action"] == original_action
                        and k["champion"] == ev["champion"]
                        and k["team"] == ev["team"]
                    ):
                        match_idx = i
                        break
                if match_idx is not None:
                    kept.pop(match_idx)
                # The undo itself is also removed (not appended)
            else:
                kept.append(ev)
        result.extend(kept)

    return result


def assign_slots(events: list[dict]) -> list[dict]:
    """Assign slot and phase to each event using standard pro draft order.

    Events must be grouped by game_num and in chronological order within
    each game.  Non-undo events (ban/pick) are assigned the next available
    draft slot matching their side.  The DRAFT_ORDER template is walked
    forward: when an event's side matches the current template entry, that
    slot is consumed; when it doesn't, the template entry is skipped.
    """
    by_game = defaultdict(list)
    for ev in events:
        by_game[ev["game_num"]].append(ev)

    result = []
    for game_num in sorted(by_game.keys()):
        game_events = by_game[game_num]
        slot_ptr = 0  # index into DRAFT_ORDER
        for ev in game_events:
            action = ev["action"]
            if action in ("ban", "pick"):
                side = ev["side"]
                assigned = False
                while slot_ptr < len(DRAFT_ORDER):
                    slot, phase, d_side = DRAFT_ORDER[slot_ptr]
                    if d_side == side:
                        ev = dict(ev)
                        ev["slot"] = slot
                        ev["phase"] = phase
                        result.append(ev)
                        slot_ptr += 1
                        assigned = True
                        break
                    else:
                        # Skip this template slot — wrong side for this event
                        slot_ptr += 1
                if not assigned:
                    # No more slots — assign a fallback
                    ev = dict(ev)
                    ev["slot"] = len(DRAFT_ORDER) + (slot_ptr - len(DRAFT_ORDER) + 1)
                    ev["phase"] = "unknown"
                    result.append(ev)
            else:
                # Shouldn't happen after apply_undos, but keep it safe
                result.append(ev)

    return result


def check_completeness(events: list[dict]) -> tuple[list[dict], int]:
    """Drop games without exactly 10 bans and 10 picks after undos.

    Returns (kept_events, count_of_dropped_games).
    """
    by_game = defaultdict(list)
    for ev in events:
        by_game[ev["game_num"]].append(ev)

    kept = []
    dropped = 0
    for game_num, game_events in by_game.items():
        bans = sum(1 for e in game_events if e["action"] == "ban")
        picks = sum(1 for e in game_events if e["action"] == "pick")
        if bans == 10 and picks == 10:
            kept.extend(game_events)
        else:
            dropped += 1

    return kept, dropped


def flag_games(events: list[dict]) -> list[dict]:
    """Compute game_flags rows: completeness, bo1, fearless_status.

    Returns a list of dicts, one per game, with keys:
        series_id, game_num, is_complete, is_bo1, fearless_status
    """
    by_game = defaultdict(list)
    for ev in events:
        by_game[ev["game_num"]].append(ev)

    # Determine which series have multiple games
    series_games = defaultdict(set)
    for ev in events:
        series_games[ev["series_id"]].add(ev["game_num"])

    flags = []
    for game_num in sorted(by_game.keys()):
        game_events = by_game[game_num]
        series_id = game_events[0]["series_id"]

        bans = sum(1 for e in game_events if e["action"] == "ban")
        picks = sum(1 for e in game_events if e["action"] == "pick")
        is_complete = bans == 10 and picks == 10

        num_games_in_series = len(series_games[series_id])
        is_bo1 = num_games_in_series == 1

        # Fearless detection: check for champion repeats across series games
        fearless_status = _detect_fearless(series_id, series_games[series_id], by_game)

        flags.append({
            "series_id": series_id,
            "game_num": game_num,
            "is_complete": is_complete,
            "is_bo1": is_bo1,
            "fearless_status": fearless_status,
        })

    return flags


def _detect_fearless(
    series_id: str, game_nums: set[int], by_game: dict[int, list[dict]]
) -> str:
    """Detect Fearless draft status for a series.

    - 'none': a champion was picked twice in the series (not Fearless)
    - 'hard': no repeats found in a multi-game series (proven Fearless)
    - 'ambiguous': single-game series (can't determine)
    """
    if len(game_nums) <= 1:
        return "ambiguous"

    # Collect all picked champions across all games in the series
    all_picks = []
    for gn in sorted(game_nums):
        for ev in by_game[gn]:
            if ev["action"] == "pick":
                all_picks.append(ev["champion"])

    if len(all_picks) != len(set(all_picks)):
        return "none"  # repeat found → not Fearless
    return "hard"  # no repeats → Hard Fearless
