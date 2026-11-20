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


def apply_undos(events: list[dict]) -> list[dict]:
    """Remove (action, champion, team, game) pairs matched by later undos."""
    raise NotImplementedError


def flag_games(events: list[dict]) -> list[dict]:
    """Compute game_flags rows: completeness, bo1, fearless_status."""
    raise NotImplementedError
