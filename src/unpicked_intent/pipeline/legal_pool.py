"""Legal-pool reconstruction and champion-game resolution (Hard Fearless).

Burned set entering game g of a series = union of ALL champions picked in
games 1..g-1 (either team — confirmed hard-rule behavior in the dataset).
Legal pool at any point = all champions minus this-game bans minus burned
minus already-picked-this-game.

Per (team, champion, game) resolution — exactly one of:
    skipped  legal, unbanned, opponent did not take it first, team did not pick
    taken    team picked it
    denied   opponent picked it first
    banned   banned this game by either team
    burned   picked by anyone in a prior game of the series

Only 'skipped' and 'taken' enter the binomial. 'denied' is kept as a
separate scouting signal (pool denial), never counted as a skip.
"""

from __future__ import annotations


def legal_pool(series_drafts: list[dict], game_num: int, at_slot: int) -> set[str]:
    """Champions legal at the given slot, under hard-fearless burn rules."""
    raise NotImplementedError


def resolve_champion_games(series_drafts: list[dict]) -> list[dict]:
    """Emit champion_game rows for a whole series."""
    raise NotImplementedError
