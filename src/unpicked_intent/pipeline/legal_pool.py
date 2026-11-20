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

from collections import defaultdict


def _get_champion_universe(champion_roles: dict[str, str]) -> set[str]:
    """Return the set of all champion names from the roles table."""
    return set(champion_roles.keys())


def _burned_champions(
    series_drafts: list[dict],
    game_num: int,
    is_bo1: bool,
) -> set[str]:
    """Compute the burned set entering game *game_num* of a series.

    burned(g) = every champion picked by either team in earlier games
    (game_num < g) of the same series.

    For Bo1 series, the burned set is always empty.
    """
    if is_bo1:
        return set()

    burned: set[str] = set()
    for ev in series_drafts:
        if ev["game_num"] < game_num and ev["action"] == "pick":
            burned.add(ev["champion"])
    return burned


def legal_pool(
    series_drafts: list[dict],
    champion_roles: dict[str, str],
    game_num: int,
    at_slot: int,
    is_bo1: bool = False,
) -> set[str]:
    """Champions legal at the given slot, under hard-fearless burn rules.

    Args:
        series_drafts: All draft events for the series (after undo folding
            and slot assignment), sorted by game_num then slot.
        champion_roles: The full champion→role mapping (the universe).
        game_num: The 1-based game number within the series.
        at_slot: The 1-based draft slot (1–20) within the game.
        is_bo1: Whether this series is a Bo1 (no burned set).

    Returns:
        Set of champion names that are legal to pick at this point.
    """
    universe = _get_champion_universe(champion_roles)
    burned = _burned_champions(series_drafts, game_num, is_bo1)

    # Collect bans and picks *so far* in this game (before the current slot)
    bans_this_game: set[str] = set()
    picks_this_game: set[str] = set()
    for ev in series_drafts:
        if ev["game_num"] != game_num:
            continue
        if ev.get("slot", 0) < at_slot:
            if ev["action"] == "ban":
                bans_this_game.add(ev["champion"])
            elif ev["action"] == "pick":
                picks_this_game.add(ev["champion"])

    return universe - burned - bans_this_game - picks_this_game


def resolve_champion_games(
    series_drafts: list[dict],
    champion_roles: dict[str, str],
    game_flags: list[dict] | None = None,
) -> list[dict]:
    """Emit champion_game rows for a whole series.

    For each complete game and each team, emits exactly one row per champion
    in the universe with the resolved status.

    Priority order (highest first):
        burned > banned > taken > denied > skipped

    Args:
        series_drafts: All draft events for the series (after undo folding
            and slot assignment). Should contain only complete games.
        champion_roles: The full champion→role mapping (the universe).
        game_flags: Optional list of game flag dicts from flag_games().
            If provided, is_bo1 is read from there to determine burned sets.

    Returns:
        A list of dicts, one per (game_id, team, champion), with keys:
        series_id, game_id, game_num, team, side, patch, champion, status.
    """
    universe = _get_champion_universe(champion_roles)

    # Group events by game_id
    by_game: dict[str, list[dict]] = defaultdict(list)
    for ev in series_drafts:
        by_game[ev["game_id"]].append(ev)

    # Build a lookup for game_flags
    flags_by_game: dict[str, dict] = {}
    if game_flags:
        for f in game_flags:
            flags_by_game[f["game_id"]] = f

    # Determine which series are Bo1
    series_ids: set[str] = set()
    for ev in series_drafts:
        series_ids.add(ev["series_id"])

    series_is_bo1: dict[str, bool] = {}
    for sid in series_ids:
        flag = next((f for f in (game_flags or []) if f["series_id"] == sid), None)
        if flag:
            series_is_bo1[sid] = flag["is_bo1"]
        else:
            # Fallback: count games in series_drafts for this series
            series_game_ids = {ev["game_id"] for ev in series_drafts if ev["series_id"] == sid}
            series_is_bo1[sid] = len(series_game_ids) <= 1

    # Sort games within each series by game_num for burned set calculation
    series_ids_in_drafts: set[str] = set()
    for ev in series_drafts:
        series_ids_in_drafts.add(ev["series_id"])

    rows: list[dict] = []

    for series_id in sorted(series_ids_in_drafts):
        is_bo1 = series_is_bo1.get(series_id, False)

        # Get all games in this series, sorted by game_num
        series_game_ids = sorted(
            {ev["game_id"] for ev in series_drafts if ev["series_id"] == series_id},
            key=lambda gid: next(ev["game_num"] for ev in by_game[gid]),
        )

        for game_id in series_game_ids:
            game_events = by_game[game_id]
            game_num = game_events[0]["game_num"]
            patch = game_events[0].get("patch", "")

            # Compute burned set for this game
            burned = _burned_champions(
                [ev for ev in series_drafts if ev["series_id"] == series_id],
                game_num,
                is_bo1,
            )

            # Collect bans (either team) and picks (by team) in this game
            bans_this_game: set[str] = set()
            picks_by_team: dict[str, set[str]] = defaultdict(set)
            teams_in_game: dict[str, str] = {}  # team -> side

            for ev in game_events:
                if ev["action"] == "ban":
                    bans_this_game.add(ev["champion"])
                elif ev["action"] == "pick":
                    picks_by_team[ev["team"]].add(ev["champion"])
                teams_in_game[ev["team"]] = ev.get("side", "blue")

            teams = sorted(teams_in_game.keys())

            for team in teams:
                side = teams_in_game[team]
                opponent_picks = set()
                for other_team in teams:
                    if other_team != team:
                        opponent_picks |= picks_by_team[other_team]

                team_picks = picks_by_team.get(team, set())

                for champion in sorted(universe):
                    # Determine status with priority: burned > banned > taken > denied > skipped
                    if champion in burned:
                        status = "burned"
                    elif champion in bans_this_game:
                        status = "banned"
                    elif champion in team_picks:
                        status = "taken"
                    elif champion in opponent_picks:
                        status = "denied"
                    else:
                        status = "skipped"

                    rows.append({
                        "series_id": series_id,
                        "game_id": game_id,
                        "game_num": game_num,
                        "team": team,
                        "side": side,
                        "patch": patch,
                        "champion": champion,
                        "status": status,
                    })

    return rows
