"""Player -> role inference. GENERATED artifact — output goes to derived/.

Method: player role = argmax over the player's champion history weighted
by champion->role priors (a mid plays mid champions). Flex players are
corrected by a hand-maintained override file under derived/ (gitignored).

Required because rosters.csv has player->champion per game but no role
column, and draft events are team-level: mapping a pick to its player
goes through rosters + this role map. Load-bearing — ~1 day of work.
"""

from __future__ import annotations


def infer(champion_history: dict[str, list[str]], priors: dict[str, str]) -> dict[str, str]:
    """Return {player: role}."""
    raise NotImplementedError
