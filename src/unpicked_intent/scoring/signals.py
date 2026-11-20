"""Observed-behavior signals — the thin layer.

Unit of evidence = ONE GAME where the champion stayed legal through that
player's pick. Never count slots: within one draft a champion is legal at
all five pick slots and those are not independent chances.

    left-open:  champion legal in k games, taken t
                significance = binomial(t | k, expected_pick_rate)
    early-ban:  mean ban position vs. meta-average (min ban count applies)
    fit:        candidate pool = player's pool ∩ meta-relevant champions;
                rank left-open tells by how strongly the flag predicted a pick

Honesty rules:
    - rank tells by p-value
    - only emit tells with n >= 4 qualifying games (MIN_N below)
    - every emitted tell carries n and expected-vs-observed
"""

from __future__ import annotations

MIN_N = 4


def left_open(observed: list[dict], baseline) -> list[dict]:
    """Return ranked tells: {champion, k_legal, t_taken, expected, p_value}.

    Significance via scipy.stats.binomtest — add scipy to deps when
    implementing.
    """
    raise NotImplementedError


def early_ban_pressure(observed: list[dict], baseline) -> list[dict]:
    raise NotImplementedError
