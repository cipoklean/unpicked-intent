"""Expected-behavior baseline — the fat layer.

Keyed on (3-played-patch window x role), pooled across ALL regions
(1,563 games). Region stays out of the key: LCS-only in a window is the
same thin data we escaped; LCS rates are computed for display context
only.

'Played patch' means a patch with actual games — 16.5, 16.12, 16.13 have
none (competitive skipped them). Build windows over the ordered list of
played patches, not patch ordinals.

Outputs:
    meta_relevant(champion, role, window) -> bool
    expected_pick_rate(champion, role, window) -> float   # binomial p
    mean_ban_position(champion, window) -> float | None   # None if too few bans
"""

from __future__ import annotations


def played_patch_windows(games: list[dict], width: int = 3) -> dict[str, list[str]]:
    """Map each patch -> the window of the `width` most recent played patches."""
    raise NotImplementedError


def expected_pick_rate(champion: str, role: str, window: list[str], events) -> float:
    raise NotImplementedError
