"""Champion -> role priors. Loads static/champion_roles.csv.

That file is hand-sourced game knowledge, deliberately NOT computed from
GRID feeds, so it sits outside the license-derivative question and is the
one committed CSV in the repo.
"""

from __future__ import annotations

import csv
from pathlib import Path

STATIC = Path(__file__).resolve().parents[3] / "static" / "champion_roles.csv"


def load() -> dict[str, str]:
    """Return {champion_name: role} from the committed static table."""
    with STATIC.open(encoding="utf-8", newline="") as f:
        return {r["champion"]: r["role"] for r in csv.DictReader(f)}
