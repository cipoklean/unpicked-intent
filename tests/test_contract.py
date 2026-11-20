"""Contract tests on synthetic fixtures — never run against feed data."""
import json
from pathlib import Path

FIXTURE = Path(__file__).parent / "fixtures" / "synthetic_draft.jsonl"


def test_fixture_conforms_to_draft_event_contract():
    rows = [json.loads(l) for l in FIXTURE.read_text().splitlines() if l.strip()]
    required = {"series_id", "game_num", "slot", "phase", "side",
                "team", "action", "champion", "patch", "league"}
    for r in rows:
        assert required <= r.keys()
        assert r["phase"] in {"ban1", "pick1", "ban2", "pick2"}
        assert r["side"] in {"blue", "red"}
        assert r["action"] in {"ban", "pick", "undo-ban", "undo-pick"}


def test_hard_fearless_no_repeat_picks():
    rows = [json.loads(l) for l in FIXTURE.read_text().splitlines() if l.strip()]
    picks = [r["champion"] for r in rows if r["action"] == "pick"]
    assert len(picks) == len(set(picks))
