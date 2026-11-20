"""Helper to create synthetic zstd-compressed JSONL fixtures for testing."""

import json
from pathlib import Path

import zstandard as zstd

FIXTURES = Path(__file__).parent


def _make_event(ev_type, team_id, champion, game_id, seq=None):
    """Build a GRID-style event dict with seriesStateDelta shape."""
    event = {
        "type": ev_type,
        "actor": {"id": team_id},
        "target": {"state": {"name": champion}},
        "seriesStateDelta": {
            "games": [
                {
                    "id": game_id,
                    "draftActions": (
                        [{"sequenceNumber": seq}] if seq is not None else []
                    ),
                }
            ]
        },
    }
    # Include a dummy seriesState object to match real feeds (ignored by ingest)
    event["seriesState"] = {"dummy": "large object ignored by ingest"}
    return event


def create_synthetic_feed():
    """Create a synthetic events_<series_id>_grid.jsonl.zst file with undos.

    Contains 2 games:
    - Game 1: complete draft with 1 ban undo and 1 pick undo (after folding: 10 bans, 10 picks)
    - Game 2: incomplete draft (only 5 bans, 5 picks) — should be dropped
    """
    # Game 1 events: we need 10 bans + 10 picks after undos.
    # Sequence numbers follow the draft order for proper sorting.
    # Undo events may have no sequenceNumber (as in real feeds).
    game1_events = [
        # Slot 1: ban/blue — T1 bans Kalista
        _make_event("team-banned-character", "T1", "Kalista", "G1", seq=1),
        # Undo: T1 undoes Kalista ban (no seq on undo, as in real feeds)
        _make_event("team-!banned-character", "T1", "Kalista", "G1"),
        # Slot 2: ban/red — T2 bans K'Sante
        _make_event("team-banned-character", "T2", "K'Sante", "G1", seq=2),
        # Slot 3: pick/blue — T1 picks Garen
        _make_event("team-picked-character", "T1", "Garen", "G1", seq=3),
        # Slot 4: pick/red — T2 picks Darius
        _make_event("team-picked-character", "T2", "Darius", "G1", seq=4),
        # Slot 5: pick/red — T2 picks Lee Sin (will be undone)
        _make_event("team-picked-character", "T2", "Lee Sin", "G1", seq=5),
        # Undo: T2 undoes Lee Sin pick (no seq on undo)
        _make_event("team-!picked-character", "T2", "Lee Sin", "G1"),
        # Slot 6: ban/blue — T1 bans Azir (replacement for Kalista)
        _make_event("team-banned-character", "T1", "Azir", "G1", seq=6),
        # Slot 7: ban/red — T2 bans Viktor
        _make_event("team-banned-character", "T2", "Viktor", "G1", seq=7),
        # Slot 8: pick/blue — T1 picks Malphite
        _make_event("team-picked-character", "T1", "Malphite", "G1", seq=8),
        # Slot 9: pick/blue — T1 picks Thresh
        _make_event("team-picked-character", "T1", "Thresh", "G1", seq=9),
        # Slot 10: pick/red — T2 picks Graves (replacement for Lee Sin)
        _make_event("team-picked-character", "T2", "Graves", "G1", seq=10),
        # Slot 11: ban/blue — T1 bans Orianna
        _make_event("team-banned-character", "T1", "Orianna", "G1", seq=11),
        # Slot 12: ban/red — T2 bans Syndra
        _make_event("team-banned-character", "T2", "Syndra", "G1", seq=12),
        # Slot 13: pick/blue — T1 picks Caitlyn
        _make_event("team-picked-character", "T1", "Caitlyn", "G1", seq=13),
        # Slot 14: pick/red — T2 picks Nautilus
        _make_event("team-picked-character", "T2", "Nautilus", "G1", seq=14),
        # Slot 15: pick/red — T2 picks Jinx
        _make_event("team-picked-character", "T2", "Jinx", "G1", seq=15),
        # Slot 16: ban/blue — T1 bans LeBlanc
        _make_event("team-banned-character", "T1", "LeBlanc", "G1", seq=16),
        # Slot 17: ban/red — T2 bans Ahri
        _make_event("team-banned-character", "T2", "Ahri", "G1", seq=17),
        # Slot 18: pick/blue — T1 picks Warwick
        _make_event("team-picked-character", "T1", "Warwick", "G1", seq=18),
        # Slot 19: pick/red — T2 picks Soraka
        _make_event("team-picked-character", "T2", "Soraka", "G1", seq=19),
        # Slot 20: ban/blue — T1 bans Yasuo
        _make_event("team-banned-character", "T1", "Yasuo", "G1", seq=20),
        # Slot 21: ban/red — T2 bans Yone
        _make_event("team-banned-character", "T2", "Yone", "G1", seq=21),
        # Slot 24: ban/blue — T1 bans Zed
        _make_event("team-banned-character", "T1", "Zed", "G1", seq=22),
    ]
    # After folding: 10 bans (K'Sante, Azir, Viktor, Orianna, Syndra, LeBlanc, Ahri, Yasuo, Yone, Zed)
    # After folding: 10 picks (Garen, Darius, Malphite, Thresh, Graves, Caitlyn, Nautilus, Jinx, Warwick, Soraka)

    # Game 2: incomplete — only 5 bans, 5 picks
    # Use champions NOT used in Game 1 so Fearless detection works correctly
    game2_events = [
        _make_event("team-banned-character", "T1", "Ryze", "G2", seq=1),
        _make_event("team-banned-character", "T2", "Sion", "G2", seq=2),
        _make_event("team-banned-character", "T1", "Sivir", "G2", seq=3),
        _make_event("team-banned-character", "T2", "Annie", "G2", seq=4),
        _make_event("team-banned-character", "T1", "Nunu & Willump", "G2", seq=5),
        _make_event("team-picked-character", "T1", "Aatrox", "G2", seq=6),
        _make_event("team-picked-character", "T2", "Camille", "G2", seq=7),
        _make_event("team-picked-character", "T2", "Hecarim", "G2", seq=8),
        _make_event("team-picked-character", "T1", "Fiora", "G2", seq=9),
        _make_event("team-picked-character", "T1", "Braum", "G2", seq=10),
    ]

    envelopes = [
        {"occurredAt": "2026-11-01T10:00:00Z", "events": game1_events},
        {"occurredAt": "2026-11-01T12:00:00Z", "events": game2_events},
    ]

    feed_path = FIXTURES / "events_SYN-2_grid.jsonl.zst"
    # Build JSONL text, then compress
    jsonl_text = "\n".join(json.dumps(env) for env in envelopes) + "\n"
    compressed = zstd.compress(jsonl_text.encode("utf-8"))
    feed_path.write_bytes(compressed)

    # Create games.csv
    games_csv = FIXTURES / "games.csv"
    games_csv.write_text(
        "series_id,game_id,game_number,patch,team1_id,team1_side,team2_id,team2_side\n"
        "SYN-2,G1,1,16.20,T1,blue,T2,red\n"
        "SYN-2,G2,2,16.20,T1,blue,T2,red\n"
    )

    print(f"Created {feed_path} and {games_csv}")


if __name__ == "__main__":
    create_synthetic_feed()
