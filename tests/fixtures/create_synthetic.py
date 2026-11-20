"""Helper to create synthetic zstd-compressed JSONL fixtures for testing.

Creates two feed files:
1. events_SYN-2_grid.jsonl.zst — two games in one series (complete + incomplete).
2. events_SYN-3_grid.jsonl.zst — a second series whose game_number is also 1,
   plus an abandoned-draft game (G_ABANDONED) not in games.csv.

Shared games.csv covers both series so the tests can verify that grouping by
game_id (not game_num) keeps the two game_num=1 games separate.
"""

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


def _make_complete_game_events(game_id, team_blue, team_red, champions_bans, champions_picks):
    """Build a complete 20-slot draft for the given game.

    Args:
        game_id: unique game identifier
        team_blue: team ID on blue side (acts at seq 1)
        team_red: team ID on red side
        champions_bans: list of 10 champion names for bans
        champions_picks: list of 10 champion names for picks
    """
    events = []
    ban_idx = 0
    pick_idx = 0
    seq = 1

    # Standard draft order: ban1(1-6), pick1(7-12), ban2(13-16), pick2(17-20)
    # ban1: blue, red, blue, red, blue, red (6 bans)
    for side in ["blue", "red", "blue", "red", "blue", "red"]:
        team = team_blue if side == "blue" else team_red
        events.append(_make_event("team-banned-character", team, champions_bans[ban_idx], game_id, seq=seq))
        ban_idx += 1
        seq += 1

    # pick1: blue, red, red, blue, blue, red (6 picks)
    for side in ["blue", "red", "red", "blue", "blue", "red"]:
        team = team_blue if side == "blue" else team_red
        events.append(_make_event("team-picked-character", team, champions_picks[pick_idx], game_id, seq=seq))
        pick_idx += 1
        seq += 1

    # ban2: red, blue, red, blue (4 bans) — corrected order
    for side in ["red", "blue", "red", "blue"]:
        team = team_blue if side == "blue" else team_red
        events.append(_make_event("team-banned-character", team, champions_bans[ban_idx], game_id, seq=seq))
        ban_idx += 1
        seq += 1

    # pick2: red, blue, blue, red (4 picks) — corrected order
    for side in ["red", "blue", "blue", "red"]:
        team = team_blue if side == "blue" else team_red
        events.append(_make_event("team-picked-character", team, champions_picks[pick_idx], game_id, seq=seq))
        pick_idx += 1
        seq += 1

    return events


def create_synthetic_feed():
    """Create synthetic feed files and shared games.csv for testing.

    SYN-2: two games (G1 complete with undos, G2 incomplete).
    SYN-3: one game (G3 complete, game_num=1 same as G1) + abandoned draft (G_ABANDONED).
    """

    # ---- SYN-2: Game 1 (G1) — complete draft with 1 ban undo + 1 pick undo ----
    # Build a clean 20-slot draft, then prepend undo events.
    g1_bans = ["K'Sante", "Azir", "Viktor", "Orianna", "Syndra", "LeBlanc", "Ahri", "Yasuo", "Yone", "Zed"]
    g1_picks = ["Garen", "Darius", "Malphite", "Thresh", "Graves", "Caitlyn", "Nautilus", "Jinx", "Warwick", "Soraka"]

    game1_events = _make_complete_game_events("G1", "T1", "T2", g1_bans, g1_picks)
    # Prepend: T1 bans Kalista (seq=1), then undoes it (no seq on undo).
    # The Kalista ban will be folded away by apply_undos via champion matching.
    game1_events.insert(0, _make_event("team-!banned-character", "T1", "Kalista", "G1"))
    game1_events.insert(0, _make_event("team-banned-character", "T1", "Kalista", "G1", seq=1))

    # Insert a pick undo: find the Lee Sin pick in the clean draft and add undo.
    # Actually, the clean draft uses g1_picks which doesn't include Lee Sin.
    # We need a pick that exists in the draft. Let's undo the first pick (Garen at seq=7).
    # Insert undo-pick for Garen right after the Garen pick event.
    # Garen is at index: after 2 prepended events + 6 ban events = index 8 (seq=7)
    # Find the Garen pick and insert undo after it.
    garen_idx = None
    for i, ev in enumerate(game1_events):
        if ev["type"] == "team-picked-character" and ev["target"]["state"]["name"] == "Garen":
            garen_idx = i
            break
    if garen_idx is not None:
        # Insert undo after Garen pick, then replace Garen with Lee Sin
        game1_events.insert(garen_idx + 1, _make_event("team-!picked-character", "T1", "Garen", "G1"))
        # Replace Garen with Lee Sin (the replacement pick)
        game1_events.insert(garen_idx + 2, _make_event("team-picked-character", "T1", "Lee Sin", "G1", seq=7))

    # After folding: Kalista ban removed, Garen pick removed, Lee Sin remains.
    # 10 bans + 10 picks. Good.

    # ---- SYN-2: Game 2 (G2) — incomplete draft (5 bans + 5 picks) ----
    # Uses correct draft side order: ban1 blue,red,blue,red,blue (seq 1-5)
    # pick1 blue,red,red,blue,blue (seq 7-11)
    game2_events = [
        _make_event("team-banned-character", "T1", "Ryze", "G2", seq=1),
        _make_event("team-banned-character", "T2", "Sion", "G2", seq=2),
        _make_event("team-banned-character", "T1", "Sivir", "G2", seq=3),
        _make_event("team-banned-character", "T2", "Annie", "G2", seq=4),
        _make_event("team-banned-character", "T1", "Nunu & Willump", "G2", seq=5),
        _make_event("team-picked-character", "T1", "Aatrox", "G2", seq=7),
        _make_event("team-picked-character", "T2", "Camille", "G2", seq=8),
        _make_event("team-picked-character", "T2", "Hecarim", "G2", seq=9),
        _make_event("team-picked-character", "T1", "Fiora", "G2", seq=10),
        _make_event("team-picked-character", "T1", "Braum", "G2", seq=11),
    ]

    # ---- SYN-3: Game 4 (G4) — complete draft, sides swapped in games.csv ----
    # T3 acts at seq 1 (so T3 is blue by draft), but games.csv says T3=red, T4=blue
    g4_bans = ["Ryze", "Sion", "Sivir", "Annie", "Nunu & Willump", "Aatrox", "Camille", "Hecarim", "Fiora", "Braum"]
    g4_picks = ["Kalista", "K'Sante", "Azir", "Viktor", "Orianna", "Syndra", "LeBlanc", "Ahri", "Yasuo", "Zed"]
    game4_events = _make_complete_game_events("G4", "T3", "T4", g4_bans, g4_picks)

    # ---- SYN-3: Game 3 (G3) — complete draft, game_num=1 (same as G1) ----
    g3_bans = ["Ryze", "Sion", "Sivir", "Annie", "Nunu & Willump", "Aatrox", "Camille", "Hecarim", "Fiora", "Braum"]
    g3_picks = ["Kalista", "K'Sante", "Azir", "Viktor", "Orianna", "Syndra", "LeBlanc", "Ahri", "Yasuo", "Zed"]
    game3_events = _make_complete_game_events("G3", "T3", "T4", g3_bans, g3_picks)

    # ---- SYN-3: Abandoned draft (G_ABANDONED) — not in games.csv ----
    abandoned_events = [
        _make_event("team-banned-character", "T3", "Garen", "G_ABANDONED", seq=1),
        _make_event("team-banned-character", "T4", "Darius", "G_ABANDONED", seq=2),
    ]

    # ---- Write SYN-2 feed ----
    syn2_envelopes = [
        {"occurredAt": "2026-11-01T10:00:00Z", "events": game1_events},
        {"occurredAt": "2026-11-01T12:00:00Z", "events": game2_events},
    ]
    feed_path = FIXTURES / "events_SYN-2_grid.jsonl.zst"
    jsonl_text = "\n".join(json.dumps(env) for env in syn2_envelopes) + "\n"
    compressed = zstd.compress(jsonl_text.encode("utf-8"))
    feed_path.write_bytes(compressed)

    # ---- Write SYN-3 feed ----
    syn3_envelopes = [
        {"occurredAt": "2026-11-02T10:00:00Z", "events": game3_events},
        {"occurredAt": "2026-11-02T10:30:00Z", "events": game4_events},
        {"occurredAt": "2026-11-02T11:00:00Z", "events": abandoned_events},
    ]
    feed_path3 = FIXTURES / "events_SYN-3_grid.jsonl.zst"
    jsonl_text3 = "\n".join(json.dumps(env) for env in syn3_envelopes) + "\n"
    compressed3 = zstd.compress(jsonl_text3.encode("utf-8"))
    feed_path3.write_bytes(compressed3)

    # ---- Write shared games.csv ----
    # SYN-2: G1 (game_num=1), G2 (game_num=2)
    # SYN-3: G3 (game_num=1, same as G1!), G4 (game_num=2, sides swapped!)
    # G_ABANDONED not listed
    games_csv = FIXTURES / "games.csv"
    games_csv.write_text(
        "series_id,game_id,game_number,patch,team1_id,team1_side,team2_id,team2_side\n"
        "SYN-2,G1,1,16.20,T1,blue,T2,red\n"
        "SYN-2,G2,2,16.20,T1,blue,T2,red\n"
        "SYN-3,G3,1,16.20,T3,blue,T4,red\n"
        "SYN-3,G4,2,16.20,T3,red,T4,blue\n"
    )

    print(f"Created {feed_path}, {feed_path3}, and {games_csv}")


if __name__ == "__main__":
    create_synthetic_feed()
