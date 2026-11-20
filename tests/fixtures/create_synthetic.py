"""Helper to create synthetic zstd-compressed JSONL fixtures for testing."""

import json
from pathlib import Path

import zstandard as zstd

FIXTURES = Path(__file__).parent


def create_synthetic_feed():
    """Create a synthetic events_<series_id>_grid.jsonl.zst file with undos.

    Contains 2 games:
    - Game 1: complete draft with 1 ban undo and 1 pick undo (after folding: 10 bans, 10 picks)
    - Game 2: incomplete draft (only 5 bans, 5 picks) — should be dropped
    """
    # Game 1 events: we need 10 bans + 10 picks after undos.
    # Events must follow the standard draft order (interleaved bans/picks).
    # DRAFT_ORDER pattern:
    #   1:ban/blue, 2:ban/red, 3:pick/blue, 4:pick/red, 5:pick/red,
    #   6:ban/blue, 7:ban/red, 8:pick/blue, 9:pick/blue, 10:pick/red,
    #   11:ban/blue, 12:ban/red, 13:pick/blue, 14:pick/red, 15:pick/red,
    #   16:ban/blue, 17:ban/red, 18:pick/blue, 19:pick/red,
    #   20:ban/blue, 21:ban/red, 22:pick/blue, 23:pick/red,
    #   24:ban/blue, 25:ban/red
    #
    # We'll place the undo events right after their target, so the
    # folded stream still follows draft order.
    #
    # After folding: 10 bans + 10 picks = 20 events mapping to slots 1-20
    # (slots 21-25 are unused since we only have 20 events)
    #
    # Strategy: put the ban undo at position 1 (T1 bans Kalista, then undoes),
    # then T1's replacement ban (Azir) goes to the next blue ban slot.
    # Put the pick undo after T2's Lee Sin pick, then Graves as replacement.
    #
    # Let's lay out the events in draft order:
    game1_events = [
        # Slot 1: ban/blue — T1 bans Kalista
        {"type": "team-banned-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Kalista"}}, "gameId": "G1"},
        # Undo: T1 undoes Kalista ban (removed during folding)
        {"type": "team-!banned-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Kalista"}}, "gameId": "G1"},
        # Slot 2: ban/red — T2 bans K'Sante
        {"type": "team-banned-character", "actor": {"id": "T2"}, "target": {"state": {"name": "K'Sante"}}, "gameId": "G1"},
        # Slot 3: pick/blue — T1 picks Garen
        {"type": "team-picked-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Garen"}}, "gameId": "G1"},
        # Slot 4: pick/red — T2 picks Darius
        {"type": "team-picked-character", "actor": {"id": "T2"}, "target": {"state": {"name": "Darius"}}, "gameId": "G1"},
        # Slot 5: pick/red — T2 picks Lee Sin (will be undone)
        {"type": "team-picked-character", "actor": {"id": "T2"}, "target": {"state": {"name": "Lee Sin"}}, "gameId": "G1"},
        # Undo: T2 undoes Lee Sin pick (removed during folding)
        {"type": "team-!picked-character", "actor": {"id": "T2"}, "target": {"state": {"name": "Lee Sin"}}, "gameId": "G1"},
        # Slot 6: ban/blue — T1 bans Azir (replacement for Kalista)
        {"type": "team-banned-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Azir"}}, "gameId": "G1"},
        # Slot 7: ban/red — T2 bans Viktor
        {"type": "team-banned-character", "actor": {"id": "T2"}, "target": {"state": {"name": "Viktor"}}, "gameId": "G1"},
        # Slot 8: pick/blue — T1 picks Malphite
        {"type": "team-picked-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Malphite"}}, "gameId": "G1"},
        # Slot 9: pick/blue — T1 picks Thresh
        {"type": "team-picked-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Thresh"}}, "gameId": "G1"},
        # Slot 10: pick/red — T2 picks Graves (replacement for Lee Sin)
        {"type": "team-picked-character", "actor": {"id": "T2"}, "target": {"state": {"name": "Graves"}}, "gameId": "G1"},
        # Slot 11: ban/blue — T1 bans Orianna
        {"type": "team-banned-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Orianna"}}, "gameId": "G1"},
        # Slot 12: ban/red — T2 bans Syndra
        {"type": "team-banned-character", "actor": {"id": "T2"}, "target": {"state": {"name": "Syndra"}}, "gameId": "G1"},
        # Slot 13: pick/blue — T1 picks Caitlyn
        {"type": "team-picked-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Caitlyn"}}, "gameId": "G1"},
        # Slot 14: pick/red — T2 picks Nautilus
        {"type": "team-picked-character", "actor": {"id": "T2"}, "target": {"state": {"name": "Nautilus"}}, "gameId": "G1"},
        # Slot 15: pick/red — T2 picks Jinx
        {"type": "team-picked-character", "actor": {"id": "T2"}, "target": {"state": {"name": "Jinx"}}, "gameId": "G1"},
        # Slot 16: ban/blue — T1 bans LeBlanc
        {"type": "team-banned-character", "actor": {"id": "T1"}, "target": {"state": {"name": "LeBlanc"}}, "gameId": "G1"},
        # Slot 17: ban/red — T2 bans Ahri
        {"type": "team-banned-character", "actor": {"id": "T2"}, "target": {"state": {"name": "Ahri"}}, "gameId": "G1"},
        # Slot 18: pick/blue — T1 picks Warwick
        {"type": "team-picked-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Warwick"}}, "gameId": "G1"},
        # Slot 19: pick/red — T2 picks Soraka
        {"type": "team-picked-character", "actor": {"id": "T2"}, "target": {"state": {"name": "Soraka"}}, "gameId": "G1"},
        # Slot 20: ban/blue — T1 bans Yasuo
        {"type": "team-banned-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Yasuo"}}, "gameId": "G1"},
        # Slot 21: ban/red — T2 bans Yone
        {"type": "team-banned-character", "actor": {"id": "T2"}, "target": {"state": {"name": "Yone"}}, "gameId": "G1"},
        # Slot 22: pick/blue — (no more picks needed, but we need 10 picks)
        # Actually we already have 10 picks: Garen, Darius, Malphite, Thresh, Graves, Caitlyn, Nautilus, Jinx, Warwick, Soraka
        # And 10 bans: K'Sante, Azir, Viktor, Orianna, Syndra, LeBlanc, Ahri, Yasuo, Yone = 9
        # Need one more ban. Let's add it at slot 24
        # Slot 24: ban/blue — T1 bans Zed
        # But we need slot 22 and 23 first. Since we have 10 picks already,
        # slots 22-23 (pick/blue, pick/red) won't be used.
        # Let's restructure: we need exactly 10 bans and 10 picks.
        # Current: 9 bans (K'Sante, Azir, Viktor, Orianna, Syndra, LeBlanc, Ahri, Yasuo, Yone)
        # Add 1 more ban at the end
        {"type": "team-banned-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Zed"}}, "gameId": "G1"},
    ]
    # After folding: 10 bans (K'Sante, Azir, Viktor, Orianna, Syndra, LeBlanc, Ahri, Yasuo, Yone, Zed)
    # After folding: 10 picks (Garen, Darius, Malphite, Thresh, Graves, Caitlyn, Nautilus, Jinx, Warwick, Soraka)

    # Game 2: incomplete — only 5 bans, 5 picks
    # Use champions NOT used in Game 1 so Fearless detection works correctly
    game2_events = [
        {"type": "team-banned-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Ryze"}}, "gameId": "G2"},
        {"type": "team-banned-character", "actor": {"id": "T2"}, "target": {"state": {"name": "Sion"}}, "gameId": "G2"},
        {"type": "team-banned-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Sivir"}}, "gameId": "G2"},
        {"type": "team-banned-character", "actor": {"id": "T2"}, "target": {"state": {"name": "Annie"}}, "gameId": "G2"},
        {"type": "team-banned-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Nunu & Willump"}}, "gameId": "G2"},
        {"type": "team-picked-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Aatrox"}}, "gameId": "G2"},
        {"type": "team-picked-character", "actor": {"id": "T2"}, "target": {"state": {"name": "Camille"}}, "gameId": "G2"},
        {"type": "team-picked-character", "actor": {"id": "T2"}, "target": {"state": {"name": "Hecarim"}}, "gameId": "G2"},
        {"type": "team-picked-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Fiora"}}, "gameId": "G2"},
        {"type": "team-picked-character", "actor": {"id": "T1"}, "target": {"state": {"name": "Braum"}}, "gameId": "G2"},
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
