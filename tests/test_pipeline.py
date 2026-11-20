"""Tests for ingest and pipeline stages using synthetic fixtures."""

import pytest
from pathlib import Path
from unittest.mock import patch

from unpicked_intent.ingest.feeds import iter_draft_events
from unpicked_intent.pipeline.normalize import (
    apply_undos,
    assign_slots,
    check_completeness,
    flag_games,
)

FIXTURES = Path(__file__).parent / "fixtures"
FEED = str(FIXTURES / "events_SYN-2_grid.jsonl.zst")
GAMES_CSV = str(FIXTURES / "games.csv")


def _load_all():
    """Load all draft events from the synthetic feed."""
    return list(iter_draft_events(FEED, GAMES_CSV))


class TestIterDraftEvents:
    """Feed ingestion tests."""

    def test_streams_events(self):
        """iter_draft_events yields events from zstd-compressed feed."""
        events = _load_all()
        assert len(events) > 0
        # All events should have the required keys
        for ev in events:
            assert "series_id" in ev
            assert "action" in ev
            assert "champion" in ev
            assert "team" in ev

    def test_includes_undo_events(self):
        """Undo events are yielded as actions, not filtered."""
        events = _load_all()
        actions = {ev["action"] for ev in events}
        assert "undo-ban" in actions
        assert "undo-pick" in actions

    def test_resolves_metadata(self):
        """Patch, game_num, and side come from games.csv."""
        events = _load_all()
        for ev in events:
            if "_skipped_count" in ev:
                continue
            assert ev["patch"] == "16.20"
            assert ev["game_num"] in (1, 2)
            assert ev["side"] in ("blue", "red")

    def test_resolves_game_id_from_series_state_delta(self):
        """game_id comes from seriesStateDelta.games[0].id, not a flat gameId key."""
        events = _load_all()
        for ev in events:
            if "_skipped_count" in ev:
                continue
            assert ev["game_id"] in ("G1", "G2")

    def test_seq_field_present(self):
        """seq field is present; non-undo events have int seq, undos may have None."""
        events = _load_all()
        for ev in events:
            if "_skipped_count" in ev:
                continue
            assert "seq" in ev
            if ev["action"] not in ("undo-ban", "undo-pick"):
                # Non-undo events should have a sequence number
                assert ev["seq"] is not None
                assert isinstance(ev["seq"], int)

    def test_raises_on_missing_game_id(self):
        """Raises ValueError when game_id from feed is not in games.csv."""
        import tempfile
        import json
        import zstandard as zstd
        from pathlib import Path

        # Create a feed with a game_id not in games.csv
        envelope = {
            "occurredAt": "2026-11-01T10:00:00Z",
            "events": [
                {
                    "type": "team-banned-character",
                    "actor": {"id": "T1"},
                    "target": {"state": {"name": "Garen"}},
                    "seriesStateDelta": {"games": [{"id": "UNKNOWN-GAME", "draftActions": [{"sequenceNumber": 1}]}]},
                }
            ],
        }
        tmp_dir = Path(tempfile.mkdtemp())
        tmp_feed = tmp_dir / "events_TEST-1_grid.jsonl.zst"
        jsonl_text = json.dumps(envelope) + "\n"
        compressed = zstd.compress(jsonl_text.encode("utf-8"))
        tmp_feed.write_bytes(compressed)

        try:
            with pytest.raises(ValueError, match="UNKNOWN-GAME"):
                list(iter_draft_events(str(tmp_feed), GAMES_CSV))
        finally:
            tmp_feed.unlink(missing_ok=True)
            if tmp_dir.exists():
                tmp_dir.rmdir()

    def test_raises_on_missing_team(self):
        """Raises ValueError when team is not in games.csv for the game."""
        import tempfile
        import json
        import zstandard as zstd
        from pathlib import Path

        # Create a feed with a team not in games.csv
        envelope = {
            "occurredAt": "2026-11-01T10:00:00Z",
            "events": [
                {
                    "type": "team-banned-character",
                    "actor": {"id": "UNKNOWN-TEAM"},
                    "target": {"state": {"name": "Garen"}},
                    "seriesStateDelta": {"games": [{"id": "G1", "draftActions": [{"sequenceNumber": 1}]}]},
                }
            ],
        }
        tmp_dir = Path(tempfile.mkdtemp())
        tmp_feed = tmp_dir / "events_TEST-2_grid.jsonl.zst"
        jsonl_text = json.dumps(envelope) + "\n"
        compressed = zstd.compress(jsonl_text.encode("utf-8"))
        tmp_feed.write_bytes(compressed)

        try:
            with pytest.raises(ValueError, match="UNKNOWN-TEAM"):
                list(iter_draft_events(str(tmp_feed), GAMES_CSV))
        finally:
            tmp_feed.unlink(missing_ok=True)
            if tmp_dir.exists():
                tmp_dir.rmdir()

    def test_two_games(self):
        """Feed contains events for two games."""
        events = _load_all()
        game_nums = {ev["game_num"] for ev in events}
        assert game_nums == {1, 2}

    def test_streams_without_full_read(self):
        """iter_draft_events reads line by line, never calling read() on the stream."""
        import zstandard as zstd

        # Wrap zstd.open to track whether read() is ever called on the returned file object
        original_open = zstd.open
        read_called = []

        class TrackingTextIOWrapper:
            """Wrap the real text I/O object to intercept read() calls."""

            def __init__(self, wrapped):
                self._wrapped = wrapped

            def __iter__(self):
                return iter_lines(self._wrapped)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self._wrapped.__exit__(*args)

            def read(self, *args, **kwargs):
                read_called.append(True)
                return self._wrapped.read(*args, **kwargs)

            def __getattr__(self, name):
                return getattr(self._wrapped, name)

        def iter_lines(f):
            for line in f:
                yield line

        def tracking_open(path, mode="rt", encoding="utf-8"):
            real = original_open(path, mode, encoding=encoding)
            return TrackingTextIOWrapper(real)

        with patch.object(zstd, "open", side_effect=tracking_open):
            events = list(iter_draft_events(FEED, GAMES_CSV))

        assert len(events) > 0, "Should still yield events"
        assert len(read_called) == 0, (
            "read() was called on the stream — the file should be iterated line by line"
        )


class TestSeqSorting:
    """Tests for seq-based event ordering in normalize."""

    def test_sorts_by_seq_not_file_order(self):
        """Events are sorted by (seq, occurredAt), not file order."""
        # Create events out of file order but with correct seq numbers
        events = [
            {"game_num": 1, "action": "ban", "champion": "B", "team": "T2", "side": "red", "seq": 2, "occurredAt": "2026-01-01T00:00:02Z"},
            {"game_num": 1, "action": "ban", "champion": "A", "team": "T1", "side": "blue", "seq": 1, "occurredAt": "2026-01-01T00:00:01Z"},
            {"game_num": 1, "action": "pick", "champion": "C", "team": "T1", "side": "blue", "seq": 3, "occurredAt": "2026-01-01T00:00:03Z"},
        ]
        folded = apply_undos(events)
        # After sorting by seq, champion A (seq=1) should come before B (seq=2)
        actions = [e["champion"] for e in folded]
        assert actions == ["A", "B", "C"]

    def test_undos_with_no_seq_sorted_after_seq_events(self):
        """Undo events without seq are sorted after events with seq."""
        events = [
            {"game_num": 1, "action": "ban", "champion": "A", "team": "T1", "side": "blue", "seq": 1, "occurredAt": "2026-01-01T00:00:01Z"},
            {"game_num": 1, "action": "undo-ban", "champion": "A", "team": "T1", "side": "blue", "seq": None, "occurredAt": "2026-01-01T00:00:02Z"},
            {"game_num": 1, "action": "ban", "champion": "B", "team": "T2", "side": "red", "seq": 2, "occurredAt": "2026-01-01T00:00:03Z"},
        ]
        folded = apply_undos(events)
        # Undo should fold the ban on A, leaving only B
        assert len(folded) == 1
        assert folded[0]["champion"] == "B"


class TestApplyUndos:
    """Undo folding tests."""

    def test_ban_undo_removes_matching_ban(self):
        """A ban undo removes the most recent matching ban by the same team."""
        events = _load_all()
        # Before undos: Game 1 has 11 bans + 1 undo-ban = 12 ban-related events
        game1_before = [e for e in events if e["game_num"] == 1]
        bans_before = sum(1 for e in game1_before if e["action"] == "ban")
        undo_bans_before = sum(1 for e in game1_before if e["action"] == "undo-ban")
        assert undo_bans_before == 1

        folded = apply_undos(events)
        game1_after = [e for e in folded if e["game_num"] == 1]
        # After folding: no undo events, one fewer ban
        assert all(e["action"] not in ("undo-ban", "undo-pick") for e in folded)
        bans_after = sum(1 for e in game1_after if e["action"] == "ban")
        assert bans_after == bans_before - 1

    def test_pick_undo_removes_matching_pick(self):
        """A pick undo removes the most recent matching pick by the same team."""
        events = _load_all()
        game1_before = [e for e in events if e["game_num"] == 1]
        picks_before = sum(1 for e in game1_before if e["action"] == "pick")
        undo_picks_before = sum(1 for e in game1_before if e["action"] == "undo-pick")
        assert undo_picks_before == 1

        folded = apply_undos(events)
        game1_after = [e for e in folded if e["game_num"] == 1]
        picks_after = sum(1 for e in game1_after if e["action"] == "pick")
        assert picks_after == picks_before - 1

    def test_undone_champion_not_in_result(self):
        """The Kalista ban (undone) should not appear in folded events."""
        events = _load_all()
        folded = apply_undos(events)
        game1 = [e for e in folded if e["game_num"] == 1]
        kalista_bans = [e for e in game1 if e["action"] == "ban" and e["champion"] == "Kalista"]
        assert len(kalista_bans) == 0

    def test_undone_pick_not_in_result(self):
        """The Lee Sin pick (undone) should not appear in folded events."""
        events = _load_all()
        folded = apply_undos(events)
        game1 = [e for e in folded if e["game_num"] == 1]
        leesin_picks = [e for e in game1 if e["action"] == "pick" and e["champion"] == "Lee Sin"]
        assert len(leesin_picks) == 0

    def test_replacement_actions_remain(self):
        """After undo, the replacement ban/pick should still be present."""
        events = _load_all()
        folded = apply_undos(events)
        game1 = [e for e in folded if e["game_num"] == 1]
        # Azir was the replacement ban after Kalista undo
        azir_bans = [e for e in game1 if e["action"] == "ban" and e["champion"] == "Azir"]
        assert len(azir_bans) == 1
        # Graves was the replacement pick after Lee Sin undo
        graves_picks = [e for e in game1 if e["action"] == "pick" and e["champion"] == "Graves"]
        assert len(graves_picks) == 1


class TestAssignSlots:
    """Slot and phase assignment tests."""

    def test_slots_assigned_in_order(self):
        """Slots are assigned sequentially following draft order."""
        events = _load_all()
        folded = apply_undos(events)
        slotted = assign_slots(folded)
        game1 = [e for e in slotted if e["game_num"] == 1]
        slots = [e["slot"] for e in game1]
        # Slots should be strictly increasing
        assert slots == sorted(slots)
        assert len(slots) == len(set(slots))

    def test_phases_assigned(self):
        """Each event receives a valid phase."""
        events = _load_all()
        folded = apply_undos(events)
        slotted = assign_slots(folded)
        for ev in slotted:
            assert ev["phase"] in ("ban1", "pick1", "ban2", "pick2")

    def test_bans_come_before_picks_in_phase_order(self):
        """ban1 phase slots come before pick1 phase slots."""
        events = _load_all()
        folded = apply_undos(events)
        slotted = assign_slots(folded)
        game1 = [e for e in slotted if e["game_num"] == 1]
        first_ban_slot = next(e["slot"] for e in game1 if e["action"] == "ban")
        first_pick_slot = next(e["slot"] for e in game1 if e["action"] == "pick")
        assert first_ban_slot < first_pick_slot


class TestCheckCompleteness:
    """Incomplete game drop tests."""

    def test_drops_incomplete_game(self):
        """Game 2 (5 bans, 5 picks) is dropped; Game 1 (10/10) is kept."""
        events = _load_all()
        folded = apply_undos(events)
        slotted = assign_slots(folded)
        kept, dropped = check_completeness(slotted)
        assert dropped == 1
        kept_games = {e["game_num"] for e in kept}
        assert 1 in kept_games
        assert 2 not in kept_games

    def test_kept_game_has_10_bans_and_10_picks(self):
        """The kept game has exactly 10 bans and 10 picks."""
        events = _load_all()
        folded = apply_undos(events)
        slotted = assign_slots(folded)
        kept, _ = check_completeness(slotted)
        bans = sum(1 for e in kept if e["action"] == "ban")
        picks = sum(1 for e in kept if e["action"] == "pick")
        assert bans == 10
        assert picks == 10


class TestFlagGames:
    """Game flagging tests."""

    def test_flags_completeness(self):
        """Complete game is flagged, incomplete is not."""
        events = _load_all()
        folded = apply_undos(events)
        slotted = assign_slots(folded)
        kept, _ = check_completeness(slotted)
        flags = flag_games(kept)
        game1_flag = next(f for f in flags if f["game_num"] == 1)
        assert game1_flag["is_complete"] is True

    def test_flags_bo1(self):
        """Multi-game series is not Bo1."""
        events = _load_all()
        folded = apply_undos(events)
        slotted = assign_slots(folded)
        # Pass all slotted events (both games) so flag_games can see the series has 2 games
        flags = flag_games(slotted)
        for f in flags:
            assert f["is_bo1"] is False  # SYN-2 has 2 games

    def test_fearless_status(self):
        """Multi-game series with no repeats is flagged as hard Fearless."""
        events = _load_all()
        folded = apply_undos(events)
        slotted = assign_slots(folded)
        # Don't drop incomplete — flag all for fearless detection
        flags = flag_games(slotted)
        for f in flags:
            # SYN-2 has 2 games, no champion repeats → hard
            assert f["fearless_status"] == "hard"
