"""Tests for ingest and pipeline stages using synthetic fixtures."""

import pytest
from pathlib import Path
from unittest.mock import patch

from unpicked_intent.ingest.feeds import iter_draft_events, IngestStats
from unpicked_intent.pipeline.normalize import (
    apply_undos,
    assign_slots,
    check_completeness,
    flag_games,
    NormalizeStats,
    DRAFT_SIDE_ORDER,
)

FIXTURES = Path(__file__).parent / "fixtures"
FEED = str(FIXTURES / "events_SYN-2_grid.jsonl.zst")
FEED3 = str(FIXTURES / "events_SYN-3_grid.jsonl.zst")
GAMES_CSV = str(FIXTURES / "games.csv")


def _load_all():
    """Load all draft events from the synthetic feed. Returns (events, stats)."""
    it, stats = iter_draft_events(FEED, GAMES_CSV)
    return list(it), stats


def _load_all3():
    """Load all draft events from SYN-3 feed. Returns (events, stats)."""
    it, stats = iter_draft_events(FEED3, GAMES_CSV)
    return list(it), stats


class TestIterDraftEvents:
    """Feed ingestion tests."""

    def test_streams_events(self):
        """iter_draft_events yields events from zstd-compressed feed."""
        events, stats = _load_all()
        assert len(events) > 0
        for ev in events:
            assert "series_id" in ev
            assert "action" in ev
            assert "champion" in ev
            assert "team" in ev

    def test_returns_ingest_stats(self):
        """iter_draft_events returns an IngestStats dataclass."""
        events, stats = _load_all()
        assert isinstance(stats, IngestStats)
        assert stats.events_read == len(events)
        assert stats.skipped_games == 0

    def test_includes_undo_events(self):
        """Undo events are yielded as actions, not filtered."""
        events, _ = _load_all()
        actions = {ev["action"] for ev in events}
        assert "undo-ban" in actions
        assert "undo-pick" in actions

    def test_resolves_metadata(self):
        """Patch, game_num, and side come from games.csv."""
        events, _ = _load_all()
        for ev in events:
            assert ev["patch"] == "16.20"
            assert ev["game_num"] in (1, 2)
            assert ev["side"] in ("blue", "red")
            assert "meta_side" in ev

    def test_resolves_game_id_from_series_state_delta(self):
        """game_id comes from seriesStateDelta.games[0].id, not a flat gameId key."""
        events, _ = _load_all()
        for ev in events:
            assert ev["game_id"] in ("G1", "G2")

    def test_seq_field_present(self):
        """seq field is present; non-undo events have int seq, undos may have None."""
        events, _ = _load_all()
        for ev in events:
            assert "seq" in ev
            if ev["action"] not in ("undo-ban", "undo-pick"):
                assert ev["seq"] is not None
                assert isinstance(ev["seq"], int)

    def test_skips_abandoned_draft(self):
        """Events for game_id not in games.csv are skipped and counted."""
        events, stats = _load_all3()
        # G_ABANDONED events should be skipped
        game_ids = {ev["game_id"] for ev in events}
        assert "G_ABANDONED" not in game_ids
        assert stats.skipped_games == 1
        # G3 and G4 events should still be present
        assert "G3" in game_ids
        assert "G4" in game_ids

    def test_no_skipped_count_in_stream(self):
        """The event stream no longer contains _skipped_count summary dicts."""
        events, _ = _load_all()
        for ev in events:
            assert "_skipped_count" not in ev

    def test_two_games(self):
        """Feed contains events for two games."""
        events, _ = _load_all()
        game_ids = {ev["game_id"] for ev in events}
        assert game_ids == {"G1", "G2"}

    def test_streams_without_full_read(self):
        """iter_draft_events reads line by line, never calling read() on the stream."""
        import zstandard as zstd

        original_open = zstd.open
        read_called = []

        class TrackingTextIOWrapper:
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
            it, _ = iter_draft_events(FEED, GAMES_CSV)
            events = list(it)

        assert len(events) > 0, "Should still yield events"
        assert len(read_called) == 0, (
            "read() was called on the stream — the file should be iterated line by line"
        )


class TestCrossSeriesGameNum:
    """Tests that grouping by game_id (not game_num) works across series."""

    def test_two_series_same_game_num(self):
        """SYN-2 G1 and SYN-3 G3 both have game_num=1 but different game_ids."""
        events2, _ = _load_all()
        events3, _ = _load_all3()
        # G1 (SYN-2) and G3 (SYN-3) both have game_num=1
        g1_events = [e for e in events2 if e["game_id"] == "G1"]
        g3_events = [e for e in events3 if e["game_id"] == "G3"]
        assert all(e["game_num"] == 1 for e in g1_events)
        assert all(e["game_num"] == 1 for e in g3_events)
        # They should not be mixed when grouped by game_id
        folded = apply_undos(events2 + events3)
        g1_folded = [e for e in folded if e["game_id"] == "G1"]
        g3_folded = [e for e in folded if e["game_id"] == "G3"]
        # G1 has undo events folded; G3 is clean
        assert all(e["action"] not in ("undo-ban", "undo-pick") for e in g1_folded)
        assert all(e["action"] not in ("undo-ban", "undo-pick") for e in g3_folded)
        # G3 should have exactly 20 events (10 bans + 10 picks, no undos)
        bans3 = sum(1 for e in g3_folded if e["action"] == "ban")
        picks3 = sum(1 for e in g3_folded if e["action"] == "pick")
        assert bans3 == 10
        assert picks3 == 10

    def test_completeness_separates_same_game_num(self):
        """check_completeness drops G2 (incomplete) but keeps G1, G3, and G4."""
        events2, _ = _load_all()
        events3, _ = _load_all3()
        all_events = events2 + events3
        folded = apply_undos(all_events)
        slotted, _ = assign_slots(folded)
        kept, dropped = check_completeness(slotted)
        assert dropped == 1  # G2 only
        kept_game_ids = {e["game_id"] for e in kept}
        assert "G1" in kept_game_ids
        assert "G3" in kept_game_ids
        assert "G4" in kept_game_ids
        assert "G2" not in kept_game_ids


class TestApplyUndos:
    """Undo folding tests."""

    def test_ban_undo_removes_matching_ban(self):
        """A ban undo removes the most recent matching ban by the same team."""
        events, _ = _load_all()
        game1_before = [e for e in events if e["game_id"] == "G1"]
        bans_before = sum(1 for e in game1_before if e["action"] == "ban")
        undo_bans_before = sum(1 for e in game1_before if e["action"] == "undo-ban")
        assert undo_bans_before == 1

        folded = apply_undos(events)
        game1_after = [e for e in folded if e["game_id"] == "G1"]
        assert all(e["action"] not in ("undo-ban", "undo-pick") for e in folded)
        bans_after = sum(1 for e in game1_after if e["action"] == "ban")
        assert bans_after == bans_before - 1

    def test_pick_undo_removes_matching_pick(self):
        """A pick undo removes the most recent matching pick by the same team."""
        events, _ = _load_all()
        game1_before = [e for e in events if e["game_id"] == "G1"]
        picks_before = sum(1 for e in game1_before if e["action"] == "pick")
        undo_picks_before = sum(1 for e in game1_before if e["action"] == "undo-pick")
        assert undo_picks_before == 1

        folded = apply_undos(events)
        game1_after = [e for e in folded if e["game_id"] == "G1"]
        picks_after = sum(1 for e in game1_after if e["action"] == "pick")
        assert picks_after == picks_before - 1

    def test_undone_ban_not_in_result(self):
        """The Kalista ban (undone) should not appear in folded events."""
        events, _ = _load_all()
        folded = apply_undos(events)
        game1 = [e for e in folded if e["game_id"] == "G1"]
        kalista_bans = [e for e in game1 if e["action"] == "ban" and e["champion"] == "Kalista"]
        assert len(kalista_bans) == 0

    def test_undone_pick_not_in_result(self):
        """The Garen pick (undone) should not appear in folded events."""
        events, _ = _load_all()
        folded = apply_undos(events)
        game1 = [e for e in folded if e["game_id"] == "G1"]
        garen_picks = [e for e in game1 if e["action"] == "pick" and e["champion"] == "Garen"]
        assert len(garen_picks) == 0

    def test_replacement_pick_in_result(self):
        """Lee Sin (replacement for undone Garen pick) should be in folded events."""
        events, _ = _load_all()
        folded = apply_undos(events)
        game1 = [e for e in folded if e["game_id"] == "G1"]
        leesin_picks = [e for e in game1 if e["action"] == "pick" and e["champion"] == "Lee Sin"]
        assert len(leesin_picks) == 1

    def test_undo_matches_by_seq(self):
        """Undo with seq matches the action with the same seq, not by champion."""
        events = [
            {"game_id": "G1", "action": "ban", "champion": "A", "team": "T1", "side": "blue", "seq": 1, "occurredAt": "2026-01-01T00:00:01Z"},
            {"game_id": "G1", "action": "ban", "champion": "B", "team": "T1", "side": "blue", "seq": 1, "occurredAt": "2026-01-01T00:00:02Z"},
            {"game_id": "G1", "action": "undo-ban", "champion": "A", "team": "T1", "side": "blue", "seq": 1, "occurredAt": "2026-01-01T00:00:03Z"},
        ]
        folded = apply_undos(events)
        # Undo with seq=1 should match the most recent action with seq=1 (champion B)
        champions = [e["champion"] for e in folded]
        assert champions == ["A"]

    def test_undo_fallback_to_champion_when_no_seq(self):
        """When undo has no seq, fallback to matching by champion + team."""
        events = [
            {"game_id": "G1", "action": "ban", "champion": "A", "team": "T1", "side": "blue", "seq": None, "occurredAt": "2026-01-01T00:00:01Z"},
            {"game_id": "G1", "action": "ban", "champion": "B", "team": "T2", "side": "red", "seq": None, "occurredAt": "2026-01-01T00:00:02Z"},
            {"game_id": "G1", "action": "undo-ban", "champion": "A", "team": "T1", "side": "blue", "seq": None, "occurredAt": "2026-01-01T00:00:03Z"},
        ]
        folded = apply_undos(events)
        assert len(folded) == 1
        assert folded[0]["champion"] == "B"


class TestAssignSlots:
    """Slot and phase assignment tests."""

    def test_slots_assigned_from_seq(self):
        """Slot is set directly from seq, not from template walking."""
        events, _ = _load_all()
        folded = apply_undos(events)
        slotted, stats = assign_slots(folded)
        game1 = [e for e in slotted if e["game_id"] == "G1"]
        for ev in game1:
            if ev["action"] in ("ban", "pick"):
                assert ev["slot"] == ev["seq"]

    def test_phases_assigned(self):
        """Each event receives a valid phase."""
        events, _ = _load_all()
        folded = apply_undos(events)
        slotted, _ = assign_slots(folded)
        for ev in slotted:
            if ev["action"] in ("ban", "pick"):
                assert ev["phase"] in ("ban1", "pick1", "ban2", "pick2")

    def test_phase_from_seq_ranges(self):
        """Phase is determined by seq range: 1-6 ban1, 7-12 pick1, 13-16 ban2, 17-20 pick2."""
        events, _ = _load_all()
        folded = apply_undos(events)
        slotted, _ = assign_slots(folded)
        for ev in slotted:
            seq = ev.get("seq")
            if seq is None:
                continue
            phase = ev["phase"]
            if 1 <= seq <= 6:
                assert phase == "ban1", f"seq {seq} should be ban1, got {phase}"
            elif 7 <= seq <= 12:
                assert phase == "pick1", f"seq {seq} should be pick1, got {phase}"
            elif 13 <= seq <= 16:
                assert phase == "ban2", f"seq {seq} should be ban2, got {phase}"
            elif 17 <= seq <= 20:
                assert phase == "pick2", f"seq {seq} should be pick2, got {phase}"

    def test_returns_normalize_stats(self):
        """assign_slots returns NormalizeStats with side_mismatches."""
        events, _ = _load_all()
        folded = apply_undos(events)
        slotted, stats = assign_slots(folded)
        assert isinstance(stats, NormalizeStats)
        assert hasattr(stats, "side_mismatches")

    def test_side_mismatch_count(self):
        """Side mismatches are counted when derived side doesn't match draft order."""
        # T1 acts at seq 1, so T1 is blue by derivation.
        # Seq 1 expects blue (T1=blue ✓), seq 2 expects red (T2=red ✓).
        # No mismatches when sides are correct.
        events = [
            {"game_id": "G1", "action": "ban", "champion": "A", "team": "T1", "side": "blue", "meta_side": "blue", "seq": 1, "occurredAt": "2026-01-01T00:00:01Z"},
            {"game_id": "G1", "action": "ban", "champion": "B", "team": "T2", "side": "red", "meta_side": "red", "seq": 2, "occurredAt": "2026-01-01T00:00:02Z"},
        ]
        _, stats = assign_slots(events)
        assert stats.side_mismatches == 0

    def test_draft_side_order_slots_13_20(self):
        """DRAFT_SIDE_ORDER slots 13-20: ban2 R B R B, pick2 R B B R."""
        assert DRAFT_SIDE_ORDER[12] == "red"   # 13
        assert DRAFT_SIDE_ORDER[13] == "blue"  # 14
        assert DRAFT_SIDE_ORDER[14] == "red"   # 15
        assert DRAFT_SIDE_ORDER[15] == "blue"  # 16
        assert DRAFT_SIDE_ORDER[16] == "red"   # 17
        assert DRAFT_SIDE_ORDER[17] == "blue"  # 18
        assert DRAFT_SIDE_ORDER[18] == "blue"  # 19
        assert DRAFT_SIDE_ORDER[19] == "red"   # 20

    def test_side_derived_from_draft(self):
        """Side is derived from draft: team at seq 1 is blue."""
        events = [
            {"game_id": "G1", "action": "ban", "champion": "A", "team": "T1", "side": "blue", "meta_side": "blue", "seq": 1, "occurredAt": "2026-01-01T00:00:01Z"},
            {"game_id": "G1", "action": "ban", "champion": "B", "team": "T2", "side": "red", "meta_side": "red", "seq": 2, "occurredAt": "2026-01-01T00:00:02Z"},
        ]
        slotted, _ = assign_slots(events)
        # T1 acts at seq 1 → derived blue; T2 → derived red
        assert slotted[0]["side"] == "blue"
        assert slotted[1]["side"] == "red"

    def test_meta_side_disagreement(self):
        """meta_side_disagreements counted when games.csv side differs from draft-derived side."""
        # T1 acts at seq 1 → derived blue, but meta_side says red (swapped)
        events = [
            {"game_id": "G1", "action": "ban", "champion": "A", "team": "T1", "side": "red", "meta_side": "red", "seq": 1, "occurredAt": "2026-01-01T00:00:01Z"},
            {"game_id": "G1", "action": "ban", "champion": "B", "team": "T2", "side": "blue", "meta_side": "blue", "seq": 2, "occurredAt": "2026-01-01T00:00:02Z"},
        ]
        slotted, stats = assign_slots(events)
        # T1 at seq 1 → derived blue, but meta_side=red → disagreement
        assert stats.meta_side_disagreements == 1
        # Derived side should be blue for T1 (from draft), not red (from games.csv)
        assert slotted[0]["side"] == "blue"

    def test_meta_side_disagreement_from_fixture(self):
        """G4 has swapped sides in games.csv: meta_side_disagreements should be 1."""
        events, _ = _load_all3()
        folded = apply_undos(events)
        slotted, stats = assign_slots(folded)
        # G3: T3 at seq 1, meta_side=blue → no disagreement
        # G4: T3 at seq 1, meta_side=red → disagreement (T3 derived blue, meta says red)
        # G_ABANDONED: skipped by ingest, not in events
        assert stats.meta_side_disagreements == 1

    def test_zero_side_mismatches_with_correct_order(self):
        """With corrected DRAFT_SIDE_ORDER and derived sides, real-style drafts have 0 mismatches."""
        events, _ = _load_all()
        folded = apply_undos(events)
        slotted, stats = assign_slots(folded)
        # G1 and G2 use correct draft order with derived sides
        assert stats.side_mismatches == 0

    def test_no_unknown_phase(self):
        """With valid seq 1-20, no event gets 'unknown' phase."""
        events, _ = _load_all()
        folded = apply_undos(events)
        slotted, _ = assign_slots(folded)
        for ev in slotted:
            if ev["action"] in ("ban", "pick"):
                assert ev["phase"] != "unknown"


class TestCheckCompleteness:
    """Incomplete game drop tests."""

    def test_drops_incomplete_game(self):
        """Game 2 (5 bans, 5 picks) is dropped; Game 1 (10/10) is kept."""
        events, _ = _load_all()
        folded = apply_undos(events)
        slotted, _ = assign_slots(folded)
        kept, dropped = check_completeness(slotted)
        assert dropped == 1
        kept_game_ids = {e["game_id"] for e in kept}
        assert "G1" in kept_game_ids
        assert "G2" not in kept_game_ids

    def test_kept_game_has_10_bans_and_10_picks(self):
        """The kept game has exactly 10 bans and 10 picks."""
        events, _ = _load_all()
        folded = apply_undos(events)
        slotted, _ = assign_slots(folded)
        kept, _ = check_completeness(slotted)
        bans = sum(1 for e in kept if e["action"] == "ban")
        picks = sum(1 for e in kept if e["action"] == "pick")
        assert bans == 10
        assert picks == 10


class TestFlagGames:
    """Game flagging tests."""

    def test_flags_completeness(self):
        """Complete game is flagged, incomplete is not."""
        events, _ = _load_all()
        folded = apply_undos(events)
        slotted, _ = assign_slots(folded)
        kept, _ = check_completeness(slotted)
        flags = flag_games(kept)
        game1_flag = next(f for f in flags if f["game_id"] == "G1")
        assert game1_flag["is_complete"] is True

    def test_flags_bo1(self):
        """Multi-game series is not Bo1."""
        events, _ = _load_all()
        folded = apply_undos(events)
        slotted, _ = assign_slots(folded)
        flags = flag_games(slotted)
        for f in flags:
            assert f["is_bo1"] is False  # SYN-2 has 2 games

    def test_flags_bo1_with_incomplete_dropped(self):
        """When incomplete game is dropped, series_game_count keeps is_bo1 correct."""
        events, _ = _load_all()
        folded = apply_undos(events)
        slotted, _ = assign_slots(folded)
        # Compute series game count before dropping
        series_game_count = {}
        from collections import Counter
        for ev in slotted:
            sid = ev["series_id"]
            if sid not in series_game_count:
                series_game_count[sid] = set()
            series_game_count[sid].add(ev["game_id"])
        series_game_count = {k: len(v) for k, v in series_game_count.items()}
        # Drop incomplete game
        kept, dropped = check_completeness(slotted)
        assert dropped == 1  # G2 dropped
        # Without series_game_count, G1 would appear as Bo1 (only game left)
        flags_without = flag_games(kept)
        g1_flag_without = next(f for f in flags_without if f["game_id"] == "G1")
        assert g1_flag_without["is_bo1"] is True  # wrong — only 1 game in kept
        # With series_game_count, G1 correctly sees 2 games in series
        flags_with = flag_games(kept, series_game_count=series_game_count)
        g1_flag_with = next(f for f in flags_with if f["game_id"] == "G1")
        assert g1_flag_with["is_bo1"] is False  # correct — 2 games in series

    def test_fearless_with_incomplete_dropped(self):
        """Fearless status uses pre-drop game count, not post-drop."""
        events, _ = _load_all()
        folded = apply_undos(events)
        slotted, _ = assign_slots(folded)
        # Compute series game count before dropping
        series_game_count = {}
        for ev in slotted:
            sid = ev["series_id"]
            if sid not in series_game_count:
                series_game_count[sid] = set()
            series_game_count[sid].add(ev["game_id"])
        series_game_count = {k: len(v) for k, v in series_game_count.items()}
        kept, _ = check_completeness(slotted)
        # Without series_game_count: only G1 remains → ambiguous
        flags_without = flag_games(kept)
        g1_flag_without = next(f for f in flags_without if f["game_id"] == "G1")
        assert g1_flag_without["fearless_status"] == "ambiguous"
        # With series_game_count: 2 games in series → hard (no repeats)
        flags_with = flag_games(kept, series_game_count=series_game_count)
        g1_flag_with = next(f for f in flags_with if f["game_id"] == "G1")
        assert g1_flag_with["fearless_status"] == "hard"

    def test_fearless_status(self):
        """Multi-game series with no repeats is flagged as hard Fearless."""
        events, _ = _load_all()
        folded = apply_undos(events)
        slotted, _ = assign_slots(folded)
        flags = flag_games(slotted)
        for f in flags:
            assert f["fearless_status"] == "hard"

    def test_flags_include_game_id(self):
        """Flag dicts include game_id for disambiguation."""
        events, _ = _load_all()
        folded = apply_undos(events)
        slotted, _ = assign_slots(folded)
        flags = flag_games(slotted)
        for f in flags:
            assert "game_id" in f


class TestSeqSorting:
    """Tests for seq-based event ordering in normalize."""

    def test_sorts_by_seq_not_file_order(self):
        """Events are sorted by (seq, occurredAt), not file order."""
        events = [
            {"game_id": "G1", "action": "ban", "champion": "B", "team": "T2", "side": "red", "seq": 2, "occurredAt": "2026-01-01T00:00:02Z"},
            {"game_id": "G1", "action": "ban", "champion": "A", "team": "T1", "side": "blue", "seq": 1, "occurredAt": "2026-01-01T00:00:01Z"},
            {"game_id": "G1", "action": "pick", "champion": "C", "team": "T1", "side": "blue", "seq": 3, "occurredAt": "2026-01-01T00:00:03Z"},
        ]
        folded = apply_undos(events)
        champions = [e["champion"] for e in folded]
        assert champions == ["A", "B", "C"]

    def test_undos_with_no_seq_sorted_after_seq_events(self):
        """Undo events without seq are sorted after events with seq."""
        events = [
            {"game_id": "G1", "action": "ban", "champion": "A", "team": "T1", "side": "blue", "seq": 1, "occurredAt": "2026-01-01T00:00:01Z"},
            {"game_id": "G1", "action": "undo-ban", "champion": "A", "team": "T1", "side": "blue", "seq": None, "occurredAt": "2026-01-01T00:00:02Z"},
            {"game_id": "G1", "action": "ban", "champion": "B", "team": "T2", "side": "red", "seq": 2, "occurredAt": "2026-01-01T00:00:03Z"},
        ]
        folded = apply_undos(events)
        assert len(folded) == 1
        assert folded[0]["champion"] == "B"
