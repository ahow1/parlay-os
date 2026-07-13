"""Tests for TIER 3 wire-in audit fixes (AUDIT.md B5, M11, M16, M6/M17).
Run: python -m pytest test_wire_ins.py -v
"""

import os
import tempfile
from unittest.mock import patch, MagicMock, call

import pytest


# ── B5: fix the crashing profile scheduler ────────────────────────────────────

class TestNightlyProfileSchedulerFixed:
    """B5: scheduler.py called run_nightly_profile_updates() with zero args
    against a 6-required-arg signature — crashed every single run, swallowed
    by the broad except. Fix: fetch today's completed games and pass real
    args per game."""

    def _fake_completed_game(self, game_pk=12345, away_score=5, home_score=2):
        return {
            "gamePk": game_pk,
            "teams": {
                "away": {"team": {"name": "San Francisco Giants"}, "score": away_score},
                "home": {"team": {"name": "Los Angeles Dodgers"}, "score": home_score},
            },
        }

    def test_calls_run_nightly_profile_updates_with_real_args(self):
        import scheduler
        game = self._fake_completed_game()
        sp_rows = [{
            "game_pk": "12345", "away_sp_id": 111, "away_sp_name": "Logan Webb",
            "home_sp_id": 222, "home_sp_name": "Tyler Glasnow",
        }]
        with patch("telegram_handler._fetch_final_games", return_value=[game]), \
             patch("db.get_sp_tracker", return_value=sp_rows), \
             patch("profile_engine.run_nightly_profile_updates") as mock_update:
            scheduler.run_nightly_profiles_task()

        assert mock_update.call_count == 1
        _, kwargs = mock_update.call_args
        assert kwargs["game_pk"] == 12345
        assert kwargs["away_team"] == "San Francisco Giants"
        assert kwargs["home_team"] == "Los Angeles Dodgers"
        assert kwargs["away_score"] == 5
        assert kwargs["home_score"] == 2
        assert kwargs["away_sp_id"] == 111
        assert kwargs["home_sp_id"] == 222
        assert kwargs["away_sp_name"] == "Logan Webb"
        assert kwargs["home_sp_name"] == "Tyler Glasnow"

    def test_no_completed_games_does_not_call_update(self):
        import scheduler
        with patch("telegram_handler._fetch_final_games", return_value=[]), \
             patch("profile_engine.run_nightly_profile_updates") as mock_update:
            scheduler.run_nightly_profiles_task()
        mock_update.assert_not_called()

    def test_missing_sp_tracker_row_falls_back_to_none_not_crash(self):
        """A completed game with no matching sp_tracker row (never scouted)
        must still call run_nightly_profile_updates, just without SP data."""
        import scheduler
        game = self._fake_completed_game()
        with patch("telegram_handler._fetch_final_games", return_value=[game]), \
             patch("db.get_sp_tracker", return_value=[]), \
             patch("profile_engine.run_nightly_profile_updates") as mock_update:
            scheduler.run_nightly_profiles_task()

        assert mock_update.call_count == 1
        _, kwargs = mock_update.call_args
        assert kwargs["away_sp_id"] is None
        assert kwargs["home_sp_id"] is None

    def test_task_never_raises_even_if_a_dependency_breaks(self):
        """Regression guard for the original bug class: whatever happens
        inside, run_nightly_profiles_task() itself must never raise."""
        import scheduler
        with patch("telegram_handler._fetch_final_games", side_effect=RuntimeError("boom")):
            scheduler.run_nightly_profiles_task()  # must not raise


class TestWeeklyTeamUpdatesTaskFixed:
    """B5 (second call site): run_weekly_team_updates(team_ids) requires a
    {team_code: team_id} dict; scheduler called it with zero args."""

    def test_weekly_team_updates_task_passes_real_team_ids(self):
        import scheduler
        from constants import MLB_TEAM_IDS
        with patch("profile_engine.run_weekly_team_updates") as mock_update:
            scheduler.run_weekly_team_updates_task()
        mock_update.assert_called_once_with(MLB_TEAM_IDS)

    def test_weekly_team_updates_task_never_raises(self):
        import scheduler
        with patch("profile_engine.run_weekly_team_updates", side_effect=RuntimeError("boom")):
            scheduler.run_weekly_team_updates_task()  # must not raise

    def test_schedule_loop_sunday_block_calls_new_task(self):
        """schedule_loop's Sunday block must call the new named task function
        (not an inline zero-arg import+call)."""
        import inspect
        import scheduler
        src = inspect.getsource(scheduler.schedule_loop)
        assert "run_weekly_team_updates_task()" in src


class TestProfileTablesReceiveRows:
    """Confirms the underlying persistence actually works — the bug was that
    the call site crashed before ever reaching this, not that persistence
    itself was broken. Isolated against a temp DB, not the live one."""

    def test_pitcher_profile_round_trips_through_real_db(self, tmp_path):
        import memory_engine as mem
        tmp_db = str(tmp_path / "test_profiles.db")
        with patch.object(mem, "DB_PATH", tmp_db):
            mem.init_memory_tables()
            mem.upsert_pitcher_profile("Logan Webb", "2026-07-13", {
                "pitcher_id": 657277, "era": 2.90, "k9": 8.1,
            })
            row = mem.get_pitcher_profile("Logan Webb")
        assert row is not None
        assert row["pitcher_id"] == 657277
        assert row["era"] == 2.90
