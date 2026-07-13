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


# ── M11: connect profile reads into sp_engine / offense_engine scoring ───────

class TestProfileReadsWiredIntoScoring:
    """M11: profile_engine persists pitcher_profiles/hitter_profiles every
    run, but memory_engine.get_pitcher_profile()/get_hitter_profile() had
    zero callers anywhere — persisted data was never read back. Wire in a
    small, capped, additive run_factor refinement in each engine; must fall
    back to 0.0 (no change) when no profile / insufficient data exists."""

    def test_sp_profile_platoon_adj_returns_zero_with_no_profile(self):
        from sp_engine import _profile_platoon_run_adj
        assert _profile_platoon_run_adj(None, era=3.50, opp_team="LAD") == 0.0

    def test_sp_profile_platoon_adj_returns_zero_when_splits_missing(self):
        from sp_engine import _profile_platoon_run_adj
        profile = {"era_vs_lhh": None, "era_vs_rhh": None}
        assert _profile_platoon_run_adj(profile, era=3.50, opp_team="LAD") == 0.0

    def test_sp_profile_platoon_adj_positive_when_matchup_worse(self):
        """Opponent has the league's highest LHB mix, pitcher's era_vs_lhh
        (5.00) is much worse than overall era (3.00) -> matchup_era > era
        -> positive adj (more expected runs)."""
        from sp_engine import _profile_platoon_run_adj
        from constants import TEAM_LHB_PCT
        profile = {"era_vs_lhh": 5.00, "era_vs_rhh": 2.50}
        opp = max(TEAM_LHB_PCT, key=TEAM_LHB_PCT.get)
        adj = _profile_platoon_run_adj(profile, era=3.00, opp_team=opp)
        assert adj > 0

    def test_sp_profile_platoon_adj_capped_at_8_pct(self):
        from sp_engine import _profile_platoon_run_adj
        profile = {"era_vs_lhh": 20.0, "era_vs_rhh": 20.0}
        adj = _profile_platoon_run_adj(profile, era=1.00, opp_team="LAD")
        assert adj == 0.08

    def test_analyze_sp_wires_profile_adj_into_run_factor(self):
        import inspect
        import sp_engine
        src = inspect.getsource(sp_engine.analyze_sp)
        assert "_profile_platoon_run_adj(" in src
        assert "profile_platoon_adj" in src

    def test_offense_profile_clutch_adj_returns_zero_with_no_lineup(self):
        from offense_engine import _profile_clutch_run_adj
        assert _profile_clutch_run_adj([]) == 0.0

    def test_offense_profile_clutch_adj_returns_zero_when_no_profiles_found(self):
        from offense_engine import _profile_clutch_run_adj
        lineup = [{"id": 1, "name": "Nobody Special"}]
        adj = _profile_clutch_run_adj(lineup, get_hitter_profile_fn=lambda name: None)
        assert adj == 0.0

    def test_offense_profile_clutch_adj_positive_when_lineup_clutch(self):
        from offense_engine import _profile_clutch_run_adj
        lineup = [{"id": 1, "name": "Hot Hitter"}, {"id": 2, "name": "Cold Hitter"}]
        profiles = {"Hot Hitter": {"wrc_risp": 140.0}, "Cold Hitter": {"wrc_risp": 130.0}}
        adj = _profile_clutch_run_adj(lineup, get_hitter_profile_fn=profiles.get)
        assert adj > 0

    def test_offense_profile_clutch_adj_capped_at_5_pct(self):
        from offense_engine import _profile_clutch_run_adj
        lineup = [{"id": 1, "name": "Legend"}]
        adj = _profile_clutch_run_adj(lineup, get_hitter_profile_fn=lambda name: {"wrc_risp": 999.0})
        assert adj == 0.05

    def test_analyze_offense_wires_profile_adj_into_run_factor(self):
        import inspect
        import offense_engine
        src = inspect.getsource(offense_engine.analyze_offense)
        assert "_profile_clutch_run_adj(" in src
        assert "profile_clutch_adj" in src


# ── TIER 3 WIRE-IN 3: log every MLB bet type reaching Telegram ───────────────

class TestWireIn3AllBetTypesLogged:
    """Only bet_type='ML' picks were ever written to `bets`. TOTAL, NRFI,
    PROP, and PARLAY picks reached Telegram/dashboard via _daily_bet_slip()
    but were never persisted anywhere — invisible to settlement, CLV
    grading, and the learning loop forever. Fix: log each pick type through
    the existing log_bet() path at the same point it's added to the slip,
    with zero change to the Telegram message text itself (isolated DB;
    never touches the real parlay_os.db)."""

    @pytest.fixture(autouse=True)
    def _isolated_db(self, tmp_path):
        import db
        tmp_db = str(tmp_path / "wire_in_3.db")
        with patch.object(db, "DB_PATH", tmp_db):
            db.init_db()
            yield db

    def _mk_ml_analysis(self, away_name, home_name, side, odds, model_p, stake, confidence=70):
        return {
            "away_name": away_name, "home_name": home_name,
            "away": away_name[:3].upper(), "home": home_name[:3].upper(),
            f"{side}_name": away_name if side == "away" else home_name,
            f"best_{side}_odds": odds,
            f"{side}_model_p": model_p,
            f"{side}_nv": 0.45,
            f"{side}_edge": 8.0,
            f"{side}_stake": stake,
            f"{side}_conv": "HIGH",
            f"{side}_confidence_score": confidence,
            "away_lineup_confirmed": True,
            "home_lineup_confirmed": True,
            "h2h": {},
        }

    def _call_slip(self):
        import brain
        locks = [
            (self._mk_ml_analysis("Team A", "Team B", "away", "-150", 0.60, 20.0), "away"),
            (self._mk_ml_analysis("Team C", "Team D", "home", "-150", 0.60, 20.0), "home"),
        ]
        all_nrfi = [{"game": "Team E @ Team F", "direction": "NRFI", "prob": 0.65, "stake": 10.0}]
        all_totals = [{
            "game": "Team G @ Team H", "direction": "OVER", "line": 8.5,
            "prob": 0.60, "market_p": 0.524, "edge_pct": 7.6, "stake": 12.0,
            "odds": "-110",
        }]
        all_k_props = [{
            "sp": "Logan Webb", "team": "SF", "game": "SF @ LAD", "line": 6.5,
            "p_over": 0.65, "market_p": 0.5, "edge_pct": 15.0, "stake": 9.0,
            "statcast_2025": False,
        }]
        all_hitter_props = [{
            "player": "Mookie Betts", "team": "LAD", "prop": "Hits O1.5",
            "line": 1.5, "model_prob": 0.62, "market_p": 0.5,
            "edge_pct": 12.0, "stake": 8.0,
        }]
        all_props = [{
            "type": "SP_DOMINANCE",
            "legs": ["SP OVER 6.5 Ks (65%)", "NRFI (65%)", "Game UNDER 8.5 (55%)"],
            "joint_prob": 0.30, "kelly_stake": 8.0, "ev": 0.05,
        }]
        result = brain._daily_bet_slip(
            locks, [], all_props, [], 1000.0,
            all_nrfi=all_nrfi, all_totals=all_totals,
            all_hitter_props=all_hitter_props, all_k_props=all_k_props,
        )
        return result

    def test_nrfi_pick_gets_a_bets_row(self, _isolated_db):
        self._call_slip()
        rows = [b for b in _isolated_db.get_bets() if b.get("type") == "NRFI"]
        assert len(rows) == 1
        assert rows[0]["stake"] == 10.0

    def test_total_pick_gets_a_bets_row(self, _isolated_db):
        self._call_slip()
        rows = [b for b in _isolated_db.get_bets() if b.get("type") == "TOTAL"]
        assert len(rows) == 1
        assert rows[0]["stake"] == 12.0

    def test_k_prop_and_hitter_prop_each_get_a_bets_row(self, _isolated_db):
        self._call_slip()
        rows = [b for b in _isolated_db.get_bets() if b.get("type") == "PROP"]
        assert len(rows) == 2
        names = {r["bet"] for r in rows}
        assert any("Logan Webb" in n for n in names)
        assert any("Mookie Betts" in n for n in names)

    def test_ml_parlay_gets_a_bets_row(self, _isolated_db):
        self._call_slip()
        rows = [b for b in _isolated_db.get_bets()
                if b.get("type") == "PARLAY" and b.get("game") == "PARLAY"]
        assert len(rows) == 1

    def test_sgp_gets_a_bets_row(self, _isolated_db):
        self._call_slip()
        rows = [b for b in _isolated_db.get_bets()
                if b.get("type") == "PARLAY" and "SGP" in (b.get("game") or "")]
        assert len(rows) == 1

    def test_telegram_message_text_unchanged_by_logging(self, _isolated_db, capsys):
        """DB writes must be a side effect only — the printed (DRY_RUN)
        Telegram message text must not reference logging at all."""
        self._call_slip()
        out = capsys.readouterr().out
        assert "LOCKS (2" in out
        assert "NRFI/YRFI (1 bets):" in out
        assert "TOTALS (1 bets):" in out
