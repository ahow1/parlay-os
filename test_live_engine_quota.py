"""Tests for live_engine.py's 2026-09-09 quota-conservation changes:
- _open_live_positions() / _game_has_open_position() (item 3: only poll
  games with an open position; skip the run entirely if none exist).
- run_live_pass()'s ordering of guard checks (quota guard > live window >
  drawdown pause > open positions) and its mid-pass quota-trip behavior.
- The cadence trade-off note being logged every pass (item 2).

Run: python -m pytest test_live_engine_quota.py -v
"""

from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest
import pytz

import db
import live_engine
import odds_quota

_ET = pytz.timezone("America/New_York")


def _today_et() -> str:
    return datetime.now(_ET).strftime("%Y-%m-%d")


@pytest.fixture(autouse=True)
def _isolated_db(tmp_path, monkeypatch):
    tmp_db = str(tmp_path / "live_quota_test.db")
    monkeypatch.setattr(db, "DB_PATH", tmp_db)
    monkeypatch.setattr(live_engine, "DB_PATH", tmp_db)
    import memory_engine
    monkeypatch.setattr(memory_engine, "DB_PATH", tmp_db)
    db.init_db()
    yield db


@pytest.fixture(autouse=True)
def _isolate_quota_state(tmp_path, monkeypatch):
    monkeypatch.setattr(odds_quota, "STATE_FILE", str(tmp_path / "odds_quota_state.json"))
    monkeypatch.setattr(odds_quota, "MONTHLY_ALLOWANCE", 500)


def _log_bet(d, bet="Boston Red Sox", game="Tampa Bay Rays @ Boston Red Sox",
             date="2026-07-28", **overrides):
    kwargs = dict(
        date=date, bet=bet, bet_type="ML", game=game,
        sp="", park="BOS", umpire="", bet_odds="-120",
        model_prob=0.55, market_prob=0.50, edge_pct=5.0,
        conviction="MEDIUM", stake=25.0,
    )
    kwargs.update(overrides)
    d.log_bet(**kwargs)


def _live_state(away="Tampa Bay Rays", home="Boston Red Sox", game_pk=123):
    return {
        "game_pk": game_pk, "away_team": away, "home_team": home,
        "away_code": "TB", "home_code": "BOS",
        "inning": 5, "top_inning": True, "outs": 1,
        "away_runs": 2, "home_runs": 4,
        "cur_pitcher_id": None, "cur_pitcher_name": "Someone",
        "cur_batter_id": None,
        "away_order": [], "home_order": [],
        "away_sp": {"id": None, "name": "TBD", "np": 0, "still_in": False},
        "home_sp": {"id": None, "name": "TBD", "np": 0, "still_in": False},
        "away_pitchers": [], "home_pitchers": [],
        "runners": {"1b": False, "2b": False, "3b": False},
        "abstract_state": "Live",
    }


class TestOpenLivePositions:
    def test_no_pending_bets_returns_empty(self, _isolated_db):
        assert live_engine._open_live_positions("2026-07-28") == []

    def test_returns_pending_bets_for_the_date(self, _isolated_db):
        _log_bet(_isolated_db)
        positions = live_engine._open_live_positions("2026-07-28")
        assert len(positions) == 1
        assert positions[0]["game"] == "Tampa Bay Rays @ Boston Red Sox"

    def test_resolved_bets_are_not_open_positions(self, _isolated_db):
        _log_bet(_isolated_db)
        _isolated_db.resolve_bet_by_id(
            bet_id=_isolated_db.get_bets()[0]["id"],
            closing_odds="-115", result="W", game_score="5-3", mark_notified=True,
        )
        assert live_engine._open_live_positions("2026-07-28") == []

    def test_other_dates_are_excluded(self, _isolated_db):
        _log_bet(_isolated_db, date="2026-07-27")
        assert live_engine._open_live_positions("2026-07-28") == []

    def test_db_error_fails_open_to_empty_list(self, _isolated_db, monkeypatch):
        monkeypatch.setattr(db, "get_bets", MagicMock(side_effect=RuntimeError("boom")))
        assert live_engine._open_live_positions("2026-07-28") == []


class TestGameHasOpenPosition:
    def test_matches_when_both_teams_present(self):
        state = _live_state()
        positions = [{"game": "Tampa Bay Rays @ Boston Red Sox"}]
        assert live_engine._game_has_open_position(state, positions) is True

    def test_no_match_for_unrelated_game(self):
        state = _live_state()
        positions = [{"game": "New York Yankees @ Baltimore Orioles"}]
        assert live_engine._game_has_open_position(state, positions) is False

    def test_no_positions_is_no_match(self):
        assert live_engine._game_has_open_position(_live_state(), []) is False

    def test_missing_team_names_never_match(self):
        state = {"away_team": "", "home_team": ""}
        positions = [{"game": "Tampa Bay Rays @ Boston Red Sox"}]
        assert live_engine._game_has_open_position(state, positions) is False

    def test_ignores_bets_with_no_game_field(self):
        state = _live_state()
        positions = [{"bet": "Boston Red Sox"}]  # no "game" key
        assert live_engine._game_has_open_position(state, positions) is False


class TestRunLivePassGuardOrdering:
    """Quota guard (already tripped) takes priority over every other check --
    it must short-circuit before touching the DB or the live window clock."""

    def test_quota_guard_skips_before_anything_else(self, monkeypatch):
        with patch.object(odds_quota, "_send_alert"):
            odds_quota.maybe_trip_guard(0.9)  # pre-trip the guard for this month

        with patch.object(live_engine, "_in_live_window") as mock_window, \
             patch.object(live_engine, "_open_live_positions") as mock_positions:
            result = live_engine.run_live_pass()

        assert result["skipped"] is True
        assert "quota guard" in result["reason"]
        mock_window.assert_not_called()
        mock_positions.assert_not_called()

    def test_outside_live_window_skips(self, monkeypatch):
        monkeypatch.setattr(live_engine, "_in_live_window", lambda: False)
        with patch.object(live_engine, "_open_live_positions") as mock_positions:
            result = live_engine.run_live_pass()
        assert result == {"skipped": True, "reason": "outside live window", "games_polled": 0, "alerts_sent": 0}
        mock_positions.assert_not_called()

    def test_drawdown_pause_skips_and_alerts(self, monkeypatch):
        monkeypatch.setattr(live_engine, "_in_live_window", lambda: True)
        monkeypatch.setattr(live_engine, "is_drawdown_pause", lambda: True)
        with patch.object(live_engine, "_send_telegram") as mock_tg:
            result = live_engine.run_live_pass()
        assert result["skipped"] is True
        assert result["reason"] == "drawdown pause"
        mock_tg.assert_called_once()
        assert "DRAWDOWN" in mock_tg.call_args[0][0]


class TestRunLivePassNoPositions:
    def test_no_open_positions_skips_entirely(self, monkeypatch, capsys):
        monkeypatch.setattr(live_engine, "_in_live_window", lambda: True)
        monkeypatch.setattr(live_engine, "is_drawdown_pause", lambda: False)
        with patch.object(live_engine, "_fetch_live_games") as mock_fetch:
            result = live_engine.run_live_pass()
        assert result == {"skipped": True, "reason": "no live positions, skipping", "games_polled": 0, "alerts_sent": 0}
        mock_fetch.assert_not_called()
        assert "no live positions, skipping" in capsys.readouterr().out

    def test_positions_exist_but_no_matching_live_game_skips(self, monkeypatch, _isolated_db):
        _log_bet(_isolated_db, game="New York Yankees @ Baltimore Orioles", date=_today_et())
        monkeypatch.setattr(live_engine, "_in_live_window", lambda: True)
        monkeypatch.setattr(live_engine, "is_drawdown_pause", lambda: False)
        # A live game exists, but for a totally different matchup.
        monkeypatch.setattr(live_engine, "_fetch_live_games", lambda today: [{"gamePk": 1}])
        monkeypatch.setattr(live_engine, "_parse_game_state", lambda g: _live_state())
        with patch.object(live_engine, "get_mlb_events") as mock_events:
            result = live_engine.run_live_pass()
        assert result["skipped"] is True
        assert result["reason"] == "no live positions, skipping"
        mock_events.assert_not_called()


class TestRunLivePassPolling:
    """Once an open position's game is actually live, exactly that game
    gets polled -- verified by mocking the expensive per-game calls and
    counting invocations."""

    def _wire_common(self, monkeypatch):
        monkeypatch.setattr(live_engine, "_in_live_window", lambda: True)
        monkeypatch.setattr(live_engine, "is_drawdown_pause", lambda: False)
        monkeypatch.setattr(live_engine, "analyze_bullpen", lambda *a, **k: {})
        monkeypatch.setattr(live_engine, "_fetch_scoring_plays", lambda gp: [])
        monkeypatch.setattr(live_engine, "run_live_cycle", lambda *a, **k: [])
        monkeypatch.setattr(live_engine, "_update_dashboard", lambda *a, **k: None)

    def test_polls_only_the_game_with_an_open_position(self, monkeypatch, _isolated_db):
        _log_bet(_isolated_db, game="Tampa Bay Rays @ Boston Red Sox", date=_today_et())
        self._wire_common(monkeypatch)
        monkeypatch.setattr(
            live_engine, "_fetch_live_games",
            lambda today: [{"gamePk": 1, "matchup": "relevant"}, {"gamePk": 2, "matchup": "irrelevant"}],
        )

        def _parse(g):
            if g["gamePk"] == 1:
                return _live_state(away="Tampa Bay Rays", home="Boston Red Sox", game_pk=1)
            return _live_state(away="New York Yankees", home="Baltimore Orioles", game_pk=2)

        monkeypatch.setattr(live_engine, "_parse_game_state", _parse)

        market_calls = []
        monkeypatch.setattr(live_engine, "get_mlb_events", lambda: [])
        monkeypatch.setattr(
            live_engine, "full_market_snapshot",
            lambda *a, **k: market_calls.append(a) or {},
        )

        result = live_engine.run_live_pass()

        assert result["skipped"] is False
        assert result["games_polled"] == 1
        assert len(market_calls) == 1  # only the relevant game triggered a market (odds) call

    def test_no_relevant_games_makes_zero_odds_calls(self, monkeypatch, _isolated_db):
        """The core quota-saving guarantee: with no open position anywhere
        live, full_market_snapshot() (the odds-consuming call) must never
        be invoked at all."""
        self._wire_common(monkeypatch)
        monkeypatch.setattr(live_engine, "_fetch_live_games", lambda today: [{"gamePk": 1}])
        monkeypatch.setattr(live_engine, "_parse_game_state", lambda g: _live_state())
        with patch.object(live_engine, "full_market_snapshot") as mock_snapshot:
            result = live_engine.run_live_pass()
        assert result["skipped"] is True
        mock_snapshot.assert_not_called()


class TestRunLivePassMidPassQuotaTrip:
    """A guard trip observed after game 1's market call must stop game 2
    from being polled in the SAME pass, not just the next one."""

    def test_stops_polling_further_games_once_tripped(self, monkeypatch, _isolated_db):
        _log_bet(_isolated_db, bet="Boston Red Sox", game="Tampa Bay Rays @ Boston Red Sox", date=_today_et())
        _log_bet(_isolated_db, bet="New York Yankees", game="New York Yankees @ Baltimore Orioles",
                 date=_today_et())
        monkeypatch.setattr(live_engine, "_in_live_window", lambda: True)
        monkeypatch.setattr(live_engine, "is_drawdown_pause", lambda: False)
        monkeypatch.setattr(live_engine, "analyze_bullpen", lambda *a, **k: {})
        monkeypatch.setattr(live_engine, "_fetch_scoring_plays", lambda gp: [])
        monkeypatch.setattr(live_engine, "run_live_cycle", lambda *a, **k: [])
        monkeypatch.setattr(live_engine, "_update_dashboard", lambda *a, **k: None)
        monkeypatch.setattr(live_engine, "get_mlb_events", lambda: [])
        monkeypatch.setattr(
            live_engine, "_fetch_live_games",
            lambda today: [{"gamePk": 1}, {"gamePk": 2}],
        )

        def _parse(g):
            if g["gamePk"] == 1:
                return _live_state(away="Tampa Bay Rays", home="Boston Red Sox", game_pk=1)
            return _live_state(away="New York Yankees", home="Baltimore Orioles", game_pk=2)

        monkeypatch.setattr(live_engine, "_parse_game_state", _parse)

        market_calls = []
        monkeypatch.setattr(
            live_engine, "full_market_snapshot",
            lambda *a, **k: market_calls.append(a) or {},
        )

        # First game's call reveals we've already crossed 80% -- trip on it.
        with patch.object(odds_quota, "get_last_snapshot", return_value={"pct_used": 0.85}), \
             patch.object(odds_quota, "_send_alert"):
            result = live_engine.run_live_pass()

        assert len(market_calls) == 1, "must stop after the game that crossed the guard, not poll game 2 too"
        assert result["reason"] == "quota guard tripped mid-pass"
        disabled, _ = odds_quota.is_live_engine_disabled()
        assert disabled is True


class TestCadenceNoteLogged:
    def test_cadence_tradeoff_is_logged_every_pass(self, monkeypatch, capsys):
        monkeypatch.setattr(live_engine, "_in_live_window", lambda: False)
        live_engine.run_live_pass()
        out = capsys.readouterr().out
        assert "every 15 min" in out
        assert "5 min" in out
