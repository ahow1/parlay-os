"""Tests for the --settle one-shot CLI mode and Telegram notification dedup.

Background: run_settlement_check() previously sent a Telegram grading
message and settled a bet's result in the same step, with nothing
tracking whether the message actually went out. If it ran more than once
over the same bet (e.g. concurrent GitHub Actions runs, a manual rerun),
or if a bet was graded through some other path (manual /settle) without
ever getting a Telegram ping, there was no way to tell "already notified"
from "not yet notified". notified_at (nullable TIMESTAMP on bets) is the
fix: NULL means not yet notified. run_settlement_check() now has two
passes -- settle-and-notify for pending bets, and a notify-only backlog
pass for bets that already have a result but never got the ping -- and
both stamp notified_at so a bet is never notified twice.

Run: python -m pytest test_settle_notify.py -v
"""

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

import db


@pytest.fixture(autouse=True)
def _isolated_db(tmp_path):
    tmp_db = str(tmp_path / "settle_test.db")
    with patch.object(db, "DB_PATH", tmp_db):
        db.init_db()
        yield db


def _log(bet="Boston Red Sox", game="Tampa Bay Rays @ Boston Red Sox",
          bet_type="ML", date="2026-07-19", **overrides):
    kwargs = dict(
        date=date, bet=bet, bet_type=bet_type, game=game,
        sp="", park="BOS", umpire="", bet_odds="-120",
        model_prob=0.55, market_prob=0.50, edge_pct=5.0,
        conviction="MEDIUM", stake=25.0,
    )
    kwargs.update(overrides)
    db.log_bet(**kwargs)


def _final_game(away_name="Tampa Bay Rays", home_name="Boston Red Sox",
                 away_score=2, home_score=5):
    return {
        "teams": {
            "away": {"team": {"name": away_name}, "score": away_score},
            "home": {"team": {"name": home_name}, "score": home_score},
        },
        "status": {"detailedState": "Final"},
    }


class TestSchema:
    def test_notified_at_column_exists(self):
        with db._conn() as conn:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(bets)")}
        assert "notified_at" in cols

    def test_new_bet_has_null_notified_at(self):
        _log()
        row = db.get_bets()[0]
        assert row["notified_at"] is None


class TestPropSkip:
    """PROP bets have no automated outcome-determination path -- must be
    skipped explicitly during settlement, not silently swallowed by the
    generic 'couldn't determine outcome' branch."""

    def test_pending_prop_bet_is_skipped_not_settled(self, capsys):
        import telegram_handler as th
        _log(bet="Aaron Judge Over 1.5 TB", bet_type="PROP", game="NYY @ BOS")

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_send") as mock_send:
            settled = th.run_settlement_check()

        assert settled == []
        mock_send.assert_not_called()
        row = db.get_bets()[0]
        assert row["result"] is None
        assert "prop settlement not implemented, skipping" in capsys.readouterr().out

    def test_prop_bet_does_not_block_other_bets_in_same_batch(self):
        import telegram_handler as th
        _log(bet="Aaron Judge Over 1.5 TB", bet_type="PROP", game="NYY @ BOS")
        _log(bet="Boston Red Sox", bet_type="ML")

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            settled = th.run_settlement_check()

        assert len(settled) == 1
        assert settled[0]["type"] == "ML"
        mock_send.assert_called_once()


class TestNotifiedAtGuard:
    """A newly-settled bet must get notified_at stamped atomically with
    its result, and exactly one Telegram send."""

    def test_settling_a_pending_bet_stamps_notified_at(self):
        import telegram_handler as th
        _log()

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()

        row = db.get_bets()[0]
        assert row["result"] == "W"
        assert row["notified_at"] is not None
        mock_send.assert_called_once()

    def test_over_cap_bet_stamps_notified_at_without_sending(self):
        import telegram_handler as th
        _log(over_cap=True, stake=0.0)

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="L"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()

        row = db.get_bets()[0]
        assert row["result"] == "L"
        assert row["notified_at"] is not None
        mock_send.assert_not_called()


class TestBacklogPass:
    """Bets already graded through some other path (e.g. manual /settle)
    with result set but notified_at still NULL must get exactly one
    Telegram notification, using the existing result -- no game re-fetch
    or outcome re-determination needed."""

    def test_resolved_bet_missing_notification_gets_one_send(self):
        import telegram_handler as th
        _log()
        row = db.get_bets()[0]
        db.resolve_bet_by_id(bet_id=row["id"], closing_odds="-110",
                              result="W", game_score="BOS 5-2 TB")
        assert db.get_bets()[0]["notified_at"] is None  # sanity: old-style write, no stamp

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[]), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            settled = th.run_settlement_check()

        assert len(settled) == 1
        assert settled[0]["backlog_notify"] is True
        mock_send.assert_called_once()
        assert db.get_bets()[0]["notified_at"] is not None

    def test_backlog_over_cap_bet_marked_notified_without_sending(self):
        import telegram_handler as th
        _log(over_cap=True, stake=0.0)
        row = db.get_bets()[0]
        db.resolve_bet_by_id(bet_id=row["id"], closing_odds="-110",
                              result="L", game_score="BOS 1-4 TB")

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[]), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()

        mock_send.assert_not_called()
        assert db.get_bets()[0]["notified_at"] is not None


class TestIdempotency:
    """Running settlement twice must send exactly one Telegram message per
    bet, no matter which pass (pending or backlog) settled/notified it."""

    def test_running_twice_sends_exactly_one_message(self):
        import telegram_handler as th
        _log()

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()
            th.run_settlement_check()

        mock_send.assert_called_once()

    def test_second_run_returns_empty_settled_list(self):
        import telegram_handler as th
        _log()

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send"), \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()
            second = th.run_settlement_check()

        assert second == []


class TestDaysBackFilter:
    """days_back bounds only the pending-settlement pass (real MLB API
    calls) -- the default (None) is unbounded, matching this function's
    original behavior so existing callers/tests are unaffected. The
    backlog notify pass is never bounded since it makes no API call."""

    def test_none_default_settles_bet_regardless_of_age(self):
        import telegram_handler as th
        old_date = (datetime.now(th.ET).date() - timedelta(days=30)).isoformat()
        _log(date=old_date)

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            settled = th.run_settlement_check()

        assert len(settled) == 1
        mock_send.assert_called_once()

    def test_days_back_excludes_older_pending_bet(self):
        import telegram_handler as th
        old_date = (datetime.now(th.ET).date() - timedelta(days=30)).isoformat()
        _log(date=old_date)

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games") as mock_fetch, \
             patch.object(th, "_send") as mock_send:
            settled = th.run_settlement_check(days_back=3)

        assert settled == []
        mock_fetch.assert_not_called()
        mock_send.assert_not_called()

    def test_days_back_includes_recent_pending_bet(self):
        import telegram_handler as th
        recent_date = (datetime.now(th.ET).date() - timedelta(days=1)).isoformat()
        _log(date=recent_date)

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            settled = th.run_settlement_check(days_back=3)

        assert len(settled) == 1
        mock_send.assert_called_once()


class TestManualSettlePaths:
    """handle_settle() (/win /loss /push Telegram commands) and the
    dashboard /api/settle + /api/resolve endpoints all resolve a bet
    outside the automated settle_bets flow. Each must stamp notified_at
    itself -- otherwise the next settle_bets run finds a resolved bet
    with notified_at still NULL and fires a spurious AUTO-SETTLE ping
    through the backlog-notify pass."""

    def test_handle_settle_stamps_notified_at(self):
        import telegram_handler as th
        _log()
        row = db.get_bets()[0]

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "sync_scout_json"):
            th.handle_settle("W", str(row["id"]))

        settled = db.get_bets()[0]
        assert settled["result"] == "W"
        assert settled["notified_at"] is not None

        # A subsequent settle_bets run must not re-notify via the backlog pass.
        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[]), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "_update_clv_log"):
            backlog_settled = th.run_settlement_check()

        assert backlog_settled == []
        mock_send.assert_not_called()

    def test_api_settle_stamps_notified_at(self):
        import api

        _log()
        row = db.get_bets()[0]

        with patch.object(api, "_db", db):
            client = api.app.test_client()
            resp = client.post("/api/settle", json={
                "bet_id": row["id"], "result": "W",
            })

        assert resp.status_code == 200
        settled = db.get_bets()[0]
        assert settled["result"] == "W"
        assert settled["notified_at"] is not None

    def test_api_resolve_stamps_notified_at(self):
        import api

        _log()
        row = db.get_bets()[0]

        with patch.object(api, "_db", db):
            client = api.app.test_client()
            resp = client.post("/api/resolve", json={
                "bet": row["bet"], "date": row["date"], "result": "win",
            })

        assert resp.status_code == 200
        settled = db.get_bets()[0]
        assert settled["result"] == "W"
        assert settled["notified_at"] is not None

    def test_backlog_pass_does_not_refire_after_any_manual_path(self):
        """settle_bets is the AUTO-SETTLE source -- once notified_at is
        stamped by any manual path, the backlog pass must skip the bet."""
        import telegram_handler as th
        import api

        _log()
        row = db.get_bets()[0]
        with patch.object(api, "_db", db):
            api.app.test_client().post("/api/settle", json={
                "bet_id": row["id"], "result": "L",
            })

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[]), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "_update_clv_log"):
            settled = th.run_settlement_check()

        assert settled == []
        mock_send.assert_not_called()


class TestRunSettleOneShot:
    """brain._run_settle() -- the --settle CLI entrypoint."""

    def test_calls_run_settlement_check_with_days_back_3(self):
        import brain
        with patch("telegram_handler.run_settlement_check", return_value=[{"id": 1}]) as mock_check:
            brain._run_settle()
        mock_check.assert_called_once_with(days_back=3)

    def test_swallows_exceptions_without_raising(self):
        import brain
        with patch("telegram_handler.run_settlement_check", side_effect=RuntimeError("boom")):
            brain._run_settle()  # must not raise
