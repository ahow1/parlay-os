"""Tests for over_cap pick tracking: picks that qualify but can't be staked
because the daily cap (ML) or props/runline pool budget is already spent.

Covers: schema, log_bet()'s stake=0 enforcement and the real-pick-supersedes-
phantom collision fix, the brain.py logging wrappers, CLV grading, auto-
settlement (incl. the Telegram suppression), and bankroll non-impact.

Run: python -m pytest test_over_cap_picks.py -v
"""

import pytest
from unittest.mock import patch, MagicMock

import db


@pytest.fixture(autouse=True)
def _isolated_db(tmp_path):
    tmp_db = str(tmp_path / "over_cap_test.db")
    with patch.object(db, "DB_PATH", tmp_db):
        db.init_db()
        yield db


def _log_real(bet="Boston Red Sox", game="Tampa Bay Rays @ Boston Red Sox", **overrides):
    kwargs = dict(
        date="2026-07-19", bet=bet, bet_type="ML", game=game,
        sp="", park="BOS", umpire="", bet_odds="-120",
        model_prob=0.55, market_prob=0.50, edge_pct=5.0, conviction="MEDIUM", stake=25.0,
    )
    kwargs.update(overrides)
    db.log_bet(**kwargs)


def _log_over_cap(bet="Minnesota Twins", game="Minnesota Twins @ Cleveland Guardians", **overrides):
    kwargs = dict(
        date="2026-07-19", bet=bet, bet_type="ML", game=game,
        sp="Joe Ryan", park="CLE", umpire="Alex MacKay", bet_odds="+120",
        model_prob=0.509, market_prob=0.392, edge_pct=11.7, conviction="MEDIUM",
        stake=25.0, over_cap=True,   # deliberately nonzero -- must be forced to 0
    )
    kwargs.update(overrides)
    db.log_bet(**kwargs)


class TestSchema:
    def test_over_cap_column_exists(self):
        with db._conn() as conn:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(bets)")}
        assert "over_cap" in cols

    def test_real_pick_defaults_over_cap_to_zero(self):
        _log_real()
        row = db.get_bets()[0]
        assert row["over_cap"] == 0


class TestLogBetOverCap:
    def test_stake_forced_to_zero_even_if_caller_passes_nonzero(self):
        _log_over_cap()
        row = db.get_bets()[0]
        assert row["over_cap"] == 1
        assert row["stake"] == 0.0

    def test_all_other_fields_preserved_same_as_a_real_pick(self):
        _log_over_cap()
        row = db.get_bets()[0]
        assert row["bet"] == "Minnesota Twins"
        assert row["type"] == "ML"
        assert row["game"] == "Minnesota Twins @ Cleveland Guardians"
        assert row["sp"] == "Joe Ryan"
        assert row["bet_odds"] == "+120"
        assert row["model_prob"] == 0.509
        assert row["market_prob"] == 0.392
        assert row["edge_pct"] == 11.7
        assert row["conviction"] == "MEDIUM"
        assert row["result"] is None   # pending, same as a real unsettled pick

    def test_over_cap_prop_pick_logs_correctly(self):
        db.log_bet(
            date="2026-07-19", bet="Chase DeLauter TB O1.5", bet_type="PROP",
            game="", sp="", park="", umpire="", bet_odds="+158",
            model_prob=0.835, market_prob=0.388, edge_pct=44.7,
            conviction="LOCK", stake=20.0, over_cap=True,
        )
        row = db.get_bets()[0]
        assert row["type"] == "PROP"
        assert row["over_cap"] == 1
        assert row["stake"] == 0.0
        assert row["edge_pct"] == 44.7


class TestCollisionHandling:
    """bets has a unique index on (date, game, bet, type) and log_bet() uses
    INSERT OR IGNORE -- an over_cap phantom row must never cause a later real
    pick at the same key to be silently dropped (e.g. cap frees up on a
    same-day rerun)."""

    def test_real_pick_supersedes_earlier_over_cap_phantom(self):
        _log_over_cap(bet="Boston Red Sox", game="Tampa Bay Rays @ Boston Red Sox")
        _log_real(bet="Boston Red Sox", game="Tampa Bay Rays @ Boston Red Sox", stake=25.0)

        rows = db.get_bets()
        assert len(rows) == 1
        assert rows[0]["over_cap"] == 0
        assert rows[0]["stake"] == 25.0

    def test_over_cap_pick_does_not_clobber_an_existing_real_pick(self):
        _log_real(bet="Boston Red Sox", game="Tampa Bay Rays @ Boston Red Sox", stake=25.0)
        _log_over_cap(bet="Boston Red Sox", game="Tampa Bay Rays @ Boston Red Sox")

        rows = db.get_bets()
        assert len(rows) == 1
        assert rows[0]["over_cap"] == 0
        assert rows[0]["stake"] == 25.0

    def test_duplicate_over_cap_logging_is_idempotent(self):
        _log_over_cap()
        _log_over_cap()
        rows = [r for r in db.get_bets() if r["bet"] == "Minnesota Twins"]
        assert len(rows) == 1

    def test_settled_over_cap_row_is_never_deleted(self):
        """The supersede-fix only ever removes an UNSETTLED phantom row --
        once an over_cap pick is graded (result set), it must survive
        forever, exactly like a real settled bet would.

        Note: a same-key real insert *after* that point is silently ignored
        by the pre-existing (date, game, bet, type) unique index + INSERT OR
        IGNORE -- the same limitation real-vs-real doubleheader picks already
        have today (the `game` field has no game-instance disambiguator).
        That's a pre-existing, out-of-scope limitation this fix doesn't
        change; what matters here is the settled row itself is untouched."""
        _log_over_cap(bet="Cleveland Guardians", game="Minnesota Twins @ Cleveland Guardians")
        row = db.get_bets()[0]
        db.resolve_bet_by_id(bet_id=row["id"], closing_odds="+115", result="L", game_score="4-2")

        _log_real(bet="Cleveland Guardians", game="Minnesota Twins @ Cleveland Guardians", stake=30.0)

        rows = db.get_bets()
        assert len(rows) == 1
        assert rows[0]["over_cap"] == 1
        assert rows[0]["result"] == "L"
        assert rows[0]["closing_odds"] == "+115"


class TestBrainWrappers:
    """brain.py's _log_bet_with_retry / _log_pick_with_retry must force
    stake=0 and pass over_cap=True through to db.log_bet regardless of what
    the caller's underlying data says."""

    def _analysis(self):
        return {
            "away_name": "Minnesota Twins", "home_name": "Cleveland Guardians",
            "home": "CLE", "umpire": "Alex MacKay",
            "away_stake": 25.0, "away_model_p": 0.509, "away_nv": 0.392,
            "away_edge": 11.7, "best_away_odds": "+120",
            "away_sp": {"name": "Joe Ryan"},
        }

    def test_log_bet_with_retry_forces_stake_zero_and_over_cap_flag(self):
        import brain
        with patch("brain._db.log_bet") as mock_log:
            ok = brain._log_bet_with_retry("2026-07-19", self._analysis(), "away", "MEDIUM", over_cap=True)
        assert ok is True
        _, kwargs = mock_log.call_args
        assert kwargs["stake"] == 0.0
        assert kwargs["over_cap"] is True
        assert kwargs["model_prob"] == 0.509
        assert kwargs["edge_pct"] == 11.7

    def test_log_bet_with_retry_over_cap_false_is_unaffected(self):
        import brain
        with patch("brain._db.log_bet") as mock_log:
            ok = brain._log_bet_with_retry("2026-07-19", self._analysis(), "away", "MEDIUM")
        assert ok is True
        _, kwargs = mock_log.call_args
        assert kwargs["stake"] == 25.0
        assert kwargs["over_cap"] is False

    def test_log_pick_with_retry_forces_stake_zero_and_over_cap_flag(self):
        import brain
        with patch("brain._db.log_bet") as mock_log:
            ok = brain._log_pick_with_retry(
                "PROP", date="2026-07-19", bet="Chase DeLauter TB O1.5", game="",
                bet_odds="+158", model_prob=0.835, market_prob=0.388,
                edge_pct=44.7, conviction="LOCK", stake=20.0, over_cap=True,
            )
        assert ok is True
        _, kwargs = mock_log.call_args
        assert kwargs["stake"] == 0.0
        assert kwargs["over_cap"] is True


class TestClvGrading:
    """The CLV grader (bankroll_engine.capture_pre_game_clv) must grade
    over_cap picks the same way it grades staked ones -- no filter on stake."""

    def test_over_cap_pending_bet_gets_a_clv_row(self):
        import bankroll_engine
        _log_over_cap()
        with patch("telegram_handler._fetch_closing_odds", return_value="+130"):
            written = bankroll_engine.capture_pre_game_clv()
        assert written == 1
        rows = db.get_clv_log(days=1)
        assert len(rows) == 1
        assert rows[0]["bet"] == "Minnesota Twins"


class TestAutoSettlement:
    """run_settlement_check() must resolve/grade over_cap bets exactly like
    real ones (result, closing_odds, clv_pct, settled_log/CLV-log entry) but
    never send a Telegram AUTO-SETTLE ping for them."""

    def _fake_final_game(self):
        return {
            "teams": {
                "away": {"team": {"name": "Minnesota Twins"}, "score": 5},
                "home": {"team": {"name": "Cleveland Guardians"}, "score": 2},
            },
            "status": {"detailedState": "Final"},
        }

    def test_over_cap_bet_settled_silently(self):
        import telegram_handler as th
        _log_over_cap()

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[self._fake_final_game()]), \
             patch.object(th, "_game_side", return_value="away"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="+130"), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log") as mock_clv_log:
            settled = th.run_settlement_check()

        assert len(settled) == 1
        assert settled[0]["over_cap"] == 1
        mock_send.assert_not_called()          # no Telegram ping for a phantom pick
        mock_clv_log.assert_called_once()       # CLV log still updated, same as a real bet

        row = db.get_bets()[0]
        assert row["result"] == "W"
        assert row["profit"] == 0.0             # stake=0 -> zero P&L regardless of outcome

    def test_real_bet_in_same_batch_still_gets_telegram_ping(self):
        import telegram_handler as th
        _log_real(bet="Minnesota Twins", game="Minnesota Twins @ Cleveland Guardians")

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[self._fake_final_game()]), \
             patch.object(th, "_game_side", return_value="away"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="+130"), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()

        mock_send.assert_called_once()


class TestBankrollUnaffected:
    def test_over_cap_win_does_not_move_current_bankroll(self):
        import bankroll_engine
        with patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("BANKROLL_OVERRIDE", None)
            baseline = bankroll_engine.current_bankroll()

            _log_over_cap()
            row = db.get_bets()[0]
            db.resolve_bet_by_id(bet_id=row["id"], closing_odds="+130", result="W", game_score="5-2")

            after = bankroll_engine.current_bankroll()
        assert after == baseline

    def test_over_cap_loss_does_not_move_current_bankroll(self):
        import bankroll_engine
        with patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("BANKROLL_OVERRIDE", None)
            baseline = bankroll_engine.current_bankroll()

            _log_over_cap()
            row = db.get_bets()[0]
            db.resolve_bet_by_id(bet_id=row["id"], closing_odds="+130", result="L", game_score="1-5")

            after = bankroll_engine.current_bankroll()
        assert after == baseline
