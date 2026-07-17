"""Tests for RUNLINE (±1.5 MLB run line) end-to-end wiring.

Covers: run_line_prob() math, edge calc against SGO spreads consensus,
pool bucket sizing (mirrors ML), slip generation + Telegram send with a
stake, bets-table persistence, auto-settlement (margin vs +/-1.5), and
CLV grading against the spread consensus (not moneyline).

Run: python -m pytest test_runline.py -v
"""

import inspect
from unittest.mock import patch

import pytest

import bankroll_engine
import brain
import sportsgameodds_client as sgo
from props_engine import run_line_prob


# ── Math: run_line_prob() ───────────────────────────────────────────────────

class TestRunLineProbMath:
    def test_probs_are_complementary(self):
        r = run_line_prob(4.6, 4.1)
        assert abs(r["away_minus_1_5"] + r["home_plus_1_5"] - 1.0) < 1e-6
        assert abs(r["home_minus_1_5"] + r["away_plus_1_5"] - 1.0) < 1e-6

    def test_heavy_favorite_more_likely_to_cover_than_dog_is_to_win_by_2(self):
        r = run_line_prob(5.5, 3.0)
        assert r["away_minus_1_5"] > r["home_minus_1_5"]

    def test_even_matchup_is_symmetric(self):
        r = run_line_prob(4.3, 4.3)
        assert r["away_minus_1_5"] == r["home_minus_1_5"]
        assert r["away_plus_1_5"] == r["home_plus_1_5"]

    def test_probabilities_bounded_zero_one(self):
        for away_xr, home_xr in [(0.5, 9.0), (9.0, 0.5), (4.4, 4.4)]:
            r = run_line_prob(away_xr, home_xr)
            for v in r.values():
                assert 0.0 <= v <= 1.0


# ── Pool bucket mirrors ML ───────────────────────────────────────────────────

class TestRunlinePoolBucketMirrorsMl:
    def test_pool_runline_equals_pool_ml(self):
        assert bankroll_engine.POOL_RUNLINE == bankroll_engine.POOL_ML

    def test_pool_budget_matches_ml_budget(self):
        assert (bankroll_engine.pool_budget("RUNLINE", 300.0)
                == bankroll_engine.pool_budget("ML", 300.0))

    def test_pool_bet_types_registered(self):
        types = bankroll_engine._POOL_BET_TYPES["RUNLINE"]
        assert "RUNLINE-1.5" in types
        assert "RUNLINE+1.5" in types

    def test_pool_exposure_counts_pending_runline_bets(self, tmp_path):
        import db
        tmp_db = str(tmp_path / "runline_pool.db")
        with patch.object(db, "DB_PATH", tmp_db):
            db.init_db()
            db.log_bet(
                date=db.datetime.now(db.ET).strftime("%Y-%m-%d"),
                bet="New York Yankees", bet_type="RUNLINE-1.5",
                game="New York Yankees @ Boston Red Sox", sp="", park="", umpire="",
                bet_odds="-116", model_prob=0.60, market_prob=0.50,
                edge_pct=10.0, conviction="HIGH", stake=35.0,
            )
            with patch.object(bankroll_engine, "_db", db):
                exposure = bankroll_engine.pool_exposure("RUNLINE")
        assert exposure == 35.0


# ── Slip generation: reaches Telegram with a stake ──────────────────────────

class TestRunlineReachesSlipWithStake:
    @pytest.fixture(autouse=True)
    def _isolated_db(self, tmp_path):
        import db
        tmp_db = str(tmp_path / "runline_slip.db")
        with patch.object(db, "DB_PATH", tmp_db):
            db.init_db()
            yield db

    def _pick(self, **overrides):
        pick = {
            "game": "New York Yankees @ Boston Red Sox",
            "team": "New York Yankees",
            "line": -1.5,
            "prob": 0.62,
            "market_p": 0.50,
            "edge_pct": 12.0,
            "conviction": "HIGH",
            "stake": 35.0,
            "odds": "+140",
            "bet_type": "RUNLINE-1.5",
        }
        pick.update(overrides)
        return pick

    def test_runline_pick_sent_with_a_stake(self, capsys):
        with patch.object(brain, "DRY_RUN", True):
            ok = brain._daily_bet_slip(
                all_locks=[], all_flips=[], all_props=[], all_fades=[], br=300.0,
                all_runline=[self._pick()],
            )
        assert ok is True
        out = capsys.readouterr().out
        assert "RUN LINE" in out
        assert "New York Yankees -1.5" in out
        assert "$35.00" in out

    def test_zero_stake_runline_pick_is_dropped(self, capsys):
        with patch.object(brain, "DRY_RUN", True):
            ok = brain._daily_bet_slip(
                all_locks=[], all_flips=[], all_props=[], all_fades=[], br=300.0,
                all_runline=[self._pick(stake=0.0)],
            )
        assert ok is False
        out = capsys.readouterr().out
        assert "RUN LINE" not in out

    def test_runline_alone_satisfies_has_any_pick_gate(self):
        """A day with zero ML/props/totals but one qualifying run-line pick
        must still send — RUNLINE must be part of the has-any-pick gate."""
        with patch.object(brain, "DRY_RUN", True):
            ok = brain._daily_bet_slip(
                all_locks=[], all_flips=[], all_props=[], all_fades=[], br=300.0,
                all_nrfi=[], all_totals=[], all_runline=[self._pick()],
            )
        assert ok is True

    def test_runline_pool_cap_blocks_picks_over_budget(self):
        """Second pick would blow the RUNLINE pool budget at this bankroll —
        must be capped, not silently overspent."""
        huge_pick = self._pick(stake=100000.0)
        with patch.object(brain, "DRY_RUN", True):
            ok = brain._daily_bet_slip(
                all_locks=[], all_flips=[], all_props=[], all_fades=[], br=300.0,
                all_runline=[huge_pick],
            )
        # $100000 stake vastly exceeds the RUNLINE pool at $300 bankroll —
        # capped out entirely -> no qualifying picks -> nothing sent.
        assert ok is False


# ── DB persistence: logs as RUNLINE ─────────────────────────────────────────

class TestRunlinePersistsToBetsTable:
    @pytest.fixture(autouse=True)
    def _isolated_db(self, tmp_path):
        import db
        tmp_db = str(tmp_path / "runline_persist.db")
        with patch.object(db, "DB_PATH", tmp_db):
            db.init_db()
            yield db

    def test_runline_pick_gets_a_bets_row(self, _isolated_db):
        pick = {
            "game": "New York Yankees @ Boston Red Sox",
            "team": "New York Yankees",
            "line": -1.5,
            "prob": 0.62,
            "market_p": 0.50,
            "edge_pct": 12.0,
            "conviction": "HIGH",
            "stake": 35.0,
            "odds": "+140",
            "bet_type": "RUNLINE-1.5",
        }
        brain._daily_bet_slip(
            all_locks=[], all_flips=[], all_props=[], all_fades=[], br=300.0,
            all_runline=[pick],
        )
        rows = [b for b in _isolated_db.get_bets() if (b.get("type") or "").startswith("RUNLINE")]
        assert len(rows) == 1
        assert rows[0]["type"] == "RUNLINE-1.5"
        assert rows[0]["bet"] == "New York Yankees"
        assert rows[0]["stake"] == 35.0


# ── Byte-identical output for existing bet types ────────────────────────────

class TestExistingBetTypesUnaffected:
    """RUNLINE must be additive only — omitting it (as every existing caller
    does) must reproduce prior behavior exactly."""

    @pytest.fixture(autouse=True)
    def _isolated_db(self, tmp_path):
        import db
        tmp_db = str(tmp_path / "runline_existing.db")
        with patch.object(db, "DB_PATH", tmp_db):
            db.init_db()
            yield db

    def test_no_qualifying_picks_without_runline_still_returns_false(self):
        with patch.object(brain, "DRY_RUN", True):
            ok = brain._daily_bet_slip(
                all_locks=[], all_flips=[], all_props=[], all_fades=[], br=300.0,
            )
        assert ok is False

    def test_totals_only_slip_output_has_no_runline_section(self, capsys):
        totals_pick = {
            "game": "SF Giants @ LA Dodgers", "direction": "OVER", "line": 8.5,
            "prob": 0.60, "market_p": 0.524, "edge_pct": 7.6, "stake": 12.0,
            "odds": "-110",
        }
        with patch.object(brain, "DRY_RUN", True):
            ok = brain._daily_bet_slip(
                all_locks=[], all_flips=[], all_props=[], all_fades=[], br=300.0,
                all_totals=[totals_pick],
            )
        assert ok is True
        out = capsys.readouterr().out
        assert "TOTALS (1 bets):" in out
        assert "RUN LINE" not in out


# ── Auto-settlement: margin vs +/-1.5 ───────────────────────────────────────

class TestDetermineOutcomeRunline:
    @staticmethod
    def _game(away_score, home_score):
        return {"teams": {"away": {"score": away_score}, "home": {"score": home_score}}}

    def test_favorite_covers_when_winning_by_2_or_more(self):
        from telegram_handler import _determine_outcome
        assert _determine_outcome({"type": "RUNLINE-1.5"}, self._game(5, 2), "away") == "W"

    def test_favorite_fails_to_cover_when_winning_by_1(self):
        from telegram_handler import _determine_outcome
        assert _determine_outcome({"type": "RUNLINE-1.5"}, self._game(3, 2), "away") == "L"

    def test_favorite_loses_outright_is_a_loss(self):
        from telegram_handler import _determine_outcome
        assert _determine_outcome({"type": "RUNLINE-1.5"}, self._game(1, 4), "away") == "L"

    def test_underdog_covers_on_narrow_loss(self):
        from telegram_handler import _determine_outcome
        assert _determine_outcome({"type": "RUNLINE+1.5"}, self._game(3, 2), "home") == "W"

    def test_underdog_fails_to_cover_on_loss_by_2_or_more(self):
        from telegram_handler import _determine_outcome
        assert _determine_outcome({"type": "RUNLINE+1.5"}, self._game(4, 2), "home") == "L"

    def test_underdog_covers_on_outright_win(self):
        from telegram_handler import _determine_outcome
        assert _determine_outcome({"type": "RUNLINE+1.5"}, self._game(1, 5), "home") == "W"

    def test_malformed_line_returns_none_not_crash(self):
        from telegram_handler import _determine_outcome
        assert _determine_outcome({"type": "RUNLINEXYZ"}, self._game(5, 2), "away") is None

    def test_existing_ml_and_totals_branches_unaffected(self):
        """Regression guard: adding the RUNLINE branch must not shift ML/F5/O/U."""
        from telegram_handler import _determine_outcome
        assert _determine_outcome({"type": "ML"}, self._game(5, 2), "away") == "W"
        assert _determine_outcome({"type": "O8.5"}, self._game(5, 4), "away") == "W"
        assert _determine_outcome({"type": "U8.5"}, self._game(2, 1), "away") == "W"


# ── CLV grading: spread consensus, not moneyline ────────────────────────────

class TestClvGradesAgainstSpreadConsensus:
    @staticmethod
    def _fake_slate():
        event = {
            "away": "New York Yankees", "home": "Boston Red Sox",
            "spreads": {
                "away_line": -1.5, "home_line": 1.5,
                "away": {"draftkings": {"american": -130}, "fanduel": {"american": -125}},
                "home": {"draftkings": {"american": 110}, "fanduel": {"american": 105}},
            },
        }
        return {"evtX": event}

    def test_sgo_grading_uses_spreads_market_not_moneyline(self):
        import telegram_handler as th
        slate = self._fake_slate()
        expected = sgo.no_vig_consensus(slate["evtX"], market="spreads")["away_american"]
        with patch("sportsgameodds_client.fetch_mlb_slate", return_value=slate):
            closing = th._fetch_closing_odds_sgo("NYY", "RUNLINE-1.5")
        assert closing == expected

    def test_sgo_grading_underdog_side(self):
        import telegram_handler as th
        slate = self._fake_slate()
        expected = sgo.no_vig_consensus(slate["evtX"], market="spreads")["home_american"]
        with patch("sportsgameodds_client.fetch_mlb_slate", return_value=slate):
            closing = th._fetch_closing_odds_sgo("BOS", "RUNLINE+1.5")
        assert closing == expected

    def test_dispatcher_selects_spreads_market_key_for_oddsapi(self):
        """The-odds-api fallback path must request the 'spreads' market for
        a RUNLINE bet_type, not 'h2h' (moneyline)."""
        import telegram_handler as th
        with patch.object(th, "ODDS_API_KEY", "fake-key"), \
             patch("telegram_handler._http_get") as mock_get:
            mock_get.return_value.raise_for_status = lambda: None
            mock_get.return_value.json = lambda: []
            th._fetch_closing_odds_oddsapi("NYY", "RUNLINE-1.5")
        _, kwargs = mock_get.call_args
        assert kwargs["params"]["markets"] == "spreads"

    def test_moneyline_grading_still_uses_moneyline_market(self):
        """Regression guard: RUNLINE dispatch must not leak into ML grading."""
        import telegram_handler as th
        slate = {
            "evtX": {
                "away": "New York Yankees", "home": "Boston Red Sox",
                "moneyline": {
                    "away": {"draftkings": {"american": 130}},
                    "home": {"draftkings": {"american": -150}},
                },
            }
        }
        expected = sgo.no_vig_consensus(slate["evtX"], market="moneyline")["away_american"]
        with patch("sportsgameodds_client.fetch_mlb_slate", return_value=slate):
            closing = th._fetch_closing_odds_sgo("NYY", "ML")
        assert closing == expected


# ── Wiring present in the scout loop (source-level regression guard) ───────

class TestRunlineWiredIntoScoutLoop:
    """run_daily_scout() is never executed directly in tests (it's a huge
    end-to-end orchestrator — every existing test mocks or source-inspects
    it instead, see test_money_bugs.py/test_fixes.py). Confirm the run-line
    generation block is actually present and reachable, matching that
    existing convention."""

    def test_run_line_prob_and_spreads_consensus_used_in_scout(self):
        src = inspect.getsource(brain.run_daily_scout)
        assert "run_line_prob(" in src
        assert 'no_vig_consensus(_rl_event, market="spreads")' in src
        assert "all_runline.append(" in src
