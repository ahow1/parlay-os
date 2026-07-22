"""Tests for real dollar caps on TOTAL and ER_PROP picks (Step 2.1 audit
follow-up). Before this fix, totals_bets and er_bets had no dollar cap at
all — totals was only trimmed to the top 5 by edge (no budget check), and
ER props bypassed the props-pool admission loop entirely, only limited by
the downstream MAX_PROPS_PER_DAY slot count. Both now share the same PROPS
pool running total as hitter/k/nrfi props (their bet_types are already
bucketed into "PROPS" in bankroll_engine._POOL_BET_TYPES), admitted in
the fixed category order hitter -> k -> nrfi -> er -> totals against one
running total (mirrors the existing hitter/k/nrfi pattern exactly).

Run: python -m pytest test_props_pool_enforcement.py -v
"""

from unittest.mock import patch

import pytest

import brain


class _IsolatedDb:
    """Fresh sqlite DB per test so pool_exposure() sees zero prior bets —
    mirrors the fixture pattern in test_runline.py."""

    @pytest.fixture(autouse=True)
    def _isolated_db(self, tmp_path, request):
        import db
        tmp_db = str(tmp_path / f"{request.node.name}.db")
        with patch.object(db, "DB_PATH", tmp_db):
            db.init_db()
            yield db


def _total_pick(**overrides):
    pick = {
        "game": "SF Giants @ LA Dodgers", "direction": "OVER", "line": 8.5,
        "prob": 0.60, "market_p": 0.524, "edge_pct": 7.6, "stake": 12.0,
        "odds": "-110",
    }
    pick.update(overrides)
    return pick


def _er_pick(**overrides):
    pick = {
        "sp": "Test Pitcher", "team": "SF Giants", "game": "SF Giants @ LA Dodgers",
        "line": 3.5, "direction": "UNDER", "model_p": 0.62, "edge_pct": 12.0,
        "stake": 15.0,
    }
    pick.update(overrides)
    return pick


class TestTotalsPoolCapBlocksOverBudget(_IsolatedDb):
    def test_totals_pool_cap_blocks_pick_over_budget(self):
        """A totals stake that vastly exceeds the PROPS pool at this
        bankroll must be capped, not silently admitted unconditionally."""
        huge_pick = _total_pick(stake=100000.0)
        with patch.object(brain, "DRY_RUN", True):
            ok = brain._daily_bet_slip(
                all_locks=[], all_flips=[], all_props=[], all_fades=[], br=300.0,
                all_totals=[huge_pick],
            )
        assert ok is False

    def test_totals_within_budget_still_reaches_slip(self):
        with patch.object(brain, "DRY_RUN", True):
            ok = brain._daily_bet_slip(
                all_locks=[], all_flips=[], all_props=[], all_fades=[], br=300.0,
                all_totals=[_total_pick(stake=12.0)],
            )
        assert ok is True


class TestErPropPoolCapBlocksOverBudget(_IsolatedDb):
    def test_er_prop_pool_cap_blocks_pick_over_budget(self):
        """An ER prop stake that vastly exceeds the PROPS pool must be
        capped — previously this hole meant ER props bypassed any dollar
        cap entirely."""
        huge_pick = _er_pick(stake=100000.0)
        with patch.object(brain, "DRY_RUN", True):
            ok = brain._daily_bet_slip(
                all_locks=[], all_flips=[], all_props=[], all_fades=[], br=300.0,
                all_er_props=[huge_pick],
            )
        assert ok is False

    def test_er_prop_within_budget_still_reaches_slip(self, capsys):
        with patch.object(brain, "DRY_RUN", True):
            ok = brain._daily_bet_slip(
                all_locks=[], all_flips=[], all_props=[], all_fades=[], br=300.0,
                all_er_props=[_er_pick(stake=15.0)],
            )
        assert ok is True
        out = capsys.readouterr().out
        assert "Test Pitcher" in out
        assert "ER U3.5" in out


class TestPropsPoolSharedAcrossCategories(_IsolatedDb):
    """PROPS pool at br=300 is $54 (daily_budget $90 * POOL_PROPS 0.60).
    Categories are admitted in the fixed order hitter -> k -> nrfi -> er ->
    totals against ONE shared running total — a category earlier in that
    order can exhaust the pool and leave nothing for ER/totals even when
    they'd otherwise qualify, matching the existing hitter/k/nrfi pattern."""

    def test_hitter_prop_consuming_most_of_pool_leaves_no_room_for_er_prop(self, capsys):
        hitter_pick = {
            "player": "Test Hitter", "team": "SF Giants", "prop": "Hits O1.5",
            "model_prob": 0.60, "market_p": 0.50, "edge_pct": 10.0, "stake": 50.0,
        }
        er_pick = _er_pick(stake=15.0)   # $50 + $15 = $65 > $54 pool -> ER cut
        with patch.object(brain, "DRY_RUN", True):
            ok = brain._daily_bet_slip(
                all_locks=[], all_flips=[], all_props=[], all_fades=[], br=300.0,
                all_hitter_props=[hitter_pick], all_er_props=[er_pick],
            )
        assert ok is True
        out = capsys.readouterr().out
        assert "Test Hitter" in out
        assert "Test Pitcher" not in out

    def test_er_prop_admitted_when_pool_has_room_after_hitter_prop(self, capsys):
        hitter_pick = {
            "player": "Test Hitter", "team": "SF Giants", "prop": "Hits O1.5",
            "model_prob": 0.60, "market_p": 0.50, "edge_pct": 10.0, "stake": 30.0,
        }
        er_pick = _er_pick(stake=15.0)   # $30 + $15 = $45 <= $54 pool -> both admitted
        with patch.object(brain, "DRY_RUN", True):
            ok = brain._daily_bet_slip(
                all_locks=[], all_flips=[], all_props=[], all_fades=[], br=300.0,
                all_hitter_props=[hitter_pick], all_er_props=[er_pick],
            )
        assert ok is True
        out = capsys.readouterr().out
        assert "Test Hitter" in out
        assert "Test Pitcher" in out


class TestExistingPropsBehaviorUnaffected(_IsolatedDb):
    """ER/TOTAL pool wiring must be additive only — no ER/TOTAL picks
    reproduces prior slip behavior exactly."""

    def test_no_qualifying_picks_still_returns_false(self):
        with patch.object(brain, "DRY_RUN", True):
            ok = brain._daily_bet_slip(
                all_locks=[], all_flips=[], all_props=[], all_fades=[], br=300.0,
            )
        assert ok is False
