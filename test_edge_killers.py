"""Tests for TIER 2 silent-edge-killer audit fixes (AUDIT.md B7, B8, M1, M2, M3, M4).
Run: python -m pytest test_edge_killers.py -v
"""

import inspect
from unittest.mock import patch

import pytest


# ── B7: situations never applied to win prob ──────────────────────────────────

class TestSituationsWiredIntoWinProb:
    """B7: brain.py imported a nonexistent get_active_situations() (silently
    failing every game), and check_situations()'s total_away_adj/
    total_home_adj were computed but never applied to the win-prob blend —
    only a flat +8 confidence bump fired on a 3+ situation stack."""

    def test_dead_get_active_situations_import_removed(self):
        import brain
        src = inspect.getsource(brain.analyze_game)
        assert "import get_active_situations" not in src, (
            "get_active_situations() doesn't exist in situations_engine.py — "
            "the import must be removed, not left to silently except:pass"
        )

    def test_total_adj_applied_before_edge_is_computed(self):
        """total_away_adj/total_home_adj must be added to away_model_p/
        home_model_p BEFORE away_edge/home_edge are derived from them —
        otherwise the adjustment never reaches edge, stake, or conviction."""
        import brain
        src = inspect.getsource(brain.analyze_game)
        apply_idx = src.index('situations_result.get("total_away_adj"')
        edge_idx  = src.index("away_edge = round((away_model_p - away_nv)")
        assert apply_idx < edge_idx, (
            "total_away_adj must be applied to away_model_p before away_edge "
            "is computed from it"
        )

    def test_only_one_check_situations_call_remains(self):
        """The duplicate late-game recomputation must be removed now that
        situations_result is computed once, early, and reused."""
        import brain
        src = inspect.getsource(brain.analyze_game)
        assert src.count("check_situations(") == 1


# ── B8: wRC+ adj computed before the real recency-weighted value ─────────────

class TestWrcPlusAdjUsesRealValue:
    """B8: wrc_plus_adj was computed from a hardcoded wrc_plus_14d=100
    placeholder, 8 lines before the real recency-weighted value overwrote
    wrc_plus_14d — wrc_plus_adj was never recomputed, so real team offensive
    form barely reached run_factor / the win-prob blend."""

    def _patched_offense(self, **overrides):
        import offense_engine as oe
        defaults = dict(
            _rolling_hitting_window=lambda team_id, days, park_factor: (
                {"wrc_plus": 130.0, "rpg": 5.0, "low_sample": False, "games": 10}
                if days == 7 else
                {"wrc_plus": 110.0, "rpg": 4.5, "low_sample": False, "games": 30}
            ),
            _team_recent_record=lambda team_id, days: {"win_pct": 0.5, "wins": 5, "losses": 5},
            _platoon_splits_real=lambda team_id: {
                "vs_lhp": {"wrc_plus": 105.0}, "vs_rhp": {"wrc_plus": 100.0},
            },
            _platoon_adjustment_real=lambda splits, hand: (999.0, 5.0),
            _team_hitting_stats=lambda team_id: {
                "avg": 0.260, "obp": 0.330, "slg": 0.430, "ops": 0.760,
                "runs": 450, "games": 90,
            },
            _wrc_plus_proxy=lambda ops, park_factor: 105.0,
            _risp_stats=lambda team_id: {"risp_avg": 0.260, "risp_ops": 0.760},
        )
        defaults.update(overrides)
        return [patch.object(oe, name, side_effect=fn) for name, fn in defaults.items()]

    def test_adj_wrc_plus_reflects_real_recency_weighted_value(self):
        import offense_engine as oe
        patchers = self._patched_offense()
        for p in patchers:
            p.start()
        try:
            result = oe.analyze_offense("NYY", game_pk=None, side="away", opp_sp_hand="R")
        finally:
            for p in patchers:
                p.stop()

        # wrc_plus_14d = 0.40*130 + 0.35*110 + 0.25*105 = 116.75 -> 116.8
        # correct adj_wrc_plus = 116.8 + platoon_delta(5.0) = 121.8
        assert result["wrc_plus_14d"] == 116.8
        assert result["adj_wrc_plus"] == 121.8, (
            "adj_wrc_plus must be computed from the real wrc_plus_14d, not "
            "the discarded 100 placeholder"
        )
        buggy_value = round(100 + 5.0, 1)
        assert result["adj_wrc_plus"] != buggy_value
