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


# ── M1: SP-missing false-positive gaps ────────────────────────────────────────

class TestSpMissingGapsClosed:
    """M1: get_game_sps() can attach a real probable pitcher's NAME to a fully
    fabricated stat-line (sp_missing=True, era/k9 = league-average defaults).
    Three consumers checked `name` instead of `sp_missing` and so could show
    a fake-ace pick/prop under a real pitcher's name."""

    def _fabricated_sp(self, **overrides):
        sp = {
            "name": "Gerrit Cole", "pitcher_id": 543037,
            "k9": 12.0, "ip": 100, "gs": 15, "ttop": True,
            "hand": "R", "era": 4.35, "xfip": 4.35,
            "sp_missing": True,
        }
        sp.update(overrides)
        return sp

    def test_sgp_builder_skips_fabricated_sp_dominance_leg(self):
        from props_engine import build_sgp_suggestions
        fabricated = self._fabricated_sp()
        nrfi_r  = {"p_nrfi": 0.60}
        total_r = {"p_under": 0.60, "p_over": 0.60}
        market  = {"totals": {"line": 8.5}}

        suggestions = build_sgp_suggestions(
            fabricated, {}, away_xr=4.0, home_xr=4.0,
            nrfi_r=nrfi_r, total_r=total_r, market=market,
            away_model_p=0.5, home_model_p=0.5,
        )
        assert not any(s["type"] == "SP_DOMINANCE" for s in suggestions), (
            "a fabricated (sp_missing=True) SP must never produce an "
            "SP_DOMINANCE SGP leg, even though its default k9 passes the "
            ">=8.0 threshold"
        )

    def test_sgp_builder_still_produces_leg_for_real_sp(self):
        """Control: with sp_missing=False (real data), the same inputs must
        still produce an SP_DOMINANCE suggestion — proves the skip above is
        actually about sp_missing, not a broken test fixture."""
        from props_engine import build_sgp_suggestions
        real_sp = self._fabricated_sp(sp_missing=False)
        nrfi_r  = {"p_nrfi": 0.60}
        total_r = {"p_under": 0.60, "p_over": 0.60}
        market  = {"totals": {"line": 8.5}}

        suggestions = build_sgp_suggestions(
            real_sp, {}, away_xr=4.0, home_xr=4.0,
            nrfi_r=nrfi_r, total_r=total_r, market=market,
            away_model_p=0.5, home_model_p=0.5,
        )
        assert any(s["type"] == "SP_DOMINANCE" for s in suggestions)

    def test_props_kprop_gate_skips_fabricated_sp(self):
        from brain import _build_props_entry
        analysis = {
            "away_sp": self._fabricated_sp(),
            "home_sp": {},
            "nrfi": {}, "total": {}, "totals_line": None,
        }
        entry = _build_props_entry(analysis, [])
        k_props = [p for p in entry["props"] if p.get("type") == "K_PROP"]
        assert k_props == [], (
            "the /props K-prop feed must skip a fabricated (sp_missing=True) "
            "SP even though its name isn't 'TBD'"
        )

    def test_props_kprop_gate_still_includes_real_sp(self):
        from brain import _build_props_entry
        analysis = {
            "away_sp": self._fabricated_sp(sp_missing=False),
            "home_sp": {},
            "nrfi": {}, "total": {}, "totals_line": None,
        }
        entry = _build_props_entry(analysis, [])
        k_props = [p for p in entry["props"] if p.get("type") == "K_PROP"]
        assert len(k_props) == 1

    def test_confidence_dampening_flags_fabricated_sp_by_sp_missing(self):
        from brain import _sp_effectively_unknown
        assert _sp_effectively_unknown(self._fabricated_sp()) is True

    def test_confidence_dampening_still_flags_true_tbd(self):
        """Regression guard: a genuinely unannounced probable pitcher (no
        name, sp_missing not set) must still be flagged."""
        from brain import _sp_effectively_unknown
        assert _sp_effectively_unknown({"name": "TBD"}) is True
        assert _sp_effectively_unknown({}) is True

    def test_confidence_dampening_does_not_flag_real_confirmed_sp(self):
        from brain import _sp_effectively_unknown
        assert _sp_effectively_unknown(self._fabricated_sp(sp_missing=False)) is False
