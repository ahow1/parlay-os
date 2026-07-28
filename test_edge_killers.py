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


# ── M2: neutral-default masking in bullpen + offense ──────────────────────────

class TestNeutralDefaultMasking:
    """M2: bullpen_run_factor() resolved a data_ok=False (UNKNOWN tier)
    bullpen to a neutral 1.0 purely by coincidence of the tier-lookup dict's
    default, with no explicit data_ok check. offense_engine.py had no
    aggregate missing-data flag at all, so a fully-down offense feed had no
    suppression path in the win-prob blend."""

    def test_bullpen_run_factor_ignores_stale_fields_when_data_not_ok(self):
        """If data_ok=False but fatigue_tier/high_fatigue_arms somehow carry
        stale non-neutral values, the old code (no data_ok check) would still
        use them. The fix must return the explicit neutral 1.0 regardless."""
        from bullpen_engine import bullpen_run_factor
        bp = {"data_ok": False, "fatigue_tier": "TIRED", "high_fatigue_arms": ["A", "B"]}
        assert bullpen_run_factor(bp) == 1.0

    def test_bullpen_run_factor_still_uses_real_data_when_ok(self):
        from bullpen_engine import bullpen_run_factor
        bp = {"data_ok": True, "fatigue_tier": "TIRED", "high_fatigue_arms": ["A", "B"]}
        assert bullpen_run_factor(bp) == round(1.04 + 2 * 0.005, 4)

    def test_analyze_offense_flags_offense_missing_on_total_fetch_failure(self):
        import offense_engine as oe
        patchers = [
            patch.object(oe, "_rolling_hitting_window", return_value={}),
            patch.object(oe, "_team_recent_record", return_value={}),
            patch.object(oe, "_platoon_splits_real", return_value={}),
            patch.object(oe, "_platoon_adjustment_real", return_value=(100.0, 0.0)),
            patch.object(oe, "_team_hitting_stats", return_value={}),
            patch.object(oe, "_wrc_plus_proxy", return_value=100.0),
            patch.object(oe, "_risp_stats", return_value={}),
        ]
        for p in patchers:
            p.start()
        try:
            result = oe.analyze_offense("NYY", game_pk=None, side="away", opp_sp_hand="R")
        finally:
            for p in patchers:
                p.stop()
        assert result.get("offense_missing") is True

    def test_analyze_offense_does_not_flag_missing_with_healthy_data(self):
        import offense_engine as oe
        patchers = [
            patch.object(oe, "_rolling_hitting_window", return_value={
                "wrc_plus": 105.0, "rpg": 4.6, "low_sample": False, "games": 10,
            }),
            patch.object(oe, "_team_recent_record", return_value={"win_pct": 0.5}),
            patch.object(oe, "_platoon_splits_real", return_value={
                "vs_lhp": {"wrc_plus": 100.0}, "vs_rhp": {"wrc_plus": 100.0},
            }),
            patch.object(oe, "_platoon_adjustment_real", return_value=(100.0, 0.0)),
            patch.object(oe, "_team_hitting_stats", return_value={
                "avg": 0.260, "obp": 0.330, "slg": 0.430, "ops": 0.760,
                "runs": 450, "games": 90,
            }),
            patch.object(oe, "_wrc_plus_proxy", return_value=105.0),
            patch.object(oe, "_risp_stats", return_value={"risp_avg": 0.26, "risp_ops": 0.76}),
        ]
        for p in patchers:
            p.start()
        try:
            result = oe.analyze_offense("NYY", game_pk=None, side="away", opp_sp_hand="R")
        finally:
            for p in patchers:
                p.stop()
        assert result.get("offense_missing") is False

    def test_default_offense_flags_missing(self):
        from offense_engine import _default_offense
        assert _default_offense("ZZZ").get("offense_missing") is True

    def test_weighted_win_prob_excludes_offense_when_data_missing(self):
        """Mirrors the existing bullpen data_ok exclusion pattern (Factor 4)
        for the new offense data_ok params (Factor 5): wildly lopsided wRC+
        must NOT move win prob when either side's offense data is missing."""
        from brain import _weighted_win_prob
        common = dict(
            away_xfip=4.35, home_xfip=4.35,
            away_bp_fatigue=4.0, home_bp_fatigue=4.0,
            home_dog_add=0.0, pyth_away_p=0.5,
            lm_direction="", lm_magnitude=0.0,
            away_platoon_edge=0.0, home_platoon_edge=0.0,
            away_momentum_score=0.0, home_momentum_score=0.0,
        )
        away_p_missing, _, _ = _weighted_win_prob(
            away_wrc=150.0, home_wrc=50.0,
            away_off_data_ok=False, home_off_data_ok=True,
            **common,
        )
        away_p_present, _, _ = _weighted_win_prob(
            away_wrc=150.0, home_wrc=50.0,
            away_off_data_ok=True, home_off_data_ok=True,
            **common,
        )
        assert away_p_missing != away_p_present, (
            "a lopsided wRC+ gap must stop moving win prob once offense "
            "data is flagged missing on one side"
        )


# ── parlay-os Step 3 diagnostic: bullpen signals computed but discarded ───────

class TestKeyRelieverWiredIntoWinProb:
    """Step 3 diagnostic found key_reliever_available/key_relievers_flagged
    threaded from bullpen_engine but never read (key_reliever_available) or
    never even passed as a parameter (key_relievers_flagged) in
    _weighted_win_prob. Fixed as two separate, small, documented additive
    adjustments applied OUTSIDE the 12-factor weighted blend (not folded
    into Factor 4/avg_fatigue, to avoid double-counting the same signal
    twice): -1.2pp when a team's key reliever (CL or top-usage RP) is
    unavailable, plus -0.5pp per additional flagged reliever beyond the
    closer (capped at -1.5pp for that term alone)."""

    def _common(self, **overrides):
        base = dict(
            away_xfip=4.35, home_xfip=4.35,
            away_bp_fatigue=2.0, home_bp_fatigue=2.0,
            home_dog_add=0.0, pyth_away_p=0.5,
            lm_direction="", lm_magnitude=0.0,
            away_platoon_edge=0.0, home_platoon_edge=0.0,
            away_momentum_score=0.0, home_momentum_score=0.0,
        )
        base.update(overrides)
        return base

    def test_away_closer_unavailable_moves_model_p_by_expected_amount(self):
        from brain import _weighted_win_prob
        away_p_avail, _, _ = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0,
            away_key_reliever_avail=True, home_key_reliever_avail=True,
            **self._common(),
        )
        away_p_unavail, _, _ = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0,
            away_key_reliever_avail=False, home_key_reliever_avail=True,
            **self._common(),
        )
        # Symmetric adjustment: away's own penalty (-1.2pp) subtracted from
        # away_p, home's penalty (0) added -- net delta is exactly -0.012.
        assert round(away_p_avail - away_p_unavail, 4) == 0.012

    def test_home_closer_unavailable_helps_away_by_expected_amount(self):
        from brain import _weighted_win_prob
        away_p_baseline, _, _ = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0,
            away_key_reliever_avail=True, home_key_reliever_avail=True,
            **self._common(),
        )
        away_p_home_out, _, _ = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0,
            away_key_reliever_avail=True, home_key_reliever_avail=False,
            **self._common(),
        )
        assert round(away_p_home_out - away_p_baseline, 4) == 0.012

    def test_flagged_count_beyond_closer_adds_on_top(self):
        """count=1 (just the closer) contributes only the base -1.2pp;
        count=2 (closer + top-usage RP both flagged) adds the extra
        -0.5pp per-arm term on top."""
        from brain import _weighted_win_prob
        away_p_one_flagged, _, _ = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0,
            away_key_reliever_avail=False, home_key_reliever_avail=True,
            away_key_relievers_flagged_count=1,
            **self._common(),
        )
        away_p_two_flagged, _, _ = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0,
            away_key_reliever_avail=False, home_key_reliever_avail=True,
            away_key_relievers_flagged_count=2,
            **self._common(),
        )
        assert round(away_p_one_flagged - away_p_two_flagged, 4) == 0.005

    def test_extra_flagged_term_is_capped(self):
        """The -0.5pp-per-extra-arm term caps at -1.5pp (i.e. 3 extra arms,
        count=4) -- count=4 and count=100 must produce identical away_p."""
        from brain import _weighted_win_prob
        away_p_at_cap, _, _ = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0,
            away_key_reliever_avail=False, home_key_reliever_avail=True,
            away_key_relievers_flagged_count=4,
            **self._common(),
        )
        away_p_huge, _, _ = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0,
            away_key_reliever_avail=False, home_key_reliever_avail=True,
            away_key_relievers_flagged_count=100,
            **self._common(),
        )
        # Extra-arm term caps at -1.5pp total regardless of how large the
        # count gets -- capped total penalty is 1.2 (base) + 1.5 (cap) = 2.7pp.
        assert away_p_at_cap == away_p_huge

    def test_both_teams_signals_stay_within_documented_caps(self):
        from brain import _weighted_win_prob
        away_p_default, _, _ = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0, **self._common(),
        )
        away_p_worst_case, _, _ = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0,
            away_key_reliever_avail=False, home_key_reliever_avail=True,
            away_key_relievers_flagged_count=100, home_key_relievers_flagged_count=0,
            **self._common(),
        )
        # Max possible single-side swing: 1.2pp base + 1.5pp capped extra = 2.7pp
        assert round(away_p_default - away_p_worst_case, 4) == 0.027

    def test_missing_bullpen_data_applies_no_penalty(self):
        """away_bp_data_ok=False must not fabricate a penalty from
        key_reliever_avail/flagged_count -- absence of data isn't evidence
        of an unavailable reliever."""
        from brain import _weighted_win_prob
        away_p_ok, _, _ = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0,
            away_key_reliever_avail=True, home_key_reliever_avail=True,
            away_bp_data_ok=True, home_bp_data_ok=True,
            **self._common(),
        )
        away_p_missing, _, _ = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0,
            away_key_reliever_avail=False, home_key_reliever_avail=True,
            away_key_relievers_flagged_count=2,
            away_bp_data_ok=False, home_bp_data_ok=True,
            **self._common(),
        )
        # away_bp_data_ok=False already neutralizes Factor 4 (bp_away_p=0.5)
        # AND must neutralize the key-reliever penalty -- both sides should
        # land on the same away_p once Factor 4 itself is neutral on both.
        away_p_both_missing_but_avail, _, _ = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0,
            away_key_reliever_avail=True, home_key_reliever_avail=True,
            away_bp_data_ok=False, home_bp_data_ok=True,
            **self._common(),
        )
        assert away_p_missing == away_p_both_missing_but_avail

    def test_key_reliever_availability_factor_present_in_diagnostics(self):
        from brain import _weighted_win_prob
        _, _, factors = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0,
            away_key_reliever_avail=False, home_key_reliever_avail=True,
            away_key_relievers_flagged_count=2,
            **self._common(),
        )
        f = next(x for x in factors if x["name"] == "key_reliever_availability")
        assert f["raw"]["away_key_reliever_avail"] is False
        assert f["raw"]["home_key_reliever_avail"] is True
        assert f["raw"]["away_key_relievers_flagged_count"] == 2
        assert f["raw"]["away_penalty_pp"] == 1.7  # 1.2 base + 0.5 extra (count-1=1 arm)
        assert f["raw"]["home_penalty_pp"] == 0.0

    def test_key_reliever_availability_factor_flags_missing_data(self):
        from brain import _weighted_win_prob
        _, _, factors = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0,
            away_bp_data_ok=False, home_bp_data_ok=True,
            **self._common(),
        )
        f = next(x for x in factors if x["name"] == "key_reliever_availability")
        assert f["raw"]["data_ok"] is False

    def test_bullpen_factor_no_longer_carries_key_reliever_fields(self):
        """The key-reliever signal is now its own separate diagnostic
        factor -- the bullpen (fatigue) factor's raw dict should not
        duplicate it, to keep the two signals visibly distinct in the
        transparency layer and avoid implying they're double-counted."""
        from brain import _weighted_win_prob
        _, _, factors = _weighted_win_prob(
            away_wrc=90.0, home_wrc=90.0,
            away_key_reliever_avail=False, home_key_reliever_avail=True,
            **self._common(),
        )
        bp_factor = next(f for f in factors if f["name"] == "bullpen")
        assert "away_key_reliever_avail" not in bp_factor["raw"]
        assert "home_key_reliever_avail" not in bp_factor["raw"]


class TestConvictionBullpenGateRestored:
    """Step 3 diagnostic found _conviction() accepted bp/market parameters
    but never referenced them -- git history shows the original _conviction()
    required fatigue_tier in (FRESH, MODERATE) for HIGH, dropped in a later
    refactor while every call site kept passing bp unchanged. Restored: a
    TIRED bullpen caps an otherwise-HIGH pick at MEDIUM."""

    def test_tired_bullpen_caps_high_at_medium(self):
        from brain import _conviction, CONVICTION_HIGH_EDGE_MIN, CONVICTION_HIGH_MODEL_MIN
        fresh_bp = {"data_ok": True, "fatigue_tier": "FRESH"}
        tired_bp = {"data_ok": True, "fatigue_tier": "TIRED"}
        assert _conviction(CONVICTION_HIGH_EDGE_MIN, CONVICTION_HIGH_MODEL_MIN, fresh_bp, {}) == "HIGH"
        assert _conviction(CONVICTION_HIGH_EDGE_MIN, CONVICTION_HIGH_MODEL_MIN, tired_bp, {}) == "MEDIUM"

    def test_tired_bullpen_with_data_not_ok_does_not_gate(self):
        """An UNKNOWN bullpen (fetch failure, data_ok=False) must not be
        treated as tired -- absence of data isn't evidence of fatigue."""
        from brain import _conviction, CONVICTION_HIGH_EDGE_MIN, CONVICTION_HIGH_MODEL_MIN
        unknown_bp = {"data_ok": False, "fatigue_tier": "UNKNOWN"}
        assert _conviction(CONVICTION_HIGH_EDGE_MIN, CONVICTION_HIGH_MODEL_MIN, unknown_bp, {}) == "HIGH"

    def test_empty_bp_dict_unaffected(self):
        """Existing test_fixes.py / test_ml_bucketing.py callers pass {} for
        bp -- must keep behaving exactly as before this fix."""
        from brain import _conviction, CONVICTION_HIGH_EDGE_MIN, CONVICTION_HIGH_MODEL_MIN
        assert _conviction(CONVICTION_HIGH_EDGE_MIN, CONVICTION_HIGH_MODEL_MIN, {}, {}) == "HIGH"

    def test_medium_tier_unaffected_by_tired_bullpen(self):
        from brain import _conviction, CONVICTION_MEDIUM_EDGE_MIN, CONVICTION_MEDIUM_MODEL_MIN
        tired_bp = {"data_ok": True, "fatigue_tier": "TIRED"}
        assert _conviction(CONVICTION_MEDIUM_EDGE_MIN, CONVICTION_MEDIUM_MODEL_MIN, tired_bp, {}) == "MEDIUM"


# ── M3, M4: silent factor failures not logged ─────────────────────────────────

class TestSilentFactorFailuresLogged:
    """M3/M4: five savant_leaderboards lookups plus H2H each used a bare
    except Exception: pass with zero logging — a broken import or API
    failure silently zeroed a win-prob blend factor with no trace anywhere.
    Fix: log every exception and record it in the existing data_health
    aggregate (the daily health check already surfaced in every scout run)."""

    FEEDS = [
        "savant_bullpen_stuff",
        "savant_bat_tracking",
        "savant_park_of_defense",
        "savant_sprint_baserunning",
        "savant_arm_angle",
        "h2h",
    ]

    # Unique substrings identifying each of the 6 try/except blocks, used to
    # scope the "no bare pass" check to the right block instead of the whole
    # (huge) function.
    MARKERS = {
        "savant_bullpen_stuff":      "bullpen_stuff_lambda_adj as _bpsla",
        "savant_bat_tracking":       "blast_tb_adj as _blastadj",
        "savant_park_of_defense":    "team_of_lambda_adj as _ofadj",
        "savant_sprint_baserunning": "sprint_lambda_adj as _sprintadj",
        "savant_arm_angle":          "arm_angle_platoon_adj as _armadj",
        "h2h":                       "get_h2h_stats(away_tid, home_tid)",
    }

    def test_every_feed_recorded_in_data_health(self):
        import brain
        src = inspect.getsource(brain.analyze_game)
        for feed in self.FEEDS:
            assert f'"{feed}"' in src, f"missing data_health tracking for {feed}"
            assert f'data_health.record_ok("{feed}"' in src

    def test_no_bare_pass_remains_around_each_block(self):
        import brain
        src = inspect.getsource(brain.analyze_game)
        for feed, marker in self.MARKERS.items():
            idx = src.index(marker)
            window = src[idx: idx + 700]
            assert "except Exception:\n        pass" not in window, (
                f"{feed} block still silently swallows exceptions with no logging"
            )
            assert "print(" in window, f"{feed} block must print/log its exception"

    def test_data_health_records_failure_on_exception(self):
        """Behavioral check on the real mechanism these blocks now call —
        data_health.record_ok(feed, False) must show up as non-'live'."""
        import data_health
        data_health.reset()
        data_health.record_ok("savant_bullpen_stuff", False)
        assert data_health.as_dict()["savant_bullpen_stuff"] == "failed"
        data_health.reset()
