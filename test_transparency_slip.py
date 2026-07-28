"""Tests for the parlay-os Step 4 transparency layer: per-pick reasoning
(_pick_narrative), the unified slip renderer (_render_slip_picks), and the
new all_ml_over_cap wiring into _daily_bet_slip().

Run: python -m pytest test_transparency_slip.py -v
"""

from unittest.mock import patch

import brain


def _ml_factors(**overrides):
    factors = [
        {"name": "sp_xwoba", "weight": 0.18, "away_p": 0.58,
         "raw": {"away_xfip": 3.10, "home_xfip": 4.40, "away_xwoba_against": 0.290,
                 "home_xwoba_against": 0.340, "xwoba_fallback_used": False}},
        {"name": "bullpen", "weight": 0.15, "away_p": 0.60,
         "raw": {"away_bp_fatigue": 1.5, "home_bp_fatigue": 7.2, "away_bp_stuff_adj": 0.01,
                 "home_bp_stuff_adj": 0.0, "away_key_reliever_avail": True,
                 "home_key_reliever_avail": False, "data_ok": True}},
        {"name": "offense", "weight": 0.13, "away_p": 0.51,
         "raw": {"away_wrc": 105, "home_wrc": 98, "away_bat_tracking_adj": 0.0,
                 "home_bat_tracking_adj": 0.0, "data_ok": True}},
        {"name": "rolling_form", "weight": 0.07, "away_p": 0.5,
         "raw": {"away_rolling_tier": "STABLE", "home_rolling_tier": "STABLE"}},
        {"name": "pitch_quality", "weight": 0.12, "away_p": 0.5,
         "raw": {"away_pitch_quality_adj": 0.0, "home_pitch_quality_adj": 0.0}},
        {"name": "pythagorean_homedog", "weight": 0.08, "away_p": 0.5,
         "raw": {"pyth_away_p": 0.5, "home_dog_add": 0.0}},
        {"name": "platoon_arm_angle", "weight": 0.08, "away_p": 0.5,
         "raw": {"away_platoon_edge": 0, "home_platoon_edge": 0, "away_arm_angle_adj": 0, "home_arm_angle_adj": 0}},
        {"name": "park_weather_of", "weight": 0.06, "away_p": 0.5, "raw": {"park_of_adj": 0.0}},
        {"name": "momentum_yoy", "weight": 0.05, "away_p": 0.5,
         "raw": {"away_momentum_score": 0, "home_momentum_score": 0, "away_yoy_adj": 0, "home_yoy_adj": 0}},
        {"name": "abs_tempo", "weight": 0.03, "away_p": 0.5,
         "raw": {"away_fps_adj": 0, "home_fps_adj": 0, "away_tempo_adj": 0, "home_tempo_adj": 0}},
        {"name": "baserunning_sprint", "weight": 0.03, "away_p": 0.5,
         "raw": {"away_sprint_adj": 0, "home_sprint_adj": 0}},
        {"name": "h2h", "weight": 0.02, "away_p": 0.5, "raw": {"h2h_away_p": 0.5}},
    ]
    return factors


def _ml_analysis(side="away", **overrides):
    a = {
        "away_name": "Boston Red Sox", "home_name": "New York Yankees",
        "ml_factors": _ml_factors(),
    }
    a.update(overrides)
    return a


class TestPickNarrativeML:
    def test_ranks_top_3_by_absolute_edge_contribution(self):
        diag = brain._build_ml_diagnostics(_ml_analysis(), "away")
        n = brain._pick_narrative("ML", diag, selection="Boston Red Sox", opp_label="New York Yankees")
        labels = [d["label"] for d in n["drivers"]]
        assert labels == ["Bullpen", "SP quality", "Offense"]
        assert n["neutral_fallbacks"] == []

    def test_flags_neutral_fallback_when_sp_missing(self):
        analysis = _ml_analysis(away_sp={"sp_missing": True})
        diag = brain._build_ml_diagnostics(analysis, "away")
        n = brain._pick_narrative("ML", diag, selection="Boston Red Sox")
        assert "SP quality" in n["neutral_fallbacks"]
        assert "SP quality" in n["why"]

    def test_key_reliever_unavailable_surfaces_as_its_own_driver(self):
        """key_reliever_availability is a separate diagnostic factor from
        Bullpen (fatigue) -- the two signals must stay visibly distinct."""
        factors = _ml_factors() + [
            {"name": "key_reliever_availability", "weight": 1.0, "away_p": 0.512,
             "raw": {"away_key_reliever_avail": False, "home_key_reliever_avail": True,
                     "away_key_relievers_flagged_count": 1, "home_key_relievers_flagged_count": 0,
                     "away_penalty_pp": 1.2, "home_penalty_pp": 0.0, "data_ok": True}},
        ]
        analysis = _ml_analysis(**{"ml_factors": factors})
        diag = brain._build_ml_diagnostics(analysis, "away")
        n = brain._pick_narrative("ML", diag, selection="Boston Red Sox", opp_label="New York Yankees")
        key_rel = next(d for d in n["drivers"] if d["label"] == "Key reliever availability")
        assert "our key reliever unavailable" in key_rel["value"]
        assert "-1.2pp" in key_rel["value"]


class TestPickNarrativeNonML:
    def test_game_bet_uses_real_raw_values_no_fabricated_percentage(self):
        analysis = {
            "away_sp": {"name": "A", "era": 2.65, "xfip": 3.69},
            "home_sp": {"name": "B", "era": 2.56, "xfip": 2.62},
            "away_bp": {"avg_fatigue": 3.0, "fatigue_tier": "MODERATE", "data_ok": True},
            "home_bp": {"avg_fatigue": 7.5, "fatigue_tier": "TIRED", "data_ok": True},
            "away_off": {"wrc_plus": 95}, "home_off": {"wrc_plus": 110},
            "weather": {"wind_mph": 12.0, "wind_label": "12mph out", "run_adjustment": 0.3, "run_factor": 1.05},
        }
        diag = brain._build_game_diagnostics(analysis, "TOTAL", {"total": {"line": 7.5}})
        n = brain._pick_narrative("TOTAL", diag, selection="Rays @ Jays UNDER 7.5")
        assert all(d["edge_pct"] is None for d in n["drivers"])  # honest: no weighted breakdown exists for this bet type
        assert any("TIRED" in d["value"] for d in n["drivers"])

    def test_missing_diagnostics_never_raises(self):
        n = brain._pick_narrative("TOTAL", None, selection="X")
        assert n["why"]
        assert n["drivers"] == []

    def test_empty_game_diagnostics_degrades_gracefully(self):
        diag = brain._build_game_diagnostics({}, "TOTAL", {})
        n = brain._pick_narrative("TOTAL", diag, selection="Y")
        assert "unavailable" in n["why"] or "Y" in n["why"]
        assert n["neutral_fallbacks"]


class TestPickNarrativeParlay:
    def test_ml_parlay_summarizes_each_leg(self):
        leg1 = brain._build_ml_diagnostics(_ml_analysis(), "away")
        leg2 = brain._build_ml_diagnostics(_ml_analysis(home_name="Tampa Bay Rays"), "home")
        diag = {"bet_type": "PARLAY_ML", "legs": [leg1, leg2]}
        n = brain._pick_narrative("PARLAY", diag, selection="Leg A + Leg B")
        assert "combines 2 legs" in n["why"]
        assert len(n["drivers"]) == 2

    def test_sgp_uses_leg_text_directly(self):
        diag = {"bet_type": "SGP", "sgp": {"legs": ["SP OVER 6.5 Ks (65%)", "NRFI (65%)"]}}
        n = brain._pick_narrative("PARLAY", diag, selection="SGP play")
        assert "SP OVER 6.5 Ks" in n["why"]
        assert len(n["drivers"]) == 2

    def test_parlay_with_no_leg_data_does_not_crash(self):
        diag = {"bet_type": "PARLAY_ML", "legs": [None, None]}
        n = brain._pick_narrative("PARLAY", diag, selection="X")
        assert n["why"]


class TestUnifiedSlipEndToEnd:
    """Exercises the real _daily_bet_slip() -> _render_slip_picks() path."""

    def _mk_ml(self, i, side="away", edge=8.0, stake=25.0):
        return {
            "away_name": f"Away{i}", "home_name": f"Home{i}",
            f"{side}_name": f"Away{i}" if side == "away" else f"Home{i}",
            f"best_{side}_odds": "+120", f"{side}_model_p": 0.55, f"{side}_nv": 0.48,
            f"{side}_edge": edge, f"{side}_stake": stake, f"{side}_confidence_score": 70,
            "away_lineup_confirmed": True, "home_lineup_confirmed": True,
            "ml_factors": _ml_factors(), "game_time_et": "7:05 PM",
        }

    def test_over_cap_ml_pick_shows_info_only_label_and_zero_stake(self):
        locks = [(self._mk_ml(1), "away")]
        ml_over_cap = [(self._mk_ml(2), "away", "HIGH")]
        sent = []
        with patch.object(brain, "_send_telegram", side_effect=lambda m: (sent.append(m), True)[1]):
            ok = brain._daily_bet_slip(locks, [], [], [], 1000.0, all_ml_over_cap=ml_over_cap)
        assert ok is True
        full = "\n".join(sent)
        assert "⚠ OVER CAP" in full
        assert "OVER CAP — info only" in full
        assert full.count("🎯 PLAY #") == 2

    def test_staked_pick_shows_dollar_and_unit_stake(self):
        locks = [(self._mk_ml(1, stake=40.0), "away")]
        sent = []
        with patch.object(brain, "_send_telegram", side_effect=lambda m: (sent.append(m), True)[1]):
            brain._daily_bet_slip(locks, [], [], [], 1000.0)
        full = "\n".join(sent)
        assert "$40.00 (2.0u)" in full

    def test_chunking_never_splits_a_play_block(self):
        locks = [(self._mk_ml(i), "away") for i in range(3)]
        flips = [(self._mk_ml(i + 100, edge=5.0), "away") for i in range(2)]
        all_totals = [{"game": f"T{i} @ T{i}b", "direction": "OVER", "line": 8.5, "prob": 0.6,
                       "market_p": 0.52, "edge_pct": 6.0 + i, "stake": 10.0, "odds": "-110"} for i in range(5)]
        all_runline = [{"game": f"R{i} @ R{i}b", "team": f"R{i}", "line": -1.5, "prob": 0.6,
                        "market_p": 0.52, "edge_pct": 6.0 + i, "stake": 10.0, "odds": "+140",
                        "bet_type": "RUNLINE-1.5", "conviction": "HIGH"} for i in range(5)]
        sent = []
        with patch.object(brain, "_send_telegram", side_effect=lambda m: (sent.append(m), True)[1]):
            brain._daily_bet_slip(locks, flips, [], [], 1000.0,
                                   all_totals=all_totals, all_runline=all_runline)
        assert len(sent) > 1, "expected this slate to need more than one Telegram message"
        for chunk in sent:
            assert chunk.count("🧠 Why") == chunk.count("⚙️  Key drivers") == chunk.count("🎯 PLAY #")
        play_nums = []
        for chunk in sent:
            for line in chunk.split("\n"):
                if line.startswith("🎯 PLAY #"):
                    play_nums.append(int(line.split("#")[1].split()[0].split("—")[0].strip()))
        assert play_nums == list(range(1, len(play_nums) + 1))

    def test_no_crash_when_pick_has_no_diagnostics(self):
        """A prop pick logged without a diagnostics dict must still render
        (graceful fallback), never crash the whole slip send."""
        locks = [(self._mk_ml(1), "away")]
        all_hitter_props = [{"player": "Test Hitter", "team": "SF", "prop": "Hits O1.5",
                              "model_prob": 0.6, "market_p": 0.5, "edge_pct": 12.0, "stake": 10.0}]
        sent = []
        with patch.object(brain, "_send_telegram", side_effect=lambda m: (sent.append(m), True)[1]):
            ok = brain._daily_bet_slip(locks, [], [], [], 1000.0, all_hitter_props=all_hitter_props)
        assert ok is True
        full = "\n".join(sent)
        assert "Test Hitter" in full
        assert "No diagnostic data captured" in full
