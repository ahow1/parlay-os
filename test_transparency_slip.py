"""Tests for the parlay-os Step 4 transparency layer: per-pick reasoning
(_pick_narrative), the unified slip renderer (_render_slip_picks), and the
new all_ml_over_cap wiring into _daily_bet_slip().

Run: python -m pytest test_transparency_slip.py -v
"""

from unittest.mock import patch

import brain
import narrative_engine


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
        # SECTION 1 (ML/F5) has no display cap (2026-09-09 redesign) -- force
        # multiple Telegram chunks via many over-cap ML picks, since
        # all_ml_over_cap isn't truncated upstream the way all_locks/
        # all_flips are (MAX_LOCKS_PER_DAY/MAX_FLIPS_PER_DAY).
        ml_over_cap = [(self._mk_ml(i + 200, edge=4.0), "away", "MEDIUM") for i in range(15)]
        sent = []
        with patch.object(brain, "_send_telegram", side_effect=lambda m: (sent.append(m), True)[1]):
            brain._daily_bet_slip(locks, flips, [], [], 1000.0, all_ml_over_cap=ml_over_cap)
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

    def test_prop_shows_full_matchup_not_bare_team_code(self):
        """2026-09-09: standalone prop plays used to fall back to a bare
        team code (e.g. "⚾ TOR") when rendered, unlike ML/TOTAL plays which
        always show the full matchup. A prop carrying a real "game" field
        (as the real hitter/K/ER-prop pipelines always populate) must show
        that full matchup, not just the team code."""
        locks = [(self._mk_ml(1), "away")]
        all_hitter_props = [{"player": "Test Hitter", "team": "TOR", "prop": "Hits O1.5",
                              "game": "Toronto Blue Jays @ Boston Red Sox",
                              "model_prob": 0.6, "market_p": 0.5, "edge_pct": 12.0, "stake": 10.0}]
        sent = []
        with patch.object(brain, "_send_telegram", side_effect=lambda m: (sent.append(m), True)[1]):
            brain._daily_bet_slip(locks, [], [], [], 1000.0, all_hitter_props=all_hitter_props)
        full = "\n".join(sent)
        assert "⚾ Toronto Blue Jays @ Boston Red Sox" in full
        assert "⚾ TOR\n" not in full and not any(
            line.strip() == "⚾ TOR" for line in full.split("\n")
        )

    def test_prop_with_no_game_field_shows_labeled_fallback_not_bare_code(self):
        """When a prop genuinely has no game field (legacy/edge-case data),
        the fallback must still be distinguishable from a real matchup --
        not a bare team code that looks like a truncated/broken matchup."""
        locks = [(self._mk_ml(1), "away")]
        all_hitter_props = [{"player": "Test Hitter", "team": "SF", "prop": "Hits O1.5",
                              "model_prob": 0.6, "market_p": 0.5, "edge_pct": 12.0, "stake": 10.0}]
        sent = []
        with patch.object(brain, "_send_telegram", side_effect=lambda m: (sent.append(m), True)[1]):
            brain._daily_bet_slip(locks, [], [], [], 1000.0, all_hitter_props=all_hitter_props)
        full = "\n".join(sent)
        assert not any(line.strip() == "⚾ SF" for line in full.split("\n"))
        assert "matchup unavailable" in full


def _pick(bet_type, edge_pct, over_cap=False, event=None, selection=None, stake=10.0):
    """Minimal slip_pick dict -- the exact shape _render_slip_picks expects,
    bypassing _daily_bet_slip's pool-budget machinery so section/top-N/
    footer behavior can be tested directly and precisely."""
    return {
        "bet_type": bet_type, "conviction": "HIGH",
        "event": event or f"{bet_type} event", "selection": selection or f"{bet_type} pick",
        "odds_str": "-110", "model_p": 0.55, "market_p": 0.5,
        "edge_pct": edge_pct, "stake": 0.0 if over_cap else stake,
        "over_cap": over_cap, "game_time_et": "", "diagnostics": None,
    }


class TestSlipSectionRedesign:
    """2026-09-09/2026-09-10 redesign: 5 labeled sections (ML/F5 unlimited,
    top-5 TOTALS, NRFI/YRFI top-5, top-3 RUNLINE, top-5 PROP, all ranked by
    edge%) instead of one flat list of every qualifying pick -- exercises
    _render_slip_picks() directly so section membership, top-N selection,
    and the suppressed-count footer can be verified without fighting
    _daily_bet_slip's pool-budget internals."""

    def _render(self, slip_picks):
        return brain._render_slip_picks(slip_picks, "2026-09-10", {}, ["HEADER"], ["SUMMARY LINE"])

    def test_ml_and_f5_have_no_display_cap(self):
        picks = [_pick("ML", 5.0 + i) for i in range(9)] + [_pick("F5", 6.0)]
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert full.count("🎯 PLAY #") == 10
        assert "SECTION 1 — ML / F5 (10)" in full

    def test_totals_capped_to_top_5_by_edge(self):
        picks = [_pick("TOTAL", edge, event=f"game{edge}") for edge in (4.0, 9.0, 5.0, 8.0, 6.0, 12.0, 7.0)]
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert "SECTION 2 — TOP 5 TOTALS (5)" in full
        # top 5 by edge: 12.0, 9.0, 8.0, 7.0, 6.0
        for edge in (12.0, 9.0, 8.0, 7.0, 6.0):
            assert f"game{edge}" in full
        for edge in (4.0, 5.0):
            assert f"game{edge}" not in full

    def test_nrfi_capped_to_top_5_by_edge(self):
        picks = [_pick("NRFI", edge, event=f"game{edge}") for edge in (4.0, 9.0, 5.0, 8.0, 6.0, 12.0, 7.0)]
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert "SECTION 3 — NRFI / YRFI (5)" in full
        for edge in (12.0, 9.0, 8.0, 7.0, 6.0):
            assert f"game{edge}" in full
        for edge in (4.0, 5.0):
            assert f"game{edge}" not in full

    def test_nrfi_shows_fewer_than_5_when_fewer_qualify(self):
        picks = [_pick("NRFI", 6.0), _pick("NRFI", 7.0)]
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert "SECTION 3 — NRFI / YRFI (2)" in full

    def test_nrfi_shows_at_least_3_when_at_least_3_exist(self):
        picks = [_pick("NRFI", 6.0), _pick("NRFI", 7.0), _pick("NRFI", 8.0)]
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert "SECTION 3 — NRFI / YRFI (3)" in full
        assert full.count("🎯 PLAY #") == 3

    def test_runlines_capped_to_top_3_by_edge(self):
        picks = [_pick("RUNLINE-1.5", edge, event=f"game{edge}") for edge in (4.0, 9.0, 5.0, 8.0, 6.0)]
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert "SECTION 4 — TOP 3 RUNLINES (3)" in full
        # top 3 by edge: 9.0, 8.0, 6.0
        assert "game9.0" in full and "game8.0" in full and "game6.0" in full
        assert "game4.0" not in full and "game5.0" not in full

    def test_props_capped_to_top_5_from_combined_pool(self):
        picks = [_pick("PROP", edge, selection=f"prop{edge}") for edge in
                 (5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0)]
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert "SECTION 5 — TOP 5 PLAYER PROPS (5)" in full
        for edge in (7.0, 8.0, 9.0, 10.0, 11.0):
            assert f"prop{edge}" in full
        for edge in (5.0, 6.0):
            assert f"prop{edge}" not in full

    def test_top_n_selection_combines_staked_and_over_cap(self):
        """An over-cap prop with a higher edge than a staked one must still
        outrank it for the top-5 slots -- "top N by edge%" means best
        overall, not staked-first-then-fill."""
        picks = [
            _pick("PROP", 6.0, over_cap=False, selection="staked-low"),
            _pick("PROP", 20.0, over_cap=True, selection="overcap-high"),
        ]
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert "overcap-high" in full
        assert "staked-low" in full  # both fit within top 5; just check ranking below
        overcap_idx = full.index("overcap-high")
        staked_idx = full.index("staked-low")
        assert overcap_idx < staked_idx, "higher-edge over-cap pick must render before lower-edge staked pick"

    def test_totals_top_n_selection_combines_staked_and_over_cap(self):
        picks = [
            _pick("TOTAL", 6.0, over_cap=False, selection="staked-low"),
            _pick("TOTAL", 20.0, over_cap=True, selection="overcap-high"),
        ]
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert full.index("overcap-high") < full.index("staked-low")

    def test_nrfi_top_n_selection_combines_staked_and_over_cap(self):
        picks = [
            _pick("NRFI", 6.0, over_cap=False, selection="staked-low"),
            _pick("NRFI", 20.0, over_cap=True, selection="overcap-high"),
        ]
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert full.index("overcap-high") < full.index("staked-low")

    def test_runline_top_n_selection_combines_staked_and_over_cap(self):
        picks = [
            _pick("RUNLINE-1.5", 6.0, over_cap=False, selection="staked-low"),
            _pick("RUNLINE-1.5", 20.0, over_cap=True, selection="overcap-high"),
        ]
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert full.index("overcap-high") < full.index("staked-low")

    def test_ml_section_orders_staked_before_over_cap_regardless_of_edge(self):
        picks = [
            _pick("ML", 4.0, over_cap=False, selection="staked"),
            _pick("ML", 20.0, over_cap=True, selection="overcap"),
        ]
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert full.index("staked") < full.index("overcap")

    def test_only_parlay_sgp_is_fully_suppressed_from_display(self):
        """TOTAL and NRFI now have their own sections -- only PARLAY/SGP
        has no section at all and is always fully suppressed."""
        picks = [_pick("TOTAL", 6.0, selection="a total"), _pick("NRFI", 6.0, selection="an nrfi"),
                 _pick("PARLAY", 6.0, selection="a parlay")]
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert "a total" in full
        assert "an nrfi" in full
        assert "a parlay" not in full
        assert full.count("🎯 PLAY #") == 2

    def test_footer_reports_suppressed_counts_for_all_capped_sections(self):
        picks = (
            [_pick("PROP", 5.0 + i) for i in range(7)] +        # 7 props -> top 5 shown, 2 suppressed
            [_pick("RUNLINE-1.5", 5.0 + i) for i in range(4)] +  # 4 runlines -> top 3 shown, 1 suppressed
            [_pick("TOTAL", 5.0 + i) for i in range(6)] +        # 6 totals -> top 5 shown, 1 suppressed
            [_pick("NRFI", 5.0 + i) for i in range(8)]           # 8 NRFI -> top 5 shown, 3 suppressed
        )
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert "+2 props" in full
        assert "+1 runlines" in full
        assert "+1 totals" in full
        assert "+3 NRFI" in full
        assert "logged for CLV tracking (not shown)" in full

    def test_footer_reports_suppressed_count_for_parlays(self):
        picks = [_pick("PARLAY", 6.0), _pick("PARLAY", 7.0)]
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert "+2 parlays" in full

    def test_no_footer_suppressed_line_when_nothing_is_cut(self):
        picks = [_pick("ML", 6.0), _pick("TOTAL", 6.0), _pick("NRFI", 6.0),
                 _pick("RUNLINE-1.5", 6.0), _pick("PROP", 6.0)]
        chunks = self._render(picks)
        full = "\n".join(chunks)
        assert "logged for CLV tracking" not in full

    def test_empty_slip_shows_no_sections_or_suppressed_footer(self):
        chunks = self._render([])
        full = "\n".join(chunks)
        assert "SECTION" not in full
        assert "logged for CLV tracking" not in full
        assert "🎯 PLAY #" not in full
        assert "SUMMARY LINE" in full  # header/footer scaffolding still present

    def test_play_numbering_is_sequential_across_all_sections(self):
        picks = (
            [_pick("ML", 9.0)] +
            [_pick("TOTAL", 8.5)] +
            [_pick("NRFI", 8.2)] +
            [_pick("RUNLINE-1.5", 8.0)] +
            [_pick("PROP", 7.0)]
        )
        chunks = self._render(picks)
        full = "\n".join(chunks)
        nums = [int(l.split("#")[1].split()[0].split("—")[0].strip())
                for l in full.split("\n") if l.startswith("🎯 PLAY #")]
        assert nums == [1, 2, 3, 4, 5]

    def test_section_header_never_separated_from_its_first_play_across_chunks(self):
        """The header-glued-to-first-block trick must survive real chunking
        -- force a split with many ML picks, then add a runline so its
        section header lands wherever the split falls, and confirm the
        header is never the very last line of a chunk without its play."""
        picks = [_pick("ML", 5.0 + i) for i in range(40)] + [_pick("RUNLINE-1.5", 9.0, event="RL game")]
        chunks = self._render(picks)
        assert len(chunks) > 1
        for chunk in chunks:
            lines = chunk.split("\n")
            for i, line in enumerate(lines):
                if line.startswith("🏆"):
                    # A play block ("🎯 PLAY #") must appear later in the SAME chunk.
                    assert any(l.startswith("🎯 PLAY #") for l in lines[i:]), \
                        "section header appeared without its first play block in the same chunk"


class TestNarrativeEngineMerge:
    """2026-09-10: real LLM narratives (narrative_engine.generate_slip_
    narratives) override the deterministic template's "Why" text and
    driver lines when available, falling back to the template per-pick
    when the LLM call fails, is skipped, or omits a given index. Mocks
    narrative_engine.generate_slip_narratives directly rather than the
    Anthropic client -- that boundary is narrative_engine's own contract,
    covered by test_narrative_engine.py."""

    def _render(self, slip_picks):
        return brain._render_slip_picks(slip_picks, "2026-09-10", {}, ["HEADER"], ["SUMMARY LINE"])

    def test_llm_narrative_replaces_template_why_text(self):
        picks = [_pick("ML", 9.0, selection="Kansas City Royals ML")]
        with patch.object(
            narrative_engine, "generate_slip_narratives",
            return_value={0: {"narrative": "A real, specific sentence about this exact game.", "drivers": ["Bullpen: MIN gassed"]}},
        ):
            chunks = self._render(picks)
        full = "\n".join(chunks)
        assert "A real, specific sentence about this exact game." in full
        assert "- Bullpen: MIN gassed" in full
        assert "No diagnostic data captured" not in full

    def test_missing_index_falls_back_to_template_for_that_pick_only(self):
        picks = [
            _pick("ML", 9.0, selection="pick-with-llm"),
            _pick("ML", 8.0, selection="pick-without-llm"),
        ]
        with patch.object(
            narrative_engine, "generate_slip_narratives",
            return_value={0: {"narrative": "LLM wrote this one.", "drivers": ["driver line"]}},
        ):
            chunks = self._render(picks)
        full = "\n".join(chunks)
        assert "LLM wrote this one." in full
        assert "No diagnostic data captured for this pick." in full  # pick-without-llm's template fallback

    def test_engine_exception_falls_back_to_template_for_every_pick(self):
        picks = [_pick("ML", 9.0), _pick("PROP", 8.0)]
        with patch.object(narrative_engine, "generate_slip_narratives", side_effect=RuntimeError("boom")):
            chunks = self._render(picks)  # must not raise
        full = "\n".join(chunks)
        assert full.count("No diagnostic data captured for this pick.") == 2

    def test_narrative_engine_import_failure_falls_back_gracefully(self):
        """If narrative_engine can't even be imported (e.g. a broken
        install), the slip must still render with template narratives --
        this is the outermost safety net in _render_slip_picks()."""
        import sys
        picks = [_pick("ML", 9.0)]
        with patch.dict(sys.modules, {"narrative_engine": None}):
            chunks = self._render(picks)  # must not raise
        full = "\n".join(chunks)
        assert "🎯 PLAY #1" in full

    def test_empty_slip_never_calls_the_narrative_engine(self):
        with patch.object(narrative_engine, "generate_slip_narratives") as mock_gen:
            self._render([])
        mock_gen.assert_not_called()

    def test_flat_index_is_consistent_across_multiple_sections(self):
        """The index passed to generate_slip_narratives (and read back for
        the merge) must match each pick's position in the FLATTENED
        display order across all 5 sections, not reset per section."""
        picks = [
            _pick("ML", 9.0, selection="ml-pick"),          # flat index 0
            _pick("TOTAL", 9.0, selection="total-pick"),    # flat index 1
            _pick("NRFI", 9.0, selection="nrfi-pick"),       # flat index 2
        ]
        captured = {}

        def _fake_generate(narrative_inputs):
            captured["inputs"] = narrative_inputs
            return {1: {"narrative": "TOTAL-SPECIFIC NARRATIVE", "drivers": []}}

        with patch.object(narrative_engine, "generate_slip_narratives", side_effect=_fake_generate):
            chunks = self._render(picks)
        full = "\n".join(chunks)
        assert captured["inputs"][1]["selection"] == "total-pick"
        assert "TOTAL-SPECIFIC NARRATIVE" in full
        # Verify it landed on the TOTAL pick's block, not the ML pick's.
        total_block_start = full.index("total-pick")
        ml_block_start = full.index("ml-pick")
        narrative_pos = full.index("TOTAL-SPECIFIC NARRATIVE")
        assert total_block_start < narrative_pos
        assert not (ml_block_start < narrative_pos < full.index("Model", ml_block_start))
