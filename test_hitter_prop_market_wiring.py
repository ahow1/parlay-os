"""Tests for wiring real SGO market_p into brain.py's prop pipelines
(PHASE 2 STEP 3). Covers _scan_hitter_props: real market override, graceful
fallback to the fixed baseline, and the exact output-dict shape downstream
Telegram/dedup code depends on.

Run: python -m pytest test_hitter_prop_market_wiring.py -v

No real network calls -- SGO events are built by hand in the same
normalized shape sportsgameodds_client._normalize_event() produces.
"""

import brain


def _mk_prop(player_id, stat, side, line, books):
    return {
        "player_id": player_id,
        "stat":      stat,
        "side":      side,
        "line":      line,
        "by_book":   {bk: {"american": str(odds)} for bk, odds in books.items()},
    }


def _gimenez_rbi_event():
    """Real odds captured from a live SGO slate fetch (Andres Gimenez, RBI
    O/U 0.5): draftkings +309/-460, fanduel +280/-400. No-vig over-prob
    from these two books averages to ~0.243 -- verified by hand against
    math_engine.no_vig_prob() during Step 2."""
    return {
        "props": [
            _mk_prop("ANDRES_GIMENEZ_1_MLB", "batter_rbis", "over", 0.5,
                     {"draftkings": 309, "fanduel": 280}),
            _mk_prop("ANDRES_GIMENEZ_1_MLB", "batter_rbis", "under", 0.5,
                     {"draftkings": -460, "fanduel": -400}),
        ]
    }


def _stats():
    # 14 games, high per-game rates so every leg clears the model's own
    # qualifying edge check regardless of which market_p (real or baseline)
    # ends up being used -- keeps these tests about the wiring, not tuning.
    return {"hits": 10, "homeRuns": 2, "totalBases": 20, "rbi": 12,
            "strikeOuts": 8, "atBats": 50, "games": 14}


def _by_prop(recs):
    return {r["prop"]: r for r in recs}


class TestBaselineFallback:
    """No SGO event at all -- must reproduce the pre-wiring behavior exactly."""

    def test_no_sgo_event_uses_fixed_baseline(self):
        recs = brain._scan_hitter_props("Andres Gimenez", "TOR", _stats(), {"sp_missing": True}, 300.0, None)
        by_prop = _by_prop(recs)
        assert "RBI O0.5" in by_prop
        assert by_prop["RBI O0.5"]["market_p"] == 0.32   # _HITTER_PROP_LINES baseline

    def test_uncovered_player_in_event_falls_back_to_baseline(self):
        """SGO has a market, but not for THIS player -- must not bleed over."""
        recs = brain._scan_hitter_props("Freddie Freeman", "LAD", _stats(), {"sp_missing": True}, 300.0,
                                         _gimenez_rbi_event())
        by_prop = _by_prop(recs)
        assert by_prop["RBI O0.5"]["market_p"] == 0.32

    def test_batter_strikeouts_has_no_sgo_mapping_stays_on_baseline(self):
        """SGO has no batter-strikeouts market at all (only pitcher_strikeouts
        is mapped) -- this leg must never attempt a real-market lookup."""
        recs = brain._scan_hitter_props("Andres Gimenez", "TOR", _stats(), {"sp_missing": True}, 300.0,
                                         _gimenez_rbi_event())
        by_prop = _by_prop(recs)
        if "SO O0.5" in by_prop:
            assert by_prop["SO O0.5"]["market_p"] == 0.55


class TestRealMarketOverride:
    def test_real_market_p_replaces_baseline(self):
        recs = brain._scan_hitter_props("Andres Gimenez", "TOR", _stats(), {"sp_missing": True}, 300.0,
                                         _gimenez_rbi_event())
        by_prop = _by_prop(recs)
        assert "RBI O0.5" in by_prop
        real_mp = by_prop["RBI O0.5"]["market_p"]
        assert real_mp != 0.32
        assert 0.20 < real_mp < 0.30   # matches the ~0.243 computed in Step 2

    def test_edge_pct_is_recomputed_against_real_market_not_baseline(self):
        baseline_recs = brain._scan_hitter_props("Andres Gimenez", "TOR", _stats(), {"sp_missing": True}, 300.0, None)
        real_recs     = brain._scan_hitter_props("Andres Gimenez", "TOR", _stats(), {"sp_missing": True}, 300.0,
                                                  _gimenez_rbi_event())
        baseline_edge = _by_prop(baseline_recs)["RBI O0.5"]["edge_pct"]
        real_edge     = _by_prop(real_recs)["RBI O0.5"]["edge_pct"]
        # real market_p (~0.243) is below the assumed baseline (0.32), so the
        # model should show *more* edge against the real market, not less.
        assert real_edge > baseline_edge

    def test_market_p_and_edge_pct_stay_internally_consistent(self):
        """edge_pct must always equal (model_prob - market_p) * 100 -- the two
        fields must never contradict each other regardless of which market_p
        (real or baseline) was actually used."""
        recs = brain._scan_hitter_props("Andres Gimenez", "TOR", _stats(), {"sp_missing": True}, 300.0,
                                         _gimenez_rbi_event())
        for r in recs:
            expected_edge = round((r["model_prob"] - r["market_p"]) * 100, 1)
            assert r["edge_pct"] == expected_edge, r


class TestGracefulDegradation:
    def test_lookup_exception_falls_back_to_baseline_not_crash(self, monkeypatch):
        def _boom(*a, **k):
            raise RuntimeError("simulated SGO lookup failure")
        monkeypatch.setattr(brain, "player_prop_market_prob", _boom)
        recs = brain._scan_hitter_props("Andres Gimenez", "TOR", _stats(), {"sp_missing": True}, 300.0,
                                         _gimenez_rbi_event())
        by_prop = _by_prop(recs)
        assert by_prop["RBI O0.5"]["market_p"] == 0.32

    def test_empty_event_no_crash(self):
        recs = brain._scan_hitter_props("Andres Gimenez", "TOR", _stats(), {"sp_missing": True}, 300.0, {"props": []})
        by_prop = _by_prop(recs)
        assert by_prop["RBI O0.5"]["market_p"] == 0.32


class TestOutputShapeUnchanged:
    """Downstream consumers (_pick_id_hitter, _format_props_message, _norm_h)
    key off this exact dict shape -- wiring in real market data must never
    add, remove, or rename a field."""

    EXPECTED_KEYS = {"player", "team", "prop", "line", "lam", "model_prob",
                      "market_p", "edge_pct", "stake", "n_games"}

    def test_keys_unchanged_without_sgo_event(self):
        recs = brain._scan_hitter_props("Andres Gimenez", "TOR", _stats(), {"sp_missing": True}, 300.0, None)
        for r in recs:
            assert set(r.keys()) == self.EXPECTED_KEYS

    def test_keys_unchanged_with_real_market(self):
        recs = brain._scan_hitter_props("Andres Gimenez", "TOR", _stats(), {"sp_missing": True}, 300.0,
                                         _gimenez_rbi_event())
        for r in recs:
            assert set(r.keys()) == self.EXPECTED_KEYS
