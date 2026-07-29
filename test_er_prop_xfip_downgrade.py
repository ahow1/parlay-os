"""
FIX #1 (2026-07-29 prop-slip review): earned_runs_engine.py must not treat an
estimated xFIP (sp_engine's ERA/BB9/K9/HR9 formula fallback, used whenever
FanGraphs/pybaseball both miss a pitcher) the same as a real one. A real
xFIP-driven signal should be able to fire at full strength; an identical
signal built on an estimated xFIP must show a smaller edge_pct/confidence
and get flagged, per the "downgrade, don't silently trust" pattern brain.py
already applies to SP-unknown moneyline picks.

Run with: python -m pytest test_er_prop_xfip_downgrade.py -v
"""

import copy

from earned_runs_engine import analyze_earned_runs, XFIP_ESTIMATE_CONF_CAP

# A big, clean ERA/xFIP regression signal — real book-shaped market line,
# large enough gap to clear the 8% edge / 60 confidence gates even after
# the estimated-xFIP shrink is applied.
BASE_SP = {
    "name": "Test Pitcher", "era": 6.80, "xfip": 4.30,
    "hand": "R", "ttop": True, "sp_missing": False,
    "ip": 110, "gs": 20,
}
OPP_OFF = {"adj_wrc_plus": 108.0}
MARKET_LINE = 4.0


def _sp_with_source(source_key_present: bool, source_value: str | None):
    sp = copy.deepcopy(BASE_SP)
    if source_key_present:
        sp["xfip_source"] = source_value
    return sp


def test_real_fangraphs_source_unaffected():
    sp = _sp_with_source(True, "fangraphs")
    result = analyze_earned_runs(sp, OPP_OFF, market_line=MARKET_LINE)
    assert result is not None
    assert result["xfip_is_estimated"] is False


def test_real_pybaseball_source_unaffected():
    sp = _sp_with_source(True, "pybaseball")
    result = analyze_earned_runs(sp, OPP_OFF, market_line=MARKET_LINE)
    assert result is not None
    assert result["xfip_is_estimated"] is False


def test_missing_xfip_source_key_treated_as_real():
    """Hand-built dicts (tests, __main__ self-test) that never set
    xfip_source must keep today's behavior — only sp_engine's explicit
    'estimated'/'unavailable' tags should trigger the downgrade."""
    sp = _sp_with_source(False, None)
    result = analyze_earned_runs(sp, OPP_OFF, market_line=MARKET_LINE)
    assert result is not None
    assert result["xfip_is_estimated"] is False


def test_estimated_source_shrinks_edge_and_confidence():
    real_sp = _sp_with_source(True, "fangraphs")
    est_sp  = _sp_with_source(True, "estimated")

    real = analyze_earned_runs(real_sp, OPP_OFF, market_line=MARKET_LINE)
    est  = analyze_earned_runs(est_sp,  OPP_OFF, market_line=MARKET_LINE)

    assert real is not None
    assert est is not None
    assert est["xfip_is_estimated"] is True
    assert est["edge_pct"] < real["edge_pct"]
    assert est["confidence"] <= real["confidence"]
    assert "estimated" in est["reasoning"].lower()


def test_unavailable_source_also_downgraded():
    sp = _sp_with_source(True, "unavailable")
    result = analyze_earned_runs(sp, OPP_OFF, market_line=MARKET_LINE)
    assert result is not None
    assert result["xfip_is_estimated"] is True


def test_estimated_source_can_still_fire_on_a_big_enough_signal():
    """Downgrade, don't block outright — a strong enough regression signal
    must still clear the gates even with the shrink applied."""
    sp = _sp_with_source(True, "estimated")
    result = analyze_earned_runs(sp, OPP_OFF, market_line=MARKET_LINE)
    assert result is not None


def test_confidence_cap_actually_clamps():
    """This engine's own math means edge_pct rarely stays in the marginal
    zone near the 8% gate (even the minimum allowed 0.5-run gap already
    produces ~29% edge with std_dev=1.10), so the shrink mostly can't push
    a real signal below the edge/confidence gates outright. The confidence
    CAP is what actually bites for strong signals: a pick whose real
    confidence would exceed XFIP_ESTIMATE_CONF_CAP must be clamped down to
    it once xFIP is flagged estimated."""
    strong_sp = {
        "name": "Test Pitcher", "era": 2.80, "xfip": 2.30,
        "hand": "R", "ttop": False, "sp_missing": False,
        "ip": 110, "gs": 20,
    }
    strong_off = {"adj_wrc_plus": 85.0}

    real = analyze_earned_runs(_sp_with_source_dict(strong_sp, "fangraphs"),
                                strong_off, market_line=4.5)
    est  = analyze_earned_runs(_sp_with_source_dict(strong_sp, "estimated"),
                                strong_off, market_line=4.5)

    assert real is not None and est is not None
    assert real["confidence"] > XFIP_ESTIMATE_CONF_CAP, \
        "test fixture must produce a real confidence above the cap, or this test proves nothing"
    assert est["confidence"] == XFIP_ESTIMATE_CONF_CAP


def _sp_with_source_dict(sp: dict, source: str) -> dict:
    sp = copy.deepcopy(sp)
    sp["xfip_source"] = source
    return sp


def test_telegram_line_flags_estimated_xfip():
    from earned_runs_engine import er_prop_telegram_line
    sp = _sp_with_source(True, "estimated")
    result = analyze_earned_runs(sp, OPP_OFF, market_line=MARKET_LINE)
    assert result is not None
    line = er_prop_telegram_line(result)
    assert "xFIP~est" in line
