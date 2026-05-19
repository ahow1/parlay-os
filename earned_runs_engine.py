"""PARLAY OS — earned_runs_engine.py
Earned runs prop engine. Sportsbooks price ER props off ERA.
We price them off xFIP — the most predictive ERA estimator.

Edge rule:
  ERA > xFIP + 2.0 → SP due to IMPROVE → bet UNDER earned runs
  ERA < xFIP - 2.0 → SP due to REGRESS → bet OVER earned runs

Additional context: opposing lineup wRC+, TTOP risk, bullpen quality.
"""


def _xfip_tier(xfip: float) -> str:
    if xfip < 3.25:   return "ELITE"
    if xfip < 3.75:   return "GREAT"
    if xfip < 4.25:   return "GOOD"
    if xfip < 4.75:   return "AVERAGE"
    if xfip < 5.25:   return "BAD"
    return "TERRIBLE"


def analyze_earned_runs(sp: dict, opp_off: dict, opp_bullpen: dict | None = None) -> dict | None:
    """
    Analyze earned runs prop for a starting pitcher.

    Args:
        sp:           SP analysis dict from sp_engine.analyze_sp()
        opp_off:      Opposing team offense from offense_engine.analyze_offense()
        opp_bullpen:  Opposing team bullpen dict (optional, affects late-game ER)

    Returns recommendation dict or None if gap < 2.0 (insufficient signal).
    """
    if not sp or sp.get("sp_missing"):
        return None

    era  = sp.get("era",  4.35)
    xfip = sp.get("xfip", 4.35)
    gap  = round(era - xfip, 2)   # positive = ERA inflated vs true skill

    if abs(gap) < 2.0:
        return None

    sp_hand  = sp.get("hand", "R")
    opp_wrc  = opp_off.get("adj_wrc_plus", 100.0)   # lineup wRC+ vs today's SP hand
    ttop     = sp.get("ttop", False)

    # TTOP penalty: facing lineup 3rd time costs ~0.5 ERA equivalent
    ttop_penalty = 0.5 if ttop else 0.0

    # Bullpen quality adjustment to game-total ER context
    bp_adj = 0.0
    if opp_bullpen:
        tier = opp_bullpen.get("fatigue_tier", "MODERATE")
        if tier == "FRESH":
            bp_adj = -0.25   # fresh bullpen limits late runs → fewer total ER
        elif tier == "TIRED":
            bp_adj = 0.25    # tired bullpen bleeds → more total ER

    # Offense adjustment: scale by lineup quality vs league average
    off_adj = (opp_wrc / 100.0) ** 0.5   # square-root dampens extremes

    # Effective xFIP in today's context
    eff_xfip = round(xfip * off_adj + ttop_penalty + bp_adj, 2)

    if gap >= 2.0:
        direction  = "UNDER"
        reasoning  = (
            f"ERA {era:.2f} far above xFIP {eff_xfip:.2f} (raw gap {gap:+.2f}) — "
            f"pitching better than results suggest; regression toward xFIP incoming"
        )
        confidence = _er_confidence(gap, opp_wrc, ttop, direction)
        tier_note  = f"SP xFIP tier: {_xfip_tier(xfip)}"
    else:
        direction  = "OVER"
        reasoning  = (
            f"ERA {era:.2f} far below xFIP {eff_xfip:.2f} (raw gap {gap:+.2f}) — "
            f"ERA masking bad underlying metrics; regression incoming"
        )
        confidence = _er_confidence(abs(gap), opp_wrc, ttop, direction)
        tier_note  = f"SP xFIP tier: {_xfip_tier(xfip)}"

    return {
        "direction":    direction,
        "era":          era,
        "xfip":         xfip,
        "eff_xfip":     eff_xfip,
        "xfip_tier":    _xfip_tier(xfip),
        "gap":          gap,
        "opp_wrc":      opp_wrc,
        "ttop":         ttop,
        "ttop_penalty": ttop_penalty,
        "bp_adj":       bp_adj,
        "confidence":   confidence,
        "reasoning":    reasoning,
        "tier_note":    tier_note,
        "sp_name":      sp.get("name", "SP"),
    }


def _er_confidence(gap: float, opp_wrc: float, ttop: bool, direction: str) -> int:
    """Confidence score for an ER prop recommendation (0–90)."""
    conf = 50

    # Size of ERA-xFIP gap
    if gap >= 3.5:
        conf += 25
    elif gap >= 2.5:
        conf += 18
    elif gap >= 2.0:
        conf += 10

    if direction == "UNDER":
        if opp_wrc < 95:    conf += 10   # weak lineup supports the under
        if not ttop:        conf +=  5   # SP won't face lineup 3x
        if ttop:            conf -=  5   # TTOP hurts the under case
    else:  # OVER
        if opp_wrc > 108:   conf += 10   # strong lineup confirms the over
        if ttop:            conf += 8    # 3rd-time-through penalty applies
        if opp_wrc < 95:    conf -=  5   # weak lineup cuts against the over

    return min(max(conf, 0), 90)


def er_prop_telegram_line(er: dict) -> str:
    """Single-line Telegram summary for an ER prop."""
    if not er:
        return ""
    sp   = er.get("sp_name", "SP")
    era  = er.get("era", 0)
    xfip = er.get("xfip", 0)
    gap  = er.get("gap", 0)
    conf = er.get("confidence", 0)
    dirn = er.get("direction", "")
    return (
        f"📊 ER PROP — {sp} {dirn}: ERA {era:.2f} vs xFIP {xfip:.2f} "
        f"(gap {gap:+.2f}) | conf={conf}/100"
    )


if __name__ == "__main__":
    sp = {
        "name": "Test Pitcher", "era": 6.80, "xfip": 4.30,
        "hand": "R", "ttop": True, "sp_missing": False,
    }
    off = {"adj_wrc_plus": 108.0}
    result = analyze_earned_runs(sp, off)
    if result:
        print(f"Direction: {result['direction']}")
        print(f"Reasoning: {result['reasoning']}")
        print(f"Confidence: {result['confidence']}/100")
        print(er_prop_telegram_line(result))
    else:
        print("No signal (gap < 2.0)")
