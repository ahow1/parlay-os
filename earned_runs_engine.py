"""PARLAY OS — earned_runs_engine.py
Earned runs prop engine. Sportsbooks price ER props off ERA.
We price them off xFIP — the most predictive ERA estimator.

Projection formula:
  base_er   = (xFIP / 9) × expected_innings
  wRC+ adj  = +0.15 ER per 10 pts wRC+ above 100
  TTOP adj  = +0.40 ER (3rd-time-through-the-order penalty)
  low pitch  = reduce to 5.0 IP if SP pace indicates pitch-count limit

Edge rule:
  projected ER < market_line - 0.5  →  UNDER
  projected ER > market_line + 0.5  →  OVER
  8%+ edge threshold (model prob vs -110 market)

Also flags ERA vs xFIP divergence for context.
"""

import math
import logging

log = logging.getLogger(__name__)


# ── SP quality tiers ──────────────────────────────────────────────────────────

def _xfip_tier(xfip: float) -> str:
    if xfip < 3.25:   return "ELITE"
    if xfip < 3.75:   return "GREAT"
    if xfip < 4.25:   return "GOOD"
    if xfip < 4.75:   return "AVERAGE"
    if xfip < 5.25:   return "BAD"
    return "TERRIBLE"


# ── Expected innings ──────────────────────────────────────────────────────────

def _expected_innings(sp: dict) -> float:
    """
    Estimate expected innings for this start.
    Default 5.5; reduced to 5.0 if SP is on a pitch-count limit or high recent pitch count.
    Bumped to 6.0 for TTOP-qualified SPs with elite xFIP.
    """
    ip = float(sp.get("ip", 0) or 0)
    gs = max(int(sp.get("gs", 1) or 1), 1)
    ip_per_start = min(ip / gs, 7.0)

    base_ip = ip_per_start if ip_per_start > 0 else 5.5

    # Reduce if SP hit a pitch-count wall recently
    if sp.get("high_pitch_recent"):
        base_ip = min(base_ip, 5.0)
    elif base_ip < 4.0:
        base_ip = 5.0   # floor for analysis

    return round(base_ip, 1)


# ── Core projection ───────────────────────────────────────────────────────────

def project_earned_runs(
    sp: dict,
    opp_wrc_plus: float = 100.0,
    bullpen: dict | None = None,
) -> float:
    """
    Project expected earned runs for a starting pitcher.

    base_er   = (xFIP / 9) × expected_innings
    wRC+ adj  = +0.15 ER per 10 pts wRC+ above 100
    TTOP adj  = +0.40 ER
    pitch adj = reduce to 5.0 IP if pitch-count limited
    bullpen   = ±0.25 ER for tired/fresh pen (affects late-game inherited runner score)
    """
    xfip    = float(sp.get("xfip", 4.35) or 4.35)
    ttop    = bool(sp.get("ttop"))
    exp_ip  = _expected_innings(sp)

    # Pitch-count limit: reduce to 5.0 IP
    if sp.get("high_pitch_recent") or exp_ip < 4.5:
        exp_ip = min(exp_ip, 5.0)

    # Base expected runs from xFIP
    base_er = (xfip / 9.0) * exp_ip

    # wRC+ adjustment: +0.15 ER per 10 pts above 100
    wrc_delta = max(opp_wrc_plus - 100.0, 0.0)
    wrc_adj   = (wrc_delta / 10.0) * 0.15

    # TTOP penalty: facing lineup third time = +0.4 ER
    ttop_adj = 0.40 if ttop else 0.0

    # Bullpen fatigue context (affects inherited runners being scored)
    bp_adj = 0.0
    if bullpen:
        tier = bullpen.get("fatigue_tier", "MODERATE")
        if tier in ("FRESH",):
            bp_adj = -0.15
        elif tier in ("TIRED", "CRITICAL"):
            bp_adj = +0.20

    projected = round(base_er + wrc_adj + ttop_adj + bp_adj, 2)
    return max(projected, 0.0)


# ── Model probability ─────────────────────────────────────────────────────────

def _er_over_prob(projected: float, market_line: float,
                  std_dev: float = 1.10) -> float:
    """
    P(actual ER > market_line) using normal approximation.
    std_dev ≈ 1.1 runs is the typical game-to-game ER variance.
    """
    z = (market_line + 0.5 - projected) / max(std_dev, 0.01)

    def _phi(x):
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))

    return round(1.0 - _phi(z), 4)


def _er_edge_pct(model_p: float, direction: str) -> float:
    """
    Edge vs -110 market (implied = 52.4%).
    Returns positive edge if model disagrees with market.
    """
    market_p = 0.524   # -110 = 52.38%
    if direction == "OVER":
        return round((model_p - market_p) * 100, 2)
    else:
        return round(((1 - model_p) - market_p) * 100, 2)


# ── Confidence ────────────────────────────────────────────────────────────────

def _er_confidence(
    projected: float,
    market_line: float,
    edge_pct: float,
    opp_wrc: float,
    ttop: bool,
    direction: str,
) -> int:
    gap = abs(projected - market_line)
    conf = 50

    # Gap size: 0.5 = minimal, 1.0 = solid, 1.5+ = strong
    if gap >= 1.5:
        conf += 20
    elif gap >= 1.0:
        conf += 12
    elif gap >= 0.5:
        conf += 5

    # Edge %
    if edge_pct >= 15:
        conf += 12
    elif edge_pct >= 10:
        conf += 7
    elif edge_pct >= 8:
        conf += 4

    if direction == "UNDER":
        if opp_wrc < 95:    conf += 8    # weak lineup: under case strengthened
        if not ttop:        conf += 4    # no TTOP risk
        if ttop:            conf -= 6    # TTOP hurts under case
    else:  # OVER
        if opp_wrc > 108:   conf += 8    # strong lineup: over case strengthened
        if ttop:            conf += 6    # 3rd-time-through penalty confirmed
        if opp_wrc < 95:    conf -= 4    # weak lineup limits over case

    return min(max(conf, 0), 90)


# ── Full analysis ─────────────────────────────────────────────────────────────

def analyze_earned_runs(
    sp: dict,
    opp_off: dict,
    opp_bullpen: dict | None = None,
    market_line: float | None = None,
) -> dict | None:
    """
    Analyze earned runs prop for a starting pitcher.

    Args:
        sp:           SP dict from sp_engine.analyze_sp()
        opp_off:      Opposing offense dict from offense_engine.analyze_offense()
        opp_bullpen:  Opposing bullpen dict (optional, for inherited runner context)
        market_line:  Sportsbook ER line (e.g. 3.5). If None, returns projection only.

    Returns recommendation dict or None when:
        - SP is missing/TBD
        - market_line provided but gap < 0.5 (insufficient signal)
        - edge_pct < 8% vs -110 market
        - confidence < 60
    """
    if not sp or sp.get("sp_missing"):
        return None

    era  = sp.get("era",  4.35)
    xfip = sp.get("xfip", 4.35)
    if xfip is None:
        xfip = era or 4.35

    opp_wrc  = float(opp_off.get("adj_wrc_plus", opp_off.get("wrc_plus", 100.0)) or 100.0)
    ttop     = bool(sp.get("ttop"))
    exp_ip   = _expected_innings(sp)
    projected = project_earned_runs(sp, opp_wrc, opp_bullpen)

    # ERA vs xFIP context (informational, not gating)
    era_xfip_gap = round((era or xfip) - xfip, 2) if era else 0.0
    regression_note = ""
    if era_xfip_gap >= 2.0:
        regression_note = f"ERA {era:.2f} far above xFIP {xfip:.2f} — regression incoming (UNDER signal)"
    elif era_xfip_gap <= -2.0:
        regression_note = f"ERA {era:.2f} far below xFIP {xfip:.2f} — ERA will rise (OVER signal)"

    # Projection-only mode (no market_line provided)
    if market_line is None:
        return {
            "projected_er":   projected,
            "xfip":           xfip,
            "xfip_tier":      _xfip_tier(xfip),
            "exp_ip":         exp_ip,
            "opp_wrc":        opp_wrc,
            "ttop":           ttop,
            "regression_note": regression_note,
            "sp_name":        sp.get("name", "SP"),
        }

    # Gap vs market line
    gap       = round(projected - market_line, 2)
    min_gap   = 0.5
    if abs(gap) < min_gap:
        return None

    direction = "OVER" if gap > 0 else "UNDER"

    # Model probability
    model_p_over = _er_over_prob(projected, market_line)
    model_p = model_p_over if direction == "OVER" else (1.0 - model_p_over)

    edge_pct = _er_edge_pct(model_p_over, direction)

    if edge_pct < 8.0:
        return None

    confidence = _er_confidence(projected, market_line, edge_pct, opp_wrc, ttop, direction)
    if confidence < 60:
        return None

    reasoning = (
        f"{'xFIP' if abs(era_xfip_gap) < 1.0 else 'ERA/xFIP'} suggests {projected:.2f} ER "
        f"vs book's {market_line} (gap {gap:+.2f})"
    )
    if ttop:
        reasoning += " | TTOP +0.40 ER applied"
    if abs(opp_wrc - 100) >= 8:
        wrc_adj = round((opp_wrc - 100.0) / 10.0 * 0.15, 2)
        reasoning += f" | wRC+ adj {wrc_adj:+.2f} ER"
    if regression_note:
        reasoning += f" | {regression_note}"

    return {
        "direction":    direction,
        "projected_er": projected,
        "market_line":  market_line,
        "gap":          gap,
        "era":          era,
        "xfip":         xfip,
        "xfip_tier":    _xfip_tier(xfip),
        "exp_ip":       exp_ip,
        "opp_wrc":      opp_wrc,
        "ttop":         ttop,
        "ttop_adj":     0.40 if ttop else 0.0,
        "model_p":      round(model_p, 4),
        "edge_pct":     edge_pct,
        "confidence":   confidence,
        "reasoning":    reasoning,
        "regression_note": regression_note,
        "sp_name":      sp.get("name", "SP"),
    }


def er_prop_telegram_line(er: dict) -> str:
    """Single-line Telegram summary for an ER prop."""
    if not er:
        return ""
    sp   = er.get("sp_name", "SP")
    proj = er.get("projected_er", 0)
    line = er.get("market_line", "?")
    gap  = er.get("gap", 0)
    xfip = er.get("xfip", 0)
    conf = er.get("confidence", 0)
    dirn = er.get("direction", "")
    edge = er.get("edge_pct", 0)
    ttop = " TTOP" if er.get("ttop") else ""
    return (
        f"📊 ER PROP — {sp} {dirn} {line} "
        f"(proj {proj:.1f}, xFIP {xfip:.2f}, gap {gap:+.2f})"
        f"{ttop} | edge {edge:+.1f}% | conf={conf}/100"
    )


if __name__ == "__main__":
    sp = {
        "name": "Test Pitcher", "era": 6.80, "xfip": 4.30,
        "hand": "R", "ttop": True, "sp_missing": False,
        "ip": 110, "gs": 20,
    }
    off = {"adj_wrc_plus": 108.0}
    result = analyze_earned_runs(sp, off, market_line=4.0)
    if result:
        print(f"Direction: {result['direction']}")
        print(f"Projected: {result['projected_er']:.2f} vs line {result['market_line']}")
        print(f"Edge: {result['edge_pct']:+.1f}% | Conf: {result['confidence']}/100")
        print(er_prop_telegram_line(result))
    else:
        print("No signal (gap < 0.5, edge < 8%, or conf < 60)")

    # Projection-only (no line)
    proj_only = analyze_earned_runs(sp, off)
    if proj_only:
        print(f"\nProjection only: {proj_only['projected_er']:.2f} ER over {proj_only['exp_ip']} IP")
