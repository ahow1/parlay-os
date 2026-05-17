"""PARLAY OS — bankroll_engine.py
Kelly sizing, daily cap, drawdown protection, stake rounding.
Starting bankroll: $150.
"""

import os
import db as _db
from math_engine import STARTING_BANKROLL, american_to_decimal

# Daily cap as fraction of current bankroll
DAILY_CAP_PCT  = 0.25   # max 10% of bankroll in action per day
# Per-bet hard max
MAX_BET_ABS    = 15.00
# Drawdown pause threshold
DRAWDOWN_PAUSE = 0.15   # pause if bankroll drops 15% below peak

CONVICTION_KELLY = {
    "HIGH":   (0.04, 0.06),  # 4-6% of bankroll  (~$9.40–$14.10 at $235)
    "MEDIUM": (0.02, 0.04),  # 2-4%              (~$4.70–$9.40  at $235)
    "LOW":    (0.01, 0.02),  # 1-2%              (~$2.35–$4.70  at $235)
}

# Edge bounds used to proportionally scale the ceiling within each tier.
# At edge_lo the ceiling equals the floor; at edge_hi it reaches the full cap.
_CONVICTION_EDGE_RANGES = {
    "HIGH":   (7.0, 10.0),
    "MEDIUM": (4.0,  7.0),
    "LOW":    (3.0,  4.0),
}


def current_bankroll() -> float:
    override = os.getenv("BANKROLL_OVERRIDE")
    if override:
        return round(float(override), 2)

    bets = _db.get_bets()
    current = float(STARTING_BANKROLL)
    for b in bets:
        result = b.get("result")
        stake  = float(b.get("stake") or 0)
        if result == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                current += (dec - 1) * stake
        elif result == "L":
            current -= stake
    pending = sum(float(b.get("stake") or 0) for b in bets if not b.get("result"))
    return round(current - pending, 2)


def peak_bankroll() -> float:
    override = os.getenv("BANKROLL_OVERRIDE")
    if override:
        return round(float(override), 2)

    bets = _db.get_bets()
    current = float(STARTING_BANKROLL)
    peak    = float(STARTING_BANKROLL)
    for b in bets:
        result = b.get("result")
        stake  = float(b.get("stake") or 0)
        if result == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                current += (dec - 1) * stake
            peak = max(peak, current)
        elif result == "L":
            current -= stake
    return round(peak, 2)


def daily_exposure() -> float:
    """Sum of stakes on today's pending bets (result IS NULL), deduplicated by (game, bet, type)."""
    import pytz
    from datetime import datetime
    ET_tz = pytz.timezone("America/New_York")
    today = datetime.now(ET_tz).strftime("%Y-%m-%d")
    bets  = _db.get_bets(date=today, unresolved_only=True)
    seen: set = set()
    total = 0.0
    for b in bets:
        key = (b.get("game", ""), b.get("bet", ""), b.get("type", ""))
        if key in seen:
            continue
        seen.add(key)
        total += float(b.get("stake") or 0)
    return round(total, 2)


def is_drawdown_pause() -> bool:
    cur  = current_bankroll()
    peak = peak_bankroll()
    if peak <= 0:
        return False
    drawdown = (peak - cur) / peak
    return drawdown >= DRAWDOWN_PAUSE


def sizing_bankroll() -> float:
    """Bankroll for Kelly sizing: cumulative settled P&L only — does NOT subtract pending.
    Pending bets are capital at risk, not lost capital. Kelly must size against total
    bankroll or it shrinks every scout run and collapses to near-zero stakes."""
    override = os.getenv("BANKROLL_OVERRIDE")
    if override:
        return round(float(override), 2)
    bets = _db.get_bets()
    current = float(STARTING_BANKROLL)
    for b in bets:
        result = b.get("result")
        stake  = float(b.get("stake") or 0)
        if result == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                current += (dec - 1) * stake
        elif result == "L":
            current -= stake
    return round(current, 2)


def kelly_stake(model_prob: float, odds_american: str, conviction: str = "MEDIUM",
                fraction: float = 0.25, edge_pct: float = 0.0) -> float:
    """
    Quarter-Kelly stake with drawdown-adjusted sizing and detailed diagnostics.
    Sizes against sizing_bankroll() (settled P&L only) so pending bets don't
    collapse stake to zero. Drawdown reduces stake proportionally.
    """
    dec = american_to_decimal(odds_american)
    if not dec or dec <= 1:
        return 0.0
    if model_prob <= 0 or model_prob >= 1:
        return 0.0

    b = dec - 1
    q = 1.0 - model_prob
    full_kelly = (model_prob * b - q) / b

    if full_kelly <= 0:
        return 0.0

    quarter_kelly = full_kelly * fraction

    lo, hi = CONVICTION_KELLY.get(conviction, (0.02, 0.04))

    # Proportionally scale the ceiling within each tier based on edge size.
    # At the tier's minimum edge the ceiling equals the floor (minimum stake).
    # At the tier's maximum edge the ceiling reaches the full cap.
    if edge_pct > 0 and conviction in _CONVICTION_EDGE_RANGES:
        e_lo, e_hi = _CONVICTION_EDGE_RANGES[conviction]
        t  = min(max((edge_pct - e_lo) / max(e_hi - e_lo, 1e-6), 0.0), 1.0)
        hi = round(lo + t * (hi - lo), 6)

    kelly_pct = max(lo, min(quarter_kelly, hi))

    br = sizing_bankroll()
    if br <= 0:
        return 0.0

    pk = peak_bankroll()
    drawdown = max(0.0, (pk - br) / pk) if pk > 0 else 0.0
    dd_scale  = 1.0
    if drawdown >= DRAWDOWN_PAUSE:
        dd_scale  = max(0.25, 1.0 - drawdown)
        kelly_pct = round(kelly_pct * dd_scale, 6)

    stake = round(br * kelly_pct, 2)
    stake = min(stake, MAX_BET_ABS)
    stake = round(round(stake / 0.10) * 0.10, 2)
    return max(stake, 0.0)


def sizing_summary(model_prob: float, odds: str, conviction: str) -> dict:
    """Return full sizing breakdown for display."""
    br    = sizing_bankroll()
    peak  = peak_bankroll()
    stake = kelly_stake(model_prob, odds, conviction)
    dd    = (peak - br) / peak if peak > 0 else 0.0
    daily = daily_exposure()
    cap   = br * DAILY_CAP_PCT

    return {
        "current_bankroll": br,
        "peak_bankroll":    peak,
        "drawdown_pct":     round(dd * 100, 1),
        "drawdown_pause":   is_drawdown_pause(),
        "daily_exposure":   daily,
        "daily_cap":        round(cap, 2),
        "daily_remaining":  round(cap - daily, 2),
        "recommended_stake": stake,
        "conviction":       conviction,
    }


if __name__ == "__main__":
    print(f"Current bankroll: ${current_bankroll():.2f}")
    print(f"Peak bankroll:    ${peak_bankroll():.2f}")
    print(f"Daily exposure:   ${daily_exposure():.2f}")
    print(f"Drawdown pause:   {is_drawdown_pause()}")
    stake = kelly_stake(0.55, "+130", "HIGH")
    print(f"Kelly stake (55% prob, +130, HIGH): ${stake:.2f}")
