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
    "HIGH":   (0.03, 0.05),  # 3-5% of bankroll
    "MEDIUM": (0.01, 0.03),  # 1-3%
    "LOW":    (0.005, 0.01), # 0.5-1%
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
    """Sum of stakes on today's unsettled bets (ET date), deduplicated by (game, bet, type)."""
    import pytz
    from datetime import datetime
    ET_tz = pytz.timezone("America/New_York")
    today = datetime.now(ET_tz).strftime("%Y-%m-%d")
    bets  = _db.get_bets()
    seen: set = set()
    total = 0.0
    for b in bets:
        if b.get("result") or b.get("date") != today:
            continue
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


def kelly_stake(model_prob: float, odds_american: str, conviction: str = "MEDIUM",
                fraction: float = 0.25) -> float:
    """
    Quarter-Kelly stake.
    Returns dollar amount, capped by daily exposure, per-bet max, and drawdown check.
    """
    if is_drawdown_pause():
        return 0.0

    dec  = american_to_decimal(odds_american)
    if not dec or dec <= 1:
        return 0.0
    if model_prob <= 0 or model_prob >= 1:
        return 0.0

    b = dec - 1  # decimal profit per unit
    q = 1.0 - model_prob
    kelly_pct = (model_prob * b - q) / b

    if kelly_pct <= 0:
        return 0.0

    # Scale by quarter-Kelly
    kelly_pct *= fraction

    # Clamp to conviction tier range
    lo, hi = CONVICTION_KELLY.get(conviction, (0.01, 0.03))
    kelly_pct = max(lo, min(kelly_pct, hi))

    br    = current_bankroll()
    if br <= 0:
        return 0.0

    stake = round(br * kelly_pct, 2)

    # Cap per bet
    stake = min(stake, MAX_BET_ABS)

    # Daily cap check
    remaining_cap = br * DAILY_CAP_PCT - daily_exposure()
    if remaining_cap <= 0:
        return 0.0
    stake = min(stake, round(remaining_cap, 2))

    # Round to nearest $0.10
    stake = round(round(stake / 0.10) * 0.10, 2)
    return max(stake, 0.0)


def sizing_summary(model_prob: float, odds: str, conviction: str) -> dict:
    """Return full sizing breakdown for display."""
    br    = current_bankroll()
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
