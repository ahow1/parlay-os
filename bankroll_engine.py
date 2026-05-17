"""PARLAY OS — bankroll_engine.py
Kelly sizing, tiered daily budget, pool system, drawdown protection.
Starting bankroll: $150.
"""

import os
from datetime import date
import db as _db
from math_engine import STARTING_BANKROLL, american_to_decimal

# ── Drawdown tiers ─────────────────────────────────────────────────────────────
DRAWDOWN_MINOR      = 0.10   # -10%: reduce stakes 25%
DRAWDOWN_PROPS_ONLY = 0.15   # -15%: reduce stakes 50%, ML bets blocked
DRAWDOWN_PAUSE      = 0.20   # -20%: full pause + Telegram alert

# Pool split — daily budget by category
POOL_ML     = 0.60
POOL_PROPS  = 0.25
POOL_PARLAY = 0.15

MAX_STAKE_PCT = 0.06   # hard cap: 6% of bankroll per bet
MIN_STAKE     = 1.00   # minimum recommended stake

# Full Kelly × conviction multiplier (replaces quarter-Kelly + conviction bands)
CONVICTION_MULTIPLIERS = {
    "HIGH":   0.40,
    "MEDIUM": 0.25,
    "PASS":   0.10,
    "LOW":    0.10,
}

# Which bet_type strings belong to each pool
_POOL_BET_TYPES = {
    "ML":     {"ML", "STRAIGHT", "MONEYLINE", "F5"},
    "PROPS":  {"PROP", "PLAYER_PROP", "NRFI", "TOTAL"},
    "PARLAY": {"PARLAY"},
}


# ── Bankroll queries ───────────────────────────────────────────────────────────

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


def sizing_bankroll() -> float:
    """Bankroll for Kelly sizing: settled P&L only — does NOT subtract pending bets.
    Pending bets are capital at risk, not lost capital. Using current_bankroll() here
    would cause phantom drawdown that collapses stakes on every scout run."""
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


# ── Tiered daily budget ────────────────────────────────────────────────────────

def daily_budget_pct(br: float) -> float:
    """Daily risk budget as fraction of bankroll, scaling up as the account grows."""
    if br >= 1000:
        return 0.20
    if br >= 500:
        return 0.18
    if br >= 300:
        return 0.15
    return 0.12


def daily_budget(br: float | None = None) -> float:
    """Total daily risk budget in dollars."""
    if br is None:
        br = sizing_bankroll()
    return round(br * daily_budget_pct(br), 2)


# ── Pool budgets ───────────────────────────────────────────────────────────────

def pool_budget(pool: str, br: float | None = None) -> float:
    """Dollar budget allocated to a pool today (before any bets placed)."""
    if br is None:
        br = sizing_bankroll()
    budget = daily_budget(br)
    pct = {"ML": POOL_ML, "PROPS": POOL_PROPS, "PARLAY": POOL_PARLAY}.get(pool.upper(), 0)
    return round(budget * pct, 2)


def pool_exposure(pool: str) -> float:
    """Today's pending stakes for a pool, inferred from bet_type in the DB."""
    import pytz
    from datetime import datetime
    ET_tz = pytz.timezone("America/New_York")
    today = datetime.now(ET_tz).strftime("%Y-%m-%d")
    types = _POOL_BET_TYPES.get(pool.upper(), set())
    bets  = _db.get_bets(date=today, unresolved_only=True)
    seen: set = set()
    total = 0.0
    for b in bets:
        key = (b.get("game", ""), b.get("bet", ""), b.get("type", ""))
        if key in seen:
            continue
        seen.add(key)
        bt = (b.get("type") or b.get("bet_type") or "").upper()
        if bt in types:
            total += float(b.get("stake") or 0)
    return round(total, 2)


def pool_remaining(pool: str, br: float | None = None) -> float:
    """Remaining pool budget for today."""
    if br is None:
        br = sizing_bankroll()
    return round(pool_budget(pool, br) - pool_exposure(pool), 2)


# ── Drawdown ───────────────────────────────────────────────────────────────────

def drawdown_tier(br: float | None = None, pk: float | None = None) -> dict:
    """
    Return drawdown status and stake scale factor.
    tier 0: <10%  → scale 1.00 (full stakes)
    tier 1: 10-15% → scale 0.75
    tier 2: 15-20% → scale 0.50, props only (ML blocked)
    tier 3: ≥20%  → scale 0.00, full pause + alert
    """
    if br is None:
        br = sizing_bankroll()
    if pk is None:
        pk = peak_bankroll()
    if pk <= 0:
        return {"tier": 0, "pct": 0.0, "scale": 1.0, "props_only": False, "pause": False}
    dd = max(0.0, (pk - br) / pk)
    dd_pct = round(dd * 100, 1)
    if dd >= DRAWDOWN_PAUSE:
        return {"tier": 3, "pct": dd_pct, "scale": 0.0, "props_only": False, "pause": True}
    elif dd >= DRAWDOWN_PROPS_ONLY:
        return {"tier": 2, "pct": dd_pct, "scale": 0.50, "props_only": True, "pause": False}
    elif dd >= DRAWDOWN_MINOR:
        return {"tier": 1, "pct": dd_pct, "scale": 0.75, "props_only": False, "pause": False}
    else:
        return {"tier": 0, "pct": dd_pct, "scale": 1.0, "props_only": False, "pause": False}


def is_drawdown_pause() -> bool:
    return drawdown_tier()["pause"]


# ── Kelly sizing ───────────────────────────────────────────────────────────────

def kelly_stake(
    model_prob: float,
    odds_american: str,
    conviction: str = "MEDIUM",
    fraction: float = 1.0,   # kept for API compatibility; ignored (multipliers replace it)
    edge_pct: float = 0.0,
) -> float:
    """
    Full Kelly × conviction-multiplier stake with tiered drawdown protection.
      HIGH   × 0.40 — target range ~$9.40–$14.10 at $235
      MEDIUM × 0.25 — target range ~$4.70–$9.40
      PASS   × 0.10 — target range ~$1.00–$4.70
    Hard cap: 6% of sizing_bankroll. Min stake: $1.00. Rounded to $0.10.
    Returns 0.0 when drawdown pause is active or Kelly is negative.
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

    mult = CONVICTION_MULTIPLIERS.get(conviction.upper(), 0.25)
    kelly_pct = full_kelly * mult
    kelly_pct = min(kelly_pct, MAX_STAKE_PCT)

    br = sizing_bankroll()
    if br <= 0:
        return 0.0

    dd = drawdown_tier(br, peak_bankroll())
    if dd["pause"]:
        return 0.0
    kelly_pct *= dd["scale"]

    stake = round(br * kelly_pct, 2)
    stake = round(round(stake / 0.10) * 0.10, 2)
    return max(stake, 0.0)   # caller applies MIN_STAKE after pool check


def sizing_summary(model_prob: float, odds: str, conviction: str) -> dict:
    """Return full sizing breakdown for display."""
    br     = sizing_bankroll()
    peak   = peak_bankroll()
    stake  = kelly_stake(model_prob, odds, conviction)
    dd     = drawdown_tier(br, peak)
    daily  = daily_exposure()
    budget = daily_budget(br)

    return {
        "current_bankroll":       br,
        "peak_bankroll":          peak,
        "drawdown_pct":           dd["pct"],
        "drawdown_tier":          dd["tier"],
        "drawdown_pause":         dd["pause"],
        "props_only":             dd["props_only"],
        "daily_budget":           budget,
        "daily_exposure":         daily,
        "daily_remaining":        round(budget - daily, 2),
        "pool_ml_remaining":      pool_remaining("ML", br),
        "pool_props_remaining":   pool_remaining("PROPS", br),
        "pool_parlay_remaining":  pool_remaining("PARLAY", br),
        "recommended_stake":      stake,
        "conviction":             conviction,
    }


# ── Growth tracker ─────────────────────────────────────────────────────────────

def growth_tracker() -> dict:
    """Week/month/all-time P&L and on-pace monthly projection."""
    from datetime import timedelta
    today = date.today()
    week_start  = (today - timedelta(days=7)).isoformat()
    month_start = today.replace(day=1).isoformat()

    bets    = _db.get_bets()
    settled = [b for b in bets if b.get("result") in ("W", "L")]

    def _pnl(subset):
        pnl = 0.0
        for b in subset:
            r = b.get("result")
            s = float(b.get("stake") or 0)
            if r == "W":
                dec = american_to_decimal(str(b.get("bet_odds", "")))
                if dec:
                    pnl += (dec - 1) * s
            elif r == "L":
                pnl -= s
        return round(pnl, 2)

    week_bets  = [b for b in settled if (b.get("date") or "") >= week_start]
    month_bets = [b for b in settled if (b.get("date") or "") >= month_start]

    br_now     = sizing_bankroll()
    total_pnl  = round(br_now - float(STARTING_BANKROLL), 2)
    week_pnl   = _pnl(week_bets)
    month_pnl  = _pnl(month_bets)

    day_of_month  = max(today.day, 1)
    monthly_pace  = round((month_pnl / day_of_month) * 30, 2)

    starting   = float(STARTING_BANKROLL)
    all_pct    = round(total_pnl / starting * 100, 1) if starting > 0 else 0.0
    week_base  = max(br_now - week_pnl, 0.01)
    month_base = max(br_now - month_pnl, 0.01)
    week_pct   = round(week_pnl  / week_base  * 100, 1)
    month_pct  = round(month_pnl / month_base * 100, 1)

    return {
        "all_time_pnl":    total_pnl,
        "all_time_pct":    all_pct,
        "week_pnl":        week_pnl,
        "week_pct":        week_pct,
        "month_pnl":       month_pnl,
        "month_pct":       month_pct,
        "monthly_pace":    monthly_pace,
        "current_bankroll": br_now,
    }


if __name__ == "__main__":
    br     = sizing_bankroll()
    pk     = peak_bankroll()
    dd     = drawdown_tier(br, pk)
    budget = daily_budget(br)
    print(f"Current bankroll: ${br:.2f}")
    print(f"Peak bankroll:    ${pk:.2f}")
    print(f"Daily budget:     ${budget:.2f} ({daily_budget_pct(br)*100:.0f}% tier)")
    print(f"Drawdown:         {dd['pct']:.1f}% (tier {dd['tier']}) scale={dd['scale']}")
    print(f"  ML pool:        ${pool_budget('ML', br):.2f} / remaining ${pool_remaining('ML', br):.2f}")
    print(f"  PROPS pool:     ${pool_budget('PROPS', br):.2f} / remaining ${pool_remaining('PROPS', br):.2f}")
    print(f"  PARLAY pool:    ${pool_budget('PARLAY', br):.2f} / remaining ${pool_remaining('PARLAY', br):.2f}")
    stake = kelly_stake(0.55, "+130", "HIGH")
    print(f"Kelly stake (55% prob, +130, HIGH): ${stake:.2f}")
    gt = growth_tracker()
    print(f"Growth: all-time {gt['all_time_pct']:+.1f}%  week {gt['week_pct']:+.1f}%  month {gt['month_pct']:+.1f}%")
    print(f"Monthly pace: ${gt['monthly_pace']:+.2f}")
