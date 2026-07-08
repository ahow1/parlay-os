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

# Pool split — daily budget by category. PROPS raised from 0.25 to fit the
# $15-20/pick PROP stake band (2-3 qualifying props/NRFI per day) without the
# pool-budget loop in _daily_bet_slip silently dropping every prop.
POOL_ML     = 2.00
POOL_PROPS  = 0.60
POOL_PARLAY = 0.15

MAX_STAKE_PCT = 0.15   # absolute safety backstop: 15% of bankroll per bet (tier ceilings below stay under this)
MIN_STAKE     = 1.00   # minimum recommended stake

# Full Kelly × conviction multiplier (replaces quarter-Kelly + conviction bands)
CONVICTION_MULTIPLIERS = {
    "HIGH":   0.65,
    "MEDIUM": 0.55,
    "PROP":   0.40,
    "PASS":   0.10,
    "LOW":    0.10,
}

# Per-conviction stake floor/ceiling as a fraction of bankroll. Kelly (mult × full_kelly)
# still decides where a bet lands *within* the band based on edge strength; the floor/ceiling
# guarantee the tier lands where Aidan wants it regardless of how close to the qualifying
# threshold the edge is. At $300 bankroll: HIGH=$30-40 (sharp), MEDIUM=$20-25 (value),
# PROP=$15-20 (K-props/hitter props/NRFI/totals). PASS/LOW have no floor — they're the
# below-threshold fallback tier and should stay small.
CONVICTION_BANDS = {
    "HIGH":   (30.0 / 300, 40.0 / 300),
    "MEDIUM": (20.0 / 300, 25.0 / 300),
    "PROP":   (15.0 / 300, 20.0 / 300),
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
    return real_sizing_bankroll()


def real_sizing_bankroll() -> float:
    """Settled P&L bankroll — never uses BANKROLL_OVERRIDE. Used for drawdown calculations."""
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


def real_peak_bankroll() -> float:
    """Peak settled P&L — never uses BANKROLL_OVERRIDE. Used for drawdown calculations."""
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


def daily_pnl() -> float:
    """Today's settled P&L in dollars (ET date). Positive = profit, negative = loss."""
    import pytz
    from datetime import datetime
    ET_tz = pytz.timezone("America/New_York")
    today = datetime.now(ET_tz).strftime("%Y-%m-%d")
    bets  = _db.get_bets(date=today)
    pnl   = 0.0
    for b in bets:
        result = b.get("result")
        stake  = float(b.get("stake") or 0)
        if result == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                pnl += (dec - 1) * stake
        elif result == "L":
            pnl -= stake
    return round(pnl, 2)


def is_daily_stop_loss_active() -> bool:
    """Return True if today's loss has hit -3% of sizing bankroll."""
    pnl = daily_pnl()
    if pnl >= 0:
        return False
    br = sizing_bankroll()
    if br <= 0:
        return False
    return abs(pnl) / br >= 0.03


# ── Tiered daily budget ────────────────────────────────────────────────────────

def daily_budget_pct(br: float) -> float:
    """Daily risk budget as fraction of bankroll, scaling up as the account grows.
    Raised from the legacy 12/15/18/20% tiers to fit the $30-40 HIGH stake band —
    at the old 15% tier ($45 at $300), a single lock could consume the entire
    daily cap and BLOCK every other qualifying bet for the rest of the day."""
    if br >= 1000:
        return 0.40
    if br >= 500:
        return 0.35
    if br >= 300:
        return 0.30
    return 0.25


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
    """Remaining pool budget for today (floored at $0.00 — never goes negative)."""
    if br is None:
        br = sizing_bankroll()
    raw = round(pool_budget(pool, br) - pool_exposure(pool), 2)
    if raw < 0:
        import logging as _logging
        _logging.getLogger(__name__).warning(
            "[POOL] %s pool over budget — exposure exceeds limit by $%.2f", pool, -raw
        )
    return max(0.0, raw)


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
        br = real_sizing_bankroll()
    if pk is None:
        pk = real_peak_bankroll()
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
    Full Kelly × conviction-multiplier stake, clamped into a per-tier band, with
    tiered drawdown protection.
      HIGH   (sharp / ML locks)             — $30-40 at $300 bankroll
      MEDIUM (value / ML flips)             — $20-25 at $300 bankroll
      PROP   (K-props/hitter props/NRFI/totals) — $15-20 at $300 bankroll
      PASS/LOW — no floor, capped by MAX_STAKE_PCT only (below-threshold fallback)
    Kelly still decides where a bet lands *within* its band based on edge strength;
    the band floor/ceiling are expressed as % of bankroll (see CONVICTION_BANDS) so
    they rescale automatically if the bankroll changes.
    Absolute safety backstop: MAX_STAKE_PCT of sizing_bankroll. Rounded to $0.10.
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

    conv = conviction.upper()
    mult = CONVICTION_MULTIPLIERS.get(conv, 0.25)
    kelly_pct = full_kelly * mult

    band = CONVICTION_BANDS.get(conv)
    if band:
        floor_pct, ceiling_pct = band
        kelly_pct = min(max(kelly_pct, floor_pct), ceiling_pct)
    kelly_pct = min(kelly_pct, MAX_STAKE_PCT)   # absolute safety backstop

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


def capture_pre_game_clv() -> int:
    """
    Fetch closing odds for today's pending bets and write to clv_log.
    Called ~1 hour before first pitch. Returns number of rows written.
    """
    import pytz
    from datetime import datetime
    ET_tz = pytz.timezone("America/New_York")
    today  = datetime.now(ET_tz).strftime("%Y-%m-%d")
    bets   = _db.get_bets(date=today, unresolved_only=True)
    if not bets:
        return 0

    try:
        from telegram_handler import _fetch_closing_odds
    except Exception:
        return 0

    try:
        from math_engine import calc_clv as _calc_clv
    except Exception:
        _calc_clv = None

    written = 0
    for b in bets:
        team     = b.get("bet") or ""
        bet_type = b.get("type") or b.get("bet_type") or "ML"
        bet_odds = str(b.get("bet_odds") or "")
        if not team or not bet_odds:
            continue
        closing = _fetch_closing_odds(team, bet_type)
        if not closing:
            continue
        clv_pct = None
        if _calc_clv:
            try:
                clv_pct = _calc_clv(bet_odds, closing).get("clv_pct")
            except Exception:
                pass
        try:
            _db.log_clv(
                date=today,
                bet=team,
                bet_type=bet_type,
                game=b.get("game") or "",
                sp=b.get("sp") or "",
                park=b.get("park") or "",
                umpire=b.get("umpire") or "",
                bet_odds=bet_odds,
                closing_odds=closing,
                clv_pct=clv_pct,
                result=None,
                model=b.get("model") or "12-factor",
                edge_pct=b.get("edge_pct"),
            )
            written += 1
        except Exception:
            pass
    return written


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
