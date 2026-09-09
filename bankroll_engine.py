"""PARLAY OS — bankroll_engine.py
Kelly sizing, tiered daily budget, pool system, drawdown protection.
Starting bankroll: $150.
"""

import os
from datetime import date, datetime, timezone
import db as _db
from math_engine import STARTING_BANKROLL, american_to_decimal

# A pending bet whose game date is this far in the past with no result is
# almost certainly orphaned (postponed/rescheduled game, API gap, settlement
# mismatch) rather than real open exposure — see AUDIT.md B10.
STUCK_PENDING_HOURS = 48

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
POOL_RUNLINE = POOL_ML   # mirrors the ML bucket — same market shape (game-level side pick)

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

# Which bet_type strings belong to each pool. RUNLINE bet_type carries the fixed
# MLB line as a suffix (RUNLINE-1.5 / RUNLINE+1.5) so settlement/CLV grading can
# tell favorite from underdog — see _determine_outcome() in telegram_handler.py.
_POOL_BET_TYPES = {
    "ML":      {"ML", "STRAIGHT", "MONEYLINE", "F5"},
    "PROPS":   {"PROP", "PLAYER_PROP", "NRFI", "TOTAL"},
    "PARLAY":  {"PARLAY"},
    "RUNLINE": {"RUNLINE-1.5", "RUNLINE+1.5"},
}


# ── Bankroll queries ───────────────────────────────────────────────────────────

def _bet_age_hours(bet: dict) -> float | None:
    """Hours elapsed since this bet's logged (game) date began, UTC basis.
    Returns None if the date is missing/unparsable."""
    bet_date = bet.get("date")
    if not bet_date:
        return None
    try:
        bet_dt = datetime.strptime(bet_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None
    return (datetime.now(timezone.utc) - bet_dt).total_seconds() / 3600.0


def _is_stuck_pending(bet: dict, hours: float = STUCK_PENDING_HOURS) -> bool:
    """A pending bet is 'stuck' if it's unresolved and its game date is more
    than `hours` in the past — see AUDIT.md B10."""
    if bet.get("result"):
        return False
    age = _bet_age_hours(bet)
    return age is not None and age > hours


def get_stuck_pending_bets(hours: float = STUCK_PENDING_HOURS) -> list:
    """Pending bets that are almost certainly orphaned rather than real open
    exposure — surfaced so they're operator-visible instead of silently
    deflating current_bankroll()."""
    return [b for b in _db.get_bets() if _is_stuck_pending(b, hours)]


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
    pending = sum(
        float(b.get("stake") or 0) for b in bets
        if not b.get("result") and not _is_stuck_pending(b)
    )
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


_anchor_healed = False


def ensure_bankroll_anchor(default_value: float = 300.0) -> None:
    """Self-heal: guarantee a bankroll_anchor row exists and no pre-anchor settled
    bets are left live in `bets`.

    fc19638 added the anchor/archive machinery but never called it, so drawdown
    silently kept replaying from STARTING_BANKROLL through 9 stale May seed bets
    (phantom 22% drawdown, see [problem B]). This closes that gap AND makes the
    fix durable: GitHub Actions checks out and re-commits parlay_os.db on every
    run, so a one-time manual archive is a single point of failure — any future
    DB reset/rollback/manual edit would silently reintroduce the bug. Running
    this idempotently on every process start means a fresh checkout always
    self-corrects instead of depending on the archived DB state surviving.

    Cheap and safe to call repeatedly: no-ops after the first successful run
    in a given process (module-level guard), and archive_bets()/set_bankroll_
    anchor() are themselves no-ops on an already-healed DB.
    """
    global _anchor_healed
    if _anchor_healed:
        return
    import pytz
    ET = pytz.timezone("America/New_York")
    today = datetime.now(ET).strftime("%Y-%m-%d")

    anchor = _db.get_bankroll_anchor()
    if anchor is None:
        value = float(os.getenv("BANKROLL_OVERRIDE") or default_value)
        _db.set_bankroll_anchor(value, note="self-heal: no anchor existed, seeded from confirmed bankroll")
        anchor = (value, today)

    _, anchor_date = anchor
    stale_ids = [
        b["id"] for b in _db.get_bets()
        if b.get("result") is not None and (b.get("date") or "") < anchor_date
    ]
    if stale_ids:
        _db.archive_bets(stale_ids, reason=f"pre-anchor settled bet (anchor_date={anchor_date})")
    _anchor_healed = True


def _anchor_start_and_bets():
    """Starting balance + the settled bets to replay for real_sizing_bankroll/real_peak_bankroll.
    Uses the most recent bankroll_anchor checkpoint (a manually-confirmed real bankroll,
    set via db.set_bankroll_anchor — never BANKROLL_OVERRIDE) if one exists, replaying only
    bets settled after the anchor date. Falls back to STARTING_BANKROLL + full history
    when no anchor has been set."""
    ensure_bankroll_anchor()
    anchor = _db.get_bankroll_anchor()
    if anchor:
        start, anchor_date = anchor
        bets = [b for b in _db.get_bets() if (b.get("date") or "") > anchor_date]
        return float(start), bets
    return float(STARTING_BANKROLL), _db.get_bets()


def real_sizing_bankroll() -> float:
    """Settled P&L bankroll — never uses BANKROLL_OVERRIDE. Used for drawdown calculations."""
    current, bets = _anchor_start_and_bets()
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
    current, bets = _anchor_start_and_bets()
    peak = current
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
    pct = {"ML": POOL_ML, "PROPS": POOL_PROPS, "PARLAY": POOL_PARLAY,
           "RUNLINE": POOL_RUNLINE}.get(pool.upper(), 0)
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
        "pool_runline_remaining": pool_remaining("RUNLINE", br),
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


# Closing Line Value = price you got vs. the price immediately before first
# pitch. The schedule below is expressed as "minutes before commence_utc"
# checkpoints: hourly out to 2h, then finer near game time. Descending order
# matters -- capture_pre_game_clv() walks it to find every checkpoint a bet
# has crossed since its last capture.
_CLV_HOURLY_CHECKPOINTS_MIN = list(range(1440, 119, -60))   # 1440,1380,...,180,120
_CLV_FINE_CHECKPOINTS_MIN   = [60, 30, 15, 5]
CLV_CHECKPOINTS_MIN = _CLV_HOURLY_CHECKPOINTS_MIN + _CLV_FINE_CHECKPOINTS_MIN


def _clv_due_checkpoints(minutes_until: float, captured_buckets: set, ceiling) -> list:
    """Which ladder checkpoints (as bucket strings) have been crossed since
    the last capture but don't have a row yet. minutes_until <= 0 (game has
    started) returns none -- capture stops at first pitch, never grades CLV
    against an in-game price.

    ceiling caps which rungs are even eligible: a checkpoint further out
    than the bet's own log-time offset (e.g. the T-3h hourly rung for a
    bet first logged only 90 minutes before commence) never happened for
    this bet and must never fire retroactively the moment it's first seen
    -- without this cap, a bet's very first tick would find every rung
    from T-24h down to "now" simultaneously due and write dozens of
    meaningless rows in one shot."""
    if minutes_until <= 0 or ceiling is None:
        return []
    return [str(c) for c in CLV_CHECKPOINTS_MIN
            if c <= ceiling and minutes_until <= c and str(c) not in captured_buckets]


def _clv_initial_offset(existing: list, minutes_until):
    """The reference offset (minutes-before-commence at first capture) that
    caps which ladder rungs are eligible for this bet -- see
    _clv_due_checkpoints. Read back from the stored "LOG" row when one
    exists; for a bet's very first tick (no rows yet), the LOG row is
    about to be written this same call, so the current minutes_until IS
    that reference point."""
    log_row = next((c for c in existing if c.get("capture_offset_bucket") == "LOG"), None)
    if log_row is not None and log_row.get("capture_offset_min") is not None:
        return log_row["capture_offset_min"]
    if not existing:
        return minutes_until
    return None


def capture_pre_game_clv() -> int:
    """
    Fetch closing-line odds for today's pending bets and append to clv_log,
    scheduled off each bet's own commence_utc rather than a blind global
    timer. Meant to be called every ~15 min by run_pre_game_clv_loop().

    Captures the FULL trajectory, never overwriting: one row the first time
    a bet is seen (bucket "LOG", whatever offset that happens to be), then
    one row per CLV_CHECKPOINTS_MIN rung the bet crosses while still
    pending and pre-commence (hourly out to T-2h, then T-60/30/15/5m).
    Dedup is (date, bet, bet_type, capture_offset_bucket) -- both checked
    here and enforced at the DB layer (idx_clv_log_dedup) -- so a repeating
    timer can capture the same bet many times across a slate without ever
    writing a duplicate checkpoint.

    Returns the number of rows written this call. Bets with no known
    commence_utc (legacy rows, or bet types that don't map to one game --
    e.g. PARLAY) fall back to a single "LOG" capture, same behavior as
    before this rewrite.
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

    now_utc = datetime.now(pytz.utc)
    written = 0
    for b in bets:
        team     = b.get("bet") or ""
        bet_type = b.get("type") or b.get("bet_type") or "ML"
        bet_odds = str(b.get("bet_odds") or "")
        if not team or not bet_odds:
            continue

        existing = _db.get_clv_captures(today, team, bet_type)
        captured_buckets = {c.get("capture_offset_bucket") for c in existing
                             if c.get("capture_offset_bucket")}

        minutes_until = None
        commence_utc = b.get("commence_utc") or ""
        if commence_utc:
            try:
                game_dt = datetime.fromisoformat(commence_utc.replace("Z", "+00:00"))
                minutes_until = (game_dt - now_utc).total_seconds() / 60.0
            except Exception:
                minutes_until = None

        buckets_due = []
        if not existing:
            buckets_due.append("LOG")
        if minutes_until is not None and minutes_until > 0:
            ceiling = _clv_initial_offset(existing, minutes_until)
            buckets_due += _clv_due_checkpoints(minutes_until, captured_buckets, ceiling)

        if not buckets_due:
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

        offset_val = round(minutes_until) if minutes_until is not None else None
        for bucket in buckets_due:
            try:
                if _db.log_clv(
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
                    capture_offset_min=offset_val,
                    capture_offset_bucket=bucket,
                ):
                    written += 1
            except Exception:
                pass
        try:
            _db.recompute_is_closing(today, team, bet_type)
        except Exception:
            pass
    return written


def _seconds_until_next_clv_checkpoint(default_sec: int = 900, min_sec: int = 60) -> int:
    """How long the loop can safely sleep before the next checkpoint on any
    pending bet becomes due. A blind fixed 15-minute cadence can straddle a
    fine checkpoint entirely -- e.g. a tick at T-8m then T-23m-after-start
    would never land in the T-0..5m window and simply never capture "T5".
    Tightens the wait as any pending bet nears one of its remaining
    CLV_CHECKPOINTS_MIN rungs (down to min_sec, so T-5m/T-15m/T-30m/T-60m
    are hit within seconds, not skipped); falls back to default_sec (15
    min, same as before this rewrite) when nothing is due soon, so a
    freshly-logged bet still gets its first "LOG" capture promptly."""
    import pytz
    from datetime import datetime
    ET_tz = pytz.timezone("America/New_York")
    today = datetime.now(ET_tz).strftime("%Y-%m-%d")
    try:
        bets = _db.get_bets(date=today, unresolved_only=True)
    except Exception:
        return default_sec
    if not bets:
        return default_sec

    now_utc = datetime.now(pytz.utc)
    best_wait_min = None
    for b in bets:
        commence_utc = b.get("commence_utc") or ""
        if not commence_utc:
            continue
        try:
            game_dt = datetime.fromisoformat(commence_utc.replace("Z", "+00:00"))
        except Exception:
            continue
        minutes_until = (game_dt - now_utc).total_seconds() / 60.0
        if minutes_until <= 0:
            continue
        team, bet_type = b.get("bet") or "", b.get("type") or b.get("bet_type") or "ML"
        try:
            existing = _db.get_clv_captures(today, team, bet_type)
        except Exception:
            existing = []
        captured = {c.get("capture_offset_bucket") for c in existing if c.get("capture_offset_bucket")}
        ceiling = _clv_initial_offset(existing, minutes_until)
        if ceiling is None:
            continue
        not_yet_due = [c for c in CLV_CHECKPOINTS_MIN
                       if c <= ceiling and str(c) not in captured and c < minutes_until]
        if not not_yet_due:
            continue
        wait_min = minutes_until - max(not_yet_due)  # nearest checkpoint still ahead
        if best_wait_min is None or wait_min < best_wait_min:
            best_wait_min = wait_min

    if best_wait_min is None:
        return default_sec
    return max(min_sec, min(default_sec, int(round(best_wait_min * 60)) + 5))


def run_pre_game_clv_loop(stop_event=None) -> None:
    """
    Background loop: calls capture_pre_game_clv() on an adaptive cadence
    (see _seconds_until_next_clv_checkpoint) so each pending pick gets its
    full pre-game line trajectory snapshotted, with a real closing line
    captured within seconds of the T-5m checkpoint rather than whatever a
    blind 15-minute timer happened to see first (TIER 3 WIRE-IN 4 —
    AUDIT.md M6: capture_pre_game_clv() was fully built but had zero
    callers anywhere outside test_fixes.py; later found to be capturing
    its FIRST tick as a permanent "closing" line, often hours early — see
    CLV_CHECKPOINTS_MIN rewrite). Meant to run as a daemon thread started
    by brain.py in --bot mode, the only actually-deployed persistent
    process — scheduler.py's schedule_loop() is not part of any deployed
    process (AUDIT.md M5) and would never fire this.

    capture_pre_game_clv() dedups per (bet, checkpoint), so a repeating
    timer is safe and won't spam clv_log with duplicate rows for a
    checkpoint already captured.

    This SQL clv_log table is now the only CLV pipeline api.py's /api/clv
    endpoints read from (consolidated -- clv_tracker.py, the only thing
    that ever wrote clv_log.json, was dead code with zero callers and has
    been deleted; nothing reads that file anymore either). Still separate
    and untouched: telegram_handler.py's auto-settler also independently
    sets bets.closing_odds/clv_pct at settlement time via its own
    post-game odds fetch -- a different, still-live pipeline, tagged
    'v1-unreliable' everywhere it's read (see math_engine.clv_stats_summary)
    since it was never made game-time aware either.
    """
    import threading
    _stop = stop_event or threading.Event()
    while not _stop.is_set():
        try:
            n = capture_pre_game_clv()
            if n:
                print(f"[CLV] Pre-game capture: {n} row(s) written")
        except Exception as e:
            print(f"[CLV] Pre-game capture loop error: {e}")
        try:
            wait_sec = _seconds_until_next_clv_checkpoint()
        except Exception:
            wait_sec = 900
        _stop.wait(wait_sec)


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
    print(f"  RUNLINE pool:   ${pool_budget('RUNLINE', br):.2f} / remaining ${pool_remaining('RUNLINE', br):.2f}")
    stake = kelly_stake(0.55, "+130", "HIGH")
    print(f"Kelly stake (55% prob, +130, HIGH): ${stake:.2f}")
    gt = growth_tracker()
    print(f"Growth: all-time {gt['all_time_pct']:+.1f}%  week {gt['week_pct']:+.1f}%  month {gt['month_pct']:+.1f}%")
    print(f"Monthly pace: ${gt['monthly_pace']:+.2f}")
