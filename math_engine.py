"""
PARLAY OS — MATH ENGINE
All betting math. Runs automatically on every bet.
No manual calculations ever.
"""

import json, os
from datetime import datetime
from typing import Optional
import pytz

ET = pytz.timezone("America/New_York")

STARTING_BANKROLL   = 150.0
DAILY_CAP_PCT       = 0.15    # max 15% of bankroll per day
DRAWDOWN_REDUCE_PCT = 0.20    # cut stakes 50% at 20% peak-to-trough drawdown
DRAWDOWN_STOP_PCT   = 0.40    # stop + Telegram alert at 40% drawdown
KELLY_FRACTION      = 0.25    # quarter Kelly
MAX_BET_PCT         = 0.05    # hard cap: 5% of bankroll per bet


def american_to_decimal(ml: str) -> Optional[float]:
    try:
        n = float(str(ml).replace("+", "").strip())
        if n == 0: return None
        return round((n / 100) + 1 if n > 0 else (100 / abs(n)) + 1, 4)
    except: return None


def decimal_to_american(dec: float) -> str:
    if dec is None or dec <= 1: return "—"
    if dec >= 2.0: return f"+{round((dec - 1) * 100)}"
    return f"{round(-100 / (dec - 1))}"


def implied_prob(ml: str) -> Optional[float]:
    """Raw implied probability from American odds. Returns as percentage."""
    try:
        n = float(str(ml).replace("+", "").strip())
        if n > 0: return round(100 / (n + 100) * 100, 2)
        else:     return round(abs(n) / (abs(n) + 100) * 100, 2)
    except: return None


def no_vig_prob(ml_side1: str, ml_side2: str) -> dict:
    """Remove bookmaker vig. Returns true probabilities for both sides."""
    p1 = implied_prob(ml_side1)
    p2 = implied_prob(ml_side2)
    if p1 is None or p2 is None:
        return {"side1_true": None, "side2_true": None, "vig_pct": None}
    total = p1 + p2
    vig   = round(total - 100, 2)
    true1 = round(p1 / total * 100, 2)
    true2 = round(p2 / total * 100, 2)
    return {
        "side1_raw":   p1,
        "side2_raw":   p2,
        "side1_true":  true1,
        "side2_true":  true2,
        "vig_pct":     vig,
        "side1_novig": decimal_to_american(100 / true1) if true1 else "—",
        "side2_novig": decimal_to_american(100 / true2) if true2 else "—",
    }


def expected_value(ml: str, true_prob_pct: float, stake: float = 100) -> dict:
    """
    EV = (true_prob x profit) - ((1 - true_prob) x stake)
    Positive = we have edge. Negative = book has edge.
    """
    dec  = american_to_decimal(ml)
    p    = true_prob_pct / 100
    if dec is None or p <= 0 or p >= 1:
        return {"ev_dollars": None, "ev_pct": None, "edge_pct": None, "verdict": "—"}
    profit = (dec - 1) * stake
    ev     = (p * profit) - ((1 - p) * stake)
    impl   = implied_prob(ml) / 100
    edge   = round((p - impl) * 100, 2)
    return {
        "ev_dollars":   round(ev, 2),
        "ev_pct":       round(ev / stake * 100, 2),
        "edge_pct":     edge,
        "implied_prob": round(impl * 100, 2),
        "true_prob":    true_prob_pct,
        "verdict": (
            "STRONG +EV" if edge > 4 else
            "+EV"         if edge > 1 else
            "MARGINAL"    if edge > -1 else
            "-EV"
        )
    }


def calc_clv(bet_odds: str, closing_odds: str) -> dict:
    """
    Closing Line Value — the only honest long-term edge metric.
    Positive CLV = we got better odds than where market closed = real edge.
    Target: +2% average over 100+ bets.
    """
    bp = implied_prob(bet_odds)
    cp = implied_prob(closing_odds)
    if bp is None or cp is None or cp == 0:
        return {"clv_pct": None, "verdict": "—", "beat_market": None}
    clv_prob = round((bp - cp) / cp * 100, 2)
    return {
        "clv_pct":      clv_prob,
        "bet_odds":     bet_odds,
        "closing_odds": closing_odds,
        "beat_market":  clv_prob > 0,
        "verdict": (
            "STRONG CLV" if clv_prob > 4  else
            "+CLV"        if clv_prob > 1  else
            "NEUTRAL"     if clv_prob > -1 else
            "-CLV"
        )
    }


def parlay_odds(legs: list) -> dict:
    """Combined parlay odds from list of American odds strings."""
    valid = [(ml, american_to_decimal(ml)) for ml in legs if american_to_decimal(ml)]
    if len(valid) < 2:
        return {"valid": False}
    combined = 1.0
    for _, dec in valid: combined *= dec
    return {
        "valid":        True,
        "legs":         len(valid),
        "decimal":      round(combined, 4),
        "american":     decimal_to_american(combined),
        "payout_25":    round((combined - 1) * 25, 2),
        "payout_15":    round((combined - 1) * 15, 2),
        "implied_prob": round((1 / combined) * 100, 2),
    }


def hedge_calc(original_odds: str, original_stake: float, hedge_odds: str) -> dict:
    """
    Exact hedge stake to guarantee profit.
    Only hedge when locked_profit > EV of riding.
    """
    orig_dec  = american_to_decimal(original_odds)
    hedge_dec = american_to_decimal(hedge_odds)
    if not orig_dec or not hedge_dec or original_stake <= 0:
        return {"valid": False}
    to_return     = orig_dec * original_stake
    hedge_stake   = to_return / hedge_dec
    locked_profit = to_return - original_stake - hedge_stake
    impl_hedge    = implied_prob(hedge_odds) / 100
    ride_prob     = 1 - impl_hedge
    ride_payout   = to_return - original_stake
    ride_ev       = round(ride_prob * ride_payout - (1 - ride_prob) * original_stake, 2)
    return {
        "valid":          True,
        "to_return":      round(to_return, 2),
        "hedge_stake":    round(hedge_stake, 2),
        "locked_profit":  round(locked_profit, 2),
        "ride_ev":        ride_ev,
        "hedge_or_ride":  "HEDGE" if locked_profit > ride_ev else "RIDE",
        "recommendation": (
            f"HEDGE — lock ${round(locked_profit,2)} > ride EV ${ride_ev}"
            if locked_profit > ride_ev else
            f"RIDE — EV ${ride_ev} > locked ${round(locked_profit,2)}"
        )
    }


def kelly_criterion(ml: str, true_prob_pct: float, bankroll: float = 100, fraction: float = 0.25) -> dict:
    """Quarter Kelly stake sizing. Conservative and mathematically sound."""
    dec = american_to_decimal(ml)
    p   = true_prob_pct / 100
    if dec is None or p <= 0 or p >= 1:
        return {"kelly_stake": None, "verdict": "—"}
    b = dec - 1
    q = 1 - p
    full_kelly = (b * p - q) / b
    if full_kelly <= 0:
        return {"kelly_stake": 0, "verdict": "No edge — Kelly says don't bet"}
    frac_kelly = full_kelly * fraction
    stake      = round(min(bankroll * frac_kelly, bankroll * 0.40), 2)
    return {
        "kelly_stake": stake,
        "kelly_pct":   round(frac_kelly * 100, 2),
        "verdict":     f"Bet ${stake} ({round(frac_kelly*100,1)}% of bankroll)"
    }


def clv_stats_summary(log: list) -> dict:
    """Full CLV performance stats. The only honest measure of long-term edge."""
    resolved = [
        b for b in log
        if b.get("closing_odds") and b.get("bet_odds")
        and b.get("result") in ["W", "L", "P"]
    ]
    if not resolved:
        return {"total": 0, "verdict": "No data yet — need 100+ bets"}

    clv_vals, wins, losses, pushes, units = [], 0, 0, 0, 0.0
    by_type = {}

    for b in resolved:
        result = b.get("result")
        if result == "W": wins += 1
        elif result == "L": losses += 1
        else: pushes += 1

        dec = american_to_decimal(b.get("bet_odds", ""))
        if dec and result == "W":  units += (dec - 1)
        elif result == "L":        units -= 1

        clv = calc_clv(b.get("bet_odds",""), b.get("closing_odds",""))
        if clv.get("clv_pct") is not None:
            clv_vals.append(clv["clv_pct"])

        btype = b.get("type", "ML")
        if btype not in by_type:
            by_type[btype] = {"wins":0,"losses":0,"clvs":[],"units":0.0}
        if result == "W":
            by_type[btype]["wins"] += 1
            if dec: by_type[btype]["units"] += (dec - 1)
        elif result == "L":
            by_type[btype]["losses"] += 1
            by_type[btype]["units"] -= 1
        if clv.get("clv_pct") is not None:
            by_type[btype]["clvs"].append(clv["clv_pct"])

    total    = wins + losses
    avg_clv  = round(sum(clv_vals)/len(clv_vals), 2) if clv_vals else 0
    win_rate = round(wins/total*100, 1) if total else 0
    pos_clv  = sum(1 for v in clv_vals if v > 0)

    type_summary = {}
    for t, d in by_type.items():
        tot = d["wins"] + d["losses"]
        type_summary[t] = {
            "wins":     d["wins"],
            "losses":   d["losses"],
            "win_rate": round(d["wins"]/tot*100, 1) if tot else 0,
            "avg_clv":  round(sum(d["clvs"])/len(d["clvs"]),2) if d["clvs"] else 0,
            "units":    round(d["units"], 2),
            "count":    tot,
        }

    return {
        "total":       len(resolved),
        "wins":        wins,
        "losses":      losses,
        "pushes":      pushes,
        "win_rate":    win_rate,
        "avg_clv":     avg_clv,
        "pos_clv_pct": round(pos_clv/len(clv_vals)*100,1) if clv_vals else 0,
        "units_net":   round(units, 2),
        "by_type":     type_summary,
        "verdict": (
            "SHARP — proven edge"             if avg_clv > 4  else
            "POSITIVE — beating the market"   if avg_clv > 2  else
            "SLIGHT EDGE"                      if avg_clv > 0  else
            "NEUTRAL — breaking even"          if avg_clv > -1 else
            "NEGATIVE — model needs adjustment"
        ),
        "sample_size": (
            f"Only {len(resolved)} bets — need 100+ for reliable data"
            if len(resolved) < 100 else "Sufficient sample size"
        )
    }


class BankrollManager:
    """Fractional Kelly sizing with conviction tiers and drawdown protection."""

    def __init__(self, bankroll_file="bankroll.json"):
        self.file = bankroll_file
        self.data = self._load()

    def _load(self):
        try:
            with open(self.file) as f:
                return json.load(f)
        except Exception:
            return {
                "starting":     STARTING_BANKROLL,
                "current":      STARTING_BANKROLL,
                "peak":         STARTING_BANKROLL,
                "sessions":     0,
                "total_wagered": 0,
                "day_wagered":  0,
                "day_start":    "",
                "stop_betting": False,
            }

    def save(self):
        with open(self.file, "w") as f:
            json.dump(self.data, f, indent=2)

    @property
    def current(self):
        return self.data["current"]

    @property
    def peak(self):
        return self.data.get("peak", self.data["starting"])

    @property
    def drawdown_pct(self):
        if self.peak <= 0:
            return 0.0
        return (self.peak - self.current) / self.peak

    @property
    def stopped(self):
        return self.data.get("stop_betting", False)

    def stake_for_conviction(self, conviction: str, edge_pct: float,
                             ml_str: str, model_prob_pct: float) -> float:
        """
        Return recommended stake based on conviction and Kelly.
        HIGH  edge → 3–5% of bankroll
        MEDIUM edge → 1–3% of bankroll
        PASS → 0
        Hard caps: 5% per bet, 15% daily, 25% fractional Kelly.
        Down 20% from peak → stakes cut 50%.
        Down 40% from peak → stop (returns 0).
        """
        if self.stopped:
            return 0.0

        br = self.current
        dd = self.drawdown_pct

        if dd >= DRAWDOWN_STOP_PCT:
            self.data["stop_betting"] = True
            self.save()
            return 0.0

        stake_mult = 0.5 if dd >= DRAWDOWN_REDUCE_PCT else 1.0

        if conviction == "HIGH":
            base_pct, max_pct = 0.03, 0.05
        elif conviction == "MEDIUM":
            base_pct, max_pct = 0.01, 0.03
        else:
            return 0.0

        # Scale within range by edge magnitude
        edge_norm = min(max(edge_pct / 10.0, 0.0), 1.0)
        target_pct = base_pct + edge_norm * (max_pct - base_pct)

        # Kelly ceiling
        kelly_result = kelly_criterion(ml_str, model_prob_pct, br, KELLY_FRACTION)
        kelly_pct = (kelly_result.get("kelly_pct") or 0) / 100

        final_pct = min(target_pct, kelly_pct if kelly_pct > 0 else target_pct, MAX_BET_PCT)
        final_pct *= stake_mult

        # Daily cap
        today = datetime.now(ET).strftime("%Y-%m-%d")
        if self.data.get("day_start") != today:
            self.data["day_start"] = today
            self.data["day_wagered"] = 0.0

        daily_cap      = br * DAILY_CAP_PCT
        already_wagered = self.data.get("day_wagered", 0.0)
        remaining      = max(daily_cap - already_wagered, 0.0)

        stake = round(min(br * final_pct, remaining), 2)
        return max(stake, 0.0)

    def record_wager(self, stake: float):
        self.data["day_wagered"] = round(
            self.data.get("day_wagered", 0.0) + stake, 2)
        self.data["total_wagered"] = round(
            self.data.get("total_wagered", 0.0) + stake, 2)
        self.save()

    def record_result(self, pnl: float):
        self.data["current"] = round(self.data["current"] + pnl, 2)
        if self.data["current"] > self.peak:
            self.data["peak"] = self.data["current"]
        self.data["sessions"] = self.data.get("sessions", 0) + 1
        self.save()

    def send_stop_alert(self, telegram_token: str, telegram_chat: str):
        try:
            import requests
            dd = self.drawdown_pct
            msg = (f"PARLAY OS — BANKROLL ALERT\n"
                   f"Down {dd*100:.0f}% from peak (${self.peak:.2f} → ${self.current:.2f})\n"
                   f"Betting STOPPED. Review model before resuming.\n"
                   f"{datetime.now(ET).strftime('%I:%M %p ET')}")
            requests.post(
                f"https://api.telegram.org/bot{telegram_token}/sendMessage",
                json={"chat_id": telegram_chat, "text": msg},
                timeout=10)
        except Exception as e:
            print(f"Stop alert err: {e}")

    def status_str(self) -> str:
        br = self.current
        dd = self.drawdown_pct
        flag = ""
        if dd >= DRAWDOWN_STOP_PCT:
            flag = f"  STOPPED — DD {dd*100:.0f}%"
        elif dd >= DRAWDOWN_REDUCE_PCT:
            flag = f"  REDUCED stakes — DD {dd*100:.0f}%"
        return f"${br:.2f} (peak ${self.peak:.2f}){flag}"


def format_clv_stats_telegram(stats: dict) -> str:
    """Format CLV stats for Telegram message."""
    if stats.get("total", 0) == 0:
        return "CLV TRACKER — No resolved bets yet."
    lines = [
        f"CLV TRACKER — {stats['total']} bets",
        f"Avg CLV: {stats['avg_clv']:+.2f}%",
        f"Win rate: {stats['win_rate']}% ({stats['wins']}W-{stats['losses']}L)",
        f"Net units: {'+' if stats['units_net'] >= 0 else ''}{stats['units_net']}u",
        stats['verdict'],
    ]
    if stats.get("by_type"):
        lines.append("BY TYPE:")
        for btype, ts in sorted(stats["by_type"].items(), key=lambda x: -x[1].get("avg_clv",0)):
            if ts.get("count",0) < 3: continue
            lines.append(f"  {btype:<12} CLV:{ts['avg_clv']:+.1f}%  WR:{ts['win_rate']}%  ({ts['count']})")
    lines.append(stats.get("sample_size",""))
    return "\n".join(lines)
