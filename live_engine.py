"""PARLAY OS — live_engine.py
60-second cycle 6pm-11pm ET. 6-gate conviction model for live betting.
"""

import os
import time
import json
import requests
from datetime import datetime
import pytz

from market_engine   import get_mlb_events, full_market_snapshot
from memory_engine   import record_live_bet, resolve_live_bet, recalibrate_model_prob
from bankroll_engine import kelly_stake, is_drawdown_pause, current_bankroll
from math_engine     import implied_prob, american_to_decimal

STATSAPI = "https://statsapi.mlb.com/api/v1"
ET       = pytz.timezone("America/New_York")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# Gate weights (must all pass for HIGH conviction)
GATES = {
    "poly_edge":      "Polymarket probability > sportsbook no-vig by ≥3%",
    "explainable":    "Deficit explainable: hot SP, cold lineup, fluky BABIP",
    "bullpen":        "Favored bullpen FRESH or MODERATE",
    "era_advantage":  "SP ERA advantage ≥ 0.75 vs opponent",
    "not_blowout":    "Score diff ≤ 3 runs (game live and winnable)",
    "sharp_money":    "Line moved toward our team since open",
}

CYCLE_SECS    = 60
LIVE_START_ET = 18   # 6pm ET
LIVE_END_ET   = 23   # 11pm ET
MIN_GATES     = 4    # need 4/6 gates for a live bet


def _send_telegram(msg: str):
    if not BOT_TOKEN or not CHAT_ID:
        print(msg)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=8,
        )
    except Exception:
        pass


def _in_live_window() -> bool:
    now  = datetime.now(ET)
    hour = now.hour
    return LIVE_START_ET <= hour < LIVE_END_ET


def _live_game_state(game_pk: int) -> dict:
    """Fetch live game state from Stats API."""
    try:
        r    = requests.get(f"{STATSAPI}/game/{game_pk}/linescore", timeout=8)
        data = r.json()
        inn  = data.get("currentInning", 0)
        top  = data.get("isTopInning", True)
        away = data.get("teams", {}).get("away", {})
        home = data.get("teams", {}).get("home", {})
        return {
            "inning":    inn,
            "top":       top,
            "away_runs": away.get("runs", 0),
            "home_runs": home.get("runs", 0),
            "status":    data.get("currentInningOrdinal", ""),
        }
    except Exception:
        return {}


def _current_sp_era(game_pk: int, side: str) -> float | None:
    """Current SP ERA from boxscore."""
    try:
        r   = requests.get(f"{STATSAPI}/game/{game_pk}/boxscore", timeout=8)
        box = r.json()
        t   = box.get("teams", {}).get(side, {})
        sp_ids = t.get("pitchers", [])[:1]
        if not sp_ids:
            return None
        pid  = sp_ids[0]
        info = t.get("players", {}).get(f"ID{pid}", {})
        era  = info.get("seasonStats", {}).get("pitching", {}).get("era")
        return float(era) if era else None
    except Exception:
        return None


# ── 6-GATE EVALUATION ─────────────────────────────────────────────────────────

def evaluate_gates(game_pk: int, bet_side: str, market: dict,
                   bp_our: dict, bp_opp: dict) -> dict:
    """
    Evaluate all 6 gates.
    bet_side: "away" or "home"
    market: from full_market_snapshot()
    bp_our, bp_opp: from bullpen_engine.analyze_bullpen()
    """
    state   = _live_game_state(game_pk)
    nv      = market.get("no_vig") or {}
    poly    = market.get("polymarket") or {}
    lm      = market.get("line_movement") or {}

    our_nv  = nv.get(bet_side, 0.5)
    poly_p  = poly.get(bet_side)
    opp_side = "home" if bet_side == "away" else "away"

    gate_results = {}

    # 1. Poly edge
    if poly_p and our_nv:
        poly_edge = poly_p - our_nv
        gate_results["poly_edge"] = poly_edge >= 0.03
    else:
        gate_results["poly_edge"] = False

    # 2. Explainable deficit (if our side is losing)
    our_runs  = state.get(f"{bet_side}_runs", 0)
    opp_runs  = state.get(f"{opp_side}_runs", 0)
    deficit   = opp_runs - our_runs
    # Passes if: we're not behind, or deficit ≤ 2 and we're in first 5 innings
    inn = state.get("inning", 0)
    gate_results["explainable"] = (deficit <= 0) or (deficit <= 2 and inn <= 5)

    # 3. Bullpen state
    gate_results["bullpen"] = bp_our.get("fatigue_tier") in ("FRESH", "MODERATE")

    # 4. ERA advantage
    our_era  = _current_sp_era(game_pk, bet_side)
    opp_era  = _current_sp_era(game_pk, opp_side)
    if our_era and opp_era:
        gate_results["era_advantage"] = opp_era - our_era >= 0.75
    else:
        gate_results["era_advantage"] = False

    # 5. Not a blowout
    gate_results["not_blowout"] = abs(our_runs - opp_runs) <= 3

    # 6. Sharp money (line movement toward our side)
    direction = lm.get("direction", "unknown")
    gate_results["sharp_money"] = direction in (f"steam_{bet_side}", "steam_away")

    gates_passed = sum(1 for v in gate_results.values() if v)

    conviction = "LOW"
    if gates_passed >= 6:
        conviction = "HIGH"
    elif gates_passed >= 5:
        conviction = "MEDIUM"
    elif gates_passed >= MIN_GATES:
        conviction = "LOW"
    else:
        conviction = "PASS"

    return {
        "gates":        gate_results,
        "gates_passed": gates_passed,
        "conviction":   conviction,
        "state":        state,
        "poly_p":       poly_p,
        "our_nv":       our_nv,
    }


# ── LIVE CYCLE ────────────────────────────────────────────────────────────────

def run_live_cycle(game_pk: int, away_code: str, home_code: str,
                   away_bp: dict, home_bp: dict,
                   event_id: str, away_team: str, home_team: str,
                   game_date: str) -> list[dict]:
    """
    Single cycle evaluation for one game.
    Returns list of live bet signals.
    """
    from constants import TEAM_SLUGS
    market = full_market_snapshot(
        event_id, away_team, home_team,
        away_code, home_code, game_date
    )

    signals = []
    for bet_side, our_bp, opp_bp in [
        ("away", away_bp, home_bp),
        ("home", home_bp, away_bp),
    ]:
        result = evaluate_gates(game_pk, bet_side, market, our_bp, opp_bp)
        conv   = result["conviction"]
        if conv == "PASS":
            continue

        best_odds_key = f"best_{bet_side}_odds"
        odds = market.get(best_odds_key)
        if not odds:
            continue

        model_prob = recalibrate_model_prob(result.get("our_nv") or 0.5)
        stake      = kelly_stake(model_prob, str(odds), conviction=conv)

        if stake <= 0:
            continue

        team_name = away_team if bet_side == "away" else home_team
        signals.append({
            "team":        team_name,
            "bet_side":    bet_side,
            "odds":        odds,
            "model_prob":  model_prob,
            "stake":       stake,
            "conviction":  conv,
            "gates_passed":result["gates_passed"],
            "gates":       result["gates"],
            "inning":      result["state"].get("inning", 0),
        })

    return signals


def _format_live_alert(sig: dict) -> str:
    gates_str = " | ".join(
        f"{'✓' if v else '✗'} {k.upper()}" for k, v in sig["gates"].items()
    )
    return (
        f"⚡ LIVE {sig['conviction']}\n"
        f"<b>{sig['team']}</b> ML {sig['odds']:+d}\n"
        f"Prob: {sig['model_prob']:.1%} | Stake: ${sig['stake']:.2f}\n"
        f"Inn {sig['inning']} | {sig['gates_passed']}/6 gates\n"
        f"{gates_str}"
    )


# ── MAIN LOOP ─────────────────────────────────────────────────────────────────

def run_live_monitor():
    """Run continuous 60-second live monitoring loop."""
    from bullpen_engine import analyze_bullpen, bullpen_run_factor
    from constants import MLB_TEAM_IDS

    _send_telegram("🟢 Live monitor started")

    while True:
        if not _in_live_window():
            time.sleep(CYCLE_SECS)
            continue

        if is_drawdown_pause():
            _send_telegram("⚠️ DRAWDOWN PAUSE active — no live bets")
            time.sleep(300)
            continue

        events = get_mlb_events()
        today  = datetime.now(ET).strftime("%Y-%m-%d")

        for e in events:
            game_pk   = None
            away_team = e["away"]
            home_team = e["home"]

            # Resolve game_pk from schedule
            try:
                sched = requests.get(
                    f"{STATSAPI}/schedule?sportId=1&date={today}&hydrate=game",
                    timeout=8
                ).json()
                for gd in sched.get("dates", []):
                    for g in gd.get("games", []):
                        at = g.get("teams", {}).get("away", {}).get("team", {}).get("name", "")
                        ht = g.get("teams", {}).get("home", {}).get("team", {}).get("name", "")
                        if away_team in at or at in away_team:
                            game_pk = g["gamePk"]
                            break
                    if game_pk:
                        break
            except Exception:
                pass

            if not game_pk:
                continue

            # Resolve team codes
            from constants import MLB_TEAM_MAP
            away_code = MLB_TEAM_MAP.get(away_team, away_team[:3].upper())
            home_code = MLB_TEAM_MAP.get(home_team, home_team[:3].upper())

            away_tid = MLB_TEAM_IDS.get(away_code)
            home_tid = MLB_TEAM_IDS.get(home_code)
            if not away_tid or not home_tid:
                continue

            away_bp = analyze_bullpen(away_tid, today, label=away_code)
            home_bp = analyze_bullpen(home_tid, today, label=home_code)

            signals = run_live_cycle(
                game_pk, away_code, home_code,
                away_bp, home_bp,
                e["id"], away_team, home_team, today
            )

            for sig in signals:
                msg = _format_live_alert(sig)
                _send_telegram(msg)
                record_live_bet(
                    str(game_pk), sig["team"],
                    datetime.now(ET).isoformat(),
                    str(sig["odds"]), sig["gates_passed"]
                )

        time.sleep(CYCLE_SECS)


if __name__ == "__main__":
    run_live_monitor()
