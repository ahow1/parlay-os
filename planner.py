"""PARLAY OS — PLANNER + SESSION DEBRIEF
Morning mode (9am ET): reads bet history, calculates ROI by factor,
identifies what's working vs noise, sends improvement report to Telegram,
auto-adjusts model weights after 50+ bets.

Debrief mode (11pm ET): pulls final scores for today's games, marks bets
WIN/LOSS, calculates day P&L, sends full debrief to Telegram.

Usage:
  python planner.py              # morning planning report
  python planner.py --debrief    # nightly debrief
"""
import os, sys, json, requests, time
from datetime import datetime, timedelta
import pytz

import db as _db
from math_engine import american_to_decimal, clv_stats_summary, STARTING_BANKROLL

ET    = pytz.timezone("America/New_York")
NOW   = datetime.now(ET)
DATE  = NOW.strftime("%Y-%m-%d")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
WEIGHT_FILE        = "model_weights.json"
MIN_SAMPLE         = 10    # minimum bets to report a factor
AUTO_ADJUST_AT     = 50    # adjust weights after this many resolved bets


# ── TELEGRAM ──────────────────────────────────────────────────────────────────

def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN:
        print(text); return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        try:
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk,
                                     "disable_web_page_preview": True}, timeout=10)
            time.sleep(0.3)
        except Exception as e:
            print(f"Telegram err: {e}")


# ── WEIGHTS ───────────────────────────────────────────────────────────────────

def load_weights():
    try:
        with open(WEIGHT_FILE) as f:
            return json.load(f)
    except Exception:
        return {"sp_xfip": 1.0, "bp_era": 1.0, "wrc_plus": 1.0,
                "park_factor": 1.0, "umpire": 1.0, "weather": 1.0,
                "platoon": 1.0, "fatigue": 1.0}


def save_weights(weights):
    with open(WEIGHT_FILE, "w") as f:
        json.dump(weights, f, indent=2)


# ── BANKROLL CALC ─────────────────────────────────────────────────────────────

def calc_bankroll(bets):
    current = STARTING_BANKROLL
    peak    = STARTING_BANKROLL
    total_wagered = 0.0
    for b in bets:
        result = b.get("result")
        stake  = float(b.get("stake") or 0)
        total_wagered += stake
        if result == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                current += (dec - 1) * stake
        elif result == "L":
            current -= stake
        peak = max(peak, current)
    return round(current, 2), round(peak, 2), round(total_wagered, 2)


# ── FACTOR ANALYSIS ───────────────────────────────────────────────────────────

def factor_roi(bets, field):
    """Group resolved bets by `field`, return ROI/WR table sorted by ROI desc."""
    resolved = [b for b in bets if b.get("result") in ("W", "L", "P")]
    groups = {}
    for b in resolved:
        key = str(b.get(field) or "?").strip() or "?"
        g = groups.setdefault(key, {"w": 0, "l": 0, "stake": 0.0, "pnl": 0.0})
        stake  = float(b.get("stake") or 0)
        result = b.get("result")
        g["stake"] += stake
        if result == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                g["pnl"] += (dec - 1) * stake
            g["w"] += 1
        elif result == "L":
            g["pnl"] -= stake
            g["l"] += 1

    rows = []
    for key, g in groups.items():
        total = g["w"] + g["l"]
        if total < MIN_SAMPLE:
            continue
        rows.append({
            "group":    key,
            "total":    total,
            "wins":     g["w"],
            "losses":   g["l"],
            "win_rate": round(g["w"] / total * 100, 1) if total else 0,
            "roi":      round(g["pnl"] / g["stake"] * 100, 2) if g["stake"] else 0,
            "pnl":      round(g["pnl"], 2),
        })
    rows.sort(key=lambda x: x["roi"], reverse=True)
    return rows


def auto_adjust_weights(bets, weights):
    resolved = [b for b in bets if b.get("result") in ("W", "L")]
    if len(resolved) < AUTO_ADJUST_AT:
        return weights, []

    adjustments = []
    new_w = dict(weights)

    by_conv = {r["group"]: r for r in factor_roi(bets, "conviction")}

    # HIGH underperforming → reduce sp_xfip weight (we may be overweighting SP)
    h = by_conv.get("HIGH")
    if h and h["roi"] < -5 and h["total"] >= 20:
        new_w["sp_xfip"] = round(max(weights["sp_xfip"] * 0.95, 0.7), 3)
        adjustments.append(f"HIGH ROI {h['roi']:.1f}% → sp_xfip {new_w['sp_xfip']:.2f}")

    # MEDIUM outperforming → boost wrc_plus weight
    m = by_conv.get("MEDIUM")
    if m and m["roi"] > 10 and m["total"] >= 15:
        new_w["wrc_plus"] = round(min(weights["wrc_plus"] * 1.05, 1.3), 3)
        adjustments.append(f"MEDIUM ROI {m['roi']:.1f}% → wrc_plus {new_w['wrc_plus']:.2f}")

    return new_w, adjustments


# ── MLB FINAL SCORES (for debrief) ────────────────────────────────────────────

def fetch_final_scores(date_str):
    """Returns {game_key: {home_score, away_score, state}} for completed games."""
    try:
        r = requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "date": date_str, "gameType": "R",
                    "hydrate": "linescore,team"},
            timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"MLB schedule err: {e}")
        return {}

    TEAM_MAP = {
        "Arizona Diamondbacks": "AZ", "Atlanta Braves": "ATL",
        "Baltimore Orioles": "BAL", "Boston Red Sox": "BOS",
        "Chicago Cubs": "CHC", "Chicago White Sox": "CWS",
        "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE",
        "Colorado Rockies": "COL", "Detroit Tigers": "DET",
        "Houston Astros": "HOU", "Kansas City Royals": "KC",
        "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD",
        "Miami Marlins": "MIA", "Milwaukee Brewers": "MIL",
        "Minnesota Twins": "MIN", "New York Mets": "NYM",
        "New York Yankees": "NYY", "Oakland Athletics": "OAK",
        "Philadelphia Phillies": "PHI", "Pittsburgh Pirates": "PIT",
        "San Diego Padres": "SD", "San Francisco Giants": "SF",
        "Seattle Mariners": "SEA", "St. Louis Cardinals": "STL",
        "Tampa Bay Rays": "TB", "Texas Rangers": "TEX",
        "Toronto Blue Jays": "TOR", "Washington Nationals": "WAS",
        "Athletics": "ATH",
    }
    results = {}
    for de in data.get("dates", []):
        for game in de.get("games", []):
            state = game.get("status", {}).get("abstractGameState", "")
            ht = game["teams"]["home"]
            at = game["teams"]["away"]
            ha = TEAM_MAP.get(ht["team"]["name"], ht["team"]["name"][:3].upper())
            aa = TEAM_MAP.get(at["team"]["name"], at["team"]["name"][:3].upper())
            ls = game.get("linescore", {})
            hs = ls.get("teams", {}).get("home", {}).get("runs")
            as_ = ls.get("teams", {}).get("away", {}).get("runs")
            results[f"{aa}@{ha}"] = {
                "home_abr": ha, "away_abr": aa,
                "home_score": hs, "away_score": as_,
                "state": state,
                "final": state == "Final",
            }
    return results


def determine_result(bet_str, game_key, scores):
    """Return 'W', 'L', 'P', or None (not yet final)."""
    info = scores.get(game_key)
    if not info or not info["final"]:
        return None
    hs = info.get("home_score")
    as_ = info.get("away_score")
    if hs is None or as_ is None:
        return None

    ha  = info["home_abr"]
    aa  = info["away_abr"]
    bet = bet_str.upper()

    if ha in bet or info.get("home_name", "") in bet:
        if hs > as_:  return "W"
        if hs < as_:  return "L"
        return "P"
    if aa in bet or info.get("away_name", "") in bet:
        if as_ > hs:  return "W"
        if as_ < hs:  return "L"
        return "P"
    return None


# ── MORNING PLAN ──────────────────────────────────────────────────────────────

def run_morning_plan():
    print(f"[PLANNER] Morning report — {NOW.strftime('%I:%M %p ET')}")
    bets     = _db.get_bets()
    resolved = [b for b in bets if b.get("result") in ("W", "L", "P")]

    clv_log   = []
    try:
        with open("clv_log.json") as f:
            clv_log = json.load(f)
    except Exception:
        pass
    clv_stats = clv_stats_summary(clv_log)

    current, peak, total_wagered = calc_bankroll(bets)
    pnl    = current - STARTING_BANKROLL
    roi    = (pnl / total_wagered * 100) if total_wagered > 0 else 0.0
    wins   = sum(1 for b in resolved if b["result"] == "W")
    losses = sum(1 for b in resolved if b["result"] == "L")
    wr     = (wins / len(resolved) * 100) if resolved else 0

    lines = [
        f"PARLAY OS — MORNING PLANNER — {DATE}",
        f"{'─'*42}",
        f"Bankroll: ${current:.2f}  (start ${STARTING_BANKROLL:.0f}  "
        f"{'+' if pnl>=0 else ''}${pnl:.2f}  {'+' if roi>=0 else ''}{roi:.1f}% ROI)",
        f"Record:   {wins}W-{losses}L ({wr:.1f}% WR)  |  {len(resolved)}/{len(bets)} settled",
        f"Avg CLV:  {clv_stats.get('avg_clv',0):+.2f}%  {clv_stats.get('verdict','—')}",
        "",
    ]

    # Factor ROI
    by_type = _db.get_roi_by_type()
    if by_type:
        lines.append("BY BET TYPE:")
        for r in by_type:
            total = r["total"] or 1
            wr_t  = round((r["wins"] or 0) / total * 100, 1)
            clv_t = (r["avg_clv"] or 0)
            lines.append(f"  {r['type']:<10} {r['total']:>3} bets  "
                         f"WR:{wr_t:.0f}%  CLV:{clv_t:+.1f}%")

    by_sp = _db.get_roi_by_sp()
    if by_sp:
        lines.append("\nTOP SPs (by volume):")
        for r in by_sp[:6]:
            total = r["total"] or 1
            wr_s  = round((r["wins"] or 0) / total * 100, 1)
            lines.append(f"  {(r['sp'] or '?')[:22]:<24} {r['total']:>2}  "
                         f"WR:{wr_s:.0f}%  CLV:{(r['avg_clv'] or 0):+.1f}%")

    by_park = _db.get_roi_by_park()
    if by_park:
        lines.append("\nPARK PERFORMANCE:")
        for r in by_park[:5]:
            total = r["total"] or 1
            wr_p  = round((r["wins"] or 0) / total * 100, 1)
            lines.append(f"  {(r['park'] or '?'):<6} {r['total']:>2}  WR:{wr_p:.0f}%  "
                         f"CLV:{(r['avg_clv'] or 0):+.1f}%")

    # Signal vs noise
    sample_n = len(resolved)
    lines.append("")
    if sample_n < 30:
        lines.append(f"SAMPLE: {sample_n} — all patterns are noise until 30+ bets")
    elif sample_n < 100:
        lines.append(f"SAMPLE: {sample_n} — directional signals, not definitive")
    else:
        lines.append(f"SAMPLE: {sample_n} — statistically meaningful patterns")

    # What's working
    by_conv = {r["group"]: r for r in factor_roi(bets, "conviction")}
    working, struggling = [], []
    for conv, r in by_conv.items():
        if r["roi"] > 5:  working.append(f"{conv} bets ({r['roi']:+.1f}% ROI)")
        elif r["roi"] < -5: struggling.append(f"{conv} bets ({r['roi']:+.1f}% ROI)")
    if working:    lines.append(f"WORKING:    {', '.join(working)}")
    if struggling: lines.append(f"STRUGGLING: {', '.join(struggling)}")

    # Auto-adjust
    weights = load_weights()
    new_w, adjustments = auto_adjust_weights(bets, weights)
    if adjustments:
        save_weights(new_w)
        lines.append(f"\nWEIGHT ADJUSTMENTS ({sample_n} bets):")
        for adj in adjustments:
            lines.append(f"  {adj}")
    elif sample_n >= AUTO_ADJUST_AT:
        lines.append(f"\nWeights stable at {sample_n} bets")

    lines.append(f"\nParlay OS Planner — {NOW.strftime('%I:%M %p ET')}")
    send_telegram("\n".join(lines))
    print("Morning plan sent.")


# ── NIGHTLY DEBRIEF ───────────────────────────────────────────────────────────

def run_debrief():
    print(f"[DEBRIEF] Nightly debrief — {NOW.strftime('%I:%M %p ET')}")
    scores = fetch_final_scores(DATE)
    print(f"  Final scores: {sum(1 for g in scores.values() if g['final'])} games done")

    # Resolve pending bets
    pending = _db.get_bets(date=DATE, unresolved_only=True)
    resolved_now = []
    for b in pending:
        game = b.get("game", "")
        result = determine_result(b.get("bet", ""), game, scores)
        if result:
            _db.resolve_bet(
                bet=b["bet"], date=DATE,
                closing_odds=b.get("bet_odds", ""),
                result=result,
                game_score=str(scores.get(game, {}).get("home_score", "")) + "-" +
                           str(scores.get(game, {}).get("away_score", "")),
            )
            resolved_now.append((b, result))
            print(f"  {b['bet']} → {result}")

    # Day P&L
    all_today = _db.get_bets(date=DATE)
    day_pnl, day_stake = 0.0, 0.0
    for b in all_today:
        stake  = float(b.get("stake") or 0)
        result = b.get("result")
        day_stake += stake
        if result == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                day_pnl += (dec - 1) * stake
        elif result == "L":
            day_pnl -= stake

    bets = _db.get_bets()
    current, peak, total_wagered = calc_bankroll(bets)
    season_pnl = current - STARTING_BANKROLL

    wins_today   = sum(1 for b in all_today if b.get("result") == "W")
    losses_today = sum(1 for b in all_today if b.get("result") == "L")
    pend_today   = sum(1 for b in all_today if not b.get("result"))

    lines = [
        f"PARLAY OS — SESSION DEBRIEF — {DATE}",
        f"{'─'*42}",
        f"Day P&L: {'+' if day_pnl>=0 else ''}${day_pnl:.2f}  "
        f"(${day_stake:.2f} wagered)",
        f"Today:   {wins_today}W-{losses_today}L  ({pend_today} pending)",
        f"Bankroll: ${current:.2f}  (season {'+' if season_pnl>=0 else ''}${season_pnl:.2f})",
        "",
    ]

    if resolved_now:
        lines.append("RESULTS:")
        for b, result in resolved_now:
            icon = "WIN" if result == "W" else "LOSS" if result == "L" else "PUSH"
            stake = float(b.get("stake") or 0)
            if result == "W":
                dec = american_to_decimal(str(b.get("bet_odds", "")))
                pnl = (dec - 1) * stake if dec else 0
                lines.append(f"  {icon:4}  {b['bet']}  {b.get('bet_odds','')}  +${pnl:.2f}")
            elif result == "L":
                lines.append(f"  {icon:4}  {b['bet']}  {b.get('bet_odds','')}  -${stake:.2f}")
            else:
                lines.append(f"  {icon:4}  {b['bet']}  {b.get('bet_odds','')}")

    if pend_today:
        lines.append(f"\n{pend_today} bets still PENDING (games not final)")

    lines.append(f"\nParlay OS Debrief — {NOW.strftime('%I:%M %p ET')}")
    send_telegram("\n".join(lines))
    print("Debrief sent.")


# ── WEEKLY ROI REPORT ─────────────────────────────────────────────────────────

def run_weekly_report():
    print(f"[WEEKLY] Weekly ROI report — {NOW.strftime('%I:%M %p ET')}")
    bets     = _db.get_bets()
    # Last 7 days
    cutoff   = (NOW - timedelta(days=7)).strftime("%Y-%m-%d")
    wk_bets  = [b for b in bets if (b.get("date") or "") >= cutoff]
    resolved = [b for b in wk_bets if b.get("result") in ("W", "L", "P")]

    week_pnl, week_stake = 0.0, 0.0
    best_bet  = None
    worst_bet = None

    for b in resolved:
        stake  = float(b.get("stake") or 0)
        result = b.get("result")
        week_stake += stake
        pnl = 0.0
        if result == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                pnl = (dec - 1) * stake
        elif result == "L":
            pnl = -stake
        week_pnl += pnl
        if best_bet is None or pnl > best_bet[1]:
            best_bet = (b, pnl)
        if worst_bet is None or pnl < worst_bet[1]:
            worst_bet = (b, pnl)

    wins   = sum(1 for b in resolved if b["result"] == "W")
    losses = sum(1 for b in resolved if b["result"] == "L")
    wr     = (wins / len(resolved) * 100) if resolved else 0
    week_roi = (week_pnl / week_stake * 100) if week_stake > 0 else 0

    clv_log = []
    try:
        with open("clv_log.json") as f:
            clv_log = json.load(f)
        wk_cutoff_dt = NOW - timedelta(days=7)
        clv_log = [c for c in clv_log if c.get("date", "") >= cutoff]
    except Exception:
        pass
    clv_stats = clv_stats_summary(clv_log)

    current, peak, total_wagered = calc_bankroll(bets)

    lines = [
        f"PARLAY OS — WEEKLY ROI REPORT",
        f"Week ending {DATE}",
        f"{'─'*42}",
        f"Week P&L:   {'+' if week_pnl>=0 else ''}${week_pnl:.2f}  "
        f"({'+' if week_roi>=0 else ''}{week_roi:.1f}% ROI)",
        f"Record:     {wins}W-{losses}L ({wr:.1f}% WR)",
        f"Bets:       {len(resolved)} settled this week",
        f"Avg CLV:    {clv_stats.get('avg_clv',0):+.2f}%",
        f"Bankroll:   ${current:.2f}",
        "",
    ]

    if best_bet:
        b, pnl = best_bet
        lines.append(f"BEST BET:   {b['bet']} ({b.get('bet_odds','')})  +${pnl:.2f}")
    if worst_bet:
        b, pnl = worst_bet
        lines.append(f"WORST BET:  {b['bet']} ({b.get('bet_odds','')})  ${pnl:.2f}")

    by_type = _db.get_roi_by_type()
    if by_type:
        lines.append("\nMODEL ACCURACY BY TYPE:")
        for r in by_type:
            total = r["total"] or 1
            wr_t  = round((r["wins"] or 0) / total * 100, 1)
            lines.append(f"  {r['type']:<10} WR:{wr_t:.0f}%  CLV:{(r['avg_clv'] or 0):+.1f}%  ({r['total']} bets)")

    lines.append(f"\nParlay OS Weekly — {NOW.strftime('%I:%M %p ET')}")
    send_telegram("\n".join(lines))
    print("Weekly report sent.")


# ── ENTRY POINT ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "--debrief":
        run_debrief()
    elif mode == "--weekly":
        run_weekly_report()
    else:
        run_morning_plan()
