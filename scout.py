"""
PARLAY OS — MEGA SCOUT
Runs daily at 1PM and 6PM ET via GitHub Actions.
Searches full MLB slate, scores every game, runs all math, sends to Telegram.
"""

import anthropic
import requests
import os
import json
from datetime import datetime
import pytz

from math_engine import (
    implied_prob, no_vig_prob, expected_value,
    kelly_criterion, parlay_odds as parlay_calc,
    clv_stats_summary, format_clv_stats_telegram,
)

ANTHROPIC_API_KEY  = os.environ["ANTHROPIC_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]
ET    = pytz.timezone("America/New_York")
today = datetime.now(ET).strftime("%A, %B %d, %Y")

SYSTEM_PROMPT = f"""You are Aidan's personal MLB betting analyst. Today is {today}.

CORE RULES:
- NO permanent team labels. Every team evaluated on THIS game only.
- Never RL on favorites. Ever.
- CONDITIONAL: CIN=Burns only. NYY=Fried/Schlittler/Warren. LAA=Soriano. DET=Skubal. PIT=Skenes+Cruz.
- HOU hard fade until Hader returns — pen ERA 6.31 worst in MLB.
- CIN pen trap: ERA 2.59 looks fine, xFIP 4.89 — regression confirmed Apr 29 (lost 2-13 COL).
- Record 5+ games below .500 overrides reputation. Judge what they are doing RIGHT NOW.
- Hot streaks are noise. Metrics only. No recency bias.
- Every game is its own independent bet.

HARD LESSONS:
APR 29: CIN lost 2-13 COL — pen regression hit. Never trust CIN late leads.
APR 29: WAS beat NYM 14-2 — injured NYM is completely different team. Always check NYM IL.
APR 29: AZ dangerous with Carroll+Moreno. AZ viable with full lineup.
APR 29: LAD lost to MIA — MIA dangerous at home. Never sleep on them.
APR 30: TOR locked at 74 vs MIN at home — MIN home edge real even as bad team.
         Road lock at struggling home team = 78+ required, not 70.
APR 30: PHI played at 10-19 because of reputation — record must override label.
COL road games more dangerous than home — pen travels well.

2026 CONTEXT:
ABS robot umpires live — command/diverse arsenal SPs gain value over pure fastball arms.
Fastball-heavy SPs lose ~47 wOBA pts. TTOP: ERA rises 4.08 to 4.57 third time through order.

ELITE SPs:
Soriano LAA 0.24 ERA | Schlittler NYY 0.86 FIP | Skenes PIT 1.27 ERA | Sanchez PHI 1.59 ERA
Skubal DET 2.08 ERA | Sale ATL 2.21 ERA | Fried NYY 2.40 ERA | Burns CIN 2.42 ERA
Warren NYY 2.49 ERA | Gausman TOR 2.57 ERA

BULLPENS ELITE: TOR (lowest xFIP), MIL, CLE, LAD, SD
BULLPENS AVOID: WAS (30th, 10 blown saves), HOU (worst ERA, Hader+Brown IL), CWS (9 blown saves), CIN (xFIP trap)
PHI Luzardo elite AT HOME specifically. BOS coaching staff fired — instability.

HOT HITTERS:
Guerrero TOR .358 leads MLB | Turang MIL 180 wRC+ confirmed | Rice NYY .500 OBP leads MLB
Pages LAD .397 AVG leads hits+RBI | Baldwin ATL ROY candidate
Carroll AZ healthy .298/.379/.579 | Ohtani LAD 48-game OBP streak

INJURIES:
HOU: Hader IL + Brown IL — hard fade until Hader returns
CHC: Horton out 15-16 months Tommy John — rotation hole all season
NYM: Always check IL — shorthanded NYM collapses
MIA: Dangerous at home — took series from LAD
ATL: Iglesias returning ~May 5 — check status

SESSION FORMAT (tiered — no more 5-leg parlays):
SHARP PLAY (78+ score): $30-40 straight ML or F5
VALUE PLAY (F5/total clear edge): $20-25
PROP PLAY (K prop or NRFI right spot): $15-20
PARLAY (2-3 legs MAX locks only): $15-20
Total: ~$100

SCORING MODEL — YOU score every game:
SP xFIP/SIERA 25% | Bullpen+Fatigue 20% | Offense wRC+ 18% | Run Diff 12%
Platoon Splits 8% | Injury/Lineup 7% | Home/Road 5% | Park+Weather 3% | Line Movement 2%
78+ = LOCK road at strong home | 70+ = LOCK standard | 50-69 = COIN FLIP | 35-49 = LEAN | <35 = PASS

BET TYPES — analyze ALL for every game:
ML: full game winner
F5: first 5 innings — USE when elite SP + shaky pen behind them
NRFI: no run first inning — USE when both SPs elite (combined ERA <4.5)
K PROP: SP strikeout over/under — softest market in MLB — USE when elite K SP vs bottom-10 K% lineup
TEAM TOTAL: one team runs over/under — USE when team faces ERA <2.5 SP
GAME TOTAL: full game over/under — USE when weather is strong factor
RL: run line — ONLY +1.5 on underdog, never RL favorites

CLV: for every pick note EARLY or WAIT — early when line likely moves against us before first pitch.

TASK:
1. Search ALL MLB games today not yet started
2. For each: confirmed SPs + ERA/FIP/K9, current ML + F5 odds, records, last 10 form, bullpen usage, injuries, weather
3. Score every game yourself
4. Analyze ALL bet types
5. Build full mega board with exact sizing
6. Be brutally honest — if only 1 lock exists tonight say so
7. Return ONLY pure JSON — zero markdown

JSON FORMAT:
{{
  "date": "{today}",
  "slate_size": 0,
  "session_verdict": "FULL / REDUCED / PASS",
  "session_note": "honest 1-2 sentence slate assessment",
  "games": [
    {{
      "away": "NYY",
      "home": "ATL",
      "time": "7:05 PM ET",
      "away_record": "22-9",
      "home_record": "20-11",
      "away_last10": "8-2",
      "home_last10": "7-3",
      "asp": "Fried",
      "asp_era": "2.40",
      "asp_fip": "2.31",
      "asp_k9": "9.8",
      "hsp": "Sale",
      "hsp_era": "2.21",
      "hsp_fip": "2.10",
      "hsp_k9": "10.1",
      "aml": "-130",
      "hml": "+110",
      "f5_away": "-120",
      "f5_home": "+100",
      "game_total": "8.0",
      "game_total_over": "-110",
      "game_total_under": "-110",
      "weather": "Wind 8mph in from CF, 71F",
      "score": 74,
      "ml_tag": "cf",
      "ml_pick": "NYY",
      "ml_pick_odds": "-130",
      "f5_tag": "lock",
      "f5_pick": "NYY F5",
      "f5_pick_odds": "-120",
      "f5_reasoning": "Fried elite through 5, NYY pen fatigue",
      "nrfi_play": true,
      "nrfi_odds": "-138",
      "nrfi_reasoning": "Both SPs elite, combined ERA 4.61",
      "team_total_play": {{"active": false, "team": "", "line": "", "odds": "", "reasoning": ""}},
      "k_prop_play": {{"active": true, "pitcher": "Fried", "line": "over 7.5 Ks", "odds": "-112", "reasoning": "ATL 28th K% vs LHP, Fried 9.8 K/9"}},
      "total_play": {{"active": true, "direction": "under", "line": "8.0", "odds": "-112", "reasoning": "Two elite SPs, wind in"}},
      "rl_play": {{"active": false, "team": "", "line": "+1.5", "odds": "", "reasoning": ""}},
      "analysis": "2-3 sharp sentences. Name pitchers and specific stats.",
      "factors": ["Fried 2.40 ERA vs ATL 28th K% vs LHP", "NYY pen rested", "NYY 3rd wRC+ vs RHP"],
      "risks": ["Sale equally elite", "ATL home field"],
      "hedge_alert": "null",
      "clv_timing": "EARLY",
      "clv_note": "NYY ML likely moves -130 to -145 by first pitch"
    }}
  ],
  "mega_board": {{
    "sharp_plays": [{{"bet": "NYY F5", "odds": "-120", "size": "$30", "type": "F5", "score": 81, "note": "Top play tonight"}}],
    "f5_plays": [],
    "prop_plays": [{{"bet": "Fried OVER 7.5 Ks", "odds": "-112", "size": "$20", "type": "K_PROP", "note": "ATL 28th K% vs LHP"}}],
    "nrfi_plays": [{{"bet": "NRFI: NYY @ ATL", "odds": "-138", "size": "$15", "type": "NRFI", "note": "Both elite SPs"}}],
    "total_plays": [{{"bet": "NYY/ATL UNDER 8.0", "odds": "-112", "size": "$15", "type": "UNDER", "note": "Wind in, two elite SPs"}}],
    "parlay": {{
      "legs": ["NYY F5 -120", "NYY/ATL UNDER 8.0 -112"],
      "combined_odds": "+165",
      "size": "$15",
      "payout": "$24.75",
      "note": "2-leg only. Both high confidence."
    }},
    "passes": [{{"game": "MIN @ TOR", "reason": "No edge at -165. True prob ~58%, implied 62%. -EV."}}],
    "total_action": "$95",
    "session_sizing_note": "Adjust if any line moves 5+ cents before placing"
  }},
  "clv_watchlist": [
    {{"bet": "NYY F5", "current_odds": "-120", "expected_close": "-132", "action": "BET EARLY"}}
  ],
  "hedge_setups": [
    {{
      "scenario": "If NYY F5 + UNDER parlay hits and have 3rd leg live",
      "current_payout": "$39.75 on $15",
      "hedge_stake": "Bet $18 on opposite side live",
      "locked_profit": "$6.75 guaranteed",
      "ride_ev": "$12.40 EV",
      "recommendation": "RIDE — EV exceeds locked profit"
    }}
  ],
  "research_flags": ["Confirm Fried starts — check 1hr before first pitch"],
  "watchlist": ["SD @ COL — monitor wind forecast, could flip total"]
}}"""


def run_mega_scout():
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    print(f"[{datetime.now(ET).strftime('%H:%M ET')}] Running mega scout for {today}...")
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4000,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": (
                f"Full mega scout for {today}. Search every MLB game not yet started. "
                "For each: confirmed SPs with stats, current ML and F5 odds, records, "
                "last 10 days form, bullpen fatigue, weather, injury news. "
                "Score every game. Analyze all bet types. Build the mega board. Pure JSON only."
            )
        }]
    )
    raw = "".join(b.text for b in response.content if b.type == "text")
    raw = raw.replace("```json","").replace("```","").strip()
    s, e = raw.find("{"), raw.rfind("}")
    if s == -1 or e == -1:
        raise ValueError(f"No JSON in response: {raw[:200]}")
    try:
        data = json.loads(raw[s:e+1])
    except json.JSONDecodeError:
        import re
        raw_clean = re.sub(r'[\x00-\x1f\x7f]', '', raw[s:e+1])
        raw_clean = re.sub(r',(\s*[}\]])', r'\1', raw_clean)
        data = json.loads(raw_clean)
    print(f"[{datetime.now(ET).strftime('%H:%M ET')}] Scout complete — {len(data.get('games',[]))} games")
    return data


def run_math_on_board(data, clv_log):
    """Run all math automatically on every recommended bet."""
    board       = data.get("mega_board", {})
    new_entries = []
    all_play_types = [
        ("sharp_plays", "ML"),
        ("f5_plays",    "F5"),
        ("prop_plays",  "PROP"),
        ("nrfi_plays",  "NRFI"),
        ("total_plays", "TOTAL"),
    ]

    # Build odds lookup from games
    odds_map = {}
    for g in data.get("games", []):
        away = g.get("away","")
        home = g.get("home","")
        odds_map[away] = {"ml": g.get("aml",""), "opp": g.get("hml","")}
        odds_map[home] = {"ml": g.get("hml",""), "opp": g.get("aml","")}

    for section_key, default_type in all_play_types:
        for play in board.get(section_key, []):
            odds  = play.get("odds","")
            btype = play.get("type", default_type)

            # Find opposing odds for no-vig calc
            team_name = play.get("bet","").split(" ")[0]
            opp_odds  = odds_map.get(team_name, {}).get("opp")

            # Implied prob
            imp = implied_prob(odds)
            play["implied_prob"] = imp

            # No-vig true probability
            if opp_odds:
                nv = no_vig_prob(odds, opp_odds)
                play["true_prob"]  = nv.get("side1_true")
                play["vig_pct"]    = nv.get("vig_pct")
            else:
                play["true_prob"] = None
                play["vig_pct"]   = None

            # EV
            if play.get("true_prob"):
                ev = expected_value(odds, play["true_prob"])
                play["ev_dollars"] = ev.get("ev_dollars")
                play["edge_pct"]   = ev.get("edge_pct")
                play["ev_verdict"] = ev.get("verdict")

            # Kelly sizing
            if play.get("true_prob"):
                kelly = kelly_criterion(odds, play["true_prob"])
                play["recommended_stake"] = kelly.get("kelly_stake")

            # Log to CLV tracker
            new_entries.append({
                "date":         today,
                "bet":          play.get("bet",""),
                "type":         btype,
                "bet_odds":     odds,
                "opp_odds":     opp_odds,
                "implied_prob": imp,
                "true_prob":    play.get("true_prob"),
                "edge_pct":     play.get("edge_pct"),
                "ev_dollars":   play.get("ev_dollars"),
                "closing_odds": None,
                "result":       None,
                "clv_pct":      None,
            })

    # Parlay math
    parlay = board.get("parlay", {})
    if parlay and parlay.get("legs"):
        odds_list = []
        for leg in parlay["legs"]:
            parts = leg.split(" ")
            for p in parts:
                if p.startswith(("+","-")) and p[1:].replace(".","").isdigit():
                    odds_list.append(p)
                    break
        if odds_list:
            pm = parlay_calc(odds_list)
            if pm.get("valid"):
                parlay["calculated_odds"]    = pm["american"]
                parlay["calculated_decimal"] = pm["decimal"]
                parlay["payout_15"]          = f"${pm['payout_15']}"
                parlay["payout_25"]          = f"${pm['payout_25']}"

    return new_entries


def format_mega_board(data, clv_stats=None):
    """Format the full mega board into Telegram messages."""
    messages = []
    board   = data.get("mega_board", {})
    verdict = data.get("session_verdict","—")
    v_emoji = "🟢" if "FULL" in verdict else "🟡" if "REDUCED" in verdict else "🔴"

    # MSG 1 — picks summary
    m1 = [f"⚾ PARLAY OS MEGA BOARD — {data.get('date', today)}",
          f"{v_emoji} {verdict}"]
    if data.get("session_note"):
        m1.append(f"_{data['session_note']}_")
    m1.append("")

    sharp = board.get("sharp_plays",[])
    if sharp:
        m1.append("🔒 SHARP PLAYS")
        for p in sharp:
            line = f"  {p.get('bet')} {p.get('odds')}  →  {p.get('size')}  Score:{p.get('score','?')}"
            if p.get("ev_dollars") is not None:
                line += f"  EV:${p['ev_dollars']}"
            if p.get("edge_pct") is not None:
                line += f"  Edge:{p['edge_pct']:+.1f}%"
            m1.append(line)
            if p.get("note"): m1.append(f"  {p['note']}")
        m1.append("")

    f5 = board.get("f5_plays",[])
    if f5:
        m1.append("5️⃣ F5 PLAYS")
        for p in f5:
            m1.append(f"  {p.get('bet')} {p.get('odds')}  →  {p.get('size')}")
            if p.get("note"): m1.append(f"  {p['note']}")
        m1.append("")

    props = board.get("prop_plays",[])
    if props:
        m1.append("🎯 PROPS")
        for p in props:
            m1.append(f"  {p.get('bet')} {p.get('odds')}  →  {p.get('size')}")
            if p.get("note"): m1.append(f"  {p['note']}")
        m1.append("")

    nrfi = board.get("nrfi_plays",[])
    if nrfi:
        m1.append("🚫 NRFI")
        for p in nrfi:
            m1.append(f"  {p.get('bet')} {p.get('odds')}  →  {p.get('size')}")
            if p.get("note"): m1.append(f"  {p['note']}")
        m1.append("")

    totals = board.get("total_plays",[])
    if totals:
        m1.append("📊 TOTALS")
        for p in totals:
            m1.append(f"  {p.get('bet')} {p.get('odds')}  →  {p.get('size')}")
            if p.get("note"): m1.append(f"  {p['note']}")
        m1.append("")

    parlay = board.get("parlay",{})
    if parlay and parlay.get("legs"):
        m1.append("🎲 PARLAY (2-3 legs max)")
        for leg in parlay["legs"]: m1.append(f"  • {leg}")
        odds_str = parlay.get("calculated_odds") or parlay.get("combined_odds","?")
        pay_str  = parlay.get("payout_15") or parlay.get("payout","?")
        m1.append(f"  Odds: {odds_str}  →  {parlay.get('size','$15')}  →  payout {pay_str}")
        if parlay.get("note"): m1.append(f"  {parlay['note']}")
        m1.append("")

    if board.get("total_action"):
        m1.append(f"💰 Total action: {board['total_action']}")
    if board.get("session_sizing_note"):
        m1.append(board['session_sizing_note'])

    messages.append("\n".join(m1))

    # MSG 2 — game breakdowns
    for g in data.get("games",[]):
        if g.get("ml_tag") == "pass" and not any([
            g.get("nrfi_play"),
            g.get("k_prop_play",{}).get("active"),
            g.get("team_total_play",{}).get("active"),
            g.get("total_play",{}).get("active"),
        ]): continue

        tag = g.get("ml_tag","").upper()
        t_emoji = "🔒" if tag=="LOCK" else "🪙" if tag=="CF" else "❌"
        gm = [f"\n{t_emoji} {g.get('away')} @ {g.get('home')} · {g.get('time','')}"]

        if g.get("away_record"):
            gm.append(f"  {g['away']} {g['away_record']} (L10:{g.get('away_last10','?')}) · {g['home']} {g.get('home_record','')} (L10:{g.get('home_last10','?')})")
        if g.get("weather"):
            gm.append(f"  {g['weather']}")
        if g.get("asp"):
            gm.append(f"  Away SP: {g['asp']} ERA:{g.get('asp_era','?')} FIP:{g.get('asp_fip','?')} K/9:{g.get('asp_k9','?')}")
            gm.append(f"  Home SP: {g.get('hsp','TBA')} ERA:{g.get('hsp_era','?')} FIP:{g.get('hsp_fip','?')} K/9:{g.get('hsp_k9','?')}")

        gm.append(f"  Score: {g.get('score','?')}/100")
        gm.append("")
        gm.append("  PLAYS:")

        if g.get("ml_tag") != "pass" and g.get("ml_pick"):
            gm.append(f"    ML: {g.get('ml_pick')} {g.get('ml_pick_odds','')} [{tag}] {g.get('clv_timing','')}")
        if g.get("f5_tag") and g["f5_tag"] != "pass" and g.get("f5_pick"):
            gm.append(f"    F5: {g.get('f5_pick')} {g.get('f5_pick_odds','')} — {g.get('f5_reasoning','')}")
        if g.get("nrfi_play"):
            gm.append(f"    NRFI: {g.get('nrfi_odds','')} — {g.get('nrfi_reasoning','')}")
        tt = g.get("team_total_play",{})
        if tt.get("active"):
            gm.append(f"    TEAM TOTAL: {tt.get('team')} {tt.get('line')} {tt.get('odds','')} — {tt.get('reasoning','')}")
        kp = g.get("k_prop_play",{})
        if kp.get("active"):
            gm.append(f"    K PROP: {kp.get('pitcher')} {kp.get('line')} {kp.get('odds','')} — {kp.get('reasoning','')}")
        gtp = g.get("total_play",{})
        if gtp.get("active"):
            gm.append(f"    TOTAL: {gtp.get('direction','').upper()} {gtp.get('line')} {gtp.get('odds','')} — {gtp.get('reasoning','')}")

        gm.append("")
        if g.get("analysis"): gm.append(f"  {g['analysis']}")
        for f in g.get("factors",[])[:3]: gm.append(f"  ↑ {f}")
        for r in g.get("risks",[])[:2]:   gm.append(f"  ⚠ {r}")
        if g.get("hedge_alert") and g["hedge_alert"] not in ["null","None",None,""]:
            gm.append(f"  🔀 HEDGE: {g['hedge_alert']}")
        gm.append("  ─────────────────")
        messages.append("\n".join(gm))

    # MSG 3 — CLV + hedge + flags
    m3 = []
    clv_wl = data.get("clv_watchlist",[])
    if clv_wl:
        m3.append("📈 CLV WATCHLIST — Timing matters")
        for c in clv_wl:
            m3.append(f"  {c.get('bet')} @ {c.get('current_odds')} → closes ~{c.get('expected_close','?')}")
            if c.get("action"): m3.append(f"  {c['action']}")
        m3.append("")

    hedges = data.get("hedge_setups",[])
    if hedges:
        m3.append("🔀 HEDGE SETUPS")
        for h in hedges:
            m3.append(f"  Scenario: {h.get('scenario','')}")
            m3.append(f"  Locked profit: {h.get('locked_profit','')} · Ride EV: {h.get('ride_ev','')}")
            m3.append(f"  → {h.get('recommendation','')}")
        m3.append("")

    passes = board.get("passes",[])
    if passes:
        m3.append("❌ PASSING ON:")
        for p in passes:
            m3.append(f"  {p.get('game','')} — {p.get('reason','')}")
        m3.append("")

    flags = data.get("research_flags",[])
    if flags:
        m3.append("🚩 CONFIRM BEFORE BETTING:")
        for f in flags: m3.append(f"  • {f}")
        m3.append("")

    if clv_stats and clv_stats.get("total",0) > 0:
        m3.append(format_clv_stats_telegram(clv_stats))

    m3.append(f"Generated {datetime.now(ET).strftime('%I:%M %p ET')} · Parlay OS")
    messages.append("\n".join(m3))
    return messages


def load_clv_log():
    try:
        with open("clv_log.json") as f: return json.load(f)
    except: return []

def save_clv_log(log):
    with open("clv_log.json","w") as f: json.dump(log, f, indent=2)


def send_telegram(text):
    import time
    url    = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
    for i, chunk in enumerate(chunks):
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": chunk,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }, timeout=10)
        if not r.ok:
            raise Exception(f"Telegram {r.status_code}: {r.text}")
        if i < len(chunks)-1: time.sleep(0.5)


def main():
    import time
    try:
        data    = run_mega_scout()
        clv_log = load_clv_log()

        new_entries = run_math_on_board(data, clv_log)

        with open("last_scout.json","w") as f:
            json.dump(data, f, indent=2)

        if new_entries:
            clv_log.extend(new_entries)
            save_clv_log(clv_log)
            print(f"  Logged {len(new_entries)} bets to CLV tracker")

        stats    = clv_stats_summary(clv_log)
        messages = format_mega_board(data, stats)

        for i, msg in enumerate(messages):
            if msg.strip():
                send_telegram(msg)
                if i < len(messages)-1: time.sleep(1)

        print(f"[{datetime.now(ET).strftime('%H:%M ET')}] Done. {len(messages)} messages sent.")

    except Exception as e:
        try:
            send_telegram(f"⚠️ PARLAY OS — Scout Failed\n{str(e)}\n{datetime.now(ET).strftime('%I:%M %p ET')}")
        except: pass
        print(f"FATAL: {e}")
        raise


if __name__ == "__main__":
    main()
