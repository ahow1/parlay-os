"""
PARLAY OS — AUTO CLV TRACKER + POST-GAME ANALYST v2
Runs every night at 11PM ET via GitHub Actions.
Full post-game debrief + bankroll management + stake recommendations.
"""

import anthropic
import requests
import os
import json
from datetime import datetime
import pytz

from math_engine import calc_clv, clv_stats_summary, format_clv_stats_telegram, kelly_criterion

ANTHROPIC_API_KEY  = os.environ["ANTHROPIC_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]
ET       = pytz.timezone("America/New_York")
now      = datetime.now(ET)
today    = now.strftime("%A, %B %d, %Y")
date_key = now.strftime("%Y-%m-%d")

CLV_LOG_FILE      = "clv_log.json"
HISTORY_FILE      = "bet_history.json"
LESSONS_FILE      = "model_lessons.json"
BANKROLL_FILE     = "bankroll.json"
STARTING_BANKROLL = 100.0

def load_json(path, default):
    try:
        with open(path) as f: return json.load(f)
    except: return default

def save_json(path, data):
    with open(path, "w") as f: json.dump(data, f, indent=2)


def get_bankroll(history):
    """Calculate current bankroll from all session results."""
    br = load_json(BANKROLL_FILE, {"starting": STARTING_BANKROLL, "current": STARTING_BANKROLL, "sessions": 0, "total_wagered": 0})
    return br


def calc_session_pnl(resolved_bets, default_stake=25.0):
    """Calculate P&L for tonight's session."""
    total_wagered = 0
    total_returned = 0
    for b in resolved_bets:
        result = b.get("result","")
        odds_str = b.get("bet_odds","")
        if not odds_str or result not in ["W","L","P"]:
            continue
        try:
            odds = float(odds_str)
            stake = default_stake
            total_wagered += stake
            if result == "W":
                profit = (odds/100 * stake) if odds > 0 else (100/abs(odds) * stake)
                total_returned += stake + profit
            elif result == "P":
                total_returned += stake
        except:
            continue
    return round(total_returned - total_wagered, 2), round(total_wagered, 2)


def recommend_stakes(bankroll, model_confidence=7):
    """
    Recommend stake sizes based on current bankroll and model confidence.
    Uses tiered Kelly-inspired sizing.
    Confidence 1-10 affects multiplier.
    """
    conf_mult = 0.7 + (model_confidence / 10) * 0.6  # 0.7 to 1.3
    base = bankroll * conf_mult

    sharp  = round(min(base * 0.35, bankroll * 0.40), 2)
    value  = round(min(base * 0.22, bankroll * 0.25), 2)
    prop   = round(min(base * 0.16, bankroll * 0.20), 2)
    parlay = round(min(base * 0.14, bankroll * 0.18), 2)
    total  = round(sharp + value + prop + parlay, 2)

    return {
        "sharp_play":  sharp,
        "value_play":  value,
        "prop_play":   prop,
        "parlay":      parlay,
        "total":       total,
        "bankroll":    round(bankroll, 2),
        "confidence":  model_confidence,
    }


RESOLVE_PROMPT = f"""You are resolving tonight's MLB bets for Parlay OS. Today is {today}.

Search for final scores of all MLB games today. For each bet determine W/L and find the closing line.

W/L rules:
- ML: did that team win? W or L.
- F5: who was winning after exactly 5 innings? W or L.
- NRFI: did neither team score in 1st inning? W=yes L=no.
- OVER/UNDER: did total runs go over/under? W or L.
- K PROP: did pitcher go over/under K line? W or L.
- RL +1.5: did team lose by 1 or win? W=yes L=lost by 2+.

Return ONLY pure JSON:
{{
  "date": "{today}",
  "resolved_bets": [
    {{
      "bet": "NYY ML",
      "type": "ML",
      "bet_odds": "-130",
      "closing_odds": "-145",
      "result": "W",
      "game_score": "NYY 5, BOS 2",
      "notes": "brief context"
    }}
  ],
  "session_summary": {{
    "wins": 0,
    "losses": 0,
    "pushes": 0,
    "narrative": "2-3 honest sentences."
  }}
}}"""


ANALYSIS_PROMPT = f"""You are a sharp MLB betting analyst reviewing tonight's session for Parlay OS. Today is {today}.

Be brutally honest. Do not sugarcoat losses. Identify exactly what went wrong and why.

Scoring model: SP xFIP/SIERA 25%, Bullpen 20%, Offense wRC+ 18%, Run Diff 12%, Platoon 8%, Injury/Lineup 7%, Home/Road 5%, Park 3%, Line 2%.

Analyze every angle:
1. Did the starting pitchers we built picks around actually perform? ERA, K count, how long they lasted.
2. Bullpen impact — did a pen blow a lead, surprise us, or hold when expected?
3. Injury surprises — any unexpected scratches or lineup changes that affected outcomes?
4. Line movement validation — did sharp money agree with our picks (CLV positive)?
5. Variance analysis — was each loss a good bet that lost to variance, or a genuinely bad bet?
6. Running model confidence 1-10 based on how well the model predicted tonight.
7. What we got right specifically.
8. What we got wrong specifically.
9. What we should have passed on and why.
10. Opportunities we missed — games we passed on that we should have played.
11. The single most important lesson from tonight.
12. Any specific model rule or weight to update.
13. What to focus on in tomorrow's session.

Return ONLY pure JSON:
{{
  "sp_performance": ["SP name: what happened, K count, innings, ERA impact"],
  "bullpen_impact": ["team: what the pen did and how it affected our bets"],
  "injury_surprises": ["any unexpected changes that affected outcomes"],
  "line_movement_verdict": "Sharp money agreed/disagreed with our picks overall",
  "variance_flags": [
    {{"bet": "NYY ML", "verdict": "GOOD BET LOST TO VARIANCE", "reason": "lost 1-0 on solo HR, model was correct"}}
  ],
  "model_confidence": 7,
  "got_right": ["specific thing 1", "specific thing 2"],
  "got_wrong": ["specific thing 1", "specific thing 2"],
  "should_have_passed": ["game and exact reason"],
  "missed_opportunities": ["game and exact reason"],
  "top_lesson": "Single most important takeaway in one sentence",
  "model_update": "Specific rule change to consider or null",
  "next_session_focus": "What to prioritize tomorrow"
}}"""


def resolve_tonight(bets_today):
    if not bets_today: return None
    import time
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    bets_formatted = json.dumps([{"bet": b.get("bet",""), "type": b.get("type",""), "bet_odds": b.get("bet_odds","")} for b in bets_today], indent=2)
    print("Waiting 60s before resolve call...")
    time.sleep(60)
    print(f"Resolving {len(bets_today)} bets...")
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2000,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        system=RESOLVE_PROMPT,
        messages=[{"role":"user","content":f"Resolve these bets from {today}:\n\n{bets_formatted}"}]
    )
    raw = "".join(b.text for b in response.content if b.type == "text")
    raw = raw.replace("```json","").replace("```","").strip()
    s, e = raw.find("{"), raw.rfind("}")
    if s == -1 or e == -1: raise ValueError("No JSON in resolver")
    return json.loads(raw[s:e+1])


def run_post_game_analysis(bets_today, resolved_data, last_scout):
    import time
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    context = {
        "bets_made": [{
            "bet":        b.get("bet",""),
            "type":       b.get("type",""),
            "odds":       b.get("bet_odds",""),
            "model_score": b.get("model_score","?"),
            "result":     next((r.get("result") for r in resolved_data.get("resolved_bets",[]) if r.get("bet")==b.get("bet")), "?"),
            "game_score": next((r.get("game_score") for r in resolved_data.get("resolved_bets",[]) if r.get("bet")==b.get("bet")), "?"),
            "closing_odds": next((r.get("closing_odds") for r in resolved_data.get("resolved_bets",[]) if r.get("bet")==b.get("bet")), "?"),
        } for b in bets_today],
        "session_narrative": resolved_data.get("session_summary",{}).get("narrative",""),
    }
    print("Waiting 60s before analysis call...")
    time.sleep(60)
    print("Running post-game analysis...")
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2000,
        system=ANALYSIS_PROMPT,
        messages=[{"role":"user","content":f"Analyze tonight:\n\n{json.dumps(context, indent=2)}"}]
    )
    raw = "".join(b.text for b in response.content if b.type == "text")
    raw = raw.replace("```json","").replace("```","").strip()
    s, e = raw.find("{"), raw.rfind("}")
    if s == -1 or e == -1: return None
    return json.loads(raw[s:e+1])


def format_nightly_recap(resolved_data, stats, analysis, bankroll_data, stakes, session_pnl):
    summary  = resolved_data.get("session_summary",{})
    bets     = resolved_data.get("resolved_bets",[])
    wins     = summary.get("wins",0)
    losses   = summary.get("losses",0)
    pnl, wagered = session_pnl
    r_emoji  = "GREEN" if wins > losses else "RED" if losses > wins else "YELLOW"

    lines = [
        "PARLAY OS — NIGHTLY RECAP",
        today,
        f"{r_emoji} {wins}W - {losses}L tonight",
        f"Session P&L: {'+'if pnl>=0 else ''}{pnl} on ${wagered} wagered",
        "",
    ]

    # Bet results
    for b in bets:
        result   = b.get("result","?")
        clv      = calc_clv(b.get("bet_odds",""), b.get("closing_odds","")) if b.get("closing_odds") else {}
        clv_pct  = clv.get("clv_pct")
        r_tag    = "W" if result=="W" else "L" if result=="L" else "P"
        clv_str  = f"CLV: {clv_pct:+.1f}%" if clv_pct is not None else "CLV: pending"
        lines.append(f"[{r_tag}] {b.get('bet','')} @ {b.get('bet_odds','')} -> closed {b.get('closing_odds','?')}")
        lines.append(f"   {clv_str} | {b.get('game_score','')}")
        if b.get("notes"): lines.append(f"   {b['notes']}")

    lines.append("")
    if summary.get("narrative"):
        lines.append("TONIGHT:")
        lines.append(summary["narrative"])
        lines.append("")

    # Bankroll section
    lines.append("=" * 30)
    lines.append("BANKROLL STATUS")
    lines.append("=" * 30)
    current_br = bankroll_data.get("current", STARTING_BANKROLL)
    starting   = bankroll_data.get("starting", STARTING_BANKROLL)
    total_pnl  = round(current_br - starting, 2)
    sessions   = bankroll_data.get("sessions", 0)
    lines.append(f"Current bankroll: ${current_br}")
    lines.append(f"All-time P&L: {'+'if total_pnl>=0 else ''}{total_pnl} ({sessions} sessions)")
    lines.append("")
    lines.append("TOMORROW STAKES:")
    lines.append(f"  Sharp play:  ${stakes['sharp_play']}")
    lines.append(f"  Value play:  ${stakes['value_play']}")
    lines.append(f"  Prop play:   ${stakes['prop_play']}")
    lines.append(f"  Parlay:      ${stakes['parlay']}")
    lines.append(f"  Total:       ${stakes['total']}")
    conf = stakes['confidence']
    conf_label = "HIGH" if conf >= 8 else "MEDIUM" if conf >= 6 else "LOW"
    lines.append(f"  Model confidence: {conf}/10 ({conf_label})")
    lines.append("")

    # Post-game analysis
    if analysis:
        lines.append("=" * 30)
        lines.append("POST-GAME DEBRIEF")
        lines.append("=" * 30)
        lines.append("")

        if analysis.get("sp_performance"):
            lines.append("SP PERFORMANCE:")
            for item in analysis["sp_performance"]:
                lines.append(f"  {item}")
            lines.append("")

        if analysis.get("bullpen_impact"):
            lines.append("BULLPEN IMPACT:")
            for item in analysis["bullpen_impact"]:
                lines.append(f"  {item}")
            lines.append("")

        if analysis.get("injury_surprises"):
            lines.append("INJURY SURPRISES:")
            for item in analysis["injury_surprises"]:
                lines.append(f"  ! {item}")
            lines.append("")

        if analysis.get("line_movement_verdict"):
            lines.append(f"LINE MOVEMENT: {analysis['line_movement_verdict']}")
            lines.append("")

        if analysis.get("variance_flags"):
            lines.append("VARIANCE FLAGS:")
            for v in analysis["variance_flags"]:
                lines.append(f"  {v.get('bet','')} — {v.get('verdict','')}")
                lines.append(f"  {v.get('reason','')}")
            lines.append("")

        if analysis.get("got_right"):
            lines.append("WHAT WE GOT RIGHT:")
            for item in analysis["got_right"]:
                lines.append(f"  + {item}")
            lines.append("")

        if analysis.get("got_wrong"):
            lines.append("WHAT WE GOT WRONG:")
            for item in analysis["got_wrong"]:
                lines.append(f"  - {item}")
            lines.append("")

        if analysis.get("should_have_passed"):
            lines.append("SHOULD HAVE PASSED:")
            for item in analysis["should_have_passed"]:
                lines.append(f"  ! {item}")
            lines.append("")

        if analysis.get("missed_opportunities"):
            lines.append("MISSED OPPORTUNITIES:")
            for item in analysis["missed_opportunities"]:
                lines.append(f"  ? {item}")
            lines.append("")

        if analysis.get("top_lesson"):
            lines.append(f"TOP LESSON: {analysis['top_lesson']}")
            lines.append("")

        if analysis.get("model_update") and analysis["model_update"] not in ["null", None]:
            lines.append(f"MODEL UPDATE: {analysis['model_update']}")
            lines.append("")

        if analysis.get("next_session_focus"):
            lines.append(f"TOMORROW FOCUS: {analysis['next_session_focus']}")
            lines.append("")

    # CLV stats
    if stats and stats.get("total",0) > 0:
        lines.append(format_clv_stats_telegram(stats))

    lines.append(f"\nAuto-resolved {now.strftime('%I:%M %p ET')} - Parlay OS")
    return "\n".join(lines)


def send_telegram(text):
    import time
    url    = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
    for i, chunk in enumerate(chunks):
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk, "disable_web_page_preview": True}, timeout=10)
        if i < len(chunks)-1: time.sleep(0.5)


def main():
    print(f"[{now.strftime('%H:%M ET')}] Starting CLV tracker for {today}...")

    clv_log     = load_json(CLV_LOG_FILE, [])
    bet_history = load_json(HISTORY_FILE, {})
    last_scout  = load_json("last_scout.json", {})
    lessons     = load_json(LESSONS_FILE, [])
    bankroll    = load_json(BANKROLL_FILE, {"starting": STARTING_BANKROLL, "current": STARTING_BANKROLL, "sessions": 0, "total_wagered": 0})

    # Find today's unresolved bets
    bets_today = [b for b in clv_log if b.get("date")==today and b.get("result") is None]

    # Fallback from last scout
    if not bets_today:
        all_picks = last_scout.get("locks",[]) + last_scout.get("coinflips",[])
        for pick in all_picks:
            parts = pick.split(" ")
            odds  = parts[-1] if parts else ""
            bets_today.append({
                "date":       today,
                "bet":        pick,
                "type":       "F5" if "F5" in pick else "NRFI" if "NRFI" in pick else "ML",
                "bet_odds":   odds,
                "closing_odds": None,
                "result":     None,
            })

    if not bets_today:
        send_telegram(f"PARLAY OS — No bets logged for {today}.")
        return

    # Resolve
    resolved_data = resolve_tonight(bets_today)
    if not resolved_data:
        raise ValueError("Resolver returned no data")

    # Update CLV log
    resolved_bets = resolved_data.get("resolved_bets",[])
    for rb in resolved_bets:
        bet_name = rb.get("bet","")
        matched  = False
        for entry in clv_log:
            if entry.get("date")==today and entry.get("bet")==bet_name and entry.get("result") is None:
                entry["closing_odds"] = rb.get("closing_odds")
                entry["result"]       = rb.get("result")
                entry["game_score"]   = rb.get("game_score","")
                if rb.get("closing_odds"):
                    clv = calc_clv(entry.get("bet_odds",""), rb["closing_odds"])
                    entry["clv_pct"] = clv.get("clv_pct")
                matched = True
                break
        if not matched:
            clv = calc_clv(rb.get("bet_odds",""), rb.get("closing_odds","")) if rb.get("closing_odds") else {}
            clv_log.append({
                "date":         today,
                "bet":          bet_name,
                "type":         rb.get("type","ML"),
                "bet_odds":     rb.get("bet_odds",""),
                "closing_odds": rb.get("closing_odds"),
                "result":       rb.get("result"),
                "clv_pct":      clv.get("clv_pct"),
                "game_score":   rb.get("game_score",""),
            })

    save_json(CLV_LOG_FILE, clv_log)

    # Update bankroll
    session_pnl = calc_session_pnl(resolved_bets)
    pnl, wagered = session_pnl
    bankroll["current"]       = round(bankroll["current"] + pnl, 2)
    bankroll["sessions"]      += 1
    bankroll["total_wagered"] = round(bankroll.get("total_wagered",0) + wagered, 2)
    save_json(BANKROLL_FILE, bankroll)

    # Save history
    bet_history[date_key] = {"bets": resolved_bets, "summary": resolved_data.get("session_summary",{}), "pnl": pnl}
    save_json(HISTORY_FILE, bet_history)

    # Post-game analysis
    analysis = None
    model_confidence = 7
    try:
        analysis = run_post_game_analysis(bets_today, resolved_data, last_scout)
        if analysis:
            model_confidence = analysis.get("model_confidence", 7)
            lesson = {"date": today, "top_lesson": analysis.get("top_lesson",""), "model_update": analysis.get("model_update",""), "confidence": model_confidence}
            lessons.append(lesson)
            save_json(LESSONS_FILE, lessons)
    except Exception as e:
        print(f"Analysis failed (non-fatal): {e}")

    # Stake recommendations for tomorrow
    stakes = recommend_stakes(bankroll["current"], model_confidence)

    # Send recap
    stats  = clv_stats_summary(clv_log)
    recap  = format_nightly_recap(resolved_data, stats, analysis, bankroll, stakes, session_pnl)
    send_telegram(recap)
    print(f"[{now.strftime('%H:%M ET')}] Done. Bankroll: ${bankroll['current']}")


if __name__ == "__main__":
    main()
