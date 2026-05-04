"""
PARLAY OS — AUTO CLV TRACKER
Runs every night at 11PM ET via GitHub Actions.
Pulls final scores, finds closing lines, calculates CLV automatically.
Sends nightly recap to Telegram. Zero manual input required.
"""

import anthropic
import requests
import os
import json
from datetime import datetime
import pytz

from math_engine import calc_clv, clv_stats_summary, format_clv_stats_telegram

ANTHROPIC_API_KEY  = os.environ["ANTHROPIC_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]
ET       = pytz.timezone("America/New_York")
now      = datetime.now(ET)
today    = now.strftime("%A, %B %d, %Y")
date_key = now.strftime("%Y-%m-%d")

CLV_LOG_FILE  = "clv_log.json"
HISTORY_FILE  = "bet_history.json"

def load_json(path, default):
    try:
        with open(path) as f: return json.load(f)
    except: return default

def save_json(path, data):
    with open(path,"w") as f: json.dump(data, f, indent=2)


RESOLVE_PROMPT = f"""You are resolving tonight's MLB bets for Parlay OS. Today is {today}.

Find final scores for all MLB games today, then for each bet determine:
1. Did it WIN or LOSE? (W/L/P for push)
2. What was the CLOSING LINE right before first pitch?

Rules for W/L:
- ML: did that team win? W or L.
- F5: who was winning after exactly 5 innings? W or L.
- NRFI: did neither team score in the 1st inning? W=yes, L=no.
- OVER/UNDER total: did runs go over or under the line? W or L.
- K PROP: did pitcher record over/under the K line? W or L.
- TEAM TOTAL: did that team score over/under their line? W or L.
- RL +1.5: did that team lose by 1 or win outright? W=yes, L=lost by 2+.

For closing lines: search the game's closing odds or last available pre-game line.
If you cannot find a closing line use null.

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
      "notes": "any relevant context"
    }}
  ],
  "session_summary": {{
    "wins": 0,
    "losses": 0,
    "pushes": 0,
    "narrative": "2-3 honest sentences on what worked, what didn't, what to learn."
  }}
}}"""


def resolve_tonight(bets_today):
    if not bets_today: return None
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    bets_formatted = json.dumps([{
        "bet":      b.get("bet",""),
        "type":     b.get("type",""),
        "bet_odds": b.get("bet_odds",""),
    } for b in bets_today], indent=2)

    print(f"Resolving {len(bets_today)} bets for {today}...")
    response = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=2000,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        system=RESOLVE_PROMPT,
        messages=[{"role":"user","content":f"Resolve these bets from {today}:\n\n{bets_formatted}"}]
    )
    raw = "".join(b.text for b in response.content if b.type == "text")
    raw = raw.replace("```json","").replace("```","").strip()
    s, e = raw.find("{"), raw.rfind("}")
    if s == -1 or e == -1:
        raise ValueError(f"No JSON in resolver: {raw[:200]}")
    return json.loads(raw[s:e+1])


def format_nightly_recap(resolved_data, stats, bets_today):
    summary = resolved_data.get("session_summary",{})
    bets    = resolved_data.get("resolved_bets",[])
    wins    = summary.get("wins",0)
    losses  = summary.get("losses",0)
    r_emoji = "🟢" if wins > losses else "🔴" if losses > wins else "🟡"

    lines = [
        f"⚾ PARLAY OS — NIGHTLY RECAP",
        f"{today}",
        f"{r_emoji} {wins}W — {losses}L tonight",
        "",
    ]

    for b in bets:
        result  = b.get("result","?")
        clv     = calc_clv(b.get("bet_odds",""), b.get("closing_odds","")) if b.get("closing_odds") else {}
        clv_pct = clv.get("clv_pct")
        r_emoji = "✅" if result=="W" else "❌" if result=="L" else "↩️"
        clv_str = f"CLV: {clv_pct:+.1f}%" if clv_pct is not None else "CLV: —"
        score   = b.get("game_score","")

        lines.append(f"{r_emoji} {b.get('bet','')} @ {b.get('bet_odds','')} → closed {b.get('closing_odds','—')}")
        lines.append(f"   {clv_str}  ·  {score}")
        if b.get("notes"): lines.append(f"   {b['notes']}")

    lines.append("")
    if summary.get("narrative"):
        lines.append("POST-SESSION:")
        lines.append(summary["narrative"])
        lines.append("")

    if stats and stats.get("total",0) > 0:
        lines.append(format_clv_stats_telegram(stats))

    lines.append(f"Auto-resolved {now.strftime('%I:%M %p ET')} · Parlay OS")
    return "\n".join(lines)


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
    print(f"[{now.strftime('%H:%M ET')}] Starting auto CLV tracker for {today}...")

    clv_log     = load_json(CLV_LOG_FILE, [])
    bet_history = load_json(HISTORY_FILE, {})
    scout_data  = load_json("last_scout.json", {})

    # Find today's unresolved bets
    bets_today = [b for b in clv_log if b.get("date") == today and b.get("result") is None]

    # Fallback: reconstruct from last scout if log is empty
    if not bets_today:
        board = scout_data.get("mega_board",{})
        all_plays = (
            board.get("sharp_plays",[]) + board.get("f5_plays",[]) +
            board.get("prop_plays",[]) + board.get("nrfi_plays",[]) +
            board.get("total_plays",[])
        )
        bets_today = [{
            "date":         today,
            "bet":          p.get("bet",""),
            "type":         p.get("type","ML"),
            "bet_odds":     p.get("odds",""),
            "closing_odds": None,
            "result":       None,
        } for p in all_plays if p.get("bet")]

    if not bets_today:
        print("No bets found for today.")
        send_telegram(f"📭 PARLAY OS — No bets logged for {today}. Scout may not have run.")
        return

    resolved_data = resolve_tonight(bets_today)
    if not resolved_data:
        raise ValueError("Resolver returned no data")

    # Update CLV log with results
    resolved_bets = resolved_data.get("resolved_bets",[])
    updated = 0
    for rb in resolved_bets:
        bet_name = rb.get("bet","")
        matched  = False
        for entry in clv_log:
            if entry.get("date")==today and entry.get("bet")==bet_name and entry.get("result") is None:
                entry["closing_odds"] = rb.get("closing_odds")
                entry["result"]       = rb.get("result")
                entry["game_score"]   = rb.get("game_score","")
                entry["notes"]        = rb.get("notes","")
                if rb.get("closing_odds"):
                    clv = calc_clv(entry.get("bet_odds",""), rb["closing_odds"])
                    entry["clv_pct"] = clv.get("clv_pct")
                updated += 1
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
                "notes":        rb.get("notes",""),
            })
            updated += 1

    save_json(CLV_LOG_FILE, clv_log)
    bet_history[date_key] = {"bets": resolved_bets, "summary": resolved_data.get("session_summary",{})}
    save_json(HISTORY_FILE, bet_history)
    print(f"Updated {updated} bets in CLV log.")

    stats  = clv_stats_summary(clv_log)
    recap  = format_nightly_recap(resolved_data, stats, bets_today)
    send_telegram(recap)
    print(f"[{now.strftime('%H:%M ET')}] Nightly recap sent.")

    if stats:
        print(f"All-time: {stats['total']} bets | Avg CLV: {stats['avg_clv']:+.2f}% | WR: {stats['win_rate']}%")


if __name__ == "__main__":
    main()
