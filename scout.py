"""
PARLAY OS — MEGA SCOUT v3
Daily MLB research bot. Runs via GitHub Actions.
Simpler JSON format, robust parsing.
"""

import anthropic
import requests
import os
import json
import re
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

SYSTEM_PROMPT = f"""You are Aidan's MLB betting analyst. Today is {today}.

RULES:
- No permanent team labels. Evaluate every game independently.
- Never RL on favorites.
- Conditional: CIN=Burns only, NYY=Fried/Schlittler/Warren, LAA=Soriano, DET=Skubal, PIT=Skenes+Cruz
- HOU hard fade until Hader returns (pen ERA 6.31 worst MLB)
- CIN pen trap: ERA looks fine xFIP 4.89 confirmed regression Apr 29
- Record 5+ games below .500 overrides reputation
- Road lock at struggling home team needs 78+ score minimum
- Hot streaks are noise. Metrics only.

2026 ELITE SPs: Soriano LAA 0.24, Schlittler NYY 0.86 FIP, Skenes PIT 1.27, Sanchez PHI 1.59, Skubal DET 2.08, Sale ATL 2.21, Fried NYY 2.40, Burns CIN 2.42, Warren NYY 2.49, Gausman TOR 2.57

BULLPENS BEST: TOR lowest xFIP, MIL, CLE, LAD, SD
BULLPENS WORST: WAS 30th, HOU Hader+Brown IL, CWS, CIN xFIP trap

SCORING MODEL you score each game 0-100:
SP xFIP/SIERA 25pct, Bullpen 20pct, Offense wRC+ 18pct, Run Diff 12pct, Platoon 8pct, Injury/Lineup 7pct, Home/Road 5pct, Park/Weather 3pct, Line Movement 2pct
70+ LOCK, 50-69 COIN FLIP, below 50 PASS

SESSION FORMAT: 100 dollars total. Sharp play 30-40 dollars, value play 20-25 dollars, prop 15-20 dollars, parlay 2-3 legs max 15-20 dollars.

TASK: Search for ALL MLB games today not yet started. For each game find confirmed SPs, current ML odds, records, recent form, injuries, weather. Score every game. Find best plays.

CRITICAL: Return ONLY a valid JSON object. Rules for your JSON:
1. No newlines inside string values
2. No apostrophes in strings - use plain English instead
3. Keep all values short and simple
4. No special characters

Return exactly this structure with real data:
{{
  "date": "{today}",
  "verdict": "FULL SESSION",
  "note": "Slate summary",
  "games": [
    {{
      "away": "NYY",
      "home": "ATL",
      "time": "7:05 PM ET",
      "away_record": "22-9",
      "home_record": "20-11",
      "asp": "Fried",
      "asp_era": "2.40",
      "hsp": "Sale",
      "hsp_era": "2.21",
      "aml": "-130",
      "hml": "+110",
      "score": 74,
      "tag": "cf",
      "pick": "NYY ML",
      "pick_odds": "-130",
      "analysis": "Short plain analysis",
      "edge1": "SP edge description",
      "edge2": "Bullpen edge description",
      "risk1": "Main risk",
      "f5_pick": "NYY F5 -120",
      "nrfi": "yes",
      "nrfi_odds": "-135",
      "k_prop": "Fried over 7.5 Ks -112",
      "total": "under 8.0 -112",
      "clv_timing": "EARLY"
    }}
  ],
  "locks": ["NYY F5 -120"],
  "coinflips": ["ATL ML +110"],
  "passes": ["MIN ML"],
  "parlay_legs": ["NYY F5 -120", "LAD ML -140"],
  "parlay_odds": "+210",
  "session_note": "Sizing and timing advice"
}}"""


def run_mega_scout():
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    print(f"[{datetime.now(ET).strftime('%H:%M ET')}] Running scout for {today}...")

    response = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=3000,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": (
                f"Search for today's MLB games ({today}) not yet started. "
                "Find confirmed SPs, ML odds, records, recent form, injuries, weather. "
                "Score every game. Return clean simple JSON only. No special characters in strings."
            )
        }]
    )

    raw = "".join(b.text for b in response.content if b.type == "text")
    raw = raw.replace("```json", "").replace("```", "").strip()

    s = raw.find("{")
    e = raw.rfind("}")
    if s == -1 or e == -1:
        raise ValueError(f"No JSON found. Response: {raw[:300]}")

    json_str = raw[s:e+1]

    # Try multiple parse strategies
    for strategy in [
        lambda x: json.loads(x),
        lambda x: json.loads(re.sub(r'[\x00-\x1f\x7f]', ' ', x)),
        lambda x: json.loads(re.sub(r',(\s*[}\]])', r'\1', re.sub(r'[\x00-\x1f\x7f]', ' ', x))),
    ]:
        try:
            data = strategy(json_str)
            print(f"[{datetime.now(ET).strftime('%H:%M ET')}] Scout complete — {len(data.get('games', []))} games")
            return data
        except json.JSONDecodeError:
            continue

    # Fallback
    return {
        "date": today,
        "verdict": "PARSE ERROR",
        "note": "JSON parse failed. Check GitHub Actions logs.",
        "games": [], "locks": [], "coinflips": [], "passes": [],
        "parlay_legs": [], "session_note": "Manual check needed"
    }


def format_message(data, clv_stats=None):
    messages = []
    verdict = data.get("verdict", "")
    v_emoji = "🟢" if "FULL" in verdict else "🟡" if "REDUCED" in verdict else "🔴"

    m1 = [
        f"PARLAY OS - {data.get('date', today)}",
        f"{v_emoji} {verdict}",
        data.get("note", ""),
        "",
    ]

    locks  = data.get("locks", [])
    cfs    = data.get("coinflips", [])
    passes = data.get("passes", [])

    if locks:
        m1.append(f"LOCKS: {', '.join(locks)}")
    if cfs:
        m1.append(f"COIN FLIPS: {', '.join(cfs)}")
    if passes:
        m1.append(f"PASS: {', '.join(passes)}")

    parlay_legs = data.get("parlay_legs", [])
    if parlay_legs:
        m1.append("")
        m1.append("PARLAY:")
        for leg in parlay_legs:
            m1.append(f"  {leg}")
        m1.append(f"  Combined: {data.get('parlay_odds', '?')}")

    if data.get("session_note"):
        m1.append("")
        m1.append(data["session_note"])

    messages.append("\n".join(m1))

    for g in data.get("games", []):
        tag     = g.get("tag", "").upper()
        score   = g.get("score", "?")
        t_emoji = "LOCK" if tag == "LOCK" else "CF" if "CF" in tag else "PASS"

        gm = [
            f"\n[{t_emoji}] {g.get('away')} @ {g.get('home')} - {g.get('time', '')}",
            f"  {g.get('away')} {g.get('away_record', '')} vs {g.get('home')} {g.get('home_record', '')}",
            f"  {g.get('asp', 'TBA')} {g.get('asp_era', '')} ERA vs {g.get('hsp', 'TBA')} {g.get('hsp_era', '')} ERA",
        ]

        if g.get("aml") or g.get("hml"):
            ap = implied_prob(g.get("aml", ""))
            hp = implied_prob(g.get("hml", ""))
            gm.append(f"  Away: {g.get('aml', '?')} ({f'{ap:.1f}' if ap else '?'}%) Home: {g.get('hml', '?')} ({f'{hp:.1f}' if hp else '?'}%)")

        gm.append(f"  Score: {score}/100")
        gm.append("")

        if g.get("pick") and "PASS" not in tag:
            pick_odds = g.get("pick_odds", "")
            line = f"  PICK: {g.get('pick')} {pick_odds}"
            if g.get("aml") and g.get("hml") and pick_odds:
                try:
                    pick_is_away = g.get("away", "") in g.get("pick", "")
                    opp = g.get("hml") if pick_is_away else g.get("aml")
                    nv = no_vig_prob(pick_odds, opp)
                    if nv.get("side1_true"):
                        ev = expected_value(pick_odds, nv["side1_true"])
                        if ev.get("ev_dollars") is not None:
                            line += f"  EV:${ev['ev_dollars']:+.0f} Edge:{ev.get('edge_pct', 0):+.1f}%"
                except:
                    pass
            gm.append(line)

        if g.get("f5_pick"):
            gm.append(f"  F5: {g['f5_pick']}")
        if g.get("nrfi") == "yes" and g.get("nrfi_odds"):
            gm.append(f"  NRFI: {g['nrfi_odds']}")
        if g.get("k_prop"):
            gm.append(f"  K PROP: {g['k_prop']}")
        if g.get("total"):
            gm.append(f"  TOTAL: {g['total']}")

        gm.append("")
        if g.get("analysis"):
            gm.append(f"  {g['analysis']}")
        if g.get("edge1"):
            gm.append(f"  + {g['edge1']}")
        if g.get("edge2"):
            gm.append(f"  + {g['edge2']}")
        if g.get("risk1"):
            gm.append(f"  ! {g['risk1']}")
        if g.get("clv_timing"):
            gm.append(f"  Timing: {g['clv_timing']}")

        gm.append("  ---")
        messages.append("\n".join(gm))

    if clv_stats and clv_stats.get("total", 0) > 0:
        messages.append(format_clv_stats_telegram(clv_stats))

    messages.append(f"\nGenerated {datetime.now(ET).strftime('%I:%M %p ET')} - Parlay OS")
    return messages


def load_clv_log():
    try:
        with open("clv_log.json") as f:
            return json.load(f)
    except:
        return []


def save_clv_log(log):
    with open("clv_log.json", "w") as f:
        json.dump(log, f, indent=2)


def send_telegram(text):
    import time
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
    for i, chunk in enumerate(chunks):
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": chunk,
            "disable_web_page_preview": True,
        }, timeout=10)
        if i < len(chunks) - 1:
            time.sleep(0.5)


def main():
    import time
    try:
        data    = run_mega_scout()
        clv_log = load_clv_log()

        new_entries = []
        for pick in data.get("locks", []) + data.get("coinflips", []):
            parts = pick.split(" ")
            odds  = parts[-1] if parts else ""
            new_entries.append({
                "date":         today,
                "bet":          pick,
                "type":         "F5" if "F5" in pick else "NRFI" if "NRFI" in pick else "ML",
                "bet_odds":     odds,
                "closing_odds": None,
                "result":       None,
                "clv_pct":      None,
            })

        if new_entries:
            clv_log.extend(new_entries)
            save_clv_log(clv_log)

        with open("last_scout.json", "w") as f:
            json.dump(data, f, indent=2)

        stats    = clv_stats_summary(clv_log)
        messages = format_message(data, stats)

        for i, msg in enumerate(messages):
            if msg.strip():
                send_telegram(msg)
                if i < len(messages) - 1:
                    time.sleep(1)

        print(f"[{datetime.now(ET).strftime('%H:%M ET')}] Done. {len(messages)} messages sent.")

    except Exception as e:
        try:
            send_telegram(f"PARLAY OS Scout Failed\n{str(e)}\n{datetime.now(ET).strftime('%I:%M %p ET')}")
        except:
            pass
        print(f"FATAL: {e}")
        raise


if __name__ == "__main__":
    main()
