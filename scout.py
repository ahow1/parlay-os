"""
PARLAY OS - MEGA SCOUT v4
Daily MLB research bot.
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
- HOU hard fade until Hader returns
- CIN pen trap xFIP 4.89
- Record 5+ games below 500 overrides reputation

ELITE SPs: Soriano LAA 0.24, Schlittler NYY 0.86 FIP, Skenes PIT 1.27, Sanchez PHI 1.59, Skubal DET 2.08, Sale ATL 2.21, Fried NYY 2.40, Burns CIN 2.42, Warren NYY 2.49, Gausman TOR 2.57

BULLPENS BEST: TOR, MIL, CLE, LAD, SD
BULLPENS WORST: WAS, HOU, CWS, CIN

SCORING: SP 25pct, Bullpen 20pct, Offense 18pct, Run Diff 12pct, Platoon 8pct, Injury 7pct, Home/Road 5pct, Park 3pct, Line 2pct
70+ LOCK, 50-69 CF, below 50 PASS

Search for ALL MLB games today not yet started. Find SPs, odds, records, form, injuries, weather. Score every game.

Return ONLY this JSON with no extra text before or after it. Use only simple ASCII characters in all string values. No apostrophes. No special characters. Keep all strings short.

Return a JSON object with keys: date, verdict, note, games (array), locks (array), coinflips (array), passes (array), parlay_legs (array), parlay_odds, session_note. Each game needs: away, home, time, away_record, home_record, asp, asp_era, hsp, hsp_era, aml, hml, score, tag, pick, pick_odds, analysis, edge1, risk1, f5_pick, nrfi, nrfi_odds, k_prop, total, clv_timing."""


def run_mega_scout():
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    print(f"[{datetime.now(ET).strftime('%H:%M ET')}] Running scout for {today}...")

    response = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=5000,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": f"Search MLB games for {today} not yet started. Find SPs, odds, records. Score every game. Return only the JSON object."
        }]
    )

    raw = "".join(b.text for b in response.content if b.type == "text")

    print(f"=== RAW RESPONSE START ===")
    print(raw[:1000])
    print(f"=== RAW RESPONSE END ===")

    raw = raw.replace("```json", "").replace("```", "").strip()

    s = raw.find("{")
    e = raw.rfind("}")

    print(f"JSON found at positions: {s} to {e}")

    if s == -1 or e == -1:
        raise ValueError(f"No JSON braces found in response")

    json_str = raw[s:e+1]

    print(f"=== JSON ATTEMPT ===")
    print(json_str[:500])
    print(f"=== JSON ATTEMPT END ===")

    strategies = [
        ("direct", lambda x: json.loads(x)),
        ("strip_control", lambda x: json.loads(re.sub(r'[\x00-\x1f\x7f]', ' ', x))),
        ("fix_trailing_commas", lambda x: json.loads(re.sub(r',(\s*[}\]])', r'\1', re.sub(r'[\x00-\x1f\x7f]', ' ', x)))),
        ("encode_decode", lambda x: json.loads(x.encode('ascii', errors='ignore').decode('ascii'))),
    ]

    for name, strategy in strategies:
        try:
            data = strategy(json_str)
            print(f"Parse succeeded with strategy: {name}")
            print(f"[{datetime.now(ET).strftime('%H:%M ET')}] Scout complete - {len(data.get('games', []))} games")
            return data
        except json.JSONDecodeError as je:
            print(f"Strategy {name} failed: {str(je)[:100]}")
            continue

    raise ValueError(f"All parse strategies failed")


def format_message(data, clv_stats=None):
    messages = []
    verdict = data.get("verdict", "")
    v_emoji = "GREEN" if "FULL" in verdict else "YELLOW" if "REDUCED" in verdict else "RED"

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
        t_label = "[LOCK]" if tag == "LOCK" else "[CF]" if "CF" in tag else "[PASS]"

        gm = [
            f"\n{t_label} {g.get('away')} @ {g.get('home')} - {g.get('time', '')}",
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
                except Exception:
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
    except Exception:
        return []


def save_clv_log(log):
    with open("clv_log.json", "w") as f:
        json.dump(log, f, indent=2)


def send_telegram(text):
    import time
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
    for i, chunk in enumerate(chunks):
        requests.post(url, json={
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
        error_msg = f"PARLAY OS Scout Failed\n{str(e)}\n{datetime.now(ET).strftime('%I:%M %p ET')}"
        print(f"FATAL: {e}")
        try:
            send_telegram(error_msg)
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
