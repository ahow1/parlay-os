"""
PARLAY OS - MEGA SCOUT FINAL
"""

import anthropic
import requests
import os
import json
import re
import time
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

SYSTEM_PROMPT = """You are an MLB betting analyst. Search for today's games and return a JSON analysis.

Key rules: Never RL favorites. HOU fade (pen ERA 6.31). CIN pen trap (xFIP 4.89). NYY only with Fried/Schlittler/Warren. PIT only with Skenes+Cruz. CIN only with Burns. LAA only with Soriano. DET only with Skubal.

Elite SPs: Soriano LAA 0.24 ERA, Schlittler NYY 0.86 FIP, Skenes PIT 1.27, Sanchez PHI 1.59, Skubal DET 2.08, Sale ATL 2.21, Fried NYY 2.40, Burns CIN 2.42, Warren NYY 2.49, Gausman TOR 2.57.

Best bullpens: TOR, MIL, CLE, LAD. Worst: WAS, HOU, CWS, CIN.

Score each game 0-100: SP 25%, Bullpen 20%, Offense 18%, Run Diff 12%, Platoon 8%, Injury 7%, Home/Road 5%, Park 3%, Line 2%. 70+ is LOCK, 50-69 is CF, under 50 is PASS.

Return ONLY valid JSON always. Even if data is incomplete or conflicting use best estimates. Never refuse. Fill unknown fields with empty strings. No apostrophes in strings."""


def run_mega_scout():
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    print(f"[{datetime.now(ET).strftime('%H:%M ET')}] Waiting 60s for rate limit...")
    time.sleep(60)
    print(f"[{datetime.now(ET).strftime('%H:%M ET')}] Running scout for {today}...")

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": f"Today is {today}. Search for the top 5 MLB games today not yet started. Find SPs, odds, records. Score each game. Return only this JSON with real data filled in: {{\"date\":\"\",\"verdict\":\"\",\"note\":\"\",\"games\":[{{\"away\":\"\",\"home\":\"\",\"time\":\"\",\"away_record\":\"\",\"home_record\":\"\",\"asp\":\"\",\"asp_era\":\"\",\"hsp\":\"\",\"hsp_era\":\"\",\"aml\":\"\",\"hml\":\"\",\"score\":0,\"tag\":\"\",\"pick\":\"\",\"pick_odds\":\"\",\"analysis\":\"\",\"edge1\":\"\",\"risk1\":\"\",\"f5_pick\":\"\",\"nrfi\":\"no\",\"nrfi_odds\":\"\",\"k_prop\":\"\",\"total\":\"\",\"clv_timing\":\"\"}}],\"locks\":[],\"coinflips\":[],\"passes\":[],\"parlay_legs\":[],\"parlay_odds\":\"\",\"session_note\":\"\"}}"
        }]
    )

    raw = "".join(b.text for b in response.content if b.type == "text")
    print(f"RAW FIRST 200: {raw[:200]}")
    raw = raw.replace("```json", "").replace("```", "").strip()
    s = raw.find("{")
    e = raw.rfind("}")
    if s == -1 or e == -1:
        raise ValueError("No JSON found")
    json_str = raw[s:e+1]

    for name, fn in [
        ("direct",     lambda x: json.loads(x)),
        ("strip_ctrl", lambda x: json.loads(re.sub(r'[\x00-\x1f\x7f]', ' ', x))),
        ("fix_commas", lambda x: json.loads(re.sub(r',(\s*[}\]])', r'\1', re.sub(r'[\x00-\x1f\x7f]', ' ', x)))),
        ("ascii",      lambda x: json.loads(x.encode('ascii', errors='ignore').decode('ascii'))),
    ]:
        try:
            data = fn(json_str)
            print(f"Parsed OK with {name} — {len(data.get('games', []))} games")
            return data
        except Exception as e:
            print(f"{name} failed: {str(e)[:60]}")

    raise ValueError("All parse strategies failed")


def format_message(data, clv_stats=None):
    messages = []
    verdict  = data.get("verdict", "")
    emoji    = "GREEN" if "FULL" in verdict else "YELLOW" if "REDUCED" in verdict else "RED"

    m1 = [f"PARLAY OS - {data.get('date', today)}", f"{emoji} {verdict}", data.get("note", ""), ""]

    locks  = data.get("locks", [])
    cfs    = data.get("coinflips", [])
    passes = data.get("passes", [])

    if locks:  m1.append(f"LOCKS: {', '.join(locks)}")
    if cfs:    m1.append(f"COIN FLIPS: {', '.join(cfs)}")
    if passes: m1.append(f"PASS: {', '.join(passes)}")

    legs = data.get("parlay_legs", [])
    if legs:
        m1.append(f"\nPARLAY ({data.get('parlay_odds', '?')}):")
        for l in legs:
            m1.append(f"  {l}")

    if data.get("session_note"):
        m1.append(f"\n{data['session_note']}")

    messages.append("\n".join(m1))

    for g in data.get("games", []):
        tag   = g.get("tag", "").upper()
        score = g.get("score", "?")
        label = "[LOCK]" if tag == "LOCK" else "[CF]" if "CF" in tag else "[PASS]"

        gm = [
            f"\n{label} {g.get('away')} @ {g.get('home')} - {g.get('time', '')}",
            f"  {g.get('away')} {g.get('away_record','')} vs {g.get('home')} {g.get('home_record','')}",
            f"  {g.get('asp','TBA')} {g.get('asp_era','')} ERA vs {g.get('hsp','TBA')} {g.get('hsp_era','')} ERA",
        ]

        ap = implied_prob(g.get("aml", ""))
        hp = implied_prob(g.get("hml", ""))
        if ap or hp:
            gm.append(f"  Away: {g.get('aml','?')} ({f'{ap:.1f}' if ap else '?'}%)  Home: {g.get('hml','?')} ({f'{hp:.1f}' if hp else '?'}%)")

        gm.append(f"  Score: {score}/100")

        if g.get("pick") and "PASS" not in tag:
            po   = g.get("pick_odds", "")
            line = f"  PICK: {g.get('pick')} {po}"
            try:
                is_away = g.get("away", "") in g.get("pick", "")
                opp     = g.get("hml") if is_away else g.get("aml")
                nv      = no_vig_prob(po, opp)
                if nv.get("side1_true"):
                    ev = expected_value(po, nv["side1_true"])
                    if ev.get("ev_dollars") is not None:
                        line += f"  EV:${ev['ev_dollars']:+.0f} Edge:{ev.get('edge_pct',0):+.1f}%"
            except Exception:
                pass
            gm.append(line)

        if g.get("f5_pick"):                          gm.append(f"  F5: {g['f5_pick']}")
        if g.get("nrfi") == "yes" and g.get("nrfi_odds"): gm.append(f"  NRFI: {g['nrfi_odds']}")
        if g.get("k_prop"):                           gm.append(f"  K PROP: {g['k_prop']}")
        if g.get("total"):                            gm.append(f"  TOTAL: {g['total']}")
        if g.get("analysis"):                         gm.append(f"\n  {g['analysis']}")
        if g.get("edge1"):                            gm.append(f"  + {g['edge1']}")
        if g.get("risk1"):                            gm.append(f"  ! {g['risk1']}")
        if g.get("clv_timing"):                       gm.append(f"  Timing: {g['clv_timing']}")
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
    url    = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
    for i, chunk in enumerate(chunks):
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text":    chunk,
            "disable_web_page_preview": True,
        }, timeout=10)
        if i < len(chunks) - 1:
            time.sleep(0.5)


def main():
    try:
        data    = run_mega_scout()
        clv_log = load_clv_log()

        entries = []
        for pick in data.get("locks", []) + data.get("coinflips", []):
            parts = pick.split(" ") if isinstance(pick, str) else []
            odds  = parts[-1] if parts else ""
            entries.append({
                "date":         today,
                "bet":          pick,
                "type":         "F5" if "F5" in pick else "NRFI" if "NRFI" in pick else "ML",
                "bet_odds":     odds,
                "closing_odds": None,
                "result":       None,
                "clv_pct":      None,
            })

        if entries:
            clv_log.extend(entries)
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
        print(f"FATAL: {e}")
        try:
            send_telegram(f"PARLAY OS Failed\n{str(e)}\n{datetime.now(ET).strftime('%I:%M %p ET')}")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
