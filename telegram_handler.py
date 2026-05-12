"""PARLAY OS — telegram_handler.py
Background Telegram bot for bet logging and settling.
Runs as a daemon thread alongside brain.py analysis.

Commands:
  bet SF ML +162 5.32                → log ML bet
  bet TB TOR over 6.5 -115 3.55     → log game total
  win SF  / won SF  / SF won         → settle win
  loss BAL / lost 3 / BAL lost       → settle loss
  push TEX                           → settle push
  bankroll / br                      → balance + today P&L
  bets / pending                     → list open bets with IDs
  results / today                    → today settled bets

All bets logged via Telegram only — no manual DB editing.
After every action, last_scout.json is synced for dashboard refresh.
"""

import os
import re
import json
import time
import threading
import requests
from datetime import date, datetime
import pytz

import db as _db
from math_engine import american_to_decimal, calc_clv, STARTING_BANKROLL

BOT_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID      = os.getenv("TELEGRAM_CHAT_ID", "")
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
ET           = pytz.timezone("America/New_York")

# ── TEAM CODE LOOKUP ──────────────────────────────────────────────────────────

MLB_CODES = {
    "AZ","ATL","BAL","BOS","CHC","CWS","CIN","CLE","COL","DET",
    "HOU","KC","LAA","LAD","MIA","MIL","MIN","NYM","NYY","ATH",
    "OAK","PHI","PIT","SD","SF","SEA","STL","TB","TEX","TOR","WAS",
}

_ALIASES = {
    "giants":"SF","padres":"SD","dodgers":"LAD","angels":"LAA",
    "mariners":"SEA","athletics":"ATH","oakland":"ATH","yankees":"NYY",
    "mets":"NYM","red sox":"BOS","redsox":"BOS","cubs":"CHC",
    "white sox":"CWS","whitesox":"CWS","reds":"CIN","guardians":"CLE",
    "rockies":"COL","tigers":"DET","astros":"HOU","royals":"KC",
    "marlins":"MIA","brewers":"MIL","twins":"MIN","phillies":"PHI",
    "pirates":"PIT","cardinals":"STL","rays":"TB","rangers":"TEX",
    "blue jays":"TOR","bluejays":"TOR","nationals":"WAS","orioles":"BAL",
    "braves":"ATL","diamondbacks":"AZ","dbacks":"AZ","blue":"TOR",
}


def _team_code(token: str) -> str | None:
    u = token.upper().strip()
    if u in MLB_CODES:
        return u
    return _ALIASES.get(token.lower().strip())


# ── MATH HELPERS ──────────────────────────────────────────────────────────────

def _to_win(stake: float, odds_str: str) -> float:
    dec = american_to_decimal(str(odds_str))
    return round((dec - 1) * stake, 2) if dec else 0.0


def _current_bankroll() -> float:
    """Derive bankroll from DB without importing bankroll_engine (avoids circular import)."""
    bets    = _db.get_bets()
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
    pending = sum(float(b.get("stake") or 0) for b in bets if not b.get("result"))
    return round(current - pending, 2)


def _bankroll_display() -> dict:
    bets    = _db.get_bets()
    today   = date.today().isoformat()
    pending = [b for b in bets if not b.get("result")]
    today_r = [b for b in bets if b.get("result") in ("W","L") and b.get("date") == today]

    pnl = 0.0
    for b in today_r:
        stake = float(b.get("stake") or 0)
        if b["result"] == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                pnl += (dec - 1) * stake
        else:
            pnl -= stake

    wins    = sum(1 for b in today_r if b["result"] == "W")
    losses  = sum(1 for b in today_r if b["result"] == "L")
    pending_risk = round(sum(float(b.get("stake") or 0) for b in pending), 2)

    return {
        "bankroll":     _current_bankroll(),
        "pending_risk": pending_risk,
        "today_pnl":    round(pnl, 2),
        "today_record": f"{wins}-{losses}",
        "pending_count": len(pending),
    }


# ── DASHBOARD SYNC ────────────────────────────────────────────────────────────

def sync_scout_json():
    """Write current bankroll + pending bets into last_scout.json for dashboard."""
    try:
        bets    = _db.get_bets()
        pending = [b for b in bets if not b.get("result")]

        try:
            with open("last_scout.json") as f:
                scout = json.load(f)
        except Exception:
            scout = {}

        bd = _bankroll_display()
        scout["bankroll"]     = bd["bankroll"]
        scout["today_pnl"]    = bd["today_pnl"]
        scout["pending_bets"] = [
            {
                "id":    b["id"],
                "bet":   b["bet"],
                "type":  b.get("type", "ML"),
                "odds":  b.get("bet_odds", ""),
                "stake": b.get("stake", 0),
                "date":  b.get("date", ""),
            }
            for b in pending
        ]
        scout["last_updated"] = datetime.now(ET).isoformat()

        with open("last_scout.json", "w") as f:
            json.dump(scout, f, indent=2)
    except Exception as e:
        print(f"[TG] scout sync error: {e}")


# ── CLV FETCH ─────────────────────────────────────────────────────────────────

def _fetch_closing_odds(team_code: str, bet_type: str = "ML") -> str | None:
    """Pull Pinnacle odds for a team from the-odds-api (used as closing line)."""
    if not ODDS_API_KEY:
        return None
    from constants import MLB_TEAM_NAMES
    names = MLB_TEAM_NAMES.get(team_code, [team_code])

    market_key = "h2h"
    if bet_type.startswith(("O", "U")):
        market_key = "totals"

    try:
        r = requests.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds",
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "us",
                "markets": market_key,
                "oddsFormat": "american",
            },
            timeout=10,
        )
        r.raise_for_status()
        for event in r.json():
            for bk in event.get("bookmakers", []):
                if bk.get("key") not in ("pinnacle", "draftkings"):
                    continue
                for mkt in bk.get("markets", []):
                    if mkt.get("key") != market_key:
                        continue
                    for oc in mkt.get("outcomes", []):
                        oc_name = oc.get("name", "")
                        if any(n.lower() in oc_name.lower() for n in names):
                            return str(oc.get("price"))
    except Exception:
        pass
    return None


# ── BET PARSER ────────────────────────────────────────────────────────────────

def parse_bet(text: str) -> dict | None:
    """
    Parse: bet [TEAM] [optional TEAM2] [ML|F5|over LINE|under LINE] [ODDS] [STAKE]
    Returns dict with team/game/type/odds/stake or None if unparseable.
    """
    parts = text.split()
    if not parts or parts[0].lower() != "bet":
        return None
    parts = parts[1:]
    if not parts:
        return None

    # ── Find odds: [+-]\d{3,4}
    odds_idx, odds = -1, None
    for i, p in enumerate(parts):
        if re.match(r'^[+-]\d{3,4}$', p):
            odds_idx, odds = i, p
            break

    # ── Find stake: last decimal / small integer, not the odds index
    stake_idx, stake = -1, None
    for i in range(len(parts) - 1, -1, -1):
        if i == odds_idx:
            continue
        p = parts[i].lstrip("$")
        if re.match(r'^\d+\.\d+$', p):
            stake_idx, stake = i, round(float(p), 2)
            break
        if re.match(r'^\d+$', p) and 1 <= int(p) <= 50:
            stake_idx, stake = i, float(p)
            break

    if odds is None or stake is None:
        return None

    # ── Parse remaining tokens for teams + bet type
    skip = {odds_idx, stake_idx}
    remaining = [p for i, p in enumerate(parts) if i not in skip]

    teams: list[str] = []
    bet_type = "ML"
    pending_dir: str | None = None  # "O" or "U" waiting for a line number

    for p in remaining:
        code = _team_code(p)
        if code:
            teams.append(code)
            continue
        lower = p.lower()
        if lower in ("ml", "moneyline", "money"):
            bet_type = "ML"
        elif lower in ("f5", "first5", "first"):
            bet_type = "F5"
        elif lower in ("over", "o"):
            pending_dir = "O"
        elif lower in ("under", "u"):
            pending_dir = "U"
        elif re.match(r'^\d+(\.\d+)?$', p):
            if pending_dir:
                bet_type = f"{pending_dir}{p}"
                pending_dir = None
            # else: mystery number, ignore

    if not teams:
        return None

    team = teams[0]
    game = f"{teams[0]}@{teams[1]}" if len(teams) >= 2 else team

    return {"team": team, "game": game, "type": bet_type, "odds": odds, "stake": stake}


# ── SETTLE PARSER ─────────────────────────────────────────────────────────────

def parse_settle(text: str) -> tuple[str, str] | None:
    """
    Return (result_code, identifier) where result_code in W/L/P.
    Handles: win SF, won SF, SF won, loss 3, lost BAL, push TEX, etc.
    """
    lower = text.lower().strip()

    result: str | None = None
    if re.search(r'\b(win|won)\b', lower):
        result = "W"
    elif re.search(r'\b(loss|lose|lost)\b', lower):
        result = "L"
    elif re.search(r'\b(push|tie|void|refund)\b', lower):
        result = "P"

    if not result:
        return None

    # Prefer numeric ID
    num = re.search(r'\b(\d+)\b', text)
    if num:
        return result, num.group(1)

    # Find team code
    for word in text.upper().split():
        if word in MLB_CODES:
            return result, word

    # Try alias in lower text
    for alias, code in _ALIASES.items():
        if alias in lower:
            return result, code

    return None


# ── BET LOOKUP ────────────────────────────────────────────────────────────────

def _find_pending(identifier: str) -> dict | None:
    """Find a pending bet by numeric ID or team code."""
    pending = [b for b in _db.get_bets() if not b.get("result")]
    if not pending:
        return None

    if identifier.isdigit():
        bid = int(identifier)
        return next((b for b in pending if b["id"] == bid), None)

    upper = identifier.upper()
    matches = [b for b in pending if b["bet"].upper() == upper or upper in b["bet"].upper()]
    return matches[-1] if matches else None


# ── COMMAND HANDLERS ─────────────────────────────────────────────────────────

def handle_bet(parsed: dict) -> str:
    today  = date.today().isoformat()
    to_win = _to_win(parsed["stake"], parsed["odds"])

    _db.log_bet(
        date=today,
        bet=parsed["team"],
        bet_type=parsed["type"],
        game=parsed["game"],
        sp="", park="", umpire="",
        bet_odds=parsed["odds"],
        model_prob=None, market_prob=None, edge_pct=None,
        conviction="MANUAL",
        stake=parsed["stake"],
    )
    sync_scout_json()

    bd = _bankroll_display()
    label = f"{parsed['team']} {parsed['type']} {parsed['odds']}"
    return (
        f"✅ Logged — {label} ${parsed['stake']:.2f}"
        f" | To win: ${to_win:.2f}"
        f" | Bankroll: ${bd['bankroll']:.2f} (${bd['pending_risk']:.2f} at risk)"
    )


def handle_settle(result: str, identifier: str) -> str:
    bet = _find_pending(identifier)
    if not bet:
        return f"❓ No pending bet found for '{identifier}' — try 'bets' to see IDs"

    # Snapshot P&L before resolving
    stake   = float(bet.get("stake") or 0)
    to_win  = _to_win(stake, str(bet.get("bet_odds", "")))
    pnl_str = f"+${to_win:.2f}" if result == "W" else (f"-${stake:.2f}" if result == "L" else "$0.00")

    # Fetch closing odds for CLV
    closing = _fetch_closing_odds(bet["bet"], bet.get("type", "ML"))

    _db.resolve_bet(
        bet=bet["bet"],
        date=bet["date"],
        closing_odds=closing or "",
        result=result,
        game_score="",
    )
    sync_scout_json()

    bd = _bankroll_display()
    label   = f"{bet['bet']} {bet.get('type','ML')}"
    emoji   = {"W": "✅", "L": "❌", "P": "\U0001f504"}
    em      = "✅" if result == "W" else ("❌" if result == "L" else "\U0001f504")
    r_label = {"W": "WIN", "L": "LOSS", "P": "PUSH"}[result]

    clv_str = ""
    if closing:
        clv = calc_clv(str(bet.get("bet_odds", "")), closing)
        if clv.get("clv_pct") is not None:
            sign = "+" if clv["clv_pct"] >= 0 else ""
            clv_str = f" | CLV: {sign}{clv['clv_pct']:.1f}% ({clv['verdict']})"

    return (
        f"{em} {label} {r_label} — {pnl_str}{clv_str}"
        f" | Bankroll: ${bd['bankroll']:.2f}"
    )


def handle_bankroll() -> str:
    bd = _bankroll_display()
    sign = "+" if bd["today_pnl"] >= 0 else ""
    return (
        f"\U0001f4b0 Bankroll: ${bd['bankroll']:.2f}\n"
        f"Today: {sign}${bd['today_pnl']:.2f} | Record: {bd['today_record']}\n"
        f"At risk: ${bd['pending_risk']:.2f} across {bd['pending_count']} pending bets"
    )


def handle_bets() -> str:
    pending = [b for b in _db.get_bets() if not b.get("result")]
    if not pending:
        return "No pending bets."
    lines = ["\U0001f4cb Pending bets:"]
    for b in pending:
        stake  = float(b.get("stake") or 0)
        to_win = _to_win(stake, str(b.get("bet_odds", "")))
        lines.append(
            f"  [{b['id']}] {b['bet']} {b.get('type','ML')} {b.get('bet_odds','')} "
            f"${stake:.2f} → +${to_win:.2f}"
        )
    return "\n".join(lines)


def handle_results() -> str:
    today   = date.today().isoformat()
    today_r = [
        b for b in _db.get_bets()
        if b.get("result") in ("W", "L", "P") and b.get("date") == today
    ]
    if not today_r:
        return f"No settled bets on {today}."
    lines = [f"\U0001f4ca Results ({today}):"]
    pnl   = 0.0
    for b in today_r:
        stake = float(b.get("stake") or 0)
        if b["result"] == "W":
            tw = _to_win(stake, str(b.get("bet_odds", "")))
            lines.append(f"  ✅ [{b['id']}] {b['bet']} {b.get('type','ML')} {b.get('bet_odds','')} +${tw:.2f}")
            pnl += tw
        elif b["result"] == "L":
            lines.append(f"  ❌ [{b['id']}] {b['bet']} {b.get('type','ML')} {b.get('bet_odds','')} -${stake:.2f}")
            pnl -= stake
        else:
            lines.append(f"  \U0001f504 [{b['id']}] {b['bet']} {b.get('type','ML')} push")
    sign = "+" if pnl >= 0 else ""
    bd   = _bankroll_display()
    lines.append(f"P&L: {sign}${pnl:.2f} | Bankroll: ${bd['bankroll']:.2f}")
    return "\n".join(lines)


HELP_TEXT = (
    "PARLAY OS Bot\n"
    "━━━━━━━━━━━━━━\n"
    "bet SF ML +162 5.32\n"
    "bet TB TOR over 6.5 -115 3.55\n"
    "win SF  |  won SF  |  SF won\n"
    "loss BAL  |  lost 3  |  BAL lost\n"
    "push TEX\n"
    "bankroll  |  bets  |  results"
)


# ── DISPATCHER ────────────────────────────────────────────────────────────────

def dispatch(text: str) -> None:
    """Route incoming message to the right handler and send reply."""
    t = text.strip()
    if not t:
        return

    lower = t.lower()

    # BET LOGGING
    if lower.startswith("bet "):
        parsed = parse_bet(t)
        _send(handle_bet(parsed) if parsed else
              "❓ Couldn't parse bet. Try: bet SF ML +162 5.32")
        return

    # SETTLE
    settle = parse_settle(t)
    if settle:
        result, identifier = settle
        _send(handle_settle(result, identifier))
        return

    # INFO
    if re.match(r'^(bankroll|br)$', lower):
        _send(handle_bankroll())
        return
    if re.match(r'^(bets|pending)$', lower):
        _send(handle_bets())
        return
    if re.match(r'^(results|today)$', lower):
        _send(handle_results())
        return
    if lower in ("help", "/help", "/start"):
        _send(HELP_TEXT)
        return


# ── TELEGRAM I/O ─────────────────────────────────────────────────────────────

def _send(msg: str) -> None:
    if not BOT_TOKEN:
        print(f"[TG OUT] {msg}")
        return
    if not CHAT_ID:
        print(f"[TG OUT] {msg}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg},
            timeout=8,
        )
    except Exception as e:
        print(f"[TG] send error: {e}")


def _poll_loop() -> None:
    """Long-poll Telegram getUpdates and dispatch commands."""
    if not BOT_TOKEN:
        print("[TG] BOT_TOKEN not set — listener inactive")
        return

    offset = 0
    print("[TG] Listener started")

    while True:
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params={"timeout": 30, "offset": offset},
                timeout=35,
            )
            for update in r.json().get("result", []):
                offset = update["update_id"] + 1
                msg = update.get("message") or update.get("edited_message")
                if not msg:
                    continue
                # Only accept from authorised chat
                if CHAT_ID and str(msg.get("chat", {}).get("id", "")) != str(CHAT_ID):
                    continue
                text = msg.get("text", "")
                if text:
                    dispatch(text)
        except Exception as e:
            print(f"[TG] poll error: {e}")
            time.sleep(5)


def start_listener() -> threading.Thread:
    """Start Telegram bot as a daemon background thread. Returns the thread."""
    t = threading.Thread(target=_poll_loop, daemon=True, name="telegram-bot")
    t.start()
    return t


# Run standalone as a persistent bot
if __name__ == "__main__":
    print("Starting Parlay OS Telegram bot (persistent mode)...")
    _poll_loop()  # block forever
