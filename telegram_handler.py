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
from api_client import get as _http_get
from datetime import date, datetime
import pytz

import db as _db
from math_engine import american_to_decimal, calc_clv, STARTING_BANKROLL

BOT_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID      = os.getenv("TELEGRAM_CHAT_ID", "")
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
ET           = pytz.timezone("America/New_York")

_LISTENER_LOCK = "/tmp/parlay_os_tg.lock"


def _acquire_listener_lock() -> bool:
    """Return True if we got the lock; False if another listener is already running."""
    try:
        if os.path.exists(_LISTENER_LOCK):
            with open(_LISTENER_LOCK) as _f:
                pid = int(_f.read().strip())
            try:
                os.kill(pid, 0)  # signal 0 = existence check only
                print(f"[TG] Listener already running (PID {pid}) — aborting to avoid 409 conflict")
                return False
            except (ProcessLookupError, PermissionError):
                pass  # stale lock — previous process is dead
        with open(_LISTENER_LOCK, "w") as _f:
            _f.write(str(os.getpid()))
        return True
    except Exception as e:
        print(f"[TG] Lock file error: {e} — proceeding anyway")
        return True


def _release_listener_lock() -> None:
    try:
        if os.path.exists(_LISTENER_LOCK):
            with open(_LISTENER_LOCK) as _f:
                pid = int(_f.read().strip())
            if pid == os.getpid():
                os.remove(_LISTENER_LOCK)
    except Exception:
        pass

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
        r = _http_get(
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
    bd   = _bankroll_display()
    sign = "+" if bd["today_pnl"] >= 0 else ""

    bets     = _db.get_bets()
    resolved = [b for b in bets if b.get("result") in ("W", "L", "P")]
    clv_vals = [b["clv_pct"] for b in resolved if b.get("clv_pct") is not None]
    clv_line = ""
    if clv_vals:
        avg_clv   = round(sum(clv_vals) / len(clv_vals), 2)
        pos_pct   = round(sum(1 for v in clv_vals if v > 0) / len(clv_vals) * 100, 0)
        clv_sign  = "+" if avg_clv >= 0 else ""
        verdict   = "SHARP" if avg_clv > 4 else "+EV" if avg_clv > 1 else "NEUTRAL" if avg_clv > -1 else "-EV"
        clv_line  = (
            f"\nCLV: {clv_sign}{avg_clv:.2f}% avg | {pos_pct:.0f}% positive | {verdict}"
            f" ({len(clv_vals)} of {len(resolved)} settled)"
        )

    return (
        f"\U0001f4b0 Bankroll: ${bd['bankroll']:.2f}\n"
        f"Today: {sign}${bd['today_pnl']:.2f} | Record: {bd['today_record']}\n"
        f"At risk: ${bd['pending_risk']:.2f} across {bd['pending_count']} pending bets"
        + clv_line
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


def handle_update(identifier: str, new_stake_str: str) -> str:
    try:
        new_stake = round(float(new_stake_str.lstrip("$")), 2)
    except ValueError:
        return "❓ Invalid stake. Try: /update 3 6.50"
    if new_stake <= 0:
        return "❓ Stake must be positive."

    bet = _find_pending(identifier)
    if not bet:
        return f"❓ No pending bet found for '{identifier}' — try /bets to see IDs"

    _db.update_bet_stake(bet["id"], new_stake)
    sync_scout_json()

    to_win = _to_win(new_stake, str(bet.get("bet_odds", "")))
    bd     = _bankroll_display()
    return (
        f"✏️ Updated — [{bet['id']}] {bet['bet']} {bet.get('type','ML')} "
        f"{bet.get('bet_odds','')} ${new_stake:.2f} | To win: ${to_win:.2f}"
        f" | Bankroll: ${bd['bankroll']:.2f} (${bd['pending_risk']:.2f} at risk)"
    )


HELP_TEXT = (
    "PARLAY OS Commands\n"
    "━━━━━━━━━━━━━━━━━━\n"
    "BET MANAGEMENT:\n"
    "/bet [team] [type] [odds] [stake]\n"
    "  e.g. /bet SF ML +162 5.32\n"
    "  e.g. /bet TB TOR over 6.5 -115 3.55\n"
    "  e.g. /bet NYY F5 -130 4.00\n"
    "/update [id] [stake]  — edit pending bet stake\n"
    "  e.g. /update 3 6.50\n"
    "/win [id or team]     — settle as win  (e.g. /win SF or /win 3)\n"
    "/loss [id or team]    — settle as loss (e.g. /loss 3)\n"
    "/push [id or team]    — settle as push (e.g. /push TEX)\n"
    "/bets                 — list all pending bets with IDs\n"
    "/bankroll             — current balance + today P&L\n"
    "/results              — today's settled bets\n"
    "\nINFO & ANALYSIS:\n"
    "/preview              — today's full game previews (pre-1pm)\n"
    "/props                — today's props slate (K, NRFI, totals, SGP)\n"
    "/edges                — current Polymarket vs sportsbook gaps\n"
    "/live                 — active live game alerts (6pm–11pm ET)\n"
    "/record               — all-time track record + ROI\n"
    "/share                — today's picks formatted for Twitter + Discord\n"
    "/fade [team] [reason] — log a manual fade\n"
    "  e.g. /fade LAD public overreaction\n"
    "\nSYSTEM:\n"
    "/status               — system health check\n"
    "/resetcap             — delete all pending bets for today (resets daily cap)\n"
    "/help                 — this message\n"
    "\nSlash prefix is optional — plain text works too.\n"
    "  e.g. 'bet SF ML +162 5.32' or 'win SF' both work."
)


# ── SHARE / FADE HANDLERS ─────────────────────────────────────────────────────

def handle_share() -> str:
    """Return today's picks formatted for Twitter and Discord."""
    today = date.today().isoformat()
    bets  = [b for b in _db.get_bets() if b.get("date") == today]
    if not bets:
        return f"No picks logged for {today}."

    def _fmt_pick(b: dict) -> str:
        conv     = b.get("conviction", "MANUAL") or "MANUAL"
        team     = b.get("bet", "")
        btype    = b.get("type", "ML") or "ML"
        odds     = b.get("bet_odds", "")
        stake    = float(b.get("stake") or 0)
        edge     = b.get("edge_pct")
        result   = b.get("result", "")
        em       = " ✅" if result == "W" else " ❌" if result == "L" else ""
        edge_str = f" | Edge: +{edge:.1f}%" if edge else ""
        tag      = f"[{conv}] " if conv and conv not in ("MANUAL", "") else ""
        return f"{tag}{team} {btype} {odds}{em}{edge_str} | ${stake:.2f}"

    today_label = datetime.now(ET).strftime("%b %d, %Y")
    twitter_lines = [f"PARLAY OS — {today_label}"]
    for b in bets:
        twitter_lines.append(_fmt_pick(b))
    twitter_lines.append("#MLB #BettingPicks")

    discord_lines = [f"**PARLAY OS — {today_label}**", "```"]
    for b in bets:
        discord_lines.append(f"  {_fmt_pick(b)}")
    discord_lines.append("```")

    twitter_text  = "\n".join(twitter_lines)
    discord_text  = "\n".join(discord_lines)
    return f"TWITTER:\n{twitter_text}\n\nDISCORD:\n{discord_text}"


def handle_fade(team_arg: str, reason: str) -> str:
    """Store a manual fade to fades.json."""
    team_code = _team_code(team_arg)
    if not team_code:
        return f"❓ Unknown team '{team_arg}' — try the abbreviation (e.g. SF, LAD, NYY)"

    today = date.today().isoformat()
    entry = {
        "date":      today,
        "team":      team_code,
        "reason":    reason,
        "timestamp": datetime.now(ET).isoformat(),
    }

    try:
        try:
            with open("fades.json") as f:
                fades = json.load(f)
        except Exception:
            fades = []
        fades.append(entry)
        with open("fades.json", "w") as f:
            json.dump(fades, f, indent=2)
    except Exception as e:
        return f"❌ Error saving fade: {e}"

    return f"✅ Fade logged — {team_code}: {reason}"


def handle_preview() -> str:
    """Return today's game previews from last scout."""
    try:
        with open("last_scout.json") as f:
            data = json.load(f)
    except Exception:
        return "No scout data yet — scout runs at 1pm ET."

    today = date.today().isoformat()
    scout_date = data.get("date", "")
    if scout_date and scout_date != today:
        return f"Scout data is from {scout_date} — today's scout hasn't run yet (runs at 1pm ET)."

    games = data.get("games", [])
    if not games:
        return "No games analyzed in today's scout."
    lines      = [f"GAME PREVIEWS — {scout_date}"]
    for g in games[:8]:
        away = g.get("away", "?")
        home = g.get("home", "?")
        ae   = g.get("away_edge", 0)
        he   = g.get("home_edge", 0)
        axr  = g.get("away_xr", 0)
        hxr  = g.get("home_xr", 0)
        lines.append(f"\n{away} @ {home}")
        lines.append(f"  Edge: {away} {ae:+.1f}% / {home} {he:+.1f}%")
        lines.append(f"  xR:   {away} {axr:.2f} / {home} {hxr:.2f}")

    bets = data.get("bets", [])
    if bets:
        lines.append(f"\n📌 {len(bets)} active pick(s) — use /bets to see pending")

    return "\n".join(lines)


def handle_live() -> str:
    """Return active live alerts from live_alerts.json."""
    try:
        with open("live_alerts.json") as f:
            alerts = json.load(f)
    except Exception:
        alerts = []

    if not alerts:
        return "No live alerts active. Live engine runs 6pm–11pm ET."

    lines = [f"🔴 LIVE ALERTS ({len(alerts)}):"]
    for a in alerts[-6:]:
        team  = a.get("team", a.get("away", "?"))
        inning = a.get("inning", "?")
        note  = a.get("note", a.get("reason", ""))
        stake = a.get("stake", 0)
        odds  = a.get("odds", "")
        lines.append(f"  {team} (inn {inning}) {odds} ${stake:.2f} — {note}")

    return "\n".join(lines)


def handle_props() -> str:
    """Return today's props slate from props_output.json."""
    try:
        with open("props_output.json") as f:
            data = json.load(f)
    except Exception:
        return "No props data yet — generated during daily scout at 1pm ET."

    # Support dict format {"date":..., "games":[...]} and legacy list format
    if isinstance(data, dict):
        games     = data.get("games", [])
        file_date = data.get("date", "")
    elif isinstance(data, list):
        games     = data
        file_date = ""
    else:
        return "No props data yet — generated during daily scout at 1pm ET."

    if not games:
        return "No props data yet — generated during daily scout at 1pm ET."

    today = date.today().isoformat()
    if file_date and file_date != today:
        return f"Props data is from {file_date} — today's scout hasn't run yet (runs at 1pm ET)."

    lines = [f"PROPS SLATE — {today}"]
    bets_found = 0

    for game in games[:8]:
        away  = game.get("away") or game.get("away_name", "?")
        home  = game.get("home") or game.get("home_name", "?")
        gtime = game.get("time", "")
        header = f"\n{away} @ {home}"
        if gtime:
            header += f" — {gtime}"
        lines.append(header)

        for p in game.get("props", []):
            ptype = p.get("type", "")
            rec   = str(p.get("recommendation") or "")
            edge  = p.get("edge_pct") or 0

            if ptype == "K_PROP":
                sp_name = p.get("sp", "SP")
                line    = p.get("market_line") or p.get("model_line", 5.0)
                p_over  = float(p.get("p_over") or 0)
                if rec == "BET" or (float(edge) > 0 if edge else False):
                    odds = p.get("market_over_ml") or p.get("model_over_ml")
                    os   = f" {'+' if odds and odds > 0 else ''}{odds}" if odds else ""
                    lines.append(f"✅ {sp_name} O{line}K{os} — EDGE: +{float(edge):.1f}%")
                    bets_found += 1
                else:
                    lines.append(f"❌ {sp_name} O{line}K — no edge ({p_over:.1%})")

            elif ptype == "NRFI":
                p_nrfi = float(p.get("p_nrfi") or 0)
                p_yrfi = float(p.get("p_yrfi") or 0)
                if rec in ("NRFI", "YRFI"):
                    e_val = float(edge) if edge else round((max(p_nrfi, p_yrfi) - 0.50) * 100, 1)
                    lines.append(f"✅ {rec} — EDGE: +{e_val:.1f}%")
                    bets_found += 1
                else:
                    lines.append(f"❌ NRFI ({p_nrfi:.1%}) / YRFI ({p_yrfi:.1%}) — no edge")

            elif ptype == "TOTAL":
                ml  = p.get("market_line")
                has_edge = (ml and rec and rec != "PASS"
                            and ("OVER" in rec or "UNDER" in rec))
                if has_edge:
                    e_val     = float(edge) if edge else 0
                    direction = "OVER" if "OVER" in rec else "UNDER"
                    mk_odds   = p.get("market_over_ml") if direction == "OVER" else p.get("market_under_ml")
                    os        = f" {'+' if mk_odds and mk_odds > 0 else ''}{mk_odds}" if mk_odds else ""
                    lines.append(f"✅ {rec}{os} — EDGE: +{e_val:.1f}%")
                    bets_found += 1
                else:
                    lines.append("❌ Game total — market efficient")

        for sgp in game.get("sgp", []):
            legs  = " + ".join(sgp.get("legs", [])[:3])
            stake = sgp.get("kelly_stake", 0) or 0
            jp    = sgp.get("joint_prob", 0)
            if jp > 0 and stake > 0:
                lines.append(f"✅ SGP: {legs} — ${float(stake):.2f}")
                bets_found += 1

    if bets_found == 0:
        lines.append("\nNo value props found today — market efficient.")

    return "\n".join(lines)


def handle_edges() -> str:
    """Return current market edges / arbitrage log."""
    try:
        with open("arbitrage_log.json") as f:
            edges = json.load(f)
    except Exception:
        edges = []

    if not edges:
        return "No market edges found. Try after 1pm ET when scout runs."

    lines = [f"📊 MARKET EDGES ({len(edges)} found):"]
    for e in edges[:6]:
        game  = e.get("game", e.get("event", "?"))
        desc  = e.get("description", e.get("note", ""))
        epct  = float(e.get("edge_pct", e.get("edge", 0)) or 0)
        lines.append(f"  {game} — {desc} — {epct:+.1f}%")

    return "\n".join(lines)


def handle_record() -> str:
    """Return all-time track record from DB."""
    bets     = _db.get_bets()
    resolved = [b for b in bets if b.get("result") in ("W", "L", "P")]
    if not resolved:
        return "No resolved bets yet — start betting!"

    wins   = sum(1 for b in resolved if b["result"] == "W")
    losses = sum(1 for b in resolved if b["result"] == "L")
    total  = wins + losses

    staked = sum(float(b.get("stake") or 0) for b in resolved if b["result"] != "P")
    pnl    = 0.0
    for b in resolved:
        stake = float(b.get("stake") or 0)
        if b["result"] == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                pnl += (dec - 1) * stake
        elif b["result"] == "L":
            pnl -= stake
    roi  = round(pnl / staked * 100, 1) if staked > 0 else 0.0
    wr   = round(wins / total * 100, 1) if total else 0.0
    sign = "+" if pnl >= 0 else ""

    br   = _current_bankroll()

    return (
        f"📊 ALL-TIME TRACK RECORD\n"
        f"Record: {wins}W-{losses}L ({wr:.1f}%)\n"
        f"P&L: {sign}${pnl:.2f} | ROI: {sign}{roi:.1f}%\n"
        f"Bankroll: ${br:.2f} | Total bets: {len(bets)}"
    )


def handle_status() -> str:
    """Run health check and return system status."""
    try:
        from health_check import run_health_check
        r        = run_health_check(auto_restart=False)
        all_ok   = r.get("all_ok", False)
        failures = r.get("failures", [])
        if all_ok:
            return "✅ All systems operational"
        lines = ["⚠️ Issues detected:"]
        for f in failures:
            lines.append(f"  • {f}")
        return "\n".join(lines)
    except Exception as e:
        return f"❌ Health check error: {e}"


# ── DISPATCHER ────────────────────────────────────────────────────────────────

def handle_resetcap() -> str:
    """Delete all pending bets logged today and confirm."""
    today   = date.today().isoformat()
    deleted = _db.reset_daily_exposure(today)
    if deleted == 0:
        return f"✅ /resetcap — no pending bets found for {today}. Daily cap already clear."
    return (
        f"✅ /resetcap — {deleted} pending bet{'s' if deleted != 1 else ''} deleted for {today}.\n"
        f"Daily exposure is now $0. Run the scout again to re-log today's picks."
    )


def dispatch(text: str) -> None:
    """Route incoming message to the right handler.
    Accepts both /command and plain-text command — slash is stripped first."""
    t = text.strip()
    if not t:
        return

    # Normalise: strip leading slash so /bet == bet, /win == win, etc.
    if t.startswith("/"):
        t = t[1:]

    lower = t.lower()

    # /bet [team] [type] [odds] [stake]
    if lower.startswith("bet "):
        parsed = parse_bet(t)   # t already starts with "bet " (slash stripped above)
        _send(handle_bet(parsed) if parsed else
              "❓ Couldn't parse. Try: /bet SF ML +162 5.32")
        return

    # /update [id] [stake]
    if lower.startswith("update "):
        parts = t.split()
        if len(parts) >= 3:
            _send(handle_update(parts[1], parts[2]))
        else:
            _send("❓ Try: /update [id] [stake] — e.g. /update 3 6.50")
        return

    # /win, /loss, /push — explicit slash settle commands
    # Accept: /win PIT, /win 3, /win PIT $6.30, /loss PHI $5.40
    # Dollar amount after identifier is optional confirmation info — strip it.
    for cmd, result_code in (("win ", "W"), ("loss ", "L"), ("push ", "P")):
        if lower.startswith(cmd):
            rest = t[len(cmd):].strip()
            if rest:
                # First whitespace-separated token is the team code or numeric ID
                identifier = rest.split()[0].lstrip("#")
                _send(handle_settle(result_code, identifier))
            else:
                _send(f"❓ Try: /{cmd.strip()} [id or team] — e.g. /{cmd.strip()} SF or /{cmd.strip()} SF $6.30")
            return

    # Natural-language settle ("won SF", "SF lost", etc.)
    settle = parse_settle(t)
    if settle:
        result_code, identifier = settle
        _send(handle_settle(result_code, identifier))
        return

    # /share — today's picks in Twitter + Discord format
    if lower == "share":
        _send(handle_share())
        return

    # /fade [TEAM] [REASON]
    if lower.startswith("fade "):
        parts = t.split(None, 2)  # ["fade", "TEAM", "reason..."]
        if len(parts) >= 2:
            team_arg = parts[1]
            reason   = parts[2] if len(parts) >= 3 else "manual fade"
            _send(handle_fade(team_arg, reason))
        else:
            _send("❓ Try: /fade [TEAM] [REASON] — e.g. /fade SF public overreaction")
        return

    # /preview — today's game previews
    if lower == "preview":
        _send(handle_preview())
        return

    # /live — active live alerts
    if lower == "live":
        _send(handle_live())
        return

    # /props — today's props slate
    if lower == "props":
        _send(handle_props())
        return

    # /edges — market edges
    if lower == "edges":
        _send(handle_edges())
        return

    # /record — all-time track record
    if lower == "record":
        _send(handle_record())
        return

    # /status — system health check
    if lower == "status":
        _send(handle_status())
        return

    # /resetcap — clear all pending bets for today
    if lower == "resetcap":
        _send(handle_resetcap())
        return

    # /bets  /bankroll  /results  /help
    if re.match(r'^(bets|pending)$', lower):
        _send(handle_bets())
        return
    if re.match(r'^(bankroll|br)$', lower):
        _send(handle_bankroll())
        return
    if re.match(r'^(results|today)$', lower):
        _send(handle_results())
        return
    if lower in ("help", "start"):
        _send(HELP_TEXT)
        return


# ── AUTO-SETTLEMENT ──────────────────────────────────────────────────────────
# Runs every 30 minutes from 9 pm – 1 am ET.
# Pulls final scores from MLB Stats API, matches pending bets, settles each one,
# fetches closing line for CLV, and pushes a Telegram confirmation.

STATSAPI = "https://statsapi.mlb.com/api/v1"

# Games are considered final when status is one of these
_FINAL_STATES = {"Final", "Game Over", "Completed Early", "Completed"}


def _fetch_final_games(game_date: str) -> list[dict]:
    """Return all completed MLB games for game_date with linescore data."""
    try:
        r = _http_get(
            f"{STATSAPI}/schedule",
            params={"sportId": 1, "date": game_date, "hydrate": "linescore,team"},
            timeout=12,
        )
        r.raise_for_status()
        out = []
        for gd in r.json().get("dates", []):
            for g in gd.get("games", []):
                if g.get("status", {}).get("detailedState", "") in _FINAL_STATES:
                    out.append(g)
        return out
    except Exception as e:
        print(f"[AUTO] schedule fetch error: {e}")
        return []


def _game_side(game: dict, team_code: str) -> str | None:
    """Return 'away' or 'home' if team_code plays in this game, else None."""
    from constants import MLB_TEAM_IDS, MLB_TEAM_NAMES
    tid   = MLB_TEAM_IDS.get(team_code)
    names = [n.lower() for n in MLB_TEAM_NAMES.get(team_code, [team_code])]
    for side in ("away", "home"):
        t = game.get("teams", {}).get(side, {}).get("team", {})
        if tid and t.get("id") == tid:
            return side
        if any(n in t.get("name", "").lower() for n in names):
            return side
    return None


def _f5_runs(game_pk: int, game: dict) -> tuple[int, int]:
    """Return (away_f5, home_f5) — sum of innings 1-5.
    Prefers inline linescore from hydrated schedule; falls back to API call."""
    innings = (game.get("linescore") or {}).get("innings", [])
    if not innings:
        try:
            r = _http_get(f"{STATSAPI}/game/{game_pk}/linescore", timeout=8)
            innings = r.json().get("innings", [])
        except Exception:
            pass
    first5  = [inn for inn in innings if 1 <= int(inn.get("num", 0)) <= 5]
    away_r  = sum(int(i.get("away", {}).get("runs") or 0) for i in first5)
    home_r  = sum(int(i.get("home", {}).get("runs") or 0) for i in first5)
    return away_r, home_r


def _determine_outcome(bet: dict, game: dict, side: str) -> str | None:
    """
    W/L/P based on bet type and final score. Returns None if undetermined.
    Handles: ML, F5, O{line}, U{line}
    """
    bet_type = (bet.get("type") or "ML").strip().upper()
    teams    = game.get("teams", {})
    opp_side = "home" if side == "away" else "away"
    game_pk  = game.get("gamePk")

    # Use isWinner when available (handles walk-offs, extra innings cleanly)
    our_winner = teams.get(side, {}).get("isWinner")
    opp_winner = teams.get(opp_side, {}).get("isWinner")
    our_score  = int(teams.get(side, {}).get("score") or 0)
    opp_score  = int(teams.get(opp_side, {}).get("score") or 0)

    if bet_type in ("ML", "MONEYLINE"):
        if our_winner is not None:
            return "W" if our_winner else "L"
        if our_score > opp_score:  return "W"
        if our_score < opp_score:  return "L"
        return "P"

    if bet_type == "F5":
        if game_pk:
            away_f5, home_f5 = _f5_runs(game_pk, game)
            our_f5  = away_f5 if side == "away" else home_f5
            opp_f5  = home_f5 if side == "away" else away_f5
        else:
            return None
        if our_f5 > opp_f5:  return "W"
        if our_f5 < opp_f5:  return "L"
        return "P"

    # Over / Under — e.g. "O8.5", "U7.5"
    if bet_type and bet_type[0] in ("O", "U"):
        try:
            line = float(bet_type[1:])
        except ValueError:
            return None
        away_r = int(teams.get("away", {}).get("score") or 0)
        home_r = int(teams.get("home", {}).get("score") or 0)
        total  = away_r + home_r
        if bet_type[0] == "O":
            if total > line:  return "W"
            if total < line:  return "L"
            return "P"
        else:
            if total < line:  return "W"
            if total > line:  return "L"
            return "P"

    return None


def _score_str(game: dict) -> str:
    teams  = game.get("teams", {})
    away_t = teams.get("away", {}).get("team", {}).get("abbreviation", "?")
    home_t = teams.get("home", {}).get("team", {}).get("abbreviation", "?")
    away_r = teams.get("away", {}).get("score", "?")
    home_r = teams.get("home", {}).get("score", "?")
    return f"{away_t} {away_r}-{home_r} {home_t}"


def _clv_str(bet_odds: str, closing: str) -> str:
    if not closing:
        return ""
    clv = calc_clv(str(bet_odds), closing)
    if clv.get("clv_pct") is None:
        return ""
    sign = "+" if clv["clv_pct"] >= 0 else ""
    return f" | CLV {sign}{clv['clv_pct']:.1f}% ({clv['verdict']})"


def _update_clv_log(settled: list[dict]) -> None:
    """Append settled bets to clv_log.json for dashboard CLV tab."""
    try:
        try:
            with open("clv_log.json") as f:
                log = json.load(f)
        except Exception:
            log = []
        for b in settled:
            log.append({
                "date":         b.get("date"),
                "bet":          b.get("bet"),
                "type":         b.get("type"),
                "game":         b.get("game"),
                "bet_odds":     b.get("bet_odds"),
                "closing_odds": b.get("closing"),
                "clv_pct":      b.get("clv_pct"),
                "result":       b.get("outcome"),
                "stake":        b.get("stake"),
                "settled_at":   datetime.now(ET).isoformat(),
            })
        with open("clv_log.json", "w") as f:
            json.dump(log, f, indent=2)
    except Exception as e:
        print(f"[AUTO] clv_log update error: {e}")


def run_settlement_check() -> list[dict]:
    """
    Core auto-settlement function.
    Checks every pending bet. For any whose game is final, determines
    outcome, fetches closing odds, settles in DB, and sends notification.
    Returns list of dicts describing what was settled.
    """
    pending = [b for b in _db.get_bets() if not b.get("result")]
    if not pending:
        return []

    # Group by date — fetch schedule once per date
    by_date: dict[str, list] = {}
    for b in pending:
        d = b.get("date") or date.today().isoformat()
        by_date.setdefault(d, []).append(b)

    settled_log = []

    for game_date, bets in by_date.items():
        games = _fetch_final_games(game_date)
        if not games:
            continue

        for bet in bets:
            team_code = bet["bet"]

            # Find matching final game
            matched_game = None
            matched_side = None
            for g in games:
                s = _game_side(g, team_code)
                if s:
                    matched_game = g
                    matched_side = s
                    break

            if matched_game is None:
                continue  # game not over yet

            outcome = _determine_outcome(bet, matched_game, matched_side)
            if outcome is None:
                print(f"[AUTO] couldn't determine outcome for {team_code} {bet.get('type')}")
                continue

            # Closing line for CLV
            closing = _fetch_closing_odds(team_code, bet.get("type", "ML"))

            score   = _score_str(matched_game)
            clv_txt = _clv_str(str(bet.get("bet_odds", "")), closing or "")

            # Settle by ID for precision
            _db.resolve_bet_by_id(
                bet_id=bet["id"],
                closing_odds=closing or "",
                result=outcome,
                game_score=score,
            )

            # P&L for message
            stake   = float(bet.get("stake") or 0)
            to_win  = _to_win(stake, str(bet.get("bet_odds", "")))
            pnl_str = (f"+${to_win:.2f}" if outcome == "W"
                       else f"-${stake:.2f}" if outcome == "L"
                       else "$0.00")
            em      = "✅" if outcome == "W" else "❌" if outcome == "L" else "🔄"
            r_lab   = {"W": "WIN", "L": "LOSS", "P": "PUSH"}[outcome]
            bd      = _bankroll_display()

            msg = (
                f"{em} AUTO-SETTLE: {team_code} {bet.get('type','ML')} {r_lab}\n"
                f"{score} | {pnl_str}{clv_txt}\n"
                f"Bankroll: ${bd['bankroll']:.2f}"
            )
            _send(msg)
            print(f"[AUTO] settled bet #{bet['id']}: {team_code} {outcome} {score}")

            settled_log.append({
                **bet,
                "outcome":  outcome,
                "score":    score,
                "closing":  closing,
                "clv_pct":  calc_clv(str(bet.get("bet_odds","")), closing).get("clv_pct") if closing else None,
            })

    if settled_log:
        sync_scout_json()
        _update_clv_log(settled_log)

    return settled_log


def _in_settlement_window() -> bool:
    """True between 4 pm and 1 am ET — covers afternoon double-headers through late games."""
    hour = datetime.now(ET).hour
    return hour >= 16 or hour < 1


def _next_window_sleep() -> int:
    """Seconds until 4 pm ET if we're outside the window."""
    now    = datetime.now(ET)
    hour   = now.hour
    minute = now.minute
    if hour >= 16 or hour < 1:
        return 30 * 60  # already in window — standard 30-min interval
    # Calculate seconds until 4 pm ET
    target_hour   = 16
    minutes_left  = (target_hour - hour) * 60 - minute
    return max(minutes_left * 60, 60)


def _settler_loop() -> None:
    """Background daemon: run settlement check every 30 min, 4 pm–1 am ET."""
    print("[AUTO] Settlement loop started")
    # Immediate check on startup so bets from earlier in the day are settled right away
    if _in_settlement_window():
        try:
            settled = run_settlement_check()
            if settled:
                print(f"[AUTO] Startup check: settled {len(settled)} bet(s)")
        except Exception as e:
            print(f"[AUTO] startup check error: {e}")
    while True:
        sleep_secs = _next_window_sleep()
        time.sleep(sleep_secs)
        if _in_settlement_window():
            try:
                settled = run_settlement_check()
                if settled:
                    print(f"[AUTO] Settled {len(settled)} bet(s)")
                else:
                    print("[AUTO] Check complete — no new settlements")
            except Exception as e:
                print(f"[AUTO] settlement error: {e}")


def start_auto_settler() -> threading.Thread:
    """Start auto-settlement as a daemon background thread."""
    t = threading.Thread(target=_settler_loop, daemon=True, name="auto-settler")
    t.start()
    return t


# ── HEDGE MONITOR ────────────────────────────────────────────────────────────

def _fetch_event_ml_odds(team_code: str) -> tuple[str | None, str | None]:
    """
    Fetch current ML odds for team_code and its opponent from the-odds-api.
    Returns (our_odds, opp_odds) as American strings, or (None, None).
    """
    if not ODDS_API_KEY:
        return None, None
    from constants import MLB_TEAM_NAMES
    names = MLB_TEAM_NAMES.get(team_code, [team_code])
    try:
        r = _http_get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds",
            params={
                "apiKey":    ODDS_API_KEY,
                "regions":   "us",
                "markets":   "h2h",
                "oddsFormat": "american",
                "bookmakers": "pinnacle,draftkings",
            },
            timeout=10,
        )
        r.raise_for_status()
        for event in r.json():
            for bk in event.get("bookmakers", []):
                for mkt in bk.get("markets", []):
                    if mkt.get("key") != "h2h":
                        continue
                    outcomes = mkt.get("outcomes", [])
                    if len(outcomes) < 2:
                        continue
                    our_idx = None
                    for i, oc in enumerate(outcomes):
                        if any(n.lower() in oc.get("name", "").lower() for n in names):
                            our_idx = i
                            break
                    if our_idx is not None:
                        opp_idx = 1 - our_idx  # works for 2-outcome h2h
                        return str(outcomes[our_idx]["price"]), str(outcomes[opp_idx]["price"])
    except Exception:
        pass
    return None, None


def check_hedge_opportunities() -> list[dict]:
    """
    Check all pending bets for full hedge, partial hedge, or middle opportunities.
    Sends Telegram alert for each and returns list of opportunity dicts.

    Triggers:
    - Odds moved 15+ points in our favor (orig_int - curr_int >= 15)
    - Team was underdog (orig > 0), now a 150+ favorite
    Middle window: line moved 10+ points (both bets could cash).
    """
    pending = [b for b in _db.get_bets() if not b.get("result")]
    if not pending:
        return []

    opportunities = []

    for bet in pending:
        team_code = bet["bet"]
        orig_odds = str(bet.get("bet_odds", ""))
        stake     = float(bet.get("stake") or 0)
        if not orig_odds or stake <= 0:
            continue

        try:
            orig_int = int(orig_odds.replace("+", ""))
        except ValueError:
            continue

        orig_dec = american_to_decimal(orig_odds)
        if not orig_dec or orig_dec <= 1:
            continue

        to_return = round(stake * orig_dec, 2)

        curr_odds_str, opp_odds_str = _fetch_event_ml_odds(team_code)
        if not curr_odds_str or not opp_odds_str:
            continue

        try:
            curr_int = int(curr_odds_str.replace("+", ""))
        except ValueError:
            continue

        curr_dec = american_to_decimal(curr_odds_str)
        opp_dec  = american_to_decimal(opp_odds_str)
        if not curr_dec or not opp_dec or curr_dec <= 1 or opp_dec <= 1:
            continue

        # Positive shift = team became more favored (moved in our favor)
        odds_shift   = orig_int - curr_int
        was_underdog = orig_int > 0
        now_big_fav  = curr_int < -100 and abs(curr_int) >= 150

        if odds_shift < 15 and not (was_underdog and now_big_fav):
            continue

        # ── Hedge math ────────────────────────────────────────────────────────
        hedge_stake   = round(to_return / opp_dec, 2)
        locked_profit = round(to_return - stake - hedge_stake, 2)

        partial_stake           = round(hedge_stake * 0.5, 2)
        partial_win_orig        = round(to_return - stake - partial_stake, 2)
        partial_win_opp         = round(partial_stake * (opp_dec - 1) - stake, 2)

        has_middle = abs(odds_shift) >= 10

        # Recommendation
        if locked_profit >= stake * 0.20:
            recommendation = "FULL HEDGE"
            rec_reason     = f"Lock ${locked_profit:.2f} profit guaranteed"
        elif locked_profit > 0:
            recommendation = "PARTIAL HEDGE"
            rec_reason     = f"Reduce variance — floor ~${round(locked_profit * 0.5, 2):.2f}"
        else:
            recommendation = "LET IT RIDE"
            rec_reason     = "Hedge not profitable at current odds"

        # Format opponent odds for display
        opp_int = int(opp_odds_str.replace("+", ""))
        opp_disp = (f"+{opp_int}" if opp_int > 0 else str(opp_int))

        # Parse opponent team from game string
        game_str = bet.get("game", team_code)
        opp_parts = [p for p in game_str.replace(" @ ", "@").split("@") if p != team_code]
        opp_team  = opp_parts[0].strip() if opp_parts else "OPP"

        full_win        = round(to_return - stake, 2)
        curr_disp       = curr_odds_str if curr_odds_str.startswith(("-", "+")) else f"+{curr_odds_str}"
        partial_loss_opp = round(abs(partial_win_opp), 2) if partial_win_opp < 0 else None

        lines = [
            "💰 HEDGE OPPORTUNITY",
            f"Original: {team_code} {bet.get('type','ML')} {orig_odds} ${stake:.2f} — to win ${full_win:.2f}",
            f"Current situation: {team_code} now {curr_disp} (moved {odds_shift:+d} pts)",
            "",
            f"FULL HEDGE: Bet {opp_team} {opp_disp} ${hedge_stake:.2f}",
            f"  → If original wins: +${locked_profit:.2f}",
            f"  → If hedge wins:    +${locked_profit:.2f}",
            f"  → Guaranteed profit: ${locked_profit:.2f}",
            "",
            f"PARTIAL HEDGE (50%): Bet {opp_team} ${partial_stake:.2f}",
            f"  → If original wins: +${partial_win_orig:.2f}",
        ]
        if partial_loss_opp is not None:
            lines.append(f"  → If hedge wins:   +${round(partial_win_opp + stake, 2):.2f} or -${partial_loss_opp:.2f}")
        else:
            lines.append(f"  → If hedge wins:   +${partial_win_opp:.2f}")
        lines += [
            "",
            f"LET IT RIDE: Original to win ${full_win:.2f} or lose ${stake:.2f}",
            "",
        ]
        if has_middle:
            middle_total = round(stake + hedge_stake, 2)
            worst_case   = min(locked_profit, partial_win_opp)
            lines += [
                f"⚡ MIDDLE WINDOW: line moved {abs(odds_shift)} pts",
                f"  Both stakes: ${stake:.2f} (orig) + ${hedge_stake:.2f} (hedge) = ${middle_total:.2f}",
                f"  If original wins: +${locked_profit:.2f}",
                f"  If hedge wins:    +${locked_profit:.2f}",
                f"  Worst case: ${worst_case:.2f}",
                "",
            ]
        lines.append(f"RECOMMENDED: {recommendation} — {rec_reason}")

        _send("\n".join(lines))

        opportunities.append({
            "bet_id":        bet["id"],
            "team":          team_code,
            "orig_odds":     orig_odds,
            "curr_odds":     curr_odds_str,
            "stake":         stake,
            "to_return":     to_return,
            "hedge_stake":   hedge_stake,
            "locked_profit": locked_profit,
            "recommendation": recommendation,
            "has_middle":    has_middle,
        })

    return opportunities


def _in_game_hours() -> bool:
    """True between noon and midnight ET — covers all MLB game windows."""
    hour = datetime.now(ET).hour
    return 12 <= hour < 24


def _hedge_loop() -> None:
    """Background daemon: run hedge check every 30 min during game hours (noon–midnight ET)."""
    print("[HEDGE] Monitor started")
    while True:
        time.sleep(30 * 60)
        if _in_game_hours():
            try:
                opps = check_hedge_opportunities()
                if opps:
                    print(f"[HEDGE] {len(opps)} opportunity/ies found")
                else:
                    print("[HEDGE] Check complete — no opportunities")
            except Exception as e:
                print(f"[HEDGE] error: {e}")


def start_hedge_monitor() -> threading.Thread:
    """Start hedge monitor as a daemon background thread."""
    t = threading.Thread(target=_hedge_loop, daemon=True, name="hedge-monitor")
    t.start()
    return t


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

    if not _acquire_listener_lock():
        return

    offset = 0
    print("[TG] Listener started")

    try:
        while True:
            try:
                r = _http_get(
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
    finally:
        _release_listener_lock()


def start_listener() -> threading.Thread:
    """Start Telegram bot as a daemon background thread. Returns the thread."""
    t = threading.Thread(target=_poll_loop, daemon=True, name="telegram-bot")
    t.start()
    return t


# Run standalone as a persistent bot
if __name__ == "__main__":
    print("Starting Parlay OS Telegram bot (persistent mode)...")
    _poll_loop()  # block forever
