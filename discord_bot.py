"""PARLAY OS — Discord webhook integration.
Sends pick embeds to a Discord channel via webhook URL.
Set DISCORD_WEBHOOK_URL in environment to enable.
"""

import os
import json
import requests
from datetime import datetime
import pytz

WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
ET = pytz.timezone("America/New_York")

_CONV_COLORS = {
    "HIGH":   0x00FF88,  # green
    "MEDIUM": 0xFFAA00,  # amber
    "MANUAL": 0x888888,  # gray
}
_WIN_COLOR  = 0x00FF88
_LOSS_COLOR = 0xFF3355
_PUSH_COLOR = 0x888888


def _post(payload: dict) -> bool:
    if not WEBHOOK_URL:
        return False
    try:
        r = requests.post(WEBHOOK_URL, json=payload, timeout=8)
        return r.status_code in (200, 204)
    except Exception as e:
        print(f"[Discord] Webhook error: {e}")
        return False


def post_picks_embed(picks: list[dict], date: str | None = None) -> bool:
    """
    Post a batch of picks as a Discord embed.
    Each pick dict: {team, odds, conviction, game, edge_pct, type, narrative}
    """
    if not picks:
        return False
    date = date or datetime.now(ET).strftime("%Y-%m-%d")
    n = len(picks)

    # Color from highest conviction present
    color = _CONV_COLORS["MANUAL"]
    if any(p.get("conviction", "").upper() == "HIGH" for p in picks):
        color = _CONV_COLORS["HIGH"]
    elif any(p.get("conviction", "").upper() == "MEDIUM" for p in picks):
        color = _CONV_COLORS["MEDIUM"]

    fields = []
    for p in picks:
        conv    = (p.get("conviction") or "MANUAL").upper()
        tag     = "🔒" if conv == "HIGH" else "⚡" if conv == "MEDIUM" else "•"
        team    = p.get("team") or p.get("bet", "—")
        odds    = p.get("odds", "")
        game    = p.get("game", "")
        edge    = p.get("edge_pct")
        narr    = p.get("narrative", "")
        ptype   = p.get("type", "ML")

        val_parts = [f"`{odds}`", f"[{ptype}]"]
        if edge:
            val_parts.append(f"edge +{edge:.1f}%")
        if game:
            val_parts.append(game)
        if narr:
            val_parts.append(f"*{narr}*")

        fields.append({
            "name":   f"{tag} {team}",
            "value":  "  ".join(val_parts),
            "inline": False,
        })

    embed = {
        "title":       f"PARLAY OS — {date}",
        "description": f"**{n} pick{'s' if n != 1 else ''}** from MLB quantitative model",
        "color":       color,
        "fields":      fields,
        "footer":      {
            "text": "For educational purposes only | Bet responsibly | parlayos.com/record"
        },
        "timestamp":   datetime.now(ET).isoformat(),
    }
    return _post({"embeds": [embed]})


def post_result_embed(pick: dict) -> bool:
    """
    Post a single settled result embed.
    pick dict: {bet, game, odds, result, clv_pct, game_score, conviction, date}
    """
    result = (pick.get("result") or "").upper()
    color  = _WIN_COLOR if result == "W" else _LOSS_COLOR if result == "L" else _PUSH_COLOR
    icon   = "✅" if result == "W" else "❌" if result == "L" else "↔️"

    team      = pick.get("bet", "—")
    game      = pick.get("game", "")
    odds      = pick.get("odds") or pick.get("bet_odds", "")
    score     = pick.get("game_score", "")
    clv       = pick.get("clv_pct")
    conv      = (pick.get("conviction") or "MANUAL").upper()
    date      = pick.get("date", datetime.now(ET).strftime("%Y-%m-%d"))

    clv_str = ""
    if clv is not None:
        clv_str = f"CLV: {'+' if clv >= 0 else ''}{clv:.2f}%"

    fields = [
        {"name": "Odds",      "value": f"`{odds}`",   "inline": True},
        {"name": "Result",    "value": f"{icon} **{result}**", "inline": True},
        {"name": "Conviction","value": conv,           "inline": True},
    ]
    if score:
        fields.append({"name": "Score", "value": score, "inline": True})
    if clv_str:
        fields.append({"name": "Closing Line Value", "value": clv_str, "inline": True})

    embed = {
        "title":       f"{icon} RESULT — {team}",
        "description": game,
        "color":       color,
        "fields":      fields,
        "footer":      {"text": f"PARLAY OS • {date}"},
        "timestamp":   datetime.now(ET).isoformat(),
    }
    return _post({"embeds": [embed]})


def post_daily_summary(wins: int, losses: int, roi: float, date: str | None = None) -> bool:
    """Post end-of-day summary embed."""
    date  = date or datetime.now(ET).strftime("%Y-%m-%d")
    color = _WIN_COLOR if roi >= 0 else _LOSS_COLOR
    total = wins + losses
    wr    = round(wins / total * 100, 1) if total > 0 else 0.0

    embed = {
        "title":       f"PARLAY OS — Daily Summary {date}",
        "description": f"**{wins}W - {losses}L** | Win rate: {wr}% | ROI: {'+' if roi >= 0 else ''}{roi:.1f}%",
        "color":       color,
        "footer":      {"text": "PARLAY OS • Full record: /record"},
        "timestamp":   datetime.now(ET).isoformat(),
    }
    return _post({"embeds": [embed]})
