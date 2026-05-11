"""PARLAY OS — Flask API
Serves dashboard data and accepts bet logging via REST endpoints.
Run: python api.py  (port 5000)
"""
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import json, os
from datetime import datetime
import pytz

import db as _db
from math_engine import american_to_decimal, clv_stats_summary, STARTING_BANKROLL

app = Flask(__name__)
CORS(app)

ET = pytz.timezone("America/New_York")


def _load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def _calc_bankroll(bets):
    current = STARTING_BANKROLL
    peak    = STARTING_BANKROLL
    for b in bets:
        result = b.get("result")
        stake  = float(b.get("stake") or 0)
        if result == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                current += (dec - 1) * stake
            peak = max(peak, current)
        elif result == "L":
            current -= stake
    # Pending bets: stake is locked/at-risk, reduce available bankroll
    pending_stakes = sum(float(b.get("stake") or 0) for b in bets if not b.get("result"))
    current = round(current - pending_stakes, 2)
    return current, round(peak, 2)


# ── STATIC DASHBOARD ──────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(".", "parlay_dashboard.html")


# ── READ ENDPOINTS ─────────────────────────────────────────────────────────────

@app.route("/api/scout")
def api_scout():
    data = _load_json("last_scout.json")
    if data is None:
        return jsonify({"error": "last_scout.json not found"}), 404
    return jsonify(data)


@app.route("/api/arbitrage")
def api_arbitrage():
    data = _load_json("arbitrage_log.json")
    if data is None:
        return jsonify({"error": "arbitrage_log.json not found"}), 404
    return jsonify(data)


@app.route("/api/clv")
def api_clv():
    return jsonify(_load_json("clv_log.json") or [])


@app.route("/api/bankroll")
def api_bankroll():
    bets = _db.get_bets()
    current, peak = _calc_bankroll(bets)
    return jsonify({
        "starting": STARTING_BANKROLL,
        "current":  current,
        "peak":     peak,
        "bets":     bets,
    })


@app.route("/api/summary")
def api_summary():
    date = datetime.now(ET).strftime("%Y-%m-%d")
    bets = _db.get_bets()
    today_bets = [b for b in bets if b.get("date") == date]

    daily_pnl = 0.0
    for b in today_bets:
        result = b.get("result")
        stake  = float(b.get("stake") or 0)
        if result == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                daily_pnl += (dec - 1) * stake
        elif result == "L":
            daily_pnl -= stake

    resolved = [b for b in bets if b.get("result") in ("W", "L", "P")]
    wins   = sum(1 for b in resolved if b["result"] == "W")
    losses = sum(1 for b in resolved if b["result"] == "L")

    current, _ = _calc_bankroll(bets)
    total_wagered = sum(float(b.get("stake") or 0) for b in bets)
    total_pnl = current - STARTING_BANKROLL
    roi = (total_pnl / total_wagered * 100) if total_wagered > 0 else 0.0

    clv_log   = _load_json("clv_log.json") or []
    clv_stats = clv_stats_summary(clv_log)

    return jsonify({
        "date":             date,
        "daily_pnl":        round(daily_pnl, 2),
        "win_rate":         round(wins / len(resolved) * 100, 1) if resolved else None,
        "wins":             wins,
        "losses":           losses,
        "avg_clv":          clv_stats.get("avg_clv"),
        "roi":              round(roi, 2),
        "current_bankroll": current,
        "total_bets":       len(bets),
        "total_resolved":   len(resolved),
        "total_wagered":    round(total_wagered, 2),
    })


# ── WRITE ENDPOINTS ────────────────────────────────────────────────────────────

@app.route("/api/bet", methods=["POST"])
def api_log_bet():
    data = request.get_json(silent=True) or {}
    date = data.get("date") or datetime.now(ET).strftime("%Y-%m-%d")
    try:
        _db.log_bet(
            date=date,
            bet=data.get("bet") or data.get("team", ""),
            bet_type=data.get("type", "ML"),
            game=data.get("game", ""),
            sp=data.get("sp", ""),
            park=data.get("park", ""),
            umpire=data.get("umpire", ""),
            bet_odds=str(data.get("odds", "")),
            model_prob=data.get("model_prob"),
            market_prob=data.get("market_prob"),
            edge_pct=data.get("edge_pct"),
            conviction=data.get("conviction", ""),
            stake=float(data.get("stake") or 0),
        )
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/resolve", methods=["POST"])
def api_resolve():
    data = request.get_json(silent=True) or {}
    date = data.get("date") or datetime.now(ET).strftime("%Y-%m-%d")
    try:
        _db.resolve_bet(
            bet=data.get("bet", ""),
            date=date,
            closing_odds=data.get("closing_odds", ""),
            result=data.get("result", ""),
            game_score=data.get("game_score", ""),
            notes=data.get("notes", ""),
        )
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
