"""PARLAY OS — Flask API
Serves dashboard data and accepts bet logging via REST endpoints.
Run: python api.py  (port 5000)
"""
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import json, os
from datetime import datetime
import pytz
import error_logger
error_logger.setup()

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
    pending_stakes = sum(float(b.get("stake") or 0) for b in bets if not b.get("result"))
    current = round(current - pending_stakes, 2)
    return current, round(peak, 2)


def _today_pnl(bets):
    today = datetime.now(ET).strftime("%Y-%m-%d")
    pnl = 0.0
    for b in bets:
        if b.get("date") != today:
            continue
        result = b.get("result")
        stake  = float(b.get("stake") or 0)
        if result == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                pnl += (dec - 1) * stake
        elif result == "L":
            pnl -= stake
    return round(pnl, 2)


# ── STATIC DASHBOARD ──────────────────────────────────────────────────────────

@app.route("/health")
def health():
    try:
        from health_check import run_health_check
        r = run_health_check(auto_restart=False)
        status = 200 if r.get("all_ok") else 503
        return jsonify({"ok": r.get("all_ok"), "failures": r.get("failures", [])}), status
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 503


@app.route("/")
def index():
    return send_from_directory(".", "parlay_dashboard.html")


# ── READ ENDPOINTS ─────────────────────────────────────────────────────────────

@app.route("/api/scout")
def api_scout():
    data = _load_json("last_scout.json")
    if data is None:
        return jsonify({"error": "last_scout.json not found"}), 404
    # Enrich game objects with full team names for dashboard matching
    try:
        from market_engine import ABR_TO_TEAM_NAME
        for g in (data.get("games") or []):
            g.setdefault("away_name", ABR_TO_TEAM_NAME.get(g.get("away", ""), g.get("away", "")))
            g.setdefault("home_name", ABR_TO_TEAM_NAME.get(g.get("home", ""), g.get("home", "")))
    except Exception:
        pass
    return jsonify(data)


@app.route("/api/bets")
def api_bets():
    bets = _db.get_bets()
    return jsonify(bets)


@app.route("/api/bankroll")
def api_bankroll():
    bets = _db.get_bets()
    current, peak = _calc_bankroll(bets)
    today = datetime.now(ET).strftime("%Y-%m-%d")
    today_bets = [b for b in bets if b.get("date") == today]
    resolved = [b for b in bets if b.get("result") in ("W", "L")]
    wins   = sum(1 for b in resolved if b["result"] == "W")
    losses = sum(1 for b in resolved if b["result"] == "L")
    pending_today = [b for b in today_bets if not b.get("result")]
    return jsonify({
        "starting":      STARTING_BANKROLL,
        "current":       current,
        "peak":          peak,
        "drawdown_pct":  round((peak - current) / peak * 100, 1) if peak > 0 else 0.0,
        "today_pnl":     _today_pnl(bets),
        "wins":          wins,
        "losses":        losses,
        "win_rate":      round(wins / len(resolved) * 100, 1) if resolved else None,
        "total_bets":    len(bets),
        "pending_count": len(pending_today),
        "bets":          bets,
    })


@app.route("/api/live")
def api_live():
    data = _load_json("live_alerts.json") or []
    return jsonify(data)


@app.route("/api/props")
def api_props():
    data = _load_json("props_output.json")
    if data is None:
        return jsonify({"error": "props_output.json not found"}), 404
    return jsonify(data)


@app.route("/api/edges")
def api_edges():
    data = _load_json("arbitrage_log.json")
    if data is None:
        return jsonify({"error": "arbitrage_log.json not found"}), 404
    return jsonify(data)


@app.route("/api/clv")
def api_clv():
    return jsonify(_load_json("clv_log.json") or [])


@app.route("/api/summary")
def api_summary():
    date = datetime.now(ET).strftime("%Y-%m-%d")
    bets = _db.get_bets()
    today_bets = [b for b in bets if b.get("date") == date]

    daily_pnl = _today_pnl(bets)
    resolved  = [b for b in bets if b.get("result") in ("W", "L", "P")]
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


@app.route("/record")
def record_page():
    return send_from_directory(".", "record.html")


@app.route("/api/record")
def api_record():
    bets     = _db.get_bets()
    resolved = [b for b in bets if b.get("result") in ("W", "L", "P")]
    wins     = sum(1 for b in resolved if b["result"] == "W")
    losses   = sum(1 for b in resolved if b["result"] == "L")
    pushes   = sum(1 for b in resolved if b["result"] == "P")

    total_wagered = sum(float(b.get("stake") or 0) for b in resolved if b["result"] != "P")
    total_pnl = 0.0
    for b in resolved:
        stake = float(b.get("stake") or 0)
        if b["result"] == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                total_pnl += (dec - 1) * stake
        elif b["result"] == "L":
            total_pnl -= stake
    roi = (total_pnl / total_wagered * 100) if total_wagered > 0 else 0.0

    clv_log   = _load_json("clv_log.json") or []
    clv_stats = clv_stats_summary(clv_log)

    by_conviction: dict = {}
    for conv in ("HIGH", "MEDIUM", "MANUAL"):
        cb = [b for b in resolved if (b.get("conviction") or "MANUAL").upper() == conv]
        if cb:
            cw = sum(1 for b in cb if b["result"] == "W")
            cl = sum(1 for b in cb if b["result"] == "L")
            by_conviction[conv] = {
                "wins":     cw,
                "losses":   cl,
                "win_rate": round(cw / len(cb) * 100, 1),
            }

    by_type: dict = {}
    for b in resolved:
        raw = (b.get("type") or "ML").strip().upper()
        btype = "TOTAL" if raw and raw[0] in ("O", "U") else raw
        rec = by_type.setdefault(btype, {"wins": 0, "losses": 0, "total": 0})
        rec["total"] += 1
        if b["result"] == "W":
            rec["wins"] += 1
        elif b["result"] == "L":
            rec["losses"] += 1
    for btype, rec in by_type.items():
        t = rec["total"]
        rec["win_rate"] = round(rec["wins"] / t * 100, 1) if t > 0 else None

    # Monthly breakdown
    by_month: dict = {}
    for b in resolved:
        month = (b.get("date") or "")[:7]
        if not month:
            continue
        rec = by_month.setdefault(month, {
            "wins": 0, "losses": 0, "pushes": 0, "total": 0,
            "pnl": 0.0, "wagered": 0.0,
        })
        rec["total"] += 1
        stake = float(b.get("stake") or 0)
        if b["result"] == "W":
            rec["wins"] += 1
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                rec["pnl"] += (dec - 1) * stake
            rec["wagered"] += stake
        elif b["result"] == "L":
            rec["losses"] += 1
            rec["pnl"] -= stake
            rec["wagered"] += stake
        else:
            rec["pushes"] += 1
    for rec in by_month.values():
        rec["pnl"]      = round(rec["pnl"], 2)
        rec["roi"]      = round(rec["pnl"] / rec["wagered"] * 100, 1) if rec["wagered"] > 0 else 0.0
        wl = rec["wins"] + rec["losses"]
        rec["win_rate"] = round(rec["wins"] / wl * 100, 1) if wl > 0 else None

    # Social proof stats
    sorted_resolved = sorted(resolved, key=lambda x: x.get("timestamp") or "", reverse=True)
    win_streak = 0
    for b in sorted_resolved:
        if b["result"] == "W":
            win_streak += 1
        else:
            break

    verified_since = None
    if resolved:
        dates = [b.get("date") for b in resolved if b.get("date")]
        verified_since = min(dates) if dates else None

    best_month = None
    best_month_roi = None
    if by_month:
        bm = max(by_month.items(), key=lambda x: x[1]["roi"])
        best_month, best_month_roi = bm[0], bm[1]["roi"]

    clv_vals = [b.get("clv_pct") for b in resolved if b.get("clv_pct") is not None]
    clv_positive_rate = (
        round(sum(1 for v in clv_vals if v > 0) / len(clv_vals) * 100, 1)
        if clv_vals else clv_stats.get("positive_rate")
    )

    return jsonify({
        "wins":              wins,
        "losses":            losses,
        "pushes":            pushes,
        "total_resolved":    len(resolved),
        "win_rate":          round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else None,
        "roi":               round(roi, 2),
        "avg_clv":           clv_stats.get("avg_clv"),
        "clv_positive_rate": clv_positive_rate,
        "by_conviction":     by_conviction,
        "by_type":           by_type,
        "by_month":          by_month,
        "social_proof": {
            "win_streak":     win_streak,
            "verified_since": verified_since,
            "best_month":     best_month,
            "best_month_roi": best_month_roi,
        },
        "picks": [
            {
                "id":          b["id"],
                "date":        b.get("date"),
                "timestamp":   b.get("timestamp"),
                "bet":         b.get("bet"),
                "type":        b.get("type"),
                "game":        b.get("game"),
                "odds":        b.get("bet_odds"),
                "result":      b.get("result"),
                "conviction":  b.get("conviction"),
                "edge_pct":    b.get("edge_pct"),
                "clv_pct":     b.get("clv_pct"),
                "verify_hash": b.get("verify_hash"),
            }
            for b in sorted_resolved
        ],
    })


@app.route("/api/verify/<verify_hash>")
def api_verify_pick(verify_hash):
    pick = _db.get_pick_by_hash(verify_hash)
    if not pick:
        return jsonify({"verified": False, "error": "Pick not found"}), 404
    return jsonify({
        "verified":   True,
        "id":         pick.get("id"),
        "date":       pick.get("date"),
        "timestamp":  pick.get("timestamp"),
        "bet":        pick.get("bet"),
        "type":       pick.get("type"),
        "game":       pick.get("game"),
        "odds":       pick.get("bet_odds"),
        "result":     pick.get("result"),
        "conviction": pick.get("conviction"),
        "edge_pct":   pick.get("edge_pct"),
        "clv_pct":    pick.get("clv_pct"),
        "verify_hash": verify_hash,
    })


@app.route("/api/picks/<pick_date>")
def api_picks_by_date(pick_date):
    try:
        datetime.strptime(pick_date, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date. Use YYYY-MM-DD"}), 400

    bets = _db.get_bets(date=pick_date)
    return jsonify({
        "date":  pick_date,
        "picks": [
            {
                "id":           b["id"],
                "bet":          b.get("bet"),
                "type":         b.get("type"),
                "game":         b.get("game"),
                "odds":         b.get("bet_odds"),
                "stake":        b.get("stake"),
                "result":       b.get("result"),
                "conviction":   b.get("conviction"),
                "edge_pct":     b.get("edge_pct"),
                "model_prob":   b.get("model_prob"),
                "market_prob":  b.get("market_prob"),
                "sp":           b.get("sp"),
                "park":         b.get("park"),
                "umpire":       b.get("umpire"),
            }
            for b in bets
        ],
    })


@app.route("/api/memory")
def api_memory():
    try:
        from memory_engine import memory_report, calibration_summary
        return jsonify({
            "report": memory_report(),
            "calibration": calibration_summary(),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/bankroll/sizing", methods=["POST"])
def api_sizing():
    data = request.get_json(silent=True) or {}
    try:
        from bankroll_engine import sizing_summary
        result = sizing_summary(
            model_prob=float(data.get("model_prob", 0.55)),
            odds=str(data.get("odds", "-110")),
            conviction=data.get("conviction", "MEDIUM"),
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
        # Return the DB bet ID so the frontend can use it for settlement
        bets = _db.get_bets(date=date)
        logged = [b for b in bets if b.get("bet") == (data.get("bet") or data.get("team", ""))
                  and b.get("game") == data.get("game", "")]
        bet_id = logged[0]["id"] if logged else None
        return jsonify({"ok": True, "bet_id": bet_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/settle", methods=["POST"])
def api_settle():
    data = request.get_json(silent=True) or {}
    bet_id = data.get("bet_id")
    result = data.get("result", "")
    if not bet_id:
        return jsonify({"error": "bet_id required"}), 400
    if result not in ("W", "L", "P"):
        return jsonify({"error": "result must be W, L, or P"}), 400
    try:
        _db.resolve_bet_by_id(
            bet_id=int(bet_id),
            closing_odds=data.get("closing_odds", ""),
            result=result,
            game_score=data.get("game_score", ""),
            notes=data.get("notes", ""),
        )
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/update", methods=["POST"])
def api_update():
    data = request.get_json(silent=True) or {}
    bet_id = data.get("bet_id")
    stake  = data.get("stake")
    if not bet_id or stake is None:
        return jsonify({"error": "bet_id and stake required"}), 400
    try:
        _db.update_bet_stake(int(bet_id), float(stake))
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
