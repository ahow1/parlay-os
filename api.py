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
        if result == "win":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                current += (dec - 1) * stake
            peak = max(peak, current)
        elif result == "loss":
            current -= stake
    pending_stakes = sum(float(b.get("stake") or 0) for b in bets if not b.get("result"))
    current = round(current - pending_stakes, 2)
    return current, round(peak, 2)


def _utc_today() -> str:
    import datetime as _dt
    return _dt.datetime.utcnow().date().isoformat()


def _today_pnl(bets):
    today = _utc_today()
    today_settled = [b for b in bets if b.get("date") == today and b.get("result") in ("win", "loss", "push")]
    print(f"[today_pnl] utc_today={today} settled_count={len(today_settled)}")
    pnl = sum(float(b.get("profit") or 0) for b in today_settled)
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
    resp = send_from_directory(".", "parlay_dashboard.html")
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


# ── READ ENDPOINTS ─────────────────────────────────────────────────────────────

def _enrich_and_supplement_scout(data: dict) -> dict:
    """Shared post-processing: add team names, fill empty bets from DB."""
    try:
        from market_engine import ABR_TO_TEAM_NAME
        for g in (data.get("games") or []):
            g.setdefault("away_name", ABR_TO_TEAM_NAME.get(g.get("away", ""), g.get("away", "")))
            g.setdefault("home_name", ABR_TO_TEAM_NAME.get(g.get("home", ""), g.get("home", "")))
    except Exception:
        pass
    today = datetime.now(ET).strftime("%Y-%m-%d")
    if not data.get("bets") and data.get("date") == today:
        db_bets = _db.get_bets(date=today)
        rows = []
        for b in db_bets:
            model_prob  = b.get("model_prob")
            market_prob = b.get("market_prob")
            edge_pct    = b.get("edge_pct")
            # Compute edge from probs if stored value is missing or placeholder
            if (not edge_pct or edge_pct == 5.0) and model_prob and market_prob:
                edge_pct = round((model_prob - market_prob) * 100, 1)
            elif not edge_pct:
                edge_pct = None
            rows.append({
                "team":             b.get("bet"),
                "game":             b.get("game"),
                "odds":             b.get("bet_odds"),
                "model_prob":       model_prob,
                "market_prob":      market_prob,
                "edge_pct":         edge_pct,
                "stake":            b.get("stake"),
                "conviction":       b.get("conviction"),
                "sp":               b.get("sp"),
                "sp_era":           b.get("sp_era"),
                "sp_xfip":          b.get("sp_xfip"),
                "bullpen_tier":     b.get("bullpen_tier"),
                "weather_adj":      b.get("weather_adj"),
                "platoon_edge":     b.get("platoon_edge"),
                "h2h":              b.get("h2h"),
                "confidence_score": b.get("confidence_score"),
            })
        data["bets"] = rows
        data["_source"] = "db_supplement"
    return data


NO_SCOUT_MSG = "Scout hasn't run today yet — check back after 1pm ET"

@app.route("/api/scout")
def api_scout():
    today = _utc_today()
    # DB-first: scout run on Railway writes here, so dashboard reads fresh data
    row = _db.get_latest_scout_output(date=today)
    if row and row.get("scout_json"):
        try:
            data = json.loads(row["scout_json"])
            data["_db_source"] = True
            return jsonify(_enrich_and_supplement_scout(data))
        except Exception:
            pass
    # Fallback to local file — only use if it's from today
    data = _load_json("last_scout.json")
    if data and data.get("date") == today:
        return jsonify(_enrich_and_supplement_scout(data))
    # No today scout available
    return jsonify({"bets": [], "games": [], "date": today, "message": NO_SCOUT_MSG})


@app.route("/api/bets")
def api_bets():
    bets = _db.get_bets()
    return jsonify(bets)


@app.route("/api/bankroll")
def api_bankroll():
    bets = _db.get_bets()
    override = os.environ.get("BANKROLL_OVERRIDE")
    if override:
        try:
            current = round(float(override), 2)
            peak    = current
            drawdown_pct = 0.0
        except (ValueError, TypeError):
            override = None
    if not override:
        current, peak = _calc_bankroll(bets)
        drawdown_pct  = round((peak - current) / peak * 100, 1) if peak > 0 else 0.0
    today         = _utc_today()
    today_bets    = [b for b in bets if b.get("date") == today]
    resolved      = [b for b in bets if b.get("result") in ("win", "loss", "push")]
    wins          = sum(1 for b in resolved if b["result"] == "win")
    losses        = sum(1 for b in resolved if b["result"] == "loss")
    pending_today = [b for b in today_bets if not b.get("result")]
    starting      = STARTING_BANKROLL
    bankroll      = float(os.environ.get("BANKROLL_OVERRIDE", starting))
    roi           = round((bankroll - starting) / starting * 100, 2)
    return jsonify({
        "starting":      STARTING_BANKROLL,
        "current":       current,
        "peak":          peak,
        "drawdown_pct":  drawdown_pct,
        "today_pnl":     _today_pnl(bets),
        "wins":          wins,
        "losses":        losses,
        "win_rate":      round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else None,
        "roi":           roi,
        "total_bets":    len(resolved),
        "pending_count": len(pending_today),
        "bets":          bets,
        "bankroll_source": "override" if override else "calculated",
    })


@app.route("/api/stats")
def api_stats():
    with _db._conn() as conn:
        wins   = conn.execute("SELECT COUNT(*) FROM bets WHERE result='win'").fetchone()[0]
        losses = conn.execute("SELECT COUNT(*) FROM bets WHERE result='loss'").fetchone()[0]
        pushes = conn.execute("SELECT COUNT(*) FROM bets WHERE result='push'").fetchone()[0]

    starting     = STARTING_BANKROLL
    bankroll     = float(os.environ.get("BANKROLL_OVERRIDE", starting))
    total_profit = bankroll - starting
    roi          = round(total_profit / starting * 100, 2)

    return jsonify({
        "total_bets":   wins + losses + pushes,
        "wins":         wins,
        "losses":       losses,
        "pushes":       pushes,
        "total_profit": round(total_profit, 2),
        "win_rate":     round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else None,
        "roi":          roi,
        "starting":     STARTING_BANKROLL,
    })


@app.route("/api/live")
def api_live():
    data = _load_json("live_alerts.json") or []
    return jsonify(data)


@app.route("/api/props")
def api_props():
    today = datetime.now(ET).strftime("%Y-%m-%d")
    # DB-first: same scout run that writes scout_json also writes props_json
    row = _db.get_latest_scout_output(date=today)
    if row and row.get("props_json"):
        try:
            return jsonify(json.loads(row["props_json"]))
        except Exception:
            pass
    # Fallback to local file
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
    today    = _utc_today()
    bets     = _db.get_bets()
    resolved = [b for b in bets if b.get("result") in ("win", "loss", "push")]
    wins     = sum(1 for b in resolved if b["result"] == "win")
    losses   = sum(1 for b in resolved if b["result"] == "loss")

    bankroll      = float(os.environ.get("BANKROLL_OVERRIDE", STARTING_BANKROLL))
    total_pnl     = bankroll - STARTING_BANKROLL
    roi           = total_pnl / STARTING_BANKROLL * 100
    total_wagered = sum(float(b.get("stake") or 0) for b in resolved if b["result"] != "push")

    clv_log   = _load_json("clv_log.json") or []
    clv_stats = clv_stats_summary(clv_log)

    return jsonify({
        "date":             today,
        "daily_pnl":        _today_pnl(bets),
        "win_rate":         round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else None,
        "wins":             wins,
        "losses":           losses,
        "avg_clv":          clv_stats.get("avg_clv"),
        "roi":              round(roi, 2),
        "current_bankroll": bankroll,
        "total_bets":       len(resolved),
        "total_resolved":   len(resolved),
        "total_wagered":    round(total_wagered, 2),
    })


@app.route("/record")
def record_page():
    return send_from_directory(".", "record.html")


@app.route("/api/record")
def api_record():
    bets     = _db.get_bets()
    resolved = [b for b in bets if b.get("result") in ("win", "loss", "push")]
    wins     = sum(1 for b in resolved if b["result"] == "win")
    losses   = sum(1 for b in resolved if b["result"] == "loss")
    pushes   = sum(1 for b in resolved if b["result"] == "push")

    starting     = STARTING_BANKROLL
    bankroll     = float(os.environ.get("BANKROLL_OVERRIDE", starting))
    total_pnl    = bankroll - starting
    roi          = total_pnl / starting * 100

    clv_log   = _load_json("clv_log.json") or []
    clv_stats = clv_stats_summary(clv_log)

    by_conviction: dict = {}
    for b in resolved:
        conv = (b.get("conviction") or "MANUAL").strip().upper()
        rec = by_conviction.setdefault(conv, {"wins": 0, "losses": 0, "pushes": 0, "total": 0})
        rec["total"] += 1
        if b["result"] == "win":
            rec["wins"] += 1
        elif b["result"] == "loss":
            rec["losses"] += 1
        else:
            rec["pushes"] += 1
    for rec in by_conviction.values():
        wl = rec["wins"] + rec["losses"]
        rec["win_rate"] = round(rec["wins"] / wl * 100, 1) if wl > 0 else None

    by_type: dict = {}
    for b in resolved:
        raw = (b.get("type") or "ML").strip().upper()
        btype = "TOTAL" if raw and raw[0] in ("O", "U") else raw
        rec = by_type.setdefault(btype, {"wins": 0, "losses": 0, "total": 0})
        rec["total"] += 1
        if b["result"] == "win":
            rec["wins"] += 1
        elif b["result"] == "loss":
            rec["losses"] += 1
    for btype, rec in by_type.items():
        t = rec["total"]
        rec["win_rate"] = round(rec["wins"] / t * 100, 1) if t > 0 else None

    # Monthly breakdown — P&L derived from bankroll, not SUM(profit), to avoid
    # stake-inflation from the seed data. Formula: (current_bankroll - starting) / months.
    by_month: dict = {}
    with _db._conn() as conn:
        month_rows = conn.execute("""
            SELECT strftime('%Y-%m', date) AS month,
                   SUM(CASE WHEN result='win'  THEN 1 ELSE 0 END) AS wins,
                   SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) AS losses,
                   SUM(CASE WHEN result='push' THEN 1 ELSE 0 END) AS pushes,
                   COUNT(*) AS total
            FROM bets
            WHERE result IN ('win','loss','push')
            GROUP BY month
            ORDER BY month
        """).fetchall()
    valid_months = [r for r in month_rows if r["month"]]
    num_months   = max(len(valid_months), 1)
    month_pnl    = round((bankroll - starting) / num_months, 2)
    month_roi    = round(month_pnl / starting * 100, 1)
    for row in valid_months:
        month = row["month"]
        wl    = (row["wins"] or 0) + (row["losses"] or 0)
        by_month[month] = {
            "wins":     row["wins"] or 0,
            "losses":   row["losses"] or 0,
            "pushes":   row["pushes"] or 0,
            "total":    row["total"] or 0,
            "pnl":      month_pnl,
            "roi":      month_roi,
            "win_rate": round((row["wins"] or 0) / wl * 100, 1) if wl > 0 else None,
        }

    # Social proof stats
    sorted_resolved = sorted(resolved, key=lambda x: x.get("timestamp") or "", reverse=True)
    win_streak = 0
    for b in sorted_resolved:
        if b["result"] == "win":
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

    with _db._conn() as conn:
        _best  = conn.execute("SELECT bet, game, profit, date, bet_odds FROM bets WHERE profit IS NOT NULL ORDER BY profit DESC LIMIT 1").fetchone()
        _worst = conn.execute("SELECT bet, game, profit, date, bet_odds FROM bets WHERE profit IS NOT NULL ORDER BY profit ASC LIMIT 1").fetchone()
    best_bet  = dict(_best)  if _best  else None
    worst_bet = dict(_worst) if _worst else None

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
        "best_bet":  {
            "bet": best_bet["bet"], "game": best_bet["game"],
            "date": best_bet["date"], "profit": best_bet["profit"],
        } if best_bet else None,
        "worst_bet": {
            "bet": worst_bet["bet"], "game": worst_bet["game"],
            "date": worst_bet["date"], "profit": worst_bet["profit"],
        } if worst_bet else None,
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
                "profit":      b.get("profit"),
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


@app.route("/api/seed", methods=["POST"])
def api_seed():
    try:
        from seed_bets import seed
        seed()
        with _db._conn() as conn:
            conn.execute("""
                UPDATE bets SET profit = CASE
                    WHEN result='win'  THEN stake
                    WHEN result='loss' THEN -stake
                    ELSE 0
                END WHERE profit IS NULL OR profit = 0
            """)
            count = conn.execute("SELECT COUNT(*) FROM bets").fetchone()[0]
        return jsonify({"ok": True, "total_bets": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/reset_bets", methods=["POST"])
def api_reset_bets():
    try:
        from seed_bets import seed
        with _db._conn() as conn:
            conn.execute("DELETE FROM bets")
        seed()
        with _db._conn() as conn:
            conn.execute("""
                UPDATE bets SET profit = CASE
                    WHEN result='win'  THEN stake
                    WHEN result='loss' THEN -stake
                    ELSE 0
                END WHERE profit IS NULL OR profit = 0
            """)
            count = conn.execute("SELECT COUNT(*) FROM bets").fetchone()[0]
        return jsonify({"ok": True, "total_bets": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
