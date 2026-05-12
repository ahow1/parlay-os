"""PARLAY OS — live_engine.py
Conviction-based live betting brain. 60-second cycle 6pm-11pm ET.
Six-component quality score (run_quality 25 / sp_status 20 / bullpen_avail 20 /
lineup_due 15 / poly_misprice 10 / sharp_money 10).
Fires alerts only when ALL gate conditions pass. Learns from outcomes.
"""

import os
import sys
import time
import json
import sqlite3
import requests
from datetime import datetime, date, timedelta
from collections import defaultdict
import pytz

from market_engine   import full_market_snapshot, get_mlb_events
from memory_engine   import init_memory_tables, recalibrate_model_prob, record_live_bet, resolve_live_bet
from bankroll_engine import kelly_stake, is_drawdown_pause, current_bankroll
from math_engine     import implied_prob, american_to_decimal
from bullpen_engine  import analyze_bullpen
from constants       import MLB_TEAM_MAP, MLB_TEAM_IDS

STATSAPI    = "https://statsapi.mlb.com/api/v1"
ET          = pytz.timezone("America/New_York")
BOT_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH     = "parlay_os.db"
ALERTS_FILE = "live_alerts.json"

CYCLE_SECS    = 60
LIVE_START_ET = 18
LIVE_END_ET   = 23

# Alert gate conditions
MIN_QUALITY_SCORE = 6.5
MAX_DEFICIT       = 3
MAX_INNING        = 7
MIN_POLY_GAP      = 0.05   # 5%

# Default component weights  (sum = 1.0)
DEFAULT_WEIGHTS = {
    "run_quality":   0.25,
    "sp_status":     0.20,
    "bullpen_avail": 0.20,
    "lineup_due":    0.15,
    "poly_misprice": 0.10,
    "sharp_money":   0.10,
}
WEIGHT_LEARN_THRESHOLD = 100  # auto-adjust after this many resolved outcomes

# Play event → how lucky/sustainable was the run (10 = very lucky / value for trailing)
_EVENT_SCORE = {
    "Grand Slam":        1.0,
    "Home Run":          3.0,
    "Triple":            4.5,
    "Double":            4.5,
    "Single":            5.0,
    "Sacrifice Fly":     5.0,
    "Fielders Choice":   5.0,
    "Forceout":          5.0,
    "Grounded Into DP":  5.0,
    "Walk":              7.0,
    "Intent Walk":       7.0,
    "Hit By Pitch":      7.0,
    "Error":             8.5,
    "Balk":              8.5,
    "Wild Pitch":        9.0,
    "Passed Ball":       9.0,
}


# ── DATABASE ──────────────────────────────────────────────────────────────────

def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _init_live_tables():
    with _conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS live_alert_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp       TEXT NOT NULL,
            game_pk         INTEGER,
            game_label      TEXT,
            bet_side        TEXT,
            inning          INTEGER,
            deficit         INTEGER,
            quality_score   REAL,
            run_quality     REAL,
            sp_status       REAL,
            bullpen_avail   REAL,
            lineup_due      REAL,
            poly_misprice   REAL,
            sharp_money     REAL,
            poly_price      REAL,
            model_prob      REAL,
            entry_odds      TEXT,
            stake           REAL,
            conviction      TEXT,
            how_runs_scored TEXT,
            outcome         INTEGER
        );

        CREATE TABLE IF NOT EXISTS live_component_weights (
            component    TEXT PRIMARY KEY,
            weight       REAL NOT NULL,
            last_updated TEXT
        );
        """)
    # Seed default weights if not yet present
    with _conn() as conn:
        for comp, w in DEFAULT_WEIGHTS.items():
            conn.execute(
                "INSERT OR IGNORE INTO live_component_weights (component, weight, last_updated)"
                " VALUES (?,?,?)",
                (comp, w, datetime.now(ET).isoformat()),
            )


# ── WEIGHT MANAGEMENT ─────────────────────────────────────────────────────────

def _load_weights() -> dict:
    try:
        with _conn() as conn:
            rows = conn.execute(
                "SELECT component, weight FROM live_component_weights"
            ).fetchall()
        if rows:
            return {r["component"]: r["weight"] for r in rows}
    except Exception:
        pass
    return dict(DEFAULT_WEIGHTS)


def _maybe_learn_weights():
    """After WEIGHT_LEARN_THRESHOLD resolved outcomes, recalibrate component weights."""
    try:
        with _conn() as conn:
            n = conn.execute(
                "SELECT COUNT(*) as c FROM live_alert_log WHERE outcome IS NOT NULL"
            ).fetchone()["c"]
        if n < WEIGHT_LEARN_THRESHOLD or n % WEIGHT_LEARN_THRESHOLD != 0:
            return

        with _conn() as conn:
            rows = conn.execute("""
                SELECT run_quality, sp_status, bullpen_avail,
                       lineup_due, poly_misprice, sharp_money, outcome
                FROM live_alert_log WHERE outcome IS NOT NULL
            """).fetchall()

        components = list(DEFAULT_WEIGHTS.keys())
        current    = _load_weights()
        correlations: dict = {}

        for comp in components:
            vals = [float(r[comp] or 5.0) for r in rows]
            outs = [int(r["outcome"]) for r in rows]
            m_v  = sum(vals) / len(vals)
            m_o  = sum(outs) / len(outs)
            num  = sum((v - m_v) * (o - m_o) for v, o in zip(vals, outs))
            d_v  = sum((v - m_v) ** 2 for v in vals) ** 0.5
            d_o  = sum((o - m_o) ** 2 for o in outs) ** 0.5
            correlations[comp] = max(num / (d_v * d_o) if d_v * d_o else 0, 0.02)

        total_c = sum(correlations.values()) or 1.0
        now_str = datetime.now(ET).isoformat()
        with _conn() as conn:
            for comp in components:
                target = correlations[comp] / total_c
                new_w  = round(0.70 * current.get(comp, DEFAULT_WEIGHTS[comp]) + 0.30 * target, 4)
                conn.execute(
                    "UPDATE live_component_weights SET weight=?, last_updated=? WHERE component=?",
                    (new_w, now_str, comp),
                )
        print(f"[LEARN] Weights recalibrated after {n} outcomes: "
              + "  ".join(f"{c}={round(correlations[c]/total_c, 3)}" for c in components))
    except Exception as e:
        print(f"[LEARN] Weight update error: {e}")


def _log_alert(
    game_pk: int, game_label: str, bet_side: str, inning: int, deficit: int,
    quality_score: float, scores: dict, poly_price: float | None,
    model_prob: float, entry_odds: str, stake: float, conviction: str,
    how_runs_scored: str,
) -> int:
    """Insert alert into live_alert_log. Returns row id."""
    now = datetime.now(ET).isoformat()
    with _conn() as conn:
        cur = conn.execute("""
            INSERT INTO live_alert_log
            (timestamp, game_pk, game_label, bet_side, inning, deficit,
             quality_score, run_quality, sp_status, bullpen_avail,
             lineup_due, poly_misprice, sharp_money,
             poly_price, model_prob, entry_odds, stake, conviction, how_runs_scored)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now, game_pk, game_label, bet_side, inning, deficit,
            quality_score,
            scores.get("run_quality", 5.0),
            scores.get("sp_status", 5.0),
            scores.get("bullpen_avail", 5.0),
            scores.get("lineup_due", 5.0),
            scores.get("poly_misprice", 5.0),
            scores.get("sharp_money", 5.0),
            poly_price, model_prob, entry_odds, stake, conviction, how_runs_scored,
        ))
        return cur.lastrowid or 0


# ── DATA FETCHING ─────────────────────────────────────────────────────────────

def _fetch_live_games(game_date: str) -> list:
    """Pull all in-progress and recently completed games with full state."""
    try:
        r = requests.get(
            f"{STATSAPI}/schedule",
            params={
                "sportId":  1,
                "date":     game_date,
                "hydrate":  "linescore,boxscore,decisions,probablePitcher",
                "gameType": "R",
            },
            timeout=12,
        )
        out = []
        for day in r.json().get("dates", []):
            for g in day.get("games", []):
                state = g.get("status", {}).get("abstractGameState", "")
                if state in ("Live", "Final"):
                    out.append(g)
        return out
    except Exception:
        return []


def _fetch_scoring_plays(game_pk: int) -> list:
    """Return last 10 scoring plays (play-by-play) for a game."""
    try:
        r = requests.get(
            f"{STATSAPI}/game/{game_pk}/playByPlay",
            params={"fields": "allPlays,result,about,matchup"},
            timeout=10,
        )
        all_plays  = r.json().get("allPlays", [])
        scoring    = [p for p in all_plays if p.get("about", {}).get("isScoringPlay")]
        return scoring[-10:]
    except Exception:
        return []


def _parse_game_state(g: dict) -> dict:
    """Flatten a schedule-hydrated game object into a clean state dict."""
    ls        = g.get("linescore", {})
    box       = g.get("boxscore", {})
    teams_ls  = ls.get("teams", {})
    teams_box = box.get("teams", {})
    game_teams = g.get("teams", {})

    inning    = ls.get("currentInning", 0)
    top       = ls.get("isTopInning", True)
    outs      = ls.get("outs", 0)
    away_runs = teams_ls.get("away", {}).get("runs", 0)
    home_runs = teams_ls.get("home", {}).get("runs", 0)

    defense = ls.get("defense", {})
    offense = ls.get("offense", {})

    cur_pitcher_id   = defense.get("pitcher", {}).get("id")
    cur_pitcher_name = defense.get("pitcher", {}).get("fullName", "Unknown")
    cur_batter_id    = offense.get("batter", {}).get("id")

    def _batting_order(side: str) -> list:
        order   = teams_box.get(side, {}).get("battingOrder", [])
        players = teams_box.get(side, {}).get("players", {})
        result  = []
        for pid in order:
            pinfo = players.get(f"ID{pid}", {}).get("person", {})
            result.append({"id": pid, "name": pinfo.get("fullName", str(pid))})
        return result

    def _pitchers_used(side: str) -> list:
        ids     = teams_box.get(side, {}).get("pitchers", [])
        players = teams_box.get(side, {}).get("players", {})
        out     = []
        for pid in ids:
            pinfo  = players.get(f"ID{pid}", {})
            pstats = pinfo.get("stats", {}).get("pitching", {})
            out.append({
                "id":   pid,
                "name": pinfo.get("person", {}).get("fullName", str(pid)),
                "np":   int(pstats.get("pitchesThrown", 0) or 0),
            })
        return out

    away_pitchers = _pitchers_used("away")
    home_pitchers = _pitchers_used("home")

    def _sp_info(pitchers: list) -> dict:
        if not pitchers:
            return {"id": None, "name": "TBD", "np": 0, "still_in": False}
        sp = pitchers[0]
        return {
            "id":       sp["id"],
            "name":     sp["name"],
            "np":       sp["np"],
            "still_in": len(pitchers) == 1,
        }

    return {
        "game_pk":        g.get("gamePk"),
        "away_team":      game_teams.get("away", {}).get("team", {}).get("name", ""),
        "home_team":      game_teams.get("home", {}).get("team", {}).get("name", ""),
        "away_code":      game_teams.get("away", {}).get("team", {}).get("abbreviation", ""),
        "home_code":      game_teams.get("home", {}).get("team", {}).get("abbreviation", ""),
        "inning":         inning,
        "top_inning":     top,
        "outs":           outs,
        "away_runs":      away_runs,
        "home_runs":      home_runs,
        "cur_pitcher_id": cur_pitcher_id,
        "cur_pitcher_name": cur_pitcher_name,
        "cur_batter_id":  cur_batter_id,
        "away_order":     _batting_order("away"),
        "home_order":     _batting_order("home"),
        "away_sp":        _sp_info(away_pitchers),
        "home_sp":        _sp_info(home_pitchers),
        "away_pitchers":  away_pitchers,
        "home_pitchers":  home_pitchers,
        "runners": {
            "1b": bool(offense.get("first")),
            "2b": bool(offense.get("second")),
            "3b": bool(offense.get("third")),
        },
        "abstract_state": g.get("status", {}).get("abstractGameState", ""),
    }


# ── QUALITY SCORE COMPONENTS ──────────────────────────────────────────────────

def _play_side(play: dict) -> str:
    half = play.get("about", {}).get("halfInning", "top")
    return "away" if half == "top" else "home"


def _score_run_quality(scoring_plays: list, leading_side: str) -> tuple:
    """
    Score how the leading team's runs came to be.
    Lucky runs (WP, error) → high score (value for trailing team).
    Legitimate runs (HR, sustained hits) → low score.
    Returns (score 0-10, narrative str).
    """
    side_plays = [p for p in scoring_plays if _play_side(p) == leading_side][-5:]
    if not side_plays:
        return 5.0, "No recent scoring data"

    events  = []
    raw_scores = []
    for p in side_plays:
        desc  = p.get("result", {}).get("description", "")
        event = p.get("result", {}).get("event", "Single")
        if "grand slam" in desc.lower():
            raw_scores.append(1.0)
            events.append("Grand Slam")
        else:
            raw_scores.append(_EVENT_SCORE.get(event, 5.0))
            events.append(event)

    score = round(sum(raw_scores) / len(raw_scores), 1)

    # Narrative
    hr_count  = events.count("Home Run")
    wp_count  = sum(1 for e in events if e in ("Wild Pitch", "Passed Ball"))
    err_count = events.count("Error")
    walk_count = sum(1 for e in events if "Walk" in e)
    gs_count  = events.count("Grand Slam")

    parts = []
    if gs_count:  parts.append(f"{gs_count} grand slam")
    if hr_count:  parts.append(f"{hr_count} solo HR{'s' if hr_count > 1 else ''}")
    if wp_count:  parts.append(f"{wp_count} wild pitch/passed ball")
    if err_count: parts.append(f"{err_count} error")
    if walk_count: parts.append(f"{walk_count} walk")

    how = f"Runs via: {', '.join(parts) if parts else ', '.join(events[-3:])}"
    return score, how


def _score_sp_status(state: dict, bet_side: str) -> tuple:
    """
    Score the trailing team's OWN SP status.
    A fresh SP holding the game = value; pulled SP = bullpen risk.
    Returns (score 0-10, narrative str).
    """
    sp = state.get(f"{bet_side}_sp", {})
    if not sp.get("still_in"):
        return 1.0, "SP already pulled — bullpen game"

    np = sp.get("np", 0)
    name = sp.get("name", "SP")
    if np < 75:
        return 9.0, f"{name} at {np}p — fresh, can go deep"
    elif np < 90:
        return 6.0, f"{name} at {np}p — probably 1-2 innings left"
    elif np < 105:
        return 3.0, f"{name} at {np}p — near hook"
    else:
        return 1.0, f"{name} at {np}p — over limit, bullpen imminent"


def _score_bullpen_avail(state: dict, bp: dict, bet_side: str) -> tuple:
    """
    Score how fresh the trailing team's bullpen is.
    Uses pre-game fatigue data cross-referenced with tonight's usage.
    Returns (score 0-10, narrative str).
    """
    pitchers_used_tonight = {
        p["id"] for p in state.get(f"{bet_side}_pitchers", [])
    }
    # Remove SP — only count relievers already used
    sp_id = state.get(f"{bet_side}_sp", {}).get("id")
    if sp_id:
        pitchers_used_tonight.discard(sp_id)

    arms        = bp.get("arms", [])
    closer_name = bp.get("closer_name", "")
    closer_avail = bp.get("closer_available", True)

    # Was closer used tonight?
    closer_used_tonight = any(
        a["name"] == closer_name and a["id"] in pitchers_used_tonight
        for a in arms if a.get("id")
    )

    # Count top-2 setup arms used tonight
    top_arms = [a for a in arms if a["name"] != closer_name][:2]
    setup_used = sum(
        1 for a in top_arms if a.get("id") in pitchers_used_tonight
    )

    if not closer_used_tonight and setup_used == 0:
        score, note = 9.0, f"Full pen available — {closer_name or 'closer'} rested"
    elif not closer_used_tonight and setup_used == 1:
        score, note = 6.0, f"{closer_name or 'closer'} rested, 1 setup used"
    elif not closer_used_tonight and setup_used >= 2:
        score, note = 4.0, f"{closer_name or 'closer'} rested but top setup arms gone"
    elif closer_used_tonight and setup_used == 0:
        score, note = 3.0, f"{closer_name or 'closer'} already used"
    else:
        score, note = 2.0, f"Closer + {setup_used} setup arms already used"

    # Adjust down for overall pre-game fatigue
    tier = bp.get("fatigue_tier", "MODERATE")
    if tier == "TIRED":
        score = max(score - 2.0, 1.0)
        note  += " (pen TIRED pre-game)"

    return round(score, 1), note


def _score_lineup_due(state: dict, bet_side: str) -> tuple:
    """
    Score quality of trailing team's lineup coming up.
    Tracks batting order position to estimate next-inning batters.
    Returns (score 0-10, narrative str).
    """
    order = state.get(f"{bet_side}_order", [])
    if not order:
        return 5.0, "Batting order unavailable"

    top_inning     = state.get("top_inning", True)
    currently_batting = (bet_side == "away") == top_inning
    cur_batter_id  = state.get("cur_batter_id")

    pos = None
    if currently_batting and cur_batter_id:
        for i, batter in enumerate(order):
            if batter["id"] == cur_batter_id:
                pos = i + 1  # 1-indexed
                break

    if pos is None:
        pos = 5  # neutral default

    # Next 3 batters from current position (wrapping)
    n    = len(order)
    idxs = [(pos - 1 + k) % n for k in range(3)]
    next3 = [order[i] for i in idxs]
    names = " → ".join(b["name"].split()[-1] for b in next3)

    if pos <= 3:
        score, label = 8.0, "Top of order"
    elif pos <= 6:
        score, label = 5.0, "Middle of order"
    else:
        score, label = 2.0, "Bottom of order"

    return score, f"{label} due: {names}"


def _score_poly_misprice(poly_price: float | None, model_prob: float,
                          bet_side: str, poly: dict) -> tuple:
    """
    Score how mispriced Polymarket is vs our model.
    Returns (score 0-10, narrative str).
    """
    if poly_price is None or poly_price <= 0:
        return 0.0, "No Polymarket data"

    gap = model_prob - poly_price
    if gap < MIN_POLY_GAP:
        return 0.0, f"Poly {poly_price:.1%} vs model {model_prob:.1%} — gap {gap*100:.1f}pt (need 5+)"

    # Scale: 5% gap = 5.0, 10% gap = 10.0, cap at 10
    score = min(round((gap / MIN_POLY_GAP) * 5.0, 1), 10.0)
    return score, f"Poly {poly_price:.1%} vs model {model_prob:.1%} — EDGE +{gap*100:.1f}pt"


def _score_sharp_money(line_movement: dict, bet_side: str) -> tuple:
    """
    Score line movement direction relative to trailing team.
    Returns (score 0-10, narrative str).
    """
    direction = (line_movement or {}).get("direction", "unknown")
    magnitude = (line_movement or {}).get("magnitude", 0.0)

    if direction in (f"steam_{bet_side}", f"toward_{bet_side}"):
        return 8.0, f"Line moving toward {bet_side.upper()} (Δ{magnitude:.3f})"
    elif direction in ("stable", "unknown", ""):
        return 5.0, "Line stable"
    else:
        return 1.0, f"Line moving AWAY from {bet_side.upper()} (Δ{magnitude:.3f})"


# ── QUALITY SCORE AGGREGATION ─────────────────────────────────────────────────

def compute_quality_score(scores: dict, weights: dict) -> float:
    """Weighted average of component scores (each 0-10). Result 0-10."""
    total = sum(weights.get(k, 0) * v for k, v in scores.items())
    w_sum = sum(weights.get(k, 0) for k in scores)
    return round(total / w_sum if w_sum else 5.0, 2)


# ── ALERT CONDITIONS ──────────────────────────────────────────────────────────

def check_alert_conditions(
    state: dict, quality_score: float,
    poly_gap: float, bet_side: str
) -> tuple:
    """
    ALL conditions must pass to fire an alert.
    Returns (passes: bool, reason: str).
    """
    opp_side  = "home" if bet_side == "away" else "away"
    our_runs  = state.get(f"{bet_side}_runs", 0)
    opp_runs  = state.get(f"{opp_side}_runs", 0)
    deficit   = opp_runs - our_runs
    inning    = state.get("inning", 0)
    abstract  = state.get("abstract_state", "")

    if abstract == "Final":
        return False, "Game already final"
    if inning > MAX_INNING:
        return False, f"Too late — inning {inning} (max {MAX_INNING})"
    if deficit < 1:
        return False, f"{bet_side.upper()} is not trailing"
    if deficit > MAX_DEFICIT:
        return False, f"Deficit {deficit} too large (max {MAX_DEFICIT})"
    if quality_score < MIN_QUALITY_SCORE:
        return False, f"Quality {quality_score:.1f} below threshold {MIN_QUALITY_SCORE}"
    if poly_gap < MIN_POLY_GAP:
        return False, f"Poly gap {poly_gap*100:.1f}pt below 5pt minimum"
    return True, "ALL CONDITIONS PASS"


# ── ALERT FORMATTING ──────────────────────────────────────────────────────────

def _format_live_alert(
    state: dict, scores: dict, narratives: dict,
    quality_score: float, conviction: str,
    poly_price: float | None, model_prob: float,
    best_odds: int | None, best_book: str,
    bet_side: str, stake: float,
    bp: dict, how_runs_scored: str,
) -> str:
    away     = state["away_team"]
    home     = state["home_team"]
    away_r   = state["away_runs"]
    home_r   = state["home_runs"]
    inning   = state["inning"]
    top      = state["top_inning"]
    opp_side = "home" if bet_side == "away" else "away"

    # Score line
    score_line = f"{away_r}-{home_r} {'NYY leads' if away_r > home_r else 'HM leads' if home_r > away_r else 'TIED'}"
    inn_label  = f"{'T' if top else 'B'}{inning}"

    # Our SP
    our_sp  = state.get(f"{bet_side}_sp", {})
    sp_str  = f"{our_sp.get('name','?')} ({our_sp.get('np',0)}p)"
    if not our_sp.get("still_in"):
        sp_str = f"BULLPEN (SP pulled)"

    # Poly edge
    poly_str = f"{poly_price:.0%}" if poly_price else "N/A"
    edge_pct = round((model_prob - (poly_price or model_prob)) * 100, 1)
    odds_str = f"{best_odds:+d} @ {best_book.upper()}" if best_odds else "N/A"

    # Conviction marker
    conv_label = "🔴 HIGH" if conviction == "HIGH" else "🟡 MEDIUM"

    # Pen description from narrative
    pen_note    = narratives.get("bullpen_avail", "")
    lineup_note = narratives.get("lineup_due", "")

    # Stake urgency: HIGH conviction on 6th/7th inning = ACT FAST
    if conviction == "HIGH" and inning >= 6:
        urgency = "ACT FAST — window closing"
    elif quality_score >= 8.0:
        urgency = "ACT FAST — very high quality setup"
    else:
        urgency = "WAIT for price confirmation"

    lines = [
        f"🚨 LIVE EDGE — {conv_label} CONVICTION",
        f"<b>{away} @ {home}</b>  —  {inn_label} | {away_r}-{home_r} | {sp_str}",
        f"Live Poly {poly_str} vs Model {model_prob:.1%} vs Books {odds_str}  |  EDGE: +{edge_pct:.1f}%",
        f"QUALITY SCORE: {quality_score:.1f}/10",
        "",
        f"WHY THIS HAS VALUE: {narratives.get('why', 'Quality score exceeded threshold with Polymarket mispricing.')}",
        f"HOW RUNS SCORED: {how_runs_scored}",
        f"PITCHING LEFT: {pen_note}",
        f"LINEUP DUE: {lineup_note}",
        f"",
        f"STAKE: ${stake:.2f} — {urgency}",
    ]

    # Component breakdown
    comp_parts = [
        f"RunQ={scores.get('run_quality',0):.1f}",
        f"SP={scores.get('sp_status',0):.1f}",
        f"BP={scores.get('bullpen_avail',0):.1f}",
        f"Lineup={scores.get('lineup_due',0):.1f}",
        f"Poly={scores.get('poly_misprice',0):.1f}",
        f"Sharp={scores.get('sharp_money',0):.1f}",
    ]
    lines.append("  ".join(comp_parts))

    return "\n".join(lines)


def _build_why_narrative(state: dict, scores: dict, narratives: dict, bet_side: str) -> str:
    """Construct a 2-sentence narrative grounded in actual game state."""
    opp_side = "home" if bet_side == "away" else "away"
    our_sp   = state.get(f"{bet_side}_sp", {})
    opp_sp   = state.get(f"{opp_side}_sp", {})

    # Sentence 1: pitcher situation
    if our_sp.get("still_in") and our_sp.get("np", 0) < 85:
        s1 = (f"{our_sp['name']} is still fresh ({our_sp['np']}p) and should hold "
              f"the opposition while the offense regroups.")
    elif not opp_sp.get("still_in"):
        s1 = (f"The opposing SP has been pulled — {state[f'{bet_side}_team']} now "
              f"faces the {opp_side.upper()} bullpen with a full lineup.")
    else:
        opp_np = opp_sp.get("np", 0)
        s1 = (f"Opposing SP {opp_sp.get('name','?')} is at {opp_np}p and is "
              f"{'approaching' if opp_np > 85 else 'still managing'} his pitch limit.")

    # Sentence 2: how runs scored + lineup
    rq_score = scores.get("run_quality", 5.0)
    lu_score = scores.get("lineup_due", 5.0)
    if rq_score >= 7.0 and lu_score >= 7.0:
        s2 = ("The deficit was built on lucky runs (errors/wild pitches) and the "
              "top of the order is due up — prime spot for a swing.")
    elif rq_score >= 7.0:
        s2 = ("The runs against were fortunate, not the result of sustained pressure — "
              "regression favors the trailing team here.")
    elif lu_score >= 7.0:
        s2 = ("The top of the batting order is due up — the highest-leverage spot to "
              "generate a rally against a tiring bullpen.")
    else:
        s2 = ("Quality score exceeded threshold across multiple components; "
              "game state supports a trailing team cover.")

    return s1 + "  " + s2


# ── MAIN CYCLE ────────────────────────────────────────────────────────────────

def run_live_cycle(
    state: dict,
    bp_away: dict, bp_home: dict,
    market: dict, scoring_plays: list,
    weights: dict,
) -> list:
    """
    Evaluate one game for both sides. Returns list of alert dicts.
    """
    alerts  = []
    game_pk = state["game_pk"]

    nv   = market.get("no_vig") or {}
    poly = market.get("polymarket") or {}
    lm   = market.get("line_movement") or {}

    for bet_side, bp in (("away", bp_away), ("home", bp_home)):
        opp_side = "home" if bet_side == "away" else "away"
        our_runs = state.get(f"{bet_side}_runs", 0)
        opp_runs = state.get(f"{opp_side}_runs", 0)
        deficit  = opp_runs - our_runs

        if deficit < 1:
            continue  # not trailing, skip

        our_nv     = nv.get(bet_side, 0.5)
        model_prob = recalibrate_model_prob(our_nv)
        poly_price = poly.get(bet_side)
        poly_gap   = (model_prob - poly_price) if poly_price else 0.0

        leading_side = opp_side

        # ── Score all 6 components ────────────────────────────────────────
        rq_score, rq_note   = _score_run_quality(scoring_plays, leading_side)
        sp_score, sp_note   = _score_sp_status(state, bet_side)
        bp_score, bp_note   = _score_bullpen_avail(state, bp, bet_side)
        lu_score, lu_note   = _score_lineup_due(state, bet_side)
        pm_score, pm_note   = _score_poly_misprice(poly_price, model_prob, bet_side, poly)
        sm_score, sm_note   = _score_sharp_money(lm, bet_side)

        scores = {
            "run_quality":   rq_score,
            "sp_status":     sp_score,
            "bullpen_avail": bp_score,
            "lineup_due":    lu_score,
            "poly_misprice": pm_score,
            "sharp_money":   sm_score,
        }
        narratives = {
            "run_quality":   rq_note,
            "sp_status":     sp_note,
            "bullpen_avail": bp_note,
            "lineup_due":    lu_note,
            "poly_misprice": pm_note,
            "sharp_money":   sm_note,
        }

        quality_score = compute_quality_score(scores, weights)
        passes, reason = check_alert_conditions(state, quality_score, poly_gap, bet_side)

        if not passes:
            continue

        # Conviction tier
        if quality_score >= 8.5 and poly_gap >= 0.10:
            conviction = "HIGH"
        elif quality_score >= 7.0 or poly_gap >= 0.07:
            conviction = "MEDIUM"
        else:
            conviction = "LOW"

        best_odds = market.get(f"best_{bet_side}_odds")
        best_book = market.get(f"best_{bet_side}_book", "")

        if not best_odds:
            continue

        stake = kelly_stake(model_prob, str(best_odds), conviction=conviction)
        if stake <= 0:
            continue

        _, how_runs_scored = _score_run_quality(scoring_plays, leading_side)
        narratives["why"] = _build_why_narrative(state, scores, narratives, bet_side)

        game_label = f"{state['away_team']} @ {state['home_team']}"

        alerts.append({
            "game_pk":       game_pk,
            "game_label":    game_label,
            "bet_side":      bet_side,
            "team":          state[f"{bet_side}_team"],
            "inning":        state["inning"],
            "deficit":       deficit,
            "quality_score": quality_score,
            "scores":        scores,
            "narratives":    narratives,
            "conviction":    conviction,
            "poly_price":    poly_price,
            "poly_gap":      poly_gap,
            "model_prob":    model_prob,
            "best_odds":     best_odds,
            "best_book":     best_book,
            "stake":         stake,
            "how_runs_scored": how_runs_scored,
            "bp":            bp,
            "state":         state,
        })

    return alerts


# ── DASHBOARD ─────────────────────────────────────────────────────────────────

def _update_dashboard(in_progress: list, active_alerts: list):
    """Write live_alerts.json for the dashboard live tab."""
    try:
        # Load recent completed alerts from DB
        recent = []
        with _conn() as conn:
            rows = conn.execute("""
                SELECT timestamp, game_label, bet_side, quality_score,
                       conviction, entry_odds, stake, outcome
                FROM live_alert_log
                ORDER BY id DESC LIMIT 20
            """).fetchall()
            for r in rows:
                recent.append({
                    "timestamp":     r["timestamp"],
                    "game":          r["game_label"],
                    "side":          r["bet_side"],
                    "quality":       r["quality_score"],
                    "conviction":    r["conviction"],
                    "odds":          r["entry_odds"],
                    "stake":         r["stake"],
                    "outcome":       r["outcome"],
                })
    except Exception:
        recent = []

    payload = {
        "last_updated":   datetime.now(ET).isoformat(),
        "in_progress":    in_progress,
        "active_alerts":  active_alerts,
        "recent_alerts":  recent,
    }
    try:
        with open(ALERTS_FILE, "w") as f:
            json.dump(payload, f, indent=2, default=str)
    except Exception as e:
        print(f"[DASH] Failed to write {ALERTS_FILE}: {e}")


# ── TELEGRAM ──────────────────────────────────────────────────────────────────

def _send_telegram(msg: str):
    if not BOT_TOKEN or not CHAT_ID:
        print(msg)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=8,
        )
    except Exception:
        pass


def _in_live_window() -> bool:
    now  = datetime.now(ET)
    return LIVE_START_ET <= now.hour < LIVE_END_ET


# ── DEDUPLICATION ─────────────────────────────────────────────────────────────

# Prevent re-alerting the same game+side within a 20-minute window
_alerted_recently: dict = {}  # key: (game_pk, bet_side) → timestamp


def _already_alerted(game_pk: int, bet_side: str) -> bool:
    key = (game_pk, bet_side)
    last = _alerted_recently.get(key)
    if last is None:
        return False
    return (datetime.now(ET) - last).seconds < 1200   # 20-minute cooldown


def _mark_alerted(game_pk: int, bet_side: str):
    _alerted_recently[(game_pk, bet_side)] = datetime.now(ET)


# ── MAIN LOOP ─────────────────────────────────────────────────────────────────

def run_live_monitor():
    """Continuous 60-second live monitoring loop, 6pm-11pm ET."""
    init_memory_tables()
    _init_live_tables()
    weights = _load_weights()
    print("[LIVE] Monitor started — weights:", weights)
    _send_telegram("🟢 Live monitor online — conviction model active")

    cycle_count = 0

    while True:
        if not _in_live_window():
            time.sleep(CYCLE_SECS)
            continue

        if is_drawdown_pause():
            _send_telegram("⚠️ DRAWDOWN PAUSE — live betting suspended")
            time.sleep(300)
            continue

        today = datetime.now(ET).strftime("%Y-%m-%d")

        # Reload weights every 10 cycles in case they were auto-updated
        if cycle_count % 10 == 0:
            weights = _load_weights()

        cycle_count += 1

        try:
            live_games = _fetch_live_games(today)
        except Exception as e:
            print(f"[LIVE] Schedule fetch error: {e}")
            time.sleep(CYCLE_SECS)
            continue

        dashboard_in_progress = []
        dashboard_alerts      = []

        for g in live_games:
            state = _parse_game_state(g)
            game_pk   = state.get("game_pk")
            away_code = state.get("away_code") or MLB_TEAM_MAP.get(state.get("away_team", ""), "")
            home_code = state.get("home_code") or MLB_TEAM_MAP.get(state.get("home_team", ""), "")

            if not game_pk:
                continue

            away_tid = MLB_TEAM_IDS.get(away_code)
            home_tid = MLB_TEAM_IDS.get(home_code)

            # Bullpen state
            try:
                bp_away = analyze_bullpen(away_tid, today, label=away_code) if away_tid else {}
                bp_home = analyze_bullpen(home_tid, today, label=home_code) if home_tid else {}
            except Exception:
                bp_away = bp_home = {}

            # Market snapshot (Polymarket + sportsbook lines + line movement)
            try:
                events = get_mlb_events()
                event_id = next(
                    (e["id"] for e in events
                     if state["away_team"] in e.get("away", "") or
                        state["home_team"] in e.get("home", "")),
                    str(game_pk),
                )
                market = full_market_snapshot(
                    event_id, state["away_team"], state["home_team"],
                    away_code, home_code, today,
                )
            except Exception:
                market = {}

            # Scoring plays for run quality analysis
            try:
                scoring_plays = _fetch_scoring_plays(game_pk)
            except Exception:
                scoring_plays = []

            # Dashboard: in-progress entry for every live game
            inn  = state["inning"]
            top  = state["top_inning"]
            ar   = state["away_runs"]
            hr   = state["home_runs"]
            cp_name = state["cur_pitcher_name"]
            np_label = ""

            # Quick quality score for dashboard (no alert logic)
            nv     = (market.get("no_vig") or {})
            for side in ("away", "home"):
                opp   = "home" if side == "away" else "away"
                our_r = ar if side == "away" else hr
                opp_r = hr if side == "away" else ar
                if opp_r <= our_r:
                    continue
                bp = bp_away if side == "away" else bp_home
                _, rq_note = _score_run_quality(scoring_plays, opp)
                rq, _  = _score_run_quality(scoring_plays, opp)
                sp, _  = _score_sp_status(state, side)
                bpa, _ = _score_bullpen_avail(state, bp, side)
                lu, _  = _score_lineup_due(state, side)
                poly_p = (market.get("polymarket") or {}).get(side)
                mp     = recalibrate_model_prob(nv.get(side, 0.5))
                pm, _  = _score_poly_misprice(poly_p, mp, side, {})
                sm, _  = _score_sharp_money(market.get("line_movement"), side)
                qs     = compute_quality_score(
                    {"run_quality": rq, "sp_status": sp, "bullpen_avail": bpa,
                     "lineup_due": lu, "poly_misprice": pm, "sharp_money": sm},
                    weights,
                )
                dashboard_in_progress.append({
                    "game_pk":       game_pk,
                    "label":         f"{state['away_team']} @ {state['home_team']}",
                    "score":         f"{ar}-{hr}",
                    "inning":        f"{'T' if top else 'B'}{inn}",
                    "pitcher":       cp_name,
                    "quality_score": qs,
                    "trailing":      side.upper(),
                })

            if state.get("abstract_state") == "Final":
                continue

            # Run full conviction analysis
            cycle_alerts = run_live_cycle(
                state, bp_away, bp_home, market, scoring_plays, weights
            )

            for alert in cycle_alerts:
                gp  = alert["game_pk"]
                bs  = alert["bet_side"]
                if _already_alerted(gp, bs):
                    continue
                _mark_alerted(gp, bs)

                # Format and send
                msg = _format_live_alert(
                    state         = alert["state"],
                    scores        = alert["scores"],
                    narratives    = alert["narratives"],
                    quality_score = alert["quality_score"],
                    conviction    = alert["conviction"],
                    poly_price    = alert["poly_price"],
                    model_prob    = alert["model_prob"],
                    best_odds     = alert["best_odds"],
                    best_book     = alert["best_book"],
                    bet_side      = bs,
                    stake         = alert["stake"],
                    bp            = alert["bp"],
                    how_runs_scored = alert["how_runs_scored"],
                )
                _send_telegram(msg)
                print(msg)

                # Log to DB
                alert_id = _log_alert(
                    game_pk         = gp,
                    game_label      = alert["game_label"],
                    bet_side        = bs,
                    inning          = alert["inning"],
                    deficit         = alert["deficit"],
                    quality_score   = alert["quality_score"],
                    scores          = alert["scores"],
                    poly_price      = alert["poly_price"],
                    model_prob      = alert["model_prob"],
                    entry_odds      = str(alert["best_odds"]),
                    stake           = alert["stake"],
                    conviction      = alert["conviction"],
                    how_runs_scored = alert["how_runs_scored"],
                )
                # Also keep compat with memory_engine live_bet_memory
                try:
                    record_live_bet(
                        str(gp), alert["team"],
                        datetime.now(ET).isoformat(),
                        str(alert["best_odds"]),
                        int(alert["quality_score"]),
                    )
                except Exception:
                    pass

                dashboard_alerts.append({
                    "game_pk":       gp,
                    "game":          alert["game_label"],
                    "side":          bs,
                    "inning":        alert["inning"],
                    "quality_score": alert["quality_score"],
                    "conviction":    alert["conviction"],
                    "odds":          alert["best_odds"],
                    "stake":         alert["stake"],
                })

                # Auto-learn check after logging
                _maybe_learn_weights()

        _update_dashboard(dashboard_in_progress, dashboard_alerts)
        time.sleep(CYCLE_SECS)


# ── PUBLIC RE-EXPORTS (backward compat with brain.py imports) ─────────────────

def run_live_cycle_compat(game_pk, away_code, home_code, away_bp, home_bp,
                           event_id, away_team, home_team, game_date) -> list:
    """Legacy wrapper kept for any callers using old run_live_cycle signature."""
    from market_engine import full_market_snapshot
    market        = full_market_snapshot(event_id, away_team, home_team, away_code, home_code, game_date)
    scoring_plays = _fetch_scoring_plays(game_pk)
    from constants import MLB_TEAM_MAP, MLB_TEAM_IDS
    state = {
        "game_pk": game_pk, "away_team": away_team, "home_team": home_team,
        "away_code": away_code, "home_code": home_code,
        "inning": 5, "top_inning": True, "outs": 0,
        "away_runs": 0, "home_runs": 0,
        "cur_pitcher_id": None, "cur_pitcher_name": "Unknown", "cur_batter_id": None,
        "away_order": [], "home_order": [],
        "away_sp": {"id": None, "name": "TBD", "np": 0, "still_in": False},
        "home_sp": {"id": None, "name": "TBD", "np": 0, "still_in": False},
        "away_pitchers": [], "home_pitchers": [],
        "runners": {}, "abstract_state": "Live",
    }
    return run_live_cycle(state, away_bp, home_bp, market, scoring_plays, _load_weights())


if __name__ == "__main__":
    run_live_monitor()
