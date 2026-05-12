"""PARLAY OS — memory_engine.py
Scout memory: every player, team, park, umpire, manager tracked since 2022.
Updates after every game. Self-improves after 50/100/200 bets.
"""

import json
import sqlite3
import logging
from datetime import date, timedelta, datetime
from math_engine import american_to_decimal

DB_PATH = "parlay_os.db"
log = logging.getLogger(__name__)


def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")    # corruption-resistant
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


# ── SCHEMA ────────────────────────────────────────────────────────────────────

def init_memory_tables():
    with _conn() as conn:
        conn.executescript("""
        -- Original calibration tables
        CREATE TABLE IF NOT EXISTS player_memory (
            player_name TEXT NOT NULL,
            stat_type   TEXT NOT NULL,
            date        TEXT NOT NULL,
            model_prob  REAL,
            actual      INTEGER,
            notes       TEXT,
            PRIMARY KEY (player_name, stat_type, date)
        );

        CREATE TABLE IF NOT EXISTS team_memory (
            team        TEXT NOT NULL,
            context     TEXT NOT NULL,
            date        TEXT NOT NULL,
            model_prob  REAL,
            actual      INTEGER,
            PRIMARY KEY (team, context, date)
        );

        CREATE TABLE IF NOT EXISTS model_calibration (
            bucket_lo   REAL NOT NULL,
            bucket_hi   REAL NOT NULL,
            hits        INTEGER DEFAULT 0,
            total       INTEGER DEFAULT 0,
            PRIMARY KEY (bucket_lo, bucket_hi)
        );

        CREATE TABLE IF NOT EXISTS live_bet_memory (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id     TEXT,
            team        TEXT,
            bet_time    TEXT,
            entry_odds  TEXT,
            gate_score  INTEGER,
            outcome     INTEGER
        );

        -- ── Deep pitcher profiles ─────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS pitcher_profiles (
            pitcher_name    TEXT NOT NULL,
            pitcher_id      INTEGER,
            stat_date       TEXT NOT NULL,
            era             REAL,
            xfip            REAL,
            fip             REAL,
            k9              REAL,
            bb9             REAL,
            hr9             REAL,
            whip            REAL,
            velocity_avg    REAL,
            spin_rate_avg   REAL,
            ip_season       REAL,
            gs_season       INTEGER,
            era_vs_lhh      REAL,
            era_vs_rhh      REAL,
            era_home        REAL,
            era_away        REAL,
            era_cold        REAL,
            era_hot         REAL,
            era_4day_rest   REAL,
            era_5day_rest   REAL,
            era_day_game    REAL,
            era_night_game  REAL,
            era_dome        REAL,
            era_outdoor     REAL,
            ttop_era_1      REAL,
            ttop_era_2      REAL,
            ttop_era_3plus  REAL,
            pitch_count_cliff INTEGER,
            raw_data        TEXT,
            PRIMARY KEY (pitcher_name, stat_date)
        );

        -- ── Deep hitter profiles ─────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS hitter_profiles (
            player_name     TEXT NOT NULL,
            player_id       INTEGER,
            team            TEXT,
            stat_date       TEXT NOT NULL,
            wrc_plus        REAL,
            woba            REAL,
            xwoba           REAL,
            babip           REAL,
            avg             REAL,
            obp             REAL,
            slg             REAL,
            iso             REAL,
            k_pct           REAL,
            bb_pct          REAL,
            barrel_pct      REAL,
            hard_pct        REAL,
            exit_velocity   REAL,
            launch_angle    REAL,
            sprint_speed    REAL,
            hot_streak_avg_games INTEGER,
            cold_streak_avg_games INTEGER,
            vs_fastball_wrc REAL,
            vs_slider_wrc   REAL,
            vs_curveball_wrc REAL,
            vs_changeup_wrc REAL,
            wrc_risp        REAL,
            wrc_high_lev    REAL,
            wrc_day         REAL,
            wrc_night       REAL,
            raw_data        TEXT,
            PRIMARY KEY (player_name, stat_date)
        );

        -- ── Bullpen memory ────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS bullpen_memory (
            team            TEXT NOT NULL,
            stat_date       TEXT NOT NULL,
            avg_xfip        REAL,
            era_7d          REAL,
            era_14d         REAL,
            high_lev_era    REAL,
            strand_rate     REAL,
            blown_save_rate REAL,
            closer_name     TEXT,
            closer_pitched_yesterday INTEGER DEFAULT 0,
            fatigue_score   REAL,
            fatigue_tier    TEXT,
            raw_data        TEXT,
            PRIMARY KEY (team, stat_date)
        );

        -- ── Umpire memory ─────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS umpire_memory (
            umpire_name     TEXT NOT NULL,
            season          INTEGER NOT NULL,
            games_worked    INTEGER DEFAULT 0,
            k_rate_9        REAL,
            bb_rate_9       REAL,
            runs_per_game   REAL,
            over_rate       REAL,
            home_win_rate   REAL,
            fps_rate        REAL,
            zone_size_sq_in REAL,
            raw_data        TEXT,
            PRIMARY KEY (umpire_name, season)
        );

        -- ── Ballpark memory ───────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS ballpark_memory (
            park_code       TEXT NOT NULL,
            season          INTEGER NOT NULL,
            run_factor      REAL DEFAULT 1.0,
            hr_factor       REAL DEFAULT 1.0,
            over_rate       REAL DEFAULT 0.50,
            avg_runs_game   REAL DEFAULT 8.7,
            wind_out_hr_boost REAL DEFAULT 0.0,
            wind_in_hr_reduce REAL DEFAULT 0.0,
            cold_under_rate REAL DEFAULT 0.0,
            hot_over_rate   REAL DEFAULT 0.0,
            raw_data        TEXT,
            PRIMARY KEY (park_code, season)
        );

        -- ── Manager memory ────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS manager_memory (
            manager_name    TEXT NOT NULL,
            team            TEXT NOT NULL,
            season          INTEGER NOT NULL,
            avg_sp_ip       REAL,
            quick_hook_rate REAL,
            steal_rate      REAL,
            phh_rate        REAL,
            ibb_rate        REAL,
            raw_data        TEXT,
            PRIMARY KEY (manager_name, team, season)
        );

        -- ── Factor reliability — tracks how accurate each feature is ──────
        CREATE TABLE IF NOT EXISTS factor_reliability (
            factor_name     TEXT NOT NULL,
            bet_type        TEXT NOT NULL,
            bets_evaluated  INTEGER DEFAULT 0,
            correct         INTEGER DEFAULT 0,
            last_updated    TEXT,
            PRIMARY KEY (factor_name, bet_type)
        );

        -- ── Post-game updates log ─────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS game_updates_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            game_pk         INTEGER,
            game_date       TEXT,
            updated_at      TEXT,
            away_team       TEXT,
            home_team       TEXT,
            away_score      INTEGER,
            home_score      INTEGER,
            away_sp         TEXT,
            home_sp         TEXT,
            umpire          TEXT,
            our_prediction  REAL,
            actual_outcome  INTEGER,
            factors_json    TEXT
        );

        -- ── Self-improvement schedule ─────────────────────────────────────
        CREATE TABLE IF NOT EXISTS improvement_schedule (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            trigger_type    TEXT,  -- 'n_bets', 'weekly', 'manual'
            trigger_value   INTEGER,
            last_run        TEXT,
            next_run        TEXT,
            status          TEXT DEFAULT 'pending',
            notes           TEXT
        );

        -- ── CLV analytics by dimension ────────────────────────────────────
        CREATE TABLE IF NOT EXISTS clv_analytics (
            dimension       TEXT NOT NULL,  -- 'team','sp','park','umpire','weather','bet_type'
            dimension_value TEXT NOT NULL,
            bets_total      INTEGER DEFAULT 0,
            bets_won        INTEGER DEFAULT 0,
            clv_sum         REAL DEFAULT 0.0,  -- sum of (closing_odds - entry_odds)
            avg_clv         REAL DEFAULT 0.0,
            win_rate        REAL DEFAULT 0.0,
            last_updated    TEXT,
            PRIMARY KEY (dimension, dimension_value)
        );

        -- ── Worst bets log (rolling 20) ───────────────────────────────────
        CREATE TABLE IF NOT EXISTS worst_bets_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            game_pk         INTEGER,
            game_date       TEXT,
            bet_side        TEXT,
            entry_odds      REAL,
            closing_odds    REAL,
            model_prob      REAL,
            clv             REAL,
            loss_amount     REAL DEFAULT 1.0,
            context_json    TEXT,
            factors_json    TEXT,
            logged_at       TEXT
        );

        -- ── Blind spots (recurring losing factor combos) ──────────────────
        CREATE TABLE IF NOT EXISTS blind_spots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            factor_combo    TEXT NOT NULL UNIQUE,  -- JSON sorted key list
            occurrence_count INTEGER DEFAULT 1,
            total_loss      REAL DEFAULT 0.0,
            first_detected  TEXT,
            last_detected   TEXT,
            suppressed      INTEGER DEFAULT 0
        );

        -- ── Model accuracy log by month and bet type ──────────────────────
        CREATE TABLE IF NOT EXISTS model_accuracy_log (
            month           TEXT NOT NULL,  -- 'YYYY-MM'
            bet_type        TEXT NOT NULL,  -- 'ML','total','runline','live'
            games           INTEGER DEFAULT 0,
            correct         INTEGER DEFAULT 0,
            accuracy        REAL DEFAULT 0.0,
            brier_score     REAL DEFAULT 0.0,
            avg_clv         REAL DEFAULT 0.0,
            logged_at       TEXT,
            PRIMARY KEY (month, bet_type)
        );
        """)

        # Seed calibration buckets
        buckets = [(i / 20, (i + 1) / 20) for i in range(20)]
        for lo, hi in buckets:
            conn.execute(
                "INSERT OR IGNORE INTO model_calibration (bucket_lo, bucket_hi) VALUES (?,?)",
                (round(lo, 2), round(hi, 2))
            )

        # Seed improvement schedule if empty
        conn.execute("""
            INSERT OR IGNORE INTO improvement_schedule (trigger_type, trigger_value, next_run)
            VALUES ('n_bets', 50, '2099-01-01')
        """)


# ── POST-GAME UPDATE ──────────────────────────────────────────────────────────

def post_game_update(
    game_pk: int,
    game_date: str,
    away_team: str,
    home_team: str,
    away_score: int,
    home_score: int,
    away_sp: str,
    home_sp: str,
    umpire: str,
    our_home_prob: float,
    factors: dict = None,
):
    """
    Called after every game completes.
    1. Log the result
    2. Update calibration
    3. Update factor reliability
    4. Check if self-improvement should trigger
    """
    actual_home_win = 1 if home_score > away_score else 0

    with _conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO game_updates_log
              (game_pk, game_date, updated_at, away_team, home_team,
               away_score, home_score, away_sp, home_sp, umpire,
               our_prediction, actual_outcome, factors_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            game_pk, game_date, datetime.utcnow().isoformat(),
            away_team, home_team, away_score, home_score,
            away_sp, home_sp, umpire,
            our_home_prob, actual_home_win,
            json.dumps(factors or {})
        ))

    # Update calibration
    _update_calibration_db(our_home_prob, actual_home_win)

    # Update factor reliability if factors provided
    if factors:
        _update_factor_reliability(factors, actual_home_win)

    # Check self-improvement triggers
    _check_improvement_triggers()

    log.info(f"Post-game update: {away_team}@{home_team} {away_score}-{home_score} "
             f"(home_win={actual_home_win}, pred={our_home_prob:.3f})")


def _update_calibration_db(model_prob: float, actual: int):
    if model_prob is None:
        return
    lo = round(int(model_prob * 20) / 20, 2)
    hi = round(lo + 0.05, 2)
    with _conn() as conn:
        conn.execute("""
            UPDATE model_calibration
            SET total = total + 1, hits = hits + ?
            WHERE bucket_lo=? AND bucket_hi=?
        """, (actual, lo, hi))


def _update_factor_reliability(factors: dict, actual_outcome: int):
    """Track which factors correctly predicted the outcome."""
    with _conn() as conn:
        for factor, predicted_positive in factors.items():
            correct = 1 if bool(predicted_positive) == bool(actual_outcome) else 0
            conn.execute("""
                INSERT INTO factor_reliability (factor_name, bet_type, bets_evaluated, correct, last_updated)
                VALUES (?, 'ML', 1, ?, ?)
                ON CONFLICT(factor_name, bet_type) DO UPDATE SET
                  bets_evaluated = bets_evaluated + 1,
                  correct = correct + ?,
                  last_updated = ?
            """, (factor, correct, datetime.utcnow().isoformat(),
                  correct, datetime.utcnow().isoformat()))


# ── ORIGINAL CALIBRATION FUNCTIONS (unchanged for compatibility) ──────────────

def record_player_result(player_name: str, stat_type: str, game_date: str,
                          model_prob: float, actual: int, notes: str = ""):
    with _conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO player_memory
            (player_name, stat_type, date, model_prob, actual, notes)
            VALUES (?,?,?,?,?,?)
        """, (player_name, stat_type, game_date, model_prob, actual, notes))
        _update_calibration(conn, model_prob, actual)


def record_team_result(team: str, context: str, game_date: str,
                        model_prob: float, actual: int):
    with _conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO team_memory
            (team, context, date, model_prob, actual)
            VALUES (?,?,?,?,?)
        """, (team, context, game_date, model_prob, actual))
        _update_calibration(conn, model_prob, actual)


def record_live_bet(game_id: str, team: str, bet_time: str,
                     entry_odds: str, gate_score: int):
    with _conn() as conn:
        conn.execute("""
            INSERT INTO live_bet_memory
            (game_id, team, bet_time, entry_odds, gate_score)
            VALUES (?,?,?,?,?)
        """, (game_id, team, bet_time, entry_odds, gate_score))


def resolve_live_bet(game_id: str, team: str, outcome: int):
    with _conn() as conn:
        conn.execute("""
            UPDATE live_bet_memory SET outcome=?
            WHERE game_id=? AND team=? AND outcome IS NULL
        """, (outcome, game_id, team))


def _update_calibration(conn, model_prob: float, actual: int):
    if model_prob is None:
        return
    lo = round(int(model_prob * 20) / 20, 2)
    hi = round(lo + 0.05, 2)
    conn.execute("""
        UPDATE model_calibration
        SET total = total + 1, hits = hits + ?
        WHERE bucket_lo=? AND bucket_hi=?
    """, (actual, lo, hi))


def calibration_summary() -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT bucket_lo, bucket_hi, hits, total FROM model_calibration ORDER BY bucket_lo"
        ).fetchall()
    result = []
    for r in rows:
        total = r["total"]
        if total == 0:
            continue
        result.append({
            "range":    f"{r['bucket_lo']:.0%}–{r['bucket_hi']:.0%}",
            "hits":     r["hits"],
            "total":    total,
            "hit_rate": round(r["hits"] / total, 3),
            "expected": round((r["bucket_lo"] + r["bucket_hi"]) / 2, 3),
        })
    return result


# ── PLAYER / TEAM PRIORS ─────────────────────────────────────────────────────

def player_prior(player_name: str, stat_type: str,
                 lookback_days: int = 30) -> float | None:
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    with _conn() as conn:
        row = conn.execute("""
            SELECT COUNT(*) as n, SUM(actual) as hits
            FROM player_memory
            WHERE player_name=? AND stat_type=? AND date >= ?
        """, (player_name, stat_type, cutoff)).fetchone()
    n, hits = (row["n"] if row else 0), (row["hits"] or 0)
    if n < 5:
        return None
    return round(hits / n, 3)


def team_prior(team: str, context: str,
               lookback_days: int = 30) -> float | None:
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    with _conn() as conn:
        row = conn.execute("""
            SELECT COUNT(*) as n, SUM(actual) as hits
            FROM team_memory
            WHERE team=? AND context=? AND date >= ?
        """, (team, context, cutoff)).fetchone()
    n, hits = (row["n"] if row else 0), (row["hits"] or 0)
    if n < 5:
        return None
    return round(hits / n, 3)


def adjust_model_prob(raw_prob: float, player_name: str = None, team: str = None,
                       stat_type: str = None, context: str = None,
                       prior_weight: float = 0.20) -> float:
    prior = None
    if player_name and stat_type:
        prior = player_prior(player_name, stat_type)
    elif team and context:
        prior = team_prior(team, context)
    if prior is None:
        return raw_prob
    adj = (1 - prior_weight) * raw_prob + prior_weight * prior
    return round(adj, 4)


# ── AUTO-CALIBRATION ──────────────────────────────────────────────────────────

def should_recalibrate() -> bool:
    with _conn() as conn:
        total_samples = conn.execute(
            "SELECT SUM(total) FROM model_calibration"
        ).fetchone()[0] or 0
    return total_samples >= 50


def calibration_multiplier(model_prob: float) -> float:
    lo = round(int(model_prob * 20) / 20, 2)
    hi = round(lo + 0.05, 2)
    with _conn() as conn:
        row = conn.execute("""
            SELECT hits, total FROM model_calibration
            WHERE bucket_lo=? AND bucket_hi=?
        """, (lo, hi)).fetchone()
    if not row or row["total"] < 10:
        return 1.0
    expected_mid = (lo + hi) / 2
    actual_rate  = row["hits"] / row["total"]
    if expected_mid <= 0:
        return 1.0
    return round(actual_rate / expected_mid, 4)


def recalibrate_model_prob(raw_prob: float) -> float:
    if not should_recalibrate():
        return raw_prob
    mult = calibration_multiplier(raw_prob)
    mult = max(0.70, min(mult, 1.30))
    return round(min(raw_prob * mult, 0.95), 4)


# ── PITCHER PROFILE STORE / RETRIEVE ─────────────────────────────────────────

def upsert_pitcher_profile(name: str, stat_date: str, data: dict):
    with _conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO pitcher_profiles
              (pitcher_name, pitcher_id, stat_date,
               era, xfip, fip, k9, bb9, hr9, whip,
               velocity_avg, spin_rate_avg, ip_season, gs_season,
               era_vs_lhh, era_vs_rhh, era_home, era_away,
               era_cold, era_hot, era_4day_rest, era_5day_rest,
               era_day_game, era_night_game, era_dome, era_outdoor,
               ttop_era_1, ttop_era_2, ttop_era_3plus, pitch_count_cliff,
               raw_data)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            name, data.get("pitcher_id"),
            stat_date,
            data.get("era"), data.get("xfip"), data.get("fip"),
            data.get("k9"), data.get("bb9"), data.get("hr9"), data.get("whip"),
            data.get("velocity_avg"), data.get("spin_rate_avg"),
            data.get("ip_season"), data.get("gs_season"),
            data.get("era_vs_lhh"), data.get("era_vs_rhh"),
            data.get("era_home"), data.get("era_away"),
            data.get("era_cold"), data.get("era_hot"),
            data.get("era_4day_rest"), data.get("era_5day_rest"),
            data.get("era_day_game"), data.get("era_night_game"),
            data.get("era_dome"), data.get("era_outdoor"),
            data.get("ttop_era_1"), data.get("ttop_era_2"), data.get("ttop_era_3plus"),
            data.get("pitch_count_cliff"),
            json.dumps(data),
        ))


def get_pitcher_profile(name: str) -> dict | None:
    with _conn() as conn:
        row = conn.execute("""
            SELECT * FROM pitcher_profiles
            WHERE pitcher_name=?
            ORDER BY stat_date DESC LIMIT 1
        """, (name,)).fetchone()
    if not row:
        return None
    d = dict(row)
    try:
        d.update(json.loads(d.get("raw_data") or "{}"))
    except Exception:
        pass
    return d


# ── HITTER PROFILE STORE / RETRIEVE ──────────────────────────────────────────

def upsert_hitter_profile(name: str, stat_date: str, data: dict):
    with _conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO hitter_profiles
              (player_name, player_id, team, stat_date,
               wrc_plus, woba, xwoba, babip, avg, obp, slg, iso,
               k_pct, bb_pct, barrel_pct, hard_pct,
               exit_velocity, launch_angle, sprint_speed,
               hot_streak_avg_games, cold_streak_avg_games,
               vs_fastball_wrc, vs_slider_wrc, vs_curveball_wrc, vs_changeup_wrc,
               wrc_risp, wrc_high_lev, wrc_day, wrc_night, raw_data)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            name, data.get("player_id"), data.get("team"), stat_date,
            data.get("wrc_plus"), data.get("woba"), data.get("xwoba"),
            data.get("babip"), data.get("avg"), data.get("obp"), data.get("slg"), data.get("iso"),
            data.get("k_pct"), data.get("bb_pct"), data.get("barrel_pct"), data.get("hard_pct"),
            data.get("exit_velocity"), data.get("launch_angle"), data.get("sprint_speed"),
            data.get("hot_streak_avg_games"), data.get("cold_streak_avg_games"),
            data.get("vs_fastball_wrc"), data.get("vs_slider_wrc"),
            data.get("vs_curveball_wrc"), data.get("vs_changeup_wrc"),
            data.get("wrc_risp"), data.get("wrc_high_lev"),
            data.get("wrc_day"), data.get("wrc_night"),
            json.dumps(data),
        ))


def get_hitter_profile(name: str) -> dict | None:
    with _conn() as conn:
        row = conn.execute("""
            SELECT * FROM hitter_profiles
            WHERE player_name=?
            ORDER BY stat_date DESC LIMIT 1
        """, (name,)).fetchone()
    if not row:
        return None
    d = dict(row)
    try:
        d.update(json.loads(d.get("raw_data") or "{}"))
    except Exception:
        pass
    return d


# ── FACTOR RELIABILITY REPORT ─────────────────────────────────────────────────

def factor_reliability_report() -> list[dict]:
    """Which factors have actually been predictive?"""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT factor_name, bet_type, bets_evaluated, correct,
                   ROUND(CAST(correct AS REAL) / MAX(bets_evaluated, 1), 3) as accuracy
            FROM factor_reliability
            WHERE bets_evaluated >= 10
            ORDER BY accuracy DESC
        """).fetchall()
    return [dict(r) for r in rows]


# ── MEMORY REPORT (expanded) ──────────────────────────────────────────────────

def memory_report() -> dict:
    cal = calibration_summary()
    overcal  = [b for b in cal if b["hit_rate"] < b["expected"] - 0.08]
    undercal = [b for b in cal if b["hit_rate"] > b["expected"] + 0.08]

    with _conn() as conn:
        n_player  = conn.execute("SELECT COUNT(*) FROM player_memory").fetchone()[0]
        n_team    = conn.execute("SELECT COUNT(*) FROM team_memory").fetchone()[0]
        n_live    = conn.execute(
            "SELECT COUNT(*) FROM live_bet_memory WHERE outcome IS NOT NULL"
        ).fetchone()[0]
        live_wins = conn.execute(
            "SELECT SUM(outcome) FROM live_bet_memory WHERE outcome IS NOT NULL"
        ).fetchone()[0] or 0
        n_pitchers = conn.execute("SELECT COUNT(DISTINCT pitcher_name) FROM pitcher_profiles").fetchone()[0]
        n_hitters  = conn.execute("SELECT COUNT(DISTINCT player_name) FROM hitter_profiles").fetchone()[0]
        n_games    = conn.execute("SELECT COUNT(*) FROM game_updates_log").fetchone()[0]
        n_factors  = conn.execute(
            "SELECT COUNT(*) FROM factor_reliability WHERE bets_evaluated >= 10"
        ).fetchone()[0]

    return {
        "calibration_buckets":       len(cal),
        "overconfident_buckets":     len(overcal),
        "underconfident_buckets":    len(undercal),
        "player_records":            n_player,
        "team_records":              n_team,
        "live_bets_resolved":        n_live,
        "live_win_rate":             round(live_wins / n_live, 3) if n_live else None,
        "pitcher_profiles":          n_pitchers,
        "hitter_profiles":           n_hitters,
        "games_tracked":             n_games,
        "reliable_factors":          n_factors,
        "ready_to_recalibrate":      should_recalibrate(),
    }


# ── WEEKLY SELF-IMPROVEMENT ───────────────────────────────────────────────────

def weekly_accuracy_report() -> dict:
    """
    Generate weekly accuracy report.
    Compares our predictions to actual outcomes from game_updates_log.
    """
    with _conn() as conn:
        rows = conn.execute("""
            SELECT our_prediction, actual_outcome
            FROM game_updates_log
            WHERE game_date >= date('now', '-7 days')
              AND our_prediction IS NOT NULL
              AND actual_outcome IS NOT NULL
        """).fetchall()

    if not rows:
        return {"status": "no_data", "games": 0}

    games  = len(rows)
    correct = sum(
        1 for r in rows
        if (r["our_prediction"] >= 0.5) == bool(r["actual_outcome"])
    )
    brier  = sum(
        (r["our_prediction"] - r["actual_outcome"]) ** 2
        for r in rows
    ) / games

    return {
        "status":    "ok",
        "games":     games,
        "accuracy":  round(correct / games, 3),
        "brier":     round(brier, 4),
        "verdict": (
            "STRONG" if correct / games > 0.58 else
            "POSITIVE" if correct / games > 0.54 else
            "NEUTRAL" if correct / games > 0.50 else
            "NEEDS_IMPROVEMENT"
        ),
    }


def should_retrain_ml() -> bool:
    """True if we have enough new games since last ML training."""
    with _conn() as conn:
        n_new = conn.execute("""
            SELECT COUNT(*) FROM game_updates_log
            WHERE updated_at > COALESCE(
                (SELECT last_run FROM improvement_schedule
                 WHERE trigger_type='n_bets' LIMIT 1),
                '2020-01-01'
            )
        """).fetchone()[0]
    return n_new >= 200


# ── CLV ANALYTICS ─────────────────────────────────────────────────────────────

def update_clv_analytics(
    entry_odds: float,
    closing_odds: float,
    won: bool,
    dimensions: dict,
):
    """
    Record one resolved bet across all dimensions.
    dimensions: {'team': 'NYY', 'sp': 'Cole', 'park': 'NYY', 'umpire': 'Meals',
                 'weather': 'rain', 'bet_type': 'ML'}
    """
    clv = closing_odds - entry_odds
    won_int = 1 if won else 0
    now = datetime.utcnow().isoformat()

    with _conn() as conn:
        for dim, val in dimensions.items():
            if not val:
                continue
            conn.execute("""
                INSERT INTO clv_analytics
                  (dimension, dimension_value, bets_total, bets_won, clv_sum,
                   avg_clv, win_rate, last_updated)
                VALUES (?,?,1,?,?,?,?,?)
                ON CONFLICT(dimension, dimension_value) DO UPDATE SET
                  bets_total  = bets_total + 1,
                  bets_won    = bets_won + ?,
                  clv_sum     = clv_sum + ?,
                  avg_clv     = (clv_sum + ?) / (bets_total + 1),
                  win_rate    = CAST(bets_won + ? AS REAL) / (bets_total + 1),
                  last_updated = ?
            """, (
                dim, str(val), won_int, clv, clv, now,
                won_int, clv, clv, won_int, now,
            ))


def get_clv_by_dimension(dimension: str, min_bets: int = 10) -> list[dict]:
    """Return CLV leaderboard for a dimension (e.g. 'team', 'park')."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT dimension_value, bets_total, bets_won, avg_clv, win_rate
            FROM clv_analytics
            WHERE dimension=? AND bets_total >= ?
            ORDER BY avg_clv DESC
        """, (dimension, min_bets)).fetchall()
    return [dict(r) for r in rows]


# ── WORST BETS LOG ────────────────────────────────────────────────────────────

def log_worst_bet(
    game_pk: int,
    game_date: str,
    bet_side: str,
    entry_odds: float,
    closing_odds: float,
    model_prob: float,
    loss_amount: float = 1.0,
    context: dict = None,
    factors: dict = None,
):
    """
    Record a losing bet. Keeps only 20 most recent rows.
    Called after a loss is confirmed by clv_tracker.
    """
    clv = closing_odds - entry_odds if closing_odds else 0.0
    now = datetime.utcnow().isoformat()

    with _conn() as conn:
        conn.execute("""
            INSERT INTO worst_bets_log
              (game_pk, game_date, bet_side, entry_odds, closing_odds,
               model_prob, clv, loss_amount, context_json, factors_json, logged_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            game_pk, game_date, bet_side, entry_odds, closing_odds,
            model_prob, clv, loss_amount,
            json.dumps(context or {}), json.dumps(factors or {}), now,
        ))
        # Keep rolling 20
        conn.execute("""
            DELETE FROM worst_bets_log
            WHERE id NOT IN (
                SELECT id FROM worst_bets_log ORDER BY logged_at DESC LIMIT 20
            )
        """)

    # Immediately check for blind spots after each loss
    if factors:
        _record_blind_spot(factors)


def _record_blind_spot(factors: dict):
    """Increment blind spot counter for this factor combination."""
    active = sorted(k for k, v in factors.items() if v)
    if len(active) < 2:
        return
    combo_key = json.dumps(active)
    now = datetime.utcnow().isoformat()
    with _conn() as conn:
        conn.execute("""
            INSERT INTO blind_spots (factor_combo, occurrence_count, first_detected, last_detected)
            VALUES (?,1,?,?)
            ON CONFLICT(factor_combo) DO UPDATE SET
              occurrence_count = occurrence_count + 1,
              last_detected    = ?
        """, (combo_key, now, now, now))


def detect_blind_spots(min_occurrences: int = 3) -> list[dict]:
    """Return factor combos that appear ≥ min_occurrences times in worst bets."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT factor_combo, occurrence_count, last_detected
            FROM blind_spots
            WHERE occurrence_count >= ? AND suppressed=0
            ORDER BY occurrence_count DESC
        """, (min_occurrences,)).fetchall()
    result = []
    for r in rows:
        result.append({
            "factors":    json.loads(r["factor_combo"]),
            "occurrences": r["occurrence_count"],
            "last_seen":   r["last_detected"],
        })
    return result


def get_worst_bets(n: int = 20) -> list[dict]:
    with _conn() as conn:
        rows = conn.execute("""
            SELECT * FROM worst_bets_log ORDER BY logged_at DESC LIMIT ?
        """, (n,)).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["context"] = json.loads(d.get("context_json") or "{}")
            d["factors"] = json.loads(d.get("factors_json") or "{}")
        except Exception:
            pass
        result.append(d)
    return result


# ── MODEL ACCURACY LOG ────────────────────────────────────────────────────────

def log_monthly_accuracy(bet_type: str = "ML"):
    """
    Aggregate current month's game_updates_log into model_accuracy_log.
    Call at end of each day or after audits.
    """
    month = datetime.utcnow().strftime("%Y-%m")
    now   = datetime.utcnow().isoformat()

    with _conn() as conn:
        rows = conn.execute("""
            SELECT our_prediction, actual_outcome
            FROM game_updates_log
            WHERE strftime('%Y-%m', game_date) = ?
              AND our_prediction IS NOT NULL
              AND actual_outcome IS NOT NULL
        """, (month,)).fetchall()

    if not rows:
        return

    games   = len(rows)
    correct = sum(1 for r in rows if (r["our_prediction"] >= 0.5) == bool(r["actual_outcome"]))
    brier   = sum((r["our_prediction"] - r["actual_outcome"]) ** 2 for r in rows) / games
    acc     = round(correct / games, 4)

    with _conn() as conn:
        conn.execute("""
            INSERT INTO model_accuracy_log
              (month, bet_type, games, correct, accuracy, brier_score, logged_at)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(month, bet_type) DO UPDATE SET
              games       = ?,
              correct     = ?,
              accuracy    = ?,
              brier_score = ?,
              logged_at   = ?
        """, (month, bet_type, games, correct, acc, round(brier, 5), now,
              games, correct, acc, round(brier, 5), now))

    log.info(f"[accuracy_log] {month} {bet_type}: {acc:.1%} over {games} games (Brier={brier:.4f})")


def get_accuracy_trend(bet_type: str = "ML", months: int = 6) -> list[dict]:
    """Return accuracy by month for the last N months."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT month, games, accuracy, brier_score
            FROM model_accuracy_log
            WHERE bet_type=?
            ORDER BY month DESC LIMIT ?
        """, (bet_type, months)).fetchall()
    return [dict(r) for r in rows]


# ── SELF-IMPROVEMENT LOOP ──────────────────────────────────────────────────────

def run_50_bet_audit() -> dict:
    """
    Audit after 50 bets: factor reliability report + accuracy snapshot.
    Identifies overweighted/underweighted factors.
    """
    log.info("[improvement] Running 50-bet factor audit...")
    reliability = factor_reliability_report()
    accuracy    = weekly_accuracy_report()
    blind       = detect_blind_spots(min_occurrences=2)

    overweight  = [f for f in reliability if f["accuracy"] < 0.48 and f["bets_evaluated"] >= 15]
    underweight = [f for f in reliability if f["accuracy"] > 0.62 and f["bets_evaluated"] >= 15]

    report = {
        "audit_type":         "50_bet",
        "run_at":             datetime.utcnow().isoformat(),
        "weekly_accuracy":    accuracy,
        "overweighted":       [f["factor_name"] for f in overweight],
        "underweighted":      [f["factor_name"] for f in underweight],
        "blind_spots":        blind,
        "total_factors":      len(reliability),
    }
    _log_improvement_run("50_bet_audit", report)
    log.info(f"[improvement] 50-bet audit: {len(overweight)} over, {len(underweight)} under, "
             f"{len(blind)} blind spots")
    return report


def run_100_bet_retrain() -> dict:
    """
    After 100 bets: retrain ML with new data + recalibrate Platt scaling.
    Also logs monthly accuracy and updates CLV analytics summary.
    """
    log.info("[improvement] Running 100-bet ML recalibration...")
    log_monthly_accuracy("ML")

    # Attempt ML retrain if available
    retrained = False
    try:
        from ml_model import train_all
        train_all()
        retrained = True
        log.info("[improvement] ML retrained successfully")
    except Exception as e:
        log.warning(f"[improvement] ML retrain skipped: {e}")

    accuracy = weekly_accuracy_report()
    cal_data = calibration_summary()

    report = {
        "audit_type":    "100_bet",
        "run_at":        datetime.utcnow().isoformat(),
        "ml_retrained":  retrained,
        "accuracy":      accuracy,
        "calibration":   cal_data[:5],
    }
    _log_improvement_run("100_bet_retrain", report)
    return report


def run_200_bet_retrain() -> dict:
    """
    After 200 bets: full deep retrain + rebuild factor weights + audit blind spots.
    """
    log.info("[improvement] Running 200-bet FULL retrain...")
    report_100 = run_100_bet_retrain()
    audit_50   = run_50_bet_audit()

    worst = get_worst_bets(20)
    blind = detect_blind_spots(min_occurrences=3)

    report = {
        "audit_type":    "200_bet",
        "run_at":        datetime.utcnow().isoformat(),
        "retrain":       report_100,
        "factor_audit":  audit_50,
        "worst_bets":    len(worst),
        "blind_spots":   blind,
        "recommendation": _generate_improvement_rec(audit_50, blind),
    }
    _log_improvement_run("200_bet_full", report)
    log.info(f"[improvement] 200-bet full retrain complete: {report['recommendation']}")
    return report


def _generate_improvement_rec(audit: dict, blind_spots: list) -> str:
    lines = []
    if audit.get("overweighted"):
        lines.append(f"Reduce weight on: {', '.join(audit['overweighted'][:3])}")
    if audit.get("underweighted"):
        lines.append(f"Increase weight on: {', '.join(audit['underweighted'][:3])}")
    if blind_spots:
        combos = ["+".join(b["factors"][:3]) for b in blind_spots[:2]]
        lines.append(f"Avoid combo: {', '.join(combos)}")
    return " | ".join(lines) if lines else "No structural changes needed"


def _log_improvement_run(run_type: str, report: dict):
    now = datetime.utcnow().isoformat()
    with _conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO improvement_schedule
              (trigger_type, trigger_value, last_run, status, notes)
            VALUES (?, 0, ?, 'completed', ?)
        """, (run_type, now, json.dumps(report)[:2000]))


def _check_improvement_triggers():
    """Check if we should retrain / recalibrate based on game count."""
    with _conn() as conn:
        total = conn.execute(
            "SELECT SUM(total) FROM model_calibration"
        ).fetchone()[0] or 0

        row = conn.execute(
            "SELECT * FROM improvement_schedule WHERE trigger_type='n_bets' LIMIT 1"
        ).fetchone()
        if not row:
            return

        trigger_val = row["trigger_value"]
        if total >= trigger_val:
            log.info(f"Improvement trigger: {total} bets >= {trigger_val}")
            next_val = trigger_val + 50
            conn.execute("""
                UPDATE improvement_schedule
                SET trigger_value=?, last_run=?, status='triggered', next_run='pending'
                WHERE trigger_type='n_bets'
            """, (next_val, datetime.utcnow().isoformat()))

            # Fire the right audit based on milestone
            if trigger_val % 200 == 0:
                run_200_bet_retrain()
            elif trigger_val % 100 == 0:
                run_100_bet_retrain()
            else:
                run_50_bet_audit()


# ── WEEKLY SELF-IMPROVEMENT (Sunday 2am) ─────────────────────────────────────

def run_weekly_maintenance() -> dict:
    """
    Full weekly maintenance pass. Called by scheduler every Sunday ~2am ET.
    """
    log.info("[maintenance] Starting weekly maintenance...")
    log_monthly_accuracy("ML")
    accuracy  = weekly_accuracy_report()
    cal       = calibration_summary()
    blind     = detect_blind_spots(min_occurrences=2)
    factors   = factor_reliability_report()

    # Rebuild CLV summaries
    clv_teams = get_clv_by_dimension("team", min_bets=5)
    clv_parks = get_clv_by_dimension("park", min_bets=5)
    clv_umps  = get_clv_by_dimension("umpire", min_bets=5)

    report = {
        "run_at":       datetime.utcnow().isoformat(),
        "accuracy":     accuracy,
        "blind_spots":  blind,
        "top_factors":  factors[:5],
        "clv_leaders": {
            "teams":   clv_teams[:3],
            "parks":   clv_parks[:3],
            "umpires": clv_umps[:3],
        },
        "calibration_ok": len([b for b in cal if abs(b["hit_rate"] - b["expected"]) > 0.10]) == 0,
    }
    _log_improvement_run("weekly_maintenance", report)
    log.info(f"[maintenance] Weekly done: accuracy={accuracy.get('accuracy','?')}, "
             f"{len(blind)} blind spots")
    return report


if __name__ == "__main__":
    init_memory_tables()
    print("Memory tables initialized")
    r = memory_report()
    for k, v in r.items():
        print(f"  {k}: {v}")
