"""PARLAY OS — confidence_engine.py
LogisticRegression confidence scoring with heuristic fallback.

Falls back to heuristic until ≥ 20 settled bets available for training.
Retrains every Sunday 2am ET (called by scheduler).

Exports:
    get_confidence_score(features)  → int 0–100
    retrain_confidence_model()      → dict (report)
    build_bet_features(analysis, side) → dict
"""

import json
import logging
import threading
from datetime import datetime

log = logging.getLogger(__name__)

MIN_BETS_TO_TRAIN = 20

# ── Feature spec ──────────────────────────────────────────────────────────────

# Full 15-feature set used at inference time (passed from brain.py)
FEATURE_NAMES = [
    "edge_pct",            # 0–100
    "model_prob",          # 0–1
    "sp_xfip_our",         # xFIP of OUR team's SP (lower = better for us)
    "sp_xfip_opp",         # xFIP of OPPONENT's SP (higher = better for us)
    "bullpen_fatigue_our", # fatigue score (lower = better)
    "bullpen_fatigue_opp", # fatigue score of opponent (higher = better for us)
    "momentum_our",        # -10 to +10
    "momentum_opp",        # -10 to +10
    "lineup_confirmed",    # 0/1
    "sharp_signal",        # 0/1 — sharp money on our side
    "lm_direction",        # +1 toward us, -1 against, 0 stable
    "abs_score_sp",        # 0–100, 50 if unknown
    "home_dog_angle",      # 0/1
    "ump_home_win_adj",    # float, umpire's home win bias
    "key_reliever_avail",  # 0/1 — our key reliever available
]

FEATURE_DEFAULTS = {
    "edge_pct":            3.0,
    "model_prob":          0.52,
    "sp_xfip_our":         4.35,
    "sp_xfip_opp":         4.35,
    "bullpen_fatigue_our": 4.0,
    "bullpen_fatigue_opp": 4.0,
    "momentum_our":        0.0,
    "momentum_opp":        0.0,
    "lineup_confirmed":    1,
    "sharp_signal":        0,
    "lm_direction":        0,
    "abs_score_sp":        50.0,
    "home_dog_angle":      0,
    "ump_home_win_adj":    0.0,
    "key_reliever_avail":  1,
}

# Thread-safe cache of the loaded model
_model_lock  = threading.Lock()
_model_cache: dict | None = None    # None = not loaded yet; {} = loaded (may be heuristic-only)
_last_loaded: str = ""              # ISO date of last load


# ── DB helpers ────────────────────────────────────────────────────────────────

def _conn():
    import sqlite3, os
    db = os.environ.get("PARLAY_DB", "parlay_os.db")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _ensure_table():
    with _conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS confidence_weights (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                trained_at    TEXT NOT NULL,
                n_bets        INTEGER,
                feature_names TEXT,
                coefficients  TEXT,
                intercept     REAL,
                accuracy      REAL,
                training_log  TEXT
            )
        """)


def _save_weights(n_bets: int, coefs: list, intercept: float,
                  accuracy: float, training_log: str = "") -> None:
    _ensure_table()
    now = datetime.utcnow().isoformat()
    with _conn() as conn:
        conn.execute("""
            INSERT INTO confidence_weights
              (trained_at, n_bets, feature_names, coefficients, intercept, accuracy, training_log)
            VALUES (?,?,?,?,?,?,?)
        """, (now, n_bets, json.dumps(FEATURE_NAMES),
              json.dumps([round(c, 6) for c in coefs]),
              round(intercept, 6), round(accuracy, 4), training_log))


def _load_latest_weights() -> dict | None:
    """Return the most recent trained weights as a dict, or None."""
    try:
        _ensure_table()
        with _conn() as conn:
            row = conn.execute("""
                SELECT * FROM confidence_weights ORDER BY trained_at DESC LIMIT 1
            """).fetchone()
        if not row:
            return None
        return {
            "n_bets":       row["n_bets"],
            "trained_at":   row["trained_at"],
            "feature_names": json.loads(row["feature_names"] or "[]"),
            "coefficients":  json.loads(row["coefficients"] or "[]"),
            "intercept":     row["intercept"] or 0.0,
            "accuracy":      row["accuracy"] or 0.0,
        }
    except Exception as e:
        log.debug(f"[confidence] load_weights failed: {e}")
        return None


# ── Training data extraction ──────────────────────────────────────────────────

def _get_settled_bets() -> list[dict]:
    """Pull all settled bets (W/L) with available feature columns."""
    try:
        with _conn() as conn:
            rows = conn.execute("""
                SELECT edge_pct, model_prob, result,
                       home_dog_angle, sharp_signal, umpire_edge,
                       sp_gb_rate, first_pitch_strike_rate
                FROM bets
                WHERE result IN ('W', 'L')
                  AND edge_pct IS NOT NULL
                  AND model_prob IS NOT NULL
            """).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        log.debug(f"[confidence] settled_bets query failed: {e}")
        return []


def _extract_training_row(bet: dict) -> tuple[list[float], int] | None:
    """Build partial feature vector + label from a settled bet. Returns None on error."""
    try:
        edge     = float(bet.get("edge_pct") or 3.0)
        model_p  = float(bet.get("model_prob") or 0.52)
        home_dog = int(bool(bet.get("home_dog_angle")))
        sharp    = 1 if bet.get("sharp_signal") else 0

        ump_adj = 0.0
        ump_raw = bet.get("umpire_edge") or ""
        try:
            if ump_raw:
                ump_data = json.loads(ump_raw)
                ump_adj = float(ump_data.get("home_win_adj", 0.0))
        except Exception:
            pass

        label = 1 if bet.get("result") == "W" else 0
        features = [edge, model_p, home_dog, sharp, ump_adj]
        return features, label
    except Exception as e:
        log.debug(f"[confidence] row extraction failed: {e}")
        return None


# ── Training ──────────────────────────────────────────────────────────────────

def retrain_confidence_model() -> dict:
    """
    Train LogisticRegression on settled bets.
    Returns report dict. Saves weights to DB if successful.
    Called Sunday 2am ET by scheduler.
    """
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler
        import numpy as np
    except ImportError:
        log.warning("[confidence] sklearn not available — skipping retrain")
        return {"status": "sklearn_unavailable", "n_bets": 0}

    bets = _get_settled_bets()
    if not bets:
        return {"status": "no_data", "n_bets": 0}

    rows = [_extract_training_row(b) for b in bets]
    rows = [r for r in rows if r is not None]
    n    = len(rows)

    if n < MIN_BETS_TO_TRAIN:
        return {"status": "insufficient_data", "n_bets": n,
                "need": MIN_BETS_TO_TRAIN - n}

    X = np.array([r[0] for r in rows], dtype=float)
    y = np.array([r[1] for r in rows], dtype=int)

    # Normalize
    scaler = StandardScaler()
    X_s    = scaler.fit_transform(X)

    model = LogisticRegression(max_iter=500, C=1.0, random_state=42)
    model.fit(X_s, y)

    acc = round(float((model.predict(X_s) == y).mean()), 4)

    # Coefs map back to the partial feature list used in training
    training_feature_names = ["edge_pct", "model_prob", "home_dog_angle",
                               "sharp_signal", "ump_home_win_adj"]
    coefs  = model.coef_[0].tolist()
    intcpt = float(model.intercept_[0])

    # Pad coefs to full FEATURE_NAMES length (pad unseen features with 0)
    full_coefs = [0.0] * len(FEATURE_NAMES)
    for i, fname in enumerate(training_feature_names):
        if fname in FEATURE_NAMES:
            full_coefs[FEATURE_NAMES.index(fname)] = coefs[i]

    training_log = json.dumps({
        "training_features": training_feature_names,
        "scaler_mean":  scaler.mean_.tolist(),
        "scaler_scale": scaler.scale_.tolist(),
        "n_bets":       n,
        "accuracy":     acc,
    })

    _save_weights(n, full_coefs, intcpt, acc, training_log)

    # Invalidate cache so next call re-loads
    global _model_cache, _last_loaded
    with _model_lock:
        _model_cache  = None
        _last_loaded  = ""

    log.info(f"[confidence] Model retrained: {n} bets, accuracy={acc:.1%}")
    return {
        "status":       "trained",
        "n_bets":       n,
        "accuracy":     acc,
        "coefficients": {FEATURE_NAMES[i]: round(full_coefs[i], 4) for i in range(len(FEATURE_NAMES))},
    }


# ── Heuristic scoring ─────────────────────────────────────────────────────────

def _heuristic_score(features: dict) -> int:
    """
    Rule-based 0–100 confidence score used when no trained model is available
    or as a blend component.
    """
    score = 50.0  # baseline

    # Edge strength: each 1% of edge above 3% = +3 pts (cap at +30)
    edge = float(features.get("edge_pct", 3.0))
    score += min((edge - 3.0) * 3.0, 30.0)

    # Model prob distance from 0.5: 0.50→0, 0.60→+10, 0.65→+15 (cap +15)
    model_p = float(features.get("model_prob", 0.52))
    score  += min(abs(model_p - 0.5) * 150.0, 15.0)

    # Lineup confirmed: +8
    if int(features.get("lineup_confirmed", 1)):
        score += 8.0

    # Sharp money on our side: +10
    if int(features.get("sharp_signal", 0)):
        score += 10.0

    # Line movement against us: -10; toward us: +5
    lm = int(features.get("lm_direction", 0))
    if lm == -1:
        score -= 10.0
    elif lm == 1:
        score += 5.0

    # Home dog angle (structural edge): +6
    if int(features.get("home_dog_angle", 0)):
        score += 6.0

    # Key reliever available: +4
    if int(features.get("key_reliever_avail", 1)):
        score += 4.0

    # ABS score: only matters if far from neutral
    abs_s = float(features.get("abs_score_sp", 50.0))
    if abs_s >= 70:
        score += 5.0
    elif abs_s <= 30:
        score -= 5.0

    # SP quality gap: our SP xFIP vs opp SP xFIP
    xfip_our = float(features.get("sp_xfip_our", 4.35))
    xfip_opp = float(features.get("sp_xfip_opp", 4.35))
    xfip_gap = xfip_opp - xfip_our   # positive = our SP better
    if xfip_gap >= 1.0:
        score += 6.0
    elif xfip_gap >= 0.5:
        score += 3.0
    elif xfip_gap <= -1.0:
        score -= 6.0

    # Momentum: our momentum vs opp
    mom_our = float(features.get("momentum_our", 0.0))
    mom_opp = float(features.get("momentum_opp", 0.0))
    mom_net = mom_our - mom_opp
    score  += min(max(mom_net * 1.5, -8.0), 8.0)

    # Umpire home win adj
    ump_adj = float(features.get("ump_home_win_adj", 0.0))
    if abs(ump_adj) >= 0.02:
        score += ump_adj * 50.0   # 0.02 → +1.0 pts

    return int(min(max(round(score), 0), 100))


# ── LogReg inference ──────────────────────────────────────────────────────────

def _logreg_score(features: dict, weights: dict) -> int | None:
    """Apply loaded LogReg weights to a feature dict. Returns 0-100 or None on error."""
    try:
        coefs     = weights.get("coefficients", [])
        intercept = weights.get("intercept", 0.0)
        if not coefs or len(coefs) != len(FEATURE_NAMES):
            return None

        dot = intercept
        for i, fname in enumerate(FEATURE_NAMES):
            val = float(features.get(fname, FEATURE_DEFAULTS.get(fname, 0.0)))
            dot += coefs[i] * val

        # Sigmoid → probability → 0-100
        import math
        prob = 1.0 / (1.0 + math.exp(-dot))
        return int(min(max(round(prob * 100), 0), 100))
    except Exception as e:
        log.debug(f"[confidence] logreg_score failed: {e}")
        return None


# ── Public API ────────────────────────────────────────────────────────────────

def _get_model() -> dict | None:
    """Return cached model weights, loading from DB if needed. Thread-safe."""
    global _model_cache, _last_loaded
    today = datetime.utcnow().strftime("%Y-%m-%d")
    with _model_lock:
        if _model_cache is not None and _last_loaded == today:
            return _model_cache if _model_cache else None
        weights = _load_latest_weights()
        if weights and weights.get("n_bets", 0) >= MIN_BETS_TO_TRAIN:
            _model_cache = weights
        else:
            _model_cache = {}   # empty dict = "checked but no valid model"
        _last_loaded = today
        return _model_cache if _model_cache else None


def get_confidence_score(features: dict) -> int:
    """
    Main API: return 0–100 confidence score for a bet.

    features: dict with keys from FEATURE_NAMES (missing keys use FEATURE_DEFAULTS).
    Uses LogReg if model is trained (≥20 settled bets), else heuristic.
    When model is available, blends: 60% heuristic + 40% LogReg.
    """
    # Fill defaults
    full = {**FEATURE_DEFAULTS, **{k: v for k, v in features.items() if v is not None}}

    heuristic = _heuristic_score(full)

    model = _get_model()
    if model:
        lr = _logreg_score(full, model)
        if lr is not None:
            blended = round(0.60 * heuristic + 0.40 * lr)
            return int(min(max(blended, 0), 100))

    return heuristic


def build_bet_features(analysis: dict, side: str) -> dict:
    """
    Extract the confidence feature dict from a brain.py analysis result + side.
    Called in brain.py before _confidence_score.
    """
    opp = "home" if side == "away" else "away"

    our_sp  = analysis.get(f"{side}_sp") or {}
    opp_sp  = analysis.get(f"{opp}_sp") or {}
    our_bp  = analysis.get(f"{side}_bp") or {}
    opp_bp  = analysis.get(f"{opp}_bp") or {}
    our_mom = analysis.get(f"{side}_momentum") or {}
    opp_mom = analysis.get(f"{opp}_momentum") or {}

    # Sharp signal: reverse line movement on our side
    sharp_side = analysis.get("market_sharp_signal", "")
    sharp_flag = 1 if (sharp_side and sharp_side == side) else 0

    # Line movement direction relative to our bet
    lm_dir_raw = analysis.get("market_line_direction", "stable")
    lm_mag     = float(analysis.get("market_line_magnitude", 0.0))
    if lm_mag < 0.02:
        lm_dir = 0
    elif f"toward_{side}" in lm_dir_raw:
        lm_dir = 1   # moving in our favour
    elif lm_dir_raw not in ("stable", "unknown", ""):
        lm_dir = -1  # moving against us
    else:
        lm_dir = 0

    # ABS score: from our SP
    abs_score = our_sp.get("abs_score")
    abs_score = float(abs_score) if abs_score is not None else 50.0

    # Home dog angle: only applies when we're betting home
    home_dog = 0
    if side == "home":
        hd = analysis.get("home_dog") or {}
        home_dog = 1 if hd.get("is_home_dog_value") else 0

    # Umpire home win adj: applies differently by side
    ump_edge = analysis.get("ump_edge") or {}
    ump_adj  = float(ump_edge.get("home_win_adj", 0.0))
    if side == "away":
        ump_adj = -ump_adj   # home bias hurts away bettors

    # Key reliever: our closer/top RP available
    our_key_rel = 1 if analysis.get(f"{side}_key_rel_avail", True) else 0

    return {
        "edge_pct":            float(analysis.get(f"{side}_edge", 3.0)),
        "model_prob":          float(analysis.get(f"{side}_model_p", 0.52)),
        "sp_xfip_our":         float(our_sp.get("xfip", 4.35)),
        "sp_xfip_opp":         float(opp_sp.get("xfip", 4.35)),
        "bullpen_fatigue_our": float(our_bp.get("avg_fatigue", 4.0)),
        "bullpen_fatigue_opp": float(opp_bp.get("avg_fatigue", 4.0)),
        "momentum_our":        float(our_mom.get("score", 0.0)),
        "momentum_opp":        float(opp_mom.get("score", 0.0)),
        "lineup_confirmed":    1 if analysis.get(f"{side}_lineup_confirmed", False) else 0,
        "sharp_signal":        sharp_flag,
        "lm_direction":        lm_dir,
        "abs_score_sp":        abs_score,
        "home_dog_angle":      home_dog,
        "ump_home_win_adj":    ump_adj,
        "key_reliever_avail":  our_key_rel,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    report = retrain_confidence_model()
    print(f"Retrain: {report}")

    sample = {
        "edge_pct": 8.0, "model_prob": 0.56, "sp_xfip_our": 3.50, "sp_xfip_opp": 4.80,
        "bullpen_fatigue_our": 3.0, "bullpen_fatigue_opp": 5.0, "momentum_our": 4.0,
        "momentum_opp": -2.0, "lineup_confirmed": 1, "sharp_signal": 1,
        "lm_direction": 1, "abs_score_sp": 72.0, "home_dog_angle": 1,
        "ump_home_win_adj": 0.02, "key_reliever_avail": 1,
    }
    score = get_confidence_score(sample)
    print(f"Sample score: {score}/100")
