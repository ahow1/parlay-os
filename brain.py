"""PARLAY OS — brain.py
CEO orchestrator. All engines report here. Nothing gets recommended without brain signing off.

Usage:
  python brain.py               # full daily scout
  python brain.py --live        # live monitoring loop
  python brain.py --props       # props-only scan
  python brain.py --test        # dry run (no DB writes, no Telegram)
"""

import os
import sys
import json
import requests
import traceback
from datetime import date, datetime
import pytz
import error_logger
error_logger.setup()

from api_client import get as _http_get

# Force line-buffered stdout so every print() appears immediately in the terminal,
# even when brain.py is launched as a subprocess or inside a PTY wrapper.
sys.stdout.reconfigure(line_buffering=True)

# ── Engine imports ────────────────────────────────────────────────────────────
import db as _db
from constants      import MLB_TEAM_MAP, MLB_TEAM_IDS, TEAM_SLUGS, PARK_FACTORS, UMPIRE_TENDENCIES
from math_engine    import (
    american_to_decimal, decimal_to_american, implied_prob,
    no_vig_prob, expected_value, parlay_odds, STARTING_BANKROLL,
)
from weather_engine import get_weather
from sp_engine      import get_game_sps
from bullpen_engine import analyze_bullpen, bullpen_run_factor
from offense_engine import analyze_offense
from market_engine  import get_mlb_events, full_market_snapshot
from bankroll_engine import (
    kelly_stake, sizing_summary, current_bankroll, peak_bankroll, is_drawdown_pause,
    daily_budget, daily_budget_pct, pool_budget, pool_exposure, pool_remaining,
    drawdown_tier, growth_tracker, MIN_STAKE,
)
from profile_engine import update_sp_profile, update_hitter_profile, update_bullpen_profile
from props_engine   import (
    k_prop, nrfi_prob, team_run_expectancy, game_total_prob,
    f5_run_expectancy, correlated_parlay, scan_k_prop,
    build_sgp_suggestions, prob_over,
)
from bet_type_validator import (
    validate_bet, BetValidationError, day_classification,
    MAX_LOCKS_PER_DAY, MAX_FLIPS_PER_DAY, MAX_PROPS_PER_DAY,
)
from h2h_engine import get_h2h_stats
from intelligence_engine import (
    sp_regression_flags, offense_regression_flags, bullpen_regression_flags,
    get_injury_flags, format_injury_section,
    weighted_momentum,
    format_sharp_pick, format_discord_pick,
)
from memory_engine  import (
    init_memory_tables, recalibrate_model_prob, adjust_model_prob,
    memory_report,
    pitcher_profile_updated_today, hitter_profile_updated_today,
    init_brain_tables, apply_brain_to_prob, get_brain_summary,
    update_bet_memory, update_sp_memory, update_team_memory,
    update_situation_memory, recalibrate_weights,
)
# ML model — imported lazily so brain.py still starts if models not trained
try:
    from ml_model import (
        predict_game, build_game_features, detect_regression_flags,
        models_available,
    )
    _ML_AVAILABLE = True
except ImportError:
    _ML_AVAILABLE = False
    def models_available(): return False
    def detect_regression_flags(*a, **k): return {"flags": [], "count": 0}

# Statcast — imported lazily; non-critical, degrades gracefully
try:
    from statcast_engine import get_pitcher_statcast, get_lineup_statcast, sp_statcast_summary
    _STATCAST_AVAILABLE = True
except ImportError:
    _STATCAST_AVAILABLE = False
    def get_pitcher_statcast(*a, **k): return {}
    def get_lineup_statcast(*a, **k): return {}
    def sp_statcast_summary(*a, **k): return ""

# New engine imports — degrade gracefully if any fail
try:
    from home_dog_engine import check_home_dog_value, home_dog_telegram_tag
    _HOME_DOG_AVAILABLE = True
except ImportError:
    _HOME_DOG_AVAILABLE = False
    def check_home_dog_value(a): return {"is_home_dog_value": False, "add_prob": 0.0, "conditions_failed": []}
    def home_dog_telegram_tag(h): return ""

try:
    from earned_runs_engine import analyze_earned_runs, er_prop_telegram_line
    _ER_AVAILABLE = True
except ImportError:
    _ER_AVAILABLE = False
    def analyze_earned_runs(*a, **k): return None
    def er_prop_telegram_line(*a): return ""

try:
    from strikeout_engine import analyze_k_prop, k_prop_telegram_line
    _STRIKEOUT_ENGINE_AVAILABLE = True
except ImportError:
    _STRIKEOUT_ENGINE_AVAILABLE = False
    def analyze_k_prop(*a, **k): return None
    def k_prop_telegram_line(*a): return ""

try:
    from line_movement_engine import start_line_polling
    _LINE_POLLING_AVAILABLE = True
except ImportError:
    _LINE_POLLING_AVAILABLE = False
    def start_line_polling(*a, **k): return None

try:
    from pitch_mix_engine import check_pitch_trap
    _PITCH_MIX_AVAILABLE = True
except ImportError:
    _PITCH_MIX_AVAILABLE = False
    def check_pitch_trap(*a, **k): return {"is_pitch_trap": False, "prob_add": 0.0, "tag": ""}

try:
    from framing_engine import get_team_framing, check_framing_edge
    _FRAMING_AVAILABLE = True
except ImportError:
    _FRAMING_AVAILABLE = False
    def get_team_framing(*a, **k): return None
    def check_framing_edge(r, *a, **k): return {"prob_adj": 0.0, "tag": "", "framing_runs": r}

try:
    from defense_engine import get_team_oaa, check_defense_edge
    _DEFENSE_AVAILABLE = True
except ImportError:
    _DEFENSE_AVAILABLE = False
    def get_team_oaa(*a, **k): return None
    def check_defense_edge(o, *a, **k): return {"run_adj": 0.0, "tag": "", "oaa": o}

try:
    from umpire_engine import get_umpire_edge, umpire_telegram_flag
    _UMPIRE_ENGINE_AVAILABLE = True
except ImportError:
    _UMPIRE_ENGINE_AVAILABLE = False
    def get_umpire_edge(n): return {"home_win_adj": 0.0, "total_adj": 0.0, "tag": "", "has_data": False}
    def umpire_telegram_flag(n, e=None): return ""

try:
    from situations_engine import check_situations, situations_telegram_line
    _SITUATIONS_AVAILABLE = True
except ImportError:
    _SITUATIONS_AVAILABLE = False
    def check_situations(*a, **k): return {"triggered": [], "n_triggered": 0, "adjustments": {}, "total_away_adj": 0.0, "total_home_adj": 0.0, "situation_stack": False, "high_conviction": False, "labels": [], "details": {}}
    def situations_telegram_line(r): return ""

try:
    from confidence_engine import get_confidence_score, build_bet_features as _build_conf_features
    _CONFIDENCE_ENGINE_AVAILABLE = True
except ImportError:
    _CONFIDENCE_ENGINE_AVAILABLE = False
    def get_confidence_score(f): return 65
    def _build_conf_features(a, s): return {}

STATSAPI  = "https://statsapi.mlb.com/api/v1"
ET        = pytz.timezone("America/New_York")
BOT_TOKEN         = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID           = os.getenv("TELEGRAM_CHAT_ID", "")
PUBLIC_CHANNEL_ID = os.getenv("TELEGRAM_PUBLIC_CHANNEL_ID", "")

DRY_RUN   = "--test" in sys.argv

# Minimum edge to recommend
MIN_EDGE_PCT = 3.0
# Minimum Pythagorean probability to include in output
MIN_PROB     = 0.48


# ── PYTHAGOREAN WIN PROB ──────────────────────────────────────────────────────

def pythagorean_prob(our_xr: float, opp_xr: float, exp: float = 1.83) -> float:
    if our_xr + opp_xr <= 0:
        return 0.5
    return round(our_xr ** exp / (our_xr ** exp + opp_xr ** exp), 4)


# ── GAME ANALYSIS ─────────────────────────────────────────────────────────────

def analyze_game(event: dict, game_date: str) -> dict | None:
    """
    Full analysis for a single game.
    event: {id, away, home, commence_utc}
    Returns analysis dict or None if insufficient data.
    """
    away_name = event["away"]
    home_name = event["home"]
    away_code = MLB_TEAM_MAP.get(away_name, away_name[:3].upper())
    home_code = MLB_TEAM_MAP.get(home_name, home_name[:3].upper())

    away_tid  = MLB_TEAM_IDS.get(away_code)
    home_tid  = MLB_TEAM_IDS.get(home_code)
    if not away_tid or not home_tid:
        print(f"  SKIP [{away_name} @ {home_name}]: unknown team code — away={away_code} home={home_code}")
        return None

    # Resolve game_pk from schedule
    game_pk   = _resolve_game_pk(away_name, game_date)

    # ── Market ───────────────────────────────────────────────────────────────
    market = full_market_snapshot(
        event["id"], away_name, home_name,
        away_code, home_code, game_date,
        commence_utc=event.get("commence_utc", ""),
    )
    nv = market.get("no_vig") or {}
    if not nv:
        books = list(market.get("ml_books", {}).keys())
        print(f"  SKIP [{away_name} @ {home_name}]: no market data — books matched: {books or 'none'}")
        return None

    away_nv = nv.get("away", 0.5)
    home_nv = nv.get("home", 0.5)

    # ── Weather (home park) ───────────────────────────────────────────────────
    weather = get_weather(home_code)
    wx_rf   = weather.get("run_factor", 1.0)

    # ── Park factor ──────────────────────────────────────────────────────────
    park_rf = PARK_FACTORS.get(home_code, 1.0)

    # ── Umpire ────────────────────────────────────────────────────────────────
    umpire    = _get_umpire(game_pk)
    ump_k, ump_run, ump_note = UMPIRE_TENDENCIES.get(umpire, (1.0, 1.0, ""))
    ump_edge  = get_umpire_edge(umpire)
    ump_home_win_adj = ump_edge.get("home_win_adj", 0.0)

    # ── SPs ───────────────────────────────────────────────────────────────────
    sps  = get_game_sps(game_pk or 0, away_code, home_code, umpire) if game_pk else {}
    away_sp = sps.get("away", {})
    home_sp = sps.get("home", {})

    # ── Bullpens ──────────────────────────────────────────────────────────────
    away_bp = analyze_bullpen(away_tid, game_date, label=away_code)
    home_bp = analyze_bullpen(home_tid, game_date, label=home_code)
    away_bp_rf = bullpen_run_factor(away_bp)
    home_bp_rf = bullpen_run_factor(home_bp)

    # ── Offense ───────────────────────────────────────────────────────────────
    away_off = analyze_offense(away_code, game_pk=game_pk, side="away",
                                opp_sp_hand=home_sp.get("hand", "R"))
    home_off = analyze_offense(home_code, game_pk=game_pk, side="home",
                                opp_sp_hand=away_sp.get("hand", "R"))

    # ── Run Expectancy ────────────────────────────────────────────────────────
    # SP factor is weighted by expected innings; use the OPPOSING bullpen for remaining innings.
    def _sp_ips(sp: dict) -> float:
        ip = sp.get("ip", 0) or 0
        gs = sp.get("gs", 1) or 1
        return min(ip / gs if gs > 0 else 5.5, 7.0)

    away_xr = team_run_expectancy(
        away_off["run_factor"],
        home_sp.get("run_factor", 1.0),
        park_rf, wx_rf,
        home_bp_rf,          # home bullpen opposes away team's scoring
        _sp_ips(home_sp),
    )
    home_xr = team_run_expectancy(
        home_off["run_factor"],
        away_sp.get("run_factor", 1.0),
        park_rf, wx_rf,
        away_bp_rf,          # away bullpen opposes home team's scoring
        _sp_ips(away_sp),
    )

    # ── Pythagorean base probability ─────────────────────────────────────────
    from constants import HOME_ADV
    adj_home_xr  = home_xr * HOME_ADV
    pyth_away_p  = pythagorean_prob(away_xr, adj_home_xr)
    pyth_home_p  = round(1.0 - pyth_away_p, 4)

    # ── ML model: run for SHAP/total/NRFI outputs; prediction fills Pyth slot ──
    shap_home = shap_away = []
    ml_total  = ml_nrfi_p = None
    ml_conf   = "weighted_8factor"
    pyth_or_ml_away_p = pyth_away_p   # slot 5 of 8-factor blend
    if _ML_AVAILABLE and models_available():
        try:
            feat_vec = build_game_features(
                away_sp, home_sp, away_off, home_off,
                away_xr, home_xr, weather, park_rf,
            )
            ml_pred           = predict_game(feat_vec)
            pyth_or_ml_away_p = ml_pred.get("away_win_prob", pyth_away_p)
            shap_home         = ml_pred.get("shap_home", [])
            shap_away         = ml_pred.get("shap_away", [])
            ml_total          = ml_pred.get("total_runs_pred")
            ml_nrfi_p         = ml_pred.get("nrfi_prob")
            ml_conf           = ml_pred.get("confidence", "weighted_8factor")
            print(f"  ML slot5={pyth_or_ml_away_p:.3f} pyth={pyth_away_p:.3f} conf={ml_conf}")
        except Exception as _ml_e:
            print(f"  ML predict failed ({_ml_e}), using Pythagorean in slot 5")

    # ── Momentum — computed here for 8-factor blend (reused below) ───────────
    away_momentum = weighted_momentum(away_tid, away_code)
    home_momentum = weighted_momentum(home_tid, home_code)

    # ── Home dog structural edge check ────────────────────────────────────────
    _hd_partial = {
        "best_home_odds": market.get("best_home_odds"),
        "home_sp":  home_sp,
        "away_sp":  away_sp,
        "home_bp":  home_bp,
        "home_off": home_off,
    }
    home_dog_result = check_home_dog_value(_hd_partial)
    home_dog_add    = home_dog_result.get("add_prob", 0.0)
    if home_dog_add > 0:
        print(f"  🐶 HOME_DOG_ANGLE fired (+{home_dog_add:.2f} home win prob in blend)")

    # ── Factor 9: Pitch trap (away offense vs home SP / home offense vs away SP)
    away_pitch_trap = check_pitch_trap(
        home_sp.get("pitcher_id", 0) or 0,
        home_sp.get("name", ""),
        away_code,
    )
    home_pitch_trap = check_pitch_trap(
        away_sp.get("pitcher_id", 0) or 0,
        away_sp.get("name", ""),
        home_code,
    )
    # prob_add = benefit for the OFFENSE; away offense benefits = away_p up
    _pt_away_adj = away_pitch_trap.get("prob_add", 0.0)   # away lineup exploits home SP
    _pt_home_adj = home_pitch_trap.get("prob_add", 0.0)   # home lineup exploits away SP

    # ── Factor 10: Key reliever availability (from bullpen engine) ────────────
    away_key_rel_avail = away_bp.get("key_reliever_available", True)
    home_key_rel_avail = home_bp.get("key_reliever_available", True)

    # ── Factor 11: Catcher framing ────────────────────────────────────────────
    away_framing_runs = get_team_framing(away_code)
    home_framing_runs = get_team_framing(home_code)
    away_framing_res  = check_framing_edge(away_framing_runs, away_code)
    home_framing_res  = check_framing_edge(home_framing_runs, home_code)
    # prob_adj is signed: positive = team's pitching benefits (harder for opponents to score)
    # In away_p terms: away framing helps away pitchers → away_p up; home framing → away_p down
    _away_framing_adj = away_framing_res.get("prob_adj", 0.0)
    _home_framing_adj = home_framing_res.get("prob_adj", 0.0)

    # ── Factor 12 data: Lineup slot (kept for DB logging, not in blend) ──────
    _away_slot_adj = away_off.get("slot_run_adj", 0.0)
    _home_slot_adj = home_off.get("slot_run_adj", 0.0)

    # ── 12-factor weighted probability blend ─────────────────────────────────
    _lm_raw       = market.get("line_movement") or {}
    _lm_dir       = _lm_raw.get("direction", "stable")
    _lm_mag       = _lm_raw.get("magnitude", 0.0)
    away_wrc_v    = away_off.get("adj_wrc_plus", away_off.get("wrc_plus", 100.0))
    home_wrc_v    = home_off.get("adj_wrc_plus", home_off.get("wrc_plus", 100.0))

    # ── Savant pipeline signals for weighted blend ────────────────────────────
    # Factor 1 — SP xwOBA against (from savant_leaderboards via sp_engine)
    _away_xwoba  = away_sp.get("xwoba_against")
    _home_xwoba  = home_sp.get("xwoba_against")

    # Factor 2 — Pitch quality: k_conf_adj from arsenal (convert to prob adj)
    _away_pq_adj = away_sp.get("k_conf_adj_savant", 0) / 300.0   # ~0.033 per 10pt
    _home_pq_adj = home_sp.get("k_conf_adj_savant", 0) / 300.0

    # Factor 3 — Rolling form tier
    _away_roll_tier = away_sp.get("rolling_xwoba_tier", "UNKNOWN")
    _home_roll_tier = home_sp.get("rolling_xwoba_tier", "UNKNOWN")

    # Factor 4 — Bullpen stuff_plus adj (from savant_leaderboards if available)
    _away_bp_stuff = 0.0
    _home_bp_stuff = 0.0
    try:
        from savant_leaderboards import bullpen_stuff_lambda_adj as _bpsla
        _away_bp_pids = [p.get("pitcher_id") for p in (away_bp.get("arms") or []) if p.get("pitcher_id")]
        _home_bp_pids = [p.get("pitcher_id") for p in (home_bp.get("arms") or []) if p.get("pitcher_id")]
        if _away_bp_pids:
            _away_bp_stuff = _bpsla(_away_bp_pids) * 0.1   # scale -0.2 → 0.02 prob adj
        if _home_bp_pids:
            _home_bp_stuff = _bpsla(_home_bp_pids) * 0.1
    except Exception:
        pass

    # Factor 5 — Bat tracking adj (team lineup average blast)
    _away_bat_adj = 0.0
    _home_bat_adj = 0.0
    try:
        from savant_leaderboards import blast_tb_adj as _blastadj
        _away_lineup_ids = [p.get("id") for p in (away_off.get("lineup") or [])[:6] if p.get("id")]
        _home_lineup_ids = [p.get("id") for p in (home_off.get("lineup") or [])[:6] if p.get("id")]
        if _away_lineup_ids:
            adjs = [_blastadj(pid) for pid in _away_lineup_ids]
            _away_bat_adj = sum(adjs) / len(adjs) if adjs else 0.0
        if _home_lineup_ids:
            adjs = [_blastadj(pid) for pid in _home_lineup_ids]
            _home_bat_adj = sum(adjs) / len(adjs) if adjs else 0.0
    except Exception:
        pass

    # Factor 8 — Park + OF defense combined adj
    _park_of_adj = 0.0
    try:
        from savant_leaderboards import team_of_lambda_adj as _ofadj
        _away_of_ids = [p.get("id") for p in (away_off.get("lineup") or []) if p.get("id")]
        _home_of_ids = [p.get("id") for p in (home_off.get("lineup") or []) if p.get("id")]
        _away_of_rv = _ofadj(_away_of_ids)
        _home_of_rv = _ofadj(_home_of_ids)
        _park_of_adj = (_away_of_rv - _home_of_rv) * 0.05
    except Exception:
        pass

    # Factor 9 — YoY conf adj (convert -3/+3 to probability adj)
    _away_yoy_adj = away_sp.get("yoy_conf_adj", 0) / 100.0
    _home_yoy_adj = home_sp.get("yoy_conf_adj", 0) / 100.0

    # Factor 10 — ABS/FPS model adj (from sp_engine)
    _away_fps_adj = away_sp.get("fps_model_adj", 0.0)
    _home_fps_adj = home_sp.get("fps_model_adj", 0.0)

    # Factor 10 — Tempo adj
    _tempo_map  = {"QUICK_WORKER": 0.01, "SLOW_WORKER": -0.01, "NORMAL": 0.0, "UNKNOWN": 0.0}
    _away_tempo_adj = _tempo_map.get(away_sp.get("tempo_label", "UNKNOWN"), 0.0)
    _home_tempo_adj = _tempo_map.get(home_sp.get("tempo_label", "UNKNOWN"), 0.0)

    # Factor 11 — Sprint speed + baserunning
    _away_sprint_adj = 0.0
    _home_sprint_adj = 0.0
    try:
        from savant_leaderboards import sprint_lambda_adj as _sprintadj, baserunning_lambda_adj as _bsadj
        _away_lineup_ids2 = [p.get("id") for p in (away_off.get("lineup") or [])[:9] if p.get("id")]
        _home_lineup_ids2 = [p.get("id") for p in (home_off.get("lineup") or [])[:9] if p.get("id")]
        _away_sprint_adj = (_sprintadj(_away_lineup_ids2) + _bsadj(away_tid or 0)) * 0.05
        _home_sprint_adj = (_sprintadj(_home_lineup_ids2) + _bsadj(home_tid or 0)) * 0.05
    except Exception:
        pass

    # Factor 7 — Arm angle platoon adj
    _away_arm_adj = 0.0
    _home_arm_adj = 0.0
    try:
        from savant_leaderboards import arm_angle_platoon_adj as _armadj
        _away_arm_ang = away_sp.get("arm_angle")
        _home_arm_ang = home_sp.get("arm_angle")
        if _away_arm_ang is not None:
            _away_arm_adj = _armadj(_away_arm_ang) * 0.5
        if _home_arm_ang is not None:
            _home_arm_adj = _armadj(_home_arm_ang) * 0.5
    except Exception:
        pass

    away_model_p, home_model_p = _weighted_win_prob(
        away_xfip              = away_sp.get("xfip", 4.35),
        home_xfip              = home_sp.get("xfip", 4.35),
        away_bp_fatigue        = away_bp.get("avg_fatigue", 4.0),
        home_bp_fatigue        = home_bp.get("avg_fatigue", 4.0),
        away_wrc               = away_wrc_v,
        home_wrc               = home_wrc_v,
        home_dog_add           = home_dog_add,
        pyth_away_p            = pyth_or_ml_away_p,
        lm_direction           = _lm_dir,
        lm_magnitude           = _lm_mag,
        away_platoon_edge      = away_off.get("platoon_edge", 0),
        home_platoon_edge      = home_off.get("platoon_edge", 0),
        away_momentum_score    = away_momentum.get("score", 0.0),
        home_momentum_score    = home_momentum.get("score", 0.0),
        pitch_trap_away_adj    = _pt_away_adj,
        pitch_trap_home_adj    = _pt_home_adj,
        away_framing_adj       = _away_framing_adj,
        home_framing_adj       = _home_framing_adj,
        away_key_reliever_avail = away_key_rel_avail,
        home_key_reliever_avail = home_key_rel_avail,
        ump_home_win_adj       = ump_home_win_adj,
        # New Savant-powered factors
        away_xwoba_against     = _away_xwoba,
        home_xwoba_against     = _home_xwoba,
        away_pitch_quality_adj = _away_pq_adj,
        home_pitch_quality_adj = _home_pq_adj,
        away_rolling_tier      = _away_roll_tier,
        home_rolling_tier      = _home_roll_tier,
        away_bp_stuff_adj      = _away_bp_stuff,
        home_bp_stuff_adj      = _home_bp_stuff,
        away_bat_tracking_adj  = _away_bat_adj,
        home_bat_tracking_adj  = _home_bat_adj,
        park_of_adj            = _park_of_adj,
        away_yoy_adj           = _away_yoy_adj,
        home_yoy_adj           = _home_yoy_adj,
        away_fps_adj           = _away_fps_adj,
        home_fps_adj           = _home_fps_adj,
        away_tempo_adj         = _away_tempo_adj,
        home_tempo_adj         = _home_tempo_adj,
        away_sprint_adj        = _away_sprint_adj,
        home_sprint_adj        = _home_sprint_adj,
        away_arm_angle_adj     = _away_arm_adj,
        home_arm_angle_adj     = _home_arm_adj,
        h2h_away_p             = 0.50,   # H2H handled separately post-blend
    )
    _a_tier, _ = _sp_tier(away_sp.get("xfip", 4.35))
    _h_tier, _ = _sp_tier(home_sp.get("xfip", 4.35))
    print(f"  12-factor: away={away_model_p:.3f} home={home_model_p:.3f} "
          f"SP:{away_sp.get('xfip',4.35):.2f}({_a_tier}) vs {home_sp.get('xfip',4.35):.2f}({_h_tier}) "
          f"xwOBA:{_away_xwoba or '?'}/{_home_xwoba or '?'} "
          f"roll:{_away_roll_tier}/{_home_roll_tier} "
          f"BP:{away_bp.get('avg_fatigue',4.0):.1f}/{home_bp.get('avg_fatigue',4.0):.1f} "
          f"Pyth={pyth_away_p:.3f} UMP_HWA:{ump_home_win_adj:+.3f}")

    # ── H2H historical matchup (10% weight) ─────────────────────────────────
    h2h = {}
    try:
        if away_tid and home_tid:
            h2h = get_h2h_stats(away_tid, home_tid)
            if h2h.get("h2h_available"):
                away_model_p = round(0.90 * away_model_p + 0.10 * h2h["away_win_rate"], 4)
                home_model_p = round(0.90 * home_model_p + 0.10 * h2h["home_win_rate"], 4)
    except Exception:
        pass

    # Memory calibration
    away_model_p = recalibrate_model_prob(away_model_p)
    home_model_p = recalibrate_model_prob(home_model_p)

    # ── Brain learning adjustments (SP correction + team bias + situations) ────
    _away_sp_id = away_sp.get("pitcher_id") if away_sp else None
    _home_sp_id = home_sp.get("pitcher_id") if home_sp else None
    _brain_situations = ""
    _away_sits: list = []
    _home_sits: list = []
    try:
        from situations_engine import get_active_situations
        _away_sits = get_active_situations(away_code, home_code, away_nv, "away") or []
        _home_sits = get_active_situations(home_code, away_code, home_nv, "home") or []
        if _away_sits:
            _brain_situations = "+".join(sorted(_away_sits))
    except Exception:
        pass

    away_model_p, _away_brain_notes = apply_brain_to_prob(
        away_model_p, _home_sp_id, away_code, _brain_situations,
    )
    home_model_p, _home_brain_notes = apply_brain_to_prob(
        home_model_p, _away_sp_id, home_code,
        "+".join(sorted(_home_sits)) if _home_sits else "",
    )
    if _away_brain_notes or _home_brain_notes:
        print(f"  [BRAIN] away_adj={away_model_p:.4f} ({', '.join(_away_brain_notes)}) "
              f"home_adj={home_model_p:.4f} ({', '.join(_home_brain_notes)})")

    # ── SP unknown → reduce confidence by 15% (pull toward 0.5) ─────────────
    # A missing SP means we don't know the run-prevention side of the ledger.
    # Rather than blocking the bet entirely, we shrink the edge conservatively.
    _SP_TBD_REDUCTION = 0.15
    away_sp_tbd = not away_sp.get("name") or away_sp.get("name") == "TBD"
    home_sp_tbd = not home_sp.get("name") or home_sp.get("name") == "TBD"
    if away_sp_tbd or home_sp_tbd:
        away_model_p = round(away_model_p - _SP_TBD_REDUCTION * (away_model_p - 0.5), 4)
        home_model_p = round(home_model_p - _SP_TBD_REDUCTION * (home_model_p - 0.5), 4)

    # ── Unconfirmed lineup → reduce confidence by 10% (pull toward 0.5) ──────
    # Unconfirmed lineup = we don't know who's batting today. Still analyze
    # the game — never skip — but shrink edge conservatively.
    _LINEUP_REDUCTION = 0.10
    away_lineup_unconfirmed = away_off.get("lineup_unconfirmed", False)
    home_lineup_unconfirmed = home_off.get("lineup_unconfirmed", False)
    if away_lineup_unconfirmed or home_lineup_unconfirmed:
        away_model_p = round(away_model_p - _LINEUP_REDUCTION * (away_model_p - 0.5), 4)
        home_model_p = round(home_model_p - _LINEUP_REDUCTION * (home_model_p - 0.5), 4)
        sides = []
        if away_lineup_unconfirmed:
            sides.append(away_code)
        if home_lineup_unconfirmed:
            sides.append(home_code)
        print(f"  [LINEUP] unconfirmed for {', '.join(sides)} — model confidence -10%")

    # ── Regression / intelligence flags ──────────────────────────────────────
    reg_flags = detect_regression_flags(away_sp, home_sp, away_off, home_off)

    # SP regression (ERA vs xFIP, velocity, control)
    away_sp_intel = sp_regression_flags(away_sp, label=away_code)
    home_sp_intel = sp_regression_flags(home_sp, label=home_code)

    # Offense regression (BABIP, BA trend)
    away_off_intel = offense_regression_flags(away_tid, away_code)
    home_off_intel = offense_regression_flags(home_tid, home_code)

    # All intelligence flags combined
    intel_flags = away_sp_intel + home_sp_intel + away_off_intel + home_off_intel

    # Injury flags (IL transactions + SP velocity risk)
    injury_flags = get_injury_flags(away_code, home_code, away_sp, home_sp)

    # Enhanced momentum (computed earlier for 8-factor blend — reuse here)
    momentum = {
        "away":     away_momentum["score"],
        "home":     home_momentum["score"],
        "away_sum": away_momentum["summary"],
        "home_sum": home_momentum["summary"],
    }

    narrative = _narrative_flags(market, away_nv, home_nv)

    # ── Edge Calculation ─────────────────────────────────────────────────────
    away_edge = round((away_model_p - away_nv) * 100, 2)
    home_edge = round((home_model_p - home_nv) * 100, 2)

    best_away_odds = market.get("best_away_odds")
    best_home_odds = market.get("best_home_odds")

    # ── Statcast for SPs ─────────────────────────────────────────────────────
    away_sc_str = sp_statcast_summary(away_sp.get("pitcher_id")) if away_sp.get("pitcher_id") else ""
    home_sc_str = sp_statcast_summary(home_sp.get("pitcher_id")) if home_sp.get("pitcher_id") else ""

    # Verbose per-game log
    away_sp_name = away_sp.get("name", "TBD") if away_sp else "TBD"
    home_sp_name = home_sp.get("name", "TBD") if home_sp else "TBD"

    away_r3_era   = away_sp.get("rolling_era_3")
    home_r3_era   = home_sp.get("rolling_era_3")
    away_sp_flags = []
    home_sp_flags = []
    if away_sp_tbd:                      away_sp_flags.append("TBD(-15%)")
    if away_sp.get("worsening_walk"):    away_sp_flags.append("BB↑")
    if away_sp.get("velocity_decline"):  away_sp_flags.append("velo↓")
    if away_sp.get("k9_declining"):      away_sp_flags.append("K↓")
    if home_sp_tbd:                      home_sp_flags.append("TBD(-15%)")
    if home_sp.get("worsening_walk"):    home_sp_flags.append("BB↑")
    if home_sp.get("velocity_decline"):  home_sp_flags.append("velo↓")
    if home_sp.get("k9_declining"):      home_sp_flags.append("K↓")

    away_lineup_tag = "UNCONFIRMED" if away_off.get("lineup_unconfirmed") else "confirmed"
    home_lineup_tag = "UNCONFIRMED" if home_off.get("lineup_unconfirmed") else "confirmed"

    def _plat_wrc(val, default=100.0) -> float:
        """Safely extract wrc_plus from a platoon value that may be a dict, float, or None."""
        if isinstance(val, dict):
            return val.get("wrc_plus", default)
        if isinstance(val, (int, float)):
            return float(val)
        return default

    away_platoon_delta = round(
        _plat_wrc(away_off.get("platoon_vs_rhp")) - _plat_wrc(away_off.get("platoon_vs_lhp")), 1
    )
    home_platoon_delta = round(
        _plat_wrc(home_off.get("platoon_vs_rhp")) - _plat_wrc(home_off.get("platoon_vs_lhp")), 1
    )

    wx_adj = weather.get("run_adjustment", 0.0)
    wx_label = weather.get("wind_label", weather.get("note", ""))

    away_hi_arms = away_bp.get("high_fatigue_arms", [])
    home_hi_arms = home_bp.get("high_fatigue_arms", [])

    print(
        f"[{away_code}@{home_code}] "
        f"model={away_model_p:.3f}/{home_model_p:.3f}  "
        f"nv={away_nv:.3f}/{home_nv:.3f}  "
        f"edge={away_edge:+.1f}/{home_edge:+.1f}%  "
        f"xR={away_xr:.2f}/{home_xr:.2f}  "
        f"SP: {away_sp_name} vs {home_sp_name}"
    )
    # Lineup status
    print(f"  Lineup: {away_code}={away_lineup_tag}  {home_code}={home_lineup_tag}")
    # SP rolling ERA
    away_r3_str = f"{away_r3_era:.2f}" if away_r3_era is not None else "N/A"
    home_r3_str = f"{home_r3_era:.2f}" if home_r3_era is not None else "N/A"
    away_xfip_str = f"/xFIP {away_sp.get('xfip'):.2f}" if away_sp.get("xfip") is not None else ""
    home_xfip_str = f"/xFIP {home_sp.get('xfip'):.2f}" if home_sp.get("xfip") is not None else ""
    print(f"  SP ERA{'/xFIP' if away_xfip_str else ''}: "
          f"{away_sp_name}={away_r3_str}{away_xfip_str}{' ['+','.join(away_sp_flags)+']' if away_sp_flags else ''}  "
          f"{home_sp_name}={home_r3_str}{home_xfip_str}{' ['+','.join(home_sp_flags)+']' if home_sp_flags else ''}")
    if away_sc_str or home_sc_str:
        print(f"  Statcast: {away_sp_name}: {away_sc_str or 'N/A'}  |  {home_sp_name}: {home_sc_str or 'N/A'}")
    # Platoon matchup vs today's SP
    away_plat_edge = away_off.get("platoon_edge", 0)
    home_plat_edge = home_off.get("platoon_edge", 0)
    away_plat_flag = " ⚡PLATOON_ADV" if away_off.get("strong_platoon_advantage") else (
                     " ⚠PLATOON_DIS" if away_off.get("strong_platoon_disadvantage") else "")
    home_plat_flag = " ⚡PLATOON_ADV" if home_off.get("strong_platoon_advantage") else (
                     " ⚠PLATOON_DIS" if home_off.get("strong_platoon_disadvantage") else "")
    if away_plat_flag or home_plat_flag:
        print(f"  Platoon vs SP: {away_code}={away_plat_edge:+.0f}wRC+{away_plat_flag}  "
              f"{home_code}={home_plat_edge:+.0f}wRC+{home_plat_flag}")

    # SP first inning ERA / YRFI lean
    _fi_parts = []
    if away_sp.get("yrfi_lean"):
        fi = away_sp.get("first_inning_era", "?")
        _fi_parts.append(f"{away_sp_name} YRFI_LEAN fi_ERA={fi}")
    if home_sp.get("yrfi_lean"):
        fi = home_sp.get("first_inning_era", "?")
        _fi_parts.append(f"{home_sp_name} YRFI_LEAN fi_ERA={fi}")
    if _fi_parts:
        print(f"  1st-inning: {' | '.join(_fi_parts)}")

    # Bullpen fatigue
    away_bp_str = f"{away_bp['fatigue_tier']}({away_bp['avg_fatigue']:.1f})"
    home_bp_str = f"{home_bp['fatigue_tier']}({home_bp['avg_fatigue']:.1f})"
    hi_str = ""
    if away_hi_arms:
        hi_str += f" ⚠ {away_code} HI-FAT: {', '.join(away_hi_arms)}"
    if home_hi_arms:
        hi_str += f" ⚠ {home_code} HI-FAT: {', '.join(home_hi_arms)}"
    print(f"  Bullpen: {away_code}={away_bp_str}  {home_code}={home_bp_str}{hi_str}")
    # Umpire + weather
    print(f"  Ump: {umpire or 'unknown'}  |  Wx: {wx_label} adj={wx_adj:+.2f}r/g  rf={wx_rf:.3f}")
    # Platoon splits
    print(f"  Platoon Δ(vR-vL wRC+): {away_code}={away_platoon_delta:+.0f}  {home_code}={home_platoon_delta:+.0f}")
    # Print regression / narrative flags
    for flag in reg_flags.get("flags", []):
        print(f"  FLAG: {flag['message']}")
    for flag in narrative.get("flags", []):
        print(f"  FLAG: {flag['message']}")
    # Intelligence flags (SP regression, offense regression)
    for flag in intel_flags:
        print(f"  INTEL [{flag.get('type','')}]: {flag.get('emoji','')} {flag['message']}")
    # Injury flags
    if injury_flags:
        print(f"  ⚠️ INJURIES ({len(injury_flags)}):")
        for inj in injury_flags:
            print(f"    {inj.get('emoji','⚠️')} {inj['message']}")
    # Momentum
    print(f"  {away_momentum.get('summary','')}")
    print(f"  {home_momentum.get('summary','')}")

    # ── Props ─────────────────────────────────────────────────────────────────
    nrfi_r = nrfi_prob(away_sp, home_sp, park_rf, wx_rf)
    _tot_mkt  = market.get("totals")
    _tot_line = _tot_mkt.get("line") if _tot_mkt else None
    total_r   = game_total_prob(away_xr, home_xr, _tot_line) if _tot_line is not None else {}
    f5_away_xr = f5_run_expectancy(away_xr, away_sp)
    f5_home_xr = f5_run_expectancy(home_xr, home_sp)

    # K props (if odds available — placeholder, real odds via props market endpoint)
    away_k_prop = k_prop(away_sp, away_sp.get("k9", 8.5) * 5 / 9,
                          ump_k_factor=ump_k) if away_sp else None
    home_k_prop = k_prop(home_sp, home_sp.get("k9", 8.5) * 5 / 9,
                          ump_k_factor=ump_k) if home_sp else None

    # ── Situational angles ────────────────────────────────────────────────────
    _sit_game_data = {
        "away_tid":           away_tid,
        "home_tid":           home_tid,
        "best_away_odds":     best_away_odds,
        "best_home_odds":     best_home_odds,
        "game_time_et":       _parse_game_time_et(event.get("commence_utc", "")),
        "series_game_number": event.get("series_game_number"),
        "games_in_series":    event.get("games_in_series"),
    }
    situations_result = check_situations(
        away_code, home_code,
        _sit_game_data,
        sp_data={"away": away_sp, "home": home_sp},
        bullpen_data={"away": away_bp, "home": home_bp},
        market_data=market,
        offense_data={"away": away_off, "home": home_off},
    )
    if situations_result.get("triggered"):
        _sit_labels = " | ".join(situations_result.get("labels", []))
        print(f"  SITUATIONS ({situations_result['n_triggered']}): {_sit_labels}"
              + (" ⚡STACK" if situations_result.get("situation_stack") else ""))

    # ── Conviction ────────────────────────────────────────────────────────────
    away_conv = _conviction(away_edge, away_model_p, away_bp, market)
    home_conv = _conviction(home_edge, home_model_p, home_bp, market)

    # ── Sizing ────────────────────────────────────────────────────────────────
    away_stake = kelly_stake(away_model_p, str(best_away_odds), away_conv,
                             edge_pct=away_edge) if best_away_odds else 0.0
    home_stake = kelly_stake(home_model_p, str(best_home_odds), home_conv,
                             edge_pct=home_edge) if best_home_odds else 0.0

    return {
        "game_pk":    game_pk,
        "away":       away_code,
        "home":       home_code,
        "away_name":  away_name,
        "home_name":  home_name,
        "away_sp":    away_sp,
        "home_sp":    home_sp,
        "umpire":     umpire,
        "ump_note":   ump_note,
        "weather":    weather,
        "away_off":   away_off,
        "home_off":   home_off,
        "away_bp":    {
            "fatigue_tier":     away_bp["fatigue_tier"],
            "avg_fatigue":      away_bp["avg_fatigue"],
            "closer":           away_bp["closer_name"],
            "closer_available": away_bp["closer_available"],
            "high_fatigue_arms": away_hi_arms,
        },
        "home_bp":    {
            "fatigue_tier":     home_bp["fatigue_tier"],
            "avg_fatigue":      home_bp["avg_fatigue"],
            "closer":           home_bp["closer_name"],
            "closer_available": home_bp["closer_available"],
            "high_fatigue_arms": home_hi_arms,
        },
        "away_xr":    away_xr,
        "home_xr":    home_xr,
        "away_model_p": away_model_p,
        "home_model_p": home_model_p,
        "away_nv":    away_nv,
        "home_nv":    home_nv,
        "away_edge":  away_edge,
        "home_edge":  home_edge,
        "best_away_odds": best_away_odds,
        "best_home_odds": best_home_odds,
        "best_away_book": market.get("best_away_book"),
        "best_home_book": market.get("best_home_book"),
        "away_conv":  away_conv,
        "home_conv":  home_conv,
        "away_stake": away_stake,
        "home_stake": home_stake,
        "nrfi":          nrfi_r,
        "total":         total_r,
        "f5_away_xr":    f5_away_xr,
        "f5_home_xr":    f5_home_xr,
        "polymarket":    market.get("polymarket"),
        "line_movement": market.get("line_movement"),
        "totals_line":       market.get("totals", {}).get("line") if market.get("totals") else None,
        "totals_best_over":  market.get("totals", {}).get("best_over")  if market.get("totals") else None,
        "totals_best_under": market.get("totals", {}).get("best_under") if market.get("totals") else None,
        # Intelligence layer
        "ml_model":      ml_conf,
        "shap_home":     shap_home,
        "shap_away":     shap_away,
        "ml_total":      ml_total,
        "ml_nrfi_p":     ml_nrfi_p,
        "reg_flags":     reg_flags.get("flags", []),
        "momentum":      momentum,
        "narrative":     narrative.get("flags", []),
        # New engine fields
        "away_lineup_confirmed": not away_off.get("lineup_unconfirmed", True),
        "home_lineup_confirmed": not home_off.get("lineup_unconfirmed", True),
        "away_sp_rolling_era":   away_sp.get("rolling_era_3"),
        "home_sp_rolling_era":   home_sp.get("rolling_era_3"),
        "away_sp_flags":         away_sp_flags,
        "home_sp_flags":         home_sp_flags,
        "away_sp_statcast":      away_sc_str,
        "home_sp_statcast":      home_sc_str,
        "wx_run_adjustment":     wx_adj,
        "wx_label":              wx_label,
        "away_platoon_delta":    away_platoon_delta,
        "home_platoon_delta":    home_platoon_delta,
        # Intelligence flags (SP/offense regression)
        "intel_flags":    intel_flags,
        "injury_flags":   injury_flags,
        # Enhanced momentum
        "away_momentum":  away_momentum,
        "home_momentum":  home_momentum,
        # Game time ET (for picks format and primetime detection)
        "game_time_et":   _parse_game_time_et(event.get("commence_utc", "")),
        "h2h":          h2h,
        "away_recent_win_pct":  away_off.get("recent_win_pct", 0.5),
        "home_recent_win_pct":  home_off.get("recent_win_pct", 0.5),
        # Platoon matchup edge flags
        "away_strong_platoon_adv":  away_off.get("strong_platoon_advantage", False),
        "home_strong_platoon_adv":  home_off.get("strong_platoon_advantage", False),
        "away_platoon_edge":        away_off.get("platoon_edge", 0.0),
        "home_platoon_edge":        home_off.get("platoon_edge", 0.0),
        # SP first inning ERA
        "away_sp_yrfi_lean":   away_sp.get("yrfi_lean", False),
        "home_sp_yrfi_lean":   home_sp.get("yrfi_lean", False),
        "away_sp_fi_era":      away_sp.get("first_inning_era"),
        "home_sp_fi_era":      home_sp.get("first_inning_era"),
        # SP tier labels for Telegram format
        "away_sp_tier": _sp_tier(away_sp.get("xfip", 4.35))[0],
        "home_sp_tier": _sp_tier(home_sp.get("xfip", 4.35))[0],
        # Home dog structural edge result
        "home_dog": home_dog_result,
        # 12-factor: pitch trap, framing, key reliever, umpire edge
        "away_pitch_trap":      away_pitch_trap,
        "home_pitch_trap":      home_pitch_trap,
        "away_framing":         away_framing_res,
        "home_framing":         home_framing_res,
        "away_key_rel_avail":   away_key_rel_avail,
        "home_key_rel_avail":   home_key_rel_avail,
        "away_slot_run_adj":    _away_slot_adj,
        "home_slot_run_adj":    _home_slot_adj,
        # Savant pipeline signals
        "away_xwoba_against":   _away_xwoba,
        "home_xwoba_against":   _home_xwoba,
        "away_rolling_tier":    _away_roll_tier,
        "home_rolling_tier":    _home_roll_tier,
        "away_tempo_label":     away_sp.get("tempo_label"),
        "home_tempo_label":     home_sp.get("tempo_label"),
        "away_arm_angle":       away_sp.get("arm_angle"),
        "home_arm_angle":       home_sp.get("arm_angle"),
        "away_fps_model_adj":   _away_fps_adj,
        "home_fps_model_adj":   _home_fps_adj,
        # Umpire engine edge
        "ump_edge":             ump_edge,
        # Sharp money signals (for confidence adjustment and FLAGS)
        "market_sharp_signal":  (market.get("reverse_line") or {}).get("sharp_side", ""),
        "market_line_direction": _lm_dir,
        "market_line_magnitude": _lm_mag,
        "ump_note":             ump_note,
        # Team IDs (needed by situations engine and pattern tracking)
        "away_tid":             away_tid,
        "home_tid":             home_tid,
        # Situational angles
        "situations":           situations_result,
    }


def _parse_game_time_et(commence_utc: str) -> str:
    """Convert UTC ISO string to '7:05 PM ET' format."""
    if not commence_utc:
        return ""
    try:
        dt_utc = datetime.fromisoformat(commence_utc.replace("Z", "+00:00"))
        dt_et  = dt_utc.astimezone(ET)
        return dt_et.strftime("%-I:%M %p ET")
    except Exception:
        return ""


def _game_in_window(commence_utc: str, window: str) -> bool:
    """True if the game's ET start time falls within the requested scout window.

    Windows:
      day     — before 3:00pm ET  (Run 1, 11am)
      evening — 3:00pm–8:00pm ET  (Run 2, 4pm)
      west    — 8:00pm+ ET        (Run 3, 7:30pm)
      all     — no filter         (manual / on-demand)
    """
    if window == "all" or not commence_utc:
        return True
    try:
        dt_et = datetime.fromisoformat(commence_utc.replace("Z", "+00:00")).astimezone(ET)
        hour  = dt_et.hour + dt_et.minute / 60.0
        if window == "day":      return hour < 15.0           # before 3pm ET
        if window == "evening":  return 15.0 <= hour < 20.0   # 3–8pm ET
        if window == "west":     return hour >= 20.0           # 8pm+ ET
    except Exception:
        pass
    return True  # include if time is unparseable


def _momentum_score(away_code: str, home_code: str) -> dict:
    """Legacy simple momentum proxy (kept for fallback use)."""
    from memory_engine import team_prior
    away_m = team_prior(away_code, "home", 7) or 0.50
    home_m = team_prior(home_code, "home", 7) or 0.50
    return {
        "away": round(away_m - 0.50, 3),
        "home": round(home_m - 0.50, 3),
    }


def _narrative_flags(market: dict, away_nv: float, home_nv: float) -> dict:
    """
    Detect public narrative / fade opportunities:
    line steam, Polymarket divergence, primetime inflation,
    public bias, and sharp reverse line movement.
    """
    flags = []
    lm = market.get("line_movement") or {}

    # Line steam
    direction = lm.get("direction", "")
    magnitude = lm.get("magnitude", 0)
    if magnitude > 0.08 and direction not in ("unknown", "stable"):
        fading_side = "away" if "home" in direction else "home"
        flags.append({
            "type":    "LINE_STEAM",
            "message": f"Large line move {direction} ({magnitude:.3f}) — consider fade of {fading_side}",
        })

    # Polymarket vs sharp book gap > 15%
    poly = market.get("polymarket") or {}
    for side in ("away", "home"):
        poly_p = poly.get(side)
        nv_p   = away_nv if side == "away" else home_nv
        if poly_p and nv_p:
            gap = abs(poly_p - nv_p)
            if gap > 0.15:
                flags.append({
                    "type":    "POLY_DIVERGENCE",
                    "message": f"Polymarket {side} {poly_p:.1%} vs sharp {nv_p:.1%} — {gap*100:.0f}pt gap",
                })

    # Primetime public inflation
    pt = market.get("primetime") or {}
    if pt.get("primetime"):
        flags.append({
            "type":    "PRIMETIME_PUBLIC_OVERREACTION",
            "message": pt.get("message", "Nationally televised — expect public inflation on favorite"),
        })

    # Public bias (square vs sharp book gap)
    pb = market.get("public_bias") or {}
    if pb.get("fade_signal"):
        flags.append({
            "type":    "PUBLIC_FADE_CANDIDATE",
            "message": pb.get("message", "Public bias detected"),
        })

    # Sharp reverse line movement
    rlm = market.get("reverse_line") or {}
    if rlm.get("sharp_side"):
        flags.append({
            "type":    "SHARP_REVERSE",
            "message": rlm.get("message", "Sharp reverse line movement detected"),
        })

    return {"flags": flags}


def _confidence_score(analysis: dict, side: str) -> int:
    """
    0-100 confidence score combining: edge strength 30%, data completeness 25%,
    model decisiveness 20%, recent form 15%, H2H 10%.
    Only bets ≥60 are recommended.
    """
    edge    = analysis.get(f"{side}_edge", 0)
    model_p = analysis.get(f"{side}_model_p", 0.5)

    # 1. Edge strength (0-30): 0 at 5%, 30 at 15%+
    edge_score = min(max((edge - 5.0) * 3.0, 0.0), 30.0)

    # 2. Data completeness (0-25)
    data_score = 0.0
    if analysis.get(f"{side}_lineup_confirmed"):
        data_score += 10
    sp = analysis.get(f"{side}_sp") or {}
    if not sp.get("sp_missing") and sp.get("name", "TBD") not in ("TBD", "", None):
        data_score += 7
    sc_str = analysis.get(f"{side}_sp_statcast", "")
    if sc_str and "[2025]" not in sc_str:   # real 2026 Statcast
        data_score += 8

    # 3. Model decisiveness (0-20): further from 50/50 = more confident
    model_score = min(abs(model_p - 0.5) * 40, 20.0)

    # 4. Recent form (0-15): recent win rate (7d)
    recent_win = analysis.get(f"{side}_recent_win_pct", 0.5)
    form_score = recent_win * 15.0

    # 5. H2H (0-10)
    h2h = analysis.get("h2h") or {}
    if h2h.get("h2h_available"):
        h2h_win = h2h.get(f"{'away' if side == 'away' else 'home'}_win_rate", 0.5)
        h2h_score = h2h_win * 10.0
    else:
        h2h_score = 5.0   # neutral when no H2H data

    total = round(edge_score + data_score + model_score + form_score + h2h_score)
    return min(max(total, 0), 100)


def _conviction(edge_pct: float, model_p: float, bp: dict, market: dict) -> str:
    if edge_pct < MIN_EDGE_PCT:
        return "PASS"
    if edge_pct >= 7 and model_p >= 0.52:
        return "HIGH"
    if edge_pct >= 4 and model_p >= 0.48:
        return "MEDIUM"
    return "PASS"


# ── SP QUALITY TIERS ─────────────────────────────────────────────────────────

def _sp_tier(xfip: float) -> tuple[str, float]:
    """SP quality tier label and signal amplifier multiplier (applied to quality score)."""
    if xfip < 3.25: return "ELITE",   1.18
    if xfip < 3.75: return "GREAT",   1.10
    if xfip < 4.25: return "GOOD",    1.04
    if xfip < 4.75: return "AVERAGE", 1.00
    if xfip < 5.25: return "BAD",     1.06
    return "TERRIBLE", 1.14


def _weighted_win_prob(
    away_xfip: float, home_xfip: float,
    away_bp_fatigue: float, home_bp_fatigue: float,
    away_wrc: float, home_wrc: float,
    home_dog_add: float,
    pyth_away_p: float,
    lm_direction: str, lm_magnitude: float,
    away_platoon_edge: float, home_platoon_edge: float,
    away_momentum_score: float, home_momentum_score: float,
    # Legacy 12-factor inputs (kept for backward compat)
    pitch_trap_away_adj: float = 0.0,
    pitch_trap_home_adj: float = 0.0,
    away_framing_adj: float = 0.0,
    home_framing_adj: float = 0.0,
    away_key_reliever_avail: bool = True,
    home_key_reliever_avail: bool = True,
    ump_home_win_adj: float = 0.0,
    # New Savant-powered factors
    away_xwoba_against: float | None = None,  # SP xwOBA against (ADD 1)
    home_xwoba_against: float | None = None,
    away_pitch_quality_adj: float = 0.0,      # pitch run value + whiff (ADDs 2-3)
    home_pitch_quality_adj: float = 0.0,
    away_rolling_tier: str = "STABLE",        # rolling xwOBA form (ADD 8)
    home_rolling_tier: str = "STABLE",
    away_bp_stuff_adj: float = 0.0,           # bullpen stuff_plus (ADD 19)
    home_bp_stuff_adj: float = 0.0,
    away_bat_tracking_adj: float = 0.0,       # bat tracking (ADD 11)
    home_bat_tracking_adj: float = 0.0,
    park_of_adj: float = 0.0,                 # park + OF defense combined (ADD 16)
    away_yoy_adj: float = 0.0,               # YoY xwOBA change (ADD 9)
    home_yoy_adj: float = 0.0,
    away_fps_adj: float = 0.0,               # ABS/FPS (ADD 7)
    home_fps_adj: float = 0.0,
    away_tempo_adj: float = 0.0,             # pitch tempo (ADD 10)
    home_tempo_adj: float = 0.0,
    away_sprint_adj: float = 0.0,           # baserunning + sprint (ADDs 13,17)
    home_sprint_adj: float = 0.0,
    away_arm_angle_adj: float = 0.0,        # arm angle platoon (ADD 4)
    home_arm_angle_adj: float = 0.0,
    h2h_away_p: float = 0.50,               # H2H win rate (ADD — existing factor)
) -> tuple[float, float]:
    """
    Updated 12-factor weighted win probability (Savant data pipeline expansion).
    Weights:
      SP xwOBA 18%, Pitch quality 12%, Rolling form 7%, Bullpen 15%,
      Offense (wRC++bat tracking) 13%, Pythagorean 8%,
      Platoon+arm angle 8%, Park+weather+OF defense 6%,
      Momentum+YoY 5%, ABS+tempo 3%, Baserunning+sprint 3%, H2H 2%.
    Returns (away_p, home_p) rounded to 4 decimal places.
    """
    # Factor 1 — SP xwOBA against (18%)
    # Convert xwOBA to quality score: lower xwOBA = better pitcher
    # Neutral xwOBA ≈ 0.320; scale so 0.280 → 1.20, 0.360 → 0.80
    _LG_XWOBA = 0.320
    away_xwoba = away_xwoba_against if away_xwoba_against is not None else _LG_XWOBA
    home_xwoba = home_xwoba_against if home_xwoba_against is not None else _LG_XWOBA
    # Fallback: if no xwOBA data, use xFIP quality
    if away_xwoba_against is None:
        _, away_tm = _sp_tier(away_xfip)
        away_sp_q = (1.0 / max(away_xfip, 0.5)) * away_tm
    else:
        away_sp_q = max(0.5, (_LG_XWOBA / max(away_xwoba, 0.10)))
    if home_xwoba_against is None:
        _, home_tm = _sp_tier(home_xfip)
        home_sp_q = (1.0 / max(home_xfip, 0.5)) * home_tm
    else:
        home_sp_q = max(0.5, (_LG_XWOBA / max(home_xwoba, 0.10)))
    sp_denom  = away_sp_q + home_sp_q
    sp_away_p = away_sp_q / sp_denom if sp_denom > 0 else 0.5

    # Factor 2 — Pitch quality: run value + whiff (12%)
    # away_pitch_quality_adj: positive = away SP has better arsenal → away team's
    # pitching dominates → home team scores less → away_p up
    pq_net    = away_pitch_quality_adj - home_pitch_quality_adj
    pq_away_p = max(0.40, min(0.60, 0.50 + pq_net))

    # Factor 3 — Rolling form (10-game xwOBA) (7%)
    # PEAKING pitcher → runs suppressed; DECLINING → more runs allowed
    def _rolling_q(tier: str) -> float:
        return {"PEAKING": 1.10, "STABLE": 1.00, "DECLINING": 0.90, "UNKNOWN": 1.00}.get(tier, 1.00)
    roll_away_q = _rolling_q(away_rolling_tier)
    roll_home_q = _rolling_q(home_rolling_tier)
    roll_denom  = roll_away_q + roll_home_q
    roll_away_p = roll_away_q / roll_denom if roll_denom > 0 else 0.5

    # Factor 4 — Bullpen (xFIP fatigue + stuff_plus) (15%)
    away_bp_q  = 1.0 / (1.0 + max(away_bp_fatigue, 0)) + away_bp_stuff_adj
    home_bp_q  = 1.0 / (1.0 + max(home_bp_fatigue, 0)) + home_bp_stuff_adj
    bp_denom   = away_bp_q + home_bp_q
    bp_away_p  = away_bp_q / bp_denom if bp_denom > 0 else 0.5

    # Factor 5 — Offense (wRC+ + bat tracking) (13%)
    away_off_q = max(away_wrc, 50.0) + away_bat_tracking_adj * 10
    home_off_q = max(home_wrc, 50.0) + home_bat_tracking_adj * 10
    off_denom  = away_off_q + home_off_q
    off_away_p = away_off_q / off_denom if off_denom > 0 else 0.5

    # Factor 6 — Pythagorean + home dog (8%)
    # Blend Pythagorean and home dog structural edge
    pyth_p      = max(0.20, min(0.80, pyth_away_p))
    hdog_away_p = 0.50 - home_dog_add
    pyth_blend  = max(0.20, min(0.80, 0.70 * pyth_p + 0.30 * hdog_away_p))

    # Factor 7 — Platoon + arm angle (8%)
    plat_diff      = (away_platoon_edge or 0) - (home_platoon_edge or 0)
    arm_net        = away_arm_angle_adj - home_arm_angle_adj
    platoon_away_p = max(0.40, min(0.60, 0.50 + plat_diff * 0.0015 + arm_net))

    # Factor 8 — Park + weather + OF defense (6%)
    # park_of_adj: positive = favors away, negative = favors home
    park_away_p = max(0.40, min(0.60, 0.50 + park_of_adj))

    # Factor 9 — Momentum + YoY (5%)
    mom_diff        = (away_momentum_score or 0) - (home_momentum_score or 0)
    yoy_net         = away_yoy_adj - home_yoy_adj  # away SP improved more than home SP → away_p up
    momentum_away_p = max(0.40, min(0.60, 0.50 + mom_diff * 1.5 + yoy_net))

    # Factor 10 — ABS + tempo (3%)
    fps_net   = away_fps_adj - home_fps_adj
    tempo_net = away_tempo_adj - home_tempo_adj
    abs_away_p = max(0.40, min(0.60, 0.50 + fps_net + tempo_net))

    # Factor 11 — Baserunning + sprint speed (3%)
    sprint_net   = away_sprint_adj - home_sprint_adj
    sprint_away_p = max(0.40, min(0.60, 0.50 + sprint_net))

    # Factor 12 — H2H historical (2%)
    h2h_away_p_clamped = max(0.35, min(0.65, h2h_away_p))

    away_p = (
        0.18 * sp_away_p +
        0.12 * pq_away_p +
        0.07 * roll_away_p +
        0.15 * bp_away_p +
        0.13 * off_away_p +
        0.08 * pyth_blend +
        0.08 * platoon_away_p +
        0.06 * park_away_p +
        0.05 * momentum_away_p +
        0.03 * abs_away_p +
        0.03 * sprint_away_p +
        0.02 * h2h_away_p_clamped
    )
    away_p = round(max(0.15, min(0.85, away_p)), 4)
    return away_p, round(1.0 - away_p, 4)


# ── SGP FORMAT ───────────────────────────────────────────────────────────────

def _format_sgp(sgp: dict, game_label: str = "") -> str:
    """Format a correlated SGP suggestion for Telegram."""
    sgp_type = sgp.get("type", "SGP")
    sp_name  = sgp.get("sp_name", "")
    header   = f"🔗 SAME-GAME PARLAY — {sgp_type}"
    if sp_name:
        header += f" ({sp_name})"
    if game_label:
        header += f"\n{game_label}"

    lines = [header]
    for leg in sgp.get("legs", []):
        lines.append(f"  • {leg}")
    lines += [
        f"Correlation: {sgp.get('correlation', '')}",
        f"Joint prob:  {sgp.get('joint_prob', 0):.1%}",
        f"Kelly stake: ${sgp.get('kelly_stake', 0):.2f}",
        f"EV:          {sgp.get('ev', 0):+.4f}",
    ]
    return "\n".join(lines)


# ── SERIES BETTING MODEL ─────────────────────────────────────────────────────
_SENT_SERIES: set = set()   # tracks (away_code, home_code, game_date) already messaged


def _fetch_polymarket_series(team_code: str, team_name: str) -> float | None:
    """Try to fetch Polymarket series winner probability. Returns 0-1 float or None."""
    try:
        r = _http_get(
            "https://clob.polymarket.com/markets",
            params={"tag": "mlb", "closed": "false"},
            timeout=8,
        )
        raw = r.json()
        markets = raw.get("data", []) if isinstance(raw, dict) else raw
        if not isinstance(markets, list):
            return None
        needle = team_name.lower().split()[-1]  # e.g. "dodgers"
        for mkt in markets:
            q = mkt.get("question", "").lower()
            if "series" not in q or needle not in q:
                continue
            for tok in mkt.get("tokens", []):
                if needle in tok.get("outcome", "").lower():
                    price = float(tok.get("price") or 0)
                    if 0 < price < 1:
                        return price
    except Exception:
        pass
    return None


def _series_analysis(events: list, game_date: str, game_analyses: dict) -> None:
    """
    For each game 1 of a series today, check SP xFIP advantage across all series games.
    If one team has xFIP advantage ≥ 0.8 in 2+ games, alert with series win probability.
    game_analyses: {(away_code, home_code): analysis_dict}
    """
    from datetime import date as _date, timedelta

    try:
        r = _http_get(
            f"{STATSAPI}/schedule",
            params={"sportId": 1, "date": game_date, "hydrate": "game,team"},
            timeout=8,
        )
        schedule_games = [
            g
            for gd in r.json().get("dates", [])
            for g in gd.get("games", [])
        ]
    except Exception:
        return

    for g in schedule_games:
        if g.get("seriesGameNumber") != 1:
            continue
        n_games = g.get("gamesInSeries", 0)
        if n_games < 2:
            continue

        away_name = g.get("teams", {}).get("away", {}).get("team", {}).get("name", "")
        home_name = g.get("teams", {}).get("home", {}).get("team", {}).get("name", "")
        away_code = MLB_TEAM_MAP.get(away_name, "")
        home_code = MLB_TEAM_MAP.get(home_name, "")
        if not away_code or not home_code:
            continue

        # Find matching analysis
        analysis = game_analyses.get((away_code, home_code))
        if analysis is None:
            continue

        away_sp_g1 = analysis.get("away_sp", {}) or {}
        home_sp_g1 = analysis.get("home_sp", {}) or {}
        away_p_g1  = analysis.get("away_model_p", 0.5)
        home_p_g1  = analysis.get("home_model_p", 0.5)

        xfip_edges = {away_code: 0, home_code: 0}  # count of games with 0.8+ xFIP edge
        xfip_details: list[str] = []

        # Game 1
        ax1 = away_sp_g1.get("xfip", 4.35)
        hx1 = home_sp_g1.get("xfip", 4.35)
        diff1 = hx1 - ax1  # positive = away SP dominant
        if abs(diff1) >= 0.8:
            winner = away_code if diff1 > 0 else home_code
            xfip_edges[winner] += 1
            xfip_details.append(f"G1 {winner} {abs(diff1):.2f} xFIP edge")

        # Games 2..n — fetch from schedule
        game_probs: dict[str, list[float]] = {away_code: [away_p_g1], home_code: [home_p_g1]}
        for offset in range(1, n_games):
            future = (_date.fromisoformat(game_date) + timedelta(days=offset)).isoformat()
            try:
                fr = _http_get(
                    f"{STATSAPI}/schedule",
                    params={"sportId": 1, "date": future, "hydrate": "game,team"},
                    timeout=8,
                )
                for fgd in fr.json().get("dates", []):
                    for fg in fgd.get("games", []):
                        fa = MLB_TEAM_MAP.get(
                            fg.get("teams", {}).get("away", {}).get("team", {}).get("name", ""), "")
                        fh = MLB_TEAM_MAP.get(
                            fg.get("teams", {}).get("home", {}).get("team", {}).get("name", ""), "")
                        if {fa, fh} != {away_code, home_code}:
                            continue
                        fpk = fg.get("gamePk")
                        if fpk:
                            fsps = get_game_sps(fpk, fa, fh, "")
                            fax = (fsps.get("away") or {}).get("xfip", 4.35)
                            fhx = (fsps.get("home") or {}).get("xfip", 4.35)
                            fdiff = fhx - fax
                            if abs(fdiff) >= 0.8:
                                fw = fa if fdiff > 0 else fh
                                xfip_edges[fw] += 1
                                xfip_details.append(
                                    f"G{offset+1} {fw} {abs(fdiff):.2f} xFIP edge"
                                )
                        # Use 0.5 as proxy for future game probs
                        game_probs.setdefault(away_code, []).append(0.5)
                        game_probs.setdefault(home_code, []).append(0.5)
            except Exception:
                pass

        # Check for 2+ game advantage
        series_team = max(xfip_edges, key=xfip_edges.get)
        if xfip_edges[series_team] < 2:
            continue

        # Series win probability — 3-game series formula
        team_is_away = (series_team == away_code)
        ps = game_probs.get(series_team, [0.5, 0.5, 0.5])
        p1 = ps[0] if len(ps) > 0 else 0.5
        p2 = ps[1] if len(ps) > 1 else 0.5
        p3 = ps[2] if len(ps) > 2 else 0.5

        if n_games == 2:
            series_prob = round(p1 * p2 + p1 * (1 - p2) * 0.5 + (1 - p1) * p2 * 0.5, 3)
        else:
            series_prob = round(
                p1 * p2
                + p1 * (1 - p2) * p3
                + (1 - p1) * p2 * p3,
                3,
            )

        team_name = away_name if team_is_away else home_name
        opp_name  = home_name if team_is_away else away_name

        # Try Polymarket
        poly_prob = _fetch_polymarket_series(series_team, team_name)

        if poly_prob is not None:
            edge_vs_poly = round(series_prob * 100 - poly_prob * 100, 1)
            if edge_vs_poly < 5.0:
                continue  # not enough edge
            poly_display = f"{round(poly_prob * 100, 1)}¢"
            edge_str     = f"+{edge_vs_poly}%"
        else:
            poly_display = "N/A"
            edge_str     = "N/A (Polymarket not available)"

        br_now      = current_bankroll()
        series_stake = round(br_now * 0.02, 2)
        model_pct   = round(series_prob * 100, 1)
        sp_adv_str  = " | ".join(xfip_details[:3])

        series_key = (
            min(away_code, home_code),
            max(away_code, home_code),
            game_date,
        )
        if series_key in _SENT_SERIES:
            continue
        _SENT_SERIES.add(series_key)

        msg = "\n".join([
            "🏆 SERIES EDGE",
            f"{team_name} vs {opp_name} — {n_games}-game series starting {game_date}",
            f"SP advantage: {series_team} — {sp_adv_str}",
            f"Model series win prob: {model_pct}%",
            f"Polymarket series price: {poly_display}",
            f"Edge: {edge_str}",
            f"STAKE: ${series_stake:.2f} — series winner market on Polymarket",
        ])
        _send_telegram(msg)


# ── DAILY BET SLIP ────────────────────────────────────────────────────────────

def _daily_bet_slip(
    all_locks: list,
    all_flips: list,
    all_props: list,
    all_fades: list,
    br: float,
    all_nrfi: list | None = None,
    all_totals: list | None = None,
    all_hitter_props: list | None = None,
    all_k_props: list | None = None,
    all_injuries: list | None = None,
) -> bool:
    """Send the daily bet slip as 3 labeled Telegram messages.
    Returns True only if Part 1 was sent (HTTP 200) AND contained at least 1 bet."""
    print(
        f"[SLIP] _daily_bet_slip called — "
        f"locks={len(all_locks)} flips={len(all_flips)} props={len(all_props)} "
        f"fades={len(all_fades)} nrfi={len(all_nrfi or [])} totals={len(all_totals or [])} "
        f"k_props={len(all_k_props or [])} hitter_props={len(all_hitter_props or [])} "
        f"injuries={len(all_injuries or [])} br=${br:.2f}"
    )
    today = date.today().strftime("%b %d, %Y")

    locks       = all_locks[:MAX_LOCKS_PER_DAY]
    flips       = all_flips[:MAX_FLIPS_PER_DAY]
    nrfi_bets   = list(all_nrfi or [])
    totals_bets = list(all_totals or [])
    hitter_bets = list(all_hitter_props or [])
    k_bets      = list(all_k_props or [])
    injuries    = list(all_injuries or [])

    # Hard filter: never send $0-stake or negative-EV props
    nrfi_bets   = [b for b in nrfi_bets   if (b.get("stake") or 0) > 0]
    totals_bets = [b for b in totals_bets if (b.get("stake") or 0) > 0]
    k_bets      = [b for b in k_bets      if (b.get("stake") or 0) > 0]
    hitter_bets = [h for h in hitter_bets if (h.get("stake") or 0) > 0]
    all_props   = [p for p in (all_props or [])
                   if (p.get("kelly_stake") or 0) > 0 and (p.get("ev") or 0) >= 0]

    # Props: max 5 per day (MAX_PROPS_PER_DAY), sorted by edge, no model > market by 25%+
    def _no_model_blowout(item: dict) -> bool:
        mp = item.get("model_prob") or item.get("p_over", 0)
        mkt = item.get("market_p", 0.5)
        return (mp - mkt) < 0.25

    k_bets      = sorted(k_bets,      key=lambda b: b.get("edge_pct", 0), reverse=True)
    k_bets      = [b for b in k_bets if _no_model_blowout(b)]
    hitter_bets = sorted(hitter_bets, key=lambda h: h.get("edge_pct", 0), reverse=True)
    hitter_bets = [h for h in hitter_bets if _no_model_blowout(h)]
    totals_bets = sorted(totals_bets, key=lambda b: b.get("edge_pct", 0), reverse=True)[:5]

    n_locks = len(locks)
    _has_props_today = bool(nrfi_bets or totals_bets or k_bets or hitter_bets or all_props)
    day_cls = day_classification(n_locks, len(flips), _has_props_today)
    s_mult  = day_cls["stake_mult"]
    print(
        f"[SLIP] Build started — locks={len(locks)} flips={len(flips)} "
        f"day={day_cls['color']} ml_allowed={day_cls.get('ml_allowed', True)} "
        f"stake_mult={s_mult}"
    )

    def _ml_stake(analysis, side):
        raw   = (analysis.get(f"{side}_stake") or 0) * s_mult
        stake = round(max(raw, MIN_STAKE), 2)
        if 0 < raw < MIN_STAKE:
            _team = analysis.get(f"{side}_name", side)
            print(f"[KELLY] WARNING — stake below minimum for {_team}, forcing to ${MIN_STAKE:.2f}")
        return stake

    # Pool budgets for this slip
    ml_pool_rem      = pool_remaining("ML", br)
    props_pool_rem   = pool_remaining("PROPS", br)
    parlay_pool_rem  = pool_remaining("PARLAY", br)
    budget           = daily_budget(br)

    # Enforce props pool budget — admit highest-edge props first until pool is exhausted
    _props_spent = 0.0
    _capped_hitter: list = []
    for _h in hitter_bets:
        _s = float(_h.get("stake") or 0)
        if _props_spent + _s > props_pool_rem:
            break
        _capped_hitter.append(_h)
        _props_spent += _s
    hitter_bets = _capped_hitter

    _capped_k: list = []
    for _b in k_bets:
        _s = float(_b.get("stake") or 0)
        if _props_spent + _s > props_pool_rem:
            break
        _capped_k.append(_b)
        _props_spent += _s
    k_bets = _capped_k

    _capped_nrfi: list = []
    for _b in nrfi_bets:
        _s = float(_b.get("stake") or 0)
        if _props_spent + _s > props_pool_rem:
            break
        _capped_nrfi.append(_b)
        _props_spent += _s
    nrfi_bets = _capped_nrfi

    # Parlay: HIGH conviction locks first; fall back to top picks by confidence
    # Rules: ≤3 legs, no leg worse than -180, combined model prob threshold
    # Stake: min(15% of daily budget, 1.5% of bankroll)
    parlay_candidates = []
    parlay_label = "PARLAY (HIGH conviction ML only):"
    _prl_min_prob = 0.35
    for a, s in locks:
        if a.get(f"best_{s}_odds") is None:
            continue
        odds_val = a.get(f"best_{s}_odds")
        try:
            if int(str(odds_val).replace("+", "")) < -180:
                continue   # skip heavy juice legs
        except (ValueError, TypeError):
            pass
        parlay_candidates.append((a, s))
        if len(parlay_candidates) == 3:
            break

    # Fall back to top picks by confidence when < 2 HIGH conviction locks
    if len(parlay_candidates) < 2 and (locks + flips):
        _fb_pool = sorted(
            locks + flips,
            key=lambda x: x[0].get(f"{x[1]}_confidence_score", 0),
            reverse=True,
        )
        _fb_cands: list = []
        for _a, _s in _fb_pool:
            _odds_v = _a.get(f"best_{_s}_odds")
            if _odds_v is None:
                continue
            try:
                if int(str(_odds_v).replace("+", "")) < -180:
                    continue
            except (ValueError, TypeError):
                pass
            _fb_cands.append((_a, _s))
            if len(_fb_cands) == 3:
                break
        if len(_fb_cands) >= 2:
            _combined_fb_p = 1.0
            for _a, _s in _fb_cands:
                _combined_fb_p *= _a.get(f"{_s}_model_p", 0.5)
            if _combined_fb_p >= 0.28:
                parlay_candidates = _fb_cands
                parlay_label = "BEST AVAILABLE PARLAY (top picks by confidence):"
                _prl_min_prob = 0.28
                print(f"  [PARLAY] No lock candidates — using best-available fallback (combined_p={_combined_fb_p:.1%})")

    prl_valid = False
    prl_stake = 0.0
    prl_win   = 0.0
    prl_data  = None
    if len(parlay_candidates) >= 2 and day_cls["ml_allowed"]:
        odds_strs = [str(a.get(f"best_{s}_odds", "")) for a, s in parlay_candidates]
        prl = parlay_odds(odds_strs)
        if prl.get("valid"):
            # Combined no-vig probability check
            combined_model_p = 1.0
            for a, s in parlay_candidates:
                combined_model_p *= a.get(f"{s}_model_p", 0.5)
            if combined_model_p >= _prl_min_prob:
                prl_stake = round(min(budget * 0.15, br * 0.015) * s_mult, 2)
                prl_win   = round((prl["decimal"] - 1) * prl_stake, 2)
                prl_valid = True
                prl_data  = prl
            else:
                print(f"  [PARLAY] Skip: combined model prob {combined_model_p:.1%} < {_prl_min_prob:.0%}")

    # Risk totals
    ml_risk = 0.0
    ml_win  = 0.0
    for analysis, side in locks + flips:
        stake = _ml_stake(analysis, side)
        odds  = analysis.get(f"best_{side}_odds")
        if odds and stake > 0:
            dec = american_to_decimal(str(odds))
            if dec:
                ml_risk += stake
                ml_win  += round((dec - 1) * stake, 2)
    if prl_valid:
        ml_risk += prl_stake
        ml_win  += prl_win

    # Compute props_risk only from bets that appear in the slip.
    nrfi_risk   = sum(b.get("stake", 0) for b in nrfi_bets)
    totals_risk = sum(b.get("stake", 0) for b in totals_bets)
    sgp_risk    = sum(p.get("kelly_stake", 0) or 0 for p in all_props[:2])
    # player_props_risk filled after all_player_props is assembled (below)
    total_risk = round(ml_risk, 2)   # updated after player props built
    total_win  = round(ml_win, 2)

    cap          = budget
    override_cap = os.getenv("OVERRIDE_RISK_CAP", "").lower() in ("1", "true", "yes")
    # cap_exceeded recomputed after all_player_props is built (total_risk updated then)

    # Growth tracker for header
    gt = growth_tracker()
    dd_status = drawdown_tier()

    # ── PART 1: ML PICKS ──────────────────────────────────────────────────────
    p1 = [
        f"PARLAY OS — {today} — {day_cls['color']} {day_cls['emoji']}",
        "PART 1/3 — ML PICKS",
        "",
        # Bankroll status
        f"💰 Bankroll: ${br:.2f} | Peak: ${peak_bankroll():.2f}",
    ]
    if dd_status["pct"] > 0:
        dd_icon = "🚨" if dd_status["pause"] else ("⚠️" if dd_status["tier"] >= 2 else "📉")
        p1.append(f"{dd_icon} Drawdown: {dd_status['pct']:.1f}% (tier {dd_status['tier']})"
                  + (" — PAUSED" if dd_status["pause"] else ""))
    p1.append(
        f"📊 Growth: week {gt['week_pct']:+.1f}% | month {gt['month_pct']:+.1f}% "
        f"| all-time {gt['all_time_pct']:+.1f}% | pace ${gt['monthly_pace']:+.2f}/mo"
    )
    p1.append(
        f"🏦 Pools — ML: ${ml_pool_rem:.2f} | PROPS: ${props_pool_rem:.2f} | "
        f"PARLAY: ${parlay_pool_rem:.2f} | Budget: ${budget:.2f}"
    )
    p1.append(f"ML risk: ${ml_risk:.2f} | To win: ${ml_win:.2f}")
    p1.append("")

    # Best bet of the day (highest edge among all picks)
    _all_picks = [(a, s) for a, s in locks + flips]
    if _all_picks:
        _best_a, _best_s = max(_all_picks, key=lambda x: x[0].get(f"{x[1]}_confidence_score", 0))
        _best_team   = _best_a.get(f"{_best_s}_name", "")
        _best_edge   = _best_a.get(f"{_best_s}_edge", 0)
        _best_conf   = _best_a.get(f"{_best_s}_confidence_score", 0)
        _best_odds   = _best_a.get(f"best_{_best_s}_odds", "")
        _best_odds_s = (f"+{_best_odds}" if isinstance(_best_odds, int) and _best_odds > 0 else str(_best_odds or ""))
        _opp_s       = "home" if _best_s == "away" else "away"
        _opp_sp      = (_best_a.get(f"{_opp_s}_sp") or {})
        _best_off    = (_best_a.get(f"{_best_s}_off") or {})
        # Two-sentence plain English explanation
        _r1 = f"Model gives {_best_team} a +{_best_edge:.1f}% edge with {_best_conf}/100 confidence score"
        _osp_name = _opp_sp.get("name", "")
        _osp_xfip = _opp_sp.get("xfip")
        _plat_adv = _best_a.get(f"{_best_s}_strong_platoon_adv", False)
        _h2h      = (_best_a.get("h2h") or {})
        _h2h_wr   = _h2h.get(f"{_best_s}_win_rate") if _h2h.get("h2h_available") else None
        if _osp_xfip and _osp_name and not _opp_sp.get("sp_missing"):
            if _osp_xfip < 4.0:
                _r2 = f"Facing {_osp_name} (xFIP {_osp_xfip:.2f}) who allows hard contact — run support expected."
            else:
                _r2 = f"Opposing {_osp_name} carries ERA-to-xFIP regression risk and weakening peripherals."
        elif _plat_adv:
            _r2 = f"Strong platoon advantage ({_best_a.get(f'{_best_s}_platoon_edge', 0):+.0f} wRC+ pts) favors this lineup today."
        elif _h2h_wr is not None:
            _r2 = f"H2H history shows {_best_team} wins {_h2h_wr:.0%} of matchups in this series."
        else:
            _r2 = f"Recent form and market inefficiency both point toward this play."
        p1.append(f"⭐ BEST BET TODAY: {_best_team} ML {_best_odds_s} — {_r1}. {_r2}")
        p1.append("")

    p1.append(f"🔒 LOCKS ({n_locks} — HIGH conviction 7%+ edge):")
    if locks:
        for analysis, side in locks:
            stake  = _ml_stake(analysis, side)
            odds   = analysis.get(f"best_{side}_odds", "")
            edge   = analysis.get(f"{side}_edge", 0)
            team   = analysis.get(f"{side}_name", "")
            game   = f"{analysis.get('away_name','')} @ {analysis.get('home_name','')}"
            odds_s = (f"+{odds}" if isinstance(odds, int) and odds > 0 else str(odds or ""))
            conf = analysis.get(f"{side}_confidence_score", 0)
            p1.append(f"  {game} — {team} ML {odds_s} — ${stake:.2f} — EDGE: +{edge:.1f}% — CONF: {conf}/100")
    else:
        p1.append("  None today")
    p1.append("")

    p1.append(f"🪙 COIN FLIPS ({len(flips)} — MEDIUM conviction 4-7% edge):")
    if flips:
        for analysis, side in flips:
            stake  = _ml_stake(analysis, side)
            odds   = analysis.get(f"best_{side}_odds", "")
            edge   = analysis.get(f"{side}_edge", 0)
            team   = analysis.get(f"{side}_name", "")
            game   = f"{analysis.get('away_name','')} @ {analysis.get('home_name','')}"
            odds_s = (f"+{odds}" if isinstance(odds, int) and odds > 0 else str(odds or ""))
            conf = analysis.get(f"{side}_confidence_score", 0)
            p1.append(f"  {game} — {team} ML {odds_s} — ${stake:.2f} — EDGE: +{edge:.1f}% — CONF: {conf}/100")
    else:
        p1.append("  None today")
    p1.append("")

    if prl_valid and prl_data:
        leg_parts = []
        for a, s in parlay_candidates:
            t     = a.get(f"{s}_name", "")
            o     = a.get(f"best_{s}_odds", "")
            o_str = (f"+{o}" if isinstance(o, int) and o > 0 else str(o or ""))
            leg_parts.append(f"{t} ML {o_str}")
        p1.append(parlay_label)
        p1.append(f"  {' + '.join(leg_parts)}")
        p1.append(f"  ({prl_data['american']}) — ${prl_stake:.2f} — to win ${prl_win:.2f}")
        p1.append("")

    if injuries:
        p1.append("⚠️ INJURIES:")
        for inj in injuries:
            p1.append(inj)
        p1.append("")

    if day_cls["color"] == "YELLOW":
        p1.append("⚠ YELLOW day — no ML picks qualified, props only")
    elif day_cls["color"] == "RED":
        p1.append("🔴 RED day — no picks of any kind qualified today")

    _has_bets = bool(locks or flips)
    print(
        f"[SLIP] Part 1 pre-send — {len(p1)} lines | "
        f"locks={len(locks)} flips={len(flips)} has_bets={_has_bets}"
    )
    print(f"[SLIP] Sending PART 1/3 ({len(p1)} lines)...")
    _p1_ok = _send_telegram("\n".join(p1))

    # ── PART 2: PLAYER PROPS + NRFI/YRFI ─────────────────────────────────────
    # Normalise k_bets and hitter_bets into a unified list, sorted by edge desc.
    def _market_odds_str(market_p: float) -> str:
        mp = max(min(market_p, 0.99), 0.01)
        if mp >= 0.5:
            return f"-{round(mp / (1.0 - mp) * 100)}"
        return f"+{round((1.0 - mp) / mp * 100)}"

    def _norm_k(b: dict) -> dict:
        last = b["sp"].split()[-1]
        # Flag K props backed by 2025 Statcast data (less reliable for current year)
        sc_flag = " [2025 data]" if b.get("statcast_2025") else ""
        leg  = f"{last} O{b['line']}K{sc_flag}"
        return {
            "player":    b["sp"],
            "team":      b.get("team", ""),
            "stat":      f"Ks O{b['line']}{sc_flag}",
            "odds_str":  "-110",
            "model_pct": round(b["p_over"] * 100, 1),
            "model_p":   b["p_over"],
            "market_p":  0.5,   # K props use -110 baseline → 0.5 market_p
            "edge_pct":  b["edge_pct"],
            "stake":     b["stake"],
            "leg_label": leg,
        }

    def _norm_h(h: dict) -> dict:
        prop  = h["prop"]   # e.g. "Hits O1.5"
        parts = prop.split(" O", 1)
        if len(parts) == 2:
            stat_name, line_s = parts[0], parts[1]
            last = h["player"].split()[-1]
            leg  = f"{last} O{line_s} {stat_name}"
        else:
            stat_name = prop
            leg = f"{h['player'].split()[-1]} {prop}"
        mp = h.get("market_p", 0.5)
        return {
            "player":    h["player"],
            "team":      h.get("team", ""),
            "stat":      prop,
            "odds_str":  _market_odds_str(mp),
            "model_pct": round(h["model_prob"] * 100, 1),
            "model_p":   h.get("model_prob", 0),
            "edge_pct":  h["edge_pct"],
            "stake":     h["stake"],
            "leg_label": leg,
        }

    # k_bets carry statcast_2025 flag from the scout; pass it through for the label
    all_player_props = (
        [_norm_k(b) for b in k_bets] +
        [_norm_h(h) for h in hitter_bets]
    )
    all_player_props = sorted(all_player_props, key=lambda x: x["edge_pct"], reverse=True)
    all_player_props = [p for p in all_player_props if p["edge_pct"] >= 5.0]
    # Enforce max 5 props per day (top 5 by edge, blowout-filtered above)
    all_player_props = all_player_props[:MAX_PROPS_PER_DAY]

    prop_locks = [p for p in all_player_props if p["edge_pct"] >= 10.0]
    prop_flips = [p for p in all_player_props if 5.0 <= p["edge_pct"] < 10.0]

    # ── Final risk total — only from bets actually shown in the slip ──────────
    player_props_risk = sum(p["stake"] for p in all_player_props)
    props_risk  = nrfi_risk + totals_risk + sgp_risk + player_props_risk
    total_risk  = round(ml_risk + props_risk, 2)
    if total_risk > cap and not override_cap:
        print(f"[SLIP] WARNING: displayed risk ${total_risk:.2f} > budget ${cap:.2f} "
              f"— scout cap should have blocked extras")

    has_p2 = bool(nrfi_bets or all_player_props or all_props)
    p2 = [
        f"PARLAY OS — {today}",
        "PART 2/3 — PLAYER PROPS + NRFI/YRFI",
        "",
    ]

    if nrfi_bets:
        p2.append(f"🌅 NRFI/YRFI ({len(nrfi_bets)} bets):")
        for bet in nrfi_bets:
            p2.append(f"  {bet['game']} — {bet['direction']} ({bet['prob']:.1%}) — ${bet['stake']:.2f}")
        p2.append("")

    # ── Props parlay: top 3 locks only ──────────────────────────────────────
    parlay_legs = prop_locks[:3]
    if len(parlay_legs) >= 2:
        prl = parlay_odds([leg["odds_str"] for leg in parlay_legs])
        if prl.get("valid"):
            combined_dec = prl["decimal"]
            joint_p      = 1.0
            for _leg in parlay_legs:
                joint_p *= _leg["model_p"]
            k_num = joint_p * (combined_dec - 1) - (1.0 - joint_p)
            k_den = combined_dec - 1
            prl_kelly_pct = max(0.0, (k_num / k_den) * 0.25) if k_den > 0 else 0.0
            prl_kelly_pct = min(prl_kelly_pct, 0.015)
            prl_stake = round(min(br * prl_kelly_pct, 5.0), 2)
            prl_win   = round((combined_dec - 1) * prl_stake, 2)
            legs_str  = " + ".join(leg["leg_label"] for leg in parlay_legs)
            p2.append(f"PROPS PARLAY (top {len(parlay_legs)} by edge, locks only):")
            p2.append(f"  {legs_str}")
            p2.append(f"  Combined odds: {prl['american']} — Stake: ${prl_stake:.2f} — To win: ${prl_win:.2f}")
            p2.append("")

    def _prop_line(p: dict) -> str:
        odds_s = p["odds_str"]
        if odds_s and odds_s[0] not in ("+", "-"):
            odds_s = f"+{odds_s}"
        return (
            f"  {p['player']} ({p['team']}) — {p['stat']} {odds_s} — "
            f"${p['stake']:.2f} — EDGE: +{p['edge_pct']:.1f}% — MODEL: {p['model_pct']:.1f}%"
        )

    if prop_locks:
        p2.append(f"🔒 LOCKS ({len(prop_locks)} — edge 10%+):")
        for prop in prop_locks:
            p2.append(_prop_line(prop))
        p2.append("")

    if prop_flips:
        p2.append(f"🪙 COIN FLIPS ({len(prop_flips)} — edge 5-10%):")
        for prop in prop_flips:
            p2.append(_prop_line(prop))
        p2.append("")

    if all_props:
        p2.append(f"🔗 SAME-GAME PARLAY ({len(all_props[:2])}):")
        for prop in all_props[:2]:
            legs_str = " + ".join(str(l) for l in prop.get("legs", [])[:2])
            stake    = prop.get("kelly_stake", 0) or 0
            ev       = prop.get("ev", 0) or 0
            ptype    = prop.get("type", "SGP")
            p2.append(f"  [{ptype}] {legs_str} — ${stake:.2f} — EV: {ev:+.4f}")
        p2.append("")

    if not has_p2:
        p2.append("No props with edge today.")

    print(f"[SLIP] Sending PART 2/3 ({len(p2)} lines)...")
    _send_telegram("\n".join(p2))

    # ── PART 3: TOTALS + FADES + RISK SUMMARY ────────────────────────────────
    p3 = [
        f"PARLAY OS — {today}",
        "PART 3/3 — TOTALS + FADES + RISK",
        "",
    ]

    if totals_bets:
        p3.append(f"📊 TOTALS ({len(totals_bets)} bets):")
        for bet in totals_bets:
            p3.append(
                f"  {bet['game']} — {bet['direction']} {bet['line']} ({bet['prob']:.1%}) — "
                f"${bet['stake']:.2f} — EDGE: +{bet['edge_pct']:.1f}%"
            )
        p3.append("")
    else:
        p3.append("📊 TOTALS: None today")
        p3.append("")

    if all_fades:
        p3.append("❌ FADES:")
        seen_fade_teams:   set = set()
        seen_fade_reasons: set = set()
        count = 0
        for analysis, side, reason in all_fades:
            team = analysis.get(f"{side}_name", "")
            if team in seen_fade_teams or reason in seen_fade_reasons or count >= 4:
                continue
            seen_fade_teams.add(team)
            seen_fade_reasons.add(reason)
            p3.append(f"  {team} — {reason}")
            count += 1
        p3.append("")

    risk_cap_pct = round(total_risk / br * 100, 1) if br > 0 else 0
    cap_exceeded = total_risk > cap
    budget_tier  = round(daily_budget_pct(br) * 100) if br > 0 else 15
    p3.append(
        f"Daily risk: ${total_risk:.2f} ({risk_cap_pct:.1f}% of bankroll) | "
        f"Budget: ${cap:.2f} ({budget_tier:.0f}% tier)"
    )
    _parlay_shown = round(prl_stake, 2) if prl_valid else 0.0
    p3.append(
        f"  ML: ${ml_risk:.2f} | Props: ${round(nrfi_risk + totals_risk + player_props_risk, 2):.2f} "
        f"| Parlay: ${_parlay_shown:.2f}"
    )

    if cap_exceeded:
        if override_cap:
            p3.append(f"⚠️ BUDGET OVERRIDE ACTIVE — ${total_risk:.2f} exceeds ${cap:.2f}")
        else:
            p3.append(f"🚨 BUDGET HIT — ${total_risk:.2f} > ${cap:.2f} — extras were blocked")
    else:
        p3.append(f"✅ Within daily budget (${cap:.2f})")

    print(f"[SLIP] Sending PART 3/3 ({len(p3)} lines)...")
    _send_telegram("\n".join(p3))
    if _p1_ok and _has_bets:
        print("[SLIP] All 3 parts sent — Part 1 confirmed OK with bets.")
    elif not _p1_ok:
        print("[SLIP] WARNING: Part 1 send failed (non-200 or exception) — slip will NOT be marked as sent.")
    else:
        print("[SLIP] All 3 parts sent — Part 1 was empty (no locks or flips).")
    return _p1_ok and _has_bets


def _send_slip_update(
    new_pick_ids: set,
    all_locks: list,
    all_flips: list,
    all_totals: list,
    all_nrfi: list,
    all_k_props: list,
    all_hitter_props: list,
    today: str,
    br: float,
) -> bool:
    """Send a brief Telegram update containing only newly-added picks plus a
    rebuilt parlay from ALL current locks. Returns True on successful send."""

    def _id_ml(a: dict, s: str) -> str:
        return f"ML:{a.get(f'{s}_name','')}:{a.get(f'best_{s}_odds','')}"

    def _id_total(b: dict) -> str:
        return f"TOT:{b['game']}:{b['direction']}:{b['line']}"

    def _id_nrfi(b: dict) -> str:
        return f"NRFI:{b['game']}:{b['direction']}"

    def _id_kprop(b: dict) -> str:
        return f"KPROP:{b['sp']}:{b['line']}"

    def _id_hitter(h: dict) -> str:
        return f"HITTER:{h['player']}:{h.get('team','')}:{h['prop']}"

    lines = [
        f"PARLAY OS — {today}",
        f"UPDATE — {len(new_pick_ids)} NEW PICK(S) ADDED",
        "",
    ]

    for a, s in all_locks + all_flips:
        if _id_ml(a, s) not in new_pick_ids:
            continue
        team   = a.get(f"{s}_name", "")
        odds   = a.get(f"best_{s}_odds", "")
        edge   = a.get(f"{s}_edge", 0)
        stake  = a.get(f"{s}_stake", 0)
        game   = f"{a.get('away_name','')} @ {a.get('home_name','')}"
        odds_s = (f"+{odds}" if isinstance(odds, int) and odds > 0 else str(odds or ""))
        label  = "🔒 LOCK" if (a, s) in all_locks else "🪙 FLIP"
        lines.append(f"{label}: {game} — {team} ML {odds_s} — ${stake:.2f} — EDGE: +{edge:.1f}%")

    for b in all_totals:
        if _id_total(b) not in new_pick_ids:
            continue
        lines.append(
            f"📊 TOTAL: {b['game']} — {b['direction']} {b['line']} "
            f"({b['prob']:.1%}) — ${b['stake']:.2f} — EDGE: +{b['edge_pct']:.1f}%"
        )

    for b in all_nrfi:
        if _id_nrfi(b) not in new_pick_ids:
            continue
        lines.append(f"🌅 NRFI: {b['game']} — {b['direction']} ({b['prob']:.1%}) — ${b['stake']:.2f}")

    for b in all_k_props:
        if b.get("edge_pct", 0) < 5.0 or _id_kprop(b) not in new_pick_ids:
            continue
        last = b["sp"].split()[-1]
        lines.append(
            f"⚾ PROP: {b['sp']} ({b.get('team','')}) — "
            f"{last} O{b['line']}K — ${b['stake']:.2f} — EDGE: +{b['edge_pct']:.1f}%"
        )

    for h in all_hitter_props:
        if h.get("edge_pct", 0) < 5.0 or _id_hitter(h) not in new_pick_ids:
            continue
        lines.append(
            f"🏏 PROP: {h['player']} ({h.get('team','')}) — "
            f"{h['prop']} — ${h['stake']:.2f} — EDGE: +{h['edge_pct']:.1f}%"
        )

    if len(lines) == 3:
        lines.append("(no new pick details matched — check scout logs)")

    # ── Rebuild parlay from all current picks so it always reflects the full day ──
    try:
        _prl_candidates: list = []
        _prl_upd_label = "♻️ UPDATED PARLAY (rebuilt from all current locks):"
        _prl_min_prob_upd = 0.35
        for _a, _s in all_locks:
            _odds_val = _a.get(f"best_{_s}_odds")
            if _odds_val is None:
                continue
            try:
                if int(str(_odds_val).replace("+", "")) < -180:
                    continue
            except (ValueError, TypeError):
                pass
            _prl_candidates.append((_a, _s))
            if len(_prl_candidates) == 3:
                break

        # Fall back to top picks by confidence when < 2 locks
        if len(_prl_candidates) < 2 and (all_locks + all_flips):
            _upd_fb_pool = sorted(
                all_locks + all_flips,
                key=lambda x: x[0].get(f"{x[1]}_confidence_score", 0),
                reverse=True,
            )
            _upd_fb_cands: list = []
            for _a, _s in _upd_fb_pool:
                _ov = _a.get(f"best_{_s}_odds")
                if _ov is None:
                    continue
                try:
                    if int(str(_ov).replace("+", "")) < -180:
                        continue
                except (ValueError, TypeError):
                    pass
                _upd_fb_cands.append((_a, _s))
                if len(_upd_fb_cands) == 3:
                    break
            if len(_upd_fb_cands) >= 2:
                _upd_combined_fb = 1.0
                for _a, _s in _upd_fb_cands:
                    _upd_combined_fb *= _a.get(f"{_s}_model_p", 0.5)
                if _upd_combined_fb >= 0.28:
                    _prl_candidates = _upd_fb_cands
                    _prl_upd_label = "♻️ BEST AVAILABLE PARLAY (top picks by confidence):"
                    _prl_min_prob_upd = 0.28

        if len(_prl_candidates) >= 2:
            _odds_strs = [str(_a.get(f"best_{_s}_odds", "")) for _a, _s in _prl_candidates]
            _prl = parlay_odds(_odds_strs)
            if _prl.get("valid"):
                _combined_model_p = 1.0
                for _a, _s in _prl_candidates:
                    _combined_model_p *= _a.get(f"{_s}_model_p", 0.5)
                if _combined_model_p >= _prl_min_prob_upd:
                    _day_cls = day_classification(len(all_locks), len(all_flips))
                    _budget  = daily_budget(br)
                    _s_mult  = _day_cls.get("stake_mult", 1.0)
                    _prl_stake = round(min(_budget * 0.15, br * 0.015) * _s_mult, 2)
                    _prl_win   = round((_prl["decimal"] - 1) * _prl_stake, 2)
                    _leg_parts = []
                    for _a, _s in _prl_candidates:
                        _t = _a.get(f"{_s}_name", "")
                        _o = _a.get(f"best_{_s}_odds", "")
                        _o_str = (f"+{_o}" if isinstance(_o, int) and _o > 0 else str(_o or ""))
                        _leg_parts.append(f"{_t} ML {_o_str}")
                    lines.append("")
                    lines.append(_prl_upd_label)
                    lines.append(f"  {' + '.join(_leg_parts)}")
                    lines.append(f"  ({_prl['american']}) — ${_prl_stake:.2f} — to win ${_prl_win:.2f}")
    except Exception as _prl_err:
        print(f"[SLIP UPDATE] Parlay rebuild error: {_prl_err}")

    print(f"[SLIP UPDATE] Sending {len(new_pick_ids)} new picks...")
    _ok = _send_telegram("\n".join(lines))
    if _ok:
        print("[SLIP UPDATE] Sent successfully.")
    else:
        print("[SLIP UPDATE] Send failed — will allow retry on next run.")
    return _ok


# ── BET RECOMMENDATION FILTER ─────────────────────────────────────────────────

def _should_recommend(game: dict, side: str, bet_type: str = "ML") -> bool:
    """Brain's final sign-off: is this bet worth sending?"""
    edge  = game.get(f"{side}_edge", 0)
    conv  = game.get(f"{side}_conv", "PASS")
    stake = game.get(f"{side}_stake", 0)
    model = game.get(f"{side}_model_p", 0)
    nv    = game.get(f"{side}_nv", 0)
    team  = game.get(f"{side}_name", side)
    odds  = game.get(f"best_{side}_odds")

    if conv == "PASS" or edge < MIN_EDGE_PCT:
        print(f"  PASS {team}: edge {edge:+.1f}% (need >{MIN_EDGE_PCT}%) model={model:.3f} nv={nv:.3f}")
        return False

    # No -200 or worse favorites — too much juice, destroys EV at scale
    if odds is not None:
        try:
            odds_int = int(str(odds).replace("+", ""))
            if odds_int < -200:
                print(f"  PASS {team}: odds {odds} worse than -200 — too juiced")
                return False
        except (ValueError, TypeError):
            pass

    # No 10+ point line moves (magnitude > 0.10 in probability = ~10pp swing)
    lm = game.get("line_movement") or {}
    if lm.get("magnitude", 0) > 0.10 and lm.get("direction", "") not in ("unknown", "stable", ""):
        print(f"  PASS {team}: large line move {lm.get('direction')} ({lm.get('magnitude', 0):.3f}) — steam fade risk")
        return False

    if stake <= 0:
        from bankroll_engine import current_bankroll as _cur_br, daily_exposure as _daily_exp, peak_bankroll as _peak_br
        _br   = _cur_br()
        _peak = _peak_br()
        _exp  = _daily_exp()
        _dd   = round((_peak - _br) / _peak * 100, 1) if _peak > 0 else 0.0
        print(
            f"  PASS {team}: stake=$0.00 — Kelly zero "
            f"[edge={edge:+.1f}% model={model:.3f} nv={nv:.3f} "
            f"br=${_br:.2f} peak=${_peak:.2f} dd={_dd:.1f}%]"
        )
        return False
    if model < MIN_PROB:
        print(f"  PASS {team}: model {model:.3f} < min {MIN_PROB}")
        return False

    dd = drawdown_tier()
    if dd["pause"]:
        print(f"  PASS {team}: drawdown pause active ({dd['pct']:.1f}% drawdown)")
        return False
    # At -15% drawdown, only props are allowed — block ML bets
    if dd["props_only"] and bet_type.upper() == "ML":
        print(f"  PASS {team}: drawdown {dd['pct']:.1f}% — props only mode (ML blocked)")
        return False

    confidence = game.get(f"{side}_confidence_score", 0)

    # Sharp money boost: reverse line movement (sharp bettors driving line against public)
    sharp_side = game.get("market_sharp_signal", "")
    if sharp_side and sharp_side == side:
        confidence = min(confidence + 10, 100)
        print(f"  [SHARP] {team}: sharp reverse line signal, confidence +10 → {confidence}")

    # Line moved against our direction (8+ cents) — smart money fading us
    lm_dir = game.get("market_line_direction", "")
    lm_mag = game.get("market_line_magnitude", 0.0)
    if lm_mag >= 0.08:
        if (side == "home" and "toward_away" in lm_dir) or (side == "away" and "toward_home" in lm_dir):
            confidence = max(confidence - 15, 0)
            print(f"  [LME] {team}: line moved against our direction ({lm_mag:.3f}), confidence -15 → {confidence}")

    # ── Sharp bettor 5-check checklist ────────────────────────────────────────
    # Each check PASS/FAIL is logged; situation_stack boosts conviction.
    _checklist = {}

    # Check 1: Line value — our model price beats market by minimum threshold
    _checklist["line_value"] = "PASS" if edge >= MIN_EDGE_PCT else "FAIL"

    # Check 2: Market efficiency — no large steam against us
    _lm_against = lm_mag >= 0.08 and (
        (side == "home" and "toward_away" in lm_dir)
        or (side == "away" and "toward_home" in lm_dir)
    )
    _checklist["market_efficiency"] = "FAIL" if _lm_against else "PASS"

    # Check 3: CLV projection — expected closing line value positive
    # If our model edge ≥ 3% at current line, we expect positive CLV at close
    _checklist["clv_projection"] = "PASS" if edge >= 3.0 else "FAIL"

    # Check 4: Kelly sanity — stake is within reasonable fraction of bankroll
    try:
        from bankroll_engine import current_bankroll as _ckb
        _ckb_val = _ckb()
        _kelly_frac = (stake / _ckb_val) if _ckb_val > 0 else 0.0
        _checklist["kelly_sanity"] = "PASS" if 0.001 <= _kelly_frac <= 0.06 else "WARN"
    except Exception:
        _checklist["kelly_sanity"] = "PASS"

    # Check 5: Situation stack — 3+ situations triggered = HIGH conviction bonus
    _sit_res = game.get("situations") or {}
    _n_sit   = _sit_res.get("n_triggered", 0)
    if _n_sit >= 3:
        _checklist["situation_stack"] = "STACK"
        confidence = min(confidence + 8, 100)
        print(f"  [SITUATIONS] {team}: stack ({_n_sit} angles), confidence +8 → {confidence}")
    elif _n_sit >= 1:
        _checklist["situation_stack"] = f"ACTIVE_{_n_sit}"
    else:
        _checklist["situation_stack"] = "NONE"

    _checks_passed = sum(1 for v in _checklist.values() if v == "PASS")
    _checklist_str = " | ".join(f"{k}={v}" for k, v in _checklist.items())
    print(f"  [CHECKLIST] {team}: {_checks_passed}/5 | {_checklist_str}")

    min_conf = 60 if conv == "HIGH" else 55
    # Strong edge overrides the confidence floor — if model edge ≥ 7% the line value
    # is the primary signal and shouldn't be blocked by market noise penalties.
    if edge >= 7.0:
        min_conf = max(min_conf - 10, 45)
    if confidence < min_conf:
        print(f"  PASS {team}: confidence {confidence}/100 < {min_conf} threshold ({conv}, edge={edge:+.1f}%)")
        return False

    # ── Veto rules ────────────────────────────────────────────────────────────
    # Veto 1: Both SPs xFIP > 5.0 — high variance coin flip, skip
    _a_sp = game.get("away_sp") or {}
    _h_sp = game.get("home_sp") or {}
    if _a_sp.get("xfip", 4.35) > 5.0 and _h_sp.get("xfip", 4.35) > 5.0:
        print(f"  PASS {team}: both SPs xFIP > 5.0 ({_a_sp.get('xfip',4.35):.2f}/{_h_sp.get('xfip',4.35):.2f}) — high variance skip")
        return False

    # Veto 2: SP threw 100+ pitches in last 4 days — fatigue/injury risk
    _sp_data = game.get(f"{side}_sp") or {}
    if _sp_data.get("high_pitch_recent", False):
        print(f"  PASS {team}: {_sp_data.get('name','SP')} threw 100+ pitches within last 4 days")
        return False

    # Veto 3: Team on 6+ game losing streak (7-day record 0 wins, 6+ losses)
    _off_data  = game.get(f"{side}_off") or {}
    _record_7d = _off_data.get("record_7d", {})
    if _record_7d.get("losses", 0) >= 6 and _record_7d.get("wins", 0) == 0:
        print(f"  PASS {team}: 6+ game losing streak (record_7d: {_record_7d})")
        return False

    print(f"  BET  {team}: edge {edge:+.1f}% model={model:.3f} nv={nv:.3f} stake=${stake:.2f} [{conv}] conf={confidence}/100")
    return True


# ── TELEGRAM FORMAT ───────────────────────────────────────────────────────────

def _format_bet_message(game: dict, side: str) -> str:
    """
    8-element Telegram pick format:
    EDGE / SP MATCHUP / BULLPEN / KEY STAT / RISK / CONFIDENCE / SITUATIONS / FLAGS
    """
    team    = game.get(f"{side}_name", game.get(side, ""))
    opp_s   = "home" if side == "away" else "away"
    odds    = game.get(f"best_{side}_odds")
    book    = game.get(f"best_{side}_book", "")
    edge    = game.get(f"{side}_edge", 0)
    model_p = game.get(f"{side}_model_p", 0)
    nv      = game.get(f"{side}_nv", 0)
    stake   = game.get(f"{side}_stake", 0)
    conv    = game.get(f"{side}_conv", "")
    sp      = game.get(f"{side}_sp") or {}
    opp_sp  = game.get(f"{opp_s}_sp") or {}
    our_bp  = game.get(f"{side}_bp") or {}
    opp_bp  = game.get(f"{opp_s}_bp") or {}
    wx      = game.get("weather") or {}
    conf    = game.get(f"{side}_confidence_score", 0)
    our_code = game.get(side, "")
    opp_code = game.get(opp_s, "")

    odds_s = (f"+{odds}" if isinstance(odds, int) and odds > 0 else str(odds or ""))
    sides_str = f"{game.get('away_name','')} @ {game.get('home_name','')}"

    conv_icon = "🔒" if conv == "HIGH" else "🪙"
    lines = [
        f"{conv_icon} {conv} | <b>{team} ML {odds_s}</b> @ {book.upper() if book else '?'}",
        f"{sides_str}",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    # ── EDGE ─────────────────────────────────────────────────────────────────
    lines.append(f"EDGE       {edge:+.1f}% | Model {model_p:.1%} vs Market {nv:.1%} | Stake ${stake:.2f}")

    # ── SP MATCHUP ────────────────────────────────────────────────────────────
    def _sp_summary(s: dict, tier_side: str) -> str:
        name  = s.get("name", "TBD")
        era   = s.get("era", "?")
        xfip  = s.get("xfip")
        tier  = game.get(f"{tier_side}_sp_tier", "")
        xfip_s = f"xFIP {xfip:.2f} [{tier}]" if xfip is not None else ""
        era_s  = f"ERA {era}" if era != "?" else "ERA ?"
        return f"{name} ({era_s}{' / ' + xfip_s if xfip_s else ''})"

    our_sp_s = _sp_summary(sp, side)
    opp_sp_s = _sp_summary(opp_sp, opp_s)
    lines.append(f"SP MATCHUP Our: {our_sp_s}")
    lines.append(f"           Opp: {opp_sp_s}")

    # ── BULLPEN ───────────────────────────────────────────────────────────────
    our_bp_tier = our_bp.get("fatigue_tier", "?")
    our_bp_fat  = our_bp.get("avg_fatigue", 0)
    opp_bp_tier = opp_bp.get("fatigue_tier", "?")
    opp_bp_fat  = opp_bp.get("avg_fatigue", 0)
    bp_adv = ""
    if our_bp_fat < opp_bp_fat - 0.5:
        bp_adv = f" ← {our_code} advantage"
    elif opp_bp_fat < our_bp_fat - 0.5:
        bp_adv = f" ← {opp_code} advantage (risk)"
    lines.append(f"BULLPEN    {our_code}: {our_bp_tier} ({our_bp_fat:.1f}) vs {opp_code}: {opp_bp_tier} ({opp_bp_fat:.1f}){bp_adv}")

    # ── KEY STAT ──────────────────────────────────────────────────────────────
    key_parts = []
    # xFIP gap (SP regression signal)
    our_xfip = sp.get("xfip")
    opp_xfip = opp_sp.get("xfip")
    if our_xfip is not None and opp_xfip is not None:
        xfip_gap = round(opp_xfip - our_xfip, 2)
        if abs(xfip_gap) >= 0.4:
            key_parts.append(f"xFIP gap {xfip_gap:+.2f} {'(our SP dominates)' if xfip_gap > 0 else '(opp SP dominates)'}")
    # Home dog structural edge
    hd = game.get("home_dog") or {}
    if hd.get("is_home_dog_value"):
        key_parts.append(f"Home dog structural +4% ({odds_s})")
    # Platoon advantage
    plat_edge = game.get(f"{side}_platoon_edge", 0)
    if game.get(f"{side}_strong_platoon_adv"):
        key_parts.append(f"Platoon edge {plat_edge:+.0f} wRC+")
    # NRFI/YRFI lean
    nrfi = game.get("nrfi") or {}
    nrfi_note = nrfi.get("note", "")
    if nrfi_note in ("nrfi", "yrfi"):
        p = nrfi.get("p_nrfi", 0) if nrfi_note == "nrfi" else nrfi.get("p_yrfi", 0)
        key_parts.append(f"{nrfi_note.upper()} {p:.0%}")
    # Momentum
    our_mom = game.get(f"{side}_momentum") or {}
    mom_score = our_mom.get("score", 0)
    if abs(mom_score) >= 0.03:
        key_parts.append(f"Momentum {'hot' if mom_score > 0 else 'cold'} ({mom_score:+.3f})")
    # Pitch trap: our offense exploits opponent SP's pitch arsenal
    _our_pt = game.get(f"{side}_pitch_trap") or {}
    if _our_pt.get("is_pitch_trap"):
        _pt_types = ", ".join(_our_pt.get("exploitable_pitches", []))
        key_parts.append(f"PITCH TRAP {_pt_types} +{_our_pt.get('prob_add', 0):.1%}")
    # Elite catcher framing (our catcher helps our pitchers)
    _our_fr = game.get(f"{side}_framing") or {}
    if _our_fr.get("is_elite"):
        key_parts.append(f"FRAMING EDGE +{_our_fr.get('framing_runs', 0):.0f} runs")
    # Opponent key reliever unavailable → bullpen edge for us
    opp_key_rel = game.get(f"{opp_s}_key_rel_avail", True)
    if not opp_key_rel:
        key_parts.append(f"Opp closer/RP unavailable today")
    if not key_parts:
        key_parts.append("Model consensus play")
    lines.append(f"KEY STAT   {' | '.join(key_parts)}")

    # ── RISK ──────────────────────────────────────────────────────────────────
    risk_parts = []
    if sp.get("high_pitch_recent"):
        risk_parts.append(f"⚠ {sp.get('name','SP')} high pitch count last 4 days")
    if sp.get("worsening_walk"):
        risk_parts.append("⚠ Walk rate rising")
    if sp.get("velocity_decline"):
        risk_parts.append("⚠ Velo declining")
    if game.get(f"{side}_sp_tbd") or sp.get("name") in ("TBD", "", None):
        risk_parts.append("⚠ SP TBD")
    if not game.get(f"{side}_lineup_confirmed"):
        risk_parts.append("⚠ Lineup unconfirmed")
    wx_adj = wx.get("run_adjustment", 0)
    if abs(wx_adj) >= 0.5:
        risk_parts.append(f"Wx: {wx.get('wind_label','')}{' (runs up)' if wx_adj > 0 else ' (runs down)'}")
    # SP ERA-xFIP regression risk for OPP SP (means they might get better — risk for our pick)
    if opp_xfip is not None and opp_sp.get("era") is not None:
        opp_era = float(opp_sp.get("era", 4.35))
        if opp_era - opp_xfip >= 1.5:
            risk_parts.append(f"Opp SP ERA/xFIP gap {opp_era - opp_xfip:+.1f} (may improve)")
    injury_flags = game.get("injury_flags") or []
    for inj in injury_flags[:2]:
        risk_parts.append(f"{inj.get('emoji','🚑')} {inj.get('message','')}")
    # Poor catcher framing (our catcher hurts our pitchers)
    _our_fr_risk = game.get(f"{side}_framing") or {}
    if _our_fr_risk.get("is_poor"):
        risk_parts.append(f"⚠ Poor framing {_our_fr_risk.get('framing_runs', 0):.0f} runs")
    # Our key reliever unavailable
    if not game.get(f"{side}_key_rel_avail", True):
        risk_parts.append("⚠ Our closer/key RP unavailable today")
    if not risk_parts:
        risk_parts.append("No major risk flags")
    lines.append(f"RISK       {' | '.join(risk_parts)}")

    # ── CONFIDENCE ────────────────────────────────────────────────────────────
    min_conf = 60 if conv == "HIGH" else 55
    lines.append(f"CONFIDENCE {conf}/100 (threshold {min_conf}) | Ump: {game.get('umpire','?')}{' — ' + game.get('ump_note','') if game.get('ump_note') else ''}")

    # ── SITUATIONS ────────────────────────────────────────────────────────────
    _sit_res = game.get("situations") or {}
    _sit_line = situations_telegram_line(_sit_res)
    if _sit_line:
        lines.append(f"SITUATIONS {_sit_line.lstrip('SITUATIONS ')}" if _sit_line.startswith("SITUATIONS") else f"SITUATIONS {_sit_line}")

    # ── FLAGS ─────────────────────────────────────────────────────────────────
    flag_parts = []
    # Pitch trap
    if _our_pt.get("is_pitch_trap"):
        _pt_types = ", ".join(_our_pt.get("exploitable_pitches", []))
        flag_parts.append(f"PITCH_TRAP {_pt_types}")
    # Home dog structural angle
    hd_result = game.get("home_dog") or {}
    if hd_result.get("is_home_dog_value"):
        flag_parts.append(f"HOME_DOG +4%")
    # Sharp money signal
    _sharp_side = game.get("market_sharp_signal", "")
    if _sharp_side and _sharp_side == side:
        flag_parts.append("SHARP_MONEY ↑")
    elif _sharp_side and _sharp_side != side:
        flag_parts.append(f"SHARP_MONEY vs us (fading {side})")
    # Umpire edge
    _ump_e = game.get("ump_edge") or {}
    _ump_tag = _ump_e.get("tag", "")
    if _ump_tag:
        flag_parts.append(f"UMP_EDGE: {_ump_tag}")
    # ABS score for our SP
    _abs = sp.get("abs_score")
    if _abs is not None:
        if _abs > 65:
            flag_parts.append(f"ABS_EDGE {_abs:.0f}/100")
        elif _abs < 35:
            flag_parts.append(f"ABS_FADE {_abs:.0f}/100")
    _xwoba_tier = sp.get("xwoba_tier", "")
    if _xwoba_tier in ("ELITE", "GREAT"):
        _xwoba_val = sp.get("xwoba_against")
        _xv_str = f" {_xwoba_val:.3f}" if _xwoba_val is not None else ""
        flag_parts.append(f"xwOBA_{_xwoba_tier}{_xv_str}")
    _roll_tier = sp.get("rolling_xwoba_tier", "")
    if _roll_tier in ("PEAKING", "DECLINING"):
        flag_parts.append(f"FORM_{_roll_tier}")
    _tempo_lbl = sp.get("tempo_label", "")
    if _tempo_lbl in ("QUICK_WORKER", "SLOW_WORKER"):
        flag_parts.append(f"TEMPO_{_tempo_lbl}")
    if flag_parts:
        lines.append(f"FLAGS      {' | '.join(flag_parts)}")

    return "\n".join(lines)


def _format_pass_message(game: dict) -> str:
    a = game.get("away_name", game.get("away", ""))
    h = game.get("home_name", game.get("home", ""))
    ae = game.get("away_edge", 0)
    he = game.get("home_edge", 0)
    return (
        f"PASS | {a} @ {h}\n"
        f"Edge: {a} {ae:+.1f}% / {h} {he:+.1f}%\n"
        f"No value found — model aligns with market."
    )


def _send_telegram(msg: str) -> bool:
    """Send a Telegram message. Returns True only if the API returned HTTP 200."""
    if DRY_RUN:
        print("[TG] DRY_RUN — printing instead of sending:")
        print(msg)
        print("---")
        return True
    if not BOT_TOKEN or not CHAT_ID:
        print(f"[TG] WARN: BOT_TOKEN={'set' if BOT_TOKEN else 'MISSING'} CHAT_ID={'set' if CHAT_ID else 'MISSING'} — printing instead:")
        print(msg)
        return True
    print(f"[TG] Sending message to chat {CHAT_ID} (len={len(msg)})...")
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=8,
        )
        print(f"[TG] Sent — status={resp.status_code} ok={resp.ok}")
        if not resp.ok:
            print(f"[TG] Response body: {resp.text[:300]}")
        return resp.status_code == 200
    except Exception as e:
        print(f"[TG] ERROR sending: {e}")
        return False


def _generate_pick_narrative(analysis: dict, side: str) -> str:
    """One-line narrative bullet for a pick — why the model likes it."""
    sp_key   = f"{side}_sp"
    sp       = (analysis.get(sp_key) or {})
    sp_name  = sp.get("name") or "SP"
    k9       = sp.get("k9")
    xfip     = sp.get("xfip")
    edge     = analysis.get(f"{side}_edge_pct")
    momentum = (analysis.get("momentum") or {}).get(f"{side}_score")

    parts = []
    if k9 and k9 >= 9.0:
        parts.append(f"{sp_name} K9={k9:.1f}")
    if xfip and xfip <= 3.8:
        parts.append(f"elite xFIP {xfip:.2f}")
    if edge and edge >= 8:
        parts.append(f"{edge:.1f}% model edge")
    if momentum and momentum >= 2:
        parts.append("hot streak")
    return " | ".join(parts) if parts else "Model consensus play"


def _post_public_channel(locks: list, flips: list, today: str) -> None:
    """Post clean pick summary to public Telegram channel (no bankroll info)."""
    if not PUBLIC_CHANNEL_ID or not BOT_TOKEN or DRY_RUN:
        return
    picks = locks + flips
    if not picks:
        return

    lines = [
        f"<b>PARLAY OS — {today}</b>",
        f"<i>MLB Model Picks • {len(picks)} play{'s' if len(picks) != 1 else ''}</i>",
        "",
    ]
    for analysis, side in picks:
        team    = analysis.get(f"{side}_team", side.upper())
        odds    = analysis.get(f"best_{side}_odds", "")
        conv    = analysis.get("conviction", "")
        game    = analysis.get("game_label", analysis.get("away_team", "") + " vs " + analysis.get("home_team", ""))
        edge    = analysis.get(f"{side}_edge_pct")
        narr    = _generate_pick_narrative(analysis, side)
        tag     = "🔒" if conv == "HIGH" else "⚡"
        edge_str = f" | edge +{edge:.1f}%" if edge else ""
        lines.append(f"{tag} <b>{team}</b> {odds}")
        lines.append(f"   {game}{edge_str}")
        lines.append(f"   <i>{narr}</i>")
        lines.append("")

    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("⚠️ For educational purposes only. Bet responsibly.")
    lines.append("📊 Full verified record: /record")

    msg = "\n".join(lines)
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": PUBLIC_CHANNEL_ID, "text": msg, "parse_mode": "HTML"},
            timeout=8,
        )
    except Exception as e:
        print(f"[TG] Public channel post error: {e}")


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _resolve_game_pk(team_name: str, game_date: str) -> int | None:
    try:
        r = _http_get(
            f"{STATSAPI}/schedule?sportId=1&date={game_date}&hydrate=game",
            timeout=8
        )
        for gd in r.json().get("dates", []):
            for g in gd.get("games", []):
                at = g.get("teams", {}).get("away", {}).get("team", {}).get("name", "")
                ht = g.get("teams", {}).get("home", {}).get("team", {}).get("name", "")
                if team_name in at or at in team_name or team_name in ht or ht in team_name:
                    return g.get("gamePk")
    except Exception:
        pass
    return None


def _get_umpire(game_pk: int | None) -> str:
    """Pull HP umpire name. Tries schedule/officials first (pre-game), then boxscore."""
    if not game_pk:
        return ""
    # Pre-game: schedule hydrate=officials is populated day-of before first pitch
    try:
        r = _http_get(
            f"{STATSAPI}/schedule",
            params={"gamePk": game_pk, "hydrate": "officials", "sportId": 1},
            timeout=8,
        )
        for day in r.json().get("dates", []):
            for g in day.get("games", []):
                if g.get("gamePk") != game_pk:
                    continue
                for official in g.get("officials", []):
                    if official.get("officialType") == "Home Plate":
                        name = official.get("official", {}).get("fullName", "")
                        if name:
                            return name
    except Exception:
        pass
    # In-game / post-game fallback: boxscore
    try:
        r   = _http_get(f"{STATSAPI}/game/{game_pk}/boxscore", timeout=8)
        for u in r.json().get("officials", []):
            if u.get("officialType") == "Home Plate":
                name = u.get("official", {}).get("fullName", "")
                if name:
                    return name
    except Exception:
        pass
    return ""


# ── PLAYER PROP ENGINE ────────────────────────────────────────────────────────

# (stat_key, display_label, over_line, market_baseline_prob)
# Market baselines represent the implied probability at a representative no-vig line.
_HITTER_PROP_LINES = [
    ("hits",       "Hits O1.5",  1.5, 0.47),
    ("homeRuns",   "HR O0.5",    0.5, 0.17),
    ("totalBases", "TB O1.5",    1.5, 0.48),
    ("rbi",        "RBI O0.5",   0.5, 0.32),
    ("strikeOuts", "SO O0.5",    0.5, 0.55),
]
_HITTER_PROP_MIN_EDGE  = 0.05   # min 5 pp above market baseline
_HITTER_PROP_MIN_GAMES = 3      # min games in 14-day window
_HITTER_PROP_TOP_N     = 6      # evaluate top N batters in batting order
_LG_K9_VS_BATTERS      = 8.7   # league-avg SP K/9 used for batter SO adjustment
_LG_ERA_HITTER_BASE    = 4.35  # league-avg ERA used for hit/TB adjustment


def _normalize_lineup_players(players: list) -> list:
    """
    Normalize a player list from the MLB lineup API.
    Handles two structures:
      • Pre-game hydration: {"person": {"id": ..., "fullName": ...}, ...}
      • Boxscore battingOrder: already flattened to {"id": ..., "fullName": ...}
    """
    result = []
    for p in players:
        if "person" in p:
            pid  = p["person"].get("id")
            name = p["person"].get("fullName", "")
        else:
            pid  = p.get("id")
            name = p.get("fullName", "")
        if pid:
            result.append({"id": pid, "fullName": name})
    return result


def _fetch_lineup(game_pk: int) -> dict:
    """Pull confirmed batting lineup via schedule?hydrate=lineups, with boxscore fallback."""
    try:
        r = _http_get(
            f"{STATSAPI}/schedule",
            params={"gamePk": game_pk, "hydrate": "lineups", "sportId": 1},
            timeout=8,
        )
        for day in r.json().get("dates", []):
            for g in day.get("games", []):
                if g.get("gamePk") != game_pk:
                    continue
                lineups = g.get("lineups") or {}
                # MLB Stats API uses awayPlayers/homePlayers pre-game, awayTeam/homeTeam in-game
                away_raw = lineups.get("awayPlayers") or lineups.get("awayTeam") or []
                home_raw = lineups.get("homePlayers") or lineups.get("homeTeam") or []
                away = _normalize_lineup_players(away_raw)
                home = _normalize_lineup_players(home_raw)
                if len(away) >= 5 or len(home) >= 5:
                    return {"away": away, "home": home, "confirmed": True}
    except Exception:
        pass
    # Fallback: boxscore batting order (~45 min before first pitch)
    try:
        r2 = _http_get(f"{STATSAPI}/game/{game_pk}/boxscore", timeout=8)
        box   = r2.json()
        teams = box.get("teams", {})
        away_order = [
            {
                "id":       pid,
                "fullName": teams.get("away", {}).get("players", {}).get(f"ID{pid}", {}).get("person", {}).get("fullName", ""),
            }
            for pid in teams.get("away", {}).get("battingOrder", [])
        ]
        home_order = [
            {
                "id":       pid,
                "fullName": teams.get("home", {}).get("players", {}).get(f"ID{pid}", {}).get("person", {}).get("fullName", ""),
            }
            for pid in teams.get("home", {}).get("battingOrder", [])
        ]
        if len(away_order) >= 5 or len(home_order) >= 5:
            return {"away": away_order, "home": home_order, "confirmed": True}
    except Exception:
        pass
    return {"away": [], "home": [], "confirmed": False}


def _player_14d_stats(player_id: int) -> dict | None:
    """
    Aggregate a batter's last-14-day game log from the MLB Stats API.
    Returns aggregated stat dict or None if fewer than _HITTER_PROP_MIN_GAMES games.
    """
    from datetime import date as _date, timedelta as _td
    end   = _date.today().isoformat()
    start = (_date.today() - _td(days=14)).isoformat()
    try:
        r = _http_get(
            f"{STATSAPI}/people/{player_id}/stats",
            params={
                "stats":     "gameLog",
                "group":     "hitting",
                "season":    "2026",
                "gameType":  "R",
                "startDate": start,
                "endDate":   end,
            },
            timeout=10,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        agg = {k: 0 for k in ("hits", "homeRuns", "totalBases", "rbi", "strikeOuts", "atBats")}
        n   = 0
        for s in splits:
            stat = s.get("stat", {})
            if int(stat.get("atBats", 0) or 0) == 0:
                continue   # skip 0-PA appearances (pinch runner etc.)
            for k in agg:
                agg[k] += int(stat.get(k, 0) or 0)
            n += 1
        if n < _HITTER_PROP_MIN_GAMES:
            return None
        agg["games"] = n
        return agg
    except Exception:
        return None


def _scan_hitter_props(
    player_name: str,
    team: str,
    stats: dict,
    opp_sp: dict,
    br: float,
) -> list:
    """
    Evaluate prop markets for one batter using Poisson model vs. market baseline.
    Adjusts λ by opponent SP quality when real SP data is available.
    Returns list of recommendation dicts, only for props with >= _HITTER_PROP_MIN_EDGE.
    """
    n     = stats["games"]
    recs  = []

    for stat_key, label, line, market_p in _HITTER_PROP_LINES:
        raw = stats.get(stat_key, 0)
        lam  = raw / n
        if lam <= 0:
            continue

        # SP quality adjustment (skip if SP data is a league-average fallback)
        if not opp_sp.get("sp_missing"):
            if stat_key == "strikeOuts":
                sp_k9 = opp_sp.get("k9", _LG_K9_VS_BATTERS)
                lam   = lam * (sp_k9 / _LG_K9_VS_BATTERS)
            elif stat_key in ("hits", "totalBases"):
                sp_era = opp_sp.get("effective_era", _LG_ERA_HITTER_BASE)
                lam    = lam * (sp_era / _LG_ERA_HITTER_BASE)

        lam      = max(lam, 0.001)
        model_p  = min(prob_over(lam, line), 0.87)   # Poisson tails blow up; 87% is realistic ceiling
        edge     = round(model_p - market_p, 4)

        if edge < _HITTER_PROP_MIN_EDGE:
            continue

        # Kelly sizing — same formula as ML bets, derived from market implied odds
        _mp = max(min(market_p, 0.99), 0.01)
        if _mp >= 0.5:
            _odds_str = f"-{round(_mp / (1.0 - _mp) * 100)}"
        else:
            _odds_str = f"+{round((1.0 - _mp) / _mp * 100)}"
        stake = kelly_stake(model_p, _odds_str, "MEDIUM")
        stake = min(stake, round(br * 0.015, 2))   # hard cap: 1.5% of bankroll per prop
        recs.append({
            "player":     player_name,
            "team":       team,
            "prop":       label,
            "line":       line,
            "lam":        round(lam, 3),
            "model_prob": model_p,
            "market_p":   market_p,
            "edge_pct":   round(edge * 100, 1),
            "stake":      stake,
            "n_games":    n,
        })

    return recs


def _fetch_game_hitter_props(
    game_pk: int | None,
    away_code: str,
    home_code: str,
    away_sp: dict,
    home_sp: dict,
    br: float,
) -> list:
    """
    Fetch confirmed lineup + 14-day stats for each starter.
    Returns [] when game_pk is None or lineup is not yet confirmed.
    """
    if not game_pk:
        return []

    lineup = _fetch_lineup(game_pk)
    if len(lineup["away"]) < 5 and len(lineup["home"]) < 5:
        print(f"  [PROPS] {away_code}@{home_code}: <5 batters confirmed in either lineup — skipping hitter props")
        return []

    all_props: list = []
    for side, players, opp_sp in (
        ("away", lineup["away"], home_sp),
        ("home", lineup["home"], away_sp),
    ):
        team = away_code if side == "away" else home_code
        for player in players[:_HITTER_PROP_TOP_N]:
            pid  = player.get("id")
            name = player.get("fullName", f"Player#{pid}")
            if not pid:
                continue
            try:
                stats = _player_14d_stats(pid)
            except Exception:
                continue
            if not stats:
                continue
            recs = _scan_hitter_props(name, team, stats, opp_sp, br)
            all_props.extend(recs)

    return all_props


# ── DAILY SCOUT ───────────────────────────────────────────────────────────────

_WINDOW_LABELS = {
    "day":     "DAY (before 3pm ET)",
    "evening": "EVENING (3–8pm ET)",
    "west":    "WEST COAST (8pm+ ET)",
    "all":     "ALL GAMES",
}


def run_daily_scout(window: str = "all"):
    """Full daily analysis: all games → recommendations → Telegram.

    window: 'day' (before 3pm ET), 'evening' (3–8pm ET),
            'west' (8pm+ ET), or 'all' (no filter, default for manual runs).
    Each run only analyzes games whose ET start time falls within the window.
    Lineups are always re-fetched fresh — no game is skipped because it was
    seen in an earlier window run.
    """
    _window_label = _WINDOW_LABELS.get(window, "ALL GAMES")
    print("=" * 60)
    print(f"Brain starting — daily scout [{_window_label}]")
    init_memory_tables()
    init_brain_tables()

    try:
        from umpire_engine import ensure_umpire_db_populated
        ensure_umpire_db_populated()
    except Exception as _ump_e:
        print(f"[UMP] startup populate failed: {_ump_e}")

    today     = date.today().isoformat()
    print(f"Date: {today} — fetching events from Odds API...")
    events    = get_mlb_events()
    print(f"Games from Odds API: {len(events)}")
    if not events:
        print("WARNING: 0 games returned — check ODDS_API_KEY env var and API quota")

    # ── Start line movement polling thread (background, 30-min intervals) ─────
    if events and _LINE_POLLING_AVAILABLE:
        try:
            start_line_polling(events, _send_telegram, today)
        except Exception as _lp_e:
            print(f"[LME] Line polling start failed: {_lp_e}")

    br        = current_bankroll()
    mem       = memory_report()
    print(f"Bankroll: ${br:.2f} | Memory cal ready: {mem['ready_to_recalibrate']}")
    _ml_bgt  = pool_budget("ML", br)
    _ml_used = pool_exposure("ML")
    _ml_rem  = max(0.0, round(_ml_bgt - _ml_used, 2))
    print(f"[POOL] ML pool: ${_ml_rem:.2f} remaining (used ${_ml_used:.2f} of ${_ml_bgt:.2f} today)")
    if _ml_used > _ml_bgt:
        _send_telegram(
            f"⚠️ [POOL] ML pool over budget — used ${_ml_used:.2f} of ${_ml_bgt:.2f} today. "
            f"No new ML bets will be placed until reset."
        )

    all_bets  = []
    all_pass  = []
    # Daily slip collections
    all_locks:    list = []   # (analysis, side) — HIGH conviction, edge ≥ 7%
    all_flips:    list = []   # (analysis, side) — MEDIUM conviction, edge 4-7%
    all_fades:    list = []   # (analysis, side, reason)
    all_sgp:      list = []   # correlated SGP suggestions across all games
    all_nrfi:         list = []   # {game, direction, prob, stake}
    all_totals:       list = []   # {game, direction, line, prob, stake}
    all_hitter_props: list = []   # {player, team, prop, stake, edge_pct, ...}
    all_k_props:      list = []   # {sp, game, line, p_over, edge_pct, stake}
    all_injuries:     list = []   # injury warning strings — collected, sent in slip
    game_key_map:     dict = {}   # (away_code, home_code) → analysis
    props_games:      list = []   # per-game props data for props_output.json
    _faded_games:     set  = set()  # game keys already represented in fades

    # Seed accumulated_risk from today's already-pending bets (stale or earlier scout)
    _prior_pending = [b for b in _db.get_bets()
                      if not b.get("result") and b.get("date") == today]
    accumulated_risk = round(sum(float(b.get("stake") or 0) for b in _prior_pending), 2)
    if accumulated_risk > 0:
        print(f"Prior risk today: ${accumulated_risk:.2f} ({len(_prior_pending)} pending bets from earlier scout)")

    _cap = daily_budget(br)
    _cap_pct = round(_cap / br * 100, 0) if br > 0 else 15
    _override_cap = os.getenv("OVERRIDE_RISK_CAP", "").lower() in ("1", "true", "yes")

    # Drawdown alert: send Telegram once if pause threshold hit
    _dd_status = drawdown_tier()
    if _dd_status["pause"]:
        _dd_alert = (
            f"🚨 DRAWDOWN ALERT — {_dd_status['pct']:.1f}% peak-to-trough\n"
            f"Bankroll: ${br:.2f} | Peak: ${peak_bankroll():.2f}\n"
            f"All betting paused. Resume tomorrow at reduced stakes."
        )
        print(_dd_alert)
        _send_telegram(_dd_alert)
    elif _dd_status["props_only"]:
        print(f"[DRAWDOWN] {_dd_status['pct']:.1f}% — props only mode, ML bets blocked, stakes at 50%")
    elif _dd_status["tier"] == 1:
        print(f"[DRAWDOWN] {_dd_status['pct']:.1f}% — minor drawdown, stakes at 75%")

    if not _override_cap and accumulated_risk >= _cap:
        _warn = (
            f"⚠️ DAILY BUDGET ALREADY HIT — ${accumulated_risk:.2f} >= ${_cap:.2f} ({_cap_pct:.0f}% tier)\n"
            f"No new bets will be logged. Set OVERRIDE_RISK_CAP=true to override."
        )
        print(_warn)
        _send_telegram(_warn)

    scout_out = {
        "timestamp": datetime.now(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "date":      today,
        "window":    window,
        "bankroll":  br,
        "games":     [],
        "bets":      [],
        "passes":    [],
    }

    for event in events:
        away_n   = event.get("away", "?")
        home_n   = event.get("home", "?")
        _commence = event.get("commence_utc", "")

        # Window filter — skip games outside this run's time slot
        if not _game_in_window(_commence, window):
            continue

        print(f"\n--- Analyzing: {away_n} @ {home_n} ---")
        # 2-hour filter: never bet on games starting within 2 hours
        if _commence:
            try:
                _game_dt = datetime.fromisoformat(_commence.replace("Z", "+00:00"))
                _hours_until = (_game_dt - datetime.now(pytz.utc)).total_seconds() / 3600
                if _hours_until < 0.0:
                    print(f"  SKIP: game starts in {_hours_until:.1f}h (< 0h) — lineup/SP still volatile")
                    continue
            except Exception:
                pass
        try:
            analysis = analyze_game(event, today)
        except Exception as e:
            print(f"  ERROR in analyze_game: {e}")
            traceback.print_exc()
            continue

        if analysis is None:
            print(f"  SKIP {away_n} @ {home_n}: no market data or unrecognised team code")
            continue

        # Compute confidence scores for both sides
        # Use confidence_engine (logistic regression) when available; fall back to heuristic
        for _cs_side in ("away", "home"):
            if _CONFIDENCE_ENGINE_AVAILABLE:
                try:
                    _conf_feats = _build_conf_features(analysis, _cs_side)
                    analysis[f"{_cs_side}_confidence_score"] = get_confidence_score(_conf_feats)
                except Exception as _ce:
                    analysis[f"{_cs_side}_confidence_score"] = _confidence_score(analysis, _cs_side)
            else:
                analysis[f"{_cs_side}_confidence_score"] = _confidence_score(analysis, _cs_side)

        scout_out["games"].append({
            "away":       analysis["away"],
            "home":       analysis["home"],
            "away_model": analysis["away_model_p"],
            "home_model": analysis["home_model_p"],
            "away_edge":  analysis["away_edge"],
            "home_edge":  analysis["home_edge"],
            "away_xr":    analysis["away_xr"],
            "home_xr":    analysis["home_xr"],
        })

        # Store for series analysis
        game_key_map[(analysis["away"], analysis["home"])] = analysis

        # ── Injury flag alert — collect for slip, do not send mid-loop ──────────
        if analysis.get("injury_flags"):
            game_label = f"{analysis.get('away_name','')} @ {analysis.get('home_name','')}"
            inj_msg = format_injury_section(analysis["injury_flags"], game_label)
            if inj_msg:
                all_injuries.append(inj_msg)

        # ── SGP suggestions for this game ─────────────────────────────────────
        try:
            _sgp_line = analysis.get("totals_line")
            sgp_market = {
                "totals": {"line": _sgp_line} if _sgp_line is not None else None,
                "polymarket": analysis.get("polymarket"),
            }
            sgp_suggestions = build_sgp_suggestions(
                analysis.get("away_sp") or {},
                analysis.get("home_sp") or {},
                analysis.get("away_xr", 4.35),
                analysis.get("home_xr", 4.35),
                analysis.get("nrfi") or {},
                analysis.get("total") or {},
                sgp_market,
                analysis.get("away_model_p", 0.5),
                analysis.get("home_model_p", 0.5),
                bankroll=br,
            )
            if sgp_suggestions:
                all_sgp.extend(sgp_suggestions)

        except Exception as sgp_err:
            print(f"  SGP error: {sgp_err}")
            sgp_suggestions = []

        # ── Hitter props for this game ────────────────────────────────────────
        try:
            game_hitter_props = _fetch_game_hitter_props(
                analysis.get("game_pk"),
                analysis["away"], analysis["home"],
                analysis.get("away_sp") or {},
                analysis.get("home_sp") or {},
                br,
            )
            all_hitter_props.extend(game_hitter_props)
        except Exception as hp_err:
            print(f"  Hitter props error: {hp_err}")
            game_hitter_props = []

        # ── Collect K-props for slip (uses analyze_k_prop when edge ≥ 0.8) ─────
        _game_lbl_kp = f"{analysis.get('away_name','')} @ {analysis.get('home_name','')}"
        for _kside, _sp in (("away", analysis.get("away_sp") or {}), ("home", analysis.get("home_sp") or {})):
            if not _sp or not _sp.get("name") or _sp.get("name") == "TBD" or _sp.get("sp_missing"):
                continue
            _k_line = round(_sp.get("k9", 8.5) * 5.0 / 9, 1)
            _opp_tid = analysis.get("home_tid" if _kside == "away" else "away_tid")
            _park    = analysis.get("home", "")
            _ump_k_f = UMPIRE_TENDENCIES.get(analysis.get("umpire", ""), (1.0, 1.0, ""))[0]
            # Try new analyze_k_prop (richer model) first
            _akp = None
            if _STRIKEOUT_ENGINE_AVAILABLE:
                try:
                    _akp = analyze_k_prop(_sp, _opp_tid, _park, _k_line, ump_k_factor=_ump_k_f)
                except Exception:
                    _akp = None
            if _akp:
                _k_stake = kelly_stake(_akp["model_p"] if "model_p" in _akp else 0.55, "-110", "MEDIUM")
                all_k_props.append({
                    "sp":         _sp.get("name"),
                    "team":       analysis.get(_kside, ""),
                    "game":       _game_lbl_kp,
                    "line":       _k_line,
                    "p_over":     _akp.get("model_p", 0.55),
                    "market_p":   0.5,
                    "edge_pct":   _akp.get("gap", 0) * 10,
                    "stake":      _k_stake,
                    "statcast_2025": False,
                    "projected_k":  _akp.get("projected_k"),
                    "whiff_rate":   _akp.get("whiff_rate"),
                    "confidence":   _akp.get("confidence", 0),
                })
                print(f"  K PROP [{_kside}]: {k_prop_telegram_line(_akp)}")
            else:
                # Fallback to simple k_prop from props_engine
                _k_r = k_prop(_sp, _k_line)
                _p_k = _k_r.get("p_over", 0)
                if _p_k >= 0.55:
                    _k_stake = kelly_stake(_p_k, "-110", "MEDIUM")
                    _sp_sc = {}
                    if _STATCAST_AVAILABLE and _sp.get("pitcher_id"):
                        try:
                            _sp_sc = get_pitcher_statcast(_sp["pitcher_id"])
                        except Exception:
                            _sp_sc = {}
                    all_k_props.append({
                        "sp":             _sp.get("name"),
                        "team":           analysis.get(_kside, ""),
                        "game":           _game_lbl_kp,
                        "line":           _k_line,
                        "p_over":         _p_k,
                        "market_p":       0.5,
                        "edge_pct":       round((_p_k - 0.50) * 100, 1),
                        "stake":          _k_stake,
                        "statcast_2025":  _sp_sc.get("STATCAST_2025", False),
                    })

        # ── ER props for this game ────────────────────────────────────────────
        if _ER_AVAILABLE:
            for _er_side, _er_sp, _er_opp_off, _er_bp in [
                ("away", analysis.get("away_sp") or {}, analysis.get("home_off") or {}, analysis.get("home_bp") or {}),
                ("home", analysis.get("home_sp") or {}, analysis.get("away_off") or {}, analysis.get("away_bp") or {}),
            ]:
                if not _er_sp or _er_sp.get("sp_missing"):
                    continue
                try:
                    # Derive a market-line proxy from ERA × expected IP / 9.
                    # Rounded to nearest 0.5 so it matches typical sportsbook ER lines.
                    # Replace with a live odds feed when one is wired up.
                    _er_era = _er_sp.get("era") or _er_sp.get("xfip") or 4.35
                    _er_gs  = max(_er_sp.get("gs", 1) or 1, 1)
                    _er_ip  = _er_sp.get("ip", 0) or 0
                    _er_ips = min(_er_ip / _er_gs, 6.0)   # cap at 6 IP (typical max)
                    _er_ips = max(_er_ips, 4.5)            # floor at 4.5 IP
                    _er_raw_line = _er_era * _er_ips / 9.0
                    _er_market_line = round(round(_er_raw_line * 2) / 2, 1)  # round to 0.5
                    _er_market_line = max(_er_market_line, 1.5)

                    _er_res = analyze_earned_runs(_er_sp, _er_opp_off, _er_bp,
                                                  market_line=_er_market_line)
                    if _er_res:
                        print(f"  ER PROP [{_er_side}]: {er_prop_telegram_line(_er_res)}")
                except Exception as _er_err:
                    pass

        # ── Collect props data for props_output.json (no per-game Telegram send) ─
        try:
            game_props_entry = _build_props_entry(analysis, sgp_suggestions or [])
            if game_props_entry is not None:
                props_games.append(game_props_entry)
        except Exception as pe:
            print(f"  Props entry error: {pe}")

        bet_found = False
        for side in ("away", "home"):
            if _should_recommend(analysis, side, bet_type="ML"):
                proposed_stake = float(analysis.get(f"{side}_stake", 0))
                proposed_stake = max(proposed_stake, MIN_STAKE)   # enforce $1.00 floor

                # ── Hard daily cap block ───────────────────────────────────────
                if not _override_cap and accumulated_risk + proposed_stake > _cap:
                    _team = analysis.get(f"{side}_name", side)
                    print(
                        f"  BLOCK {_team}: daily cap ${_cap:.2f} would be exceeded "
                        f"(accumulated ${accumulated_risk:.2f} + ${proposed_stake:.2f})"
                    )
                    continue

                accumulated_risk = round(accumulated_risk + proposed_stake, 2)
                all_bets.append(analysis)
                bet_found = True

                edge = analysis.get(f"{side}_edge", 0)
                conv = analysis.get(f"{side}_conv", "PASS")
                if conv == "HIGH" and edge >= 7:
                    all_locks.append((analysis, side))
                elif conv == "MEDIUM" and 4 <= edge < 7:
                    all_flips.append((analysis, side))

                # Validate bet type before logging
                if not DRY_RUN:
                    try:
                        validate_bet("STRAIGHT", conviction_levels=[conv])
                    except BetValidationError as ve:
                        print(f"  [VALIDATOR] {ve}")

                # DB log
                if not DRY_RUN:
                    try:
                        _pt_res  = analysis.get(f"{side}_pitch_trap") or {}
                        _fr_res  = analysis.get(f"{side}_framing")    or {}
                        _sp_data_log = analysis.get(f"{side}_sp") or {}
                        _ump_e_log   = analysis.get("ump_edge") or {}
                        _hd_log      = analysis.get("home_dog") or {}
                        _sharp_s     = analysis.get("market_sharp_signal", "")
                        _db.log_bet(
                            date=today,
                            bet=analysis.get(f"{side}_name", ""),
                            bet_type="ML",
                            game=f"{analysis['away_name']} @ {analysis['home_name']}",
                            sp=_sp_data_log.get("name", ""),
                            park=analysis["home"],
                            umpire=analysis["umpire"],
                            bet_odds=str(analysis.get(f"best_{side}_odds", "")),
                            model_prob=analysis.get(f"{side}_model_p"),
                            market_prob=analysis.get(f"{side}_nv"),
                            edge_pct=analysis.get(f"{side}_edge"),
                            conviction=analysis.get(f"{side}_conv", ""),
                            stake=float(analysis.get(f"{side}_stake", 0)),
                            pitch_trap=_pt_res.get("tag") or None,
                            framing_edge=_fr_res.get("tag") or None,
                            closer_avail=str(analysis.get(f"{side}_key_rel_avail", True)),
                            lineup_slot_score=analysis.get(f"{side}_slot_run_adj"),
                            sharp_signal=_sharp_s if _sharp_s else None,
                            umpire_edge=_ump_e_log.get("tag") or None,
                            home_dog_angle=1 if _hd_log.get("is_home_dog_value") else 0,
                            first_pitch_strike_rate=_sp_data_log.get("fp_strike_rate"),
                            sp_gb_rate=_sp_data_log.get("gb_rate"),
                        )
                    except Exception as e:
                        print(f"DB log error: {e}")

                _sp_data = analysis.get(f"{side}_sp") or {}
                scout_out["bets"].append({
                    "team":             analysis.get(f"{side}_name"),
                    "side":             side,
                    "game":             f"{analysis.get('away_name','')} @ {analysis.get('home_name','')}",
                    "odds":             analysis.get(f"best_{side}_odds"),
                    "book":             analysis.get(f"best_{side}_book"),
                    "model_prob":       analysis.get(f"{side}_model_p"),
                    "market_prob":      analysis.get(f"{side}_nv"),
                    "edge_pct":         analysis.get(f"{side}_edge"),
                    "stake":            analysis.get(f"{side}_stake"),
                    "conviction":       analysis.get(f"{side}_conv"),
                    "confidence_score": analysis.get(f"{side}_confidence_score"),
                    "sp":               _sp_data.get("name"),
                    "sp_era":           _sp_data.get("era"),
                    "sp_xfip":          _sp_data.get("xfip"),
                    "sp_yrfi_lean":     analysis.get(f"{side}_sp_yrfi_lean", False),
                    "bullpen_tier":     analysis.get(f"{side}_bullpen_tier"),
                    "weather_adj":      analysis.get("weather_run_adj"),
                    "platoon_edge":     analysis.get(f"{side}_platoon_edge"),
                    "h2h":              analysis.get("h2h"),
                })
            else:
                # Collect fades: only when SP ERA is significantly above xFIP
                # (ERA-xFIP gap ≥ 1.0 means real regression signal, not noise).
                # Limit to one team per game so we never show both sides as fades.
                sp     = analysis.get(f"{side}_sp") or {}
                sp_era  = sp.get("era")
                sp_xfip = sp.get("xfip")
                game_key = f"{analysis.get('away','')}@{analysis.get('home','')}"
                if (sp_era is not None and sp_xfip is not None
                        and (sp_era - sp_xfip) >= 1.0
                        and game_key not in _faded_games):
                    _faded_games.add(game_key)
                    sp_name = sp.get("name", "SP")
                    reason  = (f"{sp_name} ERA {sp_era:.2f} vs xFIP {sp_xfip:.2f} "
                               f"— ERA due to regress ({sp_era - sp_xfip:+.2f} gap)")
                    all_fades.append((analysis, side, reason))

        # ── Collect NRFI and totals for daily slip ─────────────────────────────
        # Only recommend these props when both SPs have real data — league-average
        # fallbacks produce meaningless probability estimates.
        _a_sp = analysis.get("away_sp") or {}
        _h_sp = analysis.get("home_sp") or {}
        _sp_ok = not _a_sp.get("sp_missing") and not _h_sp.get("sp_missing")

        nrfi_r_g  = analysis.get("nrfi") or {}
        total_r_g = analysis.get("total") or {}
        game_lbl  = f"{analysis.get('away_name','')} @ {analysis.get('home_name','')}"

        if _sp_ok:
            nrfi_note = nrfi_r_g.get("note")
            if nrfi_note in ("nrfi", "yrfi"):
                direction = "NRFI" if nrfi_note == "nrfi" else "YRFI"
                prob  = nrfi_r_g["p_nrfi"] if direction == "NRFI" else nrfi_r_g["p_yrfi"]
                stake = round(min(br * 0.010, 5.0), 2)
                all_nrfi.append({"game": game_lbl, "direction": direction, "prob": prob, "stake": stake})

            line       = analysis.get("totals_line")
            best_over  = analysis.get("totals_best_over")
            best_under = analysis.get("totals_best_under")
            p_over     = total_r_g.get("p_over", 0)
            p_under    = total_r_g.get("p_under", 0)
            if line is None:
                print(f"  SKIP totals {game_lbl}: no real market line — never defaulting to 8.5")
            elif p_over > 0 or p_under > 0:
                best_total_edge = 0.0
                best_total_bet  = None
                for direction, model_p, mkt_odds in (
                    ("OVER",  p_over,  best_over),
                    ("UNDER", p_under, best_under),
                ):
                    if mkt_odds is None:
                        continue
                    mkt_p = (implied_prob(str(mkt_odds)) or 0) / 100
                    if mkt_p <= 0:
                        continue
                    # Hard filters: skip extreme market prices (no value)
                    # and unrealistic model estimates (miscalibrated λ).
                    if mkt_p >= 0.75:
                        print(f"  SKIP totals {game_lbl} {direction}: mkt_p={mkt_p:.1%} ≥75% — no edge")
                        continue
                    if model_p >= 0.75:
                        print(f"  SKIP totals {game_lbl} {direction}: model_p={model_p:.1%} ≥75% — λ likely off")
                        continue
                    edge = model_p - mkt_p
                    if edge >= 0.03 and edge > best_total_edge:
                        best_total_edge = edge
                        best_total_bet = {
                            "game":      game_lbl,
                            "direction": direction,
                            "line":      line,
                            "prob":      round(model_p, 4),
                            "market_p":  round(mkt_p, 4),
                            "edge_pct":  round(edge * 100, 1),
                            "stake":     round(min(br * 0.0075, 4.0), 2),
                        }
                if best_total_bet and best_total_bet["stake"] > 0:
                    all_totals.append(best_total_bet)
                else:
                    print(f"  SKIP totals {game_lbl}: no side with ≥3% edge over market")

        if not bet_found:
            all_pass.append(analysis)
            scout_out["passes"].append({
                "game":  f"{analysis['away_name']} @ {analysis['home_name']}",
                "edges": {"away": analysis["away_edge"], "home": analysis["home_edge"]},
            })

    # Fix: count only the actually-recommended side's stake, not both
    n_bets = len(all_locks) + len(all_flips)
    ml_risk_scout = sum(
        a.get(f"{s}_stake", 0) for a, s in all_locks + all_flips
    )
    print(f"\nScout done [{_window_label}] — {len(events)} total | {n_bets} ML bets | ${ml_risk_scout:.2f} ML at risk")

    # ── Near-miss Telegram: if no bets qualified, show top 3 closest games ────
    if n_bets == 0 and not _dd_status["pause"]:
        _nm_candidates = []
        for _nm_a in all_pass:
            for _nm_side in ("away", "home"):
                _nm_edge  = _nm_a.get(f"{_nm_side}_edge", 0)
                _nm_model = _nm_a.get(f"{_nm_side}_model_p", 0)
                _nm_conf  = _nm_a.get(f"{_nm_side}_confidence_score", 0)
                _nm_team  = _nm_a.get(f"{_nm_side}_name", _nm_side)
                _nm_conv  = _nm_a.get(f"{_nm_side}_conv", "PASS")
                if _nm_edge <= 0:
                    continue
                _reasons = []
                if _nm_edge < 4.0:
                    _reasons.append(f"edge {_nm_edge:+.1f}% < 4%")
                if _nm_model < 0.48:
                    _reasons.append(f"model {_nm_model:.3f} < 0.48")
                _mc = 60 if _nm_conv == "HIGH" else 55
                if _nm_conf < _mc:
                    _reasons.append(f"conf {_nm_conf}/100 < {_mc}")
                if not _reasons:
                    continue
                _nm_candidates.append({
                    "team": _nm_team, "edge": _nm_edge,
                    "model": _nm_model, "conf": _nm_conf,
                    "reasons": _reasons,
                    "game": f"{_nm_a.get('away_name','')} @ {_nm_a.get('home_name','')}",
                    "odds": _nm_a.get(f"best_{_nm_side}_odds"),
                })
        _nm_candidates.sort(key=lambda x: x["edge"], reverse=True)
        _top3 = _nm_candidates[:3]
        if _top3:
            _nm_lines = ["⚠️ No qualifying bets today — top near-misses:"]
            for _i, _nm in enumerate(_top3, 1):
                _why = " | ".join(_nm["reasons"])
                _odds_s = ""
                if _nm.get("odds") is not None:
                    _o = _nm["odds"]
                    _odds_s = f" ({'+' if isinstance(_o, int) and _o > 0 else ''}{_o})"
                _nm_lines.append(
                    f"{_i}. {_nm['team']}{_odds_s} [{_nm['game']}]"
                    f" — edge {_nm['edge']:+.1f}% | {_why}"
                )
            _nm_msg = "\n".join(_nm_lines)
            # Strip HTML tags — near-miss sent as plain text to avoid Telegram 400
            import re as _re
            _nm_msg_clean = _re.sub(r"<[^>]+>", "", _nm_msg)
            print(_nm_msg_clean)
            _send_telegram(_nm_msg_clean)

    # ── Top 3 props — always send, regardless of ML pick count ───────────────
    # Collect best props (K + hitter) by edge, lowered threshold to 3% for this section
    _top_props_all = []
    for _kb in all_k_props:
        if _kb.get("edge_pct", 0) >= 3.0:
            _top_props_all.append({
                "label": f"{_kb['sp']} O{_kb['line']}K",
                "edge": _kb.get("edge_pct", 0),
                "p": _kb.get("p_over", 0),
                "game": _kb.get("game", ""),
            })
    for _hb in all_hitter_props:
        if _hb.get("edge_pct", 0) >= 3.0:
            _top_props_all.append({
                "label": f"{_hb['player']} {_hb.get('prop','')}",
                "edge": _hb.get("edge_pct", 0),
                "p": _hb.get("model_prob", 0),
                "game": _hb.get("game", ""),
            })
    _top_props_all.sort(key=lambda x: x["edge"], reverse=True)
    _top_props_3 = _top_props_all[:3]
    if _top_props_3:
        _tp_lines = [f"📊 TOP PROPS TODAY ({today}):"]
        for _i, _tp in enumerate(_top_props_3, 1):
            _tp_lines.append(
                f"  {_i}. {_tp['label']} — model {_tp['p']:.1%} | edge +{_tp['edge']:.1f}% | {_tp['game']}"
            )
        _tp_msg = "\n".join(_tp_lines)
        print(_tp_msg)
        # Only send standalone props message when there are no ML picks (otherwise props appear in slip)
        if n_bets == 0:
            _send_telegram(_tp_msg)

    # ── Series analysis (game 1 of series today) ──────────────────────────────
    print("Running series analysis...")
    try:
        _series_analysis(events, today, game_key_map)
    except Exception as series_err:
        print(f"Series analysis error: {series_err}")

    # ── Player profile updates — run in background after picks are sent ──────
    # Collect the data the thread needs before it starts (snapshots of local vars).
    _profile_analyses   = list(game_key_map.values())
    _profile_hitter_top = _HITTER_PROP_TOP_N

    def _run_profile_updates():
        import threading
        _tname = threading.current_thread().name
        print(f"[PROFILE] background thread {_tname} starting...")
        _profiled_sps: set = set()
        _profiled_bps: set = set()
        _sp_count = _bp_count = _h_count = _skip_count = 0

        for _analysis in _profile_analyses:
            _away_code = _analysis.get("away", "")
            _home_code = _analysis.get("home", "")
            _away_tid  = MLB_TEAM_IDS.get(_away_code)
            _home_tid  = MLB_TEAM_IDS.get(_home_code)

            # ── SP profiles ───────────────────────────────────────────────────
            for _sp in (_analysis.get("away_sp") or {}, _analysis.get("home_sp") or {}):
                _sp_id   = _sp.get("pitcher_id")
                _sp_name = _sp.get("name", "")
                if not _sp_id or not _sp_name or _sp_name == "TBD":
                    continue
                if _sp.get("sp_missing") or _sp_id in _profiled_sps:
                    continue
                if pitcher_profile_updated_today(_sp_name):
                    _skip_count += 1
                    _profiled_sps.add(_sp_id)
                    continue
                try:
                    update_sp_profile(_sp_name, _sp_id)
                    _profiled_sps.add(_sp_id)
                    _sp_count += 1
                except Exception as _e:
                    print(f"  [PROFILE] SP error ({_sp_name}): {_e}")

            # ── Bullpen profiles ──────────────────────────────────────────────
            for _tc, _tid in ((_away_code, _away_tid), (_home_code, _home_tid)):
                if not _tid or _tc in _profiled_bps:
                    continue
                try:
                    update_bullpen_profile(_tid, _tc)
                    _profiled_bps.add(_tc)
                    _bp_count += 1
                except Exception as _e:
                    print(f"  [PROFILE] Bullpen error ({_tc}): {_e}")

        # ── Hitter profiles ───────────────────────────────────────────────────
        _seen_hitter_ids: set = set()
        for _analysis in _profile_analyses:
            for _side in ("away", "home"):
                _tc  = _analysis.get(_side, "")
                _off = _analysis.get(f"{_side}_off") or {}
                for _player in (_off.get("lineup") or [])[:_profile_hitter_top]:
                    _hid   = _player.get("id")
                    _hname = _player.get("name") or _player.get("fullName", "")
                    if not _hid or not _hname or _hid in _seen_hitter_ids:
                        continue
                    if hitter_profile_updated_today(_hname):
                        _skip_count += 1
                        _seen_hitter_ids.add(_hid)
                        continue
                    try:
                        update_hitter_profile(_hname, _hid, _tc)
                        _seen_hitter_ids.add(_hid)
                        _h_count += 1
                    except Exception as _e:
                        print(f"  [PROFILE] Hitter error ({_hname}): {_e}")

        print(f"[PROFILE] Done — {_sp_count} SPs, {_bp_count} bullpens, "
              f"{_h_count} hitters updated, {_skip_count} skipped (already done today)")

    import threading as _threading
    _profile_thread = _threading.Thread(target=_run_profile_updates,
                                        name="profile-updater", daemon=True)
    # Thread starts after the Telegram slip is sent (below). Stored here so the
    # slip section can call _profile_thread.start() at the right moment.

    # ── Build canonical pick IDs for deduplication ────────────────────────────
    def _pick_id_ml(analysis: dict, side: str) -> str:
        return f"ML:{analysis.get(f'{side}_name','')}:{analysis.get(f'best_{side}_odds','')}"

    def _pick_id_total(b: dict) -> str:
        return f"TOT:{b['game']}:{b['direction']}:{b['line']}"

    def _pick_id_nrfi(b: dict) -> str:
        return f"NRFI:{b['game']}:{b['direction']}"

    def _pick_id_kprop(b: dict) -> str:
        return f"KPROP:{b['sp']}:{b['line']}"

    def _pick_id_hitter(h: dict) -> str:
        return f"HITTER:{h['player']}:{h.get('team','')}:{h['prop']}"

    current_pick_ids: set = set()
    for a, s in all_locks + all_flips:
        current_pick_ids.add(_pick_id_ml(a, s))
    for b in all_totals:
        current_pick_ids.add(_pick_id_total(b))
    for b in all_nrfi:
        current_pick_ids.add(_pick_id_nrfi(b))
    for b in all_k_props:
        if b.get("edge_pct", 0) >= 5.0:
            current_pick_ids.add(_pick_id_kprop(b))
    for h in all_hitter_props:
        if h.get("edge_pct", 0) >= 5.0:
            current_pick_ids.add(_pick_id_hitter(h))

    # ── Check if a slip was already sent today ─────────────────────────────────
    prev_pick_ids: set = set()
    slip_already_sent = False
    try:
        with open("last_scout.json") as _lf:
            _prev = json.load(_lf)
        if _prev.get("date") == today and _prev.get("slip_sent"):
            _prev_quality = _prev.get("slip_quality", "good")
            if _prev_quality == "good":
                slip_already_sent = True
                prev_pick_ids = set(_prev.get("sent_pick_ids", []))
            else:
                print(
                    "[SLIP] Previous slip marked as BAD QUALITY (near-zero stakes from RED day bug) "
                    "— treating as unsent, will resend with correct stakes"
                )
    except Exception:
        pass

    new_pick_ids = current_pick_ids - prev_pick_ids

    # ML-only new picks — props/totals changes never trigger the update message
    current_ml_ids = {_pick_id_ml(a, s) for a, s in all_locks + all_flips}
    prev_ml_ids    = {pid for pid in prev_pick_ids if pid.startswith("ML:")}
    new_ml_pick_ids = current_ml_ids - prev_ml_ids

    # slip_sent defaults to False — only set True after a confirmed successful send
    # that contained at least 1 bet. This prevents empty or failed sends from
    # blocking legitimate retries on the next run (Bug 3 fix).
    scout_out["slip_sent"]     = False
    scout_out["sent_pick_ids"] = sorted(prev_pick_ids)   # keep old IDs until confirmed sent

    # ── Guard: don't send RED day if lineups are broadly unconfirmed ─────────
    # If >60% of today's games have unconfirmed lineups and we have zero locks,
    # lineups haven't posted yet — send a holding message instead of RED.
    _all_analyses = list(game_key_map.values())
    _n_games_total = len(_all_analyses)
    if _n_games_total > 0 and len(all_locks) == 0:
        _unconf_count = sum(
            1 for _a in _all_analyses
            if not _a.get("away_lineup_confirmed") or not _a.get("home_lineup_confirmed")
        )
        if _unconf_count / _n_games_total > 0.60:
            _msg = (
                f"⏳ PARLAY OS — {today}\n"
                f"Lineups unconfirmed for {_unconf_count}/{_n_games_total} games — "
                f"picks pending.\nRe-running when lineups post (typically 11–11:30am ET)."
            )
            print(f"[SLIP] Lineup hold — {_unconf_count}/{_n_games_total} unconfirmed, skipping RED slip")
            _send_telegram(_msg)
            with open("last_scout.json", "w") as _sf:
                json.dump(scout_out, _sf, indent=2, default=str)
            return

    # ── Dedup: remove fades that contradict an active bet on the same team ──────
    _bet_teams = {a.get(f"{s}_name") for a, s in all_locks + all_flips}
    _before_fade_count = len(all_fades)
    all_fades = [(a, s, r) for a, s, r in all_fades
                 if a.get(f"{s}_name") not in _bet_teams]
    if len(all_fades) < _before_fade_count:
        print(f"[SLIP] Removed {_before_fade_count - len(all_fades)} fade(s) that contradicted active bets")

    # ── Daily bet slip ────────────────────────────────────────────────────────
    _slip_sent_ok = False
    if slip_already_sent and not new_ml_pick_ids:
        n_other = len(new_pick_ids - current_ml_ids)
        print(
            f"[SLIP] Slip already sent today — no new ML picks "
            f"(props/totals changes: {n_other}). Skipping update."
        )
    elif slip_already_sent:
        # New locks confirmed after the first slip (e.g. night game SPs posted).
        # Send a targeted UPDATE with the new picks and a rebuilt parlay.
        print(
            f"[SLIP] Slip already sent today — {len(new_ml_pick_ids)} new ML pick(s). "
            f"Sending update with rebuilt parlay."
        )
        _slip_sent_ok = _send_slip_update(
            new_ml_pick_ids, all_locks, all_flips,
            all_totals, all_nrfi, all_k_props, all_hitter_props,
            today, br,
        )
    else:
        print(
            f"Building daily bet slip — "
            f"locks={len(all_locks)} flips={len(all_flips)} nrfi={len(all_nrfi)} "
            f"totals={len(all_totals)} k_props={len(all_k_props)} "
            f"hitter_props={len(all_hitter_props)} injuries={len(all_injuries)}"
        )
        try:
            _slip_sent_ok = _daily_bet_slip(
                all_locks, all_flips, all_sgp, all_fades, br,
                all_nrfi, all_totals, all_hitter_props, all_k_props, all_injuries,
            )
        except Exception as slip_err:
            print(f"Daily slip EXCEPTION: {slip_err}")
            traceback.print_exc()
            _slip_sent_ok = False

    # Only mark the slip as sent if Telegram confirmed delivery AND the message
    # contained at least 1 bet. Empty or failed sends allow a retry next run.
    if _slip_sent_ok:
        # Slip quality: good only if avg Kelly stake > $1.00 (guards against RED day zero-stakes bug)
        _sent_stakes = [float(a.get(f"{s}_stake") or 0) for a, s in all_locks + all_flips]
        _avg_sent_stake = sum(_sent_stakes) / len(_sent_stakes) if _sent_stakes else 0.0
        _slip_quality = "good" if _avg_sent_stake > MIN_STAKE and bool(all_locks or all_flips) else "bad"
        scout_out["slip_sent"]     = True
        scout_out["slip_quality"]  = _slip_quality
        scout_out["sent_pick_ids"] = sorted(current_pick_ids)
        print(
            f"[SLIP] Marked as sent — quality={_slip_quality} "
            f"avg_stake=${_avg_sent_stake:.2f} — {len(current_pick_ids)} pick IDs recorded."
        )
    else:
        scout_out["slip_quality"] = "bad"
        print("[SLIP] Slip NOT marked as sent — will retry on next run.")

    # ── Sanity check: bets found but slip not sent ────────────────────────────
    if n_bets > 0 and not _slip_sent_ok and not slip_already_sent:
        _sanity_msg = (
            f"[SANITY FAIL] Scout found {n_bets} ML bet(s) but slip sent 0 — "
            f"check RED day logic and pool calculations"
        )
        print(_sanity_msg)
        if not DRY_RUN:
            _send_telegram(_sanity_msg)

    # ── Public channel post ───────────────────────────────────────────────────
    if PUBLIC_CHANNEL_ID:
        try:
            _post_public_channel(all_locks, all_flips, today)
        except Exception as pub_err:
            print(f"Public channel error: {pub_err}")

    # ── Launch profile updates in the background ──────────────────────────────
    # Picks are already sent above; profiles run asynchronously so they don't
    # block the scout return or delay the next scheduled run.
    _profile_thread.start()
    print("[PROFILE] Background profile updates started")

    # Write analyzed SPs and lineups to tracker tables for SP monitor
    if not DRY_RUN:
        for analysis in game_key_map.values():
            try:
                _gk = str(analysis.get("game_pk") or "")
                if not _gk or _gk == "0":
                    continue
                _db.upsert_sp_tracker(
                    today,
                    _gk,
                    analysis.get("away_name", ""),
                    analysis.get("home_name", ""),
                    int(analysis.get("away_sp", {}).get("pitcher_id") or 0),
                    analysis.get("away_sp", {}).get("name", "TBD"),
                    float(analysis.get("away_sp", {}).get("xfip") or 4.35),
                    int(analysis.get("home_sp", {}).get("pitcher_id") or 0),
                    analysis.get("home_sp", {}).get("name", "TBD"),
                    float(analysis.get("home_sp", {}).get("xfip") or 4.35),
                    analysis.get("game_time_et", ""),
                )
                # Lineup tracker — write projected top-4 for each team
                for _side in ("away", "home"):
                    _off = analysis.get(f"{_side}_off") or {}
                    _lineup = _off.get("lineup") or []
                    _team = analysis.get(f"{_side}_name", "")
                    if _lineup and _team:
                        _db.upsert_lineup_tracker(today, _gk, _team, _lineup[:4])
            except Exception as _te:
                print(f"[TRACKER] write error for {analysis.get('away_name')} @ {analysis.get('home_name')}: {_te}")

    # Save scout output
    if not DRY_RUN:
        with open("last_scout.json", "w") as f:
            json.dump(scout_out, f, indent=2)
        _db.log_scout_run(today, n_bets, json.dumps(scout_out))

        # Write props_output.json so /props command and API have current data
        props_out = {
            "date":      today,
            "timestamp": datetime.now(ET).isoformat(),
            "games":     props_games,
        }
        try:
            with open("props_output.json", "w") as f:
                json.dump(props_out, f, indent=2)
        except Exception as pe:
            print(f"props_output.json write error: {pe}")

        # Persist both blobs to DB so Railway dashboard reads fresh data
        try:
            _db.save_scout_output(today, json.dumps(scout_out), json.dumps(props_out))
        except Exception as dbe:
            print(f"scout_output DB write error: {dbe}")

    return scout_out


# ── PROPS MESSAGE FORMAT ──────────────────────────────────────────────────────

def _build_props_entry(analysis: dict, sgp_list: list) -> dict:
    """Build per-game props dict for props_output.json."""
    away_sp     = analysis.get("away_sp") or {}
    home_sp     = analysis.get("home_sp") or {}
    nrfi_r      = analysis.get("nrfi") or {}
    total_r     = analysis.get("total") or {}
    totals_line = analysis.get("totals_line")  # may be None — handled per-section below

    props = []

    for sp_side, sp in (("away", away_sp), ("home", home_sp)):
        if not sp or not sp.get("name") or sp.get("name") == "TBD":
            continue
        name   = sp["name"]
        k_line = round(sp.get("k9", 8.5) * 5.0 / 9, 1)
        k_r    = k_prop(sp, k_line)
        p_k    = k_r.get("p_over", 0)
        props.append({
            "type":          "K_PROP",
            "sp":            name,
            "sp_side":       sp_side,
            "model_line":    k_line,
            "p_over":        round(p_k, 4),
            "recommendation": "BET" if p_k >= 0.55 else "PASS",
        })

    sp_data_missing = away_sp.get("sp_missing") or home_sp.get("sp_missing")

    if sp_data_missing:
        props.append({"type": "NRFI", "recommendation": "SP_MISSING"})
    else:
        p_nrfi = nrfi_r.get("p_nrfi", 0)
        p_yrfi = nrfi_r.get("p_yrfi", 0)
        props.append({
            "type":          "NRFI",
            "p_nrfi":        round(p_nrfi, 4),
            "p_yrfi":        round(p_yrfi, 4),
            "recommendation": "NRFI" if p_nrfi >= 0.62 else ("YRFI" if p_yrfi >= 0.62 else "PASS"),
        })

    if totals_line is None:
        props.append({"type": "TOTAL", "recommendation": "NO_LINE"})
    elif sp_data_missing:
        props.append({"type": "TOTAL", "recommendation": "SP_MISSING"})
    else:
        p_over  = total_r.get("p_over", 0)
        p_under = total_r.get("p_under", 0)
        model_total = round((analysis.get("away_xr", 0) + analysis.get("home_xr", 0)), 2)
        props.append({
            "type":          "TOTAL",
            "model_total":   model_total,
            "market_line":   totals_line,
            "p_over":        round(p_over, 4),
            "p_under":       round(p_under, 4),
            "recommendation": (
                f"OVER {totals_line}" if p_over >= 0.55
                else (f"UNDER {totals_line}" if p_under >= 0.55 else "PASS")
            ),
        })

    sgp_out = []
    for sgp in sgp_list:
        sgp_out.append({
            "type":       sgp.get("type", "SGP"),
            "legs":       sgp.get("legs", []),
            "joint_prob": round(sgp.get("joint_prob", 0), 4),
            "kelly_stake": round(sgp.get("kelly_stake", 0), 2),
            "ev":          round(sgp.get("ev", 0), 4),
        })

    return {
        "away":      analysis.get("away"),
        "home":      analysis.get("home"),
        "away_name": analysis.get("away_name"),
        "home_name": analysis.get("home_name"),
        "time":      analysis.get("game_time_et", ""),
        "asp":       away_sp.get("name", "TBD"),
        "hsp":       home_sp.get("name", "TBD"),
        "props":     props,
        "sgp":       sgp_out,
    }


def _format_props_message(
    analysis: dict,
    sgp_list: list,
    br: float,
    hitter_props: list | None = None,
) -> str | None:
    """
    Format categorized props block for a game.
    Returns None if there's nothing worth showing.
    """
    away_sp     = analysis.get("away_sp") or {}
    home_sp     = analysis.get("home_sp") or {}
    nrfi_r      = analysis.get("nrfi") or {}
    total_r     = analysis.get("total") or {}
    totals_line = analysis.get("totals_line")
    if totals_line is None:
        return
    game_time   = analysis.get("game_time_et", "")
    away_name   = analysis.get("away_name", analysis.get("away", ""))
    home_name   = analysis.get("home_name", analysis.get("home", ""))
    away_code   = analysis.get("away", "")
    home_code   = analysis.get("home", "")
    game_label  = f"{away_name} @ {home_name}"
    time_label  = f" — {game_time}" if game_time else ""
    any_bet     = False

    def _est_odds(prob: float) -> str:
        """Estimate American odds string from model prob with 5% juice."""
        if not prob or prob <= 0 or prob >= 1:
            return ""
        dec = (1.0 / prob) * 1.05
        return decimal_to_american(dec)

    # ── PITCHER ────────────────────────────────────────────────────────────────
    pitcher_lines = []
    for sp in (away_sp, home_sp):
        if not sp or not sp.get("name") or sp.get("name") == "TBD":
            continue
        name      = sp["name"]
        k_line    = round(sp.get("k9", 8.5) * 5.0 / 9, 1)
        k_r       = k_prop(sp, k_line)
        p_k       = k_r.get("p_over", 0)
        odds      = _est_odds(p_k)
        odds_s    = f" {odds}" if odds else ""
        era_tag   = f" ERA:{sp.get('era','?')}"
        xfip_tag  = f" xFIP:{sp.get('xfip'):.2f}" if sp.get("xfip") is not None else ""
        sp_stats  = era_tag + xfip_tag
        if p_k >= 0.55:
            stake    = round(br * (0.02 if p_k >= 0.60 else 0.01), 2)
            edge_est = round((p_k - 0.50) * 100, 1)
            pitcher_lines.append(
                f"✅ BET: {name} O{k_line}K{odds_s} — ${stake:.2f} — EDGE: +{edge_est:.1f}% — MODEL: {p_k:.1%}{sp_stats}"
            )
            any_bet = True
        else:
            pitcher_lines.append(
                f"❌ PASS: {name} O{k_line}K — insufficient edge ({p_k:.1%}){sp_stats}"
            )

    sp_data_missing = away_sp.get("sp_missing") or home_sp.get("sp_missing")

    if sp_data_missing:
        pitcher_lines.append("⚠ NRFI/YRFI skipped — SP data unavailable")
    else:
        p_nrfi = nrfi_r.get("p_nrfi", 0)
        p_yrfi = nrfi_r.get("p_yrfi", 0)
        if p_nrfi >= 0.62:
            stake     = round(br * 0.015, 2)
            edge_est  = round((p_nrfi - 0.50) * 100, 1)
            odds      = _est_odds(p_nrfi)
            odds_s    = f" {odds}" if odds else ""
            pitcher_lines.append(f"✅ BET: NRFI{odds_s} — ${stake:.2f} — EDGE: +{edge_est:.1f}%")
            any_bet = True
        elif p_yrfi >= 0.62:
            stake     = round(br * 0.015, 2)
            edge_est  = round((p_yrfi - 0.50) * 100, 1)
            odds      = _est_odds(p_yrfi)
            odds_s    = f" {odds}" if odds else ""
            pitcher_lines.append(f"✅ BET: YRFI{odds_s} — ${stake:.2f} — EDGE: +{edge_est:.1f}%")
            any_bet = True
        else:
            pitcher_lines.append(
                f"❌ PASS: NRFI ({p_nrfi:.1%}) / YRFI ({p_yrfi:.1%}) — no edge"
            )

    # ── HITTERS ────────────────────────────────────────────────────────────────
    hitter_lines: list[str] = []
    for hp in (hitter_props or []):
        odds_s = _est_odds(hp["model_prob"])
        odds_d = f" ({odds_s})" if odds_s else ""
        hitter_lines.append(
            f"✅ BET: {hp['player']} — {hp['prop']}{odds_d} — "
            f"${hp['stake']:.2f} — EDGE: +{hp['edge_pct']:.1f}% — "
            f"MODEL: {hp['model_prob']:.1%} [{hp['n_games']}g]"
        )
        any_bet = True

    # ── TEAM TOTALS ────────────────────────────────────────────────────────────
    team_lines = []
    if sp_data_missing:
        team_lines.append("⚠ Totals skipped — SP data unavailable")
    else:
        p_over  = total_r.get("p_over", 0)
        p_under = total_r.get("p_under", 0)
        if p_over >= 0.55:
            stake    = round(br * 0.015, 2)
            edge_est = round((p_over - 0.50) * 100, 1)
            odds     = _est_odds(p_over)
            odds_s   = f" {odds}" if odds else ""
            team_lines.append(
                f"✅ BET: {away_code} total O{totals_line}{odds_s} — ${stake:.2f} — EDGE: +{edge_est:.1f}%"
            )
            any_bet = True
        elif p_under >= 0.55:
            stake    = round(br * 0.015, 2)
            edge_est = round((p_under - 0.50) * 100, 1)
            odds     = _est_odds(p_under)
            odds_s   = f" {odds}" if odds else ""
            team_lines.append(
                f"✅ BET: {home_code} total U{totals_line}{odds_s} — ${stake:.2f} — EDGE: +{edge_est:.1f}%"
            )
            any_bet = True
        else:
            team_lines.append(
                f"❌ PASS: game total {totals_line} — market efficient (O={p_over:.1%} U={p_under:.1%})"
            )

    # ── SAME-GAME PARLAY ───────────────────────────────────────────────────────
    sgp_lines = []
    for sgp in sgp_list:
        legs  = " + ".join(sgp.get("legs", [])[:3])
        stake = sgp.get("kelly_stake", 0) or 0
        joint = sgp.get("joint_prob", 0)
        corr  = sgp.get("correlation", "")
        if joint > 0:
            combined_am = decimal_to_american((1.0 / joint) * 0.85)
        else:
            combined_am = "N/A"
        corr_label = corr.split("—")[0].strip() if "—" in corr else corr
        sgp_lines.append(
            f"✅ {legs} ({combined_am}) — ${stake:.2f} — {corr_label}"
        )
        any_bet = True

    if not any_bet:
        return None

    sections = [f"PROPS — {game_label}{time_label}"]
    if pitcher_lines:
        sections.append("PITCHER:\n" + "\n".join(pitcher_lines))
    if hitter_lines:
        sections.append("HITTERS:\n" + "\n".join(hitter_lines))
    if team_lines:
        sections.append("TEAM:\n" + "\n".join(team_lines))
    if sgp_lines:
        sections.append("SAME-GAME PARLAY:\n" + "\n".join(sgp_lines))

    return "\n\n".join(sections)


# ── NIGHTLY DEBRIEF ───────────────────────────────────────────────────────────

def _run_debrief():
    """Pull today's settled bets, compute day results, send formatted Telegram."""
    today  = date.today().isoformat()
    bets   = _db.get_bets(date=today)
    all_bets = _db.get_bets()

    resolved = [b for b in bets if b.get("result") in ("W", "L", "P")]
    pending  = [b for b in bets if not b.get("result")]

    if not resolved and not pending:
        print(f"Debrief: no bets found for {today} — skipping Telegram")
        _run_db_backup()
        return

    wins   = sum(1 for b in resolved if b["result"] == "W")
    losses = sum(1 for b in resolved if b["result"] == "L")

    day_pnl = 0.0
    for b in resolved:
        stake = float(b.get("stake") or 0)
        if b["result"] == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            if dec:
                day_pnl += (dec - 1) * stake
        elif b["result"] == "L":
            day_pnl -= stake
    day_pnl = round(day_pnl, 2)

    total_staked = sum(float(b.get("stake") or 0) for b in resolved if b["result"] != "P")
    roi          = round(day_pnl / total_staked * 100, 1) if total_staked > 0 else 0.0

    # CLV
    from math_engine import calc_clv as _calc_clv
    clv_vals = []
    for b in resolved:
        if b.get("closing_odds") and b.get("bet_odds"):
            c = _calc_clv(str(b["bet_odds"]), str(b["closing_odds"]))
            if c.get("clv_pct") is not None:
                clv_vals.append(c["clv_pct"])
    avg_clv = round(sum(clv_vals) / len(clv_vals), 2) if clv_vals else None

    # Best call (biggest win P&L), worst call (biggest loss stake)
    best_call = worst_call = None
    best_pnl  = -999.0
    worst_pnl = 999.0
    for b in resolved:
        stake = float(b.get("stake") or 0)
        if b["result"] == "W":
            dec = american_to_decimal(str(b.get("bet_odds", "")))
            b_pnl = round((dec - 1) * stake, 2) if dec else 0.0
            if b_pnl > best_pnl:
                best_pnl, best_call = b_pnl, b
        elif b["result"] == "L":
            if -stake < worst_pnl:
                worst_pnl, worst_call = -stake, b

    # Model accuracy
    non_push = [b for b in resolved if b["result"] != "P"]
    correct  = sum(
        1 for b in non_push
        if (b["result"] == "W" and (b.get("model_prob") or 0.5) >= 0.5)
        or (b["result"] == "L" and (b.get("model_prob") or 0.5) < 0.5)
    )
    model_acc = round(correct / len(non_push) * 100, 1) if non_push else None

    br   = current_bankroll()
    from bankroll_engine import peak_bankroll as _peak_br
    peak = _peak_br()
    sign = "+" if day_pnl >= 0 else ""

    lines = [
        f"📊 DAILY DEBRIEF — {date.today().strftime('%b %d, %Y')}",
        f"Record: {wins}W-{losses}L | P&L: {sign}${day_pnl:.2f} | ROI: {sign}{roi:.1f}%",
    ]
    if avg_clv is not None:
        lines.append(f"Avg CLV: {avg_clv:+.2f}% ({len(clv_vals)} bets with CLV data)")
    if best_call:
        conv   = best_call.get("conviction", "") or ""
        edge   = best_call.get("edge_pct")
        odds_i = best_call.get("bet_odds", "")
        # "why it worked" — edge confidence + dog/chalk context
        why = []
        if conv and conv not in ("MANUAL", ""):
            why.append(f"{conv} conviction")
        if edge:
            why.append(f"+{edge:.1f}% edge")
        try:
            if int(str(odds_i).replace("+", "")) > 0:
                why.append("underdog cashed")
        except Exception:
            pass
        why_str = " / ".join(why) if why else "model correct"
        lines.append(
            f"✅ Best call: {best_call.get('bet','')} {best_call.get('type','ML')} "
            f"{odds_i} — +${best_pnl:.2f} — {why_str}"
        )
    if worst_call:
        conv   = worst_call.get("conviction", "") or ""
        edge   = worst_call.get("edge_pct")
        mp     = worst_call.get("model_prob")
        # "what we missed"
        miss = []
        if edge and float(edge) > 0:
            miss.append(f"had +{float(edge):.1f}% model edge but lost")
        elif mp and float(mp) > 0.5:
            miss.append(f"model said {float(mp):.0%} but result went other way")
        else:
            miss.append("no edge — variance")
        miss_str = " / ".join(miss)
        lines.append(
            f"❌ Worst call: {worst_call.get('bet','')} {worst_call.get('type','ML')} "
            f"{worst_call.get('bet_odds','')} — -${abs(worst_pnl):.2f} — {miss_str}"
        )
    if model_acc is not None:
        lines.append(f"Model accuracy today: {model_acc:.1f}%")
    lines.append(f"Bankroll: ${br:.2f} (all-time peak: ${peak:.2f})")
    if pending:
        lines.append(f"⏳ {len(pending)} bet(s) still pending settlement")

    msg = "\n".join(lines)
    print(msg)
    _send_telegram(msg)

    # ── Feed today's settled bets into the brain learning system ─────────────
    try:
        _feed_brain_from_settled(resolved)
    except Exception as _brain_e:
        print(f"[BRAIN] debrief feed failed: {_brain_e}")

    _run_db_backup()


def _feed_brain_from_settled(resolved: list):
    """
    After debrief: log each settled bet into bet_memory, update SP/team memory,
    update situation memory, and trigger weight recalibration every 20 new bets.
    """
    from memory_engine import init_brain_tables
    init_brain_tables()

    new_count = 0
    for b in resolved:
        bid    = b.get("id")
        result = b.get("result")  # "W" or "L"
        if result not in ("W", "L") or not bid:
            continue

        team       = (b.get("bet") or "").upper()
        game       = b.get("game") or ""
        # Derive opponent from game string "AWAY @ HOME" or "HOME vs AWAY"
        opponent = ""
        if "@" in game:
            parts = game.split("@")
            opponent = (parts[0].strip() if team in (parts[1].strip()[:3].upper()) else parts[1].strip())[:3].upper()
        situations = b.get("situations_triggered") or ""

        update_bet_memory(
            bet_id=bid,
            result=result,
            team=team,
            opponent=opponent,
            edge_pct=b.get("edge_pct"),
            model_prob=b.get("model_prob"),
            market_prob=b.get("market_prob"),
            confidence=b.get("conviction") or "",
            situations=situations,
            stake=float(b.get("stake") or 0),
            odds=str(b.get("bet_odds") or ""),
            date_str=b.get("date") or date.today().isoformat(),
        )
        new_count += 1

        # Situation memory
        if situations:
            update_situation_memory(situations, result == "W")

        # Team memory (uses model_prob as projected win prob)
        mp = b.get("model_prob")
        if mp and team:
            update_team_memory(
                team_code=team,
                projected_win_prob=float(mp),
                won=(result == "W"),
                projected_runs=0.0,   # runs data not stored per-bet
                actual_runs=0.0,
            )

    # Trigger weight recalibration every 20 new bets logged
    if new_count > 0:
        try:
            import sqlite3
            with __import__("memory_engine")._conn() as conn:
                total_row = conn.execute(
                    "SELECT COUNT(*) FROM bet_memory WHERE result IN ('W','L')"
                ).fetchone()
                total_n = total_row[0] if total_row else 0
            if total_n > 0 and total_n % 20 < new_count:
                recalibrate_weights()
                print(f"[BRAIN] Weight recalibration triggered at {total_n} settled bets")
        except Exception as _re:
            print(f"[BRAIN] Recalibrate check failed: {_re}")

    print(f"[BRAIN] Fed {new_count} settled bets into brain memory")


def _run_db_backup():
    try:
        backup_path = _db.backup_database()
        if backup_path:
            print(f"DB backup: {backup_path}")
    except Exception as e:
        print(f"DB backup warning: {e}")


# ── WEEKLY ROI REPORT ─────────────────────────────────────────────────────────

def _run_weekly_roi():
    """Compute all-time ROI and send weekly Telegram report."""
    from math_engine import clv_stats_summary
    from bankroll_engine import peak_bankroll as _peak_br

    bets     = _db.get_bets()
    resolved = [b for b in bets if b.get("result") in ("W", "L", "P")]
    wins     = sum(1 for b in resolved if b["result"] == "W")
    losses   = sum(1 for b in resolved if b["result"] == "L")
    total    = wins + losses

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
    roi     = round(pnl / staked * 100, 1) if staked > 0 else 0.0
    wr      = round(wins / total * 100, 1) if total else 0.0
    br      = current_bankroll()
    peak    = _peak_br()
    sign    = "+" if pnl >= 0 else ""

    clv_data = []
    try:
        with open("clv_log.json") as f:
            clv_data = json.load(f)
    except Exception:
        pass
    clv_stats = clv_stats_summary(clv_data) if clv_data else {}

    lines = [
        f"📊 WEEKLY ROI REPORT — {date.today().strftime('%b %d, %Y')}",
        f"All-time: {wins}W-{losses}L ({wr:.1f}%)",
        f"P&L: {sign}${pnl:.2f} | ROI: {sign}{roi:.1f}%",
        f"Bankroll: ${br:.2f} (peak: ${peak:.2f})",
    ]
    if clv_stats.get("total", 0) > 0:
        lines.append(
            f"Avg CLV: {clv_stats.get('avg_clv', 0):+.2f}% "
            f"({clv_stats.get('total', 0)} bets tracked)"
        )
    lines.append("")
    by_type = clv_stats.get("by_type") or {}
    if by_type:
        lines.append("BY TYPE:")
        for btype, ts in sorted(by_type.items(), key=lambda x: -x[1].get("avg_clv", 0)):
            if ts.get("count", 0) < 2:
                continue
            lines.append(
                f"  {btype:<12} {ts['wins']}W-{ts['losses']}L  "
                f"CLV:{ts['avg_clv']:+.1f}%  WR:{ts['win_rate']:.0f}%"
            )
    verdict = clv_stats.get("verdict", "")
    if verdict:
        lines += ["", verdict]
    sample  = clv_stats.get("sample_size", "")
    if sample:
        lines.append(sample)

    msg = "\n".join(lines)
    print(msg)
    _send_telegram(msg)


# ── LINE MOVEMENT RE-SCOUT ────────────────────────────────────────────────────

def _run_linecheck():
    """Compare 5:30pm lines to 1pm scout lines. Alert on 3+ point moves."""
    print("Running line movement check (5:30pm ET)...")

    try:
        with open("last_scout.json") as f:
            prev_scout = json.load(f)
    except Exception:
        print("No last_scout.json — cannot check line movement")
        return

    saved_bets = prev_scout.get("bets", [])
    if not saved_bets:
        print("No bets in last_scout.json — nothing to compare")
        return

    events = get_mlb_events()
    if not events:
        print("No events from Odds API — skipping linecheck")
        return

    now_et  = datetime.now(ET).strftime("%I:%M %p ET")
    today   = date.today().isoformat()
    alerts  = []

    # Build current odds map: team_code → {odds, game_label}
    curr_map: dict = {}
    for event in events:
        away_name = event.get("away", "")
        home_name = event.get("home", "")
        away_code = MLB_TEAM_MAP.get(away_name, away_name[:3].upper())
        home_code = MLB_TEAM_MAP.get(home_name, home_name[:3].upper())
        try:
            market = full_market_snapshot(
                event["id"], away_name, home_name,
                away_code, home_code, today,
                commence_utc=event.get("commence_utc", ""),
            )
        except Exception:
            continue
        label = f"{away_name} @ {home_name}"
        if market.get("best_away_odds"):
            curr_map[away_code] = {
                "odds": market["best_away_odds"],
                "game": label,
                "side": "away",
                "reverse_line": market.get("reverse_line") or {},
                "public_bias":  market.get("public_bias") or {},
            }
        if market.get("best_home_odds"):
            curr_map[home_code] = {
                "odds": market["best_home_odds"],
                "game": label,
                "side": "home",
                "reverse_line": market.get("reverse_line") or {},
                "public_bias":  market.get("public_bias") or {},
            }

    # Compare each saved bet to current odds
    for saved in saved_bets:
        team       = saved.get("team", "")
        saved_odds = saved.get("odds")
        if not team or saved_odds is None:
            continue
        curr = curr_map.get(team)
        if not curr:
            continue
        curr_odds = curr.get("odds")
        if curr_odds is None:
            continue

        try:
            saved_int = int(str(saved_odds).replace("+", ""))
            curr_int  = int(str(curr_odds).replace("+", ""))
            moved     = curr_int - saved_int
        except ValueError:
            continue

        if abs(moved) < 3:
            continue

        # Direction: negative moved = team got more favored (good if we bet them)
        direction  = "toward" if moved < 0 else "away from"
        sharp_rlm  = curr.get("reverse_line", {}).get("sharp_side") is not None
        sharp_flag = "YES" if sharp_rlm else "NO"

        saved_disp = f"+{saved_odds}" if isinstance(saved_odds, int) and saved_odds > 0 else str(saved_odds)
        curr_disp  = f"+{curr_odds}" if isinstance(curr_odds, int) and curr_odds > 0 else str(curr_odds)

        if direction == "toward" and abs(moved) >= 5:
            action = "INCREASE — line moved our way, value improved"
        elif direction == "away from" and abs(moved) >= 5:
            action = "PASS — line moved against us, edge may be gone"
        else:
            action = "HOLD — minor move, stick with original"

        alerts.append(
            f"{curr['game']}: {team} moved {saved_disp} → {curr_disp} ({moved:+d} pts)\n"
            f"Direction: {direction} our pick\n"
            f"Sharp money indicator: {sharp_flag}\n"
            f"Action: {action}"
        )

    if not alerts:
        print(f"Line movement check complete — no significant moves (≥3 pts) found")
        return

    header = f"📈 LINE MOVEMENT ALERT — {now_et}"
    body   = "\n\n".join(alerts)
    msg    = f"{header}\n\n{body}"
    print(msg)
    _send_telegram(msg)


# ── MORNING PLANNER ───────────────────────────────────────────────────────────

def _run_morning_planner():
    """
    Morning brief: only sends if games exist today AND at least 1 game
    shows potential edge >4% based on memory priors vs current market.
    Silence if no edge games found.
    """
    print("Running morning planner (9am ET)...")
    events = get_mlb_events()
    if not events:
        print("No games today — morning planner silent")
        return

    today       = date.today()
    today_str   = today.isoformat()
    today_label = today.strftime("%b %d, %Y")

    from memory_engine import team_prior as _team_prior

    watch_games: list[str] = []
    edge_found = False
    curr_odds_map: dict = {}   # team_code → current best odds (int)

    for event in events[:12]:
        away_name = event.get("away", "")
        home_name = event.get("home", "")
        away_code = MLB_TEAM_MAP.get(away_name, away_name[:3].upper())
        home_code = MLB_TEAM_MAP.get(home_name, home_name[:3].upper())

        try:
            market = full_market_snapshot(
                event["id"], away_name, home_name,
                away_code, home_code, today_str,
                commence_utc=event.get("commence_utc", ""),
            )
        except Exception:
            continue

        nv      = market.get("no_vig") or {}
        away_nv = nv.get("away") or 0.5
        home_nv = nv.get("home") or 0.5
        if away_nv == 0.5 and home_nv == 0.5:
            continue

        # Store current best odds for line-movement comparison
        if market.get("best_away_odds"):
            curr_odds_map[away_code] = market["best_away_odds"]
        if market.get("best_home_odds"):
            curr_odds_map[home_code] = market["best_home_odds"]

        # Use memory prior (7-day) as quick model proxy
        away_prior = _team_prior(away_code, "away", 7) or 0.5
        home_prior = _team_prior(home_code, "home", 7) or 0.5

        away_edge = round((away_prior - away_nv) * 100, 1)
        home_edge = round((home_prior - home_nv) * 100, 1)

        game_time = _parse_game_time_et(event.get("commence_utc", ""))
        time_tag  = f" {game_time}" if game_time else ""
        best_edge = max(abs(away_edge), abs(home_edge))

        if best_edge > 4.0:
            edge_found = True
            if abs(away_edge) >= abs(home_edge):
                watch_games.insert(0, f"{away_name} @ {home_name}{time_tag} — {away_code} prior edge {away_edge:+.1f}%")
            else:
                watch_games.insert(0, f"{away_name} @ {home_name}{time_tag} — {home_code} prior edge {home_edge:+.1f}%")
        else:
            watch_games.append(f"{away_name} @ {home_name}{time_tag}")

    if not edge_found:
        print("No edge games (>4%) found via memory priors — morning planner silent")
        return

    # ── Line movement since yesterday ─────────────────────────────────────────
    line_moves: list[str] = []
    try:
        with open("last_scout.json") as f:
            prev = json.load(f)
        prev_date = prev.get("date", "")
        if prev_date and prev_date != today_str:
            for saved_bet in prev.get("bets", []):
                team      = saved_bet.get("team", "")
                saved_ml  = saved_bet.get("odds")
                team_code = MLB_TEAM_MAP.get(team, team[:3].upper() if len(team) >= 3 else team)
                curr_ml   = curr_odds_map.get(team_code)
                if saved_ml is None or curr_ml is None:
                    continue
                try:
                    moved = int(str(curr_ml).replace("+", "")) - int(str(saved_ml).replace("+", ""))
                except ValueError:
                    continue
                if abs(moved) >= 5:
                    saved_disp = f"+{saved_ml}" if isinstance(saved_ml, int) and saved_ml > 0 else str(saved_ml)
                    curr_disp  = f"+{curr_ml}" if isinstance(curr_ml, int) and curr_ml > 0 else str(curr_ml)
                    direction  = "improved" if moved < 0 else "moved against us"
                    line_moves.append(f"{team_code} {saved_disp}→{curr_disp} ({direction})")
    except Exception:
        pass

    # Brain insights section
    brain_section = ""
    try:
        brain_section = get_brain_summary()
    except Exception:
        pass

    lines = [
        f"🌅 PARLAY OS — MORNING BRIEF — {today_label}",
        f"{len(events)} games today | Scouts: 11am ET (day), 4pm ET (evening), 6:30pm ET (west coast)",
        "Watch:",
    ]
    for g in watch_games[:3]:
        lines.append(f"• {g}")

    if line_moves:
        lines.append(f"Line movement since yesterday: {' | '.join(line_moves)}")
    else:
        lines.append("Line movement since yesterday: none significant yet")

    if brain_section:
        lines.append("")
        lines.append(brain_section)

    msg = "\n".join(lines)
    print(msg)
    _send_telegram(msg)


# ── ENTRY POINT ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        from telegram_handler import (
            start_listener, start_auto_settler, sync_scout_json,
            start_hedge_monitor,
        )
    except Exception as _te:
        print(f"[WARN] telegram_handler import failed: {_te}")
        def start_listener(): pass
        def start_auto_settler(): pass
        def sync_scout_json(): pass
        def start_hedge_monitor(): pass

    args = set(sys.argv[1:])

    # Parse --window flag (applies only to scout mode; ignored by other modes)
    _window_val = "all"
    _argv_list  = sys.argv[1:]
    for _i, _av in enumerate(_argv_list):
        if _av.startswith("--window="):
            _window_val = _av.split("=", 1)[1]
        elif _av == "--window" and _i + 1 < len(_argv_list):
            _window_val = _argv_list[_i + 1]
    if _window_val not in ("all", "day", "evening", "west"):
        print(f"[WARN] Unknown --window='{_window_val}' — defaulting to 'all'")
        _window_val = "all"

    if "--bot" in args:
        # Persistent bot mode: Telegram listener + auto-settler + hedge monitor + SP monitor
        try:
            import threading as _threading
            from telegram_handler import _poll_loop
            from sp_monitor import SPMonitor
            start_auto_settler()
            start_hedge_monitor()
            _sp_mon = SPMonitor(send_fn=_send_telegram)
            _threading.Thread(target=_sp_mon.run, name="sp-monitor", daemon=True).start()
            print("Parlay OS bot running (Ctrl-C to stop)...")
            try:
                _poll_loop()
            except Exception as e:
                error_logger.log_error("brain.__bot", e)
                print(f"[BOT] poll loop ended: {e}")
        except Exception as e:
            error_logger.log_error("brain.__bot_init", e)
            print(f"[BOT] startup failed: {e}")
        sys.exit(0)  # hard exit — never fall through to any other branch

    elif "--live" in args:
        start_listener()
        start_auto_settler()
        start_hedge_monitor()
        from live_engine import run_live_monitor
        run_live_monitor()

    elif "--debrief" in args:
        _run_debrief()

    elif "--weekly" in args:
        _run_weekly_roi()

    elif "--linecheck" in args:
        _run_linecheck()

    elif "--planner" in args:
        _run_morning_planner()

    else:
        # Scout-only mode: no listener, no polling — just run the scout and send via direct HTTP
        try:
            _wl = _WINDOW_LABELS.get(_window_val, "ALL GAMES")
            print(f"Running daily scout [{_wl}] (scout-only, no Telegram listener)...")
            run_daily_scout(window=_window_val)

            print("Scout complete — exiting")
        except KeyboardInterrupt:
            print("Scout interrupted by user")
        except Exception as e:
            error_logger.log_error("brain.run_daily_scout", e)
            print(f"[FATAL] Scout crashed: {e}")
            traceback.print_exc()
            sys.exit(1)
