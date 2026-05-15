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
from bankroll_engine import kelly_stake, sizing_summary, current_bankroll, is_drawdown_pause
from props_engine   import (
    k_prop, nrfi_prob, team_run_expectancy, game_total_prob,
    f5_run_expectancy, correlated_parlay, scan_k_prop,
    build_sgp_suggestions,
)
from bet_type_validator import (
    validate_bet, BetValidationError, day_classification,
    MAX_LOCKS_PER_DAY, MAX_FLIPS_PER_DAY, DAILY_RISK_CAP_PCT,
)
from intelligence_engine import (
    sp_regression_flags, offense_regression_flags, bullpen_regression_flags,
    get_injury_flags, format_injury_section,
    weighted_momentum,
    format_sharp_pick, format_discord_pick,
)
from memory_engine  import (
    init_memory_tables, recalibrate_model_prob, adjust_model_prob,
    memory_report
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

STATSAPI  = "https://statsapi.mlb.com/api/v1"
ET        = pytz.timezone("America/New_York")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

DRY_RUN   = "--test" in sys.argv

# Minimum edge to recommend
MIN_EDGE_PCT = 3.0
# Minimum Pythagorean probability to include in output
MIN_PROB     = 0.52


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
    away_xr = team_run_expectancy(
        away_off["run_factor"],
        home_sp.get("run_factor", 1.0),
        park_rf, wx_rf, away_bp_rf
    )
    home_xr = team_run_expectancy(
        home_off["run_factor"],
        away_sp.get("run_factor", 1.0),
        park_rf, wx_rf, home_bp_rf
    )

    # ── Pythagorean base probability ─────────────────────────────────────────
    from constants import HOME_ADV
    adj_home_xr  = home_xr * HOME_ADV
    pyth_away_p  = pythagorean_prob(away_xr, adj_home_xr)
    pyth_home_p  = round(1.0 - pyth_away_p, 4)

    # ── ML ensemble blend (if models trained, else use Pythagorean only) ─────
    if _ML_AVAILABLE and models_available():
        try:
            feat_vec = build_game_features(
                away_sp, home_sp, away_off, home_off,
                away_xr, home_xr, weather, park_rf,
            )
            ml_pred      = predict_game(feat_vec)
            ml_home_p    = ml_pred.get("home_win_prob", pyth_home_p)
            ml_away_p    = ml_pred.get("away_win_prob", pyth_away_p)
            shap_home    = ml_pred.get("shap_home", [])
            shap_away    = ml_pred.get("shap_away", [])
            ml_total     = ml_pred.get("total_runs_pred")
            ml_nrfi_p    = ml_pred.get("nrfi_prob")
            ml_conf      = ml_pred.get("confidence", "low")
            # Blend: 60% ML + 40% Pythagorean (conservative until model matures)
            away_model_p = round(0.60 * ml_away_p + 0.40 * pyth_away_p, 4)
            home_model_p = round(0.60 * ml_home_p + 0.40 * pyth_home_p, 4)
            print(f"  ML: home={ml_home_p:.3f} pyth={pyth_home_p:.3f} "
                  f"blend={home_model_p:.3f} conf={ml_conf}")
        except Exception as e:
            print(f"  ML predict failed ({e}), using Pythagorean")
            away_model_p = pyth_away_p
            home_model_p = pyth_home_p
            shap_home = shap_away = []
            ml_total = ml_nrfi_p = None
            ml_conf  = "fallback"
    else:
        away_model_p = pyth_away_p
        home_model_p = pyth_home_p
        shap_home = shap_away = []
        ml_total = ml_nrfi_p = None
        ml_conf  = "pythagorean"

    # Memory calibration
    away_model_p = recalibrate_model_prob(away_model_p)
    home_model_p = recalibrate_model_prob(home_model_p)

    # ── SP unknown → reduce confidence by 15% (pull toward 0.5) ─────────────
    # A missing SP means we don't know the run-prevention side of the ledger.
    # Rather than blocking the bet entirely, we shrink the edge conservatively.
    _SP_TBD_REDUCTION = 0.15
    away_sp_tbd = not away_sp.get("name") or away_sp.get("name") == "TBD"
    home_sp_tbd = not home_sp.get("name") or home_sp.get("name") == "TBD"
    if away_sp_tbd or home_sp_tbd:
        away_model_p = round(away_model_p - _SP_TBD_REDUCTION * (away_model_p - 0.5), 4)
        home_model_p = round(home_model_p - _SP_TBD_REDUCTION * (home_model_p - 0.5), 4)

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

    # Enhanced momentum
    away_momentum = weighted_momentum(away_tid, away_code)
    home_momentum = weighted_momentum(home_tid, home_code)
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
    print(f"  SP last-3 ERA: {away_sp_name}={away_r3_str}{' ['+','.join(away_sp_flags)+']' if away_sp_flags else ''}  "
          f"{home_sp_name}={home_r3_str}{' ['+','.join(home_sp_flags)+']' if home_sp_flags else ''}")
    if away_sc_str or home_sc_str:
        print(f"  Statcast: {away_sp_name}: {away_sc_str or 'N/A'}  |  {home_sp_name}: {home_sc_str or 'N/A'}")
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
    total_r = game_total_prob(away_xr, home_xr,
                               market.get("totals", {}).get("line", 8.5) if market.get("totals") else 8.5)
    f5_away_xr = f5_run_expectancy(away_xr, away_sp)
    f5_home_xr = f5_run_expectancy(home_xr, home_sp)

    # K props (if odds available — placeholder, real odds via props market endpoint)
    away_k_prop = k_prop(away_sp, away_sp.get("k9", 8.5) * 5 / 9,
                          ump_k_factor=ump_k) if away_sp else None
    home_k_prop = k_prop(home_sp, home_sp.get("k9", 8.5) * 5 / 9,
                          ump_k_factor=ump_k) if home_sp else None

    # ── Conviction ────────────────────────────────────────────────────────────
    away_conv = _conviction(away_edge, away_model_p, away_bp, market)
    home_conv = _conviction(home_edge, home_model_p, home_bp, market)

    # ── Sizing ────────────────────────────────────────────────────────────────
    away_stake = kelly_stake(away_model_p, str(best_away_odds), away_conv) if best_away_odds else 0.0
    home_stake = kelly_stake(home_model_p, str(best_home_odds), home_conv) if best_home_odds else 0.0

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
        "totals_line":   market.get("totals", {}).get("line") if market.get("totals") else None,
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


def _conviction(edge_pct: float, model_p: float, bp: dict, market: dict) -> str:
    if edge_pct < MIN_EDGE_PCT:
        return "PASS"

    tier = "LOW"
    if edge_pct >= 7 and bp.get("fatigue_tier") in ("FRESH", "MODERATE"):
        tier = "HIGH"
    elif edge_pct >= 4:
        tier = "MEDIUM"
    return tier


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
) -> None:
    """Format and send the complete daily bet slip to Telegram."""
    today = date.today().strftime("%b %d, %Y")

    locks = all_locks[:MAX_LOCKS_PER_DAY]
    flips = all_flips[:MAX_FLIPS_PER_DAY]

    n_locks  = len(locks)
    day_cls  = day_classification(n_locks)
    s_mult   = day_cls["stake_mult"]

    # Calculate today's total risk and to-win
    total_risk = 0.0
    total_win  = 0.0
    for analysis, side in locks + flips:
        stake = round((analysis.get(f"{side}_stake") or 0) * s_mult, 2)
        odds  = analysis.get(f"best_{side}_odds")
        if odds and stake > 0:
            dec = american_to_decimal(str(odds))
            if dec:
                total_risk += stake
                total_win  += round((dec - 1) * stake, 2)

    # ── Header ────────────────────────────────────────────────────────────────
    lines = [
        f"PARLAY OS — {today} — {day_cls['color']} {day_cls['emoji']}",
        f"Bankroll: ${br:.2f} | Risk today: ${total_risk:.2f} | To win: ${total_win:.2f}",
        "",
    ]

    # ── Locks ─────────────────────────────────────────────────────────────────
    lines.append(f"🔒 LOCKS ({n_locks} found — HIGH conviction 7%+ edge):")
    if locks:
        for analysis, side in locks:
            stake  = round((analysis.get(f"{side}_stake") or 0) * s_mult, 2)
            odds   = analysis.get(f"best_{side}_odds", "")
            edge   = analysis.get(f"{side}_edge", 0)
            team   = analysis.get(f"{side}_name", "")
            game   = f"{analysis.get('away_name','')} @ {analysis.get('home_name','')}"
            odds_s = (f"+{odds}" if isinstance(odds, int) and odds > 0 else str(odds or ""))
            lines.append(f"  {game} — {team} ML {odds_s} — ${stake:.2f} — EDGE: +{edge:.1f}%")
    else:
        lines.append("  None today")
    lines.append("")

    # ── Coin flips ────────────────────────────────────────────────────────────
    lines.append(f"🪙 COIN FLIPS ({len(flips)} found — MEDIUM conviction 4-7% edge):")
    if flips and day_cls["ml_allowed"]:
        for analysis, side in flips:
            stake  = round((analysis.get(f"{side}_stake") or 0) * s_mult, 2)
            odds   = analysis.get(f"best_{side}_odds", "")
            edge   = analysis.get(f"{side}_edge", 0)
            team   = analysis.get(f"{side}_name", "")
            game   = f"{analysis.get('away_name','')} @ {analysis.get('home_name','')}"
            odds_s = (f"+{odds}" if isinstance(odds, int) and odds > 0 else str(odds or ""))
            lines.append(f"  {game} — {team} ML {odds_s} — ${stake:.2f} — EDGE: +{edge:.1f}%")
    elif not day_cls["ml_allowed"]:
        lines.append("  🔴 RED day — no ML bets")
    else:
        lines.append("  None today")
    lines.append("")

    # ── Parlay (locks only, 2-3 legs) ─────────────────────────────────────────
    parlay_candidates = [
        (a, s) for a, s in locks
        if a.get(f"best_{s}_odds") is not None
    ][:3]

    if len(parlay_candidates) >= 2 and day_cls["ml_allowed"]:
        odds_strs = [str(a.get(f"best_{s}_odds", "")) for a, s in parlay_candidates]
        prl = parlay_odds(odds_strs)
        if prl.get("valid"):
            prl_stake = round(min(br * 0.02, 10.0) * s_mult, 2)
            prl_win   = round((prl["decimal"] - 1) * prl_stake, 2)
            leg_parts = []
            for a, s in parlay_candidates:
                t     = a.get(f"{s}_name", "")
                o     = a.get(f"best_{s}_odds", "")
                o_str = (f"+{o}" if isinstance(o, int) and o > 0 else str(o or ""))
                leg_parts.append(f"{t} ML {o_str}")
            lines.append("PARLAY (locks only):")
            lines.append(f"  {' + '.join(leg_parts)}")
            lines.append(f"  ({prl['american']}) — ${prl_stake:.2f} — to win ${prl_win:.2f}")
            lines.append("")
            total_risk += prl_stake
            total_win  += prl_win

    # ── Props slate ───────────────────────────────────────────────────────────
    if all_props:
        lines.append("PROPS SLATE:")
        for prop in all_props[:2]:
            legs_str = " + ".join(str(l) for l in prop.get("legs", [])[:2])
            stake    = prop.get("kelly_stake", 0) or 0
            ev       = prop.get("ev", 0) or 0
            ptype    = prop.get("type", "SGP")
            lines.append(f"  [{ptype}] {legs_str} — ${stake:.2f} — EV: {ev:+.4f}")
        lines.append("")

    # ── Fades ─────────────────────────────────────────────────────────────────
    if all_fades:
        lines.append("❌ FADES:")
        seen_fades: set = set()
        count = 0
        for analysis, side, reason in all_fades:
            team = analysis.get(f"{side}_name", "")
            if team in seen_fades or count >= 4:
                continue
            seen_fades.add(team)
            lines.append(f"  {team} — {reason}")
            count += 1
        lines.append("")

    # ── Day classification note ───────────────────────────────────────────────
    if day_cls["color"] == "YELLOW":
        lines.append("⚠ YELLOW day — fewer than 2 locks, stakes reduced 20%")
    elif day_cls["color"] == "RED":
        lines.append("🔴 RED day — no locks found, props only, no ML bets")

    risk_cap_pct = round(total_risk / br * 100, 1) if br > 0 else 0
    lines.append(
        f"Daily risk: {risk_cap_pct:.1f}% of bankroll "
        f"(cap: {DAILY_RISK_CAP_PCT * 100:.0f}%)"
    )

    _send_telegram("\n".join(lines))


# ── BET RECOMMENDATION FILTER ─────────────────────────────────────────────────

def _should_recommend(game: dict, side: str) -> bool:
    """Brain's final sign-off: is this bet worth sending?"""
    edge  = game.get(f"{side}_edge", 0)
    conv  = game.get(f"{side}_conv", "PASS")
    stake = game.get(f"{side}_stake", 0)
    model = game.get(f"{side}_model_p", 0)
    nv    = game.get(f"{side}_nv", 0)
    team  = game.get(f"{side}_name", side)

    if conv == "PASS" or edge < MIN_EDGE_PCT:
        print(f"  PASS {team}: edge {edge:+.1f}% (need >{MIN_EDGE_PCT}%) model={model:.3f} nv={nv:.3f}")
        return False
    if stake <= 0:
        print(f"  PASS {team}: stake=0 (daily cap hit or drawdown pause)")
        return False
    if model < MIN_PROB:
        print(f"  PASS {team}: model {model:.3f} < min {MIN_PROB}")
        return False
    if is_drawdown_pause():
        print(f"  PASS {team}: drawdown pause active")
        return False

    print(f"  BET  {team}: edge {edge:+.1f}% model={model:.3f} nv={nv:.3f} stake=${stake:.2f} [{conv}]")
    return True


# ── TELEGRAM FORMAT ───────────────────────────────────────────────────────────

def _format_bet_message(game: dict, side: str) -> str:
    team    = game.get(f"{side}_name", game.get(side, ""))
    opp_s   = "home" if side == "away" else "away"
    opp     = game.get(f"{opp_s}_name", game.get(opp_s, ""))
    odds    = game.get(f"best_{side}_odds")
    book    = game.get(f"best_{side}_book", "")
    edge    = game.get(f"{side}_edge", 0)
    model_p = game.get(f"{side}_model_p", 0)
    stake   = game.get(f"{side}_stake", 0)
    conv    = game.get(f"{side}_conv", "")
    sp      = game.get(f"{side}_sp", {})
    opp_sp  = game.get(f"{opp_s}_sp", {})
    wx      = game.get("weather", {})

    sp_str  = f"{sp.get('name','TBD')} ({sp.get('era','?')} ERA)"
    osp_str = f"{opp_sp.get('name','TBD')} ({opp_sp.get('era','?')} ERA)"
    sides_str = f"{game.get('away_name','')} @ {game.get('home_name','')}"

    lines = [
        f"BET | {conv}",
        f"<b>{team} ML</b>",
        f"{sides_str}",
        f"Odds: {odds:+d} @ {book.upper()}  |  Edge: +{edge:.1f}%",
        f"Model: {model_p:.1%}  |  Stake: ${stake:.2f}",
        f"Our SP: {sp_str}",
        f"Opp SP: {osp_str}",
        f"Park: {game.get('home','')}  |  Ump: {game.get('umpire','?')} {game.get('ump_note','')}",
        f"Wx: {wx.get('note','?')} | rf={wx.get('run_factor',1):.3f}",
    ]

    # Append props
    nrfi = game.get("nrfi", {})
    if nrfi.get("note") in ("nrfi", "yrfi"):
        lines.append(f"NRFI: {nrfi['p_nrfi']:.1%} | YRFI: {nrfi['p_yrfi']:.1%} → {nrfi['note'].upper()}")

    total = game.get("total", {})
    if total and total.get("note") != "neutral":
        lines.append(f"Total {total.get('line','?')}: O={total['p_over']:.1%} U={total['p_under']:.1%} → {total['note'].upper()}")

    poly = game.get("polymarket")
    if poly:
        p_str = " | ".join(f"{k.upper()} {v:.1%}" for k, v in poly.items())
        lines.append(f"Poly: {p_str}")

    lm = game.get("line_movement") or {}
    if lm.get("direction") not in ("unknown", "stable", None):
        lines.append(f"Line: {lm['direction']} Δ{lm['magnitude']:.3f}")

    # SHAP explanation
    shap_key  = f"shap_{side}"
    shap_data = game.get(shap_key, [])
    if shap_data:
        parts = []
        for s in shap_data[:3]:
            feat = s.get("feature", "").replace("_", " ")
            val  = s.get("shap_val", 0)
            parts.append(f"{feat} {'+' if val > 0 else ''}{val*100:.1f}%")
        lines.append(f"Why: {' | '.join(parts)}")

    # Regression / intelligence flags
    reg_flags = game.get("reg_flags", [])
    for flag in reg_flags[:2]:
        lines.append(f"⚠ {flag.get('message', '')}")

    # SP intelligence flags (ERA vs xFIP, velocity, control)
    intel_flags = game.get("intel_flags", [])
    for flag in intel_flags[:3]:
        emoji = flag.get("emoji", "⚠️")
        lines.append(f"{emoji} {flag.get('message', '')}")

    # Injury flags
    injury_flags = game.get("injury_flags", [])
    for inj in injury_flags[:2]:
        lines.append(f"{inj.get('emoji','🚑')} {inj['message']}")

    # Momentum
    away_mom = game.get("away_momentum") or {}
    home_mom = game.get("home_momentum") or {}
    if away_mom.get("summary") or home_mom.get("summary"):
        for mom in (away_mom, home_mom):
            s = mom.get("summary", "")
            if s:
                lines.append(s)

    ml_model = game.get("ml_model", "")
    if ml_model and ml_model not in ("pythagorean", "pythagorean_fallback"):
        lines.append(f"Model: {ml_model}")

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


def _send_telegram(msg: str):
    if DRY_RUN:
        print(msg)
        print("---")
        return
    if not BOT_TOKEN or not CHAT_ID:
        print(msg)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=8,
        )
    except Exception as e:
        print(f"Telegram error: {e}")


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


# ── DAILY SCOUT ───────────────────────────────────────────────────────────────

def run_daily_scout():
    """Full daily analysis: all games → recommendations → Telegram."""
    print("=" * 60)
    print("Brain starting — daily scout")
    init_memory_tables()

    today     = date.today().isoformat()
    print(f"Date: {today} — fetching events from Odds API...")
    events    = get_mlb_events()
    print(f"Games from Odds API: {len(events)}")
    if not events:
        print("WARNING: 0 games returned — check ODDS_API_KEY env var and API quota")

    br        = current_bankroll()
    mem       = memory_report()
    print(f"Bankroll: ${br:.2f} | Memory cal ready: {mem['ready_to_recalibrate']}")

    all_bets  = []
    all_pass  = []
    # Daily slip collections
    all_locks:    list = []   # (analysis, side) — HIGH conviction, edge ≥ 7%
    all_flips:    list = []   # (analysis, side) — MEDIUM conviction, edge 4-7%
    all_fades:    list = []   # (analysis, side, reason)
    all_sgp:      list = []   # correlated SGP suggestions across all games
    game_key_map: dict = {}   # (away_code, home_code) → analysis

    scout_out = {
        "timestamp": datetime.now(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "date":      today,
        "bankroll":  br,
        "games":     [],
        "bets":      [],
        "passes":    [],
    }

    for event in events:
        away_n = event.get("away", "?")
        home_n = event.get("home", "?")
        print(f"\n--- Analyzing: {away_n} @ {home_n} ---")
        try:
            analysis = analyze_game(event, today)
        except Exception as e:
            print(f"  ERROR in analyze_game: {e}")
            traceback.print_exc()
            continue

        if analysis is None:
            print(f"  SKIP {away_n} @ {home_n}: no market data or unrecognised team code")
            continue

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

        # ── Injury flag alert ─────────────────────────────────────────────────
        if analysis.get("injury_flags"):
            game_label = f"{analysis.get('away_name','')} @ {analysis.get('home_name','')}"
            inj_msg = format_injury_section(analysis["injury_flags"], game_label)
            if inj_msg:
                _send_telegram(inj_msg)

        # ── SGP suggestions for this game ─────────────────────────────────────
        try:
            sgp_market = {
                "totals": {"line": analysis.get("totals_line", 8.5)},
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
                game_label = f"{analysis.get('away_name','')} @ {analysis.get('home_name','')}"
                for sgp in sgp_suggestions:
                    _send_telegram(_format_sgp(sgp, game_label))
                all_sgp.extend(sgp_suggestions)
        except Exception as sgp_err:
            print(f"  SGP error: {sgp_err}")

        bet_found = False
        for side in ("away", "home"):
            if _should_recommend(analysis, side):
                msg = _format_bet_message(analysis, side)
                _send_telegram(msg)
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
                        _db.log_bet(
                            date=today,
                            bet=analysis.get(f"{side}_name", ""),
                            bet_type="ML",
                            game=f"{analysis['away_name']} @ {analysis['home_name']}",
                            sp=analysis.get(f"{side}_sp", {}).get("name", ""),
                            park=analysis["home"],
                            umpire=analysis["umpire"],
                            bet_odds=str(analysis.get(f"best_{side}_odds", "")),
                            model_prob=analysis.get(f"{side}_model_p"),
                            market_prob=analysis.get(f"{side}_nv"),
                            edge_pct=analysis.get(f"{side}_edge"),
                            conviction=analysis.get(f"{side}_conv", ""),
                            stake=float(analysis.get(f"{side}_stake", 0)),
                        )
                    except Exception as e:
                        print(f"DB log error: {e}")

                scout_out["bets"].append({
                    "team":       analysis.get(f"{side}_name"),
                    "side":       side,
                    "odds":       analysis.get(f"best_{side}_odds"),
                    "book":       analysis.get(f"best_{side}_book"),
                    "model_prob": analysis.get(f"{side}_model_p"),
                    "market_prob":analysis.get(f"{side}_nv"),
                    "edge_pct":   analysis.get(f"{side}_edge"),
                    "stake":      analysis.get(f"{side}_stake"),
                    "conviction": analysis.get(f"{side}_conv"),
                })
            else:
                # Collect fades: PASS games with specific warning flags
                flags = (analysis.get("narrative") or []) + (analysis.get("reg_flags") or [])
                if flags:
                    reason = flags[0].get("message", "no edge found")
                    all_fades.append((analysis, side, reason))

        if not bet_found:
            all_pass.append(analysis)
            scout_out["passes"].append({
                "game":  f"{analysis['away_name']} @ {analysis['home_name']}",
                "edges": {"away": analysis["away_edge"], "home": analysis["home_edge"]},
            })

    n_bets = len(all_bets)
    total_risk = sum(
        a.get("away_stake", 0) + a.get("home_stake", 0) for a in all_bets
    )
    print(f"\nScout done — {len(events)} games | {n_bets} bets | ${total_risk:.2f} at risk")

    # ── Series analysis (game 1 of series today) ──────────────────────────────
    print("Running series analysis...")
    try:
        _series_analysis(events, today, game_key_map)
    except Exception as series_err:
        print(f"Series analysis error: {series_err}")

    # ── Daily bet slip ────────────────────────────────────────────────────────
    print("Building daily bet slip...")
    try:
        _daily_bet_slip(all_locks, all_flips, all_sgp, all_fades, br)
    except Exception as slip_err:
        print(f"Daily slip error: {slip_err}")

    # Save scout output
    if not DRY_RUN:
        with open("last_scout.json", "w") as f:
            json.dump(scout_out, f, indent=2)
        _db.log_scout_run(today, n_bets, json.dumps(scout_out))

    return scout_out


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

    if "--bot" in args:
        # Persistent bot mode: Telegram listener + auto-settler + hedge monitor only — never run scout
        try:
            from telegram_handler import _poll_loop
            start_auto_settler()
            start_hedge_monitor()
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
        bets     = _db.get_bets()
        resolved = [b for b in bets if b.get("result") in ("W", "L")]
        pending  = [b for b in bets if not b.get("result")]
        br       = current_bankroll()
        print(f"Debrief: {len(resolved)} resolved, {len(pending)} pending | Bankroll: ${br:.2f}")
        sync_scout_json()
        # Daily DB backup (runs as part of nightly debrief job)
        try:
            from db import backup_database
            backup_path = backup_database()
            if backup_path:
                print(f"DB backup: {backup_path}")
        except Exception as e:
            print(f"DB backup warning: {e}")

    elif "--weekly" in args:
        from math_engine import clv_stats_summary
        bets   = _db.get_bets()
        wins   = sum(1 for b in bets if b.get("result") == "W")
        losses = sum(1 for b in bets if b.get("result") == "L")
        total  = wins + losses
        br     = current_bankroll()
        print(f"Weekly ROI: {wins}-{losses} ({wins/total*100:.1f}%)" if total else "No resolved bets")
        print(f"Bankroll: ${br:.2f}")

    else:
        # Default: start Telegram listener + auto-settler + hedge monitor in background, run scout once, exit
        try:
            print("Starting Telegram listener...")
            start_listener()
            print("Starting auto-settler...")
            start_auto_settler()
            print("Starting hedge monitor...")
            start_hedge_monitor()
            print("Running daily scout (this is the main blocking call)...")
            run_daily_scout()
            print("Scout complete — exiting")
        except KeyboardInterrupt:
            print("Scout interrupted by user")
        except Exception as e:
            error_logger.log_error("brain.run_daily_scout", e)
            print(f"[FATAL] Scout crashed: {e}")
            traceback.print_exc()
            sys.exit(1)
