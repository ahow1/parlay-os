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
    build_sgp_suggestions, prob_over,
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
BOT_TOKEN         = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID           = os.getenv("TELEGRAM_CHAT_ID", "")
PUBLIC_CHANNEL_ID = os.getenv("TELEGRAM_PUBLIC_CHANNEL_ID", "")

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
    away_xfip_str = f"/xFIP {away_sp.get('xfip'):.2f}" if away_sp.get("xfip") is not None else ""
    home_xfip_str = f"/xFIP {home_sp.get('xfip'):.2f}" if home_sp.get("xfip") is not None else ""
    print(f"  SP ERA{'/xFIP' if away_xfip_str else ''}: "
          f"{away_sp_name}={away_r3_str}{away_xfip_str}{' ['+','.join(away_sp_flags)+']' if away_sp_flags else ''}  "
          f"{home_sp_name}={home_r3_str}{home_xfip_str}{' ['+','.join(home_sp_flags)+']' if home_sp_flags else ''}")
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
    if edge_pct >= 7:
        return "HIGH"
    if edge_pct >= 4:
        return "MEDIUM"
    return "LOW"


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
    all_nrfi: list | None = None,
    all_totals: list | None = None,
    all_hitter_props: list | None = None,
    all_k_props: list | None = None,
    all_injuries: list | None = None,
) -> None:
    """Send the daily bet slip as 3 labeled Telegram messages."""
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

    n_locks = len(locks)
    day_cls = day_classification(n_locks)
    s_mult  = day_cls["stake_mult"]

    def _ml_stake(analysis, side):
        return round((analysis.get(f"{side}_stake") or 0) * s_mult, 2)

    # Parlay: HIGH conviction ML picks only — no props or totals
    parlay_candidates = [
        (a, s) for a, s in locks
        if a.get(f"best_{s}_odds") is not None
    ][:3]
    prl_valid = False
    prl_stake = 0.0
    prl_win   = 0.0
    prl_data  = None
    if len(parlay_candidates) >= 2 and day_cls["ml_allowed"]:
        odds_strs = [str(a.get(f"best_{s}_odds", "")) for a, s in parlay_candidates]
        prl = parlay_odds(odds_strs)
        if prl.get("valid"):
            prl_stake = round(min(br * 0.02, 10.0) * s_mult, 2)
            prl_win   = round((prl["decimal"] - 1) * prl_stake, 2)
            prl_valid = True
            prl_data  = prl

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

    props_risk = (
        sum(b.get("stake", 0) for b in nrfi_bets + totals_bets + k_bets)
        + sum(p.get("kelly_stake", 0) or 0 for p in all_props[:2])
        + sum(hp.get("stake", 0) for hp in hitter_bets[:8])
    )
    total_risk = round(ml_risk + props_risk, 2)
    total_win  = round(ml_win, 2)

    cap          = round(br * DAILY_RISK_CAP_PCT, 2)
    cap_exceeded = total_risk > cap
    override_cap = os.getenv("OVERRIDE_RISK_CAP", "").lower() in ("1", "true", "yes")

    # ── PART 1: ML PICKS ──────────────────────────────────────────────────────
    p1 = [
        f"PARLAY OS — {today} — {day_cls['color']} {day_cls['emoji']}",
        "PART 1/3 — ML PICKS",
        f"Bankroll: ${br:.2f} | ML risk: ${ml_risk:.2f} | To win: ${ml_win:.2f}",
        "",
    ]

    p1.append(f"🔒 LOCKS ({n_locks} — HIGH conviction 7%+ edge):")
    if locks:
        for analysis, side in locks:
            stake  = _ml_stake(analysis, side)
            odds   = analysis.get(f"best_{side}_odds", "")
            edge   = analysis.get(f"{side}_edge", 0)
            team   = analysis.get(f"{side}_name", "")
            game   = f"{analysis.get('away_name','')} @ {analysis.get('home_name','')}"
            odds_s = (f"+{odds}" if isinstance(odds, int) and odds > 0 else str(odds or ""))
            p1.append(f"  {game} — {team} ML {odds_s} — ${stake:.2f} — EDGE: +{edge:.1f}%")
    else:
        p1.append("  None today")
    p1.append("")

    p1.append(f"🪙 COIN FLIPS ({len(flips)} — MEDIUM conviction 4-7% edge):")
    if flips and day_cls["ml_allowed"]:
        for analysis, side in flips:
            stake  = _ml_stake(analysis, side)
            odds   = analysis.get(f"best_{side}_odds", "")
            edge   = analysis.get(f"{side}_edge", 0)
            team   = analysis.get(f"{side}_name", "")
            game   = f"{analysis.get('away_name','')} @ {analysis.get('home_name','')}"
            odds_s = (f"+{odds}" if isinstance(odds, int) and odds > 0 else str(odds or ""))
            p1.append(f"  {game} — {team} ML {odds_s} — ${stake:.2f} — EDGE: +{edge:.1f}%")
    elif not day_cls["ml_allowed"]:
        p1.append("  🔴 RED day — no ML bets")
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
        p1.append("PARLAY (HIGH conviction ML only):")
        p1.append(f"  {' + '.join(leg_parts)}")
        p1.append(f"  ({prl_data['american']}) — ${prl_stake:.2f} — to win ${prl_win:.2f}")
        p1.append("")

    if injuries:
        p1.append("⚠️ INJURIES:")
        for inj in injuries:
            p1.append(inj)
        p1.append("")

    if day_cls["color"] == "YELLOW":
        p1.append("⚠ YELLOW day — fewer than 2 locks, stakes reduced 20%")
    elif day_cls["color"] == "RED":
        p1.append("🔴 RED day — no locks found, props only, no ML bets")

    print(f"[SLIP] Sending PART 1/3 ({len(p1)} lines)...")
    _send_telegram("\n".join(p1))

    # ── PART 2: PLAYER PROPS + NRFI/YRFI ─────────────────────────────────────
    has_p2 = bool(nrfi_bets or k_bets or hitter_bets or all_props)
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

    if k_bets:
        p2.append(f"⚾ PITCHER Ks ({len(k_bets)} bets):")
        for bet in k_bets:
            p2.append(
                f"  {bet['sp']} O{bet['line']}K ({bet['game']}) — "
                f"${bet['stake']:.2f} — EDGE: +{bet['edge_pct']:.1f}%"
            )
        p2.append("")

    if hitter_bets:
        shown = hitter_bets[:8]
        p2.append(f"🏏 HITTERS ({len(hitter_bets)} props — top {len(shown)}):")
        for hp in shown:
            p2.append(
                f"  {hp['player']} ({hp['team']}) — {hp['prop']} — "
                f"${hp['stake']:.2f} — EDGE: +{hp['edge_pct']:.1f}%"
            )
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
                f"  {bet['game']} — {bet['direction']} {bet['line']} ({bet['prob']:.1%}) — ${bet['stake']:.2f}"
            )
        p3.append("")
    else:
        p3.append("📊 TOTALS: None today")
        p3.append("")

    if all_fades:
        p3.append("❌ FADES:")
        seen_fades: set = set()
        count = 0
        for analysis, side, reason in all_fades:
            team = analysis.get(f"{side}_name", "")
            if team in seen_fades or count >= 4:
                continue
            seen_fades.add(team)
            p3.append(f"  {team} — {reason}")
            count += 1
        p3.append("")

    risk_cap_pct = round(total_risk / br * 100, 1) if br > 0 else 0
    p3.append(
        f"Daily risk: ${total_risk:.2f} ({risk_cap_pct:.1f}% of bankroll) | "
        f"Cap: {DAILY_RISK_CAP_PCT * 100:.0f}% (${cap:.2f})"
    )

    if cap_exceeded:
        if override_cap:
            p3.append(f"⚠️ RISK CAP OVERRIDE ACTIVE — ${total_risk:.2f} exceeds ${cap:.2f} cap")
        else:
            p3.append(f"🚨 RISK CAP HIT — ${total_risk:.2f} > ${cap:.2f} — bets beyond cap were blocked")
    else:
        p3.append(f"✅ Within daily cap (${cap:.2f})")

    print(f"[SLIP] Sending PART 3/3 ({len(p3)} lines)...")
    _send_telegram("\n".join(p3))
    print("[SLIP] All 3 parts sent.")


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
        if is_drawdown_pause():
            print(f"  PASS {team}: stake=$0.00 — drawdown pause active (bankroll down ≥15% from peak)")
        else:
            print(f"  PASS {team}: stake=$0.00 — Kelly returned zero (edge={edge:+.1f}% model={model:.3f} nv={nv:.3f})")
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

    def _sp_line(s: dict) -> str:
        name = s.get("name", "TBD")
        era  = s.get("era", "?")
        xfip = s.get("xfip")
        xfip_str = f" / xFIP {xfip:.2f}" if xfip is not None else ""
        return f"{name} ({era} ERA{xfip_str})"

    sp_str  = _sp_line(sp)
    osp_str = _sp_line(opp_sp)
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
        print("[TG] DRY_RUN — printing instead of sending:")
        print(msg)
        print("---")
        return
    if not BOT_TOKEN or not CHAT_ID:
        print(f"[TG] WARN: BOT_TOKEN={'set' if BOT_TOKEN else 'MISSING'} CHAT_ID={'set' if CHAT_ID else 'MISSING'} — printing instead:")
        print(msg)
        return
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
    except Exception as e:
        print(f"[TG] ERROR sending: {e}")


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
        model_p  = prob_over(lam, line)
        edge     = round(model_p - market_p, 4)

        if edge < _HITTER_PROP_MIN_EDGE:
            continue

        stake = round(min(br * (0.015 if edge >= 0.08 else 0.01), 5.0), 2)
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
    all_nrfi:         list = []   # {game, direction, prob, stake}
    all_totals:       list = []   # {game, direction, line, prob, stake}
    all_hitter_props: list = []   # {player, team, prop, stake, edge_pct, ...}
    all_k_props:      list = []   # {sp, game, line, p_over, edge_pct, stake}
    all_injuries:     list = []   # injury warning strings — collected, sent in slip
    game_key_map:     dict = {}   # (away_code, home_code) → analysis
    props_games:      list = []   # per-game props data for props_output.json

    # Seed accumulated_risk from today's already-pending bets (stale or earlier scout)
    _prior_pending = [b for b in _db.get_bets()
                      if not b.get("result") and b.get("date") == today]
    accumulated_risk = round(sum(float(b.get("stake") or 0) for b in _prior_pending), 2)
    if accumulated_risk > 0:
        print(f"Prior risk today: ${accumulated_risk:.2f} ({len(_prior_pending)} pending bets from earlier scout)")

    _cap = round(br * DAILY_RISK_CAP_PCT, 2)
    _override_cap = os.getenv("OVERRIDE_RISK_CAP", "").lower() in ("1", "true", "yes")

    if not _override_cap and accumulated_risk >= _cap:
        _warn = (
            f"⚠️ DAILY RISK CAP ALREADY HIT — ${accumulated_risk:.2f} >= ${_cap:.2f}\n"
            f"No new bets will be logged. Set OVERRIDE_RISK_CAP=true to override."
        )
        print(_warn)
        _send_telegram(_warn)

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

        # ── Collect K-props for slip ──────────────────────────────────────────
        _game_lbl_kp = f"{analysis.get('away_name','')} @ {analysis.get('home_name','')}"
        for _sp in (analysis.get("away_sp") or {}, analysis.get("home_sp") or {}):
            if not _sp or not _sp.get("name") or _sp.get("name") == "TBD" or _sp.get("sp_missing"):
                continue
            _k_line = round(_sp.get("k9", 8.5) * 5.0 / 9, 1)
            _k_r    = k_prop(_sp, _k_line)
            _p_k    = _k_r.get("p_over", 0)
            if _p_k >= 0.55:
                _k_stake = round(min(br * (0.02 if _p_k >= 0.60 else 0.01), 5.0), 2)
                all_k_props.append({
                    "sp":       _sp.get("name"),
                    "game":     _game_lbl_kp,
                    "line":     _k_line,
                    "p_over":   _p_k,
                    "edge_pct": round((_p_k - 0.50) * 100, 1),
                    "stake":    _k_stake,
                })

        # ── Collect props data for props_output.json (no per-game Telegram send) ─
        try:
            game_props_entry = _build_props_entry(analysis, sgp_suggestions or [])
            if game_props_entry is not None:
                props_games.append(game_props_entry)
        except Exception as pe:
            print(f"  Props entry error: {pe}")

        bet_found = False
        for side in ("away", "home"):
            if _should_recommend(analysis, side):
                proposed_stake = float(analysis.get(f"{side}_stake", 0))

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
                    edge = model_p - mkt_p
                    if edge >= 0.05 and edge > best_total_edge:
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
                    print(f"  SKIP totals {game_lbl}: no side with ≥5% edge over market")

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
    print(
        f"Building daily bet slip — "
        f"locks={len(all_locks)} flips={len(all_flips)} nrfi={len(all_nrfi)} "
        f"totals={len(all_totals)} k_props={len(all_k_props)} "
        f"hitter_props={len(all_hitter_props)} injuries={len(all_injuries)}"
    )
    try:
        _daily_bet_slip(all_locks, all_flips, all_sgp, all_fades, br,
                        all_nrfi, all_totals, all_hitter_props, all_k_props, all_injuries)
    except Exception as slip_err:
        print(f"Daily slip EXCEPTION: {slip_err}")
        traceback.print_exc()

    # ── Public channel post ───────────────────────────────────────────────────
    if PUBLIC_CHANNEL_ID:
        try:
            _post_public_channel(all_locks, all_flips, today)
        except Exception as pub_err:
            print(f"Public channel error: {pub_err}")

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
            "recommendation": "NRFI" if p_nrfi >= 0.58 else ("YRFI" if p_yrfi >= 0.58 else "PASS"),
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
        if p_nrfi >= 0.58:
            stake     = round(br * 0.015, 2)
            edge_est  = round((p_nrfi - 0.50) * 100, 1)
            odds      = _est_odds(p_nrfi)
            odds_s    = f" {odds}" if odds else ""
            pitcher_lines.append(f"✅ BET: NRFI{odds_s} — ${stake:.2f} — EDGE: +{edge_est:.1f}%")
            any_bet = True
        elif p_yrfi >= 0.58:
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
    _run_db_backup()


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

    lines = [
        f"🌅 PARLAY OS — MORNING BRIEF — {today_label}",
        f"{len(events)} games today | Scout runs at 1pm ET",
        "Watch:",
    ]
    for g in watch_games[:3]:
        lines.append(f"• {g}")

    if line_moves:
        lines.append(f"Line movement since yesterday: {' | '.join(line_moves)}")
    else:
        lines.append("Line movement since yesterday: none significant yet")

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
        _run_debrief()

    elif "--weekly" in args:
        _run_weekly_roi()

    elif "--linecheck" in args:
        _run_linecheck()

    elif "--planner" in args:
        _run_morning_planner()

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
