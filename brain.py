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
from math_engine    import american_to_decimal, implied_prob, no_vig_prob, expected_value, STARTING_BANKROLL
from weather_engine import get_weather
from sp_engine      import get_game_sps
from bullpen_engine import analyze_bullpen, bullpen_run_factor
from offense_engine import analyze_offense
from market_engine  import get_mlb_events, full_market_snapshot
from bankroll_engine import kelly_stake, sizing_summary, current_bankroll, is_drawdown_pause
from props_engine   import (
    k_prop, nrfi_prob, team_run_expectancy, game_total_prob,
    f5_run_expectancy, correlated_parlay, scan_k_prop
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
        away_code, home_code, game_date
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

    # ── Regression / intelligence flags ──────────────────────────────────────
    reg_flags = detect_regression_flags(away_sp, home_sp, away_off, home_off)
    momentum  = _momentum_score(away_code, home_code)
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
    if away_sp.get("worsening_walk"):   away_sp_flags.append("BB↑")
    if away_sp.get("velocity_decline"): away_sp_flags.append("velo↓")
    if away_sp.get("k9_declining"):     away_sp_flags.append("K↓")
    if home_sp.get("worsening_walk"):   home_sp_flags.append("BB↑")
    if home_sp.get("velocity_decline"): home_sp_flags.append("velo↓")
    if home_sp.get("k9_declining"):     home_sp_flags.append("K↓")

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
    # Print any regression/narrative flags
    for flag in reg_flags.get("flags", []):
        print(f"  FLAG: {flag['message']}")
    for flag in narrative.get("flags", []):
        print(f"  FLAG: {flag['message']}")

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
    }


def _momentum_score(away_code: str, home_code: str) -> dict:
    """
    Simple momentum proxy: win% in last 7 games from team_memory.
    Returns dict with away/home momentum scores (-1 to +1).
    """
    from memory_engine import team_prior
    away_m = team_prior(away_code, "home", 7) or 0.50
    home_m = team_prior(home_code, "home", 7) or 0.50
    return {
        "away": round(away_m - 0.50, 3),
        "home": round(home_m - 0.50, 3),
    }


def _narrative_flags(market: dict, away_nv: float, home_nv: float) -> dict:
    """
    Detect public narrative / fade opportunities.
    Heavy public action (estimated via line gap), primetime games.
    """
    flags = []
    lm = market.get("line_movement") or {}

    # If line moved significantly toward one side, public may be piling in
    direction = lm.get("direction", "")
    magnitude = lm.get("magnitude", 0)
    if magnitude > 0.08 and direction not in ("unknown", "stable"):
        fading_side = "away" if "home" in direction else "home"
        flags.append({
            "type":    "LINE_STEAM",
            "message": f"Large line move {direction} ({magnitude:.3f}) — consider fade of {fading_side}",
        })

    # Polymarket vs sharp book gap > 15% = narrative divergence
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

    return {"flags": flags}


def _conviction(edge_pct: float, model_p: float, bp: dict, market: dict) -> str:
    if edge_pct < MIN_EDGE_PCT:
        return "PASS"
    poly = market.get("polymarket") or {}
    poly_confirms = False
    if poly:
        poly_p = poly.get("away") or poly.get("home") or 0
        poly_confirms = poly_p > 0.5 and model_p > 0.5

    tier = "LOW"
    if edge_pct >= 6 and bp.get("fatigue_tier") in ("FRESH", "MODERATE") and poly_confirms:
        tier = "HIGH"
    elif edge_pct >= 4:
        tier = "MEDIUM"
    return tier


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

    sp_key = f"{side}_sp"
    sp = game.get(sp_key, {})
    if not sp.get("name") or sp.get("name") == "TBD":
        print(f"  PASS {team}: SP TBD — no starter confirmed")
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

        bet_found = False
        for side in ("away", "home"):
            if _should_recommend(analysis, side):
                msg = _format_bet_message(analysis, side)
                _send_telegram(msg)
                all_bets.append(analysis)
                bet_found = True

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

    # Save scout output
    if not DRY_RUN:
        with open("last_scout.json", "w") as f:
            json.dump(scout_out, f, indent=2)
        _db.log_scout_run(today, n_bets, json.dumps(scout_out))

    return scout_out


# ── ENTRY POINT ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        from telegram_handler import start_listener, start_auto_settler, sync_scout_json
    except Exception as _te:
        print(f"[WARN] telegram_handler import failed: {_te}")
        # Define no-op stubs so the rest of __main__ still works
        def start_listener(): pass
        def start_auto_settler(): pass
        def sync_scout_json(): pass

    args = set(sys.argv[1:])

    if "--bot" in args:
        # Persistent bot mode: Telegram listener + auto-settler only — never run scout
        try:
            from telegram_handler import _poll_loop
            start_auto_settler()
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
        # Default: start Telegram listener + auto-settler in background, run scout once, exit
        try:
            print("Starting Telegram listener...")
            start_listener()
            print("Starting auto-settler...")
            start_auto_settler()
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
