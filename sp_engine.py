"""PARLAY OS — sp_engine.py
Fetches SP stats from MLB Stats API and applies ABS / TTOP / platoon adjustments.
Includes last-3-start rolling ERA/K/BB and real xFIP from FanGraphs leaderboard.
"""

import csv
import io
import json
import os
import re
import threading
import requests
from api_client import get as _http_get
from constants import (
    MLB_TEAM_IDS, UMPIRE_TENDENCIES,
    ABS_COMMAND_BONUS, ABS_FB_HEAVY_MALUS,
    TEAM_LHB_PCT, PLATOON_WRCPLUS_DELTA,
    LG_ERA,
)

STATSAPI = "https://statsapi.mlb.com/api/v1"
_DATA_CACHE_DIR = "data_cache"
FG_PITCHING_URL = (
    "https://www.fangraphs.com/api/leaders/major-league/data"
    "?age=&pos=all&stats=pit&lg=all&qual=0"
    "&season=2026&season1=2026&ind=0&team=0&rost=0&players=&type=8"
)

# Thread-safe in-process cache for FanGraphs leaderboard
_fg_lock    = threading.Lock()
_fg_cache: dict | None = None   # {normalized_name: xfip_float}


def _normalize_name(name: str) -> str:
    """Lowercase, strip accents, keep only letters and spaces."""
    import unicodedata
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z ]", "", name.lower()).strip()


def _load_fg_xfip() -> dict:
    """Fetch FanGraphs 2026 pitching leaderboard and return {normalized_name: xFIP}."""
    global _fg_cache
    with _fg_lock:
        if _fg_cache is not None:
            return _fg_cache
        try:
            r = _http_get(FG_PITCHING_URL, timeout=12, skip_cache=False)
            data = r.json()
            rows = data.get("data", []) if isinstance(data, dict) else data
            result = {}
            for row in rows:
                name = row.get("PlayerName") or row.get("Name") or ""
                xfip = row.get("xFIP") or row.get("xfip")
                if name and xfip is not None:
                    try:
                        result[_normalize_name(name)] = round(float(xfip), 2)
                    except (ValueError, TypeError):
                        pass
            _fg_cache = result
        except Exception:
            _fg_cache = {}
        return _fg_cache


def get_real_xfip(pitcher_name: str) -> float | None:
    """Look up real xFIP for a pitcher by name from FanGraphs. Returns None on miss."""
    if not pitcher_name or pitcher_name == "TBD":
        return None
    fg = _load_fg_xfip()
    key = _normalize_name(pitcher_name)
    return fg.get(key)


def _get_probable_pitchers(game_pk: int) -> dict:
    """Returns {away_id, home_id, away_name, home_name} via schedule (pre-game) then boxscore fallback."""
    # Primary: schedule endpoint with probablePitcher hydration — populated before first pitch
    try:
        r = _http_get(
            f"{STATSAPI}/schedule",
            params={"gamePk": game_pk, "hydrate": "probablePitcher", "sportId": 1},
            timeout=8,
        )
        for day in r.json().get("dates", []):
            for g in day.get("games", []):
                if g.get("gamePk") != game_pk:
                    continue
                teams = g.get("teams", {})
                result = {}
                found  = False
                for side in ("away", "home"):
                    pp = teams.get(side, {}).get("probablePitcher") or {}
                    result[f"{side}_id"]   = pp.get("id")
                    result[f"{side}_name"] = pp.get("fullName", "")
                    if pp.get("id"):
                        found = True
                if found:
                    return result
    except Exception:
        pass
    # Fallback: boxscore endpoint — populated in-game / post-game only
    try:
        r2 = _http_get(f"{STATSAPI}/game/{game_pk}/boxscore", timeout=8)
        box = r2.json()
        teams = box.get("teams", {})
        result = {}
        for side in ("away", "home"):
            t  = teams.get(side, {})
            pp = t.get("probablePitcher") or {}
            result[f"{side}_id"]   = pp.get("id")
            result[f"{side}_name"] = pp.get("fullName", "")
        return result
    except Exception:
        return {}


def _pitcher_season_stats(pitcher_id: int) -> dict:
    """Pull current-season pitching stats from Stats API."""
    try:
        r = _http_get(
            f"{STATSAPI}/people/{pitcher_id}/stats",
            params={"stats": "season", "group": "pitching", "season": "2026"},
            timeout=8,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s = splits[0].get("stat", {})
        ip_str   = s.get("inningsPitched", "0.0")
        ip_parts = str(ip_str).split(".")
        ip  = int(ip_parts[0]) + int(ip_parts[1] if len(ip_parts) > 1 else 0) / 3
        era = float(s.get("era", 4.35) or 4.35)
        whip= float(s.get("whip", 1.30) or 1.30)
        k9  = float(s.get("strikeoutsPer9Inn", 8.5) or 8.5)
        bb9 = float(s.get("walksPer9Inn", 3.0) or 3.0)
        hr9 = float(s.get("homeRunsPer9", 1.2) or 1.2)
        return {
            "ip":   round(ip, 1),
            "era":  era,
            "whip": whip,
            "k9":   k9,
            "bb9":  bb9,
            "hr9":  hr9,
            "gs":   int(s.get("gamesStarted", 0) or 0),
        }
    except Exception:
        return {}


def _pitcher_game_log_starts(pitcher_id: int, n: int = 10) -> list:
    """
    Pull last N game starts for a pitcher from MLB Stats API game log.
    Returns list of per-game stat dicts, sorted oldest→newest.
    """
    try:
        r = _http_get(
            f"{STATSAPI}/people/{pitcher_id}/stats",
            params={
                "stats":    "gameLog",
                "group":    "pitching",
                "season":   "2026",
                "gameType": "R",
            },
            timeout=10,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        starts = []
        for s in splits:
            stat = s.get("stat", {})
            if int(stat.get("gamesStarted", 0) or 0) == 0:
                continue
            ip_str   = stat.get("inningsPitched", "0.0")
            ip_parts = str(ip_str).split(".")
            ip = int(ip_parts[0]) + int(ip_parts[1] if len(ip_parts) > 1 else 0) / 3
            if ip <= 0:
                continue
            er = int(stat.get("earnedRuns", 0) or 0)
            k  = int(stat.get("strikeOuts", 0) or 0)
            bb = int(stat.get("baseOnBalls", 0) or 0)
            np = int(stat.get("pitchesThrown", 0) or 0)
            starts.append({
                "date":    s.get("date", ""),
                "ip":      round(ip, 2),
                "era":     round(er / ip * 9, 2),
                "k9":      round(k / ip * 9, 2),
                "bb9":     round(bb / ip * 9, 2),
                "np":      np,
                "er":      er,
                "game_pk": (s.get("game") or {}).get("gamePk"),
                "team_id": (s.get("team") or {}).get("id"),
            })
        return starts[-n:]   # last n starts, oldest first
    except Exception:
        return []


def _last_3_starts_summary(pitcher_id: int) -> dict:
    """
    Rolling stats over last 3 starts, plus trend flags.
    Velocity trend uses K-rate as proxy (velocity data requires Statcast).
    """
    starts = _pitcher_game_log_starts(pitcher_id, n=10)
    if not starts:
        return {}

    last3 = starts[-3:]
    if not last3:
        return {}

    eras = [g["era"] for g in last3]
    k9s  = [g["k9"]  for g in last3]
    bb9s = [g["bb9"] for g in last3]

    rolling_era = round(sum(eras) / len(eras), 2)
    rolling_k9  = round(sum(k9s)  / len(k9s), 2)
    rolling_bb9 = round(sum(bb9s) / len(bb9s), 2)

    # Worsening walk rate flag: BB/9 increased by >1.0 from earliest to latest start
    worsening_walk = (len(bb9s) >= 2) and (bb9s[-1] - bb9s[0] > 1.0)

    # Velocity trend proxy from K rate (Statcast velocity added in statcast_engine)
    # K9 drop >1.5 over last 3 suggests potential velocity decline
    k9_declining = (len(k9s) >= 2) and (k9s[0] - k9s[-1]) > 1.5

    # High-pitch recent flag: 100+ pitches in any start within last 4 days
    from datetime import date as _date
    _today = _date.today()
    high_pitch_recent = any(
        ((_today - _date.fromisoformat(g["date"])).days <= 4 and g.get("np", 0) >= 100)
        for g in starts[-5:]
        if g.get("date") and g.get("np") is not None
    )

    result = {
        "rolling_era_3":    rolling_era,
        "rolling_k9_3":     rolling_k9,
        "rolling_bb9_3":    rolling_bb9,
        "n_starts":         len(last3),
        "worsening_walk":   worsening_walk,
        "k9_declining":     k9_declining,
        "game_log_3":       last3,
        "high_pitch_recent": high_pitch_recent,
    }

    # Velocity trend from last 10 starts K rate trend
    # K/9 alone is NOT sufficient for INJURY_RISK — could be a command change.
    # Thresholds: velocity_decline at -1.0 K/9 (command/velo issue, flag separately);
    # velocity_injury_risk only at -3.0 K/9 (severe enough to suggest arm injury).
    # True velocity confirmation requires Statcast data (checked in intelligence_engine).
    if len(starts) >= 5:
        first_half  = starts[:len(starts)//2]
        second_half = starts[len(starts)//2:]
        k9_early = sum(g["k9"] for g in first_half) / len(first_half)
        k9_late  = sum(g["k9"] for g in second_half) / len(second_half)
        k9_trend = round(k9_late - k9_early, 2)      # negative = declining K rate
        result["k9_trend_10s"]        = k9_trend
        result["velocity_decline"]    = k9_trend < -1.0   # command or velo change
        result["velocity_injury_risk"]= k9_trend < -3.0   # severe: possible arm injury
    else:
        result["k9_trend_10s"]         = 0.0
        result["velocity_decline"]     = False
        result["velocity_injury_risk"] = False

    return result


def _linescore_cached(game_pk: int) -> dict | None:
    """Fetch linescore with disk cache so past-game linescores aren't refetched."""
    os.makedirs(_DATA_CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(_DATA_CACHE_DIR, f"ls_{game_pk}.json")
    if os.path.exists(cache_path):
        try:
            with open(cache_path) as f:
                return json.load(f)
        except Exception:
            pass
    try:
        r = _http_get(f"{STATSAPI}/game/{game_pk}/linescore", timeout=8)
        data = r.json()
        if data.get("innings"):
            with open(cache_path, "w") as f:
                json.dump(data, f)
        return data
    except Exception:
        return None


def _sp_first_inning_era(pitcher_id: int, season_era: float, n_starts: int = 3) -> dict:
    """
    Calculate SP first-inning ERA from game logs + linescores.
    Returns first_inning_era and yrfi_lean flag (fi_era > season_era + 2.0).
    Uses disk cache so linescores are only fetched once.
    """
    starts = _pitcher_game_log_starts(pitcher_id, n=n_starts + 2)
    if not starts or len(starts) < 3:
        return {}

    total_fi_runs = 0
    counted       = 0
    for s in starts[-n_starts:]:
        game_pk = s.get("game_pk")
        team_id = s.get("team_id")
        if not game_pk or not team_id:
            continue
        ls = _linescore_cached(game_pk)
        if not ls:
            continue
        innings = ls.get("innings", [])
        if not innings:
            continue
        first = innings[0]
        # Pitcher's team is home → away team scored against home SP in top of 1st
        home_team_id = (ls.get("teams", {}).get("home", {}).get("team") or {}).get("id")
        if team_id == home_team_id:
            fi_runs = int(first.get("away", {}).get("runs", 0) or 0)
        else:
            fi_runs = int(first.get("home", {}).get("runs", 0) or 0)
        total_fi_runs += fi_runs
        counted       += 1

    if counted < 2:
        return {}

    fi_era = round(total_fi_runs / counted * 9, 2)
    return {
        "first_inning_era": fi_era,
        "fi_n_starts":      counted,
        "yrfi_lean":        fi_era >= (season_era + 3.0),
    }


def _pitcher_meta(pitcher_id: int) -> dict:
    """Pull handedness from Stats API."""
    try:
        r = _http_get(
            f"{STATSAPI}/people/{pitcher_id}?hydrate=pitchingStats",
            timeout=8,
        )
        person = r.json().get("people", [{}])[0]
        hand   = person.get("pitchHand", {}).get("code", "R")
        return {"hand": hand}
    except Exception:
        return {"hand": "R"}


def _xfip_estimate(era: float, bb9: float, k9: float, hr9: float) -> float:
    """Rough xFIP from available stats."""
    xfip = (13 * (hr9 * 0.85) + 3 * bb9 - 2 * k9) / 9 + 3.17
    return round(xfip, 2)


def _ttop_flag(stats: dict) -> bool:
    """True if SP is likely to turn lineup over a 3rd time (TTOP risk)."""
    gs = stats.get("gs", 0)
    ip = stats.get("ip", 0)
    if gs == 0:
        return False
    return (ip / gs) >= 5.5


def _abs_adjustment(bb9: float, k9: float, hand: str) -> float:
    """Simple ABS heuristic fallback when no pitch mix data is available."""
    if bb9 < 2.5:
        return ABS_COMMAND_BONUS
    if bb9 > 3.5 and k9 < 7.5:
        return ABS_FB_HEAVY_MALUS
    return 0.0


def compute_abs_score(bb9: float, pm: dict | None) -> float | None:
    """
    Full ABS score using pitch mix data.
    Formula: (diversity × 0.3) + ((4.0 - bb9) × 0.3) + ((1 - fb_pct) × 0.25) + (offspeed_whiff × 0.15)
    Normalized 0–100. Returns None when insufficient pitch data (< 100 pitches).

    ABS score > 65 → +0.025 run factor (command/diverse arsenal benefits).
    ABS score < 35 → -0.020 run factor (fastball-heavy penalized by robot umpire).
    """
    if not pm:
        return None
    pitch_mix = pm.get("pitch_mix") or {}
    if len(pitch_mix) < 2:
        return None
    if pm.get("total_pitches", 0) < 100:
        return None

    # Arsenal diversity: distinct pitch types with ≥5% usage (1 type=0.0, 5+=1.0)
    significant = [pt for pt, d in pitch_mix.items() if d.get("usage_pct", 0) >= 0.05]
    n_types   = len(significant)
    diversity = min((n_types - 1) / 4.0, 1.0)

    # Fastball percentage (four-seam, two-seam, cutter, sinker)
    _FB_TYPES = {"FF", "SI", "FT", "FA", "FC"}
    fb_pct = sum(pitch_mix.get(pt, {}).get("usage_pct", 0) for pt in _FB_TYPES)
    fb_pct = min(max(fb_pct, 0.0), 1.0)

    # Off-speed weighted whiff rate (slider, curve, change, splitter, sweeper)
    _OS_TYPES = {"SL", "CU", "KC", "CH", "FS", "FO", "CS", "ST", "SV"}
    os_np     = sum(pitch_mix.get(pt, {}).get("count", 0) for pt in _OS_TYPES)
    os_whiff  = sum(
        pitch_mix.get(pt, {}).get("count", 0) * pitch_mix.get(pt, {}).get("whiff_rate", 0)
        for pt in _OS_TYPES
    )
    offspeed_whiff = (os_whiff / os_np) if os_np >= 20 else 0.10

    bb9_c = max(1.5, min(bb9, 6.0))
    raw = (diversity * 0.3) + ((4.0 - bb9_c) * 0.3) + ((1 - fb_pct) * 0.25) + (offspeed_whiff * 0.15)
    return round(min(max(raw / 1.3 * 100, 0.0), 100.0), 1)


def _sp_platoon_splits(pitcher_id: int) -> dict:
    """
    Fetch SP platoon splits vs LHH and RHH from MLB Stats API (current season only).
    Returns {vs_lhh: {woba, obp, slg, pa}, vs_rhh: {...}, platoon_vulnerability: bool}.
    wOBA > 0.360 vs one side = PLATOON_VULNERABILITY.
    Weights: 70% current season / 30% career blend NOT applied here — season-only data.
    """
    result: dict = {}
    for sit, key in (("vl", "vs_lhh"), ("vr", "vs_rhh")):
        try:
            r = _http_get(
                f"{STATSAPI}/people/{pitcher_id}/stats",
                params={
                    "stats":    "statSplits",
                    "group":    "pitching",
                    "season":   "2026",
                    "gameType": "R",
                    "sitCodes": sit,
                },
                timeout=10,
            )
            splits = r.json().get("stats", [{}])[0].get("splits", [])
            if not splits:
                continue
            s = splits[0].get("stat", {})
            obp  = float(s.get("obp", 0.320) or 0.320)
            slg  = float(s.get("slg", 0.410) or 0.410)
            woba_raw = float(s.get("wOBA") or s.get("woba") or 0.330)
            pa   = int(s.get("plateAppearances", 0) or 0)
            # wOBA proxy: league wOBA ≈ OBP * 0.72 + SLG * 0.28 when not directly available
            if woba_raw == 0.330 and obp != 0.320:
                woba_raw = round(obp * 0.72 + slg * 0.28, 3)
            result[key] = {
                "woba": round(woba_raw, 3),
                "obp":  obp,
                "slg":  slg,
                "pa":   pa,
            }
        except Exception:
            pass

    # Platoon vulnerability: wOBA > 0.360 vs either handedness (with enough PA)
    vuln = False
    vuln_side = ""
    for key, hand in (("vs_lhh", "LHH"), ("vs_rhh", "RHH")):
        split = result.get(key, {})
        if split.get("pa", 0) >= 30 and split.get("woba", 0) >= 0.360:
            vuln = True
            vuln_side = f"{split['woba']:.3f} wOBA vs {hand}"

    result["platoon_vulnerability"] = vuln
    result["platoon_vuln_detail"]   = vuln_side
    return result


def _platoon_run_factor(sp_hand: str, opp_team: str) -> float:
    """Adjust run expectancy based on SP handedness vs opponent LHB%."""
    lhb_pct   = TEAM_LHB_PCT.get(opp_team, 0.43)
    rhb_pct   = 1.0 - lhb_pct
    lhb_delta = PLATOON_WRCPLUS_DELTA.get((sp_hand, "L"), 0)
    rhb_delta = PLATOON_WRCPLUS_DELTA.get((sp_hand, "R"), 0)
    avg_delta = lhb_pct * lhb_delta + rhb_pct * rhb_delta
    return round(1.0 + avg_delta / 100, 4)


def analyze_sp(pitcher_id: int, opp_team: str, umpire: str = "",
               pitcher_name: str = "") -> dict:
    """Full SP analysis: season stats + last-3-start rolling + adjustments."""
    stats  = _pitcher_season_stats(pitcher_id)
    meta   = _pitcher_meta(pitcher_id)
    last3  = _last_3_starts_summary(pitcher_id) if pitcher_id else {}

    if not stats:
        return _default_sp(pitcher_id, opp_team, umpire)

    fi_data = _sp_first_inning_era(pitcher_id, stats.get("era", 4.35)) if pitcher_id else {}

    hand = meta.get("hand", "R")
    era  = stats.get("era", 4.35)
    k9   = stats.get("k9", 8.5)
    bb9  = stats.get("bb9", 3.0)
    hr9  = stats.get("hr9", 1.2)

    # Prefer real xFIP from FanGraphs; fall back to formula estimate
    real_xfip = get_real_xfip(pitcher_name) if pitcher_name else None
    xfip      = real_xfip if real_xfip is not None else _xfip_estimate(era, bb9, k9, hr9)

    # Use last-3-start ERA if available and significantly different from season ERA
    effective_era = era
    if last3.get("rolling_era_3") is not None:
        r3_era = last3["rolling_era_3"]
        effective_era = round(0.60 * era + 0.40 * r3_era, 2)

    ump_k, ump_run, ump_note = UMPIRE_TENDENCIES.get(umpire, (1.0, 1.0, ""))
    plat_rf  = _platoon_run_factor(hand, opp_team)
    ttop     = _ttop_flag(stats)

    # ── GB rate, FP strike rate, and ABS score from Savant pitch-level data ──
    platoon_splits = _sp_platoon_splits(pitcher_id) if pitcher_id else {}
    gb_rate        = None
    fp_strike_rate = None
    abs_score      = None
    pm             = None
    try:
        from statcast_engine import get_pitcher_pitch_mix
        pm = get_pitcher_pitch_mix(pitcher_id)
        if pm:
            gb_rate        = pm.get("gb_rate")
            fp_strike_rate = pm.get("fp_strike_rate")
            abs_score      = compute_abs_score(bb9, pm)
    except Exception:
        pass

    # ── Savant leaderboard signals (ADDs 1-10) ────────────────────────────────
    savant_signals: dict = {}
    try:
        from savant_leaderboards import sp_savant_signals
        savant_signals = sp_savant_signals(pitcher_id)
    except Exception:
        pass

    # ABS adjustment: use full score when available, else simple heuristic
    if abs_score is not None:
        if abs_score > 65:
            abs_adj = ABS_COMMAND_BONUS
        elif abs_score < 35:
            abs_adj = ABS_FB_HEAVY_MALUS
        else:
            abs_adj = 0.0
    else:
        abs_adj = _abs_adjustment(bb9, k9, hand)

    sp_quality = round(xfip / max(LG_ERA, 0.01), 4)
    run_factor = round(ump_run * plat_rf * sp_quality * (1.0 + abs_adj), 4)

    return {
        "pitcher_id":       pitcher_id,
        "hand":             hand,
        "era":              era,
        "effective_era":    effective_era,
        "xfip":             xfip,
        "xfip_source":      "fangraphs" if real_xfip is not None else "estimated",
        "k9":               k9,
        "bb9":              bb9,
        "hr9":              hr9,
        "ip":               stats.get("ip", 0),
        "gs":               stats.get("gs", 0),
        "ttop":             ttop,
        "ump_k_factor":     ump_k,
        "ump_run_factor":   ump_run,
        "ump_note":         ump_note,
        "abs_adj":          abs_adj,
        "abs_score":        abs_score,
        "plat_run_factor":  plat_rf,
        "run_factor":       run_factor,
        # Last-3-start rolling stats
        "rolling_era_3":    last3.get("rolling_era_3"),
        "rolling_k9_3":     last3.get("rolling_k9_3"),
        "rolling_bb9_3":    last3.get("rolling_bb9_3"),
        "worsening_walk":   last3.get("worsening_walk", False),
        "k9_declining":     last3.get("k9_declining", False),
        # Velocity trend (K-rate proxy; true velocity requires Statcast)
        "velocity_decline":      last3.get("velocity_decline", False),
        "velocity_injury_risk":  last3.get("velocity_injury_risk", False),
        "k9_trend_10s":          last3.get("k9_trend_10s", 0.0),
        # Fatigue: 100+ pitch start in last 4 days
        "high_pitch_recent":     last3.get("high_pitch_recent", False),
        # First inning performance
        "first_inning_era":      fi_data.get("first_inning_era"),
        "fi_n_starts":           fi_data.get("fi_n_starts"),
        "yrfi_lean":             fi_data.get("yrfi_lean", False),
        # Platoon splits (this season only)
        "platoon_splits":        platoon_splits,
        "platoon_vulnerability": platoon_splits.get("platoon_vulnerability", False),
        "platoon_vuln_detail":   platoon_splits.get("platoon_vuln_detail", ""),
        # Batted ball / command metrics from Savant
        "gb_rate":           gb_rate,
        "fp_strike_rate":    fp_strike_rate,
        # Savant leaderboard signals (ADDs 1-10)
        "savant":            savant_signals,
        "xwoba_against":     savant_signals.get("xwoba_against"),
        "xwoba_tier":        savant_signals.get("xwoba_tier", "UNKNOWN"),
        "rolling_xwoba_tier":savant_signals.get("rolling_tier", "UNKNOWN"),
        "pitch_tempo":       savant_signals.get("pitch_tempo"),
        "tempo_label":       savant_signals.get("tempo_label", "UNKNOWN"),
        "arm_angle":         savant_signals.get("arm_angle"),
        "fps_model_adj":     savant_signals.get("fps_model_adj", 0.0),
        "yoy_conf_adj":      savant_signals.get("yoy_conf_adj", 0),
        "k_conf_adj_savant": savant_signals.get("k_conf_adj", 0),
    }


def _default_sp(pitcher_id, opp_team, umpire) -> dict:
    ump_k, ump_run, ump_note = UMPIRE_TENDENCIES.get(umpire, (1.0, 1.0, ""))
    return {
        "pitcher_id":       pitcher_id,
        "sp_missing":       True,
        "hand":             "R",
        "era":              4.35,
        "effective_era":    4.35,
        "xfip":             4.35,
        "k9":               8.5,
        "bb9":              3.0,
        "hr9":              1.2,
        "ip":               0,
        "gs":               0,
        "ttop":             False,
        "ump_k_factor":     ump_k,
        "ump_run_factor":   ump_run,
        "ump_note":         ump_note,
        "abs_adj":          0.0,
        "abs_score":        None,
        "plat_run_factor":  1.0,
        "run_factor":       ump_run,
        "rolling_era_3":    None,
        "rolling_k9_3":     None,
        "rolling_bb9_3":    None,
        "worsening_walk":   False,
        "k9_declining":     False,
        "velocity_decline":  False,
        "velocity_injury_risk": False,
        "k9_trend_10s":      0.0,
        "high_pitch_recent":     False,
        "first_inning_era":      None,
        "fi_n_starts":           None,
        "yrfi_lean":             False,
        "platoon_splits":        {},
        "platoon_vulnerability": False,
        "platoon_vuln_detail":   "",
        "gb_rate":               None,
        "fp_strike_rate":        None,
        "savant":                {},
        "xwoba_against":         None,
        "xwoba_tier":            "UNKNOWN",
        "rolling_xwoba_tier":    "UNKNOWN",
        "pitch_tempo":           None,
        "tempo_label":           "UNKNOWN",
        "arm_angle":             None,
        "fps_model_adj":         0.0,
        "yoy_conf_adj":          0,
        "k_conf_adj_savant":     0,
    }


def get_game_sps(game_pk: int, away_team: str, home_team: str, umpire: str = "") -> dict:
    """Return SP analysis for both starters in a game."""
    pp        = _get_probable_pitchers(game_pk)
    away_id   = pp.get("away_id")
    home_id   = pp.get("home_id")
    away_name = pp.get("away_name", "TBD")
    home_name = pp.get("home_name", "TBD")

    away_sp = (analyze_sp(away_id, home_team, umpire, pitcher_name=away_name)
               if away_id else _default_sp(None, home_team, umpire))
    home_sp = (analyze_sp(home_id, away_team, umpire, pitcher_name=home_name)
               if home_id else _default_sp(None, away_team, umpire))

    away_sp["name"] = away_name
    home_sp["name"] = home_name

    return {"away": away_sp, "home": home_sp}


if __name__ == "__main__":
    # Quick test: Zack Wheeler ID = 554430
    r = analyze_sp(554430, "NYM", "Vic Carapazza")
    print(f"ERA={r['era']}  xFIP={r['xfip']}  effective_era={r['effective_era']}")
    print(f"rolling_era_3={r['rolling_era_3']}  rolling_k9_3={r['rolling_k9_3']}")
    print(f"worsening_walk={r['worsening_walk']}  velocity_decline={r['velocity_decline']}")
    print(f"run_factor={r['run_factor']}")
