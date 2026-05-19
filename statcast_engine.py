"""PARLAY OS — statcast_engine.py
Per-pitcher and per-batter Statcast metrics via Baseball Savant aggregated leaderboard.

Uses group_by=name so each response returns one pre-aggregated row per player
with columns: exit_velocity_avg, barrel_batted_rate, hard_hit_percent, release_speed.
Falls back gracefully if Savant is unavailable.
"""

import csv
import io
from api_client import get as _http_get
from functools import lru_cache

SAVANT_BASE = "https://baseballsavant.mlb.com"
TIMEOUT     = 15

# Savant returns these exact values when there is no real data for the player
# (placeholder / zero-sample row). Treat them as missing, not real.
_SAVANT_DEFAULT_EV     = 82.4
_SAVANT_DEFAULT_BARREL = 1.3


# ── URL builders ──────────────────────────────────────────────────────────────

def _savant_pitcher_url(pitcher_id: int, season: int = 2026) -> str:
    # group_by=name returns one aggregated row per pitcher with pre-computed averages.
    return (
        f"{SAVANT_BASE}/statcast_search/csv"
        f"?all=true&hfGT=R%7C&hfSea={season}%7C&player_type=pitcher"
        f"&pitchers_lookup%5B%5D={pitcher_id}"
        f"&group_by=name&min_pitches=0&min_results=0"
        f"&sort_col=pitches&sort_order=desc&type=details&min_pas=0"
    )


def _savant_batter_url(batter_id: int, season: int = 2026) -> str:
    return (
        f"{SAVANT_BASE}/statcast_search/csv"
        f"?all=true&hfGT=R%7C&hfSea={season}%7C&player_type=batter"
        f"&batter={batter_id}"
        f"&group_by=name&min_pitches=0&min_results=0"
        f"&sort_col=pitches&sort_order=desc&type=details&min_pas=0"
    )


# ── Pitch-level CSV parser ────────────────────────────────────────────────────

# Fastball pitch type codes used for velocity averaging
_FASTBALL_TYPES = {"FF", "SI", "FT", "FA", "FC"}

# Simplified barrel: EV ≥ 98 AND launch_angle 26–30 (close to MLB official)
_BARREL_EV_MIN = 98.0
_BARREL_LA_LO  = 26
_BARREL_LA_HI  = 30
_HARD_HIT_EV   = 95.0


def _parse_aggregated_csv(text: str, role: str = "pitcher") -> dict:
    """
    Parse Savant pitch-level CSV (what statcast_search returns) and compute
    per-player aggregates: EV avg, barrel%, hard-hit%, avg fastball velo.
    Savant returns individual pitches, so we compute averages ourselves.
    """
    try:
        text = text.lstrip("﻿")   # strip BOM
        reader = csv.DictReader(io.StringIO(text))
        rows   = list(reader)
        if not rows:
            return {}

        def _flt(val: str) -> float | None:
            if not val or val in ("", "null", "NA", "nan", "."):
                return None
            try:
                return float(val)
            except ValueError:
                return None

        ev_vals:    list[float] = []
        hh_count:   int = 0
        brl_count:  int = 0
        velo_vals:  list[float] = []

        for row in rows:
            # ── Batted-ball metrics (rows that have launch_speed) ──────────────
            ls = _flt(row.get("launch_speed", ""))
            la = _flt(row.get("launch_angle", ""))
            if ls is not None and ls > 0:
                ev_vals.append(ls)
                if ls >= _HARD_HIT_EV:
                    hh_count += 1
                if (ls >= _BARREL_EV_MIN and la is not None
                        and _BARREL_LA_LO <= la <= _BARREL_LA_HI):
                    brl_count += 1

            # ── Fastball velocity (pitcher only) ─────────────────────────────
            if role == "pitcher":
                pt = (row.get("pitch_type") or "").strip()
                rs = _flt(row.get("release_speed", ""))
                if pt in _FASTBALL_TYPES and rs is not None and rs > 0:
                    velo_vals.append(rs)

        n = len(ev_vals)
        if n == 0:
            return {"sample_batted_balls": 0}

        result: dict = {}
        result["exit_velocity_avg"]     = round(sum(ev_vals) / n, 1)
        result["hard_hit_pct"]          = round(hh_count / n * 100, 1)
        result["barrel_pct"]            = round(brl_count / n * 100, 1)
        result["sample_batted_balls"]   = n

        if role == "pitcher" and velo_vals:
            result["avg_fastball_velo"] = round(sum(velo_vals) / len(velo_vals), 1)

        return result

    except Exception:
        return {}


# ── Pitcher Statcast ──────────────────────────────────────────────────────────

@lru_cache(maxsize=64)
def _fetch_pitcher_statcast_season(pitcher_id: int, season: int) -> dict:
    """Inner fetch for a single season — no fallback logic here."""
    url = _savant_pitcher_url(pitcher_id, season)
    print(f"  [STATCAST] Fetching pitcher {pitcher_id} season={season} url={url}")
    r = _http_get(
        url, timeout=TIMEOUT,
        headers={"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/1.0)"},
        skip_cache=False,
    )
    if r.status_code != 200 or not r.text.strip():
        print(f"  [STATCAST] Pitcher {pitcher_id} season={season}: HTTP {r.status_code} or empty")
        return {}
    data = _parse_aggregated_csv(r.text, "pitcher")
    if not data:
        print(f"  [STATCAST] Pitcher {pitcher_id} season={season}: CSV parsed but no usable columns")
        return {}
    n      = data.get("sample_batted_balls", 0)
    ev     = data.get("exit_velocity_avg")
    barrel = data.get("barrel_pct")
    if n == 0:
        print(f"  [STATCAST] Pitcher {pitcher_id} season={season}: sample=0 — no real data")
        return {}
    if ev == _SAVANT_DEFAULT_EV and barrel == _SAVANT_DEFAULT_BARREL:
        print(f"  [STATCAST] Pitcher {pitcher_id} season={season}: placeholder defaults — discarding")
        return {}
    return data


@lru_cache(maxsize=64)
def get_pitcher_statcast(pitcher_id: int, season: int = 2026) -> dict:
    """
    Fetch pitcher Statcast metrics: EV allowed, barrel%, hard-hit%, avg velo.
    Falls back to 2025 season if 2026 returns no data (flags as STATCAST_2025).
    Cached per pitcher_id/season for the run. Returns {} on failure.
    """
    if not pitcher_id:
        return {}
    try:
        data = _fetch_pitcher_statcast_season(pitcher_id, season)
        if data:
            data["pitcher_id"] = pitcher_id
            n      = data.get("sample_batted_balls", 0)
            ev     = data.get("exit_velocity_avg")
            barrel = data.get("barrel_pct")
            print(f"  [STATCAST] Pitcher {pitcher_id} season={season}: OK — EV={ev} barrel={barrel} n={n}")
            return data
        # 2026 returned nothing — try 2025 as fallback
        if season == 2026:
            print(f"  [STATCAST] Pitcher {pitcher_id}: 2026 empty — trying 2025 fallback")
            fallback = _fetch_pitcher_statcast_season(pitcher_id, 2025)
            if fallback:
                fallback["pitcher_id"]    = pitcher_id
                fallback["STATCAST_2025"] = True   # flag: data is prior year
                n      = fallback.get("sample_batted_balls", 0)
                ev     = fallback.get("exit_velocity_avg")
                barrel = fallback.get("barrel_pct")
                print(f"  [STATCAST] Pitcher {pitcher_id}: 2025 fallback OK — "
                      f"EV={ev} barrel={barrel} n={n} (STATCAST_2025)")
                return fallback
            print(f"  [STATCAST] Pitcher {pitcher_id}: both 2026 and 2025 empty")
        return {}
    except Exception as e:
        print(f"  [STATCAST] Pitcher {pitcher_id}: exception — {e}")
        return {}


# ── Batter Statcast ───────────────────────────────────────────────────────────

@lru_cache(maxsize=128)
def _fetch_batter_statcast_season(batter_id: int, season: int) -> dict:
    """Inner fetch for a single season — no fallback logic here."""
    url = _savant_batter_url(batter_id, season)
    r = _http_get(
        url, timeout=TIMEOUT,
        headers={"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/1.0)"},
        skip_cache=False,
    )
    if r.status_code != 200 or not r.text.strip():
        return {}
    data = _parse_aggregated_csv(r.text, "batter")
    if not data:
        return {}
    n      = data.get("sample_batted_balls", 0)
    ev     = data.get("exit_velocity_avg")
    barrel = data.get("barrel_pct")
    if n == 0:
        return {}
    if ev == _SAVANT_DEFAULT_EV and barrel == _SAVANT_DEFAULT_BARREL:
        return {}
    return data


@lru_cache(maxsize=128)
def get_batter_statcast(batter_id: int, season: int = 2026) -> dict:
    """
    Fetch batter Statcast metrics: EV, barrel%, hard-hit%.
    Falls back to 2025 season if 2026 returns no data (flags as STATCAST_2025).
    Cached per batter_id/season. Returns {} on failure.
    """
    if not batter_id:
        return {}
    try:
        data = _fetch_batter_statcast_season(batter_id, season)
        if data:
            data["batter_id"] = batter_id
            return data
        if season == 2026:
            fallback = _fetch_batter_statcast_season(batter_id, 2025)
            if fallback:
                fallback["batter_id"]    = batter_id
                fallback["STATCAST_2025"] = True
                return fallback
        return {}
    except Exception as e:
        print(f"  [STATCAST] Batter {batter_id}: exception — {e}")
        return {}


def get_lineup_statcast(lineup: list, season: int = 2026) -> dict:
    """
    Fetch Statcast metrics for a full lineup.
    lineup: list of {id, name} dicts from offense_engine.
    Returns {player_name: statcast_dict}.
    """
    result = {}
    for player in lineup:
        pid  = player.get("id")
        name = player.get("name", str(pid))
        if not pid:
            continue
        sc = get_batter_statcast(pid, season)
        if sc:
            result[name] = sc
    return result


def sp_statcast_summary(pitcher_id: int, season: int = 2026) -> str:
    """Human-readable Statcast summary for a SP."""
    if not pitcher_id:
        return ""
    sc = get_pitcher_statcast(pitcher_id, season)
    if not sc:
        return ""
    parts = []
    if sc.get("STATCAST_2025"):
        parts.append("[2025]")
    if sc.get("exit_velocity_avg"):
        parts.append(f"EV_allowed={sc['exit_velocity_avg']}")
    if sc.get("barrel_pct") is not None:
        parts.append(f"barrel%={sc['barrel_pct']}")
    if sc.get("hard_hit_pct") is not None:
        parts.append(f"HH%={sc['hard_hit_pct']}")
    if sc.get("avg_fastball_velo"):
        parts.append(f"velo={sc['avg_fastball_velo']}")
    return " ".join(parts) if parts else ""


# ── Pitcher pitch mix + GB rate + FP strike rate ─────────────────────────────

_FP_STRIKE_DESCS = {
    "called_strike", "swinging_strike", "swinging_strike_blocked",
    "foul", "foul_tip", "hit_into_play", "foul_bunt",
    "missed_bunt", "bunt_foul_tip",
}
_BATTED_BALL_TYPES = {"ground_ball", "fly_ball", "line_drive", "popup"}


def _savant_pitcher_detail_url(pitcher_id: int, season: int = 2026) -> str:
    return (
        f"{SAVANT_BASE}/statcast_search/csv"
        f"?all=true&hfGT=R%7C&hfSea={season}%7C&player_type=pitcher"
        f"&pitchers_lookup%5B%5D={pitcher_id}"
        f"&min_pitches=0&min_results=0"
        f"&sort_col=pitches&sort_order=desc&type=details&min_pas=0"
    )


def _parse_pitch_details(text: str) -> dict:
    """
    Parse individual pitch-level Savant CSV.
    Returns gb_rate, fp_strike_rate, pitch_mix {type: {usage_pct, whiff_rate}}.
    """
    try:
        text = text.lstrip("﻿")
        reader = csv.DictReader(io.StringIO(text))

        total_pitches   = 0
        total_bb        = 0
        gb_count        = 0
        fp_total        = 0
        fp_strike_count = 0
        type_totals: dict = {}

        for row in reader:
            pt   = (row.get("pitch_type") or "").strip()
            desc = (row.get("description") or "").strip()
            bb_t = (row.get("bb_type") or "").strip()
            try:
                pnum = int(row.get("pitch_number") or 0)
            except (ValueError, TypeError):
                pnum = 0

            if not pt or pt in ("", "None"):
                continue

            total_pitches += 1
            if pt not in type_totals:
                type_totals[pt] = {"n": 0, "whiffs": 0}
            type_totals[pt]["n"] += 1
            if desc in ("swinging_strike", "swinging_strike_blocked", "foul_tip"):
                type_totals[pt]["whiffs"] += 1

            if bb_t in _BATTED_BALL_TYPES:
                total_bb += 1
                if bb_t == "ground_ball":
                    gb_count += 1

            if pnum == 1:
                fp_total += 1
                if desc in _FP_STRIKE_DESCS:
                    fp_strike_count += 1

        if total_pitches < 50:
            return {}

        pitch_mix: dict = {}
        for pt, data in type_totals.items():
            n = data["n"]
            if n < 5:
                continue
            pitch_mix[pt] = {
                "count":      n,
                "usage_pct":  round(n / total_pitches, 4),
                "whiff_rate": round(data["whiffs"] / n, 4),
            }

        gb_rate        = round(gb_count / total_bb, 4) if total_bb >= 20 else None
        fp_strike_rate = round(fp_strike_count / fp_total, 4) if fp_total >= 30 else None

        return {
            "pitch_mix":      pitch_mix,
            "gb_rate":        gb_rate,
            "fp_strike_rate": fp_strike_rate,
            "total_pitches":  total_pitches,
        }
    except Exception as e:
        print(f"  [STATCAST] pitch detail parse error: {e}")
        return {}


@lru_cache(maxsize=64)
def get_pitcher_pitch_mix(pitcher_id: int, season: int = 2026) -> dict:
    """
    Fetch per-pitch-type usage%, whiff rate, GB rate, and FP-strike rate.
    Cached per pitcher. Returns {} on failure.
    """
    if not pitcher_id:
        return {}
    for try_season in ([season, season - 1] if season == 2026 else [season]):
        url = _savant_pitcher_detail_url(pitcher_id, try_season)
        try:
            r = _http_get(
                url, timeout=30,
                headers={"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/1.0)"},
                skip_cache=False,
            )
            if r.status_code != 200 or not r.text.strip():
                continue
            result = _parse_pitch_details(r.text)
            if result:
                if try_season != season:
                    result["is_2025_data"] = True
                print(
                    f"  [STATCAST] PitchMix {pitcher_id} season={try_season}: "
                    f"gb={result.get('gb_rate')} fp={result.get('fp_strike_rate')} "
                    f"pitches={result.get('total_pitches')} "
                    f"types={list(result.get('pitch_mix', {}).keys())}"
                )
                return result
        except Exception as e:
            print(f"  [STATCAST] PitchMix fetch error {pitcher_id}: {e}")
    return {}


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        pid = int(sys.argv[1])
        print(f"Fetching pitcher {pid} Statcast...")
        sc = get_pitcher_statcast(pid)
        print(sc if sc else "No data returned")
        print("\nFetching pitch mix...")
        pm = get_pitcher_pitch_mix(pid)
        print(pm if pm else "No pitch mix data")
    else:
        print("Usage: python statcast_engine.py PITCHER_MLB_ID")
