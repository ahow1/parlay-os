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


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        pid = int(sys.argv[1])
        print(f"Fetching pitcher {pid} Statcast...")
        sc = get_pitcher_statcast(pid)
        print(sc if sc else "No data returned")
    else:
        print("Usage: python statcast_engine.py PITCHER_MLB_ID")
