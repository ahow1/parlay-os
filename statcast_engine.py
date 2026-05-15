"""PARLAY OS — statcast_engine.py
Lightweight Statcast data via Baseball Savant aggregated leaderboards.
Provides exit velocity, barrel%, hard-hit%, sprint speed for SPs and hitters.

Note: Baseball Savant's CSV endpoint is undocumented; falls back gracefully if unavailable.
"""

import csv
import io
import requests
from api_client import get as _http_get
from functools import lru_cache

SAVANT_BASE = "https://baseballsavant.mlb.com"
TIMEOUT     = 15

# ── Pitcher Statcast (exit velocity allowed, barrel%, hard-hit%) ──────────────

def _savant_pitcher_url(pitcher_id: int, season: int = 2026) -> str:
    # pitchers_lookup%5B%5D is the correct filter param (pitcher= is ignored by Savant).
    # Omit group_by=name so the response contains raw pitch rows with launch_speed /
    # release_speed columns rather than a pre-aggregated league-wide summary row.
    return (
        f"{SAVANT_BASE}/statcast_search/csv"
        f"?all=true&hfGT=R%7C&hfSea={season}%7C&player_type=pitcher"
        f"&pitchers_lookup%5B%5D={pitcher_id}&min_pitches=0"
        f"&min_results=0&sort_col=pitches&sort_order=desc&type=details&min_pas=0"
    )


def _savant_batter_url(batter_id: int, season: int = 2026) -> str:
    return (
        f"{SAVANT_BASE}/statcast_search/csv"
        f"?all=true&hfGT=R%7C&hfSea={season}%7C&player_type=batter"
        f"&batter={batter_id}&group_by=name&min_pitches=0"
        f"&min_results=0&sort_col=pitches&sort_order=desc&type=details&min_pas=0"
    )


def _parse_statcast_csv(text: str, role: str = "pitcher") -> dict:
    """
    Parse raw Statcast CSV rows into aggregate metrics.
    role='pitcher' → exit velocity / barrel allowed
    role='batter'  → exit velocity / barrel / sprint_speed
    """
    try:
        reader = csv.DictReader(io.StringIO(text))
        rows   = list(reader)
        if not rows:
            return {}

        launch_speeds = []
        barrels       = 0
        hard_hits     = 0
        total_batted  = 0
        velocities    = []

        for row in rows:
            ls = row.get("launch_speed", "")
            if ls and ls not in ("", "null", "NA"):
                try:
                    val = float(ls)
                    launch_speeds.append(val)
                    total_batted += 1
                    if val >= 95:
                        hard_hits += 1
                    # barrel proxy: ≥98 mph + launch angle 26-30°
                    la = row.get("launch_angle", "")
                    if la and la not in ("", "null", "NA"):
                        try:
                            la_val = float(la)
                            if val >= 98 and 26 <= la_val <= 30:
                                barrels += 1
                        except ValueError:
                            pass
                except ValueError:
                    pass
            if role == "pitcher":
                rv = row.get("release_speed", "")
                if rv and rv not in ("", "null", "NA"):
                    try:
                        velocities.append(float(rv))
                    except ValueError:
                        pass

        result: dict = {}
        if total_batted > 0:
            result["exit_velocity_avg"]  = round(sum(launch_speeds) / total_batted, 1)
            result["hard_hit_pct"]       = round(hard_hits / total_batted * 100, 1)
            result["barrel_pct"]         = round(barrels / total_batted * 100, 1)
        if velocities:
            result["avg_fastball_velo"]  = round(sum(velocities) / len(velocities), 1)

        # Sprint speed only available in player bio, not pitch-level CSV
        result["sample_batted_balls"] = total_batted
        return result
    except Exception:
        return {}


@lru_cache(maxsize=64)
def get_pitcher_statcast(pitcher_id: int, season: int = 2026) -> dict:
    """
    Fetch pitcher Statcast metrics: EV allowed, barrel%, hard-hit%, avg velo.
    Cached per pitcher_id/season for the run. Returns {} on failure.
    """
    try:
        url = _savant_pitcher_url(pitcher_id, season)
        r   = _http_get(url, timeout=TIMEOUT,
                           headers={"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/1.0)"})
        if r.status_code != 200 or not r.text.strip():
            return {}
        data = _parse_statcast_csv(r.text, "pitcher")
        data["pitcher_id"] = pitcher_id
        return data
    except Exception:
        return {}


@lru_cache(maxsize=128)
def get_batter_statcast(batter_id: int, season: int = 2026) -> dict:
    """
    Fetch batter Statcast metrics: EV, barrel%, hard-hit%.
    Cached per batter_id/season. Returns {} on failure.
    """
    try:
        url = _savant_batter_url(batter_id, season)
        r   = _http_get(url, timeout=TIMEOUT,
                           headers={"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/1.0)"})
        if r.status_code != 200 or not r.text.strip():
            return {}
        data = _parse_statcast_csv(r.text, "batter")
        data["batter_id"] = batter_id
        return data
    except Exception:
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
