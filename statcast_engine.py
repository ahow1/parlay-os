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


# ── Aggregated CSV parser ─────────────────────────────────────────────────────

def _parse_aggregated_csv(text: str, role: str = "pitcher") -> dict:
    """
    Parse Savant aggregated CSV (group_by=name) — one row per player.
    Columns used: exit_velocity_avg, barrel_batted_rate, hard_hit_percent,
                  release_speed (pitcher only), n_ (sample size).
    """
    try:
        reader = csv.DictReader(io.StringIO(text))
        rows   = list(reader)
        if not rows:
            return {}

        # Take first row (we requested a single player via pitchers_lookup/batter param)
        row = rows[0]

        def _flt(col: str) -> float | None:
            val = row.get(col, "")
            if not val or val in ("", "null", "NA", "nan", "."):
                return None
            try:
                return float(val)
            except ValueError:
                return None

        result: dict = {}

        ev = _flt("exit_velocity_avg")
        if ev is not None:
            result["exit_velocity_avg"] = round(ev, 1)

        # barrel_batted_rate is expressed as a percentage (0–100) in Savant CSV
        barrel = _flt("barrel_batted_rate")
        if barrel is not None:
            result["barrel_pct"] = round(barrel, 1)

        hh = _flt("hard_hit_percent")
        if hh is not None:
            result["hard_hit_pct"] = round(hh, 1)

        if role == "pitcher":
            velo = _flt("release_speed") or _flt("avg_release_speed")
            if velo is not None:
                result["avg_fastball_velo"] = round(velo, 1)

        n = _flt("n_") or _flt("pitches") or 0
        result["sample_batted_balls"] = int(n)
        return result

    except Exception:
        return {}


# ── Pitcher Statcast ──────────────────────────────────────────────────────────

@lru_cache(maxsize=64)
def get_pitcher_statcast(pitcher_id: int, season: int = 2026) -> dict:
    """
    Fetch pitcher Statcast metrics: EV allowed, barrel%, hard-hit%, avg velo.
    Cached per pitcher_id/season for the run. Returns {} on failure.
    """
    if not pitcher_id:
        return {}
    try:
        url = _savant_pitcher_url(pitcher_id, season)
        print(f"  [STATCAST] Fetching pitcher {pitcher_id} season={season}")
        r   = _http_get(
            url, timeout=TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/1.0)"},
            skip_cache=False,
        )
        if r.status_code != 200 or not r.text.strip():
            print(f"  [STATCAST] Pitcher {pitcher_id}: HTTP {r.status_code} or empty response")
            return {}
        data = _parse_aggregated_csv(r.text, "pitcher")
        if not data:
            print(f"  [STATCAST] Pitcher {pitcher_id}: CSV parsed but no usable columns")
            return {}
        # Validate: discard if sample is zero or values match Savant placeholder defaults
        n      = data.get("sample_batted_balls", 0)
        ev     = data.get("exit_velocity_avg")
        barrel = data.get("barrel_pct")
        if n == 0:
            print(f"  [STATCAST] Pitcher {pitcher_id}: sample=0 — no real data, discarding")
            return {}
        if ev == _SAVANT_DEFAULT_EV and barrel == _SAVANT_DEFAULT_BARREL:
            print(f"  [STATCAST] Pitcher {pitcher_id}: EV={ev} barrel={barrel} matches "
                  f"known placeholder defaults — discarding (lookup likely failed silently)")
            return {}
        data["pitcher_id"] = pitcher_id
        print(f"  [STATCAST] Pitcher {pitcher_id}: OK — "
              f"EV={ev} barrel={barrel} n={n}")
        return data
    except Exception as e:
        print(f"  [STATCAST] Pitcher {pitcher_id}: exception — {e}")
        return {}


# ── Batter Statcast ───────────────────────────────────────────────────────────

@lru_cache(maxsize=128)
def get_batter_statcast(batter_id: int, season: int = 2026) -> dict:
    """
    Fetch batter Statcast metrics: EV, barrel%, hard-hit%.
    Cached per batter_id/season. Returns {} on failure.
    """
    if not batter_id:
        return {}
    try:
        url = _savant_batter_url(batter_id, season)
        r   = _http_get(
            url, timeout=TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/1.0)"},
            skip_cache=False,
        )
        if r.status_code != 200 or not r.text.strip():
            print(f"  [STATCAST] Batter {batter_id}: HTTP {r.status_code} or empty response")
            return {}
        data = _parse_aggregated_csv(r.text, "batter")
        if not data:
            return {}
        n      = data.get("sample_batted_balls", 0)
        ev     = data.get("exit_velocity_avg")
        barrel = data.get("barrel_pct")
        if n == 0:
            print(f"  [STATCAST] Batter {batter_id}: sample=0 — no real data, discarding")
            return {}
        if ev == _SAVANT_DEFAULT_EV and barrel == _SAVANT_DEFAULT_BARREL:
            print(f"  [STATCAST] Batter {batter_id}: EV={ev} barrel={barrel} matches "
                  f"placeholder defaults — discarding")
            return {}
        data["batter_id"] = batter_id
        return data
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
