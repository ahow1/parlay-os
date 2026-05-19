"""PARLAY OS — strikeout_engine.py
Matchup-specific K prop engine. Books price K props off season K/9.
We price them off matchup data: whiff rate, lineup K rate vs handedness,
velocity trend, and park factors.

Projection formula:
  adj_K9 = pitcher_K9 × (lineup_K_rate / LG_K_RATE) × park_K_factor
            × (1 + whiff_bonus) × ump_k_factor
  projected_K = adj_K9 × projected_IP / 9

Edge threshold: |projected_K - line| >= 0.8 AND confidence >= 60.
"""

import csv
import io
from functools import lru_cache

from api_client import get as _http_get
from constants import LG_K_RATE, LG_WHIFF_RATE, PARK_K_FACTORS

STATSAPI    = "https://statsapi.mlb.com/api/v1"
SAVANT_BASE = "https://baseballsavant.mlb.com"
TIMEOUT     = 15

MIN_K_EDGE        = 0.8    # minimum projected-vs-line gap to recommend
ELITE_WHIFF_RATE  = 0.130  # 13%+ = elite swing-and-miss stuff


# ── Savant whiff rate ─────────────────────────────────────────────────────────

def _savant_pitcher_url(pitcher_id: int, season: int) -> str:
    return (
        f"{SAVANT_BASE}/statcast_search/csv"
        f"?all=true&hfGT=R%7C&hfSea={season}%7C&player_type=pitcher"
        f"&pitchers_lookup%5B%5D={pitcher_id}"
        f"&group_by=name&min_pitches=0&min_results=0"
        f"&sort_col=pitches&sort_order=desc&type=details&min_pas=0"
    )


def _parse_whiff_rate(text: str) -> float | None:
    """Count swinging strikes / total pitches from Savant pitch-level CSV."""
    WHIFF_DESCS = {"swinging_strike", "swinging_strike_blocked", "foul_tip"}
    try:
        text = text.lstrip("﻿")
        reader = csv.DictReader(io.StringIO(text))
        total = swings = 0
        for row in reader:
            desc = (row.get("description") or "").strip()
            if not desc:
                continue
            total += 1
            if desc in WHIFF_DESCS:
                swings += 1
        if total < 50:
            return None
        return round(swings / total, 4)
    except Exception:
        return None


@lru_cache(maxsize=64)
def _fetch_whiff(pitcher_id: int, season: int) -> float | None:
    url = _savant_pitcher_url(pitcher_id, season)
    try:
        r = _http_get(
            url, timeout=TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/1.0)"},
            skip_cache=False,
        )
        if r.status_code != 200 or not r.text.strip():
            return None
        return _parse_whiff_rate(r.text)
    except Exception:
        return None


def get_pitcher_whiff_rate(pitcher_id: int) -> float | None:
    """SwStr% from Savant. Falls back to 2025 if 2026 is empty. Cached."""
    if not pitcher_id:
        return None
    wr = _fetch_whiff(pitcher_id, 2026)
    if wr is not None:
        return wr
    return _fetch_whiff(pitcher_id, 2025)


# ── Opposing lineup K rate vs SP handedness ───────────────────────────────────

@lru_cache(maxsize=60)
def _team_k_rate_raw(team_id: int, sit: str) -> float | None:
    """K/PA from MLB Stats API statSplits for 'vl' or 'vr'."""
    try:
        r = _http_get(
            f"{STATSAPI}/teams/{team_id}/stats",
            params={"stats": "statSplits", "group": "hitting",
                    "season": "2026", "sitCodes": sit},
            timeout=10,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return None
        s  = splits[0].get("stat", {})
        so = int(s.get("strikeOuts", 0) or 0)
        pa = int(s.get("plateAppearances", 0) or 0)
        if pa < 30:
            return None
        return round(so / pa, 4)
    except Exception:
        return None


def team_k_rate_vs_handedness(team_id: int, sp_hand: str) -> float:
    """Team K rate (K/PA) vs LHP or RHP. Falls back to league average."""
    sit = "vl" if sp_hand == "L" else "vr"
    result = _team_k_rate_raw(team_id, sit)
    return result if result is not None else LG_K_RATE


# ── Core projection ───────────────────────────────────────────────────────────

def project_strikeouts(
    sp_stats: dict,
    opp_team_id: int | None,
    park_code: str,
    ump_k_factor: float = 1.0,
    whiff_rate: float | None = None,
) -> dict:
    """
    Project expected strikeouts for this start.

    Returns dict with projected_k, adj_k9, ip_exp, lineup_k_rate,
    park_k_factor, whiff_rate, whiff_bonus, k9_vs_season, elite_whiff.
    """
    k9          = sp_stats.get("k9", 8.5)
    ip          = sp_stats.get("ip", 0) or 0
    gs          = sp_stats.get("gs", 1) or 1
    ttop        = sp_stats.get("ttop", False)
    sp_hand     = sp_stats.get("hand", "R")
    rolling_k9  = sp_stats.get("rolling_k9_3")

    # Expected IP this start (TTOP flag = can go deeper)
    ip_per_start = min(ip / gs if gs > 0 else 5.0, 7.0)
    if not ttop:
        ip_per_start = min(ip_per_start, 5.5)

    # Opposing lineup K rate
    lineup_k_rate = (
        team_k_rate_vs_handedness(opp_team_id, sp_hand)
        if opp_team_id else LG_K_RATE
    )

    # Park factor
    park_k_factor = PARK_K_FACTORS.get(park_code, 1.0)

    # Whiff-rate bonus
    wr = whiff_rate or 0.0
    whiff_bonus = max((wr - 0.11) * 2.0, 0.0)

    # Adjusted K/9 for this matchup
    adj_k9 = (k9
               * (lineup_k_rate / LG_K_RATE)
               * park_k_factor
               * (1.0 + whiff_bonus)
               * ump_k_factor)

    projected_k = adj_k9 * ip_per_start / 9.0

    # K-rate trend vs season (last-3 rolling)
    k9_vs_season = 0.0
    if rolling_k9 is not None:
        k9_vs_season = round(rolling_k9 - k9, 2)

    return {
        "projected_k":   round(projected_k, 2),
        "adj_k9":        round(adj_k9, 2),
        "ip_exp":        round(ip_per_start, 1),
        "lineup_k_rate": round(lineup_k_rate, 4),
        "park_k_factor": park_k_factor,
        "whiff_rate":    round(wr, 4) if wr else None,
        "whiff_bonus":   round(whiff_bonus, 4),
        "k9_vs_season":  round(k9_vs_season, 2),
        "elite_whiff":   wr >= ELITE_WHIFF_RATE if wr else False,
    }


# ── Full analysis ─────────────────────────────────────────────────────────────

def analyze_k_prop(
    sp_stats: dict,
    opp_team_id: int | None,
    park_code: str,
    market_line: float,
    ump_k_factor: float = 1.0,
) -> dict | None:
    """
    Full K prop analysis for one SP.

    Returns recommendation dict or None if edge < 0.8 or confidence < 60.
    """
    pitcher_id = sp_stats.get("pitcher_id")
    whiff_rate = get_pitcher_whiff_rate(pitcher_id) if pitcher_id else None

    proj        = project_strikeouts(sp_stats, opp_team_id, park_code, ump_k_factor, whiff_rate)
    projected_k = proj["projected_k"]
    gap         = projected_k - market_line

    if abs(gap) < MIN_K_EDGE:
        return None

    direction = "OVER" if gap > 0 else "UNDER"

    # Confidence build-up
    conf = 50
    if abs(gap) >= 1.5:
        conf += 20
    elif abs(gap) >= 0.8:
        conf += 10

    if proj.get("elite_whiff"):
        conf += 15
    if proj.get("k9_vs_season", 0) > 1.0:
        conf += 10    # recent K rate trending above season → bodes well for OVER
    elif proj.get("k9_vs_season", 0) < -1.0:
        conf -= 10    # recent decline → caution

    lineup_k = proj.get("lineup_k_rate", LG_K_RATE)
    if direction == "OVER" and lineup_k > LG_K_RATE * 1.07:
        conf += 8    # high-K lineup strengthens OVER
    if direction == "UNDER" and lineup_k < LG_K_RATE * 0.93:
        conf += 8    # low-K lineup strengthens UNDER

    conf = min(max(conf, 0), 95)
    if conf < 60:
        return None

    sp_name = sp_stats.get("name", "SP")
    return {
        "sp_name":       sp_name,
        "direction":     direction,
        "line":          market_line,
        "projected_k":   projected_k,
        "gap":           round(gap, 2),
        "confidence":    conf,
        "whiff_rate":    proj.get("whiff_rate"),
        "elite_whiff":   proj.get("elite_whiff", False),
        "lineup_k_rate": proj.get("lineup_k_rate"),
        "park_k_factor": proj.get("park_k_factor"),
        "ip_exp":        proj.get("ip_exp"),
        "adj_k9":        proj.get("adj_k9"),
        "k9_vs_season":  proj.get("k9_vs_season"),
    }


def k_prop_telegram_line(kp: dict) -> str:
    """Single-line Telegram summary for a K prop recommendation."""
    if not kp:
        return ""
    wr  = kp.get("whiff_rate")
    wr_s = f" | SwStr%={wr:.1%}" if wr else ""
    lk  = kp.get("lineup_k_rate")
    lk_s = f" | LineupK={lk:.1%}" if lk else ""
    return (
        f"⚾ K PROP — {kp['sp_name']} {kp['direction']} {kp['line']} Ks "
        f"(proj {kp['projected_k']:.1f}, gap {kp['gap']:+.1f})"
        f"{wr_s}{lk_s} | conf={kp['confidence']}/100"
    )


if __name__ == "__main__":
    import sys
    from constants import MLB_TEAM_IDS
    sp = {
        "name": "Gerrit Cole", "pitcher_id": 543037,
        "k9": 10.8, "ip": 60, "gs": 10, "ttop": True,
        "hand": "R", "rolling_k9_3": 11.5,
    }
    opp_id = MLB_TEAM_IDS.get("CWS")
    result = analyze_k_prop(sp, opp_id, "NYY", market_line=6.5)
    if result:
        print(k_prop_telegram_line(result))
    else:
        print("No K prop edge (gap < 0.8 or conf < 60)")
