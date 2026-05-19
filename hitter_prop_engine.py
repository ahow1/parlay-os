"""PARLAY OS — hitter_prop_engine.py
Hitter prop engine using Statcast expected statistics.

Edge: xwOBA >> wOBA means the hitter is UNLUCKY — quality contact not producing
expected results. xBA >> actual BA = same signal at the BA level.
We use xSLG to project total bases.

Data source: Baseball Savant expected_statistics leaderboard (season-level).

Projection:
  proj_hits_per_game ≈ xBA × 3.5 (assuming ~3.5 AB/game)
  proj_TB_per_game   ≈ xSLG × 3.5

Edge threshold:
  Hits:        proj > line by 0.3+ AND edge >= 8%
  Total bases: proj > line by 0.3+ AND edge >= 8%
  xwOBA gap:   xwOBA - wOBA >= 0.040 → underperforming, prime candidate
"""

import csv
import io
import threading
from functools import lru_cache

from api_client import get as _http_get
from constants import PARK_FACTORS

SAVANT_BASE    = "https://baseballsavant.mlb.com"
TIMEOUT        = 20
MIN_PROJ_EDGE  = 0.30    # minimum projected vs line gap
MIN_EDGE_PCT   = 0.08    # minimum model - market probability gap
XWOBA_GAP_MIN  = 0.040   # 40-point xwOBA-wOBA gap = underperforming

# Thread-safe cache for the full Savant leaderboard
_savant_lock  = threading.Lock()
_savant_cache: dict | None = None   # {player_id: {xba, xslg, xwoba, woba, ...}}


# ── Savant expected statistics leaderboard ────────────────────────────────────

def _fetch_expected_stats_leaderboard(season: int = 2026) -> dict:
    """
    Fetch Baseball Savant expected statistics leaderboard and return
    {player_id: {xba, xslg, xwoba, woba, ...}} mapping.
    """
    global _savant_cache
    with _savant_lock:
        if _savant_cache is not None:
            return _savant_cache
        url = (
            f"{SAVANT_BASE}/expected_statistics"
            f"?type=batter&year={season}&position=&team=&min=25&csv=true"
        )
        try:
            r = _http_get(
                url, timeout=TIMEOUT,
                headers={"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/1.0)"},
                skip_cache=False,
            )
            if r.status_code != 200 or not r.text.strip():
                _savant_cache = {}
                return {}
            result = _parse_expected_stats_csv(r.text)
        except Exception as e:
            print(f"  [HITTER] Savant xStats fetch failed: {e}")
            result = {}

        if not result and season == 2026:
            # Try 2025 fallback
            url25 = url.replace(f"year={season}", "year=2025")
            try:
                r2 = _http_get(
                    url25, timeout=TIMEOUT,
                    headers={"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/1.0)"},
                    skip_cache=False,
                )
                if r2.status_code == 200 and r2.text.strip():
                    result = _parse_expected_stats_csv(r2.text)
                    for v in result.values():
                        v["SAVANT_2025"] = True
            except Exception:
                pass

        _savant_cache = result
        return result


def _parse_expected_stats_csv(text: str) -> dict:
    """Parse Savant expected stats CSV. Returns {player_id: stats_dict}."""
    def _flt(val, default=0.0) -> float:
        try:
            return float(val) if val not in ("", "null", "NA", ".", None) else default
        except (ValueError, TypeError):
            return default

    result = {}
    try:
        text = text.lstrip("﻿")
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            pid_raw = row.get("player_id") or row.get("batter") or ""
            if not pid_raw:
                continue
            try:
                pid = int(str(pid_raw).strip())
            except ValueError:
                continue

            name  = (row.get("player_name") or row.get("last_name, first_name") or "").strip()
            xba   = _flt(row.get("xba")   or row.get("est_ba"))
            xslg  = _flt(row.get("xslg")  or row.get("est_slg"))
            xwoba = _flt(row.get("xwoba") or row.get("est_woba"))
            woba  = _flt(row.get("woba"))
            ba    = _flt(row.get("ba"))
            pa    = int(_flt(row.get("pa") or row.get("ab"), 0))

            if pa < 25 or xba <= 0:
                continue

            result[pid] = {
                "name":  name,
                "xba":   xba,
                "xslg":  xslg,
                "xwoba": xwoba,
                "woba":  woba,
                "ba":    ba,
                "pa":    pa,
                "xwoba_gap": round(xwoba - woba, 4) if woba else 0.0,
                "xba_gap":   round(xba - ba, 4) if ba else 0.0,
            }
    except Exception as e:
        print(f"  [HITTER] CSV parse error: {e}")
    return result


def get_player_xstats(player_id: int) -> dict | None:
    """Fetch expected stats for a single batter. Returns None if unavailable."""
    if not player_id:
        return None
    leaderboard = _fetch_expected_stats_leaderboard()
    return leaderboard.get(player_id)


# ── Prop projections ──────────────────────────────────────────────────────────

def _proj_pa_per_game(batting_order: int) -> float:
    """Estimated PA per game by lineup slot (top of order gets more ABs)."""
    slot_pa = {1: 4.4, 2: 4.3, 3: 4.2, 4: 4.1, 5: 3.9,
               6: 3.8, 7: 3.7, 8: 3.6, 9: 3.5}
    return slot_pa.get(batting_order, 3.8)


def project_hits(xstats: dict, batting_order: int = 5,
                 park_factor: float = 1.0,
                 opp_sp_ev_allowed: float | None = None) -> float:
    """
    Projected hits per game using xBA.
    xBA > 0.280 vs SP with EV_allowed > 85 mph = strong hit prop signal.
    """
    xba  = xstats.get("xba", 0.25)
    pa   = _proj_pa_per_game(batting_order)
    ab   = pa * 0.88   # rough AB per PA (excludes walks/HBP)

    # SP EV adjustment: softer contact allowed → hit rate boost
    ev_adj = 1.0
    if opp_sp_ev_allowed is not None:
        # League avg EV allowed ≈ 88.0 mph; >88 = giving up harder contact
        ev_adj = 1.0 + max((opp_sp_ev_allowed - 88.0) / 88.0 * 0.3, -0.15)

    proj = xba * ab * park_factor * ev_adj
    return round(proj, 3)


def project_total_bases(xstats: dict, batting_order: int = 5,
                        park_factor: float = 1.0,
                        opp_sp_ev_allowed: float | None = None) -> float:
    """
    Projected total bases per game using xSLG × projected AB.
    xSLG = total_bases / at_bats (expected), so proj_TB = xSLG × AB.
    """
    xslg = xstats.get("xslg", 0.40)
    pa   = _proj_pa_per_game(batting_order)
    ab   = pa * 0.88

    ev_adj = 1.0
    if opp_sp_ev_allowed is not None:
        ev_adj = 1.0 + max((opp_sp_ev_allowed - 88.0) / 88.0 * 0.35, -0.15)

    proj = xslg * ab * park_factor * ev_adj
    return round(proj, 3)


def _implied_over_prob(proj: float, line: float, std_dev: float) -> float:
    """
    P(stat > line) using a normal approximation.
    std_dev for hits ≈ 0.90, for TB ≈ 1.20.
    """
    import math
    z = (line + 0.5 - proj) / max(std_dev, 0.01)
    # Standard normal CDF approximation
    def _phi(x):
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))
    return round(1.0 - _phi(z), 4)


# ── Full analysis ─────────────────────────────────────────────────────────────

def analyze_hitter_prop(
    player_id: int,
    player_name: str,
    team_code: str,
    batting_order: int,
    prop_type: str,           # "hits" or "total_bases"
    market_line: float,
    market_odds: str,
    park_code: str,
    opp_sp_ev_allowed: float | None = None,
) -> dict | None:
    """
    Full hitter prop analysis.

    Returns recommendation dict or None if no edge.
    prop_type: 'hits' or 'total_bases'
    """
    from math_engine import implied_prob

    xstats = get_player_xstats(player_id)
    if not xstats:
        return None

    park_rf = PARK_FACTORS.get(park_code, 1.0)

    if prop_type == "hits":
        proj   = project_hits(xstats, batting_order, park_rf, opp_sp_ev_allowed)
        std_dv = 0.90
    else:  # total_bases
        proj   = project_total_bases(xstats, batting_order, park_rf, opp_sp_ev_allowed)
        std_dv = 1.20

    gap = proj - market_line
    if abs(gap) < MIN_PROJ_EDGE:
        return None

    direction = "OVER" if gap > 0 else "UNDER"

    # Model probability
    model_p = _implied_over_prob(proj, market_line, std_dv)
    if direction == "UNDER":
        model_p = 1.0 - model_p

    # Market probability
    mkt_p_raw = implied_prob(market_odds)
    if mkt_p_raw is None:
        return None
    mkt_p = mkt_p_raw / 100.0

    edge_pct = round((model_p - mkt_p) * 100, 2)
    if edge_pct < MIN_EDGE_PCT * 100:
        return None

    # Confidence
    conf = 50
    xwoba_gap = xstats.get("xwoba_gap", 0.0)
    if xwoba_gap >= XWOBA_GAP_MIN:
        conf += 15    # clearly underperforming expected contact quality
    if abs(gap) >= 0.5:
        conf += 10
    if edge_pct >= 12:
        conf += 10
    elif edge_pct >= 8:
        conf += 5
    if xstats.get("xba", 0) >= 0.280 and opp_sp_ev_allowed and opp_sp_ev_allowed >= 88.5:
        conf += 8     # xBA + soft SP = hit prop signal
    conf = min(max(conf, 0), 90)
    if conf < 60:
        return None

    return {
        "player":         player_name,
        "player_id":      player_id,
        "team":           team_code,
        "prop_type":      prop_type,
        "direction":      direction,
        "line":           market_line,
        "projected":      proj,
        "gap":            round(gap, 3),
        "model_p":        round(model_p, 4),
        "market_p":       round(mkt_p, 4),
        "edge_pct":       edge_pct,
        "confidence":     conf,
        "xba":            xstats.get("xba"),
        "xslg":           xstats.get("xslg"),
        "xwoba":          xstats.get("xwoba"),
        "woba":           xstats.get("woba"),
        "xwoba_gap":      xwoba_gap,
        "park_code":      park_code,
        "batting_order":  batting_order,
        "is_2025_data":   xstats.get("SAVANT_2025", False),
    }


def hitter_prop_telegram_line(hp: dict) -> str:
    """Single-line Telegram summary for a hitter prop recommendation."""
    if not hp:
        return ""
    xwoba_gap = hp.get("xwoba_gap", 0.0)
    gap_s = f" | xwOBA gap {xwoba_gap:+.3f}" if abs(xwoba_gap) >= XWOBA_GAP_MIN else ""
    return (
        f"🏏 {hp['prop_type'].upper().replace('_',' ')} — {hp['player']} "
        f"{hp['direction']} {hp['line']} (proj {hp['projected']:.2f})"
        f"{gap_s} | edge {hp['edge_pct']:+.1f}% | conf={hp['confidence']}/100"
    )


def scan_lineup_props(
    lineup: list,
    park_code: str,
    opp_sp_ev_allowed: float | None,
    prop_type: str,
    market_data: dict,
    team_code: str,
) -> list[dict]:
    """
    Scan a full lineup for hitter prop edges.
    lineup: list of {id, name, batting_order} dicts from offense_engine.
    market_data: {player_id: {line, odds}} from props market.
    Returns list of analyze_hitter_prop results sorted by edge_pct desc.
    """
    results = []
    for player in lineup:
        pid   = player.get("id")
        name  = player.get("name", "")
        order = player.get("batting_order", 5)
        mkt   = market_data.get(pid)
        if not mkt:
            continue
        line  = mkt.get("line")
        odds  = mkt.get("odds", "-110")
        if line is None:
            continue
        rec = analyze_hitter_prop(
            player_id=pid, player_name=name, team_code=team_code,
            batting_order=order, prop_type=prop_type,
            market_line=line, market_odds=str(odds),
            park_code=park_code, opp_sp_ev_allowed=opp_sp_ev_allowed,
        )
        if rec:
            results.append(rec)
    results.sort(key=lambda x: x["edge_pct"], reverse=True)
    return results


if __name__ == "__main__":
    leaderboard = _fetch_expected_stats_leaderboard()
    print(f"Savant leaderboard: {len(leaderboard)} batters loaded")
    if leaderboard:
        pid, data = next(iter(leaderboard.items()))
        print(f"Sample: ID={pid} name={data.get('name')} xBA={data.get('xba')} "
              f"xwOBA={data.get('xwoba')} wOBA={data.get('woba')} "
              f"gap={data.get('xwoba_gap'):+.4f}")
