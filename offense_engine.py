"""PARLAY OS — offense_engine.py
Fetches team offensive stats, lineup confirmation, platoon splits, RISP, recent form.
"""

import requests
from api_client import get as _http_get
from datetime import date, timedelta
from constants import MLB_TEAM_IDS, TEAM_LHB_PCT, LG_RPG, PARK_FACTORS

STATSAPI = "https://statsapi.mlb.com/api/v1"

# League-average OPS used for wRC+ proxy
_LG_OPS = 0.730


def _wrc_plus_proxy(ops: float, park_factor: float = 1.0) -> float:
    """Approximate wRC+ from OPS, adjusted for park factor."""
    if ops <= 0:
        return 100.0
    ops_adj = ops / max(park_factor, 0.80)
    return round((ops_adj / _LG_OPS) * 100, 1)


# ── Rolling N-day wRC+ ────────────────────────────────────────────────────────

def _rolling_hitting_window(team_id: int, days: int = 14,
                             park_factor: float = 1.0) -> dict:
    """
    Hitting stats over the last N days from MLB Stats API (byDateRange).
    Returns wRC+ proxy, PA count, RPG, and a flag if sample < 50 PA.
    """
    end_dt   = date.today()
    start_dt = end_dt - timedelta(days=days)
    try:
        r = _http_get(
            f"{STATSAPI}/teams/{team_id}/stats",
            params={
                "stats":     "byDateRange",
                "group":     "hitting",
                "gameType":  "R",
                "season":    "2026",
                "startDate": start_dt.isoformat(),
                "endDate":   end_dt.isoformat(),
            },
            timeout=10,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s     = splits[0].get("stat", {})
        pa    = int(s.get("plateAppearances", 0) or 0)
        obp   = float(s.get("obp", 0.320) or 0.320)
        slg   = float(s.get("slg", 0.410) or 0.410)
        ops   = float(s.get("ops", 0.730) or 0.730)
        runs  = int(s.get("runs", 0) or 0)
        games = int(s.get("gamesPlayed", 1) or 1)
        return {
            "pa":          pa,
            "wrc_plus":    _wrc_plus_proxy(ops, park_factor),
            "obp":         obp,
            "slg":         slg,
            "ops":         ops,
            "rpg":         round(runs / games, 2) if games > 0 else LG_RPG,
            "games":       games,
            "window_days": days,
            "low_sample":  pa < 50,
        }
    except Exception:
        return {}


def _wrc_plus_rolling(team_id: int, park_factor: float = 1.0) -> dict:
    """
    Rolling wRC+ with automatic 14→30-day fallback when fewer than 50 PA.
    Returns dict with wrc_plus, window_days, low_sample flag.
    """
    result = _rolling_hitting_window(team_id, 14, park_factor)
    if not result:
        result = _rolling_hitting_window(team_id, 30, park_factor)
    elif result.get("low_sample"):
        fallback = _rolling_hitting_window(team_id, 30, park_factor)
        if fallback:
            result = fallback
    return result


# ── Real platoon splits ───────────────────────────────────────────────────────

def _platoon_splits_real(team_id: int) -> dict:
    """
    Actual wRC+ vs LHP and vs RHP from MLB Stats API sitCodes.
    Returns {vs_lhp: {wrc_plus, ops, obp, slg}, vs_rhp: {...}}.
    """
    result: dict = {}
    for sit, key in (("vl", "vs_lhp"), ("vr", "vs_rhp")):
        try:
            r = _http_get(
                f"{STATSAPI}/teams/{team_id}/stats",
                params={
                    "stats":    "statSplits",
                    "group":    "hitting",
                    "season":   "2026",
                    "sitCodes": sit,
                },
                timeout=10,
            )
            splits = r.json().get("stats", [{}])[0].get("splits", [])
            if not splits:
                continue
            s   = splits[0].get("stat", {})
            ops = float(s.get("ops", 0.730) or 0.730)
            obp = float(s.get("obp", 0.320) or 0.320)
            slg = float(s.get("slg", 0.410) or 0.410)
            result[key] = {
                "wrc_plus": _wrc_plus_proxy(ops),
                "ops":      ops,
                "obp":      obp,
                "slg":      slg,
            }
        except Exception:
            pass
    return result


def _platoon_adjustment_real(splits: dict, opp_sp_hand: str) -> tuple[float, float]:
    """
    Return (adj_wrc_plus, platoon_delta) using real splits.
    Falls back to generic estimate if splits unavailable.
    """
    from constants import PLATOON_WRCPLUS_DELTA
    key = "vs_lhp" if opp_sp_hand == "L" else "vs_rhp"
    split_data = splits.get(key, {})
    if split_data:
        adj_wrc = split_data.get("wrc_plus", 100.0)
        # baseline wRC+ from season (assume 100 if unknown)
        baseline = 100.0
        return adj_wrc, round(adj_wrc - baseline, 1)
    # Generic fallback
    lhb_pct = 0.43
    lhb_delta = PLATOON_WRCPLUS_DELTA.get(("L", opp_sp_hand), 0)
    rhb_delta = PLATOON_WRCPLUS_DELTA.get(("R", opp_sp_hand), 0)
    delta = lhb_pct * lhb_delta + (1 - lhb_pct) * rhb_delta
    return round(100.0 + delta, 1), round(delta, 1)


# ── Season hitting stats (fallback) ──────────────────────────────────────────

def _team_hitting_stats(team_id: int) -> dict:
    """Season team hitting stats from Stats API."""
    try:
        r      = _http_get(
            f"{STATSAPI}/teams/{team_id}/stats",
            params={"stats": "season", "group": "hitting", "season": "2026"},
            timeout=8,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s = splits[0].get("stat", {})
        return {
            "avg":   float(s.get("avg", 0.250) or 0.250),
            "obp":   float(s.get("obp", 0.320) or 0.320),
            "slg":   float(s.get("slg", 0.410) or 0.410),
            "ops":   float(s.get("ops", 0.730) or 0.730),
            "runs":  int(s.get("runs", 0) or 0),
            "games": int(s.get("gamesPlayed", 1) or 1),
        }
    except Exception:
        return {}


def _recent_form(team_id: int, last_n: int = 7) -> dict:
    """Runs/game over last N games."""
    try:
        r      = _http_get(
            f"{STATSAPI}/teams/{team_id}/stats",
            params={
                "stats":    "lastXGames",
                "group":    "hitting",
                "season":   "2026",
                "gameType": "R",
                "limit":    last_n,
            },
            timeout=8,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s     = splits[0].get("stat", {})
        games = int(s.get("gamesPlayed", last_n) or last_n)
        runs  = int(s.get("runs", 0) or 0)
        return {
            "games":         games,
            "runs":          runs,
            "runs_per_game": round(runs / games, 2) if games else LG_RPG,
        }
    except Exception:
        return {}


def _risp_stats(team_id: int) -> dict:
    """Runners in scoring position OPS — proxy from splits."""
    try:
        r      = _http_get(
            f"{STATSAPI}/teams/{team_id}/stats",
            params={
                "stats":    "statSplits",
                "group":    "hitting",
                "season":   "2026",
                "sitCodes": "RISP",
            },
            timeout=8,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s   = splits[0].get("stat", {})
        ops = float(s.get("ops", 0.730) or 0.730)
        avg = float(s.get("avg", 0.250) or 0.250)
        return {"risp_avg": avg, "risp_ops": ops}
    except Exception:
        return {}


def _team_recent_record(team_id: int, days: int = 7) -> dict:
    """W-L record over the last N days."""
    from datetime import date as _date, timedelta as _td
    end   = _date.today().isoformat()
    start = (_date.today() - _td(days=days)).isoformat()
    try:
        r = _http_get(
            f"{STATSAPI}/schedule",
            params={"teamId": team_id, "sportId": 1, "gameType": "R",
                    "startDate": start, "endDate": end},
            timeout=10,
        )
        wins = losses = 0
        for d in r.json().get("dates", []):
            for g in d.get("games", []):
                if "Final" not in (g.get("status") or {}).get("detailedState", ""):
                    continue
                for side in ("away", "home"):
                    t = g.get("teams", {}).get(side, {})
                    if t.get("team", {}).get("id") == team_id:
                        if t.get("isWinner"):
                            wins += 1
                        else:
                            losses += 1
        total = wins + losses
        return {"wins": wins, "losses": losses, "games": total,
                "win_pct": round(wins / total, 3) if total > 0 else 0.5}
    except Exception:
        return {"wins": 0, "losses": 0, "games": 0, "win_pct": 0.5}


# ── Lineup confirmation ───────────────────────────────────────────────────────

def _lineup_from_schedule(game_pk: int) -> dict:
    """
    Pull confirmed lineup from schedule hydrate=lineups.
    Returns {away: [...], home: [...], confirmed: bool}.
    """
    out = {"away": [], "home": [], "confirmed": False}
    if not game_pk:
        return out
    try:
        r = _http_get(
            f"{STATSAPI}/schedule",
            params={
                "gamePk":  game_pk,
                "hydrate": "lineups",
                "sportId": 1,
            },
            timeout=8,
        )
        for day in r.json().get("dates", []):
            for g in day.get("games", []):
                if g.get("gamePk") != game_pk:
                    continue
                lineups = g.get("lineups", {})
                if not lineups:
                    break
                out["confirmed"] = True
                for side in ("away", "home"):
                    for p in lineups.get(f"{side}Players", []):
                        out[side].append({
                            "id":            p.get("id"),
                            "name":          p.get("fullName", ""),
                            "batting_order": p.get("battingOrder", 0),
                        })
                break
    except Exception:
        pass
    return out


def _lineup_from_boxscore(game_pk: int, side: str) -> list:
    """Extract confirmed batting order from today's boxscore (if posted)."""
    try:
        r   = _http_get(f"{STATSAPI}/game/{game_pk}/boxscore", timeout=8)
        box = r.json()
        t   = box.get("teams", {}).get(side, {})
        order = t.get("battingOrder", [])
        info  = t.get("players", {})
        lineup = []
        for pid in order:
            key    = f"ID{pid}"
            player = info.get(key, {}).get("person", {})
            lineup.append({
                "id":   pid,
                "name": player.get("fullName", ""),
            })
        return lineup
    except Exception:
        return []


# ── Lineup slot quality ───────────────────────────────────────────────────────

def _player_season_ops(player_id: int) -> float | None:
    """Fetch a hitter's season OPS from MLB Stats API. Returns None on failure."""
    try:
        r = _http_get(
            f"{STATSAPI}/people/{player_id}/stats",
            params={"stats": "season", "group": "hitting", "season": "2026"},
            timeout=8,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return None
        s   = splits[0].get("stat", {})
        ops = float(s.get("ops", 0) or 0)
        return ops if ops > 0 else None
    except Exception:
        return None


def lineup_slot_quality(lineup: list, park_factor: float = 1.0) -> dict:
    """
    Compute combined wRC+ for batting order slots 1-3 vs 4-6.
    lineup: list of {id, name, batting_order} dicts.

    Returns:
        top_order_wrc:   float — combined wRC+ for spots 1-3
        mid_order_wrc:   float — combined wRC+ for spots 4-6
        slot_run_adj:    float — run expectancy adjustment (-0.15 to +0.15)
        tag:             str   — flag string for Telegram
        elite_top_order: bool
        weak_top_order:  bool
    """
    result = {
        "top_order_wrc":   None,
        "mid_order_wrc":   None,
        "slot_run_adj":    0.0,
        "tag":             "",
        "elite_top_order": False,
        "weak_top_order":  False,
    }

    if not lineup:
        return result

    # Sort by batting_order; filter to slots 1-6
    sorted_lu = sorted(lineup, key=lambda p: p.get("batting_order", 99))
    top3 = [p for p in sorted_lu if 1 <= p.get("batting_order", 0) <= 3][:3]
    mid3 = [p for p in sorted_lu if 4 <= p.get("batting_order", 0) <= 6][:3]

    if len(top3) < 2 and len(mid3) < 2:
        return result   # not enough confirmed lineup data

    def _wrc_list(players: list) -> list[float]:
        wrc_vals = []
        for p in players:
            pid = p.get("id")
            if not pid:
                continue
            ops = _player_season_ops(pid)
            if ops:
                wrc_vals.append(_wrc_plus_proxy(ops, park_factor))
        return wrc_vals

    top_vals = _wrc_list(top3)
    mid_vals = _wrc_list(mid3)

    if not top_vals:
        return result

    top_wrc = round(sum(top_vals), 1)
    mid_wrc = round(sum(mid_vals), 1) if mid_vals else None
    result["top_order_wrc"] = top_wrc
    result["mid_order_wrc"] = mid_wrc

    if top_wrc > 380:
        result["slot_run_adj"]    = 0.15
        result["elite_top_order"] = True
        result["tag"]             = f"⚡ LINEUP DEPTH: elite top-3 wRC+ {top_wrc:.0f} — top-order runs pressure"
    elif top_wrc < 260:
        result["slot_run_adj"]    = -0.15
        result["weak_top_order"]  = True
        result["tag"]             = f"⚠ LINEUP WEAKNESS: weak top-3 wRC+ {top_wrc:.0f}"

    return result


# ── Main analysis ─────────────────────────────────────────────────────────────

def analyze_offense(team_code: str, game_pk: int = None, side: str = "away",
                    opp_sp_hand: str = "R") -> dict:
    """
    Full offensive analysis for a team.
    Uses rolling 14-day wRC+ (falls back to 30-day if <50 PA),
    real platoon splits, and confirmed lineup data.
    """
    team_id    = MLB_TEAM_IDS.get(team_code)
    park_factor = PARK_FACTORS.get(team_code, 1.0)
    if not team_id:
        return _default_offense(team_code)

    # ── Recency-weighted wRC+: 40% last 7d / 35% last 30d / 25% season ──────
    w7  = _rolling_hitting_window(team_id,  7, park_factor)
    w30 = _rolling_hitting_window(team_id, 30, park_factor)
    # Season wRC+ pulled from season stats below; use w30 as placeholder for now
    wrc7  = w7.get("wrc_plus", 100.0)
    wrc30 = w30.get("wrc_plus", 100.0)
    rpg7  = w7.get("rpg", LG_RPG)
    rpg30 = w30.get("rpg", LG_RPG)
    low_sample_7d   = w7.get("low_sample", True) or w7.get("games", 0) < 3
    window_days     = 7 if not low_sample_7d else 30
    low_sample_flag = low_sample_7d and w30.get("low_sample", False)

    # Recent win/loss records for form weighting
    record7  = _team_recent_record(team_id, 7)
    record30 = _team_recent_record(team_id, 30)

    # ── Real platoon splits ───────────────────────────────────────────────────
    splits       = _platoon_splits_real(team_id)
    adj_wrc_plus, platoon_delta = _platoon_adjustment_real(splits, opp_sp_hand)

    # ── Season stats (for ratio calculations) ────────────────────────────────
    hitting  = _team_hitting_stats(team_id)
    ops      = hitting.get("ops", 0.730)
    wrc_plus_season = _wrc_plus_proxy(ops, park_factor)

    # Aggregate missing-data flag (AUDIT.md M2) — true on a genuine total
    # feed blackout (both rolling windows AND season stats came back empty),
    # not a normal low-sample edge case (which wrc_low_sample already flags).
    # Mirrors sp_missing / bullpen's data_ok so brain.py can suppress this
    # factor from the win-prob blend instead of trusting fabricated defaults.
    offense_missing = (not w7 and not w30) or not hitting

    # Recency-weighted wRC+ blend
    wrc_plus_14d = round(0.40 * wrc7 + 0.35 * wrc30 + 0.25 * wrc_plus_season, 1)
    rolling_rpg  = round(0.40 * rpg7 + 0.35 * rpg30 + 0.25 * (hitting.get("runs", 0) / max(hitting.get("games", 1), 1)), 2)

    # Blend rolling wRC+ with platoon adjustment. Must come AFTER the real
    # wrc_plus_14d assignment above — previously used a hardcoded 100
    # placeholder here and was never recomputed (AUDIT.md B8), so real team
    # offensive form barely reached run_factor / the win-prob blend.
    wrc_plus_adj = round(wrc_plus_14d + platoon_delta, 1)

    # ── RISP ─────────────────────────────────────────────────────────────────
    risp     = _risp_stats(team_id)

    # ── Lineup confirmation ───────────────────────────────────────────────────
    lineup_confirmed  = False
    lineup            = []
    lineup_unconfirmed_penalty = 0.0  # confidence reduction

    if game_pk:
        sched_lineup = _lineup_from_schedule(game_pk)
        if sched_lineup["confirmed"]:
            lineup_confirmed = True
            lineup = sched_lineup.get(side, [])
        else:
            # Fallback: boxscore batting order (works closer to game time)
            bx = _lineup_from_boxscore(game_pk, side)
            if bx:
                lineup_confirmed = True
                lineup = bx

        if not lineup_confirmed:
            lineup_unconfirmed_penalty = 0.10  # 10% confidence reduction

        import data_health
        data_health.record_ok("lineups", lineup_confirmed)

    # ── Lineup slot quality (if lineup is confirmed) ──────────────────────────
    slot_quality = lineup_slot_quality(lineup, park_factor) if lineup else {}

    # ── Run expectancy factor ─────────────────────────────────────────────────
    run_factor = round(wrc_plus_adj / 100, 4)

    # Form adjustment using recency-weighted RPG vs season RPG
    rpg_season = hitting.get("runs", 0) / max(hitting.get("games", 1), 1)
    if rolling_rpg > 0 and rpg_season > 0:
        form_adj   = min((rolling_rpg / rpg_season) ** 0.25, 1.10)
        run_factor = round(run_factor * form_adj, 4)

    # Platoon matchup edge flag (15+ wRC+ points = real edge)
    vs_today_wrc    = splits.get("vs_lhp" if opp_sp_hand == "L" else "vs_rhp", {}).get("wrc_plus", 100.0)
    vs_opp_wrc      = splits.get("vs_rhp" if opp_sp_hand == "L" else "vs_lhp", {}).get("wrc_plus", 100.0)
    platoon_edge    = round(vs_today_wrc - vs_opp_wrc, 1)
    strong_platoon_advantage    = platoon_edge >= 15
    strong_platoon_disadvantage = platoon_edge <= -15

    return {
        "team":                   team_code,
        # Season stats
        "avg":                    hitting.get("avg"),
        "obp":                    hitting.get("obp"),
        "slg":                    hitting.get("slg"),
        "ops":                    ops,
        "wrc_plus":               wrc_plus_season,
        # Rolling wRC+ (primary signal)
        "wrc_plus_14d":           wrc_plus_14d,
        "wrc_window_days":        window_days,
        "wrc_low_sample":         low_sample_flag,
        # Data health
        "offense_missing":        offense_missing,
        # Platoon-adjusted
        "adj_wrc_plus":           wrc_plus_adj,
        "platoon_delta":          round(platoon_delta, 1),
        "platoon_vs_lhp":         splits.get("vs_lhp"),
        "platoon_vs_rhp":         splits.get("vs_rhp"),
        # RISP
        "risp_avg":               risp.get("risp_avg"),
        "risp_ops":               risp.get("risp_ops"),
        # Form
        "rpg_recent":             rolling_rpg,
        "rpg_season":             round(rpg_season, 2),
        # Form detail
        "wrc_7d":                 wrc7,
        "wrc_30d":                wrc30,
        "rpg_7d":                 rpg7,
        "rpg_30d":                rpg30,
        "record_7d":              record7,
        "record_30d":             record30,
        "recent_win_pct":         record7.get("win_pct", 0.5),
        # Platoon matchup
        "platoon_edge":           platoon_edge,
        "strong_platoon_advantage":    strong_platoon_advantage,
        "strong_platoon_disadvantage": strong_platoon_disadvantage,
        # Lineup
        "lineup_confirmed":       lineup_confirmed,
        "lineup_unconfirmed":     not lineup_confirmed,
        "confidence_penalty":     lineup_unconfirmed_penalty,
        "lineup":                 lineup,
        "lhb_pct":                TEAM_LHB_PCT.get(team_code, 0.43),
        # Run factor
        "run_factor":             run_factor,
        # Woba proxy for ML model compat
        "woba":                   hitting.get("obp", 0.320),
        # Lineup slot quality
        "lineup_slot_quality":    slot_quality,
        "slot_run_adj":           slot_quality.get("slot_run_adj", 0.0),
        "elite_top_order":        slot_quality.get("elite_top_order", False),
        "weak_top_order":         slot_quality.get("weak_top_order", False),
    }


def _default_offense(team_code: str) -> dict:
    return {
        "team":               team_code,
        "avg":                0.250,
        "obp":                0.320,
        "slg":                0.410,
        "ops":                0.730,
        "wrc_plus":           100.0,
        "wrc_plus_14d":       100.0,
        "wrc_window_days":    14,
        "wrc_low_sample":     False,
        "offense_missing":    True,
        "adj_wrc_plus":       100.0,
        "platoon_delta":      0.0,
        "platoon_vs_lhp":     None,
        "platoon_vs_rhp":     None,
        "risp_avg":           None,
        "risp_ops":           None,
        "rpg_recent":         LG_RPG,
        "rpg_season":         LG_RPG,
        "wrc_7d":               100.0,
        "wrc_30d":              100.0,
        "rpg_7d":               LG_RPG,
        "rpg_30d":              LG_RPG,
        "record_7d":            {"wins": 0, "losses": 0, "games": 0, "win_pct": 0.5},
        "record_30d":           {"wins": 0, "losses": 0, "games": 0, "win_pct": 0.5},
        "recent_win_pct":       0.5,
        "platoon_edge":         0.0,
        "strong_platoon_advantage":    False,
        "strong_platoon_disadvantage": False,
        "run_factor":         1.0,
        "lineup_confirmed":   False,
        "lineup_unconfirmed": True,
        "confidence_penalty": 0.10,
        "lineup":             [],
        "lhb_pct":            0.43,
        "woba":               0.320,
        "lineup_slot_quality": {},
        "slot_run_adj":        0.0,
        "elite_top_order":     False,
        "weak_top_order":      False,
    }


if __name__ == "__main__":
    import sys
    team = sys.argv[1].upper() if len(sys.argv) > 1 else "SF"
    sp_h = sys.argv[2].upper() if len(sys.argv) > 2 else "R"
    off  = analyze_offense(team, opp_sp_hand=sp_h)
    print(f"{team} vs {sp_h}HP:")
    print(f"  wRC+_14d={off['wrc_plus_14d']} ({off['wrc_window_days']}d window) "
          f"low_sample={off['wrc_low_sample']}")
    print(f"  adj_wRC+={off['adj_wrc_plus']} platoon_delta={off['platoon_delta']:+.1f}")
    print(f"  vs LHP={off['platoon_vs_lhp']} vs RHP={off['platoon_vs_rhp']}")
    print(f"  run_factor={off['run_factor']} rpg_14d={off['rpg_recent']}")
    print(f"  lineup_confirmed={off['lineup_confirmed']}")
