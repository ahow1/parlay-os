"""PARLAY OS — intelligence_engine.py
Regression detection, momentum scoring, injury flags, and picks formatting.
All functions degrade gracefully — never block game analysis on API failures.
"""

import math
import logging
from datetime import date, timedelta
from api_client import get as _http_get

STATSAPI = "https://statsapi.mlb.com/api/v1"
_log = logging.getLogger(__name__)


# ── SP REGRESSION FLAGS ───────────────────────────────────────────────────────

def sp_regression_flags(sp: dict, label: str = "") -> list[dict]:
    """
    ERA vs xFIP divergence, velocity decline proxy, walk rate spike.
    Returns list of flag dicts {type, emoji, message, direction}.
    direction: 'fade' = bet against this SP's team | 'back' = bet for this team
    """
    flags = []
    if not sp:
        return flags

    name = sp.get("name") or label or "SP"
    era  = sp.get("era")
    xfip = sp.get("xfip")

    if era is not None and xfip is not None:
        gap = round(era - xfip, 2)
        if gap >= 1.5:
            flags.append({
                "type":      "ERA_REGRESSION_DUE",
                "emoji":     "⬇️",
                "message":   (
                    f"{name} ERA {era:.2f} but xFIP {xfip:.2f} — "
                    f"significant regression incoming, fade his team"
                ),
                "direction": "fade",
            })
        elif gap <= -1.5:
            flags.append({
                "type":      "ERA_CORRECTION_DUE",
                "emoji":     "⬆️",
                "message":   (
                    f"{name} ERA {era:.2f} but xFIP {xfip:.2f} — "
                    f"pitching much better than results, back him"
                ),
                "direction": "back",
            })

    if sp.get("velocity_decline"):
        trend = sp.get("k9_trend_10s", 0.0)
        flags.append({
            "type":      "VELOCITY_DECLINE",
            "emoji":     "⚠️",
            "message":   (
                f"{name} K/9 trend {trend:+.1f} last 10 starts — "
                f"possible velocity/command decline"
            ),
            "direction": "fade",
        })

    if sp.get("velocity_injury_risk"):
        trend = sp.get("k9_trend_10s", 0.0)
        flags.append({
            "type":      "INJURY_RISK_SP",
            "emoji":     "🚑",
            "message":   (
                f"{name} K/9 trend {trend:+.1f} — severe decline, injury risk"
            ),
            "direction": "fade",
        })

    if sp.get("worsening_walk"):
        r3_bb9 = sp.get("rolling_bb9_3")
        szn_bb9 = sp.get("bb9")
        if r3_bb9 is not None and szn_bb9 is not None:
            flags.append({
                "type":      "CONTROL_ISSUES",
                "emoji":     "⚠️",
                "message":   (
                    f"{name} BB/9 {r3_bb9:.1f} last 3 starts vs "
                    f"{szn_bb9:.1f} season — control issues flagged"
                ),
                "direction": "fade",
            })

    return flags


def offense_regression_flags(team_id: int, team_code: str) -> list[dict]:
    """
    Fetch team batting stats from Stats API.
    Checks BABIP (luck proxy) and BA trend.
    BA vs xBA skipped — requires Statcast.
    """
    flags = []
    try:
        r = _http_get(
            f"{STATSAPI}/teams/{team_id}/stats",
            params={"stats": "season", "group": "hitting",
                    "gameType": "R", "season": "2026"},
            timeout=8,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return flags
        s    = splits[0].get("stat", {})
        babip = float(s.get("babip", 0) or 0)
        ba    = float(s.get("avg", 0) or 0)
        hr    = int(s.get("homeRuns", 0) or 0)
        ab    = int(s.get("atBats", 1) or 1)

        if babip > 0.320:
            flags.append({
                "type":      "HIGH_BABIP",
                "emoji":     "📈",
                "message":   (
                    f"{team_code} BABIP {babip:.3f} — "
                    f"luck-driven offense, regression to mean likely"
                ),
                "direction": "fade",
            })

        if babip < 0.270 and babip > 0:
            flags.append({
                "type":      "LOW_BABIP_IMPROVEMENT",
                "emoji":     "⬆️",
                "message":   (
                    f"{team_code} BABIP {babip:.3f} — "
                    f"unlucky offense, improvement coming"
                ),
                "direction": "back",
            })

        # HR/FB approximation — HR per AB proxy
        if ab > 0:
            hr_per_ab = hr / ab
            if hr_per_ab > 0.050:   # ~1 HR every 20 ABs = unsustainable
                flags.append({
                    "type":      "HR_REGRESSION",
                    "emoji":     "📈",
                    "message":   (
                        f"{team_code} HR/AB {hr_per_ab:.3f} — "
                        f"power pace may be unsustainable, HR regression coming"
                    ),
                    "direction": "fade",
                })

    except Exception as e:
        _log.debug(f"Offense regression fetch error {team_code}: {e}")

    return flags


def bullpen_regression_flags(bp_era: float | None, bp_xfip: float | None,
                              team_code: str) -> list[dict]:
    """Bullpen ERA vs xFIP divergence."""
    flags = []
    if bp_era is None or bp_xfip is None:
        return flags
    gap = round(bp_era - bp_xfip, 2)
    if gap >= 1.5:
        flags.append({
            "type":      "BULLPEN_ERA_REGRESSION_DUE",
            "emoji":     "⬇️",
            "message":   (
                f"{team_code} bullpen ERA {bp_era:.2f} vs xFIP {bp_xfip:.2f} — "
                f"pen better than they look, ERA will improve"
            ),
            "direction": "back",
        })
    elif gap <= -1.5:
        flags.append({
            "type":      "BULLPEN_CORRECTION_DUE",
            "emoji":     "⬆️",
            "message":   (
                f"{team_code} bullpen ERA {bp_era:.2f} vs xFIP {bp_xfip:.2f} — "
                f"pen worse than they look, ERA will rise"
            ),
            "direction": "fade",
        })
    return flags


# ── INJURY FLAGS ──────────────────────────────────────────────────────────────

def get_injury_flags(away_code: str, home_code: str,
                     away_sp: dict, home_sp: dict) -> list[dict]:
    """
    Pull MLB transactions (IL moves) for last 48 hours.
    Also flags SP velocity/injury risk from sp_engine data.
    """
    from constants import MLB_TEAM_IDS
    flags: list[dict] = []

    today        = date.today()
    two_days_ago = today - timedelta(days=2)

    try:
        r = _http_get(
            f"{STATSAPI}/transactions",
            params={
                "startDate": two_days_ago.isoformat(),
                "endDate":   today.isoformat(),
                "sportId":   1,
            },
            timeout=8,
        )
        away_tid = MLB_TEAM_IDS.get(away_code, -1)
        home_tid = MLB_TEAM_IDS.get(home_code, -1)

        for txn in r.json().get("transactions", []):
            txn_type = (txn.get("typeCode") or "").upper()
            if txn_type not in ("IL", "DL", "DTD", "PLACED ON 10-DAY IL",
                                "PLACED ON 15-DAY IL", "PLACED ON 60-DAY IL"):
                continue
            player      = txn.get("player", {})
            player_name = player.get("nameSlot") or player.get("fullName", "Unknown")
            team_obj    = txn.get("toTeam") or txn.get("fromTeam") or {}
            team_id     = team_obj.get("id", -1)
            desc        = (txn.get("description") or "")[:60]

            code = None
            if team_id == away_tid:
                code = away_code
            elif team_id == home_tid:
                code = home_code
            if not code:
                continue

            flags.append({
                "type":    "IL_MOVE",
                "emoji":   "🚑",
                "team":    code,
                "message": f"{code}: {player_name} {txn_type} — {desc}",
            })
    except Exception as e:
        _log.debug(f"Transactions error: {e}")

    # SP-level velocity/injury flags from sp_engine
    for sp, code in ((away_sp, away_code), (home_sp, home_code)):
        if not sp:
            continue
        name = sp.get("name", "SP")
        if sp.get("velocity_injury_risk"):
            trend = sp.get("k9_trend_10s", 0.0)
            flags.append({
                "type":    "INJURY_RISK_SP",
                "emoji":   "⚠️",
                "team":    code,
                "message": (
                    f"{code}: {name} K/9 trend {trend:+.1f} — "
                    f"severe K rate decline, possible injury"
                ),
            })

    return flags


def format_injury_section(flags: list, game_label: str) -> str:
    """Return formatted injury section for Telegram, or '' if none."""
    if not flags:
        return ""
    lines = [f"⚠️ INJURY FLAGS — {game_label}:"]
    for f in flags:
        lines.append(f"  {f.get('emoji','⚠️')} {f['message']}")
    return "\n".join(lines)


# ── MOMENTUM SCORING ──────────────────────────────────────────────────────────

def weighted_momentum(team_id: int, team_code: str) -> dict:
    """
    Weighted momentum -10 to +10 over last 7 games.
      Win vs winning team  = +1.5 pts
      Win vs losing team   = +0.5 pts
      Loss vs winning team = -0.5 pts
      Loss vs losing team  = -1.5 pts
    Also: run differential trend and Pythagorean luck score.
    """
    today    = date.today()
    start_dt = today - timedelta(days=14)

    raw_score  = 0.0
    games_used = 0
    run_diffs: list[float] = []

    try:
        r = _http_get(
            f"{STATSAPI}/schedule",
            params={
                "sportId":   1,
                "teamId":    team_id,
                "startDate": start_dt.isoformat(),
                "endDate":   (today - timedelta(days=1)).isoformat(),
                "hydrate":   "linescore,team",
                "gameType":  "R",
            },
            timeout=10,
        )
        for day_data in r.json().get("dates", []):
            for g in day_data.get("games", []):
                if games_used >= 7:
                    break
                if g.get("status", {}).get("abstractGameState") != "Final":
                    continue

                teams    = g.get("teams", {})
                away_t   = teams.get("away", {})
                home_t   = teams.get("home", {})
                is_away  = away_t.get("team", {}).get("id") == team_id
                our_side = away_t if is_away else home_t
                opp_side = home_t if is_away else away_t

                won   = bool(our_side.get("isWinner", False))
                our_r = int(our_side.get("score") or 0)
                opp_r = int(opp_side.get("score") or 0)
                run_diffs.append(float(our_r - opp_r))

                rec        = opp_side.get("leagueRecord", {})
                opp_wins   = int(rec.get("wins", 40) or 40)
                opp_losses = int(rec.get("losses", 40) or 40)
                opp_total  = opp_wins + opp_losses
                opp_winpct = opp_wins / opp_total if opp_total > 0 else 0.50

                if won:
                    raw_score += 1.5 if opp_winpct > 0.50 else 0.5
                else:
                    raw_score += -0.5 if opp_winpct > 0.50 else -1.5
                games_used += 1

    except Exception as e:
        _log.debug(f"Momentum fetch error {team_code}: {e}")

    avg_rd = round(sum(run_diffs) / len(run_diffs), 2) if run_diffs else 0.0

    # Pythagorean luck: actual W - expected W from run diff
    pyth_luck = 0.0
    if run_diffs and games_used > 0:
        # Proxy RS/RA per game using 4.35 lg avg as baseline
        rs_list = [max(rd + 4.35, 0.5) for rd in run_diffs]
        ra_list = [max(4.35 - rd, 0.5) for rd in run_diffs]
        rs = sum(rs_list)
        ra = sum(ra_list)
        exp_wp = rs ** 1.83 / (rs ** 1.83 + ra ** 1.83)
        exp_w  = round(exp_wp * games_used, 1)
        act_w  = sum(1 for rd in run_diffs if rd > 0)
        pyth_luck = round(act_w - exp_w, 2)

    score = round(max(-10.0, min(10.0, raw_score)), 1)

    act_w = sum(1 for rd in run_diffs if rd > 0)
    act_l = games_used - act_w
    parts = [f"{act_w}-{act_l} last {games_used}g"] if games_used > 0 else []
    if avg_rd != 0:
        parts.append(f"{avg_rd:+.1f} avg run diff")
    if abs(pyth_luck) >= 0.5:
        parts.append(f"{pyth_luck:+.1f} Pythag luck")

    return {
        "score":       score,
        "games":       games_used,
        "raw_score":   raw_score,
        "avg_run_diff": avg_rd,
        "pyth_luck":   pyth_luck,
        "pyth_lucky":  pyth_luck > 1.0,
        "pyth_unlucky": pyth_luck < -1.0,
        "summary":     f"{team_code} momentum: {score:+.1f} ({', '.join(parts)})" if parts else f"{team_code} momentum: {score:+.1f}",
    }


# ── PICKS SERVICE FORMAT ──────────────────────────────────────────────────────

def format_sharp_pick(analysis: dict, side: str) -> str:
    """Twitter/Discord-ready short format for a recommended pick."""
    team     = analysis.get(f"{side}_name", "")
    opp_s    = "home" if side == "away" else "away"
    away_n   = analysis.get("away_name", "")
    home_n   = analysis.get("home_name", "")
    odds     = analysis.get(f"best_{side}_odds")
    odds_str = (f"+{odds}" if isinstance(odds, int) and odds > 0 else str(odds or ""))
    edge     = analysis.get(f"{side}_edge", 0)
    stake    = analysis.get(f"{side}_stake", 0)
    conv     = analysis.get(f"{side}_conv", "")
    sp       = analysis.get(f"{side}_sp") or {}
    opp_sp   = analysis.get(f"{opp_s}_sp") or {}
    gt       = analysis.get("game_time_et", "")
    bp_opp   = analysis.get(f"{opp_s}_bp") or {}

    conv_tag = {"HIGH": "HIGH", "MEDIUM": "MED"}.get(conv, conv)
    header   = f"[{conv_tag}] {away_n} @ {home_n}"
    if gt:
        header += f" — {gt}"

    lines = [
        header,
        f"{team} ML {odds_str} ✅ | Edge: +{edge:.1f}% | ${stake:.2f}",
    ]

    if sp.get("name") and opp_sp.get("name"):
        lines.append(
            f"SP: {sp['name']} ({sp.get('xfip', 4.35):.2f} xFIP) "
            f"vs {opp_sp['name']} ({opp_sp.get('xfip', 4.35):.2f} xFIP)"
        )

    if bp_opp.get("fatigue_tier") in ("HIGH", "CRITICAL"):
        lines.append(
            f"{opp_s.upper()} pen {bp_opp['fatigue_tier']} fatigue"
        )

    # Best intelligence flag
    intel = (analysis.get("intel_flags") or [])
    all_flags = (analysis.get("reg_flags") or []) + intel
    if all_flags:
        msg = all_flags[0].get("message", "")
        emoji = all_flags[0].get("emoji", "⚠️")
        if msg:
            lines.append(f"{emoji} {msg[:80]}")

    return "\n".join(lines)


def format_discord_pick(analysis: dict, side: str) -> str:
    """Discord embed-ready format."""
    sharp  = format_sharp_pick(analysis, side)
    border = "━" * 32
    return f"{border}\n{sharp}\n{border}"
