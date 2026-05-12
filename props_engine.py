"""PARLAY OS — props_engine.py
K props, NRFI, team totals, first 5, correlated parlays.
Uses Poisson distribution + SP engine data.
"""

import math
import requests
from sp_engine import analyze_sp
from constants import LG_ERA, LG_RPG, PARK_FACTORS, UMPIRE_TENDENCIES

STATSAPI = "https://statsapi.mlb.com/api/v1"


# ── POISSON HELPERS ───────────────────────────────────────────────────────────

def poisson_cdf(lam: float, k: int) -> float:
    """P(X <= k) for Poisson(lam)."""
    total = 0.0
    for i in range(k + 1):
        total += (lam ** i) * math.exp(-lam) / math.factorial(i)
    return min(total, 1.0)


def prob_over(lam: float, line: float) -> float:
    """P(X > line) where line can be 0.5, 1.5, etc."""
    k = int(line)
    return round(1.0 - poisson_cdf(lam, k), 4)


def prob_under(lam: float, line: float) -> float:
    return round(1.0 - prob_over(lam, line), 4)


# ── K PROP ────────────────────────────────────────────────────────────────────

def k_prop(sp_stats: dict, line: float, ump_k_factor: float = 1.0,
           game_script: str = "neutral") -> dict:
    """
    Strikeout prop model.
    sp_stats from sp_engine.analyze_sp()
    Returns P(over), P(under), edge.
    """
    k9   = sp_stats.get("k9", 8.5)
    ip   = sp_stats.get("ip", 0)
    gs   = sp_stats.get("gs", 1) or 1
    ttop = sp_stats.get("ttop", False)

    # Expected IP per start
    ip_per_start = ip / gs if gs > 0 else 5.0
    ip_per_start = min(ip_per_start, 7.0)

    # Game script: down early → pull SP sooner
    if game_script == "blowout":
        ip_per_start = min(ip_per_start, 5.0)

    # TTOP penalty: SP pulled before 3rd time through order
    if not ttop:
        ip_per_start = min(ip_per_start, 5.5)

    lam = k9 * ip_per_start / 9 * ump_k_factor
    lam = round(lam, 3)

    p_over  = prob_over(lam, line)
    p_under = prob_under(lam, line)

    return {
        "line":     line,
        "lam":      lam,
        "p_over":   p_over,
        "p_under":  p_under,
        "ip_exp":   round(ip_per_start, 1),
        "ttop":     ttop,
    }


# ── NRFI MODEL ────────────────────────────────────────────────────────────────

def nrfi_prob(away_sp: dict, home_sp: dict,
              park_factor: float = 1.0,
              weather_factor: float = 1.0) -> dict:
    """
    No Run First Inning probability.
    P(NRFI) = P(away scores 0 in top 1) * P(home scores 0 in bottom 1)
    Using exponential approximation: P(0 runs | λ) = e^(-λ)
    λ per half-inning ≈ (xFIP / 9) * park * weather
    """
    away_xfip = away_sp.get("xfip", 4.35)
    home_xfip = home_sp.get("xfip", 4.35)

    # Expected runs scored by home lineup against away SP in 1 inning
    lam_home_bats = (away_xfip / 9) * park_factor * weather_factor
    # Expected runs scored by away lineup against home SP in 1 inning
    lam_away_bats = (home_xfip / 9) * park_factor * weather_factor

    p_away_no_run = math.exp(-lam_away_bats)
    p_home_no_run = math.exp(-lam_home_bats)
    p_nrfi        = round(p_away_no_run * p_home_no_run, 4)
    p_yrfi        = round(1.0 - p_nrfi, 4)

    return {
        "p_nrfi":         p_nrfi,
        "p_yrfi":         p_yrfi,
        "lam_away_bats":  round(lam_away_bats, 3),
        "lam_home_bats":  round(lam_home_bats, 3),
        "note":           "nrfi" if p_nrfi > 0.58 else ("yrfi" if p_yrfi > 0.58 else "neutral"),
    }


# ── TEAM TOTAL / GAME TOTAL ───────────────────────────────────────────────────

def team_run_expectancy(off_run_factor: float, sp_run_factor: float,
                        park_factor: float = 1.0, weather_factor: float = 1.0,
                        bp_run_factor: float = 1.0) -> float:
    """
    Expected runs for one team in a full game.
    off_run_factor: from offense_engine (wRC+/100 adjusted)
    sp_run_factor:  from sp_engine (1.0 = avg, >1 = SP easier to score on)
    """
    return round(LG_RPG * off_run_factor * sp_run_factor * park_factor * weather_factor * bp_run_factor, 3)


def game_total_prob(away_xr: float, home_xr: float, total_line: float) -> dict:
    """
    P(over line), P(under line) for game total.
    Game total is sum of two Poisson rvs → Poisson(away+home).
    """
    lam = away_xr + home_xr
    p_over  = prob_over(lam, total_line)
    p_under = prob_under(lam, total_line)
    return {
        "away_xr":  away_xr,
        "home_xr":  home_xr,
        "lam":      round(lam, 3),
        "line":     total_line,
        "p_over":   p_over,
        "p_under":  p_under,
        "note":     "over" if p_over > 0.55 else ("under" if p_under > 0.55 else "neutral"),
    }


def f5_run_expectancy(full_xr: float, sp_stats: dict) -> float:
    """Approximate F5 run expectancy: scale full-game by SP dominance + typical 5-inning share."""
    gs   = sp_stats.get("gs", 1) or 1
    ip   = sp_stats.get("ip", 0)
    ips  = ip / gs if gs > 0 else 5.0
    f5_share = min(5.0 / max(ips, 5.0), 1.0) * 0.65  # ~65% of runs in first 5
    return round(full_xr * f5_share, 3)


# ── CORRELATED PARLAY BUILDER ─────────────────────────────────────────────────

def correlated_parlay(away_ml_prob: float, over_prob: float,
                      correlation: float = 0.15) -> dict:
    """
    Build correlated parlay: team ML + game over.
    Correlation boosts joint probability above independence.
    P(A and B) ≈ P(A)*P(B) + rho * sqrt(P(A)*q_A*P(B)*q_B)
    """
    pa  = away_ml_prob
    pb  = over_prob
    qa  = 1.0 - pa
    qb  = 1.0 - pb
    joint_indep  = pa * pb
    corr_adj     = correlation * math.sqrt(pa * qa * pb * qb)
    joint_corr   = min(joint_indep + corr_adj, 0.99)

    return {
        "ml_prob":    round(pa, 4),
        "over_prob":  round(pb, 4),
        "correlation": correlation,
        "joint_prob": round(joint_corr, 4),
        "joint_indep": round(joint_indep, 4),
        "edge_from_corr": round(joint_corr - joint_indep, 4),
    }


# ── PROPS SCANNER ─────────────────────────────────────────────────────────────

def scan_k_prop(sp_stats: dict, market_line: float, market_odds: str,
                ump_k_factor: float = 1.0) -> dict | None:
    """
    Returns edge dict if K prop has value, else None.
    """
    from math_engine import american_to_decimal, implied_prob

    model  = k_prop(sp_stats, market_line, ump_k_factor)
    p_over = model["p_over"]
    mkt_p  = implied_prob(market_odds)
    if mkt_p is None:
        return None

    edge = round(p_over - mkt_p, 4)
    if edge < 0.03:
        return None

    return {
        "type":        "K_PROP",
        "line":        market_line,
        "model_prob":  p_over,
        "market_prob": round(mkt_p, 4),
        "edge_pct":    round(edge * 100, 2),
        "direction":   "OVER",
        "lam":         model["lam"],
        "ttop":        model["ttop"],
    }


if __name__ == "__main__":
    # Quick test
    sp = {"xfip": 3.5, "k9": 10.2, "ip": 52, "gs": 10, "ttop": True}
    print("K Prop (6.5 Ks):", k_prop(sp, 6.5))

    sp2 = {"xfip": 4.8, "k9": 7.5, "ip": 45, "gs": 9, "ttop": False}
    print("NRFI:", nrfi_prob(sp, sp2))

    away_xr = team_run_expectancy(1.05, 1.02, 1.01)
    home_xr = team_run_expectancy(0.97, 0.98, 1.01)
    print("Game total:", game_total_prob(away_xr, home_xr, 8.5))
