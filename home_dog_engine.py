"""PARLAY OS — home_dog_engine.py
Home underdog structural edge engine.

Research shows home underdogs win ~45.9% of games but are priced at ~40%
implied probability — a persistent market inefficiency.

Flag HOME_DOG_VALUE when ALL five conditions are met:
  1. Home ML odds >= +115 (true underdog price)
  2. Home SP xFIP < 4.25 (competent pitcher)
  3. Home bullpen avg_fatigue < 4.0 (fresh bullpen)
  4. Home team NOT on 5+ game losing streak
  5. Opposing SP xFIP >= 3.0 (not facing an elite arm)

When all conditions met: add 0.04 to home win probability before blending.
"""


def check_home_dog_value(analysis: dict) -> dict:
    """
    Returns {is_home_dog_value, conditions, conditions_failed, add_prob,
             home_odds, home_xfip, away_xfip, home_fatigue}.
    """
    best_home_odds = analysis.get("best_home_odds")
    home_sp  = analysis.get("home_sp") or {}
    away_sp  = analysis.get("away_sp") or {}
    home_bp  = analysis.get("home_bp") or {}
    home_off = analysis.get("home_off") or {}

    # Condition 1: home is a genuine price underdog (+115 or better)
    cond_underdog = False
    if best_home_odds is not None:
        try:
            cond_underdog = int(best_home_odds) >= 115
        except (ValueError, TypeError):
            pass

    # Condition 2: home SP is at least competent (xFIP < 4.25)
    home_xfip = home_sp.get("xfip", 5.0)
    cond_sp_quality = home_xfip < 4.25 and not home_sp.get("sp_missing", False)

    # Condition 3: home bullpen is reasonably fresh (avg_fatigue < 4.0)
    home_fatigue = home_bp.get("avg_fatigue", 5.0)
    cond_bullpen = home_fatigue < 4.0

    # Condition 4: not on a 5+ game losing streak in last 7 days
    home_record  = home_off.get("record_7d", {})
    home_losses  = home_record.get("losses", 0)
    home_wins    = home_record.get("wins", 0)
    cond_not_cold = not (home_losses >= 5 and home_wins == 0)

    # Condition 5: opponent SP is NOT elite (xFIP >= 3.0 = not lights-out)
    away_xfip = away_sp.get("xfip", 4.35)
    cond_opp_not_elite = away_xfip >= 3.0

    conditions = {
        "home_underdog_+115":    cond_underdog,
        "home_sp_xfip_<4.25":   cond_sp_quality,
        "home_bp_fatigue_<4.0": cond_bullpen,
        "not_cold_streak":       cond_not_cold,
        "opp_sp_not_elite":      cond_opp_not_elite,
    }
    all_met  = all(conditions.values())
    failed   = [k for k, v in conditions.items() if not v]

    return {
        "is_home_dog_value": all_met,
        "conditions":        conditions,
        "conditions_failed": failed,
        "add_prob":          0.04 if all_met else 0.0,
        "home_odds":         best_home_odds,
        "home_xfip":         round(home_xfip, 2),
        "away_xfip":         round(away_xfip, 2),
        "home_fatigue":      home_fatigue,
        "home_record_7d":    home_record,
    }


def home_dog_telegram_tag(hd: dict) -> str:
    """One-line tag for Telegram if HOME_DOG_ANGLE fires."""
    if not hd.get("is_home_dog_value"):
        return ""
    odds = hd.get("home_odds", "")
    odds_sign = "+" if isinstance(odds, int) and odds > 0 else ""
    try:
        _o = int(odds or 115)
        implied_pct = round(abs(_o) / (abs(_o) + 100) * 100, 0)
    except (ValueError, TypeError):
        implied_pct = 46
    return (
        f"🐶 HOME_DOG_ANGLE: {odds_sign}{odds} "
        f"— structural +4% edge (home dog hits 45.9% vs {implied_pct:.0f}% implied)"
    )


if __name__ == "__main__":
    sample = {
        "best_home_odds": 130,
        "home_sp": {"xfip": 3.95, "sp_missing": False},
        "away_sp": {"xfip": 3.80},
        "home_bp": {"avg_fatigue": 2.5},
        "home_off": {"record_7d": {"wins": 4, "losses": 3}},
    }
    result = check_home_dog_value(sample)
    print(f"HOME_DOG_VALUE: {result['is_home_dog_value']}")
    print(f"Conditions: {result['conditions']}")
    print(f"Tag: {home_dog_telegram_tag(result)}")
