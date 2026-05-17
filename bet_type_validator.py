"""PARLAY OS — bet_type_validator.py
Enforces bet type rules for every bet before it enters the DB.
All bet creation passes through validate_bet(). Raise on violation, log always.
"""

import logging

_log = logging.getLogger(__name__)


class BetValidationError(Exception):
    """Raised when a bet violates structural or type rules."""


# ── VALIDATION ────────────────────────────────────────────────────────────────

def validate_bet(
    bet_type: str,
    legs: list | None = None,
    conviction_levels: list | None = None,
    open_bets: list | None = None,
    same_game: bool = False,
) -> None:
    """
    Validate a proposed bet against all type rules.
    Raises BetValidationError on any violation.
    Logs all validation calls (pass or fail) for audit trail.

    Args:
        bet_type:          STRAIGHT | PARLAY | HEDGE | PROP | LIVE | ML | F5 | TOTAL
        legs:              list of leg dicts (required for PARLAY)
        conviction_levels: conviction string per leg (required for PARLAY)
        open_bets:         list of open DB bets — required to validate HEDGE
        same_game:         True when PROP is same-game correlated with an ML leg
    """
    t = bet_type.upper().strip()
    errors: list[str] = []

    # ── PARLAY ────────────────────────────────────────────────────────────────
    if t == "PARLAY":
        legs = legs or []
        convictions = conviction_levels or []

        if len(legs) < 2:
            errors.append(f"Parlay requires ≥2 legs — got {len(legs)}")

        if len(legs) > 3:
            errors.append(f"Parlay has {len(legs)} legs — maximum is 3")

        for i, c in enumerate(convictions):
            if str(c).upper() != "HIGH":
                errors.append(
                    f"Leg {i+1} is {c} conviction — parlays require HIGH conviction only"
                )

        # No leg worse than -180 (too much juice, kills parlay EV)
        for i, leg in enumerate(legs):
            leg_odds = leg.get("odds") or leg.get("bet_odds")
            if leg_odds is not None:
                try:
                    o = int(str(leg_odds).replace("+", ""))
                    if o < -180:
                        errors.append(
                            f"Leg {i+1} odds {leg_odds} worse than -180 — avoid heavy juice in parlays"
                        )
                except (ValueError, TypeError):
                    pass

        for i, leg in enumerate(legs):
            leg_type = str(leg.get("type") or "").upper()
            if leg_type in ("PROP", "PLAYER_PROP") and not leg.get("same_game", False):
                errors.append(
                    f"Leg {i+1} is an uncorrelated PROP — prop legs require same_game=True"
                )

        if len(legs) >= 2:
            game_ids = [leg.get("game_id") or leg.get("game") for leg in legs]
            # Detect if any two legs are from different games AND both are props
            prop_games: set = set()
            for leg in legs:
                if str(leg.get("type") or "").upper() in ("PROP", "PLAYER_PROP"):
                    prop_games.add(leg.get("game_id") or leg.get("game") or "")
            if len(prop_games) > 1:
                errors.append("Parlay contains props from multiple games — not allowed")

    # ── HEDGE ─────────────────────────────────────────────────────────────────
    elif t == "HEDGE":
        if not open_bets:
            errors.append("HEDGE requires an existing open bet — no active position found")
        # Additional check: hedge must reference a real open bet
        if open_bets is not None and len(open_bets) == 0:
            errors.append("No open bets to hedge — cannot place standalone HEDGE")

    # ── PROP ─────────────────────────────────────────────────────────────────
    elif t in ("PROP", "PLAYER_PROP"):
        # Props are standalone unless explicitly marked same_game=True
        # No hard error — validated by caller context
        pass

    # ── LIVE ──────────────────────────────────────────────────────────────────
    elif t == "LIVE":
        # Flat 3% of bankroll — stake validation happens in live_stake()
        # No structural validation needed; enforce size via live_stake()
        pass

    # ── STRAIGHT / ML / F5 / TOTAL ──────────────────────────────────────────
    elif t in ("STRAIGHT", "ML", "F5", "TOTAL", "MONEYLINE"):
        pass  # Single-leg bets are always structurally valid

    else:
        errors.append(f"Unknown bet type '{bet_type}' — use STRAIGHT, PARLAY, HEDGE, PROP, or LIVE")

    if errors:
        msg = "; ".join(errors)
        _log.warning("[VALIDATOR] FAIL — %s: %s", bet_type, msg)
        raise BetValidationError(msg)

    _log.debug("[VALIDATOR] PASS — %s", bet_type)


# ── LIVE STAKE ────────────────────────────────────────────────────────────────

def live_stake(bankroll: float) -> float:
    """
    Flat 3% of bankroll for all LIVE bets.
    Never use Kelly on in-game bets — variance is too high with live odds.
    """
    return round(max(bankroll * 0.03, 0.0), 2)


# ── DAILY LIMITS ──────────────────────────────────────────────────────────────

MAX_LOCKS_PER_DAY  = 3    # HIGH conviction ML bets
MAX_FLIPS_PER_DAY  = 2    # MEDIUM conviction ML bets
MAX_PARLAY_LEGS    = 3    # absolute maximum
MAX_PROPS_PER_DAY  = 5    # top 5 props by edge
MIN_LOCKS_GREEN    = 2    # need ≥2 locks for GREEN day
YELLOW_STAKE_CUT   = 0.20 # reduce stakes by 20% on YELLOW day
DAILY_RISK_CAP_PCT = 0.15 # legacy fallback — active budget uses bankroll_engine.daily_budget()


def day_classification(n_locks: int) -> dict:
    """
    Return day color, stake multiplier, and ML bet permission.
    GREEN  = ≥2 locks → full stakes
    YELLOW = 1 lock  → stakes reduced 20%
    RED    = 0 locks → props only, no ML bets
    """
    if n_locks >= 2:
        return {"color": "GREEN",  "emoji": "🟢", "stake_mult": 1.0,  "ml_allowed": True}
    elif n_locks == 1:
        return {"color": "YELLOW", "emoji": "🟡", "stake_mult": 0.80, "ml_allowed": True}
    else:
        return {"color": "RED",    "emoji": "🔴", "stake_mult": 0.0,  "ml_allowed": False}
