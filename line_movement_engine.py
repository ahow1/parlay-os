"""PARLAY OS — line_movement_engine.py
Persistent line movement tracking and sharp money detection.

Polls Odds API every 30 minutes during betting hours (10am-8pm ET).
Stores snapshots in line_history DB table.

Sharp money signals:
  STEAM_MOVE:    Line moves 10+ cents in under 30 minutes (coordinated sharp action)
  REVERSE_LINE:  Public 65%+ on one side but line moves the opposite direction
  SHARP_FADE:    Line moves toward underdog despite heavy public on favorite

When signal fires: send immediate Telegram alert.
Bet within 30 minutes of alert for best number.
"""

import threading
import time
from datetime import datetime
import pytz

ET = pytz.timezone("America/New_York")

POLL_INTERVAL     = 1800   # 30 minutes in seconds
POLL_START_HOUR   = 10     # 10 AM ET
POLL_END_HOUR     = 20     # 8 PM ET
STEAM_MOVE_THRESH = 0.10   # 10 cents in decimal odds = steam move
REVERSE_PCT_THRESH = 0.65  # 65%+ public on one side triggers reverse check


# ── DB helpers ────────────────────────────────────────────────────────────────

def _store_snapshot(game_id: str, away_team: str, home_team: str,
                    away_ml: int | None, home_ml: int | None,
                    game_date: str) -> None:
    try:
        import db
        db.log_line_snapshot(game_id, away_team, home_team, away_ml, home_ml, game_date)
    except Exception as e:
        print(f"[LME] snapshot store error: {e}")


def _get_history(game_id: str, hours_back: int = 1) -> list:
    try:
        import db
        return db.get_line_history(game_id, hours_back=hours_back)
    except Exception:
        return []


# ── Odds conversion ───────────────────────────────────────────────────────────

def _american_to_decimal(ml: int) -> float:
    """American moneyline to decimal odds."""
    try:
        ml = int(ml)
        if ml > 0:
            return 1.0 + ml / 100.0
        return 1.0 + 100.0 / abs(ml)
    except (TypeError, ValueError):
        return 1.0


def _ml_cents_moved(curr: int | None, prev: int | None) -> float:
    """Change in decimal odds between two American moneyline snapshots."""
    if curr is None or prev is None:
        return 0.0
    return abs(_american_to_decimal(curr) - _american_to_decimal(prev))


# ── Signal detection ──────────────────────────────────────────────────────────

def detect_sharp_signals(
    game_id: str,
    current_away_ml: int | None,
    current_home_ml: int | None,
    away_team: str = "",
    home_team: str = "",
) -> list[dict]:
    """
    Compare current line to recent history to detect sharp money.
    Returns list of signal dicts with type, side, team, message.
    """
    signals = []
    history = _get_history(game_id, hours_back=1)
    if len(history) < 1:
        return signals

    # Oldest snapshot in the window
    prev = history[-1]
    prev_away = prev.get("away_ml")
    prev_home = prev.get("home_ml")

    away_move = _ml_cents_moved(current_away_ml, prev_away)
    home_move = _ml_cents_moved(current_home_ml, prev_home)
    max_move  = max(away_move, home_move)

    if max_move < STEAM_MOVE_THRESH:
        return signals

    # Determine which side moved
    moving_side = "away" if away_move >= home_move else "home"
    moving_team = away_team if moving_side == "away" else home_team
    curr_ml     = current_away_ml if moving_side == "away" else current_home_ml
    prev_ml     = prev_away if moving_side == "away" else prev_home

    # Direction: did the line get shorter (favorite) or longer (dog)?
    curr_dec = _american_to_decimal(curr_ml) if curr_ml else 1.0
    prev_dec = _american_to_decimal(prev_ml) if prev_ml else 1.0
    shortened = curr_dec < prev_dec   # true = line got shorter = team became more favored

    if shortened:
        signal_type = "STEAM_MOVE"
        detail = f"sharp money pushing {moving_team} — line shortened {prev_ml:+d}→{curr_ml:+d}"
    else:
        signal_type = "STEAM_MOVE"
        detail = f"sharp money on {moving_team} underdog — line lengthening {prev_ml:+d}→{curr_ml:+d}"

    signals.append({
        "type":       signal_type,
        "side":       moving_side,
        "team":       moving_team,
        "move_size":  round(max_move, 4),
        "prev_ml":    prev_ml,
        "curr_ml":    curr_ml,
        "message": (
            f"🔥 SHARP MONEY — {signal_type}: {moving_team} "
            f"| Line: {prev_ml:+d} → {curr_ml:+d} "
            f"| Move: {max_move:.3f} decimal | Bet within 30 min"
        ),
    })
    return signals


# ── Reverse line movement ─────────────────────────────────────────────────────

def detect_reverse_line(
    game_id: str,
    away_team: str,
    home_team: str,
    public_pct_away: float | None,
    current_away_ml: int | None,
    current_home_ml: int | None,
) -> dict | None:
    """
    Reverse line movement: public heavy on one side but line moving the other way.
    public_pct_away: fraction of public bets on away team (0-1), or None if unknown.
    """
    if public_pct_away is None:
        return None
    history = _get_history(game_id, hours_back=2)
    if not history:
        return None

    prev_away = history[-1].get("away_ml")
    if prev_away is None or current_away_ml is None:
        return None

    curr_dec = _american_to_decimal(current_away_ml)
    prev_dec = _american_to_decimal(prev_away)
    away_shortened = curr_dec < prev_dec   # away became more favored

    if public_pct_away >= REVERSE_PCT_THRESH and not away_shortened:
        # Public heavy on away but line moved toward home → sharp money on home
        return {
            "type":       "REVERSE_LINE",
            "sharp_side": "home",
            "sharp_team": home_team,
            "message": (
                f"⚡ REVERSE LINE MOVE: {public_pct_away:.0%} public on {away_team} "
                f"but line moving toward {home_team} — sharp money on home"
            ),
        }
    if public_pct_away <= (1 - REVERSE_PCT_THRESH) and away_shortened:
        # Public heavy on home but line moved toward away → sharp money on away
        return {
            "type":       "REVERSE_LINE",
            "sharp_side": "away",
            "sharp_team": away_team,
            "message": (
                f"⚡ REVERSE LINE MOVE: {1-public_pct_away:.0%} public on {home_team} "
                f"but line moving toward {away_team} — sharp money on away"
            ),
        }
    return None


# ── Polling cycle ─────────────────────────────────────────────────────────────

def _is_betting_hours() -> bool:
    now = datetime.now(ET)
    return POLL_START_HOUR <= now.hour < POLL_END_HOUR


def _poll_once(events: list, send_fn, game_date: str) -> None:
    """One polling pass: fetch lines, store, detect signals, alert."""
    if not _is_betting_hours():
        return
    try:
        # Import lazily to avoid circular dependency at module load
        from market_engine import _get_slate_odds, _parse_ml_bookmakers, best_odds, MARKETS_ML
        slate = _get_slate_odds(MARKETS_ML)

        for event in events:
            event_id  = event.get("id", "")
            away_name = event.get("away", "")
            home_name = event.get("home", "")

            ml_data = next((e for e in slate if e.get("id") == event_id), None)
            if not ml_data:
                continue

            books    = _parse_ml_bookmakers(ml_data, away_name, home_name)
            _, away_ml = best_odds(books, "away")
            _, home_ml = best_odds(books, "home")
            if away_ml is None or home_ml is None:
                continue

            # Store snapshot
            _store_snapshot(event_id, away_name, home_name,
                            away_ml, home_ml, game_date)

            # Detect steam
            signals = detect_sharp_signals(
                event_id, away_ml, home_ml, away_name, home_name
            )
            for sig in signals:
                msg = sig.get("message", "")
                if msg:
                    print(f"[LME] {msg}")
                    try:
                        send_fn(msg)
                    except Exception as e:
                        print(f"[LME] Telegram error: {e}")

    except Exception as e:
        print(f"[LME] Poll cycle error: {e}")


def start_line_polling(events: list, send_telegram_fn, game_date: str) -> threading.Thread:
    """
    Start background daemon thread. Polls every POLL_INTERVAL seconds.
    Stops automatically when the main process exits.
    """
    def _run():
        while True:
            _poll_once(events, send_telegram_fn, game_date)
            time.sleep(POLL_INTERVAL)

    t = threading.Thread(target=_run, daemon=True, name="line_poller")
    t.start()
    print(
        f"[LME] Line polling started — every {POLL_INTERVAL // 60} min "
        f"({POLL_START_HOUR}am–{POLL_END_HOUR % 12}pm ET)"
    )
    return t


if __name__ == "__main__":
    print("Line movement engine loaded. Call start_line_polling(events, send_fn, date) to activate.")
    print(f"  Poll interval:  {POLL_INTERVAL // 60} min")
    print(f"  Betting hours:  {POLL_START_HOUR}:00–{POLL_END_HOUR}:00 ET")
    print(f"  Steam threshold: {STEAM_MOVE_THRESH} decimal odds ({STEAM_MOVE_THRESH * 100:.0f} cents)")
