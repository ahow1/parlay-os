"""PARLAY OS — line_movement_engine.py
Persistent line movement tracking and sharp money detection.

Polls Odds API every 20 minutes during betting hours (9am-8pm ET).
Stores snapshots in line_history DB table with signal_type.

Sharp money signals:
  STEAM_MOVE:    Line moves 8+ cents in under 20 minutes (coordinated sharp action)
  REVERSE_LINE:  Public 65%+ on one side but line moves the opposite way (5+ cents)
  SHARP_FADE:    Line moves toward underdog despite heavy public on favorite
  OPENER_CLOSE:  Line has moved 12+ cents total from opening number

When signal fires: send immediate Telegram alert.
Bet within 20 minutes of alert for best number.

Exports for brain.py:
  start_line_polling(events, send_fn, game_date) → Thread
  get_line_history(game_id, hours_back)          → list
  get_sharp_signals_today()                      → list
  get_opening_line(game_id)                      → dict | None
"""

import threading
import time
import logging
from datetime import datetime, date
import pytz

ET  = pytz.timezone("America/New_York")
log = logging.getLogger(__name__)

POLL_INTERVAL      = 1200   # 20 minutes in seconds (was 1800)
POLL_START_HOUR    = 9      # 9 AM ET (was 10)
POLL_END_HOUR      = 20     # 8 PM ET
STEAM_THRESH       = 0.08   # 8+ cents in decimal odds = steam move
REVERSE_PCT_THRESH = 0.65   # 65%+ public on one side triggers reverse check
REVERSE_MIN_MOVE   = 0.05   # 5+ cents opposite move to confirm reverse
OPENER_CLOSE_THRESH = 0.12  # 12+ cents total from opening = opener/close signal
SHARP_FADE_THRESH  = 0.06   # 6+ cents toward underdog despite public on fav


# ── DB helpers ────────────────────────────────────────────────────────────────

def _conn():
    import sqlite3, os
    db = os.environ.get("PARLAY_DB", "parlay_os.db")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _ensure_signal_type_col():
    """Migration: add signal_type column to line_history if missing."""
    try:
        with _conn() as conn:
            conn.execute("ALTER TABLE line_history ADD COLUMN signal_type TEXT")
    except Exception:
        pass   # already exists


def _store_snapshot(game_id: str, away_team: str, home_team: str,
                    away_ml: int | None, home_ml: int | None,
                    game_date: str, signal_type: str = "") -> None:
    try:
        _ensure_signal_type_col()
        now = datetime.now(ET).isoformat()
        with _conn() as conn:
            conn.execute("""
                INSERT INTO line_history
                  (timestamp, game_id, away_team, home_team, away_ml, home_ml, game_date, signal_type)
                VALUES (?,?,?,?,?,?,?,?)
            """, (now, game_id, away_team, home_team, away_ml, home_ml, game_date, signal_type))
    except Exception as e:
        log.debug(f"[LME] snapshot store error: {e}")


def _get_history_raw(game_id: str, hours_back: int = 4) -> list:
    try:
        from datetime import timedelta
        cutoff = (datetime.now(ET) - timedelta(hours=hours_back)).isoformat()
        with _conn() as conn:
            rows = conn.execute("""
                SELECT * FROM line_history
                WHERE game_id=? AND timestamp >= ?
                ORDER BY timestamp DESC
            """, (game_id, cutoff)).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


# ── Public API ────────────────────────────────────────────────────────────────

def get_line_history(game_id: str, hours_back: int = 4) -> list:
    """Return line snapshots for a game, newest-first."""
    return _get_history_raw(game_id, hours_back)


def get_opening_line(game_id: str) -> dict | None:
    """Return the very first stored snapshot for a game (opening line)."""
    try:
        with _conn() as conn:
            row = conn.execute("""
                SELECT * FROM line_history WHERE game_id=? ORDER BY timestamp ASC LIMIT 1
            """, (game_id,)).fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_sharp_signals_today() -> list:
    """Return all signal rows from today that have a signal_type."""
    try:
        today = date.today().isoformat()
        with _conn() as conn:
            rows = conn.execute("""
                SELECT * FROM line_history
                WHERE game_date=? AND signal_type IS NOT NULL AND signal_type != ''
                ORDER BY timestamp DESC
            """, (today,)).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


# ── Odds conversion ───────────────────────────────────────────────────────────

def _dec(ml: int | None) -> float:
    if ml is None:
        return 2.0
    try:
        ml = int(ml)
        if ml > 0:
            return 1.0 + ml / 100.0
        return 1.0 + 100.0 / abs(ml)
    except (TypeError, ValueError):
        return 2.0


def _cents_moved(curr: int | None, prev: int | None) -> float:
    if curr is None or prev is None:
        return 0.0
    return abs(_dec(curr) - _dec(prev))


# ── Signal detection ──────────────────────────────────────────────────────────

def detect_sharp_signals(
    game_id: str,
    current_away_ml: int | None,
    current_home_ml: int | None,
    away_team: str = "",
    home_team: str = "",
    public_pct_away: float | None = None,
) -> list[dict]:
    """
    Compare current line to recent history to detect sharp money signals.

    Returns list of signal dicts:
        {type, side, team, move_size, prev_ml, curr_ml, message, confidence_adj}
    """
    signals     = []
    history     = _get_history_raw(game_id, hours_back=1)
    opening     = get_opening_line(game_id)

    if not history:
        return signals

    prev = history[-1]
    prev_away = prev.get("away_ml")
    prev_home = prev.get("home_ml")

    away_move = _cents_moved(current_away_ml, prev_away)
    home_move = _cents_moved(current_home_ml, prev_home)

    # ── STEAM_MOVE: 8+ cents in the last poll window (<= 20 min) ─────────────
    for side, curr, prev_ml, move in [
        ("away", current_away_ml, prev_away, away_move),
        ("home", current_home_ml, prev_home, home_move),
    ]:
        if move < STEAM_THRESH:
            continue
        team = away_team if side == "away" else home_team
        curr_dec = _dec(curr)
        prev_dec_v = _dec(prev_ml)
        shortened = curr_dec < prev_dec_v

        signals.append({
            "type":           "STEAM_MOVE",
            "side":           side,
            "team":           team,
            "move_size":      round(move, 4),
            "prev_ml":        prev_ml,
            "curr_ml":        curr,
            "confidence_adj": +10,    # +10 if betting same side
            "message": (
                f"🔥 STEAM_MOVE: {team} | "
                f"{prev_ml:+d} → {curr:+d} | "
                f"Move: {move:.3f} decimal | "
                f"{'Line shortened (sharp fav)' if shortened else 'Sharp dog action'}"
                f" | Bet within 20 min"
            ),
        })

    # ── REVERSE_LINE: 65%+ public one side, line 5+ cents other way ──────────
    if public_pct_away is not None and prev_away is not None and current_away_ml is not None:
        curr_away_dec = _dec(current_away_ml)
        prev_away_dec = _dec(prev_away)
        away_shortened = curr_away_dec < prev_away_dec
        away_move_abs  = abs(curr_away_dec - prev_away_dec)

        if public_pct_away >= REVERSE_PCT_THRESH and not away_shortened and away_move_abs >= REVERSE_MIN_MOVE:
            signals.append({
                "type":           "REVERSE_LINE",
                "side":           "home",
                "team":           home_team,
                "move_size":      round(away_move_abs, 4),
                "prev_ml":        prev_away,
                "curr_ml":        current_away_ml,
                "confidence_adj": +10,
                "message": (
                    f"⚡ REVERSE_LINE: {public_pct_away:.0%} public on {away_team} "
                    f"but line moving toward {home_team} "
                    f"({prev_away:+d}→{current_away_ml:+d}) — sharp money on home"
                ),
            })
        if public_pct_away <= (1 - REVERSE_PCT_THRESH) and away_shortened and away_move_abs >= REVERSE_MIN_MOVE:
            signals.append({
                "type":           "REVERSE_LINE",
                "side":           "away",
                "team":           away_team,
                "move_size":      round(away_move_abs, 4),
                "prev_ml":        prev_away,
                "curr_ml":        current_away_ml,
                "confidence_adj": +10,
                "message": (
                    f"⚡ REVERSE_LINE: {1-public_pct_away:.0%} public on {home_team} "
                    f"but line moving toward {away_team} "
                    f"({prev_away:+d}→{current_away_ml:+d}) — sharp money on away"
                ),
            })

    # ── SHARP_FADE: line moves toward underdog despite public on favorite ──────
    if public_pct_away is not None and prev_away is not None and current_away_ml is not None:
        away_is_fav_now  = current_away_ml is not None and current_away_ml < 0
        away_is_dog_now  = current_away_ml is not None and current_away_ml > 0
        curr_away_dec    = _dec(current_away_ml)
        prev_away_dec    = _dec(prev_away)
        move_toward_away = curr_away_dec > prev_away_dec   # away got longer = toward away dog
        fade_size        = abs(curr_away_dec - prev_away_dec)

        # Public heavy on away (fav), but line drifts toward home — sharp on home
        if (public_pct_away >= 0.60 and away_is_fav_now
                and not move_toward_away and fade_size >= SHARP_FADE_THRESH):
            signals.append({
                "type":           "SHARP_FADE",
                "side":           "home",
                "team":           home_team,
                "move_size":      round(fade_size, 4),
                "prev_ml":        prev_away,
                "curr_ml":        current_away_ml,
                "confidence_adj": +10,
                "message": (
                    f"📉 SHARP_FADE: {public_pct_away:.0%} public on {away_team} "
                    f"but line fading toward {home_team} — sharp fading public fav"
                ),
            })

    # ── OPENER_CLOSE: 12+ cents total from opening line ───────────────────────
    if opening and current_away_ml is not None:
        open_away = opening.get("away_ml")
        if open_away is not None:
            total_move = abs(_dec(current_away_ml) - _dec(open_away))
            if total_move >= OPENER_CLOSE_THRESH:
                curr_dec_v = _dec(current_away_ml)
                open_dec_v = _dec(open_away)
                moved_toward = "home" if curr_dec_v > open_dec_v else "away"
                moved_team   = home_team if moved_toward == "home" else away_team
                signals.append({
                    "type":           "OPENER_CLOSE",
                    "side":           moved_toward,
                    "team":           moved_team,
                    "move_size":      round(total_move, 4),
                    "prev_ml":        open_away,
                    "curr_ml":        current_away_ml,
                    "confidence_adj": +10,
                    "message": (
                        f"📊 OPENER_CLOSE: {moved_team} line moved {total_move:.3f} "
                        f"({open_away:+d}→{current_away_ml:+d}) from open — "
                        f"sustained sharp action"
                    ),
                })

    return signals


# ── Confidence adjustment helper ──────────────────────────────────────────────

def get_confidence_adj(game_id: str, side: str) -> int:
    """
    Return confidence adjustment based on today's sharp signals for this game/side.
    +10 if sharp signal matches our side, -15 if sharp signal is against us.
    """
    signals = get_sharp_signals_today()
    adj = 0
    for sig_row in signals:
        if sig_row.get("game_id") != game_id:
            continue
        sig_type = sig_row.get("signal_type", "")
        if not sig_type:
            continue
        # Parse signal type from DB row — stored as "TYPE:side"
        if ":" in sig_type:
            stype, ssid = sig_type.split(":", 1)
        else:
            stype, ssid = sig_type, ""

        if ssid == side:
            adj += 10
        elif ssid and ssid != side:
            adj -= 15
    return max(min(adj, 20), -15)


# ── Polling cycle ─────────────────────────────────────────────────────────────

def _is_betting_hours() -> bool:
    now = datetime.now(ET)
    return POLL_START_HOUR <= now.hour < POLL_END_HOUR


def _poll_once(events: list, send_fn, game_date: str) -> None:
    """One polling pass: fetch lines, store snapshot, detect signals, alert."""
    if not _is_betting_hours():
        return
    try:
        from market_engine import _get_slate_odds, _parse_ml_bookmakers, best_odds, MARKETS_ML
        slate = _get_slate_odds(MARKETS_ML)

        for event in events:
            event_id  = event.get("id", "")
            away_name = event.get("away", "")
            home_name = event.get("home", "")
            if not event_id:
                continue

            ml_data = next((e for e in slate if e.get("id") == event_id), None)
            if not ml_data:
                continue

            books    = _parse_ml_bookmakers(ml_data, away_name, home_name)
            _, away_ml = best_odds(books, "away")
            _, home_ml = best_odds(books, "home")
            if away_ml is None or home_ml is None:
                continue

            # Detect signals BEFORE storing (so prev snapshot is the previous poll)
            signals = detect_sharp_signals(
                event_id, away_ml, home_ml, away_name, home_name
            )

            # Store snapshot (with signal_type if any fired)
            sig_type_str = ""
            if signals:
                sig_type_str = "|".join(
                    f"{s['type']}:{s['side']}" for s in signals
                )

            _store_snapshot(event_id, away_name, home_name,
                            away_ml, home_ml, game_date, sig_type_str)

            # Alert on each signal
            for sig in signals:
                msg = sig.get("message", "")
                log.info(f"[LME] Signal: {msg}")
                print(f"[LME] {msg}")
                try:
                    send_fn(msg)
                except Exception as e:
                    log.error(f"[LME] Telegram send error: {e}")

    except Exception as e:
        log.error(f"[LME] Poll cycle error: {e}", exc_info=True)


def start_line_polling(
    events: list,
    send_telegram_fn,
    game_date: str,
) -> threading.Thread:
    """
    Start background daemon thread.
    Polls every 20 minutes (9am–8pm ET).
    Stops automatically when main process exits.
    """
    _ensure_signal_type_col()

    def _run():
        while True:
            try:
                _poll_once(events, send_telegram_fn, game_date)
            except Exception as e:
                log.error(f"[LME] poll_once uncaught: {e}", exc_info=True)
            time.sleep(POLL_INTERVAL)

    t = threading.Thread(target=_run, daemon=True, name="line_poller")
    t.start()
    log.info(
        f"[LME] Line polling started — every {POLL_INTERVAL // 60} min "
        f"({POLL_START_HOUR}am–{POLL_END_HOUR % 12}pm ET) | "
        f"Steam>{STEAM_THRESH} | Rev>{REVERSE_PCT_THRESH:.0%} | "
        f"Opener>{OPENER_CLOSE_THRESH}"
    )
    print(
        f"[LME] Line polling started — {POLL_INTERVAL//60}m intervals "
        f"| {POLL_START_HOUR}am-{POLL_END_HOUR % 12}pm ET"
    )
    return t


if __name__ == "__main__":
    print("Line movement engine")
    print(f"  Poll interval:  {POLL_INTERVAL // 60} min (was 30)")
    print(f"  Betting hours:  {POLL_START_HOUR}:00–{POLL_END_HOUR}:00 ET (was 10am)")
    print(f"  Steam threshold: {STEAM_THRESH} dec ({STEAM_THRESH * 100:.0f} cents, was 10¢)")
    print(f"  Reverse thresh:  {REVERSE_PCT_THRESH:.0%} public + {REVERSE_MIN_MOVE * 100:.0f}¢ opposite")
    print(f"  Opener/close:    {OPENER_CLOSE_THRESH * 100:.0f}¢ total from open")

    signals_today = get_sharp_signals_today()
    print(f"\n  Sharp signals today: {len(signals_today)}")
    for s in signals_today[:3]:
        print(f"    {s.get('signal_type','')} | {s.get('away_team','')}@{s.get('home_team','')}")
