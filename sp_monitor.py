"""PARLAY OS — SP / Lineup / IL Change Detection Monitor.

Runs as a daemon thread started by brain.py in --bot mode.
Checks:
  - SP changes every 15 min, 10am–8pm ET
  - Lineup changes every 20 min, 3pm–7pm ET
  - IL transactions at 9am, 11am, 1pm ET
Sends Telegram alerts when changes invalidate model assumptions.
"""

import json
import logging
import time
import threading
from datetime import datetime, date

import pytz
import requests

import db as _db

ET = pytz.timezone("America/New_York")

logging.basicConfig(
    filename="sp_monitor.log",
    level=logging.INFO,
    format="%(asctime)s [SP_MONITOR] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_log = logging.getLogger("sp_monitor")

MLB_API = "https://statsapi.mlb.com/api/v1"

# xFIP threshold for cancel recommendation (worse = higher xFIP for starters)
_XFIP_CANCEL_THRESHOLD = 0.50


def _now_et() -> datetime:
    return datetime.now(ET)


def _hour_et() -> float:
    n = _now_et()
    return n.hour + n.minute / 60.0


def _today() -> str:
    return date.today().isoformat()


def _mlb_get(url: str, params: dict | None = None, timeout: int = 15) -> dict | list | None:
    """GET from MLB Stats API with 429 retry and error logging."""
    for attempt in range(2):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                _log.warning("429 from MLB API — sleeping 60s then retrying")
                time.sleep(60)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            _log.error("MLB API error (%s): %s", url, e)
            if attempt == 0:
                time.sleep(5)
    return None


class SPMonitor:
    """Background SP/lineup/IL monitor. Call run() in a daemon thread."""

    def __init__(self, send_fn):
        self._send = send_fn
        self._stop = threading.Event()
        self._last_sp_check: float = 0.0
        self._last_lineup_check: float = 0.0
        self._il_checks_done: set = set()  # hours already fired (9, 11, 13)

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        _log.info("SP monitor started")
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception as e:
                _log.error("tick error: %s", e)
            self._stop.wait(60)  # 60-second heartbeat
        _log.info("SP monitor stopped")

    def _tick(self) -> None:
        now_ts = time.monotonic()
        hour   = _hour_et()

        # SP check — every 15 min, 10am–8pm ET
        if 10.0 <= hour < 20.0 and now_ts - self._last_sp_check >= 900:
            self._check_sp_changes()
            self._last_sp_check = now_ts

        # Lineup check — every 20 min, 3pm–7pm ET
        if 15.0 <= hour < 19.0 and now_ts - self._last_lineup_check >= 1200:
            self._check_lineup_changes()
            self._last_lineup_check = now_ts

        # IL check — 9am, 11am, 1pm ET (fire once per slot per day)
        for target_hour, slot_key in ((9, "09"), (11, "11"), (13, "13")):
            day_slot = f"{_today()}:{slot_key}"
            if (target_hour <= hour < target_hour + 1) and day_slot not in self._il_checks_done:
                self._check_il_transactions()
                self._il_checks_done.add(day_slot)
                # Prune old day slots to avoid unbounded growth
                self._il_checks_done = {k for k in self._il_checks_done if k.startswith(_today())}

    # ── SP Change Detection ────────────────────────────────────────────────────

    def _check_sp_changes(self) -> None:
        today = _today()
        tracked = _db.get_sp_tracker(today)
        if not tracked:
            return

        url = f"{MLB_API}/schedule"
        params = {"sportId": 1, "date": today, "hydrate": "probablePitcher"}
        data = _mlb_get(url, params)
        if not data:
            _log.warning("SP check: no data returned from MLB API")
            self._send("⚠️ SP Monitor: MLB API offline — cannot verify SPs")
            return

        live_sps: dict[str, dict] = {}  # game_pk → {away: {...}, home: {...}}
        for game_date in data.get("dates", []):
            for game in game_date.get("games", []):
                gk = str(game.get("gamePk", ""))
                teams = game.get("teams", {})
                live_sps[gk] = {
                    "away": teams.get("away", {}).get("probablePitcher") or {},
                    "home": teams.get("home", {}).get("probablePitcher") or {},
                }

        for row in tracked:
            if row.get("alert_sent"):
                continue

            gk = str(row["game_pk"])
            live = live_sps.get(gk)
            if live is None:
                continue

            orig_away_id = row.get("away_sp_id") or 0
            orig_home_id = row.get("home_sp_id") or 0
            live_away_id = live["away"].get("id") or 0
            live_home_id = live["home"].get("id") or 0
            live_away_name = live["away"].get("fullName") or "TBD"
            live_home_name = live["home"].get("fullName") or "TBD"

            away_changed = orig_away_id and live_away_id and orig_away_id != live_away_id
            home_changed = orig_home_id and live_home_id and orig_home_id != live_home_id
            away_tbd     = orig_away_id and not live_away_id
            home_tbd     = orig_home_id and not live_home_id

            if away_changed or home_changed or away_tbd or home_tbd:
                new_away = live_away_name if (away_changed or away_tbd) else row.get("away_sp_name", "")
                new_home = live_home_name if (home_changed or home_tbd) else row.get("home_sp_name", "")
                _db.mark_sp_changed(today, gk, new_away, new_home)
                self._send_sp_alert(row, new_away if (away_changed or away_tbd) else None,
                                         new_home if (home_changed or home_tbd) else None)
                _db.mark_sp_alert_sent(today, gk)
                _log.info("SP change: game %s — %s/%s → %s/%s",
                          gk, row.get("away_sp_name"), row.get("home_sp_name"),
                          new_away, new_home)

    def _send_sp_alert(self, row: dict, new_away: str | None, new_home: str | None) -> None:
        away_team = row.get("away_team", "?")
        home_team = row.get("home_team", "?")
        game_time = row.get("game_time", "?")
        today     = _today()

        # Check for pending bets on this game
        bets = _db.get_bets(date=today, unresolved_only=True)
        game_label = f"{away_team} @ {home_team}"
        pending = [b for b in bets if game_label.lower() in (b.get("game") or "").lower()]

        for side, orig_name, orig_xfip, new_name in (
            ("AWAY", row.get("away_sp_name", "?"), row.get("away_sp_xfip"), new_away),
            ("HOME", row.get("home_sp_name", "?"), row.get("home_sp_xfip"), new_home),
        ):
            if new_name is None:
                continue

            xfip_str = f"{orig_xfip:.2f}" if orig_xfip else "N/A"
            action = self._cancellation_verdict(orig_xfip, new_name)

            lines = [
                f"⚠️ SP CHANGE — {away_team} @ {home_team}",
                f"Expected ({side}): {orig_name} (xFIP {xfip_str})",
                f"Now showing: {new_name}",
                f"Game time: {game_time} ET",
                f"Action: {action}",
                f"Bet logged: {'YES' if pending else 'NO'}",
            ]
            for bet in pending:
                stake = float(bet.get("stake") or 0)
                bet_label = bet.get("bet", "?")
                lines.append(
                    f"🚨 YOU HAVE ${stake:.2f} ON {bet_label} — "
                    f"SP CHANGE INVALIDATES THIS BET\n"
                    f"Consider cancelling on Kalshi immediately"
                )
            self._send("\n".join(lines))

    @staticmethod
    def _cancellation_verdict(orig_xfip: float | None, new_sp_name: str) -> str:
        if new_sp_name in ("TBD", "", None) or not new_sp_name.strip():
            return "CANCEL RECOMMENDED — replacement SP unknown"
        if orig_xfip is None:
            return "Review any bet on this game — original analysis is invalid"
        # Can't get new SP's xFIP without a DB lookup; verdict is directional
        return "Review any bet on this game — original analysis is invalid"

    # ── Lineup Change Detection ────────────────────────────────────────────────

    def _check_lineup_changes(self) -> None:
        today = _today()
        tracked = _db.get_lineup_tracker(today)
        if not tracked:
            return

        seen_games: set[str] = set()
        for row in tracked:
            gk = str(row.get("game_pk", ""))
            if gk in seen_games or not gk:
                continue
            seen_games.add(gk)
            self._check_game_lineups(today, gk, tracked)

    def _check_game_lineups(self, today: str, game_pk: str, tracked: list) -> None:
        url = f"{MLB_API}/game/{game_pk}/boxscore"
        data = _mlb_get(url)
        if not data:
            return

        teams_data = data.get("teams", {})
        for side_key, side_label in (("away", "away"), ("home", "home")):
            team_data = teams_data.get(side_key, {})
            team_abbr = (
                team_data.get("team", {}).get("abbreviation")
                or team_data.get("team", {}).get("name", "?")
            )
            batters = team_data.get("battingOrder", [])
            if not batters:
                continue

            players = team_data.get("players", {})
            confirmed_top4 = []
            for pid in batters[:4]:
                pkey = f"ID{pid}"
                pinfo = players.get(pkey, {})
                pname = pinfo.get("person", {}).get("fullName", str(pid))
                confirmed_top4.append({"id": pid, "name": pname})

            _db.update_confirmed_lineup(today, team_abbr, confirmed_top4)

            proj_row = next(
                (r for r in tracked if str(r.get("game_pk")) == game_pk
                 and r.get("team") == team_abbr), None
            )
            if not proj_row or proj_row.get("alert_sent"):
                continue

            proj_raw = proj_row.get("projected_lineup")
            if not proj_raw:
                continue
            try:
                projected_top4 = json.loads(proj_raw)[:4]
            except (json.JSONDecodeError, TypeError):
                continue

            proj_ids = {p.get("id") for p in projected_top4}
            conf_ids = {p["id"] for p in confirmed_top4}
            missing = [p for p in projected_top4 if p.get("id") not in conf_ids]

            if missing:
                changes = [p.get("name", "?") for p in missing]
                _db.mark_lineup_alert_sent(today, team_abbr, json.dumps(changes))
                opp_side = "home" if side_key == "away" else "away"
                opp_team = (
                    teams_data.get(opp_side, {})
                    .get("team", {}).get("abbreviation", "?")
                )
                game_label = f"{team_abbr} @ {opp_team}" if side_key == "away" else f"{opp_team} @ {team_abbr}"
                for missing_player in missing:
                    slot = next(
                        (i + 1 for i, p in enumerate(projected_top4)
                         if p.get("id") == missing_player.get("id")), "?"
                    )
                    self._send_lineup_alert(team_abbr, game_label, missing_player["name"], slot)
                _log.info("Lineup change: %s — missing %s", team_abbr, changes)

    def _send_lineup_alert(self, team: str, game_label: str, player_name: str, slot) -> None:
        msg = (
            f"⚠️ LINEUP CHANGE — {game_label}\n"
            f"{player_name} NOT in confirmed lineup\n"
            f"Original model assumed {player_name} would bat #{slot}\n"
            f"Offensive wRC+ adjustment: recalculate\n"
            f"Impact: MEDIUM — key bat missing from top of order"
        )
        self._send(msg)

    # ── IL Transaction Monitor ─────────────────────────────────────────────────

    def _check_il_transactions(self) -> None:
        today = _today()
        url = f"{MLB_API}/transactions"
        params = {"sportId": 1, "date": today}
        data = _mlb_get(url, params)
        if not data:
            return

        il_codes = {"IL10", "IL15", "IL60", "DFA", "TRAN"}
        transactions = data.get("transactions", [])
        if not transactions:
            return

        # Load today's tracked games for cross-reference
        tracked_sps = _db.get_sp_tracker(today)
        game_lookup: dict[str, dict] = {
            f"{r['away_team'].upper()} @ {r['home_team'].upper()}": r
            for r in tracked_sps
        }

        for tx in transactions:
            type_cd = (tx.get("typeCode") or "").upper()
            if type_cd not in il_codes:
                continue

            player    = tx.get("person", {}).get("fullName", "?")
            team_abbr = tx.get("fromTeam", {}).get("abbreviation") or tx.get("toTeam", {}).get("abbreviation", "?")
            move_desc = tx.get("description", type_cd)

            # Find if this team plays today
            game_row = next(
                (r for r in tracked_sps
                 if r.get("away_team", "").upper() == team_abbr.upper()
                 or r.get("home_team", "").upper() == team_abbr.upper()),
                None,
            )
            game_info = "No game today" if not game_row else (
                f"{game_row['away_team']} @ {game_row['home_team']} at {game_row.get('game_time', '?')} ET"
            )

            # Check for pending bets
            bets = _db.get_bets(date=today, unresolved_only=True)
            pending = [b for b in bets if team_abbr.upper() in (b.get("game") or "").upper()]
            pending_str = f"YES ${sum(float(b.get('stake') or 0) for b in pending):.2f}" if pending else "NO"

            msg = (
                f"🏥 IL TRANSACTION — {player} ({team_abbr})\n"
                f"Move: {type_cd}\n"
                f"Today's game: {game_info}\n"
                f"Impact on model: Key player IL'd — review any bet on this game\n"
                f"Pending bet: {pending_str}"
            )
            self._send(msg)
            _log.info("IL transaction: %s (%s) — %s", player, team_abbr, type_cd)


# ── Monitor status report (used by /monitor command) ─────────────────────────

def get_monitor_status() -> str:
    """Return formatted /monitor status string for Telegram."""
    today   = _today()
    now_str = _now_et().strftime("%-I:%M %p ET")
    date_str = _now_et().strftime("%B %-d, %Y")
    tracked = _db.get_sp_tracker(today)

    if not tracked:
        return (
            f"📡 SP MONITOR STATUS — {date_str}\n"
            f"No games tracked yet | Last check: {now_str}\n"
            f"Run a scout first to populate the tracker."
        )

    changed = [r for r in tracked if r.get("sp_changed")]
    status_line = f"✅ No SP changes detected" if not changed else f"⚠️ {len(changed)} SP change(s) detected"

    lines = [
        f"📡 SP MONITOR STATUS — {date_str}",
        f"Tracking {len(tracked)} games | Last check: {now_str}",
        status_line,
        "Games monitored:",
    ]
    for row in tracked:
        away    = row.get("away_team", "?")
        home    = row.get("home_team", "?")
        away_sp = row.get("away_sp_name", "TBD")
        home_sp = row.get("home_sp_name", "TBD")
        flag    = "⚠️" if row.get("sp_changed") else "✅"
        lines.append(f"  {away} @ {home} — {away_sp} vs {home_sp} {flag}")

    return "\n".join(lines)
