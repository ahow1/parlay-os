"""PARLAY OS — umpire_engine.py
Builds and maintains umpire database from MLB Stats API historical data.
Computes home win adjustment and total run adjustment for today's HP umpire.

Edge signals:
  home_win_adj: +0.02 when umpire home_win_rate > 54% (historically favors home)
  total_adj:    +0.03 when avg_runs/game > 9.5 (over-lean); -0.03 when < 8.5 (under-lean)
Falls back to UMPIRE_TENDENCIES seed data when DB history is insufficient.
"""

import threading
from datetime import date, timedelta
from api_client import get as _http_get
from constants import UMPIRE_TENDENCIES

STATSAPI = "https://statsapi.mlb.com/api/v1"

HOME_WIN_RATE_THRESHOLD = 0.54
LOW_RUN_THRESHOLD       = 8.5
HIGH_RUN_THRESHOLD      = 9.5
MIN_GAMES_FOR_SIGNAL    = 20

_ump_lock  = threading.Lock()
_ump_cache: dict | None = None


def _name_key(name: str) -> str:
    import re, unicodedata
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z ]", "", name.lower()).strip()


def _load_from_db() -> dict:
    try:
        import db as _db
        return _db.get_all_umpire_stats()
    except Exception:
        return {}


def refresh_umpire_stats(days: int = 14) -> dict:
    """
    Fetch last N days of final MLB games with umpire/linescore data.
    Updates DB umpire_stats table. Returns {name: stats_dict}.
    """
    today  = date.today().isoformat()
    cutoff = (date.today() - timedelta(days=days)).isoformat()

    raw: dict[str, dict] = {}

    current = cutoff
    while current <= today:
        try:
            r = _http_get(
                f"{STATSAPI}/schedule",
                params={
                    "sportId":  1,
                    "date":     current,
                    "hydrate":  "officials,linescore",
                    "gameType": "R",
                },
                timeout=12,
            )
            for day in r.json().get("dates", []):
                for game in day.get("games", []):
                    if game.get("status", {}).get("abstractGameCode", "") not in ("F", "O"):
                        continue
                    ump_name = ""
                    for off in game.get("officials", []):
                        if off.get("officialType") == "Home Plate":
                            ump_name = off.get("official", {}).get("fullName", "")
                            break
                    if not ump_name:
                        continue
                    ls       = game.get("linescore") or {}
                    ls_teams = ls.get("teams", {})
                    away_r   = int(ls_teams.get("away", {}).get("runs", 0) or 0)
                    home_r   = int(ls_teams.get("home", {}).get("runs", 0) or 0)
                    total    = away_r + home_r
                    away_k   = int(ls_teams.get("away", {}).get("strikeOuts", 0) or 0)
                    home_k   = int(ls_teams.get("home", {}).get("strikeOuts", 0) or 0)

                    e = raw.setdefault(ump_name, {
                        "games": 0, "home_wins": 0,
                        "total_runs": 0.0, "total_k": 0, "overs": 0,
                    })
                    e["games"]      += 1
                    e["home_wins"]  += 1 if home_r > away_r else 0
                    e["total_runs"] += total
                    e["total_k"]    += away_k + home_k
                    e["overs"]      += 1 if total > 8.5 else 0

        except Exception as exc:
            print(f"[UMP] fetch error {current}: {exc}")

        try:
            current = (date.fromisoformat(current) + timedelta(days=1)).isoformat()
        except Exception:
            break

    result: dict[str, dict] = {}
    for name, e in raw.items():
        g = e["games"]
        if g == 0:
            continue
        stats = {
            "name":           name,
            "games":          g,
            "home_win_rate":  round(e["home_wins"] / g, 4),
            "avg_runs":       round(e["total_runs"] / g, 2),
            "k_rate":         round(e["total_k"] / g, 2),
            "over_rate":      round(e["overs"] / g, 4),
            "updated_date":   today,
        }
        result[name] = stats
        try:
            import db as _db
            _db.upsert_umpire_stats(stats)
        except Exception as exc:
            print(f"[UMP] DB upsert error ({name}): {exc}")

    print(f"[UMP] refresh_umpire_stats: {len(result)} umpires from {days}d history")
    with _ump_lock:
        global _ump_cache
        _ump_cache = result
    return result


def _get_stats() -> dict:
    global _ump_cache
    with _ump_lock:
        if _ump_cache is not None:
            return _ump_cache
    stats = _load_from_db()
    with _ump_lock:
        _ump_cache = stats
    return stats


def get_umpire_edge(name: str) -> dict:
    """
    Return umpire adjustment dict for the 12-factor model.

    home_win_adj: +0.02 if home_win_rate > 54%, else 0.
    total_adj:    +0.03 if avg_runs > 9.5; -0.03 if < 8.5; else 0.
    Falls back to UMPIRE_TENDENCIES seed data if fewer than MIN_GAMES_FOR_SIGNAL games.
    """
    if not name:
        return {"home_win_adj": 0.0, "total_adj": 0.0, "tag": "", "has_data": False}

    stats = _get_stats()
    entry = stats.get(name) or stats.get(_name_key(name))

    if entry and entry.get("games", 0) >= MIN_GAMES_FOR_SIGNAL:
        hw   = entry["home_win_rate"]
        avg  = entry["avg_runs"]
        kr   = entry.get("k_rate", 14.0)

        home_win_adj = 0.02 if hw > HOME_WIN_RATE_THRESHOLD else 0.0

        if avg > HIGH_RUN_THRESHOLD:
            total_adj = 0.03
        elif avg < LOW_RUN_THRESHOLD:
            total_adj = -0.03
        else:
            total_adj = 0.0

        tag_parts = []
        if home_win_adj > 0:
            tag_parts.append(f"Home {hw:.0%} win rate")
        if total_adj > 0:
            tag_parts.append(f"Over lean ({avg:.1f} runs/g)")
        elif total_adj < 0:
            tag_parts.append(f"Under lean ({avg:.1f} runs/g)")
        if kr > 16.0:
            tag_parts.append(f"K-heavy ({kr:.0f}K/g)")
        elif kr < 11.5:
            tag_parts.append(f"Contact zone ({kr:.0f}K/g)")

        return {
            "home_win_adj": home_win_adj,
            "total_adj":    total_adj,
            "tag":          " | ".join(tag_parts),
            "profile":      entry,
            "has_data":     True,
        }

    # Fallback: translate UMPIRE_TENDENCIES run_factor to total_adj
    k_factor, run_factor, note = UMPIRE_TENDENCIES.get(name, (1.0, 1.0, ""))
    if run_factor >= 1.04:
        total_adj = 0.03
    elif run_factor <= 0.96:
        total_adj = -0.03
    else:
        total_adj = 0.0
    return {
        "home_win_adj": 0.0,
        "total_adj":    total_adj,
        "tag":          note,
        "profile":      {},
        "has_data":     False,
    }


def umpire_telegram_flag(name: str, edge: dict | None = None) -> str:
    """Short Telegram FLAGS line for umpire edge."""
    if not name:
        return ""
    if edge is None:
        edge = get_umpire_edge(name)
    tag = edge.get("tag", "")
    if not tag:
        return ""
    return f"UMP EDGE — {name}: {tag}"


if __name__ == "__main__":
    import sys
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 14
    result = refresh_umpire_stats(days=days)
    print(f"\nUmpire stats ({len(result)} umpires):")
    for n, s in sorted(result.items(), key=lambda x: -x[1]["games"])[:10]:
        print(f"  {n}: {s['games']}g  home={s['home_win_rate']:.1%}  "
              f"avg_r={s['avg_runs']:.2f}  k_r={s['k_rate']:.1f}  over={s['over_rate']:.1%}")
    # Also test edge lookup for a known umpire
    ump = sys.argv[2] if len(sys.argv) > 2 else "Vic Carapazza"
    edge = get_umpire_edge(ump)
    print(f"\nEdge for {ump}: {edge}")
