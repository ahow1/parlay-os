"""PARLAY OS — savant_leaderboards.py
Baseball Savant + Prospect Savant leaderboard fetcher with 6-hour in-memory cache.

Leaderboards are fetched at most once per 6 hours; per-player lookups are O(1) dict reads.
Graceful fallback (None) when endpoint is unavailable or player not in data.
"""

import csv
import io
import time
import logging
from api_client import get as _http_get

log = logging.getLogger(__name__)

SAVANT_BASE   = "https://baseballsavant.mlb.com"
PS_BASE       = "https://prospectsavant.com/api"
CACHE_TTL     = 6 * 3600   # 6 hours
TIMEOUT       = 20
CURRENT_YEAR  = 2026

# ── In-memory cache ───────────────────────────────────────────────────────────
# Each entry: {ts: float, data: dict}  where data keyed by int player_id
_CACHE: dict[str, dict] = {}


def _cache_valid(key: str) -> bool:
    entry = _CACHE.get(key)
    return bool(entry and (time.time() - entry["ts"]) < CACHE_TTL)


def _get_cached(key: str) -> dict:
    return _CACHE.get(key, {}).get("data", {})


def _set_cache(key: str, data: dict) -> None:
    _CACHE[key] = {"ts": time.time(), "data": data}


# ── CSV fetch + parse helpers ─────────────────────────────────────────────────

def _flt(val: str | None) -> float | None:
    if not val or val in ("", "null", "NA", "nan", "."):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _fetch_csv(url: str, params: dict | None = None) -> list[dict]:
    """Fetch a CSV from Savant; return list of row dicts. Returns [] on error."""
    try:
        r = _http_get(
            url,
            params=params,
            timeout=TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/2.0)"},
            skip_cache=False,
        )
        if r.status_code != 200 or not r.text.strip():
            return []
        text = r.text.lstrip("﻿")
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        return rows
    except Exception as e:
        log.debug(f"[SAVANT] fetch_csv {url}: {e}")
        return []


def _player_id(row: dict, *cols) -> int | None:
    """Extract player_id from a row, trying multiple column name candidates."""
    for col in cols:
        v = row.get(col)
        if v:
            try:
                return int(v)
            except (ValueError, TypeError):
                pass
    return None


# ── ADD 1: xwOBA against (pitcher leaderboard) ───────────────────────────────

_XWOBA_KEY = "xwoba_against"

def _load_xwoba_against() -> dict:
    if _cache_valid(_XWOBA_KEY):
        return _get_cached(_XWOBA_KEY)
    rows = _fetch_csv(
        f"{SAVANT_BASE}/leaderboard/expected_statistics",
        {"type": "pitcher", "year": CURRENT_YEAR, "min": "q", "csv": "true"},
    )
    data: dict[int, float] = {}
    for row in rows:
        pid = _player_id(row, "pitcher_id", "player_id")
        if pid is None:
            # Try name-based fallback
            for k, v in row.items():
                if "player_id" in k.lower():
                    try:
                        pid = int(v)
                        break
                    except (ValueError, TypeError):
                        pass
        if pid is None:
            continue
        # Savant column is typically "est_woba_against" or "xwoba"
        val = _flt(row.get("est_woba_against") or row.get("xwoba") or row.get("xwoba_against"))
        if val is not None:
            data[pid] = val
    _set_cache(_XWOBA_KEY, data)
    log.info(f"[SAVANT] xwOBA against: {len(data)} pitchers loaded")
    return data


def get_xwoba_against(pitcher_id: int) -> float | None:
    """Return pitcher's season xwOBA against (ADD 1). None if not available."""
    return _load_xwoba_against().get(pitcher_id)


def xwoba_tier(val: float | None) -> tuple[str, int]:
    """(tier_label, conf_adj) — ELITE +3, GREAT +2, AVERAGE 0, BAD -2."""
    if val is None:
        return "UNKNOWN", 0
    if val < 0.280:
        return "ELITE", 3
    if val < 0.310:
        return "GREAT", 2
    if val < 0.340:
        return "AVERAGE", 0
    return "BAD", -2


# ── ADD 2 & 3: Pitch arsenal — run value + whiff rate ────────────────────────

_ARSENAL_KEY = "pitch_arsenal"

def _load_pitch_arsenal() -> dict:
    """Dict: pitcher_id → {pitch_type: {run_value_per_100, whiff_percent}}."""
    if _cache_valid(_ARSENAL_KEY):
        return _get_cached(_ARSENAL_KEY)
    data: dict[int, dict] = {}
    for sort_col in ("run_value_per_100", "whiff_percent"):
        rows = _fetch_csv(
            f"{SAVANT_BASE}/leaderboard/pitch-arsenal-stats",
            {"year": CURRENT_YEAR, "min_pitches": 100, "sort": sort_col, "csv": "true"},
        )
        for row in rows:
            pid = _player_id(row, "pitcher_id", "player_id")
            if pid is None:
                continue
            pt = (row.get("pitch_type") or row.get("pitch_name") or "").upper().strip()
            if not pt:
                continue
            entry = data.setdefault(pid, {})
            pitch_entry = entry.setdefault(pt, {})
            rv  = _flt(row.get("run_value_per_100") or row.get("rv_per_100") or row.get("run_value"))
            wh  = _flt(row.get("whiff_percent") or row.get("whiff_rate") or row.get("whiff_pct"))
            if rv is not None:
                pitch_entry["run_value_per_100"] = rv
            if wh is not None:
                pitch_entry["whiff_percent"] = wh
    _set_cache(_ARSENAL_KEY, data)
    log.info(f"[SAVANT] Pitch arsenal: {len(data)} pitchers loaded")
    return data


def get_pitch_arsenal(pitcher_id: int) -> dict:
    """Return {pitch_type: {run_value_per_100, whiff_percent}} for pitcher."""
    return _load_pitch_arsenal().get(pitcher_id, {})


def arsenal_summary(pitcher_id: int) -> dict:
    """
    Summarized signals from pitch arsenal (ADDs 2+3):
      best_rv: best (most negative) run_value_per_100
      primary_rv: run value of most-used pitch
      max_whiff: highest single-pitch whiff %
      k_conf_adj: confidence adjustment from arsenal quality
    """
    arsenal = get_pitch_arsenal(pitcher_id)
    if not arsenal:
        return {"best_rv": None, "primary_rv": None, "max_whiff": None, "k_conf_adj": 0}

    rvs = [v.get("run_value_per_100") for v in arsenal.values() if v.get("run_value_per_100") is not None]
    whs = [v.get("whiff_percent") for v in arsenal.values() if v.get("whiff_percent") is not None]

    best_rv  = min(rvs) if rvs else None
    # Primary pitch = first entry (Savant returns sorted by usage frequency)
    primary_rv = next((v.get("run_value_per_100") for v in arsenal.values()
                       if v.get("run_value_per_100") is not None), None)
    max_whiff = max(whs) if whs else None

    k_conf_adj = 0
    if best_rv is not None and best_rv < -2.0:
        k_conf_adj += 5    # elite pitch dominates
    if primary_rv is not None and primary_rv > 1.5:
        k_conf_adj -= 10   # primary pitch is hittable
    if max_whiff is not None and max_whiff > 35.0:
        k_conf_adj += 5    # elite K pitch regardless of SwStr%

    return {
        "best_rv": best_rv,
        "primary_rv": primary_rv,
        "max_whiff": max_whiff,
        "k_conf_adj": k_conf_adj,
    }


# ── ADD 4: Arm angle ──────────────────────────────────────────────────────────

_ARM_KEY = "arm_angle"

def _load_arm_angles() -> dict:
    if _cache_valid(_ARM_KEY):
        return _get_cached(_ARM_KEY)
    rows = _fetch_csv(
        f"{SAVANT_BASE}/leaderboard/pitcher-arm-angles",
        {"year": CURRENT_YEAR, "csv": "true"},
    )
    data: dict[int, float] = {}
    for row in rows:
        pid = _player_id(row, "pitcher_id", "player_id")
        if pid is None:
            continue
        val = _flt(row.get("arm_angle") or row.get("release_angle") or row.get("angle"))
        if val is not None:
            data[pid] = val
    _set_cache(_ARM_KEY, data)
    log.info(f"[SAVANT] Arm angles: {len(data)} pitchers loaded")
    return data


def get_arm_angle(pitcher_id: int) -> float | None:
    """Arm angle in degrees. <15=sidearm, >60=overhand."""
    return _load_arm_angles().get(pitcher_id)


def arm_angle_platoon_adj(arm_angle: float | None, batter_hand: str = "R") -> float:
    """
    Sidearm (<15°) has +5% advantage vs same-handed batters.
    Returns probability adjustment (0.0 = neutral).
    """
    if arm_angle is None:
        return 0.0
    if arm_angle < 15.0:
        return 0.05
    return 0.0


# ── ADD 5: Pitch movement ─────────────────────────────────────────────────────

_MOVEMENT_KEY = "pitch_movement"

def _load_pitch_movement() -> dict:
    if _cache_valid(_MOVEMENT_KEY):
        return _get_cached(_MOVEMENT_KEY)
    rows = _fetch_csv(
        f"{SAVANT_BASE}/leaderboard/pitch-movement",
        {"year": CURRENT_YEAR, "csv": "true"},
    )
    data: dict[int, dict] = {}
    for row in rows:
        pid = _player_id(row, "pitcher_id", "player_id")
        if pid is None:
            continue
        pfx_x = _flt(row.get("pfx_x") or row.get("horz_break") or row.get("horizontal_break"))
        pfx_z = _flt(row.get("pfx_z") or row.get("induced_vert_break") or row.get("vert_break"))
        total_move = None
        if pfx_x is not None and pfx_z is not None:
            import math
            total_move = round(math.sqrt(pfx_x**2 + pfx_z**2), 2)
        elif pfx_x is not None:
            total_move = abs(pfx_x)
        elif pfx_z is not None:
            total_move = abs(pfx_z)
        data[pid] = {"pfx_x": pfx_x, "pfx_z": pfx_z, "total_movement": total_move}
    _set_cache(_MOVEMENT_KEY, data)
    log.info(f"[SAVANT] Pitch movement: {len(data)} pitchers loaded")
    return data


def get_pitch_movement(pitcher_id: int) -> dict:
    return _load_pitch_movement().get(pitcher_id, {})


def movement_k_adj(pitcher_id: int) -> float:
    """ADD 5: +3% K confidence if total movement > 15in."""
    mv = get_pitch_movement(pitcher_id)
    total = mv.get("total_movement")
    if total is not None and total > 15.0:
        return 0.03
    return 0.0


# ── ADD 6: Active spin ────────────────────────────────────────────────────────

_SPIN_KEY = "active_spin"

def _load_active_spin() -> dict:
    if _cache_valid(_SPIN_KEY):
        return _get_cached(_SPIN_KEY)
    rows = _fetch_csv(
        f"{SAVANT_BASE}/leaderboard/active-spin",
        {"year": CURRENT_YEAR, "csv": "true"},
    )
    data: dict[int, dict] = {}
    for row in rows:
        pid = _player_id(row, "pitcher_id", "player_id")
        if pid is None:
            continue
        fb_spin = _flt(row.get("active_spin_fastball") or row.get("fastball_active_spin_percent"))
        br_spin = _flt(row.get("active_spin_breaking") or row.get("breaking_active_spin_percent"))
        data[pid] = {"active_spin_fastball": fb_spin, "active_spin_breaking": br_spin}
    _set_cache(_SPIN_KEY, data)
    log.info(f"[SAVANT] Active spin: {len(data)} pitchers loaded")
    return data


def get_active_spin(pitcher_id: int) -> dict:
    return _load_active_spin().get(pitcher_id, {})


def active_spin_conf_adj(pitcher_id: int) -> int:
    """ADD 6: +2 if either active spin >95%, -2 if both <85%."""
    spins = get_active_spin(pitcher_id)
    vals  = [v for v in (spins.get("active_spin_fastball"), spins.get("active_spin_breaking")) if v is not None]
    if not vals:
        return 0
    if any(v > 95.0 for v in vals):
        return 2
    if all(v < 85.0 for v in vals):
        return -2
    return 0


# ── ADD 7: ABS first-pitch strike rate ───────────────────────────────────────

_ABS_KEY = "abs_fps"

def _load_abs_fps() -> dict:
    if _cache_valid(_ABS_KEY):
        return _get_cached(_ABS_KEY)
    rows = _fetch_csv(
        f"{SAVANT_BASE}/leaderboard/abs-challenges",
        {"year": CURRENT_YEAR, "csv": "true"},
    )
    data: dict[int, float] = {}
    for row in rows:
        pid = _player_id(row, "pitcher_id", "player_id")
        if pid is None:
            continue
        fps = _flt(row.get("first_pitch_strike_rate") or row.get("fps_pct") or row.get("fp_strike"))
        if fps is not None:
            data[pid] = fps
    _set_cache(_ABS_KEY, data)
    log.info(f"[SAVANT] ABS FPS: {len(data)} pitchers loaded")
    return data


def get_abs_fps(pitcher_id: int) -> float | None:
    """First pitch strike rate from ABS system. >65%=command bonus, <55%=penalty."""
    return _load_abs_fps().get(pitcher_id)


def abs_fps_model_adj(pitcher_id: int) -> float:
    """ADD 7: model win prob adjustment from FPS. +3% at >65%, -2% at <55%."""
    fps = get_abs_fps(pitcher_id)
    if fps is None:
        return 0.0
    if fps > 65.0:
        return 0.03
    if fps < 55.0:
        return -0.02
    return 0.0


# ── ADD 8: Rolling 10-game xwOBA ─────────────────────────────────────────────

_ROLLING_KEY = "rolling_xwoba"

def _load_rolling_xwoba() -> dict:
    if _cache_valid(_ROLLING_KEY):
        return _get_cached(_ROLLING_KEY)
    rows = _fetch_csv(
        f"{SAVANT_BASE}/leaderboard/rolling",
        {"type": "pitcher", "year": CURRENT_YEAR, "metric": "xwoba", "window": 10, "csv": "true"},
    )
    data: dict[int, dict] = {}
    for row in rows:
        pid = _player_id(row, "pitcher_id", "player_id")
        if pid is None:
            continue
        rolling = _flt(row.get("rolling_xwoba") or row.get("xwoba_rolling") or row.get("rolling"))
        season  = _flt(row.get("xwoba") or row.get("season_xwoba"))
        data[pid] = {"rolling_xwoba": rolling, "season_xwoba": season}
    _set_cache(_ROLLING_KEY, data)
    log.info(f"[SAVANT] Rolling xwOBA: {len(data)} pitchers loaded")
    return data


def get_rolling_xwoba(pitcher_id: int) -> dict:
    return _load_rolling_xwoba().get(pitcher_id, {})


def rolling_xwoba_tier(pitcher_id: int) -> str:
    """ADD 8: DECLINING if rolling .030+ worse than season, PEAKING if .030+ better."""
    d = get_rolling_xwoba(pitcher_id)
    rolling = d.get("rolling_xwoba")
    season  = d.get("season_xwoba")
    if rolling is None or season is None:
        return "UNKNOWN"
    diff = rolling - season   # positive = xwOBA against went up = worse for pitcher
    if diff >= 0.030:
        return "DECLINING"
    if diff <= -0.030:
        return "PEAKING"
    return "STABLE"


# ── ADD 9: Year-over-year xwOBA changes ──────────────────────────────────────

_YOY_KEY = "yoy_xwoba"

def _load_yoy_xwoba() -> dict:
    if _cache_valid(_YOY_KEY):
        return _get_cached(_YOY_KEY)
    rows = _fetch_csv(
        f"{SAVANT_BASE}/leaderboard/statcast-year-to-year",
        {"type": "pitcher", "year": CURRENT_YEAR, "csv": "true"},
    )
    data: dict[int, float] = {}
    for row in rows:
        pid = _player_id(row, "pitcher_id", "player_id")
        if pid is None:
            continue
        diff = _flt(row.get("xwoba_diff") or row.get("est_woba_diff") or row.get("xwoba_change"))
        if diff is not None:
            data[pid] = diff
    _set_cache(_YOY_KEY, data)
    log.info(f"[SAVANT] YoY xwOBA: {len(data)} pitchers loaded")
    return data


def get_yoy_xwoba_diff(pitcher_id: int) -> float | None:
    """xwOBA difference YoY. Negative = pitcher improved."""
    return _load_yoy_xwoba().get(pitcher_id)


def yoy_conf_adj(pitcher_id: int) -> int:
    """ADD 9: BREAKOUT improved .020+=+3 conf, DECLINING worsened .020+=-3 conf."""
    diff = get_yoy_xwoba_diff(pitcher_id)
    if diff is None:
        return 0
    if diff <= -0.020:   # pitcher improved
        return 3
    if diff >= 0.020:    # pitcher worsened
        return -3
    return 0


# ── ADD 10: Pitch tempo ───────────────────────────────────────────────────────

_TEMPO_KEY = "pitch_tempo"

def _load_pitch_tempo() -> dict:
    if _cache_valid(_TEMPO_KEY):
        return _get_cached(_TEMPO_KEY)
    rows = _fetch_csv(
        f"{SAVANT_BASE}/leaderboard/pitch-tempo",
        {"type": "Pit", "year": CURRENT_YEAR, "csv": "true"},
    )
    data: dict[int, float] = {}
    for row in rows:
        pid = _player_id(row, "pitcher_id", "player_id")
        if pid is None:
            continue
        tempo = _flt(row.get("avg_pitch_tempo") or row.get("average_tempo") or row.get("tempo"))
        if tempo is not None:
            data[pid] = tempo
    _set_cache(_TEMPO_KEY, data)
    log.info(f"[SAVANT] Pitch tempo: {len(data)} pitchers loaded")
    return data


def get_pitch_tempo(pitcher_id: int) -> float | None:
    """Average seconds between pitches."""
    return _load_pitch_tempo().get(pitcher_id)


def tempo_label(pitcher_id: int) -> str:
    """ADD 10: QUICK_WORKER <18s, SLOW_WORKER >25s, NORMAL otherwise."""
    tempo = get_pitch_tempo(pitcher_id)
    if tempo is None:
        return "UNKNOWN"
    if tempo < 18.0:
        return "QUICK_WORKER"
    if tempo > 25.0:
        return "SLOW_WORKER"
    return "NORMAL"


# ── ADD 11: Bat tracking (blast speed) ───────────────────────────────────────

_BAT_KEY = "bat_tracking"

def _load_bat_tracking() -> dict:
    if _cache_valid(_BAT_KEY):
        return _get_cached(_BAT_KEY)
    rows = _fetch_csv(
        f"{SAVANT_BASE}/leaderboard/bat-tracking",
        {"year": CURRENT_YEAR, "csv": "true"},
    )
    data: dict[int, float] = {}
    for row in rows:
        pid = _player_id(row, "batter", "player_id", "batter_id")
        if pid is None:
            continue
        blast = _flt(row.get("blast") or row.get("swing_speed") or row.get("bat_speed"))
        if blast is not None:
            data[pid] = blast
    _set_cache(_BAT_KEY, data)
    log.info(f"[SAVANT] Bat tracking: {len(data)} batters loaded")
    return data


def get_blast(batter_id: int) -> float | None:
    """Bat speed / blast mph. >75=elite, <68=below avg."""
    return _load_bat_tracking().get(batter_id)


def blast_tb_adj(batter_id: int) -> float:
    """ADD 11: λ adjustment for TB/HR. +5% if blast >75, -5% if <68."""
    blast = get_blast(batter_id)
    if blast is None:
        return 0.0
    if blast > 75.0:
        return 0.05
    if blast < 68.0:
        return -0.05
    return 0.0


# ── ADD 12: Batter percentile rankings ───────────────────────────────────────

_PCTILE_KEY = "batter_percentiles"

def _load_batter_percentiles() -> dict:
    if _cache_valid(_PCTILE_KEY):
        return _get_cached(_PCTILE_KEY)
    rows = _fetch_csv(
        f"{SAVANT_BASE}/leaderboard/percentile-rankings",
        {"type": "batter", "year": CURRENT_YEAR, "csv": "true"},
    )
    data: dict[int, dict] = {}
    for row in rows:
        pid = _player_id(row, "player_id", "batter", "batter_id")
        if pid is None:
            continue
        data[pid] = {
            "xwoba_pctile":   _flt(row.get("xwoba") or row.get("xwoba_percentile")),
            "barrel_pctile":  _flt(row.get("barrel_batted_rate") or row.get("barrel_pct")),
            "k_pctile":       _flt(row.get("k_percent") or row.get("strikeout_percent")),
            "bb_pctile":      _flt(row.get("bb_percent") or row.get("walk_percent")),
            "hard_hit_pctile":_flt(row.get("hard_hit_percent") or row.get("hard_hit")),
        }
    _set_cache(_PCTILE_KEY, data)
    log.info(f"[SAVANT] Batter percentiles: {len(data)} batters loaded")
    return data


def get_batter_percentiles(batter_id: int) -> dict:
    return _load_batter_percentiles().get(batter_id, {})


def percentile_tb_adj(batter_id: int) -> float:
    """ADD 12: +5% TB conf if xwOBA >70th pctile; -5% if K% >70th."""
    pct = get_batter_percentiles(batter_id)
    adj = 0.0
    xwp = pct.get("xwoba_pctile")
    kp  = pct.get("k_pctile")
    if xwp is not None and xwp > 70:
        adj += 0.05
    if kp is not None and kp > 70:
        adj -= 0.05
    return adj


# ── ADD 13: Sprint speed ──────────────────────────────────────────────────────

_SPRINT_KEY = "sprint_speed"

def _load_sprint_speed() -> dict:
    if _cache_valid(_SPRINT_KEY):
        return _get_cached(_SPRINT_KEY)
    rows = _fetch_csv(
        f"{SAVANT_BASE}/leaderboard/sprint_speed",
        {"year": CURRENT_YEAR, "csv": "true"},
    )
    data: dict[int, float] = {}
    for row in rows:
        pid = _player_id(row, "player_id", "batter", "sprint_speed_player_id")
        if pid is None:
            continue
        speed = _flt(row.get("sprint_speed") or row.get("hp_to_first"))
        if speed is not None:
            data[pid] = speed
    _set_cache(_SPRINT_KEY, data)
    log.info(f"[SAVANT] Sprint speed: {len(data)} players loaded")
    return data


def get_sprint_speed(player_id: int) -> float | None:
    """Sprint speed ft/sec. >28=ELITE."""
    return _load_sprint_speed().get(player_id)


def team_avg_sprint(player_ids: list[int]) -> float | None:
    """Average sprint speed for a list of players. Returns None if no data."""
    speeds = [get_sprint_speed(pid) for pid in player_ids if get_sprint_speed(pid) is not None]
    return round(sum(speeds) / len(speeds), 2) if speeds else None


def sprint_lambda_adj(player_ids: list[int]) -> float:
    """ADD 13: team avg sprint >27.5 adds +0.2 to λ."""
    avg = team_avg_sprint(player_ids)
    if avg is not None and avg > 27.5:
        return 0.2
    return 0.0


# ── ADD 14: Swing/take + chase rate ──────────────────────────────────────────

_SWINGTAKE_KEY = "swing_take"

def _load_swing_take() -> dict:
    if _cache_valid(_SWINGTAKE_KEY):
        return _get_cached(_SWINGTAKE_KEY)
    rows = _fetch_csv(
        f"{SAVANT_BASE}/leaderboard/swing-take",
        {"year": CURRENT_YEAR, "csv": "true"},
    )
    data: dict[int, float] = {}
    for row in rows:
        pid = _player_id(row, "player_id", "batter", "batter_id")
        if pid is None:
            continue
        chase = _flt(row.get("chase_rate") or row.get("oz_swing_percent") or row.get("chase"))
        if chase is not None:
            data[pid] = chase
    _set_cache(_SWINGTAKE_KEY, data)
    log.info(f"[SAVANT] Swing/take: {len(data)} batters loaded")
    return data


def get_chase_rate(batter_id: int) -> float | None:
    """Chase rate %. >32=CHASE_VULNERABLE."""
    return _load_swing_take().get(batter_id)


def is_chase_vulnerable(batter_id: int) -> bool:
    c = get_chase_rate(batter_id)
    return c is not None and c > 32.0


def chase_k_adj(batter_id: int, primary_pitch_type: str = "") -> float:
    """ADD 14: if batter is CHASE_VULNERABLE AND pitcher's primary pitch is breaking ball → +8% K."""
    breaking_balls = {"SL", "CU", "KC", "CB", "SV", "CS", "SLIDER", "CURVE", "CURVEBALL", "SWEEPER"}
    if is_chase_vulnerable(batter_id) and primary_pitch_type.upper().strip() in breaking_balls:
        return 0.08
    return 0.0


# ── ADD 15: Catcher framing ───────────────────────────────────────────────────

_FRAMING_KEY = "catcher_framing"

def _load_catcher_framing() -> dict:
    if _cache_valid(_FRAMING_KEY):
        return _get_cached(_FRAMING_KEY)
    rows = _fetch_csv(
        f"{SAVANT_BASE}/leaderboard/catcher-framing",
        {"year": CURRENT_YEAR, "min": 304, "csv": "true"},
    )
    data: dict[int, float] = {}
    for row in rows:
        pid = _player_id(row, "player_id", "catcher_id", "fielder_2")
        if pid is None:
            continue
        runs = _flt(row.get("runs_extra_strikes") or row.get("framing_runs") or row.get("extra_strikes"))
        if runs is not None:
            data[pid] = runs
    _set_cache(_FRAMING_KEY, data)
    log.info(f"[SAVANT] Catcher framing: {len(data)} catchers loaded")
    return data


def get_catcher_framing_runs(catcher_id: int) -> float | None:
    """Runs from extra strikes. >+10=elite, <-10=poor."""
    return _load_catcher_framing().get(catcher_id)


def framing_k_adj(catcher_id: int) -> float:
    """ADD 15: +3% K conf if elite framer, -3% if poor."""
    runs = get_catcher_framing_runs(catcher_id)
    if runs is None:
        return 0.0
    if runs > 10.0:
        return 0.03
    if runs < -10.0:
        return -0.03
    return 0.0


# ── ADD 16: Outfield jump ────────────────────────────────────────────────────

_OFJUMP_KEY = "of_jump"

def _load_of_jump() -> dict:
    if _cache_valid(_OFJUMP_KEY):
        return _get_cached(_OFJUMP_KEY)
    rows = _fetch_csv(
        f"{SAVANT_BASE}/leaderboard/outfield_jump",
        {"year": CURRENT_YEAR, "csv": "true"},
    )
    data: dict[int, float] = {}
    for row in rows:
        pid = _player_id(row, "player_id", "fielder_id")
        if pid is None:
            continue
        rating = _flt(row.get("jump_rating") or row.get("jump") or row.get("outs_above_average"))
        if rating is not None:
            data[pid] = rating
    _set_cache(_OFJUMP_KEY, data)
    log.info(f"[SAVANT] OF jump: {len(data)} players loaded")
    return data


def get_of_jump(player_id: int) -> float | None:
    return _load_of_jump().get(player_id)


def team_of_lambda_adj(of_ids: list[int]) -> float:
    """ADD 16: elite OF defense (avg jump > 5) subtracts 0.15 from λ; poor (< -5) adds 0.15."""
    ratings = [get_of_jump(pid) for pid in of_ids if get_of_jump(pid) is not None]
    if not ratings:
        return 0.0
    avg = sum(ratings) / len(ratings)
    if avg > 5.0:
        return -0.15
    if avg < -5.0:
        return 0.15
    return 0.0


# ── ADD 17: Baserunning run value ─────────────────────────────────────────────

_BASERUN_KEY = "baserunning_rv"

def _load_baserunning_rv() -> dict:
    """Keyed by team_id (int), not player_id."""
    if _cache_valid(_BASERUN_KEY):
        return _get_cached(_BASERUN_KEY)
    rows = _fetch_csv(
        f"{SAVANT_BASE}/leaderboard/baserunning-run-value",
        {"year": CURRENT_YEAR, "csv": "true"},
    )
    data: dict[int, float] = {}
    for row in rows:
        tid = _player_id(row, "team_id", "fielding_team_id")
        if tid is None:
            continue
        rv = _flt(row.get("baserunning_run_value") or row.get("base_running_runs") or row.get("run_value"))
        if rv is not None:
            data[tid] = rv
    _set_cache(_BASERUN_KEY, data)
    log.info(f"[SAVANT] Baserunning RV: {len(data)} teams loaded")
    return data


def get_baserunning_rv(team_id: int) -> float | None:
    return _load_baserunning_rv().get(team_id)


def baserunning_lambda_adj(team_id: int) -> float:
    """ADD 17: >+10 adds +0.1 to λ, <-10 subtracts 0.1."""
    rv = get_baserunning_rv(team_id)
    if rv is None:
        return 0.0
    if rv > 10.0:
        return 0.1
    if rv < -10.0:
        return -0.1
    return 0.0


# ── ADD 18 & 19: Prospect Savant ─────────────────────────────────────────────

_PS_CACHE: dict[int, dict] = {}
_PS_CACHE_TS: dict[int, float] = {}
_PS_TTL = 24 * 3600   # 24-hour cache for individual prospect scores


def _get_ps_player(player_id: int) -> dict:
    """Fetch Prospect Savant data for one player. Cached 24h."""
    now = time.time()
    if player_id in _PS_CACHE and (now - _PS_CACHE_TS.get(player_id, 0)) < _PS_TTL:
        return _PS_CACHE[player_id]
    try:
        r = _http_get(
            f"{PS_BASE}/player/{player_id}",
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/2.0)"},
            skip_cache=False,
        )
        if r.status_code == 200:
            data = r.json()
            result = {
                "ps_score":        _flt(str(data.get("ps_score", ""))),
                "stuff_plus":      _flt(str(data.get("stuff_plus", "") or data.get("stuffPlus", ""))),
                "contact_quality": _flt(str(data.get("contact_quality", ""))),
                "discipline":      _flt(str(data.get("discipline", ""))),
                "agg":             _flt(str(data.get("agg", ""))),
            }
            _PS_CACHE[player_id] = result
            _PS_CACHE_TS[player_id] = now
            return result
    except Exception as e:
        log.debug(f"[PS] player {player_id}: {e}")
    empty = {"ps_score": None, "stuff_plus": None, "contact_quality": None, "discipline": None, "agg": None}
    _PS_CACHE[player_id] = empty
    _PS_CACHE_TS[player_id] = now
    return empty


def get_ps_score(player_id: int) -> dict:
    """ADD 18: Prospect Savant score for SP. Returns dict with ps_score, stuff_plus, etc."""
    return _get_ps_player(player_id)


def ps_conf_adj(player_id: int) -> int:
    """ADD 18: stuff_plus>110 +5, contact_quality>80 +3, discipline>80 +3."""
    d = get_ps_score(player_id)
    adj = 0
    sp = d.get("stuff_plus")
    cq = d.get("contact_quality")
    di = d.get("discipline")
    if sp is not None and sp > 110:
        adj += 5
    if cq is not None and cq > 80:
        adj += 3
    if di is not None and di > 80:
        adj += 3
    return adj


def get_bullpen_stuff_plus(pitcher_ids: list[int]) -> list[float]:
    """ADD 19: stuff_plus values for a list of bullpen arm IDs. Excludes None."""
    result = []
    for pid in pitcher_ids:
        d = _get_ps_player(pid)
        sp = d.get("stuff_plus")
        if sp is not None:
            result.append(sp)
    return result


def bullpen_elite_arms(pitcher_ids: list[int], threshold: float = 115.0) -> int:
    """Count of bullpen arms with stuff_plus > threshold."""
    return sum(1 for v in get_bullpen_stuff_plus(pitcher_ids) if v > threshold)


def bullpen_stuff_lambda_adj(pitcher_ids: list[int]) -> float:
    """ADD 19: 2+ elite relievers (stuff_plus>115) reduces opp λ by 0.2."""
    n_elite = bullpen_elite_arms(pitcher_ids)
    if n_elite >= 2:
        return -0.2
    return 0.0


# ── SP composite signal dict ──────────────────────────────────────────────────

def sp_savant_signals(pitcher_id: int | None) -> dict:
    """
    Aggregate all pitcher-side Savant signals into one dict.
    Safe to call with pitcher_id=None — returns all-neutral defaults.
    """
    if not pitcher_id:
        return _sp_signals_default()

    xwoba       = get_xwoba_against(pitcher_id)
    xwoba_t, xwoba_cadj = xwoba_tier(xwoba)
    arsenal     = arsenal_summary(pitcher_id)
    arm_ang     = get_arm_angle(pitcher_id)
    mv_adj      = movement_k_adj(pitcher_id)
    spin_adj    = active_spin_conf_adj(pitcher_id)
    fps         = get_abs_fps(pitcher_id)
    fps_adj     = abs_fps_model_adj(pitcher_id)
    roll_tier   = rolling_xwoba_tier(pitcher_id)
    yoy_adj     = yoy_conf_adj(pitcher_id)
    tempo       = get_pitch_tempo(pitcher_id)
    tempo_lbl   = tempo_label(pitcher_id)

    return {
        "xwoba_against":    xwoba,
        "xwoba_tier":       xwoba_t,
        "xwoba_conf_adj":   xwoba_cadj,
        "arsenal":          arsenal,
        "k_conf_adj":       arsenal.get("k_conf_adj", 0),
        "arm_angle":        arm_ang,
        "movement_k_adj":   mv_adj,
        "active_spin_adj":  spin_adj,
        "fps_rate":         fps,
        "fps_model_adj":    fps_adj,
        "rolling_tier":     roll_tier,
        "yoy_conf_adj":     yoy_adj,
        "pitch_tempo":      tempo,
        "tempo_label":      tempo_lbl,
    }


def _sp_signals_default() -> dict:
    return {
        "xwoba_against":    None,
        "xwoba_tier":       "UNKNOWN",
        "xwoba_conf_adj":   0,
        "arsenal":          {},
        "k_conf_adj":       0,
        "arm_angle":        None,
        "movement_k_adj":   0.0,
        "active_spin_adj":  0,
        "fps_rate":         None,
        "fps_model_adj":    0.0,
        "rolling_tier":     "UNKNOWN",
        "yoy_conf_adj":     0,
        "pitch_tempo":      None,
        "tempo_label":      "UNKNOWN",
    }


def batter_savant_signals(batter_id: int | None) -> dict:
    """Aggregate all batter-side Savant signals. Safe with batter_id=None."""
    if not batter_id:
        return _batter_signals_default()
    return {
        "blast":          get_blast(batter_id),
        "blast_adj":      blast_tb_adj(batter_id),
        "percentiles":    get_batter_percentiles(batter_id),
        "pctile_adj":     percentile_tb_adj(batter_id),
        "sprint_speed":   get_sprint_speed(batter_id),
        "chase_rate":     get_chase_rate(batter_id),
        "chase_vuln":     is_chase_vulnerable(batter_id),
    }


def _batter_signals_default() -> dict:
    return {
        "blast": None, "blast_adj": 0.0, "percentiles": {},
        "pctile_adj": 0.0, "sprint_speed": None,
        "chase_rate": None, "chase_vuln": False,
    }
