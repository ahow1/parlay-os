"""PARLAY OS — weather_engine.py
Fetches ballpark weather via wttr.in and adjusts run expectancy.
"""

import math
import requests
from constants import BALLPARK_CITIES, LG_RPG

# wttr.in format: JSON v1
WTTR_URL = "https://wttr.in/{city}?format=j1"


def _run_adjustment(wind_mph: float, wind_dir: int, temp_f: float,
                    humidity: int, precip_mm: float) -> float:
    """
    Compute total game run adjustment (both teams combined) in runs-per-game.
    Positive = more runs expected, negative = fewer runs.
    """
    delta = 0.0

    # Wind direction classification relative to CF (180° = blowing out to CF)
    angle_from_out = abs(wind_dir - 180)
    if angle_from_out > 180:
        angle_from_out = 360 - angle_from_out

    if wind_mph >= 10:
        if angle_from_out <= 45:          # blowing out to CF
            delta += 0.8 if wind_mph >= 15 else 0.4
        elif angle_from_out >= 135:       # blowing in from CF
            delta -= 0.8 if wind_mph >= 15 else 0.4
        else:                             # crosswind
            delta += 0.1

    # Temperature
    if temp_f < 40:
        delta -= 0.6
    elif temp_f < 50:
        delta -= 0.3

    # Humidity
    if humidity > 80:
        delta += 0.2

    # Precipitation (not in user spec but kept: heavy rain suppresses runs)
    if precip_mm > 5:
        delta -= 0.3
    elif precip_mm > 2:
        delta -= 0.1

    return round(delta, 2)


def _delta_to_factor(delta_runs: float) -> float:
    """
    Convert game-total run adjustment to per-team run factor.
    run_factor is applied to each team, so total game effect = 2 × per-team effect.
    run_factor = 1 + (delta/2) / LG_RPG
    """
    return round(1.0 + (delta_runs / 2.0) / LG_RPG, 4)


def _wind_label(wind_mph: float, wind_dir: int) -> str:
    if wind_mph < 5:
        return "calm"
    angle_from_out = abs(wind_dir - 180)
    if angle_from_out > 180:
        angle_from_out = 360 - angle_from_out
    if angle_from_out <= 45:
        return f"{wind_mph:.0f}mph out"
    elif angle_from_out >= 135:
        return f"{wind_mph:.0f}mph in"
    else:
        return f"{wind_mph:.0f}mph cross"


def get_weather(team_code: str) -> dict:
    """Return weather dict for a team's ballpark city."""
    city = BALLPARK_CITIES.get(team_code)
    if not city:
        return _default_weather(team_code)

    try:
        url = WTTR_URL.format(city=city.replace(" ", "+"))
        r = requests.get(url, timeout=8)
        r.raise_for_status()
        data = r.json()

        current = data["current_condition"][0]
        temp_f    = float(current.get("temp_F", 72))
        wind_mph  = float(current.get("windspeedMiles", 0))
        wind_dir  = int(current.get("winddirDegree", 180))
        humidity  = int(current.get("humidity", 50))
        precip_mm = float(current.get("precipMM", 0))
        desc      = current.get("weatherDesc", [{}])[0].get("value", "")

        run_adj    = _run_adjustment(wind_mph, wind_dir, temp_f, humidity, precip_mm)
        run_factor = _delta_to_factor(run_adj)
        note       = _weather_note(temp_f, wind_mph, wind_dir, humidity, precip_mm, run_adj)

        return {
            "team":           team_code,
            "city":           city,
            "temp_f":         temp_f,
            "wind_mph":       wind_mph,
            "wind_dir":       wind_dir,
            "wind_label":     _wind_label(wind_mph, wind_dir),
            "humidity":       humidity,
            "precip_mm":      precip_mm,
            "desc":           desc,
            "run_adjustment": run_adj,     # delta runs/game (+ = more runs)
            "run_factor":     run_factor,  # multiplier applied per-team
            "note":           note,
        }
    except Exception as e:
        return _default_weather(team_code, error=str(e))


def _weather_note(temp_f, wind_mph, wind_dir, humidity, precip_mm, run_adj) -> str:
    notes = []
    if precip_mm > 3:
        notes.append(f"rain {precip_mm:.1f}mm")
    if wind_mph >= 10:
        notes.append(_wind_label(wind_mph, wind_dir))
    if temp_f < 40:
        notes.append(f"cold {temp_f:.0f}°F")
    elif temp_f < 50:
        notes.append(f"cool {temp_f:.0f}°F")
    elif temp_f >= 90:
        notes.append(f"hot {temp_f:.0f}°F")
    if humidity > 80:
        notes.append(f"humid {humidity}%")
    if run_adj != 0:
        sign = "+" if run_adj > 0 else ""
        notes.append(f"Δ{sign}{run_adj:.1f}R")
    return ", ".join(notes) if notes else "neutral"


def _default_weather(team_code: str, error: str = "") -> dict:
    return {
        "team":           team_code,
        "city":           BALLPARK_CITIES.get(team_code, ""),
        "temp_f":         72.0,
        "wind_mph":       0.0,
        "wind_dir":       180,
        "wind_label":     "calm",
        "humidity":       50,
        "precip_mm":      0.0,
        "desc":           "unknown",
        "run_adjustment": 0.0,
        "run_factor":     1.0,
        "note":           f"default (error: {error})" if error else "default",
    }


if __name__ == "__main__":
    import sys
    team = sys.argv[1].upper() if len(sys.argv) > 1 else "SF"
    w = get_weather(team)
    print(f"{team} @ {w['city']}: {w['temp_f']}°F, {w['wind_label']}, humidity={w['humidity']}%")
    print(f"  run_adj={w['run_adjustment']:+.2f}R  run_factor={w['run_factor']}  note={w['note']}")
