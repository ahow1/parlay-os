"""PARLAY OS — weather_engine.py
Fetches ballpark weather via wttr.in and adjusts run expectancy.
"""

import requests
from constants import BALLPARK_CITIES

# wttr.in format: JSON v1
WTTR_URL = "https://wttr.in/{city}?format=j1"

# Wind direction → runs adjustment
# Blowing out (to CF ~180°) boosts runs, blowing in (from CF ~0°/360°) suppresses
def _wind_run_factor(wind_dir_deg: int, wind_mph: float) -> float:
    if wind_mph < 5:
        return 1.0
    # Normalize: 180° = straight out to CF = max offense
    # 0°/360° = straight in = max defense
    # Use cosine of deviation from 180° (blowing out)
    import math
    angle_from_out = abs(wind_dir_deg - 180)
    if angle_from_out > 180:
        angle_from_out = 360 - angle_from_out
    # cos(0)=1 → blowing straight out, cos(180)=-1 → blowing straight in
    cos_factor = math.cos(math.radians(angle_from_out))
    # Each mph blowing out adds ~0.5% to runs; blowing in subtracts ~0.5%
    return round(1.0 + cos_factor * wind_mph * 0.005, 4)


def _temp_run_factor(temp_f: float) -> float:
    # Ball travels ~0.5% farther per 10°F above 70°F baseline
    # Below 50°F: slight suppression
    if temp_f >= 70:
        return round(1.0 + (temp_f - 70) * 0.0005, 4)
    elif temp_f < 50:
        return round(1.0 - (50 - temp_f) * 0.003, 4)
    return 1.0


def _precip_run_factor(precip_mm: float, humidity: int) -> float:
    # Heavy rain (>5mm) → suppress runs (slower ball, pitcher grip issues both ways)
    # High humidity slightly helps hitters (less air resistance)
    factor = 1.0
    if precip_mm > 5:
        factor -= 0.03
    elif precip_mm > 2:
        factor -= 0.01
    if humidity > 75:
        factor += 0.005
    return round(factor, 4)


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
        temp_f      = float(current.get("temp_F", 72))
        wind_mph    = float(current.get("windspeedMiles", 0))
        wind_dir    = int(current.get("winddirDegree", 180))
        humidity    = int(current.get("humidity", 50))
        precip_mm   = float(current.get("precipMM", 0))
        desc        = current.get("weatherDesc", [{}])[0].get("value", "")

        wind_factor  = _wind_run_factor(wind_dir, wind_mph)
        temp_factor  = _temp_run_factor(temp_f)
        precip_factor= _precip_run_factor(precip_mm, humidity)
        run_factor   = round(wind_factor * temp_factor * precip_factor, 4)

        return {
            "team":         team_code,
            "city":         city,
            "temp_f":       temp_f,
            "wind_mph":     wind_mph,
            "wind_dir":     wind_dir,
            "humidity":     humidity,
            "precip_mm":    precip_mm,
            "desc":         desc,
            "wind_factor":  wind_factor,
            "temp_factor":  temp_factor,
            "precip_factor":precip_factor,
            "run_factor":   run_factor,
            "note":         _weather_note(temp_f, wind_mph, wind_dir, precip_mm),
        }
    except Exception as e:
        return _default_weather(team_code, error=str(e))


def _weather_note(temp_f, wind_mph, wind_dir, precip_mm) -> str:
    notes = []
    if precip_mm > 3:
        notes.append(f"rain {precip_mm:.1f}mm")
    if wind_mph >= 15:
        if 90 <= wind_dir <= 270:
            notes.append(f"wind {wind_mph:.0f}mph out")
        else:
            notes.append(f"wind {wind_mph:.0f}mph in")
    elif wind_mph >= 8:
        notes.append(f"wind {wind_mph:.0f}mph")
    if temp_f >= 90:
        notes.append(f"hot {temp_f:.0f}°F")
    elif temp_f < 50:
        notes.append(f"cold {temp_f:.0f}°F")
    return ", ".join(notes) if notes else "neutral"


def _default_weather(team_code: str, error: str = "") -> dict:
    return {
        "team":         team_code,
        "city":         BALLPARK_CITIES.get(team_code, ""),
        "temp_f":       72.0,
        "wind_mph":     0.0,
        "wind_dir":     180,
        "humidity":     50,
        "precip_mm":    0.0,
        "desc":         "unknown",
        "wind_factor":  1.0,
        "temp_factor":  1.0,
        "precip_factor":1.0,
        "run_factor":   1.0,
        "note":         f"default (error: {error})" if error else "default",
    }


if __name__ == "__main__":
    import sys
    team = sys.argv[1].upper() if len(sys.argv) > 1 else "SF"
    w = get_weather(team)
    print(f"{team} @ {w['city']}: {w['temp_f']}°F, wind {w['wind_mph']}mph dir {w['wind_dir']}°")
    print(f"  run_factor={w['run_factor']}  note={w['note']}")
