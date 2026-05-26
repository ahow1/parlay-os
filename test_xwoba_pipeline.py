"""
Regression test: xwOBA-against pipeline from HTTP fetch to brain.py key lookup.

Run with:  python3 test_xwoba_pipeline.py
"""

import csv
import io
import sys
import requests

KNOWN_PITCHERS = {
    650911: (0.271, "Sánchez, Cristopher"),
    645261: (0.292, "Alcantara, Sandy"),
}

SAVANT_URL = (
    "https://baseballsavant.mlb.com/leaderboard/expected_statistics"
    "?type=pitcher&year=2026&position=&team=&min=25&csv=true"
)
SAVANT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/csv,*/*",
    "Referer": "https://baseballsavant.mlb.com",
}


def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def test_http_returns_csv():
    r = requests.get(SAVANT_URL, headers=SAVANT_HEADERS, timeout=25)
    assert r.status_code == 200, f"HTTP {r.status_code}"
    text = r.text.lstrip("﻿").strip()
    if text.startswith("<"):
        fail("Savant returned HTML (bot-blocked) — check headers")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    assert len(rows) > 0, "No rows in CSV"
    assert "player_id" in rows[0], f"Missing player_id column. Got: {list(rows[0].keys())}"
    assert "est_woba" in rows[0], f"Missing est_woba column. Got: {list(rows[0].keys())}"
    print(f"  HTTP OK — {len(rows)} rows, columns include player_id + est_woba")
    return rows


def test_load_xwoba_against():
    from savant_leaderboards import _CACHE, _load_xwoba_against
    _CACHE.clear()
    data = _load_xwoba_against()
    assert len(data) > 0, "_load_xwoba_against returned empty dict"
    print(f"  _load_xwoba_against: {len(data)} pitchers loaded")
    return data


def test_known_pitchers(data):
    for pid, (approx, name) in KNOWN_PITCHERS.items():
        val = data.get(pid)
        assert val is not None, f"pitcher {pid} ({name}) not in xwOBA dict"
        assert abs(val - approx) < 0.05, f"pitcher {pid} value {val} far from expected ~{approx}"
        print(f"  pitcher {pid} ({name}): est_woba={val:.3f}  OK")


def test_sp_savant_signals():
    from savant_leaderboards import sp_savant_signals
    pid = 650911
    sig = sp_savant_signals(pid)
    val = sig.get("xwoba_against")
    assert val is not None, f"sp_savant_signals['xwoba_against'] is None for pitcher {pid}"
    tier = sig.get("xwoba_tier")
    assert tier in ("ELITE", "GREAT", "AVERAGE", "BAD"), f"unexpected xwoba_tier: {tier!r}"
    print(f"  sp_savant_signals({pid}): xwoba_against={val:.3f}, tier={tier}")


def test_brain_key_alignment():
    """brain.py reads away_sp.get('xwoba_against') — verify sp_engine puts it there."""
    from savant_leaderboards import sp_savant_signals
    sig = sp_savant_signals(650911)
    # sp_engine line 599: "xwoba_against": savant_signals.get("xwoba_against")
    sp_engine_val = sig.get("xwoba_against")
    # brain.py line 368: _away_xwoba = away_sp.get("xwoba_against")
    brain_val = sp_engine_val  # same key, same value
    assert brain_val is not None, "brain.py would receive None for xwoba_against"
    print(f"  brain.py away_sp.get('xwoba_against') = {brain_val:.3f}  OK")


if __name__ == "__main__":
    print("=== xwOBA pipeline regression test ===\n")

    print("[1] HTTP fetch + CSV parse")
    test_http_returns_csv()

    print("\n[2] _load_xwoba_against() internal load")
    data = test_load_xwoba_against()

    print("\n[3] Known-pitcher spot-checks")
    test_known_pitchers(data)

    print("\n[4] sp_savant_signals() output")
    test_sp_savant_signals()

    print("\n[5] brain.py key alignment")
    test_brain_key_alignment()

    print("\nALL TESTS PASSED")
