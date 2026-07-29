"""
Regression test: FanGraphs pitching leaderboard must not silently truncate.

Root cause (found 2026-07-29): FG_PITCHING_URL had no pageitems param, so
FanGraphs returned only its default 30-row page (sorted by WAR desc) while
totalCount in the same response was 761 — 81% of that night's actual
starters missed get_real_xfip() and silently fell through to the
_xfip_estimate() formula fallback, even though data_health reported the
feed as "live" (it was — just paginated).

Run with:  python3 test_fangraphs_pagination.py
"""

import sys

import sp_engine


def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def test_leaderboard_not_truncated_to_default_page():
    board, source = sp_engine._xfip_leaderboard()
    if source != "fangraphs":
        fail(f"expected source='fangraphs', got '{source}' (feed may be down — "
             f"rerun once connectivity is confirmed before trusting this result)")
    # FanGraphs' default page size is 30. A real MLB pitching leaderboard
    # (qual=0, full season) has several hundred rows. Anything near 30 means
    # pageitems stopped working (param renamed, endpoint changed, etc.).
    if len(board) < 200:
        fail(f"leaderboard only has {len(board)} pitchers — pagination is "
             f"truncating again (expected 200+)")
    print(f"OK: leaderboard has {len(board)} pitchers (not truncated)")


def test_established_starter_present():
    # Logan Webb: a real, qualified full-time starter who is NOT top-30 in
    # WAR on a given day — a good canary for "did we fall back to the
    # default 30-row page" (confirmed missing from that truncated page
    # during the 2026-07-29 investigation).
    xfip = sp_engine.get_real_xfip("Logan Webb")
    if xfip is None:
        fail("Logan Webb missing from FanGraphs leaderboard — pagination "
             "regression or name-matching regression")
    print(f"OK: Logan Webb xFIP={xfip} found in leaderboard")


if __name__ == "__main__":
    test_leaderboard_not_truncated_to_default_page()
    test_established_starter_present()
    print("All FanGraphs pagination tests passed.")
