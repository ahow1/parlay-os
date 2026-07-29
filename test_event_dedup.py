"""
FIX #3 (2026-07-29 prop-slip review): The Odds API occasionally lists the
same real-world game twice under two different event IDs, a few minutes
apart in commence_time (confirmed 2026-07-29 — ATL@NYM was analyzed twice
in one scout run with identical probable starters, which fanned out into
6 exact-duplicate hitter-prop picks, each logged as its own bets-table row
and its own CLV/calibration entry).

get_mlb_events() now runs its raw event list through _dedup_events(),
which collapses same-team-pair entries whose commence_time is within
_DEDUP_WINDOW_MINUTES of each other. A real doubleheader's two games are
hours apart, so this must never collapse a legitimate DH into one game.

Run with: python -m pytest test_event_dedup.py -v
"""

from market_engine import _dedup_events


def _ev(id_, away, home, commence_utc):
    return {"id": id_, "away": away, "home": home, "commence_utc": commence_utc}


def test_exact_duplicate_listing_collapsed():
    events = [
        _ev("abc123", "Atlanta Braves", "New York Mets", "2026-07-29T23:10:00Z"),
        _ev("def456", "Atlanta Braves", "New York Mets", "2026-07-29T23:11:00Z"),
    ]
    result = _dedup_events(events)
    assert len(result) == 1
    assert result[0]["id"] == "abc123"   # first-seen kept


def test_different_matchups_both_kept():
    events = [
        _ev("abc123", "Atlanta Braves", "New York Mets", "2026-07-29T23:10:00Z"),
        _ev("ghi789", "Houston Astros", "Los Angeles Angels", "2026-07-30T02:07:00Z"),
    ]
    result = _dedup_events(events)
    assert len(result) == 2


def test_real_doubleheader_both_games_kept():
    """Day/night doubleheader — same two teams, hours apart. Must NOT be
    collapsed, or a real second game's picks would silently vanish."""
    events = [
        _ev("dh_game1", "Chicago Cubs", "St. Louis Cardinals", "2026-07-29T17:10:00Z"),
        _ev("dh_game2", "Chicago Cubs", "St. Louis Cardinals", "2026-07-29T23:40:00Z"),
    ]
    result = _dedup_events(events)
    assert len(result) == 2


def test_home_away_swap_not_treated_as_duplicate():
    """A genuinely different game (teams swapped sides) must never collapse
    into one — only an exact (away, home) match within the time window is a
    duplicate listing."""
    events = [
        _ev("swap1", "Atlanta Braves", "New York Mets", "2026-07-29T23:10:00Z"),
        _ev("swap2", "New York Mets", "Atlanta Braves", "2026-07-29T23:15:00Z"),
    ]
    result = _dedup_events(events)
    assert len(result) == 2


def test_malformed_commence_utc_does_not_crash():
    events = [
        _ev("a", "Atlanta Braves", "New York Mets", "2026-07-29T23:10:00Z"),
        _ev("b", "Atlanta Braves", "New York Mets", "not-a-timestamp"),
    ]
    result = _dedup_events(events)
    assert len(result) == 2   # can't prove duplicate → keep both, fail safe
