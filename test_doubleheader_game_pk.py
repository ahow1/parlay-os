"""
FIX #3 (corrected root cause, 2026-07-29): the actual bug behind tonight's
duplicate props wasn't The Odds API listing one event twice (that theory,
tested in test_event_dedup.py, turned out to be wrong when verified live) --
it was _resolve_game_pk() picking the FIRST schedule match for a team name
unconditionally. On a real doubleheader day (confirmed live: ATL@NYM 2026-07
-29, gamePk 823596 Game 1 at 17:10 UTC with AJ Smith-Shawver/Sean Manaea,
gamePk 823598 Game 2 at 23:10 UTC with Chris Sale/Christian Scott), The Odds
API correctly returns two distinct events with distinct commence_time ~6h
apart, but both resolved to Game 1's gamePk -- so Game 2 got Game 1's
lineups/pitchers/props analyzed a second time under its own event, and the
slip-assembly dedup guard then silently (and wrongly) dropped all of Game
2's "picks" as duplicates of Game 1's, real ones. Game 2 got zero actual
prop coverage.

Fix: _resolve_game_pk() now takes commence_utc and, when a team-pair has
multiple games that day, picks whichever schedule entry's real start time
is closest to it.

Run with: python -m pytest test_doubleheader_game_pk.py -v
"""

from unittest.mock import patch, MagicMock

from brain import _resolve_game_pk


def _mk_schedule_response(games: list) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {"dates": [{"games": games}]}
    return resp


def _mk_game(game_pk, away, home, game_date, game_number=1):
    return {
        "gamePk": game_pk,
        "gameNumber": game_number,
        "gameDate": game_date,
        "teams": {
            "away": {"team": {"name": away}},
            "home": {"team": {"name": home}},
        },
    }


DH_GAME_1 = _mk_game(823596, "Atlanta Braves", "New York Mets", "2026-07-29T17:10:00Z", 1)
DH_GAME_2 = _mk_game(823598, "Atlanta Braves", "New York Mets", "2026-07-29T23:10:00Z", 2)


def test_single_game_returns_it_regardless_of_commence_utc():
    with patch("brain._http_get", return_value=_mk_schedule_response([DH_GAME_1])):
        pk = _resolve_game_pk("Atlanta Braves", "2026-07-29", "2026-07-29T23:11:00Z")
    assert pk == 823596


def test_doubleheader_game1_event_resolves_to_game1_pk():
    with patch("brain._http_get", return_value=_mk_schedule_response([DH_GAME_1, DH_GAME_2])):
        pk = _resolve_game_pk("Atlanta Braves", "2026-07-29", "2026-07-29T17:11:00Z")
    assert pk == 823596


def test_doubleheader_game2_event_resolves_to_game2_pk():
    """This is the exact case that was broken: the Odds API event for
    Game 2 (commence_time close to 23:10 UTC) must resolve to Game 2's
    real gamePk, not silently fall back to Game 1's."""
    with patch("brain._http_get", return_value=_mk_schedule_response([DH_GAME_1, DH_GAME_2])):
        pk = _resolve_game_pk("Atlanta Braves", "2026-07-29", "2026-07-29T23:11:00Z")
    assert pk == 823598


def test_missing_commence_utc_falls_back_to_first_match():
    """Backward compatible: callers that don't pass commence_utc (or an
    unparseable one) get the old first-match behavior, not a crash."""
    with patch("brain._http_get", return_value=_mk_schedule_response([DH_GAME_1, DH_GAME_2])):
        pk = _resolve_game_pk("Atlanta Braves", "2026-07-29", "")
    assert pk == 823596


def test_malformed_commence_utc_does_not_crash():
    with patch("brain._http_get", return_value=_mk_schedule_response([DH_GAME_1, DH_GAME_2])):
        pk = _resolve_game_pk("Atlanta Braves", "2026-07-29", "not-a-timestamp")
    assert pk == 823596


def test_no_matching_team_returns_none():
    with patch("brain._http_get", return_value=_mk_schedule_response([DH_GAME_1, DH_GAME_2])):
        pk = _resolve_game_pk("Houston Astros", "2026-07-29", "2026-07-29T23:11:00Z")
    assert pk is None
