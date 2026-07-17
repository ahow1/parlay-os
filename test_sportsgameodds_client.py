"""Tests for sportsgameodds_client.py (SGO odds source, Step 1).
Run: python -m pytest test_sportsgameodds_client.py -v

All tests use fake SGO payloads / mocked HTTP — no real network calls, no
quota burned. Real-data verification was done manually via
`python sportsgameodds_client.py` against the live API.
"""

import time
from unittest.mock import patch, MagicMock

import sportsgameodds_client as sgo


def _fake_odd(stat, bettype, entity, side, line=None, spread=None,
              books=None, opens=None):
    books = books or {"draftkings": -130}
    opens = opens or {}
    by_bookmaker = {}
    for book, price in books.items():
        by_bookmaker[book] = {
            "odds": price,
            "available": True,
        }
        if book in opens:
            by_bookmaker[book]["openOdds"] = opens[book]
    return {
        "statID": stat,
        "betTypeID": bettype,
        "periodID": "game",
        "statEntityID": entity,
        "sideID": side,
        "bookOverUnder": line,
        "bookSpread": spread,
        "byBookmaker": by_bookmaker,
    }


def _fake_event(event_id="evt1", home="Philadelphia Phillies", away="New York Mets", odds=None):
    return {
        "eventID": event_id,
        "teams": {
            "home": {"names": {"long": home}},
            "away": {"names": {"long": away}},
        },
        "status": {"startsAt": "2026-07-16T23:10:00.000Z"},
        "odds": odds or {},
    }


class TestNormalizeMoneyline:
    def test_decimal_and_implied_prob_match_math_engine(self):
        odds = {
            "ml-home": _fake_odd("points", "ml", "home", "home", books={"draftkings": -130}),
            "ml-away": _fake_odd("points", "ml", "away", "away", books={"draftkings": 110}),
        }
        ev = sgo._normalize_event(_fake_event(odds=odds))

        home = ev["moneyline"]["home"]["draftkings"]
        away = ev["moneyline"]["away"]["draftkings"]

        assert home["american"] == -130
        assert home["decimal"] == sgo.american_to_decimal("-130")
        assert home["implied_prob_pct"] == sgo.implied_prob("-130")

        assert away["american"] == 110
        assert away["decimal"] == sgo.american_to_decimal("110")

    def test_open_vs_current_both_captured(self):
        odds = {
            "ml-home": _fake_odd("points", "ml", "home", "home",
                                  books={"fanduel": -126}, opens={"fanduel": -140}),
        }
        ev = sgo._normalize_event(_fake_event(odds=odds))
        book = ev["moneyline"]["home"]["fanduel"]
        assert book["american"] == -126
        assert book["open_american"] == -140
        assert book["open_decimal"] == sgo.american_to_decimal("-140")

    def test_missing_open_odds_yields_none_not_crash(self):
        odds = {"ml-home": _fake_odd("points", "ml", "home", "home", books={"fanduel": -126})}
        ev = sgo._normalize_event(_fake_event(odds=odds))
        book = ev["moneyline"]["home"]["fanduel"]
        assert book["open_american"] is None
        assert book["open_decimal"] is None

    def test_unavailable_book_excluded(self):
        raw = _fake_odd("points", "ml", "home", "home", books={"fanduel": -126})
        raw["byBookmaker"]["fanduel"]["available"] = False
        ev = sgo._normalize_event(_fake_event(odds={"ml-home": raw}))
        assert "fanduel" not in ev["moneyline"]["home"]


class TestNormalizeTotals:
    def test_over_under_and_line_captured(self):
        odds = {
            "tot-over":  _fake_odd("points", "ou", "all", "over",  line=9.5, books={"draftkings": -110}),
            "tot-under": _fake_odd("points", "ou", "all", "under", line=9.5, books={"draftkings": -105}),
        }
        ev = sgo._normalize_event(_fake_event(odds=odds))
        assert ev["totals"]["line"] == 9.5
        assert ev["totals"]["over"]["draftkings"]["american"] == -110
        assert ev["totals"]["under"]["draftkings"]["american"] == -105


class TestNormalizeSpread:
    def test_home_away_lines_and_prices(self):
        odds = {
            "sp-away": _fake_odd("points", "sp", "away", "away", spread=1.5, books={"draftkings": -180}),
            "sp-home": _fake_odd("points", "sp", "home", "home", spread=-1.5, books={"draftkings": 150}),
        }
        ev = sgo._normalize_event(_fake_event(odds=odds))
        assert ev["spreads"]["away_line"] == 1.5
        assert ev["spreads"]["home_line"] == -1.5
        assert ev["spreads"]["away"]["draftkings"]["american"] == -180
        assert ev["spreads"]["home"]["draftkings"]["american"] == 150


class TestNormalizeProps:
    def test_known_prop_stats_included(self):
        odds = {
            "hits-over": _fake_odd("batting_hits", "ou", "PLAYER_A", "over", line=0.5,
                                    books={"fanduel": 120}),
            "hr-over":   _fake_odd("batting_homeRuns", "ou", "PLAYER_B", "over", line=0.5,
                                    books={"fanduel": 350}),
            "tb-over":   _fake_odd("batting_totalBases", "ou", "PLAYER_C", "over", line=1.5,
                                    books={"fanduel": 100}),
            "k-over":    _fake_odd("pitching_strikeouts", "ou", "PLAYER_D", "over", line=5.5,
                                    books={"fanduel": -110}),
        }
        ev = sgo._normalize_event(_fake_event(odds=odds))
        stats_found = {p["stat"] for p in ev["props"]}
        assert stats_found == {"batter_hits", "batter_home_runs", "batter_total_bases", "pitcher_strikeouts"}

    def test_player_id_and_line_preserved(self):
        odds = {"hits-over": _fake_odd("batting_hits", "ou", "PLAYER_A", "over", line=0.5,
                                        books={"fanduel": 120})}
        ev = sgo._normalize_event(_fake_event(odds=odds))
        prop = ev["props"][0]
        assert prop["player_id"] == "PLAYER_A"
        assert prop["side"] == "over"
        assert prop["line"] == 0.5

    def test_unrelated_stat_ids_excluded(self):
        odds = {"fantasy": _fake_odd("fantasyScore", "ou", "PLAYER_A", "over", line=10,
                                      books={"fanduel": -110})}
        ev = sgo._normalize_event(_fake_event(odds=odds))
        assert ev["props"] == []

    def test_non_game_period_excluded(self):
        """1st-5-innings / 1st-inning micro-markets are out of scope for Step 1."""
        raw = _fake_odd("points", "ml", "home", "home", books={"fanduel": -130})
        raw["periodID"] = "1i"
        ev = sgo._normalize_event(_fake_event(odds={"ml-1i": raw}))
        assert ev["moneyline"] == {}


class TestCaching:
    def test_fresh_cache_skips_http_call(self, tmp_path, monkeypatch):
        cache_file = tmp_path / "sgo_cache.json"
        monkeypatch.setattr(sgo, "CACHE_FILE", str(cache_file))
        fresh_cache = {"fetched_at": time.time(), "data": {"evt1": {"home": "x"}}}
        sgo._save_cache(fresh_cache)

        with patch("sportsgameodds_client._http_get") as mock_get:
            result = sgo.fetch_mlb_slate()

        mock_get.assert_not_called()
        assert result == fresh_cache["data"]

    def test_stale_cache_triggers_refetch_and_resave(self, tmp_path, monkeypatch):
        cache_file = tmp_path / "sgo_cache.json"
        monkeypatch.setattr(sgo, "CACHE_FILE", str(cache_file))
        monkeypatch.setattr(sgo, "SGO_API_KEY", "fake-key")
        stale_cache = {"fetched_at": time.time() - 999999, "data": {"stale": True}}
        sgo._save_cache(stale_cache)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": [_fake_event()]}

        with patch("sportsgameodds_client._http_get", return_value=mock_resp) as mock_get, \
             patch.object(sgo, "_rate_limit"):
            result = sgo.fetch_mlb_slate()

        mock_get.assert_called_once()
        assert "evt1" in result
        reloaded = sgo._load_cache()
        assert "evt1" in reloaded["data"]

    def test_http_failure_falls_back_to_stale_cache(self, tmp_path, monkeypatch):
        cache_file = tmp_path / "sgo_cache.json"
        monkeypatch.setattr(sgo, "CACHE_FILE", str(cache_file))
        monkeypatch.setattr(sgo, "SGO_API_KEY", "fake-key")
        stale_cache = {"fetched_at": time.time() - 999999, "data": {"evt1": {"home": "cached"}}}
        sgo._save_cache(stale_cache)

        with patch("sportsgameodds_client._http_get", side_effect=Exception("network down")), \
             patch.object(sgo, "_rate_limit"):
            result = sgo.fetch_mlb_slate()

        assert result == stale_cache["data"]

    def test_no_api_key_returns_cache_or_empty(self, tmp_path, monkeypatch):
        cache_file = tmp_path / "sgo_cache.json"
        monkeypatch.setattr(sgo, "CACHE_FILE", str(cache_file))
        monkeypatch.setattr(sgo, "SGO_API_KEY", "")
        result = sgo.fetch_mlb_slate()
        assert result == {}


class TestNoVigConsensus:
    """Step 2: no-vig consensus benchmark. Known-example values hand-derived
    by running math_engine.no_vig_prob() on each book pair below and
    averaging (real LAD @ NYY moneyline from the 2026-07-16 SGO slate,
    sgo_cache.json event KxcmCDxK1cnaAUVHTyiq):

    book        away   home
    betmgm      -110   -110  -> 50.00 / 50.00
    bovada      -113   -107  -> 50.65 / 49.35
    caesars     -110   -110  -> 50.00 / 50.00
    draftkings  -111   -108  -> 50.33 / 49.67
    espnbet     -110   -110  -> 50.00 / 50.00
    fanduel     -108   -108  -> 50.00 / 50.00
    pointsbet   -111   -111  -> 50.00 / 50.00
    unibet      -114   -107  -> 50.75 / 49.25
    williamhill -110   -110  -> 50.00 / 50.00
    avg                      -> 50.19 / 49.81
    """

    def test_known_real_game_moneyline(self):
        books_away = {
            "betmgm": -110, "bovada": -113, "caesars": -110, "draftkings": -111,
            "espnbet": -110, "fanduel": -108, "pointsbet": -111, "unibet": -114,
            "williamhill": -110,
        }
        books_home = {
            "betmgm": -110, "bovada": -107, "caesars": -110, "draftkings": -108,
            "espnbet": -110, "fanduel": -108, "pointsbet": -111, "unibet": -107,
            "williamhill": -110,
        }
        odds = {
            "ml-away": _fake_odd("points", "ml", "away", "away", books=books_away),
            "ml-home": _fake_odd("points", "ml", "home", "home", books=books_home),
        }
        ev = sgo._normalize_event(_fake_event(
            home="New York Yankees", away="Los Angeles Dodgers", odds=odds))

        consensus = sgo.no_vig_consensus(ev, market="moneyline")

        assert consensus["n_books"] == 9
        assert consensus["books_used"] == sorted(books_away)
        assert consensus["away_prob_pct"] == 50.19
        assert consensus["home_prob_pct"] == 49.81

    def test_only_books_quoting_both_sides_are_used(self):
        odds = {
            "ml-away": _fake_odd("points", "ml", "away", "away",
                                  books={"draftkings": -110, "fanduel": -108}),
            "ml-home": _fake_odd("points", "ml", "home", "home",
                                  books={"draftkings": -110}),
        }
        ev = sgo._normalize_event(_fake_event(odds=odds))
        consensus = sgo.no_vig_consensus(ev, market="moneyline")
        assert consensus["books_used"] == ["draftkings"]
        assert consensus["n_books"] == 1

    def test_no_common_book_returns_none(self):
        odds = {
            "ml-away": _fake_odd("points", "ml", "away", "away", books={"fanduel": -110}),
            "ml-home": _fake_odd("points", "ml", "home", "home", books={"draftkings": -110}),
        }
        ev = sgo._normalize_event(_fake_event(odds=odds))
        assert sgo.no_vig_consensus(ev, market="moneyline") is None

    def test_totals_market_uses_over_under(self):
        odds = {
            "tot-over":  _fake_odd("points", "ou", "all", "over",  line=9.5,
                                    books={"draftkings": -110, "fanduel": -105}),
            "tot-under": _fake_odd("points", "ou", "all", "under", line=9.5,
                                    books={"draftkings": -110, "fanduel": -115}),
        }
        ev = sgo._normalize_event(_fake_event(odds=odds))
        consensus = sgo.no_vig_consensus(ev, market="totals")
        assert consensus["market"] == "totals"
        assert consensus["n_books"] == 2
        assert "over_prob_pct" in consensus
        assert "under_prob_pct" in consensus

    def test_unsupported_market_raises(self):
        import pytest
        with pytest.raises(ValueError):
            sgo.no_vig_consensus(_fake_event(), market="spread")


class TestGetEventByTeams:
    def test_exact_match(self):
        slate = {"evt1": {"home": "Philadelphia Phillies", "away": "New York Mets"}}
        found = sgo.get_event_by_teams("New York Mets", "Philadelphia Phillies", slate=slate)
        assert found is slate["evt1"]

    def test_no_match_returns_none(self):
        slate = {"evt1": {"home": "Philadelphia Phillies", "away": "New York Mets"}}
        found = sgo.get_event_by_teams("Boston Red Sox", "New York Yankees", slate=slate)
        assert found is None
