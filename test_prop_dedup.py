"""
FIX #3 (2026-07-29 prop-slip review): tonight's slip had 6 exact duplicate
prop pairs (same player, prop, odds, model%, edge% appearing twice) — e.g.
A.J. Ewing SO O0.5, Matt Olson SO O0.5, Drake Baldwin HR O0.5. Root cause:
The Odds API listed one real-world game (ATL@NYM) twice under two different
event IDs a few minutes apart, so the whole game — and every hitter prop in
it — was analyzed and appended twice in a single scout run. Fixed at the
source in market_engine._dedup_events (see test_event_dedup.py) and, as a
second layer of defense here, brain.py's _daily_bet_slip now dedupes
all_player_props / _over_cap_props on (player, stat, game-or-team) before
they're logged or sent — so even an unrelated future bug feeding the same
pick twice can't double-write a bets row or double-weight a pick in
calibration stats.

Run with: python -m pytest test_prop_dedup.py -v
"""

from unittest.mock import patch

import pytest


class TestPropDedupGuard:
    @pytest.fixture(autouse=True)
    def _isolated_db(self, tmp_path):
        import db
        tmp_db = str(tmp_path / "prop_dedup.db")
        with patch.object(db, "DB_PATH", tmp_db):
            db.init_db()
            yield db

    def _mk_ml_analysis(self, away_name, home_name, side, odds, model_p, stake, confidence=70):
        return {
            "away_name": away_name, "home_name": home_name,
            "away": away_name[:3].upper(), "home": home_name[:3].upper(),
            f"{side}_name": away_name if side == "away" else home_name,
            f"best_{side}_odds": odds,
            f"{side}_model_p": model_p,
            f"{side}_nv": 0.45,
            f"{side}_edge": 8.0,
            f"{side}_stake": stake,
            f"{side}_conv": "HIGH",
            f"{side}_confidence_score": confidence,
            "away_lineup_confirmed": True,
            "home_lineup_confirmed": True,
            "h2h": {},
        }

    def test_exact_duplicate_hitter_props_collapsed_to_one_bets_row(self, _isolated_db):
        """Same player/prop/line/game appearing twice in all_hitter_props
        (as it did the night ATL@NYM was double-analyzed) must produce
        exactly one bets row, not two."""
        import brain
        locks = [(self._mk_ml_analysis("Team A", "Team B", "away", "-150", 0.60, 20.0), "away")]
        dup_hitter_prop = {
            "player": "Matt Olson", "team": "ATL", "game": "Atlanta Braves @ New York Mets",
            "prop": "HR O0.5", "line": 0.5, "model_prob": 0.20, "market_p": 0.197,
            "edge_pct": 6.0, "stake": 5.0,
        }
        brain._daily_bet_slip(
            locks, [], [], [], 1000.0,
            all_hitter_props=[dict(dup_hitter_prop), dict(dup_hitter_prop)],
        )
        rows = [b for b in _isolated_db.get_bets()
                if b.get("type") == "PROP" and "Matt Olson" in b.get("bet", "")]
        assert len(rows) == 1

    def test_same_player_different_game_not_deduped(self, _isolated_db):
        """A player with genuinely two different props in two different real
        games on the same day (rare but possible — e.g. a doubleheader) must
        NOT be collapsed."""
        import brain
        locks = [(self._mk_ml_analysis("Team A", "Team B", "away", "-150", 0.60, 20.0), "away")]
        prop_game1 = {
            "player": "Matt Olson", "team": "ATL", "game": "Atlanta Braves @ New York Mets (G1)",
            "prop": "HR O0.5", "line": 0.5, "model_prob": 0.20, "market_p": 0.197,
            "edge_pct": 6.0, "stake": 5.0,
        }
        prop_game2 = {
            "player": "Matt Olson", "team": "ATL", "game": "Atlanta Braves @ New York Mets (G2)",
            "prop": "HR O0.5", "line": 0.5, "model_prob": 0.21, "market_p": 0.20,
            "edge_pct": 6.5, "stake": 5.0,
        }
        brain._daily_bet_slip(
            locks, [], [], [], 1000.0,
            all_hitter_props=[prop_game1, prop_game2],
        )
        rows = [b for b in _isolated_db.get_bets()
                if b.get("type") == "PROP" and "Matt Olson" in b.get("bet", "")]
        assert len(rows) == 2

    def test_same_player_different_prop_type_not_deduped(self, _isolated_db):
        """Different prop types/lines for the same player in the same game
        (e.g. HR O0.5 and TB O1.5) are legitimately different picks."""
        import brain
        locks = [(self._mk_ml_analysis("Team A", "Team B", "away", "-150", 0.60, 20.0), "away")]
        hr_prop = {
            "player": "Matt Olson", "team": "ATL", "game": "Atlanta Braves @ New York Mets",
            "prop": "HR O0.5", "line": 0.5, "model_prob": 0.20, "market_p": 0.197,
            "edge_pct": 6.0, "stake": 5.0,
        }
        tb_prop = {
            "player": "Matt Olson", "team": "ATL", "game": "Atlanta Braves @ New York Mets",
            "prop": "TB O1.5", "line": 1.5, "model_prob": 0.55, "market_p": 0.422,
            "edge_pct": 12.0, "stake": 6.0,
        }
        brain._daily_bet_slip(
            locks, [], [], [], 1000.0,
            all_hitter_props=[hr_prop, tb_prop],
        )
        rows = [b for b in _isolated_db.get_bets()
                if b.get("type") == "PROP" and "Matt Olson" in b.get("bet", "")]
        assert len(rows) == 2
