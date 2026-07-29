# GitHub Actions Bet Settlement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Get pending bets settling automatically again without a persistent process, by fixing two real settlement bugs (TOTAL and NRFI bet types can never auto-settle today) and wiring the existing settlement check into a scheduled GitHub Actions job.

**Architecture:** `telegram_handler.run_settlement_check()` already exists and correctly settles ML bets (verified against live MLB Stats API data) — the only thing that runs it today is `start_auto_settler()`, which is called exclusively from `brain.py --bot` / `--live`, and Railway isn't deployed. This plan (1) fixes two matching/outcome bugs in `telegram_handler.py` that block TOTAL and NRFI bets specifically, (2) adds a one-shot `brain.py --settle` entry point that calls `run_settlement_check()` once and exits (mirroring the existing `--capture-clv` one-shot pattern), and (3) adds a new scheduled job to `.github/workflows/mega_scout.yml` that runs it and commits the result, the same way every other brain.py flag in that file already works.

**Tech Stack:** Python 3.11, pytest, GitHub Actions, MLB Stats API (statsapi.mlb.com), SQLite.

## Global Constraints

- Never change Kelly multipliers, stake sizing, or bankroll logic — this plan touches settlement/grading only.
- Auto-settled bets that are `over_cap=1` must stay silent (no Telegram ping) — existing behavior in `run_settlement_check`, must not regress.
- PROP and PARLAY bet types are explicitly out of scope for this plan — no auto-settle path exists for them and building one (box-score-driven prop grading) is a separate, larger project. Do not attempt it here.
- All new code must have a passing test before being considered done (TDD) — this codebase already tests `_determine_outcome` per-branch in `test_runline.py`; follow that exact pattern.
- Do not touch Railway config or deployment — Railway is intentionally not in use right now.

---

## Background — confirmed root causes (read this before starting)

Two independent, verified bugs in `telegram_handler.py` block auto-settlement for scout-generated TOTAL and NRFI bets. Both were confirmed by reading the exact bet-generation code in `brain.py`:

**TOTAL bug:** `brain.py:2303` and `brain.py:2318` log every TOTAL pick with `type="TOTAL"` literally, and put the real pick info in the `bet` text field instead: `bet=f"{bet['game']} {bet['direction']} {bet['line']}"` — e.g. `"Los Angeles Dodgers @ New York Yankees UNDER 8.5"`. But `_determine_outcome` (`telegram_handler.py:1336`) only recognizes an O/U bet when `bet_type[0] in ("O", "U")` — i.e. when `type` itself is literally `"O8.5"` or `"U7.5"`. That convention is real and still used by **manually-logged** Telegram bets (`parse_bet()`, `telegram_handler.py:283-352`, produces `type="O8.5"` for a `/bet ... over 8.5 ...` command) — so that existing branch must not be touched or removed. A new, separate `TOTAL` branch is needed that parses direction/line out of the `bet` text.

Compounding this: even with the outcome branch fixed, **the game-matching step also fails** for TOTAL (and NRFI) bets. `run_settlement_check` (`telegram_handler.py:1426-1440`) matches a pending bet to a final game by calling `_game_side(g, bet["bet"])` — which expects `bet["bet"]` to be a single team name/code. For TOTAL/NRFI bets `bet["bet"]` is the full composite string (`"... UNDER 8.5"` / `"... YRFI"`), which never matches any team name, so `matched_game` stays `None` forever regardless of the outcome-branch fix. The `game` column, however, already holds a clean `"Away Team @ Home Team"` matchup string for these bet types (confirmed via `SELECT bet,type,game FROM bets WHERE type IN ('TOTAL','NRFI')`) — that's what needs to be matched on instead.

**NRFI bug:** `brain.py:2188` and `brain.py:2201` log every NRFI pick with `type="NRFI"` literally and `bet=f"{bet['game']} {bet['direction']}"` where `direction` is `"NRFI"` or `"YRFI"` (confirmed at `brain.py:4122-4123`: `direction = "NRFI" if nrfi_note == "nrfi" else "YRFI"`). `_determine_outcome` has no `NRFI` branch at all — it falls through every `if` and returns `None` unconditionally. Same matching problem as TOTAL applies here too, since `bet["bet"]` is `"Away @ Home YRFI"`, not a team name.

**ML bets have no bug** — manually verified live against the MLB Stats API (`_fetch_final_games` + `_game_side` + `_determine_outcome` all worked correctly for 5 sampled pending ML bets). The reason ML bets are stuck is purely that nothing is currently invoking `run_settlement_check()` at all (Railway not deployed, GitHub Actions never calls it).

---

## File Structure

| File | Change |
|---|---|
| `telegram_handler.py` | Add `_match_game_by_label()` and `_first_inning_runs()` helpers; add `TOTAL` and `NRFI` branches to `_determine_outcome()`; fix the matching step in `run_settlement_check()` to route TOTAL/NRFI through the new label matcher |
| `brain.py` | Add `_run_settle()` one-shot entry point (mirrors `_run_capture_clv()`); wire `--settle` flag into the `__main__` block |
| `.github/workflows/mega_scout.yml` | Add a new `settle_bets` job on a schedule, following the exact pattern of the existing `capture_clv` job |
| `test_settlement_gaps.py` (new) | Unit tests for the two bug fixes and the new entry point |

---

## Task 1: Add `_match_game_by_label()` helper

**Files:**
- Modify: `telegram_handler.py` (insert after `_game_side`, which ends at line 1268)
- Test: `test_settlement_gaps.py` (new file)

**Interfaces:**
- Consumes: nothing new — uses `_game_side(game: dict, team_code: str) -> str | None`, already defined at `telegram_handler.py:1257`
- Produces: `_match_game_by_label(games: list[dict], game_label: str) -> dict | None` — used by Task 3

- [ ] **Step 1: Write the failing test**

Create `test_settlement_gaps.py`:

```python
"""Tests for GitHub Actions bet settlement: TOTAL/NRFI outcome branches,
label-based game matching, and the one-shot --settle entry point.

Background: brain.py logs TOTAL bets with type="TOTAL" (literal) and the
real OVER/UNDER + line in the `bet` text field, and NRFI bets with
type="NRFI" and the YRFI/NRFI direction as the last word of `bet`. Neither
was recognized by _determine_outcome, and neither's `bet` field is a team
name, so the existing team-name game matcher (_game_side on bet["bet"])
never found their game either.

Run: python -m pytest test_settlement_gaps.py -v
"""

from unittest.mock import patch

import pytest


def _final_game(away_name, home_name, away_score, home_score, game_pk=999):
    return {
        "gamePk": game_pk,
        "teams": {
            "away": {"team": {"name": away_name}, "score": away_score, "isWinner": away_score > home_score},
            "home": {"team": {"name": home_name}, "score": home_score, "isWinner": home_score > away_score},
        },
    }


class TestMatchGameByLabel:
    def test_matches_game_from_away_at_home_label(self):
        from telegram_handler import _match_game_by_label
        games = [_final_game("New York Mets", "Philadelphia Phillies", 6, 1)]
        result = _match_game_by_label(games, "New York Mets @ Philadelphia Phillies")
        assert result is games[0]

    def test_no_match_returns_none(self):
        from telegram_handler import _match_game_by_label
        games = [_final_game("New York Mets", "Philadelphia Phillies", 6, 1)]
        assert _match_game_by_label(games, "Boston Red Sox @ Toronto Blue Jays") is None

    def test_malformed_label_returns_none_not_crash(self):
        from telegram_handler import _match_game_by_label
        games = [_final_game("New York Mets", "Philadelphia Phillies", 6, 1)]
        assert _match_game_by_label(games, "not a matchup string") is None
        assert _match_game_by_label(games, "") is None
        assert _match_game_by_label([], "New York Mets @ Philadelphia Phillies") is None

    def test_does_not_match_reversed_home_away(self):
        """A label with home/away swapped from the real game must not match --
        would silently grade the wrong side's total/NRFI otherwise."""
        from telegram_handler import _match_game_by_label
        games = [_final_game("New York Mets", "Philadelphia Phillies", 6, 1)]
        assert _match_game_by_label(games, "Philadelphia Phillies @ New York Mets") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest test_settlement_gaps.py::TestMatchGameByLabel -v`
Expected: FAIL with `ImportError: cannot import name '_match_game_by_label'`

- [ ] **Step 3: Write minimal implementation**

In `telegram_handler.py`, insert immediately after `_game_side` (after line 1268, before `_f5_runs`):

```python
def _match_game_by_label(games: list[dict], game_label: str) -> dict | None:
    """Find a final game matching a 'Away Team @ Home Team' label.
    TOTAL and NRFI bets store their pick's direction/line in `bet`, not a
    plain team name, so _game_side can't be called on bet['bet'] directly
    the way it is for ML/F5/RUNLINE -- match on the `game` column instead."""
    if not game_label or " @ " not in game_label:
        return None
    away_label, home_label = [s.strip() for s in game_label.split(" @ ", 1)]
    for g in games:
        if _game_side(g, away_label) == "away" and _game_side(g, home_label) == "home":
            return g
    return None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest test_settlement_gaps.py::TestMatchGameByLabel -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add telegram_handler.py test_settlement_gaps.py
git commit -m "fix: add label-based game matching for TOTAL/NRFI settlement"
```

---

## Task 2: Add `_first_inning_runs()` helper

**Files:**
- Modify: `telegram_handler.py` (insert after `_f5_runs`, which ends at line 1284)
- Test: `test_settlement_gaps.py`

**Interfaces:**
- Consumes: `_http_get` (already imported at `telegram_handler.py:25` from `api_client`), `STATSAPI` constant (`telegram_handler.py:1231`)
- Produces: `_first_inning_runs(game_pk: int, game: dict) -> tuple[int, int]` — `(away_runs, home_runs)` scored in inning 1. Used by Task 3's NRFI branch.

- [ ] **Step 1: Write the failing test**

Add to `test_settlement_gaps.py`:

```python
class TestFirstInningRuns:
    def test_reads_inline_linescore_first_inning(self):
        from telegram_handler import _first_inning_runs
        game = {
            "linescore": {
                "innings": [
                    {"num": 1, "away": {"runs": 2}, "home": {"runs": 0}},
                    {"num": 2, "away": {"runs": 0}, "home": {"runs": 1}},
                ]
            }
        }
        assert _first_inning_runs(123, game) == (2, 0)

    def test_scoreless_first_inning(self):
        from telegram_handler import _first_inning_runs
        game = {"linescore": {"innings": [{"num": 1, "away": {"runs": 0}, "home": {"runs": 0}}]}}
        assert _first_inning_runs(123, game) == (0, 0)

    def test_falls_back_to_api_call_when_no_inline_linescore(self):
        from telegram_handler import _first_inning_runs
        fake_response = type("R", (), {"json": lambda self: {
            "innings": [{"num": 1, "away": {"runs": 1}, "home": {"runs": 3}}]
        }})()
        with patch("telegram_handler._http_get", return_value=fake_response) as mock_get:
            assert _first_inning_runs(456, {}) == (1, 3)
        mock_get.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest test_settlement_gaps.py::TestFirstInningRuns -v`
Expected: FAIL with `ImportError: cannot import name '_first_inning_runs'`

- [ ] **Step 3: Write minimal implementation**

In `telegram_handler.py`, insert immediately after `_f5_runs` (after line 1284):

```python
def _first_inning_runs(game_pk: int, game: dict) -> tuple[int, int]:
    """Return (away_1st, home_1st) -- runs scored in the top/bottom of
    inning 1. Mirrors _f5_runs's inline-linescore-first, API-fallback shape."""
    innings = (game.get("linescore") or {}).get("innings", [])
    if not innings:
        try:
            r = _http_get(f"{STATSAPI}/game/{game_pk}/linescore", timeout=8)
            innings = r.json().get("innings", [])
        except Exception:
            pass
    first  = next((inn for inn in innings if int(inn.get("num", 0)) == 1), {})
    away_r = int(first.get("away", {}).get("runs") or 0)
    home_r = int(first.get("home", {}).get("runs") or 0)
    return away_r, home_r
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest test_settlement_gaps.py::TestFirstInningRuns -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add telegram_handler.py test_settlement_gaps.py
git commit -m "feat: add first-inning runs helper for NRFI settlement"
```

---

## Task 3: Add TOTAL and NRFI branches to `_determine_outcome`

**Files:**
- Modify: `telegram_handler.py:1287-1353` (`_determine_outcome`)
- Test: `test_settlement_gaps.py`

**Interfaces:**
- Consumes: `_first_inning_runs(game_pk, game) -> tuple[int, int]` (Task 2), `re` (already imported at `telegram_handler.py:20`)
- Produces: `_determine_outcome` now also handles `bet_type == "TOTAL"` and `bet_type == "NRFI"`, returning `"W"`/`"L"`/`"P"`/`None` same as every other branch. Used by Task 4.

- [ ] **Step 1: Write the failing test**

Add to `test_settlement_gaps.py`:

```python
class TestDetermineOutcomeTotal:
    @staticmethod
    def _game(away_score, home_score):
        return {"gamePk": 1, "teams": {"away": {"score": away_score}, "home": {"score": home_score}}}

    def test_over_wins_when_total_exceeds_line(self):
        from telegram_handler import _determine_outcome
        bet = {"type": "TOTAL", "bet": "Team A @ Team B OVER 7.5"}
        assert _determine_outcome(bet, self._game(5, 4), "home") == "W"

    def test_over_loses_when_total_under_line(self):
        from telegram_handler import _determine_outcome
        bet = {"type": "TOTAL", "bet": "Team A @ Team B OVER 8.5"}
        assert _determine_outcome(bet, self._game(3, 2), "home") == "L"

    def test_under_wins_when_total_below_line(self):
        from telegram_handler import _determine_outcome
        bet = {"type": "TOTAL", "bet": "Team A @ Team B UNDER 8.5"}
        assert _determine_outcome(bet, self._game(3, 2), "home") == "W"

    def test_under_loses_when_total_exceeds_line(self):
        from telegram_handler import _determine_outcome
        bet = {"type": "TOTAL", "bet": "Team A @ Team B UNDER 7.5"}
        assert _determine_outcome(bet, self._game(5, 4), "home") == "L"

    def test_push_on_exact_line(self):
        from telegram_handler import _determine_outcome
        bet = {"type": "TOTAL", "bet": "Team A @ Team B OVER 9.0"}
        assert _determine_outcome(bet, self._game(5, 4), "home") == "P"

    def test_unparseable_bet_text_returns_none_not_crash(self):
        from telegram_handler import _determine_outcome
        bet = {"type": "TOTAL", "bet": "Team A @ Team B"}
        assert _determine_outcome(bet, self._game(5, 4), "home") is None


class TestDetermineOutcomeNrfi:
    @staticmethod
    def _game_with_first_inning(away_1st, home_1st):
        return {
            "gamePk": 1,
            "teams": {"away": {"score": away_1st + 3}, "home": {"score": home_1st + 2}},
            "linescore": {"innings": [{"num": 1, "away": {"runs": away_1st}, "home": {"runs": home_1st}}]},
        }

    def test_yrfi_wins_when_run_scores_in_first(self):
        from telegram_handler import _determine_outcome
        bet = {"type": "NRFI", "bet": "Team A @ Team B YRFI"}
        assert _determine_outcome(bet, self._game_with_first_inning(1, 0), "home") == "W"

    def test_yrfi_loses_on_scoreless_first(self):
        from telegram_handler import _determine_outcome
        bet = {"type": "NRFI", "bet": "Team A @ Team B YRFI"}
        assert _determine_outcome(bet, self._game_with_first_inning(0, 0), "home") == "L"

    def test_nrfi_wins_on_scoreless_first(self):
        from telegram_handler import _determine_outcome
        bet = {"type": "NRFI", "bet": "Team A @ Team B NRFI"}
        assert _determine_outcome(bet, self._game_with_first_inning(0, 0), "home") == "W"

    def test_nrfi_loses_when_run_scores_in_first(self):
        from telegram_handler import _determine_outcome
        bet = {"type": "NRFI", "bet": "Team A @ Team B NRFI"}
        assert _determine_outcome(bet, self._game_with_first_inning(0, 1), "home") == "L"

    def test_unparseable_direction_returns_none_not_crash(self):
        from telegram_handler import _determine_outcome
        bet = {"type": "NRFI", "bet": "Team A @ Team B"}
        assert _determine_outcome(bet, self._game_with_first_inning(0, 0), "home") is None


class TestExistingBranchesUnaffectedByTotalNrfi:
    """Regression guard: adding TOTAL/NRFI branches must not shift ML/F5/RUNLINE/
    manually-logged O-U (type='O8.5' etc, from the Telegram /bet command)."""

    def test_ml_still_works(self):
        from telegram_handler import _determine_outcome
        game = {"gamePk": 1, "teams": {"away": {"score": 5}, "home": {"score": 2}}}
        assert _determine_outcome({"type": "ML"}, game, "away") == "W"

    def test_manual_over_under_type_still_works(self):
        from telegram_handler import _determine_outcome
        game = {"gamePk": 1, "teams": {"away": {"score": 5}, "home": {"score": 4}}}
        assert _determine_outcome({"type": "O8.5"}, game, "away") == "W"
        assert _determine_outcome({"type": "U8.5"}, game, "away") == "L"

    def test_runline_still_works(self):
        from telegram_handler import _determine_outcome
        game = {"gamePk": 1, "teams": {"away": {"score": 5}, "home": {"score": 2}}}
        assert _determine_outcome({"type": "RUNLINE-1.5"}, game, "away") == "W"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest test_settlement_gaps.py::TestDetermineOutcomeTotal test_settlement_gaps.py::TestDetermineOutcomeNrfi -v`
Expected: FAIL — TOTAL/NRFI cases return `None` instead of `"W"`/`"L"`/`"P"` (the `TestExistingBranchesUnaffected...` class should already PASS since it exercises unchanged code — confirms no regression baseline before the edit)

- [ ] **Step 3: Write minimal implementation**

In `telegram_handler.py`, insert two new branches into `_determine_outcome` right after the `RUNLINE` branch ends (after line 1333, before the `# Over / Under` comment at line 1335):

```python
    # Scout-generated TOTAL bet -- type is always the literal string "TOTAL"
    # (see brain.py:2303/2318); the real OVER/UNDER direction and line live
    # in the bet text itself, e.g. "Away Team @ Home Team UNDER 8.5". This is
    # a different convention from the manually-logged O8.5/U7.5 type below,
    # which must keep working unchanged.
    if bet_type == "TOTAL":
        m = re.search(r'\b(OVER|UNDER)\s+([\d.]+)\s*$', bet.get("bet") or "", re.IGNORECASE)
        if not m:
            return None
        direction = m.group(1).upper()
        line      = float(m.group(2))
        away_r = int(teams.get("away", {}).get("score") or 0)
        home_r = int(teams.get("home", {}).get("score") or 0)
        total  = away_r + home_r
        if direction == "OVER":
            if total > line:  return "W"
            if total < line:  return "L"
            return "P"
        else:
            if total < line:  return "W"
            if total > line:  return "L"
            return "P"

    # NRFI/YRFI -- type is always the literal string "NRFI" (see
    # brain.py:2187/2200); the actual pick ("NRFI" = no run, or "YRFI" = a
    # run scores) is the last word of the bet text, e.g. "Away @ Home YRFI".
    if bet_type == "NRFI":
        words = (bet.get("bet") or "").split()
        direction = words[-1].upper() if words else ""
        if direction not in ("NRFI", "YRFI"):
            return None
        if game_pk is None:
            return None
        away_1st, home_1st = _first_inning_runs(game_pk, game)
        run_scored = (away_1st + home_1st) > 0
        if direction == "YRFI":
            return "W" if run_scored else "L"
        return "W" if not run_scored else "L"

```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest test_settlement_gaps.py -v`
Expected: PASS (all tests in `TestDetermineOutcomeTotal`, `TestDetermineOutcomeNrfi`, `TestExistingBranchesUnaffectedByTotalNrfi`, plus Tasks 1-2's tests)

- [ ] **Step 5: Commit**

```bash
git add telegram_handler.py test_settlement_gaps.py
git commit -m "fix: recognize TOTAL and NRFI bet types in settlement outcome logic"
```

---

## Task 4: Route TOTAL/NRFI bets through label matching in `run_settlement_check`

**Files:**
- Modify: `telegram_handler.py:1426-1440` (inside `run_settlement_check`)
- Test: `test_settlement_gaps.py`

**Interfaces:**
- Consumes: `_match_game_by_label` (Task 1), `_game_side` (existing)
- Produces: `run_settlement_check()` now successfully matches and settles TOTAL/NRFI bets end-to-end, not just via the two lower-level helpers/branches in isolation. Used by Task 5's `--settle` flag.

- [ ] **Step 1: Write the failing test**

Add to `test_settlement_gaps.py`:

```python
class TestRunSettlementCheckHandlesTotalAndNrfi:
    """End-to-end: run_settlement_check must find + settle TOTAL/NRFI bets,
    not just get the right answer when handed a pre-matched game (Task 3
    tests) -- the matching step itself was broken independently."""

    def _pending_total_bet(self):
        return {
            "id": 501, "date": "2026-07-19", "bet": "New York Mets @ Philadelphia Phillies UNDER 8.5",
            "type": "TOTAL", "game": "New York Mets @ Philadelphia Phillies", "bet_odds": "-102",
            "stake": 20.0, "result": None, "over_cap": 0,
        }

    def _pending_nrfi_bet(self):
        return {
            "id": 502, "date": "2026-07-19", "bet": "New York Mets @ Philadelphia Phillies NRFI",
            "type": "NRFI", "game": "New York Mets @ Philadelphia Phillies", "bet_odds": "-110",
            "stake": 20.0, "result": None, "over_cap": 0,
        }

    def _final_game(self):
        return {
            "gamePk": 777,
            "teams": {
                "away": {"team": {"name": "New York Mets"}, "score": 6, "isWinner": True},
                "home": {"team": {"name": "Philadelphia Phillies"}, "score": 1, "isWinner": False},
            },
            "linescore": {"innings": [{"num": 1, "away": {"runs": 0}, "home": {"runs": 0}}]},
        }

    def test_total_bet_gets_matched_and_settled(self):
        import telegram_handler as th
        with patch.object(th._db, "get_bets", return_value=[self._pending_total_bet()]), \
             patch("telegram_handler._fetch_final_games", return_value=[self._final_game()]), \
             patch("telegram_handler._fetch_closing_odds", return_value=None), \
             patch("telegram_handler._send"), \
             patch("telegram_handler.sync_scout_json"), \
             patch("telegram_handler._update_clv_log"), \
             patch.object(th._db, "resolve_bet_by_id") as mock_resolve:
            settled = th.run_settlement_check()

        assert len(settled) == 1
        assert settled[0]["outcome"] == "W"  # total = 7, line 8.5, UNDER wins
        mock_resolve.assert_called_once()
        assert mock_resolve.call_args.kwargs["bet_id"] == 501
        assert mock_resolve.call_args.kwargs["result"] == "W"

    def test_nrfi_bet_gets_matched_and_settled(self):
        import telegram_handler as th
        with patch.object(th._db, "get_bets", return_value=[self._pending_nrfi_bet()]), \
             patch("telegram_handler._fetch_final_games", return_value=[self._final_game()]), \
             patch("telegram_handler._fetch_closing_odds", return_value=None), \
             patch("telegram_handler._send"), \
             patch("telegram_handler.sync_scout_json"), \
             patch("telegram_handler._update_clv_log"), \
             patch.object(th._db, "resolve_bet_by_id") as mock_resolve:
            settled = th.run_settlement_check()

        assert len(settled) == 1
        assert settled[0]["outcome"] == "W"  # scoreless 1st inning, NRFI wins
        assert mock_resolve.call_args.kwargs["result"] == "W"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest test_settlement_gaps.py::TestRunSettlementCheckHandlesTotalAndNrfi -v`
Expected: FAIL — `settled` is an empty list (`matched_game` stays `None` because the loop still calls `_game_side(g, bet["bet"])` with the composite string)

- [ ] **Step 3: Write minimal implementation**

In `telegram_handler.py`, replace the matching block inside `run_settlement_check` (currently lines 1426-1440):

```python
        for bet in bets:
            team_code = bet["bet"]

            # Find matching final game
            matched_game = None
            matched_side = None
            for g in games:
                s = _game_side(g, team_code)
                if s:
                    matched_game = g
                    matched_side = s
                    break

            if matched_game is None:
                continue  # game not over yet
```

with:

```python
        for bet in bets:
            team_code = bet["bet"]
            bet_type  = (bet.get("type") or "ML").strip().upper()

            # Find matching final game. TOTAL/NRFI bets store the pick's
            # direction/line in `bet`, not a team name (see brain.py's
            # f-strings for those two types), so match on the `game`
            # matchup label instead of team-code lookup.
            if bet_type in ("TOTAL", "NRFI"):
                matched_game = _match_game_by_label(games, bet.get("game") or "")
                matched_side = "home"  # unused by the TOTAL/NRFI outcome branches
            else:
                matched_game = None
                matched_side = None
                for g in games:
                    s = _game_side(g, team_code)
                    if s:
                        matched_game = g
                        matched_side = s
                        break

            if matched_game is None:
                continue  # game not over yet (or unmatched)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest test_settlement_gaps.py -v`
Expected: PASS (all tests so far)

- [ ] **Step 5: Commit**

```bash
git add telegram_handler.py test_settlement_gaps.py
git commit -m "fix: match TOTAL/NRFI bets to their game by matchup label, not team code"
```

---

## Task 5: One-shot `--settle` entry point in brain.py

**Files:**
- Modify: `brain.py` (new function after `_run_capture_clv`, which ends at line 5390; new `elif` branch in the `__main__` block after line 5739)
- Test: `test_settlement_gaps.py`

**Interfaces:**
- Consumes: `telegram_handler.run_settlement_check() -> list[dict]` (existing, now fixed by Tasks 1-4)
- Produces: `brain._run_settle()` — no return value, never raises (matches every other `_run_*` one-shot function's error-swallowing style, e.g. `_run_capture_clv`)

- [ ] **Step 1: Write the failing test**

Add to `test_settlement_gaps.py`:

```python
class TestRunSettleOneShot:
    def test_calls_run_settlement_check_once(self):
        import brain
        with patch("telegram_handler.run_settlement_check", return_value=[{"id": 1}]) as mock_check:
            brain._run_settle()
        mock_check.assert_called_once()

    def test_swallows_exceptions_without_raising(self):
        import brain
        with patch("telegram_handler.run_settlement_check", side_effect=RuntimeError("boom")):
            brain._run_settle()  # must not raise
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest test_settlement_gaps.py::TestRunSettleOneShot -v`
Expected: FAIL with `AttributeError: module 'brain' has no attribute '_run_settle'`

- [ ] **Step 3: Write minimal implementation**

In `brain.py`, insert immediately after `_run_capture_clv` (after line 5390):

```python
def _run_settle():
    """One-shot pending-bet settlement for GitHub Actions (no persistent
    --bot process running this on a 30-min loop the way Railway would).
    Runs the same MLB Stats API settlement check start_auto_settler()'s
    background thread runs, once, then exits."""
    from telegram_handler import run_settlement_check
    print("Running settlement check (one-shot)...")
    try:
        settled = run_settlement_check()
        print(f"[SETTLE] settled {len(settled)} bet(s)")
    except Exception as e:
        error_logger.log_error("brain._run_settle", e)
        print(f"[SETTLE] settlement check failed: {e}")
```

Then in the `__main__` block, add a new `elif` after the `--planner` branch (after line 5739, before the final `else:` at line 5741):

```python
    elif "--settle" in args:
        _run_settle()

```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest test_settlement_gaps.py -v`
Expected: PASS (all tests)

- [ ] **Step 5: Verify the CLI flag actually works end to end (no network mocking — real dry run)**

Run: `python brain.py --settle`
Expected: prints `Running settlement check (one-shot)...` followed by either `[SETTLE] settled N bet(s)` or `[SETTLE] settlement check failed: ...` — must not crash with a traceback. Since this hits the real MLB Stats API and the real local `parlay_os.db`, it may actually settle real pending bets (this is intended — it's the same check `--bot` would have run). Confirm afterward with `sqlite3 parlay_os.db "SELECT result, COUNT(*) FROM bets WHERE date >= '2026-07-19' GROUP BY result;"` that some rows now show `W`/`L`/`P` instead of all being `NULL`.

- [ ] **Step 6: Commit**

```bash
git add brain.py test_settlement_gaps.py
git commit -m "feat: add one-shot --settle flag for GitHub Actions settlement job"
```

---

## Task 6: Schedule the settlement job in GitHub Actions

**Files:**
- Modify: `.github/workflows/mega_scout.yml`

**Interfaces:**
- Consumes: `python brain.py --settle` (Task 5)
- Produces: a new `settle_bets` job, runnable both on a schedule and via `workflow_dispatch`

- [ ] **Step 1: Add the new cron schedules to the top-level `on.schedule` list**

In `.github/workflows/mega_scout.yml`, add these lines to the `schedule:` list (after the existing `capture_clv` entries, before the closing of the list):

```yaml
    - cron: '0 1 * * *'      # settle_bets — 9pm ET (01:00 UTC): early finishers
    - cron: '0 3 * * *'      # settle_bets — 11pm ET (03:00 UTC): most games done
    - cron: '0 5 * * *'      # settle_bets — 1am ET (05:00 UTC): extra innings / west coast
    - cron: '0 14 * * *'     # settle_bets — 9am ET (14:00 UTC): morning catch-all for stragglers/doubleheaders
```

- [ ] **Step 2: Add `settle_bets` to the `workflow_dispatch.inputs.job.options` list**

In the same file, add `- settle_bets` to the existing `options:` list under `workflow_dispatch`.

- [ ] **Step 3: Add the `settle_bets` job**

Add this job after the existing `capture_clv` job (at the end of the `jobs:` section):

```yaml
  # ─── 8. BET SETTLEMENT — 9pm/11pm/1am/9am ET ─────────────────────────────
  # One-shot (brain.py --settle exits after a single pass -- Actions jobs
  # can't run a persistent 30-min loop the way --bot does on Railway).
  # Scheduled around when games typically finish, plus a 9am ET catch-all
  # for extra innings / postponements / doubleheaders still pending from
  # the night before. Safe to run multiple times or re-run on failure --
  # run_settlement_check() only touches rows still result IS NULL.
  settle_bets:
    if: >
      (github.event_name == 'workflow_dispatch' && github.event.inputs.job == 'settle_bets') ||
      (github.event_name == 'schedule' && (
        github.event.schedule == '0 1 * * *' ||
        github.event.schedule == '0 3 * * *' ||
        github.event.schedule == '0 5 * * *' ||
        github.event.schedule == '0 14 * * *'
      ))
    runs-on: ubuntu-latest
    timeout-minutes: 10
    concurrency:
      group: settle_bets
      cancel-in-progress: false
    steps:
      - uses: actions/checkout@v4
        with:
          token: ${{ secrets.GITHUB_TOKEN }}

      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }

      - run: pip install -q requests pytz flask flask-cors

      - name: One-shot pending bet settlement
        env:
          ODDS_API_KEY:         ${{ secrets.ODDS_API_KEY }}
          TELEGRAM_BOT_TOKEN:   ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHAT_ID:     ${{ secrets.TELEGRAM_CHAT_ID }}
          BANKROLL_OVERRIDE:    ${{ secrets.BANKROLL_OVERRIDE }}
        run: python brain.py --settle

      - name: Commit settlement outputs
        run: |
          git config user.name "Parlay OS Bot"
          git config user.email "bot@parlay-os.com"
          git add parlay_os.db last_scout.json clv_log.json || true
          git diff --staged --quiet || git commit -m "Settle: $(date -u '+%Y-%m-%d %H:%M UTC')"
          git push || true
```

- [ ] **Step 4: Validate the YAML parses**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/mega_scout.yml'))" `
Expected: no output, exit code 0 (confirms valid YAML — does not validate GitHub Actions semantics, just syntax)

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/mega_scout.yml
git commit -m "feat: schedule bet settlement as a GitHub Actions job"
```

---

## Task 7: Full regression pass

**Files:** none (verification only)

- [ ] **Step 1: Run the full new test file**

Run: `python -m pytest test_settlement_gaps.py -v`
Expected: all tests PASS

- [ ] **Step 2: Run the pre-existing settlement-adjacent test suites to confirm no regression**

Run: `python -m pytest test_runline.py test_wire_ins.py -v`
Expected: all tests PASS (these cover the RUNLINE `_determine_outcome` branch and the `_fetch_final_games`-based scheduler wiring — both must be unaffected by this plan's changes)

- [ ] **Step 3: Run the full existing test suite**

Run: `python -m pytest -q`
Expected: no new failures relative to the pre-change baseline (if any pre-existing failures are unrelated to settlement, note them but do not attempt to fix them as part of this plan)

---

## Explicitly out of scope (do not implement as part of this plan)

- **PROP settlement automation.** 181 of the pending bets are PROP type with no auto-settle path at all — `_determine_outcome` has no PROP branch and `bet["bet"]` holds a player+stat string, not a team name, so no label-based fix like Task 4 applies. Real automation needs a per-player boxscore pull (ER allowed, strikeouts) from the MLB Stats API boxscore endpoint, which doesn't exist anywhere in this codebase yet (`db.settle_prop()` exists but is uncalled and writes to the wrong table, `prop_results`, not `bets`). Treat as a separate plan.
- **PARLAY settlement.** Multi-leg parlays across different players/games can't be matched to a single game at all; stays manual-only via the existing Telegram `/settle` command.
- **Backfilling the historical stuck backlog beyond what `--settle` naturally catches.** `run_settlement_check()` already checks every pending bet regardless of date and will retroactively settle anything whose game is already final the first time it runs (this is why Task 5, Step 5 is a real dry run, not a mock) — no separate backfill script is needed for ML/TOTAL/NRFI. If the first live run doesn't clear the backlog, that's a signal to investigate further, not a sign a backfill script is needed.
- **Redeploying Railway.** Explicitly decided against for now — GitHub Actions is the settlement path going forward.
