# Bullpen Engine Followups (not built this session)

**Context:** parlay-os Step 3 diagnostic (2026-07-28) audited `bullpen_engine.py`
for depth. Two "computed but discarded" signals (`key_reliever_available`,
`key_relievers_flagged`) were wired into `_weighted_win_prob()` the same
session (see `brain.py` commit `d041c4a` and the earlier `f8879cf`/superseded
fold-in). The items below are separate, additive model improvements the
audit also surfaced — explicitly deferred to a later focused session, not
started here.

## 1. Only the top-2 relievers get an availability check

`analyze_bullpen()` only flags the CL + single highest-3-day-usage RP
(`key_relievers = (cl_arms + rp_arms)[:2]`). A gassed 3rd/4th-best arm — a
real "thin pen" spot MLB bettors would notice — is currently invisible.

**Rough scope:** extend `key_relievers` selection to top-3 or top-4 by
recent usage, decide whether the additive-per-arm penalty already added
this session (`KEY_RELIEVER_EXTRA_FLAGGED_ADJ`) needs a different rate for
non-closer/non-top-RP arms (likely yes — a 4th arm being gassed is a
smaller deal than the closer).

## 2. Fatigue window is 3 days only, no rolling weekly workload

`_pitcher_game_log(pitcher_id, days=3)` and `_fatigue_score()` only look at
the last 3 days. A reliever heavy on days 4–5 but light the last 3 reads
as fresh; there's no view of cumulative weekly workload.

**Rough scope:** add a second, longer-window (7-day) usage aggregate
alongside the existing 3-day fatigue score, likely as a new field on the
`analyze_bullpen()` return dict (`workload_7d` or similar) rather than
changing `_fatigue_score()`'s existing formula/weights.

## 3. No "3rd consecutive appearance" flag independent of pitch count

Teams avoid pitching a reliever on 3+ consecutive days regardless of how
few pitches each outing took — the current pitch-weighted formula can
score this as merely "moderate" fatigue.

**Rough scope:** derive a `consecutive_days_pitched` count per reliever
from the existing gameLog data (already fetched, just needs a streak
scan), surface it as its own flag distinct from `avg_fatigue`.

## 4. No bullpen handedness/matchup quality

The SP side has platoon/arm-angle handling (`platoon_arm_angle` factor,
`arm_angle_adj`); the bullpen side has none — no LOOGY-vs-lineup read, no
stuff-vs-opponent quality for individual relievers beyond the aggregate
Savant `bp_stuff_adj`.

**Rough scope:** likely needs a new per-reliever handedness split lookup
(similar to `offense_engine.py`'s `_platoon_adjustment_real`), scoped to
whichever reliever is actually likely to face the platoon-vulnerable part
of the opposing lineup late in the game — meaningfully harder than the
other three items here since it requires some model of *which* reliever
enters *which* inning.

## 5. No closer role stability check

Closer detection is a pure heuristic (`position == "CL"` from the roster
API, or `saves >= 5`). A team that recently demoted/promoted its closer
mid-season isn't distinguished from a stable, established closer — both
just show up as "the CL."

**Rough scope:** would need either a transaction-log signal
(`transaction_monitor.py` already exists for IL moves — could plausibly
be extended) or a rolling save-opportunity-conversion trend per pitcher.
Lowest priority of the five; role instability is real but rare enough
that it may not be worth the added complexity without graded data first
showing it matters.

---

**Sequencing note:** per the same guidance that applied to this session's
fix (`brain.py`'s `KEY_RELIEVER_UNAVAILABLE_ADJ` / `KEY_RELIEVER_EXTRA_FLAGGED_ADJ`
comment), any new weight introduced by these should be a conservative
placeholder, documented as such, and only tuned once graded CLV data
supports a different value — not tuned from hunches.
