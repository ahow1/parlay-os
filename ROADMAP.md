# PARLAY OS — Development Roadmap

Research compiled: 2026-05-25  
System: MLB betting model — Python + SQLite + Telegram + Railway + GitHub Actions + Flask dashboard  
Model: 12-factor weighted probability blend, Kelly sizing, pool budget system

---

## Priority Stack (Top 5 Moves the Needle Most)

| Rank | Feature | Est. Win-Rate Impact | Build Difficulty |
|------|---------|---------------------|-----------------|
| 1 | Late SP / lineup change detector | +3–5% avoided bad bets | Medium |
| 2 | Claude pick reviewer (pre-send contradiction check) | +2–4% edge | Low |
| 3 | Real-time injury/news agent | +2–3% avoided bad bets | Medium |
| 4 | Claude-generated Telegram narratives | Usability + trust | Low |
| 5 | Closing line monitoring agent | Better CLV, +1–2% | Medium |

---

## Section 1: Claude AI Integration Points

### 1.1 Pre-Send Pick Reviewer (HIGHEST ROI — build first)

**What it does:** Before `_daily_bet_slip()` sends the Telegram message, pass the full analysis dict for each pick to Claude with a structured prompt. Claude reviews for logical contradictions, overrides, or red flags that the model missed.

Example catches:
- "You're picking MIN ML at +140 but their SP has a 7.80 ERA in last 3 starts and the model used season xFIP"
- "You're picking the OVER but both SPs rank top-5 in xwOBA against — these signals conflict"
- "This is a revenge spot for the visiting team but you're fading them — are you sure?"
- "Bullpen flagged as TIRED but you're picking the favorite in a projected close game"

**Implementation:**
```python
# In brain.py before _send_telegram()
import anthropic
client = anthropic.Anthropic()
review = client.messages.create(
    model="claude-opus-4-7",
    max_tokens=500,
    messages=[{"role": "user", "content": f"Review this MLB pick for contradictions: {pick_summary}"}]
)
if review_flags_contradiction(review.content):
    log_warning_and_downgrade_conviction()
```

**Build difficulty:** Low — single API call per pick, ~$0.01/pick  
**Expected ROI:** High — catches model blind spots before money is at risk

---

### 1.2 Claude-Generated Telegram Narratives

**What it does:** Currently the Telegram slip is machine-formatted stats. Claude rewrites each pick into a short, readable paragraph that reads like a real handicapper's analysis.

Current output:
```
SF ML +162 | Edge: +6.2% | SP: Webb xFIP 2.91 | BP: FRESH
```

Claude output:
```
SF (+162) — Logan Webb starts at Oracle Park where he's been untouchable, 
posting a 2.91 xFIP with elite xwOBA-against (.261). The opposing bullpen 
threw 180+ pitches in the last 3 days. Webb's command + fresh Giants pen 
vs a tired Dodgers 'pen = legitimate value at this number.
```

**Implementation:** Call Claude after analysis, pass the full analysis dict as context, ask for a 2-3 sentence explanation. Cache the response — no need to regenerate.

**Build difficulty:** Low  
**Expected ROI:** Medium — better explanations = better personal review of picks, catches logical errors before they leave your brain

---

### 1.3 Dashboard Chat Interface ("Why did you pick MIN today?")

**What it does:** A chat input on the Flask dashboard. User types a natural language question, Claude queries the database via tool use and answers.

Example queries:
- "Why did you pick MIN today?"
- "What's my win rate on HIGH conviction road dogs?"
- "Show me all times we bet on a pitcher with ERA 2+ above xFIP"
- "Which situations have the worst track record?"
- "How much did we win on props vs ML this month?"

**Implementation:**
- Build a `/api/chat` endpoint in `api.py`
- Claude is given tools: `query_bets(sql)`, `get_scout_output(date)`, `get_game_analysis(game_id)`
- System prompt explains the full schema, the 12-factor model, and the Kelly sizing system
- Claude generates SQL, executes it safely (read-only connection), formats the answer

**Build difficulty:** Medium (2-3 days)  
**Expected ROI:** High usability — eliminates the need to manually inspect the DB for performance questions

---

### 1.4 Performance Pattern Analyst

**What it does:** Weekly Claude-written report that goes deeper than `weekly_pattern_report()`. Claude is given the full settled bet history and prompted to:
- Identify non-obvious winning/losing patterns
- Compare this week's picks vs historical performance on similar games
- Flag if the model is degrading in a specific area (e.g., "your bullpen factor has been wrong 7 of last 10 games")
- Suggest one specific model adjustment to test

**Schedule:** Sunday 6am ET, output to Telegram

**Build difficulty:** Low  
**Expected ROI:** Medium-High — identifies drift before it becomes expensive

---

### 1.5 Web Search for Breaking News (Pre-Scout)

**What it does:** Before `run_daily_scout()` starts analyzing games, run a Claude web search for each starting pitcher to check for:
- "Is [pitcher name] healthy today?"
- "[Team name] lineup news today [date]"
- Any IL transactions in the last 6 hours

If a flagged SP or key player shows up in injury news, reduce SP confidence by 20% or flag the game as `SKIP_INJURED_SP`.

**Implementation:**
```python
# In brain.py, before analyze_game() loop
from anthropic import Anthropic
client = Anthropic()
for event in events:
    news_check = client.messages.create(
        model="claude-opus-4-7",
        max_tokens=200,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{"role": "user", "content": f"Any injury or lineup news for {sp_name} starting today {game_date}?"}]
    )
    if injury_flagged(news_check):
        sp_data["injury_flag"] = True
```

**Build difficulty:** Medium — need to parse Claude's web search results  
**Expected ROI:** High — SP scratches are the single biggest model-breaking event; catching them pre-bet saves entire positions

---

## Section 2: Autonomous Agents

### 2.1 Late SP Change Detector (HIGHEST ROI AGENT)

**What it does:** Runs every 15 minutes from 10am–7pm ET on game days. Compares the SP we locked into the model against the current probable pitcher from MLB Stats API. If the SP changes:
1. Sends Telegram alert: "⚠️ SP CHANGE: NYY — Gerrit Cole → TBD. Bet logged at -145. Recommend cancel."
2. Logs the change in `game_updates_log`
3. Optionally auto-cancels the pending bet if confidence drops below threshold

**Why this matters:** SP changes happen 3-5 times per week across the slate. They invalidate roughly 30-40% of the model's ML recommendation. Catching this before the game starts is the highest-leverage defensive action available.

**Implementation:**
```python
# New: sp_monitor.py — runs in a thread from brain.py --bot mode
def monitor_sp_changes(known_sps: dict, game_date: str):
    while True:
        for game_pk, expected_sps in known_sps.items():
            current = get_game_sps(game_pk, ...)
            if current["away"]["name"] != expected_sps["away"]:
                send_alert(f"SP CHANGE: {game_pk}")
        time.sleep(900)  # every 15 min
```

**Build difficulty:** Medium  
**Expected ROI:** Very High — prevents 2-4 bad bets per week

---

### 2.2 Closing Line Monitor

**What it does:** Stores the odds when we first log a bet (opening line from our perspective). Every 30 minutes, polls current market odds. Sends alert when:
- Line moves 10+ points against our position (value eroding)
- Line moves 10+ points in our favor (good beat — we got better than closing)

Also computes real-time CLV before the game starts, not just after.

**Current gap:** CLV is only computed post-game. We should know during the day if we got a good number.

**Implementation:** Extend `line_movement_engine.py` — already has opening line cache. Add polling thread + Telegram push when threshold crossed.

**Build difficulty:** Low (infrastructure already exists)  
**Expected ROI:** Medium — better bet timing, know when to be satisfied vs look for better number

---

### 2.3 Live In-Game Hedge Agent

**What it does:** From 6pm–midnight ET, monitors active bets against live MLB scores via Stats API. When a team we bet ML on goes up 3+ runs after 4 innings:
- Computes hedge stake using `hedge_calc()` in `math_engine.py`
- Sends Telegram with "HEDGE OPPORTUNITY: SF up 4-0 in 5th. Hedge $X on LAD live ML to lock $Y profit"

When a team we bet on falls behind early:
- Computes EV of riding vs loss
- Flags if the situation looks like a blowout (pull SP early = our bet dead)

**Current state:** `check_hedge_opportunities()` in `telegram_handler.py` partially does this. Needs deeper integration with game state.

**Build difficulty:** Medium  
**Expected ROI:** Medium — converts some winning positions into locked profits, reduces variance

---

### 2.4 MLB News/Injury Agent

**What it does:** Runs at 9am, 11am, and 1pm ET (before scout). Scrapes:
- MLB official transaction wire (IL placements, activations) via Stats API `/transactions` endpoint
- Optional: Beat reporter keywords via Brave Search MCP

Cross-references against today's games. If a key player (SP, cleanup hitter, closer) appears in IL transactions:
- Reduces model confidence for that game
- Sends Telegram: "⚠️ NEWS: Shohei Ohtani removed from LAA lineup — LAA ML bet at risk"

**Implementation:**
```python
# In scheduler.py — add to daily tasks at 9am, 11am, 1pm
def run_transaction_check():
    from datetime import date
    url = f"{STATSAPI}/transactions?sportId=1&date={date.today()}"
    transactions = fetch(url)
    for t in transactions:
        if t["typeCode"] in ("IL10", "IL15", "IL60", "DFA"):
            check_if_affects_today_bets(t["player"])
```

**Build difficulty:** Medium  
**Expected ROI:** High — SP and cleanup hitter IL moves kill bet value

---

### 2.5 Kalshi Arbitrage Agent

**What it does:** Kalshi offers MLB game winner markets with different pricing than sportsbooks. When our model shows edge AND Kalshi has the same team at significantly different implied probability, flag the arbitrage or the better-priced side.

Example: Model says SF 58% to win. DraftKings has SF at -135 (57.4% implied). Kalshi has SF at 61¢ (61% implied). Bet SF at DraftKings where the line is better relative to our edge.

**Build difficulty:** Medium — Kalshi has an API  
**Expected ROI:** Medium — rare but pure edge when it exists

---

### 2.6 Weather Update Agent (30-Min Pre-Game)

**What it does:** Currently weather is fetched once during the scout run (~1pm). Wind conditions at game time (7pm) can be significantly different. A 15mph wind-out at Wrigley changes the total by 1+ run.

Runs 45 minutes before each game's first pitch, re-fetches weather, and:
- If run adjustment changes by >0.5 runs: updates total recommendation
- Sends Telegram: "🌬️ WEATHER UPDATE: Wrigley wind shifted to 14mph out — OVER now stronger (+0.8R)"

**Build difficulty:** Low (wttr.in already integrated)  
**Expected ROI:** Medium — especially valuable for totals and NRFI bets

---

## Section 3: MCP Servers

### 3.1 MLB Stats API MCP (Build This First)

**What it does:** Wraps all MLB Stats API endpoints as MCP tools. Enables Claude to directly query schedules, player stats, transactions, game logs, and boxscores without custom code.

Tools:
- `get_schedule(date, team_id?)` → today's games
- `get_sp(game_pk)` → starting pitchers
- `get_transactions(date)` → IL moves
- `get_player_stats(player_id, season, group)` → hitting/pitching stats
- `get_live_game(game_pk)` → in-progress game state

**Why valuable:** Any Claude agent can then use MLB data without needing the Python layer. Enables composable AI workflows.

**Build difficulty:** Low (2-3 days) — MLB Stats API is free and well-documented  
**Expected ROI:** High infrastructure value — enables everything else

---

### 3.2 The Odds API MCP

**What it does:** Wraps Odds API as MCP tools for Claude to query directly.

Tools:
- `get_current_odds(sport, market)` → live lines
- `get_historical_odds(event_id, timestamp)` → opening lines
- `get_events(sport, date)` → today's slate

**Why valuable:** Claude agents can independently verify odds, check for line movement, compare books — without going through our market_engine layer.

**Build difficulty:** Low  
**Expected ROI:** Medium — most useful if building Claude agents that run independently

---

### 3.3 Brave Search MCP (Existing — Just Connect)

**What it does:** Brave Search MCP server already exists (`@modelcontextprotocol/server-brave-search`). Connect it to Claude in `claude_desktop_config.json` or via API tool_choice.

Use cases:
- Pre-scout injury news search
- Beat reporter tweet monitoring
- Park condition updates

**Build difficulty:** Very Low — MCP already exists, just needs API key  
**Expected ROI:** High for injury detection, Medium overall

---

### 3.4 Google Sheets MCP (Existing — Connect)

**What it does:** Manual bet log in Google Sheets that syncs with the SQLite DB. Useful when logging bets from mobile without Telegram bot access.

**Build difficulty:** Low — MCP exists  
**Expected ROI:** Low for accuracy, Medium for convenience

---

### 3.5 Kalshi MCP (Build)

**What it does:** Wraps Kalshi's REST API as MCP tools.

Tools:
- `get_market(ticker)` → current market price
- `get_mlb_markets(date)` → all MLB markets today
- `get_orderbook(ticker)` → bid/ask spread

**Build difficulty:** Medium — Kalshi API requires auth, rate limits  
**Expected ROI:** Medium — niche but pure edge when arbitrage exists

---

## Section 4: AI-Powered Dashboard

### 4.1 Natural Language Query Interface

**What it does:** Chat input on the dashboard. Examples:
- "Show all games where we won with a pitcher whose ERA was 2+ above xFIP"
- "What's the ROI on HIGH conviction picks where we were a road dog?"
- "When we bet the over in COL, what's our record?"
- "Show me the 5 worst picks by model probability vs outcome"

**Implementation:**
- `/api/chat` endpoint
- Claude given read-only `query_bets(sql: str)` tool with schema context
- Claude generates safe SELECT queries, formats results as table or prose
- Dangerous SQL patterns blocked: DROP, DELETE, UPDATE, INSERT

**Build difficulty:** Medium (3-4 days)  
**Expected ROI:** High usability — replaces manual DB queries entirely

---

### 4.2 Pick Explanation Panel

**What it does:** Each pick on the dashboard has an expandable "Why?" panel. Clicking it shows a Claude-generated paragraph synthesizing the key factors that drove the pick.

Pulls from: SP xwOBA tier, xFIP, bullpen fatigue, offensive wRC+, platoon edge, park factor, weather, momentum score, situations triggered, H2H record.

**Implementation:** Pre-generate during scout run, store in `scout_output.picks[].explanation`. Display in dashboard without additional API calls.

**Build difficulty:** Low  
**Expected ROI:** Medium — primarily usability and trust-building

---

### 4.3 Anomaly Detector

**What it does:** Before the daily slip goes out, compares today's picks against historical distributions:
- Edge percentages: are today's picks within normal range?
- Conviction distribution: are we over-indexing HIGH today vs historical?
- Parlay composition: does today's parlay look like past winners or past losers?
- SP quality: are we betting games with worse-than-average SP matchups?

Flags: "⚠️ Today's picks have avg model_prob of 0.61 — 8th percentile vs historical. May be overfit day."

**Build difficulty:** Medium  
**Expected ROI:** Medium — prevents overconfidence on statistical outlier days

---

### 4.4 What-If Simulator

**What it does:** "What would our record be if we only bet HIGH conviction picks?" Runs against full settled bet history in the DB, simulates different filtering rules, returns P&L comparison.

Pre-built scenarios:
- HIGH only vs HIGH+MEDIUM
- Road dogs only
- Games with confirmed lineups only
- Exclude games with SP TBD
- Only bet when CLV > +2%

**Build difficulty:** Low (mostly SQL + math)  
**Expected ROI:** Medium — informs which filters to apply going forward

---

## Section 5: Missing Data Sources

### 5.1 Spin Rate Trend Monitoring (HIGH IMPACT)

**What it does:** A pitcher whose spin rate drops 100+ RPM across consecutive starts is fatigued, injured, or tipping pitches. Current model uses season averages.

Pull last 5 starts of spin rate data per fastball type from Statcast. Flag if trending down >5% in last 3 starts.

**Source:** Baseball Savant statcast_search CSV endpoint (already integrated)  
**Build difficulty:** Low — extend `statcast_engine.py`  
**Expected ROI:** High — spin rate decay precedes ERA blowups by 1-2 starts

---

### 5.2 Umpire Zone Maps (MEDIUM IMPACT)

**What it does:** Currently we use `UMPIRE_TENDENCIES` (k_factor, run_factor) from a static dict. Savant has per-umpire zone maps showing exact called-strike probability by location.

Real edge: some umpires squeeze outside corner (hurts sinkerballer SPs), others have wide zones (rewards command pitchers). Match umpire zone profile against today's SP's primary pitch location.

**Source:** Baseball Savant umpire leaderboard  
**Build difficulty:** Medium — need to parse zone map data  
**Expected ROI:** Medium — refines existing umpire factor, not a new signal

---

### 5.3 Catcher Framing vs Pitch Mix Matchup

**What it does:** Current `framing_engine.py` uses season-level framing runs. Deeper: does this catcher frame well specifically against sliders? Against high fastballs? Match against the opposing SP's primary pitches.

Example: Catcher A frames sliders at +8 runs/1000 pitches but is neutral on fastballs. SP B throws 45% sliders. Real framing edge is much larger than season average implies.

**Source:** Baseball Savant catcher framing by pitch type leaderboard  
**Build difficulty:** High — requires joining two leaderboards at pitch-type level  
**Expected ROI:** Medium — incremental, not a game-changer on its own

---

### 5.4 Travel/Schedule Fatigue (MEDIUM IMPACT)

**What it does:** Beyond West-to-East travel (currently tracked), model:
- Consecutive road games (days 8+ of road trip)
- Time zone crossings in last 48 hours
- Day games after night games (especially brutal for west coast teams)
- Series opener (players travel overnight, arrive late)

**Source:** MLB Stats API schedule endpoint (team location data)  
**Build difficulty:** Medium  
**Expected ROI:** Medium — especially valuable for series openers after long road trips

---

### 5.5 Public Betting Percentage Data

**What it does:** Services like The Action Network, Covers, or DraftKings public data show what percentage of bets and money are on each side. When public money is 70%+ on one side but the line hasn't moved — sharp money is fading the public side.

This is the quantitative version of the RLM signal we already detect.

**Source:** The Action Network has a public API (partially). DraftKings Insights shows public %. Action Network Pro API is paid (~$20/month).

**Build difficulty:** Medium  
**Expected ROI:** High — one of the most reliable indicators of where sharp money is

---

### 5.6 Platoon Splits vs Specific Pitch Types

**What it does:** Not just L vs R platoon — which pitch types does this lineup struggle with? If opposing SP throws 40% sweepers and this lineup has .185 BA against sweepers, that's more precise than generic platoon.

**Source:** Savant batting leaderboard filtered by pitch type  
**Build difficulty:** High — requires joining SP pitch mix data with batting performance by pitch type  
**Expected ROI:** Medium — incremental accuracy gain

---

### 5.7 Minor League Transactions / Call-Up Impact

**What it does:** When a team calls up a prospect mid-series, it usually means someone left the lineup (injury, demotion). Detects:
- Is the called-up player going into a key lineup spot?
- Does this change the LHB% of the lineup (affects platoon model)?

**Source:** MLB Stats API `/transactions?typeCode=SC` (select contract)  
**Build difficulty:** Low — extend existing transaction monitoring  
**Expected ROI:** Low — rare events, small impact per game

---

## Build Order (Prioritized by ROI)

### Phase 1: Defensive Layer (Prevent Bad Bets) — Build in 2 weeks

1. **Late SP Change Detector** — sp_monitor.py, runs every 15min, alerts + optional auto-cancel
2. **Pre-Send Claude Pick Reviewer** — API call before each Telegram slip, flags contradictions
3. **MLB Transaction Monitor** — 9am/11am/1pm IL check against today's games
4. **Weather Update Agent** — 45min pre-game weather re-fetch for totals/NRFI

### Phase 2: Intelligence Layer (Find Better Bets) — Build in month 2

5. **Closing Line Monitor** — extend line_movement_engine.py, real-time CLV alerts
6. **Spin Rate Trend Signal** — extend statcast_engine.py, flag declining velo/spin
7. **Brave Search MCP** — connect existing MCP for injury news pre-scout
8. **Public Betting % Integration** — Action Network API or Covers scrape

### Phase 3: AI Layer (Understand + Explain) — Build in month 2-3

9. **Claude Telegram Narrative Generator** — plain-English pick explanations in slip
10. **Dashboard Chat Interface** — natural language DB queries
11. **Pick Explanation Panel** — pre-generated "why" for each pick in dashboard
12. **Weekly AI Pattern Report** — Claude analyzes full settled history, suggests adjustments

### Phase 4: Expansion (New Edge) — Build in month 3-4

13. **Kalshi Arbitrage Agent** — monitor Kalshi vs sportsbook pricing gaps
14. **MLB Stats API MCP Server** — reusable MCP for all Claude agents
15. **Live Hedge Agent** — in-game position monitoring with hedge calc
16. **What-If Simulator** — dashboard filtering and scenario analysis
17. **Umpire Zone Map Integration** — Savant umpire data vs SP pitch locations
18. **Catcher Framing × Pitch Mix Matchup** — deep framing signal

---

## Top 5 Things That Move the Needle on Profitability

1. **Late SP Change Detector** — SPs change 3-5x/week. Each missed change that turns a good bet into a bad one costs $5-15 in expected value. Over a season this is the single highest-ROI defensive action. Estimate: +3-5% avoided bad bets, ~$50-100/month at current scale.

2. **Claude Pre-Send Pick Reviewer** — Catches model contradictions before money is at risk. At $0.01/pick and 3-5 picks/day, this costs <$0.50/week. One prevented bad bet pays for 10 weeks of API costs. Estimate: +2-4% quality improvement, net positive within first week.

3. **MLB Transaction Monitor (Injury Alerts)** — When a cleanup hitter goes IL day-of, our offense model is using stale data. This catches it. Estimate: +2-3% avoided bad bets, especially on close games.

4. **Spin Rate Trend Signal** — Adding this to the existing SP analysis would catch the "healthy-looking-on-paper but declining" pitcher 1-2 starts earlier than ERA shows. This is probably the single best new accuracy signal that doesn't require a new data source. Estimate: +1-2% model accuracy on SP-heavy picks.

5. **Public Betting % / Sharp Action** — The RLM signal we detect from line movement is a proxy for this. Getting actual bet % data quantifies it. When 75% of public bets are on the favorite but the line holds or moves toward the dog — that's the best sharp-money signal available. Estimate: +2-3% on games where we bet against public, improved CLV.

---

## Technical Notes

### Claude API Integration Pattern

```python
# Standard pattern for all Claude integrations
import anthropic
import os

_claude_client = None

def _get_claude():
    global _claude_client
    if _claude_client is None:
        _claude_client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    return _claude_client

def claude_review_pick(analysis: dict) -> dict:
    """Returns {ok: bool, flags: list[str], confidence_adj: float}"""
    client = _get_claude()
    # ... structured prompt with analysis dict
```

### Environment Variables to Add

```
ANTHROPIC_API_KEY=sk-ant-...
ACTION_NETWORK_API_KEY=...   # for public betting %
KALSHI_API_KEY=...           # for prediction market data
BRAVE_SEARCH_API_KEY=...     # for news search via MCP
```

### Cost Estimates at Current Scale

- Claude pick reviewer: ~$0.01/pick × 4 picks/day × 180 game days = ~$7/season
- Claude narrative generator: ~$0.02/slip/day × 180 days = ~$3.60/season
- Claude dashboard chat: ~$0.05/query, usage-dependent
- Total Claude API cost estimate: <$50/season at current scale

All Claude integrations are net positive within the first week of prevented bad bets.

---

*This roadmap is a living document. Update after each phase as actual ROI data comes in from the bet memory system.*
