# PARLAY OS — Launch Checklist

Track requirements before opening picks service to paying subscribers.

## Launch Requirements

| # | Requirement | Status | Notes |
|---|-------------|--------|-------|
| 1 | **50 verified picks minimum** | ⬜ Pending | Need 50 settled picks in DB with verify_hash. Run `/api/record` to check total_resolved. |
| 2 | **60%+ positive CLV rate** | ⬜ Pending | `clv_positive_rate` in `/api/record` must be ≥ 60% before advertising edge. |
| 3 | **Public track record page live** | ✅ Done | `/record` route serves `record.html` — sortable, Chart.js charts, no bankroll info. |
| 4 | **ToS + responsible gambling disclaimer** | ⬜ Pending | Add ToS page (`/tos`) and link from `record.html` footer. Include NCPG hotline 1-800-522-4700. |
| 5 | **Payment processing** | ⬜ Pending | Stripe Checkout for subscription tiers. Add `STRIPE_SECRET_KEY` + `STRIPE_WEBHOOK_SECRET` to env. |
| 6 | **Refund policy** | ⬜ Pending | 7-day refund window for first-time subscribers. Document in ToS and FAQ. |

## Subscription Tiers (Planned)

| Tier | Price | Includes |
|------|-------|---------|
| Free | $0 | Public record page, last 7 days of picks (results only) |
| Pro | $29/mo | Daily Telegram picks + Discord alerts + full pick history + CLV data |
| Elite | $79/mo | All Pro + early-morning preview (10am ET) + player props + injury flags |

## Distribution Channels

- **Private Telegram** (`TELEGRAM_CHAT_ID`) — full slip with bankroll context
- **Public Telegram Channel** (`TELEGRAM_PUBLIC_CHANNEL_ID`) — clean picks, no bankroll
- **Discord** (`DISCORD_WEBHOOK_URL`) — color-coded embeds via `discord_bot.py`
- **Web** — `/record` public page with Chart.js charts and verified picks table

## Environment Variables Required for Launch

```
TELEGRAM_BOT_TOKEN=           # existing
TELEGRAM_CHAT_ID=             # existing (private)
TELEGRAM_PUBLIC_CHANNEL_ID=   # new — public channel
DISCORD_WEBHOOK_URL=          # new — Discord integration
STRIPE_SECRET_KEY=            # new — payment processing
STRIPE_WEBHOOK_SECRET=        # new — Stripe webhook validation
```

## Verification System

Every pick logged via `db.log_bet()` receives a SHA256 hash:
`SHA256(game | bet | odds | timestamp)` stored in `bets.verify_hash`

Public verification: `GET /api/verify/<hash>` returns pick details without bankroll info.
Display in record table: first 8 chars of hash with link to full verification endpoint.
