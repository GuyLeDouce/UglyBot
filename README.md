# UglyBot

## Rewards environment variables

- `PASSIVE_REWARD_START_AT`: optional ISO timestamp for the earliest passive $CHARM accrual start. If unset, each collection starts accruing when its first enabled holder rule was created. Alias: `CHARM_PASSIVE_REWARD_START_AT`.

Reward accrual is tracked per NFT. If an NFT sells or transfers before it is claimed, the accrued $CHARM follows that NFT to the new owner.

## Marketplace environment variables

- `MARKETPLACE_NOTICE_CHANNEL_ID`: channel for delivered purchase notices. Defaults to `1321864977270706257`.
- `MARKETPLACE_THREAD_PARENT_CHANNEL_ID`: channel where private delivery threads are created. Defaults to the channel where `/marketplace` is used.
- `MARKETPLACE_ADMIN_USER_IDS`: comma-separated admin user IDs to add to private delivery threads.
- `MARKETPLACE_ADMIN_ROLE_IDS`: comma-separated admin role IDs whose members should be added to private delivery threads when member fetching is available.
- `MARKETPLACE_TIME_ZONE`: timezone used for monthly stock resets. Defaults to `America/Toronto`.

## Feed the Maw environment variables

- `MAW_WALLET_ADDRESS`: required. Official wallet that receives returned Squigs.
- `MAW_SQUIG_CONTRACT`: Squigs Reloaded contract. Defaults to `0x8c9a02c0585200c4c65608df6b8def543d33792a`.
- `MAW_GOAL_COUNT`: default `20`.
- `MAW_RANKING_CSV_PATH`: optional override for the local Squig ranking CSV. Defaults to `Squigs_Reloaded_Token_UglyPoints.csv` in the repository root.
- `MAW_JACKPOT_BASE_CHARM`: optional starting $CHARM jackpot before rarity contributions. Defaults to `0`.
- `MAW_JACKPOT_CHARM`: deprecated fallback for the starting jackpot when `MAW_JACKPOT_BASE_CHARM` is unset.
- `MAW_RETURN_REWARD_CHARM`: legacy-only flat event payout. New rarity events use the tier reward table instead.
- `MAW_SESSION_TTL_MINUTES`: default `20`.
- `MAW_PRIZE_CASHOUT_CHARM`: default `8000`.
- `MAW_REROLL_COST_CHARM`: default `4000`.
- `MAW_MAX_REROLLS`: default `3`.
- `MAW_POLL_INTERVAL_SECONDS`: default `30`.
- `MAW_MIN_CONFIRMATIONS`: default `2`.
- `MAW_FEED_CHANNEL_ID`: optional public Maw feed channel.
- `MAW_ADMIN_CHANNEL_ID`: optional admin/manual-review channel.
- `MAW_DIGESTION_ADMIN_CHANNEL_ID`: admin digestion queue channel. Defaults to `1477463175665287410`.
- `MAW_DIGESTION_RECEIPT_CHANNEL_ID`: public final digestion receipt channel. Defaults to `1524863373513068707`.
- `MAW_DIGESTED_IMAGE_URL`: optional image URL for final digestion receipts.
- `MAW_DIGESTED_IMAGE_PATH`: optional local image path for final digestion receipts. Used before `MAW_DIGESTED_IMAGE_URL` when present.
- `MAW_ACCEPTED_BURN_ADDRESSES`: internal-only comma-separated burn-address allowlist for validating manual admin burn transactions. The zero address and `0x000000000000000000000000000000000000dEaD` are accepted by default. This is not a user-facing transfer destination.
- `ETH_RPC_URL` or `ALCHEMY_API_KEY`: required for automatic ERC721 Transfer watching.

## Feed the Maw commands

- `/maw open`: admin only. Opens a Maw event.
- `/maw post`: admin only. Posts the public Feed the Maw panel.
- `/maw close`: admin only. Closes the active Maw event.
- `/maw status`: shows progress, tickets, jackpot status, and active session.
- `/maw inventory`: admin only. Shows Maw Pool inventory counts.
- `/maw reconcile`: admin only. Runs the transfer checker immediately.
- `/maw rank token:<token id>`: admin only. Shows the local ranking quote without creating a session.
- `/rank token_id:<token id>`: public. Shows the Squig image, Maw Rank, and class.
- `/allrank`: admin only. Exports all Squig Maw Ranks as `TOKEN ID,RANK,CLASS`.
- `/maw digestion status:<optional> token:<optional>`: admin only. Shows pending or failed Swallowed digestion workflows. Use `status:retry_request token:<id>` to retry a failed admin digestion request.
- `/squigprize user:@user reason:"optional"`: admin only. Offers a random Maw Pool Squig the user did not originally send.
- `/squigprize claim_id:<id> tx:<optional>`: admin only. Marks an accepted Squig prize delivered.

New Maw events use rarity-based rewards from the validated local CSV:

- Legendary: Maw Rank 1, 800,000 $CHARM, 20 tickets, +100,000 $CHARM jackpot.
- Epic: Maw Rank 32–443, 187,500 $CHARM, 10 tickets, +25,000 $CHARM jackpot.
- Rare: Maw Rank 444–1110, 67,500 $CHARM, 5 tickets, +5,000 $CHARM jackpot.
- Uncommon: Maw Rank 1111–2276, 22,500 $CHARM, 2 tickets, +2,000 $CHARM jackpot.
- Common: Maw Rank 2277+, 15,000 $CHARM, 1 ticket, +1,000 $CHARM jackpot.

Maw Rank is calculated from each Squig's individual `Total UglyPoints`. The 31 Squigs with a nonblank `Legend` value all share rank 1. Every other Squig is sorted by `Total UglyPoints` descending, with token ID ascending as the deterministic tie-breaker, and receives a unique rank from 32 through 4444. Each accepted Squig fills one event spot regardless of rarity. Rarity controls the immediate $CHARM payout, the number of physical Maw Ticket rows issued, and how much $CHARM is added to the jackpot.

After reviewing the reward quote, the feeder chooses the Squig’s NFT fate:

- `Swallowed`: the Squig is still sent to `MAW_WALLET_ADDRESS`, enters the digestion queue after receipt, and is manually burned by an admin. The bot records and validates the completed burn transaction, then posts a final digestion receipt.
- `Regurgitated`: the Squig is sent to the same `MAW_WALLET_ADDRESS` and becomes available Maw Pool inventory for future games, giveaways, incentives, prizes, onboarding rewards, and other approved community uses.

Both fates receive the same rarity-based $CHARM payout, Maw Tickets, jackpot contribution, and event progress. Users are never asked to send NFTs to a burn address, and the bot does not store private keys or submit burn transactions.

Swallowed Squigs are retained in Maw inventory as audit records with `pending_burn` or `digested` status and are explicitly excluded from `/squigprize` and Maw Pool selection helpers. Legacy inventory with no disposition is treated as Regurgitated for compatibility.

The live bot runs from the local validated CSV and does not require Google Sheets at runtime. Refresh the CSV with:

```bash
npm run sync:maw-ranks
```

The sync command downloads the configured Google Sheet to a temporary file, validates token IDs 1–4444 and required headers, prints the SHA-256 hash and tier distribution, then replaces the local CSV only after validation succeeds. Restart the bot after replacing the CSV unless a separate safe reload mechanism is added.

Regurgitated Squigs become Maw Pool NFT inventory and can later be awarded through `/squigprize`. Swallowed Squigs remain audit-only digestion records and are never available for prize selection. The Maw Pool is separate from the $CHARM jackpot: the pool contains NFTs, while the jackpot is paid in $CHARM. NFT delivery remains manual/admin-confirmed; the bot does not store private keys or auto-transfer NFTs. `/marketplace` remains separate from Feed the Maw and keeps its existing pricing, stock, flow, and UI.
