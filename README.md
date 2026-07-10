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
- `MAW_RETURN_REWARD_CHARM`: default `12500`.
- `MAW_JACKPOT_CHARM`: default `35000`.
- `MAW_SESSION_TTL_MINUTES`: default `20`.
- `MAW_PRIZE_CASHOUT_CHARM`: default `8000`.
- `MAW_REROLL_COST_CHARM`: default `4000`.
- `MAW_MAX_REROLLS`: default `3`.
- `MAW_POLL_INTERVAL_SECONDS`: default `30`.
- `MAW_MIN_CONFIRMATIONS`: default `2`.
- `MAW_FEED_CHANNEL_ID`: optional public Maw feed channel.
- `MAW_ADMIN_CHANNEL_ID`: optional admin/manual-review channel.
- `ETH_RPC_URL` or `ALCHEMY_API_KEY`: required for automatic ERC721 Transfer watching.

## Feed the Maw commands

- `/maw open`: admin only. Opens a Maw event.
- `/maw post`: admin only. Posts the public Feed the Maw panel.
- `/maw close`: admin only. Closes the active Maw event.
- `/maw status`: shows progress, tickets, jackpot status, and active session.
- `/maw inventory`: admin only. Shows Maw Pool inventory counts.
- `/maw reconcile`: admin only. Runs the transfer checker immediately.
- `/squigprize user:@user reason:"optional"`: admin only. Offers a random Maw Pool Squig the user did not originally send.
- `/squigprize claim_id:<id> tx:<optional>`: admin only. Marks an accepted Squig prize delivered.

Returned Squigs become Maw Pool inventory and can later be awarded through `/squigprize`. NFT delivery remains manual/admin-confirmed; the bot does not store private keys or auto-transfer NFTs. `/marketplace` remains separate from Feed the Maw and keeps its existing pricing, stock, flow, and UI.
