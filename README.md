# UglyBot

## Rewards environment variables

- `PASSIVE_REWARD_START_AT`: optional ISO timestamp for the earliest passive $CHARM accrual start. If unset, each collection starts accruing when its first enabled holder rule was created. Alias: `CHARM_PASSIVE_REWARD_START_AT`.

## Marketplace environment variables

- `MARKETPLACE_NOTICE_CHANNEL_ID`: channel for delivered purchase notices. Defaults to `1321864977270706257`.
- `MARKETPLACE_THREAD_PARENT_CHANNEL_ID`: channel where private delivery threads are created. Defaults to the channel where `/marketplace` is used.
- `MARKETPLACE_ADMIN_USER_IDS`: comma-separated admin user IDs to add to private delivery threads.
- `MARKETPLACE_ADMIN_ROLE_IDS`: comma-separated admin role IDs whose members should be added to private delivery threads when member fetching is available.
- `MARKETPLACE_TIME_ZONE`: timezone used for monthly stock resets. Defaults to `America/Toronto`.
