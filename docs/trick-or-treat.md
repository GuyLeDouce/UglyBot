# Squigs Trick or Treat — October 2026

## Public rules

Trick or Treat is a Discord-first October event for Squigs Reloaded.

- 🍬 **Treat:** one manual claim per Discord user per calendar day in `America/Toronto`.
- A Treat requires at least one Squigs Reloaded NFT across the user's event wallets and **zero active Squigs Reloaded listings** across those wallets.
- 👻 **Trick:** one verified Trick for every legitimate Squigs Reloaded secondary purchase made during the event by one of the user's linked/event wallets.
- There is **no Trick cap**.
- Every Treat and every verified Trick is one Halloween draw entry.
- The event is calculated per Discord user, not per wallet and not per NFT held.

A missed Treat cannot be back-claimed.

## Multi-wallet and anti-abuse behavior

UglyBot reuses its existing verified `wallet_links` identity.

When Trick or Treat is enabled, wallet link/unlink/reassignment activity is copied into the event audit table. Once a wallet has been associated with a Discord user during the event, that wallet remains part of that user's event-wallet history for October even if it is later unlinked. This prevents a holder from hiding a listed wallet immediately before claiming a Treat.

If the same wallet becomes associated with more than one Discord account during the event, claims are blocked for review instead of guessing which identity should receive entries.

Treat eligibility checks the user's complete event-wallet history. Trick purchases can be made by any wallet in that same event identity.

## Treat verification

The **Claim Today's Treat** button performs a fresh check:

1. event is enabled and within the configured event window;
2. current verified wallet links are synced to event history;
3. no wallet identity conflict exists;
4. the user owns at least one Squigs Reloaded NFT across event wallets;
5. OpenSea V2 active listings are queried for every event wallet;
6. no active listing whose asset contract is Squigs Reloaded is found;
7. a unique database row is inserted for that Discord user/event day.

The database has a unique constraint on `event_key + guild_id + discord_id + event_day`, so retries, duplicate Discord deliveries and double-clicks cannot create two Treats.

If ownership or listing verification is unavailable, the claim fails closed and no Treat is recorded.

### Listing source limitation

The initial implementation uses OpenSea V2's **active listings for an account** endpoint and filters those results to the Squigs Reloaded contract. This reliably covers active OpenSea listings.

It does **not** claim to detect off-OpenSea listings created on every possible marketplace. If the community treats another marketplace as a supported listing venue, add that marketplace/aggregator as a second listing source before representing the rule publicly as “listed anywhere.”

## Trick verification

The **Submit a Trick** button asks for an exact OpenSea NFT item link.

UglyBot then:

1. parses and validates that the item is the configured Squigs Reloaded contract;
2. queries OpenSea V2 NFT sale events during the configured event window;
3. finds a sale whose buyer/taker is one of the user's event wallets;
4. rejects a sale whose seller is also one of that user's event wallets;
5. optionally requires that the user still owns the Squig (default: yes);
6. verifies the transaction receipt through Ethereum RPC;
7. requires the expected ERC-721 `Transfer` of that token to the buyer;
8. requires the configured number of block confirmations;
9. persists a unique Trick row.

The uniqueness key includes event, chain, contract, token ID and transaction hash. The same purchase cannot be claimed twice, but the same Squig can legitimately be sold again later in October and produce a different Trick for the later buyer.

## Discord UI

Admin posts the panel with:

`/trickortreat post`

Public panel buttons:

- 🍬 **Claim Today's Treat**
- 👻 **Submit a Trick**
- 🎃 **My Bag**

Users can also run:

`/trickortreat bag`

Admin commands:

- `/trickortreat status`
- `/trickortreat status user:@user`
- `/trickortreat export`
- `/trickortreat draw winners:<count>`

The draw uses each user's literal `Treats + Tricks` count as weight. There is no Trick cap. Once a Discord user wins, that user is removed from later rounds of that draw so separate winner positions are unique.

A 32-byte random seed plus the complete entry snapshot are persisted. Winner selection is deterministic from that seed so a completed draw can be audited/replayed.

## Environment variables

Required for live verification:

- `TRICK_OR_TREAT_ENABLED=true`
- `OPENSEA_API_KEY`
- an Ethereum RPC through one of:
  - `TRICK_OR_TREAT_ETH_RPC_URL`
  - `ETH_RPC_URL`
  - `ALCHEMY_RPC_URL`
  - `ALCHEMY_API_KEY`

Defaults:

- `TRICK_OR_TREAT_EVENT_KEY=squigs-trick-or-treat-2026`
- `TRICK_OR_TREAT_TIME_ZONE=America/Toronto`
- `TRICK_OR_TREAT_START_AT=2026-10-01T00:00:00-04:00`
- `TRICK_OR_TREAT_END_AT=2026-10-31T23:59:59-04:00`
- `TRICK_OR_TREAT_CHAIN=ethereum`
- `TRICK_OR_TREAT_MIN_CONFIRMATIONS=2`
- `TRICK_OR_TREAT_REQUIRE_CURRENT_OWNERSHIP=true`

Optional:

- `TRICK_OR_TREAT_SQUIGS_CONTRACT` — defaults to the existing Squigs Reloaded contract.
- `TRICK_OR_TREAT_ADMIN_CHANNEL_ID` — reserved for event-specific admin routing.
- `TRICK_OR_TREAT_ACTIVITY_CHANNEL_ID` — when set, successful Tricks post a short public activity message.

## Deployment / rollout

1. Deploy with `TRICK_OR_TREAT_ENABLED=false`.
2. Confirm startup creates the Trick or Treat tables without errors.
3. Ensure the existing OpenSea key and Ethereum/Alchemy RPC are present in Railway.
4. Set the event dates/timezone.
5. Enable `TRICK_OR_TREAT_ENABLED=true`.
6. In an admin/test channel, run `/trickortreat post`.
7. Test:
   - one eligible Treat;
   - a second Treat attempt on the same day;
   - a holder with a known active OpenSea Squigs listing;
   - one real October secondary purchase;
   - duplicate Trick submission;
   - multi-wallet holder;
   - wallet unlink/relink handling.
8. Only after live listing and purchase verification succeed, post the panel publicly.

## Draw safety

The normal draw is blocked until the event end time. Admins can pass `force:true` for deliberate testing.

A forced test draw is still persisted as an auditable draw, so do not use `force` casually on production data.

## Disable / rollback

Set:

`TRICK_OR_TREAT_ENABLED=false`

The panel will stop issuing new Treats or Tricks. Existing records remain intact for audit/export.

The feature is additive and does not alter Bounty Vault, Maw, Marketplace, Duels, Mad Libs, rewards, or $CHARM balances.
