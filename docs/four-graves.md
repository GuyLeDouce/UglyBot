# Trick or Treat: The Four Graves — administrator guide

**Readiness: STAGING_VALIDATION_REQUIRED.** Feature defaults to disabled. No production activation, game execution or prize distribution is performed by this feature. This document contains administrator-only event spoilers; do not share it with participants before reveals.

## Event and participant rules

Each persisted October TREAT or verified TRICK gives one allocation unit. Holders keep using the existing `/trickortreat` claims and wallet verification. Four Graves never updates or deletes those records. There is no purchase cap and no new holding/listing requirement at burial time.

Participants choose a grave, inspect their private balance, enter a positive whole-number quantity and confirm **BURY MY TICKETS**. Allocations are permanent. They can split tickets between any graves, add further allocations while OPEN, or leave tickets unallocated. Every buried ticket becomes one contestant. A cancellation does not write an allocation. Unallocated tickets never enter grave exports.

| Stable ID | Default name | Fixed game |
|---|---|---|
| `barry_buckets` | Barry Buckets | Wheel of Names |
| `barbara_bowkins` | Barbara Bowkins | Squigwreck |
| `cletus_von_wrinkles` | Cletus Von Wrinkles | Reverse Ugly City |
| `scuba_steve_3000` | Scuba Steve 3000 | Duck Race |

Both decision buttons execute the same workflow. Choice is recorded only for storytelling and audit. Games never change. Each grave has 1 DollzOnApe, 3 Squigs Reloaded, 1 Ugly Monster and 5 separate awards of 5,000 $CHARM: **10 confirmed slots**. A single shared database configuration supplies all pools. Mystery rewards start as an empty list, never as invented placeholders.

The administrator runs every competition, prize wheel and award manually. Wheel winners are removed for the current pass. Squigwreck and Duck Race use first-place-first order; Reverse Ugly City uses last-place-first order. If prizes remain after a complete pass, manually restore the entire original contestant list, including all duplicate entries. No game engine, payout or prize-recipient system is added.

## Commands and authorization

All responses and exports are ephemeral. `/fourgraves status` is public-safe even when invoked by a regular member. Games and prizes are excluded before reveal. Administrators get additional accounting, resources, decisions and operational diagnostics.

- `/fourgraves admin`: prepare event configuration and open the administrator controls.
- `/fourgraves configure channel:<text channel> category:<optional category> image:<optional attachment> image-url:<optional HTTPS URL> config-json:<optional JSON> finalize-prizes:<optional boolean>`.
- `/fourgraves status`: totals, grave participation and reveal progress; additional detail for administrators.
- `/fourgraves export grave:<all or grave> format:<all|contestants|master|audit>`: private ZIP download. Both default to `all`.

Administration reuses `isAdmin`: configured `DEFAULT_ADMIN_USER` IDs or members with Manage Server. Every administrative button rechecks this authorization. An active-looking button never substitutes for database state, team eligibility or countdown generation.

Admin panel controls: SETUP EVENT, POST GRAVE PANEL, OPEN GRAVES, CLOSE GRAVES, STATUS, EXPORT ENTRIES, RESTART COUNTDOWN, RECONCILE ROLES, REFRESH PANELS and RETRY UNCERTAIN CREATES. Opening, closing, restarting and retrying ambiguous resource creation have explicit confirmation screens.

The public panel has four grave buttons. The private balance response then offers ENTER QUANTITY, which opens the modal without waiting on database I/O. This extra step ensures Discord's modal response deadline is met. The final confirmation includes the amount, current balance, grave and permanent-allocation warning. Receipt retries identify the original allocation rather than spending again.

### Configuration examples

Set destinations with Discord channel selectors, then use POST GRAVE PANEL in that destination. Without an image, the panel is text-only. Use a durable HTTPS artwork URL for a long event: Discord attachment URLs may expire; replace them with `/fourgraves configure image-url:...` while allocation is still open.

Finalize no mystery prizes explicitly:

```text
/fourgraves configure config-json:{"mystery":[]} finalize-prizes:true
```

To add actual confirmed mystery rewards, provide their real names and positive quantities in `mystery`. Do not use placeholder prizes. Changing `mystery` invalidates finalization unless `finalize-prizes:true` is explicitly included. Closing is blocked until finalization, so a participant cannot trigger an unconfigured reveal.

`config-json` supports:

- `names`: exactly four nonempty display names, at most 40 characters each, in fixed grave order.
- `rules`: exactly four informational game descriptions, at most 900 characters each.
- `mystery`: up to ten `{ "name": "actual confirmed NFT", "quantity": 1 }` objects; names at most 80 characters, quantities 1–100. `[]` means no additional confirmed slots.
- `revealDelaySeconds`: integer 1–30, default 3; minimum delay between durable reveal stages, processed on the recovery worker's next pass.

Configuration freezes at closing. Names never change stable grave IDs or game assignments. The base pool is stored once in event JSON rather than scattered across message code. Its confirmed quantities are supplied by the setup defaults. No game-assignment configuration endpoint exists.

## Before the event

1. Deploy to an isolated staging Railway service with `FOUR_GRAVES_ENABLED=false`.
2. Verify the startup SQL migration and existing Trick or Treat claims/tests.
3. Enable Discord Server Members Intent for the **staging bot**, then set `FOUR_GRAVES_ENABLED=true` and redeploy staging.
4. Run `/fourgraves admin` and SETUP EVENT. Verify all resource operations succeed.
5. Configure the public channel, optional category and artwork.
6. Configure actual mystery rewards or explicitly finalize an empty mystery list.
7. POST GRAVE PANEL and verify all grave buttons remain disabled until manual opening.
8. Check private channels using a non-administrator test account. Administrators can always see their managed channels.

Dedicated roles have names such as `Grave — Barry Buckets [FG123]`; the event suffix makes recovery unambiguous. Channels have stable machine markers in their topics. Do not edit those topics or reuse these roles for unrelated purposes.

Bot permissions: Manage Roles, Manage Channels, View Channel, Send Messages, Embed Links, Attach Files and Read Message History. Keep the bot's highest role above all grave roles. Existing configured administrators and Manage Server roles receive access. Team roles have no server-level permissions. Only confirmed frozen allocations grant team membership. A member in several graves receives every corresponding role.

## Manual event operation and timing

Lifecycle: `SETUP → OPEN → CLOSED_COUNTDOWN → BUTTONS_ACTIVE → REVEALED`.

1. On your chosen allocation day (for example October 30), confirm OPEN GRAVES.
2. Monitor balances and grave statistics. New valid claims remain allocatable while OPEN.
3. When ready for the reveal, confirm CLOSE GRAVES. Closing permanently freezes entries, starts the persisted 15-minute deadline and queues team access/panels.
4. Verify STATUS and repair any Discord failures. Failure never reopens allocations.
5. At the deadline, the decision becomes eligible. Panel refresh follows on the next worker pass; backend authorization uses the persisted database deadline even if UI is delayed.
6. The first eligible member per grave chooses TRICK or TREAT. The stored decision identifies the member and choice, then the same team panel progresses through suspense to the fixed game and full prize pool.
7. Export all contestants. Run competitions, prize wheels and distribution manually.

**Timing clarification:** closing on October 30 starts a countdown on October 30. There is no overnight reveal hold. For an October 31 reveal, keep graves open until 15 minutes before that intended reveal window on October 31. No calendar date automatically opens or closes anything.

RESTART COUNTDOWN requires confirmation and is prohibited after **any recorded decision**, even if its reveal message has not yet delivered. It increments generation, invalidates old buttons and never reopens allocations. Disconnects and redeployments retain the original deadline.

Empty graves are marked NO CONTESTANTS and cannot decide. Their pool remains unawarded for manual administrator review. When every populated grave has completed its reveal, the overall event becomes REVEALED; an empty grave remains explicitly unrevealed. An entirely empty event remains manageable without inventing contestants.

## PostgreSQL integration and integrity

The repository uses startup schema initialization rather than a migration runner. `modules/fourGravesSchema.sql` is an additive, idempotent startup migration, executed after `trickOrTreat.ensureTables()` on the existing `prizesPool` connection. It runs with the feature disabled too. No new database service is needed.

New tables:

- `four_graves_events`: unique `(event_key,guild_id)`, immutable source-event linkage, checked lifecycle, shared JSON config, public panel IDs, timestamps, deadline, generation, snapshot JSON/UUID/SHA-256, last export timestamp.
- `four_graves_graves`: composite `(event_id,grave_id)` primary key, checked fixed game mapping, role/channel/panel IDs, first decision JSON, reveal stage/deadline/timestamp.
- `four_graves_allocations`: append-only ledger, positive quantity check, globally unique confirmation idempotency key, composite grave foreign key, identity at allocation, source counts observed at commit, timestamp. Indexed by `(event_id,discord_id)`.
- `four_graves_operations`: unique event/operation key, attempt count, last error and completion timestamp for resource creation/reconciliation.
- `four_graves_audit`: actor, action, detail and database timestamp for setup/configuration/transitions/allocations/decisions/exports/recovery.

Allocations and lifecycle transitions lock the event row in a PostgreSQL transaction. Two users or processes cannot race a close or overspend a shared balance. The confirmation ID is retained for duplicate receipts. Closing additionally takes a short SHARE lock on the two existing claim tables to wait for in-flight inserts and keep their snapshot stable; new claims briefly wait, then resume unchanged after commit. No Discord API calls occur while this claim-table lock is held. The administrator's member-name fetch happens before the transaction. Names for departed members fall back to recorded allocation identities; zero-allocation users without cached member data use their Discord ID.

Only existing persisted `trick_or_treat_treats` and `trick_or_treat_tricks` rows are counted. No independently incremented balance exists. Original verified claim timestamps/history are never changed. Source totals at allocation are audited, but no individual TRICK/TREAT attribution is fabricated for a ticket selected from the combined balance. Audit source type is blank for these entries.

Closing stores the full immutable export snapshot, not just aggregate counts. It includes every earner (even zero allocations), identity labels, per-grave totals, individual entry IDs and ledger links. The SHA-256 covers the serialized snapshot. Subsequent claims or Discord name changes cannot change closed exports.

## Export formats

Default `four_graves_exports.zip` contains:

```text
grave_1_barry_buckets.csv
grave_2_barbara_bowkins.csv
grave_3_cletus_von_wrinkles.csv
grave_4_scuba_steve_3000.csv
four_graves_master_allocations.csv
four_graves_ticket_audit.csv
```

The four game CSVs have only a `contestant` column. Five entries for Guy are `Guy`, `Guy(1)`, `Guy(2)`, `Guy(3)`, `Guy(4)`. Numbering restarts per grave. Distinct users with colliding names get a deterministic full Discord-ID suffix; suffix-like real names are checked too. CSV names remove control/bidi characters and retain Unicode. Potential spreadsheet formulas are neutralized with a leading apostrophe. Copy parsed cell contents, not raw CSV quoting, into games.

Master columns: `discord_user_id,discord_username,discord_display_name,total_treats,total_tricks,total_earned_tickets,barry_buckets_tickets,barbara_bowkins_tickets,cletus_von_wrinkles_tickets,scuba_steve_3000_tickets,total_allocated,remaining_unallocated`.

Audit columns: `event_key,grave_identifier,grave_display_name,discord_user_id,discord_display_name,contestant,ticket_entry_number,source_type,allocation_transaction_id,allocation_timestamp,snapshot_identifier,export_generated_at,ticket_entry_id`.

Stable entry ID is `allocation_id:ordinal_within_allocation`. All exports use one consistent live snapshot, or the exact frozen snapshot after close. Audit generation timestamps naturally differ between export requests; contestant labels/order do not. Totals across the four contestant CSVs reconcile with the master allocation totals. The master always covers all users even when one grave's contestant/audit export is selected.

Oversized exports are split on complete CSV records into multiple smaller ZIP parts delivered through private ephemeral follow-ups. Each CSV part repeats its header. No unauthenticated download URL or external storage dependency is introduced. For very large events, use per-grave requests to stay within Discord's interaction-token lifetime.

## Restart, repair and failure handling

The worker polls every 15 seconds and uses a PostgreSQL advisory lock per event to prevent concurrent resource provisioning across bot processes. Database countdown eligibility is exact; Discord API delivery is eventual. Persistent custom IDs bind the version, event, grave and decision generation. Every submit rechecks current state. The feature does not depend on in-memory collectors.

Reveals edit a single stored team message. Each successful stage edit is followed by a durable stage update. A crash between edit and update repeats an idempotent edit, not a new announcement. Deletion recovery searches message history for the bot's exact event marker before creating anything. It checks the author before editing. Discord delivery failures never delete allocations.

- **Missing role/channel:** RECONCILE ROLES recreates missing dedicated resources and restores actual members' assignments. Unrelated roles/channels are untouched. Wrongly assigned dedicated grave roles are removed.
- **Participant left:** their entries remain exported. On rejoin, reconciliation restores eligible roles.
- **Missing panel:** REFRESH PANELS searches for its marker before replacement.
- **Definite Discord 4xx create rejection:** fix permissions/rate-limit cause and retry; the rejected create can safely be attempted again.
- **Unknown create outcome:** `CREATE_IN_FLIGHT` intentionally blocks blind retries. Inspect the named role, channel topic or bot-message footer. If the marked resource exists, ordinary reconciliation adopts it. If it is definitely absent, use RETRY UNCERTAIN CREATES and confirm after inspection. Do not confirm if an existing resource was renamed or its marker removed; restore the marker first.
- **Permission failure:** verify Member Intent, bot role hierarchy and private channel access. STATUS lists failed operations. Changed grave reconciliation errors also use the existing `postAdminSystemLog` route when configured; ensure that existing log channel is administrator-only.
- **Source discrepancy:** allocation/snapshot creation fails visibly rather than changing claims. Inspect source records and the ledger; there is no casual correction/withdrawal command.
- **Missing database:** startup logs a schema/startup failure. Repair connectivity and restart; do not operate the finale until migrations succeed.

Do not manually grant grave roles, remove markers, or modify private channel overwrites during the event. If a dedicated role has server permissions added, reconciliation fails closed for review. Preserve at least two available connections in the shared PostgreSQL pool while the recovery worker holds its session advisory lock.

## Railway rollout and rollback

Actual repository configuration: `railway.json` uses **RAILPACK**, `npm start` (`node index.js`), restart policy ON_FAILURE with 10 retries. `package.json` targets Node 20. No automatic production deployment is requested by this PR.

New environment variables:

| Variable | Default | Requirement |
|---|---|---|
| `FOUR_GRAVES_ENABLED` | `false` | Set `true` only in the environment being validated/activated |
| `FOUR_GRAVES_EVENT_KEY` | `squigs-four-graves-2026` | Optional; do not change after creating an event |

No new secret is required. The source event uses existing `TRICK_OR_TREAT_EVENT_KEY`; existing timezone remains `TRICK_OR_TREAT_TIME_ZONE` (America/Toronto). UTC timestamps and Discord-localized timestamps are used internally/in messages. Countdown is fixed at 15 minutes, not an environment override. Destinations, artwork, mystery prizes and text are database-backed configuration.

Staging steps:

1. In Railway, create/select an isolated staging environment and service tracking this feature branch. Use a disposable PostgreSQL database and a separate Discord bot/test guild. Never point staging at live ticket data.
2. Supply the repository's existing required application variables for that service, including its existing prizes DB connection and Discord credentials. Keep unrelated feature flags disabled as appropriate. Set `FOUR_GRAVES_ENABLED=false`.
3. Deploy through Railway's existing RAIlPACK/NPM configuration. Verify migration startup. Do not paste secrets into logs or this repository.
4. Run the automated suite against a disposable PostgreSQL database, then enable only the staging Four Graves flag and redeploy.
5. Follow the complete manual checklist below with real Discord test accounts. Observe all 15 minutes at least once.
6. Review this PR and test evidence before merging. Confirm your Railway production service's branch/automatic-deploy settings before merging: merging may trigger an existing automatic deployment even though the feature remains disabled.
7. Only after explicit production approval: deploy the reviewed commit with the flag false, verify additive tables and claim continuity, configure permissions/inputs, enable and set up. Opening and closing still require deliberate admin confirmation.

Rollback: set `FOUR_GRAVES_ENABLED=false` and redeploy. Handlers reject new Four Graves interactions and workers stop starting. Existing Trick or Treat claims continue under their independent flag. Do not drop tables, erase snapshots, undo allocations or delete Discord resources. To resume, re-enable the same event key, restart, inspect STATUS and reconcile. Expired deadlines stay expired; do not restart the countdown unless explicitly intended and no decision exists.

## Testing and staging checklist

```sh
npm install --ignore-scripts
npm run test:trick-or-treat
npm run test:four-graves
# Set FOUR_GRAVES_TEST_DATABASE_URL only to a disposable test database.
npm run test:four-graves-db
```

Database tests create a unique test schema and drop only that schema on cleanup. They never default to application database variables. CI runs them against PostgreSQL 16 using Node 20. Optional local embedded validation uses a separately installed `@electric-sql/pglite` with `FOUR_GRAVES_TEST_PGLITE=true`; it is not a production dependency and cannot prove multi-connection lock behavior.

Manual staging validation still required:

- Verify original daily Treat, purchase verification, bag, export and original draw remain unchanged. Four Graves never invokes the original admin-only draw; decide operationally whether you intend to run that separate draw so participants are not promised duplicate finale mechanics. There is no scheduled original draw in the inspected Trick or Treat module.
- Test text-only and artwork panels; posting must not open allocations.
- Use two regular accounts and an administrator. Confirm private balances, modal, cancel, final confirmation, simultaneous allocations and replay receipts.
- Split entries among all graves; leave another earner unallocated. Confirm late earned tickets become available while open.
- Close while a confirmation is pending. Verify one frozen snapshot, role isolation and the real 15-minute deadline.
- Restart/redeploy during countdown; test premature and expired/stale buttons and countdown restart before decisions.
- Have two members click simultaneously. Verify one recorded presser and the correct game/pool. Restart during the reveal delay.
- Test missing roles/panels, failed permissions, member departure/rejoin and two bot processes against the same database.
- Test one-member and empty graves. Nobody can decide for an empty grave.
- Export and import parsed names into the four manual games. Check sum of contestants against master totals, Unicode/collisions and unchanged frozen names after renaming a member.
- Disable and re-enable the feature without deleting data. Confirm original claims still work.

Remaining administrator inputs: final artwork, public panel destination, optional category, final mystery identities/quantities (or explicit none), Discord permissions/Member Intent, staging credentials/test guild and production rollout approval.
