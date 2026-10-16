# Four Graves implementation report

## Readiness

**STAGING_VALIDATION_REQUIRED.** Automated local tests passed as detailed below. Live Discord/Railway validation has not been performed. No production deployment, activation, grave opening/closing, games or prize transfers were performed.

## A. Implementation summary

Adds an independently gated Halloween finale around the existing persisted Trick or Treat claim records. Uses the existing `prizesPool`, Discord client, command routing, admin authorization, startup SQL pattern and administrator log route. Original claim verification, wallet protections, ticket history, exports and draw code are unchanged.

Implements permanent confirmed allocation, source-count audit, event-row transaction serialization, an immutable close snapshot, private team resources and membership reconciliation, persisted countdown generations, first-writer decisions, durable reveal stages, one contestant per ticket and private CSV ZIP exports. Both choices have identical outcomes. All competitions and awards remain manual.

Repository inspection found no migration framework or shared durable task queue suitable for this feature. The implementation follows startup schema creation and adds persisted operational records plus a small PostgreSQL-advisory-lock worker. It does not introduce runtime dependencies. The existing original draw is an explicit admin command, not a scheduled finale invocation.

## B. Files changed

| File | Purpose |
|---|---|
| `index.js` | Module initialization, command registration, participant/admin/modal routing, feature-gated Member Intent, startup migration/worker |
| `package.json` | Adds two test scripts; no dependency changes |
| `README.md` | Feature flag and guide link |
| `modules/fourGravesSchema.sql` | Additive idempotent PostgreSQL schema |
| `modules/fourGravesStore.js` | Authoritative accounting, transactions, snapshots, decisions, audit and operation persistence |
| `modules/fourGravesCore.js` | Fixed games/default configuration, quantity validation, deterministic labels, safe CSV, ZIP/oversize splitting |
| `modules/fourGravesDiscord.js` | Managed private resources, permissions, membership, countdown recovery and idempotent message edits |
| `modules/fourGraves.js` | Slash commands, private admin UI, configuration, confirmation flows, balances, receipts and private export delivery |
| `scripts/testFourGravesLogic.js` | Pure accounting/export/message/configuration tests using existing Node assert style and real Discord builders |
| `scripts/testFourGravesDatabase.js` | Disposable-schema PostgreSQL accounting/lifecycle/concurrency and mocked Discord recovery tests |
| `scripts/fourGravesDiscordFixture.js` | Isolated Discord resource/member/message fixture; no real Discord sends |
| `.github/workflows/four-graves-check.yml` | Node 20 / PostgreSQL 16 PR validation |
| `docs/four-graves.md` | Complete operation, staging, configuration, exports, recovery and rollback guide |
| `docs/four-graves-implementation-report.md` | This report |

## C. Database changes

New tables: `four_graves_events`, `four_graves_graves`, `four_graves_allocations`, `four_graves_operations`, `four_graves_audit`. The event row stores the full immutable frozen snapshot, its UUID and SHA-256; no separate snapshot table is necessary.

Constraints: unique event/guild; checked lifecycle; checked fixed grave/game pairs; composite grave primary/foreign keys; positive allocation quantities; unique confirmation idempotency key; unique event/operation keys; reveal stage bounds. An event/user allocation index supports accounting.

Original tables are only queried and briefly SHARE-locked during closing. Closing waits for claim inserts already in progress, captures a consistent snapshot and releases the lock before Discord operations. Claim rows are never deleted, decremented or updated. Stored source counts document what was available at allocation without inventing individual source attribution.

## D. Discord interfaces

`/fourgraves admin`, `/fourgraves configure`, `/fourgraves status`, `/fourgraves export`. Full option names and examples are in the guide. Uses the actual `isAdmin` implementation: configured admin IDs or Manage Server permission. All actions revalidate server-side; exports and balances are private.

Opening, closing and countdown restart require confirmation. Restart is blocked after any decision. RETRY UNCERTAIN CREATES also requires explicit confirmation after inspecting Discord. No undo/reassignment endpoint exists. Regular users access grave selection, private balances, quantity modal, final confirmation and receipts. Team decisions additionally require a frozen ticket in that grave, the right channel, current generation and an expired persisted deadline.

## E. Environment

- `FOUR_GRAVES_ENABLED`: defaults to `false`; enable only in the environment under validation/activation.
- `FOUR_GRAVES_EVENT_KEY`: optional, defaults to `squigs-four-graves-2026`.

Reuses existing Trick or Treat source event key/timezone and database/Discord secrets. No new secrets or services. Configurable images, destinations, text and mystery rewards live in PostgreSQL. Countdown is fixed at 15 minutes.

## F. Executed verification

Local execution used Node 24.19.0; CI targets the repository's Node 20 requirement.

| Verification | Result |
|---|---|
| `npm run test:trick-or-treat` | Passed existing regression script |
| `npm run test:four-graves` | 41 named tests passed |
| `FOUR_GRAVES_TEST_PGLITE=true npm run test:four-graves-db` | 32 named database/Discord tests passed against embedded PostgreSQL |
| JavaScript syntax checks | Passed for new modules and integrated `index.js` |
| Discord slash-command serialization | Passed using installed discord.js builders |
| ZIP verification | Passed independent Python `zipfile` CRC/file-count check |

The embedded adapter serializes connections: its successful simultaneous-request scenarios verify accounting logic, **not PostgreSQL server multi-session lock behavior**. The PR workflow runs the same suite with concurrent `pg` connections on PostgreSQL 16. Consult the PR checks for that result; do not treat local embedded results as a substitute.

No local test failures remain. Not executed here: live staging Discord/Railway tests, real gateway reconnects, real rate-limit behavior, manual game CSV imports, and production migrations. Mocked Discord tests cover resource reuse, isolation, multiple roles, wrong-role removal, departed/rejoined membership, missing roles, failed assignments, failed reveal edits, new worker-instance recovery and admin permission enforcement. Full external API ambiguity still needs staging exercises.

The tracked `.env` was checked without printing its values; the check found no non-placeholder values under common secret-bearing key names. This is a targeted check, not a guarantee about all historical repository contents. No credentials were added to this change.

## G. Deployment and rollback

Actual Railway configuration is RAIlPACK + `npm start`, ON_FAILURE with 10 retries. Use the isolated staging service/database and separate test bot instructions in `docs/four-graves.md`. Enable Server Members Intent before starting with the feature enabled. Confirm the bot's role hierarchy, permissions and administrator log-channel privacy.

Run the migration with the flag false, validate original claims, then enable only staging. Complete the guide's lifecycle, permissions, real 15-minute countdown, restart and export checklist. Review production auto-deploy branch settings before merging. Production activation requires approval and remains a separate action.

Rollback by setting the feature flag false and redeploying. Keep all allocations, snapshots, decisions, channels and roles. Re-enable the same event key and reconcile to resume. Original Trick or Treat remains independently operational.

## H. Remaining administrator inputs

Final artwork (optional until supplied); public panel channel; optional private category; real mystery NFT identities/quantities or explicit none; Member Intent and bot permission configuration; isolated staging/test bot access; confirmation that the existing independent `/trickortreat draw` will be operated intentionally; final production approval.

## I. Important operational details

Closing always starts the countdown immediately. For an October 31 reveal, close 15 minutes before that intended reveal window; closing on October 30 does not defer reveals overnight.

Discord delivery is eventually reconciled every 15 seconds. The deadline itself is authoritative in PostgreSQL. Reveal stages edit one message to avoid duplicate announcements. An uncertain external create is blocked for inspection instead of blindly creating duplicates. Oversized exports use private multipart ZIP delivery, never a public URL. Empty graves remain unawarded and flagged even if all populated graves have revealed.
