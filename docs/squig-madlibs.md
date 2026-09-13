# Squig Mad Libs — owner and recovery guide

## Review boundary

This feature is authored against main commit `fc02c69dfda8ec96120fa4f3b508d769995cfa73` on `feature/squig-madlibs`. A feature-branch commit or pull request is not authorization to merge, deploy Railway, migrate production, or transfer real $CHARM. Keep `MADLIB_ENABLED=false` until review and a separately authorized sandbox test. Automated checks never start the bot or use production credentials.

UglyBot remains one bot. Mad Libs reuses its existing client, `prizesPool`, verified wallet/Discord/DRIP mappings, holder rules, guild settings, balance/transfer helpers, admin checks and logging. It creates no new registration, shadow token balance, website, image-generation API integration or production database URL. Users generate images externally with their attached Squig reference.

## Player and administrator commands

`/madlib` is admin-only and has no required subcommand. It posts a persistent **SQUIG MAD LIBS** panel with exactly **PLAY** and **SHOW** buttons in the current channel. Administrator authorization is checked again server-side using the existing helper and freshly fetched member permissions.

PLAY first resumes an unfinished session. Otherwise an eligible member receives one free play every rolling 24 hours, measured from reservation, not a midnight reset. Multiple wallets do not create more free allowances. An extra play requires an explicit expiring **Spend 1,000 $CHARM** confirmation. Paid plays do not reset the next-free time. Confirmation rechecks the free allowance and uses a now-free play instead of charging. A changed price or expired quote is shown again before payment.

Each private question has an Answer button opening one modal input. Submitting saves the answer and revision before moving on; Back revisits a previous question. Exit saves and pauses. A separate explicit abandonment confirmation explains that an already-used free allowance or confirmed paid play is not refunded simply for cancelling. Uncertain payments cannot be abandoned and recharged to bypass review. Restarting, returning to the panel or expiring an interaction does not create another charge.

Completion stores the full **Image Prompt** and **Classic Mad Lib Story**, then shows them in separate private messages. Full UTF-8 downloads never truncate either output. Get Copyable Prompt supplies plain text or its complete text attachment; it does not claim clipboard access. Saved outputs are not regenerated from subsequently edited templates.

SHOW lists saved completed stories newest first, 20 per page. `/madlib-history` provides the same private history from any accessible text channel, including after a member loses holder access to the original panel channel. Historical read/export does not require fresh holder eligibility; starting a new play, uploading and publishing do.

Select a story, then use `/madlib-upload image:<one attachment> story_id:<optional saved story ID>`. This is a normal eligible-player command, not an administrator-only subcommand. Omitting story_id uses the saved SHOW selection. A private preview shows the full story, author, image, target channel and applicable reward rules. **Publish** is the only action that posts publicly. Replace Image stages a replacement; Cancel deletes private staging but retains the story. Reopening SHOW restores a valid staged preview and revokes old preview buttons. SHOW never charges or rerolls.

## Railway configuration

Add alongside the existing UglyBot service variables, without replacing existing credentials:

```env
MADLIB_ENABLED=false
MADLIB_CHANNEL_ID=1334884237727240267
MADLIB_REACTION_EMOJI_ID=1526597741160169522
MADLIB_FREE_COOLDOWN_HOURS=24
MADLIB_EXTRA_PLAY_COST_CHARM=1000
MADLIB_REACTION_REWARD_CHARM=100
MADLIB_REWARD_CAP_PER_POST=0
MADLIB_RECONCILE_INTERVAL_SECONDS=300
MADLIB_MAX_IMAGE_BYTES=8388608
```

Optional comma-separated existing role IDs: `MADLIB_ALLOWED_ROLE_IDS` overrides normal holder access; `MADLIB_REACTOR_ROLE_IDS` restricts qualifying reactors. Empty role values retain default rules. Blank numeric values use their defaults; invalid numbers disable only this feature rather than creating accidental free paid plays. **A reward cap of 0 means unlimited**, not zero rewards. A positive cap is an amount of $CHARM per post, not a reaction count. Changed environment settings do not rewrite accepted prices or historical publication reward/currency/role snapshots.

No image API key, duplicate DRIP secret or new database URL is required. The selected existing `prizesPool` still uses its original prizes/team database fallback. Confirm that pool's database is the intended test database before enabling. An enabled bot initializes only the additive `madlib_` schema there; the build and tests do not run a production migration.

## Access and Discord permissions

Default new-play access uses verified existing wallet links plus matching existing Squigs Reloaded holder rules/roles. When fresh ownership lookup is necessary it uses the existing helper with `suppressErrors:false`; a provider outage is not zero ownership. The feature never grants roles or runs holder-sync jobs. Role overrides do not invent a DRIP identity; financial actions still need a positively resolved member. Conflicting verified wallet/DRIP mappings fail safely rather than selecting an arbitrary recipient.

Every interaction validates the owning guild/user, current member/channel access, and the saved state/revision. The target publication channel must be a text or announcement channel in the same guild. No forum or alternate channel is silently substituted. The bot needs View Channel, Send Messages, Attach Files, Embed Links, Read Message History and Add Reactions there. The exact custom emoji must exist and be usable; external emoji require Use External Emojis and role-restricted emoji require an allowed bot role. Players need command permissions for `/madlib-upload` and `/madlib-history`.

The only added intent is GuildMessageReactions, conditionally when enabled. Existing intents remain unchanged. No global Partials or new privileged intent is enabled. One filtered raw Gateway listener feeds the same reward path as reconciliation; no competing high-level reaction award listener is installed.

## Minimal integration and source layout

`scripts/installMadlibIntegration.js` is a guarded authoring/verification utility, not a Railway startup hook. It checks the original index SHA-256 and applies seven specific additive hunks: one require, conditional reaction intent, appended command definitions, isolated ready-worker start, an opt-in strict-transfer guard, dependency injection, and a scoped route in the existing interaction handler. Reversing those hunks must reconstruct every original index byte. An unexpected newer baseline is refused rather than overwritten. Existing modules, command prices, package scripts, dependency ranges and deployment configuration are otherwise unchanged.

The strict guard inside `awardDripPoints` is opt-in only for `madlibStrictTransfer:true`; legacy callers execute their original code. The existing helper contains route/payload fallbacks that cannot safely be used for a new debit after an uncertain outcome. This narrow guard prevents currency-less retries without rewriting those legacy behaviors.

`madlib.js` owns lifecycle/UI; `madlibCore.js` validates/configures/renders; `madlibAccess.js` reuses access; `madlibStore.js` and `madlibSchema.sql` provide persistence; `madlibEconomy.js` handles scoped transfers/recovery; `madlibImages.js` and `madlibImageWorker.js` validate images; `madlibPublishing.js` publishes/repairs; `madlibWorkers.js` reconciles. The static authored library is `madlibTemplates.js` plus `data/madlibScenesA.js` and `data/madlibScenesB.js`. It expands 60 complete ordered question definitions at load time, with no runtime LLM. Every persisted session contains its complete versioned snapshot.

The ten themes have six independently authored incidents each. Story and prompt share one frozen moment and use every submitted answer. Each pair contains an authored scene-specific editorial review; the template test writes complete sample pairs and those reviews to `template-content-review.md` and `template-renders.json`. Mechanical checks cannot prove meaning or external generator fidelity. Costume inputs affect clothing only; original face, eye count and 2D identity remain intact. Animal/plural inputs often decorate existing props instead of filling the scene with extra actors. These are playful non-canon scenes, not new Ugly Labs history or financial promises.

## PostgreSQL and durable state

The versioned migration creates only `madlib_` tables under a feature advisory transaction lock. No existing table is altered and no foreign key joins to legacy tables are created. External IDs are TEXT, timestamps TIMESTAMPTZ. Row locks and unique indexes enforce one active guild/user session, one play debit, one lifetime publication per story and one reward per publication/reactor/emoji. Short bounded transactions do not span Discord/DRIP calls. Existing pool configuration is unchanged; feature-local acquisition/query/lock/statement limits prevent indefinite waits.

Active sessions, frozen templates, answers, quotes, full outputs, uploads, publication state, operation identities, reaction awards and admin audits survive restarts. Paid/reaction/publication audit records are retained and do not cascade-delete. Ordinary abandoned staged images expire after 24 hours; potentially sent publication staging remains available for review. Viewing does not extend staging expiry.

## Financial safety and recovery

DRIP is authoritative. `getMarketplaceSpendableBalance` resolves identity/configuration, not a numeric balance. Mad Libs follows Malformed Marketplace's balance-check sequence: run the existing extractDripCurrencyAmountFromPayload on the freshly resolved member and configured currency first; only if no amount is present, call the existing direct balance helper with the same verified account aliases. Finite zero is valid but insufficient; unknown is not zero or unlimited. Realm/currency and fixed sender/recipient must match the saved operation.

The strict adapter uses one documented route with the existing header/HTTP helpers:

```text
PATCH /api/v1/realms/{realmId}/members/{senderMemberId}/transfer
{ "amount": amount, "recipientId": recipientMemberId, "currencyId": currencyId }
```

Reference: https://docs.drip.re/api-reference/realm-members-balances/transfer-member-balance-of-a-currency

This matches the first route and currency-explicit payload tried by the existing Marketplace helper. Mad Libs still makes only one transfer request per armed attempt, without copying the legacy currency-less or credit fallbacks. Echoed sender, recipient, currency or amount mismatches require review. A generic response id is not assumed to be a transaction id.

Before sandbox financial acceptance, the owner must verify the existing currency really is $CHARM and the treasury/API permissions support this route. No alternative currency, generic project credit, invented idempotency header or alternate treasury is used. Debit is resolved user member to configured treasury, default 1,000; reward is treasury to **author**, default 100, never reactor; refund reverses the same confirmed debit and currency. All use scoped contexts, `requireTransfer:true` and a runtime bot-ID accessor.

Operations are persisted before remote calls and separately track prepared, resolving, in-flight, confirmed-success, confirmed-failure, retryable-failure and needs-review states. One claimant may arm an intent. A stale resolving lease can be retried because no send started; a stale in-flight lease needs review. Timeout, network reset, 409/5xx, 202 asynchronous acceptance, conflicting success body, mismatched receipt or provider success followed by database failure never silently resends. Safe known-not-sent cases use bounded backoff. Unresolved earned rewards remain pending obligations. Local uniqueness does not prove remote exactly-once settlement. Balance deltas alone are not transaction evidence, since other bot features also move balances.

Use `/madlib-admin action:status` for guild revision, totals and queues, then `action:inspect record:<ID>` for the latest immutable identity/state and revision. Every mutation requires the current revision and specific evidence/reason (8–500 characters). Never paste credentials into evidence. Output is private feature-local records; existing API settings and wallet records are never serialized.

| Action | Safety behavior |
|---|---|
| pause / resume | Current guild revision; pauses future claim/arming, not an already-sent transfer. Saved progress/history remain accessible. |
| suspend / unsuspend | Current publication revision and moderation reason; affects future awards, not already earned obligations. Existing reactions can be discovered again after unsuspension. |
| mark-sent | Only unknown outcomes at least five minutes after send; authoritative transaction evidence must match realm, currency, sender, recipient and amount. Records success and its business effect without calling DRIP. |
| mark-not-sent | Same hold/evidence requirement, with authoritative non-settlement proof; absence of a log is not sufficient. Records confirmed failure, not an automatic resend. |
| retry | Only confirmed-not-sent/retryable operations, same identity; cannot charge cancelled/completed plays. |
| refund | Only a confirmed debit for an unfinished active session prevented by a documented system failure. Unique reverse-transfer intent; ordinary cancellation/disconnect or dislike is not a system failure. |
| link-publication | Exact existing message ID, bot author and stable marker validation; links without resending. |
| retry-publication | Only explicitly known-not-sent Discord HTTP failure; reuses original identity. Ambiguous sends cannot be blindly reposted. |

Mutations are stale-revision protected, recorded in `madlib_audit`, and logged via the existing admin system logger. Missing authoritative evidence leaves a record under review rather than guessing it away.

## Upload and publication safety

Only HTTPS Discord attachment paths on the explicit CDN/media allowlist are downloaded; no arbitrary URL, credentials, custom port or redirect. Actual streamed bytes, timeout, signature and dimensions are checked. Still PNG/JPG/JPEG/WebP only, maximum configured 8 MiB and 4096 pixels per side, including 4096 x 4096. MIME metadata is optional: common raster aliases, missing/generic binary labels and mismatched raster labels are accepted only when actual bytes pass header checks and full raster decoding. File extensions alone do not establish type. SVG, HTML, executables, animations and unsupported codecs remain rejected. Decoding and PNG normalization run in a short-lived child process with application credentials omitted, so a native decoder crash does not terminate the bot. This is crash containment, not a security sandbox or an absolute native-memory cap; file size, dimensions, five-second timeout and two-decode concurrency bounds remain enforced. JPEG EXIF axis rotation is accepted, and normalized output must also fit the byte limit. The older SDK does not expose every modern attachment-limit field; the conservative local cap remains enforced and actual sandbox upload limits still need verification.

Normalized bytes live in PostgreSQL BYTEA, not Railway's ephemeral disk or a temporary signed URL. A unique publication intent snapshots the approved image revision and reward rules before send. Public posts re-upload the image and include full story/author/reward totals, not the private prompt. Mentions are disabled. Payloads are validated before a send claim.

Deterministic Discord nonce deduplication is time-limited, not permanent delivery proof. A stable bot-owned footer marker and bounded history scan recover a post sent before a database acknowledgement failed. No match remains review, not permission to send again. Deleted posts retain their lifetime identity and cannot restart rewards. Emoji seeding retries separately; its failure does not republish. Display refresh fetches the current public attachment and distinguishes confirmed paid, pending and review totals.

## Reaction policy and bounded reconciliation

One exact custom-emoji reaction from a distinct eligible human guild member earns the author the snapshotted reward. Author, bot, system/webhook, wrong guild/channel/emoji/message and previously recorded accounts are excluded. Normal/burst forms are one award. Remove-and-readd does not award twice and paid rewards are not clawed back. Reactor wallet/DRIP registration is not required. There is no default 100-reaction cap, 10,000-$CHARM cap or age expiry. Earned author rewards survive later loss of holder access. Distinct Discord accounts do not prove unique humans: existing moderation and publication suspension address suspected alt abuse. Uncapped rewards can create significant treasury obligations; monitor pending totals before enabling production.

Raw events and periodic reconciliation call one unique candidate path. Queue capacity is 500; each non-overlapping five-second tick handles at most ten raw candidates, one financial operation and two publication reconciliation jobs. Database leases coordinate multiple processes. Reconciliation pages 100 normal then burst users and persists cursors, cycling due posts fairly including old posts. Overflow/current offline reactions can be recovered; reactions added and removed entirely while offline cannot be reconstructed. There is no hidden expiry to avoid old-post work.

## Verification and separately authorized acceptance

Run `node scripts/runMadlibChecks.js` for syntax, all feature suites and exact original-versus-branch legacy regression comparison. Individual scripts are `testMadlibLogic.js`, `testMadlibTemplates.js`, `testMadlibIntegration.js` and `testMadlibDatabase.js`. PostgreSQL tests require `MADLIB_TEST_DATABASE_URL` pointing to a disposable local `madlib_test` database plus `MADLIB_TEST_DATABASE_ALLOW_RESET=true`; they create/drop only a generated test schema. Missing DB means SKIP locally and failure in CI, never a claimed passing DB test. No suite boots index.js, logs into Discord or sends live DRIP calls.

CI uses Node 20 and PostgreSQL 16 with unchanged declared dependency baselines. Existing rewards, marketplace, Maw, rarity, disposition and Bounty tests are compared with the exact original index and unchanged legacy source in the same environment. Unchanged pre-existing failures are disclosed separately; new or changed failures fail verification. Consult actual report logs rather than interpreting setup-only workflow success as feature verification.

In a separately authorized sandbox, verify the selected database, realm/currency/treasury, channel/emoji/command permissions and admin/non-admin paths. Complete a free play, inspect both outputs/downloads, authorize one real **sandbox-only** extra-play debit, restart and Resume without another charge, and confirm paid play does not reset the free clock. Generate an external reference-based image, privately upload/replace/cancel/reopen, publish once, then verify one different human's exact-emoji reaction credits the author exactly once. Check remove/readd, self/bot/wrong emoji, restart/uncached old post, permissions failure, moderation and evidence-based recovery. Recheck existing commands/jobs/prices. Automated mocks do not certify these live account permissions or image fidelity.

Only a separate owner authorization may merge/deploy and change production enablement. This build leaves Railway and production databases/credentials alone.

## Rollback

Set `MADLIB_ENABLED=false` in the authorized environment and restart/redeploy only when approved. Disabled mode registers no new commands/intents, starts no feature migration/workers and returns safe unavailable responses for old controls. It does not erase saved stories or obligations. A sent transfer cannot be recalled by disabling a flag. Retain `madlib_` data and reconcile on re-enable; do not reset balances or delete uncertain operations. No legacy feature needs changing to roll this feature back.

## Image and Marketplace checkout repair

No new Railway variable, API credential, dependency upgrade, database schema or price is introduced by this repair. The original rejected image was not supplied, so its exact format and the live DRIP account require owner testing. Standard still PNG, JPG/JPEG and WebP downloads are supported within the stated limits. AVIF, HEIC, animated GIF, SVG and documents are not supported; this is not a claim that every generator export format works. Original-download MIME metadata can be absent or incorrect. The bot verifies the bytes rather than requiring a Canva conversion.

For a saved confirmed_failure / BALANCE_UNKNOWN operation, inspect the existing record after deploying the repair. Use /madlib-admin action:inspect record:<madlib_play:ID>, then /madlib-admin action:retry with the same record, current revision from inspection, and an appropriate evidence/reason. The retry retains that paid-play intent and rechecks balance and identity. Resume opens the same session once payment succeeds. Do not mark it sent, refund it, delete its row or blindly resend an uncertain transfer. Deployment does not automatically replay old failed debits.

Regression tests cover member payload balances (including zero and numeric strings), same-account aliases, unknown versus insufficient balance, the current transfer route and payload, mismatched success receipts, saved-failure retry with competing workers, missing/generic/mismatched MIME, PNG/JPEG/WebP decoding, JPEG EXIF orientation, exact 4096-square images, corrupt decoding, nonimages, unsafe hosts, redirects, size bounds and decode concurrency. No automated test uses production credentials or sends live DRIP transfers. See the actual test report to distinguish executed checks from skipped database tests.
