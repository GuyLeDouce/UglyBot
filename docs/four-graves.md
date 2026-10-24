# TRICK OR TREAT: THE FOUR GRAVES — Finale Guide

## Experience

The existing daily Treat and verified Trick mechanisms continue unchanged. One earned ticket means one entry. Holders bury available tickets in Barry Buckets, Barbara Bowkins, Cletus Von Wrinkles, or Scuba Steve 3000 using the familiar public panel and private quantity-confirmation modal. Burials are permanent.

1. Admin SETUP EVENT, configures destinations and artwork, POST GRAVE PANEL, then OPEN GRAVES.
2. Holders bury tickets. The bot tracks real tickets but never deletes or consumes their original Treat/Trick claim records.
3. Admin CLOSE GRAVES. Tickets are frozen, private grave roles and channels are reconciled, and the existing 15-minute countdown starts.
4. After the countdown, each populated grave can choose 👻 TRICK or 🍬 TREAT. The FIRST eligible participant to click makes the choice for the whole grave. Both buttons reveal that grave's identical fixed prize pool.
5. The bot posts one @everyone notification INSIDE THAT PRIVATE GRAVE and identifies the member who chose. This is the only @everyone notification in that grave.
6. Two minutes after that decision was persisted, the bot draws and announces ONE prize winner in the same grave. Each subsequent winner is drawn and announced at least 15 seconds after the preceding winner was posted. Each grave advances on its own timeline.
7. After all prizes are announced, the administrators can download the winners CSV and distribute the NFT and $CHARM prizes manually. The bot NEVER transfers rewards.

**There are no separate Squigwreck, Wheel, Duck Race, or Ugly City games.** Old fixed-game database metadata remains only to preserve compatibility with existing schema and tests.

## Grave selection panel artwork and button order

The main Four Graves selection panel displays **https://i.imgur.com/CQPVlFX.png** above four buttons, left to right:

1. Cletus Von Wrinkles
2. Barry Buckets
3. Barbara Bowkins
4. Scuba Steve 3000

The spelling **Wrinkles** matches the original character and grave ID. Button placement is visual only; stable grave IDs, existing ticket allocations, private team channels, contest exports and winners are unchanged.

New events use the image by default. A one-time, audited startup update also refreshes stored LIVE and TEST panels, including already-closed panels, without changing frozen tickets, prizes, decisions or results. Future explicit administrator image overrides remain respected. The recovery worker edits existing bot panels to display this image and button order; it does not post duplicate selection panels.

## The living grave rosters — four stones, four lists

The public selection panel still displays the Halloween artwork, four grave buttons and spoiler-free Squigs story. **After the very first confirmed burial**, the bot posts **four independent roster panels** beneath the selection message, top to bottom:

1. Cletus' Grave
2. Barry's Grave
3. Barbara's Grave
4. Steve's Grave

Each panel displays **one line per participant**, followed by **their current count of tickets buried in that grave**, for example:

- Player 1 — 12 tickets
- Player 2 — 3 tickets
- Player 3 — 1 ticket

These counts are dynamic; the displayed numbers are examples, not fixed prizes, initial balances or hardcoded values. The bot groups entries by Discord user ID using the existing deterministic contestant labels, so participants with the same display name are still distinguished. Participants can have tickets in more than one grave.

**Crucially, this changes only the presentation.** Every actual buried ticket remains its own individual entry in the underlying immutable allocation/snapshot pool and is counted separately during random winner selection. A participant with twelve buried tickets has twelve ticket-level chances whenever they are eligible for a draw pass; the pre-existing winner rotation rules are unchanged.

The top of each roster keeps an eerie explanation of what is happening without listing prizes or spoiling the final TRICK/TREAT choice. Each panel shows its **total buried tickets and total distinct Squigs**. A grave with no burials shows a deliberately eerie empty-state placeholder.

All 58 participants can fit on a single grave panel at ordinary username lengths, even if the full 455 tickets were buried in that grave. For extreme username lengths or much larger groups, each panel shows as many names as fit under Discord's embed limit and offers **View full roster**; its ephemeral, 20-participant pages show the complete lists without spam.

Each grave's roster has its own persisted message ID and unique recovery marker. The worker creates them in visual order after the selection panel, then edits each one in place as confirmed burials arrive. An immediate refresh is requested after a burial, with routine polling as a fallback. Once the graves close, these four rosters are frozen from the event's original ticket snapshot even if fresh October claims happen later.

The rosters share nothing across live and test events, suppress Discord mentions, and are independent of the existing private grave roles/channels, countdowns, prize pools, automatic draws and stored winners. This feature does not create, destroy, combine or reassign any tickets.

## Finalized prize pools

Every grave has 1 DollzOnApe, 3 Squigs Reloaded, 1 Ugly Monster and 5 separate prizes of 5,000 $CHARM. In addition:

| Private grave | Additional Mystery NFTs | Total prizes |
|---|---|---:|
| Barry Buckets | 3 × Scribble Otter Yoink Club | 13 |
| Barbara Bowkins | 1 × Visions of the Void | 11 |
| Cletus Von Wrinkles | 3 × RugDollz 3D | 13 |
| Scuba Steve 3000 | 3 × Chill Bugs | 13 |

**50 prize awards:** 30 NFTs and 20 $CHARM awards (100,000 $CHARM total).

**Cletus bonus update:** RugDollz 3D replaces the retired 2 × Doruzu (ApeChain) bonus. On startup, UglyBot updates only SETUP/OPEN event records whose saved Cletus bonus still exactly matches the retired Doruzu allocation. The migration logs one audit entry and is safe to run repeatedly. Other graves, tickets, and previously recorded allocations are unchanged. An event that has already CLOSED or has a separately customized Cletus pool is intentionally not modified; inspect its saved config before announcing any prize changes.

The default configuration contains these exact per-grave mystery prize lists and is finalized. Existing unclosed events with an old shared mystery list are upgraded on admin setup. Already-closed legacy events are NOT silently changed; they require administrator review.

## Final public announcement: UGLY HALLOWEEN REWARDS

Once all **populated** graves have completed all their individual prize drawings and each winner has been successfully announced in their corresponding private grave channel, UglyBot automatically posts a final recap in the **ORIGINAL Choose Your Grave selection channel**, under the existing selection and four roster panels.

The public recap is titled **🎃 UGLY HALLOWEEN REWARDS** and combines winnings **per unique Discord participant across all four graves**, rather than listing the same Squig repeatedly:

```text
🎃 UGLY HALLOWEEN REWARDS
Player 1 — Squigs Reloaded, 10,000 $CHARM
Player 2 — 2 × RugDollz 3D
Player 3 — DollzOnApe, Ugly Monster, 5,000 $CHARM
```

These are examples only. Real names, numbers, prizes and $CHARM sums come exclusively from the verified and persisted draws. The bot **never invents winners** and never changes prize allocation odds or records. Duplicate NFTs are combined using a quantity (e.g. 2 × Squigs Reloaded). Multiple 5,000 $CHARM awards are summed into one credit amount for easier reading.

The recap waits for the overall state **REVEALED**, and independently verifies that every nonempty grave has a decision, a completed draw, and exactly the expected number of stored, announced prizes. Empty graves have no eligible entrants and cannot block the announcement, while unawarded prizes in empty graves do not count as won. A single unfinished grave prevents any premature public recap.

**Discord overflow:** Each public recap is one embed under the safe message length limit. If every winner and prize will not fit in one embed, UglyBot posts automatically numbered continuation panels (PART 2 / N, PART 3 / N, etc.). No participants or rewards are dropped, even if one winner has an unusually long list. Every part has its own persistent ID and marker.

**ADMIN-ONLY winner download:** The public Choose Your Grave channel receives **ONLY the winner/prize summary panels**. It never contains a CSV attachment, download button, wallet address, or download-control panel. After the finale is fully complete, an authorized administrator runs `/fourgraves rewards` or opens `/fourgraves admin → HALLOWEEN REWARDS`. The bot responds with an **ephemeral** private admin-only panel containing the **DOWNLOAD WINNERS + WALLETS** button. Both opening this panel and clicking its button re-check the bot's existing server-administrator authorization; a regular member cannot use a guessed button ID. The actual downloadable CSV is returned **ephemerally to that administrator only**.

The admin CSV `Ugly_Halloween_Rewards.csv` contains one row per unique winning Discord account: **Discord User ID, Player, Verified Wallet(s), Prizes Won, Rewards, Graves**. Wallets come from the same current, verified holder `wallet_links` used by UglyBot's ordinary wallet-verification workflow, not an OpenSea buyer address or a detached historical wallet. A user with several verified linked wallets gets **all** of them separated by semicolons. Unknown, malformed, unverified or missing wallets are never invented; the CSV clearly marks `NOT VERIFIED — CHECK BEFORE DELIVERY` for manual review. Wallet records are fetched only at the moment an administrator clicks download, and no wallet address is logged in the audit.

**Older public CSV cleanup:** If a previous deployed version of the Four Graves bot already posted a public `Ugly_Halloween_Rewards.csv`, the worker checks the original selection channel for its verified bot message and unique footer, deletes **only that old public download message**, and clears the saved reference. Missing/mismatched messages cause a diagnostic, not deletion of another user's message. This does not retroactively revoke previously downloaded copies. Administrators who see an existing public CSV should delete it promptly rather than waiting for deployment.

**Recovery safety:** The additive summary-page tracking table and original event-level publication timestamp track the public reward panels. If UglyBot restarts between recap panels, it recovers their IDs or footer markers and continues without reposting or rerolling winners. Publication completes after all public summary panels (no public CSV is ever required). The private download requires a fresh administrator interaction and retrieves up-to-date verified wallet associations. All public errors remain isolated from private grave draws.

**Private TEST mode** posts only fake-ticket reward summaries in its isolated test channel, with a prominent TEST ONLY label and no public fake CSV. It never queries real user wallets or permits a test user to view a live rewards CSV.

## Fair random winner selection

- Each individual buried ticket is a chance; users with more tickets have proportionally greater chances to win when they are eligible.
- After a participant wins one prize, their winning ticket is removed from the active pool. Their remaining tickets are also held aside until everyone with a ticket in that grave wins once. The full frozen pool then resets for another pass.
- This ensures no participant wins twice until every eligible person has had a winning turn, even when there are more prizes than holders. Tickets never cross between graves.
- Draws use a cryptographically secure random index.
- Each winner is committed to PostgreSQL BEFORE announcing. If the bot restarts or Discord delivery fails, it retries the same winner using the saved ticket and unique message marker; it cannot reroll that prize.
- First-prize due time is recorded as the stored decision timestamp plus 2 minutes. The next prize is scheduled 15 seconds after the previous announcement is confirmed. Recovery polling happens roughly every 15 seconds, so announcements can be later but not closer together.
- Empty graves have no choice and no winners; their prizes are unawarded until an administrator resolves them.

## Administrator commands

- /fourgraves admin — ephemeral administrator event controls (setup, post, open, close, status, export, repair, countdown restart, and final private reward download once finished).
- /fourgraves configure — choose public panel channel, optional private grave category and artwork, customize names, confirm final prize pools.
- /fourgraves status — members see participation and decision progress (secret prizes remain hidden before choice); admins see award progress and errors.
- /fourgraves export — private ZIP with contestant CSVs, master allocations, ticket audit and a four_graves_prize_winners.csv once drawings have begun.
- /fourgraves rewards — **admin-only ephemeral download panel** for the completed winner list, including each winning user’s current verified linked wallet(s).

The existing countdown is **15 minutes after CLOSE** before TRICK/TREAT buttons activate. The 2-minute drawing delay happens **after** the first choice in each grave. To reveal on October 31, close 15 minutes before your intended team-decision time. RESTART COUNTDOWN is only possible before any recorded decision.

Per-grave mystery adjustments (not needed for the approved default pools) are supplied through config-json using a complete mysteryByGrave object with exactly these keys: barry_buckets, barbara_bowkins, cletus_von_wrinkles, scuba_steve_3000. Each value is an array of NFT objects containing name and positive integer quantity. Reconfirm with finalize-prizes:true. The config is frozen after CLOSE.

## Permissions, recovery, and private testing

- The production Discord bot requires Server Members Intent, Manage Channels and Manage Roles. Its own channel permission overwrite additionally permits Mention Everyone **in each private live grave channel**, only for the one recorded choice announcement.
- The channel remains private to eligible team roles and administrators. Bot-message edits suppress mentions; winners do NOT tag everyone.
- A first-decision transaction admits exactly one press per grave. Announcement persistence prevents accidental double-pings.
- Event/tickets remain durable across restarts. Setting FOUR_GRAVES_ENABLED=false stops the live worker without deleting ticket data or announcements.
- The optional /fourgravestest feature uses its own synthetic database source, test event key and distinct private roles/channels. It simulates the new choice/draw flow but NEVER sends @everyone in rehearsal. See docs/four-graves-private-test.md.

## Verification

Run test:trick-or-treat, test:four-graves, test:four-graves-auto-draw, test:four-graves-private and the two disposable PostgreSQL database suites. FOUR_GRAVES_TEST_DATABASE_URL must refer ONLY to a disposable database. GitHub CI provisions its own PostgreSQL 16 instance.

**No external game is started and no automated NFT or $CHARM transfer is made.**

## Diagnosing repeated worker errors

Each Four Graves worker polls about every 15 seconds. If Railway reports repeated worker failures for an event ID, **do not reset the event, ticket allocations or stored awards**. Event IDs identify rows in the Four Graves database; they do not identify individual graves or tickets.

The worker records the underlying error under its per-event operation key. Run **/fourgraves status** (live) and **/fourgravestest status** (private simulation); these statuses now display worker failures first to avoid Discord truncation. Logs now show a label **LIVE** or **TEST**, the event ID and the first 550 characters of the underlying error. Repeated identical lines are throttled to once per five minutes without slowing normal retries or draws.

Common fixes depend on the **actual** exception:

- Missing Manage Roles / Manage Channels or Discord code 50013: check the production bot's Discord permissions and role hierarchy. Verify its managed grave roles are below the bot's highest role.
- Missing/invalid public panel destination: update the configured grave-panel channel in Discord (do not delete the private grave channels). A broken public panel is now isolated, so private team-role reconciliation and winner draws can continue. The public-panel operation retains a diagnostic until corrected.
- Failed member fetch: check Discord's privileged Server Members Intent for the bot and whether the bot can access this guild. Do not fabricate member roles from incomplete data.
- Database/schema error: verify the additive Four Graves migration ran on the configured prizes database; do not drop or recreate the ticket or prize tables.
- Missing finalized per-grave prizes or frozen-snapshot integrity mismatch: stop the affected draw and inspect event configuration and frozen snapshot; **do not reroll or overwrite winners**. Previously closed legacy events require explicit review.

If an error persists after a deployment, provide the **single first line with the actual error code/message** from Railway or the top of the private admin status. Sensitive values are redacted by worker logging; never paste secrets into chat.

## Fix for Discord gateway opcode 8 rate limiting (October 22, 2026)

If Railway logs show **Request with opcode 8 was rate limited**, the cause is repeated requests for the full guild membership. Do **not** reset Four Graves, delete channels, clear tickets, or change prize results.

- The Four Graves polling worker no longer invokes the gateway full-member fetch method. It lists guild members using Discord's paginated **HTTP List Guild Members** endpoint (up to 1,000 per page), not gateway opcode 8.
- The LIVE and TEST workers share one in-process roster request and a 15-minute membership cache per Discord guild, and refresh after expiry. A failed listing triggers a minimum two-minute cooldown. Both workers continue reconciling grave panels, countdowns and saved prize announcements even if membership refreshing is temporarily unavailable.
- Before close, no guild-wide membership list is required for resource setup, so the worker skips it entirely.
- New member join/leave events update the shared cache immediately. Routine reconciliation then restores or removes any relevant private grave roles, without a full membership re-request.
- During CLOSE, current member names come from the same safe REST roster. If this listing is temporarily unavailable, the frozen snapshot preserves the existing allocation identities without blocking closure.
- The worker reuses cached guild roles, channels and messages rather than issuing bulk resource requests every 15 seconds.
- This change does not alter any earned ticket, allocation, prize pool, previously drawn winner, draw schedule, or event state.

**Rollout:** Merge the rate-limit hotfix PR manually, redeploy the existing UglyBot, and verify the old opcode-8 errors stop. Keep FOUR_GRAVES_ENABLED unchanged. If the private rehearsal is not in use, set FOUR_GRAVES_TEST_ENABLED=false temporarily to reduce unnecessary work. Discord requires Server Members Intent to list guild members.

**Verification:** GitHub's Four Graves check now runs test:four-graves-member-roster, which validates cache sharing, HTTP pagination, guild join/leave updates, blocked-request cooldown, and no gateway member fetch on OPEN/SETUP. Production Discord HTTP and permissions behavior still requires post-deployment confirmation.
