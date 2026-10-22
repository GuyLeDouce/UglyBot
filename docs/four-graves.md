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

## Finalized prize pools

Every grave has 1 DollzOnApe, 3 Squigs Reloaded, 1 Ugly Monster and 5 separate prizes of 5,000 $CHARM. In addition:

| Private grave | Additional Mystery NFTs | Total prizes |
|---|---|---:|
| Barry Buckets | 3 × Scribble Otter Yoink Club | 13 |
| Barbara Bowkins | 1 × Visions of the Void | 11 |
| Cletus Von Wrinkles | 2 × Doruzu (ApeChain) | 12 |
| Scuba Steve 3000 | 3 × Chill Bugs | 13 |

**49 prize awards:** 29 NFTs and 20 $CHARM awards (100,000 $CHARM total).

The default configuration contains these exact per-grave mystery prize lists and is finalized. Existing unclosed events with an old shared mystery list are upgraded on admin setup. Already-closed legacy events are NOT silently changed; they require administrator review.

## Fair random winner selection

- Each individual buried ticket is a chance; users with more tickets have proportionally greater chances to win when they are eligible.
- After a participant wins one prize, their winning ticket is removed from the active pool. Their remaining tickets are also held aside until everyone with a ticket in that grave wins once. The full frozen pool then resets for another pass.
- This ensures no participant wins twice until every eligible person has had a winning turn, even when there are more prizes than holders. Tickets never cross between graves.
- Draws use a cryptographically secure random index.
- Each winner is committed to PostgreSQL BEFORE announcing. If the bot restarts or Discord delivery fails, it retries the same winner using the saved ticket and unique message marker; it cannot reroll that prize.
- First-prize due time is recorded as the stored decision timestamp plus 2 minutes. The next prize is scheduled 15 seconds after the previous announcement is confirmed. Recovery polling happens roughly every 15 seconds, so announcements can be later but not closer together.
- Empty graves have no choice and no winners; their prizes are unawarded until an administrator resolves them.

## Administrator commands

- /fourgraves admin — admin event controls (setup, post, open, close, status, export, repair, and countdown restart).
- /fourgraves configure — choose public panel channel, optional private grave category and artwork, customize names, confirm final prize pools.
- /fourgraves status — members see participation and decision progress (secret prizes remain hidden before choice); admins see award progress and errors.
- /fourgraves export — private ZIP with contestant CSVs, master allocations, ticket audit and a four_graves_prize_winners.csv once drawings have begun.

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
