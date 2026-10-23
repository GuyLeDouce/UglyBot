# Private production-server rehearsal: The Four Graves

This is a separate, **disabled by default** test facility for the production UglyBot in its actual Discord server. It creates and operates REAL Discord test channels, roles, panels, countdowns and reveals while all tickets, allocations and exports are fictional.

It is NOT the production finale. Never use /fourgraves admin or /fourgraves configure to run this simulation.

## Before you merge

- The code is gated by FOUR_GRAVES_TEST_ENABLED. Unset or false leaves the simulation inactive, even when the branch is merged and automatically deployed.
- The existing FOUR_GRAVES_ENABLED, FOUR_GRAVES_EVENT_KEY, TRICK_OR_TREAT_EVENT_KEY, and all original claim handlers stay unchanged.
- This feature uses the same existing prizes PostgreSQL connection but a different event key, a separate synthetic ticket table, and distinctly marked test resources. It never queries the original October claim tables for test ticket balances. The production test does not transfer rewards.
- The TEST worker is not launched unless FOUR_GRAVES_TEST_ENABLED=true.
- Do not enable the real Four Graves event to run the rehearsal.

## Enabling (after merging, when Railway is available)

1. In the existing UglyBot Railway production service, set FOUR_GRAVES_TEST_ENABLED=true. You do NOT need another Discord app, bot, guild or PostgreSQL service.
2. In Discord Developer Portal for this production bot, enable the privileged Server Members Intent, if not already on. The test worker reads actual guild membership to verify access. The bot must also have Manage Channels, Manage Roles, View Channels, Send Messages, Embed Links, Read Message History and Attach Files; its highest role must outrank the new grave roles.
3. Redeploy the production service so the bot logs in with the required intent. Confirm normal /trickortreat claims still work. Verify the new /fourgravestest slash command is available to members with Manage Server permission.
4. Use the commands below on the **actual Discord server**.

A Discord user with the server-wide Administrator permission can bypass private-channel denies. Other participants will not be pinged or assigned real event roles, but Discord cannot conceal these channels from other full Administrators. The controls are restricted to the administrator who created the rehearsal.

## Rehearsal sequence

1. /fourgravestest setup
   Creates a private category named four-graves-test-fg[database event ID], a private test-four-graves-control channel, and (through the real FourGravesDiscord worker) four private test grave channels and four dedicated, non-mentionable [TEST] Grave roles. Only the bot and test owner receive explicit admin access. The automatically posted panel is clearly marked TEST ONLY.

2. /fourgravestest status
   Check that the control channel and all four grave channel IDs and role IDs exist and that Worker errors says none. This is an ephemeral response visible only to you. Use /fourgravestest repair to request another worker reconciliation if resources were delayed or had a recoverable error.

3. /fourgravestest seed
   Creates 20 fictional people with 5 tickets each (100 fictional tickets total), plus 12 simulated tickets belonging to your REAL Discord account. The fictional people do not exist as Discord members and never get actual Discord roles.

4. /fourgravestest open
   Opens the real Four Graves test panel and automatically buries 5 tickets for each fictional person, distributed evenly across the four graves. It also buries one of your fake tickets into each grave so your real Discord account becomes eligible to inspect all four test-team channels after close. This automatically allocates 104 tickets and leaves 8 of YOUR simulated tickets unallocated for trying the test grave selector, quantity modal, confirm and bury flow. An interrupted open command can be retried without duplicate allocations.

5. In the private control channel, click a grave, ENTER QUANTITY, then BURY TEST TICKETS. Watch the changing balances and /fourgravestest status. None of this touches the real event.

6. /fourgravestest close
   Freezes the simulated entries and immediately starts a real, persisted 15-minute countdown. The worker gives your real account the dedicated test-team roles and reveals the four team-only channels. Check role assignments and category visibility. Frozen CSVs have one row per fake ticket and can be retrieved with /fourgravestest export.

7. Either let the actual 15 minutes elapse, or run /fourgravestest advance to move ONLY the TEST deadline to the past. In each test grave click 👻 TRICK or 🍬 TREAT. Test decisions are first-click wins, and each team message progresses through the real reveal stages. Test labels explicitly say TEST; no real payout or game is launched.

8. /fourgravestest export
   Privately downloads the four simulated contestant lists, master allocation CSV and audit CSV, using the same safe export code as the live finale.

9. /fourgravestest reset confirm:true
   Removes ONLY the resources verified with exact TEST names / channel-topic markers and the sandbox event, allocations, audit records and synthetic ticket records. If a channel/role was renamed or an unexpected child exists, reset refuses destruction. Do not manually remove markers or rename test resources while testing; correct them before retrying reset.

When finished, set FOUR_GRAVES_TEST_ENABLED=false and redeploy. You can re-enable and set up the test again later.

## What is isolated from production

- Original trick_or_treat_treats and trick_or_treat_tricks tables are not read by the test path and are neither updated nor locked by its closing operation.
- A dedicated four_graves_test_tickets source table and the separate event key four-graves-private-test-v1 provide test-only tickets and event state. Legitimate event-key records are not selected by test commands.
- Grave roles are prefixed [TEST], grave channels prefixed test-grave-, and the control topic/category have unique test markers. No existing channel, original grave role, holder role, event ticket, wallet, prize or payout is touched by test operations.
- Test messages have allowedMentions disabled, the simulated panel is visibly labeled TEST, and the original admin-alert channel does not receive test recovery messages.
- Fake test participants cannot click buttons or be given Discord roles; you use your actual admin account, with extra fictional tickets, for those real Discord interactions.

## Validation

The GitHub Four Graves workflow checks JavaScript syntax and runs: test:trick-or-treat, test:four-graves, test:four-graves-private, test:four-graves-db and test:four-graves-private-db.

The database tests require FOUR_GRAVES_TEST_DATABASE_URL pointing to a DISPOSABLE PostgreSQL instance. They never use a production database fallback. Actual production Discord/API permission behavior can only be confirmed after you merge and run /fourgravestest setup in your server.

If you need an emergency off-switch, set FOUR_GRAVES_TEST_ENABLED=false and redeploy. That stops test commands and the test worker without touching any existing test data; use the guarded reset later when safe.

## Updated finale mechanics

The separate production test worker now rehearses the real grave-specific prize pools (13, 11, 13 and 13 prizes respectively), one choice per team, a two-minute initial draw delay and fifteen seconds between sequential winners. Test mode never sends @everyone, transfers prizes or reads actual October claims. Do not run production /fourgraves commands for rehearsal.

## Four grave roster rehearsal

The private TEST event uses the same spooky, spoiler-free selection panel text as the live finale. After the first synthetic ticket burial, UglyBot posts **four independent TEST grave rosters** below the private selection panel, in Cletus / Barry / Barbara / Steve order.

Each panel displays **one row per fictional participant and their pooled ticket count**, such as `Test Participant 01 — 5 tickets`. All five actual fake ticket entries remain available individually for simulated prize draws. New TEST burials update the relevant panel in place and never create duplicate boards; the other panels stay in place. Private View buttons list 20 participants per page if needed.

The full TEST rosters and their stored message IDs are separate from live event data. TEST mode does not tag anyone, access real October claims or expose unrevealed prize pools.
