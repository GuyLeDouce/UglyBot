# Squig Mad Libs — simpler menu and public creation posts

## Display-only change

The public creation message now says:

> **Author of this ugly creation:** [the saved author display name]
>
> React below if you think it's **UGLY**! 💜

The story title, full classic story, uploaded image, and stable Mad Lib reference stay in the embed. The author is credited once in the message text; display names and story formatting are escaped and all mentions remain disabled. Reward rules, eligible/paid/pending counters, and suspension accounting no longer appear in public posts. The image prompt and raw answers are not exposed.

The public PLAY / SHOW panel now reads:

> Mad Libs is a fill-in-the-blanks word game: you supply the words before seeing the story. Our Squig version also gives you a matching image prompt for some human-world mischief.
>
> **1. PLAY** — Answer a few questions to reveal your story and image prompt.
> **2. Make your image** — Copy the prompt into your image generator and attach your Squig as the reference.
> **3. SHOW** — Choose your saved story, press **Upload Image**, and check your private preview.
>
> **Hidden until you decide to Publish.** Your answers, story, prompt and image preview stay private in Discord. Only **Publish** shares your finished story and image with the channel.
>
> One free play every 24 hours. Extra plays cost 1,000 $CHARM.

The last line uses the existing configured cooldown and price, not hard-coded defaults. This privacy description refers to Discord visibility: the bot still persists records for the existing functionality, and users use their own external image generator. Publishing shares only the finished story, author credit and image, not the private prompt or raw answer record.

## What does not change

Reaction eligibility, amount, accounting, moderation controls, payment/reward workers, deduplication, and the seeded configured emoji remain unchanged. Detailed reward rules remain in the existing private preview and owner documentation; status/inspect still expose records to authorized admins. Upload Image, Replace Image, Publish, private replies, saved stories, prompts, template versions, access roles and all prices are unchanged. No new environment variables, dependency changes, database schema or migrations, or index.js wiring.

## Rollout

1. Review the PR and passing checks, merge, and confirm Railway deploys the resulting merge commit. Do not change the working variables.
2. Run `/madlib` once in the desired channel to post the simpler main menu. Already-posted menus keep their old text; old PLAY / SHOW buttons still work. An admin may remove the old menu manually after checking the replacement.
3. New publications use the short author/reaction message immediately. Existing published creations receive it when the existing reconciliation worker next performs a display refresh. That path keeps its existing per-post one-minute minimum and up-to-one-hour clean-display throttle; backlog or Discord errors may take longer. There is no forced bulk edit, repost, lost reaction or new reward run.
4. To test, select an existing unpublished story in SHOW, use Upload Image and check the private preview. Only press Publish when ready to share the story and image. No new play or paid generation is required for this test. Confirm the published message has no reward paragraph or accounting field, and that normal reactions still work.

Rollback is a revert of this display-only commit. Do not delete publications, sessions, accounting rows or old posts to refresh the wording.

## Automated checks

Added integration coverage for the simple menu, configured price/cooldown, Publish-only privacy, escaped attribution, full story/image/marker/nonce retention, removal of accounting on existing message refresh, no repost/reattach or financial writes, and unchanged refresh throttles. Existing reward/payment, attachment/PNG, upload-modal, saved-session and PostgreSQL tests remain in place. No automated test posts to production Discord or transfers live $CHARM.
