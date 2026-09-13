# Squig Mad Libs: human-world library v2

## Content and scope

Sixty complete authored scenes replace the active v1 pool for new plays. Squigs are unusual visitors in ordinary human settings: kitchens, cafes, grocery stores, desks, laundromats, parks, buses, bedrooms and social gatherings. Each scene has one readable comic moment, a matching classic story and image prompt, and an explicit editorial note about the intended physical setup.

Six scenes belong to each category: Morning / GM; Night / GN; Everyday errands; Workweek / office; Meme reactions; Web3 desk life; Food / coffee; Weekend / outdoors; Screens / posting; Social mischief. Web3 material is fictional visual comedy such as floor-watching, touching grass, community calls and chart reactions, not token promotion or financial advice.

GM/GN cards, reaction crops, comic panels, cozy night posts, mock hero shots, album-cover compositions and desk setups are composition and mood choices. They do not change the attached Squig's original 2D linework, eye count, skin, ears, proportions, accessories or recognizable identity. A costume answer changes clothing only.

## Existing questions and saved progress

The original nine question keys, labels and limits remain: emotion, object, food, animal, adjective, colour, plural, costume and shout. Each new scene uses six or seven of these keys, within the existing 6–10-question contract. Questions are ordered by their first occurrence in the shared frozen moment. Every submitted answer appears in both outputs and is substituted once as literal data.

The 60 new scene IDs start with `hw-` and use version 2. New plays choose from this new pool. Unfinished plays retain their previously saved scene and question order, even if it is a v1 scene. Completed stories and prompts are read from their stored strings without regeneration; SHOW never replaces them with new content. No database migration, payment, cooldown, reward, identity or upload-validation change is included.

A shout supplies narrative expression, not lettering to draw. Seven scenes make a narrow, explicit exception for only GM or GN in one specified place. Other scenes request no new lettering. Original clothing graphics remain. This avoids giving the image generator contradictory no-text and GM/GN instructions.

## Export instructions in every newly rendered prompt

The shared renderer appends one `EXPORT FOR UGLYBOT` block. It requests one still **1024 x 1024** square image, preferably an actual **PNG** file; valid JPG/JPEG and WebP are also acceptable. It requests a target of at most **4 MiB** by default, or 75% of a smaller configured upload ceiling. It prints the saved ceiling in bytes and MiB, normally **8388608 bytes / 8 MiB**, and tells the user to respect any lower current Discord or bot limit. Images must not exceed 4096 pixels on either side. No animation, GIF, SVG, AVIF, HEIC, PDF, collage, document or multi-page output is requested.

`MADLIB_MAX_IMAGE_BYTES` is copied into new template snapshots using the existing template JSON field; no column is added. Old snapshots without that metadata use the default ceiling with the lower-current-limit warning. Their original questions and story remain unchanged when they finish, with the export block added to the newly rendered prompt. Existing completed prompt strings are not retroactively edited.

These instructions cannot control a third-party encoder or guarantee an exact file size. Users must check the actual downloaded image's format, dimensions and size. Merely changing its filename extension is not conversion. The bot's existing validation remains authoritative and is not loosened by this content update. Export instructions are explicitly not text to draw in the image.

## Rollout and owner checks

No new Railway variable, API credential, dependency, slash command or schema is needed. Keep the working `MADLIB_ENABLED=true` and existing image/payment settings.

1. Push `feature/madlib-human-world` and create a pull request with base `main`. Review the changed files and available test results before merging.
2. After merging, confirm Railway deploys the new merge commit. Restarting an older deployment does not install this content.
3. Run `/madlib` to post a new PLAY / SHOW panel with the updated human-world wording. Existing panels continue to work, but their old message text is not silently edited.
4. Start a genuinely new play to sample the v2 pool. Resume an older unfinished session to verify its original questions remain, and open a previously completed story in SHOW to verify it is unchanged.
5. Confirm a new prompt contains one coherent scene, all answers, the reference-fidelity instructions and one export block. Generate an image externally, download the actual supported image file within the limits, and use `/madlib-upload` on that same saved story.

The normal free-play clock and paid-play rules still apply. A content rollout does not create free rerolls, issue refunds or replay failed payments. Do not delete saved sessions or payment records to test the new pool.

## Verification

The existing `testMadlibTemplates.js` now also tests v2 categories and IDs, the original question vocabulary, all export blocks, bounded GM/GN lettering, smaller configured ceilings, literal unusual answers and a real captured v1 session fixture. `testMadlibIntegration.js` checks panel wording and configured template metadata plus unchanged completed v1 history and completion of unfinished v1 sessions. Existing payment and upload tests remain unchanged.

Run `node scripts/testMadlibTemplates.js` and `node scripts/testMadlibIntegration.js` without bot startup. Set `MADLIB_REPORT_DIR` to produce complete sample pairs and editorial reviews. `node scripts/runMadlibChecks.js` remains the full existing syntax/feature/legacy runner. Database tests require a disposable local PostgreSQL database; missing database support is a skip, not a pass. Read the delivered verification report for checks actually executed and any environment-related gaps. Automated tests do not establish live Discord/DRIP behavior or external image-generation quality.

## Rollback

Revert this content commit through review to restore the prior active scene pool. Do not drop tables, reset balances or delete saved v2 sessions: each saved session has its own complete template. The existing Mad Libs feature flag can pause the whole feature if needed, while preserving history and financial obligations.


## Reviewing all 60 scenes

The complete scene library lives in `modules/data/madlibScenesA.js` and `madlibScenesB.js`. To generate a catalogue with complete example stories, full image prompts and editorial reviews directly from that code, run:

```bash
MADLIB_REPORT_DIR=/tmp/madlib-human-world-review node scripts/testMadlibTemplates.js
```

Read `/tmp/madlib-human-world-review/template-content-review.md` and `template-renders.json`. They are generated review artifacts, not files to add to the bot runtime.
