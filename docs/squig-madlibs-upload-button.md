# Squig Mad Libs: image upload button

## Member workflow

Choose **SHOW / History**, select an unpublished saved story, then press **Upload Image**. Discord opens a private form with one file picker. Attach one still PNG, JPEG or WebP and submit. The existing image validator, corrected PNG decoder, PostgreSQL staging and private preview are reused. Only **Publish** shares the image publicly. **Replace Image** opens the same form while keeping the previous staged image until a valid replacement is saved. Closing the form has no effect on the story or staged image.

`/madlib-upload image:<file> story_id:<optional>` is retained as a fallback. There is no new payment, allowance, generation or reward operation in either upload path. All current byte/dimension/file-type restrictions still apply. Public panels remain PLAY / SHOW and do not need reposting: reopen SHOW to get the new private controls.

## Owner rollout

Merge the reviewed pull request into `main` after CI passes. Confirm Railway deploys that resulting commit. Keep existing variables, credentials, Discord intents, dependencies and permissions unchanged. No schema migration, new command-registration step, or Codespaces script is required for this update.

Use an existing completed story to test Upload Image, cancelling the form, replacing a staged image, and explicit Publish. Re-upload the original ChatGPT PNG used for the decoder fix. Check both desktop and mobile Discord. Test selection changes while a form is open, an expired/closed form, and a non-eligible user. These need live acceptance; fake API tests do not prove the current Discord client UX or permissions.

## SDK compatibility without upgrading UglyBot

Discord documents File Upload (type 19) inside Label (type 18) in modal callbacks (type 9). This module sends that exact API JSON through the existing `showModal` method; it does not depend on newer builders or set message Components V2 flags.

Modern discord.js with `ModalSubmitFields.getUploadedFiles` uses native attachment resolution. The declared baseline 14.16.3 has no such parser and assumes every modal top-level component is an ActionRow. Simply sending a new modal with a recent builder would make that old parser throw *before* UglyBot receives the interaction. Builder availability therefore is not used as the capability check.

For that older SDK only, a feature-local raw-Gateway bridge intercepts exactly `INTERACTION_CREATE` / MODAL_SUBMIT packets whose custom ID begins `madlib:upload-submit:`. The SDK emits `raw` synchronously before constructing the interaction. The bridge validates and copies the one attachment plus guild/user/channel/application/interaction identity into a bounded map, then gives the old parser an empty components array for **this new upload modal only**. The normal, existing interaction dispatcher still handles the reply. No SDK monkey-patching, second interaction handler, public-message listener, DM collector or new intent is added. Existing answer modals and every unrelated packet are unchanged.

Bridge entries have 30-second validity (lazy expiry), a 128-entry cap, and one-use consumption. They contain no interaction token, image bytes, member records or credentials. A missing/expired entry produces a private instruction to reopen SHOW or use the slash-command fallback. The bridge can receive a form submitted after a restart without needing a remembered modal-opening object. Stop/reinitialization removes its raw listener and clears entries. It also safely normalizes a previously opened modal after feature disablement so old SDK parsing cannot crash; disabled handlers do no upload/schema/payment work.

## Correct story and private state

Button/modal IDs bind the owner, completed session, draft revision and a short fingerprint of the persisted draft timestamp. This fingerprint is a freshness check, not authentication. Current guild membership, channel access, holder-role eligibility, ownership, completed state and publication state are checked before opening and again before accepting a file. The file form cannot open after deferring: opening uses a bounded read-only preflight and the button's initial response. Submit is acknowledged privately before download/decode.

Selecting another story, reopening SHOW, cancelling/recreating a draft or replacing a preview invalidates the earlier form. After decoding, the existing staging transaction checks both draft revision and timestamp under its row lock, so concurrent/stale submissions cannot overwrite a newer selection. No lock spans a network download. Cancel/recreation remains guarded even when its integer revision resets. Already sent, published, deleted or uncertain publications cannot be uploaded again. History, saved prompts, payment records and rewards are not rewritten.

## Verification and rollback

`node scripts/testMadlibUploadModal.js` tests native payload serialization, actual SDK Gateway parsing, the legacy adapter, one-file resolution, private previews, replacement/cancel semantics, stale/cross-user/eligibility checks, limits and listener cleanup. The full `node scripts/runMadlibChecks.js` includes these tests plus the current PNG, logic, template, integration, disposable-PostgreSQL and legacy suites. The compatibility workflow runs both discord.js 14.16.3 and 14.24.2 with Node 20; it does not change package.json.

Revert this PR to remove the button workflow and retain `/madlib-upload`. No data deletion or schema rollback is required. Existing saved images/stories remain usable. Alternatively the existing `MADLIB_ENABLED=false` switch disables the feature without deleting history, subject to the normal project deployment process.

References: https://docs.discord.com/developers/components/reference#file-upload ; https://discord.js.org/docs/packages/discord.js/14.24.2/ModalSubmitFields:Class
