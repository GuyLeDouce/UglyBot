# Recover an unmatched Bounty transfer

An unmatched warning means the scanner saw an NFT arrive but could not select one safe, active submission. Linked wallets alone are insufficient: the submission must also match the network and NFT and remain unexpired when scanned. Replaying the scanner does not recover an already recorded transfer because detection is deduplicated.

After deploying this change:

New **UNMATCHED BOUNTY TRANSFER** messages include a team-only **Retry Matching** button. This checks again for exactly one active, unexpired submission for the NFT and linked source wallet in the current server, then runs the same verified recovery. If nothing matches or matching is ambiguous, the transfer remains in manual review. Use the explicit recovery flow below for expired submissions. Existing Discord messages are not edited automatically.

Bounty team-review, unmatched-transfer, return, delivery and operational-summary messages are sent to `BOUNTY_TEAM_VOTE_CHANNEL_ID` first. If it is unset, inaccessible or sending fails, they fall back to `BOUNTY_REVIEW_CHANNEL_ID` (or the existing default review channel). Public voting/draw messages and the generic system-error logger retain their existing routes.

1. Get the donor's submission ID from their submission confirmation and copy the transaction hash from **View transaction** in the unmatched warning.
2. Run `/bountypool` as a bot admin or configured reviewer and click **Recover Transfer**.
3. Enter the submission ID and inbound transaction hash. An expired submission is supported.
4. The recovered NFT proceeds through normal team approval and community voting. No reward is paid by recovery itself.

If the original submission ID is unavailable, an operator can look it up using a read-only database query, filtering by guild, donor Discord ID, chain, contract and token ID. If no submission exists, the donor can submit the exact NFT link and give the team the new submission ID. **Do not send the NFT again.**

Recovery is deliberately limited to one matching ERC-721 transfer. It verifies the RPC chain, successful confirmed receipt, actual recipient, exact NFT, current Vault ownership and the source wallet's link to the submission donor. The detected transfer must still be unassigned and in manual review. The update locks the transfer and submission, resolves the unmatched record and records an audit event with the administrator and donor IDs. Existing Vault/review records block duplicate recovery. ERC-1155 transfers still require separate manual handling.

The historical warning message is retained as history; the database record is resolved and a new team-review panel is posted. If posting fails after the database commit, check review-channel access; the NFT remains in team review and must not be resubmitted or transferred again.

Run `npm run test:bounty` for the existing Bounty logic suite and the recovery regression tests. Recovery tests use stubbed RPC, Discord and database responses; deployment and live database verification are separate steps.
