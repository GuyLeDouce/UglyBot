const assert = require('assert');
const {
  MAW_DISPOSITIONS,
  MAW_DIGESTION_STATUS,
  normalizeDisposition,
  formatMawDispositionLabel,
  mawDispositionInventoryStatus,
  mawDispositionDigestionStatus,
  isRegurgitatedAvailableInventory,
  parseBurnTransactionInput,
  parseAcceptedBurnAddresses,
  digestionStatusText,
} = require('../modules/mawDisposition');
const {
  getMawRewardQuote,
  resolveMawSessionRewardSnapshot,
} = require('../modules/mawRarity');

function assertThrowsCode(fn, code) {
  assert.throws(fn, (err) => err && err.code === code);
}

assert.strictEqual(normalizeDisposition('swallowed'), MAW_DISPOSITIONS.SWALLOWED);
assert.strictEqual(normalizeDisposition('regurgitated'), MAW_DISPOSITIONS.REGURGITATED);
assert.strictEqual(normalizeDisposition(null), MAW_DISPOSITIONS.REGURGITATED);
assert.strictEqual(formatMawDispositionLabel('swallowed'), 'Swallowed');
assert.strictEqual(formatMawDispositionLabel('regurgitated'), 'Regurgitated');

assert.strictEqual(mawDispositionInventoryStatus('regurgitated'), 'available');
assert.strictEqual(mawDispositionInventoryStatus('swallowed'), 'pending_burn');
assert.strictEqual(mawDispositionDigestionStatus('regurgitated', true), MAW_DIGESTION_STATUS.NOT_APPLICABLE);
assert.strictEqual(mawDispositionDigestionStatus('swallowed', false), MAW_DIGESTION_STATUS.PENDING_TRANSFER);
assert.strictEqual(mawDispositionDigestionStatus('swallowed', true), MAW_DIGESTION_STATUS.PENDING_BURN);

assert.strictEqual(isRegurgitatedAvailableInventory({ status: 'available' }), true, 'legacy available inventory remains compatible');
assert.strictEqual(isRegurgitatedAvailableInventory({ disposition: 'regurgitated', inventory_status: 'available', status: 'available' }), true);
assert.strictEqual(isRegurgitatedAvailableInventory({ disposition: 'regurgitated', inventory_status: 'reserved_for_claim', status: 'reserved_for_claim' }), false);
assert.strictEqual(isRegurgitatedAvailableInventory({ disposition: 'swallowed', inventory_status: 'pending_burn', status: 'pending_burn' }), false);
assert.strictEqual(isRegurgitatedAvailableInventory({ disposition: 'swallowed', inventory_status: 'digested', status: 'digested' }), false);

const quote = getMawRewardQuote('1');
const swallowedSnapshot = resolveMawSessionRewardSnapshot({
  payout_amount: quote.payoutCharm,
  ticket_count: quote.ticketCount,
  jackpot_contribution_charm: quote.jackpotContributionCharm,
  rarity_tier: quote.rarityKey,
}, {});
const regurgitatedSnapshot = resolveMawSessionRewardSnapshot({
  payout_amount: quote.payoutCharm,
  ticket_count: quote.ticketCount,
  jackpot_contribution_charm: quote.jackpotContributionCharm,
  rarity_tier: quote.rarityKey,
}, {});
assert.deepStrictEqual(
  [swallowedSnapshot.payoutCharm, swallowedSnapshot.ticketCount, swallowedSnapshot.jackpotContributionCharm],
  [regurgitatedSnapshot.payoutCharm, regurgitatedSnapshot.ticketCount, regurgitatedSnapshot.jackpotContributionCharm],
  'Swallowed and Regurgitated reward values are identical'
);

function simulateInbound(state, disposition, snapshot) {
  return {
    payoutCount: state.payoutCount + 1,
    tickets: state.tickets + snapshot.ticketCount,
    jackpot: state.jackpot + snapshot.jackpotContributionCharm,
    progress: state.progress + 1,
    inventoryStatus: mawDispositionInventoryStatus(disposition),
    digestionStatus: mawDispositionDigestionStatus(disposition, true),
  };
}
const baseState = { payoutCount: 0, tickets: 0, jackpot: 0, progress: 0 };
const swallowedInbound = simulateInbound(baseState, 'swallowed', swallowedSnapshot);
const regurgitatedInbound = simulateInbound(baseState, 'regurgitated', regurgitatedSnapshot);
assert.strictEqual(swallowedInbound.payoutCount, 1);
assert.strictEqual(swallowedInbound.tickets, regurgitatedInbound.tickets);
assert.strictEqual(swallowedInbound.jackpot, regurgitatedInbound.jackpot);
assert.strictEqual(swallowedInbound.progress, regurgitatedInbound.progress);
assert.strictEqual(swallowedInbound.inventoryStatus, 'pending_burn');
assert.strictEqual(regurgitatedInbound.inventoryStatus, 'available');

function simulateBurnCompletion(state) {
  return { ...state, inventoryStatus: 'digested', digestionStatus: 'digested' };
}
const burned = simulateBurnCompletion(swallowedInbound);
assert.strictEqual(burned.payoutCount, swallowedInbound.payoutCount, 'burn completion does not issue another payout');
assert.strictEqual(burned.tickets, swallowedInbound.tickets, 'burn completion does not issue more tickets');
assert.strictEqual(burned.jackpot, swallowedInbound.jackpot, 'burn completion does not add jackpot again');
assert.strictEqual(burned.progress, swallowedInbound.progress, 'burn completion does not advance progress again');

const rawHash = `0x${'A'.repeat(64)}`;
const parsedRaw = parseBurnTransactionInput(rawHash);
assert.strictEqual(parsedRaw.hash, rawHash.toLowerCase());
assert.strictEqual(parsedRaw.url, `https://etherscan.io/tx/${rawHash.toLowerCase()}`);

const parsedUrl = parseBurnTransactionInput(`https://etherscan.io/tx/${rawHash}?foo=bar`, 'https://etherscan.io');
assert.strictEqual(parsedUrl.hash, rawHash.toLowerCase());

assertThrowsCode(() => parseBurnTransactionInput(''), 'MAW_BURN_TX_EMPTY');
assertThrowsCode(() => parseBurnTransactionInput('0x1111111111111111111111111111111111111111'), 'MAW_BURN_TX_INVALID_HASH');
assertThrowsCode(() => parseBurnTransactionInput('https://etherscan.io/address/0x1111111111111111111111111111111111111111'), 'MAW_BURN_TX_NOT_TX_URL');
assertThrowsCode(() => parseBurnTransactionInput('https://etherscan.io/token/0x1111111111111111111111111111111111111111'), 'MAW_BURN_TX_NOT_TX_URL');
assertThrowsCode(() => parseBurnTransactionInput('https://etherscan.io/block/123'), 'MAW_BURN_TX_NOT_TX_URL');
assertThrowsCode(() => parseBurnTransactionInput(`https://opensea.io/assets/ethereum/0x${'1'.repeat(40)}/1`), 'MAW_BURN_TX_UNSUPPORTED_EXPLORER');

const accepted = parseAcceptedBurnAddresses('0x000000000000000000000000000000000000BEEF', (value) => {
  const text = String(value || '').trim();
  return /^0x[a-fA-F0-9]{40}$/.test(text) ? text.toLowerCase() : null;
});
assert.ok(accepted.includes('0x0000000000000000000000000000000000000000'));
assert.ok(accepted.includes('0x000000000000000000000000000000000000dead'));
assert.ok(accepted.includes('0x000000000000000000000000000000000000beef'));

assert.strictEqual(digestionStatusText('regurgitated', 'not_applicable'), 'Added to Maw Pool');
assert.strictEqual(digestionStatusText('swallowed', 'pending_burn'), 'Awaiting digestion');
assert.strictEqual(digestionStatusText('swallowed', 'digested'), 'Digested');

console.log('Maw disposition logic tests passed.');
