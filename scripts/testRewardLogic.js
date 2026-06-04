const assert = require('assert');

const DAY_IN_MS = 24 * 60 * 60 * 1000;

function calculatePassiveClaimTotals(nftEntries, payoutAmount) {
  let totalUnits = 0;
  let baseAmount = 0;
  let unverifiedPenaltyAmount = 0;
  let rawClaimableAmount = 0;

  for (const entry of (Array.isArray(nftEntries) ? nftEntries : [])) {
    const elapsedMs = Math.max(0, Number(entry.elapsedMs || 0));
    const unitValue = Number(entry.unitValue || 0);
    const fullAmount = (elapsedMs / DAY_IN_MS) * unitValue * Number(payoutAmount || 0);
    const walletPenalty = entry.verified ? 0 : fullAmount * 0.5;
    const claimableForNft = fullAmount - walletPenalty;
    entry.claimableAmount = claimableForNft;
    totalUnits += unitValue;
    baseAmount += fullAmount;
    unverifiedPenaltyAmount += walletPenalty;
    rawClaimableAmount += claimableForNft;
  }

  return {
    totalUnits,
    baseAmount: Math.max(0, Math.floor(baseAmount)),
    unverifiedPenaltyAmount: Math.max(0, Math.floor(unverifiedPenaltyAmount)),
    claimableAmount: Math.max(0, Math.floor(rawClaimableAmount)),
  };
}

function run() {
  const multiCollectionPerUp = calculatePassiveClaimTotals([
    { collection: 'Squigs', elapsedMs: DAY_IN_MS, unitValue: 100, verified: true },
    { collection: 'Ugly Monsters', elapsedMs: DAY_IN_MS, unitValue: 50, verified: true },
  ], 2);
  assert.deepStrictEqual(multiCollectionPerUp, {
    totalUnits: 150,
    baseAmount: 300,
    unverifiedPenaltyAmount: 0,
    claimableAmount: 300,
  });

  const mixedVerification = calculatePassiveClaimTotals([
    { collection: 'Squigs', elapsedMs: DAY_IN_MS, unitValue: 100, verified: true },
    { collection: 'Charm of the Ugly', elapsedMs: DAY_IN_MS, unitValue: 100, verified: false },
  ], 1);
  assert.deepStrictEqual(mixedVerification, {
    totalUnits: 200,
    baseAmount: 200,
    unverifiedPenaltyAmount: 50,
    claimableAmount: 150,
  });

  const halfDayPerNft = calculatePassiveClaimTotals([
    { collection: 'Squigs', elapsedMs: DAY_IN_MS / 2, unitValue: 1, verified: true },
    { collection: 'Ugly Monsters', elapsedMs: DAY_IN_MS / 2, unitValue: 1, verified: true },
  ], 10);
  assert.deepStrictEqual(halfDayPerNft, {
    totalUnits: 2,
    baseAmount: 10,
    unverifiedPenaltyAmount: 0,
    claimableAmount: 10,
  });

  const floorFractionalClaim = calculatePassiveClaimTotals([
    { collection: 'Squigs', elapsedMs: DAY_IN_MS / 3, unitValue: 10, verified: true },
  ], 1);
  assert.deepStrictEqual(floorFractionalClaim, {
    totalUnits: 10,
    baseAmount: 3,
    unverifiedPenaltyAmount: 0,
    claimableAmount: 3,
  });

  console.log('Reward logic harness passed.');
}

run();
