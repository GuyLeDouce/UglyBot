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
    const walletPenalty = 0;
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

function claimableForNft({ now, lastClaimedAt, rewardStartAt, unitValue, payoutAmount }) {
  const baseline = lastClaimedAt || rewardStartAt || now;
  const elapsedMs = Math.max(0, now.getTime() - baseline.getTime());
  return Math.max(0, Math.floor((elapsedMs / DAY_IN_MS) * unitValue * payoutAmount));
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
    unverifiedPenaltyAmount: 0,
    claimableAmount: 200,
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

  const now = new Date('2026-07-06T12:00:00.000Z');
  const rewardStartAt = new Date('2026-07-03T12:00:00.000Z');
  assert.strictEqual(claimableForNft({
    now,
    rewardStartAt,
    unitValue: 10,
    payoutAmount: 2,
  }), 60);

  const lastClaimedAt = new Date('2026-07-05T12:00:00.000Z');
  assert.strictEqual(claimableForNft({
    now,
    lastClaimedAt,
    rewardStartAt,
    unitValue: 10,
    payoutAmount: 2,
  }), 20);

  console.log('Reward logic harness passed.');
}

run();
