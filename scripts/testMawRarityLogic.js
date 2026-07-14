const assert = require('assert');
const {
  MAW_RARITY_RULES,
  MAW_EXPECTED_LEGENDARY_COUNT,
  MAW_FIRST_NON_LEGENDARY_RANK,
  MAW_REWARD_RULES_VERSION,
  loadMawRankingIndex,
  calculateMawAverageRank,
  classifyMawRarity,
  getMawRewardQuote,
  formatMawAverageRank,
  validateMawRankingRows,
  parseMawRankingCsvContent,
  normalizeMawRankingHeader,
  resolveMawSessionRewardSnapshot,
  formatMawTicketRange,
  pluralizeMawTickets,
  createMawTicketPlan,
  clearMawRankingCache,
} = require('../modules/mawRarity');

function assertThrowsCode(fn, code) {
  assert.throws(fn, (err) => err && err.code === code);
}

const boundaryCases = [
  [31.5, 'legendary'],
  [32, 'epic'],
  [443.5, 'epic'],
  [444, 'rare'],
  [1110.5, 'rare'],
  [1111, 'uncommon'],
  [2276.5, 'uncommon'],
  [2277, 'common'],
];
for (const [averageRank, expectedKey] of boundaryCases) {
  assert.strictEqual(classifyMawRarity(averageRank).key, expectedKey);
}

const expectedRewards = {
  legendary: [800000, 20, 100000],
  epic: [187500, 10, 25000],
  rare: [67500, 5, 5000],
  uncommon: [22500, 2, 2000],
  common: [15000, 1, 1000],
};
for (const rule of MAW_RARITY_RULES) {
  assert.deepStrictEqual(
    [rule.payoutCharm, rule.ticketCount, rule.jackpotContributionCharm],
    expectedRewards[rule.key]
  );
}

assert.strictEqual(calculateMawAverageRank(500, 765), 632.5);
assert.strictEqual(formatMawAverageRank(632.5), '632.5');
assert.strictEqual(formatMawAverageRank(444), '444');
assert.strictEqual(normalizeMawRankingHeader('\uFEFF Overall Rank '), 'overallrank');

const quotedCsv = '\uFEFF" Token ID ","Name","Legend","Total UglyPoints","Overall Rank","Collection Rank"\n"001","Squig, One","Legend One","1200","31","32"\n"2","Two","","900","444","445"\n';
const parsedQuoted = parseMawRankingCsvContent(quotedCsv, { expectedTokenCount: 2 });
assert.strictEqual(parsedQuoted.rows.length, 2);
assert.strictEqual(parsedQuoted.tokenMap.get('1').averageRank, 1);
assert.strictEqual(parsedQuoted.tokenMap.get('1').rarityKey, 'legendary');
assert.strictEqual(parsedQuoted.tokenMap.get('1').totalUglyPoints, 1200);
assert.strictEqual(parsedQuoted.tokenMap.get('2').averageRank, MAW_FIRST_NON_LEGENDARY_RANK);
assert.strictEqual(parsedQuoted.tokenMap.get('2').rarityKey, 'epic');
assert.strictEqual(parsedQuoted.rankingSourceHash, parseMawRankingCsvContent(quotedCsv, { expectedTokenCount: 2 }).rankingSourceHash);

assertThrowsCode(() => validateMawRankingRows([
  { 'Token ID': '1', 'Total UglyPoints': '1' },
  { 'Token ID': '1', 'Total UglyPoints': '2' },
], { expectedTokenCount: 2 }), 'MAW_RANKING_DUPLICATE_TOKEN');

assertThrowsCode(() => validateMawRankingRows([
  { 'Token ID': '1', 'Total UglyPoints': '' },
], { expectedTokenCount: 1 }), 'MAW_RANKING_MISSING_SCORE');

assertThrowsCode(() => validateMawRankingRows([
  { 'Token ID': '1', 'Total UglyPoints': 'abc' },
], { expectedTokenCount: 1 }), 'MAW_RANKING_INVALID_SCORE');

assertThrowsCode(() => validateMawRankingRows([
  { 'Token ID': '1', 'Total UglyPoints': '1' },
  { 'Token ID': '3', 'Total UglyPoints': '3' },
], { expectedTokenCount: 3 }), 'MAW_RANKING_MISSING_TOKEN');

const tiedCsv = '"Token ID","Total UglyPoints","Legend"\n"1","100",""\n"2","100",""\n"3","200",""\n';
const parsedTied = parseMawRankingCsvContent(tiedCsv, { expectedTokenCount: 3 });
assert.deepStrictEqual(
  ['3', '1', '2'].map((tokenId) => parsedTied.tokenMap.get(tokenId).averageRank),
  [32, 33, 34]
);

clearMawRankingCache();
const indexA = loadMawRankingIndex({ forceReload: true });
const indexB = loadMawRankingIndex({ forceReload: true });
assert.strictEqual(indexA.rows.length, 4444);
assert.strictEqual(indexA.tokenMap.size, 4444);
for (let tokenId = 1; tokenId <= 4444; tokenId += 1) {
  assert.ok(indexA.tokenMap.has(String(tokenId)), `missing token ${tokenId}`);
}
assert.strictEqual(indexA.rankingSourceHash, indexB.rankingSourceHash);
assert.strictEqual(indexA.tierDistribution.legendary, MAW_EXPECTED_LEGENDARY_COUNT);
assert.strictEqual(indexA.tierDistribution.epic, 412);
assert.strictEqual(indexA.tierDistribution.rare, 667);
assert.strictEqual(indexA.tierDistribution.uncommon, 1166);
assert.strictEqual(indexA.tierDistribution.common, 2168);

const quote1 = getMawRewardQuote('001', { index: indexA });
assert.strictEqual(quote1.tokenId, '1');
assert.strictEqual(quote1.overallRank, null);
assert.strictEqual(quote1.collectionRank, null);
assert.strictEqual(quote1.sourceOverallRank, 303);
assert.strictEqual(quote1.sourceCollectionRank, 962);
assert.strictEqual(quote1.totalUglyPoints, 805);
assert.strictEqual(quote1.averageRank, 303);
assert.strictEqual(quote1.rarityKey, 'epic');
assert.strictEqual(quote1.payoutCharm, 187500);
assert.strictEqual(quote1.ticketCount, 10);
assert.strictEqual(quote1.jackpotContributionCharm, 25000);
assert.strictEqual(quote1.rewardRulesVersion, MAW_REWARD_RULES_VERSION);

const legendaryQuote = getMawRewardQuote('69', { index: indexA });
assert.strictEqual(legendaryQuote.averageRank, 1);
assert.strictEqual(legendaryQuote.rarityKey, 'legendary');
assert.strictEqual(legendaryQuote.isLegendary, true);

const tieA = getMawRewardQuote('549', { index: indexA });
const tieB = getMawRewardQuote('678', { index: indexA });
assert.strictEqual(tieA.totalUglyPoints, tieB.totalUglyPoints);
assert.strictEqual(tieA.averageRank, 34);
assert.strictEqual(tieB.averageRank, 35);

assert.strictEqual(formatMawTicketRange(41, 41, 1), '#41');
assert.strictEqual(formatMawTicketRange(41, 45, 5), '#41–#45 (5)');
assert.strictEqual(pluralizeMawTickets(1), '1 ticket');
assert.strictEqual(pluralizeMawTickets(5), '5 tickets');

const legacy = resolveMawSessionRewardSnapshot(
  { payout_amount: null, ticket_count: null, jackpot_contribution_charm: null, rarity_tier: null },
  { return_reward_charm: 12500 }
);
assert.deepStrictEqual(
  [legacy.payoutCharm, legacy.ticketCount, legacy.jackpotContributionCharm, legacy.payoutContext, legacy.isLegacyFlat],
  [12500, 1, 0, 'maw_return_reward', true]
);

const snapshotted = { ...quote1 };
MAW_RARITY_RULES[1].payoutCharm = 1;
assert.strictEqual(snapshotted.payoutCharm, 187500);
assert.strictEqual(MAW_RARITY_RULES[1].payoutCharm, 187500);

const plan = createMawTicketPlan(41, 10);
assert.deepStrictEqual(plan.map((row) => row.ticketNumber), [41, 42, 43, 44, 45, 46, 47, 48, 49, 50]);
assert.deepStrictEqual(plan.map((row) => row.ticketSlot), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

function simulateReceiptOnce(state, session) {
  if (session.status !== 'awaiting_transfer') return state;
  const snapshot = resolveMawSessionRewardSnapshot(session, { return_reward_charm: 12500 });
  const first = state.nextTicketNumber;
  const tickets = createMawTicketPlan(first, snapshot.ticketCount);
  session.status = 'received';
  state.nextTicketNumber += snapshot.ticketCount;
  state.ticketNumbers.push(...tickets.map((row) => row.ticketNumber));
  state.jackpotContributed += snapshot.jackpotContributionCharm;
  return state;
}
const state = { nextTicketNumber: 100, ticketNumbers: [], jackpotContributed: 0 };
const session = { status: 'awaiting_transfer', payout_amount: 187500, ticket_count: 10, jackpot_contribution_charm: 25000, rarity_tier: 'epic' };
simulateReceiptOnce(state, session);
simulateReceiptOnce(state, session);
assert.deepStrictEqual(state.ticketNumbers, [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]);
assert.strictEqual(state.jackpotContributed, 25000);

const sorted = [
  { tokenId: '5', quote: { averageRank: 200 } },
  { tokenId: '4', quote: { averageRank: 10 } },
  { tokenId: '9', quote: { averageRank: 200 } },
].sort((a, b) => {
  const rankDiff = b.quote.averageRank - a.quote.averageRank;
  if (rankDiff) return rankDiff;
  return Number(b.tokenId) - Number(a.tokenId);
});
assert.deepStrictEqual(sorted.map((row) => row.tokenId), ['9', '5', '4']);

console.log('Maw rarity logic tests passed.');
