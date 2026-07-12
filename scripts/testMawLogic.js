const assert = require('assert');
const Module = require('module');

const originalLoad = Module._load;
Module._load = function loadWithStubs(request, parent, isMain) {
  if (request === 'discord.js') {
    class ChainableStub {
      setName() { return this; }
      setDescription() { return this; }
      setRequired() { return this; }
      setMinValue() { return this; }
      addSubcommand(fn) { if (fn) fn(new ChainableStub()); return this; }
      addIntegerOption(fn) { if (fn) fn(new ChainableStub()); return this; }
      addChannelOption(fn) { if (fn) fn(new ChainableStub()); return this; }
      addUserOption(fn) { if (fn) fn(new ChainableStub()); return this; }
      addStringOption(fn) { if (fn) fn(new ChainableStub()); return this; }
      addOptions() { return this; }
      setCustomId() { return this; }
      setPlaceholder() { return this; }
      setLabel() { return this; }
      setStyle() { return this; }
      setDisabled() { return this; }
      setTitle() { return this; }
      setColor() { return this; }
      setDescription() { return this; }
      addFields() { return this; }
      addComponents() { return this; }
      setImage() { return this; }
      toJSON() { return {}; }
    }
    return {
      SlashCommandBuilder: ChainableStub,
      EmbedBuilder: ChainableStub,
      AttachmentBuilder: ChainableStub,
      ActionRowBuilder: ChainableStub,
      ButtonBuilder: ChainableStub,
      StringSelectMenuBuilder: ChainableStub,
      ModalBuilder: ChainableStub,
      TextInputBuilder: ChainableStub,
      ButtonStyle: { Success: 3, Secondary: 2, Danger: 4 },
      TextInputStyle: { Short: 1, Paragraph: 2 },
      ChannelType: { PrivateThread: 12 },
      PermissionFlagsBits: { Administrator: 8n, ManageGuild: 32n, ManageThreads: 17179869184n },
    };
  }
  if (request === 'ethers') {
    return { ethers: { JsonRpcProvider: class JsonRpcProvider {} } };
  }
  return originalLoad.call(this, request, parent, isMain);
};

const {
  calculateMawOpenSlots,
  filterEligiblePrizeSquigsForUser,
  normalizeAddress,
  getMawConfig,
  sortMawSquigsForDisplay,
  mawSquigPageCount,
  mawSquigPageItems,
} = require('../modules/mawEvent');

assert.strictEqual(calculateMawOpenSlots({ goalCount: 40, receivedCount: 18, activeTransferWindows: 3 }), 19);
assert.strictEqual(calculateMawOpenSlots({ goalCount: 40, receivedCount: 40, activeTransferWindows: 0 }), 0);
assert.strictEqual(calculateMawOpenSlots({ goalCount: 40, receivedCount: 38, activeTransferWindows: 2 }), 0);
assert.strictEqual(calculateMawOpenSlots({ goalCount: 40, receivedCount: 38, activeTransferWindows: 0 }), 2);

assert.strictEqual(
  normalizeAddress('0x8C9A02C0585200C4C65608DF6B8DEF543D33792A'),
  '0x8c9a02c0585200c4c65608df6b8def543d33792a'
);
assert.strictEqual(normalizeAddress('not-a-wallet'), null);

const savedEnv = {
  MAW_GOAL_COUNT: process.env.MAW_GOAL_COUNT,
  MAW_RETURN_REWARD_CHARM: process.env.MAW_RETURN_REWARD_CHARM,
  MAW_JACKPOT_BASE_CHARM: process.env.MAW_JACKPOT_BASE_CHARM,
  MAW_JACKPOT_CHARM: process.env.MAW_JACKPOT_CHARM,
  MAW_SESSION_TTL_MINUTES: process.env.MAW_SESSION_TTL_MINUTES,
  MAW_PRIZE_CASHOUT_CHARM: process.env.MAW_PRIZE_CASHOUT_CHARM,
  MAW_REROLL_COST_CHARM: process.env.MAW_REROLL_COST_CHARM,
  MAW_MAX_REROLLS: process.env.MAW_MAX_REROLLS,
};
for (const key of Object.keys(savedEnv)) delete process.env[key];
const defaults = getMawConfig();
assert.strictEqual(defaults.goalCount, 20);
assert.strictEqual(defaults.returnRewardCharm, 12500);
assert.strictEqual(defaults.jackpotBaseCharm, 0);
assert.strictEqual(defaults.jackpotCharm, 35000);
assert.strictEqual(defaults.sessionTtlMinutes, 20);
assert.strictEqual(defaults.prizeCashoutCharm, 8000);
assert.strictEqual(defaults.rerollCostCharm, 4000);
assert.strictEqual(defaults.maxRerolls, 3);
for (const [key, value] of Object.entries(savedEnv)) {
  if (value == null) delete process.env[key];
  else process.env[key] = value;
}

const targetUser = 'user-1';
const targetWallets = [
  '0x1111111111111111111111111111111111111111',
  '0x2222222222222222222222222222222222222222',
];
const poolRows = [
  { id: '1', status: 'available', token_id: '101', original_sender_discord_id: 'user-2', original_sender_wallet: '0x3333333333333333333333333333333333333333' },
  { id: '2', status: 'available', token_id: '102', original_sender_discord_id: targetUser, original_sender_wallet: '0x4444444444444444444444444444444444444444' },
  { id: '3', status: 'available', token_id: '103', original_sender_discord_id: 'user-3', original_sender_wallet: '0x1111111111111111111111111111111111111111' },
  { id: '4', status: 'reserved_for_claim', token_id: '104', original_sender_discord_id: 'user-4', original_sender_wallet: '0x5555555555555555555555555555555555555555' },
  { id: '5', status: 'available', token_id: '105', original_sender_discord_id: 'user-5', original_sender_wallet: '0x6666666666666666666666666666666666666666' },
];

assert.deepStrictEqual(
  filterEligiblePrizeSquigsForUser(poolRows, targetUser, targetWallets).map((row) => row.id),
  ['1', '5']
);
assert.deepStrictEqual(
  filterEligiblePrizeSquigsForUser(poolRows, targetUser, targetWallets, ['1']).map((row) => row.id),
  ['5']
);
assert.deepStrictEqual(
  filterEligiblePrizeSquigsForUser(poolRows, targetUser, targetWallets, ['1', '5']).map((row) => row.id),
  []
);

const rerollPool = [
  { id: '10', status: 'available', token_id: '201', original_sender_discord_id: 'other', original_sender_wallet: '0x7777777777777777777777777777777777777777' },
  { id: '11', status: 'available', token_id: '202', original_sender_discord_id: targetUser, original_sender_wallet: '0x8888888888888888888888888888888888888888' },
  { id: '12', status: 'available', token_id: '203', original_sender_discord_id: 'other', original_sender_wallet: targetWallets[1] },
];
assert.deepStrictEqual(
  filterEligiblePrizeSquigsForUser(rerollPool, targetUser, targetWallets, ['10']).map((row) => row.id),
  []
);
assert.deepStrictEqual(
  filterEligiblePrizeSquigsForUser(rerollPool, targetUser, targetWallets).map((row) => row.id),
  ['10']
);

assert.deepStrictEqual(
  sortMawSquigsForDisplay([
    { tokenId: '7', quote: { averageRank: 10 } },
    { tokenId: '9', quote: { averageRank: 20 } },
    { tokenId: '5', quote: { averageRank: 20 } },
  ]).map((row) => row.tokenId),
  ['9', '5', '7']
);

assert.deepStrictEqual(
  sortMawSquigsForDisplay([
    { tokenId: '1' },
    { tokenId: '26' },
    { tokenId: '25' },
    { tokenId: '4444' },
    { tokenId: '100' },
  ]).map((row) => row.tokenId),
  ['4444', '100', '26', '25', '1']
);

const allSquigs = Array.from({ length: 4444 }, (_, i) => ({ tokenId: String(i + 1) }));
const sortedSquigs = sortMawSquigsForDisplay(allSquigs);
assert.strictEqual(sortedSquigs[0].tokenId, '4444');
assert.strictEqual(sortedSquigs[sortedSquigs.length - 1].tokenId, '1');
assert.strictEqual(mawSquigPageCount(sortedSquigs), 178);
assert.strictEqual(mawSquigPageItems(sortedSquigs, 0)[0].tokenId, '4444');
assert.strictEqual(mawSquigPageItems(sortedSquigs, 0).length, 25);
assert.strictEqual(mawSquigPageItems(sortedSquigs, 177).at(-1).tokenId, '1');

console.log('Maw logic tests passed.');
