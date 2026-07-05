const assert = require('assert');
const Module = require('module');

const originalLoad = Module._load;
Module._load = function loadWithDiscordStub(request, parent, isMain) {
  if (request === 'discord.js') {
    class DiscordStub {}
    return {
      SlashCommandBuilder: DiscordStub,
      EmbedBuilder: DiscordStub,
      ActionRowBuilder: DiscordStub,
      ButtonBuilder: DiscordStub,
      StringSelectMenuBuilder: DiscordStub,
      ModalBuilder: DiscordStub,
      TextInputBuilder: DiscordStub,
      ButtonStyle: { Success: 3, Secondary: 2 },
      TextInputStyle: { Short: 1, Paragraph: 2 },
      ChannelType: { PrivateThread: 12 },
      PermissionFlagsBits: { Administrator: 8n, ManageThreads: 17179869184n },
    };
  }
  return originalLoad.call(this, request, parent, isMain);
};

const {
  MARKETPLACE_MONTHLY_CAPS,
  getMarketplaceMonthKey,
  getMarketplaceCap,
  getMarketplaceStockState,
  isHttpUrl,
  isDirectImageUrl,
} = require('../modules/marketplaceCommand');

assert.deepStrictEqual(MARKETPLACE_MONTHLY_CAPS, {
  charm: 2,
  common: 4,
  uncommon: 4,
  monster: 2,
  rare: 2,
  epic: 1,
  custom: 1,
});

assert.strictEqual(getMarketplaceCap('charm'), 2);
assert.strictEqual(getMarketplaceCap('custom'), 1);

assert.strictEqual(
  getMarketplaceMonthKey(new Date('2026-07-01T03:59:59.000Z'), 'America/Toronto'),
  '2026-06'
);
assert.strictEqual(
  getMarketplaceMonthKey(new Date('2026-07-01T04:00:00.000Z'), 'America/Toronto'),
  '2026-07'
);

assert.deepStrictEqual(
  getMarketplaceStockState('common', 3),
  { itemKey: 'common', cap: 4, sold: 3, remaining: 1, soldOut: false }
);
assert.deepStrictEqual(
  getMarketplaceStockState('common', 4),
  { itemKey: 'common', cap: 4, sold: 4, remaining: 0, soldOut: true }
);
assert.deepStrictEqual(
  getMarketplaceStockState('epic', 99),
  { itemKey: 'epic', cap: 1, sold: 99, remaining: 0, soldOut: true }
);

assert.strictEqual(isHttpUrl('https://opensea.io/assets/example'), true);
assert.strictEqual(isHttpUrl('http://example.com/reward'), true);
assert.strictEqual(isHttpUrl('ftp://example.com/reward'), false);
assert.strictEqual(isHttpUrl('not-a-url'), false);

assert.strictEqual(isDirectImageUrl('https://cdn.example.com/reward.png'), true);
assert.strictEqual(isDirectImageUrl('https://cdn.example.com/reward.JPG?width=800'), true);
assert.strictEqual(isDirectImageUrl('https://opensea.io/assets/example'), false);
assert.strictEqual(isDirectImageUrl('not-a-url'), false);

console.log('Marketplace logic tests passed.');
