const assert = require('assert');
const Module = require('module');

const originalLoad = Module._load;
let mockFetch = async () => { throw new Error('network not used in logic tests'); };
Module._load = function trickOrTreatTestStubs(request, parent, isMain) {
  if (request === 'discord.js') {
    class Stub {
      setName() { return this; } setDescription() { return this; } addSubcommand(fn) { fn?.(new Stub()); return this; }
      addUserOption(fn) { fn?.(new Stub()); return this; } addIntegerOption(fn) { fn?.(new Stub()); return this; }
      addBooleanOption(fn) { fn?.(new Stub()); return this; } setRequired() { return this; } setMinValue() { return this; }
      setMaxValue() { return this; } setTitle() { return this; } setCustomId() { return this; } setLabel() { return this; }
      setEmoji() { return this; } setStyle() { return this; } addComponents() { return this; } addFields() { return this; }
      setFooter() { return this; } setPlaceholder() { return this; } toJSON() { return {}; }
    }
    return {
      SlashCommandBuilder: Stub, EmbedBuilder: Stub, ActionRowBuilder: Stub, ButtonBuilder: Stub,
      ModalBuilder: Stub, TextInputBuilder: Stub, AttachmentBuilder: Stub,
      ButtonStyle: { Success: 3, Primary: 1, Secondary: 2 },
      TextInputStyle: { Short: 1 }, PermissionFlagsBits: { Administrator: 8n },
    };
  }
  if (request === 'node-fetch') return (...args) => mockFetch(...args);
  if (request === 'ethers') {
    return { ethers: { id: (x) => 'topic:' + x, JsonRpcProvider: class {} } };
  }
  return originalLoad.call(this, request, parent, isMain);
};

const savedEnv = { ...process.env };
(async () => {
try {
  process.env.TRICK_OR_TREAT_ENABLED = 'true';
  process.env.TRICK_OR_TREAT_TIME_ZONE = 'America/Toronto';
  process.env.TRICK_OR_TREAT_START_AT = '2026-10-01T00:00:00-04:00';
  process.env.TRICK_OR_TREAT_END_AT = '2026-10-31T23:59:59-04:00';
  process.env.OPENSEA_API_KEY = 'test-key';

  const tot = require('../modules/trickOrTreat');

  assert.strictEqual(tot.eventDayKey(new Date('2026-10-01T03:59:59Z'), 'America/Toronto'), '2026-09-30');
  assert.strictEqual(tot.eventDayKey(new Date('2026-10-01T04:00:00Z'), 'America/Toronto'), '2026-10-01');
  assert.strictEqual(tot.eventDayKey(new Date('2026-10-31T12:00:00Z'), 'America/Toronto'), '2026-10-31');

  const contract = '0x8c9a02c0585200c4c65608df6b8def543d33792a';
  const parsed = tot.parseOpenSeaSquigUrl(`https://opensea.io/item/ethereum/${contract}/003157`);
  assert.deepStrictEqual(parsed, { chain: 'ethereum', contract, tokenId: '3157' });
  assert.strictEqual(tot.parseOpenSeaSquigUrl(`https://opensea.io/assets/ethereum/${contract}/1`).tokenId, '1');
  assert.strictEqual(tot.parseOpenSeaSquigUrl('https://evil.example/item/ethereum/' + contract + '/1'), null);
  assert.strictEqual(tot.parseOpenSeaSquigUrl('https://opensea.io/collection/squigs-reloaded'), null);
  assert.strictEqual(tot.parseOpenSeaSquigUrl('https://opensea.io/item/base/' + contract + '/1').chain, 'base');
  assert.strictEqual(tot.parseOpenSeaSquigUrl('https://opensea.io/item/ethereum/0x1111111111111111111111111111111111111111/1'), null);

  const walletA = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
  const walletB = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
  const listing = (tokenId, fields = {}) => ({ asset: { contract, identifier: String(tokenId) }, maker: walletA, ...fields });
  const payload = { listings: [
    listing(100), listing(101), listing(102),
    listing(103, { status: 'fulfilled' }),
    listing(104, { status: 'cancelled' }),
    listing(105, { expiration_date: '2000-01-01T00:00:00Z' }),
  ] };
  let apiCalls = 0;
  mockFetch = async () => { apiCalls++; return { ok: true, json: async () => payload }; };
  const activeA = await tot.getActiveSquigListings(walletA);
  assert.deepStrictEqual(activeA.map((x) => x.tokenId), ['100', '101', '102'], 'fulfilled, cancelled and expired OpenSea records are ignored');
  const oneWalletListings = new Map([[walletA, activeA]]);
  assert.deepStrictEqual(tot.getDisqualifyingListings(oneWalletListings, new Map([[walletA, ['100']]])).map((x) => x.tokenId), ['100'], 'an owned active listing disqualifies');
  assert.deepStrictEqual(tot.getDisqualifyingListings(oneWalletListings, new Map([[walletA, []]])), [], 'a sold token in cached OpenSea results is ignored against fresh ownership');
  assert.deepStrictEqual(
    tot.getDisqualifyingListings(oneWalletListings, new Map([[walletA, ['100', '101']]])).map((x) => x.tokenId),
    ['100', '101'],
    'stale listing #102 cannot disqualify tokens currently owned as #100 and #101'
  );
  assert.deepStrictEqual(await tot.getAllListingsForWallets([walletA], new Map([[walletA, []]])), [], 'listing cache is reconciled against current ownership on every check');
  assert.strictEqual(apiCalls, 1, 'active listing cache still avoids repeat API calls');

  const walletListings = new Map([
    [walletA, [{ tokenId: '2', maker: walletA }]],
    [walletB, []],
  ]);
  const multiWalletOwnership = new Map([[walletA, ['1']], [walletB, ['2']]]);
  assert.deepStrictEqual(tot.getDisqualifyingListings(walletListings, multiWalletOwnership), [], 'Wallet A stale listing #2 does not count because only Wallet B owns #2');
  walletListings.set(walletB, [{ tokenId: '2', maker: walletB }]);
  assert.deepStrictEqual(
    tot.getDisqualifyingListings(walletListings, multiWalletOwnership).map((x) => [x.wallet, x.tokenId]),
    [[walletB, '2']],
    'Wallet B active listing #2 disqualifies because Wallet B owns it'
  );

  const rows = [
    { discordId: 'a', treats: 31, tricks: 0, entries: 31 },
    { discordId: 'b', treats: 1, tricks: 4, entries: 5 },
    { discordId: 'c', treats: 0, tricks: 2, entries: 2 },
  ];
  const seed = '11'.repeat(32);
  const first = tot.drawUniqueWinners(rows, 3, seed);
  const second = tot.drawUniqueWinners(rows, 3, seed);
  assert.deepStrictEqual(first, second, 'draw should be reproducible from the audit seed');
  assert.strictEqual(new Set(first.map((x) => x.discordId)).size, first.length, 'winners must be unique');
  assert.strictEqual(first.length, 3);
  assert.strictEqual(tot.drawUniqueWinners(rows, 99, seed).length, 3);

  // Tricks are intentionally uncapped: draw weight is the literal entries value supplied.
  const uncapped = [{ discordId: 'whale', treats: 31, tricks: 50, entries: 81 }];
  assert.strictEqual(tot.drawUniqueWinners(uncapped, 1, seed)[0].entries, 81);

  console.log('Trick or Treat logic tests passed.');
} finally {
  process.env = savedEnv;
  Module._load = originalLoad;
}
})().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
