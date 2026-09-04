const assert = require('assert');
const Module = require('module');

const originalLoad = Module._load;
Module._load = function loadWithStubs(request, parent, isMain) {
  if (request === 'discord.js') {
    class ChainableStub {
      static from() { return new ChainableStub(); }
      setName() { return this; } setDescription() { return this; } setDefaultMemberPermissions() { return this; }
      setCustomId() { return this; } setTitle() { return this; } setLabel() { return this; } setMaxLength() { return this; }
      setRequired() { return this; } setStyle() { return this; } addComponents() { return this; } addFields() { return this; }
      setColor() { return this; } setFooter() { return this; } setImage() { return this; } setThumbnail() { return this; }
      toJSON() { return {}; }
    }
    return { SlashCommandBuilder: ChainableStub, EmbedBuilder: ChainableStub, ActionRowBuilder: ChainableStub,
      ButtonBuilder: ChainableStub, ModalBuilder: ChainableStub, TextInputBuilder: ChainableStub,
      ButtonStyle: { Primary: 1, Secondary: 2, Success: 3, Danger: 4 }, TextInputStyle: { Short: 1, Paragraph: 2 },
      PermissionFlagsBits: { ManageGuild: 32n } };
  }
  if (request === 'ethers') {
    return { ethers: { id: value => `topic:${value}`, randomBytes: () => Buffer.alloc(32),
      Interface: class Interface {}, JsonRpcProvider: class JsonRpcProvider {}, Contract: class Contract {} } };
  }
  return originalLoad.call(this, request, parent, isMain);
};

const savedEnv = { ...process.env };
const bounty = require('../modules/bountyVault');

function deterministicRng(sequence) {
  let i = 0;
  return max => sequence[i++ % sequence.length] % max;
}

try {
  assert.strictEqual(bounty.decideVote(10, 9), 'accepted');
  assert.strictEqual(bounty.decideVote(9, 10), 'rejected');
  assert.strictEqual(bounty.decideVote(10, 10), 'rejected');
  assert.strictEqual(bounty.decideVote(0, 0), 'rejected');
  const votes = bounty.countVoteUsers(
    [{ id: '1' }, { id: '2' }, { id: '3', bot: true }, { id: '99' }],
    [{ id: '2' }, { id: '4' }, { id: '99' }],
    ['99']
  );
  assert.deepStrictEqual({ yes: votes.yes, no: votes.no }, { yes: 1, no: 1 });

  const entries = Array.from({ length: 10 }, (_, i) => ({ token_id: String(i + 1), contract_address: '0x8c9a02c0585200c4c65608df6b8def543d33792a' }));
  const nfts = [{ id: 11 }, { id: 12 }];
  const plan = bounty.buildDrawPlan(nfts, entries, deterministicRng([3, 1, 4, 1, 5, 9, 2]));
  assert.strictEqual(plan.length, 7);
  assert.strictEqual(plan.filter(x => x.prizeType === 'nft').length, 2);
  assert.deepStrictEqual(plan.filter(x => x.prizeType === 'charm').map(x => x.charmAmount), [2500, 5000, 5000, 10000, 10000]);
  assert.strictEqual(new Set(plan.map(x => x.winningEntry.token_id)).size, plan.length);
  assert.throws(() => bounty.buildDrawPlan(nfts, entries.slice(0, 6), deterministicRng([0])), /Insufficient unique entries/);

  assert.strictEqual(bounty.getMonthKey(new Date('2026-08-27T01:00:00Z')), '2026-08');
  assert.strictEqual(bounty.getMonthKey(new Date('2027-01-01T01:00:00Z')), '2026-12');
  assert.strictEqual(bounty.isFinalCalendarDay(new Date('2026-02-28T22:00:00Z')), true);
  assert.strictEqual(bounty.isFinalCalendarDay(new Date('2026-02-27T22:00:00Z')), false);
  assert.strictEqual(bounty.isMonthlyDrawDue(new Date('2026-03-01T01:00:00Z'), { drawTimeZone: 'America/Toronto', drawHour: 20, drawMinute: 0 }), true);
  assert.strictEqual(bounty.isMonthlyDrawDue(new Date('2026-07-01T00:00:00Z'), { drawTimeZone: 'America/Toronto', drawHour: 20, drawMinute: 0 }), true);
  assert.strictEqual(bounty.isMonthlyDrawDue(new Date('2026-07-01T00:00:00Z'), { drawTimeZone: 'America/Toronto', drawHour: 20, drawMinute: 1 }), false);
  assert.strictEqual(bounty.getScheduledDrawDate('2026-01', { drawTimeZone: 'America/Toronto', drawHour: 20, drawMinute: 0 }).toISOString(), '2026-02-01T01:00:00.000Z');
  assert.strictEqual(bounty.getScheduledDrawDate('2026-07', { drawTimeZone: 'America/Toronto', drawHour: 20, drawMinute: 0 }).toISOString(), '2026-08-01T00:00:00.000Z');
  assert.strictEqual(bounty.previousMonthKey('2026-01'), '2025-12');

  for (const key of Object.keys(process.env).filter(k => k.startsWith('BOUNTY_'))) delete process.env[key];
  const config = bounty.getBountyConfig(process.env);
  assert.strictEqual(config.vaultWalletAddress, '0x192907db190a47d963450e17471e05af99f65808');
  assert.strictEqual(config.reviewChannelId, '1477463175665287410');
  assert.strictEqual(config.acceptRewardCharm, 3000);
  assert.strictEqual(config.voteHours, 24);
  assert.strictEqual(config.drawHour, 20);
  assert.strictEqual(config.drawMinute, 0);
  assert.strictEqual(config.entryChain, 'ethereum');
  assert.strictEqual(config.chains.ethereum.chainId, 1);
  assert.strictEqual(config.chains.base.chainId, 8453);
  assert.strictEqual(config.chains.apechain.chainId, 33139);
  assert.strictEqual(config.chains.robinhood.chainId, 4663);

  assert.strictEqual(bounty.normalizeChain('eth'), 'ethereum');
  assert.strictEqual(bounty.normalizeChain('base'), 'base');
  assert.strictEqual(bounty.normalizeChain('ape_chain'), 'apechain');
  assert.strictEqual(bounty.normalizeChain('ApeChain'), 'apechain');
  assert.strictEqual(bounty.normalizeChain('robinhood_chain'), 'robinhood');
  assert.strictEqual(bounty.normalizeChain('polygon'), null);

  const valid = bounty.validateAndParseOpenSeaUrl('https://opensea.io/assets/ethereum/0x8C9A02c0585200c4c65608df6b8Def543D33792A/00123');
  assert.strictEqual(valid.ok, true);
  assert.strictEqual(valid.contractAddress, '0x8c9a02c0585200c4c65608df6b8def543d33792a');
  assert.strictEqual(valid.tokenId, '123');
  assert.strictEqual(valid.chain, 'ethereum');
  assert.strictEqual(bounty.validateAndParseOpenSeaUrl('https://opensea.io/item/base/0x8C9A02c0585200c4c65608df6b8Def543D33792A/123').chain, 'base');
  assert.strictEqual(bounty.validateAndParseOpenSeaUrl('https://opensea.io/item/ape_chain/0x8C9A02c0585200c4c65608df6b8Def543D33792A/123').chain, 'apechain');
  assert.strictEqual(bounty.validateAndParseOpenSeaUrl('https://opensea.io/item/robinhood/0x8C9A02c0585200c4c65608df6b8Def543D33792A/123').chain, 'robinhood');
  assert.strictEqual(bounty.validateAndParseOpenSeaUrl('https://opensea.io/item/polygon/0x8C9A02c0585200c4c65608df6b8Def543D33792A/123').chain, 'polygon');
  assert.strictEqual(bounty.validateAndParseOpenSeaUrl('http://opensea.io/assets/ethereum/x/1').ok, false);
  assert.strictEqual(bounty.validateAndParseOpenSeaUrl('https://opensea.io.evil.example/assets/ethereum/x/1').ok, false);
  assert.strictEqual(bounty.validateAndParseOpenSeaUrl('https://evil-opensea.io/item/ethereum/x/1').ok, false);
  assert.strictEqual(bounty.validateAndParseOpenSeaUrl('https://opensea.io/collection/a').ok, true);

  const multi = bounty.getBountyConfig({
    BOUNTY_ETH_RPC_URL: 'https://eth.example',
    BOUNTY_BASE_RPC_URL: 'https://base.example',
    BOUNTY_APECHAIN_RPC_URL: 'https://ape.example',
    BOUNTY_ROBINHOOD_RPC_URL: 'https://robinhood.example',
  });
  assert.deepStrictEqual(bounty.getEnabledBountyChains(multi), ['ethereum', 'base', 'apechain', 'robinhood']);
  assert.strictEqual(multi.chains.base.rpcUrl, 'https://base.example');
  assert.strictEqual(multi.chains.apechain.rpcUrl, 'https://ape.example');
  assert.strictEqual(multi.chains.robinhood.rpcUrl, 'https://robinhood.example');
  assert.strictEqual(bounty.getBountyChainConfig('ape_chain', multi).chainId, 33139);
  assert.deepStrictEqual(bounty.getEnabledBountyChains(bounty.getBountyConfig({ BOUNTY_BASE_RPC_URL: 'https://base.example' })), ['base']);

  const from = '0x2222222222222222222222222222222222222222';
  const vault = '0x1111111111111111111111111111111111111111';
  const transfer = bounty.parseInboundLog({
    topics: [
      'topic:Transfer(address,address,uint256)',
      `0x${from.slice(2).padStart(64, '0')}`,
      `0x${vault.slice(2).padStart(64, '0')}`,
      `0x${BigInt(42).toString(16).padStart(64, '0')}`,
    ],
    transactionHash: `0x${'a'.repeat(64)}`,
    index: 3,
    blockNumber: 100,
    address: '0x8C9A02c0585200c4c65608df6b8Def543D33792A',
  }, vault, 'base')[0];
  assert.strictEqual(transfer.chain, 'base');
  assert.strictEqual(transfer.tokenId, '42');
  assert.strictEqual(transfer.sourceWallet, from);

  assert.strictEqual(bounty.canTransition('awaiting_transfer', 'team_review'), true);
  assert.strictEqual(bounty.canTransition('awaiting_transfer', 'vaulted'), false);
  assert.strictEqual(bounty.canTransition('team_review', 'community_vote'), true);
  assert.strictEqual(bounty.canTransition('community_vote', 'vaulted'), true);
  assert.strictEqual(bounty.canTransition('vaulted', 'drawn_pending_delivery'), true);
  assert.strictEqual(bounty.canTransition('delivered', 'vaulted'), false);
  assert.throws(() => bounty.assertTransition('awaiting_transfer', 'vaulted'), /Illegal/);
  assert.strictEqual(bounty.isBlockBeyondHeadError({ error: { code: -32602, message: 'block range extends beyond current head block' } }), true);
  assert.strictEqual(bounty.isBlockBeyondHeadError({ message: 'server response 503 Service Unavailable' }), false);
  assert.strictEqual(bounty.isTransientRpcError({ info: { responseStatus: 503 } }), true);
  assert.strictEqual(bounty.isTransientRpcError({ error: { code: -32000, message: 'Internal error' } }), true);
  assert.strictEqual(bounty.isTransientRpcError({ code: 'BAD_DATA', value: [{ code: -32005, message: 'Too Many Requests' }] }), true);
  assert.strictEqual(bounty.isTransientRpcError({ code: 'INVALID_ARGUMENT', message: 'bad address' }), false);
  assert.strictEqual(bounty.transferBackoffMs(1), 15000);
  assert.strictEqual(bounty.transferBackoffMs(6), 300000);

  // Database-level unique constraints cover duplicate transfer logs, active submissions,
  // monthly entries, guild/month draws, NFT reuse, winners, and payout state claims.
  console.log('Bounty Vault logic tests passed.');
} finally {
  process.env = savedEnv;
  Module._load = originalLoad;
}
