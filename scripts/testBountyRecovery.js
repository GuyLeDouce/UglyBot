const assert = require('assert');
const Module = require('module');
const originalLoad = Module._load;
const wallet = '0x1855fc397d5e994b373b3952f4b4842f9596c0ae';
const vault = '0x192907db190a47d963450e17471e05af99f65808';
const contract = '0x8c9a02c0585200c4c65608df6b8def543d33792a';
const tx = '0x' + 'a'.repeat(64);
const topic = a => '0x' + a.slice(2).padStart(64, '0');
let scenario;
class Builder {
  setCustomId() { return this; } setTitle() { return this; } setLabel() { return this; }
  setMaxLength() { return this; } setRequired() { return this; } setStyle() { return this; }
  addComponents() { return this; } addFields() { return this; } setColor() { return this; }
}
Module._load = function(request, ...args) {
  if (request === 'discord.js') return {EmbedBuilder: Builder, ActionRowBuilder: Builder, ButtonBuilder: Builder,
    ModalBuilder: Builder, TextInputBuilder: Builder, ButtonStyle: {}, TextInputStyle: {}, PermissionFlagsBits: {ManageGuild: 32n}};
  if (request === 'ethers') return {ethers: {
    id: x => x,
    JsonRpcProvider: class {
      async getNetwork() { return {chainId: scenario.chainId ?? 1}; }
      async getBlockNumber() { return scenario.head ?? 110; }
      async getTransactionReceipt() { return {status: scenario.receiptStatus ?? 1, blockNumber: 100,
        logs: [{address: scenario.contract ?? contract, index: 7, topics: ['Transfer(address,address,uint256)', topic(wallet), topic(scenario.to ?? vault), '0xc55']}]}; }
    },
    Contract: class { async ownerOf() { return scenario.owner ?? vault; } },
  }};
  return originalLoad.call(this, request, ...args);
};
const bounty = require('../modules/bountyVaultCore');
Module._load = originalLoad;
process.env.BOUNTY_ETHEREUM_RPC_URL = 'https://example.invalid';
process.env.BOUNTY_VAULT_WALLET_ADDRESS = vault;
process.env.BOUNTY_MIN_CONFIRMATIONS = '2';

async function run(options = {}) {
  scenario = options;
  let committed = false, rolledBack = false, released = false, panelCount = 0;
  const writes = [], replies = [], channels = [];
  process.env.BOUNTY_TEAM_VOTE_CHANNEL_ID = options.primary ? 'primary' : '';
  process.env.BOUNTY_REVIEW_CHANNEL_ID = 'backup';
  const submission = {id: '42', guild_id: 'guild', sender_discord_id: '949454958426742804',
    chain: 'ethereum', contract_address: contract, token_id: '3157', status: options.status ?? 'expired',
    project_name: 'Squigs', opensea_url: `https://opensea.io/item/ethereum/${contract}/3157`, ...options.submission};
  const db = {release() { released = true; }, async query(sql, values) {
    if (sql === 'BEGIN' || sql.startsWith('SELECT pg_advisory')) return {rows: []};
    if (sql === 'COMMIT') { committed = true; return {rows: []}; }
    if (sql === 'ROLLBACK') { rolledBack = true; return {rows: []}; }
    if (sql.startsWith('SELECT * FROM bounty_unmatched_transfers WHERE id=')) return {rows: [{id: 6, status: options.resolved ? 'resolved' : 'manual_review', token_standard: 'erc721', chain: 'ethereum', contract_address: contract, token_id: '3157', source_wallet: wallet, tx_hash: tx}]};
    if (sql.includes('expires_at>NOW()')) return {rows: options.noMatch ? [] : options.ambiguous ? [submission, {...submission, id: '43'}] : [submission]};
    if (sql.startsWith('SELECT * FROM bounty_submissions')) return {rows: options.missing ? [] : [{...submission, ...(sql.includes('FOR UPDATE') ? options.locked : {})}]};
    if (sql.startsWith('SELECT * FROM bounty_detected')) return {rows: [{id: 5, submission_id: options.assigned ? '99' : null}]};
    if (sql.startsWith('SELECT * FROM bounty_unmatched')) return {rows: options.resolved ? [] : [{id: 6}]};
    if (sql.startsWith('SELECT id FROM bounty_submissions')) return {rows: [], rowCount: options.duplicate ? 1 : 0};
    if (sql.startsWith('UPDATE') || sql.startsWith('INSERT INTO bounty_audit')) { writes.push({sql, values}); return {rows: [], rowCount: 1}; }
    throw new Error(`Unexpected SQL: ${sql}`);
  }};
  bounty.initBountyVault({bountyPool: {...db, async connect() { return db; }},
    isAdmin: () => !options.unauthorized,
    getWalletLinks: async (guild, donor) => {
      assert.equal(guild, 'guild'); assert.equal(donor, '949454958426742804');
      return options.unlinked ? [] : [{wallet_address: wallet.toUpperCase().replace('0X', '0x')}];
    },
    client: {channels: {async fetch(id) { channels.push(id); if (options.fetchFailure && id === 'primary') throw new Error('Unavailable'); return {async send() { if (options.sendFailure && id === 'primary') throw new Error('Missing permission'); panelCount++; return {id: 'message', channelId: id}; }}; }}},
  });
  await bounty[options.retry ? 'handleComponent' : 'handleModalSubmit']({customId: options.retry ? 'bounty_retry:6' : 'bounty_recover_modal', guildId: 'guild', user: {id: 'admin'},
    fields: {getTextInputValue: key => key === 'submission_id' ? '42' : tx},
    async deferReply() {}, async reply(p) { replies.push(p.content); }, async editReply(p) { replies.push(p.content); },
  });
  return {committed, rolledBack, released, writes, panelCount, channels, reply: replies.join('\n')};
}

(async () => {
  for (const status of ['expired', 'awaiting_transfer']) {
    const r = await run({status});
    assert(r.committed, r.reply); assert(r.released); assert.equal(r.panelCount, 1);
    assert(r.writes.some(x => x.sql.includes("status='resolved'")));
    const audit = r.writes.find(x => x.sql.startsWith('INSERT INTO bounty_audit'));
    assert.equal(JSON.parse(audit.values[5]).donorDiscordId, '949454958426742804');
    assert(!r.writes.some(x => /payout|vaulted|community_vote/.test(x.sql)));
  }
  for (const options of [
    {unauthorized: true}, {missing: true}, {unlinked: true}, {to: wallet}, {owner: wallet},
    {receiptStatus: 0}, {head: 100}, {chainId: 8453}, {contract: wallet},
    {assigned: true}, {resolved: true}, {duplicate: true}, {status: 'vaulted'},
    {locked: {status: 'team_review'}}, {submission: {transfer_tx_hash: tx}},
  ]) {
    const r = await run(options);
    assert(!r.committed, JSON.stringify(options)); assert.equal(r.panelCount, 0);
    assert.equal(r.writes.length, 0, JSON.stringify(options));
  }
  for (const options of [{primary: true}, {primary: true, fetchFailure: true}, {primary: true, sendFailure: true}]) {
    const r = await run(options); assert(r.committed, r.reply);
    assert.deepEqual(r.channels, options.fetchFailure || options.sendFailure ? ['primary', 'backup'] : ['primary']);
  }
  const retried = await run({retry: true}); assert(retried.committed, retried.reply);
  for (const options of [{noMatch: true}, {ambiguous: true}, {unlinked: true}, {resolved: true}, {unauthorized: true}, {assigned: true}]) {
    const r = await run({...options, retry: true}); assert(!r.committed, JSON.stringify(options)); assert.equal(r.writes.length, 0);
  }
  console.log('Bounty recovery, retry matching and admin channel fallback tests passed.');
})().catch(e => { console.error(e); process.exitCode = 1; });
