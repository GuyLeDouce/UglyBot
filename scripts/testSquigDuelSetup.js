const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const filename = path.resolve(__dirname, '../modules/squigDuels.js');
const records = new Map();
let storageFails = false;
const pool = { async query(sql, values = []) {
  if (storageFails) throw new Error('storage offline');
  const key = values.slice(0, 2).join(':');
  if (sql.startsWith('SELECT token_id FROM squig_duel_defaults')) return { rows: records.has(key) ? [{token_id: records.get(key)}] : [] };
  if (sql.startsWith('INSERT INTO squig_duel_defaults')) records.set(key, values[2]);
  return { rows: [] };
}};
function load() {
  const context = vm.createContext({ require: createRequire(filename), module: { exports: {} }, __dirname: path.dirname(filename), process, console, Buffer, setTimeout, clearTimeout });
  vm.runInContext(fs.readFileSync(filename, 'utf8') + `
module.exports.test = {
  handleDuelDefault, handleStartButton, sendSquigSelectionPrompt, lockInSquigFighter,
  handleAcceptDecline, getDuelDefault, saveDuelDefault, buildMenuRow,
  pendingDuelDefaults, activeUserToDuel, duels, squigSelectionContent,
  configure(d, owned, stubs = {}) {
    deps = d;
    fetchOwnedSquigs = owned;
    hasHolderRole = async () => true;
    persistDuel = async () => {};
    collectWager = stubs.collectWager || (async () => ({ok: true}));
    postChallenge = async () => {};
    postReadyCheck = async () => {};
    resolveSquigImageUrl = async () => null;
    cancelDuel = async (guild, duel, reason) => { duel.status = 'cancelled'; duel.cancelReason = reason; };
    if (stubs.lock) lockInSquigFighter = stubs.lock;
    if (stubs.prompt) promptSquigSelection = stubs.prompt;
  }
};`, context, { filename });
  return context.module.exports.test;
}
const squigs = Array.from({ length: 30 }, (_, n) => ({tokenId: String(n+1), uglyPoints: n === 29 ? 950 : 500, maxHp: n === 29 ? 950 : 500, attackPower: 50}));
const guild = { id: 'guild', channels: { async fetch() { return thread; } } };
const messages = [];
const thread = { isTextBased: () => true, async send(p) { messages.push(p); return { async edit(q) { messages.push(q); } }; } };
function interaction(id, userId = 'player') {
  const replies = [];
  return { customId: id, guild, user: { id: userId }, replies, channel: {threads: { create() { throw new Error('Should not create a thread'); } }},
    async reply(p) { replies.push(p); }, async deferReply() {}, async deferUpdate() {},
    async update(p) { replies.push(p); }, async editReply(p) { replies.push(p); }, async followUp(p) { replies.push(p); } };
}
(async () => {
  let t = load();
  t.configure({ historyPool: pool }, async () => ({ok: true, squigs}));
  assert(t.buildMenuRow().toJSON().components.some(c => c.custom_id === 'sd:set_squig'));
  let i = interaction('sd:start');
  await t.handleStartButton(i);
  assert.match(i.replies[0].content, /Once saved, try/);
  assert.equal(t.activeUserToDuel.size, 0);
  i = interaction('sd:set_squig');
  await t.handleDuelDefault(i);
  let state = t.pendingDuelDefaults.get('guild:player');
  assert.equal(i.replies[0].components[0].toJSON().components[0].options.length, 25);
  const session = state.id;
  i = interaction(`sd:set_page:${session}:challenger:1`);
  await t.handleDuelDefault(i);
  assert.equal(state.selectedTokenId, '26');
  assert.equal(i.replies[0].components[0].toJSON().components[0].options.length, 5);
  i = interaction(`sd:set_select:${session}:challenger`); i.values = ['30'];
  await t.handleDuelDefault(i);
  assert.equal(state.selectedTokenId, '30');
  i = interaction(`sd:set_save:${session}:challenger`, 'intruder');
  await t.handleDuelDefault(i);
  assert.match(i.replies[0].content, /belongs to another/);
  assert.equal(records.size, 0);
  await t.handleDuelDefault(interaction(`sd:set_save:${session}:challenger`));
  assert.equal(await t.getDuelDefault('guild', 'player'), '30');
  t = load(); // Storage survives a fresh module instance / restart.
  t.configure({historyPool: pool}, async () => ({ok: true, squigs}));
  assert.equal(await t.getDuelDefault('guild', 'player'), '30');
  assert.equal(await t.getDuelDefault('other-guild', 'player'), null);
  await t.handleDuelDefault(interaction('sd:set_squig'));
  state = t.pendingDuelDefaults.get('guild:player');
  await t.handleDuelDefault(interaction(`sd:set_cancel:${state.id}:challenger`));
  assert.equal(await t.getDuelDefault('guild', 'player'), '30');
  await t.handleDuelDefault(interaction('sd:set_squig'));
  state = t.pendingDuelDefaults.get('guild:player');
  t.configure({historyPool: pool}, async () => ({ok: true, squigs: squigs.slice(0, 29)}));
  i = interaction(`sd:set_save:${state.id}:challenger`);
  await t.handleDuelDefault(i);
  assert.match(i.replies[0].content, /no longer available/);
  assert.equal(await t.getDuelDefault('guild', 'player'), '30');
  i = interaction('sd:start'); await t.handleStartButton(i);
  assert.match(i.replies[0].content, /not available/);
  let locks = 0, prompts = 0;
  t.configure({historyPool: pool}, async () => ({ok: true, squigs}), {lock: async () => {locks++;}, prompt: async () => {prompts++;}});
  const duel = {id: 'abcdefabcdef', guildId: 'guild', threadId: 'thread', challengerId: 'challenger', opponentId: 'player', challengerUglyPoints: 500, status: 'setup'};
  await t.sendSquigSelectionPrompt(guild, duel, 'opponent');
  assert.equal(prompts, 1); assert.equal(locks, 0);
  assert.equal(duel.temporaryOpponentSquig, true);
  assert.match(t.squigSelectionContent(duel, 'opponent'), /challenge only/);
  // At exactly 250 above challenger, the saved fighter is eligible.
  duel.challengerUglyPoints = 700;
  await t.sendSquigSelectionPrompt(guild, duel, 'opponent');
  assert.equal(locks, 1);
  duel.opponentId = 'first-timer';
  await t.sendSquigSelectionPrompt(guild, duel, 'opponent');
  assert.equal(prompts, 2); assert.equal(duel.temporaryOpponentSquig, false);
  duel.savedChallengerTokenId = '1';
  await t.sendSquigSelectionPrompt(guild, duel, 'challenger');
  assert.equal(locks, 2); // No selection for a configured challenger.
  t = load();
  let payments = 0;
  t.configure({historyPool: pool}, async () => ({ok: true, squigs}), {collectWager: async () => {payments++; return {ok: true};}});
  const makeDuel = () => ({...duel, status: 'setup', challengerUglyPoints: 500, wagerAmount: 50, opponentPaid: false, temporaryOpponentSquig: true});
  let d = makeDuel(); d.opponentId = 'player';
  i = interaction('unused');
  await Promise.all([t.lockInSquigFighter(i, d, 'opponent', {}, squigs[0], 'pending'), t.lockInSquigFighter(i, d, 'opponent', {}, squigs[0], 'pending')]);
  assert.equal(payments, 1);
  assert.equal(d.opponentSquigTokenId, '1');
  assert.equal(await t.getDuelDefault('guild', 'player'), '30'); // Temporary fighter never replaces default.
  d = makeDuel(); d.temporaryOpponentSquig = false; d.opponentId = 'first-timer';
  await t.lockInSquigFighter(interaction('unused', 'first-timer'), d, 'opponent', {}, squigs[1], 'pending');
  assert.equal(await t.getDuelDefault('guild', 'first-timer'), '2');
  d = makeDuel();
  await t.lockInSquigFighter(interaction('unused'), d, 'opponent', {}, squigs[29], 'pending');
  assert.equal(payments, 2); // Ineligible fighter cannot charge a wager.
  storageFails = true;
  await assert.rejects(t.saveDuelDefault('guild', 'player', '1'), /storage offline/);
  storageFails = false;
  console.log('PASS: lobby picker, pagination, authorization, cancel, persistence/restart, stale ownership, no-default Start, automatic saved fighters, 250 UP boundary, temporary challenge choice, first-time save, duplicate payment protection, storage failure.');
})().catch(err => { console.error(err); process.exitCode = 1; });
