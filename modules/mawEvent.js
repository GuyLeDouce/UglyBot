const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { ethers } = require('ethers');
const {
  SlashCommandBuilder,
  EmbedBuilder,
  AttachmentBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  StringSelectMenuBuilder,
  PermissionFlagsBits,
} = require('discord.js');
const marketplaceCommand = require('./marketplaceCommand');

const DEFAULT_SQUIG_CONTRACT = '0x8c9a02c0585200c4c65608df6b8def543d33792a';
const MAW_TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
const EPHEMERAL = 64;
const MAX_SELECT_OPTIONS = 25;
const PENDING_TTL_MS = 10 * 60 * 1000;
const CLAIM_TTL_HOURS = 24;
const DEFAULT_LOOKBACK_BLOCKS = 7200;
const DEFAULT_BLOCK_CHUNK_SIZE = 2000;
const SQUIG_IMAGE_BASE_URL = String(process.env.SQUIG_IMAGE_BASE_URL || '').replace(/\/+$/, '');
const LOCAL_SQUIG_IMAGE_DIR_CANDIDATES = [
  path.join(__dirname, '..', 'images'),
  path.join(__dirname, '..', '..', 'images'),
];

let deps = null;
let mawProvider = null;
let receiptInterval = null;
let expirationInterval = null;
let watcherStarted = false;
let receiptScanRunning = false;
let expirationRunning = false;

const pendingMawReviews = new Map();
const pendingMawSelections = new Map();
const activePrizeLocks = new Set();

function initMawEvent(injectedDeps = {}) {
  deps = injectedDeps || {};
}

function assertReady() {
  if (!deps) throw new Error('Maw module not initialized. Call initMawEvent first.');
}

function resolvePool(poolOrDeps = null) {
  const candidate = poolOrDeps?.query
    ? poolOrDeps
    : poolOrDeps?.mawPool || poolOrDeps?.marketplacePool || poolOrDeps?.prizesPool || poolOrDeps?.pool || deps?.mawPool || deps?.marketplacePool || deps?.prizesPool || deps?.pool;
  if (!candidate?.query) throw new Error('Maw database pool is not configured.');
  return candidate;
}

function intEnv(name, fallback, min = 0) {
  const parsed = Number(process.env[name]);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.floor(parsed));
}

function normalizeAddress(value) {
  const text = String(value || '').trim();
  if (!/^0x[a-fA-F0-9]{40}$/.test(text)) return null;
  return text.toLowerCase();
}

function addressTopic(address) {
  const normalized = normalizeAddress(address);
  if (!normalized) return null;
  return `0x${normalized.slice(2).padStart(64, '0')}`;
}

function shortAddress(value) {
  const normalized = normalizeAddress(value) || String(value || '');
  return normalized.length > 12 ? `${normalized.slice(0, 6)}...${normalized.slice(-4)}` : normalized;
}

function formatCharm(amount) {
  return marketplaceCommand.formatCharm(amount);
}

function formatToken(tokenId) {
  return `#${String(tokenId || '').trim()}`;
}

function formatDurationMinutes(minutes) {
  const n = Math.max(1, Math.floor(Number(minutes) || 0));
  return `${n} minute${n === 1 ? '' : 's'}`;
}

function getMawConfig() {
  return {
    mawWalletAddress: normalizeAddress(process.env.MAW_WALLET_ADDRESS),
    rawMawWalletAddress: String(process.env.MAW_WALLET_ADDRESS || '').trim(),
    squigContract: normalizeAddress(process.env.MAW_SQUIG_CONTRACT || DEFAULT_SQUIG_CONTRACT) || DEFAULT_SQUIG_CONTRACT,
    goalCount: intEnv('MAW_GOAL_COUNT', 40, 1),
    returnRewardCharm: intEnv('MAW_RETURN_REWARD_CHARM', 12500, 0),
    jackpotCharm: intEnv('MAW_JACKPOT_CHARM', 50000, 0),
    sessionTtlMinutes: intEnv('MAW_SESSION_TTL_MINUTES', 20, 1),
    prizeCashoutCharm: intEnv('MAW_PRIZE_CASHOUT_CHARM', 8000, 0),
    rerollCostCharm: intEnv('MAW_REROLL_COST_CHARM', 4000, 0),
    maxRerolls: intEnv('MAW_MAX_REROLLS', 3, 0),
    pollIntervalSeconds: intEnv('MAW_POLL_INTERVAL_SECONDS', 30, 5),
    minConfirmations: intEnv('MAW_MIN_CONFIRMATIONS', 2, 0),
    feedChannelId: String(process.env.MAW_FEED_CHANNEL_ID || '').trim() || null,
    adminChannelId: String(process.env.MAW_ADMIN_CHANNEL_ID || '').trim() || null,
  };
}

function calculateMawOpenSlots({ goalCount, receivedCount = 0, activeTransferWindows = 0 } = {}) {
  const goal = Math.max(0, Math.floor(Number(goalCount) || 0));
  const received = Math.max(0, Math.floor(Number(receivedCount) || 0));
  const active = Math.max(0, Math.floor(Number(activeTransferWindows) || 0));
  return Math.max(0, goal - received - active);
}

function normalizeWalletSet(wallets = []) {
  return new Set((Array.isArray(wallets) ? wallets : [wallets]).map(normalizeAddress).filter(Boolean));
}

function filterEligiblePrizeSquigsForUser(poolRows = [], targetUserId, targetWallets = [], excludedPoolIds = []) {
  const walletSet = normalizeWalletSet(targetWallets);
  const excludedIds = new Set((Array.isArray(excludedPoolIds) ? excludedPoolIds : [excludedPoolIds]).map((v) => String(v)));
  const targetId = String(targetUserId || '');
  return (Array.isArray(poolRows) ? poolRows : []).filter((row) => {
    if (!row) return false;
    if (String(row.status || 'available') !== 'available') return false;
    if (excludedIds.has(String(row.id))) return false;
    if (String(row.original_sender_discord_id || '') === targetId) return false;
    const originalWallet = normalizeAddress(row.original_sender_wallet);
    if (originalWallet && walletSet.has(originalWallet)) return false;
    return true;
  });
}

function randomChoice(items = []) {
  if (!items.length) return null;
  return items[Math.floor(Math.random() * items.length)] || null;
}

function randomToken(bytes = 8) {
  return crypto.randomBytes(bytes).toString('hex');
}

function advisoryLockKey(value) {
  return crypto.createHash('sha256').update(String(value)).digest().readBigInt64BE(0).toString();
}

function safeNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function sortMawSquigsForDisplay(squigs = []) {
  return [...(Array.isArray(squigs) ? squigs : [])].sort((a, b) => {
    const tokenDiff =
      safeNumber(b?.tokenId, Number.MIN_SAFE_INTEGER) -
      safeNumber(a?.tokenId, Number.MIN_SAFE_INTEGER);
    if (tokenDiff) return tokenDiff;
    return String(b?.tokenId || '').localeCompare(String(a?.tokenId || ''), undefined, { numeric: true });
  });
}

function mawSquigPageCount(squigs) {
  return Math.max(1, Math.ceil((Array.isArray(squigs) ? squigs.length : 0) / MAX_SELECT_OPTIONS));
}

function clampMawSquigPage(squigs, page = 0) {
  const maxPage = mawSquigPageCount(squigs) - 1;
  return Math.max(0, Math.min(maxPage, Math.floor(Number(page) || 0)));
}

function mawSquigPageItems(squigs, page = 0) {
  const safePage = clampMawSquigPage(squigs, page);
  const start = safePage * MAX_SELECT_OPTIONS;
  return (Array.isArray(squigs) ? squigs : []).slice(start, start + MAX_SELECT_OPTIONS);
}

function mawSquigPageLabel(squigs, page = 0) {
  const safePage = clampMawSquigPage(squigs, page);
  const total = Array.isArray(squigs) ? squigs.length : 0;
  const start = total ? (safePage * MAX_SELECT_OPTIONS) + 1 : 0;
  const end = Math.min(total, (safePage + 1) * MAX_SELECT_OPTIONS);
  return `Showing ${start}-${end} of ${total}`;
}

function mawSelectionKey(guildId, userId, eventId) {
  return `${String(guildId || '')}:${String(userId || '')}:${String(eventId || '')}`;
}

function cleanupPendingMawSelections(now = Date.now()) {
  for (const [key, state] of pendingMawSelections.entries()) {
    if (!state?.expiresAt || state.expiresAt < now) pendingMawSelections.delete(key);
  }
}

function setPendingMawSelection({ guildId, userId, eventId, squigs, page = 0 }) {
  cleanupPendingMawSelections();
  const sorted = sortMawSquigsForDisplay(squigs);
  const safePage = clampMawSquigPage(sorted, page);
  pendingMawSelections.set(mawSelectionKey(guildId, userId, eventId), {
    guildId: String(guildId),
    userId: String(userId),
    eventId: String(eventId),
    squigs: sorted,
    page: safePage,
    expiresAt: Date.now() + PENDING_TTL_MS,
  });
  return { squigs: sorted, page: safePage };
}

function getPendingMawSelection(guildId, userId, eventId) {
  const key = mawSelectionKey(guildId, userId, eventId);
  const state = pendingMawSelections.get(key);
  if (!state) return null;
  if (state.expiresAt < Date.now()) {
    pendingMawSelections.delete(key);
    return null;
  }
  return state;
}

function buildMawSquigSelectContent(squigs, page = 0) {
  const total = Array.isArray(squigs) ? squigs.length : 0;
  const sortedNote = 'Highest token IDs appear first.';
  if (total > MAX_SELECT_OPTIONS) {
    return `Select a Squig to feed the Maw.\n${mawSquigPageLabel(squigs, page)} eligible Squigs. ${sortedNote}`;
  }
  return `Select a Squig to feed the Maw.\n${total} eligible Squig${total === 1 ? '' : 's'}. ${sortedNote}`;
}

function buildMawSquigPageButtons(eventId, userId, squigs, page = 0) {
  const pageCount = mawSquigPageCount(squigs);
  if (pageCount <= 1) return [];
  const safePage = clampMawSquigPage(squigs, page);
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`maw_select_page:${eventId}:${userId}:${Math.max(0, safePage - 1)}`)
        .setLabel('Previous')
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(safePage <= 0),
      new ButtonBuilder()
        .setCustomId(`maw_select_page:${eventId}:${userId}:${Math.min(pageCount - 1, safePage + 1)}`)
        .setLabel('Next')
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(safePage >= pageCount - 1)
    ),
  ];
}

function buildMawSquigSelectRows(eventId, userId, squigs, page = 0) {
  const safePage = clampMawSquigPage(squigs, page);
  const options = mawSquigPageItems(squigs, safePage).map((entry) => ({
    label: `Squig ${formatToken(entry.tokenId)}`.slice(0, 100),
    description: `Wallet ${shortAddress(entry.wallet)}`.slice(0, 100),
    value: `${entry.wallet}:${entry.tokenId}`,
  }));
  return [
    new ActionRowBuilder().addComponents(
      new StringSelectMenuBuilder()
        .setCustomId(`maw_select_squig:${eventId}:${userId}`)
        .setPlaceholder('Pick a Squig for the Maw')
        .addOptions(options)
    ),
    ...buildMawSquigPageButtons(eventId, userId, squigs, safePage),
  ];
}

function localMawSquigImagePath(tokenId) {
  const tid = String(tokenId || '').trim();
  if (!/^\d+$/.test(tid)) return null;
  if (typeof deps?.localSquigImagePath === 'function') {
    const injectedPath = deps.localSquigImagePath(tid);
    if (injectedPath) return injectedPath;
  }
  for (const imageDir of LOCAL_SQUIG_IMAGE_DIR_CANDIDATES) {
    const candidate = path.join(imageDir, `${tid}.png`);
    if (fs.existsSync(candidate)) return candidate;
  }
  return null;
}

function mawSquigImageAttachment(tokenId) {
  const tid = String(tokenId || '').trim();
  if (!/^\d+$/.test(tid)) return { imageUrl: null, files: [] };
  const imagePath = localMawSquigImagePath(tid);
  if (imagePath) {
    const name = `maw-squig-${tid}${path.extname(imagePath) || '.png'}`;
    return {
      imageUrl: `attachment://${name}`,
      files: [new AttachmentBuilder(imagePath, { name })],
    };
  }
  if (SQUIG_IMAGE_BASE_URL) return { imageUrl: `${SQUIG_IMAGE_BASE_URL}/${tid}`, files: [] };
  return { imageUrl: null, files: [] };
}

function isAdmin(interaction) {
  if (typeof deps?.isAdmin === 'function') return deps.isAdmin(interaction);
  return Boolean(interaction?.memberPermissions?.has(PermissionFlagsBits.ManageGuild));
}

async function ensureMawTables(poolOrDeps = null) {
  const pool = resolvePool(poolOrDeps);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS maw_events (
      id BIGSERIAL PRIMARY KEY,
      guild_id TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'draft',
      goal_count INT NOT NULL,
      return_reward_charm NUMERIC NOT NULL,
      jackpot_charm NUMERIC NOT NULL,
      session_ttl_minutes INT NOT NULL DEFAULT 20,
      received_count INT NOT NULL DEFAULT 0,
      panel_channel_id TEXT,
      panel_message_id TEXT,
      feed_channel_id TEXT,
      admin_channel_id TEXT,
      draw_completed BOOLEAN NOT NULL DEFAULT FALSE,
      draw_winning_ticket_id BIGINT,
      draw_winner_user_id TEXT,
      draw_payout_status TEXT,
      draw_payout_reference TEXT,
      started_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      created_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await addColumns(pool, 'maw_events', [
    ['guild_id', 'TEXT'],
    ['status', "TEXT NOT NULL DEFAULT 'draft'"],
    ['goal_count', 'INT NOT NULL DEFAULT 40'],
    ['return_reward_charm', 'NUMERIC NOT NULL DEFAULT 12500'],
    ['jackpot_charm', 'NUMERIC NOT NULL DEFAULT 50000'],
    ['session_ttl_minutes', 'INT NOT NULL DEFAULT 20'],
    ['received_count', 'INT NOT NULL DEFAULT 0'],
    ['panel_channel_id', 'TEXT'],
    ['panel_message_id', 'TEXT'],
    ['feed_channel_id', 'TEXT'],
    ['admin_channel_id', 'TEXT'],
    ['draw_completed', 'BOOLEAN NOT NULL DEFAULT FALSE'],
    ['draw_winning_ticket_id', 'BIGINT'],
    ['draw_winner_user_id', 'TEXT'],
    ['draw_payout_status', 'TEXT'],
    ['draw_payout_reference', 'TEXT'],
    ['started_at', 'TIMESTAMPTZ'],
    ['completed_at', 'TIMESTAMPTZ'],
    ['created_by', 'TEXT'],
    ['created_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
    ['updated_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
  ]);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_events_one_open_per_guild_uidx ON maw_events (guild_id) WHERE status = 'open';`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS maw_return_sessions (
      id BIGSERIAL PRIMARY KEY,
      event_id BIGINT NOT NULL REFERENCES maw_events(id),
      guild_id TEXT NOT NULL,
      discord_user_id TEXT NOT NULL,
      source_wallet TEXT NOT NULL,
      contract_address TEXT NOT NULL,
      token_id TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'awaiting_transfer',
      expires_at TIMESTAMPTZ NOT NULL,
      received_tx_hash TEXT,
      received_log_index INT,
      received_at TIMESTAMPTZ,
      payout_amount NUMERIC,
      payout_status TEXT,
      payout_reference TEXT,
      ticket_id BIGINT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await addColumns(pool, 'maw_return_sessions', [
    ['event_id', 'BIGINT REFERENCES maw_events(id)'],
    ['guild_id', 'TEXT'],
    ['discord_user_id', 'TEXT'],
    ['source_wallet', 'TEXT'],
    ['contract_address', 'TEXT'],
    ['token_id', 'TEXT'],
    ['status', "TEXT NOT NULL DEFAULT 'awaiting_transfer'"],
    ['expires_at', 'TIMESTAMPTZ'],
    ['received_tx_hash', 'TEXT'],
    ['received_log_index', 'INT'],
    ['received_at', 'TIMESTAMPTZ'],
    ['payout_amount', 'NUMERIC'],
    ['payout_status', 'TEXT'],
    ['payout_reference', 'TEXT'],
    ['ticket_id', 'BIGINT'],
    ['created_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
    ['updated_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
  ]);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_return_sessions_received_log_uidx ON maw_return_sessions (received_tx_hash, received_log_index) WHERE received_tx_hash IS NOT NULL AND received_log_index IS NOT NULL;`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_return_sessions_active_user_uidx ON maw_return_sessions (guild_id, discord_user_id) WHERE status = 'awaiting_transfer';`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_return_sessions_active_token_uidx ON maw_return_sessions (guild_id, contract_address, token_id) WHERE status = 'awaiting_transfer';`);
  await pool.query(`CREATE INDEX IF NOT EXISTS maw_return_sessions_event_status_idx ON maw_return_sessions (event_id, status, expires_at);`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS maw_squig_pool (
      id BIGSERIAL PRIMARY KEY,
      event_id BIGINT REFERENCES maw_events(id),
      contract_address TEXT NOT NULL,
      token_id TEXT NOT NULL,
      original_sender_discord_id TEXT NOT NULL,
      original_sender_wallet TEXT NOT NULL,
      received_session_id BIGINT REFERENCES maw_return_sessions(id),
      received_tx_hash TEXT,
      status TEXT NOT NULL DEFAULT 'available',
      reserved_claim_id BIGINT,
      delivered_to_discord_id TEXT,
      delivered_to_wallet TEXT,
      delivered_tx_hash TEXT,
      times_offered INT NOT NULL DEFAULT 0,
      times_rerolled_away INT NOT NULL DEFAULT 0,
      times_cashed_out INT NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await addColumns(pool, 'maw_squig_pool', [
    ['event_id', 'BIGINT REFERENCES maw_events(id)'],
    ['contract_address', 'TEXT'],
    ['token_id', 'TEXT'],
    ['original_sender_discord_id', 'TEXT'],
    ['original_sender_wallet', 'TEXT'],
    ['received_session_id', 'BIGINT REFERENCES maw_return_sessions(id)'],
    ['received_tx_hash', 'TEXT'],
    ['status', "TEXT NOT NULL DEFAULT 'available'"],
    ['reserved_claim_id', 'BIGINT'],
    ['delivered_to_discord_id', 'TEXT'],
    ['delivered_to_wallet', 'TEXT'],
    ['delivered_tx_hash', 'TEXT'],
    ['times_offered', 'INT NOT NULL DEFAULT 0'],
    ['times_rerolled_away', 'INT NOT NULL DEFAULT 0'],
    ['times_cashed_out', 'INT NOT NULL DEFAULT 0'],
    ['created_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
    ['updated_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
  ]);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_squig_pool_active_token_uidx ON maw_squig_pool (contract_address, token_id) WHERE status <> 'retired';`);
  await pool.query(`CREATE INDEX IF NOT EXISTS maw_squig_pool_status_idx ON maw_squig_pool (status, updated_at DESC);`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS maw_tickets (
      id BIGSERIAL PRIMARY KEY,
      event_id BIGINT NOT NULL REFERENCES maw_events(id),
      ticket_number INT NOT NULL,
      discord_user_id TEXT NOT NULL,
      return_session_id BIGINT NOT NULL REFERENCES maw_return_sessions(id),
      contract_address TEXT NOT NULL,
      token_id TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await addColumns(pool, 'maw_tickets', [
    ['event_id', 'BIGINT REFERENCES maw_events(id)'],
    ['ticket_number', 'INT'],
    ['discord_user_id', 'TEXT'],
    ['return_session_id', 'BIGINT REFERENCES maw_return_sessions(id)'],
    ['contract_address', 'TEXT'],
    ['token_id', 'TEXT'],
    ['created_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
  ]);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_tickets_event_number_uidx ON maw_tickets (event_id, ticket_number);`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_tickets_event_session_uidx ON maw_tickets (event_id, return_session_id);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS maw_tickets_user_idx ON maw_tickets (event_id, discord_user_id);`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS maw_chain_cursors (
      id BIGSERIAL PRIMARY KEY,
      guild_id TEXT NOT NULL,
      contract_address TEXT NOT NULL,
      maw_wallet_address TEXT NOT NULL,
      last_processed_block BIGINT NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await addColumns(pool, 'maw_chain_cursors', [
    ['guild_id', 'TEXT'],
    ['contract_address', 'TEXT'],
    ['maw_wallet_address', 'TEXT'],
    ['last_processed_block', 'BIGINT NOT NULL DEFAULT 0'],
    ['updated_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
  ]);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_chain_cursors_lookup_uidx ON maw_chain_cursors (guild_id, contract_address, maw_wallet_address);`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS maw_unmatched_transfers (
      id BIGSERIAL PRIMARY KEY,
      guild_id TEXT,
      contract_address TEXT NOT NULL,
      token_id TEXT NOT NULL,
      from_wallet TEXT NOT NULL,
      to_wallet TEXT NOT NULL,
      tx_hash TEXT NOT NULL,
      log_index INT NOT NULL,
      block_number BIGINT NOT NULL,
      status TEXT NOT NULL DEFAULT 'manual_review',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await addColumns(pool, 'maw_unmatched_transfers', [
    ['guild_id', 'TEXT'],
    ['contract_address', 'TEXT'],
    ['token_id', 'TEXT'],
    ['from_wallet', 'TEXT'],
    ['to_wallet', 'TEXT'],
    ['tx_hash', 'TEXT'],
    ['log_index', 'INT'],
    ['block_number', 'BIGINT'],
    ['status', "TEXT NOT NULL DEFAULT 'manual_review'"],
    ['created_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
  ]);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_unmatched_transfers_log_uidx ON maw_unmatched_transfers (tx_hash, log_index);`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS squig_prize_claims (
      id BIGSERIAL PRIMARY KEY,
      guild_id TEXT NOT NULL,
      winner_discord_id TEXT NOT NULL,
      awarded_by_discord_id TEXT NOT NULL,
      reason TEXT,
      current_pool_squig_id BIGINT REFERENCES maw_squig_pool(id),
      status TEXT NOT NULL DEFAULT 'offered',
      reroll_count INT NOT NULL DEFAULT 0,
      cashout_amount NUMERIC,
      cashout_payout_reference TEXT,
      offer_channel_id TEXT,
      offer_message_id TEXT,
      delivery_thread_id TEXT,
      expires_at TIMESTAMPTZ NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await addColumns(pool, 'squig_prize_claims', [
    ['guild_id', 'TEXT'],
    ['winner_discord_id', 'TEXT'],
    ['awarded_by_discord_id', 'TEXT'],
    ['reason', 'TEXT'],
    ['current_pool_squig_id', 'BIGINT REFERENCES maw_squig_pool(id)'],
    ['status', "TEXT NOT NULL DEFAULT 'offered'"],
    ['reroll_count', 'INT NOT NULL DEFAULT 0'],
    ['cashout_amount', 'NUMERIC'],
    ['cashout_payout_reference', 'TEXT'],
    ['offer_channel_id', 'TEXT'],
    ['offer_message_id', 'TEXT'],
    ['delivery_thread_id', 'TEXT'],
    ['expires_at', 'TIMESTAMPTZ'],
    ['created_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
    ['updated_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
  ]);
  await pool.query(`CREATE INDEX IF NOT EXISTS squig_prize_claims_status_idx ON squig_prize_claims (guild_id, status, expires_at);`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS squig_prize_claims_active_pool_uidx ON squig_prize_claims (current_pool_squig_id) WHERE status IN ('offered', 'accepted_pending_delivery');`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS squig_prize_claim_history (
      id BIGSERIAL PRIMARY KEY,
      claim_id BIGINT NOT NULL REFERENCES squig_prize_claims(id),
      pool_squig_id BIGINT REFERENCES maw_squig_pool(id),
      action TEXT NOT NULL,
      charm_delta NUMERIC,
      actor_discord_id TEXT,
      note TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await addColumns(pool, 'squig_prize_claim_history', [
    ['claim_id', 'BIGINT REFERENCES squig_prize_claims(id)'],
    ['pool_squig_id', 'BIGINT REFERENCES maw_squig_pool(id)'],
    ['action', 'TEXT'],
    ['charm_delta', 'NUMERIC'],
    ['actor_discord_id', 'TEXT'],
    ['note', 'TEXT'],
    ['created_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
  ]);
  await pool.query(`CREATE INDEX IF NOT EXISTS squig_prize_claim_history_claim_idx ON squig_prize_claim_history (claim_id, created_at);`);
}

async function addColumns(pool, tableName, columns) {
  for (const [name, type] of columns) {
    await pool.query(`ALTER TABLE ${tableName} ADD COLUMN IF NOT EXISTS ${name} ${type};`);
  }
}

function buildMawSlashCommand() {
  return new SlashCommandBuilder()
    .setName('maw')
    .setDescription('Feed Squigs to the Maw')
    .addSubcommand((sub) =>
      sub
        .setName('post')
        .setDescription('Admin: post the public Maw panel in this channel')
    )
    .addSubcommand((sub) =>
      sub
        .setName('open')
        .setDescription('Admin: open a Feed the Maw event')
        .addIntegerOption((opt) => opt.setName('goal').setDescription('Squigs needed to fill the Maw').setMinValue(1).setRequired(false))
        .addIntegerOption((opt) => opt.setName('reward').setDescription('Immediate $CHARM payout per return').setMinValue(0).setRequired(false))
        .addIntegerOption((opt) => opt.setName('jackpot').setDescription('Maw Ticket Draw $CHARM prize').setMinValue(0).setRequired(false))
        .addChannelOption((opt) => opt.setName('feed_channel').setDescription('Public Maw feed channel').setRequired(false))
        .addChannelOption((opt) => opt.setName('admin_channel').setDescription('Admin review channel').setRequired(false))
    )
    .addSubcommand((sub) =>
      sub
        .setName('close')
        .setDescription('Admin: close the active Maw event')
    )
    .addSubcommand((sub) =>
      sub
        .setName('status')
        .setDescription('Show current Maw progress and your tickets')
    )
    .addSubcommand((sub) =>
      sub
        .setName('inventory')
        .setDescription('Admin: show Maw Pool inventory counts')
    )
    .addSubcommand((sub) =>
      sub
        .setName('reconcile')
        .setDescription('Admin: run the Maw transfer checker now')
    );
}

function buildSquigPrizeSlashCommand() {
  return new SlashCommandBuilder()
    .setName('squigprize')
    .setDescription('Admin: award or deliver a Squig from the Maw Pool')
    .addUserOption((opt) =>
      opt
        .setName('user')
        .setDescription('User to award a random Maw Pool Squig')
        .setRequired(false)
    )
    .addStringOption((opt) =>
      opt
        .setName('reason')
        .setDescription('Optional reason for the prize')
        .setRequired(false)
    )
    .addIntegerOption((opt) =>
      opt
        .setName('claim_id')
        .setDescription('Claim ID to mark delivered')
        .setMinValue(1)
        .setRequired(false)
    )
    .addStringOption((opt) =>
      opt
        .setName('tx')
        .setDescription('Optional delivery transaction hash')
        .setRequired(false)
    );
}

async function handleCommand(interaction) {
  if (!['maw', 'squigprize'].includes(interaction.commandName)) return false;
  assertReady();
  if (interaction.commandName === 'maw') {
    await handleMawCommand(interaction);
    return true;
  }
  await handleSquigPrizeCommand(interaction);
  return true;
}

async function handleComponent(interaction) {
  const id = String(interaction.customId || '');
  if (!id.startsWith('maw_')) return false;
  assertReady();
  if (interaction.isStringSelectMenu?.() && id.startsWith('maw_select_squig:')) {
    await handleMawSquigSelect(interaction);
    return true;
  }
  if (!interaction.isButton?.()) return false;

  if (id.startsWith('maw_feed_start:')) return handleMawFeedStart(interaction);
  if (id.startsWith('maw_select_page:')) return handleMawSquigPageButton(interaction);
  if (id.startsWith('maw_review_continue:')) return handleMawReviewContinue(interaction);
  if (id.startsWith('maw_confirm_start_timer:')) return handleMawConfirmStartTimer(interaction);
  if (id.startsWith('maw_cancel_session:')) return handleMawCancelSession(interaction);
  if (id.startsWith('maw_refresh_session:')) return handleMawRefreshSession(interaction);
  if (id.startsWith('maw_cancel:')) return handleMawPendingCancel(interaction);

  if (id.startsWith('maw_prize_accept_confirm:')) return handlePrizeAcceptConfirm(interaction);
  if (id.startsWith('maw_prize_cashout_confirm:')) return handlePrizeCashoutConfirm(interaction);
  if (id.startsWith('maw_prize_reroll_confirm:')) return handlePrizeRerollConfirm(interaction);
  if (id.startsWith('maw_prize_accept:')) return handlePrizeAccept(interaction);
  if (id.startsWith('maw_prize_cashout:')) return handlePrizeCashout(interaction);
  if (id.startsWith('maw_prize_reroll:')) return handlePrizeReroll(interaction);
  if (id.startsWith('maw_prize_delivered:')) return handlePrizeDeliveredButton(interaction);
  if (id.startsWith('maw_prize_cancel_confirm:')) return handlePrizeConfirmCancel(interaction);

  return false;
}

async function handleMawCommand(interaction) {
  const subcommand = interaction.options.getSubcommand();
  if (['post', 'open', 'close', 'inventory', 'reconcile'].includes(subcommand) && !isAdmin(interaction)) {
    await interaction.reply({ content: 'Admin only.', flags: EPHEMERAL });
    return;
  }

  if (subcommand === 'open') return handleMawOpen(interaction);
  if (subcommand === 'post') return handleMawPost(interaction);
  if (subcommand === 'close') return handleMawClose(interaction);
  if (subcommand === 'status') return handleMawStatus(interaction);
  if (subcommand === 'inventory') return handleMawInventory(interaction);
  if (subcommand === 'reconcile') return handleMawReconcile(interaction);

  await interaction.reply({ content: 'The Maw did not understand that command.', flags: EPHEMERAL });
}

async function getOpenMawEvent(guildId, db = null) {
  const pool = db || resolvePool();
  const { rows } = await pool.query(
    `SELECT * FROM maw_events WHERE guild_id = $1 AND status = 'open' ORDER BY id DESC LIMIT 1`,
    [String(guildId)]
  );
  return rows[0] || null;
}

async function getMawEventById(eventId, db = null) {
  const pool = db || resolvePool();
  const { rows } = await pool.query(`SELECT * FROM maw_events WHERE id = $1 LIMIT 1`, [String(eventId)]);
  return rows[0] || null;
}

async function expireAwaitingSessions(guildId = null, options = {}) {
  const pool = options.db || resolvePool();
  const params = [];
  let guildClause = '';
  if (guildId) {
    params.push(String(guildId));
    guildClause = ` AND guild_id = $${params.length}`;
  }
  const { rows } = await pool.query(
    `UPDATE maw_return_sessions
     SET status = 'expired',
         updated_at = NOW()
     WHERE status = 'awaiting_transfer'
       AND expires_at < NOW()
       ${guildClause}
     RETURNING *`,
    params
  );
  return rows;
}

async function countActiveTransferWindows(eventId, db = null) {
  const pool = db || resolvePool();
  const { rows } = await pool.query(
    `SELECT COUNT(*)::int AS count
     FROM maw_return_sessions
     WHERE event_id = $1
       AND status = 'awaiting_transfer'
       AND expires_at > NOW()`,
    [String(eventId)]
  );
  return Number(rows[0]?.count || 0);
}

async function getMawEventSummary(event, db = null) {
  if (!event) return null;
  const activeTransferWindows = await countActiveTransferWindows(event.id, db);
  return {
    event,
    activeTransferWindows,
    openSlots: calculateMawOpenSlots({
      goalCount: event.goal_count,
      receivedCount: event.received_count,
      activeTransferWindows,
    }),
  };
}

async function handleMawOpen(interaction) {
  const config = getMawConfig();
  if (!config.mawWalletAddress) {
    await interaction.reply({
      content: 'MAW_WALLET_ADDRESS must be configured before the Maw can open.',
      flags: EPHEMERAL,
    });
    return;
  }

  const existing = await getOpenMawEvent(interaction.guildId);
  if (existing) {
    await interaction.reply({
      content: `The Maw is already open. Event ID: ${existing.id}.`,
      flags: EPHEMERAL,
    });
    return;
  }

  const goal = interaction.options.getInteger('goal') || config.goalCount;
  const reward = interaction.options.getInteger('reward') || config.returnRewardCharm;
  const jackpot = interaction.options.getInteger('jackpot') || config.jackpotCharm;
  const feedChannel = interaction.options.getChannel('feed_channel');
  const adminChannel = interaction.options.getChannel('admin_channel');

  const pool = resolvePool();
  try {
    const { rows } = await pool.query(
      `INSERT INTO maw_events
         (guild_id, status, goal_count, return_reward_charm, jackpot_charm, session_ttl_minutes,
          feed_channel_id, admin_channel_id, started_at, created_by, created_at, updated_at)
       VALUES ($1, 'open', $2, $3, $4, $5, $6, $7, NOW(), $8, NOW(), NOW())
       RETURNING *`,
      [
        String(interaction.guildId),
        Math.floor(Number(goal) || config.goalCount),
        Math.floor(Number(reward) || config.returnRewardCharm),
        Math.floor(Number(jackpot) || config.jackpotCharm),
        config.sessionTtlMinutes,
        feedChannel?.id || config.feedChannelId,
        adminChannel?.id || config.adminChannelId,
        String(interaction.user.id),
      ]
    );
    await interaction.reply({
      content:
        `The Maw is open. Event ID: ${rows[0].id}.\n` +
        `Goal: ${rows[0].goal_count} Squigs. Return payout: ${formatCharm(rows[0].return_reward_charm)} $CHARM. Maw Ticket Draw: ${formatCharm(rows[0].jackpot_charm)} $CHARM.`,
      flags: EPHEMERAL,
    });
  } catch (err) {
    if (/maw_events_one_open_per_guild_uidx/i.test(String(err?.message || ''))) {
      await interaction.reply({ content: 'The Maw is already open in this server.', flags: EPHEMERAL });
      return;
    }
    await postAdminLog(interaction.guild, 'Maw Error', `Open failed: ${String(err?.message || err).slice(0, 800)}`);
    await interaction.reply({ content: 'The Maw coughed on the open button. Try again in a moment.', flags: EPHEMERAL });
  }
}

async function handleMawPost(interaction) {
  const config = getMawConfig();
  const event = await getOpenMawEvent(interaction.guildId);
  if (event && !config.mawWalletAddress) {
    await interaction.reply({
      content: 'MAW_WALLET_ADDRESS must be configured before the Maw can accept returns.',
      flags: EPHEMERAL,
    });
    return;
  }

  const summary = event ? await getMawEventSummary(event) : null;
  const message = await interaction.channel.send(buildMawPanelPayload(summary));
  if (event) {
    await resolvePool().query(
      `UPDATE maw_events
       SET panel_channel_id = $2,
           panel_message_id = $3,
           updated_at = NOW()
       WHERE id = $1`,
      [String(event.id), String(interaction.channel.id), String(message.id)]
    );
  }

  await interaction.reply({
    content: event
      ? 'Maw panel posted.'
      : `Closed Maw panel posted. Only admins can open it.${config.mawWalletAddress ? '' : ' MAW_WALLET_ADDRESS must be configured before the Maw can open.'}`,
    flags: EPHEMERAL,
  });
}

async function handleMawClose(interaction) {
  const event = await getOpenMawEvent(interaction.guildId);
  if (!event) {
    await interaction.reply({ content: 'The Maw is already closed.', flags: EPHEMERAL });
    return;
  }
  const { rows } = await resolvePool().query(
    `UPDATE maw_events
     SET status = 'closed',
         completed_at = COALESCE(completed_at, NOW()),
         updated_at = NOW()
     WHERE id = $1
       AND status = 'open'
     RETURNING *`,
    [String(event.id)]
  );
  if (rows[0]) await updateMawPanel(rows[0].id).catch(() => null);
  await interaction.reply({ content: 'The Maw is closed. Pool inventory remains untouched.', flags: EPHEMERAL });
}

async function handleMawStatus(interaction) {
  await expireAwaitingSessions(interaction.guildId).catch(() => []);
  const event = await getOpenMawEvent(interaction.guildId);
  if (!event) {
    await interaction.reply({ content: 'The Maw is closed right now. It is sleeping with one eye open.', flags: EPHEMERAL });
    return;
  }
  const summary = await getMawEventSummary(event);
  const pool = resolvePool();
  const tickets = await pool.query(
    `SELECT ticket_number, token_id, created_at
     FROM maw_tickets
     WHERE event_id = $1 AND discord_user_id = $2
     ORDER BY ticket_number ASC`,
    [String(event.id), String(interaction.user.id)]
  );
  const active = await pool.query(
    `SELECT *
     FROM maw_return_sessions
     WHERE event_id = $1
       AND discord_user_id = $2
       AND status = 'awaiting_transfer'
       AND expires_at > NOW()
     ORDER BY created_at DESC
     LIMIT 1`,
    [String(event.id), String(interaction.user.id)]
  );
  const ticketText = tickets.rows.length
    ? tickets.rows.map((row) => `#${row.ticket_number} (Squig ${formatToken(row.token_id)})`).join(', ')
    : 'No Maw Tickets yet.';
  const activeText = active.rows[0]
    ? `Squig ${formatToken(active.rows[0].token_id)} expires <t:${Math.floor(new Date(active.rows[0].expires_at).getTime() / 1000)}:R>.`
    : 'No active transfer window.';
  const embed = new EmbedBuilder()
    .setTitle('Maw Status')
    .setColor(0x8b1e3f)
    .addFields(
      { name: 'Progress', value: `${event.received_count} / ${event.goal_count} Squigs consumed`, inline: true },
      { name: 'Open slots', value: String(summary.openSlots), inline: true },
      { name: 'Active transfer windows', value: String(summary.activeTransferWindows), inline: true },
      { name: 'Your Maw Tickets', value: ticketText, inline: false },
      { name: 'Your active session', value: activeText, inline: false },
      { name: 'Maw Ticket Draw', value: event.draw_completed ? 'Complete.' : `${formatCharm(event.jackpot_charm)} $CHARM unlocks at goal.`, inline: false }
    );
  await interaction.reply({ embeds: [embed], flags: EPHEMERAL });
}

async function handleMawInventory(interaction) {
  const pool = resolvePool();
  const statusRows = await pool.query(
    `SELECT status, COUNT(*)::int AS count
     FROM maw_squig_pool
     GROUP BY status
     ORDER BY status`
  );
  const eventRows = await pool.query(
    `SELECT e.id, e.status, e.received_count, COUNT(p.id)::int AS pool_count
     FROM maw_events e
     LEFT JOIN maw_squig_pool p ON p.event_id = e.id
     WHERE e.guild_id = $1
     GROUP BY e.id
     ORDER BY e.id DESC
     LIMIT 10`,
    [String(interaction.guildId)]
  );
  const counts = new Map(statusRows.rows.map((row) => [String(row.status), Number(row.count || 0)]));
  const total = [...counts.values()].reduce((sum, n) => sum + n, 0);
  const eventText = eventRows.rows.length
    ? eventRows.rows.map((row) => `Event ${row.id}: ${row.status}, received ${row.received_count}, pool ${row.pool_count}`).join('\n')
    : 'No Maw events yet.';
  const embed = new EmbedBuilder()
    .setTitle('Maw Pool Inventory')
    .setColor(0x5f3dc4)
    .addFields(
      { name: 'Total pool', value: String(total), inline: true },
      { name: 'Available', value: String(counts.get('available') || 0), inline: true },
      { name: 'Reserved', value: String(counts.get('reserved_for_claim') || 0), inline: true },
      { name: 'Accepted pending delivery', value: String(counts.get('accepted_pending_delivery') || 0), inline: true },
      { name: 'Delivered', value: String(counts.get('delivered') || 0), inline: true },
      { name: 'Manual review', value: String(counts.get('manual_review') || 0), inline: true },
      { name: 'Original event counts', value: eventText.slice(0, 1024), inline: false }
    );
  await interaction.reply({ embeds: [embed], flags: EPHEMERAL });
}

async function handleMawReconcile(interaction) {
  await interaction.deferReply({ ephemeral: true });
  const result = await runMawReceiptCheck({ guildId: interaction.guildId, manual: true });
  if (!result.ok) {
    await interaction.editReply({ content: result.reason || 'The Maw watcher is not ready.' });
    return;
  }
  await interaction.editReply({
    content:
      `Maw reconcile complete.\n` +
      `Scanned events: ${result.eventsScanned}\n` +
      `Logs seen: ${result.logsSeen}\n` +
      `Matched: ${result.matched}\n` +
      `Unmatched/manual review: ${result.unmatched}\n` +
      `Duplicates skipped: ${result.duplicates}\n` +
      `Errors: ${result.errors.length ? result.errors.slice(0, 3).join(' | ') : 'none'}`,
  });
}

function buildMawPanelPayload(summary = null) {
  const event = summary?.event || null;
  if (!event) {
    const embed = new EmbedBuilder()
      .setTitle('THE MAW IS CLOSED')
      .setColor(0x313338)
      .setDescription('The Maw is not open right now. Only admins can open it.');
    return {
      embeds: [embed],
      components: [
        new ActionRowBuilder().addComponents(
          new ButtonBuilder()
            .setCustomId('maw_feed_start:closed')
            .setLabel('Feed the Maw')
            .setStyle(ButtonStyle.Danger)
            .setDisabled(true)
        ),
      ],
    };
  }

  const reward = formatCharm(event.return_reward_charm);
  const jackpot = formatCharm(event.jackpot_charm);
  const embed = new EmbedBuilder()
    .setTitle('THE MAW IS HUNGRY')
    .setColor(event.draw_completed ? 0x2f9e44 : 0x8b1e3f)
    .setDescription(
      `Return an eligible Squig to the Malformed Marketplace and receive ${reward} $CHARM. ` +
      `Every accepted Squig earns 1 Maw Ticket. If the Maw reaches ${event.goal_count} Squigs, ` +
      `one Maw Ticket wins ${jackpot} $CHARM.`
    )
    .addFields(
      { name: 'Progress', value: `${event.received_count} / ${event.goal_count} Squigs consumed`, inline: true },
      { name: 'Active transfer windows', value: String(summary.activeTransferWindows), inline: true },
      { name: 'Open slots', value: String(summary.openSlots), inline: true },
      { name: 'Return payout', value: `${reward} $CHARM`, inline: true },
      { name: 'Jackpot', value: `${jackpot} $CHARM Maw Ticket Draw`, inline: true },
      { name: 'Timer warning', value: `The transfer window is ${formatDurationMinutes(event.session_ttl_minutes)} after final confirmation.`, inline: false },
      { name: 'Returned Squigs become Maw Pool inventory', value: 'They may crawl out later through future prizes, games, store rewards, onboarding, draws, or other malformed mechanics.', inline: false }
    );
  if (event.draw_completed) {
    embed.setTitle('THE MAW IS FULL');
    embed.setDescription(`${event.goal_count} / ${event.goal_count} Squigs consumed. The ${jackpot} $CHARM Maw Ticket Draw is complete.`);
  }
  return {
    embeds: [embed],
    components: [
      new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId(`maw_feed_start:${event.id}`)
          .setLabel('Feed the Maw')
          .setStyle(ButtonStyle.Danger)
          .setDisabled(Boolean(event.draw_completed) || summary.openSlots <= 0)
      ),
    ],
  };
}

async function updateMawPanel(eventId) {
  const event = await getMawEventById(eventId);
  if (!event?.panel_channel_id || !event?.panel_message_id || !deps?.client) return false;
  const summary = event.status === 'open' || event.draw_completed
    ? await getMawEventSummary(event)
    : null;
  const channel = await deps.client.channels.fetch(event.panel_channel_id).catch(() => null);
  if (!channel?.isTextBased?.()) return false;
  const message = await channel.messages.fetch(event.panel_message_id).catch(() => null);
  if (!message) return false;
  await message.edit(buildMawPanelPayload(summary)).catch(() => null);
  return true;
}

async function handleMawFeedStart(interaction) {
  const eventId = String(interaction.customId || '').split(':')[1] || '';
  const config = getMawConfig();
  if (!config.mawWalletAddress) {
    await interaction.reply({ content: 'The Maw wallet is not configured yet. Tell an admin it needs MAW_WALLET_ADDRESS.', flags: EPHEMERAL });
    return true;
  }

  await interaction.deferReply({ ephemeral: true });
  await expireAwaitingSessions(interaction.guildId).catch(() => []);
  const event = await getOpenMawEvent(interaction.guildId);
  if (!event || String(event.id) !== eventId) {
    await interaction.editReply({ content: 'The Maw is closed or this panel is stale.' });
    return true;
  }

  const active = await getActiveUserSession(interaction.guildId, interaction.user.id);
  if (active) {
    await interaction.editReply({
      content: `You already have a Maw transfer window open for Squig ${formatToken(active.token_id)}. It expires <t:${Math.floor(new Date(active.expires_at).getTime() / 1000)}:R>.`,
      components: [buildSessionActionRow(active)],
    });
    return true;
  }

  const summary = await getMawEventSummary(event);
  if (summary.openSlots <= 0) {
    await interaction.editReply({ content: 'The Maw is full or all remaining slots are temporarily reserved by active transfer windows.' });
    return true;
  }

  const links = await deps.getWalletLinks(interaction.guildId, interaction.user.id);
  const wallets = links.map((row) => normalizeAddress(row.wallet_address)).filter(Boolean);
  if (!wallets.length) {
    await interaction.editReply({ content: 'You need a linked wallet before feeding the Maw.' });
    return true;
  }

  let eligible = [];
  try {
    eligible = await getEligibleMawSquigsForUser(interaction.guildId, interaction.user.id, wallets, config.squigContract);
  } catch (err) {
    await postAdminLog(interaction.guild, 'Maw Ownership Check', `Failed for <@${interaction.user.id}>: ${String(err?.message || err).slice(0, 800)}`);
    await interaction.editReply({ content: 'The Maw could not check your Squigs right now. Try again in a moment.' });
    return true;
  }

  if (!eligible.length) {
    await interaction.editReply({ content: 'No eligible Squigs found in your linked wallet.' });
    return true;
  }

  const selection = setPendingMawSelection({
    guildId: interaction.guildId,
    userId: interaction.user.id,
    eventId: event.id,
    squigs: eligible,
    page: 0,
  });
  await interaction.editReply({
    content: buildMawSquigSelectContent(selection.squigs, selection.page),
    components: buildMawSquigSelectRows(event.id, interaction.user.id, selection.squigs, selection.page),
  });
  return true;
}

async function getActiveUserSession(guildId, userId, db = null) {
  const pool = db || resolvePool();
  const { rows } = await pool.query(
    `SELECT *
     FROM maw_return_sessions
     WHERE guild_id = $1
       AND discord_user_id = $2
       AND status = 'awaiting_transfer'
       AND expires_at > NOW()
     ORDER BY created_at DESC
     LIMIT 1`,
    [String(guildId), String(userId)]
  );
  return rows[0] || null;
}

async function getEligibleMawSquigsForUser(guildId, userId, wallets, contractAddress) {
  const normalizedWallets = [...new Set(wallets.map(normalizeAddress).filter(Boolean))];
  const contract = normalizeAddress(contractAddress) || DEFAULT_SQUIG_CONTRACT;
  const owned = [];
  for (const wallet of normalizedWallets) {
    const ids = await deps.getOwnedTokenIdsForContractMany([wallet], contract, 'ethereum', { concurrency: 1, suppressErrors: true });
    for (const tokenId of ids) owned.push({ wallet, tokenId: String(tokenId) });
  }

  const pool = resolvePool();
  const [poolRows, sessionRows] = await Promise.all([
    pool.query(`SELECT token_id FROM maw_squig_pool WHERE contract_address = $1 AND status <> 'retired'`, [contract]),
    pool.query(
      `SELECT token_id
       FROM maw_return_sessions
       WHERE guild_id = $1
         AND contract_address = $2
         AND status = 'awaiting_transfer'
         AND expires_at > NOW()`,
      [String(guildId), contract]
    ),
  ]);
  const excluded = new Set([
    ...poolRows.rows.map((row) => String(row.token_id)),
    ...sessionRows.rows.map((row) => String(row.token_id)),
  ]);
  const seen = new Set();
  return sortMawSquigsForDisplay(owned
    .filter((entry) => {
      const token = String(entry.tokenId);
      if (excluded.has(token) || seen.has(token)) return false;
      seen.add(token);
      return true;
    }));
}

async function handleMawSquigPageButton(interaction) {
  const match = String(interaction.customId || '').match(/^maw_select_page:([^:]+):(\d{16,22}):(\d+)$/);
  if (!match) return false;
  const [, eventId, ownerId, rawPage] = match;
  if (String(interaction.user.id) !== ownerId) {
    await interaction.reply({ content: 'This Maw Squig selector is not for you.', flags: EPHEMERAL });
    return true;
  }
  const state = getPendingMawSelection(interaction.guildId, interaction.user.id, eventId);
  if (!state?.squigs?.length) {
    await interaction.update({ content: 'This Maw Squig selector expired. Press Feed the Maw again if the Maw is still hungry.', embeds: [], components: [], attachments: [] });
    return true;
  }
  const event = await getOpenMawEvent(interaction.guildId);
  if (!event || String(event.id) !== String(eventId)) {
    pendingMawSelections.delete(mawSelectionKey(interaction.guildId, interaction.user.id, eventId));
    await interaction.update({ content: 'The Maw is closed or this panel is stale.', embeds: [], components: [], attachments: [] });
    return true;
  }
  state.page = clampMawSquigPage(state.squigs, rawPage);
  await interaction.update({
    content: buildMawSquigSelectContent(state.squigs, state.page),
    embeds: [],
    components: buildMawSquigSelectRows(eventId, interaction.user.id, state.squigs, state.page),
    attachments: [],
  });
  return true;
}

async function handleMawSquigSelect(interaction) {
  const idParts = String(interaction.customId || '').split(':');
  const eventId = idParts[1] || '';
  const ownerId = idParts[2] || '';
  if (ownerId && String(interaction.user.id) !== ownerId) {
    await interaction.reply({ content: 'This Maw Squig selector is not for you.', flags: EPHEMERAL });
    return;
  }
  const selected = String(interaction.values?.[0] || '');
  const match = selected.match(/^(0x[a-fA-F0-9]{40}):(.+)$/);
  if (!match) {
    await interaction.update({ content: 'That Squig selection came out sideways. Try again.', components: [], attachments: [] });
    return;
  }
  const sourceWallet = normalizeAddress(match[1]);
  const tokenId = String(match[2] || '').trim();
  const event = await getOpenMawEvent(interaction.guildId);
  if (!event || String(event.id) !== eventId) {
    await interaction.update({ content: 'The Maw is closed or this panel is stale.', components: [] });
    return;
  }
  const summary = await getMawEventSummary(event);
  if (summary.openSlots <= 0) {
    await interaction.update({ content: 'The Maw is full or all remaining slots are temporarily reserved by active transfer windows.', components: [] });
    return;
  }
  const active = await getActiveUserSession(interaction.guildId, interaction.user.id);
  if (active) {
    await interaction.update({ content: 'You already have an active Maw transfer window.', components: [buildSessionActionRow(active)] });
    return;
  }
  const stillOwned = await deps.getOwnedTokenIdsForContractMany([sourceWallet], getMawConfig().squigContract, 'ethereum', { concurrency: 1, suppressErrors: true })
    .then((ids) => ids.map(String).includes(tokenId))
    .catch(() => false);
  if (!stillOwned) {
    await interaction.update({ content: 'The Maw cannot see that Squig in that linked wallet anymore.', components: [] });
    return;
  }

  const token = randomToken();
  pendingMawSelections.delete(mawSelectionKey(interaction.guildId, interaction.user.id, event.id));
  pendingMawReviews.set(token, {
    guildId: String(interaction.guildId),
    userId: String(interaction.user.id),
    eventId: String(event.id),
    sourceWallet,
    tokenId,
    expiresAt: Date.now() + PENDING_TTL_MS,
  });

  await interaction.update({
    ...buildMawReviewPayload(event, summary, tokenId, token),
  });
}

function getPendingMawReview(token, userId) {
  const pending = pendingMawReviews.get(String(token));
  if (!pending) return null;
  if (pending.expiresAt < Date.now()) {
    pendingMawReviews.delete(String(token));
    return null;
  }
  if (String(pending.userId) !== String(userId)) return null;
  return pending;
}

function buildMawReviewEmbed(event, summary, tokenId, imageUrl = null) {
  const embed = new EmbedBuilder()
    .setTitle('Review Maw Return')
    .setColor(0x8b1e3f)
    .setDescription('Returned Squigs become Malformed Marketplace inventory. They may be reused for future prizes, games, store rewards, onboarding, draws, or other malformed mechanics.')
    .addFields(
      { name: 'Squig', value: formatToken(tokenId), inline: true },
      { name: 'Payout', value: `${formatCharm(event.return_reward_charm)} $CHARM`, inline: true },
      { name: 'Ticket', value: '1 Maw Ticket', inline: true },
      { name: 'Current progress', value: `${event.received_count} / ${event.goal_count}`, inline: true },
      { name: 'Remaining open slots', value: String(summary.openSlots), inline: true }
    );
  if (imageUrl) embed.setImage(imageUrl);
  return embed;
}

function buildMawReviewPayload(event, summary, tokenId, token) {
  const image = mawSquigImageAttachment(tokenId);
  return {
    content: '',
    embeds: [buildMawReviewEmbed(event, summary, tokenId, image.imageUrl)],
    components: [buildMawReviewRow(token)],
    attachments: [],
    ...(image.files.length ? { files: image.files } : {}),
  };
}

function buildMawReviewRow(token) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`maw_review_continue:${token}`)
      .setLabel('Continue')
      .setStyle(ButtonStyle.Danger),
    new ButtonBuilder()
      .setCustomId(`maw_cancel:${token}`)
      .setLabel('Cancel')
      .setStyle(ButtonStyle.Secondary)
  );
}

async function handleMawReviewContinue(interaction) {
  const token = String(interaction.customId || '').split(':')[1] || '';
  const pending = getPendingMawReview(token, interaction.user.id);
  if (!pending) {
    await interaction.update({ content: 'This Maw review expired. Start again if the Maw is still hungry.', embeds: [], components: [], attachments: [] });
    return true;
  }
  const event = await getMawEventById(pending.eventId);
  if (!event || event.status !== 'open') {
    await interaction.update({ content: 'The Maw closed before the timer started.', embeds: [], components: [], attachments: [] });
    return true;
  }
  const embed = new EmbedBuilder()
    .setTitle('Final Confirmation')
    .setColor(0xd9480f)
    .setDescription(
      `You are about to start a ${formatDurationMinutes(event.session_ttl_minutes)} transfer window for Squig ${formatToken(pending.tokenId)}. ` +
      `Do not click confirm unless you are ready to send the Squig now. After you click confirm, the bot will reveal the official Maw wallet and begin watching for the transfer automatically.`
    )
    .addFields(
      { name: `Only Squig ${formatToken(pending.tokenId)} counts for this session.`, value: 'No substitutions. The Maw reads receipts.', inline: false },
      { name: 'If the timer expires before the Maw receives it, the session closes and the slot is released.', value: 'Late transfers go to manual review.', inline: false },
      { name: 'Payout after verified receipt', value: `${formatCharm(event.return_reward_charm)} $CHARM`, inline: true },
      { name: 'Maw Ticket after verified receipt', value: '1', inline: true }
    );
  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`maw_confirm_start_timer:${token}`)
      .setLabel(`Confirm - Start ${event.session_ttl_minutes} Minute Timer`)
      .setStyle(ButtonStyle.Danger),
    new ButtonBuilder()
      .setCustomId(`maw_cancel:${token}`)
      .setLabel('Cancel')
      .setStyle(ButtonStyle.Secondary)
  );
  await interaction.update({ embeds: [embed], components: [row], attachments: [] });
  return true;
}

async function handleMawPendingCancel(interaction) {
  const token = String(interaction.customId || '').split(':')[1] || '';
  pendingMawReviews.delete(token);
  await interaction.update({ content: 'Cancelled. The Maw stopped drooling for now.', embeds: [], components: [], attachments: [] });
  return true;
}

async function handleMawConfirmStartTimer(interaction) {
  const token = String(interaction.customId || '').split(':')[1] || '';
  const pending = getPendingMawReview(token, interaction.user.id);
  if (!pending) {
    await interaction.update({ content: 'This Maw confirmation expired. Start again if the Maw is still hungry.', embeds: [], components: [], attachments: [] });
    return true;
  }
  await interaction.deferUpdate();
  const result = await createMawReturnSession(pending);
  if (!result.ok) {
    await interaction.editReply({ content: result.reason || 'The Maw could not start that transfer window.', embeds: [], components: [], attachments: [] });
    return true;
  }
  pendingMawReviews.delete(token);
  await updateMawPanel(result.event.id).catch(() => null);
  await interaction.editReply({
    content: '',
    embeds: [buildMawSessionEmbed(result.event, result.session, getMawConfig())],
    components: [buildSessionActionRow(result.session)],
    attachments: [],
  });
  return true;
}

async function createMawReturnSession(pending) {
  const config = getMawConfig();
  if (!config.mawWalletAddress) return { ok: false, reason: 'The Maw wallet is not configured.' };
  const pool = resolvePool();
  if (!pool.connect) throw new Error('Maw database pool does not support transactions.');
  const db = await pool.connect();
  let done = false;
  try {
    await db.query('BEGIN');
    await db.query('SELECT pg_advisory_xact_lock($1::bigint)', [
      advisoryLockKey(`maw-session:${pending.guildId}:${pending.userId}`),
    ]);
    await expireAwaitingSessions(pending.guildId, { db });

    const eventRows = await db.query(
      `SELECT * FROM maw_events WHERE id = $1 AND guild_id = $2 AND status = 'open' FOR UPDATE`,
      [String(pending.eventId), String(pending.guildId)]
    );
    const event = eventRows.rows[0];
    if (!event) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'The Maw closed before the timer started.' };
    }
    const activeUser = await db.query(
      `SELECT id FROM maw_return_sessions
       WHERE guild_id = $1 AND discord_user_id = $2 AND status = 'awaiting_transfer' AND expires_at > NOW()
       LIMIT 1`,
      [String(pending.guildId), String(pending.userId)]
    );
    if (activeUser.rows[0]) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'You already have an active Maw transfer window.' };
    }
    const activeWindows = await countActiveTransferWindows(event.id, db);
    const openSlots = calculateMawOpenSlots({
      goalCount: event.goal_count,
      receivedCount: event.received_count,
      activeTransferWindows: activeWindows,
    });
    if (openSlots <= 0) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'The Maw is full or all remaining slots are temporarily reserved by active transfer windows.' };
    }
    const existingPool = await db.query(
      `SELECT id FROM maw_squig_pool WHERE contract_address = $1 AND token_id = $2 AND status <> 'retired' LIMIT 1 FOR UPDATE`,
      [config.squigContract, String(pending.tokenId)]
    );
    if (existingPool.rows[0]) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'That Squig is already in the Maw Pool.' };
    }
    const tokenActive = await db.query(
      `SELECT id FROM maw_return_sessions
       WHERE guild_id = $1 AND contract_address = $2 AND token_id = $3 AND status = 'awaiting_transfer' AND expires_at > NOW()
       LIMIT 1`,
      [String(pending.guildId), config.squigContract, String(pending.tokenId)]
    );
    if (tokenActive.rows[0]) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'That Squig already has an active Maw transfer window.' };
    }

    const inserted = await db.query(
      `INSERT INTO maw_return_sessions
         (event_id, guild_id, discord_user_id, source_wallet, contract_address, token_id, status, expires_at, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, 'awaiting_transfer', NOW() + ($7::int * INTERVAL '1 minute'), NOW(), NOW())
       RETURNING *`,
      [
        String(event.id),
        String(pending.guildId),
        String(pending.userId),
        normalizeAddress(pending.sourceWallet),
        config.squigContract,
        String(pending.tokenId),
        Math.floor(Number(event.session_ttl_minutes) || config.sessionTtlMinutes),
      ]
    );

    await db.query('COMMIT');
    done = true;
    return { ok: true, event, session: inserted.rows[0] };
  } catch (err) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    if (/maw_return_sessions_active_user_uidx/i.test(String(err?.message || ''))) {
      return { ok: false, reason: 'You already have an active Maw transfer window.' };
    }
    if (/maw_return_sessions_active_token_uidx/i.test(String(err?.message || ''))) {
      return { ok: false, reason: 'That Squig already has an active Maw transfer window.' };
    }
    throw err;
  } finally {
    db.release();
  }
}

function buildMawSessionEmbed(event, session, config) {
  return new EmbedBuilder()
    .setTitle('Maw Session Created')
    .setColor(0x8b1e3f)
    .setDescription(`Send only Squig ${formatToken(session.token_id)} to the official Maw wallet below. The bot is now watching for the transfer automatically.`)
    .addFields(
      { name: 'Squig', value: formatToken(session.token_id), inline: true },
      { name: 'Payout', value: `${formatCharm(event.return_reward_charm)} $CHARM`, inline: true },
      { name: 'Ticket pending', value: '1 Maw Ticket', inline: true },
      { name: 'Status', value: 'Awaiting transfer', inline: true },
      { name: 'Expires', value: `<t:${Math.floor(new Date(session.expires_at).getTime() / 1000)}:R>`, inline: true },
      { name: 'Maw wallet address', value: `\`${config.mawWalletAddress}\``, inline: false }
    );
}

function buildSessionActionRow(session) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`maw_refresh_session:${session.id}`)
      .setLabel('Refresh Status')
      .setStyle(ButtonStyle.Secondary),
    new ButtonBuilder()
      .setCustomId(`maw_cancel_session:${session.id}`)
      .setLabel('Cancel Session')
      .setStyle(ButtonStyle.Danger)
      .setDisabled(String(session.status) !== 'awaiting_transfer')
  );
}

async function handleMawRefreshSession(interaction) {
  const sessionId = String(interaction.customId || '').split(':')[1] || '';
  const session = await getSessionForUser(sessionId, interaction.user.id);
  if (!session) {
    await interaction.reply({ content: 'That Maw session was not found.', flags: EPHEMERAL });
    return true;
  }
  const event = await getMawEventById(session.event_id);
  await interaction.reply({
    embeds: [buildSessionStatusEmbed(event, session)],
    flags: EPHEMERAL,
  });
  return true;
}

async function handleMawCancelSession(interaction) {
  const sessionId = String(interaction.customId || '').split(':')[1] || '';
  const pool = resolvePool();
  const { rows } = await pool.query(
    `UPDATE maw_return_sessions
     SET status = 'cancelled',
         updated_at = NOW()
     WHERE id = $1
       AND discord_user_id = $2
       AND status = 'awaiting_transfer'
     RETURNING *`,
    [String(sessionId), String(interaction.user.id)]
  );
  if (!rows[0]) {
    await interaction.reply({ content: 'That session cannot be cancelled now. If the Maw already received it, the chewing has begun.', flags: EPHEMERAL });
    return true;
  }
  await updateMawPanel(rows[0].event_id).catch(() => null);
  await interaction.reply({ content: 'Maw session cancelled. No Squig was received, and no $CHARM was paid.', flags: EPHEMERAL });
  return true;
}

async function getSessionForUser(sessionId, userId) {
  const { rows } = await resolvePool().query(
    `SELECT * FROM maw_return_sessions WHERE id = $1 AND discord_user_id = $2 LIMIT 1`,
    [String(sessionId), String(userId)]
  );
  return rows[0] || null;
}

function buildSessionStatusEmbed(event, session) {
  const fields = [
    { name: 'Squig', value: formatToken(session.token_id), inline: true },
    { name: 'Status', value: String(session.status), inline: true },
  ];
  if (String(session.status) === 'awaiting_transfer') {
    fields.push({ name: 'Expires', value: `<t:${Math.floor(new Date(session.expires_at).getTime() / 1000)}:R>`, inline: true });
  }
  if (session.ticket_id) fields.push({ name: 'Maw Ticket', value: `#${session.ticket_id}`, inline: true });
  if (session.payout_status) fields.push({ name: 'Payout', value: String(session.payout_status), inline: true });
  return new EmbedBuilder()
    .setTitle(event ? 'Maw Session Status' : 'Maw Session')
    .setColor(0x8b1e3f)
    .addFields(fields);
}

async function handleSquigPrizeCommand(interaction) {
  if (!isAdmin(interaction)) {
    await interaction.reply({ content: 'Admin only.', flags: EPHEMERAL });
    return;
  }
  const claimId = interaction.options.getInteger('claim_id');
  if (claimId) {
    await interaction.deferReply({ ephemeral: true });
    const result = await markPrizeDelivered({
      claimId,
      actorId: interaction.user.id,
      txHash: interaction.options.getString('tx') || null,
      guild: interaction.guild,
    });
    await interaction.editReply({ content: result.reason || (result.ok ? `Claim ${claimId} marked delivered.` : 'Could not mark delivered.') });
    return;
  }

  const target = interaction.options.getUser('user');
  if (!target) {
    await interaction.reply({ content: 'Use /squigprize with a user to award, or claim_id to mark delivered.', flags: EPHEMERAL });
    return;
  }
  await interaction.deferReply({ ephemeral: true });
  const reason = interaction.options.getString('reason') || null;
  const result = await createSquigPrizeClaim(interaction.guildId, target, interaction.user.id, reason);
  if (!result.ok) {
    await interaction.editReply({ content: result.reason || 'The Maw could not cough up a Squig.' });
    return;
  }
  const message = await interaction.channel.send(buildPrizeOfferPayload(result.claim, result.squig));
  await resolvePool().query(
    `UPDATE squig_prize_claims
     SET offer_channel_id = $2,
         offer_message_id = $3,
         updated_at = NOW()
     WHERE id = $1`,
    [String(result.claim.id), String(message.channel.id), String(message.id)]
  );
  await interaction.editReply({ content: `Squig ${formatToken(result.squig.token_id)} offered to <@${target.id}>. Claim ID: ${result.claim.id}.` });
}

async function createSquigPrizeClaim(guildId, targetUser, awardedById, reason = null) {
  const links = await deps.getWalletLinks(guildId, targetUser.id);
  const wallets = links.map((row) => normalizeAddress(row.wallet_address)).filter(Boolean);
  if (!wallets.length) {
    return { ok: false, reason: 'Target user needs a linked wallet before the Maw can offer a Squig.' };
  }
  const pool = resolvePool();
  const db = await pool.connect();
  let done = false;
  try {
    await db.query('BEGIN');
    await db.query('SELECT pg_advisory_xact_lock($1::bigint)', [advisoryLockKey(`squig-prize-award:${guildId}:${targetUser.id}`)]);
    const squig = await selectEligiblePrizeSquigForUpdate(db, targetUser.id, wallets, []);
    if (!squig) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'No eligible Maw Pool Squigs are available for that user.' };
    }
    const claimRows = await db.query(
      `INSERT INTO squig_prize_claims
         (guild_id, winner_discord_id, awarded_by_discord_id, reason, current_pool_squig_id, status, expires_at, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, 'offered', NOW() + ($6::int * INTERVAL '1 hour'), NOW(), NOW())
       RETURNING *`,
      [String(guildId), String(targetUser.id), String(awardedById), reason, String(squig.id), CLAIM_TTL_HOURS]
    );
    const claim = claimRows.rows[0];
    await db.query(
      `UPDATE maw_squig_pool
       SET status = 'reserved_for_claim',
           reserved_claim_id = $2,
           times_offered = times_offered + 1,
           updated_at = NOW()
       WHERE id = $1`,
      [String(squig.id), String(claim.id)]
    );
    await insertPrizeHistory(db, claim.id, squig.id, 'initial_offer', null, awardedById, reason || 'initial offer');
    await db.query('COMMIT');
    done = true;
    return { ok: true, claim, squig };
  } catch (err) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    db.release();
  }
}

async function selectEligiblePrizeSquigForUpdate(db, targetUserId, targetWallets = [], excludedPoolIds = []) {
  const walletSet = normalizeWalletSet(targetWallets);
  const excluded = (Array.isArray(excludedPoolIds) ? excludedPoolIds : [excludedPoolIds]).map((v) => String(v)).filter(Boolean);
  const walletList = [...walletSet];
  const params = [String(targetUserId), walletList];
  let excludedClause = '';
  if (excluded.length) {
    params.push(excluded);
    excludedClause = ` AND id <> ALL($${params.length}::bigint[])`;
  }
  const { rows } = await db.query(
    `SELECT *
     FROM maw_squig_pool
     WHERE status = 'available'
       AND original_sender_discord_id <> $1
       AND NOT (LOWER(original_sender_wallet) = ANY($2::text[]))
       ${excludedClause}
     ORDER BY random()
     LIMIT 1
     FOR UPDATE SKIP LOCKED`,
    params
  );
  return rows[0] || null;
}

async function insertPrizeHistory(db, claimId, poolSquigId, action, charmDelta = null, actorDiscordId = null, note = null) {
  await db.query(
    `INSERT INTO squig_prize_claim_history
       (claim_id, pool_squig_id, action, charm_delta, actor_discord_id, note, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, NOW())`,
    [
      String(claimId),
      poolSquigId == null ? null : String(poolSquigId),
      String(action),
      charmDelta == null ? null : Math.floor(Number(charmDelta) || 0),
      actorDiscordId == null ? null : String(actorDiscordId),
      note,
    ]
  );
}

function buildPrizeOfferPayload(claim, squig, disabled = false) {
  const config = getMawConfig();
  const embed = new EmbedBuilder()
    .setTitle('A Squig has crawled out of the Maw')
    .setColor(0x5f3dc4)
    .setDescription(`<@${claim.winner_discord_id}> has won a Squig from the Maw Pool.`)
    .addFields(
      { name: 'Offer', value: `Squig ${formatToken(squig.token_id)}`, inline: true },
      {
        name: 'Options',
        value:
          `Accept Squig\n` +
          `Take ${formatCharm(config.prizeCashoutCharm)} $CHARM instead\n` +
          `Reroll for ${formatCharm(config.rerollCostCharm)} $CHARM`,
        inline: false,
      },
      { name: 'Claim ID', value: String(claim.id), inline: true }
    );
  if (claim.expires_at && !disabled) {
    embed.addFields({ name: 'Expires', value: `<t:${Math.floor(new Date(claim.expires_at).getTime() / 1000)}:R>`, inline: true });
  }
  return {
    embeds: [embed],
    components: [buildPrizeOfferRow(claim, disabled)],
  };
}

function buildPrizeOfferRow(claim, disabled = false) {
  const config = getMawConfig();
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`maw_prize_accept:${claim.id}`)
      .setLabel('Accept Squig')
      .setStyle(ButtonStyle.Success)
      .setDisabled(disabled),
    new ButtonBuilder()
      .setCustomId(`maw_prize_cashout:${claim.id}`)
      .setLabel(`Take ${formatCharm(config.prizeCashoutCharm)} $CHARM`)
      .setStyle(ButtonStyle.Secondary)
      .setDisabled(disabled),
    new ButtonBuilder()
      .setCustomId(`maw_prize_reroll:${claim.id}`)
      .setLabel(`Reroll - ${formatCharm(config.rerollCostCharm)} $CHARM`)
      .setStyle(ButtonStyle.Danger)
      .setDisabled(disabled || Number(claim.reroll_count || 0) >= config.maxRerolls)
  );
}

async function getPrizeClaimWithSquig(claimId) {
  const { rows } = await resolvePool().query(
    `SELECT c.*, p.token_id, p.contract_address, p.original_sender_discord_id, p.original_sender_wallet, p.status AS pool_status
     FROM squig_prize_claims c
     LEFT JOIN maw_squig_pool p ON p.id = c.current_pool_squig_id
     WHERE c.id = $1
     LIMIT 1`,
    [String(claimId)]
  );
  return rows[0] || null;
}

function parseClaimId(customId) {
  return String(customId || '').split(':')[1] || '';
}

async function ensurePrizeTarget(interaction, claim) {
  if (!claim) {
    await interaction.reply({ content: 'That Squig prize claim was not found.', flags: EPHEMERAL });
    return false;
  }
  if (String(claim.winner_discord_id) !== String(interaction.user.id)) {
    await interaction.reply({ content: 'This Squig is not chewing on you.', flags: EPHEMERAL });
    return false;
  }
  if (String(claim.status) !== 'offered') {
    await interaction.reply({ content: 'That Squig offer is no longer active.', flags: EPHEMERAL });
    return false;
  }
  if (new Date(claim.expires_at).getTime() < Date.now()) {
    await expirePrizeClaims(interaction.guildId).catch(() => null);
    await interaction.reply({ content: 'That Squig offer expired and slithered back into the Maw.', flags: EPHEMERAL });
    return false;
  }
  return true;
}

async function handlePrizeAccept(interaction) {
  const claim = await getPrizeClaimWithSquig(parseClaimId(interaction.customId));
  if (!(await ensurePrizeTarget(interaction, claim))) return true;
  const links = await deps.getWalletLinks(interaction.guildId, interaction.user.id);
  const wallet = links.map((row) => normalizeAddress(row.wallet_address)).find(Boolean);
  if (!wallet) {
    await interaction.reply({ content: 'You need a linked wallet before accepting a Squig.', flags: EPHEMERAL });
    return true;
  }
  const embed = new EmbedBuilder()
    .setTitle('Accept Squig')
    .setColor(0x2f9e44)
    .setDescription(`Accept Squig ${formatToken(claim.token_id)} for manual delivery to your linked wallet?`)
    .addFields(
      { name: 'Wallet', value: `\`${wallet}\``, inline: false },
      { name: 'Delivery', value: 'Manual/admin-confirmed. No private-key transfer system lives here.', inline: false }
    );
  await interaction.reply({
    embeds: [embed],
    components: [buildPrizeConfirmRow(`maw_prize_accept_confirm:${claim.id}`)],
    flags: EPHEMERAL,
  });
  return true;
}

function buildPrizeConfirmRow(confirmCustomId) {
  const claimId = String(confirmCustomId || '').split(':')[1] || '';
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(confirmCustomId)
      .setLabel('Confirm')
      .setStyle(ButtonStyle.Success),
    new ButtonBuilder()
      .setCustomId(`maw_prize_cancel_confirm:${claimId}`)
      .setLabel('Cancel')
      .setStyle(ButtonStyle.Secondary)
  );
}

async function handlePrizeConfirmCancel(interaction) {
  await interaction.update({ content: 'Cancelled.', embeds: [], components: [] });
  return true;
}

async function handlePrizeAcceptConfirm(interaction) {
  const claimId = parseClaimId(interaction.customId);
  const lockKey = `accept:${claimId}`;
  if (activePrizeLocks.has(lockKey)) {
    await interaction.reply({ content: 'That Squig is already being handled. Give it a second to crawl.', flags: EPHEMERAL });
    return true;
  }
  activePrizeLocks.add(lockKey);
  try {
    const links = await deps.getWalletLinks(interaction.guildId, interaction.user.id);
    const wallet = links.map((row) => normalizeAddress(row.wallet_address)).find(Boolean);
    if (!wallet) {
      await interaction.update({ content: 'You need a linked wallet before accepting a Squig.', embeds: [], components: [] });
      return true;
    }
    const result = await acceptPrizeClaim(claimId, interaction.user.id, wallet);
    if (!result.ok) {
      await interaction.update({ content: result.reason || 'That Squig offer is no longer active.', embeds: [], components: [] });
      return true;
    }
    await updatePrizeOfferMessage(result.claim.id, true).catch(() => null);
    await postPrizeDeliveryNotice(interaction.guild, result.claim, result.squig, wallet).catch(() => null);
    await interaction.update({ content: `You accepted Squig ${formatToken(result.squig.token_id)}. The team has been notified for delivery.`, embeds: [], components: [] });
  } finally {
    activePrizeLocks.delete(lockKey);
  }
  return true;
}

async function acceptPrizeClaim(claimId, userId, wallet) {
  const db = await resolvePool().connect();
  let done = false;
  try {
    await db.query('BEGIN');
    const claimRows = await db.query(`SELECT * FROM squig_prize_claims WHERE id = $1 FOR UPDATE`, [String(claimId)]);
    const claim = claimRows.rows[0];
    if (!claim || String(claim.winner_discord_id) !== String(userId) || String(claim.status) !== 'offered') {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'That Squig offer is no longer active.' };
    }
    const poolRows = await db.query(`SELECT * FROM maw_squig_pool WHERE id = $1 FOR UPDATE`, [String(claim.current_pool_squig_id)]);
    const squig = poolRows.rows[0];
    if (!squig || String(squig.status) !== 'reserved_for_claim') {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'That Squig is not available for this claim anymore.' };
    }
    const updatedClaim = await db.query(
      `UPDATE squig_prize_claims
       SET status = 'accepted_pending_delivery',
           updated_at = NOW()
       WHERE id = $1
       RETURNING *`,
      [String(claim.id)]
    );
    await db.query(
      `UPDATE maw_squig_pool
       SET status = 'accepted_pending_delivery',
           delivered_to_discord_id = $2,
           delivered_to_wallet = $3,
           updated_at = NOW()
       WHERE id = $1`,
      [String(squig.id), String(userId), normalizeAddress(wallet)]
    );
    await insertPrizeHistory(db, claim.id, squig.id, 'accepted', null, userId, `wallet ${normalizeAddress(wallet)}`);
    await db.query('COMMIT');
    done = true;
    return { ok: true, claim: updatedClaim.rows[0], squig: { ...squig, delivered_to_wallet: normalizeAddress(wallet) } };
  } catch (err) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    db.release();
  }
}

async function postPrizeDeliveryNotice(guild, claim, squig, wallet) {
  const channel = await resolveAdminChannel(guild, null);
  if (!channel?.isTextBased?.()) return;
  const embed = new EmbedBuilder()
    .setTitle('Squig Prize Delivery Needed')
    .setColor(0xe6b422)
    .addFields(
      { name: 'Winner', value: `<@${claim.winner_discord_id}>`, inline: true },
      { name: 'Squig', value: formatToken(squig.token_id), inline: true },
      { name: 'Claim ID', value: String(claim.id), inline: true },
      { name: 'Wallet', value: `\`${wallet}\``, inline: false },
      { name: 'Reason', value: claim.reason || 'No reason provided.', inline: false }
    );
  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`maw_prize_delivered:${claim.id}`)
      .setLabel('Mark Delivered')
      .setStyle(ButtonStyle.Success)
  );
  await channel.send({ embeds: [embed], components: [row] });
}

async function handlePrizeCashout(interaction) {
  const claim = await getPrizeClaimWithSquig(parseClaimId(interaction.customId));
  if (!(await ensurePrizeTarget(interaction, claim))) return true;
  const config = getMawConfig();
  const embed = new EmbedBuilder()
    .setTitle('Take $CHARM Instead')
    .setColor(0xe6b422)
    .setDescription(`Take ${formatCharm(config.prizeCashoutCharm)} $CHARM instead of Squig ${formatToken(claim.token_id)}?`)
    .addFields({ name: 'The Squig', value: `Squig ${formatToken(claim.token_id)} slithers back into the Maw.`, inline: false });
  await interaction.reply({
    embeds: [embed],
    components: [buildPrizeConfirmRow(`maw_prize_cashout_confirm:${claim.id}`)],
    flags: EPHEMERAL,
  });
  return true;
}

async function handlePrizeCashoutConfirm(interaction) {
  const claimId = parseClaimId(interaction.customId);
  const lockKey = `cashout:${claimId}`;
  if (activePrizeLocks.has(lockKey)) {
    await interaction.reply({ content: 'That cashout is already being handled.', flags: EPHEMERAL });
    return true;
  }
  activePrizeLocks.add(lockKey);
  try {
    let result;
    try {
      result = await cashoutPrizeClaim(claimId, interaction.user.id);
    } catch (err) {
      await postAdminLog(interaction.guild, 'Squig Prize Cashout Failure', `Claim ${claimId}: ${String(err?.message || err).slice(0, 800)}`);
      await interaction.update({ content: 'The cashout needs manual review. The team has been notified.', embeds: [], components: [] }).catch(() => null);
      return true;
    }
    if (!result.ok) {
      await interaction.update({ content: result.reason || 'That Squig offer is no longer active.', embeds: [], components: [] });
      return true;
    }
    await updatePrizeOfferMessage(result.claim.id, true).catch(() => null);
    await interaction.update({ content: `You took ${formatCharm(result.amount)} $CHARM instead of Squig ${formatToken(result.squig.token_id)}.`, embeds: [], components: [] });
    await postToOfferChannel(result.claim, `<@${interaction.user.id}> took ${formatCharm(result.amount)} $CHARM instead of Squig ${formatToken(result.squig.token_id)}. Squig ${formatToken(result.squig.token_id)} slithered back into the Maw.`).catch(() => null);
  } finally {
    activePrizeLocks.delete(lockKey);
  }
  return true;
}

async function cashoutPrizeClaim(claimId, userId) {
  const config = getMawConfig();
  const pool = resolvePool();
  const db = await pool.connect();
  let claim;
  let squig;
  let done = false;
  try {
    await db.query('BEGIN');
    const claimRows = await db.query(`SELECT * FROM squig_prize_claims WHERE id = $1 FOR UPDATE`, [String(claimId)]);
    claim = claimRows.rows[0];
    if (!claim || String(claim.winner_discord_id) !== String(userId) || String(claim.status) !== 'offered') {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'That Squig offer is no longer active.' };
    }
    const poolRows = await db.query(`SELECT * FROM maw_squig_pool WHERE id = $1 FOR UPDATE`, [String(claim.current_pool_squig_id)]);
    squig = poolRows.rows[0];
    if (!squig || String(squig.status) !== 'reserved_for_claim') {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'That Squig is not available for cashout anymore.' };
    }
    const updatedClaim = await db.query(
      `UPDATE squig_prize_claims
       SET status = 'cashed_out',
           cashout_amount = $2,
           cashout_payout_reference = 'processing',
           updated_at = NOW()
       WHERE id = $1
       RETURNING *`,
      [String(claim.id), config.prizeCashoutCharm]
    );
    await db.query(
      `UPDATE maw_squig_pool
       SET status = 'available',
           reserved_claim_id = NULL,
           times_cashed_out = times_cashed_out + 1,
           updated_at = NOW()
       WHERE id = $1`,
      [String(squig.id)]
    );
    await insertPrizeHistory(db, claim.id, squig.id, 'cashed_out', config.prizeCashoutCharm, userId, 'cashout selected');
    await db.query('COMMIT');
    done = true;
    claim = updatedClaim.rows[0];
  } catch (err) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    db.release();
  }

  try {
    const payout = await awardCharmToUser(claim.guild_id, userId, config.prizeCashoutCharm, 'maw_prize_cashout');
    await pool.query(
      `UPDATE squig_prize_claims SET cashout_payout_reference = $2, updated_at = NOW() WHERE id = $1`,
      [String(claim.id), payoutReference(payout)]
    );
  } catch (err) {
    await pool.query(
      `UPDATE squig_prize_claims SET status = 'manual_review', cashout_payout_reference = $2, updated_at = NOW() WHERE id = $1`,
      [String(claim.id), `failed:${String(err?.message || err).slice(0, 300)}`]
    ).catch(() => null);
    throw err;
  }
  return { ok: true, claim, squig, amount: config.prizeCashoutCharm };
}

async function handlePrizeReroll(interaction) {
  const claim = await getPrizeClaimWithSquig(parseClaimId(interaction.customId));
  if (!(await ensurePrizeTarget(interaction, claim))) return true;
  const config = getMawConfig();
  if (Number(claim.reroll_count || 0) >= config.maxRerolls) {
    await interaction.reply({ content: `The Maw will not reroll this one again. Max rerolls: ${config.maxRerolls}.`, flags: EPHEMERAL });
    return true;
  }
  const precheck = await precheckReroll(interaction.guildId, interaction.user.id, claim.id);
  if (!precheck.ok) {
    await interaction.reply({ content: precheck.reason, flags: EPHEMERAL });
    return true;
  }
  const balance = await marketplaceCommand.checkUserCharmBalance(deps, interaction.guildId, interaction.user.id, config.rerollCostCharm);
  if (!balance.ok) {
    await interaction.reply({ content: balance.reason || 'Could not verify your $CHARM balance right now. You were not charged.', flags: EPHEMERAL });
    return true;
  }
  if (!balance.hasEnough) {
    await interaction.reply({ content: `You need ${formatCharm(config.rerollCostCharm)} $CHARM to reroll. You were not charged.`, flags: EPHEMERAL });
    return true;
  }
  const embed = new EmbedBuilder()
    .setTitle('Reroll Squig')
    .setColor(0xd9480f)
    .setDescription(`Pay ${formatCharm(config.rerollCostCharm)} $CHARM and ask the Maw for another Squig?`)
    .addFields(
      { name: 'Current offer', value: `Squig ${formatToken(claim.token_id)}`, inline: true },
      { name: 'Rerolls used', value: `${claim.reroll_count} / ${config.maxRerolls}`, inline: true }
    );
  await interaction.reply({
    embeds: [embed],
    components: [buildPrizeConfirmRow(`maw_prize_reroll_confirm:${claim.id}`)],
    flags: EPHEMERAL,
  });
  return true;
}

async function precheckReroll(guildId, userId, claimId) {
  const claim = await getPrizeClaimWithSquig(claimId);
  const config = getMawConfig();
  if (!claim || String(claim.winner_discord_id) !== String(userId) || String(claim.status) !== 'offered') {
    return { ok: false, reason: 'That Squig offer is no longer active.' };
  }
  if (Number(claim.reroll_count || 0) >= config.maxRerolls) {
    return { ok: false, reason: `The Maw will not reroll this one again. Max rerolls: ${config.maxRerolls}.` };
  }
  const links = await deps.getWalletLinks(guildId, userId);
  const wallets = links.map((row) => normalizeAddress(row.wallet_address)).filter(Boolean);
  const excluded = await getClaimOfferedPoolIds(claimId);
  const rows = await resolvePool().query(`SELECT * FROM maw_squig_pool WHERE status = 'available'`);
  const eligible = filterEligiblePrizeSquigsForUser(rows.rows, userId, wallets, excluded);
  if (!eligible.length) {
    return { ok: false, reason: 'No eligible reroll Squigs are available. You were not charged.' };
  }
  return { ok: true };
}

async function getClaimOfferedPoolIds(claimId, db = null) {
  const pool = db || resolvePool();
  const { rows } = await pool.query(
    `SELECT DISTINCT pool_squig_id
     FROM squig_prize_claim_history
     WHERE claim_id = $1
       AND pool_squig_id IS NOT NULL`,
    [String(claimId)]
  );
  return rows.map((row) => String(row.pool_squig_id));
}

async function handlePrizeRerollConfirm(interaction) {
  const claimId = parseClaimId(interaction.customId);
  const lockKey = `reroll:${claimId}`;
  if (activePrizeLocks.has(lockKey)) {
    await interaction.reply({ content: 'That reroll is already chewing. Give it a second.', flags: EPHEMERAL });
    return true;
  }
  activePrizeLocks.add(lockKey);
  try {
    const config = getMawConfig();
    const balance = await marketplaceCommand.checkUserCharmBalance(deps, interaction.guildId, interaction.user.id, config.rerollCostCharm);
    if (!balance.ok) {
      await interaction.update({ content: balance.reason || 'Could not verify your $CHARM balance right now. You were not charged.', embeds: [], components: [] });
      return true;
    }
    if (!balance.hasEnough) {
      await interaction.update({ content: `You need ${formatCharm(config.rerollCostCharm)} $CHARM to reroll. You were not charged.`, embeds: [], components: [] });
      return true;
    }
    let result;
    try {
      result = await rerollPrizeClaim(claimId, interaction.user.id, balance.spendable);
    } catch (err) {
      await postAdminLog(interaction.guild, 'Squig Prize Reroll Failure', `Claim ${claimId}: ${String(err?.message || err).slice(0, 800)}`);
      await interaction.update({ content: 'The reroll failed before the Maw could finish chewing. You were not charged if the $CHARM transfer did not complete.', embeds: [], components: [] }).catch(() => null);
      return true;
    }
    if (!result.ok) {
      await interaction.update({ content: result.reason || 'No eligible reroll Squigs are available. You were not charged.', embeds: [], components: [] });
      return true;
    }
    await updatePrizeOfferMessage(result.claim.id, false).catch(() => null);
    const message =
      `The Maw coughed up another one. Previous offer: Squig ${formatToken(result.oldSquig.token_id)}. ` +
      `New offer: Squig ${formatToken(result.newSquig.token_id)}. Reroll cost paid: ${formatCharm(config.rerollCostCharm)} $CHARM.`;
    await interaction.update({ content: message, embeds: [], components: [] });
    await postToOfferChannel(result.claim, message).catch(() => null);
  } finally {
    activePrizeLocks.delete(lockKey);
  }
  return true;
}

async function rerollPrizeClaim(claimId, userId, spendable) {
  const config = getMawConfig();
  const pool = resolvePool();
  const db = await pool.connect();
  let done = false;
  try {
    await db.query('BEGIN');
    const claimRows = await db.query(`SELECT * FROM squig_prize_claims WHERE id = $1 FOR UPDATE`, [String(claimId)]);
    const claim = claimRows.rows[0];
    if (!claim || String(claim.winner_discord_id) !== String(userId) || String(claim.status) !== 'offered') {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'That Squig offer is no longer active.' };
    }
    if (Number(claim.reroll_count || 0) >= config.maxRerolls) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: `The Maw will not reroll this one again. Max rerolls: ${config.maxRerolls}.` };
    }
    const claimLinks = await deps.getWalletLinks(claim.guild_id, userId);
    const targetWallets = claimLinks.map((row) => normalizeAddress(row.wallet_address)).filter(Boolean);
    const excluded = await getClaimOfferedPoolIds(claim.id, db);
    const oldRows = await db.query(`SELECT * FROM maw_squig_pool WHERE id = $1 FOR UPDATE`, [String(claim.current_pool_squig_id)]);
    const oldSquig = oldRows.rows[0];
    const newSquig = await selectEligiblePrizeSquigForUpdate(db, userId, targetWallets, excluded);
    if (!newSquig) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'No eligible reroll Squigs are available. You were not charged.' };
    }

    await marketplaceCommand.transferCharmToMarketplace(
      deps,
      claim.guild_id,
      userId,
      { price: config.rerollCostCharm, name: 'Maw Squig reroll' },
      spendable
    );

    await db.query(
      `UPDATE maw_squig_pool
       SET status = 'available',
           reserved_claim_id = NULL,
           times_rerolled_away = times_rerolled_away + 1,
           updated_at = NOW()
       WHERE id = $1`,
      [String(oldSquig.id)]
    );
    await db.query(
      `UPDATE maw_squig_pool
       SET status = 'reserved_for_claim',
           reserved_claim_id = $2,
           times_offered = times_offered + 1,
           updated_at = NOW()
       WHERE id = $1`,
      [String(newSquig.id), String(claim.id)]
    );
    const updatedClaim = await db.query(
      `UPDATE squig_prize_claims
       SET current_pool_squig_id = $2,
           reroll_count = reroll_count + 1,
           updated_at = NOW()
       WHERE id = $1
       RETURNING *`,
      [String(claim.id), String(newSquig.id)]
    );
    await insertPrizeHistory(db, claim.id, oldSquig.id, 'rerolled_away', -config.rerollCostCharm, userId, `rerolled to pool squig ${newSquig.id}`);
    await insertPrizeHistory(db, claim.id, newSquig.id, 'initial_offer', null, userId, 'reroll offer');
    await db.query('COMMIT');
    done = true;
    return { ok: true, claim: updatedClaim.rows[0], oldSquig, newSquig };
  } catch (err) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    db.release();
  }
}

async function handlePrizeDeliveredButton(interaction) {
  if (!isAdmin(interaction)) {
    await interaction.reply({ content: 'Admin only.', flags: EPHEMERAL });
    return true;
  }
  const claimId = parseClaimId(interaction.customId);
  await interaction.deferReply({ ephemeral: true });
  const result = await markPrizeDelivered({
    claimId,
    actorId: interaction.user.id,
    txHash: null,
    guild: interaction.guild,
  });
  await interaction.editReply({ content: result.reason || (result.ok ? `Claim ${claimId} marked delivered.` : 'Could not mark delivered.') });
  return true;
}

async function markPrizeDelivered({ claimId, actorId, txHash = null, guild = null }) {
  const db = await resolvePool().connect();
  let done = false;
  try {
    await db.query('BEGIN');
    const claimRows = await db.query(`SELECT * FROM squig_prize_claims WHERE id = $1 FOR UPDATE`, [String(claimId)]);
    const claim = claimRows.rows[0];
    if (!claim) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'Claim not found.' };
    }
    if (String(claim.status) === 'delivered') {
      await db.query('ROLLBACK');
      done = true;
      return { ok: true, reason: 'That claim is already marked delivered.' };
    }
    if (String(claim.status) !== 'accepted_pending_delivery') {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: `Claim is not pending delivery. Current status: ${claim.status}.` };
    }
    const poolRows = await db.query(`SELECT * FROM maw_squig_pool WHERE id = $1 FOR UPDATE`, [String(claim.current_pool_squig_id)]);
    const squig = poolRows.rows[0];
    const wallet = squig?.delivered_to_wallet || null;
    const updatedClaim = await db.query(
      `UPDATE squig_prize_claims
       SET status = 'delivered',
           updated_at = NOW()
       WHERE id = $1
       RETURNING *`,
      [String(claim.id)]
    );
    await db.query(
      `UPDATE maw_squig_pool
       SET status = 'delivered',
           delivered_to_discord_id = $2,
           delivered_to_wallet = COALESCE(delivered_to_wallet, $3),
           delivered_tx_hash = $4,
           updated_at = NOW()
       WHERE id = $1`,
      [String(claim.current_pool_squig_id), String(claim.winner_discord_id), wallet, txHash ? String(txHash).trim() : null]
    );
    await insertPrizeHistory(db, claim.id, claim.current_pool_squig_id, 'delivered', null, actorId, txHash || 'marked delivered');
    await db.query('COMMIT');
    done = true;
    await updatePrizeOfferMessage(claim.id, true).catch(() => null);
    await postToOfferChannel(updatedClaim.rows[0], `Squig prize claim ${claim.id} was marked delivered for <@${claim.winner_discord_id}>.`).catch(() => null);
    return { ok: true, reason: `Claim ${claim.id} marked delivered.` };
  } catch (err) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    await postAdminLog(guild, 'Squig Prize Error', `Delivery failed for claim ${claimId}: ${String(err?.message || err).slice(0, 800)}`);
    return { ok: false, reason: 'Delivery could not be marked right now.' };
  } finally {
    db.release();
  }
}

async function updatePrizeOfferMessage(claimId, disabled = false) {
  const claim = await getPrizeClaimWithSquig(claimId);
  if (!claim?.offer_channel_id || !claim?.offer_message_id || !deps?.client) return false;
  const channel = await deps.client.channels.fetch(claim.offer_channel_id).catch(() => null);
  if (!channel?.isTextBased?.()) return false;
  const message = await channel.messages.fetch(claim.offer_message_id).catch(() => null);
  if (!message) return false;
  const squig = {
    id: claim.current_pool_squig_id,
    token_id: claim.token_id,
    contract_address: claim.contract_address,
  };
  await message.edit(buildPrizeOfferPayload(claim, squig, disabled || String(claim.status) !== 'offered'));
  return true;
}

async function postToOfferChannel(claim, content) {
  if (!claim?.offer_channel_id || !deps?.client) return false;
  const channel = await deps.client.channels.fetch(claim.offer_channel_id).catch(() => null);
  if (!channel?.isTextBased?.()) return false;
  await channel.send({ content });
  return true;
}

function getMawProvider() {
  if (deps?.ethersProvider?.getLogs) return deps.ethersProvider;
  if (mawProvider) return mawProvider;
  const rpcUrl = String(process.env.ETH_RPC_URL || process.env.ETHEREUM_RPC_URL || '').trim();
  if (rpcUrl) {
    mawProvider = new ethers.JsonRpcProvider(rpcUrl);
    return mawProvider;
  }
  const alchemyKey = String(process.env.ALCHEMY_API_KEY || '').trim();
  if (alchemyKey) {
    mawProvider = new ethers.JsonRpcProvider(`https://eth-mainnet.g.alchemy.com/v2/${alchemyKey}`);
    return mawProvider;
  }
  return null;
}

function startMawWatchers() {
  assertReady();
  if (watcherStarted) return { ok: true, alreadyStarted: true };
  watcherStarted = true;
  const config = getMawConfig();

  const pollMs = Math.max(5000, config.pollIntervalSeconds * 1000);
  receiptInterval = setInterval(() => {
    runMawReceiptCheck().catch((err) => {
      console.warn('[Maw] receipt watcher failed:', String(err?.message || err || ''));
    });
  }, pollMs);
  if (receiptInterval.unref) receiptInterval.unref();

  expirationInterval = setInterval(() => {
    runMawExpirationJobs().catch((err) => {
      console.warn('[Maw] expiration job failed:', String(err?.message || err || ''));
    });
  }, 60 * 1000);
  if (expirationInterval.unref) expirationInterval.unref();

  setTimeout(() => runMawReceiptCheck().catch(() => null), 2500).unref?.();
  setTimeout(() => runMawExpirationJobs().catch(() => null), 5000).unref?.();

  return { ok: true, alreadyStarted: false };
}

async function runMawReceiptCheck({ guildId = null } = {}) {
  const config = getMawConfig();
  if (!config.mawWalletAddress) return { ok: false, reason: 'MAW_WALLET_ADDRESS is not configured.' };
  const provider = getMawProvider();
  if (!provider) return { ok: false, reason: 'No Ethereum provider configured. Set ETH_RPC_URL or ALCHEMY_API_KEY.' };
  if (receiptScanRunning) {
    return { ok: true, eventsScanned: 0, logsSeen: 0, matched: 0, unmatched: 0, duplicates: 0, errors: ['scan already running'] };
  }
  receiptScanRunning = true;
  const totals = { ok: true, eventsScanned: 0, logsSeen: 0, matched: 0, unmatched: 0, duplicates: 0, errors: [] };
  try {
    const pool = resolvePool();
    const params = [];
    let guildClause = '';
    if (guildId) {
      params.push(String(guildId));
      guildClause = ` AND guild_id = $${params.length}`;
    }
    const { rows: events } = await pool.query(
      `SELECT * FROM maw_events WHERE status = 'open' ${guildClause} ORDER BY id ASC`,
      params
    );
    for (const event of events) {
      const result = await scanMawEventTransfers(event, provider, config);
      totals.eventsScanned += 1;
      totals.logsSeen += result.logsSeen;
      totals.matched += result.matched;
      totals.unmatched += result.unmatched;
      totals.duplicates += result.duplicates;
      totals.errors.push(...result.errors);
    }
  } finally {
    receiptScanRunning = false;
  }
  return totals;
}

async function scanMawEventTransfers(event, provider, config) {
  const pool = resolvePool();
  const result = { logsSeen: 0, matched: 0, unmatched: 0, duplicates: 0, errors: [] };
  const latestBlock = await provider.getBlockNumber();
  const safeBlock = Math.max(0, latestBlock - config.minConfirmations);
  if (!safeBlock) return result;

  let cursor = await getOrCreateCursor(event.guild_id, config.squigContract, config.mawWalletAddress, safeBlock);
  const lookback = intEnv('MAW_INITIAL_LOOKBACK_BLOCKS', DEFAULT_LOOKBACK_BLOCKS, 0);
  let fromBlock = Number(cursor.last_processed_block || 0) + 1;
  if (Number(cursor.last_processed_block || 0) <= 0) {
    fromBlock = Math.max(0, safeBlock - lookback);
  }
  if (fromBlock > safeBlock) return result;

  const chunkSize = intEnv('MAW_MAX_BLOCK_RANGE', DEFAULT_BLOCK_CHUNK_SIZE, 100);
  const toTopic = addressTopic(config.mawWalletAddress);
  for (let chunkFrom = fromBlock; chunkFrom <= safeBlock; chunkFrom += chunkSize) {
    const chunkTo = Math.min(safeBlock, chunkFrom + chunkSize - 1);
    const logs = await provider.getLogs({
      address: config.squigContract,
      fromBlock: chunkFrom,
      toBlock: chunkTo,
      topics: [MAW_TRANSFER_TOPIC, null, toTopic],
    });
    result.logsSeen += logs.length;
    for (const log of logs) {
      try {
        const processed = await processMawTransferLog(event.guild_id, config, parseTransferLog(log));
        if (processed.status === 'matched') result.matched += 1;
        else if (processed.status === 'unmatched') result.unmatched += 1;
        else if (processed.status === 'duplicate') result.duplicates += 1;
      } catch (err) {
        result.errors.push(String(err?.message || err).slice(0, 300));
      }
    }
    await pool.query(
      `UPDATE maw_chain_cursors
       SET last_processed_block = GREATEST(last_processed_block, $4),
           updated_at = NOW()
       WHERE guild_id = $1 AND contract_address = $2 AND maw_wallet_address = $3`,
      [String(event.guild_id), config.squigContract, config.mawWalletAddress, chunkTo]
    );
    cursor = { ...cursor, last_processed_block: chunkTo };
  }
  return result;
}

async function getOrCreateCursor(guildId, contractAddress, mawWalletAddress, safeBlock) {
  const pool = resolvePool();
  const lookback = intEnv('MAW_INITIAL_LOOKBACK_BLOCKS', DEFAULT_LOOKBACK_BLOCKS, 0);
  const initialBlock = Math.max(0, Number(safeBlock || 0) - lookback - 1);
  await pool.query(
    `INSERT INTO maw_chain_cursors (guild_id, contract_address, maw_wallet_address, last_processed_block, updated_at)
     VALUES ($1, $2, $3, $4, NOW())
     ON CONFLICT (guild_id, contract_address, maw_wallet_address) DO NOTHING`,
    [String(guildId), contractAddress, mawWalletAddress, initialBlock]
  );
  const { rows } = await pool.query(
    `SELECT * FROM maw_chain_cursors WHERE guild_id = $1 AND contract_address = $2 AND maw_wallet_address = $3 LIMIT 1`,
    [String(guildId), contractAddress, mawWalletAddress]
  );
  return rows[0] || { last_processed_block: initialBlock };
}

function parseTransferLog(log) {
  const topics = log.topics || [];
  const from = normalizeAddress(`0x${String(topics[1] || '').slice(-40)}`);
  const to = normalizeAddress(`0x${String(topics[2] || '').slice(-40)}`);
  const tokenId = BigInt(topics[3]).toString();
  return {
    from,
    to,
    tokenId,
    txHash: String(log.transactionHash || log.transaction_hash || ''),
    logIndex: Number(log.index ?? log.logIndex ?? log.log_index ?? 0),
    blockNumber: Number(log.blockNumber || log.block_number || 0),
  };
}

async function processMawTransferLog(guildId, config, transfer) {
  const pool = resolvePool();
  const db = await pool.connect();
  let sessionForPayout = null;
  let eventIdForPanel = null;
  let result = { status: 'unmatched' };
  let done = false;
  try {
    await db.query('BEGIN');
    await db.query('SELECT pg_advisory_xact_lock($1::bigint)', [advisoryLockKey(`maw-log:${transfer.txHash}:${transfer.logIndex}`)]);
    const duplicateSession = await db.query(
      `SELECT id FROM maw_return_sessions WHERE received_tx_hash = $1 AND received_log_index = $2 LIMIT 1 FOR UPDATE`,
      [transfer.txHash, transfer.logIndex]
    );
    if (duplicateSession.rows[0]) {
      await db.query('COMMIT');
      done = true;
      return { status: 'duplicate' };
    }
    const duplicateUnmatched = await db.query(
      `SELECT id FROM maw_unmatched_transfers WHERE tx_hash = $1 AND log_index = $2 LIMIT 1`,
      [transfer.txHash, transfer.logIndex]
    );
    if (duplicateUnmatched.rows[0]) {
      await db.query('COMMIT');
      done = true;
      return { status: 'duplicate' };
    }

    const sessionRows = await db.query(
      `SELECT s.*
       FROM maw_return_sessions s
       JOIN maw_events e ON e.id = s.event_id
       WHERE s.guild_id = $1
         AND e.status = 'open'
         AND s.status = 'awaiting_transfer'
         AND s.contract_address = $2
         AND s.token_id = $3
         AND LOWER(s.source_wallet) = $4
         AND s.expires_at > NOW()
       ORDER BY s.created_at ASC
       LIMIT 1
       FOR UPDATE OF s`,
      [String(guildId), config.squigContract, String(transfer.tokenId), transfer.from]
    );
    const session = sessionRows.rows[0];
    if (!session) {
      const inserted = await db.query(
        `INSERT INTO maw_unmatched_transfers
           (guild_id, contract_address, token_id, from_wallet, to_wallet, tx_hash, log_index, block_number, status, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'manual_review', NOW())
         ON CONFLICT (tx_hash, log_index) DO NOTHING
         RETURNING *`,
        [String(guildId), config.squigContract, String(transfer.tokenId), transfer.from, transfer.to, transfer.txHash, transfer.logIndex, transfer.blockNumber]
      );
      await db.query('COMMIT');
      done = true;
      if (inserted.rows[0]) await notifyUnmatchedTransfer(guildId, inserted.rows[0]).catch(() => null);
      return { status: inserted.rows[0] ? 'unmatched' : 'duplicate' };
    }

    const eventRows = await db.query(`SELECT * FROM maw_events WHERE id = $1 FOR UPDATE`, [String(session.event_id)]);
    const event = eventRows.rows[0];
    if (!event || String(event.status) !== 'open') {
      await db.query('ROLLBACK');
      done = true;
      return { status: 'unmatched' };
    }
    const existingPool = await db.query(
      `SELECT id FROM maw_squig_pool WHERE contract_address = $1 AND token_id = $2 AND status <> 'retired' LIMIT 1 FOR UPDATE`,
      [config.squigContract, String(transfer.tokenId)]
    );
    if (existingPool.rows[0]) {
      await db.query(
        `UPDATE maw_return_sessions
         SET status = 'manual_review',
             received_tx_hash = $2,
             received_log_index = $3,
             received_at = NOW(),
             updated_at = NOW()
         WHERE id = $1`,
        [String(session.id), transfer.txHash, transfer.logIndex]
      );
      await db.query('COMMIT');
      done = true;
      await postAdminLogByGuildId(guildId, 'Maw Manual Review', `Squig ${formatToken(transfer.tokenId)} already exists in the Maw Pool. Session ${session.id} needs review.`);
      return { status: 'matched' };
    }

    const ticketNumberRows = await db.query(
      `SELECT COALESCE(MAX(ticket_number), 0)::int + 1 AS next_number FROM maw_tickets WHERE event_id = $1`,
      [String(event.id)]
    );
    const ticketNumber = Number(ticketNumberRows.rows[0]?.next_number || 1);
    const ticketRows = await db.query(
      `INSERT INTO maw_tickets
         (event_id, ticket_number, discord_user_id, return_session_id, contract_address, token_id, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, NOW())
       ON CONFLICT (event_id, return_session_id) DO NOTHING
       RETURNING *`,
      [String(event.id), ticketNumber, String(session.discord_user_id), String(session.id), config.squigContract, String(transfer.tokenId)]
    );
    const ticket = ticketRows.rows[0] || (await db.query(
      `SELECT * FROM maw_tickets WHERE event_id = $1 AND return_session_id = $2 LIMIT 1`,
      [String(event.id), String(session.id)]
    )).rows[0];

    await db.query(
      `INSERT INTO maw_squig_pool
         (event_id, contract_address, token_id, original_sender_discord_id, original_sender_wallet,
          received_session_id, received_tx_hash, status, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'available', NOW(), NOW())`,
      [String(event.id), config.squigContract, String(transfer.tokenId), String(session.discord_user_id), transfer.from, String(session.id), transfer.txHash]
    );
    const updatedEvent = await db.query(
      `UPDATE maw_events
       SET received_count = received_count + 1,
           updated_at = NOW()
       WHERE id = $1
       RETURNING *`,
      [String(event.id)]
    );
    await db.query(
      `UPDATE maw_return_sessions
       SET status = 'received',
           received_tx_hash = $2,
           received_log_index = $3,
           received_at = NOW(),
           payout_amount = $4,
           payout_status = 'pending',
           ticket_id = $5,
           updated_at = NOW()
       WHERE id = $1`,
      [String(session.id), transfer.txHash, transfer.logIndex, Math.floor(Number(event.return_reward_charm) || 0), String(ticket.id)]
    );
    await db.query('COMMIT');
    done = true;
    sessionForPayout = session.id;
    eventIdForPanel = event.id;
    result = { status: 'matched', event: updatedEvent.rows[0], session, ticket };
  } catch (err) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    db.release();
  }

  if (sessionForPayout) {
    const paid = await payPendingMawReturnSession(sessionForPayout).catch(async (err) => {
      await postAdminLogByGuildId(guildId, 'Maw Payout Failure', `Return session ${sessionForPayout}: ${String(err?.message || err).slice(0, 800)}`);
      return null;
    });
    await updateMawPanel(eventIdForPanel).catch(() => null);
    if (paid?.ok) {
      await postMawReceiptMessages(paid).catch(() => null);
      await maybeCompleteMawGoal(eventIdForPanel).catch(async (err) => {
        await postAdminLogByGuildId(guildId, 'Maw Draw Failure', String(err?.message || err).slice(0, 800));
      });
    }
  }
  return result;
}

async function payPendingMawReturnSession(sessionId) {
  const pool = resolvePool();
  const db = await pool.connect();
  let session;
  let event;
  let ticket;
  let done = false;
  try {
    await db.query('BEGIN');
    const rows = await db.query(
      `SELECT s.*, e.goal_count, e.received_count, e.return_reward_charm, e.guild_id AS event_guild_id
       FROM maw_return_sessions s
       JOIN maw_events e ON e.id = s.event_id
       WHERE s.id = $1
       FOR UPDATE OF s`,
      [String(sessionId)]
    );
    session = rows.rows[0];
    if (!session || String(session.payout_status) !== 'pending') {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'No pending payout.' };
    }
    await db.query(
      `UPDATE maw_return_sessions
       SET payout_status = 'processing',
           updated_at = NOW()
       WHERE id = $1`,
      [String(sessionId)]
    );
    const ticketRows = await db.query(`SELECT * FROM maw_tickets WHERE id = $1 LIMIT 1`, [String(session.ticket_id)]);
    ticket = ticketRows.rows[0] || null;
    await db.query('COMMIT');
    done = true;
  } catch (err) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    db.release();
  }

  try {
    const payout = await awardCharmToUser(session.guild_id, session.discord_user_id, session.payout_amount, 'maw_return_reward');
    const paidRows = await pool.query(
      `UPDATE maw_return_sessions
       SET status = 'paid',
           payout_status = 'paid',
           payout_reference = $2,
           updated_at = NOW()
       WHERE id = $1
       RETURNING *`,
      [String(session.id), payoutReference(payout)]
    );
    const refreshedEvent = await getMawEventById(session.event_id);
    return { ok: true, session: paidRows.rows[0], event: refreshedEvent, ticket };
  } catch (err) {
    await pool.query(
      `UPDATE maw_return_sessions
       SET status = 'manual_review',
           payout_status = 'failed',
           payout_reference = $2,
           updated_at = NOW()
       WHERE id = $1`,
      [String(session.id), `failed:${String(err?.message || err).slice(0, 300)}`]
    ).catch(() => null);
    throw err;
  }
}

async function awardCharmToUser(guildId, userId, amount, context) {
  const spendable = await deps.getMarketplaceSpendableBalance(guildId, userId);
  if (!spendable.ok) throw new Error(spendable.reason || 'Could not resolve DRIP member for payout.');
  return deps.awardDripPoints(
    spendable.settings.drip_realm_id,
    spendable.memberIds,
    Math.floor(Number(amount) || 0),
    spendable.settings.currency_id,
    spendable.settings,
    {
      context,
      initiatorDiscordId: userId,
      recipientDiscordId: userId,
      senderMemberIdOverride: spendable.botMemberId,
    }
  );
}

function payoutReference(result) {
  if (!result) return 'paid';
  const parts = [
    result.endpoint || result.method || 'drip',
    result.usedSenderId ? `sender:${result.usedSenderId}` : '',
    result.usedMemberId ? `recipient:${result.usedMemberId}` : '',
    result.fallbackUsed ? 'fallback' : '',
  ].filter(Boolean);
  return parts.join('|').slice(0, 500) || 'paid';
}

async function postMawReceiptMessages({ session, event, ticket }) {
  const user = await deps.client?.users?.fetch?.(session.discord_user_id).catch(() => null);
  if (user?.send) {
    const embed = new EmbedBuilder()
      .setTitle(`The Maw consumed Squig ${formatToken(session.token_id)}`)
      .setColor(0x2f9e44)
      .setDescription(`Payout released: ${formatCharm(session.payout_amount)} $CHARM`)
      .addFields(
        { name: 'Maw Ticket', value: `#${ticket?.ticket_number || session.ticket_id}`, inline: true },
        { name: 'Progress', value: `${event.received_count} / ${event.goal_count}`, inline: true },
        { name: 'Status', value: 'Your Squig is now part of the Maw Pool.', inline: false }
      );
    await user.send({ embeds: [embed] }).catch(() => null);
  }
  const content =
    `Squig ${formatToken(session.token_id)} was fed to the Maw\n` +
    `Feeder: <@${session.discord_user_id}>\n` +
    `Payout: ${formatCharm(session.payout_amount)} $CHARM\n` +
    `Maw Ticket: #${ticket?.ticket_number || session.ticket_id}\n` +
    `Progress: ${event.received_count} / ${event.goal_count}\n` +
    `The Maw keeps what it chews.`;
  await postMawFeed(event, { content }).catch(() => null);
}

async function maybeCompleteMawGoal(eventId) {
  const pool = resolvePool();
  const db = await pool.connect();
  let draw = null;
  let done = false;
  try {
    await db.query('BEGIN');
    const eventRows = await db.query(`SELECT * FROM maw_events WHERE id = $1 FOR UPDATE`, [String(eventId)]);
    const event = eventRows.rows[0];
    if (!event || event.draw_completed || String(event.status) !== 'open' || Number(event.received_count) < Number(event.goal_count)) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'Goal not ready.' };
    }
    const ticketRows = await db.query(
      `SELECT * FROM maw_tickets WHERE event_id = $1 ORDER BY random() LIMIT 1`,
      [String(event.id)]
    );
    const ticket = ticketRows.rows[0];
    if (!ticket) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'No Maw Tickets found.' };
    }
    const updated = await db.query(
      `UPDATE maw_events
       SET draw_completed = TRUE,
           draw_winning_ticket_id = $2,
           draw_winner_user_id = $3,
           draw_payout_status = 'pending',
           status = 'completed',
           completed_at = NOW(),
           updated_at = NOW()
       WHERE id = $1
       RETURNING *`,
      [String(event.id), String(ticket.id), String(ticket.discord_user_id)]
    );
    await db.query('COMMIT');
    done = true;
    draw = { event: updated.rows[0], ticket };
  } catch (err) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    db.release();
  }

  if (!draw) return { ok: false };
  await updateMawPanel(draw.event.id).catch(() => null);
  await postMawFeed(draw.event, {
    embeds: [
      new EmbedBuilder()
        .setTitle('THE MAW IS FULL')
        .setColor(0x2f9e44)
        .setDescription(`${draw.event.goal_count} / ${draw.event.goal_count} Squigs consumed. The ${formatCharm(draw.event.jackpot_charm)} $CHARM Maw Ticket Draw is unlocked.`),
    ],
  }).catch(() => null);
  const paid = await payPendingMawJackpot(draw.event.id);
  if (paid?.ok) {
    await postMawFeed(paid.event, {
      content:
        `Maw Ticket #${paid.ticket.ticket_number} was drawn\n` +
        `Winner: <@${paid.ticket.discord_user_id}>\n` +
        `Prize: ${formatCharm(paid.event.jackpot_charm)} $CHARM\n` +
        `The Maw burps. The Marketplace gets stronger.`,
    }).catch(() => null);
  }
  return { ok: true, ...draw };
}

async function payPendingMawJackpot(eventId) {
  const pool = resolvePool();
  const db = await pool.connect();
  let event;
  let ticket;
  let done = false;
  try {
    await db.query('BEGIN');
    const eventRows = await db.query(`SELECT * FROM maw_events WHERE id = $1 FOR UPDATE`, [String(eventId)]);
    event = eventRows.rows[0];
    if (!event || String(event.draw_payout_status) !== 'pending') {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false };
    }
    const ticketRows = await db.query(`SELECT * FROM maw_tickets WHERE id = $1 LIMIT 1`, [String(event.draw_winning_ticket_id)]);
    ticket = ticketRows.rows[0];
    if (!ticket) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'Winning ticket missing.' };
    }
    await db.query(`UPDATE maw_events SET draw_payout_status = 'processing', updated_at = NOW() WHERE id = $1`, [String(event.id)]);
    await db.query('COMMIT');
    done = true;
  } catch (err) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    db.release();
  }

  try {
    const payout = await awardCharmToUser(event.guild_id, ticket.discord_user_id, event.jackpot_charm, 'maw_ticket_draw');
    const updated = await pool.query(
      `UPDATE maw_events
       SET draw_payout_status = 'paid',
           draw_payout_reference = $2,
           updated_at = NOW()
       WHERE id = $1
       RETURNING *`,
      [String(event.id), payoutReference(payout)]
    );
    return { ok: true, event: updated.rows[0], ticket };
  } catch (err) {
    await pool.query(
      `UPDATE maw_events
       SET draw_payout_status = 'failed',
           draw_payout_reference = $2,
           updated_at = NOW()
       WHERE id = $1`,
      [String(event.id), `failed:${String(err?.message || err).slice(0, 300)}`]
    ).catch(() => null);
    throw err;
  }
}

async function runMawExpirationJobs() {
  if (expirationRunning) return { ok: true, expiredSessions: 0, expiredClaims: 0 };
  expirationRunning = true;
  try {
    const expiredSessions = await expireAwaitingSessions();
    const eventIds = [...new Set(expiredSessions.map((row) => String(row.event_id)))];
    for (const eventId of eventIds) await updateMawPanel(eventId).catch(() => null);
    for (const session of expiredSessions) {
      await notifyExpiredSession(session).catch(() => null);
    }
    const expiredClaims = await expirePrizeClaims();
    const pendingReturnPayouts = await payPendingMawReturnSessions();
    const pendingDrawPayouts = await payPendingMawJackpots();
    return {
      ok: true,
      expiredSessions: expiredSessions.length,
      expiredClaims: expiredClaims.length,
      pendingReturnPayouts,
      pendingDrawPayouts,
    };
  } finally {
    expirationRunning = false;
  }
}

async function payPendingMawReturnSessions(limit = 10) {
  const { rows } = await resolvePool().query(
    `SELECT id
     FROM maw_return_sessions
     WHERE status = 'received'
       AND payout_status = 'pending'
     ORDER BY received_at ASC NULLS LAST, id ASC
     LIMIT $1`,
    [Math.max(1, Math.floor(Number(limit) || 10))]
  );
  let paid = 0;
  for (const row of rows) {
    const result = await payPendingMawReturnSession(row.id).catch(async (err) => {
      await postAdminLogByGuildId(null, 'Maw Payout Failure', `Return session ${row.id}: ${String(err?.message || err).slice(0, 800)}`);
      return null;
    });
    if (result?.ok) {
      paid += 1;
      await updateMawPanel(result.event.id).catch(() => null);
      await postMawReceiptMessages(result).catch(() => null);
      await maybeCompleteMawGoal(result.event.id).catch(() => null);
    }
  }
  return paid;
}

async function payPendingMawJackpots(limit = 5) {
  const { rows } = await resolvePool().query(
    `SELECT id
     FROM maw_events
     WHERE draw_completed = TRUE
       AND draw_payout_status = 'pending'
     ORDER BY completed_at ASC NULLS LAST, id ASC
     LIMIT $1`,
    [Math.max(1, Math.floor(Number(limit) || 5))]
  );
  let paid = 0;
  for (const row of rows) {
    const result = await payPendingMawJackpot(row.id).catch(async (err) => {
      await postAdminLogByGuildId(null, 'Maw Draw Payout Failure', `Event ${row.id}: ${String(err?.message || err).slice(0, 800)}`);
      return null;
    });
    if (result?.ok) {
      paid += 1;
      await postMawFeed(result.event, {
        content:
          `Maw Ticket #${result.ticket.ticket_number} was drawn\n` +
          `Winner: <@${result.ticket.discord_user_id}>\n` +
          `Prize: ${formatCharm(result.event.jackpot_charm)} $CHARM\n` +
          `The Maw burps. The Marketplace gets stronger.`,
      }).catch(() => null);
    }
  }
  return paid;
}

async function notifyExpiredSession(session) {
  const user = await deps.client?.users?.fetch?.(session.discord_user_id).catch(() => null);
  if (!user?.send) return;
  await user.send('Your Maw transfer window expired. No Squig was received, and no $CHARM was paid.').catch(() => null);
}

async function expirePrizeClaims(guildId = null) {
  const pool = resolvePool();
  const db = await pool.connect();
  const expired = [];
  let done = false;
  try {
    await db.query('BEGIN');
    const params = [];
    let guildClause = '';
    if (guildId) {
      params.push(String(guildId));
      guildClause = ` AND guild_id = $${params.length}`;
    }
    const claimRows = await db.query(
      `SELECT *
       FROM squig_prize_claims
       WHERE status = 'offered'
         AND expires_at < NOW()
         ${guildClause}
       FOR UPDATE`,
      params
    );
    for (const claim of claimRows.rows) {
      await db.query(
        `UPDATE squig_prize_claims SET status = 'expired', updated_at = NOW() WHERE id = $1`,
        [String(claim.id)]
      );
      if (claim.current_pool_squig_id) {
        await db.query(
          `UPDATE maw_squig_pool
           SET status = 'available',
               reserved_claim_id = NULL,
               updated_at = NOW()
           WHERE id = $1
             AND status = 'reserved_for_claim'`,
          [String(claim.current_pool_squig_id)]
        );
      }
      await insertPrizeHistory(db, claim.id, claim.current_pool_squig_id, 'expired', null, null, 'claim expired');
      expired.push(claim);
    }
    await db.query('COMMIT');
    done = true;
  } catch (err) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    db.release();
  }
  for (const claim of expired) await updatePrizeOfferMessage(claim.id, true).catch(() => null);
  return expired;
}

async function notifyUnmatchedTransfer(guildId, transfer) {
  const message =
    `Unmatched Squig transfer into the Maw wallet.\n` +
    `Squig: ${formatToken(transfer.token_id)}\n` +
    `From: \`${transfer.from_wallet}\`\n` +
    `Tx: \`${transfer.tx_hash}\`\n` +
    `Log index: ${transfer.log_index}`;
  const event = await getOpenMawEvent(guildId).catch(() => null);
  await postMawAdminMessage(event, { content: `**Maw Manual Review**\n${message}` }).catch(() => null);
  await postAdminLogByGuildId(guildId, 'Maw Manual Review', message);
}

async function postMawFeed(event, payload) {
  const channel = await resolveFeedChannel(event);
  if (!channel?.isTextBased?.()) return false;
  await channel.send(payload);
  return true;
}

async function resolveFeedChannel(event) {
  if (!deps?.client) return null;
  const channelId = event?.feed_channel_id || getMawConfig().feedChannelId || event?.panel_channel_id || null;
  if (!channelId) return null;
  return deps.client.channels.fetch(channelId).catch(() => null);
}

async function resolveAdminChannel(guild, event = null) {
  if (!deps?.client) return null;
  const channelId = event?.admin_channel_id || getMawConfig().adminChannelId || null;
  if (channelId) return deps.client.channels.fetch(channelId).catch(() => null);
  return null;
}

async function postMawAdminMessage(event = null, payload) {
  const channel = await resolveAdminChannel(null, event);
  if (!channel?.isTextBased?.()) return false;
  await channel.send(payload);
  return true;
}

async function postAdminLog(guild, category, message) {
  if (typeof deps?.postAdminSystemLog === 'function') {
    await deps.postAdminSystemLog({ guild, guildId: guild?.id || null, category, message }).catch(() => null);
  }
}

async function postAdminLogByGuildId(guildId, category, message) {
  if (typeof deps?.postAdminSystemLog === 'function') {
    await deps.postAdminSystemLog({ guildId, category, message }).catch(() => null);
  }
}

module.exports = {
  DEFAULT_SQUIG_CONTRACT,
  initMawEvent,
  buildMawSlashCommand,
  buildSquigPrizeSlashCommand,
  handleCommand,
  handleComponent,
  startMawWatchers,
  ensureMawTables,
  runMawReceiptCheck,
  runMawExpirationJobs,
  expireAwaitingSessions,
  expirePrizeClaims,
  calculateMawOpenSlots,
  filterEligiblePrizeSquigsForUser,
  sortMawSquigsForDisplay,
  mawSquigPageCount,
  mawSquigPageItems,
  normalizeAddress,
  getMawConfig,
  formatCharm,
};
