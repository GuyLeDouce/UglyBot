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
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  PermissionFlagsBits,
} = require('discord.js');
const marketplaceCommand = require('./marketplaceCommand');
const {
  MAW_REWARD_RULES_VERSION,
  MAW_RARITY_RULES,
  loadMawRankingIndex,
  getMawRewardQuote,
  formatMawAverageRank,
  formatMawRarityLabel,
  snapshotMawRewardQuote,
  resolveMawSessionRewardSnapshot,
  pluralizeMawTickets,
  formatMawTicketRange,
  summarizeMawTicketRows,
} = require('./mawRarity');
const {
  MAW_DISPOSITIONS,
  MAW_DIGESTION_STATUS,
  MAW_INVENTORY_STATUS,
  DEFAULT_DIGESTION_ADMIN_CHANNEL_ID,
  DEFAULT_DIGESTION_RECEIPT_CHANNEL_ID,
  ZERO_ADDRESS,
  normalizeDisposition,
  isValidMawDisposition,
  formatMawDispositionLabel,
  mawDispositionInventoryStatus,
  mawDispositionDigestionStatus,
  isRegurgitatedAvailableInventory,
  parseBurnTransactionInput,
  parseAcceptedBurnAddresses,
  digestionStatusText,
} = require('./mawDisposition');

const DEFAULT_SQUIG_CONTRACT = '0x8c9a02c0585200c4c65608df6b8def543d33792a';
const MAW_TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
const ERC721_OWNER_ABI = ['function ownerOf(uint256 tokenId) view returns (address)'];
const EPHEMERAL = 64;
const MAX_SELECT_OPTIONS = 25;
const PENDING_TTL_MS = 10 * 60 * 1000;
const CLAIM_TTL_HOURS = 24;
const DEFAULT_LOOKBACK_BLOCKS = 7200;
const DEFAULT_BLOCK_CHUNK_SIZE = 2000;
const MAW_RANKING_FAILURE_MESSAGE = 'The Maw could not determine the value of that Squig. Nothing has been transferred and no timer was started. An admin has been notified.';
const SQUIG_IMAGE_BASE_URL = String(process.env.SQUIG_IMAGE_BASE_URL || '').replace(/\/+$/, '');
const MAW_PANEL_IMAGE_URL = 'https://i.imgur.com/tjahRQz.png';
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

function hasEnvValue(name) {
  return String(process.env[name] ?? '').trim() !== '';
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
  const jackpotBaseCharm = hasEnvValue('MAW_JACKPOT_BASE_CHARM')
    ? intEnv('MAW_JACKPOT_BASE_CHARM', 0, 0)
    : (hasEnvValue('MAW_JACKPOT_CHARM') ? intEnv('MAW_JACKPOT_CHARM', 0, 0) : 0);
  return {
    mawWalletAddress: normalizeAddress(process.env.MAW_WALLET_ADDRESS),
    rawMawWalletAddress: String(process.env.MAW_WALLET_ADDRESS || '').trim(),
    squigContract: normalizeAddress(process.env.MAW_SQUIG_CONTRACT || DEFAULT_SQUIG_CONTRACT) || DEFAULT_SQUIG_CONTRACT,
    goalCount: intEnv('MAW_GOAL_COUNT', 20, 1),
    returnRewardCharm: intEnv('MAW_RETURN_REWARD_CHARM', 12500, 0),
    jackpotBaseCharm,
    jackpotCharm: intEnv('MAW_JACKPOT_CHARM', 35000, 0),
    sessionTtlMinutes: intEnv('MAW_SESSION_TTL_MINUTES', 20, 1),
    prizeCashoutCharm: intEnv('MAW_PRIZE_CASHOUT_CHARM', 8000, 0),
    rerollCostCharm: intEnv('MAW_REROLL_COST_CHARM', 4000, 0),
    maxRerolls: intEnv('MAW_MAX_REROLLS', 3, 0),
    pollIntervalSeconds: intEnv('MAW_POLL_INTERVAL_SECONDS', 30, 5),
    minConfirmations: intEnv('MAW_MIN_CONFIRMATIONS', 2, 0),
    feedChannelId: String(process.env.MAW_FEED_CHANNEL_ID || '').trim() || null,
    adminChannelId: String(process.env.MAW_ADMIN_CHANNEL_ID || '').trim() || null,
    digestionAdminChannelId: String(process.env.MAW_DIGESTION_ADMIN_CHANNEL_ID || DEFAULT_DIGESTION_ADMIN_CHANNEL_ID).trim(),
    digestionReceiptChannelId: String(process.env.MAW_DIGESTION_RECEIPT_CHANNEL_ID || DEFAULT_DIGESTION_RECEIPT_CHANNEL_ID).trim(),
    digestedImageUrl: String(process.env.MAW_DIGESTED_IMAGE_URL || '').trim() || null,
    digestedImagePath: String(process.env.MAW_DIGESTED_IMAGE_PATH || '').trim() || null,
    acceptedBurnAddresses: parseAcceptedBurnAddresses(process.env.MAW_ACCEPTED_BURN_ADDRESSES, normalizeAddress),
    explorerBaseUrl: String(process.env.ETH_EXPLORER_BASE_URL || process.env.ETHERSCAN_BASE_URL || 'https://etherscan.io').replace(/\/+$/, ''),
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
    if (!isRegurgitatedAvailableInventory(row)) return false;
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

function isRarityMawEvent(event) {
  return String(event?.reward_model || '') === MAW_REWARD_RULES_VERSION;
}

function shortHash(value) {
  const text = String(value || '');
  if (!text) return 'unavailable';
  if (text.startsWith('sha256:') && text.length > 22) return `${text.slice(0, 19)}...${text.slice(-8)}`;
  return text.length > 28 ? `${text.slice(0, 20)}...${text.slice(-8)}` : text;
}

function formatSignedCharm(amount) {
  const value = Math.floor(Number(amount) || 0);
  return `${value >= 0 ? '+' : '-'}${formatCharm(Math.abs(value))}`;
}

function truncateField(value, limit = 1024) {
  const text = String(value || '');
  if (text.length <= limit) return text;
  return `${text.slice(0, Math.max(0, limit - 12))}\n...truncated`;
}

function getSessionRarityLabel(session) {
  return session?.rarity_tier ? formatMawRarityLabel(session.rarity_tier) : 'Legacy';
}

function getSessionDisposition(session) {
  return normalizeDisposition(session?.squig_disposition || session?.disposition);
}

function getSessionDispositionLabel(session) {
  return formatMawDispositionLabel(getSessionDisposition(session));
}

function formatSessionMawRank(session) {
  if (session?.average_rank == null) return 'Legacy flat event';
  const rank = formatMawAverageRank(session.average_rank);
  if (session.overall_rank != null && session.collection_rank != null) {
    return `${rank}\nOverall ${formatMawAverageRank(session.overall_rank)} • Collection ${formatMawAverageRank(session.collection_rank)}`;
  }
  return rank;
}

function buildExplorerTxUrl(txHash, config = getMawConfig()) {
  const parsed = parseBurnTransactionInput(txHash, config.explorerBaseUrl);
  return parsed.url;
}

function inboundTxUrl(sessionOrTransfer, config = getMawConfig()) {
  const hash = sessionOrTransfer?.received_tx_hash || sessionOrTransfer?.txHash || sessionOrTransfer?.tx_hash;
  if (!hash) return null;
  try {
    return buildExplorerTxUrl(hash, config);
  } catch {
    return null;
  }
}

function poolEligibilityWhere(alias = '') {
  const prefix = alias ? `${alias}.` : '';
  return `COALESCE(${prefix}disposition, 'regurgitated') = 'regurgitated' AND COALESCE(${prefix}inventory_status, ${prefix}status, 'available') = 'available'`;
}

function formatAdminMessageLink(guildId, channelId, messageId) {
  if (!guildId || !channelId || !messageId) return 'Not posted';
  return `https://discord.com/channels/${guildId}/${channelId}/${messageId}`;
}

function createDigestionLogPrefix(session) {
  return `Squig ${formatToken(session?.token_id)} session ${session?.id || 'unknown'}`;
}

function buildMawRulesText() {
  return [
    'Legendary: Rank 1 • 800,000 • 20 tickets • +100,000 jackpot',
    'Epic: Rank 32–443 • 187,500 • 10 tickets • +25,000 jackpot',
    'Rare: Rank 444–1110 • 67,500 • 5 tickets • +5,000 jackpot',
    'Uncommon: Rank 1111–2276 • 22,500 • 2 tickets • +2,000 jackpot',
    'Common: Rank 2277+ • 15,000 • 1 ticket • +1,000 jackpot',
  ].join('\n');
}

function sortMawSquigsForDisplay(squigs = []) {
  return [...(Array.isArray(squigs) ? squigs : [])].sort((a, b) => {
    const rankA = safeNumber(a?.quote?.averageRank ?? a?.averageRank, Number.POSITIVE_INFINITY);
    const rankB = safeNumber(b?.quote?.averageRank ?? b?.averageRank, Number.POSITIVE_INFINITY);
    if (rankA !== rankB) return rankB - rankA;
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
  const sortedNote = (Array.isArray(squigs) && squigs.some((entry) => entry?.quote))
    ? 'Common eligible Squigs appear first; Legendary Squigs appear last.'
    : 'Highest token IDs appear first.';
  if (total > MAX_SELECT_OPTIONS) {
    return `Select a Squig to feed the Maw.\n${mawSquigPageLabel(squigs, page)} eligible Squigs. ${sortedNote}`;
  }
  return `Select a Squig to feed the Maw.\n${total} eligible Squig${total === 1 ? '' : 's'}. ${sortedNote}`;
}

async function handleMawExplain(interaction) {
  const eventId = String(interaction.customId || '').split(':')[1] || '';
  let event = null;
  if (eventId && eventId !== 'closed') {
    event = await getMawEventById(eventId).catch(() => null);
  }
  const ttl = formatDurationMinutes(event?.session_ttl_minutes || getMawConfig().sessionTtlMinutes);
  const embed = new EmbedBuilder()
    .setTitle('WTF is Feed the Maw?')
    .setColor(0x8b1e3f)
    .setDescription(
      `It’s a Squig return event.\n\n` +
      `Pick an eligible Squig, review its exact reward, choose its fate, then confirm. Only after that do you get a ${ttl} timer to send that Squig to the Malformed Maw wallet.`
    )
    .addFields(
      {
        name: 'What you get',
        value:
          `Your Squig’s Maw Rank determines:\n` +
          `• Immediate $CHARM payout\n` +
          `• Maw Ticket count\n` +
          `• Jackpot contribution`,
        inline: false,
      },
      {
        name: 'Maw Rank',
        value: 'Legendary Squigs share rank 1. Every other Squig is ranked by Total UglyPoints, with lower rank numbers earning stronger rewards.',
        inline: false,
      },
      {
        name: 'Rarity rewards',
        value: buildMawRulesText(),
        inline: false,
      },
      {
        name: 'Choose its fate',
        value:
          `**Swallowed**: enters the digestion queue and will be permanently burned by an admin.\n` +
          `**Regurgitated**: joins the Maw Pool for future prizes, games, giveaways, incentives, and rewards.\n\n` +
          `Both choices get the same payout, tickets, jackpot contribution, and event progress.`,
        inline: false,
      },
      {
        name: 'Important',
        value: `Do not send anything until the final confirmation screen starts your ${ttl} transfer window.`,
        inline: false,
      }
    );
  if (event) {
    embed.addFields({
      name: 'Current event',
      value:
        `${event.received_count} / ${event.goal_count} Squigs fed\n` +
        `Current jackpot: ${formatCharm(event.jackpot_charm)} $CHARM`,
      inline: false,
    });
  }
  await interaction.reply({ embeds: [embed], flags: EPHEMERAL });
  return true;
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

function buildMawReviewSquigPageButtons(token, squigs, page = 0) {
  const pageCount = mawSquigPageCount(squigs);
  if (pageCount <= 1) return [];
  const safePage = clampMawSquigPage(squigs, page);
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`maw_review_select_page:${token}:${Math.max(0, safePage - 1)}`)
        .setLabel('Previous')
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(safePage <= 0),
      new ButtonBuilder()
        .setCustomId(`maw_review_select_page:${token}:${Math.min(pageCount - 1, safePage + 1)}`)
        .setLabel('Next')
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(safePage >= pageCount - 1)
    ),
  ];
}

function buildMawSquigSelectRows(eventId, userId, squigs, page = 0, selectOptions = {}) {
  const safePage = clampMawSquigPage(squigs, page);
  const selectedTokenId = String(selectOptions.selectedTokenId || '').trim();
  const selectedWallet = normalizeAddress(selectOptions.selectedWallet || '');
  const menuOptions = mawSquigPageItems(squigs, safePage).map((entry) => {
    const quote = entry.quote || null;
    const entryWallet = normalizeAddress(entry.wallet || '');
    const isSelected = selectedTokenId && selectedWallet && String(entry.tokenId) === selectedTokenId && entryWallet === selectedWallet;
    return {
      label: quote
        ? `${formatToken(entry.tokenId)} • ${quote.rarityLabel}`.slice(0, 100)
        : `Squig ${formatToken(entry.tokenId)}`.slice(0, 100),
      description: quote
        ? `Rank ${formatMawAverageRank(quote.averageRank)} • ${formatCharm(quote.payoutCharm)} CHARM • ${pluralizeMawTickets(quote.ticketCount)}`.slice(0, 100)
        : `Wallet ${shortAddress(entry.wallet)}`.slice(0, 100),
      value: `${entry.wallet}:${entry.tokenId}`,
      default: Boolean(isSelected),
    };
  });
  const pageRows = selectOptions.reviewToken
    ? buildMawReviewSquigPageButtons(selectOptions.reviewToken, squigs, safePage)
    : buildMawSquigPageButtons(eventId, userId, squigs, safePage);
  return [
    new ActionRowBuilder().addComponents(
      new StringSelectMenuBuilder()
        .setCustomId(`maw_select_squig:${eventId}:${userId}`)
        .setPlaceholder(selectOptions.placeholder || 'Pick a Squig for the Maw')
        .addOptions(menuOptions)
    ),
    ...pageRows,
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
      reward_model TEXT NOT NULL DEFAULT 'flat_v1',
      reward_rules_version TEXT,
      reward_rules_json JSONB,
      ranking_source_hash TEXT,
      jackpot_base_charm NUMERIC NOT NULL DEFAULT 0,
      jackpot_contributed_charm NUMERIC NOT NULL DEFAULT 0,
      total_ticket_count INT NOT NULL DEFAULT 0,
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
    ['goal_count', 'INT NOT NULL DEFAULT 20'],
    ['return_reward_charm', 'NUMERIC NOT NULL DEFAULT 12500'],
    ['jackpot_charm', 'NUMERIC NOT NULL DEFAULT 0'],
    ['reward_model', "TEXT NOT NULL DEFAULT 'flat_v1'"],
    ['reward_rules_version', 'TEXT'],
    ['reward_rules_json', 'JSONB'],
    ['ranking_source_hash', 'TEXT'],
    ['jackpot_base_charm', 'NUMERIC NOT NULL DEFAULT 0'],
    ['jackpot_contributed_charm', 'NUMERIC NOT NULL DEFAULT 0'],
    ['total_ticket_count', 'INT NOT NULL DEFAULT 0'],
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
      overall_rank NUMERIC,
      collection_rank NUMERIC,
      average_rank NUMERIC(12,2),
      rarity_tier TEXT,
      ticket_count INT NOT NULL DEFAULT 1,
      jackpot_contribution_charm NUMERIC NOT NULL DEFAULT 0,
      reward_rules_version TEXT,
      ranking_source_hash TEXT,
      squig_disposition TEXT,
      digestion_status TEXT,
      admin_digestion_message_id TEXT,
      burn_transaction_url TEXT,
      burn_transaction_hash TEXT,
      burn_confirmed_by TEXT,
      burn_confirmed_at TIMESTAMPTZ,
      digestion_receipt_message_id TEXT,
      digestion_receipt_posted_at TIMESTAMPTZ,
      digestion_receipt_error TEXT,
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
    ['overall_rank', 'NUMERIC'],
    ['collection_rank', 'NUMERIC'],
    ['average_rank', 'NUMERIC(12,2)'],
    ['rarity_tier', 'TEXT'],
    ['ticket_count', 'INT NOT NULL DEFAULT 1'],
    ['jackpot_contribution_charm', 'NUMERIC NOT NULL DEFAULT 0'],
    ['reward_rules_version', 'TEXT'],
    ['ranking_source_hash', 'TEXT'],
    ['squig_disposition', 'TEXT'],
    ['digestion_status', 'TEXT'],
    ['admin_digestion_message_id', 'TEXT'],
    ['burn_transaction_url', 'TEXT'],
    ['burn_transaction_hash', 'TEXT'],
    ['burn_confirmed_by', 'TEXT'],
    ['burn_confirmed_at', 'TIMESTAMPTZ'],
    ['digestion_receipt_message_id', 'TEXT'],
    ['digestion_receipt_posted_at', 'TIMESTAMPTZ'],
    ['digestion_receipt_error', 'TEXT'],
    ['created_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
    ['updated_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
  ]);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_return_sessions_received_log_uidx ON maw_return_sessions (received_tx_hash, received_log_index) WHERE received_tx_hash IS NOT NULL AND received_log_index IS NOT NULL;`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_return_sessions_active_user_uidx ON maw_return_sessions (guild_id, discord_user_id) WHERE status = 'awaiting_transfer';`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_return_sessions_active_token_uidx ON maw_return_sessions (guild_id, contract_address, token_id) WHERE status = 'awaiting_transfer';`);
  await pool.query(`CREATE INDEX IF NOT EXISTS maw_return_sessions_event_status_idx ON maw_return_sessions (event_id, status, expires_at);`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_unique_burn_transaction_hash ON maw_return_sessions (burn_transaction_hash) WHERE burn_transaction_hash IS NOT NULL;`);
  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'maw_return_sessions_disposition_chk'
      ) THEN
        ALTER TABLE maw_return_sessions
        ADD CONSTRAINT maw_return_sessions_disposition_chk
        CHECK (squig_disposition IS NULL OR squig_disposition IN ('swallowed', 'regurgitated'));
      END IF;
    END $$;
  `);

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
      overall_rank NUMERIC,
      collection_rank NUMERIC,
      average_rank NUMERIC(12,2),
      rarity_tier TEXT,
      original_payout_amount NUMERIC,
      ticket_count INT NOT NULL DEFAULT 1,
      jackpot_contribution_charm NUMERIC NOT NULL DEFAULT 0,
      reward_rules_version TEXT,
      ranking_source_hash TEXT,
      disposition TEXT,
      inventory_status TEXT,
      digestion_status TEXT,
      original_feeder_user_id TEXT,
      original_feeder_wallet TEXT,
      inbound_transaction_hash TEXT,
      admin_digestion_message_id TEXT,
      burn_transaction_hash TEXT,
      burn_transaction_url TEXT,
      burn_confirmed_by TEXT,
      burn_confirmed_at TIMESTAMPTZ,
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
    ['overall_rank', 'NUMERIC'],
    ['collection_rank', 'NUMERIC'],
    ['average_rank', 'NUMERIC(12,2)'],
    ['rarity_tier', 'TEXT'],
    ['original_payout_amount', 'NUMERIC'],
    ['ticket_count', 'INT NOT NULL DEFAULT 1'],
    ['jackpot_contribution_charm', 'NUMERIC NOT NULL DEFAULT 0'],
    ['reward_rules_version', 'TEXT'],
    ['ranking_source_hash', 'TEXT'],
    ['disposition', 'TEXT'],
    ['inventory_status', 'TEXT'],
    ['digestion_status', 'TEXT'],
    ['original_feeder_user_id', 'TEXT'],
    ['original_feeder_wallet', 'TEXT'],
    ['inbound_transaction_hash', 'TEXT'],
    ['admin_digestion_message_id', 'TEXT'],
    ['burn_transaction_hash', 'TEXT'],
    ['burn_transaction_url', 'TEXT'],
    ['burn_confirmed_by', 'TEXT'],
    ['burn_confirmed_at', 'TIMESTAMPTZ'],
    ['created_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
    ['updated_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
  ]);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_squig_pool_active_token_uidx ON maw_squig_pool (contract_address, token_id) WHERE status <> 'retired';`);
  await pool.query(`CREATE INDEX IF NOT EXISTS maw_squig_pool_status_idx ON maw_squig_pool (status, updated_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS maw_squig_pool_disposition_status_idx ON maw_squig_pool (disposition, inventory_status, status);`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_squig_pool_unique_burn_transaction_hash ON maw_squig_pool (burn_transaction_hash) WHERE burn_transaction_hash IS NOT NULL;`);
  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'maw_squig_pool_disposition_chk'
      ) THEN
        ALTER TABLE maw_squig_pool
        ADD CONSTRAINT maw_squig_pool_disposition_chk
        CHECK (disposition IS NULL OR disposition IN ('swallowed', 'regurgitated'));
      END IF;
    END $$;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS maw_tickets (
      id BIGSERIAL PRIMARY KEY,
      event_id BIGINT NOT NULL REFERENCES maw_events(id),
      ticket_number INT NOT NULL,
      discord_user_id TEXT NOT NULL,
      return_session_id BIGINT NOT NULL REFERENCES maw_return_sessions(id),
      ticket_slot INT NOT NULL DEFAULT 1,
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
    ['ticket_slot', 'INT NOT NULL DEFAULT 1'],
    ['contract_address', 'TEXT'],
    ['token_id', 'TEXT'],
    ['created_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
  ]);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_tickets_event_number_uidx ON maw_tickets (event_id, ticket_number);`);
  await pool.query(`DROP INDEX IF EXISTS maw_tickets_event_session_uidx;`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS maw_tickets_event_session_slot_uidx ON maw_tickets (event_id, return_session_id, ticket_slot);`);
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
        .addIntegerOption((opt) => opt.setName('jackpot').setDescription('Optional starting $CHARM added before rarity contributions').setMinValue(0).setRequired(false))
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
    )
    .addSubcommand((sub) =>
      sub
        .setName('rank')
        .setDescription('Admin: audit a Squig Maw rarity quote')
        .addIntegerOption((opt) => opt.setName('token').setDescription('Squig token ID').setMinValue(1).setRequired(true))
    )
    .addSubcommand((sub) =>
      sub
        .setName('digestion')
        .setDescription('Admin: show pending or failed Swallowed digestion workflows')
        .addStringOption((opt) => opt.setName('status').setDescription('pending, receipt_failed, burn_verified, or digested').setRequired(false))
        .addIntegerOption((opt) => opt.setName('token').setDescription('Optional Squig token ID').setMinValue(1).setRequired(false))
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

  if (id.startsWith('maw_explain:')) return handleMawExplain(interaction);
  if (id.startsWith('maw_feed_start:')) return handleMawFeedStart(interaction);
  if (id.startsWith('maw_select_page:')) return handleMawSquigPageButton(interaction);
  if (id.startsWith('maw_review_select_page:')) return handleMawReviewSquigPageButton(interaction);
  if (id.startsWith('maw_review_continue:')) return handleMawReviewContinue(interaction);
  if (id.startsWith('maw_fate_select:')) return handleMawFateSelect(interaction);
  if (id.startsWith('maw_confirm_start_timer:')) return handleMawConfirmStartTimer(interaction);
  if (id.startsWith('maw_cancel_session:')) return handleMawCancelSession(interaction);
  if (id.startsWith('maw_refresh_session:')) return handleMawRefreshSession(interaction);
  if (id.startsWith('maw_submit_burn_tx:')) return handleMawSubmitBurnButton(interaction);
  if (id.startsWith('maw_retry_digestion_receipt:')) return handleMawRetryDigestionReceiptButton(interaction);
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

async function handleModalSubmit(interaction) {
  const id = String(interaction.customId || '');
  if (!id.startsWith('maw_')) return false;
  assertReady();
  if (id.startsWith('maw_burn_tx_modal:')) return handleMawBurnTransactionModal(interaction);
  return false;
}

async function handleMawCommand(interaction) {
  const subcommand = interaction.options.getSubcommand();
  if (['post', 'open', 'close', 'inventory', 'reconcile', 'rank', 'digestion'].includes(subcommand) && !isAdmin(interaction)) {
    await interaction.reply({ content: 'Admin only.', flags: EPHEMERAL });
    return;
  }

  if (subcommand === 'open') return handleMawOpen(interaction);
  if (subcommand === 'post') return handleMawPost(interaction);
  if (subcommand === 'close') return handleMawClose(interaction);
  if (subcommand === 'status') return handleMawStatus(interaction);
  if (subcommand === 'inventory') return handleMawInventory(interaction);
  if (subcommand === 'reconcile') return handleMawReconcile(interaction);
  if (subcommand === 'rank') return handleMawRank(interaction);
  if (subcommand === 'digestion') return handleMawDigestionStatus(interaction);

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
  const pool = db || resolvePool();
  const ticketRows = await pool.query(
    `SELECT COUNT(*)::int AS count FROM maw_tickets WHERE event_id = $1`,
    [String(event.id)]
  ).catch(() => ({ rows: [{ count: 0 }] }));
  const actualTicketCount = Number(ticketRows.rows[0]?.count || 0);
  return {
    event,
    activeTransferWindows,
    totalTicketCount: Math.max(Number(event.total_ticket_count || 0), actualTicketCount),
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

  let rankingIndex;
  try {
    rankingIndex = loadMawRankingIndex();
  } catch (err) {
    await postAdminLog(interaction.guild, 'Maw Ranking Data', `Open blocked: ${String(err?.message || err).slice(0, 1200)}`);
    await interaction.reply({
      content: 'The Maw ranking CSV could not be loaded, so no rarity event was opened. An admin log has been written.',
      flags: EPHEMERAL,
    });
    return;
  }

  const goal = interaction.options.getInteger('goal') || config.goalCount;
  const jackpotBase = interaction.options.getInteger('jackpot') ?? config.jackpotBaseCharm;
  const feedChannel = interaction.options.getChannel('feed_channel');
  const adminChannel = interaction.options.getChannel('admin_channel');

  const pool = resolvePool();
  try {
    const { rows } = await pool.query(
      `INSERT INTO maw_events
         (guild_id, status, goal_count, return_reward_charm, jackpot_charm, reward_model,
          reward_rules_version, reward_rules_json, ranking_source_hash, jackpot_base_charm,
          jackpot_contributed_charm, total_ticket_count, session_ttl_minutes, feed_channel_id,
          admin_channel_id, started_at, created_by, created_at, updated_at)
       VALUES ($1, 'open', $2, $3, $4, $5, $6, $7::jsonb, $8, $9, 0, 0, $10, $11, $12, NOW(), $13, NOW(), NOW())
       RETURNING *`,
      [
        String(interaction.guildId),
        Math.floor(Number(goal) || config.goalCount),
        config.returnRewardCharm,
        Math.floor(Number(jackpotBase) || 0),
        MAW_REWARD_RULES_VERSION,
        MAW_REWARD_RULES_VERSION,
        JSON.stringify(MAW_RARITY_RULES),
        rankingIndex.rankingSourceHash,
        Math.floor(Number(jackpotBase) || 0),
        config.sessionTtlMinutes,
        feedChannel?.id || config.feedChannelId,
        adminChannel?.id || config.adminChannelId,
        String(interaction.user.id),
      ]
    );
    await interaction.reply({
      content:
        `The Maw is open. Event ID: ${rows[0].id}.\n` +
        `Goal: ${rows[0].goal_count} Squigs. Starting jackpot: ${formatCharm(rows[0].jackpot_base_charm)} $CHARM. Rules: ${rows[0].reward_rules_version}. Ranking: ${shortHash(rows[0].ranking_source_hash)}.`,
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
  const ticketGroups = await pool.query(
    `SELECT s.id AS session_id,
            s.token_id,
            s.rarity_tier,
            s.ticket_count,
            s.squig_disposition,
            s.digestion_status,
            MIN(t.ticket_number)::int AS first_ticket_number,
            MAX(t.ticket_number)::int AS last_ticket_number,
            COUNT(t.id)::int AS physical_ticket_count
     FROM maw_tickets t
     JOIN maw_return_sessions s ON s.id = t.return_session_id
     WHERE t.event_id = $1 AND t.discord_user_id = $2
     GROUP BY s.id
     ORDER BY MIN(t.ticket_number) ASC`,
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
  const totalUserTickets = ticketGroups.rows.reduce((sum, row) => sum + Number(row.physical_ticket_count || 0), 0);
  const ticketText = ticketGroups.rows.length
    ? ticketGroups.rows.map((row) =>
      `${formatToken(row.token_id)} • ${getSessionRarityLabel(row)} • Fate: ${getSessionDispositionLabel(row)} • ${digestionStatusText(row.squig_disposition, row.digestion_status)} • Tickets ${formatMawTicketRange(row.first_ticket_number, row.last_ticket_number, row.physical_ticket_count)}`
    ).join('\n')
    : 'No Maw Tickets yet.';
  const activeSession = active.rows[0] || null;
  const activeSnapshot = activeSession ? resolveMawSessionRewardSnapshot(activeSession, event) : null;
  const activeText = activeSession
    ? [
      `Squig ${formatToken(activeSession.token_id)} expires <t:${Math.floor(new Date(activeSession.expires_at).getTime() / 1000)}:R>.`,
      `Rarity: ${activeSnapshot.rarityLabel}`,
      `Fate: ${getSessionDispositionLabel(activeSession)}`,
      `Maw Rank: ${activeSession.average_rank == null ? 'Legacy flat event' : formatMawAverageRank(activeSession.average_rank)}`,
      `Payout: ${formatCharm(activeSnapshot.payoutCharm)} $CHARM`,
      `Tickets: ${activeSnapshot.ticketCount}`,
      `Jackpot contribution: ${formatSignedCharm(activeSnapshot.jackpotContributionCharm)} $CHARM`,
      `NFT lifecycle: ${digestionStatusText(activeSession.squig_disposition, activeSession.digestion_status)}`,
    ].join('\n')
    : 'No active transfer window.';
  const embed = new EmbedBuilder()
    .setTitle('Maw Status')
    .setColor(0x8b1e3f)
    .addFields(
      { name: 'Progress', value: `${event.received_count} / ${event.goal_count} Squigs consumed`, inline: true },
      { name: 'Open spots', value: String(summary.openSlots), inline: true },
      { name: 'Active transfers', value: String(summary.activeTransferWindows), inline: true },
      { name: 'Your tickets', value: String(totalUserTickets), inline: true },
      { name: 'Squigs you fed', value: String(ticketGroups.rows.length), inline: true },
      { name: 'Event tickets', value: String(summary.totalTicketCount || event.total_ticket_count || 0), inline: true },
      { name: 'Your Maw Tickets', value: truncateField(ticketText), inline: false },
      { name: 'Your active session', value: truncateField(activeText), inline: false },
      { name: 'Maw Ticket Draw', value: event.draw_completed ? 'Complete.' : `${formatCharm(event.jackpot_charm)} $CHARM current jackpot.`, inline: false }
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
  const rarityRows = await pool.query(
    `SELECT COALESCE(rarity_tier, 'legacy') AS rarity_tier,
            COUNT(*)::int AS total_count,
            COUNT(*) FILTER (
              WHERE COALESCE(disposition, 'regurgitated') = 'regurgitated'
                AND COALESCE(inventory_status, status, 'available') = 'available'
            )::int AS available_count
     FROM maw_squig_pool
     GROUP BY COALESCE(rarity_tier, 'legacy')
     ORDER BY COALESCE(rarity_tier, 'legacy')`
  );
  const dispositionRows = await pool.query(
    `SELECT
       COUNT(*) FILTER (
         WHERE COALESCE(disposition, 'regurgitated') = 'regurgitated'
           AND COALESCE(inventory_status, status, 'available') = 'available'
       )::int AS regurgitated_available,
       COUNT(*) FILTER (
         WHERE COALESCE(disposition, 'regurgitated') = 'swallowed'
           AND COALESCE(digestion_status, '') IN ('pending_transfer', 'pending_burn', 'burn_verified', 'receipt_failed')
       )::int AS awaiting_digestion,
       COUNT(*) FILTER (
         WHERE COALESCE(disposition, 'regurgitated') = 'swallowed'
           AND COALESCE(digestion_status, '') = 'digested'
       )::int AS permanently_digested
     FROM maw_squig_pool`
  );
  const payoutRows = await pool.query(
    `SELECT COALESCE(SUM(payout_amount) FILTER (WHERE received_at IS NOT NULL), 0) AS promised,
            COALESCE(SUM(payout_amount) FILTER (WHERE payout_status = 'paid'), 0) AS paid,
            COALESCE(SUM(ticket_count) FILTER (WHERE received_at IS NOT NULL), 0) AS tickets,
            COALESCE(SUM(jackpot_contribution_charm) FILTER (WHERE received_at IS NOT NULL), 0) AS contributions
     FROM maw_return_sessions
     WHERE guild_id = $1`,
    [String(interaction.guildId)]
  );
  const recentInventoryRows = await pool.query(
    `SELECT token_id, COALESCE(disposition, 'regurgitated') AS disposition,
            COALESCE(inventory_status, status, 'available') AS inventory_status,
            COALESCE(digestion_status, 'not_applicable') AS digestion_status,
            rarity_tier
     FROM maw_squig_pool
     ORDER BY created_at DESC NULLS LAST, id DESC
     LIMIT 8`
  );
  const currentEvent = await getOpenMawEvent(interaction.guildId).catch(() => null);
  let rankingInfo = currentEvent?.ranking_source_hash
    ? `${shortHash(currentEvent.ranking_source_hash)}\nRules: ${currentEvent.reward_rules_version || currentEvent.reward_model || 'legacy'}`
    : 'No open rarity event.';
  if (!currentEvent?.ranking_source_hash) {
    const index = (() => {
      try { return loadMawRankingIndex(); } catch { return null; }
    })();
    if (index) rankingInfo = `${shortHash(index.rankingSourceHash)}\nRules: ${MAW_REWARD_RULES_VERSION}`;
  }
  const counts = new Map(statusRows.rows.map((row) => [String(row.status), Number(row.count || 0)]));
  const total = [...counts.values()].reduce((sum, n) => sum + n, 0);
  const eventText = eventRows.rows.length
    ? eventRows.rows.map((row) => `Event ${row.id}: ${row.status}, received ${row.received_count}, pool ${row.pool_count}`).join('\n')
    : 'No Maw events yet.';
  const rarityOrder = ['legendary', 'epic', 'rare', 'uncommon', 'common', 'legacy'];
  const rarityMap = new Map(rarityRows.rows.map((row) => [String(row.rarity_tier), row]));
  const availableByRarity = rarityOrder
    .filter((key) => rarityMap.has(key))
    .map((key) => `${formatMawRarityLabel(key)}: ${rarityMap.get(key).available_count}`)
    .join('\n') || 'None';
  const totalByRarity = rarityOrder
    .filter((key) => rarityMap.has(key))
    .map((key) => `${formatMawRarityLabel(key)}: ${rarityMap.get(key).total_count}`)
    .join('\n') || 'None';
  const payoutStats = payoutRows.rows[0] || {};
  const dispositionStats = dispositionRows.rows[0] || {};
  const recentText = recentInventoryRows.rows.length
    ? recentInventoryRows.rows.map((row) =>
      `${formatToken(row.token_id)} • ${formatMawDispositionLabel(row.disposition)} • ${row.inventory_status} • ${row.digestion_status} • ${formatMawRarityLabel(row.rarity_tier)}`
    ).join('\n')
    : 'No inventory rows yet.';
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
      { name: 'Regurgitated and available', value: String(dispositionStats.regurgitated_available || 0), inline: true },
      { name: 'Awaiting digestion', value: String(dispositionStats.awaiting_digestion || 0), inline: true },
      { name: 'Permanently digested', value: String(dispositionStats.permanently_digested || 0), inline: true },
      { name: 'Available by rarity', value: truncateField(availableByRarity), inline: true },
      { name: 'Accepted by rarity', value: truncateField(totalByRarity), inline: true },
      { name: 'Payouts', value: `Promised: ${formatCharm(payoutStats.promised)} $CHARM\nPaid: ${formatCharm(payoutStats.paid)} $CHARM`, inline: true },
      { name: 'Ticket and jackpot totals', value: `Tickets issued: ${Number(payoutStats.tickets || 0)}\nJackpot contributions: ${formatCharm(payoutStats.contributions)} $CHARM\nCurrent jackpot: ${currentEvent ? `${formatCharm(currentEvent.jackpot_charm)} $CHARM` : 'No open event'}`, inline: false },
      { name: 'Ranking source', value: rankingInfo, inline: false },
      { name: 'Recent inventory audit', value: truncateField(recentText), inline: false },
      { name: 'Original event counts', value: truncateField(eventText), inline: false }
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

async function handleMawRank(interaction) {
  const tokenId = interaction.options.getInteger('token');
  try {
    const quote = getMawRewardQuote(tokenId);
    const embed = new EmbedBuilder()
      .setTitle(`Maw Rank Audit ${formatToken(quote.tokenId)}`)
      .setColor(0x5f3dc4)
      .addFields(
        { name: 'Maw Rank', value: formatMawAverageRank(quote.averageRank), inline: true },
        { name: 'Total UglyPoints', value: formatMawAverageRank(quote.totalUglyPoints), inline: true },
        { name: 'Legend', value: quote.legend || 'No', inline: true },
        { name: 'Rarity', value: quote.rarityLabel, inline: true },
        { name: 'Payout', value: `${formatCharm(quote.payoutCharm)} $CHARM`, inline: true },
        { name: 'Tickets', value: String(quote.ticketCount), inline: true },
        { name: 'Jackpot contribution', value: `${formatSignedCharm(quote.jackpotContributionCharm)} $CHARM`, inline: true },
        { name: 'Rules version', value: quote.rewardRulesVersion, inline: true },
        { name: 'Ranking source', value: shortHash(quote.rankingSourceHash), inline: true }
      );
    await interaction.reply({ embeds: [embed], flags: EPHEMERAL });
  } catch (err) {
    await postAdminLog(interaction.guild, 'Maw Ranking Data', `Rank audit failed for token ${tokenId}: ${String(err?.message || err).slice(0, 1200)}`);
    await interaction.reply({ content: `Could not quote that Squig: ${String(err?.message || err).slice(0, 300)}`, flags: EPHEMERAL });
  }
}

async function handleMawDigestionStatus(interaction) {
  const statusFilter = String(interaction.options.getString('status') || 'pending').trim().toLowerCase();
  const tokenFilter = interaction.options.getInteger('token');
  const allowedStatuses = new Set(['pending', 'pending_burn', 'receipt_failed', 'burn_verified', 'digested', 'all', 'retry_request']);
  const normalizedFilter = allowedStatuses.has(statusFilter) ? statusFilter : 'pending';
  if (normalizedFilter === 'retry_request') {
    if (!tokenFilter) {
      await interaction.reply({ content: 'Provide token:<id> when retrying a digestion admin request.', flags: EPHEMERAL });
      return;
    }
    const { rows } = await resolvePool().query(
      `SELECT id FROM maw_return_sessions
       WHERE guild_id = $1
         AND token_id = $2
         AND COALESCE(squig_disposition, 'regurgitated') = 'swallowed'
       ORDER BY id DESC
       LIMIT 1`,
      [String(interaction.guildId), String(tokenFilter)]
    );
    if (!rows[0]) {
      await interaction.reply({ content: 'No Swallowed Maw session found for that token.', flags: EPHEMERAL });
      return;
    }
    await interaction.deferReply({ ephemeral: true });
    const result = await ensureSwallowedDigestionRequest(rows[0].id).catch((err) => ({ ok: false, reason: String(err?.message || err) }));
    await interaction.editReply({ content: result.ok ? 'Digestion admin request retry completed or was already posted.' : `Retry failed: ${result.reason}` });
    return;
  }
  const params = [String(interaction.guildId)];
  let where = `s.guild_id = $1 AND COALESCE(s.squig_disposition, 'regurgitated') = 'swallowed'`;
  if (tokenFilter) {
    params.push(String(tokenFilter));
    where += ` AND s.token_id = $${params.length}`;
  }
  if (normalizedFilter === 'pending') {
    where += ` AND COALESCE(s.digestion_status, '') IN ('pending_transfer', 'pending_burn', 'burn_verified', 'receipt_failed')`;
  } else if (normalizedFilter !== 'all') {
    params.push(normalizedFilter);
    where += ` AND s.digestion_status = $${params.length}`;
  }
  const { rows } = await resolvePool().query(
    `SELECT s.*, p.id AS pool_squig_id, p.status AS pool_status, p.inventory_status,
            p.admin_digestion_message_id AS pool_admin_digestion_message_id
     FROM maw_return_sessions s
     LEFT JOIN maw_squig_pool p ON p.received_session_id = s.id
     WHERE ${where}
     ORDER BY s.received_at DESC NULLS LAST, s.id DESC
     LIMIT 12`,
    params
  );
  if (!rows.length) {
    await interaction.reply({ content: 'No matching Maw digestion workflows found.', flags: EPHEMERAL });
    return;
  }
  const config = getMawConfig();
  const lines = rows.map((row) => {
    const adminMessageId = row.admin_digestion_message_id || row.pool_admin_digestion_message_id;
    const received = row.received_at ? `<t:${Math.floor(new Date(row.received_at).getTime() / 1000)}:R>` : 'not received';
    const adminLink = formatAdminMessageLink(row.guild_id, config.digestionAdminChannelId, adminMessageId);
    const inbound = inboundTxUrl(row, config) || row.received_tx_hash || 'none';
    const burn = row.burn_transaction_url || 'none';
    const error = row.digestion_receipt_error ? `\nError: ${String(row.digestion_receipt_error).slice(0, 180)}` : '';
    return [
      `${formatToken(row.token_id)} • ${getSessionRarityLabel(row)} • Maw Rank ${row.average_rank == null ? 'Legacy' : formatMawAverageRank(row.average_rank)}`,
      `Feeder: <@${row.discord_user_id}> • Received: ${received}`,
      `Status: ${row.digestion_status || 'pending_transfer'} • Inventory: ${row.inventory_status || row.pool_status || 'pending'}`,
      `Inbound: ${inbound}`,
      `Admin request: ${adminLink}`,
      `Burn: ${burn}${error}`,
    ].join('\n');
  }).join('\n\n');
  const embed = new EmbedBuilder()
    .setTitle('Maw Digestion Workflows')
    .setColor(0xd9480f)
    .setDescription(truncateField(lines, 4000));
  await interaction.reply({ embeds: [embed], flags: EPHEMERAL });
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
            .setDisabled(true),
          new ButtonBuilder()
            .setCustomId('maw_explain:closed')
            .setLabel('WTF is this?')
            .setStyle(ButtonStyle.Secondary)
        ),
      ],
    };
  }

  const jackpot = formatCharm(event.jackpot_charm);
  const transferWindow = formatDurationMinutes(event.session_ttl_minutes);
  const embed = new EmbedBuilder()
    .setTitle('THE MAW IS HUNGRY')
    .setColor(event.draw_completed ? 0x2f9e44 : 0x8b1e3f)
    .setImage(MAW_PANEL_IMAGE_URL);
  if (isRarityMawEvent(event)) {
    embed
      .setDescription(
        `Feed an eligible Squig.\n\n` +
        `Its Maw Rank sets your $CHARM payout, Maw Tickets, and jackpot boost.\n` +
        `You’ll choose **Swallowed** or **Regurgitated** before the ${transferWindow} transfer timer starts.`
      )
      .addFields(
        { name: 'PROGRESS', value: `${event.received_count} / ${event.goal_count} fed\n${summary.openSlots} open spots`, inline: true },
        { name: 'JACKPOT', value: `${jackpot} $CHARM\n+${formatCharm(event.jackpot_contributed_charm)} contributed`, inline: true },
        { name: 'TICKETS ISSUED', value: String(summary.totalTicketCount || event.total_ticket_count || 0), inline: true }
      );
  } else {
    const reward = formatCharm(event.return_reward_charm);
    embed
      .setDescription(
        `Feed an eligible Squig.\n\n` +
        `This legacy Maw event pays ${reward} $CHARM and 1 Maw Ticket after the transfer is received and verified.`
      )
      .addFields(
        { name: 'PROGRESS', value: `${event.received_count} / ${event.goal_count} fed\n${summary.openSlots} open spots`, inline: true },
        { name: 'JACKPOT', value: `${jackpot} $CHARM`, inline: true },
        { name: 'TICKETS ISSUED', value: String(summary.totalTicketCount || event.total_ticket_count || 0), inline: true }
      );
  }
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
          .setDisabled(Boolean(event.draw_completed) || summary.openSlots <= 0),
        new ButtonBuilder()
          .setCustomId(`maw_explain:${event.id}`)
          .setLabel('WTF is this?')
          .setStyle(ButtonStyle.Secondary)
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

async function deleteMawPanelMessage(event, replacementMessage = null) {
  if (!event?.panel_channel_id || !event?.panel_message_id || !deps?.client) return false;
  const oldChannelId = String(event.panel_channel_id);
  const oldMessageId = String(event.panel_message_id);
  const replacementChannelId = String(replacementMessage?.channelId || replacementMessage?.channel?.id || '');
  if (replacementChannelId === oldChannelId && String(replacementMessage?.id || '') === oldMessageId) return false;

  const channel = replacementChannelId === oldChannelId
    ? replacementMessage.channel
    : await deps.client.channels.fetch(oldChannelId).catch(() => null);
  if (!channel?.messages?.fetch) return false;
  const message = await channel.messages.fetch(oldMessageId).catch(() => null);
  if (!message?.delete) return false;
  await message.delete().catch(() => null);
  return true;
}

async function moveMawPanelBelowMessage(event, anchorMessage) {
  if (!event?.id || !anchorMessage?.channel?.send) return false;
  const latestEvent = await getMawEventById(event.id);
  if (!latestEvent) return false;
  const summary = latestEvent.status === 'open' || latestEvent.draw_completed
    ? await getMawEventSummary(latestEvent)
    : null;
  const newPanel = await anchorMessage.channel.send(buildMawPanelPayload(summary));
  try {
    await resolvePool().query(
      `UPDATE maw_events
       SET panel_channel_id = $2,
           panel_message_id = $3,
           updated_at = NOW()
       WHERE id = $1`,
      [String(latestEvent.id), String(anchorMessage.channel.id), String(newPanel.id)]
    );
  } catch (err) {
    await newPanel.delete?.().catch(() => null);
    throw err;
  }
  await deleteMawPanelMessage(latestEvent, newPanel).catch(() => null);
  return true;
}

async function handleMawFeedStart(interaction) {
  const eventId = String(interaction.customId || '').split(':')[1] || '';
  const config = getMawConfig();
  if (!config.mawWalletAddress) {
    await interaction.reply({ content: 'The Malformed Maw wallet is not configured yet. Tell an admin it needs MAW_WALLET_ADDRESS.', flags: EPHEMERAL });
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
    await interaction.editReply({ content: 'The Maw is full or all remaining spots are temporarily reserved by active transfers.' });
    return true;
  }

  if (isRarityMawEvent(event)) {
    try {
      loadMawRankingIndex();
    } catch (err) {
      await postAdminLog(interaction.guild, 'Maw Ranking Data', `Feed start blocked for <@${interaction.user.id}>: ${String(err?.message || err).slice(0, 1200)}`);
      await interaction.editReply({ content: MAW_RANKING_FAILURE_MESSAGE });
      return true;
    }
  }

  const links = await deps.getWalletLinks(interaction.guildId, interaction.user.id);
  const wallets = links.map((row) => normalizeAddress(row.wallet_address)).filter(Boolean);
  if (!wallets.length) {
    await interaction.editReply({ content: 'You need a linked wallet before feeding the Maw.' });
    return true;
  }

  let eligible = [];
  try {
    eligible = await getEligibleMawSquigsForUser(interaction.guildId, interaction.user.id, wallets, config.squigContract, event);
  } catch (err) {
    const isRankingError = String(err?.code || '').startsWith('MAW_RANKING');
    await postAdminLog(interaction.guild, isRankingError ? 'Maw Ranking Data' : 'Maw Ownership Check', `Failed for <@${interaction.user.id}>: ${String(err?.message || err).slice(0, 1200)}`);
    await interaction.editReply({ content: isRankingError ? MAW_RANKING_FAILURE_MESSAGE : 'The Maw could not check your Squigs right now. Try again in a moment.' });
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

async function getEligibleMawSquigsForUser(guildId, userId, wallets, contractAddress, event = null) {
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
  const candidates = owned
    .filter((entry) => {
      const token = String(entry.tokenId);
      if (excluded.has(token) || seen.has(token)) return false;
      seen.add(token);
      return true;
    });
  if (!isRarityMawEvent(event)) return sortMawSquigsForDisplay(candidates);

  const enriched = [];
  let invalidRankingCount = 0;
  for (const entry of candidates) {
    try {
      enriched.push({ ...entry, quote: getMawRewardQuote(entry.tokenId) });
    } catch (err) {
      invalidRankingCount += 1;
      await postAdminLogByGuildId(guildId, 'Maw Ranking Data', `Excluded Squig ${formatToken(entry.tokenId)} from user ${userId}: ${String(err?.message || err).slice(0, 800)}`);
    }
  }
  if (candidates.length && !enriched.length && invalidRankingCount === candidates.length) {
    const err = new Error('Every otherwise-eligible owned Squig was missing a valid Maw ranking quote.');
    err.code = 'MAW_RANKING_NO_VALID_OWNED';
    throw err;
  }
  return sortMawSquigsForDisplay(enriched);
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

async function handleMawReviewSquigPageButton(interaction) {
  const match = String(interaction.customId || '').match(/^maw_review_select_page:([^:]+):(\d+)$/);
  if (!match) return false;
  const [, token, rawPage] = match;
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
  const selectionState = getPendingMawSelection(interaction.guildId, interaction.user.id, event.id);
  if (!selectionState?.squigs?.length) {
    await interaction.update({ content: 'This Maw Squig selector expired. Press Feed the Maw again if the Maw is still hungry.', embeds: [], components: [], attachments: [] });
    return true;
  }
  selectionState.page = clampMawSquigPage(selectionState.squigs, rawPage);
  const summary = await getMawEventSummary(event);
  await interaction.update({
    ...buildMawReviewPayload(event, summary, pending.tokenId, token, pending.quote, selectionState, interaction.user.id, pending.sourceWallet),
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
  const selectionState = getPendingMawSelection(interaction.guildId, interaction.user.id, eventId);
  if (!selectionState?.squigs?.length) {
    await interaction.update({ content: 'This Maw Squig selector expired. Press Feed the Maw again if the Maw is still hungry.', embeds: [], components: [], attachments: [] });
    return;
  }
  const selectedEntry = selectionState.squigs.find((entry) =>
    normalizeAddress(entry.wallet) === sourceWallet && String(entry.tokenId) === tokenId
  );
  if (!selectedEntry) {
    await interaction.update({ content: 'That Squig selection is no longer valid. Press Feed the Maw again.', components: [], attachments: [] });
    return;
  }
  const event = await getOpenMawEvent(interaction.guildId);
  if (!event || String(event.id) !== eventId) {
    await interaction.update({ content: 'The Maw is closed or this panel is stale.', components: [] });
    return;
  }
  const summary = await getMawEventSummary(event);
  if (summary.openSlots <= 0) {
    await interaction.update({ content: 'The Maw is full or all remaining spots are temporarily reserved by active transfers.', components: [] });
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
  let quote = selectedEntry.quote || null;
  if (isRarityMawEvent(event) && !quote) {
    try {
      quote = getMawRewardQuote(tokenId);
    } catch (err) {
      await postAdminLog(interaction.guild, 'Maw Ranking Data', `Review blocked for Squig ${formatToken(tokenId)}: ${String(err?.message || err).slice(0, 1200)}`);
      await interaction.update({ content: MAW_RANKING_FAILURE_MESSAGE, embeds: [], components: [], attachments: [] });
      return;
    }
  }

  const token = randomToken();
  pendingMawReviews.set(token, {
    guildId: String(interaction.guildId),
    userId: String(interaction.user.id),
    eventId: String(event.id),
    sourceWallet,
    tokenId,
    quote: snapshotMawRewardQuote(quote),
    expiresAt: Date.now() + PENDING_TTL_MS,
  });

  await interaction.update({
    ...buildMawReviewPayload(event, summary, tokenId, token, quote, selectionState, interaction.user.id, sourceWallet),
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

function buildMawReviewEmbed(event, summary, tokenId, imageUrl = null, quote = null) {
  const embed = new EmbedBuilder()
    .setTitle('Review Maw Return')
    .setColor(0x8b1e3f)
    .setDescription('Accepted Squigs enter the Maw Pool and may later be used for prizes, games, store rewards, onboarding, draws, or other community features.');
  if (quote) {
    const projectedJackpot = Math.floor(Number(event.jackpot_charm) || 0) + Math.floor(Number(quote.jackpotContributionCharm) || 0);
    embed.addFields(
      { name: 'Squig', value: formatToken(tokenId), inline: true },
      { name: 'Rarity', value: quote.rarityLabel, inline: true },
      { name: 'Maw Rank', value: `${formatMawAverageRank(quote.averageRank)}\n${formatMawAverageRank(quote.totalUglyPoints)} UglyPoints`, inline: true },
      { name: 'You receive', value: `${formatCharm(quote.payoutCharm)} $CHARM`, inline: true },
      { name: 'Maw Tickets', value: String(quote.ticketCount), inline: true },
      { name: 'Jackpot contribution', value: `${formatSignedCharm(quote.jackpotContributionCharm)} $CHARM`, inline: true },
      { name: 'Current jackpot', value: `${formatCharm(event.jackpot_charm)} $CHARM`, inline: true },
      { name: 'Projected jackpot', value: `${formatCharm(projectedJackpot)} $CHARM`, inline: true },
      { name: 'Event progress', value: `${event.received_count} / ${event.goal_count}`, inline: true },
      { name: 'Remaining open spots', value: String(summary.openSlots), inline: true }
    );
  } else {
    embed.addFields(
      { name: 'Squig', value: formatToken(tokenId), inline: true },
      { name: 'Payout', value: `${formatCharm(event.return_reward_charm)} $CHARM`, inline: true },
      { name: 'Ticket', value: '1 Maw Ticket', inline: true },
      { name: 'Current progress', value: `${event.received_count} / ${event.goal_count}`, inline: true },
      { name: 'Remaining open spots', value: String(summary.openSlots), inline: true }
    );
  }
  if (imageUrl) embed.setImage(imageUrl);
  return embed;
}

function buildMawReviewPayload(event, summary, tokenId, token, quote = null, selectionState = null, userId = null, sourceWallet = null) {
  const image = mawSquigImageAttachment(tokenId);
  const selectRows = selectionState?.squigs?.length > 1
    ? buildMawSquigSelectRows(event.id, userId || selectionState.userId, selectionState.squigs, selectionState.page, {
        placeholder: 'Choose a different Squig',
        selectedTokenId: tokenId,
        selectedWallet: sourceWallet,
        reviewToken: token,
      })
    : [];
  return {
    content: '',
    embeds: [buildMawReviewEmbed(event, summary, tokenId, image.imageUrl, quote)],
    components: [...selectRows, buildMawReviewRow(token)],
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
  if (isRarityMawEvent(event) && !pending.quote) {
    await postAdminLog(interaction.guild, 'Maw Ranking Data', `Final confirmation blocked for Squig ${formatToken(pending.tokenId)}: missing pending reward quote.`);
    await interaction.update({ content: MAW_RANKING_FAILURE_MESSAGE, embeds: [], components: [], attachments: [] });
    return true;
  }
  pending.reviewToken = token;
  pendingMawSelections.delete(mawSelectionKey(interaction.guildId, interaction.user.id, event.id));
  await interaction.update(buildMawFateSelectionPayload(event, pending, token));
  return true;
}

function buildMawFateSelectionPayload(event, pending, token) {
  const embed = new EmbedBuilder()
    .setTitle('WHAT SHOULD THE MAW DO WITH YOUR SQUIG?')
    .setColor(0x8b1e3f)
    .setDescription(
      `**Swallow It**\n` +
      `The Squig will be sent to the Malformed Maw wallet and permanently burned by an admin after it is received.\n\n` +
      `**Regurgitate It**\n` +
      `The Squig will be added to the Maw Pool for future games, giveaways, incentives, prizes, and community rewards.\n\n` +
      `Both choices receive the same rarity-based $CHARM payout, Maw Tickets, and jackpot contribution.`
    )
    .addFields(
      { name: 'Squig', value: formatToken(pending.tokenId), inline: true },
      { name: 'Rarity', value: pending.quote?.rarityLabel || 'Legacy', inline: true },
      { name: 'Payout', value: `${formatCharm(pending.quote?.payoutCharm ?? event.return_reward_charm ?? 0)} $CHARM`, inline: true }
    );
  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`maw_fate_select:${token}:swallowed`)
      .setLabel('Swallow It')
      .setStyle(ButtonStyle.Danger),
    new ButtonBuilder()
      .setCustomId(`maw_fate_select:${token}:regurgitated`)
      .setLabel('Regurgitate It')
      .setStyle(ButtonStyle.Success),
    new ButtonBuilder()
      .setCustomId(`maw_cancel:${token}`)
      .setLabel('Cancel')
      .setStyle(ButtonStyle.Secondary)
  );
  return { content: '', embeds: [embed], components: [row], attachments: [] };
}

async function handleMawFateSelect(interaction) {
  const match = String(interaction.customId || '').match(/^maw_fate_select:([^:]+):(swallowed|regurgitated)$/);
  if (!match) return false;
  const [, token, disposition] = match;
  const pending = getPendingMawReview(token, interaction.user.id);
  if (!pending) {
    await interaction.update({ content: 'This Maw fate selection expired. Start again if the Maw is still hungry.', embeds: [], components: [], attachments: [] });
    return true;
  }
  const event = await getMawEventById(pending.eventId);
  if (!event || event.status !== 'open') {
    await interaction.update({ content: 'The Maw closed before the timer started.', embeds: [], components: [], attachments: [] });
    return true;
  }
  pending.squigDisposition = normalizeDisposition(disposition);
  console.info(`[Maw] User ${interaction.user.id} selected ${pending.squigDisposition} for Squig ${pending.tokenId}`);
  await postAdminLog(interaction.guild, 'Maw Fate Selected', `<@${interaction.user.id}> selected ${formatMawDispositionLabel(pending.squigDisposition)} for Squig ${formatToken(pending.tokenId)}.`).catch(() => null);
  await interaction.update(buildMawFinalConfirmationPayload(event, pending, token));
  return true;
}

function buildMawFinalConfirmationPayload(event, pending, token) {
  const quote = pending.quote || null;
  const disposition = normalizeDisposition(pending.squigDisposition);
  const isSwallowed = disposition === MAW_DISPOSITIONS.SWALLOWED;
  const title = `${isSwallowed ? 'SWALLOW' : 'REGURGITATE'} SQUIG ${formatToken(pending.tokenId)}?`;
  const fateText = isSwallowed
    ? `After the Maw receives it, the Squig will enter the digestion queue. An admin will permanently burn it and publish the completed burn transaction.`
    : `After the Maw receives it, the Squig will be added to the Maw Pool and may return as a future game prize, giveaway, incentive, onboarding reward, or community reward.`;
  const embed = new EmbedBuilder()
    .setTitle(title)
    .setColor(0xd9480f)
    .setDescription(
      `You will send Squig ${formatToken(pending.tokenId)} to the Malformed Maw wallet.\n\n` +
      `${fateText}\n\n` +
      `This choice cannot be changed after the transfer session begins.\n\n` +
      `You will receive:\n\n` +
      `• ${formatCharm(quote ? quote.payoutCharm : event.return_reward_charm)} $CHARM\n` +
      `• ${quote ? quote.ticketCount : 1} Maw Ticket${(quote ? quote.ticketCount : 1) === 1 ? '' : 's'}\n` +
      `• ${formatSignedCharm(quote ? quote.jackpotContributionCharm : 0)} $CHARM added to the Maw Jackpot\n\n` +
      `After confirming, you will have ${formatDurationMinutes(event.session_ttl_minutes)} to complete the transfer.`
    )
    .addFields(
      { name: `Only Squig ${formatToken(pending.tokenId)} counts for this session.`, value: 'No substitutions. The Maw reads receipts.', inline: false },
      { name: 'Rarity', value: quote ? quote.rarityLabel : 'Legacy flat event', inline: true },
      { name: 'Maw Rank', value: quote ? formatMawAverageRank(quote.averageRank) : 'Legacy flat event', inline: true },
      { name: 'Malformed Maw Wallet', value: 'The wallet address is revealed after you confirm and the timer starts.', inline: false }
    );
  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`maw_confirm_start_timer:${token}`)
      .setLabel(isSwallowed ? 'Feed It to Be Swallowed' : 'Add It to the Maw Pool')
      .setStyle(ButtonStyle.Danger),
    new ButtonBuilder()
      .setCustomId(`maw_cancel:${token}`)
      .setLabel('Cancel')
      .setStyle(ButtonStyle.Secondary)
  );
  return { content: '', embeds: [embed], components: [row], attachments: [] };
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
  if (!isValidMawDisposition(pending.squigDisposition)) {
    await interaction.update({ content: 'Choose whether the Maw should Swallow or Regurgitate this Squig before starting the transfer timer.', embeds: [], components: [], attachments: [] });
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
  if (!config.mawWalletAddress) return { ok: false, reason: 'The Malformed Maw wallet is not configured.' };
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
    const quote = snapshotMawRewardQuote(pending.quote);
    if (isRarityMawEvent(event) && !quote) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: MAW_RANKING_FAILURE_MESSAGE };
    }
    const disposition = normalizeDisposition(pending.squigDisposition);
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
      return { ok: false, reason: 'The Maw is full or all remaining spots are temporarily reserved by active transfers.' };
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
         (event_id, guild_id, discord_user_id, source_wallet, contract_address, token_id, status,
          expires_at, payout_amount, overall_rank, collection_rank, average_rank, rarity_tier,
          ticket_count, jackpot_contribution_charm, reward_rules_version, ranking_source_hash,
          squig_disposition, digestion_status,
          created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, 'awaiting_transfer', NOW() + ($7::int * INTERVAL '1 minute'),
          $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, NOW(), NOW())
       RETURNING *`,
      [
        String(event.id),
        String(pending.guildId),
        String(pending.userId),
        normalizeAddress(pending.sourceWallet),
        config.squigContract,
        String(pending.tokenId),
        Math.floor(Number(event.session_ttl_minutes) || config.sessionTtlMinutes),
        quote ? quote.payoutCharm : null,
        quote ? quote.overallRank : null,
        quote ? quote.collectionRank : null,
        quote ? quote.averageRank : null,
        quote ? quote.rarityKey : null,
        quote ? quote.ticketCount : 1,
        quote ? quote.jackpotContributionCharm : 0,
        quote ? quote.rewardRulesVersion : null,
        quote ? quote.rankingSourceHash : null,
        disposition,
        mawDispositionDigestionStatus(disposition, false),
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
  const snapshot = resolveMawSessionRewardSnapshot(session, event);
  const disposition = getSessionDisposition(session);
  const isSwallowed = disposition === MAW_DISPOSITIONS.SWALLOWED;
  return new EmbedBuilder()
    .setTitle('Maw Session Created')
    .setColor(0x8b1e3f)
    .setDescription(
      `Send only Squig ${formatToken(session.token_id)} to the Malformed Maw wallet shown below.\n\n` +
      (isSwallowed
        ? 'After it is received, it will enter the digestion queue and be permanently burned by an admin.'
        : 'After it is received, it will be added to the Maw Pool.')
    )
    .addFields(
      { name: 'Squig', value: formatToken(session.token_id), inline: true },
      { name: 'Fate', value: getSessionDispositionLabel(session), inline: true },
      { name: 'Rarity', value: snapshot.rarityLabel, inline: true },
      { name: 'Maw Rank', value: session.average_rank == null ? 'Legacy flat event' : formatMawAverageRank(session.average_rank), inline: true },
      { name: 'Payout', value: `${formatCharm(snapshot.payoutCharm)} $CHARM`, inline: true },
      { name: 'Ticket count', value: String(snapshot.ticketCount), inline: true },
      { name: 'Jackpot contribution', value: `${formatSignedCharm(snapshot.jackpotContributionCharm)} $CHARM`, inline: true },
      { name: 'Status', value: 'Awaiting transfer', inline: true },
      { name: 'Expires', value: `<t:${Math.floor(new Date(session.expires_at).getTime() / 1000)}:R>`, inline: true },
      { name: 'Selected source wallet', value: `\`${session.source_wallet}\``, inline: false },
      { name: 'Malformed Maw Wallet', value: `\`${config.mawWalletAddress}\``, inline: false }
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
  const snapshot = resolveMawSessionRewardSnapshot(session, event || {});
  const fields = [
    { name: 'Squig', value: formatToken(session.token_id), inline: true },
    { name: 'Fate', value: getSessionDispositionLabel(session), inline: true },
    { name: 'Status', value: String(session.status), inline: true },
    { name: 'NFT lifecycle', value: digestionStatusText(session.squig_disposition, session.digestion_status), inline: true },
    { name: 'Rarity', value: snapshot.rarityLabel, inline: true },
    { name: 'Maw Rank', value: formatSessionMawRank(session), inline: true },
    { name: 'Payout', value: `${formatCharm(snapshot.payoutCharm)} $CHARM${session.payout_status ? ` (${session.payout_status})` : ''}`, inline: true },
    { name: 'Ticket count', value: String(snapshot.ticketCount), inline: true },
    { name: 'Jackpot contribution', value: `${formatSignedCharm(snapshot.jackpotContributionCharm)} $CHARM`, inline: true },
  ];
  if (String(session.status) === 'awaiting_transfer') {
    fields.push({ name: 'Expires', value: `<t:${Math.floor(new Date(session.expires_at).getTime() / 1000)}:R>`, inline: true });
  }
  if (session.ticket_id) fields.push({ name: 'First ticket row', value: String(session.ticket_id), inline: true });
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
           inventory_status = 'reserved_for_claim',
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
      WHERE ${poolEligibilityWhere()}
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

function buildPrizeOfferPayload(claim, squig, disabled = false, options = {}) {
  const config = getMawConfig();
  const image = mawSquigImageAttachment(squig?.token_id);
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
  if (image.imageUrl) embed.setImage(image.imageUrl);
  return {
    embeds: [embed],
    components: [buildPrizeOfferRow(claim, disabled)],
    ...(options.replaceAttachments ? { attachments: [] } : {}),
    ...(image.files.length ? { files: image.files } : {}),
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
           inventory_status = 'distributed',
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
           inventory_status = 'available',
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
  const rows = await resolvePool().query(`SELECT * FROM maw_squig_pool WHERE ${poolEligibilityWhere()}`);
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
           inventory_status = 'available',
           reserved_claim_id = NULL,
           times_rerolled_away = times_rerolled_away + 1,
           updated_at = NOW()
       WHERE id = $1`,
      [String(oldSquig.id)]
    );
    await db.query(
      `UPDATE maw_squig_pool
       SET status = 'reserved_for_claim',
           inventory_status = 'reserved_for_claim',
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
           inventory_status = 'distributed',
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
  await message.edit(buildPrizeOfferPayload(claim, squig, disabled || String(claim.status) !== 'offered', { replaceAttachments: true }));
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
  if (normalizeAddress(transfer?.to) !== normalizeAddress(config.mawWalletAddress)) {
    return { status: 'unmatched', reason: 'wrong_destination' };
  }
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

    const snapshot = resolveMawSessionRewardSnapshot(session, event);
    const ticketNumberRows = await db.query(
      `SELECT COALESCE(MAX(ticket_number), 0)::int + 1 AS next_number FROM maw_tickets WHERE event_id = $1`,
      [String(event.id)]
    );
    const firstTicketNumber = Number(ticketNumberRows.rows[0]?.next_number || 1);
    const ticketRows = await db.query(
      `WITH inserted AS (
         INSERT INTO maw_tickets
           (event_id, ticket_number, discord_user_id, return_session_id, ticket_slot, contract_address, token_id, created_at)
         SELECT $1, $2::int + gs.slot - 1, $3, $4, gs.slot, $5, $6, NOW()
         FROM generate_series(1, $7::int) AS gs(slot)
         ON CONFLICT (event_id, return_session_id, ticket_slot) DO NOTHING
         RETURNING *
       )
       SELECT * FROM inserted ORDER BY ticket_slot ASC`,
      [
        String(event.id),
        firstTicketNumber,
        String(session.discord_user_id),
        String(session.id),
        config.squigContract,
        String(transfer.tokenId),
        snapshot.ticketCount,
      ]
    );
    const tickets = ticketRows.rows.length ? ticketRows.rows : (await db.query(
      `SELECT * FROM maw_tickets WHERE event_id = $1 AND return_session_id = $2 ORDER BY ticket_slot ASC`,
      [String(event.id), String(session.id)]
    )).rows;
    const ticketSummary = summarizeMawTicketRows(tickets);
    const firstTicket = tickets[0] || null;
    if (!firstTicket || !ticketSummary) {
      throw new Error(`No Maw tickets were created for return session ${session.id}.`);
    }
    const disposition = getSessionDisposition(session);
    const inventoryStatus = mawDispositionInventoryStatus(disposition);
    const digestionStatus = mawDispositionDigestionStatus(disposition, true);

    const poolInsert = await db.query(
      `INSERT INTO maw_squig_pool
         (event_id, contract_address, token_id, original_sender_discord_id, original_sender_wallet,
          received_session_id, received_tx_hash, status, overall_rank, collection_rank, average_rank,
          rarity_tier, original_payout_amount, ticket_count, jackpot_contribution_charm,
          reward_rules_version, ranking_source_hash, disposition, inventory_status, digestion_status,
          original_feeder_user_id, original_feeder_wallet, inbound_transaction_hash, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
          $17, $18, $19, $20, $21, $22, $23, NOW(), NOW())
       RETURNING *`,
      [
        String(event.id),
        config.squigContract,
        String(transfer.tokenId),
        String(session.discord_user_id),
        transfer.from,
        String(session.id),
        transfer.txHash,
        inventoryStatus,
        session.overall_rank ?? null,
        session.collection_rank ?? null,
        session.average_rank ?? null,
        session.rarity_tier ?? null,
        snapshot.payoutCharm,
        snapshot.ticketCount,
        snapshot.jackpotContributionCharm,
        session.reward_rules_version ?? null,
        session.ranking_source_hash ?? null,
        disposition,
        inventoryStatus,
        digestionStatus,
        String(session.discord_user_id),
        transfer.from,
        transfer.txHash,
      ]
    );
    const updatedEvent = await db.query(
      `UPDATE maw_events
       SET received_count = received_count + 1,
           total_ticket_count = total_ticket_count + $2,
           jackpot_contributed_charm = jackpot_contributed_charm + $3,
           jackpot_charm = jackpot_charm + $3,
           updated_at = NOW()
       WHERE id = $1
       RETURNING *`,
      [String(event.id), snapshot.ticketCount, snapshot.jackpotContributionCharm]
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
           digestion_status = $6,
           updated_at = NOW()
       WHERE id = $1`,
      [String(session.id), transfer.txHash, transfer.logIndex, snapshot.payoutCharm, String(firstTicket.id), digestionStatus]
    );
    await db.query('COMMIT');
    done = true;
    console.info(`[Maw] ${formatMawDispositionLabel(disposition)} inbound transfer accepted for Squig ${transfer.tokenId}; session ${session.id}.`);
    if (disposition === MAW_DISPOSITIONS.SWALLOWED) {
      console.info(`[Maw] Pending-burn inventory created for Squig ${transfer.tokenId}; pool row ${poolInsert.rows[0]?.id || 'unknown'}.`);
    }
    sessionForPayout = session.id;
    eventIdForPanel = event.id;
    result = { status: 'matched', event: updatedEvent.rows[0], session, tickets, ticketSummary, poolSquig: poolInsert.rows[0] };
  } catch (err) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    db.release();
  }

  if (sessionForPayout) {
    await ensureSwallowedDigestionRequest(sessionForPayout).catch(async (err) => {
      await postAdminLogByGuildId(guildId, 'Maw Digestion Request Failed', `Return session ${sessionForPayout}: ${String(err?.message || err).slice(0, 1000)}`);
    });
    const paid = await payPendingMawReturnSession(sessionForPayout).catch(async (err) => {
      await postAdminLogByGuildId(guildId, 'Maw Payout Failure', `Return session ${sessionForPayout}: ${String(err?.message || err).slice(0, 800)}`);
      return null;
    });
    if (paid?.ok) {
      const receiptResult = await postMawReceiptMessages(paid).catch(async (err) => {
        await postAdminLogByGuildId(guildId, 'Maw Receipt Failure', String(err?.message || err).slice(0, 800));
        return null;
      });
      if (!receiptResult?.panelMoved) await updateMawPanel(eventIdForPanel).catch(() => null);
    } else {
      await updateMawPanel(eventIdForPanel).catch(() => null);
    }
    await maybeCompleteMawGoal(eventIdForPanel).catch(async (err) => {
      await postAdminLogByGuildId(guildId, 'Maw Draw Failure', String(err?.message || err).slice(0, 800));
    });
  }
  return result;
}

async function payPendingMawReturnSession(sessionId) {
  const pool = resolvePool();
  const db = await pool.connect();
  let session;
  let event;
  let tickets = [];
  let done = false;
  try {
    await db.query('BEGIN');
    const rows = await db.query(
      `SELECT s.*, e.goal_count, e.received_count, e.return_reward_charm, e.guild_id AS event_guild_id,
              e.jackpot_charm, e.jackpot_base_charm, e.jackpot_contributed_charm, e.total_ticket_count
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
    const ticketRows = await db.query(
      `SELECT * FROM maw_tickets WHERE event_id = $1 AND return_session_id = $2 ORDER BY ticket_number ASC`,
      [String(session.event_id), String(session.id)]
    );
    tickets = ticketRows.rows;
    await db.query('COMMIT');
    done = true;
  } catch (err) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    db.release();
  }

  try {
    const snapshot = resolveMawSessionRewardSnapshot(session, event || {});
    const payout = await awardCharmToUser(session.guild_id, session.discord_user_id, snapshot.payoutCharm, snapshot.payoutContext);
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
    return { ok: true, session: paidRows.rows[0], event: refreshedEvent, tickets, ticketSummary: summarizeMawTicketRows(tickets) };
  } catch (err) {
    await pool.query(
      `UPDATE maw_return_sessions
       SET status = 'received',
           payout_status = 'pending',
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

async function postMawReceiptMessages({ session, event, tickets = [], ticketSummary = null }) {
  const summary = ticketSummary || summarizeMawTicketRows(tickets) || {
    text: session.ticket_id ? `row ${session.ticket_id}` : 'pending',
    ticketCount: Math.max(1, Math.floor(Number(session.ticket_count) || 1)),
  };
  const snapshot = resolveMawSessionRewardSnapshot(session, event);
  const disposition = getSessionDisposition(session);
  const isSwallowed = disposition === MAW_DISPOSITIONS.SWALLOWED;
  const receiptTitle = isSwallowed
    ? `THE MAW ACCEPTED SQUIG ${formatToken(session.token_id)}`
    : `THE MAW REGURGITATED SQUIG ${formatToken(session.token_id)}`;
  const receiptStatus = isSwallowed
    ? `Squig ${formatToken(session.token_id)} is now inside the Maw and awaiting final digestion.\n\nA final receipt will be posted after the burn has been completed and verified.`
    : `Squig ${formatToken(session.token_id)} has been added to the Maw Pool and may return as a future community reward.`;
  const ticketFieldName = Number(summary.ticketCount || snapshot.ticketCount) === 1 ? 'Maw Ticket' : 'Maw Tickets';
  const user = await deps.client?.users?.fetch?.(session.discord_user_id).catch(() => null);
  if (user?.send) {
    const embed = new EmbedBuilder()
      .setTitle(receiptTitle)
      .setColor(0x2f9e44)
      .setDescription(`Payout released: ${formatCharm(session.payout_amount)} $CHARM\n\n${receiptStatus}`)
      .addFields(
        { name: 'Fate', value: getSessionDispositionLabel(session), inline: true },
        { name: 'Rarity', value: snapshot.rarityLabel, inline: true },
        { name: 'Maw Rank', value: session.average_rank == null ? 'Legacy flat event' : formatMawAverageRank(session.average_rank), inline: true },
        { name: ticketFieldName, value: summary.text, inline: true },
        { name: 'Jackpot contribution', value: `${formatSignedCharm(snapshot.jackpotContributionCharm)} $CHARM`, inline: true },
        { name: 'Progress', value: `${event.received_count} / ${event.goal_count}`, inline: true },
        { name: 'Status', value: receiptStatus, inline: false }
      );
    await user.send({ embeds: [embed] }).catch(() => null);
  }
  const content =
    `${receiptTitle}\n\n` +
    `Feeder: <@${session.discord_user_id}>\n` +
    `Rarity: ${snapshot.rarityLabel}\n` +
    `Maw Rank: ${session.average_rank == null ? 'Legacy flat event' : formatMawAverageRank(session.average_rank)}\n` +
    `Payout: ${formatCharm(session.payout_amount)} $CHARM\n` +
    `${ticketFieldName}: ${summary.text}\n` +
    `Jackpot contribution: ${formatSignedCharm(snapshot.jackpotContributionCharm)} $CHARM\n` +
    `Current jackpot: ${formatCharm(event.jackpot_charm)} $CHARM\n` +
    `Progress: ${event.received_count} / ${event.goal_count}\n` +
    `\n${receiptStatus}`;
  const receiptMessage = await postMawFeed(event, { content }).catch(() => null);
  const panelMoved = receiptMessage
    ? await moveMawPanelBelowMessage(event, receiptMessage).catch(async (err) => {
      await postAdminLogByGuildId(event.guild_id, 'Maw Panel Move Failure', String(err?.message || err).slice(0, 800));
      return false;
    })
    : false;
  return { receiptPosted: Boolean(receiptMessage), panelMoved };
}

async function ensureSwallowedDigestionRequest(sessionId) {
  const pool = resolvePool();
  const config = getMawConfig();
  if (!deps?.client) return { ok: false, reason: 'Discord client unavailable.' };
  const db = await pool.connect();
  let done = false;
  try {
    await db.query('BEGIN');
    const rows = await db.query(
      `SELECT s.*, p.id AS pool_squig_id, p.admin_digestion_message_id AS pool_admin_digestion_message_id
       FROM maw_return_sessions s
       LEFT JOIN maw_squig_pool p ON p.received_session_id = s.id
       WHERE s.id = $1
       FOR UPDATE OF s`,
      [String(sessionId)]
    );
    const session = rows.rows[0];
    if (!session || getSessionDisposition(session) !== MAW_DISPOSITIONS.SWALLOWED) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: true, skipped: true };
    }
    if (session.admin_digestion_message_id && !String(session.admin_digestion_message_id).startsWith('failed:')) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: true, skipped: true, messageId: session.admin_digestion_message_id };
    }
    const poolRows = await db.query(
      `SELECT * FROM maw_squig_pool WHERE received_session_id = $1 FOR UPDATE`,
      [String(session.id)]
    );
    const poolSquig = poolRows.rows[0];
    if (!poolSquig) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'Swallowed pool record missing.' };
    }
    if (poolSquig.admin_digestion_message_id && !String(poolSquig.admin_digestion_message_id).startsWith('failed:')) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: true, skipped: true, messageId: poolSquig.admin_digestion_message_id };
    }

    const channel = await deps.client.channels.fetch(config.digestionAdminChannelId).catch(() => null);
    if (!channel?.isTextBased?.()) {
      throw new Error(`Maw digestion admin channel ${config.digestionAdminChannelId} is unavailable.`);
    }
    const message = await channel.send(buildMawDigestionRequestPayload(session, poolSquig, config));
    await db.query(
      `UPDATE maw_return_sessions
       SET admin_digestion_message_id = $2,
           digestion_status = 'pending_burn',
           updated_at = NOW()
       WHERE id = $1`,
      [String(session.id), String(message.id)]
    );
    await db.query(
      `UPDATE maw_squig_pool
       SET admin_digestion_message_id = $2,
           digestion_status = 'pending_burn',
           inventory_status = 'pending_burn',
           status = 'pending_burn',
           updated_at = NOW()
       WHERE id = $1`,
      [String(poolSquig.id), String(message.id)]
    );
    await db.query('COMMIT');
    done = true;
    console.info(`[Maw] Admin digestion request posted for Squig ${session.token_id} in channel ${config.digestionAdminChannelId}.`);
    await postAdminLogByGuildId(session.guild_id, 'Maw Digestion Request Posted', `${createDigestionLogPrefix(session)} awaiting burn. Message ${message.id}.`).catch(() => null);
    return { ok: true, messageId: message.id };
  } catch (err) {
    if (!done) {
      await db.query('ROLLBACK').catch(() => null);
      await pool.query(
        `UPDATE maw_return_sessions
         SET digestion_receipt_error = $2,
             updated_at = NOW()
         WHERE id = $1`,
        [String(sessionId), `admin_request_failed:${String(err?.message || err).slice(0, 500)}`]
      ).catch(() => null);
    }
    console.warn('[Maw] admin digestion request failed:', String(err?.message || err));
    throw err;
  } finally {
    db.release();
  }
}

function buildMawDigestionRequestPayload(session, poolSquig, config) {
  const inboundUrl = inboundTxUrl(session, config) || session.received_tx_hash || 'Unavailable';
  const receivedTs = session.received_at ? Math.floor(new Date(session.received_at).getTime() / 1000) : null;
  const embed = new EmbedBuilder()
    .setTitle('SQUIG AWAITING DIGESTION')
    .setColor(0xd9480f)
    .setDescription('This Squig was marked Swallowed and must now be permanently burned.')
    .addFields(
      { name: 'Squig', value: formatToken(session.token_id), inline: true },
      { name: 'Original feeder', value: `<@${session.discord_user_id}>`, inline: true },
      { name: 'Rarity', value: getSessionRarityLabel(session), inline: true },
      { name: 'Maw Rank', value: session.average_rank == null ? 'Legacy flat event' : formatMawAverageRank(session.average_rank), inline: true },
      { name: 'Source wallet', value: `\`${session.source_wallet || poolSquig.original_sender_wallet || 'unknown'}\``, inline: false },
      { name: 'Current wallet', value: `\`${config.mawWalletAddress}\``, inline: false },
      { name: 'Inbound transaction', value: inboundUrl, inline: false },
      { name: 'Received', value: receivedTs ? `<t:${receivedTs}:F>` : 'Unknown', inline: true },
      { name: 'Next step', value: 'After completing the burn from the Malformed Maw wallet, use the button below and provide the Etherscan transaction URL.', inline: false }
    );
  return {
    embeds: [embed],
    components: [buildDigestionActionRow(session.id, false)],
  };
}

function buildDigestionActionRow(sessionId, completed = false, retryReceipt = false) {
  const button = completed
    ? new ButtonBuilder()
      .setCustomId(`maw_submit_burn_tx:${sessionId}`)
      .setLabel('✅ Digestion Complete')
      .setStyle(ButtonStyle.Success)
      .setDisabled(true)
    : new ButtonBuilder()
      .setCustomId(retryReceipt ? `maw_retry_digestion_receipt:${sessionId}` : `maw_submit_burn_tx:${sessionId}`)
      .setLabel(retryReceipt ? 'Retry Digestion Receipt' : 'Submit Burn Transaction')
      .setStyle(retryReceipt ? ButtonStyle.Secondary : ButtonStyle.Danger);
  return new ActionRowBuilder().addComponents(button);
}

async function handleMawSubmitBurnButton(interaction) {
  if (!isAdmin(interaction)) {
    console.warn(`[Maw] Unauthorized burn submission button attempt by ${interaction.user?.id || 'unknown'}.`);
    await interaction.reply({ content: 'Admin only.', flags: EPHEMERAL });
    return true;
  }
  const sessionId = String(interaction.customId || '').split(':')[1] || '';
  const session = await getMawSessionWithPool(sessionId);
  if (session && String(session.guild_id) !== String(interaction.guildId)) {
    await interaction.reply({ content: 'That digestion request is not available here.', flags: EPHEMERAL });
    return true;
  }
  if (!session || getSessionDisposition(session) !== MAW_DISPOSITIONS.SWALLOWED || String(session.digestion_status || '') !== 'pending_burn') {
    if (String(session?.digestion_status || '') === 'receipt_failed' && session?.burn_transaction_hash) {
      await interaction.reply({ content: 'The burn is already verified. Use Retry Digestion Receipt instead of submitting another burn transaction.', flags: EPHEMERAL });
      return true;
    }
    await interaction.reply({ content: 'This Squig’s digestion has already been completed or is not ready for burn confirmation.', flags: EPHEMERAL });
    return true;
  }
  const modal = new ModalBuilder()
    .setCustomId(`maw_burn_tx_modal:${sessionId}`)
    .setTitle('Confirm Squig Digestion')
    .addComponents(
      new ActionRowBuilder().addComponents(
        new TextInputBuilder()
          .setCustomId('burn_tx')
          .setLabel('Burn transaction Etherscan URL')
          .setPlaceholder('https://etherscan.io/tx/0x...')
          .setRequired(true)
          .setStyle(TextInputStyle.Short)
      )
    );
  await interaction.showModal(modal);
  return true;
}

async function handleMawBurnTransactionModal(interaction) {
  if (!isAdmin(interaction)) {
    console.warn(`[Maw] Unauthorized burn modal attempt by ${interaction.user?.id || 'unknown'}.`);
    await interaction.reply({ content: 'Admin only.', flags: EPHEMERAL });
    return true;
  }
  const sessionId = String(interaction.customId || '').split(':')[1] || '';
  const config = getMawConfig();
  const input = interaction.fields?.getTextInputValue?.('burn_tx') || '';
  let parsed;
  try {
    parsed = parseBurnTransactionInput(input, config.explorerBaseUrl);
  } catch (err) {
    await interaction.reply({ content: `Burn transaction rejected: ${err.message}`, flags: EPHEMERAL });
    return true;
  }

  await interaction.deferReply({ ephemeral: true });
  await postAdminLog(interaction.guild, 'Maw Burn Submission', `<@${interaction.user.id}> submitted burn tx ${parsed.hash} for Maw session ${sessionId}.`).catch(() => null);
  console.info(`[Maw] Burn submission received for session ${sessionId}: ${parsed.hash}`);
  try {
    const session = await getMawSessionWithPool(sessionId);
    if (session && String(session.guild_id) !== String(interaction.guildId)) {
      await interaction.editReply({ content: 'That digestion request is not available here.' });
      return true;
    }
    if (!session || getSessionDisposition(session) !== MAW_DISPOSITIONS.SWALLOWED) {
      await interaction.editReply({ content: 'This Squig is not pending Swallowed digestion.' });
      return true;
    }
    if (String(session.digestion_status || '') !== 'pending_burn') {
      await interaction.editReply({ content: 'This Squig’s digestion has already been completed or is currently being processed.' });
      return true;
    }
    if (session.burn_transaction_hash) {
      await interaction.editReply({ content: 'This Squig already has a verified burn transaction. Retry the final receipt instead.' });
      return true;
    }
    const validation = await validateMawBurnTransaction({
      session,
      txHash: parsed.hash,
      provider: getMawProvider(),
      config,
    });
    const completed = await completeMawDigestionBurn({
      sessionId,
      burnTransactionHash: parsed.hash,
      burnTransactionUrl: parsed.url,
      confirmedBy: interaction.user.id,
      validation,
    });
    await interaction.editReply({ content: `Burn verified for Squig ${formatToken(completed.session.token_id)}. Posting final digestion receipt...` });
    const receipt = await postFinalDigestionReceiptForSession(sessionId).catch(async (err) => {
      await markDigestionReceiptFailed(sessionId, err);
      return { ok: false, reason: String(err?.message || err) };
    });
    if (receipt?.ok) {
      await interaction.followUp({ content: 'Final digestion receipt posted.', flags: EPHEMERAL }).catch(() => null);
    } else {
      await interaction.followUp({ content: `Burn is verified, but the final receipt failed: ${receipt?.reason || 'unknown error'}. Use the retry button or /maw digestion to retry.`, flags: EPHEMERAL }).catch(() => null);
    }
  } catch (err) {
    console.warn('[Maw] burn transaction rejected:', String(err?.message || err));
    await postAdminLog(interaction.guild, 'Maw Burn Rejected', `Session ${sessionId}: ${String(err?.message || err).slice(0, 1000)}`).catch(() => null);
    await interaction.editReply({ content: `Burn transaction rejected: ${String(err?.message || err).slice(0, 500)}` });
  }
  return true;
}

async function handleMawRetryDigestionReceiptButton(interaction) {
  if (!isAdmin(interaction)) {
    console.warn(`[Maw] Unauthorized digestion receipt retry by ${interaction.user?.id || 'unknown'}.`);
    await interaction.reply({ content: 'Admin only.', flags: EPHEMERAL });
    return true;
  }
  const sessionId = String(interaction.customId || '').split(':')[1] || '';
  await interaction.deferReply({ ephemeral: true });
  const session = await getMawSessionWithPool(sessionId);
  if (session && String(session.guild_id) !== String(interaction.guildId)) {
    await interaction.editReply({ content: 'That digestion request is not available here.' });
    return true;
  }
  console.info(`[Maw] Final digestion receipt retry requested for session ${sessionId}.`);
  const receipt = await postFinalDigestionReceiptForSession(sessionId).catch(async (err) => {
    await markDigestionReceiptFailed(sessionId, err);
    return { ok: false, reason: String(err?.message || err) };
  });
  if (!receipt?.ok) {
    await interaction.editReply({ content: `Retry failed: ${receipt?.reason || 'unknown error'}` });
    return true;
  }
  await interaction.editReply({ content: 'Final digestion receipt posted.' });
  return true;
}

async function getMawSessionWithPool(sessionId, db = null) {
  const pool = db || resolvePool();
  const { rows } = await pool.query(
    `SELECT s.*, p.id AS pool_squig_id, p.disposition AS pool_disposition,
            p.inventory_status AS pool_inventory_status, p.status AS pool_status,
            p.digestion_status AS pool_digestion_status,
            p.admin_digestion_message_id AS pool_admin_digestion_message_id,
            p.burn_transaction_hash AS pool_burn_transaction_hash,
            p.burn_transaction_url AS pool_burn_transaction_url
     FROM maw_return_sessions s
     LEFT JOIN maw_squig_pool p ON p.received_session_id = s.id
     WHERE s.id = $1
     LIMIT 1`,
    [String(sessionId)]
  );
  return rows[0] || null;
}

async function validateMawBurnTransaction({ session, txHash, provider, config }) {
  if (!provider?.getTransactionReceipt) throw new Error('No Ethereum provider is configured for burn validation.');
  const receipt = await provider.getTransactionReceipt(txHash);
  if (!receipt) throw new Error('Burn transaction receipt was not found.');
  if (receipt.status !== 1 && receipt.status !== '0x1' && receipt.status !== true) {
    throw new Error('Burn transaction failed on-chain.');
  }
  const expectedTokenId = BigInt(session.token_id).toString();
  const contract = normalizeAddress(config.squigContract);
  const mawWallet = normalizeAddress(config.mawWalletAddress);
  const burnAddresses = new Set(config.acceptedBurnAddresses.map(normalizeAddress).filter(Boolean));
  const matchingLogs = (receipt.logs || []).filter((log) => {
    const topics = log.topics || [];
    if (normalizeAddress(log.address) !== contract) return false;
    if (String(topics[0] || '').toLowerCase() !== MAW_TRANSFER_TOPIC) return false;
    if (topics.length < 4) return false;
    const from = normalizeAddress(`0x${String(topics[1] || '').slice(-40)}`);
    const tokenId = BigInt(topics[3]).toString();
    return from === mawWallet && tokenId === expectedTokenId;
  });
  if (!matchingLogs.length) {
    throw new Error('Burn transaction does not show the expected Squig leaving the Malformed Maw wallet.');
  }
  const burnLog = matchingLogs.find((log) => {
    const to = normalizeAddress(`0x${String((log.topics || [])[2] || '').slice(-40)}`);
    return burnAddresses.has(to);
  });
  if (!burnLog) {
    throw new Error('Burn transaction did not send the Squig to an accepted irrecoverable burn address.');
  }
  const burnTo = normalizeAddress(`0x${String((burnLog.topics || [])[2] || '').slice(-40)}`);

  let ownerAfter = null;
  let ownerLookupReverted = false;
  if (provider) {
    try {
      const erc721 = new ethers.Contract(contract, ERC721_OWNER_ABI, provider);
      ownerAfter = normalizeAddress(await erc721.ownerOf(expectedTokenId));
    } catch {
      ownerLookupReverted = true;
    }
  }
  if (ownerAfter === mawWallet) {
    throw new Error('Token is still owned by the Malformed Maw wallet after the submitted transaction.');
  }
  if (burnTo !== ZERO_ADDRESS && ownerAfter && ownerAfter !== burnTo) {
    throw new Error('Token is not currently owned by the accepted burn address from the submitted transaction.');
  }

  return {
    ok: true,
    burnMethod: burnTo === ZERO_ADDRESS ? 'erc721_transfer_to_zero_from_burn' : 'transfer_to_accepted_burn_address',
    burnTo,
    ownerAfter,
    ownerLookupReverted,
    validationLevel: ownerLookupReverted || burnTo === ZERO_ADDRESS ? 'erc721_transfer_event_and_owner_absence' : 'erc721_transfer_event_and_owner_at_burn_address',
  };
}

async function completeMawDigestionBurn({ sessionId, burnTransactionHash, burnTransactionUrl, confirmedBy, validation }) {
  const pool = resolvePool();
  const db = await pool.connect();
  let done = false;
  try {
    await db.query('BEGIN');
    const sessionRows = await db.query(`SELECT * FROM maw_return_sessions WHERE id = $1 FOR UPDATE`, [String(sessionId)]);
    const session = sessionRows.rows[0];
    if (!session || getSessionDisposition(session) !== MAW_DISPOSITIONS.SWALLOWED) {
      throw new Error('This Squig is not a Swallowed Maw session.');
    }
    if (String(session.digestion_status || '') !== 'pending_burn') {
      throw new Error('This Squig’s digestion has already been completed or is currently being processed.');
    }
    if (session.burn_transaction_hash) {
      throw new Error('This Squig already has a verified burn transaction.');
    }
    const poolRows = await db.query(`SELECT * FROM maw_squig_pool WHERE received_session_id = $1 FOR UPDATE`, [String(session.id)]);
    const poolSquig = poolRows.rows[0];
    if (!poolSquig || normalizeDisposition(poolSquig.disposition) !== MAW_DISPOSITIONS.SWALLOWED) {
      throw new Error('Swallowed inventory record is missing or invalid.');
    }
    const duplicate = await db.query(
      `SELECT id FROM maw_return_sessions WHERE burn_transaction_hash = $1 AND id <> $2 LIMIT 1`,
      [burnTransactionHash, String(session.id)]
    );
    if (duplicate.rows[0]) {
      console.warn(`[Maw] Duplicate burn hash rejected: ${burnTransactionHash}`);
      throw new Error('That burn transaction hash has already been used.');
    }
    await db.query(
      `UPDATE maw_return_sessions
       SET burn_transaction_hash = $2,
           burn_transaction_url = $3,
           burn_confirmed_by = $4,
           burn_confirmed_at = NOW(),
           digestion_status = 'burn_verified',
           digestion_receipt_error = NULL,
           updated_at = NOW()
       WHERE id = $1`,
      [String(session.id), burnTransactionHash, burnTransactionUrl, String(confirmedBy)]
    );
    await db.query(
      `UPDATE maw_squig_pool
       SET burn_transaction_hash = $2,
           burn_transaction_url = $3,
           burn_confirmed_by = $4,
           burn_confirmed_at = NOW(),
           inventory_status = 'digested',
           status = 'digested',
           digestion_status = 'burn_verified',
           updated_at = NOW()
       WHERE id = $1`,
      [String(poolSquig.id), burnTransactionHash, burnTransactionUrl, String(confirmedBy)]
    );
    await db.query('COMMIT');
    done = true;
    console.info(`[Maw] Burn transaction verified for Squig ${session.token_id}: ${burnTransactionHash} (${validation?.validationLevel || 'validated'}).`);
    await postAdminLogByGuildId(session.guild_id, 'Maw Burn Verified', `${createDigestionLogPrefix(session)} burn verified: ${burnTransactionUrl}.`).catch(() => null);
    return { ok: true, session: { ...session, burn_transaction_hash: burnTransactionHash, burn_transaction_url: burnTransactionUrl }, poolSquig };
  } catch (err) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    db.release();
  }
}

async function postFinalDigestionReceiptForSession(sessionId) {
  const pool = resolvePool();
  const config = getMawConfig();
  const channel = await deps.client?.channels?.fetch?.(config.digestionReceiptChannelId).catch(() => null);
  if (!channel?.isTextBased?.()) {
    throw new Error(`Maw digestion receipt channel ${config.digestionReceiptChannelId} is unavailable.`);
  }
  const db = await pool.connect();
  let session;
  let message;
  let done = false;
  try {
    await db.query('BEGIN');
    await db.query('SELECT pg_advisory_xact_lock($1::bigint)', [advisoryLockKey(`maw-digestion-receipt:${sessionId}`)]);
    const rows = await db.query(
      `SELECT s.*, p.id AS pool_squig_id
       FROM maw_return_sessions s
       LEFT JOIN maw_squig_pool p ON p.received_session_id = s.id
       WHERE s.id = $1
       FOR UPDATE OF s`,
      [String(sessionId)]
    );
    session = rows.rows[0];
    if (!session || getSessionDisposition(session) !== MAW_DISPOSITIONS.SWALLOWED) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'Swallowed session not found.' };
    }
    if (String(session.digestion_status || '') === 'digested' && session.digestion_receipt_message_id) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: true, alreadyPosted: true, messageId: session.digestion_receipt_message_id };
    }
    if (!['burn_verified', 'receipt_failed'].includes(String(session.digestion_status || ''))) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'Burn has not been verified or receipt already posted.' };
    }
    if (!session.burn_transaction_url || !session.burn_transaction_hash) {
      await db.query('ROLLBACK');
      done = true;
      return { ok: false, reason: 'Burn transaction is missing.' };
    }

    const image = mawDigestedImageAttachment(config);
    const embed = new EmbedBuilder()
      .setTitle(`SQUIG ${formatToken(session.token_id)} HAS BEEN DIGESTED`)
      .setColor(0x2f9e44)
      .setDescription(
        `<@${session.discord_user_id}>, the Maw has finished digesting your ${getSessionRarityLabel(session)} Squig.\n\n` +
        `Squig ${formatToken(session.token_id)} has now been permanently burned and removed from circulation.`
      )
      .addFields(
        { name: 'Original feeder', value: `<@${session.discord_user_id}>`, inline: true },
        { name: 'Rarity', value: getSessionRarityLabel(session), inline: true },
        { name: 'Maw Rank', value: session.average_rank == null ? 'Legacy flat event' : formatMawAverageRank(session.average_rank), inline: true },
        { name: '$CHARM received', value: formatCharm(session.payout_amount), inline: true },
        { name: 'Maw Tickets earned', value: String(Math.max(1, Math.floor(Number(session.ticket_count) || 1))), inline: true },
        { name: 'Jackpot contribution', value: `${formatSignedCharm(session.jackpot_contribution_charm)} $CHARM`, inline: true },
        { name: 'Burn receipt', value: session.burn_transaction_url, inline: false },
        { name: 'The Maw keeps what it swallows.', value: '\u200B', inline: false }
      );
    if (image.imageUrl) embed.setImage(image.imageUrl);
    message = await channel.send({
      content: `<@${session.discord_user_id}>`,
      embeds: [embed],
      allowedMentions: { users: [String(session.discord_user_id)], roles: [], parse: [] },
      ...(image.files.length ? { files: image.files } : {}),
    });
    await db.query(
      `UPDATE maw_return_sessions
       SET digestion_status = 'digested',
           digestion_receipt_message_id = $2,
           digestion_receipt_posted_at = NOW(),
           digestion_receipt_error = NULL,
           updated_at = NOW()
       WHERE id = $1`,
      [String(session.id), String(message.id)]
    );
    await db.query(
      `UPDATE maw_squig_pool
       SET digestion_status = 'digested',
           inventory_status = 'digested',
           status = 'digested',
           updated_at = NOW()
       WHERE received_session_id = $1`,
      [String(session.id)]
    );
    await db.query('COMMIT');
    done = true;
  } catch (err) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    db.release();
  }
  await disableDigestionAdminButton(session.id).catch(() => null);
  console.info(`[Maw] Final digestion receipt posted for Squig ${session.token_id}: ${message.id}`);
  await postAdminLogByGuildId(session.guild_id, 'Maw Digestion Complete', `${createDigestionLogPrefix(session)} final receipt posted: ${message.id}.`).catch(() => null);
  return { ok: true, messageId: message.id };
}

function mawDigestedImageAttachment(config = getMawConfig()) {
  const localPath = config.digestedImagePath ? path.resolve(config.digestedImagePath) : null;
  if (localPath && fs.existsSync(localPath)) {
    const name = `maw-digested${path.extname(localPath) || '.png'}`;
    return { imageUrl: `attachment://${name}`, files: [new AttachmentBuilder(localPath, { name })] };
  }
  if (localPath) console.warn(`[Maw] configured digestion image path not found: ${localPath}`);
  if (config.digestedImageUrl) return { imageUrl: config.digestedImageUrl, files: [] };
  console.warn('[Maw] no digestion image configured; posting final receipt without image.');
  return { imageUrl: null, files: [] };
}

async function markDigestionReceiptFailed(sessionId, err) {
  const message = String(err?.message || err || 'unknown error').slice(0, 500);
  await resolvePool().query(
    `UPDATE maw_return_sessions
     SET digestion_status = 'receipt_failed',
         digestion_receipt_error = $2,
         updated_at = NOW()
     WHERE id = $1`,
    [String(sessionId), message]
  ).catch(() => null);
  await resolvePool().query(
    `UPDATE maw_squig_pool
     SET digestion_status = 'receipt_failed',
         inventory_status = 'digested',
         status = 'digested',
         updated_at = NOW()
     WHERE received_session_id = $1`,
    [String(sessionId)]
  ).catch(() => null);
  const session = await getMawSessionWithPool(sessionId).catch(() => null);
  if (session?.admin_digestion_message_id) await updateDigestionAdminButtonForRetry(session).catch(() => null);
  console.warn(`[Maw] final digestion receipt failed for session ${sessionId}: ${message}`);
  await postAdminLogByGuildId(session?.guild_id || null, 'Maw Digestion Receipt Failed', `Session ${sessionId}: ${message}`).catch(() => null);
}

async function disableDigestionAdminButton(sessionId) {
  const session = await getMawSessionWithPool(sessionId);
  if (!session?.admin_digestion_message_id || !deps?.client) return false;
  const config = getMawConfig();
  const channel = await deps.client.channels.fetch(config.digestionAdminChannelId).catch(() => null);
  const message = await channel?.messages?.fetch?.(session.admin_digestion_message_id).catch(() => null);
  if (!message?.edit) return false;
  await message.edit({ components: [buildDigestionActionRow(session.id, true)] });
  return true;
}

async function updateDigestionAdminButtonForRetry(session) {
  if (!session?.admin_digestion_message_id || !deps?.client) return false;
  const config = getMawConfig();
  const channel = await deps.client.channels.fetch(config.digestionAdminChannelId).catch(() => null);
  const message = await channel?.messages?.fetch?.(session.admin_digestion_message_id).catch(() => null);
  if (!message?.edit) return false;
  await message.edit({ components: [buildDigestionActionRow(session.id, false, true)] });
  return true;
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
      `SELECT t.*, s.rarity_tier, s.average_rank, s.ticket_count AS session_ticket_count
       FROM maw_tickets t
       LEFT JOIN maw_return_sessions s ON s.id = t.return_session_id
       WHERE t.event_id = $1
       ORDER BY random()
       LIMIT 1`,
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
        .setDescription(
          `${draw.event.goal_count} / ${draw.event.goal_count} Squigs consumed.\n` +
          `Total tickets in the draw: ${draw.event.total_ticket_count || 'unknown'}\n` +
          `Starting jackpot: ${formatCharm(draw.event.jackpot_base_charm)} $CHARM\n` +
          `Rarity contributions: ${formatCharm(draw.event.jackpot_contributed_charm)} $CHARM\n` +
          `Final jackpot: ${formatCharm(draw.event.jackpot_charm)} $CHARM`
        ),
    ],
  }).catch(() => null);
  const paid = await payPendingMawJackpot(draw.event.id);
  if (paid?.ok) {
    await postMawFeed(paid.event, {
      content:
        `Maw Ticket #${paid.ticket.ticket_number} was drawn\n` +
        `Winner: <@${paid.ticket.discord_user_id}>\n` +
        `Winning Squig: ${formatToken(paid.ticket.token_id)}\n` +
        `Rarity: ${getSessionRarityLabel(paid.ticket)}\n` +
        `Squigs consumed: ${paid.event.received_count} / ${paid.event.goal_count}\n` +
        `Total tickets in draw: ${paid.event.total_ticket_count || 'unknown'}\n` +
        `Starting jackpot: ${formatCharm(paid.event.jackpot_base_charm)} $CHARM\n` +
        `Rarity contributions: ${formatCharm(paid.event.jackpot_contributed_charm)} $CHARM\n` +
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
    const ticketRows = await db.query(
      `SELECT t.*, s.rarity_tier, s.average_rank, s.ticket_count AS session_ticket_count
       FROM maw_tickets t
       LEFT JOIN maw_return_sessions s ON s.id = t.return_session_id
       WHERE t.id = $1
       LIMIT 1`,
      [String(event.draw_winning_ticket_id)]
    );
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
      const receiptResult = await postMawReceiptMessages(result).catch(async (err) => {
        await postAdminLogByGuildId(result.event?.guild_id || null, 'Maw Receipt Failure', String(err?.message || err).slice(0, 800));
        return null;
      });
      if (!receiptResult?.panelMoved) await updateMawPanel(result.event.id).catch(() => null);
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
          `Winning Squig: ${formatToken(result.ticket.token_id)}\n` +
          `Rarity: ${getSessionRarityLabel(result.ticket)}\n` +
          `Squigs consumed: ${result.event.received_count} / ${result.event.goal_count}\n` +
          `Total tickets in draw: ${result.event.total_ticket_count || 'unknown'}\n` +
          `Starting jackpot: ${formatCharm(result.event.jackpot_base_charm)} $CHARM\n` +
          `Rarity contributions: ${formatCharm(result.event.jackpot_contributed_charm)} $CHARM\n` +
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
               inventory_status = 'available',
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
    `Unmatched Squig transfer into the Malformed Maw wallet.\n` +
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
  if (!channel?.isTextBased?.()) return null;
  return channel.send(payload);
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
  handleModalSubmit,
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
  validateMawBurnTransaction,
};
