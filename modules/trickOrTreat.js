const crypto = require('crypto');
const fetch = require('node-fetch');
const { ethers } = require('ethers');
const {
  SlashCommandBuilder,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  AttachmentBuilder,
  PermissionFlagsBits,
} = require('discord.js');

const EPHEMERAL = 64;
const SQUIGS_CONTRACT_DEFAULT = '0x8c9a02c0585200c4c65608df6b8def543d33792a';
const EVENT_KEY = 'squigs-trick-or-treat-2026';
const CLAIM_BUTTON = 'tot_claim_treat';
const TRICK_BUTTON = 'tot_submit_trick';
const BAG_BUTTON = 'tot_view_bag';
const TRICK_MODAL = 'tot_trick_modal';
const TRICK_URL_FIELD = 'tot_trick_url';

let deps = null;
let schemaPromise = null;
const listingCache = new Map();
let nextOpenSeaRequestAt = 0;
const OPENSEA_MIN_INTERVAL_MS = Math.max(250, Number(process.env.TRICK_OR_TREAT_OPENSEA_INTERVAL_MS || 1200));
const OPENSEA_LISTING_CACHE_MS = Math.max(0, Number(process.env.TRICK_OR_TREAT_LISTING_CACHE_MS || 45000));
const OPENSEA_MAX_RETRIES = Math.max(1, Math.min(8, Number(process.env.TRICK_OR_TREAT_OPENSEA_RETRIES || 5)));

function initTrickOrTreat(injected = {}) {
  deps = injected || {};
}

function resolvePool() {
  const pool = deps?.trickOrTreatPool || deps?.prizesPool || deps?.pool;
  if (!pool?.query) throw new Error('Trick or Treat database pool is not configured.');
  return pool;
}

function boolEnv(name, fallback = false) {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(raw).trim().toLowerCase());
}

function getConfig(env = process.env) {
  const contract = normalizeAddress(env.TRICK_OR_TREAT_SQUIGS_CONTRACT || deps?.squigsContract || SQUIGS_CONTRACT_DEFAULT);
  return {
    enabled: boolEnv('TRICK_OR_TREAT_ENABLED', false),
    eventKey: String(env.TRICK_OR_TREAT_EVENT_KEY || EVENT_KEY).trim(),
    timeZone: String(env.TRICK_OR_TREAT_TIME_ZONE || 'America/Toronto').trim(),
    startAt: new Date(env.TRICK_OR_TREAT_START_AT || '2026-10-01T00:00:00-04:00'),
    endAt: new Date(env.TRICK_OR_TREAT_END_AT || '2026-10-31T23:59:59-04:00'),
    contract,
    chain: String(env.TRICK_OR_TREAT_CHAIN || deps?.squigsChain || 'ethereum').trim().toLowerCase(),
    openSeaApiKey: String(env.OPENSEA_API_KEY || '').trim(),
    minConfirmations: Math.max(1, Math.floor(Number(env.TRICK_OR_TREAT_MIN_CONFIRMATIONS || 2))),
    adminChannelId: String(env.TRICK_OR_TREAT_ADMIN_CHANNEL_ID || '').trim(),
    publicActivityChannelId: String(env.TRICK_OR_TREAT_ACTIVITY_CHANNEL_ID || '').trim(),
    requireCurrentOwnership: !['0', 'false', 'no', 'off'].includes(String(env.TRICK_OR_TREAT_REQUIRE_CURRENT_OWNERSHIP || 'true').toLowerCase()),
  };
}

function normalizeAddress(value) {
  const text = String(value || '').trim();
  return /^0x[0-9a-fA-F]{40}$/.test(text) ? text.toLowerCase() : null;
}

function eventIsActive(now = new Date(), cfg = getConfig()) {
  return cfg.enabled && Number.isFinite(cfg.startAt.getTime()) && Number.isFinite(cfg.endAt.getTime()) &&
    now >= cfg.startAt && now <= cfg.endAt;
}

function eventDayKey(date = new Date(), timeZone = getConfig().timeZone) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(date);
  const year = parts.find((p) => p.type === 'year')?.value;
  const month = parts.find((p) => p.type === 'month')?.value;
  const day = parts.find((p) => p.type === 'day')?.value;
  return `${year}-${month}-${day}`;
}

function isEventDay(dayKey, cfg = getConfig()) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(dayKey || ''))) return false;
  return dayKey >= eventDayKey(cfg.startAt, cfg.timeZone) && dayKey <= eventDayKey(cfg.endAt, cfg.timeZone);
}

function buildSlashCommand() {
  return new SlashCommandBuilder()
    .setName('trickortreat')
    .setDescription('Squigs Trick or Treat event')
    .addSubcommand((sub) => sub.setName('post').setDescription('Admin: post the public Trick or Treat panel'))
    .addSubcommand((sub) => sub.setName('bag').setDescription('View your Trick or Treat bag'))
    .addSubcommand((sub) => sub
      .setName('status')
      .setDescription('Admin: view event totals or a user')
      .addUserOption((opt) => opt.setName('user').setDescription('Optional user to inspect').setRequired(false)))
    .addSubcommand((sub) => sub.setName('export').setDescription('Admin: export Trick or Treat entries as CSV'))
    .addSubcommand((sub) => sub
      .setName('draw')
      .setDescription('Admin: draw unique weighted winners from final entries')
      .addIntegerOption((opt) => opt.setName('winners').setDescription('Number of unique winners').setRequired(true).setMinValue(1).setMaxValue(100))
      .addBooleanOption((opt) => opt.setName('force').setDescription('Allow draw before event end (testing/admin only)').setRequired(false)));
}

async function ensureTables() {
  if (schemaPromise) return schemaPromise;
  schemaPromise = (async () => {
    const pool = resolvePool();
    await pool.query(`
      CREATE TABLE IF NOT EXISTS trick_or_treat_wallet_history (
        id BIGSERIAL PRIMARY KEY,
        event_key TEXT NOT NULL,
        guild_id TEXT NOT NULL,
        discord_id TEXT NOT NULL,
        wallet_address TEXT NOT NULL,
        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        detached_at TIMESTAMPTZ,
        UNIQUE(event_key, guild_id, discord_id, wallet_address)
      );
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS trick_or_treat_wallet_history_user_idx
      ON trick_or_treat_wallet_history(event_key, guild_id, discord_id);
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS trick_or_treat_wallet_history_wallet_idx
      ON trick_or_treat_wallet_history(event_key, guild_id, wallet_address);
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS trick_or_treat_treats (
        id BIGSERIAL PRIMARY KEY,
        event_key TEXT NOT NULL,
        guild_id TEXT NOT NULL,
        discord_id TEXT NOT NULL,
        event_day TEXT NOT NULL,
        wallet_count INTEGER NOT NULL,
        squig_count INTEGER NOT NULL,
        claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(event_key, guild_id, discord_id, event_day)
      );
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS trick_or_treat_tricks (
        id BIGSERIAL PRIMARY KEY,
        event_key TEXT NOT NULL,
        guild_id TEXT NOT NULL,
        discord_id TEXT NOT NULL,
        chain TEXT NOT NULL,
        contract_address TEXT NOT NULL,
        token_id TEXT NOT NULL,
        transaction_hash TEXT NOT NULL,
        order_hash TEXT,
        seller_wallet TEXT,
        buyer_wallet TEXT NOT NULL,
        event_timestamp TIMESTAMPTZ NOT NULL,
        block_number BIGINT,
        verified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        source TEXT NOT NULL DEFAULT 'opensea-v2',
        UNIQUE(event_key, chain, contract_address, token_id, transaction_hash)
      );
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS trick_or_treat_tricks_user_idx
      ON trick_or_treat_tricks(event_key, guild_id, discord_id);
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS trick_or_treat_draws (
        id BIGSERIAL PRIMARY KEY,
        event_key TEXT NOT NULL,
        guild_id TEXT NOT NULL,
        requested_winners INTEGER NOT NULL,
        seed_hex TEXT NOT NULL,
        entry_snapshot JSONB NOT NULL,
        created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS trick_or_treat_draw_winners (
        draw_id BIGINT NOT NULL REFERENCES trick_or_treat_draws(id) ON DELETE CASCADE,
        position INTEGER NOT NULL,
        discord_id TEXT NOT NULL,
        entries_at_draw INTEGER NOT NULL,
        PRIMARY KEY(draw_id, position),
        UNIQUE(draw_id, discord_id)
      );
    `);
  })().catch((err) => {
    schemaPromise = null;
    throw err;
  });
  return schemaPromise;
}

async function recordWalletLinkEvent(guildId, discordId, walletAddress, attached = true) {
  const cfg = getConfig();
  if (!cfg.enabled) return;
  const wallet = normalizeAddress(walletAddress);
  if (!wallet || !guildId || !discordId) return;
  await ensureTables();
  const pool = resolvePool();
  if (attached) {
    await pool.query(
      `INSERT INTO trick_or_treat_wallet_history(event_key,guild_id,discord_id,wallet_address,detached_at)
       VALUES($1,$2,$3,$4,NULL)
       ON CONFLICT(event_key,guild_id,discord_id,wallet_address)
       DO UPDATE SET last_seen_at=NOW(),detached_at=NULL`,
      [cfg.eventKey, String(guildId), String(discordId), wallet]
    );
  } else {
    await pool.query(
      `INSERT INTO trick_or_treat_wallet_history(event_key,guild_id,discord_id,wallet_address,detached_at)
       VALUES($1,$2,$3,$4,NOW())
       ON CONFLICT(event_key,guild_id,discord_id,wallet_address)
       DO UPDATE SET last_seen_at=NOW(),detached_at=NOW()`,
      [cfg.eventKey, String(guildId), String(discordId), wallet]
    );
  }
}

async function syncCurrentWallets(guildId, discordId) {
  await ensureTables();
  const links = await deps.getWalletLinks(guildId, discordId);
  const verified = links
    .filter((row) => row?.verified !== false)
    .map((row) => normalizeAddress(row?.wallet_address))
    .filter(Boolean);
  for (const wallet of verified) await recordWalletLinkEvent(guildId, discordId, wallet, true);
  return [...new Set(verified)];
}

async function eventWallets(guildId, discordId) {
  const cfg = getConfig();
  const current = await syncCurrentWallets(guildId, discordId);
  const pool = resolvePool();
  const { rows } = await pool.query(
    `SELECT wallet_address FROM trick_or_treat_wallet_history
     WHERE event_key=$1 AND guild_id=$2 AND discord_id=$3`,
    [cfg.eventKey, String(guildId), String(discordId)]
  );
  return [...new Set([...current, ...rows.map((r) => normalizeAddress(r.wallet_address)).filter(Boolean)])];
}

async function findWalletIdentityConflicts(guildId, discordId, wallets) {
  const cfg = getConfig();
  if (!wallets.length) return [];
  const pool = resolvePool();
  const { rows } = await pool.query(
    `SELECT wallet_address, array_agg(DISTINCT discord_id) AS discord_ids
     FROM trick_or_treat_wallet_history
     WHERE event_key=$1 AND guild_id=$2 AND wallet_address = ANY($3::text[])
     GROUP BY wallet_address
     HAVING COUNT(DISTINCT discord_id) > 1`,
    [cfg.eventKey, String(guildId), wallets]
  );
  return rows.filter((row) => (row.discord_ids || []).some((id) => String(id) !== String(discordId)));
}

function openSeaHeaders(cfg) {
  if (!cfg.openSeaApiKey) throw new Error('OPENSEA_API_KEY is required for Trick or Treat verification.');
  return { accept: 'application/json', 'x-api-key': cfg.openSeaApiKey };
}

function retryAfterMs(headerValue) {
  if (!headerValue) return 0;
  const numeric = Number(headerValue);
  if (Number.isFinite(numeric)) return Math.max(0, numeric * 1000);
  const date = Date.parse(String(headerValue));
  return Number.isFinite(date) ? Math.max(0, date - Date.now()) : 0;
}

async function waitForOpenSeaSlot() {
  const now = Date.now();
  const waitMs = Math.max(0, nextOpenSeaRequestAt - now);
  nextOpenSeaRequestAt = Math.max(now, nextOpenSeaRequestAt) + OPENSEA_MIN_INTERVAL_MS;
  if (waitMs) await new Promise((resolve) => setTimeout(resolve, waitMs));
}

async function fetchJson(url, cfg, timeoutMs = 12000) {
  let lastError = null;
  for (let attempt = 1; attempt <= OPENSEA_MAX_RETRIES; attempt++) {
    await waitForOpenSeaSlot();
    const res = await fetch(url, { headers: openSeaHeaders(cfg), timeout: timeoutMs });
    if (res.ok) return res.json();

    const body = await res.text().catch(() => '');
    const err = new Error(`OpenSea API returned ${res.status}: ${body.slice(0, 180)}`);
    err.status = res.status;
    lastError = err;

    if (res.status !== 429 && res.status < 500) throw err;
    if (attempt >= OPENSEA_MAX_RETRIES) break;

    const serverDelay = retryAfterMs(res.headers?.get?.('retry-after'));
    const exponential = Math.min(30000, 1500 * (2 ** (attempt - 1)));
    const delay = Math.max(serverDelay, exponential);
    nextOpenSeaRequestAt = Math.max(nextOpenSeaRequestAt, Date.now() + delay);
  }
  throw lastError || new Error('OpenSea API request failed.');
}

async function getActiveSquigListings(wallet, cfg = getConfig()) {
  const cacheKey = `${cfg.chain}:${cfg.contract}:${wallet}`;
  const cached = listingCache.get(cacheKey);
  if (cached && Date.now() - cached.at <= OPENSEA_LISTING_CACHE_MS) {
    return cached.listings.map((x) => ({ ...x }));
  }

  let next = null;
  const matches = [];
  let pages = 0;
  do {
    const params = new URLSearchParams({ limit: '100', sort_by: 'START_TIME', sort_direction: 'desc' });
    if (next) params.set('after', next);
    const url = `https://api.opensea.io/api/v2/account/${wallet}/listings?${params.toString()}`;
    const payload = await fetchJson(url, cfg);
    for (const listing of payload?.listings || []) {
      const contract = normalizeAddress(listing?.asset?.contract);
      if (contract === cfg.contract) {
        matches.push({
          tokenId: String(listing?.asset?.identifier || ''),
          orderHash: String(listing?.order_hash || listing?.id || ''),
          maker: normalizeAddress(listing?.maker) || wallet,
        });
      }
    }
    next = payload?.next || null;
    pages++;
  } while (next && pages < 10);
  if (next) throw new Error('OpenSea listing pagination exceeded the safety limit; eligibility was not determined.');
  listingCache.set(cacheKey, { at: Date.now(), listings: matches.map((x) => ({ ...x })) });
  return matches;
}

async function getAllListingsForWallets(wallets, cfg = getConfig()) {
  const out = [];
  for (const wallet of wallets) {
    const listings = await getActiveSquigListings(wallet, cfg);
    for (const listing of listings) out.push({ wallet, ...listing });
  }
  return out;
}

function parseOpenSeaSquigUrl(value, cfg = getConfig()) {
  let url;
  try { url = new URL(String(value || '').trim()); } catch (_) { return null; }
  if (!/(^|\.)opensea\.io$/i.test(url.hostname)) return null;
  const parts = url.pathname.split('/').filter(Boolean);
  let chain = 'ethereum';
  let contract;
  let tokenId;
  if (parts[0] === 'item' && parts.length >= 4) {
    chain = parts[1];
    contract = parts[2];
    tokenId = parts[3];
  } else if (parts[0] === 'assets' && parts.length >= 4) {
    chain = parts[1];
    contract = parts[2];
    tokenId = parts[3];
  } else {
    return null;
  }
  const normalizedContract = normalizeAddress(contract);
  if (normalizedContract !== cfg.contract || !/^\d+$/.test(String(tokenId || ''))) return null;
  return { chain: String(chain).toLowerCase(), contract: normalizedContract, tokenId: String(BigInt(tokenId)) };
}

async function getSaleEvents(tokenId, cfg = getConfig()) {
  const after = Math.floor(cfg.startAt.getTime() / 1000);
  const before = Math.floor(cfg.endAt.getTime() / 1000);
  let next = null;
  const events = [];
  let pages = 0;
  do {
    const params = new URLSearchParams({
      event_type: 'sale',
      after: String(after),
      before: String(before),
      limit: '200',
    });
    if (next) params.set('next', next);
    const url = `https://api.opensea.io/api/v2/events/chain/${encodeURIComponent(cfg.chain)}/contract/${cfg.contract}/nfts/${encodeURIComponent(tokenId)}?${params.toString()}`;
    const payload = await fetchJson(url, cfg);
    events.push(...(payload?.asset_events || []));
    next = payload?.next || null;
    pages++;
  } while (next && pages < 5);
  if (next) throw new Error('OpenSea sale history pagination exceeded the safety limit.');
  return events;
}

function transactionHashFromEvent(event) {
  const tx = event?.transaction;
  if (typeof tx === 'string' && /^0x[0-9a-fA-F]{64}$/.test(tx)) return tx.toLowerCase();
  const candidates = [tx?.hash, tx?.transaction_hash, event?.transaction_hash];
  for (const candidate of candidates) {
    if (/^0x[0-9a-fA-F]{64}$/.test(String(candidate || ''))) return String(candidate).toLowerCase();
  }
  return null;
}

function eventBuyer(event) {
  return normalizeAddress(event?.taker || event?.buyer || event?.to_address);
}

function eventSeller(event) {
  return normalizeAddress(event?.maker || event?.seller || event?.from_address);
}

function rpcUrl(cfg = getConfig()) {
  const dedicated = String(process.env.TRICK_OR_TREAT_ETH_RPC_URL || '').trim();
  if (dedicated) return dedicated;

  const bounty = String(
    process.env.BOUNTY_ETHEREUM_RPC_URL ||
    process.env.BOUNTY_ETH_RPC_URL ||
    ''
  ).trim();
  if (bounty) return bounty;

  const alchemyRpc = String(process.env.ALCHEMY_RPC_URL || '').trim();
  if (alchemyRpc) return alchemyRpc;

  const alchemyKey = String(process.env.ALCHEMY_API_KEY || '').trim();
  if (alchemyKey) return `https://eth-mainnet.g.alchemy.com/v2/${alchemyKey}`;

  // Keep the generic ETH_RPC_URL only as a last-resort fallback. Some deployments
  // use it for legacy providers with exhausted quotas, while Alchemy is healthy.
  return String(process.env.ETH_RPC_URL || '').trim();
}

async function verifySaleOnChain(event, tokenId, buyer, seller, cfg = getConfig()) {
  const txHash = transactionHashFromEvent(event);
  if (!txHash) throw new Error('The sale event did not include a verifiable transaction hash.');
  const url = rpcUrl(cfg);
  if (!url) throw new Error('An Ethereum RPC is required to verify Trick transactions.');
  // Ethereum mainnet is fixed for Squigs. Pin the network so ethers does not
  // repeatedly retry network detection when an upstream RPC returns an error.
  const provider = new ethers.JsonRpcProvider(url, 1, { staticNetwork: true });
  let receipt;
  let head;
  try {
    [receipt, head] = await Promise.all([
      provider.getTransactionReceipt(txHash),
      provider.getBlockNumber(),
    ]);
  } catch (err) {
    const status = err?.info?.responseStatus || err?.status || err?.code || '';
    const message = String(err?.shortMessage || err?.message || err || '');
    const safeMessage = message
      .replace(/https?:\/\/[^\s)"']+/g, '[rpc-url-redacted]')
      .slice(0, 220);
    throw new Error(
      `Ethereum RPC verification failed${status ? ` (${status})` : ''}: ${safeMessage || 'provider unavailable'}`
    );
  }
  if (!receipt || receipt.status !== 1) throw new Error('The sale transaction is missing or unsuccessful.');
  const confirmations = Math.max(0, Number(head) - Number(receipt.blockNumber) + 1);
  if (confirmations < cfg.minConfirmations) throw new Error(`The purchase has only ${confirmations} confirmation(s). Try again shortly.`);

  const transferTopic = ethers.id('Transfer(address,address,uint256)');
  const tokenHex = `0x${BigInt(tokenId).toString(16).padStart(64, '0')}`.toLowerCase();
  let matched = false;
  for (const log of receipt.logs || []) {
    if (normalizeAddress(log.address) !== cfg.contract) continue;
    if (String(log.topics?.[0] || '').toLowerCase() !== transferTopic.toLowerCase()) continue;
    if (String(log.topics?.[3] || '').toLowerCase() !== tokenHex) continue;
    const from = normalizeAddress(`0x${String(log.topics?.[1] || '').slice(-40)}`);
    const to = normalizeAddress(`0x${String(log.topics?.[2] || '').slice(-40)}`);
    if (to === buyer && (!seller || from === seller)) {
      matched = true;
      break;
    }
  }
  if (!matched) throw new Error('The transaction did not contain the expected Squigs Reloaded transfer to your connected wallet.');
  return { txHash, blockNumber: receipt.blockNumber, confirmations };
}

async function ownedTokenIds(wallets) {
  if (!wallets.length) return [];
  const cfg = getConfig();
  if (typeof deps.getOwnedTokenIdsForContractMany === 'function') {
    return deps.getOwnedTokenIdsForContractMany(wallets, cfg.contract, cfg.chain, {
      concurrency: 2,
      suppressErrors: false,
    });
  }
  return deps.getOwnedSquigsReloadedTokenIds(wallets);
}

async function userCounts(guildId, discordId) {
  const cfg = getConfig();
  await ensureTables();
  const pool = resolvePool();
  const [{ rows: treatRows }, { rows: trickRows }] = await Promise.all([
    pool.query(
      `SELECT COUNT(*)::int AS count FROM trick_or_treat_treats WHERE event_key=$1 AND guild_id=$2 AND discord_id=$3`,
      [cfg.eventKey, String(guildId), String(discordId)]
    ),
    pool.query(
      `SELECT COUNT(*)::int AS count FROM trick_or_treat_tricks WHERE event_key=$1 AND guild_id=$2 AND discord_id=$3`,
      [cfg.eventKey, String(guildId), String(discordId)]
    ),
  ]);
  const treats = Number(treatRows[0]?.count || 0);
  const tricks = Number(trickRows[0]?.count || 0);
  return { treats, tricks, entries: treats + tricks };
}

function bagEmbed(user, counts, cfg = getConfig()) {
  return new EmbedBuilder()
    .setTitle('🎃 Your Trick or Treat Bag')
    .setDescription('Every Treat and every verified Trick is one Halloween entry.')
    .addFields(
      { name: '🍬 Treats', value: `${counts.treats} / 31`, inline: true },
      { name: '👻 Tricks', value: String(counts.tricks), inline: true },
      { name: '🎟️ Total Entries', value: String(counts.entries), inline: true },
    )
    .setFooter({ text: eventIsActive(new Date(), cfg) ? 'Stay Ugly. 🎃' : 'Event is currently closed.' });
}

function publicPanel() {
  return {
    embeds: [
      new EmbedBuilder()
        .setTitle('🎃 SQUIGS TRICK OR TREAT')
        .setDescription(
          '**October is getting ugly.**\n\n' +
          '🍬 **TREAT** — Once per day, claim while you hold at least one Squig and have **zero Squigs listed across all connected wallets**.\n\n' +
          '👻 **TRICK** — Bought a Squig on secondary? Submit the OpenSea link and let UglyBot verify the purchase.\n\n' +
          '🎟️ **Every verified Trick or Treat = 1 Halloween entry.**\n\n' +
          'No holder-size multiplier. No Trick cap. Just show up, hold ugly, and hunt.'
        )
        .setImage('https://i.imgur.com/JSXZmzh.png')
        .setFooter({ text: 'Stay Ugly. 🎃' }),
    ],
    components: [
      new ActionRowBuilder().addComponents(
        new ButtonBuilder().setCustomId(CLAIM_BUTTON).setLabel("Claim Today's Treat").setEmoji('🍬').setStyle(ButtonStyle.Success),
        new ButtonBuilder().setCustomId(TRICK_BUTTON).setLabel('Submit a Trick').setEmoji('👻').setStyle(ButtonStyle.Primary),
        new ButtonBuilder().setCustomId(BAG_BUTTON).setLabel('My Bag').setEmoji('🎃').setStyle(ButtonStyle.Secondary),
      ),
    ],
  };
}

async function claimTreat(interaction) {
  const cfg = getConfig();
  if (!eventIsActive(new Date(), cfg)) {
    await interaction.reply({ content: '🎃 Trick or Treat is not active right now.', flags: EPHEMERAL });
    return;
  }
  await interaction.deferReply({ flags: EPHEMERAL });
  await ensureTables();

  const day = eventDayKey(new Date(), cfg.timeZone);
  if (!isEventDay(day, cfg)) {
    await interaction.editReply('🎃 Today is outside the October Trick or Treat event window.');
    return;
  }

  const pool = resolvePool();
  const existing = await pool.query(
    `SELECT id FROM trick_or_treat_treats WHERE event_key=$1 AND guild_id=$2 AND discord_id=$3 AND event_day=$4`,
    [cfg.eventKey, interaction.guildId, interaction.user.id, day]
  );
  if (existing.rowCount) {
    const counts = await userCounts(interaction.guildId, interaction.user.id);
    await interaction.editReply({ content: "🍬 You already got today's Treat. Come back tomorrow.", embeds: [bagEmbed(interaction.user, counts, cfg)] });
    return;
  }

  let wallets;
  try {
    wallets = await eventWallets(interaction.guildId, interaction.user.id);
  } catch (_) {
    await interaction.editReply('🎃 UglyBot could not read your connected wallets right now. Nothing was claimed. Try again shortly.');
    return;
  }
  if (!wallets.length) {
    await interaction.editReply('🎃 **YOUR BAG IS EMPTY**\nConnect and verify a wallet holding at least one Squigs Reloaded NFT, then try again.');
    return;
  }

  const conflicts = await findWalletIdentityConflicts(interaction.guildId, interaction.user.id, wallets);
  if (conflicts.length) {
    await interaction.editReply('⚠️ One of your event wallets has been associated with another Discord account during this event. An admin needs to review it before you can claim.');
    await deps.postAdminSystemLog?.({
      guildId: interaction.guildId,
      category: 'Trick or Treat Identity Conflict',
      message: `Discord <@${interaction.user.id}> has event-wallet identity conflicts: ${conflicts.map((x) => x.wallet_address).join(', ')}`,
    }).catch(() => null);
    return;
  }

  let owned;
  try {
    owned = await ownedTokenIds(wallets);
  } catch (_) {
    await interaction.editReply('🎃 UglyBot could not verify your Squigs ownership right now. Nothing was claimed. Try again shortly.');
    return;
  }
  if (!owned.length) {
    await interaction.editReply('🎃 **YOUR BAG IS EMPTY**\nYou need at least one Squigs Reloaded NFT across your event wallets to claim today\'s Treat.');
    return;
  }

  let listings;
  try {
    listings = await getAllListingsForWallets(wallets, cfg);
  } catch (err) {
    console.warn('[TrickOrTreat] listing verification failed:', err.message);
    await interaction.editReply('🎃 UglyBot could not verify your active listings right now. Nothing was claimed. Try again shortly.');
    return;
  }
  if (listings.length) {
    const ids = [...new Set(listings.map((x) => x.tokenId).filter(Boolean))].slice(0, 15);
    await interaction.editReply(
      '👻 **NO TREAT FOR YOU**\nYou have a Squig wandering the marketplace. Delist every Squig tied to your event wallets and try again.' +
      (ids.length ? `\n\nDetected listed Squig(s): ${ids.map((id) => `#${id}`).join(', ')}` : '')
    );
    return;
  }

  try {
    await pool.query(
      `INSERT INTO trick_or_treat_treats(event_key,guild_id,discord_id,event_day,wallet_count,squig_count)
       VALUES($1,$2,$3,$4,$5,$6)`,
      [cfg.eventKey, interaction.guildId, interaction.user.id, day, wallets.length, owned.length]
    );
  } catch (err) {
    if (String(err?.code) === '23505') {
      await interaction.editReply("🍬 You already got today's Treat. Come back tomorrow.");
      return;
    }
    throw err;
  }
  const counts = await userCounts(interaction.guildId, interaction.user.id);
  await interaction.editReply({ content: '🍬 **TREAT CLAIMED!**\nAll your Squigs are home and accounted for.', embeds: [bagEmbed(interaction.user, counts, cfg)] });
}

async function showTrickModal(interaction) {
  const modal = new ModalBuilder().setCustomId(TRICK_MODAL).setTitle('Submit a Trick 👻');
  modal.addComponents(
    new ActionRowBuilder().addComponents(
      new TextInputBuilder()
        .setCustomId(TRICK_URL_FIELD)
        .setLabel('OpenSea link to your new Squig')
        .setPlaceholder('https://opensea.io/item/ethereum/0x.../3157')
        .setRequired(true)
        .setStyle(TextInputStyle.Short)
    )
  );
  await interaction.showModal(modal);
}

async function submitTrick(interaction) {
  const cfg = getConfig();
  if (!eventIsActive(new Date(), cfg)) {
    await interaction.reply({ content: '🎃 Trick or Treat is not active right now.', flags: EPHEMERAL });
    return;
  }
  const input = interaction.fields.getTextInputValue(TRICK_URL_FIELD);
  const parsed = parseOpenSeaSquigUrl(input, cfg);
  if (!parsed || parsed.chain !== cfg.chain) {
    await interaction.reply({ content: '👻 That does not look like a valid Squigs Reloaded OpenSea item link for this event.', flags: EPHEMERAL });
    return;
  }
  await interaction.deferReply({ flags: EPHEMERAL });
  await ensureTables();

  const wallets = await eventWallets(interaction.guildId, interaction.user.id);
  if (!wallets.length) {
    await interaction.editReply('Connect and verify the wallet that bought this Squig first.');
    return;
  }

  const conflicts = await findWalletIdentityConflicts(interaction.guildId, interaction.user.id, wallets);
  if (conflicts.length) {
    await interaction.editReply('⚠️ Your event wallet history needs admin review before a Trick can be verified.');
    return;
  }

  let events;
  try {
    events = await getSaleEvents(parsed.tokenId, cfg);
  } catch (err) {
    console.warn('[TrickOrTreat] sale lookup failed:', err.message);
    await interaction.editReply('👻 UglyBot could not verify that purchase right now. Nothing was claimed. Try again shortly.');
    return;
  }

  const walletSet = new Set(wallets);
  const candidates = events
    .map((event) => ({
      event,
      buyer: eventBuyer(event),
      seller: eventSeller(event),
      timestamp: Number(event?.event_timestamp || 0),
    }))
    .filter((x) => x.buyer && walletSet.has(x.buyer))
    .filter((x) => !x.seller || !walletSet.has(x.seller))
    .filter((x) => x.timestamp >= Math.floor(cfg.startAt.getTime() / 1000) && x.timestamp <= Math.floor(cfg.endAt.getTime() / 1000))
    .sort((a, b) => b.timestamp - a.timestamp);

  if (!candidates.length) {
    await interaction.editReply('👻 I could not find an October secondary sale of that Squig to one of your connected event wallets.');
    return;
  }

  if (cfg.requireCurrentOwnership) {
    const currentlyOwned = await ownedTokenIds(wallets).catch(() => null);
    if (!currentlyOwned) {
      await interaction.editReply('👻 Ownership verification is temporarily unavailable. Nothing was claimed. Try again shortly.');
      return;
    }
    if (!currentlyOwned.map(String).includes(parsed.tokenId)) {
      await interaction.editReply('👻 That purchase was found, but the Squig is no longer held by one of your connected event wallets.');
      return;
    }
  }

  let verified = null;
  let lastError = null;
  for (const candidate of candidates) {
    try {
      const chainCheck = await verifySaleOnChain(candidate.event, parsed.tokenId, candidate.buyer, candidate.seller, cfg);
      verified = { ...candidate, ...chainCheck };
      break;
    } catch (err) {
      lastError = err;
    }
  }
  if (!verified) {
    await interaction.editReply(`👻 I found a possible sale, but could not verify it on-chain. ${String(lastError?.message || 'Try again shortly.').slice(0, 300)}`);
    return;
  }

  const pool = resolvePool();
  try {
    await pool.query(
      `INSERT INTO trick_or_treat_tricks(
        event_key,guild_id,discord_id,chain,contract_address,token_id,transaction_hash,order_hash,
        seller_wallet,buyer_wallet,event_timestamp,block_number
       ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,to_timestamp($11),$12)`,
      [
        cfg.eventKey, interaction.guildId, interaction.user.id, cfg.chain, cfg.contract, parsed.tokenId,
        verified.txHash, String(verified.event?.order_hash || '') || null, verified.seller,
        verified.buyer, verified.timestamp, verified.blockNumber,
      ]
    );
  } catch (err) {
    if (String(err?.code) === '23505') {
      await interaction.editReply('👻 That exact purchase has already been claimed as a Trick.');
      return;
    }
    throw err;
  }

  const counts = await userCounts(interaction.guildId, interaction.user.id);
  await interaction.editReply({ content: `👻 **TRICK VERIFIED!**\nSquig #${parsed.tokenId} was purchased by one of your connected wallets. +1 Trick.`, embeds: [bagEmbed(interaction.user, counts, cfg)] });

  if (cfg.publicActivityChannelId && deps?.client?.channels?.fetch) {
    const channel = await deps.client.channels.fetch(cfg.publicActivityChannelId).catch(() => null);
    await channel?.send?.({ content: `👻 **TRICK VERIFIED** — Squig #${parsed.tokenId} has been rescued from secondary. Another Trick just entered the bag. 🎃` }).catch(() => null);
  }
}

async function showBag(interaction) {
  await interaction.deferReply({ flags: EPHEMERAL });
  const counts = await userCounts(interaction.guildId, interaction.user.id);
  await interaction.editReply({ embeds: [bagEmbed(interaction.user, counts)] });
}

function requireAdmin(interaction) {
  if (typeof deps?.isAdmin === 'function') return Boolean(deps.isAdmin(interaction));
  return Boolean(interaction.memberPermissions?.has(PermissionFlagsBits.Administrator));
}

async function aggregateEntries(guildId) {
  const cfg = getConfig();
  const pool = resolvePool();
  const { rows } = await pool.query(
    `WITH users AS (
       SELECT discord_id FROM trick_or_treat_treats WHERE event_key=$1 AND guild_id=$2
       UNION
       SELECT discord_id FROM trick_or_treat_tricks WHERE event_key=$1 AND guild_id=$2
     )
     SELECT u.discord_id,
       (SELECT COUNT(*)::int FROM trick_or_treat_treats t WHERE t.event_key=$1 AND t.guild_id=$2 AND t.discord_id=u.discord_id) AS treats,
       (SELECT COUNT(*)::int FROM trick_or_treat_tricks k WHERE k.event_key=$1 AND k.guild_id=$2 AND k.discord_id=u.discord_id) AS tricks
     FROM users u
     ORDER BY (SELECT COUNT(*) FROM trick_or_treat_treats t WHERE t.event_key=$1 AND t.guild_id=$2 AND t.discord_id=u.discord_id) +
              (SELECT COUNT(*) FROM trick_or_treat_tricks k WHERE k.event_key=$1 AND k.guild_id=$2 AND k.discord_id=u.discord_id) DESC, u.discord_id`,
    [cfg.eventKey, String(guildId)]
  );
  return rows.map((r) => ({ discordId: String(r.discord_id), treats: Number(r.treats || 0), tricks: Number(r.tricks || 0), entries: Number(r.treats || 0) + Number(r.tricks || 0) }));
}

function csvEscape(value) {
  const text = String(value ?? '');
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

async function exportEntries(interaction) {
  const rows = await aggregateEntries(interaction.guildId);
  const lines = ['discord_id,treats,tricks,total_entries', ...rows.map((r) => [r.discordId, r.treats, r.tricks, r.entries].map(csvEscape).join(','))];
  const file = new AttachmentBuilder(Buffer.from(lines.join('\n'), 'utf8'), { name: 'trick-or-treat-entries.csv' });
  await interaction.reply({ content: `🎃 Exported ${rows.length} participant(s).`, files: [file], flags: EPHEMERAL });
}

function deterministicPick(seedHex, round, total) {
  const digest = crypto.createHash('sha256').update(`${seedHex}:${round}`).digest('hex');
  return Number(BigInt('0x' + digest) % BigInt(total));
}

function drawUniqueWinners(rows, count, seedHex) {
  const pool = rows.filter((r) => r.entries > 0).map((r) => ({ ...r }));
  const winners = [];
  const max = Math.min(count, pool.length);
  for (let round = 0; round < max; round++) {
    const total = pool.reduce((sum, r) => sum + r.entries, 0);
    if (total <= 0) break;
    let ticket = deterministicPick(seedHex, round, total);
    let idx = 0;
    for (; idx < pool.length; idx++) {
      if (ticket < pool[idx].entries) break;
      ticket -= pool[idx].entries;
    }
    const winner = pool.splice(Math.min(idx, pool.length - 1), 1)[0];
    winners.push(winner);
  }
  return winners;
}

async function runDraw(interaction) {
  const cfg = getConfig();
  const force = interaction.options.getBoolean('force', false);
  if (!force && new Date() <= cfg.endAt) {
    await interaction.reply({ content: 'The event has not ended yet. Use force only for an intentional test/admin draw.', flags: EPHEMERAL });
    return;
  }
  await interaction.deferReply({ flags: EPHEMERAL });
  const requested = interaction.options.getInteger('winners', true);
  const rows = await aggregateEntries(interaction.guildId);
  const eligible = rows.filter((r) => r.entries > 0);
  if (!eligible.length) {
    await interaction.editReply('No eligible entries exist.');
    return;
  }
  const seedHex = crypto.randomBytes(32).toString('hex');
  const winners = drawUniqueWinners(eligible, requested, seedHex);
  const pool = resolvePool();
  const client = await pool.connect();
  let drawId;
  try {
    await client.query('BEGIN');
    const inserted = await client.query(
      `INSERT INTO trick_or_treat_draws(event_key,guild_id,requested_winners,seed_hex,entry_snapshot,created_by)
       VALUES($1,$2,$3,$4,$5::jsonb,$6) RETURNING id`,
      [cfg.eventKey, interaction.guildId, requested, seedHex, JSON.stringify(eligible), interaction.user.id]
    );
    drawId = inserted.rows[0].id;
    for (let i = 0; i < winners.length; i++) {
      await client.query(
        `INSERT INTO trick_or_treat_draw_winners(draw_id,position,discord_id,entries_at_draw) VALUES($1,$2,$3,$4)`,
        [drawId, i + 1, winners[i].discordId, winners[i].entries]
      );
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    client.release();
  }
  await interaction.editReply(
    `🎃 Draw #${drawId} complete.\n` +
    winners.map((w, i) => `${i + 1}. <@${w.discordId}> — ${w.entries} entries`).join('\n') +
    `\n\nAudit seed: \`${seedHex}\``
  );
}

async function handleCommand(interaction) {
  if (!interaction.isChatInputCommand?.() || interaction.commandName !== 'trickortreat') return false;
  await ensureTables();
  const sub = interaction.options.getSubcommand();
  if (sub === 'bag') {
    await showBag(interaction);
    return true;
  }
  if (!requireAdmin(interaction)) {
    await interaction.reply({ content: 'Admin only.', flags: EPHEMERAL });
    return true;
  }
  if (sub === 'post') {
    await interaction.channel.send(publicPanel());
    await interaction.reply({ content: '🎃 Trick or Treat panel posted.', flags: EPHEMERAL });
    return true;
  }
  if (sub === 'export') {
    await exportEntries(interaction);
    return true;
  }
  if (sub === 'draw') {
    await runDraw(interaction);
    return true;
  }
  if (sub === 'status') {
    const target = interaction.options.getUser('user', false);
    if (target) {
      const counts = await userCounts(interaction.guildId, target.id);
      await interaction.reply({ embeds: [bagEmbed(target, counts)], flags: EPHEMERAL });
      return true;
    }
    const rows = await aggregateEntries(interaction.guildId);
    const totalTreats = rows.reduce((s, r) => s + r.treats, 0);
    const totalTricks = rows.reduce((s, r) => s + r.tricks, 0);
    await interaction.reply({
      content: `🎃 Participants: **${rows.length}**\n🍬 Treats: **${totalTreats}**\n👻 Tricks: **${totalTricks}**\n🎟️ Entries: **${totalTreats + totalTricks}**\nEnabled: **${getConfig().enabled ? 'yes' : 'no'}**`,
      flags: EPHEMERAL,
    });
    return true;
  }
  return true;
}

async function handleButton(interaction) {
  if (!interaction.isButton?.()) return false;
  if (interaction.customId === CLAIM_BUTTON) {
    await claimTreat(interaction);
    return true;
  }
  if (interaction.customId === TRICK_BUTTON) {
    await showTrickModal(interaction);
    return true;
  }
  if (interaction.customId === BAG_BUTTON) {
    await showBag(interaction);
    return true;
  }
  return false;
}

async function handleModalSubmit(interaction) {
  if (!interaction.isModalSubmit?.() || interaction.customId !== TRICK_MODAL) return false;
  await submitTrick(interaction);
  return true;
}

module.exports = {
  initTrickOrTreat,
  buildSlashCommand,
  ensureTables,
  handleCommand,
  handleButton,
  handleModalSubmit,
  recordWalletLinkEvent,
  getConfig,
  eventDayKey,
  parseOpenSeaSquigUrl,
  drawUniqueWinners,
  deterministicPick,
};
