// Load local .env when running outside Railway (Railway injects envs)
try { require('dotenv').config(); } catch (_) {}

const {
  Client,
  GatewayIntentBits,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  StringSelectMenuBuilder,
  RoleSelectMenuBuilder,
  SlashCommandBuilder,
  REST,
  Routes,
  Events,
  PermissionFlagsBits,
  ChannelType,
} = require('discord.js');
const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');
const { createCanvas, loadImage, GlobalFonts } = require('@napi-rs/canvas');
const { renderSquigCardExact } = require('./card_renderer');
const { ethers } = require('ethers');
const { Pool } = require('pg');

// ===== ENV =====
const DISCORD_TOKEN      = process.env.DISCORD_BOT_TOKEN || process.env.DISCORD_TOKEN;
const DISCORD_CLIENT_ID  = process.env.DISCORD_CLIENT_ID;
const GUILD_ID           = process.env.GUILD_ID;
const ETHERSCAN_API_KEY  = process.env.ETHERSCAN_API_KEY;
const ALCHEMY_API_KEY    = process.env.ALCHEMY_API_KEY;
const OPENSEA_API_KEY    = process.env.OPENSEA_API_KEY || ''; // optional
const DEFAULT_ADMIN_USER = process.env.DEFAULT_ADMIN_USER || '';

// Visual/render tunables
const RENDER_SCALE = 3; // 1 = 750x1050. 2 = 1500x2100. 3 = 2250x3150
const MASK_EPS = 0.75; // pixels
const BG_CORNER_TIGHTEN = 2; // shave bg corners tighter than card radius
const BG_ZOOM = 1.3; // >1 zooms bg under rounded mask
const BG_PAN_X = 2;  // optional subtle pan
const BG_PAN_Y = 2;

// ===== FONT REGISTRATION (auto-download if missing) =====
let FONT_REGULAR_FAMILY = 'PlaypenSans-Regular';
let FONT_BOLD_FAMILY    = 'PlaypenSans-Bold';

async function ensureFonts() {
  const FONT_DIR = 'fonts';
  fs.mkdirSync(FONT_DIR, { recursive: true });

  const files = [
    {
      url: 'https://raw.githubusercontent.com/TypeTogether/Playpen-Sans/main/fonts/ttf/PlaypenSans-Regular.ttf',
      path: `${FONT_DIR}/PlaypenSans-Regular.ttf`,
      family: 'PlaypenSans-Regular'
    },
    {
      url: 'https://raw.githubusercontent.com/TypeTogether/Playpen-Sans/main/fonts/ttf/PlaypenSans-Bold.ttf',
      path: `${FONT_DIR}/PlaypenSans-Bold.ttf`,
      family: 'PlaypenSans-Bold'
    }
  ];

  for (const f of files) {
    if (!fs.existsSync(f.path)) {
      const r = await fetch(f.url);
      if (!r.ok) throw new Error(`Font download failed: ${r.status} (${f.url})`);
      fs.writeFileSync(f.path, Buffer.from(await r.arrayBuffer()));
    }
    try { GlobalFonts.registerFromPath(f.path, f.family); }
    catch (e) { console.warn('Font register error:', e.message); }
  }
  console.log('🖋 Fonts ready:', files.map(f => f.family).join(', '));
}
ensureFonts().catch(e => {
  console.warn('⚠️ Could not ensure fonts:', e.message);
  FONT_REGULAR_FAMILY = 'sans-serif';
  FONT_BOLD_FAMILY    = 'sans-serif';
});

// Debug env (safe booleans/ids only)
console.log('ENV CHECK:', {
  hasToken: !!DISCORD_TOKEN,
  clientId: DISCORD_CLIENT_ID,
  guildId: GUILD_ID,
  hasAlchemy: !!ALCHEMY_API_KEY,
  hasOpenSea: !!OPENSEA_API_KEY,
  hasPointsDb: !!process.env.DATABASE_URL_POINTS
});

// ===== CONTRACTS =====
const UGLY_CONTRACT    = '0x9492505633d74451bdf3079c09ccc979588bc309';
const MONSTER_CONTRACT = '0x1cD7fe72D64f6159775643ACEdc7D860dFB80348';
const SQUIGS_CONTRACT  = '0x9bf567ddf41b425264626d1b8b2c7f7c660b1c42';

// ===== CHARM DROPS =====
const CHARM_REWARD_CHANCE = 100; // 1 in 200
const CHARM_REWARDS = [150, 200, 350, 200]; // Weighted pool
const SUPPORT_TICKET_CHANNEL_ID = '1324090267699122258';
const ADMIN_LOG_CHANNEL_ID = '1477463175665287410';

// ===== CLIENT =====
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent
  ]
});

// ===== UTILS =====
async function fetchWithTimeout(url, { timeoutMs = 10000, ...opts } = {}) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...opts, signal: controller.signal });
  } finally {
    clearTimeout(t);
  }
}

const fetchWithRetry = async (url, retries = 3, delay = 1000, opts = {}) => {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const res = await fetchWithTimeout(url, { timeoutMs: 10000, ...opts });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res;
    } catch (err) {
      if (attempt === retries - 1) throw err;
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
};

async function mapLimit(items, limit, fn) {
  const results = new Array(items.length);
  let i = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (i < items.length) {
      const idx = i++;
      results[idx] = await fn(items[idx], idx);
    }
  });
  await Promise.all(workers);
  return results;
}

// ===== POSTGRES =====
const DATABASE_URL_HOLDERS = process.env.DATABASE_URL_HOLDERS || process.env.DATABASE_URL || null;
const DATABASE_URL_TEAM = process.env.DATABASE_URL_TEAM || process.env.DATABASE_URL || null;
const DATABASE_URL_POINTS = process.env.DATABASE_URL_POINTS || null;
const PGSSL = (process.env.PGSSL ?? 'true') !== 'false'; // default true on Railway

const holdersPool = new Pool(
  DATABASE_URL_HOLDERS
    ? { connectionString: DATABASE_URL_HOLDERS, ssl: PGSSL ? { rejectUnauthorized: false } : false }
    : {
        host: process.env.PGHOST,
        user: process.env.PGUSER,
        password: process.env.PGPASSWORD,
        port: Number(process.env.PGPORT || 5432),
        database: process.env.PGDATABASE,
        ssl: PGSSL ? { rejectUnauthorized: false } : false
      }
);

const teamPool = new Pool(
  DATABASE_URL_TEAM
    ? { connectionString: DATABASE_URL_TEAM, ssl: PGSSL ? { rejectUnauthorized: false } : false }
    : {
        host: process.env.PGHOST,
        user: process.env.PGUSER,
        password: process.env.PGPASSWORD,
        port: Number(process.env.PGPORT || 5432),
        database: process.env.PGDATABASE,
        ssl: PGSSL ? { rejectUnauthorized: false } : false
      }
);

const pointsPool = DATABASE_URL_POINTS
  ? new Pool(
      { connectionString: DATABASE_URL_POINTS, ssl: PGSSL ? { rejectUnauthorized: false } : false }
    )
  : teamPool;

async function ensureHoldersSchema() {
  await holdersPool.query(`
    CREATE TABLE IF NOT EXISTS wallet_links (
      guild_id TEXT NOT NULL,
      discord_id TEXT NOT NULL,
      wallet_address TEXT NOT NULL,
      drip_member_id TEXT,
      verified BOOLEAN NOT NULL DEFAULT FALSE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await holdersPool.query(`ALTER TABLE wallet_links ADD COLUMN IF NOT EXISTS drip_member_id TEXT;`);
  await holdersPool.query(`ALTER TABLE wallet_links ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();`);
  await holdersPool.query(`DROP INDEX IF EXISTS wallet_links_guild_user_idx;`);
  await holdersPool.query(`CREATE UNIQUE INDEX IF NOT EXISTS wallet_links_guild_user_wallet_idx ON wallet_links (guild_id, discord_id, wallet_address);`);
  await holdersPool.query(`CREATE INDEX IF NOT EXISTS wallet_links_guild_user_idx ON wallet_links (guild_id, discord_id);`);
  await holdersPool.query(`
    CREATE TABLE IF NOT EXISTS claims (
      id BIGSERIAL PRIMARY KEY,
      guild_id TEXT NOT NULL,
      discord_id TEXT NOT NULL,
      claim_day DATE NOT NULL,
      amount NUMERIC NOT NULL,
      wallet_address TEXT NOT NULL,
      receipt_channel_id TEXT,
      receipt_message_id TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await holdersPool.query(`CREATE UNIQUE INDEX IF NOT EXISTS claims_guild_user_day_idx ON claims (guild_id, discord_id, claim_day);`);
  await holdersPool.query(`
    CREATE TABLE IF NOT EXISTS verification_panels (
      id BIGSERIAL PRIMARY KEY,
      guild_id TEXT NOT NULL,
      channel_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      created_by TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  console.log('✅ holders schema ready');
}

async function ensureTeamSchema() {
  await teamPool.query(`
    CREATE TABLE IF NOT EXISTS guild_settings (
      guild_id TEXT PRIMARY KEY,
      drip_api_key TEXT,
      drip_client_id TEXT,
      drip_realm_id TEXT,
      currency_id TEXT,
      receipt_channel_id TEXT,
      points_label TEXT NOT NULL DEFAULT 'UglyPoints',
      payout_type TEXT NOT NULL DEFAULT 'per_up',
      payout_amount NUMERIC NOT NULL DEFAULT 1,
      claim_streak_bonus NUMERIC NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await teamPool.query(`ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS drip_client_id TEXT;`);
  await teamPool.query(`ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS receipt_channel_id TEXT;`);
  await teamPool.query(`ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS points_label TEXT NOT NULL DEFAULT 'UglyPoints';`);
  await teamPool.query(`ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS claim_streak_bonus NUMERIC NOT NULL DEFAULT 0;`);
  await teamPool.query(`
    CREATE TABLE IF NOT EXISTS holder_rules (
      id BIGSERIAL PRIMARY KEY,
      guild_id TEXT NOT NULL,
      role_id TEXT NOT NULL,
      role_name TEXT NOT NULL,
      contract_address TEXT NOT NULL,
      min_tokens INTEGER NOT NULL DEFAULT 1,
      max_tokens INTEGER,
      enabled BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await teamPool.query(`
    CREATE TABLE IF NOT EXISTS trait_role_rules (
      id BIGSERIAL PRIMARY KEY,
      guild_id TEXT NOT NULL,
      role_id TEXT NOT NULL,
      role_name TEXT NOT NULL,
      contract_address TEXT NOT NULL,
      trait_category TEXT,
      trait_value TEXT NOT NULL,
      enabled BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await teamPool.query(`
    CREATE TABLE IF NOT EXISTS holder_collections (
      id BIGSERIAL PRIMARY KEY,
      guild_id TEXT NOT NULL,
      name TEXT NOT NULL,
      contract_address TEXT NOT NULL,
      enabled BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await teamPool.query(`CREATE UNIQUE INDEX IF NOT EXISTS holder_collections_guild_contract_uidx ON holder_collections (guild_id, contract_address);`);
  console.log('✅ team schema ready');
}

async function ensurePointsSchema() {
  await pointsPool.query(`
    CREATE TABLE IF NOT EXISTS holder_point_mappings (
      guild_id TEXT NOT NULL,
      contract_address TEXT NOT NULL,
      mapping_json JSONB NOT NULL,
      created_by_discord_id TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (guild_id, contract_address)
    );
  `);
  await pointsPool.query(`ALTER TABLE holder_point_mappings ADD COLUMN IF NOT EXISTS created_by_discord_id TEXT;`);
  console.log(`✅ points schema ready (${DATABASE_URL_POINTS ? 'DATABASE_URL_POINTS' : 'team database fallback'})`);
}

ensureHoldersSchema().catch(e => console.error('Holders schema error:', e.message));
ensureTeamSchema().catch(e => console.error('Team schema error:', e.message));
ensurePointsSchema().catch(e => console.error('Points schema error:', e.message));

async function setWalletLink(guildId, discordId, walletAddress, verified = false, dripMemberId = null) {
  try {
    await holdersPool.query(
      `INSERT INTO wallet_links (guild_id, discord_id, wallet_address, verified, drip_member_id)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (guild_id, discord_id, wallet_address) DO UPDATE
       SET verified = EXCLUDED.verified, drip_member_id = EXCLUDED.drip_member_id, updated_at = NOW()`,
      [guildId, discordId, walletAddress, verified, dripMemberId]
    );
  } catch (err) {
    await postAdminSystemLog({
      guildId,
      category: 'Wallet Link Issue',
      message:
        `Failed to save wallet link.\n` +
        `Discord ID: \`${discordId}\`\n` +
        `Wallet: \`${walletAddress}\`\n` +
        `Reason: ${String(err?.message || err || '').slice(0, 300)}`
    });
    throw err;
  }
}

async function getWalletLinks(guildId, discordId) {
  const { rows } = await holdersPool.query(
    `SELECT wallet_address, verified, drip_member_id, created_at, updated_at
     FROM wallet_links
     WHERE guild_id = $1 AND discord_id = $2
     ORDER BY updated_at DESC`,
    [guildId, discordId]
  );
  return rows;
}

async function getWalletOwnerLink(guildId, walletAddress) {
  const { rows } = await holdersPool.query(
    `SELECT discord_id, verified, drip_member_id, created_at, updated_at
     FROM wallet_links
     WHERE guild_id = $1 AND wallet_address = $2
     ORDER BY updated_at DESC
     LIMIT 1`,
    [guildId, walletAddress]
  );
  return rows[0] || null;
}

async function reassignWalletLink(guildId, discordId, walletAddress, verified = false, dripMemberId = null) {
  try {
    await holdersPool.query(
      `DELETE FROM wallet_links WHERE guild_id = $1 AND wallet_address = $2 AND discord_id <> $3`,
      [guildId, walletAddress, discordId]
    );
    await setWalletLink(guildId, discordId, walletAddress, verified, dripMemberId);
  } catch (err) {
    await postAdminSystemLog({
      guildId,
      category: 'Wallet Link Issue',
      message:
        `Failed to reassign wallet link.\n` +
        `Discord ID: \`${discordId}\`\n` +
        `Wallet: \`${walletAddress}\`\n` +
        `Reason: ${String(err?.message || err || '').slice(0, 300)}`
    });
    throw err;
  }
}

async function deleteWalletLink(guildId, discordId, walletAddress) {
  try {
    const { rowCount } = await holdersPool.query(
      `DELETE FROM wallet_links WHERE guild_id = $1 AND discord_id = $2 AND wallet_address = $3`,
      [guildId, discordId, walletAddress]
    );
    return rowCount || 0;
  } catch (err) {
    await postAdminSystemLog({
      guildId,
      category: 'Wallet Link Issue',
      message:
        `Failed to remove wallet link.\n` +
        `Discord ID: \`${discordId}\`\n` +
        `Wallet: \`${walletAddress}\`\n` +
        `Reason: ${String(err?.message || err || '').slice(0, 300)}`
    });
    throw err;
  }
}

// ===== Slash command registrar (guild-scoped for fast iteration) =====
function buildSlashCommands() {
  return [
    new SlashCommandBuilder()
      .setName('launch-verification')
      .setDescription('Post the public holder verification menu in this channel')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('launch-rewards')
      .setDescription('Post the public holder rewards menu in this channel')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('setup-verification')
      .setDescription('Create/open a private admin setup channel for verification config')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('set-points-mapping')
      .setDescription('Upload a CSV attachment to set points mapping for a collection')
      .addStringOption((opt) =>
        opt
          .setName('collection')
          .setDescription('Collection name or contract address')
          .setRequired(true)
      )
      .addAttachmentOption((opt) =>
        opt
          .setName('csv_file')
          .setDescription('CSV file with category/trait/points mapping')
          .setRequired(true)
      )
      .addStringOption((opt) =>
        opt
          .setName('mode')
          .setDescription('How to apply this CSV to existing mapping')
          .setRequired(false)
          .addChoices(
            { name: 'Merge (append/update traits)', value: 'merge' },
            { name: 'Replace (overwrite mapping)', value: 'replace' }
          )
      )
      .toJSON(),
    new SlashCommandBuilder()
      .setName('remove-points-mapping')
      .setDescription('Remove points mapping for a collection')
      .addStringOption((opt) =>
        opt
          .setName('collection')
          .setDescription('Collection name or contract address')
          .setRequired(true)
      )
      .toJSON(),
    new SlashCommandBuilder()
      .setName('connectuser')
      .setDescription('Admin override: manually link and verify a user wallet with a DRIP member ID')
      .addStringOption((opt) =>
        opt
          .setName('discord_id')
          .setDescription('Discord user ID to link the wallet to')
          .setRequired(true)
      )
      .addStringOption((opt) =>
        opt
          .setName('wallet')
          .setDescription('Ethereum wallet address')
          .setRequired(true)
      )
      .addStringOption((opt) =>
        opt
          .setName('drip_user_id')
          .setDescription('DRIP member/user ID to store on the wallet link')
          .setRequired(true)
      )
      .toJSON(),
    new SlashCommandBuilder()
      .setName('disconnectuser')
      .setDescription('Admin override: remove a linked wallet from a user')
      .addStringOption((opt) =>
        opt
          .setName('discord_id')
          .setDescription('Discord user ID to remove the wallet from')
          .setRequired(true)
      )
      .addStringOption((opt) =>
        opt
          .setName('wallet')
          .setDescription('Ethereum wallet address to remove')
          .setRequired(true)
      )
      .toJSON(),
    new SlashCommandBuilder()
      .setName('listuserwallets')
      .setDescription('Admin: view linked wallets and verification status for a user')
      .addStringOption((opt) =>
        opt
          .setName('discord_id')
          .setDescription('Discord user ID to inspect')
          .setRequired(true)
      )
      .toJSON(),
    new SlashCommandBuilder()
      .setName('healthcheck')
      .setDescription('Admin: check verification and reward system health for this server')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('info')
      .setDescription('Admin: view a plain-English guide for how this bot works')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('info-user')
      .setDescription('Admin: post a public plain-English guide for users')
      .toJSON(),
  ];
}

async function registerSlashCommands(clientRef) {
  try {
    if (!DISCORD_CLIENT_ID) {
      console.warn('⚠️ DISCORD_CLIENT_ID missing; cannot register slash commands.');
      return;
    }
    const rest = new REST({ version: '10' }).setToken(DISCORD_TOKEN);
    const commands = buildSlashCommands();
    const guildIds = (GUILD_ID ? [GUILD_ID] : [...clientRef.guilds.cache.keys()]);
    if (guildIds.length === 0) {
      console.warn('⚠️ Bot is not in any guilds yet; skipping slash registration.');
      return;
    }
    for (const gid of guildIds) {
      const data = await rest.put(
        Routes.applicationGuildCommands(DISCORD_CLIENT_ID, gid),
        { body: commands }
      );
      console.log(`✅ Registered ${data.length} guild slash command(s) to ${gid}.`);
    }
  } catch (e) {
    console.error('❌ Slash register error:', e?.data ?? e);
  }
}

// ===== READY =====
client.once(Events.ClientReady, async (c) => {
  console.log(`✅ Logged in as ${c.user.tag}`);
  try { await registerSlashCommands(c); } catch (e) {
    console.error('Slash register error:', e.message);
  }
});

const RECEIPT_CHANNEL_ID = '1403005536982794371';
globalThis.__PENDING_HOLDER_RULES ||= new Map();
globalThis.__PENDING_TRAIT_ROLE_RULES ||= new Map();
globalThis.__PENDING_POINTS_MAPPING ||= new Map();
globalThis.__PENDING_CHECK_STATS ||= new Map();

function normalizeEthAddress(input) {
  const addr = String(input || '').trim();
  if (!/^0x[a-fA-F0-9]{40}$/.test(addr)) return null;
  return addr.toLowerCase();
}

function labelForContract(contractAddress) {
  const c = String(contractAddress || '').toLowerCase();
  if (c === UGLY_CONTRACT.toLowerCase()) return 'Charm of the Ugly';
  if (c === MONSTER_CONTRACT.toLowerCase()) return 'Ugly Monsters';
  if (c === SQUIGS_CONTRACT.toLowerCase()) return 'Squigs';
  return String(contractAddress || 'Unknown Contract');
}

function parseWalletAddressesInput(raw) {
  const text = String(raw || '').trim();
  if (!text) return [];
  const matches = text.match(/0x[a-fA-F0-9]{40}/g) || [];
  const out = [];
  const seen = new Set();
  for (const m of matches) {
    const normalized = normalizeEthAddress(m);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(normalized);
  }
  return out;
}

function parsePointsMappingCsv(input) {
  const text = String(input || '').trim();
  if (!text) throw new Error('CSV content is empty.');

  const lines = text.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  if (lines.length < 2) throw new Error('CSV must include a header and at least one data row.');

  const delimiter = lines[0].includes('|') ? '|' : ',';
  const split = (line) => {
    if (delimiter === '|') return String(line || '').split('|').map((x) => x.trim().replace(/^"|"$/g, ''));
    return parseSimpleCsvLine(line);
  };
  const header = split(lines[0]).map((h) => String(h || '').toLowerCase());
  const catIdx = header.indexOf('category');
  const traitIdx = header.indexOf('trait');
  const ptsIdxCandidates = ['ugly_points', 'points', 'up', 'value'].map((k) => header.indexOf(k)).filter((i) => i >= 0);
  const ptsIdx = ptsIdxCandidates[0] ?? -1;
  if (catIdx < 0 || traitIdx < 0 || ptsIdx < 0) {
    throw new Error('CSV header must include: category, trait, and ugly_points (or points).');
  }

  const table = {};
  let rowCount = 0;
  for (let i = 1; i < lines.length; i++) {
    const cols = split(lines[i]);
    const category = String(cols[catIdx] ?? '').trim();
    const trait = String(cols[traitIdx] ?? '').trim();
    const points = Number(String(cols[ptsIdx] ?? '').trim());
    if (!category || !trait || !Number.isFinite(points)) continue;
    if (!table[category]) table[category] = {};
    table[category][trait] = points;
    rowCount++;
  }
  if (!rowCount) throw new Error('No valid data rows found. Check category/trait/points columns.');
  return { table, rowCount, categoryCount: Object.keys(table).length, delimiter };
}

function mergePointsMappingTables(existingTable, incomingTable) {
  const existing = (existingTable && typeof existingTable === 'object') ? existingTable : {};
  const incoming = (incomingTable && typeof incomingTable === 'object') ? incomingTable : {};
  const merged = {};

  for (const [category, traits] of Object.entries(existing)) {
    if (!traits || typeof traits !== 'object') continue;
    merged[category] = { ...traits };
  }

  let addedTraits = 0;
  let updatedTraits = 0;
  for (const [category, traits] of Object.entries(incoming)) {
    if (!traits || typeof traits !== 'object') continue;
    if (!merged[category]) merged[category] = {};
    for (const [trait, points] of Object.entries(traits)) {
      if (Object.prototype.hasOwnProperty.call(merged[category], trait)) updatedTraits++;
      else addedTraits++;
      merged[category][trait] = points;
    }
  }

  return {
    table: merged,
    addedTraits,
    updatedTraits,
    totalCategories: Object.keys(merged).length,
  };
}

async function postWalletReceipt(guild, settings, actorDiscordId, action, walletAddress) {
  const receiptChannelId = settings?.receipt_channel_id || RECEIPT_CHANNEL_ID;
  if (!receiptChannelId) return;
  try {
    const ch = await guild.channels.fetch(receiptChannelId).catch(() => null);
    if (!ch?.isTextBased()) return;
    const ts = Math.floor(Date.now() / 1000);
    await ch.send(
      `🧾 Wallet ${action}\n` +
      `User: <@${actorDiscordId}>\n` +
      `Wallet: \`${walletAddress}\`\n` +
      `Etherscan: https://etherscan.io/address/${walletAddress}\n` +
      `When: <t:${ts}:F>`
    );
  } catch (err) {
    console.warn('⚠️ Wallet receipt post failed:', String(err?.message || err || ''));
  }
}

async function postAdminSystemLog({ guild = null, guildId = null, category = 'System', message }) {
  if (!ADMIN_LOG_CHANNEL_ID) return;
  try {
    const ch = await client.channels.fetch(ADMIN_LOG_CHANNEL_ID).catch(() => null);
    if (!ch?.isTextBased()) return;
    const guildLabel = guild?.name || guildId || 'unknown';
    await ch.send(
      `**${category}**\n` +
      `Guild: ${guildLabel}\n` +
      `${String(message || '').slice(0, 1600)}`
    );
  } catch (err) {
    console.warn('⚠️ Admin system log failed:', String(err?.message || err || ''));
  }
}

async function postRoleSyncFailures(guild, actorDiscordId, syncResult, context) {
  const failedLines = Array.isArray(syncResult?.applied)
    ? syncResult.applied.filter((line) => /skipped/i.test(String(line || '')))
    : [];
  if (!failedLines.length) return;
  await postAdminSystemLog({
    guild,
    category: 'Role Sync Failure',
    message:
      `User: <@${actorDiscordId}>\n` +
      `Context: ${context}\n` +
      `${failedLines.map((line) => `- ${line}`).join('\n')}`
  });
}

async function postAdminVerificationFlag(guild, actorDiscordId, walletAddress, reason, grantedRoles = []) {
  if (!ADMIN_LOG_CHANNEL_ID) return;
  try {
    const ch = await guild.channels.fetch(ADMIN_LOG_CHANNEL_ID).catch(() => null);
    if (!ch?.isTextBased()) return;
    const roleText = grantedRoles.length ? grantedRoles.join(', ') : 'none';
    await ch.send(
      `Wallet not DRIP-verified but holder access was granted.\n` +
      `User: <@${actorDiscordId}>\n` +
      `Wallet: \`${walletAddress}\`\n` +
      `Roles granted: ${roleText}\n` +
      `Reason: ${String(reason || 'No reason provided.').slice(0, 300)}`
    );
  } catch (err) {
    console.warn('⚠️ Admin verification flag failed:', String(err?.message || err || ''));
  }
}

function isAdmin(interaction) {
  if (getDefaultAdminIds().has(String(interaction.user?.id || ''))) return true;
  return Boolean(interaction.memberPermissions?.has(PermissionFlagsBits.ManageGuild));
}

function getDefaultAdminIds() {
  return new Set(
    String(DEFAULT_ADMIN_USER)
      .split(',')
      .map(s => s.trim())
      .filter(Boolean)
  );
}

async function getGuildSettings(guildId) {
  const { rows } = await teamPool.query(`SELECT * FROM guild_settings WHERE guild_id = $1`, [guildId]);
  return rows[0] || null;
}

async function upsertGuildSetting(guildId, field, value) {
  const allowed = new Set(['drip_api_key', 'drip_client_id', 'drip_realm_id', 'currency_id', 'receipt_channel_id', 'points_label', 'payout_type', 'payout_amount', 'claim_streak_bonus']);
  if (!allowed.has(field)) throw new Error(`Invalid setting field: ${field}`);
  await teamPool.query(
    `INSERT INTO guild_settings (guild_id, ${field}, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (guild_id) DO UPDATE
     SET ${field} = EXCLUDED.${field}, updated_at = NOW()`,
    [guildId, value]
  );
}

function getPointsLabel(settings) {
  const label = String(settings?.points_label || '').trim();
  return label || 'UglyPoints';
}

async function clearDripSettings(guildId) {
  await teamPool.query(
    `INSERT INTO guild_settings (
       guild_id, drip_api_key, drip_client_id, drip_realm_id, currency_id, receipt_channel_id, payout_type, payout_amount, claim_streak_bonus, updated_at
     )
     VALUES ($1, NULL, NULL, NULL, NULL, NULL, 'per_up', 1, 0, NOW())
     ON CONFLICT (guild_id) DO UPDATE
     SET drip_api_key = NULL,
         drip_client_id = NULL,
         drip_realm_id = NULL,
         currency_id = NULL,
         receipt_channel_id = NULL,
         payout_type = 'per_up',
         payout_amount = 1,
         claim_streak_bonus = 0,
         updated_at = NOW()`,
    [guildId]
  );
}

async function getHolderRules(guildId) {
  const { rows } = await teamPool.query(
    `SELECT * FROM holder_rules WHERE guild_id = $1 AND enabled = TRUE ORDER BY id ASC`,
    [guildId]
  );
  return rows;
}

async function getTraitRoleRules(guildId) {
  const { rows } = await teamPool.query(
    `SELECT * FROM trait_role_rules WHERE guild_id = $1 AND enabled = TRUE ORDER BY id ASC`,
    [guildId]
  );
  return rows;
}

function defaultHolderCollections() {
  return [
    { name: 'Charm of the Ugly', contract_address: UGLY_CONTRACT.toLowerCase() },
    { name: 'Ugly Monsters', contract_address: MONSTER_CONTRACT.toLowerCase() },
    { name: 'Squigs', contract_address: SQUIGS_CONTRACT.toLowerCase() },
  ];
}

async function getHolderCollections(guildId) {
  const { rows } = await teamPool.query(
    `SELECT name, contract_address FROM holder_collections WHERE guild_id = $1 AND enabled = TRUE ORDER BY created_at ASC`,
    [guildId]
  );
  const out = [];
  const seen = new Set();
  for (const c of [...defaultHolderCollections(), ...rows]) {
    const addr = normalizeEthAddress(c.contract_address);
    if (!addr || seen.has(addr)) continue;
    seen.add(addr);
    out.push({ name: String(c.name || addr), contract_address: addr });
  }
  return out;
}

async function upsertHolderCollection(guildId, name, contractAddress) {
  await teamPool.query(
    `INSERT INTO holder_collections (guild_id, name, contract_address, enabled)
     VALUES ($1, $2, $3, TRUE)
     ON CONFLICT (guild_id, contract_address) DO UPDATE
     SET name = EXCLUDED.name, enabled = TRUE`,
    [guildId, String(name || '').trim(), String(contractAddress || '').toLowerCase()]
  );
}

async function setGuildPointMapping(guildId, contractAddress, mappingTable, actorDiscordId = null) {
  await pointsPool.query(
    `INSERT INTO holder_point_mappings (guild_id, contract_address, mapping_json, created_by_discord_id, updated_at)
     VALUES ($1, $2, $3::jsonb, $4, NOW())
     ON CONFLICT (guild_id, contract_address) DO UPDATE
     SET mapping_json = EXCLUDED.mapping_json,
         created_by_discord_id = COALESCE(holder_point_mappings.created_by_discord_id, EXCLUDED.created_by_discord_id),
         updated_at = NOW()`,
    [guildId, String(contractAddress || '').toLowerCase(), JSON.stringify(mappingTable || {}), actorDiscordId ? String(actorDiscordId) : null]
  );
}

async function getGuildPointMappings(guildId) {
  const { rows } = await pointsPool.query(
    `SELECT contract_address, mapping_json FROM holder_point_mappings WHERE guild_id = $1`,
    [guildId]
  );
  const out = new Map();
  for (const r of rows) {
    const c = normalizeEthAddress(r.contract_address);
    if (!c) continue;
    out.set(c, (r.mapping_json && typeof r.mapping_json === 'object') ? r.mapping_json : {});
  }
  return out;
}

async function getGuildPointMappingsWithOwners(guildId) {
  const { rows } = await pointsPool.query(
    `SELECT contract_address, created_by_discord_id FROM holder_point_mappings WHERE guild_id = $1`,
    [guildId]
  );
  const out = [];
  for (const r of rows) {
    const contractAddress = normalizeEthAddress(r.contract_address);
    if (!contractAddress) continue;
    out.push({
      contractAddress,
      createdByDiscordId: r.created_by_discord_id ? String(r.created_by_discord_id) : null,
    });
  }
  return out;
}

async function removeGuildPointMapping(guildId, contractAddress, actorDiscordId) {
  const { rows } = await pointsPool.query(
    `SELECT created_by_discord_id FROM holder_point_mappings WHERE guild_id = $1 AND contract_address = $2`,
    [guildId, String(contractAddress || '').toLowerCase()]
  );
  const row = rows[0] || null;
  if (!row) return { ok: false, reason: 'not_found' };

  const actorId = String(actorDiscordId || '');
  const ownerId = row.created_by_discord_id ? String(row.created_by_discord_id) : null;
  const isDefaultAdminUser = getDefaultAdminIds().has(actorId);
  const canDelete = isDefaultAdminUser || (ownerId && actorId === ownerId);
  if (!canDelete) return { ok: false, reason: 'forbidden', ownerId };

  await pointsPool.query(
    `DELETE FROM holder_point_mappings WHERE guild_id = $1 AND contract_address = $2`,
    [guildId, String(contractAddress || '').toLowerCase()]
  );
  return { ok: true, ownerId };
}

async function addHolderRule(guild, { roleId, contractAddress, minTokens, maxTokens }) {
  const role = guild.roles.cache.get(roleId);
  if (!role) throw new Error(`Role not found: ${roleId}`);
  await teamPool.query(
    `INSERT INTO holder_rules (guild_id, role_id, role_name, contract_address, min_tokens, max_tokens, enabled)
     VALUES ($1, $2, $3, $4, $5, $6, TRUE)`,
    [guild.id, role.id, role.name, contractAddress.toLowerCase(), minTokens, maxTokens]
  );
  return role;
}

async function addTraitRoleRule(guild, { roleId, contractAddress, traitCategory, traitValue }) {
  const role = guild.roles.cache.get(roleId);
  if (!role) throw new Error(`Role not found: ${roleId}`);
  await teamPool.query(
    `INSERT INTO trait_role_rules (guild_id, role_id, role_name, contract_address, trait_category, trait_value, enabled)
     VALUES ($1, $2, $3, $4, $5, $6, TRUE)`,
    [guild.id, role.id, role.name, contractAddress.toLowerCase(), traitCategory || null, traitValue]
  );
  return role;
}

async function disableHolderRule(guildId, ruleId) {
  const { rows } = await teamPool.query(
    `UPDATE holder_rules SET enabled = FALSE WHERE guild_id = $1 AND id = $2 AND enabled = TRUE RETURNING id, role_name, contract_address, min_tokens, max_tokens`,
    [guildId, ruleId]
  );
  return rows[0] || null;
}

async function disableTraitRoleRule(guildId, ruleId) {
  const { rows } = await teamPool.query(
    `UPDATE trait_role_rules
     SET enabled = FALSE
     WHERE guild_id = $1 AND id = $2 AND enabled = TRUE
     RETURNING id, role_name, contract_address, trait_category, trait_value`,
    [guildId, ruleId]
  );
  return rows[0] || null;
}

async function getOwnedTokenIdsForContract(walletAddress, contractAddress) {
  if (!ALCHEMY_API_KEY) return [];
  const out = [];
  let pageKey = null;
  do {
    const u = new URL(`https://eth-mainnet.g.alchemy.com/nft/v3/${ALCHEMY_API_KEY}/getNFTsForOwner`);
    u.searchParams.set('owner', walletAddress);
    u.searchParams.append('contractAddresses[]', contractAddress);
    u.searchParams.set('withMetadata', 'false');
    u.searchParams.set('pageSize', '100');
    if (pageKey) u.searchParams.set('pageKey', pageKey);
    const res = await fetchWithRetry(u.toString(), 3, 500);
    const data = await res.json();
    for (const nft of (data?.ownedNfts || [])) {
      const tid = String(nft.tokenId || '').trim();
      if (!tid) continue;
      try {
        out.push(tid.startsWith('0x') ? BigInt(tid).toString(10) : tid);
      } catch {
        out.push(tid);
      }
    }
    pageKey = data?.pageKey || null;
  } while (pageKey);
  return out;
}

async function countOwnedForContract(walletAddress, contractAddress) {
  const ids = await getOwnedTokenIdsForContract(walletAddress, contractAddress);
  return ids.length;
}

async function getOwnedTokenIdsForContractMany(walletAddresses, contractAddress) {
  const addresses = Array.isArray(walletAddresses) ? walletAddresses : [walletAddresses];
  const normalized = [...new Set(addresses.map(a => normalizeEthAddress(a)).filter(Boolean))];
  if (!normalized.length) return [];
  const tokenArrays = await mapLimit(normalized, 4, async (walletAddress) => {
    try { return await getOwnedTokenIdsForContract(walletAddress, contractAddress); }
    catch { return []; }
  });
  const seen = new Set();
  const out = [];
  for (const arr of tokenArrays) {
    for (const tokenId of arr) {
      const k = String(tokenId);
      if (seen.has(k)) continue;
      seen.add(k);
      out.push(k);
    }
  }
  return out;
}

async function syncHolderRoles(member, walletAddresses) {
  const rules = await getHolderRules(member.guild.id);
  const traitRules = await getTraitRoleRules(member.guild.id);
  if (!rules.length && !traitRules.length) return { changed: 0, applied: [], granted: [] };
  const me = member.guild.members.me;
  if (!me?.permissions?.has(PermissionFlagsBits.ManageRoles)) {
    return { changed: 0, applied: ['Skipped: bot is missing Manage Roles permission.'], granted: [] };
  }

  const addresses = Array.isArray(walletAddresses) ? walletAddresses : [walletAddresses];
  const normalizedAddresses = [...new Set(addresses.map(a => normalizeEthAddress(a)).filter(Boolean))];
  const byContract = new Map();
  for (const r of rules) {
    if (byContract.has(r.contract_address)) continue;
    let count = 0;
    for (const walletAddress of normalizedAddresses) {
      count += await countOwnedForContract(walletAddress, r.contract_address);
    }
    byContract.set(r.contract_address, count);
  }

  const guildPointMappings = traitRules.length ? await getGuildPointMappings(member.guild.id) : new Map();
  const traitMatchesByRuleId = new Map();
  const traitContracts = [...new Set(traitRules.map((r) => String(r.contract_address || '').toLowerCase()).filter(Boolean))];
  for (const contractAddress of traitContracts) {
    const contractRules = traitRules.filter((r) => String(r.contract_address || '').toLowerCase() === contractAddress);
    if (!contractRules.length) continue;

    const table = hpTableForContract(contractAddress, guildPointMappings);
    const eligibleRules = contractRules.filter((r) => findMatchingTraitDefinition(table, r.trait_category, r.trait_value));
    for (const r of contractRules) {
      traitMatchesByRuleId.set(r.id, false);
    }
    if (!eligibleRules.length) continue;

    const tokenIds = await getOwnedTokenIdsForContractMany(normalizedAddresses, contractAddress);
    if (!tokenIds.length) continue;

    for (const tokenId of tokenIds) {
      let grouped = null;
      try {
        const meta = await getNftMetadataAlchemy(tokenId, contractAddress);
        const { attrs } = await getTraitsForToken(meta, tokenId, contractAddress);
        grouped = normalizeTraits(attrs);
      } catch {
        grouped = null;
      }
      if (!grouped) continue;

      for (const r of eligibleRules) {
        if (traitMatchesByRuleId.get(r.id)) continue;
        if (hasTraitMatch(grouped, r.trait_category, r.trait_value)) {
          traitMatchesByRuleId.set(r.id, true);
        }
      }

      if (eligibleRules.every((r) => traitMatchesByRuleId.get(r.id))) break;
    }
  }

  let changed = 0;
  const applied = [];
  const granted = [];
  for (const r of rules) {
    const count = byContract.get(r.contract_address) || 0;
    const shouldHave = count >= Number(r.min_tokens) && (r.max_tokens == null || count <= Number(r.max_tokens));
    const role = member.guild.roles.cache.get(r.role_id);
    if (!role) {
      applied.push(`${r.role_name || r.role_id}: skipped (role not found)`);
      continue;
    }
    if (!role.editable) {
      applied.push(`${role.name}: skipped (bot cannot manage this role/hierarchy)`);
      continue;
    }

    const hasRole = member.roles.cache.has(role.id);
    try {
      if (shouldHave && !hasRole) {
        await member.roles.add(role, `Holder verification (${count} in range ${r.min_tokens}-${r.max_tokens ?? '∞'})`);
        changed++;
        granted.push(role.name);
      }
      if (!shouldHave && hasRole) {
        await member.roles.remove(role, `Holder verification (${count} outside range ${r.min_tokens}-${r.max_tokens ?? '∞'})`);
        changed++;
      }
    } catch (err) {
      if (err?.code === 50001 || err?.code === 50013) {
        applied.push(`${role.name}: skipped (missing access/permissions)`); 
        continue;
      }
      throw err;
    }
    applied.push(`${role.name}: ${count} (${shouldHave ? 'eligible' : 'not eligible'})`);
  }

  for (const r of traitRules) {
    const role = member.guild.roles.cache.get(r.role_id);
    const categoryLabel = r.trait_category ? String(r.trait_category) : 'any';
    const traitLabel = `${categoryLabel}:${r.trait_value}`;
    if (!role) {
      applied.push(`${r.role_name || r.role_id}: skipped (role not found)`);
      continue;
    }
    if (!role.editable) {
      applied.push(`${role.name}: skipped (bot cannot manage this role/hierarchy)`);
      continue;
    }

    const shouldHave = Boolean(traitMatchesByRuleId.get(r.id));
    const hasRole = member.roles.cache.has(role.id);
    try {
      if (shouldHave && !hasRole) {
        await member.roles.add(role, `Trait role (${traitLabel} on ${r.contract_address})`);
        changed++;
        granted.push(role.name);
      }
      if (!shouldHave && hasRole) {
        await member.roles.remove(role, `Trait role (${traitLabel} not found on ${r.contract_address})`);
        changed++;
      }
    } catch (err) {
      if (err?.code === 50001 || err?.code === 50013) {
        applied.push(`${role.name}: skipped (missing access/permissions)`);
        continue;
      }
      throw err;
    }
    applied.push(`${role.name}: ${traitLabel} (${shouldHave ? 'eligible' : 'not eligible'})`);
  }
  return { changed, applied, granted };
}

async function hasClaimedToday(guildId, discordId) {
  const { rows } = await holdersPool.query(
    `SELECT id FROM claims WHERE guild_id = $1 AND discord_id = $2 AND claim_day = CURRENT_DATE LIMIT 1`,
    [guildId, discordId]
  );
  return rows.length > 0;
}

async function getClaimStreakBeforeToday(guildId, discordId) {
  const { rows } = await holdersPool.query(
    `SELECT claim_day
     FROM claims
     WHERE guild_id = $1 AND discord_id = $2 AND claim_day < CURRENT_DATE
     ORDER BY claim_day DESC`,
    [guildId, discordId]
  );

  let expected = new Date();
  expected.setUTCHours(0, 0, 0, 0);
  expected.setUTCDate(expected.getUTCDate() - 1);

  let streak = 0;
  for (const row of rows) {
    const claimDate = new Date(row.claim_day);
    claimDate.setUTCHours(0, 0, 0, 0);
    if (claimDate.getTime() !== expected.getTime()) break;
    streak++;
    expected.setUTCDate(expected.getUTCDate() - 1);
  }
  return streak;
}

async function computeWalletStatsForPayout(guildId, walletAddresses, payoutType) {
  const addresses = Array.isArray(walletAddresses) ? walletAddresses : [walletAddresses];
  const normalizedAddresses = [...new Set(addresses.map(a => normalizeEthAddress(a)).filter(Boolean))];
  if (!normalizedAddresses.length) return { unitTotal: 0, totalNfts: 0, totalUp: 0, byCollection: [] };

  const rules = await getHolderRules(guildId);
  const guildPointMappings = await getGuildPointMappings(guildId);
  const contracts = [...new Set(rules.map(r => String(r.contract_address || '').toLowerCase()).filter(Boolean))];
  if (!contracts.length) return { unitTotal: 0, totalNfts: 0, totalUp: 0, byCollection: [] };

  const byCollection = await mapLimit(contracts, 3, async (contractAddress) => {
    const ids = await getOwnedTokenIdsForContractMany(normalizedAddresses, contractAddress);
    return { contractAddress, ids };
  });
  const totalNfts = byCollection.reduce((sum, x) => sum + x.ids.length, 0);

  let totalUp = 0;
  if (payoutType === 'per_up') {
    const scorableContracts = byCollection.filter(({ contractAddress }) => {
      const table = hpTableForContract(contractAddress, guildPointMappings);
      return table && Object.keys(table).length > 0;
    });
    const perContractTotals = await mapLimit(scorableContracts, 2, async ({ contractAddress, ids }) => {
      const ups = await mapLimit(ids, 5, async (tokenId) => {
        try {
          const meta = await getNftMetadataAlchemy(tokenId, contractAddress);
          const { attrs } = await getTraitsForToken(meta, tokenId, contractAddress);
          const grouped = normalizeTraits(attrs);
          const table = hpTableForContract(contractAddress, guildPointMappings);
          const { total } = computeHpFromTraits(grouped, table);
          return total || 0;
        } catch {
          return 0;
        }
      });
      return ups.reduce((a, b) => a + b, 0);
    });
    totalUp = perContractTotals.reduce((a, b) => a + b, 0);
  }

  return {
    unitTotal: payoutType === 'per_nft' ? totalNfts : totalUp,
    totalNfts,
    totalUp,
    byCollection: byCollection.map(({ contractAddress, ids }) => ({ contractAddress, count: ids.length })),
  };
}

async function computeDailyRewardQuote(guildId, links, settings) {
  const payoutType = settings?.payout_type === 'per_nft' ? 'per_nft' : 'per_up';
  const payoutAmount = Number(settings?.payout_amount || 0);
  const verifiedWalletAddresses = (links || []).filter((x) => Boolean(x?.verified)).map((x) => x.wallet_address).filter(Boolean);
  const unverifiedWalletAddresses = (links || []).filter((x) => !x?.verified).map((x) => x.wallet_address).filter(Boolean);

  const [verifiedStats, unverifiedStats] = await Promise.all([
    verifiedWalletAddresses.length
      ? computeWalletStatsForPayout(guildId, verifiedWalletAddresses, payoutType)
      : Promise.resolve({ unitTotal: 0, totalNfts: 0, totalUp: 0, byCollection: [] }),
    unverifiedWalletAddresses.length
      ? computeWalletStatsForPayout(guildId, unverifiedWalletAddresses, payoutType)
      : Promise.resolve({ unitTotal: 0, totalNfts: 0, totalUp: 0, byCollection: [] }),
  ]);

  const effectiveUnits = verifiedStats.unitTotal + (unverifiedStats.unitTotal * 0.5);
  const unverifiedPenaltyAmount = Math.max(0, (unverifiedStats.unitTotal * 0.5) * payoutAmount);
  const dailyReward = Math.max(0, Math.floor(effectiveUnits * payoutAmount));

  return {
    payoutType,
    payoutAmount,
    verifiedStats,
    unverifiedStats,
    effectiveUnits,
    unverifiedPenaltyAmount,
    dailyReward,
    totalNfts: verifiedStats.totalNfts + unverifiedStats.totalNfts,
    totalUp: verifiedStats.totalUp + unverifiedStats.totalUp,
  };
}

async function getConnectedCollectionCounts(guildId, walletAddresses) {
  const collections = await getHolderCollections(guildId);
  if (!collections.length) return [];
  const out = await mapLimit(collections, 3, async (collection) => {
    const ids = await getOwnedTokenIdsForContractMany(walletAddresses, collection.contract_address);
    return {
      name: collection.name,
      contractAddress: collection.contract_address,
      count: ids.length,
    };
  });
  return out;
}

async function getDripMemberCurrencyBalance(realmId, memberIds, currencyId, settings) {
  const ids = [...new Set((Array.isArray(memberIds) ? memberIds : [memberIds]).map((v) => String(v || '').trim()).filter(Boolean))];
  if (!ids.length || !realmId || !currencyId || !settings?.drip_api_key) return null;

  const baseUrls = dripRealmBaseUrls(realmId);
  const variants = [
    { suffix: '/point-balance', queryKey: 'realmPointId' },
    { suffix: '/point-balance', queryKey: 'currencyId' },
    { suffix: '/balance', queryKey: 'currencyId' },
    { suffix: '/balance', queryKey: 'realmPointId' },
  ];

  const extractAmount = (payload) => {
    const candidates = [
      payload?.data,
      payload?.balance,
      payload?.pointBalance,
      payload,
    ];
    for (const item of candidates) {
      if (item == null) continue;
      if (typeof item === 'number') return item;
      if (typeof item === 'string' && item.trim() !== '' && Number.isFinite(Number(item))) return Number(item);
      if (typeof item === 'object') {
        const valueCandidates = [item.amount, item.balance, item.tokens, item.value];
        for (const value of valueCandidates) {
          if (value == null) continue;
          if (typeof value === 'number') return value;
          if (typeof value === 'string' && value.trim() !== '' && Number.isFinite(Number(value))) return Number(value);
        }
      }
    }
    return null;
  };

  for (const memberId of ids) {
    for (const baseUrl of baseUrls) {
      for (const variant of variants) {
        const url = new URL(`${baseUrl}/members/${encodeURIComponent(memberId)}${variant.suffix}`);
        url.searchParams.set(variant.queryKey, String(currencyId));
        const res = await fetchWithTimeout(url.toString(), {
          timeoutMs: 15000,
          headers: buildDripHeaders(settings),
        });
        if (res.ok) {
          const payload = await res.json().catch(() => ({}));
          const amount = extractAmount(payload);
          if (amount != null) return amount;
          continue;
        }
        if (res.status === 404 || res.status === 400 || res.status === 422) continue;
      }
    }
  }

  return null;
}

function verificationMenuEmbed(guildName) {
  return new EmbedBuilder()
    .setTitle('Holder Verification')
    .setDescription(
      `Welcome to **${guildName}**.\n\n` +
      `Use the buttons below:\n` +
      `• **Connect Wallet**: link one or more wallets for holder verification.\n` +
      `• **Disconnect Wallet**: unlink one specific wallet or all wallets.\n` +
      `• **Check Wallets Connected**: view all wallets currently linked.`
    )
    .setImage('https://i.imgur.com/HxdVgDc.png')
    .setColor(0x7ADDC0);
}

function verificationButtons() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('verify_connect').setLabel('Connect Wallet').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('verify_disconnect').setLabel('Disconnect Wallet').setStyle(ButtonStyle.Danger),
    new ButtonBuilder().setCustomId('verify_wallets').setLabel('Check Wallets Connected').setStyle(ButtonStyle.Secondary),
  );
}

function rewardsMenuEmbed(guildName, pointsLabel = 'UglyPoints') {
  return new EmbedBuilder()
    .setTitle('Holder Rewards')
    .setDescription(
      `Welcome to **${guildName}** rewards.\n\n` +
      `Use the buttons below:\n` +
      `• **Claim Rewards**: collect your daily holder payout.\n` +
      `• **Check NFT Status**: view a Squig token's ${pointsLabel} breakdown.\n` +
      `• **View Holdings**: see holdings by collection, ${pointsLabel}, or full summary.`
    )
    .setImage('https://i.imgur.com/WY5enXM.png')
    .setColor(0xB0DEEE);
}

function rewardsButtons() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('verify_claim').setLabel('Claim Rewards').setStyle(ButtonStyle.Success),
    new ButtonBuilder().setCustomId('verify_check_stats').setLabel('Check NFT Status').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('rewards_view_holdings').setLabel('View Holdings').setStyle(ButtonStyle.Primary),
  );
}

function setupMainEmbed() {
  return new EmbedBuilder()
    .setTitle('Holder Verification Setup')
    .setDescription(
      `Choose a setup action.\n` +
      `• Collections: add collection name + contract for setup options.\n` +
      `• Holder roles: add/remove collection-based role rules.\n` +
      `• Trait roles: add/remove trait-based role rules using built-in or custom mapped traits.\n` +
      `• Points Mapping: upload or remove category/trait/points CSV per collection.\n` +
      `• Setup DRIP: open DRIP settings + connection checks.\n` +
      `• View Config: show current settings and rules.`
    )
    .setColor(0xB0DEEE);
}

function setupDripEmbed() {
  return new EmbedBuilder()
    .setTitle('DRIP Setup')
    .setDescription(
      `Configure DRIP credentials and payout behavior, then verify connection.\n` +
      `• Credentials: API key, client ID, realm ID.\n` +
      `• Rewards: currency, receipt channel, payout type/amount.\n` +
      `• Verify DRIP Connection: checks realm + points endpoint access.`
    )
    .setColor(0x7ADDC0);
}

function infoEmbeds(guildName, settings = null) {
  const pointsLabel = getPointsLabel(settings);
  return [
    new EmbedBuilder()
      .setTitle('UglyBot Info: Overview')
      .setColor(0xB0DEEE)
      .setDescription(
        `This bot handles holder verification, holder roles, trait roles, rewards, DRIP payouts, and admin support tools for **${guildName}**.\n\n` +
        `In plain English:\n` +
        `• Users connect wallets.\n` +
        `• The bot checks what NFTs those wallets hold.\n` +
        `• It gives or removes Discord roles based on your rules.\n` +
        `• It can score NFTs using trait-to-points mappings.\n` +
        `• Users can claim daily rewards through DRIP.\n` +
        `• Admins can override links, inspect users, and monitor failures.`
      ),
    new EmbedBuilder()
      .setTitle('UglyBot Info: Public User Flow')
      .setColor(0x7ADDC0)
      .addFields(
        {
          name: 'Connect Wallet',
          value:
            `Users click **Connect Wallet** and enter one or more wallet addresses.\n` +
            `The bot links the wallet, checks DRIP if configured, and then runs holder role sync.`
        },
        {
          name: 'Wallet Verification',
          value:
            `If DRIP confirms the wallet belongs to the same DRIP member as the user, it is marked verified.\n` +
            `If DRIP cannot confirm it, the wallet can still be linked and roles can still work, but that wallet stays unverified.`
        },
        {
          name: 'Claim Rewards',
          value:
            `Users can claim once per UTC day.\n` +
            `Verified wallets count at full value.\n` +
            `Unverified wallets only count at **50%**.\n` +
            `If a user is docked, they get a hidden note telling them to verify in DRIP or open a support ticket.`
        },
        {
          name: 'Check Wallets / Holdings',
          value:
            `Users can view linked wallets, see if each wallet is verified or pending, and view holdings / total ${pointsLabel}.`
        }
      ),
    new EmbedBuilder()
      .setTitle('UglyBot Info: Admin Commands')
      .setColor(0x7A83BF)
      .addFields(
        {
          name: '/launch-verification',
          value: 'Posts the public wallet verification menu in the current channel.'
        },
        {
          name: '/launch-rewards',
          value: 'Posts the public rewards menu in the current channel.'
        },
        {
          name: '/setup-verification',
          value: 'Posts the private setup panel used to manage collections, roles, points mapping, and DRIP settings.'
        },
        {
          name: '/set-points-mapping and /remove-points-mapping',
          value: 'Adds, merges, replaces, or removes a collection trait-to-points CSV mapping.'
        },
        {
          name: '/connectuser and /disconnectuser',
          value: 'Manual override tools for staff to force-link or remove a wallet for a Discord user.'
        },
        {
          name: '/listuserwallets, /healthcheck, /info',
          value:
            `Use these to inspect a user, verify server health, and read this guide.\n` +
            `All are admin-only and reply with hidden messages.`
        }
      ),
    new EmbedBuilder()
      .setTitle('UglyBot Info: Setup And How-To')
      .setColor(0xB0DEEE)
      .addFields(
        {
          name: '1. Add Collections',
          value: 'Add the collection name and contract address first. Collections are the base for role rules and point mappings.'
        },
        {
          name: '2. Add Holder Roles',
          value: 'Create rules that give a Discord role when a user holds a collection within a min/max token range.'
        },
        {
          name: '3. Add Trait Roles',
          value: 'Create rules that give a role when a user owns an NFT with a specific trait value, using built-in traits or custom mapped traits.'
        },
        {
          name: '4. Configure Points Mapping',
          value:
            `Upload CSV with columns like \`category,trait,ugly_points\`.\n` +
            `This controls how NFT traits are scored for rewards and status checks.`
        },
        {
          name: '5. Configure DRIP',
          value:
            `Set DRIP API key, optional client ID, realm ID, currency ID, receipt channel, points label, payout type, payout amount, and claim streak bonus.\n` +
            `Then use **Verify DRIP Connection** to confirm the setup works.`
        },
        {
          name: '6. Test Before Going Live',
          value:
            `Test wallet connect, wallet disconnect, verified and unverified claims, admin overrides, and the health check.\n` +
            `Watch the admin log channel for warnings and failures.`
        }
      ),
    new EmbedBuilder()
      .setTitle('UglyBot Info: Important Rules And Safeguards')
      .setColor(0x7ADDC0)
      .addFields(
        {
          name: 'Duplicate Wallet Protection',
          value: 'A normal user cannot link a wallet that is already linked to another Discord user in the same server.'
        },
        {
          name: 'Admin Override Audit',
          value: 'Manual staff overrides are logged in the admin log channel so there is a paper trail.'
        },
        {
          name: 'Failure Logging',
          value:
            `The admin log channel records DRIP failures, role-sync failures, wallet-link issues, duplicate-link blocks, and other major interaction errors.`
        },
        {
          name: 'How Rewards Are Calculated',
          value:
            `Rewards are based on the configured payout type.\n` +
            `Per NFT = eligible NFT count x payout amount.\n` +
            `Per ${pointsLabel} = total ${pointsLabel} x payout amount.\n` +
            `A claim streak bonus can add a flat extra amount on continued streaks.`
        },
        {
          name: 'Support Flow',
          value:
            `If DRIP cannot verify a wallet, the user can still get holder roles, but their rewards are reduced for that wallet.\n` +
            `They should connect the same wallet in DRIP or open a ticket for manual help.`
        }
      ),
  ];
}

function infoUserEmbeds(guildName, settings = null) {
  const pointsLabel = getPointsLabel(settings);
  return [
    new EmbedBuilder()
      .setTitle('How Holder Verification Works')
      .setColor(0x7ADDC0)
      .setDescription(
        `Here is the simple version for **${guildName}**:\n\n` +
        `• Connect your wallet using the verification menu.\n` +
        `• The bot checks what eligible NFTs your linked wallets hold.\n` +
        `• If you qualify, it gives you the holder role(s).\n` +
        `• If your holdings change, your roles can update too.`
      ),
    new EmbedBuilder()
      .setTitle('How Rewards Work')
      .setColor(0xB0DEEE)
      .addFields(
        {
          name: 'Daily Claim',
          value:
            `You can claim once per UTC day from the rewards menu.\n` +
            `The bot calculates your reward based on your eligible NFTs and/or total ${pointsLabel}, depending on server settings.`
        },
        {
          name: 'Verified vs Unverified Wallets',
          value:
            `If your wallet is verified through DRIP, it counts at full value.\n` +
            `If your wallet is not verified through DRIP yet, it still counts, but only at **50%** for rewards.`
        },
        {
          name: 'Claim Streaks',
          value:
            `If this server has streak bonuses enabled, claiming on consecutive days can add an extra bonus to your reward.`
        }
      ),
    new EmbedBuilder()
      .setTitle('How To Avoid Reduced Rewards')
      .setColor(0x7A83BF)
      .addFields(
        {
          name: 'Verify Your Wallet In DRIP',
          value:
            `To get full rewards, make sure the same wallet you linked here is also connected to your DRIP profile in the correct realm.`
        },
        {
          name: 'If Verification Fails',
          value:
            `You can still keep your holder roles, but your unverified wallet rewards are reduced.\n` +
            `If you need help, open a support ticket with the team so they can review your wallet manually.`
        },
        {
          name: 'Useful Buttons',
          value:
            `• Connect Wallet\n` +
            `• Disconnect Wallet\n` +
            `• Check Wallets Connected\n` +
            `• Claim Rewards\n` +
            `• View Holdings`
        }
      ),
  ];
}

function setupMainButtons() {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('setup_add_rule').setLabel('Add Holder Role').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('setup_add_trait_rule').setLabel('Add Trait Role').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('setup_add_collection').setLabel('Add Collection').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('setup_remove_rule').setLabel('Remove Holder Role').setStyle(ButtonStyle.Danger),
      new ButtonBuilder().setCustomId('setup_remove_trait_rule').setLabel('Remove Trait Role').setStyle(ButtonStyle.Danger),
    ),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('setup_points_mapping').setLabel('Points Mapping').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('setup_drip_menu').setLabel('Setup DRIP').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('setup_remove_points_mapping').setLabel('Remove Points Mapping').setStyle(ButtonStyle.Danger),
      new ButtonBuilder().setCustomId('setup_view').setLabel('View Config').setStyle(ButtonStyle.Secondary),
    ),
  ];
}

function setupDripButtons() {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('setup_drip_key').setLabel('Set DRIP API Key').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('setup_client_id').setLabel('Set DRIP Client ID').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('setup_realm_id').setLabel('Set DRIP Realm ID').setStyle(ButtonStyle.Secondary),
    ),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('setup_currency_id').setLabel('Set Currency ID').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('setup_receipt_channel').setLabel('Set Receipt Channel ID').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('setup_points_label').setLabel('Set Points Label').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('setup_payout_type').setLabel('Set Payout Type').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('setup_payout_amount').setLabel('Set Payout Amount').setStyle(ButtonStyle.Secondary),
    ),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('setup_claim_streak_bonus').setLabel('Set Claim Streak Bonus').setStyle(ButtonStyle.Secondary),
    ),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('setup_verify_drip').setLabel('Verify DRIP Connection').setStyle(ButtonStyle.Success),
      new ButtonBuilder().setCustomId('setup_remove_drip').setLabel('Remove DRIP').setStyle(ButtonStyle.Danger),
      new ButtonBuilder().setCustomId('setup_back_main').setLabel('Back to Main Setup').setStyle(ButtonStyle.Secondary),
    ),
  ];
}

async function getOrCreateSetupChannel(guild) {
  let ch = guild.channels.cache.find(c => c.name === 'holder-verification-admin' && c.type === ChannelType.GuildText);
  if (ch) return ch;
  const me = guild.members.me;
  if (!me?.permissions?.has(PermissionFlagsBits.ManageChannels)) return null;
  try {
    ch = await guild.channels.create({
      name: 'holder-verification-admin',
      type: ChannelType.GuildText,
      permissionOverwrites: [
        { id: guild.roles.everyone.id, deny: [PermissionFlagsBits.ViewChannel] },
        {
          id: guild.members.me.id,
          allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory, PermissionFlagsBits.ManageChannels],
        },
      ],
      reason: 'Verification setup channel',
    });
    return ch;
  } catch (err) {
    if (err?.code === 50013) return null;
    throw err;
  }
}

function buildDripHeaders(settings, includeJson = false) {
  const headers = {
    'Authorization': `Bearer ${settings.drip_api_key}`,
  };
  if (settings.drip_client_id) headers['X-Client-Id'] = String(settings.drip_client_id);
  if (includeJson) headers['Content-Type'] = 'application/json';
  return headers;
}

function dripRealmBaseUrls(realmId) {
  const encoded = encodeURIComponent(realmId);
  return [
    `https://api.drip.re/api/v1/realm/${encoded}`,
    `https://api.drip.re/api/v1/realms/${encoded}`,
  ];
}

async function searchDripMembers(realmId, type, value, settings) {
  const typeCandidates =
    type === 'discord' || type === 'discord-id'
      ? ['discord', 'discord-id']
      : [type];
  const valueParamCandidates = ['value', 'values'];
  const baseUrls = dripRealmBaseUrls(realmId);
  let saw404 = false;
  const errors = [];

  for (const baseUrl of baseUrls) {
    for (const typeCandidate of typeCandidates) {
      for (const valueParam of valueParamCandidates) {
      const url =
        `${baseUrl}` +
        `/members/search?type=${encodeURIComponent(typeCandidate)}&${valueParam}=${encodeURIComponent(value)}`;
      const res = await fetchWithTimeout(url, { timeoutMs: 15000, headers: buildDripHeaders(settings) });
      if (res.ok) {
        const data = await res.json().catch(() => ({}));
        return Array.isArray(data?.data) ? data.data : [];
      }
      if (res.status === 404) {
        saw404 = true;
        continue;
      }
      const body = await res.text().catch(() => '');
      if (res.status === 400 || res.status === 422) {
        errors.push(`path=${baseUrl},type=${typeCandidate},${valueParam}: HTTP ${res.status} ${body}`.slice(0, 260));
        continue;
      }
      throw new Error(`DRIP member search failed: HTTP ${res.status} ${body}`);
    }
    }
  }

  if (errors.length > 0) {
    throw new Error(`DRIP member search failed (all query variants rejected): ${errors.join(' | ')}`);
  }
  if (saw404) return [];
  return [];
}

async function findDripAccountByDiscordId(discordId, settings) {
  const typeCandidates = ['discord', 'discord-id'];
  const valueParamCandidates = ['value', 'values'];
  const errors = [];

  for (const typeCandidate of typeCandidates) {
    for (const valueParam of valueParamCandidates) {
      const url =
        `https://api.drip.re/api/v1/accounts/find` +
        `?type=${encodeURIComponent(typeCandidate)}&${valueParam}=${encodeURIComponent(discordId)}`;
      const res = await fetchWithTimeout(url, { timeoutMs: 15000, headers: buildDripHeaders(settings) });
      if (res.ok) {
        const data = await res.json().catch(() => ({}));
        const out = data?.data;
        if (!out) return null;
        if (typeof out === 'string') return out;
        if (typeof out?.id === 'string') return out.id;
        if (typeof out?.accountId === 'string') return out.accountId;
        if (typeof out?.memberId === 'string') return out.memberId;
        return null;
      }
      if (res.status === 404) return null;
      const body = await res.text().catch(() => '');
      if (res.status === 400 || res.status === 422) {
        errors.push(`type=${typeCandidate},${valueParam}: HTTP ${res.status} ${body}`.slice(0, 220));
        continue;
      }
      throw new Error(`DRIP account lookup failed: HTTP ${res.status} ${body}`);
    }
  }

  if (errors.length > 0) {
    throw new Error(`DRIP account lookup failed (all query variants rejected): ${errors.join(' | ')}`);
  }
  return null;
}

async function resolveDripMemberForDiscordUser(realmId, discordId, walletAddress, settings) {
  const details = [];

  const byDiscord = await searchDripMembers(realmId, 'discord', discordId, settings);
  if (byDiscord[0]?.id) {
    return { member: byDiscord[0], source: 'discord-id', details };
  }
  details.push('discord-id search returned no members');

  if (walletAddress) {
    const byWallet = await searchDripMembers(realmId, 'wallet', walletAddress, settings);
    if (byWallet[0]?.id) {
      return { member: byWallet[0], source: 'wallet', details };
    }
    details.push('wallet search returned no members');
  }

  const accountId = await findDripAccountByDiscordId(discordId, settings);
  if (accountId) {
    const byDripId = await searchDripMembers(realmId, 'drip-id', accountId, settings);
    if (byDripId[0]?.id) {
      return { member: byDripId[0], source: 'drip-id(account)', details };
    }
    details.push('account found but not a member of this realm');
  } else {
    details.push('accounts/find by discord-id returned no account');
  }

  return { member: null, source: null, details };
}

async function findDripMemberByDiscordId(realmId, discordId, settings) {
  const resolved = await resolveDripMemberForDiscordUser(realmId, discordId, null, settings);
  return resolved.member || null;
}

function collectDripMemberIdCandidates(memberLike, fallbackId = null) {
  const ids = [
    fallbackId,
    memberLike?.id,
    memberLike?.realmMemberId,
    memberLike?.memberId,
    memberLike?.dripId,
    memberLike?.accountId,
  ]
    .map(v => (v == null ? '' : String(v).trim()))
    .filter(Boolean);
  return [...new Set(ids)];
}

function sameDripMember(a, b) {
  const left = new Set(collectDripMemberIdCandidates(a));
  const right = new Set(collectDripMemberIdCandidates(b));
  for (const id of left) {
    if (right.has(id)) return true;
  }
  return false;
}

async function verifyWalletViaDrip(realmId, discordId, walletAddress, settings) {
  const out = {
    verified: false,
    dripMemberId: null,
    reason: 'DRIP is not configured.',
  };

  if (!settings?.drip_api_key || !realmId) return out;

  try {
    const [discordMatches, walletMatches] = await Promise.all([
      searchDripMembers(realmId, 'discord', discordId, settings),
      searchDripMembers(realmId, 'wallet', walletAddress, settings),
    ]);

    const discordMember = discordMatches[0] || null;
    const walletMember = walletMatches[0] || null;
    const dripMemberId = discordMember?.id || walletMember?.id || null;
    out.dripMemberId = dripMemberId;

    if (!discordMember) {
      out.reason = 'Your Discord account is not linked to a DRIP member in this realm.';
      return out;
    }
    if (!walletMember) {
      out.reason = 'This wallet is not linked to your DRIP member in this realm.';
      return out;
    }
    if (!sameDripMember(discordMember, walletMember)) {
      out.reason = 'DRIP found your Discord profile and wallet, but they do not match the same member.';
      return out;
    }

    out.verified = true;
    out.reason = 'DRIP confirmed that this wallet matches your profile.';
    return out;
  } catch (err) {
    out.reason = `DRIP verification is temporarily unavailable: ${String(err?.message || err || '').slice(0, 180)}`;
    return out;
  }
}

async function awardDripPoints(realmId, memberIds, tokens, currencyId, settings) {
  const ids = Array.isArray(memberIds) ? memberIds : [memberIds];
  const memberIdCandidates = [...new Set(ids.map(v => (v == null ? '' : String(v).trim())).filter(Boolean))];
  if (!memberIdCandidates.length) {
    throw new Error('DRIP award failed: no member ID candidates provided.');
  }

  const baseUrls = dripRealmBaseUrls(realmId);
  const endpointVariants = [
    {
      suffix: '/point-balance',
      payload: {
        tokens: Number(tokens),
        ...(currencyId ? { realmPointId: String(currencyId) } : {})
      },
    },
    {
      suffix: '/balance',
      payload: {
        amount: Number(tokens),
        ...(currencyId ? { currencyId: String(currencyId) } : {})
      },
    },
  ];

  const notFoundAttempts = [];

  for (const memberId of memberIdCandidates) {
    for (const baseUrl of baseUrls) {
      for (const variant of endpointVariants) {
        const url = `${baseUrl}/members/${encodeURIComponent(memberId)}${variant.suffix}`;
        const res = await fetchWithTimeout(url, {
          timeoutMs: 15000,
          headers: buildDripHeaders(settings, true),
          method: 'PATCH',
          body: JSON.stringify(variant.payload),
        });
        if (res.ok) {
          const data = await res.json().catch(() => ({}));
          return { data, usedMemberId: memberId, endpoint: variant.suffix, baseUrl };
        }
        const body = await res.text().catch(() => '');
        if (res.status === 404) {
          notFoundAttempts.push(`${memberId}@${variant.suffix}: ${String(body || '404').slice(0, 90)}`);
          continue;
        }
        throw new Error(`DRIP award failed: HTTP ${res.status} ${body}`);
      }
    }
  }

  if (notFoundAttempts.length) {
    throw new Error(
      `DRIP award failed: member or endpoint not found (HTTP 404 across all variants). Attempts: ${notFoundAttempts.join(' | ')}`
    );
  }
  throw new Error('DRIP award failed: no endpoint accepted the request.');
}

async function verifyDripConnection(settings, discordProbeId) {
  const missing = [];
  if (!settings?.drip_api_key) missing.push('DRIP API Key');
  if (!settings?.drip_realm_id) missing.push('DRIP Realm ID');
  if (!settings?.currency_id) missing.push('Currency ID');
  if (missing.length > 0) {
    return { ok: false, reason: `Missing required settings: ${missing.join(', ')}` };
  }

  const configuredCurrency = String(settings.currency_id).trim();
  const baseUrls = dripRealmBaseUrls(settings.drip_realm_id);
  const pointsUrls = [];
  for (const baseUrl of baseUrls) {
    pointsUrls.push(`${baseUrl}/points`);
    pointsUrls.push(`${baseUrl}/point`);
    pointsUrls.push(`${baseUrl}/currencies`);
    pointsUrls.push(`${baseUrl}/currency`);
    pointsUrls.push(`${baseUrl}/points/${encodeURIComponent(configuredCurrency)}`);
    pointsUrls.push(`${baseUrl}/point/${encodeURIComponent(configuredCurrency)}`);
  }

  const seen = new Set();
  const attempts = [];
  let points = [];

  const extractPoints = (payload) => {
    if (Array.isArray(payload?.data)) return payload.data;
    if (Array.isArray(payload?.points)) return payload.points;
    if (Array.isArray(payload?.currencies)) return payload.currencies;
    if (Array.isArray(payload?.data?.points)) return payload.data.points;
    if (Array.isArray(payload?.data?.currencies)) return payload.data.currencies;
    if (payload?.data && typeof payload.data === 'object' && (payload.data.id || payload.data.name)) return [payload.data];
    if (payload && typeof payload === 'object' && (payload.id || payload.name)) return [payload];
    return [];
  };

  for (const pointsUrl of pointsUrls) {
    if (seen.has(pointsUrl)) continue;
    seen.add(pointsUrl);
    const pointsRes = await fetchWithTimeout(pointsUrl, { timeoutMs: 15000, headers: buildDripHeaders(settings) });
    if (pointsRes.ok) {
      const payload = await pointsRes.json().catch(() => ({}));
      const extracted = extractPoints(payload);
      if (extracted.length > 0) {
        points = extracted;
        break;
      }
      attempts.push(`${pointsRes.status} ${pointsUrl} (no point/currency data)`);
      continue;
    }
    const body = await pointsRes.text().catch(() => '');
    attempts.push(`${pointsRes.status} ${pointsUrl} ${String(body || '').slice(0, 120)}`.trim());
  }

  if (!points.length) {
    return {
      ok: false,
      reason: `Points endpoint failed. Tried: ${attempts.slice(0, 4).join(' | ') || 'no endpoints reached'}`.slice(0, 500)
    };
  }

  const matchedCurrency = points.find((p) => {
    const candidates = [p?.id, p?.currencyId, p?.realmPointId]
      .map((v) => (v == null ? '' : String(v).trim()))
      .filter(Boolean);
    return candidates.includes(configuredCurrency);
  });
  if (!matchedCurrency) {
    return {
      ok: false,
      reason: `Configured Currency ID not found in realm points (${configuredCurrency}).`
    };
  }

  let memberProbe = null;
  try {
    const probeResults = await searchDripMembers(settings.drip_realm_id, 'discord', discordProbeId, settings);
    memberProbe = {
      ok: true,
      count: probeResults.length
    };
  } catch (err) {
    memberProbe = {
      ok: false,
      error: String(err?.message || err || '').slice(0, 260)
    };
  }

  return {
    ok: true,
    pointsCount: points.length,
    currencyName: matchedCurrency?.name || null,
    currencyEmoji: matchedCurrency?.emoji || '',
    memberProbe
  };
}

async function respondInteraction(interaction, payload) {
  if (interaction.deferred) {
    const out = { ...(payload || {}) };
    if (Object.prototype.hasOwnProperty.call(out, 'flags')) delete out.flags;
    return interaction.editReply(out);
  }
  if (interaction.replied) return interaction.followUp(payload);
  return interaction.reply(payload);
}

async function handleClaim(interaction) {
  const guildId = interaction.guild.id;
  const links = await getWalletLinks(guildId, interaction.user.id);
  const walletAddresses = links.map((x) => x.wallet_address).filter(Boolean);
  if (!walletAddresses.length) {
    await respondInteraction(interaction, { content: 'Connect your wallet first.', flags: 64 });
    return;
  }
  if (await hasClaimedToday(guildId, interaction.user.id)) {
    await respondInteraction(interaction, { content: 'You already claimed today. Try again after UTC midnight.', flags: 64 });
    return;
  }

  const settings = (await getGuildSettings(guildId)) || { payout_type: 'per_up', payout_amount: 1, claim_streak_bonus: 0 };
  const pointsLabel = getPointsLabel(settings);
  const payoutType = settings.payout_type === 'per_nft' ? 'per_nft' : 'per_up';
  const claimStreakBonus = Number(settings.claim_streak_bonus || 0);
  const missing = [];
  if (!settings?.drip_api_key) missing.push('DRIP API Key');
  if (!settings?.drip_realm_id) missing.push('DRIP Realm ID');
  if (!settings?.currency_id) missing.push('Currency ID');
  if (missing.length > 0) {
    await respondInteraction(interaction, {
      content: `Claim is not configured yet. Missing: ${missing.join(', ')}.`,
      flags: 64
    });
    return;
  }
  const rewardQuote = await computeDailyRewardQuote(guildId, links, settings);
  const { verifiedStats, unverifiedStats, unverifiedPenaltyAmount } = rewardQuote;
  const stats = {
    unitTotal: verifiedStats.unitTotal + unverifiedStats.unitTotal,
    totalNfts: rewardQuote.totalNfts,
    totalUp: rewardQuote.totalUp,
  };
  const streakBeforeToday = await getClaimStreakBeforeToday(guildId, interaction.user.id);
  const streakAfterClaim = streakBeforeToday + 1;
  const streakBonusApplied = streakBeforeToday >= 1 ? claimStreakBonus : 0;
  const baseAmount = rewardQuote.dailyReward;
  const amount = Math.max(0, Math.floor(baseAmount + streakBonusApplied));

  if (amount <= 0) {
    await respondInteraction(interaction, { content: 'No payout available. Check your holdings or payout settings.', flags: 64 });
    return;
  }

  try {
    let resolved = null;
    let dripMemberId = links[0]?.drip_member_id || null;
    try {
      resolved = await resolveDripMemberForDiscordUser(
        settings.drip_realm_id,
        interaction.user.id,
        walletAddresses[0],
        settings
      );
      const resolvedPrimary = resolved?.member?.id || null;
      if (resolvedPrimary && resolvedPrimary !== dripMemberId) {
        dripMemberId = resolvedPrimary;
        for (const row of links) {
          await setWalletLink(guildId, interaction.user.id, row.wallet_address, Boolean(row.verified), dripMemberId);
        }
      }
    } catch (resolveErr) {
      if (!dripMemberId) throw resolveErr;
    }

    const dripMemberIdCandidates = collectDripMemberIdCandidates(resolved?.member, dripMemberId);
    if (!dripMemberIdCandidates.length) {
      await respondInteraction(interaction, {
        content:
          'Claim unavailable: your DRIP profile is not linked in this realm yet.\n' +
          'Please ask an admin to verify your realm/member setup.',
        flags: 64
      });
      return;
    }

    const awardResult = await awardDripPoints(
      settings.drip_realm_id,
      dripMemberIdCandidates,
      amount,
      settings.currency_id,
      settings
    );
    dripMemberId = awardResult?.usedMemberId || dripMemberIdCandidates[0];
    for (const row of links) {
      await setWalletLink(guildId, interaction.user.id, row.wallet_address, Boolean(row.verified), dripMemberId);
    }

    const receiptChannelId = settings?.receipt_channel_id || RECEIPT_CHANNEL_ID;
    let receiptChannel = null;
    let receiptMessage = null;
    let receiptWarning = '';
    try {
      receiptChannel = await interaction.guild.channels.fetch(receiptChannelId).catch(() => null);
      if (receiptChannel?.isTextBased()) {
      const earningBasis =
        payoutType === 'per_nft'
          ? `${stats.totalNfts} eligible NFT${stats.totalNfts === 1 ? '' : 's'}`
          : `${stats.totalUp} ${pointsLabel}`;
        const streakLine = streakBonusApplied > 0
          ? `\nStreak: ${streakAfterClaim} day${streakAfterClaim === 1 ? '' : 's'} (+${streakBonusApplied} $CHARM bonus)`
          : `\nStreak: ${streakAfterClaim} day${streakAfterClaim === 1 ? '' : 's'}`;
        const verificationPenaltyLine = unverifiedPenaltyAmount > 0
          ? `\nUnverified wallet dock: -${Math.floor(unverifiedPenaltyAmount)} $CHARM (50%)`
          : '';
        receiptMessage = await receiptChannel.send(
          `🧾 Claim Receipt\n` +
          `User: <@${interaction.user.id}>\n` +
          `Earning Basis: ${earningBasis}\n` +
          `Base Reward: **${baseAmount} $CHARM**` +
          `${verificationPenaltyLine}` +
          `${streakLine}\n` +
          `Reward: **${amount} $CHARM**`
        );
      } else {
        receiptWarning = '\n(Receipt channel unavailable or not text-based.)';
      }
    } catch (receiptErr) {
      const rmsg = String(receiptErr?.message || receiptErr || '');
      console.warn(`⚠️ Claim receipt send failed for guild ${guildId}:`, rmsg);
      receiptWarning = '\n(Claim receipt could not be posted to the configured channel.)';
    }

    await holdersPool.query(
      `INSERT INTO claims (guild_id, discord_id, claim_day, amount, wallet_address, receipt_channel_id, receipt_message_id)
       VALUES ($1, $2, CURRENT_DATE, $3, $4, $5, $6)`,
      [guildId, interaction.user.id, amount, walletAddresses.join(','), receiptChannel?.id || null, receiptMessage?.id || null]
    );

    const streakSummary = streakBonusApplied > 0
      ? ` Streak: **${streakAfterClaim}** days (+${streakBonusApplied} $CHARM bonus).`
      : ` Streak: **${streakAfterClaim}** day${streakAfterClaim === 1 ? '' : 's'}.`;
    const verificationSummary = unverifiedPenaltyAmount > 0
      ? ` You were docked **50%** on unverified wallet rewards. Verify your wallet in DRIP or open a ticket in <#${SUPPORT_TICKET_CHANNEL_ID}> for help.`
      : '';
    await respondInteraction(interaction, { content: `Claim complete. You received **${amount} $CHARM**.${streakSummary}${verificationSummary}${receiptWarning}`, flags: 64 });
  } catch (err) {
    console.error('Claim processing error:', err);
    const msg = String(err?.message || err || '').trim();
    if (/DRIP member search failed|DRIP award failed/i.test(msg)) {
      await postAdminSystemLog({
        guild: interaction.guild,
        category: 'DRIP Failure',
        message:
          `User: <@${interaction.user.id}>\n` +
          `Context: claim\n` +
          `Reason: ${msg.slice(0, 500)}`
      });
    } else if (/claims|wallet_links|database|relation|column/i.test(msg)) {
      await postAdminSystemLog({
        guild: interaction.guild,
        category: 'Wallet Link Issue',
        message:
          `User: <@${interaction.user.id}>\n` +
          `Context: claim record update\n` +
          `Reason: ${msg.slice(0, 500)}`
      });
    }
    let reason = 'We could not process your claim right now.';
    if (/DRIP member search failed/i.test(msg)) reason = 'We could not verify your DRIP profile right now.';
    else if (/DRIP award failed/i.test(msg)) reason = 'The DRIP transfer did not complete. Please try again in a moment.';
    else if (/claims|wallet_links|database|relation|column/i.test(msg)) reason = 'Your claim could not be recorded due to a storage issue.';
    await respondInteraction(interaction, {
      content: `Claim failed: ${reason}`,
      flags: 64
    });
  }
}

async function deleteAllWalletLinks(guildId, discordId) {
  const deletedWallet = await holdersPool.query(
    `DELETE FROM wallet_links WHERE guild_id = $1 AND discord_id = $2`,
    [guildId, discordId]
  );
  return { wallets: deletedWallet.rowCount || 0 };
}

async function removeHolderRolesFromMember(member) {
  const rules = await getHolderRules(member.guild.id);
  if (!rules.length) return 0;
  let removed = 0;
  for (const r of rules) {
    const role = member.guild.roles.cache.get(r.role_id);
    if (!role) continue;
    if (!member.roles.cache.has(role.id)) continue;
    if (!role.editable) continue;
    try {
      await member.roles.remove(role, 'User disconnected verification data');
      removed++;
    } catch {}
  }
  return removed;
}

client.on('interactionCreate', async (interaction) => {
  try {
    if (interaction.isChatInputCommand()) {
      if (interaction.commandName === 'launch-verification') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const msg = await interaction.channel.send({
          embeds: [verificationMenuEmbed(interaction.guild.name)],
          components: [verificationButtons()],
        });
        await holdersPool.query(
          `INSERT INTO verification_panels (guild_id, channel_id, message_id, created_by) VALUES ($1, $2, $3, $4)`,
          [interaction.guild.id, interaction.channel.id, msg.id, interaction.user.id]
        );
        await interaction.reply({ content: 'Verification menu launched.', flags: 64 });
        return;
      }

      if (interaction.commandName === 'launch-rewards') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const settings = await getGuildSettings(interaction.guild.id);
        const pointsLabel = getPointsLabel(settings);
        await interaction.channel.send({
          embeds: [rewardsMenuEmbed(interaction.guild.name, pointsLabel)],
          components: [rewardsButtons()],
        });
        await interaction.reply({ content: 'Rewards menu launched.', flags: 64 });
        return;
      }

      if (interaction.commandName === 'setup-verification') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.channel.send({
          embeds: [setupMainEmbed()],
          components: setupMainButtons(),
        });
        await interaction.reply({ content: `Setup panel posted in <#${interaction.channel.id}>`, flags: 64 });
        return;
      }

      if (interaction.commandName === 'set-points-mapping') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });

        const collectionInput = String(interaction.options.getString('collection', true) || '').trim();
        const mode = String(interaction.options.getString('mode', false) || 'merge').toLowerCase();
        const replaceMode = mode === 'replace';
        const file = interaction.options.getAttachment('csv_file', true);
        if (!file?.url) {
          await interaction.editReply({ content: 'CSV attachment is required.' });
          return;
        }
        if (!/\.csv($|\?)/i.test(file.name || '') && !/text\/csv|application\/vnd\.ms-excel/i.test(file.contentType || '')) {
          await interaction.editReply({ content: `Attachment must be a CSV file. Got: ${file.name || 'unknown file'}` });
          return;
        }

        const collections = await getHolderCollections(interaction.guild.id);
        const normalizedInputAddr = normalizeEthAddress(collectionInput);
        const selected = collections.find((c) => {
          const byAddress = normalizedInputAddr && String(c.contract_address).toLowerCase() === normalizedInputAddr;
          const byName = String(c.name || '').trim().toLowerCase() === collectionInput.toLowerCase();
          return byAddress || byName;
        });
        if (!selected) {
          await interaction.editReply({
            content:
              `Collection not found: \`${collectionInput}\`.\n` +
              `Use an existing collection name/address from setup, or add one via **Add Collection** first.`
          });
          return;
        }

        let csvInput = '';
        try {
          const res = await fetchWithRetry(file.url, 2, 700, {});
          csvInput = await res.text();
        } catch (err) {
          await interaction.editReply({
            content: `Could not read attached CSV: ${String(err?.message || err || 'unknown error').slice(0, 180)}`
          });
          return;
        }

        try {
          const parsed = parsePointsMappingCsv(csvInput);
          const existingMappings = await getGuildPointMappings(interaction.guild.id);
          const existingTable = existingMappings.get(String(selected.contract_address).toLowerCase()) || {};
          const merged = replaceMode
            ? { table: parsed.table, addedTraits: parsed.rowCount, updatedTraits: 0, totalCategories: parsed.categoryCount }
            : mergePointsMappingTables(existingTable, parsed.table);
          await setGuildPointMapping(interaction.guild.id, selected.contract_address, merged.table, interaction.user.id);
          const updatedLine = replaceMode ? '' : `Traits updated: ${merged.updatedTraits}\n`;
          await interaction.editReply({
            content:
              `Points mapping ${replaceMode ? 'replaced' : 'merged'} for **${selected.name}** (\`${selected.contract_address}\`).\n` +
              `Rows imported: ${parsed.rowCount}\n` +
              `${replaceMode ? 'Categories' : 'Total categories'}: ${merged.totalCategories}\n` +
              `${replaceMode ? 'Traits set' : 'Traits added'}: ${merged.addedTraits}\n` +
              `${updatedLine}` +
              `Delimiter detected: \`${parsed.delimiter}\``
          });
        } catch (err) {
          await interaction.editReply({
            content:
              `Invalid mapping format: ${String(err?.message || err || '').slice(0, 220)}\n` +
              `Required format:\n` +
              `\`category,trait,ugly_points\`\n` +
              `\`Background,Blue,250\`\n` +
              `or\n` +
              `\`category|trait|points\`\n` +
              `\`Background|Blue|250\``
          });
        }
        return;
      }

      if (interaction.commandName === 'remove-points-mapping') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });

        const collectionInput = String(interaction.options.getString('collection', true) || '').trim();
        const collections = await getHolderCollections(interaction.guild.id);
        const mappings = await getGuildPointMappingsWithOwners(interaction.guild.id);
        const normalizedInputAddr = normalizeEthAddress(collectionInput);
        const selectedByName = collections.find((c) => {
          const byAddress = normalizedInputAddr && String(c.contract_address).toLowerCase() === normalizedInputAddr;
          const byName = String(c.name || '').trim().toLowerCase() === collectionInput.toLowerCase();
          return byAddress || byName;
        });
        const selectedContract = normalizedInputAddr && mappings.find((m) => String(m.contractAddress).toLowerCase() === normalizedInputAddr)
          ? normalizedInputAddr
          : (selectedByName ? String(selectedByName.contract_address).toLowerCase() : null);
        if (!selectedContract) {
          await interaction.editReply({
            content:
              `No points mapping found for: \`${collectionInput}\`.\n` +
              `Use a mapped contract address or a collection name with an existing mapping.`
          });
          return;
        }

        const removed = await removeGuildPointMapping(interaction.guild.id, selectedContract, interaction.user.id);
        if (!removed.ok) {
          if (removed.reason === 'not_found') {
            await interaction.editReply({ content: 'No points mapping exists for that collection.' });
            return;
          }
          if (removed.reason === 'forbidden') {
            const ownerText = removed.ownerId
              ? `Only <@${removed.ownerId}> or DEFAULT_ADMIN_USER can remove it.`
              : 'Only DEFAULT_ADMIN_USER can remove this legacy mapping.';
            await interaction.editReply({
              content: `You cannot remove this mapping. ${ownerText}`
            });
            return;
          }
          await interaction.editReply({ content: 'Could not remove points mapping.' });
          return;
        }

        await interaction.editReply({
          content: `Points mapping removed for **${selectedByName?.name || labelForContract(selectedContract)}** (\`${selectedContract}\`).`
        });
        return;
      }

      if (interaction.commandName === 'connectuser') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });

        const discordId = String(interaction.options.getString('discord_id', true) || '').trim();
        const dripUserId = String(interaction.options.getString('drip_user_id', true) || '').trim();
        const walletInput = String(interaction.options.getString('wallet', true) || '').trim();
        const walletAddress = normalizeEthAddress(walletInput);

        if (!/^\d{16,20}$/.test(discordId)) {
          await interaction.editReply({ content: 'Invalid Discord ID.' });
          return;
        }
        if (!walletAddress) {
          await interaction.editReply({ content: 'Invalid wallet address.' });
          return;
        }
        if (!dripUserId) {
          await interaction.editReply({ content: 'DRIP user ID is required.' });
          return;
        }

        await reassignWalletLink(interaction.guild.id, discordId, walletAddress, true, dripUserId);
        await postAdminSystemLog({
          guild: interaction.guild,
          category: 'Admin Override',
          message:
            `Actor: <@${interaction.user.id}>\n` +
            `Action: /connectuser\n` +
            `Target: <@${discordId}>\n` +
            `Wallet: \`${walletAddress}\`\n` +
            `DRIP User ID: \`${dripUserId}\``
        });

        let syncSummary = 'Role sync skipped (member not found in this server).';
        try {
          const member = await interaction.guild.members.fetch(discordId);
          const links = await getWalletLinks(interaction.guild.id, discordId);
          const allAddresses = links.map((x) => x.wallet_address).filter(Boolean);
          const sync = await syncHolderRoles(member, allAddresses);
          await postRoleSyncFailures(interaction.guild, discordId, sync, 'admin /connectuser');
          syncSummary =
            `Role sync complete (${sync.changed} change${sync.changed === 1 ? '' : 's'}). ` +
            `${sync.granted?.length ? `Roles granted: ${sync.granted.join(', ')}` : 'Roles granted: none'}`;
        } catch (err) {
          await postAdminSystemLog({
            guild: interaction.guild,
            category: 'Role Sync Failure',
            message:
              `User: <@${discordId}>\n` +
              `Context: admin /connectuser\n` +
              `Reason: ${String(err?.message || err || '').slice(0, 500)}`
          });
        }

        await interaction.editReply({
          content:
            `Manual wallet override saved.\n` +
            `Discord ID: \`${discordId}\`\n` +
            `Wallet: \`${walletAddress}\`\n` +
            `DRIP User ID: \`${dripUserId}\`\n` +
            `Status: DRIP verified (manual override).\n` +
            `${syncSummary}`
        });
        return;
      }

      if (interaction.commandName === 'disconnectuser') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });

        const discordId = String(interaction.options.getString('discord_id', true) || '').trim();
        const walletInput = String(interaction.options.getString('wallet', true) || '').trim();
        const walletAddress = normalizeEthAddress(walletInput);

        if (!/^\d{16,20}$/.test(discordId)) {
          await interaction.editReply({ content: 'Invalid Discord ID.' });
          return;
        }
        if (!walletAddress) {
          await interaction.editReply({ content: 'Invalid wallet address.' });
          return;
        }

        const removed = await deleteWalletLink(interaction.guild.id, discordId, walletAddress);
        if (!removed) {
          await interaction.editReply({
            content:
              `No linked wallet found for that user.\n` +
              `Discord ID: \`${discordId}\`\n` +
              `Wallet: \`${walletAddress}\``
          });
          return;
        }
        await postAdminSystemLog({
          guild: interaction.guild,
          category: 'Admin Override',
          message:
            `Actor: <@${interaction.user.id}>\n` +
            `Action: /disconnectuser\n` +
            `Target: <@${discordId}>\n` +
            `Wallet: \`${walletAddress}\``
        });

        let syncSummary = 'Role sync skipped (member not found in this server).';
        try {
          const member = await interaction.guild.members.fetch(discordId);
          const links = await getWalletLinks(interaction.guild.id, discordId);
          const remainingAddresses = links.map((x) => x.wallet_address).filter(Boolean);
          const sync = await syncHolderRoles(member, remainingAddresses);
          await postRoleSyncFailures(interaction.guild, discordId, sync, 'admin /disconnectuser');
          syncSummary =
            `Role sync complete (${sync.changed} change${sync.changed === 1 ? '' : 's'}). ` +
            `${sync.granted?.length ? `Roles granted: ${sync.granted.join(', ')}` : 'Roles granted: none'}`;
        } catch (err) {
          await postAdminSystemLog({
            guild: interaction.guild,
            category: 'Role Sync Failure',
            message:
              `User: <@${discordId}>\n` +
              `Context: admin /disconnectuser\n` +
              `Reason: ${String(err?.message || err || '').slice(0, 500)}`
          });
        }

        await interaction.editReply({
          content:
            `Manual wallet disconnect complete.\n` +
            `Discord ID: \`${discordId}\`\n` +
            `Wallet: \`${walletAddress}\`\n` +
            `${syncSummary}`
        });
        return;
      }

      if (interaction.commandName === 'listuserwallets') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });
        const discordId = String(interaction.options.getString('discord_id', true) || '').trim();
        if (!/^\d{16,20}$/.test(discordId)) {
          await interaction.editReply({ content: 'Invalid Discord ID.' });
          return;
        }
        const links = await getWalletLinks(interaction.guild.id, discordId);
        const claimedToday = await hasClaimedToday(interaction.guild.id, discordId);
        const streak = await getClaimStreakBeforeToday(interaction.guild.id, discordId);
        if (!links.length) {
          await interaction.editReply({
            content:
              `No linked wallets found.\n` +
              `Discord ID: \`${discordId}\`\n` +
              `Claimed today: ${claimedToday ? 'yes' : 'no'}\n` +
              `Current streak if they claim today: ${streak + 1}`
          });
          return;
        }
        const lines = links.map((w, i) =>
          `${i + 1}. \`${w.wallet_address}\` | ${w.verified ? 'verified' : 'unverified'} | DRIP: \`${w.drip_member_id || 'none'}\``
        );
        await interaction.editReply({
          content:
            `Wallet status for <@${discordId}>:\n` +
            `Claimed today: ${claimedToday ? 'yes' : 'no'}\n` +
            `Current streak if they claim today: ${streak + 1}\n` +
            `${lines.join('\n')}`
        });
        return;
      }

      if (interaction.commandName === 'healthcheck') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });
        const settings = await getGuildSettings(interaction.guild.id);
        const holderRules = await getHolderRules(interaction.guild.id);
        const traitRules = await getTraitRoleRules(interaction.guild.id);
        const me = interaction.guild.members.me;
        const canManageRoles = Boolean(me?.permissions?.has(PermissionFlagsBits.ManageRoles));
        const receiptChannelId = settings?.receipt_channel_id || RECEIPT_CHANNEL_ID;
        const receiptChannel = await interaction.guild.channels.fetch(receiptChannelId).catch(() => null);
        const adminLogChannel = await interaction.guild.channels.fetch(ADMIN_LOG_CHANNEL_ID).catch(() => null);
        const missingRoles = [];
        const blockedRoles = [];
        for (const r of [...holderRules, ...traitRules]) {
          const role = interaction.guild.roles.cache.get(r.role_id);
          if (!role) {
            missingRoles.push(r.role_name || r.role_id);
            continue;
          }
          if (!role.editable) blockedRoles.push(role.name);
        }
        const dripReady = Boolean(settings?.drip_api_key && settings?.drip_realm_id && settings?.currency_id);
        await interaction.editReply({
          content:
            `Health Check\n` +
            `- DRIP configured: ${dripReady ? 'yes' : 'no'}\n` +
            `- Receipt channel reachable: ${receiptChannel?.isTextBased() ? 'yes' : 'no'} (\`${receiptChannelId}\`)\n` +
            `- Admin log channel reachable: ${adminLogChannel?.isTextBased() ? 'yes' : 'no'} (\`${ADMIN_LOG_CHANNEL_ID}\`)\n` +
            `- Bot can manage roles: ${canManageRoles ? 'yes' : 'no'}\n` +
            `- Holder rules: ${holderRules.length}\n` +
            `- Trait rules: ${traitRules.length}\n` +
            `- Missing roles: ${missingRoles.length ? missingRoles.join(', ') : 'none'}\n` +
            `- Unmanageable roles: ${blockedRoles.length ? blockedRoles.join(', ') : 'none'}`
        });
        return;
      }

      if (interaction.commandName === 'info') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const settings = await getGuildSettings(interaction.guild.id);
        await interaction.reply({
          embeds: infoEmbeds(interaction.guild.name, settings),
          flags: 64
        });
        return;
      }

      if (interaction.commandName === 'info-user') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const settings = await getGuildSettings(interaction.guild.id);
        await interaction.reply({
          embeds: infoUserEmbeds(interaction.guild.name, settings)
        });
        return;
      }
      return;
    }

    if (interaction.isButton()) {
      if (interaction.customId === 'verify_connect') {
        const modal = new ModalBuilder().setCustomId('verify_connect_modal').setTitle('Connect Wallet');
        modal.addComponents(
          new ActionRowBuilder().addComponents(
            new TextInputBuilder()
              .setCustomId('wallet_address')
              .setLabel('Ethereum wallet address(es)')
              .setRequired(true)
              .setPlaceholder('0x..., 0x... (multiple allowed)')
              .setStyle(TextInputStyle.Short)
          )
        );
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === 'verify_claim') {
        await interaction.deferReply({ flags: 64 });
        await handleClaim(interaction);
        return;
      }

      if (interaction.customId === 'verify_check_stats') {
        const collections = await getHolderCollections(interaction.guild.id);
        if (!collections.length) {
          await interaction.reply({ content: 'No collections configured yet.', flags: 64 });
          return;
        }
        const options = collections.slice(0, 25).map((c) => ({
          label: String(c.name).slice(0, 100),
          value: c.contract_address,
          description: String(c.contract_address).slice(0, 100),
        }));
        const row = new ActionRowBuilder().addComponents(
          new StringSelectMenuBuilder()
            .setCustomId('verify_check_stats_collection_select')
            .setPlaceholder('Select collection to check')
            .addOptions(options)
        );
        await interaction.reply({
          content: 'Select a collection, then you will enter token ID.',
          components: [row],
          flags: 64
        });
        return;
      }

      if (interaction.customId === 'verify_disconnect') {
        const modal = new ModalBuilder().setCustomId('verify_disconnect_modal').setTitle('Disconnect Wallet');
        modal.addComponents(
          new ActionRowBuilder().addComponents(
            new TextInputBuilder()
              .setCustomId('wallet_address')
              .setLabel('Wallet address or ALL')
              .setRequired(true)
              .setPlaceholder('0x... or ALL')
              .setStyle(TextInputStyle.Short)
          )
        );
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === 'verify_wallets') {
        const links = await getWalletLinks(interaction.guild.id, interaction.user.id);
        if (!links.length) {
          await interaction.reply({ content: 'No wallets connected yet.', flags: 64 });
          return;
        }
        const lines = links.map((w, i) => `${i + 1}. \`${w.wallet_address}\` - ${w.verified ? 'DRIP verified' : 'DRIP verification pending'} - https://etherscan.io/address/${w.wallet_address}`);
        await interaction.reply({
          content: `Connected wallet(s):\n${lines.join('\n')}`,
          flags: 64
        });
        return;
      }

      if (interaction.customId === 'rewards_view_holdings') {
        const settings = await getGuildSettings(interaction.guild.id);
        const pointsLabel = getPointsLabel(settings);
        const links = await getWalletLinks(interaction.guild.id, interaction.user.id);
        const walletAddresses = links.map((x) => x.wallet_address).filter(Boolean);
        if (!walletAddresses.length) {
          await interaction.reply({ content: 'No wallets connected yet.', flags: 64 });
          return;
        }

        const [collectionCounts, rewardQuote] = await Promise.all([
          getConnectedCollectionCounts(interaction.guild.id, walletAddresses),
          computeDailyRewardQuote(interaction.guild.id, links, settings || {}),
        ]);

        let dripBalanceText = 'Unavailable';
        try {
          const resolved = await resolveDripMemberForDiscordUser(
            settings?.drip_realm_id,
            interaction.user.id,
            walletAddresses[0],
            settings || {}
          ).catch(() => ({ member: null }));
          const dripMemberIdCandidates = collectDripMemberIdCandidates(
            resolved?.member,
            links.find((x) => x.drip_member_id)?.drip_member_id || null
          );
          const dripBalance = await getDripMemberCurrencyBalance(
            settings?.drip_realm_id,
            dripMemberIdCandidates,
            settings?.currency_id,
            settings || {}
          );
          if (dripBalance != null) dripBalanceText = String(Math.floor(Number(dripBalance) || 0));
        } catch {}

        const collectionLines = collectionCounts.length
          ? collectionCounts.map((x) => `• ${x.name}: **${x.count}** NFT${x.count === 1 ? '' : 's'}`).join('\n')
          : '• No connected collections found.';
        const penaltyLine = rewardQuote.unverifiedPenaltyAmount > 0
          ? `\nUnverified wallet dock: **-${Math.floor(rewardQuote.unverifiedPenaltyAmount)} $CHARM**`
          : '';

        const embed = new EmbedBuilder()
          .setTitle(`User's Holdings`)
          .setColor(0xB0DEEE)
          .setThumbnail(interaction.user.displayAvatarURL({ size: 256 }))
          .setDescription(
            `Collections connected to this server across all linked wallets:\n${collectionLines}\n\n` +
            `Daily earnings: **${rewardQuote.dailyReward} $CHARM**${penaltyLine}\n` +
            `Total ${pointsLabel}: **${rewardQuote.totalUp}**\n` +
            `Total NFTs: **${rewardQuote.totalNfts}**\n` +
            `DRIP $CHARM held: **${dripBalanceText}**`
          );

        await interaction.reply({
          embeds: [embed],
          flags: 64
        });
        return;
      }

      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }

      if (interaction.customId === 'setup_drip_menu') {
        await interaction.update({
          embeds: [setupDripEmbed()],
          components: setupDripButtons(),
        });
        return;
      }

      if (interaction.customId === 'setup_back_main') {
        await interaction.update({
          embeds: [setupMainEmbed()],
          components: setupMainButtons(),
        });
        return;
      }

      if (interaction.customId === 'setup_add_rule') {
        const collections = await getHolderCollections(interaction.guild.id);
        if (!collections.length) {
          const addBtnRow = new ActionRowBuilder().addComponents(
            new ButtonBuilder().setCustomId('setup_add_collection_from_rule').setLabel('Add Collection').setStyle(ButtonStyle.Secondary),
          );
          await interaction.reply({
            content: 'No collections found. Add one to continue.',
            components: [addBtnRow],
            flags: 64
          });
          return;
        }
        const key = `${interaction.guild.id}:${interaction.user.id}`;
        globalThis.__PENDING_HOLDER_RULES.set(key, { contractAddress: null, collectionName: null, minTokens: null, maxTokens: null, createdAt: Date.now() });
        const options = collections.slice(0, 25).map((c) => ({
          label: String(c.name).slice(0, 100),
          value: c.contract_address,
          description: String(c.contract_address).slice(0, 100),
        }));
        const row = new ActionRowBuilder().addComponents(
          new StringSelectMenuBuilder()
            .setCustomId('setup_add_rule_collection_select')
            .setPlaceholder('Select collection')
            .addOptions(options)
        );
        const addBtnRow = new ActionRowBuilder().addComponents(
          new ButtonBuilder().setCustomId('setup_add_collection_from_rule').setLabel('Add Collection').setStyle(ButtonStyle.Secondary),
        );
        await interaction.reply({
          content: 'Select collection for holder role rule. If not listed, add it below.',
          components: [row, addBtnRow],
          flags: 64
        });
        return;
      }

      if (interaction.customId === 'setup_add_trait_rule') {
        const collections = await getHolderCollections(interaction.guild.id);
        if (!collections.length) {
          const addBtnRow = new ActionRowBuilder().addComponents(
            new ButtonBuilder().setCustomId('setup_add_collection_from_trait_rule').setLabel('Add Collection').setStyle(ButtonStyle.Secondary),
          );
          await interaction.reply({
            content: 'No collections found. Add one to continue.',
            components: [addBtnRow],
            flags: 64
          });
          return;
        }
        const key = `${interaction.guild.id}:${interaction.user.id}`;
        globalThis.__PENDING_TRAIT_ROLE_RULES.set(key, {
          contractAddress: null,
          collectionName: null,
          traitCategory: null,
          traitValue: null,
          createdAt: Date.now()
        });
        const options = collections.slice(0, 25).map((c) => ({
          label: String(c.name).slice(0, 100),
          value: c.contract_address,
          description: String(c.contract_address).slice(0, 100),
        }));
        const row = new ActionRowBuilder().addComponents(
          new StringSelectMenuBuilder()
            .setCustomId('setup_add_trait_rule_collection_select')
            .setPlaceholder('Select collection')
            .addOptions(options)
        );
        const addBtnRow = new ActionRowBuilder().addComponents(
          new ButtonBuilder().setCustomId('setup_add_collection_from_trait_rule').setLabel('Add Collection').setStyle(ButtonStyle.Secondary),
        );
        await interaction.reply({
          content: 'Select collection for trait role rule. If not listed, add it below.',
          components: [row, addBtnRow],
          flags: 64
        });
        return;
      }

      if (interaction.customId === 'setup_add_collection' || interaction.customId === 'setup_add_collection_from_rule' || interaction.customId === 'setup_add_collection_from_trait_rule') {
        const modal = new ModalBuilder().setCustomId('setup_add_collection_modal').setTitle('Add Collection');
        modal.addComponents(
          new ActionRowBuilder().addComponents(
            new TextInputBuilder().setCustomId('collection_name').setLabel('Collection name').setRequired(true).setStyle(TextInputStyle.Short)
          ),
          new ActionRowBuilder().addComponents(
            new TextInputBuilder().setCustomId('contract_address').setLabel('Contract address').setRequired(true).setStyle(TextInputStyle.Short)
          ),
        );
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === 'setup_points_mapping') {
        const collections = await getHolderCollections(interaction.guild.id);
        if (!collections.length) {
          const addBtnRow = new ActionRowBuilder().addComponents(
            new ButtonBuilder().setCustomId('setup_add_collection_from_points').setLabel('Add Collection').setStyle(ButtonStyle.Secondary),
          );
          await interaction.reply({
            content: 'No collections found. Add one first, then configure points mapping.',
            components: [addBtnRow],
            flags: 64
          });
          return;
        }
        const options = collections.slice(0, 25).map((c) => ({
          label: String(c.name).slice(0, 100),
          value: c.contract_address,
          description: String(c.contract_address).slice(0, 100),
        }));
        const row = new ActionRowBuilder().addComponents(
          new StringSelectMenuBuilder()
            .setCustomId('setup_points_mapping_collection_select')
            .setPlaceholder('Select collection to map points')
            .addOptions(options)
        );
        await interaction.reply({
          content:
            `Choose a collection, then paste CSV mapping data.\n` +
            `For drag-and-drop CSV uploads, use: \`/set-points-mapping\`.\n` +
            `New uploads are merged into existing mappings by default.\n` +
            `Required columns: \`category\`, \`trait\`, and \`ugly_points\` (or \`points\`).\n` +
            `Examples:\n` +
            `\`category,trait,ugly_points\`\n` +
            `\`Background,Blue,250\`\n` +
            `or\n` +
            `\`category|trait|points\`\n` +
            `\`Background|Blue|250\``,
          components: [row],
          flags: 64
        });
        return;
      }

      if (interaction.customId === 'setup_remove_trait_rule') {
        const traitRules = await getTraitRoleRules(interaction.guild.id);
        if (!traitRules.length) {
          await interaction.reply({ content: 'No trait role rules to remove.', flags: 64 });
          return;
        }
        const options = traitRules.slice(0, 25).map((r) => ({
          label: `${r.role_name} (${r.trait_category || 'any'}:${r.trait_value})`.slice(0, 100),
          value: String(r.id),
          description: String(r.contract_address).slice(0, 100),
        }));
        const row = new ActionRowBuilder().addComponents(
          new StringSelectMenuBuilder()
            .setCustomId('setup_remove_trait_rule_select')
            .setPlaceholder('Select a trait role rule to remove')
            .addOptions(options)
        );
        await interaction.reply({ content: 'Select the trait role rule to remove:', components: [row], flags: 64 });
        return;
      }

      if (interaction.customId === 'setup_add_collection_from_points') {
        const modal = new ModalBuilder().setCustomId('setup_add_collection_modal').setTitle('Add Collection');
        modal.addComponents(
          new ActionRowBuilder().addComponents(
            new TextInputBuilder().setCustomId('collection_name').setLabel('Collection name').setRequired(true).setStyle(TextInputStyle.Short)
          ),
          new ActionRowBuilder().addComponents(
            new TextInputBuilder().setCustomId('contract_address').setLabel('Contract address').setRequired(true).setStyle(TextInputStyle.Short)
          ),
        );
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === 'setup_remove_points_mapping') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const mappings = await getGuildPointMappingsWithOwners(interaction.guild.id);
        if (!mappings.length) {
          await interaction.reply({ content: 'No points mappings to remove.', flags: 64 });
          return;
        }
        const collections = await getHolderCollections(interaction.guild.id);
        const nameByContract = new Map(collections.map((c) => [String(c.contract_address).toLowerCase(), c.name]));
        const options = mappings.slice(0, 25).map((m) => {
          const addr = String(m.contractAddress).toLowerCase();
          const ownerLabel = m.createdByDiscordId ? `Owner: ${m.createdByDiscordId}` : 'Owner: legacy';
          return {
            label: `${String(nameByContract.get(addr) || labelForContract(addr)).slice(0, 72)}`.slice(0, 100),
            value: addr,
            description: ownerLabel.slice(0, 100),
          };
        });
        const row = new ActionRowBuilder().addComponents(
          new StringSelectMenuBuilder()
            .setCustomId('setup_remove_points_mapping_select')
            .setPlaceholder('Select points mapping to remove')
            .addOptions(options)
        );
        await interaction.reply({
          content: 'Select the points mapping to remove. Only the mapping creator or DEFAULT_ADMIN_USER can remove it.',
          components: [row],
          flags: 64
        });
        return;
      }

      if (interaction.customId === 'setup_remove_rule') {
        const rules = await getHolderRules(interaction.guild.id);
        if (!rules.length) {
          await interaction.reply({ content: 'No holder rules to remove.', flags: 64 });
          return;
        }
        const options = rules.slice(0, 25).map((r) => ({
          label: `${r.role_name} (${r.min_tokens}-${r.max_tokens ?? '∞'})`.slice(0, 100),
          value: String(r.id),
          description: String(r.contract_address).slice(0, 100),
        }));
        const row = new ActionRowBuilder().addComponents(
          new StringSelectMenuBuilder()
            .setCustomId('setup_remove_rule_select')
            .setPlaceholder('Select a holder rule to remove')
            .addOptions(options)
        );
        await interaction.reply({ content: 'Select the holder role rule to remove:', components: [row], flags: 64 });
        return;
      }

      if (interaction.customId === 'setup_drip_key' || interaction.customId === 'setup_client_id' || interaction.customId === 'setup_realm_id' || interaction.customId === 'setup_currency_id' || interaction.customId === 'setup_receipt_channel' || interaction.customId === 'setup_points_label' || interaction.customId === 'setup_payout_amount' || interaction.customId === 'setup_claim_streak_bonus') {
        const fieldMap = {
          setup_drip_key: ['setup_drip_key_modal', 'DRIP API Key', 'drip_api_key', 'API key'],
          setup_client_id: ['setup_client_id_modal', 'DRIP Client ID', 'drip_client_id', 'Client ID'],
          setup_realm_id: ['setup_realm_id_modal', 'DRIP Realm ID', 'drip_realm_id', 'Realm ID'],
          setup_currency_id: ['setup_currency_id_modal', 'Currency ID', 'currency_id', 'Currency ID'],
          setup_receipt_channel: ['setup_receipt_channel_modal', 'Receipt Channel ID', 'receipt_channel_id', 'Channel ID'],
          setup_points_label: ['setup_points_label_modal', 'Points Label', 'points_label', 'Points label (e.g. UglyPoints)'],
          setup_payout_amount: ['setup_payout_amount_modal', 'Payout Amount', 'payout_amount', 'Number'],
          setup_claim_streak_bonus: ['setup_claim_streak_bonus_modal', 'Claim Streak Bonus', 'claim_streak_bonus', 'Flat bonus amount (0 disables)'],
        };
        const [id, title, field, label] = fieldMap[interaction.customId];
        const modal = new ModalBuilder().setCustomId(id).setTitle(title);
        modal.addComponents(
          new ActionRowBuilder().addComponents(new TextInputBuilder().setCustomId(field).setLabel(label).setRequired(true).setStyle(TextInputStyle.Short))
        );
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === 'setup_payout_type') {
        const settings = await getGuildSettings(interaction.guild.id);
        const pointsLabel = getPointsLabel(settings);
        const row = new ActionRowBuilder().addComponents(
          new StringSelectMenuBuilder()
            .setCustomId('setup_payout_type_select')
            .setPlaceholder('Select payout type')
            .addOptions(
              { label: 'Per NFT', value: 'per_nft', description: 'Payout = owned NFT count x payout amount' },
              { label: `Per ${pointsLabel}`, value: 'per_up', description: `Payout = total ${pointsLabel} x payout amount` },
            )
        );
        await interaction.reply({ content: 'Choose payout type:', components: [row], flags: 64 });
        return;
      }

      if (interaction.customId === 'setup_verify_drip') {
        await interaction.deferReply({ flags: 64 });
        const settings = await getGuildSettings(interaction.guild.id);
        const result = await verifyDripConnection(settings, interaction.user.id);
        if (!result.ok) {
          await postAdminSystemLog({
            guild: interaction.guild,
            category: 'DRIP Failure',
            message:
              `User: <@${interaction.user.id}>\n` +
              `Context: setup verify DRIP\n` +
              `Reason: ${String(result.reason || 'unknown').slice(0, 500)}`
          });
          await interaction.editReply({
            content:
              `DRIP verification failed.\n` +
              `${result.reason}\n` +
              `Check API key, realm, and currency settings in this panel.`
          });
          return;
        }
        const currencyLabel = `${result.currencyEmoji ? `${result.currencyEmoji} ` : ''}${result.currencyName || settings.currency_id}`;
        const memberProbeText = result.memberProbe?.ok
          ? `Member lookup endpoint reachable (probe matches: ${result.memberProbe.count}).`
          : `Member lookup probe failed: ${result.memberProbe?.error || 'unknown error'}`;
        await interaction.editReply({
          content:
            `DRIP verification passed.\n` +
            `Realm: \`${settings.drip_realm_id}\`\n` +
            `Currency: \`${settings.currency_id}\` (${currencyLabel})\n` +
            `Realm points loaded: ${result.pointsCount}\n` +
            `${memberProbeText}`
        });
        return;
      }

      if (interaction.customId === 'setup_remove_drip') {
        const row = new ActionRowBuilder().addComponents(
          new ButtonBuilder().setCustomId('setup_remove_drip_confirm').setLabel('Yes, Remove DRIP').setStyle(ButtonStyle.Danger),
          new ButtonBuilder().setCustomId('setup_remove_drip_cancel').setLabel('Cancel').setStyle(ButtonStyle.Secondary),
        );
        await interaction.reply({
          flags: 64,
          content:
            `Are you sure?\n` +
            `This will clear DRIP API key, client ID, realm ID, currency, receipt channel, and reset payout settings.`,
          components: [row],
        });
        return;
      }

      if (interaction.customId === 'setup_remove_drip_cancel') {
        await interaction.update({
          content: 'DRIP removal canceled.',
          components: [],
        });
        return;
      }

      if (interaction.customId === 'setup_remove_drip_confirm') {
        await clearDripSettings(interaction.guild.id);
        await interaction.update({
          content: 'DRIP settings removed and payout settings reset to defaults.',
          components: [],
        });
        return;
      }

      if (interaction.customId === 'setup_view') {
        const settings = await getGuildSettings(interaction.guild.id);
        const rules = await getHolderRules(interaction.guild.id);
        const traitRules = await getTraitRoleRules(interaction.guild.id);
        const collections = await getHolderCollections(interaction.guild.id);
        const mappings = await getGuildPointMappings(interaction.guild.id);
        const mappingLines = [...mappings.keys()].map((c) => `- ${labelForContract(c)}: ${c}`);
        await interaction.reply({
          flags: 64,
          content:
            `Settings:\n` +
            `- DRIP API Key: ${settings?.drip_api_key ? 'set' : 'not set'}\n` +
            `- DRIP Client ID: ${settings?.drip_client_id ? 'set' : 'not set'}\n` +
            `- DRIP Realm ID: ${settings?.drip_realm_id || 'not set'}\n` +
            `- Currency ID: ${settings?.currency_id || 'not set'}\n` +
            `- Receipt Channel ID: ${settings?.receipt_channel_id || RECEIPT_CHANNEL_ID}\n` +
            `- Points Label: ${getPointsLabel(settings)}\n` +
            `- Payout Type: ${settings?.payout_type || 'per_up'}\n` +
            `- Payout Amount: ${settings?.payout_amount || 1}\n` +
            `- Claim Streak Bonus: ${settings?.claim_streak_bonus || 0}\n\n` +
            `Collections (${collections.length}):\n` +
            `${collections.map(c => `- ${c.name}: ${c.contract_address}`).join('\n') || '- none'}\n\n` +
            `Points Mappings (${mappings.size}):\n` +
            `${mappingLines.join('\n') || '- none'}\n\n` +
            `Rules (${rules.length}):\n` +
            `${rules.map(r => `- ${r.role_name}: ${r.contract_address} (${r.min_tokens}-${r.max_tokens ?? '∞'})`).join('\n') || '- none'}\n\n` +
            `Trait Rules (${traitRules.length}):\n` +
            `${traitRules.map(r => `- ${r.role_name}: ${r.contract_address} (${r.trait_category || 'any'}:${r.trait_value})`).join('\n') || '- none'}`
        });
        return;
      }
      return;
    }

    if (interaction.isRoleSelectMenu() && interaction.customId === 'setup_add_rule_role_select') {
      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }
      const key = `${interaction.guild.id}:${interaction.user.id}`;
      const pending = globalThis.__PENDING_HOLDER_RULES.get(key);
      if (!pending || !pending.contractAddress || !Number.isInteger(pending.minTokens) || (pending.maxTokens != null && !Number.isInteger(pending.maxTokens))) {
        await interaction.reply({ content: 'No pending holder rule found. Click "Add Holder Role Rule" again.', flags: 64 });
        return;
      }
      const roleId = interaction.values?.[0];
      if (!roleId) {
        await interaction.reply({ content: 'No role selected.', flags: 64 });
        return;
      }
      const role = await addHolderRule(interaction.guild, {
        roleId,
        contractAddress: pending.contractAddress,
        minTokens: pending.minTokens,
        maxTokens: pending.maxTokens
      });
      globalThis.__PENDING_HOLDER_RULES.delete(key);
      await interaction.update({
        content: `Rule added for role **${role.name}** on \`${pending.contractAddress}\` (${pending.minTokens}-${pending.maxTokens ?? '∞'}).`,
        components: []
      });
      return;
    }

    if (interaction.isRoleSelectMenu() && interaction.customId === 'setup_add_trait_rule_role_select') {
      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }
      const key = `${interaction.guild.id}:${interaction.user.id}`;
      const pending = globalThis.__PENDING_TRAIT_ROLE_RULES.get(key);
      if (!pending || !pending.contractAddress || !pending.traitValue) {
        await interaction.reply({ content: 'No pending trait role rule found. Click "Add Trait Role" again.', flags: 64 });
        return;
      }
      const roleId = interaction.values?.[0];
      if (!roleId) {
        await interaction.reply({ content: 'No role selected.', flags: 64 });
        return;
      }
      const role = await addTraitRoleRule(interaction.guild, {
        roleId,
        contractAddress: pending.contractAddress,
        traitCategory: pending.traitCategory,
        traitValue: pending.traitValue
      });
      globalThis.__PENDING_TRAIT_ROLE_RULES.delete(key);
      await interaction.update({
        content: `Trait rule added for role **${role.name}** on \`${pending.contractAddress}\` (${pending.traitCategory || 'any'}:${pending.traitValue}).`,
        components: []
      });
      return;
    }

    if (interaction.isStringSelectMenu() && interaction.customId === 'setup_payout_type_select') {
      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }
      await upsertGuildSetting(interaction.guild.id, 'payout_type', interaction.values[0]);
      await interaction.update({ content: `Payout type set to \`${interaction.values[0]}\`.`, components: [] });
      return;
    }

    if (interaction.isStringSelectMenu() && interaction.customId === 'rewards_view_holdings_select') {
      const settings = await getGuildSettings(interaction.guild.id);
      const pointsLabel = getPointsLabel(settings);
      const links = await getWalletLinks(interaction.guild.id, interaction.user.id);
      const walletAddresses = links.map((x) => x.wallet_address).filter(Boolean);
      if (!walletAddresses.length) {
        await interaction.update({ content: 'No wallets connected yet.', components: [] });
        return;
      }
      const stats = await computeWalletStatsForPayout(interaction.guild.id, walletAddresses, 'per_up');
      const byCollectionLines = (stats.byCollection || []).map((x) => `- ${labelForContract(x.contractAddress)}: ${x.count} NFT${x.count === 1 ? '' : 's'}`);
      const mode = interaction.values?.[0] || 'all';
      let content = '';
      if (mode === 'by_collection') {
        content = `Holdings by collection:\n${byCollectionLines.join('\n') || '- none'}`;
      } else if (mode === 'up_only') {
        content = `Total ${pointsLabel}: **${stats.totalUp}**`;
      } else {
        content =
          `Holdings summary:\n` +
          `Total NFTs: **${stats.totalNfts}**\n` +
          `Total ${pointsLabel}: **${stats.totalUp}**\n` +
          `By collection:\n${byCollectionLines.join('\n') || '- none'}`;
      }
      await interaction.update({ content, components: [] });
      return;
    }

    if (interaction.isStringSelectMenu() && interaction.customId === 'verify_check_stats_collection_select') {
      const contractAddress = normalizeEthAddress(interaction.values?.[0] || '');
      if (!contractAddress) {
        await interaction.update({ content: 'Invalid collection selection.', components: [] });
        return;
      }
      const collections = await getHolderCollections(interaction.guild.id);
      const selected = collections.find((c) => String(c.contract_address).toLowerCase() === contractAddress);
      const key = `${interaction.guild.id}:${interaction.user.id}`;
      globalThis.__PENDING_CHECK_STATS.set(key, {
        contractAddress,
        collectionName: selected?.name || labelForContract(contractAddress),
        createdAt: Date.now(),
      });
      const modal = new ModalBuilder().setCustomId('verify_check_stats_modal').setTitle('Check NFT Stats');
      modal.addComponents(
        new ActionRowBuilder().addComponents(
          new TextInputBuilder()
            .setCustomId('token_id')
            .setLabel(`${selected?.name || 'Collection'} token ID`)
            .setRequired(true)
            .setPlaceholder('1234')
            .setStyle(TextInputStyle.Short)
        )
      );
      await interaction.showModal(modal);
      return;
    }

    if (interaction.isStringSelectMenu() && interaction.customId === 'setup_add_rule_collection_select') {
      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }
      const key = `${interaction.guild.id}:${interaction.user.id}`;
      const pending = globalThis.__PENDING_HOLDER_RULES.get(key);
      if (!pending) {
        await interaction.update({ content: 'No pending holder rule found. Click "Add Holder Role" again.', components: [] });
        return;
      }
      const contractAddress = normalizeEthAddress(interaction.values?.[0] || '');
      if (!contractAddress) {
        await interaction.update({ content: 'Invalid collection selection.', components: [] });
        return;
      }
      const collections = await getHolderCollections(interaction.guild.id);
      const selected = collections.find((c) => String(c.contract_address).toLowerCase() === contractAddress);
      pending.contractAddress = contractAddress;
      pending.collectionName = selected?.name || labelForContract(contractAddress);
      globalThis.__PENDING_HOLDER_RULES.set(key, pending);
      const modal = new ModalBuilder().setCustomId('setup_add_rule_modal').setTitle('Set Token Range');
      modal.addComponents(
        new ActionRowBuilder().addComponents(new TextInputBuilder().setCustomId('min_tokens').setLabel('Min tokens (inclusive)').setRequired(true).setStyle(TextInputStyle.Short)),
        new ActionRowBuilder().addComponents(new TextInputBuilder().setCustomId('max_tokens').setLabel('Max tokens (optional)').setRequired(false).setStyle(TextInputStyle.Short)),
      );
      await interaction.showModal(modal);
      return;
    }

    if (interaction.isStringSelectMenu() && interaction.customId === 'setup_add_trait_rule_collection_select') {
      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }
      const key = `${interaction.guild.id}:${interaction.user.id}`;
      const pending = globalThis.__PENDING_TRAIT_ROLE_RULES.get(key);
      if (!pending) {
        await interaction.update({ content: 'No pending trait role rule found. Click "Add Trait Role" again.', components: [] });
        return;
      }
      const contractAddress = normalizeEthAddress(interaction.values?.[0] || '');
      if (!contractAddress) {
        await interaction.update({ content: 'Invalid collection selection.', components: [] });
        return;
      }
      const collections = await getHolderCollections(interaction.guild.id);
      const selected = collections.find((c) => String(c.contract_address).toLowerCase() === contractAddress);
      pending.contractAddress = contractAddress;
      pending.collectionName = selected?.name || labelForContract(contractAddress);
      globalThis.__PENDING_TRAIT_ROLE_RULES.set(key, pending);
      const modal = new ModalBuilder().setCustomId('setup_add_trait_rule_modal').setTitle('Set Trait Role Rule');
      modal.addComponents(
        new ActionRowBuilder().addComponents(
          new TextInputBuilder()
            .setCustomId('trait_category')
            .setLabel('Trait category (optional)')
            .setRequired(false)
            .setPlaceholder('Type, Background, Head...')
            .setStyle(TextInputStyle.Short)
        ),
        new ActionRowBuilder().addComponents(
          new TextInputBuilder()
            .setCustomId('trait_value')
            .setLabel('Trait value')
            .setRequired(true)
            .setPlaceholder('Pikachugly, Green Laser...')
            .setStyle(TextInputStyle.Short)
        ),
      );
      await interaction.showModal(modal);
      return;
    }

    if (interaction.isStringSelectMenu() && interaction.customId === 'setup_points_mapping_collection_select') {
      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }
      const contractAddress = normalizeEthAddress(interaction.values?.[0] || '');
      if (!contractAddress) {
        await interaction.update({ content: 'Invalid collection selection.', components: [] });
        return;
      }
      const key = `${interaction.guild.id}:${interaction.user.id}`;
      globalThis.__PENDING_POINTS_MAPPING.set(key, { contractAddress, createdAt: Date.now() });

      const modal = new ModalBuilder().setCustomId('setup_points_mapping_modal').setTitle('Set Points Mapping CSV');
      modal.addComponents(
        new ActionRowBuilder().addComponents(
          new TextInputBuilder()
            .setCustomId('csv_input')
            .setLabel('CSV content or URL')
            .setRequired(true)
            .setStyle(TextInputStyle.Paragraph)
        )
      );
      await interaction.showModal(modal);
      return;
    }

    if (interaction.isStringSelectMenu() && interaction.customId === 'setup_remove_rule_select') {
      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }
      const ruleId = Number(interaction.values?.[0]);
      if (!Number.isInteger(ruleId) || ruleId <= 0) {
        await interaction.update({ content: 'Invalid rule selection.', components: [] });
        return;
      }
      const removed = await disableHolderRule(interaction.guild.id, ruleId);
      if (!removed) {
        await interaction.update({ content: 'Rule not found or already removed.', components: [] });
        return;
      }
      await interaction.update({
        content: `Removed holder rule: **${removed.role_name}** on \`${removed.contract_address}\` (${removed.min_tokens}-${removed.max_tokens ?? '∞'}).`,
        components: []
      });
      return;
    }

    if (interaction.isStringSelectMenu() && interaction.customId === 'setup_remove_points_mapping_select') {
      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }
      const contractAddress = normalizeEthAddress(interaction.values?.[0] || '');
      if (!contractAddress) {
        await interaction.update({ content: 'Invalid mapping selection.', components: [] });
        return;
      }
      const removed = await removeGuildPointMapping(interaction.guild.id, contractAddress, interaction.user.id);
      if (!removed.ok) {
        if (removed.reason === 'not_found') {
          await interaction.update({ content: 'Mapping not found (it may already be removed).', components: [] });
          return;
        }
        if (removed.reason === 'forbidden') {
          const ownerText = removed.ownerId
            ? `Only <@${removed.ownerId}> or DEFAULT_ADMIN_USER can remove it.`
            : 'Only DEFAULT_ADMIN_USER can remove this legacy mapping.';
          await interaction.update({
            content: `You cannot remove this mapping. ${ownerText}`,
            components: []
          });
          return;
        }
        await interaction.update({ content: 'Could not remove mapping.', components: [] });
        return;
      }

      const collections = await getHolderCollections(interaction.guild.id);
      const selected = collections.find((c) => String(c.contract_address).toLowerCase() === String(contractAddress).toLowerCase());
      await interaction.update({
        content: `Removed points mapping for **${selected?.name || labelForContract(contractAddress)}** (\`${contractAddress}\`).`,
        components: []
      });
      return;
    }

    if (interaction.isStringSelectMenu() && interaction.customId === 'setup_remove_trait_rule_select') {
      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }
      const ruleId = Number(interaction.values?.[0]);
      if (!Number.isInteger(ruleId) || ruleId <= 0) {
        await interaction.update({ content: 'Invalid rule selection.', components: [] });
        return;
      }
      const removed = await disableTraitRoleRule(interaction.guild.id, ruleId);
      if (!removed) {
        await interaction.update({ content: 'Trait rule not found or already removed.', components: [] });
        return;
      }
      await interaction.update({
        content: `Removed trait rule: **${removed.role_name}** on \`${removed.contract_address}\` (${removed.trait_category || 'any'}:${removed.trait_value}).`,
        components: []
      });
      return;
    }

    if (interaction.isModalSubmit()) {
      if (interaction.customId === 'verify_connect_modal') {
        const raw = interaction.fields.getTextInputValue('wallet_address');
        const addresses = parseWalletAddressesInput(raw);
        if (!addresses.length) {
          await interaction.reply({ content: 'Provide at least one valid Ethereum address.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });

        const settings = await getGuildSettings(interaction.guild.id);
        const existing = await getWalletLinks(interaction.guild.id, interaction.user.id);
        const existingSet = new Set(existing.map((x) => String(x.wallet_address).toLowerCase()));
        const added = [];
        const verificationResults = [];
        const blockedByOtherUser = [];
        let primaryDripMemberId = null;
        for (const addr of addresses) {
          const currentOwner = await getWalletOwnerLink(interaction.guild.id, addr);
          if (currentOwner && String(currentOwner.discord_id) !== String(interaction.user.id)) {
            blockedByOtherUser.push({ walletAddress: addr, discordId: String(currentOwner.discord_id) });
            await postAdminSystemLog({
              guild: interaction.guild,
              category: 'Wallet Link Issue',
              message:
                `Duplicate wallet link blocked.\n` +
                `Actor: <@${interaction.user.id}>\n` +
                `Wallet: \`${addr}\`\n` +
                `Already linked to: <@${currentOwner.discord_id}>`
            });
            continue;
          }
          const verification = await verifyWalletViaDrip(
            settings?.drip_realm_id,
            interaction.user.id,
            addr,
            settings
          );
          if (!primaryDripMemberId && verification.dripMemberId) primaryDripMemberId = verification.dripMemberId;
          verificationResults.push({ walletAddress: addr, ...verification });
          await setWalletLink(interaction.guild.id, interaction.user.id, addr, verification.verified, verification.dripMemberId);
          if (!existingSet.has(addr)) {
            added.push(addr);
            await postWalletReceipt(interaction.guild, settings, interaction.user.id, 'Connected', addr);
          }
        }
        const allLinks = await getWalletLinks(interaction.guild.id, interaction.user.id);
        const allAddresses = allLinks.map((x) => x.wallet_address);
        const member = await interaction.guild.members.fetch(interaction.user.id);
        const sync = await syncHolderRoles(member, allAddresses);
        await postRoleSyncFailures(interaction.guild, interaction.user.id, sync, 'wallet connect');
        const verifiedCount = verificationResults.filter((x) => x.verified).length;
        const unverifiedResults = verificationResults.filter((x) => !x.verified);
        const dripFailures = unverifiedResults.filter((x) => /temporarily unavailable/i.test(String(x.reason || '')));
        for (const item of dripFailures) {
          await postAdminSystemLog({
            guild: interaction.guild,
            category: 'DRIP Failure',
            message:
              `User: <@${interaction.user.id}>\n` +
              `Context: wallet connect\n` +
              `Wallet: \`${item.walletAddress}\`\n` +
              `Reason: ${String(item.reason || '').slice(0, 500)}`
          });
        }
        if (primaryDripMemberId) {
          for (const row of allLinks) {
            await setWalletLink(
              interaction.guild.id,
              interaction.user.id,
              row.wallet_address,
              Boolean(row.verified),
              row.drip_member_id || primaryDripMemberId
            );
          }
        }
        if (unverifiedResults.length && sync.granted?.length) {
          for (const item of unverifiedResults) {
            await postAdminVerificationFlag(
              interaction.guild,
              interaction.user.id,
              item.walletAddress,
              item.reason,
              sync.granted
            );
          }
        }
        const dripStatus = unverifiedResults.length
          ? (
              `DRIP wallet verification pending for ${unverifiedResults.length} wallet${unverifiedResults.length === 1 ? '' : 's'}.\n` +
              `To verify, connect the same wallet to your DRIP profile in this realm, then reconnect here.\n` +
              `If you need help, open a ticket in <#${SUPPORT_TICKET_CHANNEL_ID}>.\n` +
              `Reason: ${unverifiedResults[0].reason}`
            )
          : (verifiedCount
              ? `DRIP verified ${verifiedCount} wallet${verifiedCount === 1 ? '' : 's'}.`
              : 'DRIP profile check unavailable.');
        const blockedText = blockedByOtherUser.length
          ? `\nBlocked duplicate wallet(s): ${blockedByOtherUser.map((x) => `\`${x.walletAddress}\``).join(', ')}`
          : '';
        await interaction.editReply({
          content:
            `Wallet connect processed.\n` +
            `Added: ${added.length}\n` +
            `Total linked wallets: ${allAddresses.length}\n` +
            `${dripStatus}\n` +
            `${blockedText}\n` +
            `Role sync complete (${sync.changed} change${sync.changed === 1 ? '' : 's'}).\n` +
            `${sync.granted?.length ? `Roles granted: ${sync.granted.join(', ')}` : 'Roles granted: none'}`
        });
        return;
      }

      if (interaction.customId === 'verify_disconnect_modal') {
        const raw = String(interaction.fields.getTextInputValue('wallet_address') || '').trim();
        await interaction.deferReply({ flags: 64 });
        const settings = await getGuildSettings(interaction.guild.id);
        let removedWallets = [];
        if (raw.toUpperCase() === 'ALL') {
          const existing = await getWalletLinks(interaction.guild.id, interaction.user.id);
          await deleteAllWalletLinks(interaction.guild.id, interaction.user.id);
          removedWallets = existing.map((x) => x.wallet_address);
        } else {
          const addr = normalizeEthAddress(raw);
          if (!addr) {
            await interaction.editReply({ content: 'Invalid wallet address. Use a valid `0x...` address or `ALL`.' });
            return;
          }
          const removed = await deleteWalletLink(interaction.guild.id, interaction.user.id, addr);
          if (!removed) {
            await interaction.editReply({ content: 'That wallet is not connected on this server.' });
            return;
          }
          removedWallets = [addr];
        }

        for (const addr of removedWallets) {
          await postWalletReceipt(interaction.guild, settings, interaction.user.id, 'Disconnected', addr);
        }

        let removedRoles = 0;
        try {
          const member = await interaction.guild.members.fetch(interaction.user.id);
          const remainingLinks = await getWalletLinks(interaction.guild.id, interaction.user.id);
          const remainingAddresses = remainingLinks.map((x) => x.wallet_address);
          const sync = await syncHolderRoles(member, remainingAddresses);
          await postRoleSyncFailures(interaction.guild, interaction.user.id, sync, 'wallet disconnect');
          removedRoles = sync.changed;
        } catch (err) {
          await postAdminSystemLog({
            guild: interaction.guild,
            category: 'Role Sync Failure',
            message:
              `User: <@${interaction.user.id}>\n` +
              `Context: wallet disconnect\n` +
              `Reason: ${String(err?.message || err || '').slice(0, 500)}`
          });
        }
        await interaction.editReply({
          content:
            `Disconnected wallet(s): ${removedWallets.length}\n` +
            `Role sync changes: ${removedRoles}`
        });
        return;
      }

      if (interaction.customId === 'verify_check_stats_modal') {
        const tokenId = String(interaction.fields.getTextInputValue('token_id') || '').trim();
        if (!/^\d+$/.test(tokenId)) {
          await interaction.reply({ content: 'Token ID must be numeric.', flags: 64 });
          return;
        }
        const key = `${interaction.guild.id}:${interaction.user.id}`;
        const pending = globalThis.__PENDING_CHECK_STATS.get(key) || null;
        globalThis.__PENDING_CHECK_STATS.delete(key);
        const contractAddress = normalizeEthAddress(pending?.contractAddress || '') || SQUIGS_CONTRACT.toLowerCase();
        await interaction.deferReply({ flags: 64 });
        const settings = await getGuildSettings(interaction.guild.id);
        const pointsLabel = getPointsLabel(settings);
        const guildPointMappings = await getGuildPointMappings(interaction.guild.id);
        const table = hpTableForContract(contractAddress, guildPointMappings);
        const meta = await getNftMetadataAlchemy(tokenId, contractAddress);
        const { attrs } = await getTraitsForToken(meta, tokenId, contractAddress);
        const grouped = normalizeTraits(attrs);
        const hpAgg = computeHpFromTraits(grouped, table);
        const tier = hpToTierLabel(hpAgg.total || 0);
        const collectionName = String(pending?.collectionName || labelForContract(contractAddress));
        const imageUrlRaw = String(
          meta?.image?.cachedUrl ||
          meta?.image?.pngUrl ||
          meta?.image?.thumbnailUrl ||
          meta?.raw?.metadata?.image ||
          ''
        ).trim();
        const imageUrl = /^https?:\/\//i.test(imageUrlRaw)
          ? imageUrlRaw
          : (contractAddress === SQUIGS_CONTRACT.toLowerCase()
            ? `https://assets.bueno.art/images/a49527dc-149c-4cbc-9038-d4b0d1dbf0b2/default/${tokenId}`
            : null);

        const traitLines = [];
        for (const cat of Object.keys(grouped)) {
          for (const t of grouped[cat]) {
            const pts = hpAgg.per[`${cat}::${t.value}`] ?? 0;
            traitLines.push(`• ${cat} | ${t.value}: **${pts}**`);
          }
        }
        let traitsText = traitLines.join('\n');
        if (!traitsText) traitsText = '- none';
        const maxTraitsChars = 3200;
        if (traitsText.length > maxTraitsChars) {
          traitsText = `${traitsText.slice(0, maxTraitsChars)}\n... (truncated)`;
        }
        const desc =
          `Collection: **${collectionName}**\n` +
          `Contract: \`${contractAddress}\`\n` +
          `Total ${pointsLabel}: **${hpAgg.total || 0}**\n` +
          `Rarity: **${tier}**\n\n` +
          `Trait ${pointsLabel} breakdown:\n${traitsText}`;
        const embed = new EmbedBuilder()
          .setTitle(`${collectionName} #${tokenId}`)
          .setDescription(desc)
          .setColor(0x7A83BF);
        if (imageUrl) embed.setImage(imageUrl);
        await interaction.editReply({
          embeds: [embed]
        });
        return;
      }

      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }

      if (interaction.customId === 'setup_add_rule_modal') {
        const minTokens = Number(interaction.fields.getTextInputValue('min_tokens'));
        const maxRaw = String(interaction.fields.getTextInputValue('max_tokens') || '').trim();
        const maxTokens = maxRaw === '' ? null : Number(maxRaw);
        if (!Number.isInteger(minTokens) || minTokens < 0 || (maxTokens != null && (!Number.isInteger(maxTokens) || maxTokens < minTokens))) {
          await interaction.reply({ content: 'Invalid min/max token values.', flags: 64 });
          return;
        }
        const key = `${interaction.guild.id}:${interaction.user.id}`;
        const pending = globalThis.__PENDING_HOLDER_RULES.get(key);
        if (!pending?.contractAddress) {
          await interaction.reply({ content: 'No pending collection selection found. Click "Add Holder Role" again.', flags: 64 });
          return;
        }
        pending.minTokens = minTokens;
        pending.maxTokens = maxTokens;
        globalThis.__PENDING_HOLDER_RULES.set(key, pending);
        const row = new ActionRowBuilder().addComponents(
          new RoleSelectMenuBuilder()
            .setCustomId('setup_add_rule_role_select')
            .setPlaceholder('Select the holder role')
            .setMinValues(1)
            .setMaxValues(1)
        );
        await interaction.reply({
          content: `Now select which role should be assigned for **${pending.collectionName || labelForContract(pending.contractAddress)}** (\`${pending.contractAddress}\`) with range ${minTokens}-${maxTokens ?? '∞'}:`,
          components: [row],
          flags: 64
        });
        return;
      }

      if (interaction.customId === 'setup_add_trait_rule_modal') {
        const key = `${interaction.guild.id}:${interaction.user.id}`;
        const pending = globalThis.__PENDING_TRAIT_ROLE_RULES.get(key);
        if (!pending?.contractAddress) {
          await interaction.reply({ content: 'No pending collection selection found. Click "Add Trait Role" again.', flags: 64 });
          return;
        }

        const traitCategoryRaw = String(interaction.fields.getTextInputValue('trait_category') || '').trim();
        const traitValueRaw = String(interaction.fields.getTextInputValue('trait_value') || '').trim();
        if (!traitValueRaw) {
          await interaction.reply({ content: 'Trait value is required.', flags: 64 });
          return;
        }

        const guildPointMappings = await getGuildPointMappings(interaction.guild.id);
        const table = hpTableForContract(pending.contractAddress, guildPointMappings);
        const matched = findMatchingTraitDefinition(table, traitCategoryRaw, traitValueRaw);
        if (!matched) {
          await interaction.reply({
            content:
              `Trait not found in available traits for this collection.\n` +
              `Use a trait value already present in the built-in table or the configured points mapping.`,
            flags: 64
          });
          return;
        }

        pending.traitCategory = matched.category;
        pending.traitValue = matched.trait;
        globalThis.__PENDING_TRAIT_ROLE_RULES.set(key, pending);
        const row = new ActionRowBuilder().addComponents(
          new RoleSelectMenuBuilder()
            .setCustomId('setup_add_trait_rule_role_select')
            .setPlaceholder('Select the trait role')
            .setMinValues(1)
            .setMaxValues(1)
        );
        await interaction.reply({
          content: `Now select which role should be assigned for **${pending.collectionName || labelForContract(pending.contractAddress)}** (\`${pending.contractAddress}\`) when a user owns trait ${matched.category}:${matched.trait}:`,
          components: [row],
          flags: 64
        });
        return;
      }

      if (interaction.customId === 'setup_add_collection_modal') {
        const name = String(interaction.fields.getTextInputValue('collection_name') || '').trim();
        const contractAddress = normalizeEthAddress(interaction.fields.getTextInputValue('contract_address'));
        if (!name) {
          await interaction.reply({ content: 'Collection name is required.', flags: 64 });
          return;
        }
        if (!contractAddress) {
          await interaction.reply({ content: 'Invalid contract address.', flags: 64 });
          return;
        }
        await upsertHolderCollection(interaction.guild.id, name, contractAddress);
        await interaction.reply({ content: `Collection saved: **${name}** (\`${contractAddress}\`)`, flags: 64 });
        return;
      }

      if (interaction.customId === 'setup_points_mapping_modal') {
        const key = `${interaction.guild.id}:${interaction.user.id}`;
        const pending = globalThis.__PENDING_POINTS_MAPPING.get(key);
        if (!pending?.contractAddress) {
          await interaction.reply({ content: 'No pending collection selection found. Click "Points Mapping" again.', flags: 64 });
          return;
        }

        let csvInput = String(interaction.fields.getTextInputValue('csv_input') || '').trim();
        if (!csvInput) {
          await interaction.reply({ content: 'CSV content is required.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });

        if (/^https?:\/\//i.test(csvInput)) {
          try {
            const res = await fetchWithRetry(csvInput, 2, 700, {});
            csvInput = await res.text();
          } catch (err) {
            await interaction.editReply({
              content: `Could not load CSV from URL: ${String(err?.message || err || 'unknown error').slice(0, 180)}`
            });
            return;
          }
        }

        try {
          const parsed = parsePointsMappingCsv(csvInput);
          const existingMappings = await getGuildPointMappings(interaction.guild.id);
          const existingTable = existingMappings.get(String(pending.contractAddress).toLowerCase()) || {};
          const merged = mergePointsMappingTables(existingTable, parsed.table);
          await setGuildPointMapping(interaction.guild.id, pending.contractAddress, merged.table, interaction.user.id);
          globalThis.__PENDING_POINTS_MAPPING.delete(key);
          const collections = await getHolderCollections(interaction.guild.id);
          const selected = collections.find((c) => String(c.contract_address).toLowerCase() === String(pending.contractAddress).toLowerCase());
          await interaction.editReply({
            content:
              `Points mapping merged for **${selected?.name || labelForContract(pending.contractAddress)}** (\`${pending.contractAddress}\`).\n` +
              `Rows imported: ${parsed.rowCount}\n` +
              `Total categories: ${merged.totalCategories}\n` +
              `Traits added: ${merged.addedTraits}\n` +
              `Traits updated: ${merged.updatedTraits}\n` +
              `Delimiter detected: \`${parsed.delimiter}\``
          });
        } catch (err) {
          await interaction.editReply({
            content:
              `Invalid mapping format: ${String(err?.message || err || '').slice(0, 220)}\n` +
              `Required format:\n` +
              `\`category,trait,ugly_points\`\n` +
              `\`Background,Blue,250\`\n` +
              `or\n` +
              `\`category|trait|points\`\n` +
              `\`Background|Blue|250\``
          });
        }
        return;
      }

      const settingModalMap = {
        setup_drip_key_modal: 'drip_api_key',
        setup_client_id_modal: 'drip_client_id',
        setup_realm_id_modal: 'drip_realm_id',
        setup_currency_id_modal: 'currency_id',
        setup_receipt_channel_modal: 'receipt_channel_id',
        setup_points_label_modal: 'points_label',
        setup_payout_amount_modal: 'payout_amount',
        setup_claim_streak_bonus_modal: 'claim_streak_bonus',
      };
      const field = settingModalMap[interaction.customId];
      if (field) {
        const value = interaction.fields.getTextInputValue(field).trim();
        if (field === 'payout_amount' || field === 'claim_streak_bonus') {
          const n = Number(value);
          if (!Number.isFinite(n) || n < 0) {
            await interaction.reply({ content: `${field === 'payout_amount' ? 'Payout amount' : 'Claim streak bonus'} must be a non-negative number.`, flags: 64 });
            return;
          }
          await upsertGuildSetting(interaction.guild.id, field, n);
        } else {
          await upsertGuildSetting(interaction.guild.id, field, value);
        }
        await interaction.reply({ content: `Updated \`${field}\`.`, flags: 64 });
        return;
      }
    }
  } catch (err) {
    console.error('❌ Verification interaction error:', err);
    const msg = String(err?.message || err || '').trim();
    await postAdminSystemLog({
      guild: interaction.guild || null,
      guildId: interaction.guild?.id || null,
      category:
        /DRIP/i.test(msg) ? 'DRIP Failure'
        : /wallet|claims|database|relation|column/i.test(msg) ? 'Wallet Link Issue'
        : 'Interaction Failure',
      message:
        `User: ${interaction.user ? `<@${interaction.user.id}>` : 'unknown'}\n` +
        `Context: interaction handler\n` +
        `Reason: ${msg.slice(0, 500)}`
    });
    if (interaction.deferred || interaction.replied) {
      await interaction.followUp({ content: '⚠️ Something went wrong handling that action.', flags: 64 }).catch(() => {});
    } else {
      await interaction.reply({ content: '⚠️ Something went wrong handling that action.', flags: 64 }).catch(() => {});
    }
  }
});
// ===== LOGIN =====
client.login(DISCORD_TOKEN);
// ===== Helper funcs (metadata) =====
async function getNftMetadataAlchemy(tokenId, contractAddress = SQUIGS_CONTRACT) {
  const url =
    `https://eth-mainnet.g.alchemy.com/nft/v3/${ALCHEMY_API_KEY}/getNFTMetadata` +
    `?contractAddress=${contractAddress}&tokenId=${tokenId}&refreshCache=false`;
  const res = await fetchWithRetry(url, 3, 800, { timeout: 10000 });
  return res.json();
}

// ===== STRICT MINT CHECK (hot-reload safe singletons) =====
const ALCHEMY_RPC_URL = `https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_API_KEY}`;
globalThis.__SQUIGS_PROVIDER   ||= new ethers.JsonRpcProvider(ALCHEMY_RPC_URL);
globalThis.__SQUIGS_ERC721_ABI ||= ['function ownerOf(uint256 tokenId) view returns (address)'];
globalThis.__SQUIGS_ERC721     ||= new ethers.Contract(SQUIGS_CONTRACT, globalThis.__SQUIGS_ERC721_ABI, globalThis.__SQUIGS_PROVIDER);
globalThis.__SQUIGS_MINT_CACHE ||= new Map();
globalThis.__SQUIGS_MINT_CACHE_ORDER ||= [];
const squigsErc721 = globalThis.__SQUIGS_ERC721;

function mintCacheSet(key, val, max = 5000) {
  const cache = globalThis.__SQUIGS_MINT_CACHE;
  const order = globalThis.__SQUIGS_MINT_CACHE_ORDER;
  if (!cache.has(key)) order.push(key);
  cache.set(key, val);
  while (order.length > max) {
    const oldest = order.shift();
    cache.delete(oldest);
  }
}

// Not-minted messages (dedup-safe)
if (!globalThis.__SQUIGS_NOT_MINTED_MESSAGES) {
  globalThis.__SQUIGS_NOT_MINTED_MESSAGES = [
    (id) => `👀 Squig #${id} hasn’t crawled out of the mint swamp yet.\nGo hatch one at **https://squigs.io**`,
    (id) => `🫥 Squig #${id} is still a rumor. Mint your destiny at **https://squigs.io**`,
    (id) => `🌀 Squig #${id} is hiding in the spiral dimension. The portal is **https://squigs.io**`,
    (id) => `🥚 Squig #${id} is still an egg. Crack it open at **https://squigs.io**`,
    (id) => `🤫 The Squigs whisper: “#${id}? Not minted.” Try **https://squigs.io**`,
  ];
}
function notMintedLine(tokenId) {
  const list = globalThis.__SQUIGS_NOT_MINTED_MESSAGES;
  const pick = list[Math.floor(Math.random() * list.length)];
  return pick(tokenId);
}

/**
 * Strict mint check:
 *  1) ownerOf(tokenId): if it returns an address, it's minted; if it REVERTS, it's not minted.
 *  2) Fallback: Alchemy getOwnersForNFT.
 *  3) If both are unavailable, return 'UNVERIFIED' (we block rendering with a gentle message).
 */
async function isSquigMintedStrict(tokenId) {
  const cache = globalThis.__SQUIGS_MINT_CACHE;
  if (cache.has(tokenId)) return cache.get(tokenId);

  // 1) ERC-721 ownerOf via ethers
  try {
    const owner = await squigsErc721.ownerOf(tokenId);
    const minted = !!owner && owner !== '0x0000000000000000000000000000000000000000';
    mintCacheSet(tokenId, minted);
    return minted;
  } catch (e) {
    const msg = String(e?.shortMessage || e?.message || '');
    if (e?.code === 'CALL_EXCEPTION' || /execution reverted/i.test(msg)) {
      mintCacheSet(tokenId, false);
      return false;
    }
    // other errors: continue to fallback
  }

  // 2) Alchemy owners fallback
  try {
    const url = `https://eth-mainnet.g.alchemy.com/nft/v3/${ALCHEMY_API_KEY}/getOwnersForNFT` +
                `?contractAddress=${SQUIGS_CONTRACT}&tokenId=${tokenId}`;
    const res = await fetchWithRetry(url, 2, 600);
    const data = await res.json();
    const owners =
      (Array.isArray(data?.owners) && data.owners) ||
      (Array.isArray(data?.ownerAddresses) && data.ownerAddresses) ||
      [];
    const minted = owners.length > 0;
    mintCacheSet(tokenId, minted);
    return minted;
  } catch (e2) {
    console.warn(`⚠️ Mint check unavailable for #${tokenId}:`, e2.message);
    return 'UNVERIFIED';
  }
}

// -------- flexible trait extraction with OpenSea fallback --------
async function getTraitsForToken(alchemyMeta, tokenId, contractAddress = SQUIGS_CONTRACT) {
  // 1) Try Alchemy
  const attrsA = extractAttributesFlexible(alchemyMeta);
  if (attrsA.length > 0) {
    return { attrs: attrsA, source: 'alchemy' };
  }

  // 2) Fallback to OpenSea if we have an API key
  if (OPENSEA_API_KEY) {
    try {
      const attrsB = await fetchOpenSeaTraits(tokenId, contractAddress);
      if (attrsB.length > 0) {
        console.log(`ℹ️ Traits from OpenSea fallback for #${tokenId}: ${attrsB.length}`);
        return { attrs: attrsB, source: 'opensea' };
      }
    } catch (e) {
      console.warn('⚠️ OpenSea trait fallback failed:', e.message);
    }
  }

  return { attrs: [], source: 'none' };
}

function extractAttributesFlexible(alchemyMeta) {
  if (!alchemyMeta) return [];
  const candidates = [];
  const addIfAttrArray = (arr) => {
    if (Array.isArray(arr) && arr.length && looksLikeAttributeArray(arr)) candidates.push(arr);
  };

  // common Alchemy spots
  addIfAttrArray(alchemyMeta?.metadata?.attributes);
  addIfAttrArray(alchemyMeta?.raw?.metadata?.attributes);
  addIfAttrArray(alchemyMeta?.metadata?.traits);
  addIfAttrArray(alchemyMeta?.raw?.metadata?.traits);
  addIfAttrArray(alchemyMeta?.metadata?.properties?.attributes);
  addIfAttrArray(alchemyMeta?.raw?.metadata?.properties?.attributes);

  // sometimes raw JSON is a string
  const tryParse = (maybeStr) => {
    try {
      if (typeof maybeStr === 'string' && maybeStr.trim().startsWith('{')) {
        const obj = JSON.parse(maybeStr);
        addIfAttrArray(obj.attributes);
        addIfAttrArray(obj.traits);
        addIfAttrArray(obj?.properties?.attributes);
      }
    } catch {}
  };
  tryParse(alchemyMeta?.raw?.metadata);
  tryParse(alchemyMeta?.metadata);

  const first = candidates.find(Boolean) || [];
  return first.map(massageTraitKeys).filter(validAttrFilter);
}

function looksLikeAttributeArray(arr) {
  return arr.some(o => o && typeof o === 'object' &&
    ('value' in o) && ('trait_type' in o || 'traitType' in o || 'type' in o || 'key' in o));
}

function massageTraitKeys(t) {
  const trait_type = String(t.trait_type ?? t.traitType ?? t.type ?? t.key ?? '').trim();
  return { trait_type, value: t.value };
}

function validAttrFilter(t) {
  const v = String(t?.value ?? '').trim();
  if (!v) return false;
  const low = v.toLowerCase();
  return !(low === 'none' || low === 'none (ignore)');
}

// OpenSea v2: fallback trait fetch (with headers + small retry)
async function fetchOpenSeaTraits(tokenId, contractAddress = SQUIGS_CONTRACT) {
  const url = `https://api.opensea.io/api/v2/chain/ethereum/contract/${contractAddress}/nfts/${tokenId}`;
  const headers = { 'X-API-KEY': OPENSEA_API_KEY };
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetchWithTimeout(url, { headers, timeoutMs: 10000 });
      if (!res.ok) throw new Error(`OpenSea HTTP ${res.status}`);
      const data = await res.json();
      const arr =
        (Array.isArray(data?.nft?.traits) && data.nft.traits) ||
        (Array.isArray(data?.traits) && data.traits) ||
        (Array.isArray(data?.item?.traits) && data.item.traits) ||
        [];
      return arr.map(massageTraitKeys).filter(validAttrFilter);
    } catch (e) {
      if (attempt === 2) throw e;
      await new Promise(r => setTimeout(r, 500));
    }
  }
  return [];
}

// ===== TRAIT NORMALIZER =====
const TRAIT_ORDER = ['Type', 'Background', 'Body', 'Eyes', 'Head', 'Legend', 'Skin', 'Special'];
function normalizeTraits(attrs) {
  const groups = {};
  for (const k of TRAIT_ORDER) groups[k] = [];

  for (const t of (Array.isArray(attrs) ? attrs : [])) {
    const type = String(t?.trait_type ?? '').trim();
    const valStr = String(t?.value ?? '').trim();
    if (!type || !valStr) continue;
    if (valStr.toLowerCase() === 'none' || valStr.toLowerCase() === 'none (ignore)') continue;
    if (!Object.prototype.hasOwnProperty.call(groups, type)) groups[type] = [];
    groups[type].push({ value: valStr });
  }
  return groups;
}

// ===== HP-BASED TIERS =====
const NONLEG_HP_MIN = 455;   // lowest non-legend observed
const NONLEG_HP_MAX = 891;   // highest non-legend observed
const LEGEND_HP = 1000;
const CUT_UNCOMMON = 608; // ~45th percentile
const CUT_RARE     = 656; // ~70th percentile
const CUT_LEGEND   = 742; // ~95th percentile

function hpToTierLabel(hp) {
  if (hp >= LEGEND_HP) return 'Mythic';
  if (hp >= CUT_LEGEND) return 'Legendary';
  if (hp >= CUT_RARE)   return 'Rare';
  if (hp >= CUT_UNCOMMON) return 'Uncommon';
  return 'Common';
}

// ===== COLORS / THEME =====
const PALETTE = {
  cardBg: '#242623',
  frameStroke: '#000000',
  headerText: '#0F172A',
  rarityStripeByTier: {
    Mythic:   '#896936',
    Legendary:'#FFF1AE',
    Rare:     '#7ADDC0',
    Uncommon: '#7A83BF',
    Common:   '#B0DEEE',
  },
  artBackfill: '#FFFFFF',
  artStroke:   '#F9FAFB',
  traitsPanelBg:     '#b9dded',
  traitsPanelStroke: '#000000',
  traitCardFill:     '#FFFFFF',
  traitCardStroke:   '#000000',
  traitCardShadow:   '#0000001A',
  traitTitleText:    '#222625',
  traitValueText:    '#000000',
  footerText:        '#212524',
};

const UGLYDEX_PUBLIC_DIR = path.resolve(__dirname, '..', 'SquigUgly Card images', 'public');
const UGLYDEX_CARDS_DIR = path.join(UGLYDEX_PUBLIC_DIR, 'cards');

const CARD_BG_SOURCES = {
  Mythic: [
    path.join(UGLYDEX_CARDS_DIR, 'card_vide_Legendary.png'),
    'https://github.com/GuyLeDouce/UglyBot/blob/main/LEGENDARY%20BG.png?raw=true'
  ],
  Legendary: [
    path.join(UGLYDEX_CARDS_DIR, 'card_vide_Legendary.png'),
    'https://github.com/GuyLeDouce/UglyBot/blob/main/Stock%20BG.png?raw=true'
  ],
  Rare: [
    path.join(UGLYDEX_CARDS_DIR, 'card_vide_Rare.png'),
    'https://github.com/GuyLeDouce/UglyBot/blob/main/Stock%20BG.png?raw=true'
  ],
  Uncommon: [
    path.join(UGLYDEX_CARDS_DIR, 'card_vide_Common.png'),
    'https://github.com/GuyLeDouce/UglyBot/blob/main/Stock%20BG.png?raw=true'
  ],
  Common: [
    path.join(UGLYDEX_CARDS_DIR, 'card_vide_Common.png'),
    'https://github.com/GuyLeDouce/UglyBot/blob/main/Stock%20BG.png?raw=true'
  ]
};

function stripeFromRarity(label) {
  return PALETTE.rarityStripeByTier[label] || PALETTE.rarityStripeByTier.Common;
}
function hpToStripe(hp) { return stripeFromRarity(hpToTierLabel(hp)); }

const FONT_REG = (typeof FONT_REGULAR_FAMILY !== 'undefined' ? FONT_REGULAR_FAMILY : 'sans-serif');
const FONT_BOLD = (typeof FONT_BOLD_FAMILY !== 'undefined' ? FONT_BOLD_FAMILY : 'sans-serif');

function parseSimpleCsvLine(line) {
  const cells = String(line || '').split(/,(?=(?:[^"]*"[^"]*")*[^"]*$)/);
  return cells.map((cell) => {
    let out = String(cell ?? '').trim();
    if (out.startsWith('"') && out.endsWith('"')) out = out.slice(1, -1);
    return out.replace(/""/g, '"').trim();
  });
}

function loadHpTableFromCsv(csvFile) {
  const table = {};
  try {
    const raw = fs.readFileSync(csvFile, 'utf8');
    const lines = raw.split(/\r?\n/).filter(Boolean);
    if (lines.length <= 1) return table;
    for (let i = 1; i < lines.length; i++) {
      const [category, trait, pointsRaw] = parseSimpleCsvLine(lines[i]);
      const points = Number(pointsRaw);
      if (!category || !trait || !Number.isFinite(points)) continue;
      if (!table[category]) table[category] = {};
      table[category][trait] = points;
    }
  } catch (err) {
    console.warn(`⚠️ Could not load HP mapping CSV (${csvFile}):`, err.message);
  }
  return table;
}

// ====== HP SCORE TABLE + helpers ======
const HP_TABLE = {
  "Legend": {
    "Beige Giant Ears": 1000,
    "Beige Giant Head": 1000,
    "Beige Half Cut": 1000,
    "Beige Malformed": 1000,
    "Beige Zombie": 1000,
    "Brown Yeti": 1000,
    "Cornhuglyio": 1000,
    "Dark Brown Giant Ears": 1000,
    "Dark Brown Giant Head": 1000,
    "Dark Brown Half Cut": 1000,
    "Dark Brown Malformed": 1000,
    "Dark Brown Zombie": 1000,
    "Gold Halo": 1000,
    "Green Slime": 1000,
    "Green Zombie": 1000,
    "Monochrome": 1000,
    "Night": 1000,
    "Orange Zombie": 1000,
    "Pikachugly": 1000,
    "Purple Yeti": 1000,
    "Purple Zombie": 1000,
    "Robot": 1000,
    "Silver": 1000,
    "Sulks Elf": 1000,
    "Yellow Slime": 1000
  },
  "Type": {
    "Unknown": 50,
    "Squigs": 47,
    "Elf Squigs": 45
  },
  "Background": {
    "Unknown": 150,
    "Dark Blue Galaxy": 147,
    "Grey Boom": 144,
    "Grey Splash": 141,
    "Blue Splash": 138,
    "Purple Splash": 136,
    "Light Blue Boom": 134,
    "Yellow Splash": 132,
    "Green Boom": 130,
    "Light Blue Splash": 128,
    "Dark Blue Boom": 126,
    "Dark Blue Splash": 124,
    "Green Splash": 122,
    "Blue Boom": 120,
    "Purple Boom": 118,
    "Yellow Boom": 116,
    "Green": 114,
    "Purple": 112,
    "Dark Blue": 110,
    "Light Blue": 108,
    "Yellow": 106,
    "Grey": 104,
    "Blue": 102
  },
  "Body": {
    "Beavis Tee": 180,
    "Butthead Tee": 178,
    "Blue Sexy Bowling Shirt": 176,
    "Unknown": 174,
    "Super Ugly": 172,
    "Airplane Life Jacket": 170,
    "Cowboy Jacket": 168,
    "Futuristic Armor": 166,
    "Astronaut": 164,
    "Jedi": 162,
    "Sexy Bowling Shirt": 160,
    "Yellow Jersey": 158,
    "Rick Laser": 156,
    "Acupuncture": 154,
    "Ugly Food Shirt": 152,
    "Suit": 150,
    "Ugly Scene": 148,
    "Ugly Side of the Moon": 146,
    "Basketball Jersey": 144,
    "Mario Overalls": 142,
    "Pet Lover Bag": 140,
    "UGS Jacket": 138,
    "Ninja": 136,
    "Astronaut Blue": 134,
    "Maple Leafs 1967 Tracksuit": 132,
    "Caveman": 130,
    "White and Green Jacket": 128,
    "Maple Leafs 1967": 126,
    "Blue Wetsuit": 124,
    "Cheerleader": 122,
    "Sports Bra": 120,
    "Ugly Army Tank": 118,
    "Red Cowboy": 116,
    "Blue Cowboy": 114,
    "Gardener Overalls": 112,
    "PAAF White Tee": 110,
    "Indian": 108,
    "Red Baseball Shirt": 106,
    "Camo Wetsuit": 104,
    "Grey Bike Jersey": 102,
    "Prison Tee": 100,
    "Blue Baseball Jersey": 98,
    "Born Ugly Tee Tank": 96,
    "Red Varsity": 94,
    "White Ugly Tank": 92,
    "Long Sleeve Flame Tee": 90,
    "Monster Tee Bag": 88,
    "Yellow Tracksuit": 86,
    "Black Puffy": 84,
    "Bowling Shirt": 82,
    "Grease Tank": 80,
    "Light Green Tee": 78,
    "White and Blue Jacket": 76,
    "Green Varsity": 74,
    "Purple Hoodie": 72,
    "Beige Jacket": 70,
    "420 Purple Tracksuit": 68,
    "Beige Fisherman Jacket": 66,
    "Grey Fisherman Jacket": 64,
    "Pink Jacket": 62,
    "Borat": 60,
    "Blue Overalls": 58,
    "Flame Tee Tank": 56,
    "Hawaiian Shirt": 54,
    "Holey Tee": 52,
    "Yellow Puffy": 50,
    "Ugly Tank on Pink": 49,
    "Purple Shirt": 48,
    "Tattooed Body": 47,
    "Naked": 46,
    "Green Sweater": 45,
    "Black Sweater": 44,
    "Born Ugly Tee": 43,
    "Purple Tee": 42,
    "White Tee": 41,
    "Green Tee": 40
  },
  "Eyes": {
    "Unknown": 80,
    "Bionic": 78,
    "Bionic Lashes": 76,
    "Angry Cyclops": 74,
    "Sleepy Trio": 72,
    "Angry Cyclops Lashes": 70,
    "Angry Trio": 68,
    "Angry Trio Lashes": 66,
    "Cyclops": 64,
    "Cyclops Lashes": 62,
    "Trio": 61,
    "Trio Lashes": 60
  },
  "Head": {
    "Butthead Hair": 280,
    "Cape": 277,
    "Paper bag": 274,
    "Beavis Hair": 271,
    "Hairball Z": 268,
    "Buoy": 265,
    "Sheriff": 262,
    "Human Air": 259,
    "VR Headset": 256,
    "Tyre": 253,
    "Captured Piranha": 250,
    "Slasher": 247,
    "Crown": 244,
    "Unknown": 241,
    "UGS Delivery": 238,
    "Blindfold": 235,
    "Headphones": 232,
    "Hood": 229,
    "Indian": 226,
    "Diving Mask": 223,
    "Lobster": 220,
    "Green Visor": 217,
    "Acupuncture": 215,
    "Basketball": 213,
    "Elf Human Air": 211,
    "Watermelon": 209,
    "Elf Hood": 207,
    "Flower Power": 205,
    "Long Hair": 203,
    "Beer": 201,
    "Bunny": 199,
    "Imposter Mask": 197,
    "Fro Comb": 195,
    "Halo": 193,
    "Knife": 191,
    "Night Vision": 189,
    "Proud to be Ugly": 187,
    "Dinomite": 185,
    "Purr Paw": 183,
    "HeliHat": 181,
    "I Need TP": 179,
    "Panda Bike Helmet": 177,
    "Pot Head": 175,
    "Visor with Hair": 173,
    "Rainbow": 171,
    "Space Brain": 169,
    "Blonde Fro Comb": 167,
    "Zeus Hand": 165,
    "Captain": 163,
    "Pirate": 161,
    "Ski Mask": 159,
    "Dread Cap": 157,
    "Boom Bucket": 155,
    "Baseball": 153,
    "Green Cap": 151,
    "Honey Pot": 149,
    "Pomade": 147,
    "Bear Fur Hat": 145,
    "Ice Cream": 143,
    "Rastalocks": 141,
    "3D": 139,
    "Trucker": 137,
    "Umbrella Hat": 135,
    "Golfs": 133,
    "Green Ugly": 131,
    "Pink Beanie": 129,
    "Rice Dome": 127,
    "Head Canoe": 125,
    "Brain Bucket": 123,
    "Fiesta": 121,
    "Cactus": 119,
    "Fire": 117,
    "Floral": 115,
    "Green Mountie": 113,
    "Lemon Bucket": 111,
    "Notlocks": 109,
    "90's blonde": 107,
    "Bandana": 105,
    "Cowboy": 103,
    "Mountie": 101,
    "Yellow Beanie": 99,
    "90's Pink": 97,
    "Black Ugly": 95,
    "Twintails": 93,
    "Grey Cap": 91,
    "Cube Cut": 89,
    "Yellow Twintails": 87,
    "Afro": 85,
    "Green Beanie": 83,
    "Parted": 81,
    "Tin Topper": 79,
    "Bald": 77,
    "Blond Punk": 75,
    "Purple Punk": 73
  },
  "Skin": {
    "Unknown": 170,
    "Cristal": 167,
    "Cristal Elf": 164,
    "Green Camo": 161,
    "Purple Elf Space": 158,
    "Purple Space": 155,
    "Green Elf Camo": 152,
    "Green Elf": 149,
    "Green": 146,
    "Orange": 144,
    "Purple": 142,
    "Purple Elf": 140,
    "Orange Elf": 138,
    "Pink": 136,
    "Pink Elf": 134,
    "Brown": 132,
    "Beige": 130,
    "Beige Elf": 128,
    "Brown Elf": 126,
    "Dark Brown": 124,
    "Dark Brown Elf": 122
  },
  "Special": {
    "Laser Tits": 80,
    "Green Laser": 78,
    "Yellow Laser": 76,
    "Nouns Glasses": 74,
    "Ugly Necklace": 72,
    "Dino": 70,
    "Weed Necklace": 68,
    "Parakeet": 66,
    "Piranha": 64,
    "Smile Necklace": 62,
    "Monocle": 60,
    "Sad Smile Necklace": 59,
    "None": 0,
  }
};

const UGLY_HP_TABLE = loadHpTableFromCsv(path.join(__dirname, 'ugly_up.csv'));
const MONSTER_HP_TABLE = loadHpTableFromCsv(path.join(__dirname, 'monster_up.csv'));

function hpTableForContract(contractAddress, guildPointMappings = null) {
  const c = String(contractAddress || '').toLowerCase();
  const guildMap = guildPointMappings instanceof Map ? guildPointMappings.get(c) : null;
  if (guildMap && typeof guildMap === 'object' && Object.keys(guildMap).length > 0) return guildMap;
  if (c === UGLY_CONTRACT.toLowerCase()) return UGLY_HP_TABLE;
  if (c === MONSTER_CONTRACT.toLowerCase()) return MONSTER_HP_TABLE;
  return HP_TABLE;
}

function hpFor(cat, val, table = HP_TABLE) {
  const group = table?.[cat];
  if (!group) return 0;
  const key = String(val).trim();
  return Object.prototype.hasOwnProperty.call(group, key) ? group[key] : 0;
}

function computeHpFromTraits(groupedTraits, table = HP_TABLE) {
  let total = 0;
  const per = {};
  for (const cat of Object.keys(groupedTraits)) {
    for (const t of groupedTraits[cat]) {
      const s = hpFor(cat, t.value, table);
      total += s;
      per[`${cat}::${t.value}`] = s;
    }
  }
  return { total, per };
}

function findMatchingTraitDefinition(table, traitCategory, traitValue) {
  const desiredValue = String(traitValue || '').trim().toLowerCase();
  const desiredCategory = String(traitCategory || '').trim().toLowerCase();
  if (!desiredValue || !table || typeof table !== 'object') return null;

  for (const [category, traits] of Object.entries(table)) {
    if (!traits || typeof traits !== 'object') continue;
    if (desiredCategory && String(category).trim().toLowerCase() !== desiredCategory) continue;
    for (const trait of Object.keys(traits)) {
      if (String(trait).trim().toLowerCase() === desiredValue) {
        return { category, trait };
      }
    }
  }
  return null;
}

function hasTraitMatch(groupedTraits, traitCategory, traitValue) {
  const desiredValue = String(traitValue || '').trim().toLowerCase();
  const desiredCategory = String(traitCategory || '').trim().toLowerCase();
  if (!desiredValue || !groupedTraits || typeof groupedTraits !== 'object') return false;

  if (desiredCategory) {
    const entries = Array.isArray(groupedTraits[traitCategory]) ? groupedTraits[traitCategory] : [];
    return entries.some((t) => String(t?.value || '').trim().toLowerCase() === desiredValue);
  }

  for (const entries of Object.values(groupedTraits)) {
    if (!Array.isArray(entries)) continue;
    if (entries.some((t) => String(t?.value || '').trim().toLowerCase() === desiredValue)) return true;
  }
  return false;
}

// ──────────────────────────────────────────────────────────────────────────
// Rounded-corner harmony + bg over-mask trim
// ──────────────────────────────────────────────────────────────────────────
const RADIUS = {
  card: 38,
  header: 16,
  art: 26,
  traitsPanel: 18,
  traitCard: 16,
  pill: 22
};

async function drawCardBgWithoutBorder(ctx, W, H, tierLabel) {
  const bg = await loadBgByTier(tierLabel);
  if (!bg) {
    ctx.fillStyle = PALETTE.cardBg;
    ctx.fillRect(0, 0, W, H);
    return;
  }

  const TRIM_X = Math.round(bg.width  * 0.036);
  const TRIM_Y = Math.round(bg.height * 0.034);
  const sx = TRIM_X, sy = TRIM_Y;
  const sw = bg.width  - TRIM_X * 2;
  const sh = bg.height - TRIM_Y * 2;

  const OVER = (typeof MASK_EPS === 'number' ? MASK_EPS : 1.25);
  ctx.save();
  roundRectPath(ctx, -OVER, -OVER, W + OVER * 2, H + OVER * 2, RADIUS.card + 4);
  ctx.clip();

  const Z = Math.max(1, Number.isFinite(BG_ZOOM) ? BG_ZOOM : 1.06);
  ctx.save();
  ctx.translate(W / 2 + (BG_PAN_X || 0), H / 2 + (BG_PAN_Y || 0));
  ctx.scale(Z, Z);
  ctx.drawImage(bg, sx, sy, sw, sh, -W / 2, -H / 2, W, H);
  ctx.restore();
  ctx.restore();
}

// (font aliases defined earlier)

function hexToRgba(hex, a = 1) {
  const h = hex.replace('#', '');
  const v = h.length === 3 ? h.split('').map(c => c + c).join('') : h;
  const n = parseInt(v, 16);
  const r = (n >> 16) & 255, g = (n >> 8) & 255, b = n & 255;
  return `rgba(${r},${g},${b},${a})`;
}
function pillLabelForTier(label) {
  return String(label || '');
}

// ---------- image helpers / cache ----------
globalThis.__CARD_IMG_CACHE ||= {};
function isHttpUrl(value) {
  return typeof value === 'string' && /^https?:\/\//i.test(value);
}
async function loadImageCached(source) {
  if (globalThis.__CARD_IMG_CACHE[source]) return globalThis.__CARD_IMG_CACHE[source];
  const buf = await fetchBuffer(source);
  const img = await loadImage(buf);
  globalThis.__CARD_IMG_CACHE[source] = img;
  return img;
}
async function loadBgByTier(tier) {
  const list = CARD_BG_SOURCES[tier] || CARD_BG_SOURCES.Common;
  for (const source of list) {
    try {
      return await loadImageCached(source);
    } catch (e) {
      console.warn('BG load failed:', source, e.message);
    }
  }
  return null;
}
function pillTextColor() { return '#000000'; }
function drawRect(ctx, x, y, w, h, fill) { ctx.fillStyle = fill; ctx.fillRect(x, y, w, h); }
function roundRectPath(ctx, x, y, w, h, r) {
  const rr = Math.min(r, w / 2, h / 2);
  ctx.beginPath();
  ctx.moveTo(x + rr, y);
  ctx.arcTo(x + w, y, x + w, y + h, rr);
  ctx.arcTo(x + w, y + h, x, y + h, rr);
  ctx.arcTo(x, y + h, x, y, rr);
  ctx.arcTo(x, y, x + w, y, rr);
  ctx.closePath();
}
function drawRoundRect(ctx, x, y, w, h, r, fill) {
  roundRectPath(ctx, x, y, w, h, r);
  ctx.fillStyle = fill; ctx.fill();
}
function drawRoundRectShadow(ctx, x, y, w, h, r, fill, stroke, shadowColor = '#00000022', shadowBlur = 14, shadowDy = 2) {
  ctx.save();
  ctx.shadowColor = shadowColor;
  ctx.shadowBlur = shadowBlur;
  ctx.shadowOffsetX = 0;
  ctx.shadowOffsetY = shadowDy;
  drawRoundRect(ctx, x, y, w, h, r, fill);
  ctx.restore();
  if (stroke) { ctx.strokeStyle = stroke; ctx.lineWidth = 2; roundRectPath(ctx, x, y, w, h, r); ctx.stroke(); }
}
function drawTopRoundedRect(ctx, x, y, w, h, r, fill) {
  const rr = Math.min(r, w / 2, h);
  ctx.beginPath();
  ctx.moveTo(x + rr, y);
  ctx.arcTo(x + w, y, x + w, y + rr, rr);
  ctx.lineTo(x + w, y + h);
  ctx.lineTo(x,     y + h);
  ctx.lineTo(x,     y + rr);
  ctx.arcTo(x, y, x + rr, y, rr);
  ctx.closePath();
  ctx.fillStyle = fill;
  ctx.fill();
}
function cover(sw, sh, mw, mh) {
  const s = Math.max(mw / sw, mh / sh);
  const dw = Math.round(sw * s), dh = Math.round(sh * s);
  return { dx: Math.round((mw - dw) / 2), dy: Math.round((mh - dh) / 2), dw, dh };
}
function contain(sw, sh, mw, mh) {
  const s = Math.min(mw / sw, mh / sh);
  const dw = Math.round(sw * s), dh = Math.round(sh * s);
  const dx = Math.round((mw - dw) / 2);
  const dy = Math.round((mh - dh) / 2);
  return { dx, dy, dw, dh };
}
async function fetchBuffer(source) {
  if (isHttpUrl(source)) {
    const r = await fetch(source);
    if (!r.ok) throw new Error(`Image HTTP ${r.status}`);
    const ab = await r.arrayBuffer();
    return Buffer.from(ab);
  }
  return fs.promises.readFile(source);
}

// (stripeFromRarity, hpToStripe, pillLabelForTier defined earlier)

async function renderSquigCard__OLD({ name, tokenId, imageUrl, traits, rankInfo, rarityLabel, headerStripe }) {
  const W = 750, H = 1050;
  const SCALE = (typeof RENDER_SCALE !== 'undefined' ? RENDER_SCALE : 2);

  // Hi-DPI canvas
  const canvas = createCanvas(W * SCALE, H * SCALE);
  const ctx = canvas.getContext('2d');
  ctx.scale(SCALE, SCALE);
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = 'high';

  const tierLabel = (rarityLabel && String(rarityLabel)) || hpToTierLabel(rankInfo?.hpTotal || 0);
  const headerStripeFill = headerStripe || stripeFromRarity(tierLabel);

  // Background
  await drawCardBgWithoutBorder(ctx, W, H, tierLabel);

  // Layout knobs
  const HEADER_W        = 640;
  const HEADER_H        = 64;
  const HEADER_SIDE_PAD = 18;
  const HEADER_Y        = 20;

  const ART_W_MAX       = 560;

  // Rarity pill
  const PILL_H     = 62;
  const PILL_PAD_X = 24;
  const pillText   = pillLabelForTier(tierLabel);
  ctx.font         = `24px ${FONT_BOLD}`;
  const pTextW     = ctx.measureText(pillText).width;
  const pillW      = pTextW + PILL_PAD_X * 2;
  const PILL_MR    = 22;
  const PILL_MB    = 22;
  const pillX      = W - PILL_MR - pillW;
  const pillY      = H - PILL_MB - PILL_H;
  const pillCenterY = pillY + PILL_H / 2;

  // Title block
  const headerX = Math.round((W - HEADER_W) / 2);
  drawRoundRectShadow(
    ctx, headerX, HEADER_Y, HEADER_W, HEADER_H, RADIUS.header,
    headerStripeFill, null, 'rgba(0,0,0,0.16)', 14, 3
  );
  ctx.fillStyle = PALETTE.headerText;
  ctx.textBaseline = 'middle';
  const headerMidY = HEADER_Y + HEADER_H / 2;
  ctx.font = `32px ${FONT_BOLD}`;
  ctx.fillText(name, headerX + HEADER_SIDE_PAD, headerMidY);

  const hpText = `${rankInfo?.hpTotal ?? 0} UP`
  ctx.font = `26px ${FONT_BOLD}`;
  const hpW = ctx.measureText(hpText).width;
  ctx.fillText(hpText, headerX + HEADER_W - HEADER_SIDE_PAD - hpW, headerMidY);

  // Region between header and traits panel
  const headerBottom = HEADER_Y + HEADER_H;
  const TRAITS_W     = HEADER_W;
  const traitsBottom = pillCenterY;
  let TH = Math.round((traitsBottom - headerBottom) * 0.36);
  TH = Math.max(210, TH);

  const TX = Math.round((W - TRAITS_W) / 2);
  const TY = traitsBottom - TH;
  const TW = TRAITS_W;

  const midRegion = TY - headerBottom;
  const MIN_ART_H  = 380;
  const GAP_TARGET = 28;
  const GAP_MIN    = 16;

  let ART_W = Math.min(ART_W_MAX, W - 2 * (headerX - 20));
  let ART_H = ART_W;

  let G = GAP_TARGET;
  let maxArtH = midRegion - 2 * G;
  if (maxArtH < MIN_ART_H) {
    G = Math.max(GAP_MIN, Math.floor((midRegion - MIN_ART_H) / 2));
    maxArtH = midRegion - 2 * G;
  }
  ART_H = Math.min(ART_H, Math.max(100, maxArtH));
  ART_W = ART_H;

  const AX = Math.round((W - ART_W) / 2);
  const AY = Math.round(headerBottom + G);

  // Art card
  drawRoundRectShadow(
    ctx, AX, AY, ART_W, ART_H, RADIUS.art,
    PALETTE.artBackfill, null, 'rgba(0,0,0,0.14)', 14, 3
  );
  ctx.save();
  roundRectPath(ctx, AX, AY, ART_W, ART_H, RADIUS.art);
  ctx.clip();
  try {
    const img = await loadImage(await fetchBuffer(imageUrl));
    const { dx, dy, dw, dh } = contain(img.width, img.height, ART_W, ART_H);
ctx.drawImage(img, AX + dx, AY + dy, dw, dh);

  } catch {}
  ctx.restore();

  // Traits panel
  drawRoundRect(ctx, TX, TY, TW, TH, RADIUS.traitsPanel, hexToRgba(PALETTE.traitsPanelBg, 0.58));

  const PAD = 12, innerX = TX + PAD, innerY = TY + PAD, innerW = TW - PAD * 2, innerH = TH - PAD * 2;
  const COL_GAP = 12, COL_W = (innerW - COL_GAP) / 2;

  function layout(lineH = 16, titleH = 24, blockPad = 6) {
    const boxes = [];
    for (const cat of TRAIT_ORDER) {
      const items = (traits[cat] || []);
      if (!items.length) continue;

      const lines  = items.map(t => `${String(t.value)} (${hpFor(cat, t.value)} UP)`);
      const shown  = lines.slice(0, 5);
      const hidden = lines.length - shown.length;
      if (hidden > 0) shown.push(`+${hidden} more`);

      const rowsH = shown.length * lineH;
      const minRows = 32;
      const boxH = blockPad + titleH + Math.max(rowsH + 8, minRows) + blockPad;
      boxes.push({ cat, lines: shown, boxH, lineH, titleH, blockPad });
    }

    let yL = innerY, yR = innerY;
    const placed = [];
    for (const b of boxes) {
      const left = yL <= yR;
      const x = left ? innerX : innerX + COL_W + COL_GAP;
      const y = left ? yL : yR;
      placed.push({ ...b, x, y, w: COL_W });
      if (left) yL += b.boxH + COL_GAP; else yR += b.boxH + COL_GAP;
    }
    const usedH = Math.max(yL, yR) - innerY;

    if (usedH > innerH) {
      const scale = Math.max(0.82, innerH / usedH);
      return layout(
        Math.max(14, Math.floor(16 * scale)),
        Math.max(22, Math.floor(24 * scale)),
        Math.max(5,  Math.floor(6  * scale))
      );
    }
    return placed;
  }

  const placed = layout();

  const BUBBLE_R    = RADIUS.traitCard;
  const TAB_OVERLAP = 2;
  const TAB_EXTRA   = 3;
  const ROW_PAD_Y   = 6;

  for (const b of placed) {
    drawRoundRectShadow(
      ctx, b.x, b.y, b.w, b.boxH, BUBBLE_R,
      PALETTE.traitCardFill, null, 'rgba(0,0,0,0.14)', 12, 3
    );
    const tabH = b.titleH + TAB_OVERLAP + TAB_EXTRA;
    drawTopRoundedRect(ctx, b.x, b.y, b.w, tabH, BUBBLE_R, headerStripeFill);

    // Category
    ctx.fillStyle = PALETTE.traitTitleText;
    ctx.font = `16px ${FONT_BOLD}`;
    ctx.textBaseline = 'alphabetic';
    const mt = ctx.measureText(b.cat);
    const tH = (mt.actualBoundingBoxAscent || 0) + (mt.actualBoundingBoxDescent || 0);
    const titleY = b.y + (tabH - tH) / 2 + (mt.actualBoundingBoxAscent || 0);
    ctx.fillText(b.cat, b.x + (b.w - mt.width) / 2, titleY);

    // Values (centered)
    let yy = b.y + tabH + ROW_PAD_Y;
    ctx.fillStyle = PALETTE.traitValueText;
    ctx.font = `16px ${FONT_REG}`;
    ctx.textBaseline = 'middle';
    for (const line of b.lines) {
      const lw = ctx.measureText(line).width;
      ctx.fillText(line, b.x + (b.w - lw) / 2, yy + Math.floor(b.lineH / 2));
      yy += b.lineH;
    }
  }

  // Footer — centered between traits panel bottom and card bottom
  const footerY = Math.round((traitsBottom + H) / 2);
  ctx.fillStyle = PALETTE.footerText;
  ctx.font = `18px ${FONT_REG}`;
  ctx.textBaseline = 'middle';
  ctx.fillText(`Squigs • Token #${tokenId}`, 60, footerY);

  // Rarity pill
  drawRoundRectShadow(ctx, pillX, pillY, pillW, PILL_H, RADIUS.pill, headerStripeFill, null, 'rgba(0,0,0,0.14)', 12, 3);
  ctx.fillStyle = pillTextColor();
  ctx.textBaseline = 'middle';
  ctx.font = `24px ${FONT_BOLD}`;
  ctx.fillText(pillText, pillX + PILL_PAD_X, pillY + PILL_H / 2);

  return canvas.toBuffer('image/jpeg', { quality: 92 });

}


