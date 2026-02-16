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
  hasOpenSea: !!OPENSEA_API_KEY
});

// ===== CONTRACTS =====
const UGLY_CONTRACT    = '0x9492505633d74451bdf3079c09ccc979588bc309';
const MONSTER_CONTRACT = '0x1cD7fe72D64f6159775643ACEdc7D860dFB80348';
const SQUIGS_CONTRACT  = '0x9bf567ddf41b425264626d1b8b2c7f7c660b1c42';

// ===== CHARM DROPS =====
const CHARM_REWARD_CHANCE = 100; // 1 in 200
const CHARM_REWARDS = [150, 200, 350, 200]; // Weighted pool

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

async function ensureHoldersSchema() {
  await holdersPool.query(`
    CREATE TABLE IF NOT EXISTS wallet_links (
      guild_id TEXT NOT NULL,
      discord_id TEXT NOT NULL,
      wallet_address TEXT NOT NULL,
      drip_member_id TEXT,
      verified BOOLEAN NOT NULL DEFAULT FALSE,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await holdersPool.query(`ALTER TABLE wallet_links ADD COLUMN IF NOT EXISTS drip_member_id TEXT;`);
  await holdersPool.query(`CREATE UNIQUE INDEX IF NOT EXISTS wallet_links_guild_user_idx ON wallet_links (guild_id, discord_id);`);
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
      payout_type TEXT NOT NULL DEFAULT 'per_up',
      payout_amount NUMERIC NOT NULL DEFAULT 1,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await teamPool.query(`ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS drip_client_id TEXT;`);
  await teamPool.query(`ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS receipt_channel_id TEXT;`);
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
  console.log('✅ team schema ready');
}

ensureHoldersSchema().catch(e => console.error('Holders schema error:', e.message));
ensureTeamSchema().catch(e => console.error('Team schema error:', e.message));

async function setWalletLink(guildId, discordId, walletAddress, verified = false, dripMemberId = null) {
  await holdersPool.query(
    `INSERT INTO wallet_links (guild_id, discord_id, wallet_address, verified, drip_member_id)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (guild_id, discord_id) DO UPDATE
     SET wallet_address = EXCLUDED.wallet_address, verified = EXCLUDED.verified, drip_member_id = EXCLUDED.drip_member_id, updated_at = NOW()`,
    [guildId, discordId, walletAddress, verified, dripMemberId]
  );
}

async function getWalletLink(guildId, discordId) {
  const { rows } = await holdersPool.query(
    `SELECT wallet_address, verified, drip_member_id FROM wallet_links WHERE guild_id = $1 AND discord_id = $2`,
    [guildId, discordId]
  );
  return rows[0] || null;
}

// ===== Slash command registrar (guild-scoped for fast iteration) =====
function buildSlashCommands() {
  return [
    new SlashCommandBuilder()
      .setName('launch-verification')
      .setDescription('Post the public holder verification menu in this channel')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('setup-verification')
      .setDescription('Create/open a private admin setup channel for verification config')
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

function normalizeEthAddress(input) {
  const addr = String(input || '').trim();
  if (!/^0x[a-fA-F0-9]{40}$/.test(addr)) return null;
  return addr.toLowerCase();
}

function isAdmin(interaction) {
  const defaultAdmins = new Set(
    String(DEFAULT_ADMIN_USER)
      .split(',')
      .map(s => s.trim())
      .filter(Boolean)
  );
  if (defaultAdmins.has(String(interaction.user?.id || ''))) return true;
  return Boolean(interaction.memberPermissions?.has(PermissionFlagsBits.ManageGuild));
}

async function getGuildSettings(guildId) {
  const { rows } = await teamPool.query(`SELECT * FROM guild_settings WHERE guild_id = $1`, [guildId]);
  return rows[0] || null;
}

async function upsertGuildSetting(guildId, field, value) {
  const allowed = new Set(['drip_api_key', 'drip_client_id', 'drip_realm_id', 'currency_id', 'receipt_channel_id', 'payout_type', 'payout_amount']);
  if (!allowed.has(field)) throw new Error(`Invalid setting field: ${field}`);
  await teamPool.query(
    `INSERT INTO guild_settings (guild_id, ${field}, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (guild_id) DO UPDATE
     SET ${field} = EXCLUDED.${field}, updated_at = NOW()`,
    [guildId, value]
  );
}

async function getHolderRules(guildId) {
  const { rows } = await teamPool.query(
    `SELECT * FROM holder_rules WHERE guild_id = $1 AND enabled = TRUE ORDER BY id ASC`,
    [guildId]
  );
  return rows;
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

async function disableHolderRule(guildId, ruleId) {
  const { rows } = await teamPool.query(
    `UPDATE holder_rules SET enabled = FALSE WHERE guild_id = $1 AND id = $2 AND enabled = TRUE RETURNING id, role_name, contract_address, min_tokens, max_tokens`,
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

async function syncHolderRoles(member, walletAddress) {
  const rules = await getHolderRules(member.guild.id);
  if (!rules.length) return { changed: 0, applied: [], granted: [] };
  const me = member.guild.members.me;
  if (!me?.permissions?.has(PermissionFlagsBits.ManageRoles)) {
    return { changed: 0, applied: ['Skipped: bot is missing Manage Roles permission.'], granted: [] };
  }

  const byContract = new Map();
  for (const r of rules) {
    if (!byContract.has(r.contract_address)) byContract.set(r.contract_address, await countOwnedForContract(walletAddress, r.contract_address));
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
  return { changed, applied, granted };
}

async function hasClaimedToday(guildId, discordId) {
  const { rows } = await holdersPool.query(
    `SELECT id FROM claims WHERE guild_id = $1 AND discord_id = $2 AND claim_day = CURRENT_DATE LIMIT 1`,
    [guildId, discordId]
  );
  return rows.length > 0;
}

async function computeWalletStatsForPayout(guildId, walletAddress, payoutType) {
  const rules = await getHolderRules(guildId);
  const contracts = [...new Set(rules.map(r => String(r.contract_address || '').toLowerCase()).filter(Boolean))];
  if (!contracts.length) return { unitTotal: 0, totalNfts: 0, totalUp: 0 };

  const counts = await Promise.all(contracts.map(c => countOwnedForContract(walletAddress, c)));
  const totalNfts = counts.reduce((a, b) => a + b, 0);

  let totalUp = 0;
  if (payoutType === 'per_up') {
    const scorableContracts = contracts.filter((contractAddress) => {
      const table = hpTableForContract(contractAddress);
      return table && Object.keys(table).length > 0;
    });
    const perContractTotals = await mapLimit(scorableContracts, 2, async (contractAddress) => {
      const ids = await getOwnedTokenIdsForContract(walletAddress, contractAddress);
      const ups = await mapLimit(ids, 5, async (tokenId) => {
        try {
          const meta = await getNftMetadataAlchemy(tokenId, contractAddress);
          const { attrs } = await getTraitsForToken(meta, tokenId, contractAddress);
          const grouped = normalizeTraits(attrs);
          const table = hpTableForContract(contractAddress);
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
  };
}

function verificationMenuEmbed(guildName) {
  return new EmbedBuilder()
    .setTitle('Holder Verification')
    .setDescription(
      `Welcome to **${guildName}**.\n\n` +
      `Use the buttons below:\n` +
      `• **Connect Wallet**: link your wallet for holder verification.\n` +
      `• **Claim Rewards**: collect your daily holder payout.\n` +
      `• **Check NFT Stats**: view a Squig token's UP breakdown.\n` +
      `• **Disconnect**: remove your verification data from this server.`
    )
    .setColor(0x7ADDC0);
}

function verificationButtons() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('verify_connect').setLabel('Connect Wallet').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('verify_claim').setLabel('Claim Rewards').setStyle(ButtonStyle.Success),
    new ButtonBuilder().setCustomId('verify_check_stats').setLabel('Check NFT Stats').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('verify_disconnect').setLabel('Disconnect').setStyle(ButtonStyle.Danger),
  );
}

function setupMainEmbed() {
  return new EmbedBuilder()
    .setTitle('Holder Verification Setup')
    .setDescription(
      `Choose a setup action.\n` +
      `• Holder roles: add/remove contract role rules.\n` +
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

function setupMainButtons() {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('setup_add_rule').setLabel('Add Holder Role').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('setup_remove_rule').setLabel('Remove Holder Role').setStyle(ButtonStyle.Danger),
      new ButtonBuilder().setCustomId('setup_drip_menu').setLabel('Setup DRIP').setStyle(ButtonStyle.Secondary),
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
      new ButtonBuilder().setCustomId('setup_payout_type').setLabel('Set Payout Type').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('setup_payout_amount').setLabel('Set Payout Amount').setStyle(ButtonStyle.Secondary),
    ),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('setup_verify_drip').setLabel('Verify DRIP Connection').setStyle(ButtonStyle.Success),
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

  const pointsUrls = dripRealmBaseUrls(settings.drip_realm_id).map((baseUrl) => `${baseUrl}/points`);
  let pointsPayload = null;
  let pointsErr = null;
  for (const pointsUrl of pointsUrls) {
    const pointsRes = await fetchWithTimeout(pointsUrl, { timeoutMs: 15000, headers: buildDripHeaders(settings) });
    if (pointsRes.ok) {
      pointsPayload = await pointsRes.json().catch(() => ({}));
      pointsErr = null;
      break;
    }
    const body = await pointsRes.text().catch(() => '');
    pointsErr = `Points endpoint failed: HTTP ${pointsRes.status} ${body}`.slice(0, 320);
  }
  if (!pointsPayload) return { ok: false, reason: pointsErr || 'Points endpoint failed for unknown reason.' };

  const points = Array.isArray(pointsPayload?.data) ? pointsPayload.data : [];
  const configuredCurrency = String(settings.currency_id);
  const matchedCurrency = points.find(p => String(p?.id || '') === configuredCurrency);
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

async function handleClaim(interaction) {
  const guildId = interaction.guild.id;
  const link = await getWalletLink(guildId, interaction.user.id);
  if (!link?.wallet_address) {
    await interaction.reply({ content: 'Connect your wallet first.', flags: 64 });
    return;
  }
  if (await hasClaimedToday(guildId, interaction.user.id)) {
    await interaction.reply({ content: 'You already claimed today. Try again after UTC midnight.', flags: 64 });
    return;
  }

  const settings = (await getGuildSettings(guildId)) || { payout_type: 'per_up', payout_amount: 1 };
  const payoutType = settings.payout_type === 'per_nft' ? 'per_nft' : 'per_up';
  const payoutAmount = Number(settings.payout_amount || 0);
  const missing = [];
  if (!settings?.drip_api_key) missing.push('DRIP API Key');
  if (!settings?.drip_realm_id) missing.push('DRIP Realm ID');
  if (!settings?.currency_id) missing.push('Currency ID');
  if (missing.length > 0) {
    await interaction.reply({
      content: `Claim is not configured yet. Missing: ${missing.join(', ')}.`,
      flags: 64
    });
    return;
  }
  const stats = await computeWalletStatsForPayout(guildId, link.wallet_address, payoutType);
  const amount = Math.max(0, Math.floor(stats.unitTotal * payoutAmount));

  if (amount <= 0) {
    await interaction.reply({ content: 'No payout available. Check your holdings or payout settings.', flags: 64 });
    return;
  }

  try {
    let resolved = null;
    let dripMemberId = link.drip_member_id || null;
    try {
      resolved = await resolveDripMemberForDiscordUser(
        settings.drip_realm_id,
        interaction.user.id,
        link.wallet_address,
        settings
      );
      const resolvedPrimary = resolved?.member?.id || null;
      if (resolvedPrimary && resolvedPrimary !== dripMemberId) {
        dripMemberId = resolvedPrimary;
        await setWalletLink(guildId, interaction.user.id, link.wallet_address, Boolean(link.verified), dripMemberId);
      }
    } catch (resolveErr) {
      if (!dripMemberId) throw resolveErr;
    }

    const dripMemberIdCandidates = collectDripMemberIdCandidates(resolved?.member, dripMemberId);
    if (!dripMemberIdCandidates.length) {
      await interaction.reply({
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
    const dripResult = awardResult?.data || {};
    dripMemberId = awardResult?.usedMemberId || dripMemberIdCandidates[0];
    await setWalletLink(guildId, interaction.user.id, link.wallet_address, Boolean(link.verified), dripMemberId);

    const receiptChannelId = settings?.receipt_channel_id || RECEIPT_CHANNEL_ID;
    const receiptChannel = await interaction.guild.channels.fetch(receiptChannelId).catch(() => null);
    let receiptMessage = null;
    if (receiptChannel?.isTextBased()) {
      const earningBasis =
        payoutType === 'per_nft'
          ? `${stats.totalNfts} eligible NFT${stats.totalNfts === 1 ? '' : 's'}`
          : `${stats.totalUp} UglyPoints`;
      receiptMessage = await receiptChannel.send(
        `🧾 Claim Receipt\n` +
        `User: <@${interaction.user.id}>\n` +
        `Earning Basis: ${earningBasis}\n` +
        `Reward: **${amount} $CHARM**`
      );
    }

    await holdersPool.query(
      `INSERT INTO claims (guild_id, discord_id, claim_day, amount, wallet_address, receipt_channel_id, receipt_message_id)
       VALUES ($1, $2, CURRENT_DATE, $3, $4, $5, $6)`,
      [guildId, interaction.user.id, amount, link.wallet_address, receiptChannel?.id || null, receiptMessage?.id || null]
    );

    await interaction.reply({ content: `Claim complete. You received **${amount} $CHARM**.`, flags: 64 });
  } catch (err) {
    console.error('Claim processing error:', err);
    const msg = String(err?.message || err || '').trim();
    let reason = 'We could not process your claim right now.';
    if (/DRIP member search failed/i.test(msg)) reason = 'We could not verify your DRIP profile right now.';
    else if (/DRIP award failed/i.test(msg)) reason = 'The DRIP transfer did not complete. Please try again in a moment.';
    else if (/claims|wallet_links|database|relation|column/i.test(msg)) reason = 'Your claim could not be recorded due to a storage issue.';
    await interaction.reply({
      content: `Claim failed: ${reason}`,
      flags: 64
    });
  }
}

async function deleteUserVerificationData(guildId, discordId) {
  const deletedClaims = await holdersPool.query(
    `DELETE FROM claims WHERE guild_id = $1 AND discord_id = $2`,
    [guildId, discordId]
  );
  const deletedWallet = await holdersPool.query(
    `DELETE FROM wallet_links WHERE guild_id = $1 AND discord_id = $2`,
    [guildId, discordId]
  );
  return { claims: deletedClaims.rowCount || 0, wallets: deletedWallet.rowCount || 0 };
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
      return;
    }

    if (interaction.isButton()) {
      if (interaction.customId === 'verify_connect') {
        const modal = new ModalBuilder().setCustomId('verify_connect_modal').setTitle('Connect Wallet');
        modal.addComponents(
          new ActionRowBuilder().addComponents(
            new TextInputBuilder()
              .setCustomId('wallet_address')
              .setLabel('Ethereum wallet address')
              .setRequired(true)
              .setPlaceholder('0x...')
              .setStyle(TextInputStyle.Short)
          )
        );
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === 'verify_claim') {
        await handleClaim(interaction);
        return;
      }

      if (interaction.customId === 'verify_check_stats') {
        const modal = new ModalBuilder().setCustomId('verify_check_stats_modal').setTitle('Check NFT Stats');
        modal.addComponents(
          new ActionRowBuilder().addComponents(
            new TextInputBuilder()
              .setCustomId('token_id')
              .setLabel('Squig token ID')
              .setRequired(true)
              .setPlaceholder('1234')
              .setStyle(TextInputStyle.Short)
          )
        );
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === 'verify_disconnect') {
        const row = new ActionRowBuilder().addComponents(
          new ButtonBuilder().setCustomId('verify_disconnect_confirm').setLabel('Confirm Disconnect').setStyle(ButtonStyle.Danger),
          new ButtonBuilder().setCustomId('verify_disconnect_cancel').setLabel('Cancel').setStyle(ButtonStyle.Secondary),
        );
        await interaction.reply({
          content:
            'This will remove your verification data and holder roles for this server.\n' +
            'Are you sure you want to continue?',
          components: [row],
          flags: 64
        });
        return;
      }

      if (interaction.customId === 'verify_disconnect_cancel') {
        await interaction.update({
          content: 'Disconnect canceled.',
          components: []
        });
        return;
      }

      if (interaction.customId === 'verify_disconnect_confirm') {
        const result = await deleteUserVerificationData(interaction.guild.id, interaction.user.id);
        let removedRoles = 0;
        try {
          const member = await interaction.guild.members.fetch(interaction.user.id);
          removedRoles = await removeHolderRolesFromMember(member);
        } catch {}
        await interaction.update({
          content:
            `Disconnected successfully.\n` +
            `Removed holder roles: ${removedRoles}\n` +
            `Your verification records were cleared for this server.`,
          components: []
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
        const modal = new ModalBuilder().setCustomId('setup_add_rule_modal').setTitle('Add Holder Role Rule');
        modal.addComponents(
          new ActionRowBuilder().addComponents(new TextInputBuilder().setCustomId('contract_address').setLabel('Contract address').setRequired(true).setStyle(TextInputStyle.Short)),
          new ActionRowBuilder().addComponents(new TextInputBuilder().setCustomId('min_tokens').setLabel('Min tokens (inclusive)').setRequired(true).setStyle(TextInputStyle.Short)),
          new ActionRowBuilder().addComponents(new TextInputBuilder().setCustomId('max_tokens').setLabel('Max tokens (optional)').setRequired(false).setStyle(TextInputStyle.Short)),
        );
        await interaction.showModal(modal);
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

      if (interaction.customId === 'setup_drip_key' || interaction.customId === 'setup_client_id' || interaction.customId === 'setup_realm_id' || interaction.customId === 'setup_currency_id' || interaction.customId === 'setup_receipt_channel' || interaction.customId === 'setup_payout_amount') {
        const fieldMap = {
          setup_drip_key: ['setup_drip_key_modal', 'DRIP API Key', 'drip_api_key', 'API key'],
          setup_client_id: ['setup_client_id_modal', 'DRIP Client ID', 'drip_client_id', 'Client ID'],
          setup_realm_id: ['setup_realm_id_modal', 'DRIP Realm ID', 'drip_realm_id', 'Realm ID'],
          setup_currency_id: ['setup_currency_id_modal', 'Currency ID', 'currency_id', 'Currency ID'],
          setup_receipt_channel: ['setup_receipt_channel_modal', 'Receipt Channel ID', 'receipt_channel_id', 'Channel ID'],
          setup_payout_amount: ['setup_payout_amount_modal', 'Payout Amount', 'payout_amount', 'Number'],
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
        const row = new ActionRowBuilder().addComponents(
          new StringSelectMenuBuilder()
            .setCustomId('setup_payout_type_select')
            .setPlaceholder('Select payout type')
            .addOptions(
              { label: 'Per NFT', value: 'per_nft', description: 'Payout = owned NFT count x payout amount' },
              { label: 'Per UglyPoint', value: 'per_up', description: 'Payout = total UP x payout amount' },
            )
        );
        await interaction.reply({ content: 'Choose payout type:', components: [row], flags: 64 });
        return;
      }

      if (interaction.customId === 'setup_verify_drip') {
        const settings = await getGuildSettings(interaction.guild.id);
        const result = await verifyDripConnection(settings, interaction.user.id);
        if (!result.ok) {
          await interaction.reply({
            flags: 64,
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
        await interaction.reply({
          flags: 64,
          content:
            `DRIP verification passed.\n` +
            `Realm: \`${settings.drip_realm_id}\`\n` +
            `Currency: \`${settings.currency_id}\` (${currencyLabel})\n` +
            `Realm points loaded: ${result.pointsCount}\n` +
            `${memberProbeText}`
        });
        return;
      }

      if (interaction.customId === 'setup_view') {
        const settings = await getGuildSettings(interaction.guild.id);
        const rules = await getHolderRules(interaction.guild.id);
        await interaction.reply({
          flags: 64,
          content:
            `Settings:\n` +
            `- DRIP API Key: ${settings?.drip_api_key ? 'set' : 'not set'}\n` +
            `- DRIP Client ID: ${settings?.drip_client_id ? 'set' : 'not set'}\n` +
            `- DRIP Realm ID: ${settings?.drip_realm_id || 'not set'}\n` +
            `- Currency ID: ${settings?.currency_id || 'not set'}\n` +
            `- Receipt Channel ID: ${settings?.receipt_channel_id || RECEIPT_CHANNEL_ID}\n` +
            `- Payout Type: ${settings?.payout_type || 'per_up'}\n` +
            `- Payout Amount: ${settings?.payout_amount || 1}\n\n` +
            `Rules (${rules.length}):\n` +
            `${rules.map(r => `- ${r.role_name}: ${r.contract_address} (${r.min_tokens}-${r.max_tokens ?? '∞'})`).join('\n') || '- none'}`
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
      if (!pending) {
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

    if (interaction.isStringSelectMenu() && interaction.customId === 'setup_payout_type_select') {
      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }
      await upsertGuildSetting(interaction.guild.id, 'payout_type', interaction.values[0]);
      await interaction.update({ content: `Payout type set to \`${interaction.values[0]}\`.`, components: [] });
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

    if (interaction.isModalSubmit()) {
      if (interaction.customId === 'verify_connect_modal') {
        const raw = interaction.fields.getTextInputValue('wallet_address');
        const addr = normalizeEthAddress(raw);
        if (!addr) {
          await interaction.reply({ content: 'Invalid Ethereum address.', flags: 64 });
          return;
        }

        const settings = await getGuildSettings(interaction.guild.id);
        let dripMemberId = null;
        let dripStatus = 'DRIP profile check unavailable (not configured).';
        if (settings?.drip_api_key && settings?.drip_realm_id) {
          try {
            const resolved = await resolveDripMemberForDiscordUser(
              settings.drip_realm_id,
              interaction.user.id,
              addr,
              settings
            );
            const dripMember = resolved.member;
            dripMemberId = dripMember?.id || null;
            dripStatus = dripMemberId
              ? 'DRIP profile linked.'
              : 'DRIP profile not linked in this realm yet.';
          } catch (err) {
            console.error('DRIP lookup during connect failed:', err);
            dripStatus = 'DRIP profile check is temporarily unavailable.';
          }
        }

        await setWalletLink(interaction.guild.id, interaction.user.id, addr, false, dripMemberId);
        const member = await interaction.guild.members.fetch(interaction.user.id);
        const sync = await syncHolderRoles(member, addr);
        await interaction.reply({
          flags: 64,
          content:
            `Wallet connected successfully.\n` +
            `${dripStatus}\n` +
            `Role sync complete (${sync.changed} change${sync.changed === 1 ? '' : 's'}).\n` +
            `${sync.granted?.length ? `Roles granted: ${sync.granted.join(', ')}\n` : 'Roles granted: none\n'}` +
            `${sync.applied.length ? sync.applied.join('\n') : 'No holder roles matched yet.'}`
        });
        return;
      }

      if (interaction.customId === 'verify_check_stats_modal') {
        const tokenId = String(interaction.fields.getTextInputValue('token_id') || '').trim();
        if (!/^\d+$/.test(tokenId)) {
          await interaction.reply({ content: 'Token ID must be numeric.', flags: 64 });
          return;
        }
        const meta = await getNftMetadataAlchemy(tokenId);
        const { attrs } = await getTraitsForToken(meta, tokenId);
        const grouped = normalizeTraits(attrs);
        const hpAgg = computeHpFromTraits(grouped);
        const tier = hpToTierLabel(hpAgg.total || 0);
        const imageUrl = `https://assets.bueno.art/images/a49527dc-149c-4cbc-9038-d4b0d1dbf0b2/default/${tokenId}`;
        await interaction.reply({
          flags: 64,
          embeds: [
            new EmbedBuilder()
              .setTitle(`Squig #${tokenId}`)
              .setDescription(`Total UP: **${hpAgg.total || 0}**\nRarity: **${tier}**`)
              .setImage(imageUrl)
              .setColor(0x7A83BF)
          ]
        });
        return;
      }

      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }

      if (interaction.customId === 'setup_add_rule_modal') {
        const contractAddress = normalizeEthAddress(interaction.fields.getTextInputValue('contract_address'));
        const minTokens = Number(interaction.fields.getTextInputValue('min_tokens'));
        const maxRaw = String(interaction.fields.getTextInputValue('max_tokens') || '').trim();
        const maxTokens = maxRaw === '' ? null : Number(maxRaw);
        if (!contractAddress) {
          await interaction.reply({ content: 'Invalid contract address.', flags: 64 });
          return;
        }
        if (!Number.isInteger(minTokens) || minTokens < 0 || (maxTokens != null && (!Number.isInteger(maxTokens) || maxTokens < minTokens))) {
          await interaction.reply({ content: 'Invalid min/max token values.', flags: 64 });
          return;
        }
        const key = `${interaction.guild.id}:${interaction.user.id}`;
        globalThis.__PENDING_HOLDER_RULES.set(key, { contractAddress, minTokens, maxTokens, createdAt: Date.now() });
        const row = new ActionRowBuilder().addComponents(
          new RoleSelectMenuBuilder()
            .setCustomId('setup_add_rule_role_select')
            .setPlaceholder('Select the holder role')
            .setMinValues(1)
            .setMaxValues(1)
        );
        await interaction.reply({
          content: `Now select which role should be assigned for \`${contractAddress}\` (${minTokens}-${maxTokens ?? '∞'}):`,
          components: [row],
          flags: 64
        });
        return;
      }

      const settingModalMap = {
        setup_drip_key_modal: 'drip_api_key',
        setup_client_id_modal: 'drip_client_id',
        setup_realm_id_modal: 'drip_realm_id',
        setup_currency_id_modal: 'currency_id',
        setup_receipt_channel_modal: 'receipt_channel_id',
        setup_payout_amount_modal: 'payout_amount',
      };
      const field = settingModalMap[interaction.customId];
      if (field) {
        const value = interaction.fields.getTextInputValue(field).trim();
        if (field === 'payout_amount') {
          const n = Number(value);
          if (!Number.isFinite(n) || n < 0) {
            await interaction.reply({ content: 'Payout amount must be a non-negative number.', flags: 64 });
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

function hpTableForContract(contractAddress) {
  const c = String(contractAddress || '').toLowerCase();
  if (c === UGLY_CONTRACT.toLowerCase()) return UGLY_HP_TABLE;
  if (c === MONSTER_CONTRACT.toLowerCase()) return MONSTER_HP_TABLE;
  return HP_TABLE;
}

function hpFor(cat, val, table = HP_TABLE) {
  const group = table?.[cat];
  if (!group) return 0;
  const key = Object.keys(group).find(
    k => k.toLowerCase() === String(val).trim().toLowerCase()
  );
  return key ? group[key] : 0;
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


