// Load local .env only outside Railway. Railway must use its injected variables;
// silently falling back to a checked-out .env can hide a missing/rotated key.
const IS_RAILWAY_RUNTIME = Boolean(
  process.env.RAILWAY_ENVIRONMENT_ID ||
  process.env.RAILWAY_PROJECT_ID ||
  process.env.RAILWAY_SERVICE_ID
);
if (!IS_RAILWAY_RUNTIME) {
  try { require('dotenv').config(); } catch (_) {}
}


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
  ChannelSelectMenuBuilder,
  SlashCommandBuilder,
  AttachmentBuilder,
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
const portalEvent = require('./modules/portalEvent');
const marketplaceCommand = require('./modules/marketplaceCommand');
const mawEvent = require('./modules/mawEvent');
const squigDuels = require('./modules/squigDuels');
const {
  MAW_EXPECTED_TOKEN_COUNT,
  loadMawRankingIndex,
  getMawRewardQuote,
  formatMawAverageRank,
} = require('./modules/mawRarity');
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
const DRIP_INITIATOR_ID = String(process.env.DRIP_INITIATOR_ID || '').trim();
const DRIP_SENDER_ID = String(process.env.DRIP_SENDER_ID || process.env.DRIP_TRANSFER_SENDER_ID || '').trim();
const DEFAULT_DRIP_MEMBER_SENDER_ID = String(process.env.DRIP_DEFAULT_MEMBER_SENDER_ID || '').trim();
const DRIP_AUTO_VERIFY_SEED_AMOUNT = Math.max(0, Math.floor(Number(process.env.DRIP_AUTO_VERIFY_SEED_AMOUNT || 0)));
const DRIP_CREDENTIAL_BOOTSTRAP_AMOUNT = Math.max(
  0,
  Math.floor(Number(process.env.DRIP_CREDENTIAL_BOOTSTRAP_AMOUNT ?? process.env.DRIP_AUTO_VERIFY_SEED_AMOUNT ?? 1))
);
const DRIP_CREDENTIAL_SENDER_TYPE = String(process.env.DRIP_CREDENTIAL_SENDER_TYPE || 'discord-id').trim().toLowerCase();
const DRIP_CREDENTIAL_SENDER_VALUE = String(
  process.env.DRIP_CREDENTIAL_SENDER_VALUE ||
  process.env.DRIP_CREDENTIAL_SENDER_ID ||
  DISCORD_CLIENT_ID ||
  ''
).trim();
const DRIP_CREDENTIAL_SENDER_SOURCE = String(process.env.DRIP_CREDENTIAL_SENDER_SOURCE || '').trim();
const DRIP_CREDENTIAL_BOOTSTRAP_TARGET_TYPE = String(process.env.DRIP_CREDENTIAL_BOOTSTRAP_TARGET_TYPE || 'wallet').trim().toLowerCase();

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

const DOCTOR_NOTE_IMAGE_PATH = path.join(__dirname, 'ChatGPT Image Jun 19, 2026, 10_14_33 AM.png');
const MINT_TODAY_IMAGE_PATH = path.join(__dirname, 'Mint today.jpg');
const MINT_TODAY_ATTACHMENT_NAME = 'mint-today.jpg';
const DOCTOR_NAME_BOX = {
  referenceWidth: 1368,
  referenceHeight: 1088,
  centerX: 545,
  baselineY: 354,
  maxWidth: 345,
  maxFontSize: 26,
  minFontSize: 14,
};

function cleanDoctorDisplayName(value, fallback = 'Anonymous') {
  const cleaned = String(value || '')
    .replace(/[\r\n\t]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return cleaned || fallback;
}

function fitTextToWidth(ctx, text, maxWidth, {
  maxFontSize = 26,
  minFontSize = 14,
  fontFamily = 'Arial, Helvetica, sans-serif',
  weight = 500,
} = {}) {
  let fontSize = maxFontSize;
  while (fontSize > minFontSize) {
    ctx.font = `${weight} ${fontSize}px ${fontFamily}`;
    if (ctx.measureText(text).width <= maxWidth) {
      return { text, fontSize };
    }
    fontSize -= 1;
  }

  ctx.font = `${weight} ${minFontSize}px ${fontFamily}`;
  if (ctx.measureText(text).width <= maxWidth) {
    return { text, fontSize: minFontSize };
  }

  let trimmed = text;
  while (trimmed.length > 1 && ctx.measureText(`${trimmed}...`).width > maxWidth) {
    trimmed = trimmed.slice(0, -1);
  }
  return { text: `${trimmed}...`, fontSize: minFontSize };
}

async function renderDoctorNote(displayName) {
  const bg = await loadImage(DOCTOR_NOTE_IMAGE_PATH);
  const canvas = createCanvas(bg.width, bg.height);
  const ctx = canvas.getContext('2d');
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = 'high';
  ctx.drawImage(bg, 0, 0);

  const scaleX = bg.width / DOCTOR_NAME_BOX.referenceWidth;
  const scaleY = bg.height / DOCTOR_NAME_BOX.referenceHeight;
  const maxFontSize = Math.round(DOCTOR_NAME_BOX.maxFontSize * scaleY);
  const minFontSize = Math.round(DOCTOR_NAME_BOX.minFontSize * scaleY);
  const safeName = cleanDoctorDisplayName(displayName);
  const fitted = fitTextToWidth(ctx, safeName, DOCTOR_NAME_BOX.maxWidth * scaleX, {
    maxFontSize,
    minFontSize,
  });

  ctx.font = `500 ${fitted.fontSize}px Arial, Helvetica, sans-serif`;
  ctx.fillStyle = '#171717';
  ctx.textAlign = 'center';
  ctx.textBaseline = 'alphabetic';
  ctx.fillText(
    fitted.text,
    DOCTOR_NAME_BOX.centerX * scaleX,
    DOCTOR_NAME_BOX.baselineY * scaleY
  );

  return canvas.toBuffer('image/png');
}

async function buildDoctorNotePayload(displayName) {
  const noteBuffer = await renderDoctorNote(displayName);
  return {
    files: [
      new AttachmentBuilder(noteBuffer, {
        name: 'doctor-note.png',
      }),
    ],
  };
}

// Debug env (safe booleans/ids only)
console.log('ENV CHECK:', {
  hasToken: !!DISCORD_TOKEN,
  clientId: DISCORD_CLIENT_ID,
  guildId: GUILD_ID,
  hasAlchemy: !!ALCHEMY_API_KEY,
  hasOpenSea: !!OPENSEA_API_KEY,
  hasPointsDb: !!process.env.DATABASE_URL_POINTS,
  hasClaimsDb: !!process.env.DATABASE_URL_CLAIMS,
  hasPrizesDb: !!process.env.DATABASE_URL_PRIZES
});

// ===== CONTRACTS =====
const UGLY_CONTRACT    = '0x9492505633d74451bdf3079c09ccc979588bc309';
const MONSTER_CONTRACT = '0x1cD7fe72D64f6159775643ACEdc7D860dFB80348';
const OG_SQUIGS_CONTRACT = '0x9bf567ddf41b425264626d1b8b2c7f7c660b1c42';
const SQUIGS_CONTRACT  = String(process.env.SQUIG_COLLECTION_CONTRACT || '0x8c9a02c0585200c4c65608df6b8def543d33792a').toLowerCase();
const SQUIG_IMAGE_BASE_URL = String(process.env.SQUIG_IMAGE_BASE_URL || '').replace(/\/+$/, '');
const OG_SQUIG_IMAGE_BASE_URL = String(process.env.OG_SQUIG_IMAGE_BASE_URL || 'https://assets.bueno.art/images/a49527dc-149c-4cbc-9038-d4b0d1dbf0b2/default').replace(/\/+$/, '');
const DEFAULT_NFT_CHAIN = 'ethereum';
const SQUIGS_CHAIN = String(process.env.SQUIG_COLLECTION_CHAIN || process.env.SQUIG_CHAIN || DEFAULT_NFT_CHAIN).trim().toLowerCase();
const NFT_CHAIN_CONFIG = {
  ethereum: {
    label: 'Ethereum',
    alchemyNetwork: 'eth-mainnet',
    openseaChain: 'ethereum',
    explorerBaseUrl: 'https://etherscan.io',
  },
  base: {
    label: 'Base',
    alchemyNetwork: 'base-mainnet',
    openseaChain: 'base',
    explorerBaseUrl: 'https://basescan.org',
  },
  abstract: {
    label: 'Abstract',
    alchemyNetwork: 'abstract-mainnet',
    openseaChain: 'abstract',
    explorerBaseUrl: 'https://abscan.org',
  },
};
const NFT_CHAIN_ALIASES = {
  eth: 'ethereum',
  ethereum: 'ethereum',
  mainnet: 'ethereum',
  base: 'base',
  abstract: 'abstract',
  abs: 'abstract',
};

// ===== CHARM DROPS =====
const CHARM_REWARD_CHANCE = 100; // 1 in 200
const CHARM_REWARDS = [150, 200, 350, 200]; // Weighted pool
const SUPPORT_TICKET_CHANNEL_ID = '1324090267699122258';
const ADMIN_LOG_CHANNEL_ID = '1477463175665287410';
const DAILY_HOLDER_REFRESH_ENABLED = String(process.env.DAILY_HOLDER_REFRESH_ENABLED || 'true').toLowerCase() !== 'false';
const DAILY_HOLDER_REFRESH_INTERVAL_MS = Math.max(
  60 * 60 * 1000,
  Number(process.env.DAILY_HOLDER_REFRESH_INTERVAL_HOURS || 24) * 60 * 60 * 1000
);
const DAILY_HOLDER_REFRESH_START_DELAY_MS = Math.max(
  0,
  Number(process.env.DAILY_HOLDER_REFRESH_START_DELAY_MINUTES ?? 5) * 60 * 1000
);

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

function redactProviderUrl(url) {
  return String(url || '')
    .replace(/(\.g\.alchemy\.com\/(?:nft\/v3|v2)\/)[^/?]+/gi, '$1[redacted]')
    .replace(/([?&](?:api[_-]?key|apikey|key)=)[^&]+/gi, '$1[redacted]');
}

function sanitizeProviderErrorDetail(value) {
  let detail = String(value || '');
  if (ALCHEMY_API_KEY) detail = detail.replaceAll(String(ALCHEMY_API_KEY), '[redacted]');
  if (OPENSEA_API_KEY) detail = detail.replaceAll(String(OPENSEA_API_KEY), '[redacted]');
  return detail.replace(/\s+/g, ' ').trim().slice(0, 500);
}

class HttpStatusError extends Error {
  constructor(status, url, retryAfterMs = 0, responseDetail = '') {
    const detail = sanitizeProviderErrorDetail(responseDetail);
    super(`HTTP ${status}${detail ? `: ${detail}` : ''}`);
    this.name = 'HttpStatusError';
    this.status = status;
    this.url = redactProviderUrl(url);
    this.retryAfterMs = retryAfterMs;
    this.responseDetail = detail;
  }
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function numberFromEnv(name, fallback) {
  const value = Number(process.env[name]);
  return Number.isFinite(value) ? value : fallback;
}

const ALCHEMY_REQUEST_INTERVAL_MS = Math.max(
  0,
  numberFromEnv('ALCHEMY_REQUEST_INTERVAL_MS', 350)
);
const ALCHEMY_FETCH_RETRIES = Math.max(3, numberFromEnv('ALCHEMY_FETCH_RETRIES', 7));
let nextAlchemyRequestAt = 0;

function isAlchemyUrl(url) {
  try {
    return new URL(String(url)).hostname.endsWith('.g.alchemy.com');
  } catch {
    return false;
  }
}

function retryAfterToMs(value) {
  if (!value) return 0;
  const seconds = Number(value);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
  const at = Date.parse(value);
  if (!Number.isFinite(at)) return 0;
  return Math.max(0, at - Date.now());
}

async function waitForAlchemySlot() {
  if (!ALCHEMY_REQUEST_INTERVAL_MS) return;
  const now = Date.now();
  const waitMs = Math.max(0, nextAlchemyRequestAt - now);
  nextAlchemyRequestAt = Math.max(now, nextAlchemyRequestAt) + ALCHEMY_REQUEST_INTERVAL_MS;
  if (waitMs) await sleep(waitMs);
}

function reserveAlchemyCooldown(ms) {
  if (ms > 0) {
    nextAlchemyRequestAt = Math.max(nextAlchemyRequestAt, Date.now() + ms);
  }
}

function isRetriableHttpStatus(status) {
  return status === 429 || status === 408 || status >= 500;
}

const fetchWithRetry = async (url, retries = 3, delay = 1000, opts = {}) => {
  const alchemyRequest = isAlchemyUrl(url);
  const maxAttempts = alchemyRequest ? Math.max(retries, ALCHEMY_FETCH_RETRIES) : retries;
  const { timeout, timeoutMs, ...fetchOpts } = opts || {};
  const parsedTimeoutMs = Number(timeoutMs || timeout || 10000);
  const requestTimeoutMs = Number.isFinite(parsedTimeoutMs) ? parsedTimeoutMs : 10000;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      if (alchemyRequest) await waitForAlchemySlot();
      const res = await fetchWithTimeout(url, { timeoutMs: requestTimeoutMs, ...fetchOpts });
      if (!res.ok) {
        const retryAfterMs = retryAfterToMs(res.headers?.get?.('retry-after'));
        const responseDetail = await res.text().catch(() => '');
        if (alchemyRequest && res.status === 429) reserveAlchemyCooldown(retryAfterMs || delay);
        throw new HttpStatusError(res.status, url, retryAfterMs, responseDetail);
      }
      return res;
    } catch (err) {
      const status = err?.status || null;
      const retriable = !status || isRetriableHttpStatus(status);
      if (!retriable || attempt === maxAttempts - 1) throw err;

      const exponentialDelay = Math.min(30000, delay * Math.pow(2, attempt));
      const retryAfterDelay = Number(err?.retryAfterMs || 0);
      const jitter = Math.floor(Math.random() * 250);
      await sleep(Math.max(exponentialDelay, retryAfterDelay) + jitter);
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
const DATABASE_URL_CLAIMS = process.env.DATABASE_URL_CLAIMS || null;
const DATABASE_URL_PRIZES = process.env.DATABASE_URL_PRIZES || null;
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

const claimsPool = DATABASE_URL_CLAIMS
  ? new Pool(
      { connectionString: DATABASE_URL_CLAIMS, ssl: PGSSL ? { rejectUnauthorized: false } : false }
    )
  : holdersPool;

const prizesPool = DATABASE_URL_PRIZES
  ? new Pool(
      { connectionString: DATABASE_URL_PRIZES, ssl: PGSSL ? { rejectUnauthorized: false } : false }
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
      chain TEXT NOT NULL DEFAULT 'ethereum',
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
      chain TEXT NOT NULL DEFAULT 'ethereum',
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
      chain TEXT NOT NULL DEFAULT 'ethereum',
      contract_address TEXT NOT NULL,
      enabled BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await teamPool.query(`ALTER TABLE holder_rules ADD COLUMN IF NOT EXISTS chain TEXT NOT NULL DEFAULT 'ethereum';`);
  await teamPool.query(`ALTER TABLE trait_role_rules ADD COLUMN IF NOT EXISTS chain TEXT NOT NULL DEFAULT 'ethereum';`);
  await teamPool.query(`ALTER TABLE holder_collections ADD COLUMN IF NOT EXISTS chain TEXT NOT NULL DEFAULT 'ethereum';`);
  await teamPool.query(`UPDATE holder_rules SET chain = 'ethereum' WHERE chain IS NULL OR chain = '';`);
  await teamPool.query(`UPDATE trait_role_rules SET chain = 'ethereum' WHERE chain IS NULL OR chain = '';`);
  await teamPool.query(`UPDATE holder_collections SET chain = 'ethereum' WHERE chain IS NULL OR chain = '';`);
  await teamPool.query(`DROP INDEX IF EXISTS holder_collections_guild_contract_uidx;`);
  await teamPool.query(`CREATE UNIQUE INDEX IF NOT EXISTS holder_collections_guild_chain_contract_uidx ON holder_collections (guild_id, chain, contract_address);`);
  console.log('✅ team schema ready');
}

async function ensurePointsSchema() {
  await pointsPool.query(`
    CREATE TABLE IF NOT EXISTS holder_point_mappings (
      guild_id TEXT NOT NULL,
      chain TEXT NOT NULL DEFAULT 'ethereum',
      contract_address TEXT NOT NULL,
      mapping_json JSONB NOT NULL,
      created_by_discord_id TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (guild_id, chain, contract_address)
    );
  `);
  await pointsPool.query(`ALTER TABLE holder_point_mappings ADD COLUMN IF NOT EXISTS chain TEXT NOT NULL DEFAULT 'ethereum';`);
  await pointsPool.query(`ALTER TABLE holder_point_mappings ADD COLUMN IF NOT EXISTS created_by_discord_id TEXT;`);
  await pointsPool.query(`UPDATE holder_point_mappings SET chain = 'ethereum' WHERE chain IS NULL OR chain = '';`);
  await pointsPool.query(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'holder_point_mappings'::regclass
          AND conname = 'holder_point_mappings_pkey'
          AND pg_get_constraintdef(oid) <> 'PRIMARY KEY (guild_id, chain, contract_address)'
      ) THEN
        ALTER TABLE holder_point_mappings DROP CONSTRAINT holder_point_mappings_pkey;
      END IF;

      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'holder_point_mappings'::regclass
          AND contype = 'p'
      ) THEN
        ALTER TABLE holder_point_mappings ADD PRIMARY KEY (guild_id, chain, contract_address);
      END IF;
    END $$;
  `);
  console.log(`✅ points schema ready (${DATABASE_URL_POINTS ? 'DATABASE_URL_POINTS' : 'team database fallback'})`);
}

async function ensureClaimsSchema() {
  await claimsPool.query(`
    CREATE TABLE IF NOT EXISTS nft_claims (
      guild_id TEXT NOT NULL,
      chain TEXT NOT NULL DEFAULT 'ethereum',
      contract_address TEXT NOT NULL,
      token_id TEXT NOT NULL,
      last_claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_seen_owner_wallet TEXT,
      last_seen_discord_id TEXT,
      last_payout_type TEXT NOT NULL DEFAULT 'per_up',
      last_unit_value NUMERIC NOT NULL DEFAULT 0,
      last_payout_amount NUMERIC NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (guild_id, chain, contract_address, token_id)
    );
  `);
  await claimsPool.query(`ALTER TABLE nft_claims ADD COLUMN IF NOT EXISTS chain TEXT NOT NULL DEFAULT 'ethereum';`);
  await claimsPool.query(`UPDATE nft_claims SET chain = 'ethereum' WHERE chain IS NULL OR chain = '';`);
  await claimsPool.query(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'nft_claims'::regclass
          AND conname = 'nft_claims_pkey'
          AND pg_get_constraintdef(oid) <> 'PRIMARY KEY (guild_id, chain, contract_address, token_id)'
      ) THEN
        ALTER TABLE nft_claims DROP CONSTRAINT nft_claims_pkey;
      END IF;

      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'nft_claims'::regclass
          AND contype = 'p'
      ) THEN
        ALTER TABLE nft_claims ADD PRIMARY KEY (guild_id, chain, contract_address, token_id);
      END IF;
    END $$;
  `);
  await claimsPool.query(`
    CREATE TABLE IF NOT EXISTS claim_events (
      id BIGSERIAL PRIMARY KEY,
      guild_id TEXT NOT NULL,
      discord_id TEXT NOT NULL,
      amount NUMERIC NOT NULL,
      nft_count INTEGER NOT NULL DEFAULT 0,
      payout_type TEXT NOT NULL,
      wallet_addresses TEXT NOT NULL,
      receipt_channel_id TEXT,
      receipt_message_id TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await claimsPool.query(`
    CREATE TABLE IF NOT EXISTS claim_attempt_nfts (
      id BIGSERIAL PRIMARY KEY,
      claim_attempt_id TEXT NOT NULL,
      guild_id TEXT NOT NULL,
      discord_id TEXT NOT NULL,
      chain TEXT NOT NULL DEFAULT 'ethereum',
      contract_address TEXT NOT NULL,
      token_id TEXT NOT NULL,
      wallet_address TEXT,
      payout_type TEXT NOT NULL,
      unit_value NUMERIC NOT NULL DEFAULT 0,
      payout_amount NUMERIC NOT NULL DEFAULT 0,
      claimable_amount NUMERIC NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'pending',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      transfer_succeeded_at TIMESTAMPTZ,
      recorded_at TIMESTAMPTZ
    );
  `);
  await claimsPool.query(`CREATE INDEX IF NOT EXISTS claim_attempt_nfts_lookup_idx ON claim_attempt_nfts (guild_id, chain, contract_address, token_id, status, created_at);`);
  await claimsPool.query(`CREATE INDEX IF NOT EXISTS claim_attempt_nfts_attempt_idx ON claim_attempt_nfts (claim_attempt_id);`);
  console.log(`✅ claims schema ready (${DATABASE_URL_CLAIMS ? 'DATABASE_URL_CLAIMS' : 'holders database fallback'})`);
}

async function ensureMarketplaceSchema() {
  await prizesPool.query(`
    CREATE TABLE IF NOT EXISTS marketplace_items (
      id BIGSERIAL PRIMARY KEY,
      guild_id TEXT NOT NULL,
      item_type TEXT NOT NULL,
      name TEXT NOT NULL,
      description TEXT NOT NULL,
      thumbnail_url TEXT,
      image_url TEXT,
      price NUMERIC NOT NULL,
      per_user_limit INTEGER,
      total_stock INTEGER,
      allowed_role_ids TEXT,
      raffle_ends_at TIMESTAMPTZ,
      status TEXT NOT NULL DEFAULT 'draft',
      published_channel_id TEXT,
      published_message_id TEXT,
      cancelled_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      created_by_discord_id TEXT NOT NULL,
      updated_by_discord_id TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await prizesPool.query(`CREATE INDEX IF NOT EXISTS marketplace_items_guild_status_idx ON marketplace_items (guild_id, status, updated_at DESC);`);
  await prizesPool.query(`ALTER TABLE marketplace_items ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ;`);
  await prizesPool.query(`ALTER TABLE marketplace_items ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;`);
  await prizesPool.query(`
    CREATE TABLE IF NOT EXISTS marketplace_purchases (
      id BIGSERIAL PRIMARY KEY,
      item_id BIGINT NOT NULL REFERENCES marketplace_items(id) ON DELETE CASCADE,
      guild_id TEXT NOT NULL,
      discord_id TEXT NOT NULL,
      quantity INTEGER NOT NULL,
      spent_amount NUMERIC NOT NULL,
      purchase_type TEXT NOT NULL,
      refunded_amount NUMERIC,
      refunded_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await prizesPool.query(`ALTER TABLE marketplace_purchases ADD COLUMN IF NOT EXISTS refunded_amount NUMERIC;`);
  await prizesPool.query(`ALTER TABLE marketplace_purchases ADD COLUMN IF NOT EXISTS refunded_at TIMESTAMPTZ;`);
  await prizesPool.query(`
    CREATE TABLE IF NOT EXISTS marketplace_raffle_winners (
      id BIGSERIAL PRIMARY KEY,
      item_id BIGINT NOT NULL REFERENCES marketplace_items(id) ON DELETE CASCADE,
      guild_id TEXT NOT NULL,
      discord_id TEXT NOT NULL,
      winner_rank INTEGER NOT NULL,
      ticket_count INTEGER NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await prizesPool.query(`CREATE UNIQUE INDEX IF NOT EXISTS marketplace_raffle_winners_item_user_uidx ON marketplace_raffle_winners (item_id, discord_id);`);
  await prizesPool.query(`CREATE UNIQUE INDEX IF NOT EXISTS marketplace_raffle_winners_item_rank_uidx ON marketplace_raffle_winners (item_id, winner_rank);`);
  await prizesPool.query(`CREATE INDEX IF NOT EXISTS marketplace_purchases_item_idx ON marketplace_purchases (item_id, created_at DESC);`);
  await prizesPool.query(`CREATE INDEX IF NOT EXISTS marketplace_purchases_user_idx ON marketplace_purchases (guild_id, discord_id, created_at DESC);`);
  console.log(`✅ marketplace schema ready (${DATABASE_URL_PRIZES ? 'DATABASE_URL_PRIZES' : 'team database fallback'})`);
}

ensureHoldersSchema().catch(e => console.error('Holders schema error:', e.message));
ensureTeamSchema().catch(e => console.error('Team schema error:', e.message));
ensurePointsSchema().catch(e => console.error('Points schema error:', e.message));
ensureClaimsSchema().catch(e => console.error('Claims schema error:', e.message));
ensureMarketplaceSchema().catch(e => console.error('Marketplace schema error:', e.message));
marketplaceCommand.ensureMarketplaceTables({ marketplacePool: prizesPool }).catch(e => console.error('Malformed marketplace schema error:', e.message));
mawEvent.ensureMawTables({ mawPool: prizesPool }).catch(e => console.error('Maw schema error:', e.message));
squigDuels.ensureSquigDuelSchema(teamPool).catch(e => console.error('Squig duel schema error:', e.message));

async function setWalletLink(guildId, discordId, walletAddress, verified = false, dripMemberId = null) {
  try {
    await holdersPool.query(
      `INSERT INTO wallet_links (guild_id, discord_id, wallet_address, verified, drip_member_id)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (guild_id, discord_id, wallet_address) DO UPDATE
       SET verified = wallet_links.verified OR EXCLUDED.verified,
           drip_member_id = COALESCE(NULLIF(TRIM(EXCLUDED.drip_member_id), ''), NULLIF(TRIM(wallet_links.drip_member_id), '')),
           updated_at = NOW()`,
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

async function getUserDripIdForDisplay(guildId, discordId) {
  const links = await getWalletLinks(guildId, discordId);
  const storedIds = collectUniqueDripMemberIds(links.map((x) => x?.drip_member_id));
  if (storedIds.length) {
    return {
      dripMemberId: storedIds[0],
      additionalDripMemberIds: storedIds.slice(1),
      source: 'stored',
      reason: null,
    };
  }

  const settings = await getGuildSettings(guildId);
  if (!settings?.drip_api_key || !settings?.drip_realm_id) {
    return {
      dripMemberId: null,
      additionalDripMemberIds: [],
      source: null,
      reason: 'No DRIP ID is stored for your account yet, and DRIP lookup is not configured for this server.',
    };
  }

  try {
    const resolved = await resolveDripMemberForDiscordUser(
      settings.drip_realm_id,
      discordId,
      null,
      settings
    );
    const resolvedIds = collectDripMemberIdCandidates(resolved?.member || null);
    if (resolvedIds.length) {
      return {
        dripMemberId: resolvedIds[0],
        additionalDripMemberIds: resolvedIds.slice(1),
        source: resolved?.source || 'drip',
        reason: null,
      };
    }
    return {
      dripMemberId: null,
      additionalDripMemberIds: [],
      source: null,
      reason: 'I could not find a DRIP ID for your Discord account in this server\'s DRIP realm.',
    };
  } catch (err) {
    return {
      dripMemberId: null,
      additionalDripMemberIds: [],
      source: null,
      reason: `DRIP lookup is temporarily unavailable: ${String(err?.message || err || '').slice(0, 180)}`,
    };
  }
}

async function getWalletScanRows(guildId) {
  const { rows } = await holdersPool.query(
    `SELECT discord_id, wallet_address, verified, drip_member_id, created_at, updated_at
     FROM wallet_links
     WHERE guild_id = $1
       AND wallet_address IS NOT NULL
       AND NULLIF(TRIM(wallet_address), '') IS NOT NULL
     ORDER BY discord_id ASC, updated_at DESC, wallet_address ASC`,
    [guildId]
  );
  return rows;
}

async function getWalletLinkOwners() {
  const { rows } = await holdersPool.query(
    `SELECT guild_id, discord_id, COUNT(*) AS wallet_count
     FROM wallet_links
     WHERE wallet_address IS NOT NULL
       AND NULLIF(TRIM(wallet_address), '') IS NOT NULL
     GROUP BY guild_id, discord_id
     ORDER BY guild_id ASC, discord_id ASC`
  );
  return rows.map((row) => ({
    guildId: String(row.guild_id || '').trim(),
    discordId: String(row.discord_id || '').trim(),
    walletCount: Number(row.wallet_count || 0),
  })).filter((row) => row.guildId && row.discordId);
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

async function verifyUserWalletLinks(guildId, discordId, dripMemberId = null) {
  const normalizedDripMemberId = normalizeDripMemberId(dripMemberId);
  const params = [guildId, discordId];

  if (normalizedDripMemberId) {
    const { rowCount } = await holdersPool.query(
      `UPDATE wallet_links
       SET verified = TRUE,
           drip_member_id = COALESCE(NULLIF(TRIM(drip_member_id), ''), $3),
           updated_at = NOW()
       WHERE guild_id = $1
         AND discord_id = $2
         AND wallet_address IS NOT NULL
         AND NULLIF(TRIM(wallet_address), '') IS NOT NULL`,
      [guildId, discordId, normalizedDripMemberId]
    );
    return rowCount || 0;
  }

  const { rowCount } = await holdersPool.query(
    `UPDATE wallet_links
     SET verified = TRUE,
         updated_at = NOW()
     WHERE guild_id = $1
       AND discord_id = $2
       AND wallet_address IS NOT NULL
       AND NULLIF(TRIM(wallet_address), '') IS NOT NULL
       AND drip_member_id IS NOT NULL
       AND NULLIF(TRIM(drip_member_id), '') IS NOT NULL`,
    params
  );
  return rowCount || 0;
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
      .setName('dripid')
      .setDescription('Privately show your DRIP member ID for easy copy/paste')
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
      .setName('verifyuser')
      .setDescription('Admin override: mark a user\'s linked wallet(s) as DRIP verified')
      .addUserOption((opt) =>
        opt
          .setName('user')
          .setDescription('Discord user to mark as verified')
          .setRequired(true)
      )
      .toJSON(),
    new SlashCommandBuilder()
      .setName('refreshuser')
      .setDescription('Admin: recheck one user\'s wallet verification and holder roles')
      .addUserOption((opt) =>
        opt
          .setName('user')
          .setDescription('Discord user to refresh')
          .setRequired(true)
      )
      .toJSON(),
    new SlashCommandBuilder()
      .setName('verifyall')
      .setDescription('Admin: recheck DRIP wallet verification for linked users in a role')
      .addRoleOption((opt) =>
        opt
          .setName('role')
          .setDescription('Only verify linked users who currently have this role')
          .setRequired(true)
      )
      .toJSON(),
    new SlashCommandBuilder()
      .setName('listuserwallets')
      .setDescription('Admin: view linked wallets and verification status for a user')
      .addUserOption((opt) =>
        opt
          .setName('user')
          .setDescription('Discord user to inspect')
          .setRequired(true)
      )
      .toJSON(),
    new SlashCommandBuilder()
      .setName('walletcheck')
      .setDescription('Admin: check which Discord user a wallet is linked to')
      .addStringOption((opt) =>
        opt
          .setName('wallet')
          .setDescription('Ethereum wallet address to check')
          .setRequired(true)
      )
      .toJSON(),
    new SlashCommandBuilder()
      .setName('scanwallet')
      .setDescription('Admin: export linked verification wallets to a private CSV')
      .addRoleOption((opt) =>
        opt
          .setName('role')
          .setDescription('Optional: only include members with this role')
          .setRequired(false)
      )
      .toJSON(),
    new SlashCommandBuilder()
      .setName('healthcheck')
      .setDescription('Admin: check verification and reward system health for this server')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('refresh')
      .setDescription('Admin: immediately refresh all connected wallet verification and holder roles')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('paytest')
      .setDescription('Admin: send a DRIP payout test using the live reward transfer flow')
      .addStringOption((opt) =>
        opt
          .setName('discord_id')
          .setDescription('Discord user ID to receive the payout')
          .setRequired(true)
      )
      .addIntegerOption((opt) =>
        opt
          .setName('amount')
          .setDescription('Test payout amount')
          .setRequired(false)
          .setMinValue(1)
          .setMaxValue(100000)
      )
      .addStringOption((opt) =>
        opt
          .setName('recipient_member_id')
          .setDescription('Optional DRIP member ID override for the recipient')
          .setRequired(false)
      )
      .toJSON(),
    new SlashCommandBuilder()
      .setName('portal')
      .setDescription('Admin: manage the PORTAL MALFUNCTION event scheduler')
      .addSubcommand((sub) =>
        sub
          .setName('start')
          .setDescription('Start the portal scheduler')
      )
      .addSubcommand((sub) =>
        sub
          .setName('stop')
          .setDescription('Stop the portal scheduler and clear pending timers')
      )
      .addSubcommand((sub) =>
        sub
          .setName('trigger')
          .setDescription('Trigger a portal event immediately')
      )
      .toJSON(),
    new SlashCommandBuilder()
      .setName('prize')
      .setDescription('Admin: open the marketplace prize editor dashboard')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('endraffle')
      .setDescription('Admin: cancel an active raffle and refund all purchases')
      .addStringOption((opt) =>
        opt
          .setName('message_id')
          .setDescription('Published raffle message ID')
          .setRequired(true)
      )
      .toJSON(),
    new SlashCommandBuilder()
      .setName('info')
      .setDescription('Admin: view a plain-English guide for how this bot works')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('info-user')
      .setDescription('Admin: post a public plain-English guide for users')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('allrank')
      .setDescription('Admin: export every Squig Maw Rank to CSV')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('rank')
      .setDescription('Show the Maw Rank and image for a Squig')
      .addIntegerOption((opt) =>
        opt
          .setName('token_id')
          .setDescription('Squig token ID')
          .setRequired(true)
          .setMinValue(1)
          .setMaxValue(MAW_EXPECTED_TOKEN_COUNT)
      )
      .toJSON(),
    new SlashCommandBuilder()
      .setName('flex')
      .setDescription('Show a random NFT you own from Charm of the Ugly, Ugly Monsters, or Squigs')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('ugly')
      .setDescription('Show a random Charm of the Ugly NFT you own')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('monster')
      .setDescription('Show a random Ugly Monster NFT you own')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('squig')
      .setDescription('Show a random Squig you own')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('mint')
      .setDescription('Show the Squigs Reloaded mint embed')
      .toJSON(),
    new SlashCommandBuilder()
      .setName('doctor')
      .setDescription('Generate a doctor note with your Discord display name')
      .toJSON(),
    marketplaceCommand.buildMarketplaceSlashCommand().toJSON(),
    mawEvent.buildMawSlashCommand().toJSON(),
    mawEvent.buildSquigPrizeSlashCommand().toJSON(),
    squigDuels.buildSquigDuelSlashCommand().toJSON(),
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
  startMarketplaceRaffleProcessor();
  try {
    mawEvent.startMawWatchers();
    console.log('✅ Maw watchers started.');
  } catch (err) {
    console.warn('⚠️ Maw watchers not started:', err.message);
  }
  startDailyHolderVerificationRefresh();
  if (String(process.env.PORTAL_AUTO_START || '').toLowerCase() === 'true') {
    try {
      portalEvent.startPortalScheduler({
        guildId: process.env.PORTAL_GUILD_ID || null,
        channelId: process.env.PORTAL_CHANNEL_ID || null,
      });
      console.log('✅ Portal scheduler auto-started.');
    } catch (err) {
      console.warn('⚠️ Portal auto-start failed:', err.message);
    }
  }
});

const RECEIPT_CHANNEL_ID = '1403005536982794371';
globalThis.__PENDING_HOLDER_RULES ||= new Map();
globalThis.__PENDING_TRAIT_ROLE_RULES ||= new Map();
globalThis.__PENDING_POINTS_MAPPING ||= new Map();
globalThis.__PENDING_CHECK_STATS ||= new Map();
globalThis.__PENDING_PRIZE_DRAFTS ||= new Map();

function normalizeEthAddress(input) {
  const addr = String(input || '').trim();
  if (!/^0x[a-fA-F0-9]{40}$/.test(addr)) return null;
  return addr.toLowerCase();
}

function normalizeNftChain(input = DEFAULT_NFT_CHAIN) {
  const key = String(input || DEFAULT_NFT_CHAIN).trim().toLowerCase();
  return NFT_CHAIN_ALIASES[key] || null;
}

function nftChainConfig(chain) {
  const normalized = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  return NFT_CHAIN_CONFIG[normalized] || NFT_CHAIN_CONFIG[DEFAULT_NFT_CHAIN];
}

function nftChainLabel(chain) {
  return nftChainConfig(chain).label;
}

function squigsChain() {
  return normalizeNftChain(SQUIGS_CHAIN) || DEFAULT_NFT_CHAIN;
}

function isSquigsContract(contractAddress) {
  return String(contractAddress || '').toLowerCase() === SQUIGS_CONTRACT.toLowerCase();
}

function isOgSquigsContract(contractAddress) {
  return String(contractAddress || '').toLowerCase() === OG_SQUIGS_CONTRACT.toLowerCase();
}

function collectionKey(chain, contractAddress) {
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  const normalizedAddress = normalizeEthAddress(contractAddress);
  return normalizedAddress ? `${normalizedChain}:${normalizedAddress}` : null;
}

function collectionSelectValue(chain, contractAddress) {
  return collectionKey(chain, contractAddress) || `${DEFAULT_NFT_CHAIN}:${String(contractAddress || '').toLowerCase()}`;
}

function parseChainAddressInput(input) {
  const raw = String(input || '').trim();
  const explicit = raw.match(/^([a-z0-9_-]+)\s*[:/|]\s*(0x[a-fA-F0-9]{40})$/i);
  if (explicit) {
    const chain = normalizeNftChain(explicit[1]);
    const contractAddress = normalizeEthAddress(explicit[2]);
    return chain && contractAddress ? { chain, contractAddress } : null;
  }
  const contractAddress = normalizeEthAddress(raw);
  return contractAddress ? { chain: DEFAULT_NFT_CHAIN, contractAddress } : null;
}

function pointMappingKey(chain, contractAddress) {
  return collectionKey(chain, contractAddress);
}

function getPointMappingForContract(guildPointMappings, contractAddress, chain = DEFAULT_NFT_CHAIN) {
  if (!(guildPointMappings instanceof Map)) return null;
  const key = pointMappingKey(chain, contractAddress);
  const mapped = key ? guildPointMappings.get(key) : null;
  if (mapped) return mapped;
  if ((normalizeNftChain(chain) || DEFAULT_NFT_CHAIN) === DEFAULT_NFT_CHAIN) {
    return guildPointMappings.get(String(contractAddress || '').toLowerCase()) || null;
  }
  return null;
}

function chainAddressLabel(chain, contractAddress) {
  return `${nftChainLabel(chain)}:${String(contractAddress || '').toLowerCase()}`;
}

function findHolderCollectionByInput(collections, input) {
  const raw = String(input || '').trim();
  const parsed = parseChainAddressInput(raw);
  const rawLower = raw.toLowerCase();
  return (collections || []).find((c) => {
    const chain = normalizeNftChain(c.chain) || DEFAULT_NFT_CHAIN;
    const addr = String(c.contract_address || '').toLowerCase();
    const byAddress = parsed && parsed.chain === chain && parsed.contractAddress === addr;
    const byName = String(c.name || '').trim().toLowerCase() === rawLower;
    return byAddress || byName;
  }) || null;
}

function openseaAssetUrl(chain, contractAddress, tokenId) {
  const cfg = nftChainConfig(chain);
  return `https://opensea.io/assets/${cfg.openseaChain}/${contractAddress}/${tokenId}`;
}

function explorerAddressUrl(chain, walletAddress) {
  const cfg = nftChainConfig(chain);
  return `${cfg.explorerBaseUrl}/address/${walletAddress}`;
}

function alchemyNftUrl(chain, endpoint) {
  const cfg = nftChainConfig(chain);
  return `https://${cfg.alchemyNetwork}.g.alchemy.com/nft/v3/${ALCHEMY_API_KEY}/${endpoint}`;
}

function labelForContract(contractAddress, chain = DEFAULT_NFT_CHAIN) {
  const c = String(contractAddress || '').toLowerCase();
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  if (normalizedChain === DEFAULT_NFT_CHAIN && c === UGLY_CONTRACT.toLowerCase()) return 'Charm of the Ugly';
  if (normalizedChain === DEFAULT_NFT_CHAIN && c === MONSTER_CONTRACT.toLowerCase()) return 'Ugly Monsters';
  if (normalizedChain === DEFAULT_NFT_CHAIN && isOgSquigsContract(c)) return 'Squigs';
  if (isSquigsContract(c)) return 'Squigs Reloaded';
  return String(contractAddress || 'Unknown Contract');
}

function normalizeImageUrl(input) {
  if (!input) return null;
  if (typeof input === 'object') {
    const nested =
      input.gateway ||
      input.cachedUrl ||
      input.pngUrl ||
      input.thumbnailUrl ||
      input.raw ||
      input.url ||
      input.image ||
      input.href ||
      null;
    if (!nested) return null;
    return normalizeImageUrl(nested);
  }

  const value = String(input).trim();
  if (!value) return null;
  if (value.startsWith('ipfs://')) {
    const ipfsUrl = `https://ipfs.io/ipfs/${value.slice('ipfs://'.length).replace(/^ipfs\//, '')}`;
    try {
      const parsed = new URL(ipfsUrl);
      if (parsed.protocol === 'http:' || parsed.protocol === 'https:') return parsed.toString();
    } catch {}
    return null;
  }
  try {
    const parsed = new URL(value);
    if (parsed.protocol === 'http:' || parsed.protocol === 'https:') return parsed.toString();
  } catch {}
  return null;
}

function pickRandom(arr) {
  if (!Array.isArray(arr) || !arr.length) return null;
  return arr[Math.floor(Math.random() * arr.length)];
}

const LOCAL_SQUIG_METADATA_CANDIDATES = [
  path.join(__dirname, 'metadata.csv'),
  path.join(__dirname, '..', 'metadata.csv'),
];
const LOCAL_SQUIG_IMAGE_DIR_CANDIDATES = [
  path.join(__dirname, 'images'),
  path.join(__dirname, '..', 'images'),
];
let localSquigMetadataCache = null;

function parseCsvRecords(text) {
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;

  for (let i = 0; i < String(text || '').length; i++) {
    const ch = text[i];
    const next = text[i + 1];
    if (inQuotes) {
      if (ch === '"' && next === '"') {
        field += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        field += ch;
      }
      continue;
    }

    if (ch === '"') {
      inQuotes = true;
    } else if (ch === ',') {
      row.push(field);
      field = '';
    } else if (ch === '\n') {
      row.push(field);
      rows.push(row);
      row = [];
      field = '';
    } else if (ch !== '\r') {
      field += ch;
    }
  }

  if (field || row.length) {
    row.push(field);
    rows.push(row);
  }
  return rows;
}

function loadLocalSquigMetadata() {
  if (localSquigMetadataCache) return localSquigMetadataCache;
  const byTokenId = new Map();
  const csvPath = LOCAL_SQUIG_METADATA_CANDIDATES.find((candidate) => fs.existsSync(candidate));
  if (!csvPath) {
    localSquigMetadataCache = byTokenId;
    return byTokenId;
  }

  try {
    const rows = parseCsvRecords(fs.readFileSync(csvPath, 'utf8'));
    const rawHeader = (rows.shift() || []).map((h) => String(h || '').trim());
    const normalizedHeader = rawHeader.map((h) => h.toLowerCase());
    const tokenIdx = normalizedHeader.indexOf('tokenid');
    const nameIdx = normalizedHeader.indexOf('name');
    const fileIdx = normalizedHeader.indexOf('file_name');
    if (tokenIdx < 0) throw new Error('missing tokenID column');

    for (const row of rows) {
      const tokenId = String(row[tokenIdx] || '').trim();
      if (!/^\d+$/.test(tokenId)) continue;
      const attrs = [];
      for (let i = 0; i < rawHeader.length; i++) {
        const match = String(rawHeader[i] || '').match(/^attributes\[(.+)\]$/i);
        if (!match) continue;
        const value = String(row[i] || '').trim();
        if (!value) continue;
        attrs.push({ trait_type: match[1], value });
      }
      byTokenId.set(tokenId, {
        name: nameIdx >= 0 ? String(row[nameIdx] || '').trim() : '',
        fileName: fileIdx >= 0 ? String(row[fileIdx] || '').trim() : '',
        attrs,
      });
    }
  } catch (err) {
    console.warn('Could not load local Squig metadata:', String(err?.message || err || ''));
  }

  localSquigMetadataCache = byTokenId;
  return byTokenId;
}

function localSquigImagePath(tokenId) {
  const tid = String(tokenId || '').trim();
  if (!/^\d+$/.test(tid)) return null;
  const localMeta = loadLocalSquigMetadata().get(tid);
  const fileName = localMeta?.fileName || `${tid}.png`;
  const safeFileName = path.basename(fileName);
  for (const imageDir of LOCAL_SQUIG_IMAGE_DIR_CANDIDATES) {
    const candidate = path.join(imageDir, safeFileName);
    if (fs.existsSync(candidate)) return candidate;
  }
  return null;
}

function mawRankColor(rarityKey) {
  const key = String(rarityKey || '').trim().toLowerCase();
  if (key === 'legendary') return 0xffc857;
  if (key === 'epic') return 0x9b5de5;
  if (key === 'rare') return 0x00bbf9;
  if (key === 'uncommon') return 0x70e000;
  return 0xb0deee;
}

function squigRankImageAttachment(tokenId) {
  const tid = String(tokenId || '').trim();
  if (!/^\d+$/.test(tid)) return { imageUrl: null, files: [] };
  const imagePath = localSquigImagePath(tid);
  if (imagePath) {
    const name = `squig-${tid}${path.extname(imagePath) || '.png'}`;
    return {
      imageUrl: `attachment://${name}`,
      files: [new AttachmentBuilder(imagePath, { name })],
    };
  }
  return { imageUrl: null, files: [] };
}

function buildSquigRankPayload(tokenId) {
  const quote = getMawRewardQuote(tokenId);
  const image = squigRankImageAttachment(quote.tokenId);
  const embed = new EmbedBuilder()
    .setTitle(`Squig #${quote.tokenId}`)
    .setColor(mawRankColor(quote.rarityKey))
    .setDescription(
      `Rank: **${formatMawAverageRank(quote.averageRank)}**\n` +
      `Class: **${quote.rarityLabel}**`
    );

  if (image.imageUrl) embed.setImage(image.imageUrl);
  return {
    embeds: [embed],
    ...(image.files.length ? { files: image.files } : {}),
  };
}

function buildAllMawRanksExport() {
  const index = loadMawRankingIndex();
  const rows = [...index.rows].sort((a, b) => Number(a.tokenId) - Number(b.tokenId));
  const lines = ['TOKEN ID,RANK,CLASS'];
  for (const row of rows) {
    lines.push([row.tokenId, formatMawAverageRank(row.averageRank), row.rarityLabel].map(csvEscape).join(','));
  }
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  return {
    rowCount: rows.length,
    rankingSourceHash: index.rankingSourceHash,
    attachment: new AttachmentBuilder(Buffer.from(`${lines.join('\n')}\n`, 'utf8'), {
      name: `squigs-maw-ranks-${timestamp}.csv`,
    }),
  };
}

function localSquigTraits(tokenId, contractAddress = SQUIGS_CONTRACT, chain = DEFAULT_NFT_CHAIN) {
  const normalizedContract = String(contractAddress || '').toLowerCase();
  if (!isSquigsContract(normalizedContract)) return [];
  return loadLocalSquigMetadata().get(String(tokenId || '').trim())?.attrs || [];
}

const directSquigsOwnerCache = new Map();
const DIRECT_SQUIG_OWNER_CACHE_TTL_MS = Math.max(0, Number(process.env.DIRECT_SQUIG_OWNER_CACHE_TTL_MS || 5 * 60 * 1000));
const DIRECT_SQUIG_MAX_TOKEN_ID = Math.max(1, Number(process.env.SQUIG_MAX_TOKEN_ID || 4444));
const DIRECT_OG_SQUIG_MAX_TOKEN_ID = Math.max(1, Number(process.env.OG_SQUIG_MAX_TOKEN_ID || 10000));

async function getOwnedErc721ByOwnerScan(walletAddress, contractAddress, maxTokenId, cacheLabel) {
  const wallet = normalizeEthAddress(walletAddress);
  const contractAddr = normalizeEthAddress(contractAddress);
  if (!wallet) return [];
  if (!contractAddr) return [];
  const cacheKey = `ethereum:${contractAddr}:${wallet}:${cacheLabel}`;
  const cached = directSquigsOwnerCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) return cached.promise;

  const promise = (async () => {
    const abi = ['function ownerOf(uint256 tokenId) view returns (address)'];
    const contract = new ethers.Contract(contractAddr, abi, globalThis.__SQUIGS_PROVIDER);
    const ids = [];
    await mapLimit(Array.from({ length: maxTokenId }, (_, i) => i + 1), 12, async (tokenId) => {
      try {
        const owner = normalizeEthAddress(await contract.ownerOf(tokenId));
        if (owner === wallet) ids.push(String(tokenId));
      } catch {}
    });
    ids.sort((a, b) => Number(a) - Number(b));
    return ids;
  })();

  directSquigsOwnerCache.set(cacheKey, {
    expiresAt: Date.now() + DIRECT_SQUIG_OWNER_CACHE_TTL_MS,
    promise,
  });
  if (directSquigsOwnerCache.size > 500) {
    const oldestKey = directSquigsOwnerCache.keys().next().value;
    if (oldestKey && oldestKey !== cacheKey) directSquigsOwnerCache.delete(oldestKey);
  }
  promise.catch(() => directSquigsOwnerCache.delete(cacheKey));
  return promise;
}

async function getOwnedSquigsReloadedDirect(walletAddress) {
  return getOwnedErc721ByOwnerScan(walletAddress, SQUIGS_CONTRACT, DIRECT_SQUIG_MAX_TOKEN_ID, 'reloaded');
}

async function getOwnedOgSquigsDirect(walletAddress) {
  return getOwnedErc721ByOwnerScan(walletAddress, OG_SQUIGS_CONTRACT, DIRECT_OG_SQUIG_MAX_TOKEN_ID, 'og');
}

async function getOwnedSquigsReloadedTokenIds(walletAddresses) {
  const normalizedWallets = [...new Set((Array.isArray(walletAddresses) ? walletAddresses : [walletAddresses]).map((w) => normalizeEthAddress(w)).filter(Boolean))];
  const candidates = await expandCollectionsForOwnership(null, [
    { name: 'Squigs Reloaded', contractAddress: SQUIGS_CONTRACT, chain: DEFAULT_NFT_CHAIN },
  ]);
  const seen = new Set();
  const tokenIds = [];

  for (const collection of candidates.filter((c) => isSquigsContract(c.contractAddress))) {
    const ids = await getOwnedTokenIdsForContractMany(normalizedWallets, collection.contractAddress, collection.chain);
    for (const tokenId of ids) {
      const key = String(tokenId);
      if (seen.has(key)) continue;
      seen.add(key);
      tokenIds.push(key);
    }
  }

  if (!tokenIds.length) {
    const directResults = await Promise.all(normalizedWallets.map((wallet) => getOwnedSquigsReloadedDirect(wallet).catch(() => [])));
    for (const ids of directResults) {
      for (const tokenId of ids) {
        const key = String(tokenId);
        if (seen.has(key)) continue;
        seen.add(key);
        tokenIds.push(key);
      }
    }
  }

  tokenIds.sort((a, b) => Number(a) - Number(b));
  return tokenIds;
}

async function getOwnedOgSquigsTokenIds(walletAddresses) {
  const normalizedWallets = [...new Set((Array.isArray(walletAddresses) ? walletAddresses : [walletAddresses]).map((w) => normalizeEthAddress(w)).filter(Boolean))];
  const seen = new Set();
  const tokenIds = [];

  const providerIds = await getOwnedTokenIdsForContractMany(normalizedWallets, OG_SQUIGS_CONTRACT, DEFAULT_NFT_CHAIN);
  for (const tokenId of providerIds) {
    const key = String(tokenId);
    if (seen.has(key)) continue;
    seen.add(key);
    tokenIds.push(key);
  }

  if (!tokenIds.length) {
    const directResults = await Promise.all(normalizedWallets.map((wallet) => getOwnedOgSquigsDirect(wallet).catch(() => [])));
    for (const ids of directResults) {
      for (const tokenId of ids) {
        const key = String(tokenId);
        if (seen.has(key)) continue;
        seen.add(key);
        tokenIds.push(key);
      }
    }
  }

  tokenIds.sort((a, b) => Number(a) - Number(b));
  return tokenIds;
}

function ownershipCollectionKey(collection) {
  return `${normalizeNftChain(collection?.chain) || DEFAULT_NFT_CHAIN}:${String(collection?.contractAddress || '').toLowerCase()}`;
}

async function expandCollectionsForOwnership(guildId, collections) {
  const out = [];
  const seen = new Set();
  const add = (collection) => {
    const chain = normalizeNftChain(collection?.chain) || DEFAULT_NFT_CHAIN;
    const contractAddress = normalizeEthAddress(collection?.contractAddress || collection?.contract_address);
    if (!contractAddress) return;
    const normalized = {
      name: String(collection?.name || labelForContract(contractAddress, chain)),
      contractAddress,
      chain,
    };
    const key = ownershipCollectionKey(normalized);
    if (seen.has(key)) return;
    seen.add(key);
    out.push(normalized);
  };

  const holderCollections = collections.some((c) => isSquigsContract(c?.contractAddress || c?.contract_address))
    ? await getHolderCollections(guildId).catch((err) => {
        console.warn('Squigs collection lookup failed:', String(err?.message || err || ''));
        return [];
      })
    : [];
  const configuredSquigs = holderCollections.filter((c) => isSquigsContract(c.contract_address || c.contractAddress));

  for (const collection of collections) {
    if (!isSquigsContract(collection?.contractAddress || collection?.contract_address)) {
      add(collection);
      continue;
    }

    for (const configured of configuredSquigs.filter((c) => normalizeNftChain(c.chain) !== DEFAULT_NFT_CHAIN)) {
      add({ name: 'Squigs Reloaded', contractAddress: SQUIGS_CONTRACT, chain: configured.chain });
    }
    for (const configured of configuredSquigs.filter((c) => normalizeNftChain(c.chain) === squigsChain())) {
      add({ name: 'Squigs Reloaded', contractAddress: SQUIGS_CONTRACT, chain: configured.chain });
    }
    for (const configured of configuredSquigs) {
      add({ name: 'Squigs Reloaded', contractAddress: SQUIGS_CONTRACT, chain: configured.chain });
    }
    add({ name: 'Squigs Reloaded', contractAddress: SQUIGS_CONTRACT, chain: squigsChain() });
    for (const chain of Object.keys(NFT_CHAIN_CONFIG)) {
      add({ name: 'Squigs Reloaded', contractAddress: SQUIGS_CONTRACT, chain });
    }
  }

  return out;
}

async function buildRandomOwnedNftResponse(guildId, discordUserId, username, collections, commandLabel) {
  const links = await getWalletLinks(guildId, discordUserId);
  const walletAddresses = links.map((x) => x.wallet_address).filter(Boolean);
  if (!walletAddresses.length) {
    return {
      content: 'Connect your wallet first with the verification menu before using this command.'
    };
  }

  const ownershipCollections = await expandCollectionsForOwnership(guildId, collections);
  const pools = await Promise.all(
    ownershipCollections.map(async ({ contractAddress, chain = DEFAULT_NFT_CHAIN }) => {
      const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
      let tokenIds = await getOwnedTokenIdsForContractMany(walletAddresses, contractAddress, normalizedChain);
      if (!tokenIds.length && normalizedChain === DEFAULT_NFT_CHAIN && isOgSquigsContract(contractAddress)) {
        tokenIds = await getOwnedOgSquigsTokenIds(walletAddresses);
      }
      return tokenIds.map((tokenId) => ({ tokenId: String(tokenId), contractAddress, chain: normalizedChain }));
    })
  );
  const owned = pools.flat();
  if (!owned.length) {
    const names = ownershipCollections.map((x) => `${x.name} (${nftChainLabel(x.chain)})`).join(', ');
    return {
      content: `No ${commandLabel} NFTs found across your linked wallet${walletAddresses.length === 1 ? '' : 's'}.\nChecked: ${names}`
    };
  }

  const chosen = pickRandom(owned);
  const meta = await getNftMetadataAlchemy(chosen.tokenId, chosen.contractAddress, chosen.chain).catch(() => null);
  const collectionName = labelForContract(chosen.contractAddress, chosen.chain);
  const isSquig = isSquigsContract(chosen.contractAddress);
  const localSquigMeta = isSquig ? loadLocalSquigMetadata().get(String(chosen.tokenId)) : null;
  const tokenName = String(meta?.name || localSquigMeta?.name || `${collectionName} #${chosen.tokenId}`);
  const imageUrl =
    normalizeImageUrl(
      meta?.image ||
      meta?.image?.cachedUrl ||
      meta?.image?.pngUrl ||
      meta?.image?.thumbnailUrl ||
      meta?.metadata?.image ||
      meta?.raw?.metadata?.image
    ) ||
    (isSquig && SQUIG_IMAGE_BASE_URL
      ? `${SQUIG_IMAGE_BASE_URL}/${chosen.tokenId}`
      : isOgSquigsContract(chosen.contractAddress) && OG_SQUIG_IMAGE_BASE_URL
      ? `${OG_SQUIG_IMAGE_BASE_URL}/${chosen.tokenId}`
      : null);
  const localImagePath = isSquig ? localSquigImagePath(chosen.tokenId) : null;
  const localImageName = localImagePath ? `squig-${chosen.tokenId}${path.extname(localImagePath) || '.png'}` : null;

  const embed = new EmbedBuilder()
    .setTitle(tokenName)
    .setColor(0xB0DEEE)
    .setDescription(
      `Collection: **${collectionName}**\n` +
      `Chain: **${nftChainLabel(chosen.chain)}**\n` +
      `Token ID: **${chosen.tokenId}**\n` +
      `OpenSea: ${openseaAssetUrl(chosen.chain, chosen.contractAddress, chosen.tokenId)}` +
      (isSquig ? `\n[Mint A Squig](https://bueno.art/squigs/mint)` : '')
    )
    .setFooter({ text: `${commandLabel} pull for ${username}` });
  if (localImagePath) embed.setImage(`attachment://${localImageName}`);
  else if (imageUrl) embed.setImage(imageUrl);

  return {
    embeds: [embed],
    ...(localImagePath ? { files: [new AttachmentBuilder(localImagePath, { name: localImageName })] } : {}),
  };
}

async function squigsReloadedDetails(guildId, tokenIds) {
  const mappings = await getGuildPointMappings(guildId);
  const table = hpTableForContract(SQUIGS_CONTRACT, mappings, DEFAULT_NFT_CHAIN);
  return mapLimit(tokenIds, 8, async (tokenId) => {
    const localMeta = loadLocalSquigMetadata().get(String(tokenId));
    let attrs = localSquigTraits(tokenId, SQUIGS_CONTRACT, DEFAULT_NFT_CHAIN).map(massageTraitKeys).filter(validAttrFilter);
    let meta = null;
    if (!attrs.length) {
      meta = await getNftMetadataAlchemy(tokenId, SQUIGS_CONTRACT, DEFAULT_NFT_CHAIN).catch(() => null);
      ({ attrs } = await getTraitsForToken(meta, tokenId, SQUIGS_CONTRACT, DEFAULT_NFT_CHAIN));
    }
    attrs = normalizeSquigsReloadedAttrs(attrs, SQUIGS_CONTRACT);
    const grouped = normalizeTraits(attrs);
    const { total } = computeHpFromTraits(grouped, table);
    return {
      tokenId: String(tokenId),
      name: String(meta?.name || localMeta?.name || `Squig #${tokenId}`),
      uglyPoints: Math.max(0, Math.floor(Number(total) || 0)),
      imagePath: localSquigImagePath(tokenId),
    };
  });
}

async function buildRandomSquigReloadedResponse(guildId, discordUserId, username) {
  const links = await getWalletLinks(guildId, discordUserId);
  const walletAddresses = links.map((x) => x.wallet_address).filter(Boolean);
  if (!walletAddresses.length) {
    return { content: 'Connect your wallet first with the verification menu before using this command.' };
  }

  const tokenIds = await getOwnedSquigsReloadedTokenIds(walletAddresses);
  if (!tokenIds.length) {
    return {
      content:
        `No !squig NFTs found across your linked wallet${walletAddresses.length === 1 ? '' : 's'}.\n` +
        `Checked: Squigs Reloaded (Ethereum) \`${SQUIGS_CONTRACT}\``,
    };
  }

  const chosenTokenId = pickRandom(tokenIds);
  const [detail] = await squigsReloadedDetails(guildId, [chosenTokenId]);
  const imageName = detail.imagePath ? `squig-${detail.tokenId}${path.extname(detail.imagePath) || '.png'}` : null;
  const embed = new EmbedBuilder()
    .setTitle(detail.name)
    .setColor(0xB0DEEE)
    .setDescription(
      `Collection: **Squigs Reloaded**\n` +
      `Token ID: **${detail.tokenId}**\n` +
      `UglyPoints: **${detail.uglyPoints}**\n` +
      `OpenSea: ${openseaAssetUrl(DEFAULT_NFT_CHAIN, SQUIGS_CONTRACT, detail.tokenId)}`
    )
    .setFooter({ text: `!squig pull for ${username}` });
  if (detail.imagePath) embed.setImage(`attachment://${imageName}`);
  else if (SQUIG_IMAGE_BASE_URL) embed.setImage(`${SQUIG_IMAGE_BASE_URL}/${detail.tokenId}`);

  return {
    embeds: [embed],
    ...(detail.imagePath ? { files: [new AttachmentBuilder(detail.imagePath, { name: imageName })] } : {}),
  };
}

function drawGridText(ctx, text, x, y, maxWidth) {
  let value = String(text || '');
  while (value.length > 3 && ctx.measureText(value).width > maxWidth) {
    value = `${value.slice(0, -4)}...`;
  }
  ctx.fillText(value, x, y);
}

function shuffledCopy(items) {
  const out = Array.isArray(items) ? items.slice() : [];
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

async function buildSquigGridAttachment(details, username) {
  const maxShown = Math.min(details.length, 100);
  const shown = details.length > maxShown ? shuffledCopy(details).slice(0, maxShown) : details.slice(0, maxShown);
  const cols = Math.min(10, Math.max(1, Math.ceil(Math.sqrt(shown.length || 1))));
  const cell = 170;
  const pad = 24;
  const titleH = 76;
  const rows = Math.min(10, Math.max(1, Math.ceil(shown.length / cols)));
  const width = pad * 2 + cols * cell;
  const height = titleH + pad + rows * cell;
  const canvas = createCanvas(width, height);
  const ctx = canvas.getContext('2d');
  ctx.fillStyle = '#111816';
  ctx.fillRect(0, 0, width, height);
  ctx.fillStyle = '#B0DEEE';
  ctx.font = '700 28px Arial, sans-serif';
  ctx.fillText(`${username}'s Squigs Reloaded`, pad, 34);
  ctx.fillStyle = '#d4a43b';
  ctx.font = '600 16px Arial, sans-serif';
  ctx.fillText(`${details.length} owned${details.length > maxShown ? ` - showing ${maxShown} random` : ''}`, pad, 58);

  for (let i = 0; i < shown.length; i++) {
    const item = shown[i];
    const col = i % cols;
    const row = Math.floor(i / cols);
    const x = pad + col * cell;
    const y = titleH + row * cell;
    ctx.fillStyle = '#eef5f0';
    ctx.fillRect(x + 8, y + 8, cell - 16, cell - 42);
    if (item.imagePath) {
      try {
        const img = await loadImage(item.imagePath);
        const box = cell - 24;
        const scale = Math.min(box / img.width, (cell - 58) / img.height);
        const w = Math.max(1, Math.round(img.width * scale));
        const h = Math.max(1, Math.round(img.height * scale));
        ctx.drawImage(img, x + (cell - w) / 2, y + 12 + ((cell - 58) - h) / 2, w, h);
      } catch {}
    }
    ctx.fillStyle = '#ffffff';
    ctx.font = '700 15px Arial, sans-serif';
    drawGridText(ctx, `#${item.tokenId} - ${item.uglyPoints} UP`, x + 10, y + cell - 24, cell - 20);
  }

  return new AttachmentBuilder(canvas.toBuffer('image/png'), { name: 'squigs-reloaded-grid.png' });
}

async function buildSquigGridResponse(guildId, discordUserId, username) {
  const links = await getWalletLinks(guildId, discordUserId);
  const walletAddresses = links.map((x) => x.wallet_address).filter(Boolean);
  if (!walletAddresses.length) {
    return { content: 'Connect your wallet first with the verification menu before using this command.' };
  }

  const tokenIds = await getOwnedSquigsReloadedTokenIds(walletAddresses);
  if (!tokenIds.length) {
    return {
      content:
        `No !grid NFTs found across your linked wallet${walletAddresses.length === 1 ? '' : 's'}.\n` +
        `Checked: Squigs Reloaded (Ethereum) \`${SQUIGS_CONTRACT}\``,
    };
  }

  const details = await squigsReloadedDetails(guildId, tokenIds);
  const attachment = await buildSquigGridAttachment(details, username);
  return {
    content: `Squigs Reloaded owned: **${details.length}**`,
    files: [attachment],
  };
}

async function replyWithRandomOwnedNft(interaction, collections, commandLabel) {
  const payload = await buildRandomOwnedNftResponse(
    interaction.guild.id,
    interaction.user.id,
    interaction.user.username,
    collections,
    commandLabel
  );
  await interaction.editReply(payload);
}

function mintEmbed() {
  return new EmbedBuilder()
    .setTitle('Mint Squigs Reloaded')
    .setColor(0xB0DEEE)
    .setDescription(
      `Pick your ugly little space freak before they escape.\n\n` +
      `**Mint Phases**\n` +
      `**GTD:** <t:1781884800>\n` +
      `**FCFS:** <t:1781892000>\n` +
      `**Public:** <t:1781899200>\n\n` +
      `**Mint here:**\n` +
      `https://opensea.io/collection/squigs-reloaded/overview\n\n` +
      `Only use the official link above. Stay ugly. Stay safe. Don't click sketchy DMs.`
    )
    .setImage(`attachment://${MINT_TODAY_ATTACHMENT_NAME}`);
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

function csvEscape(value) {
  let text = '';
  if (value instanceof Date) {
    text = Number.isNaN(value.getTime()) ? '' : value.toISOString();
  } else if (value != null) {
    text = String(value);
  }
  if (/^[=+\-@]/.test(text)) text = `'${text}`;
  if (/[",\r\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

function formatCsvTimestamp(value) {
  if (!value) return '';
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? '' : date.toISOString();
}

function sanitizeCsvFilenamePart(value, fallback = 'all') {
  const cleaned = String(value || '')
    .trim()
    .replace(/[^a-zA-Z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 48);
  return cleaned || fallback;
}

async function buildWalletScanCsv(guild, rows, role = null) {
  const discordIds = [...new Set(rows.map((row) => String(row.discord_id || '').trim()).filter(Boolean))];
  const memberEntries = await mapLimit(discordIds, 5, async (discordId) => {
    const cached = guild.members.cache.get(discordId);
    if (cached) return [discordId, cached];
    const fetched = await guild.members.fetch(discordId).catch(() => null);
    return [discordId, fetched];
  });
  const membersById = new Map(memberEntries);
  const selectedRoleId = role?.id ? String(role.id) : null;
  const selectedRoleName = role?.name ? String(role.name) : '';
  const filteredRows = selectedRoleId
    ? rows.filter((row) => {
        const member = membersById.get(String(row.discord_id || '').trim());
        return Boolean(member?.roles?.cache?.has(selectedRoleId));
      })
    : rows;

  const headers = [
    'discord_id',
    'discord_username',
    'server_display_name',
    'wallet_address',
    'verified',
    'drip_member_id',
    'selected_role_id',
    'selected_role_name',
    'created_at',
    'updated_at',
  ];
  const lines = [headers.join(',')];
  for (const row of filteredRows) {
    const discordId = String(row.discord_id || '').trim();
    const member = membersById.get(discordId);
    const values = [
      discordId,
      member?.user?.tag || member?.user?.username || '',
      member?.displayName || '',
      row.wallet_address || '',
      row.verified ? 'true' : 'false',
      row.drip_member_id || '',
      selectedRoleId || '',
      selectedRoleName,
      formatCsvTimestamp(row.created_at),
      formatCsvTimestamp(row.updated_at),
    ];
    lines.push(values.map(csvEscape).join(','));
  }

  return {
    csv: `${lines.join('\r\n')}\r\n`,
    rowCount: filteredRows.length,
    uniqueUserCount: new Set(filteredRows.map((row) => String(row.discord_id || '').trim()).filter(Boolean)).size,
    missingMemberCount: selectedRoleId
      ? 0
      : discordIds.filter((discordId) => !membersById.get(discordId)).length,
    roleFilterSkippedMemberCount: selectedRoleId
      ? discordIds.filter((discordId) => !membersById.get(discordId)).length
      : 0,
  };
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
  const traitIdx = ['trait', 'traits'].map((k) => header.indexOf(k)).find((i) => i >= 0) ?? -1;
  const ptsIdxCandidates = ['ugly_points', 'uglypoints', 'points', 'up', 'value'].map((k) => header.indexOf(k)).filter((i) => i >= 0);
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

async function postAdminSystemLog({ guild = null, guildId = null, category = 'System', message, files = [] }) {
  if (!ADMIN_LOG_CHANNEL_ID) return;
  try {
    const ch = await client.channels.fetch(ADMIN_LOG_CHANNEL_ID).catch(() => null);
    if (!ch?.isTextBased()) return;
    const guildLabel = guild?.name || guildId || 'unknown';
    await ch.send({
      content:
        `**${category}**\n` +
        `Guild: ${guildLabel}\n` +
        `${String(message || '').slice(0, 1600)}`,
      files,
    });
  } catch (err) {
    console.warn('⚠️ Admin system log failed:', String(err?.message || err || ''));
  }
}

function csvCell(value) {
  const text = String(value ?? '');
  return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function buildPendingWalletCsv(pendingWallets = []) {
  const headers = ['discord_id', 'mention', 'wallet_address', 'drip_member_id', 'reason'];
  const rows = pendingWallets.map((item) => [
    item.discordId,
    `<@${item.discordId}>`,
    item.walletAddress,
    item.dripMemberId || '',
    item.reason || '',
  ]);
  return [headers, ...rows].map((row) => row.map(csvCell).join(',')).join('\n');
}

function pendingWalletReportAttachment(summary) {
  const pendingWallets = Array.isArray(summary?.pendingWallets) ? summary.pendingWallets : [];
  if (!pendingWallets.length) return null;
  const csv = buildPendingWalletCsv(pendingWallets);
  const roleId = String(summary?.roleId || 'role').replace(/[^a-zA-Z0-9_-]/g, '');
  const filename = `verifyall-pending-wallets-${roleId}.csv`;
  return new AttachmentBuilder(Buffer.from(csv, 'utf8'), { name: filename });
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

function sanitizeRoleNameForUser(roleName) {
  return String(roleName || 'Unknown role')
    .replace(/@/g, '@\u200b')
    .replace(/[`\r\n]/g, '');
}

function formatGrantedRolesForUser(grantedRoles = []) {
  const names = [...new Set(
    (Array.isArray(grantedRoles) ? grantedRoles : [])
      .map((roleName) => sanitizeRoleNameForUser(roleName))
      .filter(Boolean)
  )];
  return names.length
    ? names.map((roleName) => `- ${roleName}`).join('\n')
    : 'No new roles were added.';
}

function formatRoleSyncUserNote(syncResult) {
  const skipped = Array.isArray(syncResult?.applied)
    ? syncResult.applied.filter((line) => /temporarily unavailable|missing access|cannot manage|role not found/i.test(String(line || '')))
    : [];
  if (!skipped.length) return '';
  return `\nSome role checks were skipped and will work on refresh:\n${skipped.slice(0, 3).map((line) => `- ${String(line).slice(0, 180)}`).join('\n')}`;
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

function buildPortalAdminActionRow({ canTriggerNow = true } = {}) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId('portal_admin_stop')
      .setLabel('STOP')
      .setStyle(ButtonStyle.Danger),
    new ButtonBuilder()
      .setCustomId('portal_admin_change_time')
      .setLabel('CHANGE TIME')
      .setStyle(ButtonStyle.Secondary),
    new ButtonBuilder()
      .setCustomId('portal_admin_trigger_now')
      .setLabel('TRIGGER NOW')
      .setStyle(ButtonStyle.Success)
      .setDisabled(!canTriggerNow)
  );
}

function formatPortalNextTriggerText(state) {
  if (state?.portalActive) {
    return 'A portal is already active right now.';
  }
  const nextPortalAt = Number(state?.nextPortalAt || 0);
  if (!Number.isFinite(nextPortalAt) || nextPortalAt <= Date.now()) {
    return 'The next portal trigger is scheduled, but the remaining time is not available.';
  }
  const remainingMs = Math.max(0, nextPortalAt - Date.now());
  const remainingMinutes = Math.max(1, Math.round(remainingMs / 60000));
  return `It should trigger in about **${remainingMinutes} minute${remainingMinutes === 1 ? '' : 's'}**.`;
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

function getPrizeDraftKey(guildId, discordId) {
  return `${guildId}:${discordId}`;
}

const DEFAULT_PRIZE_THUMBNAIL_URL = 'https://i.imgur.com/C8TdWo8.png';

function pad2(value) {
  return String(value).padStart(2, '0');
}

function getDefaultPrizeRaffleEndDate() {
  const date = new Date(Date.now() + (24 * 60 * 60 * 1000));
  date.setSeconds(0, 0);
  return date;
}

function createEmptyPrizeDraft(guildId, discordId) {
  return {
    guildId,
    discordId,
    itemType: 'buy',
    name: '',
    description: '',
    thumbnailUrl: DEFAULT_PRIZE_THUMBNAIL_URL,
    imageUrl: '',
    price: '',
    perUserLimit: '',
    totalStock: '',
    allowedRoleIds: [],
    raffleDurationMinutes: '',
    raffleEndsAtIso: '',
    selectedChannelId: '',
    createdAt: Date.now(),
    updatedAt: Date.now(),
  };
}

function upsertPrizeDraft(guildId, discordId, patch = {}) {
  const key = getPrizeDraftKey(guildId, discordId);
  const existing = globalThis.__PENDING_PRIZE_DRAFTS.get(key) || createEmptyPrizeDraft(guildId, discordId);
  const next = { ...existing, ...patch, updatedAt: Date.now() };
  if (!String(next.thumbnailUrl || '').trim()) next.thumbnailUrl = DEFAULT_PRIZE_THUMBNAIL_URL;
  globalThis.__PENDING_PRIZE_DRAFTS.set(key, next);
  return next;
}

function getPrizeDraft(guildId, discordId) {
  return globalThis.__PENDING_PRIZE_DRAFTS.get(getPrizeDraftKey(guildId, discordId)) || null;
}

function clearPrizeDraft(guildId, discordId) {
  globalThis.__PENDING_PRIZE_DRAFTS.delete(getPrizeDraftKey(guildId, discordId));
}

function parseRoleIdsInput(input, guild) {
  const values = String(input || '')
    .split(/[,\n]/)
    .map((x) => x.trim())
    .filter(Boolean);
  const out = [];
  const seen = new Set();
  for (const raw of values) {
    const match = raw.match(/^<@&(\d{16,20})>$/) || raw.match(/^(\d{16,20})$/);
    const roleId = match?.[1] || null;
    if (!roleId || seen.has(roleId)) continue;
    if (guild && !guild.roles.cache.has(roleId)) continue;
    seen.add(roleId);
    out.push(roleId);
  }
  return out;
}

function formatRoleMentions(roleIds) {
  const ids = Array.isArray(roleIds) ? roleIds.filter(Boolean) : [];
  return ids.length ? ids.map((id) => `<@&${id}>`).join(', ') : 'Everyone';
}

function normalizeMarketplaceItemType(value) {
  return String(value || '').trim().toLowerCase() === 'raffle' ? 'raffle' : 'buy';
}

function getPrizeRaffleEndDate(draft) {
  const iso = String(draft?.raffleEndsAtIso || '').trim();
  if (iso) {
    const date = new Date(iso);
    if (Number.isFinite(date.getTime())) return date;
  }
  const durationMinutes = Number(draft?.raffleDurationMinutes);
  if (Number.isFinite(durationMinutes) && durationMinutes > 0) {
    return new Date(Date.now() + (durationMinutes * 60 * 1000));
  }
  return null;
}

function getPrizeRaffleEndsAtIso(draft) {
  const date = getPrizeRaffleEndDate(draft);
  return date ? date.toISOString() : null;
}

function getPrizeRaffleTimeParts(draft) {
  const date = getPrizeRaffleEndDate(draft) || getDefaultPrizeRaffleEndDate();
  const hour24 = date.getHours();
  const ampm = hour24 >= 12 ? 'PM' : 'AM';
  const hour12 = hour24 % 12 || 12;
  return {
    day: date.getDate(),
    month: date.getMonth() + 1,
    year: date.getFullYear(),
    hour: hour12,
    minute: date.getMinutes(),
    ampm,
  };
}

function getLastDayOfMonth(year, month) {
  return new Date(year, month, 0).getDate();
}

function buildPrizeRaffleEndDateFromParts(parts) {
  const year = Number(parts.year);
  const month = Number(parts.month);
  const day = Math.min(Number(parts.day), getLastDayOfMonth(year, month));
  const hour12 = Number(parts.hour);
  const minute = Number(parts.minute);
  const ampm = String(parts.ampm || 'AM').toUpperCase() === 'PM' ? 'PM' : 'AM';
  const hour24 = (hour12 % 12) + (ampm === 'PM' ? 12 : 0);
  const date = new Date(year, month - 1, day, hour24, minute, 0, 0);
  return Number.isFinite(date.getTime()) ? date : null;
}

function formatPrizeRaffleEndDate(draft) {
  const date = getPrizeRaffleEndDate(draft);
  if (!date) return '`not set`';
  const parts = getPrizeRaffleTimeParts(draft);
  return `**${pad2(parts.day)}/${pad2(parts.month)}/${parts.year} ${pad2(parts.hour)}:${pad2(parts.minute)} ${parts.ampm}**`;
}

function buildPrizeRaffleTimePatch(draft, part, value) {
  const parts = getPrizeRaffleTimeParts(draft);
  if (part === 'day') parts.day = Number(value);
  if (part === 'month') parts.month = Number(value);
  if (part === 'year') parts.year = Number(value);
  if (part === 'hour') parts.hour = Number(value);
  if (part === 'minute') parts.minute = Number(value);
  if (part === 'ampm') parts.ampm = String(value || 'AM').toUpperCase() === 'PM' ? 'PM' : 'AM';
  const endDate = buildPrizeRaffleEndDateFromParts(parts) || getDefaultPrizeRaffleEndDate();
  return {
    raffleEndsAtIso: endDate.toISOString(),
    raffleDurationMinutes: '',
  };
}

function formatMarketplaceTimeLeft(item) {
  if (normalizeMarketplaceItemType(item?.item_type || item?.itemType) !== 'raffle') return 'n/a';
  const endAt = item?.raffle_ends_at ? new Date(item.raffle_ends_at) : null;
  if (!endAt || !Number.isFinite(endAt.getTime())) return 'Not set';
  const now = Date.now();
  const remainingMs = endAt.getTime() - now;
  if (remainingMs <= 0) return 'Closed';
  const minutes = Math.max(1, Math.ceil(remainingMs / 60000));
  if (minutes < 60) return `${minutes} minute${minutes === 1 ? '' : 's'}`;
  const hours = Math.floor(minutes / 60);
  const extraMinutes = minutes % 60;
  if (!extraMinutes) return `${hours} hour${hours === 1 ? '' : 's'}`;
  return `${hours}h ${extraMinutes}m`;
}

function buildPrizeEditorEmbed(draft) {
  const type = normalizeMarketplaceItemType(draft?.itemType);
  const limitLabel = type === 'raffle' ? 'Tickets available' : 'Per person limit';
  const stockLabel = type === 'raffle' ? 'Winner count' : 'Total stock';
  return new EmbedBuilder()
    .setTitle(`Prize Editor | ${type === 'raffle' ? 'Raffle' : 'Buy Item'}`)
    .setColor(0xE67E22)
    .setDescription(
      `Configure the marketplace item, then press **Done** to preview the user-facing post.\n\n` +
      `Name: ${draft?.name ? `**${draft.name}**` : '`not set`'}\n` +
      `Description: ${draft?.description ? draft.description.slice(0, 300) : '`not set`'}\n` +
      `Thumbnail: ${draft?.thumbnailUrl ? draft.thumbnailUrl : '`not set`'}\n` +
      `Image: ${draft?.imageUrl ? draft.imageUrl : '`not set`'}\n` +
      `Price: ${draft?.price ? `**${draft.price} $CHARM**` : '`not set`'}\n` +
      `${limitLabel}: ${draft?.perUserLimit ? `**${draft.perUserLimit}**` : 'Unlimited'}\n` +
      `${stockLabel}: ${draft?.totalStock ? `**${draft.totalStock}**` : (type === 'raffle' ? '`not set`' : 'Unlimited')}\n` +
      `Allowed roles: ${formatRoleMentions(draft?.allowedRoleIds)}\n` +
      `Publish channel: ${draft?.selectedChannelId ? `<#${draft.selectedChannelId}>` : '`not set`'}\n` +
      `Raffle closes: ${type === 'raffle'
        ? formatPrizeRaffleEndDate(draft)
        : 'Not used for buy items'}`
    );
}

function buildPrizeEditorRows(draft) {
  const isRaffle = normalizeMarketplaceItemType(draft?.itemType) === 'raffle';
  const limitLabel = isRaffle ? 'Tickets' : 'Per Person';
  const stockLabel = isRaffle ? 'Winners' : 'Stock';
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('prize_type_buy').setLabel('Type: Buy').setStyle(isRaffle ? ButtonStyle.Secondary : ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('prize_type_raffle').setLabel('Type: Raffle').setStyle(isRaffle ? ButtonStyle.Primary : ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('prize_set_name').setLabel('Name').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('prize_set_description').setLabel('Description').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('prize_set_price').setLabel('Price').setStyle(ButtonStyle.Secondary),
    ),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('prize_set_thumbnail').setLabel('Thumbnail').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('prize_set_image').setLabel('Image').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('prize_set_limit').setLabel(limitLabel).setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('prize_set_stock').setLabel(stockLabel).setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('prize_clear_roles').setLabel('Clear Roles').setStyle(ButtonStyle.Secondary),
    ),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('prize_set_raffle_time').setLabel('Raffle Time').setStyle(ButtonStyle.Secondary).setDisabled(!isRaffle),
      new ButtonBuilder().setCustomId('prize_done').setLabel('Done').setStyle(ButtonStyle.Success),
      new ButtonBuilder().setCustomId('prize_cancel').setLabel('Cancel').setStyle(ButtonStyle.Danger),
    ),
    new ActionRowBuilder().addComponents(
      new RoleSelectMenuBuilder()
        .setCustomId('prize_roles_select')
        .setPlaceholder('Select allowed roles')
        .setMinValues(1)
        .setMaxValues(10)
    ),
  ];
}

function buildPrizeSelectMenu(customId, placeholder, options, selectedValue) {
  const selected = String(selectedValue);
  return new StringSelectMenuBuilder()
    .setCustomId(customId)
    .setPlaceholder(placeholder)
    .setMinValues(1)
    .setMaxValues(1)
    .addOptions(options.map((option) => ({
      ...option,
      default: String(option.value) === selected,
    })));
}

function buildPrizeRaffleDateRows(draft) {
  const parts = getPrizeRaffleTimeParts(draft);
  const currentYear = new Date().getFullYear();
  const yearValues = new Set([parts.year]);
  for (let year = currentYear; year <= currentYear + 4; year += 1) yearValues.add(year);
  const dayOptions = (start, end) => Array.from({ length: end - start + 1 }, (_, i) => {
    const day = start + i;
    return { label: pad2(day), value: String(day) };
  });
  return [
    new ActionRowBuilder().addComponents(
      buildPrizeSelectMenu('prize_raffle_day_early', `DD: ${pad2(parts.day)}`, dayOptions(1, 16), parts.day)
    ),
    new ActionRowBuilder().addComponents(
      buildPrizeSelectMenu('prize_raffle_day_late', `DD: ${pad2(parts.day)}`, dayOptions(17, 31), parts.day)
    ),
    new ActionRowBuilder().addComponents(
      buildPrizeSelectMenu(
        'prize_raffle_month',
        `MM: ${pad2(parts.month)}`,
        Array.from({ length: 12 }, (_, i) => {
          const month = i + 1;
          return { label: pad2(month), value: String(month) };
        }),
        parts.month
      )
    ),
    new ActionRowBuilder().addComponents(
      buildPrizeSelectMenu(
        'prize_raffle_year',
        `YYYY: ${parts.year}`,
        [...yearValues].sort((a, b) => a - b).map((year) => ({ label: String(year), value: String(year) })),
        parts.year
      )
    ),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('prize_raffle_picker_cancel').setLabel('Back').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('prize_raffle_picker_time').setLabel('Next: Time').setStyle(ButtonStyle.Primary),
    ),
  ];
}

function buildPrizeRaffleTimeRows(draft) {
  const parts = getPrizeRaffleTimeParts(draft);
  return [
    new ActionRowBuilder().addComponents(
      buildPrizeSelectMenu(
        'prize_raffle_hour',
        `HH: ${pad2(parts.hour)}`,
        Array.from({ length: 12 }, (_, i) => {
          const hour = i + 1;
          return { label: pad2(hour), value: String(hour) };
        }),
        parts.hour
      )
    ),
    new ActionRowBuilder().addComponents(
      buildPrizeSelectMenu(
        'prize_raffle_ampm',
        `AM/PM: ${parts.ampm}`,
        ['AM', 'PM'].map((value) => ({ label: value, value })),
        parts.ampm
      )
    ),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('prize_raffle_picker_date').setLabel('Back: Date').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('prize_raffle_picker_minute').setLabel('Next: Minute').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('prize_raffle_picker_cancel').setLabel('Cancel').setStyle(ButtonStyle.Secondary),
    ),
  ];
}

function buildPrizeRaffleMinuteRows(draft) {
  const parts = getPrizeRaffleTimeParts(draft);
  const minuteOptions = (start, end) => Array.from({ length: end - start + 1 }, (_, i) => {
    const minute = start + i;
    return { label: pad2(minute), value: String(minute) };
  });
  return [
    new ActionRowBuilder().addComponents(
      buildPrizeSelectMenu('prize_raffle_minute_early', `MM: ${pad2(parts.minute)}`, minuteOptions(0, 24), parts.minute)
    ),
    new ActionRowBuilder().addComponents(
      buildPrizeSelectMenu('prize_raffle_minute_mid', `MM: ${pad2(parts.minute)}`, minuteOptions(25, 49), parts.minute)
    ),
    new ActionRowBuilder().addComponents(
      buildPrizeSelectMenu('prize_raffle_minute_late', `MM: ${pad2(parts.minute)}`, minuteOptions(50, 59), parts.minute)
    ),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('prize_raffle_picker_time').setLabel('Back: Time').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('prize_raffle_picker_save').setLabel('Save').setStyle(ButtonStyle.Success),
      new ButtonBuilder().setCustomId('prize_raffle_picker_cancel').setLabel('Cancel').setStyle(ButtonStyle.Secondary),
    ),
  ];
}

function buildPrizeRafflePickerPayload(draft, screen = 'date', note = '') {
  const selectedText = formatPrizeRaffleEndDate(draft);
  const content = [
    'Select the raffle close time.',
    `Selected: ${selectedText}`,
    note ? `\n${note}` : '',
  ].filter(Boolean).join('\n');
  const components = screen === 'minute'
    ? buildPrizeRaffleMinuteRows(draft)
    : screen === 'time'
      ? buildPrizeRaffleTimeRows(draft)
      : buildPrizeRaffleDateRows(draft);
  return { content, embeds: [], components };
}

async function updatePrizeRafflePicker(interaction, draft, screen = 'date', note = '') {
  await interaction.update(buildPrizeRafflePickerPayload(draft, screen, note));
}

async function updatePrizeEditorPanel(interaction, draft) {
  const payload = {
    content: '',
    embeds: [buildPrizeEditorEmbed(draft)],
    components: buildPrizeEditorRows(draft),
  };

  if (typeof interaction.update === 'function') {
    try {
      await interaction.update(payload);
      return;
    } catch (err) {
      console.warn('Prize editor message update failed; falling back to ephemeral reply:', err?.message || err);
    }
  }

  await interaction.reply({
    ...payload,
    flags: 64
  });
}

function buildPrizePreviewRows(draft = null) {
  return [
    new ActionRowBuilder().addComponents(
      new ChannelSelectMenuBuilder()
        .setCustomId('prize_publish_channel_select')
        .setPlaceholder('Select publish channel')
        .addChannelTypes(ChannelType.GuildText)
        .setMinValues(1)
        .setMaxValues(1)
    ),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('prize_publish').setLabel('Publish').setStyle(ButtonStyle.Success).setDisabled(!draft?.selectedChannelId),
      new ButtonBuilder().setCustomId('prize_clear_channel').setLabel('Clear Channel').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('prize_edit').setLabel('Edit').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('prize_cancel').setLabel('Cancel').setStyle(ButtonStyle.Danger),
    ),
  ];
}

function validatePrizeDraft(draft) {
  const problems = [];
  const itemType = normalizeMarketplaceItemType(draft?.itemType);
  if (!String(draft?.name || '').trim()) problems.push('Item name');
  if (!String(draft?.description || '').trim()) problems.push('Item description');
  const price = Number(draft?.price);
  if (!Number.isFinite(price) || price <= 0) problems.push('Price');
  const perUserLimit = String(draft?.perUserLimit || '').trim();
  if (perUserLimit && (!Number.isInteger(Number(perUserLimit)) || Number(perUserLimit) < 1)) {
    problems.push(itemType === 'raffle' ? 'Tickets available' : 'Per person limit');
  }
  const totalStock = String(draft?.totalStock || '').trim();
  if (totalStock && (!Number.isInteger(Number(totalStock)) || Number(totalStock) < 1)) {
    problems.push(itemType === 'raffle' ? 'Winner count' : 'Total stock');
  }
  if (draft?.thumbnailUrl && !normalizeImageUrl(draft.thumbnailUrl)) problems.push('Thumbnail URL');
  if (draft?.imageUrl && !normalizeImageUrl(draft.imageUrl)) problems.push('Image URL');
  if (itemType === 'raffle') {
    const endsAt = getPrizeRaffleEndDate(draft);
    if (!endsAt || endsAt.getTime() <= Date.now()) problems.push('Raffle close time');
    if (!perUserLimit) problems.push('Tickets available');
    if (!totalStock) problems.push('Winner count');
  }
  return {
    ok: problems.length === 0,
    problems,
  };
}

async function getMarketplaceItemById(itemId) {
  const { rows } = await prizesPool.query(`SELECT * FROM marketplace_items WHERE id = $1 LIMIT 1`, [itemId]);
  return rows[0] || null;
}

async function getMarketplaceItemByPublishedMessage(guildId, messageId) {
  const { rows } = await prizesPool.query(
    `SELECT *
     FROM marketplace_items
     WHERE guild_id = $1 AND published_message_id = $2
     LIMIT 1`,
    [guildId, messageId]
  );
  return rows[0] || null;
}

async function getMarketplaceItemStats(itemId, discordId = null) {
  const [{ rows: totals }, { rows: mine }] = await Promise.all([
    prizesPool.query(
      `SELECT COALESCE(SUM(quantity), 0) AS total_purchased, COUNT(*) AS purchase_events
       FROM marketplace_purchases
       WHERE item_id = $1`,
      [itemId]
    ),
    discordId
      ? prizesPool.query(
          `SELECT COALESCE(SUM(quantity), 0) AS user_quantity, COALESCE(SUM(spent_amount), 0) AS user_spent
           FROM marketplace_purchases
           WHERE item_id = $1 AND discord_id = $2`,
          [itemId, discordId]
        )
      : Promise.resolve({ rows: [{ user_quantity: 0, user_spent: 0 }] })
  ]);
  return {
    totalPurchased: Number(totals[0]?.total_purchased || 0),
    purchaseEvents: Number(totals[0]?.purchase_events || 0),
    userQuantity: Number(mine[0]?.user_quantity || 0),
    userSpent: Number(mine[0]?.user_spent || 0),
  };
}

async function getMarketplaceEntryLeaderboard(itemId) {
  const { rows } = await prizesPool.query(
    `SELECT discord_id, COALESCE(SUM(quantity), 0) AS ticket_count
     FROM marketplace_purchases
     WHERE item_id = $1
       AND refunded_at IS NULL
     GROUP BY discord_id
     ORDER BY ticket_count DESC, discord_id ASC`,
    [itemId]
  );
  return rows.map((row) => ({
    discordId: String(row.discord_id || '').trim(),
    ticketCount: Number(row.ticket_count || 0),
  })).filter((row) => row.discordId && row.ticketCount > 0);
}

async function getMarketplaceRaffleWinners(itemId) {
  const { rows } = await prizesPool.query(
    `SELECT discord_id, winner_rank, ticket_count
     FROM marketplace_raffle_winners
     WHERE item_id = $1
     ORDER BY winner_rank ASC`,
    [itemId]
  );
  return rows.map((row) => ({
    discordId: String(row.discord_id || '').trim(),
    rank: Number(row.winner_rank || 0),
    ticketCount: Number(row.ticket_count || 0),
  })).filter((row) => row.discordId && row.rank > 0);
}

function pickRandomTicketWinners(entries, winnerCount) {
  const ticketPool = [];
  for (const entry of Array.isArray(entries) ? entries : []) {
    const discordId = String(entry.discordId || '').trim();
    const ticketCount = Math.max(0, Math.floor(Number(entry.ticketCount || 0)));
    if (!discordId || ticketCount <= 0) continue;
    for (let i = 0; i < ticketCount; i++) {
      ticketPool.push({ discordId, ticketCount });
    }
  }

  const winners = [];
  let remaining = Math.max(0, Math.floor(Number(winnerCount || 0)));
  while (ticketPool.length && remaining > 0) {
    const chosenIndex = Math.floor(Math.random() * ticketPool.length);
    winners.push(ticketPool[chosenIndex]);
    ticketPool.splice(chosenIndex, 1);
    remaining -= 1;
  }
  return winners;
}

function getMarketplaceRemainingStock(item, stats) {
  const itemType = normalizeMarketplaceItemType(item?.item_type || item?.itemType);
  const capacity = itemType === 'raffle'
    ? Number(item?.per_user_limit)
    : Number(item?.total_stock);
  if (!Number.isFinite(capacity) || capacity <= 0) return null;
  return Math.max(0, capacity - Number(stats?.totalPurchased || 0));
}

function formatMarketplaceRaffleWinnerSummary(winners) {
  if (!Array.isArray(winners) || !winners.length) {
    return 'No valid entries were available for this raffle.';
  }
  return winners
    .map((winner, index) => `${index + 1}. <@${winner.discordId}> (${winner.ticketCount} ticket${winner.ticketCount === 1 ? '' : 's'})`)
    .join('\n');
}

function buildMarketplaceItemEmbed(item, stats = {}, extras = {}) {
  const itemType = normalizeMarketplaceItemType(item?.item_type || item?.itemType);
  const remainingStock = getMarketplaceRemainingStock(item, stats);
  const allowedRoleIds = String(item?.allowed_role_ids || '')
    .split(',')
    .map((x) => x.trim())
    .filter(Boolean);
  const titleType = itemType === 'raffle' ? 'Raffle' : 'Buy';
  const status = String(item?.status || 'published');
  const raffleStatusText = status === 'completed'
    ? 'Draw completed'
    : status === 'cancelled'
      ? 'Cancelled'
      : formatMarketplaceTimeLeft(item);
  const embed = new EmbedBuilder()
    .setTitle(`${String(item?.name || 'Marketplace Item').slice(0, 220)} | ${titleType}`)
    .setColor(itemType === 'raffle' ? (status === 'completed' ? 0xE67E22 : 0xF1C40F) : 0x2ECC71)
    .setDescription(String(item?.description || '').slice(0, 4000));

  if (itemType === 'raffle' && status === 'completed') {
    const winners = Array.isArray(extras?.raffleWinners) ? extras.raffleWinners : [];
    embed.addFields(
      { name: 'Winner Window', value: formatMarketplaceRaffleWinnerSummary(winners), inline: false },
      { name: 'Total Tickets Purchased', value: `**${Math.max(0, Math.floor(Number(stats?.totalPurchased || 0)))}**`, inline: true },
      { name: 'Winners Drawn', value: winners.length ? `**${winners.length}**` : '**0**', inline: true },
      { name: 'Result', value: 'Claim button is restricted to the raffle winner(s).', inline: false },
    );
  } else {
    embed.addFields(...(itemType === 'raffle'
      ? [
          { name: 'Ticket Cost', value: `**${Math.floor(Number(item?.price || 0))} $CHARM**`, inline: true },
          { name: 'Tickets Remaining', value: remainingStock == null ? 'Unlimited' : `**${remainingStock}**`, inline: true },
          { name: 'Winners', value: Number(item?.total_stock) > 0 ? `**${item.total_stock}**` : '`not set`', inline: true },
          { name: 'Roles Allowed', value: formatRoleMentions(allowedRoleIds), inline: false },
          { name: status === 'completed' ? 'Result' : 'Time Left Until Draw', value: raffleStatusText, inline: false },
        ]
      : [
          { name: 'Cost', value: `**${Math.floor(Number(item?.price || 0))} $CHARM**`, inline: true },
          { name: 'Available', value: remainingStock == null ? 'Unlimited' : `**${remainingStock}**`, inline: true },
          { name: 'Per Person', value: Number(item?.per_user_limit) > 0 ? `**${item.per_user_limit}**` : 'Unlimited', inline: true },
          { name: 'Roles Allowed', value: formatRoleMentions(allowedRoleIds), inline: false },
          { name: 'Status', value: 'Available while stock remains', inline: false },
        ]));
  }

  embed
    .setFooter({ text: `Item ID ${item?.id || 'preview'}` });
  const thumb = normalizeImageUrl(item?.thumbnail_url || item?.thumbnailUrl || '');
  const image = normalizeImageUrl(item?.image_url || item?.imageUrl || '');
  if (thumb) embed.setThumbnail(thumb);
  if (image) embed.setImage(image);
  return embed;
}

function buildMarketplaceItemButtons(item, stats = {}, extras = {}) {
  const itemType = normalizeMarketplaceItemType(item?.item_type || item?.itemType);
  const remainingStock = getMarketplaceRemainingStock(item, stats);
  const raffleClosed = itemType === 'raffle' && formatMarketplaceTimeLeft(item) === 'Closed';
  const soldOut = remainingStock != null && remainingStock <= 0;
  const disabled = soldOut || raffleClosed || String(item?.status || 'published') !== 'published';
  const status = String(item?.status || 'published');
  if (itemType === 'raffle' && status === 'completed') {
    const winners = Array.isArray(extras?.raffleWinners) ? extras.raffleWinners : [];
    return [
      new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId(`market_entries_${item.id}`)
          .setLabel('Entries')
          .setStyle(ButtonStyle.Secondary),
        new ButtonBuilder()
          .setCustomId(`market_claim_${item.id}`)
          .setLabel('Claim')
          .setStyle(ButtonStyle.Success)
          .setDisabled(!winners.length)
      )
    ];
  }
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`market_buy_${item.id}`)
        .setLabel(itemType === 'raffle' ? 'Buy Tickets' : 'Buy')
        .setStyle(ButtonStyle.Success)
        .setDisabled(disabled),
      ...(itemType === 'raffle'
        ? [
            new ButtonBuilder()
              .setCustomId(`market_entries_${item.id}`)
              .setLabel('View Entries')
              .setStyle(ButtonStyle.Secondary)
          ]
        : [])
    )
  ];
}

async function createMarketplaceItemFromDraft(guildId, actorDiscordId, draft) {
  const itemType = normalizeMarketplaceItemType(draft?.itemType);
  const raffleEndsAt = itemType === 'raffle' ? getPrizeRaffleEndDate(draft) : null;
  const { rows } = await prizesPool.query(
    `INSERT INTO marketplace_items (
       guild_id, item_type, name, description, thumbnail_url, image_url, price, per_user_limit, total_stock,
       allowed_role_ids, raffle_ends_at, status, created_by_discord_id, updated_by_discord_id, updated_at
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'draft', $12, $12, NOW())
     RETURNING *`,
    [
      guildId,
      itemType,
      String(draft?.name || '').trim(),
      String(draft?.description || '').trim(),
      normalizeImageUrl(draft?.thumbnailUrl || DEFAULT_PRIZE_THUMBNAIL_URL) || null,
      normalizeImageUrl(draft?.imageUrl) || null,
      Math.floor(Number(draft?.price || 0)),
      Number(draft?.perUserLimit) > 0 ? Math.floor(Number(draft.perUserLimit)) : null,
      Number(draft?.totalStock) > 0 ? Math.floor(Number(draft.totalStock)) : null,
      Array.isArray(draft?.allowedRoleIds) && draft.allowedRoleIds.length ? draft.allowedRoleIds.join(',') : null,
      raffleEndsAt ? raffleEndsAt.toISOString() : null,
      actorDiscordId,
    ]
  );
  return rows[0] || null;
}

async function publishMarketplaceItem(itemId, channelId, actorDiscordId) {
  const item = await getMarketplaceItemById(itemId);
  if (!item) throw new Error('Marketplace item not found.');
  const channel = await client.channels.fetch(channelId).catch(() => null);
  if (!channel?.isTextBased()) throw new Error('Selected channel is not text-based.');
  const stats = await getMarketplaceItemStats(itemId);
  const message = await channel.send({
    embeds: [buildMarketplaceItemEmbed(item, stats)],
    components: buildMarketplaceItemButtons({ ...item, status: 'published' }, stats),
  });
  const { rows } = await prizesPool.query(
    `UPDATE marketplace_items
     SET status = 'published',
         published_channel_id = $2,
         published_message_id = $3,
         updated_by_discord_id = $4,
         updated_at = NOW()
     WHERE id = $1
     RETURNING *`,
    [itemId, channelId, message.id, actorDiscordId]
  );
  return rows[0] || item;
}

async function refreshMarketplaceItemMessage(itemId) {
  const item = await getMarketplaceItemById(itemId);
  if (!item?.published_channel_id || !item?.published_message_id) return;
  const channel = await client.channels.fetch(item.published_channel_id).catch(() => null);
  if (!channel?.isTextBased()) return;
  const message = await channel.messages.fetch(item.published_message_id).catch(() => null);
  if (!message) return;
  const [stats, raffleWinners] = await Promise.all([
    getMarketplaceItemStats(itemId),
    normalizeMarketplaceItemType(item.item_type) === 'raffle' && String(item.status) === 'completed'
      ? getMarketplaceRaffleWinners(itemId)
      : Promise.resolve([])
  ]);
  await message.edit({
    embeds: [buildMarketplaceItemEmbed(item, stats, { raffleWinners })],
    components: buildMarketplaceItemButtons(item, stats, { raffleWinners }),
  }).catch(() => null);
}

async function markMarketplaceItemCancelled(itemId, actorDiscordId) {
  const { rows } = await prizesPool.query(
    `UPDATE marketplace_items
     SET status = 'cancelled',
         cancelled_at = NOW(),
         updated_by_discord_id = $2,
         updated_at = NOW()
     WHERE id = $1
     RETURNING *`,
    [itemId, actorDiscordId]
  );
  return rows[0] || null;
}

async function markMarketplaceItemCompleted(itemId, actorDiscordId) {
  const { rows } = await prizesPool.query(
    `UPDATE marketplace_items
     SET status = 'completed',
         completed_at = NOW(),
         updated_by_discord_id = $2,
         updated_at = NOW()
     WHERE id = $1
     RETURNING *`,
    [itemId, actorDiscordId]
  );
  return rows[0] || null;
}

async function getRefundableMarketplacePurchases(itemId) {
  const { rows } = await prizesPool.query(
    `SELECT id, guild_id, discord_id, quantity, spent_amount, purchase_type, refunded_at
     FROM marketplace_purchases
     WHERE item_id = $1
       AND refunded_at IS NULL
     ORDER BY created_at ASC`,
    [itemId]
  );
  return rows;
}

async function markMarketplacePurchaseRefunded(purchaseId, refundedAmount) {
  await prizesPool.query(
    `UPDATE marketplace_purchases
     SET refunded_amount = $2,
         refunded_at = NOW()
     WHERE id = $1`,
    [purchaseId, refundedAmount]
  );
}

async function storeMarketplaceRaffleWinners(itemId, guildId, winners) {
  for (let i = 0; i < winners.length; i++) {
    const winner = winners[i];
    await prizesPool.query(
      `INSERT INTO marketplace_raffle_winners (item_id, guild_id, discord_id, winner_rank, ticket_count)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (item_id, discord_id) DO NOTHING`,
      [itemId, guildId, winner.discordId, i + 1, winner.ticketCount]
    );
  }
}

async function getExpiredPublishedRaffles(limit = 10) {
  const { rows } = await prizesPool.query(
    `SELECT *
     FROM marketplace_items
     WHERE item_type = 'raffle'
       AND status = 'published'
       AND raffle_ends_at IS NOT NULL
       AND raffle_ends_at <= NOW()
     ORDER BY raffle_ends_at ASC
     LIMIT $1`,
    [limit]
  );
  return rows;
}

async function completeMarketplaceRaffle(itemId, actorDiscordId = client.user?.id || 'system') {
  const db = await prizesPool.connect();
  try {
    await db.query('BEGIN');
    const { rows } = await db.query(`SELECT * FROM marketplace_items WHERE id = $1 FOR UPDATE`, [itemId]);
    const item = rows[0];
    if (!item) {
      await db.query('ROLLBACK');
      return { ok: false, reason: 'Raffle item not found.' };
    }
    if (String(item.item_type) !== 'raffle') {
      await db.query('ROLLBACK');
      return { ok: false, reason: 'Item is not a raffle.' };
    }
    if (String(item.status) === 'completed') {
      await db.query('ROLLBACK');
      return { ok: true, item, winners: await getMarketplaceRaffleWinners(itemId), alreadyCompleted: true };
    }
    if (String(item.status) === 'cancelled') {
      await db.query('ROLLBACK');
      return { ok: false, reason: 'Raffle has already been cancelled.' };
    }
    if (item.raffle_ends_at && new Date(item.raffle_ends_at).getTime() > Date.now()) {
      await db.query('ROLLBACK');
      return { ok: false, reason: 'Raffle has not ended yet.' };
    }

    const leaderboardRes = await db.query(
      `SELECT discord_id, COALESCE(SUM(quantity), 0) AS ticket_count
       FROM marketplace_purchases
       WHERE item_id = $1
         AND refunded_at IS NULL
       GROUP BY discord_id
       ORDER BY ticket_count DESC, discord_id ASC`,
      [itemId]
    );
    const entries = leaderboardRes.rows.map((row) => ({
      discordId: String(row.discord_id || '').trim(),
      ticketCount: Number(row.ticket_count || 0),
    })).filter((entry) => entry.discordId && entry.ticketCount > 0);

    const winnerCount = Math.max(1, Math.floor(Number(item.total_stock || 1)));
    const winners = pickRandomTicketWinners(entries, winnerCount);

    await db.query(
      `UPDATE marketplace_items
       SET status = 'completed',
           completed_at = NOW(),
           updated_by_discord_id = $2,
           updated_at = NOW()
       WHERE id = $1`,
      [itemId, actorDiscordId]
    );

    for (let i = 0; i < winners.length; i++) {
      const winner = winners[i];
      await db.query(
        `INSERT INTO marketplace_raffle_winners (item_id, guild_id, discord_id, winner_rank, ticket_count)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (item_id, discord_id) DO NOTHING`,
        [itemId, item.guild_id, winner.discordId, i + 1, winner.ticketCount]
      );
    }

    await db.query('COMMIT');
    const completedItem = await getMarketplaceItemById(itemId);
    await refreshMarketplaceItemMessage(itemId);
    return { ok: true, item: completedItem || item, winners };
  } catch (err) {
    await db.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    db.release();
  }
}

let marketplaceRaffleInterval = null;

function startMarketplaceRaffleProcessor() {
  const run = async () => {
    try {
      const expired = await getExpiredPublishedRaffles(10);
      for (const raffle of expired) {
        await completeMarketplaceRaffle(raffle.id).catch((err) => {
          console.warn('Marketplace raffle completion failed:', String(err?.message || err || ''));
        });
      }
    } catch (err) {
      console.warn('Marketplace raffle processor failed:', String(err?.message || err || ''));
    }
  };
  if (marketplaceRaffleInterval) clearInterval(marketplaceRaffleInterval);
  marketplaceRaffleInterval = setInterval(run, 60 * 1000);
  run().catch(() => null);
}

async function resolveMarketplaceMemberIds(guildId, discordId) {
  const settings = await getGuildSettings(guildId);
  const links = await getWalletLinks(guildId, discordId);
  const walletAddress = links.find((x) => x.wallet_address)?.wallet_address || null;
  let resolved = null;
  try {
    resolved = await resolveDripMemberForDiscordUser(
      settings?.drip_realm_id,
      discordId,
      walletAddress,
      settings || {}
    );
  } catch {}
  return {
    settings,
    links,
    resolvedMember: resolved?.member || null,
    memberIds: collectUniqueDripMemberIds([
      ...collectDripMemberIdCandidates(resolved?.member || null),
      ...links.map((x) => x?.drip_member_id),
    ]),
  };
}

async function getMarketplaceSpendableBalance(guildId, discordId) {
  const { settings, memberIds, resolvedMember } = await resolveMarketplaceMemberIds(guildId, discordId);
  const botMemberId = resolveConfiguredDripSenderMemberId();
  if (!settings?.drip_api_key || !settings?.drip_realm_id) {
    return { ok: false, reason: 'Marketplace unavailable: DRIP is not fully configured.' };
  }
  if (!settings?.currency_id) return { ok: false, reason: 'Marketplace unavailable: Currency ID is not configured.' };
  if (!botMemberId) return { ok: false, reason: 'Marketplace unavailable: bot DRIP member ID is not configured.' };
  if (!memberIds.length) return { ok: false, reason: 'Marketplace unavailable: no DRIP member ID found for your account.' };
  return {
    ok: true,
    settings,
    memberIds,
    resolvedMember,
    botMemberId,
  };
}

async function postMarketplacePurchaseLog({ guild, actorDiscordId, item, quantity, spentAmount }) {
  await postAdminSystemLog({
    guild,
    category: 'Marketplace Purchase',
    message:
      `User: <@${actorDiscordId}>\n` +
      `Item: **${item.name}**\n` +
      `Type: ${normalizeMarketplaceItemType(item.item_type)}\n` +
      `Quantity: ${quantity}\n` +
      `Spent: ${spentAmount} $CHARM\n` +
      `Item ID: \`${item.id}\``
  });
}

async function postMarketplaceRefundLog({ guild, actorDiscordId, item, refundedCount, refundedAmount, failures = [] }) {
  await postAdminSystemLog({
    guild,
    category: 'Marketplace Refund',
    message:
      `Actor: <@${actorDiscordId}>\n` +
      `Item: **${item.name}**\n` +
      `Type: ${normalizeMarketplaceItemType(item.item_type)}\n` +
      `Refunded purchases: ${refundedCount}\n` +
      `Refunded amount: ${refundedAmount} $CHARM\n` +
      `Failures: ${failures.length ? failures.join(' | ').slice(0, 1200) : 'none'}\n` +
      `Item ID: \`${item.id}\``
  });
}

async function cancelMarketplaceRaffleAndRefund(guild, actorDiscordId, messageId) {
  const item = await getMarketplaceItemByPublishedMessage(guild.id, messageId);
  if (!item) return { ok: false, reason: 'No marketplace raffle found for that message ID.' };
  if (normalizeMarketplaceItemType(item.item_type) !== 'raffle') {
    return { ok: false, reason: 'That marketplace post is not a raffle.' };
  }
  if (String(item.status) === 'completed') {
    return { ok: false, reason: 'That raffle has already completed and winners were drawn.' };
  }

  const cancelledItem = String(item.status) === 'cancelled'
    ? item
    : await markMarketplaceItemCancelled(item.id, actorDiscordId);

  await refreshMarketplaceItemMessage(item.id);

  const refundablePurchases = await getRefundableMarketplacePurchases(item.id);
  if (!refundablePurchases.length) {
    return {
      ok: true,
      item: cancelledItem || item,
      refundedCount: 0,
      refundedAmount: 0,
      failures: [],
    };
  }

  const settings = await getGuildSettings(guild.id);
  const botMemberId = resolveConfiguredDripSenderMemberId();
  if (!settings?.drip_api_key || !settings?.drip_realm_id || !settings?.currency_id || !botMemberId) {
    return { ok: false, reason: 'Refund failed: DRIP sender configuration is incomplete.' };
  }

  let refundedCount = 0;
  let refundedAmount = 0;
  const failures = [];

  for (const purchase of refundablePurchases) {
    try {
      const resolved = await resolveMarketplaceMemberIds(guild.id, purchase.discord_id);
      if (!resolved.memberIds.length) {
        failures.push(`<@${purchase.discord_id}>: no DRIP member ID found`);
        continue;
      }
      const amount = Math.floor(Number(purchase.spent_amount || 0));
      if (!Number.isFinite(amount) || amount <= 0) {
        failures.push(`<@${purchase.discord_id}>: invalid refund amount`);
        continue;
      }

      await awardDripPoints(
        settings.drip_realm_id,
        resolved.memberIds,
        amount,
        settings.currency_id,
        settings,
        {
          context: 'marketplace_refund',
          initiatorDiscordId: actorDiscordId,
          recipientDiscordId: purchase.discord_id,
          senderMemberIdOverride: botMemberId,
        }
      );

      await markMarketplacePurchaseRefunded(purchase.id, amount);
      refundedCount += 1;
      refundedAmount += amount;
    } catch (err) {
      failures.push(`<@${purchase.discord_id}>: ${String(err?.message || err || '').slice(0, 180)}`);
    }
  }

  await postMarketplaceRefundLog({
    guild,
    actorDiscordId,
    item: cancelledItem || item,
    refundedCount,
    refundedAmount,
    failures,
  });

  return {
    ok: true,
    item: cancelledItem || item,
    refundedCount,
    refundedAmount,
    failures,
  };
}

async function purchaseMarketplaceItem(guild, discordId, itemId, quantity) {
  const normalizedQuantity = Math.floor(Number(quantity));
  if (!Number.isFinite(normalizedQuantity) || normalizedQuantity < 1) {
    return { ok: false, reason: 'Quantity must be at least 1.' };
  }

  const item = await getMarketplaceItemById(itemId);
  if (!item || String(item.guild_id) !== String(guild.id)) {
    return { ok: false, reason: 'Marketplace item not found.' };
  }
  if (String(item.status) !== 'published') {
    return { ok: false, reason: 'That item is not available right now.' };
  }

  const itemType = normalizeMarketplaceItemType(item.item_type);
  if (itemType === 'raffle' && formatMarketplaceTimeLeft(item) === 'Closed') {
    return { ok: false, reason: 'That raffle has already closed.' };
  }

  const allowedRoleIds = String(item.allowed_role_ids || '')
    .split(',')
    .map((x) => x.trim())
    .filter(Boolean);
  if (allowedRoleIds.length) {
    const member = await guild.members.fetch(discordId).catch(() => null);
    const hasAllowedRole = member && allowedRoleIds.some((id) => member.roles.cache.has(id));
    if (!hasAllowedRole) {
      return { ok: false, reason: 'You do not have an allowed role for this item.' };
    }
  }

  const balance = await getMarketplaceSpendableBalance(guild.id, discordId);
  if (!balance.ok) return balance;

  const stats = await getMarketplaceItemStats(itemId, discordId);
  const remainingStock = getMarketplaceRemainingStock(item, stats);
  if (remainingStock != null && normalizedQuantity > remainingStock) {
    return { ok: false, reason: `Only ${remainingStock} remain for this item.` };
  }
  const perUserLimit = Number(item.per_user_limit);
  if (itemType !== 'raffle' && Number.isFinite(perUserLimit) && perUserLimit > 0 && (stats.userQuantity + normalizedQuantity) > perUserLimit) {
    return { ok: false, reason: `You can only purchase ${perUserLimit} total for this item.` };
  }

  const totalCost = Math.floor(Number(item.price || 0)) * normalizedQuantity;
  if (totalCost <= 0) return { ok: false, reason: 'That item has an invalid price.' };

  const db = await prizesPool.connect();
  try {
    await db.query('BEGIN');
    const { rows: lockedRows } = await db.query(`SELECT * FROM marketplace_items WHERE id = $1 FOR UPDATE`, [itemId]);
    const lockedItem = lockedRows[0];
    if (!lockedItem || String(lockedItem.status) !== 'published') {
      await db.query('ROLLBACK');
      return { ok: false, reason: 'That item is not available right now.' };
    }
    const lockedStatsRes = await db.query(
      `SELECT
         COALESCE(SUM(quantity), 0) AS total_purchased,
         COALESCE(SUM(CASE WHEN discord_id = $2 THEN quantity ELSE 0 END), 0) AS user_quantity
       FROM marketplace_purchases
       WHERE item_id = $1`,
      [itemId, discordId]
    );
    const lockedStats = lockedStatsRes.rows[0] || {};
    const lockedTotalPurchased = Number(lockedStats.total_purchased || 0);
    const lockedUserQuantity = Number(lockedStats.user_quantity || 0);
    const lockedRemainingStock = itemType === 'raffle'
      ? (Number.isFinite(Number(lockedItem.per_user_limit)) && Number(lockedItem.per_user_limit) > 0
          ? Math.max(0, Number(lockedItem.per_user_limit) - lockedTotalPurchased)
          : null)
      : (Number.isFinite(Number(lockedItem.total_stock)) && Number(lockedItem.total_stock) > 0
          ? Math.max(0, Number(lockedItem.total_stock) - lockedTotalPurchased)
          : null);
    if (lockedRemainingStock != null && normalizedQuantity > lockedRemainingStock) {
      await db.query('ROLLBACK');
      return { ok: false, reason: `Only ${lockedRemainingStock} remain for this item.` };
    }
    const lockedPerUserLimit = Number(lockedItem.per_user_limit);
    if (itemType !== 'raffle' && Number.isFinite(lockedPerUserLimit) && lockedPerUserLimit > 0 && (lockedUserQuantity + normalizedQuantity) > lockedPerUserLimit) {
      await db.query('ROLLBACK');
      return { ok: false, reason: `You can only purchase ${lockedPerUserLimit} total for this item.` };
    }

    const transferResult = await awardDripPoints(
      balance.settings.drip_realm_id,
      [balance.botMemberId],
      totalCost,
      balance.settings.currency_id,
      balance.settings,
      {
        context: 'marketplace_purchase',
        initiatorDiscordId: discordId,
        recipientDiscordId: client.user?.id || null,
        recipientMemberIdOverride: balance.botMemberId,
        senderMemberIdOverride: balance.memberIds[0],
      }
    );

    await db.query(
      `INSERT INTO marketplace_purchases (item_id, guild_id, discord_id, quantity, spent_amount, purchase_type)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [itemId, guild.id, discordId, normalizedQuantity, totalCost, itemType]
    );
    await db.query('COMMIT');
    return {
      ok: true,
      item: lockedItem,
      quantity: normalizedQuantity,
      spentAmount: totalCost,
      transferResult,
    };
  } catch (err) {
    await db.query('ROLLBACK').catch(() => null);
    throw err;
  } finally {
    db.release();
  }
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
    `SELECT *, COALESCE(NULLIF(chain, ''), 'ethereum') AS chain FROM holder_rules WHERE guild_id = $1 AND enabled = TRUE ORDER BY id ASC`,
    [guildId]
  );
  return rows;
}

async function getTraitRoleRules(guildId) {
  const { rows } = await teamPool.query(
    `SELECT *, COALESCE(NULLIF(chain, ''), 'ethereum') AS chain FROM trait_role_rules WHERE guild_id = $1 AND enabled = TRUE ORDER BY id ASC`,
    [guildId]
  );
  return rows;
}

function defaultHolderCollections() {
  return [
    { name: 'Charm of the Ugly', chain: DEFAULT_NFT_CHAIN, contract_address: UGLY_CONTRACT.toLowerCase() },
    { name: 'Ugly Monsters', chain: DEFAULT_NFT_CHAIN, contract_address: MONSTER_CONTRACT.toLowerCase() },
    { name: 'Squigs', chain: DEFAULT_NFT_CHAIN, contract_address: OG_SQUIGS_CONTRACT.toLowerCase() },
    { name: 'Squigs Reloaded', chain: squigsChain(), contract_address: SQUIGS_CONTRACT.toLowerCase() },
  ];
}

async function getHolderCollections(guildId) {
  const { rows } = await teamPool.query(
    `SELECT name, COALESCE(NULLIF(chain, ''), 'ethereum') AS chain, contract_address, enabled FROM holder_collections WHERE guild_id = $1 ORDER BY created_at ASC`,
    [guildId]
  );
  const out = [];
  const seen = new Set();
  const overrides = new Map();
  for (const row of rows) {
    const key = collectionKey(row.chain, row.contract_address);
    if (key) overrides.set(key, row);
  }
  for (const defaultCollection of defaultHolderCollections()) {
    const key = collectionKey(defaultCollection.chain, defaultCollection.contract_address);
    const override = key ? overrides.get(key) : null;
    if (override && !override.enabled) continue;
    const c = override || defaultCollection;
    const chain = normalizeNftChain(c.chain) || DEFAULT_NFT_CHAIN;
    const addr = normalizeEthAddress(c.contract_address);
    if (!addr || !key || seen.has(key)) continue;
    seen.add(key);
    out.push({ name: String(c.name || addr), chain, contract_address: addr });
  }
  for (const c of rows.filter((row) => row.enabled)) {
    const chain = normalizeNftChain(c.chain) || DEFAULT_NFT_CHAIN;
    const addr = normalizeEthAddress(c.contract_address);
    const key = collectionKey(chain, addr);
    if (!addr || !key || seen.has(key)) continue;
    seen.add(key);
    out.push({ name: String(c.name || addr), chain, contract_address: addr });
  }
  return out;
}

async function upsertHolderCollection(guildId, name, contractAddress, chain = DEFAULT_NFT_CHAIN) {
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  await teamPool.query(
    `INSERT INTO holder_collections (guild_id, name, chain, contract_address, enabled)
     VALUES ($1, $2, $3, $4, TRUE)
     ON CONFLICT (guild_id, chain, contract_address) DO UPDATE
     SET name = EXCLUDED.name, enabled = TRUE`,
    [guildId, String(name || '').trim(), normalizedChain, String(contractAddress || '').toLowerCase()]
  );
}

async function removeHolderCollection(guildId, name, contractAddress, chain = DEFAULT_NFT_CHAIN) {
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  const normalizedContract = normalizeEthAddress(contractAddress);
  if (!normalizedContract) throw new Error('Invalid collection contract address.');

  const client = await teamPool.connect();
  let holderRuleCount = 0;
  let traitRuleCount = 0;
  try {
    await client.query('BEGIN');
    await client.query(
      `INSERT INTO holder_collections (guild_id, name, chain, contract_address, enabled)
       VALUES ($1, $2, $3, $4, FALSE)
       ON CONFLICT (guild_id, chain, contract_address) DO UPDATE SET enabled = FALSE`,
      [guildId, String(name || normalizedContract).trim(), normalizedChain, normalizedContract]
    );
    const holderResult = await client.query(
      `UPDATE holder_rules SET enabled = FALSE
       WHERE guild_id = $1 AND chain = $2 AND contract_address = $3 AND enabled = TRUE`,
      [guildId, normalizedChain, normalizedContract]
    );
    const traitResult = await client.query(
      `UPDATE trait_role_rules SET enabled = FALSE
       WHERE guild_id = $1 AND chain = $2 AND contract_address = $3 AND enabled = TRUE`,
      [guildId, normalizedChain, normalizedContract]
    );
    holderRuleCount = holderResult.rowCount || 0;
    traitRuleCount = traitResult.rowCount || 0;
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }

  const mappingResult = await pointsPool.query(
    `DELETE FROM holder_point_mappings WHERE guild_id = $1 AND chain = $2 AND contract_address = $3`,
    [guildId, normalizedChain, normalizedContract]
  );
  return { holderRuleCount, traitRuleCount, mappingCount: mappingResult.rowCount || 0 };
}

async function setGuildPointMapping(guildId, contractAddress, mappingTable, actorDiscordId = null, chain = DEFAULT_NFT_CHAIN) {
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  await pointsPool.query(
    `INSERT INTO holder_point_mappings (guild_id, chain, contract_address, mapping_json, created_by_discord_id, updated_at)
     VALUES ($1, $2, $3, $4::jsonb, $5, NOW())
     ON CONFLICT (guild_id, chain, contract_address) DO UPDATE
     SET mapping_json = EXCLUDED.mapping_json,
         created_by_discord_id = COALESCE(holder_point_mappings.created_by_discord_id, EXCLUDED.created_by_discord_id),
         updated_at = NOW()`,
    [guildId, normalizedChain, String(contractAddress || '').toLowerCase(), JSON.stringify(mappingTable || {}), actorDiscordId ? String(actorDiscordId) : null]
  );
}

async function getGuildPointMappings(guildId) {
  const { rows } = await pointsPool.query(
    `SELECT COALESCE(NULLIF(chain, ''), 'ethereum') AS chain, contract_address, mapping_json FROM holder_point_mappings WHERE guild_id = $1`,
    [guildId]
  );
  const out = new Map();
  for (const r of rows) {
    const chain = normalizeNftChain(r.chain) || DEFAULT_NFT_CHAIN;
    const c = normalizeEthAddress(r.contract_address);
    if (!c) continue;
    const table = (r.mapping_json && typeof r.mapping_json === 'object') ? r.mapping_json : {};
    out.set(pointMappingKey(chain, c), table);
    if (chain === DEFAULT_NFT_CHAIN) out.set(c, table);
  }
  return out;
}

async function getGuildPointMappingsWithOwners(guildId) {
  const { rows } = await pointsPool.query(
    `SELECT COALESCE(NULLIF(chain, ''), 'ethereum') AS chain, contract_address, created_by_discord_id FROM holder_point_mappings WHERE guild_id = $1`,
    [guildId]
  );
  const out = [];
  for (const r of rows) {
    const chain = normalizeNftChain(r.chain) || DEFAULT_NFT_CHAIN;
    const contractAddress = normalizeEthAddress(r.contract_address);
    if (!contractAddress) continue;
    out.push({
      chain,
      contractAddress,
      createdByDiscordId: r.created_by_discord_id ? String(r.created_by_discord_id) : null,
    });
  }
  return out;
}

async function removeGuildPointMapping(guildId, contractAddress, actorDiscordId, chain = DEFAULT_NFT_CHAIN) {
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  const { rows } = await pointsPool.query(
    `SELECT created_by_discord_id FROM holder_point_mappings WHERE guild_id = $1 AND chain = $2 AND contract_address = $3`,
    [guildId, normalizedChain, String(contractAddress || '').toLowerCase()]
  );
  const row = rows[0] || null;
  if (!row) return { ok: false, reason: 'not_found' };

  const actorId = String(actorDiscordId || '');
  const ownerId = row.created_by_discord_id ? String(row.created_by_discord_id) : null;
  const isDefaultAdminUser = getDefaultAdminIds().has(actorId);
  const canDelete = isDefaultAdminUser || (ownerId && actorId === ownerId);
  if (!canDelete) return { ok: false, reason: 'forbidden', ownerId };

  await pointsPool.query(
    `DELETE FROM holder_point_mappings WHERE guild_id = $1 AND chain = $2 AND contract_address = $3`,
    [guildId, normalizedChain, String(contractAddress || '').toLowerCase()]
  );
  return { ok: true, ownerId };
}

async function addHolderRule(guild, { roleId, contractAddress, minTokens, maxTokens, chain = DEFAULT_NFT_CHAIN }) {
  const role = guild.roles.cache.get(roleId);
  if (!role) throw new Error(`Role not found: ${roleId}`);
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  await teamPool.query(
    `INSERT INTO holder_rules (guild_id, role_id, role_name, chain, contract_address, min_tokens, max_tokens, enabled)
     VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)`,
    [guild.id, role.id, role.name, normalizedChain, contractAddress.toLowerCase(), minTokens, maxTokens]
  );
  return role;
}

async function addTraitRoleRule(guild, { roleId, contractAddress, traitCategory, traitValue, chain = DEFAULT_NFT_CHAIN }) {
  const role = guild.roles.cache.get(roleId);
  if (!role) throw new Error(`Role not found: ${roleId}`);
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  await teamPool.query(
    `INSERT INTO trait_role_rules (guild_id, role_id, role_name, chain, contract_address, trait_category, trait_value, enabled)
     VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)`,
    [guild.id, role.id, role.name, normalizedChain, contractAddress.toLowerCase(), traitCategory || null, traitValue]
  );
  return role;
}

async function disableHolderRule(guildId, ruleId) {
  const { rows } = await teamPool.query(
    `UPDATE holder_rules SET enabled = FALSE WHERE guild_id = $1 AND id = $2 AND enabled = TRUE RETURNING id, role_name, COALESCE(NULLIF(chain, ''), 'ethereum') AS chain, contract_address, min_tokens, max_tokens`,
    [guildId, ruleId]
  );
  return rows[0] || null;
}

async function disableTraitRoleRule(guildId, ruleId) {
  const { rows } = await teamPool.query(
    `UPDATE trait_role_rules
     SET enabled = FALSE
     WHERE guild_id = $1 AND id = $2 AND enabled = TRUE
     RETURNING id, role_name, COALESCE(NULLIF(chain, ''), 'ethereum') AS chain, contract_address, trait_category, trait_value`,
    [guildId, ruleId]
  );
  return rows[0] || null;
}

async function getOwnedTokenIdsForContractsAlchemy(walletAddress, contractAddresses, chain = DEFAULT_NFT_CHAIN) {
  if (!ALCHEMY_API_KEY) throw new Error('Alchemy API key is not configured.');
  const normalizedWallet = normalizeEthAddress(walletAddress);
  const contracts = [...new Set(
    (Array.isArray(contractAddresses) ? contractAddresses : [contractAddresses])
      .map((addr) => String(addr || '').toLowerCase())
      .filter(Boolean)
  )];
  const out = new Map(contracts.map((contractAddress) => [collectionKey(chain, contractAddress), []]));
  if (!normalizedWallet || !contracts.length) return out;

  let pageKey = null;
  do {
    const u = new URL(alchemyNftUrl(chain, 'getNFTsForOwner'));
    u.searchParams.set('owner', normalizedWallet);
    for (const contractAddress of contracts) {
      u.searchParams.append('contractAddresses[]', contractAddress);
    }
    u.searchParams.set('withMetadata', 'false');
    u.searchParams.set('pageSize', '100');
    if (pageKey) u.searchParams.set('pageKey', pageKey);

    const res = await fetchWithRetry(u.toString(), 3, 500);
    const data = await res.json();
    for (const nft of (data?.ownedNfts || [])) {
      const nftContract = String(nft?.contract?.address || '').toLowerCase();
      const key = collectionKey(chain, nftContract);
      if (!out.has(key)) continue;
      const tid = String(nft.tokenId || '').trim();
      if (!tid) continue;
      try {
        out.get(key).push(tid.startsWith('0x') ? BigInt(tid).toString(10) : tid);
      } catch {
        out.get(key).push(tid);
      }
    }
    pageKey = data?.pageKey || null;
  } while (pageKey);

  return out;
}

const OPENSEA_OWNERSHIP_CACHE_TTL_MS = Math.max(
  0,
  numberFromEnv('OPENSEA_OWNERSHIP_CACHE_TTL_MS', 5 * 60 * 1000)
);
const openSeaOwnershipCache = new Map();

function publicRpcUrlForChain(chain = DEFAULT_NFT_CHAIN) {
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  if (normalizedChain === 'ethereum') {
    return process.env.ETHEREUM_PUBLIC_RPC_URL || 'https://ethereum-rpc.publicnode.com';
  }
  if (normalizedChain === 'base') return process.env.BASE_PUBLIC_RPC_URL || 'https://mainnet.base.org';
  if (normalizedChain === 'abstract') return process.env.ABSTRACT_PUBLIC_RPC_URL || 'https://api.mainnet.abs.xyz';
  return null;
}

async function getErc721BalanceOnChain(walletAddress, contractAddress, chain = DEFAULT_NFT_CHAIN) {
  const wallet = normalizeEthAddress(walletAddress);
  const contract = normalizeEthAddress(contractAddress);
  const rpcUrl = publicRpcUrlForChain(chain);
  if (!wallet || !contract || !rpcUrl) throw new Error('No public RPC balance fallback is available.');

  const data = `0x70a08231${wallet.slice(2).padStart(64, '0')}`;
  const res = await fetchWithRetry(rpcUrl, 3, 750, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: 1,
      method: 'eth_call',
      params: [{ to: contract, data }, 'latest'],
    }),
    timeoutMs: 10000,
  });
  const payload = await res.json();
  if (payload?.error) {
    throw new Error(`RPC balanceOf failed: ${sanitizeProviderErrorDetail(payload.error.message || JSON.stringify(payload.error))}`);
  }
  const raw = String(payload?.result || '');
  if (!/^0x[0-9a-f]+$/i.test(raw)) throw new Error('RPC balanceOf returned an invalid result.');
  const balance = BigInt(raw);
  if (balance > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error('RPC balanceOf result is too large.');
  return Number(balance);
}

async function getOpenSeaAccountNfts(walletAddress, chain = DEFAULT_NFT_CHAIN) {
  if (!OPENSEA_API_KEY) throw new Error('OpenSea API key is not configured.');
  const normalizedWallet = normalizeEthAddress(walletAddress);
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  if (!normalizedWallet) return [];

  const cacheKey = `${normalizedChain}:${normalizedWallet}`;
  const cached = openSeaOwnershipCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) return cached.promise;

  const promise = (async () => {
    const cfg = nftChainConfig(normalizedChain);
    const out = [];
    let next = null;
    do {
      const u = new URL(`https://api.opensea.io/api/v2/chain/${cfg.openseaChain}/account/${normalizedWallet}/nfts`);
      u.searchParams.set('limit', '200');
      if (next) u.searchParams.set('next', next);
      const res = await fetchWithRetry(u.toString(), 4, 1000, {
        headers: { accept: 'application/json', 'x-api-key': OPENSEA_API_KEY },
        timeoutMs: 15000,
      });
      const data = await res.json();
      for (const nft of (Array.isArray(data?.nfts) ? data.nfts : [])) {
        const contractAddress = String(nft?.contract?.address || nft?.contract || '').toLowerCase();
        const tokenId = String(nft?.identifier ?? nft?.token_id ?? '').trim();
        if (contractAddress && tokenId) out.push({ contractAddress, tokenId });
      }
      next = String(data?.next || '').trim() || null;
    } while (next);
    return out;
  })();

  openSeaOwnershipCache.set(cacheKey, {
    expiresAt: Date.now() + OPENSEA_OWNERSHIP_CACHE_TTL_MS,
    promise,
  });
  if (openSeaOwnershipCache.size > 500) {
    const oldestKey = openSeaOwnershipCache.keys().next().value;
    if (oldestKey && oldestKey !== cacheKey) openSeaOwnershipCache.delete(oldestKey);
  }
  promise.catch(() => openSeaOwnershipCache.delete(cacheKey));
  return promise;
}

async function getOwnedTokenIdsForContractsOpenSea(walletAddress, contractAddresses, chain = DEFAULT_NFT_CHAIN) {
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  const contracts = [...new Set(
    (Array.isArray(contractAddresses) ? contractAddresses : [contractAddresses])
      .map((addr) => normalizeEthAddress(addr))
      .filter(Boolean)
  )];
  const out = new Map(contracts.map((contractAddress) => [collectionKey(normalizedChain, contractAddress), []]));
  if (!contracts.length) return out;

  const wanted = new Set(contracts);
  const nfts = await getOpenSeaAccountNfts(walletAddress, normalizedChain);
  for (const nft of nfts) {
    if (!wanted.has(nft.contractAddress)) continue;
    const key = collectionKey(normalizedChain, nft.contractAddress);
    if (!out.has(key)) continue;
    try {
      out.get(key).push(nft.tokenId.startsWith('0x') ? BigInt(nft.tokenId).toString(10) : nft.tokenId);
    } catch {
      out.get(key).push(nft.tokenId);
    }
  }
  return out;
}

function providerFailureSummary(provider, err) {
  const status = Number(err?.status || 0);
  const message = sanitizeProviderErrorDetail(err?.responseDetail || err?.message || err || 'unknown error');
  return `${provider}${status ? ` HTTP ${status}` : ''}: ${message}`.slice(0, 600);
}

async function getOwnedTokenIdsForContracts(walletAddress, contractAddresses, chain = DEFAULT_NFT_CHAIN) {
  let openSeaError = null;
  let openSeaResult = null;
  try {
    openSeaResult = await getOwnedTokenIdsForContractsOpenSea(walletAddress, contractAddresses, chain);
    const tokenCount = [...openSeaResult.values()].reduce((sum, tokenIds) => sum + tokenIds.length, 0);
    if (tokenCount > 0) return openSeaResult;
  } catch (err) {
    openSeaError = err;
    console.warn(`${providerFailureSummary('OpenSea ownership lookup failed', err)}; trying Alchemy.`);
  }

  try {
    const alchemyResult = await getOwnedTokenIdsForContractsAlchemy(walletAddress, contractAddresses, chain);
    const tokenCount = [...alchemyResult.values()].reduce((sum, tokenIds) => sum + tokenIds.length, 0);
    if (tokenCount > 0) {
      if (openSeaResult) {
        console.warn(
          `OpenSea returned no ownership records for ${normalizeEthAddress(walletAddress) || walletAddress}; ` +
          `Alchemy returned ${tokenCount}.`
        );
      }
      return alchemyResult;
    }
    return openSeaResult || alchemyResult;
  } catch (alchemyError) {
    if (openSeaResult) return openSeaResult;
    const err = new Error(
      `Ownership providers unavailable. ${providerFailureSummary('OpenSea', openSeaError)}; ` +
      providerFailureSummary('Alchemy', alchemyError)
    );
    err.openSeaError = openSeaError;
    err.alchemyError = alchemyError;
    throw err;
  }
}

async function getOwnedTokenIdsForContract(walletAddress, contractAddress, chain = DEFAULT_NFT_CHAIN) {
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  const normalizedContract = normalizeEthAddress(contractAddress);
  if (!normalizedContract) return [];
  const ownedMap = await getOwnedTokenIdsForContracts(walletAddress, [normalizedContract], normalizedChain);
  return ownedMap.get(collectionKey(normalizedChain, normalizedContract)) || [];
}

async function countOwnedForContract(walletAddress, contractAddress, chain = DEFAULT_NFT_CHAIN) {
  const ids = await getOwnedTokenIdsForContract(walletAddress, contractAddress, chain);
  return ids.length;
}

async function getOwnedTokenIdsForContractMany(walletAddresses, contractAddress, chain = DEFAULT_NFT_CHAIN, options = {}) {
  const addresses = Array.isArray(walletAddresses) ? walletAddresses : [walletAddresses];
  const normalized = [...new Set(addresses.map(a => normalizeEthAddress(a)).filter(Boolean))];
  if (!normalized.length) return [];
  const suppressErrors = options.suppressErrors !== false;
  const parsedConcurrency = Number(options.concurrency || 2);
  const concurrency = Math.max(1, Number.isFinite(parsedConcurrency) ? parsedConcurrency : 2);
  const tokenArrays = await mapLimit(normalized, concurrency, async (walletAddress) => {
    try {
      return await getOwnedTokenIdsForContract(walletAddress, contractAddress, chain);
    } catch (err) {
      if (!suppressErrors) throw err;
      return [];
    }
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
  const tokenIdsByContract = new Map();
  const fallbackCountsByContract = new Map();
  const unavailableContracts = new Set();
  const unavailableTraitContracts = new Set();

  const contractGroups = new Map();
  for (const r of [...rules, ...traitRules]) {
    const chain = normalizeNftChain(r.chain) || DEFAULT_NFT_CHAIN;
    const contractAddress = String(r.contract_address || '').toLowerCase();
    const key = collectionKey(chain, contractAddress);
    if (!key) continue;
    if (!contractGroups.has(chain)) contractGroups.set(chain, new Map());
    contractGroups.get(chain).set(key, contractAddress);
    if (!tokenIdsByContract.has(key)) tokenIdsByContract.set(key, new Set());
  }

  for (const [chain, contractMap] of contractGroups.entries()) {
    const contractAddresses = [...contractMap.values()];
    await mapLimit(normalizedAddresses, 1, async (walletAddress) => {
      try {
        const ownedMap = await getOwnedTokenIdsForContracts(walletAddress, contractAddresses, chain);
        for (const [key, tokenIds] of ownedMap.entries()) {
          if (!tokenIdsByContract.has(key)) tokenIdsByContract.set(key, new Set());
          for (const tokenId of tokenIds) {
            tokenIdsByContract.get(key).add(String(tokenId));
          }
        }
      } catch (err) {
        console.warn(
          `⚠️ Holder ownership sync unavailable for ${chain}/${walletAddress}: ` +
          sanitizeProviderErrorDetail(err?.message || err)
        );
        for (const [key, contractAddress] of contractMap.entries()) {
          try {
            const balance = await getErc721BalanceOnChain(walletAddress, contractAddress, chain);
            fallbackCountsByContract.set(key, (fallbackCountsByContract.get(key) || 0) + balance);
            if (traitRules.some((rule) => collectionKey(rule.chain, rule.contract_address) === key)) {
              unavailableTraitContracts.add(key);
            }
            console.warn(`⚠️ Used on-chain balance fallback for ${key}: ${balance}.`);
          } catch (rpcError) {
            console.warn(
              `⚠️ On-chain balance fallback failed for ${key}: ` +
              sanitizeProviderErrorDetail(rpcError?.message || rpcError)
            );
            unavailableContracts.add(key);
            unavailableTraitContracts.add(key);
          }
        }
      }
    });
  }

  for (const [key, tokenIds] of tokenIdsByContract.entries()) {
    byContract.set(key, tokenIds.size + (fallbackCountsByContract.get(key) || 0));
  }

  const guildPointMappings = traitRules.length ? await getGuildPointMappings(member.guild.id) : new Map();
  const traitMatchesByRuleId = new Map();
  const traitContracts = [...new Set(traitRules.map((r) => collectionKey(r.chain, r.contract_address)).filter(Boolean))];
  for (const traitContractKey of traitContracts) {
    const contractRules = traitRules.filter((r) => collectionKey(r.chain, r.contract_address) === traitContractKey);
    if (!contractRules.length) continue;
    const chain = normalizeNftChain(contractRules[0].chain) || DEFAULT_NFT_CHAIN;
    const contractAddress = String(contractRules[0].contract_address || '').toLowerCase();

    const table = hpTableForContract(contractAddress, guildPointMappings, chain);
    const eligibleRules = contractRules.filter((r) => findMatchingTraitDefinition(table, r.trait_category, r.trait_value));
    for (const r of contractRules) {
      traitMatchesByRuleId.set(r.id, false);
    }
    if (!eligibleRules.length) continue;

    const tokenIds = [...(tokenIdsByContract.get(traitContractKey) || new Set())];
    if (!tokenIds.length) continue;

    for (const tokenId of tokenIds) {
      let grouped = null;
      try {
        const { attrs } = await getTraitsForTokenResilient(tokenId, contractAddress, chain);
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
  const roleDecisions = new Map();
  const queueRoleDecision = (role, shouldHave, reason) => {
    const existing = roleDecisions.get(role.id) || { role, entries: [] };
    existing.entries.push({ shouldHave: Boolean(shouldHave), reason });
    roleDecisions.set(role.id, existing);
  };

  for (const r of rules) {
    const chain = normalizeNftChain(r.chain) || DEFAULT_NFT_CHAIN;
    const key = collectionKey(chain, r.contract_address);
    const role = member.guild.roles.cache.get(r.role_id);
    if (!role) {
      applied.push(`${r.role_name || r.role_id}: skipped (role not found)`);
      continue;
    }
    if (!role.editable) {
      applied.push(`${role.name}: skipped (bot cannot manage this role/hierarchy)`);
      continue;
    }
    if (unavailableContracts.has(key)) {
      applied.push(`${role.name}: skipped (ownership check temporarily unavailable for ${chainAddressLabel(chain, r.contract_address)})`);
      continue;
    }

    const count = byContract.get(key) || 0;
    const shouldHave = count >= Number(r.min_tokens) && (r.max_tokens == null || count <= Number(r.max_tokens));

    queueRoleDecision(
      role,
      shouldHave,
      `holder rule ${chainAddressLabel(chain, r.contract_address)}: ${count} in range ${r.min_tokens}-${r.max_tokens ?? '∞'}`
    );
    applied.push(`${role.name}: ${count} (${shouldHave ? 'eligible' : 'not eligible'})`);
  }

  for (const r of traitRules) {
    const role = member.guild.roles.cache.get(r.role_id);
    const categoryLabel = r.trait_category ? String(r.trait_category) : 'any';
    const traitLabel = `${categoryLabel}:${r.trait_value}`;
    const key = collectionKey(r.chain, r.contract_address);
    if (!role) {
      applied.push(`${r.role_name || r.role_id}: skipped (role not found)`);
      continue;
    }
    if (!role.editable) {
      applied.push(`${role.name}: skipped (bot cannot manage this role/hierarchy)`);
      continue;
    }
    if (unavailableContracts.has(key) || unavailableTraitContracts.has(key)) {
      applied.push(`${role.name}: skipped (trait ownership check temporarily unavailable for ${chainAddressLabel(r.chain, r.contract_address)})`);
      continue;
    }

    const shouldHave = Boolean(traitMatchesByRuleId.get(r.id));
    queueRoleDecision(
      role,
      shouldHave,
      `trait rule ${traitLabel} on ${chainAddressLabel(r.chain, r.contract_address)}`
    );
    applied.push(`${role.name}: ${traitLabel} (${shouldHave ? 'eligible' : 'not eligible'})`);
  }

  for (const decision of roleDecisions.values()) {
    const { role, entries } = decision;
    const shouldHave = entries.some((entry) => entry.shouldHave);
    const eligibleReason = entries.find((entry) => entry.shouldHave)?.reason || 'holder verification';
    const hasRole = member.roles.cache.has(role.id);
    try {
      if (shouldHave && !hasRole) {
        await member.roles.add(role, `Holder verification (${eligibleReason})`);
        changed++;
        granted.push(role.name);
      }
      if (!shouldHave && hasRole) {
        await member.roles.remove(role, 'Holder verification (no matching holder or trait rules eligible)');
        changed++;
      }
    } catch (err) {
      if (err?.code === 50001 || err?.code === 50013) {
        applied.push(`${role.name}: skipped (missing access/permissions)`);
        continue;
      }
      throw err;
    }
  }
  return { changed, applied, granted };
}

async function computeWalletStatsForPayout(guildId, walletAddresses, payoutType) {
  const addresses = Array.isArray(walletAddresses) ? walletAddresses : [walletAddresses];
  const normalizedAddresses = [...new Set(addresses.map(a => normalizeEthAddress(a)).filter(Boolean))];
  if (!normalizedAddresses.length) return { unitTotal: 0, totalNfts: 0, totalUp: 0, byCollection: [] };

  const rules = await getHolderRules(guildId);
  const guildPointMappings = await getGuildPointMappings(guildId);
  const contractMap = new Map();
  for (const r of rules) {
    const key = collectionKey(r.chain, r.contract_address);
    if (!key || contractMap.has(key)) continue;
    contractMap.set(key, {
      chain: normalizeNftChain(r.chain) || DEFAULT_NFT_CHAIN,
      contractAddress: String(r.contract_address || '').toLowerCase(),
    });
  }
  const contracts = [...contractMap.values()];
  if (!contracts.length) return { unitTotal: 0, totalNfts: 0, totalUp: 0, byCollection: [] };

  const byCollection = await mapLimit(contracts, 3, async ({ chain, contractAddress }) => {
    const ids = await getOwnedTokenIdsForContractMany(normalizedAddresses, contractAddress, chain);
    return { chain, contractAddress, ids };
  });
  const totalNfts = byCollection.reduce((sum, x) => sum + x.ids.length, 0);

  let totalUp = 0;
  if (payoutType === 'per_up') {
    const scorableContracts = byCollection.filter(({ chain, contractAddress }) => {
      const table = hpTableForContract(contractAddress, guildPointMappings, chain);
      return table && Object.keys(table).length > 0;
    });
    const perContractTotals = await mapLimit(scorableContracts, 2, async ({ chain, contractAddress, ids }) => {
      const ups = await mapLimit(ids, 5, async (tokenId) => {
        try {
          const { attrs } = await getTraitsForTokenResilient(tokenId, contractAddress, chain);
          const grouped = normalizeTraits(attrs);
          const table = hpTableForContract(contractAddress, guildPointMappings, chain);
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
    byCollection: byCollection.map(({ chain, contractAddress, ids }) => ({ chain, contractAddress, count: ids.length })),
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

  const effectiveUnits = verifiedStats.unitTotal + unverifiedStats.unitTotal;
  const unverifiedPenaltyAmount = 0;
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

async function getRewardHealthSummary(guildId, settings = null) {
  const payoutType = settings?.payout_type === 'per_nft' ? 'per_nft' : 'per_up';
  const payoutAmount = Number(settings?.payout_amount || 0);
  const rules = await getHolderRules(guildId);
  const guildPointMappings = await getGuildPointMappings(guildId);
  const earningContracts = new Map();

  for (const r of rules) {
    const chain = normalizeNftChain(r.chain) || DEFAULT_NFT_CHAIN;
    const contractAddress = normalizeEthAddress(r.contract_address);
    const key = collectionKey(chain, contractAddress);
    if (!key || earningContracts.has(key)) continue;
    const table = hpTableForContract(contractAddress, guildPointMappings, chain);
    const hasScoringTable = Boolean(table && Object.keys(table).length > 0);
    const hasCustomMapping = Boolean(getPointMappingForContract(guildPointMappings, contractAddress, chain));
    earningContracts.set(key, {
      chain,
      contractAddress,
      label: labelForContract(contractAddress, chain),
      hasScoringTable,
      hasCustomMapping,
      ruleCount: 0,
    });
  }

  for (const r of rules) {
    const key = collectionKey(r.chain, r.contract_address);
    if (key && earningContracts.has(key)) earningContracts.get(key).ruleCount += 1;
  }

  const contracts = [...earningContracts.values()];
  return {
    payoutType,
    payoutAmount,
    holderRuleCount: rules.length,
    earningContracts: contracts,
    scorableContracts: contracts.filter((c) => c.hasScoringTable),
    unscoredContracts: contracts.filter((c) => !c.hasScoringTable),
  };
}

const DAY_IN_MS = 24 * 60 * 60 * 1000;
const PASSIVE_REWARD_START_AT = parsePassiveRewardStartAt(
  process.env.PASSIVE_REWARD_START_AT || process.env.CHARM_PASSIVE_REWARD_START_AT
);

function nftClaimKey(chain, contractAddress, tokenId) {
  if (tokenId == null) {
    tokenId = contractAddress;
    contractAddress = chain;
    chain = DEFAULT_NFT_CHAIN;
  }
  return `${normalizeNftChain(chain) || DEFAULT_NFT_CHAIN}:${String(contractAddress || '').toLowerCase()}:${String(tokenId || '')}`;
}

function parsePassiveRewardStartAt(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const parsed = new Date(raw);
  if (!Number.isFinite(parsed.getTime())) {
    console.warn(`Invalid PASSIVE_REWARD_START_AT ignored: ${raw}`);
    return null;
  }
  return parsed;
}

function validRewardBaselineDate(value, now = new Date()) {
  if (value == null || String(value).trim() === '') return null;
  const parsed = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(parsed.getTime())) return null;
  return parsed.getTime() > now.getTime() ? now : parsed;
}

function passiveRewardStartDate(ruleStartAt = null, now = new Date()) {
  return validRewardBaselineDate(PASSIVE_REWARD_START_AT, now)
    || validRewardBaselineDate(ruleStartAt, now)
    || now;
}

function formatNumber(value, digits = 2) {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return '0';
  if (Number.isInteger(num)) return String(num);
  return num.toFixed(digits).replace(/\.?0+$/, '');
}

function formatElapsedTimeSimple(ms) {
  const totalMs = Math.max(0, Number(ms || 0));
  const totalMinutes = Math.floor(totalMs / (60 * 1000));
  if (totalMinutes < 1) return 'less than a minute';
  if (totalMinutes < 60) return `${totalMinutes} minute${totalMinutes === 1 ? '' : 's'}`;

  const totalHours = Math.floor(totalMinutes / 60);
  if (totalHours < 24) return `${totalHours} hour${totalHours === 1 ? '' : 's'}`;

  const totalDays = Math.floor(totalHours / 24);
  if (totalDays < 7) return `${totalDays} day${totalDays === 1 ? '' : 's'}`;

  const totalWeeks = Math.floor(totalDays / 7);
  if (totalWeeks < 5) return `${totalWeeks} week${totalWeeks === 1 ? '' : 's'}`;

  const totalMonths = Math.floor(totalDays / 30);
  if (totalMonths < 12) return `${totalMonths} month${totalMonths === 1 ? '' : 's'}`;

  const totalYears = Math.floor(totalDays / 365);
  return `${totalYears} year${totalYears === 1 ? '' : 's'}`;
}

function calculatePassiveClaimTotals(nftEntries, payoutAmount) {
  let totalUnits = 0;
  let baseAmount = 0;
  let unverifiedPenaltyAmount = 0;
  let rawClaimableAmount = 0;

  for (const entry of (Array.isArray(nftEntries) ? nftEntries : [])) {
    const elapsedMs = Math.max(0, Number(entry.elapsedMs || 0));
    const unitValue = Number(entry.unitValue || 0);
    const fullAmount = (elapsedMs / DAY_IN_MS) * unitValue * Number(payoutAmount || 0);
    const walletPenalty = 0;
    const claimableForNft = fullAmount - walletPenalty;
    entry.claimableAmount = claimableForNft;
    totalUnits += unitValue;
    baseAmount += fullAmount;
    unverifiedPenaltyAmount += walletPenalty;
    rawClaimableAmount += claimableForNft;
  }

  return {
    totalUnits,
    baseAmount: Math.max(0, Math.floor(baseAmount)),
    unverifiedPenaltyAmount: Math.max(0, Math.floor(unverifiedPenaltyAmount)),
    claimableAmount: Math.max(0, Math.floor(rawClaimableAmount)),
  };
}

async function getStoredNftClaimStates(guildId, nftEntries) {
  const tokenIdsByContract = new Map();
  for (const entry of nftEntries) {
    const chain = normalizeNftChain(entry.chain) || DEFAULT_NFT_CHAIN;
    const contractAddress = String(entry.contractAddress || '').toLowerCase();
    const tokenId = String(entry.tokenId || '');
    if (!contractAddress || !tokenId) continue;
    const key = collectionKey(chain, contractAddress);
    const bucket = tokenIdsByContract.get(key) || { chain, contractAddress, tokenIds: [] };
    bucket.tokenIds.push(tokenId);
    tokenIdsByContract.set(key, bucket);
  }

  const out = new Map();
  await Promise.all(
    [...tokenIdsByContract.values()].map(async ({ chain, contractAddress, tokenIds }) => {
      const { rows } = await claimsPool.query(
        `WITH normal_claims AS (
           SELECT
             COALESCE(NULLIF(chain, ''), 'ethereum') AS chain,
             contract_address,
             token_id,
             last_claimed_at
           FROM nft_claims
           WHERE guild_id = $1 AND chain = $2 AND contract_address = $3 AND token_id = ANY($4::TEXT[])
         ),
         completed_attempts AS (
           SELECT
             COALESCE(NULLIF(chain, ''), 'ethereum') AS chain,
             contract_address,
             token_id,
             MAX(transfer_succeeded_at) AS last_claimed_at
           FROM claim_attempt_nfts
           WHERE guild_id = $1
             AND chain = $2
             AND contract_address = $3
             AND token_id = ANY($4::TEXT[])
             AND status IN ('transfer_succeeded', 'recorded')
             AND transfer_succeeded_at IS NOT NULL
           GROUP BY COALESCE(NULLIF(chain, ''), 'ethereum'), contract_address, token_id
         )
         SELECT chain, contract_address, token_id, MAX(last_claimed_at) AS last_claimed_at
         FROM (
           SELECT * FROM normal_claims
           UNION ALL
           SELECT * FROM completed_attempts
         ) s
         GROUP BY chain, contract_address, token_id`,
        [guildId, chain, contractAddress, [...new Set(tokenIds)]]
      );
      for (const row of rows) {
        out.set(nftClaimKey(row.chain, row.contract_address, row.token_id), row);
      }
    })
  );
  return out;
}

function createClaimAttemptId() {
  return crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString('hex');
}

async function createPassiveClaimAttempt(guildId, discordId, rewardQuote) {
  const claimAttemptId = createClaimAttemptId();
  const createdAt = new Date();
  await Promise.all(
    rewardQuote.nftEntries.map((entry) =>
      claimsPool.query(
        `INSERT INTO claim_attempt_nfts (
           claim_attempt_id, guild_id, discord_id, chain, contract_address, token_id, wallet_address,
           payout_type, unit_value, payout_amount, claimable_amount, status, created_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'pending', $12)`,
        [
          claimAttemptId,
          guildId,
          discordId,
          normalizeNftChain(entry.chain) || DEFAULT_NFT_CHAIN,
          entry.contractAddress,
          entry.tokenId,
          entry.walletAddress || null,
          rewardQuote.payoutType,
          Number(entry.unitValue || 0),
          Number(rewardQuote.payoutAmount || 0),
          Number(entry.claimableAmount || 0),
          createdAt.toISOString(),
        ]
      )
    )
  );
  return claimAttemptId;
}

async function markPassiveClaimAttemptTransferSucceeded(claimAttemptId) {
  await claimsPool.query(
    `UPDATE claim_attempt_nfts
     SET status = 'transfer_succeeded',
         transfer_succeeded_at = NOW()
     WHERE claim_attempt_id = $1 AND status = 'pending'`,
    [claimAttemptId]
  );
}

async function markPassiveClaimAttemptRecorded(claimAttemptId) {
  await claimsPool.query(
    `UPDATE claim_attempt_nfts
     SET status = 'recorded',
         recorded_at = NOW()
     WHERE claim_attempt_id = $1 AND status IN ('pending', 'transfer_succeeded')`,
    [claimAttemptId]
  );
}

async function markPassiveClaimAttemptFailed(claimAttemptId, status = 'failed') {
  await claimsPool.query(
    `UPDATE claim_attempt_nfts
     SET status = $2
     WHERE claim_attempt_id = $1 AND status = 'pending'`,
    [claimAttemptId, status]
  );
}

async function getPassiveClaimQuote(guildId, links, settings) {
  const payoutType = settings?.payout_type === 'per_nft' ? 'per_nft' : 'per_up';
  const payoutAmount = Number(settings?.payout_amount || 0);
  const linkMap = new Map();
  for (const link of (links || [])) {
    const walletAddress = normalizeEthAddress(link?.wallet_address);
    if (!walletAddress) continue;
    const existing = linkMap.get(walletAddress);
    linkMap.set(walletAddress, {
      wallet_address: walletAddress,
      verified: existing ? (existing.verified || Boolean(link?.verified)) : Boolean(link?.verified),
    });
  }
  const normalizedLinks = [...linkMap.values()];
  const rules = await getHolderRules(guildId);
  const contractMap = new Map();
  for (const r of rules) {
    const key = collectionKey(r.chain, r.contract_address);
    if (!key) continue;
    const existing = contractMap.get(key);
    const rewardStartAt = validRewardBaselineDate(r.created_at) || null;
    if (existing) {
      if (
        rewardStartAt &&
        (!existing.rewardStartAt || rewardStartAt.getTime() < existing.rewardStartAt.getTime())
      ) {
        existing.rewardStartAt = rewardStartAt;
      }
      continue;
    }
    contractMap.set(key, {
      chain: normalizeNftChain(r.chain) || DEFAULT_NFT_CHAIN,
      contractAddress: String(r.contract_address || '').toLowerCase(),
      rewardStartAt,
    });
  }
  const contracts = [...contractMap.values()];
  if (!normalizedLinks.length || !contracts.length || payoutAmount <= 0) {
    return {
      payoutType,
      payoutAmount,
      nftEntries: [],
      ownedNfts: 0,
      scoringErrors: [],
      totalNfts: 0,
      totalUnits: 0,
      baseAmount: 0,
      unverifiedPenaltyAmount: 0,
      claimableAmount: 0,
    };
  }

  const nftEntries = [];
  const seenNfts = new Set();
  await mapLimit(normalizedLinks, 3, async (link) => {
    await mapLimit(contracts, 3, async ({ chain, contractAddress, rewardStartAt }) => {
      const tokenIds = await getOwnedTokenIdsForContract(link.wallet_address, contractAddress, chain);
      for (const tokenId of tokenIds) {
        const key = nftClaimKey(chain, contractAddress, tokenId);
        if (seenNfts.has(key)) continue;
        seenNfts.add(key);
        nftEntries.push({
          chain,
          contractAddress,
          tokenId: String(tokenId),
          walletAddress: link.wallet_address,
          verified: Boolean(link.verified),
          rewardStartAt: passiveRewardStartDate(rewardStartAt),
          unitValue: payoutType === 'per_nft' ? 1 : 0,
        });
      }
    });
  });

  if (!nftEntries.length) {
    return {
      payoutType,
      payoutAmount,
      nftEntries: [],
      ownedNfts: 0,
      scoringErrors: [],
      totalNfts: 0,
      totalUnits: 0,
      baseAmount: 0,
      unverifiedPenaltyAmount: 0,
      claimableAmount: 0,
    };
  }

  const scoringErrors = [];
  if (payoutType === 'per_up') {
    const guildPointMappings = await getGuildPointMappings(guildId);
    await mapLimit(nftEntries, 5, async (entry) => {
      const table = hpTableForContract(entry.contractAddress, guildPointMappings, entry.chain);
      if (!table || !Object.keys(table).length) {
        entry.unitValue = 0;
        return;
      }
      try {
        const { attrs } = await getTraitsForTokenResilient(entry.tokenId, entry.contractAddress, entry.chain);
        const grouped = normalizeTraits(attrs);
        const { total } = computeHpFromTraits(grouped, table);
        entry.unitValue = total || 0;
      } catch (err) {
        entry.unitValue = 0;
        scoringErrors.push(providerFailureSummary(
          `${nftChainLabel(entry.chain)} ${entry.contractAddress} token ${entry.tokenId}`,
          err
        ));
      }
    });
  }

  const eligibleNftEntries = nftEntries.filter((entry) => Number(entry.unitValue || 0) > 0);
  if (!eligibleNftEntries.length) {
    return {
      payoutType,
      payoutAmount,
      nftEntries: [],
      ownedNfts: nftEntries.length,
      scoringErrors: [...new Set(scoringErrors)].slice(0, 3),
      totalNfts: 0,
      totalUnits: 0,
      baseAmount: 0,
      unverifiedPenaltyAmount: 0,
      claimableAmount: 0,
    };
  }

  const stateByNft = await getStoredNftClaimStates(guildId, eligibleNftEntries);
  const now = new Date();

  for (const entry of eligibleNftEntries) {
    const state = stateByNft.get(nftClaimKey(entry.chain, entry.contractAddress, entry.tokenId));
    let lastClaimedAt = state?.last_claimed_at ? new Date(state.last_claimed_at) : null;
    if (!lastClaimedAt || !Number.isFinite(lastClaimedAt.getTime())) {
      lastClaimedAt = passiveRewardStartDate(entry.rewardStartAt, now);
    }
    const elapsedMs = Math.max(0, now.getTime() - lastClaimedAt.getTime());
    entry.elapsedMs = elapsedMs;
  }
  const totals = calculatePassiveClaimTotals(eligibleNftEntries, payoutAmount);

  return {
    payoutType,
    payoutAmount,
    nftEntries: eligibleNftEntries,
    ownedNfts: nftEntries.length,
    scoringErrors: [...new Set(scoringErrors)].slice(0, 3),
    totalNfts: eligibleNftEntries.length,
    totalUnits: totals.totalUnits,
    baseAmount: totals.baseAmount,
    unverifiedPenaltyAmount: totals.unverifiedPenaltyAmount,
    claimableAmount: totals.claimableAmount,
  };
}

async function recordPassiveClaim(guildId, discordId, rewardQuote, receiptChannelId, receiptMessageId) {
  const claimTimestamp = new Date();
  await Promise.all(
    rewardQuote.nftEntries.map((entry) =>
      claimsPool.query(
        `INSERT INTO nft_claims (
           guild_id, chain, contract_address, token_id, last_claimed_at, last_seen_owner_wallet, last_seen_discord_id, last_payout_type, last_unit_value, last_payout_amount, updated_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
         ON CONFLICT (guild_id, chain, contract_address, token_id) DO UPDATE
         SET last_claimed_at = EXCLUDED.last_claimed_at,
             last_seen_owner_wallet = EXCLUDED.last_seen_owner_wallet,
             last_seen_discord_id = EXCLUDED.last_seen_discord_id,
             last_payout_type = EXCLUDED.last_payout_type,
             last_unit_value = EXCLUDED.last_unit_value,
             last_payout_amount = EXCLUDED.last_payout_amount,
             updated_at = NOW()`,
        [
          guildId,
          normalizeNftChain(entry.chain) || DEFAULT_NFT_CHAIN,
          entry.contractAddress,
          entry.tokenId,
          claimTimestamp.toISOString(),
          entry.walletAddress,
          discordId,
          rewardQuote.payoutType,
          Number(entry.unitValue || 0),
          Number(rewardQuote.payoutAmount || 0),
        ]
      )
    )
  );

  await claimsPool.query(
    `INSERT INTO claim_events (guild_id, discord_id, amount, nft_count, payout_type, wallet_addresses, receipt_channel_id, receipt_message_id)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
    [
      guildId,
      discordId,
      rewardQuote.claimableAmount,
      rewardQuote.totalNfts,
      rewardQuote.payoutType,
      [...new Set(rewardQuote.nftEntries.map((entry) => entry.walletAddress).filter(Boolean))].join(','),
      receiptChannelId || null,
      receiptMessageId || null,
    ]
  );
}

async function getPassiveRewardStartDateForContract(guildId, chain, contractAddress) {
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  const normalizedContract = String(contractAddress || '').toLowerCase();
  const { rows } = await teamPool.query(
    `SELECT MIN(created_at) AS reward_start_at
     FROM holder_rules
     WHERE guild_id = $1
       AND COALESCE(NULLIF(chain, ''), 'ethereum') = $2
       AND contract_address = $3
       AND enabled = TRUE`,
    [guildId, normalizedChain, normalizedContract]
  );
  return passiveRewardStartDate(rows[0]?.reward_start_at || null);
}

async function getClaimableAmountForNft(guildId, contractAddress, tokenId, settings, unitValueOverride = null, chain = DEFAULT_NFT_CHAIN) {
  const payoutType = settings?.payout_type === 'per_nft' ? 'per_nft' : 'per_up';
  const payoutAmount = Number(settings?.payout_amount || 0);
  const unitValue = payoutType === 'per_nft'
    ? 1
    : Number(unitValueOverride == null ? 0 : unitValueOverride);

  if (payoutAmount <= 0 || unitValue <= 0) {
    return { claimableAmount: 0, elapsedMs: 0 };
  }

  const normalizedContract = String(contractAddress || '').toLowerCase();
  const normalizedTokenId = String(tokenId || '');
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  const { rows } = await claimsPool.query(
    `SELECT last_claimed_at
     FROM nft_claims
     WHERE guild_id = $1 AND chain = $2 AND contract_address = $3 AND token_id = $4
     LIMIT 1`,
    [guildId, normalizedChain, normalizedContract, normalizedTokenId]
  );

  const now = new Date();
  let lastClaimedAt = rows[0]?.last_claimed_at ? new Date(rows[0].last_claimed_at) : null;
  if (!lastClaimedAt || !Number.isFinite(lastClaimedAt.getTime())) {
    lastClaimedAt = await getPassiveRewardStartDateForContract(guildId, normalizedChain, normalizedContract);
  }

  const elapsedMs = Math.max(0, now.getTime() - lastClaimedAt.getTime());
  const claimableAmount = Math.max(0, Math.floor((elapsedMs / DAY_IN_MS) * unitValue * payoutAmount));
  return { claimableAmount, elapsedMs };
}

async function getConnectedCollectionCounts(guildId, walletAddresses) {
  const collections = await getHolderCollections(guildId);
  if (!collections.length) return [];
  const out = await mapLimit(collections, 3, async (collection) => {
    const ids = await getOwnedTokenIdsForContractMany(walletAddresses, collection.contract_address, collection.chain);
    return {
      name: collection.name,
      chain: collection.chain,
      contractAddress: collection.contract_address,
      count: ids.length,
    };
  });
  return out;
}

function extractDripCurrencyAmountFromPayload(payload, currencyId) {
  const targetCurrency = String(currencyId || '').trim();
  if (!targetCurrency) return null;

  const parseNumeric = (value) => {
    if (value == null) return null;
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string' && value.trim() !== '' && Number.isFinite(Number(value))) return Number(value);
    return null;
  };

  const matchCurrencyAndExtract = (entry) => {
    if (!entry || typeof entry !== 'object') return null;
    const realmPoint = entry.realmPoint && typeof entry.realmPoint === 'object' ? entry.realmPoint : null;
    const entryIds = [
      entry.id,
      entry.currencyId,
      entry.realmPointId,
      entry.pointId,
      realmPoint?.id,
      realmPoint?.currencyId,
      realmPoint?.realmPointId,
    ]
      .map((v) => String(v || '').trim())
      .filter(Boolean);
    if (entryIds.length && !entryIds.includes(targetCurrency)) return null;
    const values = [
      entry.amount,
      entry.balance,
      entry.tokens,
      entry.value,
      entry.pointBalance,
      entry.currentBalance,
      realmPoint?.balance,
    ];
    for (const value of values) {
      const parsed = parseNumeric(value);
      if (parsed != null) return parsed;
    }
    return null;
  };

  const arrays = [
    payload?.data?.balances,
    payload?.data?.pointBalances,
    payload?.data?.currencies,
    payload?.balances,
    payload?.pointBalances,
    payload?.currencies,
  ].filter(Array.isArray);
  for (const arr of arrays) {
    for (const entry of arr) {
      const matched = matchCurrencyAndExtract(entry);
      if (matched != null) return matched;
    }
  }

  const candidates = [
    payload?.data,
    payload?.data?.balance,
    payload?.data?.pointBalance,
    payload?.balance,
    payload?.pointBalance,
    payload,
  ];
  for (const item of candidates) {
    if (item == null) continue;
    const direct = parseNumeric(item);
    if (direct != null) return direct;
    if (typeof item === 'object') {
      const matched = matchCurrencyAndExtract(item);
      if (matched != null) return matched;
      const valueCandidates = [item.amount, item.balance, item.tokens, item.value, item.pointBalance, item.currentBalance];
      for (const value of valueCandidates) {
        const parsed = parseNumeric(value);
        if (parsed != null) return parsed;
      }
    }
  }

  return null;
}

async function getDripMemberCurrencyBalance(realmId, memberIds, currencyId, settings) {
  const ids = [...new Set((Array.isArray(memberIds) ? memberIds : [memberIds]).map((v) => String(v || '').trim()).filter(Boolean))];
  if (!ids.length || !realmId || !currencyId || !settings?.drip_api_key) return null;

  const baseUrls = dripRealmBaseUrls(realmId);
  const variants = [
    { suffix: '', queryKey: null },
    { suffix: '/point-balance', queryKey: 'realmPointId' },
    { suffix: '/point-balance', queryKey: 'currencyId' },
    { suffix: '/balance', queryKey: 'currencyId' },
    { suffix: '/balance', queryKey: 'realmPointId' },
  ];

  const failedAttempts = [];
  for (const memberId of ids) {
    for (const baseUrl of baseUrls) {
      for (const variant of variants) {
        const url = new URL(`${baseUrl}/members/${encodeURIComponent(memberId)}${variant.suffix}`);
        if (variant.queryKey) url.searchParams.set(variant.queryKey, String(currencyId));
        const res = await fetchWithTimeout(url.toString(), {
          timeoutMs: 15000,
          headers: buildDripHeaders(settings),
        });
        if (res.ok) {
          const payload = await res.json().catch(() => ({}));
          const amount = extractDripCurrencyAmountFromPayload(payload, currencyId);
          if (amount != null) return amount;
          failedAttempts.push(`200 ${variant.suffix || '/'} (no parseable balance)`);
          continue;
        }
        failedAttempts.push(`${res.status} ${variant.suffix || '/'}`);
        if (res.status === 404 || res.status === 400 || res.status === 422) continue;
      }
    }
  }

  await postAdminSystemLog({
    guildId: null,
    category: 'DRIP Failure',
    message:
      `Could not resolve member currency balance for holdings view.\n` +
      `Realm: \`${realmId}\`\n` +
      `Currency: \`${currencyId}\`\n` +
      `Member IDs tried: ${ids.join(', ')}\n` +
      `Attempts: ${failedAttempts.slice(0, 8).join(' | ') || 'none'}`
  });

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
      `• **Check Wallets Connected**: view all wallets currently linked.\n` +
      `• **Check DRIP Status**: confirm your Discord + wallet match in DRIP and get fix steps if not.\n` +
      `• **Refresh Verification**: recheck linked wallets and scan for newly available roles.`
    )
    .setImage('https://i.imgur.com/HxdVgDc.png')
    .setColor(0x7ADDC0);
}

function verificationButtons() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('verify_connect').setLabel('Connect Wallet').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('verify_disconnect').setLabel('Disconnect Wallet').setStyle(ButtonStyle.Danger),
    new ButtonBuilder().setCustomId('verify_wallets').setLabel('Check Wallets Connected').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('verify_drip_status').setLabel('Check DRIP Status').setStyle(ButtonStyle.Success),
    new ButtonBuilder().setCustomId('verify_refresh').setLabel('Refresh Verification').setStyle(ButtonStyle.Primary),
  );
}

function rewardsMenuEmbed(guildName, pointsLabel = 'UglyPoints') {
  return new EmbedBuilder()
    .setTitle('Holder Rewards')
    .setDescription(
      `Welcome to **${guildName}** rewards.\n\n` +
      `Use the buttons below:\n` +
      `Accrued $CHARM follows each NFT when it sells or transfers.\n` +
      `• **Claim Rewards**: collect holder rewards accrued by each NFT.\n` +
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
      `• Collections: add or remove collection name + chain + contract setup.\n` +
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
        `• Users can claim passively accrued rewards through DRIP.\n` +
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
            `Users claim rewards accrued by each NFT since that NFT's last claim.\n` +
            `Verified and pending wallets currently count at full value.\n` +
            `Pending wallets remain visible in admin reports so the team can help users finish DRIP verification.`
        },
        {
          name: 'Check Wallets / Holdings',
          value:
            `Users can view linked wallets, refresh verification for new roles, see if each wallet is verified or pending, and view holdings / total ${pointsLabel}.`
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
            `Set DRIP API key, optional client ID, realm ID, currency ID, receipt channel, points label, payout type, and payout amount.\n` +
            `The claim streak bonus field is now legacy and is no longer applied.\n` +
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
            `Each NFT accrues over time and resets its own timer when claimed.\n` +
            `If an NFT sells or transfers before it is claimed, the accrued $CHARM follows that NFT to the new owner.`
        },
        {
          name: 'Support Flow',
          value:
            `If DRIP cannot verify a wallet, the user can still get holder roles and claim rewards.\n` +
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
          name: 'Passive Claim',
          value:
            `You can claim whenever you want from the rewards menu.\n` +
            `The bot calculates accrued rewards based on each eligible NFT and/or total ${pointsLabel}, depending on server settings.\n` +
            `If an NFT sells or transfers before it is claimed, the accrued $CHARM follows that NFT to the new owner.`
        },
        {
          name: 'Verified vs Unverified Wallets',
          value:
            `If your wallet is verified through DRIP, it counts at full value.\n` +
            `If your wallet is not verified through DRIP yet, it still counts at full value while it is pending.`
        },
        {
          name: 'NFT Timers',
          value:
            `Each NFT tracks its own claim timer.\n` +
            `When you claim, only the NFTs included in that claim have their accrual reset.`
        }
      ),
    new EmbedBuilder()
      .setTitle('How To Finish Verification')
      .setColor(0x7A83BF)
      .addFields(
        {
          name: 'Verify Your Wallet In DRIP',
          value:
            `Make sure the same wallet you linked here is also connected to your DRIP profile in the correct realm.`
        },
        {
          name: 'If Verification Fails',
          value:
            `You can still keep your holder roles and claim rewards while the wallet is pending.\n` +
            `If you need help, open a support ticket with the team so they can review your wallet manually.`
        },
        {
          name: 'Useful Buttons',
          value:
            `• Connect Wallet\n` +
            `• Check DRIP Status\n` +
            `• Refresh Verification\n` +
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
      new ButtonBuilder().setCustomId('setup_remove_collection').setLabel('Remove Collection').setStyle(ButtonStyle.Danger),
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
    `https://api.drip.re/api/v1/realms/${encoded}`,
    `https://api.drip.re/api/v1/realm/${encoded}`,
  ];
}

function normalizeDripMemberId(value) {
  if (value == null) return null;
  if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const lower = trimmed.toLowerCase();
  if (lower === '[object object]' || lower === 'undefined' || lower === 'null') return null;
  return trimmed;
}

function collectUniqueDripMemberIds(values) {
  const out = [];
  const seen = new Set();
  for (const value of values) {
    const normalized = normalizeDripMemberId(value);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(normalized);
  }
  return out;
}

function logDripPayout(stage, details = {}) {
  try {
    console.log(`[DRIP] ${stage}`, JSON.stringify(details));
  } catch {
    console.log(`[DRIP] ${stage}`, details);
  }
}

function isDripRecipientRealmMismatch(status, body) {
  return (
    status === 400 &&
    /recipient\s+is\s+not\s+a\s+member\s+of\s+this\s+realm/i.test(String(body || ''))
  );
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

async function findDripAccountByCredential(type, value, settings) {
  const normalizedType = String(type || '').trim().toLowerCase();
  const typeCandidates =
    normalizedType === 'discord' || normalizedType === 'discord-id'
      ? ['discord', 'discord-id']
      : [normalizedType].filter(Boolean);
  const valueParamCandidates = ['value', 'values'];
  const errors = [];

  for (const typeCandidate of typeCandidates) {
    for (const valueParam of valueParamCandidates) {
      const url =
        `https://api.drip.re/api/v1/accounts/find` +
        `?type=${encodeURIComponent(typeCandidate)}&${valueParam}=${encodeURIComponent(value)}`;
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

async function findDripAccountByDiscordId(discordId, settings) {
  return findDripAccountByCredential('discord', discordId, settings);
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

async function resolveDripMemberForDiscordOnly(realmId, discordId, settings) {
  const byDiscord = await searchDripMembers(realmId, 'discord', discordId, settings);
  if (byDiscord[0]?.id) return { member: byDiscord[0], source: 'discord-id' };

  return { member: null, source: null };
}

function collectDripMemberIdCandidates(memberLike, fallbackId = null) {
  return collectUniqueDripMemberIds([
    memberLike?.id,
    memberLike?.realmMemberId,
    memberLike?.memberId,
    memberLike?.dripMemberId,
    fallbackId,
  ]);
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

async function seedDripMemberForVerification(realmId, discordId, walletAddress, settings, options = {}) {
  const amount = Number(options.amount ?? DRIP_AUTO_VERIFY_SEED_AMOUNT);
  const currencyId = settings?.currency_id;
  if (!amount || amount <= 0) return { attempted: false, reason: 'DRIP auto-verification seed is disabled.' };
  if (!settings?.drip_api_key || !realmId || !currencyId) {
    return { attempted: false, reason: 'DRIP seed skipped because API key, realm, or currency is not configured.' };
  }

  const resolved = await resolveDripMemberForDiscordOnly(realmId, discordId, settings);
  const memberIds = collectDripMemberIdCandidates(resolved?.member || null);
  if (!memberIds.length) {
    return { attempted: false, reason: 'DRIP seed skipped because no realm member was found for this Discord account.' };
  }

  const balance = await getDripMemberCurrencyBalance(realmId, memberIds, currencyId, settings).catch(() => null);
  if (balance != null && Number(balance) >= amount) {
    return {
      attempted: false,
      memberId: memberIds[0],
      reason: `DRIP seed skipped because the member already has ${Math.floor(Number(balance) || 0)} points.`,
    };
  }

  const result = await awardDripPoints(
    realmId,
    memberIds,
    amount,
    currencyId,
    settings,
    {
      context: 'wallet_verification_seed',
      initiatorDiscordId: options.initiatorDiscordId || discordId,
      recipientDiscordId: discordId,
      recipientWalletAddress: walletAddress || null,
      recipientResolvedMember: resolved?.member || null,
      requireTransfer: true,
    }
  );

  return {
    attempted: true,
    amount,
    memberId: result?.usedMemberId || memberIds[0],
    senderId: result?.usedSenderId || null,
    result,
  };
}

function isLikelyExistingDripCredentialResponse(status, body) {
  return (
    status === 409 ||
    (
      (status === 400 || status === 422) &&
      /(already|exist|duplicate|unique|conflict)/i.test(String(body || ''))
    )
  );
}

function dripCredentialSenderConfig() {
  const allowed = new Set(['twitter-id', 'discord-id', 'wallet', 'email', 'custom']);
  const type = allowed.has(DRIP_CREDENTIAL_SENDER_TYPE) ? DRIP_CREDENTIAL_SENDER_TYPE : 'discord-id';
  const value = String(DRIP_CREDENTIAL_SENDER_VALUE || '').trim();
  const source = String(DRIP_CREDENTIAL_SENDER_SOURCE || '').trim();
  if (!value) return { ok: false, reason: 'DRIP credential sender value is not configured.' };
  if (type === 'custom' && !source) {
    return { ok: false, reason: 'DRIP credential sender source is required when sender type is custom.' };
  }
  return { ok: true, type, value, source };
}

async function postDripCredential(realmId, suffix, payload, settings, context) {
  const url = `https://api.drip.re/api/v1/realms/${encodeURIComponent(realmId)}/credentials/${suffix}`;
  const res = await fetchWithTimeout(url, {
    timeoutMs: 15000,
    headers: buildDripHeaders(settings, true),
    method: 'POST',
    body: JSON.stringify(payload),
  });
  if (res.ok) {
    const data = await res.json().catch(() => ({}));
    return { ok: true, created: true, data };
  }
  const body = await res.text().catch(() => '');
  if (isLikelyExistingDripCredentialResponse(res.status, body)) {
    return { ok: true, created: false, existing: true, status: res.status, body: String(body || '').slice(0, 300) };
  }
  throw new Error(`${context} failed: HTTP ${res.status} ${body}`);
}

async function createDripDiscordCredential(realmId, discordId, settings, options = {}) {
  const payload = {
    provider: 'discord',
    providerId: String(discordId),
    username: String(options.username || discordId).slice(0, 100),
    metadata: {
      source: 'uglybot',
      context: options.context || 'wallet_verification_bootstrap',
    },
  };
  if (options.accountId) payload.accountId = String(options.accountId);
  return postDripCredential(realmId, 'social', payload, settings, 'DRIP Discord credential creation');
}

async function createDripWalletCredential(realmId, walletAddress, settings, options = {}) {
  const payload = {
    address: walletAddress,
    chain: 'ethereum',
    walletProvider: 'uglybot',
    walletName: 'UglyBot linked wallet',
    metadata: {
      source: 'uglybot',
      discordId: String(options.discordId || ''),
      context: options.context || 'wallet_verification_bootstrap',
    },
  };
  if (options.accountId) payload.accountId = String(options.accountId);
  return postDripCredential(realmId, 'wallet', payload, settings, 'DRIP wallet credential creation');
}

function credentialTransferTarget(discordId, walletAddress) {
  if (DRIP_CREDENTIAL_BOOTSTRAP_TARGET_TYPE === 'discord-id') {
    return { type: 'discord-id', value: String(discordId), source: '' };
  }
  return { type: 'wallet', value: walletAddress, source: '' };
}

async function transferDripCredentialPoints(realmId, target, amount, currencyId, settings, options = {}) {
  const sender = dripCredentialSenderConfig();
  if (!sender.ok) throw new Error(sender.reason);
  const url = new URL(`https://api.drip.re/api/v1/realms/${encodeURIComponent(realmId)}/credentials/transfer`);
  url.searchParams.set('fromType', sender.type);
  url.searchParams.set('fromValue', sender.value);
  if (sender.type === 'custom') url.searchParams.set('fromSource', sender.source);
  url.searchParams.set('toType', target.type);
  url.searchParams.set('toValue', target.value);
  if (target.type === 'custom' && target.source) url.searchParams.set('toSource', target.source);

  const payload = { amount: Math.max(1, Math.floor(Number(amount) || 0)) };
  if (currencyId) payload.realmPointId = String(currencyId);

  logDripPayout('credential transfer request', {
    context: options.context || 'wallet_verification_bootstrap',
    realmId,
    amount: payload.amount,
    currencyId: currencyId ? String(currencyId) : null,
    fromType: sender.type,
    fromValue: sender.value,
    toType: target.type,
    toValue: target.value,
  });

  const res = await fetchWithTimeout(url.toString(), {
    timeoutMs: 15000,
    headers: buildDripHeaders(settings, true),
    method: 'PATCH',
    body: JSON.stringify(payload),
  });
  if (res.ok) {
    const data = await res.json().catch(() => ({}));
    return { ok: true, data, sender, target, amount: payload.amount };
  }
  const body = await res.text().catch(() => '');
  logDripPayout('credential transfer failure', {
    context: options.context || 'wallet_verification_bootstrap',
    realmId,
    status: res.status,
    body: String(body || '').slice(0, 500),
    fromType: sender.type,
    toType: target.type,
  });
  throw new Error(`DRIP credential transfer failed: HTTP ${res.status} ${body}`);
}

async function bootstrapDripCredentialVerification(realmId, discordId, walletAddress, settings, options = {}) {
  const amount = Number(options.amount ?? DRIP_CREDENTIAL_BOOTSTRAP_AMOUNT);
  const currencyId = settings?.currency_id;
  const normalizedWallet = normalizeEthAddress(walletAddress);
  if (!amount || amount <= 0) return { ok: false, attempted: false, reason: 'DRIP credential bootstrap is disabled.' };
  if (!settings?.drip_api_key || !realmId || !currencyId) {
    return { ok: false, attempted: false, reason: 'DRIP credential bootstrap skipped because API key, realm, or currency is not configured.' };
  }
  if (!normalizedWallet) {
    return { ok: false, attempted: false, reason: 'DRIP credential bootstrap skipped because the wallet address is invalid.' };
  }

  const [discordAccountId, walletAccountId] = await Promise.all([
    findDripAccountByCredential('discord', discordId, settings).catch(() => null),
    findDripAccountByCredential('wallet', normalizedWallet, settings).catch(() => null),
  ]);
  const accountId = discordAccountId || walletAccountId || null;
  const context = options.context || 'wallet_verification_bootstrap';

  const [discordCredential, walletCredential] = await Promise.all([
    createDripDiscordCredential(realmId, discordId, settings, {
      accountId,
      username: options.username || discordId,
      context,
    }),
    createDripWalletCredential(realmId, normalizedWallet, settings, {
      accountId,
      discordId,
      context,
    }),
  ]);

  const target = credentialTransferTarget(discordId, normalizedWallet);
  const transfer = await transferDripCredentialPoints(realmId, target, amount, currencyId, settings, { context });
  return {
    ok: true,
    attempted: true,
    amount: transfer.amount,
    accountId,
    discordCredential,
    walletCredential,
    transfer,
    reason: `DRIP credential bootstrap complete; ${transfer.amount} point${transfer.amount === 1 ? '' : 's'} sent to ${target.type}.`,
  };
}

async function verifyOrBootstrapWalletViaDrip(realmId, discordId, walletAddress, settings, options = {}) {
  const existingVerified = Boolean(options.existingVerified);
  const live = await verifyWalletViaDrip(realmId, discordId, walletAddress, settings);
  if (live.verified) return live;

  const temporaryUnavailable = /temporarily unavailable/i.test(String(live.reason || ''));
  if (temporaryUnavailable) return live;

  if (existingVerified) {
    return {
      verified: true,
      dripMemberId: live.dripMemberId || options.existingDripMemberId || null,
      reason: 'Already verified in the bot; DRIP credential bootstrap skipped for existing verified wallet.',
      bootstrapSkippedExisting: true,
    };
  }

  try {
    const bootstrap = await bootstrapDripCredentialVerification(
      realmId,
      discordId,
      walletAddress,
      settings,
      options
    );
    if (bootstrap.ok) {
      return {
        verified: true,
        dripMemberId: live.dripMemberId || options.existingDripMemberId || null,
        reason: bootstrap.reason,
        bootstrap,
      };
    }
    return {
      ...live,
      reason: `${live.reason} Bootstrap not completed: ${bootstrap.reason}`,
      bootstrap,
    };
  } catch (err) {
    return {
      ...live,
      reason: `${live.reason} Bootstrap failed: ${String(err?.message || err || '').slice(0, 220)}`,
      bootstrapError: err,
    };
  }
}

async function handleDripStatusCheck(interaction) {
  if (!interaction.deferred && !interaction.replied) {
    await interaction.deferReply({ flags: 64 });
  }
  const guildId = interaction.guild.id;
  const settings = await getGuildSettings(guildId);
  const links = await getWalletLinks(guildId, interaction.user.id);
  const walletAddresses = links.map((x) => normalizeEthAddress(x.wallet_address)).filter(Boolean);

  if (!walletAddresses.length) {
    await interaction.editReply({
      content:
        `No wallets connected yet.\n` +
        `1. Click **Connect Wallet** here first.\n` +
        `2. Then click **Check DRIP Status** again.\n` +
        `3. If you also need DRIP setup, open https://app.drip.re/user and add the same wallet to your DRIP profile.`
    });
    return;
  }

  if (!settings?.drip_api_key || !settings?.drip_realm_id) {
    await interaction.editReply({
      content:
        `DRIP is not configured for this server yet.\n` +
        `A server admin needs to set the DRIP API key and realm before wallet verification can be checked.`
    });
    return;
  }

  const checks = await mapLimit(links, 3, async (link) => {
    const walletAddress = normalizeEthAddress(link.wallet_address);
    if (!walletAddress) {
      return {
        walletAddress: String(link.wallet_address || ''),
        verified: false,
        reason: 'Wallet address is invalid or could not be normalized.',
        dripMemberId: null,
      };
    }
    if (link.verified) {
      return {
        walletAddress,
        verified: true,
        reason: 'Already verified/bootstrap-complete in this bot.',
        dripMemberId: link.drip_member_id || null,
      };
    }
    const result = await verifyWalletViaDrip(
      settings.drip_realm_id,
      interaction.user.id,
      walletAddress,
      settings
    );
    return {
      walletAddress,
      verified: Boolean(result.verified),
      reason: result.reason,
      dripMemberId: result.dripMemberId || null,
    };
  });

  const verified = checks.filter((x) => x.verified);
  const pending = checks.filter((x) => !x.verified);

  if (!pending.length) {
    const lines = verified.map((x, i) =>
      `${i + 1}. \`${x.walletAddress}\` | DRIP verified | member: \`${x.dripMemberId || 'found'}\``
    );
    await interaction.editReply({
      content: truncateDiscordContent(
        `DRIP status: verified.\n` +
        `Your linked wallet${verified.length === 1 ? ' is' : 's are'} connected to your DRIP profile in this realm.\n` +
        `${lines.join('\n')}`
      )
    });
    return;
  }

  const pendingLines = pending.map((x, i) =>
    `${i + 1}. \`${x.walletAddress}\` | ${x.reason}`
  );
  const verifiedSummary = verified.length
    ? `\nAlready verified here:\n${verified.map((x) => `• \`${x.walletAddress}\``).join('\n')}\n`
    : '\n';

  await interaction.editReply({
    content: truncateDiscordContent(
      `DRIP status needs attention.${verifiedSummary}` +
      `Wallets still not verified in DRIP:\n${pendingLines.join('\n')}\n\n` +
      `To fix this:\n` +
      `1. Open https://app.drip.re/user\n` +
      `2. Sign in with the same Discord account: <@${interaction.user.id}>\n` +
      `3. Open your profile and connect the same wallet address${pending.length === 1 ? '' : 'es'} shown above\n` +
      `4. Make sure the wallet is connected in this server's DRIP realm (\`${settings.drip_realm_id}\`)\n` +
      `5. Come back here and click **Refresh Verification**`
    )
  });
}

async function refreshLinkedWalletVerification(guild, guildId, discordId, links, settings, options = {}) {
  const context = options.context || 'wallet refresh';
  const logDripFailures = options.logDripFailures !== false;

  if (!settings?.drip_api_key || !settings?.drip_realm_id) {
    return {
      checked: false,
      checks: [],
      refreshedLinks: links,
      statusText: 'DRIP verification was not checked because DRIP is not configured for this server.',
    };
  }

  const checks = await mapLimit(links, 3, async (link) => {
    const walletAddress = normalizeEthAddress(link.wallet_address);
    if (!walletAddress) {
      return {
        walletAddress: String(link.wallet_address || ''),
        verified: false,
        dripMemberId: link.drip_member_id || null,
        reason: 'Wallet address is invalid or could not be normalized.',
        existingVerified: Boolean(link.verified),
        temporaryUnavailable: false,
      };
    }

    const result = await verifyOrBootstrapWalletViaDrip(
      settings.drip_realm_id,
      discordId,
      walletAddress,
      settings,
      {
        existingVerified: Boolean(link.verified),
        existingDripMemberId: link.drip_member_id || null,
        context,
      }
    );

    return {
      walletAddress,
      verified: Boolean(result.verified),
      dripMemberId: result.dripMemberId || link.drip_member_id || null,
      reason: result.reason,
      existingVerified: Boolean(link.verified),
      temporaryUnavailable: /temporarily unavailable/i.test(String(result.reason || '')),
    };
  });

  let primaryDripMemberId = null;
  for (const check of checks) {
    if (check.verified && check.dripMemberId && !primaryDripMemberId) {
      primaryDripMemberId = check.dripMemberId;
    }
    if (!normalizeEthAddress(check.walletAddress)) continue;

    if (check.temporaryUnavailable) {
      if (logDripFailures) {
        await postAdminSystemLog({
          guild,
          category: 'DRIP Failure',
          message:
            `User: <@${discordId}>\n` +
            `Context: ${context}\n` +
            `Wallet: \`${check.walletAddress}\`\n` +
            `Reason: ${String(check.reason || '').slice(0, 500)}`
        });
      }
      continue;
    }

    await setWalletLink(
      guildId,
      discordId,
      check.walletAddress,
      check.verified,
      check.dripMemberId
    );
  }

  let refreshedLinks = await getWalletLinks(guildId, discordId);
  if (primaryDripMemberId) {
    for (const row of refreshedLinks) {
      await setWalletLink(
        guildId,
        discordId,
        row.wallet_address,
        Boolean(row.verified),
        row.drip_member_id || primaryDripMemberId
      );
    }
    refreshedLinks = await getWalletLinks(guildId, discordId);
  }

  const verifiedCount = checks.filter((x) => x.verified).length;
  const unavailableCount = checks.filter((x) => x.temporaryUnavailable).length;
  const pending = checks.filter((x) => !x.verified && !x.temporaryUnavailable);
  const pendingLine = pending.length
    ? ` ${pending.length} wallet${pending.length === 1 ? '' : 's'} still need attention.`
    : '';
  const unavailableLine = unavailableCount
    ? ` ${unavailableCount} wallet${unavailableCount === 1 ? '' : 's'} could not be rechecked because DRIP was temporarily unavailable; their previous verified status was preserved.`
    : '';

  return {
    checked: true,
    checks,
    refreshedLinks,
    statusText:
      `DRIP rechecked ${checks.length} wallet${checks.length === 1 ? '' : 's'}; ${verifiedCount} verified.${pendingLine}${unavailableLine}`,
  };
}

async function handleVerificationRefresh(interaction) {
  const guildId = interaction.guild.id;
  await interaction.deferReply({ flags: 64 });

  const result = await refreshUserVerificationAndRoles(
    interaction.guild,
    interaction.user.id,
    'wallet refresh'
  );
  await interaction.editReply({ content: result.message });
}

async function refreshUserVerificationAndRoles(guild, discordId, context = 'wallet refresh') {
  const guildId = guild.id;
  const links = await getWalletLinks(guildId, discordId);
  if (!links.length) {
    return {
      ok: false,
      message:
        `No wallets connected yet.\n` +
        `Connect a wallet first, then refresh verification after the wallet is linked.`,
    };
  }

  const settings = await getGuildSettings(guildId);
  const verification = await refreshLinkedWalletVerification(
    guild,
    guildId,
    discordId,
    links,
    settings || {},
    { context }
  );

  const walletAddresses = verification.refreshedLinks.map((x) => x.wallet_address).filter(Boolean);
  const member = await guild.members.fetch(discordId).catch(() => null);
  const sync = member
    ? await syncHolderRoles(member, walletAddresses)
    : { changed: 0, applied: ['Skipped: member not found in this server.'], granted: [] };
  await postRoleSyncFailures(guild, discordId, sync, context);

  const unverifiedChecks = verification.checks.filter((x) => !x.verified && !x.temporaryUnavailable);
  if (unverifiedChecks.length && sync.granted?.length) {
    for (const item of unverifiedChecks) {
      await postAdminVerificationFlag(
        guild,
        discordId,
        item.walletAddress,
        item.reason,
        sync.granted
      );
    }
  }

  return {
    ok: true,
    walletCount: walletAddresses.length,
    checkedWalletCount: verification.checks.length,
    verifiedWalletCount: verification.checks.filter((x) => x.verified).length,
    pendingWalletCount: verification.checks.filter((x) => !x.verified && !x.temporaryUnavailable).length,
    unavailableWalletCount: verification.checks.filter((x) => x.temporaryUnavailable).length,
    roleChanges: Number(sync.changed || 0),
    rolesGranted: Array.isArray(sync.granted) ? sync.granted.length : 0,
    message: truncateDiscordContent(
      `Holder verification refreshed.\n` +
      `Linked wallets: ${walletAddresses.length}\n` +
      `${verification.statusText}\n` +
      `Role sync complete (${sync.changed} change${sync.changed === 1 ? '' : 's'}).\n` +
      `New roles added:\n${formatGrantedRolesForUser(sync.granted)}` +
      formatRoleSyncUserNote(sync)
    ),
  };
}

let dailyHolderRefreshInterval = null;
let dailyHolderRefreshTimeout = null;
let dailyHolderRefreshRunning = false;

async function refreshLinkedHolderOwner(owner) {
  const guildId = String(owner?.guildId || '').trim();
  const discordId = String(owner?.discordId || '').trim();
  if (!guildId || !discordId) {
    return { ok: false, skipped: true, reason: 'invalid owner row' };
  }

  const guild = await client.guilds.fetch(guildId).catch(() => null);
  if (!guild) {
    return { ok: false, skipped: true, reason: 'guild unavailable' };
  }

  const member = await guild.members.fetch(discordId).catch(() => null);
  if (!member) {
    return { ok: false, skipped: true, guildId, discordId, reason: 'member unavailable' };
  }

  const [settings, links] = await Promise.all([
    getGuildSettings(guildId),
    getWalletLinks(guildId, discordId),
  ]);
  if (!links.length) {
    return { ok: true, skipped: true, guildId, discordId, reason: 'no linked wallets' };
  }

  const verification = await refreshLinkedWalletVerification(
    guild,
    guildId,
    discordId,
    links,
    settings || {},
    {
      context: 'daily holder refresh',
      logDripFailures: false,
    }
  );
  const walletAddresses = verification.refreshedLinks.map((x) => x.wallet_address).filter(Boolean);
  const sync = await syncHolderRoles(member, walletAddresses);
  await postRoleSyncFailures(guild, discordId, sync, 'daily holder refresh');

  return {
    ok: true,
    skipped: false,
    guildId,
    discordId,
    walletCount: walletAddresses.length,
    checkedWalletCount: verification.checks.length,
    unavailableWalletCount: verification.checks.filter((x) => x.temporaryUnavailable).length,
    roleChanges: sync.changed || 0,
    rolesGranted: Array.isArray(sync.granted) ? sync.granted.length : 0,
  };
}

let roleVerifyAllRunning = false;

async function runRoleWalletVerificationRefresh(guild, role, actorDiscordId) {
  if (roleVerifyAllRunning) {
    return {
      ok: false,
      skipped: true,
      message: 'Role wallet verification skipped: another /verifyall run is still active.',
    };
  }

  roleVerifyAllRunning = true;
  const startedAt = Date.now();
  const guildId = guild.id;
  const summary = {
    roleId: role.id,
    roleName: role.name,
    owners: 0,
    matchedRole: 0,
    processed: 0,
    skipped: 0,
    failed: 0,
    walletCount: 0,
    checkedWalletCount: 0,
    verifiedWalletCount: 0,
    pendingWalletCount: 0,
    unavailableWalletCount: 0,
    roleChanges: 0,
    rolesGranted: 0,
    pendingWallets: [],
    failures: [],
  };

  try {
    const settings = await getGuildSettings(guildId);
    const missing = [];
    if (!settings?.drip_api_key) missing.push('DRIP API Key');
    if (!settings?.drip_realm_id) missing.push('DRIP Realm ID');
    if (missing.length) {
      const message = `Role wallet verification unavailable. Missing: ${missing.join(', ')}`;
      return { ok: false, skipped: false, message, summary };
    }

    const owners = (await getWalletLinkOwners()).filter((owner) => owner.guildId === guildId);
    summary.owners = owners.length;

    await mapLimit(owners, 1, async (owner) => {
      const discordId = String(owner.discordId || '').trim();
      try {
        const member = await guild.members.fetch(discordId).catch(() => null);
        if (!member) {
          summary.skipped++;
          return;
        }
        if (!member.roles.cache.has(role.id)) {
          summary.skipped++;
          return;
        }

        summary.matchedRole++;
        const links = await getWalletLinks(guildId, discordId);
        if (!links.length) {
          summary.skipped++;
          return;
        }

        const verification = await refreshLinkedWalletVerification(
          guild,
          guildId,
          discordId,
          links,
          settings || {},
          {
            context: `admin /verifyall role ${role.id}`,
            logDripFailures: false,
          }
        );
        const walletAddresses = verification.refreshedLinks.map((x) => x.wallet_address).filter(Boolean);
        const sync = await syncHolderRoles(member, walletAddresses);
        await postRoleSyncFailures(guild, discordId, sync, 'admin /verifyall');

        summary.processed++;
        summary.walletCount += walletAddresses.length;
        summary.checkedWalletCount += verification.checks.length;
        summary.verifiedWalletCount += verification.checks.filter((x) => x.verified).length;
        summary.pendingWalletCount += verification.checks.filter((x) => !x.verified && !x.temporaryUnavailable).length;
        summary.unavailableWalletCount += verification.checks.filter((x) => x.temporaryUnavailable).length;
        for (const check of verification.checks.filter((x) => !x.verified && !x.temporaryUnavailable)) {
          summary.pendingWallets.push({
            discordId,
            walletAddress: check.walletAddress,
            dripMemberId: check.dripMemberId || '',
            reason: check.reason || '',
          });
        }
        summary.roleChanges += Number(sync.changed || 0);
        summary.rolesGranted += Array.isArray(sync.granted) ? sync.granted.length : 0;
      } catch (err) {
        summary.failed++;
        if (summary.failures.length < 10) {
          summary.failures.push(`<@${discordId}>: ${String(err?.message || err || '').slice(0, 180)}`);
        }
      }
    });

    const elapsedSeconds = Math.round((Date.now() - startedAt) / 1000);
    const message =
      `Role wallet verification complete.\n` +
      `Role: <@&${role.id}>\n` +
      `Linked wallet owners found: ${summary.owners}\n` +
      `Owners with role: ${summary.matchedRole}\n` +
      `Processed: ${summary.processed}\n` +
      `Skipped: ${summary.skipped}\n` +
      `Failed: ${summary.failed}\n` +
      `Linked wallets scanned: ${summary.walletCount}\n` +
      `DRIP checks attempted: ${summary.checkedWalletCount}\n` +
      `DRIP verified wallets: ${summary.verifiedWalletCount}\n` +
      `DRIP pending wallets: ${summary.pendingWalletCount}\n` +
      `DRIP temporarily unavailable: ${summary.unavailableWalletCount}\n` +
      `Role changes: ${summary.roleChanges}\n` +
      `Roles granted: ${summary.rolesGranted}\n` +
      `Elapsed: ${elapsedSeconds}s` +
      `${summary.failures.length ? `\nFailures:\n${summary.failures.map((line) => `- ${line}`).join('\n')}` : ''}`;

    await postAdminSystemLog({
      guild,
      category: 'Admin Verify All',
      message:
        `Actor: <@${actorDiscordId}>\n` +
        message +
        `${summary.pendingWallets.length ? `\nPending wallet report attached: ${summary.pendingWallets.length} row${summary.pendingWallets.length === 1 ? '' : 's'}.` : ''}`,
      files: [pendingWalletReportAttachment(summary)].filter(Boolean),
    });
    return { ok: true, skipped: false, message, summary };
  } catch (err) {
    const message = `Role wallet verification failed: ${String(err?.message || err || '').slice(0, 500)}`;
    await postAdminSystemLog({
      guild,
      category: 'Admin Verify All',
      message:
        `Actor: <@${actorDiscordId}>\n` +
        `Role: <@&${role.id}>\n` +
        message,
    });
    return { ok: false, skipped: false, message, summary, error: err };
  } finally {
    roleVerifyAllRunning = false;
  }
}

async function runDailyHolderVerificationRefresh() {
  if (dailyHolderRefreshRunning) {
    const message = 'Daily holder verification refresh skipped: previous run still active.';
    console.log(message);
    return { ok: false, skipped: true, message };
  }

  dailyHolderRefreshRunning = true;
  const startedAt = Date.now();
  const summary = {
    owners: 0,
    processed: 0,
    skipped: 0,
    failed: 0,
    walletCount: 0,
    checkedWalletCount: 0,
    unavailableWalletCount: 0,
    roleChanges: 0,
    rolesGranted: 0,
    failures: [],
  };

  try {
    const owners = await getWalletLinkOwners();
    summary.owners = owners.length;

    await mapLimit(owners, 1, async (owner) => {
      try {
        const result = await refreshLinkedHolderOwner(owner);
        if (result.skipped) {
          summary.skipped++;
          return;
        }
        summary.processed++;
        summary.walletCount += Number(result.walletCount || 0);
        summary.checkedWalletCount += Number(result.checkedWalletCount || 0);
        summary.unavailableWalletCount += Number(result.unavailableWalletCount || 0);
        summary.roleChanges += Number(result.roleChanges || 0);
        summary.rolesGranted += Number(result.rolesGranted || 0);
      } catch (err) {
        summary.failed++;
        if (summary.failures.length < 10) {
          summary.failures.push(
            `${owner.guildId}/${owner.discordId}: ${String(err?.message || err || '').slice(0, 180)}`
          );
        }
      }
    });

    const elapsedSeconds = Math.round((Date.now() - startedAt) / 1000);
    const message =
      `Daily holder verification refresh complete.\n` +
      `Owners found: ${summary.owners}\n` +
      `Processed: ${summary.processed}\n` +
      `Skipped: ${summary.skipped}\n` +
      `Failed: ${summary.failed}\n` +
      `Linked wallets scanned: ${summary.walletCount}\n` +
      `DRIP checks attempted: ${summary.checkedWalletCount}\n` +
      `DRIP temporarily unavailable: ${summary.unavailableWalletCount}\n` +
      `Role changes: ${summary.roleChanges}\n` +
      `Roles granted: ${summary.rolesGranted}\n` +
      `Elapsed: ${elapsedSeconds}s` +
      `${summary.failures.length ? `\nFailures:\n${summary.failures.map((line) => `- ${line}`).join('\n')}` : ''}`;

    console.log(message);
    await postAdminSystemLog({
      category: 'Daily Holder Refresh',
      message,
    });
    return { ok: true, skipped: false, message, summary };
  } catch (err) {
    const message = `Daily holder verification refresh failed: ${String(err?.message || err || '').slice(0, 500)}`;
    console.warn(message);
    await postAdminSystemLog({
      category: 'Daily Holder Refresh',
      message,
    });
    return { ok: false, skipped: false, message, summary, error: err };
  } finally {
    dailyHolderRefreshRunning = false;
  }
}

function startDailyHolderVerificationRefresh() {
  if (!DAILY_HOLDER_REFRESH_ENABLED) {
    console.log('Daily holder verification refresh disabled.');
    return;
  }
  if (dailyHolderRefreshInterval || dailyHolderRefreshTimeout) {
    return;
  }

  const run = () => runDailyHolderVerificationRefresh().catch((err) => {
    console.warn('Daily holder verification refresh error:', String(err?.message || err || ''));
  });

  dailyHolderRefreshTimeout = setTimeout(() => {
    dailyHolderRefreshTimeout = null;
    run();
  }, DAILY_HOLDER_REFRESH_START_DELAY_MS);
  dailyHolderRefreshInterval = setInterval(run, DAILY_HOLDER_REFRESH_INTERVAL_MS);

  console.log(
    `Daily holder verification refresh scheduled every ${Math.round(DAILY_HOLDER_REFRESH_INTERVAL_MS / (60 * 60 * 1000))} hour(s), ` +
    `first run in ${Math.round(DAILY_HOLDER_REFRESH_START_DELAY_MS / 60000)} minute(s).`
  );
}

async function awardDripPointsProjectFallback(realmId, memberIds, tokens, currencyId, settings) {
  const ids = Array.isArray(memberIds) ? memberIds : [memberIds];
  const memberIdCandidates = collectUniqueDripMemberIds(ids);
  if (!memberIdCandidates.length) throw new Error('DRIP award failed: no member ID candidates provided.');
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
        logDripPayout('project fallback request', {
          realmId,
          url,
          memberId,
          payload: variant.payload,
        });
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
        logDripPayout('project fallback failure', {
          realmId,
          url,
          memberId,
          payload: variant.payload,
          status: res.status,
          body: String(body || '').slice(0, 500),
        });
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

function resolveConfiguredDripSenderMemberId() {
  return normalizeDripMemberId(DRIP_SENDER_ID) || normalizeDripMemberId(DEFAULT_DRIP_MEMBER_SENDER_ID) || null;
}

async function awardDripPoints(realmId, memberIds, tokens, currencyId, settings, options = {}) {
  const amount = Number(tokens);
  const recipientCandidates = collectUniqueDripMemberIds(Array.isArray(memberIds) ? memberIds : [memberIds]);
  if (!recipientCandidates.length) {
    throw new Error('DRIP award failed: no recipient member ID candidates provided.');
  }

  const senderOverride = normalizeDripMemberId(options.senderMemberIdOverride);
  const configuredSender = resolveConfiguredDripSenderMemberId();
  const senderCandidates = options.requireTransfer && senderOverride
    ? collectUniqueDripMemberIds([senderOverride])
    : collectUniqueDripMemberIds([senderOverride, configuredSender]);
  const initiatorId = normalizeDripMemberId(options.initiatorId || DRIP_INITIATOR_ID || options.initiatorDiscordId || null);
  const context = String(options.context || 'reward');

  logDripPayout('reward start', {
    context,
    realmId,
    amount,
    currencyId: currencyId ? String(currencyId) : null,
    initiatorId,
    recipientDiscordId: options.recipientDiscordId || null,
    recipientWalletAddress: options.recipientWalletAddress || null,
    recipientMemberIdOverride: normalizeDripMemberId(options.recipientMemberIdOverride),
  });

  logDripPayout('recipient resolution', {
    context,
    recipientDiscordId: options.recipientDiscordId || null,
    recipientCandidates,
    resolvedRecipientMemberIds: collectDripMemberIdCandidates(options.recipientResolvedMember || null),
  });

  const senderConfigProblem = senderCandidates.find((id) => id === DISCORD_CLIENT_ID)
    ? `invalid DRIP sender configuration: sender member ID resolves to the Discord app ID (${DISCORD_CLIENT_ID})`
    : null;
  if (senderConfigProblem) throw new Error(senderConfigProblem);

  logDripPayout('sender resolution', {
    context,
    senderCandidates,
    configuredSender,
    senderOverride,
    initiatorId,
  });

  if (!senderCandidates.length) {
    throw new Error(
      'DRIP award failed: no sender member ID configured. Set DRIP_SENDER_ID or DRIP_TRANSFER_SENDER_ID.'
    );
  }

  const routeVariants = [
    { baseUrl: `https://api.drip.re/api/v1/realms/${encodeURIComponent(realmId)}`, quiet404: false },
    { baseUrl: `https://api.drip.re/api/v1/realm/${encodeURIComponent(realmId)}`, quiet404: true },
    { baseUrl: `https://api.drip.re/api/v4/realms/${encodeURIComponent(realmId)}`, quiet404: true },
  ];
  const payloadVariants = [];
  if (currencyId) {
    payloadVariants.push({ amount, recipientId: null, currencyId: String(currencyId) });
    payloadVariants.push({ tokens: amount, recipientId: null, realmPointId: String(currencyId) });
  }
  payloadVariants.push({ amount, recipientId: null });
  payloadVariants.push({ tokens: amount, recipientId: null });

  const transferErrors = [];
  const quiet404s = [];
  for (const senderId of senderCandidates) {
    for (const recipientId of recipientCandidates) {
      for (const route of routeVariants) {
        const url = `${route.baseUrl}/members/${encodeURIComponent(senderId)}/transfer`;
        for (const payloadTemplate of payloadVariants) {
          const payload = { ...payloadTemplate, recipientId };
          logDripPayout('transfer request', {
            context,
            url,
            senderId,
            recipientId,
            payload,
          });
          const res = await fetchWithTimeout(url, {
            timeoutMs: 15000,
            headers: buildDripHeaders(settings, true),
            method: 'PATCH',
            body: JSON.stringify(payload),
          });
          if (res.ok) {
            const data = await res.json().catch(() => ({}));
            return {
              data,
              usedMemberId: recipientId,
              usedSenderId: senderId,
              endpoint: '/transfer',
              baseUrl: route.baseUrl,
              method: 'PATCH',
            };
          }
          const body = await res.text().catch(() => '');
          if (isDripRecipientRealmMismatch(res.status, body)) {
            const message =
              `DRIP transfer failed: recipient ${recipientId} is not a member of realm ${realmId}. ` +
              `Ask the user to join/connect to this DRIP realm before wallet verification seed can be sent.`;
            logDripPayout('transfer recipient realm mismatch', {
              context,
              url,
              senderId,
              recipientId,
              status: res.status,
              body: String(body || '').slice(0, 500),
            });
            throw new Error(message);
          }
          logDripPayout('transfer failure', {
            context,
            url,
            senderId,
            recipientId,
            payload,
            status: res.status,
            body: String(body || '').slice(0, 500),
          });
          if (res.status === 404 && route.quiet404) {
            quiet404s.push(`${url}: 404 ${String(body || '').slice(0, 120)}`.trim());
            continue;
          }
          transferErrors.push(
            `${res.status} ${url} payload=${JSON.stringify(payload)} body=${String(body || '').slice(0, 220)}`
          );
          if (res.status === 400 || res.status === 403 || res.status === 404 || res.status === 422) continue;
          throw new Error(`DRIP transfer failed: HTTP ${res.status} ${body}`);
        }
      }
    }
  }

  logDripPayout('transfer fallback', {
    context,
    senderCandidates,
    recipientCandidates,
    quiet404s: quiet404s.slice(0, 6),
    transferErrors: transferErrors.slice(0, 6),
  });

  if (options.requireTransfer) {
    throw new Error(
      `DRIP transfer failed and balance-credit fallback is disabled for ${context}. ` +
      `Attempts: ${(transferErrors.length ? transferErrors : quiet404s).slice(0, 4).join(' | ') || 'none'}`
    );
  }

  const fallbackResult = await awardDripPointsProjectFallback(
    realmId,
    recipientCandidates,
    amount,
    currencyId,
    settings
  );
  return {
    ...fallbackResult,
    fallbackUsed: true,
    senderCandidates,
    recipientCandidates,
  };
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

function truncateDiscordContent(content, maxLength = 1900) {
  const text = String(content || '');
  if (text.length <= maxLength) return text;
  return `${text.slice(0, Math.max(0, maxLength - 24)).trimEnd()}\n... (truncated)`;
}

function splitDiscordContent(content, maxLength = 1900) {
  const chunks = [];
  let remaining = String(content || '');

  while (remaining.length > maxLength) {
    let splitAt = remaining.lastIndexOf('\n', maxLength);
    if (splitAt <= 0) splitAt = maxLength;

    chunks.push(remaining.slice(0, splitAt));
    remaining = remaining.slice(splitAt + (remaining[splitAt] === '\n' ? 1 : 0));
  }

  if (remaining || !chunks.length) chunks.push(remaining);
  return chunks;
}

function normalizeInteractionPayload(payload, { removeFlags = false } = {}) {
  const out = { ...(payload || {}) };
  if (Object.prototype.hasOwnProperty.call(out, 'content')) {
    out.content = truncateDiscordContent(out.content);
  }
  if (removeFlags && Object.prototype.hasOwnProperty.call(out, 'flags')) {
    delete out.flags;
  }
  return out;
}

async function respondInteraction(interaction, payload) {
  if (interaction.deferred) {
    const out = normalizeInteractionPayload(payload, { removeFlags: true });
    return interaction.editReply(out);
  }
  if (interaction.replied) return interaction.followUp(normalizeInteractionPayload(payload));
  return interaction.reply(normalizeInteractionPayload(payload));
}

function claimConfirmButtons() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('verify_claim_execute').setLabel('Claim').setStyle(ButtonStyle.Success),
    new ButtonBuilder().setCustomId('verify_claim_cancel').setLabel('No Claim').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('verify_claim_calc').setLabel('Show Calculation').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('verify_drip_status').setLabel('Check DRIP Status').setStyle(ButtonStyle.Primary),
  );
}

function claimUnavailableMessage(rewardQuote, pointsLabel = 'UglyPoints') {
  const ownedNfts = Number(rewardQuote?.ownedNfts || 0);
  const scorableNfts = Number(rewardQuote?.totalNfts || 0);
  const scoringErrors = Array.isArray(rewardQuote?.scoringErrors) ? rewardQuote.scoringErrors : [];

  if (ownedNfts <= 0) {
    return 'No eligible NFTs were found across your linked wallets.';
  }
  if (scorableNfts <= 0 && scoringErrors.length) {
    return (
      `Found **${ownedNfts} NFT${ownedNfts === 1 ? '' : 's'}** from configured earning contracts, ` +
      `but the bot could not calculate ${pointsLabel}.\n` +
      `Scoring error: ${scoringErrors[0]}`
    );
  }
  if (scorableNfts <= 0) {
    return (
      `Found **${ownedNfts} NFT${ownedNfts === 1 ? '' : 's'}** from configured earning contracts, ` +
      `but none matched a positive value in the ${pointsLabel} mappings.`
    );
  }
  return (
    `Found **${scorableNfts} claimable NFT${scorableNfts === 1 ? '' : 's'}**, but the accrued reward is currently below ` +
    `the minimum whole-point payout. Your NFTs are still accruing rewards.`
  );
}

async function handleClaimPrompt(interaction) {
  if (!interaction.deferred && !interaction.replied) {
    await interaction.deferReply({ flags: 64 });
  }
  const guildId = interaction.guild.id;
  const links = await getWalletLinks(guildId, interaction.user.id);
  const walletAddresses = links.map((x) => x.wallet_address).filter(Boolean);
  if (!walletAddresses.length) {
    await respondInteraction(interaction, {
      content:
        `Connect your wallet first.\n` +
        `Use **Connect Wallet** in Holder Verification before claiming rewards.`,
      flags: 64
    });
    return;
  }

  const settings = (await getGuildSettings(guildId)) || { payout_type: 'per_up', payout_amount: 1, claim_streak_bonus: 0 };
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

  const rewardQuote = await getPassiveClaimQuote(guildId, links, settings);
  const amount = rewardQuote.claimableAmount;
  if (amount <= 0) {
    await respondInteraction(interaction, {
      content: claimUnavailableMessage(rewardQuote, getPointsLabel(settings)),
      flags: 64
    });
    return;
  }

  const amountSummary = `You will receive: **${amount} $CHARM**`;

  await respondInteraction(interaction, {
    content:
      `You are about to claim rewards. Are you sure?\n\n` +
      `${amountSummary}`,
    components: [claimConfirmButtons()],
    flags: 64
  });
}

async function handleClaimCalculation(interaction) {
  if (!interaction.deferred && !interaction.replied) {
    await interaction.deferReply({ flags: 64 });
  }
  const guildId = interaction.guild.id;
  const links = await getWalletLinks(guildId, interaction.user.id);
  const walletAddresses = links.map((x) => x.wallet_address).filter(Boolean);
  if (!walletAddresses.length) {
    await respondInteraction(interaction, {
      content: 'Connect your wallet first to calculate claimable rewards.',
      flags: 64
    });
    return;
  }

  const settings = (await getGuildSettings(guildId)) || { payout_type: 'per_up', payout_amount: 1, claim_streak_bonus: 0 };
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

  const rewardQuote = await getPassiveClaimQuote(guildId, links, settings);
  const pointsLabel = getPointsLabel(settings);
  if (rewardQuote.claimableAmount <= 0) {
    await respondInteraction(interaction, {
      content: claimUnavailableMessage(rewardQuote, pointsLabel),
      flags: 64
    });
    return;
  }

  const grossDailyRate = rewardQuote.totalUnits * rewardQuote.payoutAmount;
  const basisLine = rewardQuote.payoutType === 'per_nft'
    ? `Rate: **${rewardQuote.totalNfts} eligible NFT${rewardQuote.totalNfts === 1 ? '' : 's'} x ${formatNumber(rewardQuote.payoutAmount)} $CHARM per NFT per day = ${formatNumber(grossDailyRate)} $CHARM/day**`
    : `Rate: **${formatNumber(rewardQuote.totalUnits)} ${pointsLabel} x ${formatNumber(rewardQuote.payoutAmount)} $CHARM per ${pointsLabel} per day = ${formatNumber(grossDailyRate)} $CHARM/day**`;
  const elapsedValues = rewardQuote.nftEntries
    .map((entry) => Number(entry.elapsedMs || 0))
    .filter((value) => Number.isFinite(value) && value >= 0);
  const minElapsed = elapsedValues.length ? Math.min(...elapsedValues) : 0;
  const maxElapsed = elapsedValues.length ? Math.max(...elapsedValues) : 0;
  const timingLine = elapsedValues.length <= 1
    ? `You last claimed about **${formatElapsedTimeSimple(maxElapsed)}** ago, and it has been collecting ever since.`
    : Math.abs(maxElapsed - minElapsed) < (60 * 1000)
      ? `You last claimed these NFTs about **${formatElapsedTimeSimple(maxElapsed)}** ago, and they have been collecting ever since.`
      : `Your NFTs were last claimed between **${formatElapsedTimeSimple(minElapsed)}** and **${formatElapsedTimeSimple(maxElapsed)}** ago, and they have been collecting ever since.`;
  const accrualLine = `Accrued so far: **${rewardQuote.baseAmount} $CHARM**`;
  const verificationLine = rewardQuote.unverifiedPenaltyAmount > 0
    ? `DRIP verification adjustment: **-${rewardQuote.unverifiedPenaltyAmount} $CHARM**`
    : `DRIP verification adjustment: **0 $CHARM**`;
  const resultLine = `Claim now: **${rewardQuote.claimableAmount} $CHARM**`;
  const simpleExplain = rewardQuote.payoutType === 'per_nft'
    ? `Simple formula: each eligible NFT earns over time until that NFT is claimed. If it sells or transfers first, its accrued $CHARM follows it to the new owner.`
    : `Simple formula: each NFT earns based on its ${pointsLabel}, over time, until that NFT is claimed. If it sells or transfers first, its accrued $CHARM follows it to the new owner.`;

  await respondInteraction(interaction, {
    content:
      `Claim calculation\n` +
      `${timingLine}\n` +
      `${basisLine}\n` +
      `${accrualLine}\n` +
      `${verificationLine}\n` +
      `${resultLine}\n\n` +
      `${simpleExplain}`,
    flags: 64
  });
}

async function handleClaim(interaction) {
  const guildId = interaction.guild.id;
  const links = await getWalletLinks(guildId, interaction.user.id);
  const walletAddresses = links.map((x) => x.wallet_address).filter(Boolean);
  if (!walletAddresses.length) {
    await respondInteraction(interaction, { content: 'Connect your wallet first.', flags: 64 });
    return;
  }

  const settings = (await getGuildSettings(guildId)) || { payout_type: 'per_up', payout_amount: 1, claim_streak_bonus: 0 };
  const pointsLabel = getPointsLabel(settings);
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
  const rewardQuote = await getPassiveClaimQuote(guildId, links, settings);
  const payoutType = rewardQuote.payoutType;
  const baseAmount = rewardQuote.baseAmount;
  const amount = rewardQuote.claimableAmount;

  if (amount <= 0) {
    await respondInteraction(interaction, {
      content: claimUnavailableMessage(rewardQuote, pointsLabel),
      flags: 64
    });
    return;
  }

  let claimAttemptId = null;
  let claimTransferSucceeded = false;
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

    claimAttemptId = await createPassiveClaimAttempt(guildId, interaction.user.id, rewardQuote);

    const awardResult = await awardDripPoints(
      settings.drip_realm_id,
      dripMemberIdCandidates,
      amount,
      settings.currency_id,
      settings,
      {
        context: 'claim',
        initiatorDiscordId: interaction.user.id,
        recipientDiscordId: interaction.user.id,
        recipientWalletAddress: walletAddresses[0] || null,
        recipientResolvedMember: resolved?.member || null,
      }
    );
    await markPassiveClaimAttemptTransferSucceeded(claimAttemptId);
    claimTransferSucceeded = true;
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
            ? `${rewardQuote.totalNfts} eligible NFT${rewardQuote.totalNfts === 1 ? '' : 's'}`
            : `${formatNumber(rewardQuote.totalUnits)} ${pointsLabel}`;
        const accrualLine = `\nClaimed NFTs: ${rewardQuote.totalNfts}`;
        const verificationPenaltyLine = rewardQuote.unverifiedPenaltyAmount > 0
          ? `\nDRIP verification adjustment: -${Math.floor(rewardQuote.unverifiedPenaltyAmount)} $CHARM`
          : '';
        receiptMessage = await receiptChannel.send(
          `🧾 Claim Receipt\n` +
          `User: <@${interaction.user.id}>\n` +
          `Earning Basis: ${earningBasis}\n` +
          `Accrued Reward: **${baseAmount} $CHARM**` +
          `${verificationPenaltyLine}` +
          `${accrualLine}\n` +
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

    await recordPassiveClaim(
      guildId,
      interaction.user.id,
      rewardQuote,
      receiptChannel?.id || null,
      receiptMessage?.id || null
    );
    await markPassiveClaimAttemptRecorded(claimAttemptId);

    const verificationLine = rewardQuote.unverifiedPenaltyAmount > 0
      ? `DRIP verification adjustment: -${Math.floor(rewardQuote.unverifiedPenaltyAmount)} $CHARM\n`
      : '';
    await respondInteraction(interaction, {
      content:
        `🧾 Claim Receipt\n` +
        `${verificationLine}` +
        `Claimed NFTs: ${rewardQuote.totalNfts}\n` +
        `Reward: **${amount} $CHARM**${receiptWarning}`,
      flags: 64
    });
  } catch (err) {
    if (claimAttemptId) {
      await markPassiveClaimAttemptFailed(claimAttemptId).catch(() => null);
    }
    console.error('Claim processing error:', err);
    const msg = String(err?.message || err || '').trim();
    if (/DRIP member search failed|DRIP award failed|DRIP transfer failed|DRIP sender/i.test(msg)) {
      await postAdminSystemLog({
        guild: interaction.guild,
        category: 'DRIP Failure',
        message:
          `User: <@${interaction.user.id}>\n` +
          `Context: claim\n` +
          `Reason: ${msg.slice(0, 500)}`
      });
    } else if (/nft_claims|claim_events|claims|wallet_links|database|relation|column/i.test(msg)) {
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
    if (claimTransferSucceeded && /nft_claims|claim_events|claims|wallet_links|database|relation|column/i.test(msg)) {
      reason = 'Your DRIP reward was sent, but the claim receipt could not be fully recorded. The NFT timers were protected from a duplicate claim and staff have been notified.';
    } else if (/DRIP member search failed/i.test(msg)) reason = 'We could not verify your DRIP profile right now.';
    else if (/DRIP award failed|DRIP transfer failed|DRIP sender/i.test(msg)) reason = 'The DRIP transfer did not complete. Please try again in a moment.';
    else if (/nft_claims|claim_events|claims|wallet_links|database|relation|column/i.test(msg)) reason = 'Your claim could not be recorded due to a storage issue.';
    await respondInteraction(interaction, {
      content: `${claimTransferSucceeded ? 'Claim partially completed' : 'Claim failed'}: ${reason}`,
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

portalEvent.initPortalEvent({
  client,
  getWalletLinks,
  getGuildSettings,
  getGuildPointMappings,
  getOwnedTokenIdsForContractMany,
  getNftMetadataAlchemy,
  getTraitsForToken,
  hpTableForContract,
  resolveDripMemberForDiscordUser,
  collectDripMemberIdCandidates,
  awardDripPoints,
});

squigDuels.initSquigDuels({
  client,
  clientUserId: client.user?.id || DISCORD_CLIENT_ID || null,
  pointsPool,
  historyPool: teamPool,
  getWalletLinks,
  getHolderCollections,
  getGuildPointMappings,
  getOwnedTokenIdsForContractMany,
  getOwnedSquigsReloadedTokenIds,
  getNftMetadataAlchemy,
  getTraitsForToken,
  normalizeTraits,
  computeHpFromTraits,
  hpTableForContract,
  getMarketplaceSpendableBalance,
  getDripMemberCurrencyBalance,
  extractDripCurrencyAmountFromPayload,
  awardDripPoints,
  postAdminSystemLog,
  isAdmin,
});

mawEvent.initMawEvent({
  client,
  clientUserId: client.user?.id || DISCORD_CLIENT_ID || null,
  mawPool: prizesPool,
  getWalletLinks,
  getOwnedTokenIdsForContractMany,
  getOwnedSquigsReloadedTokenIds,
  localSquigImagePath,
  getMarketplaceSpendableBalance,
  getDripMemberCurrencyBalance,
  extractDripCurrencyAmountFromPayload,
  awardDripPoints,
  postAdminSystemLog,
  isAdmin,
});

function getMarketplaceCommandDeps() {
  return {
    clientUserId: client.user?.id || DISCORD_CLIENT_ID || null,
    marketplacePool: prizesPool,
    getWalletLinks,
    getMarketplaceSpendableBalance,
    getDripMemberCurrencyBalance,
    extractDripCurrencyAmountFromPayload,
    awardDripPoints,
    postAdminSystemLog,
  };
}

client.on('interactionCreate', async (interaction) => {
  try {
    if (interaction.isChatInputCommand()) {
      if (await squigDuels.handleCommand(interaction)) {
        return;
      }

      if (await mawEvent.handleCommand(interaction)) {
        return;
      }

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

      if (interaction.commandName === 'dripid') {
        await interaction.deferReply({ flags: 64 });
        const result = await getUserDripIdForDisplay(interaction.guild.id, interaction.user.id);

        if (!result.dripMemberId) {
          await interaction.editReply({
            content:
              `${result.reason || 'No DRIP ID was found for your account.'}\n\n` +
              `If you expected one, connect or verify your wallet through DRIP and then try \`/dripid\` again.`
          });
          return;
        }

        const extraLine = result.additionalDripMemberIds?.length
          ? `\nAdditional stored DRIP IDs found:\n${result.additionalDripMemberIds.map((id) => `\`${id}\``).join('\n')}`
          : '';
        const sourceLine = result.source === 'stored'
          ? 'Source: stored wallet link.'
          : 'Source: DRIP lookup for your Discord account.';

        await interaction.editReply({
          content:
            `Your DRIP ID:\n` +
            `\`\`\`\n${result.dripMemberId}\n\`\`\`\n` +
            `Copy this value exactly when asked for your DRIP ID.\n` +
            `${sourceLine}${extraLine}`
        });
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

      if (interaction.commandName === 'portal') {
        const isPortalAdmin = Boolean(interaction.memberPermissions?.has(PermissionFlagsBits.Administrator));
        if (!isPortalAdmin) {
          await interaction.reply({ content: 'Administrator only.', flags: 64 });
          return;
        }

        const sub = interaction.options.getSubcommand(true);
        if (sub === 'start') {
          const state = portalEvent.getPortalState();
          if (state?.schedulerEnabled) {
            await interaction.reply({
              content:
                `Portal scheduler is already active for <#${state.portalChannelId || interaction.channel.id}>.\n` +
                `${formatPortalNextTriggerText(state)}`,
              components: [buildPortalAdminActionRow({ canTriggerNow: !state.portalActive })],
              flags: 64
            });
            return;
          }

          const result = portalEvent.startPortalScheduler({
            guildId: interaction.guild.id,
            channelId: interaction.channel.id,
          });
          await interaction.reply({
            content: `Portal scheduler started.\nChannel: <#${interaction.channel.id}>\nPortal active: ${result.portalActive ? 'yes' : 'no'}`,
            flags: 64
          });
          return;
        }

        if (sub === 'stop') {
          await interaction.deferReply({ flags: 64 });
          const result = await portalEvent.stopPortalScheduler({ closeActivePortal: true });
          await interaction.editReply({
            content: `Portal scheduler stopped.\nPortal active: ${result.portalActive ? 'yes' : 'no'}`
          });
          return;
        }

        if (sub === 'trigger') {
          await interaction.deferReply({ flags: 64 });
          try {
            const out = await portalEvent.triggerPortalEvent({
              guildId: interaction.guild.id,
              channelId: interaction.channel.id,
            });
            if (!out.ok) {
              await interaction.editReply({ content: `Portal trigger failed: ${out.reason}` });
              return;
            }
            await interaction.editReply({ content: 'Portal triggered.' });
          } catch (err) {
            console.error('Portal trigger command failed:', err);
            await interaction.editReply({ content: `Portal trigger failed: ${err.message || err}` }).catch(() => {});
          }
          return;
        }
      }

      if (interaction.commandName === 'prize') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const draft = upsertPrizeDraft(interaction.guild.id, interaction.user.id, {});
        await interaction.reply({
          embeds: [buildPrizeEditorEmbed(draft)],
          components: buildPrizeEditorRows(draft),
          flags: 64
        });
        return;
      }

      if (interaction.commandName === 'endraffle') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });
        const messageId = String(interaction.options.getString('message_id', true) || '').trim();
        if (!/^\d{16,20}$/.test(messageId)) {
          await interaction.editReply({ content: 'Invalid message ID.' });
          return;
        }

        const result = await cancelMarketplaceRaffleAndRefund(interaction.guild, interaction.user.id, messageId);
        if (!result.ok) {
          await interaction.editReply({ content: result.reason || 'Could not end raffle.' });
          return;
        }

        const failureText = result.failures.length
          ? `\nRefund failures:\n${result.failures.map((x) => `- ${x}`).join('\n').slice(0, 1200)}`
          : '';
        await interaction.editReply({
          content:
            `Raffle cancelled: **${result.item.name}**\n` +
            `Refunded purchases: **${result.refundedCount}**\n` +
            `Refunded amount: **${result.refundedAmount} $CHARM**` +
            failureText
        });
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
        const selected = findHolderCollectionByInput(collections, collectionInput);
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
          const existingTable = getPointMappingForContract(existingMappings, selected.contract_address, selected.chain) || {};
          const merged = replaceMode
            ? { table: parsed.table, addedTraits: parsed.rowCount, updatedTraits: 0, totalCategories: parsed.categoryCount }
            : mergePointsMappingTables(existingTable, parsed.table);
          await setGuildPointMapping(interaction.guild.id, selected.contract_address, merged.table, interaction.user.id, selected.chain);
          const updatedLine = replaceMode ? '' : `Traits updated: ${merged.updatedTraits}\n`;
          await interaction.editReply({
            content:
              `Points mapping ${replaceMode ? 'replaced' : 'merged'} for **${selected.name}** on **${nftChainLabel(selected.chain)}** (\`${selected.contract_address}\`).\n` +
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
        const selectedByName = findHolderCollectionByInput(collections, collectionInput);
        const parsedCollectionInput = parseChainAddressInput(collectionInput);
        const selectedMapping = parsedCollectionInput
          ? mappings.find((m) => m.chain === parsedCollectionInput.chain && String(m.contractAddress).toLowerCase() === parsedCollectionInput.contractAddress)
          : null;
        const selectedChain = selectedMapping?.chain || selectedByName?.chain || null;
        const selectedContract = selectedMapping?.contractAddress || (selectedByName ? String(selectedByName.contract_address).toLowerCase() : null);
        if (!selectedContract || !selectedChain) {
          await interaction.editReply({
            content:
              `No points mapping found for: \`${collectionInput}\`.\n` +
              `Use a mapped contract address or a collection name with an existing mapping.`
          });
          return;
        }

        const removed = await removeGuildPointMapping(interaction.guild.id, selectedContract, interaction.user.id, selectedChain);
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
          content: `Points mapping removed for **${selectedByName?.name || labelForContract(selectedContract, selectedChain)}** on **${nftChainLabel(selectedChain)}** (\`${selectedContract}\`).`
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

      if (interaction.commandName === 'verifyuser') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });

        const targetUser = interaction.options.getUser('user', true);
        const discordId = String(targetUser.id || '').trim();
        const links = await getWalletLinks(interaction.guild.id, discordId);

        if (!links.length) {
          await interaction.editReply({ content: `No linked wallets found for <@${discordId}>.` });
          return;
        }

        const existingDripMemberId = collectUniqueDripMemberIds(links.map((x) => x?.drip_member_id))[0] || null;
        let resolvedDripMemberId = existingDripMemberId;

        if (!resolvedDripMemberId) {
          try {
            const settings = await getGuildSettings(interaction.guild.id);
            const candidateWallet = normalizeEthAddress(links.find((x) => x?.wallet_address)?.wallet_address || null);
            const resolved = await resolveDripMemberForDiscordUser(
              settings?.drip_realm_id,
              discordId,
              candidateWallet,
              settings || {}
            );
            resolvedDripMemberId = collectDripMemberIdCandidates(resolved?.member || null)[0] || null;
          } catch {}
        }

        if (!resolvedDripMemberId) {
          await interaction.editReply({
            content:
              `Could not verify <@${discordId}>.\n` +
              `They need at least one linked wallet and a stored or resolvable DRIP member ID first.`
          });
          return;
        }

        const updated = await verifyUserWalletLinks(interaction.guild.id, discordId, resolvedDripMemberId);
        if (!updated) {
          await interaction.editReply({
            content:
              `No wallet links were updated for <@${discordId}>.\n` +
              `Make sure they already have a wallet connected.`
          });
          return;
        }

        await postAdminSystemLog({
          guild: interaction.guild,
          category: 'Admin Override',
          message:
            `Actor: <@${interaction.user.id}>\n` +
            `Action: /verifyuser\n` +
            `Target: <@${discordId}>\n` +
            `Wallet links updated: ${updated}\n` +
            `DRIP User ID: \`${resolvedDripMemberId}\``
        });

        await interaction.editReply({
          content:
            `Manual verification override saved for <@${discordId}>.\n` +
            `Wallet links marked DRIP verified: **${updated}**\n` +
            `DRIP User ID: \`${resolvedDripMemberId}\``
        });
        return;
      }

      if (interaction.commandName === 'refreshuser') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });

        const targetUser = interaction.options.getUser('user', true);
        const discordId = String(targetUser.id || '').trim();
        const result = await refreshUserVerificationAndRoles(
          interaction.guild,
          discordId,
          'admin /refreshuser'
        );

        await postAdminSystemLog({
          guild: interaction.guild,
          category: 'Admin Refresh User',
          message:
            `Actor: <@${interaction.user.id}>\n` +
            `Target: <@${discordId}>\n` +
            `Result: ${result.ok ? 'complete' : 'not refreshed'}\n` +
            `Wallets: ${result.walletCount ?? 0}\n` +
            `DRIP checks: ${result.checkedWalletCount ?? 0}\n` +
            `Role changes: ${result.roleChanges ?? 0}\n` +
            `Roles granted: ${result.rolesGranted ?? 0}`
        });

        await interaction.editReply({
          content: truncateDiscordContent(
            `Admin refresh for <@${discordId}>:\n` +
            result.message
          )
        });
        return;
      }

      if (interaction.commandName === 'verifyall') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });

        const selectedRole = interaction.options.getRole('role', true);
        const role = selectedRole?.id ? interaction.guild.roles.cache.get(selectedRole.id) : null;
        if (!role) {
          await interaction.editReply({ content: 'Selected role was not found in this server.' });
          return;
        }

        const result = await runRoleWalletVerificationRefresh(interaction.guild, role, interaction.user.id);
        const pendingAttachment = pendingWalletReportAttachment(result?.summary);
        await interaction.editReply({
          content: (
            String(result?.message || 'No verification summary was returned.') +
            `${pendingAttachment ? '\nPending wallet report attached.' : ''}`
          ).slice(0, 1900),
          files: pendingAttachment ? [pendingAttachment] : [],
        });
        return;
      }

      if (interaction.commandName === 'listuserwallets') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });
        const targetUser = interaction.options.getUser('user', true);
        const discordId = String(targetUser.id || '').trim();
        const links = await getWalletLinks(interaction.guild.id, discordId);
        if (!links.length) {
          await interaction.editReply({
            content:
              `No linked wallets found.\n` +
              `Discord ID: \`${discordId}\`\n` +
              `Claim status is tracked per NFT in the claims database.`
          });
          return;
        }
        const lines = links.map((w, i) =>
          `${i + 1}. \`${w.wallet_address}\` | ${w.verified ? 'verified' : 'unverified'} | DRIP: \`${w.drip_member_id || 'none'}\``
        );
        await interaction.editReply({
          content:
            `Wallet status for <@${discordId}>:\n` +
            `Claim status is tracked per NFT in the claims database.\n` +
            `${lines.join('\n')}`
        });
        return;
      }

      if (interaction.commandName === 'walletcheck') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });

        const walletAddress = normalizeEthAddress(interaction.options.getString('wallet', true));
        if (!walletAddress) {
          await interaction.editReply({ content: 'Invalid wallet address.' });
          return;
        }

        const owner = await getWalletOwnerLink(interaction.guild.id, walletAddress);
        if (!owner) {
          await interaction.editReply({
            content:
              `Wallet is not linked to anyone in this server.\n` +
              `Wallet: \`${walletAddress}\``
          });
          return;
        }

        const discordId = String(owner.discord_id || '').trim();
        const member = discordId ? await interaction.guild.members.fetch(discordId).catch(() => null) : null;
        const user = member?.user || (discordId ? await client.users.fetch(discordId).catch(() => null) : null);
        const username = user
          ? `${user.username}${user.discriminator && user.discriminator !== '0' ? `#${user.discriminator}` : ''}`
          : 'Unavailable';
        const displayName = member?.displayName || user?.globalName || username;
        const createdAt = owner.created_at ? `<t:${Math.floor(new Date(owner.created_at).getTime() / 1000)}:F>` : 'unknown';
        const updatedAt = owner.updated_at ? `<t:${Math.floor(new Date(owner.updated_at).getTime() / 1000)}:F>` : 'unknown';

        await interaction.editReply({
          content:
            `Wallet is linked in this server.\n` +
            `Wallet: \`${walletAddress}\`\n` +
            `Discord: ${discordId ? `<@${discordId}>` : 'unknown'}\n` +
            `Discord ID: \`${discordId || 'unknown'}\`\n` +
            `Username: **${username}**\n` +
            `Display name: **${displayName}**\n` +
            `Verification: **${owner.verified ? 'DRIP verified' : 'unverified/pending'}**\n` +
            `DRIP Member ID: \`${owner.drip_member_id || 'none'}\`\n` +
            `Created: ${createdAt}\n` +
            `Updated: ${updatedAt}`
        });
        return;
      }

      if (interaction.commandName === 'scanwallet') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });

        const selectedRole = interaction.options.getRole('role', false);
        const role = selectedRole?.id ? interaction.guild.roles.cache.get(selectedRole.id) : null;
        if (selectedRole && !role) {
          await interaction.editReply({ content: 'Selected role was not found in this server.' });
          return;
        }

        const rows = await getWalletScanRows(interaction.guild.id);
        const exportResult = await buildWalletScanCsv(interaction.guild, rows, role);
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const rolePart = role ? sanitizeCsvFilenamePart(role.name, role.id) : 'all';
        const filename = `wallet-scan-${sanitizeCsvFilenamePart(interaction.guild.name, interaction.guild.id)}-${rolePart}-${timestamp}.csv`;
        const attachment = new AttachmentBuilder(Buffer.from(exportResult.csv, 'utf8'), { name: filename });

        await postAdminSystemLog({
          guild: interaction.guild,
          category: 'Admin Export',
          message:
            `Actor: <@${interaction.user.id}>\n` +
            `Action: /scanwallet\n` +
            `Role filter: ${role ? `<@&${role.id}>` : 'none'}\n` +
            `Wallet rows exported: ${exportResult.rowCount}\n` +
            `Unique Discord users exported: ${exportResult.uniqueUserCount}`
        });

        const missingMemberNote = exportResult.missingMemberCount
          ? `\nMember names unavailable for ${exportResult.missingMemberCount} linked user${exportResult.missingMemberCount === 1 ? '' : 's'} the bot could not fetch.`
          : '';
        const roleSkippedNote = exportResult.roleFilterSkippedMemberCount
          ? `\nRole-filtered export excluded ${exportResult.roleFilterSkippedMemberCount} linked user${exportResult.roleFilterSkippedMemberCount === 1 ? '' : 's'} whose current member record could not be fetched.`
          : '';
        await interaction.editReply({
          content:
            `Private wallet CSV ready.\n` +
            `Rows: **${exportResult.rowCount}**\n` +
            `Unique users: **${exportResult.uniqueUserCount}**\n` +
            `Role filter: ${role ? `<@&${role.id}>` : 'none'}` +
            missingMemberNote +
            roleSkippedNote,
          files: [attachment]
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
        const rewardHealth = await getRewardHealthSummary(interaction.guild.id, settings || {});
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
        const payoutReady = rewardHealth.payoutAmount > 0 && rewardHealth.earningContracts.length > 0;
        const earningLines = rewardHealth.earningContracts.slice(0, 10).map((c) =>
          `  - ${c.label} (${nftChainLabel(c.chain)}): ${c.hasScoringTable ? 'scoring ready' : 'no points mapping'}`
        );
        await interaction.editReply({
          content:
            `Health Check\n` +
            `- DRIP configured: ${dripReady ? 'yes' : 'no'}\n` +
            `- Payout type: ${rewardHealth.payoutType}\n` +
            `- Payout amount: ${formatNumber(rewardHealth.payoutAmount)}\n` +
            `- Rewards ready: ${payoutReady ? 'yes' : 'no'}\n` +
            `- Earning contracts: ${rewardHealth.earningContracts.length}\n` +
            `- Scorable earning contracts: ${rewardHealth.scorableContracts.length}\n` +
            `- Unscored earning contracts: ${rewardHealth.unscoredContracts.length}\n` +
            `- Receipt channel reachable: ${receiptChannel?.isTextBased() ? 'yes' : 'no'} (\`${receiptChannelId}\`)\n` +
            `- Admin log channel reachable: ${adminLogChannel?.isTextBased() ? 'yes' : 'no'} (\`${ADMIN_LOG_CHANNEL_ID}\`)\n` +
            `- Bot can manage roles: ${canManageRoles ? 'yes' : 'no'}\n` +
            `- Holder rules: ${holderRules.length}\n` +
            `- Trait rules: ${traitRules.length}\n` +
            `- Missing roles: ${missingRoles.length ? missingRoles.join(', ') : 'none'}\n` +
            `- Unmanageable roles: ${blockedRoles.length ? blockedRoles.join(', ') : 'none'}\n` +
            `- Earning detail:\n${earningLines.join('\n') || '  - none'}`
        });
        return;
      }

      if (interaction.commandName === 'refresh') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });
        const result = await runDailyHolderVerificationRefresh();
        await interaction.editReply({
          content:
            `${result?.ok ? 'Refresh complete.' : 'Refresh did not complete.'}\n` +
            `${String(result?.message || 'No refresh summary was returned.').slice(0, 1800)}`
        });
        return;
      }

      if (interaction.commandName === 'paytest') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });

        const settings = await getGuildSettings(interaction.guild.id);
        const missing = [];
        if (!settings?.drip_api_key) missing.push('DRIP API Key');
        if (!settings?.drip_realm_id) missing.push('DRIP Realm ID');
        if (missing.length) {
          await interaction.editReply({ content: `Paytest unavailable. Missing: ${missing.join(', ')}` });
          return;
        }

        const targetDiscordId = String(interaction.options.getString('discord_id', true) || '').trim();
        const amount = Number(interaction.options.getInteger('amount', false) || 1);
        const recipientOverride = normalizeDripMemberId(interaction.options.getString('recipient_member_id', false));
        if (!/^\d{16,20}$/.test(targetDiscordId)) {
          await interaction.editReply({ content: 'Invalid Discord ID.' });
          return;
        }
        if (!Number.isFinite(amount) || amount < 1 || amount > 100000) {
          await interaction.editReply({ content: 'Amount must be between 1 and 100000.' });
          return;
        }

        const links = await getWalletLinks(interaction.guild.id, targetDiscordId);
        const targetWallet = links.find((x) => x.wallet_address)?.wallet_address || null;

        let resolved = null;
        let resolvedErr = null;
        try {
          resolved = await resolveDripMemberForDiscordUser(
            settings.drip_realm_id,
            targetDiscordId,
            targetWallet,
            settings
          );
        } catch (err) {
          resolvedErr = err;
        }

        const recipientCandidates = collectUniqueDripMemberIds([
          recipientOverride,
          ...collectDripMemberIdCandidates(resolved?.member || null),
          ...links.map((x) => x?.drip_member_id),
        ]);
        if (!recipientCandidates.length) {
          if (resolvedErr) throw resolvedErr;
          await interaction.editReply({ content: 'Paytest failed: no DRIP recipient member ID could be resolved.' });
          return;
        }

        const payoutResult = await awardDripPoints(
          settings.drip_realm_id,
          recipientCandidates,
          amount,
          settings.currency_id,
          settings,
          {
            context: 'paytest',
            initiatorDiscordId: interaction.user.id,
            recipientDiscordId: targetDiscordId,
            recipientWalletAddress: targetWallet,
            recipientMemberIdOverride: recipientOverride,
            recipientResolvedMember: resolved?.member || null,
          }
        );

        const usedRecipientId = payoutResult?.usedMemberId || recipientCandidates[0];
        for (const row of links) {
          await setWalletLink(interaction.guild.id, targetDiscordId, row.wallet_address, Boolean(row.verified), usedRecipientId);
        }

        await interaction.editReply({
          content:
            `Paytest sent.\n` +
            `Target: <@${targetDiscordId}>\n` +
            `Amount: **${amount}**\n` +
            `Recipient member: \`${usedRecipientId}\`\n` +
            `Sender member: \`${payoutResult?.usedSenderId || 'fallback/project-level'}\`\n` +
            `Route: \`${payoutResult?.baseUrl || 'project-level fallback'}${payoutResult?.endpoint || ''}\``
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

      if (interaction.commandName === 'allrank') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });

        let exportResult;
        try {
          exportResult = buildAllMawRanksExport();
        } catch (err) {
          await interaction.editReply({
            content: `Could not build Maw Rank export: ${String(err?.message || err || 'unknown error').slice(0, 300)}`
          });
          return;
        }

        await postAdminSystemLog({
          guild: interaction.guild,
          category: 'Admin Export',
          message:
            `Actor: <@${interaction.user.id}>\n` +
            `Action: /allrank\n` +
            `Rows exported: ${exportResult.rowCount}\n` +
            `Source hash: \`${exportResult.rankingSourceHash || 'unavailable'}\``
        }).catch(() => null);

        await interaction.editReply({
          content:
            `Maw Rank CSV ready.\n` +
            `Rows: **${exportResult.rowCount}**\n` +
            `Format: \`TOKEN ID,RANK,CLASS\``,
          files: [exportResult.attachment]
        });
        return;
      }

      if (interaction.commandName === 'rank') {
        await interaction.deferReply();
        const tokenId = interaction.options.getInteger('token_id', true);
        try {
          await interaction.editReply(buildSquigRankPayload(tokenId));
        } catch (err) {
          const code = String(err?.code || '');
          const message = code === 'MAW_RANKING_MISSING_TOKEN' || code === 'MAW_RANKING_INVALID_TOKEN'
            ? `No Maw Rank found for Squig #${tokenId}.`
            : `Maw Rank data is temporarily unavailable: ${String(err?.message || err || 'unknown error').slice(0, 220)}`;
          await interaction.editReply({ content: message });
        }
        return;
      }

      if (interaction.commandName === 'flex') {
        await interaction.deferReply();
        await replyWithRandomOwnedNft(interaction, [
          { name: 'Charm of the Ugly', contractAddress: UGLY_CONTRACT },
          { name: 'Ugly Monsters', contractAddress: MONSTER_CONTRACT },
          { name: 'Squigs Reloaded', contractAddress: SQUIGS_CONTRACT, chain: squigsChain() },
        ], '/flex');
        return;
      }

      if (interaction.commandName === 'ugly') {
        await interaction.deferReply();
        await replyWithRandomOwnedNft(interaction, [
          { name: 'Charm of the Ugly', contractAddress: UGLY_CONTRACT },
        ], '/ugly');
        return;
      }

      if (interaction.commandName === 'monster') {
        await interaction.deferReply();
        await replyWithRandomOwnedNft(interaction, [
          { name: 'Ugly Monsters', contractAddress: MONSTER_CONTRACT },
        ], '/monster');
        return;
      }

      if (interaction.commandName === 'squig') {
        await interaction.deferReply();
        await replyWithRandomOwnedNft(interaction, [
          { name: 'Squigs Reloaded', contractAddress: SQUIGS_CONTRACT, chain: squigsChain() },
        ], '/squig');
        return;
      }

      if (interaction.commandName === 'mint') {
        await interaction.reply({
          embeds: [mintEmbed()],
          files: [
            new AttachmentBuilder(MINT_TODAY_IMAGE_PATH, {
              name: MINT_TODAY_ATTACHMENT_NAME,
            }),
          ],
        });
        return;
      }

      if (interaction.commandName === 'doctor') {
        await interaction.deferReply();
        const displayName = interaction.member?.displayName || interaction.user.globalName || interaction.user.username;
        await interaction.editReply(await buildDoctorNotePayload(displayName));
        return;
      }

      if (interaction.commandName === 'marketplace') {
        await marketplaceCommand.handleMarketplaceCommand(interaction, getMarketplaceCommandDeps());
        return;
      }
      return;
    }

    if (interaction.isButton()) {
      if (await squigDuels.handleButton(interaction)) {
        return;
      }

      if (await marketplaceCommand.handleMarketplaceButton(interaction, getMarketplaceCommandDeps())) {
        return;
      }

      if (await mawEvent.handleComponent(interaction)) {
        return;
      }

      if (interaction.customId === 'portal_claim') {
        await portalEvent.handlePortalClaim(interaction);
        return;
      }

      if (interaction.customId === 'portal_admin_stop') {
        if (!interaction.memberPermissions?.has(PermissionFlagsBits.Administrator)) {
          await interaction.reply({ content: 'Administrator only.', flags: 64 });
          return;
        }
        await interaction.deferUpdate();
        const result = await portalEvent.stopPortalScheduler({ closeActivePortal: true });
        await interaction.editReply({
          content: `Portal scheduler stopped.\nPortal active: ${result.portalActive ? 'yes' : 'no'}`,
          components: []
        });
        return;
      }

      if (interaction.customId === 'portal_admin_change_time') {
        if (!interaction.memberPermissions?.has(PermissionFlagsBits.Administrator)) {
          await interaction.reply({ content: 'Administrator only.', flags: 64 });
          return;
        }
        const modal = new ModalBuilder().setCustomId('portal_admin_change_time_modal').setTitle('Change Portal Timer');
        modal.addComponents(
          new ActionRowBuilder().addComponents(
            new TextInputBuilder()
              .setCustomId('portal_minutes')
              .setLabel('Next trigger in minutes')
              .setRequired(true)
              .setPlaceholder('60')
              .setStyle(TextInputStyle.Short)
          )
        );
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === 'portal_admin_trigger_now') {
        if (!interaction.memberPermissions?.has(PermissionFlagsBits.Administrator)) {
          await interaction.reply({ content: 'Administrator only.', flags: 64 });
          return;
        }
        await interaction.deferUpdate();
        const state = portalEvent.getPortalState();
        if (state?.portalActive) {
          await interaction.editReply({
            content: 'A portal is already active right now.',
            components: [buildPortalAdminActionRow({ canTriggerNow: false })]
          });
          return;
        }

        let out;
        try {
          out = await portalEvent.triggerPortalEvent({
            guildId: interaction.guild.id,
            channelId: state?.portalChannelId || interaction.channel.id,
          });
        } catch (err) {
          console.error('Portal admin trigger button failed:', err);
          await interaction.editReply({
            content: `Portal trigger failed: ${err.message || err}`,
            components: [buildPortalAdminActionRow({ canTriggerNow: true })]
          });
          return;
        }
        if (!out.ok) {
          const refreshed = portalEvent.getPortalState();
          await interaction.editReply({
            content:
              `Portal trigger failed: ${out.reason}\n` +
              `${formatPortalNextTriggerText(refreshed)}`,
            components: [buildPortalAdminActionRow({ canTriggerNow: !refreshed.portalActive })]
          });
          return;
        }

        await interaction.editReply({
          content: 'Portal triggered now.',
          components: [buildPortalAdminActionRow({ canTriggerNow: false })]
        });
        return;
      }

      if (interaction.customId === 'prize_type_buy' || interaction.customId === 'prize_type_raffle') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const draft = upsertPrizeDraft(interaction.guild.id, interaction.user.id, {
          itemType: interaction.customId === 'prize_type_raffle' ? 'raffle' : 'buy',
        });
        await interaction.update({
          embeds: [buildPrizeEditorEmbed(draft)],
          components: buildPrizeEditorRows(draft),
        });
        return;
      }

      if (interaction.customId === 'prize_set_raffle_time') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const existing = getPrizeDraft(interaction.guild.id, interaction.user.id) || upsertPrizeDraft(interaction.guild.id, interaction.user.id, {});
        if (normalizeMarketplaceItemType(existing.itemType) !== 'raffle') {
          await interaction.reply({ content: 'Switch the prize type to raffle first.', flags: 64 });
          return;
        }
        const draft = getPrizeRaffleEndDate(existing)
          ? existing
          : upsertPrizeDraft(interaction.guild.id, interaction.user.id, { raffleEndsAtIso: getDefaultPrizeRaffleEndDate().toISOString() });
        await updatePrizeRafflePicker(interaction, draft, 'date');
        return;
      }

      if ([
        'prize_raffle_picker_date',
        'prize_raffle_picker_time',
        'prize_raffle_picker_minute',
        'prize_raffle_picker_save',
        'prize_raffle_picker_cancel',
      ].includes(interaction.customId)) {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const draft = getPrizeDraft(interaction.guild.id, interaction.user.id);
        if (!draft) {
          await interaction.reply({ content: 'No pending prize draft found. Run `/prize` again.', flags: 64 });
          return;
        }
        if (interaction.customId === 'prize_raffle_picker_cancel') {
          await updatePrizeEditorPanel(interaction, draft);
          return;
        }
        if (interaction.customId === 'prize_raffle_picker_date') {
          await updatePrizeRafflePicker(interaction, draft, 'date');
          return;
        }
        if (interaction.customId === 'prize_raffle_picker_time') {
          await updatePrizeRafflePicker(interaction, draft, 'time');
          return;
        }
        if (interaction.customId === 'prize_raffle_picker_minute') {
          await updatePrizeRafflePicker(interaction, draft, 'minute');
          return;
        }
        const endsAt = getPrizeRaffleEndDate(draft);
        if (!endsAt || endsAt.getTime() <= Date.now()) {
          await updatePrizeRafflePicker(interaction, draft, 'minute', 'Selected time must be in the future.');
          return;
        }
        await updatePrizeEditorPanel(interaction, draft);
        return;
      }

      if ([
        'prize_set_name',
        'prize_set_description',
        'prize_set_price',
        'prize_set_thumbnail',
        'prize_set_image',
        'prize_set_limit',
        'prize_set_stock',
      ].includes(interaction.customId)) {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const modalConfig = {
          prize_set_name: ['prize_set_name_modal', 'Prize Name', 'prize_value', 'Item name', TextInputStyle.Short, true, 'Golden Ticket'],
          prize_set_description: ['prize_set_description_modal', 'Prize Description', 'prize_value', 'Item description', TextInputStyle.Paragraph, true, 'What users get when they buy or enter.'],
          prize_set_price: ['prize_set_price_modal', 'Prize Price', 'prize_value', 'Price in $CHARM', TextInputStyle.Short, true, '250'],
          prize_set_thumbnail: ['prize_set_thumbnail_modal', 'Prize Thumbnail URL', 'prize_value', 'Optional thumbnail URL', TextInputStyle.Short, false, 'https://...'],
          prize_set_image: ['prize_set_image_modal', 'Prize Image URL', 'prize_value', 'Optional image URL', TextInputStyle.Short, false, 'https://...'],
          prize_set_limit: [
            'prize_set_limit_modal',
            normalizeMarketplaceItemType(getPrizeDraft(interaction.guild.id, interaction.user.id)?.itemType) === 'raffle' ? 'Tickets Available' : 'Per User Limit',
            'prize_value',
            normalizeMarketplaceItemType(getPrizeDraft(interaction.guild.id, interaction.user.id)?.itemType) === 'raffle' ? 'Total raffle tickets available' : 'Blank = unlimited',
            TextInputStyle.Short,
            normalizeMarketplaceItemType(getPrizeDraft(interaction.guild.id, interaction.user.id)?.itemType) === 'raffle',
            normalizeMarketplaceItemType(getPrizeDraft(interaction.guild.id, interaction.user.id)?.itemType) === 'raffle' ? '100' : '3'
          ],
          prize_set_stock: [
            'prize_set_stock_modal',
            normalizeMarketplaceItemType(getPrizeDraft(interaction.guild.id, interaction.user.id)?.itemType) === 'raffle' ? 'Winner Count' : 'Total Stock',
            'prize_value',
            normalizeMarketplaceItemType(getPrizeDraft(interaction.guild.id, interaction.user.id)?.itemType) === 'raffle' ? 'How many winners to draw' : 'Blank = unlimited',
            TextInputStyle.Short,
            normalizeMarketplaceItemType(getPrizeDraft(interaction.guild.id, interaction.user.id)?.itemType) === 'raffle',
            normalizeMarketplaceItemType(getPrizeDraft(interaction.guild.id, interaction.user.id)?.itemType) === 'raffle' ? '3' : '25'
          ],
        };
        const [modalId, title, inputId, label, style, required, placeholder] = modalConfig[interaction.customId];
        const modal = new ModalBuilder().setCustomId(modalId).setTitle(title);
        modal.addComponents(
          new ActionRowBuilder().addComponents(
            new TextInputBuilder()
              .setCustomId(inputId)
              .setLabel(label)
              .setStyle(style)
              .setRequired(required)
              .setPlaceholder(String(placeholder || ''))
          )
        );
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === 'prize_clear_roles') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const draft = upsertPrizeDraft(interaction.guild.id, interaction.user.id, { allowedRoleIds: [] });
        await interaction.update({
          embeds: [buildPrizeEditorEmbed(draft)],
          components: buildPrizeEditorRows(draft),
        });
        return;
      }

      if (interaction.customId === 'prize_done') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const draft = getPrizeDraft(interaction.guild.id, interaction.user.id);
        if (!draft) {
          await interaction.reply({ content: 'No pending prize draft found. Run `/prize` again.', flags: 64 });
          return;
        }
        const validation = validatePrizeDraft(draft);
        if (!validation.ok) {
          await interaction.reply({
            content: `Prize draft is missing or invalid: ${validation.problems.join(', ')}`,
            flags: 64
          });
          return;
        }
        const previewItem = {
          id: 'preview',
          item_type: draft.itemType,
          name: draft.name,
          description: draft.description,
          thumbnail_url: draft.thumbnailUrl || DEFAULT_PRIZE_THUMBNAIL_URL,
          image_url: draft.imageUrl || null,
          price: draft.price,
          per_user_limit: draft.perUserLimit || null,
          total_stock: draft.totalStock || null,
          allowed_role_ids: (draft.allowedRoleIds || []).join(','),
          raffle_ends_at: normalizeMarketplaceItemType(draft.itemType) === 'raffle'
            ? getPrizeRaffleEndsAtIso(draft)
            : null,
          status: 'published',
        };
        await interaction.update({
          content: 'Preview of the user-facing marketplace post:',
          embeds: [buildMarketplaceItemEmbed(previewItem, { totalPurchased: 0 })],
          components: buildPrizePreviewRows(draft),
        });
        return;
      }

      if (interaction.customId === 'prize_clear_channel') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const draft = upsertPrizeDraft(interaction.guild.id, interaction.user.id, { selectedChannelId: '' });
        const previewItem = {
          id: 'preview',
          item_type: draft.itemType,
          name: draft.name,
          description: draft.description,
          thumbnail_url: draft.thumbnailUrl || DEFAULT_PRIZE_THUMBNAIL_URL,
          image_url: draft.imageUrl || null,
          price: draft.price,
          per_user_limit: draft.perUserLimit || null,
          total_stock: draft.totalStock || null,
          allowed_role_ids: (draft.allowedRoleIds || []).join(','),
          raffle_ends_at: normalizeMarketplaceItemType(draft.itemType) === 'raffle'
            ? getPrizeRaffleEndsAtIso(draft)
            : null,
          status: 'published',
        };
        await interaction.update({
          content: 'Preview of the user-facing marketplace post:',
          embeds: [buildMarketplaceItemEmbed(previewItem, { totalPurchased: 0 })],
          components: buildPrizePreviewRows(draft),
        });
        return;
      }

      if (interaction.customId === 'prize_publish') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const draft = getPrizeDraft(interaction.guild.id, interaction.user.id);
        if (!draft) {
          await interaction.reply({ content: 'No pending prize draft found. Run `/prize` again.', flags: 64 });
          return;
        }
        if (!draft.selectedChannelId) {
          await interaction.reply({ content: 'Select a publish channel first.', flags: 64 });
          return;
        }
        const validation = validatePrizeDraft(draft);
        if (!validation.ok) {
          await interaction.reply({
            content: `Prize draft is missing or invalid: ${validation.problems.join(', ')}`,
            flags: 64
          });
          return;
        }
        await interaction.deferUpdate();
        const item = await createMarketplaceItemFromDraft(interaction.guild.id, interaction.user.id, draft);
        const published = await publishMarketplaceItem(item.id, draft.selectedChannelId, interaction.user.id);
        clearPrizeDraft(interaction.guild.id, interaction.user.id);
        await interaction.editReply({
          content:
            `Prize published.\n` +
            `Item ID: \`${published.id}\`\n` +
            `Channel: <#${draft.selectedChannelId}>`,
          embeds: [],
          components: [],
        });
        return;
      }

      if (interaction.customId === 'prize_edit') {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const draft = getPrizeDraft(interaction.guild.id, interaction.user.id) || upsertPrizeDraft(interaction.guild.id, interaction.user.id, {});
        await interaction.update({
          content: '',
          embeds: [buildPrizeEditorEmbed(draft)],
          components: buildPrizeEditorRows(draft),
        });
        return;
      }

      if (interaction.customId === 'prize_cancel') {
        clearPrizeDraft(interaction.guild.id, interaction.user.id);
        await interaction.update({
          content: 'Prize draft canceled.',
          embeds: [],
          components: [],
        });
        return;
      }

      const marketBuyMatch = interaction.customId.match(/^market_buy_(\d+)$/);
      if (marketBuyMatch) {
        const itemId = Number(marketBuyMatch[1]);
        const item = await getMarketplaceItemById(itemId);
        if (!item) {
          await interaction.reply({ content: 'Marketplace item not found.', flags: 64 });
          return;
        }
        const modal = new ModalBuilder().setCustomId(`market_buy_modal_${itemId}`).setTitle(
          normalizeMarketplaceItemType(item.item_type) === 'raffle' ? 'Buy Raffle Tickets' : 'Buy Item'
        );
        modal.addComponents(
          new ActionRowBuilder().addComponents(
            new TextInputBuilder()
              .setCustomId('market_quantity')
              .setLabel('Quantity')
              .setRequired(true)
              .setPlaceholder('1')
              .setStyle(TextInputStyle.Short)
          )
        );
        await interaction.showModal(modal);
        return;
      }

      const marketEntriesMatch = interaction.customId.match(/^market_entries_(\d+)$/);
      if (marketEntriesMatch) {
        const itemId = Number(marketEntriesMatch[1]);
        const item = await getMarketplaceItemById(itemId);
        if (!item) {
          await interaction.reply({ content: 'Marketplace item not found.', flags: 64 });
          return;
        }
        const entries = await getMarketplaceEntryLeaderboard(itemId);
        if (!entries.length) {
          await interaction.reply({
            content: `No entries yet for **${item.name}**.`,
            flags: 64
          });
          return;
        }
        const lines = entries
          .slice(0, 100)
          .map((entry) => `<@${entry.discordId}> - ${entry.ticketCount} ticket${entry.ticketCount === 1 ? '' : 's'}`);
        const truncated = entries.length > 100 ? `\n...and ${entries.length - 100} more` : '';
        await interaction.reply({
          content:
            `Entries for **${item.name}**\n` +
            `${lines.join('\n')}${truncated}`,
          flags: 64
        });
        return;
      }

      const marketClaimMatch = interaction.customId.match(/^market_claim_(\d+)$/);
      if (marketClaimMatch) {
        const itemId = Number(marketClaimMatch[1]);
        const item = await getMarketplaceItemById(itemId);
        if (!item) {
          await interaction.reply({ content: 'Marketplace item not found.', flags: 64 });
          return;
        }
        if (normalizeMarketplaceItemType(item.item_type) !== 'raffle' || String(item.status) !== 'completed') {
          await interaction.reply({ content: 'This raffle is not ready to be claimed yet.', flags: 64 });
          return;
        }
        const winners = await getMarketplaceRaffleWinners(itemId);
        const isWinner = winners.some((winner) => String(winner.discordId) === String(interaction.user.id));
        if (!isWinner) {
          await interaction.reply({
            content: 'You did not win this raffle and cannot claim it.',
            flags: 64
          });
          return;
        }

        const claimChannelUrl = `https://discord.com/channels/${interaction.guild.id}/${SUPPORT_TICKET_CHANNEL_ID}`;
        await interaction.reply({
          content: `You won **${item.name}**. Claim it in <#${SUPPORT_TICKET_CHANNEL_ID}>.`,
          components: [
            new ActionRowBuilder().addComponents(
              new ButtonBuilder()
                .setLabel('Open Claim Channel')
                .setStyle(ButtonStyle.Link)
                .setURL(claimChannelUrl)
            )
          ],
          flags: 64
        });
        return;
      }

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
        await handleClaimPrompt(interaction);
        return;
      }

      if (interaction.customId === 'verify_claim_execute') {
        await interaction.deferReply({ flags: 64 });
        await handleClaim(interaction);
        return;
      }

      if (interaction.customId === 'verify_claim_cancel') {
        await interaction.update({
          content: 'Claim canceled.',
          components: []
        });
        return;
      }

      if (interaction.customId === 'verify_claim_calc') {
        await handleClaimCalculation(interaction);
        return;
      }

      if (interaction.customId === 'verify_check_stats') {
        await interaction.deferReply({ flags: 64 });
        const collections = await getHolderCollections(interaction.guild.id);
        if (!collections.length) {
          await interaction.editReply({ content: 'No collections configured yet.' });
          return;
        }
        const options = collections.slice(0, 25).map((c) => ({
          label: String(c.name).slice(0, 100),
          value: collectionSelectValue(c.chain, c.contract_address),
          description: `${nftChainLabel(c.chain)} ${c.contract_address}`.slice(0, 100),
        }));
        const row = new ActionRowBuilder().addComponents(
          new StringSelectMenuBuilder()
            .setCustomId('verify_check_stats_collection_select')
            .setPlaceholder('Select collection to check')
            .addOptions(options)
        );
        await interaction.editReply({
          content: 'Select a collection, then you will enter token ID.',
          components: [row],
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
        await interaction.deferReply({ flags: 64 });
        const links = await getWalletLinks(interaction.guild.id, interaction.user.id);
        if (!links.length) {
          await interaction.editReply({ content: 'No wallets connected yet.' });
          return;
        }
        const lines = links.map((w, i) => `${i + 1}. \`${w.wallet_address}\` - ${w.verified ? 'DRIP verified' : 'DRIP verification pending'} - https://etherscan.io/address/${w.wallet_address}`);
        await interaction.editReply({
          content: truncateDiscordContent(`Connected wallet(s):\n${lines.join('\n')}`),
        });
        return;
      }

      if (interaction.customId === 'verify_drip_status') {
        await handleDripStatusCheck(interaction);
        return;
      }

      if (interaction.customId === 'verify_refresh') {
        await handleVerificationRefresh(interaction);
        return;
      }

      if (interaction.customId === 'rewards_view_holdings') {
        await interaction.deferReply({ flags: 64 });
        const settings = await getGuildSettings(interaction.guild.id);
        const links = await getWalletLinks(interaction.guild.id, interaction.user.id);
        const walletAddresses = links.map((x) => x.wallet_address).filter(Boolean);
        if (!walletAddresses.length) {
          await interaction.editReply({ content: 'No wallets connected yet.' });
          return;
        }

        const collectionCounts = await getConnectedCollectionCounts(interaction.guild.id, walletAddresses);

        let dripBalanceText = 'Unavailable';
        try {
          const resolved = await resolveDripMemberForDiscordUser(
            settings?.drip_realm_id,
            interaction.user.id,
            walletAddresses[0],
            settings || {}
          ).catch(() => ({ member: null }));
          const memberPayloadBalance = extractDripCurrencyAmountFromPayload(
            resolved?.member || null,
            settings?.currency_id
          );
          const dripMemberIdCandidates = collectDripMemberIdCandidates(
            resolved?.member,
            links.find((x) => x.drip_member_id)?.drip_member_id || null
          );
          const dripBalance = memberPayloadBalance != null
            ? memberPayloadBalance
            : await getDripMemberCurrencyBalance(
                settings?.drip_realm_id,
                dripMemberIdCandidates,
                settings?.currency_id,
                settings || {}
              );
          if (dripBalance != null) dripBalanceText = String(Math.floor(Number(dripBalance) || 0));
        } catch {}

        const collectionLines = collectionCounts.length
          ? collectionCounts.map((x) => `• ${x.name} (${nftChainLabel(x.chain)}): **${x.count}** NFT${x.count === 1 ? '' : 's'}`).join('\n')
          : '• No connected collections found.';
        const embed = new EmbedBuilder()
          .setTitle(`${interaction.user.username}'s Holdings`)
          .setColor(0xB0DEEE)
          .setThumbnail(interaction.user.displayAvatarURL({ size: 256 }))
          .setDescription(
            `Collections connected to this server across all linked wallets:\n${collectionLines}\n\n` +
            `Use **Claim Rewards** to calculate current accrued earnings. Accrued $CHARM follows each NFT when it sells or transfers.\n` +
            `DRIP $CHARM held: **${dripBalanceText}**`
          );

        await interaction.editReply({ embeds: [embed] });
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

      if (interaction.customId === 'setup_remove_collection_cancel') {
        await interaction.update({ content: 'Collection removal canceled.', components: [] });
        return;
      }

      if (interaction.customId.startsWith('setup_remove_collection_confirm:')) {
        const selectedRef = parseChainAddressInput(
          interaction.customId.slice('setup_remove_collection_confirm:'.length)
        );
        if (!selectedRef) {
          await interaction.update({ content: 'Invalid collection selection.', components: [] });
          return;
        }
        const collections = await getHolderCollections(interaction.guild.id);
        const collection = collections.find(
          (c) => collectionKey(c.chain, c.contract_address) === collectionKey(selectedRef.chain, selectedRef.contractAddress)
        );
        if (!collection) {
          await interaction.update({ content: 'Collection not found or already removed.', components: [] });
          return;
        }
        const removed = await removeHolderCollection(
          interaction.guild.id,
          collection.name,
          selectedRef.contractAddress,
          selectedRef.chain
        );
        await interaction.update({
          content:
            `Removed collection **${collection.name}** on **${nftChainLabel(selectedRef.chain)}** (\`${selectedRef.contractAddress}\`).\n` +
            `Disabled holder rules: ${removed.holderRuleCount}\n` +
            `Disabled trait rules: ${removed.traitRuleCount}\n` +
            `Removed points mappings: ${removed.mappingCount}`,
          components: [],
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
        globalThis.__PENDING_HOLDER_RULES.set(key, { chain: DEFAULT_NFT_CHAIN, contractAddress: null, collectionName: null, minTokens: null, maxTokens: null, createdAt: Date.now() });
        const options = collections.slice(0, 25).map((c) => ({
          label: String(c.name).slice(0, 100),
          value: collectionSelectValue(c.chain, c.contract_address),
          description: `${nftChainLabel(c.chain)} ${c.contract_address}`.slice(0, 100),
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
          chain: DEFAULT_NFT_CHAIN,
          contractAddress: null,
          collectionName: null,
          traitCategory: null,
          traitValue: null,
          createdAt: Date.now()
        });
        const options = collections.slice(0, 25).map((c) => ({
          label: String(c.name).slice(0, 100),
          value: collectionSelectValue(c.chain, c.contract_address),
          description: `${nftChainLabel(c.chain)} ${c.contract_address}`.slice(0, 100),
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
            new TextInputBuilder().setCustomId('chain').setLabel('Chain: ethereum, base, or abstract').setRequired(false).setPlaceholder('ethereum').setStyle(TextInputStyle.Short)
          ),
          new ActionRowBuilder().addComponents(
            new TextInputBuilder().setCustomId('contract_address').setLabel('Contract address').setRequired(true).setStyle(TextInputStyle.Short)
          ),
        );
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === 'setup_remove_collection') {
        const collections = await getHolderCollections(interaction.guild.id);
        if (!collections.length) {
          await interaction.reply({ content: 'No collections to remove.', flags: 64 });
          return;
        }
        const options = collections.slice(0, 25).map((c) => ({
          label: String(c.name).slice(0, 100),
          value: collectionSelectValue(c.chain, c.contract_address),
          description: `${nftChainLabel(c.chain)} ${c.contract_address}`.slice(0, 100),
        }));
        const row = new ActionRowBuilder().addComponents(
          new StringSelectMenuBuilder()
            .setCustomId('setup_remove_collection_select')
            .setPlaceholder('Select a collection to remove')
            .addOptions(options)
        );
        await interaction.reply({
          content: 'Select a collection to remove:',
          components: [row],
          flags: 64,
        });
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
          value: collectionSelectValue(c.chain, c.contract_address),
          description: `${nftChainLabel(c.chain)} ${c.contract_address}`.slice(0, 100),
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
          description: `${nftChainLabel(r.chain)} ${r.contract_address}`.slice(0, 100),
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
            new TextInputBuilder().setCustomId('chain').setLabel('Chain: ethereum, base, or abstract').setRequired(false).setPlaceholder('ethereum').setStyle(TextInputStyle.Short)
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
        const nameByCollection = new Map(collections.map((c) => [collectionKey(c.chain, c.contract_address), c.name]));
        const options = mappings.slice(0, 25).map((m) => {
          const chain = normalizeNftChain(m.chain) || DEFAULT_NFT_CHAIN;
          const addr = String(m.contractAddress).toLowerCase();
          const key = collectionKey(chain, addr);
          const ownerLabel = m.createdByDiscordId ? `Owner: ${m.createdByDiscordId}` : 'Owner: legacy';
          return {
            label: `${String(nameByCollection.get(key) || labelForContract(addr, chain)).slice(0, 72)}`.slice(0, 100),
            value: collectionSelectValue(chain, addr),
            description: `${nftChainLabel(chain)} - ${ownerLabel}`.slice(0, 100),
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
          description: `${nftChainLabel(r.chain)} ${r.contract_address}`.slice(0, 100),
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
        const mappingOwners = await getGuildPointMappingsWithOwners(interaction.guild.id);
        const rewardHealth = await getRewardHealthSummary(interaction.guild.id, settings || {});
        const mappingLines = mappingOwners.map((m) => `- ${labelForContract(m.contractAddress, m.chain)} (${nftChainLabel(m.chain)}): ${m.contractAddress}`);
        const earningLines = rewardHealth.earningContracts.map((c) =>
          `- ${c.label} (${nftChainLabel(c.chain)}): ${c.contractAddress} | ${c.hasScoringTable ? 'scoring ready' : 'no points mapping'}`
        );
        const configContent =
            `Settings:\n` +
            `- DRIP API Key: ${settings?.drip_api_key ? 'set' : 'not set'}\n` +
            `- DRIP Client ID: ${settings?.drip_client_id ? 'set' : 'not set'}\n` +
            `- DRIP Realm ID: ${settings?.drip_realm_id || 'not set'}\n` +
            `- Currency ID: ${settings?.currency_id || 'not set'}\n` +
            `- Receipt Channel ID: ${settings?.receipt_channel_id || RECEIPT_CHANNEL_ID}\n` +
            `- Points Label: ${getPointsLabel(settings)}\n` +
            `- Payout Type: ${settings?.payout_type || 'per_up'}\n` +
            `- Payout Amount: ${settings?.payout_amount || 1}\n` +
            `- Claim Streak Bonus (legacy, unused): ${settings?.claim_streak_bonus || 0}\n\n` +
            `Collections (${collections.length}):\n` +
            `${collections.map(c => `- ${c.name} (${nftChainLabel(c.chain)}): ${c.contract_address}`).join('\n') || '- none'}\n` +
            `Note: a collection only earns rewards after it has an enabled holder rule.\n\n` +
            `Earning Contracts (${rewardHealth.earningContracts.length}):\n` +
            `${earningLines.join('\n') || '- none'}\n\n` +
            `Points Mappings (${mappingOwners.length}):\n` +
            `${mappingLines.join('\n') || '- none'}\n\n` +
            `Rules (${rules.length}):\n` +
            `${rules.map(r => `- ${r.role_name}: ${nftChainLabel(r.chain)} ${r.contract_address} (${r.min_tokens}-${r.max_tokens ?? '∞'})`).join('\n') || '- none'}\n\n` +
            `Trait Rules (${traitRules.length}):\n` +
            `${traitRules.map(r => `- ${r.role_name}: ${nftChainLabel(r.chain)} ${r.contract_address} (${r.trait_category || 'any'}:${r.trait_value})`).join('\n') || '- none'}`;
        const configChunks = splitDiscordContent(configContent);

        await interaction.reply({ flags: 64, content: configChunks[0] });
        for (const chunk of configChunks.slice(1)) {
          await interaction.followUp({ flags: 64, content: chunk });
        }
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
        chain: pending.chain || DEFAULT_NFT_CHAIN,
        contractAddress: pending.contractAddress,
        minTokens: pending.minTokens,
        maxTokens: pending.maxTokens
      });
      globalThis.__PENDING_HOLDER_RULES.delete(key);
      await interaction.update({
        content: `Rule added for role **${role.name}** on **${nftChainLabel(pending.chain)}** \`${pending.contractAddress}\` (${pending.minTokens}-${pending.maxTokens ?? '∞'}).`,
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
        chain: pending.chain || DEFAULT_NFT_CHAIN,
        contractAddress: pending.contractAddress,
        traitCategory: pending.traitCategory,
        traitValue: pending.traitValue
      });
      globalThis.__PENDING_TRAIT_ROLE_RULES.delete(key);
      await interaction.update({
        content: `Trait rule added for role **${role.name}** on **${nftChainLabel(pending.chain)}** \`${pending.contractAddress}\` (${pending.traitCategory || 'any'}:${pending.traitValue}).`,
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

    if (interaction.isStringSelectMenu() && [
      'prize_raffle_day_early',
      'prize_raffle_day_late',
      'prize_raffle_month',
      'prize_raffle_year',
      'prize_raffle_hour',
      'prize_raffle_minute_early',
      'prize_raffle_minute_mid',
      'prize_raffle_minute_late',
      'prize_raffle_ampm',
    ].includes(interaction.customId)) {
      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }
      const existing = getPrizeDraft(interaction.guild.id, interaction.user.id);
      if (!existing) {
        await interaction.reply({ content: 'No pending prize draft found. Run `/prize` again.', flags: 64 });
        return;
      }
      const partByCustomId = {
        prize_raffle_day_early: 'day',
        prize_raffle_day_late: 'day',
        prize_raffle_month: 'month',
        prize_raffle_year: 'year',
        prize_raffle_hour: 'hour',
        prize_raffle_minute_early: 'minute',
        prize_raffle_minute_mid: 'minute',
        prize_raffle_minute_late: 'minute',
        prize_raffle_ampm: 'ampm',
      };
      const screenByPart = {
        day: 'date',
        month: 'date',
        year: 'date',
        hour: 'time',
        ampm: 'time',
        minute: 'minute',
      };
      const part = partByCustomId[interaction.customId];
      const draft = upsertPrizeDraft(
        interaction.guild.id,
        interaction.user.id,
        buildPrizeRaffleTimePatch(existing, part, interaction.values?.[0])
      );
      await updatePrizeRafflePicker(interaction, draft, screenByPart[part]);
      return;
    }

    if (interaction.isRoleSelectMenu() && interaction.customId === 'prize_roles_select') {
      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }
      const draft = upsertPrizeDraft(interaction.guild.id, interaction.user.id, {
        allowedRoleIds: interaction.values || [],
      });
      await interaction.update({
        embeds: [buildPrizeEditorEmbed(draft)],
        components: buildPrizeEditorRows(draft),
      });
      return;
    }

    if (interaction.isChannelSelectMenu() && interaction.customId === 'prize_publish_channel_select') {
      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }
      const draft = upsertPrizeDraft(interaction.guild.id, interaction.user.id, {
        selectedChannelId: String(interaction.values?.[0] || '').trim(),
      });
      const previewItem = {
        id: 'preview',
        item_type: draft.itemType,
        name: draft.name,
        description: draft.description,
        thumbnail_url: draft.thumbnailUrl || DEFAULT_PRIZE_THUMBNAIL_URL,
        image_url: draft.imageUrl || null,
        price: draft.price,
        per_user_limit: draft.perUserLimit || null,
        total_stock: draft.totalStock || null,
        allowed_role_ids: (draft.allowedRoleIds || []).join(','),
        raffle_ends_at: normalizeMarketplaceItemType(draft.itemType) === 'raffle'
          ? getPrizeRaffleEndsAtIso(draft)
          : null,
        status: 'published',
      };
      await interaction.update({
        content: 'Preview of the user-facing marketplace post:',
        embeds: [buildMarketplaceItemEmbed(previewItem, { totalPurchased: 0 })],
        components: buildPrizePreviewRows(draft),
      });
      return;
    }

    if (interaction.isStringSelectMenu() && await mawEvent.handleComponent(interaction)) {
      return;
    }

    if (interaction.isStringSelectMenu() && interaction.customId === 'portal_select') {
      await portalEvent.handlePortalSelect(interaction);
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
      const byCollectionLines = (stats.byCollection || []).map((x) => `- ${labelForContract(x.contractAddress, x.chain)} (${nftChainLabel(x.chain)}): ${x.count} NFT${x.count === 1 ? '' : 's'}`);
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
      const selectedRef = parseChainAddressInput(interaction.values?.[0] || '');
      if (!selectedRef) {
        await interaction.update({ content: 'Invalid collection selection.', components: [] });
        return;
      }
      const { chain, contractAddress } = selectedRef;
      const collections = await getHolderCollections(interaction.guild.id);
      const selected = collections.find((c) => collectionKey(c.chain, c.contract_address) === collectionKey(chain, contractAddress));
      const key = `${interaction.guild.id}:${interaction.user.id}`;
      globalThis.__PENDING_CHECK_STATS.set(key, {
        chain,
        contractAddress,
        collectionName: selected?.name || labelForContract(contractAddress, chain),
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
      const selectedRef = parseChainAddressInput(interaction.values?.[0] || '');
      if (!selectedRef) {
        await interaction.update({ content: 'Invalid collection selection.', components: [] });
        return;
      }
      const { chain, contractAddress } = selectedRef;
      const collections = await getHolderCollections(interaction.guild.id);
      const selected = collections.find((c) => collectionKey(c.chain, c.contract_address) === collectionKey(chain, contractAddress));
      pending.chain = chain;
      pending.contractAddress = contractAddress;
      pending.collectionName = selected?.name || labelForContract(contractAddress, chain);
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
      const selectedRef = parseChainAddressInput(interaction.values?.[0] || '');
      if (!selectedRef) {
        await interaction.update({ content: 'Invalid collection selection.', components: [] });
        return;
      }
      const { chain, contractAddress } = selectedRef;
      const collections = await getHolderCollections(interaction.guild.id);
      const selected = collections.find((c) => collectionKey(c.chain, c.contract_address) === collectionKey(chain, contractAddress));
      pending.chain = chain;
      pending.contractAddress = contractAddress;
      pending.collectionName = selected?.name || labelForContract(contractAddress, chain);
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
      const selectedRef = parseChainAddressInput(interaction.values?.[0] || '');
      if (!selectedRef) {
        await interaction.update({ content: 'Invalid collection selection.', components: [] });
        return;
      }
      const { chain, contractAddress } = selectedRef;
      const key = `${interaction.guild.id}:${interaction.user.id}`;
      globalThis.__PENDING_POINTS_MAPPING.set(key, { chain, contractAddress, createdAt: Date.now() });

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

    if (interaction.isStringSelectMenu() && interaction.customId === 'setup_remove_collection_select') {
      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }
      const selectedRef = parseChainAddressInput(interaction.values?.[0] || '');
      if (!selectedRef) {
        await interaction.update({ content: 'Invalid collection selection.', components: [] });
        return;
      }
      const collections = await getHolderCollections(interaction.guild.id);
      const collection = collections.find(
        (c) => collectionKey(c.chain, c.contract_address) === collectionKey(selectedRef.chain, selectedRef.contractAddress)
      );
      if (!collection) {
        await interaction.update({ content: 'Collection not found or already removed.', components: [] });
        return;
      }
      const confirmRow = new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId(`setup_remove_collection_confirm:${collectionSelectValue(selectedRef.chain, selectedRef.contractAddress)}`)
          .setLabel('Remove Collection')
          .setStyle(ButtonStyle.Danger),
        new ButtonBuilder()
          .setCustomId('setup_remove_collection_cancel')
          .setLabel('Cancel')
          .setStyle(ButtonStyle.Secondary)
      );
      await interaction.update({
        content:
          `Remove **${collection.name}** on **${nftChainLabel(selectedRef.chain)}** (\`${selectedRef.contractAddress}\`)?\n\n` +
          `This also disables its holder and trait rules and deletes its points mapping. Claim history is retained.`,
        components: [confirmRow],
      });
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
        content: `Removed holder rule: **${removed.role_name}** on **${nftChainLabel(removed.chain)}** \`${removed.contract_address}\` (${removed.min_tokens}-${removed.max_tokens ?? '∞'}).`,
        components: []
      });
      return;
    }

    if (interaction.isStringSelectMenu() && interaction.customId === 'setup_remove_points_mapping_select') {
      if (!isAdmin(interaction)) {
        await interaction.reply({ content: 'Admin only.', flags: 64 });
        return;
      }
      const selectedRef = parseChainAddressInput(interaction.values?.[0] || '');
      if (!selectedRef) {
        await interaction.update({ content: 'Invalid mapping selection.', components: [] });
        return;
      }
      const { chain, contractAddress } = selectedRef;
      const removed = await removeGuildPointMapping(interaction.guild.id, contractAddress, interaction.user.id, chain);
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
      const selected = collections.find((c) => collectionKey(c.chain, c.contract_address) === collectionKey(chain, contractAddress));
      await interaction.update({
        content: `Removed points mapping for **${selected?.name || labelForContract(contractAddress, chain)}** on **${nftChainLabel(chain)}** (\`${contractAddress}\`).`,
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
        content: `Removed trait rule: **${removed.role_name}** on **${nftChainLabel(removed.chain)}** \`${removed.contract_address}\` (${removed.trait_category || 'any'}:${removed.trait_value}).`,
        components: []
      });
      return;
    }

    if (interaction.isStringSelectMenu()) {
      if (await squigDuels.handleSelectMenu(interaction)) {
        return;
      }

      if (await marketplaceCommand.handleMarketplaceSelectMenu(interaction, getMarketplaceCommandDeps())) {
        return;
      }
    }

    if (interaction.isUserSelectMenu()) {
      if (await squigDuels.handleSelectMenu(interaction)) {
        return;
      }
    }

    if (interaction.isModalSubmit()) {
      if (await squigDuels.handleModalSubmit(interaction)) {
        return;
      }

      if (await marketplaceCommand.handleMarketplaceModalSubmit(interaction, getMarketplaceCommandDeps())) {
        return;
      }

      if (await mawEvent.handleModalSubmit(interaction)) {
        return;
      }

      if ([
        'prize_set_name_modal',
        'prize_set_description_modal',
        'prize_set_price_modal',
        'prize_set_thumbnail_modal',
        'prize_set_image_modal',
        'prize_set_limit_modal',
        'prize_set_stock_modal',
      ].includes(interaction.customId)) {
        if (!isAdmin(interaction)) {
          await interaction.reply({ content: 'Admin only.', flags: 64 });
          return;
        }
        const value = String(interaction.fields.getTextInputValue('prize_value') || '').trim();
        const patch = {};
        if (interaction.customId === 'prize_set_name_modal') patch.name = value;
        if (interaction.customId === 'prize_set_description_modal') patch.description = value;
        if (interaction.customId === 'prize_set_price_modal') patch.price = value;
        if (interaction.customId === 'prize_set_thumbnail_modal') patch.thumbnailUrl = value || DEFAULT_PRIZE_THUMBNAIL_URL;
        if (interaction.customId === 'prize_set_image_modal') patch.imageUrl = value;
        if (interaction.customId === 'prize_set_limit_modal') patch.perUserLimit = value;
        if (interaction.customId === 'prize_set_stock_modal') patch.totalStock = value;
        const draft = upsertPrizeDraft(interaction.guild.id, interaction.user.id, patch);
        await updatePrizeEditorPanel(interaction, draft);
        return;
      }

      const marketBuyModalMatch = interaction.customId.match(/^market_buy_modal_(\d+)$/);
      if (marketBuyModalMatch) {
        const itemId = Number(marketBuyModalMatch[1]);
        const rawQuantity = String(interaction.fields.getTextInputValue('market_quantity') || '').trim();
        const quantity = Number(rawQuantity);
        if (!/^\d+$/.test(rawQuantity) || !Number.isFinite(quantity) || quantity < 1 || quantity > 1000) {
          await interaction.reply({ content: 'Enter a whole-number quantity between 1 and 1000.', flags: 64 });
          return;
        }
        await interaction.deferReply({ flags: 64 });
        const result = await purchaseMarketplaceItem(interaction.guild, interaction.user.id, itemId, quantity);
        if (!result.ok) {
          await interaction.editReply({ content: result.reason || 'Purchase failed.' });
          return;
        }
        await refreshMarketplaceItemMessage(itemId);
        await postMarketplacePurchaseLog({
          guild: interaction.guild,
          actorDiscordId: interaction.user.id,
          item: result.item,
          quantity: result.quantity,
          spentAmount: result.spentAmount,
        });
        await interaction.editReply({
          content:
            `${normalizeMarketplaceItemType(result.item.item_type) === 'raffle' ? 'Tickets purchased.' : 'Purchase complete.'}\n` +
            `Item: **${result.item.name}**\n` +
            `Quantity: **${result.quantity}**\n` +
            `Spent: **${result.spentAmount} $CHARM**`
        });
        return;
      }

      if (interaction.customId === 'portal_admin_change_time_modal') {
        if (!interaction.memberPermissions?.has(PermissionFlagsBits.Administrator)) {
          await interaction.reply({ content: 'Administrator only.', flags: 64 });
          return;
        }
        const rawMinutes = String(interaction.fields.getTextInputValue('portal_minutes') || '').trim();
        const minutes = Number(rawMinutes);
        if (!/^\d+$/.test(rawMinutes) || !Number.isFinite(minutes) || minutes < 1 || minutes > 10080) {
          await interaction.reply({ content: 'Enter a whole number of minutes between 1 and 10080.', flags: 64 });
          return;
        }

        const result = portalEvent.setNextPortalTriggerDelayMinutes(minutes);
        if (!result.ok) {
          await interaction.reply({ content: result.reason || 'Could not change the portal timer.', flags: 64 });
          return;
        }

        const refreshed = portalEvent.getPortalState();
        await interaction.reply({
          content:
            `Portal timer updated.\n` +
            `${formatPortalNextTriggerText(refreshed)}`,
          components: [buildPortalAdminActionRow({ canTriggerNow: !refreshed.portalActive })],
          flags: 64
        });
        return;
      }

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
        const existingByWallet = new Map(existing.map((x) => [String(x.wallet_address).toLowerCase(), x]));
        const added = [];
        const blockedByOtherUser = [];
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
          const existingLink = existingByWallet.get(String(addr).toLowerCase()) || null;
          await setWalletLink(
            interaction.guild.id,
            interaction.user.id,
            addr,
            Boolean(existingLink?.verified),
            existingLink?.drip_member_id || null
          );
          if (!existingSet.has(addr)) {
            added.push(addr);
            await postWalletReceipt(interaction.guild, settings, interaction.user.id, 'Connected', addr);
          }
        }
        const savedLinks = await getWalletLinks(interaction.guild.id, interaction.user.id);
        const verification = await refreshLinkedWalletVerification(
          interaction.guild,
          interaction.guild.id,
          interaction.user.id,
          savedLinks,
          settings || {},
          { context: 'wallet connect' }
        );
        const allLinks = verification.refreshedLinks;
        const allAddresses = allLinks.map((x) => x.wallet_address);
        const member = await interaction.guild.members.fetch(interaction.user.id);
        const sync = await syncHolderRoles(member, allAddresses);
        await postRoleSyncFailures(interaction.guild, interaction.user.id, sync, 'wallet connect');
        const dripStatus = `${verification.statusText}\n`;
        const blockedText = blockedByOtherUser.length
          ? `\nBlocked duplicate wallet(s): ${blockedByOtherUser.map((x) => `\`${x.walletAddress}\``).join(', ')}`
          : '';
        await interaction.editReply({
          content:
            `Wallet connect processed.\n` +
            `Added: ${added.length}\n` +
            `Total linked wallets: ${allAddresses.length}\n` +
            `${dripStatus}` +
            `${blockedText}\n` +
            `Role sync complete (${sync.changed} change${sync.changed === 1 ? '' : 's'}).\n` +
            `${sync.granted?.length ? `Roles granted: ${sync.granted.join(', ')}` : 'Roles granted: none'}` +
            formatRoleSyncUserNote(sync)
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
        let roleSyncNote = '';
        try {
          const member = await interaction.guild.members.fetch(interaction.user.id);
          const remainingLinks = await getWalletLinks(interaction.guild.id, interaction.user.id);
          const remainingAddresses = remainingLinks.map((x) => x.wallet_address);
          const sync = await syncHolderRoles(member, remainingAddresses);
          await postRoleSyncFailures(interaction.guild, interaction.user.id, sync, 'wallet disconnect');
          removedRoles = sync.changed;
          roleSyncNote = formatRoleSyncUserNote(sync);
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
            `Role sync changes: ${removedRoles}` +
            roleSyncNote
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
        const chain = normalizeNftChain(pending?.chain) || DEFAULT_NFT_CHAIN;
        const contractAddress = normalizeEthAddress(pending?.contractAddress || '') || SQUIGS_CONTRACT.toLowerCase();
        await interaction.deferReply({ flags: 64 });
        const settings = await getGuildSettings(interaction.guild.id);
        const pointsLabel = getPointsLabel(settings);
        const guildPointMappings = await getGuildPointMappings(interaction.guild.id);
        const table = hpTableForContract(contractAddress, guildPointMappings, chain);
        const { attrs } = await getTraitsForTokenResilient(tokenId, contractAddress, chain);
        const grouped = normalizeTraits(attrs);
        const hpAgg = computeHpFromTraits(grouped, table);
        const tier = hpToTierLabel(hpAgg.total || 0);
        const nftClaimState = await getClaimableAmountForNft(
          interaction.guild.id,
          contractAddress,
          tokenId,
          settings || {},
          hpAgg.total || 0,
          chain
        );
        const collectionName = String(pending?.collectionName || labelForContract(contractAddress, chain));
        const imageUrlRaw = String(
          meta?.image?.cachedUrl ||
          meta?.image?.pngUrl ||
          meta?.image?.thumbnailUrl ||
          meta?.raw?.metadata?.image ||
          ''
        ).trim();
        const imageUrl = /^https?:\/\//i.test(imageUrlRaw)
          ? imageUrlRaw
          : (isSquigsContract(contractAddress) && SQUIG_IMAGE_BASE_URL
            ? `${SQUIG_IMAGE_BASE_URL}/${tokenId}`
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
          `Chain: **${nftChainLabel(chain)}**\n` +
          `Contract: \`${contractAddress}\`\n` +
          `Total ${pointsLabel}: **${hpAgg.total || 0}**\n` +
          `Claimable $CHARM: **${nftClaimState.claimableAmount}**\n` +
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
          content: `Now select which role should be assigned for **${pending.collectionName || labelForContract(pending.contractAddress, pending.chain)}** on **${nftChainLabel(pending.chain)}** (\`${pending.contractAddress}\`) with range ${minTokens}-${maxTokens ?? '∞'}:`,
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
        const table = hpTableForContract(pending.contractAddress, guildPointMappings, pending.chain);
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
          content: `Now select which role should be assigned for **${pending.collectionName || labelForContract(pending.contractAddress, pending.chain)}** on **${nftChainLabel(pending.chain)}** (\`${pending.contractAddress}\`) when a user owns trait ${matched.category}:${matched.trait}:`,
          components: [row],
          flags: 64
        });
        return;
      }

      if (interaction.customId === 'setup_add_collection_modal') {
        const name = String(interaction.fields.getTextInputValue('collection_name') || '').trim();
        let chainInput = DEFAULT_NFT_CHAIN;
        try {
          chainInput = String(interaction.fields.getTextInputValue('chain') || DEFAULT_NFT_CHAIN).trim();
        } catch {}
        const chain = normalizeNftChain(chainInput);
        const contractAddress = normalizeEthAddress(interaction.fields.getTextInputValue('contract_address'));
        if (!name) {
          await interaction.reply({ content: 'Collection name is required.', flags: 64 });
          return;
        }
        if (!chain) {
          await interaction.reply({ content: 'Invalid chain. Use ethereum, base, or abstract.', flags: 64 });
          return;
        }
        if (!contractAddress) {
          await interaction.reply({ content: 'Invalid contract address.', flags: 64 });
          return;
        }
        await upsertHolderCollection(interaction.guild.id, name, contractAddress, chain);
        await interaction.reply({
          content:
            `Collection saved: **${name}** on **${nftChainLabel(chain)}** (\`${contractAddress}\`).\n` +
            `This collection will not earn rewards until it has an enabled holder rule. If payout type is per UglyPoints, it also needs a points mapping unless it is a built-in scored collection.`,
          flags: 64
        });
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
          const existingTable = getPointMappingForContract(existingMappings, pending.contractAddress, pending.chain) || {};
          const merged = mergePointsMappingTables(existingTable, parsed.table);
          await setGuildPointMapping(interaction.guild.id, pending.contractAddress, merged.table, interaction.user.id, pending.chain);
          globalThis.__PENDING_POINTS_MAPPING.delete(key);
          const collections = await getHolderCollections(interaction.guild.id);
          const selected = collections.find((c) => collectionKey(c.chain, c.contract_address) === collectionKey(pending.chain, pending.contractAddress));
          await interaction.editReply({
            content:
              `Points mapping merged for **${selected?.name || labelForContract(pending.contractAddress, pending.chain)}** on **${nftChainLabel(pending.chain)}** (\`${pending.contractAddress}\`).\n` +
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

client.on('messageCreate', async (message) => {
  if (!message.guild || message.author?.bot) return;

  if (await squigDuels.handleMessageCreate(message)) return;

  const content = String(message.content || '').trim();
  if (!content.startsWith('!')) return;

  const command = content.split(/\s+/)[0].toLowerCase();
  if (command === '!doctor') {
    try {
      await message.channel.sendTyping().catch(() => {});
      const displayName = message.member?.displayName || message.author.globalName || message.author.username;
      await message.reply(await buildDoctorNotePayload(displayName));
    } catch (err) {
      console.error('!doctor command error:', err);
      await message.reply('Something went wrong handling that command.').catch(() => {});
    }
    return;
  }

  if (command === '!squig') {
    try {
      await message.channel.sendTyping().catch(() => {});
      await message.reply(await buildRandomSquigReloadedResponse(message.guild.id, message.author.id, message.author.username));
    } catch (err) {
      console.error('!squig command error:', err);
      await message.reply('Something went wrong handling that command.').catch(() => {});
    }
    return;
  }

  if (command === '!grid') {
    try {
      await message.channel.sendTyping().catch(() => {});
      await message.reply(await buildSquigGridResponse(message.guild.id, message.author.id, message.author.username));
    } catch (err) {
      console.error('!grid command error:', err);
      await message.reply('Something went wrong handling that command.').catch(() => {});
    }
    return;
  }

  const collectionCommands = {
    '!flex': [
      { name: 'Charm of the Ugly', contractAddress: UGLY_CONTRACT },
      { name: 'Ugly Monsters', contractAddress: MONSTER_CONTRACT },
      { name: 'Squigs Reloaded', contractAddress: SQUIGS_CONTRACT, chain: squigsChain() },
    ],
    '!ugly': [
      { name: 'Charm of the Ugly', contractAddress: UGLY_CONTRACT },
    ],
    '!monster': [
      { name: 'Ugly Monsters', contractAddress: MONSTER_CONTRACT },
    ],
    '!og': [
      { name: 'Squigs', contractAddress: OG_SQUIGS_CONTRACT, chain: DEFAULT_NFT_CHAIN },
    ],
  };

  const collections = collectionCommands[command];
  if (!collections) return;

  try {
    const payload = await buildRandomOwnedNftResponse(
      message.guild.id,
      message.author.id,
      message.author.username,
      collections,
      command
    );
    await message.reply(payload);
  } catch (err) {
    console.error('! command error:', err);
    await message.reply('Something went wrong handling that command.').catch(() => {});
  }
});
// ===== LOGIN =====
client.login(DISCORD_TOKEN);
// ===== Helper funcs (metadata) =====
async function getNftMetadataAlchemy(tokenId, contractAddress = SQUIGS_CONTRACT, chain = DEFAULT_NFT_CHAIN) {
  const u = new URL(alchemyNftUrl(chain, 'getNFTMetadata'));
  u.searchParams.set('contractAddress', contractAddress);
  u.searchParams.set('tokenId', tokenId);
  u.searchParams.set('refreshCache', 'false');
  const res = await fetchWithRetry(u.toString(), 3, 800, { timeout: 10000 });
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
async function getTraitsForToken(alchemyMeta, tokenId, contractAddress = SQUIGS_CONTRACT, chain = DEFAULT_NFT_CHAIN) {
  // 1) Try Alchemy
  const attrsA = extractAttributesFlexible(alchemyMeta);
  if (attrsA.length > 0) {
    return { attrs: normalizeSquigsReloadedAttrs(attrsA, contractAddress), source: 'alchemy' };
  }

  // 2) Prefer bundled metadata when this collection has it.
  const attrsC = localSquigTraits(tokenId, contractAddress, chain);
  if (attrsC.length > 0) {
    return { attrs: normalizeSquigsReloadedAttrs(attrsC.map(massageTraitKeys).filter(validAttrFilter), contractAddress), source: 'local_csv' };
  }

  // 3) Fallback to OpenSea if we have an API key
  if (OPENSEA_API_KEY) {
    try {
      const attrsB = await fetchOpenSeaTraits(tokenId, contractAddress, chain);
      if (attrsB.length > 0) {
        console.log(`ℹ️ Traits from OpenSea fallback for #${tokenId}: ${attrsB.length}`);
        return { attrs: normalizeSquigsReloadedAttrs(attrsB, contractAddress), source: 'opensea' };
      }
    } catch (e) {
      const msg = String(e?.message || e || '');
      // 404 on OpenSea is common for missing/unindexed tokens; keep logs clean.
      if (!/OpenSea HTTP 404/i.test(msg)) {
        console.warn('⚠️ OpenSea trait fallback failed:', msg);
      }
    }
  }

  return { attrs: [], source: 'none' };
}

const NFT_TRAIT_CACHE_TTL_MS = Math.max(0, numberFromEnv('NFT_TRAIT_CACHE_TTL_MS', 15 * 60 * 1000));
const nftTraitCache = new Map();

async function fetchTraitsForTokenOpenSeaFirst(tokenId, contractAddress, chain) {
  const localAttrs = localSquigTraits(tokenId, contractAddress, chain);
  if (localAttrs.length > 0) {
    return {
      attrs: normalizeSquigsReloadedAttrs(localAttrs.map(massageTraitKeys).filter(validAttrFilter), contractAddress),
      source: 'local_csv',
    };
  }

  let openSeaError = null;
  if (OPENSEA_API_KEY) {
    try {
      const attrs = await fetchOpenSeaTraits(tokenId, contractAddress, chain);
      if (attrs.length > 0) {
        return { attrs: normalizeSquigsReloadedAttrs(attrs, contractAddress), source: 'opensea' };
      }
    } catch (err) {
      openSeaError = err;
    }
  }

  try {
    const alchemyMeta = await getNftMetadataAlchemy(tokenId, contractAddress, chain);
    const attrs = extractAttributesFlexible(alchemyMeta);
    if (attrs.length > 0) {
      return { attrs: normalizeSquigsReloadedAttrs(attrs, contractAddress), source: 'alchemy' };
    }
  } catch (err) {
    if (openSeaError) {
      throw new Error(
        `${providerFailureSummary('OpenSea traits', openSeaError)}; ${providerFailureSummary('Alchemy metadata', err)}`
      );
    }
    if (!OPENSEA_API_KEY) throw err;
  }

  return { attrs: [], source: 'none' };
}

async function getTraitsForTokenResilient(tokenId, contractAddress = SQUIGS_CONTRACT, chain = DEFAULT_NFT_CHAIN) {
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  const normalizedContract = normalizeEthAddress(contractAddress) || String(contractAddress || '').toLowerCase();
  const cacheKey = `${normalizedChain}:${normalizedContract}:${String(tokenId)}`;
  const cached = nftTraitCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) return cached.promise;

  const promise = fetchTraitsForTokenOpenSeaFirst(tokenId, normalizedContract, normalizedChain);
  nftTraitCache.set(cacheKey, { expiresAt: Date.now() + NFT_TRAIT_CACHE_TTL_MS, promise });
  if (nftTraitCache.size > 10000) {
    const oldestKey = nftTraitCache.keys().next().value;
    if (oldestKey && oldestKey !== cacheKey) nftTraitCache.delete(oldestKey);
  }
  promise.catch(() => nftTraitCache.delete(cacheKey));

  return promise;
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

function normalizeSquigsReloadedAttrs(attrs, contractAddress = SQUIGS_CONTRACT) {
  const normalized = Array.isArray(attrs) ? attrs : [];
  if (!isSquigsContract(contractAddress)) return normalized;
  return normalized.map((attr) => {
    const traitType = String(attr?.trait_type ?? '').trim();
    return {
      ...attr,
      trait_type: traitType.toLowerCase() === 'legend' ? 'Legendary' : traitType,
    };
  });
}

function validAttrFilter(t) {
  const v = String(t?.value ?? '').trim();
  if (!v) return false;
  const low = v.toLowerCase();
  return !(low === 'none' || low === 'none (ignore)');
}

// OpenSea v2: fallback trait fetch (with headers + small retry)
async function fetchOpenSeaTraits(tokenId, contractAddress = SQUIGS_CONTRACT, chain = DEFAULT_NFT_CHAIN) {
  const cfg = nftChainConfig(chain);
  const url = `https://api.opensea.io/api/v2/chain/${cfg.openseaChain}/contract/${contractAddress}/nfts/${tokenId}`;
  const headers = { 'X-API-KEY': OPENSEA_API_KEY };
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetchWithTimeout(url, { headers, timeoutMs: 10000 });
      if (res.status === 404) return [];
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
const SQUIGS_RELOADED_HP_TABLE = loadHpTableFromCsv(path.join(__dirname, 'Squigs_Reloaded_Traits_Only_UglyPoints.csv'));

function hpTableForContract(contractAddress, guildPointMappings = null, chain = DEFAULT_NFT_CHAIN) {
  const c = String(contractAddress || '').toLowerCase();
  const normalizedChain = normalizeNftChain(chain) || DEFAULT_NFT_CHAIN;
  const guildMap = getPointMappingForContract(guildPointMappings, c, normalizedChain);
  if (guildMap && typeof guildMap === 'object' && Object.keys(guildMap).length > 0) return guildMap;
  if (normalizedChain === DEFAULT_NFT_CHAIN && c === UGLY_CONTRACT.toLowerCase()) return UGLY_HP_TABLE;
  if (normalizedChain === DEFAULT_NFT_CHAIN && c === MONSTER_CONTRACT.toLowerCase()) return MONSTER_HP_TABLE;
  if (isSquigsContract(c)) return Object.keys(SQUIGS_RELOADED_HP_TABLE).length ? SQUIGS_RELOADED_HP_TABLE : HP_TABLE;
  return {};
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
  const desiredCategory = normalizeTraitCategoryForMatch(traitCategory);
  if (!desiredValue || !table || typeof table !== 'object') return null;

  for (const [category, traits] of Object.entries(table)) {
    if (!traits || typeof traits !== 'object') continue;
    if (desiredCategory && normalizeTraitCategoryForMatch(category) !== desiredCategory) continue;
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
  const desiredCategory = normalizeTraitCategoryForMatch(traitCategory);
  if (!desiredValue || !groupedTraits || typeof groupedTraits !== 'object') return false;

  if (desiredCategory) {
    const matchedCategory = Object.keys(groupedTraits).find((cat) => normalizeTraitCategoryForMatch(cat) === desiredCategory);
    const entries = matchedCategory && Array.isArray(groupedTraits[matchedCategory]) ? groupedTraits[matchedCategory] : [];
    return entries.some((t) => String(t?.value || '').trim().toLowerCase() === desiredValue);
  }

  for (const entries of Object.values(groupedTraits)) {
    if (!Array.isArray(entries)) continue;
    if (entries.some((t) => String(t?.value || '').trim().toLowerCase() === desiredValue)) return true;
  }
  return false;
}

function normalizeTraitCategoryForMatch(category) {
  const normalized = String(category || '').trim().toLowerCase();
  if (normalized === 'legend') return 'legendary';
  return normalized;
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


