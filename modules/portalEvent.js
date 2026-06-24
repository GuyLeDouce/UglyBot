const {
  EmbedBuilder,
  ButtonBuilder,
  ButtonStyle,
  ActionRowBuilder,
  StringSelectMenuBuilder,
  AttachmentBuilder,
} = require('discord.js');
const fs = require('fs');
const path = require('path');

function readPositiveIntEnv(name, fallback, { min = 1, max = Number.MAX_SAFE_INTEGER } = {}) {
  const value = Number(process.env[name]);
  if (!Number.isFinite(value)) return fallback;
  return Math.min(max, Math.max(min, Math.floor(value)));
}

const ONE_HOUR_MS = 60 * 60 * 1000;
const MIN_DELAY_MS = 4 * 60 * 60 * 1000;
const MAX_DELAY_MS = 12 * 60 * 60 * 1000;
const SINGLE_TRAIT_CHANCE = 0.85;
const MAX_SELECT_OPTIONS = 25;
const SQUIGS_CONTRACT = String(process.env.SQUIG_COLLECTION_CONTRACT || '0x8c9a02c0585200c4c65608df6b8def543d33792a').toLowerCase();
const DEFAULT_MAX_TOKEN_ID = Number(process.env.PORTAL_MAX_SQUIG_TOKEN_ID || 10000);
const REAL_SQUIG_SCAN_ATTEMPTS = readPositiveIntEnv('PORTAL_REAL_SQUIG_SCAN_ATTEMPTS', 40, { min: 1, max: 500 });
const PREVIEW_SCAN_ATTEMPTS = readPositiveIntEnv('PORTAL_PREVIEW_SCAN_ATTEMPTS', 30, { min: 0, max: 300 });
const PREVIEW_LINEAR_SCAN_ATTEMPTS = readPositiveIntEnv('PORTAL_PREVIEW_LINEAR_SCAN_ATTEMPTS', 80, { min: 0, max: 1000 });
const TOKEN_LOOKUP_TIMEOUT_MS = readPositiveIntEnv('PORTAL_TOKEN_LOOKUP_TIMEOUT_MS', 7000, { min: 1000, max: 30000 });
const REAL_SQUIG_SCAN_BUDGET_MS = readPositiveIntEnv('PORTAL_REAL_SQUIG_SCAN_BUDGET_MS', 25000, { min: 1000, max: 120000 });
const PREVIEW_SCAN_BUDGET_MS = readPositiveIntEnv('PORTAL_PREVIEW_SCAN_BUDGET_MS', 15000, { min: 1000, max: 120000 });
const CLAIM_OWNERSHIP_TIMEOUT_MS = readPositiveIntEnv('PORTAL_CLAIM_OWNERSHIP_TIMEOUT_MS', 25000, { min: 3000, max: 120000 });
const CLAIM_TRAIT_SCAN_BUDGET_MS = readPositiveIntEnv('PORTAL_CLAIM_TRAIT_SCAN_BUDGET_MS', 15000, { min: 1000, max: 120000 });
const SQUIGS_OPENSEA_URL = 'https://opensea.io/collection/squigs-reloaded';
const SQUIGS_IMAGE_TEMPLATE = String(process.env.SQUIG_IMAGE_BASE_URL || '').replace(/\/+$/, '');
const PORTAL_RECEIPT_CHANNEL_ID = '1477463175665287410';
const LOCAL_SQUIG_METADATA_CANDIDATES = [
  path.join(__dirname, '..', 'metadata.csv'),
  path.join(__dirname, 'metadata.csv'),
];
const LOCAL_SQUIG_IMAGE_DIR_CANDIDATES = [
  path.join(__dirname, '..', 'images'),
  path.join(__dirname, 'images'),
];

let deps = null;

let portalActive = false;
let portalTimeout = null;
let portalCloseTimeout = null;
let claimedUsers = new Set();
let pendingSelections = new Map();
let portalMessageRef = null;
let portalChannelId = process.env.PORTAL_CHANNEL_ID || null;
let portalGuildId = process.env.PORTAL_GUILD_ID || null;
let currentPortal = null;
let schedulerEnabled = false;
let nextPortalAt = null;
let nextPortalDelayOverrideMs = null;
let localSquigMetadataCache = null;

function initPortalEvent(injectedDeps) {
  deps = injectedDeps;
}

function assertReady() {
  if (!deps) throw new Error('Portal module not initialized. Call initPortalEvent first.');
}

function randomInt(min, maxInclusive) {
  return Math.floor(Math.random() * (maxInclusive - min + 1)) + min;
}

function pickRandom(arr) {
  if (!Array.isArray(arr) || !arr.length) return null;
  return arr[Math.floor(Math.random() * arr.length)];
}

function portalLog(message, extra = null) {
  if (extra) {
    console.log(`[portal] ${message}`, extra);
  } else {
    console.log(`[portal] ${message}`);
  }
}

function portalWarn(message, extra = null) {
  if (extra) {
    console.warn(`[portal] ${message}`, extra);
  } else {
    console.warn(`[portal] ${message}`);
  }
}

function withTimeout(promise, timeoutMs, label) {
  let timeout = null;
  const timeoutPromise = new Promise((_, reject) => {
    timeout = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
  });
  return Promise.race([promise, timeoutPromise]).finally(() => {
    if (timeout) clearTimeout(timeout);
  });
}

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
        if (!value || /^none(\s*\(ignore\))?$/i.test(value)) continue;
        attrs.push({ trait_type: match[1], value });
      }
      byTokenId.set(tokenId, {
        name: nameIdx >= 0 ? String(row[nameIdx] || '').trim() : `Squig #${tokenId}`,
        fileName: fileIdx >= 0 ? String(row[fileIdx] || '').trim() : `${tokenId}.png`,
        attrs,
      });
    }
    portalLog(`loaded ${byTokenId.size} local Squig metadata rows`);
  } catch (err) {
    portalWarn(`could not load local Squig metadata: ${err.message}`);
  }

  localSquigMetadataCache = byTokenId;
  return byTokenId;
}

function localSquigImagePath(tokenId) {
  const tid = String(tokenId || '').trim();
  if (!/^\d+$/.test(tid)) return null;
  const localMeta = loadLocalSquigMetadata().get(tid);
  const fileName = path.basename(localMeta?.fileName || `${tid}.png`);
  for (const imageDir of LOCAL_SQUIG_IMAGE_DIR_CANDIDATES) {
    const candidate = path.join(imageDir, fileName);
    if (fs.existsSync(candidate)) return candidate;
  }
  return null;
}

function localSquigTraits(tokenId) {
  return loadLocalSquigMetadata().get(String(tokenId || '').trim())?.attrs || [];
}

function attachmentNameForSquig(tokenId, prefix = 'portal-squig') {
  return `${prefix}-${String(tokenId || 'unknown').replace(/[^\w.-]/g, '')}.png`;
}

function normalizeImageUrl(input) {
  if (!input) return null;

  // Some metadata providers return nested objects for tokenUri/image fields.
  if (typeof input === 'object') {
    const nested =
      input.gateway ||
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

function squigImageUrl(tokenId) {
  const tid = String(tokenId || '').trim();
  if (!/^\d+$/.test(tid)) return null;
  if (!SQUIGS_IMAGE_TEMPLATE) return null;
  return `${SQUIGS_IMAGE_TEMPLATE}/${tid}`;
}

function traitValuesFromAttrs(attrs) {
  const out = [];
  for (const t of Array.isArray(attrs) ? attrs : []) {
    const value = String(t?.value || '').trim();
    if (!value || /^none(\s*\(ignore\))?$/i.test(value)) continue;
    out.push(value);
  }
  return out;
}

function hasRequiredTraits(traits, traitA, traitB = null) {
  const normalized = new Set((Array.isArray(traits) ? traits : []).map((x) => String(x || '').toLowerCase()));
  const a = String(traitA || '').toLowerCase();
  const b = traitB ? String(traitB).toLowerCase() : null;
  if (!a) return false;
  if (!normalized.has(a)) return false;
  if (b && !normalized.has(b)) return false;
  return true;
}

function flattenTraitTable(table) {
  const out = [];
  for (const traits of Object.values(table || {})) {
    if (!traits || typeof traits !== 'object') continue;
    for (const [trait, points] of Object.entries(traits)) {
      const uglyPoints = Number(points);
      if (!trait || !Number.isFinite(uglyPoints) || uglyPoints <= 0) continue;
      out.push({ trait: String(trait), uglyPoints: Math.floor(uglyPoints) });
    }
  }
  return out;
}

function findMappedTraitPoint(table, traitType, traitValue) {
  const desiredType = String(traitType || '').trim().toLowerCase();
  const desiredValue = String(traitValue || '').trim().toLowerCase();
  if (!desiredType || !desiredValue || !table || typeof table !== 'object') return null;

  for (const [category, traits] of Object.entries(table)) {
    if (String(category || '').trim().toLowerCase() !== desiredType) continue;
    if (!traits || typeof traits !== 'object') continue;
    for (const [trait, points] of Object.entries(traits)) {
      if (String(trait || '').trim().toLowerCase() !== desiredValue) continue;
      const p = Number(points);
      if (!Number.isFinite(p) || p <= 0) return null;
      return { category, trait: String(trait), uglyPoints: Math.floor(p) };
    }
  }
  return null;
}

function choosePortalTraits(traitPool) {
  const unique = new Map();
  for (const entry of traitPool) {
    const key = String(entry.trait || '').toLowerCase();
    if (!key || unique.has(key)) continue;
    unique.set(key, { trait: entry.trait, uglyPoints: Number(entry.uglyPoints) || 0 });
  }
  const traits = [...unique.values()].filter((t) => t.uglyPoints > 0);
  if (!traits.length) return null;

  const isDual = Math.random() > SINGLE_TRAIT_CHANCE && traits.length >= 2;
  const traitA = pickRandom(traits);
  if (!traitA) return null;
  if (!isDual) {
    return { type: 'single', traitA, traitB: null, reward: traitA.uglyPoints };
  }
  const remaining = traits.filter((t) => t.trait.toLowerCase() !== traitA.trait.toLowerCase());
  const traitB = pickRandom(remaining);
  if (!traitB) return { type: 'single', traitA, traitB: null, reward: traitA.uglyPoints };
  return { type: 'dual', traitA, traitB, reward: traitA.uglyPoints + traitB.uglyPoints };
}

function buildPortalChoiceFromAttrs(tokenId, attrs, table) {
  const mapped = [];
  for (const a of Array.isArray(attrs) ? attrs : []) {
    const category = String(a?.trait_type || '').trim();
    const value = String(a?.value || '').trim();
    if (!category || !value) continue;
    const m = findMappedTraitPoint(table, category, value);
    if (!m) continue;
    mapped.push(m);
  }
  if (!mapped.length) return null;

  const byTrait = new Map();
  for (const m of mapped) {
    const key = String(m.trait || '').toLowerCase();
    if (!key || byTrait.has(key)) continue;
    byTrait.set(key, m);
  }
  const unique = [...byTrait.values()];
  if (!unique.length) return null;

  const wantsDual = Math.random() > SINGLE_TRAIT_CHANCE;
  const traitA = pickRandom(unique);
  if (!traitA) return null;

  let type = 'single';
  let traitB = null;
  if (wantsDual) {
    const dualCandidates = unique.filter(
      (x) => x.trait.toLowerCase() !== traitA.trait.toLowerCase() &&
        String(x.category || '').toLowerCase() !== String(traitA.category || '').toLowerCase()
    );
    if (dualCandidates.length) {
      traitB = pickRandom(dualCandidates);
      type = 'dual';
    }
  }

  const reward = type === 'dual'
    ? traitA.uglyPoints + (traitB?.uglyPoints || 0)
    : traitA.uglyPoints;
  const localMeta = loadLocalSquigMetadata().get(String(tokenId));
  const imagePath = localSquigImagePath(tokenId);
  const imageUrl = normalizeImageUrl(squigImageUrl(tokenId));
  if (!imagePath && !imageUrl) return null;

  return {
    portal: { type, traitA, traitB, reward },
    preview: {
      tokenId: String(tokenId),
      imagePath,
      imageUrl,
      name: String(localMeta?.name || `Squig #${tokenId}`),
    },
  };
}

function choosePortalFromLocalSquig(table) {
  const local = loadLocalSquigMetadata();
  const tokenIds = [...local.keys()];
  if (!tokenIds.length) return null;

  for (let i = 0; i < Math.min(80, tokenIds.length); i++) {
    const tokenId = pickRandom(tokenIds);
    const result = buildPortalChoiceFromAttrs(tokenId, local.get(tokenId)?.attrs, table);
    if (result) return result;
  }
  return null;
}

async function inspectTokenForPortalChoice(tokenId, table) {
  const meta = await deps.getNftMetadataAlchemy(tokenId, SQUIGS_CONTRACT);
  const { attrs } = await deps.getTraitsForToken(meta, tokenId, SQUIGS_CONTRACT);
  const imageUrl = normalizeImageUrl(
    squigImageUrl(tokenId) ||
    meta?.image ||
    meta?.metadata?.image ||
    meta?.raw?.metadata?.image ||
    meta?.tokenUri?.gateway ||
    meta?.tokenUri?.raw
  );
  if (!imageUrl) return null;
  const choice = buildPortalChoiceFromAttrs(tokenId, attrs, table);
  if (!choice) return null;

  return {
    portal: choice.portal,
    preview: { tokenId, imageUrl, name: String(meta?.name || `Squig #${tokenId}`) },
  };
}

async function choosePortalFromRealSquig(table) {
  const deadline = Date.now() + REAL_SQUIG_SCAN_BUDGET_MS;
  let failures = 0;
  for (let i = 0; i < REAL_SQUIG_SCAN_ATTEMPTS && Date.now() < deadline; i++) {
    const tokenId = String(randomInt(1, DEFAULT_MAX_TOKEN_ID));
    try {
      const result = await withTimeout(
        inspectTokenForPortalChoice(tokenId, table),
        TOKEN_LOOKUP_TIMEOUT_MS,
        `Portal real Squig lookup #${tokenId}`
      );
      if (result) return result;
    } catch (err) {
      failures++;
      if (failures <= 3) portalWarn(`real Squig scan skipped #${tokenId}: ${err.message}`);
    }
  }
  portalWarn(`real Squig scan found no usable token after ${REAL_SQUIG_SCAN_ATTEMPTS} attempt(s) or ${REAL_SQUIG_SCAN_BUDGET_MS}ms`);
  return null;
}

async function resolvePortalChannel() {
  assertReady();
  const client = deps.client;
  const channelId = portalChannelId || process.env.PORTAL_CHANNEL_ID;
  if (!channelId) return null;
  const cached = client.channels.cache.get(channelId);
  if (cached) return cached;
  try {
    return await client.channels.fetch(channelId);
  } catch {
    return null;
  }
}

async function findSquigForPreview(requiredTraitA, requiredTraitB = null) {
  assertReady();
  const localCandidates = [];
  for (const [tokenId, meta] of loadLocalSquigMetadata().entries()) {
    const traitValues = traitValuesFromAttrs(meta?.attrs);
    if (!hasRequiredTraits(traitValues, requiredTraitA, requiredTraitB)) continue;
    const imagePath = localSquigImagePath(tokenId);
    if (!imagePath && !SQUIGS_IMAGE_TEMPLATE) continue;
    localCandidates.push({
      tokenId,
      imagePath,
      imageUrl: normalizeImageUrl(squigImageUrl(tokenId)),
      name: String(meta?.name || `Squig #${tokenId}`),
    });
    if (localCandidates.length >= 50) break;
  }
  const localPick = pickRandom(localCandidates);
  if (localPick) return localPick;

  const deadline = Date.now() + PREVIEW_SCAN_BUDGET_MS;
  const randomTries = PREVIEW_SCAN_ATTEMPTS;
  const linearTries = PREVIEW_LINEAR_SCAN_ATTEMPTS;
  const candidates = [];
  let failures = 0;

  const inspectToken = async (tokenId) => {
    try {
      await withTimeout(
        (async () => {
          const meta = await deps.getNftMetadataAlchemy(tokenId, SQUIGS_CONTRACT);
          const { attrs } = await deps.getTraitsForToken(meta, tokenId, SQUIGS_CONTRACT);
          const traitValues = traitValuesFromAttrs(attrs);
          if (!hasRequiredTraits(traitValues, requiredTraitA, requiredTraitB)) return;
          const imageUrl = normalizeImageUrl(
            squigImageUrl(tokenId) ||
            meta?.image ||
            meta?.metadata?.image ||
            meta?.raw?.metadata?.image ||
            meta?.tokenUri?.gateway ||
            meta?.tokenUri?.raw
          );
          const name = String(meta?.name || `Squig #${tokenId}`);
          candidates.push({ tokenId, imageUrl, name });
        })(),
        TOKEN_LOOKUP_TIMEOUT_MS,
        `Portal preview lookup #${tokenId}`
      );
    } catch (err) {
      failures++;
      if (failures <= 3) portalWarn(`preview scan skipped #${tokenId}: ${err.message}`);
    }
  };

  for (let i = 0; i < randomTries && Date.now() < deadline; i++) {
    const tokenId = String(randomInt(1, DEFAULT_MAX_TOKEN_ID));
    await inspectToken(tokenId);
    if (candidates.length >= 3) break;
  }

  // Fallback: linear probe from a random start for better hit-rate on rare trait combos.
  if (!candidates.length && Date.now() < deadline) {
    const start = randomInt(1, DEFAULT_MAX_TOKEN_ID);
    for (let i = 0; i < linearTries && Date.now() < deadline; i++) {
      const next = ((start + i - 1) % DEFAULT_MAX_TOKEN_ID) + 1;
      await inspectToken(String(next));
      if (candidates.length >= 3) break;
    }
  }

  return pickRandom(candidates) || null;
}

function buildPortalEmbed(portal, preview) {
  const traitLine = portal.type === 'dual'
    ? `${portal.traitA.trait} + ${portal.traitB.trait}`
    : portal.traitA.trait;

  const embed = new EmbedBuilder()
    .setTitle('PORTAL MALFUNCTION')
    .setDescription(
      `The Squig portal has glitched.\n\n` +
      `Trait detected leaking through the portal:\n\n` +
      `${traitLine}\n\n` +
      `Reward:\n` +
      `${portal.reward} $CHARM\n\n` +
      `Portal Stability Window:\n` +
      `1 Hour`
    )
    .setColor(0xE67E22);

  if (preview?.imagePath) {
    embed.setImage(`attachment://${attachmentNameForSquig(preview.tokenId, 'portal-preview')}`);
  } else if (preview?.imageUrl) {
    embed.setImage(preview.imageUrl);
  }
  if (preview?.imagePath || preview?.imageUrl) {
    embed.addFields({
      name: 'Example Squig Match',
      value: `Squig #${preview.tokenId}\nhttps://opensea.io/assets/ethereum/${SQUIGS_CONTRACT}/${preview.tokenId}`,
    });
  }
  return embed;
}

function buildPortalMessagePayload(portal, preview) {
  const payload = {
    content: '@everyone  PORTAL MALFUNCTION',
    embeds: [buildPortalEmbed(portal, preview)],
    components: [buildPortalButtonRow(false)],
  };
  if (preview?.imagePath) {
    payload.files = [
      new AttachmentBuilder(preview.imagePath, {
        name: attachmentNameForSquig(preview.tokenId, 'portal-preview'),
      }),
    ];
  }
  return payload;
}

function buildPortalButtonRow(disabled = false) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId('portal_claim')
      .setLabel('STABILIZE PORTAL')
      .setStyle(ButtonStyle.Danger)
      .setDisabled(disabled)
  );
}

function buildNoTraitFunnyResponse(portal) {
  const traitLine = portal?.type === 'dual'
    ? `${portal?.traitA?.trait} + ${portal?.traitB?.trait}`
    : `${portal?.traitA?.trait || 'Unknown Trait'}`;

  const lines = [
    `The portal rejected you. It was looking for **${traitLine}**, but your Squigs are still cooking.`,
    `Portal scanner result: **no ${traitLine} detected**. The wormhole asks for more chaos.`,
    `Your Squigs knocked, but the portal only opens for **${traitLine}** this round.`,
    `The portal sneezed and said: "Bring me **${traitLine}** and maybe we talk."`,
    `Almost! This portal run needs **${traitLine}** and your current squad missed the vibe check.`,
  ];
  const pick = lines[Math.floor(Math.random() * lines.length)];
  return (
    `${pick}\n\n` +
    `Find one on OpenSea:\n` +
    `${SQUIGS_OPENSEA_URL}`
  );
}

function clearPendingSelections() {
  pendingSelections.clear();
}

function clearTimers() {
  if (portalTimeout) clearTimeout(portalTimeout);
  if (portalCloseTimeout) clearTimeout(portalCloseTimeout);
  portalTimeout = null;
  portalCloseTimeout = null;
}

function schedulePortalWithDelay(delayMs) {
  if (!schedulerEnabled) return;
  if (portalTimeout) {
    clearTimeout(portalTimeout);
    portalTimeout = null;
  }
  const safeDelay = Math.max(60 * 1000, Math.floor(Number(delayMs) || 0));
  nextPortalAt = Date.now() + safeDelay;
  portalTimeout = setTimeout(() => {
    nextPortalAt = null;
    triggerPortalEvent().catch((err) => {
      console.error('Portal trigger error:', err);
      schedulePortal();
    });
  }, safeDelay);
}

function schedulePortal() {
  if (!schedulerEnabled) return;
  const delay = Number.isFinite(nextPortalDelayOverrideMs)
    ? nextPortalDelayOverrideMs
    : Math.floor(Math.random() * (MAX_DELAY_MS - MIN_DELAY_MS + 1)) + MIN_DELAY_MS;
  nextPortalDelayOverrideMs = null;
  schedulePortalWithDelay(delay);
}

async function closePortal({ announce = true } = {}) {
  portalActive = false;
  currentPortal = null;
  claimedUsers = new Set();
  clearPendingSelections();
  if (portalCloseTimeout) {
    clearTimeout(portalCloseTimeout);
    portalCloseTimeout = null;
  }

  if (portalMessageRef) {
    try {
      await portalMessageRef.edit({ components: [buildPortalButtonRow(true)] });
    } catch {}
  }

  if (announce && portalMessageRef?.channel) {
    try {
      await portalMessageRef.channel.send('The portal has stabilized and closed.');
    } catch {}
  }

  portalMessageRef = null;
  schedulePortal();
}

async function triggerPortalEvent(options = {}) {
  assertReady();
  if (portalActive) return { ok: false, reason: 'Portal already active.' };
  const startedAt = Date.now();
  portalLog('trigger requested', {
    channelId: options.channelId || portalChannelId || process.env.PORTAL_CHANNEL_ID || null,
    guildId: options.guildId || portalGuildId || process.env.PORTAL_GUILD_ID || null,
  });

  if (portalTimeout) {
    clearTimeout(portalTimeout);
    portalTimeout = null;
  }
  nextPortalAt = null;

  if (options.channelId) portalChannelId = String(options.channelId);
  if (options.guildId) portalGuildId = String(options.guildId);

  const channel = await resolvePortalChannel();
  if (!channel || !channel.isTextBased()) {
    schedulePortal();
    portalWarn('trigger failed: channel not found or not text-based');
    return { ok: false, reason: 'Portal channel not found or not text-based.' };
  }

  const guildPointMappings = await deps.getGuildPointMappings(channel.guild.id);
  const squigTable = deps.hpTableForContract(SQUIGS_CONTRACT, guildPointMappings);
  let selected = null;
  let preview = null;

  const localSquigChoice = choosePortalFromLocalSquig(squigTable);
  const realSquigChoice = localSquigChoice || await choosePortalFromRealSquig(squigTable);
  if (realSquigChoice?.portal && realSquigChoice?.preview) {
    selected = realSquigChoice.portal;
    preview = realSquigChoice.preview;
    portalLog(localSquigChoice ? 'selected traits from local Squig preview' : 'selected traits from real Squig preview', {
      tokenId: preview.tokenId,
      type: selected.type,
      reward: selected.reward,
    });
  } else {
    const traitPool = flattenTraitTable(squigTable);
    selected = choosePortalTraits(traitPool);
    if (!selected) {
      schedulePortal();
      portalWarn('trigger failed: no valid traits available');
      return { ok: false, reason: 'No valid traits available for portal event.' };
    }
    portalLog('selected traits from point table fallback', {
      type: selected.type,
      traitA: selected.traitA?.trait,
      traitB: selected.traitB?.trait || null,
      reward: selected.reward,
    });
    preview = await findSquigForPreview(
      selected.traitA.trait,
      selected.type === 'dual' ? selected.traitB.trait : null
    );
    if (!preview?.imageUrl) {
      portalWarn('posting portal without example Squig image; preview scan found no match');
    }
  }

  portalActive = true;
  claimedUsers = new Set();
  clearPendingSelections();
  currentPortal = {
    type: selected.type,
    reward: selected.reward,
    traitA: selected.traitA,
    traitB: selected.traitB,
    startedAt: Date.now(),
    expiresAt: Date.now() + ONE_HOUR_MS,
  };

  let portalMessage = null;
  try {
    portalMessage = await channel.send(buildPortalMessagePayload(currentPortal, preview));
  } catch (err) {
    portalActive = false;
    currentPortal = null;
    claimedUsers = new Set();
    clearPendingSelections();
    schedulePortal();
    portalWarn(`trigger failed while sending portal message: ${err.message}`);
    return { ok: false, reason: `Failed to send portal message: ${err.message}` };
  }
  portalMessageRef = portalMessage;

  portalCloseTimeout = setTimeout(() => {
    closePortal({ announce: true }).catch((err) => console.error('Portal close error:', err));
  }, ONE_HOUR_MS);

  portalLog(`trigger completed in ${Date.now() - startedAt}ms`, { messageId: portalMessage.id });
  return { ok: true, portal: currentPortal, messageId: portalMessage.id };
}

function startPortalScheduler(options = {}) {
  assertReady();
  schedulerEnabled = true;
  if (options.channelId) portalChannelId = String(options.channelId);
  if (options.guildId) portalGuildId = String(options.guildId);
  if (!portalActive && !portalTimeout) schedulePortal();
  return { ok: true, schedulerEnabled, portalActive };
}

async function stopPortalScheduler({ closeActivePortal = true } = {}) {
  assertReady();
  schedulerEnabled = false;
  if (portalTimeout) clearTimeout(portalTimeout);
  portalTimeout = null;
  nextPortalAt = null;
  nextPortalDelayOverrideMs = null;

  if (closeActivePortal && portalActive) {
    await closePortal({ announce: true });
  } else if (portalCloseTimeout) {
    clearTimeout(portalCloseTimeout);
    portalCloseTimeout = null;
  }
  return { ok: true, schedulerEnabled, portalActive };
}

function setNextPortalTriggerDelayMinutes(minutes) {
  assertReady();
  const parsedMinutes = Math.floor(Number(minutes));
  if (!Number.isFinite(parsedMinutes) || parsedMinutes < 1) {
    return { ok: false, reason: 'Minutes must be at least 1.' };
  }

  const delayMs = parsedMinutes * 60 * 1000;
  nextPortalDelayOverrideMs = delayMs;

  if (schedulerEnabled) {
    if (portalActive && currentPortal?.expiresAt) {
      nextPortalAt = Number(currentPortal.expiresAt) + delayMs;
    } else {
      schedulePortalWithDelay(delayMs);
    }
  }

  return {
    ok: true,
    schedulerEnabled,
    portalActive,
    nextPortalAt,
    delayMinutes: parsedMinutes,
  };
}

async function fetchEligibleSquigsForUser(links) {
  const walletAddresses = links.map((x) => x.wallet_address).filter(Boolean);
  if (!walletAddresses.length) return [];

  portalLog('claim ownership lookup started', { wallets: walletAddresses.length });
  const tokenIds = await withTimeout(
    deps.getOwnedTokenIdsForContractMany(walletAddresses, SQUIGS_CONTRACT, undefined, { concurrency: 3 }),
    CLAIM_OWNERSHIP_TIMEOUT_MS,
    'Portal claim ownership lookup'
  );
  portalLog('claim ownership lookup completed', { tokenCount: tokenIds.length });
  if (!tokenIds.length) return [];

  const squigs = [];
  const deadline = Date.now() + CLAIM_TRAIT_SCAN_BUDGET_MS;
  let remoteLookups = 0;
  let failures = 0;
  for (const tokenId of tokenIds) {
    try {
      let attrs = localSquigTraits(tokenId);
      let imagePath = localSquigImagePath(tokenId);
      let imageUrl = normalizeImageUrl(squigImageUrl(tokenId));
      if (!attrs.length) {
        if (Date.now() >= deadline) break;
        remoteLookups++;
        const meta = await withTimeout(
          deps.getNftMetadataAlchemy(tokenId, SQUIGS_CONTRACT),
          TOKEN_LOOKUP_TIMEOUT_MS,
          `Portal claim metadata lookup #${tokenId}`
        );
        const traitsResult = await withTimeout(
          deps.getTraitsForToken(meta, tokenId, SQUIGS_CONTRACT),
          TOKEN_LOOKUP_TIMEOUT_MS,
          `Portal claim traits lookup #${tokenId}`
        );
        attrs = traitsResult?.attrs || [];
        imageUrl = normalizeImageUrl(
          imageUrl ||
          meta?.image ||
          meta?.metadata?.image ||
          meta?.raw?.metadata?.image ||
          meta?.tokenUri?.gateway ||
          meta?.tokenUri?.raw
        );
      }
      const traits = traitValuesFromAttrs(attrs);
      const valid = hasRequiredTraits(
        traits,
        currentPortal?.traitA?.trait,
        currentPortal?.type === 'dual' ? currentPortal?.traitB?.trait : null
      );
      if (!valid) continue;
      squigs.push({ tokenId: String(tokenId), traits, imagePath, imageUrl });
    } catch (err) {
      failures++;
      if (failures <= 3) portalWarn(`claim trait scan skipped #${tokenId}: ${err.message}`);
    }
  }

  portalLog('claim trait scan completed', {
    eligibleCount: squigs.length,
    scannedTokens: tokenIds.length,
    remoteLookups,
  });
  return squigs;
}

async function handlePortalClaim(interaction) {
  assertReady();
  if (!portalActive || !currentPortal) {
    await interaction.reply({ content: 'The portal is not active right now.', flags: 64 });
    return;
  }

  await interaction.deferReply({ flags: 64 });

  const links = await deps.getWalletLinks(interaction.guild.id, interaction.user.id);
  if (!links.length) {
    await interaction.editReply({ content: 'You must link your wallet to interact with the portal.' });
    return;
  }

  if (claimedUsers.has(interaction.user.id)) {
    await interaction.editReply({ content: 'You already stabilized this portal.' });
    return;
  }

  let squigs = [];
  try {
    squigs = await fetchEligibleSquigsForUser(links);
  } catch (err) {
    portalWarn(`claim lookup failed for ${interaction.user.id}: ${err.message}`);
    await interaction.editReply({
      content: `Portal scan timed out while checking your Squigs. Try again in a moment.`
    });
    return;
  }
  if (!squigs.length) {
    await interaction.editReply({ content: buildNoTraitFunnyResponse(currentPortal) });
    return;
  }

  pendingSelections.set(interaction.user.id, {
    createdAt: Date.now(),
    squigsById: new Map(squigs.map((s) => [String(s.tokenId), s])),
  });

  const options = squigs.slice(0, MAX_SELECT_OPTIONS).map((s) => ({
    label: `Squig #${s.tokenId}`.slice(0, 100),
    value: String(s.tokenId),
  }));
  const menu = new StringSelectMenuBuilder()
    .setCustomId('portal_select')
    .setPlaceholder('Select your Squig')
    .addOptions(options);

  const row = new ActionRowBuilder().addComponents(menu);
  const extra = squigs.length > MAX_SELECT_OPTIONS
    ? `\nShowing first ${MAX_SELECT_OPTIONS} eligible Squigs.`
    : '';

  await interaction.editReply({
    content: `Select a Squig to stabilize the portal.${extra}`,
    components: [row],
  });
}

async function resolveDripRecipient(interaction, links, settings) {
  const realmId = settings?.drip_realm_id;
  const firstWallet = links.find((x) => x.wallet_address)?.wallet_address || null;
  const fromLinks = [...new Set(links.map((x) => String(x?.drip_member_id || '').trim()).filter(Boolean))];

  let resolved = null;
  try {
    resolved = await deps.resolveDripMemberForDiscordUser(
      realmId,
      interaction.user.id,
      firstWallet,
      settings
    );
  } catch {
    resolved = null;
  }
  const resolvedCandidates = deps.collectDripMemberIdCandidates(resolved?.member, null);
  // Match claim flow behavior: prefer freshly resolved member IDs, then linked fallbacks.
  return [...new Set([...resolvedCandidates, ...fromLinks])];
}

async function handlePortalSelect(interaction) {
  assertReady();
  if (!portalActive || !currentPortal) {
    await interaction.reply({ content: 'The portal has already closed.', flags: 64 });
    return;
  }

  await interaction.deferReply({ flags: 64 });

  if (claimedUsers.has(interaction.user.id)) {
    await interaction.editReply({ content: 'You already stabilized this portal.' });
    return;
  }

  const selectedTokenId = String(interaction.values?.[0] || '').trim();
  const pending = pendingSelections.get(interaction.user.id);
  if (!pending || !selectedTokenId) {
    await interaction.editReply({ content: 'Portal selection expired. Click STABILIZE PORTAL again.' });
    return;
  }
  const chosenSquig = pending.squigsById.get(selectedTokenId);
  if (!chosenSquig) {
    await interaction.editReply({ content: 'Invalid Squig selection for this portal claim.' });
    return;
  }

  const links = await deps.getWalletLinks(interaction.guild.id, interaction.user.id);
  if (!links.length) {
    await interaction.editReply({ content: 'You must link your wallet to interact with the portal.' });
    return;
  }

  const settings = await deps.getGuildSettings(interaction.guild.id);
  const missing = [];
  if (!settings?.drip_api_key) missing.push('DRIP API Key');
  if (!settings?.drip_realm_id) missing.push('DRIP Realm ID');
  if (!settings?.currency_id) missing.push('Currency ID');
  if (missing.length) {
    await interaction.editReply({ content: `Portal payout unavailable. Missing: ${missing.join(', ')}` });
    return;
  }

  const memberIds = await resolveDripRecipient(interaction, links, settings);
  if (!memberIds.length) {
    await interaction.editReply({ content: 'Portal payout failed: no DRIP member ID found for your account.' });
    return;
  }

  await deps.awardDripPoints(
    settings.drip_realm_id,
    memberIds,
    currentPortal.reward,
    settings.currency_id,
    settings,
    {
      context: 'portal',
      initiatorDiscordId: interaction.user.id,
      recipientDiscordId: interaction.user.id,
      recipientWalletAddress: links.find((x) => x.wallet_address)?.wallet_address || null,
    }
  );

  claimedUsers.add(interaction.user.id);
  pendingSelections.delete(interaction.user.id);

  const traitLine = currentPortal.type === 'dual'
    ? `${currentPortal.traitA.trait} + ${currentPortal.traitB.trait}`
    : currentPortal.traitA.trait;
  const embed = new EmbedBuilder()
    .setTitle('Portal Stabilized')
    .setDescription(
      `<@${interaction.user.id}> deployed Squig #${selectedTokenId}\n\n` +
      `Trait detected:\n` +
      `${traitLine}\n\n` +
      `Reward:\n` +
      `${currentPortal.reward} $CHARM`
    )
    .setColor(0x2ECC71);

  const selectedImageName = attachmentNameForSquig(selectedTokenId, 'portal-claim');
  const safeImageUrl = normalizeImageUrl(chosenSquig.imageUrl);
  if (chosenSquig.imagePath) {
    embed.setImage(`attachment://${selectedImageName}`);
  } else if (safeImageUrl) {
    embed.setImage(safeImageUrl);
  }

  await interaction.channel.send({
    embeds: [embed],
    ...(chosenSquig.imagePath
      ? { files: [new AttachmentBuilder(chosenSquig.imagePath, { name: selectedImageName })] }
      : {}),
  });
  try {
    const receiptChannel = deps.client.channels.cache.get(PORTAL_RECEIPT_CHANNEL_ID)
      || await deps.client.channels.fetch(PORTAL_RECEIPT_CHANNEL_ID);
    if (receiptChannel?.isTextBased()) {
      await receiptChannel.send(
        `<@${interaction.user.id}> was rewarded ${currentPortal.reward} $CHARM for helping stabilize the portal.`
      );
    }
  } catch {}
  await interaction.editReply({ content: `Portal stabilized. Sent ${currentPortal.reward} $CHARM.` });
}

function getPortalState() {
  return {
    portalActive,
    schedulerEnabled,
    portalChannelId,
    portalGuildId,
    claimedCount: claimedUsers.size,
    currentPortal,
    nextPortalAt,
    nextPortalDelayOverrideMs,
  };
}

module.exports = {
  initPortalEvent,
  startPortalScheduler,
  stopPortalScheduler,
  triggerPortalEvent,
  setNextPortalTriggerDelayMinutes,
  handlePortalClaim,
  handlePortalSelect,
  getPortalState,
};
