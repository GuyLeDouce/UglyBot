const {
  EmbedBuilder,
  ButtonBuilder,
  ButtonStyle,
  ActionRowBuilder,
  StringSelectMenuBuilder,
} = require('discord.js');

const ONE_HOUR_MS = 60 * 60 * 1000;
const MIN_DELAY_MS = 6 * 60 * 60 * 1000;
const MAX_DELAY_MS = 12 * 60 * 60 * 1000;
const SINGLE_TRAIT_CHANCE = 0.85;
const MAX_SELECT_OPTIONS = 25;
const SQUIGS_CONTRACT = '0x9bf567ddf41b425264626d1b8b2c7f7c660b1c42';
const DEFAULT_MAX_TOKEN_ID = Number(process.env.PORTAL_MAX_SQUIG_TOKEN_ID || 10000);
const SQUIGS_MINT_URL = 'https://squigs.io/';
const SQUIGS_OPENSEA_URL = 'https://opensea.io/collection/squigsnft';

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
  const randomTries = Math.max(25, Number(process.env.PORTAL_PREVIEW_SCAN_ATTEMPTS || 120));
  const linearTries = Math.max(100, Number(process.env.PORTAL_PREVIEW_LINEAR_SCAN_ATTEMPTS || 450));
  const candidates = [];

  const inspectToken = async (tokenId) => {
    try {
      const meta = await deps.getNftMetadataAlchemy(tokenId, SQUIGS_CONTRACT);
      const { attrs } = await deps.getTraitsForToken(meta, tokenId, SQUIGS_CONTRACT);
      const traitValues = traitValuesFromAttrs(attrs);
      if (!hasRequiredTraits(traitValues, requiredTraitA, requiredTraitB)) return;
      const imageUrl = normalizeImageUrl(
        meta?.image ||
        meta?.metadata?.image ||
        meta?.raw?.metadata?.image ||
        meta?.tokenUri?.gateway ||
        meta?.tokenUri?.raw
      );
      const name = String(meta?.name || `Squig #${tokenId}`);
      candidates.push({ tokenId, imageUrl, name });
    } catch {}
  };

  for (let i = 0; i < randomTries; i++) {
    const tokenId = String(randomInt(1, DEFAULT_MAX_TOKEN_ID));
    await inspectToken(tokenId);
    if (candidates.length >= 3) break;
  }

  // Fallback: linear probe from a random start for better hit-rate on rare trait combos.
  if (!candidates.length) {
    const start = randomInt(1, DEFAULT_MAX_TOKEN_ID);
    for (let i = 0; i < linearTries; i++) {
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

  if (preview?.imageUrl) {
    embed.setImage(preview.imageUrl);
    embed.addFields({
      name: 'Example Squig Match',
      value: `Squig #${preview.tokenId}\nhttps://opensea.io/assets/ethereum/${SQUIGS_CONTRACT}/${preview.tokenId}`,
    });
  }
  return embed;
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
    `Keep collecting:\n` +
    `Mint: ${SQUIGS_MINT_URL}\n` +
    `OpenSea: ${SQUIGS_OPENSEA_URL}`
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

function schedulePortal() {
  if (!schedulerEnabled) return;
  const delay = Math.floor(Math.random() * (MAX_DELAY_MS - MIN_DELAY_MS + 1)) + MIN_DELAY_MS;
  portalTimeout = setTimeout(() => {
    triggerPortalEvent().catch((err) => {
      console.error('Portal trigger error:', err);
      schedulePortal();
    });
  }, delay);
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

  if (portalTimeout) {
    clearTimeout(portalTimeout);
    portalTimeout = null;
  }

  if (options.channelId) portalChannelId = String(options.channelId);
  if (options.guildId) portalGuildId = String(options.guildId);

  const channel = await resolvePortalChannel();
  if (!channel || !channel.isTextBased()) {
    schedulePortal();
    return { ok: false, reason: 'Portal channel not found or not text-based.' };
  }

  const guildPointMappings = await deps.getGuildPointMappings(channel.guild.id);
  const squigTable = deps.hpTableForContract(SQUIGS_CONTRACT, guildPointMappings);
  const traitPool = flattenTraitTable(squigTable);
  const selected = choosePortalTraits(traitPool);
  if (!selected) {
    schedulePortal();
    return { ok: false, reason: 'No valid traits available for portal event.' };
  }

  const preview = await findSquigForPreview(
    selected.traitA.trait,
    selected.type === 'dual' ? selected.traitB.trait : null
  );

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

  const portalMessage = await channel.send({
    content: '@everyone  PORTAL MALFUNCTION',
    embeds: [buildPortalEmbed(currentPortal, preview)],
    components: [buildPortalButtonRow(false)],
  });
  portalMessageRef = portalMessage;

  portalCloseTimeout = setTimeout(() => {
    closePortal({ announce: true }).catch((err) => console.error('Portal close error:', err));
  }, ONE_HOUR_MS);

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

  if (closeActivePortal && portalActive) {
    await closePortal({ announce: true });
  } else if (portalCloseTimeout) {
    clearTimeout(portalCloseTimeout);
    portalCloseTimeout = null;
  }
  return { ok: true, schedulerEnabled, portalActive };
}

async function fetchEligibleSquigsForUser(links) {
  const walletAddresses = links.map((x) => x.wallet_address).filter(Boolean);
  if (!walletAddresses.length) return [];

  const tokenIds = await deps.getOwnedTokenIdsForContractMany(walletAddresses, SQUIGS_CONTRACT);
  if (!tokenIds.length) return [];

  const squigs = [];
  for (const tokenId of tokenIds) {
    try {
      const meta = await deps.getNftMetadataAlchemy(tokenId, SQUIGS_CONTRACT);
      const { attrs } = await deps.getTraitsForToken(meta, tokenId, SQUIGS_CONTRACT);
      const traits = traitValuesFromAttrs(attrs);
      const valid = hasRequiredTraits(
        traits,
        currentPortal?.traitA?.trait,
        currentPortal?.type === 'dual' ? currentPortal?.traitB?.trait : null
      );
      if (!valid) continue;
      const imageUrl = normalizeImageUrl(
        meta?.image ||
        meta?.metadata?.image ||
        meta?.raw?.metadata?.image ||
        meta?.tokenUri?.gateway ||
        meta?.tokenUri?.raw
      );
      squigs.push({ tokenId: String(tokenId), traits, imageUrl });
    } catch {}
  }

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

  const squigs = await fetchEligibleSquigsForUser(links);
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
    settings
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

  const safeImageUrl = normalizeImageUrl(chosenSquig.imageUrl);
  if (safeImageUrl) embed.setImage(safeImageUrl);

  await interaction.channel.send({ embeds: [embed] });
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
  };
}

module.exports = {
  initPortalEvent,
  startPortalScheduler,
  stopPortalScheduler,
  triggerPortalEvent,
  handlePortalClaim,
  handlePortalSelect,
  getPortalState,
};
