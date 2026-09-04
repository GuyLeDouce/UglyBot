const fetch = require('node-fetch');
const { EmbedBuilder } = require('discord.js');
const core = require('./bountyVaultCore');

const OPENSEA_CHAIN_SLUGS = Object.freeze({
  ethereum: 'ethereum',
  eth: 'ethereum',
  base: 'base',
  apechain: 'ape_chain',
  ape_chain: 'ape_chain',
  robinhood: 'robinhood',
  robinhood_chain: 'robinhood',
});

let injectedDeps = null;
let originalGetNftImageUrl = null;
let imageBackfillTimer = null;
let imageBackfillRunning = false;

function bountyChainKey(value) {
  const key = String(value || 'ethereum').trim().toLowerCase();
  if (key === 'eth') return 'ethereum';
  if (key === 'ape_chain') return 'apechain';
  if (key === 'robinhood_chain') return 'robinhood';
  return key;
}

function openSeaChainSlug(chain) {
  return OPENSEA_CHAIN_SLUGS[String(chain || 'ethereum').trim().toLowerCase()] || null;
}

function normalizeImageUrl(value) {
  const text = String(value || '').trim();
  if (!text) return null;
  if (/^https?:\/\//i.test(text)) return text;
  if (/^ipfs:\/\//i.test(text)) return `https://ipfs.io/ipfs/${text.replace(/^ipfs:\/\/(?:ipfs\/)?/i, '')}`;
  if (/^ar:\/\//i.test(text)) return `https://arweave.net/${text.replace(/^ar:\/\//i, '')}`;
  return null;
}

function extractOpenSeaImage(payload) {
  const nft = payload?.nft || payload?.item || payload || {};
  const candidates = [
    nft.image_url,
    nft.display_image_url,
    nft.image_original_url,
    nft.image,
    nft.imageUrl,
    nft?.metadata?.image,
    payload?.image_url,
    payload?.display_image_url,
  ];
  for (const candidate of candidates) {
    const normalized = normalizeImageUrl(candidate);
    if (normalized) return normalized;
  }
  return null;
}

function decodeHtml(value) {
  return String(value || '')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>');
}

function extractPagePreviewImage(html) {
  const text = String(html || '');
  const patterns = [
    /<meta[^>]+property=["']og:image(?::secure_url)?["'][^>]+content=["']([^"']+)["'][^>]*>/i,
    /<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:image(?::secure_url)?["'][^>]*>/i,
    /<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"']+)["'][^>]*>/i,
    /<meta[^>]+content=["']([^"']+)["'][^>]+name=["']twitter:image["'][^>]*>/i,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    const normalized = normalizeImageUrl(decodeHtml(match?.[1]));
    if (normalized) return normalized;
  }
  return null;
}

async function fetchOpenSeaApiImage(tokenId, contractAddress, chain) {
  const apiKey = String(process.env.OPENSEA_API_KEY || '').trim();
  const slug = openSeaChainSlug(chain);
  const contract = String(contractAddress || '').trim().toLowerCase();
  const token = String(tokenId || '').trim();
  if (!apiKey || !slug || !/^0x[0-9a-f]{40}$/.test(contract) || !/^\d+$/.test(token)) return null;

  const url = `https://api.opensea.io/api/v2/chain/${encodeURIComponent(slug)}/contract/${contract}/nfts/${encodeURIComponent(token)}`;
  const res = await fetch(url, {
    headers: { accept: 'application/json', 'x-api-key': apiKey },
    timeout: 8000,
  });
  if (!res.ok) return null;
  return extractOpenSeaImage(await res.json());
}

async function fetchOpenSeaPageImage(openseaUrl, tokenId, contractAddress, chain) {
  const slug = openSeaChainSlug(chain);
  const contract = String(contractAddress || '').trim().toLowerCase();
  const token = String(tokenId || '').trim();
  const fallbackUrl = slug && /^0x[0-9a-f]{40}$/.test(contract) && /^\d+$/.test(token)
    ? `https://opensea.io/item/${slug}/${contract}/${token}`
    : null;
  const url = String(openseaUrl || fallbackUrl || '').trim();
  if (!/^https:\/\/(?:www\.)?opensea\.io\//i.test(url)) return null;

  const res = await fetch(url, {
    headers: {
      accept: 'text/html,application/xhtml+xml',
      'user-agent': 'Mozilla/5.0 (compatible; UglyBot/1.0; +https://squigs.io)',
    },
    timeout: 8000,
  });
  if (!res.ok) return null;
  return extractPagePreviewImage(await res.text());
}

async function resolveBountyImage(tokenId, contractAddress, chain = 'ethereum', options = {}) {
  const chainKey = bountyChainKey(chain);
  if (typeof originalGetNftImageUrl === 'function') {
    try {
      const existing = normalizeImageUrl(await originalGetNftImageUrl(tokenId, contractAddress, chainKey, options));
      if (existing) return existing;
    } catch (error) {
      if (options?.logFailures) console.warn('[Bounty Vault] Shared NFT image lookup failed:', String(error?.message || error));
    }
  }

  try {
    const apiImage = await fetchOpenSeaApiImage(tokenId, contractAddress, chainKey);
    if (apiImage) return apiImage;
  } catch (error) {
    if (options?.logFailures) console.warn('[Bounty Vault] OpenSea API image lookup failed:', String(error?.message || error));
  }

  try {
    return await fetchOpenSeaPageImage(options?.openseaUrl, tokenId, contractAddress, chainKey);
  } catch (error) {
    if (options?.logFailures) console.warn('[Bounty Vault] OpenSea page image lookup failed:', String(error?.message || error));
    return null;
  }
}

function bountyPool() {
  const pool = injectedDeps?.bountyPool || injectedDeps?.prizesPool;
  return pool?.query ? pool : null;
}

async function editStoredEmbed(channelId, messageId, imageUrl) {
  if (!channelId || !messageId || !imageUrl || !injectedDeps?.client?.channels?.fetch) return false;
  const ch = await injectedDeps.client.channels.fetch(String(channelId)).catch(() => null);
  const message = await ch?.messages?.fetch(String(messageId)).catch(() => null);
  if (!message?.embeds?.[0]) return false;
  const embed = EmbedBuilder.from(message.embeds[0]).setImage(imageUrl);
  await message.edit({ embeds: [embed] });
  return true;
}

async function backfillMissingBountyImages() {
  if (imageBackfillRunning) return 0;
  const pool = bountyPool();
  if (!pool) return 0;
  imageBackfillRunning = true;
  let updated = 0;
  try {
    const rows = (await pool.query(`
      SELECT id, chain, contract_address, token_id, opensea_url, image_url,
             team_review_channel_id, team_review_message_id, vote_channel_id, vote_message_id
      FROM bounty_submissions
      WHERE image_url IS NULL
        AND contract_address IS NOT NULL
        AND token_id IS NOT NULL
        AND status IN ('team_review','community_vote','vaulted','return_pending','drawn_pending_delivery')
      ORDER BY updated_at DESC
      LIMIT 30
    `)).rows;

    for (const row of rows) {
      const imageUrl = await resolveBountyImage(row.token_id, row.contract_address, row.chain, {
        openseaUrl: row.opensea_url,
        logFailures: true,
      });
      if (!imageUrl) continue;
      const result = await pool.query(
        `UPDATE bounty_submissions SET image_url=$2,updated_at=NOW() WHERE id=$1 AND image_url IS NULL RETURNING id`,
        [row.id, imageUrl]
      );
      if (!result.rowCount) continue;
      updated++;
      await editStoredEmbed(row.team_review_channel_id, row.team_review_message_id, imageUrl).catch(() => null);
      await editStoredEmbed(row.vote_channel_id, row.vote_message_id, imageUrl).catch(() => null);
    }
  } finally {
    imageBackfillRunning = false;
  }
  return updated;
}

function startImageBackfill() {
  if (imageBackfillTimer) return;
  const first = setTimeout(() => {
    backfillMissingBountyImages().catch(error => console.warn('[Bounty Vault] Image backfill failed:', String(error?.message || error)));
  }, 2500);
  first.unref?.();
  imageBackfillTimer = setInterval(() => {
    backfillMissingBountyImages().catch(error => console.warn('[Bounty Vault] Image backfill failed:', String(error?.message || error)));
  }, 5 * 60 * 1000);
  imageBackfillTimer.unref?.();
}

function initBountyVault(injected = {}) {
  injectedDeps = injected || {};
  originalGetNftImageUrl = injectedDeps.getNftImageUrl;
  core.initBountyVault({
    ...injectedDeps,
    getNftImageUrl: (tokenId, contractAddress, chain, options = {}) =>
      resolveBountyImage(tokenId, contractAddress, chain, options),
  });
  startImageBackfill();
}

module.exports = {
  ...core,
  initBountyVault,
  openSeaChainSlug,
  normalizeImageUrl,
  extractOpenSeaImage,
  extractPagePreviewImage,
  resolveBountyImage,
  backfillMissingBountyImages,
};
