// scripts/buildTraitCounts.js
require('dotenv').config();
const fs = require('fs');
const fetch = require('node-fetch');

const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY;
const CONTRACT = '0x9bf567ddf41b425264626d1b8b2c7f7c660b1c42'; // Squigs
const BASE = `https://eth-mainnet.g.alchemy.com/nft/v3/${ALCHEMY_API_KEY}`;

async function fetchAll() {
  let pageKey = null;
  const counts = {}; // { TraitType: { value: n } }

  do {
    const url = new URL(`${BASE}/getNFTsForContract`);
    url.searchParams.set('contractAddress', CONTRACT);
    url.searchParams.set('withMetadata', 'true');
    url.searchParams.set('pageSize', '100');
    if (pageKey) url.searchParams.set('pageKey', pageKey);

    const res = await fetch(url.toString());
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const json = await res.json();

    for (const nft of json.nfts || []) {
      let attrs = [];
      if (Array.isArray(nft?.metadata?.attributes)) {
        attrs = nft.metadata.attributes;
      } else if (typeof nft?.raw?.metadata === 'string') {
        try {
          const parsed = JSON.parse(nft.raw.metadata);
          if (Array.isArray(parsed?.attributes)) attrs = parsed.attributes;
        } catch {}
      } else if (Array.isArray(nft?.raw?.metadata?.attributes)) {
        attrs = nft.raw.metadata.attributes;
      }

      for (const a of attrs) {
        const t = String(a?.trait_type || '').trim();
        const v = String(a?.value ?? '');
        if (!t || !v) continue;
        counts[t] = counts[t] || {};
        counts[t][v] = (counts[t][v] || 0) + 1;
      }
    }

    pageKey = json.pageKey || null;
    console.log('…fetched batch, next pageKey =', pageKey);
  } while (pageKey);

  return counts;
}

(async () => {
  const counts = await fetchAll();
  fs.writeFileSync('trait_counts.json', JSON.stringify(counts, null, 2));
  console.log('✅ Wrote trait_counts.json');
})();
