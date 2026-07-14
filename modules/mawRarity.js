const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const MAW_EXPECTED_TOKEN_COUNT = 4444;
const MAW_EXPECTED_LEGENDARY_COUNT = 31;
const MAW_LEGENDARY_SHARED_RANK = 1;
const MAW_FIRST_NON_LEGENDARY_RANK = MAW_EXPECTED_LEGENDARY_COUNT + 1;
const MAW_REWARD_RULES_VERSION = 'rarity_v1';

const MAW_RARITY_RULES = Object.freeze([
  Object.freeze({
    key: 'legendary',
    label: 'Legendary',
    maxExclusive: 32,
    payoutCharm: 800000,
    ticketCount: 20,
    jackpotContributionCharm: 100000,
  }),
  Object.freeze({
    key: 'epic',
    label: 'Epic',
    minInclusive: 32,
    maxExclusive: 444,
    payoutCharm: 187500,
    ticketCount: 10,
    jackpotContributionCharm: 25000,
  }),
  Object.freeze({
    key: 'rare',
    label: 'Rare',
    minInclusive: 444,
    maxExclusive: 1111,
    payoutCharm: 67500,
    ticketCount: 5,
    jackpotContributionCharm: 5000,
  }),
  Object.freeze({
    key: 'uncommon',
    label: 'Uncommon',
    minInclusive: 1111,
    maxExclusive: 2277,
    payoutCharm: 22500,
    ticketCount: 2,
    jackpotContributionCharm: 2000,
  }),
  Object.freeze({
    key: 'common',
    label: 'Common',
    minInclusive: 2277,
    payoutCharm: 15000,
    ticketCount: 1,
    jackpotContributionCharm: 1000,
  }),
]);

const REQUIRED_HEADERS = Object.freeze({
  tokenId: 'Token ID',
  totalUglyPoints: 'Total UglyPoints',
});

const OPTIONAL_HEADERS = Object.freeze({
  legend: 'Legend',
  overallRank: 'Overall Rank',
  collectionRank: 'Collection Rank',
});

const DEFAULT_MAW_RANKING_CSV_PATH = path.join(__dirname, '..', 'Squigs_Reloaded_Token_UglyPoints.csv');

let cachedRankingIndex = null;

function stripBomAndTrim(value) {
  return String(value ?? '').replace(/^\uFEFF/, '').trim();
}

function normalizeMawRankingHeader(value) {
  return stripBomAndTrim(value).toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function normalizeMawTokenId(value) {
  const text = stripBomAndTrim(value);
  if (!/^\d+$/.test(text)) {
    const err = new Error(`Invalid Squig token ID: ${text || '(blank)'}`);
    err.code = 'MAW_RANKING_INVALID_TOKEN';
    throw err;
  }
  const parsed = BigInt(text);
  if (parsed <= 0n) {
    const err = new Error(`Invalid Squig token ID: ${text}`);
    err.code = 'MAW_RANKING_INVALID_TOKEN';
    throw err;
  }
  return parsed.toString(10);
}

function parsePositiveRank(value, label, tokenId = null) {
  const text = stripBomAndTrim(value);
  if (!text) {
    const err = new Error(`${label} is missing${tokenId ? ` for Squig #${tokenId}` : ''}.`);
    err.code = 'MAW_RANKING_MISSING_RANK';
    throw err;
  }
  if (!/^\d+(?:\.\d+)?$/.test(text)) {
    const err = new Error(`${label} is not numeric${tokenId ? ` for Squig #${tokenId}` : ''}: ${text}`);
    err.code = 'MAW_RANKING_INVALID_RANK';
    throw err;
  }
  const parsed = Number(text);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    const err = new Error(`${label} must be greater than zero${tokenId ? ` for Squig #${tokenId}` : ''}.`);
    err.code = 'MAW_RANKING_INVALID_RANK';
    throw err;
  }
  return parsed;
}

function parseOptionalPositiveRank(value, label, tokenId = null) {
  const text = stripBomAndTrim(value);
  if (!text) return null;
  return parsePositiveRank(text, label, tokenId);
}

function parseNonNegativeScore(value, label, tokenId = null) {
  const text = stripBomAndTrim(value);
  if (!text) {
    const err = new Error(`${label} is missing${tokenId ? ` for Squig #${tokenId}` : ''}.`);
    err.code = 'MAW_RANKING_MISSING_SCORE';
    throw err;
  }
  if (!/^\d+(?:\.\d+)?$/.test(text)) {
    const err = new Error(`${label} is not numeric${tokenId ? ` for Squig #${tokenId}` : ''}: ${text}`);
    err.code = 'MAW_RANKING_INVALID_SCORE';
    throw err;
  }
  const parsed = Number(text);
  if (!Number.isFinite(parsed) || parsed < 0) {
    const err = new Error(`${label} must be zero or greater${tokenId ? ` for Squig #${tokenId}` : ''}.`);
    err.code = 'MAW_RANKING_INVALID_SCORE';
    throw err;
  }
  return parsed;
}

function parseCsvRecords(csvText) {
  const text = String(csvText ?? '');
  const rows = [];
  let row = [];
  let field = '';
  let quoted = false;

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (quoted) {
      if (ch === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i += 1;
        } else {
          quoted = false;
        }
      } else {
        field += ch;
      }
      continue;
    }

    if (ch === '"') {
      quoted = true;
    } else if (ch === ',') {
      row.push(field);
      field = '';
    } else if (ch === '\n') {
      row.push(field);
      rows.push(row);
      row = [];
      field = '';
    } else if (ch === '\r') {
      if (text[i + 1] === '\n') continue;
      row.push(field);
      rows.push(row);
      row = [];
      field = '';
    } else {
      field += ch;
    }
  }

  if (quoted) {
    const err = new Error('CSV ended while inside a quoted field.');
    err.code = 'MAW_RANKING_INVALID_CSV';
    throw err;
  }
  if (field.length || row.length) {
    row.push(field);
    rows.push(row);
  }

  const nonEmptyRows = rows.filter((cells) => cells.some((cell) => stripBomAndTrim(cell) !== ''));
  if (!nonEmptyRows.length) {
    const err = new Error('Ranking CSV is empty.');
    err.code = 'MAW_RANKING_INVALID_CSV';
    throw err;
  }

  const headers = nonEmptyRows[0].map(stripBomAndTrim);
  const records = nonEmptyRows.slice(1).map((cells, index) => {
    if (cells.length !== headers.length) {
      const err = new Error(`CSV row ${index + 2} has ${cells.length} fields; expected ${headers.length}.`);
      err.code = 'MAW_RANKING_INVALID_CSV';
      throw err;
    }
    const record = {};
    for (let i = 0; i < headers.length; i += 1) {
      record[headers[i]] = stripBomAndTrim(cells[i]);
    }
    return record;
  });

  return { headers, rows: records };
}

function resolveRequiredHeaderMap(rows) {
  const headers = Object.keys(rows[0] || {});
  const normalized = new Map(headers.map((header) => [normalizeMawRankingHeader(header), header]));
  const required = {};
  for (const [key, label] of Object.entries(REQUIRED_HEADERS)) {
    const actual = normalized.get(normalizeMawRankingHeader(label));
    if (!actual) {
      const err = new Error(`Ranking CSV is missing required header "${label}".`);
      err.code = 'MAW_RANKING_MISSING_HEADER';
      throw err;
    }
    required[key] = actual;
  }
  for (const [key, label] of Object.entries(OPTIONAL_HEADERS)) {
    const actual = normalized.get(normalizeMawRankingHeader(label));
    if (actual) required[key] = actual;
  }
  return required;
}

function calculateMawAverageRank(overallRank, collectionRank) {
  const overall = Number(overallRank);
  const collection = Number(collectionRank);
  if (!Number.isFinite(overall) || !Number.isFinite(collection)) {
    const err = new Error('Maw average rank requires numeric rank values.');
    err.code = 'MAW_RANKING_INVALID_RANK';
    throw err;
  }
  return (overall + collection) / 2;
}

function classifyMawRarity(averageRank) {
  const rank = Number(averageRank);
  if (!Number.isFinite(rank) || rank <= 0) {
    const err = new Error(`Invalid Maw average rank: ${averageRank}`);
    err.code = 'MAW_RANKING_INVALID_AVERAGE';
    throw err;
  }
  const rule = MAW_RARITY_RULES.find((candidate) => {
    const aboveMin = candidate.minInclusive == null || rank >= candidate.minInclusive;
    const belowMax = candidate.maxExclusive == null || rank < candidate.maxExclusive;
    return aboveMin && belowMax;
  });
  if (!rule) {
    const err = new Error(`No Maw rarity rule matched average rank ${rank}.`);
    err.code = 'MAW_RANKING_NO_RULE';
    throw err;
  }
  return rule;
}

function formatMawAverageRank(averageRank) {
  const rank = Number(averageRank);
  if (!Number.isFinite(rank)) return String(averageRank ?? '');
  return rank.toFixed(2).replace(/\.?0+$/, '');
}

function formatMawRarityLabel(rarityKey) {
  const key = String(rarityKey || '').trim().toLowerCase();
  const rule = MAW_RARITY_RULES.find((candidate) => candidate.key === key);
  return rule?.label || (key ? key.replace(/^\w/, (ch) => ch.toUpperCase()) : 'Legacy');
}

function validateMawRankingRows(rows, options = {}) {
  const expectedTokenCount = Math.max(1, Math.floor(Number(options.expectedTokenCount || MAW_EXPECTED_TOKEN_COUNT)));
  const requireCompleteCollection = options.requireCompleteCollection !== false;
  const expectedLegendaryCount = options.expectedLegendaryCount == null
    ? (requireCompleteCollection && expectedTokenCount === MAW_EXPECTED_TOKEN_COUNT ? MAW_EXPECTED_LEGENDARY_COUNT : null)
    : Math.max(0, Math.floor(Number(options.expectedLegendaryCount)));
  if (!Array.isArray(rows) || !rows.length) {
    const err = new Error('Ranking CSV contains no Squig rows.');
    err.code = 'MAW_RANKING_INVALID_CSV';
    throw err;
  }

  const headerMap = resolveRequiredHeaderMap(rows);
  const tokenMap = new Map();
  const rawRows = [];
  const normalizedRows = [];
  const tierDistribution = Object.fromEntries(MAW_RARITY_RULES.map((rule) => [rule.key, 0]));

  rows.forEach((row, index) => {
    const tokenId = normalizeMawTokenId(row[headerMap.tokenId]);
    if (tokenMap.has(tokenId)) {
      const err = new Error(`Duplicate Squig token ID in ranking CSV: #${tokenId}`);
      err.code = 'MAW_RANKING_DUPLICATE_TOKEN';
      throw err;
    }
    const numericTokenId = Number(tokenId);
    if (requireCompleteCollection && (numericTokenId < 1 || numericTokenId > expectedTokenCount)) {
      const err = new Error(`Squig token ID #${tokenId} is outside expected range 1-${expectedTokenCount}.`);
      err.code = 'MAW_RANKING_UNEXPECTED_TOKEN';
      throw err;
    }
    const totalUglyPoints = parseNonNegativeScore(row[headerMap.totalUglyPoints], REQUIRED_HEADERS.totalUglyPoints, tokenId);
    const legend = headerMap.legend ? stripBomAndTrim(row[headerMap.legend]) : '';
    const sourceOverallRank = headerMap.overallRank
      ? parseOptionalPositiveRank(row[headerMap.overallRank], OPTIONAL_HEADERS.overallRank, tokenId)
      : null;
    const sourceCollectionRank = headerMap.collectionRank
      ? parseOptionalPositiveRank(row[headerMap.collectionRank], OPTIONAL_HEADERS.collectionRank, tokenId)
      : null;
    const raw = Object.freeze({
      rowNumber: index + 2,
      tokenId,
      numericTokenId,
      totalUglyPoints,
      legend,
      isLegendary: legend !== '',
      sourceOverallRank,
      sourceCollectionRank,
    });
    rawRows.push(raw);
    tokenMap.set(tokenId, raw);
  });

  if (requireCompleteCollection) {
    for (let tokenId = 1; tokenId <= expectedTokenCount; tokenId += 1) {
      if (!tokenMap.has(String(tokenId))) {
        const err = new Error(`Ranking CSV is missing Squig #${tokenId}.`);
        err.code = 'MAW_RANKING_MISSING_TOKEN';
        throw err;
      }
    }
    if (tokenMap.size !== expectedTokenCount) {
      const err = new Error(`Ranking CSV contains ${tokenMap.size} unique Squigs; expected ${expectedTokenCount}.`);
      err.code = 'MAW_RANKING_TOKEN_COUNT';
      throw err;
    }
  }

  const legendaryRows = rawRows.filter((row) => row.isLegendary);
  if (expectedLegendaryCount != null && legendaryRows.length !== expectedLegendaryCount) {
    const err = new Error(`Ranking CSV contains ${legendaryRows.length} Legendary Squigs; expected ${expectedLegendaryCount}.`);
    err.code = 'MAW_RANKING_LEGENDARY_COUNT';
    throw err;
  }

  const rankByTokenId = new Map();
  for (const row of legendaryRows) {
    rankByTokenId.set(row.tokenId, MAW_LEGENDARY_SHARED_RANK);
  }
  const nonLegendaryRows = rawRows
    .filter((row) => !row.isLegendary)
    .sort((a, b) => {
      if (b.totalUglyPoints !== a.totalUglyPoints) return b.totalUglyPoints - a.totalUglyPoints;
      return a.numericTokenId - b.numericTokenId;
    });
  nonLegendaryRows.forEach((row, index) => {
    rankByTokenId.set(row.tokenId, MAW_FIRST_NON_LEGENDARY_RANK + index);
  });

  tokenMap.clear();
  for (const row of rawRows) {
    const mawRank = rankByTokenId.get(row.tokenId);
    const rarityRule = classifyMawRarity(mawRank);
    const normalized = Object.freeze({
      rowNumber: row.rowNumber,
      tokenId: row.tokenId,
      totalUglyPoints: row.totalUglyPoints,
      legend: row.legend,
      isLegendary: row.isLegendary,
      overallRank: null,
      collectionRank: null,
      sourceOverallRank: row.sourceOverallRank,
      sourceCollectionRank: row.sourceCollectionRank,
      mawRank,
      averageRank: mawRank,
      rarityKey: rarityRule.key,
      rarityLabel: rarityRule.label,
    });
    tokenMap.set(row.tokenId, normalized);
    normalizedRows.push(normalized);
    tierDistribution[rarityRule.key] = (tierDistribution[rarityRule.key] || 0) + 1;
  }

  return Object.freeze({
    rows: Object.freeze(normalizedRows),
    tokenMap,
    tierDistribution: Object.freeze({ ...tierDistribution }),
    expectedTokenCount,
  });
}

function parseMawRankingCsvContent(contents, options = {}) {
  const buffer = Buffer.isBuffer(contents) ? contents : Buffer.from(String(contents ?? ''), 'utf8');
  const rankingSourceHash = `sha256:${crypto.createHash('sha256').update(buffer).digest('hex')}`;
  const parsed = parseCsvRecords(buffer.toString('utf8'));
  const validated = validateMawRankingRows(parsed.rows, options);
  return Object.freeze({
    ...validated,
    headers: Object.freeze(parsed.headers),
    rankingSourceHash,
  });
}

function loadMawRankingIndex(options = {}) {
  const csvPath = path.resolve(
    options.csvPath ||
    process.env.MAW_RANKING_CSV_PATH ||
    DEFAULT_MAW_RANKING_CSV_PATH
  );
  const useCache = options.cache !== false;
  if (useCache && cachedRankingIndex && cachedRankingIndex.csvPath === csvPath && !options.forceReload) {
    return cachedRankingIndex;
  }

  let contents;
  try {
    contents = fs.readFileSync(csvPath);
  } catch (err) {
    const wrapped = new Error(`Maw ranking CSV could not be read at ${csvPath}: ${err.message}`);
    wrapped.code = 'MAW_RANKING_LOAD_FAILED';
    throw wrapped;
  }

  const parsed = parseMawRankingCsvContent(contents, options);
  const index = Object.freeze({
    csvPath,
    rows: parsed.rows,
    tokenMap: parsed.tokenMap,
    tierDistribution: parsed.tierDistribution,
    expectedTokenCount: parsed.expectedTokenCount,
    rankingSourceHash: parsed.rankingSourceHash,
    headers: parsed.headers,
    loadedAt: new Date().toISOString(),
  });
  if (useCache) cachedRankingIndex = index;
  return index;
}

function getMawRewardQuote(tokenId, options = {}) {
  const normalizedTokenId = normalizeMawTokenId(tokenId);
  const index = options.index || loadMawRankingIndex(options);
  const ranking = index.tokenMap.get(normalizedTokenId);
  if (!ranking) {
    const err = new Error(`No valid Maw ranking found for Squig #${normalizedTokenId}.`);
    err.code = 'MAW_RANKING_MISSING_TOKEN';
    throw err;
  }
  const rule = classifyMawRarity(ranking.averageRank);
  return Object.freeze({
    tokenId: normalizedTokenId,
    overallRank: ranking.overallRank,
    collectionRank: ranking.collectionRank,
    sourceOverallRank: ranking.sourceOverallRank,
    sourceCollectionRank: ranking.sourceCollectionRank,
    totalUglyPoints: ranking.totalUglyPoints,
    legend: ranking.legend,
    isLegendary: ranking.isLegendary,
    mawRank: ranking.mawRank,
    averageRank: ranking.averageRank,
    rarityKey: rule.key,
    rarityLabel: rule.label,
    payoutCharm: rule.payoutCharm,
    ticketCount: rule.ticketCount,
    jackpotContributionCharm: rule.jackpotContributionCharm,
    rewardRulesVersion: MAW_REWARD_RULES_VERSION,
    rankingSourceHash: index.rankingSourceHash,
  });
}

function optionalNumber(value) {
  if (value == null || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function snapshotMawRewardQuote(quote) {
  if (!quote) return null;
  return Object.freeze({
    tokenId: normalizeMawTokenId(quote.tokenId),
    overallRank: optionalNumber(quote.overallRank),
    collectionRank: optionalNumber(quote.collectionRank),
    sourceOverallRank: optionalNumber(quote.sourceOverallRank),
    sourceCollectionRank: optionalNumber(quote.sourceCollectionRank),
    totalUglyPoints: optionalNumber(quote.totalUglyPoints),
    legend: String(quote.legend || '').trim(),
    isLegendary: Boolean(quote.isLegendary),
    mawRank: Number(quote.mawRank ?? quote.averageRank),
    averageRank: Number(quote.averageRank),
    rarityKey: String(quote.rarityKey || '').trim().toLowerCase(),
    rarityLabel: String(quote.rarityLabel || formatMawRarityLabel(quote.rarityKey)).trim(),
    payoutCharm: Math.floor(Number(quote.payoutCharm) || 0),
    ticketCount: Math.max(1, Math.floor(Number(quote.ticketCount) || 1)),
    jackpotContributionCharm: Math.max(0, Math.floor(Number(quote.jackpotContributionCharm) || 0)),
    rewardRulesVersion: String(quote.rewardRulesVersion || MAW_REWARD_RULES_VERSION),
    rankingSourceHash: String(quote.rankingSourceHash || ''),
  });
}

function resolveMawSessionRewardSnapshot(session = {}, event = {}) {
  const rarityKey = String(session.rarity_tier || session.rarityKey || '').trim().toLowerCase();
  const payoutSource = session.payout_amount ?? session.payoutCharm ?? event.return_reward_charm ?? 0;
  const ticketSource = session.ticket_count ?? session.ticketCount ?? 1;
  const contributionSource = session.jackpot_contribution_charm ?? session.jackpotContributionCharm ?? 0;
  const ticketCount = Math.max(1, Math.floor(Number(ticketSource) || 1));
  return Object.freeze({
    payoutCharm: Math.max(0, Math.floor(Number(payoutSource) || 0)),
    ticketCount,
    jackpotContributionCharm: Math.max(0, Math.floor(Number(contributionSource) || 0)),
    rarityKey: rarityKey || null,
    rarityLabel: rarityKey ? formatMawRarityLabel(rarityKey) : 'Legacy',
    rewardRulesVersion: session.reward_rules_version || session.rewardRulesVersion || null,
    rankingSourceHash: session.ranking_source_hash || session.rankingSourceHash || null,
    payoutContext: rarityKey ? `maw_return_reward_${rarityKey}` : 'maw_return_reward',
    isLegacyFlat: !rarityKey,
  });
}

function pluralizeMawTickets(count) {
  const n = Math.max(0, Math.floor(Number(count) || 0));
  return `${n} ticket${n === 1 ? '' : 's'}`;
}

function formatMawTicketRange(firstTicketNumber, lastTicketNumber = firstTicketNumber, ticketCount = null) {
  const first = Math.floor(Number(firstTicketNumber) || 0);
  const last = Math.floor(Number(lastTicketNumber) || first);
  const count = ticketCount == null ? Math.max(1, last - first + 1) : Math.max(1, Math.floor(Number(ticketCount) || 1));
  if (count <= 1 || first === last) return `#${first}`;
  return `#${first}\u2013#${last} (${count})`;
}

function createMawTicketPlan(firstTicketNumber, ticketCount) {
  const first = Math.floor(Number(firstTicketNumber) || 0);
  const count = Math.max(1, Math.floor(Number(ticketCount) || 1));
  return Object.freeze(Array.from({ length: count }, (_, index) => Object.freeze({
    ticketSlot: index + 1,
    ticketNumber: first + index,
  })));
}

function summarizeMawTicketRows(rows = []) {
  const sorted = [...(Array.isArray(rows) ? rows : [])].sort((a, b) => Number(a.ticket_number) - Number(b.ticket_number));
  if (!sorted.length) return null;
  const first = sorted[0];
  const last = sorted[sorted.length - 1];
  return Object.freeze({
    firstTicketNumber: Number(first.ticket_number),
    lastTicketNumber: Number(last.ticket_number),
    ticketCount: sorted.length,
    firstTicketId: first.id == null ? null : String(first.id),
    text: formatMawTicketRange(first.ticket_number, last.ticket_number, sorted.length),
  });
}

function clearMawRankingCache() {
  cachedRankingIndex = null;
}

module.exports = {
  MAW_EXPECTED_TOKEN_COUNT,
  MAW_EXPECTED_LEGENDARY_COUNT,
  MAW_LEGENDARY_SHARED_RANK,
  MAW_FIRST_NON_LEGENDARY_RANK,
  MAW_REWARD_RULES_VERSION,
  MAW_RARITY_RULES,
  DEFAULT_MAW_RANKING_CSV_PATH,
  loadMawRankingIndex,
  calculateMawAverageRank,
  classifyMawRarity,
  getMawRewardQuote,
  formatMawAverageRank,
  validateMawRankingRows,
  parseMawRankingCsvContent,
  normalizeMawRankingHeader,
  normalizeMawTokenId,
  formatMawRarityLabel,
  snapshotMawRewardQuote,
  resolveMawSessionRewardSnapshot,
  pluralizeMawTickets,
  formatMawTicketRange,
  createMawTicketPlan,
  summarizeMawTicketRows,
  clearMawRankingCache,
};
