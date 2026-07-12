const MAW_DISPOSITIONS = Object.freeze({
  REGURGITATED: 'regurgitated',
  SWALLOWED: 'swallowed',
});

const MAW_DIGESTION_STATUS = Object.freeze({
  NOT_APPLICABLE: 'not_applicable',
  PENDING_TRANSFER: 'pending_transfer',
  PENDING_BURN: 'pending_burn',
  BURN_VERIFIED: 'burn_verified',
  DIGESTED: 'digested',
  RECEIPT_FAILED: 'receipt_failed',
});

const MAW_INVENTORY_STATUS = Object.freeze({
  AVAILABLE: 'available',
  RESERVED: 'reserved_for_claim',
  DISTRIBUTED: 'delivered',
  PENDING_BURN: 'pending_burn',
  DIGESTED: 'digested',
});

const DEFAULT_DIGESTION_ADMIN_CHANNEL_ID = '1477463175665287410';
const DEFAULT_DIGESTION_RECEIPT_CHANNEL_ID = '1524863373513068707';
const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000';
const DEFAULT_ACCEPTED_BURN_ADDRESSES = Object.freeze([
  ZERO_ADDRESS,
  '0x000000000000000000000000000000000000dead',
]);

function normalizeDisposition(value) {
  const text = String(value || '').trim().toLowerCase();
  if (text === MAW_DISPOSITIONS.SWALLOWED) return MAW_DISPOSITIONS.SWALLOWED;
  return MAW_DISPOSITIONS.REGURGITATED;
}

function isValidMawDisposition(value) {
  const text = String(value || '').trim().toLowerCase();
  return text === MAW_DISPOSITIONS.REGURGITATED || text === MAW_DISPOSITIONS.SWALLOWED;
}

function formatMawDispositionLabel(value) {
  return normalizeDisposition(value) === MAW_DISPOSITIONS.SWALLOWED ? 'Swallowed' : 'Regurgitated';
}

function mawDispositionInventoryStatus(disposition) {
  return normalizeDisposition(disposition) === MAW_DISPOSITIONS.SWALLOWED
    ? MAW_INVENTORY_STATUS.PENDING_BURN
    : MAW_INVENTORY_STATUS.AVAILABLE;
}

function mawDispositionDigestionStatus(disposition, received = false) {
  if (normalizeDisposition(disposition) !== MAW_DISPOSITIONS.SWALLOWED) return MAW_DIGESTION_STATUS.NOT_APPLICABLE;
  return received ? MAW_DIGESTION_STATUS.PENDING_BURN : MAW_DIGESTION_STATUS.PENDING_TRANSFER;
}

function isRegurgitatedAvailableInventory(row = {}) {
  const disposition = normalizeDisposition(row.disposition || row.squig_disposition);
  const status = String(row.inventory_status || row.status || '').trim() || MAW_INVENTORY_STATUS.AVAILABLE;
  return disposition === MAW_DISPOSITIONS.REGURGITATED && status === MAW_INVENTORY_STATUS.AVAILABLE;
}

function parseBurnTransactionInput(input, explorerBaseUrl = 'https://etherscan.io') {
  const raw = String(input || '').trim();
  if (!raw) {
    const err = new Error('Burn transaction is required.');
    err.code = 'MAW_BURN_TX_EMPTY';
    throw err;
  }

  let hash = raw;
  if (/^https?:\/\//i.test(raw)) {
    let url;
    try {
      url = new URL(raw);
    } catch {
      const err = new Error('Burn transaction URL is malformed.');
      err.code = 'MAW_BURN_TX_INVALID_URL';
      throw err;
    }
    if (!/(^|\.)etherscan\.io$/i.test(url.hostname)) {
      const err = new Error('Use an Etherscan transaction URL or raw Ethereum transaction hash.');
      err.code = 'MAW_BURN_TX_UNSUPPORTED_EXPLORER';
      throw err;
    }
    const match = url.pathname.match(/^\/tx\/(0x[a-fA-F0-9]{64})\/?$/);
    if (!match) {
      const err = new Error('Etherscan URL must point directly to a transaction.');
      err.code = 'MAW_BURN_TX_NOT_TX_URL';
      throw err;
    }
    hash = match[1];
  }

  hash = String(hash || '').trim().toLowerCase();
  if (!/^0x[a-f0-9]{64}$/.test(hash)) {
    const err = new Error('Burn transaction hash must be 0x followed by 64 hexadecimal characters.');
    err.code = 'MAW_BURN_TX_INVALID_HASH';
    throw err;
  }

  const base = String(explorerBaseUrl || 'https://etherscan.io').replace(/\/+$/, '');
  return Object.freeze({
    hash,
    url: `${base}/tx/${hash}`,
  });
}

function parseAcceptedBurnAddresses(value, normalizeAddress) {
  const extra = String(value || '')
    .split(',')
    .map((entry) => normalizeAddress(entry))
    .filter(Boolean);
  const defaults = DEFAULT_ACCEPTED_BURN_ADDRESSES
    .map((entry) => normalizeAddress(entry))
    .filter(Boolean);
  return [...new Set([...defaults, ...extra])];
}

function digestionStatusText(disposition, digestionStatus) {
  const normalized = normalizeDisposition(disposition);
  if (normalized === MAW_DISPOSITIONS.REGURGITATED) return 'Added to Maw Pool';
  const status = String(digestionStatus || '').trim();
  if (status === MAW_DIGESTION_STATUS.DIGESTED) return 'Digested';
  if (status === MAW_DIGESTION_STATUS.BURN_VERIFIED) return 'Burn verified; final receipt pending';
  if (status === MAW_DIGESTION_STATUS.RECEIPT_FAILED) return 'Burn verified; final receipt failed';
  return 'Awaiting digestion';
}

module.exports = {
  MAW_DISPOSITIONS,
  MAW_DIGESTION_STATUS,
  MAW_INVENTORY_STATUS,
  DEFAULT_DIGESTION_ADMIN_CHANNEL_ID,
  DEFAULT_DIGESTION_RECEIPT_CHANNEL_ID,
  ZERO_ADDRESS,
  DEFAULT_ACCEPTED_BURN_ADDRESSES,
  normalizeDisposition,
  isValidMawDisposition,
  formatMawDispositionLabel,
  mawDispositionInventoryStatus,
  mawDispositionDigestionStatus,
  isRegurgitatedAvailableInventory,
  parseBurnTransactionInput,
  parseAcceptedBurnAddresses,
  digestionStatusText,
};
