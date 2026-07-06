const crypto = require('crypto');
const {
  SlashCommandBuilder,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  StringSelectMenuBuilder,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  ChannelType,
  PermissionFlagsBits,
} = require('discord.js');

const MARKETPLACE_NOTICE_CHANNEL_ID = process.env.MARKETPLACE_NOTICE_CHANNEL_ID || '1321864977270706257';
const MARKETPLACE_THREAD_PARENT_CHANNEL_ID = process.env.MARKETPLACE_THREAD_PARENT_CHANNEL_ID || '';
const MARKETPLACE_ADMIN_USER_IDS = process.env.MARKETPLACE_ADMIN_USER_IDS || '';
const MARKETPLACE_ADMIN_ROLE_IDS = process.env.MARKETPLACE_ADMIN_ROLE_IDS || '';
const MARKETPLACE_TIME_ZONE = process.env.MARKETPLACE_TIME_ZONE || 'America/Toronto';
const MARKETPLACE_RECEIPT_CHANNEL_ID = MARKETPLACE_NOTICE_CHANNEL_ID;
const DEFAULT_MARKETPLACE_ADMIN_USER_IDS = Object.freeze([
  '1288107772248064044',
  '826581856400179210',
]);

const MARKETPLACE_PURCHASE_TABLE = 'malformed_marketplace_purchases';
const CONFIRMATION_TTL_MS = 5 * 60 * 1000;
const STALE_RESERVATION_MINUTES = 15;
const STOCK_CONSUMING_STATUSES = ['reserved', 'paid_pending_delivery', 'delivered'];

const MARKETPLACE_MONTHLY_CAPS = Object.freeze({
  charm: 2,
  common: 4,
  uncommon: 4,
  monster: 2,
  rare: 2,
  epic: 1,
  custom: 1,
});

const MARKETPLACE_ITEMS = [
  {
    order: 1,
    key: 'charm',
    buttonId: 'marketplace_buy_charm',
    confirmationKey: 'charm',
    name: 'Charm of the Ugly',
    price: 15000,
    buttonLabel: 'COTU',
  },
  {
    order: 2,
    key: 'common',
    buttonId: 'marketplace_buy_common',
    confirmationKey: 'common',
    name: 'Common Squig',
    price: 20000,
    buttonLabel: 'Common',
  },
  {
    order: 3,
    key: 'uncommon',
    buttonId: 'marketplace_buy_uncommon',
    confirmationKey: 'uncommon',
    name: 'Uncommon Squig',
    price: 30000,
    buttonLabel: 'Uncommon',
  },
  {
    order: 4,
    key: 'monster',
    buttonId: 'marketplace_buy_monster',
    confirmationKey: 'monster',
    name: 'Ugly Monster',
    price: 45000,
    buttonLabel: 'Monster',
  },
  {
    order: 5,
    key: 'rare',
    buttonId: 'marketplace_buy_rare',
    confirmationKey: 'rare',
    name: 'Rare Squig',
    price: 90000,
    buttonLabel: 'Rare',
  },
  {
    order: 6,
    key: 'epic',
    buttonId: 'marketplace_buy_epic',
    confirmationKey: 'epic',
    name: 'Epic Squig',
    price: 250000,
    buttonLabel: 'Epic',
  },
  {
    order: 7,
    key: 'custom',
    buttonId: 'marketplace_buy_custom',
    confirmationKey: 'custom',
    name: 'Custom Edition Squig',
    price: 1000000,
    buttonLabel: 'Custom Edition',
  },
];

const pendingConfirmations = new Map();
const activePurchaseLocks = new Set();

function buildMarketplaceSlashCommand() {
  return new SlashCommandBuilder()
    .setName('marketplace')
    .setDescription('Post the Malformed Marketplace purchase panel');
}

function formatCharm(amount) {
  return new Intl.NumberFormat('en-US').format(Math.floor(Number(amount) || 0));
}

function parseConfiguredIds(value) {
  return String(value || '')
    .split(',')
    .map((id) => id.trim())
    .filter(Boolean);
}

function getMarketplaceAdminUserIds() {
  return [
    ...new Set([
      ...DEFAULT_MARKETPLACE_ADMIN_USER_IDS,
      ...parseConfiguredIds(MARKETPLACE_ADMIN_USER_IDS),
    ]),
  ];
}

function truncateText(value, maxLength = 1024) {
  const text = String(value ?? '');
  return text.length > maxLength ? `${text.slice(0, Math.max(0, maxLength - 3))}...` : text;
}

function getMarketplaceItem(itemKey) {
  return MARKETPLACE_ITEMS.find((item) =>
    item.key === itemKey ||
    item.buttonId === itemKey ||
    item.confirmationKey === itemKey
  ) || null;
}

function getMarketplaceMonthKey(date = new Date(), timeZone = MARKETPLACE_TIME_ZONE) {
  let parts;
  try {
    parts = new Intl.DateTimeFormat('en-US', {
      timeZone,
      year: 'numeric',
      month: '2-digit',
    }).formatToParts(date);
  } catch (_) {
    parts = new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/Toronto',
      year: 'numeric',
      month: '2-digit',
    }).formatToParts(date);
  }

  const year = parts.find((part) => part.type === 'year')?.value;
  const month = parts.find((part) => part.type === 'month')?.value;
  return `${year}-${month}`;
}

function getMarketplaceCap(itemKey) {
  return Math.max(0, Math.floor(Number(MARKETPLACE_MONTHLY_CAPS[itemKey]) || 0));
}

function getMarketplaceStockState(itemKey, soldCount = 0) {
  const cap = getMarketplaceCap(itemKey);
  const sold = Math.max(0, Math.floor(Number(soldCount) || 0));
  const remaining = Math.max(0, cap - sold);
  return {
    itemKey,
    cap,
    sold,
    remaining,
    soldOut: remaining <= 0,
  };
}

function isHttpUrl(value) {
  const text = String(value || '').trim();
  return /^https?:\/\/\S+$/i.test(text);
}

function isDirectImageUrl(value) {
  const text = String(value || '').trim();
  if (!isHttpUrl(text)) return false;
  const pathOnly = text.split(/[?#]/)[0].toLowerCase();
  return /\.(png|jpe?g|gif|webp)$/i.test(pathOnly);
}

function resolveMarketplacePool(deps = {}) {
  const pool = deps.marketplacePool || deps.prizesPool || deps.pool || deps.db || null;
  if (!pool?.query) {
    throw new Error('Marketplace database pool is not configured.');
  }
  return pool;
}

async function ensureMarketplaceTables(deps = {}) {
  const pool = deps?.query ? deps : resolveMarketplacePool(deps);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${MARKETPLACE_PURCHASE_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      guild_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      item_key TEXT NOT NULL,
      item_name TEXT NOT NULL,
      price NUMERIC NOT NULL,
      month_key TEXT NOT NULL,
      wallet_address TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'reserved',
      thread_id TEXT,
      delivery_message_id TEXT,
      reward_image_url TEXT,
      reward_opensea_url TEXT,
      admin_note TEXT,
      delivered_by TEXT,
      delivered_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  const columns = [
    ['guild_id', 'TEXT'],
    ['user_id', 'TEXT'],
    ['item_key', 'TEXT'],
    ['item_name', 'TEXT'],
    ['price', 'NUMERIC'],
    ['month_key', 'TEXT'],
    ['wallet_address', 'TEXT'],
    ['status', "TEXT NOT NULL DEFAULT 'reserved'"],
    ['thread_id', 'TEXT'],
    ['delivery_message_id', 'TEXT'],
    ['reward_image_url', 'TEXT'],
    ['reward_opensea_url', 'TEXT'],
    ['admin_note', 'TEXT'],
    ['delivered_by', 'TEXT'],
    ['delivered_at', 'TIMESTAMPTZ'],
    ['created_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
    ['updated_at', 'TIMESTAMPTZ NOT NULL DEFAULT NOW()'],
  ];
  for (const [name, type] of columns) {
    await pool.query(`ALTER TABLE ${MARKETPLACE_PURCHASE_TABLE} ADD COLUMN IF NOT EXISTS ${name} ${type};`);
  }

  await pool.query(
    `CREATE INDEX IF NOT EXISTS malformed_marketplace_stock_idx
     ON ${MARKETPLACE_PURCHASE_TABLE} (guild_id, item_key, month_key, status);`
  );
  await pool.query(
    `CREATE INDEX IF NOT EXISTS malformed_marketplace_user_idx
     ON ${MARKETPLACE_PURCHASE_TABLE} (user_id, created_at DESC);`
  );
  await pool.query(
    `CREATE INDEX IF NOT EXISTS malformed_marketplace_thread_idx
     ON ${MARKETPLACE_PURCHASE_TABLE} (thread_id);`
  );
}

async function expireStaleReservations(db, guildId = null) {
  const params = [STALE_RESERVATION_MINUTES];
  let guildClause = '';
  if (guildId) {
    params.push(String(guildId));
    guildClause = ` AND guild_id = $${params.length}`;
  }
  await db.query(
    `UPDATE ${MARKETPLACE_PURCHASE_TABLE}
     SET status = 'expired',
         updated_at = NOW()
     WHERE status = 'reserved'
       AND created_at < NOW() - ($1::int * INTERVAL '1 minute')
       ${guildClause}`,
    params
  );
}

async function getMarketplaceStockForItem(deps, guildId, itemKey, date = new Date()) {
  const pool = resolveMarketplacePool(deps);
  const monthKey = getMarketplaceMonthKey(date);
  await expireStaleReservations(pool, guildId);
  const { rows } = await pool.query(
    `SELECT COUNT(*)::int AS sold
     FROM ${MARKETPLACE_PURCHASE_TABLE}
     WHERE guild_id = $1
       AND item_key = $2
       AND month_key = $3
       AND status = ANY($4::text[])`,
    [String(guildId), String(itemKey), monthKey, STOCK_CONSUMING_STATUSES]
  );
  return {
    ...getMarketplaceStockState(itemKey, Number(rows[0]?.sold || 0)),
    monthKey,
  };
}

async function getMarketplaceStockSummaries(deps, guildId, date = new Date()) {
  const pool = resolveMarketplacePool(deps);
  const monthKey = getMarketplaceMonthKey(date);
  await expireStaleReservations(pool, guildId);
  const { rows } = await pool.query(
    `SELECT item_key, COUNT(*)::int AS sold
     FROM ${MARKETPLACE_PURCHASE_TABLE}
     WHERE guild_id = $1
       AND month_key = $2
       AND status = ANY($3::text[])
     GROUP BY item_key`,
    [String(guildId), monthKey, STOCK_CONSUMING_STATUSES]
  );
  const soldByKey = new Map(rows.map((row) => [String(row.item_key), Number(row.sold || 0)]));
  return MARKETPLACE_ITEMS.map((item) => ({
    item,
    monthKey,
    ...getMarketplaceStockState(item.key, soldByKey.get(item.key) || 0),
  }));
}

function advisoryLockKey(value) {
  return crypto.createHash('sha256').update(String(value)).digest().readBigInt64BE(0).toString();
}

async function reserveMarketplacePurchase(deps, { guildId, userId, item, walletAddress }) {
  const pool = resolveMarketplacePool(deps);
  if (!pool?.connect) {
    throw new Error('Marketplace database pool does not support transactions.');
  }

  const monthKey = getMarketplaceMonthKey();
  const db = await pool.connect();
  let done = false;
  try {
    await db.query('BEGIN');
    await db.query('SELECT pg_advisory_xact_lock($1::bigint)', [
      advisoryLockKey(`malformed-marketplace-stock:${guildId}:${item.key}:${monthKey}`),
    ]);
    await db.query('SELECT pg_advisory_xact_lock($1::bigint)', [
      advisoryLockKey(`malformed-marketplace-user:${guildId}:${userId}`),
    ]);

    await expireStaleReservations(db, guildId);

    const inProgress = await db.query(
      `SELECT id, item_name
       FROM ${MARKETPLACE_PURCHASE_TABLE}
       WHERE guild_id = $1
         AND user_id = $2
         AND status = 'reserved'
       ORDER BY created_at DESC
       LIMIT 1`,
      [String(guildId), String(userId)]
    );
    if (inProgress.rows[0]) {
      await db.query('ROLLBACK');
      done = true;
      return {
        ok: false,
        reasonCode: 'in_progress',
        reason: 'You already have a marketplace purchase in progress. Wait for it to finish, then try again.',
        purchase: inProgress.rows[0],
      };
    }

    const stockRows = await db.query(
      `SELECT COUNT(*)::int AS sold
       FROM ${MARKETPLACE_PURCHASE_TABLE}
       WHERE guild_id = $1
         AND item_key = $2
         AND month_key = $3
         AND status = ANY($4::text[])`,
      [String(guildId), item.key, monthKey, STOCK_CONSUMING_STATUSES]
    );
    const soldBefore = Number(stockRows.rows[0]?.sold || 0);
    const stockBefore = getMarketplaceStockState(item.key, soldBefore);
    if (stockBefore.remaining <= 0) {
      await db.query('ROLLBACK');
      done = true;
      return {
        ok: false,
        reasonCode: 'sold_out',
        reason: 'That reward is sold out for this month. Stock resets at the beginning of next month.',
        stock: { ...stockBefore, monthKey },
      };
    }

    const insert = await db.query(
      `INSERT INTO ${MARKETPLACE_PURCHASE_TABLE}
         (guild_id, user_id, item_key, item_name, price, month_key, wallet_address, status, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'reserved', NOW(), NOW())
       RETURNING *`,
      [
        String(guildId),
        String(userId),
        item.key,
        item.name,
        Math.floor(Number(item.price) || 0),
        monthKey,
        walletAddress,
      ]
    );

    await db.query('COMMIT');
    done = true;
    return {
      ok: true,
      purchase: insert.rows[0],
      stock: {
        ...getMarketplaceStockState(item.key, soldBefore + 1),
        monthKey,
        soldBefore,
        remainingBefore: stockBefore.remaining,
      },
    };
  } catch (error) {
    if (!done) await db.query('ROLLBACK').catch(() => null);
    throw error;
  } finally {
    db.release();
  }
}

async function getMarketplacePurchaseById(deps, purchaseId) {
  const pool = resolveMarketplacePool(deps);
  const { rows } = await pool.query(
    `SELECT *
     FROM ${MARKETPLACE_PURCHASE_TABLE}
     WHERE id = $1
     LIMIT 1`,
    [String(purchaseId)]
  );
  return rows[0] || null;
}

async function updatePurchaseThread(deps, purchaseId, threadId) {
  const pool = resolveMarketplacePool(deps);
  const { rows } = await pool.query(
    `UPDATE ${MARKETPLACE_PURCHASE_TABLE}
     SET thread_id = $2,
         updated_at = NOW()
     WHERE id = $1
     RETURNING *`,
    [String(purchaseId), String(threadId)]
  );
  return rows[0] || null;
}

async function updatePurchaseDeliveryMessage(deps, purchaseId, messageId) {
  const pool = resolveMarketplacePool(deps);
  const { rows } = await pool.query(
    `UPDATE ${MARKETPLACE_PURCHASE_TABLE}
     SET delivery_message_id = $2,
         updated_at = NOW()
     WHERE id = $1
     RETURNING *`,
    [String(purchaseId), String(messageId)]
  );
  return rows[0] || null;
}

async function markPurchasePaid(deps, purchaseId) {
  const pool = resolveMarketplacePool(deps);
  const { rows } = await pool.query(
    `UPDATE ${MARKETPLACE_PURCHASE_TABLE}
     SET status = 'paid_pending_delivery',
         updated_at = NOW()
     WHERE id = $1
       AND status = 'reserved'
     RETURNING *`,
    [String(purchaseId)]
  );
  return rows[0] || null;
}

async function markPurchaseFailed(deps, purchaseId, status) {
  const pool = resolveMarketplacePool(deps);
  const { rows } = await pool.query(
    `UPDATE ${MARKETPLACE_PURCHASE_TABLE}
     SET status = $2,
         updated_at = NOW()
     WHERE id = $1
     RETURNING *`,
    [String(purchaseId), String(status)]
  );
  return rows[0] || null;
}

async function markPurchaseDelivered(deps, purchaseId, { imageUrl, rewardUrl, adminNote, adminId }) {
  const pool = resolveMarketplacePool(deps);
  const { rows } = await pool.query(
    `UPDATE ${MARKETPLACE_PURCHASE_TABLE}
     SET status = 'delivered',
         reward_image_url = $2,
         reward_opensea_url = $3,
         admin_note = $4,
         delivered_by = $5,
         delivered_at = NOW(),
         updated_at = NOW()
     WHERE id = $1
       AND status = 'paid_pending_delivery'
     RETURNING *`,
    [
      String(purchaseId),
      imageUrl || null,
      rewardUrl || null,
      adminNote || null,
      String(adminId),
    ]
  );
  return rows[0] || null;
}

function stockSummaryForItem(stockSummaries, item) {
  return stockSummaries?.find((summary) => summary.item?.key === item.key || summary.itemKey === item.key) || {
    item,
    monthKey: getMarketplaceMonthKey(),
    ...getMarketplaceStockState(item.key, 0),
  };
}

function buildMarketplacePanelEmbed(stockSummaries = []) {
  const hasStockSummaries = Array.isArray(stockSummaries) && stockSummaries.length > 0;
  const stockLines = hasStockSummaries
    ? MARKETPLACE_ITEMS.map((item) => {
        const stock = stockSummaryForItem(stockSummaries, item);
        return `${item.name} - ${stock.remaining}/${stock.cap} left this month`;
      }).join('\n')
    : 'Stock is temporarily unavailable. Checkout will still verify current stock before purchase.';

  return new EmbedBuilder()
    .setTitle('Malformed Marketplace')
    .setColor(0xd4a43b)
    .setDescription(
      'Spend $CHARM on limited monthly rewards. Pick an item, confirm privately, and a private delivery thread will open automatically for you and the admins.'
    )
    .setImage('https://i.imgur.com/WS0O1AA.jpeg')
    .addFields(
      {
        name: 'How it works',
        value:
          '1. Open Marketplace\n' +
          '2. Pick a reward\n' +
          '3. Confirm purchase\n' +
          '4. A private delivery thread opens automatically\n' +
          '5. Admin marks it Sent when delivered',
        inline: false,
      },
      {
        name: 'Monthly Stock',
        value: stockLines || 'Stock is loading.',
        inline: false,
      },
      {
        name: 'Reset',
        value: 'Stock resets at the beginning of each month.',
        inline: false,
      }
    )
    .setFooter({
      text: 'A linked wallet is required. Delivery is handled in an automatic private thread.',
    });
}

function buildMarketplaceItemRows() {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId('marketplace_open')
        .setLabel('Open Marketplace')
        .setStyle(ButtonStyle.Success)
    ),
  ];
}

function buildMarketplaceSelectRows(stockSummaries = []) {
  return [
    new ActionRowBuilder().addComponents(
      new StringSelectMenuBuilder()
        .setCustomId('marketplace_select_item')
        .setPlaceholder('Choose a reward')
        .addOptions(
          MARKETPLACE_ITEMS.map((item) => {
            const stock = stockSummaryForItem(stockSummaries, item);
            const soldOutPrefix = stock.soldOut ? 'SOLD OUT - ' : '';
            return {
              label: `${soldOutPrefix}${item.name}`.slice(0, 100),
              description: `${formatCharm(item.price)} $CHARM - ${stock.remaining}/${stock.cap} left this month`.slice(0, 100),
              value: item.key,
            };
          })
        )
    ),
  ];
}

function buildConfirmationEmbed(item, { walletAddress, stock }) {
  const remainingAfterPurchase = Math.max(0, Number(stock?.remaining || 0) - 1);
  return new EmbedBuilder()
    .setColor(0xe6b422)
    .setTitle('Confirm Purchase')
    .setDescription(`Confirm your ${item.name} purchase privately.`)
    .addFields(
      { name: 'Reward', value: item.name, inline: true },
      { name: 'Price', value: `${formatCharm(item.price)} $CHARM`, inline: true },
      {
        name: 'Monthly stock after purchase',
        value: `${remainingAfterPurchase}/${stock?.cap ?? getMarketplaceCap(item.key)} left this month`,
        inline: true,
      },
      { name: 'Wallet', value: `\`${walletAddress}\``, inline: false }
    );
}

function buildConfirmationRows(token) {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`marketplace_confirm_yes:${token}`)
        .setLabel('Yes')
        .setStyle(ButtonStyle.Success),
      new ButtonBuilder()
        .setCustomId(`marketplace_confirm_no:${token}`)
        .setLabel('No')
        .setStyle(ButtonStyle.Secondary)
    ),
  ];
}

function buildDeliveryButtonRow(purchase, delivered = false) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`marketplace_delivery_sent:${purchase.id}`)
      .setLabel(delivered ? 'Delivered' : 'Sent')
      .setStyle(ButtonStyle.Success)
      .setDisabled(Boolean(delivered))
  );
}

function buildDeliveryEmbed(purchase, { buyerUser = null, stock = null, delivered = false } = {}) {
  const buyerTag = buyerUser?.tag ? `\n${buyerUser.tag}` : '';
  const stockText = stock
    ? `${stock.sold}/${stock.cap} sold this month (${stock.remaining} left)`
    : `${purchase.month_key || getMarketplaceMonthKey()} month`;
  const embed = new EmbedBuilder()
    .setTitle('Marketplace Reward Delivery')
    .setColor(delivered ? 0x3ba55c : 0xe6b422)
    .addFields(
      { name: 'Buyer', value: `<@${purchase.user_id}>${buyerTag}`, inline: false },
      { name: 'Item', value: purchase.item_name, inline: true },
      { name: 'Price', value: `${formatCharm(purchase.price)} $CHARM`, inline: true },
      { name: 'Purchase ID', value: String(purchase.id), inline: true },
      { name: 'Wallet', value: `\`${purchase.wallet_address || 'Unavailable'}\``, inline: false },
      { name: 'Month stock', value: stockText, inline: true },
      { name: 'Status', value: delivered ? 'Delivered' : 'Paid, pending delivery', inline: true }
    )
    .setTimestamp();

  if (!delivered) {
    embed.setDescription(
      'Admin: deliver the reward, then press Sent. You can optionally include an image URL, OpenSea link, and note.'
    );
  }
  return embed;
}

function buildDeliveryConfirmationPayload(purchase, adminId) {
  const embed = new EmbedBuilder()
    .setColor(0x3ba55c)
    .setTitle('Reward Sent')
    .setDescription(`Reward marked sent by <@${adminId}>.`)
    .setTimestamp();

  const fields = [];
  if (purchase.reward_opensea_url) {
    fields.push({ name: 'Reward link', value: truncateText(purchase.reward_opensea_url), inline: false });
  }
  if (purchase.admin_note) {
    fields.push({ name: 'Note', value: truncateText(purchase.admin_note), inline: false });
  }
  if (purchase.reward_image_url && !isDirectImageUrl(purchase.reward_image_url)) {
    fields.push({ name: 'Image URL', value: truncateText(purchase.reward_image_url), inline: false });
  }
  if (fields.length) embed.addFields(fields);
  if (isDirectImageUrl(purchase.reward_image_url)) embed.setImage(purchase.reward_image_url);

  return {
    content: `Reward marked sent by <@${adminId}>.`,
    embeds: [embed],
  };
}

function buildMarketplaceNoticePayload(purchase, adminId) {
  const sentence = `${purchase.item_name} was purchased from the Malformed Marketplace by <@${purchase.user_id}>`;
  const embed = new EmbedBuilder()
    .setTitle('Malformed Marketplace Purchase')
    .setColor(0x3ba55c)
    .setDescription(sentence)
    .addFields(
      { name: 'Purchaser', value: `<@${purchase.user_id}>`, inline: true },
      { name: 'Prize', value: purchase.item_name, inline: true },
      { name: 'Marked sent by', value: `<@${adminId}>`, inline: true }
    )
    .setTimestamp();

  if (purchase.reward_opensea_url) {
    embed.addFields({ name: 'OpenSea / reward link', value: truncateText(purchase.reward_opensea_url), inline: false });
  }
  if (purchase.admin_note) {
    embed.addFields({ name: 'Admin note', value: truncateText(purchase.admin_note), inline: false });
  }
  if (isDirectImageUrl(purchase.reward_image_url)) embed.setImage(purchase.reward_image_url);

  return {
    content: sentence,
    embeds: [embed],
  };
}

function buildPurchaseThreadIntroPayload(purchase) {
  const buyerId = String(purchase.user_id);
  return {
    content:
      `Congrats <@${buyerId}> on your new purchase. ` +
      'The system is double checking all credentials to ensure you are Ugly enough to make this purchase. ' +
      'A receipt will pop up in here shortly and the team will get your item over to you ASAP.',
    allowedMentions: { users: [buyerId] },
  };
}

function buildMarketplaceAdminReadyPayload(purchase) {
  const buyerId = String(purchase.user_id);
  const adminIds = getMarketplaceAdminUserIds()
    .map((userId) => String(userId))
    .filter((userId) => userId && userId !== buyerId);
  const uniqueAdminIds = [...new Set(adminIds)];
  const mentions = uniqueAdminIds.map((userId) => `<@${userId}>`).join(' ');
  return {
    content: `${mentions} everything is cleared and good to go.`.trim(),
    allowedMentions: { users: uniqueAdminIds },
  };
}

function createConfirmation(interaction, item) {
  const token = crypto.randomBytes(6).toString('hex');
  pendingConfirmations.set(token, {
    token,
    guildId: interaction.guildId,
    channelId: interaction.channelId,
    userId: interaction.user.id,
    itemKey: item.key,
    expiresAt: Date.now() + CONFIRMATION_TTL_MS,
    processing: false,
    completed: false,
  });
  return token;
}

function getPendingConfirmation(token) {
  const state = pendingConfirmations.get(token);
  if (!state) return null;
  if (Date.now() > state.expiresAt) {
    pendingConfirmations.delete(token);
    return null;
  }
  return state;
}

function clearPendingConfirmation(token) {
  pendingConfirmations.delete(token);
}

function getPurchaseLockKey(guildId, userId) {
  return `${guildId}:${userId}`;
}

async function getLinkedWallet(deps, guildId, userId) {
  const links = await deps.getWalletLinks(guildId, userId);
  const walletAddress = links.find((entry) => entry?.wallet_address)?.wallet_address || null;
  return {
    walletAddress,
    links,
  };
}

async function getUserCharmBalance(deps, guildId, userId) {
  const spendable = await deps.getMarketplaceSpendableBalance(guildId, userId);
  if (!spendable.ok) return spendable;

  const resolvedMemberBalance = deps.extractDripCurrencyAmountFromPayload(
    spendable.resolvedMember || null,
    spendable.settings.currency_id
  );
  const balance = resolvedMemberBalance != null
    ? resolvedMemberBalance
    : await deps.getDripMemberCurrencyBalance(
        spendable.settings.drip_realm_id,
        spendable.memberIds,
        spendable.settings.currency_id,
        spendable.settings
      );

  if (!Number.isFinite(Number(balance))) {
    return {
      ok: false,
      reason: 'Could not check your $CHARM balance right now. Please try again in a moment.',
      spendable,
    };
  }

  return {
    ok: true,
    balance: Math.floor(Number(balance)),
    spendable,
  };
}

async function checkUserCharmBalance(deps, guildId, userId, requiredAmount) {
  const result = await getUserCharmBalance(deps, guildId, userId);
  if (!result.ok) return result;
  return {
    ...result,
    hasEnough: result.balance >= Math.floor(Number(requiredAmount) || 0),
  };
}

async function transferCharmToMarketplace(deps, guildId, userId, item, spendable) {
  const amount = Math.floor(Number(item?.price || 0));
  return deps.awardDripPoints(
    spendable.settings.drip_realm_id,
    [spendable.botMemberId],
    amount,
    spendable.settings.currency_id,
    spendable.settings,
    {
      context: 'malformed_marketplace_purchase',
      initiatorDiscordId: userId,
      recipientDiscordId: deps.clientUserId || null,
      recipientMemberIdOverride: spendable.botMemberId,
      senderMemberIdOverride: spendable.memberIds[0],
    }
  );
}

async function sendMarketplaceReceipt(deps, guild, user, member, item, walletAddress) {
  try {
    const channel = await guild.channels.fetch(MARKETPLACE_RECEIPT_CHANNEL_ID).catch(() => null);
    if (!channel?.isTextBased()) return;

    const embed = new EmbedBuilder()
      .setTitle('Marketplace Purchase Receipt')
      .setColor(0x3ba55c)
      .addFields(
        { name: 'User', value: `${user.tag}`, inline: true },
        { name: 'Display Name', value: member?.displayName || user.username, inline: true },
        { name: 'Discord User ID', value: user.id, inline: true },
        { name: 'Item Purchased', value: item.name, inline: true },
        { name: 'Amount Spent', value: `${formatCharm(item.price)} $CHARM`, inline: true },
        { name: 'Linked Wallet Address', value: walletAddress || 'Unavailable', inline: false },
        { name: 'Timestamp', value: `<t:${Math.floor(Date.now() / 1000)}:F>`, inline: false },
      )
      .setTimestamp();

    await channel.send({ embeds: [embed] });
  } catch (error) {
    await postMarketplaceLog(deps, {
      guild,
      category: 'Marketplace Receipt Failure',
      userId: user?.id,
      item,
      status: 'receipt_failed',
      error,
    });
  }
}

async function postMarketplaceLog(deps, { guild = null, guildId = null, category, purchase = null, purchaseId = null, userId = null, item = null, status = null, error = null, extra = '' }) {
  if (!deps?.postAdminSystemLog) return;
  const itemName = item?.name || item?.item_name || purchase?.item_name || 'unknown';
  const resolvedUserId = userId || purchase?.user_id || 'unknown';
  const resolvedPurchaseId = purchaseId || purchase?.id || 'none';
  const resolvedStatus = status || purchase?.status || 'unknown';
  const reason = error ? String(error?.message || error || '').slice(0, 900) : '';
  await deps.postAdminSystemLog({
    guild,
    guildId,
    category: category || 'Marketplace Failure',
    message:
      `Purchase ID: ${resolvedPurchaseId}\n` +
      `User: ${resolvedUserId !== 'unknown' ? `<@${resolvedUserId}>` : 'unknown'}\n` +
      `Item: **${itemName}**\n` +
      `Status: ${resolvedStatus}\n` +
      `${extra ? `${extra}\n` : ''}` +
      `Reason: ${reason || 'No error detail'}`,
  });
}

function memberHasMarketplaceAdminPermission(member) {
  return Boolean(
    member?.permissions?.has(PermissionFlagsBits.Administrator) ||
    member?.permissions?.has(PermissionFlagsBits.ManageThreads)
  );
}

function memberHasConfiguredMarketplaceRole(member) {
  const roleIds = parseConfiguredIds(MARKETPLACE_ADMIN_ROLE_IDS);
  if (!roleIds.length) return false;
  const roles = member?.roles;
  if (roles?.cache) return roleIds.some((roleId) => roles.cache.has(roleId));
  if (Array.isArray(roles)) return roleIds.some((roleId) => roles.includes(roleId));
  return false;
}

function isMarketplaceAdminInteraction(interaction) {
  const explicitUserIds = new Set(getMarketplaceAdminUserIds());
  if (explicitUserIds.has(String(interaction.user?.id || ''))) return true;
  if (
    interaction.memberPermissions?.has(PermissionFlagsBits.Administrator) ||
    interaction.memberPermissions?.has(PermissionFlagsBits.ManageThreads)
  ) return true;
  return memberHasConfiguredMarketplaceRole(interaction.member);
}

async function resolveMarketplaceAdminUserIds(guild) {
  const explicitUserIds = new Set(getMarketplaceAdminUserIds());
  const result = new Set(explicitUserIds);

  let fetchedAllMembers = false;
  await guild.members.fetch().then(() => {
    fetchedAllMembers = true;
  }).catch(() => null);

  for (const member of guild.members.cache.values()) {
    const explicitlyConfigured = explicitUserIds.has(member.id);
    if (member.user?.bot && !explicitlyConfigured) continue;
    if (explicitlyConfigured || memberHasConfiguredMarketplaceRole(member) || memberHasMarketplaceAdminPermission(member)) {
      result.add(member.id);
    }
  }

  return {
    userIds: [...result],
    fetchedAllMembers,
  };
}

async function resolveMarketplaceThreadParent(interaction) {
  const configuredParentId = String(MARKETPLACE_THREAD_PARENT_CHANNEL_ID || '').trim();
  let parent = configuredParentId
    ? await interaction.guild.channels.fetch(configuredParentId).catch(() => null)
    : interaction.channel;

  if (parent?.isThread?.()) {
    parent = parent.parent || await interaction.guild.channels.fetch(parent.parentId).catch(() => null);
  }
  if (!parent && interaction.channelId) {
    parent = await interaction.guild.channels.fetch(interaction.channelId).catch(() => null);
  }
  if (!parent?.threads?.create) {
    throw new Error('Marketplace thread parent channel is unavailable or cannot create threads.');
  }
  return parent;
}

function sanitizeThreadName(value) {
  return String(value || 'marketplace-delivery')
    .replace(/[\r\n\t]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 100) || 'marketplace-delivery';
}

async function createMarketplaceDeliveryThread(interaction, purchase) {
  let thread = null;
  try {
    const parent = await resolveMarketplaceThreadParent(interaction);
    thread = await parent.threads.create({
      name: sanitizeThreadName(`marketplace-${purchase.id}-${purchase.item_name}`),
      autoArchiveDuration: 1440,
      type: ChannelType.PrivateThread,
      invitable: false,
      reason: `Malformed Marketplace delivery for purchase ${purchase.id}`,
    });

    await thread.members.add(purchase.user_id);

    const adminResolution = await resolveMarketplaceAdminUserIds(interaction.guild);
    const adminAddFailures = [];
    for (const userId of adminResolution.userIds) {
      if (String(userId) === String(purchase.user_id)) continue;
      if (String(userId) === String(interaction.client.user?.id || '')) continue;
      await thread.members.add(userId).catch((error) => {
        adminAddFailures.push(`${userId}: ${String(error?.message || error || '').slice(0, 160)}`);
      });
    }

    return {
      thread,
      adminIds: adminResolution.userIds,
      adminAddFailures,
      fetchedAllMembers: adminResolution.fetchedAllMembers,
    };
  } catch (error) {
    if (thread) {
      error.thread = thread;
    }
    throw error;
  }
}

async function archiveFailedThread(thread, message) {
  if (!thread?.isTextBased?.()) return;
  await thread.send({ content: message }).catch(() => null);
  await thread.setArchived(true, 'Marketplace purchase failed').catch(() => null);
}

async function postDeliveryNotice(deps, guild, purchase, adminId) {
  const channel = await guild.channels.fetch(MARKETPLACE_NOTICE_CHANNEL_ID).catch(() => null);
  if (!channel?.isTextBased()) {
    throw new Error(`Marketplace notice channel ${MARKETPLACE_NOTICE_CHANNEL_ID} is unavailable.`);
  }
  await channel.send(buildMarketplaceNoticePayload(purchase, adminId));
}

async function safeEphemeralReply(interaction, payload) {
  const finalPayload = { ...payload };
  if (!('flags' in finalPayload)) finalPayload.flags = 64;
  if (interaction.deferred) {
    const { flags, ...editPayload } = finalPayload;
    return interaction.editReply(editPayload).catch(() => null);
  }
  if (interaction.replied) {
    return interaction.followUp(finalPayload).catch(() => null);
  }
  return interaction.reply(finalPayload).catch(() => null);
}

function insufficientBalanceMessage(item, balance) {
  return (
    `You do not have enough $CHARM for ${item.name}.\n` +
    `Balance: ${formatCharm(balance)} $CHARM\n` +
    `Price: ${formatCharm(item.price)} $CHARM`
  );
}

async function beginMarketplaceCheckout(interaction, deps, item) {
  await interaction.deferReply({ flags: 64 });

  const lockKey = getPurchaseLockKey(interaction.guildId, interaction.user.id);
  if (activePurchaseLocks.has(lockKey)) {
    await interaction.editReply({
      content: 'You already have a marketplace purchase in progress. Wait for it to finish, then try again.',
      components: [],
    });
    return true;
  }

  const stock = await getMarketplaceStockForItem(deps, interaction.guildId, item.key);
  if (stock.soldOut) {
    await interaction.editReply({
      content: 'That reward is sold out for this month. Stock resets at the beginning of next month.',
      components: [],
    });
    return true;
  }

  const wallet = await getLinkedWallet(deps, interaction.guildId, interaction.user.id);
  if (!wallet.walletAddress) {
    await interaction.editReply({
      content: 'You must link a wallet before using the marketplace.',
      components: [],
    });
    return true;
  }

  const balanceCheck = await checkUserCharmBalance(deps, interaction.guildId, interaction.user.id, item.price);
  if (!balanceCheck.ok) {
    await interaction.editReply({
      content: balanceCheck.reason || 'Could not verify your $CHARM balance right now.',
      components: [],
    });
    return true;
  }

  if (!balanceCheck.hasEnough) {
    await interaction.editReply({
      content: insufficientBalanceMessage(item, balanceCheck.balance),
      components: [],
    });
    return true;
  }

  const token = createConfirmation(interaction, item);
  await interaction.editReply({
    embeds: [buildConfirmationEmbed(item, { walletAddress: wallet.walletAddress, stock })],
    components: buildConfirmationRows(token),
  });
  return true;
}

async function handleMarketplaceCommand(interaction, deps = {}) {
  await interaction.deferReply();
  let stockSummaries = null;
  try {
    stockSummaries = await getMarketplaceStockSummaries(deps, interaction.guildId);
  } catch (error) {
    await postMarketplaceLog(deps, {
      guild: interaction.guild,
      category: 'Marketplace Stock Lookup Failure',
      userId: interaction.user.id,
      status: 'panel_stock_lookup_failed',
      error,
    });
  }
  await interaction.editReply({
    embeds: [buildMarketplacePanelEmbed(stockSummaries)],
    components: buildMarketplaceItemRows(),
  });
}

async function handleMarketplaceOpen(interaction, deps) {
  await interaction.deferReply({ flags: 64 });
  const stockSummaries = await getMarketplaceStockSummaries(deps, interaction.guildId);
  await interaction.editReply({
    content: 'Select the reward you want to purchase.',
    components: buildMarketplaceSelectRows(stockSummaries),
  });
  return true;
}

async function handleMarketplaceConfirmation(interaction, deps, action, token) {
  const state = getPendingConfirmation(token);
  if (!state) {
    await interaction.reply({
      content: 'This marketplace confirmation expired or is no longer valid. Start again from the marketplace panel.',
      flags: 64,
    });
    return true;
  }

  if (state.userId !== interaction.user.id) {
    await interaction.reply({
      content: 'This confirmation is not for you.',
      flags: 64,
    });
    return true;
  }

  const selectedItem = getMarketplaceItem(state.itemKey);
  if (!selectedItem) {
    clearPendingConfirmation(token);
    await interaction.update({
      content: 'This purchase is no longer available.',
      embeds: [],
      components: [],
    });
    return true;
  }

  if (action === 'no') {
    clearPendingConfirmation(token);
    await interaction.update({
      content: 'Purchase cancelled.',
      embeds: [],
      components: [],
    });
    return true;
  }

  if (state.processing || state.completed) {
    await interaction.reply({
      content: 'This purchase is already being processed.',
      flags: 64,
    });
    return true;
  }

  const lockKey = getPurchaseLockKey(interaction.guildId, interaction.user.id);
  if (activePurchaseLocks.has(lockKey)) {
    await interaction.reply({
      content: 'You already have a marketplace purchase in progress. Wait for it to finish, then try again.',
      flags: 64,
    });
    return true;
  }

  state.processing = true;
  activePurchaseLocks.add(lockKey);

  let reservedPurchase = null;
  let deliveryThread = null;
  try {
    await interaction.deferUpdate();

    const wallet = await getLinkedWallet(deps, interaction.guildId, interaction.user.id);
    if (!wallet.walletAddress) {
      clearPendingConfirmation(token);
      await interaction.editReply({
        content: 'You must link a wallet before using the marketplace.',
        embeds: [],
        components: [],
      });
      return true;
    }

    const stock = await getMarketplaceStockForItem(deps, interaction.guildId, selectedItem.key);
    if (stock.soldOut) {
      clearPendingConfirmation(token);
      await interaction.editReply({
        content: 'That reward is sold out for this month. Stock resets at the beginning of next month.',
        embeds: [],
        components: [],
      });
      return true;
    }

    const balanceCheck = await checkUserCharmBalance(deps, interaction.guildId, interaction.user.id, selectedItem.price);
    if (!balanceCheck.ok) {
      clearPendingConfirmation(token);
      await interaction.editReply({
        content: balanceCheck.reason || 'Could not verify your $CHARM balance right now.',
        embeds: [],
        components: [],
      });
      return true;
    }

    if (!balanceCheck.hasEnough) {
      clearPendingConfirmation(token);
      await interaction.editReply({
        content: insufficientBalanceMessage(selectedItem, balanceCheck.balance),
        embeds: [],
        components: [],
      });
      return true;
    }

    const reservation = await reserveMarketplacePurchase(deps, {
      guildId: interaction.guildId,
      userId: interaction.user.id,
      item: selectedItem,
      walletAddress: wallet.walletAddress,
    });

    if (!reservation.ok) {
      clearPendingConfirmation(token);
      await interaction.editReply({
        content: reservation.reason || 'That marketplace purchase cannot be started right now.',
        embeds: [],
        components: [],
      });
      return true;
    }

    reservedPurchase = reservation.purchase;

    let threadResult;
    try {
      threadResult = await createMarketplaceDeliveryThread(interaction, reservedPurchase);
      deliveryThread = threadResult.thread;
      await deliveryThread.send(buildPurchaseThreadIntroPayload(reservedPurchase)).catch((introMessageError) => {
        postMarketplaceLog(deps, {
          guild: interaction.guild,
          category: 'Marketplace Thread Intro Failure',
          purchase: reservedPurchase,
          item: selectedItem,
          status: reservedPurchase.status,
          error: introMessageError,
          extra: deliveryThread ? `Thread: <#${deliveryThread.id}>` : '',
        }).catch(() => null);
      });
      reservedPurchase = await updatePurchaseThread(deps, reservedPurchase.id, deliveryThread.id) || {
        ...reservedPurchase,
        thread_id: deliveryThread.id,
      };
    } catch (threadError) {
      await markPurchaseFailed(deps, reservedPurchase.id, 'failed_thread').catch(() => null);
      await archiveFailedThread(threadError.thread, 'Marketplace purchase cancelled: the buyer could not be added to this private delivery thread.');
      await postMarketplaceLog(deps, {
        guild: interaction.guild,
        category: 'Marketplace Thread Failure',
        purchase: { ...reservedPurchase, status: 'failed_thread' },
        item: selectedItem,
        error: threadError,
      });
      clearPendingConfirmation(token);
      await interaction.editReply({
        content: 'The purchase could not start because the private delivery thread could not be created. No $CHARM was charged.',
        embeds: [],
        components: [],
      });
      return true;
    }

    try {
      await transferCharmToMarketplace(
        deps,
        interaction.guildId,
        interaction.user.id,
        selectedItem,
        balanceCheck.spendable
      );
    } catch (paymentError) {
      const failedPurchase = await markPurchaseFailed(deps, reservedPurchase.id, 'failed_payment').catch(() => null);
      await archiveFailedThread(
        deliveryThread,
        'Marketplace purchase failed: the $CHARM transfer did not complete. No reward should be delivered for this purchase.'
      );
      await postMarketplaceLog(deps, {
        guild: interaction.guild,
        category: 'Marketplace Payment Failure',
        purchase: failedPurchase || { ...reservedPurchase, status: 'failed_payment' },
        item: selectedItem,
        error: paymentError,
      });
      clearPendingConfirmation(token);
      await interaction.editReply({
        content: 'The purchase failed because the $CHARM transfer did not complete. No reward was reserved for delivery.',
        embeds: [],
        components: [],
      });
      return true;
    }

    let paidPurchase = null;
    try {
      paidPurchase = await markPurchasePaid(deps, reservedPurchase.id);
    } catch (paidStatusError) {
      await postMarketplaceLog(deps, {
        guild: interaction.guild,
        category: 'Marketplace Paid Status Failure',
        purchase: reservedPurchase,
        item: selectedItem,
        status: 'reserved_after_payment',
        error: paidStatusError,
        extra: deliveryThread ? `Thread: <#${deliveryThread.id}>` : '',
      });
      await deliveryThread.send({
        content:
          `Payment was received for purchase ${reservedPurchase.id}, but the bot could not mark the purchase paid in the database. ` +
          'Admin review is required before delivery.',
      }).catch(() => null);
      clearPendingConfirmation(token);
      await interaction.editReply({
        content:
          `Your $CHARM transfer appears to have completed, but delivery setup needs admin review. ` +
          `Your private delivery thread is <#${deliveryThread.id}>.`,
        embeds: [],
        components: [],
      });
      return true;
    }

    if (!paidPurchase) {
      await postMarketplaceLog(deps, {
        guild: interaction.guild,
        category: 'Marketplace Paid Status Failure',
        purchase: reservedPurchase,
        item: selectedItem,
        status: 'reserved_after_payment',
        error: new Error('Payment completed, but the purchase row could not be marked paid_pending_delivery.'),
        extra: deliveryThread ? `Thread: <#${deliveryThread.id}>` : '',
      });
      await deliveryThread.send({
        content:
          `Payment was received for purchase ${reservedPurchase.id}, but the purchase status is not ready for admin delivery. ` +
          'Admin review is required before delivery.',
      }).catch(() => null);
      clearPendingConfirmation(token);
      await interaction.editReply({
        content:
          `Your $CHARM transfer appears to have completed, but delivery setup needs admin review. ` +
          `Your private delivery thread is <#${deliveryThread.id}>.`,
        embeds: [],
        components: [],
      });
      return true;
    }
    reservedPurchase = paidPurchase;

    try {
      const deliveryMessage = await deliveryThread.send({
        embeds: [buildDeliveryEmbed(reservedPurchase, {
          buyerUser: interaction.user,
          stock: reservation.stock,
        })],
        components: [buildDeliveryButtonRow(reservedPurchase, false)],
      });
      reservedPurchase = await updatePurchaseDeliveryMessage(deps, reservedPurchase.id, deliveryMessage.id) || reservedPurchase;
      await deliveryThread.send(buildMarketplaceAdminReadyPayload(reservedPurchase)).catch((adminReadyError) => {
        postMarketplaceLog(deps, {
          guild: interaction.guild,
          category: 'Marketplace Admin Ready Message Failure',
          purchase: reservedPurchase,
          item: selectedItem,
          status: reservedPurchase.status,
          error: adminReadyError,
          extra: deliveryThread ? `Thread: <#${deliveryThread.id}>` : '',
        }).catch(() => null);
      });
    } catch (deliveryMessageError) {
      await postMarketplaceLog(deps, {
        guild: interaction.guild,
        category: 'Marketplace Delivery Message Failure',
        purchase: reservedPurchase,
        item: selectedItem,
        error: deliveryMessageError,
        extra: deliveryThread ? `Thread: <#${deliveryThread.id}>` : '',
      });
    }

    if (threadResult?.adminAddFailures?.length) {
      await postMarketplaceLog(deps, {
        guild: interaction.guild,
        category: 'Marketplace Admin Thread Add Warning',
        purchase: reservedPurchase,
        item: selectedItem,
        status: reservedPurchase.status,
        extra: `Failed admin adds:\n${threadResult.adminAddFailures.join('\n').slice(0, 900)}`,
      });
    }

    state.completed = true;
    clearPendingConfirmation(token);

    await interaction.editReply({
      content:
        `Purchase successful - you bought ${selectedItem.name} for ${formatCharm(selectedItem.price)} $CHARM. ` +
        `Your private delivery thread is <#${deliveryThread.id}>.`,
      embeds: [],
      components: [],
    });
    return true;
  } catch (error) {
    clearPendingConfirmation(token);
    await postMarketplaceLog(deps, {
      guild: interaction.guild,
      category: 'Marketplace Purchase Failure',
      purchase: reservedPurchase,
      item: selectedItem,
      userId: interaction.user.id,
      status: reservedPurchase?.status || 'checkout_error',
      error,
      extra: deliveryThread ? `Thread: <#${deliveryThread.id}>` : '',
    });
    await interaction.editReply({
      content: 'The purchase could not be completed right now. Please try again in a moment.',
      embeds: [],
      components: [],
    }).catch(() => null);
    return true;
  } finally {
    state.processing = false;
    activePurchaseLocks.delete(lockKey);
  }
}

async function handleDeliverySentButton(interaction, deps, purchaseId) {
  if (!isMarketplaceAdminInteraction(interaction)) {
    await interaction.reply({ content: 'Admin only.', flags: 64 });
    return true;
  }

  const modal = new ModalBuilder()
    .setCustomId(`marketplace_delivery_sent_modal:${purchaseId}`)
    .setTitle('Confirm Reward Sent');

  modal.addComponents(
    new ActionRowBuilder().addComponents(
      new TextInputBuilder()
        .setCustomId('image_url')
        .setLabel('Image URL optional')
        .setStyle(TextInputStyle.Short)
        .setRequired(false)
        .setPlaceholder('https://...')
    ),
    new ActionRowBuilder().addComponents(
      new TextInputBuilder()
        .setCustomId('opensea_url')
        .setLabel('OpenSea / reward link optional')
        .setStyle(TextInputStyle.Short)
        .setRequired(false)
        .setPlaceholder('https://opensea.io/assets/...')
    ),
    new ActionRowBuilder().addComponents(
      new TextInputBuilder()
        .setCustomId('admin_note')
        .setLabel('Note optional')
        .setStyle(TextInputStyle.Paragraph)
        .setRequired(false)
        .setPlaceholder('Short delivery note')
    )
  );

  await interaction.showModal(modal);
  return true;
}

async function handleMarketplaceButton(interaction, deps) {
  const confirmMatch = interaction.customId.match(/^marketplace_confirm_(yes|no):([a-f0-9]{12})$/i);
  const deliveryMatch = interaction.customId.match(/^marketplace_delivery_sent:(\d+)$/);
  const item = getMarketplaceItem(interaction.customId);
  const isMarketplaceButton = interaction.customId === 'marketplace_open' || item || confirmMatch || deliveryMatch;
  if (!isMarketplaceButton) return false;

  try {
    if (interaction.customId === 'marketplace_open') {
      return await handleMarketplaceOpen(interaction, deps);
    }
    if (item) {
      return await beginMarketplaceCheckout(interaction, deps, item);
    }
    if (confirmMatch) {
      return await handleMarketplaceConfirmation(
        interaction,
        deps,
        String(confirmMatch[1] || '').toLowerCase(),
        String(confirmMatch[2] || '').toLowerCase()
      );
    }
    if (deliveryMatch) {
      return await handleDeliverySentButton(interaction, deps, deliveryMatch[1]);
    }
  } catch (error) {
    await postMarketplaceLog(deps, {
      guild: interaction.guild,
      category: 'Marketplace Button Failure',
      userId: interaction.user?.id,
      status: 'button_error',
      error,
      extra: `Custom ID: ${interaction.customId}`,
    });
    await safeEphemeralReply(interaction, {
      content: 'The marketplace action could not be completed right now. Please try again in a moment.',
    });
    return true;
  }

  return false;
}

async function handleMarketplaceSelectMenu(interaction, deps) {
  if (interaction.customId !== 'marketplace_select_item') return false;

  try {
    const selectedKey = String(interaction.values?.[0] || '').trim();
    const item = getMarketplaceItem(selectedKey);
    if (!item) {
      await interaction.reply({
        content: 'That marketplace item is no longer available.',
        flags: 64,
      });
      return true;
    }
    return await beginMarketplaceCheckout(interaction, deps, item);
  } catch (error) {
    await postMarketplaceLog(deps, {
      guild: interaction.guild,
      category: 'Marketplace Select Failure',
      userId: interaction.user?.id,
      status: 'select_error',
      error,
      extra: `Custom ID: ${interaction.customId}`,
    });
    await safeEphemeralReply(interaction, {
      content: 'The marketplace selection could not be completed right now. Please try again in a moment.',
    });
    return true;
  }
}

async function updateOriginalDeliveryMessage(interaction, deps, purchase) {
  if (!purchase.delivery_message_id || !purchase.thread_id) return;
  const thread = await interaction.client.channels.fetch(purchase.thread_id).catch(() => null);
  if (!thread?.isTextBased?.()) return;
  const message = await thread.messages.fetch(purchase.delivery_message_id).catch(() => null);
  if (!message) return;
  const stock = await getMarketplaceStockForItem(deps, purchase.guild_id, purchase.item_key).catch(() => null);
  await message.edit({
    embeds: [buildDeliveryEmbed(purchase, { stock, delivered: true })],
    components: [buildDeliveryButtonRow(purchase, true)],
  });
}

async function handleMarketplaceModalSubmit(interaction, deps) {
  const modalMatch = interaction.customId.match(/^marketplace_delivery_sent_modal:(\d+)$/);
  if (!modalMatch) return false;

  if (!isMarketplaceAdminInteraction(interaction)) {
    await interaction.reply({ content: 'Admin only.', flags: 64 });
    return true;
  }

  const purchaseId = modalMatch[1];
  const imageUrl = String(interaction.fields.getTextInputValue('image_url') || '').trim();
  const rewardUrl = String(interaction.fields.getTextInputValue('opensea_url') || '').trim();
  const adminNote = String(interaction.fields.getTextInputValue('admin_note') || '').trim();

  if (imageUrl && !isHttpUrl(imageUrl)) {
    await interaction.reply({ content: 'Image URL must start with http:// or https://.', flags: 64 });
    return true;
  }
  if (rewardUrl && !isHttpUrl(rewardUrl)) {
    await interaction.reply({ content: 'OpenSea / reward link must start with http:// or https://.', flags: 64 });
    return true;
  }

  await interaction.deferReply({ flags: 64 });

  let purchase = null;
  try {
    purchase = await getMarketplacePurchaseById(deps, purchaseId);
    if (!purchase) {
      await interaction.editReply({ content: 'Purchase not found.' });
      return true;
    }
    if (String(purchase.status) === 'delivered') {
      await interaction.editReply({ content: 'This purchase has already been marked delivered.' });
      return true;
    }
    if (String(purchase.status) !== 'paid_pending_delivery') {
      await interaction.editReply({ content: `This purchase is not pending delivery. Current status: ${purchase.status}.` });
      return true;
    }

    const deliveredPurchase = await markPurchaseDelivered(deps, purchaseId, {
      imageUrl,
      rewardUrl,
      adminNote,
      adminId: interaction.user.id,
    });
    if (!deliveredPurchase) {
      const refreshed = await getMarketplacePurchaseById(deps, purchaseId);
      await interaction.editReply({
        content: refreshed?.status === 'delivered'
          ? 'This purchase has already been marked delivered.'
          : 'This purchase could not be marked delivered because its status changed.',
      });
      return true;
    }
    purchase = deliveredPurchase;

    const thread = purchase.thread_id
      ? await interaction.client.channels.fetch(purchase.thread_id).catch(() => null)
      : interaction.channel;
    if (thread?.isTextBased?.()) {
      await thread.send(buildDeliveryConfirmationPayload(purchase, interaction.user.id)).catch((threadError) => {
        postMarketplaceLog(deps, {
          guild: interaction.guild,
          category: 'Marketplace Delivery Thread Notice Failure',
          purchase,
          status: purchase.status,
          error: threadError,
        }).catch(() => null);
      });
    }

    await updateOriginalDeliveryMessage(interaction, deps, purchase).catch((messageError) => {
      postMarketplaceLog(deps, {
        guild: interaction.guild,
        category: 'Marketplace Sent Button Update Failure',
        purchase,
        status: purchase.status,
        error: messageError,
      }).catch(() => null);
    });

    await postDeliveryNotice(deps, interaction.guild, purchase, interaction.user.id).catch((noticeError) => {
      postMarketplaceLog(deps, {
        guild: interaction.guild,
        category: 'Marketplace Delivery Notice Failure',
        purchase,
        status: purchase.status,
        error: noticeError,
      }).catch(() => null);
    });

    await interaction.editReply({ content: 'Reward marked delivered.' });
    return true;
  } catch (error) {
    await postMarketplaceLog(deps, {
      guild: interaction.guild,
      category: 'Marketplace Delivery Modal Failure',
      purchase,
      purchaseId,
      userId: interaction.user?.id,
      status: purchase?.status || 'modal_error',
      error,
    });
    await interaction.editReply({ content: 'The reward could not be marked delivered right now.' }).catch(() => null);
    return true;
  }
}

module.exports = {
  MARKETPLACE_ITEMS,
  MARKETPLACE_MONTHLY_CAPS,
  MARKETPLACE_NOTICE_CHANNEL_ID,
  MARKETPLACE_THREAD_PARENT_CHANNEL_ID,
  MARKETPLACE_ADMIN_USER_IDS,
  MARKETPLACE_ADMIN_ROLE_IDS,
  MARKETPLACE_TIME_ZONE,
  MARKETPLACE_RECEIPT_CHANNEL_ID,
  buildMarketplaceSlashCommand,
  formatCharm,
  getMarketplaceItem,
  getMarketplaceMonthKey,
  getMarketplaceCap,
  getMarketplaceStockState,
  isHttpUrl,
  isDirectImageUrl,
  ensureMarketplaceTables,
  getMarketplaceStockForItem,
  getMarketplaceStockSummaries,
  getLinkedWallet,
  getUserCharmBalance,
  checkUserCharmBalance,
  transferCharmToMarketplace,
  sendMarketplaceReceipt,
  handleMarketplaceCommand,
  handleMarketplaceButton,
  handleMarketplaceSelectMenu,
  handleMarketplaceModalSubmit,
};
