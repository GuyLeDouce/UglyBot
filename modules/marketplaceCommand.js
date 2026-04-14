const crypto = require('crypto');
const {
  SlashCommandBuilder,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
} = require('discord.js');

const MARKETPLACE_RECEIPT_CHANNEL_ID = '1493624608035704912';
const MARKETPLACE_TICKET_CHANNEL_ID = '1324090267699122258';
const CONFIRMATION_TTL_MS = 5 * 60 * 1000;

const MARKETPLACE_ITEMS = [
  {
    key: 'charm',
    buttonId: 'marketplace_buy_charm',
    confirmationKey: 'charm',
    name: 'Random Charm of the Ugly',
    price: 15000,
    buttonLabel: 'COTU',
  },
  {
    key: 'common',
    buttonId: 'marketplace_buy_common',
    confirmationKey: 'common',
    name: 'Random Common Squig',
    price: 20000,
    buttonLabel: 'Common',
  },
  {
    key: 'uncommon',
    buttonId: 'marketplace_buy_uncommon',
    confirmationKey: 'uncommon',
    name: 'Random Uncommon Squig',
    price: 30000,
    buttonLabel: 'Uncommon',
  },
  {
    key: 'monster',
    buttonId: 'marketplace_buy_monster',
    confirmationKey: 'monster',
    name: 'Random Ugly Monster',
    price: 45000,
    buttonLabel: 'Monster',
  },
  {
    key: 'rare',
    buttonId: 'marketplace_buy_rare',
    confirmationKey: 'rare',
    name: 'Random Rare Squig',
    price: 90000,
    buttonLabel: 'Rare',
  },
  {
    key: 'epic',
    buttonId: 'marketplace_buy_epic',
    confirmationKey: 'epic',
    name: 'Random Epic Squig',
    price: 250000,
    buttonLabel: 'Epic',
  },
  {
    key: 'custom',
    buttonId: 'marketplace_buy_custom',
    confirmationKey: 'custom',
    name: 'Custom Squig Edition',
    price: 1000000,
    buttonLabel: 'Custom Edition',
  },
];

const pendingConfirmations = new Map();
const activePurchaseLocks = new Set();

// Slash command definition for the fixed Malformed Marketplace panel.
function buildMarketplaceSlashCommand() {
  return new SlashCommandBuilder()
    .setName('marketplace')
    .setDescription('Post the Malformed Marketplace purchase panel');
}

function formatCharm(amount) {
  return new Intl.NumberFormat('en-US').format(Math.floor(Number(amount) || 0));
}

function getMarketplaceItem(itemKey) {
  return MARKETPLACE_ITEMS.find((item) =>
    item.key === itemKey ||
    item.buttonId === itemKey ||
    item.confirmationKey === itemKey
  ) || null;
}

function buildMarketplacePanelEmbed() {
  const fieldGroups = [
    MARKETPLACE_ITEMS.slice(0, 3),
    MARKETPLACE_ITEMS.slice(3, 5),
    MARKETPLACE_ITEMS.slice(5),
  ];

  return new EmbedBuilder()
    .setTitle('Malformed Marketplace')
    .setColor(0x6ac4a6)
    .setDescription(
      'Spend your $CHARM on marketplace rewards. Pick an item below to start a private checkout flow. After payment, open a ticket to claim your reward.'
    )
    .addFields(
      ...fieldGroups.map((group, index) => ({
        name: index === 0 ? 'Rewards' : '\u200b',
        value: group.map((item) => `**${item.buttonLabel}**\n${formatCharm(item.price)} $CHARM`).join('\n\n'),
        inline: true,
      })),
      {
        name: 'How It Works',
        value:
          '1. Click a button.\n' +
          '2. Confirm the purchase privately.\n' +
          `3. Open a ticket in <#${MARKETPLACE_TICKET_CHANNEL_ID}> after checkout.`,
        inline: false,
      }
    )
    .setFooter({
      text: 'Linked wallet required. Delivery is handled manually through tickets.',
    });
}

function buildMarketplaceItemRows() {
  const rowGroups = [
    MARKETPLACE_ITEMS.slice(0, 3),
    MARKETPLACE_ITEMS.slice(3, 5),
    MARKETPLACE_ITEMS.slice(5),
  ];

  return rowGroups.map((group) =>
    new ActionRowBuilder().addComponents(
      ...group.map((item) =>
        new ButtonBuilder()
          .setCustomId(item.buttonId)
          .setLabel(item.buttonLabel)
          .setStyle(item.key === 'custom' ? ButtonStyle.Secondary : ButtonStyle.Primary)
      )
    )
  );
}

function buildConfirmationEmbed(item) {
  return new EmbedBuilder()
    .setColor(0xe6b422)
    .setTitle('Confirm Purchase')
    .setDescription(
      `You are about to purchase ${item.name} for ${formatCharm(item.price)} $CHARM. Would you like to proceed?`
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

// Economy wrapper around the bot's existing DRIP marketplace balance lookup.
async function getUserCharmBalance(deps, guildId, userId) {
  const spendable = await deps.getMarketplaceSpendableBalance(guildId, userId);
  if (!spendable.ok) return spendable;

  const balance = await deps.getDripMemberCurrencyBalance(
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

// Economy wrapper around the bot's existing DRIP transfer flow.
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

// Admin receipt log for successful marketplace purchases.
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
    await deps.postAdminSystemLog({
      guild,
      category: 'Marketplace Receipt Failure',
      message:
        `User: <@${user.id}>\n` +
        `Item: **${item.name}**\n` +
        `Reason: ${String(error?.message || error || '').slice(0, 500)}`,
    });
  }
}

async function handleMarketplaceCommand(interaction) {
  await interaction.reply({
    embeds: [buildMarketplacePanelEmbed()],
    components: buildMarketplaceItemRows(),
  });
}

// Handles public buy buttons and the private yes/no confirmation flow.
async function handleMarketplaceButton(interaction, deps) {
  const item = getMarketplaceItem(interaction.customId);
  if (item) {
    const token = createConfirmation(interaction, item);
    await interaction.reply({
      embeds: [buildConfirmationEmbed(item)],
      components: buildConfirmationRows(token),
      flags: 64,
    });
    return true;
  }

  const confirmMatch = interaction.customId.match(/^marketplace_confirm_(yes|no):([a-f0-9]{12})$/i);
  if (!confirmMatch) return false;

  const action = String(confirmMatch[1] || '').toLowerCase();
  const token = String(confirmMatch[2] || '').toLowerCase();
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

  try {
    await interaction.deferUpdate();

    const wallet = await getLinkedWallet(deps, interaction.guildId, interaction.user.id);
    if (!wallet.walletAddress) {
      clearPendingConfirmation(token);
      await interaction.editReply({
        content: 'You must link a wallet first before using the marketplace.',
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
        content: 'You do not have enough $CHARM to purchase this item.',
        embeds: [],
        components: [],
      });
      return true;
    }

    await transferCharmToMarketplace(
      deps,
      interaction.guildId,
      interaction.user.id,
      selectedItem,
      balanceCheck.spendable
    );

    state.completed = true;
    clearPendingConfirmation(token);

    const member = await interaction.guild.members.fetch(interaction.user.id).catch(() => null);
    await sendMarketplaceReceipt(deps, interaction.guild, interaction.user, member, selectedItem, wallet.walletAddress);

    await interaction.editReply({
      content:
        `Purchase successful. You bought ${selectedItem.name} for ${formatCharm(selectedItem.price)} $CHARM. ` +
        `Please open a ticket in <#${MARKETPLACE_TICKET_CHANNEL_ID}> to complete your claim.`,
      embeds: [],
      components: [],
    });
    return true;
  } catch (error) {
    clearPendingConfirmation(token);
    await deps.postAdminSystemLog({
      guild: interaction.guild,
      category: 'Marketplace Purchase Failure',
      message:
        `User: <@${interaction.user.id}>\n` +
        `Item: **${selectedItem.name}**\n` +
        `Wallet: \`${(await getLinkedWallet(deps, interaction.guildId, interaction.user.id).catch(() => ({ walletAddress: null }))).walletAddress || 'unknown'}\`\n` +
        `Reason: ${String(error?.message || error || '').slice(0, 900)}`,
    });
    await interaction.editReply({
      content: 'The purchase could not be completed right now. Please try again in a moment.',
      embeds: [],
      components: [],
    });
    return true;
  } finally {
    state.processing = false;
    activePurchaseLocks.delete(lockKey);
  }
}

module.exports = {
  MARKETPLACE_ITEMS,
  MARKETPLACE_RECEIPT_CHANNEL_ID,
  MARKETPLACE_TICKET_CHANNEL_ID,
  buildMarketplaceSlashCommand,
  getMarketplaceItem,
  getLinkedWallet,
  getUserCharmBalance,
  checkUserCharmBalance,
  transferCharmToMarketplace,
  sendMarketplaceReceipt,
  handleMarketplaceCommand,
  handleMarketplaceButton,
};
