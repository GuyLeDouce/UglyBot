const crypto = require('crypto');
const {
  SlashCommandBuilder,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  StringSelectMenuBuilder,
  UserSelectMenuBuilder,
  ChannelType,
  PermissionFlagsBits,
} = require('discord.js');

const SQUIGS_CONTRACT = String(
  process.env.SQUIG_COLLECTION_CONTRACT || '0x9bf567ddf41b425264626d1b8b2c7f7c660b1c42'
).toLowerCase();
const SQUIG_IMAGE_BASE = 'https://assets.bueno.art/images/a49527dc-149c-4cbc-9038-d4b0d1dbf0b2/default';
const SQUIG_DUEL_MENU_IMAGE = 'https://i.imgur.com/KPAnMG3.png';
const BOT_DUEL_WAGER = 50;
const MAX_SELECT_OPTIONS = 25;
const ACCEPT_TIMEOUT_MS = Number(process.env.SQUIG_DUEL_ACCEPT_TIMEOUT_MS || 10 * 60 * 1000);
const SETUP_TIMEOUT_MS = Number(process.env.SQUIG_DUEL_SETUP_TIMEOUT_MS || 10 * 60 * 1000);
const ROUND_TIMEOUT_MS = Number(process.env.SQUIG_DUEL_ROUND_TIMEOUT_MS || 30 * 1000);
const SUDDEN_DEATH_AFTER_ROUND = 6;
const SUDDEN_DEATH_DAMAGE = 8;

let deps = null;

const duels = new Map();
const activeUserToDuel = new Map();
const pendingSquigSelections = new Map();
const pendingSquigViews = new Map();

function initSquigDuels(injectedDeps) {
  deps = injectedDeps;
}

function assertReady() {
  if (!deps) throw new Error('Squig Duels module not initialized. Call initSquigDuels first.');
}

async function ensureSquigDuelSchema(pool) {
  if (!pool?.query) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS squig_duels (
      id TEXT PRIMARY KEY,
      guild_id TEXT NOT NULL,
      channel_id TEXT NOT NULL,
      thread_id TEXT,
      challenger_id TEXT NOT NULL,
      opponent_id TEXT,
      wager_amount NUMERIC,
      challenger_squig_token_id TEXT,
      opponent_squig_token_id TEXT,
      challenger_ugly_points NUMERIC,
      opponent_ugly_points NUMERIC,
      winner_id TEXT,
      status TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      completed_at TIMESTAMPTZ
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS squig_duel_rounds (
      id BIGSERIAL PRIMARY KEY,
      duel_id TEXT NOT NULL REFERENCES squig_duels(id) ON DELETE CASCADE,
      round_number INTEGER NOT NULL,
      challenger_action TEXT,
      opponent_action TEXT,
      challenger_hp INTEGER,
      opponent_hp INTEGER,
      result_text TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
}

function buildSquigDuelSlashCommand() {
  return new SlashCommandBuilder()
    .setName('squigdual')
    .setDescription('Admin: post the Squig Duels game menu');
}

function isAdmin(interactionOrMember) {
  if (typeof deps?.isAdmin === 'function') return deps.isAdmin(interactionOrMember);
  return Boolean(interactionOrMember?.memberPermissions?.has(PermissionFlagsBits.ManageGuild));
}

function holderRoleId() {
  return String(process.env.HOLDER_ROLE_ID || '').trim();
}

function botUserId() {
  return deps?.client?.user?.id || deps?.clientUserId || null;
}

function formatCharm(amount) {
  return new Intl.NumberFormat('en-US').format(Math.floor(Number(amount) || 0));
}

function squigImageUrl(tokenId) {
  const tid = String(tokenId || '').trim();
  if (!/^\d+$/.test(tid)) return null;
  return `${SQUIG_IMAGE_BASE}/${tid}`;
}

function balancedHp(uglyPoints) {
  return Math.round(50 + Math.sqrt(Math.max(0, Number(uglyPoints) || 0) * 10));
}

function baseAttack(uglyPoints) {
  return Math.max(1, Math.round(8 + ((Number(uglyPoints) || 0) / 25)));
}

function randomId() {
  return crypto.randomBytes(6).toString('hex');
}

function activeStatuses() {
  return new Set(['setup', 'awaiting_accept', 'active']);
}

function registerActiveUser(userId, duelId) {
  activeUserToDuel.set(String(userId), duelId);
}

function releaseDuelUsers(duel) {
  if (duel?.challengerId) activeUserToDuel.delete(String(duel.challengerId));
  if (duel?.opponentId) activeUserToDuel.delete(String(duel.opponentId));
}

function isBotDuel(duel) {
  return Boolean(duel?.isBotDuel);
}

function randomBotAction() {
  const weighted = ['attack', 'attack', 'defend', 'heal', 'panic'];
  return weighted[Math.floor(Math.random() * weighted.length)];
}

function getDuel(id) {
  const duel = duels.get(String(id || ''));
  if (!duel) return null;
  return duel;
}

async function logDuel(guild, category, message) {
  console.log(`[SquigDuels] ${category}: ${message}`);
  try {
    if (typeof deps?.postAdminSystemLog === 'function') {
      await deps.postAdminSystemLog({ guild, category: `Squig Duels - ${category}`, message });
    }
  } catch (err) {
    console.warn('[SquigDuels] log failed:', String(err?.message || err || ''));
  }
}

async function persistDuel(duel) {
  const pool = deps?.historyPool;
  if (!pool?.query || !duel) return;
  try {
    await pool.query(
      `INSERT INTO squig_duels (
         id, guild_id, channel_id, thread_id, challenger_id, opponent_id, wager_amount,
         challenger_squig_token_id, opponent_squig_token_id,
         challenger_ugly_points, opponent_ugly_points, winner_id, status, updated_at, completed_at
       )
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW(),$14)
       ON CONFLICT (id) DO UPDATE SET
         thread_id = EXCLUDED.thread_id,
         opponent_id = EXCLUDED.opponent_id,
         wager_amount = EXCLUDED.wager_amount,
         challenger_squig_token_id = EXCLUDED.challenger_squig_token_id,
         opponent_squig_token_id = EXCLUDED.opponent_squig_token_id,
         challenger_ugly_points = EXCLUDED.challenger_ugly_points,
         opponent_ugly_points = EXCLUDED.opponent_ugly_points,
         winner_id = EXCLUDED.winner_id,
         status = EXCLUDED.status,
         updated_at = NOW(),
         completed_at = EXCLUDED.completed_at`,
      [
        duel.id,
        duel.guildId,
        duel.channelId,
        duel.threadId || null,
        duel.challengerId,
        duel.opponentId || null,
        duel.wagerAmount || null,
        duel.challengerSquigTokenId || null,
        duel.opponentSquigTokenId || null,
        duel.challengerUglyPoints || null,
        duel.opponentUglyPoints || null,
        duel.winnerId || null,
        duel.status,
        duel.completedAt ? new Date(duel.completedAt) : null,
      ]
    );
  } catch (err) {
    console.warn('[SquigDuels] persist duel failed:', String(err?.message || err || ''));
  }
}

async function persistRound(duel, result) {
  const pool = deps?.historyPool;
  if (!pool?.query || !duel || !result) return;
  try {
    await pool.query(
      `INSERT INTO squig_duel_rounds
       (duel_id, round_number, challenger_action, opponent_action, challenger_hp, opponent_hp, result_text)
       VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [
        duel.id,
        result.round,
        result.actions?.challenger || null,
        result.actions?.opponent || null,
        duel.challengerCurrentHp,
        duel.opponentCurrentHp,
        String(result.lines?.join('\n') || '').slice(0, 4000),
      ]
    );
  } catch (err) {
    console.warn('[SquigDuels] persist round failed:', String(err?.message || err || ''));
  }
}

function buildMenuEmbed() {
  return new EmbedBuilder()
    .setTitle('⚔️ Squig Duels')
    .setColor(0xd4a43b)
    .setDescription(
      'Challenge another holder, wager $CHARM, choose one of your own Squigs, and battle using UglyPoints-powered HP.\n\n' +
      '**How it works:**\n' +
      '1. Click Start Duel\n' +
      '2. Pick an opponent\n' +
      '3. Choose your $CHARM wager\n' +
      '4. Select one of your wallet-linked Squigs\n' +
      '5. Opponent accepts and matches the wager\n' +
      '6. Both Squigs battle round by round until one hits 0 HP\n\n' +
      '**Stats:**\n' +
      '- Squig HP is based on UglyPoints\n' +
      '- Higher UglyPoints helps, but strategy matters\n' +
      '- Every round you choose Attack, Defend, Heal, or Panic\n\n' +
      '**Actions:**\n' +
      'Attack — damage your opponent unless blocked\n' +
      'Defend — block attacks and stop enemy healing\n' +
      'Heal — recover HP unless stopped\n' +
      'Panic — force chaos, but both Squigs lose HP'
    )
    .setImage(SQUIG_DUEL_MENU_IMAGE)
    .setFooter({ text: 'Use your Squig wisely. The portal remembers everything.' });
}

function buildMenuRow() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId('sd:start')
      .setLabel('⚔️ Start Duel')
      .setStyle(ButtonStyle.Danger),
    new ButtonBuilder()
      .setCustomId('sd:view')
      .setLabel('View Squigs')
      .setStyle(ButtonStyle.Secondary),
    new ButtonBuilder()
      .setCustomId('sd:bot')
      .setLabel('Bot Duel')
      .setStyle(ButtonStyle.Primary)
  );
}

async function handleCommand(interaction) {
  if (interaction.commandName !== 'squigdual') return false;
  assertReady();
  if (!isAdmin(interaction)) {
    await interaction.reply({ content: 'Admin only.', flags: 64 });
    return true;
  }
  await interaction.channel.send({ embeds: [buildMenuEmbed()], components: [buildMenuRow()] });
  await interaction.reply({ content: 'Squig Duels menu posted.', flags: 64 });
  return true;
}

async function hasHolderRole(guild, userId) {
  const roleId = holderRoleId();
  if (!roleId) return true;
  const member = await guild.members.fetch(userId).catch(() => null);
  return Boolean(member?.roles?.cache?.has(roleId));
}

function parseWager(input) {
  const raw = String(input || '').replace(/,/g, '').trim();
  if (!/^\d+$/.test(raw)) return null;
  const amount = Number(raw);
  if (!Number.isSafeInteger(amount) || amount <= 0) return null;
  return amount;
}

function challengerSetupModal(duelId) {
  const modal = new ModalBuilder()
    .setCustomId(`sd:setup:${duelId}`)
    .setTitle('Set Squig Duel Wager');
  modal.addComponents(
    new ActionRowBuilder().addComponents(
      new TextInputBuilder()
        .setCustomId('wager')
        .setLabel('Wager amount in $CHARM')
        .setRequired(true)
        .setPlaceholder('100')
        .setStyle(TextInputStyle.Short)
    )
  );
  return modal;
}

function opponentSelectRows(duelId) {
  return [
    new ActionRowBuilder().addComponents(
      new UserSelectMenuBuilder()
        .setCustomId(`sd:opponent:${duelId}`)
        .setPlaceholder('Start typing to select your opponent')
        .setMinValues(1)
        .setMaxValues(1)
    ),
  ];
}

function createBaseDuel({ interaction, duelId, thread, opponentId = null, wagerAmount = null, isBotDuel = false }) {
  return {
    id: duelId,
    guildId: interaction.guild.id,
    channelId: interaction.channel.id,
    threadId: thread.id,
    challengerId: interaction.user.id,
    opponentId,
    wagerAmount,
    challengerPaid: false,
    opponentPaid: false,
    challengerSquigTokenId: null,
    opponentSquigTokenId: null,
    challengerUglyPoints: null,
    opponentUglyPoints: null,
    challengerMaxHp: null,
    opponentMaxHp: null,
    challengerCurrentHp: null,
    opponentCurrentHp: null,
    currentRound: 0,
    currentActions: {},
    status: 'setup',
    isBotDuel,
    createdAt: Date.now(),
    setupTimeout: null,
    acceptTimeout: null,
    roundTimeout: null,
    processingRound: false,
  };
}

function armSetupTimeout(guild, duel) {
  if (duel.setupTimeout) clearTimeout(duel.setupTimeout);
  duel.setupTimeout = setTimeout(() => {
    const active = getDuel(duel.id);
    if (!active || active.status !== 'setup') return;
    cancelDuel(guild, active, 'Challenger did not complete setup in time.').catch((err) => {
      console.warn('[SquigDuels] setup timeout cancel failed:', String(err?.message || err || ''));
    });
  }, SETUP_TIMEOUT_MS);
}

async function handleStartButton(interaction) {
  if (interaction.customId !== 'sd:start') return false;
  assertReady();

  if (activeUserToDuel.has(interaction.user.id)) {
    await interaction.reply({ content: 'You are already in an active Squig Duel.', flags: 64 });
    return true;
  }
  if (!(await hasHolderRole(interaction.guild, interaction.user.id))) {
    await interaction.reply({ content: 'Only holders can start a Squig Duel.', flags: 64 });
    return true;
  }
  if (!interaction.channel?.threads?.create) {
    await interaction.reply({ content: 'This channel cannot create duel threads.', flags: 64 });
    return true;
  }
  const duelId = randomId();
  const thread = await interaction.channel.threads.create({
    name: `Squig Duel-${interaction.user.username}`.slice(0, 90),
    autoArchiveDuration: 1440,
    type: ChannelType.PublicThread,
    reason: `Squig Duel created by ${interaction.user.tag}`,
  });

  const duel = createBaseDuel({ interaction, duelId, thread });
  duels.set(duelId, duel);
  registerActiveUser(interaction.user.id, duelId);
  armSetupTimeout(interaction.guild, duel);
  await thread.members.add(interaction.user.id).catch(() => null);
  await persistDuel(duel);
  await logDuel(interaction.guild, 'Created', `Duel \`${duelId}\` created by <@${interaction.user.id}> in <#${thread.id}>.`);

  await thread.send(
    `<@${interaction.user.id}> started a Squig Duel setup.\n` +
    `Holders can spectate here. Only duel participants and admins should write in this thread.`
  );
  await interaction.reply({
    content: 'Select your opponent. Start typing their name in the picker below.',
    components: opponentSelectRows(duelId),
    flags: 64,
  });
  return true;
}

async function handleBotDuelButton(interaction) {
  if (interaction.customId !== 'sd:bot') return false;
  assertReady();

  if (activeUserToDuel.has(interaction.user.id)) {
    await interaction.reply({ content: 'You are already in an active Squig Duel.', flags: 64 });
    return true;
  }
  if (!interaction.channel?.threads?.create) {
    await interaction.reply({ content: 'This channel cannot create duel threads.', flags: 64 });
    return true;
  }
  if (!botUserId()) {
    await interaction.reply({ content: 'Bot duel is unavailable until the bot user is ready.', flags: 64 });
    return true;
  }
  await interaction.deferReply({ flags: 64 });

  const duelId = randomId();
  const thread = await interaction.channel.threads.create({
    name: `Bot Squig Duel-${interaction.user.username}`.slice(0, 90),
    autoArchiveDuration: 1440,
    type: ChannelType.PublicThread,
    reason: `Bot Squig Duel created by ${interaction.user.tag}`,
  });

  const duel = createBaseDuel({
    interaction,
    duelId,
    thread,
    opponentId: botUserId(),
    wagerAmount: BOT_DUEL_WAGER,
    isBotDuel: true,
  });
  duels.set(duelId, duel);
  registerActiveUser(interaction.user.id, duelId);
  armSetupTimeout(interaction.guild, duel);

  await thread.members.add(interaction.user.id).catch(() => null);
  await persistDuel(duel);
  await logDuel(interaction.guild, 'Bot Duel Created', `Bot duel \`${duelId}\` created by <@${interaction.user.id}> in <#${thread.id}>.`);
  await thread.send(
    `<@${interaction.user.id}> started a bot Squig Duel.\n` +
    `Test wager: ${BOT_DUEL_WAGER} $CHARM. The wager is returned after the duel no matter who wins.`
  );
  await promptSquigSelection(interaction, duel, 'challenger');
  return true;
}

async function getSpendable(guildId, userId) {
  const result = await deps.getMarketplaceSpendableBalance(guildId, userId);
  if (!result.ok) return result;

  const resolvedMemberBalance = deps.extractDripCurrencyAmountFromPayload(
    result.resolvedMember || null,
    result.settings.currency_id
  );
  const balance = resolvedMemberBalance != null
    ? resolvedMemberBalance
    : await deps.getDripMemberCurrencyBalance(
        result.settings.drip_realm_id,
        result.memberIds,
        result.settings.currency_id,
        result.settings
      );

  if (!Number.isFinite(Number(balance))) {
    return { ok: false, reason: 'Could not check your $CHARM balance right now.' };
  }
  return { ...result, ok: true, balance: Math.floor(Number(balance)) };
}

async function collectWager(guild, userId, amount, context) {
  const spendable = await getSpendable(guild.id, userId);
  if (!spendable.ok) return spendable;
  if (spendable.balance < amount) {
    return { ok: false, reason: `You need ${formatCharm(amount)} $CHARM for this wager.` };
  }
  await deps.awardDripPoints(
    spendable.settings.drip_realm_id,
    [spendable.botMemberId],
    amount,
    spendable.settings.currency_id,
    spendable.settings,
    {
      context,
      initiatorDiscordId: userId,
      recipientDiscordId: botUserId(),
      recipientMemberIdOverride: spendable.botMemberId,
      senderMemberIdOverride: spendable.memberIds[0],
    }
  );
  await logDuel(guild, 'Wager Collected', `<@${userId}> escrowed ${formatCharm(amount)} $CHARM for ${context}.`);
  return { ok: true, spendable };
}

async function transferFromBot(guild, userId, amount, context, initiatorDiscordId = null) {
  const spendable = await deps.getMarketplaceSpendableBalance(guild.id, userId);
  if (!spendable.ok) return spendable;
  if (!spendable.memberIds?.length) return { ok: false, reason: 'No DRIP member ID found for recipient.' };
  await deps.awardDripPoints(
    spendable.settings.drip_realm_id,
    spendable.memberIds,
    amount,
    spendable.settings.currency_id,
    spendable.settings,
    {
      context,
      initiatorDiscordId: initiatorDiscordId || botUserId(),
      recipientDiscordId: userId,
      senderMemberIdOverride: spendable.botMemberId,
    }
  );
  await logDuel(guild, context, `Sent ${formatCharm(amount)} $CHARM to <@${userId}>.`);
  return { ok: true };
}

async function refundPaidWagers(guild, duel, reason) {
  const failures = [];
  if (duel.challengerPaid) {
    try {
      await transferFromBot(guild, duel.challengerId, duel.wagerAmount, 'squig_duel_refund', botUserId());
      duel.challengerPaid = false;
    } catch (err) {
      failures.push(`<@${duel.challengerId}>: ${String(err?.message || err || '').slice(0, 180)}`);
    }
  }
  if (duel.opponentPaid) {
    try {
      await transferFromBot(guild, duel.opponentId, duel.wagerAmount, 'squig_duel_refund', botUserId());
      duel.opponentPaid = false;
    } catch (err) {
      failures.push(`<@${duel.opponentId}>: ${String(err?.message || err || '').slice(0, 180)}`);
    }
  }
  await logDuel(guild, 'Refund', `Duel \`${duel.id}\` refund reason: ${reason}. Failures: ${failures.join(' | ') || 'none'}`);
  return failures;
}

async function cancelDuel(guild, duel, reason) {
  if (!duel || duel.status === 'cancelled' || duel.status === 'completed') return;
  if (duel.setupTimeout) clearTimeout(duel.setupTimeout);
  if (duel.acceptTimeout) clearTimeout(duel.acceptTimeout);
  if (duel.roundTimeout) clearTimeout(duel.roundTimeout);
  const failures = await refundPaidWagers(guild, duel, reason);
  duel.status = 'cancelled';
  duel.completedAt = Date.now();
  releaseDuelUsers(duel);
  await persistDuel(duel);
  const thread = await guild.channels.fetch(duel.threadId).catch(() => null);
  if (thread?.isTextBased()) {
    await thread.send(
      `Squig Duel cancelled: ${reason}` +
      (failures.length ? `\nRefund issue(s): ${failures.join(' | ')}` : '')
    ).catch(() => null);
  }
}

async function tryDirectUglyPointLookup(tokenId) {
  const pool = deps?.pointsPool;
  if (!pool?.query) return null;
  for (const tableName of ['holder_points_mapping', 'holder_point_mapping', 'holder_point_mappings']) {
    try {
      const reg = await pool.query(`SELECT to_regclass($1) AS table_name`, [tableName]);
      if (!reg.rows[0]?.table_name) continue;
      const colsRes = await pool.query(
        `SELECT column_name FROM information_schema.columns WHERE table_name = $1`,
        [tableName]
      );
      const cols = new Set(colsRes.rows.map((r) => String(r.column_name || '').toLowerCase()));
      if (!cols.has('token_id')) continue;
      const pointCol = ['ugly_points', 'uglypoints', 'points', 'total_points', 'total_ugly_points'].find((c) => cols.has(c));
      if (!pointCol) continue;
      const hasContract = cols.has('contract_address');
      const query = hasContract
        ? `SELECT ${pointCol} AS ugly_points FROM ${tableName} WHERE token_id::text = $1 AND LOWER(contract_address) = $2 LIMIT 1`
        : `SELECT ${pointCol} AS ugly_points FROM ${tableName} WHERE token_id::text = $1 LIMIT 1`;
      const params = hasContract ? [String(tokenId), SQUIGS_CONTRACT] : [String(tokenId)];
      const { rows } = await pool.query(query, params);
      const n = Number(rows[0]?.ugly_points);
      if (Number.isFinite(n) && n >= 0) return Math.floor(n);
    } catch (err) {
      console.warn('[SquigDuels] direct UglyPoints lookup skipped:', String(err?.message || err || ''));
    }
  }
  return null;
}

async function calculateUglyPoints(guildId, tokenId) {
  const direct = await tryDirectUglyPointLookup(tokenId);
  if (direct != null) return direct;
  const mappings = await deps.getGuildPointMappings(guildId);
  const table = deps.hpTableForContract(SQUIGS_CONTRACT, mappings);
  if (!table || !Object.keys(table).length) return null;
  const meta = await deps.getNftMetadataAlchemy(tokenId, SQUIGS_CONTRACT);
  const { attrs } = await deps.getTraitsForToken(meta, tokenId, SQUIGS_CONTRACT);
  const grouped = deps.normalizeTraits(attrs);
  const { total } = deps.computeHpFromTraits(grouped, table);
  const n = Number(total);
  return Number.isFinite(n) ? Math.floor(n) : null;
}

async function fetchOwnedSquigs(guildId, userId) {
  const links = await deps.getWalletLinks(guildId, userId);
  const walletAddresses = links.map((x) => x.wallet_address).filter(Boolean);
  if (!walletAddresses.length) {
    return { ok: false, reason: 'You must link your wallet before joining a Squig Duel.' };
  }

  const tokenIds = await deps.getOwnedTokenIdsForContractMany(walletAddresses, SQUIGS_CONTRACT);
  if (!tokenIds.length) {
    return { ok: false, reason: 'No Squigs found in your connected wallet.' };
  }

  const squigs = [];
  for (const tokenId of tokenIds.slice(0, 200)) {
    try {
      const uglyPoints = await calculateUglyPoints(guildId, tokenId);
      if (!Number.isFinite(Number(uglyPoints))) continue;
      const hp = balancedHp(uglyPoints);
      squigs.push({
        tokenId: String(tokenId),
        uglyPoints: Math.floor(Number(uglyPoints)),
        maxHp: hp,
        attackPower: baseAttack(uglyPoints),
        imageUrl: squigImageUrl(tokenId),
      });
    } catch (err) {
      console.warn(`[SquigDuels] Squig #${tokenId} point lookup failed:`, String(err?.message || err || ''));
    }
  }

  if (!squigs.length) {
    return { ok: false, reason: 'No UglyPoints mapping found for your Squigs.' };
  }

  squigs.sort((a, b) => Number(b.uglyPoints) - Number(a.uglyPoints));
  return { ok: true, squigs, links };
}

function buildSquigSelectRows(duelId, side, squigs) {
  const options = squigs.slice(0, MAX_SELECT_OPTIONS).map((s) => ({
    label: `Squig #${s.tokenId} | ${s.uglyPoints} UP`.slice(0, 100),
    description: `HP ${s.maxHp} | Attack ${s.attackPower}`.slice(0, 100),
    value: String(s.tokenId),
  }));
  return [
    new ActionRowBuilder().addComponents(
      new StringSelectMenuBuilder()
        .setCustomId(`sd:select:${duelId}:${side}`)
        .setPlaceholder('Choose your Squig')
        .addOptions(options)
    ),
  ];
}

function buildViewSquigRows(userId, squigs) {
  const options = squigs.slice(0, MAX_SELECT_OPTIONS).map((s) => ({
    label: `Squig #${s.tokenId}`.slice(0, 100),
    description: `${s.uglyPoints} UglyPoints | ${s.maxHp} HP | ${s.attackPower} Attack`.slice(0, 100),
    value: String(s.tokenId),
  }));
  return [
    new ActionRowBuilder().addComponents(
      new StringSelectMenuBuilder()
        .setCustomId(`sd:view_select:${userId}`)
        .setPlaceholder('Scroll to view your Squigs')
        .addOptions(options)
    ),
  ];
}

function buildSquigSelectionEmbed(squigs) {
  const shown = squigs.slice(0, MAX_SELECT_OPTIONS);
  const lines = shown.slice(0, 12).map((s) =>
    `#${s.tokenId} - ${s.uglyPoints} UglyPoints - ${s.maxHp} HP`
  );
  const embed = new EmbedBuilder()
    .setTitle('Choose Your Squig')
    .setColor(0xB0DEEE)
    .setDescription(lines.join('\n') || 'Select one of your owned Squigs below.');
  if (shown[0]?.imageUrl) {
    embed.setImage(shown[0].imageUrl);
    embed.setFooter({ text: `Preview: Squig #${shown[0].tokenId}` });
  }
  return embed;
}

function buildOwnedSquigsEmbed(user, squigs, selectedTokenId = null) {
  const selected = selectedTokenId
    ? squigs.find((s) => String(s.tokenId) === String(selectedTokenId))
    : squigs[0];
  const lines = squigs.slice(0, MAX_SELECT_OPTIONS).map((s) =>
    `#${s.tokenId} - ${s.uglyPoints} UglyPoints - ${s.maxHp} HP`
  );
  const embed = new EmbedBuilder()
    .setTitle(`${user.username}'s Squigs`)
    .setColor(0xB0DEEE)
    .setDescription(lines.join('\n') || 'No Squigs found.')
    .setFooter({
      text: squigs.length > MAX_SELECT_OPTIONS
        ? `Showing top ${MAX_SELECT_OPTIONS} Squigs by UglyPoints`
        : `${squigs.length} Squig${squigs.length === 1 ? '' : 's'} found`,
    });
  if (selected?.imageUrl) {
    embed.setImage(selected.imageUrl);
    embed.addFields({
      name: `Selected Squig #${selected.tokenId}`,
      value:
        `UglyPoints: **${selected.uglyPoints}**\n` +
        `Balanced HP: **${selected.maxHp}**\n` +
        `Attack Power: **${selected.attackPower}**`,
      inline: false,
    });
  }
  return embed;
}

async function handleViewSquigsButton(interaction) {
  if (interaction.customId !== 'sd:view') return false;
  assertReady();
  await interaction.deferReply({ flags: 64 });
  const result = await fetchOwnedSquigs(interaction.guild.id, interaction.user.id);
  if (!result.ok) {
    await interaction.editReply({ content: result.reason });
    return true;
  }
  pendingSquigViews.set(`${interaction.guild.id}:${interaction.user.id}`, {
    createdAt: Date.now(),
    squigsById: new Map(result.squigs.map((s) => [String(s.tokenId), s])),
    squigs: result.squigs,
  });
  const extra = result.squigs.length > MAX_SELECT_OPTIONS
    ? ` Showing your top ${MAX_SELECT_OPTIONS} Squigs by UglyPoints.`
    : '';
  await interaction.editReply({
    content: `Your Squigs are listed below.${extra}`,
    embeds: [buildOwnedSquigsEmbed(interaction.user, result.squigs)],
    components: buildViewSquigRows(interaction.user.id, result.squigs),
  });
  return true;
}

async function handleViewSquigSelect(interaction) {
  const match = interaction.customId.match(/^sd:view_select:(\d{16,22})$/);
  if (!match) return false;
  assertReady();
  if (interaction.user.id !== match[1]) {
    await interaction.reply({ content: 'This Squig viewer is not for you.', flags: 64 });
    return true;
  }
  const state = pendingSquigViews.get(`${interaction.guild.id}:${interaction.user.id}`);
  const tokenId = String(interaction.values?.[0] || '').trim();
  const selected = state?.squigsById?.get(tokenId);
  if (!state || !selected) {
    await interaction.reply({ content: 'This Squig viewer expired. Click View Squigs again.', flags: 64 });
    return true;
  }
  await interaction.update({
    content: `Selected Squig #${selected.tokenId}.`,
    embeds: [buildOwnedSquigsEmbed(interaction.user, state.squigs, selected.tokenId)],
    components: buildViewSquigRows(interaction.user.id, state.squigs),
  });
  return true;
}

async function promptSquigSelection(interaction, duel, side) {
  const userId = side === 'challenger' ? duel.challengerId : duel.opponentId;
  const result = await fetchOwnedSquigs(interaction.guild.id, userId);
  if (!result.ok) {
    await interaction.editReply({ content: result.reason });
    return;
  }
  pendingSquigSelections.set(`${duel.id}:${side}:${userId}`, {
    duelId: duel.id,
    side,
    userId,
    squigsById: new Map(result.squigs.map((s) => [String(s.tokenId), s])),
    createdAt: Date.now(),
  });
  const extra = result.squigs.length > MAX_SELECT_OPTIONS
    ? `\nShowing your top ${MAX_SELECT_OPTIONS} Squigs by UglyPoints.`
    : '';
  await interaction.editReply({
    content:
      `Choose your Squig.${extra}\n` +
      `Each option shows Squig ID, UglyPoints, Balanced HP, and attack power.`,
    embeds: [buildSquigSelectionEmbed(result.squigs)],
    components: buildSquigSelectRows(duel.id, side, result.squigs),
  });
}

async function handleOpponentSelect(interaction) {
  const match = interaction.customId.match(/^sd:opponent:([a-f0-9]{12})$/i);
  if (!match) return false;
  assertReady();

  const duel = getDuel(match[1]);
  if (!duel || duel.status !== 'setup') {
    await interaction.reply({ content: 'This duel setup expired or is no longer valid.', flags: 64 });
    return true;
  }
  if (interaction.user.id !== duel.challengerId) {
    await interaction.reply({ content: 'Only the challenger can select this opponent.', flags: 64 });
    return true;
  }

  const opponentId = String(interaction.values?.[0] || '').trim();
  if (!opponentId || opponentId === interaction.user.id || opponentId === botUserId()) {
    await interaction.reply({ content: 'Choose another holder as your opponent.', flags: 64 });
    return true;
  }
  const opponentActiveDuelId = activeUserToDuel.get(opponentId);
  if (opponentActiveDuelId && opponentActiveDuelId !== duel.id) {
    await interaction.reply({ content: 'That opponent is already in an active Squig Duel.', flags: 64 });
    return true;
  }
  if (!(await hasHolderRole(interaction.guild, opponentId))) {
    await interaction.reply({ content: 'That opponent does not have the holder role.', flags: 64 });
    return true;
  }

  if (duel.opponentId && duel.opponentId !== opponentId) {
    activeUserToDuel.delete(String(duel.opponentId));
  }
  duel.opponentId = opponentId;
  registerActiveUser(opponentId, duel.id);
  await persistDuel(duel);
  await interaction.showModal(challengerSetupModal(duel.id));
  return true;
}

async function handleSetupModal(interaction) {
  const match = interaction.customId.match(/^sd:setup:([a-f0-9]{12})$/i);
  if (!match) return false;
  assertReady();
  const duel = getDuel(match[1]);
  if (!duel || duel.status !== 'setup') {
    await interaction.reply({ content: 'This duel setup expired or is no longer valid.', flags: 64 });
    return true;
  }
  if (interaction.user.id !== duel.challengerId) {
    await interaction.reply({ content: 'Only the challenger can complete this setup.', flags: 64 });
    return true;
  }

  const opponentId = duel.opponentId;
  const wagerAmount = parseWager(interaction.fields.getTextInputValue('wager'));
  if (!opponentId) {
    await interaction.reply({ content: 'Select an opponent first.', flags: 64 });
    return true;
  }
  if (opponentId === interaction.user.id || opponentId === botUserId()) {
    await interaction.reply({ content: 'Choose another holder as your opponent.', flags: 64 });
    return true;
  }
  if (!wagerAmount) {
    await interaction.reply({ content: 'Wager must be a whole number greater than 0.', flags: 64 });
    return true;
  }
  const opponentActiveDuelId = activeUserToDuel.get(opponentId);
  if (opponentActiveDuelId && opponentActiveDuelId !== duel.id) {
    await interaction.reply({ content: 'That opponent is already in an active Squig Duel.', flags: 64 });
    return true;
  }
  if (!(await hasHolderRole(interaction.guild, opponentId))) {
    await interaction.reply({ content: 'That opponent does not have the holder role.', flags: 64 });
    return true;
  }

  duel.opponentId = opponentId;
  duel.wagerAmount = wagerAmount;
  if (duel.setupTimeout) clearTimeout(duel.setupTimeout);
  duel.setupTimeout = null;
  registerActiveUser(opponentId, duel.id);
  await persistDuel(duel);
  const thread = await interaction.guild.channels.fetch(duel.threadId).catch(() => null);
  await thread?.members?.add(opponentId).catch(() => null);
  await interaction.deferReply({ flags: 64 });
  await promptSquigSelection(interaction, duel, 'challenger');
  return true;
}

function challengeRows(duelId) {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`sd:accept:${duelId}`)
        .setLabel('Accept Duel')
        .setStyle(ButtonStyle.Success),
      new ButtonBuilder()
        .setCustomId(`sd:decline:${duelId}`)
        .setLabel('Decline Duel')
        .setStyle(ButtonStyle.Secondary)
    ),
  ];
}

async function postChallenge(guild, duel) {
  const thread = await guild.channels.fetch(duel.threadId).catch(() => null);
  if (!thread?.isTextBased()) return;
  duel.status = 'awaiting_accept';
  await persistDuel(duel);
  await thread.send({
    content:
      `<@${duel.challengerId}> has challenged <@${duel.opponentId}> to a Squig Duel for ` +
      `${formatCharm(duel.wagerAmount)} $CHARM.`,
    components: challengeRows(duel.id),
  });
  if (duel.acceptTimeout) clearTimeout(duel.acceptTimeout);
  duel.acceptTimeout = setTimeout(() => {
    const active = getDuel(duel.id);
    if (!active || active.status !== 'awaiting_accept') return;
    cancelDuel(guild, active, 'Opponent did not respond in time.').catch((err) => {
      console.warn('[SquigDuels] accept timeout cancel failed:', String(err?.message || err || ''));
    });
  }, ACCEPT_TIMEOUT_MS);
}

async function handleSquigSelect(interaction) {
  const match = interaction.customId.match(/^sd:select:([a-f0-9]{12}):(challenger|opponent)$/i);
  if (!match) return false;
  assertReady();
  const duel = getDuel(match[1]);
  const side = match[2];
  if (!duel || !activeStatuses().has(duel.status)) {
    await interaction.reply({ content: 'This duel is no longer active.', flags: 64 });
    return true;
  }

  const expectedUserId = side === 'challenger' ? duel.challengerId : duel.opponentId;
  if (interaction.user.id !== expectedUserId) {
    await interaction.reply({ content: 'This Squig selection is not for you.', flags: 64 });
    return true;
  }

  const tokenId = String(interaction.values?.[0] || '').trim();
  const pendingKey = `${duel.id}:${side}:${interaction.user.id}`;
  const pending = pendingSquigSelections.get(pendingKey);
  const chosen = pending?.squigsById?.get(tokenId);
  if (!chosen) {
    await interaction.reply({ content: 'Invalid Squig selection. Start your selection again.', flags: 64 });
    return true;
  }

  const ownership = await fetchOwnedSquigs(interaction.guild.id, interaction.user.id);
  if (!ownership.ok || !ownership.squigs.some((s) => String(s.tokenId) === tokenId)) {
    await interaction.reply({ content: 'That Squig is no longer found in your connected wallet.', flags: 64 });
    return true;
  }

  await interaction.deferUpdate();
  pendingSquigSelections.delete(pendingKey);

  if (side === 'challenger') {
    duel.challengerSquigTokenId = chosen.tokenId;
    duel.challengerUglyPoints = chosen.uglyPoints;
    duel.challengerMaxHp = chosen.maxHp;
    duel.challengerCurrentHp = chosen.maxHp;
    if (isBotDuel(duel)) {
      duel.opponentSquigTokenId = chosen.tokenId;
      duel.opponentUglyPoints = chosen.uglyPoints;
      duel.opponentMaxHp = chosen.maxHp;
      duel.opponentCurrentHp = chosen.maxHp;
    }
    await persistDuel(duel);
    await interaction.editReply({
      content:
        `Selected Squig #${chosen.tokenId} (${chosen.uglyPoints} UglyPoints, ${chosen.maxHp} HP).\n` +
        `Collecting your ${formatCharm(duel.wagerAmount)} $CHARM wager...`,
      embeds: [],
      components: [],
    });

    const paid = await collectWager(interaction.guild, interaction.user.id, duel.wagerAmount, 'squig_duel_challenger_wager');
    if (!paid.ok) {
      await cancelDuel(interaction.guild, duel, paid.reason || 'Challenger wager could not be collected.');
      await interaction.followUp({ content: paid.reason || 'Wager could not be collected.', flags: 64 }).catch(() => null);
      return true;
    }
    duel.challengerPaid = true;
    await persistDuel(duel);
    await logDuel(interaction.guild, 'Squig Selected', `<@${duel.challengerId}> selected Squig #${chosen.tokenId}.`);
    if (isBotDuel(duel)) {
      if (duel.setupTimeout) clearTimeout(duel.setupTimeout);
      duel.setupTimeout = null;
      await startDuel(interaction.guild, duel);
      await interaction.followUp({
        content: `Bot duel started. Your ${BOT_DUEL_WAGER} $CHARM test wager will be returned after the duel.`,
        flags: 64,
      }).catch(() => null);
      return true;
    }
    await postChallenge(interaction.guild, duel);
    await interaction.followUp({ content: 'Wager collected. Challenge posted in the duel thread.', flags: 64 }).catch(() => null);
    return true;
  }

  duel.opponentSquigTokenId = chosen.tokenId;
  duel.opponentUglyPoints = chosen.uglyPoints;
  duel.opponentMaxHp = chosen.maxHp;
  duel.opponentCurrentHp = chosen.maxHp;
  await persistDuel(duel);
  await interaction.editReply({
    content:
      `Selected Squig #${chosen.tokenId} (${chosen.uglyPoints} UglyPoints, ${chosen.maxHp} HP).\n` +
      `Collecting your ${formatCharm(duel.wagerAmount)} $CHARM wager...`,
    embeds: [],
    components: [],
  });
  const paid = await collectWager(interaction.guild, interaction.user.id, duel.wagerAmount, 'squig_duel_opponent_wager');
  if (!paid.ok) {
    await cancelDuel(interaction.guild, duel, paid.reason || 'Opponent wager could not be collected.');
    await interaction.followUp({ content: paid.reason || 'Wager could not be collected.', flags: 64 }).catch(() => null);
    return true;
  }
  duel.opponentPaid = true;
  await persistDuel(duel);
  await logDuel(interaction.guild, 'Squig Selected', `<@${duel.opponentId}> selected Squig #${chosen.tokenId}.`);
  await startDuel(interaction.guild, duel);
  await interaction.followUp({ content: 'Wager collected. Duel started.', flags: 64 }).catch(() => null);
  return true;
}

async function handleAcceptDecline(interaction) {
  const match = interaction.customId.match(/^sd:(accept|decline):([a-f0-9]{12})$/i);
  if (!match) return false;
  assertReady();
  const action = match[1];
  const duel = getDuel(match[2]);
  if (!duel || duel.status !== 'awaiting_accept') {
    await interaction.reply({ content: 'This challenge is no longer awaiting a response.', flags: 64 });
    return true;
  }
  if (interaction.user.id !== duel.opponentId) {
    await interaction.reply({ content: 'Only the challenged player can accept or decline this duel.', flags: 64 });
    return true;
  }
  if (action === 'decline') {
    await interaction.deferUpdate();
    await cancelDuel(interaction.guild, duel, 'Opponent declined the duel.');
    await interaction.editReply({ components: [] }).catch(() => null);
    return true;
  }

  if (duel.acceptTimeout) clearTimeout(duel.acceptTimeout);
  await interaction.update({ content: 'Duel accepted. Opponent is choosing a Squig.', components: [] });
  await logDuel(interaction.guild, 'Accepted', `<@${duel.opponentId}> accepted duel \`${duel.id}\`.`);
  await interaction.followUp({ content: 'Choose your Squig privately.', flags: 64 }).catch(() => null);
  const followUp = {
    ...interaction,
    editReply: (payload) => interaction.followUp({ ...payload, flags: 64 }),
  };
  await promptSquigSelection(followUp, duel, 'opponent');
  return true;
}

function buildStatusEmbed(duel, title, description) {
  return new EmbedBuilder()
    .setTitle(title)
    .setColor(0x7ADDC0)
    .setDescription(description)
    .addFields(
      {
        name: `Challenger Squig #${duel.challengerSquigTokenId}`,
        value:
          `<@${duel.challengerId}>\n` +
          `UglyPoints: **${duel.challengerUglyPoints}**\n` +
          `HP: **${Math.max(0, duel.challengerCurrentHp)} / ${duel.challengerMaxHp}**`,
        inline: true,
      },
      {
        name: `Opponent Squig #${duel.opponentSquigTokenId}`,
        value:
          `<@${duel.opponentId}>\n` +
          `UglyPoints: **${duel.opponentUglyPoints}**\n` +
          `HP: **${Math.max(0, duel.opponentCurrentHp)} / ${duel.opponentMaxHp}**`,
        inline: true,
      }
    )
    .setThumbnail(squigImageUrl(duel.challengerSquigTokenId))
    .setImage(squigImageUrl(duel.opponentSquigTokenId));
}

function actionRows(duel) {
  const actions = [
    ['attack', 'Attack', ButtonStyle.Danger],
    ['defend', 'Defend', ButtonStyle.Primary],
    ['heal', 'Heal', ButtonStyle.Success],
    ['panic', 'Panic', ButtonStyle.Secondary],
  ];
  const challengerRow = new ActionRowBuilder().addComponents(
    actions.map(([key, label, style]) =>
      new ButtonBuilder()
        .setCustomId(`sd:act:${duel.id}:challenger:${key}`)
        .setLabel(isBotDuel(duel) ? label : `C: ${label}`)
        .setStyle(style)
    )
  );
  if (isBotDuel(duel)) return [challengerRow];
  return [
    challengerRow,
    new ActionRowBuilder().addComponents(
      actions.map(([key, label, style]) =>
        new ButtonBuilder()
          .setCustomId(`sd:act:${duel.id}:opponent:${key}`)
          .setLabel(`O: ${label}`)
          .setStyle(style)
      )
    ),
  ];
}

async function startDuel(guild, duel) {
  duel.status = 'active';
  duel.currentRound = 0;
  duel.currentActions = {};
  await persistDuel(duel);
  const thread = await guild.channels.fetch(duel.threadId).catch(() => null);
  if (!thread?.isTextBased()) return;
  const startDescription = isBotDuel(duel)
    ? `Bot test duel.\nTest wager: **${formatCharm(BOT_DUEL_WAGER)} $CHARM**\nYour wager is returned after the duel no matter who wins.`
    : `Pot: **${formatCharm(duel.wagerAmount * 2)} $CHARM**\nRound actions are hidden until both players choose.`;
  await thread.send({
    embeds: [buildStatusEmbed(
      duel,
      'Squig Duel Started',
      startDescription
    )],
  });
  await logDuel(guild, 'Started', `Duel \`${duel.id}\` started.`);
  await beginRound(guild, duel);
}

async function beginRound(guild, duel) {
  if (duel.status !== 'active') return;
  duel.currentRound += 1;
  duel.currentActions = {};
  duel.processingRound = false;
  await persistDuel(duel);
  const thread = await guild.channels.fetch(duel.threadId).catch(() => null);
  if (!thread?.isTextBased()) return;
  const sudden = duel.currentRound > SUDDEN_DEATH_AFTER_ROUND;
  const roundPrompt = isBotDuel(duel)
    ? `Choose your action within ${Math.round(ROUND_TIMEOUT_MS / 1000)} seconds.\nThe bot will answer immediately after you lock in.\n`
    : `Both players have ${Math.round(ROUND_TIMEOUT_MS / 1000)} seconds to choose.\n`;
  await thread.send({
    embeds: [buildStatusEmbed(
      duel,
      `Round ${duel.currentRound}${sudden ? ' - Sudden Death' : ''}`,
      roundPrompt +
      (sudden ? `Sudden Death: both Squigs lose ${SUDDEN_DEATH_DAMAGE} HP at the end of the round.` : '')
    )],
    components: actionRows(duel),
  });
  if (duel.roundTimeout) clearTimeout(duel.roundTimeout);
  duel.roundTimeout = setTimeout(() => {
    resolveRound(guild, duel, true).catch((err) => {
      console.warn('[SquigDuels] round timeout failed:', String(err?.message || err || ''));
    });
  }, ROUND_TIMEOUT_MS);
}

async function handleActionButton(interaction) {
  const match = interaction.customId.match(/^sd:act:([a-f0-9]{12}):(challenger|opponent):(attack|defend|heal|panic)$/i);
  if (!match) return false;
  assertReady();
  const duel = getDuel(match[1]);
  const side = match[2];
  const action = match[3].toLowerCase();
  if (!duel || duel.status !== 'active') {
    await interaction.reply({ content: 'This duel round is no longer active.', flags: 64 });
    return true;
  }
  const expectedUserId = side === 'challenger' ? duel.challengerId : duel.opponentId;
  if (interaction.user.id !== expectedUserId) {
    await interaction.reply({ content: 'Only that duel participant can choose this action.', flags: 64 });
    return true;
  }
  if (duel.currentActions[side]) {
    await interaction.reply({ content: `You already chose ${duel.currentActions[side]}.`, flags: 64 });
    return true;
  }
  duel.currentActions[side] = action;
  if (isBotDuel(duel) && side === 'challenger') {
    duel.currentActions.opponent = randomBotAction();
  }
  await interaction.reply({ content: `Action locked: ${action}.`, flags: 64 });
  if (duel.currentActions.challenger && duel.currentActions.opponent) {
    if (duel.roundTimeout) clearTimeout(duel.roundTimeout);
    await resolveRound(interaction.guild, duel, false);
  }
  return true;
}

function attackDamage(attackPower) {
  const multiplier = 0.8 + (Math.random() * 0.4);
  return Math.max(1, Math.round((Number(attackPower) || 1) * multiplier));
}

function clampHp(value, maxHp) {
  return Math.max(0, Math.min(Math.round(Number(value) || 0), Math.round(Number(maxHp) || 0)));
}

function resolveRoundMath(duel, timedOut) {
  if (isBotDuel(duel) && !duel.currentActions.opponent) {
    duel.currentActions.opponent = randomBotAction();
  }
  const actions = {
    challenger: duel.currentActions.challenger || (timedOut ? 'miss' : 'miss'),
    opponent: duel.currentActions.opponent || (timedOut ? 'miss' : 'miss'),
  };
  const before = {
    challenger: duel.challengerCurrentHp,
    opponent: duel.opponentCurrentHp,
  };
  let cHp = duel.challengerCurrentHp;
  let oHp = duel.opponentCurrentHp;
  const lines = [];

  if (actions.challenger === 'miss') lines.push(`<@${duel.challengerId}> missed the action window.`);
  if (actions.opponent === 'miss') lines.push(`<@${duel.opponentId}> missed the action window.`);

  const cPanic = actions.challenger === 'panic';
  const oPanic = actions.opponent === 'panic';
  if (cPanic) {
    const selfLoss = Math.round(duel.challengerMaxHp * 0.12);
    const enemyLoss = Math.round(duel.challengerMaxHp * 0.08);
    cHp -= selfLoss;
    oHp -= enemyLoss;
    lines.push(`Squig #${duel.challengerSquigTokenId} panicked: -${selfLoss} self HP, -${enemyLoss} enemy HP.`);
  }
  if (oPanic) {
    const selfLoss = Math.round(duel.opponentMaxHp * 0.12);
    const enemyLoss = Math.round(duel.opponentMaxHp * 0.08);
    oHp -= selfLoss;
    cHp -= enemyLoss;
    lines.push(`Squig #${duel.opponentSquigTokenId} panicked: -${selfLoss} self HP, -${enemyLoss} enemy HP.`);
  }

  const cBlockedByPanic = oPanic;
  const oBlockedByPanic = cPanic;
  const cDefends = actions.challenger === 'defend' && !cBlockedByPanic;
  const oDefends = actions.opponent === 'defend' && !oBlockedByPanic;

  if (actions.challenger === 'attack') {
    if (cBlockedByPanic) {
      lines.push(`Squig #${duel.challengerSquigTokenId}'s attack missed in the panic.`);
    } else {
      let dmg = attackDamage(baseAttack(duel.challengerUglyPoints));
      if (oDefends) {
        dmg = Math.max(1, Math.round(dmg * 0.25));
        lines.push(`Squig #${duel.opponentSquigTokenId} defended and reduced the attack to ${dmg} damage.`);
      } else {
        lines.push(`Squig #${duel.challengerSquigTokenId} attacked for ${dmg} damage.`);
      }
      oHp -= dmg;
    }
  }

  if (actions.opponent === 'attack') {
    if (oBlockedByPanic) {
      lines.push(`Squig #${duel.opponentSquigTokenId}'s attack missed in the panic.`);
    } else {
      let dmg = attackDamage(baseAttack(duel.opponentUglyPoints));
      if (cDefends) {
        dmg = Math.max(1, Math.round(dmg * 0.25));
        lines.push(`Squig #${duel.challengerSquigTokenId} defended and reduced the attack to ${dmg} damage.`);
      } else {
        lines.push(`Squig #${duel.opponentSquigTokenId} attacked for ${dmg} damage.`);
      }
      cHp -= dmg;
    }
  }

  if (actions.challenger === 'heal') {
    if (cBlockedByPanic || oDefends) {
      lines.push(`Squig #${duel.challengerSquigTokenId}'s heal failed.`);
    } else {
      const heal = Math.round(10 + duel.challengerMaxHp * 0.10);
      const beforeHeal = cHp;
      cHp = Math.min(duel.challengerMaxHp, cHp + heal);
      lines.push(`Squig #${duel.challengerSquigTokenId} healed ${Math.max(0, cHp - beforeHeal)} HP.`);
    }
  }
  if (actions.opponent === 'heal') {
    if (oBlockedByPanic || cDefends) {
      lines.push(`Squig #${duel.opponentSquigTokenId}'s heal failed.`);
    } else {
      const heal = Math.round(10 + duel.opponentMaxHp * 0.10);
      const beforeHeal = oHp;
      oHp = Math.min(duel.opponentMaxHp, oHp + heal);
      lines.push(`Squig #${duel.opponentSquigTokenId} healed ${Math.max(0, oHp - beforeHeal)} HP.`);
    }
  }

  const hpBeforeSuddenDeath = { challenger: cHp, opponent: oHp };
  if (duel.currentRound > SUDDEN_DEATH_AFTER_ROUND) {
    cHp -= SUDDEN_DEATH_DAMAGE;
    oHp -= SUDDEN_DEATH_DAMAGE;
    lines.push(`Sudden Death burns both Squigs for ${SUDDEN_DEATH_DAMAGE} HP.`);
  }

  duel.challengerCurrentHp = clampHp(cHp, duel.challengerMaxHp);
  duel.opponentCurrentHp = clampHp(oHp, duel.opponentMaxHp);

  return {
    round: duel.currentRound,
    actions,
    before,
    hpBeforeSuddenDeath,
    finalBeforeClamp: { challenger: cHp, opponent: oHp },
    lines,
  };
}

function determineWinner(duel, result) {
  const cOut = duel.challengerCurrentHp <= 0;
  const oOut = duel.opponentCurrentHp <= 0;
  if (cOut && !oOut) return duel.opponentId;
  if (oOut && !cOut) return duel.challengerId;
  if (!cOut && !oOut) return null;

  const tiebreakHp = result.hpBeforeSuddenDeath || result.finalBeforeClamp || {};
  const cRaw = Number(tiebreakHp.challenger || 0);
  const oRaw = Number(tiebreakHp.opponent || 0);
  if (cRaw > oRaw) return duel.challengerId;
  if (oRaw > cRaw) return duel.opponentId;
  return Math.random() < 0.5 ? duel.challengerId : duel.opponentId;
}

async function resolveRound(guild, duel, timedOut) {
  if (!duel || duel.status !== 'active' || duel.processingRound) return;
  duel.processingRound = true;
  if (duel.roundTimeout) clearTimeout(duel.roundTimeout);
  const result = resolveRoundMath(duel, timedOut);
  await persistRound(duel, result);
  await logDuel(guild, 'Round Result', `Duel \`${duel.id}\` round ${result.round}: ${result.lines.join(' | ')}`);

  const thread = await guild.channels.fetch(duel.threadId).catch(() => null);
  const actionText =
    `<@${duel.challengerId}>: **${result.actions.challenger}**\n` +
    `<@${duel.opponentId}>: **${result.actions.opponent}**`;
  if (thread?.isTextBased()) {
    await thread.send({
      embeds: [buildStatusEmbed(
        duel,
        `Round ${result.round} Results`,
        `**Selected Actions**\n${actionText}\n\n` +
        `**Result**\n${result.lines.join('\n') || 'No effects resolved.'}\n\n` +
        `Remaining HP shown below.`
      )],
    });
  }

  const winnerId = determineWinner(duel, result);
  if (winnerId) {
    await completeDuel(guild, duel, winnerId, result);
    return;
  }
  duel.processingRound = false;
  await beginRound(guild, duel);
}

function duelTaxPercent() {
  const raw = String(process.env.DUEL_TAX_PERCENT || '').trim();
  if (!raw) return 0;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.min(100, n);
}

async function completeDuel(guild, duel, winnerId) {
  duel.status = 'completed';
  duel.winnerId = winnerId;
  duel.completedAt = Date.now();
  releaseDuelUsers(duel);
  if (isBotDuel(duel)) {
    const thread = await guild.channels.fetch(duel.threadId).catch(() => null);
    let refundOk = true;
    let refundError = '';
    if (duel.challengerPaid) {
      try {
        await transferFromBot(guild, duel.challengerId, BOT_DUEL_WAGER, 'squig_duel_bot_refund', botUserId());
        duel.challengerPaid = false;
      } catch (err) {
        refundOk = false;
        refundError = String(err?.message || err || '').slice(0, 500);
        await logDuel(guild, 'Bot Duel Refund Error', `Duel \`${duel.id}\` refund failed for <@${duel.challengerId}>: ${refundError}`);
      }
    }
    await persistDuel(duel);
    if (thread?.isTextBased()) {
      await thread.send({
        embeds: [buildStatusEmbed(
          duel,
          'Bot Squig Duel Complete',
          `Winner: <@${winnerId}>\n` +
          `Test wager: **${formatCharm(BOT_DUEL_WAGER)} $CHARM**\n` +
          (refundOk
            ? `Returned **${formatCharm(BOT_DUEL_WAGER)} $CHARM** to <@${duel.challengerId}>.`
            : `Refund failed and needs admin review: ${refundError}`)
        )],
      }).catch(() => null);
    }
    await logDuel(guild, 'Bot Duel Completed', `Duel \`${duel.id}\` winner <@${winnerId}> refund ${refundOk ? 'ok' : 'failed'}.`);
    return;
  }
  const pot = Math.floor(Number(duel.wagerAmount || 0) * 2);
  const taxPercent = duelTaxPercent();
  const taxAmount = Math.floor(pot * (taxPercent / 100));
  const payout = Math.max(0, pot - taxAmount);
  const thread = await guild.channels.fetch(duel.threadId).catch(() => null);

  let payoutOk = true;
  let payoutError = '';
  try {
    await transferFromBot(guild, winnerId, payout, 'squig_duel_payout', botUserId());
  } catch (err) {
    payoutOk = false;
    payoutError = String(err?.message || err || '').slice(0, 500);
    await logDuel(guild, 'Payout Error', `Duel \`${duel.id}\` payout failed for <@${winnerId}>: ${payoutError}`);
  }
  await persistDuel(duel);

  if (thread?.isTextBased()) {
    await thread.send({
      embeds: [buildStatusEmbed(
        duel,
        'Squig Duel Complete',
        `Winner: <@${winnerId}>\n` +
        `Pot: **${formatCharm(pot)} $CHARM**\n` +
        `Tax: **${formatCharm(taxAmount)} $CHARM** (${taxPercent}%)\n` +
        `Payout: **${formatCharm(payout)} $CHARM**\n` +
        (payoutOk ? 'Payout completed.' : `Payout failed and needs admin review: ${payoutError}`)
      )],
    }).catch(() => null);
  }
  await logDuel(guild, 'Completed', `Duel \`${duel.id}\` winner <@${winnerId}> payout ${formatCharm(payout)} $CHARM.`);
}

async function handleButton(interaction) {
  if (!String(interaction.customId || '').startsWith('sd:')) return false;
  if (await handleViewSquigsButton(interaction)) return true;
  if (await handleBotDuelButton(interaction)) return true;
  if (await handleStartButton(interaction)) return true;
  if (await handleAcceptDecline(interaction)) return true;
  if (await handleActionButton(interaction)) return true;
  return false;
}

async function handleSelectMenu(interaction) {
  const customId = String(interaction.customId || '');
  if (customId.startsWith('sd:opponent:')) return handleOpponentSelect(interaction);
  if (customId.startsWith('sd:view_select:')) return handleViewSquigSelect(interaction);
  if (customId.startsWith('sd:select:')) return handleSquigSelect(interaction);
  return false;
}

async function handleModalSubmit(interaction) {
  if (!String(interaction.customId || '').startsWith('sd:setup:')) return false;
  return handleSetupModal(interaction);
}

async function handleMessageCreate(message) {
  if (!message.guild || message.author?.bot) return false;
  const duel = [...duels.values()].find((d) => d.threadId === message.channelId && activeStatuses().has(d.status));
  if (!duel) return false;
  const allowed = new Set([duel.challengerId, duel.opponentId].filter(Boolean));
  if (allowed.has(message.author.id)) return false;
  const member = await message.guild.members.fetch(message.author.id).catch(() => null);
  if (member?.permissions?.has(PermissionFlagsBits.ManageGuild)) return false;
  await message.delete().catch(() => null);
  return true;
}

module.exports = {
  initSquigDuels,
  ensureSquigDuelSchema,
  buildSquigDuelSlashCommand,
  handleCommand,
  handleButton,
  handleSelectMenu,
  handleModalSubmit,
  handleMessageCreate,
};
