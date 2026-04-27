const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const fetch = require('node-fetch');
const { createCanvas, loadImage } = require('@napi-rs/canvas');
const {
  SlashCommandBuilder,
  EmbedBuilder,
  AttachmentBuilder,
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
const SQUIG_DUEL_PUBLIC_LOG_CHANNEL_ID = String(
  process.env.SQUIG_DUEL_PUBLIC_LOG_CHANNEL_ID || '1403005536982794371'
).trim();
const SQUIG_DUEL_LOSER_IMAGE_NAME = 'squig-duel-loser.png';
const SQUIG_DUEL_PUNCH_OVERLAY_PATH = path.join(__dirname, '..', 'squig_duel_punch_overlay.png');
const WALLET_CONNECT_CHANNEL_ID = '1476967108062740622';
const OPEN_CHALLENGE_ROLE_ID = '1389076094245671002';
const BOT_DUEL_WAGER = 50;
const BOT_DUEL_MAX_WAGER = 5000;
const BOT_DUEL_PAYOUT_MULTIPLIER = 3;
const MAX_DUEL_WAGER = 10000;
const MAX_SELECT_OPTIONS = 25;
const MAX_FAVORITE_SQUIGS = 10;
const SQUIG_SORT_LABEL = 'favorites first, then UglyPoints';
const ACCEPT_TIMEOUT_MINUTE_OPTIONS = [1, 2, 3, 5];
const DEFAULT_ACCEPT_TIMEOUT_MINUTES = 3;
const DEFAULT_ACCEPT_TIMEOUT_MS = DEFAULT_ACCEPT_TIMEOUT_MINUTES * 60 * 1000;
const SETUP_TIMEOUT_MS = Number(process.env.SQUIG_DUEL_SETUP_TIMEOUT_MS || 10 * 60 * 1000);
const ROUND_TIMEOUT_MS = Number(process.env.SQUIG_DUEL_ROUND_TIMEOUT_MS || 30 * 1000);
const ROUND_RESOLVE_DELAY_MS = Number(process.env.SQUIG_DUEL_ROUND_RESOLVE_DELAY_MS || 1500);
const NEXT_ROUND_DELAY_MS = Number(process.env.SQUIG_DUEL_NEXT_ROUND_DELAY_MS || 10 * 1000);
const THREAD_DELETE_DELAY_MS = 2 * 60 * 1000;
const MISSED_TURN_HP_PENALTY_PERCENT = Number(process.env.SQUIG_DUEL_MISSED_TURN_HP_PENALTY_PERCENT || 0.10);
const SUDDEN_DEATH_AFTER_ROUND = 6;
const SUDDEN_DEATH_DAMAGE = 8;
const HEAL_VS_DEFEND_MULTIPLIER = 0.55;
const HEAL_VS_ATTACK_MULTIPLIER = 1.30;

let deps = null;

const duels = new Map();
const activeUserToDuel = new Map();
const pendingSquigSelections = new Map();
const pendingSquigViews = new Map();
const pendingMySquigProfiles = new Map();

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
  await pool.query(`
    CREATE TABLE IF NOT EXISTS squig_duel_player_squigs (
      guild_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      token_id TEXT NOT NULL,
      nickname TEXT,
      is_favorite BOOLEAN NOT NULL DEFAULT FALSE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (guild_id, user_id, token_id)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS squig_duel_bot_daily_uses (
      guild_id TEXT NOT NULL,
      token_id TEXT NOT NULL,
      played_on DATE NOT NULL DEFAULT CURRENT_DATE,
      user_id TEXT NOT NULL,
      duel_id TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (guild_id, token_id, played_on)
    );
  `);
  await pool.query(`
    DO $$
    DECLARE
      pk_name TEXT;
      pk_cols TEXT[];
    BEGIN
      SELECT tc.constraint_name, array_agg(kcu.column_name::TEXT ORDER BY kcu.ordinal_position)
      INTO pk_name, pk_cols
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
       AND tc.table_schema = kcu.table_schema
      WHERE tc.table_schema = 'public'
        AND tc.table_name = 'squig_duel_player_squigs'
        AND tc.constraint_type = 'PRIMARY KEY'
      GROUP BY tc.constraint_name;

      IF pk_name IS NOT NULL AND pk_cols = ARRAY['guild_id', 'user_id']::TEXT[] THEN
        EXECUTE format('ALTER TABLE public.squig_duel_player_squigs DROP CONSTRAINT %I', pk_name);
        ALTER TABLE public.squig_duel_player_squigs ADD PRIMARY KEY (guild_id, user_id, token_id);
      ELSIF pk_name IS NULL THEN
        ALTER TABLE public.squig_duel_player_squigs ADD PRIMARY KEY (guild_id, user_id, token_id);
      END IF;
    END $$;
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

function loserSide(duel, winnerId) {
  if (String(winnerId) === String(duel.challengerId)) return 'opponent';
  if (String(winnerId) === String(duel.opponentId)) return 'challenger';
  return duel.challengerCurrentHp <= duel.opponentCurrentHp ? 'challenger' : 'opponent';
}

function tokenIdForSide(duel, side) {
  return side === 'challenger' ? duel.challengerSquigTokenId : duel.opponentSquigTokenId;
}

function squigDisplayName(squig) {
  const tokenId = String(squig?.tokenId || '').trim();
  const nickname = String(squig?.nickname || '').trim();
  return nickname ? `${nickname} (#${tokenId})` : `Squig #${tokenId}`;
}

function squigListLine(squig) {
  const favorite = squig.isFavorite ? '[Favorite] ' : '';
  return `${favorite}${squigDisplayName(squig)} - ${squig.uglyPoints} UglyPoints - ${squig.maxHp} HP`;
}

function squigOptionLabel(squig) {
  const name = squigDisplayName(squig);
  return `${name} | ${squig.uglyPoints} UP`.slice(0, 100);
}

function squigOptionDescription(squig) {
  const favorite = squig.isFavorite ? 'Favorite | ' : '';
  return `${favorite}HP ${squig.maxHp} | Attack ${squig.attackPower}`.slice(0, 100);
}

function squigNameForSide(duel, side) {
  const tokenId = tokenIdForSide(duel, side);
  const nickname = String(side === 'challenger' ? duel.challengerSquigName || '' : duel.opponentSquigName || '').trim();
  return nickname ? `${nickname} (#${tokenId})` : `Squig #${tokenId}`;
}

function squigCanvasNameForSide(duel, side) {
  const full = squigNameForSide(duel, side);
  return full.length > 24 ? `${full.slice(0, 21)}...` : full;
}

async function fetchImageBuffer(source) {
  const response = await fetch(source);
  if (!response.ok) throw new Error(`Image HTTP ${response.status}`);
  const arrayBuffer = await response.arrayBuffer();
  return Buffer.from(arrayBuffer);
}

function walletConnectMessage(prefix = 'Connect your wallet before joining a Squig Duel.') {
  return `${prefix} Go to <#${WALLET_CONNECT_CHANNEL_ID}> to connect your wallet.`;
}

function walletConnectMessageForUser(userId) {
  return userId
    ? `<@${userId}> needs to connect their wallet in <#${WALLET_CONNECT_CHANNEL_ID}> before joining a Squig Duel.`
    : walletConnectMessage();
}

async function hasConnectedWallet(guildId, userId) {
  const links = await deps.getWalletLinks(guildId, userId).catch(() => []);
  return links.some((x) => String(x?.wallet_address || '').trim());
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
  return new Set(['setup', 'awaiting_accept', 'awaiting_ready', 'active']);
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

function botWagerAmount(duel) {
  return Math.floor(Number(duel?.wagerAmount || BOT_DUEL_WAGER));
}

function botDuelPayoutAmount(duel) {
  return Math.floor(botWagerAmount(duel) * BOT_DUEL_PAYOUT_MULTIPLIER);
}

function randomBotAction() {
  const weighted = ['attack', 'attack', 'defend', 'heal', 'panic'];
  return weighted[Math.floor(Math.random() * weighted.length)];
}

function missedTurnPenalty(maxHp) {
  const pct = Number.isFinite(MISSED_TURN_HP_PENALTY_PERCENT)
    ? Math.max(0, MISSED_TURN_HP_PENALTY_PERCENT)
    : 0.10;
  return Math.max(1, Math.round((Number(maxHp) || 0) * pct));
}

function delay(ms) {
  const waitMs = Math.max(0, Number(ms) || 0);
  return new Promise((resolve) => setTimeout(resolve, waitMs));
}

function actionLabel(action) {
  const labels = {
    attack: 'Attack',
    defend: 'Defend',
    heal: 'Heal',
    panic: 'Panic',
    miss: 'Miss',
  };
  return labels[String(action || '').toLowerCase()] || String(action || 'Unknown');
}

function playerLabel(duel, side) {
  const userId = side === 'challenger' ? duel?.challengerId : duel?.opponentId;
  return userId ? `<@${userId}>` : (side === 'challenger' ? 'Player 1' : 'Player 2');
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

async function fetchPublicLogChannel(guild) {
  if (!SQUIG_DUEL_PUBLIC_LOG_CHANNEL_ID) return null;
  const fromGuild = guild?.channels?.fetch
    ? await guild.channels.fetch(SQUIG_DUEL_PUBLIC_LOG_CHANNEL_ID).catch(() => null)
    : null;
  if (fromGuild?.isTextBased?.()) return fromGuild;
  const cached = deps?.client?.channels?.cache?.get(SQUIG_DUEL_PUBLIC_LOG_CHANNEL_ID);
  if (cached?.isTextBased?.()) return cached;
  const fromClient = deps?.client?.channels?.fetch
    ? await deps.client.channels.fetch(SQUIG_DUEL_PUBLIC_LOG_CHANNEL_ID).catch(() => null)
    : null;
  return fromClient?.isTextBased?.() ? fromClient : null;
}

async function postPublicDuelFinalLog(guild, content) {
  const channel = await fetchPublicLogChannel(guild);
  if (!channel) return;
  await channel.send(content).catch((err) => {
    console.warn('[SquigDuels] public final log failed:', String(err?.message || err || ''));
  });
}

function scheduleDuelThreadDeletion(guild, duel) {
  if (!guild || !duel?.threadId || duel.threadDeleteTimeout) return;
  const threadId = duel.threadId;
  duel.threadDeleteTimeout = setTimeout(async () => {
    const thread = await guild.channels.fetch(threadId).catch(() => null);
    if (!thread?.delete) return;
    await thread.delete('Squig Duel ended; deleting thread after cleanup delay').catch((err) => {
      console.warn('[SquigDuels] failed to delete ended duel thread:', String(err?.message || err || ''));
    });
  }, THREAD_DELETE_DELAY_MS);
  if (typeof duel.threadDeleteTimeout.unref === 'function') {
    duel.threadDeleteTimeout.unref();
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

async function getSavedSquigProfiles(guildId, userId) {
  const pool = deps?.historyPool;
  const profiles = new Map();
  if (!pool?.query) return profiles;
  try {
    const { rows } = await pool.query(
      `SELECT token_id, nickname, is_favorite
       FROM squig_duel_player_squigs
       WHERE guild_id = $1 AND user_id = $2`,
      [String(guildId), String(userId)]
    );
    for (const row of rows) {
      const tokenId = String(row.token_id || '').trim();
      if (!tokenId) continue;
      profiles.set(tokenId, {
        tokenId,
        nickname: String(row.nickname || '').trim(),
        isFavorite: Boolean(row.is_favorite),
      });
    }
  } catch (err) {
    console.warn('[SquigDuels] saved Squig lookup failed:', String(err?.message || err || ''));
  }
  return profiles;
}

async function saveMySquigProfile(guildId, userId, profile) {
  const pool = deps?.historyPool;
  if (!pool?.query) {
    return { ok: false, reason: 'Profile storage is unavailable right now.' };
  }
  const tokenId = String(profile?.tokenId || '').trim();
  if (!/^\d+$/.test(tokenId)) {
    return { ok: false, reason: 'Choose a Squig before saving.' };
  }
  const nickname = String(profile?.nickname || '').trim().slice(0, 40) || null;
  const isFavorite = Boolean(profile?.isFavorite);
  try {
    if (!nickname && !isFavorite) {
      await pool.query(
        `DELETE FROM squig_duel_player_squigs
         WHERE guild_id = $1 AND user_id = $2 AND token_id = $3`,
        [String(guildId), String(userId), tokenId]
      );
      return { ok: true, deleted: true };
    }
    if (isFavorite) {
      const favoriteCount = await pool.query(
        `SELECT COUNT(*)::INTEGER AS count
         FROM squig_duel_player_squigs
         WHERE guild_id = $1
           AND user_id = $2
           AND is_favorite = TRUE
           AND token_id <> $3`,
        [String(guildId), String(userId), tokenId]
      );
      const count = Number(favoriteCount.rows?.[0]?.count || 0);
      if (count >= MAX_FAVORITE_SQUIGS) {
        return { ok: false, reason: `You can save up to ${MAX_FAVORITE_SQUIGS} favorite Squigs. Unfavorite one before adding another.` };
      }
    }
    await pool.query(
      `INSERT INTO squig_duel_player_squigs
       (guild_id, user_id, token_id, nickname, is_favorite, updated_at)
       VALUES ($1, $2, $3, $4, $5, NOW())
       ON CONFLICT (guild_id, user_id, token_id) DO UPDATE SET
         nickname = EXCLUDED.nickname,
         is_favorite = EXCLUDED.is_favorite,
         updated_at = NOW()`,
      [String(guildId), String(userId), tokenId, nickname, isFavorite]
    );
    return { ok: true };
  } catch (err) {
    console.warn('[SquigDuels] saved Squig persist failed:', String(err?.message || err || ''));
    return { ok: false, reason: 'Could not save your Squig profile right now.' };
  }
}

async function getBotDuelUseForToday(guildId, tokenId) {
  const pool = deps?.historyPool;
  if (!pool?.query) return null;
  const normalizedTokenId = String(tokenId || '').trim();
  if (!/^\d+$/.test(normalizedTokenId)) return null;
  try {
    const { rows } = await pool.query(
      `SELECT user_id, duel_id, played_on::TEXT AS played_on, created_at
       FROM squig_duel_bot_daily_uses
       WHERE guild_id = $1 AND token_id = $2 AND played_on = CURRENT_DATE
       LIMIT 1`,
      [String(guildId), normalizedTokenId]
    );
    return rows[0] || null;
  } catch (err) {
    console.warn('[SquigDuels] bot daily use lookup failed:', String(err?.message || err || ''));
    return null;
  }
}

async function reserveBotDuelUseForToday(guildId, userId, tokenId, duelId) {
  const pool = deps?.historyPool;
  if (!pool?.query) {
    return { ok: false, reason: 'Bot duel tracking is unavailable right now.' };
  }
  const normalizedTokenId = String(tokenId || '').trim();
  if (!/^\d+$/.test(normalizedTokenId)) {
    return { ok: false, reason: 'Choose a valid Squig before starting a bot duel.' };
  }
  try {
    const { rows } = await pool.query(
      `INSERT INTO squig_duel_bot_daily_uses
       (guild_id, token_id, played_on, user_id, duel_id)
       VALUES ($1, $2, CURRENT_DATE, $3, $4)
       ON CONFLICT (guild_id, token_id, played_on) DO NOTHING
       RETURNING played_on::TEXT AS played_on`,
      [String(guildId), normalizedTokenId, String(userId), String(duelId)]
    );
    if (rows[0]) return { ok: true, playedOn: rows[0].played_on };
    const existing = await getBotDuelUseForToday(guildId, normalizedTokenId);
    return {
      ok: false,
      reason: `Squig #${normalizedTokenId} has already played a bot duel today. Each Squig can only play the bot once per day.`,
      existing,
    };
  } catch (err) {
    console.warn('[SquigDuels] bot daily use reserve failed:', String(err?.message || err || ''));
    return { ok: false, reason: 'Could not reserve this Squig for a bot duel right now.' };
  }
}

async function releaseBotDuelUseForToday(guildId, tokenId, duelId) {
  const pool = deps?.historyPool;
  if (!pool?.query) return;
  const normalizedTokenId = String(tokenId || '').trim();
  if (!/^\d+$/.test(normalizedTokenId)) return;
  try {
    await pool.query(
      `DELETE FROM squig_duel_bot_daily_uses
       WHERE guild_id = $1 AND token_id = $2 AND played_on = CURRENT_DATE AND duel_id = $3`,
      [String(guildId), normalizedTokenId, String(duelId)]
    );
  } catch (err) {
    console.warn('[SquigDuels] bot daily use release failed:', String(err?.message || err || ''));
  }
}

function applySavedSquigNameToActiveDuels(guildId, userId, tokenId, nickname) {
  const normalizedTokenId = String(tokenId || '');
  for (const duel of duels.values()) {
    if (String(duel.guildId) !== String(guildId)) continue;
    if (String(duel.challengerId) === String(userId) && String(duel.challengerSquigTokenId) === normalizedTokenId) {
      duel.challengerSquigName = nickname || null;
    }
    if (String(duel.opponentId) === String(userId) && String(duel.opponentSquigTokenId) === normalizedTokenId) {
      duel.opponentSquigName = nickname || null;
    }
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
      '3. Choose your $CHARM wager and opponent response time\n' +
      '4. Select one of your wallet-linked Squigs\n' +
      '5. Opponent accepts and matches the wager\n' +
      '6. Both Squigs battle round by round until one hits 0 HP\n\n' +
      '**Stats:**\n' +
      '- Squig HP is based on UglyPoints\n' +
      '- Higher UglyPoints helps, but strategy matters\n' +
      '- Every round you choose Attack, Defend, Heal, or Panic\n\n' +
      '**Actions:**\n' +
      'Attack — damage your opponent unless blocked\n' +
      'Defend — block attacks and reduce enemy healing\n' +
      'Heal — recover HP; stronger against attacks\n' +
      'Panic — force chaos, but both Squigs lose HP\n\n' +
      '**Bot Duel:**\n' +
      `Choose a wager up to ${formatCharm(BOT_DUEL_MAX_WAGER)} $CHARM. Win to receive ${BOT_DUEL_PAYOUT_MULTIPLIER}x your wager; lose and the bot keeps it.\n` +
      'Each Squig can play the bot once per day.'
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
      .setCustomId('sd:my_squig')
      .setLabel('Manage Squigs')
      .setStyle(ButtonStyle.Success),
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

async function ensureHolderThreadViewAccess(guild, thread) {
  const roleId = holderRoleId();
  if (!roleId || !thread) return;
  const parent = thread.parentId
    ? await guild.channels.fetch(thread.parentId).catch(() => null)
    : thread.parent;
  if (!parent?.permissionOverwrites?.edit) return;

  const current = parent.permissionsFor(roleId);
  if (
    current?.has(PermissionFlagsBits.ViewChannel) &&
    current?.has(PermissionFlagsBits.ReadMessageHistory)
  ) {
    return;
  }

  await parent.permissionOverwrites.edit(
    roleId,
    {
      ViewChannel: true,
      ReadMessageHistory: true,
    },
    { reason: 'Allow holders to spectate Squig Duel threads' }
  ).catch((err) => {
    console.warn('[SquigDuels] failed to grant holder thread visibility:', String(err?.message || err || ''));
  });
}

function parseWager(input, maxAmount = MAX_DUEL_WAGER) {
  const raw = String(input || '').replace(/,/g, '').trim();
  if (!/^\d+$/.test(raw)) return null;
  const amount = Number(raw);
  if (!Number.isSafeInteger(amount) || amount <= 0) return null;
  if (amount > maxAmount) return null;
  return amount;
}

function parseAcceptTimeoutMinutes(input) {
  const raw = String(input || '').trim().toLowerCase();
  const match = raw.match(/^(\d+)(?:\s*(?:m|min|mins|minute|minutes))?$/);
  if (!match) return null;
  const minutes = Number(match[1]);
  return ACCEPT_TIMEOUT_MINUTE_OPTIONS.includes(minutes) ? minutes : null;
}

function acceptTimeoutMs(duel) {
  const ms = Number(duel?.acceptTimeoutMs);
  return Number.isFinite(ms) && ms > 0 ? ms : DEFAULT_ACCEPT_TIMEOUT_MS;
}

function formatAcceptTimeout(ms) {
  const minutes = Math.max(1, Math.round((Number(ms) || DEFAULT_ACCEPT_TIMEOUT_MS) / 60000));
  return `${minutes} minute${minutes === 1 ? '' : 's'}`;
}

function challengerSetupModal(duelId) {
  const modal = new ModalBuilder()
    .setCustomId(`sd:setup:${duelId}`)
    .setTitle('Set Squig Duel Rules');
  modal.addComponents(
    new ActionRowBuilder().addComponents(
      new TextInputBuilder()
        .setCustomId('wager')
        .setLabel('Wager amount in $CHARM')
        .setRequired(true)
        .setPlaceholder('100')
        .setStyle(TextInputStyle.Short)
    ),
    new ActionRowBuilder().addComponents(
      new TextInputBuilder()
        .setCustomId('accept_timeout_minutes')
        .setLabel('Opponent time: 1, 2, 3, or 5 min')
        .setRequired(true)
        .setPlaceholder(String(DEFAULT_ACCEPT_TIMEOUT_MINUTES))
        .setStyle(TextInputStyle.Short)
    )
  );
  return modal;
}

function botDuelSetupModal(userId) {
  const modal = new ModalBuilder()
    .setCustomId(`sd:bot_setup:${userId}`)
    .setTitle('Set Bot Duel Wager');
  modal.addComponents(
    new ActionRowBuilder().addComponents(
      new TextInputBuilder()
        .setCustomId('wager')
        .setLabel(`Wager amount, max ${formatCharm(BOT_DUEL_MAX_WAGER)}`)
        .setRequired(true)
        .setPlaceholder(String(BOT_DUEL_WAGER))
        .setStyle(TextInputStyle.Short)
    )
  );
  return modal;
}

function opponentSelectRows(duelId, allowOpenChallenge = false) {
  const rows = [
    new ActionRowBuilder().addComponents(
      new UserSelectMenuBuilder()
        .setCustomId(`sd:opponent:${duelId}`)
        .setPlaceholder('Start typing to select your opponent')
        .setMinValues(1)
        .setMaxValues(1)
    ),
  ];
  if (allowOpenChallenge) {
    rows.push(new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`sd:open:${duelId}`)
        .setLabel('Open Challenge')
        .setStyle(ButtonStyle.Primary)
    ));
  }
  return rows;
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
    challengerSquigName: null,
    opponentSquigName: null,
    challengerUglyPoints: null,
    opponentUglyPoints: null,
    challengerMaxHp: null,
    opponentMaxHp: null,
    challengerCurrentHp: null,
    opponentCurrentHp: null,
    currentRound: 0,
    currentActions: {},
    readyUsers: {},
    status: 'setup',
    isBotDuel,
    openChallenge: false,
    acceptTimeoutMs: DEFAULT_ACCEPT_TIMEOUT_MS,
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
  await interaction.deferReply({ flags: 64 });
  const duelId = randomId();
  const thread = await interaction.channel.threads.create({
    name: `Squig Duel-${interaction.user.username}`.slice(0, 90),
    autoArchiveDuration: 1440,
    type: ChannelType.PublicThread,
    reason: `Squig Duel created by ${interaction.user.tag}`,
  });
  await ensureHolderThreadViewAccess(interaction.guild, thread);

  const duel = createBaseDuel({ interaction, duelId, thread });
  duels.set(duelId, duel);
  registerActiveUser(interaction.user.id, duelId);
  armSetupTimeout(interaction.guild, duel);
  await thread.members.add(interaction.user.id).catch(() => null);
  await persistDuel(duel);
  await logDuel(interaction.guild, 'Created', `Duel \`${duelId}\` created by <@${interaction.user.id}> in <#${thread.id}>.`);

  const allowOpenChallenge = isAdmin(interaction);
  await thread.send({
    content:
      `<@${interaction.user.id}> started a Squig Duel setup.\n` +
      `Holders can spectate here. Only duel participants and admins should write in this thread.\n\n` +
      (allowOpenChallenge
        ? `Select your opponent, or post an open challenge for holders to accept.`
        : `Select your opponent. Start typing their name in the picker below.`),
    components: opponentSelectRows(duelId, allowOpenChallenge),
  });
  await interaction.editReply({ content: `Duel thread created: <#${thread.id}>. Continue setup there.` });
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
  await interaction.showModal(botDuelSetupModal(interaction.user.id));
  return true;
}

async function handleBotDuelSetupModal(interaction) {
  const match = interaction.customId.match(/^sd:bot_setup:(\d{16,22})$/);
  if (!match) return false;
  assertReady();
  if (interaction.user.id !== match[1]) {
    await interaction.reply({ content: 'This bot duel setup is not for you.', flags: 64 });
    return true;
  }
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
  const wagerAmount = parseWager(interaction.fields.getTextInputValue('wager'), BOT_DUEL_MAX_WAGER);
  if (!wagerAmount) {
    await interaction.reply({
      content: `Bot Duel wager must be a whole number between 1 and ${formatCharm(BOT_DUEL_MAX_WAGER)} $CHARM.`,
      flags: 64,
    });
    return true;
  }
  await interaction.deferReply({ flags: 64 });
  const eligibility = await checkDuelWagerEligibility(interaction.guild, interaction.user.id, wagerAmount);
  if (!eligibility.ok) {
    await interaction.editReply({
      content: eligibility.reason || `You need ${formatCharm(wagerAmount)} $CHARM to start this Bot Duel.`,
    });
    return true;
  }

  const duelId = randomId();
  const thread = await interaction.channel.threads.create({
    name: `Bot Squig Duel-${interaction.user.username}`.slice(0, 90),
    autoArchiveDuration: 1440,
    type: ChannelType.PublicThread,
    reason: `Bot Squig Duel created by ${interaction.user.tag}`,
  });
  await ensureHolderThreadViewAccess(interaction.guild, thread);

  const duel = createBaseDuel({
    interaction,
    duelId,
    thread,
    opponentId: botUserId(),
    wagerAmount,
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
    `Wager: ${formatCharm(wagerAmount)} $CHARM. Win and receive ${formatCharm(wagerAmount * BOT_DUEL_PAYOUT_MULTIPLIER)} $CHARM. Lose and the bot keeps the wager.\n` +
    `Each Squig can only play the bot once per day.`
  );
  await interaction.editReply({ content: `Bot duel thread created: <#${thread.id}>. Continue setup there.` });
  await sendSquigSelectionPrompt(interaction.guild, duel, 'challenger');
  return true;
}

async function getSpendable(guildId, userId) {
  const result = await deps.getMarketplaceSpendableBalance(guildId, userId);
  if (!result.ok) {
    if (!(await hasConnectedWallet(guildId, userId))) {
      return { ...result, reason: walletConnectMessage() };
    }
    return result;
  }

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

async function checkDuelWagerEligibility(guild, userId, amount = null) {
  const spendable = await getSpendable(guild.id, userId);
  if (!spendable.ok) return spendable;
  if (amount != null && spendable.balance < amount) {
    return {
      ok: false,
      reason: `You need ${formatCharm(amount)} $CHARM to join this Squig Duel.`,
      balance: spendable.balance,
      spendable,
    };
  }
  return spendable;
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
      requireTransfer: true,
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
      requireTransfer: true,
    }
  );
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
  const hadChallengerWager = Boolean(duel.challengerPaid);
  const hadOpponentWager = Boolean(duel.opponentPaid);
  const failures = await refundPaidWagers(guild, duel, reason);
  if (isBotDuel(duel) && duel.botDailyUseReserved && duel.challengerSquigTokenId) {
    await releaseBotDuelUseForToday(guild.id, duel.challengerSquigTokenId, duel.id);
    duel.botDailyUseReserved = false;
  }
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
  const refunded = [];
  if (hadChallengerWager && !duel.challengerPaid) refunded.push(`<@${duel.challengerId}>`);
  if (hadOpponentWager && duel.opponentId && !duel.opponentPaid) refunded.push(`<@${duel.opponentId}>`);
  await postPublicDuelFinalLog(
    guild,
    `Squig Duel final log\n` +
    `Reason: ${reason}\n` +
    `Refund: ${refunded.length ? `${formatCharm(duel.wagerAmount)} $CHARM each to ${refunded.join(' and ')}` : 'No paid wagers to refund.'}` +
    (failures.length ? `\nRefund issue(s): ${failures.join(' | ')}` : '')
  );
  scheduleDuelThreadDeletion(guild, duel);
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

function sortSquigsForDisplay(squigs) {
  squigs.sort((a, b) => {
    const fav = Number(Boolean(b.isFavorite)) - Number(Boolean(a.isFavorite));
    if (fav) return fav;
    const up = Number(b.uglyPoints) - Number(a.uglyPoints);
    if (up) return up;
    return Number(a.tokenId) - Number(b.tokenId);
  });
  return squigs;
}

async function fetchOwnedSquigs(guildId, userId) {
  const links = await deps.getWalletLinks(guildId, userId);
  const walletAddresses = links.map((x) => x.wallet_address).filter(Boolean);
  if (!walletAddresses.length) {
    return { ok: false, reason: walletConnectMessage('You must link your wallet before joining a Squig Duel.') };
  }

  const tokenIds = await deps.getOwnedTokenIdsForContractMany(walletAddresses, SQUIGS_CONTRACT);
  if (!tokenIds.length) {
    return { ok: false, reason: 'No Squigs found in your connected wallet.' };
  }

  const savedProfiles = await getSavedSquigProfiles(guildId, userId);
  const squigs = [];
  for (const tokenId of tokenIds.slice(0, 200)) {
    try {
      const uglyPoints = await calculateUglyPoints(guildId, tokenId);
      if (!Number.isFinite(Number(uglyPoints))) continue;
      const hp = balancedHp(uglyPoints);
      const savedProfile = savedProfiles.get(String(tokenId));
      squigs.push({
        tokenId: String(tokenId),
        nickname: savedProfile?.nickname || '',
        isFavorite: Boolean(savedProfile?.isFavorite),
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

  sortSquigsForDisplay(squigs);
  return { ok: true, squigs, links, savedProfiles };
}

function squigPageCount(squigs) {
  return Math.max(1, Math.ceil((Array.isArray(squigs) ? squigs.length : 0) / MAX_SELECT_OPTIONS));
}

function clampSquigPage(squigs, page = 0) {
  const maxPage = squigPageCount(squigs) - 1;
  return Math.max(0, Math.min(maxPage, Math.floor(Number(page) || 0)));
}

function squigPageItems(squigs, page = 0) {
  const safePage = clampSquigPage(squigs, page);
  const start = safePage * MAX_SELECT_OPTIONS;
  return squigs.slice(start, start + MAX_SELECT_OPTIONS);
}

function squigPageLabel(squigs, page = 0) {
  const safePage = clampSquigPage(squigs, page);
  const total = Array.isArray(squigs) ? squigs.length : 0;
  const start = total ? (safePage * MAX_SELECT_OPTIONS) + 1 : 0;
  const end = Math.min(total, (safePage + 1) * MAX_SELECT_OPTIONS);
  return `Showing ${start}-${end} of ${total} Squigs`;
}

function buildSquigPageButtons(prefix, page, pageCount) {
  if (pageCount <= 1) return [];
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`${prefix}:${Math.max(0, page - 1)}`)
        .setLabel('Previous')
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(page <= 0),
      new ButtonBuilder()
        .setCustomId(`${prefix}:${Math.min(pageCount - 1, page + 1)}`)
        .setLabel('Next')
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(page >= pageCount - 1)
    ),
  ];
}

function buildSquigSelectRows(duelId, side, squigs, page = 0) {
  const safePage = clampSquigPage(squigs, page);
  const pageItems = squigPageItems(squigs, safePage);
  const options = pageItems.map((s) => ({
    label: squigOptionLabel(s),
    description: squigOptionDescription(s),
    value: String(s.tokenId),
  }));
  return [
    new ActionRowBuilder().addComponents(
      new StringSelectMenuBuilder()
        .setCustomId(`sd:select:${duelId}:${side}`)
        .setPlaceholder('Choose your Squig')
        .addOptions(options)
    ),
    ...buildSquigPageButtons(`sd:select_page:${duelId}:${side}`, safePage, squigPageCount(squigs)),
  ];
}

function buildViewSquigRows(userId, squigs, page = 0) {
  const safePage = clampSquigPage(squigs, page);
  const pageItems = squigPageItems(squigs, safePage);
  const options = pageItems.map((s) => ({
    label: squigDisplayName(s).slice(0, 100),
    description: `${s.isFavorite ? 'Favorite | ' : ''}${s.uglyPoints} UglyPoints | ${s.maxHp} HP | ${s.attackPower} Attack`.slice(0, 100),
    value: String(s.tokenId),
  }));
  return [
    new ActionRowBuilder().addComponents(
      new StringSelectMenuBuilder()
        .setCustomId(`sd:view_select:${userId}`)
        .setPlaceholder('Scroll to view your Squigs')
        .addOptions(options)
    ),
    ...buildSquigPageButtons(`sd:view_page:${userId}`, safePage, squigPageCount(squigs)),
  ];
}

function buildSquigSelectionEmbed(squigs, page = 0) {
  const safePage = clampSquigPage(squigs, page);
  const shown = squigPageItems(squigs, safePage);
  const lines = shown.slice(0, 12).map((s) => squigListLine(s));
  const embed = new EmbedBuilder()
    .setTitle('Choose Your Squig')
    .setColor(0xB0DEEE)
    .setDescription(lines.join('\n') || 'Select one of your owned Squigs below.')
    .setFooter({ text: `${squigPageLabel(squigs, safePage)} sorted by ${SQUIG_SORT_LABEL}` });
  if (shown[0]?.imageUrl) {
    embed.setImage(shown[0].imageUrl);
    embed.setFooter({ text: `${squigPageLabel(squigs, safePage)} sorted by ${SQUIG_SORT_LABEL} | Preview: ${squigDisplayName(shown[0])}` });
  }
  return embed;
}

function buildOwnedSquigsEmbed(user, squigs, selectedTokenId = null, page = 0) {
  const safePage = clampSquigPage(squigs, page);
  const pageItems = squigPageItems(squigs, safePage);
  const selected = selectedTokenId
    ? squigs.find((s) => String(s.tokenId) === String(selectedTokenId))
    : pageItems[0];
  const lines = pageItems.map((s) => squigListLine(s));
  const embed = new EmbedBuilder()
    .setTitle(`${user.username}'s Squigs`)
    .setColor(0xB0DEEE)
    .setDescription(lines.join('\n') || 'No Squigs found.')
    .setFooter({
      text: squigs.length > MAX_SELECT_OPTIONS
        ? `${squigPageLabel(squigs, safePage)} sorted by ${SQUIG_SORT_LABEL}`
        : `${squigs.length} Squig${squigs.length === 1 ? '' : 's'} found`,
    });
  if (selected?.imageUrl) {
    embed.setImage(selected.imageUrl);
    embed.addFields({
      name: `Selected ${squigDisplayName(selected)}`,
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
    page: 0,
  });
  const extra = result.squigs.length > MAX_SELECT_OPTIONS
    ? ` Use Previous and Next to browse all ${result.squigs.length} Squigs.`
    : '';
  await interaction.editReply({
    content: `Your Squigs are listed below.${extra}`,
    embeds: [buildOwnedSquigsEmbed(interaction.user, result.squigs, null, 0)],
    components: buildViewSquigRows(interaction.user.id, result.squigs, 0),
  });
  return true;
}

async function handleViewSquigPageButton(interaction) {
  const match = interaction.customId.match(/^sd:view_page:(\d{16,22}):(\d+)$/);
  if (!match) return false;
  assertReady();
  if (interaction.user.id !== match[1]) {
    await interaction.reply({ content: 'This Squig viewer is not for you.', flags: 64 });
    return true;
  }
  const state = pendingSquigViews.get(`${interaction.guild.id}:${interaction.user.id}`);
  if (!state?.squigs?.length) {
    await interaction.reply({ content: 'This Squig viewer expired. Click View Squigs again.', flags: 64 });
    return true;
  }
  const page = clampSquigPage(state.squigs, match[2]);
  state.page = page;
  await interaction.update({
    content: `Your Squigs are listed below. ${squigPageLabel(state.squigs, page)}.`,
    embeds: [buildOwnedSquigsEmbed(interaction.user, state.squigs, null, page)],
    components: buildViewSquigRows(interaction.user.id, state.squigs, page),
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
    content: `Selected ${squigDisplayName(selected)}.`,
    embeds: [buildOwnedSquigsEmbed(interaction.user, state.squigs, selected.tokenId, state.page || 0)],
    components: buildViewSquigRows(interaction.user.id, state.squigs, state.page || 0),
  });
  return true;
}

function mySquigStateKey(guildId, userId) {
  return `${guildId}:${userId}`;
}

function selectedMySquig(state) {
  return state?.squigsById?.get(String(state.selectedTokenId || '')) || null;
}

function normalizeSquigNickname(input) {
  return String(input || '').trim().replace(/\s+/g, ' ').slice(0, 40);
}

function mySquigNameModal(userId, currentName = '') {
  const modal = new ModalBuilder()
    .setCustomId(`sd:my_squig_name_modal:${userId}`)
    .setTitle('Name Your Squig');
  const input = new TextInputBuilder()
    .setCustomId('squig_name')
    .setLabel('Squig name')
    .setRequired(false)
    .setPlaceholder('Blank clears the saved name')
    .setStyle(TextInputStyle.Short)
    .setMaxLength(40);
  if (currentName) input.setValue(currentName);
  modal.addComponents(new ActionRowBuilder().addComponents(input));
  return modal;
}

function buildMySquigRows(userId, state) {
  const squigs = state.squigs || [];
  const page = clampSquigPage(squigs, state.page || 0);
  const options = squigPageItems(squigs, page).map((s) => ({
    label: squigOptionLabel(s),
    description: squigOptionDescription(s),
    value: String(s.tokenId),
    default: String(s.tokenId) === String(state.selectedTokenId || ''),
  }));
  const selected = selectedMySquig(state);
  return [
    new ActionRowBuilder().addComponents(
      new StringSelectMenuBuilder()
        .setCustomId(`sd:my_squig_select:${userId}`)
        .setPlaceholder('Choose your saved Squig')
        .addOptions(options)
    ),
    ...buildSquigPageButtons(`sd:my_squig_page:${userId}`, page, squigPageCount(squigs)),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`sd:my_squig_name:${userId}`)
        .setLabel('Name')
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(!selected),
      new ButtonBuilder()
        .setCustomId(`sd:my_squig_favorite:${userId}`)
        .setLabel(selected?.isFavorite ? 'Unfavorite' : 'Favorite')
        .setStyle(selected?.isFavorite ? ButtonStyle.Secondary : ButtonStyle.Success)
        .setDisabled(!selected),
      new ButtonBuilder()
        .setCustomId(`sd:my_squig_save:${userId}`)
        .setLabel('Save')
        .setStyle(ButtonStyle.Primary)
        .setDisabled(!selected)
    ),
  ];
}

function buildMySquigEmbed(user, state) {
  const squigs = state.squigs || [];
  const page = clampSquigPage(squigs, state.page || 0);
  const selected = selectedMySquig(state);
  const lines = squigPageItems(squigs, page).slice(0, 12).map((s) => squigListLine(s));
  const embed = new EmbedBuilder()
    .setTitle(`${user.username}'s Saved Squigs`)
    .setColor(0xB0DEEE)
    .setDescription(
      `${squigPageLabel(squigs, page)} sorted by ${SQUIG_SORT_LABEL}.\n\n` +
      (lines.join('\n') || 'Choose one of your owned Squigs below.')
    );
  if (selected?.imageUrl) embed.setImage(selected.imageUrl);
  if (selected) {
    embed.addFields({
      name: squigDisplayName(selected),
      value:
        `Token: **#${selected.tokenId}**\n` +
        `Favorite: **${selected.isFavorite ? 'Yes' : 'No'}**\n` +
        `UglyPoints: **${selected.uglyPoints}**\n` +
        `Balanced HP: **${selected.maxHp}**\n` +
        `Attack Power: **${selected.attackPower}**`,
      inline: false,
    });
  }
  embed.setFooter({
    text: state.saved
      ? 'Saved. This Squig profile will persist through bot updates.'
      : `Choose a Squig, optionally name/favorite it, then press Save. Up to ${MAX_FAVORITE_SQUIGS} favorites.`,
  });
  return embed;
}

function buildMySquigPayload(interaction, state, content = null) {
  const selected = selectedMySquig(state);
  const defaultContent = selected
    ? `Editing saved Squig profiles. Selected ${squigDisplayName(selected)}.`
    : 'Editing saved Squig profiles.';
  return {
    content: content || defaultContent,
    embeds: [buildMySquigEmbed(interaction.user, state)],
    components: buildMySquigRows(interaction.user.id, state),
  };
}

async function updateMySquigInteraction(interaction, state, content = null) {
  const payload = buildMySquigPayload(interaction, state, content);
  if (typeof interaction.update === 'function') {
    try {
      await interaction.update(payload);
      return;
    } catch {}
  }
  if (interaction.deferred || interaction.replied) {
    await interaction.editReply(payload).catch(() => null);
    return;
  }
  await interaction.reply({ ...payload, flags: 64 }).catch(() => null);
}

async function handleSetMySquigButton(interaction) {
  if (interaction.customId !== 'sd:my_squig') return false;
  assertReady();
  await interaction.deferReply({ flags: 64 });
  const result = await fetchOwnedSquigs(interaction.guild.id, interaction.user.id);
  if (!result.ok) {
    await interaction.editReply({ content: result.reason });
    return true;
  }
  const selectedTokenId =
    result.squigs.find((s) => s.isFavorite)?.tokenId ||
    result.squigs.find((s) => String(s.nickname || '').trim())?.tokenId ||
    result.squigs[0]?.tokenId;
  const initiallySelected = result.squigs.find((s) => String(s.tokenId) === String(selectedTokenId));
  const state = {
    createdAt: Date.now(),
    userId: interaction.user.id,
    squigs: result.squigs,
    squigsById: new Map(result.squigs.map((s) => [String(s.tokenId), s])),
    page: selectedTokenId
      ? Math.floor(result.squigs.findIndex((s) => String(s.tokenId) === String(selectedTokenId)) / MAX_SELECT_OPTIONS)
      : 0,
    selectedTokenId,
    saved: Boolean(initiallySelected?.nickname || initiallySelected?.isFavorite),
  };
  pendingMySquigProfiles.set(mySquigStateKey(interaction.guild.id, interaction.user.id), state);
  const extra = result.squigs.length > MAX_SELECT_OPTIONS
    ? ` Use Previous and Next to browse all ${result.squigs.length} Squigs.`
    : '';
  const ownedTokenIds = new Set(result.squigs.map((s) => String(s.tokenId)));
  const missingSavedCount = [...(result.savedProfiles || new Map()).keys()].filter((tokenId) => !ownedTokenIds.has(String(tokenId))).length;
  const missingSaved = missingSavedCount
    ? ` ${missingSavedCount} saved Squig profile${missingSavedCount === 1 ? ' is' : 's are'} not currently in your connected wallet.`
    : '';
  await interaction.editReply(buildMySquigPayload(
    interaction,
    state,
    `Manage saved names and favorites for your Squigs.${extra}${missingSaved}`
  ));
  return true;
}

async function handleMySquigPageButton(interaction) {
  const match = interaction.customId.match(/^sd:my_squig_page:(\d{16,22}):(\d+)$/);
  if (!match) return false;
  assertReady();
  if (interaction.user.id !== match[1]) {
    await interaction.reply({ content: 'This Squig profile editor is not for you.', flags: 64 });
    return true;
  }
  const state = pendingMySquigProfiles.get(mySquigStateKey(interaction.guild.id, interaction.user.id));
  if (!state?.squigs?.length) {
    await interaction.reply({ content: 'This editor expired. Click Manage Squigs again.', flags: 64 });
    return true;
  }
  state.page = clampSquigPage(state.squigs, match[2]);
  await updateMySquigInteraction(interaction, state, `Editing saved Squig profiles. ${squigPageLabel(state.squigs, state.page)}.`);
  return true;
}

async function handleMySquigSelect(interaction) {
  const match = interaction.customId.match(/^sd:my_squig_select:(\d{16,22})$/);
  if (!match) return false;
  assertReady();
  if (interaction.user.id !== match[1]) {
    await interaction.reply({ content: 'This Squig profile editor is not for you.', flags: 64 });
    return true;
  }
  const state = pendingMySquigProfiles.get(mySquigStateKey(interaction.guild.id, interaction.user.id));
  const tokenId = String(interaction.values?.[0] || '').trim();
  if (!state?.squigsById?.has(tokenId)) {
    await interaction.reply({ content: 'This editor expired. Click Manage Squigs again.', flags: 64 });
    return true;
  }
  state.selectedTokenId = tokenId;
  const selected = state.squigsById.get(tokenId);
  state.saved = Boolean(selected?.nickname || selected?.isFavorite);
  await updateMySquigInteraction(interaction, state, `Selected ${squigDisplayName(selected)}. Add a name or favorite it, then press Save.`);
  return true;
}

async function handleMySquigNameButton(interaction) {
  const match = interaction.customId.match(/^sd:my_squig_name:(\d{16,22})$/);
  if (!match) return false;
  assertReady();
  if (interaction.user.id !== match[1]) {
    await interaction.reply({ content: 'This Squig profile editor is not for you.', flags: 64 });
    return true;
  }
  const state = pendingMySquigProfiles.get(mySquigStateKey(interaction.guild.id, interaction.user.id));
  if (!state || !selectedMySquig(state)) {
    await interaction.reply({ content: 'Choose a Squig before naming it.', flags: 64 });
    return true;
  }
  await interaction.showModal(mySquigNameModal(interaction.user.id, selectedMySquig(state)?.nickname || ''));
  return true;
}

async function handleMySquigNameModal(interaction) {
  const match = interaction.customId.match(/^sd:my_squig_name_modal:(\d{16,22})$/);
  if (!match) return false;
  assertReady();
  if (interaction.user.id !== match[1]) {
    await interaction.reply({ content: 'This Squig profile editor is not for you.', flags: 64 });
    return true;
  }
  const state = pendingMySquigProfiles.get(mySquigStateKey(interaction.guild.id, interaction.user.id));
  if (!state || !selectedMySquig(state)) {
    await interaction.reply({ content: 'This editor expired. Click Manage Squigs again.', flags: 64 });
    return true;
  }
  const selected = selectedMySquig(state);
  const nickname = normalizeSquigNickname(interaction.fields.getTextInputValue('squig_name'));
  if (selected) selected.nickname = nickname;
  state.saved = false;
  await updateMySquigInteraction(interaction, state, nickname ? `Name set to "${nickname}". Press Save to keep it.` : 'Name cleared. Press Save to keep it.');
  return true;
}

async function handleMySquigFavoriteButton(interaction) {
  const match = interaction.customId.match(/^sd:my_squig_favorite:(\d{16,22})$/);
  if (!match) return false;
  assertReady();
  if (interaction.user.id !== match[1]) {
    await interaction.reply({ content: 'This Squig profile editor is not for you.', flags: 64 });
    return true;
  }
  const state = pendingMySquigProfiles.get(mySquigStateKey(interaction.guild.id, interaction.user.id));
  if (!state || !selectedMySquig(state)) {
    await interaction.reply({ content: 'Choose a Squig before marking it as favorite.', flags: 64 });
    return true;
  }
  const selected = selectedMySquig(state);
  if (!selected.isFavorite) {
    const otherFavorites = state.squigs.filter((s) => s.isFavorite && String(s.tokenId) !== String(selected.tokenId)).length;
    if (otherFavorites >= MAX_FAVORITE_SQUIGS) {
      await interaction.reply({
        content: `You can save up to ${MAX_FAVORITE_SQUIGS} favorite Squigs. Unfavorite one before adding another.`,
        flags: 64,
      });
      return true;
    }
  }
  selected.isFavorite = !selected.isFavorite;
  state.saved = false;
  await updateMySquigInteraction(interaction, state, selected.isFavorite ? 'Marked as favorite. Press Save to keep it.' : 'Favorite removed. Press Save to keep it.');
  return true;
}

async function handleMySquigSaveButton(interaction) {
  const match = interaction.customId.match(/^sd:my_squig_save:(\d{16,22})$/);
  if (!match) return false;
  assertReady();
  if (interaction.user.id !== match[1]) {
    await interaction.reply({ content: 'This Squig profile editor is not for you.', flags: 64 });
    return true;
  }
  const state = pendingMySquigProfiles.get(mySquigStateKey(interaction.guild.id, interaction.user.id));
  const selected = selectedMySquig(state);
  if (!state || !selected) {
    await interaction.reply({ content: 'Choose a Squig before saving.', flags: 64 });
    return true;
  }
  const saved = await saveMySquigProfile(interaction.guild.id, interaction.user.id, {
    tokenId: selected.tokenId,
    nickname: selected.nickname,
    isFavorite: selected.isFavorite,
  });
  if (!saved.ok) {
    await interaction.reply({ content: saved.reason || 'Could not save your Squig profile.', flags: 64 });
    return true;
  }
  applySavedSquigNameToActiveDuels(interaction.guild.id, interaction.user.id, selected.tokenId, selected.nickname);
  sortSquigsForDisplay(state.squigs);
  state.squigsById = new Map(state.squigs.map((s) => [String(s.tokenId), s]));
  state.page = Math.floor(state.squigs.findIndex((s) => String(s.tokenId) === String(selected.tokenId)) / MAX_SELECT_OPTIONS);
  state.saved = Boolean(selected.nickname || selected.isFavorite);
  await updateMySquigInteraction(
    interaction,
    state,
    saved.deleted
      ? `Cleared saved profile for Squig #${selected.tokenId}.`
      : `Saved ${squigDisplayName(selected)}. This profile will persist through bot updates.`
  );
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
    squigs: result.squigs,
    page: 0,
    createdAt: Date.now(),
  });
  const extra = result.squigs.length > MAX_SELECT_OPTIONS
    ? `\nUse Previous and Next to browse all ${result.squigs.length} Squigs.`
    : '';
  await interaction.editReply({
    content:
      `Choose your Squig.${extra}\n` +
      `Each option shows Squig ID, UglyPoints, Balanced HP, and attack power.`,
    embeds: [buildSquigSelectionEmbed(result.squigs, 0)],
    components: buildSquigSelectRows(duel.id, side, result.squigs, 0),
  });
}

async function handleSquigSelectionPageButton(interaction) {
  const match = interaction.customId.match(/^sd:select_page:([a-f0-9]{12}):(challenger|opponent):(\d+)$/i);
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
  const pendingKey = `${duel.id}:${side}:${interaction.user.id}`;
  const pending = pendingSquigSelections.get(pendingKey);
  if (!pending?.squigs?.length) {
    await interaction.reply({ content: 'This Squig selection expired. Start your selection again.', flags: 64 });
    return true;
  }
  const page = clampSquigPage(pending.squigs, match[3]);
  pending.page = page;
  await interaction.update({
    content:
      `Choose your Squig.\n` +
      `${squigPageLabel(pending.squigs, page)}. Each option shows Squig ID, UglyPoints, Balanced HP, and attack power.`,
    embeds: [buildSquigSelectionEmbed(pending.squigs, page)],
    components: buildSquigSelectRows(duel.id, side, pending.squigs, page),
  });
  return true;
}

async function sendSquigSelectionPrompt(guild, duel, side) {
  const thread = await guild.channels.fetch(duel.threadId).catch(() => null);
  if (!thread?.isTextBased()) return;
  await promptSquigSelection(
    {
      guild,
      editReply: (payload) => thread.send(payload),
    },
    duel,
    side
  );
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
  const opponentEligibility = await checkDuelWagerEligibility(interaction.guild, opponentId);
  if (!opponentEligibility.ok) {
    const reason = !(await hasConnectedWallet(interaction.guild.id, opponentId))
      ? walletConnectMessageForUser(opponentId)
      : (opponentEligibility.reason || 'They are not eligible for Squig Duels right now.');
    await interaction.reply({
      content: `That opponent cannot be selected yet. ${reason}`,
      flags: 64,
    });
    return true;
  }

  if (duel.opponentId && duel.opponentId !== opponentId) {
    activeUserToDuel.delete(String(duel.opponentId));
  }
  duel.opponentId = opponentId;
  await persistDuel(duel);
  await interaction.showModal(challengerSetupModal(duel.id));
  return true;
}

async function handleOpenChallengeButton(interaction) {
  const match = interaction.customId.match(/^sd:open:([a-f0-9]{12})$/i);
  if (!match) return false;
  assertReady();

  const duel = getDuel(match[1]);
  if (!duel || duel.status !== 'setup') {
    await interaction.reply({ content: 'This duel setup expired or is no longer valid.', flags: 64 });
    return true;
  }
  if (interaction.user.id !== duel.challengerId) {
    await interaction.reply({ content: 'Only the challenger can open this challenge.', flags: 64 });
    return true;
  }
  if (!isAdmin(interaction)) {
    await interaction.reply({ content: 'Open Challenge is admin only.', flags: 64 });
    return true;
  }

  if (duel.opponentId) {
    activeUserToDuel.delete(String(duel.opponentId));
  }
  duel.opponentId = null;
  duel.openChallenge = true;
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

  const isOpenChallenge = Boolean(duel.openChallenge);
  const opponentId = duel.opponentId;
  const wagerAmount = parseWager(interaction.fields.getTextInputValue('wager'));
  const acceptTimeoutMinutes = parseAcceptTimeoutMinutes(interaction.fields.getTextInputValue('accept_timeout_minutes'));
  if (!opponentId && !isOpenChallenge) {
    await interaction.reply({ content: 'Select an opponent first.', flags: 64 });
    return true;
  }
  if (opponentId && (opponentId === interaction.user.id || opponentId === botUserId())) {
    await interaction.reply({ content: 'Choose another holder as your opponent.', flags: 64 });
    return true;
  }
  if (!wagerAmount) {
    await interaction.reply({
      content: `Wager must be a whole number between 1 and ${formatCharm(MAX_DUEL_WAGER)} $CHARM.`,
      flags: 64,
    });
    return true;
  }
  if (!acceptTimeoutMinutes) {
    await interaction.reply({
      content: `Opponent response time must be ${ACCEPT_TIMEOUT_MINUTE_OPTIONS.join(', ')} minutes.`,
      flags: 64,
    });
    return true;
  }
  if (opponentId) {
    const opponentActiveDuelId = activeUserToDuel.get(opponentId);
    if (opponentActiveDuelId && opponentActiveDuelId !== duel.id) {
      await interaction.reply({ content: 'That opponent is already in an active Squig Duel.', flags: 64 });
      return true;
    }
    if (!(await hasHolderRole(interaction.guild, opponentId))) {
      await interaction.reply({ content: 'That opponent does not have the holder role.', flags: 64 });
      return true;
    }
  }
  const challengerEligibility = await checkDuelWagerEligibility(interaction.guild, interaction.user.id, wagerAmount);
  if (!challengerEligibility.ok) {
    await interaction.reply({
      content: challengerEligibility.reason || walletConnectMessage('You need to connect your wallet before starting a Squig Duel.'),
      flags: 64,
    });
    return true;
  }
  if (opponentId) {
    const opponentEligibility = await checkDuelWagerEligibility(interaction.guild, opponentId, wagerAmount);
    if (!opponentEligibility.ok) {
      if (duel.opponentId) {
        activeUserToDuel.delete(String(duel.opponentId));
      }
      duel.opponentId = null;
      await persistDuel(duel);
      const reason = opponentEligibility.balance != null
        ? `They need ${formatCharm(wagerAmount)} $CHARM but only have ${formatCharm(opponentEligibility.balance)}.`
        : (!(await hasConnectedWallet(interaction.guild.id, opponentId))
          ? walletConnectMessageForUser(opponentId)
          : (opponentEligibility.reason || 'They are not eligible for Squig Duels right now.'));
      await interaction.reply({
        content: `That opponent cannot be selected for this wager. ${reason} Select another opponent to continue.`,
        flags: 64,
      });
      return true;
    }
  }

  duel.opponentId = opponentId || null;
  duel.openChallenge = isOpenChallenge;
  duel.wagerAmount = wagerAmount;
  duel.acceptTimeoutMs = acceptTimeoutMinutes * 60 * 1000;
  if (duel.setupTimeout) clearTimeout(duel.setupTimeout);
  duel.setupTimeout = null;
  if (opponentId) registerActiveUser(opponentId, duel.id);
  await persistDuel(duel);
  const thread = await interaction.guild.channels.fetch(duel.threadId).catch(() => null);
  if (opponentId) await thread?.members?.add(opponentId).catch(() => null);
  await interaction.reply({ content: 'Setup saved. Choose your Squig in this thread.', flags: 64 });
  await sendSquigSelectionPrompt(interaction.guild, duel, 'challenger');
  return true;
}

function challengeRows(duel) {
  const buttons = [
    new ButtonBuilder()
      .setCustomId(`sd:accept:${duel.id}`)
      .setLabel('Accept Duel')
      .setStyle(ButtonStyle.Success),
  ];
  if (!duel.openChallenge) {
    buttons.push(
      new ButtonBuilder()
        .setCustomId(`sd:decline:${duel.id}`)
        .setLabel('Decline Duel')
        .setStyle(ButtonStyle.Secondary)
    );
  }
  return [new ActionRowBuilder().addComponents(buttons)];
}

function buildChallengeEmbed(duel) {
  const tokenId = duel.challengerSquigTokenId || 'Unknown';
  const imageUrl = squigImageUrl(tokenId);
  const challengerSquigName = squigNameForSide(duel, 'challenger');
  const embed = new EmbedBuilder()
    .setTitle(duel.openChallenge ? 'Open Squig Duel Challenge' : 'Squig Duel Challenge')
    .setColor(0xd4a43b)
    .setDescription(
      `<@${duel.challengerId}> has chosen **${challengerSquigName}**.\n` +
      (duel.openChallenge
        ? `Any eligible holder can accept this duel and match the wager.`
        : `<@${duel.opponentId}>, accept the duel to choose your Squig and match the wager.`)
    )
    .addFields(
      {
        name: challengerSquigName,
        value:
          `Squig ID: **#${tokenId}**\n` +
          `UglyPoints: **${duel.challengerUglyPoints ?? 'Unknown'}**\n` +
          `HP: **${duel.challengerMaxHp ?? 'Unknown'}**`,
        inline: true,
      },
      {
        name: 'Wager',
        value: `**${formatCharm(duel.wagerAmount)} $CHARM**`,
        inline: true,
      },
      {
        name: 'Response Time',
        value: `**${formatAcceptTimeout(acceptTimeoutMs(duel))}**`,
        inline: true,
      }
    );
  if (imageUrl) embed.setImage(imageUrl);
  return embed;
}

async function postChallenge(guild, duel) {
  const thread = await guild.channels.fetch(duel.threadId).catch(() => null);
  if (!thread?.isTextBased()) return;
  duel.status = 'awaiting_accept';
  await persistDuel(duel);
  const openChallenge = Boolean(duel.openChallenge);
  const responseTimeMs = acceptTimeoutMs(duel);
  const responseTimeText = formatAcceptTimeout(responseTimeMs);
  const botFallbackAllowed = botWagerAmount(duel) <= BOT_DUEL_MAX_WAGER;
  await thread.send({
    content:
      (openChallenge
        ? `<@&${OPEN_CHALLENGE_ROLE_ID}> <@${duel.challengerId}> is looking for a Squig Duel opponent for ${formatCharm(duel.wagerAmount)} $CHARM.\n` +
          (botFallbackAllowed
            ? `First eligible holder to accept within ${responseTimeText} gets the duel. If nobody accepts, this switches to a bot battle.`
            : `First eligible holder to accept within ${responseTimeText} gets the duel. Bot fallback only supports wagers up to ${formatCharm(BOT_DUEL_MAX_WAGER)} $CHARM.`)
        : `<@${duel.challengerId}> has challenged <@${duel.opponentId}> to a Squig Duel for ${formatCharm(duel.wagerAmount)} $CHARM.\n` +
          `<@${duel.opponentId}> has ${responseTimeText} to accept.`),
    embeds: [buildChallengeEmbed(duel)],
    components: challengeRows(duel),
  });
  if (duel.acceptTimeout) clearTimeout(duel.acceptTimeout);
  duel.acceptTimeout = setTimeout(() => {
    const active = getDuel(duel.id);
    if (!active || active.status !== 'awaiting_accept') return;
    if (active.openChallenge) {
      switchOpenChallengeToBot(guild, active).catch((err) => {
        console.warn('[SquigDuels] open challenge bot fallback failed:', String(err?.message || err || ''));
      });
      return;
    }
    cancelDuel(guild, active, 'Opponent did not respond in time.').catch((err) => {
      console.warn('[SquigDuels] accept timeout cancel failed:', String(err?.message || err || ''));
    });
  }, responseTimeMs);
}

async function switchOpenChallengeToBot(guild, duel) {
  if (!duel || duel.status !== 'awaiting_accept' || !duel.openChallenge) return;
  if (!botUserId()) {
    await cancelDuel(guild, duel, 'No holder accepted in time and bot duel fallback is unavailable.');
    return;
  }
  if (botWagerAmount(duel) > BOT_DUEL_MAX_WAGER) {
    await cancelDuel(guild, duel, `No holder accepted in time. Bot duel fallback only supports wagers up to ${formatCharm(BOT_DUEL_MAX_WAGER)} $CHARM.`);
    return;
  }
  const reserve = await reserveBotDuelUseForToday(guild.id, duel.challengerId, duel.challengerSquigTokenId, duel.id);
  if (!reserve.ok) {
    await cancelDuel(guild, duel, reserve.reason || 'This Squig cannot play the bot today.');
    return;
  }
  if (duel.acceptTimeout) clearTimeout(duel.acceptTimeout);
  duel.acceptTimeout = null;
  duel.openChallenge = false;
  duel.isBotDuel = true;
  duel.botDailyUseReserved = true;
  duel.opponentId = botUserId();
  duel.opponentSquigTokenId = duel.challengerSquigTokenId;
  duel.opponentSquigName = duel.challengerSquigName;
  duel.opponentUglyPoints = duel.challengerUglyPoints;
  duel.opponentMaxHp = duel.challengerMaxHp;
  duel.opponentCurrentHp = duel.challengerCurrentHp;
  await persistDuel(duel);

  const thread = await guild.channels.fetch(duel.threadId).catch(() => null);
  if (thread?.isTextBased()) {
    await thread.send('No holder accepted in time. Switching this open challenge to a bot battle.').catch(() => null);
  }
  await startDuel(guild, duel);
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

  await interaction.deferUpdate();

  const ownership = await fetchOwnedSquigs(interaction.guild.id, interaction.user.id);
  if (!ownership.ok || !ownership.squigs.some((s) => String(s.tokenId) === tokenId)) {
    await interaction.followUp({ content: 'That Squig is no longer found in your connected wallet.', flags: 64 }).catch(() => null);
    return true;
  }

  if (side === 'challenger' && isBotDuel(duel)) {
    const reserve = await reserveBotDuelUseForToday(interaction.guild.id, interaction.user.id, chosen.tokenId, duel.id);
    if (!reserve.ok) {
      await interaction.editReply({
        content: `${reserve.reason || 'This Squig cannot play the bot today'} Choose another Squig to continue.`,
        embeds: [buildSquigSelectionEmbed(pending.squigs, pending.page || 0)],
        components: buildSquigSelectRows(duel.id, side, pending.squigs, pending.page || 0),
      }).catch(() => null);
      return true;
    }
    duel.botDailyUseReserved = true;
  }

  pendingSquigSelections.delete(pendingKey);

  if (side === 'challenger') {
    duel.challengerSquigTokenId = chosen.tokenId;
    duel.challengerSquigName = chosen.nickname || null;
    duel.challengerUglyPoints = chosen.uglyPoints;
    duel.challengerMaxHp = chosen.maxHp;
    duel.challengerCurrentHp = chosen.maxHp;
    if (isBotDuel(duel)) {
      duel.opponentSquigTokenId = chosen.tokenId;
      duel.opponentSquigName = chosen.nickname || null;
      duel.opponentUglyPoints = chosen.uglyPoints;
      duel.opponentMaxHp = chosen.maxHp;
      duel.opponentCurrentHp = chosen.maxHp;
    }
    await persistDuel(duel);
    await interaction.editReply({
      content:
        `Selected ${squigDisplayName(chosen)} (${chosen.uglyPoints} UglyPoints, ${chosen.maxHp} HP).\n` +
        `Collecting your ${formatCharm(duel.wagerAmount)} $CHARM wager...`,
      embeds: [],
      components: [],
    });

    const paid = await collectWager(interaction.guild, interaction.user.id, duel.wagerAmount, 'squig_duel_challenger_wager');
    if (!paid.ok) {
      if (isBotDuel(duel) && duel.botDailyUseReserved) {
        await releaseBotDuelUseForToday(interaction.guild.id, chosen.tokenId, duel.id);
        duel.botDailyUseReserved = false;
      }
      await cancelDuel(interaction.guild, duel, paid.reason || 'Challenger wager could not be collected.');
      await interaction.followUp({ content: paid.reason || 'Wager could not be collected.', flags: 64 }).catch(() => null);
      return true;
    }
    duel.challengerPaid = true;
    await persistDuel(duel);
    if (isBotDuel(duel)) {
      if (duel.setupTimeout) clearTimeout(duel.setupTimeout);
      duel.setupTimeout = null;
      await startDuel(interaction.guild, duel);
      await interaction.followUp({
        content: `Bot duel started. Win to receive ${formatCharm(botDuelPayoutAmount(duel))} $CHARM. Lose and the bot keeps your ${formatCharm(botWagerAmount(duel))} $CHARM wager.`,
        flags: 64,
      }).catch(() => null);
      return true;
    }
    await postChallenge(interaction.guild, duel);
    await interaction.followUp({ content: 'Wager collected. Challenge posted in the duel thread.', flags: 64 }).catch(() => null);
    return true;
  }

  duel.opponentSquigTokenId = chosen.tokenId;
  duel.opponentSquigName = chosen.nickname || null;
  duel.opponentUglyPoints = chosen.uglyPoints;
  duel.opponentMaxHp = chosen.maxHp;
  duel.opponentCurrentHp = chosen.maxHp;
  await persistDuel(duel);
  await interaction.editReply({
    content:
      `Selected ${squigDisplayName(chosen)} (${chosen.uglyPoints} UglyPoints, ${chosen.maxHp} HP).\n` +
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
  await postReadyCheck(interaction.guild, duel);
  await interaction.followUp({ content: 'Wager collected. Ready check posted in the duel thread.', flags: 64 }).catch(() => null);
  return true;
}

function readyRows(duel, disabled = false) {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`sd:ready:${duel.id}`)
        .setLabel('Ready')
        .setStyle(ButtonStyle.Success)
        .setDisabled(disabled)
    ),
  ];
}

function readyStatusText(duel) {
  const challengerReady = duel.readyUsers?.challenger ? 'Ready' : 'Waiting';
  const opponentReady = duel.readyUsers?.opponent ? 'Ready' : 'Waiting';
  return `<@${duel.challengerId}>: **${challengerReady}**\n<@${duel.opponentId}>: **${opponentReady}**`;
}

function sideReadyLabel(duel, side) {
  return duel.readyUsers?.[side] ? 'READY' : 'WAITING';
}

function containRect(sourceWidth, sourceHeight, targetWidth, targetHeight) {
  const scale = Math.min(targetWidth / sourceWidth, targetHeight / sourceHeight);
  const width = Math.round(sourceWidth * scale);
  const height = Math.round(sourceHeight * scale);
  return {
    x: Math.round((targetWidth - width) / 2),
    y: Math.round((targetHeight - height) / 2),
    width,
    height,
  };
}

function readyRoundRectPath(ctx, x, y, width, height, radius) {
  const r = Math.min(radius, width / 2, height / 2);
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + width, y, x + width, y + height, r);
  ctx.arcTo(x + width, y + height, x, y + height, r);
  ctx.arcTo(x, y + height, x, y, r);
  ctx.arcTo(x, y, x + width, y, r);
  ctx.closePath();
}

function fillReadyRoundRect(ctx, x, y, width, height, radius, fill, stroke = null, lineWidth = 4) {
  readyRoundRectPath(ctx, x, y, width, height, radius);
  ctx.fillStyle = fill;
  ctx.fill();
  if (stroke) {
    ctx.lineWidth = lineWidth;
    ctx.strokeStyle = stroke;
    ctx.stroke();
  }
}

function drawReadyBadge(ctx, text, x, y, ready) {
  fillReadyRoundRect(ctx, x, y, 176, 48, 24, ready ? '#24b36b' : '#232b2a', '#111816', 4);
  ctx.fillStyle = '#ffffff';
  ctx.font = '900 24px sans-serif';
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText(text, x + 88, y + 25);
}

function drawReadyText(ctx, text, x, y, size = 28, weight = 800, color = '#ffffff', align = 'left') {
  ctx.fillStyle = color;
  ctx.font = `${weight} ${size}px sans-serif`;
  ctx.textAlign = align;
  ctx.textBaseline = 'top';
  ctx.fillText(text, x, y);
}

async function drawReadySquigPanel(ctx, duel, side, panel) {
  const isChallenger = side === 'challenger';
  const tokenId = isChallenger ? duel.challengerSquigTokenId : duel.opponentSquigTokenId;
  const uglyPoints = isChallenger ? duel.challengerUglyPoints : duel.opponentUglyPoints;
  const maxHp = isChallenger ? duel.challengerMaxHp : duel.opponentMaxHp;
  const currentHp = isChallenger ? duel.challengerCurrentHp : duel.opponentCurrentHp;
  const ready = Boolean(duel.readyUsers?.[side]);
  const imageUrl = squigImageUrl(tokenId);
  const accent = isChallenger ? '#d4a43b' : '#7ADDC0';

  fillReadyRoundRect(ctx, panel.x, panel.y, panel.w, panel.h, 34, 'rgba(13, 17, 16, 0.86)', accent, 6);
  const artBox = {
    x: panel.x + 32,
    y: panel.y + 112,
    w: panel.w - 64,
    h: panel.h - 226,
  };
  fillReadyRoundRect(ctx, artBox.x, artBox.y, artBox.w, artBox.h, 24, '#eef5f0', '#111816', 5);

  if (imageUrl) {
    try {
      const squig = await loadImage(await fetchImageBuffer(imageUrl));
      const art = containRect(squig.width, squig.height, artBox.w - 24, artBox.h - 24);
      ctx.drawImage(squig, artBox.x + 12 + art.x, artBox.y + 12 + art.y, art.width, art.height);
    } catch (err) {
      console.warn('[SquigDuels] ready squig image failed:', String(err?.message || err || ''));
    }
  }

  drawReadyText(ctx, isChallenger ? 'CHALLENGER' : 'OPPONENT', panel.x + 34, panel.y + 30, 24, 900, accent);
  drawReadyText(ctx, squigCanvasNameForSide(duel, side), panel.x + 34, panel.y + 62, 34, 900);
  drawReadyBadge(ctx, sideReadyLabel(duel, side), panel.x + panel.w - 210, panel.y + 34, ready);

  const statsY = panel.y + panel.h - 96;
  drawReadyText(ctx, `UP ${uglyPoints ?? 'Unknown'}`, panel.x + 34, statsY, 26, 900, '#ffffff');
  drawReadyText(ctx, `HP ${Math.max(0, currentHp ?? 0)} / ${maxHp ?? 'Unknown'}`, panel.x + 210, statsY, 26, 900, '#ffffff');
  drawReadyText(ctx, `ATK ${baseAttack(uglyPoints)}`, panel.x + 430, statsY, 26, 900, '#ffffff');
}

async function buildReadyDuelAttachment(duel) {
  const width = 1400;
  const height = 900;
  const canvas = createCanvas(width, height);
  const ctx = canvas.getContext('2d');
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = 'high';

  const gradient = ctx.createLinearGradient(0, 0, width, height);
  gradient.addColorStop(0, '#101715');
  gradient.addColorStop(0.55, '#22231c');
  gradient.addColorStop(1, '#0b0f0e');
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, width, height);

  try {
    const lobby = await loadImage(await fetchImageBuffer(SQUIG_DUEL_MENU_IMAGE));
    const bg = coverRect(lobby.width, lobby.height, width, height);
    ctx.globalAlpha = 0.18;
    ctx.drawImage(lobby, bg.x, bg.y, bg.width, bg.height);
    ctx.globalAlpha = 1;
  } catch (err) {
    console.warn('[SquigDuels] ready lobby image failed:', String(err?.message || err || ''));
  }

  drawReadyText(ctx, 'SQUIG DUEL', width / 2, 38, 42, 900, '#f1d66d', 'center');
  drawReadyText(ctx, 'READY CHECK', width / 2, 90, 74, 900, '#ffffff', 'center');
  drawReadyText(ctx, 'Both fighters must lock in before Round 1 starts', width / 2, 174, 26, 700, '#b9c9c2', 'center');

  await drawReadySquigPanel(ctx, duel, 'challenger', { x: 58, y: 232, w: 560, h: 590 });
  await drawReadySquigPanel(ctx, duel, 'opponent', { x: 782, y: 232, w: 560, h: 590 });

  fillReadyRoundRect(ctx, 620, 420, 160, 118, 30, '#f1d66d', '#111816', 7);
  drawReadyText(ctx, 'VS', 700, 444, 58, 900, '#111816', 'center');

  const readyCount = Number(Boolean(duel.readyUsers?.challenger)) + Number(Boolean(duel.readyUsers?.opponent));
  drawReadyText(ctx, `${readyCount}/2 READY`, width / 2, 835, 30, 900, readyCount === 2 ? '#7ADDC0' : '#f1d66d', 'center');

  const readyKey = `${duel.readyUsers?.challenger ? 'c' : 'w'}${duel.readyUsers?.opponent ? 'o' : 'w'}`;
  const name = `squig-duel-ready-${duel.id}-${readyKey}.png`;
  return {
    imageUrl: `attachment://${name}`,
    files: [new AttachmentBuilder(canvas.toBuffer('image/png'), { name })],
  };
}

function buildReadyFallbackEmbeds(duel, title = 'Ready to Duel?') {
  return [new EmbedBuilder()
    .setTitle(title)
    .setColor(0xd4a43b)
    .setDescription(
      `<@${duel.challengerId}> and <@${duel.opponentId}>, confirm you are ready to start.\n\n` +
      `Challenger ${squigNameForSide(duel, 'challenger')}: UglyPoints **${duel.challengerUglyPoints}**, HP **${duel.challengerCurrentHp} / ${duel.challengerMaxHp}**, Attack **${baseAttack(duel.challengerUglyPoints)}**\n` +
      `Opponent ${squigNameForSide(duel, 'opponent')}: UglyPoints **${duel.opponentUglyPoints}**, HP **${duel.opponentCurrentHp} / ${duel.opponentMaxHp}**, Attack **${baseAttack(duel.opponentUglyPoints)}**\n\n` +
      readyStatusText(duel)
    )
    .setImage(SQUIG_DUEL_MENU_IMAGE)];
}

async function buildReadyCheckMessage(duel, title = 'Ready to Duel?') {
  try {
    const readyImage = await buildReadyDuelAttachment(duel);
    return {
      embeds: [new EmbedBuilder()
        .setTitle(title)
        .setColor(0xd4a43b)
        .setDescription(
          `<@${duel.challengerId}> and <@${duel.opponentId}>, confirm you are ready to start.\n\n` +
          readyStatusText(duel)
        )
        .setImage(readyImage.imageUrl)],
      files: readyImage.files,
    };
  } catch (err) {
    console.warn('[SquigDuels] ready check image render failed:', String(err?.message || err || ''));
    return { embeds: buildReadyFallbackEmbeds(duel, title), files: [] };
  }
}

async function postReadyCheck(guild, duel) {
  const thread = await guild.channels.fetch(duel.threadId).catch(() => null);
  if (!thread?.isTextBased()) return;
  duel.status = 'awaiting_ready';
  duel.readyUsers = {};
  await persistDuel(duel);
  const readyMessage = await buildReadyCheckMessage(duel);
  await thread.send({
    content:
      `<@${duel.challengerId}> <@${duel.opponentId}>\n` +
      `Both Squigs are locked in. If you are ready to duel, hit the Ready button.`,
    embeds: readyMessage.embeds,
    ...(readyMessage.files.length ? { files: readyMessage.files } : {}),
    components: readyRows(duel),
  });
}

async function handleReadyButton(interaction) {
  const match = interaction.customId.match(/^sd:ready:([a-f0-9]{12})$/i);
  if (!match) return false;
  assertReady();
  const duel = getDuel(match[1]);
  if (!duel || duel.status !== 'awaiting_ready') {
    await interaction.reply({ content: 'This duel is no longer waiting for ready checks.', flags: 64 });
    return true;
  }

  const side = interaction.user.id === duel.challengerId
    ? 'challenger'
    : (interaction.user.id === duel.opponentId ? 'opponent' : null);
  if (!side) {
    await interaction.reply({ content: 'Only duel participants can ready up.', flags: 64 });
    return true;
  }

  duel.readyUsers = duel.readyUsers || {};
  if (duel.readyUsers[side]) {
    await interaction.reply({ content: 'You are already marked ready.', flags: 64 });
    return true;
  }

  duel.readyUsers[side] = true;
  await persistDuel(duel);

  const bothReady = Boolean(duel.readyUsers.challenger && duel.readyUsers.opponent);
  if (bothReady) {
    duel.status = 'starting';
    await persistDuel(duel);
  }
  const readyMessage = await buildReadyCheckMessage(duel, bothReady ? 'Duel Starting' : 'Ready to Duel?');
  await interaction.update({
    content:
      `<@${duel.challengerId}> <@${duel.opponentId}>\n` +
      (bothReady
        ? `Both players are ready. Duel starting.`
        : `Both Squigs are locked in. If you are ready to duel, hit the Ready button.`),
    embeds: readyMessage.embeds,
    ...(readyMessage.files.length ? { files: readyMessage.files } : {}),
    components: readyRows(duel, bothReady),
  });

  if (bothReady) {
    await startDuel(interaction.guild, duel);
  }
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
  if (duel.openChallenge && action === 'decline') {
    await interaction.reply({ content: 'Open challenges can only be accepted.', flags: 64 });
    return true;
  }
  if (!duel.openChallenge && interaction.user.id !== duel.opponentId) {
    await interaction.reply({ content: 'Only the challenged player can accept or decline this duel.', flags: 64 });
    return true;
  }
  if (action === 'decline') {
    await interaction.deferUpdate();
    await cancelDuel(interaction.guild, duel, 'Opponent declined the duel.');
    await interaction.editReply({ components: [] }).catch(() => null);
    return true;
  }

  if (duel.openChallenge) {
    if (interaction.user.id === duel.challengerId || interaction.user.id === botUserId()) {
      await interaction.reply({ content: 'Choose another holder as your opponent.', flags: 64 });
      return true;
    }
    const activeDuelId = activeUserToDuel.get(interaction.user.id);
    if (activeDuelId && activeDuelId !== duel.id) {
      await interaction.reply({ content: 'You are already in an active Squig Duel.', flags: 64 });
      return true;
    }
    if (!(await hasHolderRole(interaction.guild, interaction.user.id))) {
      await interaction.reply({ content: 'Only holders can accept this Squig Duel.', flags: 64 });
      return true;
    }
    const eligibility = await checkDuelWagerEligibility(interaction.guild, interaction.user.id, duel.wagerAmount);
    if (!eligibility.ok) {
      await interaction.reply({
        content: eligibility.reason || walletConnectMessage('You need to connect your wallet before accepting this Squig Duel.'),
        flags: 64,
      });
      return true;
    }
    duel.opponentId = interaction.user.id;
    duel.openChallenge = false;
    registerActiveUser(interaction.user.id, duel.id);
    await persistDuel(duel);
    const thread = await interaction.guild.channels.fetch(duel.threadId).catch(() => null);
    await thread?.members?.add(interaction.user.id).catch(() => null);
  }

  if (duel.acceptTimeout) clearTimeout(duel.acceptTimeout);
  duel.acceptTimeout = null;
  await interaction.update({ content: 'Duel accepted. Opponent is choosing a Squig.', components: [] });
  await interaction.followUp({ content: 'Choose your Squig in this thread.', flags: 64 }).catch(() => null);
  await sendSquigSelectionPrompt(interaction.guild, duel, 'opponent');
  return true;
}

function buildStatusEmbed(duel, title, description, options = {}) {
  const thumbnailUrl = Object.prototype.hasOwnProperty.call(options, 'thumbnailUrl')
    ? options.thumbnailUrl
    : squigImageUrl(duel.challengerSquigTokenId);
  const imageUrl = Object.prototype.hasOwnProperty.call(options, 'imageUrl')
    ? options.imageUrl
    : squigImageUrl(duel.opponentSquigTokenId);
  const embed = new EmbedBuilder()
    .setTitle(title)
    .setColor(0x7ADDC0)
    .setDescription(description)
    .addFields(
      {
        name: `Challenger ${squigNameForSide(duel, 'challenger')}`,
        value:
          `<@${duel.challengerId}>\n` +
          `UglyPoints: **${duel.challengerUglyPoints}**\n` +
          `HP: **${Math.max(0, duel.challengerCurrentHp)} / ${duel.challengerMaxHp}**`,
        inline: true,
      },
      {
        name: `Opponent ${squigNameForSide(duel, 'opponent')}`,
        value:
          `<@${duel.opponentId}>\n` +
          `UglyPoints: **${duel.opponentUglyPoints}**\n` +
          `HP: **${Math.max(0, duel.opponentCurrentHp)} / ${duel.opponentMaxHp}**`,
        inline: true,
      }
    );
  if (thumbnailUrl) embed.setThumbnail(thumbnailUrl);
  if (imageUrl) embed.setImage(imageUrl);
  return embed;
}

function drawImpactOverlay(ctx, width, height) {
  ctx.save();
  ctx.translate(width * 0.51, height * 0.56);
  ctx.rotate(-0.08);

  const scale = Math.min(width, height) / 1000;
  ctx.scale(scale, scale);
  ctx.lineJoin = 'round';
  ctx.lineCap = 'round';

  ctx.save();
  ctx.translate(135, -60);
  ctx.strokeStyle = '#17201d';
  ctx.fillStyle = '#ffffff';
  ctx.lineWidth = 34;
  ctx.beginPath();
  ctx.moveTo(-85, -55);
  ctx.lineTo(-35, -178);
  ctx.lineTo(36, -92);
  ctx.lineTo(150, -182);
  ctx.lineTo(134, -43);
  ctx.lineTo(330, -68);
  ctx.lineTo(214, 78);
  ctx.lineTo(390, 148);
  ctx.lineTo(205, 184);
  ctx.lineTo(324, 346);
  ctx.lineTo(124, 293);
  ctx.lineTo(114, 510);
  ctx.lineTo(-38, 350);
  ctx.lineTo(-196, 410);
  ctx.lineTo(-140, 210);
  ctx.lineTo(-285, 122);
  ctx.lineTo(-128, 31);
  ctx.closePath();
  ctx.stroke();
  ctx.fill();
  ctx.restore();

  ctx.strokeStyle = '#17201d';
  ctx.fillStyle = '#a36f42';
  ctx.lineWidth = 34;
  ctx.beginPath();
  ctx.moveTo(-760, 225);
  ctx.lineTo(-500, 150);
  ctx.lineTo(-405, 290);
  ctx.lineTo(-730, 382);
  ctx.closePath();
  ctx.stroke();
  ctx.fill();

  ctx.fillStyle = '#d8340b';
  ctx.beginPath();
  ctx.moveTo(-470, 95);
  ctx.bezierCurveTo(-365, 60, -295, -12, -165, -30);
  ctx.bezierCurveTo(7, -55, 127, 15, 162, 170);
  ctx.bezierCurveTo(204, 354, 95, 476, -112, 504);
  ctx.bezierCurveTo(-240, 522, -302, 487, -400, 522);
  ctx.bezierCurveTo(-480, 552, -555, 435, -613, 327);
  ctx.bezierCurveTo(-675, 210, -603, 140, -470, 95);
  ctx.closePath();
  ctx.stroke();
  ctx.fill();

  ctx.beginPath();
  ctx.moveTo(-210, 45);
  ctx.bezierCurveTo(-115, -18, -42, -50, 80, -33);
  ctx.stroke();

  ctx.beginPath();
  ctx.moveTo(88, 110);
  ctx.bezierCurveTo(116, 229, 82, 329, 5, 395);
  ctx.stroke();

  ctx.restore();
}

async function loadPunchOverlay() {
  if (globalThis.__SQUIG_DUEL_PUNCH_OVERLAY) return globalThis.__SQUIG_DUEL_PUNCH_OVERLAY;
  const buffer = await fs.promises.readFile(SQUIG_DUEL_PUNCH_OVERLAY_PATH);
  const overlay = await loadImage(buffer);
  globalThis.__SQUIG_DUEL_PUNCH_OVERLAY = overlay;
  return overlay;
}

function coverRect(sourceWidth, sourceHeight, targetWidth, targetHeight) {
  const scale = Math.max(targetWidth / sourceWidth, targetHeight / sourceHeight);
  const width = Math.round(sourceWidth * scale);
  const height = Math.round(sourceHeight * scale);
  return {
    x: Math.round((targetWidth - width) / 2),
    y: Math.round((targetHeight - height) / 2),
    width,
    height,
  };
}

async function drawPunchOverlay(ctx, width, height) {
  const overlay = await loadPunchOverlay();
  const overlayCanvas = createCanvas(width, height);
  const overlayCtx = overlayCanvas.getContext('2d');
  overlayCtx.imageSmoothingEnabled = true;
  overlayCtx.imageSmoothingQuality = 'high';
  const placement = coverRect(overlay.width, overlay.height, width, height);
  overlayCtx.drawImage(overlay, placement.x, placement.y, placement.width, placement.height);

  const imageData = overlayCtx.getImageData(0, 0, width, height);
  const data = imageData.data;
  for (let i = 0; i < data.length; i += 4) {
    if (data[i] < 8 && data[i + 1] < 8 && data[i + 2] < 8) {
      data[i + 3] = 0;
    }
  }
  overlayCtx.putImageData(imageData, 0, 0);
  ctx.drawImage(overlayCanvas, 0, 0);
}

async function buildLoserSquigAttachment(duel, winnerId) {
  const side = loserSide(duel, winnerId);
  const loserTokenId = tokenIdForSide(duel, side);
  const imageUrl = squigImageUrl(loserTokenId);
  if (!imageUrl) return { imageUrl: null, files: [] };

  try {
    const squig = await loadImage(await fetchImageBuffer(imageUrl));
    const maxSide = 1200;
    const scale = Math.min(1, maxSide / Math.max(squig.width, squig.height));
    const width = Math.max(1, Math.round(squig.width * scale));
    const height = Math.max(1, Math.round(squig.height * scale));
    const canvas = createCanvas(width, height);
    const ctx = canvas.getContext('2d');
    ctx.imageSmoothingEnabled = true;
    ctx.imageSmoothingQuality = 'high';
    ctx.drawImage(squig, 0, 0, width, height);
    try {
      await drawPunchOverlay(ctx, width, height);
    } catch (err) {
      console.warn('[SquigDuels] punch overlay asset failed:', String(err?.message || err || ''));
      drawImpactOverlay(ctx, width, height);
    }

    return {
      imageUrl: `attachment://${SQUIG_DUEL_LOSER_IMAGE_NAME}`,
      files: [
        new AttachmentBuilder(canvas.toBuffer('image/png'), {
          name: SQUIG_DUEL_LOSER_IMAGE_NAME,
          description: `Losing ${squigNameForSide(duel, side)}`,
        }),
      ],
    };
  } catch (err) {
    console.warn('[SquigDuels] loser image render failed:', String(err?.message || err || ''));
    return { imageUrl, files: [] };
  }
}

function actionRows(duel) {
  const actions = [
    ['attack', 'Attack', ButtonStyle.Danger],
    ['defend', 'Defend', ButtonStyle.Primary],
    ['heal', 'Heal', ButtonStyle.Success],
    ['panic', 'Panic', ButtonStyle.Secondary],
  ];
  return [
    new ActionRowBuilder().addComponents(
      actions.map(([key, label, style]) =>
        new ButtonBuilder()
          .setCustomId(`sd:act:${duel.id}:${duel.currentRound}:${key}`)
          .setLabel(label)
          .setStyle(style)
      )
    ),
  ];
}

function roundImageUrl(duel) {
  const challengerImage = squigImageUrl(duel.challengerSquigTokenId);
  const opponentImage = squigImageUrl(duel.opponentSquigTokenId);
  return duel.currentRound % 2 === 1
    ? (challengerImage || opponentImage)
    : (opponentImage || challengerImage);
}

async function startDuel(guild, duel) {
  duel.status = 'active';
  duel.currentRound = 0;
  duel.currentActions = {};
  await persistDuel(duel);
  const thread = await guild.channels.fetch(duel.threadId).catch(() => null);
  if (!thread?.isTextBased()) return;
  const startDescription = isBotDuel(duel)
    ? `Bot duel.\nWager: **${formatCharm(botWagerAmount(duel))} $CHARM**\nPlayer win pays **${formatCharm(botDuelPayoutAmount(duel))} $CHARM**. Bot win keeps the wager.`
    : `Pot: **${formatCharm(duel.wagerAmount * 2)} $CHARM**\nActions are selected privately and revealed after both players choose.`;
  await thread.send({
    embeds: [buildStatusEmbed(
      duel,
      'Squig Duel Started',
      startDescription,
      { imageUrl: SQUIG_DUEL_MENU_IMAGE }
    )],
  });
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
    ? `Choose your action within ${Math.round(ROUND_TIMEOUT_MS / 1000)} seconds.\nThe bot will answer after you lock in.\n`
    : `Both players have ${Math.round(ROUND_TIMEOUT_MS / 1000)} seconds to choose.\nSelections reveal after both players choose.\n`;
  await thread.send({
    embeds: [buildStatusEmbed(
      duel,
      `Round ${duel.currentRound}${sudden ? ' - Sudden Death' : ''}`,
      roundPrompt +
      (sudden ? `Sudden Death: both Squigs lose ${SUDDEN_DEATH_DAMAGE} HP at the end of the round.` : ''),
      { thumbnailUrl: SQUIG_DUEL_MENU_IMAGE, imageUrl: roundImageUrl(duel) }
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
  const match = interaction.customId.match(/^sd:act:([a-f0-9]{12}):(\d+):(attack|defend|heal|panic)$/i);
  if (!match) return false;
  assertReady();
  const duel = getDuel(match[1]);
  const round = Number(match[2]);
  const action = match[3].toLowerCase();
  if (!duel || duel.status !== 'active') {
    await interaction.reply({ content: 'This duel round is no longer active.', flags: 64 });
    return true;
  }
  if (round !== duel.currentRound) {
    await interaction.reply({ content: 'This action prompt is for an old round.', flags: 64 });
    return true;
  }
  const side = interaction.user.id === duel.challengerId
    ? 'challenger'
    : (interaction.user.id === duel.opponentId ? 'opponent' : null);
  if (!side || (side === 'opponent' && isBotDuel(duel))) {
    await interaction.reply({ content: 'Only duel participants can choose an action.', flags: 64 });
    return true;
  }
  if (duel.currentActions[side]) {
    await interaction.reply({ content: 'Action already received.', flags: 64 });
    return true;
  }
  duel.currentActions[side] = action;
  if (isBotDuel(duel) && side === 'challenger') {
    duel.currentActions.opponent = randomBotAction();
  }
  await interaction.reply({ content: 'Action received. It will be revealed after both players choose.', flags: 64 });
  if (duel.currentActions.challenger && duel.currentActions.opponent) {
    if (duel.roundTimeout) clearTimeout(duel.roundTimeout);
    await delay(ROUND_RESOLVE_DELAY_MS);
    const guild = interaction.guild || await deps?.client?.guilds?.fetch?.(duel.guildId).catch(() => null);
    await resolveRound(guild, duel, false);
  }
  return true;
}

function attackDamage(attackPower) {
  const multiplier = 0.8 + (Math.random() * 0.4);
  return Math.max(1, Math.round((Number(attackPower) || 1) * multiplier));
}

function healAmount(maxHp, opposingAction) {
  const baseHealAmount = Math.round(14 + (Number(maxHp) || 0) * 0.14);
  if (opposingAction === 'defend') {
    return Math.max(1, Math.round(baseHealAmount * HEAL_VS_DEFEND_MULTIPLIER));
  }
  if (opposingAction === 'attack') {
    return Math.max(1, Math.round(baseHealAmount * HEAL_VS_ATTACK_MULTIPLIER));
  }
  return Math.max(1, baseHealAmount);
}

function healContext(opposingAction) {
  if (opposingAction === 'defend') return ' through the defense';
  if (opposingAction === 'attack') return ' under pressure';
  return '';
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
  const challenger = playerLabel(duel, 'challenger');
  const opponent = playerLabel(duel, 'opponent');

  if (actions.challenger === 'miss') {
    const penalty = missedTurnPenalty(duel.challengerMaxHp);
    cHp -= penalty;
    lines.push(`${challenger} missed the action window and lost ${penalty} HP.`);
  }
  if (actions.opponent === 'miss') {
    const penalty = missedTurnPenalty(duel.opponentMaxHp);
    oHp -= penalty;
    lines.push(`${opponent} missed the action window and lost ${penalty} HP.`);
  }

  const cPanic = actions.challenger === 'panic';
  const oPanic = actions.opponent === 'panic';
  if (cPanic) {
    const selfLoss = Math.round(duel.challengerMaxHp * 0.12);
    const enemyLoss = Math.round(duel.challengerMaxHp * 0.08);
    cHp -= selfLoss;
    oHp -= enemyLoss;
    lines.push(`${challenger} panicked: ${challenger} loses ${selfLoss} HP and ${opponent} loses ${enemyLoss} HP.`);
  }
  if (oPanic) {
    const selfLoss = Math.round(duel.opponentMaxHp * 0.12);
    const enemyLoss = Math.round(duel.opponentMaxHp * 0.08);
    oHp -= selfLoss;
    cHp -= enemyLoss;
    lines.push(`${opponent} panicked: ${opponent} loses ${selfLoss} HP and ${challenger} loses ${enemyLoss} HP.`);
  }

  const cBlockedByPanic = oPanic;
  const oBlockedByPanic = cPanic;
  const cDefends = actions.challenger === 'defend' && !cBlockedByPanic;
  const oDefends = actions.opponent === 'defend' && !oBlockedByPanic;

  if (actions.challenger === 'attack') {
    if (cBlockedByPanic) {
      lines.push(`${challenger}'s attack missed in the panic.`);
    } else {
      let dmg = attackDamage(baseAttack(duel.challengerUglyPoints));
      if (oDefends) {
        dmg = Math.max(1, Math.round(dmg * 0.25));
        cHp -= dmg;
        lines.push(`${challenger} loses ${dmg} HP when blocked by ${opponent}.`);
      } else {
        oHp -= dmg;
        lines.push(`${challenger} hits ${opponent} for ${dmg} HP.`);
      }
    }
  }

  if (actions.opponent === 'attack') {
    if (oBlockedByPanic) {
      lines.push(`${opponent}'s attack missed in the panic.`);
    } else {
      let dmg = attackDamage(baseAttack(duel.opponentUglyPoints));
      if (cDefends) {
        dmg = Math.max(1, Math.round(dmg * 0.25));
        oHp -= dmg;
        lines.push(`${opponent} loses ${dmg} HP when blocked by ${challenger}.`);
      } else {
        cHp -= dmg;
        lines.push(`${opponent} hits ${challenger} for ${dmg} HP.`);
      }
    }
  }

  if (actions.challenger === 'heal') {
    if (cBlockedByPanic) {
      lines.push(`${challenger}'s heal failed.`);
    } else {
      const heal = healAmount(duel.challengerMaxHp, actions.opponent);
      const beforeHeal = cHp;
      cHp = Math.min(duel.challengerMaxHp, cHp + heal);
      lines.push(`${challenger} heals ${Math.max(0, cHp - beforeHeal)} HP${healContext(actions.opponent)}.`);
    }
  }
  if (actions.opponent === 'heal') {
    if (oBlockedByPanic) {
      lines.push(`${opponent}'s heal failed.`);
    } else {
      const heal = healAmount(duel.opponentMaxHp, actions.challenger);
      const beforeHeal = oHp;
      oHp = Math.min(duel.opponentMaxHp, oHp + heal);
      lines.push(`${opponent} heals ${Math.max(0, oHp - beforeHeal)} HP${healContext(actions.challenger)}.`);
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

function duelCompletionReason(duel, winnerId, result = null) {
  const cOut = duel.challengerCurrentHp <= 0;
  const oOut = duel.opponentCurrentHp <= 0;
  if (cOut && oOut) {
    return `Both Squigs hit 0 HP; <@${winnerId}> won by tiebreaker.`;
  }
  if (cOut) {
    return `Challenger ${squigNameForSide(duel, 'challenger')} hit 0 HP.`;
  }
  if (oOut) {
    return `Opponent ${squigNameForSide(duel, 'opponent')} hit 0 HP.`;
  }
  if (result?.lines?.length) {
    return result.lines[result.lines.length - 1];
  }
  return 'Duel completed.';
}

async function resolveRound(guild, duel, timedOut) {
  if (!duel || duel.status !== 'active' || duel.processingRound) return;
  if (!guild) {
    console.warn('[SquigDuels] cannot resolve round without guild context.');
    return;
  }
  duel.processingRound = true;
  if (duel.roundTimeout) clearTimeout(duel.roundTimeout);
  const result = resolveRoundMath(duel, timedOut);
  await persistRound(duel, result);

  const thread = await guild.channels.fetch(duel.threadId).catch(() => null);
  if (thread?.isTextBased()) {
    const actionText =
      `${playerLabel(duel, 'challenger')}: **${actionLabel(result.actions.challenger)}**\n` +
      `${playerLabel(duel, 'opponent')}: **${actionLabel(result.actions.opponent)}**`;
    const resultLines = result.lines.length ? result.lines.join('\n') : 'No effects resolved.';
    await thread.send(
      `Selected Actions:\n${actionText}\n\n` +
      `Results:\n${resultLines}\n` +
      `Stats: Challenger ${squigNameForSide(duel, 'challenger')} HP: ${Math.max(0, duel.challengerCurrentHp)}/${duel.challengerMaxHp}, ` +
      `Opponent ${squigNameForSide(duel, 'opponent')} HP: ${Math.max(0, duel.opponentCurrentHp)}/${duel.opponentMaxHp}`
    );
  }

  const winnerId = determineWinner(duel, result);
  if (winnerId) {
    await completeDuel(guild, duel, winnerId, result);
    return;
  }
  duel.processingRound = false;
  if (thread?.isTextBased()) {
    await thread.send(`Next round starts in ${Math.round(NEXT_ROUND_DELAY_MS / 1000)} seconds.`);
  }
  await delay(NEXT_ROUND_DELAY_MS);
  await beginRound(guild, duel);
}

function duelTaxPercent() {
  const raw = String(process.env.DUEL_TAX_PERCENT || '').trim();
  if (!raw) return 0;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.min(100, n);
}

async function completeDuel(guild, duel, winnerId, result = null) {
  duel.status = 'completed';
  duel.winnerId = winnerId;
  duel.completedAt = Date.now();
  releaseDuelUsers(duel);
  const reason = duelCompletionReason(duel, winnerId, result);
  const loserImage = await buildLoserSquigAttachment(duel, winnerId);
  if (isBotDuel(duel)) {
    const wagerAmount = botWagerAmount(duel);
    const payoutAmount = botDuelPayoutAmount(duel);
    const playerWon = String(winnerId) === String(duel.challengerId);
    const thread = await guild.channels.fetch(duel.threadId).catch(() => null);
    let payoutOk = true;
    let payoutError = '';
    if (playerWon) {
      try {
        await transferFromBot(guild, duel.challengerId, payoutAmount, 'squig_duel_bot_payout', botUserId());
      } catch (err) {
        payoutOk = false;
        payoutError = String(err?.message || err || '').slice(0, 500);
      }
    }
    await persistDuel(duel);
    if (thread?.isTextBased()) {
      await thread.send({
        embeds: [buildStatusEmbed(
          duel,
          'Bot Squig Duel Complete',
          `Winner: <@${winnerId}>\n` +
          `Wager: **${formatCharm(wagerAmount)} $CHARM**\n` +
          (playerWon
            ? `Payout: **${formatCharm(payoutAmount)} $CHARM**\n` +
              (payoutOk ? 'Payout completed.' : `Payout failed and needs admin review: ${payoutError}`)
            : `The bot won. <@${duel.challengerId}> lost the wager.`),
          { imageUrl: loserImage.imageUrl }
        )],
        ...(loserImage.files.length ? { files: loserImage.files } : {}),
      }).catch(() => null);
    }
    await postPublicDuelFinalLog(
      guild,
      `Squig Duel final log\n` +
      `Reason: ${reason}\n` +
      `Winner: <@${winnerId}>\n` +
      (playerWon
        ? `Payout: ${payoutOk ? `${formatCharm(payoutAmount)} $CHARM to <@${duel.challengerId}>` : `Failed: ${payoutError}`}`
        : `Bot win: <@${duel.challengerId}> lost ${formatCharm(wagerAmount)} $CHARM`)
    );
    await logDuel(
      guild,
      playerWon ? 'Bot Payout' : 'Bot Win',
      playerWon
        ? `Bot duel \`${duel.id}\` winner <@${winnerId}> payout ${payoutOk ? `${formatCharm(payoutAmount)} $CHARM to <@${duel.challengerId}>` : `failed: ${payoutError}`}.`
        : `Bot duel \`${duel.id}\` bot won; <@${duel.challengerId}> lost ${formatCharm(wagerAmount)} $CHARM.`
    );
    scheduleDuelThreadDeletion(guild, duel);
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
        (payoutOk ? 'Payout completed.' : `Payout failed and needs admin review: ${payoutError}`),
        { imageUrl: loserImage.imageUrl }
      )],
      ...(loserImage.files.length ? { files: loserImage.files } : {}),
    }).catch(() => null);
  }
  await postPublicDuelFinalLog(
    guild,
    `Squig Duel final log\n` +
    `Reason: ${reason}\n` +
    `Winner: <@${winnerId}>\n` +
    `Payout: ${payoutOk ? `${formatCharm(payout)} $CHARM` : `Failed: ${payoutError}`}` +
    (taxAmount ? `\nTax: ${formatCharm(taxAmount)} $CHARM (${taxPercent}%)` : '')
  );
  await logDuel(guild, 'Payout', `Duel \`${duel.id}\` winner <@${winnerId}> payout ${payoutOk ? `${formatCharm(payout)} $CHARM` : `failed: ${payoutError}`}.`);
  scheduleDuelThreadDeletion(guild, duel);
}

async function handleButton(interaction) {
  if (!String(interaction.customId || '').startsWith('sd:')) return false;
  if (await handleViewSquigsButton(interaction)) return true;
  if (await handleViewSquigPageButton(interaction)) return true;
  if (await handleSetMySquigButton(interaction)) return true;
  if (await handleMySquigPageButton(interaction)) return true;
  if (await handleMySquigNameButton(interaction)) return true;
  if (await handleMySquigFavoriteButton(interaction)) return true;
  if (await handleMySquigSaveButton(interaction)) return true;
  if (await handleBotDuelButton(interaction)) return true;
  if (await handleStartButton(interaction)) return true;
  if (await handleOpenChallengeButton(interaction)) return true;
  if (await handleAcceptDecline(interaction)) return true;
  if (await handleReadyButton(interaction)) return true;
  if (await handleSquigSelectionPageButton(interaction)) return true;
  if (await handleActionButton(interaction)) return true;
  return false;
}

async function handleSelectMenu(interaction) {
  const customId = String(interaction.customId || '');
  if (customId.startsWith('sd:opponent:')) return handleOpponentSelect(interaction);
  if (customId.startsWith('sd:my_squig_select:')) return handleMySquigSelect(interaction);
  if (customId.startsWith('sd:view_select:')) return handleViewSquigSelect(interaction);
  if (customId.startsWith('sd:select:')) return handleSquigSelect(interaction);
  return false;
}

async function handleModalSubmit(interaction) {
  const customId = String(interaction.customId || '');
  if (customId.startsWith('sd:my_squig_name_modal:')) return handleMySquigNameModal(interaction);
  if (customId.startsWith('sd:bot_setup:')) return handleBotDuelSetupModal(interaction);
  if (customId.startsWith('sd:setup:')) return handleSetupModal(interaction);
  return false;
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
