// mini-roulette.js
const {
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  EmbedBuilder,
  ComponentType,
} = require('discord.js');

// Players pick 1–6. Bot rolls a die. Matches get +2 points.
async function runMiniGameRoulettePoints(opts) {
  const {
    channel,
    players,           // Map<userId, { id, username?, displayName? }>
    scores,            // Map<userId, number>
    roundNumber = 1,
    usedMiniGames,     // Set<string>
    testMode = false,
    mockPlayerIds = [],
    timeoutMs = 30_000,
  } = opts;

  const MINI_ID = 'roulette_1to6_v1';
  if (usedMiniGames && usedMiniGames.has(MINI_ID)) {
    return { id: MINI_ID, name: 'Squig Roulette', rolled: 0, picks: new Map(), winners: [], pointsAwarded: 2 };
  }

  const title = `Round ${roundNumber}: 🎲 Squig Roulette`;
  const rules = [
    'Pick a number **1–6** below.',
    'I’ll roll a die at the end.',
    '**Match = +2 points.** No match = 0.',
  ].join('\n');

  const embed = new EmbedBuilder()
    .setTitle(title)
    .setDescription(rules)
    .setFooter({ text: 'You have 30 seconds. Alerts at 10s and 20s.' });

  const row1 = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('roulette_1').setLabel('1').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('roulette_2').setLabel('2').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('roulette_3').setLabel('3').setStyle(ButtonStyle.Secondary)
  );
  const row2 = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('roulette_4').setLabel('4').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('roulette_5').setLabel('5').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('roulette_6').setLabel('6').setStyle(ButtonStyle.Secondary)
  );

  const msg = await channel.send({ embeds: [embed], components: [row1, row2] });

  const picks = new Map();
  const eligible = new Set(testMode ? mockPlayerIds : Array.from(players.keys()));
  const filter = (i) => i.message.id === msg.id && eligible.has(i.user.id);

  const collector = msg.createMessageComponentCollector({
    componentType: ComponentType.Button,
    time: timeoutMs,
    filter,
  });

  collector.on('collect', async (i) => {
    try {
      const choice = Number(i.customId.split('_')[1]);
      if (choice >= 1 && choice <= 6) {
        picks.set(i.user.id, choice);
        await i.reply({ content: `You picked **${choice}** 🎯`, ephemeral: true });
      } else {
        await i.reply({ content: `Invalid choice.`, ephemeral: true });
      }
    } catch {}
  });

  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  (async () => { await sleep(10_000); if (!collector.ended) await msg.edit({ embeds: [EmbedBuilder.from(embed).setFooter({ text: '20 seconds left…' })] }); })();
  (async () => { await sleep(20_000); if (!collector.ended) await msg.edit({ embeds: [EmbedBuilder.from(embed).setFooter({ text: '10 seconds left…' })] }); })();

  await sleep(timeoutMs);
  collector.stop('time');

  try {
    const disable = (row) => new ActionRowBuilder().addComponents(row.components.map(b => ButtonBuilder.from(b).setDisabled(true)));
    await msg.edit({ components: [disable(row1), disable(row2)] });
  } catch {}

  if (picks.size === 0) {
    await channel.send({ embeds: [new EmbedBuilder().setTitle(title).setDescription(`${rules}\n\nNo picks were made. Moving on…`)] });
    if (usedMiniGames) usedMiniGames.add(MINI_ID);
    return { id: MINI_ID, name: 'Squig Roulette', rolled: 0, picks, winners: [], pointsAwarded: 2 };
  }

  const rolled = 1 + Math.floor(Math.random() * 6);
  const winners = [];
  for (const [uid, num] of picks.entries()) {
    if (num === rolled) {
      winners.push(uid);
      scores.set(uid, (scores.get(uid) ?? 0) + 2);
    }
  }

  const nameOf = (uid) => (players.get(uid)?.displayName || players.get(uid)?.username || `<@${uid}>`);
  const byNum = new Map();
  for (const [uid, num] of picks) {
    const arr = byNum.get(num) || [];
    arr.push(nameOf(uid));
    byNum.set(num, arr);
  }

  const lines = [];
  for (let n = 1; n <= 6; n++) {
    const list = byNum.get(n) || [];
    lines.push(`**${n}** — ${list.length ? list.join(', ') : '—'}`);
  }

  await channel.send({
    embeds: [
      new EmbedBuilder()
        .setTitle('🎲 Squig Roulette — Results')
        .setDescription([
          `Rolled: **${rolled}**`,
          '',
          '**Picks:**',
          lines.join('\n'),
          '',
          winners.length ? `Winners (+2): ${winners.map(nameOf).join(', ')}` : 'No matches this time!',
        ].join('\n')),
    ],
  });

  if (usedMiniGames) usedMiniGames.add(MINI_ID);
  return { id: MINI_ID, name: 'Squig Roulette', rolled, picks, winners, pointsAwarded: 2 };
}

module.exports = { runMiniGameRoulettePoints };
