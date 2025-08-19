// Load local .env when running outside Railway (Railway injects envs)
try { require('dotenv').config(); } catch (_) {}

const {
  Client,
  GatewayIntentBits,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  AttachmentBuilder,
  SlashCommandBuilder,
  REST,
  Routes
} = require('discord.js');
const fetch = require('node-fetch');
const fs = require('fs');
const { createCanvas, loadImage, GlobalFonts } = require('@napi-rs/canvas');
const { ethers } = require('ethers');

// ===== ENV =====
const DISCORD_TOKEN      = process.env.DISCORD_BOT_TOKEN || process.env.DISCORD_TOKEN;
const DISCORD_CLIENT_ID  = process.env.DISCORD_CLIENT_ID;
const GUILD_ID           = process.env.GUILD_ID;
const ETHERSCAN_API_KEY  = process.env.ETHERSCAN_API_KEY;
const ALCHEMY_API_KEY    = process.env.ALCHEMY_API_KEY;
const OPENSEA_API_KEY    = process.env.OPENSEA_API_KEY || ''; // optional

// ===== FONT REGISTRATION (auto-download if missing) =====
// Using TypeTogether repo for TTFs to avoid GF path issues.
let FONT_REGULAR_FAMILY = 'PlaypenSans-Regular';
let FONT_BOLD_FAMILY    = 'PlaypenSans-Bold';

async function ensureFonts() {
  const FONT_DIR = 'fonts';
  fs.mkdirSync(FONT_DIR, { recursive: true });

  const files = [
    {
      url: 'https://raw.githubusercontent.com/TypeTogether/Playpen-Sans/main/fonts/ttf/PlaypenSans-Regular.ttf',
      path: `${FONT_DIR}/PlaypenSans-Regular.ttf`,
      family: 'PlaypenSans-Regular'
    },
    {
      url: 'https://raw.githubusercontent.com/TypeTogether/Playpen-Sans/main/fonts/ttf/PlaypenSans-Bold.ttf',
      path: `${FONT_DIR}/PlaypenSans-Bold.ttf`,
      family: 'PlaypenSans-Bold'
    }
  ];

  for (const f of files) {
    if (!fs.existsSync(f.path)) {
      const r = await fetch(f.url);
      if (!r.ok) throw new Error(`Font download failed: ${r.status} (${f.url})`);
      fs.writeFileSync(f.path, Buffer.from(await r.arrayBuffer()));
    }
    try { GlobalFonts.registerFromPath(f.path, f.family); } catch (e) {
      console.warn('Font register error:', e.message);
    }
  }
  console.log('🖋 Fonts ready:', files.map(f => f.family).join(', '));
}
ensureFonts().catch(e => {
  console.warn('⚠️ Could not ensure fonts:', e.message);
  FONT_REGULAR_FAMILY = 'sans-serif';
  FONT_BOLD_FAMILY    = 'sans-serif';
});

// Debug env (safe booleans/ids only)
console.log('ENV CHECK:', {
  hasToken: !!DISCORD_TOKEN,
  clientId: DISCORD_CLIENT_ID,
  guildId: GUILD_ID,
  hasAlchemy: !!ALCHEMY_API_KEY,
  hasOpenSea: !!OPENSEA_API_KEY
});

// ===== CONTRACTS =====
const UGLY_CONTRACT   = '0x9492505633d74451bdf3079c09ccc979588bc309';
const MONSTER_CONTRACT= '0x1cD7fe72D64f6159775643ACEdc7D860dFB80348';
const SQUIGS_CONTRACT = '0x9bf567ddf41b425264626d1b8b2c7f7c660b1c42';

// ===== CHARM DROPS =====
const CHARM_REWARD_CHANCE = 100; // 1 in 200
const CHARM_REWARDS = [150, 200, 350, 200]; // Weighted pool

// ===== CLIENT =====
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent
  ]
});

// ===== UTILS =====
const fetchWithRetry = async (url, retries = 3, delay = 1000, opts = {}) => {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const res = await fetch(url, { timeout: 10000, ...opts });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res;
    } catch (err) {
      if (attempt === retries - 1) throw err;
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
};

let walletLinks = {};
if (fs.existsSync('walletLinks.json')) {
  walletLinks = JSON.parse(fs.readFileSync('walletLinks.json'));
}

// ===== Slash command registrar (guild-scoped for fast iteration) =====
async function registerSlashCommands() {
  try {
    if (!DISCORD_CLIENT_ID || !GUILD_ID) {
      console.warn('⚠️ DISCORD_CLIENT_ID or GUILD_ID missing; cannot register /card.');
      return;
    }
    const rest = new REST({ version: '10' }).setToken(DISCORD_TOKEN);

    const commands = [
      new SlashCommandBuilder()
        .setName('card')
        .setDescription(`Create a Squigs trading card JPEG (${Date.now()})`)
        .addIntegerOption(o => o.setName('token_id').setDescription('Squig token ID').setRequired(true))
        .addStringOption(o => o.setName('name').setDescription('Optional display name').setRequired(false))
        .toJSON()
    ];

    const data = await rest.put(
      Routes.applicationGuildCommands(DISCORD_CLIENT_ID, GUILD_ID),
      { body: commands }
    );
    console.log(`✅ Registered ${data.length} guild slash command(s) to ${GUILD_ID}.`);

    // list commands for sanity
    const guildCmds = await rest.get(Routes.applicationGuildCommands(DISCORD_CLIENT_ID, GUILD_ID));
    console.log('🔎 Guild commands now:', guildCmds.map(c => `${c.name} (${c.id})`).join(', '));
  } catch (e) {
    console.error('❌ Slash register error:', e?.data ?? e);
  }
}

// ===== READY =====
client.on('ready', async () => {
  console.log(`✅ Logged in as ${client.user.tag}`);
  try { await registerSlashCommands(); } catch (e) { console.error('Slash register error:', e.message); }
});

// ===== RANDOM CHARM REWARD =====
function maybeRewardCharm(userId, username) {
  const roll = Math.floor(Math.random() * 200);
  if (roll === 0) {
    const rewards = [100, 100, 100, 200]; // weighted pool
    const reward = rewards[Math.floor(Math.random() * rewards.length)];
    const loreMessages = [
      "A Squig blinked and $CHARM fell out of the sky.",
      "The spirals aligned. You’ve been dripped on.",
      "You weren’t supposed to find this... but the Squigs don’t care.",
      "A whisper reached your wallet: ‘take it, fast.’",
      "This reward was meant for someone else. The Squigs disagreed.",
      "The Charmkeeper slipped. You caught it.",
      "A Squig coughed up 200 $CHARM. Please wash your hands.",
      "This token came from *somewhere very wet*. Don’t ask."
    ];
    const lore = loreMessages[Math.floor(Math.random() * loreMessages.length)];
    return { reward, lore };
  }
  return null;
}

// ===== PREFIX COMMANDS (existing ones) =====
client.on('messageCreate', async (message) => {
  if (!message.content.startsWith('!') || message.author.bot) return;

  const args = message.content.slice(1).trim().split(/ +/);
  const command = args.shift().toLowerCase();

  // !linkwallet
  if (command === 'linkwallet') {
    const address = args[0];
    if (!/^0x[a-fA-F0-9]{40}$/.test(address)) {
      return message.reply('❌ Please enter a valid Ethereum wallet address.');
    }
    walletLinks[message.author.id] = address;
    fs.writeFileSync('walletLinks.json', JSON.stringify(walletLinks, null, 2));
    try { await message.delete(); } catch (err) {
      console.warn(`⚠️ Could not delete message from ${message.author.tag}:`, err.message);
    }
    return message.channel.send({ content: '✅ Wallet linked.', allowedMentions: { repliedUser: false } });
  }

  // !ugly
  if (command === 'ugly') {
    const wallet = walletLinks[message.author.id];
    if (!wallet) return message.reply('❌ Please link your wallet first using `!linkwallet 0x...`');

    const url = `https://api.etherscan.io/api?module=account&action=tokennfttx&address=${wallet}&contractaddress=${UGLY_CONTRACT}&page=1&offset=100&sort=asc&apikey=${ETHERSCAN_API_KEY}`;
    try {
      const res = await fetchWithRetry(url);
      const data = await res.json();

      const owned = new Set();
      for (const tx of data.result) {
        if (tx.to.toLowerCase() === wallet.toLowerCase()) owned.add(tx.tokenID);
        else if (tx.from.toLowerCase() === wallet.toLowerCase()) owned.delete(tx.tokenID);
      }

      if (owned.size === 0) return message.reply('😢 You don’t own any Charm of the Ugly NFTs.');

      const tokenArray = Array.from(owned);
      const randomToken = tokenArray[Math.floor(Math.random() * tokenArray.length)];
      const imgUrl = `https://ipfs.io/ipfs/bafybeie5o7afc4yxyv3xx4jhfjzqugjwl25wuauwn3554jrp26mlcmprhe/${randomToken}`;

      const embed = new EmbedBuilder()
        .setTitle(`🧟 Charm of the Ugly`)
        .setDescription(`Token ID: **${randomToken}**`)
        .setImage(imgUrl)
        .setColor(0x8c52ff)
        .setFooter({ text: `Ugly Bot summoned this one from your wallet.` });

      return message.reply({ embeds: [embed] });
    } catch (err) {
      console.error('❌ Fetch failed (ugly):', err.message);
      return message.reply('⚠️ Error fetching your Uglies. Please try again later.');
    }
  }

  // !monster
  if (command === 'monster') {
    const wallet = walletLinks[message.author.id];
    if (!wallet) return message.reply('❌ Please link your wallet first using `!linkwallet 0x...`');

    const url = `https://api.etherscan.io/api?module=account&action=tokennfttx&address=${wallet}&contractaddress=${MONSTER_CONTRACT}&page=1&offset=100&sort=asc&apikey=${ETHERSCAN_API_KEY}`;
    try {
      const res = await fetchWithRetry(url);
      const data = await res.json();

      const owned = new Set();
      for (const tx of data.result) {
        if (tx.to.toLowerCase() === wallet.toLowerCase()) owned.add(tx.tokenID);
        else if (tx.from.toLowerCase() === wallet.toLowerCase()) owned.delete(tx.tokenID);
      }

      if (owned.size === 0) return message.reply('😢 You don’t own any Ugly Monster NFTs.');

      const tokenArray = Array.from(owned);
      const randomToken = tokenArray[Math.floor(Math.random() * tokenArray.length)];
      const imgUrl = `https://gateway.pinata.cloud/ipfs/bafybeicydaui66527mumvml5ushq5ngloqklh6rh7hv3oki2ieo6q25ns4/${randomToken}.webp`;

      const embed = new EmbedBuilder()
        .setTitle(`👹 Ugly Monster`)
        .setDescription(`Token ID: **${randomToken}**`)
        .setImage(imgUrl)
        .setColor(0xff4444)
        .setFooter({ text: `Spawned from your Ugly Monster collection.` });

      return message.reply({ embeds: [embed] });
    } catch (err) {
      console.error('❌ Fetch failed (monster):', err.message);
      return message.reply('⚠️ Error fetching your Monsters. Please try again later.');
    }
  }

  // !myuglys with pagination
  if (command === 'myuglys') {
    const wallet = walletLinks[message.author.id];
    if (!wallet) return message.reply('❌ Please link your wallet first using `!linkwallet 0x...`');

    const url = `https://api.etherscan.io/api?module=account&action=tokennfttx&address=${wallet}&contractaddress=${UGLY_CONTRACT}&page=1&offset=100&sort=asc&apikey=${ETHERSCAN_API_KEY}`;
    try {
      const res = await fetchWithRetry(url);
      const data = await res.json();

      const owned = new Set();
      for (const tx of data.result) {
        if (tx.to.toLowerCase() === wallet.toLowerCase()) owned.add(tx.tokenID);
        else if (tx.from.toLowerCase() === wallet.toLowerCase()) owned.delete(tx.tokenID);
      }

      const tokenArray = Array.from(owned);
      if (tokenArray.length === 0) return message.reply('😢 You don’t currently own any Charm of the Ugly NFTs.');

      const itemsPerPage = 5;
      let page = 0;
      const totalPages = Math.ceil(tokenArray.length / itemsPerPage);

      const generateEmbeds = (page) => {
        const start = page * itemsPerPage;
        const tokens = tokenArray.slice(start, start + itemsPerPage);
        return tokens.map(tokenId =>
          new EmbedBuilder()
            .setTitle(`🧟 Ugly #${tokenId}`)
            .setImage(`https://ipfs.io/ipfs/bafybeie5o7afc4yxyv3xx4jhfjzqugjwl25wuauwn3554jrp26mlcmprhe/${tokenId}`)
            .setColor(0x8c52ff)
            .setFooter({ text: `Page ${page + 1} of ${totalPages}` })
        );
      };

      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setCustomId('prev').setLabel('◀️').setStyle(ButtonStyle.Secondary),
        new ButtonBuilder().setCustomId('stop').setLabel('⏹️').setStyle(ButtonStyle.Danger),
        new ButtonBuilder().setCustomId('next').setLabel('▶️').setStyle(ButtonStyle.Secondary)
      );

      const messageReply = await message.reply({ embeds: generateEmbeds(page), components: [row] });
      const collector = messageReply.createMessageComponentCollector({ time: 120000 });

      collector.on('collect', async (interaction) => {
        if (interaction.user.id !== message.author.id) {
          return interaction.reply({ content: '❌ Only the original user can use these buttons.', ephemeral: true });
        }
        if (interaction.customId === 'prev') {
          page = page > 0 ? page - 1 : totalPages - 1;
          await interaction.update({ embeds: generateEmbeds(page) });
        }
        if (interaction.customId === 'next') {
          page = page < totalPages - 1 ? page + 1 : 0;
          await interaction.update({ embeds: generateEmbeds(page) });
        }
        if (interaction.customId === 'stop') {
          collector.stop();
          await interaction.update({ components: [] });
        }
      });

      collector.on('end', async () => {
        try { await messageReply.edit({ components: [] }); } catch (e) {}
      });

    } catch (err) {
      console.error('❌ Fetch failed (myuglys):', err.message);
      return message.reply('⚠️ Error fetching your Uglies. Please try again later.');
    }
  }

  // !mymonsters with pagination
  if (command === 'mymonsters') {
    const wallet = walletLinks[message.author.id];
    if (!wallet) return message.reply('❌ Please link your wallet first using `!linkwallet 0x...`');

    const url = `https://api.etherscan.io/api?module=account&action=tokennfttx&address=${wallet}&contractaddress=${MONSTER_CONTRACT}&page=1&offset=100&sort=asc&apikey=${ETHERSCAN_API_KEY}`;
    try {
      const res = await fetchWithRetry(url);
      const data = await res.json();

      const owned = new Set();
      for (const tx of data.result) {
        if (tx.to.toLowerCase() === wallet.toLowerCase()) owned.add(tx.tokenID);
        else if (tx.from.toLowerCase() === wallet.toLowerCase()) owned.delete(tx.tokenID);
      }

      const tokenArray = Array.from(owned);
      if (tokenArray.length === 0) return message.reply('😢 You don’t currently own any Ugly Monster NFTs.');

      const itemsPerPage = 5;
      let page = 0;
      const totalPages = Math.ceil(tokenArray.length / itemsPerPage);

      const generateEmbeds = (page) => {
        const start = page * itemsPerPage;
        const tokens = tokenArray.slice(start, start + itemsPerPage);
        return tokens.map(tokenId =>
          new EmbedBuilder()
            .setTitle(`👹 Monster #${tokenId}`)
            .setImage(`https://gateway.pinata.cloud/ipfs/bafybeicydaui66527mumvml5ushq5ngloqklh6rh7hv3oki2ieo6q25ns4/${tokenId}.webp`)
            .setColor(0xff4444)
            .setFooter({ text: `Page ${page + 1} of ${totalPages}` })
        );
      };

      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setCustomId('prev').setLabel('◀️').setStyle(ButtonStyle.Secondary),
        new ButtonBuilder().setCustomId('stop').setLabel('⏹️').setStyle(ButtonStyle.Danger),
        new ButtonBuilder().setCustomId('next').setLabel('▶️').setStyle(ButtonStyle.Secondary)
      );

      const messageReply = await message.reply({ embeds: generateEmbeds(page), components: [row] });
      const collector = messageReply.createMessageComponentCollector({ time: 120000 });

      collector.on('collect', async (interaction) => {
        if (interaction.user.id !== message.author.id) {
          return interaction.reply({ content: '❌ Only the original user can use these buttons.', ephemeral: true });
        }
        if (interaction.customId === 'prev') {
          page = page > 0 ? page - 1 : totalPages - 1;
          await interaction.update({ embeds: generateEmbeds(page) });
        }
        if (interaction.customId === 'next') {
          page = page < totalPages - 1 ? page + 1 : 0;
          await interaction.update({ embeds: generateEmbeds(page) });
        }
        if (interaction.customId === 'stop') {
          collector.stop();
          await interaction.update({ components: [] });
        }
      });

      collector.on('end', async () => {
        try { await messageReply.edit({ components: [] }); } catch (e) {}
      });

    } catch (err) {
      console.error('❌ Fetch failed (mymonsters):', err.message);
      return message.reply('⚠️ Error fetching your Monsters. Please try again later.');
    }
  }

  // !squig (random from linked wallet)
  if (command === 'squig' && args.length === 0) {
    const wallet = walletLinks[message.author.id];
    if (!wallet) return message.reply('❌ Please link your wallet first using `!linkwallet 0x...`');

    const url = `https://api.etherscan.io/api?module=account&action=tokennfttx&address=${wallet}&contractaddress=${SQUIGS_CONTRACT}&page=1&offset=100&sort=asc&apikey=${ETHERSCAN_API_KEY}`;
    try {
      const res = await fetchWithRetry(url);
      const data = await res.json();

      const owned = new Set();
      for (const tx of data.result) {
        if (tx.to.toLowerCase() === wallet.toLowerCase()) owned.add(tx.tokenID);
        else if (tx.from.toLowerCase() === wallet.toLowerCase()) owned.delete(tx.tokenID);
      }

      if (owned.size === 0) return message.reply('😢 You don’t own any Squigs.');

      const tokenArray = Array.from(owned);
      const randomToken = tokenArray[Math.floor(Math.random() * tokenArray.length)];
      const imgUrl = `https://assets.bueno.art/images/a49527dc-149c-4cbc-9038-d4b0d1dbf0b2/default/${randomToken}`;
      const openseaUrl = `https://opensea.io/assets/ethereum/${SQUIGS_CONTRACT}/${randomToken}`;

      const embed = new EmbedBuilder()
        .setTitle(`👁️ Squig #${randomToken}`)
        .setDescription(`[View on OpenSea](${openseaUrl})`)
        .setImage(imgUrl)
        .setColor(0xffa500)
        .setFooter({ text: `A Squig has revealed itself... briefly.` });

      await message.reply({ embeds: [embed] });

      const charmDrop = maybeRewardCharm(message.author.id, message.author.username);
      if (charmDrop) {
        message.channel.send(`🎁 **${message.author.username}** just got **${charmDrop.reward} $CHARM**!\n*${charmDrop.lore}*\n👉 <@826581856400179210> to get your $CHARM`);
        console.log(`CHARM REWARD: ${message.author.username} (${message.author.id}) got ${charmDrop.reward} $CHARM.`);
      }
    } catch (err) {
      console.error('❌ Fetch failed (squig):', err.message);
      return message.reply('⚠️ Error fetching your Squigs. Please try again later.');
    }
  }

  // !squig [tokenId]
  if (command === 'squig' && args.length === 1 && /^\d+$/.test(args[0])) {
    const tokenId = args[0];
    const imgUrl = `https://assets.bueno.art/images/a49527dc-149c-4cbc-9038-d4b0d1dbf0b2/default/${tokenId}`;
    const openseaUrl = `https://opensea.io/assets/ethereum/${SQUIGS_CONTRACT}/${tokenId}`;

    const embed = new EmbedBuilder()
      .setTitle(`👁️ Squig #${tokenId}`)
      .setDescription(`[View on OpenSea](${openseaUrl})`)
      .setImage(imgUrl)
      .setColor(0xffa500)
      .setFooter({ text: `Squig #${tokenId} is watching you...` });

    return message.reply({ embeds: [embed] });
  }

  // !mysquigs with pagination
  if (command === 'mysquigs') {
    const wallet = walletLinks[message.author.id];
    if (!wallet) return message.reply('❌ Please link your wallet first using `!linkwallet 0x...`');

    const url = `https://api.etherscan.io/api?module=account&action=tokennfttx&address=${wallet}&contractaddress=${SQUIGS_CONTRACT}&page=1&offset=100&sort=asc&apikey=${ETHERSCAN_API_KEY}`;
    try {
      const res = await fetchWithRetry(url);
      const data = await res.json();

      const owned = new Set();
      for (const tx of data.result) {
        if (tx.to.toLowerCase() === wallet.toLowerCase()) owned.add(tx.tokenID);
        else if (tx.from.toLowerCase() === wallet.toLowerCase()) owned.delete(tx.tokenID);
      }

      const tokenArray = Array.from(owned);
      if (tokenArray.length === 0) return message.reply('😢 You don’t currently own any Squigs.');

      const itemsPerPage = 5;
      let page = 0;
      const totalPages = Math.ceil(tokenArray.length / itemsPerPage);

      const generateEmbeds = (page) => {
        const start = page * itemsPerPage;
        const tokens = tokenArray.slice(start, start + itemsPerPage);
        return tokens.map(tokenId =>
          new EmbedBuilder()
            .setTitle(`👁️ Squig #${tokenId}`)
            .setImage(`https://assets.bueno.art/images/a49527dc-149c-4cbc-9038-d4b0d1dbf0b2/default/${tokenId}`)
            .setDescription(`[View on OpenSea](https://opensea.io/assets/ethereum/${SQUIGS_CONTRACT}/${tokenId})`)
            .setColor(0xffa500)
            .setFooter({ text: `Page ${page + 1} of ${totalPages}` })
        );
      };

      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setCustomId('prev').setLabel('◀️').setStyle(ButtonStyle.Secondary),
        new ButtonBuilder().setCustomId('stop').setLabel('⏹️').setStyle(ButtonStyle.Danger),
        new ButtonBuilder().setCustomId('next').setLabel('▶️').setStyle(ButtonStyle.Secondary)
      );

      const messageReply = await message.reply({ embeds: generateEmbeds(page), components: [row] });
      const collector = messageReply.createMessageComponentCollector({ time: 120000 });

      collector.on('collect', async (interaction) => {
        if (interaction.user.id !== message.author.id) {
          return interaction.reply({ content: '❌ Only the original user can use these buttons.', ephemeral: true });
        }
        if (interaction.customId === 'prev') {
          page = page > 0 ? page - 1 : totalPages - 1;
          await interaction.update({ embeds: generateEmbeds(page) });
        }
        if (interaction.customId === 'next') {
          page = page < totalPages - 1 ? page + 1 : 0;
          await interaction.update({ embeds: generateEmbeds(page) });
        }
        if (interaction.customId === 'stop') {
          collector.stop();
          await interaction.update({ components: [] });
        }
      });

      collector.on('end', async () => {
        try { await messageReply.edit({ components: [] }); } catch (e) {}

      });

      const charmDrop = maybeRewardCharm(message.author.id, message.author.username);
      if (charmDrop) {
        message.channel.send(`🎁 **${message.author.username}** just got **${charmDrop.reward} $CHARM**!\n*${charmDrop.lore}*\n👉 Ping <@826581856400179210> to get your $CHARM`);
        console.log(`CHARM REWARD: ${message.author.username} (${message.author.id}) got ${charmDrop.reward} $CHARM.`);
      }

    } catch (err) {
      console.error('❌ Fetch failed (mysquigs):', err.message);
      return message.reply('⚠️ Error fetching your Squigs. Please try again later.');
    }
  }
});

// ===== SLASH HANDLER: /card =====
client.on('interactionCreate', async (interaction) => {
  if (!interaction.isChatInputCommand()) return;
  if (interaction.commandName !== 'card') return;

  const tokenId = interaction.options.getInteger('token_id');
  const customName = interaction.options.getString('name') || null;

  try {
    await interaction.deferReply();

    // Metadata from Alchemy (no ownership check)
    const meta = await getNftMetadataAlchemy(tokenId);
    const traitsRaw =
      Array.isArray(meta?.metadata?.attributes) ? meta.metadata.attributes :
      (Array.isArray(meta?.raw?.metadata?.attributes) ? meta.raw.metadata.attributes : []);
    const traits = normalizeTraits(traitsRaw);
    const displayName = customName || meta?.metadata?.name || `Squig #${tokenId}`;
    const imageUrl = `https://assets.bueno.art/images/a49527dc-149c-4cbc-9038-d4b0d1dbf0b2/default/${tokenId}`;

    // Try to fetch OpenSea rarity rank (optional)
    const rankInfo = OPENSEA_API_KEY
      ? await fetchOpenSeaRank(tokenId).catch(() => null)
      : null;

    // Fallback rarity label if no rank
    const rarityLabel = simpleRarityLabel(traitsRaw);
    const headerStripe = rarityColorFromLabel(rarityLabel);

    console.log('Traits debug:', {
      tokenId,
      groups: Object.fromEntries(Object.entries(traits).map(([k, v]) => [k, v?.length || 0])),
      rank: rankInfo?.rank ?? null,
      label: rarityLabel
    });

    const buffer = await renderSquigCard({
      name: displayName,
      tokenId,
      imageUrl,
      traits,
      rankInfo,
      rarityLabel,
      headerStripe
    });

    const file = new AttachmentBuilder(buffer, { name: `squig-${tokenId}-card.jpg` });
    await interaction.editReply({ content: `🪪 **${displayName}**`, files: [file] });

  } catch (err) {
    console.error('❌ /card error:', err);
    if (interaction.deferred) {
      await interaction.editReply('⚠️ Something went wrong building that card.');
    } else {
      await interaction.reply({ content: '⚠️ Something went wrong building that card.', ephemeral: true });
    }
  }
});

// ===== LOGIN =====
client.login(DISCORD_TOKEN);

// ===== Helper funcs (metadata, rarity, canvas, utils) =====
async function getNftMetadataAlchemy(tokenId) {
  const url = `https://eth-mainnet.g.alchemy.com/nft/v3/${ALCHEMY_API_KEY}/getNFTMetadata` +
              `?contractAddress=${SQUIGS_CONTRACT}&tokenId=${tokenId}&refreshCache=false`;
  const res = await fetchWithRetry(url);
  return res.json();
}

// OpenSea v2: Get NFT (attempt to read rarity)
async function fetchOpenSeaRank(tokenId) {
  const url = `https://api.opensea.io/api/v2/chain/ethereum/contract/${SQUIGS_CONTRACT}/nfts/${tokenId}`;
  const res = await fetchWithRetry(url, 2, 500, { headers: { 'X-API-KEY': OPENSEA_API_KEY } });
  const data = await res.json();
  const rarity = data?.rarity || data?.item?.rarity || null;
  const rank = rarity?.rank ?? rarity?.ranking ?? null;
  const score = rarity?.score ?? null;
  const percentile = rarity?.percentile ?? null;
  const total = rarity?.max_rank ?? rarity?.collection_size ?? null;
  return rank ? { rank, score, percentile, total } : null;
}

function loadTraitCountsSafe() {
  try {
    if (fs.existsSync('trait_counts.json')) {
      return JSON.parse(fs.readFileSync('trait_counts.json', 'utf8'));
    }
  } catch {}
  return {};
}

function simpleRarityLabel(attrs) {
  const n = Array.isArray(attrs) ? attrs.length : 0;
  if (n >= 9) return 'Mythic';
  if (n >= 7) return 'Legendary';
  if (n >= 5) return 'Rare';
  if (n >= 3) return 'Uncommon';
  return 'Common';
}

function rarityColorFromLabel(label) {
  switch ((label || '').toLowerCase()) {
    case 'mythic':    return '#7C3AED';
    case 'legendary': return '#F59E0B';
    case 'rare':      return '#3B82F6';
    case 'uncommon':  return '#10B981';
    default:          return '#9CA3AF';
  }
}

// Group traits into fixed categories
function normalizeTraits(list) {
  const groups = {
    Background: [],
    Body: [],
    Eyes: [],
    Head: [],
    Legend: [],
    Skin: [],
    Special: [],
    Type: []
  };
  for (const t of (list || [])) {
    const key = (t?.trait_type || '').trim();
    const val = t?.value;
    if (!key || typeof val === 'undefined' || val === null) continue;
    const target = (key in groups) ? key : key; // keep original for unknowns
    if (!groups[target]) groups[target] = [];
    groups[target].push({ trait_type: key, value: val });
  }
  return groups;
}

// ====== RENDERER (2-column trait grid with shaded mini-cards) ======
async function renderSquigCard({ name, tokenId, imageUrl, traits, rankInfo, rarityLabel, headerStripe }) {
  const W = 750, H = 1050;
  const canvas = createCanvas(W, H);
  const ctx = canvas.getContext('2d');

  // Background
  const g = ctx.createLinearGradient(0, 0, 0, H);
  g.addColorStop(0, '#f6f7ff'); g.addColorStop(1, '#e9ecff');
  ctx.fillStyle = g; ctx.fillRect(0, 0, W, H);

  // Outer frame
  drawRoundRect(ctx, 24, 24, W - 48, H - 48, 28, '#ffffff');
  ctx.strokeStyle = '#e5e7eb'; ctx.lineWidth = 2; ctx.stroke();

  // Header stripe (rarity color)
  drawRoundRectShadow(ctx, 48, 52, W - 96, 88, 18, headerStripe);
  ctx.fillStyle = '#0f172a';
  ctx.textBaseline = 'middle';
  ctx.font = `36px ${FONT_BOLD_FAMILY}`;
  ctx.fillText(name, 64, 96);

  // Rank / rarity (right)
  const rightText = rankInfo?.rank
    ? (rankInfo?.total ? `OpenSea Rank #${rankInfo.rank}/${rankInfo.total}` : `OpenSea Rank #${rankInfo.rank}`)
    : rarityLabel;
  ctx.font = `28px ${FONT_BOLD_FAMILY}`;
  const tw = ctx.measureText(rightText).width;
  ctx.fillText(rightText, W - 64 - tw, 96);

  // Art window
  const AX = 60, AY = 160, AW = W - 120, AH = 520;
  drawRoundRect(ctx, AX, AY, AW, AH, 16, '#f9fafb');
  ctx.strokeStyle = '#e5e7eb'; ctx.stroke();
  try {
    const img = await loadImage(await fetchBuffer(imageUrl));
    const { dx, dy, dw, dh } = contain(img.width, img.height, AW - 24, AH - 24);
    ctx.drawImage(img, AX + 12 + dx, AY + 12 + dy, dw, dh);
  } catch {
    ctx.fillStyle = '#9CA3AF'; ctx.font = `26px ${FONT_REGULAR_FAMILY}`;
    ctx.fillText('Image not available', AX + 20, AY + AH / 2);
  }

  // Traits panel background
  const TX = 60, TY = AY + AH + 28, TW = W - 120, TH = H - TY - 60;
  const pg = ctx.createLinearGradient(0, TY, 0, TY + TH);
  pg.addColorStop(0, '#f8fbff'); pg.addColorStop(1, '#eef2ff');
  drawRoundRect(ctx, TX, TY, TW, TH, 16, pg);
  ctx.strokeStyle = '#dbe3ff'; ctx.lineWidth = 2; ctx.stroke();

  // Inner grid area
  const PAD = 16;
  const innerX = TX + PAD, innerY = TY + PAD;
  const innerW = TW - PAD * 2, innerH = TH - PAD * 2;
  const COL_GAP = 16, COL_W = (innerW - COL_GAP) / 2;

  // Prepare categories in order, skipping empties
  const order = ['Background', 'Body', 'Eyes', 'Head', 'Legend', 'Skin', 'Special', 'Type'];
  const boxes = [];
  for (const cat of order) {
    const items = traits[cat] || [];
    if (!items.length) continue;

    // Cap visible items per box; show "+N more" if necessary
    const maxLines = 6;
    const lines = items.slice(0, maxLines).map(t => `• ${String(t?.value ?? '')}`);
    const hidden = items.length - lines.length;
    if (hidden > 0) lines.push(`+${hidden} more`);

    // Dynamic height
    const titleH = 26;
    const lineH = 22;
    const boxH = 20 + titleH + lines.length * lineH + 14;

    boxes.push({ cat, lines, boxH });
  }

  // Masonry flow into two columns
  let yL = innerY, yR = innerY;
  const placed = [];
  for (const b of boxes) {
    const left = yL <= yR;
    const x = left ? innerX : innerX + COL_W + COL_GAP;
    const y = left ? yL : yR;
    placed.push({ ...b, x, y, w: COL_W });
    if (left) yL += b.boxH + COL_GAP; else yR += b.boxH + COL_GAP;
  }

  // Draw each mini-card
  for (const b of placed) {
    drawRoundRectShadow(ctx, b.x, b.y, b.w, b.boxH, 12, '#ffffff', '#e5e7eb');

    // Header tint
    const headH = 36;
    const headG = ctx.createLinearGradient(0, b.y, 0, b.y + headH);
    headG.addColorStop(0, '#f3f4f6'); headG.addColorStop(1, '#eceef2');
    drawRoundRect(ctx, b.x, b.y, b.w, headH, 12, headG);
    ctx.strokeStyle = '#e5e7eb'; ctx.stroke();

    // Category title
    ctx.fillStyle = '#111827';
    ctx.font = `22px ${FONT_BOLD_FAMILY}`;
    ctx.textBaseline = 'alphabetic';
    ctx.fillText(b.cat, b.x + 14, b.y + 25);

    // Optional divider line
    ctx.strokeStyle = '#e5e7eb';
    ctx.beginPath();
    ctx.moveTo(b.x, b.y + headH + 0.5);
    ctx.lineTo(b.x + b.w, b.y + headH + 0.5);
    ctx.stroke();

    // Trait lines
    let yy = b.y + headH + 12;
    ctx.fillStyle = '#334155';
    ctx.font = `18px ${FONT_REGULAR_FAMILY}`;
    for (const line of b.lines) {
      ctx.fillText(line, b.x + 14, yy);
      yy += 22;
    }
  }

  // Footer token line
  ctx.fillStyle = '#6B7280';
  ctx.font = `18px ${FONT_REGULAR_FAMILY}`;
  ctx.fillText(`Squigs • Token #${tokenId}`, TX + 18, TY + TH - 14);

  return canvas.toBuffer('image/jpeg', { quality: 0.95 });
}

function drawRoundRect(ctx, x, y, w, h, r, fill) {
  const rr = Math.min(r, w / 2, h / 2);
  ctx.beginPath();
  ctx.moveTo(x + rr, y);
  ctx.arcTo(x + w, y, x + w, y + h, rr);
  ctx.arcTo(x + w, y + h, x, y + h, rr);
  ctx.arcTo(x, y + h, x, y, rr);
  ctx.arcTo(x, y, x + w, y, rr);
  ctx.closePath();
  ctx.fillStyle = fill; ctx.fill();
}

function drawRoundRectShadow(ctx, x, y, w, h, r, fill, stroke, shadowColor = '#00000022', shadowBlur = 14, shadowDy = 2) {
  ctx.save();
  ctx.shadowColor = shadowColor;
  ctx.shadowBlur = shadowBlur;
  ctx.shadowOffsetX = 0;
  ctx.shadowOffsetY = shadowDy;

  const rr = Math.min(r, w / 2, h / 2);
  ctx.beginPath();
  ctx.moveTo(x + rr, y);
  ctx.arcTo(x + w, y, x + w, y + h, rr);
  ctx.arcTo(x + w, y + h, x, y + h, rr);
  ctx.arcTo(x, y + h, x, y, rr);
  ctx.arcTo(x, y, x + w, y, rr);
  ctx.closePath();
  ctx.fillStyle = fill;
  ctx.fill();

  ctx.restore();
  if (stroke) { ctx.strokeStyle = stroke; ctx.lineWidth = 2; ctx.stroke(); }
}

function contain(sw, sh, mw, mh) {
  const s = Math.min(mw / sw, mh / sh);
  const dw = Math.round(sw * s), dh = Math.round(sh * s);
  return { dx: Math.round((mw - dw) / 2), dy: Math.round((mh - dh) / 2), dw, dh };
}

async function fetchBuffer(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Image HTTP ${r.status}`);
  const ab = await r.arrayBuffer();
  return Buffer.from(ab);
}
