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
  Routes,
  Events,
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
// Put this near your other consts (top of file is fine)
const RENDER_SCALE = 3; // 1 = 750x1050 (old). 2 = 1500x2100 (sharper). Try 3 if file size is fine.
const MASK_EPS = 0.75; // pixels
// How much tighter to shave the bg corners than the normal card radius (in px on 750×1050)
const BG_CORNER_TIGHTEN = 2; // try 8–12; increase if you still see flecks
// 1.00 = no zoom. Start ~1.05–1.10; go higher if you still see flecks.
const BG_ZOOM = 1.3;
// Optional tiny pan if any corner still shows a speck (pixels in card space)
const BG_PAN_X = 2;
const BG_PAN_Y = 2;


// ===== FONT REGISTRATION (auto-download if missing) =====
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
    try { GlobalFonts.registerFromPath(f.path, f.family); }
    catch (e) { console.warn('Font register error:', e.message); }
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
const UGLY_CONTRACT    = '0x9492505633d74451bdf3079c09ccc979588bc309';
const MONSTER_CONTRACT = '0x1cD7fe72D64f6159775643ACEdc7D860dFB80348';
const SQUIGS_CONTRACT  = '0x9bf567ddf41b425264626d1b8b2c7f7c660b1c42';

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

    const guildCmds = await rest.get(Routes.applicationGuildCommands(DISCORD_CLIENT_ID, GUILD_ID));
    console.log('🔎 Guild commands now:', guildCmds.map(c => `${c.name} (${c.id})`).join(', '));
  } catch (e) {
    console.error('❌ Slash register error:', e?.data ?? e);
  }
}

// ===== READY =====
client.once(Events.ClientReady, async (c) => {
  console.log(`✅ Logged in as ${c.user.tag}`);
  try { await registerSlashCommands(); } catch (e) {
    console.error('Slash register error:', e.message);
  }
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

// ===== PREFIX COMMANDS =====
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

    // Strict mint gate
    const minted = await isSquigMintedStrict(tokenId);
    if (minted !== true) {
      if (minted === 'UNVERIFIED') {
        return message.reply(`⏳ I can’t verify Squig #${tokenId} right now. Please try again in a moment—or mint at **https://squigs.io**`);
      }
      return message.reply(notMintedLine(tokenId));
    }

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

    // Strict mint gate for /card
    const minted = await isSquigMintedStrict(tokenId);
    if (minted !== true) {
      const msg = minted === 'UNVERIFIED'
        ? `⏳ I can’t verify Squig #${tokenId} right now. Please try again in a moment—or mint at **https://squigs.io**`
        : notMintedLine(tokenId);
      await interaction.editReply(msg);
      return;
    }

    // --- metadata ---
    const meta = await getNftMetadataAlchemy(tokenId);

    // --- traits (Alchemy first, OpenSea fallback) ---
    const { attrs, source } = await getTraitsForToken(meta, tokenId);
    const traits = normalizeTraits(attrs);

    // debug on Railway
    console.log(`Traits debug #${tokenId}: { source: ${source}, count: ${attrs.length}, sample: ${JSON.stringify(attrs.slice(0,3))} }`);

    // --- naming / image ---
    const displayName = customName || meta?.metadata?.name || `Squig #${tokenId}`;
    const imageUrl = `https://assets.bueno.art/images/a49527dc-149c-4cbc-9038-d4b0d1dbf0b2/default/${tokenId}`;

    // --- HP scoring & stripe color from total ---
    const hpAgg   = computeHpFromTraits(traits);        // { total, per }
    const hpTotal = hpAgg.total || 0;
    const tier    = hpToTierLabel(hpTotal);             // Common..Mythic
    const stripe  = hpToStripe(hpTotal);                // color for header/trait-headers

    // --- render ---
    const buffer = await renderSquigCard({
      name: displayName,
      tokenId,
      imageUrl,
      traits,
      rankInfo: { hpTotal, per: hpAgg.per }, // shows HP in header; per-trait HP in rows
      rarityLabel: tier,                     // kept for renderer fallback
      headerStripe: stripe                   // actual fill color used
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

// ===== Helper funcs (metadata) =====
async function getNftMetadataAlchemy(tokenId) {
  const url =
    `https://eth-mainnet.g.alchemy.com/nft/v3/${ALCHEMY_API_KEY}/getNFTMetadata` +
    `?contractAddress=${SQUIGS_CONTRACT}&tokenId=${tokenId}&refreshCache=false`;
  const res = await fetchWithRetry(url, 3, 800, { timeout: 10000 });
  return res.json();
}

// ===== STRICT MINT CHECK (hot-reload safe singletons) =====
const ALCHEMY_RPC_URL = `https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_API_KEY}`;
globalThis.__SQUIGS_PROVIDER   ||= new ethers.JsonRpcProvider(ALCHEMY_RPC_URL);
globalThis.__SQUIGS_ERC721_ABI ||= ['function ownerOf(uint256 tokenId) view returns (address)'];
globalThis.__SQUIGS_ERC721     ||= new ethers.Contract(SQUIGS_CONTRACT, globalThis.__SQUIGS_ERC721_ABI, globalThis.__SQUIGS_PROVIDER);
globalThis.__SQUIGS_MINT_CACHE ||= new Map();
const squigsErc721 = globalThis.__SQUIGS_ERC721;

// Not-minted messages (dedup-safe)
if (!globalThis.__SQUIGS_NOT_MINTED_MESSAGES) {
  globalThis.__SQUIGS_NOT_MINTED_MESSAGES = [
    (id) => `👀 Squig #${id} hasn’t crawled out of the mint swamp yet.\nGo hatch one at **https://squigs.io**`,
    (id) => `🫥 Squig #${id} is still a rumor. Mint your destiny at **https://squigs.io**`,
    (id) => `🌀 Squig #${id} is hiding in the spiral dimension. The portal is **https://squigs.io**`,
    (id) => `🥚 Squig #${id} is still an egg. Crack it open at **https://squigs.io**`,
    (id) => `🤫 The Squigs whisper: “#${id}? Not minted.” Try **https://squigs.io**`,
  ];
}
function notMintedLine(tokenId) {
  const list = globalThis.__SQUIGS_NOT_MINTED_MESSAGES;
  const pick = list[Math.floor(Math.random() * list.length)];
  return pick(tokenId);
}

/**
 * Strict mint check:
 *  1) ownerOf(tokenId): if it returns an address, it's minted; if it REVERTS, it's not minted.
 *  2) Fallback: Alchemy getOwnersForNFT.
 *  3) If both are unavailable, return 'UNVERIFIED' (we block rendering with a gentle message).
 */
async function isSquigMintedStrict(tokenId) {
  const cache = globalThis.__SQUIGS_MINT_CACHE;
  if (cache.has(tokenId)) return cache.get(tokenId);

  // 1) ERC-721 ownerOf via ethers
  try {
    const owner = await squigsErc721.ownerOf(tokenId);
    const minted = !!owner && owner !== '0x0000000000000000000000000000000000000000';
    cache.set(tokenId, minted);
    return minted;
  } catch (e) {
    const msg = String(e?.shortMessage || e?.message || '');
    if (e?.code === 'CALL_EXCEPTION' || /execution reverted/i.test(msg)) {
      cache.set(tokenId, false);
      return false;
    }
    // other errors: continue to fallback
  }

  // 2) Alchemy owners fallback
  try {
    const url = `https://eth-mainnet.g.alchemy.com/nft/v3/${ALCHEMY_API_KEY}/getOwnersForNFT` +
                `?contractAddress=${SQUIGS_CONTRACT}&tokenId=${tokenId}`;
    const res = await fetchWithRetry(url, 2, 600);
    const data = await res.json();
    const owners =
      (Array.isArray(data?.owners) && data.owners) ||
      (Array.isArray(data?.ownerAddresses) && data.ownerAddresses) ||
      [];
    const minted = owners.length > 0;
    cache.set(tokenId, minted);
    return minted;
  } catch (e2) {
    console.warn(`⚠️ Mint check unavailable for #${tokenId}:`, e2.message);
    return 'UNVERIFIED';
  }
}

// -------- flexible trait extraction with OpenSea fallback --------
async function getTraitsForToken(alchemyMeta, tokenId) {
  // 1) Try Alchemy
  const attrsA = extractAttributesFlexible(alchemyMeta);
  if (attrsA.length > 0) {
    return { attrs: attrsA, source: 'alchemy' };
  }

  // 2) Fallback to OpenSea if we have an API key
  if (OPENSEA_API_KEY) {
    try {
      const attrsB = await fetchOpenSeaTraits(tokenId);
      if (attrsB.length > 0) {
        console.log(`ℹ️ Traits from OpenSea fallback for #${tokenId}: ${attrsB.length}`);
        return { attrs: attrsB, source: 'opensea' };
      }
    } catch (e) {
      console.warn('⚠️ OpenSea trait fallback failed:', e.message);
    }
  }

  return { attrs: [], source: 'none' };
}

function extractAttributesFlexible(alchemyMeta) {
  if (!alchemyMeta) return [];
  const candidates = [];
  const addIfAttrArray = (arr) => {
    if (Array.isArray(arr) && arr.length && looksLikeAttributeArray(arr)) candidates.push(arr);
  };

  // common Alchemy spots
  addIfAttrArray(alchemyMeta?.metadata?.attributes);
  addIfAttrArray(alchemyMeta?.raw?.metadata?.attributes);
  addIfAttrArray(alchemyMeta?.metadata?.traits);
  addIfAttrArray(alchemyMeta?.raw?.metadata?.traits);
  addIfAttrArray(alchemyMeta?.metadata?.properties?.attributes);
  addIfAttrArray(alchemyMeta?.raw?.metadata?.properties?.attributes);

  // sometimes raw JSON is a string
  const tryParse = (maybeStr) => {
    try {
      if (typeof maybeStr === 'string' && maybeStr.trim().startsWith('{')) {
        const obj = JSON.parse(maybeStr);
        addIfAttrArray(obj.attributes);
        addIfAttrArray(obj.traits);
        addIfAttrArray(obj?.properties?.attributes);
      }
    } catch {}
  };
  tryParse(alchemyMeta?.raw?.metadata);
  tryParse(alchemyMeta?.metadata);

  const first = candidates.find(Boolean) || [];
  return first.map(massageTraitKeys).filter(validAttrFilter);
}

function looksLikeAttributeArray(arr) {
  return arr.some(o => o && typeof o === 'object' &&
    ('value' in o) && ('trait_type' in o || 'traitType' in o || 'type' in o || 'key' in o));
}

function massageTraitKeys(t) {
  const trait_type = String(t.trait_type ?? t.traitType ?? t.type ?? t.key ?? '').trim();
  return { trait_type, value: t.value };
}

function validAttrFilter(t) {
  const v = String(t?.value ?? '').trim();
  if (!v) return false;
  const low = v.toLowerCase();
  return !(low === 'none' || low === 'none (ignore)');
}

// OpenSea v2: fallback trait fetch (with headers + small retry)
async function fetchOpenSeaTraits(tokenId) {
  const url = `https://api.opensea.io/api/v2/chain/ethereum/contract/${SQUIGS_CONTRACT}/nfts/${tokenId}`;
  const headers = { 'X-API-KEY': OPENSEA_API_KEY };
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(url, { headers, timeout: 10000 });
      if (!res.ok) throw new Error(`OpenSea HTTP ${res.status}`);
      const data = await res.json();
      const arr =
        (Array.isArray(data?.nft?.traits) && data.nft.traits) ||
        (Array.isArray(data?.traits) && data.traits) ||
        (Array.isArray(data?.item?.traits) && data.item.traits) ||
        [];
      return arr.map(massageTraitKeys).filter(validAttrFilter);
    } catch (e) {
      if (attempt === 2) throw e;
      await new Promise(r => setTimeout(r, 500));
    }
  }
  return [];
}

// ===== HP-BASED TIERS =====
// Observed non-legend range with your current HP_TABLE:
const NONLEG_HP_MIN = 455;   // lowest non-legend observed
const NONLEG_HP_MAX = 891;   // highest non-legend observed

// Legend 1/1s:
const LEGEND_HP = 1000;

// Percentile-based cutpoints from the dataset:
const CUT_UNCOMMON = 608; // ~45th percentile
const CUT_RARE     = 656; // ~70th percentile
const CUT_LEGEND   = 742; // ~95th percentile

/**
 * Map HP to a rarity label.
 * - "Mythic" = 1/1 Legends only (HP = 1000)
 * - "Legendary" = top ~5% of non-Legend scores
 * - "Rare" = next ~25%
 * - "Uncommon" = next ~25% (up to ~70th pct)
 * - "Common" = the rest
 */
function hpToTierLabel(hp) {
  if (hp >= LEGEND_HP) return 'Mythic';
  if (hp >= CUT_LEGEND) return 'Legendary';
  if (hp >= CUT_RARE)   return 'Rare';
  if (hp >= CUT_UNCOMMON) return 'Uncommon';
  return 'Common';
}



// ===== COLORS / THEME =====
const PALETTE = {
  // Frame colors are not filled anymore (bg image covers card); we keep a black outline.
  cardBg: '#242623',
  frameStroke: '#000000',
  headerText: '#0F172A',
  rarityStripeByTier: {
    Mythic:   '#896936',
    Legendary:'#FFF1AE',
    Rare:     '#7ADDC0',
    Uncommon: '#7A83BF',
    Common:   '#B0DEEE',
  },
  artBackfill: '#b9dded',
  artStroke:   '#F9FAFB',
  traitsPanelBg:     '#b9dded', // drawn with alpha for transparency
  traitsPanelStroke: '#000000',
  traitCardFill:     '#FFFFFF',
  traitCardStroke:   '#000000',
  traitCardShadow:   '#0000001A',
  traitTitleText:    '#222625',
  traitValueText:    '#000000',
  footerText:        '#212524',
};

// --- Background images by tier ---
// Mythic uses the special (LEGENDARY BG) art; all other tiers use Stock BG.
const CARD_BG_URLS = {
  Mythic: [
    'https://github.com/GuyLeDouce/UglyBot/blob/main/LEGENDARY%20BG.png?raw=true'
  ],
  Legendary: [
    'https://github.com/GuyLeDouce/UglyBot/blob/main/Stock%20BG.png?raw=true'
  ],
  Rare: [
    'https://github.com/GuyLeDouce/UglyBot/blob/main/Stock%20BG.png?raw=true'
  ],
  Uncommon: [
    'https://github.com/GuyLeDouce/UglyBot/blob/main/Stock%20BG.png?raw=true'
 ],
  Common: [
    'https://github.com/GuyLeDouce/UglyBot/blob/main/Stock%20BG.png?raw=true'
  ]
};


function stripeFromRarity(label) {
  return PALETTE.rarityStripeByTier[label] || PALETTE.rarityStripeByTier.Common;
}
function hpToStripe(hp) { return stripeFromRarity(hpToTierLabel(hp)); }

// Local font aliases
const FONT_REG =
  (typeof FONT_REGULAR_FAMILY !== 'undefined' ? FONT_REGULAR_FAMILY :
  (typeof FONT_FAMILY_REGULAR !== 'undefined' ? FONT_FAMILY_REGULAR : 'sans-serif'));
const FONT_BOLD =
  (typeof FONT_BOLD_FAMILY !== 'undefined' ? FONT_BOLD_FAMILY :
  (typeof FONT_FAMILY_BOLD !== 'undefined' ? FONT_FAMILY_BOLD : 'sans-serif'));

// ====== HP SCORE TABLE + helpers ======
const HP_TABLE = {
  "Legend": {
    "Beige Giant Ears": 1000,
    "Beige Giant Head": 1000,
    "Beige Half Cut": 1000,
    "Beige Malformed": 1000,
    "Beige Zombie": 1000,
    "Brown Yeti": 1000,
    "Cornhuglyio": 1000,
    "Dark Brown Giant Ears": 1000,
    "Dark Brown Giant Head": 1000,
    "Dark Brown Half Cut": 1000,
    "Dark Brown Malformed": 1000,
    "Dark Brown Zombie": 1000,
    "Gold Halo": 1000,
    "Green Slime": 1000,
    "Green Zombie": 1000,
    "Monochrome": 1000,
    "Night": 1000,
    "Orange Zombie": 1000,
    "Pikachugly": 1000,
    "Purple Yeti": 1000,
    "Purple Zombie": 1000,
    "Robot": 1000,
    "Silver": 1000,
    "Sulks Elf": 1000,
    "Yellow Slime": 1000
  },
  "Type": {
    "Unknown": 50,
    "Squigs": 47,
    "Elf Squigs": 45
  },
  "Background": {
    "Unknown": 150,
    "Dark Blue Galaxy": 147,
    "Grey Boom": 144,
    "Grey Splash": 141,
    "Blue Splash": 138,
    "Purple Splash": 136,
    "Light Blue Boom": 134,
    "Yellow Splash": 132,
    "Green Boom": 130,
    "Light Blue Splash": 128,
    "Dark Blue Boom": 126,
    "Dark Blue Splash": 124,
    "Green Splash": 122,
    "Blue Boom": 120,
    "Purple Boom": 118,
    "Yellow Boom": 116,
    "Green": 114,
    "Purple": 112,
    "Dark Blue": 110,
    "Light Blue": 108,
    "Yellow": 106,
    "Grey": 104,
    "Blue": 102
  },
  "Body": {
    "Beavis Tee": 180,
    "Butthead Tee": 178,
    "Blue Sexy Bowling Shirt": 176,
    "Unknown": 174,
    "Super Ugly": 172,
    "Airplane Life Jacket": 170,
    "Cowboy Jacket": 168,
    "Futuristic Armor": 166,
    "Astronaut": 164,
    "Jedi": 162,
    "Sexy Bowling Shirt": 160,
    "Yellow Jersey": 158,
    "Rick Laser": 156,
    "Acupuncture": 154,
    "Ugly Food Shirt": 152,
    "Suit": 150,
    "Ugly Scene": 148,
    "Ugly Side of the Moon": 146,
    "Basketball Jersey": 144,
    "Mario Overalls": 142,
    "Pet Lover Bag": 140,
    "UGS Jacket": 138,
    "Ninja": 136,
    "Astronaut Blue": 134,
    "Maple Leafs 1967 Tracksuit": 132,
    "Caveman": 130,
    "White and Green Jacket": 128,
    "Maple Leafs 1967": 126,
    "Blue Wetsuit": 124,
    "Cheerleader": 122,
    "Sports Bra": 120,
    "Ugly Army Tank": 118,
    "Red Cowboy": 116,
    "Blue Cowboy": 114,
    "Gardener Overalls": 112,
    "PAAF White Tee": 110,
    "Indian": 108,
    "Red Baseball Shirt": 106,
    "Camo Wetsuit": 104,
    "Grey Bike Jersey": 102,
    "Prison Tee": 100,
    "Blue Baseball Jersey": 98,
    "Born Ugly Tee Tank": 96,
    "Red Varsity": 94,
    "White Ugly Tank": 92,
    "Long Sleeve Flame Tee": 90,
    "Monster Tee Bag": 88,
    "Yellow Tracksuit": 86,
    "Black Puffy": 84,
    "Bowling Shirt": 82,
    "Grease Tank": 80,
    "Light Green Tee": 78,
    "White and Blue Jacket": 76,
    "Green Varsity": 74,
    "Purple Hoodie": 72,
    "Beige Jacket": 70,
    "420 Purple Tracksuit": 68,
    "Beige Fisherman Jacket": 66,
    "Grey Fisherman Jacket": 64,
    "Pink Jacket": 62,
    "Borat": 60,
    "Blue Overalls": 58,
    "Flame Tee Tank": 56,
    "Hawaiian Shirt": 54,
    "Holey Tee": 52,
    "Yellow Puffy": 50,
    "Ugly Tank on Pink": 49,
    "Purple Shirt": 48,
    "Tattooed Body": 47,
    "Naked": 46,
    "Green Sweater": 45,
    "Black Sweater": 44,
    "Born Ugly Tee": 43,
    "Purple Tee": 42,
    "White Tee": 41,
    "Green Tee": 40
  },
  "Eyes": {
    "Unknown": 80,
    "Bionic": 78,
    "Bionic Lashes": 76,
    "Angry Cyclops": 74,
    "Sleepy Trio": 72,
    "Angry Cyclops Lashes": 70,
    "Angry Trio": 68,
    "Angry Trio Lashes": 66,
    "Cyclops": 64,
    "Cyclops Lashes": 62,
    "Trio": 61,
    "Trio Lashes": 60
  },
  "Head": {
    "Butthead Hair": 280,
    "Cape": 277,
    "Paper bag": 274,
    "Beavis Hair": 271,
    "Hairball Z": 268,
    "Buoy": 265,
    "Sheriff": 262,
    "Human Air": 259,
    "VR Headset": 256,
    "Tyre": 253,
    "Captured Piranha": 250,
    "Slasher": 247,
    "Crown": 244,
    "Unknown": 241,
    "UGS Delivery": 238,
    "Blindfold": 235,
    "Headphones": 232,
    "Hood": 229,
    "Indian": 226,
    "Diving Mask": 223,
    "Lobster": 220,
    "Green Visor": 217,
    "Acupuncture": 215,
    "Basketball": 213,
    "Elf Human Air": 211,
    "Watermelon": 209,
    "Elf Hood": 207,
    "Flower Power": 205,
    "Long Hair": 203,
    "Beer": 201,
    "Bunny": 199,
    "Imposter Mask": 197,
    "Fro Comb": 195,
    "Halo": 193,
    "Knife": 191,
    "Night Vision": 189,
    "Proud to be Ugly": 187,
    "Dinomite": 185,
    "Purr Paw": 183,
    "HeliHat": 181,
    "I Need TP": 179,
    "Panda Bike Helmet": 177,
    "Pot Head": 175,
    "Visor with Hair": 173,
    "Rainbow": 171,
    "Space Brain": 169,
    "Blonde Fro Comb": 167,
    "Zeus Hand": 165,
    "Captain": 163,
    "Pirate": 161,
    "Ski Mask": 159,
    "Dread Cap": 157,
    "Boom Bucket": 155,
    "Baseball": 153,
    "Green Cap": 151,
    "Honey Pot": 149,
    "Pomade": 147,
    "Bear Fur Hat": 145,
    "Ice Cream": 143,
    "Rastalocks": 141,
    "3D": 139,
    "Trucker": 137,
    "Umbrella Hat": 135,
    "Golfs": 133,
    "Green Ugly": 131,
    "Pink Beanie": 129,
    "Rice Dome": 127,
    "Head Canoe": 125,
    "Brain Bucket": 123,
    "Fiesta": 121,
    "Cactus": 119,
    "Fire": 117,
    "Floral": 115,
    "Green Mountie": 113,
    "Lemon Bucket": 111,
    "Notlocks": 109,
    "90's blonde": 107,
    "Bandana": 105,
    "Cowboy": 103,
    "Mountie": 101,
    "Yellow Beanie": 99,
    "90's Pink": 97,
    "Black Ugly": 95,
    "Twintails": 93,
    "Grey Cap": 91,
    "Cube Cut": 89,
    "Yellow Twintails": 87,
    "Afro": 85,
    "Green Beanie": 83,
    "Parted": 81,
    "Tin Topper": 79,
    "Bald": 77,
    "Blond Punk": 75,
    "Purple Punk": 73
  },
  "Skin": {
    "Unknown": 170,
    "Cristal": 167,
    "Cristal Elf": 164,
    "Green Camo": 161,
    "Purple Elf Space": 158,
    "Purple Space": 155,
    "Green Elf Camo": 152,
    "Green Elf": 149,
    "Green": 146,
    "Orange": 144,
    "Purple": 142,
    "Purple Elf": 140,
    "Orange Elf": 138,
    "Pink": 136,
    "Pink Elf": 134,
    "Brown": 132,
    "Beige": 130,
    "Beige Elf": 128,
    "Brown Elf": 126,
    "Dark Brown": 124,
    "Dark Brown Elf": 122
  },
  "Special": {
    "Laser Tits": 80,
    "Green Laser": 78,
    "Yellow Laser": 76,
    "Nouns Glasses": 74,
    "Ugly Necklace": 72,
    "Dino": 70,
    "Weed Necklace": 68,
    "Parakeet": 66,
    "Piranha": 64,
    "Smile Necklace": 62,
    "Monocle": 60,
    "Sad Smile Necklace": 59,
    "None": 0,
  }
};



function hpFor(cat, val) {
  const group = HP_TABLE[cat];
  if (!group) return 0;
  const key = Object.keys(group).find(
    k => k.toLowerCase() === String(val).trim().toLowerCase()
  );
  return key ? group[key] : 0;
}

function computeHpFromTraits(groupedTraits) {
  let total = 0;
  const per = {};
  for (const cat of Object.keys(groupedTraits)) {
    for (const t of groupedTraits[cat]) {
      const s = hpFor(cat, t.value);
      total += s;
      per[`${cat}::${t.value}`] = s;
    }
  }
  return { total, per };
}

/* ──────────────────────────────────────────────────────────────────────────
   Rounded-corner harmony (one set of radii everywhere) + tiny over-mask to
   “eat” any fringe in the background corners.
────────────────────────────────────────────────────────────────────────── */
const RADIUS = {
  card: 38,        // outer card mask
  header: 16,      // title block
  art: 26,         // NFT image
  traitsPanel: 18, // semi-transparent panel
  traitCard: 16,   // white mini-cards
  pill: 22         // rarity pill
};

async function drawCardBgWithoutBorder(ctx, W, H, tierLabel) {
  const bg = await loadBgByTier(tierLabel);
  if (!bg) {
    ctx.fillStyle = PALETTE.cardBg;
    ctx.fillRect(0, 0, W, H);
    return;
  }

  // Keep your edge trim so the baked border is cropped off the source.
  const TRIM_X = Math.round(bg.width  * 0.036);
  const TRIM_Y = Math.round(bg.height * 0.034);
  const sx = TRIM_X, sy = TRIM_Y;
  const sw = bg.width  - TRIM_X * 2;
  const sh = bg.height - TRIM_Y * 2;

  // Clip to the *same* card cutout. OVER extends clip a hair outside the card,
  // so we never see a seam; it does not change the visible cutout size.
  const OVER = (typeof MASK_EPS === 'number' ? MASK_EPS : 1.25);
  ctx.save();
  roundRectPath(ctx, -OVER, -OVER, W + OVER * 2, H + OVER * 2, RADIUS.card + 4);
  ctx.clip();

  // Force-zoom the background under that mask.
  // Using a transform guarantees the zoom applies even if the image is cached.
  const Z = Math.max(1, Number.isFinite(BG_ZOOM) ? BG_ZOOM : 1.06);
  ctx.save();
  ctx.translate(W / 2 + (BG_PAN_X || 0), H / 2 + (BG_PAN_Y || 0)); // optional pan
  ctx.scale(Z, Z);
  // Draw the trimmed image centered, sized to the card *pre*-scale
  ctx.drawImage(bg, sx, sy, sw, sh, -W / 2, -H / 2, W, H);
  ctx.restore(); // undo scale/translate

  ctx.restore(); // undo clip
}

// ===== TRAIT NORMALIZER =====
const TRAIT_ORDER = ['Type', 'Background', 'Body', 'Eyes', 'Head', 'Legend', 'Skin', 'Special'];
function normalizeTraits(attrs) {
  const groups = {};
  for (const k of TRAIT_ORDER) groups[k] = [];

  for (const t of (Array.isArray(attrs) ? attrs : [])) {
    const type = String(t?.trait_type ?? '').trim();
    const valStr = String(t?.value ?? '').trim();
    if (!type || !valStr) continue;
    if (valStr.toLowerCase() === 'none' || valStr.toLowerCase() === 'none (ignore)') continue;
    if (!Object.prototype.hasOwnProperty.call(groups, type)) continue;
    groups[type].push({ value: valStr });
  }
  return groups;
}

// Back-compat alias
function rarityColorFromLabel(label) { return stripeFromRarity(label); }

// ---------- image helpers / cache ----------
globalThis.__CARD_IMG_CACHE ||= {};
async function loadImageCached(url) {
  if (globalThis.__CARD_IMG_CACHE[url]) return globalThis.__CARD_IMG_CACHE[url];
  const buf = await fetchBuffer(url);
  const img = await loadImage(buf);
  globalThis.__CARD_IMG_CACHE[url] = img;
  return img;
}
async function loadBgByTier(tier) {
  const list = CARD_BG_URLS[tier] || CARD_BG_URLS.Common;
  for (const url of list) {
    try {
      return await loadImageCached(url);
    } catch (e) {
      console.warn('BG load failed:', url, e.message);
    }
  }
  return null;
}
function hexToRgba(hex, a = 1) {
  const h = hex.replace('#', '');
  const v = h.length === 3 ? h.split('').map(c => c + c).join('') : h;
  const n = parseInt(v, 16);
  const r = (n >> 16) & 255, g = (n >> 8) & 255, b = n & 255;
  return `rgba(${r},${g},${b},${a})`;
}
// Swap labels ONLY for display in the rarity pill
function pillLabelForTier(label) {
  if (!label) return '';
  const l = String(label);
  return l === 'Mythic' ? 'Legendary'
       : l === 'Legendary' ? 'Epic'
       : l;
}

async function renderSquigCard({ name, tokenId, imageUrl, traits, rankInfo, rarityLabel, headerStripe }) {
  const W = 750, H = 1050;
  const SCALE = (typeof RENDER_SCALE !== 'undefined' ? RENDER_SCALE : 2);

  // Hi-DPI canvas
  const canvas = createCanvas(W * SCALE, H * SCALE);
  const ctx = canvas.getContext('2d');
  ctx.scale(SCALE, SCALE);
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = 'high';

  // Tier / stripe
  const tierLabel = (rarityLabel && String(rarityLabel)) || hpToTierLabel(rankInfo?.hpTotal || 0);
  const headerStripeFill = headerStripe || stripeFromRarity(tierLabel);

  // Background (trimmed; rounded mask)
  await drawCardBgWithoutBorder(ctx, W, H, tierLabel);

  // -------- Layout knobs --------
  const HEADER_W        = 640;
  const HEADER_H        = 64;
  const HEADER_SIDE_PAD = 18;
  const HEADER_Y        = 20;

  const ART_W_MAX       = 560;

  // -------- Rarity pill (bigger, near corner) --------
  const PILL_H     = 62;
  const PILL_PAD_X = 24;
  const pillText   = pillLabelForTier(tierLabel);
  ctx.font         = `24px ${FONT_BOLD}`;
  const pTextW     = ctx.measureText(pillText).width;
  const pillW      = pTextW + PILL_PAD_X * 2;
  const PILL_MR    = 22;
  const PILL_MB    = 22;
  const pillX      = W - PILL_MR - pillW;
  const pillY      = H - PILL_MB - PILL_H;
  const pillCenterY = pillY + PILL_H / 2;

  // -------- Title block --------
  const headerX = Math.round((W - HEADER_W) / 2);
  drawRoundRectShadow(
    ctx, headerX, HEADER_Y, HEADER_W, HEADER_H, RADIUS.header,
    headerStripeFill, null, 'rgba(0,0,0,0.16)', 14, 3
  );

  // Title text
  ctx.fillStyle = PALETTE.headerText;
  ctx.textBaseline = 'middle';
  const headerMidY = HEADER_Y + HEADER_H / 2;
  ctx.font = `32px ${FONT_BOLD}`;
  ctx.fillText(name, headerX + HEADER_SIDE_PAD, headerMidY);

  // HP (right)
  const hpText = `${rankInfo?.hpTotal ?? 0} HP`;
  ctx.font = `26px ${FONT_BOLD}`;
  const hpW = ctx.measureText(hpText).width;
  ctx.fillText(hpText, headerX + HEADER_W - HEADER_SIDE_PAD - hpW, headerMidY);

  // ================= ART + TRAITS =================
  const headerBottom = HEADER_Y + HEADER_H;

  // Traits panel bottom is anchored to the pill center
  const TRAITS_W     = HEADER_W;
  const traitsBottom = pillCenterY;

  // Start with a pleasant panel height; will autoshrink text if needed
  let TH = Math.round((traitsBottom - headerBottom) * 0.36);
  TH = Math.max(210, TH);

  const TX = Math.round((W - TRAITS_W) / 2);
  const TY = traitsBottom - TH;
  const TW = TRAITS_W;

  // Region available for art between title bottom and traits top
  const midRegion = TY - headerBottom;

  // Make the art large but keep equal gaps above and below it
  const MIN_ART_H  = 380;
  const GAP_TARGET = 28;  // breathing room
  const GAP_MIN    = 16;

  let ART_W = Math.min(ART_W_MAX, W - 2 * (headerX - 20));
  let ART_H = ART_W; // square

  let G = GAP_TARGET;                            // equal top/bottom gap for art
  let maxArtH = midRegion - 2 * G;
  if (maxArtH < MIN_ART_H) {
    G = Math.max(GAP_MIN, Math.floor((midRegion - MIN_ART_H) / 2));
    maxArtH = midRegion - 2 * G;
  }
  ART_H = Math.min(ART_H, Math.max(100, maxArtH));
  ART_W = ART_H;

  const AX = Math.round((W - ART_W) / 2);
  const AY = Math.round(headerBottom + G);

  // Draw art (no stroke; soft bottom-right shadow)
  drawRoundRectShadow(
    ctx, AX, AY, ART_W, ART_H, RADIUS.art,
    PALETTE.artBackfill, null, 'rgba(0,0,0,0.14)', 14, 3
  );
  ctx.save();
  roundRectPath(ctx, AX, AY, ART_W, ART_H, RADIUS.art);
  ctx.clip();
  try {
    const img = await loadImage(await fetchBuffer(imageUrl));
    const { dx, dy, dw, dh } = cover(img.width, img.height, ART_W, ART_H);
    ctx.drawImage(img, AX + dx, AY + dy, dw, dh);
  } catch {}
  ctx.restore();

  // Traits panel background
  drawRoundRect(ctx, TX, TY, TW, TH, RADIUS.traitsPanel, hexToRgba(PALETTE.traitsPanelBg, 0.58));

  // -------- Top-aligned trait layout inside the panel --------
  const PAD = 12, innerX = TX + PAD, innerY = TY + PAD, innerW = TW - PAD * 2, innerH = TH - PAD * 2;
  const COL_GAP = 12, COL_W = (innerW - COL_GAP) / 2;

  function layout(lineH = 16, titleH = 24, blockPad = 6) {
    const boxes = [];
    for (const cat of TRAIT_ORDER) {
      const items = (traits[cat] || []);
      if (!items.length) continue;

      const lines  = items.map(t => `${String(t.value)} (${hpFor(cat, t.value)} HP)`);
      const shown  = lines.slice(0, 5);
      const hidden = lines.length - shown.length;
      if (hidden > 0) shown.push(`+${hidden} more`);

      const rowsH = shown.length * lineH;
      const minRows = 32;
      const boxH = blockPad + titleH + Math.max(rowsH + 8, minRows) + blockPad;
      boxes.push({ cat, lines: shown, boxH, lineH, titleH, blockPad });
    }

    let yL = innerY, yR = innerY;
    const placed = [];
    for (const b of boxes) {
      const left = yL <= yR;
      const x = left ? innerX : innerX + COL_W + COL_GAP;
      const y = left ? yL : yR;
      placed.push({ ...b, x, y, w: COL_W });
      if (left) yL += b.boxH + COL_GAP; else yR += b.boxH + COL_GAP;
    }
    const usedH = Math.max(yL, yR) - innerY;

    // If overflow, proportionally shrink typography/padding
    if (usedH > innerH) {
      const scale = Math.max(0.82, innerH / usedH);
      return layout(
        Math.max(14, Math.floor(16 * scale)),
        Math.max(22, Math.floor(24 * scale)),
        Math.max(5,  Math.floor(6  * scale))
      );
    }
    return placed;
  }

  const placed = layout();

  // -------- Trait mini-cards (tab + shadow; larger values) --------
  const BUBBLE_R    = RADIUS.traitCard;
  const TAB_OVERLAP = 2;
  const TAB_EXTRA   = 3;
  const ROW_PAD_Y   = 6;

  for (const b of placed) {
    drawRoundRectShadow(
      ctx, b.x, b.y, b.w, b.boxH, BUBBLE_R,
      PALETTE.traitCardFill, null, 'rgba(0,0,0,0.14)', 12, 3
    );
    const tabH = b.titleH + TAB_OVERLAP + TAB_EXTRA;
    drawTopRoundedRect(ctx, b.x, b.y, b.w, tabH, BUBBLE_R, headerStripeFill);

    // Category
    ctx.fillStyle = PALETTE.traitTitleText;
    ctx.font = `16px ${FONT_BOLD}`;
    ctx.textBaseline = 'alphabetic';
    const mt = ctx.measureText(b.cat);
    const tH = (mt.actualBoundingBoxAscent || 0) + (mt.actualBoundingBoxDescent || 0);
    const titleY = b.y + (tabH - tH) / 2 + (mt.actualBoundingBoxAscent || 0);
    ctx.fillText(b.cat, b.x + (b.w - mt.width) / 2, titleY);

    // Values (centered)
    let yy = b.y + tabH + ROW_PAD_Y;
    ctx.fillStyle = PALETTE.traitValueText;
    ctx.font = `16px ${FONT_REG}`;
    ctx.textBaseline = 'middle';
    for (const line of b.lines) {
      const lw = ctx.measureText(line).width;
      ctx.fillText(line, b.x + (b.w - lw) / 2, yy + Math.floor(b.lineH / 2));
      yy += b.lineH;
    }
  }

// Footer — vertically centered between the traits panel bottom and card bottom
{
  const footerY = Math.round((traitsBottom + H) / 2); // traitsBottom = TY + TH (already defined above)
  ctx.fillStyle = PALETTE.footerText;
  ctx.font = `18px ${FONT_REG}`;
  ctx.textBaseline = 'middle';
  ctx.fillText(`Squigs • Token #${tokenId}`, 60, footerY);
}


  // Rarity pill (shadow; black text)
  drawRoundRectShadow(ctx, pillX, pillY, pillW, PILL_H, RADIUS.pill, headerStripeFill, null, 'rgba(0,0,0,0.14)', 12, 3);
  ctx.fillStyle = '#000000';
  ctx.textBaseline = 'middle';
  ctx.font = `24px ${FONT_BOLD}`;
  ctx.fillText(pillText, pillX + PILL_PAD_X, pillY + PILL_H / 2);

  return canvas.toBuffer('image/jpeg', { quality: 0.98, progressive: true });
}

// ---------- drawing helpers ----------
function drawRect(ctx, x, y, w, h, fill) { ctx.fillStyle = fill; ctx.fillRect(x, y, w, h); }
function roundRectPath(ctx, x, y, w, h, r) {
  const rr = Math.min(r, w / 2, h / 2);
  ctx.beginPath();
  ctx.moveTo(x + rr, y);
  ctx.arcTo(x + w, y, x + w, y + h, rr);
  ctx.arcTo(x + w, y + h, x, y + h, rr);
  ctx.arcTo(x, y + h, x, y, rr);
  ctx.arcTo(x, y, x + w, y, rr);
  ctx.closePath();
}
function drawRoundRect(ctx, x, y, w, h, r, fill) {
  roundRectPath(ctx, x, y, w, h, r);
  ctx.fillStyle = fill; ctx.fill();
}
function drawRoundRectShadow(ctx, x, y, w, h, r, fill, stroke, shadowColor = '#00000022', shadowBlur = 14, shadowDy = 2) {
  ctx.save();
  ctx.shadowColor = shadowColor;
  ctx.shadowBlur = shadowBlur;
  ctx.shadowOffsetX = 0;
  ctx.shadowOffsetY = shadowDy;
  drawRoundRect(ctx, x, y, w, h, r, fill);
  ctx.restore();
  if (stroke) { ctx.strokeStyle = stroke; ctx.lineWidth = 2; roundRectPath(ctx, x, y, w, h, r); ctx.stroke(); }
}
// Top-rounded, flat-bottom rectangle (for the category tab)
function drawTopRoundedRect(ctx, x, y, w, h, r, fill) {
  const rr = Math.min(r, w / 2, h);
  ctx.beginPath();
  ctx.moveTo(x + rr, y);
  ctx.arcTo(x + w, y, x + w, y + rr, rr); // top-right
  ctx.lineTo(x + w, y + h);                // right edge (square bottom)
  ctx.lineTo(x,     y + h);                // bottom edge (flat)
  ctx.lineTo(x,     y + rr);               // left edge
  ctx.arcTo(x, y, x + rr, y, rr);          // top-left
  ctx.closePath();
  ctx.fillStyle = fill;
  ctx.fill();
}
// image cover
function cover(sw, sh, mw, mh) {
  const s = Math.max(mw / sw, mh / sh);
  const dw = Math.round(sw * s), dh = Math.round(sh * s);
  return { dx: Math.round((mw - dw) / 2), dy: Math.round((mh - dh) / 2), dw, dh };
}
async function fetchBuffer(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Image HTTP ${r.status}`);
  const ab = await r.arrayBuffer();
  return Buffer.from(ab);
}
