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

  // Block unminted IDs with a funny message
  try {
    const minted = await isSquigMinted(tokenId);
    if (!minted) {
      return message.reply(notMintedLine(tokenId));
    }
  } catch (e) {
    console.warn('mint check (prefix) failed:', e.message);
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
// Block unminted IDs with a funny message
const minted = await isSquigMinted(tokenId);
if (!minted) {
  await interaction.editReply(notMintedLine(tokenId));
  return;
}

  // --- metadata ---
  const meta = await getNftMetadataAlchemy(tokenId);

  // --- traits (Alchemy first, OpenSea fallback) ---
  const { attrs, source } = await getTraitsForToken(meta, tokenId);
  const traits = normalizeTraits(attrs);

  // helpful log for debugging in Railway
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
  // skip empty/none variants
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

// ===== HP-BASED TIERS (replace the old heuristic rarity) =====
// Score domain: 595 (lowest/common) .. 1000 (highest/mythic)
const HP_MIN = 595;
const HP_MAX = 1000;

function hpToTierLabel(hp) {
  const n = Math.max(0, Math.min(1, (hp - HP_MIN) / (HP_MAX - HP_MIN))); // clamp 0..1
  if (n >= 0.95) return 'Mythic';
  if (n >= 0.75) return 'Legendary';
  if (n >= 0.50) return 'Rare';
  if (n >= 0.25) return 'Uncommon';
  return 'Common';
}

// ===== COLORS / THEME (unchanged except header shows HP) =====
const PALETTE = {
  cardBg: '#242623',
  frameFill: '#b9dded',
  frameStroke: '#CFE3FF',
  headerText: '#0F172A',
  rarityStripeByTier: {
    Mythic:   '#fadf6a',
    Legendary:'#b5a6e9',
    Rare:     '#f2d2ea',
    Uncommon: '#a6fbba',
    Common:   '#929394',
  },
  artBackfill: '#b9dded',
  artStroke:   '#F9FAFB',
  traitsPanelBg:     '#b9dded',
  traitsPanelStroke: '#000000',
  traitCardFill:   '#FFFFFF',
  traitCardStroke: '#b9dded',
  traitCardShadow: '#0000001A',
  // (fill is overridden per-rarity at draw time)
  traitHeaderFill:   '#b9dded',
  traitHeaderStroke: '#b9dded',
  traitTitleText: '#222625',
  traitValueText: '#775fbb',
  footerText: '#212524',
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

// ====== HP SCORE TABLE (your list) + helpers ======
const HP_TABLE = {
  Legend: {
    "Night": 1000, "Purple Yeti": 1000, "Beige Giant Ears": 1000,
    "Pikachugly": 1000, "Cornhuglyio": 1000,
  },
  Type: { "Elf Squigs": 60, "Squigs": 36 },
  Background: {
    "Dark Blue Galaxy": 120, "Grey Boom": 118, "Purple Splash": 116, "Green Boom": 114,
    "Grey Splash": 112, "Yellow Boom": 110, "Yellow Splash": 108, "Dark Blue Boom": 106,
    "Light Blue Boom": 104, "Purple Boom": 102, "Green Splash": 100, "Light Blue Splash": 98,
    "Dark Blue Splash": 96, "Blue Splash": 94, "Blue Bloom": 92, "Purple": 90,
    "Light Blue": 88, "Blue": 86, "Green": 84, "Grey": 82, "Dark Blue": 80, "Yellow": 72,
  },
  Body: {
    "Airplane Life Jacket": 200, "Blue Sexy Bowling Shirt": 199, "Mario Overalls": 197,
    "Butthead Tee": 195, "Futuristic Armor": 193, "White and Green Jacket": 191,
    "Cowboy Jacket": 190, "Beavis Tee": 188, "Sexy Bowling Shirt": 186, "Super Ugly": 184,
    "Yellow Jersey": 182, "Rick Laser": 181, "UGS Jacket": 179, "Ugly Side of the Moon": 177,
    "Ugly Food Shirt": 175, "Acupuncture": 173, "Prison Tee": 172, "Cheerleader": 170,
    "Caveman": 168, "Red Cowboy": 166, "Astronaut": 164, "Blue Cowboy": 162, "Red Varsity": 161,
    "Indian": 159, "Blue Wetsuit": 157, "Suit": 155, "Maple Leafs 1967 Tracksuit": 153,
    "Astronaut Blue": 152, "Maple Leafs 1967": 150, "Ugly Scene": 148, "Ninja": 146,
    "White and Blue Jacket": 144, "Hawaiian Shirt": 143, "Basketball Jersey": 141,
    "Purple Hoodie": 139, "Beige Fisherman Jacket": 137, "Pet Lover Bag": 135,
    "PAAF White Tee": 134, "Blue Baseball Jersey": 132, "Monster Tee Bag": 130,
    "Born Ugly Tee Tank": 128, "Tattooed Body": 126, "Black Puffy": 125, "Flame Tee Tank": 123,
    "Camo Wetsuit": 121, "Grey Fisherman Jacket": 120, "Ugly Army Tank": 120, "Borat": 120,
    "Sports Bra": 120, "Green Varsity": 120, "Beige Jacket": 120, "Holey Tee": 120,
    "Yellow Tracksuit": 120, "Long Sleeve Flame Tee": 120, "Bowling Shirt": 120,
    "Grey Bike Jersey": 120, "Light Green tee": 120, "White Ugly Tank": 120, "420Purple Tracksuit": 120,
    "Ugly Tank on Pink": 120, "Red BAseball Shirt": 120, "Purple Shirt": 120, "Gardener Overalls": 120,
    "Grease Tank": 120, "Yellow Puffy": 120, "Pink Jacket": 120, "Blue Overalls": 120,
    "Green Sweater": 120, "Green Tee": 120, "Born Ugly Tee": 120, "Naked": 120, "Purple Tee": 120,
    "Black Sweater": 120, "White Tee": 120,
  },
  Eyes: {
    "Bionic": 100, "Sleepy Trio": 96, "Angry Cyclops": 92, "Angry Cyclops Lashes": 88,
    "Angry Trio": 84, "Angry Trio Lashes": 80, "Bionic Lashes": 76, "Cyclops": 72,
    "Cyclops Lashes": 68, "Trio Lashes": 64, "Trio": 60,
  },
  Head: {
    "Buoy": 300, "Headphones": 299, "Cape": 298, "Captured Piranha": 296, "Butthead Hair": 295,
    "Sheriff": 294, "Paper Bag": 292, "Elf Human Air": 291, "Beavis Hair": 290, "Crown": 288,
    "Hood": 287, "VE Headset": 286, "Tyre": 284, "Hairball Z": 283, "Ski Mask": 282, "Lobster": 280,
    "Human Air": 279, "Halo": 278, "Ice Cream": 276, "Pot Head": 275, "Slasher": 274,
    "Acupuncture": 272, "Flower Power": 271, "Basketball": 270, "HeliHat": 268, "Watermelon": 267,
    "Visor with Hair": 266, "Indian": 264, "Purr Paw": 263, "Long Hair": 262, "Green Mountie": 260,
    "Trucker": 259, "Baseball": 258, "Imposter Mask": 256, "Blindfold": 255, "Boom Bucket": 254,
    "Green Visor": 252, "Space Brain": 251, "Diving Mask": 250, "Captain": 248, "Dread Cap": 247,
    "Zeus Hand": 246, "Elf Hood": 244, "Pirate": 243, "Panda Bike Helmet": 242, "Golfs": 240,
    "Rainbow": 239, "Night Vision": 238, "UGS Delivery": 236, "3D": 235, "Dinomite": 234,
    "Fire": 232, "Blonde Fro Comb": 231, "Head Canoe": 230, "Umbrella Hat": 228, "Cowboy": 227,
    "Bunny": 226, "Proud to be Ugly": 224, "Black Ugly": 223, "Knife": 222, "Green Cap": 220,
    "Bald": 219, "Cactus": 218, "I Need TP": 216, "Rastalocks": 215, "Beer": 214, "Fro Comb": 212,
    "Mountie": 211, "Pomade": 210, "Yellow Beanie": 208, "Floral": 207, "90’s Pink": 206,
    "Lemon Bucket": 204, "Pink Beanie": 203, "Grey Cap": 202, "Honey Pot": 200, "Afro": 199,
    "Bear Fur Hat": 198, "Bandana": 196, "Rice Dome": 195, "Parted": 194, "Green Ugly": 192,
    "Brain Bucket": 191, "Twintails": 190, "Notlocks": 188, "Tip Topper": 187, "Cube Cut": 186,
    "Purple Punk": 184, "90’s Blonde": 183, "Fiesta": 182, "Green Beanie": 180, "Yellow twintails": 180,
    "Blond Punk": 180,
  },
  Skin: {
    "Cristal": 150, "Cristal Elf": 147, "Purple Elf Space": 144, "Green Camo": 141, "Purple Space": 138,
    "Green Elf Camo": 135, "Green Elf": 132, "Orange": 129, "Orange Elf": 126, "Purple Elf": 123,
    "Purple": 120, "Green": 117, "Beige Elf": 114, "Pink": 111, "Dark Brown Elf": 108, "Beige": 105,
    "Brown": 102, "Pink Elf": 99, "Brown Elf": 96, "Dark Brown": 90,
  },
  Special: {
    "Laser Tits": 70, "Parakeet": 67, "Yellow Laser": 64, "Ugly Necklace": 61, "Weed Necklace": 58,
    "Dino": 55, "Nouns Glasses": 52, "Green Laser": 49, "Sad Smile Necklace": 46, "Smile Necklace": 43,
    "Monocle": 40, "Pirhana": 37, "None (Ignore)": 18,
  },
};

function hpFor(cat, val) {
  const group = HP_TABLE[cat];
  if (!group) return 0;
  const key = Object.keys(group).find(k => k.toLowerCase() === String(val).trim().toLowerCase());
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
    if (!groups.hasOwnProperty(type)) continue;
    groups[type].push({ value: valStr });
  }
  return groups;
}

// Back-compat alias
function rarityColorFromLabel(label) { return stripeFromRarity(label); }

// ====== RENDERER (uses HP in header + per-trait HP) ======
async function renderSquigCard({ name, tokenId, imageUrl, traits, rankInfo, rarityLabel, headerStripe }) {
  const W = 750, H = 1050;
  const canvas = createCanvas(W, H);
  const ctx = canvas.getContext('2d');

  // BG
  ctx.fillStyle = PALETTE.cardBg; ctx.fillRect(0, 0, W, H);

  // Frame
  drawRoundRect(ctx, 24, 24, W - 48, H - 48, 28, PALETTE.frameFill);
  ctx.strokeStyle = PALETTE.frameStroke; ctx.lineWidth = 2; ctx.stroke();

  // Header stripe
  // Prefer explicit headerStripe; otherwise derive from rarityLabel (still supported)
  const headerStripeFill = headerStripe || stripeFromRarity(rarityLabel || 'Common');
  drawRoundRectShadow(ctx, 48, 52, W - 96, 84, 18, headerStripeFill);
  ctx.fillStyle = PALETTE.headerText;
  ctx.textBaseline = 'middle';
  ctx.font = `36px ${FONT_BOLD}`;
  ctx.fillText(name, 64, 94);

  // Header right = Total HP
  const rightText = `${rankInfo?.hpTotal ?? 0} HP`;
  ctx.font = `28px ${FONT_BOLD}`;
  const tw = ctx.measureText(rightText).width;
  ctx.fillText(rightText, W - 64 - tw, 94);

  // Art window
  const AW = 420, AH = 420;
  const AX = Math.round((W - AW) / 2), AY = 160;
  roundRectPath(ctx, AX, AY, AW, AH, 22);
  ctx.save(); ctx.clip();
  drawRoundRect(ctx, AX, AY, AW, AH, 22, PALETTE.artBackfill);
  try {
    const img = await loadImage(await fetchBuffer(imageUrl));
    const { dx, dy, dw, dh } = cover(img.width, img.height, AW, AH);
    ctx.drawImage(img, AX + dx, AY + dy, dw, dh);
  } catch {}
  ctx.restore();
  ctx.strokeStyle = PALETTE.artStroke; ctx.lineWidth = 2; roundRectPath(ctx, AX, AY, AW, AH, 22); ctx.stroke();

  // Traits panel
  const TX = 60, TY = AY + AH + 20, TW = W - 120, TH = H - TY - 92;
  drawRoundRect(ctx, TX, TY, TW, TH, 16, PALETTE.traitsPanelBg);
  ctx.strokeStyle = PALETTE.traitsPanelStroke; ctx.lineWidth = 2; ctx.stroke();

  // Layout (2 cols)
  const PAD = 12, innerX = TX + PAD, innerY = TY + PAD, innerW = TW - PAD * 2, innerH = TH - PAD * 2;
  const COL_GAP = 12, COL_W = (innerW - COL_GAP) / 2;

  function layout(lineH, titleH, blockPad) {
    const boxes = [];
    for (const cat of TRAIT_ORDER) {
      const items = (traits[cat] || []);
      if (!items.length) continue;

      const lines = items.map(t => `• ${String(t.value)} (${hpFor(cat, t.value)} HP)`);
      const maxLines = 5;
      const shown = lines.slice(0, maxLines);
      const hidden = lines.length - shown.length;
      if (hidden > 0) shown.push(`+${hidden} more`);

      const rowsH = shown.length * lineH;
      const minRows = 34;
      const boxH = blockPad + titleH + Math.max(rowsH + 10, minRows) + blockPad;

      boxes.push({ cat, lines: shown, boxH, lineH, titleH, blockPad });
    }

    // Masonry two columns
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
    return { placed, usedH, lineH, titleH, blockPad };
  }

  let L = layout(16, 28, 8);
  if (L.usedH > (TH - 24)) {
    const scale = Math.max(0.75, (TH - 24) / L.usedH);
    L = layout(Math.max(12, Math.floor(16 * scale)), Math.max(24, Math.floor(28 * scale)), 6);
  }

  // Draw mini-cards
  for (const b of L.placed) {
    drawRoundRectShadow(ctx, b.x, b.y, b.w, b.boxH, 12, PALETTE.traitCardFill, PALETTE.traitCardStroke, PALETTE.traitCardShadow, 10, 2);
    const traitHeaderFill = headerStripeFill; // match rarity/HP color
    drawRoundRect(ctx, b.x, b.y, b.w, b.titleH, 12, traitHeaderFill);
    ctx.strokeStyle = PALETTE.traitHeaderStroke; ctx.lineWidth = 1.5; ctx.stroke();

    ctx.fillStyle = PALETTE.traitTitleText;
    ctx.font = `19px ${FONT_BOLD}`;
    ctx.textBaseline = 'alphabetic';
    ctx.fillText(b.cat, b.x + 12, b.y + Math.min(22, b.titleH - 8));

    const rowsY = b.y + b.titleH;
    drawRect(ctx, b.x, rowsY, b.w, b.boxH - b.titleH, PALETTE.traitCardFill);

    const avail = (b.boxH - b.titleH);
    const rowsH = b.lines.length * b.lineH;
    let yy = rowsY + Math.max(8, Math.floor((avail - rowsH) / 2) + 1);

    ctx.fillStyle = PALETTE.traitValueText;
    ctx.font = `15px ${FONT_REG}`;
    ctx.textBaseline = 'middle';
    for (const line of b.lines) {
      ctx.fillText(line, b.x + 12, yy + b.lineH / 2);
      yy += b.lineH;
    }
  }

   // Footer (left: token, right: class/tier)
  ctx.fillStyle = PALETTE.footerText;
  ctx.font = `18px ${FONT_REG}`;
  ctx.textBaseline = 'alphabetic';

  const footerY = H - 34;

  // left text
  ctx.fillText(`Squigs • Token #${tokenId}`, 60, footerY);

  // right text = class/tier (from rarityLabel or HP)
  const tierLabelFooter =
    (rarityLabel && String(rarityLabel)) ||
    hpToTierLabel(rankInfo?.hpTotal || 0);

  const classText = String(tierLabelFooter);
  const classW = ctx.measureText(classText).width;
  ctx.fillText(classText, W - 60 - classW, footerY);

  return canvas.toBuffer('image/jpeg', { quality: 0.95 });
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
// ===== MINT CHECK (Alchemy owners endpoint + tiny cache) =====
const MINT_CACHE = new Map();

const NOT_MINTED_MESSAGES = [
  (id) => `👀 Squig #${id} hasn’t crawled out of the mint swamp yet.\nGo hatch one at **https://squigs.io**`,
  (id) => `🫥 Squig #${id} is still a rumor. Mint your destiny at **https://squigs.io**`,
  (id) => `🌀 Squig #${id} is hiding in the spiral dimension. The portal is **https://squigs.io**`,
  (id) => `🥚 Squig #${id} is still an egg. Crack it open at **https://squigs.io**`,
  (id) => `🤫 The Squigs whisper: “#${id}? Not minted.” Try **https://squigs.io**`
];

function notMintedLine(tokenId) {
  const pick = NOT_MINTED_MESSAGES[Math.floor(Math.random() * NOT_MINTED_MESSAGES.length)];
  return pick(tokenId);
}

/**
 * Returns true if the token has at least one owner (i.e., minted).
 * Uses Alchemy v3: getOwnersForNFT
 */
async function isSquigMinted(tokenId) {
  if (MINT_CACHE.has(tokenId)) return MINT_CACHE.get(tokenId);
  try {
    const url =
      `https://eth-mainnet.g.alchemy.com/nft/v3/${ALCHEMY_API_KEY}/getOwnersForNFT` +
      `?contractAddress=${SQUIGS_CONTRACT}&tokenId=${tokenId}`;
    const res = await fetchWithRetry(url, 2, 600);
    const data = await res.json();
    const owners =
      (Array.isArray(data?.owners) && data.owners) ||
      (Array.isArray(data?.ownerAddresses) && data.ownerAddresses) ||
      [];
    const minted = owners.length > 0;
    MINT_CACHE.set(tokenId, minted);
    return minted;
  } catch (e) {
    // If the check fails (network hiccup), don't hard-block:
    console.warn(`⚠️ Mint check error for #${tokenId}:`, e.message);
    return true;
  }
}
