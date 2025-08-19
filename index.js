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
let FONT_FAMILY_REGULAR = 'Inter';
let FONT_FAMILY_BOLD = 'Inter';
const { GlobalFonts } = require('@napi-rs/canvas');

async function ensureFonts() {
  const files = [
    {
      url: 'https://github.com/rsms/inter/releases/download/v4.1/Inter-Regular.ttf',
      path: 'fonts/Inter-Regular.ttf', name: 'Inter-Regular'
    },
    {
      url: 'https://github.com/rsms/inter/releases/download/v4.1/Inter-Bold.ttf',
      path: 'fonts/Inter-Bold.ttf', name: 'Inter-Bold'
    }
  ];
  fs.mkdirSync('fonts', { recursive: true });
  for (const f of files) {
    if (!fs.existsSync(f.path)) {
      const r = await fetch(f.url);
      if (!r.ok) throw new Error(`Font download failed: ${r.status}`);
      fs.writeFileSync(f.path, Buffer.from(await r.arrayBuffer()));
    }
    try { GlobalFonts.registerFromPath(f.path, f.name); } catch {}
  }
  console.log('🖋 Fonts ready:', files.map(f => f.name).join(', '));
}

// call it immediately (don’t await; it runs before first /card usage)
ensureFonts().catch(e => {
  console.warn('⚠️ Could not ensure fonts:', e.message);
  FONT_FAMILY_REGULAR = 'sans-serif';
  FONT_FAMILY_BOLD = 'sans-serif';
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
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent]
});

// ===== UTILS =====
const fetchWithRetry = async (url, retries = 3, delay = 1000) => {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const res = await fetch(url, { timeout: 7000 });
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

// ===== Slash command registrar (guild-scoped) =====
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
        .setDescription(`Create a Squigs trading card JPEG • ${new Date().toISOString()}`) // force refresh
        .addIntegerOption(o => o.setName('token_id').setDescription('Squig token ID').setRequired(true))
        .addStringOption(o => o.setName('name').setDescription('Optional display name'))
        .toJSON()
    ];

    const route = Routes.applicationGuildCommands(DISCORD_CLIENT_ID, GUILD_ID);
    const putRes = await rest.put(route, { body: commands });
    console.log(`✅ Registered ${putRes.length} guild slash command(s) to ${GUILD_ID}.`);
    const listRes = await rest.get(route);
    console.log('🔎 Guild commands now:', listRes.map(c => `${c.name} (${c.id})`).join(', '));
  } catch (e) {
    console.error('❌ Slash register error:', e?.rawError ?? e?.data ?? e);
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
    const rewards = [100, 100, 100, 200];
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

// ===== PREFIX COMMANDS (your existing ones) =====
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

      const itemsPerPage = 5; let page = 0;
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
        if (interaction.user.id !== message.author.id) return interaction.reply({ content: '❌ Only the original user can use these buttons.', ephemeral: true });
        if (interaction.customId === 'prev') { page = page > 0 ? page - 1 : totalPages - 1; await interaction.update({ embeds: generateEmbeds(page) }); }
        if (interaction.customId === 'next') { page = page < totalPages - 1 ? page + 1 : 0; await interaction.update({ embeds: generateEmbeds(page) }); }
        if (interaction.customId === 'stop') { collector.stop(); await interaction.update({ components: [] }); }
      });

      collector.on('end', async () => { try { await messageReply.edit({ components: [] }); } catch (e) {} });
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

      const itemsPerPage = 5; let page = 0;
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
        if (interaction.user.id !== message.author.id) return interaction.reply({ content: '❌ Only the original user can use these buttons.', ephemeral: true });
        if (interaction.customId === 'prev') { page = page > 0 ? page - 1 : totalPages - 1; await interaction.update({ embeds: generateEmbeds(page) }); }
        if (interaction.customId === 'next') { page = page < totalPages - 1 ? page + 1 : 0; await interaction.update({ embeds: generateEmbeds(page) }); }
        if (interaction.customId === 'stop') { collector.stop(); await interaction.update({ components: [] }); }
      });

      collector.on('end', async () => { try { await messageReply.edit({ components: [] }); } catch (e) {} });
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

      const itemsPerPage = 5; let page = 0;
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
        if (interaction.user.id !== message.author.id) return interaction.reply({ content: '❌ Only the original user can use these buttons.', ephemeral: true });
        if (interaction.customId === 'prev') { page = page > 0 ? page - 1 : totalPages - 1; await interaction.update({ embeds: generateEmbeds(page) }); }
        if (interaction.customId === 'next') { page = page < totalPages - 1 ? page + 1 : 0; await interaction.update({ embeds: generateEmbeds(page) }); }
        if (interaction.customId === 'stop') { collector.stop(); await interaction.update({ components: [] }); }
      });

      collector.on('end', async () => { try { await messageReply.edit({ components: [] }); } catch (e) {} });

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

    // Traits (Alchemy → on-chain fallback)
    const { nameFromMeta, traits } = await getTraitsForSquig(tokenId);
    console.log('Traits debug:', { tokenId, count: traits.length, sample: traits.slice(0, 3) });

    const displayName = customName || nameFromMeta || `Squig #${tokenId}`;
    const traitCounts = loadTraitCountsSafe();
    const cardTraits = buildCardTraits(traits, traitCounts);

    // RARITY: try OpenSea rank, else heuristic
    const os = await getOpenSeaRarityRank(tokenId);
    let rarityText, tier;
    if (os?.rank) { rarityText = `Rank #${os.rank}`; tier = tierFromRank(os.rank); }
    else { const h = simpleRarityLabel(traits); rarityText = h; tier = h; }

    const buffer = await renderSquigCard({
      name: displayName,
      rarityText,
      tier,
      tokenId,
      imageUrl: `https://assets.bueno.art/images/a49527dc-149c-4cbc-9038-d4b0d1dbf0b2/default/${tokenId}`,
      cardTraits
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

// ===== Helpers (metadata, traits, rarity, canvas) =====
async function getTraitsForSquig(tokenId) {
  // 1) Try Alchemy REST
  try {
    const meta = await getNftMetadataAlchemy(tokenId);
    const traits = extractTraits(meta);
    const nameFromMeta = getNameFromMeta(meta);
    if (traits.length) return { nameFromMeta, traits };
  } catch (e) {
    console.warn('Alchemy getNFTMetadata failed, will try on-chain:', e.message);
  }
  // 2) On-chain fallback: tokenURI → IPFS JSON
  try {
    const provider = new ethers.JsonRpcProvider(`https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_API_KEY}`);
    const abi = ["function tokenURI(uint256) view returns (string)"];
    const contract = new ethers.Contract(SQUIGS_CONTRACT, abi, provider);
    const uri = await contract.tokenURI(tokenId);
    const http = ipfsToHttp(uri);
    const res = await fetch(http);
    if (!res.ok) throw new Error(`tokenURI fetch ${res.status}`);
    const json = await res.json();
    const traits = Array.isArray(json?.attributes) ? json.attributes : [];
    const nameFromMeta = json?.name || null;
    return { nameFromMeta, traits };
  } catch (e) {
    console.error('On-chain metadata fallback failed:', e.message);
    return { nameFromMeta: null, traits: [] };
  }
}

async function getNftMetadataAlchemy(tokenId) {
  const url = `https://eth-mainnet.g.alchemy.com/nft/v3/${ALCHEMY_API_KEY}/getNFTMetadata` +
              `?contractAddress=${SQUIGS_CONTRACT}&tokenId=${tokenId}&refreshCache=false`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Alchemy metadata error: ${res.status}`);
  return res.json();
}

function ipfsToHttp(uri) {
  if (!uri) return uri;
  return uri.replace(/^ipfs:\/\//, 'https://ipfs.io/ipfs/').replace('ipfs/ipfs/', 'ipfs/');
}

function extractTraits(meta) {
  if (Array.isArray(meta?.metadata?.attributes)) return meta.metadata.attributes;
  let raw = meta?.raw?.metadata;
  if (typeof raw === 'string') {
    try { raw = JSON.parse(raw); } catch { raw = null; }
  }
  if (Array.isArray(raw?.attributes)) return raw.attributes;
  const os = meta?.openSea || meta?.open_sea;
  if (os && os.traits && typeof os.traits === 'object') {
    const arr = [];
    for (const [trait_type, value] of Object.entries(os.traits)) arr.push({ trait_type, value });
    if (arr.length) return arr;
  }
  if (Array.isArray(meta?.attributes)) return meta.attributes;
  return [];
}

function getNameFromMeta(meta) {
  if (meta?.metadata && typeof meta.metadata === 'object' && meta.metadata.name) return meta.metadata.name;
  let raw = meta?.raw?.metadata;
  if (typeof raw === 'string') { try { raw = JSON.parse(raw); } catch { raw = null; } }
  if (raw && typeof raw === 'object' && raw.name) return raw.name;
  return null;
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

async function getOpenSeaRarityRank(tokenId) {
  if (!OPENSEA_API_KEY) return null;
  const endpoints = [
    `https://api.opensea.io/v2/chain/ethereum/contract/${SQUIGS_CONTRACT}/nfts/${tokenId}`,
    `https://api.opensea.io/api/v2/chain/ethereum/contract/${SQUIGS_CONTRACT}/nfts/${tokenId}`
  ];
  for (const url of endpoints) {
    try {
      const res = await fetch(url, { headers: { 'X-API-KEY': OPENSEA_API_KEY } });
      if (!res.ok) continue;
      const j = await res.json();
      const rankAny = j?.nft?.rarity?.rank ?? j?.rarity?.rank ?? j?.nft?.rarity_rank ?? j?.rarity_rank;
      const rank = typeof rankAny === 'number' ? rankAny : parseInt(rankAny, 10);
      if (!Number.isNaN(rank)) return { rank };
    } catch {}
  }
  return null;
}

function tierFromRank(rank) {
  if (rank <= 100) return 'Mythic';
  if (rank <= 500) return 'Legendary';
  if (rank <= 1500) return 'Rare';
  if (rank <= 3000) return 'Uncommon';
  return 'Common';
}

const CANON_ORDER = ['Background', 'Body', 'Eyes', 'Head', 'Legend', 'Skin', 'Special', 'Type'];
const TRAIT_ALIASES = {
  background: 'Background', bg: 'Background', backdrop: 'Background',
  body: 'Body',
  eyes: 'Eyes', eye: 'Eyes',
  head: 'Head', hat: 'Head', headwear: 'Head',
  legend: 'Legend',
  skin: 'Skin',
  special: 'Special', accessory: 'Special',
  type: 'Type', class: 'Type'
};

function canonicalizeTraits(rawTraits) {
  const map = {};
  (rawTraits || []).forEach(t => {
    const key = String(t?.trait_type ?? '').trim().toLowerCase();
    const canon = TRAIT_ALIASES[key];
    if (!canon) return;
    if (map[canon]) return; // first one wins
    map[canon] = (t?.value ?? '').toString();
  });
  return map;
}

function buildCardTraits(rawTraits, traitCounts) {
  const map = canonicalizeTraits(rawTraits);
  const out = [];
  for (const label of CANON_ORDER) {
    const value = map[label];
    if (!value) continue;
    const count = traitCounts?.[label]?.[value];
    out.push({ label, value, count: (typeof count === 'number' ? count : null) });
  }
  return out;
}

// ===== Card renderer =====
function headerColorForTier(tier) {
  switch (tier) {
    case 'Mythic':    return '#FF6B6B'; // red
    case 'Legendary': return '#E6B325'; // gold
    case 'Rare':      return '#6C5CE7'; // purple
    case 'Uncommon':  return '#2ECC71'; // green
    case 'Common':
    default:          return '#F2D95C'; // yellow
  }
}

async function renderSquigCard({ name, rarityText, tier, tokenId, imageUrl, cardTraits }) {
  const W = 750, H = 1050;
  const canvas = createCanvas(W, H);
  const ctx = canvas.getContext('2d');

  // Background
  const g = ctx.createLinearGradient(0, 0, 0, H);
  g.addColorStop(0, '#f6f6ff'); g.addColorStop(1, '#e9ecff');
  ctx.fillStyle = g; ctx.fillRect(0, 0, W, H);

  // Frame
  drawRoundRect(ctx, 24, 24, W - 48, H - 48, 28, '#ffffff');

  // Header (colored by tier)
  const headerColor = headerColorForTier(tier);
  drawRoundRect(ctx, 48, 52, W - 96, 80, 18, headerColor);

  // Title + rarity text
  ctx.fillStyle = '#1c1c1c';
  ctx.textBaseline = 'middle';
  ctx.font = `bold 32px ${FONT_FAMILY_BOLD}`;
  ctx.fillText(name, 64, 92);

  ctx.font = `bold 28px ${FONT_FAMILY_BOLD}`;
  const label = rarityText || '—';
  const tw = ctx.measureText(label).width;
  ctx.fillText(label, W - 64 - tw, 92);

  // Art window
  const AX = 60, AY = 150, AW = W - 120, AH = 540;
  drawRoundRect(ctx, AX, AY, AW, AH, 16, '#fafafa', '#e3e3e3');
  try {
    const img = await loadImage(await fetchBuffer(imageUrl));
    const { dx, dy, dw, dh } = contain(img.width, img.height, AW - 16, AH - 16);
    ctx.drawImage(img, AX + 8 + dx, AY + 8 + dy, dw, dh);
  } catch {
    ctx.fillStyle = '#bbb';
    ctx.font = `26px ${FONT_FAMILY_REGULAR}`;
    ctx.fillText('Image not available', AX + 20, AY + AH / 2);
  }

  // Traits panel
  const TX = 60, TY = AY + AH + 30, TW = W - 120, TH = H - TY - 60;
  drawRoundRect(ctx, TX, TY, TW, TH, 16, '#ffffff', '#e3e3e3');

  ctx.fillStyle = '#333';
  ctx.font = `bold 26px ${FONT_FAMILY_BOLD}`;
  ctx.textAlign = 'left'; ctx.textBaseline = 'alphabetic';
  ctx.fillText('Traits', TX + 16, TY + 36);

  // divider
  ctx.strokeStyle = '#E9E9EF'; ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(TX + 12, TY + 44); ctx.lineTo(TX + TW - 12, TY + 44); ctx.stroke();

  const rowH = 38;
  let y = TY + 80;

  if (!cardTraits || cardTraits.length === 0) {
    ctx.font = `20px ${FONT_FAMILY_REGULAR}`;
    ctx.fillStyle = '#8a8a8a';
    ctx.fillText('No categorized traits found for this token.', TX + 16, y);
  } else {
    (cardTraits || []).forEach(({ label, value, count }) => {
      ctx.font = `bold 22px ${FONT_FAMILY_BOLD}`;
      ctx.fillStyle = '#3a3a3a';
      ctx.fillText(label + ':', TX + 16, y);

      ctx.font = `22px ${FONT_FAMILY_REGULAR}`;
      ctx.fillStyle = '#141414';
      const valueText = String(value);
      ctx.fillText(valueText, TX + 160, y);

      if (typeof count === 'number') {
        ctx.font = `20px ${FONT_FAMILY_REGULAR}`;
        ctx.fillStyle = '#6a6a6a';
        const w = ctx.measureText(valueText).width;
        ctx.fillText(`— ${count} in collection`, TX + 160 + w + 12, y);
      }
      y += rowH;
    });
  }

  // footer inside panel
  ctx.font = `18px ${FONT_FAMILY_REGULAR}`;
  ctx.fillStyle = '#666';
  ctx.textBaseline = 'alphabetic';
  ctx.fillText(`Squigs • Token #${tokenId}`, TX + 16, TY + TH - 16);

  return canvas.toBuffer('image/jpeg', { quality: 0.95 });
}

// ========== Drawing helpers ==========
function drawRoundRect(ctx, x, y, w, h, r, fill, stroke) {
  const rr = Math.min(r, w / 2, h / 2);
  ctx.beginPath();
  ctx.moveTo(x + rr, y);
  ctx.arcTo(x + w, y, x + w, y + h, rr);
  ctx.arcTo(x + w, y + h, x, y + h, rr);
  ctx.arcTo(x, y + h, x, y, rr);
  ctx.arcTo(x, y, x + w, y, rr);
  ctx.closePath();
  if (fill) { ctx.fillStyle = fill; ctx.fill(); }
  if (stroke) { ctx.strokeStyle = stroke; ctx.stroke(); }
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
