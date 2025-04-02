const { Client, GatewayIntentBits } = require('discord.js');
const fetch = require('node-fetch');
const fs = require('fs');

// Load secrets from environment
const DISCORD_TOKEN = process.env.DISCORD_BOT_TOKEN;
const ETHERSCAN_API_KEY = process.env.ETHERSCAN_API_KEY;

// Contract for Charm of the Ugly
const COLLECTION_CONTRACT = '0x9492505633d74451bdf3079c09ccc979588bc309';

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent
  ]
});

// Load wallet links
let walletLinks = {};
if (fs.existsSync('walletLinks.json')) {
  walletLinks = JSON.parse(fs.readFileSync('walletLinks.json'));
}

client.on('ready', () => {
  console.log(`✅ Logged in as ${client.user.tag}`);
});

client.on('messageCreate', async (message) => {
  if (!message.content.startsWith('!') || message.author.bot) return;

  const args = message.content.slice(1).trim().split(/ +/);
  const command = args.shift().toLowerCase();

  // !linkwallet command
  if (command === 'linkwallet') {
    const address = args[0];
    if (!/^0x[a-fA-F0-9]{40}$/.test(address)) {
      return message.reply('❌ Please enter a valid Ethereum wallet address.');
    }
    walletLinks[message.author.id] = address;
    fs.writeFileSync('walletLinks.json', JSON.stringify(walletLinks, null, 2));
    return message.reply(`✅ Wallet linked: ${address}`);
  }

  // !ugly command - show random NFT
  if (command === 'ugly') {
    const wallet = walletLinks[message.author.id];
    if (!wallet) {
      return message.reply('❌ Please link your wallet first using `!linkwallet 0x...`');
    }

    const url = `https://api.etherscan.io/api?module=account&action=tokennfttx&address=${wallet}&contractaddress=${COLLECTION_CONTRACT}&page=1&offset=100&sort=asc&apikey=${ETHERSCAN_API_KEY}`;

    try {
      const res = await fetch(url);
      const data = await res.json();

      const owned = new Set();
      for (const tx of data.result) {
        if (tx.to.toLowerCase() === wallet.toLowerCase()) {
          owned.add(tx.tokenID);
        } else if (tx.from.toLowerCase() === wallet.toLowerCase()) {
          owned.delete(tx.tokenID);
        }
      }

      if (owned.size === 0) return message.reply('😢 You don’t own any Charm of the Ugly NFTs.');

      const tokenArray = Array.from(owned);
      const randomToken = tokenArray[Math.floor(Math.random() * tokenArray.length)];
      const imgUrl = `https://ipfs.io/ipfs/bafybeie5o7afc4yxyv3xx4jhfjzqugjwl25wuauwn3554jrp26mlcmprhe/${randomToken}`;

      return message.reply({
        content: `🎨 Here's one of your **Charm of the Ugly** NFTs!\n**Token ID**: ${randomToken}`,
        files: [{
          attachment: imgUrl,
          name: `ugly-${randomToken}.jpg`
        }]
      });

    } catch (err) {
      console.error(err);
      return message.reply('⚠️ Error fetching your NFTs. Please try again later.');
    }
  }

  // !myuglys command - show all owned NFTs (up to 10)
  if (command === 'myuglys') {
    const wallet = walletLinks[message.author.id];
    if (!wallet) {
      return message.reply('❌ Please link your wallet first using `!linkwallet 0x...`');
    }

    const url = `https://api.etherscan.io/api?module=account&action=tokennfttx&address=${wallet}&contractaddress=${COLLECTION_CONTRACT}&page=1&offset=100&sort=asc&apikey=${ETHERSCAN_API_KEY}`;

    try {
      const res = await fetch(url);
      const data = await res.json();

      const owned = new Set();
      for (const tx of data.result) {
        if (tx.to.toLowerCase() === wallet.toLowerCase()) {
          owned.add(tx.tokenID);
        } else if (tx.from.toLowerCase() === wallet.toLowerCase()) {
          owned.delete(tx.tokenID);
        }
      }

      const tokenArray = Array.from(owned);
      if (tokenArray.length === 0) {
        return message.reply('😢 You don’t currently own any Charm of the Ugly NFTs.');
      }

      const count = tokenArray.length;
      const limitedTokens = tokenArray.slice(0, 10); // Discord file limit
      const files = limitedTokens.map((tokenId) => ({
        attachment: `https://ipfs.io/ipfs/bafybeie5o7afc4yxyv3xx4jhfjzqugjwl25wuauwn3554jrp26mlcmprhe/${tokenId}`,
        name: `ugly-${tokenId}.jpg`
      }));

      const listedIds = limitedTokens.map(id => `#${id}`).join(', ');

      let replyText = `🧟‍♂️ You own **${count} Charm of the Ugly NFTs**!\nHere are the first ${limitedTokens.length}: ${listedIds}`;
      if (count > 10) replyText += `\n_(Only showing first 10 images)_`;

      return message.reply({ content: replyText, files });

    } catch (err) {
      console.error(err);
      return message.reply('⚠️ Error fetching your NFTs. Please try again later.');
    }
  }
});

client.login(DISCORD_TOKEN);
