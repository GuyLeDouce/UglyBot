const { Client, GatewayIntentBits } = require('discord.js');
const fetch = require('node-fetch');
const fs = require('fs');

const DISCORD_TOKEN = process.env.DISCORD_BOT_TOKEN;
const ETHERSCAN_API_KEY = process.env.ETHERSCAN_API_KEY;

// NFT Contracts
const UGLY_CONTRACT = '0x9492505633d74451bdf3079c09ccc979588bc309';
const MONSTER_CONTRACT = '0x1cD7fe72D64f6159775643ACEdc7D860dFB80348';

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent
  ]
});

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

// !linkwallet
if (command === 'linkwallet') {
  const address = args[0];
  if (!/^0x[a-fA-F0-9]{40}$/.test(address)) {
    return message.reply('❌ Please enter a valid Ethereum wallet address.');
  }

  walletLinks[message.author.id] = address;
  fs.writeFileSync('walletLinks.json', JSON.stringify(walletLinks, null, 2));

  try {
    await message.delete(); // Delete the original message to protect wallet privacy
  } catch (err) {
    console.warn(`⚠️ Could not delete message from ${message.author.tag}:`, err.message);
  }

  return message.channel.send({
    content: '✅ Wallet linked.',
    allowedMentions: { repliedUser: false }
  });
}


  // !ugly
  if (command === 'ugly') {
    const wallet = walletLinks[message.author.id];
    if (!wallet) {
      return message.reply('❌ Please link your wallet first using `!linkwallet 0x...`');
    }

    const url = `https://api.etherscan.io/api?module=account&action=tokennfttx&address=${wallet}&contractaddress=${UGLY_CONTRACT}&page=1&offset=100&sort=asc&apikey=${ETHERSCAN_API_KEY}`;

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
        content: `Token ID: ${randomToken}`,
        files: [{
          attachment: imgUrl,
          name: `ugly-${randomToken}.jpg`
        }]
      });

    } catch (err) {
      console.error(err);
      return message.reply('⚠️ Error fetching your Uglies. Please try again later.');
    }
  }

  // !myuglys
  if (command === 'myuglys') {
    const wallet = walletLinks[message.author.id];
    if (!wallet) {
      return message.reply('❌ Please link your wallet first using `!linkwallet 0x...`');
    }

    const url = `https://api.etherscan.io/api?module=account&action=tokennfttx&address=${wallet}&contractaddress=${UGLY_CONTRACT}&page=1&offset=100&sort=asc&apikey=${ETHERSCAN_API_KEY}`;

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

      const limitedTokens = tokenArray.slice(0, 10);
      const files = limitedTokens.map((tokenId) => ({
        attachment: `https://ipfs.io/ipfs/bafybeie5o7afc4yxyv3xx4jhfjzqugjwl25wuauwn3554jrp26mlcmprhe/${tokenId}`,
        name: `ugly-${tokenId}.jpg`
      }));

      const listedIds = limitedTokens.map(id => `Token ID: ${id}`).join('\n');

      return message.reply({ content: listedIds, files });

    } catch (err) {
      console.error(err);
      return message.reply('⚠️ Error fetching your Uglies. Please try again later.');
    }
  }

  // !monster
  if (command === 'monster') {
    const wallet = walletLinks[message.author.id];
    if (!wallet) {
      return message.reply('❌ Please link your wallet first using `!linkwallet 0x...`');
    }

    const url = `https://api.etherscan.io/api?module=account&action=tokennfttx&address=${wallet}&contractaddress=${MONSTER_CONTRACT}&page=1&offset=100&sort=asc&apikey=${ETHERSCAN_API_KEY}`;

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

      if (owned.size === 0) return message.reply('😢 You don’t own any Ugly Monster NFTs.');

      const tokenArray = Array.from(owned);
      const randomToken = tokenArray[Math.floor(Math.random() * tokenArray.length)];
      const imgUrl = `https://ipfs.io/ipfs/bafybeicydaui66527mumvml5ushq5ngloqklh6rh7hv3oki2ieo6q25ns4/${randomToken}.webp`;

      return message.reply({
        content: `Token ID: ${randomToken}`,
        files: [{
          attachment: imgUrl,
          name: `monster-${randomToken}.webp`
        }]
      });

    } catch (err) {
      console.error(err);
      return message.reply('⚠️ Error fetching your Monsters. Please try again later.');
    }
  }
});

client.login(DISCORD_TOKEN);
