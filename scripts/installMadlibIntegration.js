'use strict';
// Authoring utility: never starts the bot. Refuses an unexpected original index.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const BASELINE_SHA256='e2300e66aa556f1ccf74c61600ed4e83221aa9d8980c84d2d42dc82b5d6c6801';
const changes=[
  ["const squigDuels = require('./modules/squigDuels');","const squigDuels = require('./modules/squigDuels');\nconst madlib = require('./modules/madlib');"],
  ['    GatewayIntentBits.MessageContent\n',"    GatewayIntentBits.MessageContent,\n    ...(madlib.isEnabled() ? [GatewayIntentBits.GuildMessageReactions] : []),\n"],
  ['    squigDuels.buildSquigDuelSlashCommand().toJSON(),','    squigDuels.buildSquigDuelSlashCommand().toJSON(),\n    ...madlib.buildMadlibSlashCommands(),'],
  ['});\n\nconst RECEIPT_CHANNEL_ID',"  // Mad Libs awaits its own schema; existing ready jobs above are not delayed.\n  Promise.resolve(madlib.startMadlibWorkers()).catch(() => console.warn('[MadLibs] Worker startup failed.'));\n});\n\nconst RECEIPT_CHANNEL_ID"],
  ['async function awardDripPoints(realmId, memberIds, tokens, currencyId, settings, options = {}) {',`async function awardDripPoints(realmId, memberIds, tokens, currencyId, settings, options = {}) {
  // Opt-in only: preserve all legacy callers and their original fallback behavior.
  // Mad Libs must not try currency-less payloads or another route after a failed debit.
  if (options.madlibStrictTransfer === true) {
    return madlib.strictTransfer(realmId, memberIds, tokens, currencyId, settings, options, {
      fetchWithTimeout,
      buildDripHeaders,
      defaultSender: resolveConfiguredDripSenderMemberId(),
      botDiscordId: client.user?.id || DISCORD_CLIENT_ID,
    });
  }`],
  ['function getMarketplaceCommandDeps() {',`madlib.initMadlib({
  client,
  clientUserId: () => client.user?.id || DISCORD_CLIENT_ID || null,
  madlibPool: prizesPool,
  getWalletLinks,
  getGuildSettings,
  getHolderRules,
  getOwnedTokenIdsForContractMany,
  getMarketplaceSpendableBalance,
  getDripMemberCurrencyBalance,
  collectDripMemberIdCandidates,
  awardDripPoints,
  postAdminSystemLog,
  isAdmin,
  squigsContract: SQUIGS_CONTRACT,
  squigsChain: SQUIGS_CHAIN,
});

function getMarketplaceCommandDeps() {`],
  ["client.on('interactionCreate', async (interaction) => {\n  try {","client.on('interactionCreate', async (interaction) => {\n  try {\n    if (await madlib.handleInteraction(interaction)) return;"],
];
const sha=text=>crypto.createHash('sha256').update(text).digest('hex');
function reverse(source){let out=source;for(const [before,after] of [...changes].reverse()){if(out.split(after).length!==2)throw new Error('Missing or changed integration hunk.');out=out.replace(after,before);}return out;}
function apply(source){
  if(source.includes("const madlib = require('./modules/madlib');")){if(sha(reverse(source))!==BASELINE_SHA256)throw new Error('Unexpected already-integrated index.js; review current work rather than overwrite it.');return source;}
  if(sha(source)!==BASELINE_SHA256)throw new Error('index.js baseline changed. Re-review/rebase the additive hunks; never overwrite newer work.');
  let out=source;for(const [before,after] of changes){if(out.split(before).length!==2)throw new Error('Integration anchor is not unique.');out=out.replace(before,after);}return out;
}
if(require.main===module){const target=path.join(__dirname,'..','index.js'),source=fs.readFileSync(target,'utf8'),out=apply(source);if(out!==source)fs.writeFileSync(target,out);console.log('Seven reviewed Mad Lib integration hunks verified.');}
module.exports={apply,reverse,changes,BASELINE_SHA256,sha};
