'use strict';
const {PermissionFlagsBits,ChannelType}=require('discord.js');
const {check,MadlibError}=require('./madlibCore');
async function memberAccess(deps,guildId,userId,channelId) {
  check(guildId&&userId&&channelId,'GUILD','Use Mad Libs in a server text channel, not a DM.');
  const guild=await deps.client.guilds.fetch(guildId);
  const member=await guild.members.fetch({user:userId,force:true});
  const channel=await deps.client.channels.fetch(channelId);
  check(channel?.guildId===guildId&&[ChannelType.GuildText,ChannelType.GuildAnnouncement].includes(channel.type),'CHANNEL','Use a standard server text or announcement channel.');
  check(member.id===userId&&member.guild.id===guildId&&!member.user.bot&&!member.user.system&&channel.permissionsFor(member)?.has(PermissionFlagsBits.ViewChannel),'ACCESS','You must be a human member with access to this channel.');
  return {guild,member,channel};
}
async function eligible(deps,cfg,access) {
  const {guild,member}=access;
  if(cfg.MADLIB_ALLOWED_ROLE_IDS.length){check(cfg.MADLIB_ALLOWED_ROLE_IDS.some(r=>member.roles.cache.has(r)),'ELIGIBILITY','You need one of this feature’s approved access roles.');return access;}
  const verified=(await deps.getWalletLinks(guild.id,member.id)).filter(l=>l.verified&&l.wallet_address);
  check(verified.length,'ELIGIBILITY','Connect and verify your wallet through UglyBot’s existing verification panel first.');
  const rules=(await deps.getHolderRules(guild.id)).filter(r=>String(r.contract_address).toLowerCase()===deps.squigsContract.toLowerCase()&&Number(r.min_tokens)>=1);
  if(rules.some(r=>member.roles.cache.has(String(r.role_id))))return access;
  const chains=[...new Set(rules.map(r=>r.chain||deps.squigsChain))];if(!chains.length)chains.push(deps.squigsChain);
  try{for(const chain of chains){const owned=await deps.getOwnedTokenIdsForContractMany(verified.map(l=>l.wallet_address),deps.squigsContract,chain,{suppressErrors:false,concurrency:2});if(owned.length)return access;}}
  catch(_){throw new MadlibError('OWNERSHIP_UNAVAILABLE','The ownership provider is unavailable. This is not a finding of zero holdings; try again later.');}
  check(false,'ELIGIBILITY','Mad Libs needs verified Squigs Reloaded holder access. No roles were changed.');
}
async function reactorAccess(deps,pub,userId) {
  if(userId===pub.user_id||userId===deps.clientUserId())return null;
  const guild=await deps.client.guilds.fetch(pub.guild_id);let member;
  try{member=await guild.members.fetch({user:userId,force:true});}catch(e){if(e.code===10007||e.code===10013)return null;throw e;}
  if(member.id!==userId||member.user.id!==userId||member.guild.id!==pub.guild_id||member.user.bot||member.user.system)return null;
  if(pub.reactor_roles.length&&!pub.reactor_roles.some(r=>member.roles.cache.has(r)))return null;
  return member;
}
module.exports={memberAccess,eligible,reactorAccess};
