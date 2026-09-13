'use strict';
const {EmbedBuilder,AttachmentBuilder,PermissionFlagsBits,ChannelType}=require('discord.js');
const {check,nonce,safeDiscord,rewardRules}=require('./madlibCore');
const NO_MENTIONS=Object.freeze({parse:[],users:[],roles:[],repliedUser:false});
function marker(p){return `Mad Lib reference: ${p.id}`;}
function publicationConfig(p){return {MADLIB_REACTION_REWARD_CHARM:p.reward,MADLIB_REACTOR_ROLE_IDS:p.reactor_roles,MADLIB_REWARD_CAP_PER_POST:p.reward_cap};}
function storyEmbeds(s,title=s.template.title){
  const text=safeDiscord(s.story);check(text.length<=5200,'SIZE','This story is too large to display safely. Use its complete text export.');
  const parts=[];for(let i=0;i<text.length;){let end=Math.min(i+3500,text.length);if(end<text.length&&/[\uD800-\uDBFF]/.test(text[end-1])&&/[\uDC00-\uDFFF]/.test(text[end]))end--;parts.push(text.slice(i,end));i=end;}
  return parts.map((part,i)=>new EmbedBuilder().setTitle(i?`${title} — continued`:title).setDescription(part));
}
function publicPayload(p,s,image,totals={eligible:0,paid:'0',pending:'0',review:'0'}){
  const embeds=storyEmbeds(s),summary=`${totals.eligible} eligible reactions · ${totals.paid} $CHARM paid · ${totals.pending} pending (${totals.review} under review)`;
  embeds[0].setAuthor({name:s.display_name.slice(0,80)}).setImage(`attachment://${p.filename}`).setFooter({text:marker(p)});
  embeds[embeds.length-1].addFields({name:'Ugly Love',value:summary+(p.suspended?' · Future rewards suspended by moderation.':'')});
  return {content:rewardRules(publicationConfig(p)),embeds,files:image?[new AttachmentBuilder(image.bytes,{name:p.filename})]:undefined,allowedMentions:NO_MENTIONS,nonce:nonce(p.id),enforceNonce:true};
}
class MadlibPublishing{
  constructor(store,deps,cfg){this.store=store;this.deps=deps;this.cfg=cfg;}
  async target(guildId,channelId,emojiId){
    const channel=await this.deps.client.channels.fetch(channelId);
    check(channel?.guildId===guildId&&[ChannelType.GuildText,ChannelType.GuildAnnouncement].includes(channel.type),'CHANNEL','The configured Mad Lib publication channel must be a text or announcement channel in this server.');
    const me=channel.guild.members.me||await channel.guild.members.fetchMe(),permissions=channel.permissionsFor(me);
    const required=[PermissionFlagsBits.ViewChannel,PermissionFlagsBits.SendMessages,PermissionFlagsBits.AttachFiles,PermissionFlagsBits.EmbedLinks,PermissionFlagsBits.ReadMessageHistory,PermissionFlagsBits.AddReactions];
    check(permissions?.has(required),'PERMISSIONS','The bot needs View Channel, Send Messages, Attach Files, Embed Links, Read Message History and Add Reactions in the publication channel.');
    let emoji=this.deps.client.emojis.cache.get(emojiId);if(!emoji)emoji=await channel.guild.emojis.fetch(emojiId).catch(()=>null);
    check(emoji&&emoji.available!==false,'EMOJI','The configured custom Ugly Love emoji is unavailable. Check MADLIB_REACTION_EMOJI_ID.');
    if(emoji.guild?.id!==guildId)check(permissions.has(PermissionFlagsBits.UseExternalEmojis),'EMOJI','The bot needs Use External Emojis for this emoji.');
    if(emoji.roles?.cache?.size)check(me.permissions.has(PermissionFlagsBits.Administrator)||emoji.roles.cache.some(role=>me.roles.cache.has(role.id)),'EMOJI','The bot lacks a role allowed to use this restricted emoji.');return {channel,emoji};
  }
  async publish(p){
    if(p.status!=='prepared')return this.store.publication(p.id);
    const {channel,emoji}=await this.target(p.guild_id,p.channel_id,p.emoji_id);
    const [s,image]=await Promise.all([this.store.owned(p.guild_id,p.user_id,p.session_id),this.store.upload(p.session_id)]);
    check(image&&image.revision===p.upload_revision,'IMAGE_EXPIRED','The staged image expired or changed. Select this story in SHOW and upload a fresh image before publishing.');
    const payload=publicPayload(p,s,image);for(const embed of payload.embeds)embed.toJSON();
    const claimed=await this.store.claimPublication(p.id,p.revision);if(!claimed)return this.store.publication(p.id);let message;
    try{message=await channel.send(payload);await this.store.confirmPublication(p.id,message,claimed.lease_id);}
    catch(error){
      const known=!message&&[400,401,403,404,429].includes(error.status);
      await this.store.reviewPublication(p.id,known?`CONFIRMED_NOT_SENT_HTTP_${error.status}`:'SEND_OUTCOME_UNCERTAIN').catch(()=>{});
      await this.deps.postAdminSystemLog({guildId:p.guild_id,category:'Mad Lib Publication',message:`Publication ${p.id} needs review. No automatic repost. Use /madlib-admin inspect.`}).catch(()=>{});return this.store.publication(p.id);
    }
    await this.seed(p.id,message,emoji);return this.store.publication(p.id);
  }
  async seed(pid,message,emoji){
    try{await message.react(emoji);await this.store.query('UPDATE madlib_publications SET emoji_seeded=TRUE WHERE id=$1',[pid]);}catch(_){ /* Retry emoji seeding, never the publication. */ }
  }
  async refresh(p,message){
    if(p.last_display_at&&Date.now()-p.last_display_at.getTime()<60000)return;
    if(!p.display_dirty&&p.last_display_at&&Date.now()-p.last_display_at.getTime()<3600000)return;
    const s=await this.store.owned(p.guild_id,p.user_id,p.session_id),totals=await this.store.totals(p.id),payload=publicPayload(p,s,null,totals);
    delete payload.files;delete payload.nonce;delete payload.enforceNonce;
    const attachment=message.attachments.find(a=>a.id===p.attachment_id)||message.attachments.first();if(attachment)payload.embeds[0].setImage(attachment.url);
    await message.edit(payload);await this.store.query('UPDATE madlib_publications SET display_dirty=FALSE,last_display_at=now() WHERE id=$1',[p.id]);
  }
  async reconcileUncertain(p,channel){
    if(p.scan_before==='done')return {before:'done',delay:3600};
    const messages=await channel.messages.fetch({limit:50,before:p.scan_before||undefined,cache:false});
    const hits=[...messages.values()].filter(m=>m.author.id===this.deps.clientUserId()&&m.embeds.some(e=>e.footer?.text===marker(p)));
    if(hits.length===1){await this.store.confirmPublication(p.id,hits[0]);return {found:true};}
    if(hits.length>1){await this.store.query("UPDATE madlib_publications SET moderation_reason='DUPLICATE_MARKER_REVIEW',scan_before='done' WHERE id=$1",[p.id]);return {before:'done',delay:3600};}
    const oldest=[...messages.values()].reduce((a,m)=>!a||BigInt(m.id)<BigInt(a.id)?m:a,null);
    if(messages.size<50||oldest&&oldest.createdTimestamp<p.created_at.getTime()-10000)return {before:'done',delay:3600};return {before:oldest?.id||'done',delay:5};
  }
  async linkReviewed(guild,actor,pid,revision,messageId,evidence){
    check(evidence?.trim().length>=8&&evidence.length<=500,'EVIDENCE','Provide the publication recovery evidence or reason.');const p=await this.store.publication(pid);
    check(p?.guild_id===guild&&p.revision===revision&&p.status==='needs_review','STALE','Inspect this publication again and use its latest revision.');
    const {channel}=await this.target(guild,p.channel_id,p.emoji_id),m=await channel.messages.fetch({message:messageId,force:true,cache:false});
    check(m.author.id===this.deps.clientUserId()&&m.embeds.some(e=>e.footer?.text===marker(p)),'PUBLICATION','The message must be bot-owned and contain this exact publication marker.');
    await this.store.confirmPublication(p.id,m,null,{guild,actor,revision,evidence});return this.store.publication(pid);
  }
  async retryReviewed(guild,actor,pid,revision,evidence){
    check(evidence?.trim().length>=8&&evidence.length<=500,'EVIDENCE','Provide a specific reason for retrying this confirmed-unsent publication.');
    const p=await this.store.tx(async c=>{
      const current=await this.store.one('SELECT * FROM madlib_publications WHERE id=$1 AND guild_id=$2 FOR UPDATE',[pid,guild],c);
      check(current&&current.revision===revision&&current.status==='needs_review'&&/^CONFIRMED_NOT_SENT_HTTP_/.test(current.moderation_reason||''),'STATE','Only a confirmed-unsent publication can be retried. Ambiguous sends must be located, not reposted.');
      await this.store.audit(c,guild,actor,'retry-publication',pid,evidence);
      return this.store.one("UPDATE madlib_publications SET status='prepared',revision=revision+1,scan_before=NULL,lease_id=NULL,lease_until=NULL WHERE id=$1 RETURNING *",[pid],c);
    });
    if(!await this.store.upload(p.session_id))return p;return this.publish(p);
  }
}
module.exports={MadlibPublishing,publicPayload,storyEmbeds,NO_MENTIONS,marker};
