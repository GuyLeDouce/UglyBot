'use strict';
const assert=require('node:assert/strict');
const {Collection,PermissionsBitField,PermissionFlagsBits,ChannelType,MessageFlagsBitField}=require('discord.js');
const {EventEmitter}=require('node:events');const core=require('../modules/madlibCore');
// Execute only this existing pure Marketplace parser. Never require index.js (it starts the bot).
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'..','index.js'),'utf8');
const parserStart=source.indexOf('function extractDripCurrencyAmountFromPayload(');
const parserEnd=source.indexOf('\nasync function getDripMemberCurrencyBalance(',parserStart);
assert(parserStart>=0&&parserEnd>parserStart);
const extractDripCurrencyAmountFromPayload=vm.runInNewContext('('+source.slice(parserStart,parserEnd)+')');
const IDS=Object.freeze({guild:'100000000000000001',otherGuild:'100000000000000002',user:'200000000000000001',other:'200000000000000002',bot:'300000000000000001',channel:'1334884237727240267',emoji:'1526597741160169522',role:'400000000000000001',message:'500000000000000001'});
function harness(file){let count=0;return {async test(name,fn){await fn();count++;console.log(`PASS ${file}: ${name}`);},done(){console.log(`RESULT ${file}: ${count} test groups passed`);return count;}};}
function errorCode(code){return e=>{assert.equal(e.code,code);return true;};}
function fakeDiscord({admin=true,roles=[IDS.role]}={}){
  const member={id:IDS.user,displayName:'Guy @everyone *Ugly*',user:{id:IDS.user,username:'Guy',bot:false,system:false},roles:{cache:new Collection(roles.map(id=>[id,{id}]))},permissions:new PermissionsBitField(admin?[PermissionFlagsBits.ManageGuild]:[])};
  const guild={id:IDS.guild,members:{fetch:async({user})=>user===member.id?member:{...member,id:user,user:{...member.user,id:user}}},emojis:{fetch:async()=>({id:IDS.emoji,name:'uglylove',available:true})}};
  const me={id:IDS.bot,user:{id:IDS.bot,bot:true},roles:{cache:new Collection()},permissions:new PermissionsBitField(PermissionsBitField.All)};guild.members.me=me;guild.members.fetchMe=async()=>me;member.guild=guild;
  const messages=[],emoji={id:IDS.emoji,name:'uglylove',guild,available:true,roles:{cache:new Collection()}};
  const channel={id:IDS.channel,guildId:IDS.guild,guild,type:ChannelType.GuildText,permissionsFor:()=>new PermissionsBitField(PermissionsBitField.All),messages:{fetch:async()=>{throw Object.assign(new Error('Missing message'),{code:10008});}},send:async payload=>{messages.push(payload);return {id:IDS.message,guildId:IDS.guild,author:{id:IDS.bot},attachments:new Collection(),react:async()=>{}};}};
  const client=new EventEmitter();Object.assign(client,{user:{id:IDS.bot},guilds:{fetch:async id=>{assert.equal(id,IDS.guild);return guild;}},channels:{fetch:async id=>{assert.equal(id,IDS.channel);return channel;}},emojis:{cache:new Collection([[IDS.emoji,emoji]])}});
  const logs=[];const deps={client,clientUserId:()=>client.user.id,postAdminSystemLog:async x=>logs.push(x),isAdmin:i=>i.memberPermissions.has(PermissionFlagsBits.ManageGuild),getWalletLinks:async()=>[{verified:true,wallet_address:'0xverified',drip_member_id:'member-user'}],getHolderRules:async()=>[{contract_address:'0xsquigs',min_tokens:1,role_id:IDS.role,chain:'ethereum'}],getOwnedTokenIdsForContractMany:async()=>['7'],squigsContract:'0xsquigs',squigsChain:'ethereum',getGuildSettings:async()=>({drip_realm_id:'realm',currency_id:'charm',drip_api_key:'FAKE_TEST_KEY'}),random:()=>0};
  return {deps,member,me,guild,channel,client,messages,emoji,logs};
}
function interaction({command=null,customId=null,privateMessage=false,values=[],answer='',options={},user=IDS.user,guild=IDS.guild}={}){
  const calls=[];const i={commandName:command,customId,values,user:{id:user,username:'User'},guildId:guild,channelId:IDS.channel,attachmentSizeLimit:8388608,deferred:false,replied:false,memberPermissions:new PermissionsBitField(PermissionsBitField.All),
    message:customId?{id:IDS.message,flags:new MessageFlagsBitField(privateMessage?64:0)}:null,
    isMessageComponent:()=>Boolean(customId&&!customId.includes(':submit:')),isModalSubmit:()=>Boolean(customId?.includes(':submit:')),isFromMessage:()=>Boolean(customId),isChatInputCommand:()=>Boolean(command),
    fields:{getTextInputValue:()=>answer},options:{getString:key=>options[key]??null,getInteger:key=>options[key]??null,getAttachment:key=>options[key]??null},
    deferReply:async p=>{calls.push(['deferReply',p]);assert(!i.deferred&&!i.replied);i.deferred=true;},
    deferUpdate:async()=>{calls.push(['deferUpdate']);assert(privateMessage);assert(!i.deferred&&!i.replied);i.deferred=true;},
    reply:async p=>{calls.push(['reply',p]);assert(!i.deferred&&!i.replied);i.replied=true;return {id:IDS.message};},
    editReply:async p=>{calls.push(['editReply',p]);assert(i.deferred||i.replied);return {id:IDS.message};},
    followUp:async p=>{calls.push(['followUp',p]);assert(i.deferred||i.replied);assert.equal(p.ephemeral,true);return {id:IDS.message};},
    showModal:async modal=>{calls.push(['showModal',modal.toJSON()]);assert(!i.deferred&&!i.replied);i.replied=true;},calls};return i;
}
function session(template,user=IDS.user){return {id:core.id(),guild_id:IDS.guild,user_id:user,display_name:'A Squig',template,template_id:template.id,template_version:template.version,answers:{},state:'active',step:0,revision:0,cost:0};}
function modelStore(seed){
  let current=seed?structuredClone(seed):null,schemaCalls=0;
  return {async ensureMadlibTables(){schemaCalls++;},get schemaCalls(){return schemaCalls;},
    async owned(g,u,id){core.check(current&&current.guild_id===g&&current.user_id===u&&current.id===id,'OWNER','Not yours.');return structuredClone(current);},
    async active(g,u){return current&&current.guild_id===g&&current.user_id===u&&current.state!=='completed'?structuredClone(current):null;},
    async begin(){return {session:structuredClone(current),resumed:true};},async deliveryAttempt(){},async delivered(){},async one(){return null;},async query(){return {rows:[]};},async upload(){return null;},
    async editSession(g,u,id,revision,action,value){await this.owned(g,u,id);core.check(current.revision===revision,'STALE','Stale question.');if(action==='answer'){const q=current.template.questions[current.step];current.answers[q.key]=core.validateAnswer(value,q);current.step++;}else if(action==='back')current.step=Math.max(0,current.step-1);else if(action==='abandon')current.state='cancelled';current.revision++;if(current.step===current.template.questions.length){Object.assign(current,core.render(current.template,current.answers));current.state='completed';current.completed_at=new Date();}return structuredClone(current);},
    async history(g,u){return current?.state==='completed'&&current.guild_id===g&&current.user_id===u?[structuredClone(current)]:[];},read(){return structuredClone(current);},set(s){current=structuredClone(s);}};
}
module.exports={extractDripCurrencyAmountFromPayload,assert,harness,errorCode,IDS,fakeDiscord,interaction,session,modelStore,Collection,PermissionsBitField,PermissionFlagsBits,ChannelType};
