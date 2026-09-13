'use strict';
// Discord File Upload (19) in Label (18), sent as API JSON: no SDK/dependency upgrade.
// https://docs.discord.com/developers/components/reference#file-upload
const {ModalSubmitFields}=require('discord.js');
const {createHash}=require('node:crypto');
const {check,component,snowflake}=require('./madlibCore');
const PREFIX='madlib:upload-submit:';
const INPUT_ID='image';
const NATIVE_UPLOADS=typeof ModalSubmitFields?.prototype?.getUploadedFiles==='function';
const FALLBACK='Reopen SHOW and select your story, or use /madlib-upload to attach the image.';
function draftStamp(draft){
  const time=new Date(draft.updated_at).getTime();
  check(Number.isFinite(time),'DRAFT',FALLBACK);
  return createHash('sha256').update(`${draft.session_id}:${draft.revision}:${time}`).digest('hex').slice(0,8);
}
function modalTarget(control){
  const match=/^([0-9a-f-]{36})-([0-9a-f]{8})$/.exec(control.id);
  check(match,'UPLOAD_MODAL',FALLBACK);return {sessionId:match[1],stamp:match[2]};
}
function buildUploadModal(draft,owner,maxBytes){
  check(Number.isSafeInteger(maxBytes)&&maxBytes>0,'IMAGE_SIZE',FALLBACK);
  const payload={custom_id:component('upload-submit',`${draft.session_id}-${draftStamp(draft)}`,draft.revision,owner),title:'Upload your Squig image',components:[{
    type:18,label:'Choose one image',description:`Still PNG, JPG/JPEG or WebP. Maximum ${(maxBytes/1048576).toFixed(2)} MiB. Preview first; Publish shares it.`,
    component:{type:19,custom_id:INPUT_ID,min_values:1,max_values:1,required:true},
  }]};
  // showModal accepts a JSONEncodable on both baseline and modern discord.js.
  return {toJSON:()=>payload};
}
function uploadLimit(configured,interactionLimit,rawLimit){
  return Math.min(configured,...[interactionLimit,rawLimit].filter(n=>Number.isSafeInteger(n)&&n>0));
}
function attachmentFields(a){
  check(a&&snowflake(a.id)&&typeof a.url==='string'&&a.url.length<=4096&&Number.isSafeInteger(a.size)&&a.size>0,'UPLOAD_MODAL','Attach exactly one image. '+FALLBACK);
  const type=a.contentType??a.content_type??null;
  check(type===null||typeof type==='string'&&type.length<=128,'UPLOAD_MODAL',FALLBACK);
  // Store only fields used by the existing validator, not a raw interaction/token.
  return {id:a.id,url:a.url,size:a.size,contentType:type};
}
function rawAttachment(data){
  const rows=data.components;
  check(Array.isArray(rows)&&rows.length===1&&rows[0].type===18,'UPLOAD_MODAL',FALLBACK);
  const input=rows[0].component;
  check(input?.type===19&&input.custom_id===INPUT_ID&&Array.isArray(input.values)&&input.values.length===1&&snowflake(input.values[0]),'UPLOAD_MODAL','Choose exactly one image. '+FALLBACK);
  const id=input.values[0],all=data.resolved?.attachments;
  check(all&&Object.hasOwn(all,id)&&all[id]?.id===id,'UPLOAD_MODAL','The image attachment was missing. '+FALLBACK);
  return attachmentFields(all[id]);
}
class UploadModalBridge{
  constructor(client,{native=NATIVE_UPLOADS,now=Date.now,capacity=128,ttl=30000}={}){
    this.client=client;this.native=native;this.now=now;this.capacity=capacity;this.ttl=ttl;this.pending=new Map();this.listening=false;
    this.onRaw=packet=>this.capture(packet);
    this.start();
  }
  start(){
    if(!this.native&&!this.listening&&this.client?.on){this.client.on('raw',this.onRaw);this.listening=true;}
  }
  stop(){if(this.listening)this.client.off('raw',this.onRaw);this.listening=false;this.pending.clear();}
  get available(){return this.native||this.listening;}
  prune(){const now=this.now();for(const [id,entry] of this.pending)if(entry.expires<=now)this.pending.delete(id);}
  capture(packet){
    if(this.native||packet?.t!=='INTERACTION_CREATE'||packet.d?.type!==5||typeof packet.d.data?.custom_id!=='string'||!packet.d.data.custom_id.startsWith(PREFIX))return;
    const d=packet.d;
    // 14.16.3's ModalSubmitFields assumes ActionRow.components and throws on Label.
    // Only OUR new file-modal envelope is normalized, before the SDK constructs it.
    // Other Mad Lib answers, commands, reactions and other features are untouched.
    packet.d={...d,data:{...d.data,components:[]}};
    this.prune();
    if(!snowflake(d.id)||this.pending.has(d.id))return;
    if(this.pending.size>=this.capacity)return; // Handler replies with the fallback; no unbounded queue.
    const user=d.member?.user?.id??d.user?.id,channel=d.channel?.id??d.channel_id;
    let attachment=null;
    try{attachment=rawAttachment(d.data);}catch(_){/* Fixed private error on consume; never log raw payloads. */}
    this.pending.set(d.id,{guild:d.guild_id,user,channel,customId:d.data.custom_id,application:d.application_id,attachment,limit:d.attachment_size_limit,expires:this.now()+this.ttl});
  }
  take(i){
    check(i.isModalSubmit?.()&&i.customId?.startsWith(PREFIX),'UPLOAD_MODAL',FALLBACK);
    if(this.native){
      const field=i.fields?.getField(INPUT_ID,19),files=i.fields?.getUploadedFiles(INPUT_ID,true);
      check(field?.type===19&&files?.size===1,'UPLOAD_MODAL','Choose exactly one image. '+FALLBACK);
      return {attachment:attachmentFields(files.first()),limit:i.attachmentSizeLimit};
    }
    this.prune();const entry=this.pending.get(i.id);this.pending.delete(i.id);
    check(entry&&entry.guild===i.guildId&&entry.user===i.user.id&&entry.channel===i.channelId&&entry.customId===i.customId&&entry.application===i.applicationId&&entry.attachment,'UPLOAD_MODAL',FALLBACK);
    return {attachment:entry.attachment,limit:entry.limit};
  }
}
module.exports={buildUploadModal,draftStamp,modalTarget,uploadLimit,rawAttachment,UploadModalBridge,NATIVE_UPLOADS,FALLBACK};
