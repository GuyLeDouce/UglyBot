'use strict';
const {
  SlashCommandBuilder,PermissionFlagsBits,MessageFlags,EmbedBuilder,AttachmentBuilder,
  ActionRowBuilder,ButtonBuilder,ButtonStyle,StringSelectMenuBuilder,
  ModalBuilder,TextInputBuilder,TextInputStyle,
}=require('discord.js');
const core=require('./madlibCore');
const {MadlibStore}=require('./madlibStore');
const {MadlibEconomy,strictTransfer}=require('./madlibEconomy');
const {MadlibPublishing,storyEmbeds,NO_MENTIONS}=require('./madlibPublishing');
const {MadlibWorkers}=require('./madlibWorkers');
const {downloadAttachment}=require('./madlibImages');
const {memberAccess,eligible}=require('./madlibAccess');
const {UploadModalBridge,buildUploadModal,draftStamp,modalTarget,uploadLimit}=require('./madlibUploadModal');
const {check,component,parseComponent,safeDiscord,errorMessage}=core;
const ADMIN_ACTIONS=['status','inspect','pause','resume','suspend','unsuspend','retry','mark-sent','mark-not-sent','refund','link-publication','retry-publication'];
const COMMANDS=['madlib','madlib-upload','madlib-admin','madlib-history'];
const COMPONENT_ACTIONS=new Set(['play','show','resume','accept','answer','submit','back','exit','abandon','abandon-confirm','history','select','prompt','story','copy','export-prompt','export-story','publish','replace','cancel-image','upload','upload-submit']);
let instance=null;
function buildMadlibSlashCommands(env=process.env){
  if(!core.enabled(env))return [];
  const panel=new SlashCommandBuilder().setName('madlib').setDescription('Post the Squig Mad Libs PLAY / SHOW panel (admins only).').setDMPermission(false).setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild);
  const upload=new SlashCommandBuilder().setName('madlib-upload').setDescription('Privately preview one generated image for your completed Mad Lib.').setDMPermission(false)
    .addAttachmentOption(o=>o.setName('image').setDescription('One PNG, JPEG or WebP image; up to 8 MiB.').setRequired(true))
    .addStringOption(o=>o.setName('story_id').setDescription('Optional saved story ID; otherwise use your current SHOW selection.').setMaxLength(36));
  const admin=new SlashCommandBuilder().setName('madlib-admin').setDescription('Inspect and safely review Mad Lib payments/publications.').setDMPermission(false).setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild)
    .addStringOption(o=>o.setName('action').setDescription('Administrative action.').setRequired(true).addChoices(...ADMIN_ACTIONS.map(name=>({name,value:name}))))
    .addStringOption(o=>o.setName('record').setDescription('Operation or publication ID from status/inspect.').setMaxLength(180))
    .addIntegerOption(o=>o.setName('revision').setDescription('Latest record revision, required for changes.').setMinValue(0))
    .addStringOption(o=>o.setName('evidence').setDescription('Specific reason or authoritative transaction evidence; never API keys.').setMinLength(8).setMaxLength(500))
    .addStringOption(o=>o.setName('message_id').setDescription('Existing public bot message ID for link-publication.').setMaxLength(20));
  const history=new SlashCommandBuilder().setName('madlib-history').setDescription('Read your saved Mad Libs or resume from an accessible server text channel.').setDMPermission(false);
  return [panel,upload,admin,history].map(c=>c.toJSON());
}
const row=(...buttons)=>new ActionRowBuilder().addComponents(buttons);
const button=(label,action,record='home',revision=0,owner='public',style=ButtonStyle.Secondary)=>new ButtonBuilder().setLabel(label).setStyle(style).setCustomId(component(action,record,revision,owner));
const privatePayload=data=>({allowedMentions:NO_MENTIONS,...data});
const timestamp=date=>`<t:${Math.floor(new Date(date).getTime()/1000)}:R>`;
function isPrivateMessage(i){return Boolean(i.message?.flags?.has?.(MessageFlags.Ephemeral)||((Number(i.message?.flags?.bitfield)||0)&64));}
async function acknowledge(i){
  if(i.deferred||i.replied)return;
  if(isPrivateMessage(i)&&(i.isMessageComponent?.()||i.isModalSubmit?.()&&i.isFromMessage?.()))await i.deferUpdate();else await i.deferReply({ephemeral:true});
}
async function edit(i,data){const payload=privatePayload({content:null,embeds:[],components:[],attachments:[],...data});return i.deferred||i.replied?i.editReply(payload):i.reply({...payload,ephemeral:true});}
async function follow(i,data){return i.followUp(privatePayload({...data,ephemeral:true}));}
function resultControls(s){return [row(button('Get Copyable Prompt','copy',s.id,0,s.user_id),button('Download Prompt .txt','export-prompt',s.id,0,s.user_id),button('Download Story .txt','export-story',s.id,0,s.user_id)),row(button('Classic Story','story',s.id,0,s.user_id),button('SHOW / History','history','0',0,s.user_id))];}
function instruction(){return 'Copy the complete prompt into your image tool, attach your Squig reference there, generate the image, then return to SHOW. Prefer a still 1024 x 1024 PNG; JPEG/WebP are also accepted. Follow the export limits in your prompt and check the actual file size. UglyBot does not generate images in V1.';}
function stateMessage(s){
  if(s.state==='awaiting_payment')return 'Payment is being checked. Do not pay again. Resume opens the same saved play after confirmation.';
  if(s.state==='payment_review')return 'Payment under review. The outcome was uncertain; no second charge will be sent. Ask an admin to inspect the operation below.';
  if(s.state==='payment_failed')return 'No completed debit is recorded. The transfer was not sent or was definitively rejected. An admin can inspect and retry this same intent. You may abandon this unpaid intent.';
  if(s.state==='refund_pending')return 'A system-failure refund is pending. No new debit is being created.';return `Saved play status: ${s.state}.`;
}
class MadlibFeature{
  constructor(deps,{env=process.env,templates=null,store=null,images=downloadAttachment,timers=globalThis}={}){
    this.deps=deps;this.env=env;this.images=images;this.available=false;this.failure=null;
    // Also handles old upload modals after disable/restart without touching schema or finances.
    this.uploadModals=new UploadModalBridge(deps.client);
    this.ready=(async()=>{
      if(!core.enabled(env))return false;this.cfg=core.config(env);
      this.templates=core.validateTemplates(templates||require('./madlibTemplates')).map(template=>({
        ...template,output:{maxImageBytes:this.cfg.MADLIB_MAX_IMAGE_BYTES},
      })); // selectTemplate deep-copies this limit into each new session snapshot.
      this.store=store||new MadlibStore(deps.madlibPool,{random:deps.random||Math.random});await this.store.ensureMadlibTables();
      this.economy=new MadlibEconomy(this.store,deps);this.publishing=new MadlibPublishing(this.store,deps,this.cfg);
      this.workers=new MadlibWorkers({deps,store:this.store,economy:this.economy,publishing:this.publishing,cfg:this.cfg,timers,log:code=>this.log(code)});
      this.available=true;return true;
    })().catch(error=>{this.failure=error instanceof core.MadlibError?error.code:'INITIALIZATION_FAILED';this.available=false;this.log(this.failure);return false;});
  }
  log(code){console.warn(`[MadLibs] ${String(code).replace(/[^A-Z0-9_: -]/gi,'').slice(0,100)}`);}
  async start(){this.uploadModals.start();if(await this.ready)return this.workers.start();return false;}
  stop(){this.workers?.stop();this.uploadModals.stop();}
  async access(i){return memberAccess(this.deps,i.guildId,i.user.id,i.channelId);}
  async requireAdmin(i,access){check(await this.deps.isAdmin({user:i.user,guildId:i.guildId,guild:access.guild,member:access.member,memberPermissions:access.member.permissions}),'ADMIN','Only an UglyBot administrator can use that action.');}
  async ensurePanel(i,c){
    if(c.owner!=='public')return;check(['play','show'].includes(c.action),'OWNER','This control is not public.');
    check(await this.store.one('SELECT id FROM madlib_panels WHERE id=$1 AND guild_id=$2 AND channel_id=$3 AND message_id=$4',[c.id,i.guildId,i.channelId,i.message?.id]),'PANEL','That panel is not registered in this channel. Ask an admin to post /madlib.');
  }
  async panel(i,access){
    await this.requireAdmin(i,access);const id=core.id(),cfg=this.cfg;
    const description=['Your answers. Your Squig. Absolutely no guarantees of dignity.','Answer a few suspiciously normal questions and UglyBot will turn them into a ridiculous human-world misadventure and a matching image prompt: daily life, GM/GN, memes and web3 desk mischief.','**PLAY** to create your next questionable adventure. **SHOW** to share your generated image and collect some Ugly Love.',`One free play every ${cfg.MADLIB_FREE_COOLDOWN_HOURS} hours. Extra plays cost ${cfg.MADLIB_EXTRA_PLAY_COST_CHARM.toLocaleString('en-US')} $CHARM.`,core.rewardRules(cfg),instruction()].join('\n\n');
    const message=await access.channel.send(privatePayload({embeds:[new EmbedBuilder().setTitle('SQUIG MAD LIBS').setDescription(description)],components:[row(button('PLAY','play',id,0,'public',ButtonStyle.Primary),button('SHOW','show',id,0,'public',ButtonStyle.Success))]}));
    await this.store.query('INSERT INTO madlib_panels(id,guild_id,channel_id,message_id,creator_id) VALUES($1,$2,$3,$4,$5)',[id,i.guildId,i.channelId,message.id,i.user.id]);
    return edit(i,{content:'SQUIG MAD LIBS panel posted. PLAY and SHOW are ready.'});
  }
  async progress(i,s,{separateStory=false}={}){
    if(s.state==='completed')return this.results(i,s,separateStory);
    if(s.state!=='active'){
      const controls=[button('Resume / Check','resume',s.id,s.revision,s.user_id),button('SHOW / History','history','0',0,s.user_id)];
      if(s.state==='payment_failed')controls.push(button('Abandon unpaid intent','abandon',s.id,s.revision,s.user_id));
      const op=s.state==='payment_failed'?await this.store.operation(`madlib_play:${s.id}`,s.guild_id):null;
      const detail=op?.error_code==='INSUFFICIENT_FUNDS'?`You need ${s.cost.toLocaleString('en-US')} $CHARM. No debit was sent.`:op?.error_code==='BALANCE_UNKNOWN'?'The balance could not be confirmed; no debit was sent.':op?.error_code?`Review code: ${op.error_code}`:'';
      return edit(i,{content:`${stateMessage(s)} ${detail}\n\nSaved play: \`${s.id}\`\nPayment operation: \`madlib_play:${s.id}\``,components:[row(...controls)]});
    }
    const q=s.template.questions[s.step];check(q,'STATE','No question was found. Your saved play needs admin review.');
    const controls=[button('Answer','answer',s.id,s.revision,s.user_id,ButtonStyle.Primary)];if(s.step>0)controls.push(button('Back','back',s.id,s.revision,s.user_id));controls.push(button('Exit / Save','exit',s.id,s.revision,s.user_id));
    await this.store.deliveryAttempt(s.guild_id,s.user_id,s.id);
    await edit(i,{embeds:[new EmbedBuilder().setTitle(`Question ${s.step+1} of ${s.template.questions.length}`).setDescription(`**${safeDiscord(q.label)}**\nWord type: ${safeDiscord(q.type)}\n${safeDiscord(q.hint)}\n\nYour answers are saved after each submission. The scenario stays hidden until the end.`)],components:[row(...controls)]});
    await this.store.delivered(s.guild_id,s.user_id,s.id);
  }
  async results(i,s,separateStory=true){
    check(s.state==='completed'&&s.story&&s.prompt,'STATE','Finish all questions to view your completed outputs.');
    const embed=new EmbedBuilder().setTitle('Image Prompt').setDescription(s.prompt.length<=3900?s.prompt:'Your complete prompt is attached as UTF-8 text. Use the copy/download controls below; nothing has been truncated.');
    await edit(i,{content:instruction(),embeds:[embed],files:s.prompt.length>3900?[new AttachmentBuilder(Buffer.from(s.prompt,'utf8'),{name:`madlib-${s.id}-prompt.txt`})]:[],components:resultControls(s)});
    if(separateStory)await follow(i,{embeds:storyEmbeds(s,`Classic Mad Lib Story — ${s.template.title}`),components:[row(button('Download Story .txt','export-story',s.id,0,s.user_id),button('SHOW / History','history','0',0,s.user_id))]});
  }
  async begin(i,access,quoteId=null){
    const active=await this.store.active(i.guildId,i.user.id);if(active)return this.progress(i,active);
    await eligible(this.deps,this.cfg,access);const settings=quoteId?await this.deps.getGuildSettings(i.guildId):null;
    const result=await this.store.begin({guild:i.guildId,user:i.user.id,name:access.member.displayName||i.user.username,templates:this.templates,cfg:this.cfg,quoteId,settings});
    if(result.quote){const q=result.quote;return edit(i,{content:`Your next free play is available ${timestamp(result.nextFreeAt)}.\n\n**Spend ${q.amount.toLocaleString('en-US')} $CHARM for one extra play?** This quote expires ${timestamp(q.expires_at)}. We recheck your free allowance before charging.\n\nResume, SHOW, and history are free. Paid plays do not move your next free time. Answers survive restarts. Exit only pauses; abandonment uses the play. Confirmed system failures are recoverable or refunded once. Uncertain payments wait for admin review instead of another charge.`,components:[row(button(`Spend ${q.amount.toLocaleString('en-US')} $CHARM`,'accept',q.id,0,i.user.id,ButtonStyle.Danger),button('Exit','history','0',0,i.user.id))]});}
    let s=result.session;
    if(s.state==='awaiting_payment'){await this.progress(i,s);await this.economy.execute(`madlib_play:${s.id}`);s=await this.store.owned(i.guildId,i.user.id,s.id);}
    try{return await this.progress(i,s);}catch(error){
      // Restore only a definitely rejected first free delivery. Ambiguous sends stay resumable.
      if(!result.resumed&&!s.cost&&[400,403,404].includes(error.status))await this.store.restoreUndeliveredFree(i.guildId,i.user.id,s.id).catch(()=>{});throw error;
    }
  }
  async history(i,page=0){
    const all=await this.store.history(i.guildId,i.user.id,page),entries=all.slice(0,20);
    if(!entries.length)return edit(i,{content:'No completed stories on this page yet. PLAY a Mad Lib, finish the questions, then return to SHOW.',components:[row(button('PLAY / Resume','play','home',0,i.user.id,ButtonStyle.Primary))]});
    const menu=new StringSelectMenuBuilder().setCustomId(component('select',String(page),0,i.user.id)).setPlaceholder('Choose a saved story').addOptions(entries.map(s=>({label:s.template.title.slice(0,100),description:`${new Date(s.completed_at).toISOString().slice(0,10)} · ${s.publication_status||'Unpublished'} · ${s.id.slice(0,8)}`.slice(0,100),value:s.id})));
    const nav=[];if(page>0)nav.push(button('Previous','history',String(page-1),0,i.user.id));if(all.length>20)nav.push(button('Next','history',String(page+1),0,i.user.id));nav.push(button('PLAY / Resume','play','home',0,i.user.id));
    return edit(i,{content:`**SHOW / History — page ${page+1}**\nSelect a completed story to upload an image or revisit its existing post. Older stories remain saved.`,components:[new ActionRowBuilder().addComponents(menu),row(...nav)]});
  }
  async selected(i,sid){
    const selected=await this.store.chooseDraft(i.guildId,i.user.id,sid),s=selected.session;
    if(selected.publication)return this.publicationStatus(i,selected.publication);
    const image=await this.store.upload(s.id);if(image&&image.revision===selected.draft.revision)return this.preview(i,s,selected.draft,image);
    return edit(i,{content:`**${safeDiscord(s.template.title)}**\nStory ID: \`${s.id}\`\n\nClick **Upload Image** below and choose one PNG, JPEG or WebP. The upload and preview are private; only Publish shares it. **/madlib-upload** remains available as a fallback.\n\n${instruction()}`,components:[row(button('Upload Image','upload',s.id,selected.draft.revision,i.user.id,ButtonStyle.Primary)),row(button('Image Prompt','prompt',s.id,0,i.user.id),button('Classic Story','story',s.id,0,i.user.id),button('SHOW / History','history','0',0,i.user.id))]});
  }
  async preview(i,s,draft,image){
    const p=await this.store.one('SELECT * FROM madlib_publications WHERE session_id=$1',[s.id]);
    const cfg=p?{...this.cfg,MADLIB_CHANNEL_ID:p.channel_id,MADLIB_REACTION_REWARD_CHARM:p.reward,MADLIB_REWARD_CAP_PER_POST:p.reward_cap,MADLIB_REACTOR_ROLE_IDS:p.reactor_roles}:this.cfg;
    const embeds=storyEmbeds(s);embeds[0].setImage('attachment://madlib-preview.png');
    return edit(i,{content:`**Private preview** — ${safeDiscord(s.display_name)}\nTarget channel ID: ${cfg.MADLIB_CHANNEL_ID}\nNothing has been published. Uploads expire after 24 hours.\n\n${core.rewardRules(cfg)}`,embeds,files:[new AttachmentBuilder(image.bytes,{name:'madlib-preview.png'})],components:[row(button('Publish','publish',s.id,draft.revision,i.user.id,ButtonStyle.Success),button('Replace Image','replace',s.id,draft.revision,i.user.id),button('Cancel','cancel-image',s.id,draft.revision,i.user.id))]});
  }
  async checkUploadDraft(i,sid,revision,stamp=null){
    const s=await this.store.owned(i.guildId,i.user.id,sid);check(s.state==='completed','STATE','Finish the story before uploading.');
    const d=await this.store.draft(i.guildId,i.user.id);
    check(d?.session_id===sid&&d.revision===revision&&(!stamp||draftStamp(d)===stamp),'STALE','Your SHOW selection or preview changed. Reopen SHOW and select the story again.');
    const p=await this.store.one('SELECT * FROM madlib_publications WHERE session_id=$1',[sid]);
    check(!p||p.status==='prepared','PUBLISHED','This story already has a sent or uncertain publication. Use SHOW for its status.');
    return {session:s,draft:d};
  }
  async openUpload(i,c){
    check(i.isMessageComponent?.()&&!i.isModalSubmit?.(),'UPLOAD_MODAL','Use the Upload Image button after selecting a story in SHOW.');
    check(this.uploadModals.available,'UPLOAD_MODAL','The upload form is unavailable. Use /madlib-upload for the same saved story.');
    let timer;
    try{
      const selected=await Promise.race([(async()=>{
        await this.ready;check(this.available,'UNAVAILABLE','Mad Libs is unavailable. Your story is saved.');
        const access=await this.access(i);await eligible(this.deps,this.cfg,access);
        return this.checkUploadDraft(i,c.id,c.revision);
      })(),new Promise((_,reject)=>{timer=setTimeout(()=>reject(new core.MadlibError('SLOW','Checks are taking too long to open the form. Try Upload Image again, or use /madlib-upload.')),1400);})]);
      // A modal must use this button's initial response; never defer before showModal.
      try{await i.showModal(buildUploadModal(selected.draft,i.user.id,uploadLimit(this.cfg.MADLIB_MAX_IMAGE_BYTES,i.attachmentSizeLimit)));}
      catch(_){throw new core.MadlibError('UPLOAD_MODAL','Discord could not open the upload form. Reopen SHOW or use /madlib-upload for the same saved story.');}
    }finally{clearTimeout(timer);}
  }
  async saveUploadedImage(i,access,sid,revision,attachment,rawLimit=null,stamp=null){
    await eligible(this.deps,this.cfg,access);
    const {session:s,draft}=await this.checkUploadDraft(i,sid,revision,stamp);
    const image=await this.images(attachment,uploadLimit(this.cfg.MADLIB_MAX_IMAGE_BYTES,i.attachmentSizeLimit,rawLimit));
    // The database rechecks the selection under lock AFTER download/decode finishes.
    const saved=await this.store.stage(i.guildId,i.user.id,s.id,revision,image,draft.updated_at);
    return this.preview(i,s,saved,image);
  }
  async upload(i,access){
    await eligible(this.deps,this.cfg,access);const supplied=i.options.getString('story_id');
    if(supplied){const selection=await this.store.chooseDraft(i.guildId,i.user.id,supplied);if(selection.publication)return this.publicationStatus(i,selection.publication);}
    const draft=await this.store.draft(i.guildId,i.user.id);check(draft,'DRAFT','Select your completed story through SHOW first, or supply your story_id.');
    return this.saveUploadedImage(i,access,draft.session_id,draft.revision,i.options.getAttachment('image',true));
  }
  async submitUpload(i,access,c){
    const target=modalTarget(c),received=this.uploadModals.take(i);
    return this.saveUploadedImage(i,access,target.sessionId,c.revision,received.attachment,received.limit,target.stamp);
  }
  async publicationStatus(i,p){
    check(p.guild_id===i.guildId&&p.user_id===i.user.id,'OWNER','That publication is not yours in this server.');
    const content=p.status==='published'?'Published! One lifetime post is recorded for this story.':p.status==='deleted'?'The post was deleted. Its publication identity and past rewards remain recorded; it cannot be reposted for another reward run.':p.status==='prepared'?'This publication has not been sent yet. Use Publish saved preview to retry its preflight checks.':'Publication needs confirmation or admin review. Do not upload/repost the same story; the original send will be reconciled.';
    const controls=[button('SHOW / History','history','0',0,i.user.id)];
    if(p.message_id&&p.status==='published')controls.unshift(new ButtonBuilder().setLabel('View Post').setStyle(ButtonStyle.Link).setURL(`https://discord.com/channels/${p.guild_id}/${p.channel_id}/${p.message_id}`));
    if(p.status==='prepared')controls.unshift(button('Publish saved preview','publish',p.session_id,p.upload_revision,i.user.id,ButtonStyle.Success));
    return edit(i,{content:`${content}\nPublication: \`${p.id}\`\nState: ${p.status}`,components:[row(...controls)]});
  }
  async publish(i,access,c){
    await eligible(this.deps,this.cfg,access);const s=await this.store.owned(i.guildId,i.user.id,c.id);check(s.state==='completed','STATE','Finish the story before publishing.');
    const existing=await this.store.one('SELECT * FROM madlib_publications WHERE session_id=$1',[s.id]);if(existing&&existing.status!=='prepared')return this.publicationStatus(i,existing);
    if(existing){const d=await this.store.draft(i.guildId,i.user.id);check(existing.upload_revision===c.revision&&d?.session_id===s.id&&d.revision===c.revision,'STALE','Your approved preview changed. Open SHOW and review the latest image before publishing.');}
    const target=existing?.channel_id||this.cfg.MADLIB_CHANNEL_ID;await memberAccess(this.deps,i.guildId,i.user.id,target);await this.publishing.target(i.guildId,target,existing?.emoji_id||this.cfg.MADLIB_REACTION_EMOJI_ID);
    const settings=await this.deps.getGuildSettings(i.guildId),p=existing||await this.store.publicationIntent(i.guildId,i.user.id,c.id,c.revision,this.cfg,settings),published=await this.publishing.publish(p);
    if(published?.status==='published')this.workers.remember(published);return this.publicationStatus(i,published);
  }
  async admin(i,access){
    await this.requireAdmin(i,access);const action=i.options.getString('action',true);check(ADMIN_ACTIONS.includes(action),'ACTION','Unknown administrative action.');
    const record=i.options.getString('record'),revision=i.options.getInteger('revision'),evidence=i.options.getString('evidence');let data;
    if(action==='status')data=await this.store.status(i.guildId);
    else if(action==='inspect'){check(record,'RECORD','Supply an operation or publication ID.');data=await this.store.operation(record,i.guildId)||await this.store.publication(record);check(data&&data.guild_id===i.guildId,'RECORD','No such record in this server.');}
    else{
      check(Number.isSafeInteger(revision)&&revision>=0,'REVISION','Inspect/status first, then supply the latest revision.');
      if(action==='link-publication'){const messageId=i.options.getString('message_id');check(core.snowflake(messageId),'MESSAGE','Supply the existing bot message ID.');data=await this.publishing.linkReviewed(i.guildId,i.user.id,record,revision,messageId,evidence);}
      else if(action==='retry-publication')data=await this.publishing.retryReviewed(i.guildId,i.user.id,record,revision,evidence);
      else data=await this.store.admin({guild:i.guildId,actor:i.user.id,action,record:record||i.guildId,revision,evidence});
      await this.deps.postAdminSystemLog({guildId:i.guildId,category:'Mad Lib Admin',message:`Actor: ${i.user.id}\nAction: ${action}\nRecord: ${safeDiscord(record||i.guildId)}\nReason/evidence: ${safeDiscord(evidence||'')}`}).catch(()=>{});
    }
    return edit(i,{content:`Mad Lib admin ${action}. State changes are audited. A retry queues only a confirmed-not-sent operation; mark-sent never calls DRIP.\nReview the attached record and use its latest revision for changes.`,files:[new AttachmentBuilder(Buffer.from(JSON.stringify(data,null,2),'utf8'),{name:'madlib-admin-record.json'})]});
  }
  async openAnswer(i,c){
    // Modal opening consumes the initial response; never defer and then showModal.
    let timer;try{
      const s=await Promise.race([(async()=>{await this.ready;check(this.available,'UNAVAILABLE','Mad Libs is unavailable.');await this.access(i);const s=await this.store.owned(i.guildId,i.user.id,c.id);check(s.state==='active'&&s.revision===c.revision,'STALE','That question changed. Use Resume.');return s;})(),new Promise((_,reject)=>{timer=setTimeout(()=>reject(new core.MadlibError('SLOW','Validation is taking too long. Press Resume and try Answer again.')),1400);})]);
      const q=s.template.questions[s.step];check(q,'STATE','No active question.');
      const input=new TextInputBuilder().setCustomId('answer').setLabel(q.label).setStyle(TextInputStyle.Short).setRequired(true).setMinLength(1).setMaxLength(q.maxLength).setPlaceholder(q.example.slice(0,100));if(s.answers[q.key])input.setValue(s.answers[q.key]);
      return await i.showModal(new ModalBuilder().setCustomId(component('submit',s.id,s.revision,s.user_id)).setTitle(`Question ${s.step+1} of ${s.template.questions.length}`).addComponents(new ActionRowBuilder().addComponents(input)));
    }finally{if(timer)clearTimeout(timer);}
  }
  async handleInteraction(i){
    const slash=COMMANDS.includes(i.commandName),ours=typeof i.customId==='string'&&i.customId.startsWith('madlib:');if(!slash&&!ours)return false;let c;
    try{
      if(ours){c=parseComponent(i.customId);check(COMPONENT_ACTIONS.has(c.action),'ACTION','That control is not recognized. Reopen PLAY or SHOW.');check(c.owner==='public'||c.owner===i.user.id,'OWNER','That private control belongs to another member.');}
      if(!core.enabled(this.env)){await acknowledge(i);await edit(i,{content:'Squig Mad Libs is currently disabled. Saved stories and pending payment obligations have not been erased.'});return true;}
      if(c&&['upload','replace'].includes(c.action)){check(c.owner!=='public','OWNER','Upload controls are private.');await this.openUpload(i,c);return true;}
      if(c?.action==='answer'){check(c.owner!=='public','OWNER','Answer controls are private.');await this.openAnswer(i,c);return true;}
      await acknowledge(i);await this.ready;check(this.available,'UNAVAILABLE',`Squig Mad Libs is unavailable (${this.failure||'initializing'}). Existing UglyBot features are unchanged.`);
      const access=await this.access(i);if(c)await this.ensurePanel(i,c);
      if(slash){if(i.commandName==='madlib')await this.panel(i,access);else if(i.commandName==='madlib-upload')await this.upload(i,access);else if(i.commandName==='madlib-history')await this.history(i);else await this.admin(i,access);return true;}
      const g=i.guildId,u=i.user.id;
      if(c.action==='play'||c.action==='accept')await this.begin(i,access,c.action==='accept'?c.id:null);
      else if(['show','history'].includes(c.action))await this.history(i,c.action==='show'?0:Number(c.id));
      else if(c.action==='select'){check(i.values?.length===1,'SELECT','Choose one story.');await this.selected(i,i.values[0]);}
      else if(c.action==='publish')await this.publish(i,access,c);
      else if(c.action==='upload-submit')await this.submitUpload(i,access,c);
      else if(c.action==='cancel-image'){await this.store.cancelUpload(g,u,c.id,c.revision);await edit(i,{content:'Private upload cancelled. Your completed story remains in SHOW; no public post was made.'});}
      else{
        const s=await this.store.owned(g,u,c.id);
        if(c.action==='resume')await this.progress(i,s);
        else if(c.action==='submit'||c.action==='back'){const changed=await this.store.editSession(g,u,s.id,c.revision,c.action==='submit'?'answer':'back',c.action==='submit'?i.fields.getTextInputValue('answer'):undefined);await this.progress(i,changed,{separateStory:changed.state==='completed'});}
        else if(c.action==='exit'){check(s.revision===c.revision,'STALE','Saved progress changed; Resume reads the latest version.');await edit(i,{content:'Progress saved. Exit does not abandon or charge the play. Use Resume whenever you return.',components:[row(button('Resume','resume',s.id,s.revision,u,ButtonStyle.Primary),button('Abandon this play','abandon',s.id,s.revision,u))]});}
        else if(c.action==='abandon'){check(s.revision===c.revision&&['active','payment_failed'].includes(s.state),'STALE','Resume for the latest state. Uncertain payments cannot be abandoned.');await edit(i,{content:`**Abandon this play?** ${s.state==='payment_failed'?'No completed debit is recorded.':'The free allowance or confirmed paid play has already been used. Abandoning will not refund it or unlock another free reroll.'} Exit/Resume is the safe way to take a break.`,components:[row(button('Keep / Resume','resume',s.id,s.revision,u),button('Confirm abandonment','abandon-confirm',s.id,s.revision,u,ButtonStyle.Danger))]});}
        else if(c.action==='abandon-confirm'){await this.store.editSession(g,u,s.id,c.revision,'abandon');await edit(i,{content:'The play was abandoned. Your free-play clock and any confirmed payment records were preserved.'});}
        else{
          check(s.state==='completed','STATE','Finish your story before exporting.');
          if(c.action==='prompt')await this.results(i,s,false);
          else if(c.action==='story')await edit(i,{embeds:storyEmbeds(s,`Classic Mad Lib Story — ${s.template.title}`),components:resultControls(s)});
          else if(c.action==='copy')await edit(i,{content:s.prompt.length<=1900?s.prompt:'The complete plain-text prompt is attached. Open the file and copy its full contents into your image tool.',files:[new AttachmentBuilder(Buffer.from(s.prompt,'utf8'),{name:`madlib-${s.id}-prompt.txt`})],components:resultControls(s)});
          else if(c.action==='export-prompt'||c.action==='export-story'){const kind=c.action==='export-prompt'?'prompt':'story';await edit(i,{content:`Complete ${kind} attached as UTF-8 plain text.`,files:[new AttachmentBuilder(Buffer.from(s[kind],'utf8'),{name:`madlib-${s.id}-${kind}.txt`})],components:resultControls(s)});}
          else check(false,'ACTION','Unsupported control.');
        }
      }
    }catch(error){
      this.log(error instanceof core.MadlibError?error.code:'ACTION_FAILED');
      try{
        if(isPrivateMessage(i)&&(i.deferred||i.replied))await follow(i,{content:errorMessage(error),components:[row(button('SHOW / History','history','0',0,i.user.id),...(c?.id&&/^[0-9a-f-]{36}$/.test(c.id)?[button('Resume','resume',c.id,0,i.user.id)]:[]))]});
        else{await acknowledge(i);await edit(i,{content:errorMessage(error)});}
      }catch(_){ /* Expired interaction token: persistent progress is accessible from a fresh panel. */ }
    }
    return true;
  }
}
function initMadlib(deps,options){instance?.stop();instance=new MadlibFeature(deps,options);return instance.ready;}
async function handleInteraction(i){
  if(instance)return instance.handleInteraction(i);if(!COMMANDS.includes(i.commandName)&&!i.customId?.startsWith('madlib:'))return false;
  try{await acknowledge(i);await edit(i,{content:'Squig Mad Libs is not initialized. Other UglyBot features are unaffected.'});}catch(_){}return true;
}
module.exports={buildMadlibSlashCommands,initMadlib,handleInteraction,strictTransfer,isEnabled:core.enabled,
  ensureMadlibTables:()=>instance?.ready||Promise.resolve(false),startMadlibWorkers:()=>instance?.start(),stopMadlibWorkers:()=>instance?.stop(),
  MadlibFeature,privatePayload,acknowledge,resultControls,COMPONENT_ACTIONS};