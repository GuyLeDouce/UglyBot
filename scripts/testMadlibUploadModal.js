'use strict';
// No bot login or live Discord/DRIP calls. Exercise real SDK parsing/serialization with fake REST.
const {Client,ModalSubmitInteraction,ButtonInteraction,version,Collection}=require('discord.js');
const {assert,harness,errorCode,IDS,fakeDiscord,interaction,session,modelStore,PermissionsBitField}=require('./madlibTestUtils');
const core=require('../modules/madlibCore'),templates=require('../modules/madlibTemplates');
const {MadlibFeature}=require('../modules/madlib');
const {UploadModalBridge,buildUploadModal,draftStamp,modalTarget,rawAttachment,uploadLimit,NATIVE_UPLOADS}=require('../modules/madlibUploadModal');
const run=harness('upload-modal'),env={MADLIB_ENABLED:'true',MADLIB_ALLOWED_ROLE_IDS:IDS.role};
const FILE='600000000000000001';let sequence=700000000000000001n;
const file={id:FILE,url:`https://cdn.discordapp.com/ephemeral-attachments/${IDS.channel}/${FILE}/squig.png?ex=123&is=456&hm=TEST`,size:1024,content_type:'image/png'};
function packet(customId,{user=IDS.user,guild=IDS.guild,limit=8388608}={}){
  return {t:'INTERACTION_CREATE',d:{id:String(sequence++),type:5,application_id:IDS.bot,token:'FAKE_TEST_TOKEN',version:1,entitlements:[],authorizing_integration_owners:{'0':guild},guild_id:guild,channel_id:IDS.channel,channel:{id:IDS.channel,type:0,guild_id:guild},app_permissions:'0',member:{user:{id:user,username:'Tester',discriminator:'0',avatar:null},permissions:'0',roles:[]},attachment_size_limit:limit,
    data:{custom_id:customId,components:[{type:18,component:{type:19,custom_id:'image',values:[FILE]}}],resolved:{attachments:{[FILE]:{...file}}}}}};
}
function readEdit(i){return i.calls.filter(([k])=>k==='editReply').at(-1)?.[1];}
function readText(i){return i.calls.filter(([k])=>['editReply','followUp'].includes(k)).map(x=>x[1].content).join(' ');}
function complete(){const s=session(templates[0]);Object.assign(s,core.render(s.template,Object.fromEntries(s.template.questions.map(q=>[q.key,q.example]))));s.state='completed';s.completed_at=new Date();return s;}
async function setup({images=null}={}){
  const f=fakeDiscord(),s=complete(),store=modelStore(s);let draft={guild_id:IDS.guild,user_id:IDS.user,session_id:s.id,revision:0,updated_at:new Date('2026-09-13T00:00:00Z')},staged=null,pub=null,downloads=0,writes=0;
  store.chooseDraft=async(g,u,sid)=>{const selected=await store.owned(g,u,sid);core.check(selected.state==='completed','STATE','Not finished');if(pub&&pub.status!=='prepared')return {session:selected,publication:pub};draft={...draft,session_id:sid,revision:draft.revision+1,updated_at:new Date(draft.updated_at.getTime()+1)};return {session:selected,draft:{...draft}};};
  store.draft=async()=>draft&&({...draft});store.upload=async()=>staged;
  store.one=async()=>pub;
  store.stage=async(g,u,id,revision,image,stamp)=>{
    await store.owned(g,u,id);core.check(!pub||pub.status==='prepared','PUBLISHED','Published');
    core.check(draft?.session_id===id&&draft.revision===revision&&(!stamp||draft.updated_at.getTime()===new Date(stamp).getTime()),'STALE','Selection changed');
    writes++;draft={...draft,revision:revision+1,updated_at:new Date(draft.updated_at.getTime()+1)};staged={...image,revision:draft.revision};return {...draft};
  };
  const app=new MadlibFeature(f.deps,{env,templates,store,images:async(...args)=>{downloads++;return images?images(...args):{bytes:Buffer.from('fake-validated-png'),media_type:'image/png',width:64,height:64};}});
  await app.ready;
  return {...f,s,store,app,get draft(){return draft;},set draft(value){draft=value;},get staged(){return staged;},get downloads(){return downloads;},get writes(){return writes;},set pub(value){pub=value;}};
}
function buttonI(f,action='upload',user=IDS.user){return interaction({customId:core.component(action,f.s.id,f.draft.revision,IDS.user),privateMessage:true,user});}
function modalI(f,data){
  const p=structuredClone(data),i=interaction({customId:p.d.data.custom_id,privateMessage:false,user:p.d.member.user.id,guild:p.d.guild_id});
  Object.assign(i,{id:p.d.id,applicationId:p.d.application_id,isModalSubmit:()=>true,isMessageComponent:()=>false,attachmentSizeLimit:p.d.attachment_size_limit});
  i.fields={getField:()=>({type:19}),getUploadedFiles:()=>new Collection([[FILE,{...p.d.data.resolved.attachments[FILE],contentType:p.d.data.resolved.attachments[FILE]?.content_type}]])};
  f.client.emit('raw',p);return i;
}
(async()=>{
  console.log(`SDK compatibility: discord.js=${version}, native uploads=${NATIVE_UPLOADS}`);
  await run.test('selecting an unpublished story offers Upload Image bound to that exact saved draft',async()=>{
    const f=await setup(),i=interaction({customId:core.component('select','0',0,IDS.user),privateMessage:true,values:[f.s.id]});
    await f.app.handleInteraction(i);const p=readEdit(i),upload=p.components[0].toJSON().components[0];
    assert.equal(upload.label,'Upload Image');assert.equal(upload.style,1);assert.equal(upload.custom_id,core.component('upload',f.s.id,f.draft.revision,IDS.user));assert(p.content.includes('/madlib-upload'));assert.equal(f.downloads,0);assert.equal(f.messages.length,0);f.app.stop();
  });
  await run.test('Upload Image opens a real one-file modal with the initial response and creates no upload or charge',async()=>{
    const f=await setup(),i=buttonI(f);await f.app.handleInteraction(i);assert.deepEqual(i.calls.map(x=>x[0]),['showModal']);
    const m=i.calls[0][1];assert.equal(m.components[0].type,18);assert.deepEqual(m.components[0].component,{type:19,custom_id:'image',min_values:1,max_values:1,required:true});
    const c=core.parseComponent(m.custom_id);assert.equal(modalTarget(c).sessionId,f.s.id);assert.equal(c.owner,IDS.user);assert.equal(f.draft.revision,0);assert.equal(f.staged,null);assert.equal(f.downloads,0);f.app.stop();
  });
  await run.test('SDK button showModal serializes raw file/label payload without unsupported builders',async()=>{
    const client=new Client({intents:[]}),f=await setup(),p=packet('unused');let request;
    p.d.type=3;p.d.data={component_type:2,custom_id:core.component('upload',f.s.id,0,IDS.user)};p.d.message={id:IDS.message,channel_id:IDS.channel,type:0,author:p.d.member.user,content:'',flags:64,attachments:[],embeds:[],components:[],timestamp:new Date().toISOString()};
    client.rest.post=async(route,options)=>{request={route,...options};return {};};
    const i=new ButtonInteraction(client,p.d),m=buildUploadModal(f.draft,IDS.user,8388608);await i.showModal(m);
    assert.equal(request.body.type,9);assert.equal(request.body.data.components[0].component.type,19);assert.equal(request.auth,false);assert(i.replied);f.app.stop();await client.destroy();
  });
  await run.test('real SDK gateway construction and attachment resolution work on the installed baseline or modern parser',async()=>{
    const f=await setup(),client=new Client({intents:[]}),bridge=new UploadModalBridge(client),m=buildUploadModal(f.draft,IDS.user,8388608).toJSON(),p=packet(m.custom_id);let emitted;
    client.user=client.users._add({id:IDS.bot,username:'TestBot',discriminator:'0',bot:true});client.once('interactionCreate',i=>{emitted=i;});client.emit('raw',p);client.actions.InteractionCreate.handle(p.d);
    assert(emitted instanceof ModalSubmitInteraction);const r=bridge.take(emitted);assert.equal(r.attachment.url,file.url);assert.equal(r.attachment.contentType,'image/png');
    if(!NATIVE_UPLOADS){assert.deepEqual(p.d.data.components,[]);assert.equal(r.limit,8388608);assert.equal(bridge.pending.size,0);}else assert.equal(p.d.data.components[0].type,18);
    bridge.stop();f.app.stop();await client.destroy();
  });
  await run.test('submit uses the existing validation/staging/preview and never publishes automatically',async()=>{
    let seen;const f=await setup({images:async(a,max)=>{seen={a,max};return {bytes:Buffer.from('validated'),media_type:'image/png',width:64,height:64};}});
    const m=buildUploadModal(f.draft,IDS.user,8388608).toJSON(),i=modalI(f,packet(m.custom_id,{limit:2097152}));
    await f.app.handleInteraction(i);assert.equal(i.calls[0][0],'deferReply');assert.equal(i.calls[0][1].ephemeral,true);assert.equal(seen.max,2097152);assert.equal(seen.a.url,file.url);assert.equal(f.writes,1);assert.equal(f.messages.length,0);
    const preview=readEdit(i);assert(preview.content.includes('Nothing has been published'));assert.deepEqual(preview.components[0].toJSON().components.map(c=>c.label),['Publish','Replace Image','Cancel']);assert.equal(preview.files.length,1);f.app.stop();
  });
  await run.test('Replace Image opens the same picker and dismissing it keeps the old staged image',async()=>{
    const f=await setup();await f.store.stage(IDS.guild,IDS.user,f.s.id,0,{bytes:Buffer.from('original'),media_type:'image/png',width:64,height:64},f.draft.updated_at);
    const before=Buffer.from(f.staged.bytes),i=buttonI(f,'replace');await f.app.handleInteraction(i);assert.deepEqual(i.calls.map(x=>x[0]),['showModal']);assert.deepEqual(f.staged.bytes,before);assert.equal(f.writes,1);f.app.stop();
  });
  await run.test('selecting a different story or reopening SHOW invalidates an old upload modal',async()=>{
    for(const change of ['revision','story']){const f=await setup(),m=buildUploadModal(f.draft,IDS.user,8388608).toJSON();f.draft={...f.draft,...(change==='revision'?{revision:1}:{session_id:core.id()})};
      const i=modalI(f,packet(m.custom_id));await f.app.handleInteraction(i);assert.equal(f.downloads,0);assert.equal(f.writes,0);assert(readText(i).includes('selection'));f.app.stop();}
  });
  await run.test('cancel then reselect cannot reuse a modal with an old draft timestamp even if the revision resets',async()=>{
    const f=await setup(),m=buildUploadModal(f.draft,IDS.user,8388608).toJSON();f.draft={...f.draft,updated_at:new Date(f.draft.updated_at.getTime()+1000)};
    await f.app.handleInteraction(modalI(f,packet(m.custom_id)));assert.equal(f.downloads,0);assert.equal(f.writes,0);f.app.stop();
  });
  await run.test('cross-user cross-guild and forged public upload controls disclose no image and perform no writes',async()=>{
    const f=await setup(),thief=buttonI(f,'upload',IDS.other);await f.app.handleInteraction(thief);assert(!thief.calls.some(x=>x[0]==='showModal'));
    for(const attrs of [{user:IDS.other},{guild:IDS.otherGuild}]){const m=buildUploadModal(f.draft,IDS.user,8388608).toJSON();await f.app.handleInteraction(modalI(f,packet(m.custom_id,attrs)));}
    const forged=interaction({customId:core.component('upload',f.s.id,0,'public'),privateMessage:true});await f.app.handleInteraction(forged);assert.equal(f.downloads,0);assert.equal(f.writes,0);assert(!forged.calls.some(x=>x[0]==='showModal'));f.app.stop();
  });
  await run.test('eligibility is checked at opening and again at submission; existing history access is unaffected',async()=>{
    const f=await setup(),m=buildUploadModal(f.draft,IDS.user,8388608).toJSON();f.member.roles.cache.clear();
    const b=buttonI(f);await f.app.handleInteraction(b);assert(!b.calls.some(x=>x[0]==='showModal'));await f.app.handleInteraction(modalI(f,packet(m.custom_id)));assert.equal(f.downloads,0);
    const history=interaction({command:'madlib-history'});await f.app.handleInteraction(history);assert(readEdit(history).components);f.app.stop();
  });
  await run.test('published and ambiguous publications refuse upload and replace before downloading',async()=>{
    for(const status of ['published','publishing','needs_review','deleted']){const f=await setup(),m=buildUploadModal(f.draft,IDS.user,8388608).toJSON();f.pub={status};
      const b=buttonI(f,'replace');await f.app.handleInteraction(b);assert(!b.calls.some(x=>x[0]==='showModal'));await f.app.handleInteraction(modalI(f,packet(m.custom_id)));assert.equal(f.downloads,0);assert.equal(f.writes,0);f.app.stop();}
  });
  await run.test('concurrent submits can validate but only one can replace a revision; changes during decoding reject',async()=>{
    const f=await setup(),m=buildUploadModal(f.draft,IDS.user,8388608).toJSON();await Promise.all([f.app.handleInteraction(modalI(f,packet(m.custom_id))),f.app.handleInteraction(modalI(f,packet(m.custom_id)))]);assert.equal(f.writes,1);f.app.stop();
    let g;g=await setup({images:async()=>{g.draft={...g.draft,updated_at:new Date(g.draft.updated_at.getTime()+1000)};return {bytes:Buffer.from('x'),media_type:'image/png',width:64,height:64};}});
    const mm=buildUploadModal(g.draft,IDS.user,8388608).toJSON();await g.app.handleInteraction(modalI(g,packet(mm.custom_id)));assert.equal(g.writes,0);g.app.stop();
  });
  await run.test('bad image leaves the saved story and prior preview untouched',async()=>{
    const f=await setup({images:async()=>{throw new core.MadlibError('IMAGE','Rejected by the same raster decoder');}});const before=f.store.read(),m=buildUploadModal(f.draft,IDS.user,8388608).toJSON();await f.app.handleInteraction(modalI(f,packet(m.custom_id)));
    assert.equal(f.writes,0);assert.deepEqual(f.store.read(),before);assert.equal(f.messages.length,0);f.app.stop();
  });
  await run.test('raw adapter rejects missing/multiple/mismatched attachments and retains no tokens or raw packets',()=>{
    const base=packet('madlib:upload-submit:example:0:'+IDS.user).d.data;assert.equal(rawAttachment(base).id,FILE);
    for(const mutate of [d=>d.components=[],d=>d.components[0].component.values=[],d=>d.components[0].component.values=[FILE,FILE],d=>d.components[0].component.type=4,d=>d.resolved.attachments={},d=>d.resolved.attachments[FILE].id=IDS.other,d=>d.resolved.attachments[FILE].url='x'.repeat(4097)]){const d=structuredClone(base);mutate(d);assert.throws(()=>rawAttachment(d),errorCode('UPLOAD_MODAL'));}
    const client=fakeDiscord().client,bridge=new UploadModalBridge(client,{native:false}),p=packet('madlib:upload-submit:example:0:'+IDS.user);client.emit('raw',p);const text=JSON.stringify([...bridge.pending]);assert(!text.includes('FAKE_TEST_TOKEN'));assert(!text.includes('member'));bridge.stop();
  });
  await run.test('legacy bridge is bounded consumed once identity-bound restart-safe and removable',()=>{
    let now=0;const client=fakeDiscord().client,b=new UploadModalBridge(client,{native:false,now:()=>now,capacity:2,ttl:50}),p=packet('madlib:upload-submit:x:0:'+IDS.user);
    b.start();assert.equal(client.listenerCount('raw'),1);client.emit('raw',p);client.emit('raw',packet(p.d.data.custom_id));client.emit('raw',packet(p.d.data.custom_id));assert.equal(b.pending.size,2);
    const i={id:p.d.id,customId:p.d.data.custom_id,isModalSubmit:()=>true,guildId:IDS.guild,channelId:IDS.channel,user:{id:IDS.user},applicationId:IDS.bot};assert.equal(b.take(i).attachment.id,FILE);assert.throws(()=>b.take(i),errorCode('UPLOAD_MODAL'));
    now=100;b.prune();assert.equal(b.pending.size,0);b.stop();assert.equal(client.listenerCount('raw'),0);
    const restarted=new UploadModalBridge(client,{native:false}),p2=packet(i.customId);client.emit('raw',p2);assert.equal(restarted.take({...i,id:p2.d.id}).attachment.id,FILE);restarted.stop();
  });
  await run.test('unrelated events and answer modals are byte-for-byte unchanged by the legacy bridge',()=>{
    const client=fakeDiscord().client,b=new UploadModalBridge(client,{native:false});
    for(const p of [{t:'MESSAGE_REACTION_ADD',d:{emoji:{id:IDS.emoji}}},packet('madlib:submit:old-answer:0:'+IDS.user),packet('other:upload-submit:anything')]){const old=JSON.stringify(p);client.emit('raw',p);assert.equal(JSON.stringify(p),old);}assert.equal(b.pending.size,0);b.stop();
  });
  await run.test('disabled feature acknowledges an existing file modal privately without schema decode or staging',async()=>{
    const f=await setup(),m=buildUploadModal(f.draft,IDS.user,8388608).toJSON();f.app.env={MADLIB_ENABLED:'false'};const i=modalI(f,packet(m.custom_id));await f.app.handleInteraction(i);assert(readText(i).includes('disabled'));assert.equal(f.downloads,0);assert.equal(f.writes,0);f.app.stop();
  });
  await run.test('failed or slow modal openings cannot later send an unrequested form',async()=>{
    const f=await setup(),b=buttonI(f);b.showModal=async()=>{throw Error('Discord unavailable');};await f.app.handleInteraction(b);assert.equal(f.downloads,0);assert.equal(f.writes,0);
    const old=f.store.owned.bind(f.store);f.store.owned=async(...args)=>{await new Promise(r=>setTimeout(r,1550));return old(...args);};const slow=buttonI(f);await f.app.handleInteraction(slow);await new Promise(r=>setTimeout(r,200));assert(!slow.calls.some(x=>x[0]==='showModal'));assert(readText(slow).includes('/madlib-upload'));f.app.stop();
  });
  await run.test('effective limits are conservative and max-length custom IDs fit the Discord contract',()=>{
    assert.equal(uploadLimit(8388608,2097152,1048576),1048576);assert.equal(uploadLimit(8388608,0,-1),8388608);assert.equal(uploadLimit(8388608,'1024',NaN),8388608);
    const m=buildUploadModal({session_id:core.id(),revision:999999999,updated_at:new Date()},'12345678901234567890',8388608).toJSON();assert(m.custom_id.length<=100);assert(m.components[0].description.length<=100);assert.equal(core.parseComponent(m.custom_id).revision,999999999);
  });
  run.done();
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
