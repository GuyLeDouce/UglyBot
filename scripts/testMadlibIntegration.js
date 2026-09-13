'use strict';
const fs=require('node:fs'),path=require('node:path');
const {assert,harness,errorCode,IDS,fakeDiscord,interaction,session,modelStore,Collection,PermissionFlagsBits,PermissionsBitField}=require('./madlibTestUtils');
const core=require('../modules/madlibCore');const {MadlibFeature,buildMadlibSlashCommands}=require('../modules/madlib');
const {MadlibPublishing,publicPayload,marker}=require('../modules/madlibPublishing');const {MadlibWorkers}=require('../modules/madlibWorkers');
const {eligible,reactorAccess,memberAccess}=require('../modules/madlibAccess');const templates=require('../modules/madlibTemplates');const integration=require('./installMadlibIntegration');
const run=harness('integration'),env={MADLIB_ENABLED:'true',MADLIB_ALLOWED_ROLE_IDS:IDS.role};
async function feature(seed=session(templates[0]),extra={}){const f=fakeDiscord(),store=modelStore(seed),app=new MadlibFeature(f.deps,{env,store,templates,...extra});await app.ready;return {app,store,...f};}
const lastEdit=i=>i.calls.filter(([name])=>name==='editReply').at(-1)?.[1];
(async()=>{
  await run.test('disabled mode touches no schema and leaves unrelated interactions untouched',async()=>{
    let db=0;const a=new MadlibFeature({}, {env:{MADLIB_ENABLED:'false'},store:{ensureMadlibTables:async()=>{db++;}}});assert.equal(await a.ready,false);assert.equal(db,0);assert.deepEqual(buildMadlibSlashCommands({}),[]);
    const unrelated=interaction({command:'check-balance'});assert.equal(await a.handleInteraction(unrelated),false);assert.deepEqual(unrelated.calls,[]);
    const old=interaction({customId:core.component('play','panel',0,'public')});assert.equal(await a.handleInteraction(old),true);assert.equal(old.calls[0][1].ephemeral,true);assert(lastEdit(old).content.includes('disabled'));assert.equal(db,0);
  });
  await run.test('invalid templates disable only Mad Libs with private diagnostics',async()=>{
    const f=fakeDiscord();let db=0;const a=new MadlibFeature(f.deps,{env,templates:[],store:{ensureMadlibTables:async()=>{db++;}}});assert.equal(await a.ready,false);assert.equal(db,0);
    const i=interaction({command:'madlib-history'});await a.handleInteraction(i);assert(lastEdit(i).content.includes('unavailable'));assert.equal(await a.handleInteraction(interaction({command:'duel'})),false);
  });
  await run.test('admin panel preserves PLAY SHOW; upload and history remain normal-player commands',async()=>{
    const defs=buildMadlibSlashCommands(env);assert.deepEqual(defs.map(c=>c.name),['madlib','madlib-upload','madlib-admin','madlib-history']);assert.equal(defs[1].default_member_permissions,undefined);assert.equal(defs[3].default_member_permissions,undefined);assert.equal(defs[0].options.length,0);assert.equal(defs[1].options[0].type,11);assert.equal(defs[1].options[0].required,true);
    const {app,messages}=await feature(),i=interaction({command:'madlib'});await app.handleInteraction(i);assert.equal(i.calls[0][0],'deferReply');assert.equal(messages.length,1);assert.equal(messages[0].embeds[0].toJSON().title,'SQUIG MAD LIBS');assert.deepEqual(messages[0].components[0].toJSON().components.map(b=>b.label),['PLAY','SHOW']);
    const denied=await feature();denied.member.permissions=new PermissionsBitField([]);const j=interaction({command:'madlib'});await denied.app.handleInteraction(j);assert.equal(denied.messages.length,0);assert(lastEdit(j).content.includes('administrator'));
  });
  await run.test('holder access reuses verified links and rules; outages are not zero holdings',async()=>{
    const f=fakeDiscord(),access=await memberAccess(f.deps,IDS.guild,IDS.user,IDS.channel);await eligible(f.deps,core.config({}),access);
    let ownership=0;f.member.roles.cache.clear();f.deps.getOwnedTokenIdsForContractMany=async(...args)=>{ownership++;assert.equal(args[3].suppressErrors,false);return ['1'];};await eligible(f.deps,core.config({}),access);assert.equal(ownership,1);
    f.deps.getOwnedTokenIdsForContractMany=async()=>{throw new Error('provider unavailable');};await assert.rejects(()=>eligible(f.deps,core.config({}),access),errorCode('OWNERSHIP_UNAVAILABLE'));
    f.deps.getOwnedTokenIdsForContractMany=async()=>[];await assert.rejects(()=>eligible(f.deps,core.config({}),access),errorCode('ELIGIBILITY'));
    f.member.roles.cache.set(IDS.role,{id:IDS.role});f.deps.getWalletLinks=async()=>{throw new Error('override must not create registration');};await eligible(f.deps,core.config({MADLIB_ALLOWED_ROLE_IDS:IDS.role}),access);
  });
  await run.test('PLAY resumes existing progress without another begin or charge; scenario stays hidden',async()=>{
    const {app,store}=await feature();store.begin=async()=>{throw new Error('must resume');};const s=store.read(),i=interaction({customId:core.component('play','home',0,IDS.user),privateMessage:true});await app.handleInteraction(i);const data=lastEdit(i);assert(data.embeds[0].toJSON().title.startsWith('Question 1 of'));assert(!JSON.stringify(data).includes(s.template.title));assert.equal(i.calls[0][0],'deferUpdate');
  });
  await run.test('one-input modal uses initial response; submit saves once and never chains another modal',async()=>{
    const {app,store}=await feature(),s=store.read();const open=interaction({customId:core.component('answer',s.id,s.revision,IDS.user),privateMessage:true});await app.handleInteraction(open);assert.deepEqual(open.calls.map(c=>c[0]),['showModal']);assert.equal(open.calls[0][1].components.length,1);assert.equal(open.calls[0][1].components[0].components.length,1);
    const submit=interaction({customId:core.component('submit',s.id,s.revision,IDS.user),privateMessage:true,answer:'curiously happy'});await app.handleInteraction(submit);assert.equal(store.read().step,1);assert.equal(store.read().answers.emotion,'curiously happy');assert(!submit.calls.some(c=>c[0]==='showModal'));assert.equal(lastEdit(submit).embeds[0].toJSON().title,'Question 2 of 6');
    const stale=interaction({customId:core.component('submit',s.id,s.revision,IDS.user),privateMessage:true,answer:'overwrite'});await app.handleInteraction(stale);assert.equal(store.read().answers.emotion,'curiously happy');assert(stale.calls.some(c=>c[0]==='followUp'));
  });
  await run.test('invalid answers preserve controls; cross-user and forged public controls expose nothing',async()=>{
    const {app,store}=await feature(),s=store.read();const blank=interaction({customId:core.component('submit',s.id,0,IDS.user),privateMessage:true,answer:' '});await app.handleInteraction(blank);assert.equal(store.read().step,0);assert(blank.calls.some(c=>c[0]==='followUp'));
    const thief=interaction({customId:core.component('answer',s.id,0,IDS.user),privateMessage:true,user:IDS.other});await app.handleInteraction(thief);assert(!thief.calls.some(c=>c[0]==='showModal'));assert(!JSON.stringify(thief.calls).includes(s.template.title));
    const forged=interaction({customId:core.component('play','fakepanel',0,'public')});await app.handleInteraction(forged);assert(lastEdit(forged).content.includes('not registered'));
  });
  await run.test('completion shows separate private prompt/story and exports full saved bytes after holder loss',async()=>{
    const {app,store}=await feature();for(let n=0;n<6;n++){const s=store.read(),i=interaction({customId:core.component('submit',s.id,s.revision,IDS.user),privateMessage:true,answer:s.template.questions[s.step].example});await app.handleInteraction(i);if(n===5){assert.equal(lastEdit(i).embeds[0].toJSON().title,'Image Prompt');const f=i.calls.find(c=>c[0]==='followUp');assert(f&&f[1].ephemeral);assert(f[1].embeds[0].toJSON().title.startsWith('Classic Mad Lib Story'));}}
    const s=store.read();assert(s.prompt&&s.story&&s.state==='completed');for(const kind of ['prompt','story']){const i=interaction({customId:core.component(`export-${kind}`,s.id,0,IDS.user),privateMessage:true});await app.handleInteraction(i);assert.equal(lastEdit(i).files[0].attachment.toString('utf8'),s[kind]);}
    app.deps.getWalletLinks=async()=>[];app.deps.getOwnedTokenIdsForContractMany=async()=>[];const h=interaction({command:'madlib-history'});await app.handleInteraction(h);assert(lastEdit(h).content.includes('SHOW / History'));assert(!lastEdit(h).content.includes('Connect'));
  });
  await run.test('history pages cap selects at twenty with owner-bound pagination',async()=>{
    const {app,store}=await feature();store.history=async()=>Array.from({length:21},(_,n)=>({...session(templates[n]),state:'completed',completed_at:new Date('2026-09-12T12:00:00Z')}));
    const i=interaction({command:'madlib-history'});await app.handleInteraction(i);const menu=lastEdit(i).components[0].toJSON().components[0];assert.equal(menu.options.length,20);assert(menu.custom_id.endsWith(IDS.user));assert(lastEdit(i).components[1].toJSON().components.some(b=>b.label==='Next'));
  });
  await run.test('normal-player upload is private; only Publish sends the approved story/image publicly',async()=>{
    const s=session(templates[0]);s.state='completed';Object.assign(s,core.render(s.template,Object.fromEntries(s.template.questions.map(q=>[q.key,q.example]))));
    const f=await feature(s,{images:async()=>({bytes:Buffer.from('test-image'),media_type:'image/png',width:64,height:64})}),d={session_id:s.id,revision:1};f.store.draft=async()=>d;f.store.stage=async()=>({...d,revision:2});
    const i=interaction({command:'madlib-upload',options:{image:{id:'file'}}});await f.app.handleInteraction(i);assert.equal(f.messages.length,0);assert.equal(i.calls[0][1].ephemeral,true);assert(lastEdit(i).content.includes('Nothing has been published'));assert.equal(lastEdit(i).files.length,1);assert.deepEqual(lastEdit(i).components[0].toJSON().components.map(b=>b.label),['Publish','Replace Image','Cancel']);
    let sends=0;const p={id:core.id(),session_id:s.id,user_id:IDS.user,guild_id:IDS.guild,status:'prepared',channel_id:IDS.channel,emoji_id:IDS.emoji,upload_revision:2};f.store.publicationIntent=async()=>p;f.app.publishing.target=async()=>({channel:f.channel,emoji:f.emoji});f.app.publishing.publish=async()=>{sends++;return {...p,status:'published',message_id:IDS.message};};
    const publish=interaction({customId:core.component('publish',s.id,2,IDS.user),privateMessage:true});await f.app.handleInteraction(publish);assert.equal(sends,1);assert(lastEdit(publish).content.includes('Published!'));
  });
  await run.test('public post excludes private prompt and mentions; stable marker and supported nonce are present',()=>{
    const s=session(templates[0]);Object.assign(s,core.render(s.template,Object.fromEntries(s.template.questions.map(q=>[q.key,q.example]))));const p={id:core.id(),filename:'image.png',reward:100,reward_cap:0,reactor_roles:[]},payload=publicPayload(p,s,{bytes:Buffer.from('png')}),json=JSON.stringify(payload.embeds.map(e=>e.toJSON()));
    assert(json.includes(marker(p)));assert(!json.includes('SINGLE FROZEN MOMENT'));assert(!json.includes('drip_member_id'));assert.equal(payload.enforceNonce,true);assert(payload.nonce.length<=25);assert.deepEqual(payload.allowedMentions.parse,[]);
  });
  await run.test('uncertain publication is found by its marker without any automatic second send',async()=>{
    const f=fakeDiscord(),s=session(templates[0]);Object.assign(s,core.render(s.template,Object.fromEntries(s.template.questions.map(q=>[q.key,q.example]))));s.state='completed';let sends=0,message;
    let p={id:core.id(),guild_id:IDS.guild,user_id:IDS.user,session_id:s.id,channel_id:IDS.channel,emoji_id:IDS.emoji,filename:'image.png',upload_revision:1,revision:0,status:'prepared',reward:100,reward_cap:0,reactor_roles:[],created_at:new Date()};
    const store={owned:async()=>s,upload:async()=>({bytes:Buffer.from('png'),revision:1}),claimPublication:async(id,revision)=>{if(p.status!=='prepared'||p.revision!==revision)return null;p={...p,status:'publishing',lease_id:'lease'};return p;},publication:async()=>p,reviewPublication:async()=>{p={...p,status:'needs_review'};},confirmPublication:async(id,m)=>{p={...p,status:'published',message_id:m.id};},query:async()=>({rows:[]})};
    f.channel.send=async payload=>{sends++;message={id:IDS.message,author:{id:IDS.bot},embeds:payload.embeds.map(e=>e.toJSON()),createdTimestamp:Date.now(),attachments:new Collection(),react:async()=>{}};throw new Error('socket closed after send');};
    const publisher=new MadlibPublishing(store,f.deps,core.config({}));assert.equal((await publisher.publish(p)).status,'needs_review');await publisher.publish(p);assert.equal(sends,1);f.channel.messages.fetch=async()=>new Collection([[message.id,message]]);assert.equal((await publisher.reconcileUncertain(p,f.channel)).found,true);assert.equal(p.status,'published');assert.equal(sends,1);
  });
  await run.test('raw routing filters unrelated guild emoji bots and self; repeated candidates share one award key',async()=>{
    const f=fakeDiscord(),p={id:core.id(),guild_id:IDS.guild,channel_id:IDS.channel,message_id:IDS.message,user_id:IDS.user,emoji_id:IDS.emoji,reactor_roles:[],suspended:false},awarded=new Set();let count=0;
    const store={one:async(sql,args)=>awarded.has(args[1])?{exists:1}:null,recordReaction:async(pub,user)=>{if(awarded.has(user))return null;awarded.add(user);count++;return 'op';}};
    const w=new MadlibWorkers({deps:f.deps,store,economy:{},publishing:{},cfg:core.config({})});w.running=true;w.remember(p);const packet={t:'MESSAGE_REACTION_ADD',d:{guild_id:IDS.guild,channel_id:IDS.channel,message_id:IDS.message,user_id:IDS.other,emoji:{id:IDS.emoji}}};
    assert.equal(w.enqueue({...packet,d:{...packet.d,emoji:{id:'wrong'}}}),false);assert.equal(w.enqueue({...packet,d:{...packet.d,guild_id:IDS.otherGuild}}),false);assert.equal(w.enqueue(packet),true);await w.candidate(p,IDS.user);await w.candidate(p,IDS.bot);await w.candidate(p,IDS.other);await w.candidate(p,IDS.other);assert.equal(count,1);
    f.guild.members.fetch=async({user})=>({...f.member,id:user,user:{id:user,bot:true},guild:f.guild});assert.equal(await reactorAccess(f.deps,p,'600000000000000001'),null);
  });
  await run.test('worker lifecycle is start-once and stoppable; SDK raw and burst APIs are actually available',async()=>{
    const f=fakeDiscord();let clears=0;const timer={unref(){}},store={recoverStale:async()=>{},query:async()=>({rows:[]}),claimReconcile:async()=>null};
    const w=new MadlibWorkers({deps:f.deps,store,economy:{execute:async()=>{}},publishing:{},cfg:core.config({}),timers:{setInterval:()=>timer,clearInterval:()=>{clears++;}}});assert.equal(w.start(),true);assert.equal(w.start(),false);assert.equal(f.client.listenerCount('raw'),1);w.stop();assert.equal(f.client.listenerCount('raw'),0);assert.equal(clears,1);
    const base=require.resolve('discord.js').replace(/src\/index.js$/,'src/');const reaction=fs.readFileSync(path.join(base,'managers/ReactionUserManager.js'),'utf8');assert(reaction.includes('type = ReactionType.Normal'));assert(reaction.includes('makeURLSearchParams({ limit, after, type })'));
    const ws=fs.readFileSync(path.join(base,'client/websocket/WebSocketManager.js'),'utf8'),at=ws.indexOf('this.client.emit(Events.Raw');assert(at>=0);assert(ws.indexOf('this.handlePacket',at)>at);assert(fs.readFileSync(path.join(base,'structures/MessagePayload.js'),'utf8').includes('enforce_nonce'));
  });
  await run.test('reverse seven additive integration hunks restores every original index byte',()=>{
    const source=fs.readFileSync(path.join(__dirname,'..','index.js'),'utf8');assert.equal(integration.sha(integration.reverse(source)),integration.BASELINE_SHA256);assert.equal(integration.apply(source),source);assert.equal(integration.changes.length,7);
    assert(source.includes('...madlib.buildMadlibSlashCommands()'));assert(source.includes('...(madlib.isEnabled() ? [GatewayIntentBits.GuildMessageReactions] : [])'));assert(source.includes('clientUserId: () => client.user?.id'));const original=integration.reverse(source);assert.equal(source.match(/client\.on\('interactionCreate'/g).length,original.match(/client\.on\('interactionCreate'/g).length);assert.throws(()=>integration.apply(original+'\n// other work'),/baseline changed/);
  });run.done();
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
