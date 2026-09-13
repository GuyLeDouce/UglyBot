'use strict';
const fs=require('node:fs'),path=require('node:path');
const {assert,harness,errorCode}=require('./madlibTestUtils');
const core=require('../modules/madlibCore');
const {storyEmbeds,publicPayload}=require('../modules/madlibPublishing');
const templates=require('../modules/madlibTemplates');const run=harness('templates');
(async()=>{
  const active=core.validateTemplates(templates),report=[];
  await run.test('60 enabled versioned pairs, six in ten categories, no duplicate IDs or moments',()=>{
    assert.equal(active.length,60);assert.equal(new Set(active.map(t=>t.id)).size,60);assert.equal(new Set(active.map(t=>t.scene.moment)).size,60);
    const counts=new Map();for(const t of active)counts.set(t.category,(counts.get(t.category)||0)+1);assert.equal(counts.size,10);assert([...counts.values()].every(n=>n===6));
    assert.throws(()=>core.validateTemplates(active.slice(0,49)),errorCode('TEMPLATES'));
    const bad=structuredClone(active);bad[0].scene.moment+=' {{missing_question}}';assert.throws(()=>core.validateTemplates(bad),errorCode('TEMPLATES'));
    const duplicate=structuredClone(active);duplicate[1].id=duplicate[0].id;assert.throws(()=>core.validateTemplates(duplicate),errorCode('TEMPLATES'));
  });
  await run.test('all sixty render matching complete outputs for samples Unicode plurals and maximum lengths',()=>{
    for(const t of active){
      const sample=Object.fromEntries(t.questions.map(q=>[q.key,q.example]));
      const unicode={emotion:'oddly hopeful',object:'café umbrella',food:'crème brûlée',animal:'owl',adjective:'elegant',colour:'orange',plural:'geese',costume:'emerald waistcoat',shout:'We’re wonderfully weird! 💜'};
      const punctuation=Object.fromEntries(t.questions.map(q=>[q.key,`@here *${q.key}* \\ "" \\ ${'Z'.repeat(q.maxLength)}`.slice(0,q.maxLength)]));
      for(const answers of [sample,unicode,punctuation]){
        const output=core.render(t,answers);assert(output.story.length>200);assert(output.prompt.length>700);assert(output.prompt.includes(core.STYLE));
        for(const q of t.questions){const answer=core.validateAnswer(answers[q.key],q);assert(output.story.includes(answer),`${t.id}: story missing ${q.key}`);assert(output.prompt.includes(JSON.stringify(answer)),`${t.id}: prompt missing ${q.key}`);}
        for(const s of [output.story,output.prompt])assert(!/\{\{[a-z_]+\}\}/.test(s),`${t.id}: unresolved placeholder`);
        assert(!/\b[Aa]n? \{\{/.test(t.scene.moment),`${t.id}: brittle indefinite article before arbitrary input`);
        const record={id:'sample-story',user_id:'sample-author',template:t,display_name:'A Squig',...output};
        for(const e of storyEmbeds(record))assert(e.toJSON().description.length<=4096);
        const pub=publicPayload({id:'sample-publication',filename:'image.png',reward:100,reward_cap:0,reactor_roles:[]},record,null);
        let size=0;for(const e of pub.embeds){const x=e.toJSON();size+=(x.title?.length||0)+(x.description?.length||0)+(x.author?.name?.length||0)+(x.footer?.text?.length||0)+(x.fields||[]).reduce((n,f)=>n+f.name.length+f.value.length,0);}assert(size<=6000,`${t.id}: embed budget exceeded`);
        assert(!JSON.stringify(pub.embeds.map(e=>e.toJSON())).includes('SINGLE FROZEN MOMENT'));assert.deepEqual(pub.allowedMentions.parse,[]);
      }
      report.push({id:t.id,title:t.title,category:t.category,questionKeys:t.questions.map(q=>q.key),location:t.scene.location,moment:t.scene.moment,editorialReview:t.review,...core.render(t,sample)});
    }
  });
  await run.test('shared frozen moments prevent renderer drift; each pair has a separate authored meaning review',()=>{
    const source=fs.readFileSync(path.join(__dirname,'..','modules','madlibCore.js'),'utf8');assert(source.includes('[t.lead, t.scene.moment, t.ending]'));assert(source.includes('SINGLE FROZEN MOMENT: ${substitute(t.scene.moment'));
    // A regex cannot prove semantic coherence. These records are authored editorial judgments,
    // separate from the mechanically checked shared moment and placeholder contract.
    for(const t of active){assert(t.review&&t.review.length>=60,`Missing content review: ${t.id}`);assert(t.scene.location&&t.scene.composition);}
    assert.equal(new Set(active.map(t=>t.review)).size,60);
  });
  await run.test('human-world v2 covers all ten themes with the original nine question types',()=>{
    const allowed=new Set(['emotion','object','food','animal','adjective','colour','plural','costume','shout']);const used=new Set();
    assert.deepEqual([...new Set(active.map(t=>t.category))],['Morning / GM','Night / GN','Everyday errands','Workweek / office','Meme reactions','Web3 desk life','Food / coffee','Weekend / outdoors','Screens / posting','Social mischief']);
    for(const t of active){
      assert(t.id.startsWith('hw-'));assert.equal(t.version,2);assert.equal(t.scene.world,'human-world');
      assert(!/Ugly City|The Maw|Ugly Labs/.test([t.scene.location,t.lead,t.scene.moment,t.ending].join(' ')));
      assert.deepEqual(t.questions.map(q=>q.key),[...new Set(core.keysIn(t.scene.moment))]);
      for(const q of t.questions){assert(allowed.has(q.key));used.add(q.key);assert.equal(q.maxLength,q.key==='shout'?100:60);}
    }assert.equal(used.size,9);
  });
  await run.test('every prompt includes compatible export settings and bounded optional GM GN lettering',()=>{
    let gm=0,gn=0;
    for(const t of active){
      const p=core.render(t,Object.fromEntries(t.questions.map(q=>[q.key,q.example]))).prompt;
      assert.equal(p.split('EXPORT FOR UGLYBOT:').length,2);
      for(const text of ['1024 x 1024','8388608 bytes','4194304 bytes','PNG (.png)','JPG/JPEG','WebP','4096 pixels','NOT words to draw','prompt text cannot enforce'])assert(p.includes(text),`${t.id}: ${text}`);
      assert(p.includes(core.STYLE));assert(p.includes('ordinary human-world location'));
      if(t.scene.lettering){assert(['GM','GN'].includes(t.scene.lettering));assert(p.includes(`LETTERING: Only the short greeting ${t.scene.lettering}`));assert(!p.includes('Do not add new lettering'));if(t.scene.lettering==='GM')gm++;else gn++;}
      else assert(p.includes('Do not add new lettering'));
      assert(p.includes('Any shout is narrative context for expression only, not lettering'));
    }assert(gm>=3&&gn>=3);
    const bad=structuredClone(active);bad[0].scene.lettering='@everyone BAD';assert.throws(()=>core.validateTemplates(bad),errorCode('TEMPLATES'));
    assert.throws(()=>core.render(bad[0],Object.fromEntries(bad[0].questions.map(q=>[q.key,q.example]))),errorCode('RENDER'));
  });
  await run.test('export limits are snapshot-specific and never change story text or answers',()=>{
    const t=structuredClone(active[0]),a=Object.fromEntries(t.questions.map(q=>[q.key,q.example]));
    const original=core.render(t,a);t.output={maxImageBytes:2097152};const smaller=core.render(t,a);
    assert.equal(smaller.story,original.story);assert.deepEqual(smaller.answers,original.answers);
    assert(smaller.prompt.includes('2097152 bytes (2 MiB)'));assert(smaller.prompt.includes('1572864 bytes'));assert(!smaller.prompt.includes('8388608 bytes'));
    const frozen=core.selectTemplate([t],[],()=>0);t.output.maxImageBytes=8388608;assert.equal(frozen.output.maxImageBytes,2097152);
    assert(core.imageOutputGuidance(1).includes('at most 1 bytes'));
    for(const n of [0,-1,NaN,Infinity,8388609,'8388608',1.5])assert.throws(()=>core.imageOutputGuidance(n),errorCode('RENDER'));
  });
  await run.test('old version-one snapshots still render their original story after library replacement',()=>{
    const saved=require('./fixtures/madlib-v1-session.json'),before=JSON.stringify(saved);
    assert.equal(saved.template.version,1);assert(!active.some(t=>t.id===saved.template.id));
    const out=core.render(saved.template,saved.answers);assert.equal(out.story,saved.story);assert.deepEqual(out.answers,saved.answers);
    assert(out.prompt.includes(saved.template.scene.location));assert(out.prompt.includes('EXPORT FOR UGLYBOT:'));
    assert.equal(JSON.stringify(saved),before);assert(!saved.prompt.includes('EXPORT FOR UGLYBOT:'));
  });
  await run.test('all sixty scenes keep unusual answers literal with complete story-prompt correspondence',()=>{
    for(const t of active){
      const a=Object.fromEntries(t.questions.map(q=>[q.key,q.example]));a.object='{{food}}; a blue chair';
      if(t.questions.some(q=>q.key==='shout'))a.shout='GM? "Not financial advice!" 💜';
      const out=core.render(t,a);assert(out.story.includes(a.object));assert(out.prompt.includes(JSON.stringify(a.object)));assert.equal(out.answers.object,a.object);
      for(const q of t.questions){assert(out.story.includes(a[q.key]));assert(out.prompt.includes(JSON.stringify(a[q.key])));}
      assert(out.story.length<=3800);assert(out.prompt.length<=12000);
    }
  });
  if(process.env.MADLIB_REPORT_DIR){
    const dir=process.env.MADLIB_REPORT_DIR;fs.mkdirSync(dir,{recursive:true});fs.writeFileSync(path.join(dir,'template-renders.json'),JSON.stringify(report,null,2)+'\n');
    const intro=['# Sixty Squig Mad Lib scene pairs — content review and complete samples','','Every sample below is rendered from the actual versioned library with its example answers. Each entry has an authored scene-specific meaning review. The tests also render Unicode, plural and maximum-length cases. These checks do not claim that image-generation fidelity or semantic quality can be proved by regex; no images were generated or external model invoked. Arbitrary user objects can remain deliberately absurd.','','Both outputs use one shared frozen moment. Costume inputs affect clothing only; facial identity and original reference art style are retained. No new project lore or actual financial payout is asserted by the fictional stories.',''];
    for(const r of report)intro.push(`## ${r.title} (${r.id})`,`Theme: ${r.category}`,`Ordered questions: ${r.questionKeys.join(', ')}`,`Editorial review: ${r.editorialReview}`,'','### Classic story',r.story,'','### Complete image prompt','```text',r.prompt,'```','');
    fs.writeFileSync(path.join(dir,'template-content-review.md'),intro.join('\n')+'\n');
  }
  run.done();
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
