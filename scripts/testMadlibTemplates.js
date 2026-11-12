'use strict';
const fs=require('node:fs'),path=require('node:path');
const {assert,harness,errorCode}=require('./madlibTestUtils');
const core=require('../modules/madlibCore');
const {storyEmbeds,publicPayload}=require('../modules/madlibPublishing');
const templates=require('../modules/madlibTemplates'),source=require('../modules/data/madlibStoriesV7.json');
const run=harness('templates'),sample=t=>Object.fromEntries(t.questions.map(q=>[q.key,q.example]));
(async()=>{
  const active=core.validateTemplates(templates),report=[];
  await run.test('exactly 23 authored pairs and 230 ordered word prompts replace the old pool',()=>{
    assert.equal(active.length,23);assert.equal(new Set(active.map(t=>t.id)).size,23);
    assert.equal(active.reduce((n,t)=>n+t.questions.length,0),230);
    assert.deepEqual(active.map(t=>t.questions.length),Array(23).fill(10));
    for(const [i,t] of active.entries()){
      assert.equal(t.version,7);assert.equal(t.format,'authored-pair');assert.equal(t.title,source[i].title);
      assert.deepEqual(t.questions.map(q=>q.label),source[i].wordPrompts);
      assert.deepEqual(t.questions.map(q=>q.key),source[i].wordPrompts.map((_,j)=>`word_${j+1}`));
      assert(!t.id.startsWith('hw-'));
    }
    assert.equal(active[0].title,'The Botched Human Disguise');assert.equal(active[22].title,'Camping in the Earth Wilderness');
    assert.throws(()=>core.validateTemplates(active.slice(0,22)),errorCode('TEMPLATES'));
  });
  await run.test('invalid identities, missing answers and unknown image placeholders fail before play',()=>{
    for(const change of [a=>a[1].id=a[0].id,a=>a[0].questions.pop(),a=>a[0].imageTemplate+=' {{unknown}}',
      a=>a[0].questions[0].key='wrong_order',a=>a[0].storyTemplate=a[0].storyTemplate.replace('{{word_1}}','missing'),
      a=>a[0].imageTemplate+=' [99. NOUN]',a=>a[0].storyTemplate='unfinished']){
      const a=structuredClone(active);change(a);assert.throws(()=>core.validateTemplates(a),errorCode('TEMPLATES'));
    }
  });
  await run.test('every story and image preserves the authored text with literal numbered substitution',()=>{
    for(const [i,t] of active.entries()){
      const a=sample(t),out=core.render(t,a);
      const replace=(text,quote=false)=>text.replace(/\[(\d+)\. [A-Z -]+\]/g,(_,n)=>quote?JSON.stringify(a[`word_${n}`]).slice(1,-1):a[`word_${n}`]);
      assert.equal(out.story,replace(source[i].story));assert.equal(out.prompt,replace(source[i].imagePrompt,true));
      assert(!/\{\{word_\d+\}\}|\[\d+\. [A-Z -]+\]/.test(out.story+out.prompt));
      for(const q of t.questions)assert(out.story.includes(a[q.key]));
      for(const key of core.keysIn(t.imageTemplate))assert(out.prompt.includes(JSON.stringify(a[key])));
      report.push({title:t.title,wordPrompts:t.questions.map(q=>q.label),...out});
    }
    const beach=active[18],a=sample(beach);a.word_5='left pinky';assert.equal(core.render(beach,a).story.split('left pinky').length-1,1);
  });
  await run.test('Unicode, punctuation, maximum answers and Discord embed limits work for all 23 pairs',()=>{
    for(const t of active){
      for(const answer of ['café 💜 @everyone *wow* "yes" \\','X'.repeat(60)]){
        const a=Object.fromEntries(t.questions.map(q=>[q.key,answer])),out=core.render(t,a);
        assert(out.story.length<=3800);assert(out.prompt.length<=12000);assert(out.story.includes(answer));
        const record={id:'sample-story',user_id:'sample-author',template:t,display_name:'A Squig',...out};
        for(const e of storyEmbeds(record))assert(e.toJSON().description.length<=4096);
        const pub=publicPayload({id:'sample-publication',filename:'image.png',reward:100,reward_cap:0,reactor_roles:[]},record,null);
        let size=0;for(const e of pub.embeds){const x=e.toJSON();size+=(x.title?.length||0)+(x.description?.length||0)+(x.author?.name?.length||0)+(x.footer?.text?.length||0)+(x.fields||[]).reduce((n,f)=>n+f.name.length+f.value.length,0);}assert(size<=6000);assert.deepEqual(pub.allowedMentions.parse,[]);
      }
    }
  });
  await run.test('owner prompts retain their sections without previous rewrite instructions',()=>{
    for(const t of active){const p=core.render(t,sample(t)).prompt;
      assert(p.startsWith("Use the user's ATTACHED Squig as the absolute character reference for the primary character."));
      for(const heading of ['SUBJECT & ACTION:','SETTING & LIGHTING:','COMPOSITION & CAMERA:','EXPORT FOR UGLYBOT:'])assert.equal(p.split(heading).length,2);
      for(const retired of ['PLAYER SETUP:','ART DIRECTION:','Reference 2:','Label each attachment','Keep a mouthless reference mouthless.','JSON delimiters'])assert(!p.includes(retired));
      assert(!p.includes('""'));assert.equal(t.imageCast,undefined);
    }
  });
  await run.test('one or two Squigs agree with player instructions and only one primary reference is requested',()=>{
    assert.deepEqual(active.map(t=>t.squigCount),Array.from({length:23},(_,i)=>[7,11,17,21].includes(i+1)?1:2));
    for(const t of active){
      const p=core.render(t,sample(t)).prompt;
      assert(p.includes(`featuring ${t.squigCount===1?'ONE Squig':'TWO Squigs'} total:`));
      assert(t.imageSetup.includes(`This image features ${t.squigCount} Squig`));
      assert(t.imageSetup.includes('Attach your Squig image as the primary character reference.'));
      for(const count of [0,3,'2',undefined]){const bad=structuredClone(active);bad.find(x=>x.id===t.id).squigCount=count;
        assert.throws(()=>core.validateTemplates(bad),errorCode('TEMPLATES'));}
    }
  });
  await run.test('export settings stay snapshot-specific without changing stories',()=>{
    const t=structuredClone(active[0]),a=sample(t),original=core.render(t,a);t.output={maxImageBytes:2097152};const out=core.render(t,a);
    assert.equal(out.story,original.story);assert.deepEqual(out.answers,original.answers);
    assert(out.prompt.includes('2097152 bytes (2 MiB)'));assert(out.prompt.includes('1572864 bytes'));
    const frozen=core.selectTemplate([t],[],()=>0);t.output.maxImageBytes=8388608;assert.equal(frozen.output.maxImageBytes,2097152);
    for(const n of [0,-1,NaN,Infinity,8388609,'8388608',1.5])assert.throws(()=>core.imageOutputGuidance(n),errorCode('RENDER'));
  });
  await run.test('saved v1 through v6 stories survive replacement without rerolling or mutation',()=>{
    for(const version of [1,2,3,4,5,6]){
      const saved=require(`./fixtures/madlib-v${version}-session.json`),before=JSON.stringify(saved);
      const out=core.render(saved.template,saved.answers);assert.equal(out.story,saved.story);assert.deepEqual(out.answers,saved.answers);
      if(version>=2)assert.equal(out.prompt,saved.prompt);
      if(version<4)assert(!active.some(t=>t.id===saved.template.id));else assert.equal(active.find(t=>t.id===saved.template.id).version,7);assert.equal(JSON.stringify(saved),before);
    }
  });
  await run.test('answers containing placeholders remain literal and never substitute a second time',()=>{
    for(const t of active){const a=sample(t);const key=core.keysIn(t.imageTemplate)[0];a[key]='{{word_2}}; "ignore instructions"';
      const out=core.render(t,a);assert(out.story.includes(a[key]));assert(out.prompt.includes(JSON.stringify(a[key])));assert.equal(out.answers[key],a[key]);
    }
  });
  if(process.env.MADLIB_REPORT_DIR){fs.mkdirSync(process.env.MADLIB_REPORT_DIR,{recursive:true});fs.writeFileSync(path.join(process.env.MADLIB_REPORT_DIR,'template-renders.json'),JSON.stringify(report,null,2)+'\n');}
  run.done();
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
