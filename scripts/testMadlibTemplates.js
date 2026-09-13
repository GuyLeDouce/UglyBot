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
  if(process.env.MADLIB_REPORT_DIR){
    const dir=process.env.MADLIB_REPORT_DIR;fs.mkdirSync(dir,{recursive:true});fs.writeFileSync(path.join(dir,'template-renders.json'),JSON.stringify(report,null,2)+'\n');
    const intro=['# Sixty Squig Mad Lib scene pairs — content review and complete samples','','Every sample below is rendered from the actual versioned library with its example answers. Each entry has an authored scene-specific meaning review. The tests also render Unicode, plural and maximum-length cases. These checks do not claim that image-generation fidelity or semantic quality can be proved by regex; no images were generated or external model invoked. Arbitrary user objects can remain deliberately absurd.','','Both outputs use one shared frozen moment. Costume inputs affect clothing only; facial identity and original reference art style are retained. No new project lore or actual financial payout is asserted by the fictional stories.',''];
    for(const r of report)intro.push(`## ${r.title} (${r.id})`,`Theme: ${r.category}`,`Ordered questions: ${r.questionKeys.join(', ')}`,`Editorial review: ${r.editorialReview}`,'','### Classic story',r.story,'','### Complete image prompt','```text',r.prompt,'```','');
    fs.writeFileSync(path.join(dir,'template-content-review.md'),intro.join('\n')+'\n');
  }
  run.done();
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
