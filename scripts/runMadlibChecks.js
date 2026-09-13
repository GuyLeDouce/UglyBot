'use strict';
// Test runner only: never requires index.js, reads .env, or starts the bot.
const fs=require('node:fs'),os=require('node:os'),path=require('node:path');const {spawnSync}=require('node:child_process');
const {reverse,BASELINE_SHA256,sha}=require('./installMadlibIntegration');
const root=path.join(__dirname,'..'),reportDir=path.resolve(process.env.MADLIB_REPORT_DIR||path.join(root,'madlib-ci'));
fs.mkdirSync(reportDir,{recursive:true});
const result={runtime:process.version,baseCommit:'fc02c69dfda8ec96120fa4f3b508d769995cfa73',productionServicesUsed:false,syntax:[],feature:[],legacy:[],baselineIssues:[]};
function execute(name,args,cwd=root,env={}){
  const r=spawnSync(process.execPath,args,{cwd,env:{...process.env,MADLIB_ENABLED:'false',MADLIB_REPORT_DIR:reportDir,...env},encoding:'utf8',timeout:180000,maxBuffer:16*1024*1024});
  const text=[r.stdout||'',r.stderr||'',r.error?String(r.error):''].join('\n');fs.writeFileSync(path.join(reportDir,`${name}.log`),text);
  const skipped=/^SKIP postgres:/m.test(text),record={name,exitCode:r.status,signal:r.signal,skipped,status:r.status===0&&!skipped?'passed':skipped?'skipped':'failed',log:`${name}.log`};
  console.log(`${record.status.toUpperCase()} ${name}${skipped?' (database not executed)':''}`);return {...record,text};
}
const recordOnly=({text,...r})=>r;let temp,failed=false;
try{
  const codeFiles=[...fs.readdirSync(path.join(root,'modules')).filter(n=>/^madlib.*\.js$/.test(n)).map(n=>`modules/${n}`),...fs.readdirSync(path.join(root,'modules','data')).filter(n=>/^madlib.*\.js$/.test(n)).map(n=>`modules/data/${n}`),...fs.readdirSync(__dirname).filter(n=>/^(?:testMadlib|installMadlib|runMadlib|madlibTestUtils).*\.js$/.test(n)).map(n=>`scripts/${n}`),'index.js'];
  for(const file of codeFiles){const r=execute(`syntax-${path.basename(file)}`,['--check',file]);result.syntax.push(recordOnly(r));if(r.status!=='passed')failed=true;}
  for(const test of ['testMadlibLogic','testMadlibTemplates','testMadlibIntegration','testMadlibDatabase','testMadlibPngDecode']){
    const r=execute(test,[`scripts/${test}.js`]),counts=[...r.text.matchAll(/^RESULT .*?: (\d+) test groups passed/gm)].map(m=>Number(m[1]));
    result.feature.push({...recordOnly(r),testGroups:counts.reduce((a,b)=>a+b,0)});if(r.status!=='passed'||counts.length!==1||counts[0]<1)failed=true;
  }
  // Reconstruct exactly the reviewed index and run unchanged legacy code in the same runtime.
  temp=fs.mkdtempSync(path.join(os.tmpdir(),'madlib-legacy-'));const original=reverse(fs.readFileSync(path.join(root,'index.js'),'utf8'));
  if(sha(original)!==BASELINE_SHA256)throw new Error('Original index reconstruction failed; baseline comparison is unsafe.');
  fs.writeFileSync(path.join(temp,'index.js'),original);fs.copyFileSync(path.join(root,'package.json'),path.join(temp,'package.json'));
  fs.cpSync(path.join(root,'modules'),path.join(temp,'modules'),{recursive:true,filter:source=>!/^madlib/i.test(path.basename(source))&&path.basename(source)!=='squigMadlibTemplates.json'});
  fs.mkdirSync(path.join(temp,'scripts'));
  const legacy=['testRewardLogic','testMarketplaceLogic','testMawLogic','testMawRarityLogic','testMawDispositionLogic','testBountyVaultLogic','testBountyRecovery'];
  for(const name of legacy)fs.copyFileSync(path.join(__dirname,`${name}.js`),path.join(temp,'scripts',`${name}.js`));
  for(const file of fs.readdirSync(root).filter(n=>/\.(csv|cvs)$/i.test(n)))fs.copyFileSync(path.join(root,file),path.join(temp,file));
  fs.symlinkSync(path.join(root,'node_modules'),path.join(temp,'node_modules'),'dir');
  const normalize=text=>text.replaceAll(root,'<repository>').replaceAll(temp,'<repository>');
  for(const name of legacy){
    const before=execute(`baseline-${name}`,[`scripts/${name}.js`],temp),after=execute(`branch-${name}`,[`scripts/${name}.js`]);
    const unchangedFailure=before.exitCode!==0&&after.exitCode===before.exitCode&&normalize(before.text)===normalize(after.text);
    const disposition=after.exitCode===0?'passed':unchangedFailure?'unchanged_baseline_failure':'new_or_changed_failure';
    result.legacy.push({name,baseline:recordOnly(before),branch:recordOnly(after),disposition});
    if(unchangedFailure)result.baselineIssues.push({name,logs:[before.log,after.log]});else if(after.exitCode!==0)failed=true;
  }
  result.dependencies={};
  for(const name of ['discord.js','pg','node-fetch','@napi-rs/canvas','ethers','dotenv']){
    let entry;try{entry=require.resolve(name,{paths:[root]});}catch(_){}
    let version='unresolved';if(entry){let d=path.dirname(entry);while(d!==path.dirname(d)){const p=path.join(d,'package.json');if(fs.existsSync(p)){const meta=JSON.parse(fs.readFileSync(p,'utf8'));if(meta.name===name){version=meta.version;break;}}d=path.dirname(d);}}result.dependencies[name]=version;
  }
}catch(error){failed=true;result.runnerError=String(error.message);console.error(error.stack);}
finally{
  if(temp)fs.rmSync(temp,{recursive:true,force:true});
  result.featureVerification=result.feature.length===5&&result.feature.every(r=>r.status==='passed'&&r.testGroups>0)?'passed':'incomplete_or_failed';
  result.status=failed?'failed_or_incomplete':result.baselineIssues.length?'passed_with_baseline_disclosures':'passed';
  fs.writeFileSync(path.join(reportDir,'verification.json'),JSON.stringify(result,null,2)+'\n');
  const lines=['# Mad Libs verification',`Runtime: ${result.runtime}`,`Feature verification: **${result.featureVerification}**`,`Unchanged legacy baseline failures: **${result.baselineIssues.length}**`,'','| Suite | Status | Groups |','|---|---|---|',...result.feature.map(r=>`| ${r.name} | ${r.status} | ${r.testGroups} |`),'','| Legacy suite | Baseline comparison |','|---|---|',...result.legacy.map(r=>`| ${r.name} | ${r.disposition} |`),'','No production database, Discord token or DRIP key was used. Live sandbox acceptance remains separately authorized. See verification.json and individual logs for exact results.'];
  if(result.runnerError)lines.push('',`Runner error: ${result.runnerError}`);
  fs.writeFileSync(path.join(reportDir,'verification.md'),lines.join('\n')+'\n');console.log(lines.join('\n'));process.exitCode=failed?1:0;
}
