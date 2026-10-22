"use strict";
const assert = require("node:assert/strict");
const { Collection } = require("discord.js");
const {
  FourGravesDiscord, getGuildMemberRoster, noteGuildMemberChange,
} = require("../modules/fourGravesDiscord");
const { DEFAULT_CONFIG, GRAVES } = require("../modules/fourGravesCore");
const { fixture } = require("./fourGravesDiscordFixture");

async function main() {
  const mocked = fixture("208");
  const shared = await Promise.all([
    getGuildMemberRoster(mocked.guild),
    getGuildMemberRoster(mocked.guild),
  ]);
  assert.strictEqual(shared[0],shared[1]);
  assert.equal(shared[0].size,3);
  assert.equal(mocked.calls.restList,1);
  assert.equal(mocked.calls.gatewayFetch,0);
  await getGuildMemberRoster(mocked.guild);
  assert.equal(mocked.calls.restList,1); // 15-minute cache

  const joined = {
    id:"888",guild:mocked.guild, roles: {cache: new Collection()},
    user:{id:"888",username:"joined"},displayName:"joined",
  };
  noteGuildMemberChange(joined,true);
  assert.ok((await getGuildMemberRoster(mocked.guild)).has("888"));
  noteGuildMemberChange(joined,false);
  assert.equal((await getGuildMemberRoster(mocked.guild)).has("888"),false);

  const big = new Collection(Array.from({length:1001},(_,i)=>{
    const id=String(i+1000);
    return [id,{id,user:{id,username:id},guild:null}];
  }));
  const calls=[];
  const paged={members:{list:async ({limit,after})=>{
    calls.push({limit,after});
    const values=[...big.values()].filter(x=>!after||BigInt(x.id)>BigInt(after)).slice(0,limit);
    return new Collection(values.map(x=>[x.id,x]));
  }}};
  const all=await getGuildMemberRoster(paged);
  assert.equal(all.size,1001);
  assert.equal(calls.length,2);
  assert.equal(calls[0].limit,1000);
  assert.equal(calls[1].after,"1999");
  assert.equal(calls[1].limit,1000);

  const denied={memberList:0,members:{list:async ()=>{
    denied.memberList++;
    throw Error("REST member list rate limited. Retry after 29.174 seconds.");
  }}};
  await assert.rejects(()=>getGuildMemberRoster(denied),/rate limited/);
  await assert.rejects(()=>getGuildMemberRoster(denied),/cooling down/);
  assert.equal(denied.memberList,1); // no 15-second request loop

  const other=fixture("300");
  const store={
    testMode:true,
    pool:{
      query:async(sql)=>sql.includes("SELECT * FROM four_graves_graves")
        ? {rows:GRAVES.map(g=>({
          grave_id:g.id,role_id:null,channel_id:null,
          decision:null,reveal_stage:0,
        }))} : {rows:[],rowCount:1},
    },
    operation:async()=>{},
  };
  const worker=new FourGravesDiscord(store,other.deps);
  const e={
    id:"7",guild_id:other.guild.id,
    config:{...DEFAULT_CONFIG,testMode:true,testOwnerId:"10"},
    closed_at:null,panel_channel_id:null,
  };
  await worker.reconcile(e);
  assert.equal(other.calls.gatewayFetch,0);
  assert.equal(other.calls.restList,0); // OPEN/setup never scans memberships
  await worker.reconcile(e);
  assert.equal(other.calls.restList,0);

  console.log("PASS no opcode 8, REST pages, cache sharing, member joins/leaves, cooldown and OPEN worker");
}
main().catch(e=>{console.error(e);process.exitCode=1;});
