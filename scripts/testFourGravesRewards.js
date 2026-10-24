"use strict";
const assert = require("node:assert/strict");
const { GRAVES, DEFAULT_CONFIG, prizeSlots } = require("../modules/fourGravesCore");
const {
  readyForPublicRewards, aggregateRewards, rewardSummaryPages,
  adminRewardsPanel, verifiedWallets, resolveWinnerWallets, adminRewardsCsv,
  CSV_FILENAME, DESCRIPTION_LIMIT,
} = require("../modules/fourGravesRewards");

function mockCompleted(unique = false, populatedGraves = GRAVES.map(g=>g.id)) {
  const snapshot = { users: [], entries: [] };
  const awards = [];
  const users = new Set();
  const graves = GRAVES.map(g=>({
    grave_id:g.id, decision:null, draw_completed_at:null,
  }));
  let personNumber = 0;
  for (const g of GRAVES.filter(g=>populatedGraves.includes(g.id))) {
    const state = graves.find(x=>x.grave_id===g.id);
    state.decision = { user_id:"100", choice:"TRICK" };
    state.draw_completed_at = new Date("2026-10-31T23:00:00Z");
    const slots = prizeSlots(DEFAULT_CONFIG,g.id);
    for (let index=0; index<slots.length; index++) {
      const id = unique ? String(1000 + personNumber++) : String(100 + index % 2);
      const name = unique ?
        "Player " + id + " " + "VeryLongSquigUsername".repeat(3) :
        id === "100" ? "Player 1" : "Player 2";
      const ticketId = g.id + ":" + id;
      if (!snapshot.entries.some(e=>e.id===ticketId)) snapshot.entries.push({
        grave_id:g.id,discord_id:id,id:ticketId,label:name,
      });
      if (!users.has(id)) {
        snapshot.users.push({discord_id:id,display_name:name});
        users.add(id);
      }
      const slot = slots[index];
      awards.push({
        grave_id:g.id,prize_index:index+1,prize_unit:slot.unit,
        prize_name:slot.name,winner_discord_id:id,
        winner_display_name:name,ticket_entry_id:ticketId,
        announced_at:new Date("2026-10-31T23:00:00Z"),
        message_id:"winner-post:"+g.id+":"+(index+1),
      });
    }
  }
  return {
    event:{
      id:"555",guild_id:"guild",event_key:"squigs-four-graves-2026",
      config:DEFAULT_CONFIG,closed_at:new Date("2026-10-31T22:00:00Z"),
      state:"REVEALED", panel_channel_id:"original-selection",
    },
    snapshot,graves,awards,
  };
}

async function main() {
  const data = mockCompleted();
  assert.equal(data.awards.length,50);
  assert.equal(readyForPublicRewards(data),true);
  const users = aggregateRewards(data);
  assert.deepEqual(users.map(u=>u.label),["Player 1","Player 2"]);
  assert.equal(users.reduce((n,u)=>n+u.prizesWon,0),50);
  assert.equal(users.reduce((n,u)=>n+
    u.prizes.filter(p=>p.endsWith(" $CHARM")).reduce((s,v)=>
      s+Number(v.split(" ")[0].replaceAll(",","")),0),0),100000);
  assert.ok(users.some(u=>u.prizes.some(v=>v.includes("Squigs Reloaded"))));
  assert.ok(users.some(u=>u.prizes.some(v=>v.includes("RugDollz 3D"))));
  assert.ok(users.some(u=>u.graves.length===4));

  const pages = rewardSummaryPages(data);
  assert.equal(pages.length,1);
  assert.match(pages[0].embeds[0].title,/UGLY HALLOWEEN REWARDS/);
  assert.match(pages[0].embeds[0].description,/Player 1 — /);
  assert.match(pages[0].embeds[0].description,/Player 2 — /);
  assert.deepEqual(pages[0].allowedMentions,{parse:[]});
  assert.equal(pages[0].embeds[0].footer.text,"four-graves:555:rewards:page:0");
  // Nothing public can contain a wallet address, download UI or attached file.
  assert.equal(pages[0].files,undefined);
  assert.deepEqual(pages[0].components,[]);
  assert.doesNotMatch(JSON.stringify(pages),/admin-rewards-download|wallet|Verified Wallet/i);
  assert.doesNotMatch(JSON.stringify(pages),/0x[a-f0-9]{40}/i);

  const panel = adminRewardsPanel(data);
  assert.match(panel.embeds[0].title,/ADMIN ONLY/);
  assert.equal(panel.components[0].components[0].custom_id,
    "fg1:555:admin-rewards-download");
  assert.deepEqual(panel.allowedMentions,{parse:[]});
  assert.equal(panel.files,undefined);
  assert.throws(()=>adminRewardsPanel({...data,awards:data.awards.slice(1)}),
    /not complete/);

  // Reject incomplete, mismatched and unannounced grave winners.
  assert.equal(readyForPublicRewards({...data,
    event:{...data.event,state:"BUTTONS_ACTIVE"}}),false);
  assert.equal(readyForPublicRewards({...data,
    graves:data.graves.map((g,i)=>i===0?{...g,draw_completed_at:null}:g)}),false);
  assert.equal(readyForPublicRewards({...data,
    awards:data.awards.slice(1)}),false);
  assert.equal(readyForPublicRewards({...data,
    awards:data.awards.map((a,i)=>i?a:{...a,message_id:null})}),false);
  assert.equal(readyForPublicRewards({...data,
    awards:data.awards.map((a,i)=>i?a:{...a,prize_name:"Different Prize"})}),false);
  assert.equal(readyForPublicRewards({...data,
    awards:data.awards.map((a,i)=>i?a:{...a,ticket_entry_id:"not-buried"})}),false);
  assert.throws(()=>rewardSummaryPages({...data,awards:data.awards.slice(1)}),
    /not all finalized/);

  const oneGrave=mockCompleted(false,["barry_buckets"]);
  assert.equal(oneGrave.awards.length,13);
  assert.equal(readyForPublicRewards(oneGrave),true);
  assert.equal(readyForPublicRewards(mockCompleted(false,[])),false);

  const many=mockCompleted(true);
  assert.equal(aggregateRewards(many).length,50);
  const multi=rewardSummaryPages(many);
  assert.ok(multi.length>1,"Long winning lists must wrap to additional panels.");
  for(let i=0;i<multi.length;i++) {
    assert.ok(multi[i].embeds[0].description.length<=DESCRIPTION_LIMIT);
    assert.match(multi[i].embeds[0].title,
      new RegExp("PART "+(i+1)+" / "+multi.length));
    assert.equal(multi[i].embeds[0].footer.text,"four-graves:555:rewards:page:"+i);
    assert.deepEqual(multi[i].allowedMentions,{parse:[]});
    assert.equal(multi[i].files,undefined);
  }
  assert.ok(multi.map(m=>m.embeds[0].description).join("\n").includes("Player 1049"));

  const unsafe = mockCompleted();
  unsafe.snapshot.users.find(u=>u.discord_id==="100").display_name="@everyone <@123>";
  const unsafeSummary=rewardSummaryPages(unsafe);
  assert.doesNotMatch(unsafeSummary[0].embeds[0].description,/@everyone|<@123>/);

  const walletA="0x"+"a".repeat(40);
  const walletB="0x"+"b".repeat(40);
  const unverified="0x"+"c".repeat(40);
  assert.deepEqual(verifiedWallets([
    {wallet_address:walletA,verified:true},
    {wallet_address:walletA.toUpperCase().replace(/^0X/,"0x"),verified:true},
    {wallet_address:walletB,verified:true},
    {wallet_address:unverified,verified:false},
    {wallet_address:"this-is-not-a-wallet",verified:true},
    {wallet_address:"",verified:true},
  ]),[walletA,walletB]);
  assert.throws(()=>verifiedWallets(null),/invalid records/);
  const calls = [];
  const mockWalletLinks=async (guildId,userId)=>{
    calls.push([guildId,userId]);
    if(userId==="100") return [
      {wallet_address:walletB,verified:true},
      {wallet_address:walletA,verified:true},
      {wallet_address:walletB,verified:true},
      {wallet_address:unverified,verified:false},
    ];
    return [{wallet_address:unverified,verified:false}];
  };
  const wallets=await resolveWinnerWallets("guild",users,mockWalletLinks);
  assert.deepEqual(wallets.get("100"),[walletA,walletB]);
  assert.deepEqual(wallets.get("101"),[]);
  assert.equal(calls.length,2);
  assert.ok(calls.every(pair=>pair[0]==="guild"));

  const winnersCsv=await adminRewardsCsv(data,"guild",mockWalletLinks);
  assert.match(winnersCsv,/Verified Wallet\(s\)/);
  assert.match(winnersCsv,/Discord User ID/);
  assert.match(winnersCsv,/Player 1/);
  assert.match(winnersCsv,/RugDollz 3D/);
  assert.match(winnersCsv,/\$CHARM/);
  assert.ok(winnersCsv.includes(walletA+"; "+walletB));
  assert.doesNotMatch(winnersCsv,new RegExp(unverified));
  assert.match(winnersCsv,/NOT VERIFIED — CHECK BEFORE DELIVERY/);
  assert.equal(winnersCsv.split("\r\n").length,4);
  assert.throws(()=>adminRewardsPanel({...data,awards:data.awards.slice(1)}),
    /not complete/);
  await assert.rejects(()=>adminRewardsCsv(data,"another-guild",mockWalletLinks),
    /guild mismatch/);
  await assert.rejects(()=>adminRewardsCsv(data,"guild",null),/not configured/);
  await assert.rejects(()=>adminRewardsCsv(data,"guild",async()=>{throw Error("wallet DB down")}),
    /wallet DB down/);

  unsafe.snapshot.users.find(u=>u.discord_id==="100").display_name="=IMPORTDATA(1)";
  const safeCsv=await adminRewardsCsv(unsafe,"guild",mockWalletLinks);
  assert.match(safeCsv,/"'=IMPORTDATA/);

  assert.equal(CSV_FILENAME,"Ugly_Halloween_Rewards.csv");
  console.log("PASS 50 prizes, public recap only, ephemeral admin-only panel, verified multi-wallet CSV, incomplete gates, overflow, and safe names");
}
main().catch(err=>{console.error(err);process.exitCode=1;});
