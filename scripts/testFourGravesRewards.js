"use strict";
const assert = require("node:assert/strict");
const { GRAVES, DEFAULT_CONFIG, prizeSlots } = require("../modules/fourGravesCore");
const {
  readyForPublicRewards, aggregateRewards, rewardSummaryPages, rewardsCsv,
  rewardDownloadMessage, CSV_FILENAME, DESCRIPTION_LIMIT,
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

const download = rewardDownloadMessage(data);
assert.equal(download.files.length,1);
assert.equal(download.files[0].name,CSV_FILENAME);
assert.ok(Buffer.isBuffer(download.files[0].attachment));
assert.equal(download.embeds[0].footer.text,"four-graves:555:rewards:download");
assert.deepEqual(download.allowedMentions,{parse:[]});
assert.equal(download.files[0].attachment.toString("utf8"),rewardsCsv(data));
const csv = rewardsCsv(data);
assert.match(csv,/Player 1/);
assert.match(csv,/RugDollz 3D/);
assert.match(csv,/\$CHARM/);
assert.equal(csv.split("\r\n").length,4); // header, two winners, trailing blank

// Do not send summary if an entire grave or any single award is unfinished.
assert.equal(readyForPublicRewards({...data,
  event:{...data.event,state:"BUTTONS_ACTIVE"}}),false);
assert.equal(readyForPublicRewards({...data,
  graves:data.graves.map((g,i)=>i===0?{...g,draw_completed_at:null}:g)}),false);
assert.equal(readyForPublicRewards({...data,
  awards:data.awards.slice(1)}),false);
assert.equal(readyForPublicRewards({...data,
  awards:data.awards.map((a,i)=>i? a:{...a,message_id:null})}),false);
assert.equal(readyForPublicRewards({...data,
  awards:data.awards.map((a,i)=>i? a:{...a,prize_name:"Different Prize"})}),false);
assert.equal(readyForPublicRewards({...data,
  awards:data.awards.map((a,i)=>i? a:{...a,ticket_entry_id:"not-buried"})}),false);
assert.throws(()=>rewardSummaryPages({...data,awards:data.awards.slice(1)}),
  /not all finalized/);

const oneGrave=mockCompleted(false,["barry_buckets"]);
assert.equal(oneGrave.awards.length,13);
assert.equal(readyForPublicRewards(oneGrave),true); // Empty graves don't block
const noGrave=mockCompleted(false,[]);
assert.equal(readyForPublicRewards(noGrave),false);

// If the recap cannot fit in one Discord embed, use multiple panels.
const many=mockCompleted(true);
assert.equal(many.awards.length,50);
assert.equal(aggregateRewards(many).length,50);
const multi=rewardSummaryPages(many);
assert.ok(multi.length>1,"Long winning lists must wrap to additional panels.");
for (let i=0;i<multi.length;i++) {
  assert.ok(multi[i].embeds[0].description.length<=DESCRIPTION_LIMIT);
  assert.match(multi[i].embeds[0].title,
    new RegExp("PART "+(i+1)+" / "+multi.length));
  assert.equal(multi[i].embeds[0].footer.text,"four-graves:555:rewards:page:"+i);
  assert.deepEqual(multi[i].allowedMentions,{parse:[]});
}
assert.ok(multi.map(m=>m.embeds[0].description).join("\n").includes("Player 1049"));
assert.equal(rewardsCsv(many).split("\r\n").length,52);

// Winner names can't ping real members or distort the Discord Markdown layout.
const unsafe=mockCompleted();
unsafe.snapshot.users.find(u=>u.discord_id==="100").display_name="@everyone <@123>";
const unsafeSummary=rewardSummaryPages(unsafe);
assert.doesNotMatch(unsafeSummary[0].embeds[0].description,/@everyone|<@123>/);
assert.deepEqual(unsafeSummary[0].allowedMentions,{parse:[]});
// CSV escapes formulas, while a file can preserve the human display name.
unsafe.snapshot.users.find(u=>u.discord_id==="100").display_name="=IMPORTDATA(1)";
assert.match(rewardsCsv(unsafe),/"'=IMPORTDATA/);
console.log("PASS final rewards aggregation, 50 prizes, 100000 CHARM, completeness gate, overflow pages, CSV and no mentions");
