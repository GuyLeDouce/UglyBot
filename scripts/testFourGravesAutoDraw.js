"use strict";
const assert = require("node:assert/strict");
const { GRAVES, DEFAULT_CONFIG, prizePoolForGrave, prizeSlots } = require("../modules/fourGravesCore");
const { pickNextTicket, prizeWinnerCsv, choiceAnnouncement, winnerAnnouncement } = require("../modules/fourGravesDraw");
const { decisionPanel } = require("../modules/fourGravesDiscord");

const totals = GRAVES.map(g => prizeSlots(DEFAULT_CONFIG,g.id).length);
assert.deepEqual(totals,[13,11,12,13]);
assert.equal(totals.reduce((a,b)=>a+b,0),49);
const mystery = {
  barry_buckets: ["Scribble Otter Yoink Club",3],
  barbara_bowkins: ["Visions of the Void",1],
  cletus_von_wrinkles: ["Doruzu (ApeChain)",2],
  scuba_steve_3000: ["Chill Bugs",3],
};
for (const g of GRAVES) {
  const pool = prizePoolForGrave(DEFAULT_CONFIG,g.id);
  assert.deepEqual(pool.at(-1),{name:mystery[g.id][0],quantity:mystery[g.id][1]});
  assert.equal(pool.find(p=>p.name==="5,000 $CHARM").quantity,5);
  assert.equal(pool.find(p=>p.name==="Squigs Reloaded").quantity,3);
}
assert.throws(() => prizePoolForGrave({ ...DEFAULT_CONFIG,mysteryByGrave:{} },GRAVES[0].id),/finalized/);
const entries = [
  {id:"A-1",discord_id:"A",display_name:"A"},
  {id:"A-2",discord_id:"A",display_name:"A"},
  {id:"A-3",discord_id:"A",display_name:"A"},
  {id:"B-1",discord_id:"B",display_name:"B"},
  {id:"C-1",discord_id:"C",display_name:"C"},
];
const awards=[];
function add(t) {
  const r = pickNextTicket(entries,awards,t);
  awards.push({pass_number:r.pass_number,winner_discord_id:r.ticket.discord_id,ticket_entry_id:r.ticket.id});
  return r;
}
assert.equal(add(()=>0).ticket.id,"A-1");
assert.equal(add(()=>0).ticket.id,"B-1"); // Remaining tickets for A are ineligible this pass.
assert.equal(add(()=>0).ticket.id,"C-1");
assert.equal(add(()=>1).pass_number,2); // Everyone won once, pool resets.
assert.equal(awards[3].ticket_entry_id,"A-2");
assert.equal(add(()=>0).ticket.id,"B-1");
assert.equal(add(()=>0).ticket.id,"C-1");
assert.throws(()=>pickNextTicket(entries,[],()=>100),/invalid/);
assert.equal(pickNextTicket([],[],()=>0),null);

const ev={id:"55",config:DEFAULT_CONFIG};
const decision={grave_id:GRAVES[0].id,decision:{user_id:"123",choice:"TRICK",at:new Date().toISOString()}};
const notice=choiceAnnouncement(ev,decision);
assert.match(notice.content,/@everyone/);
assert.match(notice.content,/<@123>/);
assert.deepEqual(notice.allowedMentions,{parse:["everyone"],users:["123"]});
const testNotice=choiceAnnouncement({...ev,config:{...DEFAULT_CONFIG,testMode:true}},decision);
assert.doesNotMatch(testNotice.content,/@everyone/);
assert.deepEqual(testNotice.allowedMentions,{parse:[]});
const panel=decisionPanel({
  ...ev,state:"BUTTONS_ACTIVE",generation:1,
  deadline:new Date().toISOString(),
  snapshot:{entries:[{grave_id:GRAVES[0].id}]},
}, {...decision,reveal_stage:3});
assert.doesNotMatch(panel.embeds[0].description,/Your game|Duck Race|Wheel of Names|Squigwreck/);
assert.match(panel.embeds[0].description,/Scribble Otter Yoink Club/);
assert.match(panel.embeds[0].description,/PRIZE POOL/);
const announcement=winnerAnnouncement(ev,GRAVES[0].id,{
  prize_index:1,prize_name:"DollzOnApe",
  winner_display_name:"@everyone *bad*",
  ticket_entry_id:"9:1",pass_number:1,
});
assert.ok(!announcement.content.includes("@everyone"));
assert.doesNotMatch(announcement.embeds[0].description,/@everyone/);
assert.deepEqual(announcement.allowedMentions,{parse:[]});
assert.match(announcement.embeds[0].footer.text,/prize:1$/);
const csv=prizeWinnerCsv({...ev,snapshot_hash:"abc"},[{
  grave_id:GRAVES[0].id,prize_index:1,prize_name:"DollzOnApe",prize_unit:1,
  winner_discord_id:"123",winner_display_name:"Test",
  ticket_entry_id:"9:1",pass_number:1,announced_at:"today",message_id:"456",
}]);
assert.match(csv,/DollzOnApe/);
assert.match(csv,/9:1/);
console.log("PASS four finalized prize pools, participant cycles, single allowed team ping, and winner CSV");
