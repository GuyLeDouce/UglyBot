"use strict";
const assert = require("node:assert/strict");
const { DEFAULT_CONFIG, GRAVES, PANEL_INDICES } = require("../modules/fourGravesCore");
const { rosterPanel, rosterPage, graveParticipants, PAGE_SIZE } = require("../modules/fourGravesRoster");
const { publicPanel } = require("../modules/fourGravesDiscord");

const event = { id:"37",state:"OPEN",config:DEFAULT_CONFIG };
const entries = [];
function bury(graveId, userId, name, count) {
  for (let number=0;number<count;number++) entries.push({
    id:graveId+":"+userId+":"+number,grave_id:graveId,discord_id:userId,
    label:name+(number?"("+number+")":""), display_name:name,
  });
}
bury("barbara_bowkins","1","Player 1",2);
bury("barbara_bowkins","2","Player 2",1);
bury("cletus_von_wrinkles","1","Player 1",4);
bury("cletus_von_wrinkles","3","Player 3",2);
bury("scuba_steve_3000","1","Player 1",3);
bury("scuba_steve_3000","2","Player 2",2);
bury("scuba_steve_3000","3","Player 3",3);
bury("barry_buckets","3","Player 3",4);
bury("barry_buckets","1","Player 1",5);
const snapshot={entries};
const originalEntries=JSON.stringify(snapshot);
assert.equal(rosterPanel(event,{entries:[]},GRAVES[0].id),null);
assert.deepEqual(PANEL_INDICES,[2,0,1,3]);

for(const index of PANEL_INDICES) {
  const grave=GRAVES[index];
  const panel=rosterPanel(event,snapshot,grave.id);
  assert.equal(panel.embeds.length,1);
  assert.equal(panel.components[0].components.length,1);
  assert.equal(panel.components[0].components[0].custom_id,"fg1:37:roster:"+index+":0");
  assert.equal(panel.embeds[0].footer.text,"four-graves:37:roster:"+grave.id);
  assert.deepEqual(panel.allowedMentions,{parse:[]});
  assert.match(panel.embeds[0].description,/DEAD ARE KEEPING SCORE/);
  assert.match(panel.embeds[0].description,/Every ticket counts/);
  assert.match(panel.embeds[0].description,/No promises/);
  assert.doesNotMatch(JSON.stringify(panel),/DollzOnApe|RugDollz|5,000|Chill Bugs|Visions of the Void/);
}
const cletus=rosterPanel(event,snapshot,"cletus_von_wrinkles");
assert.match(cletus.embeds[0].title,/Cletus' Grave/);
assert.match(cletus.embeds[0].description,/Player 1 — 4 tickets/);
assert.match(cletus.embeds[0].description,/Player 3 — 2 tickets/);
assert.doesNotMatch(cletus.embeds[0].description,/Player 1\(1\)/);
assert.match(cletus.embeds[0].fields[0].value,/6 tickets · 2 Squigs/);
assert.equal(rosterPanel(event,snapshot,"barry_buckets").embeds[0].fields[0].value,
  "9 tickets · 2 Squigs");
assert.match(rosterPanel(event,snapshot,"barbara_bowkins").embeds[0].description,/Player 2 — 1 ticket/);
assert.match(rosterPanel(event,snapshot,"scuba_steve_3000").embeds[0].description,/Player 2 — 2 tickets/);
assert.equal(graveParticipants(snapshot,"cletus_von_wrinkles").players.length,2);
assert.equal(graveParticipants(snapshot,"cletus_von_wrinkles").tickets,6);
assert.equal(PANEL_INDICES.reduce((total,index)=>
  total+graveParticipants(snapshot,GRAVES[index].id).tickets,0),entries.length);
assert.equal(JSON.stringify(snapshot),originalEntries); // No changes to pooled tickets or draw odds.

const sparse={entries:[{
  id:"x:1", grave_id:"cletus_von_wrinkles",
  discord_id:"123", label:"First Squig",
}]};
assert.match(rosterPanel(event,sparse,"barry_buckets").embeds[0].description,
  /Nobody has buried a ticket/);
assert.match(rosterPanel(event,sparse,"barry_buckets").embeds[0].fields[0].value,
  /0 tickets · 0 Squigs/);

const testEvent={...event,config:{...event.config,testMode:true}};
assert.match(rosterPanel(testEvent,snapshot,"barry_buckets").embeds[0].title,/TEST/);
assert.ok(!JSON.stringify(rosterPanel(testEvent,snapshot,"barry_buckets")).includes("@everyone"));
const publicSelection=publicPanel(event);
assert.match(publicSelection.embeds[0].description,/THE DEAD DON'T PICK THEIR COMPANY/);

// A hard skew of all 455 tickets into a single grave still fits because the
// 58 participants are shown ONCE with the count of their real ticket entries.
const huge={entries:[]};
for(let player=1;player<=58;player++){
  const id=String(player);
  const count=player<=49?8:7;  // 49*8 + 9*7 = 455
  for(let entry=0;entry<count;entry++)huge.entries.push({
    id:id+":"+entry,grave_id:"cletus_von_wrinkles",discord_id:id,
    label:"Player "+player+(entry?"("+entry+")":""),
  });
}
assert.equal(huge.entries.length,455);
const roster=graveParticipants(huge,"cletus_von_wrinkles");
assert.equal(roster.players.length,58);
assert.equal(roster.tickets,455);
assert.equal(roster.players.reduce((n,p)=>n+p.tickets,0),455);
const bigPanel=rosterPanel(event,huge,"cletus_von_wrinkles");
assert.ok(bigPanel.embeds[0].description.length<=4096);
assert.match(bigPanel.embeds[0].description,/Player 1 — 8 tickets/);
assert.match(bigPanel.embeds[0].description,/Player 58 — 7 tickets/);
assert.match(bigPanel.embeds[0].fields[0].value,/455 tickets · 58 Squigs/);
assert.equal(JSON.stringify(huge.entries).includes("Player 1(7)"),true);
for(let index=0;index<4;index++)
  assert.ok(rosterPanel(event,huge,GRAVES[index].id));
const pageOne=rosterPage(event,huge,2,0);
assert.match(pageOne.content,/455 tickets \/ 58 Squigs. Page 1 of 3/);
assert.match(pageOne.embeds[0].description,/Player 20 — 8 tickets/);
assert.doesNotMatch(pageOne.embeds[0].description,/Player 21 — 8 tickets/);
assert.equal(pageOne.components[0].components[0].disabled,true);
assert.equal(pageOne.components[0].components[1].disabled,false);
const middle=rosterPage(event,huge,2,1);
assert.match(middle.content,/Page 2 of 3/);
assert.equal(middle.components[0].components[0].disabled,false);
assert.equal(middle.components[0].components[1].disabled,false);
const last=rosterPage(event,huge,2,999);
assert.match(last.embeds[0].description,/Player 58 — 7 tickets/);
assert.equal(last.components[0].components[1].disabled,true);
assert.equal(PAGE_SIZE,20);

// Extremely long usernames are trimmed to a safe embed budget. The complete
// participant list remains accessible on private pages without overflow.
const long={entries:[]};
for(let n=0;n<58;n++)long.entries.push({
  id:"long:"+n,grave_id:"barbara_bowkins",discord_id:String(n),
  label:"A".repeat(83)+" "+n,
});
const limited=rosterPanel(event,long,"barbara_bowkins");
assert.ok(limited.embeds[0].description.length<=4096);
assert.match(limited.embeds[0].description,/more Squigs/);
for(const page of [0,1,2])
  assert.ok(rosterPage(event,long,1,page).embeds[0].description.length<=4096);

// Unsafe user-generated Discord markup must never ping members or roles.
const names={entries:[{
  id:"danger",grave_id:"barbara_bowkins",discord_id:"x",
  label:"@everyone <@123> **bad**",
}]};
const escaped=rosterPanel(event,names,"barbara_bowkins");
assert.doesNotMatch(JSON.stringify(escaped),/@everyone|<@123>/);
assert.deepEqual(escaped.allowedMentions,{parse:[]});
assert.doesNotMatch(JSON.stringify(rosterPage(event,names,1)),/@everyone|<@123>/);
assert.throws(()=>rosterPage(event,snapshot,99),/Unknown grave/);
assert.throws(()=>graveParticipants(snapshot,"invalid"),/Unknown grave/);
console.log("PASS four independent player-count rosters, 455 pooled tickets / 58 Squigs, pagination, no spoilers or mentions");
