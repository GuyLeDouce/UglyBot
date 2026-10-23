"use strict";
const assert = require("node:assert/strict");
const { DEFAULT_CONFIG, GRAVES } = require("../modules/fourGravesCore");
const { rosterPanel, rosterPage, PAGE_SIZE } = require("../modules/fourGravesRoster");
const { publicPanel } = require("../modules/fourGravesDiscord");

const event = { id:"37",state:"OPEN",config:DEFAULT_CONFIG };
assert.equal(rosterPanel(event,{entries:[]}),null);
const snapshot = {entries:[
  {grave_id:"barbara_bowkins",discord_id:"1",label:"Player 1",id:"1:1"},
  {grave_id:"barbara_bowkins",discord_id:"1",label:"Player 1(1)",id:"1:2"},
  {grave_id:"barbara_bowkins",discord_id:"2",label:"Player 2",id:"2:1"},
  {grave_id:"cletus_von_wrinkles",discord_id:"1",label:"Player 1(3)",id:"3:1"},
  {grave_id:"cletus_von_wrinkles",discord_id:"3",label:"Player 3",id:"4:1"},
  {grave_id:"cletus_von_wrinkles",discord_id:"3",label:"Player 3(1)",id:"4:2"},
  {grave_id:"scuba_steve_3000",discord_id:"1",label:"Player 1(2)",id:"5:1"},
  {grave_id:"scuba_steve_3000",discord_id:"2",label:"Player 2(1)",id:"6:1"},
  {grave_id:"scuba_steve_3000",discord_id:"3",label:"Player 3(2)",id:"7:1"},
  {grave_id:"barry_buckets",discord_id:"3",label:"Player 3(3)",id:"8:1"},
  {grave_id:"barry_buckets",discord_id:"1",label:"Player 1(4)",id:"9:1"},
]};
const panel = rosterPanel(event,snapshot);
assert.equal(panel.embeds.length,1);
assert.match(panel.embeds[0].title,/DEAD ARE KEEPING SCORE/);
const text = panel.embeds[0].description;
assert.ok(text.indexOf("Cletus' Grave") < text.indexOf("Barry's Grave"));
assert.ok(text.indexOf("Barry's Grave") < text.indexOf("Barbara's Grave"));
assert.ok(text.indexOf("Barbara's Grave") < text.indexOf("Steve's Grave"));
for (const name of [
  "Player 1","Player 1(1)","Player 2","Player 1(3)",
  "Player 3","Player 3(1)","Player 1(2)","Player 2(1)",
  "Player 3(2)","Player 3(3)","Player 1(4)",
]) assert.ok(text.includes(name),name+" must appear exactly as its ticket label");
assert.match(text,/Every name below is a buried ticket/);
assert.match(text,/stones aren't talking/);
for (const word of ["RugDollz","Scribble Otter","5,000","DollzOnApe","Visions of the Void"])
  assert.doesNotMatch(text,new RegExp(word,"i"));
assert.deepEqual(panel.allowedMentions,{parse:[]});
assert.equal(panel.components[0].components.length,4);
assert.deepEqual(panel.components[0].components.map(x=>x.custom_id),[
  "fg1:37:roster:2:0","fg1:37:roster:0:0",
  "fg1:37:roster:1:0","fg1:37:roster:3:0",
]);
assert.equal(panel.embeds[0].footer.text,"four-graves:37:roster");
const first = publicPanel(event);
assert.match(first.embeds[0].description,/THE DEAD DON'T PICK THEIR COMPANY/);
assert.match(first.embeds[0].description,/One of you will eventually choose TRICK or TREAT/);
assert.doesNotMatch(first.embeds[0].description,/RugDollz|DollzOnApe|Chill Bugs/);
assert.deepEqual(first.allowedMentions,{parse:[]});

const newSnapshot = {entries:snapshot.entries.concat(
  {grave_id:"cletus_von_wrinkles",discord_id:"4",label:"New Squig",id:"10:1"}
)};
assert.notEqual(rosterPanel(event,newSnapshot).embeds[0].description,text);
assert.match(rosterPanel(event,newSnapshot).embeds[0].description,/New Squig/);
const page = rosterPage(event,snapshot,2,0);
assert.match(page.content,/Cletus' Grave/);
assert.match(page.embeds[0].description,/Player 3\(1\)/);
assert.deepEqual(page.allowedMentions,{parse:[]});
assert.equal(page.components.length,0);
const empty = rosterPage(event,snapshot,0);
assert.match(empty.content,/Barry's Grave/);
assert.throws(()=>rosterPage(event,snapshot,99),/Unknown grave/);
const testPanel = rosterPanel({...event,config:{...event.config,testMode:true}},snapshot);
assert.match(testPanel.embeds[0].title,/TEST/);
assert.ok(!JSON.stringify(testPanel).includes("@everyone"));

const lots = Array.from({length:204},(_,i)=>({
  grave_id:"barbara_bowkins",
  discord_id:String(i%7),
  label:i===8 ? "@everyone <@123> **evil**" : "Player "+i,
  id:"big:"+i,
}));
const longPanel=rosterPanel(event,{entries:lots});
assert.ok(longPanel.embeds[0].description.length<=4096);
assert.match(longPanel.embeds[0].description,/more tickets/);
assert.doesNotMatch(longPanel.embeds[0].description,/@everyone/);
const longPage=rosterPage(event,{entries:lots},1,0);
assert.equal(longPage.components[0].components[0].disabled,true);
assert.equal(longPage.components[0].components[1].disabled,false);
assert.ok(longPage.embeds[0].description.length<=4096);
assert.doesNotMatch(longPage.embeds[0].description,/@everyone/);
const pageTwo=rosterPage(event,{entries:lots},1,1);
assert.equal(pageTwo.components[0].components[0].disabled,false);
assert.equal(pageTwo.content.includes("Page 2 of"),true);
const last=rosterPage(event,{entries:lots},1,999);
assert.equal(last.components[0].components[1].disabled,true);
assert.equal(PAGE_SIZE,20);
assert.equal(GRAVES.length,4);
console.log("PASS eerie selection/roster copy, ticket identity/order, no mentions/spoilers, and paginated large teams");
