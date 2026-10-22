"use strict";
const { SlashCommandBuilder, ChannelType } = require("discord.js");
const { FourGravesStore } = require("./fourGravesStore");
const { publicPanel, button, row, cid } = require("./fourGravesDiscord");
const { GRAVES, quantity, exportFiles, privateArchives } = require("./fourGravesCore");
const { drawSummary, winnersCsv } = require("./fourGravesDraw");

let deps, store, ready;
const enabled = () =>
  String(process.env.FOUR_GRAVES_ENABLED || "false").toLowerCase() === "true";

function initFourGraves(d) {
  deps = d;
  store = new FourGravesStore(d.pool, {
    eventKey: process.env.FOUR_GRAVES_EVENT_KEY || "squigs-four-graves-2026",
    sourceEventKey:
      process.env.TRICK_OR_TREAT_EVENT_KEY || "squigs-trick-or-treat-2026",
    simpleDraw: true,
  });
}
async function start() {
  if (!ready) ready = store.migrate().catch(err => { ready = null; throw err; });
  await ready;
  // NO recovery worker, private-team roles, 15-minute timer, or decision buttons.
}
function buildSlashCommand() {
  return new SlashCommandBuilder()
    .setName("fourgraves")
    .setDescription("Trick or Treat: bury tickets in four graves, then draw winners")
    .addSubcommand(s => s.setName("admin").setDescription("Admin: open control panel"))
    .addSubcommand(s => s.setName("status").setDescription("View grave entry totals"))
    .addSubcommand(s => s.setName("draw").setDescription("Admin: confirm one random draw per prize"))
    .addSubcommand(s => s.setName("publish").setDescription("Admin: publish saved winners without tagging anyone"))
    .addSubcommand(s => s.setName("configure")
      .setDescription("Admin: configure panels, names, and prize pool")
      .addChannelOption(o => o.setName("channel")
        .setDescription("Grave ticket-allocation panel channel")
        .addChannelTypes(ChannelType.GuildText))
      .addChannelOption(o => o.setName("results-channel")
        .setDescription("Channel for winners; defaults to grave panel channel")
        .addChannelTypes(ChannelType.GuildText))
      .addAttachmentOption(o => o.setName("image")
        .setDescription("Optional grave panel artwork"))
      .addStringOption(o => o.setName("image-url")
        .setDescription("Durable HTTPS artwork URL"))
      .addStringOption(o => o.setName("config-json")
        .setDescription("Optional four grave names and mystery prizes"))
      .addBooleanOption(o => o.setName("finalize-prizes")
        .setDescription("Confirm configured prize pool before closing")))
    .addSubcommand(s => s.setName("export")
      .setDescription("Admin: private ticket, audit and drawn-winner CSV ZIP")
      .addStringOption(o => o.setName("grave").setDescription("Grave")
        .addChoices({name:"All",value:"all"},
          ...GRAVES.map(g => ({name:g.name,value:g.id}))))
      .addStringOption(o => o.setName("format").setDescription("Ticket export format")
        .addChoices(...["all","contestants","master","audit"].map(value =>
          ({name:value,value})))));
}
async function reply(i, payload) {
  const p = typeof payload === "string" ? { content: payload } : payload;
  const safe = { ...p, allowedMentions: { parse: [] } };
  if (i.deferred || i.replied) return i.editReply(safe);
  return i.reply({ ...safe, flags: 64 });
}
function admin(i) {
  if (!deps.isAdmin(i)) throw Error("Administrator access required.");
}
function sourceCheck(e) {
  if (e.config?.testMode ||
      e.source_event_key !== store.sourceEventKey ||
      e.event_key !== store.eventKey)
    throw Error("Four Graves event identity mismatch; refusing to operate.");
  return e;
}
function status(data, isAdmin) {
  const { event: e, snapshot: s, draw } = data;
  const state = e.state === "CLOSED_COUNTDOWN" ? "CLOSED" :
    e.state === "REVEALED" ? "RESULTS PUBLISHED" : e.state;
  let out = "**TRICK OR TREAT — FOUR GRAVES: " + state + "**\n" +
    "Participants: " + s.users.filter(u => u.allocated).length +
    "\nBuried tickets: " + s.entries.length + "\n";
  for (const [index,g] of GRAVES.entries()) {
    const entries = s.entries.filter(x => x.grave_id === g.id);
    out += e.config.names[index] + ": " + entries.length +
      " tickets from " + new Set(entries.map(x=>x.discord_id)).size +
      " holders\n";
  }
  if (!isAdmin) return {content:out.slice(0,1900)};
  const total=s.users.reduce((n,u)=>n+u.total,0);
  const awarded=draw ? draw.results.filter(x=>x.winner).length : 0;
  out += "\nEarned: " + total + "; not buried: " + (total-s.entries.length) +
    "\nPrize pool finalized: " + !!e.config.prizesFinalized +
    "\nSnapshot: " + (e.snapshot_id || "live") +
    "\nRandom draw: " + (draw ? awarded + "/" + draw.results.length +
      " awarded (" + draw.results_hash.slice(0,16) + "...)" : "not yet run") +
    "\nWinners published: " + (draw?.published_at || "no");
  return {content:out.slice(0,1900)};
}
function controls(e) {
  const actions=[
    ["post","POST GRAVE PANEL"],
    ["open","OPEN GRAVES"],
    ["close","CLOSE GRAVES"],
    ["draw","DRAW WINNERS"],
    ["publish","PUBLISH RESULTS"],
    ["status","STATUS"],
    ["export","EXPORT"],
  ];
  return {
    content:"**FOUR GRAVES — ADMIN**\n" +
      "1. Configure prizes and panel. 2. Post and open the graves. " +
      "3. Close to freeze tickets. 4. Draw once. 5. Publish the saved winners.\n" +
      "No separate games, roles or countdown.",
    components:[
      row(...actions.slice(0,4).map(([action,label])=>button(cid(e,"admin",action),label))),
      row(...actions.slice(4).map(([action,label])=>button(cid(e,"admin",action),label))),
    ],
  };
}
function validatePatch(input) {
  if (!input || typeof input !== "object" || Array.isArray(input))
    throw Error("Configuration must be a JSON object.");
  const patch={};
  for (const k of Object.keys(input))
    if (!["names","mystery"].includes(k))
      throw Error("Unsupported configuration: " + k);
  if (input.names !== undefined) {
    if (!Array.isArray(input.names) || input.names.length !== 4 ||
        input.names.some(x=>typeof x!=="string" || !x.trim() || x.length>40))
      throw Error("Provide exactly four nonempty grave names (max 40 characters).");
    patch.names=input.names;
  }
  if (input.mystery !== undefined) {
    if (!Array.isArray(input.mystery) || input.mystery.length>10 ||
        input.mystery.some(x=>!x || typeof x.name!=="string" ||
          !x.name.trim() || x.name.length>80 ||
          !Number.isInteger(x.quantity) || x.quantity<1 || x.quantity>100))
      throw Error("Mystery rewards need a name and quantity between 1 and 100.");
    patch.mystery=input.mystery;
    patch.prizesFinalized=false;
  }
  return patch;
}
async function getChannel(guild,id) {
  if (!id) throw Error("Configure the destination channel first.");
  const channel=await guild.channels.fetch(id);
  if (!channel || channel.guildId!==guild.id ||
      channel.type!==ChannelType.GuildText)
    throw Error("Configured destination is not a text channel in this server.");
  return channel;
}
async function findMessage(channel, messageId, marker) {
  if (messageId) {
    let m;
    try { m=await channel.messages.fetch(messageId); }
    catch (err) {if (Number(err.code)!==10008) throw err;}
    if (m) {
      if (m.author.id!==deps.client.user.id ||
          !m.embeds.some(e=>e.footer?.text===marker))
        throw Error("Saved message identity mismatch; refusing to edit it.");
      return m;
    }
  }
  // Recover a sent-but-not-persisted message, rather than posting duplicates.
  let before;
  for (let i=0;i<10;i++) {
    const batch=await channel.messages.fetch({limit:100,...(before?{before}:{})});
    const found=[...batch.values()].find(m=>
      m.author.id===deps.client.user.id &&
      m.embeds.some(e=>e.footer?.text===marker));
    if (found) return found;
    if (batch.size<100) break;
    before=batch.last().id;
  }
  return null;
}
async function postPanel(i) {
  const d=await store.read(i.guildId);
  const e=sourceCheck(d.event);
  const dest=e.panel_channel_id || i.channelId;
  const channel=await getChannel(i.guild,dest);
  const payload=publicPanel(e);
  const marker="four-graves:"+e.id+":public";
  const previous=await findMessage(channel,e.panel_message_id,marker);
  const m=previous ? await previous.edit(payload) : await channel.send(payload);
  await deps.pool.query(
    "UPDATE four_graves_events SET panel_channel_id=$2,panel_message_id=$3 WHERE id=$1 AND event_key=$4",
    [e.id,channel.id,m.id,store.eventKey]
  );
  return "Grave ticket panel " + (previous?"refreshed":"posted") +
    " in <#"+channel.id+">. No private team channels or roles were created.";
}
async function publish(i) {
  admin(i);
  const d=await store.read(i.guildId);
  const e=sourceCheck(d.event);
  if (!d.draw) throw Error("Draw the winners before publishing.");
  if (d.draw.published_at) return "Winners already published in <#"+
    d.draw.channel_id+">. Draw and announcement are immutable.";
  if (e.state!=="CLOSED_COUNTDOWN" || e.deadline)
    throw Error("Cannot publish from a legacy countdown or unexpected event state.");
  const payload=drawSummary(e,d.draw.results);
  payload.embeds[0].footer={text:"four-graves:"+e.id+":winners"};
  const channel=await getChannel(i.guild,e.config.resultsChannelId||e.panel_channel_id);
  const client=await deps.pool.connect();
  let locked=false;
  try {
    locked=(await client.query("SELECT pg_try_advisory_lock(46292,$1::int) AS locked",[e.id])).rows[0].locked;
    if (!locked) throw Error("A winner announcement is already in progress.");
    const latest=await store.read(i.guildId);
    if (latest.draw?.published_at)
      return "Winners already published in <#"+latest.draw.channel_id+">.";
    if (!latest.draw || latest.draw.results_hash!==d.draw.results_hash)
      throw Error("Stored draw changed unexpectedly; refusing to publish.");
    const marker="four-graves:"+e.id+":winners";
    const found=await findMessage(channel,latest.draw.message_id,marker);
    const m=found ? await found.edit(payload) : await channel.send(payload);
    await store.markPublished(i.guildId,i.user.id,channel.id,m.id);
  } finally {
    if (locked) await client.query("SELECT pg_advisory_unlock(46292,$1::int)",[e.id]);
    client.release();
  }
  try { await postPanel(i); }
  catch (err) { console.warn("[FourGraves] Results published but panel refresh failed:",err.message); }
  return "Winners published in <#"+channel.id+"> without mentions. " +
    "Prizes still need to be delivered manually.";
}
async function exportReply(i,grave="all",format="all") {
  admin(i);
  const d=await store.read(i.guildId);
  const files=exportFiles(d.event,d.snapshot,grave,format);
  if (d.draw) files["four_graves_prize_winners.csv"]=
    winnersCsv(d.event,d.draw.results.filter(x=>grave==="all" || x.grave_id===grave));
  const parts=privateArchives(files,i.attachmentSizeLimit||8*1024*1024);
  await reply(i,{
    content:"Private ZIP: "+d.snapshot.entries.length+" buried tickets"+
      (d.draw?" and permanently recorded prize winners.":" (no draw yet)."),
    files:[parts[0]],
  });
  for (const part of parts.slice(1))
    await i.followUp({files:[part],flags:64,allowedMentions:{parse:[]}});
}
async function doAction(i,e,action,confirmed=false) {
  admin(i);
  const hazardous=["open","close","draw","publish"];
  if (hazardous.includes(action) && !confirmed) {
    const warnings={
      open:"OPEN the four graves? Holders can permanently bury tickets.",
      close:"CLOSE all four graves permanently? Freeze all buried entries now. "+
        "There is no countdown and no re-opening.",
      draw:"DRAW all prizes now? Every grave draws from its own frozen tickets. "+
        "The winners are saved once; THERE IS NO REDRAW.",
      publish:"PUBLISH the recorded winners publicly? This announces the final "+
        "draw without tagging users. It does not transfer rewards.",
    };
    return reply(i,{
      content:"**CONFIRM: "+warnings[action]+"**",
      components:[row(
        button(cid(e,"confirm-admin",action,e.generation),"CONFIRM "+action.toUpperCase(),false,4),
        button(cid(e,"cancel"),"CANCEL"),
      )],
    });
  }
  if (action==="status") return reply(i,status(await store.read(i.guildId),true));
  if (action==="export") return exportReply(i);
  if (action==="post") return reply(i,await postPanel(i));
  if (action==="open" || action==="close") {
    await store.transition(i.guildId,i.user.id,action);
    const after=await store.read(i.guildId);
    if (after.event.panel_channel_id) await postPanel(i);
    return reply(i,action==="open"
      ?"The four graves are OPEN. Tickets can now be buried."
      :"The graves are CLOSED. Entries are frozen. "+
       "Next: DRAW WINNERS, then PUBLISH RESULTS.");
  }
  if (action==="draw") {
    const record=await store.draw(i.guildId,i.user.id);
    return reply(i,"The one-time draw is saved and cannot be re-rolled.\n"+
      "Awarded prizes: "+record.results.filter(x=>x.winner).length+
      "/"+record.results.length+".\n"+
      "Verification hash: "+record.results_hash+
      "\nUse /fourgraves export to review winners before publishing.");
  }
  if (action==="publish") return reply(i,await publish(i));
  throw Error("Unknown or retired Four Graves action.");
}
async function handle(i) {
  const isCommand=i.isChatInputCommand?.() && i.commandName==="fourgraves";
  if (!isCommand && !String(i.customId||"").startsWith("fg1:")) return false;
  try {
    if (!enabled()) throw Error("Four Graves is currently disabled. Existing claims are unchanged.");
    if (!i.guildId) throw Error("Use this in the server.");
    if (isCommand) {
      await i.deferReply({flags:64});
      const sub=i.options.getSubcommand();
      if (sub==="admin") {
        admin(i);
        const e=sourceCheck(await store.setup(i.guildId,i.user.id));
        return reply(i,controls(e));
      }
      if (sub==="status")
        return reply(i,status(await store.read(i.guildId),deps.isAdmin(i)));
      if (sub==="draw" || sub==="publish") {
        admin(i);
        const e=sourceCheck((await store.read(i.guildId)).event);
        return doAction(i,e,sub);
      }
      if (sub==="export") return exportReply(i,
        i.options.getString("grave")||"all",
        i.options.getString("format")||"all");
      if (sub==="configure") {
        admin(i);
        const e=sourceCheck(await store.setup(i.guildId,i.user.id));
        const patch=validatePatch(JSON.parse(i.options.getString("config-json")||"{}"));
        const channel=i.options.getChannel("channel");
        const results=i.options.getChannel("results-channel");
        const attachment=i.options.getAttachment("image");
        const image=attachment?.url||i.options.getString("image-url");
        if (image) {
          const url=new URL(image);
          if (url.protocol!=="https:" || url.username || url.password)
            throw Error("Use an HTTPS image URL.");
          if (attachment && !/^image\/(png|jpeg|webp|gif)$/.test(attachment.contentType||""))
            throw Error("Use a PNG, JPG, WebP, or GIF.");
          patch.image=image;
        }
        const flag=i.options.getBoolean("finalize-prizes");
        if (flag!==null) patch.prizesFinalized=flag;
        if (results) patch.resultsChannelId=results.id;
        if (Object.keys(patch).length) await store.configure(i.guildId,i.user.id,patch);
        if (channel) await store.transaction(async c=>{
          const current=sourceCheck(await store.event(c,i.guildId,true));
          if (current.closed_at) throw Error("Destinations are frozen after closing.");
          if (current.panel_message_id && current.panel_channel_id!==channel.id)
            throw Error("The existing ticket panel cannot be moved.");
          await c.query("UPDATE four_graves_events SET panel_channel_id=$2 WHERE id=$1",
            [current.id,channel.id]);
        });
        return reply(i,"Configuration saved. Use POST GRAVE PANEL in the admin controls. "+
          "No game setup is required.");
      }
      throw Error("Unknown Four Graves command.");
    }
    const [,eventId,action,...args]=i.customId.split(":");
    // Discord requires showing modals immediately. The submit is rechecked transactionally.
    if (action==="quantity") {
      if (args[1]!==i.user.id) throw Error("That private button belongs to someone else.");
      await i.showModal({
        custom_id:cid({id:eventId},"amount",args[0]),
        title:"Bury your tickets",
        components:[row({
          type:4,custom_id:"quantity",
          label:"Positive whole-number ticket quantity",
          style:1,required:true,max_length:10,
        })],
      });
      return true;
    }
    await i.deferReply({flags:64});
    const d=await store.read(i.guildId),e=sourceCheck(d.event);
    if (String(e.id)!==eventId) throw Error("This button belongs to another event.");
    if (action==="cancel") return reply(i,"Cancelled. No tickets were buried.");
    if (action==="admin" || action==="confirm-admin") {
      if (action==="confirm-admin" && Number(args[1])!==e.generation)
        throw Error("Stale confirmation. Open a new admin panel.");
      return doAction(i,e,args[0],action==="confirm-admin");
    }
    const gi=Number(args[0]), g=GRAVES[gi];
    if (!g) throw Error("Unknown grave.");
    const user={
      id:i.user.id,
      username:i.user.username,
      display_name:i.member?.displayName||i.user.globalName||i.user.username,
    };
    const balance=d.snapshot.users.find(u=>u.discord_id===i.user.id);
    const available=balance?balance.total-balance.allocated:0;
    if (action==="grave") {
      if (e.state!=="OPEN") throw Error("THE GRAVES ARE SEALED.");
      return reply(i,{
        content:"**"+e.config.names[gi]+"**\nEarned: "+(balance?.total||0)+
          "\nAlready buried: "+(balance?.allocated||0)+
          "\nAvailable: "+available+"\nIn this grave: "+(balance?.graves[gi]||0),
        components:[row(button(cid(e,"quantity",gi,i.user.id),"ENTER QUANTITY"))],
      });
    }
    if (action==="amount") {
      if (e.state!=="OPEN") throw Error("THE GRAVES ARE SEALED.");
      const amount=quantity(i.fields.getTextInputValue("quantity"));
      if (amount>available) throw Error("NOT ENOUGH TICKETS. Available: "+available);
      return reply(i,{
        content:"Bury **"+amount+"** tickets in **"+e.config.names[gi]+
          "**?\nOnce buried, tickets cannot be moved or withdrawn.",
        components:[row(
          button(cid(e,"bury",gi,amount,i.user.id,i.id),"BURY MY TICKETS",false,4),
          button(cid(e,"cancel"),"CANCEL"),
        )],
      });
    }
    if (action==="bury") {
      if (args[2]!==i.user.id) throw Error("This confirmation belongs to another user.");
      const receipt=await store.allocate(i.guildId,user,g.id,quantity(args[1]),args[3]);
      const after=await store.read(i.guildId);
      const u=after.snapshot.users.find(x=>x.discord_id===i.user.id);
      return reply(i,"YOUR TICKETS ARE BURIED.\n"+
        receipt.quantity+" tickets in "+e.config.names[gi]+
        ". Remaining: "+(u.total-u.allocated)+".");
    }
    throw Error("That old grave action was retired. Prizes are now drawn randomly.");
  } catch (err) {
    await reply(i,{content:String(err.message).slice(0,1700),components:[]});
  }
  return true;
}
module.exports={initFourGraves,start,buildSlashCommand,handle,enabled,validatePatch,status};
