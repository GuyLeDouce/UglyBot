"use strict";

const { SlashCommandBuilder, ChannelType, PermissionFlagsBits: P } = require("discord.js");
const { FourGravesStore } = require("./fourGravesStore");
const { FourGravesDiscord, button, row, cid } = require("./fourGravesDiscord");
const { GRAVES, exportFiles, privateArchives } = require("./fourGravesCore");

// Separate key, separate source table and separate Discord resources.
// This feature intentionally never writes to Trick or Treat claim tables.
const TEST_KEY = "four-graves-private-test-v1";
const SOURCE_KEY = "four-graves-private-test-synthetic";
const enabled = () => String(process.env.FOUR_GRAVES_TEST_ENABLED || "false").toLowerCase() === "true";
let deps, store, worker;

function init(d) {
  deps = d;
  store = new FourGravesStore(d.pool, {
    eventKey: TEST_KEY,
    sourceEventKey: SOURCE_KEY,
    testMode: true,
  });
  worker = new FourGravesDiscord(store, { ...d, enabled });
}

async function start() {
  await store.migrate();
  if (enabled()) worker.start();
}

function buildSlashCommand() {
  const b = new SlashCommandBuilder()
    .setName("fourgravestest")
    .setDescription("Private production-server finale rehearsal with fake tickets")
    .setDefaultMemberPermissions(P.ManageGuild)
    .setDMPermission(false);
  const actions = [
    ["setup", "Create a private test category, controls, roles and channels"],
    ["seed", "Generate 20 fake people and 100 fake tickets"],
    ["open", "Open test graves and automatically bury fake tickets"],
    ["close", "Freeze fake entries and begin the real 15-minute countdown"],
    ["advance", "TEST ONLY: skip the countdown to activate decision buttons"],
    ["status", "View rehearsal progress, channel IDs, roles and errors"],
    ["repair", "Retry regular test resource and permission reconciliation"],
    ["export", "Download private fake contestant CSV exports"],
    ["reset", "Delete only the verified test resources and fake data"],
  ];
  for (const [name, description] of actions) {
    b.addSubcommand(s => {
      s.setName(name).setDescription(description);
      if (name === "reset")
        s.addBooleanOption(o => o.setName("confirm")
          .setDescription("I understand this destroys the test rehearsal ONLY")
          .setRequired(true));
      return s;
    });
  }
  return b;
}

async function reply(i, payload) {
  const data = typeof payload === "string" ? { content: payload } : payload;
  const safe = { ...data, allowedMentions: { parse: [] } };
  if (i.deferred || i.replied) return i.editReply(safe);
  return i.reply({ ...safe, flags: 64 });
}

function owner(i, e) {
  if (!i.guildId || !deps.isAdmin(i)) throw Error("Server administrator access required.");
  if (e && (e.config?.testMode !== true ||
      e.source_event_key !== SOURCE_KEY ||
      e.event_key !== TEST_KEY ||
      e.config.testOwnerId !== i.user.id))
    throw Error("Test owner or event isolation mismatch. Refusing to operate.");
}

async function event(guildId) {
  const r = await deps.pool.query(
    "SELECT * FROM four_graves_events WHERE event_key=$1 AND guild_id=$2",
    [TEST_KEY, guildId],
  );
  return r.rows[0] || null;
}

const categoryName = e => "four-graves-test-fg" + e.id;
const controlMarker = e => "four-graves-test:" + e.id + ":control";

function privateOverwrites(guildId, userId) {
  const allow = [P.ViewChannel, P.SendMessages, P.ReadMessageHistory];
  return [
    { id: guildId, deny: [P.ViewChannel] },
    { id: userId, type: 1, allow },
    { id: deps.client.user.id, type: 1,
      allow: [...allow, P.ManageChannels, P.ManageMessages, P.AttachFiles, P.EmbedLinks] },
  ];
}

async function setup(i) {
  const e = await store.setup(i.guildId, i.user.id);
  owner(i, e);
  const guild = i.guild;
  await guild.channels.fetch();
  let category = e.config.category ? guild.channels.cache.get(e.config.category) : null;
  if (category && (category.name !== categoryName(e) ||
                   category.type !== ChannelType.GuildCategory))
    throw Error("Stored test category marker mismatch; refusing to reuse it.");
  if (!category) {
    category = guild.channels.cache.find(c =>
      c.type === ChannelType.GuildCategory && c.name === categoryName(e));
    if (!category)
      category = await guild.channels.create({
        name: categoryName(e),
        type: ChannelType.GuildCategory,
        permissionOverwrites: privateOverwrites(i.guildId, i.user.id),
        reason: "Four Graves private simulation only",
      });
    await store.configure(i.guildId, i.user.id, { category: category.id });
  }
  const current = await event(i.guildId);
  let control = current.panel_channel_id ? guild.channels.cache.get(current.panel_channel_id) : null;
  if (control && (control.topic !== controlMarker(e) ||
      control.type !== ChannelType.GuildText ||
      control.parentId !== category.id))
    throw Error("Stored test control channel marker mismatch; refusing to reuse it.");
  if (!control) {
    await guild.channels.fetch();
    control = guild.channels.cache.find(c => c.topic === controlMarker(e));
    if (control && (control.type !== ChannelType.GuildText ||
                    control.parentId !== category.id))
      throw Error("Existing marked test control is outside the private category.");
    if (!control)
      control = await guild.channels.create({
        name: "test-four-graves-control",
        type: ChannelType.GuildText,
        topic: controlMarker(e),
        parent: category.id,
        permissionOverwrites: privateOverwrites(i.guildId, i.user.id),
        reason: "Four Graves private simulation only",
      });
    await deps.pool.query(
      "UPDATE four_graves_events SET panel_channel_id=$2 WHERE id=$1 AND event_key=$3",
      [e.id, control.id, TEST_KEY],
    );
  }
  await worker.tick();
  return "🧪 **Private Four Graves rehearsal prepared.**\nCategory: <#" +
    category.id + ">\nControl panel: <#" + control.id +
    ">\nThe actual Four Graves worker creates the four TEST grave channels and roles. " +
    "Use /fourgravestest status to check permissions and errors.";
}

async function seed(i) {
  await store.transaction(async c => {
    const e = await store.event(c, i.guildId, true);
    owner(i, e);
    if (e.state !== "SETUP") throw Error("Seed only before opening. Reset to run another test.");
    for (let n = 1; n <= 20; n++) {
      const id = "test-fake-" + String(n).padStart(2, "0");
      await c.query(
        "INSERT INTO four_graves_test_tickets(event_key,guild_id,discord_id,treats,tricks) " +
        "VALUES($1,$2,$3,3,2) ON CONFLICT(event_key,guild_id,discord_id) " +
        "DO UPDATE SET treats=EXCLUDED.treats,tricks=EXCLUDED.tricks",
        [TEST_KEY, i.guildId, id],
      );
    }
    await c.query(
      "INSERT INTO four_graves_test_tickets(event_key,guild_id,discord_id,treats,tricks) " +
      "VALUES($1,$2,$3,8,4) ON CONFLICT(event_key,guild_id,discord_id) " +
      "DO UPDATE SET treats=EXCLUDED.treats,tricks=EXCLUDED.tricks",
      [TEST_KEY, i.guildId, i.user.id],
    );
  });
  return "🧪 Seeded **20 fake participants with 100 fake tickets**, plus " +
    "**12 synthetic tickets for your real Discord account**. " +
    "No Trick or Treat claims, wallets or balances were used.";
}

async function open(i) {
  const e = await event(i.guildId);
  if (!e) throw Error("Run /fourgravestest setup first.");
  owner(i, e);
  if (!["SETUP", "OPEN"].includes(e.state))
    throw Error("The rehearsal is closed. Reset before opening again.");
  const users = await store.earned(deps.pool, { guild_id: i.guildId });
  if (users.length !== 21 || !users.find(u => u.discord_id === i.user.id))
    throw Error("Run /fourgravestest seed before opening.");
  if (e.state === "SETUP") await store.transition(i.guildId, i.user.id, "open");
  // Idempotency keys make an interrupted OPEN command safe to retry.
  for (const u of users) {
    if (u.discord_id.startsWith("test-fake-")) {
      const n = Number(u.discord_id.slice(-2));
      const name = "Test Participant " + String(n).padStart(2, "0");
      await store.allocate(i.guildId,
        { id: u.discord_id, username: name, display_name: name },
        GRAVES[(n - 1) % 4].id, 5,
        "test-autobury-" + i.guildId + "-" + u.discord_id,
      );
    } else {
      for (const g of GRAVES) {
        await store.allocate(i.guildId,
          { id: i.user.id, username: i.user.username,
            display_name: i.member?.displayName || i.user.globalName || i.user.username },
          g.id, 1, "test-autobury-" + i.guildId + "-" + i.user.id + "-" + g.id,
        );
      }
    }
  }
  await worker.tick();
  return "🧪 **Test graves OPEN.** 104 tickets buried (100 fake + 4 yours); " +
    "8 of your synthetic tickets remain available to practice the quantity modal and burial. " +
    "The private test panel's four grave buttons are now active.";
}

async function close(i) {
  const e = await event(i.guildId);
  if (!e) throw Error("Run /fourgravestest setup first.");
  owner(i, e);
  if (e.state !== "OPEN") throw Error("Test graves must be OPEN before closing.");
  const d = await store.read(i.guildId);
  if (!d.snapshot.entries.length) throw Error("No test tickets have been buried.");
  await store.transition(i.guildId, i.user.id, "close", {
    [i.user.id]: { username: i.user.username,
      display_name: i.member?.displayName || i.user.globalName || i.user.username },
  });
  await worker.tick();
  return "🧪 **Test graves CLOSED.** A genuine **15-minute countdown** " +
    "has started, and test-only grave roles/channel permissions are being applied. " +
    "Use /fourgravestest advance to skip the remainder of the TEST countdown.";
}

async function advance(i) {
  await store.transaction(async c => {
    const e = await store.event(c, i.guildId, true);
    owner(i, e);
    if (e.state !== "CLOSED_COUNTDOWN")
      throw Error("Only a closed, counting-down test can be advanced.");
    await c.query(
      "UPDATE four_graves_events SET deadline=clock_timestamp()-interval '1 second' " +
      "WHERE id=$1 AND event_key=$2",
      [e.id, TEST_KEY],
    );
    await store.audit(c, e.id, i.user.id, "test-skip-countdown");
  });
  await worker.tick();
  return "🧪 **Test countdown advanced.** TRICK/TREAT buttons should now be active " +
    "in each populated private test grave. First eligible click per grave wins.";
}

async function status(i) {
  const d = await store.read(i.guildId);
  owner(i, d.event);
  const e = d.event;
  const s = d.snapshot;
  const lines = [
    "🧪 **FOUR GRAVES TEST — " + e.state + "**",
    "Synthetic people: " + s.users.filter(u => u.discord_id.startsWith("test-fake-")).length +
      " | Tickets total: " + s.users.reduce((n, u) => n + u.total, 0) +
      " | Buried: " + s.entries.length,
    "Countdown: " + (e.deadline ? "<t:" +
      Math.floor(new Date(e.deadline).getTime() / 1000) + ":R>" : "not started"),
    "Control: " + (e.panel_channel_id ? "<#" + e.panel_channel_id + ">" : "missing"),
  ];
  for (const g of GRAVES) {
    const x = d.graves.find(z => z.grave_id === g.id);
    lines.push(g.name + ": " +
      s.entries.filter(z => z.grave_id === g.id).length + " entries, " +
      (x.channel_id ? "<#" + x.channel_id + ">" : "CHANNEL MISSING") +
      ", role " + (x.role_id || "missing") +
      ", " + (x.decision?.choice || "waiting"));
  }
  const problems = d.operations.filter(x => x.last_error);
  lines.push("Worker errors: " + (problems.length ?
    problems.map(x => x.operation_key + ": " + x.last_error).join("; ") : "none"));
  return lines.join("\n").slice(0, 1850);
}

async function exportTest(i) {
  const d = await store.read(i.guildId);
  owner(i, d.event);
  const files = exportFiles(d.event, d.snapshot, "all", "all");
  const parts = privateArchives(files, i.attachmentSizeLimit || 8 * 1024 * 1024);
  await reply(i, {
    content: "🧪 **TEST ONLY** — " + d.snapshot.entries.length +
      " synthetic tickets. " + parts.length + " private archive(s).",
    files: [parts[0]],
  });
  for (const part of parts.slice(1))
    await i.followUp({ flags: 64, files: [part], allowedMentions: { parse: [] } });
}

// Do not delete anything unless both the database event and the Discord
// resource identity prove it was created specifically by this rehearsal.
async function reset(i) {
  const e = await event(i.guildId);
  if (!e) return "No private test exists to reset.";
  owner(i, e);
  const client = await deps.pool.connect();
  let locked = false;
  try {
    locked = (await client.query(
      "SELECT pg_try_advisory_lock(46291,$1::int) AS locked", [e.id]
    )).rows[0].locked;
    if (!locked) throw Error("Test worker is busy. Retry reset shortly.");
    const guild = i.guild;
    await guild.channels.fetch();
    await guild.roles.fetch();
    const graves = (await client.query(
      "SELECT * FROM four_graves_graves WHERE event_id=$1", [e.id]
    )).rows;
    // Validate ALL owned resources before the first destructive API call.
    for (const g of graves) {
      const mark = "four-graves:" + e.id + ":" + g.grave_id;
      const ch = g.channel_id && guild.channels.cache.get(g.channel_id);
      if (ch && (ch.topic !== mark || !ch.name.startsWith("test-grave-")))
        throw Error("Unrecognized channel " + g.channel_id + "; aborting reset.");
      const role = g.role_id && guild.roles.cache.get(g.role_id);
      if (role && (!role.name.startsWith("[TEST] Grave ") ||
                   !role.name.endsWith("[FG" + e.id + "]")))
        throw Error("Unrecognized role " + g.role_id + "; aborting reset.");
    }
    const control = e.panel_channel_id && guild.channels.cache.get(e.panel_channel_id);
    if (control && (control.topic !== controlMarker(e) ||
                    control.name !== "test-four-graves-control"))
      throw Error("Unrecognized test control channel; aborting reset.");
    const cat = e.config.category && guild.channels.cache.get(e.config.category);
    if (cat && (cat.type !== ChannelType.GuildCategory ||
                cat.name !== categoryName(e)))
      throw Error("Unrecognized test category; aborting reset.");

    // Also recover an in-flight Discord creation whose ID was not persisted.
    for (const g of graves) {
      const mark = "four-graves:" + e.id + ":" + g.grave_id;
      for (const ch of [...guild.channels.cache.values()].filter(c => c.topic === mark))
        if (ch.name.startsWith("test-grave-"))
          await ch.delete("Private Four Graves TEST reset");
        else throw Error("A channel marker exists without the test name; aborting reset.");
      for (const role of [...guild.roles.cache.values()].filter(r =>
        r.name.startsWith("[TEST] Grave ") && r.name.endsWith("[FG" + e.id + "]")))
        await role.delete("Private Four Graves TEST reset");
    }
    for (const ch of [...guild.channels.cache.values()].filter(
      c => c.topic === controlMarker(e)))
      if (ch.name === "test-four-graves-control")
        await ch.delete("Private Four Graves TEST reset");
      else throw Error("Control marker mismatch; aborting reset.");
    await guild.channels.fetch();
    const category = guild.channels.cache.find(c =>
      c.type === ChannelType.GuildCategory && c.name === categoryName(e));
    if (category) {
      if (guild.channels.cache.some(c => c.parentId === category.id))
        throw Error("Test category still has channels; refusing to remove test records.");
      await category.delete("Private Four Graves TEST reset");
    }
    await store.transaction(async c => {
      const current = await store.event(c, i.guildId, true);
      owner(i, current);
      await c.query("DELETE FROM four_graves_audit WHERE event_id=$1", [e.id]);
      await c.query("DELETE FROM four_graves_operations WHERE event_id=$1", [e.id]);
      await c.query("DELETE FROM four_graves_awards WHERE event_id=$1", [e.id]);
      await c.query("DELETE FROM four_graves_allocations WHERE event_id=$1", [e.id]);
      await c.query("DELETE FROM four_graves_graves WHERE event_id=$1", [e.id]);
      await c.query(
        "DELETE FROM four_graves_events WHERE id=$1 AND event_key=$2",
        [e.id, TEST_KEY],
      );
      await c.query(
        "DELETE FROM four_graves_test_tickets WHERE event_key=$1 AND guild_id=$2",
        [TEST_KEY, i.guildId],
      );
    });
    return "🧪 **Private rehearsal reset.** Only verified TEST roles, channels " +
      "and synthetic records were removed. Live Trick or Treat is unchanged.";
  } finally {
    if (locked) await client.query(
      "SELECT pg_advisory_unlock(46291,$1::int)", [e.id]);
    client.release();
  }
}

function testInteraction(i) {
  const parts = String(i.customId || "").split(":");
  return parts[0] === "fg1" && /^\d+$/.test(parts[1] || "") ? parts : null;
}

async function handleTestComponent(i, e, parts) {
  owner(i, e);
  const [, , action, ...args] = parts;
  if (action === "quantity") {
    if (i.channelId !== e.panel_channel_id || e.state !== "OPEN")
      throw Error("Use the open TEST control panel to bury tickets.");
    if (args[1] !== i.user.id) throw Error("That quantity button belongs to another user.");
    await i.showModal({
      custom_id: cid(e, "amount", args[0]),
      title: "Bury TEST tickets",
      components: [{ type: 1, components: [{
        type: 4, custom_id: "quantity", label: "Number of fake tickets",
        style: 1, required: true, max_length: 10,
      }] }],
    });
    return;
  }
  await i.deferReply({ flags: 64 });
  if (action === "cancel") return reply(i, "Cancelled. No fake tickets buried.");
  if (["grave", "amount", "bury"].includes(action) &&
      i.channelId !== e.panel_channel_id)
    throw Error("Use the private TEST control panel.");
  const gi = Number(args[0]);
  const g = GRAVES[gi];
  if (!g) throw Error("Unknown grave.");
  if (action === "grave") {
    if (e.state !== "OPEN") throw Error("The test graves are not open.");
    const d = await store.read(i.guildId);
    const u = d.snapshot.users.find(x => x.discord_id === i.user.id);
    const available = u ? u.total - u.allocated : 0;
    return reply(i, {
      content: "🧪 **" + g.name + " — TEST**\nEarned: " + (u?.total || 0) +
        "\nAlready buried: " + (u?.allocated || 0) +
        "\nAvailable: " + available +
        "\nIn this grave: " + (u?.graves[gi] || 0),
      components: [row(button(cid(e, "quantity", gi, i.user.id), "ENTER QUANTITY"))],
    });
  }
  if (action === "amount") {
    if (!i.isModalSubmit?.() || e.state !== "OPEN")
      throw Error("Test allocation modal is no longer available.");
    const n = Number(i.fields.getTextInputValue("quantity"));
    if (!Number.isSafeInteger(n) || n < 1 || n > 2147483647)
      throw Error("Enter a positive whole-number quantity.");
    const d = await store.read(i.guildId);
    const u = d.snapshot.users.find(x => x.discord_id === i.user.id);
    const available = u ? u.total - u.allocated : 0;
    if (n > available) throw Error("Not enough synthetic tickets: " + available);
    return reply(i, {
      content: "🧪 TEST ONLY — Bury **" + n + "** fake tickets with **" +
        g.name + "**? This is permanent until TEST reset.",
      components: [row(
        button(cid(e, "bury", gi, n, i.user.id, i.id), "BURY TEST TICKETS", false, 4),
        button(cid(e, "cancel"), "CANCEL"),
      )],
    });
  }
  if (action === "bury") {
    if (e.state !== "OPEN" || args[2] !== i.user.id)
      throw Error("This test allocation is closed or belongs to another user.");
    const receipt = await store.allocate(
      i.guildId,
      { id: i.user.id, username: i.user.username,
        display_name: i.member?.displayName || i.user.globalName || i.user.username },
      g.id, Number(args[1]), args[3],
    );
    return reply(i, "🧪 BURIED " + receipt.quantity + " TEST tickets in " + g.name +
      ". Allocation #" + receipt.id + ". No live tickets were used.");
  }
  if (action === "decide") {
    const [ , generation, choice] = args;
    const d = await store.read(i.guildId);
    const state = d.graves.find(x => x.grave_id === g.id);
    if (i.channelId !== state.channel_id)
      throw Error("Use the decision panel in the correct TEST grave.");
    await store.decide(
      i.guildId,
      { id: i.user.id,
        display_name: i.member?.displayName || i.user.globalName || i.user.username },
      g.id, choice, generation,
    );
    await worker.tick();
    return reply(i, "🧪 " + choice + " sealed for " + g.name +
      ". The actual TEST reveal worker will advance the panel.");
  }
  throw Error("Unknown TEST interaction.");
}

async function handle(i) {
  const command = i.isChatInputCommand?.() && i.commandName === "fourgravestest";
  const parts = !command && (i.isButton?.() || i.isModalSubmit?.())
    ? testInteraction(i) : null;
  if (!command && !parts) return false;
  let e;
  if (parts) {
    // Return false for LIVE Four Graves events; those belong to the original handler.
    e = await event(i.guildId);
    if (!e || String(e.id) !== parts[1]) return false;
  }
  if (!enabled()) {
    await reply(i, "🧪 Four Graves private rehearsal is disabled.");
    return true;
  }
  try {
    if (command) {
      await i.deferReply({ flags: 64 });
      owner(i);
      const action = i.options.getSubcommand();
      e = await event(i.guildId);
      if (e) owner(i, e);
      if (action === "setup") return await reply(i, await setup(i));
      if (!e) throw Error("Run /fourgravestest setup first.");
      if (action === "seed") return await reply(i, await seed(i));
      if (action === "open") return await reply(i, await open(i));
      if (action === "close") return await reply(i, await close(i));
      if (action === "advance") return await reply(i, await advance(i));
      if (action === "status") return await reply(i, await status(i));
      if (action === "repair") {
        await worker.tick();
        return await reply(i, await status(i));
      }
      if (action === "export") return await exportTest(i);
      if (action === "reset") {
        if (i.options.getBoolean("confirm") !== true)
          throw Error("Reset cancelled. Nothing was deleted.");
        return await reply(i, await reset(i));
      }
      throw Error("Unknown TEST command.");
    }
    await handleTestComponent(i, e, parts);
    return true;
  } catch (err) {
    await reply(i, "🧪 TEST ERROR: " + String(err.message).slice(0, 1500));
  }
  return true;
}

module.exports = { init, start, enabled, buildSlashCommand, handle };
