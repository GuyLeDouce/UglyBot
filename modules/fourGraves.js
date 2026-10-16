"use strict";
const { SlashCommandBuilder, ChannelType } = require("discord.js");
const { FourGravesStore } = require("./fourGravesStore");
const { FourGravesDiscord, button, row, cid } = require("./fourGravesDiscord");
const {
  GRAVES,
  quantity,
  exportFiles,
  privateArchives,
} = require("./fourGravesCore");
let deps, store, worker, ready;
const enabled = () =>
  String(process.env.FOUR_GRAVES_ENABLED || "false").toLowerCase() === "true";
function initFourGraves(d) {
  deps = d;
  store = new FourGravesStore(d.pool, {
    eventKey: process.env.FOUR_GRAVES_EVENT_KEY || "squigs-four-graves-2026",
    sourceEventKey:
      process.env.TRICK_OR_TREAT_EVENT_KEY || "squigs-trick-or-treat-2026",
  });
  worker = new FourGravesDiscord(store, { ...d, enabled });
}
async function start() {
  if (!ready)
    ready = store.migrate().catch((e) => {
      ready = null;
      throw e;
    });
  await ready;
  if (enabled()) worker.start();
}
function buildSlashCommand() {
  return new SlashCommandBuilder()
    .setName("fourgraves")
    .setDescription("Trick or Treat: The Four Graves")
    .addSubcommand((s) =>
      s.setName("admin").setDescription("Administrator management panel"),
    )
    .addSubcommand((s) =>
      s.setName("status").setDescription("View grave event status"),
    )
    .addSubcommand((s) =>
      s
        .setName("configure")
        .setDescription(
          "Admin: configure destinations, image and prize finalization",
        )
        .addChannelOption((o) =>
          o
            .setName("channel")
            .setDescription("Public panel destination")
            .addChannelTypes(ChannelType.GuildText),
        )
        .addChannelOption((o) =>
          o
            .setName("category")
            .setDescription("Private team category")
            .addChannelTypes(ChannelType.GuildCategory),
        )
        .addAttachmentOption((o) =>
          o.setName("image").setDescription("Grave artwork attachment"),
        )
        .addStringOption((o) =>
          o.setName("image-url").setDescription("HTTPS artwork URL"),
        )
        .addStringOption((o) =>
          o
            .setName("config-json")
            .setDescription(
              "Names, rules, mystery prizes, reveal delay; see administrator guide",
            ),
        )
        .addBooleanOption((o) =>
          o
            .setName("finalize-prizes")
            .setDescription(
              "Confirm the full identical prize pool, including mystery prizes",
            ),
        ),
    )
    .addSubcommand((s) =>
      s
        .setName("export")
        .setDescription("Admin: private contestant exports")
        .addStringOption((o) =>
          o
            .setName("grave")
            .setDescription("Grave")
            .addChoices(
              { name: "All", value: "all" },
              ...GRAVES.map((g) => ({ name: g.name, value: g.id })),
            ),
        )
        .addStringOption((o) =>
          o
            .setName("format")
            .setDescription("Export format")
            .addChoices(
              ...["all", "contestants", "master", "audit"].map((v) => ({
                name: v,
                value: v,
              })),
            ),
        ),
    );
}
const reply = (i, p) =>
  i.deferred || i.replied
    ? i.editReply(typeof p === "string" ? { content: p } : p)
    : i.reply(
        typeof p === "string" ? { content: p, flags: 64 } : { ...p, flags: 64 },
      );
function admin(i) {
  if (!deps.isAdmin(i)) throw Error("Administrator access required.");
}
function controls(e) {
  const actions = [
    ["setup", "SETUP EVENT"],
    ["post", "POST GRAVE PANEL"],
    ["open", "OPEN GRAVES"],
    ["close", "CLOSE GRAVES"],
    ["status", "STATUS"],
    ["export", "EXPORT ENTRIES"],
    ["restart", "RESTART COUNTDOWN"],
    ["repair", "RECONCILE ROLES"],
    ["refresh", "REFRESH PANELS"],
    ["retry-resources", "RETRY UNCERTAIN CREATES"],
  ];
  return {
    content:
      "FOUR GRAVES — ADMIN\nUse /fourgraves configure for image, destinations, prize configuration and finalization.",
    components: [
      row(
        ...actions.slice(0, 5).map(([a, l]) => button(cid(e, "admin", a), l)),
      ),
      row(...actions.slice(5).map(([a, l]) => button(cid(e, "admin", a), l))),
    ],
  };
}
function status(data, isAdmin) {
  const { event: e, snapshot: s, graves, operations } = data;
  const total = s.users.reduce((n, u) => n + u.total, 0);
  let text = `**THE FOUR GRAVES — ${e.state}**\nParticipants: ${s.users.filter((u) => u.allocated).length}\nTickets buried: ${s.entries.length}\n`;
  if (e.deadline)
    text += `Decision deadline: <t:${Math.floor(new Date(e.deadline) / 1000)}:F>\n`;
  for (const [i, g] of GRAVES.entries()) {
    const state = graves.find((x) => x.grave_id === g.id),
      entries = s.entries.filter((x) => x.grave_id === g.id);
    text += `${e.config.names[i]}: ${entries.length} tickets, ${new Set(entries.map((x) => x.discord_id)).size} members — ${state.reveal_stage === 3 ? "REVEALED" : !entries.length ? "NO CONTESTANTS" : "unrevealed"}\n`;
    if (isAdmin)
      text += `Role ${state.role_id || "missing"}; channel ${state.channel_id || "missing"}; reveal stage ${state.reveal_stage}${state.decision ? `; ${state.decision.choice} by ${state.decision.user_id}` : ""}\n`;
  }
  if (isAdmin)
    text += `\nTreats: ${s.users.reduce((n, u) => n + u.treats, 0)}; Tricks: ${s.users.reduce((n, u) => n + u.tricks, 0)}\nEarned: ${total}; unallocated: ${total - s.entries.length}\nOpened: ${e.opened_at || "never"}\nClosed: ${e.closed_at || "never"}\nCountdown generation: ${e.generation}\nSnapshot: ${e.snapshot_id || "live"}\nLast export: ${e.last_export_at || "never"}\nPrize pool finalized: ${!!e.config.prizesFinalized}\nPending operations:\n${
      operations
        .filter((o) => o.last_error)
        .map((o) => `${o.operation_key}: ${o.last_error}`)
        .join("\n") || "none"
    }`;
  return { content: text.slice(0, 1950), allowedMentions: { parse: [] } };
}
function validatePatch(input) {
  const patch = {};
  for (const k of Object.keys(input))
    if (!["names", "rules", "mystery", "revealDelaySeconds"].includes(k))
      throw Error(`Unsupported config key: ${k}`);
  for (const k of ["names", "rules"])
    if (input[k] !== undefined) {
      if (
        !Array.isArray(input[k]) ||
        input[k].length !== 4 ||
        input[k].some(
          (v) =>
            typeof v !== "string" ||
            !v.trim() ||
            v.length > (k === "names" ? 40 : 900),
        )
      )
        throw Error(`Provide four ${k} strings within documented limits.`);
      patch[k] = input[k];
    }
  if (input.mystery !== undefined) {
    if (
      !Array.isArray(input.mystery) ||
      input.mystery.length > 10 ||
      input.mystery.some(
        (p) =>
          typeof p.name !== "string" ||
          !p.name.trim() ||
          p.name.length > 80 ||
          !Number.isInteger(p.quantity) ||
          p.quantity < 1 ||
          p.quantity > 100,
      )
    )
      throw Error(
        "Mystery prizes need names and positive integer quantities; [] means none.",
      );
    patch.mystery = input.mystery;
    patch.prizesFinalized = false;
  }
  if (input.revealDelaySeconds !== undefined) {
    if (
      !Number.isInteger(input.revealDelaySeconds) ||
      input.revealDelaySeconds < 1 ||
      input.revealDelaySeconds > 30
    )
      throw Error("Reveal delay must be 1–30 seconds.");
    patch.revealDelaySeconds = input.revealDelaySeconds;
  }
  return patch;
}
async function exportsReply(i, grave = "all", format = "all") {
  admin(i);
  const { event, snapshot } = await store.read(i.guildId);
  const files = exportFiles(event, snapshot, grave, format),
    parts = privateArchives(files, i.attachmentSizeLimit || 8 * 1024 * 1024);
  await reply(i, {
    content: `Private export: ${snapshot.entries.length} allocated tickets (${event.snapshot_id || "live snapshot"}). ${parts.length} ZIP part(s).`,
    files: [parts[0]],
    components: [],
  });
  for (const part of parts.slice(1))
    await i.followUp({
      files: [part],
      flags: 64,
      allowedMentions: { parse: [] },
    });
  await store.pool.query(
    "UPDATE four_graves_events SET last_export_at=clock_timestamp() WHERE id=$1",
    [event.id],
  );
  await store.audit(store.pool, event.id, i.user.id, "export", {
    grave,
    format,
  });
}
async function identities(i) {
  const members = await i.guild.members.fetch();
  return Object.fromEntries(
    [...members.values()].map((m) => [
      m.id,
      { username: m.user.username, display_name: m.displayName },
    ]),
  );
}
async function adminAction(i, e, action, confirmed = false) {
  admin(i);
  if (action === "retry-resources") {
    if (!confirmed)
      return reply(i, {
        content:
          "Only proceed after inspecting Discord and confirming every CREATE_IN_FLIGHT resource is absent or retains its original Four Graves name/topic/footer. Renamed resources need administrator review first. This authorizes retrying uncertain creation requests; no ticket data changes.",
        components: [
          row(
            button(
              cid(e, "confirm-admin", action, e.generation),
              "CONFIRM RECOVERY",
              false,
              4,
            ),
            button(cid(e, "cancel"), "CANCEL"),
          ),
        ],
      });
    const c = await store.pool.connect();
    try {
      const locked = (
        await c.query("SELECT pg_try_advisory_lock(46291,$1::int) AS locked", [
          e.id,
        ])
      ).rows[0].locked;
      if (!locked)
        throw Error("Recovery is already running. Try again shortly.");
      try {
        await c.query(
          "UPDATE four_graves_operations SET last_error='ADMIN_CONFIRMED_RETRY' WHERE event_id=$1 AND last_error='CREATE_IN_FLIGHT'",
          [e.id],
        );
        await store.audit(c, e.id, i.user.id, "retry-uncertain-resources");
      } finally {
        await c.query("SELECT pg_advisory_unlock(46291,$1::int)", [e.id]);
      }
    } finally {
      c.release();
    }
    await reply(
      i,
      "Recovery authorized. Existing marked resources will be reused.",
    );
    void worker
      .tick()
      .catch(() =>
        console.warn("[FourGraves] recovery deferred; see admin status."),
      );
    return;
  }
  if (["open", "close", "restart"].includes(action) && !confirmed)
    return reply(i, {
      content:
        action === "close"
          ? "**FINAL WARNING — CLOSE ALL FOUR GRAVES?**\nPermanently stop allocation, freeze all entries, assign team roles, unlock private channels and start the 15-minute countdown. Existing allocations cannot be changed."
          : `Confirm ${action.toUpperCase()}? ${action === "restart" ? "This invalidates old decision buttons and starts a fresh 15 minutes. Allocations remain closed." : "Participants may permanently bury tickets."}`,
      components: [
        row(
          button(
            cid(e, "confirm-admin", action, e.generation),
            `CONFIRM ${action.toUpperCase()}`,
            false,
            4,
          ),
          button(cid(e, "cancel"), "CANCEL"),
        ),
      ],
    });
  if (["open", "close", "restart"].includes(action)) {
    await store.transition(
      i.guildId,
      i.user.id,
      action,
      action === "close" ? await identities(i) : {},
    );
    await reply(i, {
      content: `${action.toUpperCase()} recorded. Discord reconciliation will follow.`,
      components: [],
    });
    void worker
      .tick()
      .catch(() =>
        console.warn("[FourGraves] recovery deferred; see admin status."),
      );
    return;
  }
  if (action === "export") return exportsReply(i);
  if (action === "status")
    return reply(i, status(await store.read(i.guildId), true));
  if (action === "setup") await store.setup(i.guildId, i.user.id);
  if (action === "post") {
    await store.transaction(async (c) => {
      const ev = await store.event(c, i.guildId, true);
      await c.query(
        "UPDATE four_graves_events SET panel_channel_id=$2 WHERE id=$1",
        [ev.id, ev.panel_channel_id || i.channelId],
      );
    });
  }
  await worker.tick();
  await reply(i, {
    ...status(await store.read(i.guildId), true),
    components: controls(e).components,
  });
}
async function handle(i) {
  const isCommand = i.isChatInputCommand?.() && i.commandName === "fourgraves";
  if (!isCommand && !String(i.customId || "").startsWith("fg1:")) return false;
  try {
    if (!enabled())
      throw Error(
        "The Four Graves is disabled. Existing ticket claims are unchanged.",
      );
    if (!i.guildId) throw Error("Use this in the server.");
    if (isCommand) {
      await i.deferReply({ flags: 64 });
      const sub = i.options.getSubcommand();
      if (sub === "admin") {
        admin(i);
        const e = await store.setup(i.guildId, i.user.id);
        await reply(i, controls(e));
      } else if (sub === "status")
        await reply(i, status(await store.read(i.guildId), deps.isAdmin(i)));
      else if (sub === "export")
        await exportsReply(
          i,
          i.options.getString("grave") || "all",
          i.options.getString("format") || "all",
        );
      else if (sub === "configure") {
        admin(i);
        const patch = validatePatch(
          JSON.parse(i.options.getString("config-json") || "{}"),
        );
        const cat = i.options.getChannel("category"),
          channel = i.options.getChannel("channel"),
          attachment = i.options.getAttachment("image");
        const image = attachment?.url || i.options.getString("image-url");
        if (image) {
          const url = new URL(image);
          if (url.protocol !== "https:" || url.username || url.password)
            throw Error("Use an HTTPS image URL.");
          if (
            attachment &&
            !/^image\/(png|jpeg|webp|gif)$/.test(attachment.contentType || "")
          )
            throw Error("Use a PNG, JPG, WebP or GIF.");
          patch.image = image;
        }
        if (cat) patch.category = cat.id;
        if (i.options.getBoolean("finalize-prizes") !== null)
          patch.prizesFinalized = i.options.getBoolean("finalize-prizes");
        await store.configure(i.guildId, i.user.id, patch);
        if (channel)
          await store.transaction(async (c) => {
            const e = await store.event(c, i.guildId, true);
            if (e.panel_message_id && e.panel_channel_id !== channel.id)
              throw Error(
                "An existing panel cannot be moved; retain its destination.",
              );
            await c.query(
              "UPDATE four_graves_events SET panel_channel_id=$2 WHERE id=$1",
              [e.id, channel.id],
            );
          });
        await reply(
          i,
          "Configuration saved. Use POST GRAVE PANEL / REFRESH PANELS.",
        );
      }
      return true;
    }
    const [, eventId, action, ...args] = i.customId.split(":");
    // Modal presentation does not perform I/O; its submit handler validates every field again.
    if (action === "quantity") {
      const [gi, owner] = args;
      if (owner !== i.user.id)
        throw Error("This private interaction belongs to another user.");
      await i.showModal({
        custom_id: cid({ id: eventId }, "amount", gi),
        title: "Bury your tickets",
        components: [
          row({
            type: 4,
            custom_id: "quantity",
            label: "Positive whole-number ticket quantity",
            style: 1,
            required: true,
            max_length: 10,
          }),
        ],
      });
      return true;
    }
    await i.deferReply({ flags: 64 });
    const data = await store.read(i.guildId),
      e = data.event;
    if (String(e.id) !== eventId)
      throw Error("This panel belongs to another event.");
    const [gi] = args,
      g = GRAVES[Number(gi)],
      user = {
        id: i.user.id,
        username: i.user.username,
        display_name:
          i.member.displayName || i.user.globalName || i.user.username,
      };
    if (action === "cancel") {
      await reply(i, "Cancelled. No tickets were buried.");
      return true;
    }
    if (action === "admin" || action === "confirm-admin") {
      if (action === "confirm-admin" && Number(args[1]) !== e.generation)
        throw Error(
          "Stale administrative confirmation. Open a fresh admin panel.",
        );
      await adminAction(i, e, args[0], action === "confirm-admin");
      return true;
    }
    if (!g) throw Error("Unknown grave.");
    const balance = data.snapshot.users.find((u) => u.discord_id === i.user.id);
    const available = balance ? balance.total - balance.allocated : 0;
    if (action === "grave") {
      if (e.state !== "OPEN") throw Error("THE GRAVES ARE SEALED.");
      await reply(i, {
        content: `**${e.config.names[Number(gi)]}**\nEarned: ${balance?.total || 0}\nAlready buried: ${balance?.allocated || 0}\nAvailable: ${available}\nIn this grave: ${balance?.graves[Number(gi)] || 0}`,
        components: [
          row(button(cid(e, "quantity", gi, i.user.id), "ENTER QUANTITY")),
        ],
      });
    } else if (action === "amount") {
      if (e.state !== "OPEN") throw Error("THE GRAVES ARE SEALED.");
      const amount = quantity(i.fields.getTextInputValue("quantity"));
      if (amount > available)
        throw Error(`NOT ENOUGH TICKETS. Only ${available} available.`);
      await reply(i, {
        content: `**BURY YOUR TICKETS?**\nYou're about to bury **${amount}** tickets in **${e.config.names[Number(gi)]}**.\nCurrent available balance: ${available}.\n\nOnce confirmed, they cannot be withdrawn, transferred or reassigned. An administrator cannot casually reverse this allocation.\n\n**THIS DECISION IS PERMANENT.**`,
        components: [
          row(
            button(
              cid(e, "bury", gi, amount, i.user.id, i.id),
              "BURY MY TICKETS",
              false,
              4,
            ),
            button(cid(e, "cancel"), "CANCEL"),
          ),
        ],
      });
    } else if (action === "bury") {
      const [, amount, owner, key] = args;
      if (owner !== i.user.id)
        throw Error("This confirmation belongs to another user.");
      const receipt = await store.allocate(
        i.guildId,
        user,
        g.id,
        quantity(amount),
        key,
      );
      const after = await store.read(i.guildId);
      const u = after.snapshot.users.find((u) => u.discord_id === i.user.id);
      await reply(
        i,
        `YOUR TICKETS HAVE BEEN BURIED.\nAllocation #${receipt.id}: ${receipt.quantity} tickets with ${e.config.names[Number(gi)]}.\nRemaining tickets: ${u.total - u.allocated}.`,
      );
    } else if (action === "decide") {
      const [, generation, choice] = args;
      const state = data.graves.find((x) => x.grave_id === g.id);
      if (i.channelId !== state.channel_id)
        throw Error("Use the decision panel in your grave channel.");
      await store.decide(i.guildId, user, g.id, choice, generation);
      await reply(i, "Your choice is sealed. The grave is opening...");
      void worker
        .tick()
        .catch(() =>
          console.warn("[FourGraves] recovery deferred; see admin status."),
        );
    } else throw Error("Unknown Four Graves interaction.");
  } catch (err) {
    await reply(i, {
      content: String(err.message).slice(0, 1800),
      components: [],
      allowedMentions: { parse: [] },
    });
  }
  return true;
}
module.exports = {
  initFourGraves,
  start,
  buildSlashCommand,
  handle,
  enabled,
  validatePatch,
  status,
};
