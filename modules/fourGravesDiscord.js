"use strict";
const {
  PermissionFlagsBits: P,
  PermissionsBitField,
  ChannelType,
} = require("discord.js");
const { GRAVES, prizePoolForGrave } = require("./fourGravesCore");
const { choiceAnnouncement, winnerAnnouncement } = require("./fourGravesDraw");
const button = (custom_id, label, disabled = false, style = 2) => ({
  type: 2,
  custom_id,
  label,
  style,
  disabled,
});
const row = (...components) => ({ type: 1, components });
const cid = (e, action, ...args) => ["fg1", e.id, action, ...args].join(":");
const mentions = { parse: [] };
function publicPanel(e) {
  return {
    embeds: [
      {
        title: e.config.testMode ? "🧪 TEST ONLY — THE FOUR GRAVES" : "TRICK OR TREAT: THE FOUR GRAVES",
        description: e.config.testMode
          ? "🧪 **PRIVATE SIMULATION — NO REAL TICKETS OR PRIZES.**\n\n" +
            "Use the test controls to practice burying fake tickets. These four graves " +
            "are separate from the real October Trick or Treat event. " +
            "Test allocations can be removed with /fourgravestest reset.\n\nSTAY UGLY."
          : "The dead are waiting.\n\nEvery TREAT and TRICK you've earned is worth one ticket. Choose where to bury them. You may divide them between any of the four graves.\n\n**Once buried, tickets can never be moved or recovered. They remain valid contest entries.**\n\nAdministrators decide when graves open and close. Your choices determine your teams. The games and prizes remain a mystery.\n\nSTAY UGLY.",
        ...(e.config.image ? { image: { url: e.config.image } } : {}),
        footer: { text: `four-graves:${e.id}:public` },
      },
    ],
    components: [
      row(
        ...GRAVES.map((g, i) =>
          button(cid(e, "grave", i), e.config.names[i], e.state !== "OPEN"),
        ),
      ),
    ],
    allowedMentions: mentions,
  };
}
function decisionPanel(e, g) {
  const i = GRAVES.findIndex((x) => x.id === g.grave_id),
    decision = g.decision;
  const eligible = e.snapshot?.entries.some((x) => x.grave_id === g.grave_id);
  const active = e.state === "BUTTONS_ACTIVE" && !decision && eligible;
  let description = eligible
    ? `Your tickets are buried. Your team has been chosen.\n\nTRICK or TREAT. One person decides for everyone here. Discuss it. Argue about it. Or be the asshole who presses first.\n\nButtons eligible <t:${Math.floor(new Date(e.deadline).getTime() / 1000)}:R>.`
    : "This grave has no eligible contestants. The administrator will handle its unused prize pool manually.";
  if (decision)
    description = `${e.config.testMode ? "TEST OPERATOR" : `<@${decision.user_id}>`} HAS SEALED YOUR FATE.\nThey chose **${decision.choice}**.\n\nThere is no turning back...`;
  if (g.reveal_stage >= 2) description += "\n\nTHE GRAVE IS OPENING...";
  if (g.reveal_stage >= 3)
    description += "\n\n**" + e.config.names[i].toUpperCase() +
      " HAS AWAKENED.**\n\n**YOUR GRAVE PRIZE POOL**\n" +
      prizePoolForGrave(e.config,g.grave_id).map(p => p.quantity + " × " + p.name).join("\n") +
      "\n\nWinners are drawn automatically, starting 2 minutes after the choice, " +
      "one prize every 15 seconds. Each participant wins at most once per pass. " +
      "The pool resets after everyone has won once. Prize transfers are manual. Stay Ugly.";
  return {
    embeds: [
      {
        title: `${e.config.testMode ? "🧪 TEST — " : ""}${decision ? "THE FATE OF THIS GRAVE IS SEALED" : "YOUR FATE AWAITS"}`,
        description,
        footer: { text: `four-graves:${e.id}:${g.grave_id}` },
      },
    ],
    components: [
      row(
        button(cid(e, "decide", i, e.generation, "TRICK"), "👻 TRICK", !active),
        button(cid(e, "decide", i, e.generation, "TREAT"), "🍬 TREAT", !active),
      ),
    ],
    allowedMentions: mentions,
  };
}
async function setPermissions(channel, overwrites) {
  const expected = overwrites
    .map(
      (o) =>
        `${o.id}:${PermissionsBitField.resolve(o.allow || [])}:${PermissionsBitField.resolve(o.deny || [])}`,
    )
    .sort();
  const existing = channel.permissionOverwrites.cache
    ? [...channel.permissionOverwrites.cache.values()]
        .map((o) => `${o.id}:${o.allow.bitfield}:${o.deny.bitfield}`)
        .sort()
    : [];
  if (JSON.stringify(expected) !== JSON.stringify(existing))
    await channel.permissionOverwrites.set(overwrites);
}
class FourGravesDiscord {
  constructor(store, deps) {
    this.store = store;
    this.deps = deps;
    this.busy = false;
  }
  async permissions(guild, e, role) {
    await guild.roles.fetch();
    const allow = [P.ViewChannel, P.SendMessages, P.ReadMessageHistory];
    const overwrites = [
      { id: guild.id, deny: [P.ViewChannel] },
      { id: role.id, ...(e.closed_at ? { allow } : { deny: [P.ViewChannel] }) },
      {
        id: this.deps.client.user.id,
        allow: [
          ...allow,
          P.ManageChannels,
          P.ManageMessages,
          P.AttachFiles,
          P.EmbedLinks,
          ...(e.config.testMode ? [] : [P.MentionEveryone]),
        ],
      },
    ];
    const isTest = e.config.testMode === true;
    const adminIds = new Set(isTest ? [e.config.testOwnerId] : this.deps.adminIds());
    for (const r of (isTest ? [] : guild.roles.cache.values()))
      if (
        r.id !== guild.id &&
        r.id !== role.id &&
        r.permissions.has(P.ManageGuild)
      )
        overwrites.push({ id: r.id, allow });
    for (const id of adminIds) overwrites.push({ id, allow, type: 1 });
    return overwrites;
  }
  async managedResource(e, key, find, create, save) {
    const op = (
      await this.store.pool.query(
        "SELECT * FROM four_graves_operations WHERE event_id=$1 AND operation_key=$2",
        [e.id, key],
      )
    ).rows[0];
    const found = await find();
    if (found) {
      if (!op)
        throw Error(
          `Unmanaged resource matches ${key}; administrator review required.`,
        );
      await save(found);
      await this.store.operation(e.id, key);
      return found;
    }
    // A previously started create with unknown result is reconciled by its dedicated marker before another create.
    if (op?.last_error === "CREATE_IN_FLIGHT")
      throw Error(
        `Uncertain ${key}; inspect Discord and retry reconciliation after confirming its absence.`,
      );
    await this.store.operation(e.id, key, "CREATE_IN_FLIGHT");
    let resource;
    try {
      resource = await create();
    } catch (err) {
      if (Number(err.status) >= 400 && Number(err.status) < 500)
        await this.store.operation(e.id, key, "CREATE_REJECTED");
      throw err;
    }
    await save(resource);
    await this.store.operation(e.id, key);
    return resource;
  }
  async resources(guild, e, g) {
    const i = GRAVES.findIndex((x) => x.id === g.grave_id);
    const isTest = e.config.testMode === true;
    const roleName = `${isTest ? "[TEST] " : ""}Grave — ${e.config.names[i]} [FG${e.id}]`;
    await guild.roles.fetch();
    let role = g.role_id ? guild.roles.cache.get(g.role_id) : null;
    if (!role)
      role = await this.managedResource(
        e,
        `role:${g.grave_id}`,
        async () => {
          const matches = [...guild.roles.cache.values()].filter(
            (x) => x.name === roleName,
          );
          if (matches.length > 1)
            throw Error(
              "Multiple matching grave roles: administrator review required.",
            );
          return matches[0];
        },
        () =>
          guild.roles.create({
            name: roleName,
            permissions: [],
            mentionable: false,
            reason: `Four Graves ${e.id}`,
          }),
        (r) =>
          this.store.pool.query(
            "UPDATE four_graves_graves SET role_id=$3 WHERE event_id=$1 AND grave_id=$2",
            [e.id, g.grave_id, r.id],
          ),
      );
    if (role.editable === false)
      throw Error("Move the bot role above its grave roles.");
    if (role.permissions.bitfield !== 0n)
      throw Error(
        "Managed grave role has unexpected server permissions; remove those permissions before reconciliation.",
      );
    if (role.name !== roleName && role.setName) await role.setName(roleName);
    g.role_id = role.id;
    await guild.channels.fetch();
    let channel = g.channel_id ? guild.channels.cache.get(g.channel_id) : null;
    const marker = `four-graves:${e.id}:${g.grave_id}`;
    const overwrites = await this.permissions(guild, e, role);
    if (!channel)
      channel = await this.managedResource(
        e,
        `channel:${g.grave_id}`,
        async () => {
          const matches = [...guild.channels.cache.values()].filter(
            (x) => x.topic === marker,
          );
          if (matches.length > 1)
            throw Error(
              "Multiple matching grave channels: administrator review required.",
            );
          return matches[0];
        },
        () =>
          guild.channels.create({
            name: (isTest ? "test-" : "") + "grave-" + g.grave_id.replaceAll("_", "-"),
            type: ChannelType.GuildText,
            topic: marker,
            parent: e.config.category || undefined,
            permissionOverwrites: overwrites,
            reason: `Four Graves ${e.id}`,
          }),
        (r) =>
          this.store.pool.query(
            "UPDATE four_graves_graves SET channel_id=$3 WHERE event_id=$1 AND grave_id=$2",
            [e.id, g.grave_id, r.id],
          ),
      );
    if (channel.topic !== marker || channel.type !== ChannelType.GuildText)
      throw Error("Stored channel is not a dedicated Four Graves channel.");
    if (e.config.category && channel.parentId !== e.config.category) {
      await channel.setParent(e.config.category, { lockPermissions: false });
    }
    // Restrict team access until membership reconciliation has completed.
    g.channel_id = channel.id;
    return { role, channel, overwrites };
  }
  async message(e, channel, messageId, key, payload, save) {
    let message;
    if (messageId) {
      try {
        message = await channel.messages.fetch(messageId);
      } catch (err) {
        if (Number(err.code) !== 10008) throw err;
      }
    }
    if (message) {
      if (message.author.id !== this.deps.client.user.id)
        throw Error("Panel author mismatch.");
      const embed = message.embeds[0];
      const same =
        embed?.title === payload.embeds[0].title &&
        embed?.description === payload.embeds[0].description &&
        embed?.image?.url === payload.embeds[0].image?.url &&
        JSON.stringify(message.components.map((x) => x.toJSON())) ===
          JSON.stringify(payload.components);
      if (!same) await message.edit({ ...payload, allowedMentions: mentions });
      return message;
    }
    const marker = payload.embeds[0].footer.text;
    return this.managedResource(
      e,
      key,
      async () => {
        let before;
        while (true) {
          const batch = await channel.messages.fetch({
            limit: 100,
            ...(before ? { before } : {}),
          });
          const found = [...batch.values()].find(
            (m) =>
              m.author.id === this.deps.client.user.id &&
              m.embeds.some((x) => x.footer?.text === marker),
          );
          if (found) {
            await found.edit({ ...payload, allowedMentions: mentions });
            return found;
          }
          if (batch.size < 100) break;
          before = batch.last().id;
        }
        return null;
      },
      () => channel.send(payload),
      save,
    );
  }
  async reconcile(e) {
    if (this.store.testMode !== (e.config?.testMode === true))
      throw Error("Four Graves event-mode mismatch; refusing Discord changes.");
    const guild = await this.deps.client.guilds.fetch(e.guild_id);
    const me = await guild.members.fetchMe();
    if (!me.permissions.has([P.ManageRoles, P.ManageChannels]))
      throw Error("Bot needs Manage Roles and Manage Channels.");
    const graves = (
      await this.store.pool.query(
        "SELECT * FROM four_graves_graves WHERE event_id=$1 ORDER BY grave_id",
        [e.id],
      )
    ).rows;
    // Fetch complete membership so a wrongly assigned dedicated team role cannot grant unintended access.
    const members = await guild.members.fetch();
    for (const g of graves) {
      try {
        const { role, channel, overwrites } = await this.resources(guild, e, g);
        const eligible = new Set(
          (e.snapshot?.entries || [])
            .filter((x) => x.grave_id === g.grave_id)
            .map((x) => x.discord_id),
        );
        const changes = [...members.values()].some(
          (member) =>
            (!!e.closed_at && eligible.has(member.id)) !==
            member.roles.cache.has(role.id),
        );
        if (changes)
          await setPermissions(
            channel,
            overwrites.map((x) =>
              x.id === role.id ? { id: role.id, deny: [P.ViewChannel] } : x,
            ),
          );
        for (const member of members.values()) {
          const wanted = !!e.closed_at && eligible.has(member.id),
            has = member.roles.cache.has(role.id);
          if (wanted && !has) await member.roles.add(role);
          if (!wanted && has) await member.roles.remove(role);
        }
        await setPermissions(channel, overwrites);
        if (e.closed_at) {
          // Advance at most one durable stage per pass. The single stored message is edited, not repeatedly announced.
          const previousStage = g.reveal_stage;
          const due =
            g.decision &&
            g.reveal_stage < 3 &&
            (!g.next_reveal_at || new Date(g.next_reveal_at) <= new Date());
          if (due) g.reveal_stage++;
          await this.message(
            e,
            channel,
            g.message_id,
            `panel:${g.grave_id}`,
            decisionPanel(e, g),
            (m) =>
              this.store.pool.query(
                "UPDATE four_graves_graves SET message_id=$3 WHERE event_id=$1 AND grave_id=$2",
                [e.id, g.grave_id, m.id],
              ),
          );
          if (due)
            await this.store.pool.query(
              "UPDATE four_graves_graves SET reveal_stage=$3,next_reveal_at=clock_timestamp()+($4::int*interval '1 second'),revealed_at=CASE WHEN $3=3 THEN clock_timestamp() ELSE revealed_at END WHERE event_id=$1 AND grave_id=$2 AND reveal_stage=$5",
              [
                e.id,
                g.grave_id,
                g.reveal_stage,
                e.config.revealDelaySeconds,
                previousStage,
              ],
            );
        }
        await this.store.operation(e.id, `reconcile:${g.grave_id}`);
      } catch (err) {
        const key = `reconcile:${g.grave_id}`,
          error = String(err.message).slice(0, 500);
        const previous = (
          await this.store.pool.query(
            "SELECT last_error FROM four_graves_operations WHERE event_id=$1 AND operation_key=$2",
            [e.id, key],
          )
        ).rows[0];
        await this.store.operation(e.id, key, error);
        if (!e.config.testMode && previous?.last_error !== error && this.deps.postAdminSystemLog)
          await this.deps
            .postAdminSystemLog({
              guildId: e.guild_id,
              category: "Four Graves",
              message: `Event ${e.id}: ${g.grave_id} requires recovery. Run /fourgraves admin → STATUS. Tickets remain intact.`,
            })
            .catch(() => {});
      }
    }
    if (e.panel_channel_id) {
      const channel = await guild.channels.fetch(e.panel_channel_id);
      if (
        !channel ||
        channel.guildId !== e.guild_id ||
        channel.type !== ChannelType.GuildText
      )
        throw Error("Invalid public panel channel.");
      await this.message(
        e,
        channel,
        e.panel_message_id,
        "public-panel",
        publicPanel(e),
        (m) =>
          this.store.pool.query(
            "UPDATE four_graves_events SET panel_message_id=$2 WHERE id=$1",
            [e.id, m.id],
          ),
      );
    }
    // Empty graves never reveal themselves and are excluded from event-completion readiness.
    if (e.closed_at && graves.some((g) => g.decision))
      await this.store.pool.query(
        "UPDATE four_graves_events SET state='REVEALED' WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM four_graves_graves g WHERE g.event_id=$1 AND g.reveal_stage<3 AND EXISTS(SELECT 1 FROM four_graves_allocations a WHERE a.event_id=$1 AND a.grave_id=g.grave_id))",
        [e.id],
      );
  }
  async tick() {
    if (this.busy || !this.deps.enabled()) return;
    this.busy = true;
    try {
      const events = (
        await this.store.pool.query(
          "SELECT id FROM four_graves_events WHERE event_key=$1",
          [this.store.eventKey],
        )
      ).rows;
      for (const event of events) {
        const c = await this.store.pool.connect();
        let locked = false;
        try {
          locked = (
            await c.query(
              "SELECT pg_try_advisory_lock(46291,$1::int) AS locked",
              [event.id],
            )
          ).rows[0].locked;
          if (!locked) continue;
          // Row lock makes generation/deadline changes atomic with countdown completion.
          await c.query(
            "UPDATE four_graves_events SET state='BUTTONS_ACTIVE' WHERE id=$1 AND state='CLOSED_COUNTDOWN' AND deadline<=clock_timestamp()",
            [event.id],
          );
          const e = (
            await c.query("SELECT * FROM four_graves_events WHERE id=$1", [
              event.id,
            ])
          ).rows[0];
          if (this.store.testMode && (!e.config?.category || !e.panel_channel_id)) continue;
          await this.reconcile(e);
          await this.store.operation(e.id, "worker");
        } catch (err) {
          await this.store.operation(
            event.id,
            "worker",
            String(err.message).slice(0, 500),
          );
          console.warn(
            "[FourGraves] worker failed for event",
            event.id,
            "; see private admin status.",
          );
        } finally {
          if (locked)
            await c.query("SELECT pg_advisory_unlock(46291,$1::int)", [
              event.id,
            ]);
          c.release();
        }
      }
    } finally {
      this.busy = false;
    }
  }
  start() {
    if (this.timer) return;
    this.timer = setInterval(
      () =>
        this.tick().catch(() =>
          console.warn("[FourGraves] recovery deferred; inspect admin status."),
        ),
      15000,
    );
    this.timer.unref?.();
    void this.tick().catch(() =>
      console.warn("[FourGraves] recovery startup deferred."),
    );
  }
}
module.exports = {
  FourGravesDiscord,
  publicPanel,
  decisionPanel,
  button,
  row,
  cid,
};
