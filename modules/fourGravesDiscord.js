"use strict";
const {
  PermissionFlagsBits: P,
  PermissionsBitField,
  ChannelType,
  Collection,
} = require("discord.js");
const { GRAVES, prizePoolForGrave, PANEL_IMAGE_URL } = require("./fourGravesCore");
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
// Presentation order only: keep GRAVES indices stable for tickets, roles and exports.
const PANEL_ORDER = Object.freeze([
  "cletus_von_wrinkles",
  "barry_buckets",
  "barbara_bowkins",
  "scuba_steve_3000",
]);
const PANEL_INDICES = Object.freeze(PANEL_ORDER.map(id => {
  const index = GRAVES.findIndex(grave => grave.id === id);
  if (index < 0) throw Error("Four Graves panel references an unknown grave: " + id);
  return index;
}));

// Surface the real failure without logging credentials or flooding Railway every 15s.
function diagnosticError(err) {
  const code = err?.code ?? err?.rawError?.code ?? err?.status ?? "";
  const msg = String(err?.message || err || "Unknown worker error")
    .replace(/(postgres(?:ql)?:\/\/)[^\s]+/gi, "$1[redacted]")
    .replace(/(password|token|authorization)\s*[:=]\s*[^\s,;]+/gi, "$1=[redacted]")
    .replace(/\s+/g, " ").slice(0, 550);
  return (code ? "[" + String(code).slice(0, 32) + "] " : "") + msg;
}

const MEMBER_ROSTER_TTL_MS = 15 * 60 * 1000;
const MEMBER_ROSTER_BACKOFF_MS = 2 * 60 * 1000;
// Keyed by the actual Discord guild instance, so LIVE and TEST share the
// exact same fetch and cache within this bot process.
const memberRosterStates = new WeakMap();

function memberRosterState(guild) {
  let state = memberRosterStates.get(guild);
  if (!state) {
    state = { members: null, refreshedAt: 0, retryAt: 0, pending: null };
    memberRosterStates.set(guild, state);
  }
  return state;
}

function noteGuildMemberChange(member, joined) {
  if (!member?.guild || !member.id) return;
  const state = memberRosterStates.get(member.guild);
  if (!state?.members) return;
  if (joined) state.members.set(member.id, member);
  else state.members.delete(member.id);
}

// Discord's guild.members.fetch() without a user ID emits gateway opcode 8.
// NEVER use it in periodic workers. GuildMemberManager.list uses the paginated
// REST List Guild Members route, and is shared/cached across LIVE + TEST.
async function listGuildMembersRest(guild) {
  if (typeof guild.members.list !== "function")
    throw Error("discord.js GuildMemberManager.list() is required for safe member reconciliation.");
  const members = new Collection();
  let after = undefined;
  for (let page = 0; page < 500; page++) {
    const batch = await guild.members.list({ limit: 1000, ...(after ? { after } : {}) });
    if (!batch || typeof batch.values !== "function" || typeof batch.size !== "number")
      throw Error("REST member listing returned an invalid page.");
    const pageMembers = [...batch.values()];
    for (const member of pageMembers) members.set(member.id, member);
    if (batch.size < 1000) return members;
    const last = typeof batch.lastKey === "function"
      ? batch.lastKey() : pageMembers.at(-1)?.id;
    if (!last || last === after)
      throw Error("REST member listing pagination stalled.");
    after = last;
  }
  throw Error("Member listing exceeded the maximum safe number of pages.");
}

async function getGuildMemberRoster(guild) {
  const state = memberRosterState(guild);
  const now = Date.now();
  if (state.members && now - state.refreshedAt < MEMBER_ROSTER_TTL_MS)
    return state.members;
  if (state.pending) return state.pending;
  if (now < state.retryAt) {
    if (state.members) return state.members;
    const err = Error("Member roster refresh cooling down after a failed REST request.");
    err.code = "MEMBER_ROSTER_BACKOFF";
    throw err;
  }
  const pending = listGuildMembersRest(guild).then(members => {
    state.members = members;
    state.refreshedAt = Date.now();
    state.retryAt = 0;
    return members;
  }).catch(err => {
    const msg = String(err?.message || "");
    const retry = Number(/retry after ([\d.]+) seconds?/i.exec(msg)?.[1] || 0);
    state.retryAt = Date.now() + Math.max(MEMBER_ROSTER_BACKOFF_MS, retry * 1000 + 5000);
    if (state.members) return state.members;
    throw err;
  }).finally(() => { state.pending = null; });
  state.pending = pending;
  return pending;
}

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
        // Refresh older persisted panels even when their config.image was null.
        // An explicitly configured custom image still takes precedence.
        image: { url: e.config.image || PANEL_IMAGE_URL },
        footer: { text: `four-graves:${e.id}:public` },
      },
    ],
    components: [
      row(
        ...PANEL_INDICES.map(i =>
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
    this.lastLoggedFailures = new Map();
  }
  logFailure(scope, eventId, err) {
    const detail = diagnosticError(err);
    const fingerprint = detail.replace(/Retry after [\d.]+ seconds?/gi, "Retry after [variable] seconds");
    const key = scope + ":" + eventId;
    const previous = this.lastLoggedFailures.get(key);
    const now = Date.now();
    // Distinct failures appear immediately; identical messages repeat at most every 5 minutes.
    if (!previous || previous.fingerprint !== fingerprint || now - previous.time >= 300000) {
      console.warn(
        "[FourGraves:" + (this.store.testMode ? "TEST" : "LIVE") + "] " +
        scope + " failed for event " + eventId + ": " + detail,
      );
      this.lastLoggedFailures.set(key, { detail, fingerprint, time: now });
    }
  }
  async permissions(guild, e, role) {
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
    let role = g.role_id ? guild.roles.cache.get(g.role_id) : null;
    if (!role && g.role_id) {
      try { role = await guild.roles.fetch(g.role_id); }
      catch (err) { if (Number(err.code) !== 10011 && Number(err.status) !== 404) throw err; }
    }
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
    let channel = g.channel_id ? guild.channels.cache.get(g.channel_id) : null;
    if (!channel && g.channel_id) {
      try { channel = await guild.channels.fetch(g.channel_id); }
      catch (err) { if (Number(err.code) !== 10003 && Number(err.status) !== 404) throw err; }
    }
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
        message = channel.messages.cache?.get(messageId) || await channel.messages.fetch(messageId);
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
    const me = guild.members.me || await guild.members.fetchMe();
    if (!me.permissions.has([P.ManageRoles, P.ManageChannels]))
      throw Error("Bot needs Manage Roles and Manage Channels.");
    const graves = (
      await this.store.pool.query(
        "SELECT * FROM four_graves_graves WHERE event_id=$1 ORDER BY grave_id",
        [e.id],
      )
    ).rows;
    // The 15-second timer must NEVER issue gateway opcode 8.
    // Before CLOSE, all grave roles are invisible, so no member scan is needed.
    // After CLOSE, one shared REST pagination/cache feeds LIVE and TEST.
    let members = null;
    if (e.closed_at) {
      try {
        members = await getGuildMemberRoster(guild);
        await this.store.operation(e.id, "member-roster");
      } catch (err) {
        // Do not stall already-saved choices, announcements, or prize draws
        // merely because a roster refresh is temporarily unavailable.
        if (err.code !== "MEMBER_ROSTER_BACKOFF") {
          await this.store.operation(e.id, "member-roster", diagnosticError(err));
          this.logFailure("member-roster", e.id, err);
        }
      }
    }
    for (const g of graves) {
      try {
        const { role, channel, overwrites } = await this.resources(guild, e, g);
        const eligible = new Set(
          (e.snapshot?.entries || [])
            .filter((x) => x.grave_id === g.grave_id)
            .map((x) => x.discord_id),
        );
        const changes = members && [...members.values()].some(
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
        for (const member of members?.values() || []) {
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
          if (g.decision) {
            // Send the one-time @everyone announcement; on replay the exact
            // footer is adopted, and edits always suppress further pings.
            await this.message(
              e, channel, g.choice_message_id, "choice:"+g.grave_id,
              choiceAnnouncement(e,g),
              m => this.store.pool.query(
                "UPDATE four_graves_graves SET choice_message_id=$3 WHERE event_id=$1 AND grave_id=$2",
                [e.id,g.grave_id,m.id],
              ),
            );
          }
          if (g.decision && g.reveal_stage >= 3) {
            // Only one due prize is selected and announced per pass.
            // Failed sends are retried with the SAME recorded ticket.
            const award = await this.store.nextPrize(e.guild_id,g.grave_id);
            if (award) {
              const m = await this.message(
                e, channel, award.message_id,
                "award:"+g.grave_id+":"+award.prize_index,
                winnerAnnouncement(e,g.grave_id,award),
                msg => this.store.pool.query(
                  "UPDATE four_graves_awards SET message_id=$4 WHERE event_id=$1 AND grave_id=$2 AND prize_index=$3",
                  [e.id,g.grave_id,award.prize_index,msg.id],
                ),
              );
              await this.store.markPrizeAnnounced(
                e.guild_id,g.grave_id,award.prize_index,m.id,
              );
            }
          }
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
      try {
        const channel = guild.channels.cache.get(e.panel_channel_id) ||
          await guild.channels.fetch(e.panel_channel_id);
        if (
          !channel ||
          channel.guildId !== e.guild_id ||
          channel.type !== ChannelType.GuildText
        )
          throw Error("Configured grave-selection panel channel is missing or not a text channel: " + e.panel_channel_id);
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
        await this.store.operation(e.id, "public-panel");
      } catch (err) {
        // A deleted or inaccessible public panel must not block already-open
        // private grave channels, countdowns or persisted winner announcements.
        await this.store.operation(e.id, "public-panel", diagnosticError(err));
        this.logFailure("public-panel", e.id, err);
      }
    }
    // Do not mark the event complete until every populated grave has
    // chosen and announced its entire prize pool of winners.
    if (e.closed_at && graves.some(g => g.decision))
      await this.store.pool.query(
        "UPDATE four_graves_events SET state=\'REVEALED\' WHERE id=$1 AND NOT EXISTS (" +
          "SELECT 1 FROM four_graves_graves g WHERE g.event_id=$1 " +
          "AND EXISTS (SELECT 1 FROM four_graves_allocations a WHERE a.event_id=$1 AND a.grave_id=g.grave_id) " +
          "AND (g.decision IS NULL OR g.draw_completed_at IS NULL))",
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
          // Preserve the original error even if the database audit write also fails.
          try {
            await this.store.operation(event.id, "worker", diagnosticError(err));
          } catch (auditErr) {
            this.logFailure("worker-audit", event.id, auditErr);
          }
          this.logFailure("worker", event.id, err);
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
        this.tick().catch(err => this.logFailure("worker-loop", "all", err)),
      15000,
    );
    this.timer.unref?.();
    void this.tick().catch(err => this.logFailure("worker-startup", "all", err));
  }
}
module.exports = {
  FourGravesDiscord,
  getGuildMemberRoster,
  noteGuildMemberChange,
  publicPanel,
  decisionPanel,
  button,
  row,
  cid,
};
