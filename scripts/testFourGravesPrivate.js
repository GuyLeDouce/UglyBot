"use strict";
const assert = require("node:assert/strict");
const { PermissionFlagsBits: P } = require("discord.js");
const { FourGravesStore } = require("../modules/fourGravesStore");
const { FourGravesDiscord, publicPanel, decisionPanel } = require("../modules/fourGravesDiscord");
const { GRAVES, DEFAULT_CONFIG } = require("../modules/fourGravesCore");
const { buildSlashCommand, enabled } = require("../modules/fourGravesTest");
const { fixture } = require("./fourGravesDiscordFixture");

async function main() {
  const key = "four-graves-private-test-unit";
  assert.throws(() => new FourGravesStore({}, {
    eventKey: "squigs-four-graves-2026", testMode: true,
  }), /Invalid sandbox key/);

  const queries = [];
  const mockPool = {
    query: async (sql, params) => {
      queries.push({ sql, params });
      if (sql.includes("SELECT discord_id,treats,tricks FROM four_graves_test_tickets"))
        return { rows: [{ discord_id: "10", treats: 3, tricks: 2 }] };
      return { rows: [], rowCount: 1 };
    },
  };
  const store = new FourGravesStore(mockPool, {
    eventKey: key, sourceEventKey: "four-graves-private-test-synthetic", testMode: true,
  });
  const earned = await store.earned(mockPool, {
    guild_id: "100", source_event_key: "squigs-trick-or-treat-2026",
  });
  assert.equal(earned.length, 1);
  assert.equal(earned[0].discord_id, "10");
  assert.ok(queries[0].sql.includes("four_graves_test_tickets"));
  assert.ok(!queries[0].sql.includes("trick_or_treat_treats"));
  assert.deepEqual(queries[0].params, [key, "100"]);

  const mock = fixture("100");
  const w = new FourGravesDiscord(store, {
    ...mock.deps, adminIds: () => new Set(["global-admin"]),
  });
  const e = {
    id: "123", guild_id: "100", state: "SETUP",
    config: {
      ...DEFAULT_CONFIG, testMode: true, testOwnerId: "10",
    },
    closed_at: null, deadline: null, generation: 0,
  };
  const g = { grave_id: GRAVES[0].id, role_id: null, channel_id: null };
  const { role, channel, overwrites } = await w.resources(mock.guild, e, g);
  assert.ok(role.name.startsWith("[TEST] Grave"));
  assert.ok(channel.name.startsWith("test-grave-"));
  assert.equal(channel.topic, "four-graves:123:barry_buckets");
  assert.ok(overwrites.some(x => x.id === "100" && x.deny.includes(P.ViewChannel)));
  assert.ok(overwrites.some(x => x.id === "10" && x.allow.includes(P.ViewChannel)));
  assert.ok(!overwrites.some(x => x.id === "global-admin"));
  assert.ok(!overwrites.some(x => x.id === "900")); // ManageGuild role gets no test access
  assert.ok(!role.mentionable);
  assert.equal(role.permissions.bitfield, 0n);
  assert.match(publicPanel(e).embeds[0].title, /TEST ONLY/);
  assert.match(publicPanel(e).embeds[0].description, /NO REAL TICKETS/);
  assert.deepEqual(publicPanel(e).allowedMentions, { parse: [] });

  const state = { ...e,
    state: "BUTTONS_ACTIVE", generation: 1,
    deadline: new Date().toISOString(),
    snapshot: { entries: [{ grave_id: GRAVES[0].id, discord_id: "10" }] },
  };
  const panel = decisionPanel(state, {
    ...g, reveal_stage: 3, decision: { user_id: "10", choice: "TREAT" },
  });
  assert.match(panel.embeds[0].title, /TEST/);
  assert.match(panel.embeds[0].description, /TEST OPERATOR/);
  assert.doesNotMatch(panel.embeds[0].description, /<@10>/);
  assert.deepEqual(panel.allowedMentions, { parse: [] });
  assert.equal(panel.components[0].components[0].disabled, true);
  assert.equal(panel.components[0].components[1].disabled, true);

  await assert.rejects(() => w.reconcile({
    ...e, config: { ...DEFAULT_CONFIG, testMode: false },
  }), /event-mode mismatch/);
  const command = buildSlashCommand().toJSON();
  assert.equal(command.name, "fourgravestest");
  const reset = command.options.find(x => x.name === "reset");
  assert.equal(reset.options.find(x => x.name === "confirm").required, true);
  assert.ok(command.options.some(x => x.name === "advance"));
  assert.equal(enabled(), process.env.FOUR_GRAVES_TEST_ENABLED === "true");
  console.log("PASS private Four Graves test identity, tickets, roles, panels and commands");
}

main().catch(e => { console.error(e); process.exitCode = 1; });
