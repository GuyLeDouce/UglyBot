"use strict";
const assert = require("assert/strict");
const { FourGravesStore } = require("../modules/fourGravesStore");
const { GRAVES, exportFiles } = require("../modules/fourGravesCore");
async function main() {
  let pool, cleanup;
  const embedded = process.env.FOUR_GRAVES_TEST_PGLITE === "true";
  if (embedded) {
    const { PGlite } = require("@electric-sql/pglite");
    const db = new PGlite();
    let tail = Promise.resolve();
    pool = {
      query: (sql, params) =>
        params
          ? db.query(sql, params)
          : db.exec(sql).then((rs) => rs.at(-1) || { rows: [], rowCount: 0 }),
      connect: async () => {
        let release;
        const next = new Promise((r) => (release = r));
        const previous = tail;
        tail = next;
        await previous;
        return { query: pool.query, release };
      },
    };
    const q = pool.query;
    pool.query = async (...a) => {
      const r = await q(...a);
      return { ...r, rowCount: r.rowCount ?? r.affectedRows ?? r.rows?.length };
    };
    cleanup = () => db.close();
  } else {
    const url = process.env.FOUR_GRAVES_TEST_DATABASE_URL;
    if (!url)
      throw Error(
        "Set FOUR_GRAVES_TEST_DATABASE_URL to a disposable PostgreSQL database. No production fallback.",
      );
    const { Pool } = require("pg");
    const schema = "fg_test_" + Date.now();
    const admin = new Pool({ connectionString: url });
    await admin.query(`CREATE SCHEMA ${schema}`);
    pool = new Pool({
      connectionString: url,
      options: `-c search_path=${schema}`,
    });
    cleanup = async () => {
      await pool.end();
      await admin.query(`DROP SCHEMA ${schema} CASCADE`);
      await admin.end();
    };
  }
  let count = 0;
  const test = async (name, fn) => {
    await fn();
    count++;
    console.log("PASS", name);
  };
  try {
    const tot = require("../modules/trickOrTreat");
    tot.initTrickOrTreat({ pool });
    await tot.ensureTables();
    const s = new FourGravesStore(pool);
    await s.migrate();
    const guild = "100",
      user = { id: "10", username: "guy", display_name: "Guy" };
    const treat = async (id, n) => {
      for (let i = 0; i < n; i++)
        await pool.query(
          "INSERT INTO trick_or_treat_treats(event_key,guild_id,discord_id,event_day,wallet_count,squig_count) VALUES($1,$2,$3,$4,1,1)",
          [s.sourceEventKey, guild, id, `day-${i}`],
        );
    };
    await treat(user.id, 10);
    await treat("20", 2);
    await pool.query(
      "INSERT INTO trick_or_treat_tricks(event_key,guild_id,discord_id,chain,contract_address,token_id,transaction_hash,buyer_wallet,event_timestamp) VALUES($1,$2,'20','ethereum','contract','1','tx','buyer',clock_timestamp())",
      [s.sourceEventKey, guild],
    );
    await test("setup remains closed and repeated setup is safe", async () => {
      await s.setup(guild, "admin");
      await s.setup(guild, "admin");
      const d = await s.read(guild);
      assert.equal(d.event.state, "SETUP");
      assert.equal(d.graves.length, 4);
    });
    await test("cannot allocate before manual opening", () =>
      assert.rejects(
        () => s.allocate(guild, user, GRAVES[0].id, 1, "before"),
        /SEALED/,
      ));
    await s.transition(guild, "admin", "open");
    await test("literal treat and trick counts", async () => {
      const u = (await s.read(guild)).snapshot.users.find(
        (u) => u.discord_id === "20",
      );
      assert.equal(u.treats, 2);
      assert.equal(u.tricks, 1);
      assert.equal(u.total, 3);
    });
    await test("concurrent allocations never overspend", async () => {
      const r = await Promise.allSettled([
        s.allocate(guild, user, GRAVES[0].id, 8, "a"),
        s.allocate(guild, user, GRAVES[1].id, 8, "b"),
      ]);
      assert.equal(r.filter((x) => x.status === "fulfilled").length, 1);
      assert.equal((await s.read(guild)).snapshot.entries.length, 8);
    });
    const a = (await s.read(guild)).snapshot.entries[0];
    const key = a.grave_id === GRAVES[0].id ? "a" : "b";
    await test("confirmation replay returns original allocation", async () => {
      const r = await s.allocate(guild, user, a.grave_id, 8, key);
      assert.equal(String(r.id), String(a.allocation_id));
      assert.equal((await s.read(guild)).snapshot.entries.length, 8);
    });
    await test("invalid quantities", async () => {
      for (const q of [0, -1, 1.5])
        await assert.rejects(() =>
          s.allocate(guild, user, GRAVES[0].id, q, "bad"),
        );
    });
    await s.allocate(guild, user, GRAVES[2].id, 2, "c");
    await test("new earned claims become allocatable", async () => {
      await pool.query(
        "INSERT INTO trick_or_treat_treats(event_key,guild_id,discord_id,event_day,wallet_count,squig_count) VALUES($1,$2,'10','new-day',1,1)",
        [s.sourceEventKey, guild],
      );
      await s.allocate(guild, user, GRAVES[3].id, 1, "new");
    });
    await s.configure(guild, "admin", { prizesFinalized: false });
    await test("close requires prize finalization", () =>
      assert.rejects(() => s.transition(guild, "admin", "close"), /Finalize/));
    await s.configure(guild, "admin", { prizesFinalized: true });
    await test("close racing allocation preserves exact snapshot", async () => {
      const r = await Promise.allSettled([
        s.transition(guild, "admin", "close", {
          10: { username: "guy", display_name: "Frozen Guy" },
        }),
        s.allocate(
          guild,
          { id: "20", username: "v", display_name: "Vince" },
          GRAVES[0].id,
          1,
          "race",
        ),
      ]);
      assert.equal(r[0].status, "fulfilled");
      const d = await s.read(guild);
      const n = (
        await pool.query(
          "SELECT SUM(quantity)::int AS n FROM four_graves_allocations",
        )
      ).rows[0].n;
      assert.equal(d.snapshot.entries.length, n);
    });
    const closed = await s.read(guild),
      before = JSON.stringify(
        exportFiles(closed.event, closed.snapshot, "all", "all", "fixed"),
      );
    await test("duplicate close preserves snapshot and countdown", async () => {
      await s.transition(guild, "admin", "close");
      const d = await s.read(guild);
      assert.equal(d.event.snapshot_id, closed.event.snapshot_id);
      assert.equal(
        new Date(d.event.deadline).getTime(),
        new Date(closed.event.deadline).getTime(),
      );
    });
    await test("post-close claims do not change frozen exports", async () => {
      await pool.query(
        "INSERT INTO trick_or_treat_treats(event_key,guild_id,discord_id,event_day,wallet_count,squig_count) VALUES($1,$2,'10','post-close',1,1)",
        [s.sourceEventKey, guild],
      );
      const d = await s.read(guild);
      assert.equal(
        JSON.stringify(exportFiles(d.event, d.snapshot, "all", "all", "fixed")),
        before,
      );
    });
    await test("closed allocation fails", () =>
      assert.rejects(
        () => s.allocate(guild, user, GRAVES[0].id, 1, "late"),
        /SEALED/,
      ));
    await test("old confirmation replay after close still returns receipt", () =>
      s.allocate(guild, user, a.grave_id, 8, key));
    await test("early decisions fail", () =>
      assert.rejects(
        () => s.decide(guild, user, a.grave_id, "TRICK", 1),
        /not active/,
      ));
    await s.transition(guild, "admin", "restart");
    await pool.query(
      "UPDATE four_graves_events SET deadline=clock_timestamp()-interval '1 second'",
    );
    await test("stale generation fails", () =>
      assert.rejects(
        () => s.decide(guild, user, a.grave_id, "TRICK", 1),
        /not active/,
      ));
    await test("nonmember cannot decide", () =>
      assert.rejects(
        () => s.decide(guild, { id: "999" }, a.grave_id, "TRICK", 2),
        /BELONG/,
      ));
    await test("first concurrent decision wins exactly once", async () => {
      const r = await Promise.allSettled(
        ["TRICK", "TREAT"].map((choice) =>
          s.decide(guild, user, a.grave_id, choice, 2),
        ),
      );
      assert.equal(r.filter((x) => x.status === "fulfilled").length, 1);
    });
    await test("restart prohibited after decision", () =>
      assert.rejects(
        () => s.transition(guild, "admin", "restart"),
        /decision/,
      ));
    await test("other graves reveal independently", () =>
      s.decide(guild, user, GRAVES[2].id, "TREAT", 2));
    await test("recovery from new store instance retains decision", async () => {
      const next = new FourGravesStore(pool);
      const d = await next.read(guild);
      assert.equal(
        d.graves.find((x) => x.grave_id === a.grave_id).decision.user_id,
        user.id,
      );
    });
    await test("claims never deleted or decremented", async () =>
      assert.equal(
        (
          await pool.query(
            "SELECT COUNT(*)::int AS n FROM trick_or_treat_treats",
          )
        ).rows[0].n,
        14,
      ));
    const { fixture } = require("./fourGravesDiscordFixture");
    const { FourGravesDiscord } = require("../modules/fourGravesDiscord");
    const mock = fixture(guild),
      worker = new FourGravesDiscord(s, mock.deps);
    const current = async () => (await s.read(guild)).event;
    await test("creates four private channels and roles", async () => {
      await worker.reconcile(await current());
      assert.equal(mock.channels.size, 4);
      assert.equal(mock.roles.size, 5);
    });
    await test("repeated reconciliation reuses resources and messages", async () => {
      await worker.reconcile(await current());
      assert.equal(mock.channels.size, 4);
      assert.equal(mock.roles.size, 5);
      const graves = (await s.read(guild)).graves;
      for (const g of graves)
        assert.equal((await mock.channels.get(g.channel_id).messages.fetch({})).size,
          1 + Number(!!g.decision));
    });
    await test("missing public panel is recorded without breaking private graves", async () => {
      const original = await current();
      await pool.query(
        "UPDATE four_graves_events SET panel_channel_id=$2 WHERE id=$1",
        [original.id, "deleted-public-channel"],
      );
      try {
        // The private graves and winner workflow must still reconcile.
        await worker.reconcile(await current());
        const d = await s.read(guild);
        const failure = d.operations.find(x => x.operation_key === "public-panel");
        assert.match(String(failure?.last_error), /missing|not a text channel/i);
        assert.equal(mock.channels.size, 4);
        for (const g of d.graves) assert.ok(mock.channels.get(g.channel_id));
      } finally {
        await pool.query(
          "UPDATE four_graves_events SET panel_channel_id=NULL WHERE id=$1",
          [original.id],
        );
        await s.operation(original.id, "public-panel");
      }
    });
    await test("membership assigned for every allocated grave", async () => {
      const d = await s.read(guild);
      for (const g of d.graves)
        assert.equal(
          mock.members.get("10").roles.cache.has(g.role_id),
          d.snapshot.entries.some(
            (x) => x.discord_id === "10" && x.grave_id === g.grave_id,
          ),
        );
    });
    await test("permissions isolate teams and preserve admin access", async () => {
      const d = await s.read(guild);
      for (const g of d.graves) {
        const ow = mock.channels.get(g.channel_id).overwrites;
        assert.ok(ow.find((x) => x.id === guild).deny.length);
        assert.ok(ow.find((x) => x.id === "admin").allow.length);
        assert.ok(ow.find((x) => x.id === "900").allow.length);
        assert.equal(
          ow.filter((x) => d.graves.some((y) => y.role_id === x.id)).length,
          1,
        );
      }
    });
    await test("incorrect dedicated roles removed", async () => {
      const g = (await s.read(guild)).graves[0];
      mock.members
        .get("999")
        .roles.cache.set(g.role_id, mock.roles.get(g.role_id));
      await worker.reconcile(await current());
      assert.equal(mock.members.get("999").roles.cache.size, 0);
    });
    await test("role assignment failure preserves snapshot and retries", async () => {
      const before = (await current()).snapshot_id;
      mock.members.get("10").roles.cache.clear();
      mock.fail.role = true;
      await worker.reconcile(await current());
      assert.equal((await current()).snapshot_id, before);
      assert.ok((await s.read(guild)).operations.some((o) => o.last_error));
      mock.fail.role = false;
      await worker.reconcile(await current());
      assert.ok(mock.members.get("10").roles.cache.size);
    });
    await test("rejoin restores team roles", async () => {
      mock.members.get("10").roles.cache.clear();
      await worker.reconcile(await current());
      assert.ok(mock.members.get("10").roles.cache.size);
    });
    await test("deleted role safely recreated", async () => {
      const g = (await s.read(guild)).graves[0];
      mock.roles.delete(g.role_id);
      await worker.reconcile(await current());
      assert.notEqual((await s.read(guild)).graves[0].role_id, g.role_id);
      assert.equal(mock.roles.size, 5);
    });
    await test("failed reveal edit resumes without duplicate messages", async () => {
      mock.fail.message = true;
      await pool.query(
        "UPDATE four_graves_graves SET next_reveal_at=clock_timestamp()-interval '1 second' WHERE decision IS NOT NULL",
      );
      await worker.reconcile(await current());
      mock.fail.message = false;
      await worker.reconcile(await current());
      const graves = (await s.read(guild)).graves;
      for (const g of graves)
        assert.equal((await mock.channels.get(g.channel_id).messages.fetch({})).size,
          1 + Number(!!g.decision));
    });
    await test("restart completes reveal using persisted decision", async () => {
      const next = new FourGravesDiscord(new FourGravesStore(pool), mock.deps);
      for (let i = 0; i < 3; i++) {
        await pool.query(
          "UPDATE four_graves_graves SET next_reveal_at=clock_timestamp()-interval '1 second' WHERE decision IS NOT NULL",
        );
        await next.reconcile(await current());
      }
      const d = await s.read(guild);
      for (const g of d.graves.filter((g) => g.decision))
        assert.equal(g.reveal_stage, 3);
    });
    await test("first decision publishes exact one-time team choice ping", async () => {
      const d = await s.read(guild);
      for (const g of d.graves.filter(x=>x.decision)) {
        assert.ok(g.choice_message_id);
        const messages = await mock.channels.get(g.channel_id).messages.fetch({});
        const choice = [...messages.values()].find(m =>
          m.embeds?.some(x=>x.footer?.text ===
            "four-graves:"+d.event.id+":"+g.grave_id+":choice"));
        assert.ok(choice, "Choice notification must survive worker recovery");
      }
    });
    await test("two-minute wait and one winner every 15 seconds", async () => {
      const d = await s.read(guild);
      const g = d.graves.find(x=>x.decision && d.snapshot.entries.some(t=>t.grave_id===x.grave_id));
      assert.ok(g);
      assert.equal(await s.nextPrize(guild,g.grave_id),null);
      await pool.query(
        "UPDATE four_graves_graves SET next_draw_at=clock_timestamp()-interval '1 second' WHERE event_id=$1 AND grave_id=$2",
        [d.event.id,g.grave_id],
      );
      const winner = await s.nextPrize(guild,g.grave_id);
      assert.equal(winner.prize_index,1);
      assert.ok(winner.ticket_entry_id);
      const same = await s.nextPrize(guild,g.grave_id);
      assert.equal(String(same.ticket_entry_id),String(winner.ticket_entry_id));
      await s.markPrizeAnnounced(guild,g.grave_id,1,"fake-prize-message-1");
      assert.equal(await s.nextPrize(guild,g.grave_id),null);
      await pool.query(
        "UPDATE four_graves_graves SET next_draw_at=clock_timestamp()-interval '1 second' WHERE event_id=$1 AND grave_id=$2",
        [d.event.id,g.grave_id],
      );
      const second = await s.nextPrize(guild,g.grave_id);
      assert.equal(second.prize_index,2);
      if(d.snapshot.entries.some(x=>x.grave_id===g.grave_id && x.discord_id!==winner.winner_discord_id))
        assert.notEqual(second.winner_discord_id,winner.winner_discord_id);
      await s.markPrizeAnnounced(guild,g.grave_id,2,"fake-prize-message-2");
      const stored = await s.read(guild);
      assert.equal(stored.awards.filter(a=>a.grave_id===g.grave_id).length,2);
      assert.ok(stored.awards.every(a=>a.announced_at));
    });
    await test("admin export and configuration are permission checked", async () => {
      const api = require("../modules/fourGraves");
      const old = process.env.FOUR_GRAVES_ENABLED;
      process.env.FOUR_GRAVES_ENABLED = "true";
      api.initFourGraves({ pool, ...mock.deps, isAdmin: () => false });
      try {
        for (const command of ["export", "configure", "admin"]) {
          let response;
          const interaction = {
            guildId: guild,
            user: { id: "999" },
            commandName: "fourgraves",
            isChatInputCommand: () => true,
            options: { getSubcommand: () => command, getString: () => null },
            deferReply: async () => {
              interaction.deferred = true;
            },
            editReply: async (value) => {
              response = value;
            },
          };
          assert.equal(await api.handle(interaction), true);
          assert.match(response.content, /Administrator access/);
        }
      } finally {
        if (old === undefined) delete process.env.FOUR_GRAVES_ENABLED;
        else process.env.FOUR_GRAVES_ENABLED = old;
      }
    });
    await test("legacy live and test grave panels get one-time artwork updates", async () => {
      const { PANEL_IMAGE_URL, PANEL_IMAGE_VERSION } = require("../modules/fourGravesCore");
      const artStore = new FourGravesStore(pool, {
        eventKey: "squigs-four-graves-panel-artwork-fixture",
        sourceEventKey: s.sourceEventKey,
      });
      const oldIds = [];
      for (const guildId of ["art-open", "art-closed"]) {
        const e = await artStore.setup(guildId, "admin");
        oldIds.push(String(e.id));
        if (guildId === "art-closed") {
          await artStore.transition(guildId, "admin", "open");
          await artStore.transition(guildId, "admin", "close");
        } else await artStore.transition(guildId, "admin", "open");
        // Emulate an old production record, potentially with custom old art.
        await pool.query(
          "UPDATE four_graves_events SET config=jsonb_set(config - 'panelArtworkVersion', '{image}', to_jsonb($2::text), true) WHERE id=$1",
          [e.id, "https://example.com/old-grave-art.png"],
        );
      }
      const other = await artStore.setup("art-custom", "admin");
      await artStore.configure("art-custom", "admin", {
        image: "https://example.com/admin-overridden.png",
      });
      const before = (await artStore.read("art-closed")).event;
      await artStore.migrate();
      for (const guildId of ["art-open", "art-closed"]) {
        const changed = (await artStore.read(guildId)).event;
        assert.equal(changed.config.image, PANEL_IMAGE_URL);
        assert.equal(changed.config.panelArtworkVersion, PANEL_IMAGE_VERSION);
        assert.equal(changed.config.mysteryByGrave.cletus_von_wrinkles[0].name, "RugDollz 3D");
        if (guildId === "art-closed") {
          assert.equal(changed.state, "CLOSED_COUNTDOWN");
          assert.equal(changed.snapshot_hash, before.snapshot_hash);
          assert.equal(changed.snapshot_id, before.snapshot_id);
        }
      }
      assert.equal((await artStore.read("art-custom")).event.config.image,
        "https://example.com/admin-overridden.png");
      const auditCount = async () => (
        await pool.query(
          "SELECT event_id,action FROM four_graves_audit WHERE action='grave-panel-artwork-update'"
        )
      ).rows;
      const audited = await auditCount();
      assert.deepEqual(audited.map(a => String(a.event_id)).sort(),oldIds.sort());
      await artStore.migrate();
      assert.equal((await auditCount()).length,2);
      // Explicit admin image overrides after the one-time migration survive
      // future restarts; the migration never becomes a recurring overwrite.
      await artStore.configure("art-open", "admin", {
        image: "https://example.com/new-custom-art.png",
      });
      await artStore.migrate();
      assert.equal((await artStore.read("art-open")).event.config.image,
        "https://example.com/new-custom-art.png");
      assert.equal((await auditCount()).length,2);
      assert.notEqual(String(other.id),"");
    });
    await test("Cletus RugDollz upgrade updates only unclosed matching pools", async () => {
      const legacy = new FourGravesStore(pool, {
        eventKey: "squigs-four-graves-prize-change-fixture",
        sourceEventKey: s.sourceEventKey,
      });
      const oldPrize = [{ name: "Doruzu (ApeChain)", quantity: 2 }];
      const newPrize = [{ name: "RugDollz 3D", quantity: 3 }];
      const customPrize = [{ name: "Alternate Custom NFT", quantity: 2 }];
      for (const guildId of ["prize-open", "prize-closed", "prize-custom"]) {
        const e = await legacy.setup(guildId, "admin");
        const old = guildId === "prize-custom" ? customPrize : oldPrize;
        await pool.query(
          "UPDATE four_graves_events SET config=jsonb_set(config,'{mysteryByGrave,cletus_von_wrinkles}',$2::jsonb,false) WHERE id=$1",
          [e.id, JSON.stringify(old)],
        );
        if (guildId !== "prize-custom") await legacy.transition(guildId, "admin", "open");
        if (guildId === "prize-closed") await legacy.transition(guildId, "admin", "close");
      }
      const frozenBefore = (await legacy.read("prize-closed")).event;
      await legacy.migrate();
      const open = (await legacy.read("prize-open")).event;
      const closed = (await legacy.read("prize-closed")).event;
      const custom = (await legacy.read("prize-custom")).event;
      assert.deepEqual(open.config.mysteryByGrave.cletus_von_wrinkles, newPrize);
      assert.equal(open.config.prizesFinalized, true);
      assert.equal(open.state, "OPEN");
      assert.deepEqual(closed.config.mysteryByGrave.cletus_von_wrinkles, oldPrize);
      assert.equal(closed.snapshot_hash, frozenBefore.snapshot_hash);
      assert.equal(closed.state, "CLOSED_COUNTDOWN");
      assert.deepEqual(custom.config.mysteryByGrave.cletus_von_wrinkles, customPrize);
      assert.equal(open.config.mysteryByGrave.barry_buckets[0].name, "Scribble Otter Yoink Club");
      const audits = () => pool.query(
        "SELECT event_id,action,detail FROM four_graves_audit WHERE action='cletus-bonus-prize-update'",
      );
      const first = await audits();
      assert.equal(first.rows.length, 1);
      assert.equal(String(first.rows[0].event_id), String(open.id));
      assert.deepEqual(first.rows[0].detail.old, oldPrize);
      assert.deepEqual(first.rows[0].detail.new, newPrize);
      await legacy.migrate();
      assert.equal((await audits()).rows.length, 1);
      assert.deepEqual(
        (await legacy.read("prize-open")).event.config.mysteryByGrave.cletus_von_wrinkles,
        newPrize,
      );
    });
    await test("four player-count rosters follow first burial, update, and freeze without duplicates", async () => {
      const { fixture } = require("./fourGravesDiscordFixture");
      const { FourGravesDiscord } = require("../modules/fourGravesDiscord");
      const { PANEL_INDICES } = require("../modules/fourGravesCore");
      const simulatedGuild = "roster-test-guild";
      const env = fixture(simulatedGuild);
      const panel = await env.guild.channels.create({name:"select-your-grave"});
      await s.setup(simulatedGuild,"admin");
      await pool.query(
        "UPDATE four_graves_events SET panel_channel_id=$2 WHERE event_key=$1 AND guild_id=$3",
        [s.eventKey,panel.id,simulatedGuild],
      );
      const runner = new FourGravesDiscord(s,env.deps);
      const currentRoster = async () => (await s.read(simulatedGuild)).event;
      const rosterId = async graveId =>
        (await s.read(simulatedGuild)).graves.find(x=>x.grave_id===graveId).roster_message_id;

      await runner.reconcile(await currentRoster());
      assert.equal((await panel.messages.fetch({})).size,1);
      assert.ok((await s.read(simulatedGuild)).graves.every(g=>!g.roster_message_id));

      for (const [userId, day] of [
        ["10","roster-day-1"],["10","roster-day-2"],["20","roster-day-1"]
      ]) await pool.query(
        "INSERT INTO trick_or_treat_treats(event_key,guild_id,discord_id,event_day,wallet_count,squig_count) " +
        "VALUES($1,$2,$3,$4,1,1)",
        [s.sourceEventKey,simulatedGuild,userId,day],
      );
      await s.transition(simulatedGuild,"admin","open");
      await s.allocate(simulatedGuild,
        {id:"10",username:"player_1",display_name:"Player 1"},
        GRAVES[2].id,2,"roster-test-ticket-1");
      await runner.reconcile(await currentRoster());
      const ev = await currentRoster();
      const graveIds = PANEL_INDICES.map(index=>GRAVES[index].id);
      const rows = (await s.read(simulatedGuild)).graves;
      assert.ok(rows.every(g=>g.roster_message_id));
      const rosterIds = Object.fromEntries(rows.map(g=>[g.grave_id,g.roster_message_id]));
      const messages = [...(await panel.messages.fetch({})).values()];
      assert.equal(messages.length,5,"Selection + four distinct grave roster messages");
      assert.deepEqual(messages.map(m=>m.embeds[0].footer?.text),[
        "four-graves:"+ev.id+":public",
        ...graveIds.map(id=>"four-graves:"+ev.id+":roster:"+id),
      ]);
      const cletus = await panel.messages.fetch(rosterIds.cletus_von_wrinkles);
      assert.match(cletus.embeds[0].description,/Player 1 — 2 tickets/);
      assert.doesNotMatch(cletus.embeds[0].description,/Player 1\(1\)/);
      assert.match(cletus.embeds[0].fields[0].value,/2 tickets · 1 Squig/);
      assert.doesNotMatch(cletus.embeds[0].description,/RugDollz|DollzOnApe/);
      const barryEmpty = await panel.messages.fetch(rosterIds.barry_buckets);
      assert.match(barryEmpty.embeds[0].description,/Nobody has buried/);

      await s.allocate(simulatedGuild,
        {id:"20",username:"player_2",display_name:"Player 2"},
        GRAVES[0].id,1,"roster-test-ticket-2");
      await runner.reconcile(await currentRoster());
      assert.equal((await panel.messages.fetch({})).size,5);
      assert.deepEqual(
        Object.fromEntries((await s.read(simulatedGuild)).graves.map(g=>[g.grave_id,g.roster_message_id])),
        rosterIds,
      );
      const barry = await panel.messages.fetch(rosterIds.barry_buckets);
      assert.match(barry.embeds[0].description,/Player 2 — 1 ticket/);
      assert.match(barry.embeds[0].fields[0].value,/1 ticket · 1 Squig/);

      // A third ticket by the same player updates their ONE roster line,
      // never a second numbered line, while the draw still has 3 entries.
      await pool.query(
        "INSERT INTO trick_or_treat_treats(event_key,guild_id,discord_id,event_day,wallet_count,squig_count) " +
        "VALUES($1,$2,'10','roster-day-3',1,1)",
        [s.sourceEventKey,simulatedGuild],
      );
      await s.allocate(simulatedGuild,
        {id:"10",username:"player_1",display_name:"Player 1"},
        GRAVES[2].id,1,"roster-test-ticket-3");
      await runner.reconcile(await currentRoster());
      const updated = await panel.messages.fetch(rosterIds.cletus_von_wrinkles);
      assert.match(updated.embeds[0].description,/Player 1 — 3 tickets/);
      assert.doesNotMatch(updated.embeds[0].description,/Player 1\(/);
      assert.equal((await s.read(simulatedGuild)).snapshot.entries.filter(
        ticket=>ticket.grave_id===GRAVES[2].id).length,3);
      assert.equal((await panel.messages.fetch({})).size,5);
      const frozenDescriptions = Object.fromEntries(await Promise.all(
        graveIds.map(async id=>[id,(await panel.messages.fetch(rosterIds[id])).embeds[0].description])
      ));
      await s.transition(simulatedGuild,"admin","close");
      await runner.reconcile(await currentRoster());
      await pool.query(
        "INSERT INTO trick_or_treat_treats(event_key,guild_id,discord_id,event_day,wallet_count,squig_count) " +
        "VALUES($1,$2,'20','roster-day-2',1,1)",
        [s.sourceEventKey,simulatedGuild],
      );
      await runner.reconcile(await currentRoster());
      for (const id of graveIds)
        assert.equal((await panel.messages.fetch(rosterIds[id])).embeds[0].description,
          frozenDescriptions[id]);
      assert.equal((await panel.messages.fetch({})).size,5);
      const ops = (await s.read(simulatedGuild)).operations;
      assert.ok(graveIds.every(id=>ops.some(o=>
        o.operation_key==="public-roster:"+id && !o.last_error)));
    });
    console.log(
      `${count} database tests passed (${embedded ? "embedded PostgreSQL; pooled concurrency requires server suite" : "PostgreSQL server with concurrent pooled connections"}).`,
    );
  } finally {
    await cleanup();
  }
}
main().catch((e) => {
  console.error(e.message);
  process.exitCode = 1;
});
