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
