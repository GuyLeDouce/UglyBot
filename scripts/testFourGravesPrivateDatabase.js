"use strict";
const assert = require("node:assert/strict");
const { randomBytes } = require("node:crypto");
const { Pool } = require("pg");
const { FourGravesStore } = require("../modules/fourGravesStore");
const { GRAVES } = require("../modules/fourGravesCore");

async function main() {
  const url = process.env.FOUR_GRAVES_TEST_DATABASE_URL;
  if (!url) throw Error("FOUR_GRAVES_TEST_DATABASE_URL must be a disposable PostgreSQL database.");
  const schema = "fg_private_" + randomBytes(5).toString("hex");
  const admin = new Pool({ connectionString: url });
  let pool;
  try {
    await admin.query("CREATE SCHEMA " + schema);
    pool = new Pool({ connectionString: url, options: "-c search_path=" + schema });
    const originalClaims = require("../modules/trickOrTreat");
    originalClaims.initTrickOrTreat({ pool });
    await originalClaims.ensureTables();
    const live = new FourGravesStore(pool);
    const sandbox = new FourGravesStore(pool, {
      testMode: true,
      eventKey: "four-graves-private-test-db",
      sourceEventKey: "four-graves-private-test-synthetic",
    });
    await sandbox.migrate();
    const guild = "12345";
    const owner = { id: "10", username: "Operator", display_name: "Operator" };
    const fake = { id: "test-fake-01", username: "Test Participant 01",
      display_name: "Test Participant 01" };
    // Real claim rows (in disposable test schema) must NEVER be used by sandbox.
    await pool.query(
      "INSERT INTO trick_or_treat_treats(event_key,guild_id,discord_id,event_day,wallet_count,squig_count) " +
      "VALUES($1,$2,$3,'oct-01',1,1)",
      [live.sourceEventKey, guild, "99"],
    );
    await pool.query(
      "INSERT INTO trick_or_treat_tricks(event_key,guild_id,discord_id,chain,contract_address,token_id,transaction_hash,buyer_wallet,event_timestamp) " +
      "VALUES($1,$2,'99','ethereum','contract','1','transaction-01','buyer',clock_timestamp())",
      [live.sourceEventKey, guild],
    );
    await live.setup(guild, "real-owner");
    const originalBefore = await live.read(guild);
    assert.equal(originalBefore.snapshot.users.length, 1);
    assert.equal(originalBefore.snapshot.users[0].total, 2);

    await sandbox.setup(guild, owner.id);
    await assert.rejects(() => sandbox.setup(guild, "different-owner"), /ownership/i);
    const initial = await sandbox.read(guild);
    assert.equal(initial.event.config.testMode, true);
    assert.equal(initial.event.config.testOwnerId, owner.id);
    assert.equal(initial.event.config.prizesFinalized, true);
    assert.equal(initial.event.source_event_key, sandbox.sourceEventKey);
    assert.equal(initial.snapshot.users.length, 0); // Cannot see live claims.
    for (const [user, treats, tricks] of [[fake, 3, 2], [owner, 2, 1]])
      await pool.query(
        "INSERT INTO four_graves_test_tickets(event_key,guild_id,discord_id,treats,tricks) VALUES($1,$2,$3,$4,$5)",
        [sandbox.eventKey, guild, user.id, treats, tricks],
      );
    const s = await sandbox.read(guild);
    assert.equal(s.snapshot.users.length, 2);
    assert.equal(s.snapshot.users.reduce((n,u)=>n+u.total,0), 8);

    await sandbox.transition(guild, owner.id, "open");
    await sandbox.allocate(guild, fake, GRAVES[0].id, 5, "fake-ticket-01");
    await sandbox.allocate(guild, owner, GRAVES[1].id, 1, "owner-ticket-01");
    await sandbox.transition(guild, owner.id, "close");
    const finished = await sandbox.read(guild);
    assert.equal(finished.snapshot.entries.length, 6);
    assert.equal(finished.event.state, "CLOSED_COUNTDOWN");
    assert.ok(finished.event.snapshot_hash);
    assert.ok(finished.event.deadline);
    // The actual finale was neither opened nor allocated.
    const actualAfter = await live.read(guild);
    assert.equal(actualAfter.event.state, "SETUP");
    assert.equal(actualAfter.snapshot.entries.length, 0);
    assert.deepEqual(actualAfter.snapshot.users, originalBefore.snapshot.users);
    const treats = (await pool.query("SELECT COUNT(*)::int AS n FROM trick_or_treat_treats")).rows[0].n;
    const tricks = (await pool.query("SELECT COUNT(*)::int AS n FROM trick_or_treat_tricks")).rows[0].n;
    assert.equal(treats, 1);
    assert.equal(tricks, 1);
    console.log("PASS sandbox uses synthetic tickets; live records, event state and claims unchanged");
  } finally {
    if (pool) await pool.end();
    await admin.query("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
    await admin.end();
  }
}
main().catch(e => { console.error(e); process.exitCode = 1; });
