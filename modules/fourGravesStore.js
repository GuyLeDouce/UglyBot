"use strict";
const fs = require("fs");
const path = require("path");
const { randomUUID } = require("crypto");
const {
  GRAVES,
  DEFAULT_CONFIG,
  quantity,
  buildSnapshot,
  hash,
} = require("./fourGravesCore");
class FourGravesStore {
  constructor(
    pool,
    {
      eventKey = "squigs-four-graves-2026",
      sourceEventKey = "squigs-trick-or-treat-2026",
    } = {},
  ) {
    Object.assign(this, { pool, eventKey, sourceEventKey });
  }
  async migrate() {
    await this.pool.query(
      fs.readFileSync(path.join(__dirname, "fourGravesSchema.sql"), "utf8"),
    );
  }
  async transaction(fn) {
    const c = await this.pool.connect();
    try {
      await c.query("BEGIN");
      const value = await fn(c);
      await c.query("COMMIT");
      return value;
    } catch (e) {
      await c.query("ROLLBACK");
      throw e;
    } finally {
      c.release();
    }
  }
  async event(c, guild, lock = false) {
    const e = (
      await c.query(
        `SELECT *,clock_timestamp() AS db_now FROM four_graves_events WHERE event_key=$1 AND guild_id=$2${lock ? " FOR UPDATE" : ""}`,
        [this.eventKey, guild],
      )
    ).rows[0];
    if (!e) throw Error("An administrator must SETUP EVENT first.");
    return e;
  }
  async setup(guild, actor) {
    return this.transaction(async (c) => {
      await c.query(
        "INSERT INTO four_graves_events(event_key,guild_id,source_event_key,config) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",
        [
          this.eventKey,
          guild,
          this.sourceEventKey,
          JSON.stringify(DEFAULT_CONFIG),
        ],
      );
      const e = await this.event(c, guild, true);
      for (const g of GRAVES)
        await c.query(
          "INSERT INTO four_graves_graves(event_id,grave_id,fixed_game) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
          [e.id, g.id, g.game],
        );
      await this.audit(c, e.id, actor, "setup");
      return e;
    });
  }
  async audit(c, id, actor, action, detail = {}) {
    await c.query(
      "INSERT INTO four_graves_audit(event_id,actor_id,action,detail) VALUES($1,$2,$3,$4)",
      [id, actor, action, JSON.stringify(detail)],
    );
  }
  async earned(c, e) {
    return (
      await c.query(
        `WITH claims AS (
 SELECT discord_id,'treat' AS kind FROM trick_or_treat_treats WHERE event_key=$1 AND guild_id=$2
 UNION ALL SELECT discord_id,'trick' AS kind FROM trick_or_treat_tricks WHERE event_key=$1 AND guild_id=$2)
 SELECT discord_id,COUNT(*) FILTER(WHERE kind='treat')::int AS treats,COUNT(*) FILTER(WHERE kind='trick')::int AS tricks FROM claims GROUP BY discord_id ORDER BY discord_id`,
        [e.source_event_key, e.guild_id],
      )
    ).rows;
  }
  async allocations(c, e) {
    return (
      await c.query(
        "SELECT * FROM four_graves_allocations WHERE event_id=$1 ORDER BY id",
        [e.id],
      )
    ).rows;
  }
  async snapshot(c, e, identities = {}) {
    if (e.snapshot) return e.snapshot;
    return buildSnapshot(
      await this.earned(c, e),
      await this.allocations(c, e),
      identities,
    );
  }
  async read(guild) {
    return this.transaction(async (c) => {
      const e = await this.event(c, guild, true);
      return {
        event: e,
        snapshot: await this.snapshot(c, e),
        graves: (
          await c.query(
            "SELECT * FROM four_graves_graves WHERE event_id=$1 ORDER BY grave_id",
            [e.id],
          )
        ).rows,
        operations: (
          await c.query(
            "SELECT * FROM four_graves_operations WHERE event_id=$1",
            [e.id],
          )
        ).rows,
      };
    });
  }
  async allocate(guild, user, grave, amount, key) {
    quantity(amount);
    if (!GRAVES.some((g) => g.id === grave)) throw Error("Unknown grave.");
    return this.transaction(async (c) => {
      const e = await this.event(c, guild, true);
      const prior = (
        await c.query(
          "SELECT * FROM four_graves_allocations WHERE idempotency_key=$1",
          [key],
        )
      ).rows[0];
      if (prior) {
        if (
          prior.event_id !== e.id ||
          prior.discord_id !== user.id ||
          prior.grave_id !== grave ||
          prior.quantity !== amount
        )
          throw Error("Confirmation does not match its original allocation.");
        return prior;
      }
      if (e.state !== "OPEN")
        throw Error("THE GRAVES ARE SEALED. No more allocations.");
      const s = await this.snapshot(c, e),
        u = s.users.find((u) => u.discord_id === user.id);
      if (!u || amount > u.total - u.allocated)
        throw Error(
          `NOT ENOUGH TICKETS. You only have ${u ? u.total - u.allocated : 0} left. Even the dead can count.`,
        );
      const a = (
        await c.query(
          "INSERT INTO four_graves_allocations(event_id,grave_id,discord_id,username,display_name,quantity,idempotency_key,source_counts) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *",
          [
            e.id,
            grave,
            user.id,
            user.username,
            user.display_name,
            amount,
            key,
            JSON.stringify({
              treats: u.treats,
              tricks: u.tricks,
              previouslyAllocated: u.allocated,
            }),
          ],
        )
      ).rows[0];
      await this.audit(c, e.id, user.id, "allocate", { allocationId: a.id });
      return a;
    });
  }
  async configure(guild, actor, patch) {
    return this.transaction(async (c) => {
      const e = await this.event(c, guild, true);
      if (e.closed_at) throw Error("Configuration is frozen after closing.");
      const cfg = { ...e.config, ...patch };
      await c.query("UPDATE four_graves_events SET config=$2 WHERE id=$1", [
        e.id,
        JSON.stringify(cfg),
      ]);
      await this.audit(c, e.id, actor, "configure", {
        fields: Object.keys(patch),
      });
    });
  }
  async transition(guild, actor, action, identities = {}) {
    return this.transaction(async (c) => {
      const e = await this.event(c, guild, true);
      if (action === "open") {
        if (e.state === "OPEN") return e;
        if (e.state !== "SETUP")
          throw Error("Closed graves cannot be reopened.");
        await c.query(
          "UPDATE four_graves_events SET state='OPEN',opened_at=clock_timestamp() WHERE id=$1",
          [e.id],
        );
      } else if (action === "close") {
        if (e.closed_at) return e;
        if (e.state !== "OPEN") throw Error("Open the graves before closing.");
        if (!e.config.prizesFinalized)
          throw Error(
            "Finalize the identical prize pool (including an explicit empty mystery list) before closing.",
          );
        // SHARE excludes concurrent claim INSERTs while taking the exact close snapshot. Existing claim code is unchanged.
        await c.query(
          "LOCK TABLE trick_or_treat_treats,trick_or_treat_tricks IN SHARE MODE",
        );
        const s = await this.snapshot(c, e, identities);
        await c.query(
          "UPDATE four_graves_events SET state='CLOSED_COUNTDOWN',closed_at=clock_timestamp(),deadline=clock_timestamp()+interval '15 minutes',generation=generation+1,snapshot=$2,snapshot_hash=$3,snapshot_id=$4 WHERE id=$1",
          [e.id, JSON.stringify(s), hash(s), randomUUID()],
        );
      } else if (action === "restart") {
        if (!["CLOSED_COUNTDOWN", "BUTTONS_ACTIVE"].includes(e.state))
          throw Error("No countdown to restart.");
        if (
          (
            await c.query(
              "SELECT 1 FROM four_graves_graves WHERE event_id=$1 AND decision IS NOT NULL",
              [e.id],
            )
          ).rowCount
        )
          throw Error("Countdown cannot restart after any recorded decision.");
        await c.query(
          "UPDATE four_graves_events SET state='CLOSED_COUNTDOWN',deadline=clock_timestamp()+interval '15 minutes',generation=generation+1 WHERE id=$1",
          [e.id],
        );
      } else throw Error("Unknown transition.");
      await this.audit(c, e.id, actor, action);
      return this.event(c, guild);
    });
  }
  async decide(guild, user, grave, choice, generation) {
    if (!["TRICK", "TREAT"].includes(choice)) throw Error("Invalid choice.");
    return this.transaction(async (c) => {
      const e = await this.event(c, guild, true);
      if (
        !e.closed_at ||
        !["CLOSED_COUNTDOWN", "BUTTONS_ACTIVE"].includes(e.state) ||
        Number(generation) !== e.generation ||
        new Date(e.db_now) < new Date(e.deadline)
      )
        throw Error("These decision buttons are not active.");
      if (
        !e.snapshot.entries.some(
          (x) => x.discord_id === user.id && x.grave_id === grave,
        )
      )
        throw Error(
          "YOU DON'T BELONG HERE. You didn't bury any tickets in this grave.",
        );
      const decision = {
        user_id: user.id,
        display_name: user.display_name,
        choice,
        generation: e.generation,
        at: new Date(e.db_now).toISOString(),
      };
      const r = await c.query(
        "UPDATE four_graves_graves SET decision=$3,next_reveal_at=clock_timestamp() WHERE event_id=$1 AND grave_id=$2 AND decision IS NULL RETURNING *",
        [e.id, grave, JSON.stringify(decision)],
      );
      if (!r.rowCount)
        throw Error("TOO LATE. Someone already sealed this grave's fate.");
      await this.audit(c, e.id, user.id, "decision", { grave, choice });
      return r.rows[0];
    });
  }
  async operation(id, key, error = null) {
    await this.pool.query(
      `INSERT INTO four_graves_operations(event_id,operation_key,attempts,last_error,completed_at) VALUES($1,$2,1,$3,CASE WHEN $3::text IS NULL THEN clock_timestamp() END) ON CONFLICT(event_id,operation_key) DO UPDATE SET attempts=four_graves_operations.attempts+1,last_error=EXCLUDED.last_error,completed_at=EXCLUDED.completed_at`,
      [id, key, error],
    );
  }
}
module.exports = { FourGravesStore };
