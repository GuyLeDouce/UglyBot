"use strict";
const fs = require("fs");
const path = require("path");
const { randomUUID, randomInt } = require("crypto");
const { prizeSlots } = require("./fourGravesCore");
const { pickNextTicket } = require("./fourGravesDraw");
const {
  GRAVES,
  DEFAULT_CONFIG,
  PANEL_IMAGE_URL,
  PANEL_IMAGE_VERSION,
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
      testMode = false,
    } = {},
  ) {
    if (testMode && !eventKey.startsWith("four-graves-private-test-")) throw Error("Invalid sandbox key.");
    Object.assign(this, { pool, eventKey, sourceEventKey, testMode });
  }
  async migrate() {
    await this.pool.query(
      fs.readFileSync(path.join(__dirname, "fourGravesSchema.sql"), "utf8"),
    );
    // Only convert the exact retired Doruzu pool in events still accepting
    // allocations. A closed grave's prize configuration or recorded winners
    // must never be silently rewritten after its entries are frozen.
    // This idempotent update runs on startup without requiring an admin to
    // reopen /fourgraves admin, and audits each changed event atomically.
    await this.pool.query(
      `WITH changed AS (
         UPDATE four_graves_events
         SET config=jsonb_set(
           config, '{mysteryByGrave,cletus_von_wrinkles}', $3::jsonb, false
         )
         WHERE event_key=$1 AND source_event_key=$2
           AND closed_at IS NULL AND state IN ('SETUP','OPEN')
           AND config #> '{mysteryByGrave,cletus_von_wrinkles}' = $4::jsonb
         RETURNING id
       )
       INSERT INTO four_graves_audit(event_id,actor_id,action,detail)
       SELECT id,'SYSTEM','cletus-bonus-prize-update',$5::jsonb FROM changed`,
      [
        this.eventKey,
        this.sourceEventKey,
        JSON.stringify(DEFAULT_CONFIG.mysteryByGrave.cletus_von_wrinkles),
        JSON.stringify([{ name: "Doruzu (ApeChain)", quantity: 2 }]),
        JSON.stringify({
          old: [{ name: "Doruzu (ApeChain)", quantity: 2 }],
          new: DEFAULT_CONFIG.mysteryByGrave.cletus_von_wrinkles,
          reason: "Approved Cletus bonus-prize replacement",
        }),
      ],
    );
    // Rebrand existing panel artwork exactly once, even when an event is
    // already CLOSED. Only visual JSON fields change; ticket snapshots,
    // winner records and prize lists remain untouched.
    // Later explicit /fourgraves configure image overrides survive restarts.
    await this.pool.query(
      `WITH changed AS (
         UPDATE four_graves_events
         SET config=jsonb_set(
           jsonb_set(config, '{image}', to_jsonb($3::text), true),
           '{panelArtworkVersion}', to_jsonb($4::text), true
         )
         WHERE event_key=$1 AND source_event_key=$2
           AND (config->>'panelArtworkVersion') IS DISTINCT FROM $4
         RETURNING id
       )
       INSERT INTO four_graves_audit(event_id,actor_id,action,detail)
       SELECT id,'SYSTEM','grave-panel-artwork-update',$5::jsonb FROM changed`,
      [
        this.eventKey,
        this.sourceEventKey,
        PANEL_IMAGE_URL,
        PANEL_IMAGE_VERSION,
        JSON.stringify({
          artwork: PANEL_IMAGE_URL,
          version: PANEL_IMAGE_VERSION,
          reason: "Approved Four Graves panel artwork refresh",
        }),
      ],
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
      const config = this.testMode
        ? { ...DEFAULT_CONFIG, testMode: true, testOwnerId: String(actor), prizesFinalized: true }
        : DEFAULT_CONFIG;
      await c.query(
        "INSERT INTO four_graves_events(event_key,guild_id,source_event_key,config) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",
        [
          this.eventKey,
          guild,
          this.sourceEventKey,
          JSON.stringify(config),
        ],
      );
      const e = await this.event(c, guild, true);
      if (!e.closed_at && !e.config?.mysteryByGrave) {
        // Upgrade an existing, not-yet-closed event to the confirmed four pools.
        // Do not alter any frozen/closed event snapshot or prize configuration.
        await c.query("UPDATE four_graves_events SET config=$2 WHERE id=$1",
          [e.id, JSON.stringify({ ...e.config, mysteryByGrave: DEFAULT_CONFIG.mysteryByGrave,
            prizesFinalized: true })]);
      }
      if (this.testMode && (e.config?.testMode !== true || e.config.testOwnerId !== String(actor) || e.source_event_key !== this.sourceEventKey))
        throw Error("Test ownership or source mismatch: refusing to operate.");
      for (const g of GRAVES)
        await c.query(
          "INSERT INTO four_graves_graves(event_id,grave_id,fixed_game) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
          [e.id, g.id, g.game],
        );
      await this.audit(c, e.id, actor, "setup");
      return this.event(c, guild);
    });
  }
  async audit(c, id, actor, action, detail = {}) {
    await c.query(
      "INSERT INTO four_graves_audit(event_id,actor_id,action,detail) VALUES($1,$2,$3,$4)",
      [id, actor, action, JSON.stringify(detail)],
    );
  }
  async earned(c, e) {
    if (this.testMode) return (await c.query("SELECT discord_id,treats,tricks FROM four_graves_test_tickets WHERE event_key=$1 AND guild_id=$2 ORDER BY discord_id", [this.eventKey,e.guild_id])).rows;
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
        awards: (
          await c.query("SELECT * FROM four_graves_awards WHERE event_id=$1 ORDER BY grave_id,prize_index", [e.id])
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
          throw Error("Finalize the four prize pools before closing.");
        for (const grave of GRAVES) prizeSlots(e.config, grave.id);
        // SHARE excludes concurrent claim INSERTs while taking the exact close snapshot. Existing claim code is unchanged.
        if (!this.testMode) await c.query(
          "LOCK TABLE trick_or_treat_treats,trick_or_treat_tricks IN SHARE MODE",
        );
        // Atomic, restart-safe distribution of all remaining earned tickets.
        // The event row is locked by this transaction.
        const before = await this.snapshot(c, e, identities);
        for (const u of before.users) {
          const remaining = u.total - u.allocated;
          if (!remaining) continue;
          const counts = Array(GRAVES.length).fill(0);
          for (let n = 0; n < remaining; n++) counts[randomInt(GRAVES.length)]++;
          for (let gi = 0; gi < counts.length; gi++) {
            if (!counts[gi]) continue;
            const grave = GRAVES[gi];
            await c.query(
              "INSERT INTO four_graves_allocations(event_id,grave_id,discord_id,username,display_name,quantity,idempotency_key,source_counts) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
              [e.id, grave.id, u.discord_id, u.username, u.display_name, counts[gi],
               "auto-close:" + e.id + ":" + u.discord_id + ":" + grave.id,
               JSON.stringify({ automated: true, remainingBeforeClose: remaining })],
            );
          }
          await this.audit(c, e.id, "AUTO_CLOSE", "auto-distribute", {
            discord_id: u.discord_id, quantity: remaining, grave_counts: counts,
          });
        }
        const s = await this.snapshot(c, e, identities);
        if (s.users.some(u => u.total !== u.allocated))
          throw Error("Ticket reconciliation failed; close rolled back.");
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
        "UPDATE four_graves_graves SET decision=$3,next_reveal_at=clock_timestamp(),next_draw_at=clock_timestamp()+interval '2 minutes' WHERE event_id=$1 AND grave_id=$2 AND decision IS NULL RETURNING *",
        [e.id, grave, JSON.stringify(decision)],
      );
      if (!r.rowCount)
        throw Error("TOO LATE. Someone already sealed this grave's fate.");
      await this.audit(c, e.id, user.id, "decision", { grave, choice });
      return r.rows[0];
    });
  }
  // Draw precisely one prize at a time, using the immutable close snapshot.
  // A pending, unannounced draw is always returned first, so a reconnect never rerolls it.
  async nextPrize(guild, graveId) {
    if (!GRAVES.some(g=>g.id===graveId)) throw Error("Unknown grave.");
    return this.transaction(async c => {
      const e = await this.event(c, guild, true);
      if (!e.closed_at || !e.snapshot || !e.snapshot_hash ||
          hash(e.snapshot) !== e.snapshot_hash)
        throw Error("Frozen ticket snapshot integrity check failed.");
      const g = (await c.query(
        "SELECT * FROM four_graves_graves WHERE event_id=$1 AND grave_id=$2 FOR UPDATE",
        [e.id,graveId],
      )).rows[0];
      if (!g || !g.decision) return null;
      const prior = (await c.query(
        "SELECT * FROM four_graves_awards WHERE event_id=$1 AND grave_id=$2 ORDER BY prize_index",
        [e.id,graveId],
      )).rows;
      const outstanding = prior.find(a=>!a.announced_at);
      if (outstanding) return outstanding;
      const slots = prizeSlots(e.config, graveId);
      if (prior.length >= slots.length || g.draw_completed_at || !g.next_draw_at ||
          !g.reveal_stage || g.reveal_stage < 3 ||
          new Date(g.next_draw_at) > new Date(e.db_now)) return null;
      const entries = e.snapshot.entries.filter(x=>x.grave_id===graveId);
      if (!entries.length) return null;
      const chosen = pickNextTicket(entries,prior);
      const prize = slots[prior.length];
      const inserted = (await c.query(
        "INSERT INTO four_graves_awards(event_id,grave_id,prize_index,prize_name,prize_unit,winner_discord_id,winner_display_name,ticket_entry_id,pass_number) " +
        "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *",
        [e.id,graveId,prior.length+1,prize.name,prize.unit,
          chosen.ticket.discord_id,chosen.ticket.display_name,
          chosen.ticket.id,chosen.pass_number],
      )).rows[0];
      await this.audit(c,e.id,"AUTO_DRAW","prize-drawn",{
        graveId,prizeIndex:inserted.prize_index,ticket:inserted.ticket_entry_id,
        pass:inserted.pass_number,
      });
      return inserted;
    });
  }
  async markPrizeAnnounced(guild, graveId, prizeIndex, messageId) {
    return this.transaction(async c => {
      const e = await this.event(c, guild, true);
      const g = (await c.query(
        "SELECT * FROM four_graves_graves WHERE event_id=$1 AND grave_id=$2 FOR UPDATE",
        [e.id,graveId],
      )).rows[0];
      const award = (await c.query(
        "SELECT * FROM four_graves_awards WHERE event_id=$1 AND grave_id=$2 AND prize_index=$3 FOR UPDATE",
        [e.id,graveId,prizeIndex],
      )).rows[0];
      if (!g || !g.decision || !award) throw Error("Unrecognized grave prize.");
      if (award.announced_at) {
        if (award.message_id !== messageId) throw Error("Prize announcement ID mismatch.");
        return award;
      }
      const updated = (await c.query(
        "UPDATE four_graves_awards SET message_id=$4,announced_at=clock_timestamp() " +
        "WHERE event_id=$1 AND grave_id=$2 AND prize_index=$3 RETURNING *",
        [e.id,graveId,prizeIndex,messageId],
      )).rows[0];
      const complete = prizeIndex >= prizeSlots(e.config,graveId).length;
      if (complete)
        await c.query("UPDATE four_graves_graves SET next_draw_at=NULL,draw_completed_at=clock_timestamp() WHERE event_id=$1 AND grave_id=$2", [e.id,graveId]);
      else
        await c.query("UPDATE four_graves_graves SET next_draw_at=clock_timestamp()+interval '15 seconds' WHERE event_id=$1 AND grave_id=$2", [e.id,graveId]);
      await this.audit(c,e.id,"AUTO_DRAW","prize-announced",{graveId,prizeIndex,messageId});
      return updated;
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
