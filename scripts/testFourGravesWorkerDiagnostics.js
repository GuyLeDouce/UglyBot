"use strict";
const assert = require("node:assert/strict");
const { FourGravesDiscord } = require("../modules/fourGravesDiscord");

async function main() {
  const operations = [];
  const client = {
    query: async (sql, params) => {
      if (sql.includes("pg_try_advisory_lock"))
        return { rows: [{locked:true}] };
      if (sql.includes("SELECT * FROM four_graves_events"))
        return {rows:[{id:params[0], config:{testMode:true, category:"test-cat"}, panel_channel_id:"test-control"}]};
      return {rows:[],rowCount:1};
    },
    release: () => {},
  };
  const pool = {
    query: async sql => sql.includes("SELECT id FROM four_graves_events")
      ? {rows:[{id:1},{id:3}]} : {rows:[],rowCount:1},
    connect: async () => client,
  };
  const store = {
    testMode: true,
    eventKey: "four-graves-private-test-logging",
    pool,
    operation: async (id, name, error) => operations.push({id,name,error}),
  };
  const worker = new FourGravesDiscord(store,{enabled:()=>true});
  worker.reconcile = async event => {
    const err = new Error("Cannot fetch members at postgresql://dbuser:secret@private.host/db; token=mysecret");
    err.code = "50013";
    throw err;
  };
  const warnings=[];
  const original = console.warn;
  console.warn = (...args) => warnings.push(args.join(" "));
  try {
    await worker.tick();
    assert.equal(operations.length,2);
    assert.ok(operations.every(x=>x.name==="worker"));
    assert.deepEqual(operations.map(x=>x.id),[1,3]);
    assert.ok(operations.every(x=>x.error.includes("[50013]")));
    assert.ok(warnings.some(x=>x.includes("event 1")));
    assert.ok(warnings.some(x=>x.includes("event 3")));
    assert.ok(warnings.every(x=>!x.includes("secret")));
    assert.ok(warnings.every(x=>!x.includes("mysecret")));
    assert.ok(warnings.every(x=>x.includes("[FourGraves:TEST]")));
    await worker.tick();
    // Database diagnostics are refreshed, but identical log lines are throttled.
    assert.equal(operations.length,4);
    assert.equal(warnings.length,2);
    worker.reconcile=async()=>{};
    await worker.tick();
    assert.ok(operations.filter(x=>x.error===undefined).length===2);
  } finally {
    console.warn=original;
  }
  console.log("PASS event-specific diagnostic messages, secret redaction and rate-limited worker retry logs");
}
main().catch(e=>{console.error(e);process.exitCode=1;});
