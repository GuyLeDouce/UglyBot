"use strict";
const assert = require("assert/strict");
const {
  GRAVES,
  DEFAULT_CONFIG,
  quantity,
  buildSnapshot,
  exportFiles,
  zip,
  privateArchives,
  csvRecords,
} = require("../modules/fourGravesCore");
const { publicPanel, decisionPanel } = require("../modules/fourGravesDiscord");
const { validatePatch, buildSlashCommand } = require("../modules/fourGraves");
let tests = 0;
function test(name, fn) {
  fn();
  tests++;
  console.log("PASS", name);
}
const allocations = [
  {
    id: "1",
    grave_id: GRAVES[0].id,
    discord_id: "10",
    quantity: 5,
    display_name: "Guy",
    username: "guy",
    created_at: "2026-10-30T12:00:00Z",
  },
  {
    id: "2",
    grave_id: GRAVES[1].id,
    discord_id: "10",
    quantity: 2,
    display_name: "Guy",
    username: "guy",
    created_at: "2026-10-30T12:00:01Z",
  },
  {
    id: "3",
    grave_id: GRAVES[0].id,
    discord_id: "20",
    quantity: 2,
    display_name: "Guy",
    username: "other",
    created_at: "2026-10-30T12:00:02Z",
  },
  {
    id: "4",
    grave_id: GRAVES[0].id,
    discord_id: "30",
    quantity: 1,
    display_name: "Guy(1)",
    username: "third",
    created_at: "2026-10-30T12:00:03Z",
  },
];
const earned = [
  { discord_id: "10", treats: 5, tricks: 4 },
  { discord_id: "20", treats: 2, tricks: 1 },
  { discord_id: "30", treats: 0, tricks: 1 },
  { discord_id: "40", treats: 1, tricks: 0 },
];
const snapshot = buildSnapshot(earned, allocations),
  event = {
    id: "1",
    event_key: "test",
    state: "OPEN",
    config: structuredClone(DEFAULT_CONFIG),
    generation: 1,
    deadline: "2026-10-30T12:15:00Z",
    snapshot,
  };
for (const value of [
  "0",
  "-1",
  "1.5",
  "1e2",
  "+2",
  " 2",
  "2x",
  "NaN",
  "2147483648",
  "",
])
  test(`reject quantity ${JSON.stringify(value)}`, () =>
    assert.throws(() => quantity(value)));
test("positive integer", () => assert.equal(quantity("17"), 17));
test("all fixed games", () =>
  assert.deepEqual(
    GRAVES.map((g) => g.game),
    ["Wheel of Names", "Squigwreck", "Reverse Ugly City", "Duck Race"],
  ));
test("ten confirmed prize slots", () =>
  assert.equal(
    DEFAULT_CONFIG.prizes.reduce((n, p) => n + p.quantity, 0),
    10,
  ));
test("mystery not invented", () =>
  assert.deepEqual(DEFAULT_CONFIG.mystery, []));
test("each ticket is an entry", () =>
  assert.equal(snapshot.entries.length, 10));
test("numbering per grave", () =>
  assert.deepEqual(
    snapshot.entries
      .filter((x) => x.grave_id === GRAVES[1].id)
      .map((x) => x.label),
    ["Guy", "Guy(1)"],
  ));
test("numbering starts without suffix", () =>
  assert.deepEqual(
    snapshot.entries.slice(0, 5).map((x) => x.label),
    ["Guy", "Guy(1)", "Guy(2)", "Guy(3)", "Guy(4)"],
  ));
test("collisions including suffix-like real names", () =>
  assert.equal(
    new Set(
      snapshot.entries
        .filter((e) => e.grave_id === GRAVES[0].id)
        .map((e) => e.label),
    ).size,
    8,
  ));
test("zero-allocation earners retained", () =>
  assert.equal(snapshot.users.find((u) => u.discord_id === "40").allocated, 0));
test("unallocated excluded", () =>
  assert.equal(
    snapshot.users.reduce((n, u) => n + u.total - u.allocated, 0),
    4,
  ));
test("deterministic allocation ordering", () =>
  assert.deepEqual(
    buildSnapshot([...earned].reverse(), [...allocations].reverse()),
    snapshot,
  ));
test("missing source discrepancy", () =>
  assert.throws(() => buildSnapshot([], allocations), /discrepancy/));
test("overspent discrepancy", () =>
  assert.throws(
    () =>
      buildSnapshot(
        [{ discord_id: "10", treats: 0, tricks: 0 }],
        [allocations[0]],
      ),
    /discrepancy/,
  ));
test("Unicode preserved", () =>
  assert.equal(
    buildSnapshot(
      [{ discord_id: "1", treats: 1, tricks: 0 }],
      [
        {
          ...allocations[0],
          discord_id: "1",
          quantity: 1,
          display_name: "🍬 Émilie",
        },
      ],
    ).entries[0].label,
    "🍬 Émilie",
  ));
test("formula injection neutralized", () => {
  const s = buildSnapshot(
    [{ discord_id: "1", treats: 1, tricks: 0 }],
    [
      {
        ...allocations[0],
        discord_id: "1",
        quantity: 1,
        display_name: '=IMPORTXML("x")',
      },
    ],
  );
  assert.match(
    exportFiles(event, s)["grave_1_barry_buckets.csv"],
    /"'=IMPORTXML/,
  );
});
const files = exportFiles(event, snapshot, "all", "all", "fixed");
test("six requested export files", () =>
  assert.equal(Object.keys(files).length, 6));
test("only one contestant column", () =>
  assert.equal(
    files["grave_1_barry_buckets.csv"].split("\r\n")[0],
    '"contestant"',
  ));
test("grave totals reconcile", () =>
  assert.equal(
    Object.entries(files)
      .filter(([k]) => k.startsWith("grave_"))
      .reduce((n, [, v]) => n + v.split("\r\n").length - 2, 0),
    10,
  ));
test("unchanged exports deterministic", () =>
  assert.deepEqual(exportFiles(event, snapshot, "all", "all", "fixed"), files));
test("public panel hides secrets", () => {
  const text = JSON.stringify(publicPanel(event));
  for (const g of GRAVES) assert.ok(!text.includes(g.game));
  assert.ok(!text.includes("DollzOnApe"));
});
test("setup buttons disabled", () =>
  assert.ok(
    publicPanel({ ...event, state: "SETUP" }).components[0].components.every(
      (b) => b.disabled,
    ),
  ));
test("countdown buttons disabled", () =>
  assert.ok(
    decisionPanel(
      { ...event, state: "CLOSED_COUNTDOWN" },
      { grave_id: GRAVES[0].id, reveal_stage: 0 },
    ).components[0].components.every((b) => b.disabled),
  ));
test("active buttons enabled for populated grave", () =>
  assert.ok(
    decisionPanel(
      { ...event, state: "BUTTONS_ACTIVE" },
      { grave_id: GRAVES[0].id, reveal_stage: 0 },
    ).components[0].components.every((b) => !b.disabled),
  ));
test("empty grave buttons disabled", () =>
  assert.ok(
    decisionPanel(
      { ...event, state: "BUTTONS_ACTIVE" },
      { grave_id: GRAVES[3].id, reveal_stage: 0 },
    ).components[0].components.every((b) => b.disabled),
  ));
test("both decisions have same outcome", () => {
  const panel = (choice) =>
    decisionPanel(
      { ...event, state: "BUTTONS_ACTIVE" },
      {
        grave_id: GRAVES[0].id,
        reveal_stage: 3,
        decision: { user_id: "10", choice },
      },
    ).embeds[0].description;
  assert.equal(
    panel("TRICK").replace("TRICK", "CHOICE"),
    panel("TREAT").replace("TREAT", "CHOICE"),
  );
});
test("decision identifies presser", () =>
  assert.match(
    decisionPanel(event, {
      grave_id: GRAVES[0].id,
      reveal_stage: 1,
      decision: { user_id: "10", choice: "TRICK" },
    }).embeds[0].description,
    /<@10>/,
  ));
test("rules cannot change fixed games", () =>
  assert.throws(() => validatePatch({ game: "wrong" })));
test("mystery update invalidates finalization", () =>
  assert.equal(validatePatch({ mystery: [] }).prizesFinalized, false));
test("invalid mystery quantities rejected", () =>
  assert.throws(() =>
    validatePatch({ mystery: [{ name: "NFT", quantity: 0 }] }),
  ));
test("slash command serializes with Discord builders", () =>
  assert.equal(buildSlashCommand().toJSON().name, "fourgraves"));
test("ZIP signatures", () => {
  const z = zip(files);
  assert.equal(z.readUInt32LE(0), 0x04034b50);
  assert.equal(z.readUInt32LE(z.length - 22), 0x06054b50);
  require("fs").writeFileSync("/tmp/four-graves-test.zip", z);
});
test("private multipart exports respect limits", () => {
  const parts = privateArchives(
    {
      "large.csv":
        '"contestant"\r\n' +
        Array.from({ length: 100 }, (_, i) => '"Guy ' + i + '"\r\n').join(""),
    },
    700,
  );
  assert.ok(parts.length > 1);
  assert.ok(parts.every((p) => p.attachment.length <= 700));
});
test("multipart splitting preserves quoted multiline records", () =>
  assert.equal(csvRecords('"name"\r\n"Guy\nJunior"\r\n').length, 2));
console.log(`${tests} Four Graves logic tests passed.`);
