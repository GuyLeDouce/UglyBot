"use strict";
const crypto = require("crypto");
const GRAVES = Object.freeze([
  {
    id: "barry_buckets",
    name: "Barry Buckets",
    game: "Wheel of Names",
    rules:
      "Every ticket is one name. The administrator spins the contestant wheel and a separate prize wheel, removing each winning entry for the current pass. Reset the full original contestant pool when necessary.",
  },
  {
    id: "barbara_bowkins",
    name: "Barbara Bowkins",
    game: "Squigwreck",
    rules:
      "Every ticket is a separate racer. The administrator imports entries into Squig Survival: Squigwreck. First place receives first prize-wheel position, followed by finishing order. Reset the full original pool for another manual pass if prizes remain.",
  },
  {
    id: "cletus_von_wrinkles",
    name: "Cletus Von Wrinkles",
    game: "Reverse Ugly City",
    rules:
      "Every ticket is a separate survivor in Squig Survival: Rise of Ugly City. LAST place receives first prize-wheel position, then second-last, continuing toward first. Reset the full original pool for another manual pass if prizes remain.",
  },
  {
    id: "scuba_steve_3000",
    name: "Scuba Steve 3000",
    game: "Duck Race",
    rules:
      "Every ticket is a separate duck. The administrator records the external race. First place receives first prize-wheel position, followed by finishing order. Reset the full original pool for another manual pass if prizes remain.",
  },
]);
const DEFAULT_CONFIG = {
  names: GRAVES.map((g) => g.name),
  rules: GRAVES.map((g) => g.rules),
  image: null,
  category: null,
  prizes: [
    { name: "DollzOnApe", quantity: 1 },
    { name: "Squigs Reloaded", quantity: 3 },
    { name: "Ugly Monster", quantity: 1 },
    { name: "5,000 $CHARM", quantity: 5 },
  ],
  mystery: [],
  prizesFinalized: false,
  revealDelaySeconds: 3,
};
function quantity(value) {
  if (
    !/^[1-9]\d*$/.test(String(value)) ||
    !Number.isSafeInteger(Number(value)) ||
    Number(value) > 2147483647
  )
    throw Error("Enter a positive whole-number quantity.");
  return Number(value);
}
function safeName(value) {
  return (
    String(value || "Unnamed")
      .normalize("NFC")
      .replace(/[\x00-\x1f\x7f\u202a-\u202e\u2066-\u2069]/g, " ")
      .trim()
      .slice(0, 80) || "Unnamed"
  );
}
function cell(value) {
  let s = String(value ?? "");
  if (/^[\s]*[=+@-]/u.test(s)) s = "'" + s;
  return '"' + s.replace(/"/g, '""') + '"';
}
function csv(headers, rows) {
  return (
    [headers, ...rows].map((r) => r.map(cell).join(",")).join("\r\n") + "\r\n"
  );
}
function buildSnapshot(earned, allocations, identities = {}) {
  const users = new Map(
    earned.map((u) => [
      u.discord_id,
      {
        ...u,
        username: identities[u.discord_id]?.username || u.discord_id,
        display_name: identities[u.discord_id]?.display_name || u.discord_id,
        graves: [0, 0, 0, 0],
      },
    ]),
  );
  const ordered = [...allocations].sort((a, b) =>
    BigInt(a.id) < BigInt(b.id) ? -1 : 1,
  );
  for (const a of ordered) {
    const u = users.get(a.discord_id);
    if (!u)
      throw Error("Ticket source discrepancy: allocated user has no earnings.");
    u.graves[GRAVES.findIndex((g) => g.id === a.grave_id)] += a.quantity;
    if (!identities[a.discord_id]) {
      u.display_name = a.display_name;
      u.username = a.username;
    }
  }
  const people = [...users.values()].sort((a, b) =>
    a.discord_id.localeCompare(b.discord_id),
  );
  for (const u of people) {
    u.total = Number(u.treats) + Number(u.tricks);
    u.allocated = u.graves.reduce((a, b) => a + b, 0);
    if (u.allocated > u.total)
      throw Error("Ticket source discrepancy: allocation exceeds earnings.");
  }
  const entries = [];
  for (const [gi, g] of GRAVES.entries()) {
    const used = new Set(),
      bases = new Set(),
      labels = new Map(),
      counts = new Map();
    for (const u of people.filter((u) => u.graves[gi])) {
      let base = safeName(u.display_name); // Reserve all generated labels, including suffix-like real names.
      const candidate = base;
      let n = 0;
      while (
        bases.has(base) ||
        Array.from({ length: u.graves[gi] }, (_, i) =>
          i ? `${base}(${i})` : base,
        ).some((x) => used.has(x))
      ) {
        base = `${candidate} [User ${u.discord_id}${n ? `-${n}` : ""}]`;
        n++;
      }
      bases.add(base);
      labels.set(u.discord_id, base);
      for (let i = 0; i < u.graves[gi]; i++)
        used.add(i ? `${base}(${i})` : base);
    }
    for (const a of ordered.filter((a) => a.grave_id === g.id))
      for (let i = 0; i < a.quantity; i++) {
        const n = counts.get(a.discord_id) || 0;
        counts.set(a.discord_id, n + 1);
        entries.push({
          id: `${a.id}:${i + 1}`,
          grave_id: g.id,
          discord_id: a.discord_id,
          display_name: users.get(a.discord_id).display_name,
          label: labels.get(a.discord_id) + (n ? `(${n})` : ""),
          number: n + 1,
          allocation_id: a.id,
          allocated_at: a.created_at,
          source_type: "",
        });
      }
  }
  return { users: people, entries };
}
function hash(snapshot) {
  return crypto
    .createHash("sha256")
    .update(JSON.stringify(snapshot))
    .digest("hex");
}
function exportFiles(
  event,
  snapshot,
  grave = "all",
  format = "all",
  now = new Date().toISOString(),
) {
  const files = {};
  if (["all", "contestants"].includes(format))
    GRAVES.forEach((g, i) => {
      if (grave === "all" || grave === g.id)
        files[`grave_${i + 1}_${g.id}.csv`] = csv(
          ["contestant"],
          snapshot.entries
            .filter((e) => e.grave_id === g.id)
            .map((e) => [e.label]),
        );
    });
  if (["all", "master"].includes(format))
    files["four_graves_master_allocations.csv"] = csv(
      [
        "discord_user_id",
        "discord_username",
        "discord_display_name",
        "total_treats",
        "total_tricks",
        "total_earned_tickets",
        ...GRAVES.map((g) => g.id + "_tickets"),
        "total_allocated",
        "remaining_unallocated",
      ],
      snapshot.users.map((u) => [
        u.discord_id,
        u.username,
        u.display_name,
        u.treats,
        u.tricks,
        u.total,
        ...u.graves,
        u.allocated,
        u.total - u.allocated,
      ]),
    );
  if (["all", "audit"].includes(format))
    files["four_graves_ticket_audit.csv"] = csv(
      [
        "event_key",
        "grave_identifier",
        "grave_display_name",
        "discord_user_id",
        "discord_display_name",
        "contestant",
        "ticket_entry_number",
        "source_type",
        "allocation_transaction_id",
        "allocation_timestamp",
        "snapshot_identifier",
        "export_generated_at",
        "ticket_entry_id",
      ],
      snapshot.entries
        .filter((e) => grave === "all" || grave === e.grave_id)
        .map((e) => [
          event.event_key,
          e.grave_id,
          event.config.names[GRAVES.findIndex((g) => g.id === e.grave_id)],
          e.discord_id,
          e.display_name,
          e.label,
          e.number,
          e.source_type,
          e.allocation_id,
          e.allocated_at,
          event.snapshot_id || "live",
          now,
          e.id,
        ]),
    );
  return files;
}
// Small standards-compliant stored ZIP writer: no dependency or external executable.
function zip(files) {
  const local = [],
    central = [];
  let offset = 0;
  for (const [name, value] of Object.entries(files)) {
    const data = Buffer.from(value),
      filename = Buffer.from(name);
    let crc = 0xffffffff;
    for (const b of data) {
      crc ^= b;
      for (let k = 0; k < 8; k++)
        crc = (crc >>> 1) ^ (crc & 1 ? 0xedb88320 : 0);
    }
    crc = (crc ^ 0xffffffff) >>> 0;
    const h = Buffer.alloc(30);
    h.writeUInt32LE(0x04034b50);
    h.writeUInt16LE(20, 4);
    h.writeUInt16LE(0x800, 6);
    h.writeUInt16LE(33, 12);
    h.writeUInt32LE(crc, 14);
    h.writeUInt32LE(data.length, 18);
    h.writeUInt32LE(data.length, 22);
    h.writeUInt16LE(filename.length, 26);
    local.push(h, filename, data);
    const c = Buffer.alloc(46);
    c.writeUInt32LE(0x02014b50);
    c.writeUInt16LE(20, 4);
    c.writeUInt16LE(20, 6);
    c.writeUInt16LE(0x800, 8);
    c.writeUInt16LE(33, 14);
    c.writeUInt32LE(crc, 16);
    c.writeUInt32LE(data.length, 20);
    c.writeUInt32LE(data.length, 24);
    c.writeUInt16LE(filename.length, 28);
    c.writeUInt32LE(offset, 42);
    central.push(c, filename);
    offset += h.length + filename.length + data.length;
  }
  const cd = Buffer.concat(central),
    end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50);
  end.writeUInt16LE(Object.keys(files).length, 8);
  end.writeUInt16LE(Object.keys(files).length, 10);
  end.writeUInt32LE(cd.length, 12);
  end.writeUInt32LE(offset, 16);
  return Buffer.concat([...local, cd, end]);
}
// Preserve quoted multiline CSV records when splitting oversized private exports.
function csvRecords(text) {
  const records = [];
  let start = 0,
    quoted = false;
  for (let i = 0; i < text.length; i++) {
    if (text[i] === '"') {
      if (quoted && text[i + 1] === '"') {
        i++;
        continue;
      }
      quoted = !quoted;
    }
    if (!quoted && text[i] === "\n") {
      records.push(text.slice(start, i + 1));
      start = i + 1;
    }
  }
  if (start < text.length) records.push(text.slice(start));
  return records;
}
function privateArchives(files, limit) {
  const complete = zip(files);
  if (complete.length <= limit)
    return [{ name: "four_graves_exports.zip", attachment: complete }];
  const result = [];
  for (const [name, contents] of Object.entries(files)) {
    const records = csvRecords(contents),
      header = records.shift();
    let current = header,
      part = 1;
    const emit = () => {
      const filename = name.replace(".csv", `_part_${part++}.csv`);
      result.push({
        name: filename.replace(".csv", ".zip"),
        attachment: zip({ [filename]: current }),
      });
      current = header;
    };
    for (const record of records) {
      if (Buffer.byteLength(header + record) + 512 > limit)
        throw Error(
          "A single CSV row exceeds the attachment limit. Contact an administrator.",
        );
      if (Buffer.byteLength(current + record) + 512 > limit) emit();
      current += record;
    }
    if (current !== header || !records.length) emit();
  }
  return result;
}
module.exports = {
  privateArchives,
  csvRecords,
  GRAVES,
  DEFAULT_CONFIG,
  quantity,
  safeName,
  csv,
  buildSnapshot,
  hash,
  exportFiles,
  zip,
};
