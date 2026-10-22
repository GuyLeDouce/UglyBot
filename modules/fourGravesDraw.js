"use strict";
const { randomInt, createHash } = require("node:crypto");
const { GRAVES, csv } = require("./fourGravesCore");

/**
 * Immutable raffle result: every individual buried ticket is one entry.
 * A selected ticket leaves the pool until every ticket has been picked once.
 * When there are fewer tickets than prize slots, start another fresh pass.
 * Holders with multiple tickets may win multiple awards.
 */
function drawPrizeWinners(snapshot, config, pick = randomInt) {
  if (!snapshot || !Array.isArray(snapshot.entries))
    throw Error("A frozen ticket snapshot is required.");
  if (!config || config.prizesFinalized !== true)
    throw Error("Finalize prizes before drawing.");
  const prizes = [...(config.prizes || []), ...(config.mystery || [])];
  if (!prizes.length) throw Error("There are no configured prizes.");
  let totalSlots = 0;
  for (const prize of prizes) {
    if (typeof prize.name !== "string" || !prize.name.trim() ||
        !Number.isSafeInteger(prize.quantity) || prize.quantity < 1 ||
        prize.quantity > 1000)
      throw Error("Invalid finalized prize pool.");
    totalSlots += prize.quantity;
  }
  if (totalSlots > 2000)
    throw Error("Prize configuration exceeds the supported draw size.");
  const output = [];
  for (const g of GRAVES) {
    const tickets = snapshot.entries.filter(x => x.grave_id === g.id);
    let pool = [];
    for (const prize of prizes) {
      for (let unit = 1; unit <= prize.quantity; unit++) {
        let winner = null;
        if (tickets.length) {
          if (!pool.length) pool = tickets.slice();
          const index = pick(pool.length);
          if (!Number.isInteger(index) || index < 0 || index >= pool.length)
            throw Error("Random picker returned an invalid ticket index.");
          const t = pool[index];
          pool[index] = pool[pool.length - 1];
          pool.pop();
          winner = {
            ticket_entry_id: t.id,
            discord_id: t.discord_id,
            display_name: t.display_name,
            allocation_id: t.allocation_id,
          };
        }
        output.push({
          grave_id: g.id,
          prize_name: prize.name,
          prize_number: unit,
          winner,
        });
      }
    }
  }
  return output;
}
function winnersHash(records) {
  return createHash("sha256").update(JSON.stringify(records)).digest("hex");
}
function winnersCsv(event, records) {
  return csv(
    ["grave","prize","prize_number","winner_discord_id","winner_display_name",
      "winning_ticket_entry_id","allocation_id","snapshot_hash"],
    records.map(x => [
      event.config.names[GRAVES.findIndex(g => g.id === x.grave_id)],
      x.prize_name,x.prize_number,x.winner?.discord_id || "",
      x.winner?.display_name || "",x.winner?.ticket_entry_id || "",
      x.winner?.allocation_id || "",event.snapshot_hash || "",
    ]),
  );
}
function safeDiscordName(value) {
  return String(value || "Unknown").replace(/[\r\n\t]/g, " ")
    .replace(/[@]/g, "@\u200b")
    .replace(/[<>*_~`|]/g, "\\$&")
    .slice(0, 90);
}
function drawSummary(event, records) {
  const cards = GRAVES.map((g, idx) => {
    const chosen = records.filter(x => x.grave_id === g.id);
    const lines = chosen.map(x =>
      "**" + safeDiscordName(x.prize_name) +
      (chosen.filter(y => y.prize_name === x.prize_name).length > 1
        ? " #" + x.prize_number : "") + "** — " +
      (x.winner ? safeDiscordName(x.winner.display_name) : "No entries — unawarded")
    );
    const description = lines.join("\n");
    return {
      title: event.config.names[idx] + " — WINNERS",
      description,
      color: 0x7655a6,
    };
  });
  // Discord allows 4096 per embed and 6000 characters across a message.
  const length = cards.reduce((n,x)=>n+x.title.length+x.description.length,0);
  if (cards.some(x=>x.description.length>3900) || length>5800)
    throw Error("Too many winners for one announcement. Use the private winners CSV export and announce them in multiple posts.");
  return {
    content: "**TRICK OR TREAT: THE FOUR GRAVES — WINNERS**\n" +
      "Each buried ticket was a chance. All four grave draws are complete. " +
      "No prize distribution is automatic.",
    embeds: cards,
    allowedMentions: { parse: [] },
  };
}
module.exports = { drawPrizeWinners, winnersHash, winnersCsv, safeDiscordName, drawSummary };
