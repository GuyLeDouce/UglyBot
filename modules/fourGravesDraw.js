"use strict";
const { randomInt } = require("node:crypto");
const { GRAVES, prizeSlots, csv } = require("./fourGravesCore");

// One account wins once per pass; tickets weight the selection within that pass.
// The exact winning ticket is consumed. When all participating accounts have
// won once, start a new pass with the complete frozen ticket pool.
function pickNextTicket(entries, previousAwards, pick = randomInt) {
  if (!Array.isArray(entries) || entries.length === 0) return null;
  const pass = previousAwards.length ?
    Number(previousAwards[previousAwards.length - 1].pass_number) : 1;
  const won = new Set(previousAwards
    .filter(a => Number(a.pass_number) === pass).map(a => a.winner_discord_id));
  let eligible = entries.filter(t => !won.has(t.discord_id));
  const nextPass = eligible.length ? pass : pass + 1;
  if (!eligible.length) eligible = entries;
  const index = pick(eligible.length);
  if (!Number.isSafeInteger(index) || index < 0 || index >= eligible.length)
    throw Error("Random number generator returned an invalid ticket index.");
  return { ticket: eligible[index], pass_number: nextPass };
}

function prizeWinnerCsv(event, awards, grave = "all") {
  return csv(
    ["grave","prize_order","prize","prize_unit","winner_discord_id",
      "winner_display_name","winning_ticket_entry_id","pass_number",
      "announced_at","message_id","snapshot_hash"],
    awards.filter(a => grave === "all" || a.grave_id === grave).map(a => [
      event.config.names[GRAVES.findIndex(g => g.id === a.grave_id)],
      a.prize_index, a.prize_name, a.prize_unit,
      a.winner_discord_id, a.winner_display_name,
      a.ticket_entry_id, a.pass_number, a.announced_at || "",
      a.message_id || "",event.snapshot_hash || "",
    ]),
  );
}
function escapeDiscord(value) {
  return String(value || "Unknown").replace(/[\u0000-\u001f\u007f]/g," ")
    .replace(/@/g,"@\u200b").replace(/[<>*_`~|]/g,"\\$&").slice(0,100);
}
function choiceAnnouncement(event, grave) {
  const i = GRAVES.findIndex(g => g.id === grave.grave_id);
  const d = grave.decision;
  if (!d) throw Error("Cannot announce an undecided grave.");
  const test = event.config?.testMode === true;
  const who = test ? "the test operator" : "<@" + d.user_id + ">";
  return {
    content: (test ? "🧪 TEST ONLY — " : "@everyone ") +
      who + " chose **" + d.choice + "** for everyone buried in **" +
      escapeDiscord(event.config.names[i]) + "**!\n" +
      "Your team's prize pool is being revealed. " +
      "The first winner will be drawn in **2 minutes**; " +
      "one prize winner every **15 seconds** after that.",
    embeds: [{
      title: (test ? "🧪 TEST — " : "") + d.choice + " HAS BEEN CHOSEN",
      description: "First choice wins for the entire grave. " +
        "TRICK and TREAT reveal the exact same prizes.",
      footer: { text: "four-graves:" + event.id + ":" + grave.grave_id + ":choice" },
    }],
    components: [],
    allowedMentions: test ? {parse:[]} :
      { parse: ["everyone"], users: [d.user_id] },
  };
}
function winnerAnnouncement(event, graveId, award) {
  const i = GRAVES.findIndex(g => g.id === graveId);
  const total = prizeSlots(event.config, graveId).length;
  const test = event.config?.testMode === true;
  return {
    content: (test ? "🧪 TEST — " : "") + "🎁 **PRIZE " +
      award.prize_index + " / " + total + "** — **" +
      escapeDiscord(event.config.names[i]) + "**",
    embeds: [{
      title: escapeDiscord(award.prize_name) + " — Winner!",
      description: "🏆 **" + escapeDiscord(award.winner_display_name) +
        "**\nWinning ticket: `" + award.ticket_entry_id +
        "`\nDraw pass: " + award.pass_number +
        "\n\nNext winner in approximately 15 seconds, unless this was the last prize. " +
        "Prize distribution is handled manually.",
      footer: { text: "four-graves:" + event.id + ":" +
        graveId + ":prize:" + award.prize_index },
    }],
    components: [],
    allowedMentions: { parse: [] },
  };
}
module.exports = { pickNextTicket, prizeWinnerCsv, choiceAnnouncement, winnerAnnouncement, escapeDiscord };
