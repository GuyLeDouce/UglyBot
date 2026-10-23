"use strict";
const { GRAVES } = require("./fourGravesCore");
const { escapeDiscord } = require("./fourGravesDraw");

// Each grave gets its own, persistent public panel. One row represents one
// participant and the COUNT of their individual tickets in that grave.
// Original entries are never changed or merged in the draw / export snapshot.
const PREVIEW_BUDGET = 3650;
const PAGE_SIZE = 20;
const mentions = { parse: [] };
const GRAVE_HEADINGS = Object.freeze({
  cletus_von_wrinkles: "Cletus' Grave",
  barry_buckets: "Barry's Grave",
  barbara_bowkins: "Barbara's Grave",
  scuba_steve_3000: "Steve's Grave",
});

function safe(value) {
  return escapeDiscord(value || "Unnamed");
}
function graveParticipants(snapshot, graveId) {
  if (!GRAVES.some(g => g.id === graveId)) throw Error("Unknown grave.");
  if (!snapshot || !Array.isArray(snapshot.entries)) throw Error("Ticket snapshot required.");
  const players = new Map();
  let tickets = 0;
  for (const ticket of snapshot.entries) {
    if (ticket.grave_id !== graveId) continue;
    tickets++;
    const id = String(ticket.discord_id);
    let p = players.get(id);
    if (!p) {
      // The first deterministic contestant label is the plain display name;
      // it already handles collisions between real Discord users.
      p = { discord_id: id, name: safe(ticket.label), tickets: 0 };
      players.set(id, p);
    }
    p.tickets++;
  }
  return { players: [...players.values()], tickets };
}
function displayPlayer(player) {
  return player.name + " — " + player.tickets +
    (player.tickets === 1 ? " ticket" : " tickets");
}
function previewPlayers(players, space = PREVIEW_BUDGET) {
  if (!players.length) return "*Nobody has buried a ticket here. Yet.*";
  const lines = [];
  let used = 0;
  for (const player of players) {
    const line = displayPlayer(player);
    // Keep enough room to explain that more people are available via View.
    if (used + line.length + 100 > space) break;
    lines.push(line);
    used += line.length + 1;
  }
  const remaining = players.length - lines.length;
  return lines.join("\n") +
    (remaining ? "\n*... and " + remaining + " more Squigs. View the full grave roster.*" : "");
}

function rosterPanel(event, snapshot, graveId) {
  if (!snapshot || !Array.isArray(snapshot.entries) || snapshot.entries.length === 0)
    return null; // No public rosters until the first confirmed burial.
  const index = GRAVES.findIndex(g => g.id === graveId);
  if (index < 0) throw Error("Unknown grave.");
  const { tickets, players } = graveParticipants(snapshot, graveId);
  const description =
    "**THE DEAD ARE KEEPING SCORE.**\n\n" +
    "One name per Squig. The number beside it is how many tickets they've buried " +
    "in this grave. Every ticket counts when fate comes calling.\n\n" +
    "**More tickets, more chances. No promises about what wakes up next.**\n\n" +
    previewPlayers(players);
  if (description.length > 4096) throw Error("Grave roster exceeds Discord's embed limit.");
  return {
    embeds: [{
      title: (event.config.testMode ? "🧪 TEST — " : "🪦 ") + GRAVE_HEADINGS[graveId],
      description,
      footer: { text: "four-graves:" + event.id + ":roster:" + graveId },
      fields: [{
        name: "Buried here",
        value: tickets + (tickets === 1 ? " ticket" : " tickets") +
          " · " + players.length + (players.length === 1 ? " Squig" : " Squigs"),
        inline: false,
      }],
    }],
    components: [{
      type: 1,
      components: [{
        type: 2, style: 2, disabled: false,
        custom_id: ["fg1", event.id, "roster", index, 0].join(":"),
        label: "View full " + String(event.config.names[index]).slice(0, 52) + " roster",
      }],
    }],
    allowedMentions: mentions,
  };
}
function rosterPage(event, snapshot, graveIndex, requestedPage = 0) {
  if (!Number.isSafeInteger(graveIndex) || graveIndex < 0 || graveIndex >= GRAVES.length)
    throw Error("Unknown grave roster.");
  const grave = GRAVES[graveIndex];
  const { players, tickets } = graveParticipants(snapshot, grave.id);
  const pages = Math.max(1, Math.ceil(players.length / PAGE_SIZE));
  const page = Number.isSafeInteger(requestedPage)
    ? Math.max(0, Math.min(requestedPage, pages - 1)) : 0;
  const current = players.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  const components = [{
    type: 1,
    components: [
      {
        type: 2, label: "Previous", style: 2, disabled: page === 0,
        custom_id: ["fg1", event.id, "roster", graveIndex, Math.max(page - 1, 0)].join(":"),
      },
      {
        type: 2, label: "Next", style: 2, disabled: page + 1 >= pages,
        custom_id: ["fg1", event.id, "roster", graveIndex, Math.min(page + 1, pages - 1)].join(":"),
      },
    ],
  }];
  return {
    content: "**" + GRAVE_HEADINGS[grave.id] + "** — " +
      tickets + " tickets / " + players.length + " Squigs. Page " +
      (page + 1) + " of " + pages + ".",
    embeds: [{
      title: event.config.testMode ? "🧪 TEST — GRAVE ROSTER" : "🪦 THE NAMES IN THE DIRT",
      description: current.length
        ? "**Every buried ticket counts. What that buys you is still a mystery.**\n\n" +
          current.map(displayPlayer).join("\n")
        : "*Nobody has buried a ticket here. Yet.*",
      footer: { text: "four-graves:" + event.id + ":" + grave.id + ":roster-page" },
    }],
    components: pages > 1 ? components : [],
    allowedMentions: mentions,
  };
}
module.exports = { rosterPanel, rosterPage, graveParticipants, PREVIEW_BUDGET, PAGE_SIZE };
