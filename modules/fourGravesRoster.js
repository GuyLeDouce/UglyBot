"use strict";
const { GRAVES, PANEL_INDICES } = require("./fourGravesCore");
const { escapeDiscord } = require("./fourGravesDraw");

// A single permanent public message follows the grave selection panel. Only
// the first confirmed burial creates it; later burials EDIT that same message.
const PREVIEW_BUDGET = 650;
const PAGE_SIZE = 20;
const mentions = { parse: [] };

function graveTickets(snapshot, graveId) {
  return snapshot.entries.filter(entry => entry.grave_id === graveId);
}
function safe(value) {
  return escapeDiscord(value || "Unnamed");
}
function publicPreview(entries) {
  if (!entries.length) return "*No tickets buried yet.*";
  const lines = [];
  let used = 0;
  for (const entry of entries) {
    const label = safe(entry.label);
    if (used + label.length + 1 > PREVIEW_BUDGET) break;
    lines.push(label);
    used += label.length + 1;
  }
  const hidden = entries.length - lines.length;
  return lines.join("\n") +
    (hidden ? "\n*… and " + hidden + " more tickets. View this grave to see all.*" : "");
}
function rosterPanel(event, snapshot) {
  if (!snapshot || !Array.isArray(snapshot.entries) || !snapshot.entries.length)
    return null;
  const description = [
    "Each name below is a **buried ticket**. Repeated names with (1), (2) and so on are separate entries.",
    ...PANEL_INDICES.map(index => {
      const grave = GRAVES[index];
      const entries = graveTickets(snapshot, grave.id);
      const holders = new Set(entries.map(entry => entry.discord_id)).size;
      return "🪦 **" + safe(event.config.names[index]) + "'s Grave** — " +
        entries.length + " ticket" + (entries.length === 1 ? "" : "s") +
        " / " + holders + " member" + (holders === 1 ? "" : "s") +
        "\n" + publicPreview(entries);
    }),
  ].join("\n\n");
  if (description.length > 4096)
    throw Error("Four Graves roster preview exceeds Discord embed limit.");
  return {
    embeds: [{
      title: event.config.testMode ? "🧪 TEST — GRAVE TEAMS" : "🪦 THE FOUR GRAVES — TEAMS",
      description,
      footer: { text: "four-graves:" + event.id + ":roster" },
    }],
    components: [{
      type: 1,
      components: PANEL_INDICES.map(index => ({
        type: 2,
        custom_id: ["fg1",event.id,"roster",index,0].join(":"),
        label: "View " + String(event.config.names[index]).slice(0,40),
        style: 2,
        disabled: false,
      })),
    }],
    allowedMentions: mentions,
  };
}

function rosterPage(event, snapshot, graveIndex, requestedPage = 0) {
  if (!Number.isSafeInteger(graveIndex) || graveIndex < 0 || graveIndex >= GRAVES.length)
    throw Error("Unknown grave roster.");
  const grave = GRAVES[graveIndex];
  const entries = graveTickets(snapshot, grave.id);
  const pages = Math.max(1, Math.ceil(entries.length / PAGE_SIZE));
  const page = Number.isSafeInteger(requestedPage)
    ? Math.max(0, Math.min(requestedPage, pages - 1)) : 0;
  const current = entries.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  const holders = new Set(entries.map(x => x.discord_id)).size;
  const actions = [{
    type: 1,
    components: [
      {
        type: 2, label: "Previous", style: 2, disabled: page === 0,
        custom_id: ["fg1",event.id,"roster",graveIndex,Math.max(page - 1,0)].join(":"),
      },
      {
        type: 2, label: "Next", style: 2, disabled: page + 1 >= pages,
        custom_id: ["fg1",event.id,"roster",graveIndex,Math.min(page + 1,pages - 1)].join(":"),
      },
    ],
  }];
  return {
    content: "**" + safe(event.config.names[graveIndex]) + "'s Grave** — " +
      entries.length + " tickets / " + holders + " members. Page " + (page + 1) + " of " + pages + ".",
    embeds: [{
      title: event.config.testMode ? "🧪 TEST — BURIED TICKETS" : "🪦 BURIED TICKETS",
      description: current.length
        ? current.map(x => safe(x.label)).join("\n")
        : "*No tickets buried in this grave yet.*",
      footer: { text: "four-graves:" + event.id + ":" + grave.id + ":roster-page" },
    }],
    components: pages > 1 ? actions : [],
    allowedMentions: mentions,
  };
}

module.exports = { rosterPanel, rosterPage, PREVIEW_BUDGET, PAGE_SIZE };
