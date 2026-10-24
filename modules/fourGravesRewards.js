"use strict";

const { GRAVES, prizeSlots, csv } = require("./fourGravesCore");
const { escapeDiscord } = require("./fourGravesDraw");

const DESCRIPTION_LIMIT = 3400; // Discord embed description limit is 4096.
const LINE_LIMIT = 1100;
const NO_MENTIONS = { parse: [] };
const CSV_FILENAME = "Ugly_Halloween_Rewards.csv";

function readyForPublicRewards({ event, snapshot, graves, awards }) {
  if (!event || event.state !== "REVEALED" || !event.closed_at ||
      !snapshot || !Array.isArray(snapshot.entries) ||
      !Array.isArray(graves) || !Array.isArray(awards)) return false;
  const populated = GRAVES.filter(g =>
    snapshot.entries.some(t => t.grave_id === g.id));
  if (!populated.length) return false;
  for (const g of populated) {
    const state = graves.find(x => x.grave_id === g.id);
    if (!state?.decision || !state.draw_completed_at) return false;
    let slots;
    try { slots = prizeSlots(event.config, g.id); }
    catch { return false; }
    const winning = awards.filter(a => a.grave_id === g.id);
    if (winning.length !== slots.length) return false;
    const numbers = new Set();
    for (const award of winning) {
      if (!Number.isInteger(award.prize_index) ||
          award.prize_index < 1 || award.prize_index > slots.length ||
          numbers.has(award.prize_index) ||
          !award.winner_discord_id || !award.announced_at ||
          !award.message_id || !award.ticket_entry_id ||
          !snapshot.entries.some(t =>
            t.id === award.ticket_entry_id &&
            t.discord_id === award.winner_discord_id &&
            t.grave_id === g.id)) return false;
      const slot = slots[award.prize_index - 1];
      if (award.prize_name !== slot.name || Number(award.prize_unit) !== slot.unit)
        return false;
      numbers.add(award.prize_index);
    }
  }
  // There may be no awards for an empty grave, but NEVER permit awards
  // from a grave with no buried tickets.
  return awards.every(a => populated.some(g => g.id === a.grave_id));
}

function aggregateRewards({ event, snapshot, awards }) {
  const entriesById = new Map((snapshot?.users || []).map(u =>
    [String(u.discord_id), u]));
  const byUser = new Map();
  const gravesById = new Map(GRAVES.map((g, i) =>
    [g.id, event.config.names[i]]));
  const orderedAwards = [...awards].sort((a, b) =>
    GRAVES.findIndex(g => g.id === a.grave_id) -
      GRAVES.findIndex(g => g.id === b.grave_id) ||
    Number(a.prize_index) - Number(b.prize_index));
  for (const award of orderedAwards) {
    const id = String(award.winner_discord_id);
    let user = byUser.get(id);
    if (!user) {
      user = {
        discordId: id,
        name: String(entriesById.get(id)?.display_name ||
          award.winner_display_name || id),
        items: new Map(),
        charm: 0,
        prizesWon: 0,
        graveNames: new Set(),
      };
      byUser.set(id, user);
    }
    const name = String(award.prize_name);
    const charm = /^([\d,]+)\s+\$CHARM$/i.exec(name);
    if (charm) user.charm += Number(charm[1].replaceAll(",", ""));
    else user.items.set(name, (user.items.get(name) || 0) + 1);
    user.prizesWon++;
    user.graveNames.add(gravesById.get(award.grave_id) || award.grave_id);
  }
  const rows = [...byUser.values()].sort((a, b) =>
    a.name.localeCompare(b.name, "en", { sensitivity: "base" }) ||
    a.discordId.localeCompare(b.discordId));
  const collidingNames = new Map();
  for (const row of rows) {
    const key = row.name.trim().toLowerCase();
    collidingNames.set(key, (collidingNames.get(key) || 0) + 1);
  }
  return rows.map(row => {
    // Disambiguate identical Discord display names without triggering pings.
    const duplicate = collidingNames.get(row.name.trim().toLowerCase()) > 1;
    const label = escapeDiscord(row.name) +
      (duplicate ? " [User " + row.discordId + "]" : "");
    const prizes = [...row.items.entries()].map(([name, quantity]) =>
      (quantity > 1 ? quantity + " × " : "") + escapeDiscord(name));
    const csvPrizes = [...row.items.entries()].map(([name, quantity]) =>
      (quantity > 1 ? quantity + " × " : "") + name);
    if (row.charm) {
      prizes.push(row.charm.toLocaleString("en-US") + " $CHARM");
      csvPrizes.push(row.charm.toLocaleString("en-US") + " $CHARM");
    }
    return {
      discordId: row.discordId,
      label,
      csvLabel: row.name + (duplicate ? " [User " + row.discordId + "]" : ""),
      prizesWon: row.prizesWon,
      prizes,
      csvPrizes,
      graves: [...row.graveNames],
    };
  });
}

function formatRewardLines(rows) {
  const result = [];
  for (const player of rows) {
    const name = player.label;
    let line = name + " — ";
    let first = true;
    for (const reward of player.prizes) {
      const addition = (first ? "" : ", ") + reward;
      if (line.length + addition.length > LINE_LIMIT && !first) {
        result.push(line);
        line = name + " (continued) — " + reward;
      } else {
        line += addition;
      }
      first = false;
    }
    result.push(line);
  }
  return result;
}

function rewardSummaryPages(data) {
  if (!readyForPublicRewards(data))
    throw Error("Four Graves winners are not all finalized and announced.");
  const players = aggregateRewards(data);
  const lines = formatRewardLines(players);
  const intro = "The graves have given up their secrets. Every eligible team draw is complete.\n" +
    "**Here are the Squigs who walked away with the rewards.**\n\n";
  const pages = [];
  let text = intro;
  for (const line of lines) {
    if (text.length + line.length + 1 > DESCRIPTION_LIMIT) {
      pages.push(text.trimEnd());
      text = "";
    }
    text += line + "\n";
  }
  if (text.trim()) pages.push(text.trimEnd());
  return pages.map((description, i) => ({
    embeds: [{
      title: (data.event.config?.testMode ? "🧪 TEST ONLY — " : "🎃 ") +
        "UGLY HALLOWEEN REWARDS" +
        (pages.length > 1 ? " — PART " + (i + 1) + " / " + pages.length : ""),
      description,
      footer: { text: "four-graves:" + data.event.id + ":rewards:page:" + i },
    }],
    components: [],
    allowedMentions: NO_MENTIONS,
  }));
}

function rewardsCsv(data) {
  if (!readyForPublicRewards(data))
    throw Error("Cannot download incomplete grave winners.");
  const rows = aggregateRewards(data);
  return csv(
    ["Player", "Prizes Won", "Rewards", "Graves"],
    rows.map(p => [
      p.csvLabel,
      p.prizesWon,
      p.csvPrizes.join("; "),
      p.graves.join("; "),
    ]),
  );
}

// PUBLIC CHANNELS RECEIVE ONLY THE RECAP EMBEDS, NEVER THE CSV FILE
// OR A DOWNLOAD BUTTON. The following panel exists only as an ephemeral
// response to an authorized Four Graves admin.
function adminRewardsPanel(data) {
  if (!readyForPublicRewards(data))
    throw Error("The grave results are not complete. No winner download is available yet.");
  return {
    embeds: [{
      title: "🔒 UGLY HALLOWEEN REWARDS — ADMIN ONLY",
      description:
        "The Halloween prize draw is complete. Download the **private** " +
        "winner CSV with each Squig's combined rewards, Discord ID and " +
        "**current verified wallet address(es)**.\n\n" +
        "Multiple verified wallets are included. Missing or unverified " +
        "wallets are marked for manual review before you deliver NFTs.",
      footer: { text: "four-graves:" + data.event.id + ":rewards:admin-private" },
    }],
    components: [{
      type: 1,
      components: [{
        type: 2, style: 1, disabled: false,
        label: "DOWNLOAD WINNERS + WALLETS",
        custom_id: "fg1:" + data.event.id + ":admin-rewards-download",
      }],
    }],
    allowedMentions: NO_MENTIONS,
  };
}

function verifiedWallets(links) {
  if (!Array.isArray(links)) throw Error("Verified wallet lookup returned invalid records.");
  const seen = new Set();
  const addresses = [];
  for (const link of links) {
    // Only currently linked, VERIFIED wallets can appear on the payout list.
    // Do not use trade buyer addresses, unverifiable strings or historical
    // wallets that the holder unlinked during Trick or Treat.
    if (link?.verified !== true) continue;
    const addr = String(link.wallet_address || "").trim();
    if (!/^0x[a-fA-F0-9]{40}$/.test(addr)) continue;
    const canonical = addr.toLowerCase();
    if (!seen.has(canonical)) {
      seen.add(canonical);
      addresses.push(addr);
    }
  }
  return addresses.sort((a,b)=>a.toLowerCase().localeCompare(b.toLowerCase()));
}

async function resolveWinnerWallets(guildId, users, getWalletLinks) {
  if (typeof getWalletLinks !== "function")
    throw Error("Verified wallet lookup was not configured. Export cannot continue.");
  const result = new Map();
  let next = 0;
  // Keep query concurrency low: wallet_links lives in the holders database.
  await Promise.all(Array.from({length:Math.min(4,users.length)}, async () => {
    while (next < users.length) {
      const index = next++;
      const userId = users[index].discordId;
      const links = await getWalletLinks(guildId,userId);
      result.set(userId,verifiedWallets(links));
    }
  }));
  return result;
}

async function adminRewardsCsv(data, guildId, getWalletLinks) {
  if (!readyForPublicRewards(data))
    throw Error("Cannot export Halloween rewards before every populated grave is finalized.");
  if (String(guildId) !== String(data.event.guild_id))
    throw Error("Event guild mismatch: refusing wallet export.");
  const winners = aggregateRewards(data);
  const wallets = await resolveWinnerWallets(guildId,winners,getWalletLinks);
  return csv(
    ["Discord User ID","Player","Verified Wallet(s)","Prizes Won","Rewards","Graves"],
    winners.map(user=>[
      user.discordId,
      user.csvLabel,
      (wallets.get(user.discordId) || []).join("; ") ||
        "NOT VERIFIED — CHECK BEFORE DELIVERY",
      user.prizesWon,
      user.csvPrizes.join("; "),
      user.graves.join("; "),
    ]),
  );
}

module.exports = {
  readyForPublicRewards,
  aggregateRewards,
  rewardSummaryPages,
  rewardsCsv,
  adminRewardsPanel,
  verifiedWallets,
  resolveWinnerWallets,
  adminRewardsCsv,
  CSV_FILENAME,
  DESCRIPTION_LIMIT,
};
