const fs = require('fs');
const os = require('os');
const path = require('path');
const fetch = require('node-fetch');
const {
  DEFAULT_MAW_RANKING_CSV_PATH,
  MAW_RARITY_RULES,
  parseMawRankingCsvContent,
} = require('../modules/mawRarity');

const SPREADSHEET_ID = '13529PzSp0MmhaimmXIuAwwX3CmkbkHwfxMKrnbLfZqA';
const SHEET_GID = '523852047';
const CSV_URL = `https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}/export?format=csv&gid=${SHEET_GID}`;

function distributionText(distribution) {
  return MAW_RARITY_RULES
    .map((rule) => `${rule.label}: ${distribution[rule.key] || 0}`)
    .join('\n');
}

async function main() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'maw-rankings-'));
  const tempPath = path.join(tempDir, 'Squigs_Reloaded_Token_UglyPoints.csv');
  try {
    const response = await fetch(CSV_URL);
    if (!response.ok) {
      throw new Error(`Google Sheet download failed: HTTP ${response.status} ${response.statusText}`);
    }
    const buffer = await response.buffer();
    fs.writeFileSync(tempPath, buffer);

    const parsed = parseMawRankingCsvContent(fs.readFileSync(tempPath));
    fs.copyFileSync(tempPath, DEFAULT_MAW_RANKING_CSV_PATH);

    console.log(`Maw ranking CSV synced: ${DEFAULT_MAW_RANKING_CSV_PATH}`);
    console.log(`Rows: ${parsed.rows.length}`);
    console.log(`Hash: ${parsed.rankingSourceHash}`);
    console.log('Tier distribution:');
    console.log(distributionText(parsed.tierDistribution));
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

main().catch((err) => {
  console.error(`Maw ranking sync failed: ${err.message}`);
  process.exitCode = 1;
});
