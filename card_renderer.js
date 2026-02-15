const fs = require('fs');
const fetch = require('node-fetch');
const { createCanvas, loadImage } = require('@napi-rs/canvas');

// ---------- utils ----------
function isHttpUrl(v) { return typeof v === 'string' && /^https?:\/\//i.test(v); }

async function fetchBuffer(source) {
  if (isHttpUrl(source)) {
    const r = await fetch(source);
    if (!r.ok) throw new Error(`Image HTTP ${r.status}`);
    const ab = await r.arrayBuffer();
    return Buffer.from(ab);
  }
  return fs.promises.readFile(source);
}

function roundRectPath(ctx, x, y, w, h, r) {
  const rr = Math.min(r, w / 2, h / 2);
  ctx.beginPath();
  ctx.moveTo(x + rr, y);
  ctx.arcTo(x + w, y, x + w, y + h, rr);
  ctx.arcTo(x + w, y + h, x, y + h, rr);
  ctx.arcTo(x, y + h, x, y, rr);
  ctx.arcTo(x, y, x + w, y, rr);
  ctx.closePath();
}

function drawRoundRect(ctx, x, y, w, h, r, fill, stroke, lineW = 3) {
  roundRectPath(ctx, x, y, w, h, r);
  if (fill) { ctx.fillStyle = fill; ctx.fill(); }
  if (stroke) { ctx.lineWidth = lineW; ctx.strokeStyle = stroke; ctx.stroke(); }
}

function drawLightning(ctx, x, y, s = 1, color = '#111') {
  // simple bolt icon (matches your samples closely)
  ctx.save();
  ctx.translate(x, y);
  ctx.scale(s, s);
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.moveTo(0, 0);
  ctx.lineTo(10, 0);
  ctx.lineTo(4, 12);
  ctx.lineTo(12, 12);
  ctx.lineTo(-2, 30);
  ctx.lineTo(3, 16);
  ctx.lineTo(-6, 16);
  ctx.closePath();
  ctx.fill();
  ctx.restore();
}

function tierOuterFill(tier) {
  // outer frame tint (eyeballed from your examples)
  switch (tier) {
    case 'Uncommon': return '#74DCC3';
    case 'Rare':     return '#7D86C1';
    case 'Epic':     return '#E3B6F0'; // gradient comes from bg image; this is just fallback
    case 'Legendary':return '#E0B35A';
    default:         return '#BFE7F6'; // Common
  }
}

async function loadBgByTier(tierLabel, bgSources, cache) {
  const list = (bgSources && bgSources[tierLabel]) || (bgSources && bgSources.Common) || [];
  for (const source of list) {
    try {
      if (cache[source]) return cache[source];
      const buf = await fetchBuffer(source);
      const img = await loadImage(buf);
      cache[source] = img;
      return img;
    } catch {}
  }
  return null;
}

function cover(sw, sh, mw, mh) {
  const s = Math.max(mw / sw, mh / sh);
  const dw = Math.round(sw * s), dh = Math.round(sh * s);
  return { dx: Math.round((mw - dw) / 2), dy: Math.round((mh - dh) / 2), dw, dh };
}

function contain(sw, sh, mw, mh) {
  const s = Math.min(mw / sw, mh / sh);
  const dw = Math.round(sw * s), dh = Math.round(sh * s);
  return { dx: Math.round((mw - dw) / 2), dy: Math.round((mh - dh) / 2), dw, dh };
}

// ---------- main renderer ----------
/**
 * Renders EXACT template style like your samples (560x792 base).
 * - Standard layout for Common/Uncommon/Rare/Epic
 * - Special Legendary layout for hpTotal >= 1000 (Legend card)
 */
async function renderSquigCardExact({
  name,
  tokenId,
  imageUrl,
  traits,                 // grouped traits (Type, Background, Body, Eyes, Head, Skin, Special, Legend optional)
  rankInfo,               // { hpTotal }
  tierLabel,              // 'Common'|'Uncommon'|'Rare'|'Epic'|'Legendary'|'Mythic'
  bgSources,              // CARD_BG_SOURCES from your main file
  hpFor,                  // function hpFor(cat, val) from main file
  fonts = { reg: 'sans-serif', bold: 'sans-serif' },
  scale = 1               // your RENDER_SCALE
}) {
  const W = 560, H = 792;
  const SCALE = Math.max(1, Number(scale) || 1);

  const canvas = createCanvas(W * SCALE, H * SCALE);
  const ctx = canvas.getContext('2d');
  ctx.scale(SCALE, SCALE);
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = 'high';

  const hp = Number(rankInfo?.hpTotal || 0);
  const isLegend = hp >= 1000 || tierLabel === 'Legendary' || tierLabel === 'Mythic';

  const FONT_REG  = fonts.reg || 'sans-serif';
  const FONT_BOLD = fonts.bold || 'sans-serif';

  // --- outer frame + bg image ---
  const rCard = 28;
  drawRoundRect(ctx, 0, 0, W, H, rCard, tierOuterFill(isLegend ? 'Legendary' : tierLabel), '#111', 4);

  // clip inner to rounded
  const pad = 10;
  ctx.save();
  roundRectPath(ctx, pad, pad, W - pad * 2, H - pad * 2, rCard - 8);
  ctx.clip();

  // background art (your png templates)
  const cache = (globalThis.__CARD_BG_CACHE ||= {});
  const bg = await loadBgByTier(isLegend ? 'Legendary' : tierLabel, bgSources, cache);
  if (bg) {
    const { dx, dy, dw, dh } = cover(bg.width, bg.height, W, H);
    ctx.drawImage(bg, dx, dy, dw, dh);
  } else {
    ctx.fillStyle = '#BFE7F6';
    ctx.fillRect(0, 0, W, H);
  }

  ctx.restore();

  // ---- Standard Layout (matches Common/Uncommon/Rare/Epic samples) ----
  if (!isLegend) {
    // Top name pill
    const topPad = 22;
    const pillH = 44;
    const leftPillW = 260;
    drawRoundRect(ctx, 28, topPad, leftPillW, pillH, 18, '#D9EEF7', '#111', 3);

    ctx.fillStyle = '#111';
    ctx.font = `800 22px ${FONT_BOLD}`;
    ctx.textBaseline = 'middle';
    ctx.fillText(name, 44, topPad + pillH / 2);

    // UP Tag (yellow)
    const tagW = 160, tagH = 52;
    const tagX = W - 28 - tagW, tagY = topPad - 6;
    drawRoundRect(ctx, tagX, tagY, tagW, tagH, 18, '#F6D257', '#111', 3);

    ctx.font = `900 22px ${FONT_BOLD}`;
    ctx.fillText(`${hp}UP`, tagX + 20, tagY + tagH / 2);

    drawLightning(ctx, tagX + tagW - 34, tagY + 14, 0.9, '#111');

    // Art window
    const artX = 28, artY = 82, artW = W - 56, artH = 400;
    drawRoundRect(ctx, artX, artY, artW, artH, 22, '#D9EEF7', '#111', 4);

    // inner art clip + image
    const inner = 10;
    ctx.save();
    roundRectPath(ctx, artX + inner, artY + inner, artW - inner * 2, artH - inner * 2, 18);
    ctx.clip();
    try {
      const img = await loadImage(await fetchBuffer(imageUrl));
      const { dx, dy, dw, dh } = contain(img.width, img.height, artW - inner * 2, artH - inner * 2);
      ctx.drawImage(img, artX + inner + dx, artY + inner + dy, dw, dh);
    } catch {}
    ctx.restore();

    // Trait pills (2 columns x 3 rows)
    const pills = [
      { k: 'BG',    cat: 'Background' },
      { k: 'BODY',  cat: 'Body' },
      { k: 'EYES',  cat: 'Eyes' },
      { k: 'HEAD',  cat: 'Head' },
      { k: 'SKIN',  cat: 'Skin' },
      { k: 'SPECIAL', cat: 'Special' },
    ];

    const traitBaseY = 500;
    const traitPillW = 240;
    const traitPillH = 60;
    const gapX = 24;
    const gapY = 14;

    const leftX = 40;
    const rightX = leftX + traitPillW + gapX;

    function firstVal(cat) {
      const arr = traits?.[cat] || [];
      return arr[0]?.value || 'Unknown';
    }

    ctx.textAlign = 'center';
    ctx.fillStyle = '#111';

    for (let i = 0; i < pills.length; i++) {
      const row = Math.floor(i / 2);
      const col = i % 2;
      const x = col === 0 ? leftX : rightX;
      const y = traitBaseY + row * (traitPillH + gapY);

      drawRoundRect(ctx, x, y, traitPillW, traitPillH, 20, '#D9EEF7', '#111', 3);

      const cat = pills[i].cat;
      const key = pills[i].k;
      const val = firstVal(cat);
      const up = (typeof hpFor === 'function') ? Number(hpFor(cat, val) || 0) : 0;
      const title = `${key} (${up}UP)`;

      // Title line
      ctx.font = `900 18px ${FONT_BOLD}`;
      ctx.fillText(title, x + traitPillW / 2, y + 22);

      // Value line
      ctx.font = `600 16px ${FONT_REG}`;
      ctx.fillText(val, x + traitPillW / 2, y + 44);
    }

    // Type pill (bottom left)
    const typeX = 40, typeY = 708, typeW = 240, typeH = 56;
    drawRoundRect(ctx, typeX, typeY, typeW, typeH, 20, '#D9EEF7', '#111', 3);
    ctx.font = `900 18px ${FONT_BOLD}`;
    ctx.fillStyle = '#111';
    ctx.fillText('TYPE', typeX + typeW / 2, typeY + 22);
    ctx.font = `600 16px ${FONT_REG}`;
    ctx.fillText(firstVal('Type'), typeX + typeW / 2, typeY + 42);

    // Rarity banner (bottom right, diagonal-ish block like your samples)
    const bannerW = 230, bannerH = 66;
    const bx = W - 40 - bannerW, by = 708;
    drawRoundRect(ctx, bx, by, bannerW, bannerH, 20, '#F7F7F7', '#111', 4);
    ctx.font = `1000 22px ${FONT_BOLD}`;
    ctx.fillStyle = '#111';
    ctx.fillText(String(tierLabel || 'COMMON').toUpperCase(), bx + bannerW / 2, by + bannerH / 2 + 2);

    return canvas.toBuffer('image/png');
  }

  // ---- Legendary Layout (matches your Pikachugly sample style) ----
  {
    // top left name
    const topPad = 22;
    const pillH = 44;
    drawRoundRect(ctx, 28, topPad, 240, pillH, 18, '#F2E8D1', '#111', 3);
    ctx.fillStyle = '#111';
    ctx.font = `900 22px ${FONT_BOLD}`;
    ctx.textBaseline = 'middle';
    ctx.fillText(name, 44, topPad + pillH / 2);

    // top right UP tag
    const tagW = 170, tagH = 52;
    const tagX = W - 28 - tagW, tagY = topPad - 6;
    drawRoundRect(ctx, tagX, tagY, tagW, tagH, 18, '#F6D257', '#111', 3);
    ctx.font = `900 22px ${FONT_BOLD}`;
    ctx.fillText(`${hp}UP`, tagX + 20, tagY + tagH / 2);
    drawLightning(ctx, tagX + tagW - 34, tagY + 14, 0.9, '#111');

    // art window
    const artX = 28, artY = 82, artW = W - 56, artH = 420;
    drawRoundRect(ctx, artX, artY, artW, artH, 22, '#F2E8D1', '#111', 4);

    const inner = 10;
    ctx.save();
    roundRectPath(ctx, artX + inner, artY + inner, artW - inner * 2, artH - inner * 2, 18);
    ctx.clip();
    try {
      const img = await loadImage(await fetchBuffer(imageUrl));
      const { dx, dy, dw, dh } = contain(img.width, img.height, artW - inner * 2, artH - inner * 2);
      ctx.drawImage(img, artX + inner + dx, artY + inner + dy, dw, dh);
    } catch {}
    ctx.restore();

    // big LEGENDARY plate (center)
    const plateX = 40, plateY = 520, plateW = W - 80, plateH = 108;
    drawRoundRect(ctx, plateX, plateY, plateW, plateH, 28, '#F7F7F7', '#111', 5);
    ctx.textAlign = 'center';
    ctx.fillStyle = '#111';
    ctx.font = `1000 34px ${FONT_BOLD}`;
    ctx.textBaseline = 'middle';
    ctx.fillText('LEGENDARY', plateX + plateW / 2, plateY + plateH / 2 + 2);

    // legend pill (bottom left)
    const legX = 28, legY = 700, legW = 260, legH = 64;
    drawRoundRect(ctx, legX, legY, legW, legH, 20, '#F2E8D1', '#111', 3);
    ctx.font = `900 16px ${FONT_BOLD}`;
    ctx.fillText(`LEGEND (${hp}UP)`, legX + legW / 2, legY + 24);

    // If your grouped traits include Legend, show its name; else show "Legend"
    const legendVal = (traits?.Legend?.[0]?.value) || (traits?.legend?.[0]?.value) || 'Legend';
    ctx.font = `600 16px ${FONT_REG}`;
    ctx.fillText(legendVal, legX + legW / 2, legY + 46);

    return canvas.toBuffer('image/png');
  }
}

module.exports = { renderSquigCardExact };
