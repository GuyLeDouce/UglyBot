'use strict';
const RASTER_TYPES = new Set(['image/png', 'image/jpeg', 'image/webp']);

/** Decode already bounded, signature-checked raster bytes without the legacy
 * Buffer source SVG-sniffing preflight (canvas upstream issue #1308 / PR #1310).
 * Do NOT use loadImage(dataURL): its JS wrapper converts it back to Buffer.
 * This data URL is created here from bytes, never accepted as a fetch target.
 */
function decodeRaster(Image, bytes, info) {
  if (!Buffer.isBuffer(bytes) || !RASTER_TYPES.has(info?.media_type)) {
    return Promise.reject(new Error('Invalid raster input'));
  }
  return new Promise((resolve, reject) => {
    const image = new Image();
    image.onload = () => resolve(image);
    image.onerror = reject;
    try { image.src = `data:${info.media_type};base64,${bytes.toString('base64')}`; }
    catch (error) { reject(error); }
  });
}

function safeVersion(value) {
  return typeof value === 'string' && /^\d+\.\d+\.\d+(?:[-+][\w.-]+)?$/.test(value) ? value : 'unknown';
}
function failureReason(error) {
  // Only fixed categories leave the child, never paths, URLs, payloads or raw errors.
  const message = String(error?.message || '');
  if (/invalid svg/i.test(message)) return 'SVG_MISDETECTED';
  if (/cannot find module|cannot find native binding|failed to load native binding/i.test(message)) return 'NATIVE_BINDING';
  if (/shared object|dlopen|GLIBC|symbol not found/i.test(message)) return 'NATIVE_LIBRARY';
  if (/dimensions changed/i.test(message)) return 'DIMENSION_MISMATCH';
  return 'FAILED';
}

// Run native decoding outside the bot process. Never load index.js here.
if (require.main === module && typeof process.send === 'function') {
  process.once('message', async ({bytes, info, maxBytes}) => {
    let stage = 'INIT', canvasVersion = 'unknown';
    const diagnostics = reason => ({stage, reason, canvasVersion});
    try {
      canvasVersion = safeVersion(require('@napi-rs/canvas/package.json').version);
      const {Image, createCanvas} = require('@napi-rs/canvas');
      stage = 'DECODE';
      const image = await decodeRaster(Image, Buffer.from(bytes), info);
      stage = 'DIMENSIONS';
      const {width, height, media_type} = info;
      const same = image.width === width && image.height === height;
      const oriented = media_type === 'image/jpeg' && image.width === height && image.height === width;
      if (!same && !oriented) throw Error('Dimensions changed');
      stage = 'ENCODE';
      const canvas = createCanvas(image.width, image.height);
      canvas.getContext('2d').drawImage(image, 0, 0);
      const normalized = Buffer.from(canvas.toBuffer('image/png'));
      if (normalized.length > maxBytes) {
        process.send({ok:false, code:'IMAGE_SIZE', diagnostics:diagnostics('OUTPUT_SIZE')}); return;
      }
      process.send({ok:true, bytes:normalized, width:image.width, height:image.height});
    } catch (error) {
      process.send({ok:false, diagnostics:diagnostics(failureReason(error))});
    }
  });
}
module.exports = {decodeRaster, safeVersion, failureReason};
