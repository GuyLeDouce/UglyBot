# Mad Libs PNG metadata decoder repair

## Scope and diagnosis

The owner supplied a valid 1254 x 1254 PNG (1,392,971 bytes) that passed the current 8 MiB / 4096-pixel limits, but the live worker returned the generic IMAGE error. Its caBX metadata contains an SVG fragment. The original image and its private metadata are **not committed** to this repository. Synthetic regression fixtures recreate the relevant byte pattern.

Canvas upstream issue #1308 and PR #1310 document a Buffer-source preflight that searches for `<svg` before respecting the raster signature. This exists in canvas 0.1.100, which is allowed by this repository's unchanged `^0.1.66` range. The earlier verification baseline 0.1.66 can accept the same PNG, explaining why baseline-only tests missed the problem. The exact library version deployed on Railway has not been read here.

Primary upstream references:
- https://github.com/Brooooooklyn/canvas/issues/1308
- https://github.com/Brooooooklyn/canvas/pull/1310
- https://github.com/Brooooooklyn/canvas/blob/v0.1.100/src/image.rs

## Implementation

The isolated Mad Lib worker constructs a raster data URL from the already downloaded, bounded and signature-validated bytes and assigns it directly to `new Image().src`. This uses the library's raster-first decoding path; it must not go through `loadImage(dataURL)` because that wrapper converts data URLs back to Buffer. It waits for `onload` before drawing. This is an in-memory source, not a remote fetch, a caller-supplied URL or a temporary file.

No global monkey patch or dependency upgrade is used. No PNG metadata or pixel bytes are rewritten before decoding. Normalized output remains PNG with the same dimensions, as in the existing uploader. The usual canvas re-encoding strips metadata from the published copy; the original attachment is unchanged. Actual SVG files are still rejected by the existing header validator. Signed Discord attachment URL restrictions, blocked redirects, image/stream byte limits, dimension checks, child-process timeout and concurrency limits all remain in place.

Failed worker replies now distinguish INIT, DECODE, DIMENSIONS, ENCODE and REPLY stages. Logs contain only fixed reason categories and validated canvas/Node version strings, never raw native errors, filenames, image data, signed URLs, tokens or other application credentials. The existing top-level IMAGE code is retained for compatibility.

Payments, rewards, saved stories, the human-world scene library, database schema, main application wiring and Railway variables are unchanged.

## Tests and rollout

`node scripts/testMadlibPngDecode.js` runs synthetic metadata/chunk/pixel regression tests and is also included in `node scripts/runMadlibChecks.js`. The additional PR workflow runs that suite and the existing Mad Lib logic suite on Node 20 with canvas 0.1.66 and 0.1.100. On 0.1.100 the test requires the previous Buffer path to reproduce SVG_MISDETECTED, then requires the corrected path to succeed. Consult actual CI results rather than treating this description as a claim that CI passed.

For the original owner-provided file, test locally without publishing or committing the artwork:

```bash
MADLIB_TEST_IMAGE_PATH='/absolute/path/to/original.png' node scripts/testMadlibPngDecode.js
```

After merging the PR, confirm that Railway deploys its resulting merge commit. Keep the existing variables; no new setting is needed. Reopen SHOW, select the same completed story and freshly attach the original PNG using `/madlib-upload`. No new play or payment is required. If it still fails, the private error now contains a stage code; share that and the matching `IMAGE_WORKER stage=...` line. Automated tests do not certify live Discord download, production storage or final preview delivery.

Rollback is a revert of this repair commit followed by the owner's deployment process. Keep all saved Mad Lib records; do not delete image/history/payment tables to troubleshoot decoding.
