# Plan: one-shot-colour (Bayer/CFA) frame support

Status: in progress, 2026-09-23. Branch: `develop`. A task heading marked **[done]** was
committed on its own; the rest are open.

## Why

The first dataset from a second instrument — NGC 7331, ZWO ASI585MC on a 1568 mm scope,
captured by ZWO ASIAIR Mini, 248 × 60 s — fails QC on every frame sampled (7 of 7
`LOW_STARS`, 0–5 stars against `QC_STARS_MIN=10`). Two independent causes:

1. **The frames are raw Bayer mosaics (`BAYERPAT=RGGB`) and the pipeline has no CFA support at
   all.** The R/G/B sky pedestals differ by ~1900 ADU (R 4544, G 5552, B 3664 on a sample
   frame), and that checkerboard is read as noise: `sep`'s global RMS is 835 on the mosaic vs
   528 on the same frame reduced to 2×2 superpixels, and a 10σ extraction finds 41 vs 197
   sources. Image subtraction would be worse still — `astroalign` resamples by sub-pixel
   shifts, mixing colour channels, which leaves a colour residual at every star.
2. **`STAR_FWHM_MIN_ARCSEC=2.5` is an instrument-specific floor.** At 0.38″/px the stars
   measure 1.4–2.9″ (4–7 px) with the pipeline's own moment-based FWHM, so the hot-pixel
   floor removes nearly every real star. It was harmless on the previous telescope (~4.5″
   stars). A hot-pixel floor is a property of the pixel grid, not of the sky.

Also found: the `NGC 7331/Сложения/` folder holds ASIAIR live stacks (`NAXIS=3` RGB cubes,
`STACKCNT` > 1, `IMAGETYP=Light`) that `watcher.py`'s recursive scan would ingest as ordinary
epochs.

## T0 — design decision **[done]**

**Recommended: convert at ingest.** Step 0 of `pipeline.analyze_frame()` replaces a CFA frame
with a mono 2×2-superpixel frame before anything else reads it, and preserves the untouched
original in a separate raw archive. Every downstream module (QC, astap, extraction,
photometry, subtraction, forced photometry, finder charts, catalog preview) keeps reading a
plain 2-D mono image with consistent headers, and archived frames — later read back as
subtraction references and chart epochs — are already converted, so there is exactly one
conversion point.

Rejected alternative: convert in memory in a shared loader. It keeps the archive raw, but
astap solves the file on disk, so its WCS would be in the native grid while every in-memory
module works in the superpixel grid; each of the ~20 `fits.open()` sites, plus the WCS
written back into the header at archive time, would need a coordinate transform. That is the
same class of "two descriptions of one frame" bug the 2026-08-18 audit spent several findings
removing.

Superpixel over a true debayer: summing an aligned 2×2 block is independent of the Bayer
phase (`XBAYROFF`/`YBAYROFF` don't matter), introduces no interpolation (so no correlated
noise, see finding H13), and yields a luminance-like band close to the "L" / Gaia G situation
the photometry already handles. Cost: half the linear resolution — at 0.38″/px native,
0.76″/px still samples typical seeing adequately.

## Tasks

### T1 — `modules/cfa.py`: detection and superpixel conversion **[done]**
- `is_cfa(header) -> bool`: `BAYERPAT` (or `COLORTYP`) present and `NAXIS == 2`; not already
  converted (`CFACONV` marker absent).
- `to_superpixel(fits_path, dest_path) -> dict`: 2×2 block **mean** (keeps the ADU range, so
  `SATURATION_ADU` keeps its meaning). An odd trailing row/column is cropped.
- Saturation must survive averaging: a block containing any sub-pixel ≥ `SATURATION_ADU` is
  written as the block maximum, not the mean, so the existing threshold still flags it
  (a mean of one saturated and three unsaturated pixels would otherwise slip below it).
- Header rewrite: `NAXIS1/2` halved; `XPIXSZ`/`YPIXSZ` ×2 (ZWO writes them "with binning",
  and `fits_header.resolve_pixel_scale_arcsec()` uses them as effective pixel size);
  `XBINNING`/`YBINNING` ×2 for the record; `EGAIN` ×4 (a mean of 4 pixels carries 4× the
  electrons per ADU) — `GAIN` (vendor setting) untouched; remove `BAYERPAT`, `XBAYROFF`,
  `YBAYROFF`, `BZERO`/`BSCALE`; write float32; add `CFACONV = T` and a `HISTORY` card naming
  the original pattern and file.
- Strip any WCS the capture software wrote (`CTYPE*`, `CRPIX*`, `CRVAL*`, `CD*`/`PC*`/
  `CDELT*`, SIP `A_*`/`B_*`/`AP_*`/`BP_*`): it describes the native grid, and astap re-solves
  anyway. The mount's `RA`/`DEC` stay — they seed astap's narrow search.
- Missing `FILTER` → `FILTER = 'OSC'` (see T3).
- Pure function over files, no config side effects; `SATURATION_ADU` read from `config`.

### T2 — pipeline integration and raw preservation **[done]**
- `pipeline.analyze_frame()` step 0, before `fits_header.extract_headers()`: if `is_cfa()`,
  convert into a temp file beside the original (a failed conversion then leaves nothing
  moved), move the original to `FITS_RAW_ARCHIVE/{object}/` — new setting, default
  `/fits/raw`, new bind mount in `docker-compose.yml` for both services, same collision rule
  as `qc.py`'s rejected-file move (numeric suffix, never overwrite; C12) — and atomically
  rename the temp file onto the original path. The temp
  name must not end in `.fit`/`.fits`, and the rename must not make `watcher.py` enqueue the
  path a second time.
- **The raw copy is the operator's colour original** (for artistic stacking in Siril,
  PixInsight, DSS): byte-identical to the file that arrived, original filename, never renamed,
  never written to by the pipeline — no normalizer rename, no WCS or QC stamping. Pinned by a
  test comparing checksums.
- Idempotent: a frame carrying `CFACONV` is never converted twice — required for recovery
  `ANALYZE` tasks and operator re-analysis of archive paths.
- `catalog_preview.render()` must not touch its input (read-only diagnostic): convert into
  its existing `TemporaryDirectory()` and work on the copy.
- Log one INFO line per conversion (pattern, old/new size, new plate scale).

### T3 — `OSC` as a broadband filter **[done]**
- `modules/normalizer.py`: recognise `OSC` (and common spellings: `CFA`, `RGB`, `Color`,
  empty-on-a-CFA-frame is handled in T1) → canonical `OSC`; not narrowband.
- Photometry: no configured colour term for `OSC` → plain median zero point (pre-H5
  behaviour, consistent between epochs). Do **not** add a measured value to settings
  — frames come from several instruments, and a value measured on one is wrong for the next; per-instrument colour terms stay a separate follow-up.
- Subtraction's same-filter reference selection then naturally keeps OSC and mono-L apart.
- Note: an OSC camera shooting through a dual-band filter (`LeNhance` etc.) keeps that
  filter's name and stays narrowband — `FILTER` wins over the `OSC` default.

### T4 — refuse inputs the pipeline cannot analyse **[done]**
- Before QC: a frame with `NAXIS != 2` (colour cubes) or `STACKCNT > 1` (capture-software
  stacks) is not an epoch. Move it to `FITS_REJECTED/{object}/UNSUPPORTED_{filename}` with a
  WARNING and stop — not registered, not archived. Leaving it in `incoming` would re-enqueue
  it on every watcher restart (`process_existing_files()`).
- Make this check independent of CFA support: it protects mono pipelines just as much.

### T5 — hot-pixel FWHM floor in pixels
- Replace `STAR_FWHM_MIN_ARCSEC` with `STAR_FWHM_MIN_PX` in `modules/qc.py` (star-count mask
  and both median subsets) and `modules/astrometry/_extraction.py` (static floor under the
  dynamic `psf_fwhm / 1.5` bound). A hot pixel's footprint is fixed in pixels whatever the
  optics.
- Default chosen from measurements, not guessed: measured FWHM of hot pixels vs faintest real
  stars on both the IC3322A archive and the NGC 7331 set (expected ~1.5 px); record the
  numbers in the commit message.
- `config.py`, `.env.example`, `.env.test`, README; tests derived from config
  — CI runs on `.env.test`, so fixtures must not hardcode `.env` values.
- Cross-repo: the observatory-api `settings` seed still has `STAR_FWHM_MIN_ARCSEC` — queue
  in `docs/API-TASKS.md` (rename the row, keep the description in pixels). Same entry: the
  seed's `NARROWBAND_FILTERS` still lacks the M9 multi-band filters.
- `SEP_MIN_AREA=15` has the same problem (found in T2's check on a real converted frame: 3
  detections at 10σ, against 197 at `minarea=5`). A star's footprint above the threshold
  scales with FWHM², so a fixed pixel count admits only the brightest stars once the PSF is
  ~2 px across. Measure on both datasets and either derive the minimum area from the frame's
  own PSF (e.g. a fraction of π·FWHM²) or pick a default that works at both scales.
- Leave `QC_FWHM_MAX_ARCSEC` in arcsec: seeing *is* an angle, so that one is correct as is.

### T6 — tests
- `tests/test_cfa.py`: synthetic RGGB frame with distinct channel pedestals → flat
  superpixel background; a star's flux conserved (×¼ per mean); a block with one saturated
  sub-pixel stays ≥ `SATURATION_ADU`; odd dimensions; header keys (`XPIXSZ`, `EGAIN`,
  `CFACONV`, WCS stripped, `FILTER='OSC'` only when absent); mono frame passthrough;
  idempotency; all four Bayer phases give identical output.
- Pipeline: CFA frame → raw copy in `FITS_RAW_ARCHIVE`, converted frame analysed; collision
  suffix; cube/stack → `UNSUPPORTED_` in rejected, no API calls; catalog preview leaves its
  input untouched.
- QC/extraction: pixel floor behaves identically at two different plate scales.
- Full suite under `.env` and `.env.test`.

### T7 — documentation
- `CLAUDE.md`: new step 0 in the `pipeline.py` list; `modules/cfa.py` section (design
  decision and why superpixel); `OSC` row in the filter tables; `STAR_FWHM_MIN_PX` in the
  QC/astrometry sections; T4 in the QC "Action" paragraph.
- `README.md`: `FITS_RAW_ARCHIVE` volume and setting; `.env.example` in sync with `config.py`.

### T8 — validation run
- Unit tests green → rerun `qc.analyze()` on the NGC 7331 sample: target is the large
  majority `OK`, star counts in the tens-to-hundreds, not 0–5.
- Full run on NGC 7331 (`Сложения/` moved out of `incoming` beforehand). Check: QC flag
  distribution, astap solve rate and `pixel_scale_arcsec ≈ 0.76`, Gaia calibration rate,
  subtraction kicking in after the third archived frame, anomaly list by type — every
  `UNKNOWN`/`MOVING_UNKNOWN` reviewed by chart.
- Regression: re-analyse a handful of IC3322A frames (mono, unaffected by T1–T4) and compare
  star counts / QC flags with T5's new floor against the previous run.

## Commits
One task per commit (T1+T2 may land together if T2 is too thin alone), no co-author lines,
pushed and merged by the user.

## Out of scope / follow-ups
- Per-channel photometry (e.g. green-only, closer to Gaia G) or a true debayer.
- Per-instrument colour terms keyed by `TELESCOP`/`INSTRUME` + filter (existing follow-up).
- Task lease/heartbeat timeout in observatory-api (existing follow-up).
