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

### T5 — hot-pixel FWHM floor in pixels **[done]**
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
- `SEP_MIN_AREA=15` looked like the same problem (T2's check on one converted frame found 3
  detections), but measured on 7 converted frames it is not, and it stays: with the pixel
  floor at 1.2 px, `qc.analyze()` passes all 7 as `OK` with 15–92 stars, FWHM 2.1–2.5″
  (≈3 px), elongation 1.1–1.2. Lowering it to 7 doubles the detections but pins the median
  FWHM at 1.67 px — the moment-based width of a tiny thresholded footprint, not the PSF — and
  that estimate would then set the `psf / 1.5` bounds in extraction. It also keeps the
  `SOURCES_ALL_ELONGATION_MAX=15` argument in CLAUDE.md (finding L4) valid.
- Result of the measurement that set the 1.2 px default: a single lit pixel reads 0.68 px, a
  lit 2×2 block 1.18 px; real stars on the converted NGC 7331 frames have a median of
  2.0–2.5 px and a 5th percentile of 1.2–1.4 px. The IC3322A archive was no longer on disk to
  cross-check; the old floor (2.5″) did not do the Vesta-incident work anyway — the
  per-frame `psf / 1.5` bound did, and it is unchanged.
- Leave `QC_FWHM_MAX_ARCSEC` in arcsec: seeing *is* an angle, so that one is correct as is.

### T6 — tests **[done]**
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
- Outcome: each task's tests landed with that task (T1–T5); T6 added the end-to-end check —
  a real Bayer frame through `analyze_frame()` is QC'd as mono, archived as mono, and its
  colour original sits byte-identical in `FITS_RAW_ARCHIVE`. 1114 passed under both env files.

### T7 — documentation **[done]**
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

### T9 — history prefetch that scales with the archive **[done]**
Not a colour-camera issue, but found on the same NGC 7331 run: the worker was OOM-killed
(`docker events`: `oom`, `die 137`) 94 frames into a 228-frame `DETECT_ANOMALIES` task.
Profile of the latest frame: `POST /sources/near/batch` returned **870 828 rows** for a
database holding **62 353** observations — RSS 2.27 GB, and 33 s of client-side filtering.
`_prefetch.py` queries per 0.1° tile with a radius of `MOVING_CONE_MAX_ARCSEC + 400″` ≈ 17′,
so on a 24′×14′ field every one of the ~14 tile queries returns the whole field's history, and
it grows with every frame (`before_time`).

What the classifier actually consumes:
- **Narrow cone** (`MATCH_CONE_ARCSEC`) around every source, all epochs — existence check and
  light curve. Bounded by the field's true history (≤ 62k rows here, not 870k).
- **Wide cone** (up to `MOVING_CONE_MAX_ARCSEC`) only for *uncatalogued* current sources —
  `_is_position_shifted()` is never reached for a catalogued one — and only *uncatalogued or
  MPC* historical detections. A catalogued star does not move; its absent detection tonight
  is a non-detection (faint, cloud, edge), not a vacated position, so counting it as motion
  evidence was a false-positive route. On this run: 39 uncatalogued observations of 62 353.
- **Coverage** per tile — 1.6k rows, unchanged.

Tasks:
- `api_client.get_sources_near_batch(..., uncatalogued_only=False)` — sent only when True.
- `_prefetch.py`: narrow query on per-source positions at `MATCH_CONE_ARCSEC`; wide query on
  uncatalogued sources only at the widest moving cone, `uncatalogued_only=True`, no tile
  margin; coverage as before; three requests concurrently.
- `_classify_source_sync()` takes the source's own narrow history and wide pool instead of a
  tile map; `detect()` passes them by index.
- Tests: the two queries' radii/positions/flag; a vacated uncatalogued detection still gives
  `MOVING_UNKNOWN`; catalogued sources never trigger a wide query.
- observatory-api (own branch, own commit): `uncatalogued_only` in `nearBatch()` —
  `JOIN sources … (catalog_name IS NULL OR catalog_name = 'MPC')`; plus a per-position
  bounding-box pre-check before the haversine (the loop is positions × candidates over one
  union box: ~18M haversines per request here). Documented in `docs/API.md`.
- An older API ignores the flag: the wide query then returns catalogued history too — the
  previous semantics, just without the saving. No crash either way.
- Then reset the stuck `DETECT_ANOMALIES` task to `PENDING` and let it finish.

Outcome, latest NGC 7331 frame: 870 828 rows / 2.27 GB RSS / 38 s → 51 981 narrow + 12 wide
rows / 284 MB / 0.8 s (observatory-api `develop`, `ae891fe`). One classification changed as
intended: the source at ra=339.4096 dec=34.3024 went from `MOVING_UNKNOWN` to `UNKNOWN` — its
"vacated position" evidence had been a catalogued star missing from this frame.

### T10 — zero point from every star with a Gaia magnitude **[done]**
From the T8 review of 136 `VARIABLE_STAR` on NGC 7331: references were only "Gaia DR3" matches,
and Simbad (first in the matching order) names most bright stars around NGC 7331. Median 3
references per frame, 63 photometry runs with fewer than 3 (uncalibrated), and one frame off 4
stars (scatter 0.28) put every star 0.4 mag too bright — 25 alerts from one epoch; 34 of the 136
were in the 4 frames with a frame-wide offset above 0.1 mag.
- `_attach_gaia_color()` also attaches the Gaia G magnitude (`_gaia_mag`) to a star another
  catalog named; `photometry._reference_mag()` accepts it.
- `PHOTOMETRY_MAX_ZERO_POINT_ERR` (0.1 mag): a frame whose zero point's standard error exceeds
  it is left uncalibrated. On the standard error, not the scatter — a field of mixed colours
  with no colour term (OSC) scatters widely and still pins the zero point.
- Real frames (worker container): references 4 → 85, 0 → 17, 3 → 72, 3 → 58.

### T11 — a magnitude change must hold for two epochs **[done]**
From the T8 review: 116 of 136 `VARIABLE_STAR` triggers were forced-photometry measurements of
faint stars (16–17.7 mag), 115 of them brighter in one epoch by ~5σ of the star's own light
curve and normal in the next — the signature of a cosmic ray or hot pixel in the aperture.
- `_classify._change_is_confirmed()`: the previous `VARIABILITY_CONFIRM_EPOCHS − 1` same-filter
  epochs must show the change too (same direction, same significance test, against the baseline
  of the remaining history), for every Δmag branch.
- On the stored light curves it keeps 55 of 136 — none in the frames T10 fixes; the remainder
  are edge vignetting on unflattened frames (the "fainter" blind-detection triggers) and short
  early baselines, left for the rest of the T8 review.

## Commits
One task per commit (T1+T2 may land together if T2 is too thin alone), no co-author lines,
pushed and merged by the user.

## Out of scope / follow-ups
- Per-channel photometry (e.g. green-only, closer to Gaia G) or a true debayer.
- Per-instrument colour terms keyed by `TELESCOP`/`INSTRUME` + filter (existing follow-up).
- Task lease/heartbeat timeout in observatory-api (existing follow-up).
