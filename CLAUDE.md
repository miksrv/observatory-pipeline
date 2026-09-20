# CLAUDE.md — Observatory FITS Analysis Pipeline

This file gives an AI assistant the **design context** for this repo — what each module does,
why it's built that way, and how data flows between them. It deliberately does *not* repeat
what's already documented elsewhere; each of those facts has exactly one home:

| Topic | Source of truth |
|---|---|
| Deployment, Docker setup, environment variables, project structure, dependencies | [README.md](README.md) |
| REST API endpoint contracts (full request/response JSON) | [docs/API.md](docs/API.md) |
| `modules/anomaly_detector/` internals (batch prefetch, classification flowchart) | [docs/anomaly-detector.md](docs/anomaly-detector.md) |
| Open data-quality questions under investigation | [docs/ISSUES.md](docs/ISSUES.md) |

When something changes, update it in that one place — don't copy it here too.

---

## Project Overview

An automated Python service that runs on a **dedicated observatory server** and:
1. Detects new FITS frames as they arrive
2. Performs quality control (marks bad frames)
3. Runs astrometry (plate solving) and photometry (source extraction)
4. Performs image subtraction against archived frames of the same object to catch faint
   transients and moving objects that catalog matching alone would miss (optional module —
   degrades gracefully when too few reference frames exist yet)
5. Cross-matches detected sources against external astronomical catalogs
6. Compares against historical observations stored in the remote database
7. Classifies anomalies (supernovae, asteroids, comets, variable stars, space debris, unknowns)
8. Computes ephemerides for known solar system objects
9. Reports everything to the remote API — the pipeline has **no direct database access**

---

## Architecture: Two Repositories

### This repository — `observatory-pipeline` (Python)
- Runs on the **observatory server** (dedicated machine, local to the telescope)
- Deployed via **Docker / docker-compose**
- Communicates with the remote backend exclusively through **REST API + API Key**
- Has NO knowledge of the database schema — all persistence goes through API calls
- Handles all heavy astronomical computation locally

### Separate repository — `observatory-api` (CodeIgniter 4 / PHP)
- Runs on **cloud hosting**
- Provides a REST API consumed by both this pipeline and the observatory website
- Owns the MariaDB database and its schema
- Handles authentication (API Key for the pipeline, JWT or session for the website)
- This pipeline does NOT need to know table structure — only API endpoints and response shapes

---

## Infrastructure

```
[Observatory Server]                    [Cloud Hosting]
┌─────────────────────────┐            ┌──────────────────────────┐
│  docker-compose          │            │  CodeIgniter 4 API        │
│  ┌───────────────────┐  │  HTTPS +   │  ┌────────────────────┐  │
│  │  pipeline service │──┼─API Key───▶│  │  REST endpoints    │  │
│  └───────────────────┘  │            │  └────────────────────┘  │
│                          │            │           │               │
│  Volumes (on host disk): │            │  ┌────────▼───────────┐  │
│  /data/fits/incoming     │            │  │  MariaDB           │  │
│  /data/fits/archive      │            │  └────────────────────┘  │
│  /data/fits/rejected     │            │                           │
│  /data/astap/catalogs    │            │  Also consumed by:        │
└─────────────────────────┘            │  - Observatory website    │
                                        └──────────────────────────┘
```

`/data/...` is the **production** (Linux observatory server) convention; local development uses
macOS-friendly paths under `~/observatory-data/`. See README.md for the full setup.

**Security:** The pipeline server's outbound IP should be whitelisted on the cloud firewall.
The API key must be stored in `.env` and never committed to git.

---

## Docker & Configuration

Facts specific to *why* the image is built the way it is (deployment steps and the full
environment-variable reference live in README.md, not here):

- `astap` is installed from a **pre-downloaded archive** under `install/` (`install/astap_*.tar.gz`),
  not fetched at build time — this keeps builds offline and reproducible. Default is the amd64
  archive; swap in `astap_aarch64.tar.gz` for ARM64 / Apple Silicon.
- `xvfb` and the GTK/Pango system packages are required because `astap` needs a (virtual) display
  even when invoked headless — `modules/astrometry/_astap.py` runs it via `xvfb-run`.
- The source tree itself is bind-mounted to `/app` in `docker-compose.yml`, so code edits take
  effect on `docker compose restart pipeline` without a rebuild. A rebuild is only needed after
  changing `requirements.txt` or the Dockerfile itself.
- `config.py` is the authoritative source for every setting and its default; `.env.example`
  mirrors it and must be kept in sync by hand when a default changes.

---

## FITS Header Extraction

### `modules/fits_header.py`

Extracts all relevant metadata from FITS headers into a structured dictionary.
Standard FITS keywords supported (with common aliases):

| Category | Keywords | Description |
|---|---|---|
| **Observation** | `DATE-OBS`, `TIME-OBS`, `MJD-OBS` | Observation timestamp (see below — these three are resolved together, not first-non-empty) |
| **Target** | `OBJECT`, `OBJNAME`, `TARGET` | Name of the observed object (e.g., "M51", "NGC 1234") |
| **Coordinates** | `RA`, `DEC`, `OBJCTRA`, `OBJCTDEC` | Target coordinates (if provided by telescope) |
| **Exposure** | `EXPTIME`, `EXPOSURE` | Exposure time in seconds |
| **Filter** | `FILTER`, `FILTNAM`, `FILTERID` | Filter name (e.g., "V", "B", "R", "Ha", "Luminance") |
| **Instrument** | `INSTRUME`, `CAMERA` | Camera/instrument name |
| **Telescope** | `TELESCOP` | Telescope name/model |
| **Optics** | `FOCALLEN`, `APTDIA`, `APERTURE` | Focal length (mm), aperture diameter (mm) |
| **Sensor** | `CCD-TEMP`, `SET-TEMP`, `CCDTEMP` | Sensor temperature (°C) |
| **Pixel scale** | `XPIXSZ`, `PIXSIZE`, `PIXSCALE1`, `PIXELSZ`, `PIXSCALE` | Pixel size (µm) or plate scale (arcsec/px); used to estimate FOV before/without plate solving |
| **Binning** | `XBINNING`, `YBINNING`, `BINNING` | Pixel binning (e.g., 1x1, 2x2) |
| **Gain/Offset** | `GAIN`, `EGAIN`, `OFFSET` | Gain (e-/ADU), offset/bias level |
| **Image size** | `NAXIS1`, `NAXIS2` | Image dimensions in pixels |
| **Observer** | `OBSERVER`, `AUTHOR` | Name of the observer |
| **Site** | `SITENAME`, `OBSERVAT`, `SITELONG`, `SITELAT`, `SITEELEV` | Observatory location |
| **Software** | `SWCREATE`, `SOFTWARE` | Capture software name |
| **Frame type** | `IMAGETYP`, `FRAME` | Frame type: Light, Dark, Flat, Bias |
| **Airmass** | `AIRMASS` | Atmospheric airmass at observation time |

Function signature:
```python
def extract_headers(fits_path: str) -> dict:
    """
    Extract all relevant FITS headers into a normalized dictionary.
    Missing headers are set to None.
    Returns dict with keys matching the API payload structure.
    """
```

The `OBJECT` header is critical for organizing frames into subdirectories by target.

**Timestamp resolution.** The three timestamp keywords are resolved *together*, not taken
first-non-empty: `DATE-OBS` already carrying a time of day is used as-is (the modern convention,
and the common case); a date-only `DATE-OBS` is combined with `TIME-OBS` (the older convention,
where the time of day lives in its own keyword — and if `TIME-OBS` itself holds a full timestamp,
as some capture software writes, that wins over splicing); failing that, `MJD-OBS` is converted.
A bare `TIME-OBS` time-of-day with no date anywhere yields `None` rather than being returned:
nothing downstream can parse it, so returning it only turns a missing timestamp into a corrupt
one. Taking the first non-empty key instead — as an earlier revision did — silently placed every
frame of an old-convention night at midnight, an hours-scale epoch error for the SkyBot/Horizons
queries and for history comparisons (audit 2026-08-18, finding C10). A date-only frame with no
`TIME-OBS` at all still resolves to midnight, but logs a warning saying so.

**Exposure midpoint.** Alongside `obs_time` (the timestamp exactly as the header gives it), the
returned dict carries `obs_time_mid` — `obs_time + EXPTIME/2`, via the public
`fits_header.midpoint_time()`. `DATE-OBS` is the shutter-**open** time per the FITS convention,
but a moving object's position is only meaningful at the instant its light was centroided: a fast
NEO at 20–30″/min is already tens of arcsec away by mid-exposure on a several-minute frame — a
meaningful fraction of `MOVING_CONE_ARCSEC` (audit 2026-08-18, finding C9). It is a *separate*
field rather than a correction applied to `obs_time`, because `obs_time` is what the frame is
registered under (`POST /frames`) and what `normalizer.py` builds the filename's DateTime field
from; both must keep meaning exactly what the header says. Only the three consumers that compute
a position read it: the MPC/SkyBot cone search (`modules/catalog_matcher/_match.py` — every other
catalog there is stationary on this timescale), JPL Horizons
(`modules/anomaly_detector/_detect.py` → `_ephemeris_resolution.py`), and forced photometry's
Gaia proper-motion propagation. Anomaly detection's own history/coverage queries deliberately
keep `obs_time`. Each falls back to `obs_time` when no midpoint could be computed (no `EXPTIME`,
or an unparseable timestamp). The standalone `DETECT_ANOMALIES` path has no local FITS access, so
it recomputes the midpoint from the stored `GET /frames/{id}` record instead
(`pipeline._frame_exptime()` tolerates both the flattened and the nested `observation.exptime`
shape).

---

## Module Descriptions & Responsibilities

### `config.py`
Loads all configuration from environment variables (`.env`). Every module imports from here.
No hardcoded paths, thresholds, or credentials anywhere else.

### `watcher.py`
- Uses `watchdog` to monitor `FITS_INCOMING` directory for new `.fits` / `.fit` files
- Does **not** call `pipeline.run()` (or anything in `pipeline.py`) itself — it only buffers
  arriving paths and submits them as batched `ANALYZE` tasks via `api_client.create_task()`;
  `worker.py` is what actually calls `pipeline.analyze_frame()` per item. See "Job queue" below
  for the full design and why batching (not one task per file) matters for both a bulk import and
  a live overnight run.
- On new file detected: waits briefly for write to complete, then calls `enqueue_path(filepath)`,
  which appends to a module-level pending-batch buffer and (re)arms a `WATCHER_DEBOUNCE_SEC`
  debounce timer (`threading.Timer`) — or, if the buffer has now reached
  `WATCHER_MAX_BATCH_SIZE`, arms a zero-delay one instead of waiting out the full debounce window.
  When the timer fires, `flush_pending_batch()` submits everything buffered so far as **one**
  `ANALYZE` task and clears the buffer.
- The duplicate-event guard moved with it: `enqueue_path()` skips a path already sitting in the
  current pending batch — not, as before the module split, a path whose `pipeline.run()` call was
  still in flight, since there's no such long-running in-process call left in `watcher.py` at all
  now. Still guards against the same real incident (watchdog delivering two `FileCreatedEvent`s
  for the same path — e.g. the polling emitter used for Docker Desktop bind mounts on macOS, or a
  capture program that writes-then-renames the file).
- The flush itself always runs via `threading.Timer` (even the zero-delay max-batch-size case),
  never inline inside the watchdog observer's own event-delivery thread — so a slow
  `POST /tasks` call never delays detection of the next arriving file.
- `process_existing_files()` (the startup scan of files already sitting in `FITS_INCOMING`) also
  goes through `enqueue_path()` now, so a backlog from downtime becomes one bulk batch (or a few,
  if it exceeds `WATCHER_MAX_BATCH_SIZE`) instead of one event per file.
- On `KeyboardInterrupt`, flushes whatever's still buffered before exiting, so files that arrived
  just before the debounce window would have fired aren't silently dropped from the queue's view
  (they're still on disk either way — nothing here ever moves a file).
- Configures `logging.basicConfig()` using `config.LOG_LEVEL` (`DEBUG`/`INFO`/`WARNING`/`ERROR`)
- Logs all events

### `pipeline.py`
Orchestrates processing of a single FITS file in order:
1. `fits_header.extract_headers(fits_path)` → returns all FITS metadata
2. `normalizer.normalize_headers()` → normalize object name, filter, frame type (if enabled)
3. **Check frame type** (`IMAGETYP` header):
   - If `Dark`, `Flat`, or `Bias` → rename file (if normalization enabled) → move to `/fits/archive/{object}/` → **STOP** (no analysis needed)
   - If `Light` → continue processing
4. `qc.analyze(fits_path, move_on_reject=False)` → returns metrics + quality flag. The pipeline
   itself owns the file's fate from here (not `qc.py` — see that module's section below), so it
   passes `move_on_reject=False` rather than letting `qc.analyze()` move the file out from under
   it before the frame can be registered.
5. If `quality_flag != OK` → steps 6–9.5 below (astrometry, subtraction, catalog matching,
   photometry, forced photometry) are all **skipped** — there is nothing to detect sources
   against, and each of those steps' own existing guard (e.g. catalog matching's `if
   catalog_matcher is not None and sources:`) already no-ops once `sources` stays empty. Processing
   does **not** stop here anymore: the frame is still registered (step 10 below) with its QC
   metrics and non-`OK` `quality_flag`, `sources` is posted as an empty list (step 12 — this is
   also the exact mechanism a re-analysis "downgrade" relies on, see below), and the file is still
   archived (step 12.5) exactly like an `OK` frame, so a later re-analysis (after tuning QC
   thresholds) can find it again. This is a deliberate change from silently dropping a QC-rejected
   frame — see "Why QC-failed frames are registered, not dropped" below.
6. `astrometry.solve(fits_path, psf_fwhm_arcsec=...)` → returns WCS + two source lists: `sources` (strict star filter) and `sources_all` (loose filter — also keeps bright/saturated and faint detections, used for matching). This selection is made *before* step 7's merge below — `sources`/`sources_all` must already exist as names before anything tries to extend them. `psf_fwhm_arcsec` is step 4's `qc_result["fwhm_median"]`, forwarded only when `qc_result["fwhm_unit"] == "arcsec"` (see step 7 below for why).
7. `subtraction.run(fits_path, archive_dir, filter_name, wcs=astro_result["wcs"], psf_fwhm_arcsec=...)` → if ≥`SUBTRACTION_MIN_FRAMES` archived frames of the same object exist, aligns them (via `astroalign`), builds a median reference, subtracts, and returns candidate sources found only in the difference image. These are merged into the source list and flagged `_from_subtraction=True`. Skipped gracefully otherwise. The `wcs` passed here is step 6's already-solved WCS, not re-derived from `fits_path`'s own header — that header isn't corrected until step 14.5 archives the frame (see `modules/astrometry/`'s section below), so re-deriving it here would give subtraction's candidates a different systematic sky-position offset than every other source in the frame. `psf_fwhm_arcsec` is `qc_result["fwhm_median"]` from step 4 — passed only when `qc_result["fwhm_unit"] == "arcsec"` (it can instead be a raw pixel count when the frame's headers don't carry enough to derive a plate scale; see `modules/qc.py` below), since `astrometry.solve()`'s call in step 6 uses the same guard. See `modules/subtraction.py`'s section below for what this enables.
8. `catalog_matcher.match(sources, frame_meta)` → identifies known objects. **Runs before photometry** so matched Gaia DR3 stars can serve as the photometric zero-point reference.
8.5. `_dedupe_by_catalog_identity(sources, extra)` → collapses sources that share the same
     `(catalog_name, catalog_id)` within this one frame into a single representative source —
     otherwise a moving object matched both by the normal detection and by a nearby subtraction
     candidate (a real risk: `MOVING_CONE_ARCSEC` is wide enough for several nearby diff-image
     blobs to each independently match the same MPC object) would be posted/classified as two
     separate observations of the same object. Uncatalogued sources (`catalog_name is None`) are
     never merged — they have no stable identity to deduplicate on. Among duplicates, a normal
     detection is preferred over a subtraction candidate; among two of the same kind, the
     brighter one (higher flux) is kept.
9. `photometry.measure(fits_path, sources)` → returns calibrated magnitudes
9.5. `forced_photometry.run(fits_path, sources, gaia_stars, mpc_objects, wcs=astro_result["wcs"],
     zero_point=..., obs_time=...)` → a second, independent detection path (**"forced
     photometry" / "precovery"**, see git log for this feature's design history):
     for every Gaia DR3 star and MPC/SkyBot object within this frame's footprint that has
     no corresponding entry in `sources` (i.e. blind detection + step 8's forward matching
     never caught it — either too faint for `SEP_DETECT_THRESH`, or bright enough but
     rejected by the star filter/WCS residual/streak masking), measures the flux at that
     exact predicted pixel position anyway instead of silently treating it as "not
     detected". `gaia_stars`/`mpc_objects` are **not** re-queried — they're
     `catalog_matcher.get_gaia_stars()`/`get_mpc_objects()`, thin wrappers that hit the
     same in-process/on-disk cache step 8's `match()` call just populated for this same
     field, so this step costs zero extra network round-trips. Results are appended to
     `sources` in the same shape as an ordinary catalog-matched source (so they flow
     through steps 10–15 unchanged) and merged in **after** step 10 tags every other
     source with `mag`/`_filter`, tagging its own new entries the same way rather than
     running that pass twice. A measurement whose significance falls below
     `FORCED_PHOTOMETRY_MIN_SNR` is a genuine non-detection and is dropped outright — it is
     **not** reported as an "upper limit" magnitude, since the wire schema
     (`POST /frames/{id}/sources`, docs/API.md §2) has no field to distinguish a real
     magnitude from one; adding that is a separate, cross-repo change to observatory-api's
     schema, not made here. Scoped to Gaia DR3 + MPC only for now (2MASS/Pan-STARRS are a
     possible future extension); gated by `FORCED_PHOTOMETRY_ENABLED`, `FORCED_PHOTOMETRY_MAG_LIMIT`
     caps how faint a Gaia star is worth forcing (MPC objects are already pre-filtered by
     `MPC_MAG_LIMIT` upstream in step 8, so no separate cutoff applies to them here).
     Best-effort — any failure here is logged and swallowed; `sources` simply keeps
     whatever step 9 already gave it. See `modules/forced_photometry.py`'s section below.
10. Populate each source's unified `mag` field: `mag_calibrated` if `calibrated`, else
    `None` — **never** a fallback to the raw `mag_instrumental`, which has no absolute
    zero-point and is not a real magnitude on its own (see docs/ISSUES.md #2, where an
    earlier revision's instrumental fallback was the dominant cause of extreme, e.g. −15,
    magnitudes reaching the API whenever a whole frame failed to calibrate). This is the
    field the API payload documents and the one `anomaly_detector.py` reads for
    magnitude-change comparisons.
11. `api_client.post_frame(frame_data)` → registers the frame, gets back `frame_id`. `frame_data`
    (`pipeline._build_frame_payload()`) also carries `pointing_error_arcsec`/
    `pointing_error_ra_arcsec`/`pointing_error_dec_arcsec` — the mount's pointing error, computed by
    `pipeline._compute_pointing_error()` as the angular separation between the mount's own reported
    target position (`RA`/`DEC`/`OBJCTRA`/`OBJCTDEC` header keywords, read once by
    `fits_header.extract_headers()` before anything in this pipeline touches the file) and this
    frame's actual plate-solved centre (`astro_result["ra_center"]`/`["dec_center"]`, step 6) — the
    same discrepancy astap's own `.wcs` comment already reports for every solve (see
    `modules/astrometry/`'s `_wcs.py` real-incident docstring, `UGC_6930` — astap had found and
    logged a ~178" "Mount offset" that an earlier revision of this codebase silently discarded
    instead of trusting). All three fields are `None` together whenever either position is missing
    (no mount-reported target at all, or astrometry never solved this frame — QC-rejected before
    step 6, or astap failed). See docs/API.md §1 for the exact wire fields, and note there that
    the API is expected to keep the first value it ever stored per `frame_id` rather than overwrite
    it on a later re-analysis of the same file — the pipeline itself sends a freshly (re)computed
    value on every call regardless, same as every other field in this payload.
12. `api_client.post_sources(frame_id, filename, sources)` → saves all detected sources (already
    catalog-matched and photometrically calibrated); returns `source_ids` (positionally parallel
    to `sources`), which this step zips back onto each source dict as `_source_id` so
    `anomaly_detector.py` can populate `anomalies[].source_id`.
12.5. Move file to `/fits/archive/{object_name}/` directory. Just before the move,
     `_write_solved_wcs()` bakes astap's verified solve into the file's own header — clearing
     any pre-existing `CD`/`PC`/`CDELT`/`CROTA` cards first, since `WCS.to_header()` emits a
     PC+CDELT representation even for a CD-matrix WCS and `header.update()` removes nothing,
     so an incoming mount-pointing CD matrix would otherwise survive alongside astap's solve
     and the file would describe two transforms at once (audit 2026-08-18, finding H17) — and
     `_write_qc_headers()` stamps `QCFLAG`/`QCFWHM` beside it — both so that a *later* frame
     reading this one back off disk (finder charts for the WCS, subtraction's reference screen
     for the QC verdict) doesn't have to re-derive or re-ask for what this run already knew. Runs immediately after step 12, NOT
     after anomaly detection (an earlier revision of this file ran it later, between steps 14 and
     15) — anomaly detection never touches the local file at all, so there was no reason to delay
     archiving behind it, and doing so would have blocked decoupling anomaly detection into a task
     that might run much later (see `modules/anomaly_detector/` below). Must still run **before**
     step 15: that step looks up this same frame's own file at its archive path, so moving it any
     later than this would mean the current epoch is never found there. This is where
     **Module 1** ends — steps 1–12.5 are `pipeline.analyze_frame(fits_path)`'s entire body,
     independently callable as a task item (see "Job queue" below).
13. `anomaly_detector.detect(frame_id, sources, catalog_matches, frame_meta)` → finds anomalies, using the batched history/coverage API calls (see `api_client/` below)
14. `api_client.post_anomalies(frame_id, filename, anomalies)` → saves anomalies, but **only when
     step 13 actually ran to completion**. An empty list is a meaningful payload when detection
     genuinely found nothing (and is still posted then), but this call *replaces* the frame's
     anomaly set — posting `[]` because detection *failed* would erase anomalies a previous
     successful run had stored, turning one transient failure into permanent data loss (audit
     2026-08-18, finding C8). A failed or unavailable classifier therefore leaves the frame's
     stored anomalies untouched. Steps 13–14 are
     **Module 2** — `pipeline.detect_anomalies_for_frame_data()` (in-memory `sources`, used right
     after step 12.5 above) or its standalone counterpart
     `pipeline.detect_anomalies_for_frame_id(frame_id)` (reconstructs `sources` purely from
     `GET /frames/{id}` + `GET /frames/{id}/sources`, no local FITS access — see docs/API.md
     section 14 and "Job queue" below). This call **replaces** the frame's anomaly set rather than
     appending to it (docs/API.md section 3), so a re-run under a different classifier doesn't
     leave stale anomalies from the previous run behind.
15. `finder_chart.update_charts_for_sources(anomaly_types_by_source_id, designation_by_source_id)`
     — **Module 3**, via `pipeline.generate_charts_for_anomalies()` (in-memory) or
     `pipeline.generate_charts_for_source_ids()` (standalone, task-driven — see "Job queue" below).
     → for every anomaly with a resolved `source_id` (deduped per frame), (re)generates and
     uploads that source's finder/discovery chart(s): fetches every source's full position track in
     a single `POST /sources/tracks/batch` call, renders each against the matching local archive
     FITS files, then uploads each rendered chart individually via
     `POST /sources/{id}/chart` — one request per (source_id, style) pair. `anomaly_types_by_source_id`
     maps each source_id to a *list* of anomaly_types, not a single value — a source classified more
     than one way over its lifetime (e.g. `UNKNOWN` on the frame it was first seen, `MOVING_UNKNOWN`
     once it had moved) needs both its "track" and "stamp_strip" charts rendered, not just one
     arbitrarily overwriting the other (real incident, 2026-08-11, source_id
     `6a7be36b4d7578.98132403`: 12 `MOVING_UNKNOWN` + 1 `UNKNOWN` anomalies produced only a single
     chart before this fix — see `modules/finder_chart.py`'s module docstring). `designation_by_source_id` is built here
     preferring each anomaly's own `mpc_designation` (set by `anomaly_detector.py` from the exact
     source that produced that classification) over `sources`.`catalog_id` looked up by
     `source_id` — the latter is a fallback only, since `source_id` is resolved positionally by
     the API and can end up shared with an unrelated, previously-catalogued object at nearly the
     same sky position
     (real incident, 2026-08-06, `Vesta_A807_FA` test data: an MPC-matched asteroid's `source_id`
     also carried a `Gaia DR3` star's identity in `sources`, from a different detection sharing
     that row — using the `sources` lookup unconditionally would have shown `ASTEROID
     (3971465931154563840)` instead of `ASTEROID (2014 RY1)`). An uncatalogued source_id is simply
     absent from the dict rather than mapped to `None`. Best-effort — gated by `CHART_ENABLED`,
     and any failure (missing local file, API error, rendering error) only downgrades that one
     source_id's own result to `False`; it never affects any other source_id in the same call or
     frame processing overall. See `modules/finder_chart.py` below.

**Calibration frames (Dark, Flat, Bias):** These frames are used for image calibration but
contain no astronomical data to analyze. The pipeline simply normalizes the filename
(if `NORMALIZE_ENABLED=true`) and moves them to the archive. No QC, astrometry, photometry,
or API calls are performed.

### Job queue: `worker.py`

Separate process (own `docker-compose.yml` service) that polls observatory-api's `tasks` table
(docs/API.md section 14) and dispatches each task's items to the matching `pipeline.py` stage:

| Task `type` | Dispatched to | Item carries |
|---|---|---|
| `ANALYZE` | `pipeline.analyze_frame(item["filename"])` | `filename` — the FULL path to the FITS file, not just a basename |
| `DETECT_ANOMALIES` | `pipeline.detect_anomalies_for_frame_id(item["frame_id"])` | `frame_id` |
| `GENERATE_CHARTS` | `pipeline.generate_charts_for_source_ids(...)`, batched across the WHOLE task | `source_id` (required) + optionally `anomaly_id` + `payload` (`{"anomaly_type", "designation"}`) |
| `PREVIEW_CATALOG_MATCH` | `pipeline.preview_catalog_match(item["filename"], task_id, item["id"])` | `filename` — same "full path, not a basename" convention as `ANALYZE` |
| `RESTART` | Clean process exit → Docker restarts container → re-fetches remote settings | (none — signal task, no items) |

`GENERATE_CHARTS` only requires `source_id` — `payload.anomaly_type` is optional, not a second
required field. An item created from a resolved anomaly (observatory-api's
`Web\AnomaliesController::createTask()`) carries `anomaly_id` + `payload.anomaly_type`/
`designation`, and the chart title shows that anomaly_type (e.g. `ASTEROID (4 Vesta)`). An item
created directly from a source with no anomaly at all
(`Web\SourcesController::createTask()`, `/ui/sources/generate-charts`) intentionally sends only
`source_id` — that endpoint's own docstring says the pipeline decides the chart style itself, and
`worker.py`'s `_run_charts_task()` passes a missing `anomaly_type` through as `None` rather than
failing the item. `modules/finder_chart.py`'s `_style_for_source()` already has a sensible
fallback for `None`: "before_after" for a source with exactly one detected epoch (same as any
other anomaly_type), "stamp_strip" for 2+ epochs (no motion evidence to justify "track"); the
chart title simply omits the anomaly_type in that case.

A single task can carry **more than one item for the same `source_id`**, each with a different
`anomaly_type` — observatory-api's `Web\AnomaliesController::createTask()` submits one item per
distinct `anomaly_type` within a selected group, rather than collapsing a source's whole anomaly
history down to one arbitrary type (see that controller's own docstring). `worker.py`'s
`_run_charts_task()` collects all of a source_id's items into one list before the batched
`generate_charts_for_source_ids()` call, and looks up each item's own outcome afterwards by
`(source_id, anomaly_type)` — not by `source_id` alone — since `modules/finder_chart.py` renders
one chart per distinct *style* those types imply (see that module's section below), and two items
of the same source_id can resolve to two different styles that must both succeed or fail
independently.

A bare basename with no directory component at all (no full path) is not rejected outright: both
`analyze_frame()` and `preview_catalog_match()` run it through `pipeline._resolve_bare_filename()`
first, which searches every `FITS_ARCHIVE/{object}/` subdirectory for an exact filename match and
substitutes the full path if it finds exactly one. This exists because observatory-api's
`Web\FramesController::createTask()` debug page builds `ANALYZE`/`PREVIEW_CATALOG_MATCH` task
items straight from an already-registered frame's `frames.filename` column (a basename — see
`api_client/`'s section below), and the API has no way to supply a real full path itself:
it has no filesystem access to `/fits/...` at all (see "Architecture: Two Repositories"), so it
can't know `FITS_ARCHIVE`'s actual value for this deployment. Only the pipeline process can
resolve it, hence the fallback lives here rather than on the API side. Zero or more than one match
falls through to the input unchanged, so the ordinary "file not found" failure still surfaces
rather than a confusing resolver-internal one.

Exists so any of the three modules can be re-run independently of the other two — e.g. re-running
anomaly detection across an object's entire observation history (old and new frames alike, via
`GET /frames?object=...`, docs/API.md section 13) after fixing the classifier, without re-running
astrometry/photometry on every frame again. This is the concrete capability the three-way module
split unlocks; `pipeline.run()` alone can't express it at all — though it's worth noting
`watcher.py` no longer calls `run()` either; it submits `ANALYZE` tasks (see `watcher.py` above),
so in practice `run()` today is mainly a convenience composition for tests and any ad hoc
single-file invocation, not something on the live ingestion path.

After a `DETECT_ANOMALIES` task finishes all its items, anomalies are saved to the API. The
operator then decides which anomalies need charts and submits a `GENERATE_CHARTS` task from the
UI — referencing specific `anomaly_id`s from the `anomalies` table. There is no automatic
follow-up task creation between these two stages. Each `GENERATE_CHARTS` task item carries
`anomaly_id` (for traceability), `source_id` (denormalized from the anomaly for pipeline
convenience), and `payload` with `{"anomaly_type", "designation"}` so the pipeline doesn't need a
separate fetch.

**`RESTART` is a signal task, not a pipeline stage** — it carries no items and performs no
astronomical computation. Use case: an operator changes pipeline configuration parameters via the
API's `settings` table; since `worker.py` only fetches remote settings once at startup
(`GET /settings` → `config.apply_remote_settings()`), a settings change has no effect until the
worker restarts. The API (or an operator) submits a `RESTART` task; the worker picks it up on its
next poll (after the current task, if any, finishes), marks the task `COMPLETED`, and exits with
code `0`. Docker's `restart: unless-stopped` policy brings the container back up, and the fresh
process re-fetches settings on startup — the new values take effect without any manual
`docker compose restart`. The `pipeline` (watcher) service is a separate container that also
fetches settings at startup; if it also needs the new values, restart it manually
(`docker compose restart pipeline`) or submit a separate operational mechanism for it.

Politeness ("не нагружать сервер"): polls `GET /tasks?status=PENDING&limit=1&order=asc` (oldest
queued task first) at `TASK_POLL_INTERVAL_SEC` when idle, backing off exponentially up to
`TASK_POLL_BACKOFF_MAX_SEC` on consecutive empty polls and resetting the moment a task is found.
A busy queue is drained back-to-back with no sleep between tasks.

**Known limitation:** no lease/heartbeat/timeout mechanism yet — a task a worker claims (`PATCH
status=RUNNING`) and then crashes on stays stuck at `RUNNING` forever. Reset it by hand
(`PATCH /tasks/{id} {"status": "PENDING"}`) if that happens during testing.

**`PREVIEW_CATALOG_MATCH` is a diagnostic tool, not a fourth production module** — it never
registers a frame or source, never archives/rejects its input file, and doesn't chain into any
follow-up task (unlike `ANALYZE` → `DETECT_ANOMALIES` → `GENERATE_CHARTS`); its only API call is
uploading the rendered chart. It exists to let an operator visually sanity-check catalog-matching
quality on a batch of files — new or already archived — by rendering a PNG per frame with detected
sources circled green (matched a catalog) or red (didn't). It still calls the real
`modules/catalog_matcher/`, so repeated frames of the same object/session within one task benefit
from its on-disk cache exactly like a production `ANALYZE` run — only the first frame per sky tile
actually re-hits Gaia/Simbad/2MASS/Pan-STARRS/MPC. See `modules/catalog_preview.py` below. Its
result (`{"matched", "total", "quality_flag", "chart_uploaded"}`) is written onto each item's own
`payload` via `POST /tasks/{id}/items/progress` (see docs/API.md section 14), not just logged —
that endpoint's `payload` field is genuinely bidirectional: `GENERATE_CHARTS` reads it as input at
task-creation time, this task type writes it as a result at completion time.

### `modules/catalog_preview.py`

Backs the `PREVIEW_CATALOG_MATCH` task type (single public entry point: `render(fits_path)`). Runs
a frame through the real `qc.analyze()` (with `move_on_reject=False` — this module must never move
a rejected frame, since it's a read-only diagnostic tool, not part of the QC accept/reject
pipeline) → `astrometry.solve()` → `subtraction.run()` → `catalog_matcher.match()` — then renders
every detected source (`sources_all`, the loose filter `catalog_matcher`/`anomaly_detector`
actually operate on) as a circle on the frame's own pixel data: green + `CatalogName:id` label for
a matched source, plain red for an unmatched one. Circles are drawn at each source's *originally
detected* (RA, Dec) — before `catalog_matcher.match()`'s WCS-offset correction shifts
`source["ra"]`/`["dec"]` in place for cross-matching — and reuse the exact WCS `astrometry.solve()`
produced (astap's own fresh `.wcs` sidecar), not a fresh `WCS(header)` read from the file, so
circles land on the actual stars visible in this image rather than the Gaia-corrected sky position.

Nothing here writes a file that outlives the call: the PNG is rendered straight into an in-memory
buffer and returned as bytes; astap's `.ini`/`.wcs`/`.log` side files land in a `tempfile.
TemporaryDirectory()` that's removed on the way out regardless of success or failure.
`pipeline.preview_catalog_match()` uploads those bytes via
`POST /tasks/{task_id}/items/{item_id}/chart` (observatory-api's `SourceChartModel`, keyed by
`task_item_id` instead of `source_id` since a catalog-preview chart has no source at all) — that
upload is the only place the image ends up; there is deliberately no local-save option. There is
no standalone CLI script for this — create a `PREVIEW_CATALOG_MATCH` task instead.

### `modules/qc.py`
Computes quality metrics from a FITS file without plate solving:
- **FWHM** (median over detected stars) — indicator of focus quality
- **Elongation** (major/minor axis ratio of PSF ellipse) — indicator of tracking/trailing

  Both medians gate `BLUR`/`TRAIL`, so each is taken over the frame's *stars*, not over every
  raw `sep` detection — otherwise whatever extended, non-stellar morphology the field contains
  (nebula filaments, galaxies, compact knots) is averaged in, and a well-focused, well-tracked
  narrowband frame of a nebula can be rejected for what it was pointed at. The narrowband case
  is the sharp one, since such a frame is allowed a much smaller sample
  (`QC_STARS_MIN_NARROWBAND`) in which the clumps can outnumber the stars outright (audit
  2026-08-18, finding C11).

  Reusing the `star_count` mask for this does **not** work: it cuts at `STAR_FWHM_MAX_ARCSEC`
  and `STAR_ELONGATION_MAX`, whose defaults (8.0″, 1.5) sit at or below `QC_FWHM_MAX_ARCSEC`
  (8.0″) and `QC_ELONGATION_MAX` (2.0), so a median over its survivors could never exceed either
  QC threshold and both flags would become dead branches — the same failure as finding C2. Each
  median is therefore filtered on the *other* axis, never on the one it measures: `fwhm_median`
  over **round** sources (plus a relative pass dropping anything far broader than that subset's
  own compact population, which is what catches a round-*and*-extended blob), `elongation_median`
  over **compact** ones. Both reject anything sharper than `STAR_FWHM_MIN_ARCSEC` (hot pixels — a
  floor can only bias upward, so it can't hide blur). A subset of fewer than 3 sources falls back
  to the raw all-detections median, which is also what keeps both flags reachable on a frame so
  badly blurred or trailed that its own stars fall outside the opposite axis' bound.
- **SNR** (signal-to-noise ratio of detected sources) — computed and reported as `snr_median`,
  but **not currently compared** against `QC_SNR_MIN` in the accept/reject decision (see
  Known Issues #2 below — the threshold is effectively dead)
- **Sky background** (median + sigma after sigma-clipping) — compared against
  `QC_SKY_BACKGROUND_MAX` (see flag table below). Twilight, moonlight, cloud, or stray light
  raise this without necessarily blurring FWHM or trailing stars — a frame can look perfectly
  sharp and untracked-blurred while still being unusable because faint stars are drowned in an
  elevated background, which is exactly why this check is independent of BLUR/TRAIL.
- **Star count** (minimum threshold check against `QC_STARS_MIN` — or `QC_STARS_MIN_NARROWBAND`
  when the frame's own filter is narrowband, per `modules.normalizer.is_narrowband()`; see "Filters
  — real astronomy context" below for why a narrowband frame needs a softer floor. A hard-coded
  floor of 3 raw detections is also enforced independently, before either threshold is even applied)
- **Cosmic ray fraction** (via astroscrappy)

Quality flags and classification:
| Condition | Flag |
|---|---|
| FWHM > QC_FWHM_MAX_ARCSEC | `BLUR` |
| Elongation > QC_ELONGATION_MAX | `TRAIL` |
| Sky background > QC_SKY_BACKGROUND_MAX | `HIGH_BACKGROUND` |
| Star count < QC_STARS_MIN (or < 3 raw detections) | `LOW_STARS` |
| Multiple issues, or a FITS read / background-estimation / extraction failure | `BAD` |
| All good | `OK` |

The "Action" a non-`OK` flag triggers depends entirely on the caller's `move_on_reject` argument
(default `True`) — `analyze()` itself never decides this. `pipeline.py`'s `analyze_frame()` passes
`move_on_reject=False` and handles the file/registration itself (see that module's section above
and "Why QC-failed frames are registered, not dropped" below) — a non-`OK` flag no longer means
the file gets moved to `/fits/rejected/` at all in the production pipeline; it means the frame is
registered with that flag and archived normally. `move_on_reject=True` (the default `analyze()` itself falls back to when no argument is given)
still does move the file straight to `/fits/rejected/{object}/{FLAG}_filename.fits` — kept for any
ad hoc/test caller that invokes `qc.analyze()` directly rather than through `pipeline.py`. A
destination that already exists gets a numeric suffix (`{FLAG}_filename_1.fits`) rather than
being overwritten: `shutil.move()` overwrites silently on POSIX, which destroyed the earlier
file outright in the one subsystem whose whole purpose is to keep a rejected frame for manual
review (audit 2026-08-18, finding C12).
`modules/catalog_preview.py` (the `PREVIEW_CATALOG_MATCH` task type) also always passes
`move_on_reject=False`, for the same reason as `pipeline.py`: it must never move/touch its input
frame — see that module's section below.

`LOW_STARS` only fires when `BLUR`, `TRAIL`, and `HIGH_BACKGROUND` are all false — a low star
count is treated as a *consequence* of one of those three (sources filtered out, or too faint
to detect), not a separate root cause, so it isn't double-counted alongside whichever of them
actually explains it. `QC_SKY_BACKGROUND_MAX` has no universal default that fits every
site/instrument (same as `QC_FWHM_MAX_ARCSEC`) — tune it to your own site's typical dark-sky
`sky_background` reading on good frames.

**Important:** As of the QC-failed-frame registration change (see "Why QC-failed frames are
registered, not dropped" below), a bad frame **is** sent to the API — just with no sources,
astrometry, or photometry, since there's nothing to detect against. What this saves bandwidth/
storage/database-cleanliness on is the (much larger) source/photometry/catalog-matching payload a
bad frame would otherwise generate, not the frame registration itself — the operator needs the
frame + its QC metrics to see *why* it was rejected without SSHing into the observatory server.

### `modules/normalizer.py`
Normalizes FITS header values and filenames for consistency across different capture software:

**Object Name Normalization:**
| Input | Normalized |
|---|---|
| `M 51`, `M_51`, `m51` | `M51` |
| `NGC 1234`, `NGC_1234`, `ngc1234` | `NGC1234` |
| `IC 5070`, `IC_5070` | `IC5070` |
| `C 14`, `Caldwell 14` | `C14` |
| `Sh2 101`, `SH 101` | `SH2-101` |
| `Abell 39` | `Abell39` |
| `UGC 1234`, `PGC 1234`, `MCG 1234`, `Mrk 1234`, `Arp 1234`, `VCC 1234`, `ESO 1234`, `UGCA 1234` | `{PREFIX}{number}` (same pattern as NGC/IC) |
| `Andromeda Galaxy` | `Andromeda_Galaxy` |

**Filter Name Normalization:**
| Input | Normalized |
|---|---|
| `Luminance`, `Lum`, `L`, `Clear`, `clr` | `L` |
| `Red`, `RED`, `r` | `R` |
| `Green`, `g` | `G` |
| `Blue`, `BLUE`, `b` | `B` |
| `H-Alpha`, `Halpha`, `Ha` | `Ha` |
| `OIII`, `O3`, `[OIII]` | `OIII` |
| `SII`, `S2`, `[SII]` | `SII` |
| `NII`, `N2`, `N-II`, `Nitrogen-II`, `[NII]` | `NII` |
| Johnson-Cousins / SDSS filters `U`, `V`, `I`, `u'`, `g'`, `r'`, `i'`, `z'` | passed through as-is (recognized, not remapped) |

**Frame Type Normalization:**
| Input | Normalized |
|---|---|
| `Light Frame`, `light`, `LIGHT`, `Object`, `science` | `Light` |
| `Dark Frame`, `dark` | `Dark` |
| `Flat Field`, `flat`, `skyflat`, `domeflat` | `Flat` |
| `Bias`, `zero`, `offset` | `Bias` |

**Filename Generation:**
Files are renamed to a standardized format (enabled by `NORMALIZE_ENABLED=true`):
```
Light:            {Object}_Light_{Filter}_{Exptime}_{DateTime}[_{Seq}].fits
Dark/Flat/Bias:   {Object}_{FrameType}_{Exptime}_{DateTime}[_{Seq}].fits
```
The frame-type token is the full normalized word — `Light`, `Dark`, `Flat`, `Bias` — not a
one-letter code, and the filter token is present on Light frames only. (An earlier revision
used `L`/`D`/`F`/`B` codes; `modules/subtraction.py`'s reference-frame selection parses this
field, so the two must be read together.)

Examples:
- `M45_Light_B_60_2020-10-15T01-24-51.fits` (M45, Light, Blue filter, 60s)
- `M51_Light_Ha_300_2024-03-15T22-01-34.fits` (M51, Light, Ha filter, 300s)
- `NGC1234_Light_L_120_2024-03-15T22-01-34.fits` (NGC1234, Light, Luminance, 120s)
- `M42_Dark_300_2024-03-15T22-01-34.fits` (Dark frame, no filter)

When normalization is enabled, the API receives only normalized values (no duplicates).

### `modules/astrometry/`
A package, not a single file — split one file per step of `solve()`'s own pipeline
(`_astap.py` runs the binary, `_wcs.py` reads/validates the resulting WCS,
`_frame_geometry.py` derives centre/FOV/pixel scale from it, `_extraction.py` runs
sep + star filtering, `_streak.py` is the pre-pass `_extraction.py` calls before its
own `sep.extract()`). `__init__.py` holds `solve()` itself as the orchestrator and
re-exports it, so every call site elsewhere in this codebase is unchanged.
- Before running `sep` for point-source extraction, a coarse, low-threshold, non-deblended
  pre-pass (`_build_streak_mask()`, `config.STREAK_*`) finds long thin streaks — satellite/
  aircraft trails crossing a single exposure, and diffraction-spike arms radiating from bright/
  saturated stars — and masks their pixels out first. Without this, the trail/spike fragments
  into several small, roundish sep objects at the ordinary extraction settings (deblending splits
  an already-faint, gap-prone elongated feature into round sub-blobs), each individually clearing
  `STAR_ELONGATION_MAX` and getting reported as an ordinary star (real incident, 2026-08-07,
  `T_CrB_Light_L_60_2024-05-28T19-06-10.fits`: a full-frame satellite trail produced 5 spurious
  "stars" sitting exactly along its track; with this pre-pass, `sources`/`sources_all` count drops
  from 757 to 752 and none of the survivors carry elongation above 3). A candidate from the coarse
  pass is only ever masked when it is BOTH highly elongated (`STREAK_ELONGATION_MIN`) AND far
  longer than any real stellar PSF footprint (`STREAK_MIN_LENGTH_ARCSEC`, measured off the coarse
  object's own bounding-box diagonal) — a combination no ordinary star, even a deblended close
  pair, ever reaches; the real extraction's own `deblend_cont` is left completely untouched, so its
  ability to split a genuinely close double star in a crowded field is unaffected. `STREAK_DETECT_SIGMA`
  (the coarse pass's own threshold) is deliberately lower than `SEP_DETECT_THRESH` — a faint
  trail's brightness dips below a higher threshold often enough along its length that it still
  breaks into several disconnected coarse components too short to individually clear
  `STREAK_MIN_LENGTH_ARCSEC` (verified on the same T_CrB frame's difference image, see
  `modules/subtraction.py` below). `modules/qc.py` duplicates this same helper (its own copy, kept
  in sync by hand — same convention as the FWHM/elongation filtering logic both modules already
  independently reimplement) so its `fwhm_median`/`elongation_median`/`star_count` stay consistent
  with what this module will end up extracting from the same frame.

  Each masked streak is then **re-emitted as one detection of its own**, at its own centroid,
  appended to `sources_all` (never to `sources` — a trail is not a star and must not reach the
  photometric reference set) and bypassing that list's elongation ceiling, which exists to reject
  a near-zero minor axis's degenerate `a/b` rather than a feature deliberately selected for being
  elongated. The mask's two thresholds cannot geometrically tell a satellite or aircraft trail
  from a genuine fast NEO trailing within a single exposure — both are a long, thin streak — so
  masking alone erased a real moving object's pixels before `sep.extract()` ever ran, with no
  second chance, since the frame is never re-analysed from other data (audit 2026-08-18, finding
  H16). One detection per streak restores the evidence without restoring the fragmentation: enough
  for the MPC cone search to identify a known object there, and for the `SPACE_DEBRIS` branch to
  classify an unknown one.
- Calls `astap` binary as a subprocess via `xvfb-run` (astap needs a display even headless) for plate solving,
  invoked without `-update` — astap therefore never writes into the FITS file itself, only into a `.wcs` side
  file (plus `.ini`/`.log`) next to it, or under an optional `output_base` (`-o`) path
- Validates the resulting WCS for plausibility before returning it (`_wcs.py`'s
  `_is_plausible_wcs()`): reference coordinates on the sphere, a plate scale between
  `ASTROMETRY_PIXEL_SCALE_MIN_ARCSEC` and `ASTROMETRY_PIXEL_SCALE_MAX_ARCSEC`, a non-degenerate
  CD matrix, and a finite pixel→world round trip at the frame centre. Nothing checked the solve
  beyond astap reporting "Solution found" and `has_celestial` — but the WCS is authoritative by
  construction (every source position, catalog match and anomaly coordinate comes from it, and no
  downstream module has anything to compare it against), so a false star-pattern match became a
  systematic position error for the whole frame with no distinguishing log line (audit
  2026-08-18, finding H15). The risk concentrates in `ASTAP_RETRY_WIDE_SEARCH`'s blind 30°
  retry. A failure is a hard one — `solve()` returns `{}` — because a confidently wrong WCS is
  worse than none: the frame's sources would be posted at wrong coordinates and then compared
  against history at those same wrong coordinates. astap's own free-text solve report (`Solved in
  0.1 sec. Offset 3.0'. Mount offset ...`) is logged from the `.wcs` COMMENT cards but not gated
  on — its wording varies by astap version and search mode.
- Parses the WCS from that fresh `.wcs` side file — deliberately preferred over any WCS the incoming FITS
  header might already carry, even when the header's own WCS already looks celestial. A capture program can
  write an approximate WCS from mount pointing alone (not a real plate solve) with valid-looking
  `CTYPE`/`CRVAL`/`CD*` keywords; trusting that over astap's own verified solve defeats the purpose of running
  astap at all (real incident, 2026-08-06, `UGC_6930` test frame: header WCS and astap's fresh solve differed
  by ~178″/3′ — astap's own `.wcs` comment had already reported and corrected that exact "Mount offset", but
  the pipeline was silently discarding it and using the stale header value for every downstream step). Only
  falls back to the header's own WCS if the `.wcs` side file is missing or fails to parse.
- Runs `sep` (SourceExtractor) for source detection, dynamically narrowing **both** FWHM bounds
  around an estimated `psf_fwhm_arcsec` (this frame's own measured stellar PSF, from
  `qc.analyze()`'s `fwhm_median`) when available: upper bound → `psf_fwhm_arcsec × 1.5` (rejects
  compact galaxies broader than the stellar PSF), lower bound → `psf_fwhm_arcsec / 1.5` (rejects
  hot/warm sensor pixel clusters and similar artifacts far sharper than any real star in this
  frame — a static, site-agnostic `STAR_FWHM_MIN_ARCSEC` floor alone can sit comfortably below a
  hot pixel's measured FWHM even when that pixel is still far more compact than every genuine
  star here; real incident, 2026-08-06, Vesta test frames, `sources_all` fed hot pixels ~2.6–3.0″
  FWHM into anomaly detection as `UNKNOWN` alerts on a frame whose real stars measured ~4.5″).
  Both bounds fall back to the static `STAR_FWHM_MIN_ARCSEC`/`STAR_FWHM_MAX_ARCSEC` config values
  when no PSF estimate is available. This tightened lower bound applies to `sources_all` too, not
  just the strict `sources` list, since both share the same underlying FWHM mask.
- Converts pixel coordinates to (RA, Dec) using `astropy.wcs.WCS`
- Returns a dict: `{ra_center, dec_center, fov_deg, position_angle_deg, naxis1, naxis2, sources, sources_all, wcs}`
  - `sources` — strict star filter, list of dicts `{ra, dec, flux, fwhm, elongation, saturated, near_edge, ...}`
  - `sources_all` — loose filter; additionally keeps bright/saturated and faint detections rejected by the strict filter, used downstream for catalog matching / WCS offset correction so moving or transient objects aren't lost. Its own elongation ceiling is `SOURCES_ALL_ELONGATION_MAX` (15.0 default), deliberately far above `STAR_ELONGATION_MAX` — a trailed detection is precisely what `modules/anomaly_detector/` needs to classify `SPACE_DEBRIS`, and this list is the only detection list that module ever sees. It must stay above `SPACE_DEBRIS_EDGE_ELONGATION_MIN`: as a hardcoded `5.0` it sat below that setting's `6.0`, so a trailed source near the frame edge was cut here at extraction time and the edge branch of `SPACE_DEBRIS` could never fire on anything (audit 2026-08-18, finding C2). `_extraction.py` logs a warning when a deployment configures the two back into that dead state. The ceiling still rejects the degenerate `a/b` ratios a near-zero minor axis produces.
  - `wcs` — the `astropy.wcs.WCS` object itself, also consumed by `modules/subtraction.py` to convert difference-image pixel candidates back to sky coordinates
  - `position_angle_deg` (`float | None`) — this frame's own orientation on the sky (0° = North
    up, increasing clockwise toward the image's +X pixel axis), derived from the solved WCS via a
    pixel↔world round trip rather than decoding the CD/PC matrix's trig directly (stays correct
    regardless of the matrix's flip/determinant sign convention or any SIP/TPV terms). `None` if
    that round trip fails. Persisted to the API purely as a diagnostic (see "Camera rotation" under
    `modules/subtraction.py` below) — never gates anything on its own; a large difference between
    two frames is expected and unremarkable (e.g. a meridian flip), not a data-quality signal.
  - `saturated` (bool, on every source in both lists) — raw ADU at the detection's peak
    (`sep`'s background-subtracted `peak` field with `bkg.globalback` added back) at or above
    `SATURATION_ADU`. Added because bright/saturated stars are deliberately kept in `sources_all`
    (to not lose asteroids), but aperture photometry on a saturated PSF core produces a physically
    meaningless flux — `modules/photometry.py` reads this flag to skip measuring such a source
    instead of returning an extreme (e.g. −14) magnitude. See docs/ISSUES.md #2.
  - `near_edge` (bool, on every source in both lists) — pixel position within
    `EDGE_MARGIN_FRAC` of any frame edge (computed straight from `sep`'s own `x`/`y`, no WCS
    needed). Coma and other off-axis aberrations progressively stretch a star's PSF toward the
    edges/corners of a wide-field frame, inflating its measured `elongation` for purely optical
    reasons rather than real motion or trailing — `modules/anomaly_detector/` reads this flag to
    demand a higher elongation bar before classifying such a source `SPACE_DEBRIS` (real incident,
    2026-08-07, `T_CrB` frames: 305 anomalies out of 4 frames, the vast majority coma-elongated but
    otherwise ordinary corner stars). Deliberately no leading underscore, same as `saturated` above
    — `api_client`'s `_to_wire_source()` lets it travel to the API unfiltered and it's persisted on
    `source_observations`, so `pipeline.py`'s standalone `_from_wire_source()` can reconstruct it
    for a decoupled `DETECT_ANOMALIES` re-run with no in-memory pixel position to recompute it from.

### `modules/photometry.py`
- Aperture photometry via `photutils.aperture`
- Differential photometry against Gaia reference stars in the field (requires ≥3 Gaia DR3 matches to compute a zero-point) — this makes brightness measurements immune to atmospheric transparency variations
- Each source's sky annulus is **sigma-clipped** (`PHOTOMETRY_SKY_SIGMA_CLIP`, 3σ) before its
  median is taken. The ring is a background sample only in principle — in practice it routinely
  catches a neighbouring star, a cosmic ray, or, worst, the host galaxy's own light under a
  `SUPERNOVA_CANDIDATE` — and an unclipped median subtracts that contamination straight out of the
  source's flux, a systematic bias worst exactly where photometry matters most (audit 2026-08-18,
  finding H7). A non-positive threshold restores the unclipped median.
  `modules/forced_photometry.py` duplicates the same helper by hand.
- Zero-point reference stars are screened through Gaia's own quality flags before anything is
  fitted: a star the catalog calls variable, one flagged `duplicated_source`, or one whose
  astrometric solution fits badly (`ruwe` above `PHOTOMETRY_REF_MAX_RUWE`, usually an unresolved
  binary or a blend whose aperture holds two stars' flux) must not anchor a calibration (audit
  2026-08-18, finding H6). The flags travel on the matched source as `_catalog_flags`
  (`modules/catalog_matcher/_gaia.py`); a flag the catalog didn't supply counts as acceptable, so
  a narrower astroquery column set changes nothing. Screening only narrows the set — if it would
  leave fewer than the 3 references a zero-point needs, the unscreened set is used instead and the
  fallback is logged, since a zero-point anchored on a few imperfect stars beats losing
  calibration for the whole frame.
- `zero_point_err` is a **small-sample-corrected** robust scatter (`_robust_scatter()`), not a
  plain `1.4826 × MAD`. At the minimum n=3, a "two good references plus one outlier" set puts the
  median on one of the two good values and drives the MAD to exactly zero — a perfect reported
  error at the moment the calibration is least trustworthy. Croux & Rousseeuw's finite-sample
  factor corrects the MAD's low bias, and below 6 references the estimate is floored by the
  consistency-scaled mean absolute deviation, which can't collapse unless every value is
  identical: with three references a single outlier genuinely can't be identified as one, so the
  honest estimate keeps its influence rather than discarding it. Both corrections converge to the
  previous behaviour as n grows.
- The zero-point carries a **colour term**, not a single constant offset: a star's instrumental
  magnitude in R/B/V/I differs from its Gaia broadband G magnitude by an amount that depends on
  the star's own colour, so one median offset leaves a systematic bias that drifts night to night
  with whatever mix of red and blue reference stars the field supplied — enough to move many stars
  in one epoch together past `DELTA_MAG_ALERT` and read as a frame-wide variability signal (audit
  2026-08-18, finding H5). `_compute_zero_point()` fits `catalog_mag − mag_instrumental = zp + k ×
  (BP−RP − color_ref)` with 3σ-clipped passes (`PHOTOMETRY_COLOR_TERM_*`), reporting `zp` **at**
  `color_ref` (the reference set's own median BP−RP). A source whose Gaia BP−RP is known — carried
  on `_catalog_color`, set by `modules/catalog_matcher/_gaia.py` — gets `k` applied to it; one
  whose colour is unknown (every uncatalogued transient, every MPC object) uses `zp` bare, which
  amounts to assuming a typical colour for the field, and has `mag_err` widened by
  `|k| × color_scatter` rather than that assumption being left silent. The fit is skipped — falling
  back to the plain median, i.e. the previous behaviour exactly — when too few references carry a
  colour, when their colour span is too narrow to constrain a slope, or when the fitted `k` exceeds
  `PHOTOMETRY_COLOR_TERM_MAX`. `modules/forced_photometry.py` receives the same solution from
  `pipeline.py` (`_color_term`/`_color_ref`/`_color_scatter`, read off a measured source the way
  `zero_point` already is) and applies it identically.
- Adds the following fields to each source: `flux_aperture`, `flux_err`, `mag_instrumental`, `mag_calibrated`, `mag_err`, `snr`, `calibrated` (bool), `edge_flag`, `zero_point`, `zero_point_err`
- `flux_err` is `sqrt(|net_flux| / gain + ap_area × sky_sigma²)`. The aperture sum is in ADU,
  but photon shot noise is Poissonian in **electrons** — `N_e = net_flux × gain`, whose variance
  converts back to ADU as `N_e / gain² = net_flux / gain`. Using `net_flux` itself as the
  variance silently assumed exactly 1 e⁻/ADU, which real cameras almost never are (CCD ~0.5–2;
  CMOS from well under 1 to several), biasing every `snr` in the frame in one direction or the
  other (audit 2026-08-18, finding C7). `sky_sigma` needs no such conversion — it is the
  empirical per-pixel background scatter measured off this frame's own ADU values, so it already
  carries read noise and sky shot noise together in ADU. The gain comes from
  `_resolve_gain()`: an explicit `gain=` argument, else `config.PHOTOMETRY_GAIN_E_PER_ADU`, else
  the frame's own header — **`EGAIN` before `GAIN`**, the one place in the pipeline where that
  order matters, since on most CMOS cameras `EGAIN` is the true e⁻/ADU conversion while `GAIN`
  holds the camera's own gain *setting* in arbitrary vendor units (0–500 on a ZWO ASI). A value
  outside the plausible e⁻/ADU range is rejected with a warning and `1.0` used instead — feeding
  a gain setting into the formula would be far more wrong than the assumption it replaced.
  `modules/forced_photometry.py` duplicates the same helper and formula by hand, the convention
  that module already follows for the rest of this module's photometry math.
- `snr` is `flux_aperture / flux_err` — the same flux/noise convention already used by
  `modules/qc.py`'s `snr_median` and (as a cruder pixel-space proxy, before real aperture
  photometry has run) `modules/subtraction.py`'s own candidate `snr`. Computed here rather than
  reused from `astrometry.py`'s detection-time `peak / bkg.globalrms` significance, since that
  metric is tuned for star-vs-noise filtering (`STAR_SNR_MIN`), not for reporting the actual
  significance of the flux a source is ultimately photometered at. This step **overwrites** any
  provisional `snr` a source already carried — in practice, only an image-subtraction candidate
  merged in at `pipeline.py` step 7 carries one before this step runs — with the real
  aperture-photometry-derived value, so every source's `snr` in the API payload is computed the
  same way regardless of origin.
- A source carrying `saturated=True` (set by `astrometry.solve()`) is never measured — all of the
  fields above stay `None` for it, exactly as for an out-of-bounds source. Saturated sources are
  also excluded from the Gaia DR3 reference set used to compute the frame's zero-point, so one
  saturated "Gaia match" can't corrupt calibration for every other source in the frame. See
  docs/ISSUES.md #2.
- `measure(fits_path, sources, skip_calibration=False)` — `pipeline.py` passes `skip_calibration=True`
  whenever the frame's own filter is narrowband (`modules.normalizer.is_narrowband()`). Aperture
  photometry itself still runs; only the Gaia zero-point step is skipped unconditionally, so
  `calibrated` stays `False`/`mag_calibrated` stays `None` for every source regardless of how many
  Gaia DR3 matches happen to fall in the field — a narrowband zero-point is systematically biased
  relative to Gaia's broadband G even when ≥3 matches exist. See "Filters — real astronomy context"
  below.

### `modules/subtraction.py`
Image subtraction (difference imaging) — a second, independent detection path for transients
and moving objects that catalog cross-matching alone would miss (e.g. objects with no catalog
entry at all, at any position).

1. Looks in `/fits/archive/{object}/` for ≥`SUBTRACTION_MIN_FRAMES` previously archived frames
   of the same object (same filter preferred; falls back to any filter if there aren't enough
   same-filter frames). Candidates are also **screened on quality** (`_screen_by_quality()`):
   a frame whose stamped `QCFLAG` is anything but `OK` is excluded, as is one whose stamped
   `QCFWHM` exceeds the new frame's own measured FWHM by more than
   `SUBTRACTION_REF_MAX_FWHM_RATIO`. Selection used to be recency (plus PA-closeness) alone,
   which was harmless while QC-failed frames went to `/fits/rejected` — but they are archived
   now, into this very directory, and differencing a sharp frame against a blurred reference
   leaves the classic ring-shaped residual at every star plus a noise floor raised enough to bury
   the faint real transients (audit 2026-08-18, finding H10). Those two keywords are stamped into
   each frame's own header by `pipeline.py`'s `_write_qc_headers()` at archive time: the pipeline
   has no database and the API has no filesystem access, so the frame's header is the only place
   the two can meet. A frame carrying neither (archived before this existed, or placed there by
   hand) is kept, and if screening would leave fewer than `SUBTRACTION_MIN_FRAMES` the unscreened
   set is used instead with a warning. Both the frame type and the filter are read out of the filename
   **positionally** (`_parse_normalized_filename()`), not as a substring: the fields are anchored
   on the DateTime token and counted leftward from it, since the object name itself may contain
   underscores (`Andromeda_Galaxy`, `4_Vesta`). Two things this fixes (audit 2026-08-18, finding
   C5): **Dark/Flat/Bias frames are never eligible as references** — `pipeline.py` archives them
   into this same per-object directory, and a starless calibration frame would, being recent,
   also crowd genuine science frames out of the newest-first `_MAX_FRAMES` selection; and the
   filter field can no longer be confused with the frame-type field that used to share its
   alphabet (under the earlier `L`/`D`/`F`/`B` filename revision every Light frame carried `_L_`,
   so a request for Luminance returned the whole directory — Ha and OIII included — as a
   "same-filter" stack, compounding step 2.5's scale mismatch with a filter mismatch). Position
   disambiguates those legacy codes rather than rejecting them, so an archive written by the
   older revision still parses. A filename that doesn't follow the convention at all
   (`NORMALIZE_ENABLED=false`, or a file placed there by hand) keeps the old substring-based
   filter test and is never excluded as calibration — it can't be identified either way, and
   must not lose subtraction over it.
2. Aligns each reference frame to the new frame using `astroalign` (triangle-pattern matching —
   does not require WCS). Reference frames are handed to `astroalign` even when their pixel
   dimensions differ from the new frame's (e.g. archived with a different camera/resolution) —
   `astroalign` resamples onto the new frame's pixel grid regardless of the source's original
   shape, so a shape mismatch alone is not a reason to skip a candidate reference frame.
2.5. Normalizes each aligned reference onto the new frame's own photometric scale before it
   enters the stack: a frame's signal in ADU scales as `exposure_time / gain` (gain in e⁻/ADU),
   so the multiplier is `(t_new / t_ref) × (g_ref / g_new)` (`_flux_scale_factor()`). An object's
   archive routinely mixes exposure times (auto-exposure, a different session, a different
   camera profile), and stacking those in raw ADU leaves a residual of roughly `(K−1) × flux` at
   the position of **every** star in the frame once the stack is subtracted — hundreds of false
   `UNKNOWN`/`SPACE_DEBRIS` candidates, plus a raised noise floor hiding the genuine faint
   transients this module exists to find (audit 2026-08-18, finding C4). Each factor
   independently falls back to `1.0` when its keyword is missing on either side, so a
   header-poor archive behaves exactly as it did before this existed rather than losing
   subtraction entirely. `gain` prefers `EGAIN` over `GAIN` for the same reason
   `modules/photometry.py`'s `_resolve_gain()` does, and `PHOTOMETRY_GAIN_E_PER_ADU` deliberately
   doesn't override it here — a deployment-wide value is identical on both sides and cancels in
   the ratio. The scale is applied to the median stack only, never to the aligned references
   themselves: step 4 below compares those against `SATURATION_ADU`, and a scaled-down
   reference's saturated core would otherwise drop below that threshold and escape masking. The
   scaled reference's bias/sky pedestal survives as a smooth `(K−1) × pedestal` term, which step
   5's own `sep.Background()` pass removes before extraction.
3. Builds a per-pixel **median stack** of the aligned reference frames as the "reference image",
   then subtracts it from the new frame to get a difference image. The stack honours each
   reference's own `astroalign` **footprint** — the mask of target pixels astroalign could not
   fill from that source frame (the band a shift or rotation leaves empty, the region outside a
   smaller sensor's field). Those values are not measurements, and averaging them in put a step
   into the difference image that reads as a bright residual none of the saturation/streak/
   `near_edge` filters are looking for (audit 2026-08-18, finding H8). `_median_reference()`
   masks them out; a pixel no reference covered at all takes the new frame's own value, so the
   difference there is exactly zero and nothing can be detected in it. A reference whose
   footprint is missing or unusable counts as fully valid, i.e. the previous behaviour.
   **Non-finite** reference values are excluded by the same mask: `np.median()` propagates NaN
   rather than ignoring it, so one NaN pixel in one archived file (masked pixels from a previous
   calibration pass leave them) nulled the reference at that position — silently, and for every
   frame that archive is ever a reference for (audit 2026-08-18, finding H9). A pixel the *new*
   frame has no value for can't be repaired by any reference, so `run()` zeroes it in the
   difference and folds it into the same detection mask the saturated vicinity uses, keeping it
   out of `sep`'s background/RMS estimate for the whole frame.
4. Masks the vicinity (`SATURATION_MASK_RADIUS_ARCSEC`, converted to pixels via the frame's WCS
   plate scale, dilated with `scipy.ndimage.binary_dilation`) of any pixel at or above
   `SATURATION_ADU` in the new frame **or any aligned reference frame** — `astroalign` resampling
   leaves large non-Gaussian residuals around saturated stars even under near-perfect
   registration, which `sep` would otherwise report as spurious bright "transients". Masked pixels
   are zeroed in the background-subtracted diff image before extraction, so no candidate can be
   detected there. See docs/ISSUES.md #1, #2.
4.5. Also masks any streak-like feature found by the same coarse pre-pass `modules/astrometry/_streak.py`
   uses (`_build_streak_mask()`, duplicated here — `config.STREAK_*`), run over the diff image
   itself rather than the raw frame. A satellite trail present in the new frame but absent from the
   reference stack shows up in the diff image as a strong positive residual just like any other
   transient — and, left unmasked, fragments into dozens of separate elongated candidates rather
   than one, each individually classifiable by `anomaly_detector.py` as its own `SPACE_DEBRIS`
   anomaly (real incident, 2026-08-07, `T_CrB` test frames: 42 elongation>3 candidates strung along
   a single trail). This pre-pass's own `minarea` is hardcoded to `5` here — matching this module's
   own final detection pass below, **not** `config.SEP_MIN_AREA` (15, the main-frame extraction
   context in `modules/astrometry/_extraction.py`/`modules/qc.py`) — a coarser coarse-pass `minarea` than the
   real detection left small trail fragments invisible to the pre-pass while the real, more
   sensitive pass still detected them individually. Reduced the 42 false candidates to 21.
   `STREAK_DETECT_SIGMA`'s default (`3.0`, lower than `SUBTRACTION_DETECT_SIGMA`'s `5.0`) was tuned
   against this same real frame: at `5.0σ` the coarse pass still couldn't connect the (very faint)
   trail's brighter knots into long-enough coarse features, leaving 21 of the 42 candidates
   unmasked; at `3.0σ` only 1 remained. Once the mask exists, the background and RMS are
   **re-measured with it excluded** before the detection threshold is set. The mask can only be
   found on an already-background-subtracted image, so the first pass necessarily measured the
   RMS with the trail still in frame — and since the threshold is `SUBTRACTION_DETECT_SIGMA × rms`,
   one bright track quietly raised the bar for every faint real transient elsewhere in the same
   frame (audit 2026-08-18, finding H12). `modules/astrometry/_extraction.py` re-measures the same
   way, for the same reason: there the RMS is both the threshold's scale and the denominator of
   every source's SNR.
5. Detects sources on the (masked) difference image via `sep.Background` + `sep.extract`, with
   threshold `SUBTRACTION_DETECT_SIGMA × background_rms`. Each candidate's `snr` is
   `flux / (rms × sqrt(npix) × noise_corr)` — that last factor because `rms × sqrt(npix)` is the
   aperture noise only when neighbouring pixels' noise is independent, and interpolation makes it
   otherwise: `astroalign` resamples every reference onto this frame's grid (and
   `_prerotate_reference()` may interpolate once more before it), spreading each input pixel's
   noise across several output ones, so the aperture holds fewer independent measurements than
   pixels and the uncorrected figure overstates significance — worst exactly after a large
   pre-rotation (audit 2026-08-18, finding H13). `_noise_correlation_factor()` measures it from
   this frame's own difference image rather than assuming a value, by comparing its per-pixel
   MAD scatter against that of a box-averaged copy (independent noise falls as `1/box`; whatever
   it falls short of that is the correlation), capped by `SUBTRACTION_NOISE_CORR_MAX` — `1.0`
   restores the previous formula. `fwhm`/`elongation` per candidate are derived from `sep`'s `a`/`b` second-moment axes (same Gaussian approximation as `modules/astrometry/_extraction.py`), since `sep.extract()` doesn't return a native `fwhm` field.
5.5. Rejects any candidate whose `fwhm` is below `psf_fwhm_arcsec / 1.5` (converted to pixels via
   the frame's plate scale — same ratio `modules/astrometry/_extraction.py` uses for its own lower FWHM bound;
   see that module's section above), where `psf_fwhm_arcsec` is `pipeline.py`'s forwarded
   `qc.analyze()` measurement of this frame's actual stellar PSF. This exists because step 3's
   median reference stack only removes reference-frame artifacts that move between frames when
   `astroalign` resamples them onto the new frame's grid — a sensor hot/warm pixel is fixed to the
   *detector* grid, not the sky, so each reference's own copy of it lands at a different resampled
   pixel and gets averaged away, while the **new** frame's own hot pixel sits untouched at its
   native position and survives the subtraction as a sharp, undiffused positive residual with no
   real-star-like PSF profile at all (real incident, 2026-08-06, Vesta test frames — see
   `modules/astrometry/`'s section above for the same underlying failure mode). Skipped
   (behavior unchanged) when `psf_fwhm_arcsec` or the frame's plate scale isn't available.
6. Converts detected pixel positions back to (RA, Dec) using the frame's WCS — preferring the
   already-solved `wcs` passed in from `astrometry.solve()` (see `pipeline.py` step 7 above) over
   re-deriving one from the new frame's own header, which can still carry a stale/mount-pointing
   WCS at this point (`pipeline.py` only corrects the header at archive time, step 14.5 — see
   `modules/astrometry/`'s section below). `wcs=None` (e.g. a caller that never ran astrometry
   itself) falls back to reading whatever WCS the file's own header has.
7. Returns `{"performed": bool, "reference_frame_count": int, "candidates": [...]}`. Every
   candidate is tagged `_from_subtraction=True` so `anomaly_detector.py` can apply looser
   coverage rules to it (see below). Candidates whose pixel position falls within the
   `EDGE_MARGIN_FRAC` zone are held to a much higher bar: they survive only if they are both
   round (`elongation ≤ SUBTRACTION_EDGE_ELONGATION_MAX`) and strong
   (`snr ≥ SUBTRACTION_EDGE_SNR_MIN`). Coma and other off-axis aberrations change the PSF shape
   between frames (rotation, guiding, focus shift), so the median reference stack never perfectly
   cancels an edge star's coma wing and `sep` picks the leftover up as a spurious "new source" —
   real incident, 2026-08-10 analysis: 53 of 80 `UNKNOWN` alerts were
   `from_subtraction + near_edge`, every one a coma residual of an ordinary catalogued star.
   Rejecting the whole zone outright (the earlier behaviour) also meant a genuine transient
   landing near the edge — routine under a dithering pattern — could never be found by
   subtraction at all (audit 2026-08-18, finding H11). What separates the two is shape and
   strength, not position: an aberration stretches a PSF into an arc, and it is the *mismatch*
   between two such arcs that fails to cancel, so a residual is elongated and usually weak. Both
   bars are deliberately stricter than their whole-frame equivalents, because this is where the
   false positives concentrate. `modules/anomaly_detector/_classify.py` re-applies the identical
   test in `_survives_edge_zone()` rather than trusting this one — the standalone
   `DETECT_ANOMALIES` path reconstructs its sources from the API and may carry rows an earlier,
   looser revision wrote.

Gracefully skipped (`performed=False`) when fewer than `SUBTRACTION_MIN_FRAMES` archived frames
exist yet — e.g. the very first observations of a new target.

#### Camera rotation

Real investigation, 2026-08-14, source_id `6a7cfbae64e706.89320404` (a Gaia DR3 star, `catalog_id`
`3901066435010508672`, object `IC3322A`): frames of the same object taken on different sessions can
have a substantially different camera/rotator orientation (e.g. a meridian flip on a German
equatorial mount) — one session's frames rotated ~180° relative to another's.

**Catalog matching needs no fix at all here.** `astrometry.solve()` plate-solves every frame
independently from scratch (astap has no notion of "the previous frame's orientation"), so the
resulting WCS already fully encodes whatever rotation that particular frame has. `catalog_matcher`
cross-matches purely by (RA, Dec) — sky coordinates, not pixel geometry — so camera rotation is
invisible to it by construction. The DB confirms this: the star above matched to the *same*
`source_id` across sessions dated 2026-08-14, with (RA, Dec) consistent to well under 1″ throughout.

**Image subtraction (`modules/subtraction.py`) is the one place pixel geometry actually matters**,
since it differences raw pixels rather than comparing sky coordinates. `astroalign` already solves
for rotation as part of its triangle-matching registration — including a full 180° difference — so
a differently-rotated reference is never a reason to exclude it outright; doing so would be
wasteful (an archive that naturally splits into two roughly-180°-apart PA clusters could otherwise
never accumulate `SUBTRACTION_MIN_FRAMES` "acceptable" references). Two mechanisms address this
instead, both soft/best-effort — see that module's own section above for the exact functions:

1. **`position_angle_deg`** — every plate-solved frame's own orientation (0° = North up, increasing
   clockwise toward the image's own +X pixel axis) is derived from its solved WCS via a
   pixel↔world round trip (`_position_angle_deg()`, independently duplicated once in
   `modules/astrometry/_frame_geometry.py` and once in `modules/subtraction.py`, same convention as
   this codebase's other small duplicated geometry helpers — see that module's own section). It's
   returned by `astrometry.solve()`, threaded through `pipeline.py`'s frame payload, and persisted
   as a nullable `frames.position_angle_deg` column (docs/API.md §1) purely for operator diagnostics
   (e.g. spotting a ~180° difference between sessions without re-opening archived FITS files) — it
   never gates or excludes anything on the API side.
2. **Pre-rotation, not exclusion** — before handing a reference frame to `astroalign`,
   `subtraction.py`'s `_prerotate_reference()` coarse-rotates it by the *known* angle
   (`new_frame_PA − reference_PA`, via `scipy.ndimage.rotate`) toward the new frame's own
   orientation. This doesn't replace `astroalign`'s own fine, star-matching registration — it just
   gives it a near-zero residual angle to start from instead of a large, unknown one, which is both
   faster and less prone to a wrong/degenerate match on a sparse or partly-symmetric star field.
   Skipped entirely below `SUBTRACTION_PREROTATE_MIN_DEG` (not worth the interpolation cost for a
   negligible angle) or whenever either frame's WCS/PA is unavailable — never a hard failure.
   The rotation lets the canvas **grow** (`reshape=True`): rotating onto the same canvas is a crop
   for any angle that isn't a multiple of 90°, and since the gate is 2° it fires on modest field
   rotation (an alt-az mount without a de-rotator), not only on meridian flips — so the stars it
   discarded were the ones `astroalign` needs to find a transform at all, raising the failure rate
   exactly for the large-angle cases pre-rotation exists to help (audit 2026-08-18, finding H14).
   The constant fill that remains is marked as invalid rather than left as a hard zero/data
   boundary the median stack can't cancel: a validity map is rotated by the same angle, the
   reference is handed to `astroalign` as a masked array, and `propagate_mask=True` carries that
   marking through its own resampling into the footprint `_median_reference()` already excludes.
   `_find_archive_frames()` additionally uses PA-closeness as a *soft* tiebreaker when there are more
   candidate references than `_MAX_FRAMES` (a reference needing less correction is a marginally
   safer bet), never as a hard filter.

**What this does *not* fix**: coma/optical-aberration residuals near bright stars after
differencing are a property of the aberration pattern being fixed to the *sensor*, not the sky —
rotating the pixel data (by any method, pre-rotation or `astroalign` alone) necessarily moves that
pattern relative to the star field whenever two frames have a real orientation difference. Neither
mechanism above removes this; it's still mitigated the same way as always, downstream on the diff
image itself (`near_edge` exclusion, saturation masking, the streak/FWHM-floor filters — see this
module's own section above).

The investigation also surfaced an unrelated but related bug in `anomaly_detector.py`: a subtraction
candidate landing in a sky tile with no prior `POST /frames/covering/batch` record yet was
unconditionally classified `UNKNOWN` + alert regardless of `catalog_name` — so the Gaia star above,
recovered as a subtraction candidate (an ordinary registration residual near it) before its object's
first frame was ever marked "covered", fired a false `UNKNOWN` alert purely on that technicality. See
that module's section below and `docs/anomaly-detector.md` for the fix.

### `modules/catalog_matcher/`
A package, not a single file — one file per catalog (`_gaia.py`, `_simbad.py`, `_2mass.py`,
`_panstarrs.py`, `_mpc.py`, each holding that catalog's own query + match functions),
plus `_cache.py` (shared query cache), `_wcs_offset.py` (the vote-accumulator below), and
`_match.py` (the `match()` orchestrator itself, which calls each catalog through a
qualified submodule reference — `_gaia._query_gaia(...)`, not a bare `_query_gaia(...)` —
so that a test patching `modules.catalog_matcher._gaia.Gaia` reaches the actual call).
`__init__.py` re-exports `match()`/`get_gaia_stars()`/`get_mpc_objects()`, so every call
site elsewhere in this codebase is unchanged. Cross-matches the source list against
external catalogs using
`astropy.coordinates.SkyCoord.match_to_catalog_sky()` with cone radius `MATCH_CONE_ARCSEC`
(`MOVING_CONE_ARCSEC` for the MPC step, since moving objects shift between frames).

Every Gaia DR3 star is **proper-motion propagated** from its own `ref_epoch` (J2016.0 for DR3)
to the frame's `obs_time` before it is used for anything — once, in `match()`, so both the
WCS-offset accumulator and `_match_gaia()` see the corrected positions (`_gaia`'s
`_propagate_to_epoch()`). A high-proper-motion star has drifted several arcsec since DR3,
comparable to `MATCH_CONE_ARCSEC` itself; uncorrected it simply fails to match, ends up
`catalog_name=None`, satisfies the anomaly detector's "shifted" condition, and is reported
`MOVING_UNKNOWN`, while also voting for a wrong offset on behalf of every other source in the
frame (audit 2026-08-18, finding H1). The star dicts are copied rather than mutated — the query
returns the *cached* list, shared with frames of the same region at other epochs. A star with no
astrometric proper-motion solution, an unparseable `obs_time`, or a position at the pole keeps
its catalog position. `get_gaia_stars()` deliberately returns un-propagated positions, since
`modules/forced_photometry.py` applies the same correction itself.

Before matching, computes a **WCS offset correction**: an all-pairs vote-accumulator matches
the source list against Gaia DR3 to estimate a small systematic RA/Dec offset, then applies
that offset **in-place** to every source's `ra`/`dec` before the remaining catalogs are queried.
The correction is skipped only when the median source-to-Gaia separation is already ≤ 2″ (the
vote accumulator's own noise floor — below which it wouldn't apply a correction anyway); any
median_sep above that triggers the full vote-accumulator computation. After correction, a
validation pass re-measures median separation and logs it alongside the pre-correction value
for diagnostic comparison.

Catalogs queried **in this order** (sequential exclusive matching — once matched, a source
skips the remaining catalogs): **Simbad → Gaia DR3 → 2MASS → Pan-STARRS DR1 → MPC/SkyBot**.
Rationale: Simbad first gives correct `object_type` for known named objects (instead of generic
"STAR"); Gaia handles the bulk of stars; 2MASS catches red/cool stars faint in the optical;
Pan-STARRS DR1 pushes depth further for the remaining faint optical sources (mitigates, but
doesn't solve, the "faint UNKNOWN" problem — see Known Issues #1); MPC/SkyBot identifies moving
solar system objects at the observation epoch. Per-catalog source/access details and rate limits
are in "External Catalogs & APIs" below.

**MPC/SkyBot is the one exception to "exclusive".** It runs last but sees **every** source, not
just the unclaimed remainder — an asteroid projecting within `MATCH_CONE_ARCSEC` of a background
star (routine in a dense field or near the galactic plane) would otherwise be permanently tagged
`Gaia DR3`/`Simbad` before SkyBot ever got a look, losing its `ASTEROID`/`COMET` classification
and ephemeris, while the MPC object itself was handed to whatever *other* unclaimed source
happened to be nearest within the 120″ cone — a false stationary "asteroid" on top of the real
miss (audit 2026-08-18, finding C3). Conflicts are resolved positionally rather than by catalog
order: a source already claimed by a stellar catalog is taken over **only** when the ephemeris
position sits within the tight `MATCH_CONE_ARCSEC` of it (the blend case); beyond that, the MPC
object falls back to the nearest *unclaimed* source within `MOVING_CONE_ARCSEC` as before —
120″ is far too loose to justify overwriting an established identification, since at that radius
some catalogued star is almost always present regardless. A takeover logs both identities.

Each matched source is enriched **in-place** with `catalog_name`, `catalog_id`, `catalog_mag`,
`object_type` — its `ra`/`dec` fields are the already offset-corrected coordinates, there are
no separate `source_ra`/`source_dec` fields.
`catalog_mag` is G-band for Gaia, J-band for 2MASS, r-band for Pan-STARRS, `None` for Simbad/MPC.
Unmatched sources get `catalog_name = None`.

### `modules/forced_photometry.py`

**Forced photometry / reverse matching** (a.k.a. **precovery** for solar-system objects) —
implements a feature proposed as ROADMAP.md #1 (see git log for the design history). Where `catalog_matcher.py` above asks "what catalog object does this
detected source match?", this module asks the reverse question: "for every Gaia DR3 star / MPC
object in this frame's footprint, is there a detected source at its predicted position — and if
not, what's actually there anyway?" The single public entry point is
`await forced_photometry.run(fits_path, sources, gaia_stars, mpc_objects, wcs, naxis1, naxis2,
zero_point, zero_point_err, obs_time, psf_fwhm_arcsec=...) -> list[dict]`, called from
`pipeline.py`'s step 9.5, right after photometry's own zero-point calibration.

This closes two gaps forward matching alone leaves open:
- A star/object genuinely too faint for the blind SEP extraction's necessarily-high detection
  threshold (`SEP_DETECT_THRESH`, ~10σ by default) to have found at all. Forced photometry tests
  exactly one hypothesis (a specific known position) rather than scanning every independent
  resolution element in the frame for an unknown number of sources, so a much lower significance
  (`FORCED_PHOTOMETRY_MIN_SNR`, default 3.0) is statistically justified here — the same
  "look-elsewhere effect" argument behind why blind extraction's own threshold has to stay high.
- A star bright enough to detect that blind extraction's own star filter
  (elongation/FWHM/SNR bounds), a WCS residual, or streak masking happened to miss anyway — this
  pass recovers those "for free" since it never depends on `sep` having found the source in the
  first place; it only needs the catalog position and the frame's own WCS.

No new network queries: `gaia_stars`/`mpc_objects` are **not** re-fetched here — they're
`catalog_matcher.get_gaia_stars()`/`get_mpc_objects()`, thin public wrappers around that module's
own private, cached `_query_gaia()`/`_query_mpc()` (added specifically for this module to reuse,
without changing `match()`'s own return contract) — calling them right after `catalog_matcher.match()`
for the same field is a cache hit, so this pass costs zero extra Gaia/SkyBot round-trips. For each
eligible catalog entry not already present in `sources` (matched by `catalog_id`, built into a
lookup set before any pixel work starts):
- **Gaia DR3**: proper-motion-corrects the star's catalog position from its Gaia `ref_epoch`
  (`pmra`/`pmdec`, added to `_query_gaia()`'s output specifically for this) to the frame's actual
  `obs_time` before projecting to a pixel — a high-proper-motion star can have moved several
  arcsec since Gaia's own reference epoch. Falls back to the uncorrected position when a star has
  no astrometric proper-motion solution. Only stars within `FORCED_PHOTOMETRY_MAG_LIMIT` (a
  site-specific depth cutoff, same convention as `MPC_MAG_LIMIT`) are attempted — forcing every
  Gaia star down to its own ~21 mag completeness limit in a dense field would mean thousands of
  uninformative noise measurements.
- **MPC/SkyBot**: no proper-motion correction needed — `_query_mpc()` already returns each
  object's position computed at the exact observation epoch. No separate depth cutoff either:
  `_query_mpc()` already filters by `MPC_MAG_LIMIT` before these objects ever reach this module.

Aperture photometry at the predicted pixel reuses the same aperture/annulus-sizing and net-flux/
flux-error formulas as `modules/photometry.py` — including its gain-corrected Poisson term and
the `_resolve_gain()` helper behind it (see that module's section above; this is the module C7
hurt most, since an overstated `flux_err` understates the significance and drops real faint
recoveries against `FORCED_PHOTOMETRY_MIN_SNR`) — duplicated by hand rather than imported, the same
convention `modules/qc.py`/`modules/subtraction.py` already use for `astrometry.py`'s streak-mask
helper. A position is rejected outright (not reported at all) when its aperture would fall outside
the frame, or any pixel under it is at/above `SATURATION_ADU` — a forced measurement on a saturated
core is exactly as physically meaningless as it is for a blindly-detected source (see
`modules/photometry.py`'s section above). **A genuine non-detection (significance below
`FORCED_PHOTOMETRY_MIN_SNR`) is silently dropped, never reported as an "upper limit" magnitude** —
the wire schema (`POST /frames/{id}/sources`, docs/API.md §2) has no field to distinguish a real
magnitude from an upper limit, and adding one is a separate, cross-repo change to
observatory-api's schema, not made here.

Every recovered position comes back shaped exactly like an ordinary catalog-matched, photometered
source (`ra`, `dec`, `flux`, `catalog_name`/`catalog_id`/`catalog_mag`/`object_type`,
`flux_aperture`/`flux_err`/`mag_instrumental`/`mag_calibrated`/`mag_err`/`calibrated`, `zero_point`,
`near_edge`, `saturated=False`) plus an internal `_forced_photometry=True` marker — leading
underscore, so `api_client`'s `_to_wire_source()` strips it before the wire the same way it strips
`_from_subtraction`/`_source_id`; this flag is **not yet persisted** on the wire in this first pass
(a source recovered this way is currently indistinguishable, after the fact, from a blindly
detected one — see "Open considerations" in this feature's original ROADMAP.md proposal (see git log) for why this was deferred
rather than adding a new observatory-api column up front). Because these results already carry
`catalog_name`/`object_type`, they flow into `anomaly_detector.py`'s existing classification paths
unchanged — a recovered MPC object becomes an ordinary `ASTEROID`/`COMET` anomaly with ephemeris,
and a recovered Gaia star's magnitude joins the same historical Δmag comparisons any blindly
detected star's would, with no code changes needed in that module.

Scoped to Gaia DR3 + MPC/SkyBot only for now — 2MASS/Pan-STARRS forced photometry is a possible
future extension, not implemented. Gated end-to-end by `FORCED_PHOTOMETRY_ENABLED`. Best-effort
throughout: any failure (FITS I/O, WCS projection, missing catalog data) is logged and returns
`[]`, never raising — `pipeline.py`'s step 9.5 treats that identically to "nothing to recover" and
continues with whatever `sources` already had.

### `modules/anomaly_detector/`
Core logic. A package, not a single file — split by concern (`types.py`, `_otypes.py`,
`_geometry.py`, `_history.py`, `_movement.py`, `_prefetch.py`, `_classify.py`,
`_ephemeris_resolution.py`, `_detect.py`; see that package's own `__init__.py` docstring
for the exact map and [docs/anomaly-detector.md](docs/anomaly-detector.md) for the mechanics).
`__init__.py` re-exports `detect()`/`AnomalyType`, so every call site elsewhere in this
codebase still does `from modules import anomaly_detector; anomaly_detector.detect(...)`
unchanged. For all detected sources in a frame **at once** (batched, not one API round-trip per source):

Every returned anomaly dict includes `source_id` — the resolved `sources.id` read off the
source's `_source_id` key, which `pipeline.py`'s Step 12 attaches from the `source_ids` array
returned by `POST /frames/{id}/sources`. `None` when that round-trip couldn't resolve one
(post_sources failed, or the API predates this field).

1. **Query history via API** — `POST /sources/near/batch` with every source position in a single call, returning historical sources near each (RA, Dec) from previous frames. Queried for **every** source regardless of catalog-match status — this is what makes the Δmag-based classifications below (`VARIABLE_STAR`, `BINARY_STAR`, and the "already-known host brightened" path of `SUPERNOVA_CANDIDATE`) reachable at all for a catalog-matched source.
2. **Coverage check** — `POST /frames/covering/batch` — did we ever observe each sky position before? (batched the same way)
3. **Classify** each source. Real priority order in code: MPC/SkyBot match first → **if unmatched (`catalog_name is None`) and `saturated=True`, suppressed outright** (see below) → unmatched, no detection within `MATCH_CONE_ARCSEC` of this exact position, elongation above the trail threshold (a single-exposure trail — `SPACE_DEBRIS_ELONGATION_MIN`, or the higher `SPACE_DEBRIS_EDGE_ELONGATION_MIN` when the source is flagged `near_edge` — see below) → `SPACE_DEBRIS` immediately, no position-shift evidence required (see below) → position-shifted-but-unmatched, elongation at or below that same threshold (→ `MOVING_UNKNOWN`) → no historical coverage (→ `FIRST_OBSERVATION`, *unless* the source came from image subtraction — see below) → no prior detection at this exact position but near a Simbad galaxy (→ `SUPERNOVA_CANDIDATE`) → not in history or any catalog (→ `UNKNOWN`) → in catalog but not history (→ `KNOWN_CATALOG_NEW`) → **has** prior history and brightened beyond `DELTA_MAG_ALERT`: near a Simbad galaxy (→ `SUPERNOVA_CANDIDATE`) → known binary (→ `BINARY_STAR`) → known variable (→ `VARIABLE_STAR`) → nothing in any catalog explains it, but the source's own same-filter light curve does (→ `VARIABLE_STAR`, light-curve based):

| Situation | Classification |
|---|---|
| Unmatched (`catalog_name is None`) and `saturated=True` | Suppressed — `return None`, no anomaly record at all (bright-star/subtraction artifact, not a real transient; see docs/ISSUES.md #1, #2) |
| Unmatched (`catalog_name is None`) and `near_edge=True` | Suppressed — `return None` (coma shifts the measured centroid away from the star's true catalog position, making catalog matching miss it; these are overwhelmingly ordinary stars with optical distortion, not real transients — real incident, 2026-08-10: 27 of 80 UNKNOWN alerts were non-subtraction near_edge sources). **Exempt**: a subtraction candidate that is round and strong (`_survives_edge_zone()` — the same `SUBTRACTION_EDGE_ELONGATION_MAX`/`SUBTRACTION_EDGE_SNR_MIN` bar `modules/subtraction.py` applies at extraction). It is not the shape an aberration residual takes, and unlike an ordinary detection it carries pixel-level evidence that nothing was there before (audit 2026-08-18, finding H11) |
| No historical coverage | `FIRST_OBSERVATION` — not an anomaly, just note |
| No historical coverage, but the source was detected via image subtraction (`_from_subtraction=True`) and `near_edge=True` | Suppressed — `return None` (defense in depth for standalone `DETECT_ANOMALIES` re-runs; fresh subtraction applies the same test at extraction time), **unless** it is round and strong per `_survives_edge_zone()` |
| No historical coverage, source was detected via image subtraction (`_from_subtraction=True`), `near_edge=False`, and `catalog_name is not None` | Suppressed — `return None` (a known catalog object — most likely an ordinary astroalign registration residual near it, not a real transient; see "camera rotation" below. Real incident, 2026-08-14, source_id `6a7cfbae64e706.89320404`, a Gaia DR3 star — this branch used to ignore `catalog_name` entirely) |
| No historical coverage, source was detected via image subtraction (`_from_subtraction=True`), `near_edge=False`, and `catalog_name is None` | `UNKNOWN` → **ALERT** (subtraction already confirms it's absent from the reference stack, so missing API coverage doesn't downgrade it) |
| Area covered, source not in history at all, near a Simbad galaxy | `SUPERNOVA_CANDIDATE` → **ALERT** (new point source, no baseline to compare against) |
| Area covered, source not in history, found in catalog (not a galaxy) | `KNOWN_CATALOG_NEW` — was below detection threshold |
| Area covered, source not in history, not in any catalog, `near_edge=True` | Suppressed — `return None` (same coma-shifted-centroid rationale as above) |
| Area covered, source not in history, not in any catalog, `near_edge=False` | `UNKNOWN` → **ALERT** |
| Source **has** prior history, brightened by more than `DELTA_MAG_ALERT`, near a Simbad galaxy | `SUPERNOVA_CANDIDATE` → **ALERT** (already-known host got brighter) |
| Source in history, Δmag > DELTA_MAG_ALERT, known binary (Simbad) | `BINARY_STAR` |
| Source in history, Δmag > DELTA_MAG_ALERT, known variable (Simbad) | `VARIABLE_STAR` |
| Source in history, Δmag > DELTA_MAG_ALERT, no catalog classification that explains it, but the change exceeds `VARIABILITY_SIGMA` × the source's own same-filter historical scatter over ≥ `VARIABILITY_MIN_EPOCHS` epochs | `VARIABLE_STAR` (light-curve based — see below) |
| Source present but shifted > MATCH_CONE_ARCSEC, matches MPC | `ASTEROID` or `COMET` |
| Unmatched, no detection within `MATCH_CONE_ARCSEC` of this position, elongation > `SPACE_DEBRIS_ELONGATION_MIN` (3.0 default), or > `SPACE_DEBRIS_EDGE_ELONGATION_MIN` (6.0 default) when `near_edge=True` | `SPACE_DEBRIS` → **ALERT** (elongation alone is treated as sufficient trail evidence — no "vacated old position" proof required, see below) |
| Source present but shifted, not in MPC, `near_edge=True` | Suppressed — `return None` (coma shifts centroid between frames, creating false "position shifted" evidence) |
| Source present but shifted, not in MPC, `near_edge=False`, elongation ≤ 3.0 | `MOVING_UNKNOWN` → **ALERT** |

"Shifted" (for the unmatched `MOVING_UNKNOWN` branch specifically — MPC matches don't need this
check, and as of the fix below neither does `SPACE_DEBRIS`) requires **both**: no historical
detection within `MATCH_CONE_ARCSEC` of the source's *current* position, **and** a historical
detection within the wider `MOVING_CONE_ARCSEC` whose own position is no longer occupied by
anything else in *this* frame. Checking only the second half (an earlier revision's entire
condition) false-positived on almost every uncatalogued source: `MOVING_CONE_ARCSEC` (120″ by
default) covers enough sky that some unrelated historical detection — a neighbouring star, a
galaxy smudge, anything ever recorded nearby — is virtually always present there, whether or not
this particular source moved at all (real incident, 2026-08-06: several sources whose position
drifted by <1″ across epochs — ordinary centroid/seeing noise — were repeatedly flagged
`MOVING_UNKNOWN` solely because an unrelated star sat within 120″; see docs/ISSUES.md #1).
Requiring the *old* position to have actually emptied out rules that out while still catching real
movers, whose previous position is — by definition — vacated once they've moved away from it.

That wider cone is sized **per candidate**, not once for the whole search
(`_movement.py`'s `_find_wide_history()`/`_wide_cone_radius_arcsec()`): `MOVING_CONE_ARCSEC` is its
floor, extended to `MOVING_RATE_ARCSEC_PER_MIN × elapsed minutes` for a historical detection no
older than `MOVING_EXTEND_MAX_GAP_MIN`, capped at `MOVING_CONE_MAX_ARCSEC`. How far an object can
legitimately have moved between two frames is a rate times a time gap, not a constant — with a
fixed 120″ radius, anything that moved further than that between frames had its own previous
position outside the search entirely, so the second half of the evidence could never be satisfied
and a genuine fast mover fell through to plain `UNKNOWN` (no track chart, no ephemeris) or was
dropped as `FIRST_OBSERVATION` (audit 2026-08-18, finding H3). Both bounds exist because the
cone's false-positive risk grows with its area; without them the extension degenerates into a
permanently wide cone — exactly what the two-condition test above was added to stop. `_prefetch.py`
sizes its batch query off `MOVING_CONE_MAX_ARCSEC` accordingly, since a candidate the API never
returned can't be filtered back in client-side.

`SPACE_DEBRIS` deliberately does **not** wait for that second half of the evidence. A satellite or
debris trail's entire visible track — both "endpoints" — exists within a single exposure; unlike a
slow asteroid-like mover, it never had a *prior* detection anywhere nearby whose position could be
shown to have vacated, so gating it behind that same "shifted" proof meant a genuine trail could
never satisfy condition 2 and always fell through to generic `UNKNOWN` instead (real incident,
2026-08-07, `C_2020_R4_ATLAS` frames: several frame-spanning trails were reported `UNKNOWN` with a
`stamp_strip`/blink chart rather than `SPACE_DEBRIS` with a `track` chart, because nothing had ever
been detected near either end of the trail for the old code to show had "vacated"). For an
unmatched source with no detection at all within `MATCH_CONE_ARCSEC` of its current position
(condition 1 alone, still required), elongation above the trail threshold is treated as sufficient
evidence on its own of a single-exposure trail. A recurring elongated detection — e.g. a
diffraction spike or an uncatalogued extended object sitting at the exact same position every
frame, the opposite signature of a trail — still fails condition 1 and is unaffected, falling
through to the ordinary stationary-source classification further down.

That threshold is itself edge-aware: `SPACE_DEBRIS_ELONGATION_MIN` (3.0 default) for an ordinary
source, but the higher `SPACE_DEBRIS_EDGE_ELONGATION_MIN` (6.0 default) whenever the source is
flagged `near_edge` (set by `astrometry.py`/`subtraction.py` from the detection's own pixel
position vs. `EDGE_MARGIN_FRAC` — see those modules' sections above). Coma and other off-axis
aberrations progressively stretch a perfectly ordinary, non-moving star's PSF toward the
edges/corners of a wide-field frame, inflating its measured elongation for purely optical
reasons — real incident, 2026-08-07: 4 `T_CrB` frames produced 305 anomalies, the vast majority
being coma-elongated but otherwise ordinary corner stars firing this exact shortcut with no real
motion at all. A genuine single-exposure satellite/debris trail is typically far more elongated
than coma alone produces, so raising the bar near the edge (rather than removing the
elongation-alone shortcut there entirely) keeps real edge-of-frame trails detectable while
filtering out the aberration.

The saturated-artifact suppression is deliberately scoped to `catalog_name is None`: a saturated
source that *is* MPC- or Simbad-matched (a genuinely bright asteroid, a known star flaring) is a
legitimate detection and is still classified normally — just without a usable `magnitude`, since
`photometry.py` never measures a saturated source (see that module's section above).

`SUPERNOVA_CANDIDATE` therefore has two independent triggers: a brand-new point source with no
prior detection at all near a known galaxy, and an already-catalogued/known galaxy that
*brightens* (not dims — a fading foreground star near a galaxy is not a supernova signature) by
more than `DELTA_MAG_ALERT`. Both use the same `MATCH_CONE_ARCSEC` (5″ by default) "near galaxy"
radius as ordinary star matching — there is no separate, wider radius for extended galaxy disks.

Magnitude comparisons (`delta_mag`) read the `mag` field that `pipeline.py` populates right
after `photometry.measure()` (see that module's section above) — `photometry.py` itself only
ever sets `mag_instrumental`/`mag_calibrated`, never `mag`. `mag` is `None` whenever the source
wasn't calibrated (see that module's section above), so an uncalibrated source's `delta_mag`
is always `None` too — it correctly never triggers `VARIABLE_STAR`/`BINARY_STAR`/the
brightening branch of `SUPERNOVA_CANDIDATE` rather than firing on a meaningless number.

`_same_filter_history()` further restricts which historical detections `median_hist_mag` (and
therefore `delta_mag`) is computed from, to only those carrying the *same filter* as the current
source — comparing an L-band magnitude against an old R-band or Hα epoch is a color-term artifact,
not real variability (see "Filters — real astronomy context" below). The current source's own
filter travels as `_filter` (attached by `pipeline.py`'s Step 5.5 in-process, or by
`_from_wire_source()` from the parent frame's own filter for the standalone
`detect_anomalies_for_frame_id()` path); each historical detection's filter comes from
`POST /sources/near/batch`'s `filter` field (docs/API.md), joined server-side from the frame that
produced it (`source_observations` itself has no filter column). This restriction is scoped to the
magnitude comparison only — the **existence** check (`history`/`n_history`, used for
`FIRST_OBSERVATION`/`UNKNOWN`/`KNOWN_CATALOG_NEW` above) stays filter-agnostic, since a position
already detected in a different filter is still a real prior detection, not a new source.

The `VARIABLE_STAR`/`BINARY_STAR`/brightening-`SUPERNOVA_CANDIDATE` branches all gate on
`object_type`, which only `modules/catalog_matcher/_simbad.py` ever fills with a real Simbad
OTYPE; `_gaia.py`, `_2mass.py` and `_panstarrs.py` hardcode the generic
`"STAR"`, which no OTYPE classifier matches. A star known solely through Gaia DR3 (the
overwhelming majority of any field) could therefore change brightness by several magnitudes and
be dropped silently, leaving the Δmag detector able only to re-confirm variability Simbad already
knew about — never to discover any (audit 2026-08-18, finding C1). A final, catalog-independent
branch closes that: a source whose own same-filter history spans at least
`VARIABILITY_MIN_EPOCHS` (3 by default) epochs and whose `delta_mag` exceeds `VARIABILITY_SIGMA`
(3.0) times that history's own robust scatter — `_history.py`'s `_history_mag_scatter()`, a
MAD-derived 1σ equivalent, chosen over an RMS so that one bad epoch through cloud can't inflate
the baseline enough to mask the very change it calibrates — is reported as `VARIABLE_STAR`
regardless of what (if anything) the catalogs call it. Screening against the source's *own*
scatter rather than a flat threshold is what keeps an intrinsically noisy source (low SNR,
blended neighbour, variable seeing) quiet: a large `delta_mag` is unremarkable against a large
scatter. `DELTA_MAG_ALERT` still applies on top as an absolute floor, and the anomaly's `notes`
field states that the classification came from the light curve rather than a catalog — the
distinction is not carried by `anomaly_type`, since the enum is mirrored as an `ENUM` column
constraint in observatory-api and a new member can't be added from this repository alone.

4. For `ASTEROID` / `COMET`: calls `ephemeris.py` to compute current ephemeris via JPL Horizons.

`FAINT_UNCATALOGUED` (proposed in Known Issues #1) is **not implemented** — it's still only a
`TODO` comment in the source.

These 10 `anomaly_type` values are defined as `AnomalyType(str, Enum)` in this module (a `str`
mixin, so it still serializes/compares as a plain string everywhere) and mirrored as an `ENUM`
column constraint on `observatory-api`'s `anomalies.anomaly_type` (also in
`AnomalyModel::ALLOWED_TYPES`); `FramesController::saveAnomalies` rejects any anomaly with an
unrecognized `anomaly_type` with `400` before inserting anything from that batch. The two lists
must be kept in sync **by hand** — adding `FAINT_UNCATALOGUED` later means updating both the
Python enum and the API's migration/model together.

A full deep-dive into this module's batch prefetch strategy and classification flowchart lives
in **[docs/anomaly-detector.md](docs/anomaly-detector.md)**.

### `modules/ephemeris.py`
- Queries JPL Horizons via `astroquery.jplhorizons`
- Given MPC designation + observation time → returns predicted (RA, Dec, mag, distance_au, angular_velocity)
- Results included in the anomaly payload sent to API
- astroquery's `Horizons` client is fully synchronous and carries no timeout of its own, so the
  blocking call runs via `asyncio.to_thread()` under an `EPHEMERIS_TIMEOUT_SEC` budget — without
  it, an unresponsive Horizons stalls the worker's whole event loop (and made
  `_ephemeris_resolution.py`'s `asyncio.gather()` concurrent in name only). A timeout, like any
  other failure here, returns `None`: an ephemeris is supplementary detail on an anomaly that was
  already classified without it. `_resolve_ephemerides()` gathers with `return_exceptions=True`
  so that one designation's failure costs only that anomaly its ephemeris — anything escaping
  `query()`'s own `except Exception` used to propagate out of `detect()`, and `pipeline.py` would
  then post an **empty** anomaly list, which *replaces* the frame's whole anomaly set (audit
  2026-08-18, finding C8).

### `modules/finder_chart/`
A package, not a single file — split one file per chart variant, plus shared infrastructure
(`_style.py` picks which variant a source's anomaly_type(s)/epoch-count resolve to — routing only,
no rendering; `_io.py` holds FITS loading, the display stretch, and PNG/GIF assembly shared by 2+
variants). Five renderable variants, each its own file: `_style_track.py` (static "track"),
`_style_track_gif.py` ("track_gif"), `_style_stamp_strip.py` (static "stamp_strip"),
`_style_stamp_strip_gif.py` ("stamp_strip_gif"), `_style_before_after.py` ("before_after" — no GIF
counterpart). Each `*_gif.py` is its own independent renderer, **not** a wrapper that re-calls its
static sibling's render function per epoch-count subset — that was the original approach and had
two real problems: (1) the static chart's own figure height grows with epoch count (room for its
bottom legend), so a GIF assembled from N differently-sized PNGs got every frame past the first
silently cropped to the first frame's own canvas size when Pillow composited them; (2) every frame
reused one single background image with only synthetic markers changing, rather than each frame
showing that epoch's own real pixel data. `_style_track_gif.py`/`_style_stamp_strip_gif.py` fix
both: one fixed canvas size for every frame, no bottom legend (replaced by a short one-line
per-frame caption), and each frame cropped from that epoch's own actual FITS data — `_style_track_gif.py`
crops every frame to one shared, fixed sky window (computed once from all epochs) so the star
field itself visibly shifts between real exposures, the way an actual discovery blink comparator
works, while still drawing the cumulative marker trail (epochs 1..k) projected into each frame's own
WCS. `__init__.py` keeps `_render_charts_for_source()`/`update_charts_for_sources()` as the
orchestrator itself (same convention as `modules/astrometry/`'s `solve()`) rather than moving them
to their own file — `tests/test_finder_chart.py` patches `_render_track_chart`/`_render_stamp_strip`/
`_render_track_gif` as bare attributes directly on the package, which only takes effect for code
that resolves those names through `__init__.py`'s own namespace, i.e. code defined directly in
`__init__.py` — and re-exports `update_charts_for_sources`, so every call site elsewhere in this
codebase is unchanged.

Per-source finder/discovery chart generation — for an anomaly with a resolved `source_id`,
builds a small PNG visualizing every frame that source has ever been detected on, with its
position marked on each, and uploads it to the API. The chart is always fully regenerated from
the source's complete track (never patched in place), so each new epoch simply produces an
updated image with one more mark on it — see pipeline.py Step 15.

Two rendering styles, chosen by `anomaly_type`:

| Style | Anomaly types | What it shows |
|---|---|---|
| `track` | `ASTEROID`, `COMET`, `MOVING_UNKNOWN`, `SPACE_DEBRIS` | A crop of the most recent epoch's own frame, zoomed to the epoch cluster (not the whole frame — a full wide-field frame scaled down to figure size makes a slow mover's few-dozen-pixel drift between epochs invisible; the crop half-size is whichever is bigger: a generous fixed context window, or the cluster's own footprint plus margin, so a genuinely wide multi-epoch trail still renders in full) with a colored filled marker at every epoch's true position, using a color gradient from cool (oldest) to warm (newest) so time progression is visible at a glance. Markers are connected by gradient-colored track-line segments with a direction arrowhead on the last segment, immediately showing motion direction. Each marker carries a compact date+time label (and magnitude when available) in a color-matched badge at the end of a thin leader line, spread evenly around the point cluster's centroid to avoid collision. Every epoch's (RA, Dec) is converted into the *background* epoch's WCS pixel grid via `WCS.world_to_pixel()` — no pixel-level alignment between frames is needed, only a per-epoch coordinate transform. Each epoch's RA/Dec is listed in a small monospace legend under the image, together with angular separation, time gap, and angular velocity (″/hr) from the previous epoch — the single most useful number for judging a mover's nature at a glance. |
| `stamp_strip` | everything else (`SUPERNOVA_CANDIDATE`, `UNKNOWN`, `VARIABLE_STAR`, `BINARY_STAR`, `KNOWN_CATALOG_NEW`, `FIRST_OBSERVATION`) | One small crop per epoch, centred on that epoch's own detected position using that frame's own WCS, each circled and labelled with its timestamp, magnitude, and RA/Dec — a "blink" before/after strip for a source that isn't expected to move. |

Both styles' chart title is either just `anomaly_type`, or — when the underlying source is
catalog-matched — `anomaly_type` plus its resolved catalog designation in parentheses, e.g.
`ASTEROID (Vesta)` or `VARIABLE_STAR (TYC 1430-1407-1)`. An uncatalogued source's chart keeps the
bare `anomaly_type` title.

**A source can hold both charts at once.** `modules/anomaly_detector/` classifies a source
independently on every frame it appears on, so the same `source_id` can accumulate anomalies of
more than one `anomaly_type` over its lifetime — e.g. `UNKNOWN` on the frame it was first seen (no
history yet), `MOVING_UNKNOWN` on a later frame once it had moved. Real incident, 2026-08-11:
source_id `6a7be36b4d7578.98132403` had 12 `MOVING_UNKNOWN` + 1 `UNKNOWN` anomalies, but
`GENERATE_CHARTS` only ever produced a single chart, because both the API's task-creation logic
and this module collapsed a source down to one arbitrary `anomaly_type` before rendering. Fixed on
both sides: observatory-api's `Web\AnomaliesController::createTask()` now submits one task item
per distinct `anomaly_type` within a group (see "Job queue" above), and this module renders one
chart per distinct *style* those types resolve to — `_group_types_by_style()` partitions a
source's (deduplicated) `anomaly_types` list into style groups, and each group gets its own
render + upload, sharing the same already-loaded epochs. `observatory-api`'s `source_charts` table
was migrated to match: `UNIQUE(source_id, style)` instead of `UNIQUE(source_id)`
(`2026-08-11-000001_SourceChartsUniqueByStyle.php`), so a "track" and a "stamp_strip" chart for the
same source_id coexist rather than one overwriting the other.

The single public entry point,
`update_charts_for_sources(anomaly_types_by_source_id, designation_by_source_id=None)`, takes every
(source_id → [anomaly_type, ...]) pair for one call at once (see pipeline.py Step 15), so it can
fetch every source's track and upload every chart in one HTTP round trip each, regardless of how
many anomalies/styles are involved. A list entry of `None` is valid (a chart requested directly by
source_id, with no anomaly at all — see "Job queue" above); duplicate entries in a source's list
are harmless, deduplicated internally. `designation_by_source_id` is optional and keyed by plain
source_id (not by type) — built by pipeline.py from `sources`' own `catalog_name`/`catalog_id`
(already resolved by catalog matching, Step 8), not queried by this module itself; a source_id
absent from it gets the bare-`anomaly_type` title on every one of its charts.

Steps:
1. `api_client.get_source_tracks_batch(source_ids)` → `POST /sources/tracks/batch` — every
   requested source's full chronological position track in one call: for each source, every
   frame it was observed on, with the (RA, Dec) it was actually detected at *on that specific
   frame* (a moving object's position differs epoch to epoch). A source_id absent from the
   response (unknown to the API, or empty track) is treated the same as an empty track.
2. Per source: caps to the most recent `CHART_MAX_EPOCHS` (oldest dropped) to bound image size
   and local FITS I/O.
3. Per source: locates each epoch's FITS file locally at `FITS_ARCHIVE/{object}/{filename}` and
   loads its pixel data + WCS. Epochs whose file is missing locally (e.g. archive rotated/pruned)
   are skipped rather than failing that source's whole chart. This is why `pipeline.py`'s archive
   move (step 14.5) must run *before* this step: the current frame's own epoch is looked up at
   this same path. The loaded epochs are shared across every style this source ends up rendering —
   loaded once, not once per style.
4. Per source, per distinct style implied by its `anomaly_types`: renders the PNG (`track` or
   `stamp_strip`, per that group's representative anomaly type — the first non-`None` entry in the
   group) using `matplotlib` with a zscale + asinh stretch (`astropy.visualization`) — the standard
   DS9-style display stretch.
5. Per rendered chart: `api_client.upload_source_chart(source_id, png_bytes, style, frame_count)`
   → `POST /sources/{id}/chart` — uploaded individually as raw PNG bytes, replacing any previous
   chart of that SAME style for that source (a different style already stored for the same
   source_id is left untouched — see observatory-api's `SourceChartModel` docblock).

Gated by `CHART_ENABLED` (default `true`). Best-effort throughout: for a given source_id, a
missing local file, an API error, or a rendering failure is logged and downgrades to `False` only
the specific `anomaly_type` entries covered by that failed style — returned as a nested
`dict[str, dict[Optional[str], bool]]` (source_id → anomaly_type → success), not a flat
`dict[str, bool]`, since two anomaly_types for the same source can now resolve to two independent
upload outcomes. Never raises, and a failure never affects any other source_id or style in the
same call (pipeline.py's Step 15 calls this once per frame with every anomaly's source_id, deduped
per frame, unconditionally) or frame processing overall.

**Animated GIF companions** (`CHART_GIF_ENABLED`, default `true`): whenever a source's chart style
is `track` or `stamp_strip` (2+ epochs — `before_after` has no animated counterpart, since a
single-occurrence source has at most two still images to begin with), a matching animated GIF is
also rendered and uploaded as its own chart, keyed by its own style: `track_gif` (cumulative
reveal — frame *k* re-renders the `track` chart using only its first *k* epochs, so the trail grows
one marker per frame) or `stamp_strip_gif` (one epoch's own crop per frame — an actual "blink"
instead of the static side-by-side grid). Both reuse `_render_track_chart()`/`_render_stamp_strip()`
as their own per-frame renderer (no separate drawing code) and are assembled into a looping GIF via
Pillow (`_pngs_to_gif()`, `CHART_GIF_FRAME_DURATION_MS` per frame). The GIF is a bonus asset:
`update_charts_for_sources()`'s return value reports only the static chart's own outcome, and a
GIF render/upload failure is logged and otherwise ignored — it never downgrades an anomaly_type's
already-successful result. On observatory-api's side this needed `source_charts.style` to accept
`track_gif`/`stamp_strip_gif` (`2026-08-11-000002_AddGifStylesToSourceCharts.php`) and
`SourcesController::uploadChart()`/`chart()` to stop hardcoding `.png`/`image/png` — see that
repo's `CLAUDE.md`. `api_client.upload_source_chart()` sets the outgoing `Content-Type` from the
image bytes' own magic number (`_content_type_for_image_bytes()`), not from `style`, so it needs
no signature change to carry either format.

**Camera rotation** (see the top-level "camera rotation" discussion under `modules/subtraction.py`
above): `stamp_strip`, `stamp_strip_gif`, and `track_gif` each draw a crop taken from a DIFFERENT
epoch's own raw pixel data — unlike `track`, which only ever shows ONE epoch's own pixels and
projects every other epoch onto it purely as a sky-coordinate marker (orientation-independent by
construction). A camera/rotator orientation difference between epochs (e.g. a meridian flip
between sessions) would otherwise show the star field visibly rotated/flipped between stamps or
animation frames — defeating the entire point of a "blink comparator", where only the
astrophysical content is supposed to change. `modules/finder_chart/_io.py`'s
`_prerotation_delta_deg()`/`_rotate_crop()`/`_rotate_point_in_crop()` (same `_position_angle_deg()`
formula/sign-convention as `modules/astrometry/`'s and `modules/subtraction.py`'s identical copies)
coarse-rotate each non-reference epoch's crop — and any marker/track point drawn on it — toward one
shared reference orientation before drawing: the most recent epoch's own WCS, the same "background"
convention `track`/`track_gif` already use elsewhere. Gated by `CHART_PREROTATE_MIN_DEG` (default
2°) — skipped for a negligible difference, same as `modules/subtraction.py`'s
`SUBTRACTION_PREROTATE_MIN_DEG`. A large orientation difference (e.g. ~180°) is never a reason to
drop an epoch from a chart, only to rotate its crop before display.

### `api_client/`
All communication with the remote `observatory-api`. A package, not a single file — split by
REST resource, mirroring docs/API.md's own section grouping: `frames.py` (§1, 5, 7, 12, 13),
`sources.py` (§2, 4, 6, 8, 9, 11), `anomalies.py` (§3), `tasks.py` (§14), `settings.py` (§16), and
an internal `_shared.py` for the retry decorator, `AsyncClient` factory, and batch-response
normalization every one of those files uses. `__init__.py` re-exports every public function, so
every call site elsewhere in this codebase does a plain `import api_client;
api_client.post_frame(...)` — the split into several files is invisible outside this package.
Uses `httpx` with async support and `tenacity` for automatic retry on transient failures
(HTTP 5xx and transport/timeout errors — never on HTTP 4xx). Sends `X-API-Key`,
`Content-Type: application/json`, `Accept: application/json` on every request. Exact retry
parameters and endpoint request/response shapes are documented once, in
**[docs/API.md](docs/API.md)** — not repeated here.

Besides the batched endpoints `anomaly_detector.py` and `finder_chart.py` actually call
(`/sources/near/batch`, `/frames/covering/batch`, `/sources/tracks/batch`),
the client still implements/exports their older single-position/single-source counterparts
(`/sources/near`, `/frames/covering`, `/sources/{id}/track`) — kept for
API completeness, no longer called from this codebase. `finder_chart.py` uploads each chart
individually via `POST /sources/{id}/chart` (one request per source_id).

Also implements the frame-listing (`get_frames`, `get_frame`, `get_frame_sources`) and task-queue
(`create_task`, `get_tasks`, `get_task`, `update_task`, `post_task_items_progress`) functions that
back `pipeline.detect_anomalies_for_frame_id()` and `worker.py` — see docs/API.md sections 13–14
and "Job queue" above.

`post_sources()`'s internal `_to_wire_source()` translates each source dict into the wire shape
before sending: renames `_from_subtraction` (leading underscore — this codebase's convention for
"internal, not for the wire") to `from_subtraction`, and strips every other leading-underscore key
(`_source_id`, `_wcs_offset_ra`, `_wcs_offset_dec`, ...). Only adds `from_subtraction` to the wire
dict when the source actually carries `_from_subtraction` truthy, so a source with no such key at
all (the normal case for anything from `astrometry.py`) travels unchanged — the API defaults an
omitted `from_subtraction` to `false` itself.

The pipeline treats the API as a black box. If the API changes its DB schema internally,
the pipeline only cares that the endpoint contracts remain stable.

---

## File Organization by Target Object

Frames are organized into subdirectories based on the `OBJECT` FITS header keyword (see
README.md → "File Organization" for the directory layout example).

**Directory naming rules:**
- Object name is sanitized: spaces → underscores, special chars removed
- If `OBJECT` header is missing or empty → use `_UNKNOWN`
- Directories are created automatically if they don't exist

---

## Key Astronomical Concepts

### Plate solving
Determining the exact celestial coordinates of a FITS frame by matching detected star
patterns against a star catalog. Tool: `astap` (offline, fast, ~2–5 sec).
Requires local star catalog files (D50 = 50M stars, ~8 GB, or H18 for smaller FOV).
Result: WCS (World Coordinate System) header embedded in the FITS file.

### FWHM (Full Width at Half Maximum)
Measure of star sharpness in arcseconds. Larger FWHM = blurrier stars.
Caused by: poor focus, atmospheric seeing, or optical aberrations.
Threshold: `QC_FWHM_MAX_ARCSEC` (default 8.0″, adjust for your telescope).

### Elongation
Ratio of major to minor axis of a star's PSF ellipse. Should be close to 1.0 for round stars.
Values > 2.0 indicate trailing (telescope tracking problem) or strong coma. In `anomaly_detector.py`
a separate elongation threshold of `3.0` also decides `SPACE_DEBRIS` vs `MOVING_UNKNOWN` for
unmatched moving sources (see that module's section above).

### Image subtraction (difference imaging)
Aligning and subtracting a stack of previously archived reference frames of the same field from
a new frame to reveal only what changed — transients, moving objects, and variable stars — without
depending on any external catalog. Implemented in `modules/subtraction.py` using `astroalign`
for alignment and `sep` for detection on the difference image. Complements catalog cross-matching,
which can only flag "not in any catalog", not "genuinely new pixel-level change".

### Cone search
Spatial query: find all objects within N arcseconds of a given (RA, Dec) point.
Implemented in the API using: `WHERE ra BETWEEN (ra-r) AND (ra+r) AND dec BETWEEN (dec-r) AND (dec+r)`
(box approximation, fast with indexed columns, accurate enough at small radii).
For precise spherical distance, use Haversine formula in application code.

### Differential photometry
Measuring a star's brightness relative to nearby reference stars in the same frame.
Makes measurements immune to atmospheric transparency variations.
Reference stars come from Gaia DR3 catalog.

### Ephemeris
Predicted position of a solar system object (asteroid, comet, planet) at a given time.
Computed via JPL Horizons API. Inputs: MPC designation + time. Outputs: RA, Dec, magnitude,
distance, angular velocity.

### Filters — real astronomy context

A monochrome camera shoots the exact same field through several different filters — this
pipeline never gates *whether* a Light frame gets analyzed on which one it used (that's decided
purely by `IMAGETYP` — see `pipeline.py` above). What the filter *does* change is which parts of
the analysis its results can be trusted for:

- **Broadband** — Johnson-Cousins `U`/`V`/`I` (`u'`/`g'`/`r'`/`i'`/`z'` are the SDSS analogs),
  and `L`/Luminance/Clear (panchromatic — the closest analog to Gaia's own broadband G-band).
  Used for star fields; astrometry, catalog matching, and Gaia zero-point calibration all work
  normally.
- **Narrowband** (`Ha`, `OIII`, `SII`, `NII` — `config.NARROWBAND_FILTERS`) — isolates one
  emission line (e.g. Hα at 656.3 nm) for imaging nebulae/emission regions. Only the sliver of a
  star's continuum that falls inside that narrow bandpass leaks through, so a narrowband frame of
  the *same field* genuinely contains far fewer, fainter stars than a broadband one of it — this
  is expected, not a quality problem with the frame.

**Color term:** a star's brightness in filter R differs from its brightness in filter G (or in
Gaia's broadband G) purely from its temperature/color, independent of anything actually changing.
Every serious time-domain survey (ZTF, LSST, ...) therefore keeps light curves **per filter** —
comparing an L-band magnitude against an old R-band or Hα epoch of the same object reads as a
brightness change that is really just a filter swap. This is why real, production-grade filter
handling looks different at each pipeline stage rather than a single global gate:

| Stage | Filter-dependent? | What this pipeline does |
|---|---|---|
| QC star-count floor | Yes | `modules/qc.py` uses the softer `QC_STARS_MIN_NARROWBAND` instead of `QC_STARS_MIN` when the frame's filter is narrowband (`modules.normalizer.is_narrowband()`) — the broadband floor would reject good narrowband data as `LOW_STARS` |
| Astrometry / plate solving | No | Works off however many stars are actually detected, whatever the filter |
| Catalog matching (by RA/Dec) | No | Position-based cross-matching doesn't care what filter produced the position |
| Gaia zero-point calibration | Yes | `modules/photometry.py`'s `skip_calibration` (set by `pipeline.py` from `is_narrowband()`) skips it unconditionally on a narrowband frame — too few Gaia-bright stars pass through the bandpass, and even a zero-point computed from the few that do is systematically biased relative to Gaia's broadband G, regardless of match count |
| Subtraction (differencing) | Yes, and already filter-aware | `modules/subtraction.py` matches its reference stack by filter (see that module's section above) — same-filter differencing is valid and is in fact the *best* transient signal available on a narrowband frame, since it needs no cross-filter magnitude comparison at all |
| Anomaly Δmag comparison | Yes | `modules/anomaly_detector/_history.py`'s `_same_filter_history()` restricts the historical magnitude used for `VARIABLE_STAR`/`BINARY_STAR`/the brightening branch of `SUPERNOVA_CANDIDATE` to detections carrying the *same* filter as the current source (via each source's `_filter`, and each historical detection's `filter` — see `POST /sources/near/batch` in docs/API.md). The **existence** check (whether this position has ever been detected before, at any point in `FIRST_OBSERVATION`/`UNKNOWN`/`KNOWN_CATALOG_NEW`) stays filter-agnostic on purpose — an ordinary LRGB sequence re-images the same field in 3-4 different filters per session, and a position already seen in R must not look "brand new" the moment an L-filtered frame comes in |

Position-only classifications (`ASTEROID`/`COMET`/`MOVING_UNKNOWN`/`SPACE_DEBRIS`, and `UNKNOWN`
via subtraction) never depend on magnitude at all, so they are unaffected by any of this — a
moving object is a moving object regardless of what filter caught it moving.

---

## External Catalogs & APIs

Catalog matching order and rationale are covered under `modules/catalog_matcher/` above; this
is the per-catalog reference (source, depth, access method, rate limit).

### Simbad
- Source: CDS Strasbourg (Centre de Données astronomiques de Strasbourg)
- Content: named astronomical objects — variable stars, double stars, galaxies, nebulae, quasars, etc.
- Access: `astroquery.simbad.Simbad.query_region()`
- Rate limit: ~5–6 req/sec (shared CDS infrastructure); 1-hr cache is sufficient

### Gaia DR3
- Source: ESA Gaia mission, Data Release 3
- Content: ~1.8 billion stars with precise positions, proper motions, G-band magnitudes; complete to ~mag 20–21
- Access: `astroquery.gaia.Gaia.cone_search()`
- Rate limit: no hard limit; queries take 1–5 s; 1-hr cache is sufficient

### 2MASS (Two Micron All Sky Survey)
- Source: IPAC / NASA; catalog hosted on VizieR (CDS)
- Content: ~470 million point sources to K≈14.3 / J≈15.8
- Access: `astroquery.vizier.Vizier.query_region(catalog="II/246")`
- Rate limit: same CDS infrastructure as Simbad; 1-hr cache is sufficient

### Pan-STARRS DR1
- Source: Pan-STARRS1 Surveys (University of Hawaii); catalog hosted on VizieR (CDS)
- Content: ~3 billion optical sources over δ > −30°, deeper than Gaia in the optical (~23.3 mag)
- Access: `astroquery.vizier.Vizier.query_region(catalog="II/349/ps1")` — the code queries **DR1**
  specifically (VizieR `II/349/ps1`), not DR2
- Rate limit: same CDS/VizieR infrastructure as Simbad and 2MASS; 1-hr cache is sufficient
- Coverage limit: only queried for `dec_center > -30°`

### MPC (Minor Planet Center)
- Source: IAU Minor Planet Center / IMCCE SkyBot
- Content: all known asteroids and comets with orbital elements
- Access: `astroquery.imcce.Skybot.cone_search()` at observation epoch

### JPL Horizons
- Source: NASA Jet Propulsion Laboratory
- Content: high-precision ephemerides for solar system bodies
- Access: `astroquery.jplhorizons.Horizons`
- Use: computing predicted position of a known asteroid/comet at observation time (called from `ephemeris.py`)

Not queried yet: **SDSS DR17** (~mag 22, ~35% sky coverage) — a possible further fallback for
the faint-`UNKNOWN` problem, see Known Issues #1.

---

## Coding Conventions

- Python 3.11+
- Type hints on all function signatures
- `async/await` for all API calls (via `httpx.AsyncClient`)
- All configuration via `config.py` (which reads `.env`) — no magic strings in modules
- Each module exposes one primary async function, e.g. `await qc.analyze(fits_path)`
- Log using the Python `logging` module. Per-frame context is passed via `extra={"fits_filename": ...}`
  (named `fits_filename`, not `filename`, to avoid clashing with the reserved `LogRecord.filename`
  attribute). Note: as of this writing, `watcher.py`'s log format string does not actually
  interpolate `fits_filename` into the printed output, and `frame_id` is only ever included
  inline in message text rather than as a structured `extra` key — the "structured logging"
  goal isn't fully realized yet in the current code.
- Errors in external catalog queries (network timeout, rate limit) must be caught and logged —
  they must NOT crash the pipeline. The frame should still be processed with partial results.
- Errors in the observatory API calls: retry up to 3 attempts total (2 retries) with exponential
  backoff (see `api_client/` above and docs/API.md for the exact parameters), then log
  and continue — do not lose the frame
- Unit tests in `tests/` use `pytest` and mock all external calls (API, catalogs, astap subprocess)
- **All Markdown documents in this project are written in English** — this applies to every
  `.md` file (README.md, CLAUDE.md, docs/API.md, docs/anomaly-detector.md, everything under
  `docs/`, etc.), regardless of what language the request to write them was made in. Only the
  prose is English; code identifiers, config keys, and CLI examples inside those documents keep
  their original form as usual.

---

## Development Notes & Decisions

### Why pipeline → API, not pipeline → DB directly
Cleaner separation: the pipeline is a write-only science client. The API owns all data integrity,
validation, and business logic. This also allows the website and other future clients to share
the same API without duplicating logic.

### Why astap over astrometry.net
`astap` works fully offline, is fast (~2–5 sec per frame), and supports the same star catalog
formats. `astrometry.net` requires internet or a large local install. For an observatory
processing frames in bulk, offline operation is critical.

### Why sep over photutils for source extraction
`sep` is a Python wrapper over the original SourceExtractor C code — significantly faster for
bulk extraction. `photutils` is used for aperture photometry where its higher-level API is
more convenient.

### Why image subtraction in addition to catalog matching
Catalog cross-matching can only ever say "not found in any catalog we queried" — it cannot
distinguish a genuinely new pixel-level change from a source that's simply too faint for every
catalog checked. Differencing against a local median stack of the object's own archived history
gives a second, catalog-independent signal: "this literally wasn't there before, at the pixel
level." `modules/subtraction.py` implements this and feeds its candidates into the same
`anomaly_detector.py` classification path, tagged so they can bypass the coverage check (see
that module's section above).

### Why saturation is a flag, not a filter
A saturated star is astrometrically real and sometimes exactly what you want to keep tracking
(a bright asteroid, a flaring known variable) — dropping it from `sources`/`sources_all` outright
would lose that. What's unreliable is only the *magnitude*: aperture photometry integrates flux
over a clipped PSF core, and `-2.5*log10(net_flux)` on that garbage flux legitimately produces an
extreme (e.g. −14) number that isn't real (see docs/ISSUES.md #2 for the investigation that
uncovered this). So `astrometry.py` marks the source `saturated=True` and lets it flow through
normally; `photometry.py` is the one place that actually acts on the flag, by refusing to measure
it. `anomaly_detector.py` additionally suppresses `saturated=True` sources that have no catalog
match at all, since those are overwhelmingly bright-star/subtraction artifacts rather than real
transients (see docs/ISSUES.md #1) — a saturated source that *is* catalog-matched is left alone.

### MariaDB spatial queries in the API
Since MariaDB lacks pgSphere, the API implements cone searches using a bounding-box WHERE clause
on indexed (ra, dec) columns, followed by Haversine filtering in PHP for precise distances.
This is fast enough for the expected data volumes (millions of sources).

### Frame coverage check
Before classifying a missing source as "truly new", the pipeline asks the API in a single
batched call per frame: "have we ever observed these sky points before?" (`POST /frames/covering/batch`).
Without this check, the first observation of any field would generate false UNKNOWN alerts
for every single source.

### Catalog query caching
Implemented in `modules/catalog_matcher/_cache.py`: an in-process dict (fast path within one run) backed
by files under `CATALOG_CACHE_DIR`, TTL `CACHE_TTL_HOURS` (default 1 hour, from `mtime`). The disk
tier exists specifically because a pipeline restart — frequent during testing, and after every
code change without `--reload` — would otherwise throw away every cached query and re-hit
Gaia/Simbad/2MASS/Pan-STARRS/MPC for the same sky region on the very next run. `CATALOG_CACHE_DIR`
must be bind-mounted from a path OUTSIDE the container (see `docker-compose.yml`'s `worker`
service) so it survives a container rebuild/recreate too — a path only inside the container's
writable layer dies with the container, exactly what this cache exists to avoid. In a non-Docker
production deployment it's just a plain host directory; nothing here depends on being
containerized. A disk write failure (permission, disk full, not mounted) is logged and swallowed,
degrading to in-process-only caching for the rest of that run rather than breaking catalog
matching — see that module's `_cache_set()` docstring.

### Why bad frames go to /fits/rejected instead of API
Bad frames (blur, trailing, low star count) have no scientific value for the analysis pipeline.
Sending them to the API would waste bandwidth/storage, pollute the database with unusable data,
and complicate queries. Instead, they are moved locally to `/fits/rejected/` organized by target
object, with a prefix indicating the rejection reason — this allows manual review if needed.

### Why finder charts, and why two rendering styles
An anomaly on its own is a single (RA, Dec, mag, anomaly_type) row — useful for the API and any
downstream automation, but hard for a person to sanity-check without re-running the pipeline's
own plate-solved FITS files by hand. `modules/finder_chart.py` closes that gap: for a source with
a resolved `source_id`, it always regenerates a small PNG from that source's *complete* track
(every frame it has ever been detected on), so the very next anomaly for the same object simply
produces an updated image with one more epoch on it — one PNG per style the source's anomaly
history actually calls for (usually one, but both at once when the source's history spans both
categories — see this module's section above).

The two styles are deliberately different because "did this move?" and "did this change?" are
different questions:
- A moving object's (RA, Dec) is different on every frame *by design* — the useful picture is a
  single background image with each epoch's position marked and connected, so the motion itself
  is visible at a glance. Pixel-level alignment between epochs is unnecessary for this: only a
  per-epoch WCS coordinate transform onto the background frame's own pixel grid is needed.
- A stationary anomaly's position is expected to stay put — what matters is whether the *pixels*
  at that position changed (a supernova candidate appearing, a variable star brightening). A
  strip of small before/after crops, one per epoch, is the natural way to "blink" through that,
  and needs no cross-epoch alignment at all: each crop uses only its own frame's own WCS.

### Why the pipeline renders the chart but the API stores it
Only the pipeline has filesystem access to the archived FITS files a chart is built from (see
"Architecture: Two Repositories" above — the API has no knowledge of, or access to, the
observatory server's `/fits/...` volumes). But only the API can serve the finished image back out
to a future consumer such as the observatory website, since the pipeline has no inbound HTTP
server of its own. Hence the split: `modules/finder_chart.py` does all the rendering locally
(cheap — the epochs it needs are already sitting in `/fits/archive/{object}/`, no re-download
required) and uploads only the finished PNG via the API.

---

## Known Issues & Future Improvements

Resolved issues are not tracked here — see `git log` for that history. Only genuinely open items
stay in this section.

### 1. Faint UNKNOWN sources (mag > 20)

**Problem:** Sources fainter than ~20 mag are often marked as `UNKNOWN` anomalies because they
fall below the completeness limit of Gaia DR3 (~21 mag). These are NOT new discoveries — just
normal faint stars missing from the catalog.

**Status:** Partially mitigated. Pan-STARRS DR1 (depth ~23.3 mag) was added as a fourth catalog
in `modules/catalog_matcher/` specifically to catch faint optical sources Gaia misses. However,
there is still **no magnitude threshold** in `anomaly_detector.py`'s `UNKNOWN` branch — a source
that even Pan-STARRS doesn't catalog is still unconditionally flagged `UNKNOWN`, however faint
it is.

**Remaining possible solutions:**
- Add a magnitude threshold to skip/downgrade the `UNKNOWN` alert for sources with mag > 20 — not implemented
- Query SDSS DR17 (~22 mag, ~35% sky coverage) as a further fallback — not implemented
- Add a new classification `FAINT_UNCATALOGUED` distinct from true `UNKNOWN` — not implemented (still just a `TODO` comment)

**Location:** `modules/anomaly_detector/_classify.py`, the `UNKNOWN` classification branch.

### 2. `QC_SNR_MIN` is configured but not enforced

**Problem:** `config.QC_SNR_MIN` is documented (here, in `.env.example`, and in README.md) as
"minimum acceptable median SNR", but `modules/qc.py` computes and returns `snr_median` without
ever comparing it against `QC_SNR_MIN` in the BLUR/TRAIL/LOW_STARS/BAD decision logic. The
threshold currently has no effect on whether a frame is accepted or rejected.

**Location:** `modules/qc.py`, the flag-decision block; `config.py`.

**Location:** `watcher.py`.
