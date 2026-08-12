# CLAUDE.md — Observatory FITS Analysis Pipeline

This file gives an AI assistant the **design context** for this repo — what each module does,
why it's built that way, and how data flows between them. It deliberately does *not* repeat
what's already documented elsewhere; each of those facts has exactly one home:

| Topic | Source of truth |
|---|---|
| Deployment, Docker setup, environment variables, project structure, dependencies | [README.md](README.md) |
| REST API endpoint contracts (full request/response JSON) | [docs/API.md](docs/API.md) |
| `modules/anomaly_detector.py` internals (batch prefetch, classification flowchart) | [docs/anomaly-detector.md](docs/anomaly-detector.md) |
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
  even when invoked headless — `modules/astrometry.py` runs it via `xvfb-run`.
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
| **Observation** | `DATE-OBS`, `TIME-OBS`, `MJD-OBS` | Observation timestamp |
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
4. `qc.analyze(fits_path)` → returns metrics + quality flag
5. If `quality_flag != OK` → move file to `/fits/rejected/{object_name}/` → **STOP** (no API call)
6. `astrometry.solve(fits_path, psf_fwhm_arcsec=...)` → returns WCS + two source lists: `sources` (strict star filter) and `sources_all` (loose filter — also keeps bright/saturated and faint detections, used for matching). This selection is made *before* step 7's merge below — `sources`/`sources_all` must already exist as names before anything tries to extend them. `psf_fwhm_arcsec` is step 4's `qc_result["fwhm_median"]`, forwarded only when `qc_result["fwhm_unit"] == "arcsec"` (see step 7 below for why).
7. `subtraction.run(fits_path, archive_dir, filter_name, wcs=astro_result["wcs"], psf_fwhm_arcsec=...)` → if ≥`SUBTRACTION_MIN_FRAMES` archived frames of the same object exist, aligns them (via `astroalign`), builds a median reference, subtracts, and returns candidate sources found only in the difference image. These are merged into the source list and flagged `_from_subtraction=True`. Skipped gracefully otherwise. The `wcs` passed here is step 6's already-solved WCS, not re-derived from `fits_path`'s own header — that header isn't corrected until step 14.5 archives the frame (see `modules/astrometry.py`'s section below), so re-deriving it here would give subtraction's candidates a different systematic sky-position offset than every other source in the frame. `psf_fwhm_arcsec` is `qc_result["fwhm_median"]` from step 4 — passed only when `qc_result["fwhm_unit"] == "arcsec"` (it can instead be a raw pixel count when the frame's headers don't carry enough to derive a plate scale; see `modules/qc.py` below), since `astrometry.solve()`'s call in step 6 uses the same guard. See `modules/subtraction.py`'s section below for what this enables.
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
11. `api_client.post_frame(frame_data)` → registers the frame, gets back `frame_id`
12. `api_client.post_sources(frame_id, filename, sources)` → saves all detected sources (already
    catalog-matched and photometrically calibrated); returns `source_ids` (positionally parallel
    to `sources`), which this step zips back onto each source dict as `_source_id` so
    `anomaly_detector.py` can populate `anomalies[].source_id`.
12.5. Move file to `/fits/archive/{object_name}/` directory. Runs immediately after step 12, NOT
     after anomaly detection (an earlier revision of this file ran it later, between steps 14 and
     15) — anomaly detection never touches the local file at all, so there was no reason to delay
     archiving behind it, and doing so would have blocked decoupling anomaly detection into a task
     that might run much later (see `modules/anomaly_detector.py` below). Must still run **before**
     step 15: that step looks up this same frame's own file at its archive path, so moving it any
     later than this would mean the current epoch is never found there. This is where
     **Module 1** ends — steps 1–12.5 are `pipeline.analyze_frame(fits_path)`'s entire body,
     independently callable as a task item (see "Job queue" below).
13. `anomaly_detector.detect(frame_id, sources, catalog_matches, frame_meta)` → finds anomalies, using the batched history/coverage API calls (see `api_client/client.py` below)
14. `api_client.post_anomalies(frame_id, filename, anomalies)` → saves anomalies. Steps 13–14 are
     **Module 2** — `pipeline.detect_anomalies_for_frame_data()` (in-memory `sources`, used right
     after step 12.5 above) or its standalone counterpart
     `pipeline.detect_anomalies_for_frame_id(frame_id)` (reconstructs `sources` purely from
     `GET /frames/{id}` + `GET /frames/{id}/sources`, no local FITS access — see docs/API.md
     section 14 and "Job queue" below). This call **replaces** the frame's anomaly set rather than
     appending to it (docs/API.md section 3), so a re-run under a different classifier doesn't
     leave stale anomalies from the previous run behind.
15. `finder_chart.update_charts_for_sources(anomaly_type_by_source_id, designation_by_source_id)`
     — **Module 3**, via `pipeline.generate_charts_for_anomalies()` (in-memory) or
     `pipeline.generate_charts_for_source_ids()` (standalone, task-driven — see "Job queue" below).
     → for every anomaly with a resolved `source_id` (deduped per frame), (re)generates and
     uploads that source's finder/discovery chart: fetches every source's full position track in a
     single `POST /sources/tracks/batch` call, renders each against the matching local archive
     FITS files, then uploads each rendered chart individually via
     `POST /sources/{id}/chart` — one request per chart. `designation_by_source_id` is built here
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

A bare basename with no directory component at all (no full path) is not rejected outright: both
`analyze_frame()` and `preview_catalog_match()` run it through `pipeline._resolve_bare_filename()`
first, which searches every `FITS_ARCHIVE/{object}/` subdirectory for an exact filename match and
substitutes the full path if it finds exactly one. This exists because observatory-api's
`Web\FramesController::createTask()` debug page builds `ANALYZE`/`PREVIEW_CATALOG_MATCH` task
items straight from an already-registered frame's `frames.filename` column (a basename — see
`api_client/client.py`'s section below), and the API has no way to supply a real full path itself:
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
`modules/catalog_matcher.py`, so repeated frames of the same object/session within one task benefit
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
no more standalone CLI script for this (removed — see debug/README.md); create a
`PREVIEW_CATALOG_MATCH` task instead.

### `modules/qc.py`
Computes quality metrics from a FITS file without plate solving:
- **FWHM** (median over detected stars) — indicator of focus quality
- **Elongation** (major/minor axis ratio of PSF ellipse) — indicator of tracking/trailing
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

Quality flags and handling:
| Condition | Flag | Action |
|---|---|---|
| FWHM > QC_FWHM_MAX_ARCSEC | `BLUR` | Move to `/fits/rejected/{object}/BLUR_filename.fits` |
| Elongation > QC_ELONGATION_MAX | `TRAIL` | Move to `/fits/rejected/{object}/TRAIL_filename.fits` |
| Sky background > QC_SKY_BACKGROUND_MAX | `HIGH_BACKGROUND` | Move to `/fits/rejected/{object}/HIGH_BACKGROUND_filename.fits` |
| Star count < QC_STARS_MIN (or < 3 raw detections) | `LOW_STARS` | Move to `/fits/rejected/{object}/LOW_STARS_filename.fits` |
| Multiple issues, or a FITS read / background-estimation / extraction failure | `BAD` | Move to `/fits/rejected/{object}/BAD_filename.fits` |
| All good | `OK` | Continue processing |

`LOW_STARS` only fires when `BLUR`, `TRAIL`, and `HIGH_BACKGROUND` are all false — a low star
count is treated as a *consequence* of one of those three (sources filtered out, or too faint
to detect), not a separate root cause, so it isn't double-counted alongside whichever of them
actually explains it. `QC_SKY_BACKGROUND_MAX` has no universal default that fits every
site/instrument (same as `QC_FWHM_MAX_ARCSEC`) — tune it to your own site's typical dark-sky
`sky_background` reading on good frames.

**Important:** Bad frames are NOT sent to the API. They are moved to the `rejected` folder
with a prefix indicating the rejection reason. This saves bandwidth, storage, and keeps the
database clean from unusable data.

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
{Object}_{FrameType}_{Filter}_{Exptime}_{DateTime}.fits
```
Frame type uses short codes: L=Light, D=Dark, F=Flat, B=Bias

Examples:
- `M45_L_B_60_2020-10-15T01-24-51.fits` (M45, Light, Blue filter, 60s)
- `M51_L_Ha_300_2024-03-15T22-01-34.fits` (M51, Light, Ha filter, 300s)
- `NGC1234_L_L_120_2024-03-15T22-01-34.fits` (NGC1234, Light, Luminance, 120s)
- `M42_D_300_2024-03-15T22-01-34.fits` (Dark frame, no filter)

When normalization is enabled, the API receives only normalized values (no duplicates).

### `modules/astrometry.py`
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
- Calls `astap` binary as a subprocess via `xvfb-run` (astap needs a display even headless) for plate solving,
  invoked without `-update` — astap therefore never writes into the FITS file itself, only into a `.wcs` side
  file (plus `.ini`/`.log`) next to it, or under an optional `output_base` (`-o`) path
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
- Returns a dict: `{ra_center, dec_center, fov_deg, naxis1, naxis2, sources, sources_all, wcs}`
  - `sources` — strict star filter, list of dicts `{ra, dec, flux, fwhm, elongation, saturated, near_edge, ...}`
  - `sources_all` — loose filter; additionally keeps bright/saturated and faint detections rejected by the strict filter, used downstream for catalog matching / WCS offset correction so moving or transient objects aren't lost
  - `wcs` — the `astropy.wcs.WCS` object itself, also consumed by `modules/subtraction.py` to convert difference-image pixel candidates back to sky coordinates
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
    reasons rather than real motion or trailing — `modules/anomaly_detector.py` reads this flag to
    demand a higher elongation bar before classifying such a source `SPACE_DEBRIS` (real incident,
    2026-08-07, `T_CrB` frames: 305 anomalies out of 4 frames, the vast majority coma-elongated but
    otherwise ordinary corner stars). Deliberately no leading underscore, same as `saturated` above
    — `api_client`'s `_to_wire_source()` lets it travel to the API unfiltered and it's persisted on
    `source_observations`, so `pipeline.py`'s standalone `_from_wire_source()` can reconstruct it
    for a decoupled `DETECT_ANOMALIES` re-run with no in-memory pixel position to recompute it from.

### `modules/photometry.py`
- Aperture photometry via `photutils.aperture`
- Differential photometry against Gaia reference stars in the field (requires ≥3 Gaia DR3 matches to compute a zero-point) — this makes brightness measurements immune to atmospheric transparency variations
- Adds the following fields to each source: `flux_aperture`, `flux_err`, `mag_instrumental`, `mag_calibrated`, `mag_err`, `snr`, `calibrated` (bool), `edge_flag`, `zero_point`, `zero_point_err`
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
   of the same object (same filter preferred, matched case-insensitively by a `_{FILTER}_`
   filename token; falls back to any filter if there aren't enough same-filter frames).
2. Aligns each reference frame to the new frame using `astroalign` (triangle-pattern matching —
   does not require WCS). Reference frames are handed to `astroalign` even when their pixel
   dimensions differ from the new frame's (e.g. archived with a different camera/resolution) —
   `astroalign` resamples onto the new frame's pixel grid regardless of the source's original
   shape, so a shape mismatch alone is not a reason to skip a candidate reference frame.
3. Builds a per-pixel **median stack** of the aligned reference frames as the "reference image", then subtracts it from the new frame to get a difference image.
4. Masks the vicinity (`SATURATION_MASK_RADIUS_ARCSEC`, converted to pixels via the frame's WCS
   plate scale, dilated with `scipy.ndimage.binary_dilation`) of any pixel at or above
   `SATURATION_ADU` in the new frame **or any aligned reference frame** — `astroalign` resampling
   leaves large non-Gaussian residuals around saturated stars even under near-perfect
   registration, which `sep` would otherwise report as spurious bright "transients". Masked pixels
   are zeroed in the background-subtracted diff image before extraction, so no candidate can be
   detected there. See docs/ISSUES.md #1, #2.
4.5. Also masks any streak-like feature found by the same coarse pre-pass `modules/astrometry.py`
   uses (`_build_streak_mask()`, duplicated here — `config.STREAK_*`), run over the diff image
   itself rather than the raw frame. A satellite trail present in the new frame but absent from the
   reference stack shows up in the diff image as a strong positive residual just like any other
   transient — and, left unmasked, fragments into dozens of separate elongated candidates rather
   than one, each individually classifiable by `anomaly_detector.py` as its own `SPACE_DEBRIS`
   anomaly (real incident, 2026-08-07, `T_CrB` test frames: 42 elongation>3 candidates strung along
   a single trail). This pre-pass's own `minarea` is hardcoded to `5` here — matching this module's
   own final detection pass below, **not** `config.SEP_MIN_AREA` (15, the main-frame extraction
   context in `modules/astrometry.py`/`modules/qc.py`) — a coarser coarse-pass `minarea` than the
   real detection left small trail fragments invisible to the pre-pass while the real, more
   sensitive pass still detected them individually. Reduced the 42 false candidates to 21.
   `STREAK_DETECT_SIGMA`'s default (`3.0`, lower than `SUBTRACTION_DETECT_SIGMA`'s `5.0`) was tuned
   against this same real frame: at `5.0σ` the coarse pass still couldn't connect the (very faint)
   trail's brighter knots into long-enough coarse features, leaving 21 of the 42 candidates
   unmasked; at `3.0σ` only 1 remained.
5. Detects sources on the (masked) difference image via `sep.Background` + `sep.extract`, with threshold `SUBTRACTION_DETECT_SIGMA × background_rms`. `fwhm`/`elongation` per candidate are derived from `sep`'s `a`/`b` second-moment axes (same Gaussian approximation as `modules/astrometry.py`), since `sep.extract()` doesn't return a native `fwhm` field.
5.5. Rejects any candidate whose `fwhm` is below `psf_fwhm_arcsec / 1.5` (converted to pixels via
   the frame's plate scale — same ratio `modules/astrometry.py` uses for its own lower FWHM bound;
   see that module's section above), where `psf_fwhm_arcsec` is `pipeline.py`'s forwarded
   `qc.analyze()` measurement of this frame's actual stellar PSF. This exists because step 3's
   median reference stack only removes reference-frame artifacts that move between frames when
   `astroalign` resamples them onto the new frame's grid — a sensor hot/warm pixel is fixed to the
   *detector* grid, not the sky, so each reference's own copy of it lands at a different resampled
   pixel and gets averaged away, while the **new** frame's own hot pixel sits untouched at its
   native position and survives the subtraction as a sharp, undiffused positive residual with no
   real-star-like PSF profile at all (real incident, 2026-08-06, Vesta test frames — see
   `modules/astrometry.py`'s section above for the same underlying failure mode). Skipped
   (behavior unchanged) when `psf_fwhm_arcsec` or the frame's plate scale isn't available.
6. Converts detected pixel positions back to (RA, Dec) using the frame's WCS — preferring the
   already-solved `wcs` passed in from `astrometry.solve()` (see `pipeline.py` step 7 above) over
   re-deriving one from the new frame's own header, which can still carry a stale/mount-pointing
   WCS at this point (`pipeline.py` only corrects the header at archive time, step 14.5 — see
   `modules/astrometry.py`'s section below). `wcs=None` (e.g. a caller that never ran astrometry
   itself) falls back to reading whatever WCS the file's own header has.
7. Returns `{"performed": bool, "reference_frame_count": int, "candidates": [...]}`. Every
   candidate is tagged `_from_subtraction=True` so `anomaly_detector.py` can apply looser
   coverage rules to it (see below). Candidates whose pixel position falls within the
   `EDGE_MARGIN_FRAC` zone are **rejected outright** (not returned in `candidates` at all) —
   coma and other off-axis aberrations change the PSF shape between frames (rotation, guiding,
   focus shift), so the median reference stack never perfectly cancels an edge star's coma wing;
   the resulting residual is picked up by `sep` as a spurious "new source". Real incident,
   2026-08-10 analysis: 53 of 80 `UNKNOWN` alerts were `from_subtraction + near_edge` — every
   one a coma residual of an ordinary catalogued star. Candidates surviving this filter still
   carry `near_edge=False` (by construction — the only ones left are interior).

Gracefully skipped (`performed=False`) when fewer than `SUBTRACTION_MIN_FRAMES` archived frames
exist yet — e.g. the very first observations of a new target.

### `modules/catalog_matcher.py`
Cross-matches the source list against external catalogs using
`astropy.coordinates.SkyCoord.match_to_catalog_sky()` with cone radius `MATCH_CONE_ARCSEC`
(`MOVING_CONE_ARCSEC` for the MPC step, since moving objects shift between frames).

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
flux-error formulas as `modules/photometry.py` — duplicated by hand rather than imported, the same
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

### `modules/anomaly_detector.py`
Core logic. For all detected sources in a frame **at once** (batched, not one API round-trip per source):

Every returned anomaly dict includes `source_id` — the resolved `sources.id` read off the
source's `_source_id` key, which `pipeline.py`'s Step 12 attaches from the `source_ids` array
returned by `POST /frames/{id}/sources`. `None` when that round-trip couldn't resolve one
(post_sources failed, or the API predates this field).

1. **Query history via API** — `POST /sources/near/batch` with every source position in a single call, returning historical sources near each (RA, Dec) from previous frames. Queried for **every** source regardless of catalog-match status — this is what makes the Δmag-based classifications below (`VARIABLE_STAR`, `BINARY_STAR`, and the "already-known host brightened" path of `SUPERNOVA_CANDIDATE`) reachable at all for a catalog-matched source.
2. **Coverage check** — `POST /frames/covering/batch` — did we ever observe each sky position before? (batched the same way)
3. **Classify** each source. Real priority order in code: MPC/SkyBot match first → **if unmatched (`catalog_name is None`) and `saturated=True`, suppressed outright** (see below) → unmatched, no detection within `MATCH_CONE_ARCSEC` of this exact position, elongation above the trail threshold (a single-exposure trail — `SPACE_DEBRIS_ELONGATION_MIN`, or the higher `SPACE_DEBRIS_EDGE_ELONGATION_MIN` when the source is flagged `near_edge` — see below) → `SPACE_DEBRIS` immediately, no position-shift evidence required (see below) → position-shifted-but-unmatched, elongation at or below that same threshold (→ `MOVING_UNKNOWN`) → no historical coverage (→ `FIRST_OBSERVATION`, *unless* the source came from image subtraction — see below) → no prior detection at this exact position but near a Simbad galaxy (→ `SUPERNOVA_CANDIDATE`) → not in history or any catalog (→ `UNKNOWN`) → in catalog but not history (→ `KNOWN_CATALOG_NEW`) → **has** prior history and brightened beyond `DELTA_MAG_ALERT`: near a Simbad galaxy (→ `SUPERNOVA_CANDIDATE`) → known binary (→ `BINARY_STAR`) → known variable (→ `VARIABLE_STAR`):

| Situation | Classification |
|---|---|
| Unmatched (`catalog_name is None`) and `saturated=True` | Suppressed — `return None`, no anomaly record at all (bright-star/subtraction artifact, not a real transient; see docs/ISSUES.md #1, #2) |
| Unmatched (`catalog_name is None`) and `near_edge=True` | Suppressed — `return None` (coma shifts the measured centroid away from the star's true catalog position, making catalog matching miss it; these are overwhelmingly ordinary stars with optical distortion, not real transients — real incident, 2026-08-10: 27 of 80 UNKNOWN alerts were non-subtraction near_edge sources) |
| No historical coverage | `FIRST_OBSERVATION` — not an anomaly, just note |
| No historical coverage, but the source was detected via image subtraction (`_from_subtraction=True`) and `near_edge=True` | Suppressed — `return None` (defense in depth for standalone `DETECT_ANOMALIES` re-runs; fresh subtraction already filters edge candidates at extraction time) |
| No historical coverage, but the source was detected via image subtraction (`_from_subtraction=True`) and `near_edge=False` | `UNKNOWN` → **ALERT** (subtraction already confirms it's absent from the reference stack, so missing API coverage doesn't downgrade it) |
| Area covered, source not in history at all, near a Simbad galaxy | `SUPERNOVA_CANDIDATE` → **ALERT** (new point source, no baseline to compare against) |
| Area covered, source not in history, found in catalog (not a galaxy) | `KNOWN_CATALOG_NEW` — was below detection threshold |
| Area covered, source not in history, not in any catalog, `near_edge=True` | Suppressed — `return None` (same coma-shifted-centroid rationale as above) |
| Area covered, source not in history, not in any catalog, `near_edge=False` | `UNKNOWN` → **ALERT** |
| Source **has** prior history, brightened by more than `DELTA_MAG_ALERT`, near a Simbad galaxy | `SUPERNOVA_CANDIDATE` → **ALERT** (already-known host got brighter) |
| Source in history, Δmag > DELTA_MAG_ALERT, known binary (Simbad) | `BINARY_STAR` |
| Source in history, Δmag > DELTA_MAG_ALERT, known variable (Simbad) | `VARIABLE_STAR` |
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

### `modules/finder_chart.py`
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

The single public entry point,
`update_charts_for_sources(anomaly_type_by_source_id, designation_by_source_id=None)`, takes every
(source_id → anomaly_type) pair for one frame at once (see pipeline.py Step 15), so it can fetch
every source's track and upload every chart in one HTTP round trip each, regardless of how many
anomalies the frame has. `designation_by_source_id` is optional and keyed the same way — built by
pipeline.py from `sources`' own `catalog_name`/`catalog_id` (already resolved by catalog matching,
Step 8), not queried by this module itself; a source_id absent from it gets the bare-`anomaly_type`
title.

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
   this same path.
4. Per source: renders the PNG (`track` or `stamp_strip`, per that source's anomaly type) using
   `matplotlib` with a zscale + asinh stretch (`astropy.visualization`) — the standard DS9-style
   display stretch.
5. Per source: `api_client.upload_source_chart(source_id, png_bytes, style, frame_count)` →
   `POST /sources/{id}/chart` — each chart uploaded individually as raw PNG bytes, replacing
   any previous chart for that source.

Gated by `CHART_ENABLED` (default `true`). Best-effort throughout: for a given source_id, a
missing local file, an API error, or a rendering failure is logged and that source_id's own
result is `False` in the returned dict — it never raises, and it never affects any other
source_id in the same call (pipeline.py's Step 15 calls this once per frame with every
anomaly's source_id, deduped per frame, unconditionally) or frame processing overall.

### `api_client/client.py`
All communication with the remote `observatory-api`. Uses `httpx` with async support and
`tenacity` for automatic retry on transient failures (HTTP 5xx and transport/timeout errors —
never on HTTP 4xx). Sends `X-API-Key`, `Content-Type: application/json`, `Accept: application/json`
on every request. Exact retry parameters and endpoint request/response shapes are documented
once, in **[docs/API.md](docs/API.md)** — not repeated here.

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
| Anomaly Δmag comparison | Yes | `modules/anomaly_detector.py`'s `_same_filter_history()` restricts the historical magnitude used for `VARIABLE_STAR`/`BINARY_STAR`/the brightening branch of `SUPERNOVA_CANDIDATE` to detections carrying the *same* filter as the current source (via each source's `_filter`, and each historical detection's `filter` — see `POST /sources/near/batch` in docs/API.md). The **existence** check (whether this position has ever been detected before, at any point in `FIRST_OBSERVATION`/`UNKNOWN`/`KNOWN_CATALOG_NEW`) stays filter-agnostic on purpose — an ordinary LRGB sequence re-images the same field in 3-4 different filters per session, and a position already seen in R must not look "brand new" the moment an L-filtered frame comes in |

Position-only classifications (`ASTEROID`/`COMET`/`MOVING_UNKNOWN`/`SPACE_DEBRIS`, and `UNKNOWN`
via subtraction) never depend on magnitude at all, so they are unaffected by any of this — a
moving object is a moving object regardless of what filter caught it moving.

---

## External Catalogs & APIs

Catalog matching order and rationale are covered under `modules/catalog_matcher.py` above; this
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
  backoff (see `api_client/client.py` above and docs/API.md for the exact parameters), then log
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
Implemented in `modules/catalog_matcher.py`: an in-process dict (fast path within one run) backed
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
produces an updated image with one more epoch on it.

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
in `modules/catalog_matcher.py` specifically to catch faint optical sources Gaia misses. However,
there is still **no magnitude threshold** in `anomaly_detector.py`'s `UNKNOWN` branch — a source
that even Pan-STARRS doesn't catalog is still unconditionally flagged `UNKNOWN`, however faint
it is.

**Remaining possible solutions:**
- Add a magnitude threshold to skip/downgrade the `UNKNOWN` alert for sources with mag > 20 — not implemented
- Query SDSS DR17 (~22 mag, ~35% sky coverage) as a further fallback — not implemented
- Add a new classification `FAINT_UNCATALOGUED` distinct from true `UNKNOWN` — not implemented (still just a `TODO` comment)

**Location:** `modules/anomaly_detector.py`, the `UNKNOWN` classification branch.

### 2. `QC_SNR_MIN` is configured but not enforced

**Problem:** `config.QC_SNR_MIN` is documented (here, in `.env.example`, and in README.md) as
"minimum acceptable median SNR", but `modules/qc.py` computes and returns `snr_median` without
ever comparing it against `QC_SNR_MIN` in the BLUR/TRAIL/LOW_STARS/BAD decision logic. The
threshold currently has no effect on whether a frame is accepted or rejected.

**Location:** `modules/qc.py`, the flag-decision block; `config.py`.

**Location:** `watcher.py`.
