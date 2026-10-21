# CLAUDE.md — Observatory FITS Analysis Pipeline

Design context for this repo: what each module does, why it's built that way, and how data
flows between them. Facts that already have a home elsewhere are not repeated here:

| Topic | Source of truth |
|---|---|
| Deployment, Docker setup, environment variables, project structure, dependencies | [README.md](README.md) |
| REST API endpoint contracts (full request/response JSON) | [docs/API.md](docs/API.md) |
| `modules/anomaly_detector/` internals (batch prefetch, classification flowchart) | [docs/anomaly-detector.md](docs/anomaly-detector.md) |
| Rationale behind individual fixes ("finding C4", "H16", …) | [docs/AUDIT-2026-08-18.md](docs/AUDIT-2026-08-18.md), `git log` |
| Open data-quality questions; API-side work queued from here | [docs/ISSUES.md](docs/ISSUES.md), [docs/API-TASKS.md](docs/API-TASKS.md) |
| Every setting and its default | `config.py` (`.env.example` mirrors it, kept in sync by hand) |
| Current version, what changed in each release | [CHANGELOG.md](CHANGELOG.md) (see "Versioning and releases") |
| Proposed new capabilities (not scheduled) | [ROADMAP.md](ROADMAP.md) |

Detailed per-function reasoning lives in the module docstrings. When something changes, update
it in its one home — don't copy it here.

---

## Project Overview

An automated Python service on a **dedicated observatory server** that:
1. Detects new FITS frames as they arrive
2. Runs quality control (flags bad frames)
3. Plate-solves (astrometry) and extracts/photometers sources
4. Runs image subtraction against archived frames of the same object (optional — skipped while
   too few references exist)
5. Cross-matches sources against external catalogs, and forces photometry at catalog positions
6. Compares against historical observations held by the remote API
7. Classifies anomalies (supernovae, asteroids, comets, variable stars, space debris, unknowns)
8. Computes ephemerides for known solar system objects and renders finder charts
9. Reports everything to the remote API — the pipeline has **no direct database access**

## Architecture: Two Repositories

- **`observatory-pipeline`** (this repo, Python) — runs next to the telescope in Docker; does all
  heavy computation; talks to the backend only via **REST + API key**; knows nothing about the
  DB schema.
- **`observatory-api`** (CodeIgniter 4 / PHP, cloud) — owns MariaDB and its schema, serves both
  the pipeline and the observatory website. Has **no filesystem access** to `/fits/...`.

```
[Observatory Server]                    [Cloud Hosting]
┌─────────────────────────┐            ┌──────────────────────────┐
│  docker-compose          │  HTTPS +   │  CodeIgniter 4 API        │
│  pipeline (watcher)      │─API Key───▶│  REST endpoints           │
│  worker                  │            │        │                  │
│  Volumes:                │            │  MariaDB                  │
│  /data/fits/incoming     │            │                           │
│  /data/fits/archive      │            │  Also consumed by the     │
│  /data/fits/rejected     │            │  observatory website      │
│  /data/fits/raw          │            │                           │
│  /data/astap/catalogs    │            └──────────────────────────┘
└─────────────────────────┘
```

Host paths hang off `HOST_DATA_DIR` (compose-only, from `.env`): `/data` on a Linux server, unset →
`~/Developer/data` for macOS development — `docker-compose.yml` is never edited per host.
The API key lives in `.env` and is never committed. Frames come from **several telescopes and
cameras** — never bake one instrument's numbers into defaults.

## Docker notes (why, not how)

- `astap` is installed from pre-downloaded archives in `install/` (offline, reproducible builds):
  `astap_amd64.tar.gz` and `astap_aarch64.tar.gz`, picked by the image's platform.
- `.dockerignore` keeps `.env` out of the image — secrets arrive only via `env_file` at runtime.
- `xvfb` + GTK/Pango are needed because astap requires a (virtual) display even headless;
  `modules/astrometry/_astap.py` runs it via `xvfb-run`.
- The source tree is bind-mounted to `/app`, so code edits need only
  `docker compose restart pipeline`/`worker`; rebuild only after changing `requirements.txt` or
  the Dockerfile.
- `CATALOG_CACHE_DIR` must be bind-mounted from outside the container so the catalog cache
  survives restarts and rebuilds.
- Logs: `logging_setup.configure_logging()` writes stderr plus, when `LOG_DIR` is set (compose
  sets `/logs`, bind-mounted from `HOST_DATA_DIR/logs`), a size-rotated `watcher.log` /
  `worker.log` — one file per service so two processes never rotate the same file. Docker's
  json log is capped and dies with the container; the files are the history.

---

## Processing flow

`watcher.py` never processes frames itself; it batches arriving paths into `ANALYZE` tasks, and
`worker.py` dispatches each task item to a `pipeline.py` stage. The work is split into three
independently re-runnable modules.

### Module 1 — `pipeline.analyze_frame(fits_path)`

0. **Refuse / convert.** `NAXIS != 2` (colour cube) or `STACKCNT`/`NCOMBINE > 1` (a capture-software
   stack) → moved to `FITS_REJECTED/{object}/UNSUPPORTED_{filename}`, not registered. A Bayer/CFA
   frame is replaced in place by its mono 2×2 superpixel (`modules/cfa.py`) after the original is
   copied to `FITS_RAW_ARCHIVE/{object}/`.
1. `fits_header.extract_headers()` → metadata dict.
2. `normalizer` → normalized object name, filter, frame type.
3. `Dark`/`Flat`/`Bias` → rename (`{Camera}_…`), archive to `FITS_ARCHIVE/_calibration/{night}/{type}/`,
   **stop** — not registered with the API.
4. `qc.analyze(fits_path, move_on_reject=False)` → metrics + `quality_flag`. The pipeline, not
   `qc.py`, owns the file's fate.
5. Non-`OK` flag → steps 6–10.5 are skipped, but the frame is **still registered** (with its QC
   metrics and an empty source list) and archived, so the operator sees why it failed and a later
   re-analysis can find it.
6. `astrometry.solve()` → WCS, `pixel_scale_arcsec`, `sources` (strict star filter) and
   `sources_all` (loose — also bright/saturated/faint/trailed detections; used for matching and
   anomaly detection). Right after the solve the PSF anchor is re-computed as
   `qc fwhm_median_px × solved pixel scale` and reused by steps 7, 9.5 and the archived `QCFWHM`
   (header plate scales can describe the wrong optics — M16).
7. `subtraction.run(..., wcs=solved_wcs)` → difference-image candidates, tagged
   `_from_subtraction=True`, merged into the source list.
8. `catalog_matcher.match()` — runs **before** photometry so Gaia stars can set the zero point.
8.5–8.6, 10.5. **Four dedup passes**, each for a different duplicate shape:
   - `_dedupe_by_catalog_identity` — same `(catalog_name, catalog_id)` twice in one frame. Prefers
     a normal detection, unless the pair is further apart than `MATCH_CONE_ARCSEC` (then the
     subtraction candidate is the mover and wins).
   - `_dedupe_uncatalogued_subtraction_pair` — an uncatalogued object's normal detection and its
     own subtraction candidate within `MATCH_CONE_ARCSEC`.
   - `_dedupe_unmatched_near_matched` — an unmatched deblending fragment beside a matched star
     (subtraction candidates exempt — they carry evidence of change).
   - `_dedupe_cross_catalog_duplicates` (after step 9.5) — one star claimed under two catalogs
     because forced photometry only checks its own `catalog_id`; keeps the earlier catalog in
     `_CATALOG_PRIORITY`.
9. `photometry.measure(..., wcs=solved_wcs, filter_name=...)` → calibrated magnitudes.
9.5. `forced_photometry.run()` — measures every in-footprint Gaia/MPC entry blind detection missed.
10. Unified `mag` = `mag_calibrated` if calibrated, else `None` — **never** the instrumental
    magnitude (it has no zero point).
11. `api_client.post_frame()` → `frame_id`. Payload includes `pointing_error_arcsec` (+RA/Dec
    components): mount-reported target vs plate-solved centre; all `None` if either is missing.
12. `api_client.post_sources()` → `source_ids` (positionally parallel), attached as `_source_id`.
12.5. Archive to `FITS_ARCHIVE/{object}/{filter}/` (`archive_layout.destination_for()`), first
    writing astap's solve into the header (`_write_solved_wcs()`, clearing stale
    `CD`/`PC`/`CDELT`/`CROTA` first) and stamping
    `QCFLAG`/`QCFWHM` (`_write_qc_headers()`) — the header is the only place a later frame's
    subtraction reference screen and finder charts can read them from.

### Module 2 — anomaly detection

13. `anomaly_detector.detect()` with batched history/coverage calls.
14. `api_client.post_anomalies()` — this **replaces** the frame's anomaly set, so it is called
    **only** when detection completed. Posting `[]` after a failure would erase earlier results.
    `HistoryUnavailable` (an API outage during the prefetch) re-queues `DETECT_ANOMALIES`; a
    failed `GET /frames/{id}/sources` fails the item.

Entry points: `pipeline.detect_anomalies_for_frame_id(frame_id)` — what a `DETECT_ANOMALIES` task
runs; rebuilds sources from `GET /frames/{id}` + `/sources`, no FITS access — or
`pipeline.detect_anomalies_for_frame_data()` (in-memory, used only by `pipeline.run()`). Nothing
chains detection after ingest: the operator submits `DETECT_ANOMALIES` for the frames they want.
Anomaly history/coverage queries use `obs_time`; position-computing consumers use `obs_time_mid`
(see `fits_header` below).

### Module 3 — finder charts

15. `finder_chart.update_charts_for_sources(anomaly_types_by_source_id, designation_by_source_id)`
    via `pipeline.generate_charts_for_anomalies()` or `generate_charts_for_source_ids()`. Each
    source maps to a **list** of anomaly types (one source can need several chart styles). The
    designation prefers the anomaly's own `mpc_designation` over a `catalog_id` lookup by
    `source_id`, because the API resolves `source_id` positionally and it can be shared with an
    unrelated catalogued star.

`pipeline.run()` composes all three, but is only used by tests and ad hoc invocations — the live
ingestion path goes through the task queue.

### Recovery after API failures

API calls retry 3 times total on 5xx/transport errors (never 4xx). If `POST /sources` or
`POST /anomalies` still fails, or the anomaly history prefetch does, `pipeline._queue_recovery_task()`
re-queues an `ANALYZE` (archive path) or `DETECT_ANOMALIES` task; both endpoints are idempotent. The re-queue count rides in the
item `payload`, capped by `API_RECOVERY_MAX_ATTEMPTS`; if even that fails the item is logged at
ERROR for manual submission.

---

## Job queue

### `watcher.py`
- `watchdog` on `FITS_INCOMING`; each new file goes through `enqueue_path()` into a pending batch
  with a `WATCHER_DEBOUNCE_SEC` debounce (zero-delay once `WATCHER_MAX_BATCH_SIZE` is reached).
  `flush_pending_batch()` submits the batch as **one** `ANALYZE` task.
- Skips a path already in the pending batch (watchdog can deliver duplicate create events —
  polling emitter on macOS bind mounts, write-then-rename capture software).
- Flushes always run on a `threading.Timer`, never in the observer thread, so a slow
  `POST /tasks` never delays detection.
- The startup scan (`process_existing_files()`) uses the same path, so a backlog becomes a few
  bulk batches; files that `PENDING`/`RUNNING` ANALYZE tasks already carry are skipped
  (`_mark_already_queued()`), so a restart does not queue them twice. `KeyboardInterrupt` stops
  the observer, then flushes what's buffered. Nothing here ever moves a file.

### `worker.py`
Polls observatory-api's `tasks` table (docs/API.md §14) and dispatches items:

| Task `type` | Dispatched to | Item carries |
|---|---|---|
| `ANALYZE` | `pipeline.analyze_frame(item["filename"])` | full FITS path |
| `DETECT_ANOMALIES` | `pipeline.detect_anomalies_for_frame_id(item["frame_id"])` | `frame_id` |
| `GENERATE_CHARTS` | `pipeline.generate_charts_for_source_ids(...)`, one call per `source_id` | `source_id` (required), optional `anomaly_id`, `payload` `{anomaly_type, designation}` |
| `PREVIEW_CATALOG_MATCH` | `pipeline.preview_catalog_match(filename, task_id, item_id)` | full FITS path |
| `DELETE_FRAME` | `pipeline.move_archived_file_to_rejected(filename, object)` after `GET /frames/{id}`; the API cascades the DB delete once the item is `DONE` | `frame_id` |
| `RESTART` | clean exit → Docker restarts → settings re-fetched | none |

- **Bare basenames** (the API's debug page only knows `frames.filename`) are resolved by
  `pipeline._resolve_bare_filename()` via `archive_layout.find_archived()`; no match passes
  through unchanged so the normal "file not found" surfaces, several are logged and the first
  wins.
- **`GENERATE_CHARTS`**: `payload.anomaly_type` is optional (a chart requested straight from a
  source sends only `source_id`; `None` falls back to before_after/stamp_strip). One task may carry
  several items per `source_id` with different types; they're grouped into one call and each item's
  outcome is looked up by `(source_id, anomaly_type)`. Progress is reported after each source.
  Chart generation is always operator-triggered — nothing chains automatically from
  `DETECT_ANOMALIES`.
- **`RESTART`** exists because remote settings (`GET /settings` → `config.apply_remote_settings()`)
  are read only at startup. It restarts the worker only; restart the `pipeline` container by hand
  if it needs new settings too.
- **`PREVIEW_CATALOG_MATCH`** is a diagnostic, not a production module: never registers, archives
  or moves anything; its result (`{matched, total, quality_flag, chart_uploaded}`) is written to
  the item's `payload` via `POST /tasks/{id}/items/progress` (the `payload` field is bidirectional).
- **Polling:** `GET /tasks?status=PENDING&limit=1&order=asc&claimable=1` every `TASK_POLL_INTERVAL_SEC`,
  exponential backoff to `TASK_POLL_BACKOFF_MAX_SEC` when idle; a busy queue drains back-to-back.
- **Claiming** (`PATCH status=RUNNING`) is atomic on the API side (`409` if no longer `PENDING`,
  or targeted at another pipeline); the worker stops on a failed claim.
- **Several pipelines, one API:** each server has its own `API_KEY` (issued by the API's
  `php spark pipeline:create <name>`); the key is the pipeline's identity — no id setting here.
  The API records the frames a pipeline registers as its own and targets every task needing them
  at it (`tasks.pipeline_id`); tasks the pipeline creates itself (`api_client.create_task()`) are
  targeted at itself. Shared: history, coverage, anomalies and remote settings.
- **Shutdown:** SIGTERM/SIGINT is checked only *between* items (an item cut halfway could register
  a frame without its sources). The item in progress finishes, nothing new starts, and a task
  with unstarted items is `PATCH`ed back to `PENDING`; compose's `stop_grace_period: 5m` covers a
  long ANALYZE item. A second signal exits at once — the handler is `signal.signal()`, not the
  loop's, because CPU-bound steps hold the loop for minutes. astap runs in its own session, so a
  terminal Ctrl+C does not kill it mid-solve. The watcher maps SIGTERM onto its Ctrl+C path
  (flush the pending batch) — as PID 1 it would otherwise ignore SIGTERM until Docker's SIGKILL.
- **Lost progress reports:** an item outcome the API did not take (`post_task_items_progress()`
  → `None`) is retried once the handler returns; if it still fails, the task goes back to
  `PENDING` (its unreported items are redone) instead of sitting at `RUNNING` forever.
- **Known limitation:** no lease/heartbeat. A worker that dies mid-task without that chance (OOM
  kill, SIGKILL after the grace period) leaves it `RUNNING` forever — reset by hand with
  `PATCH /tasks/{id} {"status": "PENDING"}`. The item it was on stays `RUNNING` (the worker's
  live-progress hint, `_mark_running()`); the worker treats `RUNNING` items like `PENDING` ones
  (`_UNFINISHED_ITEM_STATUSES`), so the next claim redoes it.

---

## Modules

### `config.py`
Loads everything from environment variables. No hardcoded paths, thresholds or credentials
anywhere else.

### `modules/fits_header.py`
`extract_headers(fits_path) -> dict` — normalized metadata, missing keys `None`. Keyword aliases
are in the code; `OBJECT` drives the archive subdirectory. Non-obvious resolution rules:

- **Timestamp:** `DATE-OBS`/`TIME-OBS`/`MJD-OBS` are resolved together, not first-non-empty:
  `DATE-OBS` with a time → as is; date-only `DATE-OBS` + `TIME-OBS` (a full timestamp in
  `TIME-OBS` wins); else `MJD-OBS`. A bare time with no date → `None`. Date-only with no time →
  midnight, with a warning.
- **`obs_time_mid`** = `obs_time + EXPTIME/2` (`midpoint_time()`), a separate field: `obs_time`
  stays what the frame is registered and named under. Only position-computing consumers use the
  midpoint — the MPC/SkyBot cone search, JPL Horizons, forced photometry's proper-motion
  propagation — falling back to `obs_time`. The standalone `DETECT_ANOMALIES` path recomputes it
  from the stored frame (`pipeline._frame_exptime()` accepts flat or nested `observation.exptime`).
- **Plate scale:** `resolve_pixel_scale_arcsec()` is the single resolver (also used by `qc.py`).
  `XPIXSZ`/`PIXSIZE`/`PIXELSZ` are µm; `PIXSCALE`/`PIXSCALE1` are ambiguous (µm or ″/px). Order:
  µm keyword + `FOCALLEN` → the ambiguous card's comment naming a unit → read as ″/px, logged.
- **Equinox:** mount RA/Dec in a non-J2000 equinox (e.g. JNow) is precessed to ICRS (`_to_icrs()`);
  `RADESYS: ICRS` short-circuits. Applies only to the mount position, not the WCS.
- **Numeric RA units:** a bare number in `[0, 24)` may be hours; `_resolve_numeric_ra()` decides
  from the card comment, else whichever reading lands closer to `CRVAL1`, else keeps degrees and
  logs. A wrong guess also breaks astap's narrow search.

### `modules/normalizer.py`
Normalizes object names (`M 51`→`M51`, `Sh2 101`→`SH2-101`, `Abell 39`/`A39`→`Abell39` — bare
`A<n>` only when n ≤ 2712, so `A807 FA` stays a minor-planet designation), filters (`Luminance`→`L`,
`OSC`/`RGB`/`Color`→`OSC`, `H-Alpha`→`Ha`, `L-eNhance`→`LeNhance`, Johnson/SDSS passed through, …)
and frame types (`Light`/`Dark`/`Flat`/`Bias`). Full mapping tables are in the code.
`is_narrowband()` uses `config.NARROWBAND_FILTERS`.

Filenames (`NORMALIZE_ENABLED=true`):
```
Light:     {Object}_Light_{Filter}_{Exptime}_{DateTime}[_{Seq}].fits
Flat:      {Camera}_Flat_{Filter}_{Exptime}_{DateTime}[_{Seq}].fits
Dark/Bias: {Camera}_{FrameType}_{Exptime}_{DateTime}[_{Seq}].fits
```
Flats carry the filter (they are filter-specific; without it they overwrote each other). A
missing filter is the token `NONE`. Calibration is named after the sanitized `INSTRUME`
(`_UNKNOWN` when missing), not the object — it says which camera the frame fits. Filter and camera
tokens have `_` turned into `-` so they stay one positional token.
`modules/subtraction.py` parses these names **positionally** from the DateTime token leftward
(object names may contain `_`), and still understands the legacy `L`/`D`/`F`/`B` type codes and
the legacy filter-less Light name.

### `modules/archive_layout.py`
The one home of the archive layout (`config.py` keeps only the roots); nothing else composes
paths under `FITS_ARCHIVE` by hand. `FITS_ARCHIVE` doubles as the root of a public read-only FTP
share, so the layout is for browsing:

- Lights → `{object}/{filter}/` (`NONE` without a filter, `_UNKNOWN` without `OBJECT`); no
  instrument level — subtraction's instrument screen keeps optics apart instead.
- Dark/Flat/Bias → `_calibration/{night}/{type}/`, chosen by the **frame type**, never `OBJECT`
  (capture software writes the current target into calibration frames). Night = UTC date of
  `obs_time − 12 h`.
- `find_archived(filename, object)` resolves an API basename: own object's filter dirs, every
  object's, `_calibration`, then the legacy flat `{object}/` (a half-migrated archive still
  works). `tools/migrate_archive_layout.py` moves a flat archive once (Lights keep their names —
  the API stores them; calibration is renamed to the camera scheme).
- Object directory names: spaces → `_`, special characters removed (`sanitize_object_name()`,
  which also strips leading underscores, so `_calibration`/`_UNKNOWN` cannot collide).

### `modules/cfa.py`
One-shot-colour support: `is_cfa(header)`, `to_superpixel(src, dest)`. The Bayer mosaic's channel
pedestals read as noise to everything downstream, so the frame is converted **once, at ingest**,
on disk — converting in memory would leave astap's WCS in a different pixel grid from every other
module.
- 2×2 **mean** superpixel (phase-independent, no interpolation, luminance-like band, half
  resolution); a block containing a saturated pixel keeps its **max** so saturation stays visible.
- Header: pixel size ×2, `EGAIN` ×4 (`GAIN` untouched), Bayer/BZERO/BSCALE/native WCS removed
  (mount `RA`/`DEC` kept), `CFACONV = T` (idempotent), `CFAPAT` = original pattern,
  `FILTER = 'OSC'` if none recorded.
- Safe ordering: convert to temp → copy original to `FITS_RAW_ARCHIVE` (numeric suffix, never
  overwritten; the operator's file for artistic stacking) → overwrite the original path in place
  (same inode, so the watcher does not re-enqueue it). `catalog_preview.py` converts a throwaway
  copy instead.

### `modules/qc.py`
Metrics without plate solving: FWHM, elongation, SNR, sky background, star count, cosmic-ray
fraction.

| Condition | Flag |
|---|---|
| FWHM > `QC_FWHM_MAX_ARCSEC` | `BLUR` |
| Elongation > `QC_ELONGATION_MAX` | `TRAIL` |
| Sky background > `QC_SKY_BACKGROUND_MAX` | `HIGH_BACKGROUND` |
| Stars < `QC_STARS_MIN` (`QC_STARS_MIN_NARROWBAND` for narrowband), or < 3 raw detections | `LOW_STARS` |
| Multiple issues; read/background/extraction failure; < 3 detections under a bright sky | `BAD` |
| All good | `OK` |

- `LOW_STARS` fires only when BLUR/TRAIL/HIGH_BACKGROUND don't already explain it.
- The FWHM/elongation medians are over **stars**, each filtered on the *other* axis
  (`fwhm_median` over round sources plus a relative pass against extended blobs;
  `elongation_median` over compact ones) — filtering on the measured axis would make both flags
  unreachable. Hot pixels are cut by `STAR_FWHM_MIN_PX` (pixels, not arcsec — a hot pixel's size
  is set by the grid). Fewer than 3 survivors → raw median. `SEP_MIN_AREA` stays 15 on purpose.
- `move_on_reject=True` (the default, for ad hoc callers only) moves the file to
  `FITS_REJECTED/{object}/{FLAG}_filename` with a numeric suffix on collision. `pipeline.py` and
  `catalog_preview.py` always pass `False`.
- `QC_SKY_BACKGROUND_MAX` and `QC_FWHM_MAX_ARCSEC` have no universal default — tune per site.
- Keeps its own copy of the streak-mask helper so its stats match what astrometry extracts.

### `modules/astrometry/`
Package: `_astap.py` (runs astap), `_wcs.py` (read/validate WCS), `_frame_geometry.py`
(centre/FOV/scale/position angle), `_extraction.py` (sep + star filters), `_streak.py`
(streak pre-pass). `solve()` lives in `__init__.py`.

- **astap** runs via `xvfb-run` without `-update` (never writes into the FITS; only `.wcs`/`.ini`/
  `.log` side files), on a worker thread (`asyncio.to_thread`) with `subprocess.run(timeout=)`.
- The fresh `.wcs` side file is **preferred over any WCS already in the header** — capture
  software writes mount-pointing WCS that looks valid but can be arcminutes off. Header WCS is only
  a fallback.
- `_is_plausible_wcs()` checks the solve (sky-valid reference, scale within
  `ASTROMETRY_PIXEL_SCALE_MIN/MAX_ARCSEC`, non-degenerate CD, finite round trip); failure →
  `solve()` returns `{}`. A confidently wrong WCS is worse than none (risk concentrates in the
  `ASTAP_RETRY_WIDE_SEARCH` blind retry).
- **Streak pre-pass** (`config.STREAK_*`): a coarse, low-threshold, non-deblended pass masks
  features that are both highly elongated and far longer than any PSF (satellite trails,
  diffraction spikes), so they don't fragment into fake round "stars". Each masked streak is then
  re-emitted as **one** `_streak=True` detection in `sources_all` only (never photometered), so a
  genuine fast NEO trail still reaches MPC matching / `SPACE_DEBRIS`.
- **FWHM bounds** tighten around the frame's own PSF when known: `[psf/1.5, psf×1.5]` (rejects hot
  pixels and compact galaxies); otherwise `STAR_FWHM_MIN_PX` × scale / `STAR_FWHM_MAX_ARCSEC`.
  Applies to both lists.
- **Elongation** = `a / max(b, 1/√12 px)`, so a degenerate sep fit cannot yield millions.
  The same clamp is duplicated in `qc.py`, `subtraction.py`, `_streak.py`.
- Background RMS is re-measured with the streak mask excluded (it sets threshold and SNR).
- Returns `{ra_center, dec_center, fov_deg, position_angle_deg, pixel_scale_arcsec, naxis1,
  naxis2, sources, sources_all, wcs}`:
  - `sources` — strict star filter; `sources_all` — loose, elongation ceiling
    `SOURCES_ALL_ELONGATION_MAX` (must stay above `SPACE_DEBRIS_EDGE_ELONGATION_MIN`, warned
    otherwise).
  - `saturated` — peak raw ADU ≥ `SATURATION_ADU`; `near_edge` — within `EDGE_MARGIN_FRAC` of an
    edge. Both are wire fields (persisted, so standalone re-runs can use them).
  - `position_angle_deg` — 0° = North up, clockwise toward +X, via pixel↔world round trip.
    Diagnostic only.

### `modules/photometry.py`
Aperture photometry (`photutils`) placed through the **solved** WCS passed in (the header may
still hold a mount estimate), falling back to header then `.wcs` file.

- **Zero point:** differential against every star with a Gaia G magnitude (`_reference_mag()` —
  Gaia DR3 matches and Simbad-named stars carrying `_gaia_mag`), ≥3 required. References are
  screened on Gaia flags (variable, `duplicated_source`, `ruwe > PHOTOMETRY_REF_MAX_RUWE`), falling
  back to the unscreened set if fewer than 3 survive. Saturated stars never count.
  `zero_point_err` uses a small-sample-corrected robust scatter (`_robust_scatter()`).
  A frame whose zero-point standard error exceeds `PHOTOMETRY_MAX_ZERO_POINT_ERR` stays
  uncalibrated.
- **Colour term:** `catalog_mag − mag_inst = zp + k × (BP−RP − color_ref)`, with `k` **fixed per
  filter** in `PHOTOMETRY_COLOR_TERMS` (it belongs to the instrument, and epochs must be
  transformed identically). The per-frame fit is only **logged**, as calibration data for that
  setting. No configured `k` → plain median. Sources with unknown colour use `zp` bare with
  `mag_err` widened by `|k| × color_scatter`.
- **Narrowband** (`skip_calibration=True`): aperture photometry runs, calibration never does.
- **Noise:** `flux_err = sqrt(|net|/gain + area × sky_sigma²)`; gain from `_resolve_gain()`:
  argument → `PHOTOMETRY_GAIN_E_PER_ADU` → header `EGAIN` before `GAIN` (`GAIN` is often a vendor
  setting); implausible → 1.0 with a warning. `snr = flux/flux_err`, overwriting any provisional
  value — except on a subtraction candidate, which keeps its difference-image SNR (the one the
  near-edge rule is tuned for; the aperture value goes to `_aperture_snr`).
- Sky annulus is sigma-clipped (`PHOTOMETRY_SKY_SIGMA_CLIP`).
- **Not measured:** `saturated` sources, `_streak` sources, annulus wider than a quarter frame,
  non-finite flux. Below `PHOTOMETRY_MIN_SNR` the aperture numbers are kept but it stays
  uncalibrated.
- Adds `flux_aperture, flux_err, mag_instrumental, mag_calibrated, mag_err, snr, calibrated,
  edge_flag, zero_point, zero_point_err`. `edge_flag` is the same definition as `near_edge`.

### `modules/subtraction.py`
Catalog-independent detection: difference the new frame against a median stack of archived
frames of the same object. `performed=False` with fewer than `SUBTRACTION_MIN_FRAMES` references.

1. **Reference selection** — from the object directory's filter subdirectories (plus legacy flat
   files); same filter preferred (fallback: any), the new frame's own file excluded. Never
   Dark/Flat/Bias (parsed positionally from the filename — a defence for un-migrated archives).
   `_screen_by_quality()` first drops any candidate whose `TELESCOP`/`INSTRUME` differs from the
   new frame's, or whose archived solve's plate scale (header `XPIXSZ`+`FOCALLEN` for an unsolved,
   QC-failed frame) is off the new frame's by more than `SUBTRACTION_REF_MAX_SCALE_DIFF` (INDI's
   `TELESCOP` is the mount's name, so the scale is what separates two telescopes on one mount) —
   **no fallback**: another optics' frame makes a wrong
   difference image, not a degraded one. It then excludes `QCFLAG != OK` and `QCFWHM` worse than
   `SUBTRACTION_REF_MAX_FWHM_RATIO` × the new frame's; if that leaves too few, the
   same-instrument unscreened set is used. Frames without a keyword are kept on that keyword.
   Position-angle closeness ranks the whole candidate list as a soft tiebreaker.
2. **Pre-rotation** (`_prerotate_reference()`) by the known PA difference above
   `SUBTRACTION_PREROTATE_MIN_DEG`, growing the canvas (`reshape=True`) and marking fill as invalid.
   Never excludes a reference for rotation — astroalign handles even 180°.
3. **Alignment** with `astroalign` (shape mismatches are fine; it resamples onto the new grid).
4. **Flux scaling** of the stack by `(t_new/t_ref) × (g_ref/g_new)` (`_flux_scale_factor()`; each
   factor → 1.0 if a keyword is missing). Applied to the stack only, so saturation masking still
   sees raw reference ADU.
5. **Median stack** honouring each reference's astroalign footprint and excluding non-finite
   pixels; uncovered pixels take the new frame's value (difference = 0).
6. **Masks:** vicinity of saturated pixels in the new frame or any reference
   (`SATURATION_MASK_RADIUS_ARCSEC`); streaks on the diff image (own copy of the pre-pass,
   `minarea=5`); non-finite new-frame pixels. Background/RMS re-measured with masks excluded.
7. **Detection** at `SUBTRACTION_DETECT_SIGMA`; `snr` corrected for interpolation-correlated noise
   (`_noise_correlation_factor()`, capped by `SUBTRACTION_NOISE_CORR_MAX`). Candidates sharper than
   `psf/1.5` are rejected (the new frame's own hot pixels survive subtraction).
8. **Edge zone** (`EDGE_MARGIN_FRAC`): candidates survive only if round
   (`SUBTRACTION_EDGE_ELONGATION_MAX`) and strong (`SUBTRACTION_EDGE_SNR_MIN`) — coma residuals
   are elongated and weak.
9. Pixel → sky via the **solved** WCS passed in. Returns
   `{performed, reference_frame_count, candidates}`, candidates tagged `_from_subtraction=True`.

**Camera rotation** (e.g. meridian flips): catalog matching is unaffected (every frame is solved
independently and matching is by RA/Dec). Only pixel-level steps care — subtraction (above) and
the finder chart crops. Coma residuals fixed to the sensor cannot be removed by any rotation;
they're handled by the edge/saturation/streak/FWHM filters.

### `modules/catalog_matcher/`
Package: one file per catalog (`_simbad.py`, `_gaia.py`, `_2mass.py`, `_panstarrs.py`, `_mpc.py`),
plus `_cache.py`, `_wcs_offset.py`, `_match.py`. `_match.py` calls catalogs through qualified
submodule references (`_gaia._query_gaia(...)`) so tests can patch them.

- **Order (sequential exclusive):** Simbad → Gaia DR3 → 2MASS → Pan-STARRS DR1 → MPC/SkyBot, cone
  `MATCH_CONE_ARCSEC`. Simbad first for real object types.
- **SkyBot retries** (`SKYBOT_MAX_ATTEMPTS`, doubling delay) with astroquery's HTTP cache
  **off**: IMCCE intermittently returns a non-VOTable body ("No table found"), and astroquery
  caches the raw response before parsing, so a cached bad answer would defeat every retry.
  Failures are never cached by `_cache` either.
- **MPC is the exception:** it sees every source. It takes over a stellar-claimed source only when
  the ephemeris position is within `MATCH_CONE_ARCSEC` (a blend); otherwise it matches the nearest
  *unclaimed* source within `MOVING_CONE_ARCSEC`. Takeovers log both identities.
- **Gaia proper motion:** every Gaia star is propagated from `ref_epoch` to `obs_time` once in
  `match()` (copies — the cached list is shared). `get_gaia_stars()` returns un-propagated
  positions; forced photometry propagates itself.
- **Gaia data on Simbad-named stars:** `_attach_gaia_color()` adds `_gaia_mag`, `_catalog_color`
  and quality flags, so calibration doesn't depend on which catalog named the star. Not for MPC
  objects; never overwrites.
- **WCS offset correction:** a Gaia vote accumulator estimates a systematic offset and applies it
  in place to every source's `ra`/`dec` — skipped when the median separation is already ≤ 2″.
  `pipeline._apply_wcs_offset_to_solution()` then shifts the solved WCS's `CRVAL` (and the frame
  centre) by the same offset, so photometry, forced photometry and the archived header project
  sky positions onto the right pixels.
- Matched sources gain `catalog_name, catalog_id, catalog_mag, object_type` (`catalog_mag`: G for
  Gaia, J for 2MASS, r for Pan-STARRS, `None` for Simbad/MPC). Unmatched → `catalog_name = None`.
- **Cache:** in-process dict + files in `CATALOG_CACHE_DIR`, TTL `CACHE_TTL_HOURS`. Keys are 0.1°
  **tiles**: each catalog queries around the tile centre with the tile half-diagonal added to the
  radius (`_cache_position()`, `_cache_radius_margin_deg()`). Disk-write failures degrade to
  in-memory only. The TTL is read at call time (remote settings arrive after import); a disk hit
  keeps the file's own age.

### `modules/forced_photometry.py`
Reverse matching ("precovery"): for every Gaia DR3 star (≤ `FORCED_PHOTOMETRY_MAG_LIMIT`) and
MPC object in the footprint with no source carrying its `catalog_id`, measure flux at the
predicted pixel. A single-hypothesis test justifies the low `FORCED_PHOTOMETRY_MIN_SNR`.

- No extra network calls: `get_gaia_stars()`/`get_mpc_objects()` hit the cache `match()` just
  filled.
- Gaia positions are proper-motion propagated to `obs_time_mid`; MPC positions are already at
  epoch.
- Photometry math (aperture sizing, gain, sigma-clipped sky, colour term) is a hand-kept copy of
  `photometry.py`'s.
- **Skipped:** aperture off-frame; saturated pixel inside the circular aperture (`_aperture_max()`);
  another catalog entry within `FORCED_PHOTOMETRY_BLEND_FWHM` × FWHM; significance below the SNR
  threshold (dropped — the wire has no "upper limit" field); calibrated magnitude off Gaia G by
  more than `FORCED_PHOTOMETRY_MAX_CATALOG_DEVIATION_MAG` (it measured something else).
- Results look like ordinary catalog-matched sources plus `_forced_photometry=True` (stripped
  before the wire — not yet persisted). Gated by `FORCED_PHOTOMETRY_ENABLED`; any failure → `[]`.

### `modules/anomaly_detector/`
Package split by concern (`types.py`, `_otypes.py`, `_geometry.py`, `_history.py`, `_movement.py`,
`_prefetch.py`, `_classify.py`, `_ephemeris_resolution.py`, `_detect.py`); see its `__init__.py`
and **[docs/anomaly-detector.md](docs/anomaly-detector.md)** for the mechanics and flowchart.

- **Prefetch** (batched, one call per frame): a **narrow** history query (`MATCH_CONE_ARCSEC`)
  around every source (existence + light curve), a **wide** one only around uncatalogued sources
  with `uncatalogued_only`, and `POST /frames/covering/batch` for coverage. A failed request
  (the API client returns `None`, never `{}`) raises `HistoryUnavailable`: classifying without
  history would read every source as never observed.
- Every anomaly carries `source_id` (from `_source_id`, or `None`).
- **Classification table:**

| Situation | Result |
|---|---|
| MPC match | `ASTEROID` / `COMET` (+ JPL Horizons ephemeris) |
| Unmatched + `saturated` | suppressed, unless `_could_be_a_new_bright_object()` (round, interior, nothing ever seen at a covered position) |
| Unmatched + `near_edge` | suppressed, unless a subtraction candidate passing `_survives_edge_zone()` |
| Unmatched, no detection within `MATCH_CONE_ARCSEC`, elongation > `SPACE_DEBRIS_ELONGATION_MIN` (`…_EDGE_…` near the edge) | `SPACE_DEBRIS` — recorded, not an alert; no "vacated position" proof needed |
| Unmatched, shifted (see below), not near edge | `MOVING_UNKNOWN` → alert |
| Uncatalogued subtraction candidate within `SUBTRACTION_RESIDUAL_RADIUS_FWHM` × FWHM of a catalogued star within `SUBTRACTION_RESIDUAL_MAX_DMAG` | suppressed (that star's residual) |
| No coverage, normal detection | `FIRST_OBSERVATION` — logged only, not posted |
| No coverage, subtraction candidate, catalogued | suppressed (registration residual) |
| No coverage, subtraction candidate, uncatalogued | `UNKNOWN` → alert |
| Covered, no history, near a Simbad galaxy | `SUPERNOVA_CANDIDATE` → alert |
| Covered, no history, catalogued | `KNOWN_CATALOG_NEW` — logged only, not posted |
| Covered, no history, uncatalogued | `UNKNOWN` → alert |
| History, significant brightening, near galaxy | `SUPERNOVA_CANDIDATE` → alert |
| History, significant Δmag, Simbad binary / variable | `BINARY_STAR` / `VARIABLE_STAR` |
| History, significant Δmag beyond `VARIABILITY_SIGMA` × own same-filter scatter over ≥ `VARIABILITY_MIN_EPOCHS`, not a galaxy/cluster | `VARIABLE_STAR` (light-curve based; stated in `notes`) |

- **"Shifted"** needs both: no history within `MATCH_CONE_ARCSEC` of the current position, **and**
  a historical detection in the wide cone whose position is now empty. The wide radius is
  per-candidate: `MOVING_CONE_ARCSEC` floor, extended to `MOVING_RATE_ARCSEC_PER_MIN × gap` for
  gaps ≤ `MOVING_EXTEND_MAX_GAP_MIN`, capped at `MOVING_CONE_MAX_ARCSEC`.
- **Significant Δmag** = above `DELTA_MAG_ALERT` **and** above `VARIABILITY_SIGMA` × (`mag_err` ⊕
  historical scatter), **and confirmed** by the previous `VARIABILITY_CONFIRM_EPOCHS − 1`
  same-filter epochs (one deviant epoch is usually a cosmic ray). Uses the calibrated `mag` only.
- Δmag history is restricted to the **same filter** (`_same_filter_history()`, current filter in
  `_filter`, historical `filter` joined by the API). The existence check stays filter-agnostic.
- `object_type` comes only from Simbad; Gaia/2MASS/Pan-STARRS set a generic `"STAR"`.
- Ephemerides: `ephemeris.py` per designation, gathered with `return_exceptions=True` so one
  failure costs only that anomaly its ephemeris.
- The 10 `AnomalyType` values are mirrored by hand as an `ENUM` in observatory-api
  (`AnomalyModel::ALLOWED_TYPES`); an unknown type is rejected with 400. Change both together.

### `modules/ephemeris.py`
JPL Horizons via `astroquery.jplhorizons` → predicted RA/Dec, mag, distance, angular velocity.
The synchronous client runs in `asyncio.to_thread()` under `EPHEMERIS_TIMEOUT_SEC`; any failure
returns `None`.

### `modules/finder_chart/`
Per-source finder/discovery charts, always fully regenerated from the source's complete track and
uploaded to the API (`POST /sources/{id}/chart`, one request per source+style). Package: `_style.py`
(routing), `_io.py` (FITS loading, stretch, PNG/GIF assembly, rotation helpers), and one file per
renderer. The orchestrator stays in `__init__.py` because tests patch renderers on the package
namespace.

| Style | When | Shows |
|---|---|---|
| `track` | `ASTEROID`, `COMET`, `MOVING_UNKNOWN`, `SPACE_DEBRIS` | latest epoch's crop, every epoch's position projected onto it, colour-graded track with direction arrow and a legend with separation/time gap/″/hr |
| `stamp_strip` | other types, 2+ epochs | one crop per epoch from its own frame — a blink strip |
| `before_after` | a single detected epoch (any type) | before/after pair |
| `track_gif`, `stamp_strip_gif` | companions to the two above (`CHART_GIF_ENABLED`) | independent renderers: fixed canvas, each frame from that epoch's own pixels |

- Flow: `POST /sources/tracks/batch` → thin to `CHART_MAX_EPOCHS`, evenly spaced in time with the
  first and last kept (`_select_epochs_evenly()`) → load each epoch from
  `archive_layout.find_archived(filename, object)` (missing files skipped) → render each distinct style the
  source's anomaly types imply (`_group_types_by_style()`) → upload. API side stores
  `UNIQUE(source_id, style)`.
- Title: `anomaly_type`, plus `(designation)` when catalogued.
- Crops from different epochs are pre-rotated to the latest epoch's orientation above
  `CHART_PREROTATE_MIN_DEG`.
- Returns `source_id → anomaly_type → bool`. Best-effort: failures only affect that source/style;
  a GIF failure never downgrades the static result. Gated by `CHART_ENABLED`.
- The pipeline renders (only it has the FITS files); the API stores and serves (only it has an
  HTTP server).

### `modules/catalog_preview.py`
Backs `PREVIEW_CATALOG_MATCH`: `render(fits_path)` runs real QC (`move_on_reject=False`) →
`astrometry.solve()` → `subtraction.run()` → `catalog_matcher.match()` and draws `sources_all`
(green + label = matched, red = unmatched) at their originally detected positions with astap's
WCS. Output is in-memory PNG bytes only; astap side files go to a `TemporaryDirectory`. Never
touches its input (CFA frames are converted in a throwaway copy).

### `api_client/`
Package split by resource (`frames.py`, `sources.py`, `anomalies.py`, `tasks.py`, `settings.py`,
shared `_shared.py`), all re-exported from `__init__.py`. `httpx` async + `tenacity` retries.
Sends `X-API-Key`, JSON headers and `X-Pipeline-Version` (`version.py`, the top `CHANGELOG.md`
heading; the API shows it on its dashboard). Older single-position endpoints (`/sources/near`,
`/frames/covering`, `/sources/{id}/track`) are still exported but unused.

`_to_wire_source()`: renames `_from_subtraction` → `from_subtraction` (only when truthy), strips
every other leading-underscore key. **Convention: a leading underscore means "internal, not for
the wire".** The API is treated as a black box — only endpoint contracts matter.

---

## Filters

A Light frame is always analyzed; the filter only changes what its results can be trusted for.
Broadband: `U`/`V`/`I`, SDSS, `L`, and `OSC` (its own token — different bandpass from mono `L`).
Narrowband: `Ha`, `OIII`, `SII`, `NII` and multi-band OSC filters (`LeNhance`, `LeXtreme`,
`LuLtimate`, `NBZ`, `QuadBand`, `TriBand`, `DuoBand`) — `config.NARROWBAND_FILTERS`. Narrowband
frames genuinely show fewer, fainter stars.

| Stage | Filter-dependent? | Behaviour |
|---|---|---|
| QC star floor | yes | `QC_STARS_MIN_NARROWBAND` for narrowband |
| Astrometry, catalog matching | no | |
| Gaia zero point | yes | skipped for narrowband; colour term per filter |
| Subtraction | yes | same-filter reference stack preferred |
| Δmag comparison | yes | same-filter history only; existence check filter-agnostic |

Position-only classifications (`ASTEROID`/`COMET`/`MOVING_UNKNOWN`/`SPACE_DEBRIS`, subtraction
`UNKNOWN`) don't depend on the filter.

## External catalogs

| Catalog | Access | Notes |
|---|---|---|
| Simbad | `Simbad.query_region()` | named objects with types; CDS ~5–6 req/s |
| Gaia DR3 | `Gaia.cone_search()` | ~1.8B stars, complete to ~20–21 mag; proper motions |
| 2MASS | `Vizier` `II/246` | red/cool stars |
| Pan-STARRS DR1 | `Vizier` `II/349/ps1` | to ~23.3 mag; only for `dec_center > -30°` |
| MPC/SkyBot | `Skybot.cone_search()` at `obs_time_mid` | radius = half-diagonal + `MOVING_CONE_ARCSEC` |
| JPL Horizons | `astroquery.jplhorizons` | ephemerides (`ephemeris.py`) |

All catalog queries are cached (see `catalog_matcher` above) and their errors are caught and
logged — a catalog failure never stops a frame.

---

## Design decisions

- **Pipeline → API, never → DB:** the pipeline is a write-only science client; the API owns
  integrity and serves the website too.
- **astap over astrometry.net:** fully offline, ~2–5 s per frame.
- **sep for extraction, photutils for apertures:** sep is SourceExtractor's C code (fast in bulk).
- **Image subtraction as well as catalogs:** catalogs can only say "not catalogued"; subtraction
  says "this wasn't here before, at the pixel level".
- **Saturation is a flag, not a filter:** the position is real (bright asteroid, flaring star);
  only the magnitude is meaningless, so photometry refuses it and the detector suppresses only
  *uncatalogued* saturated sources (with the new-bright-object exemption).
- **Coverage check:** without `POST /frames/covering/batch`, the first observation of every field
  would alert on every source.
- **QC-failed frames are registered and archived**, not dropped — the operator needs to see what
  failed and why; subtraction's `QCFLAG` screen keeps them out of reference stacks.
- **Cone searches in the API:** MariaDB has no pgSphere; bounding box on indexed `(ra, dec)` +
  Haversine in PHP.

## Coding conventions

- Python 3.11+, type hints on all signatures, `async/await` for API calls (`httpx.AsyncClient`).
- All configuration via `config.py`; no magic strings in modules.
- Each module exposes one primary async function (`await qc.analyze(fits_path)`).
- `logging` with `extra={"fits_filename": ...}` (not `filename` — reserved on `LogRecord`). The
  current log format doesn't print it yet, and `frame_id` only appears in message text.
- External catalog errors are caught and logged; the frame continues with partial results.
- Small helpers (streak mask, elongation clamp, position angle, photometry math) are deliberately
  **hand-duplicated** across modules and kept in sync by hand; `resolve_pixel_scale_arcsec()` is
  one of the few shared ones.
- Tests in `tests/` use `pytest` and mock every external call (API, catalogs, astap).
- **All Markdown in this repo is written in English**, whatever language the request was in.

## Versioning and releases

The version lives in one place: the top `## X.Y.Z` heading of `CHANGELOG.md` (repo root). There
is no other version field to keep in sync.

**While working on a branch:** every change worth noting gets one short line in the top section
of `CHANGELOG.md`. If the top section is already released (its `vX.Y.Z` tag exists), start a new
one above it first. Format, as in the existing sections:

```markdown
## 1.2.0

### Minor Changes

- Tasks: each pipeline polls only the tasks it may claim
```

- `### Patch Changes` by default; `### Minor Changes` for noticeable new features or behaviour
  changes (a new endpoint or task type, a schema migration, a changed contract); a major bump
  practically never.
- One line per change, `- Area: what changed`, written for someone deploying it, English only.
- A change that needs the matching release of observatory-api says so on its line, e.g.
  `(requires observatory-api 1.2.0)` — the two are deployed separately.

**Release branch:** a version's work is collected on `release/X.Y.Z` (e.g. `release/1.2.0`),
branched from `main`; feature branches for that version merge into it, or commit there directly.

**Pull request:** from the release branch into `main`, titled with the new version (`v1.2.0`),
the body summarising that version's `CHANGELOG.md` section.

## Release

Only when asked, and only after the version's PR is merged into `main`:

1. `git checkout main && git pull` — release what is on `origin/main`, nothing local.
2. Tag the merge commit with `v` + the top `CHANGELOG.md` version and push the tag:
   `git tag v1.2.0 && git push origin v1.2.0`.
3. Create the GitHub release for that tag: title is the tag name, body is that version's section
   from `CHANGELOG.md` without the `## X.Y.Z` heading (it starts with `### Minor Changes` /
   `### Patch Changes`), marked as latest:
   `gh release create v1.2.0 --title v1.2.0 --notes-file <section.md> --latest`.

## Known issues

1. **Faint `UNKNOWN` sources (mag > 20).** Faint stars below Gaia's limit and absent from
   Pan-STARRS are still flagged `UNKNOWN` — there is no magnitude threshold in the `UNKNOWN`
   branch (`modules/anomaly_detector/_classify.py`). Ideas, not implemented: a magnitude cutoff,
   SDSS DR17 as a further catalog, a `FAINT_UNCATALOGUED` type (only a `TODO` comment so far; would
   need the API enum too).
2. **`QC_SNR_MIN` is configured but not enforced.** `qc.py` reports `snr_median` but never
   compares it against the threshold.
