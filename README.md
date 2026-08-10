# Observatory FITS Analysis Pipeline

[![Tests](https://github.com/miksrv/observatory-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/miksrv/observatory-pipeline/actions/workflows/tests.yml)

Automated Python service for processing astronomical FITS frames from an observatory telescope. Runs on a dedicated observatory server, performs quality control, plate solving, source extraction, catalog cross-matching, anomaly detection, and reports everything to a remote REST API.

---

## Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Modules](#modules)
- [Anomaly Detector Deep-Dive](docs/anomaly-detector.md)
- [Deployment with Docker](#deployment-with-docker)
  - [1. Install Docker](#1-install-docker)
  - [2. Prepare directories on the host](#2-prepare-directories-on-the-host)
  - [3. Download ASTAP star catalogs](#3-download-astap-star-catalogs)
  - [4. Configure environment variables](#4-configure-environment-variables)
  - [Configuration Reference](#configuration-reference)
  - [Remote Configuration via API](#remote-configuration-via-api)
  - [5. Build and start the container](#5-build-and-start-the-container)
  - [External volumes reference](#external-volumes-reference)
  - [What installs automatically](#what-installs-automatically)
  - [Updating the pipeline](#updating-the-pipeline)
- [Running Locally (without Docker)](#running-locally-without-docker)
- [File Organization](#file-organization)
- [Tests](#tests)
- [API Endpoints Used](#api-endpoints-used)
- [Dependencies](#dependencies)
- [Security](#security)
- [Task Tracking](#task-tracking)

---

## Overview

The pipeline monitors an incoming directory for new FITS files. Each file is processed through a sequence of steps: quality check → plate solving → image subtraction → catalog matching → photometry → anomaly detection → archiving. Bad frames are rejected locally without involving the API, keeping the remote database clean.

```
New FITS file
     │
     ▼
fits_header.extract_headers()   ← parse metadata
     │
     ▼
qc.analyze()                    ← check quality
     ├─ BAD → /fits/rejected/{object}/   STOP
     └─ OK ──────────────────────────────┐
                                         ▼
                              astrometry.solve()    ← plate solve + source list
                                         │
                                         ▼
                              subtraction.run()     ← diff vs archived frames (optional)
                                         │
                                         ▼
                         catalog_matcher.match()    ← Simbad + Gaia DR3 + 2MASS + Pan-STARRS + MPC
                                         │
                                         ▼
                              photometry.measure()  ← calibrated magnitudes (Gaia zero-point)
                                         │
                                         ▼
                         api_client.post_frame()    ← register frame, get frame_id
                                         │
                                         ▼
                         api_client.post_sources()  ← save all sources
                                         │
                                         ▼
                         anomaly_detector.detect()  ← classify anomalies
                                         │
                                         ▼
                         api_client.post_anomalies()
                                         │
                                         ▼
                         finder_chart.update_charts_for_sources()  ← per-source discovery chart (optional)
                                         │
                                         ▼
                         /fits/archive/{object}/    ← archive frame
```

> **Note:** catalog matching and photometry run *before* the frame/sources are posted to the
> API (not after, as older diagrams of this pipeline showed) — this lets matched Gaia DR3 stars
> serve as the photometric zero-point reference. Image subtraction is a newer step, inserted
> between astrometry and catalog matching. Finder-chart generation is the newest step, running
> after anomalies are posted — for every anomaly with a resolved `source_id` it (re)builds a
> small PNG from that source's full detection history and uploads it to the API.

---

## Architecture

This repository is one of two components:

| Component | Language | Role |
|---|---|---|
| **observatory-pipeline** (this repo) | Python 3.11 | Runs on the observatory server. Heavy astronomical computation, file management, API client. |
| **observatory-api** | PHP / CodeIgniter 4 | Runs on cloud hosting. REST API, MariaDB persistence, website backend. |

The pipeline communicates with the API exclusively over HTTPS using an API key. It has **no direct database access** — all persistence goes through API calls.

```
[Observatory Server]                     [Cloud Hosting]
┌─────────────────────────┐             ┌──────────────────────────┐
│  docker-compose         │             │  CodeIgniter 4 API       │
│  ┌───────────────────┐  │   HTTPS +   │  ┌────────────────────┐  │
│  │  pipeline service │──┼──API Key───▶│  │  REST endpoints    │  │
│  └───────────────────┘  │             │  └────────────────────┘  │
│                         │             │           │              │
│  Volumes (on host disk):│             │  ┌────────▼───────────┐  │
│  /data/fits/incoming    │             │  │  MariaDB           │  │
│  /data/fits/archive     │             │  └────────────────────┘  │
│  /data/fits/rejected    │             └──────────────────────────┘
│  /data/astap/catalogs   │
└─────────────────────────┘
```

---

## Project Structure

```
observatory-pipeline/
├── CLAUDE.md                  ← AI assistant context and project spec
├── Dockerfile
├── docker-compose.yml
├── .env.example               ← environment variable template
├── requirements.txt
├── config.py                  ← loads all settings from .env
├── watcher.py                 ← entry point, monitors incoming folder
├── pipeline.py                ← orchestrator for a single FITS file
│
├── docs/
│   ├── API.md                 ← REST API endpoint reference
│   ├── ISSUES.md              ← open data-quality questions for observatory-api
│   └── anomaly-detector.md    ← deep-dive into modules/anomaly_detector.py
│
├── modules/
│   ├── fits_header.py         ← extract FITS headers into structured dict
│   ├── normalizer.py          ← normalize object/filter/frame-type names and filenames
│   ├── qc.py                  ← quality control: bad frame detection & rejection
│   ├── astrometry.py          ← plate solving (astap) + source extraction (sep)
│   ├── photometry.py          ← aperture photometry (photutils)
│   ├── subtraction.py         ← image subtraction: align + diff archived frames (transients/movers)
│   ├── catalog_matcher.py     ← cross-match: Simbad, Gaia DR3, 2MASS, Pan-STARRS DR1, MPC
│   ├── anomaly_detector.py    ← comparison with history + anomaly classification
│   ├── ephemeris.py           ← JPL Horizons queries for solar system objects
│   └── finder_chart.py        ← per-source discovery-chart PNG (track / stamp-strip) generation + upload
│
├── api_client/
│   └── client.py              ← all HTTP calls to observatory-api
│
└── tests/
    ├── test_fits_header.py
    ├── test_normalizer.py
    ├── test_qc.py
    ├── test_astrometry.py
    ├── test_photometry.py
    ├── test_api_client.py
    ├── test_catalog_matcher.py
    ├── test_ephemeris.py
    ├── test_anomaly_detector.py
    ├── test_subtraction.py
    ├── test_finder_chart.py
    └── test_pipeline.py
```

> Every module in `modules/` has dedicated unit tests, including `subtraction.py`.

---

## Modules

### `watcher.py`
Entry point. Uses `watchdog` to monitor `FITS_INCOMING` for new `.fits` / `.fit` files. Does not call the pipeline itself — buffers arriving paths and, after a `WATCHER_DEBOUNCE_SEC` quiet period (or immediately if `WATCHER_MAX_BATCH_SIZE` is reached), submits everything buffered as one `ANALYZE` task to observatory-api's job queue. `worker.py` is what actually processes each item. See CLAUDE.md's `watcher.py` and "Job queue" sections for the full design.

### `pipeline.py`
Three independently callable stages, plus `run()` composing all three for one file (still what `watcher.py` calls): `analyze_frame()` (QC → astrometry → image subtraction → catalog matching → photometry → `post_frame`/`post_sources` → archive — catalog matching and photometry intentionally run *before* the frame is posted to the API, so matched Gaia DR3 stars can be used as the photometric zero-point reference), `detect_anomalies_for_frame_data()`/`detect_anomalies_for_frame_id()` (anomaly detection → `post_anomalies`), and `generate_charts_for_anomalies()`/`generate_charts_for_source_ids()` (finder charts). Each stage handles failures in the modules it calls gracefully — a crash in catalog matching does not abort the frame.

### `worker.py`
Entry point for the job-queue worker (separate `docker-compose.yml` service). Polls observatory-api's `GET /tasks?status=PENDING` and dispatches each task's items to the matching `pipeline.py` stage — see CLAUDE.md's "Job queue" section. Lets any of the three production stages be re-run independently (e.g. re-classifying an object's whole observation history under a fixed anomaly-detection bug, without re-running astrometry/photometry on every frame again). Also dispatches a fourth, non-production task type, `PREVIEW_CATALOG_MATCH` — a diagnostic tool (see `modules/catalog_preview.py` below) that renders a PNG per frame with detected sources circled green (catalog-matched) or red (unmatched) and uploads it to observatory-api, without ever registering a frame/source or touching the input file.

### `modules/catalog_preview.py`
Backs `PREVIEW_CATALOG_MATCH`. Runs `qc.analyze()` (never moving a rejected frame — this is a read-only diagnostic) → `astrometry.solve()` → `subtraction.run()` → `catalog_matcher.match()` on a frame, then renders every detected source as a circle on the frame's own pixel data straight into an in-memory PNG (no local file left behind — `pipeline.preview_catalog_match()` uploads the bytes via `POST /tasks/{task_id}/items/{item_id}/chart` and that's the only place the image ends up). Calls the real `catalog_matcher.match()`, so repeated frames of the same object/session in one task benefit from its on-disk cache exactly like a production `ANALYZE` run.

### `modules/fits_header.py`
Reads the FITS primary header using `astropy.io.fits`. Normalizes keyword aliases (e.g., `CCD-TEMP` vs `CCDTEMP`, `EXPTIME` vs `EXPOSURE`, and pixel-scale aliases `XPIXSZ`/`PIXSIZE`/`PIXSCALE1`/`PIXELSZ`/`PIXSCALE`). Returns a structured dict ready for the API payload.

### `modules/normalizer.py`
Normalizes FITS header values and filenames for consistency across different capture software: object names (`M 51` → `M51`, plus Caldwell/Sharpless/Abell/UGC/PGC/MCG/Mrk/Arp/VCC/ESO catalog prefixes), filter names (`Blue` → `B`, `Luminance` → `L`, `H-Alpha` → `Ha`, plus `G`, `NII`, and standard Johnson-Cousins/SDSS filters), and frame types (`Light Frame` → `Light`, `domeflat` → `Flat`, `science` → `Light`). Also generates the standardized filename `{Object}_{Type}_{Filter}_{Exp}_{DateTime}.fits` when `NORMALIZE_ENABLED=true`.

### `modules/qc.py`
Computes quality metrics without plate solving:

| Metric | Description |
|---|---|
| **FWHM** | Median star sharpness in arcseconds — indicates focus quality |
| **Elongation** | Major/minor axis ratio — indicates tracking or trailing |
| **SNR** | Median signal-to-noise ratio of detected sources |
| **Sky background** | Median background level after sigma clipping |
| **Star count** | Minimum number of detectable stars |
| **Cosmic ray fraction** | Via `astroscrappy` |

Quality flags:

| Flag | Condition | Action |
|---|---|---|
| `OK` | All metrics pass | Continue processing |
| `BLUR` | FWHM > threshold | Move to `/fits/rejected/{object}/BLUR_*.fits` |
| `TRAIL` | Elongation > threshold | Move to `/fits/rejected/{object}/TRAIL_*.fits` |
| `LOW_STARS` | Star count < minimum | Move to `/fits/rejected/{object}/LOW_STARS_*.fits` |
| `BAD` | Multiple issues | Move to `/fits/rejected/{object}/BAD_*.fits` |

Bad frames are never sent to the API — this keeps the remote database clean.

### `modules/astrometry.py`
Calls the `astap` binary as a subprocess (via `xvfb-run`, since astap needs a display even headless) for plate solving. Parses the resulting WCS header written back into the FITS file. Runs `sep` (SourceExtractor Python wrapper) for source detection. Converts pixel coordinates to (RA, Dec) using `astropy.wcs.WCS`.

Returns a dict with `ra_center`, `dec_center`, `fov_deg`, `naxis1`/`naxis2`, the `wcs` object itself, and two source lists: `sources` (strict star filter) and `sources_all` (a looser filter that also keeps bright/saturated and faint detections, used for catalog matching and WCS offset correction so moving/transient objects aren't lost). Every source also carries a `saturated` bool (peak ADU ≥ `SATURATION_ADU`) and a `near_edge` bool (pixel position within `EDGE_MARGIN_FRAC` of any frame edge — see the anomaly detector's `SPACE_DEBRIS` classification below).

### `modules/photometry.py`
Aperture photometry via `photutils`. Performs differential photometry against Gaia DR3 reference stars in the field (requires ≥3 Gaia matches for a zero-point) — this makes brightness measurements immune to atmospheric transparency variations. Adds `flux_aperture`, `mag_instrumental`, `mag_calibrated`, `mag_err`, `calibrated`, `zero_point` (and a few more) to each source. A source flagged `saturated=True` is never measured (its aperture flux is not physically meaningful) and is excluded from the Gaia zero-point reference set.

### `modules/subtraction.py`
Image subtraction (difference imaging) — a second detection path for transients and moving objects that catalog matching alone would miss. If ≥`SUBTRACTION_MIN_FRAMES` frames of the same object are already archived, aligns them with `astroalign`, builds a median reference image, subtracts it from the new frame, masks out the vicinity of any saturated pixel (`SATURATION_MASK_RADIUS_ARCSEC`, to suppress astroalign residual artifacts around bright stars), and runs `sep` on the (masked) difference image (threshold `SUBTRACTION_DETECT_SIGMA`). Candidates are merged into the source list, flagged `_from_subtraction=True` so `anomaly_detector.py` can treat them with different coverage rules. Skipped gracefully when too few reference frames exist yet — e.g. for a brand-new target.

### `modules/catalog_matcher.py`
Cross-matches the source list against external catalogs using `astropy.coordinates.SkyCoord.match_to_catalog_sky()` with cone radius `MATCH_CONE_ARCSEC` (`MOVING_CONE_ARCSEC` for MPC). Also computes a Gaia-based WCS offset correction applied in-place to every source's coordinates before the remaining catalogs are queried.

Catalogs, queried in order:
1. **Simbad** — named objects: variable stars, double stars, galaxies, nebulae — provides rich `object_type`
2. **Gaia DR3** — ~1.8 billion stars, precise positions and G-band magnitudes; drives the WCS offset correction
3. **2MASS** — fallback for red/cool stars faint or absent in Gaia; J-band magnitude
4. **Pan-STARRS DR1** — fallback for faint optical sources below Gaia's completeness limit (δ > −30° only); r-band magnitude
5. **MPC** — all known asteroids and comets with orbital elements, at the observation epoch

### `modules/anomaly_detector.py`
Core science logic. Queries the API in a single batched call per frame (`POST /sources/near/batch`, `POST /frames/covering/batch`) for historical observations near every source at once, then classifies:

`anomaly_type` is a fixed enum of 10 values — `AnomalyType(str, Enum)` in `modules/anomaly_detector.py`, mirrored by `AnomalyModel::ALLOWED_TYPES` and an `ENUM` column constraint on the `observatory-api` side:

| `anomaly_type` | When assigned | Alert? |
|---|---|---|
| `FIRST_OBSERVATION` | Sky area never observed before | No |
| `KNOWN_CATALOG_NEW` | Not in history, but found in a catalog (Simbad/Gaia/2MASS/Pan-STARRS) — was simply below the detection threshold before | No |
| `VARIABLE_STAR` | Has history, Δmag > `DELTA_MAG_ALERT`, Simbad classifies it as a known variable | No (logged) |
| `BINARY_STAR` | Has history, Δmag > `DELTA_MAG_ALERT`, Simbad classifies it as a known binary | No (logged) |
| `ASTEROID` | Shifted source, matched in MPC/SkyBot as an asteroid | No (logged + ephemeris) |
| `COMET` | Shifted source, matched in MPC/SkyBot as a comet | No (logged + ephemeris) |
| `SUPERNOVA_CANDIDATE` | New point source with no history near a Simbad galaxy, or an already-known galaxy brightening beyond `DELTA_MAG_ALERT` | **YES** |
| `MOVING_UNKNOWN` | Shifted source, not in MPC, elongation at or below the trail threshold | **YES** |
| `SPACE_DEBRIS` | Shifted source, not in MPC, elongation above the trail threshold — `SPACE_DEBRIS_ELONGATION_MIN` (3.0), or `SPACE_DEBRIS_EDGE_ELONGATION_MIN` (6.0) near the frame edge (fast trail) | **YES** |
| `UNKNOWN` | New point source, not in any catalog, area covered — or detected via image subtraction regardless of coverage | **YES** |

`FAINT_UNCATALOGUED`, a proposed classification for faint uncatalogued sources, is not implemented yet (see [CLAUDE.md](CLAUDE.md) Known Issues).

> A full deep-dive into this module — the batch prefetch strategy, the exact classification priority order, and a flowchart diagram — lives in **[docs/anomaly-detector.md](docs/anomaly-detector.md)**.

### `modules/ephemeris.py`
Queries JPL Horizons via `astroquery.jplhorizons`. Given an MPC designation and observation time, returns predicted (RA, Dec, magnitude, distance in AU, angular velocity in arcsec/hour).

### `modules/finder_chart.py`
For every anomaly with a resolved `source_id`, (re)generates and uploads a small PNG showing every
frame that source has ever been detected on, with its position circled on each — fully
regenerated from the source's complete track on every new epoch, never patched in place. Two
styles, chosen by `anomaly_type`: a **track** chart (`ASTEROID`/`COMET`/`MOVING_UNKNOWN`/
`SPACE_DEBRIS`) — one background frame with a numbered circle + motion line per epoch, via a
per-epoch `WCS.world_to_pixel()` transform (no pixel-level alignment needed); or a **stamp strip**
(everything else) — one small crop per epoch centred on that epoch's own detected position, each
circled and labelled with timestamp/magnitude, for a classic before/after "blink" comparison.
`update_charts_for_sources()` handles every source_id for one frame in a single call — one
`POST /sources/tracks/batch` to fetch all their tracks, then uploads each rendered chart
individually via `POST /sources/{id}/chart`. Gated by
`CHART_ENABLED`; best-effort — a missing local archive file, API error, or rendering failure for
one source_id is logged and only downgrades that source_id's own result, never affecting any
other source_id in the same call or frame processing overall.

### `api_client/client.py`
All HTTP communication with the remote API. Uses `httpx` with async support and `tenacity` for automatic retry on transient failures (up to 3 attempts total — 2 retries — with exponential backoff). Sends `X-API-Key` and `Content-Type: application/json` on every request. Besides the single-position `get_sources_near`/`get_frames_covering`, it also exposes batched `get_sources_near_batch`/`get_frames_covering_batch`, which is what `anomaly_detector.py` actually calls, plus `get_source_tracks_batch` for `modules/finder_chart.py` (the single-item `get_source_track` is still implemented/exported, just no longer called by that module). Chart uploads use the per-source `upload_source_chart` endpoint — one request per chart. Also provides `get_settings()` for fetching remote configuration from the API at startup (see [Remote Configuration](#remote-configuration-via-api) below).

---

## Deployment with Docker

The recommended way to run the pipeline in production. Docker handles all Python dependencies automatically — no manual `pip install` needed on the server.

### 1. Install Docker

**Ubuntu / Debian:**
```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker
```

**Verify:**
```bash
docker --version
docker compose version
```

Minimum required versions: Docker 20.10+, Docker Compose 2.0+.

---

### 2. Prepare directories on the host

The pipeline expects four directories on the host machine. The `docker-compose.yml` committed
to the repo defaults to macOS-friendly paths under your home directory (`~/observatory-data/...`),
so it works out of the box for local development without `sudo`:

```bash
mkdir -p ~/observatory-data/fits/incoming
mkdir -p ~/observatory-data/fits/archive
mkdir -p ~/observatory-data/fits/rejected
mkdir -p ~/observatory-data/astap/catalogs
```

For a **production Linux observatory server**, the convention used throughout the rest of this
document is `/data/fits/...` / `/data/astap/...` — create those instead and update the left
side of each volume mount in `docker-compose.yml` accordingly:

```bash
sudo mkdir -p /data/fits/incoming
sudo mkdir -p /data/fits/archive
sudo mkdir -p /data/fits/rejected
sudo mkdir -p /data/astap/catalogs

sudo chown -R $USER:$USER /data/fits /data/astap
```

> Either way, these paths are just defaults. You can change them to any location on your
> server — just update the left side of each volume mount in `docker-compose.yml` and the
> corresponding environment variables in `.env`.

---

### 3. Download ASTAP star catalogs

The `astap` plate solver (included in the Docker image) requires a local star catalog. **Catalogs are NOT bundled in the image** — they must be downloaded once and stored in `/data/astap/catalogs` (or whichever host directory you mount to `/astap/catalogs` inside the container).

ASTAP catalogs are available at SourceForge: **https://sourceforge.net/projects/astap-program/files/star_databases/**

**Recommended catalog: D50** (~3.3 GB, 50 million stars, works for most telescope apertures)

```bash
# Download and extract directly into the catalog directory
cd /data/astap/catalogs

# D50 catalog (recommended — works for most setups)
wget -O d50.zip "https://sourceforge.net/projects/astap-program/files/star_databases/d50_star_database.zip/download"
unzip d50.zip
rm d50.zip
```

**Alternative catalogs:**

| Catalog | Size | Stars | Use case |
|---------|------|-------|----------|
| **D80** | ~8 GB | 80 million | Large aperture telescopes, faint stars |
| **D50** | ~3.3 GB | 50 million | Recommended for most setups |
| **D20** | ~1.3 GB | 20 million | Smaller telescopes, faster solving |
| **D05** | ~300 MB | 5 million | Wide field, very fast solving |
| **W08** | ~100 MB | Wide field | Ultra-wide field lenses |

```bash
# Example: Download D20 (smaller, faster)
wget -O d20.zip "https://sourceforge.net/projects/astap-program/files/star_databases/d20_star_database.zip/download"
unzip d20.zip
rm d20.zip
```

After extraction the directory should contain files with extensions `.1476`, `.290`, etc. The pipeline passes `/astap/catalogs` to `astap` via the `ASTAP_CATALOGS` environment variable.

> The catalog download is a one-time operation. The same catalog directory is reused across container rebuilds and restarts because it is mounted from the host.

---

### 4. Configure environment variables

Copy the template and fill in your values:

```bash
cd /path/to/observatory-pipeline
cp .env.example .env
nano .env
```

`.env` contents:

```env
# ── Required ──────────────────────────────────────────────────────────────────
API_BASE_URL=https://your-cloud-host.com/api/v1
API_KEY=your-secret-api-key-here

# ── FITS directory paths (inside the container) ───────────────────────────────
# These match the right-hand side of the volume mounts in docker-compose.yml.
# Change only if you customise the container paths.
FITS_INCOMING=/fits/incoming
FITS_ARCHIVE=/fits/archive
FITS_REJECTED=/fits/rejected

# ── ASTAP plate solver ────────────────────────────────────────────────────────
ASTAP_BINARY=/usr/local/bin/astap
ASTAP_CATALOGS=/astap/catalogs
ASTAP_FOV_HINT=0              # FOV hint in degrees (0 = auto-detect from FITS)

# ── Observatory site (used for JPL Horizons topocentric ephemerides) ──────────
SITE_LAT=55.7558
SITE_LON=37.6173
SITE_ELEV=150

# ── Quality control thresholds (tune for your telescope and local seeing) ─────
QC_FWHM_MAX_ARCSEC=8.0
QC_ELONGATION_MAX=2.0
QC_SNR_MIN=5.0
QC_STARS_MIN=10
QC_STARS_MIN_NARROWBAND=5     # softer floor for Hα/[OIII]/[SII]/[NII] frames — see below
QC_SKY_BACKGROUND_MAX=20000.0

# ── Narrowband filters (modules/normalizer.py, modules/qc.py, modules/photometry.py) ──
NARROWBAND_FILTERS=Ha,OIII,SII,NII

# ── Streak masking (astrometry + qc + subtraction modules) ────────────────────
STREAK_DETECT_SIGMA=3.0
STREAK_ELONGATION_MIN=5.0
STREAK_MIN_LENGTH_ARCSEC=30.0
STREAK_MASK_DILATE_ARCSEC=3.0

# ── Saturation detection (astrometry + subtraction modules) ───────────────────
SATURATION_ADU=60000
SATURATION_MASK_RADIUS_ARCSEC=10.0

# ── Cross-matching ────────────────────────────────────────────────────────────
MATCH_CONE_ARCSEC=5.0
MOVING_CONE_ARCSEC=120.0       # code default; see note below
DELTA_MAG_ALERT=0.5

# ── Image subtraction (modules/subtraction.py) ────────────────────────────────
SUBTRACTION_MIN_FRAMES=3
SUBTRACTION_DETECT_SIGMA=5.0

# ── Finder charts (modules/finder_chart.py) ────────────────────────────────────
CHART_ENABLED=true
CHART_STAMP_SIZE_ARCSEC=60.0
CHART_MAX_EPOCHS=12

# ── Normalization ─────────────────────────────────────────────────────────────
NORMALIZE_ENABLED=true        # Normalize object/filter names and filenames

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_LEVEL=INFO                 # DEBUG, INFO, WARNING, or ERROR
```

> `API_KEY` must never be committed to git. It is listed in `.gitignore`.
>
> `.env.example` is kept in sync with `config.py`'s real defaults: `MOVING_CONE_ARCSEC=120.0`
> (widened because fast movers like Vesta travel ~60"/hr and 30" was too tight to reliably catch
> cross-frame shifts), `SUBTRACTION_MIN_FRAMES=3`, `SUBTRACTION_DETECT_SIGMA=5.0`,
> `CHART_ENABLED=true`, `CHART_STAMP_SIZE_ARCSEC=60.0`, `CHART_MAX_EPOCHS=12`.

#### Configuration Reference

All settings are loaded from environment variables via `config.py`. Here is the complete list:

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| **API** |
| `API_BASE_URL` | — | Yes | Base URL of the observatory-api (e.g., `https://your-cloud-host.com/api/v1`) |
| `API_KEY` | — | Yes | Secret API key for authentication |
| **FITS Directories** |
| `FITS_INCOMING` | `/fits/incoming` | No | Directory to watch for new FITS files |
| `FITS_ARCHIVE` | `/fits/archive` | No | Directory for successfully processed frames |
| `FITS_REJECTED` | `/fits/rejected` | No | Directory for frames that fail QC |
| **ASTAP Plate Solver** |
| `ASTAP_BINARY` | `/usr/local/bin/astap` | No | Path to the astap executable |
| `ASTAP_CATALOGS` | `/astap/catalogs` | No | Path to ASTAP star catalog directory |
| `ASTAP_FOV_HINT` | `0` | No | Field-of-view hint in degrees for faster plate solving. Set to 0 for auto-detection from FITS headers. Providing your telescope's approximate FOV (e.g., `1.5`) significantly speeds up plate solving. |
| **Quality Control** |
| `QC_FWHM_MAX_ARCSEC` | `8.0` | No | Maximum acceptable median FWHM in arcseconds. Frames exceeding this are rejected as `BLUR`. |
| `QC_ELONGATION_MAX` | `2.0` | No | Maximum acceptable PSF elongation ratio (major/minor axis). Values >2.0 indicate star trailing due to tracking issues. |
| `QC_SNR_MIN` | `5.0` | No | Minimum acceptable median SNR of detected sources. |
| `QC_STARS_MIN` | `10` | No | Minimum number of detected stars. Frames with fewer stars are rejected as `LOW_STARS`. |
| `QC_STARS_MIN_NARROWBAND` | `5` | No | Same as `QC_STARS_MIN`, but used instead of it when the frame's own filter is narrowband (see `NARROWBAND_FILTERS` below) — a narrowband frame of the same field genuinely detects far fewer stars than a broadband one. |
| `QC_SKY_BACKGROUND_MAX` | `20000.0` | No | Maximum acceptable median sky background in ADU. Frames exceeding this are rejected as `HIGH_BACKGROUND` (twilight, moonlight, cloud, stray light). Tune to your own site's typical dark-sky background. |
| **Narrowband Filters** |
| `NARROWBAND_FILTERS` | `Ha,OIII,SII,NII` | No | Comma-separated, normalized filter short codes too narrow to carry a representative star sample or a trustworthy Gaia DR3 zero-point. Frames shot in one of these use `QC_STARS_MIN_NARROWBAND` above, and `modules/photometry.py` never attempts magnitude calibration on them (see CLAUDE.md's "Filters — real astronomy context"). |
| **Star Detection Filtering** |
| `SEP_DETECT_THRESH` | `10.0` | No | Detection threshold in sigma above background for SEP source extraction. Higher = fewer, more reliable detections. |
| `SEP_MIN_AREA` | `15` | No | Minimum connected pixels for a valid source detection. Filters out hot pixels and noise. |
| `STAR_FWHM_MIN_ARCSEC` | `2.5` | No | Minimum FWHM in arcseconds. Sources below this are likely hot pixels or cosmic rays. |
| `STAR_FWHM_MAX_ARCSEC` | `8.0` | No | Maximum FWHM in arcseconds. Sources above this are extended objects (nebulae, galaxies) or badly defocused. |
| `STAR_ELONGATION_MAX` | `1.5` | No | Maximum elongation for valid star detections. Filters out trails and extended objects. |
| `STAR_SNR_MIN` | `50.0` | No | Minimum SNR (peak/rms) for valid star detections. Higher = fewer but more reliable. |
| **Streak Masking** |
| `STREAK_DETECT_SIGMA` | `3.0` | No | Coarse, non-deblended pre-pass's own detection threshold (sigma above background) — finds satellite/aircraft trails and diffraction-spike arms before the real point-source/diff-image extraction runs, so they can't fragment into false stars (or, on the subtraction diff image, into many separate `SPACE_DEBRIS`-eligible candidates for one trail). Kept lower than `SEP_DETECT_THRESH`/`SUBTRACTION_DETECT_SIGMA` — a faint trail's brightness can otherwise dip below a higher threshold often enough to still fragment. |
| `STREAK_ELONGATION_MIN` | `5.0` | No | A coarse candidate is masked as a streak only when its elongation is at or above this AND its bounding-box diagonal is at or above `STREAK_MIN_LENGTH_ARCSEC` — an ordinary star, even a deblended close pair, never reaches both. |
| `STREAK_MIN_LENGTH_ARCSEC` | `30.0` | No | See `STREAK_ELONGATION_MIN` above. |
| `STREAK_MASK_DILATE_ARCSEC` | `3.0` | No | Small dilation applied to the coarse streak mask before use, so the segmentation boundary doesn't leave a thin rim of trail pixels detectable as their own tiny fragment. |
| **Saturation Detection** |
| `SATURATION_ADU` | `60000` | No | Sensor pixel value (ADU) at/above which a pixel is considered saturated. `modules/astrometry.py` flags any source whose peak reaches this as `saturated`; `modules/subtraction.py` masks its vicinity out of difference-image detection. Tune per camera's full-well/bit depth. |
| `SATURATION_MASK_RADIUS_ARCSEC` | `10.0` | No | Radius in arcseconds around a saturated pixel that `modules/subtraction.py` excludes from diff-image source detection, to suppress astroalign residual artifacts near bright/saturated stars. |
| **Cross-Matching** |
| `MATCH_CONE_ARCSEC` | `5.0` | No | Cone search radius in arcseconds for point-source catalog matching (Simbad, Gaia, 2MASS, Pan-STARRS). |
| `MOVING_CONE_ARCSEC` | `120.0` | No | Wider cone radius in arcseconds for moving-object (MPC) detection. Widened from an earlier default of `30.0` because fast movers like Vesta travel ~60"/hr. `.env.example` is up to date with this value — see the note above. |
| `DELTA_MAG_ALERT` | `0.5` | No | Magnitude delta threshold that triggers a variability alert. |
| **Edge-of-Frame Geometry** |
| `EDGE_MARGIN_FRAC` | `0.1` | No | Fraction of NAXIS1/NAXIS2 treated as "near the edge" — coma and other off-axis aberrations stretch a star's PSF near the edges/corners of a wide-field frame, inflating its measured elongation. `modules/astrometry.py`/`modules/subtraction.py` flag such sources `near_edge`; tune to your own optics. |
| `SPACE_DEBRIS_ELONGATION_MIN` | `3.0` | No | Elongation threshold for the "single-exposure trail" `SPACE_DEBRIS` shortcut in `modules/anomaly_detector.py`, for a source not flagged `near_edge`. |
| `SPACE_DEBRIS_EDGE_ELONGATION_MIN` | `6.0` | No | Same shortcut's threshold for a source flagged `near_edge` — higher, since coma alone can already push an ordinary star's elongation past `SPACE_DEBRIS_ELONGATION_MIN` near the edge of a wide-field frame. |
| **Image Subtraction** |
| `SUBTRACTION_MIN_FRAMES` | `3` | No | Minimum number of archived reference frames of the same object required before `modules/subtraction.py` will attempt image subtraction. |
| `SUBTRACTION_DETECT_SIGMA` | `5.0` | No | Detection threshold on the difference image, in multiples of background RMS. |
| **Finder Charts** |
| `CHART_ENABLED` | `true` | No | Enable/disable per-source finder-chart generation (`modules/finder_chart.py`). |
| `CHART_STAMP_SIZE_ARCSEC` | `60.0` | No | Half-width of the per-epoch crop for the `stamp_strip` style (stationary anomalies), in arcseconds. |
| `CHART_MAX_EPOCHS` | `12` | No | Cap on the number of epochs drawn on one chart (oldest dropped first). |
| **Watcher Batching** |
| `WATCHER_DEBOUNCE_SEC` | `5.0` | No | Quiet period after the most recently arrived FITS file before `watcher.py` submits everything buffered so far as one `ANALYZE` task. |
| `WATCHER_MAX_BATCH_SIZE` | `200` | No | Flush the pending batch immediately once it reaches this many files, instead of waiting out the full debounce window. |
| **Catalog Query Cache** |
| `CATALOG_CACHE_DIR` | `/cache/catalog` | No | On-disk directory for cached Gaia/Simbad/2MASS/Pan-STARRS/MPC query results. Mount from a host path OUTSIDE the container (see `docker-compose.yml`'s `worker` service) so it survives a container rebuild — important since this gets restarted often during testing. |
| `CACHE_TTL_HOURS` | `1.0` | No | How long a cached catalog query result stays valid, in hours. |
| **Job Queue Worker** |
| `TASK_POLL_INTERVAL_SEC` | `10.0` | No | How often `worker.py` polls `GET /tasks?status=PENDING` when idle. |
| `TASK_POLL_BACKOFF_MAX_SEC` | `60.0` | No | Idle polling backs off exponentially up to this ceiling, resetting the moment a task is found. |
| **Observatory Site** |
| `SITE_LAT` | `0.0` | No | Observatory latitude in decimal degrees (positive = North). Used for topocentric ephemeris queries to JPL Horizons. |
| `SITE_LON` | `0.0` | No | Observatory longitude in decimal degrees (positive = East). |
| `SITE_ELEV` | `0` | No | Observatory elevation in metres above sea level. |
| **Normalization** |
| `NORMALIZE_ENABLED` | `true` | No | Enable automatic normalization of FITS header values and filenames. Normalizes object names (`M 51` → `M51`), filter names (`Blue` → `B`, `Luminance` → `L`, `H-Alpha` → `Ha`), frame types (`Light Frame` → `Light`), and renames files to standard format `{Object}_{Type}_{Filter}_{Exp}_{DateTime}.fits`. Ensures consistency across different capture software. |
| **Logging** |
| `LOG_LEVEL` | `INFO` | No | Logging verbosity: `DEBUG`, `INFO`, `WARNING`, or `ERROR`. Set via `watcher.py`'s `logging.basicConfig()`. |

#### Remote Configuration via API

In addition to `.env` / environment variables, most of the settings above can be overridden
centrally via the observatory-api's `settings` table — without redeploying `.env` files or
rebuilding Docker images on the observatory server.

**How it works:**

1. Both `watcher.py` and `worker.py` call `GET /settings` **once at startup** (before any
   processing begins).
2. The API returns a flat `{param_name: string_value}` map from its `settings` database table.
3. `config.apply_remote_settings()` casts each value to its correct Python type and overrides
   the corresponding module-level global in `config.py`.
4. If a parameter is **not present** in the API response, its local default (from `.env` or the
   hardcoded fallback in `config.py`) stays in effect.
5. If the API is **unreachable or returns an error**, all local defaults are used — the pipeline
   still starts normally.

**Security:** credentials (`API_KEY`), filesystem paths (`FITS_INCOMING`, `FITS_ARCHIVE`,
`FITS_REJECTED`, `ASTAP_BINARY`, `ASTAP_CATALOGS`, `CATALOG_CACHE_DIR`), and `API_BASE_URL`
cannot be overridden remotely — only the tunable thresholds, toggles, and site parameters
listed in [docs/API.md section 16](docs/API.md#16-pipeline-configuration-remote-settings) are
accepted.

**To change a setting remotely:** insert or update a row in the `settings` table on the API side
(e.g. `param = "QC_FWHM_MAX_ARCSEC"`, `value = "6.0"`), then restart the relevant container
(`docker compose restart pipeline worker`). The new value will be fetched on the next startup.

---

### 5. Build and start the container

```bash
# Build image and start in the background
docker compose up --build -d

# Follow live logs
docker compose logs -f

# Stop the pipeline
docker compose down
```

On the **first build**, Docker will:
1. Pull the `python:3.11-slim` base image
2. Install system packages (`libcfitsio-dev`, `file`, `libgtk-3-0`, `libharfbuzz-gobject0`, `libpango-1.0-0`, `libpangocairo-1.0-0`, `xvfb`)
3. Install the `astap` binary from the local archive under `install/` (it is **not** downloaded at build time — see [Development on Apple Silicon](#development-on-apple-silicon-m1m2m3-macs) below for how that archive gets there)
4. Install all Python dependencies from `requirements.txt`

Subsequent builds reuse cached layers and are much faster unless `requirements.txt` changes.

---

### Development on Apple Silicon (M1/M2/M3 Macs)

The repository includes the `astap` binary for **amd64** architecture in `install/astap_amd64.tar.gz`. On Apple Silicon Macs, you have two options:

**Option 1: Use amd64 emulation (simpler, recommended for development)**

Build and run with amd64 emulation using `DOCKER_DEFAULT_PLATFORM`:

```bash
# Build and run with amd64 emulation


# Or set the variable for your shell session
export DOCKER_DEFAULT_PLATFORM=linux/amd64
docker compose up --build -d

# Follow logs
docker compose logs -f
```

Or add the platform specification permanently to `docker-compose.yml`:

```yaml
services:
  pipeline:
    platform: linux/amd64
    build: .
    # ... rest of config
```

> **Note:** Running under emulation is slower than native ARM execution, but it works without additional setup.

**Option 2: Use native ARM64 binary (faster)**

Download the ARM64 version of astap and place it in the `install/` directory:

```bash
cd install/
rm astap_amd64.tar.gz
wget -O astap_aarch64.tar.gz "https://sourceforge.net/projects/astap-program/files/linux_installer/astap_aarch64.tar.gz/download"
```

Then build normally without platform override:

```bash
docker compose up --build -d
```

---

### External volumes reference

All persistent data lives **outside** the container on the host filesystem. The mapping is defined in `docker-compose.yml`. The version committed to the repo defaults to macOS-friendly home-directory paths:

```yaml
services:
  pipeline:
    build: .
    extra_hosts:
      - "host.docker.internal:host-gateway"
    volumes:
      # macOS: use paths under home directory (Docker has access by default)
      # Linux production: change to /data/fits/... paths
      - ~/observatory-data/fits/incoming:/fits/incoming     # watch this for new frames
      - ~/observatory-data/fits/archive:/fits/archive       # processed frames stored here
      - ~/observatory-data/fits/rejected:/fits/rejected     # bad frames moved here
      - ~/observatory-data/astap/catalogs:/astap/catalogs   # star catalogs for plate solving
    env_file:
      - .env
    restart: unless-stopped
```

| Host path (macOS default) | Host path (Linux production) | Container path | Purpose |
|-----------|-----------|----------------|---------|
| `~/observatory-data/fits/incoming` | `/data/fits/incoming` | `/fits/incoming` | Drop new `.fits` / `.fit` files here. The watcher detects them automatically. |
| `~/observatory-data/fits/archive` | `/data/fits/archive` | `/fits/archive` | Successfully processed frames are moved here, organized by object name. |
| `~/observatory-data/fits/rejected` | `/data/fits/rejected` | `/fits/rejected` | Frames that fail QC are moved here with a prefix indicating the reason (`BLUR_`, `TRAIL_`, `LOW_STARS_`, `BAD_`). |
| `~/observatory-data/astap/catalogs` | `/data/astap/catalogs` | `/astap/catalogs` | ASTAP star catalog files. Download once; survives container rebuilds. |

To use different host paths, edit the **left side** of each volume entry in `docker-compose.yml` and update the corresponding variables in `.env`. `extra_hosts: host.docker.internal:host-gateway` lets the container reach services running on the host machine.

---

### What installs automatically

When the Docker image is built, the following are installed **without any manual steps**:

| Component | How | Notes |
|-----------|-----|-------|
| Python 3.11 | Base image `python:3.11-slim` | |
| All Python packages | `pip install -r requirements.txt` | astropy, astroquery, photutils, sep, astroscrappy, astroalign, httpx, tenacity, watchdog, numpy, python-dotenv (plus `pytest`/`pytest-asyncio` for tests) |
| `libcfitsio` | `apt-get install libcfitsio-dev` | C library required by `astropy.io.fits` and `sep` |
| `xvfb` + GTK/Pango libs | `apt-get install xvfb libgtk-3-0 ...` | Virtual display required to run `astap` headless |
| `astap` binary | Extracted from `install/astap_*.tar.gz` | The plate solver executable, bundled in the repository as a pre-downloaded archive |

**What is NOT automatic and requires manual action:**
- ASTAP star catalog files — must be downloaded once as described in [step 3](#3-download-astap-star-catalogs)
- The `.env` file — must be created from `.env.example`
- For ARM64 (Raspberry Pi, Apple Silicon): replace `install/astap_amd64.tar.gz` with `astap_aarch64.tar.gz`

---

### Updating the pipeline

```bash
git pull
docker compose up --build -d
```

Python packages are reinstalled only when `requirements.txt` changes. Catalog files are unaffected.

---

## Running Locally (without Docker)

For development and testing only.

```bash
git clone https://github.com/miksrv/observatory-pipeline.git
cd observatory-pipeline

python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate

pip install -r requirements.txt

cp .env.example .env
# edit .env with your values

python watcher.py
```

You will also need `astap` installed locally and a catalog downloaded to the path specified in `ASTAP_CATALOGS`.

---

## File Organization

Frames are organized into subdirectories by the `OBJECT` FITS header keyword:

```
/fits/archive/
├── M51/
│   ├── frame_20240315_220134.fits
│   └── frame_20240315_220434.fits
├── NGC_1234/
└── _UNKNOWN/           ← frames without OBJECT header

/fits/rejected/
├── M51/
│   ├── BLUR_frame_20240315_221034.fits
│   └── TRAIL_frame_20240315_221534.fits
└── _UNKNOWN/
```

Object name is sanitized: spaces become underscores, special characters are removed. If the `OBJECT` header is missing, frames go to `_UNKNOWN`.

---

## Tests

All external calls (API, astronomical catalogs, astap subprocess) are mocked. No network access or real FITS files are required.

```bash
# Run the full suite
pytest

# With verbose output
pytest -v

# With coverage report
pytest --cov=. --cov-report=term-missing

# Run a specific module
pytest tests/test_anomaly_detector.py
```

As of the last manual check, coverage was around **95%** overall, with all critical modules
above 80% — every module under `modules/`, including `subtraction.py` and `finder_chart.py`,
has its own dedicated test file. Coverage is not tracked in CI: the GitHub Actions workflow runs
plain `pytest tests/ -v --tb=short`, not `pytest --cov`. Run the `--cov` command above locally
if you need an up-to-date number.

---

## API Endpoints Used

Full request/response documentation for every endpoint is in **[docs/API.md](docs/API.md)**.

| Method | Endpoint | Purpose | Docs |
|--------|----------|---------|------|
| `POST` | `/frames` | Register a new processed frame | [→ docs/API.md](docs/API.md#1-register-a-frame) |
| `POST` | `/frames/{id}/sources` | Save detected sources for a frame | [→ docs/API.md](docs/API.md#2-save-sources-for-a-frame) |
| `POST` | `/frames/{id}/anomalies` | Save detected anomalies | [→ docs/API.md](docs/API.md#3-save-anomalies-for-a-frame) |
| `GET` | `/sources/near` | Get historical sources near a single (RA, Dec) — implemented, but no longer called by `anomaly_detector.py` | [→ docs/API.md](docs/API.md#4-get-historical-sources-near-a-sky-position) |
| `GET` | `/frames/covering` | Get frames that covered a single sky point — same caveat as above | [→ docs/API.md](docs/API.md#5-get-frames-covering-a-sky-position) |
| `POST` | `/sources/near/batch` | Get historical sources near **multiple** positions in one call — what `anomaly_detector.py` actually uses | [→ docs/API.md](docs/API.md#6-get-historical-sources-near-multiple-positions-batch) |
| `POST` | `/frames/covering/batch` | Coverage check for **multiple** sky positions in one call — what `anomaly_detector.py` actually uses | [→ docs/API.md](docs/API.md#7-get-frames-covering-multiple-positions-batch) |
| `GET` | `/sources/{id}/track` | Per-epoch position track for a source — implemented, but no longer called by `modules/finder_chart.py` | [→ docs/API.md](docs/API.md#8-get-a-sources-position-track) |
| `POST` | `/sources/{id}/chart` | Upload/replace a source's finder-chart PNG — same caveat as above | [→ docs/API.md](docs/API.md#9-upload-a-sources-finder-chart) |
| `GET` | `/sources/{id}/chart.png` | Fetch a source's stored finder-chart PNG — not called by the pipeline itself | [→ docs/API.md](docs/API.md#10-get-a-sources-finder-chart) |
| `POST` | `/sources/tracks/batch` | Get position tracks for **multiple** sources in one call — what `modules/finder_chart.py` actually uses | [→ docs/API.md](docs/API.md#11-get-position-tracks-for-multiple-sources-batch) |
| `GET` | `/settings` | Fetch remote pipeline configuration from the API's `settings` table — called once at startup by both `watcher.py` and `worker.py` | [→ docs/API.md](docs/API.md#16-pipeline-configuration-remote-settings) |

**Authentication:** every request sends `X-API-Key: <value>` and `Content-Type: application/json`. The key is read from `API_KEY` in `.env`.

**Retry policy:** HTTP 5xx and transport errors are retried (3 attempts total, ~2s between
each) via `tenacity`; HTTP 4xx errors are logged immediately and not retried. Exact parameters
and the reasoning behind the ~2s spacing are in [docs/API.md](docs/API.md#error-responses).

---

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| `astropy` | >=6.0 | FITS I/O, WCS, coordinate transforms |
| `astroquery` | >=0.4.7 | Gaia, Simbad, MPC, JPL Horizons queries |
| `photutils` | >=1.12 | Aperture photometry |
| `sep` | >=1.4 | Fast source extraction (SourceExtractor wrapper) |
| `astroscrappy` | >=1.1 | Cosmic ray detection and removal |
| `astroalign` | >=2.4 | Frame alignment for `modules/subtraction.py` (image subtraction) |
| `matplotlib` | >=3.8 | PNG rendering for `modules/finder_chart.py` (finder/discovery charts) |
| `numpy` | >=1.26 | Numerical operations |
| `scipy` | >=1.11 | Binary dilation of the saturation mask in `modules/subtraction.py` |
| `httpx` | >=0.27 | Async HTTP client for API calls |
| `tenacity` | >=8.2 | Retry logic with exponential backoff |
| `watchdog` | >=4.0 | Filesystem monitoring |
| `python-dotenv` | >=1.0 | `.env` file loading |
| `pytest` | >=8.0 | Test runner (dev/test dependency, in the main `requirements.txt`) |
| `pytest-asyncio` | >=0.23 | Async test support (dev/test dependency) |

---

## Security

- The API key is loaded from `.env` and sent as an `X-API-Key` header on every request
- The observatory server's outbound IP should be whitelisted on the cloud firewall
- Never commit `.env` to git — it is listed in `.gitignore`
- The pipeline has no inbound network exposure — it only makes outbound HTTPS calls

---

## Task Tracking

All project tasks are tracked as **GitHub Issues** in this repository.

---

## License

See [LICENSE](LICENSE).
