# Observatory FITS Analysis Pipeline

[![Tests](https://github.com/miksrv/observatory-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/miksrv/observatory-pipeline/actions/workflows/tests.yml)

Automated Python service for processing astronomical FITS frames from an observatory telescope. Runs on a dedicated observatory server, performs quality control, plate solving, source extraction, catalog cross-matching, anomaly detection, and reports everything to a remote REST API.

---

## Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Modules](#modules)
- [Deployment with Docker](#deployment-with-docker)
  - [1. Install Docker](#1-install-docker)
  - [2. Prepare directories on the host](#2-prepare-directories-on-the-host)
  - [3. Download ASTAP star catalogs](#3-download-astap-star-catalogs)
  - [4. Configure environment variables](#4-configure-environment-variables)
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

The pipeline monitors an incoming directory for new FITS files. Each file is processed through a sequence of steps: quality check → plate solving → photometry → catalog matching → anomaly detection → archiving. Bad frames are rejected locally without involving the API, keeping the remote database clean.

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
                              photometry.measure()  ← calibrated magnitudes
                                         │
                                         ▼
                         api_client.post_frame()    ← register frame, get frame_id
                                         │
                                         ▼
                         api_client.post_sources()  ← save all sources
                                         │
                                         ▼
                         catalog_matcher.match()    ← Gaia DR3 + Simbad + MPC
                                         │
                                         ▼
                         anomaly_detector.detect()  ← classify anomalies
                                         │
                                         ▼
                         api_client.post_anomalies()
                                         │
                                         ▼
                         /fits/archive/{object}/    ← archive frame
```

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
├── API.md                     ← REST API endpoint reference
├── Dockerfile
├── docker-compose.yml
├── .env.example               ← environment variable template
├── requirements.txt
├── config.py                  ← loads all settings from .env
├── watcher.py                 ← entry point, monitors incoming folder
├── pipeline.py                ← orchestrator for a single FITS file
│
├── modules/
│   ├── fits_header.py         ← extract FITS headers into structured dict
│   ├── qc.py                  ← quality control: bad frame detection & rejection
│   ├── astrometry.py          ← plate solving (astap) + source extraction (sep)
│   ├── photometry.py          ← aperture photometry (photutils)
│   ├── catalog_matcher.py     ← cross-match: Gaia DR3, Simbad/Vizier, MPC
│   ├── anomaly_detector.py    ← comparison with history + anomaly classification
│   └── ephemeris.py           ← JPL Horizons queries for solar system objects
│
├── api_client/
│   └── client.py              ← all HTTP calls to observatory-api
│
└── tests/
    ├── test_fits_header.py
    ├── test_qc.py
    ├── test_astrometry.py
    ├── test_photometry.py
    ├── test_api_client.py
    ├── test_catalog_matcher.py
    ├── test_ephemeris.py
    ├── test_anomaly_detector.py
    └── test_pipeline.py
```

---

## Modules

### `watcher.py`
Entry point. Uses `watchdog` to monitor `FITS_INCOMING` for new `.fits` / `.fit` files. On detection, waits briefly for the write to complete, then calls `pipeline.run(filepath)`.

### `pipeline.py`
Orchestrates processing of a single FITS file. Calls each module in order and handles failures gracefully — a crash in catalog matching does not abort the frame.

### `modules/fits_header.py`
Reads the FITS primary header using `astropy.io.fits`. Normalizes keyword aliases (e.g., `CCD-TEMP` vs `CCDTEMP`, `EXPTIME` vs `EXPOSURE`). Returns a structured dict ready for the API payload.

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
Calls the `astap` binary as a subprocess for plate solving. Parses the resulting WCS header written back into the FITS file. Runs `sep` (SourceExtractor Python wrapper) for source detection. Converts pixel coordinates to (RA, Dec) using `astropy.wcs.WCS`.

Returns: frame center (RA, Dec), field of view in degrees, and a list of sources `[(ra, dec, flux, fwhm, elongation), ...]`.

### `modules/photometry.py`
Aperture photometry via `photutils`. Performs differential photometry against Gaia DR3 reference stars in the field — this makes brightness measurements immune to atmospheric transparency variations.

### `modules/catalog_matcher.py`
Cross-matches the source list against external catalogs using `astropy.coordinates.SkyCoord.match_to_catalog_sky()` with cone radius `MATCH_CONE_ARCSEC`.

Catalogs:
- **Gaia DR3** — ~1.8 billion stars, precise positions and magnitudes
- **Simbad / Vizier** — named objects: variable stars, double stars, galaxies, nebulae
- **MPC** — all known asteroids and comets with orbital elements

### `modules/anomaly_detector.py`
Core science logic. For each detected source, queries the API for historical observations, then classifies:

| Classification | Condition | Alert? |
|---|---|---|
| `FIRST_OBSERVATION` | Sky area never observed before | No |
| `KNOWN_CATALOG_NEW` | Not in history, found in catalog | No |
| `VARIABLE_STAR` | Known variable, brightness changed | No |
| `BINARY_STAR` | Known binary, brightness changed | No |
| `ASTEROID` | Moving, matched in MPC | No (+ ephemeris) |
| `COMET` | Moving, matched in MPC as comet | No (+ ephemeris) |
| `SUPERNOVA_CANDIDATE` | Brightening near galaxy, not in catalogs | **YES** |
| `MOVING_UNKNOWN` | Moving, not in MPC | **YES** |
| `SPACE_DEBRIS` | Fast trail, not in MPC | **YES** |
| `UNKNOWN` | New point source, area covered, not in any catalog | **YES** |

### `modules/ephemeris.py`
Queries JPL Horizons via `astroquery.jplhorizons`. Given an MPC designation and observation time, returns predicted (RA, Dec, magnitude, distance in AU, angular velocity in arcsec/hour).

### `api_client/client.py`
All HTTP communication with the remote API. Uses `httpx` with async support and `tenacity` for automatic retry on transient failures (3 retries, exponential backoff). Sends `X-API-Key` and `Content-Type: application/json` on every request.

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

The pipeline expects four directories on the host machine. Create them before starting the container:

```bash
sudo mkdir -p /data/fits/incoming
sudo mkdir -p /data/fits/archive
sudo mkdir -p /data/fits/rejected
sudo mkdir -p /data/astap/catalogs
```

Set ownership so the container process can read and write:

```bash
sudo chown -R $USER:$USER /data/fits /data/astap
```

> These paths are defaults. You can change them to any location on your server — just update the left side of each volume mount in `docker-compose.yml` and the corresponding environment variables in `.env`.

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

# ── Observatory site (used for JPL Horizons topocentric ephemerides) ──────────
SITE_LAT=55.7558
SITE_LON=37.6173
SITE_ELEV=150

# ── Quality control thresholds (tune for your telescope and local seeing) ─────
QC_FWHM_MAX_ARCSEC=8.0
QC_ELONGATION_MAX=2.0
QC_SNR_MIN=5.0
QC_STARS_MIN=10

# ── Cross-matching ────────────────────────────────────────────────────────────
MATCH_CONE_ARCSEC=5.0
MOVING_CONE_ARCSEC=30.0
DELTA_MAG_ALERT=0.5
```

> `API_KEY` must never be committed to git. It is listed in `.gitignore`.

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
2. Install system packages (`libcfitsio-dev`, `wget`)
3. Download the `astap` binary from hnsky.org
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

All persistent data lives **outside** the container on the host filesystem. The mapping is defined in `docker-compose.yml`:

```yaml
services:
  pipeline:
    build: .
    volumes:
      - /data/fits/incoming:/fits/incoming     # watch this for new frames
      - /data/fits/archive:/fits/archive       # processed frames stored here
      - /data/fits/rejected:/fits/rejected     # bad frames moved here
      - /data/astap/catalogs:/astap/catalogs   # star catalogs for plate solving
    env_file:
      - .env
    restart: unless-stopped
```

| Host path | Container path | Purpose |
|-----------|----------------|---------|
| `/data/fits/incoming` | `/fits/incoming` | Drop new `.fits` / `.fit` files here. The watcher detects them automatically. |
| `/data/fits/archive` | `/fits/archive` | Successfully processed frames are moved here, organized by object name. |
| `/data/fits/rejected` | `/fits/rejected` | Frames that fail QC are moved here with a prefix indicating the reason (`BLUR_`, `TRAIL_`, `LOW_STARS_`, `BAD_`). |
| `/data/astap/catalogs` | `/astap/catalogs` | ASTAP star catalog files. Download once; survives container rebuilds. |

To use different host paths, edit the **left side** of each volume entry in `docker-compose.yml` and update the corresponding variables in `.env`.

---

### What installs automatically

When the Docker image is built, the following are installed **without any manual steps**:

| Component | How | Notes |
|-----------|-----|-------|
| Python 3.11 | Base image `python:3.11-slim` | |
| All Python packages | `pip install -r requirements.txt` | astropy, astroquery, photutils, sep, astroscrappy, httpx, tenacity, watchdog, numpy, python-dotenv |
| `libcfitsio` | `apt-get install libcfitsio-dev` | C library required by `astropy.io.fits` and `sep` |
| `astap` binary | Copied from `install/` directory | The plate solver executable, bundled in the repository |

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

Current coverage: **95%** overall. All critical modules exceed 80%.

---

## API Endpoints Used

Full request/response documentation for every endpoint is in **[API.md](API.md)**.

| Method | Endpoint | Purpose | Docs |
|--------|----------|---------|------|
| `POST` | `/frames` | Register a new processed frame | [→ API.md](API.md#1-register-a-frame) |
| `POST` | `/frames/{id}/sources` | Save detected sources for a frame | [→ API.md](API.md#2-save-sources-for-a-frame) |
| `POST` | `/frames/{id}/anomalies` | Save detected anomalies | [→ API.md](API.md#3-save-anomalies-for-a-frame) |
| `GET` | `/sources/near` | Get historical sources near (RA, Dec) | [→ API.md](API.md#4-get-historical-sources-near-a-sky-position) |
| `GET` | `/frames/covering` | Get frames that covered a sky point | [→ API.md](API.md#5-get-frames-covering-a-sky-position) |

**Authentication:** every request sends `X-API-Key: <value>` and `Content-Type: application/json`. The key is read from `API_KEY` in `.env`.

**Retry policy:** HTTP 5xx and transport errors are retried up to 3 times with exponential backoff (2 s → 4 s → 8 s). HTTP 4xx errors are logged immediately and not retried.

---

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| `astropy` | >=6.0 | FITS I/O, WCS, coordinate transforms |
| `astroquery` | >=0.4.7 | Gaia, Simbad, MPC, JPL Horizons queries |
| `photutils` | >=1.12 | Aperture photometry |
| `sep` | >=1.4 | Fast source extraction (SourceExtractor wrapper) |
| `astroscrappy` | >=1.1 | Cosmic ray detection and removal |
| `numpy` | >=1.26 | Numerical operations |
| `httpx` | >=0.27 | Async HTTP client for API calls |
| `tenacity` | >=8.2 | Retry logic with exponential backoff |
| `watchdog` | >=4.0 | Filesystem monitoring |
| `python-dotenv` | >=1.0 | `.env` file loading |

---

## Security

- The API key is loaded from `.env` and sent as an `X-API-Key` header on every request
- The observatory server's outbound IP should be whitelisted on the cloud firewall
- Never commit `.env` to git — it is listed in `.gitignore`
- The pipeline has no inbound network exposure — it only makes outbound HTTPS calls

---

## Task Tracking

All project tasks are tracked on the GitHub Project board:
**https://github.com/users/miksrv/projects/10**

---

## License

See [LICENSE](LICENSE).
