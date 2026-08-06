# CLAUDE.md — Observatory FITS Analysis Pipeline

This file provides full context for AI-assisted development of the `observatory-pipeline` project.
Always read this file at the start of a session before writing any code.

---

## Task Management — GitHub Issues

All tasks for this project are tracked as **GitHub Issues** in this repository.
The old workflow (draft-only cards on the GitHub Project board, no Issues) has been retired —
Issues are now the normal, sole way to track work here.

### Rules for Working with Tasks

1. **Always provide a clear title** — short, descriptive, action-oriented (e.g., "Implement QC module", "Add Gaia DR3 cross-matching")
2. **Always write a description** — explain what needs to be done, acceptance criteria, and any relevant context
3. **Check existing issues before starting work** — search open/closed issues for anything related to your task
4. **Update issue status as work progresses** — labels/comments (and board columns, if the issue is added to a project board) should reflect Todo → In Progress → Done

### Issue Description Template

When creating a new issue, include:
- **What**: Clear description of the task
- **Why**: Reason or motivation for the task
- **Acceptance criteria**: How to verify the task is complete
- **Notes**: Any technical details, links, or dependencies

Example:
```
**What**: Implement ephemeris calculation for asteroids using JPL Horizons API

**Why**: Need to compute predicted positions for detected asteroids to include in anomaly reports

**Acceptance criteria**:
- [ ] Query JPL Horizons with MPC designation and observation time
- [ ] Return predicted RA, Dec, magnitude, distance, angular velocity
- [ ] Handle API errors gracefully with logging
- [ ] Add unit tests with mocked API responses

**Notes**: Use astroquery.jplhorizons module. See CLAUDE.md for expected output format.
```

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

`/data/...` above is the **production** (Linux observatory server) convention. The
`docker-compose.yml` actually committed to the repo currently defaults to macOS-friendly
paths for local development — see "Docker Setup" below.

**Security:** The pipeline server's outbound IP should be whitelisted on the cloud firewall.
The API key must be stored in `.env` and never committed to git.

---

## Docker Setup

### `docker-compose.yml`

```yaml
services:
  pipeline:
    build: .
    extra_hosts:
      - "host.docker.internal:host-gateway"
    volumes:
      # macOS: use paths under home directory (Docker has access by default)
      # Linux production: change to /data/fits/... paths
      - ~/observatory-data/fits/incoming:/fits/incoming
      - ~/observatory-data/fits/archive:/fits/archive
      - ~/observatory-data/fits/rejected:/fits/rejected
      - ~/observatory-data/astap/catalogs:/astap/catalogs
    env_file:
      - .env
    restart: unless-stopped
```

> The file committed to the repo is pre-configured for local **macOS** development (paths
> under `~/observatory-data/`), with `extra_hosts: host.docker.internal:host-gateway` added
> for reaching services on the host. For a production Linux observatory server, change the
> left-hand side of each volume mount to `/data/fits/...` / `/data/astap/...`, matching the
> paths used throughout the rest of this document.

### `Dockerfile`

```dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libcfitsio-dev \
    file \
    libgtk-3-0 \
    libharfbuzz-gobject0 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

# Install astap binary from a local archive (NOT downloaded at build time).
# Download manually from https://sourceforge.net/projects/astap-program/files/linux_installer/
# and place the appropriate tar.gz for your architecture in install/
# (the repo ships install/astap_amd64.tar.gz by default).
COPY install/astap_*.tar.gz /tmp/
RUN tar -xzf /tmp/astap_*.tar.gz -C / && \
    chmod +x /opt/astap/astap && \
    rm -rf /tmp/astap*.tar.gz

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "watcher.py"]
```

`xvfb` and the GTK/Pango libraries are required because `astap` needs a (virtual) display
even when invoked headless — `modules/astrometry.py` runs it via `xvfb-run`. astap is no
longer downloaded from hnsky.org at build time (that approach was replaced): it now ships as
a pre-downloaded archive under `install/` and is installed from there, so builds work offline
and are reproducible. For Apple Silicon / ARM64, swap in an `astap_aarch64.tar.gz` archive
(see README.md).

### `.env.example`

The block below reflects the actual defaults defined in `config.py` (the authoritative source):

```
API_BASE_URL=https://your-cloud-host.com/api/v1
API_KEY=your-secret-api-key-here

FITS_INCOMING=/fits/incoming
FITS_ARCHIVE=/fits/archive
FITS_REJECTED=/fits/rejected
ASTAP_BINARY=/usr/local/bin/astap
ASTAP_CATALOGS=/astap/catalogs
# FOV hint in degrees for faster plate solving (0 = auto-detect from FITS headers)
ASTAP_FOV_HINT=0

# Observatory site (used for topocentric JPL Horizons ephemerides)
SITE_LAT=0.0
SITE_LON=0.0
SITE_ELEV=0

# QC thresholds (adjust for your telescope/seeing)
QC_FWHM_MAX_ARCSEC=8.0
QC_ELONGATION_MAX=2.0
QC_SNR_MIN=5.0
QC_STARS_MIN=10

# Star detection filtering (astrometry module)
# These filter raw SEP detections to keep only point sources (stars)
# and reject extended objects (nebula parts, galaxies) and artifacts.
SEP_DETECT_THRESH=10.0
SEP_MIN_AREA=15
STAR_FWHM_MIN_ARCSEC=2.5
STAR_FWHM_MAX_ARCSEC=8.0
STAR_ELONGATION_MAX=1.5
STAR_SNR_MIN=50.0

# Cross-match cone radius
MATCH_CONE_ARCSEC=5.0
# Cone to search for moving objects (wider). Default widened from 30" to 120":
# fast movers like Vesta travel ~60"/hr, so 30" was too tight to reliably catch
# cross-frame position shifts.
MOVING_CONE_ARCSEC=120.0
# Magnitude delta to trigger variability alert
DELTA_MAG_ALERT=0.5

# Image subtraction (modules/subtraction.py)
# Minimum archived reference frames of the same object required to attempt subtraction.
SUBTRACTION_MIN_FRAMES=3
# Detection threshold on the difference image (multiples of background RMS).
SUBTRACTION_DETECT_SIGMA=5.0

# Normalization (enabled by default)
# When true, normalizes object names (M 51 → M51), filter names (Blue → B),
# frame types (Light Frame → Light), and renames files to standard format.
NORMALIZE_ENABLED=true

# Logging verbosity: DEBUG, INFO, WARNING, ERROR
LOG_LEVEL=INFO
```

> `.env.example` is kept in sync with these `config.py` defaults (`MOVING_CONE_ARCSEC=120.0`,
> `SUBTRACTION_MIN_FRAMES=3`, `SUBTRACTION_DETECT_SIGMA=5.0`). It previously drifted — see
> the resolved Known Issues #5.

---

## Project Structure

```
observatory-pipeline/
├── CLAUDE.md                  ← this file
├── README.md
├── API.md                     ← REST API endpoint reference (full request/response docs)
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── requirements.txt
├── config.py                  ← loads all settings from .env
├── watcher.py                 ← entry point, monitors incoming folder
├── pipeline.py                ← orchestrator for a single FITS file
│
├── modules/
│   ├── __init__.py
│   ├── qc.py                  ← quality control, bad frame detection & moving to rejected
│   ├── fits_header.py         ← extract all relevant FITS headers into structured dict
│   ├── normalizer.py          ← normalize object names, filter names, filenames
│   ├── astrometry.py          ← plate solving (astap) + source extraction (sep)
│   ├── photometry.py          ← aperture photometry (photutils)
│   ├── subtraction.py         ← image subtraction: align + diff archived frames to find transients/movers
│   ├── catalog_matcher.py     ← cross-match: Simbad, Gaia DR3, 2MASS, Pan-STARRS DR1, MPC
│   ├── anomaly_detector.py    ← comparison with history + anomaly classification
│   └── ephemeris.py           ← JPL Horizons queries for solar system objects
│
├── api_client/
│   ├── __init__.py
│   └── client.py              ← all HTTP calls to the observatory-api
│
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_qc.py
    ├── test_fits_header.py
    ├── test_normalizer.py
    ├── test_astrometry.py
    ├── test_photometry.py
    ├── test_catalog_matcher.py
    ├── test_ephemeris.py
    ├── test_anomaly_detector.py
    ├── test_api_client.py
    ├── test_subtraction.py
    └── test_pipeline.py
```

> Every module in `modules/`, including `subtraction.py`, has a corresponding test file.
> `tests/test_subtraction.py` previously did not exist — see the resolved Known Issues #5.

---

## Python Dependencies (`requirements.txt`)

```
astropy>=6.0
astroquery>=0.4.7
photutils>=1.12
sep>=1.4
astroscrappy>=1.1
astroalign>=2.4       # frame alignment for modules/subtraction.py
numpy>=1.26
httpx>=0.27           # async HTTP client for API calls
tenacity>=8.2         # retry logic for API calls
watchdog>=4.0
python-dotenv>=1.0
pytest>=8.0           # dev/test dependency
pytest-asyncio>=0.23  # dev/test dependency
```

`pytest`/`pytest-asyncio` live in the main `requirements.txt` rather than a separate
`requirements-dev.txt` — the Docker image and CI both install them as part of the normal
`pip install -r requirements.txt` step.

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
- On new file detected: waits briefly for write to complete, then calls `pipeline.run(filepath)`
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
6. `astrometry.solve(fits_path)` → returns WCS + two source lists: `sources` (strict star filter) and `sources_all` (loose filter — also keeps bright/saturated and faint detections, used for matching)
7. `subtraction.run(fits_path, archive_dir, filter_name)` → if ≥`SUBTRACTION_MIN_FRAMES` archived frames of the same object exist, aligns them (via `astroalign`), builds a median reference, subtracts, and returns candidate sources found only in the difference image. These are merged into the source list and flagged `_from_subtraction=True`. Skipped gracefully otherwise.
8. `catalog_matcher.match(sources, frame_meta)` → identifies known objects. **Runs before photometry** so matched Gaia DR3 stars can serve as the photometric zero-point reference.
8.5. `_dedupe_by_catalog_identity(sources, extra)` → collapses sources that share the same
     `(catalog_name, catalog_id)` within this one frame into a single representative source
     (see Known Issues #9) — otherwise a moving object matched both by the normal detection
     and by a nearby subtraction candidate would be posted/classified as two separate
     observations of the same object.
9. `photometry.measure(fits_path, sources)` → returns calibrated magnitudes
10. Populate each source's unified `mag` field (`mag_calibrated` if `calibrated`, else
    `mag_instrumental`) — this is the field the API payload documents and the one
    `anomaly_detector.py` reads for magnitude-change comparisons.
11. `api_client.post_frame(frame_data)` → registers the frame, gets back `frame_id`
12. `api_client.post_sources(frame_id, filename, sources)` → saves all detected sources (already
    catalog-matched and photometrically calibrated); returns `source_ids` (positionally parallel
    to `sources`), which this step zips back onto each source dict as `_source_id` so
    `anomaly_detector.py` can populate `anomalies[].source_id` (see Known Issues #8).
13. `anomaly_detector.detect(frame_id, sources, catalog_matches, frame_meta)` → finds anomalies, using the batched history/coverage API calls (see `api_client/client.py` below)
14. `api_client.post_anomalies(frame_id, filename, anomalies)` → saves anomalies
15. Move file to `/fits/archive/{object_name}/` directory

> **Note:** this order differs from earlier revisions of this document. Catalog matching and
> photometry now run **before** `post_frame`/`post_sources` (not after), and image subtraction
> (step 7) is a step inserted between astrometry and catalog matching that did not exist before.
> The `sources`/`sources_all` selection (step 6) is made immediately after astrometry, *before*
> step 7's merge — an earlier revision made this selection later (in what is now step 8),
> which raised `UnboundLocalError` on every frame where subtraction actually found candidates;
> see the resolved Known Issues #3.

**Calibration frames (Dark, Flat, Bias):** These frames are used for image calibration but
contain no astronomical data to analyze. The pipeline simply normalizes the filename
(if `NORMALIZE_ENABLED=true`) and moves them to the archive. No QC, astrometry, photometry,
or API calls are performed.

### `modules/qc.py`
Computes quality metrics from a FITS file without plate solving:
- **FWHM** (median over detected stars) — indicator of focus quality
- **Elongation** (major/minor axis ratio of PSF ellipse) — indicator of tracking/trailing
- **SNR** (signal-to-noise ratio of detected sources) — computed and reported, but currently
  **not** used in the pass/fail decision (see Known Issues #4 — `QC_SNR_MIN` is effectively dead)
- **Sky background** (median + sigma after sigma-clipping)
- **Star count** (minimum threshold check against `QC_STARS_MIN`; a hard-coded floor of 3 raw
  detections is also enforced independently, before `QC_STARS_MIN` is even applied)
- **Cosmic ray fraction** (via astroscrappy)

Quality flags and handling:
| Condition | Flag | Action |
|---|---|---|
| FWHM > QC_FWHM_MAX_ARCSEC | `BLUR` | Move to `/fits/rejected/{object}/BLUR_filename.fits` |
| Elongation > QC_ELONGATION_MAX | `TRAIL` | Move to `/fits/rejected/{object}/TRAIL_filename.fits` |
| Star count < QC_STARS_MIN (or < 3 raw detections) | `LOW_STARS` | Move to `/fits/rejected/{object}/LOW_STARS_filename.fits` |
| Multiple issues, or a FITS read / background-estimation / extraction failure | `BAD` | Move to `/fits/rejected/{object}/BAD_filename.fits` |
| All good | `OK` | Continue processing |

**Important:** Bad frames are NOT sent to the API. They are moved to the `rejected` folder
with a prefix indicating the rejection reason. This saves bandwidth, storage, and keeps the
database clean from unusable data.

### `modules/fits_header.py`
- Reads FITS primary header using `astropy.io.fits`
- Normalizes keyword aliases (e.g., `CCD-TEMP` vs `CCDTEMP`)
- Also extracts pixel size / plate-scale aliases (`XPIXSZ`, `PIXSIZE`, `PIXSCALE1`, `PIXELSZ`, `PIXSCALE`), used later to estimate FOV
- Returns structured dict ready for API payload
- Extracts `OBJECT` field for directory organization

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
- Calls `astap` binary as a subprocess via `xvfb-run` (astap needs a display even headless) for plate solving
- Parses resulting WCS header written back into the FITS file
- Runs `sep` (SourceExtractor) for source detection, dynamically narrowing the upper FWHM bound using an estimated `psf_fwhm_arcsec` when available
- Converts pixel coordinates to (RA, Dec) using `astropy.wcs.WCS`
- Returns a dict: `{ra_center, dec_center, fov_deg, naxis1, naxis2, sources, sources_all, wcs}`
  - `sources` — strict star filter, list of dicts `{ra, dec, flux, fwhm, elongation, ...}`
  - `sources_all` — loose filter; additionally keeps bright/saturated and faint detections rejected by the strict filter, used downstream for catalog matching / WCS offset correction so moving or transient objects aren't lost
  - `wcs` — the `astropy.wcs.WCS` object itself, also consumed by `modules/subtraction.py` to convert difference-image pixel candidates back to sky coordinates

### `modules/photometry.py`
- Aperture photometry via `photutils.aperture`
- Differential photometry against Gaia reference stars in the field (requires ≥3 Gaia DR3 matches to compute a zero-point) — this makes brightness measurements immune to atmospheric transparency variations
- Adds the following fields to each source: `flux_aperture`, `flux_err`, `mag_instrumental`, `mag_calibrated`, `mag_err`, `calibrated` (bool), `edge_flag`, `zero_point`, `zero_point_err`

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
4. Detects sources on the difference image via `sep.Background` + `sep.extract`, with threshold `SUBTRACTION_DETECT_SIGMA × background_rms`. `fwhm`/`elongation` per candidate are derived from `sep`'s `a`/`b` second-moment axes (same Gaussian approximation as `modules/astrometry.py`), since `sep.extract()` doesn't return a native `fwhm` field.
5. Converts detected pixel positions back to (RA, Dec) using the frame's WCS.
6. Returns `{"performed": bool, "reference_frame_count": int, "candidates": [...]}`. Every
   candidate is tagged `_from_subtraction=True` so `anomaly_detector.py` can apply looser
   coverage rules to it (see below).

Gracefully skipped (`performed=False`) when fewer than `SUBTRACTION_MIN_FRAMES` archived frames
exist yet — e.g. the very first observations of a new target.

Covered by `tests/test_subtraction.py` (previously this module had no test file — see the
resolved Known Issues #5).

### `modules/catalog_matcher.py`
Cross-matches the source list against external catalogs using
`astropy.coordinates.SkyCoord.match_to_catalog_sky()` with cone radius `MATCH_CONE_ARCSEC`
(`MOVING_CONE_ARCSEC` for the MPC step, since moving objects shift between frames).

Before matching, computes a **WCS offset correction**: an all-pairs vote-accumulator matches
the source list against Gaia DR3 to estimate a small systematic RA/Dec offset, then applies
that offset **in-place** to every source's `ra`/`dec` before the remaining catalogs are queried.

Catalogs queried **in this order** (sequential exclusive matching — once matched, a source
skips the remaining catalogs):
1. **Simbad** — named objects first: variable stars, binaries, galaxies, nebulae — provides rich `object_type`
2. **Gaia DR3** — dense stellar catalog with G-band magnitudes; also drives the WCS offset correction above
3. **2MASS** (VizieR catalog `II/246`) — fallback for red/cool stars (late M/K dwarfs, reddened) absent in Gaia; J-band magnitude
4. **Pan-STARRS DR1** (VizieR catalog `II/349/ps1`) — fallback for faint optical sources below Gaia's completeness limit; r-band magnitude; only queried when `dec_center > -30°` (Pan-STARRS1 sky coverage)
5. **MPC / SkyBot** — solar system objects (asteroids, comets) at the observation epoch; wider cone (`MOVING_CONE_ARCSEC`)

Rationale: Simbad first gives correct `object_type` for known named objects (instead of generic
"STAR"). Gaia handles the bulk of stars. 2MASS catches red/cool stars faint in the optical.
Pan-STARRS DR1 pushes depth further for the remaining faint optical sources — this directly
mitigates the "faint UNKNOWN" problem (see Known Issues #1), though only partially.

Rate limits (all free, no auth):
- Simbad, 2MASS & Pan-STARRS/VizieR: shared CDS infrastructure, ~5–6 req/sec recommended; 1-hr in-process cache is sufficient
- Gaia DR3: ESA TAP+, no hard limit, queries take 1–5 s; 1-hr cache is sufficient
- MPC/SkyBot: IMCCE, no hard limit; epoch-dependent

Each matched source is enriched **in-place** with `catalog_name`, `catalog_id`, `catalog_mag`,
`object_type` — its `ra`/`dec` fields are the already offset-corrected coordinates, there are
no separate `source_ra`/`source_dec` fields.
`catalog_mag` is G-band for Gaia, J-band for 2MASS, r-band for Pan-STARRS, `None` for Simbad/MPC.
Unmatched sources get `catalog_name = None`.

> **Naming note:** this document previously referred to this catalog as "Pan-STARRS DR2" in
> the Known Issues section below. The code actually queries **Pan-STARRS DR1** (VizieR
> `II/349/ps1`) — the docstrings in `catalog_matcher.py` itself are internally inconsistent
> about this too (module header says DR1, one inline comment says DR2).

### `modules/anomaly_detector.py`
Core logic. For all detected sources in a frame **at once** (batched, not one API round-trip per source):

Every returned anomaly dict includes `source_id` — the resolved `sources.id` read off the
source's `_source_id` key, which `pipeline.py`'s Step 7 attaches from the `source_ids` array
returned by `POST /frames/{id}/sources` (see Known Issues #8). `None` when that round-trip
couldn't resolve one (post_sources failed, or the API predates this field).

1. **Query history via API** — `POST /sources/near/batch` with every source position in a single call, returning historical sources near each (RA, Dec) from previous frames. This is queried for **every** source regardless of catalog-match status — an earlier revision only queried it for unmatched/MPC sources, which made the Δmag-based classifications below (`VARIABLE_STAR`, `BINARY_STAR`, and the "already-known host brightened" path of `SUPERNOVA_CANDIDATE`) permanently unreachable for any catalog-matched source, since they always saw an empty history; see the resolved Known Issues #3.
2. **Coverage check** — `POST /frames/covering/batch` — did we ever observe each sky position before? (batched the same way)

   Both batch calls replaced the older single-position `GET /sources/near` / `GET /frames/covering`
   endpoints, to avoid O(N) API round-trips per frame. Those single-position endpoints are still
   implemented and exported by `api_client/client.py`, but `anomaly_detector.py` no longer calls them.
3. **Classify** each source. Real priority order in code: MPC/SkyBot match first → position-shifted-but-unmatched (split into `MOVING_UNKNOWN` vs `SPACE_DEBRIS` by a PSF elongation > 3.0 threshold) → no historical coverage (→ `FIRST_OBSERVATION`, *unless* the source came from image subtraction — see below) → no prior detection at this exact position but near a Simbad galaxy (→ `SUPERNOVA_CANDIDATE`) → not in history or any catalog (→ `UNKNOWN`) → in catalog but not history (→ `KNOWN_CATALOG_NEW`) → **has** prior history and brightened beyond `DELTA_MAG_ALERT`: near a Simbad galaxy (→ `SUPERNOVA_CANDIDATE`) → known binary (→ `BINARY_STAR`) → known variable (→ `VARIABLE_STAR`):

| Situation | Classification |
|---|---|
| No historical coverage | `FIRST_OBSERVATION` — not an anomaly, just note |
| No historical coverage, but the source was detected via image subtraction (`_from_subtraction=True`) | `UNKNOWN` → **ALERT** (subtraction already confirms it's absent from the reference stack, so missing API coverage doesn't downgrade it) |
| Area covered, source not in history at all, near a Simbad galaxy | `SUPERNOVA_CANDIDATE` → **ALERT** (new point source, no baseline to compare against) |
| Area covered, source not in history, found in catalog (not a galaxy) | `KNOWN_CATALOG_NEW` — was below detection threshold |
| Area covered, source not in history, not in any catalog | `UNKNOWN` → **ALERT** |
| Source **has** prior history, brightened by more than `DELTA_MAG_ALERT`, near a Simbad galaxy | `SUPERNOVA_CANDIDATE` → **ALERT** (already-known host got brighter) |
| Source in history, Δmag > DELTA_MAG_ALERT, known binary (Simbad) | `BINARY_STAR` |
| Source in history, Δmag > DELTA_MAG_ALERT, known variable (Simbad) | `VARIABLE_STAR` |
| Source present but shifted > MATCH_CONE_ARCSEC, matches MPC | `ASTEROID` or `COMET` |
| Source present but shifted, not in MPC, elongation ≤ 3.0 | `MOVING_UNKNOWN` → **ALERT** |
| Source present but shifted, not in MPC, elongation > 3.0 (fast trail) | `SPACE_DEBRIS` → **ALERT** |

`SUPERNOVA_CANDIDATE` therefore has two independent triggers: a brand-new point source with no
prior detection at all near a known galaxy, and an already-catalogued/known galaxy that
*brightens* (not dims — a fading foreground star near a galaxy is not a supernova signature) by
more than `DELTA_MAG_ALERT`. Both use the same `MATCH_CONE_ARCSEC` (5″ by default) "near galaxy"
radius as ordinary star matching — there is no separate, wider radius for extended galaxy disks.

Magnitude comparisons (`delta_mag`) read the `mag` field that `pipeline.py` populates right
after `photometry.measure()` (see that module's section above) — `photometry.py` itself only
ever sets `mag_instrumental`/`mag_calibrated`, never `mag`.

4. For `ASTEROID` / `COMET`: calls `ephemeris.py` to compute current ephemeris via JPL Horizons.

`FAINT_UNCATALOGUED` (proposed in Known Issues #1) is **not implemented** — it's still only a
`TODO` comment in the source.

### `modules/ephemeris.py`
- Queries JPL Horizons via `astroquery.jplhorizons`
- Given MPC designation + observation time → returns predicted (RA, Dec, mag, distance_au, angular_velocity)
- Results included in the anomaly payload sent to API

### `api_client/client.py`
All communication with the remote `observatory-api`. Uses `httpx` with async support and
`tenacity` for automatic retry on transient failures.

**Retry behaviour:** `tenacity.retry(stop_after_attempt(3), wait_exponential(multiplier=1, min=2, max=10))`
on HTTP 5xx and transport/timeout errors — up to 3 attempts total (2 retries). HTTP 4xx errors
are logged immediately and never retried.

> **Correction:** with these exact `tenacity` parameters the actual wait between attempts is
> **2s, then 2s** — not "2s → 4s → 8s" as stated elsewhere (README.md, API.md). `min=2` clamps
> the first two exponential terms (which would be 1s and 2s) up to 2s each, and the attempt
> that would produce 4s/8s never happens because `stop_after_attempt(3)` stops the retry loop
> first. If genuinely exponential backoff up to 8s is desired, `stop_after_attempt` needs to
> allow a 4th attempt, or `multiplier` needs to be raised.

**Headers sent on every request:**
```
X-API-Key: {API_KEY}
Content-Type: application/json
Accept: application/json
```

**Endpoints used** (defined in `observatory-api`, listed here for reference):

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/frames` | Register a new processed frame |
| `POST` | `/frames/{id}/sources` | Save detected sources for a frame |
| `POST` | `/frames/{id}/anomalies` | Save detected anomalies |
| `GET` | `/sources/near` | Get historical sources near a single (RA, Dec) — still implemented/exported, but no longer called by `anomaly_detector.py` |
| `GET` | `/frames/covering` | Get frames that covered a single sky point — same caveat as above |
| `POST` | `/sources/near/batch` | Get historical sources near **multiple** (RA, Dec) positions in one call — used by `anomaly_detector.py` |
| `POST` | `/frames/covering/batch` | Coverage check for **multiple** sky positions in one call — used by `anomaly_detector.py` |
| `GET` | `/frames/{id}/qc` | (future) retrieve QC metrics — not implemented in the client yet |

Full request/response documentation for every endpoint (including the batch ones) lives in **`API.md`**.

Query parameters for `/sources/near`:
```
ra={float}&dec={float}&radius_arcsec={float}&before_time={ISO8601}
```

Query parameters for `/frames/covering`:
```
ra={float}&dec={float}&before_time={ISO8601}
```

The pipeline treats the API as a black box. If the API changes its DB schema internally,
the pipeline only cares that the endpoint contracts remain stable.

---

## File Organization by Target Object

Frames are organized into subdirectories based on the `OBJECT` FITS header keyword:

```
/fits/archive/
├── M51/
│   ├── frame_20240315_220134.fits
│   ├── frame_20240315_220434.fits
│   └── ...
├── NGC_1234/
│   └── ...
├── Andromeda/
│   └── ...
└── _UNKNOWN/
    └── ...  (frames without OBJECT header)

/fits/rejected/
├── M51/
│   ├── BLUR_frame_20240315_221034.fits
│   ├── TRAIL_frame_20240315_221534.fits
│   └── ...
├── NGC_1234/
│   └── BAD_frame_20240316_012345.fits
└── _UNKNOWN/
    └── ...
```

**Directory naming rules:**
- Object name is sanitized: spaces → underscores, special chars removed
- If `OBJECT` header is missing or empty → use `_UNKNOWN`
- Directories are created automatically if they don't exist

---

## Processing Flow — Single FITS File

```
New file detected by watchdog
        │
        ▼
fits_header.extract_headers()
  extract OBJECT, OBSERVER, CCD-TEMP, IMAGETYP, etc.
        │
        ▼
normalizer.normalize_headers()
  normalize object name, filter, frame type
  generate normalized filename
        │
        ▼
Check frame_type (IMAGETYP header)
  ├─ Dark/Flat/Bias → rename + move to /fits/archive/{object}/ → STOP (no analysis)
  └─ Light ─────────────────────────────────────────────────────────┐
                                                                     ▼
                                                            qc.analyze()
  ├─ BAD/BLUR/TRAIL/LOW_STARS → move to /fits/rejected/{object}/ → STOP (no API call)
  └─ OK ──────────────────────────────────────────────────────────────┐
                                                                       ▼
                                                          astrometry.solve()
                                                          plate solve + sources / sources_all
                                                                       │
                                                                       ▼
                                                          subtraction.run()  (optional)
                                                          diff vs archived frames → candidates,
                                                          merged into sources, _from_subtraction=True
                                                                       │
                                                                       ▼
                                                     catalog_matcher.match()
                                                     Simbad + Gaia DR3 + 2MASS + Pan-STARRS + MPC
                                                                       │
                                                                       ▼
                                                          photometry.measure()
                                                          calibrated magnitudes (Gaia zero-point)
                                                                       │
                                                                       ▼
                                                     api_client.post_frame()
                                                     → receive frame_id
                                                                       │
                                                                       ▼
                                                     api_client.post_sources()
                                                     (includes filename for correlation)
                                                                       │
                                                                       ▼
                                                     anomaly_detector.detect()
                                                     ├─ POST /sources/near/batch    (API)
                                                     ├─ POST /frames/covering/batch (API)
                                                     ├─ classify each source
                                                     └─ ephemeris.py for asteroids
                                                                       │
                                                                       ▼
                                                     api_client.post_anomalies()
                                                     (includes filename for correlation)
                                                                       │
                                                                       ▼
                                        rename to normalized filename
                                        move to /fits/archive/{object_normalized}/
```

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

---

## External Catalogs & APIs

Catalog matching order: **Simbad → Gaia DR3 → 2MASS → Pan-STARRS DR1 → MPC**

### Simbad
- Source: CDS Strasbourg (Centre de Données astronomiques de Strasbourg)
- Content: named astronomical objects — variable stars, double stars, galaxies, nebulae, quasars, etc.
- Access: `astroquery.simbad.Simbad.query_region()`
- Use: **first** — identifies object type (`V*`, `EB*`, `G`, `QSO`, etc.) for named objects
- Rate limit: ~5–6 req/sec (shared CDS infrastructure); 1-hr cache is sufficient

### Gaia DR3
- Source: ESA Gaia mission, Data Release 3
- Content: ~1.8 billion stars with precise positions, proper motions, G-band magnitudes
- Access: `astroquery.gaia.Gaia.cone_search()`
- Use: **second** — primary stellar reference for matching, WCS offset correction, and differential photometry
- Rate limit: no hard limit; queries take 1–5 s; 1-hr cache is sufficient

### 2MASS (Two Micron All Sky Survey)
- Source: IPAC / NASA; catalog hosted on VizieR (CDS)
- Content: ~470 million point sources to K≈14.3 / J≈15.8
- Access: `astroquery.vizier.Vizier.query_region(catalog="II/246")`
- Use: **third** — fallback for red/cool stars (late M/K dwarfs, reddened sources) faint or absent in Gaia; stores J-band magnitude
- Rate limit: same CDS infrastructure as Simbad; 1-hr cache is sufficient

### Pan-STARRS DR1
- Source: Pan-STARRS1 Surveys (University of Hawaii); catalog hosted on VizieR (CDS)
- Content: ~3 billion optical sources over δ > −30°, deeper than Gaia in the optical (~23.3 mag)
- Access: `astroquery.vizier.Vizier.query_region(catalog="II/349/ps1")`
- Use: **fourth** — fallback for faint optical sources below Gaia's completeness limit; stores r-band magnitude; mitigates (partially) the "faint UNKNOWN" problem in Known Issues #1
- Rate limit: same CDS/VizieR infrastructure as Simbad and 2MASS; 1-hr cache is sufficient
- Coverage limit: only queried for `dec_center > -30°`

### MPC (Minor Planet Center)
- Source: IAU Minor Planet Center / IMCCE SkyBot
- Content: all known asteroids and comets with orbital elements
- Access: `astroquery.imcce.Skybot.cone_search()` at observation epoch
- Use: **fifth** — identifying moving solar system objects; wider cone (`MOVING_CONE_ARCSEC`)

### JPL Horizons
- Source: NASA Jet Propulsion Laboratory
- Content: high-precision ephemerides for solar system bodies
- Access: `astroquery.jplhorizons.Horizons`
- Use: computing predicted position of a known asteroid/comet at observation time (called from `ephemeris.py`)

---

## Data Payloads (API Request Bodies)

### POST /frames
```json
{
  "filename": "M51_L_V_120_2024-03-15T22-01-34.fits",
  "original_filepath": "/fits/archive/M51/M51_L_V_120_2024-03-15T22-01-34.fits",
  "obs_time": "2024-03-15T22:01:34Z",
  "ra_center": 123.456,
  "dec_center": 45.678,
  "fov_deg": 1.25,
  "quality_flag": "OK",

  "observation": {
    "object": "M51",
    "exptime": 120.0,
    "filter": "V",
    "frame_type": "Light",
    "airmass": 1.23
  },

  "instrument": {
    "telescope": "Celestron EdgeHD 11",
    "camera": "ZWO ASI2600MM Pro",
    "focal_length_mm": 2800,
    "aperture_mm": 280
  },

  "sensor": {
    "temp_celsius": -10.0,
    "temp_setpoint_celsius": -10.0,
    "binning_x": 1,
    "binning_y": 1,
    "gain": 100,
    "offset": 50,
    "width_px": 6248,
    "height_px": 4176
  },

  "observer": {
    "name": "John Smith",
    "site_name": "Backyard Observatory",
    "site_lat": 55.7558,
    "site_lon": 37.6173,
    "site_elev_m": 150
  },

  "software": {
    "capture": "N.I.N.A. 2.1"
  },

  "qc": {
    "fwhm_median": 3.2,
    "elongation": 1.1,
    "snr_median": 42.5,
    "sky_background": 850.3,
    "star_count": 287
  }
}
```

> `qc.eccentricity` appeared in earlier revisions of this example but is **not** currently
> computed by `modules/qc.py` or sent by `pipeline.py` — removed from the example above to
> match reality. If it gets implemented later, add it back here.

**Note:** When `NORMALIZE_ENABLED=true` (default), all values are normalized before sending:
- `filename` — normalized filename (e.g., `M51_L_Ha_300_2024-03-15T22-01-34.fits`)
- `observation.object` — normalized object name (e.g., "M51")
- `observation.filter` — normalized filter name (e.g., "Ha")
- `observation.frame_type` — normalized frame type (e.g., "Light")

### POST /frames/{id}/sources

The `{id}` in URL is the `frame_id` returned from POST /frames. Additionally, `filename`
is included in the request body for logging and correlation purposes.

```json
{
  "filename": "M51_L_V_120_2024-03-15T22-01-34.fits",
  "sources": [
    {
      "ra": 123.461,
      "dec": 45.682,
      "mag": 14.23,
      "flux": 45230.5,
      "fwhm": 3.1,
      "catalog_name": "Gaia DR3",
      "catalog_id": "Gaia DR3 1234567890",
      "catalog_mag": 14.15,
      "object_type": "STAR"
    }
  ]
}
```

**Response** includes `source_ids` — positionally parallel to the request's `sources[]`
(`null` for a skipped/invalid entry) — which `pipeline.py` uses to attach `source_id` to the
matching anomaly before the next call (see `modules/anomaly_detector.py` and Known Issues #8):

```json
{
  "message": "Sources saved successfully",
  "count": 1,
  "new_sources": 0,
  "matched_sources": 1,
  "source_ids": ["6a7415c324e514.28790200"]
}
```

### POST /frames/{id}/anomalies

The `{id}` in URL is the `frame_id` returned from POST /frames. Additionally, `filename`
is included in the request body for logging and correlation purposes.

```json
{
  "filename": "M51_L_V_120_2024-03-15T22-01-34.fits",
  "anomalies": [
    {
      "anomaly_type": "ASTEROID",
      "source_id": "6a7415c324e514.28790200",
      "ra": 123.489,
      "dec": 45.701,
      "magnitude": 17.8,
      "delta_mag": null,
      "mpc_designation": "2019 XY3",
      "ephemeris": {
        "predicted_ra": 123.491,
        "predicted_dec": 45.700,
        "predicted_mag": 17.9,
        "distance_au": 1.23,
        "angular_velocity_arcsec_per_hour": 45.2
      },
      "notes": "Matched MPC object within 3.2 arcsec"
    },
    {
      "anomaly_type": "UNKNOWN",
      "ra": 123.502,
      "dec": 45.699,
      "magnitude": 16.1,
      "delta_mag": null,
      "mpc_designation": null,
      "ephemeris": null,
      "notes": "Not found in Gaia DR3, Simbad, or MPC within 5 arcsec. Area covered by 14 previous frames."
    }
  ]
}
```

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
  backoff (tenacity `wait_exponential` — currently ~2s between attempts, see `api_client/client.py`
  above), then log and continue — do not lose the frame
- Unit tests in `tests/` use `pytest` and mock all external calls (API, catalogs, astap subprocess)
- **All Markdown documents in this project are written in English** — this applies to every
  `.md` file (README.md, CLAUDE.md, API.md, ISSUES.md, everything under `docs/`, etc.),
  regardless of what language the request to write them was made in. Only the prose is
  English; code identifiers, config keys, and CLI examples inside those documents keep
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

### MariaDB spatial queries in the API
Since MariaDB lacks pgSphere, the API implements cone searches using a bounding-box WHERE clause
on indexed (ra, dec) columns, followed by Haversine filtering in PHP for precise distances.
This is fast enough for the expected data volumes (millions of sources).

### Frame coverage check
Before classifying a missing source as "truly new", the pipeline asks the API in a single
batched call per frame: "have we ever observed these sky points before?" (`POST /frames/covering/batch`).
Without this check, the first observation of any field would generate false UNKNOWN alerts
for every single source. (Earlier revisions of this pipeline made this call per-source via
`GET /frames/covering` — replaced by the batch endpoint to avoid O(N) API round-trips per frame.)

### Catalog query caching
Gaia and Simbad queries for a given sky region should be cached locally (simple dict or Redis)
within a pipeline run to avoid redundant network calls when multiple sources fall in the same
catalog tile. Cache TTL: 1 hour.

### Why bad frames go to /fits/rejected instead of API
Bad frames (blur, trailing, low star count) have no scientific value for the analysis pipeline.
Sending them to the API would:
- Waste bandwidth and storage
- Pollute the database with unusable data
- Complicate queries (need to filter by quality_flag everywhere)

Instead, they are moved locally to `/fits/rejected/` organized by target object, with a prefix
indicating the rejection reason. This allows manual review if needed, and keeps the API clean.

### Directory organization by OBJECT header
Frames are automatically organized into subdirectories based on the FITS `OBJECT` header.
This makes it easy to:
- Find all frames of a specific target
- Manage disk space per target
- Review observations by object
- Archive or delete old observation runs

---

## Anomaly Types Reference

| Type | Description | Alert? |
|---|---|---|
| `FIRST_OBSERVATION` | Sky area never observed before | No |
| `KNOWN_CATALOG_NEW` | Not in history but found in catalog | No |
| `VARIABLE_STAR` | Known variable, brightness changed | No (logged) |
| `BINARY_STAR` | Known binary, periodic variation | No (logged) |
| `ASTEROID` | Moving, matched in MPC | No (logged + ephemeris) |
| `COMET` | Moving, matched in MPC as comet | No (logged + ephemeris) |
| `SUPERNOVA_CANDIDATE` | New point source near a galaxy, or an already-known galaxy brightening beyond `DELTA_MAG_ALERT` | **YES** |
| `MOVING_UNKNOWN` | Moving, not in MPC, elongation ≤ 3.0 | **YES** |
| `SPACE_DEBRIS` | Moving, not in MPC, elongation > 3.0 (fast trail) | **YES** |
| `UNKNOWN` | New point source, not in any catalog, area covered — or detected via image subtraction regardless of coverage | **YES** |

`FAINT_UNCATALOGUED` does not exist yet — see Known Issues #1.

These 10 values are defined as `modules/anomaly_detector.py`'s `AnomalyType(str, Enum)` (a
`str` mixin, so it still serializes/compares as a plain string everywhere — API payloads,
`_ALERT_TYPES` membership checks, and existing test assertions using bare string literals all
work unchanged). The same 10 values are enforced as an `ENUM` column constraint on
`observatory-api`'s `anomalies.anomaly_type` (mirrored in `AnomalyModel::ALLOWED_TYPES`);
`FramesController::saveAnomalies` rejects any anomaly with an unrecognized `anomaly_type` with
`400` before inserting anything from that batch. The two lists must be kept in sync by hand —
adding `FAINT_UNCATALOGUED` later means updating both the Python enum and the API's migration/
model together.

---

## Common FITS Header Keywords Reference

For quick reference, here are the most common FITS keywords the pipeline should handle:

```
# Observation
DATE-OBS    = '2024-03-15T22:01:34'  / Observation date and time (UTC)
EXPTIME     = 120.0                   / Exposure time in seconds
OBJECT      = 'M51'                   / Target object name
FILTER      = 'V'                     / Filter name
IMAGETYP    = 'Light'                 / Frame type (Light, Dark, Flat, Bias)
AIRMASS     = 1.23                    / Atmospheric airmass

# Coordinates (may be updated by plate solving)
RA          = 202.4696                / Right ascension (degrees)
DEC         = 47.1952                 / Declination (degrees)
OBJCTRA     = '13 29 52.7'            / Object RA in HMS format
OBJCTDEC    = '+47 11 43'             / Object Dec in DMS format

# Instrument
TELESCOP    = 'Celestron EdgeHD 11'   / Telescope name
INSTRUME    = 'ZWO ASI2600MM Pro'     / Camera/instrument name
FOCALLEN    = 2800                    / Focal length in mm
APTDIA      = 280                     / Aperture diameter in mm

# Sensor
CCD-TEMP    = -10.0                   / Actual sensor temperature (Celsius)
SET-TEMP    = -10.0                   / Target sensor temperature
XPIXSZ      = 3.76                    / Pixel size in microns
XBINNING    = 1                       / Horizontal binning
YBINNING    = 1                       / Vertical binning
GAIN        = 100                     / Gain setting
OFFSET      = 50                      / Offset/bias setting
NAXIS1      = 6248                    / Image width in pixels
NAXIS2      = 4176                    / Image height in pixels

# Observer and site
OBSERVER    = 'John Smith'            / Observer name
SITENAME    = 'Backyard Observatory'  / Site name
SITELAT     = 55.7558                 / Site latitude (degrees)
SITELONG    = 37.6173                 / Site longitude (degrees)
SITEELEV    = 150                     / Site elevation (meters)

# Software
SWCREATE    = 'N.I.N.A. 2.1'          / Capture software
```

---

## Known Issues & Future Improvements

### 1. Faint UNKNOWN sources (mag > 20)

**Problem:** Sources fainter than ~20 mag are often marked as `UNKNOWN` anomalies because they
fall below the completeness limit of Gaia DR3 (~21 mag). These are NOT new discoveries — just
normal faint stars missing from the catalog.

**Status:** Partially mitigated. Pan-STARRS DR1 (depth ~23.3 mag) was added as a fourth catalog
in `modules/catalog_matcher.py` specifically to catch faint optical sources Gaia misses (see
"External Catalogs & APIs" above). However, there is still **no magnitude threshold** in
`anomaly_detector.py`'s `UNKNOWN` branch — a source that even Pan-STARRS doesn't catalog is
still unconditionally flagged `UNKNOWN`, however faint it is.

**Remaining possible solutions:**
- Add a magnitude threshold to skip/downgrade the `UNKNOWN` alert for sources with mag > 20 — not implemented
- Query SDSS DR17 (~22 mag, ~35% sky coverage) as a further fallback — not implemented
- Add a new classification `FAINT_UNCATALOGUED` distinct from true `UNKNOWN` — not implemented (still just a `TODO` comment)

**Location:** `modules/anomaly_detector.py`, the `UNKNOWN` classification branch (roughly
lines 513–540, with the `TODO` comment itself at lines ~514–519).

---

### 2. Catalog depth summary

| Catalog | Depth (mag) | Coverage | Used for | Order |
|---------|-------------|----------|----------|-------|
| Simbad | Variable | All-sky | Named objects (V*, G, EB*, etc.) — rich object types | 1st |
| Gaia DR3 | ~21 (complete to ~20) | All-sky | Primary stellar matching, photometry calibration (G-band) | 2nd |
| 2MASS | K≈14.3, J≈15.8 | All-sky | Fallback for red/cool stars absent in Gaia (J-band mag) | 3rd |
| Pan-STARRS DR1 | ~23.3 | δ > −30° | **Now used** — fallback for faint optical sources below Gaia completeness (r-band mag) | 4th |
| MPC/SkyBot | — | All-sky | Asteroids and comets at observation epoch | 5th |
| SDSS DR17 | ~22 | ~35% sky | NOT YET USED — could help further with faint sources (issue #1) | — |

---

### 3. ~~Possible `UnboundLocalError` in `pipeline.py` when subtraction finds candidates~~ — RESOLVED

**Problem (fixed):** In `pipeline.py`, the image-subtraction merge line `sources = sources + sub_candidates`
(Step 3.5) read `sources` **before** it had ever been assigned — the actual first assignment,
`sources: list = astro_result.get("sources_all") or ...`, happened later, in what was then
Step 4. Because a name assigned anywhere inside a Python function is local to the *whole*
function, that earlier read raised `UnboundLocalError` — but only on the code path where
`subtraction.run()` returned a non-empty `candidates` list, i.e. exactly once enough archived
reference frames of the object existed and subtraction was doing something useful. The
exception was caught by the surrounding `try/except`, so it never crashed the pipeline outright
— it silently discarded every subtraction candidate on every frame where subtraction actually
found something, which is precisely the case that matters (e.g. a moving object like Vesta not
caught by MPC/SkyBot, relying on the subtraction path as the fallback detection route).

**Fix:** the `sources`/`sources_all` selection now happens immediately after astrometry, before
the Step 3.5 merge (see the `pipeline.py` section above and its step-order note). Regression
tests: `tests/test_pipeline.py::test_subtraction_candidates_merged_without_crashing`.

---

### 4. `QC_SNR_MIN` is configured but not enforced

**Problem:** `config.QC_SNR_MIN` is documented (here, in `.env.example`, and in README.md) as
"minimum acceptable median SNR", but `modules/qc.py` computes and returns `snr_median` without
ever comparing it against `QC_SNR_MIN` in the BLUR/TRAIL/LOW_STARS/BAD decision logic. The
threshold currently has no effect on whether a frame is accepted or rejected.

**Location:** `modules/qc.py`, the flag-decision block (~lines 384–409); `config.py:52`.

---

### 5. ~~`.env.example` drift vs. `config.py`, and no tests for `subtraction.py`~~ — RESOLVED

- `.env.example` now matches `config.py`'s actual defaults: `MOVING_CONE_ARCSEC=120.0`
  (widened for fast movers like Vesta — see inline comment in `config.py`), plus
  `SUBTRACTION_MIN_FRAMES=3` / `SUBTRACTION_DETECT_SIGMA=5.0`, neither of which it used to
  define at all.
- `modules/subtraction.py` now has `tests/test_subtraction.py`, matching every other module
  under `modules/`.

---

### 6. RESOLVED — `anomaly_detector.py` never fetched history for catalog-matched sources, making `VARIABLE_STAR`/`BINARY_STAR`/the brightening path of `SUPERNOVA_CANDIDATE` permanently unreachable

**Problem (fixed):** `_prefetch_history_data()` only added a source's sky tile to the
source-history batch query when `catalog_name in (None, "MPC")`. `_classify_source_sync()`
mirrored this: for any catalog-matched source (`catalog_name is not None` — which is required
for `object_type` to ever be set at all, since it comes from Simbad) it forced `history = []`
unconditionally. But that same `history` value feeds the magnitude-change comparison further
down the function (`VARIABLE_STAR` / `BINARY_STAR`, and what is now the "already-known host
brightened" branch of `SUPERNOVA_CANDIDATE`) — all three require both a Simbad `object_type`
**and** a non-empty history, which was structurally impossible to have at the same time. A
supernova brightening in a galaxy that had already accumulated observation history (the normal
case for any non-trivial monitoring campaign) could never be classified this way.

**Fix:** history is now queried and computed for every source regardless of catalog-match
status. Regression tests: `tests/test_anomaly_detector.py::TestDetectStationaryClassifications`
(`test_detect_variable_star`, `test_detect_binary_star`,
`test_detect_supernova_candidate_brightening`,
`test_detect_no_anomaly_stable_star_with_history`).

---

### 7. RESOLVED — `modules/subtraction.py`: shape-mismatch gate and a `numpy.void.get()` misuse silently disabled subtraction

Two compounding bugs, both fixed:

- **Shape-equality gate before alignment.** `run()` skipped any archived reference frame whose
  pixel dimensions didn't exactly match the new frame's, *before* ever calling `astroalign`.
  `astroalign` is specifically designed to align frames with different resolution/scale/rotation
  by resampling onto the target's pixel grid — rejecting shape mismatches up front defeated the
  one case (different camera/resolution between the archived and new frames) subtraction most
  needs to handle.
- **`obj.get(...)` on a `numpy.void` record.** `_detect_diff_sources()` iterated `sep.extract()`'s
  structured array and called `.get("b", ...)` / `.get("fwhm", ...)` / `.get("a", ...)` on each
  row — but a `numpy.void` record supports bracket access (`obj["field"]`), not `.get()`. This
  raised `AttributeError` on the **first** detected object every time, which was caught by the
  function's own top-level `try/except` and turned into a silent `return []`. In other words,
  `_detect_diff_sources()` returned real candidates *never* — it only ever "succeeded" vacuously
  when there was nothing to detect in the first place. `"fwhm"` also isn't a native `sep.extract()`
  field; it's now derived from the `a`/`b` second-moment axes, matching `modules/astrometry.py`'s
  own FWHM formula.

**Fix:** removed the shape-equality gate (kept the `None`-load-failure check); replaced the
`.get()` calls with proper bracket access and a computed `fwhm`. Regression tests:
`tests/test_subtraction.py::TestRun::test_regression_differently_shaped_reference_frames_are_still_aligned`,
`TestDetectDiffSources::test_detects_injected_blob`.

---

### 8. RESOLVED — `anomalies.source_id` was never populated (always `NULL` in the API)

**Problem (fixed):** the `anomalies` table and its `AnomalyModel`/`saveAnomalies` controller
(in `observatory-api`) always supported an optional `source_id` FK to `sources.id`, but nothing
on the pipeline side ever set it. Two structural gaps, both fixed:

- `POST /frames/{id}/sources` created/matched a `sources` row for every valid source (see that
  module's section above) but never told the caller *which* `sources.id` it resolved to — the
  response only ever contained `count`/`new_sources`/`matched_sources`.
- `anomaly_detector.py` therefore had no `sources.id` to attach to the anomaly dicts it built,
  so `api_client.post_anomalies()` always sent `source_id` omitted, and the API's
  `$anomaly['source_id'] ?? null` fallback always resolved to `null`.
- Separately, the `anomalies.source_id` column itself had **no FK constraint** to `sources.id`
  at all (a stale comment in the migration justified this as "to allow TRUNCATE on sources").

**Fix:**
- `FramesController::saveSources` (`observatory-api`) now returns `source_ids` — positionally
  parallel to the request's `sources[]` array (`null` for a skipped/invalid entry).
- `api_client.post_sources()` returns that array; `pipeline.py`'s Step 7 zips it back onto each
  source dict as `_source_id`.
- `anomaly_detector.py` reads `_source_id` off each source and includes it as `source_id` in
  every anomaly dict it returns.
- `anomalies.source_id` now has a real FK to `sources.id` (`ON DELETE SET NULL`, `ON UPDATE
  CASCADE`) added to the existing `CreateAnomaliesTable` migration (not a new one) — an anomaly
  is a detection *event* and must survive its linked source later being removed from the
  catalog, so `SET NULL` rather than `CASCADE` on delete.

Regression tests: `tests/test_pipeline.py::test_source_id_propagated_to_anomaly_detector` (+
length-mismatch/`None` fallback tests), `tests/test_anomaly_detector.py` (`source_id` assertions
across the MPC/moving/UNKNOWN branches), and `observatory-api`'s
`tests/Feature/SourcesTest.php` / `AnomaliesTest.php` (`source_ids` response shape, FK
`ON DELETE SET NULL` behavior).

---

### 9. RESOLVED — Duplicate detections of the same catalog object within one frame inflated `sources.observation_count` and produced duplicate `anomalies` rows

**Problem (fixed):** a single physical moving object (e.g. an MPC-matched asteroid) could appear
more than once in one frame's `sources` list — once from the normal source-extractor detection
and again via one or more nearby image-subtraction candidates, since `MOVING_CONE_ARCSEC` (120″
by default) is wide enough for several nearby diff-image blobs to each independently match the
same MPC object. Every duplicate was posted to `POST /frames/{id}/sources` as a separate
observation (inflating `sources.observation_count` — e.g. Vesta showed `observation_count=9`
after only 6 frames) and classified independently by `anomaly_detector.py`, producing duplicate
`ASTEROID`/`COMET` rows in `anomalies` for what was physically one object seen once.

**Fix:** `pipeline.py` gained a new Step 4.5, immediately after catalog matching: sources sharing
the same `(catalog_name, catalog_id)` identity within one frame are collapsed into a single
representative source (`_dedupe_by_catalog_identity()`). Uncatalogued sources (`catalog_name is
None`) are never merged, since they have no stable identity to deduplicate on. When duplicates
exist, a normal detection is preferred over a subtraction candidate; among two of the same kind,
the brighter one (higher flux) is kept. Regression tests:
`tests/test_pipeline.py::TestDedupeByCatalogIdentity`,
`test_pipeline_dedupes_duplicate_catalog_matches_before_posting`.

---

### 10. RESOLVED — Duplicate frame registration from a duplicate filesystem event

**Problem (fixed):** the same FITS file was observed registered as **two separate frames** a few
seconds apart (same filename, same `obs_time`, identical photometry) — inflating
`object_stats.frame_count` and every one of that frame's sources' `observation_count` by one.
`watchdog` is known to occasionally deliver two `FileCreatedEvent`s for the same path — e.g. the
polling-based emitter used for Docker Desktop bind mounts on macOS, or a capture program that
writes-then-renames the file — and `watcher.py` had no guard against dispatching the same path
twice.

**Fix:** `watcher.py` now tracks in-flight paths in a module-level set guarded by a lock;
`process_fits_file()` skips dispatch if the path is already being processed, or if it no longer
exists (the common case where the duplicate event arrives after the first dispatch already
finished and moved the file out of `FITS_INCOMING`). Regression tests:
`tests/test_pipeline.py::test_process_fits_file_skips_nonexistent_path`,
`test_process_fits_file_skips_path_already_in_flight`,
`test_process_fits_file_clears_in_flight_marker_after_success`,
`test_process_fits_file_clears_in_flight_marker_even_on_failure`.
