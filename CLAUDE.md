# CLAUDE.md — Observatory FITS Analysis Pipeline

This file provides full context for AI-assisted development of the `observatory-pipeline` project.
Always read this file at the start of a session before writing any code.

---

## Task Management — GitHub Project

All tasks for this project are tracked in the GitHub Project board:
**https://github.com/users/miksrv/projects/10**

### Rules for Working with Tasks

1. **Use Project cards only** — do NOT create GitHub Issues, only project cards (draft items)
2. **Always provide a clear title** — short, descriptive, action-oriented (e.g., "Implement QC module", "Add Gaia DR3 cross-matching")
3. **Always write a description** — explain what needs to be done, acceptance criteria, and any relevant context
4. **Check the board before starting work** — look for existing cards related to your task
5. **Update card status** — move cards through columns as work progresses (Todo → In Progress → Done)

### Card Description Template

When creating a new card, include:
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
4. Cross-matches detected sources against external astronomical catalogs
5. Compares against historical observations stored in the remote database
6. Classifies anomalies (supernovae, asteroids, comets, variable stars, space debris, unknowns)
7. Computes ephemerides for known solar system objects
8. Reports everything to the remote API — the pipeline has **no direct database access**

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

**Security:** The pipeline server's outbound IP should be whitelisted on the cloud firewall.
The API key must be stored in `.env` and never committed to git.

---

## Docker Setup

### `docker-compose.yml`

```yaml
services:
  pipeline:
    build: .
    volumes:
      - /data/fits/incoming:/fits/incoming
      - /data/fits/archive:/fits/archive
      - /data/fits/rejected:/fits/rejected
      - /data/astap/catalogs:/astap/catalogs
    env_file:
      - .env
    restart: unless-stopped
```

### `Dockerfile`

```dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libcfitsio-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install astap binary
RUN wget -q https://www.hnsky.org/astap/astap_amd64 -O /usr/local/bin/astap \
    && chmod +x /usr/local/bin/astap

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "watcher.py"]
```

### `.env.example`

```
API_BASE_URL=https://your-cloud-host.com/api/v1
API_KEY=your-secret-api-key-here

FITS_INCOMING=/fits/incoming
FITS_ARCHIVE=/fits/archive
FITS_REJECTED=/fits/rejected
ASTAP_BINARY=/usr/local/bin/astap
ASTAP_CATALOGS=/astap/catalogs

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
# Cone to search for moving objects (wider)
MOVING_CONE_ARCSEC=30.0
# Magnitude delta to trigger variability alert
DELTA_MAG_ALERT=0.5

# Normalization (enabled by default)
# When true, normalizes object names (M 51 → M51), filter names (Blue → B),
# frame types (Light Frame → Light), and renames files to standard format.
NORMALIZE_ENABLED=true
```

---

## Project Structure


```
observatory-pipeline/
├── CLAUDE.md                  ← this file
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
│   ├── catalog_matcher.py     ← cross-match: Gaia DR3, Simbad/Vizier, MPC
│   ├── anomaly_detector.py    ← comparison with history + anomaly classification
│   └── ephemeris.py           ← JPL Horizons queries for solar system objects
│
├── api_client/
│   ├── __init__.py
│   └── client.py              ← all HTTP calls to the observatory-api
│
└── tests/
    ├── test_qc.py
    ├── test_fits_header.py
    ├── test_normalizer.py
    ├── test_astrometry.py
    └── test_anomaly_detector.py
```

---

## Python Dependencies (`requirements.txt`)

```
astropy>=6.0
astroquery>=0.4.7
photutils>=1.12
sep>=1.4
astroscrappy>=1.1
numpy>=1.26
httpx>=0.27          # async HTTP client for API calls
tenacity>=8.2        # retry logic for API calls
watchdog>=4.0
python-dotenv>=1.0
```

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
6. `astrometry.solve(fits_path)` → returns WCS + source list `[(ra, dec, flux, fwhm, elongation), ...]`
7. `photometry.measure(fits_path, sources)` → returns calibrated magnitudes
8. `api_client.post_frame(frame_data)` → registers the frame, gets back `frame_id`
9. `api_client.post_sources(frame_id, filename, sources)` → saves all detected sources
10. `catalog_matcher.match(sources, frame_meta)` → identifies known objects
11. `anomaly_detector.detect(frame_id, sources, catalog_matches)` → finds anomalies
12. `api_client.post_anomalies(frame_id, filename, anomalies)` → saves anomalies
13. Move file to `/fits/archive/{object_name}/` directory

**Calibration frames (Dark, Flat, Bias):** These frames are used for image calibration but
contain no astronomical data to analyze. The pipeline simply normalizes the filename
(if `NORMALIZE_ENABLED=true`) and moves them to the archive. No QC, astrometry, photometry,
or API calls are performed.

### `modules/qc.py`
Computes quality metrics from a FITS file without plate solving:
- **FWHM** (median over detected stars) — indicator of focus quality
- **Elongation** (major/minor axis ratio of PSF ellipse) — indicator of tracking/trailing
- **SNR** (signal-to-noise ratio of detected sources)
- **Sky background** (median + sigma after sigma-clipping)
- **Star count** (minimum threshold check)
- **Cosmic ray fraction** (via astroscrappy)

Quality flags and handling:
| Condition | Flag | Action |
|---|---|---|
| FWHM > QC_FWHM_MAX_ARCSEC | `BLUR` | Move to `/fits/rejected/{object}/BLUR_filename.fits` |
| Elongation > QC_ELONGATION_MAX | `TRAIL` | Move to `/fits/rejected/{object}/TRAIL_filename.fits` |
| Star count < QC_STARS_MIN | `LOW_STARS` | Move to `/fits/rejected/{object}/LOW_STARS_filename.fits` |
| Multiple issues | `BAD` | Move to `/fits/rejected/{object}/BAD_filename.fits` |
| All good | `OK` | Continue processing |

**Important:** Bad frames are NOT sent to the API. They are moved to the `rejected` folder
with a prefix indicating the rejection reason. This saves bandwidth, storage, and keeps the
database clean from unusable data.

### `modules/fits_header.py`
- Reads FITS primary header using `astropy.io.fits`
- Normalizes keyword aliases (e.g., `CCD-TEMP` vs `CCDTEMP`)
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
| `Andromeda Galaxy` | `Andromeda_Galaxy` |

**Filter Name Normalization:**
| Input | Normalized |
|---|---|
| `Luminance`, `Lum`, `L`, `Clear` | `L` |
| `Red`, `RED`, `r` | `R` |
| `Blue`, `BLUE`, `b` | `B` |
| `H-Alpha`, `Halpha`, `Ha` | `Ha` |
| `OIII`, `O3`, `[OIII]` | `OIII` |
| `SII`, `S2`, `[SII]` | `SII` |

**Frame Type Normalization:**
| Input | Normalized |
|---|---|
| `Light Frame`, `light`, `LIGHT`, `Object` | `Light` |
| `Dark Frame`, `dark` | `Dark` |
| `Flat Field`, `flat`, `skyflat` | `Flat` |
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
- Calls `astap` binary as subprocess for plate solving
- Parses resulting WCS header written back into the FITS file
- Runs `sep` (SourceExtractor) for source detection
- Converts pixel coordinates to (RA, Dec) using `astropy.wcs.WCS`
- Returns: frame center (RA, Dec), FOV in degrees, list of sources

### `modules/photometry.py`
- Aperture photometry via `photutils.aperture`
- Differential photometry against Gaia reference stars in the field
- Returns instrumental magnitude + calibrated magnitude where possible

### `modules/catalog_matcher.py`
Cross-matches the source list against external catalogs using
`astropy.coordinates.SkyCoord.match_to_catalog_sky()` with cone radius `MATCH_CONE_ARCSEC`.

Catalogs queried **in this order** (sequential exclusive matching — once matched, source skips remaining):
1. **Simbad** — named objects first: variable stars, binaries, galaxies, nebulae — provides rich `object_type`
2. **Gaia DR3** — dense stellar catalog with G-band magnitudes; also performs WCS offset correction on all sources
3. **2MASS** (VizieR catalog `II/246`) — fallback for red/cool stars (late M/K dwarfs, reddened) absent in Gaia; J-band magnitude
4. **MPC / SkyBot** — solar system objects (asteroids, comets) at the observation epoch; wider cone (`MOVING_CONE_ARCSEC`)

Rationale: Simbad first gives correct `object_type` for known named objects (instead of generic "STAR").
Gaia handles the bulk of stars. 2MASS catches the remainder that are faint in the optical but bright in NIR.

Rate limits (all free, no auth):
- Simbad & 2MASS/VizieR: CDS infrastructure, ~5–6 req/sec recommended; 1-hr in-process cache is sufficient
- Gaia DR3: ESA TAP+, no hard limit, queries take 1–5 s; 1-hr cache is sufficient
- MPC/SkyBot: IMCCE, no hard limit; epoch-dependent

Returns for each source: `{source_ra, source_dec, catalog_name, catalog_id, catalog_mag, object_type}`
`catalog_mag` is G-band for Gaia, J-band for 2MASS, None for Simbad/MPC.
Unmatched sources get `catalog_name = None`.

### `modules/anomaly_detector.py`
Core logic. For each detected source:

1. **Query history via API** — GET sources near (RA, Dec) from previous frames covering this sky area
2. **Coverage check** — did we ever observe this sky area before? (via API: frames covering this point)
3. **Classify**:

| Situation | Classification |
|---|---|
| No historical coverage | `FIRST_OBSERVATION` — not an anomaly, just note |
| Area covered, source not in history, found in catalog | `KNOWN_CATALOG_NEW` — was below detection threshold |
| Area covered, source not in history, not in any catalog | `UNKNOWN` → **ALERT** |
| Source in history, Δmag > DELTA_MAG_ALERT, near galaxy (Simbad) | `SUPERNOVA_CANDIDATE` → **ALERT** |
| Source in history, Δmag > DELTA_MAG_ALERT, known variable (Simbad) | `VARIABLE_STAR` |
| Source present but shifted > MATCH_CONE_ARCSEC, matches MPC | `ASTEROID` or `COMET` |
| Source present but shifted, not in MPC | `MOVING_UNKNOWN` → **ALERT** |
| Fast trail, not in MPC | `SPACE_DEBRIS` |

4. For `ASTEROID` / `COMET`: calls `ephemeris.py` to compute current ephemeris via JPL Horizons.

### `modules/ephemeris.py`
- Queries JPL Horizons via `astroquery.jplhorizons`
- Given MPC designation + observation time → returns predicted (RA, Dec, mag, distance_au, angular_velocity)
- Results included in the anomaly payload sent to API

### `api_client/client.py`
All communication with the remote `observatory-api`. Uses `httpx` with async support and
`tenacity` for automatic retry on transient failures (3 retries, exponential backoff).

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
| `GET` | `/sources/near` | Get historical sources near (RA, Dec) |
| `GET` | `/frames/covering` | Get frames that covered a sky point |
| `GET` | `/frames/{id}/qc` | (future) retrieve QC metrics |

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
                                                          plate solve + sources
                                                                       │
                                                                       ▼
                                                          photometry.measure()
                                                          calibrated magnitudes
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
                                                     catalog_matcher.match()
                                                     Gaia + Simbad + MPC
                                                                       │
                                                                       ▼
                                                     anomaly_detector.detect()
                                                     ├─ GET /sources/near  (API)
                                                     ├─ GET /frames/covering (API)
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
Values > 2.0 indicate trailing (telescope tracking problem) or strong coma.

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

Catalog matching order: **Simbad → Gaia DR3 → 2MASS → MPC**

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
- Use: **second** — primary stellar reference for matching and differential photometry; also performs WCS offset correction
- Rate limit: no hard limit; queries take 1–5 s; 1-hr cache is sufficient

### 2MASS (Two Micron All Sky Survey)
- Source: IPAC / NASA; catalog hosted on VizieR (CDS)
- Content: ~470 million point sources to K≈14.3 / J≈15.8
- Access: `astroquery.vizier.Vizier.query_region(catalog="II/246")`
- Use: **third** — fallback for red/cool stars (late M/K dwarfs, reddened sources) faint or absent in Gaia; stores J-band magnitude
- Rate limit: same CDS infrastructure as Simbad; 1-hr cache is sufficient

### MPC (Minor Planet Center)
- Source: IAU Minor Planet Center / IMCCE SkyBot
- Content: all known asteroids and comets with orbital elements
- Access: `astroquery.imcce.Skybot.cone_search()` at observation epoch
- Use: **fourth** — identifying moving solar system objects; wider cone (`MOVING_CONE_ARCSEC`)

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
    "star_count": 287,
    "eccentricity": 0.4
  }
}
```

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
      "object_type": "STAR"
    }
  ]
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
- Log using Python `logging` module (structured, with `frame_id` and `filename` in every record)
- Errors in external catalog queries (network timeout, rate limit) must be caught and logged —
  they must NOT crash the pipeline. The frame should still be processed with partial results.
- Errors in the observatory API calls: retry 3 times with exponential backoff (tenacity),
  then log and continue — do not lose the frame
- Unit tests in `tests/` use `pytest` and mock all external calls (API, catalogs, astap subprocess)

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

### MariaDB spatial queries in the API
Since MariaDB lacks pgSphere, the API implements cone searches using a bounding-box WHERE clause
on indexed (ra, dec) columns, followed by Haversine filtering in PHP for precise distances.
This is fast enough for the expected data volumes (millions of sources).

### Frame coverage check
Before classifying a missing source as "truly new", the pipeline asks the API:
"Have we ever observed this sky point before?" (`GET /frames/covering`).
Without this check, the first observation of any field would generate false UNKNOWN alerts
for every single source.

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
| `SUPERNOVA_CANDIDATE` | Brightening in/near galaxy, not in catalogs | **YES** |
| `MOVING_UNKNOWN` | Moving, not in MPC | **YES** |
| `SPACE_DEBRIS` | Fast trail, not in MPC | **YES** |
| `UNKNOWN` | New point source, not in any catalog, area covered | **YES** |

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

**Possible solutions:**
- Add magnitude threshold to skip UNKNOWN alert for sources with mag > 20
- Query deeper catalogs (Pan-STARRS DR2 ~23.3 mag, SDSS DR17 ~22 mag) for faint sources
- Add new classification `FAINT_UNCATALOGUED` distinct from true `UNKNOWN`

**Location:** `modules/anomaly_detector.py` line ~481

---

### 2. Catalog depth summary

| Catalog | Depth (mag) | Coverage | Used for | Order |
|---------|-------------|----------|----------|-------|
| Simbad | Variable | All-sky | Named objects (V*, G, EB*, etc.) — rich object types | 1st |
| Gaia DR3 | ~21 (complete to ~20) | All-sky | Primary stellar matching, photometry calibration (G-band) | 2nd |
| 2MASS | K≈14.3, J≈15.8 | All-sky | Fallback for red/cool stars absent in Gaia (J-band mag) | 3rd |
| MPC/SkyBot | — | All-sky | Asteroids and comets at observation epoch | 4th |
| Pan-STARRS DR2 | ~23.3 | δ > −30° | NOT YET USED — could help with faint sources (issue #1) | — |
| SDSS DR17 | ~22 | ~35% sky | NOT YET USED — could help with faint sources (issue #1) | — |

