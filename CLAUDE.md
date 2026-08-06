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
- Tracks in-flight paths in a module-level set guarded by a lock, so a duplicate filesystem
  event for the same path (watchdog is known to occasionally deliver two `FileCreatedEvent`s —
  e.g. the polling emitter used for Docker Desktop bind mounts on macOS) is skipped rather than
  processed twice
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
6. `astrometry.solve(fits_path)` → returns WCS + two source lists: `sources` (strict star filter) and `sources_all` (loose filter — also keeps bright/saturated and faint detections, used for matching). This selection is made *before* step 7's merge below — `sources`/`sources_all` must already exist as names before anything tries to extend them.
7. `subtraction.run(fits_path, archive_dir, filter_name, wcs=astro_result["wcs"])` → if ≥`SUBTRACTION_MIN_FRAMES` archived frames of the same object exist, aligns them (via `astroalign`), builds a median reference, subtracts, and returns candidate sources found only in the difference image. These are merged into the source list and flagged `_from_subtraction=True`. Skipped gracefully otherwise. The `wcs` passed here is step 6's already-solved WCS, not re-derived from `fits_path`'s own header — that header isn't corrected until step 14.5 archives the frame (see `modules/astrometry.py`'s section below), so re-deriving it here would give subtraction's candidates a different systematic sky-position offset than every other source in the frame.
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
13. `anomaly_detector.detect(frame_id, sources, catalog_matches, frame_meta)` → finds anomalies, using the batched history/coverage API calls (see `api_client/client.py` below)
14. `api_client.post_anomalies(frame_id, filename, anomalies)` → saves anomalies
14.5. Move file to `/fits/archive/{object_name}/` directory — must run **before** step 15: that
     step looks up this same frame's own file at its archive path, so moving it later than
     chart generation would mean the current epoch is never found there.
15. `finder_chart.update_charts_for_sources(anomaly_type_by_source_id)` → for every anomaly with
     a resolved `source_id` (deduped per frame), (re)generates and uploads that source's finder/
     discovery chart, one call for the whole frame: fetches every source's full position track in
     a single `POST /sources/tracks/batch` call, renders each against the matching local archive
     FITS files, then uploads every rendered chart in a single `POST /sources/charts/batch` call
     — one HTTP round trip each for the whole frame, regardless of how many anomalies it has.
     Best-effort — gated by `CHART_ENABLED`, and any failure (missing local file, API error,
     rendering error) only downgrades that one source_id's own result to `False`; it never
     affects any other source_id in the same call or frame processing overall. See
     `modules/finder_chart.py` below.

**Calibration frames (Dark, Flat, Bias):** These frames are used for image calibration but
contain no astronomical data to analyze. The pipeline simply normalizes the filename
(if `NORMALIZE_ENABLED=true`) and moves them to the archive. No QC, astrometry, photometry,
or API calls are performed.

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
- **Star count** (minimum threshold check against `QC_STARS_MIN`; a hard-coded floor of 3 raw
  detections is also enforced independently, before `QC_STARS_MIN` is even applied)
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
- Runs `sep` (SourceExtractor) for source detection, dynamically narrowing the upper FWHM bound using an estimated `psf_fwhm_arcsec` when available
- Converts pixel coordinates to (RA, Dec) using `astropy.wcs.WCS`
- Returns a dict: `{ra_center, dec_center, fov_deg, naxis1, naxis2, sources, sources_all, wcs}`
  - `sources` — strict star filter, list of dicts `{ra, dec, flux, fwhm, elongation, saturated, ...}`
  - `sources_all` — loose filter; additionally keeps bright/saturated and faint detections rejected by the strict filter, used downstream for catalog matching / WCS offset correction so moving or transient objects aren't lost
  - `wcs` — the `astropy.wcs.WCS` object itself, also consumed by `modules/subtraction.py` to convert difference-image pixel candidates back to sky coordinates
  - `saturated` (bool, on every source in both lists) — raw ADU at the detection's peak
    (`sep`'s background-subtracted `peak` field with `bkg.globalback` added back) at or above
    `SATURATION_ADU`. Added because bright/saturated stars are deliberately kept in `sources_all`
    (to not lose asteroids), but aperture photometry on a saturated PSF core produces a physically
    meaningless flux — `modules/photometry.py` reads this flag to skip measuring such a source
    instead of returning an extreme (e.g. −14) magnitude. See docs/ISSUES.md #2.

### `modules/photometry.py`
- Aperture photometry via `photutils.aperture`
- Differential photometry against Gaia reference stars in the field (requires ≥3 Gaia DR3 matches to compute a zero-point) — this makes brightness measurements immune to atmospheric transparency variations
- Adds the following fields to each source: `flux_aperture`, `flux_err`, `mag_instrumental`, `mag_calibrated`, `mag_err`, `calibrated` (bool), `edge_flag`, `zero_point`, `zero_point_err`
- A source carrying `saturated=True` (set by `astrometry.solve()`) is never measured — all of the
  fields above stay `None` for it, exactly as for an out-of-bounds source. Saturated sources are
  also excluded from the Gaia DR3 reference set used to compute the frame's zero-point, so one
  saturated "Gaia match" can't corrupt calibration for every other source in the frame. See
  docs/ISSUES.md #2.

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
5. Detects sources on the (masked) difference image via `sep.Background` + `sep.extract`, with threshold `SUBTRACTION_DETECT_SIGMA × background_rms`. `fwhm`/`elongation` per candidate are derived from `sep`'s `a`/`b` second-moment axes (same Gaussian approximation as `modules/astrometry.py`), since `sep.extract()` doesn't return a native `fwhm` field.
6. Converts detected pixel positions back to (RA, Dec) using the frame's WCS — preferring the
   already-solved `wcs` passed in from `astrometry.solve()` (see `pipeline.py` step 7 above) over
   re-deriving one from the new frame's own header, which can still carry a stale/mount-pointing
   WCS at this point (`pipeline.py` only corrects the header at archive time, step 14.5 — see
   `modules/astrometry.py`'s section below). `wcs=None` (e.g. a caller that never ran astrometry
   itself) falls back to reading whatever WCS the file's own header has.
7. Returns `{"performed": bool, "reference_frame_count": int, "candidates": [...]}`. Every
   candidate is tagged `_from_subtraction=True` so `anomaly_detector.py` can apply looser
   coverage rules to it (see below).

Gracefully skipped (`performed=False`) when fewer than `SUBTRACTION_MIN_FRAMES` archived frames
exist yet — e.g. the very first observations of a new target.

### `modules/catalog_matcher.py`
Cross-matches the source list against external catalogs using
`astropy.coordinates.SkyCoord.match_to_catalog_sky()` with cone radius `MATCH_CONE_ARCSEC`
(`MOVING_CONE_ARCSEC` for the MPC step, since moving objects shift between frames).

Before matching, computes a **WCS offset correction**: an all-pairs vote-accumulator matches
the source list against Gaia DR3 to estimate a small systematic RA/Dec offset, then applies
that offset **in-place** to every source's `ra`/`dec` before the remaining catalogs are queried.

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

### `modules/anomaly_detector.py`
Core logic. For all detected sources in a frame **at once** (batched, not one API round-trip per source):

Every returned anomaly dict includes `source_id` — the resolved `sources.id` read off the
source's `_source_id` key, which `pipeline.py`'s Step 12 attaches from the `source_ids` array
returned by `POST /frames/{id}/sources`. `None` when that round-trip couldn't resolve one
(post_sources failed, or the API predates this field).

1. **Query history via API** — `POST /sources/near/batch` with every source position in a single call, returning historical sources near each (RA, Dec) from previous frames. Queried for **every** source regardless of catalog-match status — this is what makes the Δmag-based classifications below (`VARIABLE_STAR`, `BINARY_STAR`, and the "already-known host brightened" path of `SUPERNOVA_CANDIDATE`) reachable at all for a catalog-matched source.
2. **Coverage check** — `POST /frames/covering/batch` — did we ever observe each sky position before? (batched the same way)
3. **Classify** each source. Real priority order in code: MPC/SkyBot match first → **if unmatched (`catalog_name is None`) and `saturated=True`, suppressed outright** (see below) → position-shifted-but-unmatched (split into `MOVING_UNKNOWN` vs `SPACE_DEBRIS` by a PSF elongation > 3.0 threshold) → no historical coverage (→ `FIRST_OBSERVATION`, *unless* the source came from image subtraction — see below) → no prior detection at this exact position but near a Simbad galaxy (→ `SUPERNOVA_CANDIDATE`) → not in history or any catalog (→ `UNKNOWN`) → in catalog but not history (→ `KNOWN_CATALOG_NEW`) → **has** prior history and brightened beyond `DELTA_MAG_ALERT`: near a Simbad galaxy (→ `SUPERNOVA_CANDIDATE`) → known binary (→ `BINARY_STAR`) → known variable (→ `VARIABLE_STAR`):

| Situation | Classification |
|---|---|
| Unmatched (`catalog_name is None`) and `saturated=True` | Suppressed — `return None`, no anomaly record at all (bright-star/subtraction artifact, not a real transient; see docs/ISSUES.md #1, #2) |
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
| `track` | `ASTEROID`, `COMET`, `MOVING_UNKNOWN`, `SPACE_DEBRIS` | One background image (the most recent epoch's own frame) with a small marker at every epoch's true position + connecting line, in chronological order. Every epoch's (RA, Dec) is converted into the *background* epoch's WCS pixel grid via `WCS.world_to_pixel()` — no pixel-level alignment between frames is needed, only a per-epoch coordinate transform. Each marker's epoch number sits at the end of a short leader line spread evenly around the point cluster's centroid, rather than on top of the marker itself — epochs are often only a few pixels apart (e.g. a slow-moving asteroid on a wide-field frame), and a label stacked directly on the point would both obscure it and collide with neighbouring labels. |
| `stamp_strip` | everything else (`SUPERNOVA_CANDIDATE`, `UNKNOWN`, `VARIABLE_STAR`, `BINARY_STAR`, `KNOWN_CATALOG_NEW`, `FIRST_OBSERVATION`) | One small crop per epoch, centred on that epoch's own detected position using that frame's own WCS, each circled and labelled with its timestamp and magnitude — a "blink" before/after strip for a source that isn't expected to move. |

The single public entry point, `update_charts_for_sources(anomaly_type_by_source_id)`, takes
every (source_id → anomaly_type) pair for one frame at once (see pipeline.py Step 15), so it can
fetch every source's track and upload every chart in one HTTP round trip each, regardless of how
many anomalies the frame has.

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
5. `api_client.upload_source_charts_batch(charts)` → `POST /sources/charts/batch` — every
   successfully rendered chart for this frame uploaded in one call (PNGs travel base64-encoded
   inside the JSON body, since a raw-bytes request body can only ever carry one image), replacing
   any previous chart for each of those sources.

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
(`/sources/near/batch`, `/frames/covering/batch`, `/sources/tracks/batch`, `/sources/charts/batch`),
the client still implements/exports their older single-position/single-source counterparts
(`/sources/near`, `/frames/covering`, `/sources/{id}/track`, `/sources/{id}/chart`) — kept for
API completeness, no longer called from this codebase.

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
Gaia and Simbad queries for a given sky region should be cached locally (simple dict or Redis)
within a pipeline run to avoid redundant network calls when multiple sources fall in the same
catalog tile. Cache TTL: 1 hour.

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
