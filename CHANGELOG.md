# Changelog

## 1.4.1

### Patch Changes

- Finder charts: a source with more than `CHART_MAX_EPOCHS` epochs shows that many spread evenly in time over its whole history (first and last always included), instead of only the most recent ones
- Subtraction: an unsolved (QC-failed) archived frame is screened by its header plate scale (`XPIXSZ` + `FOCALLEN`) — it used to count as "scale unknown", so the QC fallback could stack another telescope's frames into the reference
- 8-bit frames (`BITPIX=8`): saturation is detected at the file's own ceiling (255) instead of `SATURATION_ADU`, photometry stays uncalibrated (a stretched export's magnitudes put faint stars ~1 mag too bright), and subtraction never mixes 8-bit and raw frames in one reference stack — re-analyze existing 8-bit frames (and re-run anomaly detection on their objects) after deploying
## 1.4.0

### Minor Changes

- API: every request carries the pipeline's version (`X-Pipeline-Version`, from the top `CHANGELOG.md` heading), shown beside the pipeline's name on the API dashboard; the worker and watcher log it at startup (shown by observatory-api 1.3.0; older APIs ignore it)

### Patch Changes

- Analysis: cross-catalog duplicate removal takes milliseconds instead of 3–8 minutes per frame (same result)
- Subtraction: reference frames whose plate scale differs from the new frame's by more than `SUBTRACTION_REF_MAX_SCALE_DIFF` (default 5%) are no longer stacked — INDI writes the mount's name into `TELESCOP`, so two telescopes on one mount with the same camera used to be mixed
- Anomalies: a source is "near a galaxy" only for Simbad's galaxy object types (exact list) — red giants (`RG*`), globular clusters, nebulae and other types merely containing the letter G no longer raise `SUPERNOVA_CANDIDATE`; Seyfert, LINER, radio and low-surface-brightness galaxies now do; a fading `LSB`/`SBG` galaxy is no longer reported as `BINARY_STAR`

## 1.3.0

### Minor Changes

- Tasks: pipeline reports an intermediate RUNNING status for a task item right before processing starts, so the operator UI can show which file is currently being worked on instead of only queued/done; an item left RUNNING is redone like a PENDING one (requires observatory-api 1.2.0)

## 1.2.0

### Minor Changes

- Archive: Lights filed by object and filter (`{object}/{filter}/`, `NONE` without a filter), calibration under `_calibration/{night}/{type}/` with the camera instead of the object in the filename — laid out to serve as a read-only FTP root; run `tools/migrate_archive_layout.py` once after deploying (see README "File Organization")
- Subtraction: reference frames only from the same telescope and camera (`TELESCOP`/`INSTRUME`), with no fallback to other instruments

### Patch Changes

- Catalog preview: subtraction looks for references under the normalized object name and filter, as ingest archives them
- Subtraction: a re-analysed frame no longer costs its own reference stack a slot
- Frame deletion: a file already in `rejected` is no longer overwritten
- Photometry: when catalog matching corrects a systematic WCS offset, the solved WCS is shifted too — apertures, forced photometry and the archived header no longer sit off the stars by the correction
- Anomaly detection: an API outage while fetching history, coverage or a frame's sources no longer replaces the frame's stored anomalies with an empty or wrong set; detection is re-queued (history) or the item fails (sources)
- Catalog matching: an asteroid that takes over a catalogued star no longer becomes a zero-point reference with that star's Gaia magnitude
- Catalog matching: the WCS offset is found in fields straddling RA 0h; corrected RA stays within 0–360°
- Catalog cache: `CACHE_TTL_HOURS` from the API settings takes effect; an entry read from disk expires with its file; a malformed SkyBot answer is no longer cached
- Photometry: saturated stars are dropped before the reference quality screen, so a frame is no longer left uncalibrated when enough usable stars exist
- Astrometry: saturation is judged from the raw peak pixel instead of the peak plus the global sky, so saturated cores on a bright local background are flagged
- Photometry: pixel scale is the geometric mean of both axes everywhere, as in astrometry
- Anomaly detection: a near-edge subtraction candidate is judged on its difference-image SNR, not on an aperture SNR that photometry may not have measured
- Worker: an item report the API did not take is retried, and the task is re-queued instead of staying `RUNNING` (failed after `API_RECOVERY_MAX_ATTEMPTS` re-queues); an unexpected API response no longer crashes the worker with a task claimed
- Worker: a second `Ctrl+C`/SIGTERM exits at once even during a long CPU step; a terminal `Ctrl+C` no longer kills astap mid-solve
- Watcher: a restart no longer resubmits files that queued ANALYZE tasks already carry; a stop during the startup scan is handled; up to 1 minute to submit buffered files on stop
- Worker: graceful shutdown — on `docker compose stop`/`down` the item in progress finishes and an interrupted task goes back to the queue instead of staying `RUNNING`; the worker gets up to 5 minutes to stop
- Watcher: on `docker compose stop`/`down` the buffered files are submitted before exiting instead of being dropped by Docker's kill
- Docs: `.env.example` and README show the real `QC_SKY_BACKGROUND_MAX` default (50000)

## 1.1.0

### Minor Changes

- Logging: `watcher.log` and `worker.log` written to `HOST_DATA_DIR/logs` with size rotation (`LOG_DIR`, `LOG_FILE_MAX_MB`, `LOG_FILE_BACKUP_COUNT`), surviving container restarts and recreates; uncaught crashes are logged there too
- Docker: the containers' own json logs capped at 3 × 10 MB per service

### Patch Changes

- Catalog matching: a failed MPC/SkyBot query is retried (`SKYBOT_MAX_ATTEMPTS`, `SKYBOT_RETRY_DELAY_SEC`) with astroquery's HTTP cache off, so an intermittent malformed IMCCE answer no longer costs a frame its asteroid matching

## 1.0.1

### Patch Changes

- Docker: host data paths set by one `HOST_DATA_DIR` in `.env` (`/data` on Linux servers, `~/Developer/data` by default) — `docker-compose.yml` no longer edited per host
- Docker: `astap` archives for both amd64 and arm64 bundled, the right one picked automatically at build time
- Docker: `.env` and local tooling kept out of the image via `.dockerignore`

## 1.0.0

### Major Changes

- Watcher: picks up new FITS frames from the incoming folder and queues them for analysis in batches
- Worker: runs tasks from the observatory-api queue: analysis, anomaly detection, finder charts, catalog-match preview, frame deletion, restart
- Ingest: normalised file names and archive layout; calibration frames archived; Bayer colour frames converted to mono; stacked frames rejected
- Quality control: FWHM, elongation, sky background and star count; failed frames are still registered and archived with the reason
- Astrometry: offline plate solving with ASTAP, source extraction with satellite-trail masking
- Image subtraction: new frames differenced against a stack of archived frames of the same object
- Catalogs: cross-match with Simbad, Gaia DR3, 2MASS, Pan-STARRS and MPC/SkyBot, plus forced photometry at catalog positions
- Photometry: aperture photometry calibrated against Gaia, with per-filter colour terms
- Anomalies: asteroids, comets, supernova candidates, variable and binary stars, moving unknowns and space debris, with JPL Horizons ephemerides
- Finder charts: track, blink-strip and before/after charts with animated GIFs, uploaded to the API
- Settings: configuration fetched from the API at startup; failed API uploads are re-queued automatically
