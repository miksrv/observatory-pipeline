# Changelog

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
