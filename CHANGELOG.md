# Changelog

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
