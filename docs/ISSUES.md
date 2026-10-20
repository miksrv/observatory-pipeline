# ISSUES.md — open questions and deferred follow-ups

Open items found on real data or in review and consciously deferred. A fixed item is deleted —
`git log` is its history. Numbers are never reused, so a gap means a closed item.

Not here: things that cannot be done in this repository (`docs/API-TASKS.md`), new capabilities
under discussion (`ROADMAP.md`), and the two limitations in `CLAUDE.md` "Known issues" (faint
`UNKNOWN` sources; `QC_SNR_MIN` not enforced), which stay there because that file is read every
session.

---

## 1. Too many `MOVING_UNKNOWN` / `UNKNOWN` anomalies

**Observation (2026-08-05):** the two types dominated `anomalies.anomaly_type`. The remaining
known cause is the missing magnitude threshold in the `UNKNOWN` branch (`CLAUDE.md` Known
issue #1).

**Still open:**
- [ ] After a deployment with real traffic, compare the share of `UNKNOWN`/`MOVING_UNKNOWN`
      before and after (`SELECT anomaly_type, COUNT(*) FROM anomalies GROUP BY anomaly_type`).
      Never done; the test campaigns since (IC3322A, NGC 7331) were on truncated databases.
- [ ] Decide on the magnitude threshold / `FAINT_UNCATALOGUED` type (needs the API enum too).

## 3. → observatory-api — orphaned finder charts after re-running `DETECT_ANOMALIES`

Re-running detection replaces the frame's anomaly set (`POST /frames/{id}/anomalies`), but a
chart already uploaded for a `source_id` whose anomaly no longer reproduces is neither deleted
nor regenerated — chart generation is operator-triggered and only touches sources that still
have an anomaly. The stale chart stays in observatory-api until cleaned up there. Every
classifier fix that removes previously valid anomalies (the Gaia-magnitude zero point, the
two-epoch confirmation of a magnitude change, the `SPACE_DEBRIS` edge threshold) leaves such orphans.

- [ ] Decide whether the API should drop charts of sources with no remaining anomaly when a
      frame's anomaly set is replaced, or offer a cleanup endpoint. Belongs in
      `docs/API-TASKS.md` once decided; recorded here because the symptom is pipeline-visible.

## 5. Recent transients: TNS cross-match, Simbad transient types

**Observation (NGC 7331 run, 2026-09-23):** SN 2026aaiv (discovered 2026-09-01, ~12.3 mag,
29″ from the nucleus) was matched to Simbad's "SN 2014C" 3.5″ away and reported only as
`VARIABLE_STAR`; with today's zero point it produces nothing. A transient present since the field's
first frame has no "before", "near a galaxy" is the 5″ match cone, and a Simbad supernova
gives the source a permanent, unremarkable stellar identity although a 2014 supernova is far
fainter than 12 mag today.

- [ ] **Simbad transient object types** (`SN*`, `SN?`, `Nova`, `No?`, …) must not claim a
      source as its stellar identity in `catalog_matcher`; the source continues to
      Gaia/2MASS/Pan-STARRS and, if none claims it, is treated as uncatalogued. Can go first;
      without it SN 2026aaiv becomes a silent `KNOWN_CATALOG_NEW` once older frames provide
      coverage, with it one `UNKNOWN` alert on the first 2026 frame.
- [ ] **TNS cross-match** (Transient Name Server; Simbad lags it by weeks to months): cone
      query per frame, cached like the other catalogs; a detection at a TNS position gets its
      name and type and is reported as a known transient. Needs a TNS bot key
      (`TNS_BOT_ID` / `TNS_BOT_NAME` / `TNS_API_KEY`, skipped while unset) — **the operator
      creates it**; read the TNS rate limits before coding.
- [ ] Deferred: "near a galaxy" from the galaxy's own extent (Simbad major/minor axes) instead
      of the 5″ cone — noisier (HII regions and knots in the disk), a separate task.

## 7. Per-instrument photometry and history

Frames come from several telescopes and cameras, but two things are keyed by filter alone:

- [ ] `PHOTOMETRY_COLOR_TERMS` — the colour term belongs to the instrument + filter, not the
      filter. Needs a key such as `{TELESCOP}/{INSTRUME}/{filter}` and the per-frame fit that
      `photometry.py` already logs as calibration data for it.
- [ ] Light-curve history in the Δmag branches is same-filter but instrument-agnostic; two
      cameras' `L` are not the same bandpass. Either carry the instrument into the history
      filter (API joins it) or accept the scatter it adds to `VARIABILITY_SIGMA`.

## 8. Streak → asteroid identity should check the trail length

A `_streak=True` detection matched to an MPC object within `MOVING_CONE_ARCSEC` becomes
`ASTEROID`; nothing checks that the trail's length is what SkyBot's rate × exposure predicts.
A satellite trail crossing an asteroid's position inherits its name.

- [ ] Require trail length ≈ rate × `EXPTIME` (within a tolerance) before accepting the
      identity; otherwise keep it `SPACE_DEBRIS`.

## 9. Diffuse subtraction residuals

Candidates with FWHM ≫ the frame's PSF (cloud edges, gradients, galaxy-core misregistration)
pass the sharpness screen (which only rejects *sharper* than `psf/1.5`) and reach the anomaly
detector. Reviewed by eye on the charts so far.

- [ ] Add an upper FWHM bound for subtraction candidates (a multiple of the frame PSF), or
      route them to the real/bogus scorer once `docs/PLAN-real-bogus.md` lands.

## 10. `FORCED_PHOTOMETRY_MAG_LIMIT` too deep for short exposures

The default (20 mag) is below what a 300 s frame on the test telescope reaches; forced
photometry then measures sky at thousands of Gaia positions, and the ones that pass
`FORCED_PHOTOMETRY_MIN_SNR` by chance feed one-epoch `VARIABLE_STAR` triggers (now suppressed
by `VARIABILITY_CONFIRM_EPOCHS`). A site setting today.

- [ ] Derive the limit per frame from the frame's own depth (e.g. the faintest blind detection
      at `PHOTOMETRY_MIN_SNR`, plus a margin) instead of a fixed magnitude.

## 11. Edge-vignetting `VARIABLE_STAR` triggers on unflattened frames

After the zero-point and confirmation fixes, the remaining false `VARIABLE_STAR` on NGC 7331 were stars near the frame edge on
unflattened frames: fainter by vignetting in one epoch, normal in another where they sat nearer
the centre. The confirmation rule does not help when several epochs share the same pointing.

- [ ] Options: a radial zero-point term (fit against Gaia per frame, like the colour term), or
      excluding `near_edge` detections from the Δmag branches. Not decided.

## 12. Catalog cache never expires on disk

`modules/catalog_matcher/_cache.py` honours `CACHE_TTL_HOURS` on read but never deletes an
expired file, and the MPC/SkyBot key includes the exact epoch, so every frame writes a file that
is never read again (137 files for one NGC 7331 night). Old-format and expired files were
deleted by hand on 2026-09-23.

- [ ] Sweep expired files at startup or on write (cheap: the TTL is in the file's
      `fetched_at`), and consider rounding the MPC epoch key to a few minutes.

## 13. Variable/binary Simbad OTYPE tests are substring matches

`_is_galaxy()` became an exact set from Simbad's `otypedef` table after a red giant (`RG*`) was
posted as `SUPERNOVA_CANDIDATE` (2026-10-20, lookatstars NGC 7129). Checked against the same
table, `_is_variable_star()` / `_is_binary_star()` (`modules/anomaly_detector/_otypes.py`)
have the same shape of problem: `HV*` (high-velocity star) reads as variable, the galaxies
`LSB`/`SBG` read as binary, and `Ce*`, `El*`, `Ro*`, `Pu*`, `Er*`, `Ir*`, `No*` and the X-ray
binaries (`XB*`, `LXB`, `HXB`) are missed — `Cep`/`Ell` never match a current code. A missed
type still reaches the light-curve `VARIABLE_STAR` branch; a wrong one misnames an alert.

- [ ] Exact sets per class, decided code by code from `otypedef` (variability is spread across
  several branches of Simbad's hierarchy, so a subtree rule is not enough).
