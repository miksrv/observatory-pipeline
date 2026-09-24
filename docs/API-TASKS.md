# API-TASKS.md — Tasks deferred to the `observatory-api` repository

Working through [AUDIT-2026-08-18.md](AUDIT-2026-08-18.md) in this (`observatory-pipeline`)
repository surfaces changes that cannot be made here at all, because they belong to the
separate `observatory-api` (CodeIgniter 4 / PHP) repository — a DB migration, an endpoint
contract change, a new wire field, or new server-side validation.

Those are collected here rather than silently skipped. Nothing in this file has been
implemented; it is a queue to be picked up in `observatory-api` later.

Each entry records:
- **Origin** — the audit finding (or other work) that produced it
- **What** — the change required on the API side
- **Why** — what in the pipeline is blocked or degraded without it
- **Pipeline side** — what, if anything, was already done here in the meantime

---

## ✅ DONE — 4. `uncatalogued_only` on `POST /sources/near/batch`

Done in `observatory-api` on 2026-09-23 (branch `develop`, commit `ae891fe`).

**Origin** — `docs/PLAN-OSC-SUPPORT.md` T9: the worker was OOM-killed on the 228-frame NGC 7331
`DETECT_ANOMALIES` run; one frame's history prefetch returned 870 828 rows for 62 353 stored
observations.

**What** — optional `uncatalogued_only` (bool): only observations whose source has
`catalog_name IS NULL OR catalog_name = 'MPC'`. Also: candidates sorted by dec and each position
binary-searches its own ±radius slice before the haversine, instead of positions × candidates.

**Why** — the pipeline's wide-cone moving-object query only ever uses uncatalogued history; on a
well-observed field the unfiltered wide cone is almost entirely stars.

**Pipeline side** — done: `_prefetch.py` sends the flag on the wide query. Measured on the latest
NGC 7331 frame: 870 828 rows / 2.27 GB RSS / 38 s before; 51 981 + 12 rows / 284 MB / 0.8 s after.

## 3. Settings seed: `STAR_FWHM_MIN_PX` and the post-M9 `NARROWBAND_FILTERS`

**Origin** — one-shot-colour support, `docs/PLAN-OSC-SUPPORT.md` T5 (2026-09-23), and the
pre-run check of the local `settings` table the same day.

**What** — in the settings seed migration (`2026-08-10-000001_CreateSettingsTable.php`) and in
a new migration for existing databases:
- rename the row `STAR_FWHM_MIN_ARCSEC` (value `2.5`) to `STAR_FWHM_MIN_PX`, value `1.2`,
  description "Minimum star FWHM in pixels — sharper sources are hot pixels or cosmic rays";
- set `NARROWBAND_FILTERS` to
  `Ha,OIII,SII,NII,LeNhance,LeXtreme,LuLtimate,NBZ,QuadBand,TriBand,DuoBand`.
Also update the settings list in the API's own docs if it names either.

**Why** — a remote setting overrides the pipeline's default. `STAR_FWHM_MIN_ARCSEC` no longer
exists in the pipeline, so the old row is silently ignored and the new parameter can't be tuned
remotely until it has a row. The old `NARROWBAND_FILTERS` value undoes audit finding M9 on
every deployment: a frame through an L-eNhance or other multi-band filter is held to the
broadband star floor and calibrated against Gaia.

**Pipeline side** — done: `config.py` has `STAR_FWHM_MIN_PX` (default 1.2) and accepts it from
`GET /settings`; `STAR_FWHM_MIN_ARCSEC` is gone. The local development database's
`NARROWBAND_FILTERS` row was corrected by hand on 2026-09-23.

## ✅ DONE — 2. `SPACE_DEBRIS` must not be an alert

Done in `observatory-api` on 2026-09-22 (branch `develop`, commit `3fc4087`).

**Origin** — post-remediation test run, 2026-09-22: 38 `SPACE_DEBRIS` anomalies on 7 IC3322A
frames, every one an ordinary satellite pass, all flagged `is_alert=1` alongside the 12 genuine
alerts.

**What** — remove `'SPACE_DEBRIS'` from `AnomalyModel::ALERT_TYPES`
(`app/Models/AnomalyModel.php`). `FramesController::saveAnomalies` derives the persisted
`is_alert` from that list, so nothing the pipeline sends can change it.

**Why** — the type exists so that a genuine fast mover's single-exposure track is never erased
(audit finding H16) and so that trails don't land in `UNKNOWN`; it is bookkeeping, not
something an operator has to act on. With it in the alert set, alerts on a night with a few
passes are mostly trails.

**Pipeline side** — done: `modules/anomaly_detector/types.py` no longer lists it in
`_ALERT_TYPES`, so it is logged at INFO and not counted as an alert; the classification itself
is unchanged.

## ✅ DONE — 1. Frame coverage misses the frame's own corners

Done in `observatory-api` on 2026-09-22 (branch `develop`, commit `700c4ff`): the coverage radius
is the frame's half-diagonal (`SkyMath::coverageRadiusArcsec()`), from `width_px`/`height_px` when
known and the square-frame worst case otherwise.

**Origin** — post-remediation test run, 2026-09-22 (63 IC3322A frames): 10 of 11 `UNKNOWN`
alerts carried the note "no prior coverage in the API history" at positions that earlier frames
had detected sources at.

**What** — `GET /frames/covering` and `POST /frames/covering/batch`
(`Api\V1\FramesController`) count a frame as covering a point when the point lies within
`fov_deg / 2` of `(ra_center, dec_center)`. `fov_deg` is the frame's **longest axis**
(docs/API.md §1), so that circle is the frame's inscribed circle along its long side and never
reaches the corners: on a 4656×3520 frame at 0.78″/px the corners lie up to ~0.63° from the
centre while the test radius is 0.50°. Every point in the corner regions is therefore reported
"never covered" however many frames imaged it. Options, in order of preference:
- store `width_px`/`height_px`/`position_angle_deg` (already on `frames`) and test the point
  against the actual rotated rectangle;
- or, as a cheap superset, test against the half-**diagonal** instead — the frame's
  circumscribed circle. That errs toward "covered", which is the safe direction for this
  check: a false "covered" can at worst turn a `FIRST_OBSERVATION` note into an
  `UNKNOWN`/`KNOWN_CATALOG_NEW` classification that the other evidence still has to support,
  whereas a false "not covered" sends every image-subtraction candidate in a corner straight to
  the no-coverage `UNKNOWN` alert branch, bypassing the history check.

**Why** — `modules/anomaly_detector/_classify.py`'s no-coverage branch alerts `UNKNOWN` for any
uncatalogued subtraction candidate without consulting history, on the premise that the area has
genuinely never been imaged. The corner regions break that premise on every frame.

**Pipeline side** — mitigated, not fixed: an uncatalogued subtraction candidate lying within
`SUBTRACTION_RESIDUAL_RADIUS_FWHM` × its own FWHM of a catalogued star of the same brightness is now
suppressed as that star's residual, which removed all 10 observed cases. A genuine corner
transient still reaches the no-coverage branch rather than the history-aware one until this is
fixed.
