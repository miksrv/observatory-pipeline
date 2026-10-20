# API-TASKS.md — Tasks deferred to the `observatory-api` repository

Changes found while working in this (`observatory-pipeline`) repository that cannot be made here,
because they belong to the separate `observatory-api` (CodeIgniter 4 / PHP) repository — a DB
migration, an endpoint contract change, a new wire field, or new server-side validation.

A queue to be picked up in `observatory-api`. A done entry is deleted — `git log` in either
repository is its history. Numbers are never reused, so a gap means a done entry.

Each entry records:
- **Origin** — the audit finding (or other work) that produced it
- **What** — the change required on the API side
- **Why** — what in the pipeline is blocked or degraded without it
- **Pipeline side** — what, if anything, was already done here in the meantime

---

## 3. Settings seed: `STAR_FWHM_MIN_PX` and the post-M9 `NARROWBAND_FILTERS`

**Origin** — one-shot-colour support (the hot-pixel floor moved from arcsec to pixels,
2026-09-23), and the pre-run check of the local `settings` table the same day.

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

## 5. Register calibration frames

**Origin** — the archive layout (2026-10-16): the pipeline files Lights under
`FITS_ARCHIVE/{object}/{filter}/` and calibration under `_calibration/{night}/{type}/`, and the
archive is the root of a public read-only FTP share.

**What** — calibration frames are never registered (`POST /frames` is not called for
Dark/Flat/Bias), so the site — the archive's only index — cannot show `_calibration/`. Proposal:
accept `frame_type != Light` without `ra_center`/`dec_center`, with a minimal payload (type,
camera, exposure, binning, temperature, filter for Flats, night, filename), so the site can list
"calibration available for this night". No other contract change: `frames.filename` stays a
basename, resolved to a path by the pipeline (`modules/archive_layout.py`).

**Why** — without it `_calibration/` is browse-only over FTP.

**Pipeline side** — nothing yet: a `post_frame()` call in `analyze_frame()`'s calibration branch
once the API accepts it.
