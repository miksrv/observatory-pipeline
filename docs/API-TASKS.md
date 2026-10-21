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
