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

## 1. Frame coverage misses the frame's own corners

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
`SUBTRACTION_RESIDUAL_RADIUS_ARCSEC` of a catalogued star of the same brightness is now
suppressed as that star's residual, which removed all 10 observed cases. A genuine corner
transient still reaches the no-coverage branch rather than the history-aware one until this is
fixed.
