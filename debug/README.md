# debug/ — pipeline algorithm test tools

This directory holds standalone scripts for visually testing individual
pipeline stages against real FITS frames, without touching the API, the
archive, or any other pipeline side effects. These are developer tools, not
part of the production pipeline (`pipeline.py` never imports anything from
here).

## debug_catalog_match.py

Runs a single FITS frame through the real pipeline steps up to catalog
matching — `qc.analyze()` → `astrometry.solve()` → `subtraction.run()` →
`catalog_matcher.match()` — using the actual production code from
`modules/`, with no API calls and no file moves. It then renders every
detected source (`sources_all`, the loose filter that `catalog_matcher`/
`anomaly_detector` actually operate on) as a circle on top of the frame's
own pixel data:

- **Green circle + label** `CatalogName:id` — the source matched a catalog
  (Simbad / Gaia DR3 / 2MASS / Pan-STARRS / MPC).
- **Red circle, no label** — the source did not match any catalog.

Circles are drawn at each source's *originally detected* (RA, Dec) — i.e.
before `catalog_matcher.match()`'s WCS-offset correction shifts
`source["ra"]`/`["dec"]` in place for cross-matching. This keeps circles
aligned with what's actually visible on this frame's own pixel grid rather
than the Gaia-corrected sky position.

### Usage

```bash
# From inside the pipeline container (needs astap, sep, astropy, etc.):
python debug/debug_catalog_match.py <path-to-fits> [output.png]
```

If `output.png` is omitted, the image is saved to
`debug/output/<fits-filename-stem>.png` (the directory is created
automatically). `debug/output/` is gitignored — treat it as scratch space.

### What to look at

- **A field where most stars are red** — the WCS-offset correction
  (`catalog_matcher._compute_wcs_offset()`) likely failed to detect or fully
  correct a real astrometric offset for that frame; check the printed
  `WCS offset` log lines (run with `logging.basicConfig(level=logging.INFO)`
  if you need the detail) for the raw/corrected median separation.
- **Green circles that don't sit on a visible star** — either the offset
  correction converged on a wrong value, or (less likely) a genuine
  catalog-alignment issue independent of this frame's WCS.
- **Printed QC line** — `quality_flag`, `fwhm`, `elongation`, `stars`,
  `snr_median`, `sky_background`. Useful for sanity-checking why a frame
  was or wasn't rejected before it ever reaches astrometry.

### Background

Built 2026-08-06 while investigating a false single-epoch "new source"
anomaly on the `Vesta_A807_FA` test object: a real, catalogued star was
appearing as an unmatched, uncatalogued detection on one frame in its
history. This tool made it possible to see directly that ~32 of 40 detected
sources on that frame matched no catalog at all, despite thousands of Gaia
stars being available in that sky region — which led to finding and fixing
a real bug in `catalog_matcher._compute_wcs_offset()`'s significance test
(see git history / CLAUDE.md for the fix itself). Kept around as a
general-purpose tool for testing catalog-matching behaviour on any FITS
frame going forward, not just that one incident.

Also on 2026-08-06: this script's own "no file moves" promise turned out to
be false for any frame that fails QC. `qc.analyze()` moves a rejected frame
to `FITS_REJECTED` *itself*, internally, before ever returning to its
caller — this script called it exactly like `pipeline.py` does and so
inherited that side effect, silently relocating two `HIGH_BACKGROUND` test
frames out of `debug/` and into `/fits/rejected/{object}/` (not deleted,
just moved — but still a real violation of what this tool is for). Fixed by
adding `qc.analyze(fits_path, move_on_reject=False)`; this script now always
passes that. If you add a new stage here that calls another module
function shared with `pipeline.py`, check whether it has a similar hidden
side effect before assuming "read-only" from the docstring alone.

## debug_anomaly_charts.py

Renders debug PNGs for anomalies already sitting in the test database — a
different kind of tool than `debug_catalog_match.py`: it doesn't run any
pipeline stage, it reads back what a real pipeline run already produced and
visualizes it more usefully than one flat frame per anomaly row.

It connects **directly to the test `observatory-api` database** (a
deliberate, scoped exception to "the pipeline has no direct DB access" — see
CLAUDE.md; this is a read-only developer tool, not `pipeline.py`) and to the
local FITS archive. No API calls, no file moves.

Anomalies are grouped by `source_id` — the same cross-frame identity
`pipeline.py`'s dedup step and `anomaly_detector.py` use — instead of
rendering one image per row:

- **2+ frames for the same `source_id`** → the exact same rendering
  `modules/finder_chart.py` uploads to the API: a `track` mosaic (one
  background frame, every epoch's position marked and connected, with a
  small RA/Dec legend under the image keyed by marker number) for moving
  types (`ASTEROID`/`COMET`/`MOVING_UNKNOWN`/`SPACE_DEBRIS`), or a
  `stamp_strip` (one crop per epoch, "blink" style, each captioned with its
  own RA/Dec) for everything else.
- **Exactly one frame for that `source_id`** (or an anomaly with
  `source_id = NULL`, which has no cross-frame identity to group on at all)
  → a 2-panel **BEFORE/AFTER** mosaic instead: a crop at the anomaly's exact
  sky position from the most recent *earlier* frame of the same object
  (nothing expected there), next to a crop from the anomaly's own frame
  (circled). Both panels are captioned with the anomaly's RA/Dec. Falls
  back to a single labelled panel, with an explicit note why, if no earlier
  frame exists at all or it can't be loaded locally.

Every chart's title also carries the source's catalog designation, when it
has one — one extra query against the `sources` table (`catalog_name`/
`catalog_id`), e.g. `ASTEROID (Vesta)` instead of a bare `ASTEROID`. Absent
entirely for an uncatalogued source (e.g. most `MOVING_UNKNOWN`/`UNKNOWN`
anomalies), rather than showing an empty parenthesis.

### Usage

```bash
# From inside the pipeline container (needs matplotlib, astropy, pymysql —
# pymysql was added to requirements.txt for this tool specifically, so a
# rebuild is needed once: `docker compose build pipeline`):
python debug/debug_anomaly_charts.py <object_name> [--out-dir debug/output]

# e.g.
python debug/debug_anomaly_charts.py Vesta_A807_FA
```

`object_name` must match `frames.object` exactly (the normalized directory
name, e.g. `Vesta_A807_FA`), not the raw FITS `OBJECT` header value.

Database connection defaults to this repo's local test-DB setup (MariaDB on
`host.docker.internal:3306`, matching `docker-compose.yml`'s
`extra_hosts: host.docker.internal:host-gateway` and observatory-api's own
`.env` test credentials) and can be overridden with `DEBUG_DB_HOST` /
`DEBUG_DB_PORT` / `DEBUG_DB_NAME` / `DEBUG_DB_USER` / `DEBUG_DB_PASSWORD`.
These are deliberately separate env vars from `config.py` — adding DB
credentials there would misrepresent the production pipeline as having DB
access, which it doesn't.

### Background

Built 2026-08-06 after running the pipeline over 5 `Vesta_A807_FA` test
frames and finding the resulting `anomalies` rows hard to sanity-check one
at a time — e.g. anomaly `6a7514b504c7e0.85372499` (`MOVING_UNKNOWN`, a
single-frame detection with no other row sharing its `source_id`) rendered
as one flat, uninformative frame with no way to see at a glance that the
position was empty a moment earlier. The BEFORE/AFTER mosaic and the
source_id grouping (reusing `modules/finder_chart.py`'s own rendering for
the multi-frame case) were both added directly in response to that.

One side effect worth knowing about while reading the output: on this same
`Vesta_A807_FA` test set, the *same* physical asteroid ends up split across
two different `source_id`s partway through its 5-frame run (it drifted far
enough between frames 3 and 4 that whatever positional matching
`observatory-api` uses to fold a new, uncatalogued detection into an
existing `sources` row no longer considered them the same source). This
script doesn't try to resolve that — it groups strictly by `source_id`, as
stored — so you'll see it as two separate mosaics for what is actually one
continuous track. That's a real characteristic of the current data worth
being aware of when reading the charts, not a bug in this tool.

Follow-up the same day: the mosaics didn't show *where* each crop actually
was, and a catalog-matched object (e.g. Vesta itself, MPC-matched) still
showed up as a bare "ASTEROID" with no way to tell it apart from any other
asteroid at a glance. Both fixed by adding RA/Dec captions (every panel in
the BEFORE/AFTER mosaic, every stamp in a `stamp_strip`, a small legend
under a `track` image) and a catalog-designation lookup shown next to
anomaly_type — the RA/Dec and designation rendering itself now lives in
`modules/finder_chart.py`, not duplicated here, so production finder charts
(uploaded via the API) show the same information, not just this debug tool's
output. See that module's docstring and pipeline.py's finder-chart step for
how `designation_by_source_id` gets built from the already catalog-matched
`sources` list.

Second follow-up the same day, both found by inspecting the actual rendered
output against `Vesta_A807_FA`:

- **The designation lookup above (`sources`.`catalog_id`) can show the
  wrong name.** `sources.id` is resolved positionally by the API, so a
  moving object that happens to pass near an already-catalogued star's
  position can get folded into that SAME `sources` row — whose
  `catalog_name`/`catalog_id` then reflects whichever detection updated it
  last (possibly the star, from a different frame entirely), not this
  anomaly. Confirmed on this exact data set: anomaly
  `6a7514b504c7f9.00535350` has `mpc_designation = "2014 RY1"`, but its
  `source_id`'s `sources` row carried `catalog_name = "Gaia DR3"` from an
  unrelated star sharing that row — a naive lookup rendered
  `ASTEROID (3971465931154563840)` instead of `ASTEROID (2014 RY1)`. Fixed
  by preferring each anomaly's own `mpc_designation` (captured once, at
  classification time, from the exact detection it's about) over the
  `sources`-table lookup, which is now only a fallback for anomaly types
  that don't carry an `mpc_designation` field at all. See
  `resolve_designation()` and `fetch_catalog_designations()`'s docstring —
  and `pipeline.py`'s own version of the same fix, at the finder-chart step.
- **A `track` mosaic for closely-spaced epochs looked uninformative** (the
  `Vesta_A807_FA__..._93390255__ASTEROID.png` case) — the chart drew the
  *entire* multi-thousand-pixel-wide frame scaled down to figure size, so a
  slow mover's few dozen pixels of drift between epochs was invisible; the
  numbered markers ended up on top of each other. `modules/finder_chart.py`'s
  `_render_track_chart()` now crops to the epoch cluster (with margin, or a
  generous fixed context window for a very tight cluster) instead of the
  whole frame — the same object's motion is now clearly visible at a glance,
  and a genuinely wide multi-epoch trail still renders in full since the
  crop grows with the cluster's own footprint. This is a production
  `modules/finder_chart.py` fix, not a debug-tool-only one — it affects the
  same charts uploaded via the API.

Third follow-up: the BEFORE/AFTER mosaic showed the SAME RA/Dec repeated
under both panels with no visible shift, which read as a bug ("shouldn't
each panel have its own position?"). It's not — a single-occurrence
anomaly has exactly one detected position, and the entire point of the
BEFORE/AFTER comparison is "was anything at this *one, fixed* sky position
before vs. after" (the "before" crop is centred on the *same* point the
anomaly was later found at, precisely so the comparison means something —
using two different positions would compare two unrelated patches of sky).
Both panels being centred crops around that one point is also why there's
never a visible pixel shift, regardless of any real pointing/dither
difference between the two frames — cropping always re-centres on the
query point. The repeated number was genuinely confusing, though, so it now
appears once, in the figure's overall title, explicitly labelled "same in
both panels, by design" — see `render_before_after()`'s docstring.

(For contrast: a `track` mosaic's per-epoch legend — the OTHER case with
multiple RA/Dec values in one image — genuinely differs number to number,
because a multi-occurrence group there is a moving object detected at
different positions on different frames. Same tool, two different reasons
a coordinate does or doesn't change within one image; worth keeping straight
when reading either style's output.)
