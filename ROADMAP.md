# ROADMAP.md — proposed future improvements

Ideas under consideration but not yet implemented. Unlike `docs/ISSUES.md` (open
data-quality questions about data already in production), this file tracks *new
capabilities* being discussed for the pipeline. Nothing here is scheduled — an item
moves out of this file once it's actually implemented (see `git log` for that history).

---

## 1. Forced photometry on catalog/ephemeris positions ("reverse matching")

**Idea:** today, matching only runs forward — detected sources (from `sep` extraction in
`modules/astrometry.py`) are cross-matched against external catalogs in
`modules/catalog_matcher.py`. Add a second, reverse pass: for every catalog star (Gaia
DR3 / 2MASS / Pan-STARRS) and every MPC/SkyBot ephemeris position that falls within the
frame's footprint but has **no** corresponding entry in `sources_all`, measure the flux
at that exact predicted pixel position anyway, instead of silently treating it as "not
detected".

**Why this is a real technique, not just relabeling:** this is standard practice under
the name *forced photometry* (also *known-position photometry*), and for solar-system
objects specifically, *precovery*. Examples: ZTF runs a dedicated forced-photometry
service; the LSST/Rubin DIA pipeline forces photometry on every known `DIAObject` at
every visit regardless of whether blind detection fired that visit; precovery of
asteroids/comets at an ephemeris-predicted position (even below the blind-detection
threshold) is routine MPC practice.

The reason it can recover genuinely fainter objects, not just duplicate blind detection
with extra steps, is the look-elsewhere effect: blind extraction scans every independent
resolution element in the frame, so its detection threshold (`SEP_DETECT_THRESH`) has to
stay high (~5σ) to keep the false-positive rate low across millions of trials. Forced
photometry at an already-known position tests exactly one hypothesis, so a much lower
significance (~2–3σ) is statistically justified there without inflating false positives
frame-wide.

**How it would fit the existing architecture:**
- The full-field catalog star lists are very likely already in memory and don't need a
  new network round-trip: `catalog_matcher.py`'s WCS-offset correction already runs an
  all-pairs vote accumulator against Gaia DR3 for the whole field (a region query, not
  one query per detected source), and 2MASS/Pan-STARRS go through VizieR's
  `query_region()` the same way. The reverse pass would reuse those results rather than
  re-querying any catalog.
- New work per catalog star: `WCS.world_to_pixel()` to get its predicted pixel position
  (applying Gaia's proper motion for the epoch gap, since forced photometry needs a
  precise pixel, unlike cross-matching within `MATCH_CONE_ARCSEC`); skip anything already
  matched to an entry in `sources_all` (to avoid double-reporting the same star); run
  aperture photometry there via the existing primitives in `modules/photometry.py`,
  reusing the frame's already-computed `sep.Background` map.
- The same logic applies to MPC/SkyBot ephemeris positions for known asteroids/comets
  expected in the frame — this is the precovery case specifically.

**Open considerations / risks:**
- Must respect the saturation mask and streak mask (`_build_streak_mask()`) before
  placing an aperture, or it will pick up garbage in regions `subtraction.py` already
  treats as unusable.
- A non-detection (flux consistent with zero within the noise) must be recorded as an
  upper limit, not misread as "the star disappeared" — this needs careful handling before
  it reaches `anomaly_detector.py`.
- Catalog depth needs a cutoff. Gaia DR3 alone is complete to ~21 mag; forcing
  photometry on literally every catalog star in a dense/low-galactic-latitude field could
  mean thousands of positions per frame, almost all of them uninformative noise. A
  reasonable bound: only force photometry for catalog stars within a couple of magnitudes
  of this frame's own estimated limiting magnitude (derivable from `qc.py`'s
  `sky_background`/`fwhm_median`/`snr_median`), not down to the catalog's absolute limit.

**Expected processing-time impact:** no new external API calls (catalog data is already
fetched for forward matching), so the added cost is purely local: a vectorized
world-to-pixel transform plus vectorized aperture photometry, both cheap per position.
The dominant variable is how many extra positions get measured — for a sparse field the
overhead should be well under a second; for a dense field with no depth cutoff it could
reach several seconds, which is why the magnitude cutoff above matters. Even in the dense
case this stays well below the cost of plate solving (~2–5s via `astap`) or a live
external catalog query.

**Payoff for `modules/anomaly_detector.py`:** currently, a known catalog star that dims
below the blind-extraction threshold simply vanishes from this frame's source list with
no record at all. Forced photometry turns that silence into an actual measured
flux/magnitude with an error bar, making it possible to detect and report "known star
significantly fainter than its catalog magnitude" — the signature of an eclipsing binary
at minimum, or a fading event — which the blind-detection-only pipeline currently cannot
see at all.

**Status:** proposed, not implemented. No code changes yet.

---

## 2. Rejected frames: register with the API, and reconcile QC verdicts after a threshold change

**Idea:** today a frame that fails `qc.analyze()` is moved to
`/fits/rejected/{object}/{FLAG}_filename.fits` and never reaches the API at all —
`pipeline.py` returns before any `api_client` call once `quality_flag != "OK"`. Two
related gaps follow from that:

1. There is no visibility into rejected frames from the API/website side — an operator
   has to look at the observatory server's filesystem directly to see rejection rates or
   reasons, even though `qc.analyze()` already computes `fwhm_median`,
   `elongation_median`, `sky_background`, `star_count`, `snr_median` for every rejected
   frame; that data is simply discarded today.
2. QC thresholds (`QC_FWHM_MAX_ARCSEC`, `QC_ELONGATION_MAX`, `QC_SKY_BACKGROUND_MAX`,
   `QC_STARS_MIN`) are inherently site/instrument-specific and get retuned during
   commissioning or after a change in conditions. Retuning has no way to reach
   *already-processed* frames: a frame accepted under an old threshold that would now
   fail stays accepted forever, and a frame rejected under an old (too strict) threshold
   stays rejected forever, even once the threshold is loosened.

**Proposed design:**

- **Always register the frame**, even when QC rejects it: call `api_client.post_frame()`
  with `quality_flag` (one of `OK`/`BLUR`/`TRAIL`/`HIGH_BACKGROUND`/`LOW_STARS`/`BAD`) and
  the QC metrics above, plus the relative path the file was moved to. `sources=[]`, no
  WCS — a rejected frame still never runs astrometry/photometry/anomaly detection, that
  part of the pipeline is unchanged. The move to
  `/fits/rejected/{object}/{FLAG}_filename.fits` still happens exactly as today; only the
  "no API call" half of the current behavior changes.
- **A new task type, `RECHECK_QC`** (item: full path to a file, same "full path, not a
  basename" convention as `ANALYZE`), lets an operator re-run just the QC step against
  the *current* config and reconcile the file's location/API record with the new
  verdict:

  | Was | Now | Action |
  |---|---|---|
  | rejected | rejected (same or different flag) | Update `quality_flag` on the existing API record; rename the file's QC prefix if the flag changed |
  | rejected | OK | **Promote** — literally re-run the existing `pipeline.analyze_frame(path)` unchanged. It re-derives the normalized filename from the FITS headers on its own (as it always does when archiving), so whatever name/prefix the file currently has under `/fits/rejected/...` is irrelevant — no separate "strip the QC prefix" logic is needed, and no fix to today's rejected-file naming is needed either. The only API-side requirement is that `POST /frames` (or a dedicated update endpoint) upserts into the *same* frame record instead of creating a duplicate, keyed by a natural identity (filename with any QC prefix stripped, object, DATE-OBS) |
  | accepted | accepted | No-op |
  | accepted | not OK | **Demote** — move the file to `/fits/rejected/{object}/{FLAG}_filename.fits` (its filename is already normalized from the original archive step, so this is a pure prefix-add, no rename needed), then call a new atomic API endpoint, `POST /frames/{id}/reject`, which **hard-deletes** that frame's `sources`/`anomalies`/charts in one DB transaction |
  | Dark/Flat/Bias | — | Skipped — calibration frames never go through `qc.analyze()` at all (see `pipeline.py` Step 3), so there is no QC verdict to reconcile |

- **Demote uses hard delete, not a soft-delete/tombstone flag** — simpler, at the accepted
  cost that any external link to a since-deleted `anomaly_id`/chart (e.g. already shared
  from the website) will 404 without a trace.
- **Stale downstream classifications are not recomputed automatically.** Demoting a frame
  removes its sources from the position/magnitude history that neighboring frames of the
  *same object* may have already used for `VARIABLE_STAR`/`BINARY_STAR`/the brightening
  branch of `SUPERNOVA_CANDIDATE`, or for the coverage check behind
  `FIRST_OBSERVATION`/`UNKNOWN`. Recomputing this is not automatic — deliberately, to
  match the existing precedent that there's no automatic follow-up task creation between
  `DETECT_ANOMALIES` and `GENERATE_CHARTS` (see `worker.py`'s section in CLAUDE.md) — an
  operator who demotes a frame is expected to separately create a bulk
  `DETECT_ANOMALIES` task covering every other frame of that object (`GET
  /frames?object=...`, already-documented endpoint), the same mechanism the job-queue
  module split was built to support. That recompute can in turn leave orphaned finder
  charts, exactly the already-open limitation in `docs/ISSUES.md` #3 — not solved further
  by this proposal. Recompute granularity is deliberately object-level, not positional —
  there is no API query for "which frames have sources near frame X's footprint", and
  building one for this alone isn't judged worth it.

**Open considerations, deferred rather than decided:**
- Whether a rejected frame's quicklook chart should be persisted on the frame record
  itself (a new `POST /frames/{id}/chart`) or left to the existing ad hoc
  `PREVIEW_CATALOG_MATCH` task (which already works unmodified against a path under
  `/fits/rejected/...`, since it takes any path and never moves/registers anything) —
  leaning toward the latter, not decided.
- How an operator goes from "I changed `QC_FWHM_MAX_ARCSEC`" to the actual list of files
  to feed into a bulk `RECHECK_QC` task — needs a small helper that walks
  `FITS_ARCHIVE`/`FITS_REJECTED`, not designed yet.
- Whether `_resolve_bare_filename()` should be extended to also search `FITS_REJECTED`
  (today it only searches `FITS_ARCHIVE`), so `RECHECK_QC`/`ANALYZE` items can use a bare
  basename for a rejected file too — minor, not required.
- Exact shapes of the two required `observatory-api` additions (upsert semantics on frame
  registration; the new `POST /frames/{id}/reject` cascade-delete endpoint) belong to
  that repository's schema/migration design, not this one.

**Status:** proposed, not implemented. No code changes yet.
