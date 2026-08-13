# ROADMAP.md — proposed future improvements

Ideas under consideration but not yet implemented. Unlike `docs/ISSUES.md` (open
data-quality questions about data already in production), this file tracks *new
capabilities* being discussed for the pipeline. Nothing here is scheduled — an item
moves out of this file once it's actually implemented (see `git log` for that history).

---

## 1. Rejected frames: register with the API, and reconcile QC verdicts after a threshold change

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

---

## 2. Explicit `chart_type` selection for `GENERATE_CHARTS`

**Idea:** today `modules/finder_chart/`'s style for a given chart request is entirely
*inferred*, never chosen by the caller: `_style._style_for_source(anomaly_type, n_epochs)`
picks `before_after` (exactly 1 loaded epoch, regardless of `anomaly_type`), else `track`
(if `anomaly_type` is one of `MOVING_TYPES`) or `stamp_strip` (everything else). On top of
whichever static style gets picked, `track`/`stamp_strip` *also* always get an automatic
`track_gif`/`stamp_strip_gif` companion whenever `config.CHART_GIF_ENABLED` is true — there
is no way to ask for one without the other on a single request; the only lever is the
global config flag, which affects every source and every call at once. Concretely, today a
`GENERATE_CHARTS` item for an `ASTEROID` anomaly with 2+ epochs unconditionally produces
**two** uploaded chart artifacts (`track` + `track_gif`) — and the task item's own
DONE/FAILED status only ever reflects the static PNG's outcome; the GIF's own success or
failure is invisible to the caller, only logged (see `_render_charts_for_source()`'s
docstring in `__init__.py`).

**Proposed design:**

- A new, optional `payload.chart_type` field on a `GENERATE_CHARTS` task item — one of
  `track`, `stamp_strip`, `before_after`, `track_gif`, `stamp_strip_gif` — read by
  `worker.py`'s `_run_charts_task()` alongside the existing `payload.anomaly_type`. When
  present, it takes precedence over the current `anomaly_type`-based inference; when
  absent, behavior is unchanged (backward compatible with `AnomaliesController::createTask()`,
  which doesn't know about this field yet).
- When an explicit `chart_type` is given, render/upload **only that one artifact** — no
  automatic bundling of the static+GIF pair the way `CHART_GIF_ENABLED` does today. This
  gives per-request control over whether a GIF gets generated at all, independent of the
  global flag.
- **Downgrade rule** (already agreed, not yet implemented): if the requested `chart_type`
  needs 2+ epochs (`track`/`stamp_strip`/either `*_gif`) but the source only has 1 loaded
  epoch, silently fall back to `before_after` — the only thing renderable from a single
  point — but surface this in the result (e.g. `{"ok": bool, "style": ..., "requested":
  ...}` instead of a bare `bool`), so a caller can tell it got something different from
  what it asked for.
- `pipeline.generate_charts_for_source_ids()` / `finder_chart.update_charts_for_sources()`
  need their `anomaly_types_by_source_id: dict[str, list[Optional[str]]]` parameter
  generalized to carry a `chart_type` alongside each `anomaly_type` entry (e.g. a list of
  small `{"anomaly_type": ..., "chart_type": ...}` dicts instead of bare strings) — see
  `_style._group_types_by_style()`, which would need to honor an explicit `chart_type` per
  entry ahead of its own inference.

**Explicitly out of scope for this proposal:** `modules/catalog_preview.py` /
`PREVIEW_CATALOG_MATCH` already picks its own two variants (raw QC-reject preview vs.
catalog-match-circled preview) automatically from the frame's own QC status — no forced
choice needed there, confirmed while this idea was being discussed.

**Open considerations, deferred rather than decided:**
- `observatory-api` doesn't send this field from anywhere yet — `AnomaliesController::createTask()`
  and `/ui/sources/generate-charts` would both need a UI control and a schema/payload change
  to actually let an operator pick a `chart_type`; that's a separate, cross-repo change.
- Exact shape of the "downgraded" result — whether `worker.py` surfaces it as a `note` on
  `POST /tasks/{id}/items/progress`, or only via logs — not decided (see this proposal's
  discussion history for the two options considered).
- Whether an explicit `chart_type` of `track_gif`/`stamp_strip_gif` should still also try
  the static sibling (so at least *something* is retrievable if the GIF path is the one
  actually wanted for display), or genuinely produce only the one requested artifact as
  currently proposed — leaning toward "only the one requested", not decided.

**Status:** proposed, not implemented. No code changes yet.
