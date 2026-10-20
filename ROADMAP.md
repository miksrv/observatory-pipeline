# ROADMAP.md — proposed future improvements

Ideas under consideration but not yet implemented. Unlike `docs/ISSUES.md` (open
questions and deferred follow-ups), this file tracks *new
capabilities* being discussed for the pipeline. Nothing here is scheduled — an item
moves out of this file once it's actually implemented (see `git log` for that history).

---

## Explicit `chart_type` selection for `GENERATE_CHARTS`

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
