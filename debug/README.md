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
