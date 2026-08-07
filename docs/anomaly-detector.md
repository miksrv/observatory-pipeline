# Anomaly Detector Mechanics (`modules/anomaly_detector.py`)

This document explains in detail how `modules/anomaly_detector.py` works — the
pipeline's central "science" component, which compares sources detected in a frame
against observation history and classifies them into anomaly types.

For the overall pipeline context and the full module list, see
[../CLAUDE.md](../CLAUDE.md) and [../README.md](../README.md).

---

## Entry point

```python
await anomaly_detector.detect(frame_id, sources, catalog_matches, frame_meta) -> list[dict]
```

Called from `pipeline.py` after `photometry.measure()`, and after
`api_client.post_sources()` has returned `source_ids` for each source (see Steps 12–13
in `CLAUDE.md`). The input source list has already been through:

- plate solving and source extraction (`astrometry.py`, sources flagged `saturated=True`
  when their peak ADU reaches `SATURATION_ADU`),
- optional image subtraction (`subtraction.py`, candidates flagged `_from_subtraction=True`;
  the vicinity of any saturated pixel is masked out of diff-image detection before
  candidates are even produced — see docs/ISSUES.md #1, #2),
- catalog cross-matching (`catalog_matcher.py`: Simbad, Gaia DR3, 2MASS, Pan-STARRS DR1, MPC),
- photometry (`photometry.py`, `mag` field, set by `pipeline.py` to `mag_calibrated` when the
  source was calibrated, `None` otherwise — never the raw `mag_instrumental`, which has no
  absolute zero-point; see docs/ISSUES.md #2).

The output is a list of anomaly dicts ready to be sent to `POST /frames/{id}/anomalies`.
`FIRST_OBSERVATION` and `KNOWN_CATALOG_NEW` are **never included** in this list — they
are only logged (`logger.debug`), since they are not an actionable signal.

---

## Batch API prefetch

A key architectural feature: instead of one HTTP request per source (`O(N)`), the
module makes **exactly two** batch requests for the entire frame (`O(1)`):

1. `POST /sources/near/batch` — historical sources near every position.
2. `POST /frames/covering/batch` — which positions were already covered by earlier frames.

Both requests group source coordinates into `0.1°×0.1°` tiles (`_tile_key()`, ~6
arcminutes) so nearby sources reuse the same result instead of issuing duplicate
queries. Both requests run concurrently via `asyncio.gather`.

Important: **history is fetched for every source without exception**, including
already catalog-matched ones — it's needed not only to detect position shifts (movers)
but also to compute `delta_mag` for already-known variable/binary stars and galaxies.
(An earlier revision skipped this for catalog-matched sources, which made
`VARIABLE_STAR`/`BINARY_STAR`/the "brightening" branch of `SUPERNOVA_CANDIDATE`
permanently unreachable — fixed; see git history if you need the details.)

If both batch requests come back empty, it's either the very first frame of this sky
area, or an API failure; a warning is logged.

---

## Process diagram

```mermaid
flowchart TD
    Start(["detect(frame_id, sources,\ncatalog_matches, frame_meta)"]) --> Empty{"sources\nempty?"}
    Empty -- yes --> ReturnEmpty(["return []"])
    Empty -- no --> Prefetch["_prefetch_history_data()\n2 batch requests in parallel:\nPOST /sources/near/batch\nPOST /frames/covering/batch\n(grouped into 0.1° tiles)"]

    Prefetch --> Loop["for each source:\n_classify_source_sync()"]

    Loop --> P1{"catalog_name\n== 'MPC'?"}
    P1 -- yes --> Asteroid["ASTEROID / COMET\n(by object_type)\n⚑ _needs_ephemeris=True"]

    P1 -- no --> Sat{"catalog_name is None\nAND saturated == True?"}
    Sat -- yes --> Suppressed(["suppressed — return None\n(bright-star/subtraction artifact,\nnot a real transient)"])
    Sat -- no --> P2{"catalog_name is None\nAND no history within MATCH_CONE_ARCSEC\nof the CURRENT position\nAND a wide-cone (MOVING_CONE_ARCSEC) historical\nposition has vacated\n(no source in THIS frame near it)?"}
    P2 -- yes --> Elong{"elongation > 3.0?"}
    Elong -- yes --> SpaceDebris["🔔 SPACE_DEBRIS"]
    Elong -- no --> MovingUnknown["🔔 MOVING_UNKNOWN"]

    P2 -- no --> Coverage{"n_coverage == 0?\n(sky area never\nimaged before)"}
    Coverage -- yes --> FromSub{"_from_subtraction\n== True?"}
    FromSub -- no --> FirstObs["FIRST_OBSERVATION\n(not an anomaly, logged only)"]
    FromSub -- yes --> UnkNewArea["🔔 UNKNOWN\n(subtraction already confirmed\nnovelty despite\nno coverage record)"]

    Coverage -- no, area\nwas covered --> History{"n_history == 0?\n(no prior detection\nwithin MATCH_CONE_ARCSEC)"}

    History -- yes --> Galaxy{"near a Simbad galaxy?\n(_is_galaxy)"}
    Galaxy -- yes --> SNnew["🔔 SUPERNOVA_CANDIDATE\n(new point source,\nno baseline to compare)"]
    Galaxy -- no --> InCatalog{"catalog_name\nis not None?"}
    InCatalog -- no --> UnkNoCat["🔔 UNKNOWN\n(not found in\nany catalog)"]
    InCatalog -- yes --> KnownNew["KNOWN_CATALOG_NEW\n(known object, was simply\nbelow detection threshold\nbefore; not an anomaly)"]

    History -- no, has\nhistory --> DeltaMag["delta_mag = mag − median(history.mag)"]
    DeltaMag --> Changed{"abs(delta_mag) >\nDELTA_MAG_ALERT?"}
    Changed -- no --> NoAnomaly(["no anomaly"])
    Changed -- yes --> Bright{"delta_mag < 0\n(got brighter)\nAND near a galaxy?"}
    Bright -- yes --> SNbright["🔔 SUPERNOVA_CANDIDATE\n(already-known host\nbrightened)"]
    Bright -- no --> Binary{"Simbad object_type\nindicates a\ndouble/eclipsing binary?"}
    Binary -- yes --> BinaryStar["BINARY_STAR"]
    Binary -- no --> Variable{"Simbad object_type\nindicates a\nvariable star?"}
    Variable -- yes --> VarStar["VARIABLE_STAR"]
    Variable -- no --> NoAnomaly

    Asteroid --> Collect["build anomaly dict\n(source_id, ra, dec, mag,\ndelta_mag, notes, ...)"]
    SpaceDebris --> Collect
    MovingUnknown --> Collect
    UnkNewArea --> Collect
    SNnew --> Collect
    UnkNoCat --> Collect
    SNbright --> Collect
    BinaryStar --> Collect
    VarStar --> Collect

    Collect --> Ephem["_resolve_ephemerides()\nfor everything flagged\n_needs_ephemeris=True:\nparallel ephemeris.query()\nvia JPL Horizons"]
    Ephem --> Return(["return list[dict]\nanomalies (excluding FIRST_OBSERVATION\nand KNOWN_CATALOG_NEW)"])

    FirstObs -.-> LogOnly(["logged only, not\nin the output list"])
    KnownNew -.-> LogOnly
    NoAnomaly -.-> LogOnly2(["None — excluded\nfrom the output list"])
    Suppressed -.-> LogOnly2
```

🔔 — the type is a member of `_ALERT_TYPES` and is logged with `logger.warning`
("ALERT") rather than `logger.info`/`logger.debug`.

---

## Classification priority (important!)

Branches are checked in a fixed order inside `_classify_source_sync()` — as soon as
one condition matches, the function returns and no further checks run:

1. **MPC match** — `catalog_name == "MPC"` → `ASTEROID`/`COMET`, regardless of history
   or coverage.
2. **Saturated, unmatched source** — `catalog_name is None` and `saturated == True` →
   suppressed outright (`return None`, no anomaly record). A saturated star leaves
   large `astroalign` residual artifacts around it even under near-perfect
   registration (see `modules/subtraction.py`'s saturation masking), and an
   uncatalogued detection sitting on top of one is overwhelmingly that artifact, not
   a real transient or mover — see docs/ISSUES.md #1, #2. This check runs *before*
   the position-shift check below, so a saturated artifact can never become
   `MOVING_UNKNOWN`/`SPACE_DEBRIS` either. Scoped to `catalog_name is None` only: a
   saturated source that *is* MPC- or Simbad-matched (bullet 1, or the history-based
   branches below) is a legitimate detection and is unaffected — it just never gets a
   usable `magnitude`, since `photometry.py` never measures a saturated source.
3. **Unmatched position-shifted source** — `catalog_name is None` and **both**: (a) no
   historical detection within `MATCH_CONE_ARCSEC` (5″) of the source's *current*
   position, and (b) a historical detection within the wider `MOVING_CONE_ARCSEC`
   (120″) whose own position is no longer occupied by anything else in *this* frame
   (`_is_still_occupied()` is `False` for it) → `MOVING_UNKNOWN` (elongation ≤ 3.0) or
   `SPACE_DEBRIS` (elongation > 3.0, fast trail).

   Condition (b) alone (an earlier revision's entire check) is true near almost any
   populated field regardless of real motion: `MOVING_CONE_ARCSEC` covers enough sky
   that some unrelated historical detection — a neighbouring star, a galaxy smudge,
   anything ever recorded nearby — is virtually always present there. That made
   ordinary sub-arcsecond centroid/seeing noise on an otherwise-static source
   indistinguishable from a real mover, as long as *anything else* happened to be
   within 120″ (real incident, 2026-08-06 — see docs/ISSUES.md #1). Condition (a)
   requires the source's *own* current position to be genuinely new, and the
   "vacated" check in (b) requires the *candidate's* old position to have genuinely
   emptied out — a persistent neighbour that's still detected at its own spot in this
   frame no longer counts as evidence that anything moved.
4. **Stationary sources** — decided next by coverage (`coverage`) and local history
   (`history`, `MATCH_CONE_ARCSEC` cone):
   - no coverage at all → `FIRST_OBSERVATION` (not an anomaly), except when
     `_from_subtraction=True` → then `UNKNOWN` (subtraction already proved novelty at
     the pixel level, so missing API coverage doesn't override that);
   - covered, no history, near a galaxy (Simbad OTYPE) → `SUPERNOVA_CANDIDATE`;
   - covered, no history, not in any catalog → `UNKNOWN`;
   - covered, no history, in a catalog (not a galaxy) → `KNOWN_CATALOG_NEW` (not an anomaly);
   - has history → compare `delta_mag = mag − median(history.mag)` against
     `DELTA_MAG_ALERT` (default 0.5). If the threshold is exceeded:
     - a change near a galaxy only counts when the source **brightened**, i.e.
       `delta_mag < 0` → `SUPERNOVA_CANDIDATE` (checked first — takes priority over
       binary/variable stars);
     - otherwise, if Simbad classifies the object as a double/eclipsing binary
       (`**`, `EB`, `SB`) → `BINARY_STAR`;
     - otherwise, if Simbad classifies the object as a variable
       (`V*`, `RR`, `Cep`, `BY`, `RS`, `Ell`, `bL`) → `VARIABLE_STAR`;
     - otherwise — no anomaly.

Table of Simbad OTYPE substrings used by the classifiers:

| Function | OTYPE substrings |
|---|---|
| `_is_variable_star` | `V*`, `RR`, `Cep`, `BY`, `RS`, `Ell`, `bL` |
| `_is_binary_star` | `**`, `EB`, `SB` |
| `_is_galaxy` | `G`, `SFG`, `AGN`, `GiG` |

---

## Anomaly type table

| `anomaly_type` | When assigned | Alert? |
|---|---|---|
| `FIRST_OBSERVATION` | Sky area never observed before | No (logged only, not returned) |
| `KNOWN_CATALOG_NEW` | Not in history, but found in a catalog | No (logged only, not returned) |
| `VARIABLE_STAR` | Has history, Δmag > `DELTA_MAG_ALERT`, Simbad variable | No (logged) |
| `BINARY_STAR` | Has history, Δmag > `DELTA_MAG_ALERT`, Simbad binary | No (logged) |
| `ASTEROID` | Matched in MPC/SkyBot, type "asteroid" | No (logged + ephemeris) |
| `COMET` | Matched in MPC/SkyBot, type "comet" | No (logged + ephemeris) |
| `SUPERNOVA_CANDIDATE` | New point source near a galaxy with no history, **or** an already-known galaxy brightening beyond Δmag | **Yes** |
| `MOVING_UNKNOWN` | Position-shifted source, not in MPC, elongation ≤ 3.0 | **Yes** |
| `SPACE_DEBRIS` | Position-shifted source, not in MPC, elongation > 3.0 | **Yes** |
| `UNKNOWN` | New source outside any catalog in a covered area, or detected via image subtraction regardless of coverage | **Yes** |

The list is fixed as `AnomalyType(str, Enum)` and must match
`AnomalyModel::ALLOWED_TYPES`/the `ENUM` constraint on the `observatory-api` side — the
two lists are kept in sync by hand (see `CLAUDE.md`, `modules/anomaly_detector.py` section).

---

## Ephemeris resolution

After all sources are classified, `_resolve_ephemerides()` concurrently
(`asyncio.gather`) calls `ephemeris.query(designation, obs_time)` for every anomaly
flagged `_needs_ephemeris=True` (i.e. only `ASTEROID`/`COMET`) and fills in the
`ephemeris` field. The internal `_needs_ephemeris` sentinel is removed from the final
dicts before they're returned.

---

## Returned item format

```python
{
    "anomaly_type":    AnomalyType,       # see table above
    "source_id":       str | None,        # sources.id, from _source_id (see pipeline.py Step 12)
    "ra":              float,
    "dec":             float,
    "magnitude":       float | None,
    "delta_mag":       float | None,      # None for MPC / moving sources
    "mpc_designation": str | None,
    "ephemeris":       dict | None,       # filled by _resolve_ephemerides()
    "notes":           str,               # human-readable explanation
}
```

---

## Error handling

- A failure in the batch history/coverage prefetch (`_prefetch_history_data`) is
  caught entirely — classification continues with empty dicts (every source falls
  through to `FIRST_OBSERVATION`/no anomaly rather than crashing the frame).
- A failure classifying a single source (`_classify_source_sync`) is caught at the
  loop level in `detect()` — the rest of the frame's sources are still processed normally.

---

## Known limitations

- There is no magnitude threshold for `UNKNOWN` — very faint sources (mag > 20) that
  aren't even in Pan-STARRS DR1 are still unconditionally flagged `UNKNOWN`. A
  `FAINT_UNCATALOGUED` classification is not implemented yet (see Known Issues #1 in
  [../CLAUDE.md](../CLAUDE.md)).
- Both "galaxy" branches (`SUPERNOVA_CANDIDATE`) use the same `MATCH_CONE_ARCSEC`
  radius (5″ by default) — there is no separate, wider radius for extended galaxy disks.
- The saturated-artifact suppression (priority 2 above) only ever returns `None` for a
  matching source — it does not by itself confirm how much of the historically observed
  `UNKNOWN`/`MOVING_UNKNOWN` volume was actually caused by saturation artifacts, since
  that requires comparing anomaly counts before/after the fix against the deployed
  database (see docs/ISSUES.md #1's remaining checklist). If a large volume of
  `UNKNOWN` persists after deploying this fix and the `MOVING_CONE_ARCSEC` fix below,
  the more likely remaining cause is the missing magnitude threshold discussed above.

Resolved issues in this module (history not queried for catalog-matched sources,
`source_id` not propagated to anomalies, etc.) are no longer tracked in the docs — see
`git log -- modules/anomaly_detector.py` for that history.
