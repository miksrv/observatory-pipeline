# ISSUES.md — вопросы по данным в БД `observatory-api`, требующие разбора

Зафиксировано 2026-08-05 по итогам разбора реальных данных в таблице `anomalies`
(и связанной `sources`); дополнено 2026-08-07 по итогам отдельного разбора
(пункты 3 и 4 ниже — написаны на английском, см. правило про язык
markdown-документов в CLAUDE.md). Уже реализованные и протестированные
исправления сюда не переносятся — их история в `git log`; там, где фикс
закрыл только часть пункта, здесь остаётся только реально открытый хвост.
Обновлено 2026-08-11.

---

## 1. Аномально много записей `MOVING_UNKNOWN` и `UNKNOWN`

**Наблюдение:** в `anomalies.anomaly_type` доминируют `MOVING_UNKNOWN` и `UNKNOWN`.

**Разбор:**
- Отсутствие порога по магнитуде в `UNKNOWN`-ветке — уже задокументированный
  Known Issues #1 в CLAUDE.md (`modules/anomaly_detector.py`, TODO ~строки 541–546).
  Это единственная из трёх причин ниже, которая **до сих пор не устранена**.
- `MOVING_CONE_ARCSEC=120″` — широкий конус, из-за которого почти любой мелкий шум
  около предыдущей позиции детекции трактовался как «сдвиг» → `MOVING_UNKNOWN`.
  Устранено `_is_position_shifted()` (требует, чтобы старая позиция реально
  «опустела»), уже в коде.
- Мусорные кандидаты из `modules/subtraction.py` возле ярких/насыщенных звёзд,
  не матчащиеся ни в один каталог. Устранено saturation-флагом +
  near_edge-подавлением (`modules/anomaly_detector.py`), уже в коде.

**Что осталось сделать** (требует доступа к БД на проде — вне возможностей
этой сессии, поскольку `observatory-pipeline` не имеет прямого доступа к БД
по архитектуре — см. CLAUDE.md "Why pipeline → API, not pipeline → DB
directly"):
- [ ] После деплоя проверить `SELECT anomaly_type, COUNT(*) FROM anomalies GROUP BY anomaly_type;`
      и сравнить долю `UNKNOWN`/`MOVING_UNKNOWN` до/после фиксов
- [ ] Решить, реализовывать ли порог по магнитуде / `FAINT_UNCATALOGUED`
      (Known Issues #1 в CLAUDE.md) — решение до сих пор не принято

---

## 2. Frequent `0 calibrated` frames (open sub-question from the extreme-`magnitude` investigation)

**Background:** the original observation — `anomalies`/`sources` rows with impossibly
bright `magnitude` (e.g. −13.8722 and brighter than the full Moon) — has been root-caused
and fixed in code: `astrometry.py` flags saturated sources so `photometry.py` never
measures them, and `pipeline.py` now sets `mag = None` for an uncalibrated source instead
of falling back to raw `mag_instrumental`. That part is closed.

**Still open:** the test run that surfaced this (Vesta, 5 frames) also showed 2 of those
5 frames coming back `0 calibrated` from `photometry.py` (`zero_point=None` — fewer than
3 Gaia DR3 matches in the field), a much higher failure rate than expected and never
investigated further:
- Is `fov_deg` too narrow around Vesta specifically?
- Is `MATCH_CONE_ARCSEC` too strict for the Gaia cross-match?
- Or is that patch of sky genuinely Gaia-sparse?

**What's left to do:**
- [ ] Investigate why Gaia DR3 zero-point calibration fails so often on these test
      frames — check the three hypotheses above against real data
- [ ] File a separate GitHub Issue for this if it needs formal tracking (not created yet)

---

## 3. Orphaned finder charts after re-running `DETECT_ANOMALIES` (residual from the coma/edge-elongation fix)

**Background:** the original observation — 4 `T_CrB` frames producing 305 anomalies,
almost all false `SPACE_DEBRIS` from coma-elongated corner stars — has been root-caused
and fixed: `modules/anomaly_detector.py`'s `SPACE_DEBRIS` shortcut now uses a higher,
edge-aware elongation threshold (`SPACE_DEBRIS_EDGE_ELONGATION_MIN`) for sources flagged
`near_edge`. That part is closed.

**Still open (known limitation of the fix):** re-running `DETECT_ANOMALIES` for an
already-processed frame replaces that frame's anomaly set via
`POST /frames/{id}/anomalies` (a previously false `SPACE_DEBRIS` row disappears), but a
finder chart already uploaded for that `source_id` is **not** deleted or regenerated —
`POST /sources/{id}/chart` only touches `source_id`s that still have a resolved anomaly
in the new run (see `modules/finder_chart.py` / `worker.py`'s `GENERATE_CHARTS`
batching). A stale chart for a now-non-anomalous source stays orphaned in
`observatory-api` until cleaned up there directly.

**What's left to do:**
- [ ] Re-run `DETECT_ANOMALIES` on the 4 `T_CrB` frames and compare the anomaly count
      before/after
- [ ] Manually clean up orphaned charts in `observatory-api` for source_ids whose
      `SPACE_DEBRIS`/`MOVING_UNKNOWN` anomaly no longer reproduces after this fix
- [ ] Decide whether `observatory-api` needs an automated cleanup mechanism for orphaned
      charts, so this doesn't require manual intervention every time a classifier fix
      removes previously-valid anomalies

---

## 4. Possible density-dependent WCS-offset correction failure on star-rich frames

**Observation (2026-08-07):** on frames with a large number of detected sources
(~1000), the sky position reported for a source — and consequently the link generated
to the Aladin portal — appears slightly off from the actual star. On sparser frames
(~300 sources) the same kind of link lands precisely on the star.

**Hypothesis (not yet verified against logs — reasoned from code, not confirmed by a real
repro):** `modules/catalog_matcher.py::_compute_wcs_offset()` receives the frame's full
`sources_all` list (loose filter — every detection, not just strict stars) together with
Gaia DR3 stars for the same field, then:
1. Runs a quick nearest-neighbour check and exits early with no correction
   (`return 0.0, 0.0`) whenever `median_sep <= 10.0` arcsec — treating the WCS as
   "already accurate".
2. Otherwise falls back to an all-pairs vote accumulator (`search_around_sky` +
   histogram peak) to estimate a systematic offset.

On a star-dense field, both `sources_all` and the Gaia catalog for that sky region are
correspondingly dense, so the chance nearest-neighbour separation between an arbitrary
source and an unrelated, nearby-but-wrong Gaia star can be small purely from geometry —
independent of whether a genuine systematic WCS offset (typical for astap, up to ~30″)
is actually present. This could make step 1's `median_sep <= 10.0` shortcut fire
incorrectly on crowded fields, silently skipping the offset correction that a sparser
field of the same real WCS quality would correctly apply. The vote accumulator's
background-pair count (`expected_bg`) also scales with source density, which could weaken
its significance test in the same regime.

This is **not** an issue with `modules/astrometry.py`'s astap invocation — astap receives
only the raw FITS file and does its own internal star detection/matching; it never sees
`sources`/`sources_all`, so limiting the sep-detected source count would not change
astap's own WCS solve at all. Any fix belongs in `catalog_matcher.py`'s offset-correction
logic, not in source extraction, and catalog matching itself should keep using the full
source list (`sources_all`) as it does today.

**What's left to do:**
- [ ] Compare pipeline logs for a real dense (~1000 sources) frame vs. a sparse
      (~300 sources) frame of similar depth: check whether `"Gaia match (raw): min=...
      median=..."` and `"WCS offset detected: dRA=... dDec=..."` show the correction
      being skipped on the dense frame but applied on the sparse one
- [ ] If confirmed, make the `median_sep <= 10.0` early-exit density-aware (or drop it
      and always rely on the vote accumulator) instead of capping the number of sources
      fed into `_compute_wcs_offset()`
- [ ] Re-verify the vote accumulator's significance test (`MIN_PEAK_VOTES`/`SIGMA_MARGIN`)
      still holds up as source/Gaia density increases
