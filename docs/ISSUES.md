# ISSUES.md — вопросы по данным в БД `observatory-api`, требующие разбора

Зафиксировано 2026-08-05 по итогам разбора реальных данных в таблице `anomalies`
(и связанной `sources`). Пункты ниже требуют доведения до конца — детали и
ссылки на код см. в разборе. (Пункт 4 добавлен 2026-08-07 по итогам отдельного
разбора и написан на английском — см. правило про язык markdown-документов в
CLAUDE.md. Пункт 5 добавлен 2026-08-07.) Уже реализованные и протестированные
исправления сюда не переносятся — их история в `git log`; здесь остаются
только реально открытые пункты.

---

## 1. Аномально много записей `MOVING_UNKNOWN` и `UNKNOWN`

**Наблюдение:** в `anomalies.anomaly_type` доминируют `MOVING_UNKNOWN` и `UNKNOWN`.

**Разбор:**
- Отсутствие порога по магнитуде в `UNKNOWN`-ветке — уже задокументированный
  Known Issues #1 в CLAUDE.md (`modules/anomaly_detector.py`, TODO ~строки 541–546).
- `MOVING_CONE_ARCSEC=120″` — широкий конус, из-за которого почти любой мелкий шум
  около предыдущей позиции детекции трактуется как «сдвиг» → `MOVING_UNKNOWN`.
- Подозрение: значительная доля этих записей — не реальные объекты, а мусорные
  кандидаты из `modules/subtraction.py` возле ярких/насыщенных звёзд (см. п.2) —
  они не матчатся ни в один каталог и попадают в `UNKNOWN`/`MOVING_UNKNOWN`, хотя
  физически являются остаточными артефактами выравнивания (`astroalign`), а не
  транзиентами.

**Что осталось сделать** (требует доступа к БД на проде — вне возможностей
этой сессии, поскольку `observatory-pipeline` не имеет прямого доступа к БД
по архитектуре — см. CLAUDE.md "Why pipeline → API, not pipeline → DB
directly"):
- [ ] После деплоя проверить `SELECT anomaly_type, COUNT(*) FROM anomalies GROUP BY anomaly_type;`
      и сравнить долю `UNKNOWN`/`MOVING_UNKNOWN` до/после обоих фиксов (насыщение +
      `_is_position_shifted()`)
- [ ] Если доля `UNKNOWN` всё ещё велика — единственная оставшаяся объяснённая
      причина это отсутствие порога по магнитуде (Known Issues #1 в CLAUDE.md);
      тюнинг сознательно не тронут в этом заходе, т.к. требует данных с прода,
      а не только разбора кода
- [ ] Решить, реализовывать ли порог по магнитуде / `FAINT_UNCATALOGUED` (Known Issues #1)

---

## 2. Экстремально яркие `magnitude` (например −13.8722 и ниже)

**Наблюдение:** в `anomalies`/`sources` встречаются `magnitude` ярче полной Луны
(~−12.7) — физически невозможно для точечного источника на этих кадрах.

**Разбор (проверено по коду, не гипотеза):**
- Формула фотометрии в `modules/photometry.py` корректна:
  ```python
  mag_instrumental = -2.5 * log10(net_flux)
  mag_calibrated  = mag_instrumental + zero_point   # zero_point = median(cat_mag - inst_mag)
  ```
  Знака / путаницы `log10(flux/flux_ref)` нет, `zero_point` берётся из Gaia G-band
  корректно. **Это не баг в арифметике.**
- Настоящая причина: `modules/astrometry.py` сознательно оставляет насыщенные/очень
  яркие звёзды в `sources_all` (ради ловли астероидов), но **нигде в цепочке
  `astrometry → subtraction → photometry` нет проверки на насыщение сенсора**.
  Апертура на насыщенной звезде захватывает огромный `net_flux` →
  `-2.5·log10(net_flux)` легитимно даёт −14…−16. Единственный существующий флаг —
  `edge_flag` (близость к краю кадра); флага сатурации не существует.
- Дополнительный канал: `modules/subtraction.py` не маскирует область вокруг ярких/
  насыщенных звёзд перед `sep.extract()` на разностном изображении — остаточный
  сигнал от неидеального `astroalign` там тоже даёт экстремальную magnitude при
  последующем измерении фотометрии на реальном кадре.

**Обновление (2026-08-06, после реального прогона на пересобранном контейнере):**
saturation-фикс выше — реальный и нужный, но живой прогон на тестовых FITS
(Vesta, 5 кадров) показал, что он покрывает **не главную** причину. В
`anomalies` появилось 110 строк, из них 53 — с magnitude от −10.4 до −15.7.
Разбор по `frame_id`:

```
frame_id                    cnt   min_mag   max_mag
6a74e84d216b85.65585507       4    12.5173   16.3266   ← нормально
6a74e86e55ff05.42727773       9    12.3322   16.6787   ← нормально
6a74e8d15c6599.22127971      44    11.7111   18.7713   ← нормально
6a74e89982ddd4.01278675      31   -15.2624  -13.3135   ← ВСЕ отрицательные
6a74e911e13831.02521433      22   -15.7013  -10.3981   ← ВСЕ отрицательные
```

Два «плохих» `frame_id` 1-в-1 совпадают с двумя кадрами, для которых в логах
пайплайна `photometry` написала `0 calibrated  zero_point=None` (в узком поле
вокруг Vesta в этих кадрах Gaia DR3 дала 0 совпадений — `< 3` референсов,
`_compute_zero_point()` не считает zero-point). Проверка на пиксельном уровне
конкретных источников с "-15" подтвердила: рядом нет насыщения (реальный
максимум в апертуре — единицы-десятки тысяч ADU, ниже `SATURATION_ADU`) — это
просто **непрокалиброванный `mag_instrumental`**, который `pipeline.py`
(Step 5.5, до фикса) подставлял в `mag` вместо `mag_calibrated`, когда
`calibrated=False`. `mag_instrumental = -2.5·log10(flux_ADU)` не имеет
абсолютного zero-point и не является настоящей звёздной величиной сама по
себе — у откалиброванных кадров в этом же прогоне `zero_point` был ~21.7–22.4,
то есть тот же самый "-15" после калибровки превратился бы в совершенно
нормальные +7. Это, судя по всему, и есть основная причина исходного примера
"−13.8722" из самого первого наблюдения по этой проблеме — сатурация объясняет
только часть случаев.

**Что осталось сделать:**
- [ ] После деплоя перепроверить в БД: непрокалиброванные кадры теперь должны
      давать `sources`/`anomalies` строки с `magnitude = NULL`, а не
      экстремальным числом (старые строки в БД этим фиксом не исправляются
      retroactively)
- [ ] Отдельно решить, что делать с самим фактом частых `0 calibrated`
      кадров (в тестовом прогоне — 2 из 5!) — возможно, стоит завести отдельный
      issue: почему в поле вокруг Vesta Gaia DR3 не даёт ни одного совпадения
      (слишком узкое `fov_deg`? слишком строгий `MATCH_CONE_ARCSEC` при
      cross-match с Gaia? реально мало каталогизированных звёзд в этом
      конкретном участке неба?) — это отдельный вопрос от «что подставлять в
      mag, если калибровки нет»
- [ ] Завести отдельный GitHub Issue по правилам из CLAUDE.md (What/Why/AC/Notes),
      если требуется формальное отслеживание — не создан в рамках этой сессии

---

## 3. `anomalies.delta_mag` всегда `NULL`

**Назначение поля:** `delta_mag = mag − median(историческая mag)` — разница текущей
яркости источника относительно его истории наблюдений. Используется как триггер
для классификаций `VARIABLE_STAR`, `BINARY_STAR` и «потепление известной галактики»
ветки `SUPERNOVA_CANDIDATE` (порог `DELTA_MAG_ALERT`).

**Разбор:**
- Для 7 из 10 типов anomaly_type (`ASTEROID`, `COMET`, `MOVING_UNKNOWN`,
  `SPACE_DEBRIS`, `UNKNOWN`, «новый источник возле галактики»-ветка
  `SUPERNOVA_CANDIDATE`) поле **по дизайну всегда `None`** — это ожидаемо, раз
  таблица переполнена этими типами (см. п.1).
- Но `delta_mag = NULL` абсолютно во всех строках означает, что записей
  `VARIABLE_STAR` / `BINARY_STAR` / «brightening»-ветки `SUPERNOVA_CANDIDATE` в
  таблице нет вообще. Это в точности баг, который уже исправлен в коде (см.
  `git log -- modules/anomaly_detector.py`): до фикса `_prefetch_history_data()` /
  `_classify_source_sync()` принудительно обнуляли `history=[]` для любого
  catalog-matched источника, а `object_type` (нужен для этих веток) приходит
  только от Simbad — то есть тоже требует catalog-match. Оба условия были
  взаимоисключающими → ветки были физически недостижимы.
- API-сторона (маппинг JSON, колонка `delta_mag FLOAT NULL` в миграции) — без
  проблем, дело не в API.
- **Важно:** `modules/anomaly_detector.py` в рабочей копии сейчас модифицирован,
  но не закоммичен (см. `git status` на момент разбора) — похоже, этот фикс
  существует локально, но не задеплоен на observatory-сервер, поэтому в реальных
  данных API его эффект не виден.

**Что сделать:**
- [ ] Проверить `SELECT anomaly_type, COUNT(*) FROM anomalies GROUP BY anomaly_type;`
      — если `VARIABLE_STAR`/`BINARY_STAR` отсутствуют полностью (count=0),
      гипотеза подтверждена
- [ ] Проверить версию/дату сборки образа pipeline на проде против даты коммита
      фикса (см. `git log -- modules/anomaly_detector.py`)
- [ ] Закоммитить текущие изменения `modules/anomaly_detector.py` и передеплоить
- [ ] После передеплоя перепроверить — `delta_mag` должен начать заполняться для
      реальных переменных/двойных звёзд и повторных вспышек около известных галактик

---

## 4. Coma near the frame edge inflating `SPACE_DEBRIS` false positives

**Observation (2026-08-07):** analyzing 4 new `T_CrB` frames produced 305 anomalies —
almost all false positives. A specific example (finder chart for anomaly
`6a7677e3b0e3a7.00850315`) showed a star at the very edge of the frame, visibly
elongated/blurred by coma — not a real trail or moving object.

**Root cause (verified in code, not a hypothesis):** the `SPACE_DEBRIS` shortcut in
`modules/anomaly_detector.py` (`if not history and elongation > 3.0: SPACE_DEBRIS`) used a
single global elongation threshold with **no awareness of where on the frame the source
actually sits**. Neither `modules/astrometry.py`, `modules/subtraction.py`, nor
`modules/anomaly_detector.py` tracked a source's position relative to the frame edge
anywhere in the pipeline before this fix — the only pre-existing edge-related field,
`edge_flag` in `modules/photometry.py`, is a separate, purely photometric concept (aperture
proximity to the image boundary, hardcoded 10px threshold) that never reaches
`anomaly_detector.py` at all. Coma and other off-axis optical aberrations progressively
stretch an ordinary, non-moving star's PSF toward the edges/corners of a wide-field frame,
inflating its measured `elongation` for purely optical reasons. An uncatalogued star there
(too faint for Gaia/Simbad, or simply outside their match cone) with no prior history at
its position would clear `elongation > 3.0` from coma alone and get reported `SPACE_DEBRIS`
with no real motion involved.

**Known limitation of the fix already shipped for this:** re-running `DETECT_ANOMALIES` for
an already-processed frame replaces that frame's anomaly set via `POST /frames/{id}/anomalies`
(a previously false `SPACE_DEBRIS` row disappears), but a finder chart already uploaded for
that `source_id` is **not** deleted or regenerated — `POST /sources/charts/batch` only touches
`source_id`s that still have a resolved anomaly in the new run (see
`modules/finder_chart.py` / `worker.py`'s `GENERATE_CHARTS` batching). A stale chart for a
now-non-anomalous source stays orphaned in `observatory-api` until cleaned up there directly.

**What's left to do:**
- [ ] After deploying, re-run `DETECT_ANOMALIES` on the 4 `T_CrB` frames and compare the
      anomaly count before/after
- [ ] Decide whether `EDGE_MARGIN_FRAC`/`SPACE_DEBRIS_EDGE_ELONGATION_MIN`'s defaults need
      tuning for this specific telescope/corrector — both are site-specific, same as
      `QC_FWHM_MAX_ARCSEC`
- [ ] Manually clean up orphaned charts in `observatory-api` for source_ids whose
      `SPACE_DEBRIS`/`MOVING_UNKNOWN` anomaly no longer reproduces after this fix

---

## 5. Possible density-dependent WCS-offset correction failure on star-rich frames

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
