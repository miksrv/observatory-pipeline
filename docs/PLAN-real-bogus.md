# Plan: real/bogus classifier for subtraction candidates

Status: planned 2026-10-13, not started.

Goal: score every subtraction candidate with the probability that it is a real astrophysical
change rather than an artefact (subtraction residual, hot pixel, cosmic ray, misalignment), and
use that score to cut false `UNKNOWN` alerts (Known issue #1 in `CLAUDE.md`).

## 0. Hard constraint: the pipeline must work without it

The pipeline runs on several servers, and not every one will have a model, a GPU or the ML
dependencies. So:

- **No GPU at inference.** A real/bogus CNN is tiny (~10⁴–10⁵ parameters); scoring a few hundred
  cutouts on a CPU takes milliseconds. The GPU (lookatstars, RTX 2080 Ti) is needed **only for
  training**. The pipeline never calls another server for a score.
- **The model is a file**, not a service: an ONNX file at `REALBOGUS_MODEL_PATH` (in a bind-mounted
  directory, like the astap catalogs). Unset or missing → the step is skipped.
- **The dependency is optional:** `onnxruntime` (CPU build) is imported lazily; if it is not
  installed, log once at INFO and skip.
- **Any failure → `rb_score = None`** for that frame, never an exception that stops the frame.
- **`rb_score = None` behaves exactly like today.** Every decision that uses the score has an
  explicit "no score" branch equal to the current logic, so a pipeline without the model produces
  byte-identical results, and all current tests stay valid.

## 1. Cutouts (this repo)

New module `modules/realbogus/` (`_cutout.py`, `_score.py`, `run()` in `__init__.py`).

- `subtraction.run()` already holds `new_data`, `reference` and `diff` in memory. It gains an
  internal return field with a cutout **triplet** (new / reference / difference) per candidate.
  Arrays never go on the wire.
- **Instrument-independent geometry:** the cutout half-width is a multiple of the frame's own PSF
  (e.g. `REALBOGUS_CUTOUT_FWHM` × FWHM, default ~5), resampled to a fixed grid (e.g. 31×31).
  A fixed pixel size would mean something different on every camera.
- **Normalization:** difference cutout in units of the diff background RMS; new/reference cutouts
  sky-subtracted and scaled by the same factor. No per-camera constants.
- Masked pixels (saturation vicinity, non-finite, off-footprint) become an extra mask channel, not
  zeros the network would read as sky.
- The **same `_cutout.py`** is used by the dataset exporter (§3) and at inference, so training and
  production can never drift apart.

## 2. Scoring and how it is used

- `rb_score` (0..1) and `rb_model` (the model's version string) on each subtraction candidate.
  Scores from different models are not comparable, so the version is always stored with the
  score.
- **Phase A, shadow mode:** the score is computed and persisted but changes nothing. This gives
  real-world data to choose thresholds from.
- **Phase B, use it:** in `anomaly_detector/_classify.py`, an uncatalogued subtraction candidate
  with `rb_score < REALBOGUS_MIN_SCORE` is suppressed instead of becoming `UNKNOWN` (logged with
  the score). Not applied to MPC matches, `SPACE_DEBRIS` or streaks: they have independent evidence.
  `rb_score is None` → current behaviour.
- Optionally later: the score as an extra signal in `_could_be_a_new_bright_object()` and
  `_survives_edge_zone()`. Decide after Phase A data.
- New settings in `config.py` + `.env.example`: `REALBOGUS_MODEL_PATH` (empty default),
  `REALBOGUS_MIN_SCORE`, `REALBOGUS_CUTOUT_FWHM`, `REALBOGUS_CUTOUT_SIZE`.
- **API side** (queue in `docs/API-TASKS.md`): nullable `sources.rb_score` (float) and
  `sources.rb_model` (string), accepted by `POST /sources` and returned by the read endpoints so
  standalone `DETECT_ANOMALIES` can use them. Until then the pipeline strips them as
  `_rb_score` / `_rb_model` (leading underscore = not for the wire), and Phase A writes them to the
  log only.

## 3. Dataset (exporter in this repo, training outside the image)

`tools/export_rb_dataset.py`: re-runs QC → astrometry → subtraction over `FITS_ARCHIVE` (or
`/data/archive/FITS` on lookatstars) **without** registering anything, and writes cutouts +
metadata (frame, camera/telescope, filter, FWHM, pixel scale, candidate fields) to `.npz` shards.

Labels, from cheapest to most expensive:

1. **Injected fakes (main source of "real").** Before subtraction, add synthetic stars into the new
   frame at random positions and magnitudes, using a PSF built from the frame's own stars. The
   truth is known exactly and it works for any camera. Inject a few per frame, away from bright
   stars, and record a range of SNR so the model learns the faint end.
2. **Known movers.** Subtraction candidates matched to MPC/SkyBot are genuine changes (an asteroid
   was not there in the reference).
3. **Confirmed variables.** Candidates on stars with a confirmed same-filter Δmag.
4. **Bogus.** Candidates already suppressed as a catalogued star's residual, saturation-vicinity
   rejects, sharper-than-PSF rejects (hot pixels, if re-enabled in export mode), and candidates on
   frames whose subtraction had alignment trouble.
5. **Manual review.** A small local labelling tool (shows the triplet, keys R/B/?) for the
   uncertain middle, writing JSON next to the shards. No API dependency. A labelling page on the
   future website can come later.

First step before anything else: run the exporter over the archive and **count** candidates per
class. If reals are scarce, injection carries the training set; if bogus labels are noisy, manual
review goes first.

## 4. Training (lookatstars, separate from the pipeline image)

- Lives in a separate repo (e.g. `observatory-ml`) or at least outside `requirements.txt`: PyTorch
  + CUDA must never land in the pipeline image. It reads the `.npz` shards and writes an `.onnx`
  file plus a JSON card (version, training data range, metrics, cutout parameters).
- On lookatstars: NVIDIA userspace driver + `nvidia-container-toolkit`, training in a container
  with `--gpus all`. A small CNN trains in minutes on the 2080 Ti.
- **Split by night and object, not by candidate**: candidates from the same frame are strongly
  correlated, and a random split would overstate accuracy.
- Metrics: false-positive rate at a fixed true-positive rate (e.g. 95–98%; a missed transient costs
  more than an extra alert), broken down **per camera/telescope** and per filter. A camera that
  does worse gets more training data, not a separate threshold.
- The cutout parameters are recorded in the model card, and the pipeline refuses a model whose card
  doesn't match its own cutout settings (log a warning, skip scoring).

## 5. Rollout

1. Exporter + cutout module + counts (no behaviour change). PR, Patch.
2. First model; scoring in shadow mode, log only. PR, Minor (new optional setting).
3. API fields (`rb_score`, `rb_model`). Release together with observatory-api.
4. Phase B threshold, chosen from shadow-mode data. PR, Minor.
5. Retrain periodically as the archive grows; the model version in the data makes old and new
   scores distinguishable.

Each step: `CHANGELOG.md` line, `CLAUDE.md` module section, tests that mock the model (a fake
scorer returning fixed scores) and one that checks `rb_score = None` reproduces current results.
