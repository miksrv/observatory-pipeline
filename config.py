"""
config.py — Load all pipeline configuration from environment variables.

Every module imports from here. No module should contain hardcoded paths,
thresholds, or credentials.
"""

import os
from dotenv import load_dotenv

load_dotenv()


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Required environment variable '{name}' is not set. "
                         f"Check your .env file.")
    return value


def _get(name: str, default: str) -> str:
    return os.getenv(name, default)


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
API_BASE_URL: str = _require("API_BASE_URL")
API_KEY: str = _require("API_KEY")

# ---------------------------------------------------------------------------
# FITS directory paths
# ---------------------------------------------------------------------------
FITS_INCOMING: str = _get("FITS_INCOMING", "/fits/incoming")
FITS_ARCHIVE: str = _get("FITS_ARCHIVE", "/fits/archive")
FITS_REJECTED: str = _get("FITS_REJECTED", "/fits/rejected")

# ---------------------------------------------------------------------------
# ASTAP plate solver
# ---------------------------------------------------------------------------
ASTAP_BINARY: str = _get("ASTAP_BINARY", "/usr/local/bin/astap")
ASTAP_CATALOGS: str = _get("ASTAP_CATALOGS", "/astap/catalogs")
# Optional FOV hint in degrees (0 = auto-detect from FITS headers)
ASTAP_FOV_HINT: float = float(_get("ASTAP_FOV_HINT", "0"))

# ---------------------------------------------------------------------------
# Quality control thresholds
# ---------------------------------------------------------------------------
QC_FWHM_MAX_ARCSEC: float = float(_get("QC_FWHM_MAX_ARCSEC", "8.0"))
QC_ELONGATION_MAX: float = float(_get("QC_ELONGATION_MAX", "2.0"))
QC_SNR_MIN: float = float(_get("QC_SNR_MIN", "5.0"))
QC_STARS_MIN: int = int(_get("QC_STARS_MIN", "10"))
# Maximum acceptable median sky background (ADU). Frames exceeding this are
# rejected as HIGH_BACKGROUND — twilight, moonlight, cloud, or stray light
# raise the background without necessarily blurring FWHM or trailing stars,
# so BLUR/TRAIL/LOW_STARS alone can miss it (see docs/ISSUES.md). Default is
# a generic, generous ballpark relative to SATURATION_ADU — tune to your own
# site's typical dark-sky background (log the actual qc_sky_background
# values your setup reports on good frames, then set this a comfortable
# margin above them).
QC_SKY_BACKGROUND_MAX: float = float(_get("QC_SKY_BACKGROUND_MAX", "50000.0"))
# Star-count floor applied instead of QC_STARS_MIN when the frame's own
# filter is narrowband (see NARROWBAND_FILTERS below). A narrowband frame of
# the exact same field genuinely detects far fewer stars than a broadband one
# — only the sliver of stellar continuum that leaks through an Hα/[OIII]/
# [SII]/[NII] bandpass is visible at all — so holding it to the broadband
# QC_STARS_MIN systematically rejects perfectly good narrowband data as
# LOW_STARS. Still must clear the hard-coded floor of 3 raw detections
# enforced independently in modules/qc.py before either threshold applies.
QC_STARS_MIN_NARROWBAND: int = int(_get("QC_STARS_MIN_NARROWBAND", "5"))

# ---------------------------------------------------------------------------
# Narrowband filters
# ---------------------------------------------------------------------------
# Emission-line filters whose bandpass is too narrow to carry a
# representative sample of field stars, or a trustworthy Gaia DR3 photometric
# zero-point — see CLAUDE.md's "Filters — real astronomy context" section.
# Compared against the *normalized* filter short code (see
# modules/normalizer.normalize_filter_name()/is_narrowband()), so this must
# use those canonical codes ("Ha", "OIII", "SII", "NII"), not raw header
# spellings like "H-Alpha" or "[OIII]".
NARROWBAND_FILTERS: frozenset[str] = frozenset(
    f.strip() for f in _get("NARROWBAND_FILTERS", "Ha,OIII,SII,NII").split(",") if f.strip()
)

# ---------------------------------------------------------------------------
# Star detection filtering (astrometry module)
# These parameters filter raw SEP detections to keep only point sources (stars)
# and reject extended objects (nebula parts, galaxies) and artifacts.
# ---------------------------------------------------------------------------
STAR_FWHM_MIN_ARCSEC: float = float(_get("STAR_FWHM_MIN_ARCSEC", "2.5"))
STAR_FWHM_MAX_ARCSEC: float = float(_get("STAR_FWHM_MAX_ARCSEC", "8.0"))
STAR_ELONGATION_MAX: float = float(_get("STAR_ELONGATION_MAX", "1.5"))
STAR_SNR_MIN: float = float(_get("STAR_SNR_MIN", "50.0"))

# SEP source extraction parameters
SEP_DETECT_THRESH: float = float(_get("SEP_DETECT_THRESH", "10.0"))  # sigma above background
SEP_MIN_AREA: int = int(_get("SEP_MIN_AREA", "15"))  # minimum connected pixels

# ---------------------------------------------------------------------------
# Streak masking (astrometry + qc + subtraction modules)
# ---------------------------------------------------------------------------
# A satellite/aircraft trail crossing a single exposure, or a diffraction-spike
# arm radiating from a bright/saturated star, is frequently too faint along
# parts of its length to survive as one connected sep.extract() object even
# with deblending disabled — real incident, 2026-08-07, `T_CrB` test frame
# (`T_CrB_Light_L_60_2024-05-28T19-06-10.fits`): a full-frame satellite trail
# fragmented into several small, roundish sep objects at the ordinary
# detection settings, each individually clearing STAR_ELONGATION_MAX and
# getting reported as an ordinary star (on the difference image in
# modules/subtraction.py, the same trail fragmented into 40+ separate
# elongated candidates, each independently classifiable as its own
# SPACE_DEBRIS anomaly). A coarse, low-threshold, non-deblended pre-pass finds
# these long thin features BEFORE the real point-source extraction runs and
# masks their pixels out, so they can never fragment into false stars/
# candidates in the first place. This intentionally leaves the real
# extraction's own deblend_cont completely untouched — it is exactly as
# effective at splitting a genuinely close double star in a crowded field as
# before this fix.
#
# Only a coarse candidate that is BOTH highly elongated (>=
# STREAK_ELONGATION_MIN) AND far longer than any real stellar PSF footprint
# (bounding-box diagonal >= STREAK_MIN_LENGTH_ARCSEC) gets masked — an
# ordinary star, even a blended pair, never reaches that combination.
#
# Deliberately lower than SEP_DETECT_THRESH/SUBTRACTION_DETECT_SIGMA (both
# 10.0/5.0 by default) — verified against real data (2026-08-07, T_CrB test
# frames): at 5.0σ, a genuinely faint satellite trail's brightness dips below
# the coarse pass's own threshold often enough that it still fragments into
# dozens of small, disconnected coarse candidates too short to individually
# clear STREAK_MIN_LENGTH_ARCSEC — on the subtraction diff image specifically,
# 21 of 42 original false SPACE_DEBRIS-eligible candidates survived at 5.0σ;
# dropping to 3.0σ let the coarse pass connect nearly the entire trail into
# one long feature, leaving just 1.
STREAK_DETECT_SIGMA: float = float(_get("STREAK_DETECT_SIGMA", "3.0"))
STREAK_ELONGATION_MIN: float = float(_get("STREAK_ELONGATION_MIN", "5.0"))
STREAK_MIN_LENGTH_ARCSEC: float = float(_get("STREAK_MIN_LENGTH_ARCSEC", "30.0"))
# Small dilation applied to the coarse streak mask before it's subtracted out,
# so the (unresolved, often noisy) segmentation boundary doesn't leave a thin
# rim of trail pixels just outside the mask still detectable as their own
# tiny fragment.
STREAK_MASK_DILATE_ARCSEC: float = float(_get("STREAK_MASK_DILATE_ARCSEC", "3.0"))

# ---------------------------------------------------------------------------
# Saturation detection (astrometry + subtraction modules)
# ---------------------------------------------------------------------------
# Sensor pixel value (ADU) at/above which a pixel is considered saturated.
# Default assumes a 16-bit sensor with some headroom below the hard 65535
# ceiling for non-linearity near full well — tune per camera.
SATURATION_ADU: float = float(_get("SATURATION_ADU", "60000"))
# Radius (arcsec) around a saturated pixel that modules/subtraction.py
# excludes from difference-image source detection, to suppress astroalign
# residual artifacts around bright/saturated stars (see docs/ISSUES.md #1, #2).
SATURATION_MASK_RADIUS_ARCSEC: float = float(_get("SATURATION_MASK_RADIUS_ARCSEC", "10.0"))

# ---------------------------------------------------------------------------
# Cross-matching
# ---------------------------------------------------------------------------
MATCH_CONE_ARCSEC: float = float(_get("MATCH_CONE_ARCSEC", "5.0"))
# Default widened from 30" to 120": fast-moving objects like Vesta travel ~60"/hr,
# so 30" was too tight to detect cross-frame position shifts reliably.
MOVING_CONE_ARCSEC: float = float(_get("MOVING_CONE_ARCSEC", "120.0"))
DELTA_MAG_ALERT: float = float(_get("DELTA_MAG_ALERT", "0.5"))

# Faintest predicted visual magnitude (V) for an MPC/SkyBot object to be
# eligible for source matching. Objects fainter than this are almost certainly
# below the pipeline's detection threshold and would only ever "match" to an
# unrelated background star (real incident, 2026-08-10: SkyBot returned 130+
# asteroids in a 1° Vesta field, most at V > 20; the pipeline matched each one
# to its nearest unmatched star, producing dozens of spurious ASTEROID
# anomalies that didn't move between frames). Set to your telescope's
# approximate detection limit for the typical exposure time.
MPC_MAG_LIMIT: float = float(_get("MPC_MAG_LIMIT", "19.0"))

# ---------------------------------------------------------------------------
# Edge-of-frame geometry (astrometry + subtraction + anomaly_detector modules)
# ---------------------------------------------------------------------------
# Fraction of the frame's width/height (NAXIS1/NAXIS2) treated as "near the
# edge". Coma and other off-axis optical aberrations progressively stretch a
# star's PSF toward the corners/edges of a wide-field frame — a perfectly
# ordinary, non-moving star there can measure an inflated `elongation` for
# purely optical reasons, not because it's a trail. modules/astrometry/_extraction.py
# (and modules/subtraction.py, for diff-image candidates) flags any source
# whose pixel position falls within this fraction of NAXIS1/NAXIS2 from any
# edge as `near_edge` — see SPACE_DEBRIS_EDGE_ELONGATION_MIN below for how
# modules/anomaly_detector/ uses it. No universal default fits every
# telescope/corrector combination — a well-corrected wide-field refractor
# needs a much smaller margin than a fast Newtonian; tune to how far coma
# actually reaches into your own frames.
EDGE_MARGIN_FRAC: float = float(_get("EDGE_MARGIN_FRAC", "0.05"))
# Elongation threshold for the "single-exposure trail" SPACE_DEBRIS shortcut
# in modules/anomaly_detector/ (an unmatched source with no history at its
# current position needs no further evidence beyond this to be classified
# SPACE_DEBRIS — see that module's docstring). Extracted to config instead of
# staying hardcoded so the edge-aware variant below has a documented
# counterpart to sit next to.
SPACE_DEBRIS_ELONGATION_MIN: float = float(_get("SPACE_DEBRIS_ELONGATION_MIN", "3.0"))
# Same shortcut, but for a source flagged `near_edge` — a substantially
# higher bar, since coma alone can already push an ordinary star's elongation
# past SPACE_DEBRIS_ELONGATION_MIN near the edge of a wide-field frame. Real
# incident, 2026-08-07: 4 T_CrB frames produced 305 anomalies, the vast
# majority being coma-elongated but otherwise ordinary corner stars firing
# this exact shortcut. A genuine single-exposure satellite/debris trail is
# typically far more elongated than coma alone produces, so raising the bar
# near the edge (rather than removing the elongation-alone shortcut there
# entirely) keeps real edge-of-frame trails detectable while filtering out
# the aberration.
SPACE_DEBRIS_EDGE_ELONGATION_MIN: float = float(_get("SPACE_DEBRIS_EDGE_ELONGATION_MIN", "6.0"))

# ---------------------------------------------------------------------------
# Image subtraction
# ---------------------------------------------------------------------------
# Minimum number of archived reference frames required to attempt subtraction.
SUBTRACTION_MIN_FRAMES: int = int(_get("SUBTRACTION_MIN_FRAMES", "3"))
# Detection threshold on the difference image (multiples of background RMS).
SUBTRACTION_DETECT_SIGMA: float = float(_get("SUBTRACTION_DETECT_SIGMA", "5.0"))

# ---------------------------------------------------------------------------
# Forced photometry (modules/forced_photometry.py) — reverse matching
# ---------------------------------------------------------------------------
# A second, independent pass run AFTER catalog_matcher.py's forward matching:
# for every Gaia DR3 star (and every MPC/SkyBot object) within this frame's
# footprint that has no corresponding entry in sources_all, measure the flux
# at that exact predicted pixel position anyway, instead of silently treating
# it as "not detected". Recovers objects too faint for the blind SEP
# extraction's necessarily-high detection threshold (SEP_DETECT_THRESH),
# and — just as importantly — catalog stars the blind extractor's own star
# filter (elongation/FWHM/SNR bounds) or WCS residual happened to miss even
# though they are bright enough to detect. See modules/forced_photometry.py's
# own docstring for the full rationale ("forced photometry" / "precovery";
# originally proposed as ROADMAP.md #1 — see git log for that history).
#
# Set false to disable this pass entirely (it adds one aperture-photometry
# measurement per eligible catalog star/MPC object, on top of the already-
# fetched Gaia/MPC field lists — no new network queries).
FORCED_PHOTOMETRY_ENABLED: bool = _get("FORCED_PHOTOMETRY_ENABLED", "true").lower() in ("true", "1", "yes")
# Faintest Gaia DR3 G-band magnitude eligible for forced photometry. Same
# rationale as MPC_MAG_LIMIT above: forcing a measurement on literally every
# Gaia star in the field (down to Gaia's own ~21 mag completeness limit)
# would mean thousands of uninformative noise measurements in a dense field.
# No universal default fits every telescope/exposure combination — set this
# a couple of magnitudes above your own setup's typical detection limit for
# a representative exposure time (see qc.py's snr_median/sky_background on
# good frames for a sense of where that limit actually sits for your gear).
# Not applied to MPC/SkyBot objects — _query_mpc() already filters those by
# MPC_MAG_LIMIT before they ever reach this module.
FORCED_PHOTOMETRY_MAG_LIMIT: float = float(_get("FORCED_PHOTOMETRY_MAG_LIMIT", "20.0"))
# Minimum significance (net_flux / flux_err) for a forced-photometry
# measurement to be reported at all. Below this, the position is treated as
# a genuine non-detection and silently dropped — NOT reported with a faint
# "upper limit" magnitude, since the API's source payload (docs/API.md §2)
# has no field to distinguish a real magnitude from an upper limit; adding
# one is a separate, cross-repo change (observatory-api's schema), not made
# here. A lower bar than blind extraction's SEP_DETECT_THRESH (10.0σ by
# default) is intentional and statistically justified: forced photometry
# tests exactly one hypothesis (a specific known position) rather than
# scanning every independent resolution element in the frame for an unknown
# number of sources, so the "look-elsewhere effect" that forces blind
# extraction's threshold up doesn't apply here.
FORCED_PHOTOMETRY_MIN_SNR: float = float(_get("FORCED_PHOTOMETRY_MIN_SNR", "3.0"))

# ---------------------------------------------------------------------------
# Observatory site coordinates (used for topocentric Horizons queries)
# ---------------------------------------------------------------------------
SITE_LAT: float = float(_get("SITE_LAT", "0.0"))   # degrees, positive = North
SITE_LON: float = float(_get("SITE_LON", "0.0"))   # degrees, positive = East
SITE_ELEV: int  = int(_get("SITE_ELEV", "0"))      # metres above sea level

# ---------------------------------------------------------------------------
# Finder charts (modules/finder_chart.py)
# ---------------------------------------------------------------------------
# Set false to skip chart generation entirely (it adds a local render step
# plus two extra API round-trips per alerting-anomaly source).
CHART_ENABLED: bool = _get("CHART_ENABLED", "true").lower() in ("true", "1", "yes")
# Half-width of the per-epoch crop used by the "stamp_strip" style
# (stationary anomalies), in arcseconds. Converted to pixels per-frame using
# that frame's own WCS plate scale.
CHART_STAMP_SIZE_ARCSEC: float = float(_get("CHART_STAMP_SIZE_ARCSEC", "60.0"))
# Cap on the number of epochs drawn on one chart (oldest dropped first if a
# source's track is longer than this) — keeps the rendered image size, and
# the number of local archive FITS files opened per chart, bounded for
# sources with a very long observation history.
CHART_MAX_EPOCHS: int = int(_get("CHART_MAX_EPOCHS", "12"))
# When a source's chart style is "track" or "stamp_strip" (2+ epochs — a
# single-epoch source always gets "before_after", which has no animation
# equivalent), also render and upload an animated GIF alongside the static
# PNG: "track_gif" (cumulative reveal of the motion track, one epoch added
# per frame) or "stamp_strip_gif" (one epoch's own crop per frame — a
# proper "blink" instead of a static side-by-side grid). Independent of
# CHART_ENABLED being true; set false to keep generating only the static
# PNGs. Best-effort like every other chart upload — a GIF render/upload
# failure never affects the PNG's own already-reported result (see
# modules/finder_chart.py's update_charts_for_sources() docstring).
CHART_GIF_ENABLED: bool = _get("CHART_GIF_ENABLED", "true").lower() in ("true", "1", "yes")
# Per-frame display duration of the animated GIF, in milliseconds.
CHART_GIF_FRAME_DURATION_MS: int = int(_get("CHART_GIF_FRAME_DURATION_MS", "700"))

# ---------------------------------------------------------------------------
# Normalization settings
# ---------------------------------------------------------------------------
# When enabled, normalizes object names (M 51 → M51), filter names (Blue → B),
# frame types (Light Frame → Light), and renames files to standard format.
NORMALIZE_ENABLED: bool = _get("NORMALIZE_ENABLED", "true").lower() in ("true", "1", "yes")

# ---------------------------------------------------------------------------
# Catalog query cache (modules/catalog_matcher/)
# ---------------------------------------------------------------------------
# On-disk cache directory for external catalog query results (Gaia/Simbad/
# 2MASS/Pan-STARRS/MPC), keyed by catalog + sky tile. Backs the in-process
# dict with a persistent store so restarting the pipeline (frequent during
# testing) doesn't throw away query results and re-hit the network for the
# same sky region. Mount this from a host path OUTSIDE the container in
# docker-compose.yml (see CATALOG_CACHE_DIR's volume there) — a path that
# only lives inside the container's writable layer is gone on every restart,
# exactly the case this cache exists to survive. In a non-Docker production
# deployment this is just a plain directory on disk; nothing here depends on
# being containerized.
CATALOG_CACHE_DIR: str = _get("CATALOG_CACHE_DIR", "/cache/catalog")
# How long a cached catalog query result stays valid, in hours. Was a
# hardcoded 1-hour constant in catalog_matcher.py; exposed here so it can be
# tuned (e.g. shortened while iterating on matching logic during testing,
# without editing code) per config.py's "no hardcoded thresholds in modules"
# convention.
CACHE_TTL_HOURS: float = float(_get("CACHE_TTL_HOURS", "1.0"))

# ---------------------------------------------------------------------------
# Watcher batching (watcher.py)
# ---------------------------------------------------------------------------
# How long to wait, after the most recently arrived FITS file, before
# submitting everything buffered so far as one ANALYZE task. Re-armed on
# every new arrival, so a burst of files arriving close together (a bulk
# import) is still submitted as one task rather than one per file; a single
# frame arriving in isolation (a live overnight run) is submitted on its own
# once this quiet period elapses.
WATCHER_DEBOUNCE_SEC: float = float(_get("WATCHER_DEBOUNCE_SEC", "5.0"))
# Flush the pending batch immediately once it reaches this many files,
# instead of waiting out the full debounce window — bounds how large a
# single ANALYZE task can grow during a large bulk import, and means
# progress becomes visible sooner than "one giant task sitting at 0% for a
# long time" would.
WATCHER_MAX_BATCH_SIZE: int = int(_get("WATCHER_MAX_BATCH_SIZE", "200"))
# When true, watcher.py watches FITS_INCOMING with watchdog's PollingObserver
# (periodic directory listing/diff) instead of the platform-native,
# inotify-based Observer. Needed on Docker Desktop for macOS: FITS_INCOMING is
# a bind-mounted host directory there (see docker-compose.yml), and host-side
# changes to a bind mount don't reliably generate inotify events inside the
# Linux container — this is most visible for a bulk directory copy, where the
# new subdirectory's recursive watch can lose the race against files already
# landing inside it, silently dropping their on_created events (real
# incident, 2026-08-11: a whole subdirectory of FITS files copied into
# FITS_INCOMING produced zero "New FITS file detected" log lines and no
# ANALYZE task, even though process_existing_files()'s plain os.walk() at the
# next watcher restart found every one of them). PollingObserver doesn't
# depend on the kernel/FUSE layer delivering change notifications at all, so
# it isn't subject to either failure mode — at the cost of a periodic
# directory listing instead of instant event delivery. Leave false on the
# real observatory server (Linux, local disk, no bind-mount translation
# layer), where the native Observer is more efficient and has no such gap.
WATCHER_USE_POLLING_OBSERVER: bool = _get("WATCHER_USE_POLLING_OBSERVER", "false").lower() in ("true", "1", "yes")
# How often PollingObserver re-lists FITS_INCOMING, in seconds. Only used
# when WATCHER_USE_POLLING_OBSERVER is true. Independent of
# WATCHER_DEBOUNCE_SEC — this is how fast a new file is *noticed*, the
# debounce is how long the watcher then waits before submitting it.
WATCHER_POLLING_INTERVAL_SEC: float = float(_get("WATCHER_POLLING_INTERVAL_SEC", "2.0"))

# ---------------------------------------------------------------------------
# Task queue worker (worker.py)
# ---------------------------------------------------------------------------
# How often the worker polls GET /tasks?status=PENDING when idle, in seconds.
TASK_POLL_INTERVAL_SEC: float = float(_get("TASK_POLL_INTERVAL_SEC", "10.0"))
# Idle polling backs off exponentially (doubling each empty poll) up to this
# ceiling, then holds steady — keeps the API quiet during long idle stretches
# without ever waiting so long that a newly-submitted task sits unnoticed for
# an unreasonable amount of time. Resets to TASK_POLL_INTERVAL_SEC the moment
# a task is found.
TASK_POLL_BACKOFF_MAX_SEC: float = float(_get("TASK_POLL_BACKOFF_MAX_SEC", "60.0"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
# Log verbosity level. Valid values: DEBUG, INFO, WARNING, ERROR.
# Use INFO for normal operation; DEBUG for troubleshooting.
LOG_LEVEL: str = _get("LOG_LEVEL", "INFO").upper()


# ---------------------------------------------------------------------------
# Remote settings overlay
# ---------------------------------------------------------------------------
# Registry of config parameters that the API's GET /settings endpoint can
# override at startup.  Maps param name → (python type, cast function).
# Only parameters listed here are accepted from the remote side — API_KEY,
# paths, and other security-/deployment-sensitive values are deliberately
# excluded.  Values arrive as strings from the API (the settings table stores
# everything as text); the cast function converts them to the correct Python
# type, matching the same conversion each _get() call above already applies.
_OVERRIDABLE: dict[str, type] = {
    # QC thresholds
    "QC_FWHM_MAX_ARCSEC": float,
    "QC_ELONGATION_MAX": float,
    "QC_SNR_MIN": float,
    "QC_STARS_MIN": int,
    "QC_SKY_BACKGROUND_MAX": float,
    "QC_STARS_MIN_NARROWBAND": int,
    # Star detection filtering
    "STAR_FWHM_MIN_ARCSEC": float,
    "STAR_FWHM_MAX_ARCSEC": float,
    "STAR_ELONGATION_MAX": float,
    "STAR_SNR_MIN": float,
    # SEP extraction
    "SEP_DETECT_THRESH": float,
    "SEP_MIN_AREA": int,
    # Streak masking
    "STREAK_DETECT_SIGMA": float,
    "STREAK_ELONGATION_MIN": float,
    "STREAK_MIN_LENGTH_ARCSEC": float,
    "STREAK_MASK_DILATE_ARCSEC": float,
    # Saturation
    "SATURATION_ADU": float,
    "SATURATION_MASK_RADIUS_ARCSEC": float,
    # Cross-matching
    "MATCH_CONE_ARCSEC": float,
    "MOVING_CONE_ARCSEC": float,
    "DELTA_MAG_ALERT": float,
    "MPC_MAG_LIMIT": float,
    # Edge geometry
    "EDGE_MARGIN_FRAC": float,
    "SPACE_DEBRIS_ELONGATION_MIN": float,
    "SPACE_DEBRIS_EDGE_ELONGATION_MIN": float,
    # Image subtraction
    "SUBTRACTION_MIN_FRAMES": int,
    "SUBTRACTION_DETECT_SIGMA": float,
    # Forced photometry
    "FORCED_PHOTOMETRY_ENABLED": None,  # special: bool from string
    "FORCED_PHOTOMETRY_MAG_LIMIT": float,
    "FORCED_PHOTOMETRY_MIN_SNR": float,
    # Observatory site
    "SITE_LAT": float,
    "SITE_LON": float,
    "SITE_ELEV": int,
    # Finder charts
    "CHART_ENABLED": None,  # special: bool from string
    "CHART_STAMP_SIZE_ARCSEC": float,
    "CHART_MAX_EPOCHS": int,
    "CHART_GIF_ENABLED": None,  # special: bool from string
    "CHART_GIF_FRAME_DURATION_MS": int,
    # Normalization
    "NORMALIZE_ENABLED": None,  # special: bool from string
    # Catalog cache
    "CACHE_TTL_HOURS": float,
    # Watcher batching
    "WATCHER_DEBOUNCE_SEC": float,
    "WATCHER_MAX_BATCH_SIZE": int,
    # Worker polling
    "TASK_POLL_INTERVAL_SEC": float,
    "TASK_POLL_BACKOFF_MAX_SEC": float,
    # Logging
    "LOG_LEVEL": None,  # special: str.upper()
    # ASTAP
    "ASTAP_FOV_HINT": float,
    # Narrowband filters
    "NARROWBAND_FILTERS": None,  # special: frozenset from CSV
}

_BOOL_KEYS = {"CHART_ENABLED", "CHART_GIF_ENABLED", "NORMALIZE_ENABLED", "FORCED_PHOTOMETRY_ENABLED"}


def _cast_value(name: str, raw: str) -> object:
    """Convert a raw string value from the API into the correct Python type."""
    if name in _BOOL_KEYS:
        return raw.strip().lower() in ("true", "1", "yes")
    if name == "LOG_LEVEL":
        return raw.strip().upper()
    if name == "NARROWBAND_FILTERS":
        return frozenset(f.strip() for f in raw.split(",") if f.strip())
    typ = _OVERRIDABLE[name]
    return typ(raw)


def apply_remote_settings(settings: dict[str, str]) -> int:
    """
    Override module-level config globals with values from the API.

    Parameters
    ----------
    settings:
        Flat ``{param_name: string_value}`` dict as returned by
        ``GET /settings`` (the ``data`` field of the response).

    Returns
    -------
    int
        Number of parameters successfully applied.
    """
    import logging
    _logger = logging.getLogger(__name__)

    applied = 0
    for name, raw in settings.items():
        if name not in _OVERRIDABLE:
            _logger.debug("Remote setting '%s' is not overridable — skipped", name)
            continue
        try:
            value = _cast_value(name, str(raw))
            globals()[name] = value
            _logger.info("Remote setting applied: %s = %r", name, value)
            applied += 1
        except (ValueError, TypeError) as exc:
            _logger.warning(
                "Remote setting '%s' has invalid value '%s': %s — keeping default",
                name, raw, exc,
            )
    return applied
