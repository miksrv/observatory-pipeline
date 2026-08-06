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

# ---------------------------------------------------------------------------
# Image subtraction
# ---------------------------------------------------------------------------
# Minimum number of archived reference frames required to attempt subtraction.
SUBTRACTION_MIN_FRAMES: int = int(_get("SUBTRACTION_MIN_FRAMES", "3"))
# Detection threshold on the difference image (multiples of background RMS).
SUBTRACTION_DETECT_SIGMA: float = float(_get("SUBTRACTION_DETECT_SIGMA", "5.0"))

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

# ---------------------------------------------------------------------------
# Normalization settings
# ---------------------------------------------------------------------------
# When enabled, normalizes object names (M 51 → M51), filter names (Blue → B),
# frame types (Light Frame → Light), and renames files to standard format.
NORMALIZE_ENABLED: bool = _get("NORMALIZE_ENABLED", "true").lower() in ("true", "1", "yes")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
# Log verbosity level. Valid values: DEBUG, INFO, WARNING, ERROR.
# Use INFO for normal operation; DEBUG for troubleshooting.
LOG_LEVEL: str = _get("LOG_LEVEL", "INFO").upper()

