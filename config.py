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
# Cross-matching
# ---------------------------------------------------------------------------
MATCH_CONE_ARCSEC: float = float(_get("MATCH_CONE_ARCSEC", "5.0"))
MOVING_CONE_ARCSEC: float = float(_get("MOVING_CONE_ARCSEC", "30.0"))
DELTA_MAG_ALERT: float = float(_get("DELTA_MAG_ALERT", "0.5"))

# ---------------------------------------------------------------------------
# Observatory site coordinates (used for topocentric Horizons queries)
# ---------------------------------------------------------------------------
SITE_LAT: float = float(_get("SITE_LAT", "0.0"))   # degrees, positive = North
SITE_LON: float = float(_get("SITE_LON", "0.0"))   # degrees, positive = East
SITE_ELEV: int  = int(_get("SITE_ELEV", "0"))      # metres above sea level

# ---------------------------------------------------------------------------
# Normalization settings
# ---------------------------------------------------------------------------
# When enabled, normalizes object names (M 51 → M51), filter names (Blue → B),
# frame types (Light Frame → Light), and renames files to standard format.
NORMALIZE_ENABLED: bool = _get("NORMALIZE_ENABLED", "true").lower() in ("true", "1", "yes")

