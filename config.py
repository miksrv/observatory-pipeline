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

# ---------------------------------------------------------------------------
# Quality control thresholds
# ---------------------------------------------------------------------------
QC_FWHM_MAX_ARCSEC: float = float(_get("QC_FWHM_MAX_ARCSEC", "8.0"))
QC_ELONGATION_MAX: float = float(_get("QC_ELONGATION_MAX", "2.0"))
QC_SNR_MIN: float = float(_get("QC_SNR_MIN", "5.0"))
QC_STARS_MIN: int = int(_get("QC_STARS_MIN", "10"))

# ---------------------------------------------------------------------------
# Cross-matching
# ---------------------------------------------------------------------------
MATCH_CONE_ARCSEC: float = float(_get("MATCH_CONE_ARCSEC", "5.0"))
MOVING_CONE_ARCSEC: float = float(_get("MOVING_CONE_ARCSEC", "30.0"))
DELTA_MAG_ALERT: float = float(_get("DELTA_MAG_ALERT", "0.5"))
