"""
modules/catalog_matcher/_sbident.py — JPL Small-Body Identification API
(`ssd-api.jpl.nasa.gov/sb_ident.api`), the second source of known asteroids
and comets in a field after IMCCE SkyBot.

Internal helpers only. `_mpc._query_mpc()` decides which source answers a
frame, owns the cache and the radius; this module only turns one cone into
one request and parses the answer into the same object dicts SkyBot yields.

Why a second source at all: SkyBot is the one catalog here run by a single
small institute, and it answered nginx 502 for days (2026-10-23 onward) — every
frame ingested meanwhile lost all ASTEROID/COMET matching. JPL computes the
same thing from its own orbit database, on unrelated infrastructure.

How it differs from SkyBot, and how each difference is absorbed here:

* **Rectangular field, not a cone.** The half-widths are set so the rectangle
  contains the requested circle (the RA one widened by 1/cos(dec), since JPL
  counts it in RA-coordinate degrees), then results outside the circle are
  dropped — both sources answer the same question.
* **Two passes.** The default first pass is a two-body approximation printed
  to whole seconds of RA (15") with ~40" estimated errors — wider than
  MATCH_CONE_ARCSEC, the cone a takeover of a catalogued star relies on. Only
  the second pass (`two-pass=true`, numerically integrated, 0.01 s / 0.1")
  is used.
* **No geocentre.** JPL refuses observatory code 500, so the observer is the
  site from SITE_LAT/SITE_LON/SITE_ELEV (the same one ephemeris.py gives
  Horizons). Left at its 0/0 default that is a point on the equator — a
  parallax error of the same size as SkyBot's geocentric one.
* **Names.** JPL prints "4 Vesta (A807 FA)", "23918 (1998 SH133)",
  "(2014 RY1)", "C/2020 R4 (ATLAS)", "29P/Schwassmann-Wachmann 1". SkyBot's
  name for the same objects is "Vesta", "1998 SH133", "2014 RY1",
  "C/2020 R4", "29P", and that string becomes the source's `catalog_id` —
  which history, dedup and finder-chart tracks key on — so JPL names are
  converted to SkyBot's (`_skybot_style_name()`). One object must not get two
  identities depending on which service happened to be up.
* **Magnitude limit** is applied by the service (`vmag-lim`); objects with no
  magnitude parameters are left out by its own default (`mag-required`).
"""

from __future__ import annotations

import logging
import math
import re

import astropy.units as u
import httpx
from astropy.coordinates import SkyCoord
from astropy.time import Time

import config

logger = logging.getLogger(__name__)

_SBIDENT_URL = "https://ssd-api.jpl.nasa.gov/sb_ident.api"

# The JSON layout the parser below was written against; a different major
# version is logged (JPL: "no guarantee the format is unchanged").
_EXPECTED_VERSION_MAJOR = "1"

# Comet designations: numbered periodic ("29P/…", "73P-B/…") or provisional
# with an orbit-type prefix ("C/2020 R4 (ATLAS)", "P/2010 H2 (Vales)").
_NUMBERED_COMET_RE = re.compile(r"^(\d+[PDCXI](?:-[A-Z]+)?)(?:/|$)")
_TRAILING_FRAGMENT_RE = re.compile(r"\s\S+-([A-Z]{1,2})$")
_PROVISIONAL_COMET_RE = re.compile(r"^([PDCXI]/\d{4} [A-Z]+\d*(?:-[A-Z]+)?)")
# Asteroids: "<number> <name> (<provisional>)", "<number> (<provisional>)",
# "(<provisional>)".
_NUMBERED_ASTEROID_RE = re.compile(r"^\d+\s+(.*?)\s*(?:\(([^)]*)\))?$")
_UNNUMBERED_ASTEROID_RE = re.compile(r"^\(([^)]*)\)$")

_DEC_RE = re.compile(r"^([+-]?)(\d+)\s+(\d+)'([\d.]+)\"?$")


def _skybot_style_name(jpl_name: str) -> tuple[str, str]:
    """
    (designation as SkyBot writes it, "ASTEROID" | "COMET") for a JPL object name.

    See the module docstring for why the designation must match SkyBot's.
    A name in no recognised shape is passed through unchanged as an asteroid.
    """
    name = jpl_name.strip()

    match = _NUMBERED_COMET_RE.match(name)
    if match:
        designation = match.group(1)
        # A fragment can also be printed after the name ("128P/Shoemaker-Holt
        # 1-A" beside "128P/Shoemaker-Holt 1"): it must not collapse into its
        # parent's designation.
        fragment = _TRAILING_FRAGMENT_RE.search(name)
        if fragment and "-" not in designation:
            designation += "-" + fragment.group(1)
        return designation, "COMET"
    match = _PROVISIONAL_COMET_RE.match(name)
    if match:
        return match.group(1), "COMET"

    match = _UNNUMBERED_ASTEROID_RE.match(name)
    if match:
        return match.group(1).strip(), "ASTEROID"
    match = _NUMBERED_ASTEROID_RE.match(name)
    if match:
        proper_name = match.group(1).strip()
        provisional = (match.group(2) or "").strip()
        return proper_name or provisional or name, "ASTEROID"

    return name, "ASTEROID"


def _sexagesimal(value_deg: float, *, hours: bool) -> str:
    """`hh-mm-ss.ss` / `[M]dd-mm-ss.s` — the only form sb_ident accepts for a centre."""
    sign = "M" if value_deg < 0 and not hours else ""
    total = abs(value_deg) / 15.0 if hours else abs(value_deg)
    decimals = 2 if hours else 1
    seconds_total = round(total * 3600.0, decimals)
    whole, seconds = divmod(seconds_total, 60.0)
    first, minutes = divmod(int(whole), 60)
    if hours:
        first %= 24
    return f"{sign}{first:02d}-{minutes:02d}-{seconds:0{3 + decimals}.{decimals}f}"


def _parse_dec(text: str) -> float:
    """`+11 48'12.0"` → degrees."""
    match = _DEC_RE.match(text.strip())
    if not match:
        raise ValueError(f"unparseable declination {text!r}")
    sign, deg, minutes, seconds = match.groups()
    value = int(deg) + int(minutes) / 60.0 + float(seconds) / 3600.0
    return -value if sign == "-" else value


def _column(fields: list[str], prefix: str) -> int:
    for index, field in enumerate(fields):
        if field.startswith(prefix):
            return index
    raise ValueError(f"sb_ident answer has no {prefix!r} column: {fields}")


def _query_sbident(ra_deg: float, dec_deg: float, radius_deg: float, epoch: Time) -> list[dict]:
    """
    Known asteroids and comets within `radius_deg` of (ra, dec) at `epoch`.

    Returns dicts with keys ra, dec, designation, object_type — the shape
    `_mpc._query_skybot()` yields. Raises on any failure (transport, HTTP
    error, malformed answer): the caller decides what a failure means.
    """
    dec_hwidth = min(radius_deg, 90.0)
    cos_dec = math.cos(math.radians(dec_deg))
    if abs(dec_deg) + radius_deg >= 90.0 or cos_dec <= 0.0:
        ra_hwidth = 180.0  # the circle contains a pole: every RA
    else:
        ra_hwidth = min(radius_deg / cos_dec, 180.0)

    params = {
        "lat": f"{config.SITE_LAT:.6f}",
        "lon": f"{config.SITE_LON:.6f}",
        "alt": f"{config.SITE_ELEV / 1000.0:.4f}",
        "obs-time": epoch.utc.strftime("%Y-%m-%d_%H:%M:%S"),
        "fov-ra-center": _sexagesimal(ra_deg, hours=True),
        "fov-dec-center": _sexagesimal(dec_deg, hours=False),
        "fov-ra-hwidth": f"{ra_hwidth:.6f}",
        "fov-dec-hwidth": f"{dec_hwidth:.6f}",
        "vmag-lim": f"{config.MPC_MAG_LIMIT:.1f}",
        "two-pass": "true",
    }

    response = httpx.get(_SBIDENT_URL, params=params, timeout=config.JPL_SBIDENT_TIMEOUT_SEC)
    if response.status_code != 200:
        # 400 carries a JSON "message" naming the bad parameter.
        raise RuntimeError(f"sb_ident HTTP {response.status_code}: {response.text[:300]}")
    payload = response.json()

    version = str(payload.get("signature", {}).get("version", ""))
    if not version.startswith(_EXPECTED_VERSION_MAJOR + "."):
        logger.warning("JPL sb_ident answered with format version %r (parser written for 1.x)", version)

    rows = payload.get("data_second_pass")
    if rows is None:
        # An empty field has no data arrays at all, only a warning; a field
        # whose first-pass candidates all fall outside it after integration
        # reports n_second_pass = 0. Anything else is a malformed answer.
        if payload.get("warning") == "no matching records" or str(payload.get("n_second_pass")) == "0":
            return []
        raise ValueError(f"sb_ident answer has no second-pass data: keys {sorted(payload)}")

    fields = payload.get("fields_second") or []
    name_i = _column(fields, "Object name")
    ra_i = _column(fields, "Astrometric RA")
    dec_i = _column(fields, "Astrometric Dec")
    mag_i = _column(fields, "Visual magnitude")

    centre = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg)
    objects: list[dict] = []
    for row in rows:
        try:
            coord = SkyCoord(row[ra_i], _parse_dec(row[dec_i]), unit=(u.hourangle, u.deg))
            if centre.separation(coord).deg > radius_deg:
                continue
            designation, object_type = _skybot_style_name(row[name_i])
            logger.info(
                "JPL sb_ident object: %s (%s)  type=%s  V=%s  ra=%.4f dec=%.4f",
                designation, row[name_i], object_type, row[mag_i], coord.ra.deg, coord.dec.deg,
            )
            objects.append({
                "ra": float(coord.ra.deg),
                "dec": float(coord.dec.deg),
                "designation": designation,
                "object_type": object_type,
            })
        except Exception as row_exc:
            logger.warning("JPL sb_ident: skipping malformed row %r: %s", row, row_exc)
    return objects
