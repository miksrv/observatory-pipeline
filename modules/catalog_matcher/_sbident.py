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
import time

import astropy.units as u
import httpx
from astropy.coordinates import Angle, SkyCoord
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
    if hours:
        # Rounded to the printed 0.01 s first, so 23:59:59.999 wraps to 00 rather than printing 24.
        seconds = round(value_deg / 15.0 * 3600.0, 2) % 86400.0
        return Angle(seconds / 3600.0, u.hourangle).to_string(sep="-", precision=2, pad=True)
    text = Angle(value_deg, u.deg).to_string(unit=u.deg, sep="-", precision=1, pad=True)
    return "M" + text[1:] if text.startswith("-") else text


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


_site_warned = False


def _warn_once_if_site_unset() -> None:
    """
    JPL computes positions for the observer's site. SITE_* left at 0/0 puts it
    on the equator: harmless for a main-belt asteroid (a few arcsec, as
    SkyBot's geocentre), but a near-Earth object's parallax from the wrong
    site can exceed MOVING_CONE_ARCSEC and leave it unmatched.
    """
    global _site_warned
    if not _site_warned and config.SITE_LAT == 0.0 and config.SITE_LON == 0.0:
        _site_warned = True
        logger.warning(
            "JPL sb_ident: SITE_LAT/SITE_LON are 0/0 — positions are computed for a point on "
            "the equator; set the observatory's site for near-Earth objects to match"
        )


def _get_with_retry(params: dict) -> dict:
    """
    One sb_ident request, retried up to JPL_SBIDENT_MAX_ATTEMPTS in total on a
    transport error or a 5xx (JPL documents 503 as "likely resolved after a
    delay") — during a SkyBot outage it is the only source left. A 4xx is a
    bad request and raised at once.
    """
    attempts = max(1, config.JPL_SBIDENT_MAX_ATTEMPTS)
    delay = config.SKYBOT_RETRY_DELAY_SEC
    for attempt in range(1, attempts + 1):
        try:
            response = httpx.get(_SBIDENT_URL, params=params, timeout=config.JPL_SBIDENT_TIMEOUT_SEC)
        except httpx.TransportError as exc:
            error: Exception = exc
        else:
            if response.status_code == 200:
                return response.json()
            # 400 carries a JSON "message" naming the bad parameter.
            error = RuntimeError(f"sb_ident HTTP {response.status_code}: {response.text[:300]}")
            if response.status_code < 500:
                raise error
        if attempt == attempts:
            raise error
        logger.warning(
            "JPL sb_ident attempt %d/%d failed (%s) — retrying in %.1fs", attempt, attempts, error, delay,
        )
        time.sleep(delay)
        delay *= 2
    raise AssertionError("unreachable")


def _query_sbident(ra_deg: float, dec_deg: float, radius_deg: float, epoch: Time) -> list[dict]:
    """
    Known asteroids and comets within `radius_deg` of (ra, dec) at `epoch`.

    Returns dicts with keys ra, dec, designation, object_type — the shape
    `_mpc._query_skybot()` yields. Raises on any failure (transport, HTTP
    error, malformed answer): the caller decides what a failure means.
    """
    dec_hwidth = min(radius_deg, 90.0)
    if abs(dec_deg) + radius_deg >= 90.0:
        ra_hwidth = 180.0  # the circle contains a pole: every RA
    else:
        # The circle's widest RA extent, which radius/cos(dec) underestimates
        # near a pole (17.2 deg instead of 17.5 at dec 85 deg, radius 1.5 deg).
        ra_hwidth = math.degrees(math.asin(
            math.sin(math.radians(radius_deg)) / math.cos(math.radians(dec_deg))
        ))

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

    _warn_once_if_site_unset()
    payload = _get_with_retry(params)

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
