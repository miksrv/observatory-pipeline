"""
modules/ephemeris.py — Query JPL Horizons for solar system object ephemerides.

The single public entry point is:

    await ephemeris.query(designation: str, obs_time: str) -> dict | None

Returns a dict with predicted position, magnitude, distance, and angular
velocity for the named object at the given observation time, using the
observatory's topocentric coordinates from config.py.

Returns None on any error (network timeout, unknown designation, rate limit).
Errors are logged but never raised — the pipeline continues with partial results.
No tenacity retries: JPL Horizons enforces strict rate limits.
"""

from __future__ import annotations

import logging
import math

import numpy as np
import numpy.ma as npma
from astropy.time import Time
from astroquery.jplhorizons import Horizons

import config

logger = logging.getLogger(__name__)


async def query(designation: str, obs_time: str) -> dict | None:
    """
    Query JPL Horizons for the ephemeris of a solar system object.

    Parameters
    ----------
    designation:
        MPC/JPL designation string, e.g. "2019 XY3", "C/2023 A3", "433" (Eros).
    obs_time:
        ISO 8601 observation timestamp string, e.g. "2024-03-15T22:01:34Z".
        Must be parseable by astropy.time.Time with format='isot', scale='utc'.

    Returns
    -------
    dict with keys:
        predicted_ra                        float   degrees
        predicted_dec                       float   degrees
        predicted_mag                       float | None
        distance_au                         float | None
        angular_velocity_arcsec_per_hour    float | None
    Returns None if the query fails for any reason.
    """
    try:
        # Convert ISO 8601 → Julian Date for the Horizons epochs parameter
        jd = Time(obs_time, format="isot", scale="utc").jd

        # Topocentric location — elevation is stored in metres in config, but
        # Horizons expects kilometres (location dict 'elevation' key is in km).
        location = {
            "lon":       config.SITE_LON,
            "lat":       config.SITE_LAT,
            "elevation": config.SITE_ELEV / 1000.0,
        }

        horizons = Horizons(id=designation, location=location, epochs=jd)
        eph = horizons.ephemerides()

        row = eph[0]

        # RA / Dec — always present in a valid Horizons response
        ra  = float(row["RA"])
        dec = float(row["DEC"])

        # Apparent visual magnitude — may be masked for dark/featureless bodies
        predicted_mag: float | None = None
        try:
            v_val = row["V"]
            if not npma.is_masked(v_val):
                predicted_mag = float(v_val)
        except (KeyError, TypeError, ValueError):
            pass

        # Observer-centred distance in AU
        distance_au: float | None = None
        try:
            delta_val = row["delta"]
            if not npma.is_masked(delta_val):
                distance_au = float(delta_val)
        except (KeyError, TypeError, ValueError):
            pass

        # Angular velocity: sqrt(dRA_cos_dec^2 + dDec^2), arcsec/hour
        angular_velocity: float | None = None
        try:
            dra_val  = row["dRA*cosD"]
            ddec_val = row["dDec"]
            if not npma.is_masked(dra_val) and not npma.is_masked(ddec_val):
                angular_velocity = math.sqrt(float(dra_val) ** 2 + float(ddec_val) ** 2)
        except (KeyError, TypeError, ValueError):
            pass

        logger.debug(
            "Horizons ephemeris for %s: RA=%.4f Dec=%.4f",
            designation, ra, dec,
        )

        return {
            "predicted_ra":                     ra,
            "predicted_dec":                    dec,
            "predicted_mag":                    predicted_mag,
            "distance_au":                      distance_au,
            "angular_velocity_arcsec_per_hour": angular_velocity,
        }

    except Exception as e:
        logger.warning(
            "Horizons query failed for %s at %s: %s",
            designation, obs_time, e,
        )
        return None
