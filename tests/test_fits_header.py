"""
tests/test_fits_header.py — Unit tests for modules/fits_header.py

All FITS files are constructed in-memory; no real files needed on disk.
"""

import math
import os
import tempfile

import astropy.io.fits as fits
import pytest

from modules.fits_header import extract_headers, sanitize_object_name


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_fits(headers: dict) -> str:
    """Write a minimal FITS file with given headers to a temp file, return path."""
    hdu = fits.PrimaryHDU()
    for key, value in headers.items():
        hdu.header[key] = value
    tmp = tempfile.NamedTemporaryFile(suffix=".fits", delete=False)
    hdu.writeto(tmp.name, overwrite=True)
    tmp.close()
    return tmp.name


def _cleanup(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# sanitize_object_name
# ---------------------------------------------------------------------------

class TestSanitizeObjectName:
    def test_simple_name(self):
        assert sanitize_object_name("M51") == "M51"

    def test_spaces_become_underscores(self):
        assert sanitize_object_name("NGC 1234") == "NGC_1234"

    def test_special_chars_stripped(self):
        assert sanitize_object_name("M51 (whirlpool!)") == "M51_whirlpool"

    def test_allowed_special_chars_kept(self):
        assert sanitize_object_name("2019 XY3") == "2019_XY3"
        assert sanitize_object_name("alpha-Ori") == "alpha-Ori"
        assert sanitize_object_name("HD+12345") == "HD+12345"

    def test_none_returns_unknown(self):
        assert sanitize_object_name(None) == "_UNKNOWN"

    def test_empty_string_returns_unknown(self):
        assert sanitize_object_name("") == "_UNKNOWN"

    def test_whitespace_only_returns_unknown(self):
        assert sanitize_object_name("   ") == "_UNKNOWN"

    def test_all_special_chars_returns_unknown(self):
        assert sanitize_object_name("!!!") == "_UNKNOWN"

    def test_unicode_stripped(self):
        result = sanitize_object_name("Ångström")
        assert result == "ngstrm" or result == "_UNKNOWN" or "ngstr" in result
        # Key assertion: no non-ASCII in result
        assert result.isascii() or result == "_UNKNOWN"


# ---------------------------------------------------------------------------
# Standard keyword extraction
# ---------------------------------------------------------------------------

class TestStandardKeywords:
    def setup_method(self):
        self.path = _write_fits({
            "DATE-OBS": "2024-03-15T22:01:34",
            "EXPTIME":  120.0,
            "OBJECT":   "M51",
            "FILTER":   "V",
            "IMAGETYP": "Light",
            "AIRMASS":  1.23,
            "RA":       202.4696,
            "DEC":      47.1952,
            "TELESCOP": "Celestron EdgeHD 11",
            "INSTRUME": "ZWO ASI2600MM Pro",
            "FOCALLEN": 2800.0,
            "APTDIA":   280.0,
            "CCD-TEMP": -10.0,
            "SET-TEMP": -10.0,
            "XBINNING": 1,
            "YBINNING": 1,
            "GAIN":     100.0,
            "OFFSET":   50.0,
            "NAXIS1":   6248,
            "NAXIS2":   4176,
            "OBSERVER": "John Smith",
            "SITENAME": "Backyard Observatory",
            "SITELAT":  55.7558,
            "SITELONG": 37.6173,
            "SITEELEV": 150.0,
            "SWCREATE": "N.I.N.A. 2.1",
        })

    def teardown_method(self):
        _cleanup(self.path)

    def test_obs_time(self):
        result = extract_headers(self.path)
        assert result["obs_time"] == "2024-03-15T22:01:34"

    def test_coordinates(self):
        result = extract_headers(self.path)
        assert abs(result["ra"] - 202.4696) < 0.001
        assert abs(result["dec"] - 47.1952) < 0.001

    def test_object_name_sanitized(self):
        result = extract_headers(self.path)
        assert result["object_name"] == "M51"

    def test_observation_group(self):
        obs = extract_headers(self.path)["observation"]
        assert obs["object"] == "M51"
        assert obs["exptime"] == 120.0
        assert obs["filter"] == "V"
        assert obs["frame_type"] == "Light"
        assert abs(obs["airmass"] - 1.23) < 0.001

    def test_instrument_group(self):
        inst = extract_headers(self.path)["instrument"]
        assert inst["telescope"] == "Celestron EdgeHD 11"
        assert inst["camera"] == "ZWO ASI2600MM Pro"
        assert inst["focal_length_mm"] == 2800.0
        assert inst["aperture_mm"] == 280.0

    def test_sensor_group(self):
        sensor = extract_headers(self.path)["sensor"]
        assert sensor["temp_celsius"] == -10.0
        assert sensor["temp_setpoint_celsius"] == -10.0
        assert sensor["binning_x"] == 1
        assert sensor["binning_y"] == 1
        assert sensor["gain"] == 100.0
        assert sensor["offset"] == 50.0
        assert sensor["width_px"] == 6248
        assert sensor["height_px"] == 4176

    def test_observer_group(self):
        obs = extract_headers(self.path)["observer"]
        assert obs["name"] == "John Smith"
        assert obs["site_name"] == "Backyard Observatory"
        assert abs(obs["site_lat"] - 55.7558) < 0.0001
        assert abs(obs["site_lon"] - 37.6173) < 0.0001
        assert obs["site_elev_m"] == 150.0

    def test_software_group(self):
        assert extract_headers(self.path)["software"]["capture"] == "N.I.N.A. 2.1"


# ---------------------------------------------------------------------------
# Alias keyword resolution
# ---------------------------------------------------------------------------

class TestAliasResolution:
    def test_objname_alias(self):
        path = _write_fits({"OBJNAME": "NGC_1234"})
        try:
            result = extract_headers(path)
            assert result["observation"]["object"] == "NGC_1234"
        finally:
            _cleanup(path)

    def test_target_alias(self):
        path = _write_fits({"TARGET": "Andromeda"})
        try:
            result = extract_headers(path)
            assert result["observation"]["object"] == "Andromeda"
        finally:
            _cleanup(path)

    def test_exposure_alias(self):
        path = _write_fits({"EXPOSURE": 60.0})
        try:
            assert extract_headers(path)["observation"]["exptime"] == 60.0
        finally:
            _cleanup(path)

    def test_filtnam_alias(self):
        path = _write_fits({"FILTNAM": "Ha"})
        try:
            assert extract_headers(path)["observation"]["filter"] == "Ha"
        finally:
            _cleanup(path)

    def test_filterid_alias(self):
        path = _write_fits({"FILTERID": "B"})
        try:
            assert extract_headers(path)["observation"]["filter"] == "B"
        finally:
            _cleanup(path)

    def test_frame_alias(self):
        path = _write_fits({"FRAME": "Dark"})
        try:
            assert extract_headers(path)["observation"]["frame_type"] == "Dark"
        finally:
            _cleanup(path)

    def test_camera_alias(self):
        path = _write_fits({"CAMERA": "Canon EOS"})
        try:
            assert extract_headers(path)["instrument"]["camera"] == "Canon EOS"
        finally:
            _cleanup(path)

    def test_aperture_alias(self):
        path = _write_fits({"APERTURE": 100.0})
        try:
            assert extract_headers(path)["instrument"]["aperture_mm"] == 100.0
        finally:
            _cleanup(path)

    def test_ccdtemp_alias(self):
        path = _write_fits({"CCDTEMP": -15.0})
        try:
            assert extract_headers(path)["sensor"]["temp_celsius"] == -15.0
        finally:
            _cleanup(path)

    def test_egain_alias(self):
        path = _write_fits({"EGAIN": 1.5})
        try:
            assert extract_headers(path)["sensor"]["gain"] == 1.5
        finally:
            _cleanup(path)

    def test_binning_alias_fills_both_axes(self):
        path = _write_fits({"BINNING": 2})
        try:
            sensor = extract_headers(path)["sensor"]
            assert sensor["binning_x"] == 2
            assert sensor["binning_y"] == 2
        finally:
            _cleanup(path)

    def test_author_alias(self):
        path = _write_fits({"AUTHOR": "Jane Doe"})
        try:
            assert extract_headers(path)["observer"]["name"] == "Jane Doe"
        finally:
            _cleanup(path)

    def test_observat_alias(self):
        path = _write_fits({"OBSERVAT": "Mt. Wilson"})
        try:
            assert extract_headers(path)["observer"]["site_name"] == "Mt. Wilson"
        finally:
            _cleanup(path)

    def test_software_alias(self):
        path = _write_fits({"SOFTWARE": "Sequence Generator Pro"})
        try:
            assert extract_headers(path)["software"]["capture"] == "Sequence Generator Pro"
        finally:
            _cleanup(path)


# ---------------------------------------------------------------------------
# Sexagesimal coordinate conversion
# ---------------------------------------------------------------------------

class TestCoordinateConversion:
    def test_ra_hms_string(self):
        path = _write_fits({"OBJCTRA": "13 29 52.7"})
        try:
            ra = extract_headers(path)["ra"]
            assert ra is not None
            # 13h 29m 52.7s = 13 + 29/60 + 52.7/3600 hours * 15 deg/hour ≈ 202.469°
            assert abs(ra - 202.469) < 0.01
        finally:
            _cleanup(path)

    def test_dec_dms_string_positive(self):
        path = _write_fits({"OBJCTDEC": "+47 11 43"})
        try:
            dec = extract_headers(path)["dec"]
            assert dec is not None
            # +47° 11' 43" ≈ 47.195°
            assert abs(dec - 47.195) < 0.01
        finally:
            _cleanup(path)

    def test_dec_dms_string_negative(self):
        path = _write_fits({"OBJCTDEC": "-30 15 00"})
        try:
            dec = extract_headers(path)["dec"]
            assert dec is not None
            assert abs(dec - (-30.25)) < 0.01
        finally:
            _cleanup(path)

    def test_ra_decimal_passthrough(self):
        path = _write_fits({"RA": 180.0})
        try:
            assert extract_headers(path)["ra"] == 180.0
        finally:
            _cleanup(path)

    def test_ra_colon_separated_hms(self):
        # Some cameras write OBJCTRA as "HH:MM:SS.s"
        path = _write_fits({"OBJCTRA": "13:29:52.7"})
        try:
            ra = extract_headers(path)["ra"]
            assert ra is not None
            assert abs(ra - 202.469) < 0.01
        finally:
            _cleanup(path)

    def test_malformed_sexagesimal_returns_none(self):
        path = _write_fits({"OBJCTRA": "not_a_coord"})
        try:
            # Must not raise — returns None gracefully
            ra = extract_headers(path)["ra"]
            assert ra is None
        finally:
            _cleanup(path)


# ---------------------------------------------------------------------------
# Missing / empty headers
# ---------------------------------------------------------------------------

class TestMissingHeaders:
    def test_empty_fits_returns_none_values(self):
        path = _write_fits({})
        try:
            result = extract_headers(path)
            assert result["obs_time"] is None
            assert result["ra"] is None
            assert result["dec"] is None
            assert result["object_name"] == "_UNKNOWN"
            assert result["observation"]["object"] is None
            assert result["observation"]["exptime"] is None
            assert result["instrument"]["telescope"] is None
            assert result["sensor"]["temp_celsius"] is None
            assert result["observer"]["name"] is None
            assert result["software"]["capture"] is None
        finally:
            _cleanup(path)

    def test_no_keyerror_on_any_missing_key(self):
        path = _write_fits({"DATE-OBS": "2024-01-01T00:00:00"})
        try:
            result = extract_headers(path)   # must not raise
            assert isinstance(result, dict)
        finally:
            _cleanup(path)

    def test_missing_object_gives_unknown_dir_name(self):
        path = _write_fits({"EXPTIME": 30.0})
        try:
            assert extract_headers(path)["object_name"] == "_UNKNOWN"
        finally:
            _cleanup(path)

    def test_mjd_obs_converted_to_isot(self):
        # MJD 60384.0 = 2024-03-15T00:00:00.000
        path = _write_fits({"MJD-OBS": 60384.0})
        try:
            obs_time = extract_headers(path)["obs_time"]
            assert obs_time is not None
            assert "2024-03-15" in obs_time
        finally:
            _cleanup(path)

    def test_date_obs_takes_priority_over_mjd(self):
        path = _write_fits({"DATE-OBS": "2024-03-15T22:01:34", "MJD-OBS": 60384.0})
        try:
            assert extract_headers(path)["obs_time"] == "2024-03-15T22:01:34"
        finally:
            _cleanup(path)

    def test_nonexistent_file_returns_empty_dict(self):
        result = extract_headers("/nonexistent/path/frame.fits")
        assert isinstance(result, dict)
        assert result["object_name"] == "_UNKNOWN"
        assert result["ra"] is None

    def test_object_with_spaces_sanitized(self):
        path = _write_fits({"OBJECT": "NGC 1234"})
        try:
            assert extract_headers(path)["object_name"] == "NGC_1234"
        finally:
            _cleanup(path)


# ---------------------------------------------------------------------------
# Output structure completeness
# ---------------------------------------------------------------------------

class TestOutputStructure:
    EXPECTED_KEYS = {
        "obs_time", "ra", "dec", "object_name",
        "observation", "instrument", "sensor", "observer", "software",
    }
    EXPECTED_OBSERVATION_KEYS = {"object", "exptime", "filter", "frame_type", "airmass"}
    EXPECTED_INSTRUMENT_KEYS = {"telescope", "camera", "focal_length_mm", "aperture_mm"}
    EXPECTED_SENSOR_KEYS = {
        "temp_celsius", "temp_setpoint_celsius", "binning_x", "binning_y",
        "gain", "offset", "width_px", "height_px",
    }
    EXPECTED_OBSERVER_KEYS = {"name", "site_name", "site_lat", "site_lon", "site_elev_m"}
    EXPECTED_SOFTWARE_KEYS = {"capture"}

    def test_all_top_level_keys_present(self):
        path = _write_fits({})
        try:
            result = extract_headers(path)
            assert self.EXPECTED_KEYS == set(result.keys())
        finally:
            _cleanup(path)

    def test_observation_keys_present(self):
        path = _write_fits({})
        try:
            assert self.EXPECTED_OBSERVATION_KEYS == set(extract_headers(path)["observation"].keys())
        finally:
            _cleanup(path)

    def test_instrument_keys_present(self):
        path = _write_fits({})
        try:
            assert self.EXPECTED_INSTRUMENT_KEYS == set(extract_headers(path)["instrument"].keys())
        finally:
            _cleanup(path)

    def test_sensor_keys_present(self):
        path = _write_fits({})
        try:
            assert self.EXPECTED_SENSOR_KEYS == set(extract_headers(path)["sensor"].keys())
        finally:
            _cleanup(path)

    def test_observer_keys_present(self):
        path = _write_fits({})
        try:
            assert self.EXPECTED_OBSERVER_KEYS == set(extract_headers(path)["observer"].keys())
        finally:
            _cleanup(path)

    def test_software_keys_present(self):
        path = _write_fits({})
        try:
            assert self.EXPECTED_SOFTWARE_KEYS == set(extract_headers(path)["software"].keys())
        finally:
            _cleanup(path)
