"""
tests/test_anomaly_detector.py — Unit tests for modules/anomaly_detector.py

All API and ephemeris calls are mocked at the module namespace level:
    patch("modules.anomaly_detector.api_client.get_sources_near")
    patch("modules.anomaly_detector.api_client.get_frames_covering")
    patch("modules.anomaly_detector.ephemeris.query")

asyncio_mode = auto in pytest.ini — no @pytest.mark.asyncio decorators needed.
"""

from __future__ import annotations

import math
from unittest.mock import AsyncMock, patch

import pytest

import config
import modules.anomaly_detector as ad


# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

_RA  = 83.82
_DEC = -5.39
_OBS_TIME  = "2024-03-15T22:01:34Z"
_FRAME_ID  = "frame-001"
_FILENAME  = "test_frame.fits"

_FRAME_META = {
    "frame_id":   _FRAME_ID,
    "obs_time":   _OBS_TIME,
    "filename":   _FILENAME,
    "ra_center":  _RA,
    "dec_center": _DEC,
    "fov_deg":    1.0,
}

_EPH_DICT = {
    "predicted_ra":                     123.491,
    "predicted_dec":                    45.700,
    "predicted_mag":                    17.9,
    "distance_au":                      1.23,
    "angular_velocity_arcsec_per_hour": 45.2,
}


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------

def _make_source(
    ra: float = _RA,
    dec: float = _DEC,
    mag: float = 14.5,
    flux: float = 10_000.0,
    fwhm: float = 3.0,
    elongation: float = 1.1,
    catalog_name: str | None = None,
    catalog_id: str | None = None,
    catalog_mag: float | None = None,
    object_type: str | None = None,
) -> dict:
    return {
        "ra":           ra,
        "dec":          dec,
        "mag":          mag,
        "flux":         flux,
        "fwhm":         fwhm,
        "elongation":   elongation,
        "catalog_name": catalog_name,
        "catalog_id":   catalog_id,
        "catalog_mag":  catalog_mag,
        "object_type":  object_type,
    }


def _make_hist_source(ra: float = _RA, dec: float = _DEC, mag: float = 14.5) -> dict:
    return {"ra": ra, "dec": dec, "mag": mag}


def _make_coverage_frame() -> dict:
    return {"frame_id": "prev-001", "ra_center": _RA, "dec_center": _DEC}


# ---------------------------------------------------------------------------
# Patch helpers — return default "no history, no coverage" mocks
# ---------------------------------------------------------------------------

def _no_data_mocks():
    """
    Returns (mock_sources_near, mock_frames_covering) both returning empty lists.
    Useful as base for parametrised tests that override one side.
    """
    sources_near   = AsyncMock(return_value=[])
    frames_covering = AsyncMock(return_value=[])
    return sources_near, frames_covering


# ===========================================================================
# Helper unit tests
# ===========================================================================

class TestHaversineArcsec:

    def test_haversine_same_point(self):
        """Zero separation when both points are identical."""
        result = ad._haversine_arcsec(_RA, _DEC, _RA, _DEC)
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_haversine_known_separation(self):
        """1 arcminute north of a point should give ~60 arcsec separation."""
        dec_offset = _DEC + (1.0 / 60.0)  # 1 arcminute north
        result = ad._haversine_arcsec(_RA, _DEC, _RA, dec_offset)
        assert result == pytest.approx(60.0, rel=1e-4)


class TestObjectTypeClassifiers:

    def test_is_variable_star_matches(self):
        for otype in ("V*", "RR", "Cep", "BY", "RS", "Ell", "bL"):
            assert ad._is_variable_star(otype) is True, f"Expected True for '{otype}'"

    def test_is_variable_star_no_match(self):
        assert ad._is_variable_star("STAR") is False
        assert ad._is_variable_star(None) is False

    def test_is_binary_star_matches(self):
        for otype in ("**", "EB", "SB"):
            assert ad._is_binary_star(otype) is True, f"Expected True for '{otype}'"

    def test_is_galaxy_matches(self):
        for otype in ("G", "AGN", "SFG", "GiG"):
            assert ad._is_galaxy(otype) is True, f"Expected True for '{otype}'"

    def test_is_galaxy_none(self):
        assert ad._is_galaxy(None) is False


class TestHistoryMedianMag:

    def test_history_median_mag_normal(self):
        """Median across three sources with known magnitudes."""
        history = [
            _make_hist_source(mag=14.0),
            _make_hist_source(mag=15.0),
            _make_hist_source(mag=16.0),
        ]
        result = ad._history_median_mag(history)
        assert result == pytest.approx(15.0)

    def test_history_median_mag_empty(self):
        """Empty list must return None."""
        assert ad._history_median_mag([]) is None


class TestIsPositionShifted:

    def test_is_position_shifted_no_history(self):
        """No historical sources — cannot be shifted."""
        assert ad._is_position_shifted(_RA, _DEC, []) is False

    def test_is_position_shifted_close(self):
        """Historical source within MATCH_CONE_ARCSEC — not shifted."""
        # Place hist source 1 arcsec away (well inside 5 arcsec default cone)
        tiny_offset = 1.0 / 3600.0
        hist = [_make_hist_source(ra=_RA + tiny_offset, dec=_DEC)]
        assert ad._is_position_shifted(_RA, _DEC, hist) is False

    def test_is_position_shifted_far(self):
        """Historical source more than MATCH_CONE_ARCSEC away — shifted."""
        # 60 arcsec offset in dec — far outside any reasonable narrow cone
        large_offset = 60.0 / 3600.0
        hist = [_make_hist_source(ra=_RA, dec=_DEC + large_offset)]
        assert ad._is_position_shifted(_RA, _DEC, hist) is True


# ===========================================================================
# detect() integration tests
# ===========================================================================

class TestDetectEmptyAndFirstObservation:

    async def test_detect_empty_sources(self):
        """Empty sources list must return an empty anomaly list immediately."""
        result = await ad.detect(_FRAME_ID, [], [], _FRAME_META)
        assert result == []

    async def test_detect_first_observation(self):
        """Coverage returns [] → FIRST_OBSERVATION → source NOT in output."""
        source = _make_source()

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near", new_callable=AsyncMock) as mock_near,
            patch("modules.anomaly_detector.api_client.get_frames_covering", new_callable=AsyncMock) as mock_cov,
        ):
            mock_near.return_value   = []
            mock_cov.return_value    = []

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []


class TestDetectStationaryClassifications:

    async def test_detect_unknown_alert(self):
        """Covered, no history, no catalog match → UNKNOWN."""
        source = _make_source(catalog_name=None)

        async def _sources_side(ra, dec, radius_arcsec, before_time):
            return []

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near", side_effect=_sources_side),
            patch("modules.anomaly_detector.api_client.get_frames_covering", return_value=[_make_coverage_frame()]),
        ):
            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "UNKNOWN"
        assert result[0]["ra"] == pytest.approx(_RA)
        assert result[0]["dec"] == pytest.approx(_DEC)
        assert result[0]["mpc_designation"] is None
        assert result[0]["ephemeris"] is None

    async def test_detect_known_catalog_new(self):
        """Covered, no history, has catalog match → KNOWN_CATALOG_NEW → not in output."""
        source = _make_source(catalog_name="Gaia DR3", catalog_id="Gaia DR3 999")

        async def _sources_side(ra, dec, radius_arcsec, before_time):
            return []

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near", side_effect=_sources_side),
            patch("modules.anomaly_detector.api_client.get_frames_covering", return_value=[_make_coverage_frame()]),
        ):
            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []

    async def test_detect_supernova_candidate(self):
        """Covered, no history, galaxy object_type → SUPERNOVA_CANDIDATE."""
        source = _make_source(catalog_name="Simbad", object_type="G")

        async def _sources_side(ra, dec, radius_arcsec, before_time):
            return []

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near", side_effect=_sources_side),
            patch("modules.anomaly_detector.api_client.get_frames_covering", return_value=[_make_coverage_frame()]),
        ):
            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "SUPERNOVA_CANDIDATE"
        assert result[0]["ephemeris"] is None
        assert result[0]["mpc_designation"] is None

    async def test_detect_variable_star(self):
        """History with brightness change and variable OTYPE → VARIABLE_STAR."""
        # Current mag = 14.5; history median = 12.0 → delta = 2.5 > DELTA_MAG_ALERT
        source = _make_source(mag=14.5, catalog_name="Simbad", object_type="V*")
        hist   = [_make_hist_source(mag=12.0)]

        async def _sources_side(ra, dec, radius_arcsec, before_time):
            # Both wide-cone and narrow-cone return history (we just need narrow non-empty)
            return hist

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near", side_effect=_sources_side),
            patch("modules.anomaly_detector.api_client.get_frames_covering", return_value=[_make_coverage_frame()]),
        ):
            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        anomaly = result[0]
        assert anomaly["anomaly_type"] == "VARIABLE_STAR"
        assert anomaly["delta_mag"] == pytest.approx(2.5)
        assert anomaly["mpc_designation"] is None

    async def test_detect_binary_star(self):
        """History with brightness change and binary OTYPE → BINARY_STAR."""
        source = _make_source(mag=14.5, catalog_name="Simbad", object_type="EB")
        hist   = [_make_hist_source(mag=12.0)]

        async def _sources_side(ra, dec, radius_arcsec, before_time):
            return hist

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near", side_effect=_sources_side),
            patch("modules.anomaly_detector.api_client.get_frames_covering", return_value=[_make_coverage_frame()]),
        ):
            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "BINARY_STAR"
        assert result[0]["delta_mag"] == pytest.approx(2.5)

    async def test_detect_no_anomaly_stable_star(self):
        """History present, mag change below threshold → no anomaly."""
        # delta_mag = 0.1 which is below default DELTA_MAG_ALERT of 0.5
        source = _make_source(mag=14.5, catalog_name="Gaia DR3")
        hist   = [_make_hist_source(mag=14.4)]

        async def _sources_side(ra, dec, radius_arcsec, before_time):
            return hist

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near", side_effect=_sources_side),
            patch("modules.anomaly_detector.api_client.get_frames_covering", return_value=[_make_coverage_frame()]),
        ):
            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []


class TestDetectMpcMovingObjects:

    async def test_detect_asteroid(self):
        """MPC-matched ASTEROID → ASTEROID anomaly with ephemeris resolved."""
        designation = "2019 XY3"
        source = _make_source(
            catalog_name="MPC",
            catalog_id=designation,
            object_type="ASTEROID",
        )

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near", new_callable=AsyncMock, return_value=[]),
            patch("modules.anomaly_detector.api_client.get_frames_covering", new_callable=AsyncMock, return_value=[]),
            patch("modules.anomaly_detector.ephemeris.query", new_callable=AsyncMock, return_value=_EPH_DICT) as mock_eph,
        ):
            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        anomaly = result[0]
        assert anomaly["anomaly_type"] == "ASTEROID"
        assert anomaly["mpc_designation"] == designation
        assert anomaly["ephemeris"] == _EPH_DICT
        assert "_needs_ephemeris" not in anomaly
        mock_eph.assert_awaited_once_with(designation, _OBS_TIME)

    async def test_detect_comet(self):
        """MPC-matched non-ASTEROID → COMET."""
        source = _make_source(
            catalog_name="MPC",
            catalog_id="C/2024 A1",
            object_type="COMET",
        )

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near", new_callable=AsyncMock, return_value=[]),
            patch("modules.anomaly_detector.api_client.get_frames_covering", new_callable=AsyncMock, return_value=[]),
            patch("modules.anomaly_detector.ephemeris.query", new_callable=AsyncMock, return_value=_EPH_DICT),
        ):
            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "COMET"
        assert result[0]["mpc_designation"] == "C/2024 A1"

    async def test_detect_asteroid_ephemeris_failure(self):
        """ephemeris.query() returns None → anomaly still returned with ephemeris=None."""
        source = _make_source(
            catalog_name="MPC",
            catalog_id="2019 XY3",
            object_type="ASTEROID",
        )

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near", new_callable=AsyncMock, return_value=[]),
            patch("modules.anomaly_detector.api_client.get_frames_covering", new_callable=AsyncMock, return_value=[]),
            patch("modules.anomaly_detector.ephemeris.query", new_callable=AsyncMock, return_value=None),
        ):
            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "ASTEROID"
        assert result[0]["ephemeris"] is None
        assert "_needs_ephemeris" not in result[0]


class TestDetectUnmatchedMovingObjects:

    def _far_hist_source(self) -> dict:
        """A historical source 60 arcsec away — triggers position-shifted logic."""
        large_offset = 60.0 / 3600.0  # 60 arcsec in degrees
        return _make_hist_source(ra=_RA, dec=_DEC + large_offset)

    async def test_detect_moving_unknown(self):
        """Wide-cone history has far source, no MPC, elongation < 3 → MOVING_UNKNOWN."""
        source = _make_source(catalog_name=None, elongation=1.2)
        far    = self._far_hist_source()

        async def _sources_side(ra, dec, radius_arcsec, before_time):
            if radius_arcsec == config.MOVING_CONE_ARCSEC:
                return [far]
            return []

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near", side_effect=_sources_side),
            patch("modules.anomaly_detector.api_client.get_frames_covering", new_callable=AsyncMock, return_value=[]),
            patch("modules.anomaly_detector.ephemeris.query", new_callable=AsyncMock, return_value=None),
        ):
            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "MOVING_UNKNOWN"
        assert result[0]["mpc_designation"] is None
        assert result[0]["ephemeris"] is None

    async def test_detect_space_debris(self):
        """Wide-cone history has far source, no MPC, elongation > 3 → SPACE_DEBRIS."""
        source = _make_source(catalog_name=None, elongation=4.5)
        far    = self._far_hist_source()

        async def _sources_side(ra, dec, radius_arcsec, before_time):
            if radius_arcsec == config.MOVING_CONE_ARCSEC:
                return [far]
            return []

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near", side_effect=_sources_side),
            patch("modules.anomaly_detector.api_client.get_frames_covering", new_callable=AsyncMock, return_value=[]),
            patch("modules.anomaly_detector.ephemeris.query", new_callable=AsyncMock, return_value=None),
        ):
            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "SPACE_DEBRIS"


class TestDetectResilienceAndMixedSources:

    async def test_detect_api_failure_continues(self):
        """get_sources_near raising an exception must not crash; source is skipped."""
        source = _make_source(catalog_name=None)

        with (
            patch(
                "modules.anomaly_detector.api_client.get_sources_near",
                side_effect=Exception("simulated timeout"),
            ),
            patch("modules.anomaly_detector.api_client.get_frames_covering", new_callable=AsyncMock, return_value=[]),
        ):
            # Must not raise — pipeline continues
            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        # Source is not classified (all API calls failed) — empty or gracefully skipped
        # The module catches the exception and returns [] (no anomaly recorded)
        assert isinstance(result, list)

    async def test_detect_multiple_sources_mixed(self):
        """
        3 sources:
          - source_a: covered, no history, no catalog → UNKNOWN (alert)
          - source_b: no coverage → FIRST_OBSERVATION (suppressed)
          - source_c: covered, history, mag stable → no anomaly

        Only source_a should appear in the output.
        """
        # Slightly offset RAs so they hit different cache tiles
        source_a = _make_source(ra=83.82,  dec=-5.39, catalog_name=None, mag=14.5)
        source_b = _make_source(ra=83.93,  dec=-5.50, catalog_name=None, mag=15.0)
        source_c = _make_source(ra=84.05,  dec=-5.60, catalog_name="Gaia DR3", mag=14.4)

        hist_c = [_make_hist_source(ra=84.05, dec=-5.60, mag=14.4)]

        async def _sources_side(ra, dec, radius_arcsec, before_time):
            # source_c gets narrow-cone history; everything else gets nothing
            if abs(ra - 84.05) < 0.1:
                return hist_c
            return []

        async def _covering_side(ra, dec, before_time):
            # source_b has no coverage; source_a and source_c do
            if abs(ra - 83.93) < 0.1:
                return []
            return [_make_coverage_frame()]

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near", side_effect=_sources_side),
            patch("modules.anomaly_detector.api_client.get_frames_covering", side_effect=_covering_side),
        ):
            result = await ad.detect(
                _FRAME_ID,
                [source_a, source_b, source_c],
                [source_a, source_b, source_c],
                _FRAME_META,
            )

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "UNKNOWN"
        assert result[0]["ra"] == pytest.approx(83.82)
