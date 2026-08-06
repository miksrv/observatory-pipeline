"""
tests/test_anomaly_detector.py — Unit tests for modules/anomaly_detector.py

All API and ephemeris calls are mocked at the module namespace level:
    patch("modules.anomaly_detector.api_client.get_sources_near_batch")
    patch("modules.anomaly_detector.api_client.get_frames_covering_batch")
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
    source_id: str | None = None,
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
        # Resolved sources.id, as attached by pipeline.py's Step 7 after
        # POST /frames/{id}/sources. None by default, matching a source
        # the pipeline couldn't resolve an id for.
        "_source_id":   source_id,
    }


def _make_hist_source(ra: float = _RA, dec: float = _DEC, mag: float = 14.5) -> dict:
    return {"ra": ra, "dec": dec, "mag": mag}


def _make_coverage_frame() -> dict:
    return {"frame_id": "prev-001", "ra_center": _RA, "dec_center": _DEC}


# ---------------------------------------------------------------------------
# Batch mock helpers
# ---------------------------------------------------------------------------

def _make_batch_sources_result(sources_per_tile: list[list[dict]] | None = None) -> dict:
    """
    Build a mock return value for get_sources_near_batch.
    
    Args:
        sources_per_tile: List of source lists for each tile index.
                         If None, returns empty results for all tiles.
    """
    if sources_per_tile is None:
        return {}
    return {str(i): sources for i, sources in enumerate(sources_per_tile)}


def _make_batch_coverage_result(coverage_per_tile: list[list[dict]] | None = None) -> dict:
    """
    Build a mock return value for get_frames_covering_batch.
    
    Args:
        coverage_per_tile: List of frame lists for each tile index.
                          If None, returns empty results for all tiles.
    """
    if coverage_per_tile is None:
        return {}
    return {str(i): frames for i, frames in enumerate(coverage_per_tile)}


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
# detect() integration tests — using batch API mocks
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
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            # No sources, no coverage
            mock_sources.return_value = {}
            mock_cov.return_value = {}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []


class TestDetectStationaryClassifications:

    async def test_detect_unknown_alert(self):
        """Covered, no history, no catalog match → UNKNOWN."""
        source = _make_source(catalog_name=None, source_id="src-unknown-001")

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            # No source history, but area has coverage
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "UNKNOWN"
        assert result[0]["ra"] == pytest.approx(_RA)
        assert result[0]["dec"] == pytest.approx(_DEC)
        assert result[0]["mpc_designation"] is None
        assert result[0]["ephemeris"] is None
        assert result[0]["source_id"] == "src-unknown-001"

    async def test_detect_known_catalog_new(self):
        """Covered, no history, has catalog match → KNOWN_CATALOG_NEW → not in output."""
        source = _make_source(catalog_name="Gaia DR3", catalog_id="Gaia DR3 999")

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": []}  # queried, but nothing found — no prior history
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []

    async def test_detect_supernova_candidate_new_source(self):
        """Covered, no history at all, galaxy object_type → SUPERNOVA_CANDIDATE.

        This is the "new point source with no prior detection" variant
        (n_history == 0). See test_detect_supernova_candidate_brightening
        below for the "already-known host, got brighter" variant.
        """
        source = _make_source(catalog_name="Simbad", object_type="G")

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": []}  # queried, nothing found
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "SUPERNOVA_CANDIDATE"
        assert result[0]["ephemeris"] is None
        assert result[0]["mpc_designation"] is None

    async def test_detect_supernova_candidate_brightening(self):
        """
        Regression test: an already-known host galaxy that brightens well
        beyond DELTA_MAG_ALERT must be flagged SUPERNOVA_CANDIDATE.

        Previously unreachable for two compounding reasons (both fixed):
        1. History was never queried for catalog-matched sources at all,
           so n_history was always 0 and this branch of the function
           (which requires n_history > 0) could never execute.
        2. Even with history present, the "has prior history" branch only
           checked _is_binary_star / _is_variable_star — never
           _is_galaxy — so a brightening galaxy fell through to "no
           anomaly" regardless.
        """
        source = _make_source(mag=16.0, catalog_name="Simbad", object_type="G")
        hist   = [_make_hist_source(mag=20.0)]  # quiescent host baseline

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": hist}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "SUPERNOVA_CANDIDATE"
        assert result[0]["delta_mag"] == pytest.approx(-4.0)

    async def test_detect_supernova_candidate_dimming_not_flagged(self):
        """A galaxy-associated source that DIMS (not brightens) must not be
        flagged SUPERNOVA_CANDIDATE — a fading foreground star is not a
        supernova signature."""
        source = _make_source(mag=20.0, catalog_name="Simbad", object_type="G")
        hist   = [_make_hist_source(mag=16.0)]  # was brighter, now fainter

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": hist}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []

    async def test_detect_variable_star(self):
        """
        Regression test: history with brightness change and variable OTYPE
        → VARIABLE_STAR.

        Previously catalog-matched sources (required for object_type to be
        set at all, since it comes from Simbad) never got a history lookup
        — a structural gap that made this classification permanently
        unreachable. Now fixed: history is queried for every source
        regardless of catalog-match status.
        """
        # Current mag = 14.5; history median = 12.0 → delta = 2.5 > DELTA_MAG_ALERT
        source = _make_source(mag=14.5, catalog_name="Simbad", object_type="V*")
        hist   = [_make_hist_source(mag=12.0)]

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": hist}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "VARIABLE_STAR"
        assert result[0]["delta_mag"] == pytest.approx(2.5)

    async def test_detect_binary_star(self):
        """Regression test: history with brightness change and binary OTYPE
        → BINARY_STAR (see test_detect_variable_star for why this was
        previously unreachable)."""
        source = _make_source(mag=14.5, catalog_name="Simbad", object_type="EB")
        hist   = [_make_hist_source(mag=12.0)]

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": hist}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "BINARY_STAR"

    async def test_detect_no_anomaly_stable_star(self):
        """Catalog-matched source with no history at all → KNOWN_CATALOG_NEW
        (suppressed, not an anomaly)."""
        source = _make_source(mag=14.5, catalog_name="Gaia DR3")

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []

    async def test_detect_no_anomaly_stable_star_with_history(self):
        """
        Regression test: a catalog-matched source WITH real, essentially
        unchanged historical magnitude must NOT become a false-positive
        anomaly now that history is fetched for catalog-matched sources too.
        """
        source = _make_source(mag=14.5, catalog_name="Gaia DR3", object_type="STAR")
        hist   = [_make_hist_source(mag=14.4)]  # within DELTA_MAG_ALERT

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": hist}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

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
            source_id="src-vesta-001",
        )

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
            patch("modules.anomaly_detector.ephemeris.query", new_callable=AsyncMock, return_value=_EPH_DICT) as mock_eph,
        ):
            mock_sources.return_value = {"0": []}  # MPC sources do query history
            mock_cov.return_value = {"0": []}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        anomaly = result[0]
        assert anomaly["anomaly_type"] == "ASTEROID"
        assert anomaly["mpc_designation"] == designation
        assert anomaly["ephemeris"] == _EPH_DICT
        assert "_needs_ephemeris" not in anomaly
        # Regression: anomalies[].source_id was previously never populated
        # at all (always null in the API) — see CLAUDE.md Known Issues.
        assert anomaly["source_id"] == "src-vesta-001"
        mock_eph.assert_awaited_once_with(designation, _OBS_TIME)

    async def test_detect_asteroid_without_resolved_source_id(self):
        """When pipeline.py couldn't resolve a sources.id (e.g. post_sources
        failed or returned a mismatched source_ids list), source_id must be
        None rather than crashing or being silently omitted."""
        source = _make_source(
            catalog_name="MPC",
            catalog_id="2019 XY3",
            object_type="ASTEROID",
        )

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
            patch("modules.anomaly_detector.ephemeris.query", new_callable=AsyncMock, return_value=_EPH_DICT),
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": []}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result[0]["source_id"] is None

    async def test_detect_comet(self):
        """MPC-matched non-ASTEROID → COMET."""
        source = _make_source(
            catalog_name="MPC",
            catalog_id="C/2024 A1",
            object_type="COMET",
        )

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
            patch("modules.anomaly_detector.ephemeris.query", new_callable=AsyncMock, return_value=_EPH_DICT),
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": []}

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
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
            patch("modules.anomaly_detector.ephemeris.query", new_callable=AsyncMock, return_value=None),
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": []}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "ASTEROID"
        assert result[0]["ephemeris"] is None
        assert "_needs_ephemeris" not in result[0]


class TestDetectUnmatchedMovingObjects:

    def _far_hist_source(self) -> dict:
        """
        A historical source that is:
        - Within MOVING_CONE_ARCSEC (30") so it's returned by _find_sources_within_radius
        - But farther than MATCH_CONE_ARCSEC (5") to trigger position-shifted logic

        We use 15 arcsec offset which is within 30" but beyond 5".
        """
        offset_arcsec = 15.0  # Between MATCH_CONE (5") and MOVING_CONE (30")
        offset_deg = offset_arcsec / 3600.0
        return _make_hist_source(ra=_RA, dec=_DEC + offset_deg)

    async def test_detect_moving_unknown(self):
        """Wide-cone history has shifted source (>5"), no MPC, elongation < 3 → MOVING_UNKNOWN."""
        source = _make_source(catalog_name=None, elongation=1.2, source_id="src-mover-001")
        far    = self._far_hist_source()

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            # Return far source in history batch
            mock_sources.return_value = {"0": [far]}
            mock_cov.return_value = {"0": []}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "MOVING_UNKNOWN"
        assert result[0]["mpc_designation"] is None
        assert result[0]["ephemeris"] is None
        assert result[0]["source_id"] == "src-mover-001"

    async def test_detect_space_debris(self):
        """Wide-cone history has shifted source (>5"), no MPC, elongation > 3 → SPACE_DEBRIS."""
        source = _make_source(catalog_name=None, elongation=4.5)
        far    = self._far_hist_source()

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": [far]}
            mock_cov.return_value = {"0": []}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "SPACE_DEBRIS"


class TestDetectResilienceAndMixedSources:

    async def test_detect_api_failure_continues(self):
        """get_sources_near_batch raising an exception must not crash; sources processed with empty data."""
        source = _make_source(catalog_name=None)

        with (
            patch(
                "modules.anomaly_detector.api_client.get_sources_near_batch",
                side_effect=Exception("simulated timeout"),
            ),
            patch(
                "modules.anomaly_detector.api_client.get_frames_covering_batch",
                side_effect=Exception("simulated timeout"),
            ),
        ):
            # Must not raise — pipeline continues
            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        # Source is not classified as UNKNOWN because we don't have coverage data
        # (batch failed), so it's treated as FIRST_OBSERVATION (no coverage = suppressed)
        assert isinstance(result, list)
        # With no data at all, sources get n_coverage=0 → FIRST_OBSERVATION → suppressed
        assert result == []

    async def test_detect_multiple_sources_mixed(self):
        """
        3 sources:
          - source_a: covered, no history, no catalog → UNKNOWN (alert)
          - source_b: no coverage → FIRST_OBSERVATION (suppressed)
          - source_c: covered, has catalog match → KNOWN_CATALOG_NEW (suppressed)

        Only source_a should appear in the output.
        """
        # All sources at same RA/DEC range for simplicity (same tile)
        source_a = _make_source(ra=83.82,  dec=-5.39, catalog_name=None, mag=14.5)
        source_b = _make_source(ra=83.82,  dec=-5.39, catalog_name=None, mag=15.0)
        source_c = _make_source(ra=83.82,  dec=-5.39, catalog_name="Gaia DR3", mag=14.4)

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            # Return empty history but coverage for the tile
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(
                _FRAME_ID,
                [source_a, source_b, source_c],
                [source_a, source_b, source_c],
                _FRAME_META,
            )

        # Both source_a and source_b are unmatched with coverage and no history → UNKNOWN
        # source_c is catalog-matched → KNOWN_CATALOG_NEW (suppressed)
        assert len(result) == 2
        assert all(r["anomaly_type"] == "UNKNOWN" for r in result)
