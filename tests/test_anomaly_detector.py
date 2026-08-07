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
    saturated: bool = False,
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
        "saturated":    saturated,
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


class TestIsStillOccupied:

    def test_still_occupied_true_when_current_source_nearby(self):
        """A current-frame source within MATCH_CONE_ARCSEC counts as occupied."""
        tiny_offset = 1.0 / 3600.0
        current = [(_RA + tiny_offset, _DEC)]
        assert ad._is_still_occupied(_RA, _DEC, current) is True

    def test_still_occupied_false_when_nothing_nearby(self):
        """No current-frame source anywhere near — not occupied."""
        far_offset = 60.0 / 3600.0
        current = [(_RA + far_offset, _DEC)]
        assert ad._is_still_occupied(_RA, _DEC, current) is False

    def test_still_occupied_false_when_no_current_sources(self):
        assert ad._is_still_occupied(_RA, _DEC, []) is False


class TestIsPositionShifted:
    """
    _is_position_shifted(narrow_history, wide_history, current_frame_positions)
    requires BOTH: nothing at the current position (narrow_history empty),
    AND a wide-cone historical position that has genuinely emptied out (not
    still occupied by something in the current frame). See docs/ISSUES.md #1.
    """

    def test_no_narrow_no_wide_history(self):
        """No history anywhere — cannot be shifted."""
        assert ad._is_position_shifted([], [], []) is False

    def test_narrow_history_present_short_circuits(self):
        """
        Something already detected within MATCH_CONE_ARCSEC of the current
        position — this is NOT a "new" position, regardless of what's in the
        wide cone or the current frame. This is the exact false-positive
        this fix targets: sub-arcsecond centroid/seeing noise on an
        otherwise-stable source used to still fall through to the wide-cone
        check and get flagged MOVING_UNKNOWN purely because some unrelated
        object happened to be nearby.
        """
        narrow = [_make_hist_source(ra=_RA, dec=_DEC)]
        far_offset = 60.0 / 3600.0
        wide = [_make_hist_source(ra=_RA, dec=_DEC + far_offset)]
        assert ad._is_position_shifted(narrow, wide, []) is False

    def test_wide_history_vacated_is_shifted(self):
        """
        Nothing at the current position, and the wide-cone historical
        position is empty in the current frame too — genuine mover.
        """
        far_offset = 15.0 / 3600.0
        wide = [_make_hist_source(ra=_RA, dec=_DEC + far_offset)]
        assert ad._is_position_shifted([], wide, []) is True

    def test_wide_history_still_occupied_is_not_shifted(self):
        """
        Nothing at the current position, BUT the wide-cone historical
        position is still occupied by another source in THIS frame — that's
        a permanent neighbour (another star/galaxy), not evidence that
        anything moved away from there. Must NOT be flagged shifted.
        """
        far_offset = 15.0 / 3600.0
        neighbour_dec = _DEC + far_offset
        wide = [_make_hist_source(ra=_RA, dec=neighbour_dec)]
        current_frame_positions = [(_RA, neighbour_dec)]  # neighbour still there now
        assert ad._is_position_shifted([], wide, current_frame_positions) is False

    def test_mixed_wide_history_one_vacated_one_still_occupied(self):
        """
        Two wide-cone candidates: one still occupied (persistent neighbour,
        ignored), one genuinely vacated (real mover's old spot) — must still
        report shifted because of the second one.
        """
        occupied_dec = _DEC + 10.0 / 3600.0
        vacated_dec  = _DEC + 20.0 / 3600.0
        wide = [
            _make_hist_source(ra=_RA, dec=occupied_dec),
            _make_hist_source(ra=_RA, dec=vacated_dec),
        ]
        current_frame_positions = [(_RA, occupied_dec)]
        assert ad._is_position_shifted([], wide, current_frame_positions) is True


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

    async def test_detect_persistent_neighbour_is_not_moving(self):
        """
        Regression for docs/ISSUES.md #1: a faint uncatalogued source with no
        detection of its own within MATCH_CONE_ARCSEC (e.g. its first-ever
        epoch, or one where centroid noise happens to exceed 5") must NOT be
        flagged MOVING_UNKNOWN just because a bright/persistent neighbour
        sits within MOVING_CONE_ARCSEC — as long as that neighbour is still
        detected at its own spot in THIS frame (i.e. it plainly didn't move
        anywhere; it was never the thing "shifting"). The area has prior
        coverage but no history at the target's own position, so this must
        fall through to UNKNOWN — not MOVING_UNKNOWN/SPACE_DEBRIS.
        """
        neighbour_offset_deg = 15.0 / 3600.0
        neighbour_ra, neighbour_dec = _RA, _DEC + neighbour_offset_deg

        target    = _make_source(catalog_name=None, elongation=1.2)
        neighbour = _make_source(ra=neighbour_ra, dec=neighbour_dec, catalog_name=None)
        # The neighbour's own past detections, at the exact spot it's still
        # sitting at in this frame — this is what used to trip up
        # _is_position_shifted() for the unrelated `target` source.
        neighbour_hist = _make_hist_source(ra=neighbour_ra, dec=neighbour_dec)

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": [neighbour_hist]}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [target, neighbour], [target, neighbour], _FRAME_META)

        target_anomaly = next(a for a in result if a["ra"] == pytest.approx(_RA) and a["dec"] == pytest.approx(_DEC))
        assert target_anomaly["anomaly_type"] == "UNKNOWN"


class TestDetectSaturatedArtifacts:
    """
    Regression coverage for docs/ISSUES.md #1/#2: a saturated, uncatalogued
    detection is treated as a bright-star/subtraction artifact and never
    reported as MOVING_UNKNOWN/SPACE_DEBRIS/UNKNOWN. A saturated but
    catalog-matched (MPC) source is unaffected — it's a legitimate bright
    object, just without a usable magnitude (see photometry.py).
    """

    def _far_hist_source(self) -> dict:
        offset_arcsec = 15.0  # within MOVING_CONE_ARCSEC, beyond MATCH_CONE_ARCSEC
        offset_deg = offset_arcsec / 3600.0
        return _make_hist_source(ra=_RA, dec=_DEC + offset_deg)

    async def test_saturated_unmatched_shifted_source_suppressed(self):
        """Would otherwise be MOVING_UNKNOWN — must be suppressed instead."""
        source = _make_source(catalog_name=None, elongation=1.2, saturated=True)
        far = self._far_hist_source()

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": [far]}
            mock_cov.return_value = {"0": []}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []

    async def test_saturated_unmatched_covered_source_suppressed(self):
        """Would otherwise be UNKNOWN — must be suppressed instead."""
        source = _make_source(catalog_name=None, saturated=True)

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []

    async def test_saturated_mpc_matched_source_still_classified(self):
        """A saturated but MPC-matched source is a legitimate bright asteroid — must still fire."""
        source = _make_source(
            catalog_name="MPC",
            catalog_id="2019 XY3",
            object_type="ASTEROID",
            saturated=True,
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
        assert result[0]["anomaly_type"] == "ASTEROID"


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
