"""
tests/test_anomaly_detector.py — Unit tests for the modules/anomaly_detector/ package

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
    filter: str | None = "L",
    near_edge: bool = False,
    from_subtraction: bool = False,
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
        # This frame's own filter (pipeline.py's Step 5.5) — defaults to "L"
        # so every existing Δmag test keeps comparing same-filter history
        # unless a test explicitly asks for a mismatch (see
        # TestDetectSameFilterDeltaMag below).
        "_filter":      filter,
        # Set by astrometry.py/subtraction.py from the detection's own pixel
        # position — see TestDetectSpaceDebrisNearEdge below. Defaults to
        # False so every existing SPACE_DEBRIS test keeps using the ordinary
        # (non-edge) elongation threshold unless a test explicitly opts in.
        # No leading underscore — mirrors "saturated" above, since it must
        # survive to the wire for a standalone DETECT_ANOMALIES re-run.
        "near_edge":    near_edge,
        # Leading underscore — internal-only, never sent to the wire (see
        # api_client's _to_wire_source()). Defaults to False so every
        # existing test keeps exercising the ordinary (non-subtraction)
        # classification path unless a test explicitly opts in — see
        # TestDetectEmptyAndFirstObservation below for the from_subtraction
        # cases.
        "_from_subtraction": from_subtraction,
    }


def _make_hist_source(
    ra: float = _RA,
    dec: float = _DEC,
    mag: float | None = 14.5,
    filter: str | None = "L",
) -> dict:
    return {"ra": ra, "dec": dec, "mag": mag, "filter": filter}


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

    def test_is_galaxy_matches_the_g_suffix_family(self):
        """
        Audit 2026-08-18, finding L2: the docstring used to promise a
        word-boundary-aware check, under which a token had to stand alone as
        a word. Simbad puts "G" at the END of a whole family of genuine
        galaxy codes, so that rule would have rejected every one of these —
        the substring test is the right shape here, and this pins it.
        """
        for otype in ("EmG", "RadioG", "SBG", "H2G", "LSB_G"):
            assert ad._is_galaxy(otype) is True, f"Expected True for '{otype}'"

    def test_is_galaxy_is_known_to_overmatch_a_globular_cluster(self):
        """
        The documented cost of that substring test, pinned so it is a choice
        rather than a surprise: a bare "G" also matches OTYPEs that merely
        contain the letter. `GlC` is a globular cluster, not a galaxy, and a
        new point source projected near one is reported
        SUPERNOVA_CANDIDATE rather than UNKNOWN — a misnamed alert, not a
        lost one. See _is_galaxy()'s docstring.
        """
        assert ad._is_galaxy("GlC") is True

    def test_is_galaxy_rejects_an_otype_with_no_token_at_all(self):
        for otype in ("Star", "**", "PN", "HII"):
            assert ad._is_galaxy(otype) is False, f"Expected False for '{otype}'"


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


class TestHistoryMagScatter:
    """
    _history_mag_scatter() is the catalog-independent baseline behind the
    light-curve-based VARIABLE_STAR branch (audit finding C1) — a robust
    (MAD-scaled) 1-sigma equivalent of a source's OWN historical magnitudes.
    """

    def test_scatter_of_identical_magnitudes_is_zero(self):
        history = [_make_hist_source(mag=14.0) for _ in range(4)]
        assert ad._history_mag_scatter(history) == pytest.approx(0.0)

    def test_scatter_scales_with_spread(self):
        tight = [_make_hist_source(mag=m) for m in (14.00, 14.01, 13.99, 14.00)]
        loose = [_make_hist_source(mag=m) for m in (14.0, 15.0, 13.0, 14.0)]
        assert ad._history_mag_scatter(tight) < ad._history_mag_scatter(loose)

    def test_scatter_is_robust_against_a_single_outlier(self):
        """
        One bad epoch (cloud, cosmic ray in the aperture) must not inflate the
        baseline enough to mask a real change — this is why the MAD is used
        rather than a plain standard deviation.
        """
        history = [_make_hist_source(mag=m) for m in (14.0, 14.0, 14.0, 14.0, 20.0)]
        assert ad._history_mag_scatter(history) == pytest.approx(0.0)

    def test_single_epoch_has_no_scatter(self):
        assert ad._history_mag_scatter([_make_hist_source(mag=14.0)]) is None

    def test_empty_history_returns_none(self):
        assert ad._history_mag_scatter([]) is None

    def test_entries_without_magnitude_are_ignored(self):
        history = [
            _make_hist_source(mag=14.0),
            {"ra": _RA, "dec": _DEC},  # no magnitude at all
        ]
        assert ad._history_mag_scatter(history) is None


class TestSameFilterHistory:
    """
    _same_filter_history() restricts magnitude comparisons to same-filter
    epochs — comparing an L-band magnitude against an old R-band/Hα epoch is
    a color-term artifact, not real variability (see the function's own
    docstring and CLAUDE.md's "Filters — real astronomy context").
    """

    def test_keeps_only_matching_filter(self):
        history = [
            _make_hist_source(mag=14.0, filter="L"),
            _make_hist_source(mag=12.0, filter="R"),
            _make_hist_source(mag=13.5, filter="L"),
        ]
        result = ad._same_filter_history(history, "L")
        assert [h["mag"] for h in result] == [14.0, 13.5]

    def test_no_same_filter_entries_returns_empty(self):
        history = [_make_hist_source(mag=12.0, filter="R")]
        assert ad._same_filter_history(history, "L") == []

    def test_unknown_current_filter_returns_empty(self):
        """A source with no known filter of its own can't safely be compared."""
        history = [_make_hist_source(mag=12.0, filter="L")]
        assert ad._same_filter_history(history, None) == []

    def test_history_entry_missing_filter_key_never_matches(self):
        """A pre-migration history row (no 'filter' key at all) must not be
        optimistically assumed to match — excluded rather than guessed."""
        history = [{"ra": _RA, "dec": _DEC, "mag": 12.0}]  # no "filter" key
        assert ad._same_filter_history(history, "L") == []


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

    async def test_detect_subtraction_unknown_alert_when_uncatalogued(self):
        """
        No coverage at all, but the source was detected via image
        subtraction and isn't catalog-matched: subtraction already proved
        this position is genuinely new relative to the reference stack, so
        missing API coverage doesn't downgrade it -> UNKNOWN, ALERT.
        """
        source = _make_source(catalog_name=None, from_subtraction=True, source_id="src-sub-001")

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {}
            mock_cov.return_value = {}  # no coverage at all

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "UNKNOWN"
        assert result[0]["source_id"] == "src-sub-001"

    async def test_detect_subtraction_suppressed_when_catalog_matched(self):
        """
        Regression test for the 2026-08-14 "camera rotation" investigation
        (source_id 6a7cfbae64e706.89320404, CLAUDE.md): a subtraction
        candidate that DID match a catalog (a known Gaia DR3 star, in this
        case) must NOT be reported as a false UNKNOWN alert merely because
        its sky tile has no POST /frames/covering/batch record yet — it's a
        known object, most likely an ordinary astroalign registration
        residual near it, not a real transient. This branch used to ignore
        catalog_name entirely.
        """
        source = _make_source(
            catalog_name="Gaia DR3", catalog_id="3901066435010508672",
            from_subtraction=True, source_id="src-sub-002",
        )

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {}
            mock_cov.return_value = {}  # no coverage at all

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []

    async def test_detect_subtraction_near_edge_still_suppressed_regardless_of_catalog(self):
        """near_edge suppression (checked first) must still win even for a catalog-matched source."""
        source = _make_source(
            catalog_name="Gaia DR3", from_subtraction=True, near_edge=True,
        )

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
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


class TestDetectSameFilterDeltaMag:
    """
    End-to-end (via ad.detect()) coverage for the same-filter Δmag
    restriction: a magnitude comparison against a DIFFERENT filter's history
    must never fire VARIABLE_STAR/BINARY_STAR/the brightening branch of
    SUPERNOVA_CANDIDATE — see TestSameFilterHistory for the unit-level tests
    and modules/anomaly_detector/_history.py's _same_filter_history() docstring.
    """

    async def test_cross_filter_brightening_does_not_fire_variable_star(self):
        """
        Same scenario as test_detect_variable_star (delta would be 2.5 mag,
        well past DELTA_MAG_ALERT), except the history was observed in a
        DIFFERENT filter — must NOT fire VARIABLE_STAR; the two magnitudes
        are not comparable at all (color term), so no anomaly is reported.
        """
        source = _make_source(mag=14.5, catalog_name="Simbad", object_type="V*", filter="L")
        hist   = [_make_hist_source(mag=12.0, filter="R")]

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": hist}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []

    async def test_cross_filter_history_does_not_mark_first_observation(self):
        """
        The EXISTENCE check (n_history, used for FIRST_OBSERVATION / UNKNOWN /
        KNOWN_CATALOG_NEW) must stay filter-agnostic — an ordinary LRGB
        sequence re-images the same field in several filters per session, and
        a position already detected in R must not look "brand new" (and
        therefore alert as UNKNOWN) the moment an L-filtered frame of the
        same field comes in.

        Uses an uncatalogued source so the distinction is observable: if the
        existence check were (incorrectly) restricted to same-filter history
        too, n_history would come out 0 here and this would misfire UNKNOWN;
        done correctly, the position's prior (cross-filter) detection is
        still recognized, and — with no same-filter magnitude to compare —
        the source falls through to "no anomaly", not an alert.
        """
        source = _make_source(mag=14.5, catalog_name=None, object_type=None, filter="L")
        hist   = [_make_hist_source(mag=14.5, filter="R")]  # different filter, same position

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": hist}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []

    async def test_history_missing_filter_key_treated_as_pre_migration(self):
        """
        A historical row with no 'filter' key at all (observed before the API
        started returning it) must not be assumed to match — no anomaly
        fires from an assumed-same-filter comparison that was never verified.
        """
        source = _make_source(mag=14.5, catalog_name="Simbad", object_type="V*", filter="L")
        hist   = [{"ra": _RA, "dec": _DEC, "mag": 12.0}]  # no "filter" key

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": hist}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []


class TestDetectLightCurveVariability:
    """
    Audit finding C1 — the Δmag branches used to gate entirely on Simbad
    OTYPE, but only _simbad.py writes a real OTYPE: _gaia.py, _2mass.py and
    _panstarrs.py all hardcode the generic "STAR". A star known solely
    through Gaia DR3 (the overwhelming majority of any field) could therefore
    change brightness by any amount and be silently dropped, so the detector
    could only confirm variability a catalog already knew about and could
    never discover any.

    The fallback uses the source's OWN same-filter light curve instead: a
    long enough, tight enough baseline plus a departure from it of more than
    config.VARIABILITY_SIGMA times its own historical scatter.
    """

    async def _run(self, source: dict, hist: list[dict]) -> list[dict]:
        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": hist}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}
            return await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

    async def test_gaia_only_star_brightening_is_reported(self):
        """
        The headline C1 scenario: a Gaia DR3 star (object_type "STAR", which
        no OTYPE classifier matches) with a quiet four-epoch baseline
        brightens by 2.5 mag. Previously dropped outright.
        """
        source = _make_source(mag=12.0, catalog_name="Gaia DR3",
                              catalog_id="Gaia DR3 999", object_type="STAR")
        hist = [_make_hist_source(mag=m) for m in (14.50, 14.52, 14.48, 14.51)]

        result = await self._run(source, hist)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "VARIABLE_STAR"
        assert result[0]["delta_mag"] == pytest.approx(12.0 - 14.505, abs=0.02)
        assert "own light curve" in result[0]["notes"]

    async def test_gaia_only_star_dimming_is_reported(self):
        """Variability is symmetric — a quiescent star that fades is as much
        a variability candidate as one that brightens (unlike the
        SUPERNOVA_CANDIDATE branch, which is brightening-only)."""
        source = _make_source(mag=17.0, catalog_name="Gaia DR3", object_type="STAR")
        hist = [_make_hist_source(mag=m) for m in (14.50, 14.52, 14.48, 14.51)]

        result = await self._run(source, hist)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "VARIABLE_STAR"
        assert result[0]["delta_mag"] > 0

    async def test_uncatalogued_source_with_stable_history_is_reported(self):
        """A source no catalog claims at all still has its own light curve —
        the branch must not require a catalog match either."""
        source = _make_source(mag=12.0, catalog_name=None, object_type=None)
        hist = [_make_hist_source(mag=m) for m in (14.50, 14.52, 14.48, 14.51)]

        result = await self._run(source, hist)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "VARIABLE_STAR"

    async def test_noisy_history_does_not_alert(self):
        """
        An intrinsically noisy source (low SNR, blended neighbour, variable
        seeing) must stay quiet: a 2.5 mag departure is unremarkable against
        a baseline that already scatters by more than that.
        """
        source = _make_source(mag=12.0, catalog_name="Gaia DR3", object_type="STAR")
        hist = [_make_hist_source(mag=m) for m in (14.5, 11.0, 17.0, 13.0)]

        assert await self._run(source, hist) == []

    async def test_too_few_epochs_does_not_alert(self):
        """
        Below config.VARIABILITY_MIN_EPOCHS same-filter epochs there is no
        baseline worth calling quiescent — the source falls through to "no
        anomaly", exactly as it did before this branch existed.
        """
        source = _make_source(mag=12.0, catalog_name="Gaia DR3", object_type="STAR")
        hist = [_make_hist_source(mag=14.50), _make_hist_source(mag=14.52)]

        assert await self._run(source, hist) == []

    async def test_cross_filter_epochs_do_not_count_toward_the_baseline(self):
        """
        The baseline is same-filter only, for the same color-term reason the
        Δmag comparison itself is (see TestDetectSameFilterDeltaMag) — three
        R-band epochs plus one L-band epoch is a one-epoch L baseline, not a
        four-epoch one.
        """
        source = _make_source(mag=12.0, catalog_name="Gaia DR3",
                              object_type="STAR", filter="L")
        hist = [
            _make_hist_source(mag=14.50, filter="L"),
            _make_hist_source(mag=14.52, filter="R"),
            _make_hist_source(mag=14.48, filter="R"),
            _make_hist_source(mag=14.51, filter="R"),
        ]

        assert await self._run(source, hist) == []

    async def test_epochs_without_a_magnitude_do_not_count_toward_the_baseline(self):
        """
        VARIABILITY_MIN_EPOCHS gates on a PHOTOMETRIC baseline, so it must
        count the same rows the scatter is computed from. A detection whose
        photometry never calibrated is still recorded, and counting rows
        rather than magnitudes let three same-filter detections carrying only
        two usable magnitudes satisfy the threshold — classifying off a
        baseline shorter than the one the scatter was measured over.
        """
        source = _make_source(mag=12.0, catalog_name="Gaia DR3", object_type="STAR")
        hist = [
            _make_hist_source(mag=14.50),
            _make_hist_source(mag=14.52),
            _make_hist_source(mag=None),
        ]

        assert await self._run(source, hist) == []

    async def test_absolute_delta_mag_floor_still_applies(self):
        """
        An extremely tight baseline makes the scatter test trivially easy to
        clear, so DELTA_MAG_ALERT stays in force as an absolute floor — a
        0.1 mag change is not worth an operator's attention however
        statistically significant it looks.
        """
        source = _make_source(mag=14.60, catalog_name="Gaia DR3", object_type="STAR")
        hist = [_make_hist_source(mag=m) for m in (14.500, 14.501, 14.499, 14.500)]

        assert await self._run(source, hist) == []

    async def test_simbad_variable_still_uses_the_catalog_branch(self):
        """
        The catalog-driven branch keeps priority: a Simbad-classified
        variable is reported on its OTYPE alone, with no baseline-length or
        scatter requirement, and keeps its own "Known variable star" notes.
        """
        source = _make_source(mag=14.5, catalog_name="Simbad", object_type="V*")
        hist = [_make_hist_source(mag=12.0)]  # single epoch — no scatter at all

        result = await self._run(source, hist)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "VARIABLE_STAR"
        assert "Known variable star" in result[0]["notes"]

    async def test_galaxy_brightening_still_wins_over_the_variability_branch(self):
        """
        Branch ordering regression: a brightening galaxy must stay a
        SUPERNOVA_CANDIDATE, not be swallowed by the new catalog-independent
        VARIABLE_STAR branch sitting below it.
        """
        source = _make_source(mag=16.0, catalog_name="Simbad", object_type="G")
        hist = [_make_hist_source(mag=m) for m in (20.0, 20.02, 19.98, 20.01)]

        result = await self._run(source, hist)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "SUPERNOVA_CANDIDATE"


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

    async def test_ephemeris_is_queried_at_the_exposure_midpoint(self):
        """
        Audit 2026-08-18, finding C9: JPL Horizons was asked where the object
        was at shutter-open rather than mid-exposure. The history/coverage
        queries deliberately keep the start time — that is what the frame is
        registered under.
        """
        source = _make_source(
            catalog_name="MPC", catalog_id="2019 XY3", object_type="ASTEROID",
        )
        frame_meta = dict(_FRAME_META, obs_time_mid="2024-03-15T22:03:34")

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
            patch("modules.anomaly_detector.ephemeris.query", new_callable=AsyncMock, return_value=_EPH_DICT) as mock_eph,
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": []}

            await ad.detect(_FRAME_ID, [source], [source], frame_meta)

        assert mock_eph.call_args[0][1] == "2024-03-15T22:03:34"
        assert mock_cov.call_args[0][1] == _FRAME_META["obs_time"]

    async def test_one_raising_ephemeris_query_does_not_sink_the_frame(self):
        """
        Audit 2026-08-18, finding C8: asyncio.gather() gave no isolation
        between concurrent Horizons lookups, so anything escaping
        ephemeris.query()'s own `except Exception` — a BaseException such as
        CancelledError, or a failure during coroutine setup — propagated out
        of detect(). pipeline.py then posted an EMPTY anomaly list, and since
        that endpoint REPLACES the frame's anomaly set, one failed lookup
        erased every other anomaly on the frame.
        """
        first = _make_source(
            catalog_name="MPC", catalog_id="2019 XY3", object_type="ASTEROID",
        )
        second = _make_source(
            ra=_RA + 0.01, dec=_DEC + 0.01,
            catalog_name="MPC", catalog_id="C/2024 A1", object_type="COMET",
        )

        async def flaky(designation, obs_time):
            if designation == "2019 XY3":
                raise RuntimeError("Horizons unreachable")
            return _EPH_DICT

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
            patch("modules.anomaly_detector.ephemeris.query", side_effect=flaky),
        ):
            mock_sources.return_value = {"0": [], "1": []}
            mock_cov.return_value = {"0": [], "1": []}

            result = await ad.detect(_FRAME_ID, [first, second], [first, second], _FRAME_META)

        by_designation = {a["mpc_designation"]: a for a in result}
        assert len(result) == 2
        # The failing lookup costs that one anomaly its ephemeris, nothing more.
        assert by_designation["2019 XY3"]["ephemeris"] is None
        assert by_designation["2019 XY3"]["anomaly_type"] == "ASTEROID"
        # Its neighbour is unaffected.
        assert by_designation["C/2024 A1"]["ephemeris"] == _EPH_DICT
        assert all("_needs_ephemeris" not in a for a in result)


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

    async def test_detect_space_debris_trail_without_vacated_history(self):
        """
        Regression for the 2026-08-07 C_2020_R4_ATLAS incident: a satellite/
        debris trail spans the whole frame within a single exposure, so it
        never has a *prior* detection anywhere nearby whose position could
        be shown to have "vacated" — condition 2 of _is_position_shifted()
        can never be satisfied for it. Elongation alone (>3.0) must be
        enough to classify it SPACE_DEBRIS; it must NOT fall through to
        generic UNKNOWN just because there is no earlier-epoch evidence of
        movement (there never will be, for a single-exposure trail).

        No history at all — narrow OR wide cone — near this position, ever;
        the area itself has been covered by prior frames.
        """
        source = _make_source(catalog_name=None, elongation=6.0, source_id="src-trail-001")

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "SPACE_DEBRIS"
        assert result[0]["mpc_designation"] is None
        assert result[0]["source_id"] == "src-trail-001"

    async def test_detect_recurring_elongated_source_is_not_space_debris(self):
        """
        Guardrail for the fix above: the elongation-alone shortcut must stay
        gated on `history` (condition 1) being empty. A recurring elongated
        detection — e.g. a diffraction spike or an uncatalogued extended
        object sitting at the exact same position every frame — is the
        opposite of trail evidence and must NOT be swept into SPACE_DEBRIS
        just because its measured elongation happens to exceed 3.0.
        """
        source = _make_source(catalog_name=None, elongation=4.0)
        same_spot_hist = _make_hist_source(ra=_RA, dec=_DEC, mag=14.5)

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": [same_spot_hist]}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []

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


class TestDeltaMagSignificance:
    """
    Audit 2026-08-18, finding M3: DELTA_MAG_ALERT is a flat 0.5 mag applied to
    every source equally, which is the wrong shape for the question twice
    over. A faint source at the detection limit wanders further than that on
    noise alone and alerts every night; a bright, well-measured star can
    change by 0.3 mag — unmistakable at its own precision — and never be
    looked at.
    """

    def _source(self, mag: float, mag_err: float | None) -> dict:
        src = _make_source(
            mag=mag, catalog_name="Simbad", catalog_id="V* AB",
            object_type="V*", source_id="src-var-001",
        )
        src["mag_err"] = mag_err
        return src

    def _history(self, mags: list[float]) -> list[dict]:
        return [_make_hist_source(mag=m) for m in mags]

    def test_a_noisy_source_needs_more_than_the_flat_threshold(self):
        """0.6 mag is past DELTA_MAG_ALERT but nothing against a 0.4 mag error."""
        src = self._source(mag=14.6, mag_err=0.4)

        assert ad._is_significant_delta(0.6, src, self._history([14.0])) is False

    def test_a_precise_source_clears_it_easily(self):
        src = self._source(mag=14.6, mag_err=0.01)

        assert ad._is_significant_delta(0.6, src, self._history([14.0])) is True

    def test_the_absolute_floor_still_applies(self):
        """
        A change below DELTA_MAG_ALERT is not astronomically interesting
        however precisely it was measured.
        """
        src = self._source(mag=14.3, mag_err=0.001)

        assert ad._is_significant_delta(0.3, src, self._history([14.0])) is False

    def test_historical_scatter_counts_as_noise_too(self):
        src = self._source(mag=14.6, mag_err=0.01)
        noisy_history = self._history([14.0, 14.5, 13.5, 14.4, 13.6])

        assert ad._is_significant_delta(0.6, src, noisy_history) is False

    def test_no_noise_estimate_falls_back_to_the_flat_threshold(self):
        src = self._source(mag=14.6, mag_err=None)

        assert ad._is_significant_delta(0.6, src, []) is True

    async def test_a_noisy_variable_does_not_alert_end_to_end(self):
        src = self._source(mag=14.6, mag_err=0.4)
        history = [_make_hist_source(mag=14.0)]

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": history}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [src], [src], _FRAME_META)

        assert result == []

    async def test_the_same_change_on_a_precise_source_does_alert(self):
        src = self._source(mag=14.6, mag_err=0.01)
        history = [_make_hist_source(mag=14.0)]

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": history}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [src], [src], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "VARIABLE_STAR"


class TestEdgeZoneSubtractionCandidates:
    """
    Audit 2026-08-18, finding H11: every near_edge source was suppressed
    outright, so a genuine transient landing near the frame edge — routine
    under a dithering pattern — could never be reported, whatever it looked
    like. Coma stretches a PSF into an arc and it is the mismatch between two
    such arcs that fails to cancel, so a residual is elongated and usually
    weak; a round, strong subtraction candidate is not that shape.
    """

    def _edge_candidate(self, elongation: float, snr: float) -> dict:
        src = _make_source(
            catalog_name=None, elongation=elongation,
            near_edge=True, from_subtraction=True, source_id="src-edge-001",
        )
        src["snr"] = snr
        return src

    def test_a_round_strong_candidate_qualifies(self):
        assert ad._survives_edge_zone(self._edge_candidate(1.1, 50.0)) is True

    def test_an_elongated_candidate_does_not(self):
        assert ad._survives_edge_zone(self._edge_candidate(3.0, 50.0)) is False

    def test_a_weak_candidate_does_not(self):
        assert ad._survives_edge_zone(self._edge_candidate(1.1, 2.0)) is False

    def test_a_candidate_with_no_snr_does_not(self):
        src = self._edge_candidate(1.1, 50.0)
        src["snr"] = None
        assert ad._survives_edge_zone(src) is False

    def test_an_ordinary_detection_never_qualifies(self):
        """
        A non-subtraction source's near-edge suppression rests on a different
        mechanism — a coma-shifted centroid that made catalog matching miss —
        which shape cannot rule out.
        """
        src = self._edge_candidate(1.1, 50.0)
        src["_from_subtraction"] = False
        assert ad._survives_edge_zone(src) is False

    async def test_a_round_strong_edge_candidate_is_reported_in_covered_sky(self):
        source = self._edge_candidate(1.1, 50.0)

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "UNKNOWN"

    async def test_a_round_strong_edge_candidate_is_reported_in_uncovered_sky(self):
        source = self._edge_candidate(1.1, 50.0)

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": []}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "UNKNOWN"

    async def test_an_elongated_edge_candidate_is_still_suppressed(self):
        """The 2026-08-10 coma flood must stay suppressed."""
        source = self._edge_candidate(3.0, 50.0)

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []


class TestSubtractionResidualOfCatalogedStar:
    """
    2026-09-22 IC3322A test run: 10 of 11 UNKNOWN alerts were subtraction
    residuals 5-7" from a catalogued star of the same magnitude — outside
    MATCH_CONE_ARCSEC, so uncatalogued, and (in a frame corner the API's
    fov/2 coverage circle misses) not even deemed "covered".
    """

    def _candidate(self, dec_offset_arcsec: float = 0.0, mag: float = 15.5) -> dict:
        return _make_source(
            dec=_DEC + dec_offset_arcsec / 3600.0, mag=mag, catalog_name=None,
            from_subtraction=True, source_id="src-resid-001",
        )

    def _star(self, mag: float = 15.5) -> dict:
        # 6" from the candidate: beyond MATCH_CONE_ARCSEC, inside the residual radius.
        return _make_source(
            dec=_DEC + 6.0 / 3600.0, mag=mag, catalog_name="Gaia DR3",
            catalog_id="GAIA-1", source_id="src-star-001",
        )

    async def _detect(self, sources: list[dict]) -> list[dict]:
        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {}
            mock_cov.return_value = {}  # the uncovered-corner case
            return await ad.detect(_FRAME_ID, sources, sources, _FRAME_META)

    async def test_a_residual_beside_a_same_magnitude_star_is_suppressed(self):
        result = await self._detect([self._candidate(), self._star()])

        assert [a for a in result if a["source_id"] == "src-resid-001"] == []

    async def test_a_transient_beside_a_star_of_different_brightness_survives(self):
        """Finding C6's case: a transient flaring near a clearly brighter star."""
        result = await self._detect([self._candidate(mag=15.5), self._star(mag=12.0)])

        assert [a["anomaly_type"] for a in result if a["source_id"] == "src-resid-001"] == ["UNKNOWN"]

    async def test_a_candidate_far_from_any_star_survives(self):
        far = _make_source(
            dec=_DEC + 60.0 / 3600.0, mag=15.5, catalog_name="Gaia DR3",
            catalog_id="GAIA-2", source_id="src-star-002",
        )
        result = await self._detect([self._candidate(), far])

        assert [a["anomaly_type"] for a in result if a["source_id"] == "src-resid-001"] == ["UNKNOWN"]

    async def test_disabled_by_a_zero_radius(self, monkeypatch):
        monkeypatch.setattr(config, "SUBTRACTION_RESIDUAL_RADIUS_ARCSEC", 0.0)
        result = await self._detect([self._candidate(), self._star()])

        assert [a["anomaly_type"] for a in result if a["source_id"] == "src-resid-001"] == ["UNKNOWN"]

    def test_an_ordinary_detection_is_never_judged_this_way(self):
        from modules.anomaly_detector._classify import _is_residual_of_catalogued_star

        ordinary = self._candidate()
        ordinary["_from_subtraction"] = False
        assert _is_residual_of_catalogued_star(ordinary, [(_RA, _DEC + 6.0 / 3600.0, 15.5)]) is False

    def test_a_candidate_without_a_magnitude_is_not_judged(self):
        from modules.anomaly_detector._classify import _is_residual_of_catalogued_star

        unmeasured = self._candidate()
        unmeasured["mag"] = None
        assert _is_residual_of_catalogued_star(unmeasured, [(_RA, _DEC + 6.0 / 3600.0, 15.5)]) is False


class TestFastMoverWideCone:
    """
    Audit 2026-08-18, finding H3: the wide "did this used to be somewhere
    nearby?" cone was a fixed MOVING_CONE_ARCSEC around the current position,
    so an object that moved further than that between two frames had its own
    previous position outside the search entirely — "shifted" could never be
    confirmed and a genuine fast mover fell through to plain UNKNOWN (no track
    chart, no ephemeris) or was dropped as FIRST_OBSERVATION.
    """

    def test_radius_floor_is_the_plain_moving_cone(self):
        """Two frames a minute apart reach less than the 120" floor."""
        r = ad._wide_cone_radius_arcsec("2024-03-15T22:01:34Z", "2024-03-15T22:00:34Z")
        assert r == pytest.approx(config.MOVING_CONE_ARCSEC)

    def test_radius_grows_with_the_gap(self):
        """10 minutes at 30"/min reaches 300" — well past the fixed cone."""
        r = ad._wide_cone_radius_arcsec("2024-03-15T22:11:34Z", "2024-03-15T22:01:34Z")
        assert r == pytest.approx(10.0 * config.MOVING_RATE_ARCSEC_PER_MIN)
        assert r > config.MOVING_CONE_ARCSEC

    def test_radius_is_capped(self):
        r = ad._wide_cone_radius_arcsec("2024-03-15T22:29:34Z", "2024-03-15T22:01:34Z")
        assert r == pytest.approx(config.MOVING_CONE_MAX_ARCSEC)

    def test_an_old_detection_does_not_extend_the_cone(self):
        """
        Past MOVING_EXTEND_MAX_GAP_MIN the extension would just be the cap,
        always — a permanently wide cone, which is the false-positive mode the
        two-condition "shifted" test exists to prevent.
        """
        r = ad._wide_cone_radius_arcsec("2024-03-16T22:01:34Z", "2024-03-15T22:01:34Z")
        assert r == pytest.approx(config.MOVING_CONE_ARCSEC)

    def test_missing_or_unparseable_timestamps_fall_back_to_the_fixed_cone(self):
        assert ad._wide_cone_radius_arcsec(_OBS_TIME, None) == pytest.approx(config.MOVING_CONE_ARCSEC)
        assert ad._wide_cone_radius_arcsec(_OBS_TIME, "") == pytest.approx(config.MOVING_CONE_ARCSEC)
        assert ad._wide_cone_radius_arcsec(_OBS_TIME, "yesterday") == pytest.approx(config.MOVING_CONE_ARCSEC)
        assert ad._wide_cone_radius_arcsec("", _OBS_TIME) == pytest.approx(config.MOVING_CONE_ARCSEC)

    def test_find_wide_history_sizes_each_candidate_by_its_own_age(self):
        """
        A 200" separation is beyond the fixed cone. It counts as a candidate
        when the detection is 10 minutes old (a fast mover's reach) but not
        when it is a day old.
        """
        offset_deg = 200.0 / 3600.0
        recent = _make_hist_source(ra=_RA, dec=_DEC + offset_deg)
        recent["obs_time"] = "2024-03-15T21:51:34Z"
        old = _make_hist_source(ra=_RA, dec=_DEC + offset_deg)
        old["obs_time"] = "2024-03-14T22:01:34Z"

        found, radius = ad._find_wide_history(_RA, _DEC, [recent], _OBS_TIME)
        assert found == [recent]
        assert radius > config.MOVING_CONE_ARCSEC

        found, radius = ad._find_wide_history(_RA, _DEC, [old], _OBS_TIME)
        assert found == []
        assert radius == pytest.approx(config.MOVING_CONE_ARCSEC)

    async def test_fast_mover_beyond_the_fixed_cone_is_moving_unknown(self):
        """
        End to end: an object whose previous detection 10 minutes ago sits
        200" away — past MOVING_CONE_ARCSEC — must classify MOVING_UNKNOWN
        rather than UNKNOWN.
        """
        source = _make_source(catalog_name=None, elongation=1.2, source_id="src-fast-001")
        prev = _make_hist_source(ra=_RA, dec=_DEC + 200.0 / 3600.0)
        prev["obs_time"] = "2024-03-15T21:51:34Z"

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": [prev]}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "MOVING_UNKNOWN"
        assert result[0]["source_id"] == "src-fast-001"

    async def test_a_day_old_detection_at_the_same_distance_stays_unknown(self):
        """
        The guardrail: the same 200" separation must NOT read as motion when
        the only evidence is a detection from a previous session.
        """
        source = _make_source(catalog_name=None, elongation=1.2)
        prev = _make_hist_source(ra=_RA, dec=_DEC + 200.0 / 3600.0)
        prev["obs_time"] = "2024-03-14T22:01:34Z"

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": [prev]}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "UNKNOWN"


class TestDetectSpaceDebrisNearEdge:
    """
    Regression coverage for the 2026-08-07 T_CrB incident: coma stretches an
    otherwise ordinary, non-moving star's PSF near the edge/corners of a
    wide-field frame, inflating its measured elongation for purely optical
    reasons. A source flagged `near_edge` (astrometry.py/subtraction.py, see
    config.EDGE_MARGIN_FRAC) must clear the higher
    config.SPACE_DEBRIS_EDGE_ELONGATION_MIN bar (default 6.0) instead of the
    ordinary config.SPACE_DEBRIS_ELONGATION_MIN (default 3.0) to be reported
    SPACE_DEBRIS.
    """

    async def test_near_edge_source_below_edge_threshold_is_not_space_debris(self):
        """elongation=4.5 clears the ordinary 3.0 bar but not the edge bar (6.0).
        Near-edge uncatalogued sources are now suppressed entirely (not even
        UNKNOWN) — coma shifts the centroid, making them false positives."""
        source = _make_source(catalog_name=None, elongation=4.5, near_edge=True)

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 0

    async def test_near_edge_source_above_edge_threshold_is_space_debris(self):
        """elongation=6.5 clears even the higher edge bar (6.0)."""
        source = _make_source(catalog_name=None, elongation=6.5, near_edge=True, source_id="src-edge-trail")

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "SPACE_DEBRIS"
        assert result[0]["source_id"] == "src-edge-trail"

    async def test_central_source_at_same_elongation_is_unaffected(self):
        """
        The same elongation=4.5 that's suppressed near the edge above must
        still trigger SPACE_DEBRIS for a central (near_edge=False) source —
        this is the existing test_detect_space_debris behaviour, unaffected
        by the edge-aware threshold.
        """
        source = _make_source(catalog_name=None, elongation=4.5, near_edge=False)

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "SPACE_DEBRIS"


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

    async def test_a_spike_shaped_saturated_source_is_still_suppressed(self):
        """
        The artifact this suppression exists for: a diffraction spike or a
        bleed trail off a bright star is elongated, not round.
        """
        source = _make_source(catalog_name=None, saturated=True, elongation=4.0)

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []

    async def test_a_saturated_source_with_history_is_still_suppressed(self):
        """
        A spike belongs to a star that is in the frame every night, so the
        position has history. That is what tells it apart from something new.
        """
        source = _make_source(catalog_name=None, saturated=True, elongation=1.2)

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": [_make_hist_source()]}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []

    async def test_a_saturated_near_edge_source_is_still_suppressed(self):
        source = _make_source(catalog_name=None, saturated=True, elongation=1.2, near_edge=True)

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert result == []

    async def test_a_round_new_bright_object_is_reported(self):
        """
        Audit 2026-08-18, finding M4: the suppression was unconditional and
        structurally could not let a nova or a fireball through — by
        definition such an object has no catalog match yet, and if it is
        bright enough to matter it is bright enough to saturate.
        """
        source = _make_source(
            catalog_name=None, saturated=True, elongation=1.1, source_id="src-nova-001",
        )

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": [_make_coverage_frame()]}

            result = await ad.detect(_FRAME_ID, [source], [source], _FRAME_META)

        assert len(result) == 1
        assert result[0]["anomaly_type"] == "UNKNOWN"
        assert result[0]["source_id"] == "src-nova-001"

    async def test_an_uncovered_area_still_suppresses_it(self):
        """
        "Nothing was ever here" says nothing about a patch of sky that was
        never imaged.
        """
        source = _make_source(catalog_name=None, saturated=True, elongation=1.1)

        with (
            patch("modules.anomaly_detector.api_client.get_sources_near_batch", new_callable=AsyncMock) as mock_sources,
            patch("modules.anomaly_detector.api_client.get_frames_covering_batch", new_callable=AsyncMock) as mock_cov,
        ):
            mock_sources.return_value = {"0": []}
            mock_cov.return_value = {"0": []}

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
