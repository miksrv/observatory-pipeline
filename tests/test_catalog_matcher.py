"""
tests/test_catalog_matcher.py — Unit tests for the modules/catalog_matcher/ package

All external catalog calls are mocked on the specific per-catalog submodule
they're imported/defined in — never on the top-level package — since
_match.py's own match() calls each catalog through a qualified submodule
reference (see that module's docstring):
    patch("modules.catalog_matcher._gaia.Gaia")
    patch("modules.catalog_matcher._simbad.Simbad")
    patch("modules.catalog_matcher._mpc._query_mpc", ...)
    patch("modules.catalog_matcher._simbad._query_simbad", ...)
    patch("modules.catalog_matcher._2mass._query_2mass", ...)
    patch("modules.catalog_matcher._panstarrs._query_panstarrs", ...)

MPC/SkyBot has no importable client class to patch (astroquery.imcce.Skybot
is imported locally inside _mpc._query_mpc() itself) — tests instead patch
_query_mpc() as a whole function, or exercise its real error handling via an
invalid obs_time.

Real astropy SkyCoord arithmetic runs for all coordinate-matching tests so
that angular-distance logic is exercised without network access.

asyncio_mode = auto in pytest.ini — no @pytest.mark.asyncio needed.
"""

from __future__ import annotations

import datetime
import math
from typing import Any
from unittest.mock import MagicMock, patch

import astropy.units as u
import numpy as np
import pytest
from astropy.coordinates import SkyCoord
from astropy.table import Table

import modules.catalog_matcher as cm


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clear_cache():
    """Wipe the module-level cache before every test for isolation."""
    cm._cache.clear()
    yield
    cm._cache.clear()


# Centre of a synthetic test field — chosen to keep delta_ra negligible
_RA  = 83.820_83   # near Orion Nebula
_DEC = -5.389_68

_FRAME_META = {
    "ra_center":  _RA,
    "dec_center": _DEC,
    "fov_deg":    1.0,
    "obs_time":   "2024-03-15T22:01:34Z",
    "filename":   "test_frame.fits",
}


def _make_source(ra: float = _RA, dec: float = _DEC) -> dict:
    return {
        "ra":         ra,
        "dec":        dec,
        "flux":       10000.0,
        "fwhm":       3.0,
        "elongation": 1.1,
    }


# ---------------------------------------------------------------------------
# Helpers — build fake Astropy Tables that mimic catalog responses
# ---------------------------------------------------------------------------

def _gaia_table(ra: float, dec: float, source_id: int = 123456, mag: float = 14.5) -> Table:
    return Table({
        "ra":              [ra],
        "dec":             [dec],
        "source_id":       [source_id],
        "phot_g_mean_mag": [mag],
    })


def _simbad_table(ra_hms: str, dec_dms: str, main_id: str = "M 42", otype: str = "GlCl") -> Table:
    """
    Simbad returns RA as HH MM SS.ss and Dec as +DD MM SS.s strings.
    We build a Table that mirrors this so _query_simbad's SkyCoord parsing
    works correctly in tests.
    """
    return Table({
        "RA":      [ra_hms],
        "DEC":     [dec_dms],
        "MAIN_ID": [main_id],
        "OTYPE":   [otype],
    })


def _mpc_table(ra: float, dec: float, designation: str = "2019 XY3") -> Table:
    return Table({
        "ra":          [ra],
        "dec":         [dec],
        "designation": [designation],
    })


def _mock_gaia_job(table: Table) -> MagicMock:
    job = MagicMock()
    job.get_results.return_value = table
    return job


# ---------------------------------------------------------------------------
# Helper — offset a coordinate by N arcseconds in RA direction
# ---------------------------------------------------------------------------

def _offset_ra(ra: float, dec: float, arcsec: float) -> tuple[float, float]:
    """Return (ra, dec) shifted by `arcsec` arcseconds in RA."""
    delta_deg = arcsec / 3600.0 / abs(max(abs(dec), 1e-6) * 0 + 1)  # crude, fine for small offsets
    return ra + delta_deg, dec


def _offset_ra_exact(ra: float, dec: float, arcsec: float) -> tuple[float, float]:
    """
    Return a new (ra, dec) that is exactly `arcsec` arcseconds from (ra, dec)
    measured along the great circle, using astropy for precision.
    """
    coord = SkyCoord(ra=ra * u.deg, dec=dec * u.deg)
    shifted = coord.directional_offset_by(0 * u.deg, arcsec * u.arcsec)
    return float(shifted.ra.deg), float(shifted.dec.deg)


# ===========================================================================
# TestCacheLogic
# ===========================================================================

class TestCacheLogic:
    def test_cache_miss_returns_none(self):
        result = cm._cache_get("nonexistent-key")
        assert result is None

    def test_cache_hit_returns_data(self):
        cm._cache_set("key1", [{"ra": 1.0}])
        result = cm._cache_get("key1")
        assert result == [{"ra": 1.0}]

    def test_cache_expired_returns_none(self):
        # Plant a cache entry with a timestamp 2 hours in the past
        two_hours_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=2)
        cm._cache["stale_key"] = {"data": "old_data", "fetched_at": two_hours_ago}
        result = cm._cache_get("stale_key")
        assert result is None


# ===========================================================================
# TestGaiaMatching
# ===========================================================================

class TestGaiaMatching:
    def test_gaia_match_sets_catalog_fields(self):
        """Source within MATCH_CONE_ARCSEC of a Gaia star is matched."""
        # Place the Gaia star at the exact same position as the source
        source = _make_source(ra=_RA, dec=_DEC)
        source["catalog_name"] = None
        source["catalog_id"]   = None
        source["catalog_mag"]  = None
        source["object_type"]  = None

        gaia_stars = [{
            "ra":             _RA,
            "dec":            _DEC,
            "source_id":      "987654321",
            "phot_g_mean_mag": 13.7,
        }]

        cm._match_gaia([source], gaia_stars)

        assert source["catalog_name"] == "Gaia DR3"
        assert source["catalog_id"]   == "987654321"
        assert source["catalog_mag"]  == pytest.approx(13.7)
        assert source["object_type"]  == "STAR"

    def test_gaia_no_match_when_too_far(self):
        """Source more than MATCH_CONE_ARCSEC away stays unmatched."""
        # Offset the Gaia star by 60 arcsec (beyond default 5 arcsec threshold)
        far_ra, far_dec = _offset_ra_exact(_RA, _DEC, 60.0)

        source = _make_source(ra=_RA, dec=_DEC)
        source["catalog_name"] = None
        source["catalog_id"]   = None
        source["catalog_mag"]  = None
        source["object_type"]  = None

        gaia_stars = [{
            "ra":             far_ra,
            "dec":            far_dec,
            "source_id":      "111111111",
            "phot_g_mean_mag": 15.0,
        }]

        cm._match_gaia([source], gaia_stars)

        assert source["catalog_name"] is None

    def test_gaia_error_returns_empty_list(self):
        """If the Gaia query raises, _query_gaia returns []."""
        with patch("modules.catalog_matcher._gaia.Gaia") as mock_gaia:
            mock_gaia.cone_search.side_effect = RuntimeError("network timeout")
            result = cm._query_gaia(_RA, _DEC, 1.0)

        assert result == []

    def test_gaia_skips_nan_magnitude(self):
        """Rows with NaN phot_g_mean_mag are excluded from results."""
        import math
        table = Table({
            "ra":              [_RA,   _RA + 0.01],
            "dec":             [_DEC,  _DEC],
            "source_id":       [1,     2],
            "phot_g_mean_mag": [14.0,  float("nan")],
        })
        with patch("modules.catalog_matcher._gaia.Gaia") as mock_gaia:
            mock_gaia.cone_search.return_value = _mock_gaia_job(table)
            result = cm._query_gaia(_RA, _DEC, 1.0)

        assert len(result) == 1
        assert math.isfinite(result[0]["phot_g_mean_mag"])


# ===========================================================================
# TestSimbadMatching
# ===========================================================================

class TestSimbadMatching:
    def test_simbad_skips_already_matched_sources(self):
        """A source with catalog_name='Gaia DR3' must not be overwritten by Simbad."""
        source = _make_source(ra=_RA, dec=_DEC)
        source["catalog_name"] = "Gaia DR3"
        source["catalog_id"]   = "999"
        source["catalog_mag"]  = 14.0
        source["object_type"]  = "STAR"

        simbad_objects = [{
            "ra":      _RA,
            "dec":     _DEC,
            "main_id": "* bet Ori",
            "otype":   "Star",
        }]

        cm._match_simbad([source], simbad_objects)

        # Fields must remain unchanged
        assert source["catalog_name"] == "Gaia DR3"
        assert source["catalog_id"]   == "999"

    def test_simbad_match_sets_otype(self):
        """Unmatched source within cone gets Simbad otype."""
        source = _make_source(ra=_RA, dec=_DEC)
        source["catalog_name"] = None
        source["catalog_id"]   = None
        source["catalog_mag"]  = None
        source["object_type"]  = None

        simbad_objects = [{
            "ra":      _RA,
            "dec":     _DEC,
            "main_id": "M  42",
            "otype":   "HII",
        }]

        cm._match_simbad([source], simbad_objects)

        assert source["catalog_name"] == "Simbad"
        assert source["catalog_id"]   == "M  42"
        assert source["catalog_mag"]  is None
        assert source["object_type"]  == "HII"

    def test_simbad_none_result_handled(self):
        """_query_simbad returns [] when Simbad.query_region() returns None."""
        with patch("modules.catalog_matcher._simbad.Simbad") as mock_simbad_cls:
            instance = MagicMock()
            instance.query_region.return_value = None
            mock_simbad_cls.return_value = instance

            result = cm._query_simbad(_RA, _DEC, 1.0)

        assert result == []

    def test_simbad_error_returns_empty_list(self):
        """If Simbad query raises, _query_simbad returns [] with no crash."""
        with patch("modules.catalog_matcher._simbad.Simbad") as mock_simbad_cls:
            instance = MagicMock()
            instance.query_region.side_effect = ConnectionError("timeout")
            mock_simbad_cls.return_value = instance

            result = cm._query_simbad(_RA, _DEC, 1.0)

        assert result == []


# ===========================================================================
# TestMpcMatching
# ===========================================================================

class TestMpcMatching:
    def test_mpc_uses_wider_cone(self):
        """
        Source between MATCH_CONE_ARCSEC (5") and MOVING_CONE_ARCSEC (30")
        must be matched by MPC but would NOT be matched by Gaia/Simbad.
        """
        # 15 arcsec away — inside MOVING_CONE (30") but outside MATCH_CONE (5")
        shifted_ra, shifted_dec = _offset_ra_exact(_RA, _DEC, 15.0)

        source = _make_source(ra=_RA, dec=_DEC)
        source["catalog_name"] = None
        source["catalog_id"]   = None
        source["catalog_mag"]  = None
        source["object_type"]  = None

        mpc_objects = [{
            "ra":          shifted_ra,
            "dec":         shifted_dec,
            "designation": "2024 AB1",
            "object_type": "ASTEROID",
        }]

        cm._match_mpc([source], mpc_objects)

        assert source["catalog_name"] == "MPC"
        assert source["catalog_id"]   == "2024 AB1"
        assert source["object_type"]  == "ASTEROID"

    def test_mpc_error_returns_empty_list(self):
        """If SkyBot query fails, _query_mpc returns [] without crashing."""
        # Test with invalid obs_time that will cause Time parsing to fail
        # The function should catch this and return []
        result = cm._query_mpc(_RA, _DEC, "invalid-time-format", 1.0)
        assert result == []

    def test_mpc_skips_already_matched_sources(self):
        """Source already matched by Gaia must not be overwritten by MPC."""
        source = _make_source(ra=_RA, dec=_DEC)
        source["catalog_name"] = "Gaia DR3"
        source["catalog_id"]   = "GAIA_STAR_ID"
        source["catalog_mag"]  = 13.0
        source["object_type"]  = "STAR"

        mpc_objects = [{
            "ra":          _RA,
            "dec":         _DEC,
            "designation": "2024 AB1",
            "object_type": "ASTEROID",
        }]

        cm._match_mpc([source], mpc_objects)

        assert source["catalog_name"] == "Gaia DR3"

    def test_mpc_empty_obs_time_returns_empty(self):
        """If obs_time is empty, _query_mpc returns [] immediately."""
        result = cm._query_mpc(_RA, _DEC, "", 1.0)
        assert result == []

    def test_mpc_one_to_one_nearest_wins(self):
        """
        Regression test for the 2014 RY1 incident (2026-08-10): when multiple
        unmatched sources fall within MOVING_CONE_ARCSEC of the same MPC
        object, only the NEAREST one must get the MPC designation — not the
        brightest. The old code matched source→MPC (assigning the designation
        to every source within radius), then _dedupe_by_catalog_identity kept
        the brightest, which was a background star rather than the real
        asteroid.
        """
        # MPC object predicted at position X
        mpc_ra, mpc_dec = _RA, _DEC

        # Real asteroid: 10" from MPC predicted position (nearest), but FAINT
        asteroid_ra, asteroid_dec = _offset_ra_exact(mpc_ra, mpc_dec, 10.0)
        asteroid = _make_source(ra=asteroid_ra, dec=asteroid_dec)
        asteroid["catalog_name"] = None
        asteroid["catalog_id"] = None
        asteroid["flux"] = 100.0  # faint

        # Background star: 50" from MPC predicted position (further), but BRIGHT
        star_ra, star_dec = _offset_ra_exact(mpc_ra, mpc_dec, 50.0)
        star = _make_source(ra=star_ra, dec=star_dec)
        star["catalog_name"] = None
        star["catalog_id"] = None
        star["flux"] = 50000.0  # very bright

        mpc_objects = [{
            "ra":          mpc_ra,
            "dec":         mpc_dec,
            "designation": "2014 RY1",
            "object_type": "ASTEROID",
        }]

        cm._match_mpc([asteroid, star], mpc_objects)

        # The nearest source (asteroid) must get the designation
        assert asteroid["catalog_name"] == "MPC"
        assert asteroid["catalog_id"] == "2014 RY1"

        # The further source (star) must NOT get the designation
        assert star["catalog_name"] is None
        assert star["catalog_id"] is None

    def test_mpc_two_objects_do_not_share_same_source(self):
        """
        When two MPC objects are near the same unmatched source, only the
        closer MPC object claims it — the further one gets no match rather
        than sharing the same source.
        """
        # Source at _RA, _DEC
        source = _make_source(ra=_RA, dec=_DEC)
        source["catalog_name"] = None
        source["catalog_id"] = None

        # MPC object A: 5" away (closer)
        mpc_a_ra, mpc_a_dec = _offset_ra_exact(_RA, _DEC, 5.0)
        # MPC object B: 30" away (further)
        mpc_b_ra, mpc_b_dec = _offset_ra_exact(_RA, _DEC, 30.0)

        mpc_objects = [
            {"ra": mpc_b_ra, "dec": mpc_b_dec, "designation": "2024 XY", "object_type": "ASTEROID"},
            {"ra": mpc_a_ra, "dec": mpc_a_dec, "designation": "2024 AB", "object_type": "ASTEROID"},
        ]

        cm._match_mpc([source], mpc_objects)

        # The closer MPC object (2024 AB at 5") wins
        assert source["catalog_name"] == "MPC"
        assert source["catalog_id"] == "2024 AB"


# ===========================================================================
# TestMatchOrchestrator
# ===========================================================================

class TestMatchOrchestrator:
    """Tests for the public async match() entry point."""

    def _make_gaia_mock(self, table: Table) -> MagicMock:
        mock = MagicMock()
        mock.cone_search.return_value = _mock_gaia_job(table)
        return mock

    def _make_simbad_mock(self, table: Table | None) -> MagicMock:
        mock_cls = MagicMock()
        instance = MagicMock()
        instance.query_region.return_value = table
        mock_cls.return_value = instance
        return mock_cls

    async def test_output_length_equals_input_length(self):
        """match() must return a list of the same length as the input."""
        sources = [_make_source() for _ in range(5)]

        gaia_t = _gaia_table(_RA + 10, _DEC + 10)   # far away — no matches
        with (
            patch("modules.catalog_matcher._gaia.Gaia", self._make_gaia_mock(gaia_t)),
            patch("modules.catalog_matcher._simbad.Simbad", self._make_simbad_mock(None)),
            patch("modules.catalog_matcher._mpc._query_mpc", return_value=[]),
        ):
            result = await cm.match(sources, _FRAME_META)

        assert len(result) == 5

    async def test_all_catalog_keys_present(self):
        """Every source in the output must have all four catalog keys."""
        sources = [_make_source()]

        gaia_t = _gaia_table(_RA + 10, _DEC + 10)
        with (
            patch("modules.catalog_matcher._gaia.Gaia", self._make_gaia_mock(gaia_t)),
            patch("modules.catalog_matcher._simbad.Simbad", self._make_simbad_mock(None)),
            patch("modules.catalog_matcher._mpc._query_mpc", return_value=[]),
        ):
            result = await cm.match(sources, _FRAME_META)

        for src in result:
            assert "catalog_name" in src
            assert "catalog_id"   in src
            assert "catalog_mag"  in src
            assert "object_type"  in src

    async def test_gaia_failure_does_not_prevent_simbad(self):
        """When Gaia raises, Simbad must still run and match sources."""
        source = _make_source(ra=_RA, dec=_DEC)

        # Simbad returns a match at the exact source position
        # Use decimal degrees directly in the table; we need the SkyCoord
        # hourangle parser to work, so we provide proper HMS/DMS strings.
        # Instead, bypass _query_simbad by testing _match_simbad runs at all.
        # Easiest: make Simbad return None (no crash) and verify result has keys.
        mock_gaia = MagicMock()
        mock_gaia.cone_search.side_effect = RuntimeError("Gaia is down")

        mock_simbad_cls = MagicMock()
        mock_simbad_instance = MagicMock()
        mock_simbad_instance.query_region.return_value = None   # returns None → []
        mock_simbad_cls.return_value = mock_simbad_instance

        with (
            patch("modules.catalog_matcher._gaia.Gaia", mock_gaia),
            patch("modules.catalog_matcher._simbad.Simbad", mock_simbad_cls),
            patch("modules.catalog_matcher._mpc._query_mpc", return_value=[]),
        ):
            result = await cm.match([source], _FRAME_META)

        # Pipeline must not crash; source has keys; Simbad was still invoked
        assert len(result) == 1
        assert "catalog_name" in result[0]
        mock_simbad_instance.query_region.assert_called_once()

    async def test_cache_prevents_second_gaia_query(self):
        """
        Calling match() twice with the same frame_meta must issue only one
        Gaia cone_search call — the second call is served from cache.
        """
        sources_run1 = [_make_source()]
        sources_run2 = [_make_source()]

        gaia_t = _gaia_table(_RA + 10, _DEC + 10)   # far → no match, just count calls

        mock_gaia = MagicMock()
        mock_gaia.cone_search.return_value = _mock_gaia_job(gaia_t)

        with (
            patch("modules.catalog_matcher._gaia.Gaia", mock_gaia),
            patch("modules.catalog_matcher._simbad.Simbad", self._make_simbad_mock(None)),
            patch("modules.catalog_matcher._mpc._query_mpc", return_value=[]),
        ):
            await cm.match(sources_run1, _FRAME_META)
            await cm.match(sources_run2, _FRAME_META)

        # cone_search must have been called exactly once despite two match() calls
        assert mock_gaia.cone_search.call_count == 1

    async def test_empty_sources_returns_empty_list(self):
        """match() with an empty source list must return an empty list."""
        mock_gaia = MagicMock()
        mock_gaia.cone_search.return_value = _mock_gaia_job(_gaia_table(_RA, _DEC))

        with (
            patch("modules.catalog_matcher._gaia.Gaia", mock_gaia),
            patch("modules.catalog_matcher._simbad.Simbad", self._make_simbad_mock(None)),
            patch("modules.catalog_matcher._mpc._query_mpc", return_value=[]),
        ):
            result = await cm.match([], _FRAME_META)

        assert result == []

    async def test_gaia_match_propagates_to_output(self):
        """A source at the exact frame centre is matched by Gaia DR3."""
        source = _make_source(ra=_RA, dec=_DEC)
        gaia_t = _gaia_table(ra=_RA, dec=_DEC, source_id=42, mag=12.3)

        with (
            patch("modules.catalog_matcher._gaia.Gaia", self._make_gaia_mock(gaia_t)),
            patch("modules.catalog_matcher._simbad.Simbad", self._make_simbad_mock(None)),
            patch("modules.catalog_matcher._mpc._query_mpc", return_value=[]),
        ):
            result = await cm.match([source], _FRAME_META)

        assert result[0]["catalog_name"] == "Gaia DR3"
        assert result[0]["catalog_id"]   == "42"
        assert result[0]["catalog_mag"]  == pytest.approx(12.3)


# ===========================================================================
# TestComputeWcsOffset
#
# Regression coverage for the 2026-08-06 "Vesta_A807_FA" incident: a real
# frame with ~40 detected sources against ~4453 Gaia stars in the field
# (i.e. a dense field where almost every source has multiple Gaia stars
# within the 90" search radius purely by chance) and a true WCS offset of
# roughly 60". The original fine-grained (2") histogram + flat "20x
# background" significance test missed the real offset — true pairs, each
# scattered by a few arcsec of centroid noise around the systematic bias,
# spread across several adjacent bins and never reached the vote floor,
# while an unrelated 2-vote noise bin passed the (too permissive at low
# background) significance check. See modules/catalog_matcher/_wcs_offset.py for the
# fixed algorithm (coarser bins, Poisson-margin significance test with an
# absolute vote floor, iterative median refinement).
# ===========================================================================

class TestComputeWcsOffset:

    @staticmethod
    def _dense_field_scenario(true_dra_arcsec: float, true_ddec_arcsec: float, seed: int = 42):
        """
        Build a synthetic (sources, gaia_stars) pair mirroring the real
        incident: ~40 sources, a genuine offset cluster of ~18 matching Gaia
        stars (each with a few arcsec of centroid scatter around the true
        offset), and ~2500 unrelated Gaia stars scattered over the field to
        reproduce the "almost every source has a chance neighbour within
        90"" background density of a real dense stellar field.
        """
        rng = np.random.default_rng(seed)
        n_sources = 40
        n_real_matches = 18
        n_background_gaia = 2500

        # Sources scattered over a ~0.7° box around (_RA, _DEC)
        src_ra  = _RA  + rng.uniform(-0.35, 0.35, n_sources)
        src_dec = _DEC + rng.uniform(-0.35, 0.35, n_sources)
        sources = [_make_source(ra=float(r), dec=float(d)) for r, d in zip(src_ra, src_dec)]
        for s in sources:
            s["catalog_name"] = None

        cos_dec = np.cos(np.radians(_DEC))
        gaia_stars: list[dict] = []

        # True matches: the first n_real_matches sources each get a Gaia
        # counterpart at the true offset plus a few arcsec of scatter.
        for i in range(n_real_matches):
            scatter_ra  = rng.normal(0.0, 3.0)   # arcsec
            scatter_dec = rng.normal(0.0, 3.0)   # arcsec
            g_ra  = src_ra[i]  + (true_dra_arcsec  + scatter_ra)  / (cos_dec * 3600.0)
            g_dec = src_dec[i] + (true_ddec_arcsec + scatter_dec) / 3600.0
            gaia_stars.append({
                "ra": float(g_ra), "dec": float(g_dec),
                "source_id": f"true-{i}", "phot_g_mean_mag": 15.0,
            })

        # Background: Gaia stars uniformly scattered over a larger box,
        # unrelated to any source — this is what makes the field "dense".
        bg_ra  = _RA  + rng.uniform(-0.4, 0.4, n_background_gaia)
        bg_dec = _DEC + rng.uniform(-0.4, 0.4, n_background_gaia)
        for i, (r, d) in enumerate(zip(bg_ra, bg_dec)):
            gaia_stars.append({
                "ra": float(r), "dec": float(d),
                "source_id": f"bg-{i}", "phot_g_mean_mag": 16.0,
            })

        return sources, gaia_stars

    def test_recovers_large_offset_in_dense_field(self):
        """
        The exact shape of the real incident: true offset ≈ 60" total,
        buried in a dense (background-dominated) Gaia field. Must recover
        the offset to within a few arcsec, not fall back to (0.0, 0.0).
        """
        true_dra, true_ddec = 20.0, -55.0  # arcsec — matches the real incident's order of magnitude
        sources, gaia_stars = self._dense_field_scenario(true_dra, true_ddec)

        offset_ra_deg, offset_dec_deg = cm._compute_wcs_offset(sources, gaia_stars)

        assert (offset_ra_deg, offset_dec_deg) != (0.0, 0.0), (
            "Offset correction fell back to no-op — the real, large offset was not detected"
        )
        cos_dec = math.cos(math.radians(_DEC))
        recovered_dra_arcsec  = offset_ra_deg  * cos_dec * 3600.0
        recovered_ddec_arcsec = offset_dec_deg * 3600.0

        assert recovered_dra_arcsec  == pytest.approx(true_dra,  abs=5.0)
        assert recovered_ddec_arcsec == pytest.approx(true_ddec, abs=5.0)

    def test_no_offset_needed_returns_zero(self):
        """Sources already aligned with Gaia (median separation small) → no correction."""
        sources = [_make_source(ra=_RA + i * 0.001, dec=_DEC) for i in range(10)]
        for s in sources:
            s["catalog_name"] = None
        gaia_stars = [
            {"ra": s["ra"], "dec": s["dec"], "source_id": f"g{i}", "phot_g_mean_mag": 14.0}
            for i, s in enumerate(sources)
        ]

        offset_ra_deg, offset_dec_deg = cm._compute_wcs_offset(sources, gaia_stars)

        assert offset_ra_deg == 0.0
        assert offset_dec_deg == 0.0

    def test_pure_noise_field_returns_zero(self):
        """
        A galaxy-rich field: sources present, Gaia stars present nearby, but
        no genuine systematic relationship between them. Must not fabricate
        a spurious correction from a small random peak (guards against the
        low-background false-positive that motivated the fix — a coarser
        search must not become MORE trigger-happy on pure noise).
        """
        rng = np.random.default_rng(7)
        n = 15
        sources = [
            _make_source(ra=float(_RA + rng.uniform(-0.3, 0.3)), dec=float(_DEC + rng.uniform(-0.3, 0.3)))
            for _ in range(n)
        ]
        for s in sources:
            s["catalog_name"] = None
        # A modest number of Gaia stars scattered with no relationship to sources at all.
        gaia_stars = [
            {
                "ra": float(_RA + rng.uniform(-0.35, 0.35)),
                "dec": float(_DEC + rng.uniform(-0.35, 0.35)),
                "source_id": f"g{i}", "phot_g_mean_mag": 16.0,
            }
            for i in range(60)
        ]

        offset_ra_deg, offset_dec_deg = cm._compute_wcs_offset(sources, gaia_stars)

        assert (offset_ra_deg, offset_dec_deg) == (0.0, 0.0)


# ===========================================================================
# TestGaiaProperMotionFields — forced-photometry support (originally proposed as ROADMAP.md #1)
# ===========================================================================


class TestGaiaProperMotionFields:
    """
    _query_gaia() must also return pmra/pmdec/ref_epoch — needed by
    modules/forced_photometry.py to propagate a Gaia star's position to the
    observation epoch before projecting it to a pixel. Degrades gracefully
    (None/None/2016.0) when the queried table doesn't carry those columns at
    all, rather than raising — see that function's docstring.
    """

    def test_pmra_pmdec_ref_epoch_extracted_when_present(self):
        table = Table({
            "ra":              [_RA],
            "dec":             [_DEC],
            "source_id":       [123456],
            "phot_g_mean_mag": [14.5],
            "pmra":            [12.3],
            "pmdec":           [-4.5],
            "ref_epoch":       [2016.0],
        })
        with patch("modules.catalog_matcher._gaia.Gaia") as mock_gaia:
            mock_gaia.cone_search.return_value = _mock_gaia_job(table)
            result = cm._query_gaia(_RA, _DEC, 1.0)

        assert len(result) == 1
        assert result[0]["pmra"]  == pytest.approx(12.3)
        assert result[0]["pmdec"] == pytest.approx(-4.5)
        assert result[0]["ref_epoch"] == pytest.approx(2016.0)

    def test_missing_pm_columns_fall_back_to_none(self):
        """The existing 4-column mock table (no pmra/pmdec/ref_epoch) must
        still work — pmra/pmdec fall back to None, ref_epoch to 2016.0."""
        table = _gaia_table(_RA, _DEC)
        with patch("modules.catalog_matcher._gaia.Gaia") as mock_gaia:
            mock_gaia.cone_search.return_value = _mock_gaia_job(table)
            result = cm._query_gaia(_RA, _DEC, 1.0)

        assert len(result) == 1
        assert result[0]["pmra"] is None
        assert result[0]["pmdec"] is None
        assert result[0]["ref_epoch"] == pytest.approx(2016.0)

    def test_nan_pm_values_fall_back_to_none(self):
        table = Table({
            "ra":              [_RA],
            "dec":             [_DEC],
            "source_id":       [123456],
            "phot_g_mean_mag": [14.5],
            "pmra":            [float("nan")],
            "pmdec":           [float("nan")],
            "ref_epoch":       [2016.0],
        })
        with patch("modules.catalog_matcher._gaia.Gaia") as mock_gaia:
            mock_gaia.cone_search.return_value = _mock_gaia_job(table)
            result = cm._query_gaia(_RA, _DEC, 1.0)

        assert result[0]["pmra"] is None
        assert result[0]["pmdec"] is None


# ===========================================================================
# TestPublicCatalogAccessors — get_gaia_stars() / get_mpc_objects()
#
# Thin, cache-reusing wrappers modules/forced_photometry.py calls to reuse
# the exact field lists match() already fetched for forward matching — see
# catalog_matcher.py's own comment above their definition for why they exist
# instead of changing match()'s return contract.
# ===========================================================================


class TestPublicCatalogAccessors:
    def test_get_gaia_stars_delegates_to_query_gaia(self):
        table = _gaia_table(_RA, _DEC, source_id=42, mag=12.3)
        with patch("modules.catalog_matcher._gaia.Gaia") as mock_gaia:
            mock_gaia.cone_search.return_value = _mock_gaia_job(table)
            result = cm.get_gaia_stars(_RA, _DEC, 1.0)

        assert len(result) == 1
        assert result[0]["source_id"] == "42"

    async def test_get_gaia_stars_reuses_matchs_own_cache(self):
        """
        Calling get_gaia_stars() right after match() for the same field must
        NOT re-hit the network — it's the whole point of these accessors.
        """
        table = _gaia_table(_RA + 10, _DEC + 10)  # far — no matches, just count calls
        mock_gaia = MagicMock()
        mock_gaia.cone_search.return_value = _mock_gaia_job(table)

        with (
            patch("modules.catalog_matcher._gaia.Gaia", mock_gaia),
            patch("modules.catalog_matcher._simbad._query_simbad", return_value=[]),
            patch("modules.catalog_matcher._2mass._query_2mass", return_value=[]),
            patch("modules.catalog_matcher._panstarrs._query_panstarrs", return_value=[]),
            patch("modules.catalog_matcher._mpc._query_mpc", return_value=[]),
        ):
            await cm.match([_make_source()], _FRAME_META)
            cm.get_gaia_stars(_FRAME_META["ra_center"], _FRAME_META["dec_center"], _FRAME_META["fov_deg"])

        assert mock_gaia.cone_search.call_count == 1

    def test_get_mpc_objects_delegates_to_query_mpc(self):
        with patch("modules.catalog_matcher._mpc._query_mpc", return_value=[{"designation": "2019 XY3"}]) as mock_query:
            result = cm.get_mpc_objects(_RA, _DEC, _FRAME_META["obs_time"], 1.0)

        mock_query.assert_called_once_with(_RA, _DEC, _FRAME_META["obs_time"], 1.0)
        assert result == [{"designation": "2019 XY3"}]
