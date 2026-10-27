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
import os
from typing import Any
from unittest.mock import MagicMock, patch

import astropy.units as u
import numpy as np
import pytest
from astropy.coordinates import SkyCoord
from astropy.table import Table

import config
import modules.catalog_matcher as cm
from modules.catalog_matcher import _mpc, _sbident


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clear_cache(monkeypatch):
    """Wipe the module-level cache before every test for isolation."""
    cm._cache.clear()
    _mpc._down_until.clear()
    # A failed SkyBot query falls through to JPL's sb_ident: never over the
    # network in a test. TestJplSbident patches httpx.get itself.
    def _no_network(*args, **kwargs):
        raise RuntimeError("network disabled in tests")
    monkeypatch.setattr(_sbident.httpx, "get", _no_network)
    yield
    cm._cache.clear()
    _mpc._down_until.clear()


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


def _queried_radius_deg(mock_gaia) -> float:
    """The radius, in degrees, of the most recent Gaia cone_search call."""
    radius = mock_gaia.cone_search.call_args.kwargs.get("radius")
    if radius is None:
        radius = mock_gaia.cone_search.call_args.args[1]
    return radius.to(u.deg).value


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

    def test_the_ttl_is_read_at_call_time(self, monkeypatch):
        """Remote settings are applied after import; a changed TTL must take effect."""
        one_hour_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
        cm._cache["ttl_key"] = {"data": "x", "fetched_at": one_hour_ago}

        monkeypatch.setattr(config, "CACHE_TTL_HOURS", 2.0)
        assert cm._cache_get("ttl_key") == "x"
        monkeypatch.setattr(config, "CACHE_TTL_HOURS", 0.5)
        assert cm._cache_get("ttl_key") is None

    def test_a_disk_hit_keeps_the_files_own_age(self, tmp_path, monkeypatch):
        import importlib
        cache_mod = importlib.import_module("modules.catalog_matcher._cache")
        monkeypatch.setattr(config, "CATALOG_CACHE_DIR", str(tmp_path))
        monkeypatch.setattr(config, "CACHE_TTL_HOURS", 24.0)
        key = "disk_age_key"
        path = cache_mod._cache_file_path(key)
        with open(path, "w") as f:
            f.write("[1]")
        ten_hours_ago = datetime.datetime.now().timestamp() - 10 * 3600
        os.utime(path, (ten_hours_ago, ten_hours_ago))
        cache_mod._cache.pop(key, None)

        assert cache_mod._cache_get(key) == [1]
        age = datetime.datetime.now(datetime.timezone.utc) - cache_mod._cache[key]["fetched_at"]
        assert age > datetime.timedelta(hours=9)

    def test_a_pre_versioned_disk_entry_is_not_read_back(self, tmp_path, monkeypatch):
        """
        A disk file written before the cache's key meaning changed (M5) must
        not be served as a hit under a coinciding new key.
        """
        import importlib
        # The package re-exports the dict `_cache` under the same name as the
        # submodule, so fetch the module itself.
        cache_mod = importlib.import_module("modules.catalog_matcher._cache")

        monkeypatch.setattr(config, "CATALOG_CACHE_DIR", str(tmp_path))
        key = "gaia:10.0:20.0:1.1"
        legacy = tmp_path / (key.replace(":", "_") + ".json")
        legacy.write_text('[{"ra": 10.0, "dec": 20.0}]')
        cache_mod._cache.pop(key, None)

        assert cache_mod._cache_get(key) is None
        assert os.path.basename(cache_mod._cache_file_path(key)).startswith(
            cache_mod._CACHE_FORMAT_VERSION + "_"
        )


# ===========================================================================
# TestCacheTileGeometry — audit 2026-08-18, finding M5
#
# Every catalog queried around its frame's actual centre but cached under the
# centre rounded to 0.1 deg. A key is therefore a tile, not a point: a second
# frame sharing it can sit up to a tile diagonal away, and the circle drawn
# around the FIRST frame need not contain the second one's edge region at all.
# ===========================================================================


class TestCacheTileGeometry:
    def test_nearby_centres_share_one_tile(self):
        a = cm._cache_position(_RA, _DEC)
        b = cm._cache_position(_RA + 0.02, _DEC - 0.02)

        assert a == b

    def test_the_margin_covers_the_tile_half_diagonal(self):
        assert cm._cache_radius_margin_deg() == pytest.approx(0.1 * math.sqrt(2) / 2.0)

    def test_the_query_is_centred_on_the_tile_not_the_frame(self):
        """
        This is what makes the cached content a function of the key, which is
        what a key is supposed to mean.
        """
        table = _gaia_table(_RA, _DEC)
        with patch("modules.catalog_matcher._gaia.Gaia") as mock_gaia:
            mock_gaia.cone_search.return_value = _mock_gaia_job(table)
            cm._query_gaia(_RA, _DEC, 1.0)

        coord = mock_gaia.cone_search.call_args.args[0]
        key_ra, key_dec = cm._cache_position(_RA, _DEC)
        assert coord.ra.deg == pytest.approx(key_ra)
        assert coord.dec.deg == pytest.approx(key_dec)

    def test_every_frame_in_the_tile_is_inside_the_queried_circle(self):
        """
        The property the fix has to guarantee: the worst-placed frame sharing
        this key still has its whole footprint inside what was queried.
        """
        fov_deg = 1.0
        queried_radius = fov_deg * math.sqrt(2) / 2.0 + cm._cache_radius_margin_deg()

        # Worst case: a frame centre at the tile's own corner.
        offset = 0.05
        worst_case_distance = math.hypot(offset, offset) + fov_deg * math.sqrt(2) / 2.0

        assert worst_case_distance <= queried_radius + 1e-9

    def test_two_frames_in_one_tile_make_one_query(self):
        table = _gaia_table(_RA, _DEC)
        with patch("modules.catalog_matcher._gaia.Gaia") as mock_gaia:
            mock_gaia.cone_search.return_value = _mock_gaia_job(table)
            cm._query_gaia(_RA, _DEC, 1.0)
            cm._query_gaia(_RA + 0.02, _DEC - 0.02, 1.0)

        assert mock_gaia.cone_search.call_count == 1

    def test_the_fov_bucket_rounds_up(self):
        """
        Rounding to NEAREST is what made the cached cone stop being a function
        of its key: 0.96 and 1.04 deg share the bucket, and whichever arrived
        first decided how much sky was fetched. Rounding up guarantees the
        bucket is at least as wide as every frame in it.
        """
        assert cm._cache_fov_deg(0.96) == pytest.approx(1.0)
        assert cm._cache_fov_deg(1.04) == pytest.approx(1.1)
        assert cm._cache_fov_deg(1.0) == pytest.approx(1.0)

    def test_a_narrow_frame_does_not_shrink_the_cone_a_wider_one_reuses(self):
        """
        The failure this guards: the 0.96 deg frame fills the bucket, the 1.04
        deg frame hits it, and every Gaia star near the wider frame's edge is
        simply gone — purely because of which frame arrived first. Rounding up
        puts the two in different buckets, so the wider one queries its own
        cone instead of inheriting the narrower one's.
        """
        table = _gaia_table(_RA, _DEC)
        with patch("modules.catalog_matcher._gaia.Gaia") as mock_gaia:
            mock_gaia.cone_search.return_value = _mock_gaia_job(table)
            cm._query_gaia(_RA, _DEC, 0.96)
            narrow = _queried_radius_deg(mock_gaia)
            cm._query_gaia(_RA, _DEC, 1.04)
            wide = _queried_radius_deg(mock_gaia)

        assert mock_gaia.cone_search.call_count == 2
        assert wide > narrow

    def test_the_queried_cone_covers_the_widest_frame_in_its_bucket(self):
        """
        The property the bucket has to guarantee: a hit always covers its
        requester, however wide a frame the bucket admits.
        """
        fov_deg = 0.96
        bucket = cm._cache_fov_deg(fov_deg)
        table = _gaia_table(_RA, _DEC)

        with patch("modules.catalog_matcher._gaia.Gaia") as mock_gaia:
            mock_gaia.cone_search.return_value = _mock_gaia_job(table)
            cm._query_gaia(_RA, _DEC, fov_deg)
            queried_deg = _queried_radius_deg(mock_gaia)

        widest_half_diagonal = bucket * math.sqrt(2) / 2.0
        assert bucket >= fov_deg
        assert queried_deg >= widest_half_diagonal - 1e-9

    def test_two_frames_in_one_fov_bucket_make_one_query(self):
        table = _gaia_table(_RA, _DEC)
        with patch("modules.catalog_matcher._gaia.Gaia") as mock_gaia:
            mock_gaia.cone_search.return_value = _mock_gaia_job(table)
            cm._query_gaia(_RA, _DEC, 0.96)
            cm._query_gaia(_RA, _DEC, 0.92)

        assert mock_gaia.cone_search.call_count == 1

    def test_the_skybot_key_separates_frames_of_different_widths(self):
        """
        The MPC key omitted fov_deg entirely although its radius depends on
        it, so a narrow frame's result satisfied a wider frame at the same
        tile and epoch — whose own edge, moving margin included, had never
        been queried.
        """
        epoch = "2024-03-15T22:01:34"
        radii: list[float] = []

        class _FakeSkybot:
            @staticmethod
            def cone_search(coord, rad, epoch, cache=True):
                radii.append(rad.to(u.deg).value)
                return None

        with patch("astroquery.imcce.Skybot", _FakeSkybot):
            cm._query_mpc(_RA, _DEC, epoch, 0.5)
            cm._query_mpc(_RA, _DEC, epoch, 2.0)

        assert len(radii) == 2
        assert radii[1] > radii[0]


# ===========================================================================
# TestSkybotRetry
# ===========================================================================

def _skybot_table(ra: float, dec: float) -> Table:
    """One bright asteroid at (ra, dec), in SkyBot's column layout."""
    return Table({
        "Name": ["Krat"], "RA": [ra] * u.deg, "DEC": [dec] * u.deg,
        "Type": ["MB>Inner"], "V": [15.6],
    })


class TestSkybotRetry:
    """
    IMCCE intermittently answers a valid cone search with a non-VOTable body
    ("No table found"), and the same query succeeds moments later — on
    lookatstars (2026-10-12) one such blip cost a whole frame its MPC matching.
    """

    @pytest.fixture(autouse=True)
    def no_sleep(self, monkeypatch, tmp_path):
        # Each test starts from an empty disk cache too, not only the
        # in-process dict the module-level fixture clears.
        monkeypatch.setattr(config, "CATALOG_CACHE_DIR", str(tmp_path))
        self.sleeps: list[float] = []
        monkeypatch.setattr("modules.catalog_matcher._mpc.time.sleep", self.sleeps.append)

    def _fake_skybot(self, outcomes: list):
        calls: list[dict] = []

        class _FakeSkybot:
            @staticmethod
            def cone_search(coord, rad, epoch, cache=True):
                calls.append({"cache": cache})
                outcome = outcomes[len(calls) - 1]
                if isinstance(outcome, Exception):
                    raise outcome
                return outcome

        return _FakeSkybot, calls

    def test_a_transient_failure_is_retried(self, monkeypatch):
        monkeypatch.setattr(config, "SKYBOT_MAX_ATTEMPTS", 3)
        monkeypatch.setattr(config, "SKYBOT_RETRY_DELAY_SEC", 2.0)
        fake, calls = self._fake_skybot([ValueError("No table found"), _skybot_table(_RA, _DEC)])

        with patch("astroquery.imcce.Skybot", fake):
            objects = cm._query_mpc(_RA, _DEC, "2024-03-15T22:01:34", 1.0)

        assert [o["designation"] for o in objects] == ["Krat"]
        assert len(calls) == 2
        assert self.sleeps == [2.0]

    def test_astroquerys_http_cache_is_bypassed(self, monkeypatch):
        """
        astroquery caches the raw response before parsing it: with its default
        cache=True a malformed answer would be stored and re-read by every retry.
        """
        fake, calls = self._fake_skybot([ValueError("No table found"), _skybot_table(_RA, _DEC)])

        with patch("astroquery.imcce.Skybot", fake):
            cm._query_mpc(_RA, _DEC, "2024-03-15T22:01:34", 1.0)

        assert calls and all(c["cache"] is False for c in calls)

    def test_gives_up_after_the_configured_attempts_without_caching(self, monkeypatch):
        monkeypatch.setattr(config, "SKYBOT_MAX_ATTEMPTS", 3)
        monkeypatch.setattr(config, "SKYBOT_RETRY_DELAY_SEC", 1.0)
        fake, calls = self._fake_skybot([ValueError("No table found")] * 3 + [_skybot_table(_RA, _DEC)])

        with patch("astroquery.imcce.Skybot", fake):
            assert cm._query_mpc(_RA, _DEC, "2024-03-15T22:01:34", 1.0) == []
            assert len(calls) == 3
            assert self.sleeps == [1.0, 2.0]

            # The failure was not cached: the next caller (forced photometry,
            # a later frame at the same epoch) queries again and succeeds.
            objects = cm._query_mpc(_RA, _DEC, "2024-03-15T22:01:34", 1.0)

        assert [o["designation"] for o in objects] == ["Krat"]

    def test_one_attempt_disables_retrying(self, monkeypatch):
        monkeypatch.setattr(config, "SKYBOT_MAX_ATTEMPTS", 1)
        fake, calls = self._fake_skybot([ValueError("No table found"), _skybot_table(_RA, _DEC)])

        with patch("astroquery.imcce.Skybot", fake):
            assert cm._query_mpc(_RA, _DEC, "2024-03-15T22:01:34", 1.0) == []

        assert len(calls) == 1
        assert self.sleeps == []


# ===========================================================================
# TestMpcSourceFallback — SkyBot first, JPL sb_ident when it fails
# ===========================================================================

_EPOCH = "2024-03-15T22:01:34"
_JPL_OBJECT = {"ra": _RA, "dec": _DEC, "designation": "Krat", "object_type": "ASTEROID"}


class TestMpcSourceFallback:
    """
    SkyBot answered nginx 502 for days from 2026-10-23 and every frame
    ingested meanwhile lost its ASTEROID/COMET matching; JPL's Small-Body
    Identification API answers the same question from other infrastructure.
    """

    @pytest.fixture(autouse=True)
    def sources(self, monkeypatch, tmp_path):
        monkeypatch.setattr(config, "CATALOG_CACHE_DIR", str(tmp_path))
        monkeypatch.setattr(config, "MPC_SOURCES", ("skybot", "jpl"))
        monkeypatch.setattr(config, "MPC_SOURCE_COOLDOWN_SEC", 900.0)
        monkeypatch.setattr(config, "SKYBOT_MAX_ATTEMPTS", 1)
        self.calls: list[str] = []
        self.skybot: Any = ValueError("502 Bad Gateway")
        self.jpl: Any = [_JPL_OBJECT]

        def _answer(name, outcome):
            self.calls.append(name)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        monkeypatch.setattr(_mpc, "_query_skybot", lambda *a: _answer("skybot", self.skybot))
        monkeypatch.setattr(_sbident, "_query_sbident", lambda *a: _answer("jpl", self.jpl))

    def test_skybot_answers_and_jpl_is_not_asked(self):
        self.skybot = []
        assert cm._query_mpc(_RA, _DEC, _EPOCH, 1.0) == []
        assert self.calls == ["skybot"]

    def test_a_failed_skybot_query_falls_back_to_jpl(self):
        assert cm._query_mpc(_RA, _DEC, _EPOCH, 1.0) == [_JPL_OBJECT]
        assert self.calls == ["skybot", "jpl"]

    def test_the_fallback_answer_is_cached(self):
        cm._query_mpc(_RA, _DEC, _EPOCH, 1.0)
        assert cm._query_mpc(_RA, _DEC, _EPOCH, 1.0) == [_JPL_OBJECT]
        assert self.calls == ["skybot", "jpl"]

    def test_a_failed_source_is_asked_last_during_its_cooldown(self):
        cm._query_mpc(_RA, _DEC, _EPOCH, 1.0)
        self.calls.clear()

        cm._query_mpc(_RA, _DEC, "2024-03-15T22:11:34", 1.0)

        assert self.calls == ["jpl"]

    def test_a_down_source_is_still_asked_when_the_other_fails_too(self):
        cm._query_mpc(_RA, _DEC, _EPOCH, 1.0)
        self.calls.clear()
        self.skybot = [_JPL_OBJECT]
        self.jpl = RuntimeError("HTTP 503")

        assert cm._query_mpc(_RA, _DEC, "2024-03-15T22:11:34", 1.0) == [_JPL_OBJECT]
        assert self.calls == ["jpl", "skybot"]

    def test_a_source_that_answers_again_gets_its_place_back(self):
        cm._query_mpc(_RA, _DEC, _EPOCH, 1.0)
        self.skybot = []
        self.jpl = RuntimeError("HTTP 503")
        cm._query_mpc(_RA, _DEC, "2024-03-15T22:11:34", 1.0)
        self.calls.clear()
        self.jpl = []

        cm._query_mpc(_RA, _DEC, "2024-03-15T22:21:34", 1.0)

        assert self.calls == ["skybot"]

    def test_zero_cooldown_keeps_the_configured_order(self, monkeypatch):
        monkeypatch.setattr(config, "MPC_SOURCE_COOLDOWN_SEC", 0.0)
        cm._query_mpc(_RA, _DEC, _EPOCH, 1.0)
        self.calls.clear()

        cm._query_mpc(_RA, _DEC, "2024-03-15T22:11:34", 1.0)

        assert self.calls == ["skybot", "jpl"]

    def test_every_source_failing_returns_empty_and_caches_nothing(self):
        self.jpl = RuntimeError("HTTP 503")
        assert cm._query_mpc(_RA, _DEC, _EPOCH, 1.0) == []

        self.jpl = [_JPL_OBJECT]
        assert cm._query_mpc(_RA, _DEC, _EPOCH, 1.0) == [_JPL_OBJECT]

    def test_only_the_configured_sources_are_asked(self, monkeypatch):
        monkeypatch.setattr(config, "MPC_SOURCES", ("jpl", "horizons"))
        assert cm._query_mpc(_RA, _DEC, _EPOCH, 1.0) == [_JPL_OBJECT]
        assert self.calls == ["jpl"]

    def test_both_sources_get_the_same_cone(self, monkeypatch):
        seen: list[tuple] = []
        monkeypatch.setattr(_mpc, "_query_skybot", lambda *a: seen.append(a) or (_ for _ in ()).throw(ValueError("502")))
        monkeypatch.setattr(_sbident, "_query_sbident", lambda *a: seen.append(a) or [])

        cm._query_mpc(_RA, _DEC, _EPOCH, 1.0)

        assert len(seen) == 2
        assert seen[0][:3] == seen[1][:3]
        assert seen[0][3] == seen[1][3]


# ===========================================================================
# TestJplSbident — request and parsing of JPL's Small-Body Identification API
# ===========================================================================

_SBIDENT_FIELDS = [
    "Object name", "Astrometric RA (hh:mm:ss)", "Astrometric Dec (dd mm'ss\")",
    "Dist. from center RA (\")", "Dist. from center Dec (\")", "Dist. from center Norm (\")",
    "Visual magnitude (V)", "RA rate (\"/h)", "Dec rate (\"/h)",
]


def _sbident_payload(rows: list[list[str]]) -> dict:
    """An sb_ident 1.1 answer with `rows` as its second pass (abridged from a real one)."""
    return {
        "signature": {"source": "NASA/JPL Small-Body Identification API", "version": "1.1"},
        "n_first_pass": len(rows) + 3,
        "n_second_pass": len(rows),
        "fields_second": _SBIDENT_FIELDS,
        "data_second_pass": rows,
    }


class _FakeResponse:
    def __init__(self, status_code: int, payload: Any = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


class TestJplSbident:

    @pytest.fixture(autouse=True)
    def fake_http(self, monkeypatch):
        monkeypatch.setattr(config, "SITE_LAT", 52.5)
        monkeypatch.setattr(config, "SITE_LON", -60.25)
        monkeypatch.setattr(config, "SITE_ELEV", 300)
        monkeypatch.setattr(config, "MPC_MAG_LIMIT", 19.0)
        self.requests: list[dict] = []
        self.response = _FakeResponse(200, _sbident_payload([]))

        def _get(url, params, timeout):
            self.requests.append(params)
            return self.response

        monkeypatch.setattr(_sbident.httpx, "get", _get)

    def _query(self, ra=30.0, dec=12.0, radius=0.8):
        from astropy.time import Time
        return _sbident._query_sbident(ra, dec, radius, Time("2026-10-20T22:00:00.4"))

    def test_request_asks_for_the_integrated_second_pass_at_the_site(self):
        self._query()
        params = self.requests[0]
        assert params["two-pass"] == "true"
        assert (params["lat"], params["lon"], params["alt"]) == ("52.500000", "-60.250000", "0.3000")
        assert params["obs-time"] == "2026-10-20_22:00:00"
        assert params["fov-ra-center"] == "02-00-00.00"
        assert params["fov-dec-center"] == "12-00-00.0"
        assert params["vmag-lim"] == "19.0"

    def test_the_rectangle_contains_the_cone(self):
        """sb_ident counts the RA half-width in RA degrees, not degrees on the sky."""
        self._query(dec=60.0, radius=0.8)
        params = self.requests[0]
        assert float(params["fov-dec-hwidth"]) == pytest.approx(0.8)
        assert float(params["fov-ra-hwidth"]) == pytest.approx(1.6, rel=1e-4)

    def test_a_cone_over_the_pole_takes_every_ra(self):
        self._query(dec=89.5, radius=0.8)
        assert float(self.requests[0]["fov-ra-hwidth"]) == 180.0

    def test_a_negative_declination_is_written_with_m(self):
        self._query(ra=83.82083, dec=-5.38968)
        params = self.requests[0]
        assert params["fov-dec-center"] == "M05-23-22.8"
        assert params["fov-ra-center"] == "05-35-17.00"

    def test_rows_are_parsed_and_named_as_skybot_names_them(self):
        self.response = _FakeResponse(200, _sbident_payload([
            ["10252 Heidigraf (4164 T-1)", "01:59:35.88", "+11 48'12.0\"", "", "", "", "17.2", "", ""],
            ["C/2020 R4 (ATLAS)", "02:00:10.00", "+12 05'00.0\"", "", "", "", "15.0", "", ""],
        ]))

        objects = self._query()

        assert [(o["designation"], o["object_type"]) for o in objects] == [
            ("Heidigraf", "ASTEROID"), ("C/2020 R4", "COMET"),
        ]
        assert objects[0]["ra"] == pytest.approx(29.899500, abs=1e-6)
        assert objects[0]["dec"] == pytest.approx(11.803333, abs=1e-6)
        assert set(objects[0]) == {"ra", "dec", "designation", "object_type"}

    def test_a_negative_declination_is_parsed(self):
        self.response = _FakeResponse(200, _sbident_payload([
            ["(2014 RY1)", "05:35:17.00", "-05 23'22.8\"", "", "", "", "18.0", "", ""],
            ["(2014 RY2)", "05:35:17.00", "-00 30'00.0\"", "", "", "", "18.0", "", ""],
        ]))
        objects = self._query(ra=83.82083, dec=-5.38968, radius=5.0)
        assert objects[0]["dec"] == pytest.approx(-5.38967, abs=1e-5)
        assert objects[1]["dec"] == pytest.approx(-0.5)

    def test_objects_in_the_rectangles_corners_are_dropped(self):
        self.response = _FakeResponse(200, _sbident_payload([
            # 0.75 deg off in both axes: inside the 0.8 deg half-widths, outside the cone.
            ["(2020 AA1)", "02:03:04.00", "+12 45'00.0\"", "", "", "", "17.0", "", ""],
        ]))
        assert self._query() == []

    def test_an_empty_field_is_an_empty_answer(self):
        self.response = _FakeResponse(200, {
            "signature": {"version": "1.1"}, "warning": "no matching records",
        })
        assert self._query() == []

    def test_no_object_left_after_the_second_pass_is_an_empty_answer(self):
        self.response = _FakeResponse(200, {
            "signature": {"version": "1.1"}, "n_first_pass": 4, "n_second_pass": 0,
            "fields_first": _SBIDENT_FIELDS, "data_first_pass": [],
        })
        assert self._query() == []

    @pytest.mark.parametrize("response", [
        _FakeResponse(503, text="Service Unavailable"),
        _FakeResponse(400, {"message": "invalid 'fov-dec-center' format", "code": 400}),
        _FakeResponse(200, {"signature": {"version": "1.1"}, "n_first_pass": 4}),
    ])
    def test_a_failure_raises_for_the_caller_to_handle(self, response):
        self.response = response
        with pytest.raises(Exception):
            self._query()


@pytest.mark.parametrize("jpl_name, expected", [
    ("4 Vesta (A807 FA)", ("Vesta", "ASTEROID")),
    ("23918 (1998 SH133)", ("1998 SH133", "ASTEROID")),
    ("(2014 RY1)", ("2014 RY1", "ASTEROID")),
    ("10252 Heidigraf (4164 T-1)", ("Heidigraf", "ASTEROID")),
    ("136108 Haumea (2003 EL61)", ("Haumea", "ASTEROID")),
    ("C/2020 R4 (ATLAS)", ("C/2020 R4", "COMET")),
    ("C/2019 Y4-B (ATLAS)", ("C/2019 Y4-B", "COMET")),
    ("P/2010 H2 (Vales)", ("P/2010 H2", "COMET")),
    ("29P/Schwassmann-Wachmann 1", ("29P", "COMET")),
    ("128P/Shoemaker-Holt 1", ("128P", "COMET")),
    ("128P/Shoemaker-Holt 1-A", ("128P-A", "COMET")),
    ("11P/Tempel-Swift-LINEAR", ("11P", "COMET")),
    ("73P-B/Schwassmann-Wachmann 3", ("73P-B", "COMET")),
])
def test_jpl_names_become_skybot_designations(jpl_name, expected):
    assert _sbident._skybot_style_name(jpl_name) == expected


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

    def test_a_simbad_claimed_star_still_gets_its_gaia_colour(self):
        """
        The catalog that names a source must not decide how its magnitude is
        calibrated: the same star, Simbad-claimed one night and recovered
        under its Gaia identity another, was 0.9 mag apart between epochs
        purely from the colour term being applied only the second time.
        """
        star = _make_source(ra=_RA, dec=_DEC)
        star["catalog_name"] = "Simbad"
        star["catalog_id"]   = "2MASS J12241242+0704552"
        star["object_type"]  = "PM*"
        gaia_stars = [{
            "ra": _RA, "dec": _DEC, "source_id": "3901036404599908224",
            "phot_g_mean_mag": 13.65, "bp_rp": 2.4, "ruwe": 1.0,
        }]

        cm._match_gaia([star], gaia_stars)      # leaves a claimed source alone
        n = cm._attach_gaia_color([star], gaia_stars)

        assert n == 1
        assert star["catalog_name"] == "Simbad"  # identity untouched
        assert star["_catalog_color"] == pytest.approx(2.4)
        # ...and its Gaia G magnitude, so it can anchor the zero point.
        assert star["_gaia_mag"] == pytest.approx(13.65)
        assert star["_catalog_flags"]["ruwe"] == pytest.approx(1.0)

    def test_colour_attachment_respects_the_match_cone_and_existing_colours(self):
        far_ra, far_dec = _offset_ra_exact(_RA, _DEC, 3 * config.MATCH_CONE_ARCSEC)
        distant = _make_source(ra=_RA, dec=_DEC)
        distant["catalog_name"] = "Simbad"
        already = _make_source(ra=far_ra, dec=far_dec)
        already["catalog_name"] = "2MASS"
        already["_catalog_color"] = 0.7
        mpc = _make_source(ra=far_ra, dec=far_dec)
        mpc["catalog_name"] = "MPC"
        gaia_stars = [{"ra": far_ra, "dec": far_dec, "source_id": "1", "phot_g_mean_mag": 15.0, "bp_rp": 1.9}]

        n = cm._attach_gaia_color([distant, already, mpc], gaia_stars)

        # Only the star at the Gaia position is enriched: it gains the G
        # magnitude (a zero-point reference) but keeps the colour it had.
        assert n == 1
        assert "_catalog_color" not in distant and "_gaia_mag" not in distant
        assert already["_catalog_color"] == pytest.approx(0.7)
        assert already["_gaia_mag"] == pytest.approx(15.0)
        assert "_catalog_color" not in mpc and "_gaia_mag" not in mpc

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

    def test_simbad_radius_covers_the_frame_diagonal(self):
        """
        Audit 2026-08-18, finding H2: Simbad was the one catalog querying a
        circle inscribed in the frame (fov_deg / 2) rather than one covering
        its corners, leaving ~29% of a square frame's area never searched for
        named objects — and a named object missed there loses the OTYPE every
        VARIABLE_STAR/BINARY_STAR/SUPERNOVA_CANDIDATE branch gates on.
        """
        with patch("modules.catalog_matcher._simbad.Simbad") as mock_simbad_cls:
            instance = MagicMock()
            instance.query_region.return_value = None
            mock_simbad_cls.return_value = instance

            cm._query_simbad(_RA, _DEC, 1.0)

        radius = instance.query_region.call_args.kwargs["radius"]
        # Plus the cache tile's half-diagonal, so the cached result covers
        # every frame that rounds to the same key (finding M5).
        assert radius.to(u.deg).value == pytest.approx(
            math.sqrt(2) / 2.0 + cm._cache_radius_margin_deg()
        )

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

# Separations for the MPC tests below, expressed as fractions of the two
# configured cones rather than as literal arcseconds.
#
# `MOVING_CONE_ARCSEC` is deployment-tunable and the two env files this repo
# ships disagree about it: `.env.test`, which CI copies over `.env`, sets 30"
# while a working `.env` commonly has the 120" default. A literal "60 arcsec,
# comfortably inside the wide cone" is therefore true on a dev machine and
# false in CI — that is exactly how
# test_mpc_prefers_an_unclaimed_source_beyond_the_tight_cone came to pass
# locally and fail on the PR, with three of its neighbours passing in CI only
# because their "inside the wide cone" source had fallen outside it and the
# branch under test never ran at all.
_TIGHT_CONE = config.MATCH_CONE_ARCSEC
_WIDE_CONE = config.MOVING_CONE_ARCSEC
# Both are beyond the tight cone and inside the wide one, with NEAR closer.
_NEAR_IN_WIDE_CONE = _TIGHT_CONE + (_WIDE_CONE - _TIGHT_CONE) * 0.35
_FAR_IN_WIDE_CONE = _TIGHT_CONE + (_WIDE_CONE - _TIGHT_CONE) * 0.70


class TestMpcMatching:
    def test_mpc_uses_wider_cone(self):
        """
        A source between MATCH_CONE_ARCSEC and MOVING_CONE_ARCSEC must be
        matched by MPC but would NOT be matched by Gaia/Simbad.
        """
        shifted_ra, shifted_dec = _offset_ra_exact(_RA, _DEC, _NEAR_IN_WIDE_CONE)

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

    def test_mpc_radius_is_the_half_diagonal_plus_its_own_match_cone(self):
        """
        Audit 2026-08-18, finding L1: SkyBot was queried with the full
        `fov_deg` as its radius while every other catalog uses the frame's
        half-diagonal — about twice the sky area, on every frame, from a
        shared public service whose cache key includes the exact epoch and
        so is re-hit far more often than Gaia's or Simbad's.

        It keeps one term the others don't need: `_match_mpc()` pairs an
        ephemeris position with a source up to MOVING_CONE_ARCSEC away, so
        an object that legitimately matches a corner source can sit that far
        outside the frame.
        """
        fov_deg = 1.0
        captured: dict = {}

        class _FakeSkybot:
            @staticmethod
            def cone_search(coord, rad, epoch, cache=True):
                captured["rad"] = rad
                return None

        with patch("astroquery.imcce.Skybot", _FakeSkybot):
            cm._query_mpc(_RA, _DEC, "2024-03-15T22:01:34", fov_deg)

        expected_deg = (
            fov_deg * math.sqrt(2) / 2.0
            + config.MOVING_CONE_ARCSEC / 3600.0
            + cm._cache_radius_margin_deg()
        )
        assert captured["rad"].to(u.deg).value == pytest.approx(expected_deg)
        # Strictly smaller than the old full-fov radius it replaces.
        assert expected_deg < fov_deg + cm._cache_radius_margin_deg()

    def test_mpc_radius_still_reaches_a_corner_sources_whole_match_cone(self):
        """
        The property the radius has to guarantee, for the worst-placed frame
        that can share this tile's cache key: an MPC object sitting a full
        MOVING_CONE_ARCSEC beyond that frame's furthest corner is still
        inside what was queried.
        """
        fov_deg = 1.0
        queried = (
            fov_deg * math.sqrt(2) / 2.0
            + config.MOVING_CONE_ARCSEC / 3600.0
            + cm._cache_radius_margin_deg()
        )

        offset = 0.05  # frame centre at the tile's own corner
        worst_case = (
            math.hypot(offset, offset)
            + fov_deg * math.sqrt(2) / 2.0
            + config.MOVING_CONE_ARCSEC / 3600.0
        )

        assert worst_case <= queried + 1e-9

    def test_mpc_error_returns_empty_list(self):
        """If SkyBot query fails, _query_mpc returns [] without crashing."""
        # Test with invalid obs_time that will cause Time parsing to fail
        # The function should catch this and return []
        result = cm._query_mpc(_RA, _DEC, "invalid-time-format", 1.0)
        assert result == []

    def test_mpc_takes_over_a_coincident_catalogued_source(self):
        """
        Audit 2026-08-18, finding C3: a solar system object projecting within
        MATCH_CONE_ARCSEC of a background star used to be permanently tagged
        with that star's identity, because MPC only ever saw the sources no
        earlier catalog had claimed. A tight positional coincidence now hands
        the source to MPC instead — the ASTEROID/COMET classification and its
        ephemeris are the far costlier thing to lose.
        """
        # Gaia-matched star 2" from the ephemeris position — inside the tight cone
        star_ra, star_dec = _offset_ra_exact(_RA, _DEC, 2.0)
        source = _make_source(ra=star_ra, dec=star_dec)
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

        assert source["catalog_name"] == "MPC"
        assert source["catalog_id"]   == "2024 AB1"
        assert source["object_type"]  == "ASTEROID"
        assert source["catalog_mag"]  is None

    def test_a_takeover_drops_the_displaced_stars_gaia_fields(self):
        """
        The displaced star's BP-RP must not follow the source into MPC:
        photometry applies _catalog_color through the colour term regardless
        of catalog_name, which would correct the asteroid's magnitude with the
        background star's colour.
        """
        star_ra, star_dec = _offset_ra_exact(_RA, _DEC, 2.0)
        source = _make_source(ra=star_ra, dec=star_dec)
        source["catalog_name"]   = "Gaia DR3"
        source["catalog_id"]     = "GAIA_STAR_ID"
        source["catalog_mag"]    = 13.0
        source["object_type"]    = "STAR"
        source["_catalog_color"] = 2.7
        source["_catalog_flags"] = {"ruwe": 1.0}
        source["_gaia_mag"]      = 13.0

        mpc_objects = [{"ra": _RA, "dec": _DEC, "designation": "2024 AB1", "object_type": "ASTEROID"}]

        cm._match_mpc([source], mpc_objects)

        assert source["catalog_name"] == "MPC"
        assert "_catalog_color" not in source
        assert "_catalog_flags" not in source
        # Otherwise the asteroid anchors the zero point with the star's G.
        assert "_gaia_mag" not in source

    def test_mpc_leaves_a_distant_catalogued_source_alone(self):
        """
        Beyond MATCH_CONE_ARCSEC, an established identification is never
        overwritten: MOVING_CONE_ARCSEC is wide enough that some catalogued
        star is almost always inside it, whether or not it has anything to do
        with the moving object.
        """
        star_ra, star_dec = _offset_ra_exact(_RA, _DEC, _NEAR_IN_WIDE_CONE)
        source = _make_source(ra=star_ra, dec=star_dec)
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
        assert source["catalog_id"]   == "GAIA_STAR_ID"

    def test_mpc_prefers_an_unclaimed_source_beyond_the_tight_cone(self):
        """
        With no tight coincidence available, the MPC object falls back to the
        nearest *unclaimed* source within MOVING_CONE_ARCSEC — even though a
        catalogued one sits closer.
        """
        star_ra, star_dec = _offset_ra_exact(_RA, _DEC, _NEAR_IN_WIDE_CONE)
        star = _make_source(ra=star_ra, dec=star_dec)
        star["catalog_name"] = "Gaia DR3"
        star["catalog_id"]   = "GAIA_STAR_ID"
        star["object_type"]  = "STAR"

        free_ra, free_dec = _offset_ra_exact(_RA, _DEC, _FAR_IN_WIDE_CONE)
        free = _make_source(ra=free_ra, dec=free_dec)
        free["catalog_name"] = None
        free["catalog_id"]   = None

        mpc_objects = [{
            "ra":          _RA,
            "dec":         _DEC,
            "designation": "2024 AB1",
            "object_type": "ASTEROID",
        }]

        cm._match_mpc([star, free], mpc_objects)

        assert star["catalog_name"] == "Gaia DR3"
        assert free["catalog_name"] == "MPC"
        assert free["catalog_id"]   == "2024 AB1"

    def test_mpc_blend_does_not_displace_the_designation_onto_a_bystander(self):
        """
        The second half of finding C3: once the real (blended) detection was
        out of reach, the MPC object was handed to whatever unmatched source
        happened to be nearest within the wide cone — a false stationary
        "asteroid" on top of the real miss. The blended detection must win,
        and the bystander must stay uncatalogued.
        """
        blend_ra, blend_dec = _offset_ra_exact(_RA, _DEC, _TIGHT_CONE * 0.2)
        blend = _make_source(ra=blend_ra, dec=blend_dec)
        blend["catalog_name"] = "Gaia DR3"
        blend["catalog_id"]   = "GAIA_STAR_ID"
        blend["object_type"]  = "STAR"

        bystander_ra, bystander_dec = _offset_ra_exact(_RA, _DEC, _FAR_IN_WIDE_CONE)
        bystander = _make_source(ra=bystander_ra, dec=bystander_dec)
        bystander["catalog_name"] = None
        bystander["catalog_id"]   = None

        mpc_objects = [{
            "ra":          _RA,
            "dec":         _DEC,
            "designation": "2024 AB1",
            "object_type": "ASTEROID",
        }]

        cm._match_mpc([blend, bystander], mpc_objects)

        assert blend["catalog_name"] == "MPC"
        assert blend["catalog_id"]   == "2024 AB1"
        assert bystander["catalog_name"] is None

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

        # MPC object A: inside the tight cone (closer)
        mpc_a_ra, mpc_a_dec = _offset_ra_exact(_RA, _DEC, _TIGHT_CONE * 0.5)
        # MPC object B: further, but still genuinely competing inside the wide cone
        mpc_b_ra, mpc_b_dec = _offset_ra_exact(_RA, _DEC, _NEAR_IN_WIDE_CONE)

        mpc_objects = [
            {"ra": mpc_b_ra, "dec": mpc_b_dec, "designation": "2024 XY", "object_type": "ASTEROID"},
            {"ra": mpc_a_ra, "dec": mpc_a_dec, "designation": "2024 AB", "object_type": "ASTEROID"},
        ]

        cm._match_mpc([source], mpc_objects)

        # The closer MPC object (2024 AB) wins
        assert source["catalog_name"] == "MPC"
        assert source["catalog_id"] == "2024 AB"

    def test_a_loser_falls_back_to_its_next_nearest_eligible_source(self):
        """
        Two MPC objects competing for the same source used to cost the loser
        its match entirely: only one proposal was ever built per object (its
        own nearest source), so once that source was claimed the object had
        nothing to fall back to — even with another perfectly eligible
        unclaimed source of its own inside the cone. A real MPC detection was
        lost to nothing more than the order the objects happened to be in.
        """
        near = _make_source(ra=_RA, dec=_DEC)
        near["catalog_name"] = None
        near["catalog_id"] = None

        spare_ra, spare_dec = _offset_ra_exact(_RA, _DEC, _FAR_IN_WIDE_CONE)
        spare = _make_source(ra=spare_ra, dec=spare_dec)
        spare["catalog_name"] = None
        spare["catalog_id"] = None

        # Both objects sit nearest to `near`; B is the closer of the two, so A
        # is the one that has to fall back onto `spare`.
        a_ra, a_dec = _offset_ra_exact(_RA, _DEC, _TIGHT_CONE * 0.9)
        b_ra, b_dec = _offset_ra_exact(_RA, _DEC, _TIGHT_CONE * 0.2)

        mpc_objects = [
            {"ra": a_ra, "dec": a_dec, "designation": "2024 AA", "object_type": "ASTEROID"},
            {"ra": b_ra, "dec": b_dec, "designation": "2024 BB", "object_type": "ASTEROID"},
        ]

        cm._match_mpc([near, spare], mpc_objects)

        assert near["catalog_id"] == "2024 BB"
        assert spare["catalog_id"] == "2024 AA"

    def test_one_object_never_claims_two_sources(self):
        """
        The assignment is one-to-one in both directions: an object that has
        already taken a source does not go on to claim a second one just
        because that one is also inside its cone.
        """
        first = _make_source(ra=_RA, dec=_DEC)
        first["catalog_name"] = None
        first["catalog_id"] = None

        second_ra, second_dec = _offset_ra_exact(_RA, _DEC, _FAR_IN_WIDE_CONE)
        second = _make_source(ra=second_ra, dec=second_dec)
        second["catalog_name"] = None
        second["catalog_id"] = None

        mpc_objects = [
            {"ra": _RA, "dec": _DEC, "designation": "2024 AA", "object_type": "ASTEROID"},
        ]

        cm._match_mpc([first, second], mpc_objects)

        assert first["catalog_id"] == "2024 AA"
        assert second["catalog_name"] is None


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

    async def test_skybot_is_queried_at_the_exposure_midpoint(self):
        """
        Audit 2026-08-18, finding C9: DATE-OBS is shutter-open, so a fast
        mover is already tens of arcsec from its start-of-exposure position
        by mid-exposure. The MPC stage — and only it, every other catalog
        here being stationary on this timescale — runs at the midpoint the
        caller supplies.
        """
        sources = [_make_source()]
        frame_meta = dict(_FRAME_META, obs_time_mid="2024-03-15T22:03:34")

        gaia_t = _gaia_table(_RA + 10, _DEC + 10)
        with (
            patch("modules.catalog_matcher._gaia.Gaia", self._make_gaia_mock(gaia_t)),
            patch("modules.catalog_matcher._simbad.Simbad", self._make_simbad_mock(None)),
            patch("modules.catalog_matcher._mpc._query_mpc", return_value=[]) as mock_mpc,
        ):
            await cm.match(sources, frame_meta)

        assert mock_mpc.call_args[0][2] == "2024-03-15T22:03:34"

    async def test_skybot_falls_back_to_the_start_time(self):
        """A caller that computed no midpoint keeps the previous behaviour."""
        sources = [_make_source()]

        gaia_t = _gaia_table(_RA + 10, _DEC + 10)
        with (
            patch("modules.catalog_matcher._gaia.Gaia", self._make_gaia_mock(gaia_t)),
            patch("modules.catalog_matcher._simbad.Simbad", self._make_simbad_mock(None)),
            patch("modules.catalog_matcher._mpc._query_mpc", return_value=[]) as mock_mpc,
        ):
            await cm.match(sources, _FRAME_META)

        assert mock_mpc.call_args[0][2] == _FRAME_META["obs_time"]

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

    def test_recovers_an_offset_across_ra_zero(self):
        """True pairs 359.99° vs 0.01° apart must vote for a few arcsec, not ±360°."""
        true_dra, true_ddec = 20.0, -40.0
        dec = 10.0
        cos_dec = math.cos(math.radians(dec))
        rng = np.random.default_rng(7)
        sources, gaia_stars = [], []
        for i in range(30):
            ra = (rng.uniform(-0.1, 0.1)) % 360.0
            d = dec + rng.uniform(-0.1, 0.1)
            src = _make_source(ra=ra, dec=d)
            src["catalog_name"] = None
            sources.append(src)
            gaia_stars.append({
                "ra": (ra + true_dra / (cos_dec * 3600.0)) % 360.0,
                "dec": d + true_ddec / 3600.0,
                "source_id": f"g{i}", "phot_g_mean_mag": 14.0,
            })

        offset_ra_deg, offset_dec_deg = cm._compute_wcs_offset(sources, gaia_stars)

        assert offset_ra_deg * cos_dec * 3600.0 == pytest.approx(true_dra, abs=3.0)
        assert offset_dec_deg * 3600.0 == pytest.approx(true_ddec, abs=3.0)

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
# TestGaiaProperMotionFields — forced-photometry support
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


# ===========================================================================
# TestGaiaProperMotionPropagation — audit 2026-08-18, finding H1
#
# Gaia DR3's positions are at J2016.0. A high-proper-motion star has drifted
# several arcsec by now — comparable to MATCH_CONE_ARCSEC itself — so matching
# and the WCS-offset accumulator both have to see the star where it actually
# is at the observation epoch, not where it was a decade ago.
# ===========================================================================


class TestGaiaProperMotionPropagation:
    def test_high_proper_motion_star_is_moved_to_the_observation_epoch(self):
        """500 mas/yr over ~8 years is 4" — enough to miss a 5" cone."""
        star = {
            "ra":              _RA,
            "dec":             _DEC,
            "source_id":       "1",
            "phot_g_mean_mag": 14.5,
            "pmra":            500.0,
            "pmdec":           -500.0,
            "ref_epoch":       2016.0,
        }
        out = cm._propagate_to_epoch([star], "2024-01-01T00:00:00")

        moved = SkyCoord(ra=out[0]["ra"] * u.deg, dec=out[0]["dec"] * u.deg)
        original = SkyCoord(ra=_RA * u.deg, dec=_DEC * u.deg)
        sep = moved.separation(original).to(u.arcsec).value
        assert sep == pytest.approx(math.hypot(4.0, 4.0), abs=0.2)

    def test_pmra_is_divided_back_out_by_cos_dec(self):
        """
        pmra is Gaia's mu_alpha* (already × cos(dec)); the RA coordinate
        offset is therefore larger than pmra × dt by 1/cos(dec).
        """
        dec = 60.0  # cos(dec) = 0.5 — a factor-of-two effect, easy to see
        star = {
            "ra": _RA, "dec": dec, "source_id": "1", "phot_g_mean_mag": 14.5,
            "pmra": 3600.0, "pmdec": 0.0, "ref_epoch": 2016.0,
        }
        out = cm._propagate_to_epoch([star], "2017-01-01T00:00:00")

        d_ra_arcsec = (out[0]["ra"] - _RA) * 3600.0
        assert d_ra_arcsec == pytest.approx(3.6 / math.cos(math.radians(dec)), rel=0.02)
        assert out[0]["dec"] == pytest.approx(dec)

    def test_star_without_a_proper_motion_solution_is_left_alone(self):
        star = {
            "ra": _RA, "dec": _DEC, "source_id": "1", "phot_g_mean_mag": 14.5,
            "pmra": None, "pmdec": None, "ref_epoch": 2016.0,
        }
        out = cm._propagate_to_epoch([star], "2024-01-01T00:00:00")

        assert out[0]["ra"] == _RA
        assert out[0]["dec"] == _DEC

    def test_unparseable_obs_time_degrades_to_the_catalog_position(self):
        star = {
            "ra": _RA, "dec": _DEC, "source_id": "1", "phot_g_mean_mag": 14.5,
            "pmra": 500.0, "pmdec": 500.0, "ref_epoch": 2016.0,
        }
        for bad in ("", None, "not-a-date"):
            out = cm._propagate_to_epoch([star], bad)
            assert out[0]["ra"] == _RA
            assert out[0]["dec"] == _DEC

    def test_cached_catalog_list_is_never_mutated_in_place(self):
        """
        _query_gaia() hands back the cached list itself, and the same sky
        region is re-used by frames from other epochs within the cache TTL —
        propagating in place would write one frame's epoch into every later
        frame's catalog.
        """
        star = {
            "ra": _RA, "dec": _DEC, "source_id": "1", "phot_g_mean_mag": 14.5,
            "pmra": 500.0, "pmdec": 500.0, "ref_epoch": 2016.0,
        }
        cached = [star]
        cm._propagate_to_epoch(cached, "2024-01-01T00:00:00")

        assert cached[0]["ra"] == _RA
        assert cached[0]["dec"] == _DEC

    async def test_match_finds_a_star_that_has_drifted_out_of_the_cone(self):
        """
        End-to-end: a star whose J2016.0 position is 8" from the detection
        (beyond MATCH_CONE_ARCSEC) but whose propagated position lands on it
        must match Gaia rather than staying uncatalogued.
        """
        detected_ra, detected_dec = _RA, _DEC
        # Catalog position 8" west in RA; +1000 mas/yr in RA over 8 years
        # carries it back onto the detection.
        cat_ra = detected_ra - 8.0 / 3600.0 / math.cos(math.radians(_DEC))
        pmra = 1000.0

        table = Table({
            "ra":              [cat_ra],
            "dec":             [detected_dec],
            "source_id":       [777],
            "phot_g_mean_mag": [14.5],
            "pmra":            [pmra],
            "pmdec":           [0.0],
            "ref_epoch":       [2016.0],
        })
        mock_gaia = MagicMock()
        mock_gaia.cone_search.return_value = _mock_gaia_job(table)

        sources = [_make_source(detected_ra, detected_dec)]
        frame_meta = dict(_FRAME_META, obs_time="2024-01-01T00:00:00")

        with (
            patch("modules.catalog_matcher._gaia.Gaia", mock_gaia),
            patch("modules.catalog_matcher._simbad._query_simbad", return_value=[]),
            patch("modules.catalog_matcher._2mass._query_2mass", return_value=[]),
            patch("modules.catalog_matcher._panstarrs._query_panstarrs", return_value=[]),
            patch("modules.catalog_matcher._mpc._query_mpc", return_value=[]),
        ):
            result = await cm.match(sources, frame_meta)

        assert result[0]["catalog_name"] == "Gaia DR3"
        assert result[0]["catalog_id"] == "777"
