"""
tests/test_finder_chart.py — Unit tests for modules/finder_chart.py

Covers:
  - _style_for_anomaly_type(): moving vs. stationary anomaly_type routing
  - _local_fits_path(): archive path resolution from a track epoch dict
  - _load_frame(): loading a 2-D image + WCS, graceful None on failure
  - _arcsec_per_pixel(): plate scale from a synthetic WCS
  - _crop_around(): centred crop + edge-clipping behaviour
  - _render_track_chart() / _render_stamp_strip(): real (tiny) rendering,
    checked only for a valid PNG signature — not pixel content
  - update_charts_for_sources(): end-to-end batch orchestration, with
    api_client mocked out (no real HTTP calls) and real tiny FITS fixtures
    on disk

asyncio_mode = auto is set in pytest.ini, so async tests need no decorator.
"""

from __future__ import annotations

import numpy as np
import pytest
from astropy.io import fits
from astropy.wcs import WCS as AstropyWCS

import config
from modules import finder_chart

PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def _make_wcs_fits(path, ra=202.47, dec=47.20, scale_deg=0.000278, size=100):
    """Write a tiny synthetic FITS file with a valid celestial WCS."""
    w = AstropyWCS(naxis=2)
    w.wcs.ctype = ["RA---TAN", "DEC--TAN"]
    w.wcs.crpix = [size / 2.0, size / 2.0]
    w.wcs.crval = [ra, dec]
    w.wcs.cdelt = [-scale_deg, scale_deg]
    w.wcs.set()

    rng = np.random.default_rng(42)
    data = rng.normal(loc=100.0, scale=5.0, size=(size, size)).astype(np.float32)
    fits.PrimaryHDU(data=data, header=w.to_header()).writeto(path)
    return w


# ---------------------------------------------------------------------------
# _style_for_anomaly_type
# ---------------------------------------------------------------------------

class TestStyleForAnomalyType:

    @pytest.mark.parametrize("anomaly_type", ["ASTEROID", "COMET", "MOVING_UNKNOWN", "SPACE_DEBRIS"])
    def test_moving_types_use_track_style(self, anomaly_type):
        assert finder_chart._style_for_anomaly_type(anomaly_type) == finder_chart.STYLE_TRACK

    @pytest.mark.parametrize("anomaly_type", [
        "SUPERNOVA_CANDIDATE", "UNKNOWN", "VARIABLE_STAR",
        "BINARY_STAR", "KNOWN_CATALOG_NEW", "FIRST_OBSERVATION",
    ])
    def test_stationary_types_use_stamp_strip_style(self, anomaly_type):
        assert finder_chart._style_for_anomaly_type(anomaly_type) == finder_chart.STYLE_STAMP_STRIP


# ---------------------------------------------------------------------------
# _style_for_source
# ---------------------------------------------------------------------------

class TestStyleForSource:

    @pytest.mark.parametrize("anomaly_type", [
        "ASTEROID", "COMET", "MOVING_UNKNOWN", "SPACE_DEBRIS",
        "SUPERNOVA_CANDIDATE", "UNKNOWN", "VARIABLE_STAR",
        "BINARY_STAR", "KNOWN_CATALOG_NEW", "FIRST_OBSERVATION",
    ])
    def test_single_epoch_always_uses_before_after_regardless_of_type(self, anomaly_type):
        assert finder_chart._style_for_source(anomaly_type, 1) == finder_chart.STYLE_BEFORE_AFTER

    def test_zero_epochs_uses_before_after_too(self):
        """Defensive: _render_chart_for_source() never actually calls this with 0 loaded
        epochs (it returns None first), but the boundary itself shouldn't crash or pick
        a style that then indexes into an empty list."""
        assert finder_chart._style_for_source("UNKNOWN", 0) == finder_chart.STYLE_BEFORE_AFTER

    def test_two_epochs_moving_type_uses_track(self):
        assert finder_chart._style_for_source("ASTEROID", 2) == finder_chart.STYLE_TRACK

    def test_two_epochs_stationary_type_uses_stamp_strip(self):
        assert finder_chart._style_for_source("UNKNOWN", 2) == finder_chart.STYLE_STAMP_STRIP


# ---------------------------------------------------------------------------
# _local_fits_path
# ---------------------------------------------------------------------------

class TestLocalFitsPath:

    def test_joins_archive_object_and_filename(self, monkeypatch, tmp_path):
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))
        epoch = {"object": "Vesta_A807_FA", "filename": "frame1.fits"}

        result = finder_chart._local_fits_path(epoch)

        assert result == str(tmp_path / "Vesta_A807_FA" / "frame1.fits")

    def test_missing_object_falls_back_to_unknown(self, monkeypatch, tmp_path):
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))
        epoch = {"object": None, "filename": "frame1.fits"}

        result = finder_chart._local_fits_path(epoch)

        assert result == str(tmp_path / "_UNKNOWN" / "frame1.fits")


# ---------------------------------------------------------------------------
# _load_frame
# ---------------------------------------------------------------------------

class TestLoadFrame:

    def test_loads_data_and_wcs(self, tmp_path):
        path = tmp_path / "solved.fits"
        _make_wcs_fits(path)

        result = finder_chart._load_frame(str(path))

        assert result is not None
        data, wcs = result
        assert data.shape == (100, 100)
        assert wcs.has_celestial

    def test_missing_file_returns_none(self, tmp_path):
        assert finder_chart._load_frame(str(tmp_path / "missing.fits")) is None

    def test_no_wcs_returns_none(self, tmp_path):
        path = tmp_path / "no_wcs.fits"
        fits.PrimaryHDU(data=np.zeros((10, 10), dtype=np.float32)).writeto(path)

        assert finder_chart._load_frame(str(path)) is None


# ---------------------------------------------------------------------------
# _arcsec_per_pixel
# ---------------------------------------------------------------------------

class TestArcsecPerPixel:

    def test_matches_known_scale(self, tmp_path):
        path = tmp_path / "solved.fits"
        wcs = _make_wcs_fits(path, scale_deg=0.000278)  # ~1.0008"/px

        result = finder_chart._arcsec_per_pixel(wcs)

        assert result == pytest.approx(0.000278 * 3600.0, rel=1e-3)

    def test_degenerate_wcs_falls_back(self):
        # A bare, unset WCS: proj_plane_pixel_scales() raises rather than
        # returning something silently wrong — must fall back, not crash.
        bare = AstropyWCS(naxis=2)
        assert finder_chart._arcsec_per_pixel(bare) > 0


# ---------------------------------------------------------------------------
# _grid_layout
# ---------------------------------------------------------------------------

class TestGridLayout:

    @pytest.mark.parametrize("n, expected", [
        (1, (1, 1)),
        (2, (1, 2)),
        (3, (1, 3)),
        (4, (2, 2)),
        (5, (2, 3)),
        (6, (2, 3)),
        (7, (3, 3)),
        (8, (3, 3)),
        (9, (3, 3)),
        (10, (3, 4)),
        (16, (4, 4)),
    ])
    def test_grid_dimensions(self, n, expected):
        assert finder_chart._grid_layout(n) == expected


# ---------------------------------------------------------------------------
# _format_angular_shift / _angular_separation_arcsec
# ---------------------------------------------------------------------------

class TestFormatAngularShift:

    def test_sub_arcminute_uses_arcsec(self):
        assert finder_chart._format_angular_shift(12.345) == "12.35″"

    def test_boundary_below_60_arcsec_stays_arcsec(self):
        assert finder_chart._format_angular_shift(59.99).endswith("″")

    def test_arcminute_range_uses_arcmin(self):
        assert finder_chart._format_angular_shift(90.0) == "1.50′"

    def test_boundary_at_3600_arcsec_uses_degrees(self):
        # Exactly 1° — the arcmin branch is `< 3600.0`, so 3600.0 itself
        # falls through to degrees.
        assert finder_chart._format_angular_shift(3600.0) == "1.000°"

    def test_multi_degree_uses_degrees(self):
        assert finder_chart._format_angular_shift(7200.0) == "2.000°"


class TestAngularSeparationArcsec:

    def test_same_point_is_zero(self):
        sep = finder_chart._angular_separation_arcsec(202.47, 47.20, 202.47, 47.20)
        assert sep == pytest.approx(0.0, abs=1e-6)

    def test_matches_known_small_offset(self):
        # 1 arcsec in Dec at Dec=0 is exactly 1 arcsec of great-circle separation.
        sep = finder_chart._angular_separation_arcsec(10.0, 0.0, 10.0, 1.0 / 3600.0)
        assert sep == pytest.approx(1.0, rel=1e-3)


# ---------------------------------------------------------------------------
# _crop_around
# ---------------------------------------------------------------------------

class TestCropAround:

    def test_centred_crop_matches_expected_size_and_centre(self, tmp_path):
        path = tmp_path / "solved.fits"
        wcs = _make_wcs_fits(path, ra=202.47, dec=47.20, size=100)
        data, _ = finder_chart._load_frame(str(path))

        crop, (cx, cy) = finder_chart._crop_around(data, wcs, ra=202.47, dec=47.20, half_size_px=20)

        assert crop.shape == (40, 40)
        # The requested (ra, dec) is the WCS reference point (CRVAL), which
        # sits at the centre of the frame — so it should also sit at the
        # centre of an unclipped crop.
        assert cx == pytest.approx(20.0, abs=1.0)
        assert cy == pytest.approx(20.0, abs=1.0)

    def test_edge_clipping_shrinks_crop_without_erroring(self, tmp_path):
        path = tmp_path / "solved.fits"
        wcs = _make_wcs_fits(path, ra=202.47, dec=47.20, size=100)
        data, _ = finder_chart._load_frame(str(path))

        # CRVAL sits at pixel (49, 49) 0-indexed (see subtraction.py's own
        # convention) — request a huge half-size so the box is clipped hard
        # against the frame edges.
        crop, (cx, cy) = finder_chart._crop_around(data, wcs, ra=202.47, dec=47.20, half_size_px=1000)

        assert crop.shape == (100, 100)
        assert 0 <= cx <= 100
        assert 0 <= cy <= 100

    def test_position_outside_frame_raises(self, tmp_path):
        path = tmp_path / "solved.fits"
        wcs = _make_wcs_fits(path, ra=202.47, dec=47.20, size=100)
        data, _ = finder_chart._load_frame(str(path))

        # Far outside the 100x100 frame entirely.
        with pytest.raises(ValueError):
            finder_chart._crop_around(data, wcs, ra=250.0, dec=47.20, half_size_px=5)


# ---------------------------------------------------------------------------
# Rendering — real (tiny) output, checked only for a valid PNG signature
# ---------------------------------------------------------------------------

class TestRendering:

    def _loaded_epoch(self, tmp_path, name, ra, dec, obs_time, mag=15.0):
        path = tmp_path / name
        wcs = _make_wcs_fits(path, ra=ra, dec=dec)
        data, wcs = finder_chart._load_frame(str(path))
        return {
            "frame_id": name,
            "filename": name,
            "object": "TestObj",
            "obs_time": obs_time,
            "ra": ra,
            "dec": dec,
            "mag": mag,
            "data": data,
            "wcs": wcs,
        }

    def test_render_track_chart_produces_valid_png(self, tmp_path):
        epochs = [
            self._loaded_epoch(tmp_path, "e0.fits", 202.47, 47.20, "2024-01-01T00:00:00Z"),
            self._loaded_epoch(tmp_path, "e1.fits", 202.48, 47.21, "2024-01-01T01:00:00Z"),
        ]

        png_bytes = finder_chart._render_track_chart(epochs)

        assert png_bytes[:8] == PNG_SIGNATURE
        assert len(png_bytes) > 100

    def test_render_stamp_strip_produces_valid_png_for_single_epoch(self, tmp_path):
        epochs = [self._loaded_epoch(tmp_path, "e0.fits", 202.47, 47.20, "2024-01-01T00:00:00Z")]

        png_bytes = finder_chart._render_stamp_strip(epochs)

        assert png_bytes[:8] == PNG_SIGNATURE

    def test_render_stamp_strip_produces_valid_png_for_multiple_epochs(self, tmp_path):
        epochs = [
            self._loaded_epoch(tmp_path, "e0.fits", 202.47, 47.20, "2024-01-01T00:00:00Z", mag=15.0),
            self._loaded_epoch(tmp_path, "e1.fits", 202.47, 47.20, "2024-01-02T00:00:00Z", mag=14.2),
            self._loaded_epoch(tmp_path, "e2.fits", 202.47, 47.20, "2024-01-03T00:00:00Z", mag=None),
        ]

        png_bytes = finder_chart._render_stamp_strip(epochs)

        assert png_bytes[:8] == PNG_SIGNATURE

    def test_render_stamp_strip_grid_layout_for_many_epochs(self, tmp_path):
        """With many epochs the stamp strip should use a grid layout and still produce valid PNG."""
        epochs = [
            self._loaded_epoch(tmp_path, f"e{i}.fits", 202.47, 47.20, f"2024-01-{i+1:02d}T00:00:00Z")
            for i in range(9)
        ]

        png_bytes = finder_chart._render_stamp_strip(epochs)

        assert png_bytes[:8] == PNG_SIGNATURE

    def test_render_before_after_chart_with_before_panel_produces_valid_png(self, tmp_path):
        current_ep = self._loaded_epoch(tmp_path, "after.fits", 202.47, 47.20, "2024-01-02T00:00:00Z")
        before_ep = self._loaded_epoch(tmp_path, "before.fits", 202.47, 47.20, "2024-01-01T00:00:00Z")

        png_bytes = finder_chart._render_before_after_chart(current_ep, before_ep, label="ASTEROID (4 Vesta)")

        assert png_bytes[:8] == PNG_SIGNATURE
        assert len(png_bytes) > 100

    def test_render_before_after_chart_without_before_panel_produces_valid_png(self, tmp_path):
        """No before_ep — falls back to a single panel, with a note why."""
        current_ep = self._loaded_epoch(tmp_path, "after.fits", 202.47, 47.20, "2024-01-02T00:00:00Z")

        png_bytes = finder_chart._render_before_after_chart(
            current_ep, None, label="MOVING_UNKNOWN",
            missing_reason="No earlier frame of TestObj exists.",
        )

        assert png_bytes[:8] == PNG_SIGNATURE

    def test_render_before_after_chart_works_without_label(self, tmp_path):
        current_ep = self._loaded_epoch(tmp_path, "after.fits", 202.47, 47.20, "2024-01-02T00:00:00Z")

        png_bytes = finder_chart._render_before_after_chart(current_ep, None)

        assert png_bytes[:8] == PNG_SIGNATURE

    def test_render_before_after_chart_survives_crop_failure(self, tmp_path):
        """A crop that fails on either panel (e.g. position outside frame bounds after
        clipping) must render "n/a" placeholders rather than raise."""
        current_ep = self._loaded_epoch(tmp_path, "after.fits", 202.47, 47.20, "2024-01-02T00:00:00Z")
        # Position far outside this frame's bounds — _crop_around() raises for both panels.
        current_ep["ra"], current_ep["dec"] = 250.0, 47.20
        before_ep = self._loaded_epoch(tmp_path, "before.fits", 202.47, 47.20, "2024-01-01T00:00:00Z")

        png_bytes = finder_chart._render_before_after_chart(current_ep, before_ep)

        assert png_bytes[:8] == PNG_SIGNATURE


# ---------------------------------------------------------------------------
# _fetch_and_load_earlier_frame / _get_earlier_frame_epoch
# ---------------------------------------------------------------------------

class TestGetEarlierFrameEpoch:

    async def test_no_object_or_obs_time_returns_reason_without_calling_api(self, monkeypatch):
        called = False

        async def fake_get_nearest_frame_before(object_name, before_time):
            nonlocal called
            called = True
            return None
        monkeypatch.setattr(finder_chart.api_client, "get_nearest_frame_before", fake_get_nearest_frame_before)

        result, reason = await finder_chart._fetch_and_load_earlier_frame(None, "2024-01-01T00:00:00Z")

        assert result is None
        assert reason is not None
        assert called is False

    async def test_no_earlier_frame_returns_reason(self, monkeypatch):
        monkeypatch.setattr(
            finder_chart.api_client, "get_nearest_frame_before",
            lambda object_name, before_time: _async_return(None),
        )

        result, reason = await finder_chart._fetch_and_load_earlier_frame("M51", "2024-01-01T00:00:00Z")

        assert result is None
        assert "M51" in reason

    async def test_frame_found_but_not_loadable_locally_returns_reason(self, monkeypatch, tmp_path):
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))  # nothing archived here
        monkeypatch.setattr(
            finder_chart.api_client, "get_nearest_frame_before",
            lambda object_name, before_time: _async_return(
                {"id": "f1", "filename": "missing.fits", "object": "M51", "obs_time": "2023-12-01T00:00:00Z"},
            ),
        )

        result, reason = await finder_chart._fetch_and_load_earlier_frame("M51", "2024-01-01T00:00:00Z")

        assert result is None
        assert "missing.fits" in reason

    async def test_frame_found_and_loaded_returns_epoch_dict(self, monkeypatch, tmp_path):
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))
        obj_dir = tmp_path / "M51"
        obj_dir.mkdir()
        _make_wcs_fits(obj_dir / "earlier.fits", ra=202.47, dec=47.20)
        monkeypatch.setattr(
            finder_chart.api_client, "get_nearest_frame_before",
            lambda object_name, before_time: _async_return(
                {"id": "f1", "filename": "earlier.fits", "object": "M51", "obs_time": "2023-12-01T00:00:00Z"},
            ),
        )

        result, reason = await finder_chart._fetch_and_load_earlier_frame("M51", "2024-01-01T00:00:00Z")

        assert reason is None
        assert result is not None
        assert result["obs_time"] == "2023-12-01T00:00:00Z"
        assert result["wcs"].has_celestial

    async def test_api_error_returns_reason_without_raising(self, monkeypatch):
        async def raising(object_name, before_time):
            raise RuntimeError("boom")
        monkeypatch.setattr(finder_chart.api_client, "get_nearest_frame_before", raising)

        result, reason = await finder_chart._fetch_and_load_earlier_frame("M51", "2024-01-01T00:00:00Z")

        assert result is None
        assert reason is not None

    async def test_caches_by_object_and_obs_time(self, monkeypatch, tmp_path):
        """Two epochs sharing (object, obs_time) must trigger exactly one API call."""
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))
        calls = []

        async def fake_get_nearest_frame_before(object_name, before_time):
            calls.append((object_name, before_time))
            return None
        monkeypatch.setattr(finder_chart.api_client, "get_nearest_frame_before", fake_get_nearest_frame_before)

        cache: dict = {}
        ep_a = {"object": "M51", "obs_time": "2024-01-01T00:00:00Z"}
        ep_b = {"object": "M51", "obs_time": "2024-01-01T00:00:00Z"}

        await finder_chart._get_earlier_frame_epoch(ep_a, cache)
        await finder_chart._get_earlier_frame_epoch(ep_b, cache)

        assert len(calls) == 1

    async def test_different_obs_time_is_not_cached_together(self, monkeypatch, tmp_path):
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))
        calls = []

        async def fake_get_nearest_frame_before(object_name, before_time):
            calls.append((object_name, before_time))
            return None
        monkeypatch.setattr(finder_chart.api_client, "get_nearest_frame_before", fake_get_nearest_frame_before)

        cache: dict = {}
        await finder_chart._get_earlier_frame_epoch({"object": "M51", "obs_time": "2024-01-01T00:00:00Z"}, cache)
        await finder_chart._get_earlier_frame_epoch({"object": "M51", "obs_time": "2024-02-01T00:00:00Z"}, cache)

        assert len(calls) == 2


# ---------------------------------------------------------------------------
# update_charts_for_sources — end-to-end batch orchestration
# ---------------------------------------------------------------------------

class TestUpdateChartsForSources:

    @staticmethod
    def _epoch(filename, obj, ra, dec, obs_time="2024-01-01T00:00:00Z", mag=15.0):
        return {
            "frame_id": filename, "filename": filename, "object": obj,
            "obs_time": obs_time, "ra": ra, "dec": dec, "mag": mag,
        }

    async def test_disabled_skips_without_calling_api(self, monkeypatch):
        monkeypatch.setattr(config, "CHART_ENABLED", False)
        called = False

        async def fake_get_tracks_batch(source_ids):
            nonlocal called
            called = True
            return {}

        monkeypatch.setattr(finder_chart.api_client, "get_source_tracks_batch", fake_get_tracks_batch)

        result = await finder_chart.update_charts_for_sources({"src1": ["ASTEROID"]})

        assert result == {"src1": {"ASTEROID": False}}
        assert called is False

    async def test_empty_input_returns_empty_dict_without_calling_api(self, monkeypatch):
        monkeypatch.setattr(config, "CHART_ENABLED", True)
        called = False

        async def fake_get_tracks_batch(source_ids):
            nonlocal called
            called = True
            return {}

        monkeypatch.setattr(finder_chart.api_client, "get_source_tracks_batch", fake_get_tracks_batch)

        result = await finder_chart.update_charts_for_sources({})

        assert result == {}
        assert called is False

    async def test_no_epochs_returns_false(self, monkeypatch):
        monkeypatch.setattr(config, "CHART_ENABLED", True)
        monkeypatch.setattr(
            finder_chart.api_client, "get_source_tracks_batch", lambda source_ids: _async_return({})
        )

        result = await finder_chart.update_charts_for_sources({"src1": ["ASTEROID"]})

        assert result == {"src1": {"ASTEROID": False}}

    async def test_tracks_batch_fetch_error_returns_false_for_all(self, monkeypatch):
        monkeypatch.setattr(config, "CHART_ENABLED", True)

        async def raising(source_ids):
            raise RuntimeError("boom")

        monkeypatch.setattr(finder_chart.api_client, "get_source_tracks_batch", raising)

        result = await finder_chart.update_charts_for_sources({"src1": ["ASTEROID"], "src2": ["UNKNOWN"]})

        assert result == {"src1": {"ASTEROID": False}, "src2": {"UNKNOWN": False}}

    async def test_all_epochs_missing_locally_returns_false(self, monkeypatch, tmp_path):
        monkeypatch.setattr(config, "CHART_ENABLED", True)
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))  # empty — nothing archived
        epochs = [self._epoch("missing.fits", "Vesta_A807_FA", 202.47, 47.20)]
        monkeypatch.setattr(
            finder_chart.api_client, "get_source_tracks_batch",
            lambda source_ids: _async_return({"src1": epochs}),
        )

        result = await finder_chart.update_charts_for_sources({"src1": ["ASTEROID"]})

        assert result == {"src1": {"ASTEROID": False}}

    async def test_happy_path_moving_type_uploads_track_style(self, monkeypatch, tmp_path):
        monkeypatch.setattr(config, "CHART_ENABLED", True)
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))

        obj_dir = tmp_path / "Vesta_A807_FA"
        obj_dir.mkdir()
        _make_wcs_fits(obj_dir / "e0.fits", ra=202.47, dec=47.20)
        _make_wcs_fits(obj_dir / "e1.fits", ra=202.48, dec=47.21)

        epochs = [
            self._epoch("e0.fits", "Vesta_A807_FA", 202.47, 47.20, "2024-01-01T00:00:00Z"),
            self._epoch("e1.fits", "Vesta_A807_FA", 202.48, 47.21, "2024-01-01T01:00:00Z"),
        ]
        monkeypatch.setattr(
            finder_chart.api_client, "get_source_tracks_batch",
            lambda source_ids: _async_return({"src1": epochs}),
        )

        upload_calls = []

        async def fake_upload_chart(source_id, png_bytes, style, frame_count):
            upload_calls.append({"source_id": source_id, "png_bytes": png_bytes, "style": style, "frame_count": frame_count})
            return True

        monkeypatch.setattr(finder_chart.api_client, "upload_source_chart", fake_upload_chart)

        result = await finder_chart.update_charts_for_sources({"src1": ["ASTEROID"]})

        assert result == {"src1": {"ASTEROID": True}}
        assert len(upload_calls) == 1
        chart = upload_calls[0]
        assert chart["source_id"] == "src1"
        assert chart["png_bytes"][:8] == PNG_SIGNATURE
        assert chart["style"] == finder_chart.STYLE_TRACK
        assert chart["frame_count"] == 2

    async def test_happy_path_stationary_type_uploads_stamp_strip_style(self, monkeypatch, tmp_path):
        """2+ epochs — a single epoch instead gets STYLE_BEFORE_AFTER, see TestBeforeAfterStyle."""
        monkeypatch.setattr(config, "CHART_ENABLED", True)
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))

        obj_dir = tmp_path / "M51"
        obj_dir.mkdir()
        _make_wcs_fits(obj_dir / "e0.fits", ra=202.47, dec=47.20)
        _make_wcs_fits(obj_dir / "e1.fits", ra=202.47, dec=47.20)

        epochs = [
            self._epoch("e0.fits", "M51", 202.47, 47.20, "2024-01-01T00:00:00Z"),
            self._epoch("e1.fits", "M51", 202.47, 47.20, "2024-02-01T00:00:00Z"),
        ]
        monkeypatch.setattr(
            finder_chart.api_client, "get_source_tracks_batch",
            lambda source_ids: _async_return({"src2": epochs}),
        )

        upload_calls = []

        async def fake_upload_chart(source_id, png_bytes, style, frame_count):
            upload_calls.append({"source_id": source_id, "style": style, "frame_count": frame_count})
            return True

        monkeypatch.setattr(finder_chart.api_client, "upload_source_chart", fake_upload_chart)

        result = await finder_chart.update_charts_for_sources({"src2": ["SUPERNOVA_CANDIDATE"]})

        assert result == {"src2": {"SUPERNOVA_CANDIDATE": True}}
        assert upload_calls[0]["style"] == finder_chart.STYLE_STAMP_STRIP
        assert upload_calls[0]["frame_count"] == 2

    async def test_single_epoch_source_with_earlier_frame_uploads_before_after_2_panel(self, monkeypatch, tmp_path):
        """A source with exactly one epoch AND a real earlier frame of the object
        renders the 2-panel before_after style, frame_count=2."""
        monkeypatch.setattr(config, "CHART_ENABLED", True)
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))

        obj_dir = tmp_path / "Vesta_A807_FA"
        obj_dir.mkdir()
        _make_wcs_fits(obj_dir / "current.fits", ra=167.55, dec=17.28)
        _make_wcs_fits(obj_dir / "earlier.fits", ra=167.55, dec=17.28)

        epochs = [self._epoch("current.fits", "Vesta_A807_FA", 167.55, 17.28, "2021-03-14T18:05:44Z")]
        monkeypatch.setattr(
            finder_chart.api_client, "get_source_tracks_batch",
            lambda source_ids: _async_return({"src1": epochs}),
        )
        monkeypatch.setattr(
            finder_chart.api_client, "get_nearest_frame_before",
            lambda object_name, before_time: _async_return({
                "id": "f0", "filename": "earlier.fits", "object": "Vesta_A807_FA",
                "obs_time": "2021-03-14T17:05:27Z",
            }),
        )

        upload_calls = []

        async def fake_upload_chart(source_id, png_bytes, style, frame_count):
            upload_calls.append({"source_id": source_id, "style": style, "frame_count": frame_count})
            return True
        monkeypatch.setattr(finder_chart.api_client, "upload_source_chart", fake_upload_chart)

        result = await finder_chart.update_charts_for_sources({"src1": ["MOVING_UNKNOWN"]})

        assert result == {"src1": {"MOVING_UNKNOWN": True}}
        assert upload_calls[0]["style"] == finder_chart.STYLE_BEFORE_AFTER
        assert upload_calls[0]["frame_count"] == 2

    async def test_single_epoch_source_with_no_earlier_frame_uploads_before_after_1_panel(self, monkeypatch, tmp_path):
        """No earlier frame exists (this object's first-ever frame) — falls back to a
        1-panel before_after chart, frame_count=1, but still uploads successfully."""
        monkeypatch.setattr(config, "CHART_ENABLED", True)
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))

        obj_dir = tmp_path / "Vesta_A807_FA"
        obj_dir.mkdir()
        _make_wcs_fits(obj_dir / "current.fits", ra=167.55, dec=17.28)

        epochs = [self._epoch("current.fits", "Vesta_A807_FA", 167.55, 17.28, "2021-03-14T18:05:44Z")]
        monkeypatch.setattr(
            finder_chart.api_client, "get_source_tracks_batch",
            lambda source_ids: _async_return({"src1": epochs}),
        )
        monkeypatch.setattr(
            finder_chart.api_client, "get_nearest_frame_before",
            lambda object_name, before_time: _async_return(None),
        )

        upload_calls = []

        async def fake_upload_chart(source_id, png_bytes, style, frame_count):
            upload_calls.append({"source_id": source_id, "style": style, "frame_count": frame_count})
            return True
        monkeypatch.setattr(finder_chart.api_client, "upload_source_chart", fake_upload_chart)

        result = await finder_chart.update_charts_for_sources({"src1": ["MOVING_UNKNOWN"]})

        assert result == {"src1": {"MOVING_UNKNOWN": True}}
        assert upload_calls[0]["style"] == finder_chart.STYLE_BEFORE_AFTER
        assert upload_calls[0]["frame_count"] == 1

    async def test_epoch_count_capped_at_chart_max_epochs(self, monkeypatch, tmp_path):
        monkeypatch.setattr(config, "CHART_ENABLED", True)
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))
        monkeypatch.setattr(config, "CHART_MAX_EPOCHS", 2)

        obj_dir = tmp_path / "Vesta_A807_FA"
        obj_dir.mkdir()
        for i in range(4):
            _make_wcs_fits(obj_dir / f"e{i}.fits", ra=202.47 + i * 0.01, dec=47.20)

        # Chronologically ordered oldest-first, as the real API returns them.
        epochs = [
            self._epoch(f"e{i}.fits", "Vesta_A807_FA", 202.47 + i * 0.01, 47.20, f"2024-01-0{i + 1}T00:00:00Z")
            for i in range(4)
        ]
        monkeypatch.setattr(
            finder_chart.api_client, "get_source_tracks_batch",
            lambda source_ids: _async_return({"src1": epochs}),
        )

        upload_calls = []

        async def fake_upload_chart(source_id, png_bytes, style, frame_count):
            upload_calls.append(frame_count)
            return True

        monkeypatch.setattr(finder_chart.api_client, "upload_source_chart", fake_upload_chart)

        result = await finder_chart.update_charts_for_sources({"src1": ["ASTEROID"]})

        assert result == {"src1": {"ASTEROID": True}}
        # Only the most recent CHART_MAX_EPOCHS=2 epochs (e2, e3) should have
        # been loaded and included — e0/e1 dropped as the oldest.
        assert upload_calls == [2]

    async def test_upload_failure_propagates_false(self, monkeypatch, tmp_path):
        monkeypatch.setattr(config, "CHART_ENABLED", True)
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))

        obj_dir = tmp_path / "M51"
        obj_dir.mkdir()
        _make_wcs_fits(obj_dir / "e0.fits", ra=202.47, dec=47.20)
        epochs = [self._epoch("e0.fits", "M51", 202.47, 47.20)]
        monkeypatch.setattr(
            finder_chart.api_client, "get_source_tracks_batch",
            lambda source_ids: _async_return({"src1": epochs}),
        )
        monkeypatch.setattr(
            finder_chart.api_client, "upload_source_chart",
            lambda source_id, png_bytes, style, frame_count: _async_return(False),
        )

        result = await finder_chart.update_charts_for_sources({"src1": ["UNKNOWN"]})

        assert result == {"src1": {"UNKNOWN": False}}

    async def test_designation_shown_next_to_anomaly_type_in_chart_label(self, monkeypatch, tmp_path):
        """A catalog-matched source's designation must reach the renderer as
        "{anomaly_type} ({designation})" — e.g. "ASTEROID (4 Vesta)"."""
        monkeypatch.setattr(config, "CHART_ENABLED", True)
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))

        obj_dir = tmp_path / "Vesta_A807_FA"
        obj_dir.mkdir()
        _make_wcs_fits(obj_dir / "e0.fits", ra=202.47, dec=47.20)
        _make_wcs_fits(obj_dir / "e1.fits", ra=202.48, dec=47.21)
        epochs = [
            self._epoch("e0.fits", "Vesta_A807_FA", 202.47, 47.20, "2024-01-01T00:00:00Z"),
            self._epoch("e1.fits", "Vesta_A807_FA", 202.48, 47.21, "2024-01-01T01:00:00Z"),
        ]
        monkeypatch.setattr(
            finder_chart.api_client, "get_source_tracks_batch",
            lambda source_ids: _async_return({"src1": epochs}),
        )
        monkeypatch.setattr(
            finder_chart.api_client, "upload_source_chart",
            lambda source_id, png_bytes, style, frame_count: _async_return(True),
        )

        captured_labels = []
        real_render = finder_chart._render_track_chart

        def spy_render_track_chart(loaded_epochs, label=None):
            captured_labels.append(label)
            return real_render(loaded_epochs, label=label)
        monkeypatch.setattr(finder_chart, "_render_track_chart", spy_render_track_chart)

        result = await finder_chart.update_charts_for_sources(
            {"src1": ["ASTEROID"]}, {"src1": "4 Vesta"},
        )

        assert result == {"src1": {"ASTEROID": True}}
        assert captured_labels == ["ASTEROID (4 Vesta)"]

    async def test_uncatalogued_source_labels_with_anomaly_type_only(self, monkeypatch, tmp_path):
        """No designation_by_source_id entry (or param omitted entirely) —> bare anomaly_type label."""
        monkeypatch.setattr(config, "CHART_ENABLED", True)
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))

        obj_dir = tmp_path / "M51"
        obj_dir.mkdir()
        _make_wcs_fits(obj_dir / "e0.fits", ra=202.47, dec=47.20)
        _make_wcs_fits(obj_dir / "e1.fits", ra=202.47, dec=47.20)
        epochs = [
            self._epoch("e0.fits", "M51", 202.47, 47.20, "2024-01-01T00:00:00Z"),
            self._epoch("e1.fits", "M51", 202.47, 47.20, "2024-02-01T00:00:00Z"),
        ]
        monkeypatch.setattr(
            finder_chart.api_client, "get_source_tracks_batch",
            lambda source_ids: _async_return({"src1": epochs}),
        )
        monkeypatch.setattr(
            finder_chart.api_client, "upload_source_chart",
            lambda source_id, png_bytes, style, frame_count: _async_return(True),
        )

        captured_labels = []
        real_render = finder_chart._render_stamp_strip

        def spy_render_stamp_strip(loaded_epochs, label=None):
            captured_labels.append(label)
            return real_render(loaded_epochs, label=label)
        monkeypatch.setattr(finder_chart, "_render_stamp_strip", spy_render_stamp_strip)

        # No designation_by_source_id argument at all — must not raise, and
        # must not invent a designation out of nowhere.
        result = await finder_chart.update_charts_for_sources({"src1": ["UNKNOWN"]})

        assert result == {"src1": {"UNKNOWN": True}}
        assert captured_labels == ["UNKNOWN"]

    async def test_batches_multiple_sources_into_single_track_call(self, monkeypatch, tmp_path):
        """N sources use one track-fetch call; each chart is uploaded individually."""
        monkeypatch.setattr(config, "CHART_ENABLED", True)
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))

        obj_dir = tmp_path / "M51"
        obj_dir.mkdir()
        _make_wcs_fits(obj_dir / "e0.fits", ra=202.47, dec=47.20)
        _make_wcs_fits(obj_dir / "e1.fits", ra=202.50, dec=47.25)

        epochs_by_source = {
            "src1": [self._epoch("e0.fits", "M51", 202.47, 47.20)],
            "src2": [self._epoch("e1.fits", "M51", 202.50, 47.25)],
        }

        track_calls = []

        async def fake_get_tracks_batch(source_ids):
            track_calls.append(list(source_ids))
            return epochs_by_source

        monkeypatch.setattr(finder_chart.api_client, "get_source_tracks_batch", fake_get_tracks_batch)

        upload_calls = []

        async def fake_upload_chart(source_id, png_bytes, style, frame_count):
            upload_calls.append({"source_id": source_id, "style": style})
            return True

        monkeypatch.setattr(finder_chart.api_client, "upload_source_chart", fake_upload_chart)

        # Both sources here have exactly one epoch, so both render as
        # STYLE_BEFORE_AFTER — explicitly mocked (rather than relying on a
        # real, unreachable API call failing fast) so this test stays
        # deterministic. Both epochs share the same default object/obs_time
        # (see _epoch()'s defaults), so this must be called at most once —
        # see _get_earlier_frame_epoch()'s caching.
        nearest_before_calls = []

        async def fake_get_nearest_frame_before(object_name, before_time):
            nearest_before_calls.append((object_name, before_time))
            return None

        monkeypatch.setattr(finder_chart.api_client, "get_nearest_frame_before", fake_get_nearest_frame_before)

        result = await finder_chart.update_charts_for_sources(
            {"src1": ["UNKNOWN"], "src2": ["SUPERNOVA_CANDIDATE"]}
        )

        assert result == {"src1": {"UNKNOWN": True}, "src2": {"SUPERNOVA_CANDIDATE": True}}
        # Exactly one track-fetch call for both sources.
        assert len(track_calls) == 1
        assert sorted(track_calls[0]) == ["src1", "src2"]
        # Each chart uploaded individually — one call per source.
        assert len(upload_calls) == 2
        assert upload_calls[0]["style"] == finder_chart.STYLE_BEFORE_AFTER
        assert len(nearest_before_calls) == 1

    async def test_source_with_two_anomaly_types_gets_one_chart_per_style(self, monkeypatch, tmp_path):
        """Regression for the 2026-08-11 UI report: a source classified both
        MOVING_UNKNOWN (on one frame) and UNKNOWN (on another) over its
        lifetime must get BOTH its "track" and "stamp_strip" charts
        (re)generated — not just whichever type an upstream caller happened
        to pick."""
        monkeypatch.setattr(config, "CHART_ENABLED", True)
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))

        obj_dir = tmp_path / "C_2020_R4_ATLAS"
        obj_dir.mkdir()
        _make_wcs_fits(obj_dir / "e0.fits", ra=222.65, dec=32.62)
        _make_wcs_fits(obj_dir / "e1.fits", ra=222.66, dec=32.63)

        epochs = [
            self._epoch("e0.fits", "C_2020_R4_ATLAS", 222.65, 32.62, "2024-01-01T00:00:00Z"),
            self._epoch("e1.fits", "C_2020_R4_ATLAS", 222.66, 32.63, "2024-01-01T01:00:00Z"),
        ]
        monkeypatch.setattr(
            finder_chart.api_client, "get_source_tracks_batch",
            lambda source_ids: _async_return({"src1": epochs}),
        )

        upload_calls = []

        async def fake_upload_chart(source_id, png_bytes, style, frame_count):
            upload_calls.append({"source_id": source_id, "style": style})
            return True

        monkeypatch.setattr(finder_chart.api_client, "upload_source_chart", fake_upload_chart)

        result = await finder_chart.update_charts_for_sources({"src1": ["MOVING_UNKNOWN", "UNKNOWN"]})

        assert result == {"src1": {"MOVING_UNKNOWN": True, "UNKNOWN": True}}
        assert len(upload_calls) == 2
        assert {c["style"] for c in upload_calls} == {finder_chart.STYLE_TRACK, finder_chart.STYLE_STAMP_STRIP}

    async def test_duplicate_anomaly_types_in_one_source_render_only_once(self, monkeypatch, tmp_path):
        """Two items for the same source_id both carrying MOVING_UNKNOWN (e.g.
        two GENERATE_CHARTS task items referencing different anomaly_ids of
        the same type) must not upload the same "track" chart twice."""
        monkeypatch.setattr(config, "CHART_ENABLED", True)
        monkeypatch.setattr(config, "FITS_ARCHIVE", str(tmp_path))

        obj_dir = tmp_path / "M51"
        obj_dir.mkdir()
        _make_wcs_fits(obj_dir / "e0.fits", ra=202.47, dec=47.20)
        _make_wcs_fits(obj_dir / "e1.fits", ra=202.48, dec=47.21)

        epochs = [
            self._epoch("e0.fits", "M51", 202.47, 47.20, "2024-01-01T00:00:00Z"),
            self._epoch("e1.fits", "M51", 202.48, 47.21, "2024-01-01T01:00:00Z"),
        ]
        monkeypatch.setattr(
            finder_chart.api_client, "get_source_tracks_batch",
            lambda source_ids: _async_return({"src1": epochs}),
        )

        upload_calls = []

        async def fake_upload_chart(source_id, png_bytes, style, frame_count):
            upload_calls.append(style)
            return True

        monkeypatch.setattr(finder_chart.api_client, "upload_source_chart", fake_upload_chart)

        result = await finder_chart.update_charts_for_sources({"src1": ["MOVING_UNKNOWN", "MOVING_UNKNOWN"]})

        assert result == {"src1": {"MOVING_UNKNOWN": True}}
        assert upload_calls == [finder_chart.STYLE_TRACK]


async def _async_return(value):
    """Helper: an awaitable that resolves to `value` — for lambda-based async monkeypatches."""
    return value
