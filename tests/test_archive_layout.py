"""
Tests for modules/archive_layout.py — the FITS_ARCHIVE layout.
"""
from __future__ import annotations

import os

import pytest

import config
from modules import archive_layout


@pytest.fixture
def archive(tmp_path, monkeypatch):
    root = tmp_path / "archive"
    root.mkdir()
    monkeypatch.setattr(config, "FITS_ARCHIVE", str(root))
    return root


def _touch(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x")
    return str(path)


class TestNightDate:

    @pytest.mark.parametrize("obs_time, night", [
        ("2026-04-12T18:02:10", "2026-04-12"),
        ("2026-04-12T23:59:59", "2026-04-12"),
        ("2026-04-13T02:00:00", "2026-04-12"),   # after midnight → the evening before
        ("2026-04-13T11:59:59", "2026-04-12"),
        ("2026-04-13T12:00:00", "2026-04-13"),   # rollover at noon UTC
        ("2026-04-13T02:00:00.123456", "2026-04-12"),
    ])
    def test_night_rolls_over_at_noon_utc(self, obs_time, night):
        assert archive_layout.night_date(obs_time) == night

    @pytest.mark.parametrize("obs_time", [None, "", "not a date"])
    def test_missing_or_unreadable_time_is_unknown(self, obs_time):
        assert archive_layout.night_date(obs_time) == "_UNKNOWN"


class TestDirectories:

    def test_light_dir(self, archive):
        assert archive_layout.light_dir("M51", "Ha") == str(archive / "M51" / "Ha")

    def test_missing_filter_is_none(self, archive):
        assert archive_layout.light_dir("M51", None) == str(archive / "M51" / "NONE")
        assert archive_layout.light_dir("M51", "") == str(archive / "M51" / "NONE")

    def test_missing_object_is_unknown(self, archive):
        assert archive_layout.light_dir(None, "L") == str(archive / "_UNKNOWN" / "L")
        assert archive_layout.light_dir("_UNKNOWN", "L") == str(archive / "_UNKNOWN" / "L")

    def test_filter_is_one_safe_token(self):
        assert archive_layout.filter_token("Red 2/x") == "Red-2x"

    def test_calibration_dir(self, archive):
        assert archive_layout.calibration_dir("Dark", "2026-04-13T02:00:00") == str(
            archive / "_calibration" / "2026-04-12" / "Dark"
        )

    def test_destination_for_ignores_object_for_calibration(self, archive):
        header = {
            "object_name": "M51",
            "obs_time": "2026-04-12T18:30:05",
            "observation": {"frame_type": "Flat", "filter": "L"},
        }
        assert archive_layout.destination_for(header) == str(
            archive / "_calibration" / "2026-04-12" / "Flat"
        )

    def test_destination_for_light(self, archive):
        header = {"object_name": "M51", "observation": {"frame_type": "Light", "filter": "L"}}
        assert archive_layout.destination_for(header) == str(archive / "M51" / "L")


class TestFindArchived:

    def test_finds_a_light_in_its_filter_directory(self, archive):
        path = _touch(archive / "M51" / "L" / "a.fits")
        assert archive_layout.find_archived("a.fits", "M51") == path
        assert archive_layout.find_archived("a.fits") == path

    def test_finds_calibration(self, archive):
        path = _touch(archive / "_calibration" / "2026-04-12" / "Dark" / "cam_Dark.fits")
        assert archive_layout.find_archived("cam_Dark.fits") == path

    def test_legacy_flat_layout_still_resolves(self, archive):
        path = _touch(archive / "M51" / "a.fits")
        assert archive_layout.find_archived("a.fits", "M51") == path
        assert archive_layout.find_archived("a.fits") == path

    def test_the_new_layout_wins_over_a_legacy_copy(self, archive):
        _touch(archive / "M31" / "a.fits")
        new = _touch(archive / "M31" / "L" / "a.fits")
        assert archive_layout.find_archived("a.fits") == new

    def test_own_object_is_searched_first(self, archive):
        _touch(archive / "M31" / "L" / "a.fits")
        own = _touch(archive / "M51" / "L" / "a.fits")
        assert archive_layout.find_archived("a.fits", "M51") == own

    def test_missing_returns_none(self, archive):
        assert archive_layout.find_archived("nope.fits", "M51") is None

    def test_glob_characters_in_the_name_are_literal(self, archive):
        path = _touch(archive / "M51" / "L" / "a[1].fits")
        _touch(archive / "M51" / "L" / "a1.fits")
        assert archive_layout.find_archived("a[1].fits") == path

    def test_more_than_one_hit_is_logged(self, archive, caplog):
        first = _touch(archive / "M31" / "L" / "a.fits")
        _touch(archive / "M51" / "L" / "a.fits")
        assert archive_layout.find_archived("a.fits") == first
        assert "more than once" in caplog.text
