"""
Tests for tools/migrate_archive_layout.py.
"""
from __future__ import annotations

import importlib.util
import os

import numpy as np
from astropy.io import fits

_TOOL = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tools", "migrate_archive_layout.py")
_spec = importlib.util.spec_from_file_location("migrate_archive_layout", _TOOL)
migrate_archive_layout = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migrate_archive_layout)


def _write(path, **cards):
    path.parent.mkdir(parents=True, exist_ok=True)
    hdu = fits.PrimaryHDU(np.zeros((4, 4), dtype=np.float32))
    for key, value in cards.items():
        hdu.header[key] = value
    hdu.writeto(str(path))
    return path


def _files(root):
    return sorted(
        os.path.relpath(os.path.join(d, f), root) for d, _, fs in os.walk(root) for f in fs
    )


def _legacy_archive(root):
    _write(root / "M51" / "M51_Light_L_300_2026-04-12T22-15-03.fits",
           OBJECT="M51", IMAGETYP="Light", FILTER="Luminance", EXPTIME=300.0,
           **{"DATE-OBS": "2026-04-12T22:15:03"})
    _write(root / "M51" / "M51_Light_300_2026-04-12T22-20-03.fits",
           OBJECT="M51", IMAGETYP="Light", EXPTIME=300.0, **{"DATE-OBS": "2026-04-12T22:20:03"})
    _write(root / "M51" / "M51_Dark_300_2026-04-13T02-10-41.fits",
           OBJECT="M51", IMAGETYP="Dark", EXPTIME=300.0, INSTRUME="ZWO ASI1600MM",
           **{"DATE-OBS": "2026-04-13T02:10:41"})
    _write(root / "M51" / "M51_Flat_L_3_2026-04-12T18-30-05.fits",
           OBJECT="M51", IMAGETYP="Flat", FILTER="L", EXPTIME=3.0, INSTRUME="ZWO ASI1600MM",
           **{"DATE-OBS": "2026-04-12T18:30:05"})
    (root / "M51" / "M51_Light_L_300_2026-04-12T22-15-03.wcs").write_text("side file")
    (root / "M51" / "broken.fits").write_bytes(b"not a fits file")


def test_migrates_lights_by_filter_and_calibration_by_night(tmp_path):
    root = tmp_path / "archive"
    _legacy_archive(root)

    skipped = migrate_archive_layout.migrate(str(root), dry_run=False)

    assert skipped == 1
    assert _files(root) == sorted([
        "M51/L/M51_Light_L_300_2026-04-12T22-15-03.fits",
        "M51/NONE/M51_Light_300_2026-04-12T22-20-03.fits",
        "M51/M51_Light_L_300_2026-04-12T22-15-03.wcs",   # side files stay
        "M51/broken.fits",                                # unreadable: reported, not moved
        "_calibration/2026-04-12/Dark/ZWO-ASI1600MM_Dark_300_2026-04-13T02-10-41.fits",
        "_calibration/2026-04-12/Flat/ZWO-ASI1600MM_Flat_L_3_2026-04-12T18-30-05.fits",
    ])


def test_dry_run_changes_nothing(tmp_path, capsys):
    root = tmp_path / "archive"
    _legacy_archive(root)
    before = _files(root)

    migrate_archive_layout.migrate(str(root), dry_run=True)

    assert _files(root) == before
    assert "_calibration" in capsys.readouterr().out


def test_second_run_moves_nothing(tmp_path):
    root = tmp_path / "archive"
    _legacy_archive(root)
    migrate_archive_layout.migrate(str(root), dry_run=False)
    after_first = _files(root)

    migrate_archive_layout.migrate(str(root), dry_run=False)

    assert _files(root) == after_first


def test_never_overwrites_and_removes_emptied_directories(tmp_path):
    root = tmp_path / "archive"
    for obj in ("M51", "M31"):
        _write(root / obj / f"{obj}_Dark_300_2026-04-12T20-00-00.fits",
               IMAGETYP="Dark", EXPTIME=300.0, INSTRUME="ASI1600MM",
               **{"DATE-OBS": "2026-04-12T20:00:00"})

    migrate_archive_layout.migrate(str(root), dry_run=False)

    assert _files(root) == [
        "_calibration/2026-04-12/Dark/ASI1600MM_Dark_300_2026-04-12T20-00-00.fits",
        "_calibration/2026-04-12/Dark/ASI1600MM_Dark_300_2026-04-12T20-00-00_1.fits",
    ]
    assert not (root / "M51").exists() and not (root / "M31").exists()
