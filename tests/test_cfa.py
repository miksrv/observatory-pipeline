"""
tests/test_cfa.py — Unit tests for modules/cfa.py
"""

import os

import astropy.io.fits as fits
import numpy as np
import pytest

import config
from modules import cfa, fits_header

# Per-channel sky pedestals far enough apart that an unconverted mosaic's
# checkerboard would dominate any noise estimate (cf. the ASI585MC frames).
_PEDESTAL = {"R": 4500.0, "G": 5500.0, "B": 3700.0}


def _mosaic(pattern: str, height: int = 40, width: int = 60) -> np.ndarray:
    """A noiseless Bayer mosaic: each pixel carries its channel's pedestal."""
    cells = np.array([[_PEDESTAL[pattern[0]], _PEDESTAL[pattern[1]]],
                      [_PEDESTAL[pattern[2]], _PEDESTAL[pattern[3]]]])
    return np.tile(cells, ((height + 1) // 2, (width + 1) // 2))[:height, :width]


def _write(tmp_path, data: np.ndarray, **cards) -> str:
    path = str(tmp_path / "raw.fit")
    hdu = fits.PrimaryHDU(data=data.astype(np.uint16))  # BZERO=32768, as ASIAIR writes
    header = {"BAYERPAT": "RGGB", "XPIXSZ": 2.9, "YPIXSZ": 2.9, "FOCALLEN": 1568,
              "EGAIN": 0.94, "GAIN": 200, "XBINNING": 1, "YBINNING": 1,
              "RA": 339.58, "DEC": 34.56, "IMAGETYP": "Light", "OBJECT": "NGC 7331"}
    header.update(cards)
    for key, value in header.items():
        if value is not None:
            hdu.header[key] = value
    hdu.writeto(path)
    return path


def _convert(tmp_path, src: str):
    dest = str(tmp_path / "mono.fit.tmp")
    info = cfa.to_superpixel(src, dest)
    with fits.open(dest) as hdul:
        return info, np.asarray(hdul[0].data), hdul[0].header.copy()


class TestIsCfa:
    def test_bayer_mosaic(self):
        assert cfa.is_cfa(fits.Header({"NAXIS": 2, "BAYERPAT": "RGGB"}))

    def test_colortyp_alias(self):
        assert cfa.is_cfa(fits.Header({"NAXIS": 2, "COLORTYP": "gbrg"}))

    def test_mono_frame(self):
        assert not cfa.is_cfa(fits.Header({"NAXIS": 2}))

    def test_colour_cube_is_not_a_mosaic(self):
        assert not cfa.is_cfa(fits.Header({"NAXIS": 3, "BAYERPAT": "RGGB"}))

    def test_non_pattern_value(self):
        assert not cfa.is_cfa(fits.Header({"NAXIS": 2, "COLORTYP": "MONO"}))

    def test_already_converted(self):
        assert not cfa.is_cfa(fits.Header({"NAXIS": 2, "BAYERPAT": "RGGB", "CFACONV": True}))


class TestToSuperpixel:
    def test_checkerboard_disappears(self, tmp_path):
        _, mono, _ = _convert(tmp_path, _write(tmp_path, _mosaic("RGGB")))
        expected = (_PEDESTAL["R"] + 2 * _PEDESTAL["G"] + _PEDESTAL["B"]) / 4
        assert mono.shape == (20, 30)
        np.testing.assert_allclose(mono, expected)

    @pytest.mark.parametrize("pattern", ["RGGB", "BGGR", "GRBG", "GBRG"])
    def test_independent_of_bayer_phase(self, tmp_path, pattern):
        _, mono, header = _convert(tmp_path, _write(tmp_path, _mosaic(pattern), BAYERPAT=pattern))
        np.testing.assert_allclose(mono, (4500 + 2 * 5500 + 3700) / 4)
        assert header["CFAPAT"] == pattern

    def test_star_flux_is_conserved_per_mean(self, tmp_path):
        data = _mosaic("RGGB")
        data[10:14, 20:24] += 1000.0  # a 4×4 "star" = 16 000 ADU above the sky
        sky = (4500 + 2 * 5500 + 3700) / 4
        _, mono, _ = _convert(tmp_path, _write(tmp_path, data))
        assert (mono - sky).sum() == pytest.approx(16000 / 4)

    def test_saturated_subpixel_survives_averaging(self, tmp_path):
        data = _mosaic("RGGB")
        data[10, 10] = 65520  # one clipped pixel; the block mean would be ~20 000
        _, mono, _ = _convert(tmp_path, _write(tmp_path, data))
        assert mono[5, 5] >= config.SATURATION_ADU
        assert mono[5, 6] < config.SATURATION_ADU

    def test_saturated_block_count(self, tmp_path):
        data = _mosaic("RGGB")
        data[0, 0] = data[20, 20] = 65520
        info, _, _ = _convert(tmp_path, _write(tmp_path, data))
        assert info["saturated_blocks"] == 2

    def test_odd_dimensions_are_cropped(self, tmp_path):
        info, mono, _ = _convert(tmp_path, _write(tmp_path, _mosaic("RGGB", 41, 61)))
        assert info["native_shape"] == (41, 61)
        assert mono.shape == (20, 30)

    def test_pixel_geometry_and_gain(self, tmp_path):
        _, _, header = _convert(tmp_path, _write(tmp_path, _mosaic("RGGB")))
        assert header["XPIXSZ"] == pytest.approx(5.8)
        assert header["YPIXSZ"] == pytest.approx(5.8)
        assert header["XBINNING"] == 2
        assert header["EGAIN"] == pytest.approx(3.76)
        assert header["GAIN"] == 200  # vendor gain setting, not e-/ADU
        assert fits_header.resolve_pixel_scale_arcsec(header) == pytest.approx(
            206.265 * 5.8 / 1568, rel=1e-3)

    def test_mosaic_and_scaling_keywords_removed(self, tmp_path):
        src = _write(tmp_path, _mosaic("RGGB"), XBAYROFF=0, YBAYROFF=1)
        _, mono, header = _convert(tmp_path, src)
        for key in ("BAYERPAT", "XBAYROFF", "YBAYROFF", "BZERO", "BSCALE"):
            assert key not in header
        assert header["BITPIX"] == -32
        assert header["CFACONV"] is True
        assert not cfa.is_cfa(header)

    def test_capture_wcs_stripped_mount_pointing_kept(self, tmp_path):
        src = _write(tmp_path, _mosaic("RGGB"), CTYPE1="RA---TAN-SIP", CTYPE2="DEC--TAN-SIP",
                     CRPIX1=30.0, CRPIX2=20.0, CRVAL1=339.26, CRVAL2=34.35,
                     CD1_1=3e-5, CD1_2=1e-4, CD2_1=-1e-4, CD2_2=3e-5,
                     A_ORDER=2, A_0_2=5.7e-7, AP_0_0=0.0028, EQUINOX=2000.0)
        _, _, header = _convert(tmp_path, src)
        for key in ("CTYPE1", "CRPIX1", "CRVAL1", "CD1_1", "A_ORDER", "A_0_2", "AP_0_0"):
            assert key not in header
        assert header["RA"] == pytest.approx(339.58)
        assert header["DEC"] == pytest.approx(34.56)
        assert header["EQUINOX"] == 2000.0

    def test_missing_filter_becomes_osc(self, tmp_path):
        _, _, header = _convert(tmp_path, _write(tmp_path, _mosaic("RGGB")))
        assert header["FILTER"] == "OSC"

    def test_recorded_filter_is_kept(self, tmp_path):
        _, _, header = _convert(tmp_path, _write(tmp_path, _mosaic("RGGB"), FILTER="L-eNhance"))
        assert header["FILTER"] == "L-eNhance"

    def test_image_size_keywords_follow_new_shape(self, tmp_path):
        _, _, header = _convert(tmp_path, _write(tmp_path, _mosaic("RGGB"), IMAGEW=60, IMAGEH=40))
        assert (header["IMAGEW"], header["IMAGEH"]) == (30, 20)

    def test_source_is_left_untouched(self, tmp_path):
        src = _write(tmp_path, _mosaic("RGGB"))
        before = open(src, "rb").read()
        _convert(tmp_path, src)
        assert open(src, "rb").read() == before

    def test_mono_frame_is_rejected(self, tmp_path):
        src = _write(tmp_path, _mosaic("RGGB"), BAYERPAT=None)
        with pytest.raises(ValueError):
            cfa.to_superpixel(src, str(tmp_path / "out.tmp"))
        assert not os.path.exists(tmp_path / "out.tmp")

    def test_existing_destination_is_not_overwritten(self, tmp_path):
        src = _write(tmp_path, _mosaic("RGGB"))
        dest = tmp_path / "exists.tmp"
        dest.write_bytes(b"keep")
        with pytest.raises(OSError):
            cfa.to_superpixel(src, str(dest))
        assert dest.read_bytes() == b"keep"
