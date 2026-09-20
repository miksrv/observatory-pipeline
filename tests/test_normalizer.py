"""
tests/test_normalizer.py — Unit tests for the normalizer module.
"""

import pytest
from modules import normalizer


class TestNormalizeObjectName:
    """Tests for normalize_object_name function."""

    def test_messier_variants(self):
        """Test Messier catalog normalization."""
        assert normalizer.normalize_object_name("M51")[0] == "M51"
        assert normalizer.normalize_object_name("M 51")[0] == "M51"
        assert normalizer.normalize_object_name("M_51")[0] == "M51"
        assert normalizer.normalize_object_name("M-51")[0] == "M51"
        assert normalizer.normalize_object_name("m51")[0] == "M51"
        assert normalizer.normalize_object_name("m_51")[0] == "M51"

    def test_ngc_variants(self):
        """Test NGC catalog normalization."""
        assert normalizer.normalize_object_name("NGC1234")[0] == "NGC1234"
        assert normalizer.normalize_object_name("NGC 1234")[0] == "NGC1234"
        assert normalizer.normalize_object_name("NGC_1234")[0] == "NGC1234"
        assert normalizer.normalize_object_name("ngc1234")[0] == "NGC1234"
        assert normalizer.normalize_object_name("ngc 1234")[0] == "NGC1234"

    def test_ic_variants(self):
        """Test IC catalog normalization."""
        assert normalizer.normalize_object_name("IC1234")[0] == "IC1234"
        assert normalizer.normalize_object_name("IC 1234")[0] == "IC1234"
        assert normalizer.normalize_object_name("ic_1234")[0] == "IC1234"

    def test_caldwell_variants(self):
        """Test Caldwell catalog normalization."""
        assert normalizer.normalize_object_name("C1")[0] == "C1"
        assert normalizer.normalize_object_name("C 1")[0] == "C1"
        assert normalizer.normalize_object_name("c_14")[0] == "C14"

    def test_sharpless_variants(self):
        """Test Sharpless catalog normalization."""
        assert normalizer.normalize_object_name("SH2-1")[0] == "SH2-1"
        assert normalizer.normalize_object_name("Sh2 1")[0] == "SH2-1"
        assert normalizer.normalize_object_name("sh_106")[0] == "SH2-106"

    def test_abell_variants(self):
        """Test Abell catalog normalization."""
        assert normalizer.normalize_object_name("Abell1")[0] == "Abell1"
        assert normalizer.normalize_object_name("ABELL 1")[0] == "Abell1"
        assert normalizer.normalize_object_name("abell_426")[0] == "Abell426"

    def test_custom_names(self):
        """Test custom object names."""
        assert normalizer.normalize_object_name("Andromeda Galaxy")[0] == "Andromeda_Galaxy"
        assert normalizer.normalize_object_name("Horsehead Nebula")[0] == "Horsehead_Nebula"

    def test_none_and_empty(self):
        """Test None and empty string handling."""
        assert normalizer.normalize_object_name(None)[0] == "_UNKNOWN"
        assert normalizer.normalize_object_name("")[0] == "_UNKNOWN"
        assert normalizer.normalize_object_name("   ")[0] == "_UNKNOWN"

    def test_preserves_original(self):
        """Test that original value is preserved."""
        norm, raw = normalizer.normalize_object_name("M 51")
        assert norm == "M51"
        assert raw == "M 51"


class TestNormalizeFilterName:
    """Tests for normalize_filter_name function."""

    def test_luminance_variants(self):
        """Test luminance filter normalization."""
        assert normalizer.normalize_filter_name("Luminance")[0] == "L"
        assert normalizer.normalize_filter_name("luminance")[0] == "L"
        assert normalizer.normalize_filter_name("Lum")[0] == "L"
        assert normalizer.normalize_filter_name("L")[0] == "L"
        assert normalizer.normalize_filter_name("Clear")[0] == "L"

    def test_rgb_variants(self):
        """Test RGB filter normalization."""
        assert normalizer.normalize_filter_name("Red")[0] == "R"
        assert normalizer.normalize_filter_name("RED")[0] == "R"
        assert normalizer.normalize_filter_name("r")[0] == "R"
        
        assert normalizer.normalize_filter_name("Green")[0] == "G"
        assert normalizer.normalize_filter_name("GREEN")[0] == "G"
        assert normalizer.normalize_filter_name("g")[0] == "G"
        
        assert normalizer.normalize_filter_name("Blue")[0] == "B"
        assert normalizer.normalize_filter_name("BLUE")[0] == "B"
        assert normalizer.normalize_filter_name("b")[0] == "B"

    def test_narrowband_variants(self):
        """Test narrowband filter normalization."""
        # H-alpha
        assert normalizer.normalize_filter_name("Ha")[0] == "Ha"
        assert normalizer.normalize_filter_name("H-Alpha")[0] == "Ha"
        assert normalizer.normalize_filter_name("Halpha")[0] == "Ha"
        assert normalizer.normalize_filter_name("Hydrogen-Alpha")[0] == "Ha"
        
        # OIII
        assert normalizer.normalize_filter_name("OIII")[0] == "OIII"
        assert normalizer.normalize_filter_name("O3")[0] == "OIII"
        assert normalizer.normalize_filter_name("[OIII]")[0] == "OIII"
        
        # SII
        assert normalizer.normalize_filter_name("SII")[0] == "SII"
        assert normalizer.normalize_filter_name("S2")[0] == "SII"
        assert normalizer.normalize_filter_name("[SII]")[0] == "SII"

    def test_filter_suffix_removal(self):
        """Test removal of ' Filter' suffix."""
        assert normalizer.normalize_filter_name("Red Filter")[0] == "R"
        assert normalizer.normalize_filter_name("Blue filter")[0] == "B"
        assert normalizer.normalize_filter_name("Luminance Filter")[0] == "L"

    def test_is_narrowband_true_for_emission_line_filters(self):
        """Ha/OIII/SII/NII (and raw header spellings thereof) are narrowband."""
        assert normalizer.is_narrowband("Ha") is True
        assert normalizer.is_narrowband("H-Alpha") is True
        assert normalizer.is_narrowband("OIII") is True
        assert normalizer.is_narrowband("[OIII]") is True
        assert normalizer.is_narrowband("SII") is True
        assert normalizer.is_narrowband("NII") is True
        assert normalizer.is_narrowband("[NII]") is True

    def test_is_narrowband_false_for_broadband_filters(self):
        """L/R/G/B/Johnson-Cousins/SDSS filters are not narrowband."""
        assert normalizer.is_narrowband("L") is False
        assert normalizer.is_narrowband("Luminance") is False
        assert normalizer.is_narrowband("R") is False
        assert normalizer.is_narrowband("G") is False
        assert normalizer.is_narrowband("B") is False
        assert normalizer.is_narrowband("V") is False
        assert normalizer.is_narrowband("r'") is False

    def test_is_narrowband_false_for_none_and_unknown(self):
        """Missing or unrecognized filter values must not be treated as narrowband."""
        assert normalizer.is_narrowband(None) is False
        assert normalizer.is_narrowband("SomeWeirdFilterName") is False

    def test_none_and_empty(self):
        """Test None and empty string handling."""
        assert normalizer.normalize_filter_name(None)[0] is None
        assert normalizer.normalize_filter_name("")[0] is None

    def test_preserves_original(self):
        """Test that original value is preserved."""
        norm, raw = normalizer.normalize_filter_name("Blue")
        assert norm == "B"
        assert raw == "Blue"


class TestNormalizeFrameType:
    """Tests for normalize_frame_type function."""

    def test_light_variants(self):
        """Test light frame normalization."""
        assert normalizer.normalize_frame_type("Light")[0] == "Light"
        assert normalizer.normalize_frame_type("light")[0] == "Light"
        assert normalizer.normalize_frame_type("LIGHT")[0] == "Light"
        assert normalizer.normalize_frame_type("Light Frame")[0] == "Light"
        assert normalizer.normalize_frame_type("Object")[0] == "Light"
        assert normalizer.normalize_frame_type("Science")[0] == "Light"

    def test_dark_variants(self):
        """Test dark frame normalization."""
        assert normalizer.normalize_frame_type("Dark")[0] == "Dark"
        assert normalizer.normalize_frame_type("dark")[0] == "Dark"
        assert normalizer.normalize_frame_type("Dark Frame")[0] == "Dark"

    def test_flat_variants(self):
        """Test flat frame normalization."""
        assert normalizer.normalize_frame_type("Flat")[0] == "Flat"
        assert normalizer.normalize_frame_type("flat")[0] == "Flat"
        assert normalizer.normalize_frame_type("Flat Field")[0] == "Flat"
        assert normalizer.normalize_frame_type("skyflat")[0] == "Flat"
        assert normalizer.normalize_frame_type("domeflat")[0] == "Flat"

    def test_bias_variants(self):
        """Test bias frame normalization."""
        assert normalizer.normalize_frame_type("Bias")[0] == "Bias"
        assert normalizer.normalize_frame_type("bias")[0] == "Bias"
        assert normalizer.normalize_frame_type("Zero")[0] == "Bias"
        assert normalizer.normalize_frame_type("Offset")[0] == "Bias"

    def test_none_and_empty(self):
        """Test None and empty string handling."""
        assert normalizer.normalize_frame_type(None)[0] is None
        assert normalizer.normalize_frame_type("")[0] is None

    def test_preserves_original(self):
        """Test that original value is preserved."""
        norm, raw = normalizer.normalize_frame_type("Light Frame")
        assert norm == "Light"
        assert raw == "Light Frame"


class TestGenerateNormalizedFilename:
    """
    Tests for generate_normalized_filename function.

    The frame-type token is the full normalized word (Light/Dark/Flat/Bias),
    not a one-letter code — changed deliberately in 3b4e369 ("updates
    normalized filename generation to use full frame type names and
    Light-only filter tokens"), which left these tests and CLAUDE.md/README
    still describing the old L/D/F/B codes.
    """

    def test_full_light_frame(self):
        """Test filename generation for light frame with all fields."""
        filename = normalizer.generate_normalized_filename(
            object_name="M51",
            frame_type="Light",
            filter_name="Ha",
            exptime=300.0,
            obs_time="2024-03-15T22:01:34",
        )
        assert filename == "M51_Light_Ha_300_2024-03-15T22-01-34.fits"

    def test_dark_frame_no_filter(self):
        """Test that dark frames exclude filter from filename."""
        filename = normalizer.generate_normalized_filename(
            object_name="M51",
            frame_type="Dark",
            filter_name="L",  # Should be ignored for dark frames
            exptime=300.0,
            obs_time="2024-03-15T22:01:34",
        )
        assert filename == "M51_Dark_300_2024-03-15T22-01-34.fits"

    def test_bias_frame_no_filter(self):
        """Test that bias frames exclude filter from filename."""
        filename = normalizer.generate_normalized_filename(
            object_name="_UNKNOWN",
            frame_type="Bias",
            filter_name="L",  # Should be ignored for bias frames
            exptime=0.0,
            obs_time="2024-03-15T22:01:34",
        )
        assert filename == "_UNKNOWN_Bias_0_2024-03-15T22-01-34.fits"

    def test_fractional_exptime(self):
        """Test fractional exposure time formatting."""
        filename = normalizer.generate_normalized_filename(
            object_name="M51",
            frame_type="Light",
            filter_name="L",
            exptime=0.5,
            obs_time="2024-03-15T22:01:34",
        )
        assert "0.5" in filename
        assert filename == "M51_Light_L_0.5_2024-03-15T22-01-34.fits"

    def test_sequence_number(self):
        """Test sequence number formatting."""
        filename = normalizer.generate_normalized_filename(
            object_name="M51",
            frame_type="Light",
            filter_name="L",
            exptime=120.0,
            obs_time="2024-03-15T22:01:34",
            sequence_num=5,
        )
        assert filename.endswith("_005.fits")

    def test_missing_fields(self):
        """Test filename generation with missing optional fields."""
        filename = normalizer.generate_normalized_filename(
            object_name="M51",
            frame_type=None,
            filter_name=None,
            exptime=None,
            obs_time=None,
        )
        # Should at least have object name and a timestamp
        assert filename.startswith("M51_")
        assert filename.endswith(".fits")


class TestNormalizeHeaders:
    """Tests for normalize_headers function."""

    def test_normalizes_fields_in_place(self):
        """Test that fields are normalized in place (replaced, not added)."""
        raw_headers = {
            "object_name": "M_51",
            "observation": {
                "object": "M 51",
                "filter": "Blue",
                "frame_type": "Light Frame",
            },
        }
        
        result = normalizer.normalize_headers(raw_headers)
        
        # Object name should be normalized
        assert result["object_name"] == "M51"
        
        # Observation object should be normalized
        assert result["observation"]["object"] == "M51"
        
        # Filter should be normalized
        assert result["observation"]["filter"] == "B"
        
        # Frame type should be normalized
        assert result["observation"]["frame_type"] == "Light"

    def test_preserves_other_fields(self):
        """Test that non-normalized fields are preserved."""
        raw_headers = {
            "object_name": "M_51",
            "obs_time": "2024-03-15T22:01:34",
            "ra": 123.456,
            "dec": 45.678,
            "observation": {
                "filter": "Blue",
                "frame_type": "Light Frame",
                "exptime": 120.0,
                "airmass": 1.2,
            },
        }
        
        result = normalizer.normalize_headers(raw_headers)
        
        # Non-normalized fields should be preserved
        assert result["obs_time"] == "2024-03-15T22:01:34"
        assert result["ra"] == 123.456
        assert result["dec"] == 45.678
        assert result["observation"]["exptime"] == 120.0
        assert result["observation"]["airmass"] == 1.2


class TestExtractObjectFromFilename:
    """
    Fallback used by pipeline.py when the OBJECT header is missing or empty.
    Audit 2026-08-18, finding H4: the loop terminated at the first pure-digit
    segment, taking it for an exposure time — but a numbered minor planet or
    star carries its number first, so those frames collected nothing and
    archived under "_UNKNOWN" instead of their real object directory,
    breaking history continuity for modules/subtraction.py.
    """

    @pytest.mark.parametrize("filename,expected", [
        ("UGC6930_Light_Luminance_300_secs_2021-01-01.fits", "UGC6930"),
        ("M51_L_L_60_2024-03-15T22-01-34.fits",              "M51"),
        ("Andromeda_Galaxy_Light_L_300_2024-03-15.fits",     "Andromeda_Galaxy"),
        ("NGC1234_Dark_300_2024-03-15.fits",                 "NGC1234"),
        ("M51_300_2024-03-15T22-01-34.fits",                 "M51"),
    ])
    def test_ordinary_names_are_unchanged(self, filename, expected):
        assert normalizer.extract_object_from_filename(filename) == expected

    @pytest.mark.parametrize("filename,expected", [
        ("4_Vesta_Light_L_120_2024-03-15T22-01-34.fits", "4_Vesta"),
        ("433_Eros_Light_L_300_2024-03-15.fits",         "433_Eros"),
        ("61_Cygni_Light_V_60_2024-03-15.fits",          "61_Cygni"),
    ])
    def test_numbered_designations_keep_their_number(self, filename, expected):
        assert normalizer.extract_object_from_filename(filename) == expected

    def test_a_leading_exposure_time_still_yields_unknown(self):
        """
        The guardrail: a timestamp segment contains letters ("...T22-01-34"),
        so the designation test checks that the next segment STARTS with one.
        A filename that really does lead with an exposure time must keep
        falling back to "_UNKNOWN" rather than swallowing the timestamp.
        """
        assert normalizer.extract_object_from_filename("300_2024-03-15T22-01-34.fits") == "_UNKNOWN"

    def test_no_usable_segment_yields_unknown(self):
        assert normalizer.extract_object_from_filename("Light_L_300_2024-03-15.fits") == "_UNKNOWN"


class TestMultiBandFilters:
    """
    Audit 2026-08-18, finding M9: L-eNhance, L-eXtreme, NBZ and the
    dual/tri/quad-band families each pass two or three emission lines and
    block everything between them, so where stars are concerned they are as
    narrow as a single-line filter. They were absent from FILTER_MAP and from
    NARROWBAND_FILTERS, so such a frame was held to the broadband
    QC_STARS_MIN and had a Gaia zero-point computed for it that no bandpass
    supports.
    """

    @pytest.mark.parametrize("raw,expected", [
        ("L-eNhance", "LeNhance"),
        ("l enhance", "LeNhance"),
        ("L-eXtreme", "LeXtreme"),
        ("L-Ultimate", "LuLtimate"),
        ("NBZ", "NBZ"),
        ("Quad Band", "QuadBand"),
        ("ALP-T", "QuadBand"),
        ("Tri-Band", "TriBand"),
        ("Triad", "TriBand"),
        ("Dual Band", "DuoBand"),
        ("Duo-Band", "DuoBand"),
    ])
    def test_normalization(self, raw, expected):
        assert normalizer.normalize_filter_name(raw)[0] == expected

    @pytest.mark.parametrize("raw", [
        "L-eNhance", "L-eXtreme", "L-Ultimate", "NBZ",
        "Quad Band", "Tri-Band", "Dual Band",
    ])
    def test_they_count_as_narrowband(self, raw):
        assert normalizer.is_narrowband(raw) is True

    def test_each_keeps_a_token_of_its_own(self):
        """
        modules/subtraction.py matches its reference stack on this field, and
        an L-eXtreme frame is not a substitute for an L-eNhance one.
        """
        tokens = {
            normalizer.normalize_filter_name(raw)[0]
            for raw in ("L-eNhance", "L-eXtreme", "L-Ultimate", "NBZ", "Quad Band", "Tri-Band", "Dual Band")
        }
        assert len(tokens) == 7

    @pytest.mark.parametrize("raw", ["L", "R", "G", "B", "V"])
    def test_broadband_filters_are_unaffected(self, raw):
        assert normalizer.is_narrowband(raw) is False


class TestAbellShorthand:
    """
    Audit 2026-08-18, finding M10: observers and planetarium software use
    "A39" and "Abell 39" interchangeably. Unrecognized, the same physical
    object arrived under two different object_names across sessions and its
    frames split across two archive directories — the same failure mode as
    H4, with the same consequence for subtraction's history.
    """

    @pytest.mark.parametrize("raw", ["A39", "A 39", "A_39", "a39", "Abell 39", "Abell_39"])
    def test_every_spelling_lands_in_one_directory(self, raw):
        assert normalizer.normalize_object_name(raw)[0] == "Abell39"

    def test_a_cluster_number_is_recognized(self):
        assert normalizer.normalize_object_name("A2151")[0] == "Abell2151"

    @pytest.mark.parametrize("raw", ["A807 FA", "A807_FA"])
    def test_an_old_style_minor_planet_designation_is_not_swallowed(self, raw):
        """
        "A807 FA" is the 1807 discovery, not Abell 807. The trailing letter
        group is what distinguishes them — the repo's own Vesta test data is
        filed under "Vesta_A807_FA".
        """
        assert normalizer.normalize_object_name(raw)[0] != "Abell807"

    def test_a_number_past_the_catalog_is_left_alone(self):
        """The Abell cluster catalog ends at 2712."""
        assert normalizer.normalize_object_name("A3000")[0] == "A3000"

    def test_a_bare_letter_is_left_alone(self):
        assert normalizer.normalize_object_name("A")[0] == "A"
