"""
tests/test_ephemeris.py — Unit tests for modules/ephemeris.py

All external calls are mocked at the module namespace level:
    patch("modules.ephemeris.Horizons")

asyncio_mode = auto in pytest.ini — no @pytest.mark.asyncio decorators needed.
"""

from __future__ import annotations

import math
from unittest.mock import MagicMock, patch

import numpy as np
import numpy.ma as npma
import pytest

import modules.ephemeris as ephemeris


# ---------------------------------------------------------------------------
# Constants shared across tests
# ---------------------------------------------------------------------------

_DESIGNATION = "2019 XY3"
_OBS_TIME    = "2024-03-15T22:01:34Z"

_EXPECTED_RA  = 123.456
_EXPECTED_DEC = 45.678
_EXPECTED_MAG = 17.8
_EXPECTED_AU  = 1.23
_EXPECTED_DRA  = 3.0   # arcsec/hour  (dRA*cosD)
_EXPECTED_DDEC = 4.0   # arcsec/hour  (dDec)
_EXPECTED_VEL  = 5.0   # sqrt(3^2 + 4^2)


# ---------------------------------------------------------------------------
# Helpers — build realistic mock Horizons ephemerides tables
# ---------------------------------------------------------------------------

def _make_eph_row(
    ra: float = _EXPECTED_RA,
    dec: float = _EXPECTED_DEC,
    v: float | None = _EXPECTED_MAG,
    delta: float | None = _EXPECTED_AU,
    dra: float | None = _EXPECTED_DRA,
    ddec: float | None = _EXPECTED_DDEC,
) -> MagicMock:
    """
    Return a MagicMock that behaves like a single Horizons ephemerides row.
    Individual column values are accessed as row["COLUMN_NAME"].
    Pass None for a column value to simulate a masked entry.
    """
    def _masked_or_val(v_in: float | None) -> object:
        if v_in is None:
            return npma.masked
        return np.float64(v_in)

    values = {
        "RA":       np.float64(ra),
        "DEC":      np.float64(dec),
        "V":        _masked_or_val(v),
        "delta":    _masked_or_val(delta),
        "dRA*cosD": _masked_or_val(dra),
        "dDec":     _masked_or_val(ddec),
    }

    row = MagicMock()
    row.__getitem__.side_effect = lambda key: values[key]
    return row


def _make_horizons_mock(row: MagicMock) -> MagicMock:
    """Return a mock that replaces the Horizons class."""
    eph_table = MagicMock()
    eph_table.__getitem__.return_value = row   # eph[0]

    instance = MagicMock()
    instance.ephemerides.return_value = eph_table

    mock_cls = MagicMock(return_value=instance)
    return mock_cls


# ===========================================================================
# Tests
# ===========================================================================

class TestEphemerisQuery:

    async def test_query_returns_ephemeris_dict(self):
        """Happy path: all columns present and unmasked — correct dict returned."""
        row = _make_eph_row()
        mock_horizons = _make_horizons_mock(row)

        with patch("modules.ephemeris.Horizons", mock_horizons):
            result = await ephemeris.query(_DESIGNATION, _OBS_TIME)

        assert result is not None
        assert set(result.keys()) == {
            "predicted_ra",
            "predicted_dec",
            "predicted_mag",
            "distance_au",
            "angular_velocity_arcsec_per_hour",
        }
        assert result["predicted_ra"]  == pytest.approx(_EXPECTED_RA)
        assert result["predicted_dec"] == pytest.approx(_EXPECTED_DEC)
        assert result["predicted_mag"] == pytest.approx(_EXPECTED_MAG)
        assert result["distance_au"]   == pytest.approx(_EXPECTED_AU)
        assert result["angular_velocity_arcsec_per_hour"] == pytest.approx(_EXPECTED_VEL)

    async def test_query_returns_none_on_network_error(self):
        """Any exception from Horizons is caught; query() returns None."""
        mock_cls = MagicMock()
        mock_cls.side_effect = Exception("timeout")

        with patch("modules.ephemeris.Horizons", mock_cls):
            result = await ephemeris.query(_DESIGNATION, _OBS_TIME)

        assert result is None

    async def test_query_handles_masked_magnitude(self):
        """When V column is masked, predicted_mag must be None."""
        row = _make_eph_row(v=None)
        mock_horizons = _make_horizons_mock(row)

        with patch("modules.ephemeris.Horizons", mock_horizons):
            result = await ephemeris.query(_DESIGNATION, _OBS_TIME)

        assert result is not None
        assert result["predicted_mag"] is None
        # Other fields must still be populated
        assert result["predicted_ra"] == pytest.approx(_EXPECTED_RA)
        assert result["distance_au"]  == pytest.approx(_EXPECTED_AU)

    async def test_query_handles_missing_delta_column(self):
        """When delta column is absent (KeyError), distance_au must be None."""
        row = _make_eph_row()
        # Override __getitem__ to raise KeyError specifically for "delta"
        original_side_effect = row.__getitem__.side_effect

        def _side_effect(key: str) -> object:
            if key == "delta":
                raise KeyError("delta")
            return original_side_effect(key)

        row.__getitem__.side_effect = _side_effect
        mock_horizons = _make_horizons_mock(row)

        with patch("modules.ephemeris.Horizons", mock_horizons):
            result = await ephemeris.query(_DESIGNATION, _OBS_TIME)

        assert result is not None
        assert result["distance_au"] is None
        # RA/Dec must still be correct
        assert result["predicted_ra"] == pytest.approx(_EXPECTED_RA)

    async def test_query_angular_velocity_computed_correctly(self):
        """dRA=3.0, dDec=4.0 → angular_velocity = 5.0 (Pythagorean triple)."""
        row = _make_eph_row(dra=3.0, ddec=4.0)
        mock_horizons = _make_horizons_mock(row)

        with patch("modules.ephemeris.Horizons", mock_horizons):
            result = await ephemeris.query(_DESIGNATION, _OBS_TIME)

        assert result is not None
        assert result["angular_velocity_arcsec_per_hour"] == pytest.approx(5.0)

    async def test_query_horizons_called_with_site_coords(self):
        """Horizons() must be instantiated with the config site coordinates."""
        row = _make_eph_row()
        mock_horizons = _make_horizons_mock(row)

        import config
        with patch("modules.ephemeris.Horizons", mock_horizons):
            await ephemeris.query(_DESIGNATION, _OBS_TIME)

        call_kwargs = mock_horizons.call_args
        location = call_kwargs.kwargs.get("location") or call_kwargs.args[1]
        assert location["lat"]       == config.SITE_LAT
        assert location["lon"]       == config.SITE_LON
        assert location["elevation"] == pytest.approx(config.SITE_ELEV / 1000.0)

    async def test_query_handles_masked_angular_velocity(self):
        """When dRA*cosD and dDec are masked, angular_velocity must be None."""
        row = _make_eph_row(dra=None, ddec=None)
        mock_horizons = _make_horizons_mock(row)

        with patch("modules.ephemeris.Horizons", mock_horizons):
            result = await ephemeris.query(_DESIGNATION, _OBS_TIME)

        assert result is not None
        assert result["angular_velocity_arcsec_per_hour"] is None

    async def test_query_returns_none_on_ephemerides_error(self):
        """If .ephemerides() raises, query() returns None without propagating."""
        instance = MagicMock()
        instance.ephemerides.side_effect = RuntimeError("connection refused")

        mock_cls = MagicMock(return_value=instance)

        with patch("modules.ephemeris.Horizons", mock_cls):
            result = await ephemeris.query(_DESIGNATION, _OBS_TIME)

        assert result is None
