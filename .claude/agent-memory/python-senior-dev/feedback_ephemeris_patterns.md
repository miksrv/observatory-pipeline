---
name: Ephemeris module mock patterns
description: How to mock Horizons, simulate masked numpy values, and test partial-column failures in ephemeris tests
type: feedback
---

Mock target is always `modules.ephemeris.Horizons` (the class, not an instance).

The mock chain is: `mock_cls(return_value=instance)` → `instance.ephemerides()` returns a table mock → `table[0]` returns the row mock.

Row columns are accessed via `row["COLUMN_NAME"]`; use `row.__getitem__.side_effect = lambda key: values[key]` with a dict of `np.float64` values.

Masked values are simulated with `numpy.ma.masked` (the singleton constant), NOT `None` — the production code checks `npma.is_masked(val)`. Passing `None` to the module would not trigger the mask check.

To simulate a missing column (KeyError) while leaving other columns intact, override `__getitem__.side_effect` with a function that raises `KeyError` only for the target key and delegates to the original side_effect otherwise.

**Why:** Horizons returns astropy QTable rows where missing or inapplicable values appear as numpy masked scalars. The module must distinguish "value is zero" from "value is absent". Using `npma.masked` in tests ensures the production check (`npma.is_masked(val)`) is actually exercised.

**How to apply:** Use this pattern in all future Horizons-touching tests. Never use Python `None` as a stand-in for a masked column value when the production code uses `npma.is_masked()`.

---

Elevation unit conversion: config stores `SITE_ELEV` in metres; Horizons location dict expects kilometres. The module divides by 1000.0. Test assertion: `location["elevation"] == pytest.approx(config.SITE_ELEV / 1000.0)`.

No tenacity retries on the ephemeris module — JPL Horizons has strict rate limits. A single broad `try/except Exception` wraps the entire query body; on error, log a WARNING and return `None`.
