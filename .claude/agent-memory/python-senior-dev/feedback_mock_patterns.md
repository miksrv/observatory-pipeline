---
name: feedback_mock_patterns
description: Correct patterns for mocking sep, astropy fits, and astroscrappy in qc module tests
type: feedback
---

Python's dunder method dispatch for operators (`__rsub__`, `__sub__`, etc.) bypasses instance `__dict__` and looks up `type(obj).__dunder__`. Setting `mock_instance.__rsub__ = lambda ...` on a `MagicMock` instance does NOT work for operator dispatch — numpy's ndarray will still see `MagicMock.__rsub__` from the class, returning another Mock instead of an ndarray.

**Rule:** Never mock `sep.Background` with a plain `MagicMock` when the test exercises `data - bkg`. Use a dedicated class that defines `__rsub__` at the class level.

**Why:** Discovered during `test_qc.py` implementation — `sep.Background(data)` mocked as `MagicMock` caused `data - bkg` to return a `(0,)` shaped Mock, which then broke `sep.extract` with `broadcast` errors. All tests that exercised the full pipeline path failed silently with BAD flag.

**How to apply:** Use the `_FakeBackground` pattern (class with `__rsub__` defined on the class, not instance). Similarly create `_FakeHDU` / `_FakeHeader` / `_FakeHDUL` classes instead of MagicMocks when the mock must support subscript access (`hdul[0]`) and dict-like `.get()`.

**Trail flag FWHM collision:** With plate scale ~1 arcsec/px and `QC_FWHM_MAX_ARCSEC=8.0`, semi-axes `a=5, b=1` produce FWHM ≈ 8.49 arcsec — this inadvertently triggers BLUR alongside TRAIL, yielding BAD. Use `a=4, b=1` (FWHM ≈ 6.9 arcsec) for a clean TRAIL-only test case. Document this in the test docstring.
