"""
modules/astrometry — Plate solving and source extraction for FITS frames.

The single public entry point is:

    await astrometry.solve(fits_path: str) -> dict

It calls the astap binary for plate solving (writing WCS keywords back into
the FITS file), then reads the WCS, computes the frame centre and FOV, and
runs sep (SourceExtractor) to build a source list with (RA, Dec) coordinates.

Returns an empty dict on any failure so the pipeline can detect and handle
the error without crashing.

Split into one file per step of `solve()`'s own pipeline (promoting the
original module's "Step 1/2/3/4" comments into actual function boundaries):

  _astap.py           Step 1 — run the astap binary, confirm a solution
  _wcs.py             Step 2 — read/validate the resulting WCS
  _frame_geometry.py  Step 3 — frame centre, FOV, pixel scale from that WCS
  _extraction.py      Step 4 — sep source extraction + star filtering
  _streak.py          the streak-masking pre-pass _extraction.py calls
                       before its real sep.extract() (see config.STREAK_*)

`solve()` itself stays here as the orchestrator: it runs the four steps in
order, wraps steps 2–4 in the same single try/except the original monolithic
function used (so any exception from WCS reading, frame geometry, or source
extraction still produces exactly one "Astrometry post-processing failed"
log line and an empty dict — not a new one per step).

`subprocess`, `astropy.io.fits` (as `fits`), `sep`, and `os` are imported
here too (unused directly) so that
``patch("modules.astrometry.subprocess.run", ...)``,
``patch("modules.astrometry.fits.open", ...)``,
``patch("modules.astrometry.sep.Background", ...)``/``sep.extract``, and
``patch("modules.astrometry.os.path.exists", ...)`` — the mocking strategy
tests/test_astrometry.py uses — resolve regardless of which submodule
actually calls them. The one exception is ``WCS`` (imported as a bare name
via ``from astropy.wcs import WCS``, not a module attribute chain): tests
patch it directly on the submodule that constructs it —
``patch("modules.astrometry._wcs.WCS", ...)`` — since a bare-imported class
name doesn't survive being patched on a different file the way a shared
module's own attribute does. See
`.claude/agent-memory/python-senior-dev/feedback_module_to_package_split.md`.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import astropy.io.fits as fits  # noqa: F401 — see docstring above; patch() target resolution.
import sep  # noqa: F401 — see docstring above; patch() target resolution.
import subprocess  # noqa: F401 — see docstring above; patch() target resolution.

from ._astap import _run_astap
from ._extraction import _extract_sources
from ._frame_geometry import _frame_center_and_scale
from ._wcs import _read_wcs

logger = logging.getLogger(__name__)


async def solve(
    fits_path: str,
    psf_fwhm_arcsec: float | None = None,
    output_base: str | None = None,
) -> dict[str, Any]:
    """
    Plate-solve a FITS frame and extract calibrated source positions.

    Runs astap for plate solving, then reads the WCS it writes back into
    the file, computes the frame centre and FOV, and runs sep for source
    extraction.  All (x, y) pixel positions are converted to (RA, Dec)
    using the solved WCS.

    Parameters
    ----------
    fits_path:
        Absolute path to the FITS file on disk.
    psf_fwhm_arcsec:
        Median PSF FWHM in arcseconds from QC analysis. When provided, the
        star filter's FWHM bounds are tightened around it: the upper bound to
        ``psf_fwhm_arcsec * 1.5`` (capped at ``STAR_FWHM_MAX_ARCSEC``) to
        better reject compact galaxies and other extended sources whose FWHM
        significantly exceeds stellar PSF, and the lower bound to
        ``psf_fwhm_arcsec / 1.5`` (floored at ``STAR_FWHM_MIN_ARCSEC``) to
        reject hot/warm pixel clusters and other artifacts that are far
        sharper than any real star in this frame.
    output_base:
        Base path (no extension) astap should write its own output files
        under — ``-o`` on the astap command line. astap only ever opens
        ``fits_path`` itself for writing when invoked with ``-update``
        (which we never pass, here or anywhere else in this module); without
        it, ``-o`` affects only where the ``.ini``/``.wcs``/``.log`` side
        files land, never the input. Defaults to None, which keeps astap's
        own default of writing them next to ``fits_path`` (production
        behaviour, unchanged) — debug/debug_catalog_match.py passes a
        scratch path here instead, so repeatedly running it doesn't litter
        the debug frame's own directory with side files.

    Returns
    -------
    On success, a dict with keys:
        ra_center   float   – frame centre RA in decimal degrees
        dec_center  float   – frame centre Dec in decimal degrees
        fov_deg     float   – field of view (larger image dimension) in degrees
        naxis1      int     – image width in pixels
        naxis2      int     – image height in pixels
        sources     list    – list of source dicts; each has:
                              ra, dec, flux, fwhm (arcsec), elongation (a/b),
                              saturated (bool — peak ADU >= config.SATURATION_ADU;
                              see docs/ISSUES.md #2),
                              near_edge (bool — pixel position within
                              config.EDGE_MARGIN_FRAC of any frame edge; lets
                              anomaly_detector.py demand stronger elongation
                              evidence there, since coma inflates it near the
                              edge of a wide-field frame)
        wcs         WCS     – astropy WCS object for downstream coordinate work

    Before source extraction, a coarse streak-masking pre-pass (see
    ``_streak._build_streak_mask()`` and ``config.STREAK_*``) removes satellite/
    aircraft trails and bright-star diffraction-spike arms from the image so
    they cannot fragment into spurious point-like "stars" in either
    ``sources`` or ``sources_all``.

    Returns ``{}`` on any failure (astap error, WCS invalid, sep failure).
    """
    fits_filename = os.path.basename(fits_path)
    logger.info("Starting astrometry for fits_filename=%s", fits_filename)

    # ------------------------------------------------------------------
    # Step 1 — Run astap plate solver
    # ------------------------------------------------------------------
    if not _run_astap(fits_path, output_base):
        return {}

    # ------------------------------------------------------------------
    # Steps 2–4 — WCS extraction, centre/FOV computation, sep extraction
    # ------------------------------------------------------------------
    try:
        # Step 2 — Read WCS
        wcs_result = _read_wcs(fits_path, output_base)
        if wcs_result is None:
            return {}
        wcs, naxis1, naxis2 = wcs_result

        # Step 3 — Frame centre and FOV
        ra_center, dec_center, fov_deg, pixel_scale_arcsec = _frame_center_and_scale(
            wcs, naxis1, naxis2, fits_filename
        )

        # Step 4 — Source extraction with sep
        sources, sources_all = _extract_sources(
            fits_path, wcs, pixel_scale_arcsec, naxis1, naxis2,
            psf_fwhm_arcsec, fits_filename,
        )

        return {
            "ra_center":   ra_center,
            "dec_center":  dec_center,
            "fov_deg":     fov_deg,
            "naxis1":      naxis1,
            "naxis2":      naxis2,
            "sources":     sources,      # strict stars: for photometry calibration only
            "sources_all": sources_all,  # all detections: for catalog matching + anomaly detection
            "wcs":         wcs,
        }

    except Exception as exc:
        logger.error(
            "Astrometry post-processing failed for %s: %s", fits_path, exc
        )
        return {}
