"""
modules/astrometry — Plate solving and source extraction for FITS frames.

The single public entry point is:

    await astrometry.solve(fits_path: str) -> dict

It calls the astap binary for plate solving, then reads the WCS it produced,
computes the frame centre and FOV, and runs sep (SourceExtractor) to build a
source list with (RA, Dec) coordinates.

astap is invoked **without** `-update`, so it never writes into `fits_path`
itself — the solved WCS lands in a `.wcs` side file next to the frame (plus
`.ini`/`.log`), or under `output_base` when one is given, and `_wcs.py` reads
it back from there. This docstring used to say the opposite ("writing WCS
keywords back into the FITS file"), which matters because it is exactly the
question `_wcs.py` has to answer — whether the WCS it reads is astap's fresh
solve or the capture software's mount-pointing estimate that was already in
the header (the 2026-08-06 UGC_6930 incident; audit 2026-08-18, finding L3).
The frame's header does get a WCS written into it eventually, but by
`pipeline.py`'s `_write_solved_wcs()` at archive time, not by astap.

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
    psf_fwhm_px: float | None = None,
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

        Prefer ``psf_fwhm_px`` below when the caller has it: this value is
        only as good as the plate scale it was converted with, and QC's own
        conversion uses the frame's *headers*.
    psf_fwhm_px:
        The same median PSF FWHM, in raw pixels — ``qc.analyze()``'s
        ``fwhm_median_px``. When given, it wins over ``psf_fwhm_arcsec``:
        this function converts it with the plate scale it has just solved
        for, which is the same scale every extracted source's own
        ``fwhm`` is measured against a few lines later.

        The distinction matters whenever the header's optical setup is
        wrong — an unaccounted focal reducer or Barlow, a camera swapped
        without updating ``XPIXSZ``, a wrong ``FOCALLEN`` — which the solve
        detects and the header does not. The anchor was then skewed by the
        ratio between the two scales while the FWHMs it gates were not,
        so the bounds either rejected every real star in the frame or
        stopped rejecting anything (audit 2026-08-18, finding M16). It also
        lets a frame whose headers carry no plate scale at all (an all-sky
        lens with no ``XPIXSZ``/``FOCALLEN``) have an anchor for the first
        time, where before it had none.
    output_base:
        Base path (no extension) astap should write its own output files
        under — ``-o`` on the astap command line. astap only ever opens
        ``fits_path`` itself for writing when invoked with ``-update``
        (which we never pass, here or anywhere else in this module); without
        it, ``-o`` affects only where the ``.ini``/``.wcs``/``.log`` side
        files land, never the input. Defaults to None, which keeps astap's
        own default of writing them next to ``fits_path`` (production
        behaviour, unchanged) — a caller doing repeated ad hoc solves against
        the same frame can pass a scratch path here instead, so it doesn't
        litter that frame's own directory with side files.

    Returns
    -------
    On success, a dict with keys:
        ra_center   float   – frame centre RA in decimal degrees
        dec_center  float   – frame centre Dec in decimal degrees
        fov_deg     float   – field of view in degrees: the larger of the
                              two axes' own angular extents (each axis'
                              pixel count times that axis' own plate
                              scale — they differ under anisotropic
                              binning; see _frame_geometry.py)
        position_angle_deg  float | None – this frame's own orientation on the
                              sky (0 = North up, increasing clockwise toward
                              +X — see _frame_geometry._position_angle_deg()'s
                              docstring); None if the WCS round-trip used to
                              derive it failed. Two frames differing by ~180
                              here are rotated relative to each other (e.g. a
                              meridian flip) — modules/subtraction.py uses
                              this to coarse-pre-rotate a reference frame
                              before the fine astroalign registration; it is
                              also persisted to the API (docs/API.md §1) for
                              operator diagnostics without re-opening FITS
                              files. Not a reason to exclude any frame from
                              anything on its own.
        pixel_scale_arcsec  float – the solved plate scale (geometric mean
                              of both axes; see _frame_geometry.py). Returned
                              so a caller can re-anchor a header-derived
                              measurement — e.g. QC's FWHM — onto the scale
                              this solve actually found.
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
    if not await _run_astap(fits_path, output_base):
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

        # Step 3 — Frame centre, FOV, and position angle
        ra_center, dec_center, fov_deg, pixel_scale_arcsec, position_angle_deg = (
            _frame_center_and_scale(wcs, naxis1, naxis2, fits_filename)
        )

        # Re-anchor the PSF estimate onto the scale we have just solved
        # for. QC measured it in pixels and converted with whatever the
        # headers claimed; the FWHM bounds it gates are compared against
        # source FWHMs computed with *this* scale, so the two have to come
        # from the same place (audit 2026-08-18, finding M16).
        if psf_fwhm_px is not None and psf_fwhm_px > 0 and pixel_scale_arcsec > 0:
            solved_psf_fwhm_arcsec = psf_fwhm_px * pixel_scale_arcsec
            if (
                psf_fwhm_arcsec is not None
                and psf_fwhm_arcsec > 0
                and abs(solved_psf_fwhm_arcsec - psf_fwhm_arcsec) > 0.05 * psf_fwhm_arcsec
            ):
                logger.warning(
                    "PSF anchor re-scaled for %s: %.3f\"/px solved vs the headers' own "
                    "scale puts the QC FWHM at %.3f\" rather than %.3f\". Using the solved "
                    "value — the header's optical setup disagrees with the plate solve "
                    "(an unaccounted reducer/Barlow, a swapped camera, a wrong FOCALLEN).",
                    fits_filename, pixel_scale_arcsec,
                    solved_psf_fwhm_arcsec, psf_fwhm_arcsec,
                )
            psf_fwhm_arcsec = solved_psf_fwhm_arcsec

        # Step 4 — Source extraction with sep
        sources, sources_all = _extract_sources(
            fits_path, wcs, pixel_scale_arcsec, naxis1, naxis2,
            psf_fwhm_arcsec, fits_filename,
        )

        return {
            "ra_center":         ra_center,
            "dec_center":        dec_center,
            "fov_deg":           fov_deg,
            "pixel_scale_arcsec": pixel_scale_arcsec,
            "position_angle_deg": position_angle_deg,
            "naxis1":            naxis1,
            "naxis2":            naxis2,
            "sources":           sources,      # strict stars: for photometry calibration only
            "sources_all":       sources_all,  # all detections: for catalog matching + anomaly detection
            "wcs":               wcs,
        }

    except Exception as exc:
        logger.error(
            "Astrometry post-processing failed for %s: %s", fits_path, exc
        )
        return {}
