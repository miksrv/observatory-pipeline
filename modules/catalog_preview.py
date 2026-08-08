"""
modules/catalog_preview.py — visual diagnostic for catalog-matching quality.

The single public entry point is:

    await catalog_preview.render(fits_path: str) -> dict

Runs a FITS frame through the real pipeline steps up to catalog matching
(qc -> astrometry -> subtraction -> catalog_matcher), WITHOUT any API calls
or file moves — this never registers a frame, never archives/rejects the
input file, and never touches fits_path itself — then renders every
detected source (sources_all — the loose filter catalog_matcher/
anomaly_detector actually use) as a circle on the frame:
  - Green circle + label "CatalogName: id" for a matched source
  - Red circle, no label, for an unmatched source

Circles are drawn at each source's ORIGINALLY DETECTED (ra, dec) — i.e.
before catalog_matcher's WCS-offset correction shifts source["ra"]/["dec"]
in-place for cross-matching purposes — so they line up with what's actually
visible on this frame's own pixel data.

Nothing here writes a file that outlives this call: the PNG is rendered
straight into an in-memory buffer and returned as bytes (the caller — see
pipeline.preview_catalog_match() — uploads it to observatory-api, which is
where it actually lives), and astap's .ini/.wcs/.log side files (written via
astrometry.solve()'s `output_base`) land in a throwaway temporary directory
that's removed before this function returns, regardless of success or
failure. There is deliberately no "save a copy locally" option — the
PREVIEW_CATALOG_MATCH task type's whole point is a result you look at via
the API, not a local file to go hunt for on the observatory server.

Backs the PREVIEW_CATALOG_MATCH task type (see pipeline.preview_catalog_match()
and worker.py). Calling modules/catalog_matcher.py directly (not a separate
copy of the matching logic) means repeated frames of the same object/session
benefit from its on-disk cache exactly the same way a production ANALYZE run
would — re-running this on many frames of one field re-hits Gaia/Simbad/
2MASS/Pan-STARRS/MPC only for the first one per sky tile, not every frame.
"""
from __future__ import annotations

import io
import logging
import os
import tempfile

import matplotlib
matplotlib.use("Agg")  # headless: no display available (or wanted) on the observatory server
import matplotlib.pyplot as plt
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.visualization import AsinhStretch, ImageNormalize, ZScaleInterval

import config
from modules import astrometry, catalog_matcher, qc, subtraction
from modules.fits_header import sanitize_object_name

logger = logging.getLogger(__name__)


def _stretch(data: np.ndarray) -> np.ndarray:
    vmin, vmax = ZScaleInterval().get_limits(data)
    return ImageNormalize(vmin=vmin, vmax=vmax, stretch=AsinhStretch())(data)


async def render(fits_path: str) -> dict:
    """
    Render a catalog-match diagnostic PNG for one FITS frame.

    Parameters
    ----------
    fits_path:
        Path to the FITS file. Never modified, moved, or removed — safe to
        point at a file still sitting in FITS_INCOMING, or an already
        archived/rejected one.

    Returns
    -------
    dict
        {"png_bytes": bytes, "matched": int, "total": int,
         "quality_flag": str} — `matched`/`total` are the diagnostic's
        headline numbers (also captioned on the image itself).

    Raises
    ------
    RuntimeError
        If astrometry.solve() fails to produce a WCS solution — there's no
        useful image to render circles onto without one. The caller (see
        pipeline.preview_catalog_match()) is expected to catch this and
        report the item as FAILED, same as any other stage's per-item
        failure handling.
    """
    filename = os.path.basename(fits_path)
    extra = {"fits_filename": filename}

    with fits.open(fits_path) as hdul:
        header = hdul[0].header
        object_name = sanitize_object_name(header.get("OBJECT"))
        filter_name = header.get("FILTER") or header.get("FILTNAM") or header.get("FILTERID")
        obs_time = header.get("DATE-OBS", "")

    # move_on_reject=False: this module must never touch the input frame —
    # qc.analyze() otherwise moves a non-OK frame to FITS_REJECTED itself,
    # on its own, before returning (see debug/README.md's "Background" for
    # the incident that established this convention).
    qc_result = await qc.analyze(fits_path, move_on_reject=False)
    quality_flag = qc_result.get("quality_flag", "BAD")
    logger.info(
        "QC: flag=%s fwhm=%s elongation=%s stars=%s",
        quality_flag,
        qc_result.get("fwhm_median"),
        qc_result.get("elongation_median"),
        qc_result.get("star_count"),
        extra=extra,
    )

    # astap's .ini/.wcs/.log side files need SOME directory to land in
    # (output_base below) — a throwaway temp dir that's removed on the way
    # out, success or failure, rather than anywhere inside the repo/archive.
    with tempfile.TemporaryDirectory(prefix="catalog_preview_") as scratch_dir:
        output_base = os.path.join(scratch_dir, os.path.splitext(filename)[0])
        astro = await astrometry.solve(
            fits_path, psf_fwhm_arcsec=qc_result.get("fwhm_median"), output_base=output_base,
        )
        if not astro:
            raise RuntimeError(f"Astrometry failed for {filename} — no WCS solution to render against")

        logger.info(
            "Astrometry: center=(%.5f, %.5f) fov=%.4f sources=%d sources_all=%d",
            astro["ra_center"], astro["dec_center"], astro["fov_deg"],
            len(astro["sources"]), len(astro["sources_all"]),
            extra=extra,
        )

        sources = list(astro["sources_all"])  # loose filter, same list catalog_matcher/anomaly_detector use

        archive_dir = os.path.join(config.FITS_ARCHIVE, object_name)
        try:
            sub_result = await subtraction.run(fits_path, archive_dir, filter_name)
            logger.info(
                "Subtraction: performed=%s reference_frame_count=%d candidates=%d",
                sub_result.get("performed"),
                sub_result.get("reference_frame_count", 0),
                len(sub_result.get("candidates", [])),
                extra=extra,
            )
            for c in sub_result.get("candidates", []):
                c["_from_subtraction"] = True
                sources.append(c)
        except Exception as exc:
            logger.warning("Subtraction skipped/failed: %s", exc, extra=extra)

        frame_meta = {
            "filename": filename,
            "ra_center": astro["ra_center"],
            "dec_center": astro["dec_center"],
            "fov_deg": astro["fov_deg"],
            "obs_time": obs_time,
        }

        # catalog_matcher.match() shifts source["ra"]/["dec"] in-place by the
        # computed WCS-offset correction (needed for cross-matching against
        # catalogs) — keep the ORIGINAL detected position for plotting, since
        # this frame's own pixel WCS was never changed and circles must line
        # up with what's actually visible on THIS image, not the
        # Gaia-corrected sky position.
        for s in sources:
            s["_plot_ra"], s["_plot_dec"] = s["ra"], s["dec"]

        sources = await catalog_matcher.match(sources, frame_meta)

        n_matched = sum(1 for s in sources if s.get("catalog_name"))
        logger.info("Catalog matching: matched=%d/%d", n_matched, len(sources), extra=extra)

        # ------------------------------------------------------------------
        # Render
        # ------------------------------------------------------------------
        # Must reuse astro["wcs"] — the WCS astrometry.solve() actually used
        # to derive every source's ra/dec (astap's own fresh .wcs sidecar,
        # preferred over the file's own header). Re-reading
        # WCS(hdul[0].header) here instead would re-derive it from
        # fits_path's own (never rewritten — this module must never touch
        # its input) header, which can disagree with astro["wcs"] — sources
        # would still MATCH correctly (matching happens in sky coordinates),
        # but every circle would be plotted off the actual star, since
        # world_to_pixel() would be going through a different WCS than the
        # one that produced _plot_ra/_plot_dec.
        with fits.open(fits_path) as hdul:
            data = hdul[0].data.astype(np.float32)
        wcs = astro["wcs"]

        fig, ax = plt.subplots(figsize=(14, 11), dpi=130)
        try:
            ax.imshow(_stretch(data), cmap="gray", origin="lower")

            for s in sources:
                x, y = wcs.world_to_pixel(SkyCoord(ra=s["_plot_ra"], dec=s["_plot_dec"], unit="deg"))
                matched = s.get("catalog_name") is not None
                color = "#40ff40" if matched else "#ff4040"
                ax.add_patch(plt.Circle((float(x), float(y)), radius=14, edgecolor=color, facecolor="none", linewidth=1.2))
                if matched:
                    label = f'{s["catalog_name"]}:{s.get("catalog_id", "")}'
                    ax.annotate(label, (float(x), float(y)), xytext=(float(x) + 16, float(y) + 16),
                                color=color, fontsize=6)

            ax.set_title(
                f"{filename}\nmatched={n_matched}/{len(sources)}  "
                f"(green=matched, red=unmatched)", fontsize=10,
            )
            ax.set_xticks([])
            ax.set_yticks([])
            fig.tight_layout()

            buf = io.BytesIO()
            fig.savefig(buf, format="png")
            png_bytes = buf.getvalue()
        finally:
            # Unlike a one-shot CLI script (whose process exit reclaims this
            # either way), this module runs inside worker.py's long-lived
            # process across many files/tasks — leaving figures open would
            # leak memory for as long as the worker keeps running.
            plt.close(fig)

    # `scratch_dir` (and every astap side file in it) is already gone by
    # this point — the `with tempfile.TemporaryDirectory()` block above
    # exited, success or failure, on the way to getting here.

    logger.info("Rendered diagnostic PNG (%d bytes) for %s", len(png_bytes), filename, extra=extra)

    return {
        "png_bytes": png_bytes,
        "matched": n_matched,
        "total": len(sources),
        "quality_flag": quality_flag,
    }
