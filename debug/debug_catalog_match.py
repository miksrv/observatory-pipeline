"""
debug/debug_catalog_match.py — catalog-matching visual test tool.

See debug/README.md for what this directory is for and how to use this
script. In short: runs a single FITS frame through the real pipeline steps
up to catalog matching (qc -> astrometry -> subtraction -> catalog_matcher),
WITHOUT any API calls or file moves, then renders every detected source
(sources_all — the loose filter catalog_matcher/anomaly_detector actually
use) as a circle on the frame:
  - Green circle + label "CatalogName: id" for a matched source
  - Red circle, no label, for an unmatched source

Circles are drawn at each source's ORIGINALLY DETECTED (ra, dec) — i.e.
before catalog_matcher's WCS-offset correction shifts source["ra"]/["dec"]
in-place for cross-matching purposes — so they line up with what's actually
visible on this frame's own pixel data.

Usage:
    python debug/debug_catalog_match.py <path-to-fits> [output.png]

If output.png is omitted, the image is saved to
debug/output/<fits-stem>.png (created automatically).
"""
from __future__ import annotations

import asyncio
import os
import sys

# Allow running as `python debug/debug_catalog_match.py ...` from anywhere —
# make the project root (parent of this file's directory) importable so
# `import config` / `from modules import ...` resolve regardless of cwd.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.visualization import AsinhStretch, ImageNormalize, ZScaleInterval

import config
from modules import astrometry, catalog_matcher, qc, subtraction
from modules.fits_header import sanitize_object_name


def _stretch(data: np.ndarray) -> np.ndarray:
    vmin, vmax = ZScaleInterval().get_limits(data)
    return ImageNormalize(vmin=vmin, vmax=vmax, stretch=AsinhStretch())(data)


async def main(fits_path: str, out_png: str) -> None:
    filename = os.path.basename(fits_path)

    with fits.open(fits_path) as hdul:
        header = hdul[0].header
        object_name = sanitize_object_name(header.get("OBJECT"))
        filter_name = header.get("FILTER") or header.get("FILTNAM") or header.get("FILTERID")
        obs_time = header.get("DATE-OBS", "")

    print(f"=== QC: {filename}  (object={object_name}) ===")
    # move_on_reject=False: this tool must never touch the input frame (see
    # module docstring above) — qc.analyze() otherwise moves a non-OK frame
    # to FITS_REJECTED itself, on its own, before returning (found the hard
    # way: 2026-08-06, see debug/README.md "Background").
    qc_result = await qc.analyze(fits_path, move_on_reject=False)
    print(f"  quality_flag={qc_result.get('quality_flag')}  "
          f"fwhm={qc_result.get('fwhm_median')}  elongation={qc_result.get('elongation_median')}  "
          f"stars={qc_result.get('star_count')}  snr_median={qc_result.get('snr_median')}  "
          f"sky_background={qc_result.get('sky_background')}")

    print("=== Astrometry ===")
    # output_base: keep astap's .ini/.wcs/.log side files out of the debug
    # frame's own directory — they'd otherwise pile up there on every rerun.
    # Doesn't touch fits_path itself (see astrometry.solve()'s docstring).
    output_dir = os.path.dirname(out_png) or "."
    os.makedirs(output_dir, exist_ok=True)
    output_base = os.path.join(output_dir, os.path.splitext(filename)[0])
    astro = await astrometry.solve(
        fits_path, psf_fwhm_arcsec=qc_result.get("fwhm_median"), output_base=output_base,
    )
    if not astro:
        print("  astrometry FAILED — aborting")
        return
    print(f"  center=({astro['ra_center']:.5f}, {astro['dec_center']:.5f})  "
          f"fov={astro['fov_deg']:.4f}  sources={len(astro['sources'])}  "
          f"sources_all={len(astro['sources_all'])}")

    sources = list(astro["sources_all"])  # loose filter, same list catalog_matcher/anomaly_detector use

    archive_dir = os.path.join(config.FITS_ARCHIVE, object_name)
    print(f"=== Subtraction (reference: {archive_dir}) ===")
    try:
        sub_result = await subtraction.run(fits_path, archive_dir, filter_name)
        print(f"  performed={sub_result.get('performed')}  "
              f"reference_frame_count={sub_result.get('reference_frame_count')}  "
              f"candidates={len(sub_result.get('candidates', []))}")
        for c in sub_result.get("candidates", []):
            c["_from_subtraction"] = True
            sources.append(c)
    except Exception as exc:
        print(f"  subtraction skipped/failed: {exc}")

    frame_meta = {
        "filename":    filename,
        "ra_center":   astro["ra_center"],
        "dec_center":  astro["dec_center"],
        "fov_deg":     astro["fov_deg"],
        "obs_time":    obs_time,
    }

    # catalog_matcher.match() shifts source["ra"]/["dec"] in-place by the
    # computed WCS-offset correction (needed for cross-matching against
    # catalogs) — keep the ORIGINAL detected position for plotting, since
    # this frame's own pixel WCS was never changed and circles must line up
    # with what's actually visible on THIS image, not the Gaia-true position.
    for s in sources:
        s["_plot_ra"], s["_plot_dec"] = s["ra"], s["dec"]

    print(f"=== Catalog matching: {len(sources)} sources ===")
    sources = await catalog_matcher.match(sources, frame_meta)

    n_matched = sum(1 for s in sources if s.get("catalog_name"))
    print(f"  matched={n_matched}/{len(sources)}")

    # ------------------------------------------------------------------
    # Render
    # ------------------------------------------------------------------
    # Must reuse astro["wcs"] — the WCS astrometry.solve() actually used to
    # derive every source's ra/dec (astap's own fresh .wcs sidecar,
    # preferred over the file's own header since the 2026-08-06 UGC_6930
    # fix). Re-reading WCS(hdul[0].header) here instead would re-derive it
    # from fits_path's own (never rewritten — this script must never touch
    # its input) header, which can now disagree with astro["wcs"] by the
    # same offset that fix was about — sources would still MATCH correctly
    # (matching happens in sky coordinates), but every circle would be
    # plotted off the actual star, since world_to_pixel() would be going
    # through a different WCS than the one that produced _plot_ra/_plot_dec.
    with fits.open(fits_path) as hdul:
        data = hdul[0].data.astype(np.float32)
    wcs = astro["wcs"]

    fig, ax = plt.subplots(figsize=(14, 11), dpi=130)
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
    ax.set_xticks([]); ax.set_yticks([])
    fig.tight_layout()

    os.makedirs(os.path.dirname(out_png) or ".", exist_ok=True)
    fig.savefig(out_png)
    print(f"Saved {out_png}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    in_fits = sys.argv[1]
    if len(sys.argv) > 2:
        default_out = sys.argv[2]
    else:
        stem = os.path.splitext(os.path.basename(in_fits))[0]
        default_out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", f"{stem}.png")

    asyncio.run(main(in_fits, default_out))
