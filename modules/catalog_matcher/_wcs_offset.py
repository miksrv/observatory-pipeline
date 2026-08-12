"""
modules/catalog_matcher/_wcs_offset.py — the systematic WCS offset (dRA,
dDec) vote-accumulator, computed against Gaia DR3 before any catalog is
actually matched.

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import logging
import math

import astropy.units as u
import numpy as np
from astropy.coordinates import SkyCoord, search_around_sky

import config

logger = logging.getLogger(__name__)


def _compute_wcs_offset(sources: list[dict], gaia_stars: list[dict]) -> tuple[float, float]:
    """
    Compute and return the systematic WCS offset (dRA, dDec) in degrees.

    Matches all detected sources against the Gaia catalog using a 2D vote
    accumulator on (dRA, dDec) vectors. True source→star pairs vote for the
    same offset bin and produce a sharp histogram peak; random/false matches
    are scattered uniformly and create only background noise.

    Returns (offset_ra_deg, offset_dec_deg). Returns (0.0, 0.0) when:
      - Not enough sources or Gaia stars to compute reliably
      - Median separation is already within tolerance (no correction needed)
      - No statistically significant peak found (field dominated by galaxies, etc.)

    The caller is responsible for applying the returned offset to source
    coordinates before any catalog matching.
    """
    if not gaia_stars or not sources:
        return 0.0, 0.0

    source_coords = SkyCoord(
        ra=[s["ra"] for s in sources] * u.deg,
        dec=[s["dec"] for s in sources] * u.deg,
    )
    gaia_coords = SkyCoord(
        ra=[g["ra"] for g in gaia_stars] * u.deg,
        dec=[g["dec"] for g in gaia_stars] * u.deg,
    )

    # ------------------------------------------------------------------
    # Quick nearest-neighbour pass — logging and early-exit only.
    # ------------------------------------------------------------------
    idx_nn, sep2d_nn, _ = source_coords.match_to_catalog_sky(gaia_coords)
    sep_nn = sep2d_nn.to(u.arcsec).value

    within_5  = int(np.sum(sep_nn <= 5.0))
    within_10 = int(np.sum(sep_nn <= 10.0))
    within_30 = int(np.sum(sep_nn <= 30.0))
    within_60 = int(np.sum(sep_nn <= 60.0))
    min_sep    = float(np.min(sep_nn))    if len(sep_nn) > 0 else 0.0
    max_sep    = float(np.max(sep_nn))    if len(sep_nn) > 0 else 0.0
    median_sep = float(np.median(sep_nn)) if len(sep_nn) > 0 else 0.0

    logger.info(
        "Gaia match (raw): min=%.2f\" median=%.2f\" max=%.2f\"  "
        "within 5\"=%d, 10\"=%d, 30\"=%d, 60\"=%d (threshold=%.1f\")",
        min_sep, median_sep, max_sep, within_5, within_10, within_30, within_60,
        config.MATCH_CONE_ARCSEC,
    )

    # Early exit when the WCS is already accurate enough that no
    # correction could meaningfully improve source positions.
    #
    # This threshold must be at or below the vote accumulator's own noise
    # floor (the ``total_offset <= 2.0`` guard further down) — the
    # accumulator never applies a correction smaller than 2″ anyway, so
    # running it for median_sep < 2″ only wastes CPU. Setting this higher
    # (the original hard-coded 10″, or even MATCH_CONE_ARCSEC at 5″) leaves
    # real 3–4″ systematic offsets uncorrected: sources technically still
    # fall inside the matching cone, but their stored positions in the
    # database are off by that much, degrading anomaly detector's positional
    # comparisons, finder chart overlays, and Aladin cross-checks.
    #
    # 2.0″ gives maximum correction accuracy: every detectable systematic
    # shift above the centroiding noise floor gets corrected, while the
    # accumulator's own significance tests (MIN_PEAK_VOTES, Poisson σ,
    # total_offset ≤ 2″) still prevent applying noise as a "correction".
    WCS_OFFSET_MIN_ARCSEC = 2.0
    if median_sep <= WCS_OFFSET_MIN_ARCSEC:
        # WCS is already accurate — no correction needed
        return 0.0, 0.0

    # ------------------------------------------------------------------
    # All-pairs vote accumulator
    #
    # The nearest-neighbour approach fails for sparse fields with large
    # WCS offsets: with only ~10 sources and an offset of 30-60", each
    # source's nearest Gaia star is typically a WRONG star at ~10" away,
    # so every source votes for a different random offset → peak = 1.
    #
    # The fix: use search_around_sky to collect ALL (source, Gaia-star)
    # pairs within MAX_SEARCH_RADIUS. True pairs (correct star at the
    # systematic offset distance) all vote for the same histogram bin and
    # produce a sharp peak. False pairs (nearby wrong stars) are scattered
    # uniformly and create only background noise.
    # ------------------------------------------------------------------
    MAX_SEARCH_ARCSEC = 90.0
    idx_src, idx_cat, _, _ = search_around_sky(
        source_coords, gaia_coords, MAX_SEARCH_ARCSEC * u.arcsec
    )
    n_pairs = len(idx_src)

    if n_pairs < 3:
        logger.debug(
            "Too few source-Gaia pairs (%d) within %.0f\" — cannot compute WCS offset",
            n_pairs, MAX_SEARCH_ARCSEC,
        )
        return 0.0, 0.0

    logger.info(
        "WCS offset search: %d source-Gaia pairs within %.0f\" (%d sources, %d Gaia stars)",
        n_pairs, MAX_SEARCH_ARCSEC, len(sources), len(gaia_stars),
    )

    mean_dec = float(np.mean([s["dec"] for s in sources]))
    cos_dec  = math.cos(math.radians(mean_dec))

    dra_arcsec  = np.array([
        (gaia_stars[idx_cat[k]]["ra"]  - sources[idx_src[k]]["ra"])  * cos_dec * 3600.0
        for k in range(n_pairs)
    ])
    ddec_arcsec = np.array([
        (gaia_stars[idx_cat[k]]["dec"] - sources[idx_src[k]]["dec"]) * 3600.0
        for k in range(n_pairs)
    ])

    # ------------------------------------------------------------------
    # Bin size and significance test.
    #
    # Real incident (2026-08-06, "Vesta_A807_FA" field, 40 sources vs 4453
    # Gaia stars in a ~1° frame — i.e. ~10000 stars/deg², so almost EVERY
    # source has several Gaia stars within 90" by pure chance):
    #   - True offset (confirmed independently against ASTAP's own solve
    #     log): dRA≈+18" dDec≈-60" (total≈63").
    #   - With the old BIN_SIZE=2" bins, the ~15-20 true pairs spread out
    #     over several adjacent 2"-bins (real seeing/centroid scatter on
    #     top of the systematic offset), so no single bin held more than
    #     2 votes — below even the old "≥3 votes" floor. Meanwhile a
    #     random noise bin elsewhere reached 2 votes just from the sheer
    #     number of background pairs (245 pairs over 8100 bins), and the
    #     old `expected_bg * 20` threshold is a flat multiplicative rule
    #     that — at this bin size — was *more* permissive for that noise
    #     bin than for the real, larger, but more spread-out peak.
    #
    # Fix: use a coarser bin (COARSE_BIN_ARCSEC) so real pairs sharing the
    # same systematic offset land in one bin despite a few arcsec of
    # individual scatter, and replace the flat "20x background" rule with
    # a proper Poisson excess test (peak must clear background by several
    # σ) *plus* an absolute floor on the vote count — the σ-only test is
    # unreliable at very low background (sqrt(tiny number) is tiny, so
    # even a 2-vote noise bin can look "many σ" above almost-zero
    # background, which is exactly the false peak seen above).
    # ------------------------------------------------------------------
    COARSE_BIN_ARCSEC = 15.0
    RANGE    = MAX_SEARCH_ARCSEC
    n_bins   = int(2 * RANGE / COARSE_BIN_ARCSEC)

    H, ra_edges, dec_edges = np.histogram2d(
        dra_arcsec, ddec_arcsec,
        bins=n_bins,
        range=[[-RANGE, RANGE], [-RANGE, RANGE]],
    )
    peak_i, peak_j = np.unravel_index(np.argmax(H), H.shape)
    peak_count  = int(H[peak_i, peak_j])
    expected_bg = n_pairs / float(n_bins ** 2)

    MIN_PEAK_VOTES = 5     # absolute floor — never trust a 2-3 vote "peak"
    SIGMA_MARGIN   = 5.0   # peak must exceed background by this many Poisson σ
    sig_threshold = expected_bg + SIGMA_MARGIN * math.sqrt(max(expected_bg, 0.5))

    peak_dra  = float((ra_edges[peak_i]  + ra_edges[peak_i + 1])  / 2.0)
    peak_ddec = float((dec_edges[peak_j] + dec_edges[peak_j + 1]) / 2.0)
    total_offset = math.sqrt(peak_dra ** 2 + peak_ddec ** 2)

    logger.info(
        "WCS offset accumulator: %d pairs → peak=%d (bg≈%.3f, threshold=%.1f, "
        "min_votes=%d) at dRA=%.1f\" dDec=%.1f\" (total=%.1f\")",
        n_pairs, peak_count, expected_bg, sig_threshold, MIN_PEAK_VOTES,
        peak_dra, peak_ddec, total_offset,
    )

    if peak_count < MIN_PEAK_VOTES or peak_count < sig_threshold or total_offset <= 2.0:
        logger.debug(
            "No significant WCS offset detected (peak=%d < threshold=%.1f or votes, "
            "offset=%.1f\"). This is expected for galaxy-rich fields where most "
            "detections are extended sources not present in Gaia.",
            peak_count, sig_threshold, total_offset,
        )
        return 0.0, 0.0

    # Refine: iterative sigma-clip-style median around the coarse peak.
    #
    # A single median over the ±COARSE_BIN_ARCSEC window (the old behaviour)
    # is still contaminated by background pairs caught in that same wide
    # window — in the real incident above, that pulled the one-shot estimate
    # to (12.6", -57.3") vs. a true offset around (21", -57"). Re-centering
    # the window on each new estimate and re-taking the median sheds most of
    # that contamination within a few passes, since true pairs stay inside
    # a tight window around the true offset while background pairs fall out
    # once the window re-centers. REFINE_RADIUS_ARCSEC is deliberately
    # tighter than COARSE_BIN_ARCSEC for this reason.
    REFINE_RADIUS_ARCSEC = 10.0
    refined_dra, refined_ddec = peak_dra, peak_ddec
    for _ in range(15):
        near_mask = (
            (np.abs(dra_arcsec  - refined_dra)  <= REFINE_RADIUS_ARCSEC) &
            (np.abs(ddec_arcsec - refined_ddec) <= REFINE_RADIUS_ARCSEC)
        )
        if not near_mask.any():
            break
        new_dra  = float(np.median(dra_arcsec[near_mask]))
        new_ddec = float(np.median(ddec_arcsec[near_mask]))
        if math.isclose(new_dra, refined_dra, abs_tol=0.05) and math.isclose(new_ddec, refined_ddec, abs_tol=0.05):
            refined_dra, refined_ddec = new_dra, new_ddec
            break
        refined_dra, refined_ddec = new_dra, new_ddec

    offset_ra_deg  = refined_dra  / (cos_dec * 3600.0)
    offset_dec_deg = refined_ddec / 3600.0

    logger.info(
        "WCS offset detected: dRA=%.2f\" dDec=%.2f\" (total=%.2f\") — "
        "will be applied to all source coordinates before catalog matching",
        refined_dra, refined_ddec, math.sqrt(refined_dra ** 2 + refined_ddec ** 2),
    )
    return offset_ra_deg, offset_dec_deg
