"""
modules/anomaly_detector/_classify.py — the priority-ordered per-source
classification logic. See docs/anomaly-detector.md's "Classification
priority" section for the full flowchart/table this function implements.

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import logging

import config

from ._geometry import _find_sources_within_radius, _tile_key
from ._history import _history_median_mag, _same_filter_history
from ._movement import _is_position_shifted
from ._otypes import _is_binary_star, _is_galaxy, _is_variable_star
from .types import AnomalyType

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-source classification (using prefetched data)
# ---------------------------------------------------------------------------

def _classify_source_sync(
    source: dict,
    frame_id: str,
    log_filename: str,
    history_by_tile: dict[tuple, list],
    coverage_by_tile: dict[tuple, list],
    current_frame_positions: list[tuple[float, float]],
) -> dict | None:
    """
    Classify a single source using PREFETCHED batch data (synchronous).

    No API calls are made here - all data comes from the batch prefetch.

    Returns an anomaly dict, or None if no reportable anomaly is found.
    """
    ra  = float(source["ra"])
    dec = float(source["dec"])
    mag: float | None = source.get("mag")

    catalog_name:     str | None = source.get("catalog_name")
    catalog_id:       str | None = source.get("catalog_id")
    object_type:      str | None = source.get("object_type")
    elongation:       float      = float(source.get("elongation", 0.0))
    from_subtraction: bool       = bool(source.get("_from_subtraction", False))
    # Set by astrometry.py / subtraction.py from the detection's own pixel
    # position vs. config.EDGE_MARGIN_FRAC — see the SPACE_DEBRIS branch
    # below for why this matters (coma inflates elongation near the edge of
    # a wide-field frame, independent of any real motion). No leading
    # underscore (unlike "_from_subtraction" above) — persisted on the wire
    # like "saturated", so a standalone DETECT_ANOMALIES re-run
    # (pipeline.py's _from_wire_source()) can reconstruct it too.
    near_edge:        bool       = bool(source.get("near_edge", False))
    # This frame's own filter (pipeline.py's Step 5.5 / _from_wire_source()),
    # used below to restrict the Δmag comparison to same-filter history —
    # see _same_filter_history()'s docstring for why.
    source_filter:    str | None = source.get("_filter")
    # Resolved sources.id from POST /frames/{id}/sources — see pipeline.py's
    # Step 7, which zips the API's returned `source_ids` back onto each
    # source dict as "_source_id". None if that round-trip failed/mismatched
    # or this source was skipped by the API (invalid ra/dec).
    source_id:        str | None = source.get("_source_id")

    extra = {"frame_id": frame_id, "log_filename": log_filename}
    tile = _tile_key(ra, dec)

    # Narrow-cone (MATCH_CONE_ARCSEC) history at THIS source's own position.
    # Computed once up front — Priority 2 needs it to gate the position-shift
    # check (see _is_position_shifted's docstring), and Priority 3 needs the
    # exact same query for the UNKNOWN/FIRST_OBSERVATION/KNOWN_CATALOG_NEW
    # distinction and for delta_mag, so there is no reason to run it twice.
    tile_sources = history_by_tile.get(tile, [])
    history = _find_sources_within_radius(ra, dec, config.MATCH_CONE_ARCSEC, tile_sources)
    n_history = len(history)

    # ------------------------------------------------------------------
    # Priority 1 — MPC-matched moving objects
    # ------------------------------------------------------------------

    if catalog_name == "MPC":
        anomaly_type = AnomalyType.ASTEROID if object_type == "ASTEROID" else AnomalyType.COMET

        logger.info(
            "Classified as %s: designation=%s ra=%.4f dec=%.4f",
            anomaly_type, catalog_id, ra, dec,
            extra=extra,
        )

        return {
            "anomaly_type":    anomaly_type,
            "source_id":       source_id,
            "ra":              ra,
            "dec":             dec,
            "magnitude":       mag,
            "delta_mag":       None,
            "mpc_designation": catalog_id,
            "ephemeris":       None,
            "notes":           f"Matched MPC object '{catalog_id}' (type: {object_type})",
            "_needs_ephemeris": True,
        }

    # ------------------------------------------------------------------
    # Priority 2 — Position-shifted unmatched moving objects
    # ------------------------------------------------------------------

    if catalog_name is None:
        # A saturated, uncatalogued detection is almost certainly a
        # bright-star / astroalign residual artifact, not a real transient
        # or moving object — see docs/ISSUES.md #1 and #2. This check is
        # deliberately scoped to catalog_name is None: an MPC- or
        # Simbad-matched source (e.g. a genuinely bright asteroid or a known
        # star flaring) is a legitimate detection and must still be
        # classified normally, just without a computed magnitude (see
        # photometry.py, which never measures a saturated source).
        if bool(source.get("saturated")):
            logger.debug(
                "Suppressed: saturated + uncatalogued detection ra=%.4f dec=%.4f "
                "— treated as bright-star/subtraction artifact, not a real "
                "transient (see docs/ISSUES.md #1, #2)",
                ra, dec,
                extra=extra,
            )
            return None

        # Wide-cone (MOVING_CONE_ARCSEC) history — candidates for "this used
        # to be somewhere nearby". _is_position_shifted() itself gates on
        # `history` being empty (this exact spot is new) and additionally
        # requires that a wide-cone candidate's own position is no longer
        # occupied by anything in THIS frame — see its docstring for why
        # "any nearby historical detection" alone is not sufficient evidence
        # of a mover (docs/ISSUES.md #1).
        wide_history = _find_sources_within_radius(ra, dec, config.MOVING_CONE_ARCSEC, tile_sources)

        # A trail this elongated is, on its own, sufficient evidence of a
        # fast single-exposure mover (satellite / space debris) — unlike a
        # slow asteroid-like point source, it never "used to be" anywhere
        # nearby in an earlier frame, because its entire visible track exists
        # only within this one exposure. _is_position_shifted()'s "a wide-cone
        # historical detection's old position has vacated" requirement
        # (condition 2) structurally can never be satisfied for it, since
        # there was never a prior detection near either endpoint of the trail
        # to begin with — so gating SPACE_DEBRIS behind that check meant a
        # genuine satellite/debris trail always fell through to generic
        # UNKNOWN instead (real incident, 2026-08-07, C_2020_R4_ATLAS frames:
        # several frame-spanning trails were reported as UNKNOWN with a
        # stamp_strip/blink chart rather than SPACE_DEBRIS with a track chart).
        # Still gated on `history` (condition 1) being empty — a recurring
        # elongated artifact or extended object sitting at the SAME position
        # every frame must not be swept in here; that case correctly belongs
        # to Priority 3 below.
        #
        # The elongation bar itself is edge-aware: a source flagged
        # `near_edge` (see astrometry.py/subtraction.py) uses the much
        # higher SPACE_DEBRIS_EDGE_ELONGATION_MIN instead of the ordinary
        # SPACE_DEBRIS_ELONGATION_MIN. Coma and other off-axis aberrations
        # progressively stretch a perfectly ordinary, non-moving star's PSF
        # toward the edges/corners of a wide-field frame, inflating its
        # measured elongation for purely optical reasons — real incident,
        # 2026-08-07: 4 T_CrB frames produced 305 anomalies, the vast
        # majority being coma-elongated but otherwise ordinary corner stars
        # firing this exact shortcut with no real motion at all. A genuine
        # single-exposure satellite/debris trail is typically far more
        # elongated than coma alone produces, so raising the bar near the
        # edge (rather than removing the elongation-alone shortcut there
        # entirely) keeps real edge-of-frame trails detectable while
        # filtering out the aberration.
        trail_elongation_min = (
            config.SPACE_DEBRIS_EDGE_ELONGATION_MIN if near_edge
            else config.SPACE_DEBRIS_ELONGATION_MIN
        )
        if not history and elongation > trail_elongation_min:
            anomaly_type = AnomalyType.SPACE_DEBRIS

            logger.warning(
                "ALERT — %s: unmatched trail-like source, elongation alone is "
                "sufficient (no position-shift evidence needed) ra=%.4f dec=%.4f "
                "elongation=%.2f near_edge=%s threshold=%.2f",
                anomaly_type, ra, dec, elongation, near_edge, trail_elongation_min,
                extra=extra,
            )

            return {
                "anomaly_type":    anomaly_type,
                "source_id":       source_id,
                "ra":              ra,
                "dec":             dec,
                "magnitude":       mag,
                "delta_mag":       None,
                "mpc_designation": None,
                "ephemeris":       None,
                "notes": (
                    f"Elongation={elongation:.2f} exceeds {trail_elongation_min:.2f}"
                    f"{' (edge-of-frame threshold — coma-affected zone)' if near_edge else ''}"
                    f" — consistent with a single-exposure trail rather than a "
                    f"point source. No detection within "
                    f"{config.MATCH_CONE_ARCSEC:.1f} arcsec of this position; "
                    f"not matched in MPC."
                ),
            }

        if _is_position_shifted(history, wide_history, current_frame_positions):
            # Near-edge sources get their centroid shifted by coma between
            # frames (rotation, guiding) — this looks like "position shifted"
            # but is purely optical. All 7 MOVING_UNKNOWN near_edge=1 in the
            # 2026-08-10 analysis were ordinary coma-shifted stars.
            if near_edge:
                logger.debug(
                    "Suppressed MOVING_UNKNOWN: near_edge position-shifted "
                    "source ra=%.4f dec=%.4f elongation=%.2f — likely "
                    "coma-shifted centroid, not a real mover",
                    ra, dec, elongation,
                    extra=extra,
                )
                return None

            anomaly_type = AnomalyType.MOVING_UNKNOWN

            logger.warning(
                "ALERT — %s: unmatched position-shifted source ra=%.4f dec=%.4f elongation=%.2f",
                anomaly_type, ra, dec, elongation,
                extra=extra,
            )

            return {
                "anomaly_type":    anomaly_type,
                "source_id":       source_id,
                "ra":              ra,
                "dec":             dec,
                "magnitude":       mag,
                "delta_mag":       None,
                "mpc_designation": None,
                "ephemeris":       None,
                "notes": (
                    f"No detection within {config.MATCH_CONE_ARCSEC:.1f} arcsec of this position, "
                    f"but a historical detection within {config.MOVING_CONE_ARCSEC:.1f} arcsec of it "
                    f"is no longer present in this frame; not matched in MPC. "
                    f"Elongation={elongation:.2f}."
                ),
            }

    # ------------------------------------------------------------------
    # Priority 3 — Stationary source classification
    # ------------------------------------------------------------------

    # Get coverage from prefetched data
    coverage = coverage_by_tile.get(tile, [])
    n_coverage = len(coverage)

    # `history`/`n_history` (MATCH_CONE_ARCSEC cone) were already computed
    # above — needed regardless of catalog-match status: unmatched sources
    # use it for the UNKNOWN / FIRST_OBSERVATION / KNOWN_CATALOG_NEW
    # distinction below, and catalog-matched sources use it further down to
    # detect magnitude changes (VARIABLE_STAR / BINARY_STAR /
    # SUPERNOVA_CANDIDATE) — forcing history to [] for catalog-matched
    # sources, as an earlier revision did, made those three classifications
    # permanently unreachable.

    # --- FIRST_OBSERVATION: sky area never imaged before ---
    if n_coverage == 0:
        if not from_subtraction:
            logger.debug(
                "FIRST_OBSERVATION: ra=%.4f dec=%.4f — sky area has no prior coverage",
                ra, dec,
                extra=extra,
            )
            return None  # Not an anomaly — do not report to API
        # Subtraction already confirmed this source is new relative to the
        # reference stack, even though the API has no prior coverage record.
        # However, near-edge subtraction candidates are overwhelmingly coma
        # residuals, not real transients — suppress them here too (defense
        # in depth: subtraction.py now filters them at extraction time, but
        # a standalone DETECT_ANOMALIES re-run may still carry old
        # near_edge + from_subtraction rows from the API).
        if near_edge:
            logger.debug(
                "Suppressed UNKNOWN (subtraction, new area): near_edge "
                "ra=%.4f dec=%.4f — likely coma residual",
                ra, dec,
                extra=extra,
            )
            return None
        # A subtraction candidate that DID match a catalog (Simbad/Gaia/
        # 2MASS/Pan-STARRS — MPC is handled by Priority 1, above) is a known
        # object, not a genuine transient — most likely an ordinary
        # astroalign registration residual near that object (e.g. a small
        # centroid offset from imperfect alignment when the reference stack
        # includes a frame at a different camera/rotator orientation; see
        # CLAUDE.md's "camera rotation" discussion) that happens to fall in
        # a sky tile with no POST /frames/covering/batch record yet — e.g.
        # one of the very first frames of a session, before enough archived
        # frames exist for "coverage" to be established at all. Treat it
        # the same as KNOWN_CATALOG_NEW further below: a legitimate catalog
        # match, not an anomaly. Real incident, 2026-08-14, source_id
        # 6a7cfbae64e706.89320404 (a Gaia DR3 star, catalog_id
        # 3901066435010508672): this branch unconditionally alerted UNKNOWN
        # for it without ever consulting catalog_name at all.
        if catalog_name is not None:
            logger.debug(
                "Suppressed UNKNOWN (subtraction, new area): catalog_name=%s "
                "catalog_id=%s ra=%.4f dec=%.4f — known object, not a real "
                "transient",
                catalog_name, catalog_id, ra, dec,
                extra=extra,
            )
            return None
        logger.info(
            "UNKNOWN (subtraction, new area): ra=%.4f dec=%.4f mag=%s",
            ra, dec, mag,
            extra=extra,
        )
        return {
            "anomaly_type":    AnomalyType.UNKNOWN,
            "source_id":       source_id,
            "ra":              ra,
            "dec":             dec,
            "magnitude":       mag,
            "delta_mag":       None,
            "mpc_designation": None,
            "ephemeris":       None,
            "notes": (
                "Detected via image subtraction in a sky area with no prior "
                "coverage in the API history."
            ),
        }

    # --- Area has prior coverage from here on ---

    # --- SUPERNOVA_CANDIDATE: new source in/near a galaxy ---
    if n_history == 0 and _is_galaxy(object_type):
        logger.warning(
            "ALERT — SUPERNOVA_CANDIDATE: new source near galaxy object_type=%s "
            "ra=%.4f dec=%.4f mag=%s",
            object_type, ra, dec, mag,
            extra=extra,
        )
        return {
            "anomaly_type":    AnomalyType.SUPERNOVA_CANDIDATE,
            "source_id":       source_id,
            "ra":              ra,
            "dec":             dec,
            "magnitude":       mag,
            "delta_mag":       None,
            "mpc_designation": None,
            "ephemeris":       None,
            "notes": (
                f"New source (no prior detections) near galaxy "
                f"(object_type='{object_type}'). Area covered by "
                f"{n_coverage} prior frame(s)."
            ),
        }

    # --- UNKNOWN: covered, no history, no catalog match ---
    # TODO: Many UNKNOWN sources with mag > 20 are simply faint stars beyond Gaia DR3
    # completeness limit (~21 mag). Consider:
    #   1. Adding magnitude threshold (e.g., skip UNKNOWN alert if mag > 20)
    #   2. Querying deeper catalogs (Pan-STARRS DR2, SDSS) for faint sources
    #   3. Adding "FAINT_UNCATALOGUED" classification distinct from true UNKNOWN
    # See: https://github.com/users/miksrv/projects/10 for tracking
    if n_history == 0 and catalog_name is None:
        # Suppress near-edge uncatalogued sources — coma and other off-axis
        # aberrations shift the measured centroid away from the star's true
        # catalog position (the WCS offset correction is computed from the
        # frame median, but local distortion at the edge is larger), so
        # catalog matching misses it and it arrives here as "not in any
        # catalog".  These are overwhelmingly ordinary stars with optical
        # distortion, not real transients.  Real incident, 2026-08-10
        # analysis: 27 of 80 UNKNOWN alerts were non-subtraction near_edge
        # sources — every one a normal star whose centroid was coma-shifted
        # past MATCH_CONE_ARCSEC.
        if near_edge:
            logger.debug(
                "Suppressed UNKNOWN: near_edge uncatalogued source ra=%.4f "
                "dec=%.4f mag=%s — likely coma-shifted centroid, not a real "
                "transient",
                ra, dec, mag,
                extra=extra,
            )
            return None

        logger.warning(
            "ALERT — UNKNOWN: new uncatalogued source ra=%.4f dec=%.4f mag=%s "
            "covered_by=%d frames",
            ra, dec, mag, n_coverage,
            extra=extra,
        )
        return {
            "anomaly_type":    AnomalyType.UNKNOWN,
            "source_id":       source_id,
            "ra":              ra,
            "dec":             dec,
            "magnitude":       mag,
            "delta_mag":       None,
            "mpc_designation": None,
            "ephemeris":       None,
            "notes": (
                f"Not found in Gaia DR3, Simbad, or MPC within "
                f"{config.MATCH_CONE_ARCSEC:.1f} arcsec. "
                f"Area covered by {n_coverage} previous frame(s)."
            ),
        }

    # --- KNOWN_CATALOG_NEW: covered, no history, but matched in catalog ---
    if n_history == 0 and catalog_name is not None:
        logger.debug(
            "KNOWN_CATALOG_NEW: ra=%.4f dec=%.4f catalog=%s id=%s — "
            "below prior detection threshold",
            ra, dec, catalog_name, catalog_id,
            extra=extra,
        )
        return None  # Known object newly above threshold — not an anomaly

    # --- Source HAS prior history from here ---

    # Magnitude comparison uses only same-filter history — see
    # _same_filter_history()'s docstring. `history` itself (the existence
    # check above, n_history) stays filter-agnostic on purpose.
    median_hist_mag = _history_median_mag(_same_filter_history(history, source_filter))
    delta_mag: float | None = None

    if mag is not None and median_hist_mag is not None:
        delta_mag = mag - median_hist_mag  # negative = brighter than history

    mag_changed = (
        delta_mag is not None
        and abs(delta_mag) > config.DELTA_MAG_ALERT
    )

    if mag_changed:
        # --- SUPERNOVA_CANDIDATE — brightening in/near an already-known
        # galaxy. Checked first, before BINARY_STAR/VARIABLE_STAR: a genuine
        # supernova should take priority, and in practice the checks don't
        # overlap since the galaxy OTYPE tokens in _GALAXY_OTYPES are
        # disjoint from the binary/variable ones. Only "brightening"
        # (delta_mag negative — mag = mag - median_hist_mag, so lower mag
        # means brighter) counts here; a host galaxy's own foreground star
        # simply fading is not a supernova signature. ---
        if delta_mag is not None and delta_mag < -config.DELTA_MAG_ALERT and _is_galaxy(object_type):
            logger.warning(
                "ALERT — SUPERNOVA_CANDIDATE: brightening near galaxy object_type=%s "
                "ra=%.4f dec=%.4f delta_mag=%.3f",
                object_type, ra, dec, delta_mag,
                extra=extra,
            )
            return {
                "anomaly_type":    AnomalyType.SUPERNOVA_CANDIDATE,
                "source_id":       source_id,
                "ra":              ra,
                "dec":             dec,
                "magnitude":       mag,
                "delta_mag":       delta_mag,
                "mpc_designation": None,
                "ephemeris":       None,
                "notes": (
                    f"Brightness increase near known galaxy "
                    f"(object_type='{object_type}') delta_mag={delta_mag:.3f} "
                    f"(threshold {config.DELTA_MAG_ALERT:.2f})."
                ),
            }

        # --- BINARY_STAR — check before VARIABLE_STAR (more specific match) ---
        if _is_binary_star(object_type):
            logger.info(
                "BINARY_STAR: ra=%.4f dec=%.4f delta_mag=%.3f object_type=%s",
                ra, dec, delta_mag, object_type,
                extra=extra,
            )
            return {
                "anomaly_type":    AnomalyType.BINARY_STAR,
                "source_id":       source_id,
                "ra":              ra,
                "dec":             dec,
                "magnitude":       mag,
                "delta_mag":       delta_mag,
                "mpc_designation": None,
                "ephemeris":       None,
                "notes": (
                    f"Binary/eclipsing binary brightness change "
                    f"delta_mag={delta_mag:.3f} (threshold "
                    f"{config.DELTA_MAG_ALERT:.2f}). "
                    f"object_type='{object_type}'."
                ),
            }

        # --- VARIABLE_STAR ---
        if _is_variable_star(object_type):
            logger.info(
                "VARIABLE_STAR: ra=%.4f dec=%.4f delta_mag=%.3f object_type=%s",
                ra, dec, delta_mag, object_type,
                extra=extra,
            )
            return {
                "anomaly_type":    AnomalyType.VARIABLE_STAR,
                "source_id":       source_id,
                "ra":              ra,
                "dec":             dec,
                "magnitude":       mag,
                "delta_mag":       delta_mag,
                "mpc_designation": None,
                "ephemeris":       None,
                "notes": (
                    f"Known variable star brightness change "
                    f"delta_mag={delta_mag:.3f} (threshold "
                    f"{config.DELTA_MAG_ALERT:.2f}). "
                    f"object_type='{object_type}'."
                ),
            }

    # Source is consistent with history (or has no magnitude to compare).
    # No anomaly.
    logger.debug(
        "No anomaly: ra=%.4f dec=%.4f catalog=%s history=%d coverage=%d",
        ra, dec, catalog_name, n_history, n_coverage,
        extra=extra,
    )
    return None
