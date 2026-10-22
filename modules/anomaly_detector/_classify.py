"""
modules/anomaly_detector/_classify.py — the priority-ordered per-source
classification logic. See docs/anomaly-detector.md's "Classification
priority" section for the full flowchart/table this function implements.

Internal helpers only — not part of this package's public surface.
"""

from __future__ import annotations

import logging
import math

import config

from ._geometry import _find_sources_within_radius, _haversine_arcsec, _tile_key
from ._history import (
    _extract_mag,
    _history_mag_epochs,
    _history_mag_scatter,
    _history_median_mag,
    _same_filter_history,
)
from ._movement import _find_wide_history, _is_position_shifted
from ._otypes import _is_binary_star, _is_galaxy, _is_non_stellar, _is_variable_star
from .types import AnomalyType

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-source classification (using prefetched data)
# ---------------------------------------------------------------------------

def _could_be_a_new_bright_object(
    history: list[dict],
    n_coverage: int,
    near_edge: bool,
    elongation: float,
) -> bool:
    """
    Whether a saturated, uncatalogued detection has enough circumstantial
    evidence to be a genuinely new bright object — a nova, a bright outburst,
    a fireball — rather than the bright-star artifact such a detection almost
    always is.

    The suppression this qualifies was unconditional, and structurally could
    not let such an object through: by definition it has no catalog match yet,
    and if it is bright enough to matter it is bright enough to saturate
    (audit 2026-08-18, finding M4). The exemption cannot lean on
    `_from_subtraction` either, which would otherwise be the obvious evidence:
    `modules/subtraction.py` masks the vicinity of every saturated pixel out
    of the difference image, so a saturated transient never becomes a
    subtraction candidate in the first place.

    What is left is circumstantial, and all of it is required:

    * **prior coverage, and nothing ever detected here.** A diffraction spike
      or a bloomed column belongs to a star that is in the frame every night,
      so the position has history. A genuinely new object does not — and the
      area must have been imaged before, or "nothing was here" says nothing.
    * **not near the frame edge**, where the aberrations that manufacture
      spurious detections are worst.
    * **star-like in shape.** A spike or a bleed trail is elongated; a real
      point source, saturated or not, is round.

    A source that qualifies still carries no usable magnitude —
    `modules/photometry.py` never measures a saturated core — so it is
    reported as an UNKNOWN alert for a human to look at, which is exactly what
    a candidate nova warrants.
    """
    return (
        not history
        and n_coverage > 0
        and not near_edge
        and 0.0 < elongation <= config.STAR_ELONGATION_MAX
    )


def _delta_mag_noise(source: dict, same_filter_history: list[dict]) -> float | None:
    """
    How much this source's magnitude is expected to wander from measurement
    error alone, as a 1-sigma value — the source's own `mag_err` and its
    historical scatter added in quadrature.

    `DELTA_MAG_ALERT` is a flat 0.5 mag applied to every source equally, which
    is the wrong shape for the question twice over: a faint source at the
    detection limit wanders further than that on noise alone and alerts every
    night, while a bright, well-measured star can change by 0.3 mag —
    unmistakable at its own precision — and never be looked at (audit
    2026-08-18, finding M3).

    Returns None when neither term is available, in which case the caller
    keeps the flat threshold on its own, exactly as before.
    """
    terms: list[float] = []

    mag_err = source.get("mag_err")
    if mag_err is not None:
        try:
            value = float(mag_err)
            if math.isfinite(value) and value > 0:
                terms.append(value)
        except (TypeError, ValueError):
            pass

    scatter = _history_mag_scatter(same_filter_history)
    if scatter is not None and scatter > 0:
        terms.append(scatter)

    if not terms:
        return None
    return math.sqrt(sum(term ** 2 for term in terms))


def _change_is_confirmed(delta_mag: float, same_filter_history: list[dict]) -> bool:
    """
    True when the most recent `VARIABILITY_CONFIRM_EPOCHS - 1` same-filter
    epochs of the history show the same change as the current one: each
    departs, in the same direction, from the baseline formed by the rest of
    the history, and passes the same significance test
    (`_is_significant_delta()`) against it.

    One deviant epoch is what a cosmic ray or hot pixel landing in the
    aperture produces, and on the NGC 7331 run (2026-09-23) 115 of the 136
    VARIABLE_STARs were exactly that: a faint star brighter in one frame and
    back to normal in the next. A real change that persists is reported one
    epoch later instead; a single-epoch event is not reported at all, which is
    the price of the rule. The history rows carry no mag_err, so the earlier
    epochs are judged on the baseline's scatter alone.
    """
    n_confirm = config.VARIABILITY_CONFIRM_EPOCHS - 1
    if n_confirm <= 0:
        return True

    measured = sorted(
        (src for src in same_filter_history if _extract_mag(src) is not None),
        key=lambda src: str(src.get("obs_time") or ""),
    )
    if len(measured) <= n_confirm:
        return False

    recent, baseline = measured[-n_confirm:], measured[:-n_confirm]
    baseline_mag = _history_median_mag(baseline)
    if baseline_mag is None:
        return False

    for src in recent:
        earlier_delta = _extract_mag(src) - baseline_mag
        if earlier_delta * delta_mag <= 0:
            return False
        if not _is_significant_delta(earlier_delta, {}, baseline):
            return False
    return True


def _is_significant_delta(
    delta_mag: float,
    source: dict,
    same_filter_history: list[dict],
) -> bool:
    """
    Whether a magnitude change is large enough to be worth reporting: past the
    absolute `DELTA_MAG_ALERT` floor AND past `VARIABILITY_SIGMA` times what
    this particular source's own noise would produce.

    The floor stays because a change smaller than it is not astronomically
    interesting however precisely it was measured; the significance test is
    what stops the floor from meaning wildly different things for a bright
    star and a faint one. `VARIABILITY_SIGMA` is reused rather than given a
    setting of its own — it already answers exactly this question for the
    catalog-independent VARIABLE_STAR branch, and two separate knobs for one
    idea would only drift apart.

    With no usable noise estimate — no `mag_err`, no same-filter history to
    take a scatter from — the floor applies alone, which is the previous
    behaviour.
    """
    if abs(delta_mag) <= config.DELTA_MAG_ALERT:
        return False

    noise = _delta_mag_noise(source, same_filter_history)
    if noise is None:
        return True

    return abs(delta_mag) > config.VARIABILITY_SIGMA * noise


def _survives_edge_zone(source: dict) -> bool:
    """
    Whether a near-edge **subtraction** candidate is compact and strong enough
    to be worth reporting rather than suppressed as an aberration residual.

    Every `near_edge` source used to be suppressed outright, on the strength
    of a real incident (2026-08-10: 53 of 80 UNKNOWN alerts were
    `from_subtraction` + `near_edge`, every one a coma residual of an ordinary
    catalogued star). But that also means a genuine transient landing near the
    frame edge — which a dithering pattern makes routine — can never be
    reported, whatever it looks like (audit 2026-08-18, finding H11).

    What separates the two is shape and strength, not position. Coma and the
    other off-axis aberrations stretch a PSF into an arc, and it is the
    *mismatch* between two such arcs that the median reference stack fails to
    cancel, so a residual is elongated and usually weak. A round, strong
    residual is not that shape at all.

    The same two thresholds `modules/subtraction.py` applies at extraction
    time (`SUBTRACTION_EDGE_ELONGATION_MAX`, `SUBTRACTION_EDGE_SNR_MIN`) are
    re-applied here rather than trusted from there, because the standalone
    `DETECT_ANOMALIES` path reconstructs its sources from the API and may
    carry rows a previous, looser revision of that module wrote. A source that
    did NOT come from subtraction never qualifies: its near-edge suppression
    rests on a different mechanism — a coma-shifted centroid that made catalog
    matching miss — which shape cannot rule out.
    """
    if not source.get("_from_subtraction"):
        return False

    try:
        elongation = float(source.get("elongation") or 0.0)
    except (TypeError, ValueError):
        return False
    if not (0.0 < elongation <= config.SUBTRACTION_EDGE_ELONGATION_MAX):
        return False

    snr = source.get("snr")
    if snr is None:
        return False
    try:
        return float(snr) >= config.SUBTRACTION_EDGE_SNR_MIN
    except (TypeError, ValueError):
        return False


def _is_residual_of_catalogued_star(
    source: dict,
    catalogued_frame_sources: list[tuple],
    frame_psf_arcsec: float | None = None,
) -> bool:
    """
    Whether an uncatalogued subtraction candidate is a catalogued star's own
    residual rather than a transient: a catalogued source of about the same
    measured brightness (within SUBTRACTION_RESIDUAL_MAX_DMAG) lies within
    SUBTRACTION_RESIDUAL_RADIUS_FWHM times the candidate's own FWHM of it in
    this same frame — in FWHM rather than arcseconds because the pipeline
    serves several telescopes, and how far a residual lands from its star
    scales with the PSF. Never less than MATCH_CONE_ARCSEC (inside it the
    source would have been matched); twice that cone when the FWHM is unknown.

    A coma-shifted or imperfectly cancelled stellar PSF leaves a difference
    residual whose centroid can land just outside MATCH_CONE_ARCSEC, so it
    reaches here uncatalogued; photometered on the new frame at that
    position, its aperture holds mostly the star itself, which is why the two
    magnitudes agree. Seen on the 2026-09-22 IC3322A test run: 10 of 11
    UNKNOWN alerts were 5-7" from a star of the same magnitude, recurring at
    the same positions across sessions.

    The magnitude condition is what keeps the finding-C6 case alive — a
    transient flaring beside a star of clearly different brightness. It is
    asymmetric: a residual is part of the star's light, so it may measure up
    to SUBTRACTION_RESIDUAL_MAX_FAINTER_DMAG fainter than the star (M51
    2025-04: 65 alerts 5-7" from a star and 1.0-1.9 mag fainter) but only
    SUBTRACTION_RESIDUAL_MAX_DMAG brighter — a transient outshining its
    neighbour is never taken for its residual. A transient up to 2 mag
    FAINTER than a neighbour this close is lost; that is the accepted cost.
    No magnitude on the candidate, or the check disabled, decides nothing.

    On a frame without calibrated magnitudes (narrowband, 8-bit) the
    brightness comparison falls back to the flux ratio, as a magnitude
    difference; entries are ``(ra, dec, mag, flux)`` (a 3-tuple is mag-only).

    Not for a diffuse candidate — wider than SUBTRACTION_DIFFUSE_FWHM_RATIO ×
    the frame's PSF (`frame_psf_arcsec`): its FWHM would stretch the radius
    over any star within a minute of arc, and a new comet beside one would be
    dropped. Such a candidate is held to the alert confirmation instead
    (`_confirm._needs_confirmation()`).
    """
    factor = config.SUBTRACTION_RESIDUAL_RADIUS_FWHM
    if factor <= 0 or not catalogued_frame_sources:
        return False
    if not source.get("_from_subtraction") or source.get("catalog_name") is not None:
        return False
    try:
        ra, dec = float(source["ra"]), float(source["dec"])
    except (KeyError, TypeError, ValueError):
        return False
    if not (math.isfinite(ra) and math.isfinite(dec)):
        return False
    mag = _finite(source.get("mag"))
    flux = _finite(source.get("flux"))
    if mag is None and (flux is None or flux <= 0):
        return False

    try:
        fwhm = float(source.get("fwhm") or 0.0)
    except (TypeError, ValueError):
        fwhm = 0.0
    diffuse_ratio = config.SUBTRACTION_DIFFUSE_FWHM_RATIO
    if frame_psf_arcsec and diffuse_ratio > 0 and fwhm > diffuse_ratio * frame_psf_arcsec:
        return False
    radius = factor * fwhm if fwhm > 0 else 2.0 * config.MATCH_CONE_ARCSEC
    radius = max(radius, config.MATCH_CONE_ARCSEC)

    for entry in catalogued_frame_sources:
        c_ra, c_dec, c_mag = entry[0], entry[1], entry[2]
        c_flux = entry[3] if len(entry) > 3 else None
        if abs(c_dec - dec) * 3600.0 > radius:
            continue
        # Positive when the candidate is fainter than the star.
        if mag is not None and c_mag is not None:
            fainter_by = mag - c_mag
        elif flux is not None and flux > 0 and c_flux is not None and c_flux > 0:
            fainter_by = 2.5 * math.log10(c_flux / flux)
        else:
            continue
        if not -config.SUBTRACTION_RESIDUAL_MAX_DMAG <= fainter_by <= max(
            config.SUBTRACTION_RESIDUAL_MAX_DMAG, config.SUBTRACTION_RESIDUAL_MAX_FAINTER_DMAG
        ):
            continue
        if _haversine_arcsec(ra, dec, c_ra, c_dec) <= radius:
            return True
    return False


def _finite(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _assess_change(
    source: dict,
    history: list[dict],
    extra: dict,
) -> tuple[float | None, bool]:
    """
    The source's magnitude change against `history`, and whether it counts:
    significant (`_is_significant_delta()`, audit 2026-08-18 finding M3) and
    shown by the previous epochs too (`_change_is_confirmed()` — a single
    deviant epoch is what a cosmic ray or hot pixel in the aperture makes).
    """
    mag = source.get("mag")
    median_hist_mag = _history_median_mag(history)
    if mag is None or median_hist_mag is None:
        return None, False

    delta_mag = mag - median_hist_mag  # negative = brighter than history
    if not _is_significant_delta(delta_mag, source, history):
        return delta_mag, False
    if not _change_is_confirmed(delta_mag, history):
        logger.debug(
            "Unconfirmed magnitude change at ra=%s dec=%s delta_mag=%.3f — "
            "the previous %d same-filter epoch(s) do not show it; waiting for "
            "the next epoch before classifying",
            source.get("ra"), source.get("dec"), delta_mag,
            config.VARIABILITY_CONFIRM_EPOCHS - 1,
            extra=extra,
        )
        return delta_mag, False
    return delta_mag, True


def _same_optics_history(history: list[dict]) -> list[dict]:
    """
    The epochs not known to come from other optics. `detect()` marks each
    history row `_same_optics` from the stored record of the frame behind it
    (`_frames._same_optics()`); a row it could not judge carries None and is
    kept, so a missing frame record never shortens a light curve.
    """
    return [src for src in history if src.get("_same_optics") is not False]


def _light_curve_unmeasurable(source: dict, frame_psf_arcsec: float | None) -> str | None:
    """
    Why this source's own light curve cannot carry a VARIABLE_STAR claim, or
    None when it can. Only for the catalog-independent branch — a catalogued
    variable is expected to vary, this is for a source nobody says varies.

    * Simbad calls it a cluster or interstellar matter (`_is_non_stellar()`)
      — not one star, and in a nearby galaxy a knot whose magnitude follows
      the seeing.
    * It is extended: FWHM over `VARIABILITY_MAX_FWHM_RATIO` × the frame's
      PSF. Same reason, for a knot Simbad does not type.
    * Its magnitude error exceeds `VARIABILITY_MAX_MAG_ERR`. Near the
      detection limit the epochs that survive a SNR cut are the ones noise
      pushed up — forced photometry of faint Gaia stars read about a
      magnitude "brighter" this way, epoch after epoch.
    * It is near the frame edge, where vignetting, coma and a clipped
      aperture move the magnitude with the pointing.

    A missing value decides nothing: rows written before a field existed
    keep the previous behaviour.
    """
    object_type = source.get("object_type")
    if _is_non_stellar(object_type):
        return f"Simbad object_type={object_type} is not a single star"

    fwhm = _finite(source.get("fwhm"))
    if fwhm is not None and frame_psf_arcsec and fwhm > config.VARIABILITY_MAX_FWHM_RATIO * frame_psf_arcsec:
        return (
            f"extended (FWHM {fwhm:.1f}\" > {config.VARIABILITY_MAX_FWHM_RATIO:g} × "
            f"PSF {frame_psf_arcsec:.1f}\")"
        )

    mag_err = _finite(source.get("mag_err"))
    if mag_err is not None and mag_err > config.VARIABILITY_MAX_MAG_ERR:
        return f"mag_err {mag_err:.3f} > {config.VARIABILITY_MAX_MAG_ERR:g}"

    if source.get("near_edge"):
        return "near the frame edge"
    return None


def _classify_source_sync(
    source: dict,
    frame_id: str,
    log_filename: str,
    narrow_history: list[dict],
    wide_pool: list[dict],
    coverage_by_tile: dict[tuple, list],
    current_frame_positions: list[tuple[float, float]],
    obs_time: str = "",
    catalogued_frame_sources: list[tuple] | None = None,
    frame_psf_arcsec: float | None = None,
    allow_moving: bool = True,
) -> dict | None:
    """
    Classify a single source using PREFETCHED batch data (synchronous).

    No API calls are made here - all data comes from the batch prefetch
    (`_prefetch._prefetch_history_data()`): ``narrow_history`` is this
    source's own narrow-cone history, ``wide_pool`` its uncatalogued
    wide-cone candidates (empty for a catalogued source, which never uses
    them). ``frame_psf_arcsec`` is the frame's own seeing (QC's median
    FWHM), or None when unknown. ``allow_moving=False`` skips the
    MOVING_UNKNOWN branch — what the source is without the motion evidence,
    which detect() keeps as the fallback should `_confirm` withdraw it.

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

    extra = {"frame_id": frame_id, "fits_filename": log_filename}
    tile = _tile_key(ra, dec)

    # Narrow-cone (MATCH_CONE_ARCSEC) history at THIS source's own position.
    # Computed once up front — Priority 2 needs it to gate the position-shift
    # check (see _is_position_shifted's docstring), and Priority 3 needs the
    # exact same query for the UNKNOWN/FIRST_OBSERVATION/KNOWN_CATALOG_NEW
    # distinction and for delta_mag, so there is no reason to run it twice.
    # Already a MATCH_CONE_ARCSEC query around this very position; the filter
    # is kept so the cone is defined here, whatever the API returned.
    history = _find_sources_within_radius(ra, dec, config.MATCH_CONE_ARCSEC, narrow_history)
    n_history = len(history)

    # Coverage is read here rather than in Priority 3 where it is mainly used:
    # the saturated-artifact suppression below needs it too, to tell a bright
    # object that genuinely was not there before from one that is there every
    # night (audit 2026-08-18, finding M4).
    coverage = coverage_by_tile.get(tile, [])
    n_coverage = len(coverage)

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
    # A subtraction candidate beside a catalogued star of the same
    # brightness is that star's residual, whatever shape it has — checked
    # before every branch below, not only the UNKNOWN ones it was written
    # for. Misregistration at a wide field's edges leaves such residuals by
    # the hundred, and they reached SPACE_DEBRIS (a dipole is elongated),
    # MOVING_UNKNOWN and, through the star's own history, VARIABLE_STAR
    # (M51 L 2025-04: every one of the 47-49 VARIABLE_STARs per frame).
    # ------------------------------------------------------------------
    if from_subtraction and _is_residual_of_catalogued_star(
        source, catalogued_frame_sources or [], frame_psf_arcsec
    ):
        logger.debug(
            "Suppressed: subtraction candidate ra=%.4f dec=%.4f mag=%s sits beside "
            "a catalogued star of the same brightness — its residual, not a change",
            ra, dec, mag,
            extra=extra,
        )
        return None

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
            if _could_be_a_new_bright_object(history, n_coverage, near_edge, elongation):
                # Falls through to the ordinary stationary-source path below,
                # which reports it as an UNKNOWN alert — see
                # _could_be_a_new_bright_object() for the evidence required
                # and why a blanket suppression could never let a nova
                # through.
                logger.warning(
                    "Saturated + uncatalogued detection at ra=%.4f dec=%.4f is "
                    "round, interior, and has no history in %d covering "
                    "frame(s) — classifying it rather than suppressing it as "
                    "an artifact. Its magnitude is unmeasurable (saturated core)",
                    ra, dec, n_coverage,
                    extra=extra,
                )
            else:
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
        # Radius is per-candidate, not one number for the whole search: a
        # historical detection from a few minutes ago is admitted out to a
        # fast mover's reach over that gap, while an older one is held to the
        # plain MOVING_CONE_ARCSEC. A fixed 120" cone left MOVING_UNKNOWN
        # structurally unreachable for anything that moved further than that
        # between frames — its own previous position was outside the search
        # entirely, so "shifted" could never be confirmed and the object fell
        # through to a generic UNKNOWN or was dropped as FIRST_OBSERVATION
        # (audit 2026-08-18, finding H3). See _wide_cone_radius_arcsec() for
        # the two bounds that keep the extension from becoming a permanently
        # wide cone.
        wide_history, wide_radius = _find_wide_history(ra, dec, wide_pool, obs_time)

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

            logger.info(
                "%s: unmatched trail-like source, elongation alone is "
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

        if allow_moving and _is_position_shifted(history, wide_history, current_frame_positions):
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
                    f"but a historical detection within {wide_radius:.1f} arcsec of it "
                    f"is no longer present in this frame; not matched in MPC. "
                    f"Elongation={elongation:.2f}."
                ),
            }

    # ------------------------------------------------------------------
    # Priority 3 — Stationary source classification
    # ------------------------------------------------------------------

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
        # in depth: subtraction.py applies the same test at extraction time,
        # but a standalone DETECT_ANOMALIES re-run may still carry old
        # near_edge + from_subtraction rows from the API). A candidate that
        # is round and strong is not the shape an aberration residual takes,
        # and is let through — see _survives_edge_zone().
        if near_edge and not _survives_edge_zone(source):
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
    # Only a subtraction candidate: `object_type` is this source's OWN
    # catalog identity, so an ordinary detection carrying a galaxy type IS
    # the galaxy — a faint one detected tonight and not on shallower frames
    # before, which is KNOWN_CATALOG_NEW below, not a supernova. A
    # subtraction candidate matched to the galaxy is a pixel-level change on it.
    if n_history == 0 and _is_galaxy(object_type) and from_subtraction:
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
        #
        # A round, strong subtraction candidate is exempt: it is not the shape
        # an aberration residual takes, and unlike an ordinary detection it
        # carries independent pixel-level evidence that nothing was there
        # before (see _survives_edge_zone()).
        if near_edge and not _survives_edge_zone(source):
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

    # A subtraction candidate is not compared against history by magnitude.
    # Its `mag` is aperture photometry at the residual's centroid, and the
    # history in the cone around it is usually a neighbouring star's (the
    # dedup passes keep the normal detection of a source over its own
    # candidate). Measured against that star's light curve, a residual 4"
    # from a 13.5 mag star read as the star fading by 0.7 mag. A real change
    # of a known source is judged on that source's own normal detection.
    if from_subtraction:
        logger.debug(
            "Not Δmag-classified: subtraction candidate ra=%.4f dec=%.4f mag=%s "
            "on a position with %d prior detection(s)",
            ra, dec, mag, n_history,
            extra=extra,
        )
        return None

    # Magnitude comparison uses only same-filter history — see
    # _same_filter_history()'s docstring. `history` itself (the existence
    # check above, n_history) stays filter-agnostic on purpose.
    same_filter_history = _same_filter_history(history, source_filter)
    delta_mag, mag_changed = _assess_change(source, same_filter_history, extra)

    # --- SUPERNOVA_CANDIDATE — brightening in/near an already-known galaxy.
    # Checked first, before BINARY_STAR/VARIABLE_STAR: a genuine supernova
    # should take priority; the binary branch below excludes galaxies itself,
    # since its substring test matches LSB and SBG. Only "brightening"
    # (delta_mag negative — mag = mag - median_hist_mag, so lower mag means
    # brighter) counts here; a host galaxy's own foreground star simply
    # fading is not a supernova signature. Judged on the whole same-filter
    # history, whatever optics took it: an alert is never given up because
    # the earlier epochs came from another telescope. ---
    if mag_changed and delta_mag is not None and delta_mag < -config.DELTA_MAG_ALERT and _is_galaxy(object_type):
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

    # The star branches below compare the source with epochs from the same
    # optics only. A magnitude through another telescope's plate scale and
    # aperture is not the same measurement: on M51 the 2025 frames (1.53″/px)
    # read the galaxy's knots up to a magnitude brighter than the 2022 history
    # (0.50″/px) did. An epoch whose optics are unknown is kept.
    star_history = _same_optics_history(same_filter_history)
    if len(star_history) != len(same_filter_history):
        delta_mag, mag_changed = _assess_change(source, star_history, extra)

    if mag_changed:
        # --- BINARY_STAR — check before VARIABLE_STAR (more specific match).
        # Not for a galaxy: the binary substring test also matches LSB and
        # SBG, and a galaxy's "fading" is measurement (see the extended-object
        # note below). ---
        if _is_binary_star(object_type) and not _is_galaxy(object_type):
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

        # --- VARIABLE_STAR (catalog-independent) — the source's OWN light
        # curve says it changed. Reached only when none of the OTYPE-gated
        # branches above fired, which for any star known solely through
        # Gaia DR3/2MASS/Pan-STARRS is every time: those three modules
        # hardcode the generic object_type "STAR", and no OTYPE classifier
        # matches it. Before this branch existed, such a source — the
        # overwhelming majority of every field — was silently dropped here
        # no matter how far it had moved in magnitude, so the Δmag detector
        # could only ever re-confirm variability Simbad already knew about
        # and could never discover any (audit 2026-08-18, finding C1).
        #
        # The evidence used instead of a catalog label is the source's own
        # same-filter history: a long enough, tight enough baseline, and a
        # current magnitude that departs from it by more than
        # VARIABILITY_SIGMA times that baseline's own scatter. An
        # intrinsically noisy source (low SNR, blended neighbour, variable
        # seeing) therefore stays quiet — a large Δmag is unremarkable
        # against a large scatter — while a genuinely quiescent star that
        # suddenly brightens or fades is reported. DELTA_MAG_ALERT is still
        # required on top (`mag_changed`, above), so an implausibly tight
        # history cannot alert on a photometrically meaningless change.
        #
        # Reported as VARIABLE_STAR rather than a new anomaly_type of its
        # own: the enum is mirrored as an ENUM column constraint in
        # observatory-api, and a new member cannot be introduced from this
        # repository alone. The notes field states explicitly that the
        # classification came from the light curve rather than from a
        # catalog, so the distinction survives for an operator.
        scatter = _history_mag_scatter(star_history)
        # Epochs that actually carry a magnitude, not rows: an epoch whose
        # photometry never calibrated contributes nothing to `scatter`, so
        # counting it here would let VARIABILITY_MIN_EPOCHS be satisfied by a
        # baseline shorter than the one the scatter was measured over.
        n_same_filter = _history_mag_epochs(star_history)

        # An extended object is not a candidate here: a galaxy's or cluster's
        # aperture magnitude moves with seeing, focus and the aperture radius
        # (sized from the detection's own FWHM), not with the object. Its
        # brightening is the SUPERNOVA_CANDIDATE branch above; its "fading"
        # is measurement, and was reported as VARIABLE_STAR for NGC 4370 and
        # NGC 4341 on the 2026-09-22 IC3322A test run.
        if _is_galaxy(object_type):
            logger.debug(
                "No light-curve variability check for extended object_type=%s "
                "at ra=%.4f dec=%.4f",
                object_type, ra, dec,
                extra=extra,
            )
            return None

        # Nor any measurement too poor to carry the claim — see
        # _light_curve_unmeasurable().
        reason = _light_curve_unmeasurable(source, frame_psf_arcsec)
        if reason is not None:
            logger.debug(
                "No light-curve variability check at ra=%.4f dec=%.4f "
                "delta_mag=%.3f: %s",
                ra, dec, delta_mag, reason,
                extra=extra,
            )
            return None

        if (
            scatter is not None
            and n_same_filter >= config.VARIABILITY_MIN_EPOCHS
            and abs(delta_mag) > config.VARIABILITY_SIGMA * scatter
        ):
            logger.warning(
                "VARIABLE_STAR (light-curve based): ra=%.4f dec=%.4f "
                "delta_mag=%.3f scatter=%.3f epochs=%d catalog=%s object_type=%s",
                ra, dec, delta_mag, scatter, n_same_filter, catalog_name, object_type,
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
                    f"Brightness change delta_mag={delta_mag:.3f} (threshold "
                    f"{config.DELTA_MAG_ALERT:.2f}) exceeds "
                    f"{config.VARIABILITY_SIGMA:.1f}x this source's own "
                    f"historical scatter ({scatter:.3f} mag over "
                    f"{n_same_filter} same-filter epochs). Variability "
                    f"candidate identified from its own light curve, not "
                    f"from a catalog classification "
                    f"(catalog_name='{catalog_name}', "
                    f"object_type='{object_type}')."
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
