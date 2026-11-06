"""
modules/astrometry/_astap.py — Step 1: invoking the astap plate-solver
binary and confirming it actually produced a solution.

Internal helper only — not part of this package's public surface.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import re
import shutil
import subprocess
import tempfile

import astropy.io.fits as fits
from astropy.wcs import WCS

import config

logger = logging.getLogger(__name__)

# Every card that describes a world coordinate system — linear part, SIP
# distortion, projection parameters — with an optional alternate-WCS suffix.
# Matched as whole card names: a bare prefix would also catch structural or
# instrument keywords (`PCOUNT`, `CDS…`).
_WCS_CARD_RE = re.compile(
    r"^(?:CTYPE|CUNIT|CRVAL|CRPIX|CDELT|CROTA|CRDER|CSYER)\d[A-Z]?$"
    r"|^(?:CD|PC|PV|PS)\d+_\d+[A-Z]?$"
    r"|^(?:A|B|AP|BP)_(?:ORDER|DMAX|\d+_\d+)$"
    r"|^(?:WCSAXES|LONPOLE|LATPOLE|WCSNAME)[A-Z]?$"
)

# Header cards astap reads its position hint from when no WCS is present.
_RA_HINT_KEYS = ("RA", "OBJCTRA")
_DEC_HINT_KEYS = ("DEC", "OBJCTDEC")
# … and its plate-scale hint (focal length in mm and pixel size in µm).
_SCALE_HINT_KEYS = ("FOCALLEN", "XPIXSZ")


def _wcs_free_copy(fits_path: str, tmp_dir: str) -> tuple[str, float | None]:
    """
    Give astap a copy of ``fits_path`` whose header carries no WCS.

    astap seeds its solve from a WCS it finds in the header. For a frame this
    pipeline has already solved and archived (every re-ANALYZE), that WCS is
    astap's own earlier TAN-SIP solution — and from that start astap
    sometimes converges on a wrong distortion fit. Real incident (2026-11-03,
    re-ANALYZE of the M33 2020 frames, ASI1600 at 0.78"/px): 31 of ~110
    L/R frames came back with SIP terms three orders of magnitude too large
    (A_0_2 = 2e-5 vs 1.8e-8 on the first solve), moving pixels up to 228"
    from where the first solve had them; ~800 sources per frame went
    unmatched and turned into MOVING_UNKNOWN alerts. astap's fit depends on
    the scale it starts from: the same pixels solve correctly from the
    header optics (FOCALLEN+XPIXSZ, ~1% off the true scale) — with the RA/DEC
    hint anywhere from the mount position to the exact solved centre — and
    wrongly when seeded with the previous solve's exact scale, whether as a
    header WCS or as ``-fov`` (both tried on the frame above).

    So astap always solves the frame as a first ingest would see it: pixels
    and instrument keywords, no WCS. The hint astap then uses is RA/DEC
    (or OBJCTRA/OBJCTDEC) and FOCALLEN+XPIXSZ; when the header lacks them,
    they are derived from the WCS being removed — RA/DEC from its centre,
    and a ``-fov`` (image height in degrees, astap's convention) returned
    for the caller to pass on — so a header whose only pointing information
    was its WCS keeps a narrow search. The header optics win over the WCS
    scale on purpose, even though they can describe the wrong setup (audit
    finding M16): they are what the first ingest solved from.

    Returns ``(path_for_astap, fov_hint_deg)``: ``(fits_path, None)``
    unchanged when the header has no WCS cards or cannot be read — astap
    then sees exactly what it saw before this existed.
    """
    try:
        with fits.open(fits_path) as hdul:
            header = hdul[0].header
            wcs_keys = [k for k in header.keys() if k and _WCS_CARD_RE.match(k.upper())]
            if not wcs_keys:
                return fits_path, None

            naxis1 = int(header.get("NAXIS1") or 0)
            naxis2 = int(header.get("NAXIS2") or 0)
            centre: tuple[float, float] | None = None
            fov_deg: float | None = None
            try:
                wcs = WCS(header).celestial
                if wcs.has_celestial and naxis1 > 0 and naxis2 > 0:
                    ra, dec = wcs.all_pix2world([[naxis1 / 2.0, naxis2 / 2.0]], 0)[0]
                    if math.isfinite(ra) and math.isfinite(dec):
                        centre = (float(ra) % 360.0, float(dec))
                    scale_deg = float(abs(wcs.proj_plane_pixel_scales()[1].value))
                    if math.isfinite(scale_deg) and scale_deg > 0:
                        fov_deg = scale_deg * naxis2
            except Exception:
                pass  # an unusable WCS gives no hint; astap still gets the frame without it

            has_position = (
                any(k in header for k in _RA_HINT_KEYS) and any(k in header for k in _DEC_HINT_KEYS)
            )
            has_scale = all(header.get(k) for k in _SCALE_HINT_KEYS)
    except Exception as exc:
        logger.debug("Could not read %s's header to strip its WCS for astap: %s", fits_path, exc)
        return fits_path, None

    copy_path = os.path.join(tmp_dir, os.path.basename(fits_path))
    try:
        shutil.copyfile(fits_path, copy_path)
        # output_verify="ignore": capture software writes cards astropy refuses
        # to flush (INDI's unquoted SITELAT = *.******E+**), and astap reads them —
        # every other card reaches astap exactly as the frame has it.
        with fits.open(
            copy_path, mode="update", do_not_scale_image_data=True, output_verify="ignore",
        ) as hdul:
            header = hdul[0].header
            for key in wcs_keys:
                header.remove(key, ignore_missing=True, remove_all=True)
            if not has_position and centre is not None:
                header["RA"] = (centre[0], "[deg] hint from the stripped WCS centre")
                header["DEC"] = (centre[1], "[deg] hint from the stripped WCS centre")
    except Exception as exc:
        logger.warning(
            "Could not prepare a WCS-free copy of %s for astap (%s) — solving the "
            "file as it is",
            os.path.basename(fits_path), exc,
        )
        return fits_path, None

    logger.info(
        "astap: solving a WCS-free copy of %s (%d WCS card(s) removed%s) — a header "
        "WCS can steer astap into a wrong distortion fit",
        os.path.basename(fits_path), len(set(wcs_keys)),
        "" if has_position or centre is None else ", RA/DEC hint taken from its centre",
    )
    return copy_path, (None if has_scale else fov_deg)


async def _run_astap(fits_path: str, output_base: str | None) -> bool:
    """
    Run astap plate solver against fits_path via xvfb-run.

    Never passes ``-update``, so astap only ever writes into a ``.wcs`` side
    file (plus ``.ini``/``.log``) next to ``fits_path`` — or under
    ``output_base`` (``-o``) if given — never into ``fits_path`` itself.
    A frame whose header carries a WCS is solved through a WCS-free copy
    (``_wcs_free_copy()``), the side files still landing beside ``fits_path``.

    First attempt uses the narrow, header-based search window (``-r 0``, or
    ``-fov``/``-r 10`` when ``config.ASTAP_FOV_HINT`` is set) — cheap, and
    correct whenever the header's own RA/Dec estimate is roughly accurate.
    If that attempt runs cleanly but finds no match at all ("No solution
    found"), retries once with a wide, effectively-blind search radius
    (``config.ASTAP_WIDE_SEARCH_RADIUS_DEG``) before giving up — gated by
    ``config.ASTAP_RETRY_WIDE_SEARCH``. Real incident, 2026-08-12: 24
    IC3322A frames' header RA/Dec (mount pointing) was off by ~10° in Dec
    from the true sky position — well outside the narrow window's ~0.77°
    reach — even though every one of those frames had a perfectly good,
    plate-solvable star field (confirmed manually with ``-r 25``:
    "Solution found... Mount Δα=1.3d, Δδ=-10.1d"). Any other failure
    (timeout, missing binary, permission error, non-solution-related
    subprocess error) is NOT retried — a wider radius wouldn't fix those.

    Async because the astap subprocess runs off the event loop — see
    ``_run_astap_attempt()``.

    Returns
    -------
    bool
        True once astap's own output confirms "Solution found" (or
        lower-case "solution found"), on either attempt. False if both
        attempts fail — every case already logged at the point of failure,
        so the caller only needs the boolean.
    """
    with tempfile.TemporaryDirectory(prefix="astap-input-") as tmp_dir:
        input_path, fov_hint_deg = await asyncio.to_thread(_wcs_free_copy, fits_path, tmp_dir)
        if input_path != fits_path and output_base is None:
            # Side files belong next to the frame (where _read_wcs() and
            # pipeline._cleanup_astap_files() look), not next to the copy.
            output_base = os.path.splitext(fits_path)[0]

        outcome = await _run_astap_attempt(
            input_path, output_base, wide_radius_deg=None, fov_hint_deg=fov_hint_deg,
        )
        if outcome == "solved":
            return True

        if outcome == "no_solution" and config.ASTAP_RETRY_WIDE_SEARCH:
            logger.info(
                "astap found no solution in the narrow header-based search window "
                "for %s — retrying with a wide %.0f° search radius (the header's "
                "own RA/Dec estimate, e.g. mount pointing, may be significantly off)",
                fits_path, config.ASTAP_WIDE_SEARCH_RADIUS_DEG,
            )
            outcome = await _run_astap_attempt(
                input_path, output_base,
                wide_radius_deg=config.ASTAP_WIDE_SEARCH_RADIUS_DEG,
                fov_hint_deg=fov_hint_deg,
            )
            return outcome == "solved"

        return False


async def _run_astap_attempt(
    fits_path: str, output_base: str | None, wide_radius_deg: float | None,
    fov_hint_deg: float | None = None,
) -> str:
    """
    Run a single astap subprocess attempt.

    ``wide_radius_deg`` is None for the ordinary narrow, header-based search
    (``-r 0`` or the FOV-hint path); a float to override with an explicit
    wide/blind search radius (the retry attempt — takes priority over
    ``config.ASTAP_FOV_HINT``, since the hint is exactly what a mis-pointed
    header would make unreliable).

    ``fov_hint_deg`` (from ``_wcs_free_copy()``) is passed as ``-fov`` when
    the header has no FOCALLEN/XPIXSZ for astap to compute the scale from;
    ``config.ASTAP_FOV_HINT`` wins over it.

    Uses ``config.ASTAP_WIDE_SEARCH_TIMEOUT_SEC`` for the wide attempt and
    ``config.ASTAP_TIMEOUT_SEC`` for the narrow one — a SEPARATE, larger
    budget for the wide case, not the narrow attempt's own timeout reused.
    A blind search over tens of degrees against the full star catalog is a
    fundamentally more expensive match than a narrow, header-guided one; see
    ASTAP_WIDE_SEARCH_TIMEOUT_SEC's own config.py docstring for the real
    incident (2026-08-13) this fixes — sharing the narrow attempt's budget
    meant the wide retry was killed by the timeout, not by genuinely
    exhausting the search, on almost every mis-pointed frame it was meant to
    rescue.

    The subprocess itself is launched on a worker thread
    (``asyncio.to_thread``), not inline, so the seconds-to-minutes astap
    spends solving do not block the event loop the rest of ``solve()`` runs
    on (audit 2026-08-18, finding L6). Nothing depends on that today — the
    worker drains one task item at a time — but a blocking call inside an
    ``async def`` is a trap for whoever first tries to solve a batch of
    frames concurrently: the tasks would run strictly one after another
    while looking like they don't. ``modules/ephemeris.py`` already does
    exactly this for its own blocking call, for the same reason.

    ``subprocess.run``'s own ``timeout=`` is kept rather than wrapping the
    thread in ``asyncio.wait_for``: it is what actually kills and reaps the
    astap child process when the budget runs out. An ``asyncio`` timeout
    around a thread can only abandon the thread, leaving astap running.

    Returns
    -------
    str
        ``"solved"`` — astap confirmed a solution.
        ``"no_solution"`` — astap ran cleanly (rc=0) but found no match;
            worth retrying with a wider radius.
        ``"error"`` — subprocess/timeout/binary failure; retrying won't help.
    """
    # Use xvfb-run to provide a virtual display for astap (GTK app)
    cmd: list[str] = [
        "xvfb-run", "-a",
        config.ASTAP_BINARY,
        "-f", fits_path,
        "-d", config.ASTAP_CATALOGS,
        "-speed", "0",    # accuracy: 0 = highest
        "-wcs",           # write the solved WCS to a .wcs side file (read
                          # back by _wcs.py — no -update here, so fits_path
                          # itself is never opened for writing by astap)
    ]

    if config.ASTAP_SIP_ENABLED:
        cmd.extend(["-sip", "y"])  # distortion terms — see config.ASTAP_SIP_ENABLED

    if output_base:
        cmd.extend(["-o", output_base])  # redirect .ini/.wcs/.log only

    if wide_radius_deg is not None:
        cmd.extend(["-r", str(wide_radius_deg)])
    elif config.ASTAP_FOV_HINT > 0:
        # Add FOV hint if configured (helps with plate scale accuracy)
        cmd.extend(["-fov", str(config.ASTAP_FOV_HINT)])
        cmd.extend(["-r", "10"])  # narrow search radius when FOV is known
        logger.debug("ASTAP using explicit FOV hint: %.2f°", config.ASTAP_FOV_HINT)
    else:
        cmd.extend(["-r", "0"])   # auto-detect from FITS headers

    if fov_hint_deg is not None and config.ASTAP_FOV_HINT <= 0:
        cmd.extend(["-fov", f"{fov_hint_deg:.4f}"])

    # Separate, larger budget for the wide/blind retry — see this function's
    # own docstring and ASTAP_WIDE_SEARCH_TIMEOUT_SEC's config.py docstring
    # for why sharing the narrow attempt's timeout here silently defeated
    # the retry on real (mis-pointed) data.
    timeout_sec = (
        config.ASTAP_WIDE_SEARCH_TIMEOUT_SEC if wide_radius_deg is not None
        else config.ASTAP_TIMEOUT_SEC
    )

    try:
        result = await asyncio.to_thread(
            subprocess.run,
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            # Its own session, out of the worker's process group: Ctrl+C in
            # a terminal signals the whole foreground group, which would kill
            # astap mid-solve while the worker is still finishing its item
            # gracefully (worker.py's shutdown handling).
            start_new_session=True,
        )
    except subprocess.TimeoutExpired:
        logger.warning(
            "astap timed out after %.0fs for %s (search radius=%s)",
            timeout_sec, fits_path,
            wide_radius_deg if wide_radius_deg is not None else "auto",
        )
        return "error"
    except FileNotFoundError:
        logger.error(
            "astap binary not found at %s — plate solving disabled",
            config.ASTAP_BINARY,
        )
        return "error"
    except PermissionError:
        logger.error(
            "astap binary at %s is not executable (permission denied). "
            "This may happen if astap is not available for this CPU architecture "
            "(e.g., running on ARM/Apple Silicon when only amd64 binary exists). "
            "Plate solving is disabled.",
            config.ASTAP_BINARY,
        )
        return "error"
    except OSError as exc:
        logger.error(
            "Failed to execute astap at %s: %s — plate solving disabled",
            config.ASTAP_BINARY,
            exc,
        )
        return "error"

    # Check astap's own output for a success or a clean "tried and failed"
    # marker BEFORE looking at the return code: empirically, this astap
    # build exits with rc=1 (not 0) for an ordinary "No solution found" —
    # not just for a genuine crash/misconfiguration — so branching on rc
    # alone (an earlier version of this function did) misclassifies the
    # single most common, retry-worthy failure as a hard "error" and never
    # gets to the wide-radius retry at all.
    astap_output = result.stdout + result.stderr
    astap_output_lower = astap_output.lower()

    # "no solution found" must be checked BEFORE "solution found" — it
    # contains "solution found" as a substring, so checking the success
    # marker first misclassified every genuine "No solution found" as a
    # success (real incident, 2026-08-13: this made the "no_solution" branch
    # below dead code, so ASTAP_RETRY_WIDE_SEARCH's wide-radius retry never
    # actually ran on any of the 24 mis-pointed IC3322A frames it was meant
    # to rescue — _run_astap() just returned True on the narrow attempt's
    # own failure output, and _read_wcs() silently fell back to the file's
    # stale header WCS since no real .wcs side file existed to read).
    if "no solution found" in astap_output_lower:
        logger.warning(
            "astap found no solution for %s (rc=%d, search radius=%s). Output: %s",
            fits_path,
            result.returncode,
            wide_radius_deg if wide_radius_deg is not None else "auto",
            astap_output[:500],
        )
        return "no_solution"

    if "solution found" in astap_output_lower:
        logger.debug("astap succeeded for %s", os.path.basename(fits_path))
        return "solved"

    # Anything else — a genuine crash, missing catalog, corrupt input, or
    # any other rc!=0 with no recognizable "solution"/"no solution" marker
    # at all. A wider search radius wouldn't fix any of these, so this is
    # NOT retried.
    logger.warning(
        "astap failed (rc=%d) for %s: %s",
        result.returncode,
        fits_path,
        (result.stderr or result.stdout)[:200],
    )
    return "error"
