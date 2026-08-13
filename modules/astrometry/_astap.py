"""
modules/astrometry/_astap.py — Step 1: invoking the astap plate-solver
binary and confirming it actually produced a solution.

Internal helper only — not part of this package's public surface.
"""

from __future__ import annotations

import logging
import os
import subprocess

import config

logger = logging.getLogger(__name__)


def _run_astap(fits_path: str, output_base: str | None) -> bool:
    """
    Run astap plate solver against fits_path via xvfb-run.

    Never passes ``-update``, so astap only ever writes into a ``.wcs`` side
    file (plus ``.ini``/``.log``) next to ``fits_path`` — or under
    ``output_base`` (``-o``) if given — never into ``fits_path`` itself.

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

    Returns
    -------
    bool
        True once astap's own output confirms "Solution found" (or
        lower-case "solution found"), on either attempt. False if both
        attempts fail — every case already logged at the point of failure,
        so the caller only needs the boolean.
    """
    outcome = _run_astap_attempt(fits_path, output_base, wide_radius_deg=None)
    if outcome == "solved":
        return True

    if outcome == "no_solution" and config.ASTAP_RETRY_WIDE_SEARCH:
        logger.info(
            "astap found no solution in the narrow header-based search window "
            "for %s — retrying with a wide %.0f° search radius (the header's "
            "own RA/Dec estimate, e.g. mount pointing, may be significantly off)",
            fits_path, config.ASTAP_WIDE_SEARCH_RADIUS_DEG,
        )
        outcome = _run_astap_attempt(
            fits_path, output_base,
            wide_radius_deg=config.ASTAP_WIDE_SEARCH_RADIUS_DEG,
        )
        return outcome == "solved"

    return False


def _run_astap_attempt(
    fits_path: str, output_base: str | None, wide_radius_deg: float | None,
) -> str:
    """
    Run a single astap subprocess attempt.

    ``wide_radius_deg`` is None for the ordinary narrow, header-based search
    (``-r 0`` or the FOV-hint path); a float to override with an explicit
    wide/blind search radius (the retry attempt — takes priority over
    ``config.ASTAP_FOV_HINT``, since the hint is exactly what a mis-pointed
    header would make unreliable).

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

    # Separate, larger budget for the wide/blind retry — see this function's
    # own docstring and ASTAP_WIDE_SEARCH_TIMEOUT_SEC's config.py docstring
    # for why sharing the narrow attempt's timeout here silently defeated
    # the retry on real (mis-pointed) data.
    timeout_sec = (
        config.ASTAP_WIDE_SEARCH_TIMEOUT_SEC if wide_radius_deg is not None
        else config.ASTAP_TIMEOUT_SEC
    )

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
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
