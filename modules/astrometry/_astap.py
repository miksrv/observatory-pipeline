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

    Returns
    -------
    bool
        True once astap's own output confirms "Solution found" (or
        lower-case "solution found"). False on any subprocess error, a
        non-zero return code, or a zero return code with no solution marker
        in stdout/stderr — every case already logged at the point of
        failure, so the caller only needs the boolean.
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

    # Add FOV hint if configured (helps with plate scale accuracy)
    if config.ASTAP_FOV_HINT > 0:
        cmd.extend(["-fov", str(config.ASTAP_FOV_HINT)])
        cmd.extend(["-r", "10"])  # narrow search radius when FOV is known
        logger.debug("ASTAP using explicit FOV hint: %.2f°", config.ASTAP_FOV_HINT)
    else:
        cmd.extend(["-r", "0"])   # auto-detect from FITS headers

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        logger.warning("astap timed out after 60s for %s", fits_path)
        return False
    except FileNotFoundError:
        logger.error(
            "astap binary not found at %s — plate solving disabled",
            config.ASTAP_BINARY,
        )
        return False
    except PermissionError:
        logger.error(
            "astap binary at %s is not executable (permission denied). "
            "This may happen if astap is not available for this CPU architecture "
            "(e.g., running on ARM/Apple Silicon when only amd64 binary exists). "
            "Plate solving is disabled.",
            config.ASTAP_BINARY,
        )
        return False
    except OSError as exc:
        logger.error(
            "Failed to execute astap at %s: %s — plate solving disabled",
            config.ASTAP_BINARY,
            exc,
        )
        return False

    if result.returncode != 0:
        logger.warning(
            "astap failed (rc=%d) for %s: %s",
            result.returncode,
            fits_path,
            result.stderr[:200],
        )
        return False

    # Check astap stdout for "Solution found" or similar success indicator
    # astap outputs "Solution found:" when plate solve succeeds
    astap_output = result.stdout + result.stderr
    if "Solution found" not in astap_output and "solution found" not in astap_output.lower():
        logger.warning(
            "astap returned rc=0 but no solution found in output for %s. Output: %s",
            fits_path,
            astap_output[:500],
        )
        return False

    logger.debug("astap succeeded for %s", os.path.basename(fits_path))
    return True
