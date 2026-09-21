"""
modules/anomaly_detector/_otypes.py — Simbad OTYPE substring classifiers.

Internal helpers only — not part of this package's public surface (leading
underscore on the filename, same convention as api_client/_shared.py).
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Simbad object-type substring classifiers
# ---------------------------------------------------------------------------

# Known variable-star OTYPE substrings (Simbad OTYPE field)
_VARIABLE_STAR_OTYPES: tuple[str, ...] = ("V*", "RR", "Cep", "BY", "RS", "Ell", "bL")

# Known binary/eclipsing-binary OTYPE substrings
_BINARY_STAR_OTYPES: tuple[str, ...] = ("**", "EB", "SB")

# Galaxy-related OTYPE substrings — proximity triggers SUPERNOVA_CANDIDATE
_GALAXY_OTYPES: tuple[str, ...] = ("G", "SFG", "AGN", "GiG")


# ---------------------------------------------------------------------------
# Object-type classifiers
# ---------------------------------------------------------------------------

def _is_variable_star(object_type: str | None) -> bool:
    """Return True if the Simbad OTYPE indicates a known variable star."""
    if object_type is None:
        return False
    return any(token in object_type for token in _VARIABLE_STAR_OTYPES)


def _is_binary_star(object_type: str | None) -> bool:
    """Return True if the Simbad OTYPE indicates a binary / eclipsing binary."""
    if object_type is None:
        return False
    return any(token in object_type for token in _BINARY_STAR_OTYPES)


def _is_galaxy(object_type: str | None) -> bool:
    """
    Return True if the Simbad OTYPE contains any of `_GALAXY_OTYPES` as a
    plain substring — in practice, almost always the bare letter "G".

    This docstring used to describe a word-boundary-aware check, under which
    each token had to appear as a standalone word, and claimed the substring
    check was merely an equivalent simplification. Neither half was true: the
    code has always been a plain substring test, and a standalone-word rule
    would in fact be *wrong* for this vocabulary (audit 2026-08-18, finding
    L2). Simbad uses "G" as a trailing marker on a whole family
    of genuine galaxy codes — `EmG`, `RadioG`, `SBG`, `H2G`, `LSB_G` — every
    one of which a standalone-word rule would reject. The substring test is
    the right shape here; what was wrong was the description of it.

    What it costs: the bare "G" also matches OTYPEs that merely contain the
    letter and are not galaxies at all. The one realistic in an observatory
    frame is `GlC` — a globular cluster, which by definition sits in fields
    worth imaging. A new point source projected near one is then reported
    `SUPERNOVA_CANDIDATE` rather than `UNKNOWN`. Both are alerts and both put
    the source in front of a person, so this misnames an alert rather than
    losing one; it is recorded here rather than papered over with an
    exclusion list, since getting such a list right needs Simbad's actual
    OTYPE vocabulary checked against the version astroquery returns, not
    guessed at.
    """
    if object_type is None:
        return False
    # Check each token directly — Simbad OTYPEs are short codes, not sentences.
    return any(token in object_type for token in _GALAXY_OTYPES)
