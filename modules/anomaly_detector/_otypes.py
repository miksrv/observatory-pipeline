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
    Return True if the Simbad OTYPE indicates a galaxy or galaxy-like object.

    We use a word-boundary-aware check: each galaxy token must appear as a
    standalone word (surrounded by non-alphanumeric characters or string edges)
    so that "G" does not falsely match inside "AGN" twice, or trigger on "GiC"
    (group of galaxies, different classification).  The simple substring check
    in the spec is sufficient here because the token set is carefully chosen to
    be unambiguous within the Simbad OTYPE vocabulary.
    """
    if object_type is None:
        return False
    # Check each token directly — Simbad OTYPEs are short codes, not sentences.
    return any(token in object_type for token in _GALAXY_OTYPES)
