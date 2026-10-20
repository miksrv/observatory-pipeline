"""
modules/anomaly_detector/_otypes.py — Simbad OTYPE classifiers.

Internal helpers only — not part of this package's public surface (leading
underscore on the filename, same convention as api_client/_shared.py).
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Simbad object-type classifiers
# ---------------------------------------------------------------------------

# Known variable-star OTYPE substrings (Simbad OTYPE field)
_VARIABLE_STAR_OTYPES: tuple[str, ...] = ("V*", "RR", "Cep", "BY", "RS", "Ell", "bL")

# Known binary/eclipsing-binary OTYPE substrings
_BINARY_STAR_OTYPES: tuple[str, ...] = ("**", "EB", "SB")

# Simbad OTYPEs that count as a galaxy (a possible supernova host) — matched
# exactly, as the short code Simbad returns or its long label. Taken from
# Simbad's own `otypedef` table (checked 2026-10-20): the "G" branch of its
# hierarchy plus the galaxy systems (ClG, GrG, IG, PaG, PoG), candidates ("?")
# included. Left out on purpose: the QSO/blazar subtree (G > AGN > QSO) —
# point-like, and its own variability must not read as a supernova — and the
# high-redshift aggregates SCG/PCG and lensed images LeG.
_GALAXY_OTYPES: frozenset[str] = frozenset({
    # short codes
    "G", "G?", "AGN", "AG?", "LIN", "SyG", "Sy1", "Sy2", "rG", "EmG", "GiC",
    "BiC", "GiG", "GiP", "H2G", "LSB", "SBG", "bCG", "ClG", "C?G", "GrG",
    "Gr?", "CGG", "IG", "PaG", "PoG",
    # long labels
    "Galaxy", "Galaxy_Candidate", "AGN_Candidate", "LINER", "Seyfert",
    "Seyfert1", "Seyfert2", "RadioG", "EmissionG", "GtowardsCl", "BrightestCG",
    "GtowardsGroup", "GinPair", "HIIG", "LowSurfBrghtG", "StarburstG",
    "BlueCompactG", "ClG_Candidate", "GroupG", "GroupG_Candidate",
    "Compact_Gr_G", "InteractingG", "PairG", "PartofG",
})


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
    Return True if the Simbad OTYPE is one of `_GALAXY_OTYPES`.

    This used to be a substring test for "G", on the grounds that Simbad ends
    a family of galaxy codes with it (`EmG`, `SBG`, `H2G`). But Simbad's
    vocabulary has as many non-galaxies carrying the letter: `RG*` (a red
    giant), `GlC` (globular cluster), `GNe` (nebula), `CGb` (cometary
    globule), `MGr` (moving group) — and galaxy codes without it (`LIN`,
    `Sy1`, `Sy2`, `rG`, `LSB`). On the first lookatstars run (2026-10-20,
    NGC 7129) a red giant undetected in one frame was posted twice as
    SUPERNOVA_CANDIDATE "near galaxy object_type=RG*". The exact set closes
    both directions; a non-galaxy Simbad object now takes the ordinary
    catalogued-object branches like any other star.
    """
    if object_type is None:
        return False
    return object_type.strip() in _GALAXY_OTYPES
