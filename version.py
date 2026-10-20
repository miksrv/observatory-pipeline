"""
version.py — the release this checkout is, for the API and the logs.

The version lives in one place: the top `## X.Y.Z` heading of CHANGELOG.md
(CLAUDE.md, "Versioning and releases"). The server runs a checked-out release
tag with the source tree bind-mounted to /app, so the file is always there;
None if it is not (or carries no heading), and the API then simply gets no
version.

api_client sends it as `X-Pipeline-Version` on every request; observatory-api
stores it with the pipeline's heartbeat and shows it on the dashboard.
"""

from __future__ import annotations

import os
import re
from typing import Optional

_CHANGELOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CHANGELOG.md")
_HEADING = re.compile(r"^## (\d+\.\d+\.\d+)\s*$", re.MULTILINE)


def read_version(path: str = _CHANGELOG) -> Optional[str]:
    """The first `## X.Y.Z` heading in *path*, or None."""
    try:
        with open(path, encoding="utf-8") as fh:
            match = _HEADING.search(fh.read())
    except OSError:
        return None
    return match.group(1) if match else None


VERSION: Optional[str] = read_version()
