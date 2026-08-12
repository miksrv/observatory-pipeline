"""
debug/debug_anomaly_charts.py — anomaly visualization tool.

See debug/README.md for what this directory is for. In short: this script
connects DIRECTLY to the test `observatory-api` database (bypassing the API
entirely — a deliberate exception to "the pipeline has no direct database
access", justified because this is a read-only developer tool, not part of
`pipeline.py`) and renders one debug PNG per *group* of anomalies, instead of
one PNG per anomaly row.

Why grouping instead of one-image-per-anomaly: a single anomaly row on its
own is rarely informative — it's one (ra, dec, mag) at one epoch, with no way
to see whether it was actually new/moving/changed without cross-referencing
other frames by hand. This script does that cross-referencing for you:

  - Anomalies sharing the same non-NULL `source_id` are one group — the same
    physical detection identity that appeared on 2+ frames. Rendered with the
    SAME styles production finder charts use (modules/finder_chart.py):
    "track" for moving types (ASTEROID/COMET/MOVING_UNKNOWN/SPACE_DEBRIS),
    "stamp_strip" (one crop per epoch) for everything else.
  - A `source_id` (or a NULL-source_id anomaly, which has no cross-frame
    identity to group by at all — see pipeline.py's dedup-by-catalog-identity
    step for why NULL never merges with anything) that appears on only ONE
    frame gets a 2-panel BEFORE/AFTER mosaic instead: a crop at the exact
    same sky position from the most recent EARLIER frame of the same object
    (expected to show nothing there) next to a crop from the anomaly's own
    frame (circled). This is the direct "blink test" for a single-epoch
    anomaly like the Vesta MOVING_UNKNOWN example that prompted this script
    (anomaly 6a7514b504c7e0.85372499 — a single-frame detection that used to
    render as one uninformative frame with no way to see it wasn't there a
    moment earlier).

No API calls, no file moves — read-only against both the database and the
local FITS archive.

Usage:
    # From inside the pipeline container (needs matplotlib, astropy, pymysql):
    python debug/debug_anomaly_charts.py <object_name> [--out-dir debug/output]

Example:
    python debug/debug_anomaly_charts.py Vesta_A807_FA

Database connection defaults to this repo's local test-DB setup (see
docker-compose.yml / observatory-api's .env) and can be overridden with
DEBUG_DB_HOST / DEBUG_DB_PORT / DEBUG_DB_NAME / DEBUG_DB_USER /
DEBUG_DB_PASSWORD — deliberately separate from config.py, which must keep
documenting "no DB access" for the production pipeline modules.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict
from typing import Any, Optional

# Allow running as `python debug/debug_anomaly_charts.py ...` from anywhere —
# make the project root (parent of this file's directory) importable so
# `import config` / `from modules import ...` resolve regardless of cwd.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
import pymysql.cursors

import config
# Reuse the production rendering code (and its exact visual style) instead
# of re-implementing it — same reasoning as debug_catalog_match.py reusing
# modules/catalog_matcher/ etc. Underscore names are "internal to the
# module", not "off limits to a debug tool in the same repo" — see
# debug/README.md. Since debug/debug_anomaly_charts.py now calls the exact
# same _render_track_chart() / _render_stamp_strip() / _render_before_after_chart()
# functions modules/finder_chart.py uses for the charts it actually uploads
# via the API, a chart generated here is pixel-for-pixel what the real
# pipeline would produce for the same (epochs, anomaly_type, designation) —
# see debug/README.md's "Fourth follow-up" for the remaining, structural
# differences that this sharing does NOT eliminate (which frames count as
# "epochs" at all, and CHART_MAX_EPOCHS).
from modules import finder_chart
from modules.finder_chart import _load_frame

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DEBUG_DB_HOST", "host.docker.internal")
DB_PORT = int(os.getenv("DEBUG_DB_PORT", "3306"))
DB_NAME = os.getenv("DEBUG_DB_NAME", "db")
DB_USER = os.getenv("DEBUG_DB_USER", "user")
DB_PASSWORD = os.getenv("DEBUG_DB_PASSWORD", "password")


# ---------------------------------------------------------------------------
# Database access
# ---------------------------------------------------------------------------

def _connect() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, cursorclass=pymysql.cursors.DictCursor,
    )


def fetch_anomalies_for_object(conn: pymysql.connections.Connection, object_name: str) -> list[dict]:
    """Every anomaly row for `object_name`, joined with its frame, chronological."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.id AS anomaly_id, a.source_id, a.anomaly_type, a.ra, a.`dec` AS `dec`,
                   a.magnitude, a.delta_mag, a.is_alert, a.mpc_designation,
                   f.id AS frame_id, f.filename, f.object, f.obs_time
            FROM anomalies a
            JOIN frames f ON f.id = a.frame_id
            WHERE f.object = %s
            ORDER BY f.obs_time ASC, a.id ASC
            """,
            (object_name,),
        )
        return list(cur.fetchall())


def fetch_catalog_designations(conn: pymysql.connections.Connection, source_ids: list[str]) -> dict[str, str]:
    """
    catalog_id for every given source_id that actually has a catalog match
    (catalog_name is not NULL) — the same identity debug_catalog_match.py
    labels sources with. Used to show e.g. "ASTEROID (4 Vesta)" instead of
    a bare anomaly_type on the rendered chart — but only as a FALLBACK,
    behind each anomaly's own "mpc_designation" (see resolve_designation()
    below): `sources.id` is resolved positionally by the API, so a moving
    object that happens to pass near an already-catalogued star's position
    can get folded into that SAME `sources` row, whose catalog_name/
    catalog_id may then reflect the star (from a different detection —
    possibly a different frame entirely), not this specific anomaly. Real
    incident, 2026-08-06, Vesta_A807_FA test data: anomaly
    6a7514b504c7f9.00535350 has mpc_designation="2014 RY1", but its
    source_id's `sources` row carries catalog_name="Gaia DR3" from an
    unrelated star sharing that id — see pipeline.py's own comment on the
    same issue, at the finder-chart step.
    """
    source_ids = sorted({sid for sid in source_ids if sid})
    if not source_ids:
        return {}
    with conn.cursor() as cur:
        placeholders = ",".join(["%s"] * len(source_ids))
        cur.execute(
            f"""
            SELECT id, catalog_id
            FROM sources
            WHERE id IN ({placeholders}) AND catalog_name IS NOT NULL AND catalog_id IS NOT NULL
            """,
            source_ids,
        )
        return {row["id"]: row["catalog_id"] for row in cur.fetchall()}


def resolve_designation(occurrences: list[dict], catalog_designations: dict[str, str]) -> Optional[str]:
    """
    Pick the designation to show for a group of occurrences (chronological):
    the most recent occurrence's own `mpc_designation` if it has one (always
    correct — captured once, at classification time, from the exact
    detection it's about), else the fallback `sources`-table lookup by
    source_id (see fetch_catalog_designations()' docstring for why that one
    can be stale/misleading), else None (uncatalogued).
    """
    latest = occurrences[-1]
    if latest.get("mpc_designation"):
        return latest["mpc_designation"]
    return catalog_designations.get(latest.get("source_id"))


def fetch_earlier_frame(conn: pymysql.connections.Connection, object_name: str, before_obs_time: Any) -> Optional[dict]:
    """Most recent frame of `object_name` strictly before `before_obs_time`, or None."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id AS frame_id, filename, object, obs_time
            FROM frames
            WHERE object = %s AND obs_time < %s
            ORDER BY obs_time DESC
            LIMIT 1
            """,
            (object_name, before_obs_time),
        )
        return cur.fetchone()


# ---------------------------------------------------------------------------
# Grouping
# ---------------------------------------------------------------------------

def group_by_source(anomalies: list[dict]) -> dict[str, list[dict]]:
    """
    Group anomaly rows by source_id. An anomaly with source_id=NULL has no
    stable cross-frame identity (see pipeline.py's _dedupe_by_catalog_identity
    docs) — each becomes its own singleton group, keyed by its own anomaly id
    so it never collides with a real source_id or another NULL anomaly.
    """
    groups: dict[str, list[dict]] = defaultdict(list)
    for a in anomalies:
        key = a["source_id"] or f"anomaly:{a['anomaly_id']}"
        groups[key].append(a)
    return groups


# ---------------------------------------------------------------------------
# FITS loading
# ---------------------------------------------------------------------------

def _epoch_fits_path(object_name: str, filename: str) -> str:
    return os.path.join(config.FITS_ARCHIVE, object_name, filename)


def _load_epoch(object_name: str, occ: dict) -> Optional[dict]:
    """Load one occurrence's own frame data+WCS, shaped like finder_chart's epoch dicts."""
    path = _epoch_fits_path(object_name, occ["filename"])
    frame = _load_frame(path)
    if frame is None:
        logger.warning("cannot load %s locally (anomaly=%s)", path, occ["anomaly_id"])
        return None
    data, wcs = frame
    return {
        **occ,
        "data": data,
        "wcs": wcs,
        "ra": occ["ra"],
        "dec": occ["dec"],
        "obs_time": str(occ["obs_time"]),
        "mag": occ.get("magnitude"),
    }


# ---------------------------------------------------------------------------
# Rendering — mirrors modules/finder_chart.py's own _render_chart_for_source():
# cap to CHART_MAX_EPOCHS, load whatever's still present locally, pick a
# style from the loaded count + anomaly_type, render via the SAME production
# rendering functions. The only real differences from production are
# upstream of this function — which rows count as "epochs" at all (this
# script's anomalies-table grouping vs. the API's full source_observations
# track) — see debug/README.md.
# ---------------------------------------------------------------------------

def render_chart_for_group(
    conn: pymysql.connections.Connection, object_name: str, occurrences: list[dict],
    out_path: str, designation: Optional[str] = None,
) -> bool:
    """Renders and writes one chart PNG for one source_id group. Returns False on any failure."""
    if len(occurrences) > config.CHART_MAX_EPOCHS:
        logger.info(
            "group has %d occurrences — keeping the most recent %d (CHART_MAX_EPOCHS)",
            len(occurrences), config.CHART_MAX_EPOCHS,
        )
        occurrences = occurrences[-config.CHART_MAX_EPOCHS:]

    loaded = [ep for ep in (_load_epoch(object_name, occ) for occ in occurrences) if ep is not None]
    if not loaded:
        logger.warning("none of %d occurrence(s) could be loaded — skipping group", len(occurrences))
        return False

    latest_type = occurrences[-1]["anomaly_type"]
    style = finder_chart._style_for_source(latest_type, len(loaded))
    label = f"{latest_type} ({designation})" if designation else latest_type

    if style == finder_chart.STYLE_BEFORE_AFTER:
        current_ep = loaded[-1]
        earlier_frame = fetch_earlier_frame(conn, object_name, current_ep["obs_time"])
        before_ep = None
        missing_reason = None
        if earlier_frame is None:
            missing_reason = f"No earlier frame of {object_name} exists — this is its first-ever frame."
        else:
            before_frame = _load_frame(_epoch_fits_path(object_name, earlier_frame["filename"]))
            if before_frame is None:
                missing_reason = (
                    f"Earlier frame {earlier_frame['filename']} exists in the DB but could not be "
                    f"loaded from the local archive ({config.FITS_ARCHIVE})."
                )
            else:
                before_ep = {"data": before_frame[0], "wcs": before_frame[1], "obs_time": str(earlier_frame["obs_time"])}
        png_bytes = finder_chart._render_before_after_chart(current_ep, before_ep, label=label, missing_reason=missing_reason)
    elif style == finder_chart.STYLE_TRACK:
        png_bytes = finder_chart._render_track_chart(loaded, label=label)
    else:
        png_bytes = finder_chart._render_stamp_strip(loaded, label=label)

    with open(out_path, "wb") as f:
        f.write(png_bytes)
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(object_name: str, out_dir: str) -> None:
    conn = _connect()
    try:
        anomalies = fetch_anomalies_for_object(conn, object_name)
        if not anomalies:
            print(f"No anomalies found for object={object_name!r}")
            return
        print(f"Fetched {len(anomalies)} anomaly row(s) for object={object_name!r}")

        groups = group_by_source(anomalies)
        n_singleton_no_source = sum(1 for k in groups if k.startswith("anomaly:"))
        print(f"Grouped into {len(groups)} group(s) "
              f"({n_singleton_no_source} with no source_id at all)")

        # One batch fallback lookup for every group's catalog designation,
        # keyed by the real source_id (singleton "anomaly:*" keys never have
        # one — source_id is NULL for exactly those rows). Each group's
        # actual designation is resolved by resolve_designation() below,
        # which prefers the anomaly's own mpc_designation over this table.
        designations = fetch_catalog_designations(conn, [a["source_id"] for a in anomalies])
        if designations:
            print(f"Resolved {len(designations)} fallback catalog designation(s) from `sources`: "
                  + ", ".join(f"{sid}={d}" for sid, d in designations.items()))

        os.makedirs(out_dir, exist_ok=True)
        n_ok = n_fail = 0
        for key, occurrences in groups.items():
            occurrences = sorted(occurrences, key=lambda o: o["obs_time"])
            types = sorted({o["anomaly_type"] for o in occurrences})
            slug = key.replace(":", "_").replace(".", "_")
            out_path = os.path.join(out_dir, f"{object_name}__{slug}__{'-'.join(types)}.png")

            designation = resolve_designation(occurrences, designations)
            try:
                ok = render_chart_for_group(conn, object_name, occurrences, out_path, designation)
            except Exception:
                logger.exception("rendering group %s failed", key)
                ok = False

            if ok:
                n_ok += 1
                print(f"  OK   {key}  ({len(occurrences)} occurrence(s), types={types}) -> {out_path}")
            else:
                n_fail += 1
                print(f"  FAIL {key}  ({len(occurrences)} occurrence(s), types={types})")

        print(f"\nDone: {n_ok} chart(s) written to {out_dir}, {n_fail} failed")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("object_name", help="Normalized OBJECT name as stored in frames.object (e.g. Vesta_A807_FA)")
    parser.add_argument(
        "--out-dir",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "output"),
        help="Output directory for rendered PNGs (default: debug/output)",
    )
    args = parser.parse_args()
    main(args.object_name, args.out_dir)
