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

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pymysql
import pymysql.cursors

import config
# Reuse the production rendering code (and its exact visual style) for the
# multi-epoch case instead of re-implementing it — same reasoning as
# debug_catalog_match.py reusing modules/catalog_matcher.py etc. Underscore
# names are "internal to the module", not "off limits to a debug tool in the
# same repo" — see debug/README.md.
from modules.finder_chart import (
    MOVING_TYPES,
    _arcsec_per_pixel,
    _crop_around,
    _load_frame,
    _render_stamp_strip,
    _render_track_chart,
    _stamp_half_size_px,
    _stretch,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DEBUG_DB_HOST", "host.docker.internal")
DB_PORT = int(os.getenv("DEBUG_DB_PORT", "3306"))
DB_NAME = os.getenv("DEBUG_DB_NAME", "db")
DB_USER = os.getenv("DEBUG_DB_USER", "user")
DB_PASSWORD = os.getenv("DEBUG_DB_PASSWORD", "password")

BEFORE_COLOR = "#999999"
AFTER_COLOR = "#ff4040"


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
# Rendering — multi-occurrence groups (2+ frames for the same source_id)
# ---------------------------------------------------------------------------

def render_multi_frame_group(
    object_name: str, occurrences: list[dict], out_path: str, designation: Optional[str] = None,
) -> bool:
    """
    Same rendering as production finder charts: "track" style for moving
    anomaly types, "stamp_strip" (one crop per epoch) for everything else —
    RA/Dec per epoch included the same way (see modules/finder_chart.py's
    own docstring). Style is chosen from the LATEST occurrence's
    anomaly_type, matching how
    modules/finder_chart.update_charts_for_sources() picks style from the
    anomaly that just triggered the update. `designation`, if the source is
    catalog-matched, is shown next to that anomaly_type, e.g.
    "ASTEROID (4 Vesta)".
    """
    loaded = [ep for ep in (_load_epoch(object_name, occ) for occ in occurrences) if ep is not None]
    if not loaded:
        logger.warning("none of %d occurrence(s) could be loaded — skipping group", len(occurrences))
        return False

    latest_type = occurrences[-1]["anomaly_type"]
    style = "track" if latest_type in MOVING_TYPES else "stamp_strip"
    label = f"{latest_type} ({designation})" if designation else latest_type
    png_bytes = _render_track_chart(loaded, label=label) if style == "track" else _render_stamp_strip(loaded, label=label)

    with open(out_path, "wb") as f:
        f.write(png_bytes)
    return True


# ---------------------------------------------------------------------------
# Rendering — single-occurrence groups: before/after 2-panel mosaic
# ---------------------------------------------------------------------------

def render_before_after(
    object_name: str, occ: dict, earlier_frame: Optional[dict], out_path: str, designation: Optional[str] = None,
) -> bool:
    """
    2-panel mosaic: a crop at this anomaly's exact sky position from the most
    recent earlier frame of the same object (nothing expected there) next to
    a crop from the anomaly's own frame (circled). Falls back to a 1-panel
    "after only" view — with an explicit note why — if there is no earlier
    frame at all, or it exists in the DB but isn't loadable locally.

    Both panels are centred on the SAME (ra, dec) — this anomaly's own
    detected position — on purpose: the whole point of the comparison is
    "was anything at this exact sky position before vs. after", so the two
    crops must be queried at one fixed point, not each panel's own position
    (there is no "each panel's own position" here in the first place — a
    single-occurrence anomaly has exactly one). That shared coordinate is
    shown once, in the figure's overall title, rather than repeated
    identically under both panels — showing the same number twice previously
    read as a bug (coordinates that don't shift between "before" and
    "after") rather than the intentional fixed query point it actually is.
    `designation`, if the source is catalog-matched, is shown next to
    anomaly_type on the AFTER panel, e.g. "ASTEROID (4 Vesta)".
    """
    after_ep = _load_epoch(object_name, occ)
    if after_ep is None:
        return False

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

    n_panels = 2 if before_ep else 1
    fig, axes = plt.subplots(1, n_panels, figsize=(4.6 * n_panels, 4.7), dpi=120)
    axes = [axes] if n_panels == 1 else list(axes)

    if before_ep:
        ax_before = axes[0]
        try:
            half_px = _stamp_half_size_px(before_ep["wcs"])
            crop, (cx, cy) = _crop_around(before_ep["data"], before_ep["wcs"], occ["ra"], occ["dec"], half_px)
            ax_before.imshow(_stretch(crop), cmap="gray", origin="lower")
            r = max(6.0, 10.0 / _arcsec_per_pixel(before_ep["wcs"]))
            ax_before.add_patch(plt.Circle((cx, cy), radius=r, edgecolor=BEFORE_COLOR,
                                            facecolor="none", linewidth=1.2, linestyle="--"))
        except Exception as exc:
            logger.debug("before-crop failed for anomaly=%s: %s", occ["anomaly_id"], exc)
            ax_before.text(0.5, 0.5, "crop failed", ha="center", va="center", transform=ax_before.transAxes)
        ax_before.set_title(f"BEFORE — {before_ep['obs_time']}\n(nothing expected here)",
                             fontsize=9, color=BEFORE_COLOR)
        ax_before.set_xticks([]); ax_before.set_yticks([])

    ax_after = axes[-1]
    try:
        half_px = _stamp_half_size_px(after_ep["wcs"])
        crop, (cx, cy) = _crop_around(after_ep["data"], after_ep["wcs"], occ["ra"], occ["dec"], half_px)
        ax_after.imshow(_stretch(crop), cmap="gray", origin="lower")
        r = max(6.0, 10.0 / _arcsec_per_pixel(after_ep["wcs"]))
        ax_after.add_patch(plt.Circle((cx, cy), radius=r, edgecolor=AFTER_COLOR, facecolor="none", linewidth=1.8))
    except Exception as exc:
        logger.debug("after-crop failed for anomaly=%s: %s", occ["anomaly_id"], exc)
        ax_after.text(0.5, 0.5, "crop failed", ha="center", va="center", transform=ax_after.transAxes)
    mag_txt = f"  mag={occ['magnitude']:.2f}" if occ.get("magnitude") is not None else ""
    type_txt = f"{occ['anomaly_type']} ({designation})" if designation else occ["anomaly_type"]
    ax_after.set_title(f"AFTER — {after_ep['obs_time']}\n{type_txt}{mag_txt}",
                        fontsize=9, color=AFTER_COLOR)
    ax_after.set_xticks([]); ax_after.set_yticks([])

    source_txt = f"source_id={occ['source_id']}" if occ["source_id"] else "no source_id"
    # The shared query position lives here, once, rather than repeated
    # identically under both panels — see the docstring above for why it's
    # identical in the first place (both crops are centred on this same
    # point on purpose, not a bug).
    fig.suptitle(
        f"{object_name} — anomaly {occ['anomaly_id']}  ({source_txt})\n"
        f"fixed query position: RA {occ['ra']:.4f}°  Dec {occ['dec']:.4f}°  (same in both panels, by design)",
        fontsize=10,
    )
    if missing_reason:
        fig.text(0.5, 0.01, missing_reason, ha="center", fontsize=7.5, color=BEFORE_COLOR)
    fig.tight_layout(rect=(0, 0.04, 1, 0.86))

    fig.savefig(out_path)
    plt.close(fig)
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
                if len(occurrences) >= 2:
                    ok = render_multi_frame_group(object_name, occurrences, out_path, designation)
                else:
                    occ = occurrences[0]
                    earlier = fetch_earlier_frame(conn, object_name, occ["obs_time"])
                    ok = render_before_after(object_name, occ, earlier, out_path, designation)
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
