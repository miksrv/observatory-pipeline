"""
tools/migrate_archive_layout.py — move a flat FITS_ARCHIVE into the
object/filter + _calibration layout (modules/archive_layout.py).

One-off and idempotent; run with both containers stopped:

    docker compose run --rm --no-deps worker python tools/migrate_archive_layout.py --dry-run
    docker compose run --rm --no-deps worker python tools/migrate_archive_layout.py

Only FITS files lying directly in `FITS_ARCHIVE/{object}/` (the legacy flat
layout) are touched; anything already under `{object}/{filter}/` or
`_calibration/` is left alone, so a second run moves nothing.

- A Light stays under the object directory it is in (that is the object the
  API registered it under) and moves into its filter subdirectory; its name
  is kept, because observatory-api stores it in `frames.filename`.
- A Dark/Flat/Bias moves to `_calibration/{night}/{type}/` and is renamed to
  `{Camera}_{Type}_…` — calibration frames are never registered, so no index
  refers to the old name. A frame without a timestamp keeps its old name.
- Never overwrites (numeric suffix on collision), leaves astap side files and
  non-FITS files where they are, removes directories the moves emptied.
- A file whose header cannot be read is reported and skipped.
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from astropy.io import fits  # noqa: E402

import config  # noqa: E402
from modules import archive_layout, fits_header, normalizer, qc  # noqa: E402

logger = logging.getLogger("migrate_archive_layout")

_FITS_EXTENSIONS = (".fits", ".fit", ".fts")


def plan_moves(archive_root: str) -> tuple[list[tuple[str, str]], list[str]]:
    """
    Every `(source, destination)` move the migration would make, and the
    files it has to skip. Destinations are not yet de-collided.
    """
    moves: list[tuple[str, str]] = []
    skipped: list[str] = []

    for object_dir in sorted(os.listdir(archive_root)):
        object_path = os.path.join(archive_root, object_dir)
        if object_dir == archive_layout.CALIBRATION_DIR or not os.path.isdir(object_path):
            continue

        for name in sorted(os.listdir(object_path)):
            src = os.path.join(object_path, name)
            if not os.path.isfile(src) or not name.lower().endswith(_FITS_EXTENSIONS):
                continue

            # extract_headers() turns a read error into an all-None dict,
            # which would file the frame as a filter-less Light; check first.
            try:
                fits.getheader(src)
            except Exception as exc:
                logger.warning("SKIP %s: cannot read header (%s)", src, exc)
                skipped.append(src)
                continue
            # Normalized only when ingest normalizes too, so frames land in
            # the filter directory pipeline.py and subtraction.py look in.
            header = fits_header.extract_headers(src)
            if config.NORMALIZE_ENABLED:
                header = normalizer.normalize_headers(header)
            observation = header.get("observation") or {}
            frame_type = observation.get("frame_type")

            if archive_layout.is_calibration(frame_type):
                dest_dir = os.path.join(
                    archive_root, archive_layout.CALIBRATION_DIR,
                    archive_layout.night_date(header.get("obs_time")), frame_type,
                )
                dest_name = name
                if header.get("obs_time") and config.NORMALIZE_ENABLED:
                    dest_name = normalizer.generate_normalized_filename(
                        object_name=object_dir,
                        frame_type=frame_type,
                        filter_name=observation.get("filter"),
                        exptime=observation.get("exptime"),
                        obs_time=header.get("obs_time"),
                        camera=(header.get("instrument") or {}).get("camera"),
                    )
            else:
                dest_dir = os.path.join(
                    object_path, archive_layout.filter_token(observation.get("filter")),
                )
                dest_name = name

            moves.append((src, os.path.join(dest_dir, dest_name)))

    return moves, skipped


def _remove_empty_dirs(archive_root: str) -> int:
    removed = 0
    for object_dir in os.listdir(archive_root):
        path = os.path.join(archive_root, object_dir)
        if os.path.isdir(path) and object_dir != archive_layout.CALIBRATION_DIR and not os.listdir(path):
            os.rmdir(path)
            logger.info("Removed empty directory %s", path)
            removed += 1
    return removed


def migrate(archive_root: str, dry_run: bool) -> int:
    """Run the migration; returns the number of files skipped."""
    moves, skipped = plan_moves(archive_root)

    for src, dest in moves:
        if dry_run:
            print(f"{src} -> {dest}")
            continue
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        final = qc._unique_destination(dest)
        if final != dest:
            logger.warning("%s exists — storing as %s", dest, os.path.basename(final))
        shutil.move(src, final)
        logger.info("%s -> %s", src, final)

    if not dry_run:
        _remove_empty_dirs(archive_root)

    logger.info(
        "%s: %d file(s) %s, %d skipped",
        archive_root, len(moves), "would move" if dry_run else "moved", len(skipped),
    )
    return len(skipped)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--archive", default=config.FITS_ARCHIVE,
                        help="archive root (default: FITS_ARCHIVE = %(default)s)")
    parser.add_argument("--dry-run", action="store_true", help="print the moves, change nothing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if not os.path.isdir(args.archive):
        logger.error("Archive root %s does not exist", args.archive)
        return 2
    return 1 if migrate(args.archive, args.dry_run) else 0


if __name__ == "__main__":
    sys.exit(main())
