# Plan: watcher must not analyse half-written files

Status: planned 2026-10-13, not started.

Goal: the watcher enqueues a frame only once it is completely written into `FITS_INCOMING`, and
also picks up frames that arrive by rename.

Out of scope: how files reach the server. Uploading (e.g. an SFTP drop directory on lookatstars)
is a separate service, unrelated to this repo; files are moved from there into `incoming` by hand.
An atomic `mv` within one filesystem is already safe today — the fix matters for `cp`/`rsync` and
temp-name-then-rename writers.

## Problem and fix

**Problem** (`watcher.py`, `FitsEventHandler.on_created`): on a new file the watcher sleeps a fixed
2 s and enqueues it. Two consequences:

1. **Slow copies.** A 30–60 MB frame copied with `cp`/`rsync` from another disk or over the
   network can take longer than 2 s, so the worker can pick up a truncated file.
2. **Temp-name-then-rename writes are missed.** `rsync` (`.name.fits.XXXXXX`), WinSCP (`.filepart`)
   and capture software that writes a temp file write under a temporary
   name and rename at the end. The rename arrives as a
   `FileMovedEvent`; there is no `on_moved` handler, and the temp name fails the extension check
   in `on_created`. The frame is only found by `process_existing_files()` at the next restart.

**Fix:**
- Add `on_moved`: when `event.dest_path` has a FITS extension, treat it like a new file.
- Replace the fixed `time.sleep(2)` with a size-stability check: enqueue only after the file's size
  (and mtime) has stayed unchanged for N consecutive polls. Do it off the observer thread
  (consistent with the existing "flushes run on a `threading.Timer`" rule), so a long copy does
  not block detection of other files.
- New settings in `config.py` + `.env.example` (e.g. `WATCHER_STABLE_CHECK_SEC`,
  `WATCHER_STABLE_CHECKS`, plus a max-wait so a stalled copy is eventually dropped with a
  warning). No instrument-specific defaults.
- Startup scan (`process_existing_files()`) should apply the same stability check.
- Tests in `tests/` (mock time/fs): growing file is not enqueued until stable; rename into a FITS
  name is enqueued; rename to a non-FITS name is ignored; duplicate create+move does not double-enqueue.
- `CHANGELOG.md` line (Patch Changes, e.g. `- Watcher: wait for files to be fully written; pick up renamed files`),
  update the `watcher.py` bullet in `CLAUDE.md`.
- Feature branch → PR → release tag → `git checkout` the tag on lookatstars.

## Test

- Copy a large frame into `incoming` with `cp` over a slow link (or `pv -L` to throttle) and with
  `rsync`; confirm in `/data/logs/watcher.log` that it is enqueued once, after the copy finishes,
  and that ANALYZE succeeds.
- Containers are normally stopped on lookatstars — start for the test, stop afterwards
  (`docker compose start|stop` in `/opt/observatory-pipeline`).
