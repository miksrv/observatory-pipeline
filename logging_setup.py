"""
logging_setup.py — root logger configuration shared by watcher.py and worker.py.

Every record goes to stderr (what `docker compose logs` shows) and, when
`config.LOG_DIR` is set, also to a size-rotated file `{LOG_DIR}/{service}.log`.
The file is the copy that survives the container: Docker's own log is deleted
with the container on every `docker compose down` / recreate, while LOG_DIR is a
bind-mounted host directory (docker-compose.yml).

Each service writes its own file, so a RotatingFileHandler never has two
processes rotating the same file underneath each other.
"""

from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler

import config

_FORMAT = "%(asctime)s %(levelname)-5s %(module)s — %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"

logger = logging.getLogger(__name__)


def configure_logging(service: str) -> None:
    """
    Configure the root logger for one service (`watcher`, `worker`).

    A file that cannot be opened (missing permissions on the mount, read-only
    filesystem) degrades to stderr-only with a warning — losing the log file
    must never stop frames from being processed.
    """
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL, logging.INFO),
        format=_FORMAT,
        datefmt=_DATEFMT,
    )

    # An uncaught exception would otherwise reach only stderr, i.e. only the
    # Docker log that disappears with the container — exactly the crash worth
    # keeping.
    def _log_uncaught(exc_type, exc_value, exc_tb):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_tb)
            return
        logging.getLogger(service).critical(
            "Uncaught exception — %s exiting", service, exc_info=(exc_type, exc_value, exc_tb),
        )

    sys.excepthook = _log_uncaught

    if not config.LOG_DIR:
        return

    path = os.path.join(config.LOG_DIR, f"{service}.log")
    try:
        os.makedirs(config.LOG_DIR, exist_ok=True)
        handler = RotatingFileHandler(
            path,
            maxBytes=int(config.LOG_FILE_MAX_MB * 1024 * 1024),
            backupCount=config.LOG_FILE_BACKUP_COUNT,
            encoding="utf-8",
        )
    except OSError as exc:
        logger.warning("Log file %s unavailable (%s) — logging to stderr only", path, exc)
        return

    handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
    logging.getLogger().addHandler(handler)
    logger.info(
        "Logging to %s (rotated at %.0f MB, %d backups kept)",
        path, config.LOG_FILE_MAX_MB, config.LOG_FILE_BACKUP_COUNT,
    )
