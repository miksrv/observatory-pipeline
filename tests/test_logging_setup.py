"""
tests/test_logging_setup.py — Unit tests for logging_setup.configure_logging()
"""

from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler

import pytest

import config
import logging_setup


@pytest.fixture(autouse=True)
def restore_root_logger():
    """configure_logging() mutates process-wide state; put it all back."""
    root = logging.getLogger()
    handlers, level, hook = root.handlers[:], root.level, sys.excepthook
    yield
    for h in root.handlers[:]:
        if h not in handlers:
            h.close()
            root.removeHandler(h)
    root.setLevel(level)
    sys.excepthook = hook


def _file_handlers() -> list[logging.Handler]:
    return [h for h in logging.getLogger().handlers if isinstance(h, RotatingFileHandler)]


def test_without_log_dir_only_stderr_is_used(monkeypatch):
    monkeypatch.setattr(config, "LOG_DIR", "")
    logging_setup.configure_logging("worker")
    assert _file_handlers() == []


def test_records_go_to_a_rotating_file_per_service(monkeypatch, tmp_path):
    log_dir = tmp_path / "logs"  # created on demand
    monkeypatch.setattr(config, "LOG_DIR", str(log_dir))
    monkeypatch.setattr(config, "LOG_FILE_MAX_MB", 1)
    monkeypatch.setattr(config, "LOG_FILE_BACKUP_COUNT", 4)

    logging_setup.configure_logging("worker")
    logging.getLogger("pipeline").warning("frame registered")
    for h in _file_handlers():
        h.flush()

    [handler] = _file_handlers()
    assert handler.baseFilename == str(log_dir / "worker.log")
    assert handler.maxBytes == 1024 * 1024
    assert handler.backupCount == 4
    assert "frame registered" in (log_dir / "worker.log").read_text(encoding="utf-8")


def test_an_unusable_log_dir_degrades_to_stderr(monkeypatch, tmp_path):
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("")
    monkeypatch.setattr(config, "LOG_DIR", os.path.join(str(blocker), "logs"))

    logging_setup.configure_logging("watcher")  # must not raise

    assert _file_handlers() == []


def test_uncaught_exceptions_reach_the_log_file(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "LOG_DIR", str(tmp_path))
    logging_setup.configure_logging("worker")

    try:
        raise RuntimeError("worker crashed")
    except RuntimeError:
        sys.excepthook(*sys.exc_info())
    for h in _file_handlers():
        h.flush()

    text = (tmp_path / "worker.log").read_text(encoding="utf-8")
    assert "Uncaught exception" in text
    assert "worker crashed" in text
