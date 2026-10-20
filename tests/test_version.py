"""
Tests for version.py and the X-Pipeline-Version header api_client sends with it.
"""

from __future__ import annotations

import re

import api_client._shared as shared
import version


def test_reads_the_top_heading(tmp_path):
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text("# Changelog\n\n## 1.4.0\n\n### Minor Changes\n\n- x\n\n## 1.3.0\n", encoding="utf-8")

    assert version.read_version(str(changelog)) == "1.4.0"


def test_no_heading_or_no_file_is_none(tmp_path):
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text("# Changelog\n\nnothing released yet\n", encoding="utf-8")

    assert version.read_version(str(changelog)) is None
    assert version.read_version(str(tmp_path / "missing.md")) is None


def test_the_repository_changelog_yields_a_version():
    assert re.fullmatch(r"\d+\.\d+\.\d+", version.VERSION or "")


async def test_the_client_sends_the_version(monkeypatch):
    monkeypatch.setattr(version, "VERSION", "9.8.7")

    async with shared._make_client() as client:
        assert client.headers["X-Pipeline-Version"] == "9.8.7"
        assert "X-API-Key" in client.headers


async def test_no_version_no_header(monkeypatch):
    monkeypatch.setattr(version, "VERSION", None)

    async with shared._make_client() as client:
        assert "X-Pipeline-Version" not in client.headers
