"""
Tests for pipeline/src/resume_sync.py.

Covers:
- check_resume_changed: returns True when table is empty (first run)
- check_resume_changed: returns True when hash differs from stored value
- check_resume_changed: returns False when hash matches stored value
- check_resume_changed: returns True when resume_hash column is NULL in latest row
- extract_resume_text: returns a string path
- extract_resume_text: written temp file contains the extracted text
- extract_resume_text: pages that return None are skipped gracefully
- extract_resume_text: multi-page PDF concatenates with newline separator
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.src.resume_sync import check_resume_changed, extract_resume_text


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _make_db(tmp_path: Path) -> sqlite3.Connection:
    """Create a minimal in-memory DB with the profile_snapshots table."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE profile_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_yaml TEXT NOT NULL DEFAULT '',
            resume_hash TEXT,
            extracted_skills TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()
    return conn


@pytest.fixture()
def db_conn(tmp_path: Path) -> sqlite3.Connection:
    """Return a fresh in-memory connection with profile_snapshots schema."""
    conn = _make_db(tmp_path)
    yield conn
    conn.close()


@pytest.fixture()
def sample_pdf(tmp_path: Path) -> Path:
    """Write minimal bytes to a file that serves as a fake PDF path."""
    pdf_path = tmp_path / "resume.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake pdf content for hashing")
    return pdf_path


# ---------------------------------------------------------------------------
# check_resume_changed
# ---------------------------------------------------------------------------


class TestCheckResumeChanged:
    def test_returns_true_when_table_empty(
        self, db_conn: sqlite3.Connection, sample_pdf: Path
    ) -> None:
        """First run with empty profile_snapshots must return True."""
        assert check_resume_changed(db_conn, sample_pdf) is True

    def test_returns_true_when_hash_differs(
        self, db_conn: sqlite3.Connection, sample_pdf: Path
    ) -> None:
        """Stored hash is different from current file — should return True."""
        db_conn.execute(
            "INSERT INTO profile_snapshots (profile_yaml, resume_hash) VALUES ('', 'deadbeef')"
        )
        db_conn.commit()
        assert check_resume_changed(db_conn, sample_pdf) is True

    def test_returns_false_when_hash_matches(
        self, db_conn: sqlite3.Connection, sample_pdf: Path
    ) -> None:
        """Stored hash matches current file — should return False."""
        import hashlib

        digest = hashlib.sha256(sample_pdf.read_bytes()).hexdigest()
        db_conn.execute(
            "INSERT INTO profile_snapshots (profile_yaml, resume_hash) VALUES ('', ?)",
            (digest,),
        )
        db_conn.commit()
        assert check_resume_changed(db_conn, sample_pdf) is False

    def test_returns_true_when_resume_hash_is_null(
        self, db_conn: sqlite3.Connection, sample_pdf: Path
    ) -> None:
        """Row exists but resume_hash is NULL — treated as changed (bootstrap)."""
        db_conn.execute(
            "INSERT INTO profile_snapshots (profile_yaml, resume_hash) VALUES ('', NULL)"
        )
        db_conn.commit()
        assert check_resume_changed(db_conn, sample_pdf) is True

    def test_uses_most_recent_row(
        self, db_conn: sqlite3.Connection, sample_pdf: Path
    ) -> None:
        """Only the latest (highest id) row is used for comparison."""
        import hashlib

        correct_hash = hashlib.sha256(sample_pdf.read_bytes()).hexdigest()
        # Insert an old stale row with a wrong hash first.
        db_conn.execute(
            "INSERT INTO profile_snapshots (profile_yaml, resume_hash) VALUES ('', 'stale_hash')"
        )
        # Insert the current correct hash as the latest row.
        db_conn.execute(
            "INSERT INTO profile_snapshots (profile_yaml, resume_hash) VALUES ('', ?)",
            (correct_hash,),
        )
        db_conn.commit()
        assert check_resume_changed(db_conn, sample_pdf) is False

    def test_accepts_string_path(
        self, db_conn: sqlite3.Connection, sample_pdf: Path
    ) -> None:
        """resume_path can be a plain string (not only Path)."""
        result = check_resume_changed(db_conn, str(sample_pdf))
        assert isinstance(result, bool)

    def test_raises_for_missing_file(
        self, db_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        """FileNotFoundError raised when PDF does not exist."""
        missing = tmp_path / "nonexistent.pdf"
        with pytest.raises(FileNotFoundError):
            check_resume_changed(db_conn, missing)


# ---------------------------------------------------------------------------
# extract_resume_text
# ---------------------------------------------------------------------------


class TestExtractResumeText:
    def _make_mock_pdf(self, page_texts: list[str | None]) -> MagicMock:
        """Build a mock pdfplumber.open context manager.

        Args:
            page_texts: List of strings (or None) returned by each page's
                extract_text() call.
        """
        pages = []
        for text in page_texts:
            page = MagicMock()
            page.extract_text.return_value = text
            pages.append(page)

        pdf_mock = MagicMock()
        pdf_mock.__enter__ = MagicMock(return_value=pdf_mock)
        pdf_mock.__exit__ = MagicMock(return_value=False)
        pdf_mock.pages = pages
        return pdf_mock

    def test_returns_string(self, sample_pdf: Path) -> None:
        """Return value must be a string."""
        mock_pdf = self._make_mock_pdf(["Some resume text"])
        with patch("pipeline.src.resume_sync.pdfplumber.open", return_value=mock_pdf):
            result = extract_resume_text(sample_pdf)
        assert isinstance(result, str)
        os.unlink(result)

    def test_temp_file_exists_and_contains_text(self, sample_pdf: Path) -> None:
        """The returned path must point to a file containing the extracted text."""
        mock_pdf = self._make_mock_pdf(["Skills: Python, SQL"])
        with patch("pipeline.src.resume_sync.pdfplumber.open", return_value=mock_pdf):
            tmp_path = extract_resume_text(sample_pdf)

        try:
            assert os.path.isfile(tmp_path)
            content = Path(tmp_path).read_text(encoding="utf-8")
            assert "Skills: Python, SQL" in content
        finally:
            os.unlink(tmp_path)

    def test_none_pages_skipped(self, sample_pdf: Path) -> None:
        """Pages returning None from extract_text are silently skipped."""
        mock_pdf = self._make_mock_pdf(["Page one text", None, "Page three text"])
        with patch("pipeline.src.resume_sync.pdfplumber.open", return_value=mock_pdf):
            tmp_path = extract_resume_text(sample_pdf)

        try:
            content = Path(tmp_path).read_text(encoding="utf-8")
            assert "Page one text" in content
            assert "Page three text" in content
        finally:
            os.unlink(tmp_path)

    def test_multipage_joined_with_newline(self, sample_pdf: Path) -> None:
        """Multiple pages are joined with a newline separator."""
        mock_pdf = self._make_mock_pdf(["First page", "Second page"])
        with patch("pipeline.src.resume_sync.pdfplumber.open", return_value=mock_pdf):
            tmp_path = extract_resume_text(sample_pdf)

        try:
            content = Path(tmp_path).read_text(encoding="utf-8")
            assert content == "First page\nSecond page"
        finally:
            os.unlink(tmp_path)

    def test_empty_pdf_creates_empty_file(self, sample_pdf: Path) -> None:
        """A PDF with no extractable text produces an empty temp file."""
        mock_pdf = self._make_mock_pdf([None, None])
        with patch("pipeline.src.resume_sync.pdfplumber.open", return_value=mock_pdf):
            tmp_path = extract_resume_text(sample_pdf)

        try:
            content = Path(tmp_path).read_text(encoding="utf-8")
            assert content == ""
        finally:
            os.unlink(tmp_path)

    def test_temp_file_has_txt_suffix(self, sample_pdf: Path) -> None:
        """Temp file should end with .txt."""
        mock_pdf = self._make_mock_pdf(["text"])
        with patch("pipeline.src.resume_sync.pdfplumber.open", return_value=mock_pdf):
            tmp_path = extract_resume_text(sample_pdf)

        try:
            assert tmp_path.endswith(".txt")
        finally:
            os.unlink(tmp_path)

    def test_accepts_string_path(self, sample_pdf: Path) -> None:
        """resume_path can be passed as a string."""
        mock_pdf = self._make_mock_pdf(["text content"])
        with patch("pipeline.src.resume_sync.pdfplumber.open", return_value=mock_pdf):
            tmp_path = extract_resume_text(str(sample_pdf))

        assert isinstance(tmp_path, str)
        os.unlink(tmp_path)
