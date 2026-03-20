"""
Resume sync module for the Jobseeker V2 pipeline.

Provides two utilities:

    check_resume_changed(db_connection, resume_path)
        Computes the SHA-256 hash of the PDF at *resume_path* and compares it
        against the most recent ``profile_snapshots.resume_hash`` row.
        Returns ``True`` when the hashes differ *or* when the table is empty
        (first run / bootstrap case).

    extract_resume_text(resume_path)
        Opens the PDF with pdfplumber, concatenates text from every page,
        writes the result to a ``tempfile.NamedTemporaryFile`` (delete=False),
        and returns the path to that temp file as a string.  The caller is
        responsible for deleting the temp file when finished.

Notes:
    - pdfplumber must be installed (add ``pdfplumber`` to requirements).
    - The temp file is written in UTF-8.  Pages with no extractable text
      contribute an empty string; they are silently skipped.
    - All I/O errors surface as the underlying exception (FileNotFoundError,
      sqlite3.Error, etc.).  Callers should wrap accordingly.
"""

from __future__ import annotations

import hashlib
import sqlite3
import tempfile
from pathlib import Path
from typing import Union

import pdfplumber


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _sha256_of_file(path: Union[str, Path]) -> str:
    """Return the hex-encoded SHA-256 digest of *path*.

    Args:
        path: Filesystem path to the file.

    Returns:
        64-character lowercase hex string.
    """
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def check_resume_changed(
    db_connection: sqlite3.Connection,
    resume_path: Union[str, Path],
) -> bool:
    """Return True when the PDF at *resume_path* has changed since last sync.

    Computes the SHA-256 hash of the file and compares it against the
    ``resume_hash`` column of the most recent row in ``profile_snapshots``.
    Returns ``True`` on first run when the table is empty.

    Args:
        db_connection: An open ``sqlite3.Connection`` to the pipeline database.
            The ``profile_snapshots`` table must already exist (created by
            ``pipeline.src.database.init_db``).
        resume_path: Path to the resume PDF file.

    Returns:
        ``True`` if the resume has changed (or the table is empty);
        ``False`` if the hash matches the most recent snapshot.

    Raises:
        FileNotFoundError: If *resume_path* does not exist.
        sqlite3.Error: If the database query fails.
    """
    current_hash = _sha256_of_file(resume_path)

    cursor = db_connection.execute(
        "SELECT resume_hash FROM profile_snapshots ORDER BY id DESC LIMIT 1"
    )
    row = cursor.fetchone()

    # Bootstrap case: table is empty or latest row has no hash stored yet.
    if row is None or row[0] is None:
        return True

    return current_hash != row[0]


def extract_resume_text(resume_path: Union[str, Path]) -> str:
    """Extract plain text from a PDF resume and persist it to a temp file.

    Opens *resume_path* with pdfplumber, concatenates the text extracted from
    every page (pages that yield ``None`` are skipped), writes the result to a
    ``tempfile.NamedTemporaryFile`` in UTF-8 encoding with ``delete=False``,
    and returns the path to that temp file.

    The caller is responsible for removing the temp file when it is no longer
    needed::

        tmp_path = extract_resume_text("resume.pdf")
        try:
            # ... downstream processing ...
        finally:
            os.unlink(tmp_path)

    Args:
        resume_path: Path to the resume PDF file.

    Returns:
        Absolute path (string) of the temp file containing the extracted text.

    Raises:
        FileNotFoundError: If *resume_path* does not exist.
        pdfplumber.exceptions.PDFSyntaxError: If the PDF is malformed.
    """
    pages_text: list[str] = []
    with pdfplumber.open(resume_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages_text.append(text)

    full_text = "\n".join(pages_text)

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".txt",
        encoding="utf-8",
        delete=False,
    ) as tmp:
        tmp.write(full_text)
        tmp_path = tmp.name

    return tmp_path
