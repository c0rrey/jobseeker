"""
Resume sync module for the Jobseeker V2 pipeline.

Provides two utilities:

    check_resume_changed(db_connection, resume_path)
        Computes the SHA-256 hash of the file at *resume_path* and compares it
        against the most recent ``profile_snapshots.resume_hash`` row.
        Returns ``True`` when the hashes differ *or* when the table is empty
        (first run / bootstrap case).

    extract_resume_text(resume_path)
        Reads text from the resume file at *resume_path*.  Supported formats:

        - ``.pdf``  — opened with pdfplumber; text is concatenated across pages.
        - ``.md``   — read directly as UTF-8 plain text (pdfplumber not used).

        The result is written to a ``tempfile.NamedTemporaryFile`` (delete=False)
        and the path to that temp file is returned as a string.  The caller is
        responsible for deleting the temp file when finished.

Notes:
    - pdfplumber must be installed (add ``pdfplumber`` to requirements) for PDF
      support.  Markdown files do not require pdfplumber.
    - The temp file is written in UTF-8.  PDF pages with no extractable text
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


def _extract_text_from_pdf(resume_path: Union[str, Path]) -> str:
    """Return concatenated text from all pages of a PDF file.

    Args:
        resume_path: Path to the PDF file.

    Returns:
        Plain text extracted from every page, joined with newline separators.
        Pages that yield ``None`` from pdfplumber are silently skipped.

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
    return "\n".join(pages_text)


def _extract_text_from_md(resume_path: Union[str, Path]) -> str:
    """Return the full contents of a markdown resume file as plain text.

    Args:
        resume_path: Path to the ``.md`` file.

    Returns:
        Complete file contents decoded as UTF-8.

    Raises:
        FileNotFoundError: If *resume_path* does not exist.
    """
    return Path(resume_path).read_text(encoding="utf-8")


def extract_resume_text(resume_path: Union[str, Path]) -> str:
    """Extract plain text from a resume file and persist it to a temp file.

    Dispatches to the appropriate reader based on the file extension:

    - ``.pdf`` — uses pdfplumber to extract text page by page.
    - ``.md``  — reads the file directly as UTF-8 plain text.

    The extracted text is written to a ``tempfile.NamedTemporaryFile`` in UTF-8
    encoding with ``delete=False``, and the path to that temp file is returned.

    The caller is responsible for removing the temp file when it is no longer
    needed::

        tmp_path = extract_resume_text("resume.pdf")
        try:
            # ... downstream processing ...
        finally:
            os.unlink(tmp_path)

    Args:
        resume_path: Path to the resume file (``.pdf`` or ``.md``).

    Returns:
        Absolute path (string) of the temp file containing the extracted text.

    Raises:
        FileNotFoundError: If *resume_path* does not exist.
        ValueError: If the file extension is not ``.pdf`` or ``.md``.
        pdfplumber.exceptions.PDFSyntaxError: If a PDF file is malformed.
    """
    path = Path(resume_path)
    suffix = path.suffix.lower()

    if suffix == ".pdf":
        full_text = _extract_text_from_pdf(resume_path)
    elif suffix == ".md":
        full_text = _extract_text_from_md(resume_path)
    else:
        raise ValueError(
            f"Unsupported resume file extension '{suffix}'. "
            "Expected '.pdf' or '.md'."
        )

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".txt",
        encoding="utf-8",
        delete=False,
    ) as tmp:
        tmp.write(full_text)
        tmp_path = tmp.name

    return tmp_path
