"""
Guards the Cloudflare Pages per-file size limit.

Cloudflare Pages refuses to upload any single asset larger than 25 MiB, and it
fails the whole deploy rather than skipping the file. docs/ is the publish
directory, so every file in it has to stay under the limit. The parquet is the
only file anywhere near it — see build_explorer_data.py for how it is kept small.
"""

import os
from pathlib import Path

import pytest

DOCS = Path(__file__).parent.parent / "docs"

# Cloudflare Pages hard limit: 25 MiB per file.
MAX_BYTES = 25 * 1024 * 1024

# Kept in sync with build_explorer_data.MAX_BYTES.
assert MAX_BYTES == 26_214_400


def docs_files():
    return sorted(p for p in DOCS.rglob("*") if p.is_file())


def test_docs_dir_exists():
    assert DOCS.is_dir(), f"Publish directory missing: {DOCS}"
    assert docs_files(), "docs/ is empty — nothing would be published"


@pytest.mark.parametrize("rel", [str(p.relative_to(DOCS)) for p in docs_files()])
def test_file_under_cloudflare_limit(rel):
    size = (DOCS / rel).stat().st_size
    assert size <= MAX_BYTES, (
        f"docs/{rel} is {size:,} bytes, over the Cloudflare Pages "
        f"per-file limit of {MAX_BYTES:,} bytes "
        f"({size / MAX_BYTES:.2f}x). Cloudflare will reject the deploy."
    )


def test_no_oversized_files_anywhere_in_docs():
    """Whole-tree assertion, so a newly added file is caught even though the
    parametrized test above collects its file list at import time."""
    over = [
        (str(p.relative_to(DOCS)), p.stat().st_size)
        for p in docs_files()
        if p.stat().st_size > MAX_BYTES
    ]
    assert not over, "Files over the Cloudflare Pages 25 MiB limit: " + ", ".join(
        f"docs/{n} ({s:,} bytes)" for n, s in over
    )


def test_404_page_exists():
    """Cloudflare Pages has no built-in 404 — without this file it serves
    index.html with HTTP 200 for every unmatched path."""
    p = DOCS / "404.html"
    assert p.is_file(), "docs/404.html is missing"
    assert p.stat().st_size > 0


def test_total_docs_size_reported(capsys):
    """Not an assertion on total size — Pages limits per file, not per site.
    Prints the budget so a rebuild that creeps upward is visible in CI logs."""
    files = docs_files()
    total = sum(p.stat().st_size for p in files)
    largest = max(files, key=lambda p: p.stat().st_size)
    with capsys.disabled():
        print(
            f"\n  docs/: {len(files)} files, {total:,} bytes total. "
            f"Largest: docs/{largest.relative_to(DOCS)} "
            f"({largest.stat().st_size:,} bytes, "
            f"{largest.stat().st_size / MAX_BYTES * 100:.1f}% of limit)"
        )
    assert total > 0
