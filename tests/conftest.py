"""Shared pytest fixtures and configuration."""

from pathlib import Path

import pytest

TEST_DATA_DIR = Path(__file__).resolve().parent / "test_data"
TEST_IBT = TEST_DATA_DIR / "test.ibt"

needs_test_data = pytest.mark.skipif(
    not TEST_IBT.exists() or not TEST_IBT.resolve().exists(),
    reason="Test IBT file not available (symlink target missing)",
)


def pytest_collection_modifyitems(items):
    """Auto-skip tests that use the test IBT file when it's not available."""
    if TEST_IBT.exists() and TEST_IBT.resolve().exists():
        return
    for item in items:
        # Skip any test in a file that references test_data/test.ibt
        if "test_data" in str(item.fspath):
            item.add_marker(needs_test_data)
        elif hasattr(item, "module"):
            src = Path(item.module.__file__).read_text()
            if "test_data" in src or "test.ibt" in src:
                item.add_marker(needs_test_data)
