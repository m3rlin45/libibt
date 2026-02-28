"""Test bytes input support for libibt in Pyodide/JupyterLite environments.

This module tests that ibt() can load IBT files from bytes, BytesIO, and file paths.
This is critical for JupyterLite where files are often loaded via fetch/pyfetch
and don't have file descriptors that support mmap.

The file bytes are passed to run_bytes_input_tests() by the JavaScript test runner.
"""

import io
import os
import sys
import unittest
from typing import Any

# Module-level storage for file bytes, set by run_bytes_input_tests()
_file_bytes: bytes = b""


class TestBytesInputPyodide(unittest.TestCase):
    """Test ibt() with bytes input in Pyodide environment."""

    file_bytes: bytes

    @classmethod
    def setUpClass(cls) -> None:
        """Load file bytes from module-level storage."""
        cls.file_bytes = _file_bytes
        print(f"Received {len(cls.file_bytes)} bytes")

    def test_bytesio_has_no_fileno(self) -> None:
        """BytesIO cannot be used with mmap (no fileno)."""
        bio = io.BytesIO(self.file_bytes)
        with self.assertRaises(io.UnsupportedOperation):
            bio.fileno()

    def test_ibt_with_bytesio(self) -> None:
        """ibt() can load from BytesIO."""
        from libibt import ibt

        bio = io.BytesIO(self.file_bytes)
        log = ibt(bio)
        self.assertGreater(len(log.channels), 0)

    def test_ibt_with_bytes(self) -> None:
        """ibt() can load from bytes directly."""
        from libibt import ibt

        log = ibt(self.file_bytes)
        self.assertGreater(len(log.channels), 0)

    def test_ibt_with_file_path(self) -> None:
        """ibt() can load from file path (original usage)."""
        from libibt import ibt

        temp_path = "/tmp/test_file.ibt"
        try:
            with open(temp_path, "wb") as f:
                f.write(self.file_bytes)
            log = ibt(temp_path)
            self.assertGreater(len(log.channels), 0)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def test_bytes_and_file_produce_same_channels(self) -> None:
        """Loading from bytes produces same channels as file path."""
        from libibt import ibt

        # Load from bytes
        log_bytes = ibt(self.file_bytes)

        # Load from file
        temp_path = "/tmp/test_file.ibt"
        try:
            with open(temp_path, "wb") as f:
                f.write(self.file_bytes)
            log_file = ibt(temp_path)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

        # Compare
        self.assertEqual(len(log_bytes.channels), len(log_file.channels))
        self.assertEqual(sorted(log_bytes.channels.keys()), sorted(log_file.channels.keys()))


def run_bytes_input_tests(js_file_bytes: Any) -> int:
    """Run bytes input tests and return exit code.

    Args:
        js_file_bytes: JavaScript Uint8Array containing the IBT file bytes.
    """
    global _file_bytes
    _file_bytes = bytes(js_file_bytes.to_py())

    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestBytesInputPyodide)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Print summary
    print(f"\n{'='*60}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    print(f"{'='*60}")

    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    # This script is meant to be run from Pyodide with js_file_bytes passed in
    print("This module should be run via run_bytes_input_tests(js_file_bytes)")
    sys.exit(1)
