"""Run unit tests in Pyodide environment."""

import os
import sys
import unittest


def run_tests() -> int:
    """Run all unit tests and return exit code."""
    # Change to root so relative paths like "tests/test_data/test.ibt" resolve correctly
    os.chdir("/")
    sys.path.insert(0, "/tests")

    # Import the existing test modules (must be inside function for Pyodide)
    # These modules are only available at runtime in the Pyodide filesystem
    from test_parse import (  # type: ignore[import-not-found]
        test_channel_count,
        test_each_channel_has_timecodes,
        test_lap_count,
        test_laps_schema,
        test_metadata_keys,
        test_record_count_per_channel,
        test_tick_rate,
    )
    from test_channels import (  # type: ignore[import-not-found]
        test_speed_metadata,
        test_timecodes_type,
    )

    # Wrap pytest-style functions in a unittest TestCase
    class TestParsePyodide(unittest.TestCase):
        def test_channel_count(self) -> None:
            test_channel_count()

        def test_record_count_per_channel(self) -> None:
            test_record_count_per_channel()

        def test_tick_rate(self) -> None:
            test_tick_rate()

        def test_lap_count(self) -> None:
            test_lap_count()

        def test_metadata_fields(self) -> None:
            test_metadata_keys()

        def test_laps_table_schema(self) -> None:
            test_laps_schema()

        def test_channel_has_timecodes(self) -> None:
            test_each_channel_has_timecodes()

        def test_timecodes_type(self) -> None:
            test_timecodes_type()

        def test_channel_metadata(self) -> None:
            test_speed_metadata()

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    suite.addTests(loader.loadTestsFromTestCase(TestParsePyodide))

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
    sys.exit(run_tests())
