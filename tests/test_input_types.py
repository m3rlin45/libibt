"""Tests for the source types accepted by ibt()."""

import io
from pathlib import Path

import pytest

from libibt import ibt

TEST_FILE = "tests/test_data/test.ibt"


@pytest.fixture(scope="module")
def ibt_bytes() -> bytes:
    return Path(TEST_FILE).read_bytes()


@pytest.fixture(scope="module")
def reference_log():
    return ibt(TEST_FILE)


def assert_same_log(log, reference_log) -> None:
    assert set(log.channels.keys()) == set(reference_log.channels.keys())
    assert log.laps.equals(reference_log.laps)
    name = next(iter(reference_log.channels))
    assert log.channels[name].equals(reference_log.channels[name])


def test_str_path(reference_log):
    assert reference_log.file_name == TEST_FILE
    assert len(reference_log.channels) > 0


def test_pathlike(reference_log):
    log = ibt(Path(TEST_FILE))
    assert log.file_name == TEST_FILE
    assert_same_log(log, reference_log)


def test_bytes(ibt_bytes, reference_log):
    log = ibt(ibt_bytes)
    assert log.file_name == "<bytes>"
    assert_same_log(log, reference_log)


def test_bytearray(ibt_bytes, reference_log):
    log = ibt(bytearray(ibt_bytes))
    assert log.file_name == "<bytes>"
    assert_same_log(log, reference_log)


def test_memoryview(ibt_bytes, reference_log):
    log = ibt(memoryview(ibt_bytes))
    assert log.file_name == "<bytes>"
    assert_same_log(log, reference_log)


def test_file_like(ibt_bytes, reference_log):
    log = ibt(io.BytesIO(ibt_bytes))
    assert log.file_name == "<bytes>"
    assert_same_log(log, reference_log)


def test_invalid_source_type():
    with pytest.raises(TypeError):
        ibt(12345)  # type: ignore[arg-type]
