"""Unit tests for ChannelMetadata dataclass."""

import pyarrow as pa
import pytest

from libibt import ibt, ChannelMetadata
from libibt.base import LogFile

TEST_FILE = "tests/test_data/test.ibt"


# --- Roundtrip tests ---


class TestChannelMetadataRoundtrip:
    def test_roundtrip_all_fields(self):
        original = ChannelMetadata(units="m/s", desc="Vehicle speed", interpolate=True)
        field = pa.field("test", pa.float32(), metadata=original.to_field_metadata())
        restored = ChannelMetadata.from_field(field)
        assert restored == original

    def test_defaults_roundtrip(self):
        original = ChannelMetadata()
        field = pa.field("test", pa.float32(), metadata=original.to_field_metadata())
        restored = ChannelMetadata.from_field(field)
        assert restored == original

    def test_interpolate_true_roundtrip(self):
        meta = ChannelMetadata(interpolate=True)
        raw = meta.to_field_metadata()
        assert raw[b"interpolate"] == b"True"
        field = pa.field("test", pa.float32(), metadata=raw)
        restored = ChannelMetadata.from_field(field)
        assert restored.interpolate is True

    def test_interpolate_false_roundtrip(self):
        meta = ChannelMetadata(interpolate=False)
        raw = meta.to_field_metadata()
        assert raw[b"interpolate"] == b"False"
        field = pa.field("test", pa.float32(), metadata=raw)
        restored = ChannelMetadata.from_field(field)
        assert restored.interpolate is False


# --- from_field / from_channel_table ---


class TestChannelMetadataFromField:
    def test_from_field_missing_metadata(self):
        field = pa.field("test", pa.float32())
        meta = ChannelMetadata.from_field(field)
        assert meta == ChannelMetadata()

    def test_from_field_partial_metadata(self):
        field = pa.field("test", pa.float32(), metadata={b"units": b"rpm"})
        meta = ChannelMetadata.from_field(field)
        assert meta.units == "rpm"
        assert meta.desc == ""
        assert meta.interpolate is False

    def test_from_channel_table(self):
        meta = ChannelMetadata(units="m/s", desc="speed", interpolate=True)
        table = pa.table(
            {
                "timecodes": pa.array([0, 100], type=pa.int64()),
                "speed": pa.array([1.0, 2.0], type=pa.float32()),
            }
        )
        field = table.schema.field("speed").with_metadata(meta.to_field_metadata())
        table = table.cast(pa.schema([table.schema.field("timecodes"), field]))

        restored = ChannelMetadata.from_channel_table(table)
        assert restored.units == "m/s"
        assert restored.desc == "speed"
        assert restored.interpolate is True


# --- to_field_metadata ---


class TestChannelMetadataToFieldMetadata:
    def test_all_keys_present(self):
        meta = ChannelMetadata()
        raw = meta.to_field_metadata()
        assert set(raw.keys()) == {b"units", b"desc", b"interpolate"}

    def test_all_values_are_bytes(self):
        meta = ChannelMetadata(units="rpm", desc="Engine speed", interpolate=True)
        raw = meta.to_field_metadata()
        for key, value in raw.items():
            assert isinstance(key, bytes), f"Key {key!r} is not bytes"
            assert isinstance(value, bytes), f"Value for {key!r} is not bytes"


# --- Dataclass properties ---


class TestChannelMetadataProperties:
    def test_frozen(self):
        meta = ChannelMetadata(units="rpm")
        with pytest.raises(AttributeError):
            meta.units = "m/s"  # type: ignore[misc]

    def test_equality(self):
        a = ChannelMetadata(units="rpm", desc="test", interpolate=True)
        b = ChannelMetadata(units="rpm", desc="test", interpolate=True)
        assert a == b

    def test_inequality(self):
        a = ChannelMetadata(units="rpm")
        b = ChannelMetadata(units="m/s")
        assert a != b

    def test_default_values(self):
        meta = ChannelMetadata()
        assert meta.units == ""
        assert meta.desc == ""
        assert meta.interpolate is False


# --- Real file metadata ---


class TestChannelMetadataFromFile:
    def test_speed_metadata(self):
        log = ibt(TEST_FILE)
        meta = ChannelMetadata.from_channel_table(log.channels["Speed"])
        assert meta.units == "m/s"
        assert meta.interpolate is True
        assert meta.desc != ""

    def test_lap_metadata(self):
        log = ibt(TEST_FILE)
        meta = ChannelMetadata.from_channel_table(log.channels["Lap"])
        assert meta.interpolate is False

    def test_rpm_metadata(self):
        log = ibt(TEST_FILE)
        meta = ChannelMetadata.from_channel_table(log.channels["RPM"])
        assert meta.units == "revs/min"
        assert meta.interpolate is True


# --- Preservation through LogFile operations ---


class TestChannelMetadataPreservation:
    @staticmethod
    def _make_channel(name: str, meta: ChannelMetadata) -> pa.Table:
        table = pa.table(
            {
                "timecodes": pa.array([0, 100, 200], type=pa.int64()),
                name: pa.array([1.0, 2.0, 3.0], type=pa.float32()),
            }
        )
        field = table.schema.field(name).with_metadata(meta.to_field_metadata())
        return table.cast(pa.schema([table.schema.field("timecodes"), field]))

    @staticmethod
    def _make_log(channels: dict[str, pa.Table]) -> LogFile:
        return LogFile(
            channels=channels,
            laps=pa.table(
                {
                    "num": pa.array([0], type=pa.int32()),
                    "start_time": pa.array([0], type=pa.int64()),
                    "end_time": pa.array([300], type=pa.int64()),
                }
            ),
            metadata={},
            file_name="test.ibt",
        )

    def test_preserved_through_select_channels(self):
        meta = ChannelMetadata(units="rpm", desc="Engine speed", interpolate=True)
        log = self._make_log({"RPM": self._make_channel("RPM", meta)})
        result = log.select_channels(["RPM"])
        assert ChannelMetadata.from_channel_table(result.channels["RPM"]) == meta

    def test_preserved_through_filter_by_time_range(self):
        meta = ChannelMetadata(units="m/s", desc="speed", interpolate=True)
        log = self._make_log({"Speed": self._make_channel("Speed", meta)})
        result = log.filter_by_time_range(0, 150)
        assert ChannelMetadata.from_channel_table(result.channels["Speed"]) == meta

    def test_preserved_through_filter_by_lap(self):
        meta = ChannelMetadata(units="deg", desc="steering angle", interpolate=True)
        log = self._make_log({"Steering": self._make_channel("Steering", meta)})
        result = log.filter_by_lap(0)
        assert ChannelMetadata.from_channel_table(result.channels["Steering"]) == meta

    def test_preserved_through_resample_to_timecodes(self):
        meta = ChannelMetadata(units="rpm", desc="engine", interpolate=True)
        log = self._make_log({"RPM": self._make_channel("RPM", meta)})
        target = pa.array([0, 50, 100, 150, 200], type=pa.int64())
        result = log.resample_to_timecodes(target)
        assert ChannelMetadata.from_channel_table(result.channels["RPM"]) == meta

    def test_preserved_through_get_channels_as_table(self):
        meta = ChannelMetadata(units="bar", desc="brake pressure", interpolate=True)
        log = self._make_log({"BRK": self._make_channel("BRK", meta)})
        result = log.get_channels_as_table()
        assert ChannelMetadata.from_field(result.schema.field("BRK")) == meta
