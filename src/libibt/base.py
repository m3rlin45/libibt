from collections.abc import Sequence
from dataclasses import dataclass
import sys
from typing import Any
import pyarrow as pa
import pyarrow.compute as pc
import numpy as np

if sys.byteorder != "little":
    raise RuntimeError("libibt requires a little-endian platform")


@dataclass(eq=False)
class LogFile:
    """
    Container for parsed IBT telemetry data.

    Attributes:
        channels: Dict mapping channel names to PyArrow tables. Each table has
            'timecodes' (int64, ms) and '<channel_name>' columns. Channel metadata
            (units, desc, interpolate) stored in schema.field.metadata with bytes keys.
        laps: PyArrow table with columns: num (int), start_time (int), end_time (int).
            Times are in milliseconds.
        metadata: Dict of session metadata (track, session info, etc.)
        file_name: Original filename or "<bytes>" if loaded from bytes.

    Example:
        >>> log = ibt('file.ibt')
        >>> log.channels['Speed'].to_pandas()  # Single channel
        >>> log.get_channels_as_table().to_pandas()  # All merged
    """

    channels: dict[str, pa.Table]
    laps: pa.Table
    metadata: dict[str, Any]
    file_name: str

    def __post_init__(self) -> None:
        import yaml

        raw = self.metadata.get("session_info_yaml", "")
        if not raw:
            return
        parsed = yaml.safe_load(raw)
        if not parsed:
            return

        self.metadata["session_info"] = parsed

        weekend = parsed.get("WeekendInfo", {})
        driver_info = parsed.get("DriverInfo", {})
        session_info = parsed.get("SessionInfo", {})

        # Event/session type
        self.metadata["event_type"] = weekend.get("EventType")
        current_num = session_info.get("CurrentSessionNum")
        sessions = session_info.get("Sessions", [])
        current_session = next((s for s in sessions if s.get("SessionNum") == current_num), None)
        if current_session:
            self.metadata["session_type"] = current_session.get("SessionType")
            self.metadata["session_name"] = current_session.get("SessionName")

        # Driver (the one who recorded the file)
        driver_idx = driver_info.get("DriverCarIdx")
        drivers = driver_info.get("Drivers", [])
        driver = next((d for d in drivers if d.get("CarIdx") == driver_idx), None)
        if driver:
            self.metadata["driver_name"] = driver.get("UserName")
            self.metadata["driver_user_id"] = driver.get("UserID")
            self.metadata["driver_irating"] = driver.get("IRating")
            self.metadata["driver_license"] = driver.get("LicString")
            self.metadata["car_name"] = driver.get("CarScreenName")
            self.metadata["car_id"] = driver.get("CarID")

        # Car specs
        self.metadata["car_gear_count"] = driver_info.get("DriverCarGearNumForward")
        self.metadata["car_redline_rpm"] = driver_info.get("DriverCarRedLine")
        self.metadata["car_shift_rpm"] = driver_info.get("DriverCarSLShiftRPM")
        self.metadata["car_idle_rpm"] = driver_info.get("DriverCarIdleRPM")

        # Track (supplement existing fields)
        self.metadata["track_id"] = weekend.get("TrackID")
        self.metadata["track_type"] = weekend.get("TrackType")

        # Weather
        self.metadata["weather_temp"] = weekend.get("TrackAirTemp")
        self.metadata["weather_surface_temp"] = weekend.get("TrackSurfaceTemp")
        self.metadata["weather_humidity"] = weekend.get("TrackRelativeHumidity")
        self.metadata["weather_skies"] = weekend.get("TrackSkies")
        self.metadata["weather_wind_speed"] = weekend.get("TrackWindVel")
        self.metadata["weather_wind_dir"] = weekend.get("TrackWindDir")

        # Number of drivers (excluding pace car)
        self.metadata["num_drivers"] = sum(1 for d in drivers if not d.get("CarIsPaceCar", 0))

        # Car setup
        self.metadata["car_setup"] = parsed.get("CarSetup")

        # Sectors
        split_info = parsed.get("SplitTimeInfo", {})
        self.metadata["sectors"] = split_info.get("Sectors")

    def __repr__(self) -> str:
        return (
            f"LogFile(file_name={self.file_name!r}, "
            f"channels={len(self.channels)}, "
            f"laps={self.laps.num_rows})"
        )

    def get_channels_as_table(self) -> pa.Table:
        """
        Merge all channels into a single PyArrow table.

        Since all IBT channels share the same 60 Hz timebase, this is a simple
        horizontal concatenation (no interpolation needed for the base case).
        For resampled data, performs interpolation/forward-fill as needed.

        Returns:
            A PyArrow table with a 'timecodes' column and one column per channel.
        """
        if not self.channels:
            return pa.table({"timecodes": pa.array([], type=pa.int64())})

        # Compute union of all channel timecodes
        timecode_arrays = [
            channel_table.column("timecodes").to_numpy() for channel_table in self.channels.values()
        ]
        union_timecodes = pa.array(np.unique(np.concatenate(timecode_arrays)), type=pa.int64())

        resampled = self.resample_to_timecodes(union_timecodes)

        channel_names = sorted(resampled.channels.keys())

        # Collect metadata for restoration
        channel_metadata = {}
        for name in channel_names:
            field = resampled.channels[name].schema.field(name)
            if field.metadata:
                channel_metadata[name] = field.metadata

        # Build the result table
        columns_dict: dict[str, Any] = {"timecodes": union_timecodes}
        for name in channel_names:
            columns_dict[name] = resampled.channels[name].column(name)

        result = pa.table(columns_dict)

        # Restore schema with metadata
        if channel_metadata:
            new_fields = []
            for field in result.schema:
                if field.name in channel_metadata:
                    new_fields.append(field.with_metadata(channel_metadata[field.name]))
                else:
                    new_fields.append(field)
            new_schema = pa.schema(new_fields)
            result = result.cast(new_schema)

        return result

    def select_channels(self, channel_names: Sequence[str]) -> "LogFile":
        """
        Create a new LogFile with only the specified channels.

        Args:
            channel_names: Sequence of channel names to include.

        Returns:
            New LogFile containing only the specified channels.

        Raises:
            KeyError: If any channel name is not found.
        """
        missing = set(channel_names) - set(self.channels.keys())
        if missing:
            raise KeyError(f"Channels not found: {sorted(missing)}")

        new_channels = {name: self.channels[name] for name in channel_names}
        return LogFile(
            channels=new_channels,
            laps=self.laps,
            metadata=self.metadata,
            file_name=self.file_name,
        )

    def filter_by_time_range(
        self,
        start_time: int,
        end_time: int,
        channel_names: Sequence[str] | None = None,
    ) -> "LogFile":
        """
        Filter channels to a time range [start_time, end_time) at native sample rates.

        Args:
            start_time: Start time in milliseconds (inclusive).
            end_time: End time in milliseconds (exclusive).
            channel_names: Optional sequence of channel names to include.

        Returns:
            New LogFile with channels filtered to the time range.
        """
        source = self.select_channels(channel_names) if channel_names is not None else self

        new_channels = {}
        for name, channel_table in source.channels.items():
            timecodes = channel_table.column("timecodes")
            mask = pc.and_(
                pc.greater_equal(timecodes, start_time),
                pc.less(timecodes, end_time),
            )
            new_channels[name] = channel_table.filter(mask)

        laps_start = self.laps.column("start_time")
        laps_end = self.laps.column("end_time")
        laps_mask = pc.and_(
            pc.less(laps_start, end_time),
            pc.greater(laps_end, start_time),
        )
        new_laps = self.laps.filter(laps_mask)

        return LogFile(
            channels=new_channels,
            laps=new_laps,
            metadata=self.metadata,
            file_name=self.file_name,
        )

    def filter_by_lap(
        self,
        lap_num: int,
        channel_names: Sequence[str] | None = None,
    ) -> "LogFile":
        """
        Filter channels to a specific lap's time range.

        Args:
            lap_num: The lap number to filter to.
            channel_names: Optional sequence of channel names to include.

        Returns:
            New LogFile with channels filtered to the lap's time range.

        Raises:
            ValueError: If lap_num is not found in the laps table.
        """
        lap_nums = self.laps.column("num").to_pylist()
        if lap_num not in lap_nums:
            raise ValueError(f"Lap {lap_num} not found. Available laps: {lap_nums}")

        lap_idx = lap_nums.index(lap_num)
        start_time = self.laps.column("start_time")[lap_idx].as_py()
        end_time = self.laps.column("end_time")[lap_idx].as_py()

        return self.filter_by_time_range(int(start_time), int(end_time), channel_names)

    def resample_to_timecodes(
        self,
        timecodes: pa.Array,
        channel_names: Sequence[str] | None = None,
    ) -> "LogFile":
        """
        Resample all channels to a target timebase.

        For channels with interpolate="True" metadata, performs linear interpolation.
        For other channels, uses forward-fill then backward-fill for leading nulls.

        Args:
            timecodes: Target timecodes array (int64, milliseconds).
            channel_names: Optional sequence of channel names to include.

        Returns:
            New LogFile with all channels resampled to the target timecodes.
        """
        source = self.select_channels(channel_names) if channel_names is not None else self

        target_timecodes_np = timecodes.to_numpy()
        new_channels = {}

        for name, channel_table in source.channels.items():
            field = channel_table.schema.field(name)
            channel_timecodes = channel_table.column("timecodes").to_numpy()
            channel_values = channel_table.column(name).to_numpy(zero_copy_only=False)

            should_interpolate = False
            if field.metadata:
                interpolate_value = field.metadata.get(b"interpolate", b"").decode("utf-8")
                should_interpolate = interpolate_value == "True"

            if should_interpolate:
                resampled_values = np.interp(
                    target_timecodes_np,
                    channel_timecodes,
                    channel_values,
                )
            else:
                indices = np.searchsorted(channel_timecodes, target_timecodes_np, side="right") - 1
                leading_mask = indices < 0
                indices = np.clip(indices, 0, len(channel_values) - 1)
                resampled_values = channel_values[indices]
                if np.any(leading_mask):
                    resampled_values = resampled_values.copy()
                    resampled_values[leading_mask] = channel_values[0]

            output_type = field.type
            if should_interpolate and pa.types.is_integer(field.type):
                output_type = pa.float64()

            new_table = pa.table(
                {
                    "timecodes": timecodes,
                    name: pa.array(resampled_values, type=output_type),
                }
            )

            if field.metadata:
                new_field = new_table.schema.field(name).with_metadata(field.metadata)
                new_schema = pa.schema([new_table.schema.field("timecodes"), new_field])
                new_table = new_table.cast(new_schema)

            new_channels[name] = new_table

        return LogFile(
            channels=new_channels,
            laps=self.laps,
            metadata=self.metadata,
            file_name=self.file_name,
        )

    def resample_to_channel(
        self,
        reference_channel: str,
        channel_names: Sequence[str] | None = None,
    ) -> "LogFile":
        """
        Resample all channels to match a reference channel's timebase.

        Args:
            reference_channel: Name of the channel whose timecodes will be used.
            channel_names: Optional sequence of channel names to include.

        Returns:
            New LogFile with all channels resampled to the reference channel's timecodes.

        Raises:
            KeyError: If reference_channel is not found.
        """
        if reference_channel not in self.channels:
            raise KeyError(f"Reference channel not found: {reference_channel}")

        ref_timecodes = self.channels[reference_channel].column("timecodes").combine_chunks()

        return self.resample_to_timecodes(ref_timecodes, channel_names)
