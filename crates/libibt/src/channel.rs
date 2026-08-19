use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, UInt32Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};

use crate::error::{IbtError, Result};
use crate::var_header::{VarHeader, VarType};

/// Typed metadata for a single telemetry channel.
pub struct ChannelMetadata {
    pub units: String,
    pub desc: String,
    pub interpolate: bool,
}

impl ChannelMetadata {
    /// Build metadata from a VarHeader.
    pub fn from_var_header(var: &VarHeader) -> Self {
        ChannelMetadata {
            units: var.unit.clone(),
            desc: var.desc.clone(),
            interpolate: matches!(var.var_type, VarType::Float | VarType::Double),
        }
    }

    /// Convert to a HashMap suitable for Arrow field metadata.
    pub fn to_hashmap(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("units".to_string(), self.units.clone());
        m.insert("desc".to_string(), self.desc.clone());
        m.insert(
            "interpolate".to_string(),
            if self.interpolate { "True" } else { "False" }.to_string(),
        );
        m
    }
}

/// Read every element of a variable from all records, record-major
/// (record 0's elements, then record 1's, ...). Scalars produce one value
/// per record. Returns the flat values array and its Arrow element type.
fn read_flattened_values(
    var: &VarHeader,
    records: &[u8],
    buf_len: usize,
    record_count: usize,
) -> (ArrayRef, DataType) {
    let count = var.count as usize;
    let offset = var.offset as usize;
    let elem_size = var.var_type.element_size();

    let element_offsets = |i: usize| {
        let base = i * buf_len + offset;
        (0..count).map(move |k| base + k * elem_size)
    };

    match var.var_type {
        VarType::Bool => {
            let values: Vec<bool> = (0..record_count)
                .flat_map(|i| element_offsets(i).map(|o| records[o] != 0))
                .collect();
            (Arc::new(BooleanArray::from(values)), DataType::Boolean)
        }
        VarType::Char => {
            let values: Vec<u8> = (0..record_count)
                .flat_map(|i| element_offsets(i).map(|o| records[o]))
                .collect();
            (Arc::new(UInt8Array::from(values)), DataType::UInt8)
        }
        VarType::Int => {
            let values: Vec<i32> = (0..record_count)
                .flat_map(|i| {
                    element_offsets(i)
                        .map(|o| i32::from_le_bytes(records[o..o + 4].try_into().unwrap()))
                })
                .collect();
            (Arc::new(Int32Array::from(values)), DataType::Int32)
        }
        VarType::BitField => {
            let values: Vec<u32> = (0..record_count)
                .flat_map(|i| {
                    element_offsets(i)
                        .map(|o| u32::from_le_bytes(records[o..o + 4].try_into().unwrap()))
                })
                .collect();
            (Arc::new(UInt32Array::from(values)), DataType::UInt32)
        }
        VarType::Float => {
            let values: Vec<f32> = (0..record_count)
                .flat_map(|i| {
                    element_offsets(i)
                        .map(|o| f32::from_le_bytes(records[o..o + 4].try_into().unwrap()))
                })
                .collect();
            (Arc::new(Float32Array::from(values)), DataType::Float32)
        }
        VarType::Double => {
            let values: Vec<f64> = (0..record_count)
                .flat_map(|i| {
                    element_offsets(i)
                        .map(|o| f64::from_le_bytes(records[o..o + 8].try_into().unwrap()))
                })
                .collect();
            (Arc::new(Float64Array::from(values)), DataType::Float64)
        }
    }
}

/// Build an Arrow RecordBatch for a single scalar (count == 1) channel.
///
/// The batch has columns: `timecodes` (Int64 ms) and the channel value column.
/// Channel metadata (units, desc, interpolate) is stored in field-level metadata.
///
/// Array variables are handled by `build_subsample_channel_batch` (`_ST`
/// time-subsample arrays) or `build_list_channel_batch` (entity-indexed
/// arrays such as `CarIdx*`).
pub fn build_channel_batch(
    name: &str,
    timecodes: &Arc<Int64Array>,
    var: &VarHeader,
    records: &[u8],
    buf_len: usize,
    record_count: usize,
) -> Result<RecordBatch> {
    if var.count != 1 {
        return Err(IbtError::OutOfBounds(format!(
            "'{}' is an array variable with {} elements",
            var.name, var.count
        )));
    }

    let metadata = ChannelMetadata::from_var_header(var).to_hashmap();
    let (values_col, data_type) = read_flattened_values(var, records, buf_len, record_count);

    let schema = Schema::new(vec![
        Field::new("timecodes", DataType::Int64, false),
        Field::new(name, data_type, false).with_metadata(metadata),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(timecodes.as_ref().clone()), values_col],
    )?;

    Ok(batch)
}

/// Build a vector-valued channel for an entity-indexed array variable.
///
/// Each row is a `FixedSizeList` of `count` elements aligned with the
/// tick's timestamp — e.g. `CarIdxLapDistPct` becomes one channel whose
/// rows are 64-element vectors indexed by car index. This mirrors how
/// pyirsdk and the official SDK expose array variables (a vector per
/// sample).
pub fn build_list_channel_batch(
    var: &VarHeader,
    timecodes: &Arc<Int64Array>,
    records: &[u8],
    buf_len: usize,
    record_count: usize,
) -> Result<RecordBatch> {
    if var.count < 2 {
        return Err(IbtError::OutOfBounds(format!(
            "Variable '{}' is not an array (count {})",
            var.name, var.count
        )));
    }

    let metadata = ChannelMetadata::from_var_header(var).to_hashmap();
    let (values, item_type) = read_flattened_values(var, records, buf_len, record_count);

    let item_field = Arc::new(Field::new("item", item_type, false));
    let list_col = FixedSizeListArray::try_new(item_field.clone(), var.count, values, None)
        .map_err(|e| IbtError::InvalidHeader(e.to_string()))?;
    let list_type = DataType::FixedSizeList(item_field, var.count);

    let schema = Schema::new(vec![
        Field::new("timecodes", DataType::Int64, false),
        Field::new(&var.name, list_type, false).with_metadata(metadata),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(timecodes.as_ref().clone()), Arc::new(list_col)],
    )?;

    Ok(batch)
}

/// Build a single higher-rate channel for a time-subsample array variable.
///
/// iRacing `_ST` variables record `count` sub-samples per tick (e.g. 6
/// samples at 360 Hz for a 60 Hz tick), with the last sub-sample aligned
/// with the tick's timestamp. This interleaves all sub-samples into one
/// channel of `record_count * count` rows, back-dating each sub-sample k
/// by `(count-1-k) / (tick_rate * count)` seconds from its tick.
pub fn build_subsample_channel_batch(
    var: &VarHeader,
    timecodes: &Arc<Int64Array>,
    records: &[u8],
    buf_len: usize,
    record_count: usize,
    tick_rate: i32,
) -> Result<RecordBatch> {
    let count = var.count as usize;
    if count < 2 {
        return Err(IbtError::OutOfBounds(format!(
            "Variable '{}' is not an array (count {})",
            var.name, var.count
        )));
    }
    if tick_rate <= 0 {
        return Err(IbtError::InvalidHeader(format!(
            "Cannot build sub-sample channel with tick rate {}",
            tick_rate
        )));
    }

    let metadata = ChannelMetadata::from_var_header(var).to_hashmap();

    // Millisecond offset of sub-sample k behind its tick's timestamp
    let sub_period_ms = 1000.0 / (tick_rate as f64 * count as f64);
    let back_offsets: Vec<i64> = (0..count)
        .map(|k| ((count - 1 - k) as f64 * sub_period_ms).round() as i64)
        .collect();

    let mut sub_timecodes: Vec<i64> = Vec::with_capacity(record_count * count);
    for i in 0..record_count {
        let tick = timecodes.value(i);
        for back in &back_offsets {
            sub_timecodes.push(tick - back);
        }
    }

    let (values_col, data_type) = read_flattened_values(var, records, buf_len, record_count);

    let schema = Schema::new(vec![
        Field::new("timecodes", DataType::Int64, false),
        Field::new(&var.name, data_type, false).with_metadata(metadata),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int64Array::from(sub_timecodes)), values_col],
    )?;

    Ok(batch)
}

/// A classified lap record with session information.
pub struct LapRecord {
    pub num: i32,
    pub start_time: i64,
    pub end_time: i64,
    pub lap_type: String,
    pub session: i32,
}

/// Build the laps table from classified lap records.
///
/// Returns a RecordBatch with columns: num (Int32), start_time (Int64), end_time (Int64),
/// lap_type (Utf8), session (Int32).
pub fn build_laps_batch(laps: &[LapRecord]) -> Result<RecordBatch> {
    let nums: Vec<i32> = laps.iter().map(|l| l.num).collect();
    let starts: Vec<i64> = laps.iter().map(|l| l.start_time).collect();
    let ends: Vec<i64> = laps.iter().map(|l| l.end_time).collect();
    let types: Vec<&str> = laps.iter().map(|l| l.lap_type.as_str()).collect();
    let sessions: Vec<i32> = laps.iter().map(|l| l.session).collect();

    let schema = Schema::new(vec![
        Field::new("num", DataType::Int32, false),
        Field::new("start_time", DataType::Int64, false),
        Field::new("end_time", DataType::Int64, false),
        Field::new("lap_type", DataType::Utf8, false),
        Field::new("session", DataType::Int32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(nums)),
            Arc::new(Int64Array::from(starts)),
            Arc::new(Int64Array::from(ends)),
            Arc::new(StringArray::from(types)),
            Arc::new(Int32Array::from(sessions)),
        ],
    )?;

    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn float_array_var() -> VarHeader {
        VarHeader {
            var_type: VarType::Float,
            offset: 4,
            count: 3,
            name: "Arr".to_string(),
            desc: "test array".to_string(),
            unit: "m".to_string(),
        }
    }

    /// Two records, each: one i32 pad then 3 f32 elements.
    fn float_array_records() -> Vec<u8> {
        let mut records = Vec::new();
        for record in 0..2 {
            records.extend_from_slice(&(-1i32).to_le_bytes());
            for element in 0..3 {
                let value = (record * 10 + element) as f32;
                records.extend_from_slice(&value.to_le_bytes());
            }
        }
        records
    }

    #[test]
    fn test_build_list_channel_batch() {
        let var = float_array_var();
        let records = float_array_records();
        let timecodes = Arc::new(Int64Array::from(vec![0i64, 1000]));

        let batch = build_list_channel_batch(&var, &timecodes, &records, 16, 2).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let field = batch.schema().field(1).clone();
        assert_eq!(field.name(), "Arr");
        assert_eq!(field.metadata()["units"], "m");
        assert_eq!(field.metadata()["interpolate"], "True");
        assert!(matches!(field.data_type(), DataType::FixedSizeList(_, 3)));

        let list = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap()
            .clone();
        let flat = list
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .clone();
        // record-major: row 0 = [0,1,2], row 1 = [10,11,12]
        assert_eq!(flat.values(), &[0.0, 1.0, 2.0, 10.0, 11.0, 12.0]);
    }

    #[test]
    fn test_build_list_channel_batch_rejects_scalar() {
        let var = VarHeader {
            var_type: VarType::Float,
            offset: 4,
            count: 1,
            name: "Scalar".to_string(),
            desc: "".to_string(),
            unit: "".to_string(),
        };
        let records = float_array_records();
        let timecodes = Arc::new(Int64Array::from(vec![0i64, 1000]));
        assert!(build_list_channel_batch(&var, &timecodes, &records, 16, 2).is_err());
    }

    #[test]
    fn test_build_channel_batch_scalar() {
        let var = VarHeader {
            var_type: VarType::Float,
            offset: 4,
            count: 1,
            name: "Scalar".to_string(),
            desc: "".to_string(),
            unit: "".to_string(),
        };
        let records = float_array_records();
        let timecodes = Arc::new(Int64Array::from(vec![0i64, 1000]));

        let batch = build_channel_batch("Scalar", &timecodes, &var, &records, 16, 2).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .clone();
        assert_eq!(values.values(), &[0.0, 10.0]);
    }

    #[test]
    fn test_build_channel_batch_rejects_array() {
        let var = float_array_var();
        let records = float_array_records();
        let timecodes = Arc::new(Int64Array::from(vec![0i64, 1000]));
        assert!(build_channel_batch("Arr", &timecodes, &var, &records, 16, 2).is_err());
    }

    #[test]
    fn test_build_subsample_channel_batch() {
        let var = float_array_var();
        let records = float_array_records();
        let timecodes = Arc::new(Int64Array::from(vec![0i64, 17]));

        // 3 sub-samples per tick at 60 Hz -> 180 Hz, period ~5.56ms.
        // Sub-sample k is back-dated (count-1-k) periods from its tick.
        let batch = build_subsample_channel_batch(&var, &timecodes, &records, 16, 2, 60).unwrap();
        assert_eq!(batch.num_rows(), 6);

        let field = batch.schema().field(1).clone();
        assert_eq!(field.name(), "Arr");
        assert_eq!(field.metadata()["units"], "m");

        let sub_timecodes = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        assert_eq!(sub_timecodes.values(), &[-11, -6, 0, 6, 11, 17]);

        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .clone();
        assert_eq!(values.values(), &[0.0, 1.0, 2.0, 10.0, 11.0, 12.0]);
    }

    #[test]
    fn test_build_subsample_channel_batch_rejects_scalar() {
        let var = VarHeader {
            var_type: VarType::Float,
            offset: 4,
            count: 1,
            name: "Scalar".to_string(),
            desc: "".to_string(),
            unit: "".to_string(),
        };
        let records = float_array_records();
        let timecodes = Arc::new(Int64Array::from(vec![0i64, 17]));
        assert!(build_subsample_channel_batch(&var, &timecodes, &records, 16, 2, 60).is_err());
        // and a zero tick rate is rejected for arrays
        let var = float_array_var();
        assert!(build_subsample_channel_batch(&var, &timecodes, &records, 16, 2, 0).is_err());
    }
}
