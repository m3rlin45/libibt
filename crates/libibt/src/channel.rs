use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, UInt32Array, UInt8Array,
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

/// Build an Arrow RecordBatch for a single channel.
///
/// The batch has columns: `timecodes` (Int64 ms) and the channel value column.
/// Channel metadata (units, desc, interpolate) is stored in field-level metadata.
pub fn build_channel_batch(
    name: &str,
    timecodes: &Arc<Int64Array>,
    var: &VarHeader,
    records: &[u8],
    buf_len: usize,
    record_count: usize,
) -> Result<RecordBatch> {
    let metadata = ChannelMetadata::from_var_header(var).to_hashmap();

    if var.count > 1 {
        return Err(IbtError::OutOfBounds(
            "Array variables (count > 1) are not supported".to_string(),
        ));
    }

    let offset = var.offset as usize;

    let (values_col, data_type): (ArrayRef, DataType) = match var.var_type {
        VarType::Bool => {
            let values: Vec<bool> = (0..record_count)
                .map(|i| records[i * buf_len + offset] != 0)
                .collect();
            (Arc::new(BooleanArray::from(values)), DataType::Boolean)
        }
        VarType::Char => {
            let values: Vec<u8> = (0..record_count)
                .map(|i| records[i * buf_len + offset])
                .collect();
            (Arc::new(UInt8Array::from(values)), DataType::UInt8)
        }
        VarType::Int => {
            let values: Vec<i32> = (0..record_count)
                .map(|i| {
                    let o = i * buf_len + offset;
                    i32::from_le_bytes(records[o..o + 4].try_into().unwrap())
                })
                .collect();
            (Arc::new(Int32Array::from(values)), DataType::Int32)
        }
        VarType::BitField => {
            let values: Vec<u32> = (0..record_count)
                .map(|i| {
                    let o = i * buf_len + offset;
                    u32::from_le_bytes(records[o..o + 4].try_into().unwrap())
                })
                .collect();
            (Arc::new(UInt32Array::from(values)), DataType::UInt32)
        }
        VarType::Float => {
            let values: Vec<f32> = (0..record_count)
                .map(|i| {
                    let o = i * buf_len + offset;
                    f32::from_le_bytes(records[o..o + 4].try_into().unwrap())
                })
                .collect();
            (Arc::new(Float32Array::from(values)), DataType::Float32)
        }
        VarType::Double => {
            let values: Vec<f64> = (0..record_count)
                .map(|i| {
                    let o = i * buf_len + offset;
                    f64::from_le_bytes(records[o..o + 8].try_into().unwrap())
                })
                .collect();
            (Arc::new(Float64Array::from(values)), DataType::Float64)
        }
    };

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
