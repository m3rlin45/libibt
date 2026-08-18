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
///
/// For array variables (count > 1) this builds the channel for a single
/// element; callers use `build_var_batches` to expand all elements.
pub fn build_channel_batch(
    name: &str,
    timecodes: &Arc<Int64Array>,
    var: &VarHeader,
    records: &[u8],
    buf_len: usize,
    record_count: usize,
) -> Result<RecordBatch> {
    build_element_batch(name, timecodes, var, 0, records, buf_len, record_count)
}

/// Build Arrow RecordBatches for every channel a variable produces.
///
/// Scalar variables (count == 1) produce a single channel named after the
/// variable. Array variables (count > 1) produce one channel per element,
/// named `Name[0]`, `Name[1]`, ... All elements share the variable's
/// units/desc/interpolate metadata.
pub fn build_var_batches(
    var: &VarHeader,
    timecodes: &Arc<Int64Array>,
    records: &[u8],
    buf_len: usize,
    record_count: usize,
) -> Result<Vec<(String, RecordBatch)>> {
    if var.count == 1 {
        let batch =
            build_element_batch(&var.name, timecodes, var, 0, records, buf_len, record_count)?;
        return Ok(vec![(var.name.clone(), batch)]);
    }

    (0..var.count as usize)
        .map(|element| {
            let name = format!("{}[{}]", var.name, element);
            let batch = build_element_batch(
                &name,
                timecodes,
                var,
                element,
                records,
                buf_len,
                record_count,
            )?;
            Ok((name, batch))
        })
        .collect()
}

/// Split an array element channel name like `"Name[3]"` into `("Name", 3)`.
///
/// Returns None if the name does not end in a bracketed non-negative integer.
pub(crate) fn parse_element_name(name: &str) -> Option<(&str, usize)> {
    let rest = name.strip_suffix(']')?;
    let open = rest.rfind('[')?;
    let element: usize = rest[open + 1..].parse().ok()?;
    Some((&rest[..open], element))
}

/// Build the RecordBatch for one element of a variable.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_element_batch(
    name: &str,
    timecodes: &Arc<Int64Array>,
    var: &VarHeader,
    element: usize,
    records: &[u8],
    buf_len: usize,
    record_count: usize,
) -> Result<RecordBatch> {
    if element >= var.count as usize {
        return Err(IbtError::OutOfBounds(format!(
            "Element {} out of range for variable '{}' (count {})",
            element, var.name, var.count
        )));
    }

    let metadata = ChannelMetadata::from_var_header(var).to_hashmap();

    let offset = var.offset as usize + element * var.var_type.element_size();

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
    fn test_parse_element_name() {
        assert_eq!(parse_element_name("Arr[0]"), Some(("Arr", 0)));
        assert_eq!(parse_element_name("Arr[12]"), Some(("Arr", 12)));
        assert_eq!(parse_element_name("Arr"), None);
        assert_eq!(parse_element_name("Arr[]"), None);
        assert_eq!(parse_element_name("Arr[-1]"), None);
        assert_eq!(parse_element_name("Arr[x]"), None);
    }

    #[test]
    fn test_build_var_batches_expands_array() {
        let var = float_array_var();
        let records = float_array_records();
        let timecodes = Arc::new(Int64Array::from(vec![0i64, 1000]));

        let batches = build_var_batches(&var, &timecodes, &records, 16, 2).unwrap();
        assert_eq!(batches.len(), 3);

        for (element, (name, batch)) in batches.iter().enumerate() {
            assert_eq!(name, &format!("Arr[{}]", element));
            assert_eq!(batch.num_rows(), 2);
            let field = batch.schema().field(1).clone();
            assert_eq!(field.name(), name);
            assert_eq!(field.metadata()["units"], "m");
            assert_eq!(field.metadata()["interpolate"], "True");
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .clone();
            assert_eq!(values.value(0), element as f32);
            assert_eq!(values.value(1), (10 + element) as f32);
        }
    }

    #[test]
    fn test_build_var_batches_scalar_unchanged() {
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

        let batches = build_var_batches(&var, &timecodes, &records, 16, 2).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0, "Scalar");
    }

    #[test]
    fn test_build_element_batch_out_of_range() {
        let var = float_array_var();
        let records = float_array_records();
        let timecodes = Arc::new(Int64Array::from(vec![0i64, 1000]));

        let result = build_element_batch("Arr[3]", &timecodes, &var, 3, &records, 16, 2);
        assert!(result.is_err());
    }
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
