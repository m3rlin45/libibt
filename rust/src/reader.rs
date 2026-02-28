use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use memmap2::Mmap;

use crate::channel::{build_channel_batch, build_laps_batch};
use crate::error::{IbtError, Result};
use crate::header::{checked_usize, DiskSubHeader, IbtHeader};
use crate::session_info::extract_session_yaml;
use crate::var_header::{VarHeader, VarType};

/// A parsed IBT telemetry file.
///
/// Memory-maps the file for zero-copy access to telemetry data.
pub struct IbtFile {
    mmap: Mmap,
    pub header: IbtHeader,
    pub sub_header: DiskSubHeader,
    pub var_headers: Vec<VarHeader>,
    session_yaml: String,
    var_index: HashMap<String, usize>,
}

impl IbtFile {
    /// Open and parse an IBT file from disk.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = std::fs::File::open(path.as_ref())?;
        let mmap = unsafe { Mmap::map(&file)? };
        Self::from_mmap(mmap)
    }

    /// Parse an IBT file from in-memory bytes.
    pub fn from_bytes(data: Vec<u8>) -> Result<Self> {
        let mut mmap = memmap2::MmapMut::map_anon(data.len())?;
        mmap.copy_from_slice(&data);
        let mmap = mmap.make_read_only()?;
        Self::from_mmap(mmap)
    }

    fn from_mmap(mmap: Mmap) -> Result<Self> {
        let data = &mmap[..];

        let header = IbtHeader::parse(data)?;
        let sub_header = DiskSubHeader::parse(data)?;

        let var_header_offset = checked_usize(header.var_header_offset, "var_header_offset")?;
        let num_vars = checked_usize(header.num_vars, "num_vars")?;
        let var_headers = VarHeader::parse_all(data, var_header_offset, num_vars)?;

        let session_info_offset = checked_usize(header.session_info_offset, "session_info_offset")?;
        let session_info_len = checked_usize(header.session_info_len, "session_info_len")?;
        let session_yaml = extract_session_yaml(data, session_info_offset, session_info_len)?;

        // Validate each variable fits within a single record buffer
        let buf_len = checked_usize(header.buf_len, "buf_len")?;
        for var in &var_headers {
            let var_offset = checked_usize(var.offset, "var offset")?;
            let var_end = var_offset + var.byte_size();
            if var_end > buf_len {
                return Err(IbtError::InvalidHeader(format!(
                    "Variable '{}' offset {} + size {} exceeds buffer length {}",
                    var.name,
                    var.offset,
                    var.byte_size(),
                    header.buf_len
                )));
            }
        }

        // Validate data region fits in the file
        let data_offset = header.data_offset()?;
        let record_count = checked_usize(sub_header.session_record_count, "session_record_count")?;
        let expected_end = buf_len
            .checked_mul(record_count)
            .and_then(|v| v.checked_add(data_offset))
            .ok_or_else(|| IbtError::InvalidHeader("Data region arithmetic overflow".into()))?;
        if mmap.len() < expected_end {
            return Err(IbtError::InvalidHeader(format!(
                "File too small for {} records: {} bytes (need {})",
                record_count,
                mmap.len(),
                expected_end
            )));
        }

        let var_index: HashMap<String, usize> = var_headers
            .iter()
            .enumerate()
            .map(|(i, vh)| (vh.name.clone(), i))
            .collect();

        Ok(Self {
            mmap,
            header,
            sub_header,
            var_headers,
            session_yaml,
            var_index,
        })
    }

    /// Raw telemetry data region (all records, contiguous).
    fn records_data(&self) -> Result<&[u8]> {
        let start = self.header.data_offset()?;
        let len = self.header.buf_len as usize * self.record_count();
        Ok(&self.mmap[start..start + len])
    }

    pub fn record_count(&self) -> usize {
        self.sub_header.session_record_count as usize
    }

    pub fn buf_len(&self) -> usize {
        self.header.buf_len as usize
    }

    pub fn tick_rate(&self) -> i32 {
        self.header.tick_rate
    }

    pub fn session_info_yaml(&self) -> &str {
        &self.session_yaml
    }

    pub fn var_by_name(&self, name: &str) -> Option<&VarHeader> {
        self.var_index.get(name).map(|&i| &self.var_headers[i])
    }

    /// Build timecodes from SessionTime (f64 seconds → i64 milliseconds).
    pub fn build_timecodes(&self) -> Result<Arc<Int64Array>> {
        let session_time = self
            .var_by_name("SessionTime")
            .ok_or_else(|| IbtError::InvalidHeader("Missing SessionTime variable".into()))?;

        if session_time.var_type != VarType::Double {
            return Err(IbtError::InvalidHeader(format!(
                "SessionTime should be double, got {:?}",
                session_time.var_type
            )));
        }

        let records = self.records_data()?;
        let buf_len = self.buf_len();
        let offset = session_time.offset as usize;
        let count = self.record_count();

        let values: Vec<i64> = (0..count)
            .map(|i| {
                let o = i * buf_len + offset;
                let seconds = f64::from_le_bytes(records[o..o + 8].try_into().unwrap());
                (seconds * 1000.0).round() as i64
            })
            .collect();

        Ok(Arc::new(Int64Array::from(values)))
    }

    /// Build an Arrow RecordBatch for a single channel.
    pub fn channel_to_arrow(
        &self,
        var_name: &str,
        timecodes: &Arc<Int64Array>,
    ) -> Result<RecordBatch> {
        let var = self
            .var_by_name(var_name)
            .ok_or_else(|| IbtError::OutOfBounds(format!("Variable not found: {}", var_name)))?
            .clone();

        build_channel_batch(
            var_name,
            timecodes,
            &var,
            self.records_data()?,
            self.buf_len(),
            self.record_count(),
        )
    }

    /// Build Arrow RecordBatches for all scalar channels.
    pub fn all_channels_to_arrow(
        &self,
        timecodes: &Arc<Int64Array>,
    ) -> Result<Vec<(String, RecordBatch)>> {
        let records = self.records_data()?;
        let buf_len = self.buf_len();
        let record_count = self.record_count();

        self.var_headers
            .iter()
            .filter(|var| var.count == 1) // skip array variables
            .map(|var| {
                let batch =
                    build_channel_batch(&var.name, timecodes, var, records, buf_len, record_count)?;
                Ok((var.name.clone(), batch))
            })
            .collect()
    }

    /// Extract lap boundaries from the Lap telemetry variable.
    pub fn extract_laps(&self, timecodes: &Arc<Int64Array>) -> Result<RecordBatch> {
        let lap_var = match self.var_by_name("Lap") {
            Some(v) => v,
            None => return build_laps_batch(&[]),
        };

        let records = self.records_data()?;
        let buf_len = self.buf_len();
        let record_count = self.record_count();
        let offset = lap_var.offset as usize;

        if record_count == 0 {
            return build_laps_batch(&[]);
        }

        // Read all lap values
        let lap_values: Vec<i32> = (0..record_count)
            .map(|i| {
                let o = i * buf_len + offset;
                i32::from_le_bytes(records[o..o + 4].try_into().unwrap())
            })
            .collect();

        // Find lap transitions
        let mut laps: Vec<(i32, i64, i64)> = Vec::new();
        let mut current_lap = lap_values[0];
        let mut lap_start_time = timecodes.value(0);

        #[allow(clippy::needless_range_loop)]
        for i in 1..record_count {
            if lap_values[i] != current_lap {
                let transition_time = timecodes.value(i);
                laps.push((current_lap, lap_start_time, transition_time));
                current_lap = lap_values[i];
                lap_start_time = transition_time;
            }
        }

        // Close the final lap
        let end_time = timecodes.value(record_count - 1);
        laps.push((current_lap, lap_start_time, end_time));

        build_laps_batch(&laps)
    }
}
