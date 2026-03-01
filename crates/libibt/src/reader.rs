use std::collections::HashMap;
use std::path::Path;

use memmap2::Mmap;

use crate::error::{IbtError, Result};
use crate::header::{checked_usize, DiskSubHeader, IbtHeader};
use crate::session_info::extract_session_yaml;
use crate::var_header::VarHeader;

/// A parsed IBT telemetry file.
///
/// Memory-maps the file for zero-copy access to telemetry data.
pub struct IbtFile {
    // Kept alive to maintain the memory mapping; accessed via records_data()
    #[allow(dead_code)]
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
    #[cfg_attr(not(feature = "arrow"), allow(dead_code))]
    pub(crate) fn records_data(&self) -> Result<&[u8]> {
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
}

#[cfg(feature = "arrow")]
mod arrow_ext {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch};

    use crate::channel::{build_channel_batch, build_laps_batch, LapRecord};
    use crate::error::{IbtError, Result};
    use crate::var_header::{VarHeader, VarType};

    use super::IbtFile;

    impl IbtFile {
        /// Build timecodes from SessionTime (f64 seconds -> i64 milliseconds).
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
                    let batch = build_channel_batch(
                        &var.name,
                        timecodes,
                        var,
                        records,
                        buf_len,
                        record_count,
                    )?;
                    Ok((var.name.clone(), batch))
                })
                .collect()
        }

        /// Read a bool variable's values from raw records.
        fn read_bool_values(
            var: &VarHeader,
            records: &[u8],
            buf_len: usize,
            record_count: usize,
        ) -> Vec<bool> {
            let offset = var.offset as usize;
            (0..record_count)
                .map(|i| records[i * buf_len + offset] != 0)
                .collect()
        }

        /// Read a float variable's values from raw records (handles f32 and f64).
        fn read_float_values(
            var: &VarHeader,
            records: &[u8],
            buf_len: usize,
            record_count: usize,
        ) -> Vec<f32> {
            let offset = var.offset as usize;
            match var.var_type {
                VarType::Float => (0..record_count)
                    .map(|i| {
                        let o = i * buf_len + offset;
                        f32::from_le_bytes(records[o..o + 4].try_into().unwrap())
                    })
                    .collect(),
                VarType::Double => (0..record_count)
                    .map(|i| {
                        let o = i * buf_len + offset;
                        f64::from_le_bytes(records[o..o + 8].try_into().unwrap()) as f32
                    })
                    .collect(),
                _ => vec![0.0; record_count],
            }
        }

        /// Classify a single lap based on sample count, pit road status, and track distance.
        fn classify_lap(
            sample_count: usize,
            on_pit_road_start: bool,
            on_pit_road_end: bool,
            lap_dist_pct_start: f32,
            lap_dist_pct_end: f32,
        ) -> &'static str {
            if sample_count < 2 {
                return "incomplete";
            }
            if on_pit_road_start {
                return "out";
            }
            if on_pit_road_end {
                return "in";
            }
            let sf_threshold = 0.02;
            let sf_upper = 1.0 - sf_threshold;
            let start_away = lap_dist_pct_start > sf_threshold && lap_dist_pct_start < sf_upper;
            let end_away = lap_dist_pct_end > sf_threshold && lap_dist_pct_end < sf_upper;
            if start_away || end_away {
                return "partial";
            }
            "full"
        }

        /// Extract lap boundaries with classification and session detection.
        #[allow(clippy::too_many_lines)]
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

            // Read classification signals (optional — gracefully degrade if missing)
            let on_pit_road_values: Option<Vec<bool>> = self
                .var_by_name("OnPitRoad")
                .map(|var| Self::read_bool_values(var, records, buf_len, record_count));
            let lap_dist_pct_values: Option<Vec<f32>> = self
                .var_by_name("LapDistPct")
                .map(|var| Self::read_float_values(var, records, buf_len, record_count));

            // Find lap transitions and record boundaries
            struct LapBoundary {
                num: i32,
                start_idx: usize,
                end_idx: usize, // exclusive
                start_time: i64,
                end_time: i64,
            }

            let mut boundaries: Vec<LapBoundary> = Vec::new();
            let mut current_lap = lap_values[0];
            let mut lap_start_idx: usize = 0;
            let mut lap_start_time = timecodes.value(0);

            #[allow(clippy::needless_range_loop)]
            for i in 1..record_count {
                if lap_values[i] != current_lap {
                    let transition_time = timecodes.value(i);
                    boundaries.push(LapBoundary {
                        num: current_lap,
                        start_idx: lap_start_idx,
                        end_idx: i,
                        start_time: lap_start_time,
                        end_time: transition_time,
                    });
                    current_lap = lap_values[i];
                    lap_start_idx = i;
                    lap_start_time = transition_time;
                }
            }

            // Close the final lap
            let end_time = timecodes.value(record_count - 1);
            boundaries.push(LapBoundary {
                num: current_lap,
                start_idx: lap_start_idx,
                end_idx: record_count,
                start_time: lap_start_time,
                end_time,
            });

            // Classify each lap and detect sessions
            let mut session: i32 = 0;
            let mut first_lap = true;
            let mut lap_records: Vec<LapRecord> = Vec::with_capacity(boundaries.len());

            for boundary in &boundaries {
                // Session detection: increment when lap number resets to 0
                if boundary.num == 0 && !first_lap {
                    session += 1;
                }
                first_lap = false;

                let sample_count = boundary.end_idx - boundary.start_idx;

                let on_pit_start = on_pit_road_values
                    .as_ref()
                    .is_some_and(|v| v[boundary.start_idx]);
                let on_pit_end = on_pit_road_values
                    .as_ref()
                    .is_some_and(|v| v[boundary.end_idx.saturating_sub(1)]);

                let dist_pct_start = lap_dist_pct_values
                    .as_ref()
                    .map_or(0.0, |v| v[boundary.start_idx]);
                let dist_pct_end = lap_dist_pct_values
                    .as_ref()
                    .map_or(0.0, |v| v[boundary.end_idx.saturating_sub(1)]);

                let lap_type = Self::classify_lap(
                    sample_count,
                    on_pit_start,
                    on_pit_end,
                    dist_pct_start,
                    dist_pct_end,
                );

                lap_records.push(LapRecord {
                    num: boundary.num,
                    start_time: boundary.start_time,
                    end_time: boundary.end_time,
                    lap_type: lap_type.to_string(),
                    session,
                });
            }

            build_laps_batch(&lap_records)
        }
    }
}

#[cfg(all(test, feature = "arrow"))]
mod tests {
    fn classify_lap(
        sample_count: usize,
        on_pit_road_start: bool,
        on_pit_road_end: bool,
        lap_dist_pct_start: f32,
        lap_dist_pct_end: f32,
    ) -> &'static str {
        if sample_count < 2 {
            return "incomplete";
        }
        if on_pit_road_start {
            return "out";
        }
        if on_pit_road_end {
            return "in";
        }
        let sf_threshold = 0.02;
        let sf_upper = 1.0 - sf_threshold;
        let start_away = lap_dist_pct_start > sf_threshold && lap_dist_pct_start < sf_upper;
        let end_away = lap_dist_pct_end > sf_threshold && lap_dist_pct_end < sf_upper;
        if start_away || end_away {
            return "partial";
        }
        "full"
    }

    #[test]
    fn test_classify_full_lap() {
        assert_eq!(classify_lap(100, false, false, 0.0, 0.0), "full");
        assert_eq!(classify_lap(100, false, false, 0.01, 0.99), "full");
    }

    #[test]
    fn test_classify_incomplete_lap() {
        assert_eq!(classify_lap(0, false, false, 0.0, 0.0), "incomplete");
        assert_eq!(classify_lap(1, false, false, 0.0, 0.0), "incomplete");
    }

    #[test]
    fn test_classify_out_lap() {
        assert_eq!(classify_lap(100, true, false, 0.0, 0.0), "out");
        // OnPitRoad at start takes priority over end
        assert_eq!(classify_lap(100, true, true, 0.0, 0.0), "out");
    }

    #[test]
    fn test_classify_in_lap() {
        assert_eq!(classify_lap(100, false, true, 0.0, 0.0), "in");
    }

    #[test]
    fn test_classify_partial_lap() {
        // Start away from S/F
        assert_eq!(classify_lap(100, false, false, 0.5, 0.0), "partial");
        // End away from S/F
        assert_eq!(classify_lap(100, false, false, 0.0, 0.5), "partial");
        // Both away
        assert_eq!(classify_lap(100, false, false, 0.3, 0.7), "partial");
    }

    #[test]
    fn test_classify_priority_incomplete_over_pit() {
        // < 2 samples wins over pit road
        assert_eq!(classify_lap(1, true, false, 0.0, 0.0), "incomplete");
    }

    #[test]
    fn test_classify_priority_pit_over_partial() {
        // OnPitRoad wins over LapDistPct
        assert_eq!(classify_lap(100, true, false, 0.5, 0.5), "out");
        assert_eq!(classify_lap(100, false, true, 0.5, 0.5), "in");
    }

    #[test]
    fn test_classify_near_sf_thresholds() {
        // At the threshold boundary (0.02) — not away from S/F
        assert_eq!(classify_lap(100, false, false, 0.02, 0.0), "full");
        assert_eq!(classify_lap(100, false, false, 0.0, 0.98), "full");
        // Just beyond threshold
        assert_eq!(classify_lap(100, false, false, 0.021, 0.0), "partial");
        assert_eq!(classify_lap(100, false, false, 0.0, 0.979), "partial");
    }

    #[test]
    fn test_session_detection_no_resets() {
        let lap_nums = vec![0i32, 1, 2, 3];
        let mut session = 0i32;
        let mut first_lap = true;
        let mut sessions = Vec::new();

        for &num in &lap_nums {
            if num == 0 && !first_lap {
                session += 1;
            }
            first_lap = false;
            sessions.push(session);
        }

        assert_eq!(sessions, vec![0, 0, 0, 0]);
    }

    #[test]
    fn test_session_detection_with_resets() {
        // Lap numbers [0, 1, 0, 1, 2] — one reset, two sessions
        let lap_nums = vec![0i32, 1, 0, 1, 2];
        let mut session = 0i32;
        let mut first_lap = true;
        let mut sessions = Vec::new();

        for &num in &lap_nums {
            if num == 0 && !first_lap {
                session += 1;
            }
            first_lap = false;
            sessions.push(session);
        }

        assert_eq!(sessions, vec![0, 0, 1, 1, 1]);
    }

    #[test]
    fn test_session_detection_multiple_resets() {
        // Lap numbers [0, 1, 0, 1, 0, 1, 2, 3] — two resets, three sessions
        let lap_nums = vec![0i32, 1, 0, 1, 0, 1, 2, 3];
        let mut session = 0i32;
        let mut first_lap = true;
        let mut sessions = Vec::new();

        for &num in &lap_nums {
            if num == 0 && !first_lap {
                session += 1;
            }
            first_lap = false;
            sessions.push(session);
        }

        assert_eq!(sessions, vec![0, 0, 1, 1, 2, 2, 2, 2]);
    }
}
