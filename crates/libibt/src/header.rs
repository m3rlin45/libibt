use crate::error::{IbtError, Result};

/// Main IBT file header (112 bytes at offset 0).
#[derive(Debug, Clone)]
pub struct IbtHeader {
    pub ver: i32,
    pub status: i32,
    pub tick_rate: i32,
    pub session_info_update: i32,
    pub session_info_offset: i32,
    pub session_info_len: i32,
    pub num_vars: i32,
    pub var_header_offset: i32,
    pub num_buf: i32,
    pub buf_len: i32,
    pub var_bufs: [VarBufEntry; 4],
}

/// A variable buffer descriptor within the header.
#[derive(Debug, Clone, Copy, Default)]
pub struct VarBufEntry {
    pub tick_count: i32,
    pub buf_offset: i32,
}

/// Disk sub-header (32 bytes at offset 112).
#[derive(Debug, Clone)]
pub struct DiskSubHeader {
    pub session_start_date: i64,
    pub start_time: f64,
    pub end_time: f64,
    pub lap_count: i32,
    pub session_record_count: i32,
}

const HEADER_SIZE: usize = 112;
const DISK_SUB_HEADER_OFFSET: usize = HEADER_SIZE;
const DISK_SUB_HEADER_SIZE: usize = 32;

fn read_i32(data: &[u8], offset: usize) -> i32 {
    i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}

fn read_i64(data: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
}

fn read_f64(data: &[u8], offset: usize) -> f64 {
    f64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
}

/// Validate that an i32 header field is non-negative and convert to usize.
pub(crate) fn checked_usize(val: i32, field: &str) -> Result<usize> {
    usize::try_from(val).map_err(|_| {
        IbtError::InvalidHeader(format!("{} must be non-negative, got {}", field, val))
    })
}

impl IbtHeader {
    /// Parse the main IBT header from the first 112 bytes.
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(IbtError::InvalidHeader(format!(
                "File too small for header: {} bytes (need {})",
                data.len(),
                HEADER_SIZE
            )));
        }

        let ver = read_i32(data, 0);
        if ver != 1 && ver != 2 {
            return Err(IbtError::InvalidHeader(format!(
                "Unsupported IBT version: {}",
                ver
            )));
        }

        let mut var_bufs = [VarBufEntry::default(); 4];
        for (i, buf) in var_bufs.iter_mut().enumerate() {
            let base = 48 + i * 16;
            *buf = VarBufEntry {
                tick_count: read_i32(data, base),
                buf_offset: read_i32(data, base + 4),
            };
        }

        Ok(Self {
            ver,
            status: read_i32(data, 4),
            tick_rate: read_i32(data, 8),
            session_info_update: read_i32(data, 12),
            session_info_offset: read_i32(data, 16),
            session_info_len: read_i32(data, 20),
            num_vars: read_i32(data, 24),
            var_header_offset: read_i32(data, 28),
            num_buf: read_i32(data, 32),
            buf_len: read_i32(data, 36),
            var_bufs,
        })
    }

    /// Offset to the start of telemetry data records.
    pub fn data_offset(&self) -> Result<usize> {
        checked_usize(self.var_bufs[0].buf_offset, "buf_offset")
    }
}

impl DiskSubHeader {
    /// Parse the disk sub-header from offset 112.
    pub fn parse(data: &[u8]) -> Result<Self> {
        let min_size = DISK_SUB_HEADER_OFFSET + DISK_SUB_HEADER_SIZE;
        if data.len() < min_size {
            return Err(IbtError::InvalidHeader(format!(
                "File too small for disk sub-header: {} bytes (need {})",
                data.len(),
                min_size
            )));
        }

        let off = DISK_SUB_HEADER_OFFSET;
        Ok(Self {
            session_start_date: read_i64(data, off),
            start_time: read_f64(data, off + 8),
            end_time: read_f64(data, off + 16),
            lap_count: read_i32(data, off + 24),
            session_record_count: read_i32(data, off + 28),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_too_small() {
        let data = vec![0u8; 50];
        assert!(IbtHeader::parse(&data).is_err());
    }

    #[test]
    fn test_header_bad_version() {
        let mut data = vec![0u8; 112];
        data[0] = 99;
        assert!(IbtHeader::parse(&data).is_err());
    }
}
