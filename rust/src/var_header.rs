use crate::error::{IbtError, Result};

/// IBT variable type identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum VarType {
    Char = 0,
    Bool = 1,
    Int = 2,
    BitField = 3,
    Float = 4,
    Double = 5,
}

impl VarType {
    pub fn from_i32(v: i32) -> Result<Self> {
        match v {
            0 => Ok(Self::Char),
            1 => Ok(Self::Bool),
            2 => Ok(Self::Int),
            3 => Ok(Self::BitField),
            4 => Ok(Self::Float),
            5 => Ok(Self::Double),
            _ => Err(IbtError::InvalidHeader(format!(
                "Unknown variable type: {}",
                v
            ))),
        }
    }

    /// Size in bytes of a single element of this type.
    pub fn element_size(self) -> usize {
        match self {
            Self::Char | Self::Bool => 1,
            Self::Int | Self::BitField | Self::Float => 4,
            Self::Double => 8,
        }
    }
}

/// Metadata for a single telemetry variable.
#[derive(Debug, Clone)]
pub struct VarHeader {
    pub var_type: VarType,
    pub offset: i32,
    pub count: i32,
    pub name: String,
    pub desc: String,
    pub unit: String,
}

const VAR_HEADER_SIZE: usize = 144;

fn read_fixed_string(data: &[u8]) -> String {
    let end = data.iter().position(|&b| b == 0).unwrap_or(data.len());
    String::from_utf8_lossy(&data[..end]).into_owned()
}

impl VarHeader {
    pub const SIZE: usize = VAR_HEADER_SIZE;

    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < VAR_HEADER_SIZE {
            return Err(IbtError::InvalidHeader(format!(
                "Variable header too small: {} bytes (need {})",
                data.len(),
                VAR_HEADER_SIZE
            )));
        }

        let var_type =
            VarType::from_i32(i32::from_le_bytes(data[0..4].try_into().unwrap()))?;
        let offset = i32::from_le_bytes(data[4..8].try_into().unwrap());
        let count = i32::from_le_bytes(data[8..12].try_into().unwrap());
        if count < 0 {
            return Err(IbtError::InvalidHeader(format!(
                "Variable count must be non-negative, got {}",
                count
            )));
        }
        let name = read_fixed_string(&data[16..48]);
        let desc = read_fixed_string(&data[48..112]);
        let unit = read_fixed_string(&data[112..144]);

        Ok(Self {
            var_type,
            offset,
            count,
            name,
            desc,
            unit,
        })
    }

    pub fn parse_all(data: &[u8], offset: usize, count: usize) -> Result<Vec<Self>> {
        let end = offset + count * VAR_HEADER_SIZE;
        if data.len() < end {
            return Err(IbtError::InvalidHeader(format!(
                "File too small for {} variable headers at offset {}: {} bytes (need {})",
                count,
                offset,
                data.len(),
                end
            )));
        }

        (0..count)
            .map(|i| {
                let start = offset + i * VAR_HEADER_SIZE;
                Self::parse(&data[start..start + VAR_HEADER_SIZE])
            })
            .collect()
    }

    pub fn byte_size(&self) -> usize {
        self.var_type.element_size() * self.count as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_var_type_sizes() {
        assert_eq!(VarType::Char.element_size(), 1);
        assert_eq!(VarType::Bool.element_size(), 1);
        assert_eq!(VarType::Int.element_size(), 4);
        assert_eq!(VarType::BitField.element_size(), 4);
        assert_eq!(VarType::Float.element_size(), 4);
        assert_eq!(VarType::Double.element_size(), 8);
    }

    #[test]
    fn test_var_type_invalid() {
        assert!(VarType::from_i32(6).is_err());
        assert!(VarType::from_i32(-1).is_err());
    }
}
