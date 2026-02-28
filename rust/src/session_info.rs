use crate::error::{IbtError, Result};

/// Extract the session info YAML string from the file data.
///
/// The session info area may contain leading null bytes before the `---` YAML marker.
pub fn extract_session_yaml(data: &[u8], offset: usize, len: usize) -> Result<String> {
    let end = offset + len;
    if data.len() < end {
        return Err(IbtError::InvalidHeader(format!(
            "File too small for session info: {} bytes (need {})",
            data.len(),
            end
        )));
    }

    let raw = &data[offset..end];

    // Find the YAML start marker
    let yaml_start = raw.windows(3).position(|w| w == b"---").unwrap_or_else(|| {
        // No marker found; skip leading nulls
        raw.iter().position(|&b| b != 0).unwrap_or(0)
    });

    // Trim trailing nulls
    let yaml_end = raw[yaml_start..]
        .iter()
        .rposition(|&b| b != 0)
        .map(|p| yaml_start + p + 1)
        .unwrap_or(yaml_start);

    // iRacing YAML may contain Latin-1 encoded characters (e.g. driver names).
    // Use lossy conversion to replace invalid UTF-8 bytes with U+FFFD.
    Ok(String::from_utf8_lossy(&raw[yaml_start..yaml_end]).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_yaml_with_leading_nulls() {
        let mut data = vec![0u8; 100];
        let yaml = b"---\nWeekendInfo:\n  TrackName: test\n";
        data[10..10 + yaml.len()].copy_from_slice(yaml);
        let result = extract_session_yaml(&data, 0, 100).unwrap();
        assert!(result.starts_with("---"));
        assert!(result.contains("TrackName"));
    }
}
