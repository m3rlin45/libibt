//! The session metadata document: the keys every binding exposes, their
//! order and their value types.
//!
//! Bindings map [`MetadataValue`] onto their own types (a Python dict, a
//! JSON object) and add nothing of their own, so the layout is defined once.

use crate::reader::IbtFile;

/// One value of the metadata document.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataValue {
    Str(String),
    Int(i64),
    Float(f64),
}

/// `Key: value` lines of the session-info YAML that are lifted into the
/// document, with the key each is published under.
const SCANNED: &[(&str, &str)] = &[
    ("TrackName: ", "track_name"),
    ("TrackDisplayName: ", "track_display_name"),
    ("TrackCity: ", "track_city"),
    ("TrackCountry: ", "track_country"),
    ("TrackLength: ", "track_length"),
    ("SeriesID: ", "series_id"),
    ("SeasonID: ", "season_id"),
    ("SessionID: ", "session_id"),
    ("SubSessionID: ", "sub_session_id"),
];

/// The [`SCANNED`] values found in `yaml`, in the order their keys first
/// appear. A key that appears again keeps its place and takes the later
/// value, as assigning to a Python dict does.
fn scan_session_yaml(yaml: &str) -> Vec<(&'static str, String)> {
    let mut found: Vec<(&'static str, String)> = Vec::new();
    for line in yaml.lines() {
        let trimmed = line.trim();
        for &(prefix, key) in SCANNED {
            if let Some(rest) = trimmed.strip_prefix(prefix) {
                let value = rest.trim().to_string();
                match found.iter_mut().find(|(k, _)| *k == key) {
                    Some(entry) => entry.1 = value,
                    None => found.push((key, value)),
                }
                break;
            }
        }
    }
    found
}

impl IbtFile {
    /// The session metadata as ordered `(key, value)` pairs: the raw
    /// session-info YAML, the track and session fields scanned from it (the
    /// ones the file carries, in the order the YAML lists them), then the
    /// header counters.
    pub fn metadata_entries(&self) -> Vec<(&'static str, MetadataValue)> {
        use MetadataValue::{Float, Int, Str};

        let yaml = self.session_info_yaml();
        let mut entries = vec![("session_info_yaml", Str(yaml.to_string()))];
        entries.extend(
            scan_session_yaml(yaml)
                .into_iter()
                .map(|(key, value)| (key, Str(value))),
        );
        entries.push(("tick_rate", Int(self.tick_rate().into())));
        entries.push(("record_count", Int(self.record_count() as i64)));
        entries.push(("lap_count", Int(self.sub_header.lap_count.into())));
        entries.push((
            "session_start_date",
            Int(self.sub_header.session_start_date),
        ));
        entries.push(("start_time", Float(self.sub_header.start_time)));
        entries.push(("end_time", Float(self.sub_header.end_time)));
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scanned_keys_come_out_in_yaml_order() {
        let yaml = "---\nWeekendInfo:\n TrackName: spa\n TrackLength: 7.00 km\n \
                    TrackDisplayName: Spa\n SeriesID: 12\n";
        let found = scan_session_yaml(yaml);
        let keys: Vec<&str> = found.iter().map(|(key, _)| *key).collect();
        assert_eq!(
            keys,
            [
                "track_name",
                "track_length",
                "track_display_name",
                "series_id"
            ]
        );
        assert_eq!(found[1].1, "7.00 km");
    }

    #[test]
    fn a_repeated_key_keeps_its_place_and_takes_the_last_value() {
        let yaml = "SessionID: 1\nTrackName: a\nSessionID: 2\n";
        assert_eq!(
            scan_session_yaml(yaml),
            [
                ("session_id", "2".to_string()),
                ("track_name", "a".to_string())
            ]
        );
    }

    #[test]
    fn only_exact_keys_match() {
        let yaml = " TrackLengthOfficial: 4.41 km\n TrackNameShort: x\n MySessionID: 3\n";
        assert!(scan_session_yaml(yaml).is_empty());
    }
}
