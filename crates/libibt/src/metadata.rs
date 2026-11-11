//! The session metadata document: the keys every binding exposes, their
//! order and their value types.
//!
//! Bindings map [`MetadataValue`] onto their own types (a Python dict, a
//! JSON object) and add nothing of their own, so the layout is defined once.

use std::fmt;

use crate::error::Result;
use crate::reader::IbtFile;
use crate::session_yaml;

/// A calendar date (Python's `datetime.date`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date {
    pub year: u16,
    pub month: u8,
    pub day: u8,
}

/// A date and time of day (Python's `datetime.datetime`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateTime {
    pub date: Date,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub microsecond: u32,
    /// Offset from UTC in minutes; `None` for a time without a zone.
    pub utc_offset_minutes: Option<i32>,
}

/// ISO 8601, as Python's `date.isoformat()`.
impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

/// ISO 8601, as Python's `datetime.isoformat()`.
impl fmt::Display for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}T{:02}:{:02}:{:02}",
            self.date, self.hour, self.minute, self.second
        )?;
        if self.microsecond != 0 {
            write!(f, ".{:06}", self.microsecond)?;
        }
        if let Some(offset) = self.utc_offset_minutes {
            let sign = if offset < 0 { '-' } else { '+' };
            let offset = offset.unsigned_abs();
            write!(f, "{sign}{:02}:{:02}", offset / 60, offset % 60)?;
        }
        Ok(())
    }
}

impl DateTime {
    /// Microseconds since the proleptic Gregorian epoch, in UTC, for
    /// comparing two zoned times.
    fn utc_instant(&self) -> i64 {
        // Days from civil, Howard Hinnant's algorithm.
        let (y, m, d) = (
            i64::from(self.date.year),
            i64::from(self.date.month),
            i64::from(self.date.day),
        );
        let y = if m <= 2 { y - 1 } else { y };
        let era = y.div_euclid(400);
        let yoe = y.rem_euclid(400);
        let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
        let days = era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy;
        let seconds = days * 86_400
            + i64::from(self.hour) * 3600
            + i64::from(self.minute) * 60
            + i64::from(self.second)
            - i64::from(self.utc_offset_minutes.unwrap_or(0)) * 60;
        seconds * 1_000_000 + i64::from(self.microsecond)
    }
}

/// One value of the metadata document. The flat entries are strings and
/// numbers; `session_info`, `car_setup` and `sectors` are the parsed
/// session-info YAML and nest.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Date(Date),
    DateTime(DateTime),
    List(Vec<MetadataValue>),
    /// A mapping in document order. Keys are scalars, almost always
    /// strings, and unique in the sense of [`MetadataValue::py_eq`].
    Map(Vec<(MetadataValue, MetadataValue)>),
}

static NULL: MetadataValue = MetadataValue::Null;

impl MetadataValue {
    /// The value under the string key `key`, if this is a mapping that has
    /// it. `None` for anything that is not a mapping.
    pub fn get(&self, key: &str) -> Option<&MetadataValue> {
        match self {
            MetadataValue::Map(pairs) => pairs
                .iter()
                .find(|(k, _)| matches!(k, MetadataValue::Str(s) if s == key))
                .map(|(_, value)| value),
            _ => None,
        }
    }

    /// Python's `bool(value)`.
    pub fn is_truthy(&self) -> bool {
        match self {
            MetadataValue::Null => false,
            MetadataValue::Bool(v) => *v,
            MetadataValue::Int(v) => *v != 0,
            MetadataValue::Float(v) => *v != 0.0,
            MetadataValue::Str(v) => !v.is_empty(),
            MetadataValue::Date(_) | MetadataValue::DateTime(_) => true,
            MetadataValue::List(v) => !v.is_empty(),
            MetadataValue::Map(v) => !v.is_empty(),
        }
    }

    /// Python's `a == b`: numbers compare by value across bool, int and
    /// float (`1 == 1.0 == True`), mappings regardless of order.
    pub fn py_eq(&self, other: &MetadataValue) -> bool {
        use MetadataValue::{Bool, Date, DateTime, Float, Int, List, Map, Null, Str};
        match (self, other) {
            (Null, Null) => true,
            (Str(a), Str(b)) => a == b,
            (Date(a), Date(b)) => a == b,
            (DateTime(a), DateTime(b)) => {
                match (a.utc_offset_minutes, b.utc_offset_minutes) {
                    (None, None) => a == b,
                    (Some(_), Some(_)) => a.utc_instant() == b.utc_instant(),
                    _ => false, // a zoned and an unzoned time are never equal
                }
            }
            (List(a), List(b)) => a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.py_eq(y)),
            (Map(a), Map(b)) => {
                a.len() == b.len()
                    && a.iter().all(|(key, value)| {
                        b.iter()
                            .find(|(k, _)| k.py_eq(key))
                            .is_some_and(|(_, v)| v.py_eq(value))
                    })
            }
            (Bool(_) | Int(_) | Float(_), Bool(_) | Int(_) | Float(_)) => {
                match (self.as_number(), other.as_number()) {
                    (Number::Int(a), Number::Int(b)) => a == b,
                    (Number::Float(a), Number::Float(b)) => a == b,
                    (Number::Int(i), Number::Float(f)) | (Number::Float(f), Number::Int(i)) => {
                        // Exact, as Python compares an int with a float.
                        f.fract() == 0.0
                            && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0)
                                .contains(&f)
                            && f as i64 == i
                    }
                }
            }
            _ => false,
        }
    }

    fn as_number(&self) -> Number {
        match self {
            MetadataValue::Bool(v) => Number::Int(i64::from(*v)),
            MetadataValue::Int(v) => Number::Int(*v),
            MetadataValue::Float(v) => Number::Float(*v),
            _ => Number::Float(f64::NAN),
        }
    }

    /// How many values this one holds, itself included.
    pub(crate) fn node_count(&self) -> usize {
        match self {
            MetadataValue::List(items) => 1 + items.iter().map(Self::node_count).sum::<usize>(),
            MetadataValue::Map(pairs) => {
                1 + pairs
                    .iter()
                    .map(|(k, v)| k.node_count() + v.node_count())
                    .sum::<usize>()
            }
            _ => 1,
        }
    }
}

enum Number {
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

/// `mapping[key]`, or `Null` when the key is missing or `mapping` is not a
/// mapping (Python's `mapping.get(key)`).
fn field(mapping: Option<&MetadataValue>, key: &str) -> MetadataValue {
    mapping
        .and_then(|m| m.get(key))
        .cloned()
        .unwrap_or(NULL.clone())
}

/// The mappings in `mapping[key]`, when that is a list.
fn mappings<'a>(
    mapping: Option<&'a MetadataValue>,
    key: &str,
) -> impl Iterator<Item = &'a MetadataValue> {
    let items: &[MetadataValue] = match mapping.and_then(|m| m.get(key)) {
        Some(MetadataValue::List(items)) => items,
        _ => &[],
    };
    items
        .iter()
        .filter(|item| matches!(item, MetadataValue::Map(_)))
}

/// The entries derived from the parsed session info: the current session,
/// the driver who recorded the file, the car, the track and the weather.
///
/// A section that is missing, or is not the mapping or list it should be,
/// counts as empty: its entries are `Null` (or left out, for the session
/// and driver groups, which only appear when their record is found).
fn derived_entries(parsed: &MetadataValue, entries: &mut Vec<(&'static str, MetadataValue)>) {
    let weekend = parsed.get("WeekendInfo");
    let driver_info = parsed.get("DriverInfo");
    let session_info = parsed.get("SessionInfo");

    // Event and the session the recording was made in
    entries.push(("event_type", field(weekend, "EventType")));
    let current_num = field(session_info, "CurrentSessionNum");
    let current_session = mappings(session_info, "Sessions")
        .find(|s| s.get("SessionNum").unwrap_or(&NULL).py_eq(&current_num));
    if let Some(session) = current_session.filter(|s| s.is_truthy()) {
        entries.push(("session_type", field(Some(session), "SessionType")));
        entries.push(("session_name", field(Some(session), "SessionName")));
    }

    // Driver (the one who recorded the file)
    let driver_idx = field(driver_info, "DriverCarIdx");
    let driver = mappings(driver_info, "Drivers")
        .find(|d| d.get("CarIdx").unwrap_or(&NULL).py_eq(&driver_idx));
    if let Some(driver) = driver.filter(|d| d.is_truthy()) {
        let driver = Some(driver);
        entries.push(("driver_name", field(driver, "UserName")));
        entries.push(("driver_user_id", field(driver, "UserID")));
        entries.push(("driver_irating", field(driver, "IRating")));
        entries.push(("driver_license", field(driver, "LicString")));
        entries.push(("car_name", field(driver, "CarScreenName")));
        entries.push(("car_id", field(driver, "CarID")));
    }

    // Car specs
    entries.push((
        "car_gear_count",
        field(driver_info, "DriverCarGearNumForward"),
    ));
    entries.push(("car_redline_rpm", field(driver_info, "DriverCarRedLine")));
    entries.push(("car_shift_rpm", field(driver_info, "DriverCarSLShiftRPM")));
    entries.push(("car_idle_rpm", field(driver_info, "DriverCarIdleRPM")));

    // Track
    entries.push(("track_id", field(weekend, "TrackID")));
    entries.push(("track_type", field(weekend, "TrackType")));

    // Weather
    entries.push(("weather_temp", field(weekend, "TrackAirTemp")));
    entries.push(("weather_surface_temp", field(weekend, "TrackSurfaceTemp")));
    entries.push(("weather_humidity", field(weekend, "TrackRelativeHumidity")));
    entries.push(("weather_skies", field(weekend, "TrackSkies")));
    entries.push(("weather_wind_speed", field(weekend, "TrackWindVel")));
    entries.push(("weather_wind_dir", field(weekend, "TrackWindDir")));

    // Number of drivers, the pace car excluded
    let num_drivers = mappings(driver_info, "Drivers")
        .filter(|d| !d.get("CarIsPaceCar").is_some_and(MetadataValue::is_truthy))
        .count();
    entries.push(("num_drivers", MetadataValue::Int(num_drivers as i64)));

    entries.push(("car_setup", field(Some(parsed), "CarSetup")));
    entries.push(("sectors", field(parsed.get("SplitTimeInfo"), "Sectors")));
}

impl IbtFile {
    /// The session metadata as ordered `(key, value)` pairs:
    ///
    /// 1. the raw session-info YAML (`session_info_yaml`);
    /// 2. the track and session fields scanned from it (the ones the file
    ///    carries, in the order the YAML lists them);
    /// 3. the header counters (`tick_rate` .. `end_time`);
    /// 4. unless the YAML is empty, the parsed YAML (`session_info`) and the
    ///    entries derived from it (`event_type` .. `sectors`).
    ///
    /// Fails when the session-info YAML cannot be parsed.
    pub fn metadata_entries(&self) -> Result<Vec<(&'static str, MetadataValue)>> {
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

        if !yaml.is_empty() {
            let parsed = session_yaml::load(yaml)?;
            if parsed.is_truthy() {
                let mut derived = Vec::new();
                derived_entries(&parsed, &mut derived);
                entries.push(("session_info", parsed));
                entries.extend(derived);
            }
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use MetadataValue::{Bool, Float, Int, Null, Str};

    fn s(text: &str) -> MetadataValue {
        Str(text.to_string())
    }

    fn derived(yaml: &str) -> Vec<(&'static str, MetadataValue)> {
        let parsed = session_yaml::load(yaml).unwrap();
        let mut entries = Vec::new();
        derived_entries(&parsed, &mut entries);
        entries
    }

    fn value<'a>(
        entries: &'a [(&'static str, MetadataValue)],
        key: &str,
    ) -> Option<&'a MetadataValue> {
        entries.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
    }

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

    #[test]
    fn python_equality_and_truthiness() {
        assert!(Int(1).py_eq(&Float(1.0)) && Int(1).py_eq(&Bool(true)));
        assert!(Int(0).py_eq(&Bool(false)) && !Int(2).py_eq(&Bool(true)));
        assert!(!Int(1).py_eq(&s("1")) && !Null.py_eq(&Int(0)) && Null.py_eq(&Null));
        assert!(!Float(f64::NAN).py_eq(&Float(f64::NAN)));
        assert!(!Int(i64::MAX).py_eq(&Float(9_223_372_036_854_775_808.0)));
        let a = session_yaml::load("{x: 1, y: [2, 3]}").unwrap();
        let b = session_yaml::load("{y: [2.0, 3], x: yes}").unwrap();
        assert!(a.py_eq(&b));
        for falsy in [Null, Bool(false), Int(0), Float(0.0), s("")] {
            assert!(!falsy.is_truthy());
        }
        assert!(s("0").is_truthy() && Float(f64::NAN).is_truthy());
    }

    #[test]
    fn derived_entries_follow_the_current_session_and_the_recording_driver() {
        let entries = derived(
            "WeekendInfo:\n EventType: Race\n TrackID: 152\n TrackSkies: Dynamic\n\
             SessionInfo:\n CurrentSessionNum: 1\n Sessions:\n \
             - SessionNum: 0\n   SessionType: Practice\n   SessionName: PRACTICE\n \
             - SessionNum: 1\n   SessionType: Race\n   SessionName: RACE\n\
             DriverInfo:\n DriverCarIdx: 7\n DriverCarGearNumForward: 6\n Drivers:\n \
             - CarIdx: 0\n   UserName: Pace Car\n   CarIsPaceCar: 1\n \
             - CarIdx: 7\n   UserName: Driver Seven\n   UserID: 77\n   CarIsPaceCar: 0\n \
             - CarIdx: 9\n   UserName: Driver Nine\n\
             CarSetup:\n UpdateCount: 6\n\
             SplitTimeInfo:\n Sectors:\n - SectorNum: 0\n   SectorStartPct: 0.0\n",
        );
        let keys: Vec<&str> = entries.iter().map(|(key, _)| *key).collect();
        assert_eq!(
            keys,
            [
                "event_type",
                "session_type",
                "session_name",
                "driver_name",
                "driver_user_id",
                "driver_irating",
                "driver_license",
                "car_name",
                "car_id",
                "car_gear_count",
                "car_redline_rpm",
                "car_shift_rpm",
                "car_idle_rpm",
                "track_id",
                "track_type",
                "weather_temp",
                "weather_surface_temp",
                "weather_humidity",
                "weather_skies",
                "weather_wind_speed",
                "weather_wind_dir",
                "num_drivers",
                "car_setup",
                "sectors",
            ]
        );
        assert_eq!(value(&entries, "session_name"), Some(&s("RACE")));
        assert_eq!(value(&entries, "driver_name"), Some(&s("Driver Seven")));
        assert_eq!(value(&entries, "driver_user_id"), Some(&Int(77)));
        assert_eq!(value(&entries, "driver_irating"), Some(&Null));
        assert_eq!(value(&entries, "car_gear_count"), Some(&Int(6)));
        assert_eq!(value(&entries, "track_type"), Some(&Null));
        assert_eq!(value(&entries, "num_drivers"), Some(&Int(2)));
        assert!(matches!(
            value(&entries, "car_setup"),
            Some(MetadataValue::Map(_))
        ));
        assert!(matches!(
            value(&entries, "sectors"),
            Some(MetadataValue::List(_))
        ));
    }

    #[test]
    fn missing_or_misshapen_sections_count_as_empty() {
        let entries =
            derived("WeekendInfo: nothing\nDriverInfo:\n Drivers: none\nSessionInfo: [1, 2]\n");
        assert_eq!(value(&entries, "event_type"), Some(&Null));
        assert_eq!(value(&entries, "session_type"), None);
        assert_eq!(value(&entries, "driver_name"), None);
        assert_eq!(value(&entries, "num_drivers"), Some(&Int(0)));
        assert_eq!(value(&entries, "car_setup"), Some(&Null));
        assert_eq!(value(&entries, "sectors"), Some(&Null));
    }

    #[test]
    fn iso_formatting() {
        let date = Date {
            year: 2026,
            month: 2,
            day: 7,
        };
        assert_eq!(date.to_string(), "2026-02-07");
        let mut time = DateTime {
            date,
            hour: 8,
            minute: 5,
            second: 9,
            microsecond: 0,
            utc_offset_minutes: None,
        };
        assert_eq!(time.to_string(), "2026-02-07T08:05:09");
        time.microsecond = 500;
        time.utc_offset_minutes = Some(-330);
        assert_eq!(time.to_string(), "2026-02-07T08:05:09.000500-05:30");
        // The same instant in two zones is equal, as in Python.
        let utc = DateTime {
            hour: 13,
            minute: 35,
            utc_offset_minutes: Some(0),
            ..time
        };
        assert!(MetadataValue::DateTime(time).py_eq(&MetadataValue::DateTime(utc)));
    }
}
