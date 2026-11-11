//! Loads the session-info YAML the way PyYAML's `safe_load` does.
//!
//! The Python package used to parse this document with PyYAML, so the values
//! it produced (which strings become numbers, booleans, nulls or dates) are
//! what callers of `LogFile.metadata` have always seen. This module
//! reproduces them: `saphyr-parser` turns the text into events, and the
//! scalar rules below are PyYAML's YAML 1.1 resolver and constructors, not
//! the YAML 1.2 core schema. `yes`/`no`/`on`/`off` are booleans, `1:30` is
//! the integer 90, `1.0e5` is a string (PyYAML wants a signed exponent) and
//! `2026-02-27` is a date. `tests/test_session_yaml.py` checks all of it
//! against PyYAML itself.
//!
//! Known differences, none of which iRacing's output reaches (each is pinned
//! by `test_documented_differences`):
//! - explicit tags (`!!str`, `!custom`) are an error;
//! - an integer outside `i64` is an error (Python integers are unbounded);
//! - a structure that contains itself (an alias inside its own anchor) is an
//!   error, as is nesting deeper than [`MAX_DEPTH`] or a document that
//!   expands past [`MAX_NODES`] values;
//! - the scanner is YAML 1.2: NEL (U+0085) is not a line break, and a plain
//!   scalar may start with `:` inside a flow collection.

use std::collections::HashMap;

use saphyr_parser::{Event, Parser, ScalarStyle, StrInput};

use crate::error::{IbtError, Result};
use crate::metadata::{Date, DateTime, MetadataValue};

/// Nesting beyond this is rejected instead of risking the stack.
const MAX_DEPTH: usize = 128;
/// Total values a document may expand to. Aliases are expanded by copying,
/// so without a cap a few nested aliases could ask for gigabytes.
const MAX_NODES: usize = 1_000_000;

fn error<T>(message: impl Into<String>) -> Result<T> {
    Err(IbtError::Yaml(message.into()))
}

/// Load `text` as a single YAML document. An empty stream is `Null`.
pub fn load(text: &str) -> Result<MetadataValue> {
    check_printable(text)?;
    let mut loader = Loader {
        events: Parser::new_from_str(text),
        anchors: HashMap::new(),
        nodes: 0,
    };
    loader.document()
}

/// PyYAML refuses a stream holding characters outside YAML's printable set
/// before it parses anything.
fn check_printable(text: &str) -> Result<()> {
    let printable = |c: char| {
        matches!(c,
            '\t' | '\n' | '\r' | ' '..='~' | '\u{85}' | '\u{A0}'..='\u{D7FF}'
            | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..='\u{10FFFF}')
    };
    match text.chars().find(|&c| !printable(c)) {
        Some(c) => error(format!(
            "unacceptable character #x{:04x}: special characters are not allowed",
            c as u32
        )),
        None => Ok(()),
    }
}

struct Loader<'a> {
    events: Parser<'a, StrInput<'a>>,
    /// Finished values by anchor id, for aliases.
    anchors: HashMap<usize, MetadataValue>,
    nodes: usize,
}

impl<'a> Loader<'a> {
    fn next(&mut self) -> Result<Event<'a>> {
        match self.events.next() {
            Some(Ok((event, _span))) => Ok(event),
            Some(Err(e)) => error(e.to_string()),
            None => error("unexpected end of the YAML stream"),
        }
    }

    fn document(&mut self) -> Result<MetadataValue> {
        let mut value = None;
        loop {
            match self.next()? {
                Event::StreamStart | Event::DocumentEnd | Event::Nothing => {}
                Event::StreamEnd => return Ok(value.unwrap_or(MetadataValue::Null)),
                Event::DocumentStart(_) => {
                    if value.is_some() {
                        return error("expected a single document in the stream");
                    }
                    let first = self.next()?;
                    value = Some(self.node(first, 0)?);
                }
                other => return error(format!("unexpected YAML event {other:?}")),
            }
        }
    }

    fn count(&mut self, nodes: usize) -> Result<()> {
        self.nodes = self.nodes.saturating_add(nodes);
        if self.nodes > MAX_NODES {
            return error("the YAML document is too large");
        }
        Ok(())
    }

    fn anchored(&mut self, anchor: usize, value: MetadataValue) -> MetadataValue {
        if anchor != 0 {
            self.anchors.insert(anchor, value.clone());
        }
        value
    }

    fn node(&mut self, event: Event<'a>, depth: usize) -> Result<MetadataValue> {
        if depth > MAX_DEPTH {
            return error("the YAML document is nested too deeply");
        }
        match event {
            Event::Alias(anchor) => match self.anchors.get(&anchor) {
                Some(value) => {
                    let value = value.clone();
                    self.count(value.node_count())?;
                    Ok(value)
                }
                None => error("found an alias to an anchor that is not complete yet"),
            },
            Event::Scalar(text, style, anchor, tag) => {
                reject_tag(tag.is_some())?;
                self.count(1)?;
                let value = if style == ScalarStyle::Plain {
                    resolve_plain(&text)?
                } else {
                    MetadataValue::Str(text.into_owned())
                };
                Ok(self.anchored(anchor, value))
            }
            Event::SequenceStart(anchor, tag) => {
                reject_tag(tag.is_some())?;
                self.count(1)?;
                let mut items = Vec::new();
                loop {
                    match self.next()? {
                        Event::SequenceEnd => break,
                        item => items.push(self.node(item, depth + 1)?),
                    }
                }
                Ok(self.anchored(anchor, MetadataValue::List(items)))
            }
            Event::MappingStart(anchor, tag) => {
                reject_tag(tag.is_some())?;
                self.count(1)?;
                let pairs = self.mapping(depth)?;
                Ok(self.anchored(anchor, MetadataValue::Map(pairs)))
            }
            other => error(format!("unexpected YAML event {other:?}")),
        }
    }

    /// The pairs of a mapping, up to its end event. Keys behave as in a
    /// Python dict: a repeated key keeps its place and takes the later
    /// value. `<<` merge keys contribute their pairs ahead of the mapping's
    /// own, which therefore win.
    fn mapping(&mut self, depth: usize) -> Result<Vec<(MetadataValue, MetadataValue)>> {
        let mut merged = Vec::new();
        let mut own = Vec::new();
        loop {
            let key_event = self.next()?;
            if matches!(key_event, Event::MappingEnd) {
                break;
            }
            if is_plain(&key_event, "<<") {
                let value_event = self.next()?;
                match self.node(value_event, depth + 1)? {
                    MetadataValue::Map(pairs) => merged.extend(pairs),
                    MetadataValue::List(items) => {
                        // Earlier mappings in the list take precedence.
                        for item in items.into_iter().rev() {
                            match item {
                                MetadataValue::Map(pairs) => merged.extend(pairs),
                                _ => return error("expected a mapping for merging"),
                            }
                        }
                    }
                    _ => return error("expected a mapping or list of mappings for merging"),
                }
                continue;
            }
            // A plain `=` is YAML 1.1's "value" key; as a key it is a string.
            let key = if is_plain(&key_event, "=") {
                self.count(1)?;
                MetadataValue::Str("=".to_string())
            } else {
                self.node(key_event, depth + 1)?
            };
            if matches!(key, MetadataValue::List(_) | MetadataValue::Map(_)) {
                return error("found unhashable key");
            }
            let value_event = self.next()?;
            let value = self.node(value_event, depth + 1)?;
            own.push((key, value));
        }

        let mut pairs: Vec<(MetadataValue, MetadataValue)> = Vec::new();
        let mut string_keys: HashMap<String, usize> = HashMap::new();
        for (key, value) in merged.into_iter().chain(own) {
            let existing = match &key {
                MetadataValue::Str(s) => string_keys.get(s).copied(),
                other => pairs.iter().position(|(k, _)| k.py_eq(other)),
            };
            match existing {
                Some(index) => pairs[index].1 = value,
                None => {
                    if let MetadataValue::Str(s) = &key {
                        string_keys.insert(s.clone(), pairs.len());
                    }
                    pairs.push((key, value));
                }
            }
        }
        Ok(pairs)
    }
}

fn is_plain(event: &Event<'_>, text: &str) -> bool {
    matches!(event, Event::Scalar(value, ScalarStyle::Plain, _, None) if value == text)
}

fn reject_tag(tagged: bool) -> Result<()> {
    if tagged {
        return error("explicit YAML tags are not supported");
    }
    Ok(())
}

/// A plain (unquoted) scalar as PyYAML types it. Its implicit resolvers are
/// tried in PyYAML's order: bool, float, int, merge, null, timestamp, value.
fn resolve_plain(text: &str) -> Result<MetadataValue> {
    let bytes = text.as_bytes();
    if let Some(value) = resolve_bool(text) {
        return Ok(MetadataValue::Bool(value));
    }
    if is_float(bytes) {
        return Ok(MetadataValue::Float(construct_float(text)));
    }
    if is_int(bytes) {
        return construct_int(text).map(MetadataValue::Int);
    }
    if text == "<<" {
        return error("a merge key (<<) is only allowed as a mapping key");
    }
    if matches!(text, "" | "~" | "null" | "Null" | "NULL") {
        return Ok(MetadataValue::Null);
    }
    if let Some(timestamp) = match_timestamp(bytes) {
        return construct_timestamp(&timestamp);
    }
    if text == "=" {
        return error("a value key (=) is only allowed as a mapping key");
    }
    Ok(MetadataValue::Str(text.to_string()))
}

fn resolve_bool(text: &str) -> Option<bool> {
    match text {
        "yes" | "Yes" | "YES" | "true" | "True" | "TRUE" | "on" | "On" | "ON" => Some(true),
        "no" | "No" | "NO" | "false" | "False" | "FALSE" | "off" | "Off" | "OFF" => Some(false),
        _ => None,
    }
}

/// A cursor over a scalar's bytes, for transcribing PyYAML's regular
/// expressions. Every pattern below is deterministic (no alternative shares
/// a prefix that needs backtracking), so greedy matching is exact.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Cursor { bytes, at: 0 }
    }

    fn done(&self) -> bool {
        self.at == self.bytes.len()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    /// Consume one byte if it satisfies `accept`.
    fn one(&mut self, accept: impl Fn(u8) -> bool) -> bool {
        match self.peek() {
            Some(b) if accept(b) => {
                self.at += 1;
                true
            }
            _ => false,
        }
    }

    /// Consume every leading byte that satisfies `accept`; returns them.
    fn many(&mut self, accept: impl Fn(u8) -> bool) -> &'a [u8] {
        let start = self.at;
        while self.one(&accept) {}
        &self.bytes[start..self.at]
    }

    fn sign(&mut self) -> bool {
        self.one(|b| b == b'-' || b == b'+')
    }

    /// `(?::[0-5]?[0-9])+`: one or more base-60 groups.
    fn sexagesimal_groups(&mut self) -> bool {
        let mut groups = 0;
        while self.one(|b| b == b':') {
            let digits = self.many(|b| b.is_ascii_digit());
            let valid = match digits {
                [_] => true,
                [tens, _] => *tens <= b'5',
                _ => false,
            };
            if !valid {
                return false;
            }
            groups += 1;
        }
        groups > 0
    }

    /// `(?:[eE][-+][0-9]+)?` followed by the end of the scalar.
    fn optional_exponent_then_end(&mut self) -> bool {
        if self.done() {
            return true;
        }
        self.one(|b| b == b'e' || b == b'E')
            && self.sign()
            && !self.many(|b| b.is_ascii_digit()).is_empty()
            && self.done()
    }
}

fn digit_or_underscore(b: u8) -> bool {
    b.is_ascii_digit() || b == b'_'
}

/// PyYAML's float resolver:
/// `[-+]?[0-9][0-9_]*\.[0-9_]*(?:[eE][-+][0-9]+)?`,
/// `\.[0-9][0-9_]*(?:[eE][-+][0-9]+)?`,
/// `[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]*`,
/// `[-+]?\.(?:inf|Inf|INF)` or `\.(?:nan|NaN|NAN)`.
fn is_float(bytes: &[u8]) -> bool {
    if matches!(bytes, b".nan" | b".NaN" | b".NAN") {
        return true;
    }
    let unsigned = match bytes {
        [b'-' | b'+', rest @ ..] => rest,
        _ => bytes,
    };
    if matches!(unsigned, b".inf" | b".Inf" | b".INF") {
        return true;
    }

    // \.[0-9][0-9_]*(exponent)? -- no sign allowed
    let mut c = Cursor::new(bytes);
    if c.one(|b| b == b'.') {
        return c.one(|b| b.is_ascii_digit()) && {
            c.many(digit_or_underscore);
            c.optional_exponent_then_end()
        };
    }

    let mut c = Cursor::new(unsigned);
    if !c.one(|b| b.is_ascii_digit()) {
        return false;
    }
    c.many(digit_or_underscore);
    if c.peek() == Some(b':') {
        // base 60: groups, then a mandatory fraction part
        return c.sexagesimal_groups() && c.one(|b| b == b'.') && {
            c.many(digit_or_underscore);
            c.done()
        };
    }
    c.one(|b| b == b'.') && {
        c.many(digit_or_underscore);
        c.optional_exponent_then_end()
    }
}

/// PyYAML's `construct_yaml_float`.
fn construct_float(text: &str) -> f64 {
    let cleaned = text.replace('_', "").to_lowercase();
    let (sign, digits) = match cleaned.as_bytes().first() {
        Some(b'-') => (-1.0, &cleaned[1..]),
        Some(b'+') => (1.0, &cleaned[1..]),
        _ => (1.0, cleaned.as_str()),
    };
    if digits == ".inf" {
        return sign * f64::INFINITY;
    }
    if digits == ".nan" {
        return f64::NAN;
    }
    // The resolver only lets through text Rust's float parser accepts too.
    let parse = |part: &str| part.parse::<f64>().unwrap_or(f64::NAN);
    if digits.contains(':') {
        let mut value = 0.0;
        let mut base: u128 = 1;
        for part in digits.rsplit(':') {
            value += parse(part) * base as f64;
            base = base.saturating_mul(60);
        }
        return sign * value;
    }
    sign * parse(digits)
}

/// PyYAML's int resolver: `[-+]?0b[0-1_]+`, `[-+]?0[0-7_]+`,
/// `[-+]?(?:0|[1-9][0-9_]*)`, `[-+]?0x[0-9a-fA-F_]+` or
/// `[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+`.
fn is_int(bytes: &[u8]) -> bool {
    let unsigned = match bytes {
        [b'-' | b'+', rest @ ..] => rest,
        _ => bytes,
    };
    let all =
        |rest: &[u8], accept: fn(u8) -> bool| !rest.is_empty() && rest.iter().all(|&b| accept(b));
    match unsigned {
        [b'0'] => true,
        [b'0', b'b', rest @ ..] => all(rest, |b| matches!(b, b'0' | b'1' | b'_')),
        [b'0', b'x', rest @ ..] => all(rest, |b| b.is_ascii_hexdigit() || b == b'_'),
        [b'0', rest @ ..] => all(rest, |b| matches!(b, b'0'..=b'7' | b'_')),
        [b'1'..=b'9', ..] => {
            let mut c = Cursor::new(unsigned);
            c.many(digit_or_underscore);
            c.done() || (c.sexagesimal_groups() && c.done())
        }
        _ => false,
    }
}

/// PyYAML's `construct_yaml_int`.
fn construct_int(text: &str) -> Result<i64> {
    let cleaned = text.replace('_', "");
    let (negative, digits) = match cleaned.as_bytes().first() {
        Some(b'-') => (true, &cleaned[1..]),
        Some(b'+') => (false, &cleaned[1..]),
        _ => (false, cleaned.as_str()),
    };
    let out_of_range = || IbtError::Yaml(format!("integer {text} is out of range"));
    let radix = |digits: &str, radix: u32| {
        // Python raises ValueError here too, for `0b_` or `0x_`.
        i128::from_str_radix(digits, radix)
            .map_err(|_| IbtError::Yaml(format!("invalid integer {text}")))
    };
    let magnitude = if digits == "0" {
        0
    } else if let Some(binary) = digits.strip_prefix("0b") {
        radix(binary, 2)?
    } else if let Some(hex) = digits.strip_prefix("0x") {
        radix(hex, 16)?
    } else if digits.starts_with('0') {
        radix(digits, 8)?
    } else if digits.contains(':') {
        let mut value: i128 = 0;
        let mut base: i128 = 1;
        for part in digits.rsplit(':') {
            let digit = radix(part, 10)?;
            value = digit
                .checked_mul(base)
                .and_then(|term| value.checked_add(term))
                .ok_or_else(out_of_range)?;
            base = base.checked_mul(60).ok_or_else(out_of_range)?;
        }
        value
    } else {
        radix(digits, 10)?
    };
    let value = if negative { -magnitude } else { magnitude };
    i64::try_from(value).map_err(|_| out_of_range())
}

/// The pieces of a scalar that matched PyYAML's timestamp resolver.
struct Timestamp<'a> {
    year: &'a [u8],
    month: &'a [u8],
    day: &'a [u8],
    time: Option<TimeOfDay<'a>>,
}

struct TimeOfDay<'a> {
    hour: &'a [u8],
    minute: &'a [u8],
    second: &'a [u8],
    fraction: &'a [u8],
    zone: Zone<'a>,
}

enum Zone<'a> {
    None,
    Utc,
    Offset {
        negative: bool,
        hours: &'a [u8],
        minutes: &'a [u8],
    },
}

/// PyYAML's timestamp resolver: `YYYY-MM-DD`, or
/// `YYYY-M?M-D?D(?:[Tt]|[ \t]+)H?H:MM:SS(?:\.[0-9]*)?` with an optional
/// zone, `[ \t]*(?:Z|[-+]H?H(?::MM)?)`.
fn match_timestamp(bytes: &[u8]) -> Option<Timestamp<'_>> {
    let digit = |b: u8| b.is_ascii_digit();
    let blank = |b: u8| b == b' ' || b == b'\t';
    let mut c = Cursor::new(bytes);

    let year = c.many(digit);
    if year.len() != 4 || !c.one(|b| b == b'-') {
        return None;
    }
    let month = c.many(digit);
    if !matches!(month.len(), 1 | 2) || !c.one(|b| b == b'-') {
        return None;
    }
    let day = c.many(digit);
    if !matches!(day.len(), 1 | 2) {
        return None;
    }
    if c.done() {
        // The date-only form needs two-digit month and day.
        return (month.len() == 2 && day.len() == 2).then_some(Timestamp {
            year,
            month,
            day,
            time: None,
        });
    }

    if !c.one(|b| b == b'T' || b == b't') && c.many(blank).is_empty() {
        return None;
    }
    let hour = c.many(digit);
    if !matches!(hour.len(), 1 | 2) || !c.one(|b| b == b':') {
        return None;
    }
    let minute = c.many(digit);
    if minute.len() != 2 || !c.one(|b| b == b':') {
        return None;
    }
    let second = c.many(digit);
    if second.len() != 2 {
        return None;
    }
    let fraction = if c.one(|b| b == b'.') {
        c.many(digit)
    } else {
        &[]
    };

    let zone = if c.done() {
        Zone::None
    } else {
        c.many(blank);
        if c.one(|b| b == b'Z') {
            Zone::Utc
        } else {
            let negative = match c.peek() {
                Some(b'-') => true,
                Some(b'+') => false,
                _ => return None,
            };
            c.at += 1;
            let hours = c.many(digit);
            if !matches!(hours.len(), 1 | 2) {
                return None;
            }
            let minutes = if c.one(|b| b == b':') {
                let minutes = c.many(digit);
                if minutes.len() != 2 {
                    return None;
                }
                minutes
            } else {
                &[]
            };
            Zone::Offset {
                negative,
                hours,
                minutes,
            }
        }
    };
    c.done().then_some(Timestamp {
        year,
        month,
        day,
        time: Some(TimeOfDay {
            hour,
            minute,
            second,
            fraction,
            zone,
        }),
    })
}

/// At most six ASCII digits as a number.
fn number(digits: &[u8]) -> u32 {
    digits
        .iter()
        .fold(0, |value, &b| value * 10 + u32::from(b - b'0'))
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        2 => 28,
        _ => 31,
    }
}

/// PyYAML's `construct_yaml_timestamp`: a `datetime.date`, or a
/// `datetime.datetime` when there is a time. Out-of-range fields are the
/// `ValueError` Python's constructors raise.
fn construct_timestamp(timestamp: &Timestamp<'_>) -> Result<MetadataValue> {
    let (year, month, day) = (
        number(timestamp.year),
        number(timestamp.month),
        number(timestamp.day),
    );
    if year < 1 {
        return error(format!("year {year} is out of range"));
    }
    if !(1..=12).contains(&month) {
        return error("month must be in 1..12");
    }
    if day < 1 || day > days_in_month(year, month) {
        return error("day is out of range for month");
    }
    let date = Date {
        year: year as u16,
        month: month as u8,
        day: day as u8,
    };
    let Some(time) = &timestamp.time else {
        return Ok(MetadataValue::Date(date));
    };

    let (hour, minute, second) = (number(time.hour), number(time.minute), number(time.second));
    if hour > 23 {
        return error("hour must be in 0..23");
    }
    if minute > 59 {
        return error("minute must be in 0..59");
    }
    if second > 59 {
        return error("second must be in 0..59");
    }
    // The first six fraction digits, right-padded: microseconds.
    let mut microsecond = number(&time.fraction[..time.fraction.len().min(6)]);
    for _ in time.fraction.len()..6 {
        microsecond *= 10;
    }
    let utc_offset_minutes = match time.zone {
        Zone::None => None,
        Zone::Utc => Some(0),
        Zone::Offset {
            negative,
            hours,
            minutes,
        } => {
            let total = (number(hours) * 60 + number(minutes)) as i32;
            if total >= 24 * 60 {
                return error("the UTC offset must be strictly between -24 and 24 hours");
            }
            Some(if negative { -total } else { total })
        }
    };
    Ok(MetadataValue::DateTime(DateTime {
        date,
        hour: hour as u8,
        minute: minute as u8,
        second: second as u8,
        microsecond,
        utc_offset_minutes,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use MetadataValue::{Bool, Float, Int, List, Map, Null, Str};

    fn s(text: &str) -> MetadataValue {
        Str(text.to_string())
    }

    fn scalar(text: &str) -> MetadataValue {
        resolve_plain(text).unwrap()
    }

    /// A date or time scalar in ISO 8601.
    fn iso(text: &str) -> String {
        match scalar(text) {
            MetadataValue::Date(date) => date.to_string(),
            MetadataValue::DateTime(time) => time.to_string(),
            other => panic!("{text} resolved to {other:?}"),
        }
    }

    #[test]
    fn booleans_and_nulls_follow_yaml_1_1() {
        for text in [
            "yes", "Yes", "YES", "true", "True", "TRUE", "on", "On", "ON",
        ] {
            assert_eq!(scalar(text), Bool(true), "{text}");
        }
        for text in [
            "no", "No", "NO", "false", "False", "FALSE", "off", "Off", "OFF",
        ] {
            assert_eq!(scalar(text), Bool(false), "{text}");
        }
        for text in ["", "~", "null", "Null", "NULL"] {
            assert_eq!(scalar(text), Null, "{text}");
        }
        // Not in PyYAML's lists: single letters and odd capitalisation.
        for text in ["y", "n", "Y", "N", "tRue", "oN", "nUll", "None", "nil"] {
            assert_eq!(scalar(text), s(text), "{text}");
        }
    }

    #[test]
    fn integers() {
        assert_eq!(scalar("0"), Int(0));
        assert_eq!(scalar("-0"), Int(0));
        assert_eq!(scalar("152"), Int(152));
        assert_eq!(scalar("+7"), Int(7));
        assert_eq!(scalar("-12_345"), Int(-12345));
        assert_eq!(scalar("0b1010"), Int(10));
        assert_eq!(scalar("-0x1F"), Int(-31));
        assert_eq!(scalar("0o17"), s("0o17")); // YAML 1.2 octal is a string
        assert_eq!(scalar("017"), Int(15)); // YAML 1.1 octal
        assert_eq!(scalar("0_"), Int(0));
        assert_eq!(scalar("08"), s("08"));
        assert_eq!(scalar("1:30"), Int(90)); // base 60
        assert_eq!(scalar("-1:00:00"), Int(-3600));
        assert_eq!(scalar("1:60"), s("1:60"));
        assert_eq!(scalar("0:30"), s("0:30")); // base 60 cannot start with 0
        assert_eq!(scalar("9223372036854775807"), Int(i64::MAX));
        assert_eq!(scalar("-9223372036854775808"), Int(i64::MIN));
        assert!(resolve_plain("9223372036854775808").is_err());
        assert!(resolve_plain("0b_").is_err());
    }

    #[test]
    fn floats() {
        assert_eq!(scalar("4.4069"), Float(4.4069));
        assert_eq!(scalar("-1."), Float(-1.0));
        assert_eq!(scalar(".5"), Float(0.5));
        assert_eq!(scalar("-.5"), s("-.5")); // PyYAML: no sign on the bare-dot form
        assert_eq!(scalar("1_000.5"), Float(1000.5));
        assert_eq!(scalar("1.5e+3"), Float(1500.0));
        assert_eq!(scalar("1.5e3"), s("1.5e3")); // the exponent needs a sign
        assert_eq!(scalar("1e5"), s("1e5")); // and a dot
        assert_eq!(scalar("1:30.5"), Float(90.5));
        assert_eq!(scalar("0:30.5"), Float(30.5));
        assert_eq!(scalar(".inf"), Float(f64::INFINITY));
        assert_eq!(scalar("-.Inf"), Float(f64::NEG_INFINITY));
        assert!(matches!(scalar(".NaN"), Float(v) if v.is_nan()));
        assert_eq!(scalar("+.nan"), s("+.nan"));
        assert!(matches!(scalar("-0.0"), Float(v) if v == 0.0 && v.is_sign_negative()));
        assert_eq!(scalar("4.4069 km"), s("4.4069 km"));
        assert_eq!(scalar("1.2.3"), s("1.2.3"));
    }

    #[test]
    fn timestamps() {
        assert_eq!(
            scalar("2026-02-27"),
            MetadataValue::Date(Date {
                year: 2026,
                month: 2,
                day: 27
            })
        );
        assert_eq!(scalar("2026-2-27"), s("2026-2-27"));
        assert!(resolve_plain("2026-02-30").is_err());
        assert!(resolve_plain("0000-01-01").is_err());
        assert_eq!(iso("2024-02-29"), "2024-02-29");
        assert_eq!(iso("2026-2-7 8:05:09.5"), "2026-02-07T08:05:09.500000");
        assert_eq!(iso("2026-02-27t20:40:38Z"), "2026-02-27T20:40:38+00:00");
        assert_eq!(
            iso("2026-02-27T20:40:38.1234567 -5:30"),
            "2026-02-27T20:40:38.123456-05:30"
        );
        assert_eq!(scalar("2026-02-27T20:40"), s("2026-02-27T20:40"));
        assert!(resolve_plain("2026-02-27T24:00:00").is_err());
        assert!(resolve_plain("2026-02-27T00:00:00+24:00").is_err());
    }

    #[test]
    fn only_plain_scalars_are_typed() {
        let doc = "a: 12\nb: \"12\"\nc: '12'\nd: |\n  12\ne: yes\nf: \"yes\"\n";
        assert_eq!(
            load(doc).unwrap(),
            Map(vec![
                (s("a"), Int(12)),
                (s("b"), s("12")),
                (s("c"), s("12")),
                (s("d"), s("12\n")),
                (s("e"), Bool(true)),
                (s("f"), s("yes")),
            ])
        );
    }

    #[test]
    fn structure() {
        let doc = "---\nWeekendInfo:\n TrackName: spa\n TrackID: 152\n Empty:\nDrivers:\n - CarIdx: 0\n   UserName: A B\n - CarIdx: 1\n";
        assert_eq!(
            load(doc).unwrap(),
            Map(vec![
                (
                    s("WeekendInfo"),
                    Map(vec![
                        (s("TrackName"), s("spa")),
                        (s("TrackID"), Int(152)),
                        (s("Empty"), Null),
                    ])
                ),
                (
                    s("Drivers"),
                    List(vec![
                        Map(vec![(s("CarIdx"), Int(0)), (s("UserName"), s("A B"))]),
                        Map(vec![(s("CarIdx"), Int(1))]),
                    ])
                ),
            ])
        );
        assert_eq!(load("").unwrap(), Null);
        assert_eq!(load("---\n").unwrap(), Null);
        assert_eq!(
            load("[1, two, 3.0]").unwrap(),
            List(vec![Int(1), s("two"), Float(3.0)])
        );
    }

    #[test]
    fn repeated_and_typed_keys_behave_like_a_python_dict() {
        // Later value, first position; 1, 1.0 and true are one key.
        assert_eq!(
            load("a: 1\nb: 2\na: 3\n").unwrap(),
            Map(vec![(s("a"), Int(3)), (s("b"), Int(2))])
        );
        assert_eq!(
            load("1: a\n1.0: b\nyes: c\n\"1\": d\n").unwrap(),
            Map(vec![(Int(1), s("c")), (s("1"), s("d"))])
        );
        assert_eq!(load("=: 1\n").unwrap(), Map(vec![(s("="), Int(1))]));
        assert!(load("[1, 2]: a\n").is_err());
    }

    #[test]
    fn anchors_aliases_and_merge_keys() {
        let doc = "base: &b {x: 1, y: 2}\nuse: *b\nmerged:\n  <<: *b\n  y: 3\n  z: 4\n";
        let base = Map(vec![(s("x"), Int(1)), (s("y"), Int(2))]);
        assert_eq!(
            load(doc).unwrap(),
            Map(vec![
                (s("base"), base.clone()),
                (s("use"), base),
                (
                    s("merged"),
                    Map(vec![(s("x"), Int(1)), (s("y"), Int(3)), (s("z"), Int(4))])
                ),
            ])
        );
        // In a list of mappings the earlier one wins.
        assert_eq!(
            load("<<: [{a: 1}, {a: 2, b: 3}]\n").unwrap(),
            Map(vec![(s("a"), Int(1)), (s("b"), Int(3))])
        );
        assert!(load("a: <<\n").is_err());
        assert!(load("<<: 1\n").is_err());
    }

    #[test]
    fn what_pyyaml_rejects_is_rejected() {
        assert!(load("a: 1\n---\nb: 2\n").is_err()); // two documents
        assert!(load("a: [1, 2\n").is_err());
        assert!(load("a: b: c\n").is_err());
        assert!(load("a: \u{7}\n").is_err()); // control character
        assert!(load("a: !!str 1\n").is_err()); // tags: a documented difference
        assert!(load("a: =\n").is_err());
    }

    #[test]
    fn alias_expansion_is_bounded() {
        let mut doc = String::from("a0: &a0 [x, x, x, x, x, x, x, x, x, x]\n");
        for i in 1..12 {
            let p = i - 1;
            doc.push_str(&format!(
                "a{i}: &a{i} [*a{p}, *a{p}, *a{p}, *a{p}, *a{p}, *a{p}, *a{p}, *a{p}, *a{p}, *a{p}]\n"
            ));
        }
        assert!(load(&doc).is_err());
    }
}
