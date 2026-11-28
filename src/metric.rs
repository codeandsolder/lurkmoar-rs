use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use crate::{Client, Error, lock};

pub(crate) type Labels = Vec<(String, String)>;

/// Validated and escaped metric selector. Handles share it across submissions.
#[derive(Debug)]
pub(crate) struct Series(Box<str>);

impl Series {
    pub(crate) fn new<I, K, V>(
        name: &str,
        labels: I,
        static_labels: &[(String, String)],
    ) -> Result<Self, Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        validate_metric_name(name)?;
        let labels = labels
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<Labels>();
        validate_labels(&labels)?;
        let mut selector = name.to_owned();
        let mut separator = '{';
        for (key, value) in static_labels {
            let value = labels
                .iter()
                .find(|(candidate, _)| candidate == key)
                .map_or(value, |(_, value)| value);
            append_label(&mut selector, &mut separator, key, value);
        }
        for (key, value) in &labels {
            if !static_labels.iter().any(|(candidate, _)| candidate == key) {
                append_label(&mut selector, &mut separator, key, value);
            }
        }
        finish_labels(&mut selector, separator);
        Ok(Self(selector.into_boxed_str()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct Gauge {
    client: Client,
    series: Arc<Series>,
    last_bits: Arc<Mutex<Option<u64>>>,
}

impl Gauge {
    pub(crate) fn new<I, K, V>(client: Client, name: &str, labels: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let series = client.series(name, labels)?;
        Ok(Self {
            client,
            series,
            last_bits: Arc::new(Mutex::new(None)),
        })
    }

    /// Publish a new gauge value if it differs bit-for-bit from the last value.
    ///
    /// # Errors
    /// Returns an error if state is poisoned, the delivery worker is unavailable,
    /// or the backlog is full. The gauge is unchanged on admission failure.
    pub fn set(&self, value: f64) -> Result<bool, Error> {
        let bits = value.to_bits();
        let mut last = lock(&self.last_bits)?;
        if *last == Some(bits) {
            return Ok(false);
        }
        self.client.submit_series(Arc::clone(&self.series), value)?;
        *last = Some(bits);
        drop(last);
        Ok(true)
    }
}

#[derive(Debug, Clone)]
pub struct Counter {
    client: Client,
    series: Arc<Series>,
    value: Arc<Mutex<f64>>,
}

impl Counter {
    pub(crate) fn new<I, K, V>(client: Client, name: &str, labels: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let series = client.series(name, labels)?;
        Ok(Self {
            client,
            series,
            value: Arc::new(Mutex::new(0.0)),
        })
    }

    /// Increment the counter by one and publish the new absolute value.
    ///
    /// # Errors
    /// Returns an error if the resulting value overflows, state is poisoned,
    /// the delivery worker is unavailable, or the backlog is full.
    pub fn inc(&self) -> Result<f64, Error> {
        self.add(1.0)
    }

    /// Add a non-negative finite delta and publish the new absolute value.
    ///
    /// # Errors
    /// Returns an error for an invalid delta, non-finite resulting value, or unavailable state/delivery worker. The counter is unchanged on error.
    pub fn add(&self, delta: f64) -> Result<f64, Error> {
        if !delta.is_finite() || delta < 0.0 {
            return Err(Error::InvalidMetric(
                "counter delta must be finite and non-negative".into(),
            ));
        }
        let mut current = lock(&self.value)?;
        let next = *current + delta;
        if !next.is_finite() {
            return Err(Error::InvalidMetric("counter value would overflow".into()));
        }
        // Admission and state commit share the same critical section; failed
        // admission cannot advance state and concurrent values remain ordered.
        self.client.submit_series(Arc::clone(&self.series), next)?;
        *current = next;
        drop(current);
        Ok(next)
    }

    /// Return the current process-local counter value.
    ///
    /// # Errors
    /// Returns an error if the counter state mutex is poisoned.
    pub fn get(&self) -> Result<f64, Error> {
        Ok(*lock(&self.value)?)
    }
}

pub(crate) fn validate_labels(labels: &[(String, String)]) -> Result<(), Error> {
    for (index, (key, _)) in labels.iter().enumerate() {
        validate_label_name(key)?;
        if labels[..index].iter().any(|(previous, _)| previous == key) {
            return Err(Error::InvalidMetric(format!(
                "duplicate label name {key:?}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_metric_name(name: &str) -> Result<(), Error> {
    validate_identifier(name, true)
        .then_some(())
        .ok_or_else(|| Error::InvalidMetric(format!("invalid Prometheus metric name {name:?}")))
}

fn validate_label_name(name: &str) -> Result<(), Error> {
    (validate_identifier(name, false) && name != "__name__")
        .then_some(())
        .ok_or_else(|| Error::InvalidMetric(format!("invalid Prometheus label name {name:?}")))
}

fn validate_identifier(value: &str, allow_colon: bool) -> bool {
    let mut chars = value.bytes();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == b'_' || first.is_ascii_alphabetic() || (allow_colon && first == b':')) {
        return false;
    }
    chars.all(|ch| ch == b'_' || ch.is_ascii_alphanumeric() || (allow_colon && ch == b':'))
}

fn append_label(out: &mut String, separator: &mut char, key: &str, value: &str) {
    out.push(*separator);
    *separator = ',';
    out.push_str(key);
    out.push_str("=\"");
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '"' => out.push_str("\\\""),
            ch => out.push(ch),
        }
    }
    out.push('"');
}

fn finish_labels(out: &mut String, separator: char) {
    if separator == ',' {
        out.push('}');
    }
}

fn trim_horizontal(value: &str) -> &str {
    value.trim_start_matches([' ', '\t'])
}

fn invalid_sample() -> Error {
    Error::InvalidMetric("invalid untimestamped Prometheus sample".into())
}

/// Parse a legacy Prometheus label value, returning its already escaped content.
fn quoted_value(value: &str) -> Result<(&str, &str), Error> {
    let value = value.strip_prefix('"').ok_or_else(invalid_sample)?;
    let mut escaped = false;
    for (index, byte) in value.bytes().enumerate() {
        if escaped {
            if !matches!(byte, b'\\' | b'"' | b'n') {
                return Err(invalid_sample());
            }
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            return Ok((&value[..index], &value[index + 1..]));
        }
    }
    Err(invalid_sample())
}

fn parse_text_labels(mut input: &str) -> Result<(Vec<(&str, &str)>, &str), Error> {
    let mut labels = Vec::new();
    loop {
        input = trim_horizontal(input);
        if let Some(rest) = input.strip_prefix('}') {
            return Ok((labels, rest));
        }
        let end = input.find(['=', ' ', '\t']).ok_or_else(invalid_sample)?;
        let (key, rest) = input.split_at(end);
        validate_label_name(key)?;
        if labels.iter().any(|(previous, _)| *previous == key) {
            return Err(Error::InvalidMetric(format!(
                "duplicate label name {key:?}"
            )));
        }
        let rest = trim_horizontal(rest)
            .strip_prefix('=')
            .ok_or_else(invalid_sample)?;
        let (value, rest) = quoted_value(trim_horizontal(rest))?;
        labels.push((key, value));
        let rest = trim_horizontal(rest);
        if let Some(rest) = rest.strip_prefix('}') {
            return Ok((labels, rest));
        }
        input = rest.strip_prefix(',').ok_or_else(invalid_sample)?;
    }
}

fn render_text_line(
    line: &str,
    static_labels: &[(String, String)],
    timestamp_ms: u64,
    out: &mut String,
) -> Result<(), Error> {
    let name_end = line.find(['{', ' ', '\t']).ok_or_else(invalid_sample)?;
    let (name, rest) = line.split_at(name_end);
    validate_metric_name(name)?;
    let (labels, rest) = if let Some(rest) = trim_horizontal(rest).strip_prefix('{') {
        parse_text_labels(rest)?
    } else {
        (Vec::new(), rest)
    };
    if !rest.starts_with([' ', '\t']) {
        return Err(invalid_sample());
    }
    let value = rest.trim_matches([' ', '\t']);
    let numeric = match value {
        "NaN" => f64::NAN,
        "+Inf" | "Inf" => f64::INFINITY,
        "-Inf" => f64::NEG_INFINITY,
        _ => {
            let number = value.parse::<f64>().map_err(|_| invalid_sample())?;
            if !number.is_finite() {
                return Err(invalid_sample());
            }
            number
        }
    };
    out.push_str(name);
    let mut separator = '{';
    for (key, value) in &labels {
        out.push(separator);
        separator = ',';
        out.push_str(key);
        out.push_str("=\"");
        out.push_str(value);
        out.push('"');
    }
    for (key, value) in static_labels {
        if !labels.iter().any(|(candidate, _)| *candidate == key) {
            append_label(out, &mut separator, key, value);
        }
    }
    finish_labels(out, separator);
    out.push(' ');
    if numeric.is_finite() {
        out.push_str(value);
    } else {
        append_value(out, numeric);
    }
    // Formatting into a String is infallible.
    let _ = writeln!(out, " {timestamp_ms}");
    Ok(())
}

/// Validate and render the entire snapshot before admission; never silently
/// drop individual malformed lines or attach duplicate static labels.
pub(crate) fn render_snapshot(
    text: &str,
    static_labels: &[(String, String)],
    timestamp_ms: u64,
) -> Result<(Box<str>, u64), Error> {
    let mut rendered = String::with_capacity(text.len());
    let mut samples = 0;
    for line in text.lines() {
        let line = trim_horizontal(line);
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        render_text_line(line, static_labels, timestamp_ms, &mut rendered)?;
        samples += 1;
    }
    Ok((rendered.into_boxed_str(), samples))
}

fn append_value(out: &mut String, value: f64) {
    if value.is_nan() {
        out.push_str("NaN");
    } else if value == f64::INFINITY {
        out.push_str("+Inf");
    } else if value == f64::NEG_INFINITY {
        out.push_str("-Inf");
    } else {
        let _ = write!(out, "{value}");
    }
}

pub(crate) fn render_sample(out: &mut String, series: &Series, value: f64, timestamp_ms: u64) {
    out.push_str(series.as_str());
    out.push(' ');
    append_value(out, value);
    let _ = writeln!(out, " {timestamp_ms}");
}

#[cfg(test)]
mod tests {
    use super::{Counter, validate_label_name, validate_metric_name};
    use crate::{
        Client,
        worker::{Command, Payload},
    };

    #[test]
    fn counter_updates_are_ordered_and_failed_updates_do_not_commit()
    -> Result<(), Box<dyn std::error::Error>> {
        let (client, mut receiver) = Client::channel(Vec::new(), 512, 1024 * 1024)?;
        let counter = Counter::new(client, "counter", std::iter::empty::<(&str, &str)>())?;
        std::thread::scope(|scope| -> Result<(), crate::Error> {
            let mut handles = Vec::new();
            for _ in 0..4 {
                handles.push(scope.spawn(|| {
                    for _ in 0..64 {
                        let _ = counter.inc()?;
                    }
                    Ok::<_, crate::Error>(())
                }));
            }
            for handle in handles {
                handle
                    .join()
                    .map_err(|_| crate::Error::State("counter thread panicked".into()))??;
            }
            Ok(())
        })?;
        // Observe the real worker queue, independently of the lock state.
        for expected in 1..=256 {
            let Command::Payload(crate::worker::QueuedPayload {
                payload: Payload::Sample { value, .. },
                ..
            }) = receiver.try_recv()?
            else {
                return Err("expected one sample per increment".into());
            };
            assert_eq!(value.to_bits(), f64::from(expected).to_bits());
        }
        assert!(receiver.try_recv().is_err());
        assert_eq!(counter.get()?.to_bits(), 256.0_f64.to_bits());

        drop(receiver);
        assert!(counter.inc().is_err());
        assert_eq!(counter.get()?.to_bits(), 256.0_f64.to_bits());
        Ok(())
    }

    #[test]
    fn counter_rejects_arithmetic_overflow_without_publishing()
    -> Result<(), Box<dyn std::error::Error>> {
        let (client, mut receiver) = Client::channel(Vec::new(), 512, 1024 * 1024)?;
        let counter = Counter::new(client, "counter", std::iter::empty::<(&str, &str)>())?;
        assert_eq!(counter.add(f64::MAX)?.to_bits(), f64::MAX.to_bits());
        assert!(counter.add(f64::MAX).is_err());
        assert_eq!(counter.get()?.to_bits(), f64::MAX.to_bits());
        let Command::Payload(crate::worker::QueuedPayload {
            payload: Payload::Sample { value, .. },
            ..
        }) = receiver.try_recv()?
        else {
            return Err("expected a sample".into());
        };
        assert_eq!(value.to_bits(), f64::MAX.to_bits());
        assert!(receiver.try_recv().is_err());
        Ok(())
    }

    #[test]
    fn prometheus_identifiers_are_validated() {
        assert!(validate_metric_name("http_requests_total").is_ok());
        assert!(validate_metric_name(":synthetic").is_ok());
        assert!(validate_metric_name("9bad").is_err());
        assert!(validate_label_name("instance").is_ok());
        assert!(validate_label_name(":bad").is_err());
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::{Series, render_snapshot};
    use crate::{Client, Error};

    #[test]
    fn validates_and_stamps_entire_snapshot() -> Result<(), Error> {
        let labels = [
            ("instance".into(), "default".into()),
            ("region".into(), "eu".into()),
        ];
        let input = r#"  # HELP demo help
 demo{ instance = "a } b", escaped="quote\"slash\\newline\n", } 1.25e-3
 demo 2
 demo{} +Inf
 demo -Inf
 demo NaN
 "#;
        let (actual, count) = render_snapshot(input, &labels, 123)?;
        let expected = concat!(
            "demo{instance=\"a } b\",escaped=\"quote\\\"slash\\\\newline\\n\",region=\"eu\"} 1.25e-3 123\n",
            "demo{instance=\"default\",region=\"eu\"} 2 123\n",
            "demo{instance=\"default\",region=\"eu\"} +Inf 123\n",
            "demo{instance=\"default\",region=\"eu\"} -Inf 123\n",
            "demo{instance=\"default\",region=\"eu\"} NaN 123\n",
        );
        assert_eq!(&*actual, expected);
        assert_eq!(count, 5);
        assert_eq!(render_snapshot(" # comment\n\t ", &[], 0)?.1, 0);
        Ok(())
    }

    #[test]
    fn rejects_invalid_snapshots_and_duplicate_labels() {
        for input in [
            "ok 1\nbad",
            "x 1 123",
            "x{a=\"b\",a=\"c\"} 1",
            "x{__name__=\"y\"} 1",
            "x{a=\"unterminated} 1",
            "x{a=\"bad\\q\"} 1",
            "x 1e9999",
            "x nope",
        ] {
            assert!(
                render_snapshot(input, &[], 0).is_err(),
                "accepted {input:?}"
            );
        }
        assert!(Series::new("x", [("a", "b"), ("a", "c")], &[]).is_err());
    }

    #[test]
    fn dequeued_payload_still_holds_admission_and_handle_state_rolls_back()
    -> Result<(), Box<dyn std::error::Error>> {
        let (client, mut receiver) = Client::channel(Vec::new(), 1, 4096)?;
        let gauge = client.gauge("gauge", std::iter::empty::<(&str, &str)>())?;
        let counter = client.counter("counter", std::iter::empty::<(&str, &str)>())?;
        assert!(gauge.set(1.0)?);
        let outstanding = receiver.try_recv()?;
        assert!(matches!(gauge.set(2.0), Err(Error::Backpressure)));
        assert!(matches!(counter.inc(), Err(Error::Backpressure)));
        assert_eq!(counter.get()?.to_bits(), 0.0_f64.to_bits());
        assert_eq!(client.health().outstanding_payloads, 1);
        drop(outstanding);
        assert!(gauge.set(2.0)?);
        drop(receiver.try_recv()?);
        assert_eq!(counter.inc()?.to_bits(), 1.0_f64.to_bits());
        assert_eq!(client.health().rejected_payloads_total, 2);
        Ok(())
    }
}
