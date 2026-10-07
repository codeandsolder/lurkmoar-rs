use std::sync::Mutex;

use crate::{Client, Error, lock};

#[derive(Debug, Clone)]
pub struct Gauge {
    client: Client,
    name: String,
    labels: Vec<(String, String)>,
    last_bits: std::sync::Arc<Mutex<Option<u64>>>,
}

impl Gauge {
    pub(crate) fn new<I, K, V>(client: Client, name: &str, labels: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        validate_metric_name(name)?;
        let labels = labels
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<Vec<_>>();
        for (key, _) in &labels {
            validate_label_name(key)?;
        }
        Ok(Self {
            client,
            name: name.to_owned(),
            labels,
            last_bits: std::sync::Arc::new(Mutex::new(None)),
        })
    }

    /// Publish a new gauge value if it differs bit-for-bit from the last value.
    ///
    /// # Errors
    /// Returns an error if the delivery worker is unavailable.
    pub fn set(&self, value: f64) -> Result<bool, Error> {
        let bits = value.to_bits();
        let mut last = lock(&self.last_bits)?;
        if last.is_some_and(|previous| previous == bits) {
            return Ok(false);
        }
        self.client.sample(&self.name, value, self.labels.clone())?;
        *last = Some(bits);
        drop(last);
        Ok(true)
    }
}

#[derive(Debug, Clone)]
pub struct Counter {
    client: Client,
    name: String,
    labels: Vec<(String, String)>,
    value: std::sync::Arc<Mutex<f64>>,
}

impl Counter {
    pub(crate) fn new<I, K, V>(client: Client, name: &str, labels: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        validate_metric_name(name)?;
        let labels = labels
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<Vec<_>>();
        for (key, _) in &labels {
            validate_label_name(key)?;
        }
        Ok(Self {
            client,
            name: name.to_owned(),
            labels,
            value: std::sync::Arc::new(Mutex::new(0.0)),
        })
    }

    /// Increment the counter by one and publish the new absolute value.
    ///
    /// # Errors
    /// Returns an error if the counter state or delivery worker is unavailable.
    pub fn inc(&self) -> Result<f64, Error> {
        self.add(1.0)
    }

    /// Add a non-negative finite delta and publish the new absolute value.
    ///
    /// # Errors
    /// Returns an error for an invalid delta or unavailable state/delivery worker.
    pub fn add(&self, delta: f64) -> Result<f64, Error> {
        if !delta.is_finite() || delta < 0.0 {
            return Err(Error::InvalidMetric(
                "counter delta must be finite and non-negative".into(),
            ));
        }
        let value = {
            let mut current = lock(&self.value)?;
            *current += delta;
            *current
        };
        self.client.sample(&self.name, value, self.labels.clone())?;
        Ok(value)
    }

    /// Return the current process-local counter value.
    ///
    /// # Errors
    /// Returns an error if the counter state mutex is poisoned.
    pub fn get(&self) -> Result<f64, Error> {
        Ok(*lock(&self.value)?)
    }
}

pub fn validate_metric_name(name: &str) -> Result<(), Error> {
    validate_identifier(name, true)
        .then_some(())
        .ok_or_else(|| Error::InvalidMetric(format!("invalid Prometheus metric name {name:?}")))
}

pub fn validate_label_name(name: &str) -> Result<(), Error> {
    validate_identifier(name, false)
        .then_some(())
        .ok_or_else(|| Error::InvalidMetric(format!("invalid Prometheus label name {name:?}")))
}

fn validate_identifier(value: &str, allow_colon: bool) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_alphabetic() || (allow_colon && first == ':')) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric() || (allow_colon && ch == ':'))
}

#[cfg(test)]
mod tests {
    use super::{validate_label_name, validate_metric_name};

    #[test]
    fn prometheus_identifiers_are_validated() {
        assert!(validate_metric_name("http_requests_total").is_ok());
        assert!(validate_metric_name(":synthetic").is_ok());
        assert!(validate_metric_name("9bad").is_err());
        assert!(validate_label_name("instance").is_ok());
        assert!(validate_label_name(":bad").is_err());
    }
}
