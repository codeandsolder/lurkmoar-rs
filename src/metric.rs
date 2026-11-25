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
        // Keep queue admission and state commit in the same critical section:
        // competing callers cannot publish an older absolute value after a
        // newer one, and a failed enqueue leaves the counter unchanged.
        self.client.sample(&self.name, next, self.labels.clone())?;
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
    use super::{Counter, validate_label_name, validate_metric_name};
    use crate::{
        Client, Health,
        worker::{Command, Payload},
    };
    use std::sync::Arc;

    #[test]
    fn counter_updates_are_ordered_and_failed_updates_do_not_commit()
    -> Result<(), Box<dyn std::error::Error>> {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let client = Client {
            sender,
            health: Arc::new(Health::default()),
        };
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
            let Command::Payload(Payload::Sample { value, .. }) = receiver.try_recv()? else {
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
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let client = Client {
            sender,
            health: Arc::new(Health::default()),
        };
        let counter = Counter::new(client, "counter", std::iter::empty::<(&str, &str)>())?;
        assert_eq!(counter.add(f64::MAX)?.to_bits(), f64::MAX.to_bits());
        assert!(counter.add(f64::MAX).is_err());
        assert_eq!(counter.get()?.to_bits(), f64::MAX.to_bits());
        let Command::Payload(Payload::Sample { value, .. }) = receiver.try_recv()? else {
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
