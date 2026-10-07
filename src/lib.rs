//! Change-driven Prometheus metrics delivery with bounded outage spooling.
//!
//! `lurkmoar` deliberately does not run an HTTP server and does not impose a
//! sampling clock. Callers submit changes when they happen; a background worker
//! sends them immediately in steady state and only introduces a clock after a
//! delivery failure, for retries and bounded disk spill.

mod metric;
mod spool;
mod worker;

use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use metric::{Counter, Gauge};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_RETRY: Duration = Duration::from_secs(1);
const DEFAULT_SPILL_AFTER: Duration = Duration::from_secs(10);
const DEFAULT_SPOOL_MAX: u64 = 10 * 1024 * 1024;

#[derive(Debug)]
pub enum Error {
    InvalidConfig(String),
    InvalidMetric(String),
    Io(std::io::Error),
    Closed,
    Delivery(String),
    State(String),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "invalid configuration: {message}"),
            Self::InvalidMetric(message) => write!(formatter, "invalid metric: {message}"),
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Closed => formatter.write_str("metrics worker is closed"),
            Self::Delivery(message) => write!(formatter, "metrics delivery failed: {message}"),
            Self::State(message) => write!(formatter, "metrics state error: {message}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthSnapshot {
    pub attempts_total: u64,
    pub failures_total: u64,
    pub dropped_batches_total: u64,
    pub pending_batches: usize,
    pub spool_bytes: u64,
    pub last_success_unixtime: u64,
}

#[derive(Debug, Default)]
pub(crate) struct Health {
    pub(crate) attempts_total: AtomicU64,
    pub(crate) failures_total: AtomicU64,
    pub(crate) dropped_batches_total: AtomicU64,
    pub(crate) pending_batches: AtomicUsize,
    pub(crate) spool_bytes: AtomicU64,
    pub(crate) last_success_unixtime: AtomicU64,
}

impl Health {
    fn snapshot(&self) -> HealthSnapshot {
        HealthSnapshot {
            attempts_total: self.attempts_total.load(Ordering::Relaxed),
            failures_total: self.failures_total.load(Ordering::Relaxed),
            dropped_batches_total: self.dropped_batches_total.load(Ordering::Relaxed),
            pending_batches: self.pending_batches.load(Ordering::Relaxed),
            spool_bytes: self.spool_bytes.load(Ordering::Relaxed),
            last_success_unixtime: self.last_success_unixtime.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientBuilder {
    endpoint: String,
    static_labels: Vec<(String, String)>,
    timeout: Duration,
    retry_interval: Duration,
    spill_after: Duration,
    spool_dir: Option<PathBuf>,
    spool_max_bytes: u64,
}

impl ClientBuilder {
    #[must_use]
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            static_labels: Vec::new(),
            timeout: DEFAULT_TIMEOUT,
            retry_interval: DEFAULT_RETRY,
            spill_after: DEFAULT_SPILL_AFTER,
            spool_dir: None,
            spool_max_bytes: DEFAULT_SPOOL_MAX,
        }
    }

    #[must_use]
    pub fn static_label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.static_labels.push((name.into(), value.into()));
        self
    }

    #[must_use]
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[must_use]
    pub const fn retry_interval(mut self, retry_interval: Duration) -> Self {
        self.retry_interval = retry_interval;
        self
    }

    #[must_use]
    pub const fn spill_after(mut self, spill_after: Duration) -> Self {
        self.spill_after = spill_after;
        self
    }

    #[must_use]
    pub fn spool(mut self, directory: impl Into<PathBuf>, max_bytes: u64) -> Self {
        self.spool_dir = Some(directory.into());
        self.spool_max_bytes = max_bytes;
        self
    }

    /// Start the background delivery worker.
    ///
    /// # Errors
    /// Returns an error for invalid configuration, spool setup failure, or thread startup failure.
    pub fn build(self) -> Result<Client, Error> {
        if !(self.endpoint.starts_with("http://") || self.endpoint.starts_with("https://")) {
            return Err(Error::InvalidConfig(
                "endpoint must use http:// or https://".into(),
            ));
        }
        if self.timeout.is_zero() || self.retry_interval.is_zero() || self.spill_after.is_zero() {
            return Err(Error::InvalidConfig("timeouts must be non-zero".into()));
        }
        if self.spool_dir.is_some() && self.spool_max_bytes == 0 {
            return Err(Error::InvalidConfig(
                "spool max bytes must be non-zero".into(),
            ));
        }
        for (name, _) in &self.static_labels {
            metric::validate_label_name(name)?;
        }
        if let Some(directory) = &self.spool_dir {
            std::fs::create_dir_all(directory)?;
        }

        let health = Arc::new(Health::default());
        let (sender, receiver) = mpsc::channel();
        worker::spawn(
            receiver,
            Arc::clone(&health),
            worker::Config {
                endpoint: self.endpoint,
                static_labels: self.static_labels,
                timeout: self.timeout,
                retry_interval: self.retry_interval,
                spill_after: self.spill_after,
                spool_dir: self.spool_dir,
                spool_max_bytes: self.spool_max_bytes,
            },
        )?;
        Ok(Client { sender, health })
    }
}

#[derive(Debug, Clone)]
pub struct Client {
    pub(crate) sender: mpsc::Sender<worker::Command>,
    health: Arc<Health>,
}

impl Client {
    /// Submit an untimestamped Prometheus text snapshot for immediate delivery.
    ///
    /// # Errors
    /// Returns an error if the delivery worker is unavailable.
    pub fn submit_prometheus_text(&self, text: impl Into<String>) -> Result<(), Error> {
        self.sender
            .send(worker::Command::Payload(worker::Payload::PrometheusText {
                text: text.into(),
                timestamp_ms: unix_millis(),
            }))
            .map_err(|_| Error::Closed)
    }

    /// Submit one metric sample, timestamped at the call site.
    ///
    /// # Errors
    /// Returns an error for invalid metric/label names or an unavailable delivery worker.
    pub fn sample<I, K, V>(&self, name: &str, value: f64, labels: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        metric::validate_metric_name(name)?;
        let labels = labels
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<Vec<_>>();
        for (key, _) in &labels {
            metric::validate_label_name(key)?;
        }
        self.sender
            .send(worker::Command::Payload(worker::Payload::Sample {
                name: name.to_owned(),
                value,
                labels,
                timestamp_ms: unix_millis(),
            }))
            .map_err(|_| Error::Closed)
    }

    /// Create a change-detecting gauge handle.
    ///
    /// # Errors
    /// Returns an error for invalid metric or label names.
    pub fn gauge<I, K, V>(&self, name: &str, labels: I) -> Result<Gauge, Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Gauge::new(self.clone(), name, labels)
    }

    /// Create a process-local monotonically increasing counter handle.
    ///
    /// # Errors
    /// Returns an error for invalid metric or label names.
    pub fn counter<I, K, V>(&self, name: &str, labels: I) -> Result<Counter, Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Counter::new(self.clone(), name, labels)
    }

    #[must_use]
    pub fn health(&self) -> HealthSnapshot {
        self.health.snapshot()
    }

    #[must_use]
    pub fn health_prometheus(&self, prefix: &str) -> String {
        let health = self.health();
        format!(
            concat!(
                "{}_attempts_total {}\n",
                "{}_failures_total {}\n",
                "{}_dropped_batches_total {}\n",
                "{}_pending_batches {}\n",
                "{}_spool_bytes {}\n",
                "{}_last_success_unixtime {}\n"
            ),
            prefix,
            health.attempts_total,
            prefix,
            health.failures_total,
            prefix,
            health.dropped_batches_total,
            prefix,
            health.pending_batches,
            prefix,
            health.spool_bytes,
            prefix,
            health.last_success_unixtime,
        )
    }

    /// Force an immediate replay/delivery attempt for all queued data.
    ///
    /// # Errors
    /// Returns an error if the worker is unavailable or delivery currently fails.
    pub fn flush(&self) -> Result<(), Error> {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.sender
            .send(worker::Command::Flush(sender))
            .map_err(|_| Error::Closed)?;
        receiver.recv().map_err(|_| Error::Closed)?
    }
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> Result<std::sync::MutexGuard<'_, T>, Error> {
    mutex
        .lock()
        .map_err(|_| Error::State("mutex poisoned".into()))
}

pub(crate) fn unix_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

pub(crate) fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
