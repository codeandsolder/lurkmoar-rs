//! Change-driven Prometheus metrics delivery with bounded outage spooling.
//!
//! `lurkmoar` deliberately does not run an HTTP server and does not impose a
//! sampling clock. Callers submit changes when they happen; a background worker
//! sends them immediately in steady state and only introduces a clock after a
//! delivery failure, for retries and bounded disk spill.

mod admission;
mod metric;
mod spool;
mod worker;

use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{Receiver, Sender, error::TrySendError};

use admission::Admission;

pub use metric::{Counter, Gauge};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_RETRY: Duration = Duration::from_secs(1);
const DEFAULT_SPILL_AFTER: Duration = Duration::from_secs(10);
const DEFAULT_SPOOL_MAX: u64 = 10 * 1024 * 1024;
const DEFAULT_BACKLOG_PAYLOADS: usize = 4096;
const DEFAULT_BACKLOG_BYTES: usize = 32 * 1024 * 1024;

/// Configuration, admission, delivery or state failure.
#[derive(Debug)]
pub enum Error {
    /// Invalid endpoint, timeout, labels or backlog configuration.
    InvalidConfig(String),
    /// Invalid metric name, labels, value or text snapshot.
    InvalidMetric(String),
    /// Filesystem, compression or thread initialization failure.
    Io(std::io::Error),
    /// The delivery worker no longer accepts commands.
    Closed,
    /// The bounded RAM backlog or command queue is full; retry later.
    Backpressure,
    /// An individual payload cannot fit the configured admission budget.
    PayloadTooLarge { bytes: usize, limit: usize },
    /// HTTP transport failed before receiving a successful response.
    Transport(wreq::Error),
    /// The server did not acknowledge the request with a 2xx response.
    HttpStatus(wreq::StatusCode),
    /// Previously admitted batches were lost since the preceding successful barrier.
    Dropped { batches: u64 },
    /// An internal state invariant or synchronization operation failed.
    State(String),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "invalid configuration: {message}"),
            Self::InvalidMetric(message) => write!(formatter, "invalid metric: {message}"),
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Closed => formatter.write_str("metrics worker is closed"),
            Self::Backpressure => formatter.write_str("metrics RAM backlog is full"),
            Self::PayloadTooLarge { bytes, limit } => write!(
                formatter,
                "metrics payload requires {bytes} bytes, exceeding RAM backlog limit {limit}"
            ),
            Self::Transport(error) => write!(formatter, "metrics transport failed: {error}"),
            Self::HttpStatus(status) => write!(formatter, "metrics delivery failed: HTTP {status}"),
            Self::Dropped { batches } => write!(formatter, "{batches} metrics batches were lost"),
            Self::State(message) => write!(formatter, "metrics state error: {message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Transport(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Point-in-time observations of delivery health. Counters are process-local;
/// concurrent updates may be reflected at slightly different instants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthSnapshot {
    /// HTTP requests attempted, including replay and retries.
    pub attempts_total: u64,
    /// HTTP requests that did not receive a successful acknowledgement.
    pub failures_total: u64,
    /// Admitted batches lost or corrupt spool frames discarded.
    pub dropped_batches_total: u64,
    /// Payloads rejected by admission or worker closure.
    pub rejected_payloads_total: u64,
    /// Admitted payloads not yet acknowledged, spooled or dropped.
    pub outstanding_payloads: usize,
    /// Accounted bytes reserved by those payloads (not process RSS).
    pub outstanding_bytes: usize,
    /// Successfully acknowledged HTTP requests.
    pub sent_batches_total: u64,
    /// Samples in acknowledged requests, including replay.
    pub sent_samples_total: u64,
    /// Compressed body bytes acknowledged by the server.
    pub sent_compressed_bytes_total: u64,
    /// Uncompressed sample bytes in acknowledged requests.
    pub sent_uncompressed_bytes_total: u64,
    /// Successfully acknowledged requests containing persisted data.
    pub replayed_batches_total: u64,
    /// Samples compressed by this worker for live delivery.
    pub encoded_samples_total: u64,
    /// Compressed bytes produced for live delivery.
    pub encoded_compressed_bytes_total: u64,
    /// Uncompressed bytes encoded for live delivery.
    pub encoded_uncompressed_bytes_total: u64,
    /// Cumulative elapsed time for successful and failed HTTP attempts.
    pub request_duration_microseconds_total: u64,
    /// Elapsed time of the most recently completed HTTP attempt.
    pub last_request_duration_microseconds: u64,
    /// Encoded batches awaiting acknowledgement in RAM.
    pub pending_batches: usize,
    /// Bytes of published spool files awaiting acknowledgement.
    pub spool_bytes: u64,
    /// Age of the oldest encoded RAM or persisted batch; zero when empty.
    pub oldest_pending_age_seconds: u64,
    /// Unix seconds of the most recent acknowledgement, or zero before one.
    pub last_success_unixtime: u64,
}

#[derive(Debug, Default)]
struct Health {
    attempts_total: AtomicU64,
    failures_total: AtomicU64,
    dropped_batches_total: AtomicU64,
    rejected_payloads_total: AtomicU64,
    sent_batches_total: AtomicU64,
    sent_samples_total: AtomicU64,
    sent_compressed_bytes_total: AtomicU64,
    sent_uncompressed_bytes_total: AtomicU64,
    replayed_batches_total: AtomicU64,
    encoded_samples_total: AtomicU64,
    encoded_compressed_bytes_total: AtomicU64,
    encoded_uncompressed_bytes_total: AtomicU64,
    request_duration_microseconds_total: AtomicU64,
    last_request_duration_microseconds: AtomicU64,
    pending_batches: AtomicUsize,
    spool_bytes: AtomicU64,
    oldest_ram_unixtime: AtomicU64,
    oldest_spool_unixtime: AtomicU64,
    last_success_unixtime: AtomicU64,
}

impl Health {
    fn snapshot(&self, admission: &Admission) -> HealthSnapshot {
        let oldest_ram = self.oldest_ram_unixtime.load(Ordering::Relaxed);
        let oldest_spool = self.oldest_spool_unixtime.load(Ordering::Relaxed);
        let oldest_pending = [oldest_ram, oldest_spool]
            .into_iter()
            .filter(|&value| value != 0)
            .min()
            .unwrap_or(0);
        let (outstanding_payloads, outstanding_bytes) = admission.outstanding();
        HealthSnapshot {
            attempts_total: self.attempts_total.load(Ordering::Relaxed),
            failures_total: self.failures_total.load(Ordering::Relaxed),
            dropped_batches_total: self.dropped_batches_total.load(Ordering::Relaxed),
            rejected_payloads_total: self.rejected_payloads_total.load(Ordering::Relaxed),
            outstanding_payloads,
            outstanding_bytes,
            sent_batches_total: self.sent_batches_total.load(Ordering::Relaxed),
            sent_samples_total: self.sent_samples_total.load(Ordering::Relaxed),
            sent_compressed_bytes_total: self.sent_compressed_bytes_total.load(Ordering::Relaxed),
            sent_uncompressed_bytes_total: self
                .sent_uncompressed_bytes_total
                .load(Ordering::Relaxed),
            replayed_batches_total: self.replayed_batches_total.load(Ordering::Relaxed),
            encoded_samples_total: self.encoded_samples_total.load(Ordering::Relaxed),
            encoded_compressed_bytes_total: self
                .encoded_compressed_bytes_total
                .load(Ordering::Relaxed),
            encoded_uncompressed_bytes_total: self
                .encoded_uncompressed_bytes_total
                .load(Ordering::Relaxed),
            request_duration_microseconds_total: self
                .request_duration_microseconds_total
                .load(Ordering::Relaxed),
            last_request_duration_microseconds: self
                .last_request_duration_microseconds
                .load(Ordering::Relaxed),
            pending_batches: self.pending_batches.load(Ordering::Relaxed),
            spool_bytes: self.spool_bytes.load(Ordering::Relaxed),
            oldest_pending_age_seconds: if oldest_pending == 0 {
                0
            } else {
                unix_seconds().saturating_sub(oldest_pending - 1)
            },
            last_success_unixtime: self.last_success_unixtime.load(Ordering::Relaxed),
        }
    }
}

/// Explicit configuration for an independently owned delivery worker.
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    endpoint: String,
    static_labels: Vec<(String, String)>,
    timeout: Duration,
    retry_interval: Duration,
    spill_after: Duration,
    spool_dir: Option<PathBuf>,
    spool_max_bytes: u64,
    backlog_payloads: usize,
    backlog_bytes: usize,
}

impl ClientBuilder {
    /// Configure an absolute HTTP or HTTPS ingestion endpoint.
    #[must_use = "the constructed client builder must be used"]
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            static_labels: Vec::new(),
            timeout: DEFAULT_TIMEOUT,
            retry_interval: DEFAULT_RETRY,
            spill_after: DEFAULT_SPILL_AFTER,
            spool_dir: None,
            spool_max_bytes: DEFAULT_SPOOL_MAX,
            backlog_payloads: DEFAULT_BACKLOG_PAYLOADS,
            backlog_bytes: DEFAULT_BACKLOG_BYTES,
        }
    }

    /// Add a default label to all samples; explicit sample labels take precedence.
    #[must_use = "the returned builder contains the new static label"]
    pub fn static_label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.static_labels.push((name.into(), value.into()));
        self
    }

    /// Set the timeout of each HTTP request.
    #[must_use = "the returned builder contains the new request timeout"]
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the delay from a failed request to its next automatic attempt.
    #[must_use = "the returned builder contains the new retry interval"]
    pub const fn retry_interval(mut self, retry_interval: Duration) -> Self {
        self.retry_interval = retry_interval;
        self
    }

    /// Set the delay before spilling the RAM backlog during an outage.
    #[must_use = "the returned builder contains the new spill delay"]
    pub const fn spill_after(mut self, spill_after: Duration) -> Self {
        self.spill_after = spill_after;
        self
    }

    /// Enable disk spooling in an exclusive directory with a hard file-byte limit.
    #[must_use = "the returned builder contains the new spool configuration"]
    pub fn spool(mut self, directory: impl Into<PathBuf>, max_bytes: u64) -> Self {
        self.spool_dir = Some(directory.into());
        self.spool_max_bytes = max_bytes;
        self
    }

    /// Bound admitted RAM work across the queue and outage backlog. The byte
    /// budget accounts for encoded input and a per-payload allocation allowance;
    /// compression scratch space and already persisted data are separate.
    #[must_use = "the returned builder contains the new backlog limits"]
    pub const fn backlog_limits(mut self, max_payloads: usize, max_bytes: usize) -> Self {
        self.backlog_payloads = max_payloads;
        self.backlog_bytes = max_bytes;
        self
    }

    /// Start the background delivery worker.
    ///
    /// # Errors
    /// Returns an error for invalid configuration, spool setup failure, or thread startup failure.
    pub fn build(self) -> Result<Client, Error> {
        let endpoint = self
            .endpoint
            .parse::<wreq::Uri>()
            .map_err(|_| Error::InvalidConfig("invalid HTTP endpoint URI".into()))?;
        if !matches!(endpoint.scheme_str(), Some("http" | "https"))
            || endpoint.host().is_none_or(str::is_empty)
        {
            return Err(Error::InvalidConfig(
                "endpoint must be an absolute http:// or https:// URI".into(),
            ));
        }
        if [self.timeout, self.retry_interval, self.spill_after]
            .into_iter()
            .any(|duration| duration.is_zero() || Instant::now().checked_add(duration).is_none())
        {
            return Err(Error::InvalidConfig(
                "timeouts must be non-zero and representable".into(),
            ));
        }
        if self.spool_dir.is_some() && self.spool_max_bytes == 0 {
            return Err(Error::InvalidConfig(
                "spool max bytes must be non-zero".into(),
            ));
        }
        metric::validate_labels(&self.static_labels)?;
        let (client, receiver) = Client::channel(
            self.static_labels,
            self.backlog_payloads,
            self.backlog_bytes,
        )?;
        let spool = self.spool_dir.map(spool::Store::open).transpose()?;
        worker::spawn(
            receiver,
            Arc::clone(&client.health),
            worker::Config {
                endpoint,
                timeout: self.timeout,
                retry_interval: self.retry_interval,
                spill_after: self.spill_after,
                spool,
                spool_max_bytes: self.spool_max_bytes,
            },
        )?;
        Ok(client)
    }
}

/// Cloneable submission handle. All clones share the same bounded worker.
#[derive(Debug, Clone)]
pub struct Client {
    sender: Sender<worker::Command>,
    health: Arc<Health>,
    admission: Arc<Admission>,
    static_labels: Arc<[(String, String)]>,
}

impl Client {
    fn channel(
        static_labels: metric::Labels,
        max_payloads: usize,
        max_bytes: usize,
    ) -> Result<(Self, Receiver<worker::Command>), Error> {
        let admission = Arc::new(Admission::new(max_payloads, max_bytes)?);
        // One extra slot allows a flush barrier even at the payload limit.
        let (sender, receiver) = tokio::sync::mpsc::channel(max_payloads + 1);
        Ok((
            Self {
                sender,
                health: Arc::new(Health::default()),
                admission,
                static_labels: static_labels.into(),
            },
            receiver,
        ))
    }

    fn send(&self, command: worker::Command) -> Result<(), Error> {
        self.sender.try_send(command).map_err(|error| match error {
            TrySendError::Closed(_) => Error::Closed,
            TrySendError::Full(_) => Error::Backpressure,
        })
    }

    fn enqueue(&self, payload: worker::Payload) -> Result<(), Error> {
        let result = self
            .admission
            .reserve(payload.memory_cost())
            .and_then(|permit| {
                self.send(worker::Command::Payload(worker::QueuedPayload {
                    payload,
                    permit,
                }))
            });
        if result.is_err() {
            let _ = self
                .health
                .rejected_payloads_total
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn series<I, K, V>(&self, name: &str, labels: I) -> Result<metric::Series, Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        metric::Series::new(name, labels, &self.static_labels)
    }

    fn submit_series(&self, series: metric::Series, value: f64) -> Result<(), Error> {
        self.enqueue(worker::Payload::Sample {
            series,
            value,
            timestamp_ms: unix_millis(),
        })
    }

    /// Submit an untimestamped Prometheus text snapshot for immediate delivery.
    /// Explicit sample labels override static labels. An empty/comment-only
    /// snapshot does not enqueue a request.
    /// # Errors
    /// Returns an error for malformed samples, exhausted RAM admission limits,
    /// or an unavailable delivery worker. Invalid snapshots are rejected whole.
    pub fn submit_prometheus_text(&self, text: impl AsRef<str>) -> Result<(), Error> {
        let timestamp_ms = unix_millis();
        let (text, samples) =
            metric::render_snapshot(text.as_ref(), &self.static_labels, timestamp_ms)?;
        if samples == 0 {
            return Ok(());
        }
        self.enqueue(worker::Payload::PrometheusText {
            text,
            samples,
            timestamp_ms,
        })
    }

    /// Submit one metric sample, timestamped at the call site.
    /// # Errors
    /// Returns an error for invalid metric/label names, duplicate labels,
    /// exhausted RAM admission limits, or an unavailable delivery worker.
    pub fn sample<I, K, V>(&self, name: &str, value: f64, labels: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.submit_series(self.series(name, labels)?, value)
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

    /// Read the current delivery health without contacting the worker.
    #[must_use = "the returned snapshot contains observed delivery health"]
    pub fn health(&self) -> HealthSnapshot {
        self.health.snapshot(&self.admission)
    }

    /// Render health counters and gauges using a caller-supplied metric-name prefix.
    #[must_use = "the returned encoded metrics must be used"]
    pub fn health_prometheus(&self, prefix: &str) -> String {
        let health = self.health();
        format!(
            concat!(
                "{}_attempts_total {}\n",
                "{}_failures_total {}\n",
                "{}_dropped_batches_total {}\n",
                "{}_rejected_payloads_total {}\n",
                "{}_outstanding_payloads {}\n",
                "{}_outstanding_bytes {}\n",
                "{}_sent_batches_total {}\n",
                "{}_sent_samples_total {}\n",
                "{}_sent_compressed_bytes_total {}\n",
                "{}_sent_uncompressed_bytes_total {}\n",
                "{}_replayed_batches_total {}\n",
                "{}_encoded_samples_total {}\n",
                "{}_encoded_compressed_bytes_total {}\n",
                "{}_encoded_uncompressed_bytes_total {}\n",
                "{}_request_duration_microseconds_total {}\n",
                "{}_last_request_duration_microseconds {}\n",
                "{}_pending_batches {}\n",
                "{}_spool_bytes {}\n",
                "{}_oldest_pending_age_seconds {}\n",
                "{}_last_success_unixtime {}\n"
            ),
            prefix,
            health.attempts_total,
            prefix,
            health.failures_total,
            prefix,
            health.dropped_batches_total,
            prefix,
            health.rejected_payloads_total,
            prefix,
            health.outstanding_payloads,
            prefix,
            health.outstanding_bytes,
            prefix,
            health.sent_batches_total,
            prefix,
            health.sent_samples_total,
            prefix,
            health.sent_compressed_bytes_total,
            prefix,
            health.sent_uncompressed_bytes_total,
            prefix,
            health.replayed_batches_total,
            prefix,
            health.encoded_samples_total,
            prefix,
            health.encoded_compressed_bytes_total,
            prefix,
            health.encoded_uncompressed_bytes_total,
            prefix,
            health.request_duration_microseconds_total,
            prefix,
            health.last_request_duration_microseconds,
            prefix,
            health.pending_batches,
            prefix,
            health.spool_bytes,
            prefix,
            health.oldest_pending_age_seconds,
            prefix,
            health.last_success_unixtime,
        )
    }

    /// Close all handles, drain accepted commands and persist any remaining
    /// outage data before returning. The spool directory lock is released.
    /// Call outside async executor threads; this is a synchronous barrier.
    ///
    /// # Errors
    /// Returns an error if shutdown cannot be enqueued or accepted data cannot
    /// be delivered or durably stored (including spool capacity exhaustion).
    pub fn shutdown(&self) -> Result<(), Error> {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.send(worker::Command::Shutdown(sender))?;
        receiver.recv().map_err(|_| Error::Closed)?
    }

    /// Force an immediate replay/delivery attempt for all queued data.
    ///
    /// # Errors
    /// Returns an error if the worker is unavailable, the command queue is full,
    /// delivery currently fails, or previously accepted batches were dropped.
    /// This synchronous barrier may block; call it outside async executor threads.
    pub fn flush(&self) -> Result<(), Error> {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.send(worker::Command::Flush(sender))?;
        receiver.recv().map_err(|_| Error::Closed)?
    }
}

fn lock<T>(mutex: &Mutex<T>) -> Result<std::sync::MutexGuard<'_, T>, Error> {
    mutex
        .lock()
        .map_err(|_| Error::State("mutex poisoned".into()))
}

fn unix_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
