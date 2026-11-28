use bytes::Bytes;
use flate2::Compression;
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use std::collections::VecDeque;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::time::Instant;
use wreq::{Client, Uri};

use crate::{
    Error, Health,
    admission::Permit,
    metric::{self, Series},
    spool, unix_seconds,
};

#[derive(Debug)]
pub(super) enum Payload {
    PrometheusText {
        text: Box<str>,
        samples: u64,
        timestamp_ms: u64,
    },
    Sample {
        series: Arc<Series>,
        value: f64,
        timestamp_ms: u64,
    },
}

impl Payload {
    pub fn memory_cost(&self) -> usize {
        // Account for queue/permit/batch metadata, gzip framing and the longest
        // decimal double representation. Shared selectors are charged per sample.
        match self {
            Self::PrometheusText { text, .. } => text.len().saturating_add(256),
            Self::Sample { series, .. } => series.as_str().len().saturating_add(768),
        }
    }

    const fn timestamp_ms(&self) -> u64 {
        match self {
            Self::PrometheusText { timestamp_ms, .. } | Self::Sample { timestamp_ms, .. } => {
                *timestamp_ms
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct QueuedPayload {
    pub payload: Payload,
    pub permit: Permit,
}

type Waiter = mpsc::SyncSender<Result<(), Error>>;

#[derive(Debug)]
pub(super) enum Command {
    Payload(QueuedPayload),
    Flush(Waiter),
    Shutdown(Waiter),
}

#[derive(Debug)]
pub(super) struct Config {
    pub endpoint: Uri,
    pub timeout: Duration,
    pub retry_interval: Duration,
    pub spill_after: Duration,
    pub spool: Option<spool::Store>,
    pub spool_max_bytes: u64,
}

#[derive(Debug)]
struct Encoded {
    gzip: Bytes,
    uncompressed_bytes: u64,
    samples: u64,
}

#[derive(Debug)]
struct Batch {
    data: Encoded,
    oldest_ms: u64,
    _permits: Vec<Permit>,
}

pub(super) fn spawn(
    receiver: Receiver<Command>,
    health: Arc<Health>,
    config: Config,
) -> Result<(), Error> {
    let (ready, startup) = mpsc::sync_channel(1);
    let _worker = thread::Builder::new()
        .name("lurkmoar".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = ready.send(Err(error.into()));
                    return;
                }
            };
            runtime.block_on(async move {
                let client = match http_client(config.timeout) {
                    Ok(client) => client,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                if let Err(error) = refresh_spool(&health, config.spool.as_ref()) {
                    let _ = ready.send(Err(error));
                    return;
                }
                if ready.send(Ok(())).is_err() {
                    return;
                }
                let (waiter, result) = run(&client, receiver, &health, &config).await;
                // Explicit shutdown completion means the spool lock was released.
                drop(config);
                if let Some(waiter) = waiter {
                    let _ = waiter.send(result);
                } else if let Err(error) = result {
                    tracing::error!(%error, "lurkmoar: implicit shutdown failed");
                }
            });
        })?;
    startup.recv().map_err(|_| Error::Closed)?
}

fn http_client(timeout: Duration) -> Result<Client, Error> {
    Client::builder()
        .timeout(timeout)
        .redirect(wreq::redirect::Policy::none())
        .build()
        .map_err(|error| Error::Transport(error.without_uri()))
}

// A large snapshot is atomic; otherwise stop at either request limit. No
// deliberate steady-state batching timer. Deferred commands preserve FIFO
// order even at byte boundaries and across flush/shutdown barriers.
const LIVE_MAX_PAYLOADS: usize = 128;
const REQUEST_TARGET_BYTES: usize = 512 * 1024;

struct Inbox {
    receiver: Receiver<Command>,
    deferred: Option<Command>,
}

impl Inbox {
    async fn recv(&mut self) -> Option<Command> {
        match self.deferred.take() {
            Some(command) => Some(command),
            None => self.receiver.recv().await,
        }
    }

    fn take_batch(&mut self, first: QueuedPayload) -> Vec<QueuedPayload> {
        let mut bytes = first.payload.memory_cost();
        let mut payloads = vec![first];
        while payloads.len() < LIVE_MAX_PAYLOADS && bytes < REQUEST_TARGET_BYTES {
            match self.receiver.try_recv() {
                Ok(Command::Payload(next))
                    if bytes.saturating_add(next.payload.memory_cost()) <= REQUEST_TARGET_BYTES =>
                {
                    bytes += next.payload.memory_cost();
                    payloads.push(next);
                }
                Ok(command) => {
                    self.deferred = Some(command);
                    break;
                }
                Err(_) => break, // empty or disconnected; recv observes closure
            }
        }
        payloads
    }
}

#[derive(Debug)]
struct Outage {
    retry_at: Instant,
    spill_at: Option<Instant>,
}

impl Outage {
    fn new(config: &Config) -> Self {
        let now = Instant::now();
        Self {
            retry_at: now + config.retry_interval,
            spill_at: config.spool.as_ref().map(|_| now + config.spill_after),
        }
    }

    fn deadline(&self) -> Instant {
        self.spill_at
            .map_or(self.retry_at, |spill| spill.min(self.retry_at))
    }
}

struct Delivery<'a> {
    client: &'a Client,
    health: &'a Health,
    config: &'a Config,
    pending: VecDeque<Batch>,
    outage: Option<Outage>,
    reported_drops: u64,
}

impl Delivery<'_> {
    fn record_drop(&self, batches: u64) {
        let _ = self
            .health
            .dropped_batches_total
            .fetch_add(batches, Ordering::Relaxed);
    }

    fn report_new_drops(&mut self) -> Result<(), Error> {
        let total = self.health.dropped_batches_total.load(Ordering::Relaxed);
        let batches = total.saturating_sub(self.reported_drops);
        self.reported_drops = total;
        if batches == 0 {
            Ok(())
        } else {
            Err(Error::Dropped { batches })
        }
    }

    fn failed(&mut self, error: &Error) {
        tracing::warn!(%error, "lurkmoar: delivery failed");
        if let Some(outage) = &mut self.outage {
            // Backoff starts after the failed request, not before it.
            outage.retry_at = Instant::now() + self.config.retry_interval;
        } else {
            self.outage = Some(Outage::new(self.config));
        }
    }

    async fn send_pending(&mut self) -> Result<(), Error> {
        while let Some(batch) = self.pending.front() {
            send_gzip(
                self.client,
                self.health,
                &self.config.endpoint,
                &batch.data,
                false,
            )
            .await?;
            let _ = self.pending.pop_front();
            self.refresh_ram();
        }
        Ok(())
    }

    async fn recover(&mut self) -> Result<(), Error> {
        let result = async {
            replay_spool(self.client, self.health, self.config).await?;
            self.send_pending().await
        }
        .await;
        match &result {
            Ok(()) => self.outage = None,
            Err(error) => self.failed(error),
        }
        result
    }

    fn refresh_ram(&self) {
        self.health
            .pending_batches
            .store(self.pending.len(), Ordering::Relaxed);
        let oldest = self.pending.iter().map(|batch| batch.oldest_ms).min();
        self.health
            .oldest_ram_unixtime
            .store(encode_age(oldest), Ordering::Relaxed);
    }

    async fn accept(&mut self, payloads: Vec<QueuedPayload>) {
        match encode_batch(payloads) {
            Ok(batch) => {
                record_encoded(self.health, &batch.data);
                self.pending.push_back(batch);
                self.refresh_ram();
                if self.outage.is_none()
                    && let Err(error) = self.send_pending().await
                {
                    self.failed(&error);
                }
            }
            Err(error) => {
                self.record_drop(1);
                tracing::error!(%error, "lurkmoar: failed to encode admitted batch");
            }
        }
    }

    async fn flush(&mut self) -> Result<(), Error> {
        if self.outage.is_some() {
            self.recover().await?;
        }
        self.report_new_drops()
    }

    async fn tick(&mut self) {
        let Some(outage) = &self.outage else {
            return;
        };
        let now = Instant::now();
        let spill_due = outage.spill_at.is_some_and(|deadline| deadline <= now);
        let retry_due = outage.retry_at <= now;
        if spill_due {
            if let Err(error) = self.spill() {
                tracing::warn!(%error, "lurkmoar: spool write failed; retaining RAM backlog");
            }
            if let Some(outage) = &mut self.outage {
                outage.spill_at = Some(Instant::now() + self.config.spill_after);
            }
        }
        if retry_due {
            // recover records the failure and schedules the next attempt.
            let _ = self.recover().await;
        }
    }

    fn spill(&mut self) -> Result<(), Error> {
        let Some(store) = &self.config.spool else {
            return Ok(());
        };
        if self.pending.is_empty() {
            return Ok(());
        }
        let existing = store
            .files()?
            .iter()
            .fold(0_u64, |sum, entry| sum.saturating_add(entry.bytes));
        let mut available = self
            .config
            .spool_max_bytes
            .saturating_sub(existing)
            .saturating_sub(spool::HEADER_BYTES);
        let mut count = 0;
        for batch in &self.pending {
            let bytes = u64::try_from(batch.data.gzip.len())
                .unwrap_or(u64::MAX)
                .saturating_add(spool::FRAME_HEADER_BYTES);
            if bytes > available {
                break;
            }
            available -= bytes;
            count += 1;
        }
        if count > 0 {
            let encoded = spool::encode(
                self.pending
                    .iter()
                    .take(count)
                    .map(|batch| batch.data.gzip.as_ref()),
            )?;
            let oldest = self
                .pending
                .iter()
                .take(count)
                .map(|batch| batch.oldest_ms)
                .min()
                .ok_or_else(|| Error::State("nonempty spill has no timestamp".into()))?;
            store.write(oldest, &encoded)?;
            drop(self.pending.drain(..count));
        }
        // Preserve the maximal oldest prefix that fits. Never evict older
        // persisted backlog to admit newer data, and never silently lose it.
        if !self.pending.is_empty() {
            let dropped = u64::try_from(self.pending.len()).unwrap_or(u64::MAX);
            self.record_drop(dropped);
            tracing::warn!(
                dropped,
                "lurkmoar: spool capacity exhausted; dropping newest batches"
            );
            self.pending.clear();
        }
        self.refresh_ram();
        refresh_spool(self.health, Some(store))
    }

    fn finish(&mut self) -> Result<(), Error> {
        let result = self.spill();
        if !self.pending.is_empty() {
            let dropped = u64::try_from(self.pending.len()).unwrap_or(u64::MAX);
            self.record_drop(dropped);
            tracing::error!(
                dropped,
                "lurkmoar: shutdown could not persist pending batches"
            );
            self.pending.clear();
            self.refresh_ram();
        }
        result?;
        self.report_new_drops()
    }
}

async fn run(
    client: &Client,
    receiver: Receiver<Command>,
    health: &Health,
    config: &Config,
) -> (Option<Waiter>, Result<(), Error>) {
    let mut inbox = Inbox {
        receiver,
        deferred: None,
    };
    let mut delivery = Delivery {
        client,
        health,
        config,
        pending: VecDeque::new(),
        outage: None,
        reported_drops: 0,
    };
    let mut shutdown = None;
    // Historical backlog always precedes accepted live samples.
    let _ = delivery.recover().await;
    loop {
        let command = if let Some(outage) = &delivery.outage
            && shutdown.is_none()
        {
            let deadline = outage.deadline();
            if deadline <= Instant::now() {
                delivery.tick().await;
                continue;
            }
            match tokio::time::timeout_at(deadline, inbox.recv()).await {
                Ok(command) => command,
                Err(_) => {
                    delivery.tick().await;
                    continue;
                }
            }
        } else {
            inbox.recv().await
        };
        match command {
            Some(Command::Payload(first)) => delivery.accept(inbox.take_batch(first)).await,
            Some(Command::Flush(waiter)) => {
                let _ = waiter.send(delivery.flush().await);
            }
            Some(Command::Shutdown(waiter)) => {
                if shutdown.is_none() {
                    // Closing rejects concurrent submissions; all commands
                    // accepted before closure are drained before completion.
                    inbox.receiver.close();
                    shutdown = Some(waiter);
                } else {
                    let _ = waiter.send(Err(Error::Closed));
                }
            }
            None => return (shutdown, delivery.finish()),
        }
    }
}

// A zero atomic value means no backlog. Encode an epoch timestamp as one so
// even data timestamped at the epoch can have an accurate age.
fn encode_age(timestamp_ms: Option<u64>) -> u64 {
    timestamp_ms.map_or(0, |timestamp| timestamp / 1000 + 1)
}

fn refresh_spool(health: &Health, store: Option<&spool::Store>) -> Result<(), Error> {
    let files = store
        .map(spool::Store::files)
        .transpose()?
        .unwrap_or_default();
    record_spool_inventory(health, &files);
    Ok(())
}

fn record_spool_inventory(health: &Health, files: &[spool::Entry]) {
    health.spool_bytes.store(
        files
            .iter()
            .fold(0_u64, |sum, entry| sum.saturating_add(entry.bytes)),
        Ordering::Relaxed,
    );
    health.oldest_spool_unixtime.store(
        encode_age(files.first().map(|entry| entry.timestamp_ms)),
        Ordering::Relaxed,
    );
}

fn encode_batch(payloads: Vec<QueuedPayload>) -> Result<Batch, Error> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    let mut scratch = String::new();
    let mut samples = 0_u64;
    let mut uncompressed_bytes = 0_u64;
    let mut oldest_ms = u64::MAX;
    let mut permits = Vec::with_capacity(payloads.len());
    for QueuedPayload { payload, permit } in payloads {
        oldest_ms = oldest_ms.min(payload.timestamp_ms());
        let (text, count) = match &payload {
            Payload::PrometheusText { text, samples, .. } => (text.as_ref(), *samples),
            Payload::Sample {
                series,
                value,
                timestamp_ms,
            } => {
                scratch.clear();
                metric::render_sample(&mut scratch, series, *value, *timestamp_ms);
                (scratch.as_str(), 1)
            }
        };
        encoder.write_all(text.as_bytes())?;
        uncompressed_bytes =
            uncompressed_bytes.saturating_add(u64::try_from(text.len()).unwrap_or(u64::MAX));
        samples = samples.saturating_add(count);
        permits.push(permit);
    }
    Ok(Batch {
        data: Encoded {
            gzip: Bytes::from(encoder.finish()?),
            uncompressed_bytes,
            samples,
        },
        oldest_ms,
        _permits: permits,
    })
}

// Validate gzip CRC/length and count lines without materializing decompressed
// data. The producer emits one newline per sample. Gzip permits concatenated
// members (RFC 1952); Liberta/VictoriaMetrics use a multistream reader.
#[derive(Default)]
struct SampleCount(u64);

impl Write for SampleCount {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(
            u64::try_from(data.iter().filter(|&&byte| byte == b'\n').count()).unwrap_or(u64::MAX),
        );
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn inspect_gzip(gzip: &[u8]) -> io::Result<(u64, u64)> {
    if gzip.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty gzip frame",
        ));
    }
    let mut count = SampleCount::default();
    let bytes = io::copy(&mut MultiGzDecoder::new(gzip), &mut count)?;
    Ok((bytes, count.0))
}

struct ReplayFile {
    path: PathBuf,
    dropped: u64,
}

#[derive(Default)]
struct ReplayBatch {
    files: Vec<ReplayFile>,
    gzip: Vec<u8>,
    uncompressed_bytes: u64,
    samples: u64,
}

impl ReplayBatch {
    fn read(path: PathBuf) -> Result<Self, Error> {
        let contents = fs::read(&path)?;
        let mut batch = Self::default();
        let mut dropped = 0_u64;
        match spool::decode(&contents) {
            Ok(frames) => {
                for frame in frames {
                    match frame.and_then(|frame| inspect_gzip(frame).map(|stats| (frame, stats))) {
                        Ok((frame, (bytes, samples))) => {
                            batch.gzip.extend_from_slice(frame);
                            batch.uncompressed_bytes =
                                batch.uncompressed_bytes.saturating_add(bytes);
                            batch.samples = batch.samples.saturating_add(samples);
                        }
                        Err(error) => {
                            tracing::warn!(path = %path.display(), %error, "lurkmoar: corrupt spool frame; preserving valid frames");
                            dropped = dropped.saturating_add(1);
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "lurkmoar: corrupt spool file header");
                dropped = 1;
            }
        }
        batch.files.push(ReplayFile { path, dropped });
        Ok(batch)
    }

    fn append(&mut self, mut other: Self) {
        self.files.append(&mut other.files);
        self.gzip.append(&mut other.gzip);
        self.uncompressed_bytes = self
            .uncompressed_bytes
            .saturating_add(other.uncompressed_bytes);
        self.samples = self.samples.saturating_add(other.samples);
    }
}

const REPLAY_MAX_FILES: usize = 128;

async fn send_replay_batch(
    client: &Client,
    health: &Health,
    config: &Config,
    batch: ReplayBatch,
) -> Result<(), Error> {
    let ReplayBatch {
        files,
        gzip,
        uncompressed_bytes,
        samples,
    } = batch;
    if !gzip.is_empty() {
        let data = Encoded {
            gzip: Bytes::from(gzip),
            uncompressed_bytes,
            samples,
        };
        send_gzip(client, health, &config.endpoint, &data, true).await?;
    }
    // Only acknowledge the source files after the complete grouped HTTP
    // request succeeds. If removal fails midway, the remaining files replay
    // at least once. Account corrupt frames only when their file is removed.
    let removal = files.into_iter().try_for_each(|file| {
        fs::remove_file(file.path)?;
        let _ = health
            .dropped_batches_total
            .fetch_add(file.dropped, Ordering::Relaxed);
        Ok::<(), io::Error>(())
    });
    let refresh = refresh_spool(health, config.spool.as_ref());
    if let Err(error) = removal {
        if let Err(refresh_error) = refresh {
            tracing::warn!(%refresh_error, "lurkmoar: cannot refresh spool after partial acknowledgement");
        }
        return Err(error.into());
    }
    refresh
}

async fn replay_spool(client: &Client, health: &Health, config: &Config) -> Result<(), Error> {
    let Some(store) = &config.spool else {
        return Ok(());
    };
    let mut batch = ReplayBatch::default();
    let entries = store.files()?;
    record_spool_inventory(health, &entries);
    for entry in entries {
        let file = match ReplayBatch::read(entry.path) {
            Ok(file) => file,
            Err(error) => {
                if !batch.files.is_empty() {
                    send_replay_batch(client, health, config, batch).await?;
                }
                return Err(error);
            }
        };
        if !batch.files.is_empty()
            && (batch.files.len() >= REPLAY_MAX_FILES
                || batch
                    .uncompressed_bytes
                    .saturating_add(file.uncompressed_bytes)
                    > REQUEST_TARGET_BYTES as u64)
        {
            send_replay_batch(client, health, config, std::mem::take(&mut batch)).await?;
        }
        batch.append(file);
        if batch.files.len() >= REPLAY_MAX_FILES
            || batch.uncompressed_bytes >= REQUEST_TARGET_BYTES as u64
        {
            send_replay_batch(client, health, config, std::mem::take(&mut batch)).await?;
        }
    }
    if !batch.files.is_empty() {
        send_replay_batch(client, health, config, batch).await?;
    }
    Ok(())
}

async fn send_gzip(
    client: &Client,
    health: &Health,
    endpoint: &Uri,
    data: &Encoded,
    replayed: bool,
) -> Result<(), Error> {
    let _ = health.attempts_total.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let result = client
        .post(endpoint.clone())
        .header("Content-Type", "text/plain; version=0.0.4")
        .header("Content-Encoding", "gzip")
        .body(Bytes::clone(&data.gzip))
        .send()
        .await
        .map_err(|error| Error::Transport(error.without_uri()))
        .and_then(|response| {
            if response.status().is_success() {
                Ok(())
            } else {
                Err(Error::HttpStatus(response.status()))
            }
        });
    let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    let _ = health
        .request_duration_microseconds_total
        .fetch_add(micros, Ordering::Relaxed);
    health
        .last_request_duration_microseconds
        .store(micros, Ordering::Relaxed);
    match result {
        Ok(()) => {
            let _ = health.sent_batches_total.fetch_add(1, Ordering::Relaxed);
            let _ = health
                .sent_samples_total
                .fetch_add(data.samples, Ordering::Relaxed);
            let _ = health.sent_compressed_bytes_total.fetch_add(
                u64::try_from(data.gzip.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            let _ = health
                .sent_uncompressed_bytes_total
                .fetch_add(data.uncompressed_bytes, Ordering::Relaxed);
            if replayed {
                let _ = health
                    .replayed_batches_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            health
                .last_success_unixtime
                .store(unix_seconds(), Ordering::Relaxed);
            Ok(())
        }
        Err(error) => {
            let _ = health.failures_total.fetch_add(1, Ordering::Relaxed);
            Err(error)
        }
    }
}

fn record_encoded(health: &Health, data: &Encoded) {
    let _ = health
        .encoded_samples_total
        .fetch_add(data.samples, Ordering::Relaxed);
    let _ = health.encoded_compressed_bytes_total.fetch_add(
        u64::try_from(data.gzip.len()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    let _ = health
        .encoded_uncompressed_bytes_total
        .fetch_add(data.uncompressed_bytes, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::{
        Command, Inbox, LIVE_MAX_PAYLOADS, REQUEST_TARGET_BYTES, encode_batch, inspect_gzip,
    };
    use crate::{Client, Error};
    use std::sync::mpsc;

    fn take_payload(inbox: &mut Inbox) -> Result<super::QueuedPayload, Box<dyn std::error::Error>> {
        let command = match inbox.deferred.take() {
            Some(command) => command,
            None => inbox.receiver.try_recv()?,
        };
        match command {
            Command::Payload(payload) => Ok(payload),
            _ => Err("expected payload".into()),
        }
    }

    #[test]
    fn batching_preserves_fifo_barriers_and_count_limit() -> Result<(), Box<dyn std::error::Error>>
    {
        let (client, receiver) = Client::channel(Vec::new(), 256, 2 * REQUEST_TARGET_BYTES)?;
        for _ in 0..=LIVE_MAX_PAYLOADS {
            client.submit_prometheus_text("sample 1")?;
        }
        let (flush, waiter) = mpsc::sync_channel(1);
        client.send(Command::Flush(flush))?;
        client.submit_prometheus_text("after 2")?;
        let mut inbox = Inbox {
            receiver,
            deferred: None,
        };
        let first = take_payload(&mut inbox)?;
        let first = inbox.take_batch(first);
        assert_eq!(first.len(), LIVE_MAX_PAYLOADS);
        let encoded = encode_batch(first)?;
        assert_eq!(
            inspect_gzip(&encoded.data.gzip)?,
            (encoded.data.uncompressed_bytes, LIVE_MAX_PAYLOADS as u64)
        );
        let second = take_payload(&mut inbox)?;
        let second = inbox.take_batch(second);
        assert_eq!(second.len(), 1);
        let Some(Command::Flush(flush)) = inbox.deferred.take() else {
            return Err("expected flush barrier".into());
        };
        flush.send(Ok(()))?;
        waiter.recv()??;
        let last = take_payload(&mut inbox)?;
        assert_eq!(inbox.take_batch(last).len(), 1);
        assert!(inbox.receiver.try_recv().is_err());
        assert_eq!(client.health().outstanding_payloads, LIVE_MAX_PAYLOADS + 1);
        drop(encoded);
        drop(second);
        assert_eq!(client.health().outstanding_payloads, 0);
        Ok(())
    }

    #[test]
    fn batching_defers_payload_that_crosses_byte_target() -> Result<(), Box<dyn std::error::Error>>
    {
        let (client, receiver) = Client::channel(Vec::new(), 4, 4 * REQUEST_TARGET_BYTES)?;
        let text = format!(
            "sample{{label=\"{}\"}} 1",
            "a".repeat(REQUEST_TARGET_BYTES / 2)
        );
        client.submit_prometheus_text(&text)?;
        client.submit_prometheus_text(&text)?;
        let mut inbox = Inbox {
            receiver,
            deferred: None,
        };
        let first = take_payload(&mut inbox)?;
        assert_eq!(inbox.take_batch(first).len(), 1);
        assert!(matches!(inbox.deferred, Some(Command::Payload(_))));
        let second = take_payload(&mut inbox)?;
        assert_eq!(inbox.take_batch(second).len(), 1);
        assert_eq!(client.health().outstanding_payloads, 0);
        Ok(())
    }

    #[test]
    fn oversized_submission_is_rejected_without_using_capacity() -> Result<(), Error> {
        let (client, _receiver) = Client::channel(Vec::new(), 1, 1024)?;
        let text = format!("sample{{label=\"{}\"}} 1", "a".repeat(1024));
        assert!(matches!(
            client.submit_prometheus_text(text),
            Err(Error::PayloadTooLarge { .. })
        ));
        assert_eq!(client.health().outstanding_payloads, 0);
        client.submit_prometheus_text("sample 1")?;
        Ok(())
    }
}
