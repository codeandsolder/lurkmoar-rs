use bytes::Bytes;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use reqwest::blocking::Client;
use std::collections::VecDeque;
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::{Error, Health, spool, unix_millis, unix_seconds};

#[derive(Debug)]
pub enum Payload {
    PrometheusText {
        text: String,
        timestamp_ms: u64,
    },
    Sample {
        name: String,
        value: f64,
        labels: Vec<(String, String)>,
        timestamp_ms: u64,
    },
}

#[derive(Debug)]
pub enum Command {
    Payload(Payload),
    Flush(mpsc::SyncSender<Result<(), Error>>),
}

#[derive(Debug)]
pub struct Config {
    pub endpoint: String,
    pub static_labels: Vec<(String, String)>,
    pub timeout: Duration,
    pub retry_interval: Duration,
    pub spill_after: Duration,
    pub spool_dir: Option<PathBuf>,
    pub spool_max_bytes: u64,
}

#[derive(Debug)]
struct Batch {
    gzip: Bytes,
    uncompressed_bytes: u64,
    samples: u64,
}

pub fn spawn(
    receiver: mpsc::Receiver<Command>,
    health: Arc<Health>,
    config: Config,
) -> Result<(), Error> {
    let (ready, startup) = mpsc::sync_channel(1);
    let _worker = thread::Builder::new()
        .name("lurkmoar".into())
        .spawn(move || {
            // The blocking client owns an internal runtime. Construct and
            // destroy it on the delivery thread, never in the caller's runtime.
            match http_client(config.timeout) {
                Ok(client) => {
                    if ready.send(Ok(())).is_ok() {
                        run(&client, &receiver, &health, &config);
                    }
                }
                Err(error) => {
                    let _ = ready.send(Err(error));
                }
            }
        })?;
    startup.recv().map_err(|_| Error::Closed)?
}

fn http_client(timeout: Duration) -> Result<Client, Error> {
    let builder = Client::builder()
        .timeout(timeout)
        // A redirect may turn a metrics POST into a GET; never acknowledge it
        // as a successful ingestion.
        .redirect(reqwest::redirect::Policy::none());
    #[cfg(all(not(target_os = "android"), any(unix, windows)))]
    let builder = builder.tls_certs_merge(
        webpki_root_certs::TLS_SERVER_ROOT_CERTS
            .iter()
            .map(|cert| reqwest::Certificate::from_der(cert.as_ref()))
            .collect::<reqwest::Result<Vec<_>>>()
            .map_err(|error| Error::Delivery(error.to_string()))?,
    );
    builder
        .build()
        .map_err(|error| Error::Delivery(error.to_string()))
}

// Cap each steady-state compression/delivery unit, even if producers
// replenish the command queue faster than consumption. This is a payload
// count bound (like the replay file-count bound), not a total memory bound.
// A flush is a FIFO barrier: it must not wait for later submissions.
const LIVE_MAX_PAYLOADS_PER_REQUEST: usize = 128;

struct ReadyBatch {
    payloads: Vec<Payload>,
    flush: Option<mpsc::SyncSender<Result<(), Error>>>,
    disconnected: bool,
}

fn take_ready_batch(first: Payload, receiver: &mpsc::Receiver<Command>) -> ReadyBatch {
    let mut batch = ReadyBatch {
        payloads: vec![first],
        flush: None,
        disconnected: false,
    };
    while batch.payloads.len() < LIVE_MAX_PAYLOADS_PER_REQUEST {
        match receiver.try_recv() {
            Ok(Command::Payload(payload)) => batch.payloads.push(payload),
            Ok(Command::Flush(waiter)) => {
                batch.flush = Some(waiter);
                break;
            }
            Err(mpsc::TryRecvError::Empty) => break,
            Err(mpsc::TryRecvError::Disconnected) => {
                batch.disconnected = true;
                break;
            }
        }
    }
    batch
}

#[expect(
    clippy::too_many_lines,
    reason = "the delivery state machine is easier to audit as one ordered loop"
)]
fn run(client: &Client, receiver: &mpsc::Receiver<Command>, health: &Health, config: &Config) {
    update_spool_bytes(health, config.spool_dir.as_deref());

    let mut pending = VecDeque::new();
    let mut offline_since = if replay_spool(client, health, config).is_ok() {
        None
    } else {
        Some(Instant::now())
    };
    let mut retry_deadline = Instant::now() + config.retry_interval;
    let mut spill_deadline = offline_since.map(|start| start + config.spill_after);

    loop {
        if offline_since.is_none() {
            match receiver.recv() {
                Ok(Command::Payload(first)) => {
                    let ReadyBatch {
                        payloads,
                        flush,
                        disconnected,
                    } = take_ready_batch(first, receiver);
                    let mut encoding_error = None;
                    match encode_batch(&payloads, &config.static_labels) {
                        Ok(batch) => {
                            record_encoded(health, &batch);
                            if let Err(error) = send_gzip(
                                client,
                                health,
                                &config.endpoint,
                                &batch.gzip,
                                batch.uncompressed_bytes,
                                batch.samples,
                                false,
                            ) {
                                pending.push_back(batch);
                                set_pending(health, pending.len());
                                tracing::warn!("lurkmoar: delivery failed: {error}");
                                let now = Instant::now();
                                offline_since = Some(now);
                                retry_deadline = now + config.retry_interval;
                                spill_deadline = Some(now + config.spill_after);
                            }
                        }
                        Err(error) => {
                            tracing::warn!("lurkmoar: cannot encode metrics: {error}");
                            let _ = health.dropped_batches_total.fetch_add(1, Ordering::Relaxed);
                            encoding_error = Some(error);
                        }
                    }
                    if let Some(waiter) = flush {
                        let delivery = attempt_delivery(client, health, config, &mut pending);
                        if delivery.is_ok() {
                            offline_since = None;
                            spill_deadline = None;
                        } else {
                            let now = Instant::now();
                            if offline_since.is_none() {
                                offline_since = Some(now);
                                spill_deadline = Some(now + config.spill_after);
                            }
                            retry_deadline = now + config.retry_interval;
                        }
                        let _ = waiter.send(encoding_error.map_or(delivery, Err));
                    }
                    if disconnected {
                        shutdown(health, config, &mut pending);
                        return;
                    }
                }
                Ok(Command::Flush(waiter)) => {
                    let result = attempt_delivery(client, health, config, &mut pending);
                    if result.is_err() {
                        let now = Instant::now();
                        offline_since = Some(now);
                        retry_deadline = now + config.retry_interval;
                        spill_deadline = Some(now + config.spill_after);
                    }
                    let _ = waiter.send(result);
                }
                Err(_) => {
                    shutdown(health, config, &mut pending);
                    return;
                }
            }
            continue;
        }

        let now = Instant::now();
        let next_spill = spill_deadline.unwrap_or(now + config.spill_after);
        let wake_at = retry_deadline.min(next_spill);
        let wait = wake_at.saturating_duration_since(now);
        match receiver.recv_timeout(wait) {
            Ok(Command::Payload(payload)) => {
                match encode_batch(&[payload], &config.static_labels) {
                    Ok(batch) => {
                        record_encoded(health, &batch);
                        pending.push_back(batch);
                        set_pending(health, pending.len());
                    }
                    Err(error) => tracing::warn!("lurkmoar: cannot encode metrics: {error}"),
                }
            }
            Ok(Command::Flush(waiter)) => {
                let result = attempt_delivery(client, health, config, &mut pending);
                if result.is_ok() {
                    offline_since = None;
                    spill_deadline = None;
                } else {
                    retry_deadline = Instant::now() + config.retry_interval;
                }
                let _ = waiter.send(result);
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                shutdown(health, config, &mut pending);
                return;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }

        let now = Instant::now();
        if spill_deadline.is_some_and(|deadline| now >= deadline) {
            if let Err(error) = spill_pending(health, config, &mut pending) {
                tracing::warn!("lurkmoar: spool write failed: {error}");
            }
            spill_deadline = Some(now + config.spill_after);
        }
        if now >= retry_deadline {
            if attempt_delivery(client, health, config, &mut pending).is_ok() {
                offline_since = None;
                spill_deadline = None;
            } else {
                // Measure backoff from completion of the failed attempt. Using the
                // pre-attempt timestamp can collapse the delay to zero when the
                // request itself takes at least one retry interval.
                retry_deadline = Instant::now() + config.retry_interval;
            }
        }
    }
}

fn shutdown(health: &Health, config: &Config, pending: &mut VecDeque<Batch>) {
    if !pending.is_empty()
        && let Err(error) = spill_pending(health, config, pending)
    {
        tracing::warn!("lurkmoar: shutdown spool failed: {error}");
    }
}

fn attempt_delivery(
    client: &Client,
    health: &Health,
    config: &Config,
    pending: &mut VecDeque<Batch>,
) -> Result<(), Error> {
    replay_spool(client, health, config)?;
    while let Some(batch) = pending.front() {
        send_gzip(
            client,
            health,
            &config.endpoint,
            &batch.gzip,
            batch.uncompressed_bytes,
            batch.samples,
            false,
        )?;
        let _ = pending.pop_front();
        set_pending(health, pending.len());
    }
    Ok(())
}

// Batch persisted frames into a bounded number of HTTP requests. Replaying
// one small historical frame per HTTP request can take minutes after an
// outage, starving fresh sccache_info heartbeats and breaking health checks.
// Bound *both* number of spool files and uncompressed payload size.
const REPLAY_MAX_FILES_PER_REQUEST: usize = 128;
const REPLAY_MAX_UNCOMPRESSED_BYTES: usize = 512 * 1024;

/// Submit all fully buffered files as one gzip document. On any delivery
/// failure, leave every file intact so the next attempt can replay it.
fn send_replay_files(
    client: &Client,
    health: &Health,
    config: &Config,
    paths: &mut Vec<PathBuf>,
    text: &mut String,
    samples: &mut u64,
) -> Result<(), Error> {
    if paths.is_empty() {
        return Ok(());
    }
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(text.as_bytes())?;
    let payload = Bytes::from(encoder.finish()?);
    send_gzip(
        client,
        health,
        &config.endpoint,
        &payload,
        u64::try_from(text.len()).unwrap_or(u64::MAX),
        *samples,
        true,
    )?;
    for path in paths.drain(..) {
        fs::remove_file(path)?;
    }
    text.clear();
    *samples = 0;
    Ok(())
}

fn replay_spool(client: &Client, health: &Health, config: &Config) -> Result<(), Error> {
    let Some(directory) = config.spool_dir.as_deref() else {
        return Ok(());
    };
    let mut paths = Vec::new();
    let mut text = String::new();
    let mut samples = 0_u64;
    for path in spool::files(directory)? {
        let contents = fs::read(&path)?;
        let batches = match spool::decode(&contents) {
            Ok(batches) => batches,
            Err(error) => {
                tracing::warn!(
                    "lurkmoar: discarding corrupt spool {}: {error}",
                    path.display()
                );
                fs::remove_file(&path)?;
                continue;
            }
        };
        let mut file_text = String::new();
        let mut file_samples = 0_u64;
        for batch in batches {
            let mut decoder = GzDecoder::new(batch);
            let _ = decoder.read_to_string(&mut file_text)?;
        }
        for line in file_text.lines() {
            if !line.is_empty() {
                file_samples = file_samples.saturating_add(1);
            }
        }
        if !paths.is_empty()
            && (paths.len() >= REPLAY_MAX_FILES_PER_REQUEST
                || text.len().saturating_add(file_text.len()) > REPLAY_MAX_UNCOMPRESSED_BYTES)
        {
            send_replay_files(client, health, config, &mut paths, &mut text, &mut samples)?;
        }
        text.push_str(&file_text);
        samples = samples.saturating_add(file_samples);
        paths.push(path);
        if paths.len() >= REPLAY_MAX_FILES_PER_REQUEST
            || text.len() >= REPLAY_MAX_UNCOMPRESSED_BYTES
        {
            send_replay_files(client, health, config, &mut paths, &mut text, &mut samples)?;
        }
    }
    send_replay_files(client, health, config, &mut paths, &mut text, &mut samples)?;
    update_spool_bytes(health, Some(directory));
    Ok(())
}

fn spill_pending(
    health: &Health,
    config: &Config,
    pending: &mut VecDeque<Batch>,
) -> Result<(), Error> {
    let Some(directory) = config.spool_dir.as_deref() else {
        return Ok(());
    };
    if pending.is_empty() {
        return Ok(());
    }
    let encoded = spool::encode(pending.iter().map(|batch| batch.gzip.as_ref()));
    let existing = spool::size(directory)?;
    let encoded_len = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
    if existing.saturating_add(encoded_len) > config.spool_max_bytes {
        let _ = health.dropped_batches_total.fetch_add(
            u64::try_from(pending.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        pending.clear();
        set_pending(health, 0);
        update_spool_bytes(health, Some(directory));
        return Ok(());
    }
    let _ = spool::write(directory, unix_millis(), &encoded)?;
    pending.clear();
    set_pending(health, 0);
    update_spool_bytes(health, Some(directory));
    Ok(())
}

fn send_gzip(
    client: &Client,
    health: &Health,
    endpoint: &str,
    body: &Bytes,
    uncompressed_bytes: u64,
    samples: u64,
    replayed: bool,
) -> Result<(), Error> {
    let _ = health.attempts_total.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let result = client
        .post(endpoint)
        .header("Content-Type", "text/plain; version=0.0.4")
        .header("Content-Encoding", "gzip")
        .body(Bytes::clone(body))
        .send()
        .map_err(|error| Error::Delivery(error.without_url().to_string()))
        .and_then(|response| {
            if response.status().is_success() {
                Ok(())
            } else {
                Err(Error::Delivery(format!("HTTP {}", response.status())))
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
                .fetch_add(samples, Ordering::Relaxed);
            let _ = health.sent_compressed_bytes_total.fetch_add(
                u64::try_from(body.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            let _ = health
                .sent_uncompressed_bytes_total
                .fetch_add(uncompressed_bytes, Ordering::Relaxed);
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

fn record_encoded(health: &Health, batch: &Batch) {
    let _ = health
        .encoded_samples_total
        .fetch_add(batch.samples, Ordering::Relaxed);
    let _ = health.encoded_compressed_bytes_total.fetch_add(
        u64::try_from(batch.gzip.len()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    let _ = health
        .encoded_uncompressed_bytes_total
        .fetch_add(batch.uncompressed_bytes, Ordering::Relaxed);
}

fn encode_batch(payloads: &[Payload], static_labels: &[(String, String)]) -> Result<Batch, Error> {
    let mut text = String::new();
    let mut samples = 0_u64;
    for payload in payloads {
        match payload {
            Payload::PrometheusText {
                text: source,
                timestamp_ms,
            } => {
                for line in source.lines() {
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    let Some(space) = sample_value_separator(line) else {
                        continue;
                    };
                    let metric = add_labels(&line[..space], static_labels);
                    let Some(value) = line[space..].split_whitespace().next() else {
                        continue;
                    };
                    text.push_str(&metric);
                    text.push(' ');
                    text.push_str(value);
                    text.push(' ');
                    text.push_str(&timestamp_ms.to_string());
                    text.push('\n');
                    samples = samples.saturating_add(1);
                }
            }
            Payload::Sample {
                name,
                value,
                labels,
                timestamp_ms,
            } => {
                let mut combined = static_labels.to_vec();
                for (key, value) in labels {
                    if let Some((_, existing)) = combined
                        .iter_mut()
                        .find(|(existing_key, _)| existing_key == key)
                    {
                        existing.clone_from(value);
                    } else {
                        combined.push((key.clone(), value.clone()));
                    }
                }
                text.push_str(&add_labels(name, &combined));
                text.push(' ');
                text.push_str(&render_value(*value));
                text.push(' ');
                text.push_str(&timestamp_ms.to_string());
                text.push('\n');
                samples = samples.saturating_add(1);
            }
        }
    }
    let uncompressed_bytes = u64::try_from(text.len()).unwrap_or(u64::MAX);
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(text.as_bytes())?;
    let gzip = Bytes::from(encoder.finish()?);
    Ok(Batch {
        gzip,
        uncompressed_bytes,
        samples,
    })
}

fn sample_value_separator(line: &str) -> Option<usize> {
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in line.bytes().enumerate() {
        if quoted {
            if escaped {
                escaped = false;
                continue;
            }
            match byte {
                b'\\' => escaped = true,
                b'"' => quoted = false,
                _ => {}
            }
            continue;
        }
        match byte {
            b'"' => quoted = true,
            b' ' | b'\t' => return Some(index),
            _ => {}
        }
    }
    None
}

fn add_labels(metric: &str, labels: &[(String, String)]) -> String {
    if labels.is_empty() {
        return metric.to_owned();
    }
    let rendered = labels
        .iter()
        .map(|(key, value)| format!("{key}=\"{}\"", escape_label_value(value)))
        .collect::<Vec<_>>()
        .join(",");
    metric.strip_suffix('}').map_or_else(
        || format!("{metric}{{{rendered}}}"),
        |prefix| {
            if prefix.ends_with('{') {
                format!("{prefix}{rendered}}}")
            } else {
                format!("{prefix},{rendered}}}")
            }
        },
    )
}

fn escape_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn render_value(value: f64) -> String {
    if value.is_nan() {
        "NaN".into()
    } else if value == f64::INFINITY {
        "+Inf".into()
    } else if value == f64::NEG_INFINITY {
        "-Inf".into()
    } else {
        value.to_string()
    }
}

fn set_pending(health: &Health, value: usize) {
    health.pending_batches.store(value, Ordering::Relaxed);
    if value > 0 {
        let _ = health.oldest_pending_unixtime.compare_exchange(
            0,
            unix_seconds(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }
}

fn update_spool_bytes(health: &Health, directory: Option<&std::path::Path>) {
    let bytes = directory
        .and_then(|path| spool::size(path).ok())
        .unwrap_or(0);
    health.spool_bytes.store(bytes, Ordering::Relaxed);
    if bytes > 0 {
        let _ = health.oldest_pending_unixtime.compare_exchange(
            0,
            unix_seconds(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    } else if health.pending_batches.load(Ordering::Relaxed) == 0 {
        health.oldest_pending_unixtime.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::{Payload, add_labels, encode_batch, sample_value_separator};
    use flate2::read::GzDecoder;
    use std::io::Read;

    fn decode_gzip(bytes: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
        let mut decoder = GzDecoder::new(bytes);
        let mut text = String::new();
        let _ = decoder.read_to_string(&mut text)?;
        Ok(text)
    }

    #[test]
    fn text_is_timestamped_and_static_labels_are_added() -> Result<(), Box<dyn std::error::Error>> {
        let payload = Payload::PrometheusText {
            text: "# HELP x demo\nx{a=\"b\"} 1.5\ny 2\n".into(),
            timestamp_ms: 1_234,
        };
        let gzip = encode_batch(&[payload], &[("instance".into(), "host-a".into())])?;
        assert_eq!(
            decode_gzip(&gzip.gzip)?,
            "x{a=\"b\",instance=\"host-a\"} 1.5 1234\ny{instance=\"host-a\"} 2 1234\n"
        );
        Ok(())
    }

    #[test]
    fn labels_are_escaped() {
        assert_eq!(
            add_labels("x", &[("a".into(), "q\"\\\n".into())]),
            "x{a=\"q\\\"\\\\\\n\"}"
        );
    }
    #[test]
    fn sample_separator_ignores_whitespace_inside_labels() {
        let line = r#"x{a="hello world",b="q\" z"} 1"#;
        assert_eq!(sample_value_separator(line), line.rfind(" 1"));
        assert_eq!(sample_value_separator("plain\t2"), Some(5));
    }

    #[test]
    fn text_labels_with_spaces_and_escapes_survive_timestamping()
    -> Result<(), Box<dyn std::error::Error>> {
        let payload = Payload::PrometheusText {
            text: concat!(
                "uname_info{version=\"#1 SMP PREEMPT_DYNAMIC Thu Oct 1\",note=\"a\\\" b\\\\ c\"} 1\n",
                "reason_info{reason=\"multiple input files\"} 3\n",
            ).into(),
            timestamp_ms: 1_234,
        };
        let gzip = encode_batch(&[payload], &[("instance".into(), "host-a".into())])?;
        assert_eq!(
            decode_gzip(&gzip.gzip)?,
            concat!(
                "uname_info{version=\"#1 SMP PREEMPT_DYNAMIC Thu Oct 1\",note=\"a\\\" b\\\\ c\",instance=\"host-a\"} 1 1234\n",
                "reason_info{reason=\"multiple input files\",instance=\"host-a\"} 3 1234\n",
            )
        );
        Ok(())
    }
}

#[cfg(test)]
mod batch_tests {
    use super::{Command, LIVE_MAX_PAYLOADS_PER_REQUEST, Payload, take_ready_batch};
    use std::sync::mpsc;

    fn payload(index: usize) -> Payload {
        Payload::PrometheusText {
            text: index.to_string(),
            timestamp_ms: 0,
        }
    }

    #[test]
    fn ready_batches_are_bounded_and_do_not_consume_after_flush()
    -> Result<(), Box<dyn std::error::Error>> {
        let (sender, receiver) = mpsc::channel();
        let (flush, waiter) = mpsc::sync_channel(1);
        for index in 1..=LIVE_MAX_PAYLOADS_PER_REQUEST {
            sender.send(Command::Payload(payload(index)))?;
        }
        sender.send(Command::Flush(flush))?;
        sender.send(Command::Payload(payload(LIVE_MAX_PAYLOADS_PER_REQUEST + 1)))?;
        drop(sender);

        let first = take_ready_batch(payload(0), &receiver);
        assert_eq!(first.payloads.len(), LIVE_MAX_PAYLOADS_PER_REQUEST);
        assert!(first.flush.is_none());
        assert!(!first.disconnected);
        let Command::Payload(next) = receiver.try_recv()? else {
            return Err("expected remaining payload before flush".into());
        };
        let second = take_ready_batch(next, &receiver);
        assert_eq!(second.payloads.len(), 1);
        let Some(flush) = second.flush else {
            return Err("expected flush barrier".into());
        };
        flush.send(Ok(()))?;
        waiter.recv()??;
        let Command::Payload(last) = receiver.try_recv()? else {
            return Err("expected payload after flush".into());
        };
        let final_batch = take_ready_batch(last, &receiver);
        assert_eq!(final_batch.payloads.len(), 1);
        assert!(final_batch.disconnected);
        Ok(())
    }
}
