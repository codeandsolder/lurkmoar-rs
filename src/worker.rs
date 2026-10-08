use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
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
    gzip: Vec<u8>,
    uncompressed_bytes: u64,
    samples: u64,
}

pub fn spawn(
    receiver: mpsc::Receiver<Command>,
    health: Arc<Health>,
    config: Config,
) -> Result<(), Error> {
    thread::Builder::new()
        .name("lurkmoar".into())
        .spawn(move || run(&receiver, &health, &config))
        .map(|_| ())
        .map_err(Error::Io)
}

#[expect(
    clippy::too_many_lines,
    reason = "the delivery state machine is easier to audit as one ordered loop"
)]
fn run(receiver: &mpsc::Receiver<Command>, health: &Health, config: &Config) {
    let agent_config = ureq::Agent::config_builder()
        .timeout_global(Some(config.timeout))
        .build();
    let agent: ureq::Agent = agent_config.into();
    update_spool_bytes(health, config.spool_dir.as_deref());

    let mut pending = VecDeque::new();
    let mut offline_since = if replay_spool(&agent, health, config).is_ok() {
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
                    let mut payloads = vec![first];
                    let mut flush_waiters = Vec::new();
                    let mut disconnected = false;
                    loop {
                        match receiver.try_recv() {
                            Ok(Command::Payload(payload)) => payloads.push(payload),
                            Ok(Command::Flush(waiter)) => flush_waiters.push(waiter),
                            Err(mpsc::TryRecvError::Empty) => break,
                            Err(mpsc::TryRecvError::Disconnected) => {
                                disconnected = true;
                                break;
                            }
                        }
                    }
                    match encode_batch(&payloads, &config.static_labels) {
                        Ok(batch) => {
                            record_encoded(health, &batch);
                            if let Err(error) = send_gzip(
                                &agent,
                                health,
                                &config.endpoint,
                                &batch.gzip,
                                batch.uncompressed_bytes,
                                batch.samples,
                                false,
                            ) {
                                pending.push_back(batch);
                                set_pending(health, pending.len());
                                eprintln!("lurkmoar: delivery failed: {error}");
                                let now = Instant::now();
                                offline_since = Some(now);
                                retry_deadline = now + config.retry_interval;
                                spill_deadline = Some(now + config.spill_after);
                            }
                        }
                        Err(error) => eprintln!("lurkmoar: cannot encode metrics: {error}"),
                    }
                    for waiter in flush_waiters {
                        let flush_result = attempt_delivery(&agent, health, config, &mut pending);
                        let _ = waiter.send(flush_result);
                    }
                    if disconnected {
                        shutdown(health, config, &mut pending);
                        return;
                    }
                }
                Ok(Command::Flush(waiter)) => {
                    let _ = waiter.send(attempt_delivery(&agent, health, config, &mut pending));
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
                    Err(error) => eprintln!("lurkmoar: cannot encode metrics: {error}"),
                }
            }
            Ok(Command::Flush(waiter)) => {
                let result = attempt_delivery(&agent, health, config, &mut pending);
                if result.is_ok() {
                    offline_since = None;
                    spill_deadline = None;
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
                eprintln!("lurkmoar: spool write failed: {error}");
            }
            spill_deadline = Some(now + config.spill_after);
        }
        if now >= retry_deadline {
            if attempt_delivery(&agent, health, config, &mut pending).is_ok() {
                offline_since = None;
                spill_deadline = None;
            } else {
                retry_deadline = now + config.retry_interval;
            }
        }
    }
}

fn shutdown(health: &Health, config: &Config, pending: &mut VecDeque<Batch>) {
    if !pending.is_empty()
        && let Err(error) = spill_pending(health, config, pending)
    {
        eprintln!("lurkmoar: shutdown spool failed: {error}");
    }
}

fn attempt_delivery(
    agent: &ureq::Agent,
    health: &Health,
    config: &Config,
    pending: &mut VecDeque<Batch>,
) -> Result<(), Error> {
    replay_spool(agent, health, config)?;
    while let Some(batch) = pending.front() {
        send_gzip(
            agent,
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

fn replay_spool(agent: &ureq::Agent, health: &Health, config: &Config) -> Result<(), Error> {
    let Some(directory) = config.spool_dir.as_deref() else {
        return Ok(());
    };
    for path in spool::files(directory)? {
        let contents = fs::read(&path)?;
        let batches = match spool::decode(&contents) {
            Ok(batches) => batches,
            Err(error) => {
                eprintln!(
                    "lurkmoar: discarding corrupt spool {}: {error}",
                    path.display()
                );
                fs::remove_file(&path)?;
                continue;
            }
        };
        for batch in batches {
            let (uncompressed_bytes, samples) = inspect_gzip(batch)?;
            send_gzip(
                agent,
                health,
                &config.endpoint,
                batch,
                uncompressed_bytes,
                samples,
                true,
            )?;
        }
        fs::remove_file(&path)?;
    }
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
    let encoded = spool::encode(pending.iter().map(|batch| batch.gzip.as_slice()));
    let existing = spool::size(directory)?;
    let encoded_len = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
    if existing.saturating_add(encoded_len) > config.spool_max_bytes {
        health.dropped_batches_total.fetch_add(
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
    agent: &ureq::Agent,
    health: &Health,
    endpoint: &str,
    body: &[u8],
    uncompressed_bytes: u64,
    samples: u64,
    replayed: bool,
) -> Result<(), Error> {
    health.attempts_total.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let result = agent
        .post(endpoint)
        .header("Content-Type", "text/plain; version=0.0.4")
        .header("Content-Encoding", "gzip")
        .send(body);
    let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    health
        .request_duration_microseconds_total
        .fetch_add(micros, Ordering::Relaxed);
    health
        .last_request_duration_microseconds
        .store(micros, Ordering::Relaxed);
    match result {
        Ok(_) => {
            health.sent_batches_total.fetch_add(1, Ordering::Relaxed);
            health
                .sent_samples_total
                .fetch_add(samples, Ordering::Relaxed);
            health.sent_compressed_bytes_total.fetch_add(
                u64::try_from(body.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            health
                .sent_uncompressed_bytes_total
                .fetch_add(uncompressed_bytes, Ordering::Relaxed);
            if replayed {
                health
                    .replayed_batches_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            health
                .last_success_unixtime
                .store(unix_seconds(), Ordering::Relaxed);
            Ok(())
        }
        Err(error) => {
            health.failures_total.fetch_add(1, Ordering::Relaxed);
            Err(Error::Delivery(error.to_string()))
        }
    }
}

fn record_encoded(health: &Health, batch: &Batch) {
    health
        .encoded_samples_total
        .fetch_add(batch.samples, Ordering::Relaxed);
    health.encoded_compressed_bytes_total.fetch_add(
        u64::try_from(batch.gzip.len()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    health
        .encoded_uncompressed_bytes_total
        .fetch_add(batch.uncompressed_bytes, Ordering::Relaxed);
}

fn inspect_gzip(body: &[u8]) -> Result<(u64, u64), Error> {
    let mut decoder = GzDecoder::new(body);
    let mut text = String::new();
    decoder.read_to_string(&mut text).map_err(Error::Io)?;
    let bytes = u64::try_from(text.len()).unwrap_or(u64::MAX);
    let samples =
        u64::try_from(text.lines().filter(|line| !line.is_empty()).count()).unwrap_or(u64::MAX);
    Ok((bytes, samples))
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
    let gzip = encoder.finish().map_err(Error::Io)?;
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
        decoder.read_to_string(&mut text)?;
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
