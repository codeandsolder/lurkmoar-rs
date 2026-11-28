use flate2::Compression;
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use lurkmoar::ClientBuilder;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> TestResult<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "lurkmoar-test-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            eprintln!("failed to remove test directory: {error}");
        }
    }
}

fn read_body(stream: &mut TcpStream) -> TestResult<Vec<u8>> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut received = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err("connection closed before headers completed".into());
        }
        received.extend_from_slice(&buffer[..count]);
        if let Some(position) = received.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let headers = std::str::from_utf8(&received[..header_end])?;
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .ok_or("missing content-length")?;
    while received.len() - header_end < content_length {
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err("connection closed before body completed".into());
        }
        received.extend_from_slice(&buffer[..count]);
    }
    Ok(received[header_end..header_end + content_length].to_vec())
}

fn read_request(mut stream: TcpStream) -> TestResult<String> {
    let body = read_body(&mut stream)?;
    let mut decoder = MultiGzDecoder::new(body.as_slice());
    let mut text = String::new();
    let _ = decoder.read_to_string(&mut text)?;
    stream
        .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")?;
    Ok(text)
}

#[test]
fn change_is_sent_without_a_batch_timer() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (sender, receiver) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let result = listener
            .accept()
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })
            .and_then(|(stream, _)| read_request(stream));
        let _ = sender.send(result);
    });

    let client = ClientBuilder::new(format!("http://{address}/import"))
        .static_label("instance", "test-host")
        .build()?;
    let gauge = client.gauge("demo_state", [("slot", "7")])?;
    assert!(gauge.set(3.0)?);

    let body = receiver.recv_timeout(Duration::from_millis(500))??;
    assert!(body.starts_with("demo_state{instance=\"test-host\",slot=\"7\"} 3 "));
    client.flush()?;
    let health = client.health();
    drop(client);
    assert_eq!(health.sent_batches_total, 1);
    assert_eq!(health.sent_samples_total, 1);
    assert!(health.sent_compressed_bytes_total > 0);
    assert!(health.sent_uncompressed_bytes_total > 0);
    assert_eq!(health.encoded_samples_total, 1);
    assert!(health.last_request_duration_microseconds > 0);
    assert!(!gauge.set(3.0)?);
    drop(gauge);
    server.join().map_err(|_| "server thread panicked")?;
    Ok(())
}

#[test]
fn outage_spills_then_replays() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let probe = TcpListener::bind("127.0.0.1:0")?;
    let address = probe.local_addr()?;
    drop(probe);
    let spool_dir = TestDir::new()?;

    let client = ClientBuilder::new(format!("http://{address}/import"))
        .retry_interval(Duration::from_millis(20))
        .spill_after(Duration::from_millis(50))
        .timeout(Duration::from_millis(20))
        .spool(spool_dir.path(), 1024 * 1024)
        .build()?;
    client.sample("rare_change", 1.0, std::iter::empty::<(&str, &str)>())?;

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while client.health().spool_bytes == 0 && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(client.health().failures_total > 0);
    assert!(client.health().spool_bytes > 0);
    assert!(client.health().oldest_pending_age_seconds <= 2);

    let listener = TcpListener::bind(address)?;
    let (sender, receiver) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let result = listener
            .accept()
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })
            .and_then(|(stream, _)| read_request(stream));
        let _ = sender.send(result);
    });
    client.flush()?;
    let body = receiver.recv_timeout(Duration::from_secs(1))??;
    assert!(body.starts_with("rare_change 1 "));
    let health = client.health();
    assert_eq!(health.spool_bytes, 0);
    assert_eq!(health.pending_batches, 0);
    assert_eq!(health.oldest_pending_age_seconds, 0);
    assert_eq!(health.replayed_batches_total, 1);
    assert_eq!(health.sent_samples_total, 1);
    server.join().map_err(|_| "server thread panicked")?;
    client.shutdown()?;
    drop(client);
    Ok(())
}

fn consume_request(stream: &mut TcpStream) -> TestResult {
    drop(read_body(stream)?);
    Ok(())
}

#[test]
fn retry_interval_starts_after_failed_attempt_finishes()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (sender, receiver) = mpsc::sync_channel(1);
    let server = thread::spawn(
        move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let failure = b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

            let (mut first, _) = listener.accept()?;
            consume_request(&mut first)?;
            first.write_all(failure)?;

            let (mut second, _) = listener.accept()?;
            consume_request(&mut second)?;
            thread::sleep(Duration::from_millis(150));
            second.write_all(failure)?;
            let second_finished = std::time::Instant::now();

            let (mut third, _) = listener.accept()?;
            let gap = second_finished.elapsed();
            consume_request(&mut third)?;
            third.write_all(failure)?;
            sender.send(gap)?;
            Ok(())
        },
    );

    let client = ClientBuilder::new(format!("http://{address}/import"))
        .retry_interval(Duration::from_millis(100))
        .timeout(Duration::from_secs(1))
        .build()?;
    client.sample("retry_test", 1.0, std::iter::empty::<(&str, &str)>())?;

    let gap = receiver.recv_timeout(Duration::from_secs(2))?;
    assert!(
        gap >= Duration::from_millis(75),
        "retry started only {gap:?} after the previous failed attempt completed"
    );
    server.join().map_err(|_| "server thread panicked")??;
    assert!(matches!(
        client.shutdown(),
        Err(lurkmoar::Error::Dropped { batches: 1 })
    ));
    Ok(())
}

#[test]
fn historical_backlog_replays_in_bounded_multi_file_requests()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let spool_dir = TestDir::new()?;
    for index in 0..130_u32 {
        let mut gzip = GzEncoder::new(Vec::new(), Compression::fast());
        gzip.write_all(
            format!("replay_marker{{part=\"{index}\"}} {index} 1794500000000\n").as_bytes(),
        )?;
        let compressed = gzip.finish()?;
        let mut contents = b"LURKMR01".to_vec();
        contents.extend_from_slice(&u32::try_from(compressed.len())?.to_le_bytes());
        contents.extend_from_slice(&compressed);
        std::fs::write(
            spool_dir.path().join(format!("{index:020}.spool")),
            contents,
        )?;
    }

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (sender, receiver) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let mut bodies = Vec::new();
        for _ in 0..2 {
            let (stream, _) = listener.accept()?;
            bodies.push(read_request(stream)?);
        }
        sender.send(bodies)?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });
    let client = ClientBuilder::new(format!("http://{address}/import"))
        .spool(spool_dir.path(), 1024 * 1024)
        .build()?;
    client.flush()?;
    let bodies = receiver.recv_timeout(Duration::from_secs(5))?;
    assert_eq!(bodies.len(), 2);
    assert_eq!(bodies[0].lines().count(), 128);
    assert_eq!(bodies[1].lines().count(), 2);
    assert!(bodies[0].contains("replay_marker{part=\"0\"} 0"));
    assert!(bodies[1].contains("replay_marker{part=\"129\"} 129"));
    assert_eq!(client.health().sent_samples_total, 130);
    assert_eq!(client.health().replayed_batches_total, 2);
    assert_eq!(client.health().spool_bytes, 0);
    assert_eq!(std::fs::read_dir(spool_dir.path())?.count(), 1); // persistent advisory lock
    server.join().map_err(|_| "server thread panicked")??;
    client.shutdown()?;
    drop(client);
    Ok(())
}

#[test]
fn redirected_ingestion_is_not_acknowledged() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let server = thread::spawn(
        move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept()?;
                consume_request(&mut stream)?;
                stream.write_all(b"HTTP/1.1 302 Found\r\nLocation: /wrong\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")?;
            }
            Ok(())
        },
    );
    let client = ClientBuilder::new(format!("http://{address}/import"))
        .retry_interval(Duration::from_secs(60))
        .build()?;
    client.sample("redirect_test", 1.0, std::iter::empty::<(&str, &str)>())?;
    assert!(client.flush().is_err());
    let health = client.health();
    drop(client);
    assert_eq!(health.sent_samples_total, 0);
    assert_eq!(health.failures_total, 2);
    server.join().map_err(|_| "server thread panicked")??;
    Ok(())
}

#[test]
fn ram_only_backlog_is_bounded_and_recovers() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (failed, wait_failed) = mpsc::sync_channel(1);
    let (resume, wait_resume) = mpsc::sync_channel(1);
    let server = thread::spawn(move || -> TestResult<String> {
        let (mut stream, _) = listener.accept()?;
        consume_request(&mut stream)?;
        stream.write_all(
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )?;
        failed.send(())?;
        wait_resume.recv_timeout(Duration::from_secs(5))?;
        let (stream, _) = listener.accept()?;
        read_request(stream)
    });
    let client = ClientBuilder::new(format!("http://{address}/import"))
        .retry_interval(Duration::from_secs(60))
        .backlog_limits(1, 4096)
        .build()?;
    let counter = client.counter("events", std::iter::empty::<(&str, &str)>())?;
    let _ = counter.inc()?;
    wait_failed.recv_timeout(Duration::from_secs(5))?;
    assert!(matches!(counter.inc(), Err(lurkmoar::Error::Backpressure)));
    assert_eq!(counter.get()?.to_bits(), 1.0_f64.to_bits());
    assert_eq!(client.health().outstanding_payloads, 1);
    resume.send(())?;
    client.flush()?;
    assert!(
        server
            .join()
            .map_err(|_| "server thread panicked")??
            .starts_with("events 1 ")
    );
    let health = client.health();
    assert_eq!(health.outstanding_payloads, 0);
    assert_eq!(health.outstanding_bytes, 0);
    assert_eq!(health.pending_batches, 0);
    assert_eq!(health.oldest_pending_age_seconds, 0);
    client.shutdown()?;
    drop(client);
    Ok(())
}

#[test]
fn exclusive_spool_lock_is_released_by_shutdown() -> TestResult {
    let directory = TestDir::new()?;
    let builder = ClientBuilder::new("http://127.0.0.1:1/import").spool(directory.path(), 4096);
    let first = builder.clone().build()?;
    assert!(builder.clone().build().is_err());
    first.shutdown()?;
    assert!(matches!(
        first.sample("x", 1.0, std::iter::empty::<(&str, &str)>()),
        Err(lurkmoar::Error::Closed)
    ));
    drop(first);
    builder.build()?.shutdown()?;
    Ok(())
}

#[test]
fn corrupt_spool_frame_does_not_block_valid_frames_or_hide_loss() -> TestResult {
    let directory = TestDir::new()?;
    let mut contents = b"LURKMR01".to_vec();
    for input in ["first 1 1\n", "", "last 2 3\n"] {
        let frame = if input.is_empty() {
            b"not gzip".to_vec()
        } else {
            let mut gzip = GzEncoder::new(Vec::new(), Compression::fast());
            gzip.write_all(input.as_bytes())?;
            gzip.finish()?
        };
        contents.extend_from_slice(&u32::try_from(frame.len())?.to_le_bytes());
        contents.extend_from_slice(&frame);
    }
    std::fs::write(
        directory.path().join("00000000000000000001.spool"),
        contents,
    )?;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let server = thread::spawn(move || -> TestResult<String> {
        let (stream, _) = listener.accept()?;
        read_request(stream)
    });
    let client = ClientBuilder::new(format!("http://{address}/import"))
        .spool(directory.path(), 4096)
        .build()?;
    assert!(matches!(
        client.flush(),
        Err(lurkmoar::Error::Dropped { batches: 1 })
    ));
    assert_eq!(
        server.join().map_err(|_| "server thread panicked")??,
        "first 1 1\nlast 2 3\n"
    );
    let health = client.health();
    assert_eq!(health.dropped_batches_total, 1);
    assert_eq!(health.sent_samples_total, 2);
    assert_eq!(health.spool_bytes, 0);
    client.flush()?;
    client.shutdown()?;
    drop(client);
    Ok(())
}

#[test]
fn shutdown_reports_unpersisted_work() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let server = thread::spawn(move || -> TestResult {
        let (mut stream, _) = listener.accept()?;
        consume_request(&mut stream)?;
        stream.write_all(
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )?;
        Ok(())
    });
    let client = ClientBuilder::new(format!("http://{address}/import"))
        .retry_interval(Duration::from_secs(60))
        .build()?;
    client.sample("x", 1.0, std::iter::empty::<(&str, &str)>())?;
    assert!(matches!(
        client.shutdown(),
        Err(lurkmoar::Error::Dropped { batches: 1 })
    ));
    assert_eq!(client.health().dropped_batches_total, 1);
    assert_eq!(client.health().outstanding_payloads, 0);
    server.join().map_err(|_| "server thread panicked")??;
    Ok(())
}
