use flate2::read::GzDecoder;
use lurkmoar::ClientBuilder;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

fn read_request(mut stream: TcpStream) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
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
    let body = &received[header_end..header_end + content_length];
    let mut decoder = GzDecoder::new(body);
    let mut text = String::new();
    decoder.read_to_string(&mut text)?;
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
    assert_eq!(health.sent_batches_total, 1);
    assert_eq!(health.sent_samples_total, 1);
    assert!(health.sent_compressed_bytes_total > 0);
    assert!(health.sent_uncompressed_bytes_total > 0);
    assert_eq!(health.encoded_samples_total, 1);
    assert!(health.last_request_duration_microseconds > 0);
    assert!(!gauge.set(3.0)?);
    server.join().map_err(|_| "server thread panicked")?;
    Ok(())
}

#[test]
fn outage_spills_then_replays() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let probe = TcpListener::bind("127.0.0.1:0")?;
    let address = probe.local_addr()?;
    drop(probe);
    let spool_dir = std::env::temp_dir().join(format!(
        "lurkmoar-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir_all(&spool_dir)?;

    let client = ClientBuilder::new(format!("http://{address}/import"))
        .retry_interval(Duration::from_millis(20))
        .spill_after(Duration::from_millis(50))
        .timeout(Duration::from_millis(20))
        .spool(&spool_dir, 1024 * 1024)
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
    std::fs::remove_dir_all(&spool_dir)?;
    Ok(())
}
