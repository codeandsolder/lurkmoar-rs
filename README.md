# lurkmoar-rs

Change-driven Prometheus metrics delivery for Rust daemons. No HTTP server,
sampling clock or deliberate steady-state batching timer: a submission wakes
its background delivery worker immediately. Already queued changes may share
one HTTP request.

```rust
use lurkmoar::ClientBuilder;

let metrics = ClientBuilder::new("https://liberta.example/api/v1/import/prometheus")
    .static_label("instance", "edge-a")
    .spool("/var/lib/my-daemon/metrics-spool", 10 * 1024 * 1024)
    .build()?;

let exits = metrics.gauge("warpproxy_active_exits", [("role", "exit")])?;
exits.set(7.0)?; // sends now
exits.set(7.0)?; // no change, no send

let failures = metrics.counter("warpproxy_failures_total", [("kind", "duplicate_exit")])?;
failures.inc()?;
```

Collectors can submit an untimestamped Prometheus 0.0.4 text snapshot using
`metrics.submit_prometheus_text(rendered_snapshot)?`. Blank lines and comments
are omitted; every sample receives the same submission timestamp. Explicit
sample labels override configured static labels. Invalid syntax, duplicate
labels, reserved `__name__` labels and preexisting timestamps are rejected
rather than silently dropping individual lines. An empty snapshot sends nothing.

## Admission and outage behavior

- Admitted work is bounded across the command queue, compression and RAM retry
  backlog (default: 4096 payloads and 32 MiB of accounted bytes). Configure both
  with `backlog_limits(max_payloads, max_bytes)`. The byte budget charges rendered
  input and per-payload overhead; it is not an exact process RSS limit.
- A full backlog returns `Error::Backpressure` immediately; an oversized
  individual payload returns `Error::PayloadTooLarge`. A failed submission does
  not advance gauge or counter state. Reservations are released only after
  acknowledgement, durable spill or an explicitly recorded drop.
- Only HTTP 2xx responses acknowledge delivery. Redirects are not followed.
  Failed batches remain compressed in RAM and retry after the configured delay
  (default 1 s, measured from completion of the failed request).
- With a spool directory, after a continuous outage reaches `spill_after`
  (default 10 s), pending batches are atomically persisted. Further outage
  chunks spill at the same cadence. The oldest prefix that fits the hard disk
  byte cap is preserved; newer batches that cannot fit are explicitly dropped.
  With no spool, RAM limits apply for the entire outage.
- A spool directory belongs to one worker at a time. Its persistent advisory
  lock file must not be deleted; a staged file from an interrupted write is
  cleaned up when the next worker acquires the lock. A spool should be on a
  local filesystem supporting advisory locks and atomic rename.
- Recovery sends persisted files oldest-first, then RAM. Ambiguous HTTP
  acknowledgements can replay a batch: configure backend deduplication if
  duplicate timestamps matter (e.g. 1 ms for Liberta/VictoriaMetrics).

`Client::flush()` is a synchronous FIFO barrier that attempts delivery of all
preceding accepted work; it reports delivery failures and newly discovered or
recorded losses. `Client::shutdown()` closes admission for all cloned handles,
drains accepted commands, persists outstanding outage work when possible,
reports losses, and releases the spool lock before returning. Both can block;
call them outside async executor threads. Dropping every handle also drains the
queue, but explicit shutdown is necessary to observe the outcome.

## Recovery and observability

Ordinary live batches target at most 128 payloads and 512 KiB of accounted
input. Historical files are grouped by up to 128 files and 512 KiB of
uncompressed lines; a large individual snapshot or spool file remains atomic.
Replay validates gzip frames and counts samples without materializing the
uncompressed contents, then concatenates intact gzip members into a standard
multistream gzip POST. Source files are removed only after successful delivery
of their valid frames. Corrupt frames are counted and logged without blocking
other valid frames in the same file. A malformed trailing frame retains its
valid prefix.

`Client::health()` and `Client::health_prometheus(prefix)` expose request and
failure counts, accepted-work reservations and rejection counts, RAM batches,
disk bytes, oldest pending age, loss counts, replay counts, sample and byte
counts, request latency and last successful delivery time. Replay retains
original submission timestamps. Pending age covers encoded RAM and persisted
work; reservations also include not-yet-encoded queued submissions.

The crate owns metric serialization and delivery, not application scheduling,
metric collection, an HTTP server or dashboard definitions.
