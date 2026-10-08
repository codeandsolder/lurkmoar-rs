# lurkmoar-rs

Tiny change-driven Prometheus metrics delivery for Rust daemons.

`lurkmoar` does not run a metrics HTTP server and does not impose a sampling interval. A metric change is timestamped when submitted and wakes a background sender immediately. If several changes are already queued when the worker wakes, they may share the same HTTP request; there is no deliberate steady-state batching timer.

```rust
use lurkmoar::ClientBuilder;

let metrics = ClientBuilder::new("https://liberta.example/api/v1/import/prometheus")
    .static_label("instance", "edge-a")
    .spool("/var/lib/my-daemon/metrics-spool", 10 * 1024 * 1024)
    .build()?;

let exits = metrics.gauge("warpproxy_active_exits", [])?;
exits.set(7.0)?; // sends now
exits.set(7.0)?; // no change, no send

let failures = metrics.counter("warpproxy_failures_total", [("kind", "duplicate_exit")])?;
failures.inc()?;
```

Collectors that already efficiently render a complete Prometheus text snapshot can submit it without converting thousands of metrics into individual API calls:

```rust
metrics.submit_prometheus_text(rendered_snapshot)?;
```

The text API expects an untimestamped current snapshot. HELP/TYPE comments are omitted from the pushed payload; samples receive one collection timestamp and configured static labels.

## Failure behavior

Normal operation does not write metric data to disk.

1. A send failure keeps already-compressed batches in RAM.
2. Delivery is retried periodically (1 s by default).
3. After a continuous outage reaches `spill_after` (10 s by default), all current pending batches are serialized into one spool file/write.
4. Further outage chunks spill at the same cadence.
5. The spool has a hard byte cap. If the next chunk would exceed it, that newest chunk is dropped rather than deleting older backlog.
6. Recovery replays spool files oldest-first, then RAM.
7. Ambiguous HTTP acknowledgement can replay a batch; configure a tiny backend dedup interval if duplicate timestamps matter (for Liberta/VictoriaMetrics we use 1 ms).

`Client::health()` and `Client::health_prometheus(prefix)` expose attempts/failures, sent batches and samples, compressed/uncompressed byte totals, encode totals, replay count, cumulative and most-recent HTTP latency, RAM backlog, spool bytes, oldest pending age, dropped batches, and last successful delivery time. This makes compression efficiency, retry/replay behavior and delivery lag visible without each application reinventing transport instrumentation.

## Scope

The crate intentionally owns only metric serialization and reliable delivery. It does **not** own an HTTP server, application scheduling, dashboard definitions, or metric collection. Applications decide when a fact changed and call the crate.
