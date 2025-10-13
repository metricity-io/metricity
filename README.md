# Metricity

Metricity is a prototype telemetry pipeline written in Zig. The current CLI focuses on wiring
syslog-style sources through a single SQL transform and emitting the transformed rows to a
console sink.

## Building

```sh
zig build
```

## Coverage

Run `zig build coverage` to execute the test suites under `kcov` and collect the report in `zig-out/kcov`.  
Make sure `kcov` is installed on your system and available on `$PATH`.

## Commands

- `metricity run --config <path> [--once] [--max-batches <n>]`
  - Starts the pipeline using the provided configuration.
  - `--once` stops after the first batch of events.
  - `--max-batches` stops after *n* batches. Omitting both flags keeps the pipeline running
    until a termination signal is received.
- `metricity check --config <path>`
  - Parses the configuration, validates wiring, and ensures transforms can be compiled without
    starting the runtime.

Both commands also support the CLI help flag (`-h`/`--help`) inherited from the global parser.

## Example configuration

An end-to-end example is available in `examples/basic.toml`:

```toml
[sources.syslog_in]
type = "syslog"
address = "udp://0.0.0.0:514"

[transforms.sql_enrich]
type = "sql"
inputs = ["syslog_in"]
query = "SELECT syslog_severity, message FROM logs WHERE syslog_severity <= 4"

[sinks.console]
type = "console"
inputs = ["sql_enrich"]
target = "stdout"
```

### Topology restrictions

The current engine supports a single source, an optional SQL transform, and exactly one sink.
`metricity check` reports an error if additional components are present or the wiring is
incomplete. Each transform must declare the list of upstream inputs (`inputs = [...]`) and
sinks list their upstream producers the same way.

### SQL transforms

The SQL transform now implements streaming aggregation. Every statement must contain at least one
aggregate (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`). Incoming events update the state of their group and
immediately emit the latest aggregate values, making it easy to track counters or running totals
per key. `WHERE` filters still apply before aggregation, while `HAVING` filters use the aggregated
values. Group keys are limited to column references.

See `docs/sql.md` for a full reference of the supported syntax, semantics and error modes.

### Console sink

The console sink prints each transformed row as a single JSON-like line to either STDOUT or STDERR.
This makes it easy to inspect the pipeline output or redirect it for further processing.

### Syslog source configuration

Syslog sources accept a handful of transport-related tuning knobs in addition to the address and
parser mode shown above. Notable fields:

- `message_size_limit` — caps per-frame payload size before truncation.
- `tcp_high_watermark` / `tcp_low_watermark` — backpressure thresholds for TCP ring buffers.
- `allowed_peers` — CIDR filters applied before parsing.
- `flush_partial_on_close` — when `true`, any buffered bytes left in a TCP connection are flushed
  as a truncated frame when the peer closes the stream. This helps avoid losing partial messages
  on orderly shutdowns while keeping the default behavior unchanged.

### Stdin source

Metricity now ships with a native stdin source for quick prototyping and piping log streams:

```toml
[sources.stdin]
type = "stdin"
codec = "text" # or "ndjson", "raw"

multiline.mode = "disabled" # "pattern" or "stanza" when aggregation is needed
queue_capacity = 1024
when_full = "drop_oldest"
flush_partial_on_close = true
```

Key options:

- `codec` — `text` keeps raw lines, `ndjson` decodes JSON objects, `raw` bypasses UTF-8 checks.
- `multiline.*` — optional aggregation facilities (pattern-based or stanza mode).
- `queue_capacity` / `when_full` — backpressure behavior shared with other sources.
- `rate_limit_per_sec` / `rate_limit_burst` — token bucket limiter for bursty producers.
- `flush_partial_on_close` — emit partially buffered data when the upstream closes stdin.

### Metrics

When `pipeline.Options.metrics` is set to a `source.Metrics` sink, the pipeline exports the
following telemetry:

- `pipeline_channel_depth_<kind>_<node>` — queue depth gauge for each transform/sink channel.
- `pipeline_channel_capacity_<kind>_<node>` — gauge with the configured channel capacity.
- `pipeline_channel_drop_oldest_total_<kind>_<node>` — counter of events evicted under
  `drop_oldest`.
- `pipeline_channel_drop_newest_total_<kind>_<node>` — counter of events dropped under
  `drop_newest`.
- `pipeline_channel_push_block_ns_total_<kind>_<node>` — total nanoseconds producers spent
  waiting for capacity.
- `pipeline_channel_push_block_events_total_<kind>_<node>` — number of blocking occurrences.
- `pipeline_ack_success_total`, `pipeline_ack_retryable_total`, `pipeline_ack_permanent_total` —
  acknowledgement outcome counters.
- `pipeline_ack_latency_ns_total` and `pipeline_ack_latency_events_total` — cumulative latency and
  event count for batch acknowledgements.
- `sources_syslog_frames_corrupted_total{reason="length|overflow|invalid"}` — syslog source frames
  rejected by the framer or parser, labeled by the reason.

## Signals and shutdown

`metricity run` installs handlers for `SIGINT` and `SIGTERM`. Press `Ctrl+C` to stop the runtime
gracefully; the collector and sinks are flushed before the process exits.
