# ADR-0001: Windowed Aggregations, Event-Time Semantics, and Side Output

## Context

Current SQL transforms operate on unbounded incremental aggregates without windowing.
Watermark, allowed lateness, and side-output concepts are absent, and group state is keyed
solely by logical `GROUP BY` expressions. Introducing TUMBLING, HOPPING, and SESSION windows
with event-time semantics requires a coordinated design affecting the parser, runtime,
pipeline integration, and observability.

## Decision

The system will introduce event-time–driven window semantics with explicit configuration and
SQL overrides, watermark management, and structured side output. The key aspects are:

### Event-Time Resolution

1. **SQL override** — a query may specify `WINDOW ... ON <expr>` and `WATERMARK(...)`. When
   present, this definition provides the authoritative event-time expression, watermark lag,
   and allowed lateness.
2. **Transform configuration defaults** — each SQL transform exposes
   `event_time_field`, `watermark_lag`, and `allowed_lateness`. These values apply when the
   SQL plan omits overrides.
3. **Source metadata fallback** — if configuration does not resolve a timestamp, the runtime
   attempts to use source metadata (e.g. `Event.metadata.received_at` or `@timestamp`).
4. **Processing-time fallback** — when all previous sources fail, processing time is used,
   and the pipeline logs a warning once per transform and increments a dedicated metric.

### Window Lifecycle

- Windows are keyed by `(group_key, window_start_ns)`. The runtime maintains active windows
  and enforces `allowed_lateness`. Watermarks close windows when they exceed `window_end +
  allowed_lateness`.
- Supported window kinds:
  - **TUMBLING** — fixed-size, contiguous windows.
  - **HOPPING** — overlapping windows defined by `size` and `slide`.
  - **SESSION** — dynamically sized windows with a `gap` parameter.
- Each window maintains its own aggregate state and exposes `window_start`, `window_end`,
  and `window_id = hash(plan_id, window_start_ns, window_end_ns)`.

### Watermark Management

- Every transform shard tracks its event-time watermark (monotonically non-decreasing).
- A global coordinator computes `global_watermark = min(shard_watermark_i)` and closes
  windows when `global_watermark >= window_end + allowed_lateness`.
- Watermark lag metrics and fallback occurrences are emitted for observability.

### Global Windows and Merge Semantics

- Aggregates implement an associative `merge` interface in addition to `update`.
- Shards emit `WindowPartial{ window_id, shard_id, aggregate_snapshot, watermark }` when a
  window is ready locally.
- The coordinator merges partials for each `window_id` and emits a single final row.
- Emission is idempotent: the coordinator tracks `emit_epoch` to avoid duplicates after
  retries.

### Side Output

Side-output events follow a structured payload:

```json
{
  "kind": "late|evicted|error|duplicate",
  "reason": "allowed_lateness_exceeded|max_groups|division_by_zero|...",
  "event_time": "2025-10-18T12:34:56.789Z",
  "processing_time": "2025-10-18T12:34:58.000Z",
  "watermark": "2025-10-18T12:34:56.000Z",
  "window": { "start": "...", "end": "..." },
  "group_key": { "k1": "...", "k2": 123 },
  "original_event": { ... },
  "error_detail": "stack/diag",
  "plan_id": "hash/sql",
  "shard": 3
}
```

Configuration controls routing:

```toml
[transforms.sql.side_output]
late.sink = "dlq_kafka"          # or "reinject", "metrics_only"
late.include_original = true
evicted.sink = "metrics_only"
error.sink = "dlq_kafka"
duplicate.sink = "metrics_only"
```

- `metrics_only` increments counters without forwarding the payload.
- `dlq_*` delivers to a dead-letter sink (Kafka/S3/etc.) with backpressure protection
  and drop-on-saturation policy.
- `reinject` routes to a configured input queue; disabled by default for `late` events.

Side-output channels have rate limits to protect the main pipeline. Drops are counted and
surfaced via metrics.

## Consequences

- Parser, planner, and runtime must accept window definitions and propagate overrides.
- Group state storage needs to handle multiple windows per group and enforce quotas per
  `(group, window)`.
- Transform workers must maintain watermark state, emit partials, and surface side-output
  events according to configuration.
- A new coordinator component orchestrates partial merges and global watermarks.
- Observability expands with metrics for window lifecycle, watermark lag, side-output
  events, and fallback usage.

## Status

Accepted — implementation pending.
