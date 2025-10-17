# Observability & Safety By Default

This document captures the target instrumentation, quotas, and failure policies
that will be implemented in the Metricity pipeline. The goal is to provide
predictable behaviour under load, surface actionable telemetry, and make the
runtime fail safe by default.

## Metrics

All metrics use the existing `source.Metrics` sink. Labels follow the pattern
`{component_kind}_{node_id}` unless specified otherwise.

| Metric | Type | Description |
| --- | --- | --- |
| `pipeline_events_ingested_total_source_<id>` | counter | Number of events pulled from each source (collector stage). |
| `pipeline_events_processed_total_transform_<id>` | counter | Events accepted by a transform after filters. |
| `pipeline_rows_emitted_total_sink_<id>` | counter | Rows flushed to each sink. |
| `pipeline_event_latency_ns_total_node_<id>` / `_count_node_<id>` | counters | End-to-end latency per source node (collector → transform → sink) to derive p50/p99 on the dashboard. |
| `pipeline_transform_exec_ns_total_<id>` / `_count_<id>` | counters | CPU cost of SQL program execution per event (used for budget enforcement). |
| `pipeline_group_state_bytes_<id>` | gauge | Current memory footprint of each SQL transform (key material + aggregate state). |
| `pipeline_group_count_<id>` | gauge | Active group count per transform. |
| `pipeline_group_evictions_total_transform_<id>_<reason>` | counter | TTL / max_groups / state_bytes evictions. |
| `pipeline_late_events_total_<id>` | counter | Events older than the configured lateness threshold. |
| `pipeline_row_truncate_total_transform_<id>_<target>` | counter | Rows/fields truncated to satisfy byte limits (`row`). |
| `pipeline_errors_total_transform_<id>_<policy>` | counter | Policy driven error handling occurrences (skip_event/null/clamp/error). |
| `pipeline_ack_latency_ns_total` / `_events_total` | counter | Existing ack metrics (retained for dashboards). |
| `collector_backpressure_total` et al. | counter/gauge | Existing collector metrics (retained, exposed on dashboards). |

Dashboards will plot instant rate (`rate()` / `irate()`) for EPS and derived
latency percentiles using histogram-like aggregates (`*_ns_total` + `_count`).

## Limits & Quotas

Configuration gains a new `limits` block under each SQL transform, with safe
defaults applied when omitted:

```toml
[transforms.sql_enrich.limits]
max_groups = 10_000              # soft upper bound, eviction kicks in above this
max_state_bytes = "64MiB"        # total memory budget per transform
max_row_bytes = "64KiB"          # outbound row size
max_group_bytes = "1MiB"         # per-group state footprint
cpu_budget_ns_per_sec = "100ms"  # CPU time budget per wall second
late_event_threshold_seconds = 5 # drop/flag events older than this
```

* **Group store** – still honours TTL (default 15 minutes). When any limit is
  breached the oldest groups are evicted and `pipeline_group_evictions_total`
  is incremented with the relevant reason.
* **Row/group byte limits** – rows or state exceeding the limit are truncated or
  rejected based on the configured error policy (see below). Truncation emits
  `pipeline_row_truncate_total`.
* **CPU budget** – each transform tracks execution CPU time. When the
  accumulated time within a sliding second exceeds the budget, subsequent events
  are throttled (marked as retryable failures) until the budget recovers.
* **Late events** – when `metadata.received_at` is present and older than the
  threshold, the event is counted in `pipeline_late_events_total` and handled
  according to the error policy.

## Error Handling Policies

Transforms expose a new `error_policy` option with the following semantics:

| Policy | Behaviour |
| --- | --- |
| `skip_event` (default) | Acknowledge the event without emitting a row; increments `pipeline_errors_total_transform_<id>_skip_event`. |
| `null` | Acknowledge without emission while marking the occurrence via metrics. |
| `clamp` | Truncate string payloads down to the configured row cap when possible; otherwise behaves like `skip_event`. |
| `error` | Surface an error, mark the batch as failed (retryable), and stop processing once budgets expire. |

Policies apply to:

* Row oversize detections.
* Late event thresholds.
* Aggregate state expansion beyond configured per-group or total budgets.
* CPU budget exhaustion (defaults to `skip_event` to avoid runaway loops).

## Alerting & Dashboards

* **Dashboards** – aggregate EPS per source/transform/sink, plot state size,
  eviction rates, latency, late events, and error policy hits. Existing channel
  depth metrics remain to visualise backpressure.
* **Alerts** – trigger on sustained eviction spikes, late event surges, error
  policy hits, and CPU budget breaches (based on `errors_total` counters).

## Testing & Validation

Load tests will:

1. Saturate groups to verify eviction metrics and ensure no OOM.
2. Send oversized payloads to validate truncation/error policies.
3. Inject slow SQL expressions to confirm CPU budget throttling.
4. Feed skewed timestamps to observe late event handling.

The runtime should degrade gracefully, never exceeding configured state memory,
and always confirm behaviour through the new telemetry.
