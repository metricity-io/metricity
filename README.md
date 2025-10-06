# Metricity

Metricity is a prototype telemetry pipeline written in Zig. The current CLI focuses on wiring
syslog-style sources through a single SQL transform and emitting the transformed rows to a
console sink.

## Building

```sh
zig build
```

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
query = "SELECT value + 1 AS next_value, message FROM logs WHERE level = 'info'"

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

The SQL transform accepts a `SELECT` statement. The projection is evaluated against each event and
produces a row of name/value pairs. `WHERE` clauses can be used to drop events that do not match.
The runtime currently understands column references that map to log-event fields and the message
body.

### Console sink

The console sink prints each transformed row as a single JSON-like line to either STDOUT or STDERR.
This makes it easy to inspect the pipeline output or redirect it for further processing.

## Signals and shutdown

`metricity run` installs handlers for `SIGINT` and `SIGTERM`. Press `Ctrl+C` to stop the runtime
gracefully; the collector and sinks are flushed before the process exits.

