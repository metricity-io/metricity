# Testing Module Structure

Metricity exposes shared testing utilities via `@import("testing")`. The module mirrors the
layout used by TigerBeetle and is split into the following namespaces:

- `testing.fuzz` – harness configuration, corpora management and shared entry points for fuzzing.
- `testing.property` – deterministic PRNG helpers and generators for property-based checks.
- `testing.pipeline` – scaffolding for backpressure and pipeline integration fixtures.
- `testing.bench` – helpers for collecting latency samples and reporting benchmark metrics.

Further sections will describe concrete helpers as they are implemented.

## Fuzzing

`testing.fuzz` provides:

- a configurable runner (`testing.fuzz.run`) that supports custom input generators via `generateInput` on the harness context;
- generator helpers (`testing.fuzz.common`) for synthetic configurations and SQL statements that exercise the parsers with both valid corpora and random noise.

The `testing.fuzz.config` and `testing.fuzz.sql` harnesses show these utilities in action: they mix structured inputs with raw byte fuzzing and perform extra validation (pipeline validation, SQL semantic analysis) whenever parsing succeeds.

## Property & Backpressure

- `testing.property` exposes a reusable PRNG (`DefaultPrng`, `nextRange`) and uses it in `testing/property/sql.zig` for a property-based associativity check in the SQL runtime.
- `testing.pipeline` offers collector-level backpressure scenarios (`runCollectorBackpressureTest`) built around a configurable `SourceFactory`, ensuring that batches eventually flow through after a burst of `Backpressure` responses.
