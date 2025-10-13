# SQL Transform Reference

The SQL transform applies a streaming aggregation to each log event that enters the Metricity
pipeline. A query is parsed and compiled once, then reused for every event. Each incoming event
updates the aggregation state of its group and immediately produces a row exposing the latest
values for that group. Downstream sinks consume those rows.

## Query Shape

Only single-statement `SELECT` queries are accepted. Statements must contain at least one aggregate
function. Non-aggregate expressions are limited to group keys. The supported surface can be
described informally as:

```
SELECT <group_expr>, …, <aggregate_expr>, …
FROM logs [AS alias]
[WHERE <predicate>]
[GROUP BY <column>, …]
[HAVING <aggregate_predicate>]
```

Key properties of the current implementation:

- Exactly one table (`logs`) may appear in `FROM`. Aliases are supported for qualifiers.
- `GROUP BY` may only reference column identifiers. Derived expressions and grouping sets are
  rejected.
- `HAVING` may reference aggregate functions and grouped columns.
- At least one aggregate function (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) must appear in the
  projection list. Queries without aggregates are rejected at compile time.
- `ORDER BY`, `DISTINCT`, joins, subqueries and window functions are not supported.

The table below summarizes the dialect:

| Element        | Status                     | Notes |
| -------------- | -------------------------- | ----- |
| `SELECT`       | ✅ required                | Aggregates and grouped columns only. |
| `DISTINCT`     | ❌ unsupported             | Compile-time `UnsupportedFeature`. |
| `FROM`         | ✅ single table            | Only `logs` with an optional alias. |
| `JOIN`         | ❌ unsupported             | Syntax parses but compilation fails. |
| `WHERE`        | ✅ optional                | Evaluated per event before aggregation. |
| `GROUP BY`     | ✅ required for non-aggregate columns | Column references only. |
| `HAVING`       | ✅ optional                | Uses aggregated values. |
| `ORDER BY`     | ❌ unsupported             | Compile-time `UnsupportedFeature`. |
| `LIMIT/OFFSET` | ❌ unsupported             | Parser rejects the clause. |

## Streaming Semantics

Aggregation is incremental: every event updates the state of its group and immediately emits a row
with the latest aggregate values. There is no windowing or tumbling semantics yet. This means, for
example, that a query such as `HAVING COUNT(*) > 10` will begin emitting rows only after the eleventh
matching event arrives for the group. State can be cleared programatically via `metricity check
--once` (which reinitializes the transform) or, in tests, by calling `Program.reset()`.

## State Retention & Eviction

Each transform worker keeps group state in memory. To prevent unbounded growth, the runtime applies
eviction policies:

- Groups that go idle for more than 15 minutes are evicted (TTL).
- Each worker keeps up to 50 000 groups; least recently updated groups are dropped when the limit is
  exceeded.
- A sweep runs every 60 seconds to apply the TTL policy.

These defaults can be overridden in the transform configuration:

```toml
[transforms.sql_enrich]
type = "sql"
inputs = ["syslog_in"]
query = "SELECT host, COUNT(*) AS total FROM logs GROUP BY host"
eviction_ttl_seconds = 900          # Set to 0 to disable TTL
eviction_max_groups = 75000         # Set to 0 for no limit
eviction_sweep_seconds = 30         # Set to 0 to disable periodic sweeps
```

The max-groups limit is also enforced after every event; TTL relies on the periodic sweep (or the next
execution triggered by an event) to reclaim stale groups.

## Supported Aggregate Functions

| Function                | Notes |
| ----------------------- | ----- |
| `COUNT(*)`              | Counts all events in the group. |
| `COUNT(expr)`           | Counts non-`NULL` values of `expr`. |
| `SUM(expr)`             | Adds integers/floats. Integer overflow raises `ArithmeticOverflow`. |
| `AVG(expr)`             | Returns a floating-point average of non-`NULL` values. |
| `MIN(expr)` / `MAX(expr)` | Works on numeric and string operands. Strings compared lexicographically. |

Nested aggregates and `DISTINCT` arguments are not supported. Aggregates may appear in `HAVING` and
in the projection list. Every aggregate invocation is tracked independently, even if textually
identical.

## Event Data Model

Queries operate on log events described in `src/source/event.zig`. Available columns:

| Column        | Type      | Description |
| ------------- | --------- | ----------- |
| `message`     | `string`  | Original log body. When projected, it becomes the group key. |
| `source_id`   | `string`? | Source identifier; `NULL` when missing. |
| `<field>`     | dynamic   | Values from the event `fields` array. |

Dynamic fields obey case-insensitive lookup for unquoted identifiers. Quoted identifiers must match
exactly. Missing fields evaluate to `NULL`.

## Expressions

Non-aggregate expressions (used in `WHERE`, `HAVING` and aggregate arguments) support:

- Literals (`'text'`, `123`, `3.14`, `TRUE/FALSE`, `NULL`).
- Column references (`message`, `source_id`, `fields...`).
- Unary operators `+`, `-`, `NOT`.
- Binary operators `+`, `-`, `*`, `/`, comparison operators, and boolean `AND` / `OR`.
- Nested expressions combining the above.

Function calls other than the aggregate functions listed above are rejected with
`UnsupportedFunction`.

## Examples

```sql
-- Count events per message
SELECT message, COUNT(*) AS total
FROM logs
GROUP BY message;

-- Track running CPU utilisation per host (ignoring NULL readings)
SELECT host, SUM(cpu_usage) AS usage_sum, COUNT(cpu_usage) AS samples
FROM logs
WHERE cpu_usage != NULL
GROUP BY host;

-- Emit rows only after two or more occurrences of the same message
SELECT message, COUNT(*) AS occurrences
FROM logs
GROUP BY message
HAVING COUNT(*) >= 2;
```

Each statement emits a row for every processed event. Downstream consumers see the latest aggregate
values for the relevant group.

## Error Handling

The runtime returns structured errors surfaced by `metricity run` / `metricity check`:

| Error                   | Trigger |
| ----------------------- | ------- |
| `UnsupportedFeature`    | Unsupported clauses, missing aggregates, non-column `GROUP BY`, nested aggregates, `DISTINCT`. |
| `UnknownColumn`         | Column references that do not match event fields or the declared group columns. |
| `TypeMismatch`          | Boolean contexts evaluating to non-boolean values, arithmetic on non-numeric operands, aggregates applied to unsupported types. |
| `UnsupportedFunction`   | Function calls other than the supported aggregate family. |
| `DivideByZero`          | Division by zero inside expressions. |
| `ArithmeticOverflow`    | Integer arithmetic overflow (including inside aggregates). |

When these errors occur during execution, the offending event is considered failed and the pipeline
propagates the failure upstream.
