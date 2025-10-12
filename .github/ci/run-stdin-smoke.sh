#!/usr/bin/env bash

set -euo pipefail

LOG_PATH="${LOG_PATH:-metricity-stdin-smoke.log}"
LOG_DIR="$(dirname "$LOG_PATH")"

if [[ "$LOG_DIR" != "." ]]; then
  mkdir -p "$LOG_DIR"
fi

TMP_DIR="$(mktemp -d)"
metricity_pid=""

cleanup() {
  if [[ -n "${metricity_pid:-}" ]] && kill -0 "$metricity_pid" >/dev/null 2>&1; then
    kill "$metricity_pid" || true
    wait "$metricity_pid" || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

CASE_TIMEOUT_SECONDS=30
CRASH_SIGNATURE='segmentation fault|bus error|double free|panic'
LEAK_SIGNATURE='error(gpa):'

write_config() {
  local path=$1
  local codec=$2
  shift 2

  {
    cat <<EOF
[sources.stdin_in]
type = "stdin"
codec = "$codec"
EOF

    if (($# > 0)); then
      printf '%s\n' "$@"
    fi

    cat <<'EOF'

[transforms.sql_passthrough]
type = "sql"
inputs = ["stdin_in"]
query = "SELECT message FROM logs"

[sinks.console]
type = "console"
inputs = ["sql_passthrough"]
target = "stdout"
EOF
  } >"$path"
}

run_case() {
  local name=$1
  local config=$2
  local input=$3
  shift 3
  local case_log="$TMP_DIR/${name}.log"

  echo ">>> Case: $name (config: $config)" | tee -a "$LOG_PATH"

  if [[ -n "$input" ]]; then
    zig-out/bin/metricity run --config "$config" --once --max-batches 1 <"$input" >"$case_log" 2>&1 &
  else
    zig-out/bin/metricity run --config "$config" --once --max-batches 1 >"$case_log" 2>&1 &
  fi
  metricity_pid=$!

  for _ in $(seq 1 $CASE_TIMEOUT_SECONDS); do
    if ! kill -0 "$metricity_pid" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done

  if kill -0 "$metricity_pid" >/dev/null 2>&1; then
    echo "metricity process for case '$name' did not exit within timeout" >&2
    cat "$case_log" >&2 || true
    exit 1
  fi

  status=0
  wait "$metricity_pid" || status=$?
  metricity_pid=""

  if [[ $status -ne 0 ]]; then
    echo "metricity exited with status $status for case '$name'" >&2
    cat "$case_log" >&2 || true
    exit "$status"
  fi

  if grep -Eiq "$CRASH_SIGNATURE" "$case_log"; then
    echo "Detected failure signature in output for case '$name'" >&2
    cat "$case_log" >&2 || true
    exit 1
  fi

  if grep -Fq "$LEAK_SIGNATURE" "$case_log"; then
    echo "Detected allocator leak report in output for case '$name'" >&2
    cat "$case_log" >&2 || true
    exit 1
  fi

  for expected in "$@"; do
    if [[ -n "$expected" ]] && ! grep -Fq "$expected" "$case_log"; then
      echo "Expected pattern '$expected' not found in case '$name'" >&2
      cat "$case_log" >&2 || true
      exit 1
    fi
  done

  {
    echo
    echo "=== $name ==="
    cat "$case_log"
  } >>"$LOG_PATH"

  echo "Case '$name' completed successfully." | tee -a "$LOG_PATH"
}

: >"$LOG_PATH"

text_config="$TMP_DIR/stdin-text.toml"
write_config "$text_config" "text"

text_input="$TMP_DIR/stdin-text.log"
cat <<'EOF' >"$text_input"
stdin smoke test (text)
second line
EOF

ndjson_config="$TMP_DIR/stdin-ndjson.toml"
write_config "$ndjson_config" "ndjson" 'json_message_key = "body"'

ndjson_input="$TMP_DIR/stdin-ndjson.log"
cat <<'EOF' >"$ndjson_input"
{"body":"stdin smoke test (ndjson)","severity":4}
{"body":"stdin ndjson follow-up","severity":3}
EOF

invalid_config="$TMP_DIR/stdin-invalid.toml"
write_config "$invalid_config" "ndjson"

invalid_input="$TMP_DIR/stdin-invalid.log"
cat <<'EOF' >"$invalid_input"
{"body": "truncated json line"
EOF

partial_config="$TMP_DIR/stdin-partial.toml"
write_config "$partial_config" "text"

partial_input="$TMP_DIR/stdin-partial.log"
printf 'stdin smoke partial line without newline' >"$partial_input"

run_case "text-basic" "$text_config" "$text_input"
run_case "ndjson" "$ndjson_config" "$ndjson_input"
run_case "invalid-json" "$invalid_config" "$invalid_input"
run_case "partial-flush" "$partial_config" "$partial_input"

echo
echo "=== metricity output ==="
cat "$LOG_PATH"

echo "Smoke test completed successfully."
