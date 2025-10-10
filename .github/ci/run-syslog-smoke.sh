#!/usr/bin/env bash

set -euo pipefail

CONFIG_PATH="${1:-examples/basic.toml}"
PORT="${PORT:-5514}"
LOG_PATH="${LOG_PATH:-metricity-syslog-smoke.log}"

if ! command -v nc >/dev/null 2>&1; then
  echo "nc command is required for syslog smoke test" >&2
  exit 1
fi

log_dir="$(dirname "$LOG_PATH")"
if [[ "$log_dir" != "." ]]; then
  mkdir -p "$log_dir"
fi

echo "Running syslog smoke test with config: $CONFIG_PATH"
echo "Logging output to: $LOG_PATH"

zig-out/bin/metricity run --config "$CONFIG_PATH" --once --max-batches 1 >"$LOG_PATH" 2>&1 &
metricity_pid=$!

cleanup() {
  if kill -0 "$metricity_pid" >/dev/null 2>&1; then
    kill "$metricity_pid" || true
    wait "$metricity_pid" || true
  fi
}
trap cleanup EXIT

sleep 2

message='<12>1 2024-01-01T00:00:00Z localhost app - - - Syslog smoke test'
printf "%s\n" "$message" | nc -u -w0 127.0.0.1 "$PORT"

for _ in {1..30}; do
  if ! kill -0 "$metricity_pid" 2>/dev/null; then
    break
  fi
  sleep 1
done

if kill -0 "$metricity_pid" 2>/dev/null; then
  echo "metricity process did not exit within timeout" >&2
  exit 1
fi

status=0
wait "$metricity_pid" || status=$?
if [[ $status -ne 0 ]]; then
  echo "metricity exited with status $status" >&2
  cat "$LOG_PATH" >&2 || true
  exit "$status"
fi

echo "=== metricity output ==="
cat "$LOG_PATH"

if grep -Eiq 'segmentation fault|bus error|double free|panic' "$LOG_PATH"; then
  echo "Detected failure signature in metricity output" >&2
  exit 1
fi

echo "Smoke test completed successfully."
