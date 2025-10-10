#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/.github/ci/logs}"
mkdir -p "$LOG_DIR"

metricity_pid=""
current_log=""

cleanup() {
  if [[ -n "${metricity_pid}" ]] && kill -0 "$metricity_pid" >/dev/null 2>&1; then
    kill "$metricity_pid" || true
    wait "$metricity_pid" || true
  fi
}
trap cleanup EXIT

start_metricity() {
  local config="$1"
  local log_path="$2"

  echo "Starting metricity with config: $config"
  echo "Log file: $log_path"

  zig-out/bin/metricity run --config "$config" --once --max-batches 1 >"$log_path" 2>&1 &
  metricity_pid=$!
  current_log="$log_path"
  sleep 2
}

wait_for_metricity() {
  local log_path="$1"

  for _ in {1..45}; do
    if ! kill -0 "$metricity_pid" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done

  if kill -0 "$metricity_pid" >/dev/null 2>&1; then
    echo "metricity process did not exit within timeout" >&2
    return 1
  fi

  set +e
  wait "$metricity_pid"
  status=$?
  set -e

  metricity_pid=""

  echo "=== metricity output ($log_path) ==="
  cat "$log_path"

  if [[ $status -ne 0 ]]; then
    echo "metricity exited with status $status" >&2
    return "$status"
  fi

  return 0
}

assert_log_clean() {
  local log_path="$1"
  if [[ -s "$log_path" ]] && grep -Eiq 'segmentation fault|bus error|double free|panic' "$log_path"; then
    echo "Failure signature detected in $log_path" >&2
    return 1
  fi
  return 0
}

run_udp_scenario() {
  local config="$ROOT_DIR/.github/ci/configs/syslog_udp.toml"
  local log_path="$LOG_DIR/syslog-udp.log"
  local port=5514

  start_metricity "$config" "$log_path"

  local message='<13>1 2024-01-01T00:00:00Z localhost app - - - UDP scenario message'
  printf "%s\n" "$message" | nc -u -w0 127.0.0.1 "$port"

  wait_for_metricity "$log_path"
  assert_log_clean "$log_path"
  echo "UDP scenario passed."
}

run_tcp_scenario() {
  local config="$ROOT_DIR/.github/ci/configs/syslog_tcp.toml"
  local log_path="$LOG_DIR/syslog-tcp.log"
  local port=6514

  start_metricity "$config" "$log_path"

  local message='<14>1 2024-01-02T00:00:00Z localhost app - - - TCP scenario message'
  printf "%s\n" "$message" | nc 127.0.0.1 "$port"

  wait_for_metricity "$log_path"
  assert_log_clean "$log_path"
  echo "TCP scenario passed."
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_command nc

echo "Running syslog end-to-end scenarios..."
run_udp_scenario
run_tcp_scenario
echo "All scenarios completed successfully."
