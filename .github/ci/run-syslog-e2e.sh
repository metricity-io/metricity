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
  printf "%s\n" "$message" | nc -u -w1 127.0.0.1 "$port"

  wait_for_metricity "$log_path"
  assert_log_clean "$log_path"
  echo "UDP scenario passed."
}

run_tcp_scenario() {
  local config="$ROOT_DIR/.github/ci/configs/syslog_tcp.toml"
  local log_path="$LOG_DIR/syslog-tcp.log"
  local port=6514

  start_metricity "$config" "$log_path"

  local message='<14>1 2024-01-02T00:00:00Z localhost app - - - TCP partial scenario message'
  # Send payload in two chunks without newline to exercise flush_partial_on_close.
  local tail_chars=10
  local message_len=${#message}
  if (( message_len <= tail_chars )); then
    printf "%s\n" "$message" | nc 127.0.0.1 "$port"
  else
    local prefix_len=$(( message_len - tail_chars ))
    local prefix="${message:0:prefix_len}"
    local suffix="${message:prefix_len}"
    (
      printf "%s" "$prefix"
      sleep 0.5
      printf "%s\n" "$suffix"
    ) | nc -w1 127.0.0.1 "$port"
  fi

  wait_for_metricity "$log_path"
  assert_log_clean "$log_path"
  echo "TCP scenario passed."
}

run_rate_limit_scenario() {
  local config="$ROOT_DIR/.github/ci/configs/syslog_rate_limit.toml"
  local log_path="$LOG_DIR/syslog-rate-limit.log"
  local port=5516

  start_metricity "$config" "$log_path"

  for i in {1..3}; do
    local message="<16>1 2024-01-04T00:00:0${i}Z localhost app - - - Rate limit test message $i"
    printf "%s\n" "$message" | nc -u -w1 127.0.0.1 "$port"
  done

  wait_for_metricity "$log_path"
  assert_log_clean "$log_path"
  echo "Rate limit scenario passed."
}

run_rfc3164_scenario() {
  local config="$ROOT_DIR/.github/ci/configs/syslog_rfc3164.toml"
  local log_path="$LOG_DIR/syslog-rfc3164.log"
  local port=5518

  start_metricity "$config" "$log_path"

  local message='<17>Jan  5 00:00:00 localhost app: RFC3164 scenario message'
  printf "%s\n" "$message" | nc -u -w1 127.0.0.1 "$port"

  wait_for_metricity "$log_path"
  assert_log_clean "$log_path"
  echo "RFC3164 scenario passed."
}

run_acl_scenario() {
  local config="$ROOT_DIR/.github/ci/configs/syslog_acl.toml"
  local log_path="$LOG_DIR/syslog-acl.log"
  local port=6515

  start_metricity "$config" "$log_path"

  local denied='<19>1 2024-01-06T00:00:01Z localhost app - - - ACL denied message'
  printf "%s\n" "$denied" | nc -w1 127.0.0.1 "$port"

  local observed=false
  for _ in {1..15}; do
    if grep -q "dropped packet from disallowed peer" "$log_path"; then
      observed=true
      break
    fi
    sleep 1
  done

  if [[ "$observed" != true ]]; then
    echo "Expected ACL rejection log entry not found in $log_path" >&2
    if [[ -n "$metricity_pid" ]] && kill -0 "$metricity_pid" >/dev/null 2>&1; then
      kill "$metricity_pid" >/dev/null 2>&1 || true
      wait "$metricity_pid" || true
      metricity_pid=""
    fi
    return 1
  fi

  if [[ -n "$metricity_pid" ]] && kill -0 "$metricity_pid" >/dev/null 2>&1; then
    kill -s INT "$metricity_pid" >/dev/null 2>&1 || true
  fi

  wait_for_metricity "$log_path"
  assert_log_clean "$log_path"
  echo "ACL scenario passed."
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
run_rate_limit_scenario
run_rfc3164_scenario
run_acl_scenario
echo "All scenarios completed successfully."
