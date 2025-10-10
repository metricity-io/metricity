#!/usr/bin/env bash

set -euo pipefail

CONFIG_PATH="${1:-examples/basic.toml}"
BASE_PORT="${PORT:-5514}"
LOG_PATH="${LOG_PATH:-metricity-syslog-smoke.log}"
LOG_DIR="$(dirname "$LOG_PATH")"

if ! command -v nc >/dev/null 2>&1; then
  echo "nc command is required for syslog smoke test" >&2
  exit 1
fi

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

udp_config() {
  local path=$1
  local port=$2
  cat <<EOF >"$path"
[sources.syslog_in]
type = "syslog"
address = "udp://127.0.0.1:${port}"

[transforms.sql_enrich]
type = "sql"
inputs = ["syslog_in"]
query = "SELECT syslog_severity, message FROM logs WHERE syslog_severity <= 4"

[sinks.console]
type = "console"
inputs = ["sql_enrich"]
target = "stdout"
EOF
}

tcp_config() {
  local path=$1
  local port=$2
  cat <<EOF >"$path"
[sources.syslog_in]
type = "syslog"
address = "tcp://127.0.0.1:${port}"
transport = "tcp"
parser = "rfc5424"

[transforms.sql_enrich]
type = "sql"
inputs = ["syslog_in"]
query = "SELECT syslog_severity, message FROM logs WHERE syslog_severity <= 4"

[sinks.console]
type = "console"
inputs = ["sql_enrich"]
target = "stdout"
EOF
}

send_udp_messages() {
  local port=$1
  shift
  for payload in "$@"; do
    printf "%s\n" "$payload" | nc -u -w0 127.0.0.1 "$port"
    sleep 1
  done
}

send_tcp_messages() {
  local port=$1
  shift
  for payload in "$@"; do
    { printf "%s\n" "$payload"; sleep 1; } | nc 127.0.0.1 "$port"
    sleep 1
  done
}

run_case() {
  local name=$1
  local config=$2
  local port=$3
  shift 3
  local sender=$1
  shift

  local case_log="$TMP_DIR/${name}.log"

  echo ">>> Case: $name (config: $config)" | tee -a "$LOG_PATH"

  zig-out/bin/metricity run --config "$config" --once --max-batches 1 >"$case_log" 2>&1 &
  metricity_pid=$!

  sleep 2

  "$sender" "$port" "$@"

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

  {
    echo
    echo "=== $name ==="
    cat "$case_log"
  } >>"$LOG_PATH"

  echo "Case '$name' completed successfully." | tee -a "$LOG_PATH"
}

: >"$LOG_PATH"

run_startup_case() {
  local name=$1
  local config=$2
  local case_log="$TMP_DIR/${name}.log"

  echo ">>> Case: $name (config: $config)" | tee -a "$LOG_PATH"

  zig-out/bin/metricity run --config "$config" --max-batches 0 >"$case_log" 2>&1 &
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

  {
    echo
    echo "=== $name ==="
    cat "$case_log"
  } >>"$LOG_PATH"

  echo "Case '$name' completed successfully." | tee -a "$LOG_PATH"
}

udp_rfc5424_port=$BASE_PORT
udp_rfc3164_port=$((BASE_PORT + 1))
tcp_rfc5424_port=$((BASE_PORT + 2))
udp_invalid_port=$((BASE_PORT + 3))

udp_rfc3164_config="$TMP_DIR/udp-rfc3164.toml"
tcp_rfc5424_config="$TMP_DIR/tcp-rfc5424.toml"
udp_invalid_config="$TMP_DIR/udp-invalid.toml"

udp_config "$udp_rfc3164_config" "$udp_rfc3164_port"
tcp_config "$tcp_rfc5424_config" "$tcp_rfc5424_port"
udp_config "$udp_invalid_config" "$udp_invalid_port"

run_case "udp-rfc5424" "$CONFIG_PATH" "$udp_rfc5424_port" send_udp_messages \
  "<12>1 2024-01-01T00:00:00Z localhost app - - - Syslog smoke test (RFC5424 UDP)"

run_case "udp-rfc3164" "$udp_rfc3164_config" "$udp_rfc3164_port" send_udp_messages \
  "<34>Oct 11 22:14:15 host app[123]: Syslog smoke test (RFC3164 UDP)"

run_startup_case "tcp-startup" "$tcp_rfc5424_config"

run_case "tcp-rfc5424" "$tcp_rfc5424_config" "$tcp_rfc5424_port" send_tcp_messages \
  "<12>1 2024-01-01T00:00:02Z localhost app - - - Syslog smoke test (RFC5424 TCP)"

run_case "udp-invalid" "$udp_invalid_config" "$udp_invalid_port" send_udp_messages \
  "not-a-syslog-packet" \
  "<12>1 2024-01-01T00:00:01Z localhost app - - - Follow-up message after invalid payload"

echo
echo "=== metricity output ==="
cat "$LOG_PATH"

echo "Smoke test completed successfully."
