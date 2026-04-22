#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"

show_status() {
    local name="$1"
    local pid_file="$2"

    if [[ ! -f "$pid_file" ]]; then
        echo "$name: detenido (sin pid file)"
        return
    fi

    local pid
    pid="$(cat "$pid_file")"

    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        echo "$name: activo (PID=$pid)"
    else
        echo "$name: detenido (PID stale: $pid)"
    fi
}

show_status "simulated" "$LOG_DIR/producer_simulated.pid"
show_status "csv" "$LOG_DIR/producer_csv.pid"
show_status "aemet" "$LOG_DIR/producer_aemet.pid"
show_status "watchdog" "$LOG_DIR/producer_watchdog.pid"
