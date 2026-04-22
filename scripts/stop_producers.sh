#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"

stop_pid_file() {
    local label="$1"
    local pid_file="$2"

    if [[ ! -f "$pid_file" ]]; then
        return
    fi

    local pid
    pid="$(cat "$pid_file")"

    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
        echo "$label detenido: PID=$pid"
    else
        echo "$label no activo: $pid"
    fi

    rm -f "$pid_file"
}

stop_one() {
    local pid_file="$1"

    if [[ ! -f "$pid_file" ]]; then
        return
    fi

    local pid
    pid="$(cat "$pid_file")"

    stop_pid_file "Proceso" "$pid_file"
}

stop_pid_file "Watchdog" "$LOG_DIR/producer_watchdog.pid"

stop_one "$LOG_DIR/producer_simulated.pid"
stop_one "$LOG_DIR/producer_csv.pid"
stop_one "$LOG_DIR/producer_aemet.pid"

echo "Parada completada."
