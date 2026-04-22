#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
WATCHDOG_SLEEP="${WATCHDOG_SLEEP:-5}"
WATCHDOG_PID_FILE="$LOG_DIR/producer_watchdog.pid"
WATCHDOG_LOG_FILE="$LOG_DIR/producer_watchdog.log"

mkdir -p "$LOG_DIR"

SIM_EPS="${SIM_EPS:-2}"
CSV_EPS="${CSV_EPS:-2}"
CSV_MAX_EVENTS="${CSV_MAX_EVENTS:-0}"
CSV_PATH="${CSV_PATH:-data/greenhouse_crop_yields.csv}"
AEMET_EPS="${AEMET_EPS:-0.0033}"
AEMET_MAX_EVENTS="${AEMET_MAX_EVENTS:-0}"
AEMET_STATION_ID="${AEMET_STATION_ID:-6325O}"

if [[ -x "$ROOT_DIR/.venv/bin/python3" ]]; then
    PYTHON_BIN="$ROOT_DIR/.venv/bin/python3"
else
    PYTHON_BIN="$(command -v python3 || true)"
fi

if [[ -z "$PYTHON_BIN" ]]; then
    echo "Error: no se encontro python3 en el sistema."
    exit 1
fi

if ! "$PYTHON_BIN" -c "import confluent_kafka, requests, dotenv" >/dev/null 2>&1; then
    echo "Error: faltan dependencias Python para los productores en: $PYTHON_BIN"
    echo "Instala dependencias con:"
    echo "  python3 -m venv .venv"
    echo "  source .venv/bin/activate"
    echo "  python3 -m pip install -r greenhouse/requirements.txt"
    exit 1
fi

if [[ -f "$ROOT_DIR/.env" ]]; then
    # shellcheck disable=SC1091
    source "$ROOT_DIR/.env"
fi

if [[ -z "${AEMET_API_KEY:-}" ]]; then
    echo "Error: AEMET_API_KEY no esta definido. Configuralo en .env o en el entorno."
    exit 1
fi

cd "$ROOT_DIR"

is_pid_active() {
    local pid_file="$1"

    if [[ ! -f "$pid_file" ]]; then
        return 1
    fi

    local pid
    pid="$(cat "$pid_file")"
    [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

start_one() {
    local name="$1"
    local pid_file="$2"
    local log_file="$3"
    shift 3

    echo "Iniciando productor $name..."
    nohup setsid "$PYTHON_BIN" -u "$@" < /dev/null >> "$log_file" 2>&1 &
    local pid=$!
    echo "$pid" > "$pid_file"

    sleep 1
    if ! kill -0 "$pid" 2>/dev/null; then
        echo "Error: productor $name se detuvo al iniciar."
        echo "Ultimas lineas de $log_file:"
        tail -n 20 "$log_file" || true
        return 1
    fi
}

ensure_running() {
    local name="$1"
    local pid_file="$2"
    local log_file="$3"
    shift 3

    if is_pid_active "$pid_file"; then
        return
    fi

    if [[ -f "$pid_file" ]]; then
        echo "Watchdog: PID stale detectado para $name, relanzando..."
    else
        echo "Watchdog: $name sin PID, lanzando..."
    fi

    start_one "$name" "$pid_file" "$log_file" "$@"
}

run_watchdog_loop() {
    echo "Watchdog activo. Intervalo de chequeo: ${WATCHDOG_SLEEP}s"

    while true; do
        ensure_running \
            "simulado" \
            "$LOG_DIR/producer_simulated.pid" \
            "$LOG_DIR/producer_simulated.log" \
            greenhouse/producer/producer_simulated.py \
            --events-per-second "$SIM_EPS"

        ensure_running \
            "csv" \
            "$LOG_DIR/producer_csv.pid" \
            "$LOG_DIR/producer_csv.log" \
            greenhouse/producer/producer_csv.py \
            --csv-path "$CSV_PATH" \
            --events-per-second "$CSV_EPS" \
            --max-events "$CSV_MAX_EVENTS"

        ensure_running \
            "aemet" \
            "$LOG_DIR/producer_aemet.pid" \
            "$LOG_DIR/producer_aemet.log" \
            greenhouse/producer/producer_aemet.py \
            --events-per-second "$AEMET_EPS" \
            --max-events "$AEMET_MAX_EVENTS" \
            --aemet-station "$AEMET_STATION_ID"

        sleep "$WATCHDOG_SLEEP"
    done
}

start_watchdog() {
    if is_pid_active "$WATCHDOG_PID_FILE"; then
        local pid
        pid="$(cat "$WATCHDOG_PID_FILE")"
        echo "Watchdog ya activo (PID=$pid)"
        return
    fi

    rm -f "$WATCHDOG_PID_FILE"
    echo "Iniciando watchdog de productores..."
    nohup setsid bash "$ROOT_DIR/scripts/start_producers.sh" --watchdog < /dev/null > "$WATCHDOG_LOG_FILE" 2>&1 &
    local pid=$!
    echo "$pid" > "$WATCHDOG_PID_FILE"

    sleep 1
    if ! kill -0 "$pid" 2>/dev/null; then
        echo "Error: watchdog no pudo iniciarse."
        echo "Ultimas lineas de $WATCHDOG_LOG_FILE:"
        tail -n 20 "$WATCHDOG_LOG_FILE" || true
        return 1
    fi
}

if [[ "${1:-}" == "--watchdog" ]]; then
    run_watchdog_loop
    exit 0
fi

start_one \
    "simulado" \
    "$LOG_DIR/producer_simulated.pid" \
    "$LOG_DIR/producer_simulated.log" \
    greenhouse/producer/producer_simulated.py \
    --events-per-second "$SIM_EPS"

start_one \
    "csv" \
    "$LOG_DIR/producer_csv.pid" \
    "$LOG_DIR/producer_csv.log" \
    greenhouse/producer/producer_csv.py \
    --csv-path "$CSV_PATH" \
    --events-per-second "$CSV_EPS" \
    --max-events "$CSV_MAX_EVENTS"

start_one \
    "aemet" \
    "$LOG_DIR/producer_aemet.pid" \
    "$LOG_DIR/producer_aemet.log" \
    greenhouse/producer/producer_aemet.py \
    --events-per-second "$AEMET_EPS" \
    --max-events "$AEMET_MAX_EVENTS" \
    --aemet-station "$AEMET_STATION_ID"

start_watchdog

echo "Productores iniciados."
echo "Python usado: $PYTHON_BIN"
echo "PIDs guardados en $LOG_DIR/*.pid"
echo "Logs en $LOG_DIR/*.log"
echo "Watchdog PID: $WATCHDOG_PID_FILE"
echo "Watchdog log: $WATCHDOG_LOG_FILE"
