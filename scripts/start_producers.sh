#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"

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

start_one() {
    local name="$1"
    local pid_file="$2"
    local log_file="$3"
    shift 3

    echo "Iniciando productor $name..."
    nohup "$PYTHON_BIN" -u "$@" > "$log_file" 2>&1 &
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

echo "Productores iniciados."
echo "Python usado: $PYTHON_BIN"
echo "PIDs guardados en $LOG_DIR/*.pid"
echo "Logs en $LOG_DIR/*.log"
