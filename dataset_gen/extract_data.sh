#!/bin/bash
# extract_data.sh - Extrae métricas desde Prometheus en formato JSON

PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"
OUTPUT_DIR="./tfm_dataset"
mkdir -p "$OUTPUT_DIR"

# Parámetros de rango temporal (por defecto las últimas 3 horas con paso de 5s)
CURRENT_TIME=$(date +%s)
START_TIME="${1:-$(($CURRENT_TIME - 10800))}"
END_TIME="${2:-$CURRENT_TIME}"
STEP="${3:-5s}"

echo "=========================================================="
echo " Extrayendo métricas de Prometheus ($PROMETHEUS_URL)"
echo " Rango: $START_TIME a $END_TIME (Paso: $STEP)"
echo "=========================================================="

fetch_metric() {
  local description="$1"
  local query="$2"
  local output_file="$3"

  echo "[+] Descargando métrica: $description..."
  curl -s -G "${PROMETHEUS_URL}/api/v1/query_range" \
    --data-urlencode "query=${query}" \
    --data-urlencode "start=${START_TIME}" \
    --data-urlencode "end=${END_TIME}" \
    --data-urlencode "step=${STEP}" \
    > "${OUTPUT_DIR}/${output_file}"
}

# --- MÉTRICAS DE EMITTER (Kinesis Emitter) ---
fetch_metric "Registros Enviados por Emisor" \
  "emitter_records_sent_total" \
  "emitter_records_sent.json"

fetch_metric "Latencia del Emisor (s)" \
  "emitter_latency_seconds" \
  "emitter_latency.json"

fetch_metric "Estado de Salud del Emisor (1=OK, 0=KO)" \
  "emitter_status" \
  "emitter_status.json"

fetch_metric "Consumo de CPU del Emisor" \
  'sum(rate(container_cpu_usage_seconds_total{image=~".*python:3.13-slim.*"}[1m]))' \
  "emitter_cpu_usage.json"

fetch_metric "Consumo de Memoria del Emisor" \
  'sum(container_memory_usage_bytes{image=~".*python:3.13-slim.*"})' \
  "emitter_memory_usage.json"

# --- MÉTRICAS DE AIRFLOW & INFRAESTRUCTURA ---
fetch_metric "Consumo de CPU de Airflow" \
  'sum(rate(container_cpu_usage_seconds_total{image=~".*apache/airflow:2.7.1.*"}[1m]))' \
  "cpu_usage.json"

fetch_metric "Consumo de Memoria de Airflow" \
  'sum(container_memory_usage_bytes{image=~".*apache/airflow:2.7.1.*"})' \
  "memory_usage.json"

# --- MÉTRICAS DEL PIPELINE DE DATOS ---
fetch_metric "Registros Extraídos de Kinesis" \
  "tfm_records_extracted" \
  "records_extracted.json"

fetch_metric "Registros Escritos en DW" \
  "tfm_records_loaded" \
  "records_loaded.json"

fetch_metric "Tamaño del Stream Kinesis (Lag en ms)" \
  "tfm_kinesis_stream_lag_ms" \
  "kinesis_lag.json"

# --- EXTRACCIÓN SINCRONIZADA DE LOGS (Docker & Airflow) ---
echo "=========================================================="
echo " Extrayendo logs de servicios y tareas para RCA..."
python3 extract_logs.py "$START_TIME" "$END_TIME" "${OUTPUT_DIR}/raw_logs.jsonl"

echo "=========================================================="
echo " Extracción completa (métricas + logs) en ${OUTPUT_DIR}/"
echo "=========================================================="