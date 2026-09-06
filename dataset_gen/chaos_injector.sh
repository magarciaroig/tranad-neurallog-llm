#!/bin/bash
# Script de Chaos Engineering con Etiquetado Automático y Resolución Dinámica de Contenedores

ENGINE="docker"
COMPOSE="docker compose"

EVENT_LOG="./tfm_dataset/chaos_events.csv"
mkdir -p ./tfm_dataset
# Crear cabecera si el archivo no existe
if [ ! -f "$EVENT_LOG" ]; then
    echo "timestamp,action,anomaly_type" > "$EVENT_LOG"
fi

log_event() {
    local action=$1
    local type=$2
    local current_time=$(date +%s)
    echo "$current_time,$action,$type" >> "$EVENT_LOG"
}

get_container_id() {
    local service_name=$1
    local fallback_name=$2
    local cid=$($COMPOSE ps -q "$service_name" 2>/dev/null)
    if [ -z "$cid" ]; then
        cid="$fallback_name"
    fi
    echo "$cid"
}

echo "=== INYECCIÓN DE ANOMALÍAS (Motor detectado: $ENGINE) ==="
echo "1) Estrés de CPU en Scheduler"
echo "2) Caída del Data Warehouse"
echo "3) Detener Microservicio Emisor"
echo "4) Interrumpir AWS LocalStack"
echo "5) Restaurar estado normal"
read -p "Opción: " opcion

case $opcion in
  1)
    echo "[!] Limitando CPU..."
    CID=$(get_container_id "airflow-scheduler" "dataset_gen-airflow-scheduler-1")
    if $ENGINE update --cpus="0.1" "$CID"; then
        log_event "START" "cpu_stress"
        echo "-> Anomalía (cpu_stress) inyectada y registrada en $CID."
    else
        echo "[X] Error al aplicar estrés de CPU."
    fi
    ;;
  2)
    echo "[!] Pausando el Data Warehouse..."
    CID=$(get_container_id "data_warehouse" "dataset_gen-data_warehouse-1")
    if $ENGINE pause "$CID"; then
        log_event "START" "dw_timeout"
        echo "-> Anomalía (dw_timeout) inyectada y registrada en $CID."
    else
        echo "[X] Error al pausar el Data Warehouse."
    fi
    ;;
  3)
    echo "[!] Deteniendo el Emisor..."
    CID=$(get_container_id "emitter" "kinesis_emitter")
    if $ENGINE stop "$CID"; then
        log_event "START" "emitter_down"
        echo "-> Anomalía (emitter_down) inyectada y registrada en $CID."
    else
        echo "[X] Error al detener el Emisor."
    fi
    ;;
  4)
    echo "[!] Pausando AWS LocalStack..."
    CID=$(get_container_id "localstack" "localstack")
    if $ENGINE pause "$CID"; then
        log_event "START" "aws_down"
        echo "-> Anomalía (aws_down) inyectada y registrada en $CID."
    else
        echo "[X] Error al pausar LocalStack."
    fi
    ;;
  5)
    echo "[+] Restaurando sistema..."
    CID_SCHED=$(get_container_id "airflow-scheduler" "dataset_gen-airflow-scheduler-1")
    CID_DW=$(get_container_id "data_warehouse" "dataset_gen-data_warehouse-1")
    CID_EMIT=$(get_container_id "emitter" "kinesis_emitter")
    CID_LOCAL=$(get_container_id "localstack" "localstack")

    $ENGINE update --cpus="0.0" "$CID_SCHED" 2>/dev/null || true
    $ENGINE unpause "$CID_DW" 2>/dev/null || true
    $ENGINE start "$CID_EMIT" 2>/dev/null || true
    $ENGINE unpause "$CID_LOCAL" 2>/dev/null || true
    log_event "END" "none"
    echo "-> Entorno estabilizado y registrado."
    ;;
  *)
    echo "Opción no válida."
    ;;
esac