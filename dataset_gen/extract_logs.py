#!/usr/bin/env python3
"""
extract_logs.py - Extractor de logs estructurados sincronizado para Docker y Airflow

Extrae, normaliza y estructura los logs generados por los contenedores Docker
y las tareas de Airflow en una ventana de tiempo [START_TIME, END_TIME]
en formato JSON estructurado optimizado para HitAnomaly y LLM RCA.
"""

import sys
import os
import json
import re
import subprocess
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_LOGS_PATH = os.path.join(BASE_DIR, 'tfm_dataset', 'raw_logs.jsonl')
AIRFLOW_LOGS_DIR = os.path.join(BASE_DIR, 'logs')

DEFAULT_CONTAINERS = [
    'kinesis_emitter',
    'data_warehouse',
    'localstack',
    'postgres',
    'pushgateway',
    'cadvisor'
]

# Patrones de respaldo para detectar errores en servicios sin logging estructurado (Postgres, etc.)
ERROR_PATTERNS = [
    r'\bERROR\b',
    r'\bCRITICAL\b',
    r'\bFATAL\b',
    r'\bEXCEPTION\b',
    r'\bTRACEBACK\b',
    r'\bTIMEOUT\b',
    r'\bFAILED\b',
    r'\bFAIL\b',
    r'\bCONNECTION\s+REFUSED\b',
    r'\bBROKEN\s+PIPE\b',
    r'\bTASK\s+FAILED\b',
    r'\bMARKING\s+TASK\s+AS\s+FAILED\b',
    r'\bCLIENTERROR\b',
    r'\bENDPOINTCONNECTIONERROR\b',
    r'\bOPERATIONALERROR\b',
    r'\bDEADLOCK\b'
]
ERROR_REGEX = re.compile('|'.join(ERROR_PATTERNS), re.IGNORECASE)
WARN_REGEX = re.compile(r'\b(WARNING|WARN|RETRY|RETRIES|DEPRECATED)\b', re.IGNORECASE)


def try_parse_json(text):
    """Intenta parsear un texto o fragmento como JSON estructurado."""
    if not text:
        return None
    text = text.strip()
    if text.startswith('{') and text.endswith('}'):
        try:
            return json.loads(text)
        except Exception:
            pass
    # Buscar '{' que pueda contener un objeto JSON válido (evita prefijos como {file.py:line} de Airflow)
    for match in re.finditer(r'\{', text):
        start_idx = match.start()
        end_idx = text.rfind('}')
        if start_idx < end_idx:
            try:
                candidate = text[start_idx:end_idx + 1]
                obj = json.loads(candidate)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                continue
    return None


def parse_docker_timestamp(ts_str):
    """Convierte un timestamp ISO8601 de Docker a epoch en segundos."""
    try:
        clean_ts = ts_str.strip()
        if '.' in clean_ts:
            base, frac = clean_ts.split('.', 1)
            frac_clean = re.sub(r'[^0-9]', '', frac)[:6].ljust(6, '0')
            tz_part = '+00:00' if 'Z' in clean_ts or '+00:00' in clean_ts else '+00:00'
            iso_fmt = f"{base}.{frac_clean}{tz_part}"
            dt = datetime.fromisoformat(iso_fmt)
        else:
            base = clean_ts.replace('Z', '+00:00')
            dt = datetime.fromisoformat(base)
        return int(dt.timestamp())
    except Exception:
        return None


def normalize_component_name(container_name):
    """Mapea nombres de contenedores a nombres canónicos de componentes del sistema."""
    name = container_name.lower()
    if 'emitter' in name:
        return 'emitter'
    elif 'warehouse' in name or 'analytics' in name:
        return 'data_warehouse'
    elif 'localstack' in name:
        return 'localstack'
    elif 'scheduler' in name:
        return 'airflow-scheduler'
    elif 'webserver' in name:
        return 'airflow-webserver'
    elif 'postgres' in name:
        return 'postgres'
    elif 'pushgateway' in name:
        return 'pushgateway'
    elif 'cadvisor' in name:
        return 'cadvisor'
    return container_name


def get_all_active_docker_containers():
    """Detecta todos los contenedores activos del proyecto."""
    containers = list(DEFAULT_CONTAINERS)
    try:
        result = subprocess.run(
            ['docker', 'ps', '-a', '--format', '{{.Names}}'],
            capture_output=True, text=True, check=False
        )
        if result.returncode == 0:
            for name in result.stdout.strip().splitlines():
                name = name.strip()
                if name and name not in containers:
                    if any(k in name for k in ['airflow', 'emitter', 'warehouse', 'localstack', 'postgres']):
                        containers.append(name)
    except Exception as e:
        print(f"[!] Advertencia al buscar contenedores Docker: {e}")
    return containers


def extract_docker_logs(start_ts, end_ts):
    """Extrae logs estructurados y sin procesar de los contenedores Docker."""
    logs = []
    containers = get_all_active_docker_containers()
    
    for container in containers:
        try:
            cmd = ['docker', 'logs', '--timestamps', f'--since={start_ts}', container]
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            output = (result.stdout or '') + '\n' + (result.stderr or '')
            
            for line in output.splitlines():
                line = line.strip()
                if not line:
                    continue
                    
                parts = line.split(' ', 1)
                if len(parts) < 2:
                    continue
                    
                ts_str, payload = parts[0], parts[1]
                epoch_ts = parse_docker_timestamp(ts_str)
                
                # Intentar parseo de log estructurado en JSON
                parsed_json = try_parse_json(payload)
                if parsed_json and isinstance(parsed_json, dict):
                    entry_epoch = parsed_json.get('epoch') or epoch_ts or int(datetime.now().timestamp())
                    if entry_epoch < start_ts or entry_epoch > end_ts:
                        continue
                        
                    level = parsed_json.get('level', 'INFO').upper()
                    is_problem = level in ['ERROR', 'CRITICAL', 'FATAL'] or bool(ERROR_REGEX.search(parsed_json.get('message', '')))
                    
                    logs.append({
                        "timestamp": entry_epoch,
                        "iso_timestamp": parsed_json.get('timestamp') or datetime.fromtimestamp(entry_epoch, tz=timezone.utc).isoformat(),
                        "level": level,
                        "component": parsed_json.get('component') or normalize_component_name(container),
                        "service_name": parsed_json.get('service_name') or container,
                        "host": parsed_json.get('host') or container,
                        "thread": parsed_json.get('thread', 'MainThread'),
                        "process": parsed_json.get('process', 1),
                        "dag_id": parsed_json.get('dag_id'),
                        "task_id": parsed_json.get('task_id'),
                        "run_id": parsed_json.get('run_id'),
                        "trace_id": parsed_json.get('trace_id'),
                        "template": parsed_json.get('template') or parsed_json.get('message', payload),
                        "message": parsed_json.get('message', payload),
                        "attributes": parsed_json.get('attributes', {}),
                        "exception": parsed_json.get('exception'),
                        "is_problematic": is_problem,
                        "source": "docker_container"
                    })
                else:
                    if epoch_ts is None or epoch_ts < start_ts or epoch_ts > end_ts:
                        continue
                        
                    is_problem = bool(ERROR_REGEX.search(payload))
                    is_warn = bool(WARN_REGEX.search(payload))
                    level = "ERROR" if is_problem else ("WARNING" if is_warn else "INFO")
                    comp = normalize_component_name(container)
                    
                    logs.append({
                        "timestamp": epoch_ts,
                        "iso_timestamp": datetime.fromtimestamp(epoch_ts, tz=timezone.utc).isoformat(),
                        "level": level,
                        "component": comp,
                        "service_name": container,
                        "host": container,
                        "thread": "MainThread",
                        "process": 1,
                        "dag_id": None,
                        "task_id": None,
                        "run_id": None,
                        "trace_id": None,
                        "template": payload,
                        "message": payload,
                        "attributes": {},
                        "exception": None,
                        "is_problematic": is_problem,
                        "source": "docker_container"
                    })
        except Exception as e:
            print(f"[!] No se pudieron extraer logs de Docker para {container}: {e}")
            
    return logs


def parse_airflow_log_timestamp(line):
    """Parsea el timestamp del log de Airflow en formato [YYYY-MM-DDTHH:MM:SS.sss+0000] o similar."""
    match1 = re.match(r'^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:\+\d{4}|Z)?)\]', line)
    if match1:
        raw_ts = match1.group(1)
        try:
            if re.search(r'\+\d{4}$', raw_ts):
                raw_ts = raw_ts[:-2] + ':' + raw_ts[-2:]
            dt = datetime.fromisoformat(raw_ts.replace('Z', '+00:00'))
            return int(dt.timestamp())
        except Exception:
            pass

    match2 = re.match(r'^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
    if match2:
        try:
            dt = datetime.strptime(match2.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            pass

    return None


def extract_airflow_task_logs(start_ts, end_ts):
    """Recorre el directorio de logs de Airflow y extrae logs estructurados de las tareas."""
    logs = []
    if not os.path.exists(AIRFLOW_LOGS_DIR):
        return logs

    for root, _, files in os.walk(AIRFLOW_LOGS_DIR):
        for file in files:
            if not file.endswith('.log'):
                continue
            file_path = os.path.join(root, file)
            
            dag_match = re.search(r'dag_id=([^/]+)', file_path)
            task_match = re.search(r'task_id=([^/]+)', file_path)
            run_match = re.search(r'run_id=([^/]+)', file_path)
            
            dag_id = dag_match.group(1) if dag_match else 'airflow_dag'
            task_id = task_match.group(1) if task_match else 'airflow_task'
            run_id = run_match.group(1) if run_match else 'unknown_run'

            try:
                with open(file_path, 'r', errors='ignore') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        
                        epoch_ts = parse_airflow_log_timestamp(line)
                        
                        # Buscar si la línea contiene un JSON emitido por log_structured
                        parsed_json = try_parse_json(line)
                        if parsed_json and isinstance(parsed_json, dict):
                            entry_epoch = parsed_json.get('epoch') or epoch_ts or int(datetime.now().timestamp())
                            if entry_epoch < start_ts or entry_epoch > end_ts:
                                continue
                                
                            level = parsed_json.get('level', 'INFO').upper()
                            is_problem = level in ['ERROR', 'CRITICAL', 'FATAL'] or bool(ERROR_REGEX.search(parsed_json.get('message', '')))
                            
                            logs.append({
                                "timestamp": entry_epoch,
                                "iso_timestamp": parsed_json.get('timestamp') or datetime.fromtimestamp(entry_epoch, tz=timezone.utc).isoformat(),
                                "level": level,
                                "component": parsed_json.get('component') or task_id,
                                "service_name": "airflow",
                                "host": parsed_json.get('host') or "airflow-worker",
                                "thread": parsed_json.get('thread', 'MainThread'),
                                "process": parsed_json.get('process', 1),
                                "dag_id": parsed_json.get('dag_id') or dag_id,
                                "task_id": parsed_json.get('task_id') or task_id,
                                "run_id": parsed_json.get('run_id') or run_id,
                                "trace_id": parsed_json.get('trace_id'),
                                "template": parsed_json.get('template') or parsed_json.get('message', line),
                                "message": parsed_json.get('message', line),
                                "attributes": parsed_json.get('attributes', {}),
                                "exception": parsed_json.get('exception'),
                                "is_problematic": is_problem,
                                "source": "airflow_task_log"
                            })
                        else:
                            if epoch_ts is None or epoch_ts < start_ts or epoch_ts > end_ts:
                                continue
                                
                            is_problem = bool(ERROR_REGEX.search(line))
                            is_warn = bool(WARN_REGEX.search(line))
                            level = "ERROR" if is_problem else ("WARNING" if is_warn else "INFO")
                            
                            logs.append({
                                "timestamp": epoch_ts,
                                "iso_timestamp": datetime.fromtimestamp(epoch_ts, tz=timezone.utc).isoformat(),
                                "level": level,
                                "component": task_id,
                                "service_name": "airflow",
                                "host": "airflow-worker",
                                "thread": "MainThread",
                                "process": 1,
                                "dag_id": dag_id,
                                "task_id": task_id,
                                "run_id": run_id,
                                "trace_id": None,
                                "template": line,
                                "message": line,
                                "attributes": {},
                                "exception": None,
                                "is_problematic": is_problem,
                                "source": "airflow_task_log"
                            })
            except Exception as e:
                print(f"[!] Error leyendo fichero de log {file_path}: {e}")

    return logs


def main():
    current_time = int(datetime.now().timestamp())
    start_ts = int(sys.argv[1]) if len(sys.argv) > 1 else (current_time - 10800)
    end_ts = int(sys.argv[2]) if len(sys.argv) > 2 else current_time
    output_path = sys.argv[3] if len(sys.argv) > 3 else OUTPUT_LOGS_PATH

    print("==========================================================")
    print(" Extrayendo logs estructurados de Docker y Airflow")
    print(f" Rango: {start_ts} a {end_ts}")
    print("==========================================================")

    docker_logs = extract_docker_logs(start_ts, end_ts)
    print(f"[+] Extraídas {len(docker_logs)} líneas de logs desde contenedores Docker.")

    airflow_logs = extract_airflow_task_logs(start_ts, end_ts)
    print(f"[+] Extraídas {len(airflow_logs)} líneas de logs desde tareas de Airflow.")

    all_logs = docker_logs + airflow_logs
    all_logs.sort(key=lambda x: (x['timestamp'], x['source']))

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        for log in all_logs:
            f.write(json.dumps(log, ensure_ascii=False) + '\n')

    prob_count = sum(1 for l in all_logs if l['is_problematic'])
    print(f"¡Éxito! Total de logs guardados: {len(all_logs)} (Logs problemáticos detectados: {prob_count})")
    print(f"Archivo de salida: {output_path}")


if __name__ == '__main__':
    main()
