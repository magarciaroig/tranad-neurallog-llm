import json
import pandas as pd
import os
from datetime import datetime, timezone
from topology_mapper import (
    DATA_PIPELINE_DAG,
    METRIC_TO_COMPONENT_MAP,
    ANOMALY_GROUND_TRUTH,
    get_components_for_metrics
)

# Rutas de métricas del Emisor (Emitter)
EMITTER_SENT_JSON_PATH = './tfm_dataset/emitter_records_sent.json'
EMITTER_LATENCY_JSON_PATH = './tfm_dataset/emitter_latency.json'
EMITTER_STATUS_JSON_PATH = './tfm_dataset/emitter_status.json'
EMITTER_CPU_JSON_PATH = './tfm_dataset/emitter_cpu_usage.json'
EMITTER_MEM_JSON_PATH = './tfm_dataset/emitter_memory_usage.json'

# Rutas de métricas de Airflow, DW y Kinesis
CPU_JSON_PATH = './tfm_dataset/cpu_usage.json'
MEM_JSON_PATH = './tfm_dataset/memory_usage.json'
EXTRACTED_JSON_PATH = './tfm_dataset/records_extracted.json'
LOADED_JSON_PATH = './tfm_dataset/records_loaded.json'
LAG_JSON_PATH = './tfm_dataset/kinesis_lag.json'

# Rutas de eventos de caos, logs y salidas
EVENTS_CSV_PATH = './tfm_dataset/chaos_events.csv'
RAW_LOGS_PATH = './tfm_dataset/raw_logs.jsonl'
OUTPUT_CSV_PATH = './tfm_dataset/tranad_dataset.csv'
OUTPUT_LOGS_PATH = './tfm_dataset/dataset_logs.jsonl'
OUTPUT_BENCHMARK_PATH = './tfm_dataset/rca_eval_benchmark.json'


def parse_prometheus_json(json_file_path, value_column_name):
    """Lee un JSON de Prometheus y devuelve un DataFrame con timestamp y el valor."""
    if not os.path.exists(json_file_path):
        return pd.DataFrame(columns=['timestamp', value_column_name])
        
    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[!] Advertencia: No se pudo leer {json_file_path}: {e}")
        return pd.DataFrame(columns=['timestamp', value_column_name])
        
    if not isinstance(data, dict) or data.get('status') != 'success' or not data.get('data') or not data['data'].get('result'):
        return pd.DataFrame(columns=['timestamp', value_column_name])
        
    result_item = data['data']['result'][0]
    
    # Manejar formato 'matrix' (query_range con 'values')
    if 'values' in result_item:
        values = result_item['values']
    # Manejar formato 'vector' (instant queries con 'value')
    elif 'value' in result_item:
        values = [result_item['value']]
    else:
        return pd.DataFrame(columns=['timestamp', value_column_name])
        
    df = pd.DataFrame(values, columns=['timestamp', value_column_name])
    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
    df.dropna(subset=['timestamp'], inplace=True)
    df['timestamp'] = df['timestamp'].astype(float).astype(int)
    df[value_column_name] = pd.to_numeric(df[value_column_name], errors='coerce').fillna(0.0)
    
    return df


def label_logs(raw_logs_path, df_events, output_logs_path):
    """Asigna etiquetas de anomalía y tipo de anomalía a cada línea de log."""
    if not os.path.exists(raw_logs_path):
        print(f"[i] No se encontró {raw_logs_path}, omitiendo etiquetado de logs.")
        return []

    logs = []
    with open(raw_logs_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    logs.append(json.loads(line))
                except Exception:
                    pass

    if not logs:
        return []

    # Ordenar eventos cronológicamente
    has_events = df_events is not None and not df_events.empty

    labeled_logs = []
    for log in logs:
        ts = log.get('timestamp', 0)
        is_anomaly = 0
        anomaly_type = 'none'

        if has_events:
            past_events = df_events[df_events['timestamp'] <= ts]
            if not past_events.empty:
                last_event = past_events.iloc[-1]
                if last_event['action'] == 'START':
                    is_anomaly = 1
                    anomaly_type = last_event['anomaly_type']

        log_labeled = dict(log)
        log_labeled['is_anomaly'] = is_anomaly
        log_labeled['anomaly_type'] = anomaly_type
        labeled_logs.append(log_labeled)

    with open(output_logs_path, 'w', encoding='utf-8') as f:
        for item in labeled_logs:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')

    print(f"[+] Logs etiquetados y guardados en {output_logs_path} ({len(labeled_logs)} líneas).")
    return labeled_logs


def generate_rca_eval_benchmark(df_events, df_metrics, labeled_logs, output_benchmark_path):
    """
    Genera un conjunto de evaluación estructurado para medir la calidad del LLM
    en el diagnóstico de Causa Raíz (RCA).
    """
    if df_events is None or df_events.empty:
        print("[i] No hay eventos de caos registrados, omitiendo benchmark RCA.")
        return

    start_events = df_events[df_events['action'] == 'START'].reset_index(drop=True)
    if start_events.empty:
        return

    benchmark_cases = []

    for idx, row in start_events.iterrows():
        t_start = int(row['timestamp'])
        a_type = str(row['anomaly_type'])

        # Localizar el siguiente END o asignar ventana de 10 minutos
        end_events = df_events[(df_events['timestamp'] > t_start) & (df_events['action'] == 'END')]
        if not end_events.empty:
            t_end = int(end_events.iloc[0]['timestamp'])
        else:
            t_end = t_start + 600

        duration = max(t_end - t_start, 1)

        # Información Ground Truth
        gt_info = ANOMALY_GROUND_TRUTH.get(a_type, {
            "category": "UNKNOWN",
            "description": f"Anomalía de tipo {a_type}",
            "expected_anomalous_metrics": [],
            "affected_components": [],
            "remediation_actions": []
        })

        expected_metrics = gt_info.get("expected_anomalous_metrics", [])
        affected_comps = gt_info.get("affected_components", [])
        
        # Obtener mapeo topológico para los componentes sospechosos
        topo_mapping = get_components_for_metrics(expected_metrics)

        # Filtrar logs de la ventana
        window_logs = [l for l in labeled_logs if t_start <= l.get('timestamp', 0) <= t_end]
        problematic_logs = [l for l in window_logs if l.get('is_problematic', False)]

        # Filtrar logs relevantes de componentes sospechosos
        candidate_component_names = set(affected_comps + topo_mapping["components"] + topo_mapping["docker_services"] + topo_mapping["airflow_tasks"])
        relevant_problem_logs = [
            l for l in problematic_logs
            if l.get('component') in candidate_component_names or l.get('service_name') in candidate_component_names or l.get('task_id') in candidate_component_names
        ]

        # Formato legible de fechas
        start_iso = datetime.fromtimestamp(t_start, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        end_iso = datetime.fromtimestamp(t_end, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

        # Formatear muestra de logs para el prompt
        log_sample_lines = []
        for l in relevant_problem_logs[:15]:
            comp = l.get('component') or l.get('service_name') or 'system'
            log_sample_lines.append(f"[{comp}] [{l.get('level', 'INFO')}] {l.get('message')}")
        
        log_snippets_text = "\n".join(log_sample_lines) if log_sample_lines else "(No se registraron excepciones explícitas en logs durante esta ventana)"

        # Construir plantilla del prompt para el LLM
        prompt_preview = f"""### CONTEXTO DE ARQUITECTURA (DAG TOPOLOGY):
Flujo del Pipeline: {DATA_PIPELINE_DAG['flow_description']}

### DETECCIÓN DE ANOMALÍA (TranAD+):
- Ventana temporal: {start_iso} - {end_iso} (Duración: {duration}s)
- Métricas con alto error de reconstrucción: {', '.join(expected_metrics) if expected_metrics else 'N/A'}
- Componentes candidatos identificados por Linaje/Topología: {', '.join(affected_comps)}

### LOGS EXTRAÍDOS DE COMPONENTES CANDIDATOS (Nivel ERROR/WARN):
{log_snippets_text}

### INSTRUCCIONES PARA EL DIAGNÓSTICO (RCA):
1. Determina la causa raíz exacta del problema en el pipeline.
2. Explica la propagación del fallo en cascada (upstream vs downstream).
3. Propone las acciones técnicas inmediatas de remediación para restablecer el servicio.
"""

        case = {
            "anomaly_id": f"incident_{idx:03d}",
            "anomaly_type": a_type,
            "category": gt_info.get("category"),
            "window": {
                "start_epoch": t_start,
                "end_epoch": t_end,
                "start_iso": start_iso,
                "end_iso": end_iso,
                "duration_seconds": duration
            },
            "ground_truth": {
                "root_cause_summary": gt_info.get("description"),
                "expected_anomalous_metrics": expected_metrics,
                "primary_affected_components": affected_comps,
                "expected_cascade_effects": gt_info.get("expected_cascade_effects", ""),
                "recommended_remediations": gt_info.get("remediation_actions")
            },
            "evidence": {
                "total_window_logs": len(window_logs),
                "problematic_logs_count": len(problematic_logs),
                "relevant_problem_logs_sample": relevant_problem_logs[:20]
            },
            "llm_prompt_template": prompt_preview
        }
        benchmark_cases.append(case)

    with open(output_benchmark_path, 'w', encoding='utf-8') as f:
        json.dump(benchmark_cases, f, indent=2, ensure_ascii=False)

    print(f"[+] Benchmark RCA generado en {output_benchmark_path} ({len(benchmark_cases)} casos de prueba).")


def main():
    print("Iniciando procesamiento bimodal del dataset (Métricas + Logs + Benchmark RCA)...")

    # 1. Cargar métricas del Emisor
    df_emit_sent = parse_prometheus_json(EMITTER_SENT_JSON_PATH, 'emitter_records_sent')
    df_emit_lat = parse_prometheus_json(EMITTER_LATENCY_JSON_PATH, 'emitter_latency')
    df_emit_status = parse_prometheus_json(EMITTER_STATUS_JSON_PATH, 'emitter_status')
    df_emit_cpu = parse_prometheus_json(EMITTER_CPU_JSON_PATH, 'emitter_cpu_usage')
    df_emit_mem = parse_prometheus_json(EMITTER_MEM_JSON_PATH, 'emitter_memory_usage')

    # 2. Cargar métricas de Airflow y DW
    df_cpu = parse_prometheus_json(CPU_JSON_PATH, 'cpu_usage')
    df_mem = parse_prometheus_json(MEM_JSON_PATH, 'memory_usage')
    df_ext = parse_prometheus_json(EXTRACTED_JSON_PATH, 'records_extracted')
    df_load = parse_prometheus_json(LOADED_JSON_PATH, 'records_loaded')
    df_lag = parse_prometheus_json(LAG_JSON_PATH, 'kinesis_lag_ms')

    # 3. Unir todas por timestamp
    dfs = [
        df_emit_status, df_emit_sent, df_emit_lat, df_emit_cpu, df_emit_mem,
        df_cpu, df_mem, df_ext, df_load, df_lag
    ]
    
    non_empty_dfs = [df for df in dfs if not df.empty]
    
    if not non_empty_dfs:
        print("[!] No se encontraron métricas válidas en los ficheros JSON.")
        df_metrics = pd.DataFrame()
    else:
        df_metrics = non_empty_dfs[0]
        for df in non_empty_dfs[1:]:
            df_metrics = pd.merge(df_metrics, df, on='timestamp', how='outer')

        df_metrics.sort_values('timestamp', inplace=True)
        df_metrics.reset_index(drop=True, inplace=True)
        
        # Forward fill y luego ceros
        df_metrics.ffill(inplace=True)
        df_metrics.fillna(0, inplace=True) 

        expected_feature_cols = [
            'emitter_status', 'emitter_records_sent', 'emitter_latency',
            'emitter_cpu_usage', 'emitter_memory_usage',
            'cpu_usage', 'memory_usage',
            'records_extracted', 'records_loaded', 'kinesis_lag_ms'
        ]
        for col in expected_feature_cols:
            if col not in df_metrics.columns:
                df_metrics[col] = 0.0

        df_metrics['is_anomaly'] = 0
        df_metrics['anomaly_type'] = 'none'

    # 4. Cargar eventos de Caos
    df_events = None
    if os.path.exists(EVENTS_CSV_PATH):
        df_events = pd.read_csv(EVENTS_CSV_PATH)

    # 5. Aplicar etiquetas a métricas
    if not df_metrics.empty and df_events is not None and not df_events.empty:
        for i, row in df_metrics.iterrows():
            ts = row['timestamp']
            past_events = df_events[df_events['timestamp'] <= ts]
            if not past_events.empty:
                last_event = past_events.iloc[-1]
                if last_event['action'] == 'START':
                    df_metrics.at[i, 'is_anomaly'] = 1
                    df_metrics.at[i, 'anomaly_type'] = last_event['anomaly_type']

    if not df_metrics.empty:
        ordered_cols = ['timestamp'] + expected_feature_cols + ['is_anomaly', 'anomaly_type']
        remaining_cols = [c for c in df_metrics.columns if c not in ordered_cols]
        df_metrics = df_metrics[ordered_cols + remaining_cols]
        df_metrics.to_csv(OUTPUT_CSV_PATH, index=False)
        print(f"¡Éxito! Dataset de métricas guardado en: {OUTPUT_CSV_PATH} ({len(df_metrics)} filas, {len(df_metrics.columns)} columnas)")

    # 6. Procesar y etiquetar logs
    labeled_logs = label_logs(RAW_LOGS_PATH, df_events, OUTPUT_LOGS_PATH)

    # 7. Generar benchmark de evaluación RCA para el LLM
    generate_rca_eval_benchmark(df_events, df_metrics, labeled_logs, OUTPUT_BENCHMARK_PATH)


if __name__ == '__main__':
    main()