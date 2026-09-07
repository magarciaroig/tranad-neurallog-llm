#!/usr/bin/env python3
"""
llm_rca_assistant.py - Asistente LLM para Root Cause Analysis (RCA)

Conecta las detecciones de anomalías y atribuciones de métricas de TranAD+
con el Linaje/Topología del DAG y los logs relevantes para generar un
diagnóstico en lenguaje natural con recomendaciones de remediación.
"""

import sys
import os
import json
import argparse
from datetime import datetime, timezone
import urllib.request
import urllib.error

from topology_mapper import (
    DATA_PIPELINE_DAG,
    METRIC_TO_COMPONENT_MAP,
    ANOMALY_GROUND_TRUTH,
    get_components_for_metrics
)

SYSTEM_PROMPT = """Eres un Ingeniero Principal de Datos y Especialista en SRE/Observabilidad de Pipelines Distribuidas de Datos.
Tu función es realizar el Análisis de Causa Raíz (RCA - Root Cause Analysis) y el diagnóstico técnico de anomalías detectadas en una pipeline de datos.

Se te proporcionará:
1. La topología y flujo del DAG del pipeline.
2. La ventana temporal y las métricas que el algoritmo de series temporales (TranAD+) identificó con alto error de reconstrucción.
3. Los logs extraídos de los componentes candidatos en dicha ventana temporal.

Debes generar un informe conciso y estructurado con el siguiente formato:
### 1. Diagnóstico de Causa Raíz (Root Cause)
- Componente origen del fallo.
- Motivo exacto del fallo basado en la evidencia de los logs y métricas.

### 2. Análisis de Propagación en Cascada (Downstream / Upstream Impact)
- Cómo afectó este problema a los componentes dependientes en el linaje de datos (retrasos, acumulación de lag, pérdida/bloqueo de registros).

### 3. Acciones Técnicas de Remediación
- Acciones inmediatas para mitigar el incidente.
- Medidas preventivas a medio/largo plazo en infraestructura o código.
"""


def extract_problematic_logs_for_components(logs_file, start_ts, end_ts, candidate_components, max_logs=25):
    """Filtra y clasifica los logs relevantes en la ventana temporal para los componentes indicados."""
    if not os.path.exists(logs_file):
        return []

    collected = []
    target_names = set(candidate_components)

    with open(logs_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue

            ts = item.get('timestamp', 0)
            if start_ts <= ts <= end_ts:
                comp = item.get('component') or item.get('service_name') or item.get('task_id')
                # Coincidencia con componentes objetivo o logs generales de error
                if comp in target_names or any(t in str(comp) for t in target_names):
                    collected.append(item)

    # Priorizar logs problemáticos (ERROR / WARNING)
    problematic = [l for l in collected if l.get('is_problematic', False) or l.get('level') in ['ERROR', 'CRITICAL']]
    warnings = [l for l in collected if l.get('level') == 'WARNING' and l not in problematic]
    others = [l for l in collected if l not in problematic and l not in warnings]

    ranked_logs = problematic + warnings + others
    return ranked_logs[:max_logs]


def build_rca_prompt(candidate_metrics, start_ts, end_ts, logs):
    """Construye el prompt estructurado para el LLM con topología, métricas y logs."""
    start_iso = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    end_iso = datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    duration = max(end_ts - start_ts, 1)

    mapping = get_components_for_metrics(candidate_metrics)
    all_targets = mapping["components"] + mapping["docker_services"] + mapping["airflow_tasks"]

    log_snippets = []
    for l in logs:
        comp = l.get('component') or l.get('service_name') or 'unknown'
        level = l.get('level', 'INFO')
        msg = l.get('message', '')
        log_snippets.append(f"[{comp}] [{level}] {msg}")

    logs_text = "\n".join(log_snippets) if log_snippets else "(No se encontraron logs de error explícitos en este intervalo)"

    user_content = f"""=== CONTEXTO DEL SISTEMA Y TOPOLOGÍA DEL PIPELINE ===
Flujo de datos: {DATA_PIPELINE_DAG['flow_description']}
Nodos del sistema: {json.dumps(list(DATA_PIPELINE_DAG['nodes'].keys()))}

=== ALERTA Y DETECCIÓN DE ANOMALÍA (TranAD+) ===
- Intervalo temporal afectado: {start_iso} a {end_iso} ({duration} segundos)
- Métricas con mayor desvío / error de reconstrucción: {', '.join(candidate_metrics)}
- Componentes candidatos identificados: {', '.join(all_targets)}

=== LOGS EXTRAÍDOS DE LOS COMPONENTES CANDIDATOS ===
{logs_text}

=== SOLICITUD ===
Realiza el diagnóstico técnico de Causa Raíz (RCA), explicando el origen, impacto en cascada y las acciones de mitigación necesarias.
"""
    return user_content


def query_gemini_api(system_instruction, prompt_text, api_key=None, model="gemini-2.5-flash"):
    """Realiza una consulta a la API de Gemini mediante HTTP POST nativo."""
    key = api_key or os.environ.get("GEMINI_API_KEY")
    if not key:
        return None, "No se encontró GEMINI_API_KEY en las variables de entorno."

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": prompt_text}]
            }
        ],
        "systemInstruction": {
            "parts": [{"text": system_instruction}]
        },
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": 2048
        }
    }

    try:
        req_data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            url,
            data=req_data,
            headers={'Content-Type': 'application/json'}
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            res_body = response.read().decode('utf-8')
            res_json = json.loads(res_body)
            candidates = res_json.get('candidates', [])
            if candidates and 'content' in candidates[0]:
                parts = candidates[0]['content'].get('parts', [])
                if parts:
                    return parts[0].get('text', ''), None
            return None, f"Respuesta inesperada de la API: {res_body}"
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode('utf-8')
        return None, f"HTTP Error {e.code}: {err_msg}"
    except Exception as e:
        return None, f"Error en la llamada a la API: {e}"


def run_assessment(candidate_metrics, start_ts, end_ts, logs_path='./tfm_dataset/dataset_logs.jsonl', model="gemini-2.5-flash", api_key=None):
    """Ejecuta el ciclo completo de evaluación de causa raíz para un incidente."""
    mapping = get_components_for_metrics(candidate_metrics)
    target_components = mapping["components"] + mapping["docker_services"] + mapping["airflow_tasks"]

    # Extraer logs
    logs = extract_problematic_logs_for_components(logs_path, start_ts, end_ts, target_components)

    # Construir Prompt
    prompt = build_rca_prompt(candidate_metrics, start_ts, end_ts, logs)

    print("================================================================================")
    print(" 🔍 PROMPT GENERADO PARA EL LLM (Root Cause Assessment)")
    print("================================================================================")
    print(prompt)
    print("================================================================================")

    # Consultar LLM si está configurada la API Key
    key = api_key or os.environ.get("GEMINI_API_KEY")
    if key:
        print(f"\n[+] Enviando prompt a la API de Gemini ({model})...\n")
        response_text, err = query_gemini_api(SYSTEM_PROMPT, prompt, api_key=key, model=model)
        if response_text:
            print("================================================================================")
            print(" 🤖 EVALUACIÓN Y DIAGNÓSTICO DEL LLM")
            print("================================================================================")
            print(response_text)
            print("================================================================================")
            return response_text
        else:
            print(f"[!] No se pudo obtener respuesta del LLM: {err}")
    else:
        print("\n[i] Para invocar directamente el modelo, define la variable de entorno GEMINI_API_KEY o usa --api-key.")

    return None


def run_benchmark_eval(benchmark_path='./tfm_dataset/rca_eval_benchmark.json', incident_id=None, logs_path='./tfm_dataset/dataset_logs.jsonl'):
    """Ejecuta la evaluación a partir de un caso del benchmark generado."""
    if not os.path.exists(benchmark_path):
        print(f"[!] No se encontró el archivo de benchmark en {benchmark_path}")
        return

    with open(benchmark_path, 'r', encoding='utf-8') as f:
        cases = json.load(f)

    if not cases:
        print("[!] El archivo de benchmark no contiene casos de prueba.")
        return

    target_cases = [c for c in cases if c['anomaly_id'] == incident_id] if incident_id else cases

    for case in target_cases:
        print(f"\n==================== EVALUANDO {case['anomaly_id']} ({case['anomaly_type']}) ====================")
        print(f"Ground Truth Causa Raíz: {case['ground_truth']['root_cause_summary']}")
        print(f"Métricas esperadas: {case['ground_truth']['expected_anomalous_metrics']}")

        run_assessment(
            candidate_metrics=case['ground_truth']['expected_anomalous_metrics'],
            start_ts=case['window']['start_epoch'],
            end_ts=case['window']['end_epoch'],
            logs_path=logs_path
        )


def main():
    parser = argparse.ArgumentParser(description="Asistente LLM para Diagnóstico de Causa Raíz (RCA)")
    parser.add_argument('--metrics', type=str, help="Métricas anómalas separadas por coma (ej: records_loaded,cpu_usage)")
    parser.add_argument('--start-time', type=int, help="Timestamp epoch de inicio de la anomalía")
    parser.add_argument('--end-time', type=int, help="Timestamp epoch de fin de la anomalía")
    parser.add_argument('--logs-file', type=str, default='./tfm_dataset/dataset_logs.jsonl', help="Ruta al archivo dataset_logs.jsonl")
    parser.add_argument('--benchmark-file', type=str, default='./tfm_dataset/rca_eval_benchmark.json', help="Ruta al archivo rca_eval_benchmark.json")
    parser.add_argument('--incident-id', type=str, help="Evaluar un caso específico del benchmark (ej: incident_000)")
    parser.add_argument('--api-key', type=str, help="API Key de Gemini (opcional)")
    parser.add_argument('--model', type=str, default="gemini-2.5-flash", help="Modelo de Gemini a usar")

    args = parser.parse_args()

    if args.incident_id or (not args.metrics and os.path.exists(args.benchmark_file)):
        run_benchmark_eval(args.benchmark_file, args.incident_id, args.logs_file)
    elif args.metrics and args.start_time:
        metrics_list = [m.strip() for m in args.metrics.split(',')]
        end_time = args.end_time or (args.start_time + 300)
        run_assessment(metrics_list, args.start_time, end_time, args.logs_file, args.model, args.api_key)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
