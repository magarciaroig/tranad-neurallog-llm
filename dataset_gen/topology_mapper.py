"""
topology_mapper.py - Topología y Linaje del Pipeline de Datos

Define la arquitectura, linaje del DAG, y el mapeo determinista entre
métricas de series temporales (TranAD+), componentes/contenedores y fuentes de logs.
"""

# Definición de la topología del grafo y linaje
DATA_PIPELINE_DAG = {
    "nodes": {
        "emitter": {
            "type": "data_source",
            "service_name": "kinesis_emitter",
            "description": "Microservicio emisor de datos de sensores a Kinesis Stream",
            "downstream": ["kinesis_stream"]
        },
        "kinesis_stream": {
            "type": "message_broker",
            "service_name": "localstack",
            "description": "Stream Kinesis (AWS LocalStack)",
            "downstream": ["extract_task"]
        },
        "extract_task": {
            "type": "airflow_task",
            "service_name": "airflow-scheduler",
            "dag_id": "tfm_distributed_pipeline",
            "task_id": "extract_task",
            "description": "Extracción de registros desde Kinesis Stream (boto3)",
            "downstream": ["transform_task"]
        },
        "transform_task": {
            "type": "airflow_task",
            "service_name": "airflow-scheduler",
            "dag_id": "tfm_distributed_pipeline",
            "task_id": "transform_task",
            "description": "Validación y transformación de datos",
            "downstream": ["load_task"]
        },
        "load_task": {
            "type": "airflow_task",
            "service_name": "airflow-scheduler",
            "dag_id": "tfm_distributed_pipeline",
            "task_id": "load_task",
            "description": "Carga de datos transformados en Data Warehouse PostgreSQL (psycopg2)",
            "downstream": ["data_warehouse"]
        },
        "data_warehouse": {
            "type": "database",
            "service_name": "data_warehouse",
            "description": "Base de datos PostgreSQL destino (analytics_db)",
            "downstream": []
        },
        "airflow_infra": {
            "type": "infrastructure",
            "service_name": "airflow-scheduler",
            "description": "Scheduler y Worker de Airflow",
            "downstream": ["extract_task", "transform_task", "load_task"]
        }
    },
    "flow_description": "emitter -> kinesis (localstack) -> extract_task -> transform_task -> load_task -> data_warehouse"
}

# Mapeo de métricas a componentes sospechosos y fuentes de logs
METRIC_TO_COMPONENT_MAP = {
    "emitter_status": {
        "components": ["emitter"],
        "docker_services": ["kinesis_emitter"],
        "airflow_tasks": []
    },
    "emitter_records_sent": {
        "components": ["emitter"],
        "docker_services": ["kinesis_emitter"],
        "airflow_tasks": []
    },
    "emitter_latency": {
        "components": ["emitter", "kinesis_stream"],
        "docker_services": ["kinesis_emitter", "localstack"],
        "airflow_tasks": []
    },
    "emitter_cpu_usage": {
        "components": ["emitter"],
        "docker_services": ["kinesis_emitter"],
        "airflow_tasks": []
    },
    "emitter_memory_usage": {
        "components": ["emitter"],
        "docker_services": ["kinesis_emitter"],
        "airflow_tasks": []
    },
    "cpu_usage": {
        "components": ["airflow_infra"],
        "docker_services": ["airflow-scheduler", "airflow-webserver"],
        "airflow_tasks": ["extract_task", "transform_task", "load_task"]
    },
    "memory_usage": {
        "components": ["airflow_infra"],
        "docker_services": ["airflow-scheduler", "airflow-webserver"],
        "airflow_tasks": ["extract_task", "transform_task", "load_task"]
    },
    "records_extracted": {
        "components": ["extract_task", "kinesis_stream", "emitter"],
        "docker_services": ["localstack", "airflow-scheduler"],
        "airflow_tasks": ["extract_task"]
    },
    "kinesis_lag_ms": {
        "components": ["extract_task", "kinesis_stream", "emitter"],
        "docker_services": ["localstack", "airflow-scheduler"],
        "airflow_tasks": ["extract_task"]
    },
    "records_loaded": {
        "components": ["load_task", "data_warehouse", "transform_task"],
        "docker_services": ["data_warehouse", "airflow-scheduler"],
        "airflow_tasks": ["load_task", "transform_task"]
    }
}

# Diccionario de causas raíz para Ground Truth
ANOMALY_GROUND_TRUTH = {
    "cpu_stress": {
        "category": "INFRASTRUCTURE_RESOURCE_STARVATION",
        "description": "Estrés y estrangulamiento de CPU inducido en el contenedor de Airflow Scheduler, degradando la ejecución de hilos y planificación de tareas.",
        "expected_anomalous_metrics": ["cpu_usage", "records_extracted", "kinesis_lag_ms"],
        "affected_components": ["airflow_infra", "extract_task", "transform_task", "load_task"],
        "expected_cascade_effects": "Retraso crítico en el disparo de dagruns; incremento exponencial de lag en el stream de Kinesis por falta de ejecución oportuna de 'extract_task'; contrapresión en el pipeline.",
        "remediation_actions": [
            "Escalar horizontalmente los workers de Airflow (Celery/KubernetesExecutor).",
            "Aumentar límites de asignación de CPU (CPU limits / quotas en Docker/K8s).",
            "Optimizar la concurrencia de DAGs en airflow.cfg (max_active_runs, parallelism)."
        ]
    },
    "dw_timeout": {
        "category": "DATABASE_UNAVAILABLE",
        "description": "Interrupción / pausa del contenedor PostgreSQL Data Warehouse (analytics_db) en el puerto 5432, impidiendo la persistencia de lotes.",
        "expected_anomalous_metrics": ["records_loaded"],
        "affected_components": ["data_warehouse", "load_task"],
        "expected_cascade_effects": "Fallo directo por timeout de conexión TCP en 'load_task'; reintentos sucesivos y retención de lotes en memoria/staging de Airflow; degradación temporal de disponibilidad del data warehouse para consumidores analíticos downstream.",
        "remediation_actions": [
            "Verificar el estado del servicio PostgreSQL y su conectividad de red en el puerto 5432.",
            "Revisar el pool de conexiones de base de datos y deadlocks en el DW.",
            "Implementar políticas de retry con backoff exponencial en la tarea 'load_task'."
        ]
    },
    "emitter_down": {
        "category": "SOURCE_PRODUCER_FAILURE",
        "description": "Detención silenciosa del microservicio emisor de datos (kinesis_emitter) que inyecta telemetría continua al stream de Kinesis.",
        "expected_anomalous_metrics": ["emitter_status", "emitter_records_sent", "records_extracted"],
        "affected_components": ["emitter", "kinesis_stream", "extract_task"],
        "expected_cascade_effects": "Drenaje de registros remanentes en el stream de Kinesis; caída a 0 de la ingesta en 'extract_task' (records_extracted = 0); propagación secuencial de inanición de datos (data starvation) hacia 'transform_task' y 'load_task' por ausencia de registros entrantes.",
        "remediation_actions": [
            "Reiniciar el contenedor/proceso 'kinesis_emitter'.",
            "Comprobar logs del emisor para descartar Out-Of-Memory (OOM) o excepciones en el hilo de emisión.",
            "Configurar una sonda de healthcheck y política de auto-restart en docker-compose / Kubernetes."
        ]
    },
    "aws_down": {
        "category": "MESSAGE_BROKER_UNAVAILABLE",
        "description": "Pausa / congelación del contenedor AWS LocalStack (Kinesis endpoint), manifestado a nivel de proceso por saturación de CPU y bloqueo del bucle de eventos asíncronos (CPU starvation en kinesis_mock_server) impidiendo responder a peticiones HTTP en el puerto 4566.",
        "expected_anomalous_metrics": ["records_extracted", "kinesis_lag_ms", "emitter_latency"],
        "affected_components": ["kinesis_stream", "emitter", "extract_task"],
        "expected_cascade_effects": "Impacto bidireccional severo en el pipeline: aguas arriba (upstream), el microservicio 'emitter' experimenta timeouts de conexión/escritura (put_record), saturación de búfer y aumento de 'emitter_latency'; aguas abajo (downstream), la tarea 'extract_task' de Airflow sufre timeouts de lectura (get_records) e interrupción de la ingesta (records_extracted = 0), propagando inanición de datos (data starvation) hacia 'transform_task' y 'load_task'.",
        "remediation_actions": [
            "Reanudar / reiniciar el servicio LocalStack/AWS Kinesis.",
            "Comprobar latencia de red y resolución DNS del endpoint http://localstack:4566.",
            "Asegurar credenciales y permisos IAM para operaciones get_records y put_record."
        ]
    }
}

def get_components_for_metrics(candidate_metrics):
    """
    Dado un conjunto de métricas candidatas con alto error de reconstrucción en TranAD+,
    devuelve los componentes, servicios docker y tareas de Airflow sospechosos.
    """
    components = set()
    docker_services = set()
    airflow_tasks = set()
    
    for metric in candidate_metrics:
        info = METRIC_TO_COMPONENT_MAP.get(metric)
        if info:
            components.update(info.get("components", []))
            docker_services.update(info.get("docker_services", []))
            airflow_tasks.update(info.get("airflow_tasks", []))
            
    return {
        "components": sorted(list(components)),
        "docker_services": sorted(list(docker_services)),
        "airflow_tasks": sorted(list(airflow_tasks))
    }
