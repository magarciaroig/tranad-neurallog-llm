import json
import socket
import logging
from datetime import datetime, timedelta, timezone
import urllib.request
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import boto3

# --- LOGGER ESTRUCTURADO PARA AIRFLOW (HitAnomaly / LogRobust compatible) ---
task_logger = logging.getLogger("airflow.task")
HOSTNAME = socket.gethostname()

def log_structured(level, template, message, task_id, trace_id=None, attributes=None, exc_info=False):
    """Emite un log estructurado en JSON a través del sistema de logging de Airflow."""
    now = datetime.now(timezone.utc)
    log_entry = {
        "timestamp": now.isoformat(),
        "epoch": int(now.timestamp()),
        "level": level,
        "service_name": "airflow",
        "component": task_id,
        "task_id": task_id,
        "host": HOSTNAME,
        "template": template,
        "message": message,
        "trace_id": trace_id,
        "attributes": attributes or {}
    }
    json_str = json.dumps(log_entry, ensure_ascii=False)
    if level == "ERROR":
        task_logger.error(json_str, exc_info=exc_info)
    elif level == "WARNING":
        task_logger.warning(json_str)
    else:
        task_logger.info(json_str)

# Función auxiliar para inicializar boto3 (Kinesis)
def get_kinesis_client():
    return boto3.client('kinesis', region_name='eu-west-1')

# Función para enviar métricas a Pushgateway
def push_metric(job_name, metric_name, value):
    """Envía métricas a Pushgateway por HTTP POST"""
    try:
        data = f"{metric_name} {value}\n".encode('utf-8')
        req = urllib.request.Request(
            f"http://pushgateway:9091/metrics/job/{job_name}", 
            data=data, 
            method='POST'
        )
        urllib.request.urlopen(req, timeout=2)
    except Exception as e:
        log_structured(
            "WARNING",
            "No se pudo enviar métrica a Pushgateway: <*ERROR*>",
            f"No se pudo enviar métrica a Pushgateway: {e}",
            task_id="metric_pusher",
            attributes={"metric_name": metric_name, "error": str(e)}
        )

# Tarea de Extracción
def extract_data(**kwargs):
    client = get_kinesis_client()
    stream_name = 'tfm-data-stream'
    shard_id = 'shardId-000000000000'
    last_sequence_number = Variable.get('kinesis_last_seq_num', default_var=None)
    
    try:
        shard_iterator = None
        if last_sequence_number:
            try:
                iterator_resp = client.get_shard_iterator(
                    StreamName=stream_name, ShardId=shard_id,
                    ShardIteratorType='AFTER_SEQUENCE_NUMBER', StartingSequenceNumber=last_sequence_number
                )
                shard_iterator = iterator_resp['ShardIterator']
            except Exception as iter_err:
                log_structured(
                    "WARNING",
                    "SequenceNumber guardado no encontrado en Kinesis. Reiniciando con TRIM_HORIZON.",
                    f"SequenceNumber guardado ({last_sequence_number}) no encontrado en Kinesis ({iter_err}). Reiniciando con TRIM_HORIZON...",
                    task_id="extract_task",
                    attributes={"last_sequence_number": str(last_sequence_number), "error": str(iter_err)}
                )
                iterator_resp = client.get_shard_iterator(
                    StreamName=stream_name, ShardId=shard_id,
                    ShardIteratorType='TRIM_HORIZON'
                )
                shard_iterator = iterator_resp['ShardIterator']
        else:
            iterator_resp = client.get_shard_iterator(
                StreamName=stream_name, ShardId=shard_id,
                ShardIteratorType='TRIM_HORIZON'
            )
            shard_iterator = iterator_resp['ShardIterator']
            
        extracted_batch = []
        max_seq_num_in_batch = last_sequence_number
        lag_ms = 0
        
        while shard_iterator:
            response = client.get_records(ShardIterator=shard_iterator, Limit=100)
            records = response['Records']
            
            lag_ms = response.get('MillisBehindLatest', 0)
            
            if not records:
                break
                
            for record in records:
                data = json.loads(record['Data'])
                extracted_batch.append(data)
                max_seq_num_in_batch = record['SequenceNumber']
                
            shard_iterator = response.get('NextShardIterator')
            if lag_ms == 0:
                break

        push_metric('airflow_pipeline', 'tfm_records_extracted', len(extracted_batch))
        push_metric('airflow_pipeline', 'tfm_kinesis_stream_lag_ms', lag_ms)

        if not extracted_batch:
            log_structured(
                "INFO",
                "No hay datos nuevos en Kinesis stream.",
                "No hay datos nuevos en Kinesis stream.",
                task_id="extract_task",
                attributes={"extracted_count": 0, "lag_ms": lag_ms}
            )
            kwargs['ti'].xcom_push(key='pipeline_batch', value=[])
            return

        if max_seq_num_in_batch != last_sequence_number:
            Variable.set('kinesis_last_seq_num', max_seq_num_in_batch)
            
        kwargs['ti'].xcom_push(key='pipeline_batch', value=extracted_batch)
        log_structured(
            "INFO",
            "Extraídos <*COUNT*> registros de Kinesis. Lag: <*LAG*>ms.",
            f"Extraídos {len(extracted_batch)} registros de Kinesis. Lag: {lag_ms}ms.",
            task_id="extract_task",
            attributes={"extracted_count": len(extracted_batch), "lag_ms": lag_ms, "max_seq_num": str(max_seq_num_in_batch)}
        )
        
    except Exception as e:
        push_metric('airflow_pipeline', 'tfm_records_extracted', 0)
        push_metric('airflow_pipeline', 'tfm_kinesis_stream_lag_ms', 999999)
        log_structured(
            "ERROR",
            "ERROR crítico de conexión a Kinesis: <*ERROR*>",
            f"ERROR crítico de conexión a Kinesis: {str(e)}",
            task_id="extract_task",
            attributes={"error": str(e)},
            exc_info=True
        )
        raise e

# Tarea de Transformación
def transform_data(**kwargs):
    batch = kwargs['ti'].xcom_pull(key='pipeline_batch', task_ids='extract_task')
    if not batch:
        log_structured(
            "INFO",
            "Lote vacío para transformar.",
            "Lote vacío para transformar.",
            task_id="transform_task",
            attributes={"transformed_count": 0}
        )
        kwargs['ti'].xcom_push(key='transformed_batch', value=[])
        return

    transformed_batch = []
    for item in batch:
        trace_id = item.get('trace_id') or item.get('id') or 'unknown'
        sensor_id = item.get('sensor_id') or item.get('sensor') or 'unknown'
        value_raw = item.get('value') if item.get('value') is not None else item.get('measurement', 0.0)
        try:
            val_float = float(value_raw)
        except (ValueError, TypeError):
            val_float = 0.0

        transformed_item = {
            'trace_id': trace_id,
            'sensor_id': sensor_id,
            'value': val_float,
            'status': 'PROCESSED' if val_float > 0 else 'INVALID'
        }
        transformed_batch.append(transformed_item)
        
    kwargs['ti'].xcom_push(key='transformed_batch', value=transformed_batch)
    log_structured(
        "INFO",
        "Transformados <*COUNT*> registros.",
        f"Transformados {len(transformed_batch)} registros.",
        task_id="transform_task",
        attributes={"transformed_count": len(transformed_batch)}
    )

# Tarea de Carga
def load_data(**kwargs):
    batch = kwargs['ti'].xcom_pull(key='transformed_batch', task_ids='transform_task')
    if not batch:
        push_metric('airflow_pipeline', 'tfm_records_loaded', 0)
        log_structured(
            "INFO",
            "Lote vacío para cargar en DW.",
            "Lote vacío para cargar en DW.",
            task_id="load_task",
            attributes={"loaded_count": 0}
        )
        return
        
    pg_hook = PostgresHook(postgres_conn_id='dw_conn')
    create_table_query = """
    CREATE TABLE IF NOT EXISTS metrics_data (
        trace_id VARCHAR(100) PRIMARY KEY,
        sensor_id VARCHAR(50),
        value FLOAT,
        status VARCHAR(50),
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    pg_hook.run(create_table_query)
    
    insert_query = """
    INSERT INTO metrics_data (trace_id, sensor_id, value, status)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (trace_id) DO UPDATE SET value = EXCLUDED.value, status = EXCLUDED.status;
    """
    
    records = [(item['trace_id'], item.get('sensor_id'), item['value'], item['status']) for item in batch]
    
    try:
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(insert_query, records)
        conn.commit()
        cursor.close()
        conn.close()
        
        push_metric('airflow_pipeline', 'tfm_records_loaded', len(batch))
        log_structured(
            "INFO",
            "Carga de <*COUNT*> registros en Data Warehouse exitosa.",
            f"Carga de {len(batch)} registros en Data Warehouse exitosa.",
            task_id="load_task",
            attributes={"loaded_count": len(batch)}
        )
        
    except Exception as e:
        push_metric('airflow_pipeline', 'tfm_records_loaded', 0)
        log_structured(
            "ERROR",
            "Error en base de datos Data Warehouse: <*ERROR*>",
            f"Error en base de datos Data Warehouse: {str(e)}",
            task_id="load_task",
            attributes={"error": str(e), "batch_size": len(batch)},
            exc_info=True
        )
        raise e


default_args = {
    'owner': 'tfm_student',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Definición del DAG
with DAG(
    'tfm_distributed_pipeline', 
    default_args=default_args, 
    schedule_interval=timedelta(minutes=1), 
    catchup=False,
    is_paused_upon_creation=False 
) as dag:
    
    t1_extract = PythonOperator(task_id='extract_task', python_callable=extract_data)
    t2_transform = PythonOperator(task_id='transform_task', python_callable=transform_data)
    t3_load = PythonOperator(task_id='load_task', python_callable=load_data)
    
    t1_extract >> t2_transform >> t3_load