import boto3
import json
import time
import uuid
import os
import socket
import logging
import urllib.request
from datetime import datetime, timezone

# --- CONFIGURACIÓN DE LOGGING ESTRUCTURADO (JSON) ---
class StructuredJsonFormatter(logging.Formatter):
    """
    Formateador de logs en formato JSON estructurado optimizado
    para algoritmos de detección de anomalías como HitAnomaly / LogRobust.
    """
    def __init__(self, service_name="kinesis_emitter", component_name="emitter"):
        super().__init__()
        self.service_name = service_name
        self.component_name = component_name
        self.hostname = socket.gethostname()

    def format(self, record):
        now = datetime.now(timezone.utc)
        log_entry = {
            "timestamp": now.isoformat(),
            "epoch": int(record.created),
            "level": record.levelname,
            "logger": record.name,
            "service_name": self.service_name,
            "component": self.component_name,
            "host": self.hostname,
            "thread": record.threadName,
            "process": record.process,
            "message": record.getMessage(),
            "template": getattr(record, "template", record.getMessage()),
            "trace_id": getattr(record, "trace_id", None),
            "attributes": getattr(record, "attributes", {})
        }
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "stack": self.formatException(record.exc_info)
            }
        return json.dumps(log_entry, ensure_ascii=False)


# Configuración del Logger
handler = logging.StreamHandler()
handler.setFormatter(StructuredJsonFormatter(service_name="kinesis_emitter", component_name="emitter"))

logger = logging.getLogger("kinesis_emitter")
logger.setLevel(logging.INFO)
logger.handlers = [handler]
logger.propagate = False

kinesis_client = boto3.client(
    'kinesis',
    endpoint_url=os.environ.get('KINESIS_ENDPOINT', 'http://localstack:4566'),
    region_name=os.environ.get('AWS_DEFAULT_REGION', 'eu-west-1'),
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID', 'test'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY', 'test')
)

STREAM_NAME = 'tfm-data-stream'
PUSHGATEWAY_URL = 'http://pushgateway:9091/metrics/job/kinesis_emitter'

def push_metric(metric_name, value):
    try:
        data = f"{metric_name} {value}\n".encode('utf-8')
        req = urllib.request.Request(PUSHGATEWAY_URL, data=data, method='POST')
        urllib.request.urlopen(req, timeout=2)
    except Exception as e:
        logger.warning(
            "No se pudo enviar métrica a Pushgateway: %s",
            str(e),
            extra={
                "template": "No se pudo enviar métrica a Pushgateway: <*ERROR*>",
                "attributes": {"metric_name": metric_name, "error": str(e)}
            }
        )

def emit_data():
    logger.info(
        "Iniciando emisión continua de datos a Kinesis...",
        extra={"template": "Iniciando emisión continua de datos a Kinesis..."}
    )
    records_sent = 0
    
    while True:
        start_time = time.time()
        trace_id = str(uuid.uuid4())
        try:
            payload = {
                "trace_id": trace_id,
                "sensor_id": "sensor_01",
                "value": 42.5,
                "timestamp": time.time()
            }
            
            kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(payload),
                PartitionKey="partition_1"
            )
            
            latency = time.time() - start_time
            records_sent += 1
            
            push_metric('emitter_records_sent_total', records_sent)
            push_metric('emitter_latency_seconds', latency)
            push_metric('emitter_status', 1)
            
            logger.info(
                "Evento emitido a Kinesis correctamente. Latencia: %.4fs",
                latency,
                extra={
                    "template": "Evento emitido a Kinesis correctamente. Latencia: <*LATENCY*>",
                    "trace_id": trace_id,
                    "attributes": {
                        "sensor_id": "sensor_01",
                        "latency": round(latency, 6),
                        "records_sent": records_sent,
                        "stream_name": STREAM_NAME
                    }
                }
            )
        except Exception as e:
            push_metric('emitter_status', 0)
            logger.error(
                "Error crítico al emitir registro a Kinesis: %s",
                str(e),
                exc_info=True,
                extra={
                    "template": "Error crítico al emitir registro a Kinesis: <*ERROR*>",
                    "trace_id": trace_id,
                    "attributes": {
                        "stream_name": STREAM_NAME,
                        "error_message": str(e)
                    }
                }
            )
            
        time.sleep(5)

if __name__ == "__main__":
    time.sleep(5)
    emit_data()