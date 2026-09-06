# Protocolo de Generación de Datasets
**Ingeniería del Caos y Preparación de Datos para TranAD+**

> **Objetivo:** Generar tres conjuntos de datos estrictamente separados temporalmente (Train, Validation y Test) para garantizar que el modelo de detección de anomalías basado en reconstrucción aprenda correctamente y sea evaluado sin sesgos de memorización.

---

## 🛠 Preparación del Entorno

Antes de iniciar el proceso, asegúrate de tener una carpeta segura donde guardaremos los resultados finales para evitar que sean sobrescritos por ejecuciones posteriores.

```bash
mkdir -p ./datasets_finales
```

---

## 🔄 Flujo de Extracción y Procesamiento de Datos

El proceso de recolección y etiquetado consta de 3 herramientas integradas:

1. **`extract_data.sh`:** Script principal automatizado. Descarga las series temporales de métricas desde Prometheus (`query_range`) e invoca internamente a `extract_logs.py` para extraer los logs sincronizados de Docker y Airflow en `./tfm_dataset/raw_logs.jsonl`.
2. **`extract_logs.py` (Opcional / Manual):** Permite extraer de forma independiente los logs de contenedores (`docker logs`) y tareas de Airflow para una ventana temporal específica:
   ```bash
   python extract_logs.py <start_time_epoch> <end_time_epoch> ./tfm_dataset/raw_logs.jsonl
   ```
3. **`label_dataset.py`:** Cruza las métricas y los logs brutos con los eventos de inyección de caos (`chaos_events.csv`) para generar:
   - `tranad_dataset.csv`: Dataset multivariado ($m=10$ métricas) listo para TranAD+.
   - `dataset_logs.jsonl`: Trazas de logs sincronizadas y etiquetadas (`is_anomaly`, `anomaly_type`).
   - `rca_eval_benchmark.json`: Casos estructurados de incidentes para evaluar Causa Raíz con LLM.

---

## 🟢 Fase 1: Conjunto de Entrenamiento (Train)

El conjunto de entrenamiento debe ser **100% libre de anomalías**. TranAD+ utilizará estos datos para aprender el patrón de comportamiento "saludable" de tu infraestructura (uso base de CPU y Memoria en Airflow).

### Pasos de Ejecución

1. **Limpieza absoluta:** Destruye cualquier contenedor previo y limpia volúmenes para empezar de cero.
   ```bash
   docker compose down -v
   rm -rf ./tfm_dataset/*
   ```

2. **Despliegue inicial:** Levanta la infraestructura de forma normal.
   ```bash
   docker compose up -d
   ```

3. **Recolección de datos normales:** Inicia el DAG `tfm_distributed_pipeline` en la interfaz de Airflow. Deja el sistema funcionando ininterrumpidamente durante **2 a 3 horas**. *¡No toques el inyector de caos!*

4. **Extracción de Métricas y Logs:** Una vez transcurrido el tiempo, ejecuta el script de extracción (que descarga métricas de Prometheus y ejecuta `extract_logs.py`):
   ```bash
   ./extract_data.sh
   ```
   *(Opcional: Si deseas volver a extraer únicamente los logs sin redescargar métricas, ejecuta: `python extract_logs.py`)*

5. **Etiquetado y Procesamiento:**
   ```bash
   python label_dataset.py
   ```

6. **Guardado seguro:** Mueve los resultados a la carpeta final.
   ```bash
   mv ./tfm_dataset/tranad_dataset.csv ./datasets_finales/train_dataset.csv
   mv ./tfm_dataset/dataset_logs.jsonl ./datasets_finales/train_logs.jsonl 2>/dev/null || true
   ```

---

## 🟡 Fase 2: Conjunto de Validación (Validation)

El conjunto de validación sirve para afinar los hiperparámetros y definir el umbral (*Threshold* con *POT*) de detección. Debe contener mayoritariamente tráfico normal, intercalado con **anomalías leves y de corta duración**.

### Pasos de Ejecución

1. **Reinicio del entorno:**
   ```bash
   docker compose down -v
   rm -rf ./tfm_dataset/*
   docker compose up -d
   ```

2. **Tráfico base (Normal):** Activa el DAG y deja correr el sistema estable durante **45 - 60 minutos**.

3. **Inyección de Caos (Leve):** Ejecuta `./chaos_injector.sh`. Selecciona la *Opción 1 (Estrés CPU)*. Déjalo actuar solo por **5 minutos**. Luego, ejecuta la *Opción 5 (Restaurar)*.

4. **Estabilización:** Deja el sistema correr normalmente 15 minutos más.

5. **Extracción y Guardado:**
   ```bash
   # Extraer métricas de Prometheus y logs (mediante extract_logs.py integrado)
   ./extract_data.sh
   
   # Etiquetar métricas, logs y generar benchmark RCA
   python label_dataset.py
   
   # Guardar artefactos
   mv ./tfm_dataset/tranad_dataset.csv ./datasets_finales/val_dataset.csv
   mv ./tfm_dataset/dataset_logs.jsonl ./datasets_finales/val_logs.jsonl 2>/dev/null || true
   mv ./tfm_dataset/rca_eval_benchmark.json ./datasets_finales/val_rca_benchmark.json 2>/dev/null || true
   ```

---

## 🔴 Fase 3: Conjunto de Prueba (Test)

Este es el examen final de tu modelo y del sistema de explicación con LLM. Demostrará la robustez de TranAD+ y la capacidad del LLM para diagnosticar la causa raíz a partir de métricas y logs. Debe incluir **anomalías severas, prolongadas y variadas**.

### Pasos de Ejecución

1. **Reinicio del entorno:**
   ```bash
   docker compose down -v
   rm -rf ./tfm_dataset/*
   docker compose up -d
   ```

2. **Tráfico base (Normal):** Activa el DAG y espera **30 minutos**.

3. **Inyección de Caos (Severo):**
   * Inyecta la *Opción 2 (Caída del Data Warehouse)* durante 10 minutos. Restaura (Opción 5).
   * Espera 15 minutos de tráfico normal.
   * Inyecta la *Opción 3 (Detener Emisor)* durante 15 minutos. Restaura (Opción 5).
   * Espera 10 minutos de tráfico normal.
   * Inyecta la *Opción 4 (Interrumpir AWS LocalStack)* durante 10 minutos. Restaura (Opción 5).
   * Espera 10 minutos de tráfico normal.

4. **Extracción y Guardado:**
   ```bash
   # Extraer métricas y logs completos
   ./extract_data.sh
   
   # Etiquetar datasets y generar casos de prueba para el LLM
   python label_dataset.py
   
   # Guardar artefactos finales
   mv ./tfm_dataset/tranad_dataset.csv ./datasets_finales/test_dataset.csv
   mv ./tfm_dataset/dataset_logs.jsonl ./datasets_finales/test_logs.jsonl
   mv ./tfm_dataset/rca_eval_benchmark.json ./datasets_finales/test_rca_benchmark.json
   ```

---

## 📊 Resumen de la Estructura Final

Al concluir estas tres fases, tu carpeta `datasets_finales` contendrá el dataset multimodal listo tanto para **TranAD+** como para el **LLM**:

| Archivo | Propósito | % de Anomalías Aprox. |
| :--- | :--- | :--- |
| `train_dataset.csv` | Aprendizaje de la reconstrucción base (TranAD+). | 0% |
| `train_logs.jsonl` | Logs operacionales en estado saludable. | 0% |
| `val_dataset.csv` | Cálculo de umbrales y ajuste (Threshold en TranAD+). | 5% - 10% |
| `val_rca_benchmark.json` | Casos de validación rápida para prompts de RCA. | ~1-2 anomalías |
| `test_dataset.csv` | Evaluación de métricas de detección (Precision, Recall, F1). | 20% - 30% |
| `test_logs.jsonl` | Logs sincronizados y etiquetados de Docker y Airflow. | 20% - 30% |
| `test_rca_benchmark.json` | Benchmark de evaluación de Causa Raíz (RCA) para el LLM. | Multi-incidente |

---

## 🤖 Evaluación de Causa Raíz (RCA) con LLM

Para ejecutar la evaluación de RCA sobre los incidentes detectados:

```bash
# Diagnóstico de un caso del benchmark
python llm_rca_assistant.py --benchmark-file ./datasets_finales/test_rca_benchmark.json --incident-id incident_000

# Diagnóstico dinámico a partir de métricas anómalas detectadas por TranAD+
python llm_rca_assistant.py \
  --metrics "records_loaded,cpu_usage" \
  --start-time 1786821180 \
  --end-time 1786821480 \
  --logs-file ./datasets_finales/test_logs.jsonl
```

---

## 📈 Columnas y Características del Dataset Generado

| Columna | Origen / Componente | Descripción |
| :--- | :--- | :--- |
| `timestamp` | Prometheus | Marca de tiempo epoch en segundos. |
| `emitter_status` | Pushgateway (`emitter`) | Estado operacional del emisor (1 = OK, 0 = Caído). |
| `emitter_records_sent` | Pushgateway (`emitter`) | Contador acumulado de registros emitidos a Kinesis. |
| `emitter_latency` | Pushgateway (`emitter`) | Latencia de envío por evento en el emisor (segundos). |
| `emitter_cpu_usage` | cAdvisor (`emitter`) | Tasa de uso de CPU del contenedor emisor. |
| `emitter_memory_usage` | cAdvisor (`emitter`) | Uso de memoria RAM del contenedor emisor (bytes). |
| `cpu_usage` | cAdvisor (`airflow`) | Tasa de uso de CPU de Airflow Scheduler/Worker. |
| `memory_usage` | cAdvisor (`airflow`) | Uso de memoria RAM de Airflow Scheduler/Worker (bytes). |
| `records_extracted` | Pushgateway (`airflow`) | Número de registros extraídos de Kinesis en la ejecución. |
| `records_loaded` | Pushgateway (`airflow`) | Número de registros cargados en PostgreSQL (DW). |
| `kinesis_lag_ms` | Pushgateway (`airflow`) | Retraso o lag de lectura del stream Kinesis (ms). |
| `is_anomaly` | Ground Truth (`chaos_injector`) | Etiqueta binaria (0 = Normal, 1 = Anomalía). |
| `anomaly_type` | Ground Truth (`chaos_injector`) | Tipo de anomalía (`cpu_stress`, `dw_timeout`, `emitter_down`, `aws_down`, `none`). |