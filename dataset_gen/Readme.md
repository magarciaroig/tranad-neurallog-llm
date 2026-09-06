# Generación de datasets para anomaly detection en el contexto de pipelines de data engineering

El siguiente desarrollo se enmarca en un proyecto de fin de master, concretamente en el ámbito
de anomaly detection en el contexto de detectar problemas en pipelines de datos complejas, donde
un error en un componente produce problemas en otros downstream, y no es fácil interpretar la causa.

El principal algoritmo para anomaly detection a usar será TranAD+, que está basado en transformers
(el paper está disponible en el fichero doc/tran_adplus.pdf). Así mismo el tutor del máster
recomienda que me base en el TFM de Javier Moreno que también se basó en el algortimo de TranAD
(está disponible en doc/TFM_Javier_Moreno_checkpoint_v6_1.pdf).

## Objetivo del TFM

El TFM en el que nos basamos estaba emmarcado en el ámbito de la cyber seguridad, pero en ete caso
nos queremos centrar en detectar y explicar anomalías en el contexto de pipelines de ingestión y
transformación de datos.

En este ámbito en concreto no hay disponibles abundantes/adecuados datasets de datos públicos que podamos usar para el enrenamiento, por lo que hemos optado por generar un dataset sintético de una
hipotético sistema distribuido de ingestión y transformación de datos.

En este componente nos centraremos en la generación de este dataset sintético, crearemos una serie
de sistemas corriendo en docker (docker-compose.yml), que similan nuestra pipeline de ingestión y transformación de datos. Cada uno de los componentes emite una serie de métricas que usaremos para construir el dataset con el que entrenaremos el algoritmo de anomaly detection, y más tarde el componente de explicabilidad en lenguaje humano de los resultados:

- emitter: Sistema que emite datos a un stream de kinesis.
  - Métricas: `emitter_records_sent_total`, `emitter_latency_seconds`, `emitter_status`, consumo de CPU y memoria.
- airflow: DAG con la transformación de datos, desde kinesis hasta una base de datos postgresql
  - extract_data: Consumir desde el kinesis stream (`tfm_records_extracted`, `tfm_kinesis_stream_lag_ms`)
  - transform_data: Transformación de datos
  - load_data: Ingestar los datos en PostgreSQL (`tfm_records_loaded`)
  - Infraestructura: Consumo de CPU y memoria de scheduler/workers
- data_warehouse: base de datos postgresql donde ingestar los datos
- prometheus: Recopilación y almacenamiento de series temporales de métricas
- cadvisor: Captura métricas de sistemas en docker (cpu, memoria, etc)
- pushgateway: Enviar métricas calculadas desde las aplicaciones (emitter y airflow)
- localstack: Implementación de API de servicios AWS corriendo localmente en docker

# Estrategia de generación de datasets

Para el algortimo de anomaly detection necesitamos tanto logs y métricas generados en situaciones
normales, como con periodos con problemas.

Para introducir anomalías en nuestro entorno simulado hemos creado el script `chaos_injector.sh`
(por ejemplo, producir estrés de CPU, caída del DW, detención del emisor, interrupción de AWS LocalStack).

Combinando esto con los scripts `extract_data.sh` (extracción de series temporales de Prometheus y logs de contenedores/Airflow), `extract_logs.py` y `label_dataset.py`, generamos los datasets multimodales necesarios para:
1. **Entrenamiento y evaluación de TranAD+:** `train_dataset.csv`, `val_dataset.csv`, `test_dataset.csv`.
2. **Evaluación de Causa Raíz con LLM:** `test_logs.jsonl` y `test_rca_benchmark.json`.

El procedimiento paso a paso está documentado en `protocolo_generacion_datasets.md`.

## Integración con TranAD+ y LLM para Root Cause Analysis (RCA)

1. **Mapeo Topológico (`topology_mapper.py`):** Define el grafo de dependencias de la pipeline y mapea de forma determinista las métricas anómalas identificadas por TranAD+ hacia los componentes y fuentes de logs sospechosos.
2. **Extracción y Etiquetado de Logs (`extract_logs.py`, `label_dataset.py`):** Extrae logs de Docker y tareas de Airflow sincronizados temporalmente y etiquetados con el tipo de anomalía.
3. **Asistente de RCA (`llm_rca_assistant.py`):** Conecta las métricas con mayor error de reconstrucción detectadas por TranAD+ con los logs de error de los componentes asociados para generar el prompt técnico que se pasa al LLM para el diagnóstico final y acciones de mitigación.