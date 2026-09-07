> [!NOTE]
> **Documentación en español disponible:** Para la versión en español de esta documentación, consulte [`Readme.md`](Readme.md).

# Dataset Generation for Anomaly Detection in Data Engineering Pipelines

This module is part of a Master's Thesis focusing on anomaly detection and root cause analysis in complex data engineering pipelines. In distributed data architectures, a fault in an upstream component frequently triggers cascading disruptions across downstream stages, making root cause interpretation difficult.

The core time-series anomaly detection algorithm utilized is **TranAD+**, a transformer-based architecture (*paper available at `doc/tran_adplus.pdf`*), building upon the foundational Master's Thesis by Javier Moreno on TranAD (*available at `doc/TFM_Javier_Moreno_checkpoint_v6_1.pdf`*).

---

## 🎯 Master's Thesis Objectives

While previous work focused on cybersecurity network telemetry, this thesis focuses on **detecting, diagnosing, and explaining anomalies in distributed data ingestion and transformation pipelines**.

Due to the absence of realistic, publicly available benchmark datasets in this domain, we implemented a containerized simulation environment using Docker (`docker-compose.yml`) that reproduces a realistic distributed data pipeline. Each component emits operational telemetry used to construct multimodal datasets for training anomaly detection models and powering automated LLM-based Root Cause Analysis:

* **`emitter`**: Microservice producing streaming events to an AWS Kinesis stream.
  * Telemetry: `emitter_records_sent_total`, `emitter_latency_seconds`, `emitter_status`, CPU and RAM consumption.
* **`airflow`**: Distributed workflow DAG orchestrating data movement from Kinesis to PostgreSQL:
  * `extract_data`: Consumes records from Kinesis (`tfm_records_extracted`, `tfm_kinesis_stream_lag_ms`).
  * `transform_data`: Data transformation and validation.
  * `load_data`: Ingestion into PostgreSQL (`tfm_records_loaded`).
  * Infrastructure: Scheduler and worker CPU and RAM usage.
* **`data_warehouse`**: PostgreSQL database acting as the target analytical store.
* **`prometheus`**: Time-series database scraping and storing metric telemetry.
* **`cadvisor`**: Captures container-level resource statistics (CPU, memory, I/O).
* **`pushgateway`**: Intermediary sink for application-level metrics from the emitter and Airflow tasks.
* **`localstack`**: Local emulation of AWS services (Kinesis) running in Docker.

---

## 🔬 Dataset Generation Strategy

Training robust anomaly detection and causal reasoning models requires both nominal healthy operational data and controlled failure periods.

To systematically introduce realistic failures into our simulated environment, we developed the interactive script `chaos_injector.sh` (e.g., CPU starvation, Data Warehouse timeouts, producer crashes, and AWS LocalStack network outages).

Combining chaos injection with `extract_data.sh` (pulling Prometheus metric series and container/Airflow logs), `extract_logs.py`, and `label_dataset.py`, we construct multimodal datasets supporting:
1. **TranAD+ Training and Evaluation:** `train_dataset.csv`, `val_dataset.csv`, and `test_dataset.csv`.
2. **LLM Root Cause Analysis Evaluation:** `test_logs.jsonl` and `test_rca_benchmark.json`.

The complete step-by-step protocol is documented in [`protocolo_generacion_datasets.en.md`](protocolo_generacion_datasets.en.md).

---

## 🔗 Integration with TranAD+ and LLM for Root Cause Analysis (RCA)

1. **Topological Mapping (`topology_mapper.py`):** Formalizes pipeline DAG dependencies and deterministically maps anomalous metrics identified by TranAD+ to suspect candidate components and log sources.
2. **Log Extraction and Labeling (`extract_logs.py`, `label_dataset.py`):** Extracts time-synchronized logs from Docker and Airflow tasks, annotating them with ground-truth anomaly labels and incident windows.
3. **RCA Assistant (`llm_rca_assistant.py`):** Connects high reconstruction error metrics from TranAD+ with candidate component logs to assemble structured multimodal prompts evaluated by LLMs for automated diagnosis and mitigation planning.
