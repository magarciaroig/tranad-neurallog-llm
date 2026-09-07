> [!NOTE]
> **Documentación en español disponible:** Para la versión en español de este protocolo, consulte [`protocolo_generacion_datasets.md`](protocolo_generacion_datasets.md).

# Dataset Generation Protocol
**Chaos Engineering and Data Preparation for TranAD+**

> **Objective:** Generate three strictly temporally separated datasets (Train, Validation, and Test) to ensure that the reconstruction-based anomaly detection model learns nominal patterns effectively and is evaluated without memorization bias or data leakage.

---

## 🛠 Environment Preparation

Before beginning the process, ensure you have a dedicated directory where final outputs will be stored to prevent accidental overwrites during subsequent runs:

```bash
mkdir -p ./datasets_finales
```

---

## 🔄 Data Extraction and Processing Flow

The collection and labeling pipeline consists of 3 integrated tools:

1. **`extract_data.sh`:** Primary automated script. Downloads metric time series from Prometheus (`query_range`) and internally invokes `extract_logs.py` to extract synchronized Docker and Airflow logs into `./tfm_dataset/raw_logs.jsonl`.
2. **`extract_logs.py` (Optional / Manual):** Allows independently extracting container logs (`docker logs`) and Airflow task logs for a specific temporal window:
   ```bash
   python extract_logs.py <start_time_epoch> <end_time_epoch> ./tfm_dataset/raw_logs.jsonl
   ```
3. **`label_dataset.py`:** Merges raw metrics and logs with ground-truth chaos injection events (`chaos_events.csv`) to generate:
   - `tranad_dataset.csv`: Multivariate dataset ($m=10$ metrics) formatted for TranAD+.
   - `dataset_logs.jsonl`: Labeled and synchronized log traces (`is_anomaly`, `anomaly_type`).
   - `rca_eval_benchmark.json`: Structured incident test cases for Root Cause Analysis evaluation with LLMs.

---

## 🟢 Phase 1: Training Set (Train)

The training set must be **100% anomaly-free**. TranAD+ uses this nominal baseline to learn the healthy operational dynamics of the data engineering infrastructure (baseline CPU and memory footprints across Airflow, Emitter, and Data Warehouse).

### Execution Steps

1. **Complete cleanup:** Tear down any existing containers and prune volumes to start from a completely clean state.
   ```bash
   docker compose down -v
   rm -rf ./tfm_dataset/*
   ```

2. **Initial deployment:** Spin up the pipeline infrastructure normally.
   ```bash
   docker compose up -d
   ```

3. **Nominal data collection:** Enable the `tfm_distributed_pipeline` DAG in the Airflow UI. Let the system run uninterrupted for **2 to 3 hours**. *Do not trigger the chaos injector!*

4. **Metrics and Logs Extraction:** Once the time window has elapsed, execute the extraction script (which downloads Prometheus metrics and executes `extract_logs.py`):
   ```bash
   ./extract_data.sh
   ```
   *(Optional: If you need to re-extract logs without re-downloading metrics, run: `python extract_logs.py`)*

5. **Labeling and Processing:**
   ```bash
   python label_dataset.py
   ```

6. **Artifact Archival:** Move the generated datasets to the final directory.
   ```bash
   mv ./tfm_dataset/tranad_dataset.csv ./datasets_finales/train_dataset.csv
   mv ./tfm_dataset/dataset_logs.jsonl ./datasets_finales/train_logs.jsonl 2>/dev/null || true
   ```

---

## 🟡 Phase 2: Validation Set (Validation)

The validation set is used to calibrate hyperparameters and determine the dynamic anomaly detection threshold (*POT / Peaks Over Threshold*). It must consist predominantly of nominal traffic interspersed with **mild, short-duration anomalies**.

### Execution Steps

1. **Environment reset:**
   ```bash
   docker compose down -v
   rm -rf ./tfm_dataset/*
   docker compose up -d
   ```

2. **Baseline nominal traffic:** Enable the DAG and let the pipeline operate steadily for **45 - 60 minutes**.

3. **Chaos Injection (Mild):** Run `./chaos_injector.sh`. Select *Option 1 (CPU Stress)*. Allow it to run for only **5 minutes**, then execute *Option 5 (Restore)*.

4. **Stabilization:** Allow the system to run nominal traffic for an additional 15 minutes.

5. **Extraction and Archival:**
   ```bash
   # Extract Prometheus metrics and logs (via integrated extract_logs.py)
   ./extract_data.sh
   
   # Label metrics, logs, and generate RCA benchmark
   python label_dataset.py
   
   # Archive artifacts
   mv ./tfm_dataset/tranad_dataset.csv ./datasets_finales/val_dataset.csv
   mv ./tfm_dataset/dataset_logs.jsonl ./datasets_finales/val_logs.jsonl 2>/dev/null || true
   mv ./tfm_dataset/rca_eval_benchmark.json ./datasets_finales/val_rca_benchmark.json 2>/dev/null || true
   ```

---

## 🔴 Phase 3: Test Set (Test)

This split serves as the rigorous final benchmark for both the anomaly detection model and the LLM-based root cause explanation engine. It validates the robustness of TranAD+ and the capability of the LLM to diagnose root causes across multimodal telemetry (metrics + logs). It must contain **severe, prolonged, and multi-faceted anomalies**.

### Execution Steps

1. **Environment reset:**
   ```bash
   docker compose down -v
   rm -rf ./tfm_dataset/*
   docker compose up -d
   ```

2. **Baseline nominal traffic:** Enable the DAG and wait **30 minutes**.

3. **Chaos Injection (Severe):**
   * Inject *Option 2 (Data Warehouse Down / DB Timeout)* for 10 minutes. Restore (*Option 5*).
   * Wait 15 minutes under nominal traffic.
   * Inject *Option 3 (Stop Emitter)* for 15 minutes. Restore (*Option 5*).
   * Wait 10 minutes under nominal traffic.
   * Inject *Option 4 (Disrupt AWS LocalStack)* for 10 minutes. Restore (*Option 5*).
   * Wait 10 minutes under nominal traffic.

4. **Extraction and Archival:**
   ```bash
   # Extract comprehensive metrics and logs
   ./extract_data.sh
   
   # Label datasets and generate evaluation benchmark for LLM RCA
   python label_dataset.py
   
   # Archive final artifacts
   mv ./tfm_dataset/tranad_dataset.csv ./datasets_finales/test_dataset.csv
   mv ./tfm_dataset/dataset_logs.jsonl ./datasets_finales/test_logs.jsonl
   mv ./tfm_dataset/rca_eval_benchmark.json ./datasets_finales/test_rca_benchmark.json
   ```

---

## 📊 Final Dataset Summary

Upon completing these three phases, your `datasets_finales/` directory will contain the multimodal benchmark dataset ready for both **TranAD+** and **LLM RCA**:

| File | Purpose | Approx. Anomaly % |
| :--- | :--- | :--- |
| `train_dataset.csv` | Unsupervised baseline reconstruction learning (TranAD+). | 0% |
| `train_logs.jsonl` | Operational baseline logs during healthy system state. | 0% |
| `val_dataset.csv` | Dynamic threshold calibration and tuning (POT in TranAD+). | 5% - 10% |
| `val_rca_benchmark.json` | Rapid validation test cases for RCA prompt tuning. | ~1-2 anomalies |
| `test_dataset.csv` | Detection performance evaluation (Precision, Recall, F1, PR-AUC). | 20% - 30% |
| `test_logs.jsonl` | Synchronized and labeled Docker and Airflow logs. | 20% - 30% |
| `test_rca_benchmark.json` | Ground Truth Root Cause Analysis (RCA) benchmark for LLM evaluation. | Multi-incident |

---

## 🤖 Root Cause Analysis (RCA) Evaluation with LLMs

To evaluate the RCA Assistant against detected incidents:

```bash
# Benchmark incident diagnosis
python llm_rca_assistant.py --benchmark-file ./datasets_finales/test_rca_benchmark.json --incident-id incident_000

# Dynamic diagnosis from anomalous metric window detected by TranAD+
python llm_rca_assistant.py \
  --metrics "records_loaded,cpu_usage" \
  --start-time 1786821180 \
  --end-time 1786821480 \
  --logs-file ./datasets_finales/test_logs.jsonl
```

---

## 📈 Dataset Columns and Feature Schema

| Column | Source / Component | Description |
| :--- | :--- | :--- |
| `timestamp` | Prometheus | Epoch timestamp in seconds (regular $\\Delta t = 5$s). |
| `emitter_status` | Pushgateway (`emitter`) | Producer operational status (1 = Healthy/OK, 0 = Down). |
| `emitter_records_sent` | Pushgateway (`emitter`) | Cumulative counter of records emitted to Kinesis stream. |
| `emitter_latency` | Pushgateway (`emitter`) | Per-record emission latency in the producer (seconds). |
| `emitter_cpu_usage` | cAdvisor (`emitter`) | CPU utilization rate of the emitter container. |
| `emitter_memory_usage` | cAdvisor (`emitter`) | RAM consumption of the emitter container (bytes). |
| `cpu_usage` | cAdvisor (`airflow`) | Aggregated CPU utilization rate of Airflow Scheduler/Worker. |
| `memory_usage` | cAdvisor (`airflow`) | RAM consumption of Airflow Scheduler/Worker (bytes). |
| `records_extracted` | Pushgateway (`airflow`) | Number of records extracted from Kinesis during DAG run. |
| `records_loaded` | Pushgateway (`airflow`) | Number of records loaded into PostgreSQL Data Warehouse. |
| `kinesis_lag_ms` | Pushgateway (`airflow`) | Consumer reading lag from the Kinesis stream (milliseconds). |
| `is_anomaly` | Ground Truth (`chaos_injector`) | Binary anomaly label (0 = Nominal, 1 = Anomaly). |
| `anomaly_type` | Ground Truth (`chaos_injector`) | Anomaly classification (`cpu_stress`, `dw_timeout`, `emitter_down`, `aws_down`, `none`). |
