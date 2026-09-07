> [!NOTE]
> **Documentación en español disponible:** Para la versión en español de esta documentación, consulte [`README.md`](README.md).

# 🚀 TranAD+ in PyTorch: Anomaly Detection and AIOps in Data Pipelines

This repository contains the implementation and experimental workflow of the **TranAD+** algorithm (*A Transformer-Based Deep Learning Approach to Anomaly Detection of High-Bandwidth Multivariate Time-Series Satellite Communications*, Yermakov et al., 2025; based on *TranAD*, Tuli et al., 2022) developed in **PyTorch** with **local GPU acceleration (NVIDIA CUDA)**.

The project is developed as a **Master's Thesis (TFM)** focused on automated detection, diagnosis, and explainability (**Root Cause Analysis - RCA**) of complex failures in distributed data engineering pipelines (Producer Emitter, AWS Kinesis Stream, Apache Airflow, PostgreSQL Data Warehouse, and Docker).

---

## 📁 Repository Structure

```text
.
├── environment.yml                                      # Conda environment with GPU support (CUDA 12.1/12.4)
├── README.en.md                                         # English setup, workflow, and execution guide
├── README.md                                            # Spanish documentation (Guía en español)
├── 01_data_normalization.ipynb                          # Step 1: Cleaning, normalization, and multiresolution sliding windows
├── 02_tranad_plus_training.ipynb                        # Step 2: TranAD+ model architecture, adversarial training, and evaluation
├── 03_neurallog_relevant_log_selection.ipynb            # Step 3: Unsupervised dense semantic log selection with NeuralLog
├── 04_llm_assistant_rca_diagnosis.ipynb                 # Step 4: Multimodal LLM RCA diagnosis, SRE Judge benchmark, and ablation study
├── models/                                              # Trained model checkpoints (.pt)
│   └── tranad_plus_best.pt
├── processed_data/                                      # Processed tensors, scalers, and evaluation metrics
│   ├── tranad_processed_tensors_w5.pt
│   ├── tranad_processed_tensors_w10.pt
│   ├── tranad_processed_tensors_w15.pt
│   ├── tranad_processed_tensors_w20.pt
│   ├── tranad_scaler.joblib
│   ├── dataset_metadata.json
│   ├── grid_search_results.csv
│   ├── test_detections.csv
│   └── tranad_evaluation_metrics.json
├── llms_output/                                         # LLM diagnostic reports, benchmarks, and quantitative metrics
│   ├── cached_llm_responses.json
│   ├── llm_rca_diagnostic_reports.json
│   ├── llm_rca_diagnostic_reports.md
│   └── llm_rca_benchmark_metrics.json
└── dataset_gen/                                         # Chaos engineering and simulation infrastructure
    ├── chaos_injector.sh                                # Interactive chaos injection script
    ├── docker-compose.yml                               # Pipeline services (Airflow, Kinesis, DW, Prometheus)
    ├── extract_data.sh                                  # Automated metrics and logs extraction
    ├── extract_logs.py                                  # Structured log parser
    ├── label_dataset.py                                 # Bimodal dataset labeling
    ├── topology_mapper.py                               # DAG dependency graph and component lineage
    ├── llm_rca_assistant.py                             # Standalone LLM Root Cause Assistant script
    ├── Readme.en.md                                     # Dataset generation overview in English
    ├── protocolo_generacion_datasets.en.md              # Step-by-step dataset generation protocol in English
    └── datasets_finales/                                # Official Master's Thesis benchmark datasets
        ├── train_dataset.csv / train_logs.jsonl
        ├── val_dataset.csv / val_logs.jsonl / val_rca_benchmark.json
        └── test_dataset.csv / test_logs.jsonl / test_rca_benchmark.json
```

---

## ⚙️ 1. Prerequisites and Environment Setup

### Hardware and Software Requirements
* **GPU:** NVIDIA CUDA-compatible GPU (CPU fallback supported).
* **NVIDIA Driver:** Version $\ge 525$ (compatible with CUDA 12.x / 13.0).
* **Package Manager:** Anaconda3 or Miniconda.

### Installation Steps

1. **Create the reproducible Conda virtual environment:**
   ```bash
   conda env create -f environment.yml
   ```

2. **Activate the environment:**
   ```bash
   conda activate tranad_plus
   ```

3. **Register the Jupyter kernel:**
   ```bash
   python -m ipykernel install --user --name tranad_plus --display-name "Python (TranAD+ GPU)"
   ```

4. **Verify GPU availability and PyTorch installation:**
   ```bash
   python -c "import torch; print('CUDA Available:', torch.cuda.is_available()); print('Device:', torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'CPU')"
   ```

---

## 📊 2. Dataset Strategy

Following the experimental protocol in [`dataset_gen/protocolo_generacion_datasets.en.md`](dataset_gen/protocolo_generacion_datasets.en.md), data is partitioned into three strictly separated temporal splits:

| Split | File | Anomaly % | Role in TranAD+ / AIOps Architecture |
| :--- | :--- | :--- | :--- |
| **Train** | `train_dataset.csv` | **0%** | Unsupervised learning of nominal system dynamics and baseline reconstruction during healthy operations. |
| **Validation** | `val_dataset.csv` | **5% – 10%** | Hyperparameter calibration and dynamic threshold determination (*POT / optimal F1*). |
| **Test** | `test_dataset.csv` | **20% – 35%** | End-to-end evaluation (Precision, Recall, F1, ROC-AUC, PR-AUC) and multimodal RCA benchmark with LLMs. |

### Multivariate Pipeline Metrics ($m=10$)

1. `emitter_status`: Producer operational health status (1 = OK, 0 = Down).
2. `emitter_records_sent`: Cumulative count of events emitted to the Kinesis stream.
3. `emitter_latency`: Per-event emission latency (seconds).
4. `emitter_cpu_usage`: CPU utilization rate of the emitter container.
5. `emitter_memory_usage`: RAM consumption of the emitter container (bytes).
6. `cpu_usage`: Combined CPU utilization rate of Airflow Scheduler/Workers.
7. `memory_usage`: RAM consumption of Airflow Scheduler/Workers (bytes).
8. `records_extracted`: Records retrieved from Kinesis in the extraction task.
9. `records_loaded`: Records inserted into PostgreSQL Data Warehouse.
10. `kinesis_lag_ms`: Consumer reading lag from the Kinesis stream (milliseconds).

---

## 📓 3. Experimental Notebook Workflow

### 🔹 Step 1: [`01_data_normalization.ipynb`](01_data_normalization.ipynb)
* **Cleaning and Imputation:** Strict regular frequency verification ($\Delta t = 5$s) and forward fill (`ffill`).
* **MinMax Normalization with `clip=True`:** Fitted exclusively on Train to prevent *data leakage* and bounded in $[0.0, 1.0]$ to avoid *Softmax NaN overflow* under extreme fault sentinel values (e.g., $999.999\text{ ms}$).
* **Multiresolution Sliding Windows:** 3D tensor formatting $(N, w, m)$ across candidate temporal horizons $w \in \{5, 10, 15, 20\}$ ($25\text{s}$ to $100\text{s}$).
* **Artifact Export:** Produces `processed_data/tranad_processed_tensors_w{5,10,15,20}.pt` and `tranad_scaler.joblib`.

### 🔹 Step 2: [`02_tranad_plus_training.ipynb`](02_tranad_plus_training.ipynb)
* **TranAD+ Architecture:** Transformer Encoder + Dual Parallel Decoders with **Inverse Bottleneck** ($d_{\text{model}}=64 \to d_{\text{ff}}=256 \to d_{\text{model}}=64$).
* **Two-Phase Adversarial Training:** Reconstruction loss minimization ($L_1$) and adversarial discrimination ($L_2$) accelerated on local GPU.
* **Dynamic Threshold Calibration:** Optimal threshold computation $\tau^*$ over the Validation split.
* **Test Evaluation:** Precision, Recall, F1-Score, ROC/PR curves, and confusion matrices.

### 🔹 Step 3: [`03_neurallog_relevant_log_selection.ipynb`](03_neurallog_relevant_log_selection.ipynb)
* **Unsupervised NeuralLog Adaptation (Zero-Shot Template-Free):** Dense semantic representation of structured logs via `all-MiniLM-L6-v2` ($384$-d) + 2-gram temporal context ($384$-d) + observable scalar metadata ($3$-d), forming a unified $771$-dimensional vector per log with zero label leakage.
* **Unsupervised Modeling:** `IsolationForest` fitted exclusively on healthy Train logs ($22,885$ records, 0% anomalies).
* **Optimal Threshold Calibration:** Calibration of $\tau_{\text{logs}}^*$ on Validation to maximize F1-Score.
* **Test Evaluation & Noise Reduction:** Achieves $\text{ROC-AUC} = 0.9758$ and successfully filters out $>97.6\%$ of routine `INFO` messages.
* **Ablation Study:** Empirical comparison against naive `ERROR` severity filtering, uniform sampling, and `TF-IDF + Isolation Forest`.
* **Diversified Top-K Retrieval (IR) Benchmark:** Semantic template deduplication and ranking of critical logs for benchmark incidents, achieving $\text{Recall@5} = 100\%$ and $\text{MRR} = 1.0$.
* **Artifact Export:** Produces `processed_data/selected_relevant_logs_for_rca.json` and NumPy embeddings cache.

### 🔹 Step 4: [`04_llm_assistant_rca_diagnosis.ipynb`](04_llm_assistant_rca_diagnosis.ipynb)
* **Autonomous Root Cause Diagnosis with LLMs:** Ingests multimodal incident packets (DAG Topology + Quantitative TranAD+ + NeuralLog Top-K logs) to synthesize technical incident reports and actionable remediation plans.
* **SRE Prompt Auditability:** Collapsible HTML element (`<details>`, collapsed by default) allowing operators to inspect exact assembled multimodal prompts without visual clutter.
* **Comparative Multi-Model Benchmark (3 LLMs × 3 Test Incidents):**
  - **`gemini-3.5-flash`** (*Cloud Frontier Reasoning* - Native CoT): Selected benchmark winner with **75.83%** mean global accuracy and 14.81s latency (highest causal depth and cascade lineage reasoning).
  - **`gemini-3.1-flash-lite`** (*Cloud Fast / Triage*): 65.07% accuracy and ultra-low 3.72s latency (ideal for real-time preliminary triage).
  - **`deepseek-r1:14b`** (*Local On-Premises GPU via Ollama*): 48.57% accuracy, 141.57s latency, ensuring absolute data privacy within corporate boundaries.
* **Unified Provider-Agnostic Abstraction (`llm_client.py`):** Seamless switching between Google GenAI SDK and LiteLLM, secure `.env` credential loading, and strict Chain-of-Thought (CoT) decoupling.
* **Formal LLM-as-a-Judge Evaluation:** Blind 4-dimensional SRE rubric evaluating Root Component (35%), Mechanism (30%), Cascade (15%), and Remediation (20%) against official Ground Truth.
* **Empirical Demonstration of NeuralLog (4-Way Multi-Incident Ablation Study):** Systematic evaluation on winning model (`gemini-3.5-flash`), demonstrating the clear superiority of **NeuralLog (75.83%)** over **Hybrid Mode (68.67%)**, **Conventional ERROR/WARN Filtering (65.13%)**, and metric isolation **No Logs (42.17%)**. Empirically validates attention hijacking, context pollution, and the necessity of semantic log filtering for silent failure root cause diagnosis.
* **Artifact Export:** Generates `llms_output/llm_rca_diagnostic_reports.json`, `llms_output/llm_rca_diagnostic_reports.md`, and `llms_output/llm_rca_benchmark_metrics.json`.

---

## ⚡ 4. Executing the Notebooks

### Interactive Mode (VS Code / Jupyter Lab)
1. Open the target notebook (`01_...`, `02_...`, `03_...`, or `04_...`).
2. Select the **`Python (TranAD+ GPU)`** kernel in the upper-right corner.
3. Optionally configure model parameters in the header of Notebook 4 if benchmarking alternate LLM configurations.
4. Execute cells sequentially (`Shift + Enter` or *Run All*).

### Automated Headless CLI Execution
```bash
conda activate tranad_plus

# Step 1: Cleaning, Normalization, and Windows
jupyter nbconvert --to notebook --execute --inplace 01_data_normalization.ipynb

# Step 2: TranAD+ Adversarial Training and Evaluation
jupyter nbconvert --to notebook --execute --inplace 02_tranad_plus_training.ipynb

# Step 3: NeuralLog Semantic Log Selection
jupyter nbconvert --to notebook --execute --inplace 03_neurallog_relevant_log_selection.ipynb

# Step 4: Multimodal LLM RCA Assistant & SRE Judge Benchmark
jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=600 04_llm_assistant_rca_diagnosis.ipynb
```
