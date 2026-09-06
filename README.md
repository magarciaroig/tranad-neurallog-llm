# 🚀 TranAD+ en PyTorch: Detección de Anomalías y AIOps en Pipelines de Datos

Este repositorio contiene la implementación y el flujo experimental del algoritmo **TranAD+** (*A Transformer-Based Deep Learning Approach to Anomaly Detection of High-Bandwidth Multivariate Time-Series Satellite Communications*, Yermakov et al., 2025; basado en *TranAD*, Tuli et al., 2022) desarrollado en **PyTorch** con aceleración por **GPU local (NVIDIA CUDA)**.

El proyecto se enmarca en un **Trabajo de Fin de Máster (TFM)** enfocado en la detección, diagnóstico y explicabilidad (*Root Cause Analysis - RCA*) de fallos en pipelines complejas y distribuidas de ingeniería de datos (Emisor, Kinesis Stream, Apache Airflow, PostgreSQL Data Warehouse y Docker).

---

## 📁 Estructura del Repositorio

```text
.
├── environment.yml                                      # Entorno Conda con soporte GPU (CUDA 12.1/12.4)
├── README.md                                            # Guía de instalación, configuración y ejecución
├── 01_dataset_cleaning_and_normalization.ipynb          # Paso 1: Limpieza, normalización y ventanas
├── 02_tranad_plus_model_training_and_evaluation.ipynb  # Paso 2: Modelo TranAD+, entrenamiento y evaluación RCA
├── models/                                              # Checkpoints de modelos entrenados (.pt)
│   └── tranad_plus_best.pt
├── processed_data/                                      # Tensores procesados, escaladores y predicciones
│   ├── tranad_processed_tensors_w5.pt
│   ├── tranad_processed_tensors_w10.pt
│   ├── tranad_processed_tensors_w15.pt
│   ├── tranad_processed_tensors_w20.pt
│   ├── tranad_scaler.joblib
│   ├── dataset_metadata.json
│   ├── grid_search_results.csv
│   ├── test_detections.csv
│   └── tranad_evaluation_metrics.json
└── dataset_gen/                                         # Infraestructura de simulación y caos
    ├── chaos_injector.sh                                # Inyección interactiva de caos
    ├── docker-compose.yml                               # Servicios (Airflow, Kinesis, DW, Prometheus)
    ├── extract_data.sh                                  # Extracción de métricas y logs
    ├── extract_logs.py                                  # Extracción estructurada de logs
    ├── label_dataset.py                                 # Etiquetado bimodal de datasets
    ├── topology_mapper.py                               # Grafo topológico y linaje de componentes
    ├── llm_rca_assistant.py                             # Asistente de Causa Raíz con LLM
    ├── protocolo_generacion_datasets.md                 # Guía paso a paso de generación
    └── datasets_finales/                                # Datasets generados para el TFM
        ├── train_dataset.csv / train_logs.jsonl
        ├── val_dataset.csv / val_logs.jsonl / val_rca_benchmark.json
        └── test_dataset.csv / test_logs.jsonl / test_rca_benchmark.json
```

---

## ⚙️ 1. Requisitos Previos e Instalación del Entorno

### Requisitos de Hardware y Software
* **GPU:** Dispositivo compatible con NVIDIA CUDA (o ejecución en CPU como fallback).
* **Driver NVIDIA:** Versión $\ge 525$ (CUDA 12.x / 13.0 compatible).
* **Gestor de Paquetes:** Anaconda3 o Miniconda.

### Pasos de Instalación

1. **Crear el entorno virtual reproducible desde `environment.yml`:**
   ```bash
   conda env create -f environment.yml
   ```

2. **Activar el entorno:**
   ```bash
   conda activate tranad_plus
   ```

3. **Registrar el kernel para Jupyter:**
   ```bash
   python -m ipykernel install --user --name tranad_plus --display-name "Python (TranAD+ GPU)"
   ```

4. **Comprobar la disponibilidad de la GPU y PyTorch:**
   ```bash
   python -c "import torch; print('CUDA Disponible:', torch.cuda.is_available()); print('Dispositivo:', torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'CPU')"
   ```

---

## 📊 2. Estrategia de Datasets

Siguiendo el protocolo establecido en [`dataset_gen/protocolo_generacion_datasets.md`](dataset_gen/protocolo_generacion_datasets.md), los datos se dividen en 3 conjuntos temporales independientes:

| Conjunto | Fichero | % Anomalías | Propósito en TranAD+ |
| :--- | :--- | :--- | :--- |
| **Train** | `train_dataset.csv` | **0%** | Aprendizaje de la dinámica y reconstrucción base del pipeline en estado saludable. |
| **Validation** | `val_dataset.csv` | **5% – 10%** | Calibración de hiperparámetros y cálculo del umbral de detección dinámico (*POT / F1 óptimo*). |
| **Test** | `test_dataset.csv` | **20% – 35%** | Evaluación de métricas (Precision, Recall, F1, ROC-AUC) y benchmark de RCA con LLM. |

### Variables Métricas del Pipeline ($m=10$)

1. `emitter_status`: Estado operacional del productor (1 = OK, 0 = Caído).
2. `emitter_records_sent`: Contador acumulativo de eventos emitidos a Kinesis.
3. `emitter_latency`: Latencia de envío por evento (segundos).
4. `emitter_cpu_usage`: Tasa de uso de CPU del contenedor emisor.
5. `emitter_memory_usage`: Consumo de memoria RAM del emisor (bytes).
6. `cpu_usage`: Tasa de uso de CPU de Airflow Scheduler/Worker.
7. `memory_usage`: Consumo de memoria RAM de Airflow (bytes).
8. `records_extracted`: Registros leídos de Kinesis en la tarea de extracción.
9. `records_loaded`: Registros insertados en PostgreSQL (DW).
10. `kinesis_lag_ms`: Retraso de lectura del stream Kinesis (ms).

---

## 📓 3. Flujo de Cuadernos Experimentales

### 🔹 Paso 1: `01_dataset_cleaning_and_normalization.ipynb`
* **Limpieza e Imputación:** Control de frecuencia regular ($\Delta t = 5$s) y `ffill`.
* **Normalización MinMax con `clip=True`:** Ajustado exclusivamente en Train para evitar *data leakage* y acotado en $[0.0, 1.0]$ para prevenir desbordamientos de atención (*Softmax NaN overflow*) ante valores centinela de fallo (ej. $999.999\text{ ms}$).
* **Ventanas Deslizantes Multirresolución:** Estructuración de tensores tridimensionales $(N, w, m)$ para horizontes candidatos $w \in \{5, 10, 15, 20\}$ ($25\text{s}$ a $100\text{s}$).
* **Exportación:** Genera `processed_data/tranad_processed_tensors_w{5,10,15,20}.pt` y `tranad_scaler.joblib`.

### 🔹 Paso 2: `02_tranad_plus_model_training_and_evaluation.ipynb`
* **Arquitectura TranAD+:** Transformer Encoder + Doble Decodificador paralelo con **Inverse Bottleneck** ($d_{\text{model}}=64 \to d_{\text{ff}}=256 \to d_{\text{model}}=64$).
* **Entrenamiento Adversarial en 2 Fases:** Minimización de reconstrucción ($L_1$) y discriminación adversaria ($L_2$) sobre la GPU local.
* **Calibración de Umbral Dinámico:** Cálculo del umbral óptimo $\tau^*$ sobre Validación.
* **Evaluación en Test:** Precision, Recall, F1-Score, Curvas ROC/PR y matrices de confusión.
### 🔹 Paso 3: `03_neurallog_seleccion_logs_relevantes.ipynb`
* **Adaptación No Supervisada de NeuralLog (Zero-Shot Template-Free):** Representación semántica densa de logs estructurados con `all-MiniLM-L6-v2` ($384$-d) + contexto temporal 2-gram ($384$-d) + metadatos escalares observables ($3$-d), conformando un vector unificado de $771$ variables por log con cero fuga de etiquetas.
* **Entrenamiento no Supervisado:** Ajuste de `IsolationForest` sobre los logs saludables de Train ($22.885$ registros, 0% anomalías) garantizando cero fuga de datos.
* **Calibración Óptima de Umbral:** Calibración de $\tau_{\text{logs}}^*$ en Validación maximizando F1-Score.
* **Evaluación en Test y Reducción de Ruido:** $\text{ROC-AUC} = 0.9758$ y descarte exitoso de $>97.6\%$ de logs rutinarios de nivel `INFO`.
* **Estudio de Ablación:** Comparativa empírica frente a filtro ingenuo por nivel `ERROR`, muestreo uniforme y `TF-IDF + Isolation Forest`.
* **Benchmark de Recuperación Top-K (IR) Diversificado:** Deduplicación semántica por plantillas y ranking de logs críticos para los incidentes del benchmark, logrando $\text{Recall@5} = 100\%$ y $\text{MRR} = 1.0$.
* **Exportación y Caché:** Genera `processed_data/selected_relevant_logs_for_rca.json`, `processed_data/neurallog_evaluation_metrics.json` y caché nativa NumPy `processed_data/neurallog_emb_{train,val,test}.npy`.

### 🔹 Paso 4: `04_asistente_llm_diagnostico_rca.ipynb`
* **Diagnóstico Autónomo de Causa Raíz con LLM:** Ingesta del paquete multimodal de incidentes (Topología + TranAD+ + Top-K logs de NeuralLog) para generar informes técnicos y planes de remediación estructurados.
* **Auditoría de Prompts SRE:** Visor colapsable HTML (`<details>`, cerrado por defecto) para inspeccionar el prompt multimodal exacto ensamblado para cada incidente sin saturar la vista interactiva.
* **Benchmark Comparativo Multi-Modelo (3 LLMs × 3 Incidentes):**
  - **`gemini-3.5-flash`** (*Cloud Frontier Reasoning* - CoT nativo): Ganador del benchmark con **75.83%** de precisión global media y 14.81s de latencia media (máxima profundidad causal y linaje en cascada).
  - **`gemini-3.1-flash-lite`** (*Cloud Fast / Triage*): 65.07% de precisión global media y latencia mínima de 3.72s (ideal para primera línea de triaje en tiempo real).
  - **`deepseek-r1:14b`** (*Local On-Premises GPU vía Ollama*): 48.57% de precisión, 141.57s de latencia y soberanía absoluta de datos sin salida a nube pública.
* **Capa Unificada Agnóstica (`llm_client.py`):** Integración homogénea entre Google GenAI SDK y LiteLLM, con carga segura de credenciales `.env` y desacoplamiento estricto del razonamiento reflexivo (CoT).
* **Evaluación Cuantitativa de 4 Dimensiones:** Rúbrica objetiva contrastando Componente Raíz (35%), Mecanismo (30%), Cascada (15%) y Remediación (20%) con tablas comparativas por incidente y resumen global de promedios.
* **Demostración Empírica del Valor de NeuralLog (Estudio de Ablación Multi-Incidente de 4 Vías en LLM Ganador):** Evaluación sistemática sobre todos los incidentes de prueba con `gemini-3.5-flash`, demostrando la superioridad de la observabilidad multimodal con **NeuralLog (75.83%)** frente al **Modo Híbrido (68.67%)**, al **Filtro Convencional ERROR/WARN (65.13%)** y al aislamiento métrico **Sin Logs (42.17%)**. Valida empíricamente el fenómeno de vulnerabilidad a distractores (*attention hijacking*) y la necesidad indispensable de la evidencia semántica para evitar alucinaciones en fallos silenciosos.
* **Exportación de Artefactos:** Genera `llms_output/llm_rca_diagnostic_reports.json`, `llms_output/llm_rca_diagnostic_reports.md` y `llms_output/llm_rca_benchmark_metrics.json`.

---

## ⚡ 4. Ejecución de los Cuadernos

### Modo Interactivo (VS Code / Jupyter Lab)
1. Abre el notebook correspondiente (`01_...`, `02_...`, `03_...` o `04_...`).
2. Selecciona el kernel **`Python (TranAD+ GPU)`** en la esquina superior derecha.
3. Configura opcionalmente el proveedor/modelo en la cabecera del Cuaderno 4 si deseas evaluar otro LLM.
4. Ejecuta las celdas de forma secuencial (`Shift + Enter` o *Run All*).

### Modo Automático por Línea de Comandos (Headless CLI)
```bash
conda activate tranad_plus

# Ejecutar Paso 1: Limpieza y Ventanas
jupyter nbconvert --to notebook --execute --inplace 01_normalizacion_datos.ipynb

# Ejecutar Paso 2: Entrenamiento y Evaluación de TranAD+
jupyter nbconvert --to notebook --execute --inplace 02_tranad_plus_entrenamiento.ipynb

# Ejecutar Paso 3: NeuralLog y Selección de Logs Relevantes
jupyter nbconvert --to notebook --execute --inplace 03_neurallog_seleccion_logs_relevantes.ipynb

# Ejecutar Paso 4: Asistente LLM y Diagnóstico RCA Multimodal
jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=600 04_asistente_llm_diagnostico_rca.ipynb
```
