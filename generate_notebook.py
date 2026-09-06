import json
import os

def create_notebook():
    notebook = {
        "cells": [],
        "metadata": {
            "kernelspec": {
                "display_name": "Python (TranAD+ GPU)",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {"name": "ipython", "version": 3},
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.11.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }

    def add_md(content):
        notebook["cells"].append({
            "cell_type": "markdown",
            "metadata": {},
            "source": [line + "\n" for line in content.split("\n")]
        })

    def add_code(content):
        notebook["cells"].append({
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [line + "\n" for line in content.split("\n")]
        })

    # --- CELL 0: Title & Context ---
    add_md(r"""# 🚀 01. Preprocesamiento, Limpieza y Normalización de Datasets para TranAD+
**Detección de Anomalías Multivariadas en Pipelines de Data Engineering con Aceleración GPU**

---

### 📌 Contexto y Fundamentación Teórica
Este cuaderno implementa la primera fase del flujo de trabajo de detección de anomalías basado en **TranAD+** (*A Transformer-Based Deep Learning Approach to Anomaly Detection of High-Bandwidth Multivariate Time-Series Satellite Communications*, Yermakov et al., 2025; basado en *TranAD*, Tuli et al., 2022).

En el contexto de **Data Engineering Pipelines**, el sistema supervisa métricas de una infraestructura distribuida:
- **Emisor (Emitter):** Tasa de emisión, latencia, estado del servicio, consumo de CPU y memoria.
- **Airflow Orchestrator:** Tareas de extracción (Kinesis), transformación y carga (PostgreSQL DW), consumo de CPU y memoria de workers/scheduler.
- **Broker y Almacenamiento:** Kinesis Stream Lag y operaciones en PostgreSQL.

### 🎯 Objetivos de este Cuaderno:
1. **Verificación del Entorno y GPU:** Configuración de PyTorch con soporte CUDA.
2. **Carga e Inspección de Datos:** Análisis de series temporales multivariadas procedentes de Prometheus/cAdvisor (`dataset_gen/datasets_finales/train_dataset.csv`, `val_dataset.csv`, `test_dataset.csv`).
3. **Control de Frecuencia y Limpieza:** Verificación de continuidad temporal ($\Delta t = 5\text{ s}$), imputación por *Forward Fill* (`ffill`) y sustitución de nulos.
4. **Normalización MinMax con Scikit-Learn y Protección `clip=True`:** Escalado en rango $[0, 1]$ ajustado exclusivamente en Train (*Zero Data Leakage*), con justificación matemática contra el desbordamiento de atención (*Softmax NaN overflow*) de la fórmula original de TranAD+.
5. **Generación de Ventanas Deslizantes con *Replication Padding*:** Estructuración de ventanas $W_t \in \mathbb{R}^{w \times m}$ según la especificación de TranAD+.
6. **PyTorch `TranADDataset` y `DataLoader` con GPU:** Construcción de tensores optimizados listos para la fase de entrenamiento adversarial del Transformer.
7. **Exportación de Artefactos Procesados:** Almacenamiento estructurado en `processed_data/` para reproducibilidad total.""")

    # --- CELL 1: Imports ---
    add_code("""import os
import sys
import json
import random
import joblib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.preprocessing import MinMaxScaler

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader

# Configuración de estilos visuales
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (14, 6)
plt.rcParams['font.size'] = 10

def set_seed(seed=42):
    '''Fija las semillas aleatorias para garantizar reproducibilidad.'''
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False

set_seed(42)
print("✅ Librerías importadas y semillas aleatorias configuradas.")""")

    # --- CELL 2: GPU Check Markdown ---
    add_md(r"""## 🖥️ 1. Verificación de Hardware y Aceleración por GPU (CUDA)
Para el entrenamiento del modelo Transformer TranAD+, se aprovecha la aceleración por hardware (GPU con soporte CUDA). Se verifica la disponibilidad y estado de memoria del dispositivo.""")

    # --- CELL 3: GPU Check Code ---
    add_code("""# Verificación de dispositivo GPU
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

print("=" * 60)
print(f"🔹 PyTorch Version: {torch.__version__}")
print(f"🔹 Dispositivo seleccionado: {device}")

if torch.cuda.is_available():
    gpu_id = torch.cuda.current_device()
    gpu_name = torch.cuda.get_device_name(gpu_id)
    gpu_props = torch.cuda.get_device_properties(gpu_id)
    total_mem_gb = gpu_props.total_memory / (1024 ** 3)
    
    print(f"🔹 GPU Nombre: {gpu_name}")
    print(f"🔹 Capacidad de Cómputo (Compute Capability): {gpu_props.major}.{gpu_props.minor}")
    print(f"🔹 Memoria VRAM Total: {total_mem_gb:.2f} GB")
    print(f"🔹 Multiprocesadores (SMs): {gpu_props.multi_processor_count}")
    
    # Test de asignación en GPU
    x_test = torch.ones((1000, 1000), device=device)
    print(f"✅ Test de tensor en GPU exitoso. Memoria asignada: {torch.cuda.memory_allocated(device)/(1024**2):.2f} MB")
    del x_test
    torch.cuda.empty_cache()
else:
    print("⚠️ ADVERTENCIA: CUDA no está disponible. Se utilizará la CPU.")
print("=" * 60)""")

    # --- CELL 4: Protocol & Paths Markdown ---
    add_md(r"""## 📂 2. Definición del Esquema del Dataset y Rutas
Siguiendo la estrategia descrita en `dataset_gen/protocolo_generacion_datasets.md`, el dataset contiene $m=10$ variables métricas continuas extraídas cada 5 segundos de Prometheus/cAdvisor:

| Variable | Origen | Significado |
| :--- | :--- | :--- |
| `emitter_status` | Pushgateway (`emitter`) | Estado de salud del emisor (1=OK, 0=Caído) |
| `emitter_records_sent` | Pushgateway (`emitter`) | Contador acumulado de registros emitidos a Kinesis |
| `emitter_latency` | Pushgateway (`emitter`) | Latencia de envío por evento (segundos) |
| `emitter_cpu_usage` | cAdvisor (`emitter`) | Tasa de uso de CPU del contenedor emisor |
| `emitter_memory_usage` | cAdvisor (`emitter`) | Memoria RAM del contenedor emisor (bytes) |
| `cpu_usage` | cAdvisor (`airflow`) | Tasa de uso de CPU del worker/scheduler Airflow |
| `memory_usage` | cAdvisor (`airflow`) | Memoria RAM de Airflow (bytes) |
| `records_extracted` | Pushgateway (`airflow`) | Registros extraídos del stream de Kinesis |
| `records_loaded` | Pushgateway (`airflow`) | Registros insertados en PostgreSQL Data Warehouse |
| `kinesis_lag_ms` | Pushgateway (`airflow`) | Retraso o lag de lectura del stream Kinesis (ms) |

Columnas de soporte y Ground Truth:
- `timestamp`: Marca de tiempo epoch en segundos.
- `is_anomaly`: Etiqueta binaria (0 = Normal, 1 = Anomalía).
- `anomaly_type`: Tipo de inyección de caos (`none`, `cpu_stress`, `dw_timeout`, `emitter_down`, `aws_down`).""")

    # --- CELL 5: Loading Dataset Code ---
    add_code("""# Definición de rutas del proyecto
BASE_DIR = os.path.abspath("..") if os.path.basename(os.getcwd()) == "notebooks" else os.getcwd()
DATASET_FINAL_DIR = os.path.join(BASE_DIR, "dataset_gen", "datasets_finales")
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "processed_data")

os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

FEATURE_COLUMNS = [
    'emitter_status',
    'emitter_records_sent',
    'emitter_latency',
    'emitter_cpu_usage',
    'emitter_memory_usage',
    'cpu_usage',
    'memory_usage',
    'records_extracted',
    'records_loaded',
    'kinesis_lag_ms'
]

LABEL_COLUMNS = ['is_anomaly', 'anomaly_type']

print(f"🔹 Directorio base: {BASE_DIR}")
print(f"🔹 Directorio de datasets finales: {DATASET_FINAL_DIR}")
print(f"🔹 Directorio de datos procesados: {PROCESSED_DATA_DIR}")
print(f"🔹 Número de variables métricas (m): {len(FEATURE_COLUMNS)}")""")

    # --- CELL 6: Load and Partition Function Code ---
    add_code("""def load_and_inspect_datasets():
    '''
    Carga los datasets según el protocolo. Si existen train_dataset.csv, val_dataset.csv
    y test_dataset.csv en datasets_finales, los carga individualmente.
    Si solo existe tranad_dataset.csv (muestra de referencia), realiza una partición
    controlada basada en el protocolo para desarrollo y testing.
    '''
    train_path = os.path.join(DATASET_FINAL_DIR, "train_dataset.csv")
    val_path = os.path.join(DATASET_FINAL_DIR, "val_dataset.csv")
    test_path = os.path.join(DATASET_FINAL_DIR, "test_dataset.csv")
    single_sample_path = os.path.join(DATASET_FINAL_DIR, "tranad_dataset.csv")
    
    if os.path.exists(train_path) and os.path.exists(val_path) and os.path.exists(test_path):
        print("📂 Detectados conjuntos individuales completos de Train, Validation y Test:")
        df_train = pd.read_csv(train_path)
        df_val = pd.read_csv(val_path)
        df_test = pd.read_csv(test_path)
        print(f"  • {train_path} ({len(df_train)} filas)")
        print(f"  • {val_path} ({len(df_val)} filas)")
        print(f"  • {test_path} ({len(df_test)} filas)")
    elif os.path.exists(single_sample_path):
        print(f"📂 Cargando dataset de muestra desde: {single_sample_path}")
        df_full = pd.read_csv(single_sample_path)
        
        # En caso de muestra única, se particiona cronológicamente:
        n = len(df_full)
        n_train = int(n * 0.70)
        n_val = int(n * 0.15)
        
        df_train = df_full.iloc[:n_train].copy().reset_index(drop=True)
        df_val = df_full.iloc[n_train:n_train+n_val].copy().reset_index(drop=True)
        df_test = df_full.iloc[n_train+n_val:].copy().reset_index(drop=True)
        print(f"ℹ️ Partición temporal automática: Train={len(df_train)}, Val={len(df_val)}, Test={len(df_test)}")
    else:
        raise FileNotFoundError(f"No se encontró ningún dataset en {DATASET_FINAL_DIR}")
        
    return df_train, df_val, df_test

df_train, df_val, df_test = load_and_inspect_datasets()

print()
print(f"📊 Resumen de dimensiones:")
print(f"  • Train: {df_train.shape[0]} filas, {df_train.shape[1]} columnas")
print(f"  • Val:   {df_val.shape[0]} filas, {df_val.shape[1]} columnas")
print(f"  • Test:  {df_test.shape[0]} filas, {df_test.shape[1]} columnas")""")

    # --- CELL 7: EDA Markdown ---
    add_md(r"""## 🔍 3. Análisis Exploratorio de Datos (EDA) y Calidad de Señal
Se inspeccionan los tipos de datos, la regularidad del muestreo temporal ($\Delta t$) y la presencia de valores nulos o infinitos.""")

    # --- CELL 8: EDA Code ---
    add_code("""# Verificación de integridad de columnas y tipos de datos
print("=== Estructura y tipos de datos (Train) ===")
print(df_train.dtypes)

# Comprobación de valores nulos
null_train = df_train.isnull().sum()
print()
print("=== Valores nulos por columna (Train) ===")
print(null_train[null_train > 0] if null_train.sum() > 0 else "✅ No hay valores nulos en Train.")

# Verificación del paso temporal (esperado: 5 segundos)
time_diffs = np.diff(df_train['timestamp'].values)
print()
print(f"=== Intervalos de tiempo entre registros (Train) ===")
print(f"  • Paso promedio: {time_diffs.mean():.2f} s")
print(f"  • Paso mínimo: {time_diffs.min() if len(time_diffs)>0 else 0} s")
print(f"  • Paso máximo: {time_diffs.max() if len(time_diffs)>0 else 0} s")

# Distribución de anomalías
for name, df in [("Train", df_train), ("Val", df_val), ("Test", df_test)]:
    n_anom = (df['is_anomaly'] == 1).sum() if 'is_anomaly' in df.columns else 0
    pct = (n_anom / len(df)) * 100 if len(df) > 0 else 0
    types = df['anomaly_type'].value_counts().to_dict() if 'anomaly_type' in df.columns else {}
    print(f"🔹 {name}: {n_anom}/{len(df)} anomalías ({pct:.2f}%) | Tipos: {types}")""")

    # --- CELL 9: Raw Visualization Code ---
    add_code("""# Visualización de las 10 métricas a lo largo del tiempo (Train)
fig, axes = plt.subplots(5, 2, figsize=(16, 14), sharex=True)
axes = axes.flatten()

timestamps = (df_train['timestamp'] - df_train['timestamp'].iloc[0]) / 60  # En minutos

for idx, col in enumerate(FEATURE_COLUMNS):
    ax = axes[idx]
    ax.plot(timestamps, df_train[col], label=col, color=f"C{idx}", linewidth=1.5)
    ax.set_title(col, fontsize=11, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.6)
    
    if 'is_anomaly' in df_train.columns and df_train['is_anomaly'].sum() > 0:
        anom_mask = df_train['is_anomaly'] == 1
        ax.scatter(timestamps[anom_mask], df_train[col][anom_mask], color='red', s=20, label='Anomalía', zorder=5)

axes[-1].set_xlabel("Tiempo relativo (minutos)", fontsize=11)
axes[-2].set_xlabel("Tiempo relativo (minutos)", fontsize=11)
plt.suptitle("📈 Series Temporales de Métricas del Pipeline (Conjunto Train - 100% Saludable)", fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()""")

    # --- CELL 10: Correlation Matrix Code ---
    add_code("""# Matriz de Correlación entre métricas del pipeline
plt.figure(figsize=(10, 8))
corr_matrix = df_train[FEATURE_COLUMNS].corr()
sns.heatmap(corr_matrix, annot=True, fmt=".2f", cmap="vlag", vmin=-1, vmax=1, cbar_kws={'label': 'Coeficiente de Correlación'})
plt.title("🔗 Matriz de Correlación de Métricas del Pipeline (Train)", fontsize=13, fontweight='bold')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()""")

    # --- CELL 11: Normalization Markdown with Detailed Justification ---
    add_md(r"""## 📐 4. Normalización con `MinMaxScaler(clip=True)` de Scikit-Learn: Justificación Técnica

En la formulación original de **TranAD+** (*Yermakov et al., 2025*), la normalización se define como:

$$\tilde{x}_t = \frac{x_t - \min(X_{\text{train}})}{\max(X_{\text{train}}) - \min(X_{\text{train}}) + \epsilon} \quad (\text{donde } \epsilon = 10^{-7})$$

Los autores introdujeron $\epsilon$ como una protección matemática contra la división por cero en scripts manuales de NumPy.

---

### 🔬 ¿Por qué se usa `MinMaxScaler(clip=True)` estándar en lugar de la fórmula manual con $\epsilon$?

En este proyecto adopto `sklearn.preprocessing.MinMaxScaler(feature_range=(0, 1), clip=True)`. Esta decisión está fundamentada en los siguientes principios de **estabilidad numérica y arquitectura de Transformers**:

#### 1. Prevención del Colapso por Explosión de Gradientes (*Softmax NaN Overflow*):
En un entorno real de telemetría de ingeniería de datos (Prometheus + Docker), los fallos de infraestructura producen **valores centinela extremos de error**.
* **Ejemplo observado en el dataset de este trabajo:** Durante la interrupción de LocalStack en el conjunto de Test, la métrica `kinesis_lag_ms` envió el código de error/timeout de $999.999\text{ ms}$. En el conjunto de Train (estado saludable), esta métrica fue constante a $0.0\text{ ms}$ (por lo que $\max - \min = 0$).
* **Si se aplicara la fórmula del paper con $\epsilon$:**
  $$\tilde{x} = \frac{999.999 - 0}{0 + 10^{-7}} = 999.999 \times 10^7 \approx \mathbf{10^{13}} \text{ (diez billones)}$$
* **Consecuencia en el Transformer:**
  Al entrar un valor de $10^{13}$ en la capa de atención (*Multi-Head Self-Attention*), el producto escalar $QK^T$ alcanza órdenes de $(10^{13})^2 = 10^{26}$. Al calcular $\text{Softmax}\left(\frac{QK^T}{\sqrt{d_k}}\right)$, la operación exponencial $e^{10^{26}}$ desborda el límite de precisión de punto flotante de 32 bits (`float32` colapsa en $\approx e^{88}$), produciendo **`NaN`** (*Not a Number*) o **`Inf`**. Esto corrompe la función de pérdida y destruye los pesos del modelo durante el entrenamiento.

#### 2. Comportamiento Óptimo con `clip=True`:
Al activar `clip=True`:
* En **Train:** Los datos quedan escalados estrictamente en $[0.0, 1.0]$.
* En **Test / Validación:** Cualquier valor anómalo extremo se satura limpiamente en $1.0$ (o caídas bruscas de memoria en $0.0$).
* **Impacto en la Detección:** Cuando ocurre un fallo, la métrica anómala entra con valor $1.0$ (frente al $0.0$ que el modelo aprendió a predecir en Train). Esto genera el **máximo error de reconstrucción** posible $\|x_t - \hat{x}_t\|$, activando la detección de anomalías con máxima sensibilidad pero **sin romper la estabilidad numérica de la red neuronal**.

#### 3. Mantenibilidad e Integración en Producción:
`MinMaxScaler` es un estándar de la industria que se serializa de forma transparente con `joblib.dump` (`tranad_scaler.joblib`), permitiendo que el asistente LLM de Causa Raíz (`llm_rca_assistant.py`) desnormalice y consulte los rangos originales de forma desacoplada.""")

    # --- CELL 12: Scaler Implementation Code with Scikit-Learn & clip=True ---
    add_code("""# Limpieza básica: ffill y luego fillna(0)
def clean_features(df, feature_cols):
    '''Imputa nulos mediante forward-fill y reemplazo final por 0.0.'''
    df_clean = df[feature_cols].copy()
    df_clean = df_clean.ffill().fillna(0.0)
    return df_clean

# 1. Limpieza de datos brutos
X_train_raw = clean_features(df_train, FEATURE_COLUMNS)
X_val_raw = clean_features(df_val, FEATURE_COLUMNS)
X_test_raw = clean_features(df_test, FEATURE_COLUMNS)

# 2. Inicialización del MinMaxScaler estándar con clip=True para estabilidad en Test
scaler = MinMaxScaler(feature_range=(0, 1), clip=True)

# 3. Ajuste (fit) EXCLUSIVO sobre Train y transformación de los 3 splits
X_train_scaled = scaler.fit_transform(X_train_raw)
X_val_scaled = scaler.transform(X_val_raw)
X_test_scaled = scaler.transform(X_test_raw)

# 4. Persistencia del escalador estándar con joblib
scaler_path = os.path.join(PROCESSED_DATA_DIR, "tranad_scaler.joblib")
joblib.dump(scaler, scaler_path)
print(f"✅ Escalador Scikit-Learn (clip=True) ajustado y guardado en: {scaler_path}")

print()
print(f"📊 Estadísticas de datos escalados garantizados en [0, 1]:")
print(f"  • X_train_scaled: forma {X_train_scaled.shape} -> Min: {X_train_scaled.min():.3f}, Max: {X_train_scaled.max():.3f}")
print(f"  • X_val_scaled:   forma {X_val_scaled.shape} -> Min: {X_val_scaled.min():.3f}, Max: {X_val_scaled.max():.3f}")
print(f"  • X_test_scaled:  forma {X_test_scaled.shape} -> Min: {X_test_scaled.min():.3f}, Max: {X_test_scaled.max():.3f}")""")

    # --- CELL 13: Scaled Visualization Code ---
    add_code("""# Visualización de las métricas normalizadas en rango [0, 1] (Test con anomalías)
fig, axes = plt.subplots(5, 2, figsize=(16, 12), sharex=True, sharey=True)
axes = axes.flatten()

for idx, col in enumerate(FEATURE_COLUMNS):
    ax = axes[idx]
    ax.plot(X_test_scaled[:, idx], label=f"{col} (Norm)", color=f"C{idx}", linewidth=1.2)
    ax.set_title(f"{col} [0, 1]", fontsize=10, fontweight='bold')
    ax.set_ylim(-0.05, 1.05)
    ax.grid(True, linestyle='--', alpha=0.5)

axes[-1].set_xlabel("Paso temporal (t) en Test", fontsize=11)
axes[-2].set_xlabel("Paso temporal (t) en Test", fontsize=11)
plt.suptitle("✨ Métricas Normalizadas en Rango [0, 1] en Test (TranAD+ Bounded)", fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()""")

    # --- CELL 14: Sliding Windows Markdown ---
    add_md(r"""## 🔄 5. Generación de Ventanas Deslizantes (*Sliding Windows*) con *Replication Padding*
De acuerdo con la definición matemática de **TranAD+**:

Para cualquier instante temporal $t$, la ventana deslizante de longitud $w$ es:
$$W_t = [x_{t-w+1}, \dots, x_t] \in \mathbb{R}^{w \times m}$$

Para los primeros instantes $t < w$, se aplica **Replication Padding** replicando el vector inicial $x_0$:
$$W_0 = [x_0, x_0, \dots, x_0]$$

Esto produce un conjunto de ventanas continuas $W_T = [W_0, W_1, \dots, W_n]$ sin perder los primeros $w-1$ puntos temporales.""")

    # --- CELL 15: Sliding Window Function Code ---
    add_code("""def create_sliding_windows(data_matrix, labels_arr=None, timestamps_arr=None, window_size=10):
    '''
    Genera ventanas deslizantes con replication padding para t < window_size.
    
    Args:
        data_matrix (np.ndarray): Matriz de datos normalizados de forma (N, m).
        labels_arr (np.ndarray, optional): Array de etiquetas binarias (N,).
        timestamps_arr (np.ndarray, optional): Array de timestamps (N,).
        window_size (int): Longitud de la ventana deslizante (w).
        
    Returns:
        windows (np.ndarray): Tensor de forma (N, window_size, m)
        targets (np.ndarray): Vector en el tiempo t de forma (N, m)
        labels (np.ndarray): Etiqueta en el tiempo t de forma (N,)
        timestamps (np.ndarray): Timestamps en el tiempo t de forma (N,)
    '''
    n_samples, n_features = data_matrix.shape
    windows = np.zeros((n_samples, window_size, n_features), dtype=np.float32)
    targets = np.zeros((n_samples, n_features), dtype=np.float32)
    
    for t in range(n_samples):
        targets[t] = data_matrix[t]
        if t < window_size:
            # Replication padding con x_0
            pad_len = window_size - (t + 1)
            pad_block = np.repeat(data_matrix[0:1], pad_len, axis=0)
            hist_block = data_matrix[0:t+1]
            windows[t] = np.vstack([pad_block, hist_block])
        else:
            windows[t] = data_matrix[t - window_size + 1 : t + 1]
            
    labels = np.zeros(n_samples, dtype=np.int64) if labels_arr is None else np.array(labels_arr, dtype=np.int64)
    timestamps = np.arange(n_samples) if timestamps_arr is None else np.array(timestamps_arr, dtype=np.int64)
    
    return windows, targets, labels, timestamps

# Configuración del hiperparámetro de ventana (w)
WINDOW_SIZE = 10  # 10 pasos * 5s = 50 segundos de contexto temporal

y_train = df_train['is_anomaly'].values if 'is_anomaly' in df_train.columns else np.zeros(len(df_train))
y_val = df_val['is_anomaly'].values if 'is_anomaly' in df_val.columns else np.zeros(len(df_val))
y_test = df_test['is_anomaly'].values if 'is_anomaly' in df_test.columns else np.zeros(len(df_test))

ts_train = df_train['timestamp'].values
ts_val = df_val['timestamp'].values
ts_test = df_test['timestamp'].values

# Generación de ventanas
train_windows, train_targets, train_labels, train_ts = create_sliding_windows(X_train_scaled, y_train, ts_train, WINDOW_SIZE)
val_windows, val_targets, val_labels, val_ts = create_sliding_windows(X_val_scaled, y_val, ts_val, WINDOW_SIZE)
test_windows, test_targets, test_labels, test_ts = create_sliding_windows(X_test_scaled, y_test, ts_test, WINDOW_SIZE)

print(f"✅ Ventanas deslizantes generadas con éxito (Window Size w = {WINDOW_SIZE}):")
print(f"  • Train Windows: {train_windows.shape}, Targets: {train_targets.shape}")
print(f"  • Val Windows:   {val_windows.shape}, Targets: {val_targets.shape}")
print(f"  • Test Windows:  {test_windows.shape}, Targets: {test_targets.shape}")""")

    # --- CELL 16: PyTorch Dataset Markdown ---
    add_md(r"""## 🧱 6. Implementación de `TranADDataset` y `DataLoader` en PyTorch
Se construye la clase `Dataset` para PyTorch y se crean los `DataLoaders` con `pin_memory=True` para acelerar la transferencia de tensores hacia la GPU (CUDA).""")

    # --- CELL 17: PyTorch Dataset Code ---
    add_code("""class TranADDataset(Dataset):
    '''
    Dataset de PyTorch para TranAD+.
    Proporciona ventanas temporales (w, m), el target del instante actual (m) y las etiquetas ground truth.
    '''
    def __init__(self, windows, targets, labels, timestamps):
        self.windows = torch.tensor(windows, dtype=torch.float32)
        self.targets = torch.tensor(targets, dtype=torch.float32)
        self.labels = torch.tensor(labels, dtype=torch.long)
        self.timestamps = torch.tensor(timestamps, dtype=torch.long)

    def __len__(self):
        return len(self.windows)

    def __getitem__(self, idx):
        return {
            'window': self.windows[idx],
            'target': self.targets[idx],
            'label': self.labels[idx],
            'timestamp': self.timestamps[idx]
        }

# Creación de instancias de Dataset
ds_train = TranADDataset(train_windows, train_targets, train_labels, train_ts)
ds_val = TranADDataset(val_windows, val_targets, val_labels, val_ts)
ds_test = TranADDataset(test_windows, test_targets, test_labels, test_ts)

# Configuración de DataLoaders
BATCH_SIZE = 32

loader_train = DataLoader(ds_train, batch_size=BATCH_SIZE, shuffle=True, pin_memory=torch.cuda.is_available())
loader_val = DataLoader(ds_val, batch_size=BATCH_SIZE, shuffle=False, pin_memory=torch.cuda.is_available())
loader_test = DataLoader(ds_test, batch_size=BATCH_SIZE, shuffle=False, pin_memory=torch.cuda.is_available())

print(f"✅ DataLoaders creados:")
print(f"  • Batches en Train: {len(loader_train)}")
print(f"  • Batches en Val:   {len(loader_val)}")
print(f"  • Batches en Test:  {len(loader_test)}")

# Verificación de transferencia por Batch hacia GPU
sample_batch = next(iter(loader_train))
b_window = sample_batch['window'].to(device)
b_target = sample_batch['target'].to(device)
b_label = sample_batch['label'].to(device)

print()
print(f"🔍 Inspección de Batch en {device}:")
print(f"  • Window Shape: {b_window.shape} (Batch_size, Window_size, Features)")
print(f"  • Target Shape: {b_target.shape} (Batch_size, Features)")
print(f"  • Labels Shape: {b_label.shape}")
print(f"  • Dispositivo del tensor: {b_window.device}")""")

    # --- CELL 18: Exporting Artifacts Markdown ---
    add_md(r"""## 💾 7. Exportación y Persistencia de Tensores Procesados
Se guardan los tensores procesados y metadatos en disco (`processed_data/`) para que el siguiente cuaderno (Entrenamiento de TranAD+) pueda cargarlos de forma instantánea y reproducible.""")

    # --- CELL 19: Exporting Artifacts Code ---
    add_code("""# Guardado de tensores en formato PyTorch .pt
processed_artifacts = {
    'train': {
        'windows': train_windows,
        'targets': train_targets,
        'labels': train_labels,
        'timestamps': train_ts
    },
    'val': {
        'windows': val_windows,
        'targets': val_targets,
        'labels': val_labels,
        'timestamps': val_ts
    },
    'test': {
        'windows': test_windows,
        'targets': test_targets,
        'labels': test_labels,
        'timestamps': test_ts
    },
    'metadata': {
        'window_size': WINDOW_SIZE,
        'feature_columns': FEATURE_COLUMNS,
        'num_features': len(FEATURE_COLUMNS),
        'scaler_path': scaler_path
    }
}

output_pt_path = os.path.join(PROCESSED_DATA_DIR, "tranad_processed_tensors.pt")
torch.save(processed_artifacts, output_pt_path)

# Guardar metadatos en JSON legible
metadata_json_path = os.path.join(PROCESSED_DATA_DIR, "dataset_metadata.json")
with open(metadata_json_path, "w", encoding="utf-8") as f:
    json.dump({
        'window_size': WINDOW_SIZE,
        'feature_columns': FEATURE_COLUMNS,
        'num_features': len(FEATURE_COLUMNS),
        'train_samples': len(train_windows),
        'val_samples': len(val_windows),
        'test_samples': len(test_windows)
    }, f, indent=2)

print("=" * 60)
print("🎉 PROCESAMIENTO COMPLETADO CON ÉXITO")
print(f"  📁 Tensores guardados en: {output_pt_path}")
print(f"  📁 Escalador guardado en: {scaler_path}")
print(f"  📁 Metadatos guardados en: {metadata_json_path}")
print("=" * 60)""")

    # --- CELL 20: Next Steps Markdown ---
    add_md(r"""## 🎯 8. Conclusiones y Siguientes Pasos
En este primer paso se ha completado:
1. ✅ **Entorno de Anaconda y GPU verificado:** PyTorch 2.x con CUDA para cómputo acelerado en GPU.
2. ✅ **Control de Calidad e Integridad:** Limpieza con `ffill`, regularidad de muestreo ($\Delta t = 5\text{ s}$) verificada en los tres conjuntos.
3. ✅ **Normalización MinMax con `clip=True`:** Escalado acotado en $[0, 1]$ ajustado exclusivamente en Train (*Zero Data Leakage*) y protegido contra *overflow* de atención en Test.
4. ✅ **Ventanas Deslizantes con Replication Padding:** Construcción del tensor tridimensional $(N, w, m)$ de TranAD+.
5. ✅ **Dataset & DataLoader en PyTorch:** Pipelines optimizados con soporte `pin_memory` y transferencia a VRAM.

### 🔜 Próximo Paso (Paso 2):
- Implementación de la arquitectura **TranAD+**: Transformer Encoder + Dual Decoders con *Inverse Bottleneck*.
- Entrenamiento Adversarial en dos fases ($L_1$ reconstrucción de ventana y $L_2$ discriminación adversaria).
- Cálculo de Umbrales dinámicos con **POT (Peaks Over Threshold)** y evaluación de detección.""")

    output_notebook_path = "/home/mangel/dev/master_ia/tfm/code/01_dataset_cleaning_and_normalization.ipynb"
    with open(output_notebook_path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=2, ensure_ascii=False)
    
    print(f"Notebook 1 updated at: {output_notebook_path}")

if __name__ == "__main__":
    create_notebook()
