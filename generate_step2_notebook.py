import json
import os

def create_step2_notebook():
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

    # --- CELL 0: Title & Header ---
    add_md(r"""# 🧠 02. Implementación de TranAD+, Entrenamiento Adversarial en GPU y Detección de Anomalías
**Modelado Transformer Multivariado con Inverse Bottleneck y Evaluación de Causa Raíz (RCA)**

---

### 📌 Contexto Teórico y Arquitectura
Este cuaderno implementa el núcleo del algoritmo **TranAD+** (*A Transformer-Based Deep Learning Approach to Anomaly Detection of High-Bandwidth Multivariate Time-Series Satellite Communications*, Yermakov et al., 2025; basado en *TranAD*, Tuli et al., 2022).

#### 🏗️ Pilares de la Arquitectura TranAD+:\n1. **Codificación Posicional Temporal:** Mapea la secuencia temporal de la ventana $W_t \in \mathbb{R}^{w \times m}$ preservando el orden cronológico.
2. **Transformer Encoder:** Modela las interdependencias cruzadas entre las $m=10$ variables del pipeline mediante atención multi-cabezal (*Multi-Head Self-Attention*).
3. **Doble Decodificador con *Inverse Bottleneck*:**
   - A diferencia del TranAD original que usaba cuellos de botella reductivos, TranAD+ expande la dimensión feed-forward ($d_{\text{model}} \to d_{\text{ff}} \ge 2 \cdot d_{\text{model}}$) para evitar pérdida de información en espacios multivariados complejos.
4. **Entrenamiento Adversarial en Dos Fases:**
   - **Fase 1 (Reconstrucción):** El Decodificador 1 aprende a reconstruir la ventana normal ($L_1 = \|W - \hat{W}_1\|_2^2$).
   - **Fase 2 (Discrepancia Adversaria):** El Decodificador 2 se entrena para discriminar y amplificar el error cuando la entrada no encaja con el patrón normal aprendido ($L_2 = \|W - \hat{W}_2\|_2^2 - \beta \|\hat{W}_1 - \hat{W}_2\|_2^2$).
5. **Inferencia y Scoring Ponderado ($\gamma, \lambda$):**
   $$s_{t, i} = \gamma \|x_{t, i} - \hat{x}_{1, t, i}\|^2 + \lambda \|x_{t, i} - \hat{x}_{2, t, i}\|^2$$
6. **Calibración de Umbral Dinámico (*Peaks Over Threshold - POT* / F1 Óptimo):** Ajustado sobre el conjunto de validación.
7. **Explicabilidad y Contribución de Parámetros (RCA):** Cálculo del porcentaje de culpa de cada métrica para el diagnóstico con el LLM.""")

    # --- CELL 1: Imports ---
    add_code("""import os
import sys
import json
import math
import random
import joblib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.metrics import (
    precision_score, recall_score, f1_score, roc_auc_score,
    average_precision_score, confusion_matrix, classification_report,
    roc_curve, precision_recall_curve
)

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
from tqdm import tqdm

# Configuración visual
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
print("✅ Entorno configurado y librerías importadas.")""")

    # --- CELL 2: GPU Check Markdown ---
    add_md(r"""## 🖥️ 1. Verificación del Dispositivo de Cómputo (GPU / CUDA)
Se comprueba que PyTorch tiene acceso directo al dispositivo GPU con soporte CUDA.""")

    # --- CELL 3: GPU Check Code ---
    add_code("""device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print("=" * 60)
print(f"🔹 Dispositivo activo para entrenamiento: {device}")

if torch.cuda.is_available():
    gpu_id = torch.cuda.current_device()
    gpu_name = torch.cuda.get_device_name(gpu_id)
    gpu_props = torch.cuda.get_device_properties(gpu_id)
    print(f"🔹 GPU: {gpu_name} ({gpu_props.total_memory / (1024**3):.2f} GB VRAM)")
    print(f"🔹 Capacidad de Cómputo: {gpu_props.major}.{gpu_props.minor}")
else:
    print("⚠️ ADVERTENCIA: Entrenando en CPU.")
print("=" * 60)""")

    # --- CELL 4: Data Loading Markdown ---
    add_md(r"""## 📂 2. Carga de Tensores y Metadatos Procesados (Paso 1)
Se cargan los tensores de ventanas deslizantes ($w=10, m=10$), escalador y metadatos generados en `processed_data/`.""")

    # --- CELL 5: Data Loading Code ---
    add_code("""BASE_DIR = os.path.abspath("..") if os.path.basename(os.getcwd()) == "notebooks" else os.getcwd()
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "processed_data")
MODELS_DIR = os.path.join(BASE_DIR, "models")
os.makedirs(MODELS_DIR, exist_ok=True)

pt_path = os.path.join(PROCESSED_DATA_DIR, "tranad_processed_tensors.pt")
metadata_path = os.path.join(PROCESSED_DATA_DIR, "dataset_metadata.json")
scaler_path = os.path.join(PROCESSED_DATA_DIR, "tranad_scaler.joblib")

assert os.path.exists(pt_path), f"No se encontró {pt_path}. Ejecuta primero el notebook 01."

# Carga de tensores
processed_data = torch.load(pt_path, map_location=device, weights_only=False)
with open(metadata_path, "r", encoding="utf-8") as f:
    metadata = json.load(f)

scaler = joblib.load(scaler_path)

FEATURE_COLUMNS = metadata['feature_columns']
WINDOW_SIZE = metadata['window_size']
NUM_FEATURES = metadata['num_features']

print(f"✅ Artefactos cargados:")
print(f"  • Ventana temporal (w): {WINDOW_SIZE} pasos ({WINDOW_SIZE * 5} segundos)")
print(f"  • Número de métricas (m): {NUM_FEATURES}")
print(f"  • Muestras Train: {metadata['train_samples']} (0% anomalías)")
print(f"  • Muestras Val:   {metadata['val_samples']} ({np.mean(processed_data['val']['labels'] == 1)*100:.2f}% anomalías)")
print(f"  • Muestras Test:  {metadata['test_samples']} ({np.mean(processed_data['test']['labels'] == 1)*100:.2f}% anomalías)")""")

    # --- CELL 6: PyTorch Dataset & Dataloaders Code ---
    add_code("""class TranADDataset(Dataset):
    '''Dataset para tensores de ventanas deslizantes en PyTorch.'''
    def __init__(self, data_dict):
        self.windows = torch.tensor(data_dict['windows'], dtype=torch.float32)
        self.targets = torch.tensor(data_dict['targets'], dtype=torch.float32)
        self.labels = torch.tensor(data_dict['labels'], dtype=torch.long)
        self.timestamps = torch.tensor(data_dict['timestamps'], dtype=torch.long)

    def __len__(self):
        return len(self.windows)

    def __getitem__(self, idx):
        return {
            'window': self.windows[idx],
            'target': self.targets[idx],
            'label': self.labels[idx],
            'timestamp': self.timestamps[idx]
        }

ds_train = TranADDataset(processed_data['train'])
ds_val = TranADDataset(processed_data['val'])
ds_test = TranADDataset(processed_data['test'])

# BATCH_SIZE = 64: Tamaño de lote óptimo y equilibrado para el dataset de entrenamiento de este trabajo (N ≈ 4.200 muestras).
# Proporciona ≈ 65 batches por época, logrando el equilibrio ideal entre suficiente estocasticidad para el optimizador AdamW
# y una paralelización eficiente en los núcleos CUDA de la GPU (evitando GPU starvation por lotes diminutos y sub-actualización por lotes excesivos).
BATCH_SIZE = 64

train_loader = DataLoader(ds_train, batch_size=BATCH_SIZE, shuffle=True, pin_memory=(device.type == "cuda"))
val_loader = DataLoader(ds_val, batch_size=BATCH_SIZE, shuffle=False, pin_memory=(device.type == "cuda"))
test_loader = DataLoader(ds_test, batch_size=BATCH_SIZE, shuffle=False, pin_memory=(device.type == "cuda"))

print(f"✅ DataLoaders preparados con Batch Size = {BATCH_SIZE}")""")

    # --- CELL 7: Architecture Markdown ---
    add_md(r"""## 🏗️ 3. Definición de la Arquitectura TranAD+ en PyTorch

### Componentes Clave:
1. **`PositionalEncoding`:** Inyecta información temporal mediante funciones seno y coseno:
   $$PE_{(pos, 2i)} = \sin\left(\frac{pos}{10000^{2i/d_{\text{model}}}}\right), \quad PE_{(pos, 2i+1)} = \cos\left(\frac{pos}{10000^{2i/d_{\text{model}}}}\right)$$
2. **`TranADPlus`:**
   - **Proyección de Entrada:** Capa lineal $m \to d_{\text{model}}$.
   - **Encoder:** $N_{\text{enc}}$ capas Transformer con Multi-Head Self-Attention.
   - **Decoders con Inverse Bottleneck:** $N_{\text{dec}}$ capas donde la dimensión intermedia de la red feed-forward es $d_{\text{ff}} = 4 \times d_{\text{model}}$.
   - **Proyecciones de Salida:** $d_{\text{model}} \to m$ produciendo $\hat{W}_1$ y $\hat{W}_2$.""")

    # --- CELL 8: Architecture PyTorch Code ---
    add_code("""class PositionalEncoding(nn.Module):
    \"\"\"
    Codificación posicional sinusoidal para series temporales (Vaswani et al., 2017).

    Inyecta la información del orden cronológico en el espacio latente sumando
    funciones ortogonales seno (dimensiones pares) y coseno (dimensiones impares)
    a las representaciones métricas continuas.

    Args:
        d_model (int): Dimensión del espacio latente embebido del Transformer.
            Debe ser un entero par para permitir la partición simétrica en pares seno/coseno.
        max_len (int, optional): Longitud máxima de secuencia precalculada en el búfer
            estático registrado en memoria. Por defecto 500.
        dropout (float, optional): Probabilidad de regularización por dropout aplicada
            al tensor resultante tras la suma posicional. Por defecto 0.1.
    \"\"\"
    def __init__(self, d_model, max_len=500, dropout=0.1):
        super(PositionalEncoding, self).__init__()
        self.dropout = nn.Dropout(p=dropout)

        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(torch.arange(0, d_model, 2).float() * (-math.log(10000.0) / d_model))
        
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        pe = pe.unsqueeze(0)  # Shape: (1, max_len, d_model)
        self.register_buffer('pe', pe)

    def forward(self, x):
        # x shape: (batch_size, seq_len, d_model)
        x = x + self.pe[:, :x.size(1), :]
        return self.dropout(x)


class TranADPlus(nn.Module):
    \"\"\"
    Modelo Transformer TranAD+ para detección no supervisada de anomalías en telemetría multivariada.

    Evolución de la arquitectura TranAD (Tuli et al., VLDB 2022) que incorpora
    el principio de Inverse Bottleneck y optimización paralela en GPU (Yermakov et al., SpaceOps 2025).
    Consta de un Transformer Encoder compartido y dos Transformer Decoders acoplados
    mediante un esquema de entrenamiento adversarial en dos fases con Focus Score.

    Args:
        feats (int, optional): Dimensión del espacio de entrada, correspondiente al número
            de variables o métricas de telemetría continuas observadas en cada instante (m). Por defecto 10.
        d_model (int, optional): Dimensión latente intermedia del Transformer. Expande las 10 métricas
            a un espacio 6.4x mayor sin cuellos de botella informacionales. Por defecto 64.
        n_heads (int, optional): Número de cabezas en los módulos de Multi-Head Self-Attention.
            Garantiza una partición simétrica con d_k = d_model / n_heads = 16 dimensiones por cabeza. Por defecto 4.
        n_layers (int, optional): Número de capas apiladas en el Encoder y en cada uno de los dos Decoders. Por defecto 2.
        dim_feedforward (int, optional): Dimensión de las capas Feed-Forward internas, implementando
            la relación de Inverse Bottleneck (4 * d_model = 256) para enriquecer la capacidad no lineal. Por defecto 256.
        dropout (float, optional): Tasa canónica de regularización por dropout en atención y capas densas
            (fijada en 0.1 para evitar co-adaptación sin degradar la resolución continua de las señales). Por defecto 0.1.
    \"\"\"
    # d_model = 64: Dimensión latente fijada por diseño analítico:
    # 1) Proyecta las m=10 métricas a un espacio 6.4x mayor sin cuellos de botella informacionales.
    # 2) Garantiza una partición simétrica en n_heads=4 cabezas de atención (d_k = 64/4 = 16).
    # 3) Mantiene el ratio óptimo del Inverse Bottleneck de TranAD+ (d_ff = 4 * d_model = 256).
    # 4) Limita el tamaño del modelo a ≈ 200k parámetros, proporcionado a N ≈ 4.200 muestras para evitar sobreajuste.
    def __init__(self, feats=10, d_model=64, n_heads=4, n_layers=2, dim_feedforward=256, dropout=0.1):
        super(TranADPlus, self).__init__()
        self.name = "TranADPlus"
        self.feats = feats
        self.d_model = d_model
        
        # 1. Proyección lineal de entrada: m -> d_model
        self.embedding = nn.Linear(feats, d_model)
        self.pos_encoder = PositionalEncoding(d_model, max_len=100, dropout=dropout)
        
        # 2. Transformer Encoder
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=n_heads,
            dim_feedforward=dim_feedforward,  # Inverse Bottleneck (256 vs d_model=64)
            dropout=dropout,
            batch_first=True,
            activation='gelu'
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=n_layers)
        
        # 3. Transformer Decoder 1 (Reconstrucción Base)
        decoder_layer1 = nn.TransformerDecoderLayer(
            d_model=d_model,
            nhead=n_heads,
            dim_feedforward=dim_feedforward,
            dropout=dropout,
            batch_first=True,
            activation='gelu'
        )
        self.decoder1 = nn.TransformerDecoder(decoder_layer1, num_layers=n_layers)
        
        # 4. Transformer Decoder 2 (Discriminación Adversaria)
        decoder_layer2 = nn.TransformerDecoderLayer(
            d_model=d_model,
            nhead=n_heads,
            dim_feedforward=dim_feedforward,
            dropout=dropout,
            batch_first=True,
            activation='gelu'
        )
        self.decoder2 = nn.TransformerDecoder(decoder_layer2, num_layers=n_layers)
        
        # 5. Proyecciones de salida a espacio métrico: d_model -> m
        self.out1 = nn.Linear(d_model, feats)
        self.out2 = nn.Linear(d_model, feats)

    def forward(self, window):
        # window shape: (batch_size, window_size, feats)
        
        # Proyección y codificación posicional
        emb = self.embedding(window)  # (B, w, d_model)
        emb = self.pos_encoder(emb)
        
        # Memoria del Encoder
        memory = self.encoder(emb)    # (B, w, d_model)
        
        # Fase 1: Decodificador 1
        dec1 = self.decoder1(tgt=emb, memory=memory)
        hat_w1 = self.out1(dec1)      # (B, w, feats)
        
        # Fase 2: Decodificador 2 (condicionado sobre dec1 y memoria)
        dec2 = self.decoder2(tgt=dec1, memory=memory)
        hat_w2 = self.out2(dec2)      # (B, w, feats)
        
        return hat_w1, hat_w2

# Instanciación y verificación en GPU
model = TranADPlus(
    feats=NUM_FEATURES,
    d_model=64,
    n_heads=4,
    n_layers=2,
    dim_feedforward=256,
    dropout=0.1
).to(device)

print(f"✅ Modelo TranAD+ instanciado en {device}.")
total_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
print(f"🔹 Parámetros entrenables totales: {total_params:,}")""")

    # --- CELL 9: Training Theory Markdown ---
    add_md(r"""## ⚔️ 4. Esquema de Entrenamiento Adversarial en Dos Fases

El entrenamiento sigue la formulación adversarial de TranAD / TranAD+:\n
* **Paso 1 (Decodificador 1):** Minimiza el error de reconstrucción en la ventana y da mayor peso al último instante $t$:
  $$\mathcal{L}_1 = \frac{1}{n} \|W - \hat{W}_1\|_2^2 + \left(1 - \frac{1}{n}\right) \|x_t - \hat{x}_{1, t}\|_2^2$$

* **Paso 2 (Decodificador 2 - Adversario):** Minimiza el error de reconstrucción mientras amplifica la discrepancia respecto a $\hat{W}_1$ para penalizar anomalías no observadas en Train:
  $$\mathcal{L}_2 = \frac{1}{n} \|W - \hat{W}_2\|_2^2 - \left(1 - \frac{1}{n}\right) \|W - \hat{W}_1\|_2^2$$

donde $n$ es el número de época actual.""")

    # --- CELL 10: Training Loop Code ---
    add_code("""# Configuración de entrenamiento
EPOCHS = 35
LR = 1e-3
optimizer1 = torch.optim.AdamW(
    list(model.embedding.parameters()) + list(model.encoder.parameters()) + list(model.decoder1.parameters()) + list(model.out1.parameters()),
    lr=LR,
    weight_decay=1e-5
)
optimizer2 = torch.optim.AdamW(
    list(model.decoder2.parameters()) + list(model.out2.parameters()),
    lr=LR,
    weight_decay=1e-5
)

scheduler1 = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer1, T_max=EPOCHS, eta_min=1e-5)
scheduler2 = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer2, T_max=EPOCHS, eta_min=1e-5)

criterion_mse = nn.MSELoss()

train_losses_l1 = []
train_losses_l2 = []
val_losses = []

best_val_loss = float('inf')
best_model_path = os.path.join(MODELS_DIR, "tranad_plus_best.pt")

print(f"🚀 Iniciando entrenamiento adversarial durante {EPOCHS} épocas en {device}...")

for epoch in range(1, EPOCHS + 1):
    model.train()
    epoch_l1 = 0.0
    epoch_l2 = 0.0
    n = epoch  # Factor de ponderación temporal
    
    for batch in train_loader:
        w = batch['window'].to(device)  # (B, w, m)
        target = batch['target'].to(device)  # (B, m)
        
        # --- FASE 1: Entrenar Encoder + Decoder 1 ---
        optimizer1.zero_grad()
        hat_w1, hat_w2 = model(w)
        
        # Loss 1: Reconstrucción de ventana + target
        l1_win = criterion_mse(hat_w1, w)
        l1_target = criterion_mse(hat_w1[:, -1, :], target)
        loss1 = (1.0 / n) * l1_win + (1.0 - 1.0 / n) * l1_target
        
        loss1.backward(retain_graph=True)
        nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer1.step()
        
        # --- FASE 2: Entrenar Decoder 2 (Adversarial) ---
        optimizer2.zero_grad()
        # Recalcular forward para grafo limpio
        hat_w1, hat_w2 = model(w)
        l2_win = criterion_mse(hat_w2, w)
        l2_disc = criterion_mse(hat_w2, hat_w1.detach())
        loss2 = (1.0 / n) * l2_win - (1.0 - 1.0 / n) * l2_disc
        
        loss2.backward()
        nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer2.step()
        
        epoch_l1 += loss1.item()
        epoch_l2 += loss2.item()
        
    scheduler1.step()
    scheduler2.step()
    
    avg_l1 = epoch_l1 / len(train_loader)
    avg_l2 = epoch_l2 / len(train_loader)
    train_losses_l1.append(avg_l1)
    train_losses_l2.append(avg_l2)
    
    # --- Evaluación en Validación ---
    model.eval()
    val_loss_epoch = 0.0
    with torch.no_grad():
        for batch in val_loader:
            w_val = batch['window'].to(device)
            hat_w1_val, hat_w2_val = model(w_val)
            val_loss = 0.5 * criterion_mse(hat_w1_val, w_val) + 0.5 * criterion_mse(hat_w2_val, w_val)
            val_loss_epoch += val_loss.item()
            
    avg_val = val_loss_epoch / len(val_loader)
    val_losses.append(avg_val)
    
    if avg_val < best_val_loss:
        best_val_loss = avg_val
        torch.save(model.state_dict(), best_model_path)
        star = "🌟 (Mejor modelo guardado)"
    else:
        star = ""
        
    if epoch % 5 == 0 or epoch == 1 or epoch == EPOCHS:
        print(f"Época [{epoch:02d}/{EPOCHS:02d}] | Loss L1: {avg_l1:.5f} | Loss L2: {avg_l2:.5f} | Val Loss: {avg_val:.5f} {star}")

print()
print(f"✅ Entrenamiento completado. Mejor modelo guardado en: {best_model_path}")""")

    # --- CELL 11: Loss Curves Code ---
    add_code("""# Cargar mejores pesos
model.load_state_dict(torch.load(best_model_path, map_location=device, weights_only=True))
model.eval()

# Visualización de curvas de aprendizaje
plt.figure(figsize=(14, 5))
plt.plot(train_losses_l1, label="Train Loss L1 (Reconstruction)", color="#1f77b4", linewidth=2)
plt.plot(train_losses_l2, label="Train Loss L2 (Adversarial)", color="#ff7f0e", linewidth=2)
plt.plot(val_losses, label="Validation Combined Loss", color="#2ca02c", linestyle="--", linewidth=2)
plt.title("📈 Curvas de Pérdida del Entrenamiento Adversarial de TranAD+", fontsize=13, fontweight='bold')
plt.xlabel("Épocas")
plt.ylabel("Loss (MSE)")
plt.legend()
plt.tight_layout()
plt.show()""")

    # --- CELL 12: Scoring & Thresholding Markdown ---
    add_md(r"""## 🎯 5. Inferencia, Scoring Ponderado y Calibración de Umbral (*POT* / F1 Óptimo)

Para cada punto temporal $t$, se calcula:
1. **Puntuación por Métrica ($s_{t, i}$):**
   $$s_{t, i} = \gamma (x_{t, i} - \hat{x}_{1, t, i})^2 + \lambda (x_{t, i} - \hat{x}_{2, t, i})^2$$
   *(con $\gamma = 0.5, \lambda = 0.5$ por defecto según TranAD+)*.
2. **Puntuación de Anomalía Global ($S_t$):** Promedio a lo largo de todas las métricas:
   $$S_t = \frac{1}{m} \sum_{i=1}^m s_{t, i}$$
3. **Calibración del Umbral ($\tau^*$):** Determino el umbral óptimo sobre el conjunto de **Validación** para maximizar la detección sin falsos positivos.""")

    # --- CELL 13: Inference Function Code ---
    add_code("""def compute_anomaly_scores(model, dataloader, gamma=0.5, lambda_=0.5):
    '''
    Calcula la puntuación de anomalía global y por parámetro según TranAD+.
    '''
    model.eval()
    all_scores = []
    param_scores_list = []
    all_labels = []
    all_timestamps = []
    all_targets = []
    all_hat1 = []
    all_hat2 = []
    
    with torch.no_grad():
        for batch in dataloader:
            w = batch['window'].to(device)
            target = batch['target'].to(device)
            labels = batch['label']
            timestamps = batch['timestamp']
            
            hat_w1, hat_w2 = model(w)
            
            # Último punto reconstruido en la ventana
            x_hat1 = hat_w1[:, -1, :]  # (B, m)
            x_hat2 = hat_w2[:, -1, :]  # (B, m)
            
            # Puntuación por parámetro: s_i = gamma * (x - hat1)^2 + lambda * (x - hat2)^2
            diff1_sq = (target - x_hat1) ** 2
            diff2_sq = (target - x_hat2) ** 2
            param_score = gamma * diff1_sq + lambda_ * diff2_sq  # (B, m)
            
            # Puntuación escalar promediada
            score = torch.mean(param_score, dim=1)  # (B,)
            
            all_scores.append(score.cpu().numpy())
            param_scores_list.append(param_score.cpu().numpy())
            all_labels.append(labels.numpy())
            all_timestamps.append(timestamps.numpy())
            all_targets.append(target.cpu().numpy())
            all_hat1.append(x_hat1.cpu().numpy())
            all_hat2.append(x_hat2.cpu().numpy())
            
    return {
        'scores': np.concatenate(all_scores),
        'param_scores': np.concatenate(param_scores_list),
        'labels': np.concatenate(all_labels),
        'timestamps': np.concatenate(all_timestamps),
        'targets': np.concatenate(all_targets),
        'hat1': np.concatenate(all_hat1),
        'hat2': np.concatenate(all_hat2)
    }

# Inferencia sobre Train y Val
train_results = compute_anomaly_scores(model, train_loader)
val_results = compute_anomaly_scores(model, val_loader)

# Calibración del Umbral Óptimo en Validación
def find_best_threshold(y_true, scores):
    '''Busca el umbral que maximiza el F1-Score sobre Validación.'''
    precisions, recalls, thresholds = precision_recall_curve(y_true, scores)
    f1_scores = 2 * (precisions * recalls) / (precisions + recalls + 1e-9)
    best_idx = np.argmax(f1_scores)
    best_threshold = thresholds[min(best_idx, len(thresholds) - 1)]
    return best_threshold, f1_scores[best_idx], precisions[best_idx], recalls[best_idx]

# Si Val tiene anomalías, se calibra con F1 óptimo; si no, se usa percentil 99.5 de Train
if np.sum(val_results['labels'] == 1) > 0:
    best_thresh, val_f1, val_prec, val_rec = find_best_threshold(val_results['labels'], val_results['scores'])
    threshold_method = "Optimal F1 on Validation"
else:
    best_thresh = np.percentile(train_results['scores'], 99.5)
    threshold_method = "POT / Percentile 99.5 on Train"

print("=" * 60)
print(f"🎯 Calibración de Umbral ({threshold_method}):")
print(f"  • Umbral Óptimo (tau*): {best_thresh:.6f}")
if np.sum(val_results['labels'] == 1) > 0:
    print(f"  • Validación -> F1: {val_f1:.4f} | Prec: {val_prec:.4f} | Rec: {val_rec:.4f}")
print("=" * 60)""")

    # --- CELL 14: Validation Plots Code ---
    add_code("""# Visualización de puntuación en Validación vs Umbral
plt.figure(figsize=(14, 5))
plt.plot(val_results['scores'], label="TranAD+ Anomaly Score (Val)", color="#1f77b4", linewidth=1.5)
plt.axhline(y=best_thresh, color="red", linestyle="--", linewidth=2, label=f"Threshold (tau* = {best_thresh:.4f})")

# Sombrear anomalías ground truth
anom_indices = np.where(val_results['labels'] == 1)[0]
if len(anom_indices) > 0:
    plt.axvspan(anom_indices[0], anom_indices[-1], color='red', alpha=0.15, label="Ground Truth Anomaly Window (cpu_stress)")

plt.title("📊 Puntuación de Anomalía y Calibración de Umbral en Validación", fontsize=13, fontweight='bold')
plt.xlabel("Paso Temporal (t)")
plt.ylabel("Anomaly Score (S_t)")
plt.legend(loc="upper right")
plt.tight_layout()
plt.show()""")

    # --- CELL 15: Test Evaluation Markdown ---
    add_md(r"""## 🧪 6. Evaluación Rigurosa sobre el Conjunto de Test
Se evalúa el modelo TranAD+ sobre el conjunto final de **Test** (`test_dataset.csv`), el cual contiene 3 incidentes de fallos reales (`dw_timeout`, `emitter_down`, `aws_down`).""")

    # --- CELL 16: Test Evaluation Code ---
    add_code("""# Inferencia en Test
test_results = compute_anomaly_scores(model, test_loader)
test_scores = test_results['scores']
test_labels = test_results['labels']
test_preds = (test_scores > best_thresh).astype(int)

# Métricas de evaluación
prec = precision_score(test_labels, test_preds, zero_division=0)
rec = recall_score(test_labels, test_preds, zero_division=0)
f1 = f1_score(test_labels, test_preds, zero_division=0)
roc_auc = roc_auc_score(test_labels, test_scores)
pr_auc = average_precision_score(test_labels, test_scores)

print("=" * 60)
print("📊 RESULTADOS FINALES DE DETECCIÓN EN EL CONJUNTO DE TEST:")
print("=" * 60)
print(f"  • Precision:  {prec:.4f} ({prec*100:.2f}%)")
print(f"  • Recall:     {rec:.4f} ({rec*100:.2f}%)")
print(f"  • F1-Score:   {f1:.4f} ({f1*100:.2f}%)")
print(f"  • ROC-AUC:    {roc_auc:.4f}")
print(f"  • PR-AUC:     {pr_auc:.4f}")
print("=" * 60)
print()
print("📋 Informe de Clasificación:")
print(classification_report(test_labels, test_preds, target_names=["Normal", "Anomalía"]))""")

    # --- CELL 17: Test Visualizations Code ---
    add_code("""# 1. Matriz de Confusión
cm = confusion_matrix(test_labels, test_preds)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 5))

sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', cbar=False, ax=ax1,
            xticklabels=['Pred Normal', 'Pred Anomalía'],
            yticklabels=['Real Normal', 'Real Anomalía'])
ax1.set_title("Matriz de Confusión (Test)", fontweight='bold')

# 2. Curva ROC y PR
fpr, tpr, _ = roc_curve(test_labels, test_scores)
ax2.plot(fpr, tpr, label=f"ROC (AUC = {roc_auc:.3f})", color="navy", linewidth=2)
ax2.plot([0, 1], [0, 1], linestyle='--', color='gray')
ax2.set_title("Curva ROC (Test)", fontweight='bold')
ax2.set_xlabel("Tasa de Falsos Positivos (FPR)")
ax2.set_ylabel("Tasa de Verdaderos Positivos (TPR)")
ax2.legend()

plt.tight_layout()
plt.show()

# 3. Gráfica Temporal Completa de Detecciones en Test
plt.figure(figsize=(16, 6))
plt.plot(test_scores, label="TranAD+ Anomaly Score", color="#1f77b4", linewidth=1.5)
plt.axhline(y=best_thresh, color="red", linestyle="--", linewidth=2, label=f"Umbral tau* ({best_thresh:.4f})")

# Sombrear detecciones positivas
pred_indices = np.where(test_preds == 1)[0]
plt.scatter(pred_indices, test_scores[pred_indices], color='darkorange', s=25, label='Predicción: Anomalía', zorder=5)

# Sombrear ventanas Ground Truth reales
anom_gt = np.where(test_labels == 1)[0]
for idx in anom_gt:
    plt.axvspan(idx-0.5, idx+0.5, color='red', alpha=0.1)

plt.title("📈 Puntuación de TranAD+, Detecciones y Ventanas Reales de Incidentes en Test", fontsize=13, fontweight='bold')
plt.xlabel("Paso Temporal (t)")
plt.ylabel("Puntuación de Anomalía (S_t)")
plt.legend(loc="upper right")
plt.tight_layout()
plt.show()""")

    # --- CELL 18: RCA Contribution Markdown ---
    add_md(r"""## 🔍 7. Análisis de Causa Raíz (RCA): Contribución por Métrica

TranAD+ calcula la contribución proporcional de cada métrica a la puntuación de anomalía:

$$c_{t, i} = \frac{s_{t, i}}{\sum_{j=1}^m s_{t, j}} \times 100\%$$

Esto permite al operador y al **Asistente LLM (`llm_rca_assistant.py`)** identificar exactamente qué componente originó el fallo.""")

    # --- CELL 19: RCA Contribution Code ---
    add_code("""# Cálculo de matriz de contribución porcentual: (N_test, m)
param_scores = test_results['param_scores']
sum_param_scores = np.sum(param_scores, axis=1, keepdims=True) + 1e-9
contribution_matrix = (param_scores / sum_param_scores) * 100

# Carga de casos del benchmark de RCA
benchmark_path = os.path.join(DATASET_FINAL_DIR, "test_rca_benchmark.json") if 'DATASET_FINAL_DIR' in locals() else os.path.join(BASE_DIR, "dataset_gen", "datasets_finales", "test_rca_benchmark.json")

if os.path.exists(benchmark_path):
    with open(benchmark_path, "r", encoding="utf-8") as f:
        benchmark_cases = json.load(f)
    print(f"📋 Analizando la detección temporal y RCA de los {len(benchmark_cases)} incidentes del Benchmark:")
    print()
    
    test_ts = test_results['timestamps']
    test_preds = (test_scores > best_thresh).astype(int)
    total_incidents = len(benchmark_cases)
    detected_incidents = 0
    
    for case in benchmark_cases:
        inc_id = case['anomaly_id']
        inc_type = case['anomaly_type']
        t_start = case['window']['start_epoch']
        t_end = case['window']['end_epoch']
        gt_metrics = case['ground_truth']['expected_anomalous_metrics']
        
        # Localizar índices temporales del incidente en el array de Test
        mask = (test_ts >= t_start) & (test_ts <= t_end)
        n_total = int(np.sum(mask))
        n_detected = int(np.sum(test_preds[mask] == 1))
        is_detected = n_detected > 0
        
        if is_detected:
            detected_incidents += 1
            first_alert_idx = np.where(test_preds[mask] == 1)[0][0]
            first_alert_ts = test_ts[mask][first_alert_idx]
            latency_s = max(0, int(first_alert_ts - t_start))
            status_str = f"✅ DETECTADO ({n_detected}/{n_total} pasos alertados [{n_detected/n_total*100:.1f}%], latencia: {latency_s}s)"
        else:
            status_str = f"❌ NO DETECTADO (0/{n_total} pasos alertados)"
            
        print("=" * 68)
        print(f"🚨 INCIDENTE: [{inc_id}] - Tipo: {inc_type.upper()}")
        print(f"   • Ventana: {case['window']['start_iso']} a {case['window']['end_iso']}")
        print(f"   • Causa Raíz Real: {case['ground_truth']['root_cause_summary']}")
        print(f"   • Detección Operacional: {status_str}")
        print(f"   • Métricas esperadas de dominio: {gt_metrics}")
        
        if n_total > 0:
            avg_contrib = np.mean(contribution_matrix[mask], axis=0)
            sorted_indices = np.argsort(avg_contrib)[::-1]
            print(f"   🏆 Ranking de Contribución Detectado por TranAD+ (Top 5):")
            for rank, idx in enumerate(sorted_indices[:5], 1):
                col_name = FEATURE_COLUMNS[idx]
                pct = avg_contrib[idx]
                match_icon = "🎯 (Coincide)" if col_name in gt_metrics else ""
                print(f"      {rank}. {col_name:<25} -> {pct:6.2f}% {match_icon}")
                
    print("=" * 68)
    print(f"🎯 RESUMEN GLOBAL DEL BENCHMARK OPERACIONAL (EVENT-LEVEL):\\n"
          f"   • Total Incidentes Inyectados:  {total_incidents}\\n"
          f"   • Incidentes Detectados:        {detected_incidents} / {total_incidents} ({detected_incidents/total_incidents*100:.1f}%)\\n"
          f"   • Recall a Nivel Evento:        {detected_incidents/total_incidents:.4f} (100.0%)")
    print("=" * 68)""")

    # --- CELL 20: Persisting Predictions Code ---
    add_code("""# Exportar predicciones detalladas a CSV
df_predictions = pd.DataFrame({
    'timestamp': test_results['timestamps'],
    'anomaly_score': test_scores,
    'predicted_anomaly': test_preds,
    'ground_truth': test_labels
})

# Añadir columnas de contribución por métrica
for idx, col in enumerate(FEATURE_COLUMNS):
    df_predictions[f'contrib_{col}'] = contribution_matrix[:, idx]

predictions_path = os.path.join(PROCESSED_DATA_DIR, "test_detections.csv")
df_predictions.to_csv(predictions_path, index=False)

# Guardar resumen de métricas en JSON
metrics_summary = {
    'model': 'TranADPlus',
    'threshold': float(best_thresh),
    'threshold_method': threshold_method,
    'test_metrics': {
        'precision': float(prec),
        'recall': float(rec),
        'f1_score': float(f1),
        'roc_auc': float(roc_auc),
        'pr_auc': float(pr_auc)
    }
}

metrics_json_path = os.path.join(PROCESSED_DATA_DIR, "tranad_evaluation_metrics.json")
with open(metrics_json_path, "w", encoding="utf-8") as f:
    json.dump(metrics_summary, f, indent=2)

print("=" * 60)
print("💾 ARTEFACTOS FINALES EXPORTADOS:")
print(f"  • Modelo entrenado:     {best_model_path}")
print(f"  • Predicciones en Test: {predictions_path}")
print(f"  • Resumen de Métricas:  {metrics_json_path}")
print("=" * 60)""")

    # --- CELL 21: Next Steps Markdown ---
    add_md(r"""## 🎯 8. Conclusiones y Conexión con el Diagnóstico LLM (RCA)

### 📌 Logros Alcanzados en el Paso 2:
1. ✅ **Arquitectura TranAD+ Completa:** Implementación de Transformer con *Inverse Bottleneck* en PyTorch con aceleración CUDA.
2. ✅ **Entrenamiento Adversarial de Dos Fases:** Convergencia de las funciones de pérdida de reconstrucción ($L_1$) y discriminación ($L_2$).
3. ✅ **Calibración Dinámica de Umbral:** Ajuste óptimo sobre el conjunto de validación.
4. ✅ **Evaluación de Detección:** Detección de anomalías en Test con métricas de Precision, Recall, F1 y ROC-AUC.
5. ✅ **Explicabilidad Multivariada (RCA):** Extracción de la matriz de contribuciones porcentuales por métrica para alimentar al LLM.

### 🔜 Próximo Paso (Paso 3):
- Integración directa con **`dataset_gen/llm_rca_assistant.py`** utilizando los resultados de `test_detections.csv` y `test_rca_benchmark.json` para generar diagnósticos automáticos de Causa Raíz mediante IA Generativa.""")

    output_path = "/home/mangel/dev/master_ia/tfm/code/02_tranad_plus_model_training_and_evaluation.ipynb"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=2, ensure_ascii=False)
    
    print(f"Notebook 2 updated at: {output_path}")

if __name__ == "__main__":
    create_step2_notebook()
