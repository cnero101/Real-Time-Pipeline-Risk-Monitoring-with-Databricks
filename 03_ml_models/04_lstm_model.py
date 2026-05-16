# Databricks notebook source
# DBTITLE 1,Cell 1
# Cell 1: Install and verify PyTorch
%pip install torch

import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from torch.utils.data import DataLoader, TensorDataset

print(f'PyTorch version : {torch.__version__}')
print(f'CUDA available  : {torch.cuda.is_available()}')
print('PyTorch ready')

# COMMAND ----------

# Cell 2: Restart kernel to activate PyTorch
dbutils.library.restartPython()

# COMMAND ----------

# Cell 3: Verify PyTorch and all imports
import torch
import torch.nn as nn
import numpy as np
import pandas as pd
import mlflow
import mlflow.pytorch
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import classification_report
import warnings
warnings.filterwarnings('ignore')

print(f'PyTorch version : {torch.__version__}')
print(f'CUDA available  : {torch.cuda.is_available()}')
print(f'MLflow version  : {mlflow.__version__}')
print()
print('All imports successful — ready to build LSTM')

# COMMAND ----------

# Cell 4: Generate slow-leak training data
import random
import uuid
from datetime import datetime, timezone, timedelta

PIPE_SEGMENTS = ['PIPE-001', 'PIPE-002', 'PIPE-003', 'PIPE-004', 'PIPE-005']

def generate_slow_leak_sequence(pipe_id, sequence_length=50):
    records  = []
    base_time = datetime.now(timezone.utc)
    pressure    = random.gauss(1000, 30)
    flow        = random.gauss(200, 15)
    acoustic    = random.gauss(0.3, 0.05)
    temperature = random.gauss(45, 3)

    for i in range(sequence_length):
        if i >= 20:
            leak_progress = (i - 20) / 30
            pressure    -= random.gauss(10, 2)
            flow        += random.gauss(3, 1)
            acoustic    += random.gauss(0.04, 0.01)
            temperature -= random.gauss(0.3, 0.1)
            is_anomaly   = 'slow_leak' if leak_progress > 0.3 else 'normal'
        else:
            pressure    += random.gauss(0, 5)
            flow        += random.gauss(0, 3)
            acoustic    += random.gauss(0, 0.02)
            temperature += random.gauss(0, 0.5)
            is_anomaly   = 'normal'

        records.append({
            'event_id':        str(uuid.uuid4()),
            'pipe_segment_id': pipe_id,
            'event_timestamp': base_time + timedelta(seconds=i*30),
            'pressure_psi':    round(max(pressure, 100), 2),
            'flow_rate_lpm':   round(max(flow, 0), 2),
            'acoustic_g_rms':  round(max(acoustic, 0), 4),
            'temperature_c':   round(temperature, 2),
            'is_anomaly':      is_anomaly,
            'sequence_id':     f'{pipe_id}_seq_{i}',
        })
    return records

# Generate 20 sequences per segment
print('Generating slow-leak sequences...')
all_records = []
for pipe_id in PIPE_SEGMENTS:
    for _ in range(20):
        all_records.extend(generate_slow_leak_sequence(pipe_id))

slow_leak_df = pd.DataFrame(all_records)
print(f'Total records    : {len(all_records)}')
print(f'Slow leak events : {(slow_leak_df["is_anomaly"] == "slow_leak").sum()}')
print(f'Normal events    : {(slow_leak_df["is_anomaly"] == "normal").sum()}')
print()
sample = slow_leak_df[slow_leak_df['pipe_segment_id'] == 'PIPE-001'].head(30)
print(sample[['event_timestamp', 'pressure_psi', 'acoustic_g_rms', 'is_anomaly']].to_string(index=False))

# COMMAND ----------

# Cell 5: Load existing Silver data and combine with slow-leak data
CATALOG      = 'main'
SCHEMA       = 'pipeline_leak'
SILVER_TABLE = f'{CATALOG}.{SCHEMA}.silver_sensor_clean'

FEATURE_COLS = ['pressure_psi', 'flow_rate_lpm', 'acoustic_g_rms', 'temperature_c']

# Load existing Silver data
existing_df = spark.table(SILVER_TABLE).toPandas()

# Convert any categorical columns to string
for col in existing_df.select_dtypes(include='category').columns:
    existing_df[col] = existing_df[col].astype(str)

# Prepare slow leak data
slow_leak_df['event_timestamp'] = pd.to_datetime(slow_leak_df['event_timestamp'], utc=True)
existing_df['event_timestamp']  = pd.to_datetime(existing_df['event_timestamp'],  utc=True)

# Combine both datasets
combined_df = pd.concat([
    existing_df[FEATURE_COLS + ['is_anomaly', 'pipe_segment_id', 'event_timestamp']],
    slow_leak_df[FEATURE_COLS + ['is_anomaly', 'pipe_segment_id', 'event_timestamp']]
], ignore_index=True)

# Binary label: 1 = any anomaly, 0 = normal
combined_df['label'] = combined_df['is_anomaly'].apply(
    lambda x: 0 if x == 'normal' else 1
)

# Convert pipe_segment_id to string before sorting
combined_df['pipe_segment_id']  = combined_df['pipe_segment_id'].astype(str)
combined_df['event_timestamp']  = pd.to_datetime(combined_df['event_timestamp'], utc=True)

combined_df = combined_df.sort_values(
    ['pipe_segment_id', 'event_timestamp']
).reset_index(drop=True)

print(f'Existing Silver records : {len(existing_df)}')
print(f'New slow-leak records   : {len(slow_leak_df)}')
print(f'Combined total          : {len(combined_df)}')
print(f'Anomaly events          : {combined_df["label"].sum()}')
print(f'Normal events           : {(combined_df["label"] == 0).sum()}')
print(f'Anomaly rate            : {combined_df["label"].mean()*100:.1f}%')

# COMMAND ----------

# Cell 6: Build LSTM sequences and prepare training data
SEQUENCE_LENGTH = 20  # LSTM looks back 20 readings to detect trends

def create_sequences(data, labels, seq_length):
    X, y = [], []
    for i in range(len(data) - seq_length):
        X.append(data[i:i + seq_length])
        y.append(labels[i + seq_length])
    return np.array(X), np.array(y)

# Scale features to 0-1 range
scaler_lstm = MinMaxScaler()
features    = scaler_lstm.fit_transform(combined_df[FEATURE_COLS].values)
labels      = combined_df['label'].values

# Create sequences
X, y = create_sequences(features, labels, SEQUENCE_LENGTH)

# Train/test split — 80/20
split       = int(len(X) * 0.8)
X_train, X_test = X[:split], X[split:]
y_train, y_test = y[:split], y[split:]

# Convert to PyTorch tensors
X_train_t = torch.FloatTensor(X_train)
y_train_t = torch.FloatTensor(y_train)
X_test_t  = torch.FloatTensor(X_test)
y_test_t  = torch.FloatTensor(y_test)

# Create DataLoaders
train_dataset = TensorDataset(X_train_t, y_train_t)
train_loader  = DataLoader(train_dataset, batch_size=32, shuffle=True)

print(f'Sequence length  : {SEQUENCE_LENGTH} readings')
print(f'Total sequences  : {len(X)}')
print(f'Training samples : {len(X_train)}')
print(f'Test samples     : {len(X_test)}')
print(f'Feature shape    : {X_train.shape}')
print(f'Anomaly rate     : {y_train.mean()*100:.1f}%')

# COMMAND ----------

# Cell 7: Define LSTM model and train
class PipelineLSTM(nn.Module):
    def __init__(self, input_size=4, hidden_size=64, num_layers=2, dropout=0.2):
        super(PipelineLSTM, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers  = num_layers
        self.lstm = nn.LSTM(
            input_size  = input_size,
            hidden_size = hidden_size,
            num_layers  = num_layers,
            batch_first = True,
            dropout     = dropout
        )
        self.fc      = nn.Linear(hidden_size, 1)
        self.sigmoid = nn.Sigmoid()

    def forward(self, x):
        h0  = torch.zeros(self.num_layers, x.size(0), self.hidden_size)
        c0  = torch.zeros(self.num_layers, x.size(0), self.hidden_size)
        out, _ = self.lstm(x, (h0, c0))
        out = self.fc(out[:, -1, :])
        return self.sigmoid(out)

# Initialize model, loss, optimizer
model_lstm = PipelineLSTM(input_size=4, hidden_size=64, num_layers=2, dropout=0.2)
criterion  = nn.BCELoss()
optimizer  = torch.optim.Adam(model_lstm.parameters(), lr=0.001)

# Training loop — 15 epochs
EPOCHS = 15
print('Training LSTM...')
print(f'Epoch   Loss')
print('-' * 20)

for epoch in range(EPOCHS):
    model_lstm.train()
    total_loss = 0
    for X_batch, y_batch in train_loader:
        optimizer.zero_grad()
        outputs    = model_lstm(X_batch).squeeze()
        loss       = criterion(outputs, y_batch)
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
    avg_loss = total_loss / len(train_loader)
    print(f'{epoch+1:>5}   {avg_loss:.4f}')

print()
print('Training complete')

# COMMAND ----------

# DBTITLE 1,Cell 8
# Cell 8: Evaluate LSTM and register in MLflow
model_lstm.eval()

with torch.no_grad():
    y_pred_prob = model_lstm(X_test_t).squeeze().numpy()
    y_pred      = (y_pred_prob >= 0.5).astype(int)

report = classification_report(
    y_test, y_pred,
    target_names=['Normal', 'Anomaly'],
    output_dict=True
)

print('=== LSTM EVALUATION ===')
print(classification_report(y_test, y_pred, target_names=['Normal', 'Anomaly']))

# Log to MLflow
mlflow.set_experiment('/Users/ifeanyinjoku2@gmail.com/pipeline_leak_anomaly_detection')

# Create signature and input example for Unity Catalog
with torch.no_grad():
    sample_input = X_train_t[:5]
    sample_output = model_lstm(sample_input).squeeze().numpy()

signature = mlflow.models.infer_signature(
    model_input=sample_input.numpy(),
    model_output=sample_output
)

with mlflow.start_run(run_name='lstm_sequence_detector_v1') as run:
    mlflow.log_param('model_type',       'LSTM')
    mlflow.log_param('sequence_length',  20)
    mlflow.log_param('hidden_size',      64)
    mlflow.log_param('num_layers',       2)
    mlflow.log_param('epochs',           15)
    mlflow.log_param('training_samples', len(X_train))
    mlflow.log_param('features',         str(FEATURE_COLS))
    mlflow.log_metric('precision_anomaly', report['Anomaly']['precision'])
    mlflow.log_metric('recall_anomaly',    report['Anomaly']['recall'])
    mlflow.log_metric('f1_anomaly',        report['Anomaly']['f1-score'])
    mlflow.log_metric('accuracy',          report['accuracy'])
    mlflow.pytorch.log_model(
        model_lstm,
        'lstm_model',
        signature=signature,
        input_example=sample_input.numpy(),
        registered_model_name='pipeline_lstm_detector'
    )
    print(f'Run ID: {run.info.run_id}')
    print('LSTM registered in MLflow as pipeline_lstm_detector')

# COMMAND ----------

# Cell 9: Register LSTM with signature for Unity Catalog
from mlflow.models.signature import infer_signature

# Create sample input/output for signature
sample_input  = X_train_t[:5].numpy()
sample_output = model_lstm(X_train_t[:5]).detach().numpy()

signature = infer_signature(sample_input, sample_output)

mlflow.set_experiment('/Users/ifeanyinjoku2@gmail.com/pipeline_leak_anomaly_detection')

with mlflow.start_run(run_name='lstm_sequence_detector_v2') as run:
    mlflow.log_param('model_type',       'LSTM')
    mlflow.log_param('sequence_length',  20)
    mlflow.log_param('hidden_size',      64)
    mlflow.log_param('num_layers',       2)
    mlflow.log_param('epochs',           15)
    mlflow.log_param('training_samples', len(X_train))
    mlflow.log_metric('precision_anomaly', report['Anomaly']['precision'])
    mlflow.log_metric('recall_anomaly',    report['Anomaly']['recall'])
    mlflow.log_metric('f1_anomaly',        report['Anomaly']['f1-score'])
    mlflow.log_metric('accuracy',          report['accuracy'])

    mlflow.pytorch.log_model(
        model_lstm,
        'lstm_model',
        signature=signature,
        input_example=sample_input,
        registered_model_name='pipeline_lstm_detector'
    )

    print(f'Run ID: {run.info.run_id}')
    print('LSTM v2 registered with signature in MLflow')

# Set production alias
from mlflow.tracking import MlflowClient
client   = MlflowClient()
versions = client.search_model_versions("name='workspace.default.pipeline_lstm_detector'")
latest   = max([int(v.version) for v in versions])

client.set_registered_model_alias(
    name='workspace.default.pipeline_lstm_detector',
    alias='production',
    version=latest
)

print(f'LSTM version {latest} promoted to production alias')
print()
print('=== MLflow Model Registry ===')
print('Isolation Forest : workspace.default.pipeline_leak_detector@production')
print('LSTM             : workspace.default.pipeline_lstm_detector@production')
print()
print('Both models now live in MLflow. Gold scoring can use either or both.')

# COMMAND ----------

# Cell 10: Dual-model Gold scoring — Isolation Forest + LSTM combined
import mlflow.sklearn
import mlflow.pytorch

CATALOG      = 'main'
SCHEMA       = 'pipeline_leak'
SILVER_TABLE = f'{CATALOG}.{SCHEMA}.silver_sensor_clean'
GOLD_TABLE   = f'{CATALOG}.{SCHEMA}.gold_alerts_dual'

IF_MODEL     = 'workspace.default.pipeline_leak_detector'
SCALER_MODEL = 'workspace.default.pipeline_scaler'
LSTM_MODEL   = 'workspace.default.pipeline_lstm_detector'

IF_FEATURE_COLS = [
    'pressure_psi', 'flow_rate_lpm', 'acoustic_g_rms', 'temperature_c',
    'pressure_delta', 'flow_imbalance_ratio', 'acoustic_zscore'
]
LSTM_FEATURE_COLS = [
    'pressure_psi', 'flow_rate_lpm', 'acoustic_g_rms', 'temperature_c'
]

# Load all three models
print('Loading models from MLflow registry...')
if_model   = mlflow.sklearn.load_model(f'models:/{IF_MODEL}@production')
if_scaler  = mlflow.sklearn.load_model(f'models:/{SCALER_MODEL}@production')
lstm_model = mlflow.pytorch.load_model(f'models:/{LSTM_MODEL}@production')
lstm_model.eval()
print('All models loaded')

# Load Silver data
pdf = spark.table(SILVER_TABLE).toPandas()
for col in pdf.select_dtypes(include='category').columns:
    pdf[col] = pdf[col].astype(str)
print(f'Silver records: {len(pdf)}')

# COMMAND ----------

# Cell 11: Score with both models and combine results
from sklearn.preprocessing import MinMaxScaler as SklearnScaler

# ── Isolation Forest scoring ──────────────────────────────────
X_if     = if_scaler.transform(pdf[IF_FEATURE_COLS].values)
if_preds = if_model.predict(X_if)
if_scores = if_model.decision_function(X_if)

pdf['if_prediction'] = if_preds
pdf['if_score']      = if_scores
pdf['if_anomaly']    = (if_preds == -1).astype(int)

# ── LSTM scoring ──────────────────────────────────────────────
lstm_scaler = SklearnScaler()
X_lstm_raw  = lstm_scaler.fit_transform(pdf[LSTM_FEATURE_COLS].values)

SEQUENCE_LENGTH = 20
lstm_preds_all  = np.zeros(len(pdf))

for i in range(SEQUENCE_LENGTH, len(pdf)):
    seq = X_lstm_raw[i-SEQUENCE_LENGTH:i]
    seq_tensor = torch.FloatTensor(seq).unsqueeze(0)
    with torch.no_grad():
        prob = lstm_model(seq_tensor).squeeze().item()
    lstm_preds_all[i] = 1 if prob >= 0.5 else 0

pdf['lstm_anomaly'] = lstm_preds_all.astype(int)

# ── Combined decision ─────────────────────────────────────────
# CRITICAL: both models agree it is an anomaly
# WARNING: only one model flags it
# NORMAL: neither model flags it
def combined_severity(row):
    if_flag   = row['if_anomaly']
    lstm_flag = row['lstm_anomaly']
    if if_flag == 1 and lstm_flag == 1:
        return 'CRITICAL'
    elif if_flag == 1 or lstm_flag == 1:
        return 'WARNING'
    return 'NORMAL'

pdf['severity']      = pdf.apply(combined_severity, axis=1)
pdf['detection_source'] = pdf.apply(
    lambda r: 'BOTH' if r['if_anomaly'] and r['lstm_anomaly']
    else 'IF_ONLY' if r['if_anomaly']
    else 'LSTM_ONLY' if r['lstm_anomaly']
    else 'NONE', axis=1
)

print('=== DUAL MODEL SCORING RESULTS ===')
print()
print('Severity distribution:')
print(pdf['severity'].value_counts().to_string())
print()
print('Detection source breakdown:')
print(pdf['detection_source'].value_counts().to_string())
print()
print('Key insight:')
lstm_only = (pdf['detection_source'] == 'LSTM_ONLY').sum()
print(f'  LSTM caught {lstm_only} slow leaks that Isolation Forest missed')

# COMMAND ----------

# Cell 12: Write dual-model Gold table
def build_alert_message(row):
    if row['severity'] == 'NORMAL':
        return ''
    triggers = []
    if pd.notna(row.get('pressure_delta')) and row['pressure_delta'] < -100:
        triggers.append(f"Pressure drop {row['pressure_delta']:.0f} PSI")
    if pd.notna(row.get('flow_imbalance_ratio')) and row['flow_imbalance_ratio'] > 1.25:
        triggers.append(f"Flow spike x{row['flow_imbalance_ratio']:.2f}")
    if pd.notna(row.get('acoustic_zscore')) and abs(row['acoustic_zscore']) > 2.0:
        triggers.append(f"Acoustic anomaly z={row['acoustic_zscore']:.2f}")
    source = row.get('detection_source', '')
    if source == 'LSTM_ONLY':
        triggers.append('Slow leak trend detected by LSTM')
    elif source == 'BOTH':
        triggers.append('Confirmed by both IF and LSTM')
    return '; '.join(triggers) if triggers else 'Multi-sensor anomaly'

pdf['alert_message'] = pdf.apply(build_alert_message, axis=1)

gold_cols = [
    'event_id', 'pipe_segment_id', 'event_timestamp',
    'pressure_psi', 'flow_rate_lpm', 'acoustic_g_rms', 'temperature_c',
    'pressure_delta', 'flow_imbalance_ratio', 'acoustic_zscore',
    'if_score', 'if_anomaly', 'lstm_anomaly',
    'severity', 'detection_source', 'alert_message', 'is_anomaly'
]

df_gold_dual = spark.createDataFrame(pdf[gold_cols])

(
    df_gold_dual
    .write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .partitionBy('pipe_segment_id', 'severity')
    .saveAsTable(f'{CATALOG}.{SCHEMA}.gold_alerts_dual')
)

print('=== DUAL MODEL GOLD TABLE ===')
spark.sql(f"""
    SELECT
        severity,
        detection_source,
        pipe_segment_id,
        COUNT(*) AS events
    FROM {CATALOG}.{SCHEMA}.gold_alerts_dual
    WHERE severity != 'NORMAL'
    GROUP BY severity, detection_source, pipe_segment_id
    ORDER BY severity, pipe_segment_id
""").show(30)

print('Dual model Gold table complete.')
print()
print('=== COMPLETE ML MODEL REGISTRY ===')
print('Isolation Forest : workspace.default.pipeline_leak_detector@production')
print('LSTM             : workspace.default.pipeline_lstm_detector@production')
print('Scaler           : workspace.default.pipeline_scaler@production')
print()
print('Burst leaks  -> Isolation Forest catches instantly')
print('Slow leaks   -> LSTM catches from sequential patterns')
print('Both flag    -> CRITICAL (highest confidence)')
print('One flags    -> WARNING (investigate)')