# Databricks notebook source
# Cell 1: Load Silver data and prepare feature matrix
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split

CATALOG = 'main'
SCHEMA  = 'pipeline_leak'
SILVER_TABLE = f'{CATALOG}.{SCHEMA}.silver_sensor_clean'

FEATURE_COLS = [
    'pressure_psi',
    'flow_rate_lpm',
    'acoustic_g_rms',
    'temperature_c',
    'pressure_delta',
    'flow_imbalance_ratio',
    'acoustic_zscore',
]

# Load Silver and convert to Pandas for scikit-learn
df = spark.table(SILVER_TABLE).toPandas()
X  = df[FEATURE_COLS].values
y  = (df['is_anomaly'] == 'leak').astype(int).values

print(f'Total samples : {len(X)}')
print(f'Leak events   : {y.sum()} ({y.mean()*100:.1f}%)')
print(f'Normal events : {(1-y).sum()}')
print(f'Features      : {FEATURE_COLS}')

# COMMAND ----------

# Cell 2: Train Isolation Forest and log everything to MLflow with signature
from mlflow.models.signature import infer_signature

mlflow.set_experiment('/Users/your-email@gmail.com/pipeline_leak_anomaly_detection')

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

scaler     = StandardScaler()
X_train_sc = scaler.fit_transform(X_train)
X_test_sc  = scaler.transform(X_test)

with mlflow.start_run(run_name='isolation_forest_v1') as run:
    contamination = 0.08
    n_estimators  = 200

    mlflow.log_param('model_type',    'IsolationForest')
    mlflow.log_param('contamination', contamination)
    mlflow.log_param('n_estimators',  n_estimators)
    mlflow.log_param('features',      str(FEATURE_COLS))

    model = IsolationForest(
        contamination=contamination,
        n_estimators=n_estimators,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train_sc)

    # -1 = anomaly, 1 = normal — convert to 1=leak, 0=normal
    y_pred = (model.predict(X_test_sc) == -1).astype(int)

    report = classification_report(y_test, y_pred, output_dict=True)
    mlflow.log_metric('precision_leak', report['1']['precision'])
    mlflow.log_metric('recall_leak',    report['1']['recall'])
    mlflow.log_metric('f1_leak',        report['1']['f1-score'])
    mlflow.log_metric('accuracy',       report['accuracy'])

    # Infer signatures so Unity Catalog accepts the model
    scaler_signature = infer_signature(X_train, scaler.transform(X_train))
    model_signature  = infer_signature(X_train_sc, model.predict(X_train_sc))

    mlflow.sklearn.log_model(
        scaler, 'scaler',
        signature=scaler_signature,
        input_example=X_train[:3],
        registered_model_name='pipeline_scaler'
    )
    mlflow.sklearn.log_model(
        model, 'model',
        signature=model_signature,
        input_example=X_train_sc[:3],
        registered_model_name='pipeline_leak_detector'
    )

    print(f'Run ID: {run.info.run_id}')
    print()
    print(classification_report(y_test, y_pred, target_names=['Normal', 'Leak']))

# COMMAND ----------

# Cell 3: Promote model to Production using full Unity Catalog model names
from mlflow.tracking import MlflowClient

client = MlflowClient()

# Full three-part names: catalog.schema.model_name
DETECTOR_MODEL = 'workspace.default.pipeline_leak_detector'
SCALER_MODEL   = 'workspace.default.pipeline_scaler'

# Get the latest version of the leak detector and set production alias
versions = client.search_model_versions(f"name='{DETECTOR_MODEL}'")
latest   = max([int(v.version) for v in versions])

client.set_registered_model_alias(
    name=DETECTOR_MODEL,
    alias='production',
    version=latest
)

# Get the latest version of Scaler and set production alias
versions_sc = client.search_model_versions(f"name='{SCALER_MODEL}'")
latest_sc   = max([int(v.version) for v in versions_sc])

client.set_registered_model_alias(
    name=SCALER_MODEL,
    alias='production',
    version=latest_sc
)

print(f'pipeline_leak_detector version {latest}    → alias: production')
print(f'pipeline_scaler        version {latest_sc} → alias: production')
print()

# Verify we can load them back by alias
detector = mlflow.sklearn.load_model(f'models:/{DETECTOR_MODEL}@production')
scaler   = mlflow.sklearn.load_model(f'models:/{SCALER_MODEL}@production')

print(f'Detector loaded: {type(detector).__name__}')
print(f'Scaler loaded  : {type(scaler).__name__}')
print()
print('MLflow Model Registry complete.')
print('Next: Gold layer — scoring and alert classification.')
